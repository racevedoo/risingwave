// Copyright 2023 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::{BTreeSet, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::future::try_join_all;
use itertools::Itertools;
use risingwave_pb::common::ActorInfo;
use risingwave_pb::meta::PausedReason;
use risingwave_pb::stream_plan::barrier::{BarrierKind, Mutation};
use risingwave_pb::stream_plan::AddMutation;
use risingwave_pb::stream_service::{
    BroadcastActorInfoTableRequest, BuildActorsRequest, ForceStopActorsRequest, UpdateActorsRequest,
};
use tokio_retry::strategy::{jitter, ExponentialBackoff};
use tracing::{debug, warn, Instrument};
use uuid::Uuid;

use super::TracedEpoch;
use crate::barrier::command::CommandContext;
use crate::barrier::info::BarrierActorInfo;
use crate::barrier::{CheckpointControl, Command, GlobalBarrierManager};
use crate::manager::WorkerId;
use crate::model::{BarrierManagerState, MigrationPlan};
use crate::stream::build_actor_connector_splits;
use crate::MetaResult;

impl GlobalBarrierManager {
    // Retry base interval in milliseconds.
    const RECOVERY_RETRY_BASE_INTERVAL: u64 = 20;
    // Retry max interval.
    const RECOVERY_RETRY_MAX_INTERVAL: Duration = Duration::from_secs(5);

    #[inline(always)]
    /// Initialize a retry strategy for operation in recovery.
    fn get_retry_strategy() -> impl Iterator<Item = Duration> {
        ExponentialBackoff::from_millis(Self::RECOVERY_RETRY_BASE_INTERVAL)
            .max_delay(Self::RECOVERY_RETRY_MAX_INTERVAL)
            .map(jitter)
    }

    async fn resolve_actor_info_for_recovery(&self) -> BarrierActorInfo {
        self.resolve_actor_info(
            &mut CheckpointControl::new(self.metrics.clone()),
            &Command::barrier(),
        )
        .await
    }

    /// Clean up all dirty streaming jobs.
    async fn clean_dirty_fragments(&self) -> MetaResult<()> {
        // List stream jobs, includes both created / creating.
        // Need to rewrite as well.
        // List ALL stream jobs.
        let stream_job_ids = self.catalog_manager.list_stream_job_ids().await?;
        let to_drop_table_fragments = self
            .fragment_manager
            .list_dirty_table_fragments(|tf| {
                !stream_job_ids.contains(&tf.table_id().table_id) || !tf.is_created()
            })
            .await;

        let to_drop_streaming_ids = to_drop_table_fragments
            .iter()
            .map(|t| t.table_id())
            .collect();

        debug!("clean dirty table fragments: {:?}", to_drop_streaming_ids);
        self.fragment_manager
            .drop_table_fragments_vec(&to_drop_streaming_ids)
            .await?;

        // unregister compaction group for dirty table fragments.
        let _ = self.hummock_manager
            .unregister_table_fragments_vec(
                &to_drop_table_fragments
            )
            .await.inspect_err(|e|
            tracing::warn!(
            "Failed to unregister compaction group for {:#?}. They will be cleaned up on node restart. {:#?}",
            to_drop_table_fragments,
            e)
        );

        // clean up source connector dirty changes.
        self.source_manager
            .drop_source_change(&to_drop_table_fragments)
            .await;

        Ok(())
    }

    /// Recovery the whole cluster from the latest epoch.
    ///
    /// If `paused_reason` is `Some`, all data sources (including connectors and DMLs) will be
    /// immediately paused after recovery, until the user manually resume them either by restarting
    /// the cluster or `risectl` command. Used for debugging purpose.
    ///
    /// Returns the new state of the barrier manager after recovery.
    pub(crate) async fn recovery(
        &self,
        prev_epoch: TracedEpoch,
        paused_reason: Option<PausedReason>,
    ) -> BarrierManagerState {
        // Mark blocked and abort buffered schedules, they might be dirty already.
        self.scheduled_barriers
            .abort_and_mark_blocked("cluster is under recovering")
            .await;

        tracing::info!("recovery start!");
        self.clean_dirty_fragments()
            .await
            .expect("clean dirty fragments");
        self.sink_manager.reset().await;
        let retry_strategy = Self::get_retry_strategy();

        // We take retry into consideration because this is the latency user sees for a cluster to
        // get recovered.
        let recovery_timer = self.metrics.recovery_latency.start_timer();

        let state = tokio_retry::Retry::spawn(retry_strategy, || {
            async {
                let recovery_result: MetaResult<_> = try {
                    // Resolve actor info for recovery. If there's no actor to recover, most of the
                    // following steps will be no-op, while the compute nodes will still be reset.
                    let mut info = self.resolve_actor_info_for_recovery().await;

                    // Migrate actors in expired CN to newly joined one.
                    let migrated = self.migrate_actors(&info).await.inspect_err(|err| {
                        warn!(err = ?err, "migrate actors failed");
                    })?;
                    if migrated {
                        info = self.resolve_actor_info_for_recovery().await;
                    }

                    // Reset all compute nodes, stop and drop existing actors.
                    self.reset_compute_nodes(&info).await.inspect_err(|err| {
                        warn!(err = ?err, "reset compute nodes failed");
                    })?;

                    // update and build all actors.
                    self.update_actors(&info).await.inspect_err(|err| {
                        warn!(err = ?err, "update actors failed");
                    })?;
                    self.build_actors(&info).await.inspect_err(|err| {
                        warn!(err = ?err, "build_actors failed");
                    })?;

                    // get split assignments for all actors
                    let source_split_assignments = self.source_manager.list_assignments().await;
                    let command = Command::Plain(Some(Mutation::Add(AddMutation {
                        // Actors built during recovery is not treated as newly added actors.
                        actor_dispatchers: Default::default(),
                        added_actors: Default::default(),
                        actor_splits: build_actor_connector_splits(&source_split_assignments),
                        pause: paused_reason.is_some(),
                    })));

                    // Use a different `curr_epoch` for each recovery attempt.
                    let new_epoch = prev_epoch.next();

                    // Inject the `Initial` barrier to initialize all executors.
                    let command_ctx = Arc::new(CommandContext::new(
                        self.fragment_manager.clone(),
                        self.env.stream_client_pool_ref(),
                        info,
                        prev_epoch.clone(),
                        new_epoch.clone(),
                        paused_reason,
                        command,
                        BarrierKind::Initial,
                        self.source_manager.clone(),
                        tracing::Span::current(), // recovery span
                    ));

                    #[cfg(not(all(test, feature = "failpoints")))]
                    {
                        use risingwave_common::util::epoch::INVALID_EPOCH;

                        let mce = self.hummock_manager.get_current_max_committed_epoch().await;

                        if mce != INVALID_EPOCH {
                            command_ctx.wait_epoch_commit(mce).await?;
                        }
                    }

                    let (barrier_complete_tx, mut barrier_complete_rx) =
                        tokio::sync::mpsc::unbounded_channel();
                    self.inject_barrier(command_ctx.clone(), &barrier_complete_tx)
                        .await;
                    let res = match barrier_complete_rx.recv().await.unwrap().result {
                        Ok(response) => {
                            if let Err(err) = command_ctx.post_collect().await {
                                warn!(err = ?err, "post_collect failed");
                                Err(err)
                            } else {
                                Ok((new_epoch, response))
                            }
                        }
                        Err(err) => {
                            warn!(err = ?err, "inject_barrier failed");
                            Err(err)
                        }
                    };
                    let (new_epoch, _) = res?;

                    BarrierManagerState::new(new_epoch, command_ctx.next_paused_reason())
                };
                if recovery_result.is_err() {
                    self.metrics.recovery_failure_cnt.inc();
                }
                recovery_result
            }
            .instrument(tracing::info_span!("recovery_attempt"))
        })
        .await
        .expect("Retry until recovery success.");

        recovery_timer.observe_duration();
        self.scheduled_barriers.mark_ready().await;

        tracing::info!(
            epoch = state.in_flight_prev_epoch().value().0,
            paused = ?state.paused_reason(),
            "recovery success"
        );

        state
    }

    /// Migrate actors in expired CNs to newly joined ones, return true if any actor is migrated.
    async fn migrate_actors(&self, info: &BarrierActorInfo) -> MetaResult<bool> {
        debug!("start migrate actors.");

        // 1. get expired workers.
        let expired_workers: HashSet<WorkerId> = info
            .actor_map
            .iter()
            .filter(|(&worker, actors)| !actors.is_empty() && !info.node_map.contains_key(&worker))
            .map(|(&worker, _)| worker)
            .collect();
        if expired_workers.is_empty() {
            debug!("no expired workers, skipping.");
            return Ok(false);
        }
        let migration_plan = self.generate_migration_plan(expired_workers).await?;
        // 2. start to migrate fragment one-by-one.
        self.fragment_manager
            .migrate_fragment_actors(&migration_plan)
            .await?;
        // 3. remove the migration plan.
        migration_plan.delete(self.env.meta_store()).await?;

        debug!("migrate actors succeed.");
        Ok(true)
    }

    /// This function will generate a migration plan, which includes the mapping for all expired and
    /// in-used parallel unit to a new one.
    async fn generate_migration_plan(
        &self,
        expired_workers: HashSet<WorkerId>,
    ) -> MetaResult<MigrationPlan> {
        let mut cached_plan = MigrationPlan::get(self.env.meta_store()).await?;

        let all_worker_parallel_units = self.fragment_manager.all_worker_parallel_units().await;

        let (expired_inuse_workers, inuse_workers): (Vec<_>, Vec<_>) = all_worker_parallel_units
            .into_iter()
            .partition(|(worker, _)| expired_workers.contains(worker));

        let mut to_migrate_parallel_units: BTreeSet<_> = expired_inuse_workers
            .into_iter()
            .flat_map(|(_, pu)| pu.into_iter())
            .collect();
        let mut inuse_parallel_units: HashSet<_> = inuse_workers
            .into_iter()
            .flat_map(|(_, pu)| pu.into_iter())
            .collect();

        cached_plan.parallel_unit_plan.retain(|from, to| {
            if to_migrate_parallel_units.contains(from) {
                if !to_migrate_parallel_units.contains(&to.id) {
                    // clean up target parallel units in migration plan that are expired and not
                    // used by any actors.
                    return !expired_workers.contains(&to.worker_node_id);
                }
                return true;
            }
            false
        });
        to_migrate_parallel_units.retain(|id| !cached_plan.parallel_unit_plan.contains_key(id));
        inuse_parallel_units.extend(cached_plan.parallel_unit_plan.values().map(|pu| pu.id));

        if to_migrate_parallel_units.is_empty() {
            // all expired parallel units are already in migration plan.
            debug!("all expired parallel units are already in migration plan.");
            return Ok(cached_plan);
        }
        let mut to_migrate_parallel_units =
            to_migrate_parallel_units.into_iter().rev().collect_vec();
        debug!(
            "got to migrate parallel units {:#?}",
            to_migrate_parallel_units
        );

        let start = Instant::now();
        // if in-used expire parallel units are not empty, should wait for newly joined worker.
        'discovery: while !to_migrate_parallel_units.is_empty() {
            let mut new_parallel_units = self
                .cluster_manager
                .list_active_streaming_parallel_units()
                .await;
            new_parallel_units.retain(|pu| !inuse_parallel_units.contains(&pu.id));

            if !new_parallel_units.is_empty() {
                debug!("new parallel units found: {:#?}", new_parallel_units);
                for target_parallel_unit in new_parallel_units {
                    if let Some(from) = to_migrate_parallel_units.pop() {
                        debug!(
                            "plan to migrate from parallel unit {} to {}",
                            from, target_parallel_unit.id
                        );
                        inuse_parallel_units.insert(target_parallel_unit.id);
                        cached_plan
                            .parallel_unit_plan
                            .insert(from, target_parallel_unit);
                    } else {
                        break 'discovery;
                    }
                }
            }
            warn!(
                "waiting for new workers to join, elapsed: {}s",
                start.elapsed().as_secs()
            );
            // wait to get newly joined CN
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        // update migration plan, if there is a chain in the plan, update it.
        let mut new_plan = MigrationPlan::default();
        for (from, to) in &cached_plan.parallel_unit_plan {
            let mut to = to.clone();
            while let Some(target) = cached_plan.parallel_unit_plan.get(&to.id) {
                to = target.clone();
            }
            new_plan.parallel_unit_plan.insert(*from, to);
        }

        assert!(
            new_plan
                .parallel_unit_plan
                .values()
                .map(|pu| pu.id)
                .all_unique(),
            "target parallel units must be unique: {:?}",
            new_plan.parallel_unit_plan
        );

        new_plan.insert(self.env.meta_store()).await?;
        Ok(new_plan)
    }

    /// Update all actors in compute nodes.
    async fn update_actors(&self, info: &BarrierActorInfo) -> MetaResult<()> {
        if info.actor_map.is_empty() {
            tracing::debug!("no actor to update, skipping.");
            return Ok(());
        }

        let mut actor_infos = vec![];
        for (node_id, actors) in &info.actor_map {
            let host = info
                .node_map
                .get(node_id)
                .ok_or_else(|| anyhow::anyhow!("worker evicted, wait for online."))?
                .host
                .clone();
            actor_infos.extend(actors.iter().map(|&actor_id| ActorInfo {
                actor_id,
                host: host.clone(),
            }));
        }

        let mut node_actors = self.fragment_manager.all_node_actors(false).await;
        for (node_id, actors) in &info.actor_map {
            let node = info.node_map.get(node_id).unwrap();
            let client = self.env.stream_client_pool().get(node).await?;

            client
                .broadcast_actor_info_table(BroadcastActorInfoTableRequest {
                    info: actor_infos.clone(),
                })
                .await?;

            let request_id = Uuid::new_v4().to_string();
            tracing::debug!(request_id = request_id.as_str(), actors = ?actors, "update actors");
            client
                .update_actors(UpdateActorsRequest {
                    request_id,
                    actors: node_actors.remove(node_id).unwrap_or_default(),
                })
                .await?;
        }

        Ok(())
    }

    /// Build all actors in compute nodes.
    async fn build_actors(&self, info: &BarrierActorInfo) -> MetaResult<()> {
        if info.actor_map.is_empty() {
            tracing::debug!("no actor to build, skipping.");
            return Ok(());
        }

        for (node_id, actors) in &info.actor_map {
            let node = info.node_map.get(node_id).unwrap();
            let client = self.env.stream_client_pool().get(node).await?;

            let request_id = Uuid::new_v4().to_string();
            tracing::debug!(request_id = request_id.as_str(), actors = ?actors, "build actors");
            client
                .build_actors(BuildActorsRequest {
                    request_id,
                    actor_id: actors.to_owned(),
                })
                .await?;
        }

        Ok(())
    }

    /// Reset all compute nodes by calling `force_stop_actors`.
    async fn reset_compute_nodes(&self, info: &BarrierActorInfo) -> MetaResult<()> {
        let futures = info.node_map.values().map(|worker_node| async move {
            let client = self.env.stream_client_pool().get(worker_node).await?;
            debug!(worker = ?worker_node.id, "force stop actors");
            client
                .force_stop_actors(ForceStopActorsRequest {
                    request_id: Uuid::new_v4().to_string(),
                })
                .await
        });

        try_join_all(futures).await?;
        debug!("all compute nodes have been reset.");

        Ok(())
    }
}
