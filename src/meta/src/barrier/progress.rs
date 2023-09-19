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

use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use itertools::Itertools;
use risingwave_common::catalog::TableId;
use risingwave_common::util::epoch::Epoch;
use risingwave_pb::ddl_service::DdlProgress;
use risingwave_pb::hummock::HummockVersionStats;
use risingwave_pb::stream_service::barrier_complete_response::CreateMviewProgress;

use super::command::CommandContext;
use super::notifier::Notifier;
use crate::barrier::Command;
use crate::model::ActorId;

type CreateMviewEpoch = Epoch;
type ConsumedRows = u64;

#[derive(Clone, Copy, Debug)]
enum ChainState {
    Init,
    ConsumingUpstream(Epoch, ConsumedRows),
    Done,
}

/// Progress of all actors containing chain nodes while creating mview.
#[derive(Debug)]
struct Progress {
    states: HashMap<ActorId, ChainState>,

    done_count: usize,

    /// Creating mv id.
    creating_mv_id: TableId,

    /// Upstream mv count. Keep track of how many times each upstream MV appears.
    upstream_mv_count: HashMap<TableId, usize>,

    /// Upstream mvs total key count.
    upstream_total_key_count: u64,

    /// Consumed rows
    consumed_rows: u64,

    /// DDL definition
    definition: String,
}

impl Progress {
    /// Create a [`Progress`] for some creating mview, with all `actors` containing the chain nodes.
    fn new(
        actors: impl IntoIterator<Item = ActorId>,
        creating_mv_id: TableId,
        upstream_mv_count: HashMap<TableId, usize>,
        upstream_total_key_count: u64,
        definition: String,
    ) -> Self {
        let states = actors
            .into_iter()
            .map(|a| (a, ChainState::Init))
            .collect::<HashMap<_, _>>();
        assert!(!states.is_empty());

        Self {
            states,
            done_count: 0,
            creating_mv_id,
            upstream_mv_count,
            upstream_total_key_count,
            consumed_rows: 0,
            definition,
        }
    }

    /// Update the progress of `actor`.
    fn update(&mut self, actor: ActorId, new_state: ChainState, upstream_total_key_count: u64) {
        self.upstream_total_key_count = upstream_total_key_count;
        match self.states.remove(&actor).unwrap() {
            ChainState::Init => {}
            ChainState::ConsumingUpstream(_, old_consumed_rows) => {
                if !matches!(new_state, ChainState::Done) {
                    self.consumed_rows -= old_consumed_rows;
                }
            }
            ChainState::Done => panic!("should not report done multiple times"),
        };
        match &new_state {
            ChainState::Init => {}
            ChainState::ConsumingUpstream(_, new_consumed_rows) => {
                self.consumed_rows += new_consumed_rows;
            }
            ChainState::Done => {
                self.done_count += 1;
            }
        };
        self.states.insert(actor, new_state);
        self.calculate_progress();
    }

    /// Returns whether all chains are done.
    fn is_done(&self) -> bool {
        self.done_count == self.states.len()
    }

    /// Returns the ids of all actors containing the chain nodes for the mview tracked by this
    /// [`Progress`].
    fn actors(&self) -> impl Iterator<Item = ActorId> + '_ {
        self.states.keys().cloned()
    }

    /// `progress` = `consumed_rows` / `upstream_total_key_count`
    fn calculate_progress(&self) -> f64 {
        if self.is_done() || self.states.is_empty() {
            return 1.0;
        }
        let mut upstream_total_key_count = self.upstream_total_key_count as f64;
        if upstream_total_key_count == 0.0 {
            upstream_total_key_count = 1.0
        }
        let mut progress = self.consumed_rows as f64 / upstream_total_key_count;
        if progress >= 1.0 {
            progress = 0.99;
        }
        progress
    }
}

/// The command tracking by the [`CreateMviewProgressTracker`].
pub(super) struct TrackingCommand {
    /// The context of the command.
    pub context: Arc<CommandContext>,

    /// Should be called when the command is finished.
    pub notifiers: Vec<Notifier>,
}

/// Track the progress of all creating mviews. When creation is done, `notify_finished` will be
/// called on registered notifiers.
///
/// Tracking is done as follows:
/// Several ActorIds constitute a StreamJob.
/// A StreamJob is IDed by the Epoch of its initial barrier,
/// i.e. CreateMviewEpoch.
/// We can ID it that way because the initial barrier should ONLY
/// be used for exactly one StreamJob.
/// We don't allow multiple stream jobs scheduled on the same barrier.
///
/// With `actor_map` we can use any `ActorId` to find the ID of the StreamJob,
/// and with `progress_map` we can use the ID of the StreamJob
/// to view its progress.
///
/// We track the progress of each ActorId in a StreamJob,
/// because ALL of their progress consitutes the progress of the StreamJob.
pub(super) struct CreateMviewProgressTracker {
    // TODO(kwannoel): The real purpose of `CreateMviewEpoch`
    // Is to serve as a unique identifier for a stream job.
    // We should create a new type for this purpose
    // to make that clear.
    // Additionally if are to allow concurrent mview creation in the
    // future, CreateMViewEpoch will not be a good candidate for this.
    /// Progress of the create-mview DDL indicated by the epoch.
    progress_map: HashMap<CreateMviewEpoch, (Progress, TrackingCommand)>,

    /// Find the epoch of the create-mview DDL by the actor containing the chain node.
    actor_map: HashMap<ActorId, CreateMviewEpoch>,
}

/// FIXME: This is just a mock to simulate backfill state.
/// We need to get the actual state from the barriers.
#[derive(Clone, Copy, Debug)]
pub enum BackfillStatus {
    Init,
    ConsumingUpstream,
    Done,
}

pub struct BackfillState {
    status: BackfillStatus,
    consumed_rows: u64,
    epoch: CreateMviewEpoch, // TODO: Does not seem necessary..?
}

impl BackfillState {
    pub fn new(status: BackfillStatus, consumed_rows: u64, epoch: CreateMviewEpoch) -> Self {
        Self {
            status,
            consumed_rows,
            epoch,
        }
    }

    pub fn get_status(&self) -> BackfillStatus {
        self.status
    }

    pub fn get_consumed_rows(&self) -> u64 {
        self.consumed_rows
    }

    pub fn get_epoch(&self) -> CreateMviewEpoch {
        self.epoch
    }
}

/// Each Materialized View which scans from upstream MVs has a chain executor.
/// We should track progress of each MaterializedView which has a stateful executor.
/// This is built from barriers passing through the backfill executor.
pub struct BackfillProgress {
    inner: Vec<(ActorId, BackfillState)>,
}

impl CreateMviewProgressTracker {
    /// Empty backfill state -> Can't recover, just start from scratch.
    /// Non-empty backfill state -> Recover from the backfill state.
    ///
    /// We also need to recover row counts from etcd store,
    /// that is not persisted by backfill executor.
    pub fn recover(backfill_progress: BackfillProgress) -> Self {
        let mut progress_map = Default::default();
        let mut actor_map = Default::default();

        let progress_per_actor = backfill_progress.inner;
        for (actor_id, backfill_state) in progress_per_actor {
            let status = backfill_state.get_status();
            let consumed_rows = backfill_state.get_consumed_rows();
            let epoch = backfill_state.get_epoch();
        }
        Self {
            progress_map,
            actor_map,
        }
    }

    pub fn new() -> Self {
        Self {
            progress_map: Default::default(),
            actor_map: Default::default(),
        }
    }

    pub fn gen_ddl_progress(&self) -> Vec<DdlProgress> {
        self.progress_map
            .values()
            .map(|(x, _)| DdlProgress {
                id: x.creating_mv_id.table_id as u64,
                statement: x.definition.clone(),
                progress: format!("{:.2}%", x.calculate_progress() * 100.0),
            })
            .collect()
    }

    /// Try to find the target create-streaming-job command from track.
    ///
    /// Return the target command as it should be cancelled based on the input actors.
    pub fn find_cancelled_command(
        &mut self,
        actors_to_cancel: HashSet<ActorId>,
    ) -> Option<TrackingCommand> {
        let epochs = actors_to_cancel
            .into_iter()
            .map(|actor_id| self.actor_map.get(&actor_id))
            .collect_vec();
        assert!(epochs.iter().all_equal());
        // If the target command found in progress map, return and remove it. Note that the command
        // should have finished if not found.
        if let Some(Some(epoch)) = epochs.first() {
            Some(self.progress_map.remove(epoch).unwrap().1)
        } else {
            None
        }
    }

    /// Add a new create-mview DDL command to track.
    ///
    /// If the actors to track is empty, return the given command as it can be finished immediately.
    pub fn add(
        &mut self,
        command: TrackingCommand,
        version_stats: &HummockVersionStats,
    ) -> Option<TrackingCommand> {
        let actors = command.context.actors_to_track();
        if actors.is_empty() {
            // The command can be finished immediately.
            return Some(command);
        }

        let ddl_epoch = command.context.curr_epoch.value();
        for &actor in &actors {
            self.actor_map.insert(actor, ddl_epoch);
        }

        let (creating_mv_id, upstream_mv_count, upstream_total_key_count, definition) =
            if let Command::CreateStreamingJob {
                table_fragments,
                dispatchers,
                upstream_mview_actors,
                definition,
                ..
            } = &command.context.command
            {
                // Keep track of how many times each upstream MV appears.
                let mut upstream_mv_count = HashMap::new();
                for (table_id, actors) in upstream_mview_actors {
                    assert!(!actors.is_empty());
                    let dispatch_count: usize = dispatchers
                        .iter()
                        .filter(|(upstream_actor_id, _)| actors.contains(upstream_actor_id))
                        .map(|(_, v)| v.len())
                        .sum();
                    upstream_mv_count.insert(*table_id, dispatch_count / actors.len());
                }

                let upstream_total_key_count: u64 = upstream_mv_count
                    .iter()
                    .map(|(upstream_mv, count)| {
                        *count as u64
                            * version_stats
                                .table_stats
                                .get(&upstream_mv.table_id)
                                .map_or(0, |stat| stat.total_key_count as u64)
                    })
                    .sum();
                (
                    table_fragments.table_id(),
                    upstream_mv_count,
                    upstream_total_key_count,
                    definition.to_string(),
                )
            } else {
                unreachable!("Must be CreateStreamingJob.");
            };

        let progress = Progress::new(
            actors,
            creating_mv_id,
            upstream_mv_count,
            upstream_total_key_count,
            definition,
        );
        let old = self.progress_map.insert(ddl_epoch, (progress, command));
        assert!(old.is_none());
        None
    }

    /// Update the progress of `actor` according to the Pb struct.
    ///
    /// If all actors in this MV have finished, returns the command.
    pub fn update(
        &mut self,
        progress: &CreateMviewProgress,
        version_stats: &HummockVersionStats,
    ) -> Option<TrackingCommand> {
        let actor = progress.chain_actor_id;
        let Some(epoch) = self.actor_map.get(&actor).copied() else {
            panic!(
                "no tracked progress for actor {}, is it already finished?",
                actor
            );
        };

        let new_state = if progress.done {
            ChainState::Done
        } else {
            ChainState::ConsumingUpstream(progress.consumed_epoch.into(), progress.consumed_rows)
        };

        match self.progress_map.entry(epoch) {
            Entry::Occupied(mut o) => {
                let progress = &mut o.get_mut().0;

                let upstream_total_key_count: u64 = progress
                    .upstream_mv_count
                    .iter()
                    .map(|(upstream_mv, count)| {
                        *count as u64
                            * version_stats
                                .table_stats
                                .get(&upstream_mv.table_id)
                                .map_or(0, |stat| stat.total_key_count as u64)
                    })
                    .sum();

                progress.update(actor, new_state, upstream_total_key_count);

                if progress.is_done() {
                    tracing::debug!("all actors done for creating mview with epoch {}!", epoch);

                    // Clean-up the mapping from actors to DDL epoch.
                    for actor in o.get().0.actors() {
                        self.actor_map.remove(&actor);
                    }
                    Some(o.remove().1)
                } else {
                    None
                }
            }
            Entry::Vacant(_) => {
                tracing::warn!(
                    "update the progress of an non-existent creating streaming job: {progress:?}, which could be cancelled"
                );
                None
            }
        }
    }
}
