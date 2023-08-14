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

use std::pin::pin;
use std::sync::Arc;

use either::Either;
use futures::stream::select_with_strategy;
use futures::{pin_mut, stream, StreamExt, TryStreamExt};
use futures_async_stream::try_stream;
use risingwave_common::array::{Op, StreamChunk};
use risingwave_common::catalog::Schema;
use risingwave_common::row::OwnedRow;
use risingwave_common::types::Datum;
use risingwave_common::util::chunk_coalesce::DataChunkBuilder;
use risingwave_common::util::epoch::EpochPair;
use risingwave_common::{bail, row};
use risingwave_hummock_sdk::HummockReadEpoch;
use risingwave_storage::store::PrefetchOptions;
use risingwave_storage::table::batch_table::storage_table::StorageTable;
use risingwave_storage::table::get_second;
use risingwave_storage::StateStore;

use crate::common::table::state_table::StateTable;
use crate::executor::backfill::utils;
use crate::executor::backfill::utils::{
    check_all_vnode_finished, compute_bounds, construct_initial_finished_state, get_new_pos,
    iter_chunks, mapping_chunk, mapping_message, mark_chunk,
};
use crate::executor::monitor::StreamingMetrics;
use crate::executor::{
    expect_first_barrier, Barrier, BoxedExecutor, BoxedMessageStream, Executor, ExecutorInfo,
    Message, PkIndices, PkIndicesRef, StreamExecutorError, StreamExecutorResult,
};
use crate::task::{ActorId, CreateMviewProgress};

/// An implementation of the RFC: Use Backfill To Let Mv On Mv Stream Again.(https://github.com/risingwavelabs/rfcs/pull/13)
/// `BackfillExecutor` is used to create a materialized view on another materialized view.
///
/// It can only buffer chunks between two barriers instead of unbundled memory usage of
/// `RearrangedChainExecutor`.
///
/// It uses the latest epoch to read the snapshot of the upstream mv during two barriers and all the
/// `StreamChunk` of the snapshot read will forward to the downstream.
///
/// It uses `current_pos` to record the progress of the backfill (the pk of the upstream mv) and
/// `current_pos` is initiated as an empty `Row`.
///
/// All upstream messages during the two barriers interval will be buffered and decide to forward or
/// ignore based on the `current_pos` at the end of the later barrier. Once `current_pos` reaches
/// the end of the upstream mv pk, the backfill would finish.
///
/// Notice:
/// The pk we are talking about here refers to the storage primary key.
/// We rely on the scheduler to schedule the `BackfillExecutor` together with the upstream mv/table
/// in the same worker, so that we can read uncommitted data from the upstream table without
/// waiting.
pub struct BackfillExecutor<S: StateStore> {
    /// Upstream table
    upstream_table: StorageTable<S>,
    /// Upstream with the same schema with the upstream table.
    upstream: BoxedExecutor,

    /// Internal state table for persisting state of backfill state.
    state_table: Option<StateTable<S>>,

    /// The column indices need to be forwarded to the downstream from the upstream and table scan.
    output_indices: Vec<usize>,

    progress: CreateMviewProgress,

    actor_id: ActorId,

    info: ExecutorInfo,

    metrics: Arc<StreamingMetrics>,

    chunk_size: usize,
}

impl<S> BackfillExecutor<S>
where
    S: StateStore,
{
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        upstream_table: StorageTable<S>,
        upstream: BoxedExecutor,
        state_table: Option<StateTable<S>>,
        output_indices: Vec<usize>,
        progress: CreateMviewProgress,
        schema: Schema,
        pk_indices: PkIndices,
        metrics: Arc<StreamingMetrics>,
        chunk_size: usize,
    ) -> Self {
        Self {
            info: ExecutorInfo {
                schema,
                pk_indices,
                identity: "BackfillExecutor".to_owned(),
            },
            upstream_table,
            upstream,
            state_table,
            output_indices,
            actor_id: progress.actor_id(),
            progress,
            metrics,
            chunk_size,
        }
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn execute_inner(mut self) {
        // The primary key columns, in the output columns of the upstream_table scan.
        let pk_in_output_indices = self.upstream_table.pk_in_output_indices().unwrap();
        let state_len = pk_in_output_indices.len() + 2; // +1 for backfill_finished, +1 for vnode key.

        let pk_order = self.upstream_table.pk_serializer().get_order_types();

        let upstream_table_id = self.upstream_table.table_id().table_id;

        let mut upstream = self.upstream.execute();

        // Poll the upstream to get the first barrier.
        let first_barrier = expect_first_barrier(&mut upstream).await?;
        let init_epoch = first_barrier.epoch.prev;
        if let Some(state_table) = self.state_table.as_mut() {
            state_table.init_epoch(first_barrier.epoch);
        }

        let is_finished = if let Some(state_table) = self.state_table.as_mut() {
            let is_finished = check_all_vnode_finished(state_table).await?;
            if is_finished {
                assert!(!first_barrier.is_newly_added(self.actor_id));
            }
            is_finished
        } else {
            // Maintain backwards compatibility with no state table
            !first_barrier.is_newly_added(self.actor_id)
        };

        let mut builder =
            DataChunkBuilder::new(self.upstream_table.schema().data_types(), self.chunk_size);

        // If the snapshot is empty, we don't need to backfill.
        // We cannot complete progress now, as we want to persist
        // finished state to state store first.
        // As such we will wait for next barrier.
        let is_snapshot_empty: bool = {
            if is_finished {
                // It is finished, so just assign a value to avoid accessing storage table again.
                false
            } else {
                let snapshot = Self::snapshot_read(
                    &self.upstream_table,
                    init_epoch,
                    None,
                    false,
                    self.chunk_size,
                    &mut builder,
                );
                pin_mut!(snapshot);
                snapshot.try_next().await?.unwrap().is_none()
            }
        };

        // | backfill_is_finished | snapshot_empty | need_to_backfill |
        // | t                    | t/f            | f                |
        // | f                    | t              | f                |
        // | f                    | f              | t                |
        let to_backfill = !is_finished && !is_snapshot_empty;

        // Current position of the upstream_table storage primary key.
        // `None` means it starts from the beginning.
        let mut current_pos: Option<OwnedRow> = None;

        // Use these to persist state.
        // They contain the backfill position,
        // as well as the progress.
        // However, they do not contain the vnode key at index 0.
        // That is filled in when we flush the state table.
        let mut current_state: Vec<Datum> = vec![None; state_len];
        let mut old_state: Option<Vec<Datum>> = None;

        // The first barrier message should be propagated.
        yield Message::Barrier(first_barrier);

        // If no need backfill, but state was still "unfinished" we need to finish it.
        // So we just update the state + progress to meta at the next barrier to finish progress,
        // and forward other messages.
        //
        // Reason for persisting on second barrier rather than first:
        // We can't update meta with progress as finished until state_table
        // has been updated.
        // We also can't update state_table in first epoch, since state_table
        // expects to have been initialized in previous epoch.

        // The epoch used to snapshot read upstream mv.
        let mut snapshot_read_epoch = init_epoch;

        // Keep track of rows from the snapshot.
        let mut total_snapshot_processed_rows: u64 = 0;

        // Backfill Algorithm:
        //
        //   backfill_stream
        //  /               \
        // upstream       snapshot
        //
        // We construct a backfill stream with upstream as its left input and mv snapshot read
        // stream as its right input. When a chunk comes from upstream, we will buffer it.
        //
        // When a barrier comes from upstream:
        //  - Update the `snapshot_read_epoch`.
        //  - For each row of the upstream chunk buffer, forward it to downstream if its pk <=
        //    `current_pos`, otherwise ignore it.
        //  - reconstruct the whole backfill stream with upstream and new mv snapshot read stream
        //    with the `snapshot_read_epoch`.
        //
        // When a chunk comes from snapshot, we forward it to the downstream and raise
        // `current_pos`.
        //
        // When we reach the end of the snapshot read stream, it means backfill has been
        // finished.
        //
        // Once the backfill loop ends, we forward the upstream directly to the downstream.
        if to_backfill {
            let mut upstream_chunk_buffer: Vec<StreamChunk> = vec![];
            let mut pending_barrier: Option<Barrier> = None;
            'backfill_loop: loop {
                let mut cur_barrier_snapshot_processed_rows: u64 = 0;
                let mut cur_barrier_upstream_processed_rows: u64 = 0;

                // We should not buffer rows from previous epoch, else we can have duplicates.
                assert!(upstream_chunk_buffer.is_empty());

                {
                    let left_upstream = upstream.by_ref().map(Either::Left);

                    let right_snapshot = pin!(Self::snapshot_read(
                        &self.upstream_table,
                        snapshot_read_epoch,
                        current_pos.clone(),
                        true,
                        self.chunk_size,
                        &mut builder
                    )
                    .map(Either::Right),);

                    // Prefer to select upstream, so we can stop snapshot stream as soon as the
                    // barrier comes.
                    let backfill_stream =
                        select_with_strategy(left_upstream, right_snapshot, |_: &mut ()| {
                            stream::PollNext::Left
                        });

                    #[for_await]
                    for either in backfill_stream {
                        match either {
                            // Upstream
                            Either::Left(msg) => {
                                match msg? {
                                    Message::Barrier(barrier) => {
                                        // We have to process barrier outside of the loop.
                                        // This is because the backfill stream holds a mutable
                                        // reference to our chunk builder.
                                        // We want to create another mutable reference
                                        // to flush remaining chunks from the chunk builder
                                        // on barrier.
                                        // Hence we break here and process it after this block.
                                        pending_barrier = Some(barrier);
                                        break;
                                    }
                                    Message::Chunk(chunk) => {
                                        // Buffer the upstream chunk.
                                        upstream_chunk_buffer.push(chunk.compact());
                                    }
                                    Message::Watermark(_) => {
                                        // Ignore watermark during backfill.
                                    }
                                }
                            }
                            // Snapshot read
                            Either::Right(msg) => {
                                match msg? {
                                    None => {
                                        // End of the snapshot read stream.
                                        // We should not mark the chunk anymore,
                                        // otherwise, we will ignore some rows
                                        // in the buffer. Here we choose to never mark the chunk.
                                        // Consume with the renaming stream buffer chunk without
                                        // mark.
                                        for chunk in upstream_chunk_buffer.drain(..) {
                                            let chunk_cardinality = chunk.cardinality() as u64;
                                            cur_barrier_upstream_processed_rows +=
                                                chunk_cardinality;
                                            yield Message::Chunk(mapping_chunk(
                                                chunk,
                                                &self.output_indices,
                                            ));
                                        }

                                        break 'backfill_loop;
                                    }
                                    Some(chunk) => {
                                        // Raise the current position.
                                        // As snapshot read streams are ordered by pk, so we can
                                        // just use the last row to update `current_pos`.
                                        current_pos =
                                            Some(get_new_pos(&chunk, &pk_in_output_indices));

                                        let chunk_cardinality = chunk.cardinality() as u64;
                                        cur_barrier_snapshot_processed_rows += chunk_cardinality;
                                        total_snapshot_processed_rows += chunk_cardinality;
                                        yield Message::Chunk(mapping_chunk(
                                            chunk,
                                            &self.output_indices,
                                        ));
                                    }
                                }
                            }
                        }
                    }
                }
                // When we break out of inner backfill_stream loop, it means we have a barrier.
                // If there are no updates and there are no snapshots left,
                // we already finished backfill and should have exited the outer backfill loop.
                let barrier = match pending_barrier.take() {
                    Some(barrier) => barrier,
                    None => bail!("BUG: current_backfill loop exited without a barrier"),
                };

                // Process barrier:
                // - consume snapshot rows left in builder
                // - consume upstream buffer chunk
                // - switch snapshot

                // Consume snapshot rows left in builder
                let chunk = builder.build_data_chunk();
                let chunk_cardinality = chunk.cardinality() as u64;
                if chunk_cardinality > 0 {
                    let ops = vec![Op::Insert; chunk.capacity()];
                    let chunk = StreamChunk::from_parts(ops, chunk);
                    current_pos = Some(get_new_pos(&chunk, &pk_in_output_indices));

                    cur_barrier_snapshot_processed_rows += chunk_cardinality;
                    total_snapshot_processed_rows += chunk_cardinality;
                    yield Message::Chunk(mapping_chunk(chunk, &self.output_indices));
                }

                // Consume upstream buffer chunk
                // If no current_pos, means we did not process any snapshot
                // yet. In that case
                // we can just ignore the upstream buffer chunk, but still need to clean it.
                if let Some(current_pos) = &current_pos {
                    for chunk in upstream_chunk_buffer.drain(..) {
                        cur_barrier_upstream_processed_rows += chunk.cardinality() as u64;
                        yield Message::Chunk(mapping_chunk(
                            mark_chunk(chunk, current_pos, &pk_in_output_indices, pk_order),
                            &self.output_indices,
                        ));
                    }
                } else {
                    upstream_chunk_buffer.clear()
                }

                self.metrics
                    .backfill_snapshot_read_row_count
                    .with_label_values(&[
                        upstream_table_id.to_string().as_str(),
                        self.actor_id.to_string().as_str(),
                    ])
                    .inc_by(cur_barrier_snapshot_processed_rows);

                self.metrics
                    .backfill_upstream_output_row_count
                    .with_label_values(&[
                        upstream_table_id.to_string().as_str(),
                        self.actor_id.to_string().as_str(),
                    ])
                    .inc_by(cur_barrier_upstream_processed_rows);

                // Update snapshot read epoch.
                snapshot_read_epoch = barrier.epoch.prev;

                self.progress.update(
                    barrier.epoch.curr,
                    snapshot_read_epoch,
                    total_snapshot_processed_rows,
                );

                // Persist state on barrier
                Self::persist_state(
                    barrier.epoch,
                    &mut self.state_table,
                    false,
                    &current_pos,
                    &mut old_state,
                    &mut current_state,
                )
                .await?;

                yield Message::Barrier(barrier);

                // We will switch snapshot at the start of the next iteration of the backfill loop.
            }
        }

        tracing::trace!(
            actor = self.actor_id,
            "Backfill has already finished and forward messages directly to the downstream"
        );

        // Wait for first barrier to come after backfill is finished.
        // So we can update our progress + persist the status.
        while let Some(Ok(msg)) = upstream.next().await {
            if let Some(msg) = mapping_message(msg, &self.output_indices) {
                // If not finished then we need to update state, otherwise no need.
                if let Message::Barrier(barrier) = &msg && !is_finished {
                    // If snapshot was empty, we do not need to backfill,
                    // but we still need to persist the finished state.
                    // We currently persist it on the second barrier here rather than first.
                    // This is because we can't update state table in first epoch,
                    // since it expects to have been initialized in previous epoch
                    // (there's no epoch before the first epoch).
                    if is_snapshot_empty {
                        current_pos =
                            Some(construct_initial_finished_state(pk_in_output_indices.len()))
                    }

                    // We will update current_pos at least once,
                    // since snapshot read has to be non-empty,
                    // Or snapshot was empty and we construct a placeholder state.
                    debug_assert_ne!(current_pos, None);

                    Self::persist_state(
                        barrier.epoch,
                        &mut self.state_table,
                        true,
                        &current_pos,
                        &mut old_state,
                        &mut current_state,
                    )
                    .await?;
                    self.progress.finish(barrier.epoch.curr);
                    yield msg;
                    break;
                }
                yield msg;
            }
        }

        // After progress finished + state persisted,
        // we can forward messages directly to the downstream,
        // as backfill is finished.
        #[for_await]
        for msg in upstream {
            if let Some(msg) = mapping_message(msg?, &self.output_indices) {
                if let Some(state_table) = self.state_table.as_mut() && let Message::Barrier(barrier) = &msg {
                        state_table.commit_no_data_expected(barrier.epoch);
                    }
                yield msg;
            }
        }
    }

    #[try_stream(ok = Option<StreamChunk>, error = StreamExecutorError)]
    async fn snapshot_read<'a>(
        upstream_table: &'a StorageTable<S>,
        epoch: u64,
        current_pos: Option<OwnedRow>,
        ordered: bool,
        chunk_size: usize,
        builder: &'a mut DataChunkBuilder,
    ) {
        let range_bounds = compute_bounds(upstream_table.pk_indices(), current_pos);
        let range_bounds = match range_bounds {
            None => {
                yield None;
                return Ok(());
            }
            Some(range_bounds) => range_bounds,
        };

        // We use uncommitted read here, because we have already scheduled the `BackfillExecutor`
        // together with the upstream mv.
        let iter = upstream_table
            .batch_iter_with_pk_bounds(
                HummockReadEpoch::NoWait(epoch),
                row::empty(),
                range_bounds,
                ordered,
                PrefetchOptions::new_for_exhaust_iter(),
            )
            .await?
            .map(get_second);

        pin_mut!(iter);

        #[for_await]
        for chunk in iter_chunks(iter, chunk_size, builder) {
            yield chunk?;
        }
    }

    async fn persist_state(
        epoch: EpochPair,
        table: &mut Option<StateTable<S>>,
        is_finished: bool,
        current_pos: &Option<OwnedRow>,
        old_state: &mut Option<Vec<Datum>>,
        current_state: &mut [Datum],
    ) -> StreamExecutorResult<()> {
        // Backwards compatibility with no state table in backfill.
        let Some(table) = table else {
            return Ok(())
        };
        utils::persist_state(
            epoch,
            table,
            is_finished,
            current_pos,
            old_state,
            current_state,
        )
        .await
    }
}

impl<S> Executor for BackfillExecutor<S>
where
    S: StateStore,
{
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        self.execute_inner().boxed()
    }

    fn schema(&self) -> &Schema {
        &self.info.schema
    }

    fn pk_indices(&self) -> PkIndicesRef<'_> {
        &self.info.pk_indices
    }

    fn identity(&self) -> &str {
        &self.info.identity
    }
}
