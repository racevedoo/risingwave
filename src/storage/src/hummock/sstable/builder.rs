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

use std::cmp;
use std::collections::BTreeSet;
use std::sync::Arc;

use bytes::BytesMut;
use risingwave_hummock_sdk::key::{user_key, FullKey, MAX_KEY_LEN};
use risingwave_hummock_sdk::table_stats::{TableStats, TableStatsMap};
use risingwave_hummock_sdk::{HummockEpoch, KeyComparator, LocalSstableInfo};
use risingwave_pb::hummock::SstableInfo;

use super::utils::CompressionAlgorithm;
use super::{
    BlockBuilder, BlockBuilderOptions, BlockMeta, MonotonicDeleteEvent, SstableMeta, SstableWriter,
    DEFAULT_ENTRY_SIZE, DEFAULT_RESTART_INTERVAL, VERSION,
};
use crate::filter_key_extractor::{FilterKeyExtractorImpl, FullKeyFilterKeyExtractor};
use crate::hummock::sstable::FilterBuilder;
use crate::hummock::value::HummockValue;
use crate::hummock::{HummockResult, MemoryLimiter, Xor16FilterBuilder, DEFAULT_BLOCK_SIZE};
use crate::opts::StorageOpts;

pub const DEFAULT_SSTABLE_SIZE: usize = 4 * 1024 * 1024;
pub const DEFAULT_BLOOM_FALSE_POSITIVE: f64 = 0.001;
pub const DEFAULT_MAX_SST_SIZE: u64 = 512 * 1024 * 1024;

#[derive(Clone, Debug)]
pub struct SstableBuilderOptions {
    /// Approximate sstable capacity.
    pub capacity: usize,
    /// Approximate block capacity.
    pub block_capacity: usize,
    /// Restart point interval.
    pub restart_interval: usize,
    /// False positive probability of bloom filter.
    pub bloom_false_positive: f64,
    /// Compression algorithm.
    pub compression_algorithm: CompressionAlgorithm,
    pub max_sst_size: u64,
}

impl From<&StorageOpts> for SstableBuilderOptions {
    fn from(options: &StorageOpts) -> SstableBuilderOptions {
        let capacity: usize = (options.sstable_size_mb as usize) * (1 << 20);
        SstableBuilderOptions {
            capacity,
            block_capacity: (options.block_size_kb as usize) * (1 << 10),
            restart_interval: DEFAULT_RESTART_INTERVAL,
            bloom_false_positive: options.bloom_false_positive,
            compression_algorithm: CompressionAlgorithm::None,
            max_sst_size: options.compactor_max_sst_size,
        }
    }
}

impl Default for SstableBuilderOptions {
    fn default() -> Self {
        Self {
            capacity: DEFAULT_SSTABLE_SIZE,
            block_capacity: DEFAULT_BLOCK_SIZE,
            restart_interval: DEFAULT_RESTART_INTERVAL,
            bloom_false_positive: DEFAULT_BLOOM_FALSE_POSITIVE,
            compression_algorithm: CompressionAlgorithm::None,
            max_sst_size: DEFAULT_MAX_SST_SIZE,
        }
    }
}

pub struct SstableBuilderOutput<WO> {
    pub sst_info: LocalSstableInfo,
    pub bloom_filter_size: usize,
    pub writer_output: WO,
    pub avg_key_size: usize,
    pub avg_value_size: usize,
    pub epoch_count: usize,
}

pub struct SstableBuilder<W: SstableWriter, F: FilterBuilder> {
    /// Options.
    options: SstableBuilderOptions,
    /// Data writer.
    writer: W,
    /// Current block builder.
    block_builder: BlockBuilder,
    filter_key_extractor: Arc<FilterKeyExtractorImpl>,
    /// Block metadata vec.
    block_metas: Vec<BlockMeta>,
    /// Assume that watermark1 is 5, watermark2 is 7, watermark3 is 11, delete ranges
    /// `{ [0, wmk1) in epoch1, [wmk1, wmk2) in epoch2, [wmk2, wmk3) in epoch3 }`
    /// can be transformed into events below:
    /// `{ <0, +epoch1> <wmk1, -epoch1> <wmk1, +epoch2> <wmk2, -epoch2> <wmk2, +epoch3> <wmk3,
    /// -epoch3> }`
    /// Then we can get monotonic events (they are in order by user key) as below:
    /// `{ <0, epoch1>, <wmk1, epoch2>, <wmk2, epoch3>, <wmk3, +inf> }`
    /// which means that delete range of [0, wmk1) is epoch1, delete range of [wmk1, wmk2) if
    /// epoch2, etc. In this example, at the event key wmk1 (5), delete range changes from
    /// epoch1 to epoch2, thus the `new epoch` is epoch2. epoch2 will be used from the event
    /// key wmk1 (5) and till the next event key wmk2 (7) (not inclusive).
    /// If there is no range deletes between current event key and next event key, `new_epoch` will
    /// be `HummockEpoch::MAX`.
    monotonic_deletes: Vec<MonotonicDeleteEvent>,
    /// `table_id` of added keys.
    table_ids: BTreeSet<u32>,
    last_full_key: Vec<u8>,
    /// Buffer for encoded key and value to avoid allocation.
    raw_key: BytesMut,
    raw_value: BytesMut,
    last_table_id: Option<u32>,
    sstable_id: u64,

    /// `stale_key_count` counts range_tombstones as well.
    stale_key_count: u64,
    /// `total_key_count` counts range_tombstones as well.
    total_key_count: u64,
    /// Per table stats.
    table_stats: TableStatsMap,
    /// `last_table_stats` accumulates stats for `last_table_id` and finalizes it in `table_stats`
    /// by `finalize_last_table_stats`
    last_table_stats: TableStats,
    range_tombstone_size: usize,

    filter_builder: F,

    epoch_set: BTreeSet<u64>,

    memory_limiter: Option<Arc<MemoryLimiter>>,
}

impl<W: SstableWriter> SstableBuilder<W, Xor16FilterBuilder> {
    pub fn for_test(sstable_id: u64, writer: W, options: SstableBuilderOptions) -> Self {
        Self::new(
            sstable_id,
            writer,
            Xor16FilterBuilder::new(options.capacity / DEFAULT_ENTRY_SIZE + 1),
            options,
            Arc::new(FilterKeyExtractorImpl::FullKey(FullKeyFilterKeyExtractor)),
            None,
        )
    }
}

impl<W: SstableWriter, F: FilterBuilder> SstableBuilder<W, F> {
    pub fn new(
        sstable_id: u64,
        writer: W,
        filter_builder: F,
        options: SstableBuilderOptions,
        filter_key_extractor: Arc<FilterKeyExtractorImpl>,
        memory_limiter: Option<Arc<MemoryLimiter>>,
    ) -> Self {
        Self {
            options: options.clone(),
            writer,
            block_builder: BlockBuilder::new(BlockBuilderOptions {
                capacity: options.block_capacity,
                restart_interval: options.restart_interval,
                compression_algorithm: options.compression_algorithm,
            }),
            filter_builder,
            block_metas: Vec::with_capacity(options.capacity / options.block_capacity + 1),
            table_ids: BTreeSet::new(),
            last_table_id: None,
            raw_key: BytesMut::new(),
            raw_value: BytesMut::new(),
            last_full_key: vec![],
            monotonic_deletes: vec![],
            sstable_id,
            filter_key_extractor,
            stale_key_count: 0,
            total_key_count: 0,
            table_stats: Default::default(),
            last_table_stats: Default::default(),
            range_tombstone_size: 0,
            epoch_set: BTreeSet::default(),
            memory_limiter,
        }
    }

    pub fn add_monotonic_deletes(&mut self, events: Vec<MonotonicDeleteEvent>) {
        for event in events {
            self.add_monotonic_delete(event);
        }
    }

    /// Add kv pair to sstable.
    pub fn add_monotonic_delete(&mut self, event: MonotonicDeleteEvent) {
        let table_id = event.event_key.left_user_key.table_id.table_id();
        if self.last_table_id.is_none() || self.last_table_id.unwrap() != table_id {
            self.table_ids.insert(table_id);
        }
        if event.new_epoch == HummockEpoch::MAX
            && self.monotonic_deletes.last().map_or(true, |last| {
                last.new_epoch == HummockEpoch::MAX
                    && last.event_key.left_user_key.table_id
                        == event.event_key.left_user_key.table_id
            })
        {
            // This range would never delete any key so we can merge it with last range.
            return;
        }
        if event.new_epoch != HummockEpoch::MAX {
            self.epoch_set.insert(event.new_epoch);
        }
        self.stale_key_count += 1;
        self.total_key_count += 1;
        self.range_tombstone_size += event.encoded_size();
        self.monotonic_deletes.push(event);
    }

    pub fn last_range_tombstone_epoch(&self) -> HummockEpoch {
        self.monotonic_deletes
            .last()
            .map_or(HummockEpoch::MAX, |delete| delete.new_epoch)
    }

    pub fn last_range_tombstone(&self) -> Option<&MonotonicDeleteEvent> {
        self.monotonic_deletes.last()
    }

    /// Add kv pair to sstable.
    pub async fn add_for_test(
        &mut self,
        full_key: FullKey<&[u8]>,
        value: HummockValue<&[u8]>,
        is_new_user_key: bool,
    ) -> HummockResult<()> {
        self.add(full_key, value, is_new_user_key).await
    }

    /// Add kv pair to sstable.
    pub async fn add(
        &mut self,
        full_key: FullKey<&[u8]>,
        value: HummockValue<&[u8]>,
        is_new_user_key: bool,
    ) -> HummockResult<()> {
        const LARGE_KEY_LEN: usize = MAX_KEY_LEN >> 1;

        let mut is_new_table = false;

        let table_key_len = full_key.user_key.table_key.as_ref().len();
        if table_key_len >= LARGE_KEY_LEN {
            let table_id = full_key.user_key.table_id.table_id();
            tracing::warn!(
                "A large key (table_id={}, len={}, epoch={}) is added to block",
                table_id,
                table_key_len,
                full_key.epoch
            );
        }

        // TODO: refine me
        full_key.encode_into(&mut self.raw_key);
        value.encode(&mut self.raw_value);
        if is_new_user_key {
            let table_id = full_key.user_key.table_id.table_id();
            is_new_table = self.last_table_id.is_none() || self.last_table_id.unwrap() != table_id;
            if is_new_table {
                self.table_ids.insert(table_id);
                self.finalize_last_table_stats();
                self.last_table_id = Some(table_id);
                if !self.block_builder.is_empty() {
                    self.build_block().await?;
                }
            } else if self.block_builder.approximate_len() >= self.options.block_capacity {
                self.build_block().await?;
            }
        } else {
            self.stale_key_count += 1;
        }
        self.total_key_count += 1;
        self.last_table_stats.total_key_count += 1;

        self.epoch_set.insert(full_key.epoch);

        if is_new_table && !self.block_builder.is_empty() {
            self.build_block().await?;
        }

        // Rotate block builder if the previous one has been built.
        if self.block_builder.is_empty() {
            self.block_metas.push(BlockMeta {
                offset: self.writer.data_len() as u32,
                len: 0,
                smallest_key: full_key.encode(),
                uncompressed_size: 0,
            })
        }

        let table_id = full_key.user_key.table_id.table_id();
        let mut extract_key = user_key(&self.raw_key);
        extract_key = self.filter_key_extractor.extract(extract_key);
        // add bloom_filter check
        if !extract_key.is_empty() {
            self.filter_builder.add_key(extract_key, table_id);
        }
        self.block_builder.add(full_key, self.raw_value.as_ref());
        self.last_table_stats.total_key_size += full_key.encoded_len() as i64;
        self.last_table_stats.total_value_size += value.encoded_len() as i64;

        self.last_full_key.clear();
        self.last_full_key.extend_from_slice(&self.raw_key);

        self.raw_key.clear();
        self.raw_value.clear();
        Ok(())
    }

    /// Finish building sst.
    ///
    /// Unlike most LSM-Tree implementations, sstable meta and data are encoded separately.
    /// Both meta and data has its own object (file).
    ///
    /// # Format
    ///
    /// data:
    ///
    /// ```plain
    /// | Block 0 | ... | Block N-1 | N (4B) |
    /// ```
    pub async fn finish(mut self) -> HummockResult<SstableBuilderOutput<W::Output>> {
        let mut smallest_key = if self.block_metas.is_empty() {
            vec![]
        } else {
            self.block_metas[0].smallest_key.clone()
        };
        let mut largest_key = self.last_full_key.clone();
        self.finalize_last_table_stats();

        self.build_block().await?;
        let mut right_exclusive = false;
        let meta_offset = self.writer.data_len() as u64;

        assert!(self.monotonic_deletes.is_empty() || self.monotonic_deletes.len() > 1);

        if let Some(monotonic_delete) = self.monotonic_deletes.last() {
            assert_eq!(monotonic_delete.new_epoch, HummockEpoch::MAX);
            if monotonic_delete.event_key.is_exclude_left_key {
                if largest_key.is_empty()
                    || !KeyComparator::encoded_greater_than_unencoded(
                        user_key(&largest_key),
                        &monotonic_delete.event_key.left_user_key,
                    )
                {
                    largest_key = FullKey::from_user_key(
                        monotonic_delete.event_key.left_user_key.clone(),
                        HummockEpoch::MIN,
                    )
                    .encode();
                }
            } else if largest_key.is_empty()
                || KeyComparator::encoded_less_than_unencoded(
                    user_key(&largest_key),
                    &monotonic_delete.event_key.left_user_key,
                )
            {
                // use MAX as epoch because the last monotonic delete must be
                // `HummockEpoch::MAX`, so we can not include any version of
                // this key.
                largest_key = FullKey::from_user_key(
                    monotonic_delete.event_key.left_user_key.clone(),
                    HummockEpoch::MAX,
                )
                .encode();
                right_exclusive = true;
            }
        }
        if let Some(monotonic_delete) = self.monotonic_deletes.first() {
            assert_ne!(monotonic_delete.new_epoch, HummockEpoch::MAX);
            if smallest_key.is_empty()
                || !KeyComparator::encoded_less_than_unencoded(
                    user_key(&smallest_key),
                    &monotonic_delete.event_key.left_user_key,
                )
            {
                smallest_key = FullKey::from_user_key(
                    monotonic_delete.event_key.left_user_key.clone(),
                    HummockEpoch::MAX,
                )
                .encode();
            }
        }
        let bloom_filter = if self.options.bloom_false_positive > 0.0 {
            self.filter_builder.finish(self.memory_limiter.clone())
        } else {
            vec![]
        };

        let uncompressed_file_size = self
            .block_metas
            .iter()
            .map(|block_meta| block_meta.uncompressed_size as u64)
            .sum::<u64>();

        let mut meta = SstableMeta {
            block_metas: self.block_metas,
            bloom_filter,
            estimated_size: 0,
            key_count: self.total_key_count as u32,
            smallest_key,
            largest_key,
            version: VERSION,
            meta_offset,
            monotonic_tombstone_events: self.monotonic_deletes,
        };

        // FIXME: just workaround
        let encoded_size_u32 = u32::try_from(meta.encoded_size()).unwrap();
        let meta_offset_u32 = u32::try_from(meta_offset).unwrap();
        meta.estimated_size = encoded_size_u32.checked_add(meta_offset_u32).unwrap();

        // Expand the epoch of the whole sst by tombstone epoch
        let (tombstone_min_epoch, tombstone_max_epoch) = {
            let mut tombstone_min_epoch = u64::MAX;
            let mut tombstone_max_epoch = u64::MIN;

            for monotonic_delete in &meta.monotonic_tombstone_events {
                if monotonic_delete.new_epoch != HummockEpoch::MAX {
                    tombstone_min_epoch = cmp::min(tombstone_min_epoch, monotonic_delete.new_epoch);
                    tombstone_max_epoch = cmp::max(tombstone_max_epoch, monotonic_delete.new_epoch);
                }
            }

            (tombstone_min_epoch, tombstone_max_epoch)
        };

        let (avg_key_size, avg_value_size) = if self.table_stats.is_empty() {
            (0, 0)
        } else {
            let total_key_count: usize = self
                .table_stats
                .values()
                .map(|s| s.total_key_count as usize)
                .sum();

            if total_key_count == 0 {
                (0, 0)
            } else {
                let total_key_size: usize = self
                    .table_stats
                    .values()
                    .map(|s| s.total_key_size as usize)
                    .sum();

                let total_value_size: usize = self
                    .table_stats
                    .values()
                    .map(|s| s.total_value_size as usize)
                    .sum();

                (
                    total_key_size / total_key_count,
                    total_value_size / total_key_count,
                )
            }
        };

        let (min_epoch, max_epoch) = {
            if self.epoch_set.is_empty() {
                (u64::MAX, u64::MIN)
            } else {
                (
                    *self.epoch_set.first().unwrap(),
                    *self.epoch_set.last().unwrap(),
                )
            }
        };

        let sst_info = SstableInfo {
            object_id: self.sstable_id,
            sst_id: self.sstable_id,
            key_range: Some(risingwave_pb::hummock::KeyRange {
                left: meta.smallest_key.clone(),
                right: meta.largest_key.clone(),
                right_exclusive,
            }),
            file_size: meta.estimated_size as u64,
            table_ids: self.table_ids.into_iter().collect(),
            meta_offset: meta.meta_offset,
            stale_key_count: self.stale_key_count,
            total_key_count: self.total_key_count,
            uncompressed_file_size: uncompressed_file_size + meta.encoded_size() as u64,
            min_epoch: cmp::min(min_epoch, tombstone_min_epoch),
            max_epoch: cmp::max(max_epoch, tombstone_max_epoch),
            range_tombstone_count: meta.monotonic_tombstone_events.len() as u64,
        };
        tracing::trace!(
            "meta_size {} bloom_filter_size {}  add_key_counts {} stale_key_count {} min_epoch {} max_epoch {} epoch_count {}",
            meta.encoded_size(),
            meta.bloom_filter.len(),
            self.total_key_count,
            self.stale_key_count,
            min_epoch,
            max_epoch,
            self.epoch_set.len()
        );
        let bloom_filter_size = meta.bloom_filter.len();

        let writer_output = self.writer.finish(meta).await?;
        Ok(SstableBuilderOutput::<W::Output> {
            sst_info: LocalSstableInfo::with_stats(sst_info, self.table_stats),
            bloom_filter_size,
            writer_output,
            avg_key_size,
            avg_value_size,
            epoch_count: self.epoch_set.len(),
        })
    }

    pub fn approximate_len(&self) -> usize {
        self.writer.data_len()
            + self.block_builder.approximate_len()
            + self.filter_builder.approximate_len()
            + self.range_tombstone_size
    }

    async fn build_block(&mut self) -> HummockResult<()> {
        // Skip empty block.
        if self.block_builder.is_empty() {
            self.block_metas.pop();
            return Ok(());
        }

        let block_meta = self.block_metas.last_mut().unwrap();
        block_meta.uncompressed_size = self.block_builder.uncompressed_block_size() as u32;
        let block = self.block_builder.build();
        self.writer.write_block(block, block_meta).await?;
        self.filter_builder
            .switch_block(self.memory_limiter.clone());
        block_meta.len = self.writer.data_len() as u32 - block_meta.offset;
        self.block_builder.clear();
        Ok(())
    }

    pub fn len(&self) -> usize {
        self.total_key_count as usize
    }

    pub fn is_empty(&self) -> bool {
        self.total_key_count > 0
    }

    /// Returns true if we roughly reached capacity
    pub fn reach_capacity(&self) -> bool {
        self.approximate_len() >= self.options.capacity
    }

    pub fn reach_max_sst_size(&self) -> bool {
        self.approximate_len() as u64 >= self.options.max_sst_size
    }

    fn finalize_last_table_stats(&mut self) {
        if self.table_ids.is_empty() || self.last_table_id.is_none() {
            return;
        }
        self.table_stats.insert(
            self.last_table_id.unwrap(),
            std::mem::take(&mut self.last_table_stats),
        );
    }
}

#[cfg(test)]
pub(super) mod tests {
    use std::collections::Bound;

    use risingwave_common::catalog::TableId;
    use risingwave_hummock_sdk::key::UserKey;

    use super::*;
    use crate::assert_bytes_eq;
    use crate::hummock::iterator::test_utils::mock_sstable_store;
    use crate::hummock::sstable::xor_filter::BlockedXor16FilterBuilder;
    use crate::hummock::test_utils::{
        default_builder_opt_for_test, gen_test_sstable_impl, mock_sst_writer, test_key_of,
        test_value_of, TEST_KEYS_COUNT,
    };
    use crate::hummock::{CachePolicy, Sstable, Xor16FilterBuilder, Xor8FilterBuilder};

    #[tokio::test]
    async fn test_empty() {
        let opt = SstableBuilderOptions {
            capacity: 0,
            block_capacity: 4096,
            restart_interval: 16,
            bloom_false_positive: 0.001,
            ..Default::default()
        };

        let b = SstableBuilder::for_test(0, mock_sst_writer(&opt), opt);

        b.finish().await.unwrap();
    }

    #[tokio::test]
    async fn test_empty_with_delete_range() {
        let opt = SstableBuilderOptions {
            capacity: 0,
            block_capacity: 4096,
            restart_interval: 16,
            bloom_false_positive: 0.1,
            ..Default::default()
        };
        let table_id = TableId::default();
        let mut b = SstableBuilder::for_test(0, mock_sst_writer(&opt), opt);
        b.add_monotonic_deletes(vec![
            MonotonicDeleteEvent::new(table_id, b"abcd".to_vec(), 0),
            MonotonicDeleteEvent::new(table_id, b"eeee".to_vec(), HummockEpoch::MAX),
        ]);
        let s = b.finish().await.unwrap().sst_info;
        let key_range = s.sst_info.key_range.unwrap();
        assert_eq!(
            user_key(&key_range.left),
            UserKey::for_test(TableId::default(), b"abcd").encode()
        );
        assert_eq!(
            user_key(&key_range.right),
            UserKey::for_test(TableId::default(), b"eeee").encode()
        );
    }

    #[tokio::test]
    async fn test_basic() {
        let opt = default_builder_opt_for_test();
        let mut b = SstableBuilder::for_test(0, mock_sst_writer(&opt), opt);

        for i in 0..TEST_KEYS_COUNT {
            b.add_for_test(
                test_key_of(i).to_ref(),
                HummockValue::put(&test_value_of(i)),
                true,
            )
            .await
            .unwrap();
        }

        let output = b.finish().await.unwrap();
        let info = output.sst_info.sst_info;

        assert_bytes_eq!(
            test_key_of(0).encode(),
            info.key_range.as_ref().unwrap().left
        );
        assert_bytes_eq!(
            test_key_of(TEST_KEYS_COUNT - 1).encode(),
            info.key_range.as_ref().unwrap().right
        );
        let (data, meta) = output.writer_output;
        assert_eq!(info.file_size, meta.estimated_size as u64);
        let offset = info.meta_offset as usize;
        let meta2 = SstableMeta::decode(&mut &data[offset..]).unwrap();
        assert_eq!(meta2, meta);
    }

    async fn test_with_bloom_filter<F: FilterBuilder>(with_blooms: bool) {
        let key_count = 1000;

        let opts = SstableBuilderOptions {
            capacity: 0,
            block_capacity: 4096,
            restart_interval: 16,
            bloom_false_positive: if with_blooms { 0.01 } else { 0.0 },
            ..Default::default()
        };

        // build remote table
        let sstable_store = mock_sstable_store();
        let (table, _) = gen_test_sstable_impl::<Vec<u8>, F>(
            opts,
            0,
            (0..TEST_KEYS_COUNT).map(|i| (test_key_of(i), HummockValue::put(test_value_of(i)))),
            vec![],
            sstable_store,
            CachePolicy::NotFill,
        )
        .await;

        assert_eq!(table.has_bloom_filter(), with_blooms);
        for i in 0..key_count {
            let full_key = test_key_of(i);
            if table.has_bloom_filter() {
                let hash = Sstable::hash_for_bloom_filter(full_key.user_key.encode().as_slice(), 0);
                let key_ref = full_key.user_key.as_ref();
                assert!(
                    table.may_match_hash(
                        &(Bound::Included(key_ref), Bound::Included(key_ref)),
                        hash
                    ),
                    "failed at {}",
                    i
                );
            }
        }
    }

    #[tokio::test]
    async fn test_bloom_filter() {
        test_with_bloom_filter::<Xor16FilterBuilder>(false).await;
        test_with_bloom_filter::<Xor16FilterBuilder>(true).await;
        test_with_bloom_filter::<Xor8FilterBuilder>(true).await;
        test_with_bloom_filter::<BlockedXor16FilterBuilder>(true).await;
    }
}
