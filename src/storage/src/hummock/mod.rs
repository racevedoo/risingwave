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

//! Hummock is the state store of the streaming system.

use std::ops::{Bound, Deref};
use std::sync::atomic::AtomicU64;
use std::sync::Arc;

use arc_swap::ArcSwap;
use bytes::Bytes;
use risingwave_hummock_sdk::key::{FullKey, TableKey, UserKeyRangeRef};
use risingwave_hummock_sdk::{HummockEpoch, *};
#[cfg(any(test, feature = "test"))]
use risingwave_pb::hummock::HummockVersion;
use risingwave_pb::hummock::{version_update_payload, SstableInfo};
use risingwave_rpc_client::HummockMetaClient;
use tokio::sync::mpsc::{unbounded_channel, UnboundedSender};
use tracing::log::error;

mod block_cache;
pub use block_cache::*;

use crate::hummock::store::state_store::LocalHummockStorage;
use crate::opts::StorageOpts;

mod file_cache;
pub use file_cache::*;

pub mod sstable;
pub use sstable::*;

pub mod compactor;
pub mod conflict_detector;
mod error;
pub mod hummock_meta_client;
pub mod iterator;
pub mod shared_buffer;
pub mod sstable_store;
mod state_store;
#[cfg(any(test, feature = "test"))]
pub mod test_utils;
pub mod utils;
pub use utils::MemoryLimiter;
pub mod backup_reader;
pub mod event_handler;
pub mod local_version;
pub mod observer_manager;
pub mod store;
pub mod vacuum;
mod validator;
pub mod value;
pub mod write_limiter;

pub use error::*;
pub use risingwave_common::cache::{CacheableEntry, LookupResult, LruCache};
use risingwave_common_service::observer_manager::{NotificationClient, ObserverManager};
pub use validator::*;
use value::*;

use self::event_handler::ReadVersionMappingType;
use self::iterator::HummockIterator;
pub use self::sstable_store::*;
use super::monitor::HummockStateStoreMetrics;
use crate::filter_key_extractor::{FilterKeyExtractorManager, FilterKeyExtractorManagerRef};
use crate::hummock::backup_reader::{BackupReader, BackupReaderRef};
use crate::hummock::compactor::CompactorContext;
use crate::hummock::event_handler::hummock_event_handler::BufferTracker;
use crate::hummock::event_handler::{HummockEvent, HummockEventHandler};
use crate::hummock::local_version::pinned_version::{start_pinned_version_worker, PinnedVersion};
use crate::hummock::observer_manager::HummockObserverNode;
use crate::hummock::store::memtable::ImmutableMemtable;
use crate::hummock::store::version::HummockVersionReader;
use crate::hummock::write_limiter::{WriteLimiter, WriteLimiterRef};
use crate::monitor::{CompactorMetrics, StoreLocalStatistic};
use crate::store::{NewLocalOptions, ReadOptions};

struct HummockStorageShutdownGuard {
    shutdown_sender: UnboundedSender<HummockEvent>,
}

impl Drop for HummockStorageShutdownGuard {
    fn drop(&mut self) {
        let _ = self
            .shutdown_sender
            .send(HummockEvent::Shutdown)
            .inspect_err(|e| error!("unable to send shutdown: {:?}", e));
    }
}

/// Hummock is the state store backend.
#[derive(Clone)]
pub struct HummockStorage {
    hummock_event_sender: UnboundedSender<HummockEvent>,

    context: Arc<CompactorContext>,

    buffer_tracker: BufferTracker,

    version_update_notifier_tx: Arc<tokio::sync::watch::Sender<HummockEpoch>>,

    seal_epoch: Arc<AtomicU64>,

    pinned_version: Arc<ArcSwap<PinnedVersion>>,

    hummock_version_reader: HummockVersionReader,

    _shutdown_guard: Arc<HummockStorageShutdownGuard>,

    read_version_mapping: Arc<ReadVersionMappingType>,

    backup_reader: BackupReaderRef,

    /// current_epoch < min_current_epoch cannot be read.
    min_current_epoch: Arc<AtomicU64>,

    write_limiter: WriteLimiterRef,
}

impl HummockStorage {
    /// Creates a [`HummockStorage`].
    #[allow(clippy::too_many_arguments)]
    pub async fn new(
        options: Arc<StorageOpts>,
        sstable_store: SstableStoreRef,
        hummock_meta_client: Arc<dyn HummockMetaClient>,
        notification_client: impl NotificationClient,
        filter_key_extractor_manager: Arc<FilterKeyExtractorManager>,
        state_store_metrics: Arc<HummockStateStoreMetrics>,
        compactor_metrics: Arc<CompactorMetrics>,
    ) -> HummockResult<Self> {
        let sstable_object_id_manager = Arc::new(SstableObjectIdManager::new(
            hummock_meta_client.clone(),
            options.sstable_id_remote_fetch_number,
        ));
        let backup_reader = BackupReader::new(
            &options.backup_storage_url,
            &options.backup_storage_directory,
        )
        .await
        .map_err(HummockError::read_backup_error)?;
        let write_limiter = Arc::new(WriteLimiter::default());
        let (event_tx, mut event_rx) = unbounded_channel();

        let observer_manager = ObserverManager::new(
            notification_client,
            HummockObserverNode::new(
                filter_key_extractor_manager.clone(),
                backup_reader.clone(),
                event_tx.clone(),
                write_limiter.clone(),
            ),
        )
        .await;
        observer_manager.start().await;

        let hummock_version = match event_rx.recv().await {
            Some(HummockEvent::VersionUpdate(version_update_payload::Payload::PinnedVersion(version))) => version,
            _ => unreachable!("the hummock observer manager is the first one to take the event tx. Should be full hummock version")
        };

        let (pin_version_tx, pin_version_rx) = unbounded_channel();
        let pinned_version = PinnedVersion::new(hummock_version, pin_version_tx);
        tokio::spawn(start_pinned_version_worker(
            pin_version_rx,
            hummock_meta_client.clone(),
        ));

        let compactor_context = Arc::new(CompactorContext::new_local_compact_context(
            options.clone(),
            sstable_store.clone(),
            hummock_meta_client.clone(),
            compactor_metrics.clone(),
            sstable_object_id_manager.clone(),
            filter_key_extractor_manager.clone(),
        ));

        let seal_epoch = Arc::new(AtomicU64::new(pinned_version.max_committed_epoch()));
        let min_current_epoch = Arc::new(AtomicU64::new(pinned_version.max_committed_epoch()));
        let hummock_event_handler = HummockEventHandler::new(
            event_tx.clone(),
            event_rx,
            pinned_version,
            compactor_context.clone(),
            state_store_metrics.clone(),
            options
                .data_file_cache_refill_levels
                .iter()
                .copied()
                .collect(),
        );

        let instance = Self {
            context: compactor_context,
            buffer_tracker: hummock_event_handler.buffer_tracker().clone(),
            version_update_notifier_tx: hummock_event_handler.version_update_notifier_tx(),
            seal_epoch,
            hummock_event_sender: event_tx.clone(),
            pinned_version: hummock_event_handler.pinned_version(),
            hummock_version_reader: HummockVersionReader::new(
                sstable_store,
                state_store_metrics.clone(),
            ),
            _shutdown_guard: Arc::new(HummockStorageShutdownGuard {
                shutdown_sender: event_tx,
            }),
            read_version_mapping: hummock_event_handler.read_version_mapping(),
            backup_reader,
            min_current_epoch,
            write_limiter,
        };

        tokio::spawn(hummock_event_handler.start_hummock_event_handler_worker());

        Ok(instance)
    }

    async fn new_local_inner(&self, option: NewLocalOptions) -> LocalHummockStorage {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.hummock_event_sender
            .send(HummockEvent::RegisterReadVersion {
                table_id: option.table_id,
                new_read_version_sender: tx,
                is_replicated: option.is_replicated,
            })
            .unwrap();

        let (basic_read_version, instance_guard) = rx.await.unwrap();
        LocalHummockStorage::new(
            instance_guard,
            basic_read_version,
            self.hummock_version_reader.clone(),
            self.hummock_event_sender.clone(),
            self.buffer_tracker.get_memory_limiter().clone(),
            self.write_limiter.clone(),
            option,
        )
    }

    pub fn sstable_store(&self) -> SstableStoreRef {
        self.context.sstable_store.clone()
    }

    pub fn sstable_object_id_manager(&self) -> &SstableObjectIdManagerRef {
        &self.context.sstable_object_id_manager
    }

    pub fn filter_key_extractor_manager(&self) -> &FilterKeyExtractorManagerRef {
        &self.context.filter_key_extractor_manager
    }

    pub fn get_memory_limiter(&self) -> Arc<MemoryLimiter> {
        self.buffer_tracker.get_memory_limiter().clone()
    }

    pub fn get_pinned_version(&self) -> PinnedVersion {
        self.pinned_version.load().deref().deref().clone()
    }

    pub fn backup_reader(&self) -> BackupReaderRef {
        self.backup_reader.clone()
    }
}

#[cfg(any(test, feature = "test"))]
impl HummockStorage {
    /// Used in the compaction test tool
    pub async fn update_version_and_wait(&self, version: HummockVersion) {
        use tokio::task::yield_now;
        let version_id = version.id;
        self.hummock_event_sender
            .send(HummockEvent::VersionUpdate(
                version_update_payload::Payload::PinnedVersion(version),
            ))
            .unwrap();
        loop {
            if self.pinned_version.load().id() >= version_id {
                break;
            }

            yield_now().await
        }
    }

    pub async fn wait_version(&self, version: HummockVersion) {
        use tokio::task::yield_now;
        loop {
            if self.pinned_version.load().id() >= version.id {
                break;
            }

            yield_now().await
        }
    }

    pub fn get_shared_buffer_size(&self) -> usize {
        self.buffer_tracker.get_buffer_size()
    }

    pub async fn try_wait_epoch_for_test(&self, wait_epoch: u64) {
        let mut rx = self.version_update_notifier_tx.subscribe();
        while *(rx.borrow_and_update()) < wait_epoch {
            rx.changed().await.unwrap();
        }
    }

    /// Creates a [`HummockStorage`] with default stats. Should only be used by tests.
    pub async fn for_test(
        options: Arc<StorageOpts>,
        sstable_store: SstableStoreRef,
        hummock_meta_client: Arc<dyn HummockMetaClient>,
        notification_client: impl NotificationClient,
    ) -> HummockResult<Self> {
        Self::new(
            options,
            sstable_store,
            hummock_meta_client,
            notification_client,
            Arc::new(FilterKeyExtractorManager::default()),
            Arc::new(HummockStateStoreMetrics::unused()),
            Arc::new(CompactorMetrics::unused()),
        )
        .await
    }

    pub fn storage_opts(&self) -> &Arc<StorageOpts> {
        &self.context.storage_opts
    }

    pub fn version_reader(&self) -> &HummockVersionReader {
        &self.hummock_version_reader
    }

    #[cfg(any(test, feature = "test"))]
    pub async fn wait_version_update(&self, old_id: u64) -> u64 {
        use tokio::task::yield_now;
        loop {
            let cur_id = self.pinned_version.load().id();
            if cur_id > old_id {
                return cur_id;
            }
            yield_now().await;
        }
    }
}

pub async fn get_from_sstable_info(
    sstable_store_ref: SstableStoreRef,
    sstable_info: &SstableInfo,
    full_key: FullKey<&[u8]>,
    read_options: &ReadOptions,
    dist_key_hash: Option<u64>,
    local_stats: &mut StoreLocalStatistic,
) -> HummockResult<Option<(HummockValue<Bytes>, HummockEpoch)>> {
    let sstable = sstable_store_ref.sstable(sstable_info, local_stats).await?;

    // Bloom filter key is the distribution key, which is no need to be the prefix of pk, and do not
    // contain `TablePrefix` and `VnodePrefix`.
    if let Some(hash) = dist_key_hash
        && !hit_sstable_bloom_filter(sstable.value(), &(Bound::Included(full_key.user_key), Bound::Included(full_key.user_key)), hash, local_stats)
    {
        if !read_options.ignore_range_tombstone {
            let delete_epoch = get_min_delete_range_epoch_from_sstable(
                sstable.value().as_ref(),
                full_key.user_key,
            );
            if delete_epoch <= full_key.epoch {
                return Ok(Some((HummockValue::Delete, delete_epoch)));
            }
        }

        return Ok(None);
    }

    // TODO: now SstableIterator does not use prefetch through SstableIteratorReadOptions, so we
    // use default before refinement.
    let mut iter = SstableIterator::create(
        sstable,
        sstable_store_ref.clone(),
        Arc::new(SstableIteratorReadOptions::from_read_options(read_options)),
    );
    iter.seek(full_key).await?;
    // Iterator has sought passed the borders.
    if !iter.is_valid() {
        if !read_options.ignore_range_tombstone {
            let delete_epoch = get_min_delete_range_epoch_from_sstable(
                iter.sst().value().as_ref(),
                full_key.user_key,
            );
            if delete_epoch <= full_key.epoch {
                return Ok(Some((HummockValue::Delete, delete_epoch)));
            }
        }

        return Ok(None);
    }

    // Iterator gets us the key, we tell if it's the key we want
    // or key next to it.
    let value = if iter.key().user_key == full_key.user_key {
        Some((iter.value().to_bytes(), iter.key().epoch))
    } else if !read_options.ignore_range_tombstone {
        let delete_epoch =
            get_min_delete_range_epoch_from_sstable(iter.sst().value().as_ref(), full_key.user_key);
        if delete_epoch <= full_key.epoch {
            Some((HummockValue::Delete, delete_epoch))
        } else {
            None
        }
    } else {
        None
    };

    iter.collect_local_statistic(local_stats);

    Ok(value)
}

pub fn hit_sstable_bloom_filter(
    sstable_info_ref: &Sstable,
    user_key_range: &UserKeyRangeRef<'_>,
    prefix_hash: u64,
    local_stats: &mut StoreLocalStatistic,
) -> bool {
    local_stats.bloom_filter_check_counts += 1;
    let may_exist = sstable_info_ref.may_match_hash(user_key_range, prefix_hash);
    if !may_exist {
        local_stats.bloom_filter_true_negative_counts += 1;
    }
    may_exist
}

/// Get `user_value` from `ImmutableMemtable`
pub fn get_from_batch(
    imm: &ImmutableMemtable,
    table_key: TableKey<&[u8]>,
    read_epoch: HummockEpoch,
    read_options: &ReadOptions,
    local_stats: &mut StoreLocalStatistic,
) -> Option<(HummockValue<Bytes>, HummockEpoch)> {
    imm.get(table_key, read_epoch, read_options).map(|v| {
        local_stats.get_shared_buffer_hit_counts += 1;
        v
    })
}
