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

use risingwave_pb::meta::PbSystemParams;

use super::system_params_to_kv;

/// A wrapper for [`risingwave_pb::meta::SystemParams`] for 2 purposes:
/// - Avoid misuse of deprecated fields by hiding their getters.
/// - Abstract fallback logic for fields that might not be provided by meta service due to backward
///   compatibility.
#[derive(Clone, Debug, PartialEq)]
pub struct SystemParamsReader {
    prost: PbSystemParams,
}

impl From<PbSystemParams> for SystemParamsReader {
    fn from(prost: PbSystemParams) -> Self {
        Self { prost }
    }
}

impl SystemParamsReader {
    pub fn barrier_interval_ms(&self) -> u32 {
        self.prost.barrier_interval_ms.unwrap()
    }

    pub fn checkpoint_frequency(&self) -> u64 {
        self.prost.checkpoint_frequency.unwrap()
    }

    pub fn parallel_compact_size_mb(&self) -> u32 {
        self.prost.parallel_compact_size_mb.unwrap()
    }

    pub fn sstable_size_mb(&self) -> u32 {
        self.prost.sstable_size_mb.unwrap()
    }

    pub fn block_size_kb(&self) -> u32 {
        self.prost.block_size_kb.unwrap()
    }

    pub fn bloom_false_positive(&self) -> f64 {
        self.prost.bloom_false_positive.unwrap()
    }

    pub fn state_store(&self) -> &str {
        self.prost.state_store.as_ref().unwrap()
    }

    pub fn data_directory(&self) -> &str {
        self.prost.data_directory.as_ref().unwrap()
    }

    pub fn backup_storage_url(&self) -> &str {
        self.prost.backup_storage_url.as_ref().unwrap()
    }

    pub fn backup_storage_directory(&self) -> &str {
        self.prost.backup_storage_directory.as_ref().unwrap()
    }

    pub fn max_concurrent_creating_streaming_jobs(&self) -> u32 {
        self.prost.max_concurrent_creating_streaming_jobs.unwrap()
    }

    pub fn telemetry_enabled(&self) -> bool {
        self.prost.telemetry_enabled.unwrap()
    }

    pub fn to_kv(&self) -> Vec<(String, String)> {
        system_params_to_kv(&self.prost).unwrap()
    }
}
