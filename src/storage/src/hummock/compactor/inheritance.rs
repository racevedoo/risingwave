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

use std::collections::{BTreeMap, BTreeSet};

use risingwave_hummock_sdk::HummockSstableObjectId;
use risingwave_pb::hummock::{
    BlockInheritance as PbBlockInheritance, ParentInfo as PbParentInfo,
    SstableInheritance as PbSstableInheritance,
};

const PARENT_INFO_MASK: u64 = 1 << 63;

pub struct ParentInfo;

impl ParentInfo {
    pub fn from_proto(proto: &PbParentInfo) -> BTreeSet<usize> {
        let mut res = BTreeSet::new();
        let mut iter = proto.info.iter();
        loop {
            let Some(u) = iter.next() else { break };

            // single
            if u & PARENT_INFO_MASK == 0 {
                res.insert(*u as usize);
                continue;
            }
            // range
            let v = iter.next().unwrap();
            for i in *u..*v {
                res.insert(i as usize);
            }
        }
        res
    }

    pub fn to_proto(info: &BTreeSet<usize>) -> PbParentInfo {
        let mut proto = PbParentInfo::default();
        for v in info {
            let Some(prev) = proto.info.last_mut() else {
                proto.info.push(*v as u64);
                continue;
            };

            if (*prev | (!PARENT_INFO_MASK)) + 1 != *v as u64 {
                proto.info.push(*v as u64);
                continue;
            }
        }
        proto
    }
}

#[derive(Debug, Clone, Default)]
pub struct BlockInheritance {
    pub parents: BTreeMap<HummockSstableObjectId, BTreeSet<usize>>,
}

impl BlockInheritance {
    pub fn from_proto(proto: &PbBlockInheritance) -> Self {
        let mut parents = BTreeMap::new();

        Self {
            parents: proto.parents.iter().map(ParentInfo::from_proto).collect(),
        }
    }

    pub fn to_proto(&self) -> PbBlockInheritance {
        PbBlockInheritance {
            parents: self.parents.iter().map(ParentInfo::to_proto).collect(),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct SstableInheritance {
    pub blocks: Vec<BlockInheritance>,
}

impl SstableInheritance {
    pub fn from_proto(proto: &PbSstableInheritance) -> Self {
        Self {
            blocks: proto
                .blocks
                .iter()
                .map(BlockInheritance::from_proto)
                .collect(),
        }
    }

    pub fn to_proto(&self) -> PbSstableInheritance {
        PbSstableInheritance {
            blocks: self.blocks.iter().map(BlockInheritance::to_proto).collect(),
        }
    }
}
