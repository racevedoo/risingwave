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

use anyhow::anyhow;
use risingwave_common::array::StreamChunk;
use risingwave_common::buffer::Bitmap;
use risingwave_pb::connector_service::SinkMetadata;
use risingwave_rpc_client::{CoordinatorStreamHandle, SinkCoordinationRpcClient};
use tracing::warn;

use crate::sink::{Result, SinkError, SinkParam, SinkWriter};

pub struct CoordinatedSinkWriter<W: SinkWriter<CommitMetadata = Option<SinkMetadata>>> {
    epoch: u64,
    coordinator_stream_handle: CoordinatorStreamHandle,
    inner: W,
}

impl<W: SinkWriter<CommitMetadata = Option<SinkMetadata>>> CoordinatedSinkWriter<W> {
    pub async fn new(
        client: SinkCoordinationRpcClient,
        param: SinkParam,
        vnode_bitmap: Bitmap,
        inner: W,
    ) -> Result<Self> {
        Ok(Self {
            epoch: 0,
            coordinator_stream_handle: CoordinatorStreamHandle::new(
                client,
                param.to_proto(),
                vnode_bitmap,
            )
            .await?,
            inner,
        })
    }
}

#[async_trait::async_trait]
impl<W: SinkWriter<CommitMetadata = Option<SinkMetadata>>> SinkWriter for CoordinatedSinkWriter<W> {
    async fn begin_epoch(&mut self, epoch: u64) -> Result<()> {
        self.epoch = epoch;
        self.inner.begin_epoch(epoch).await
    }

    async fn write_batch(&mut self, chunk: StreamChunk) -> Result<()> {
        self.inner.write_batch(chunk).await
    }

    async fn barrier(&mut self, is_checkpoint: bool) -> Result<Self::CommitMetadata> {
        let metadata = self.inner.barrier(is_checkpoint).await?;
        if is_checkpoint {
            let metadata = metadata.ok_or_else(|| {
                SinkError::Coordinator(anyhow!("should get metadata on checkpoint barrier"))
            })?;
            // TODO: add metrics to measure time to commit
            self.coordinator_stream_handle
                .commit(self.epoch, metadata)
                .await?;
            Ok(())
        } else {
            if metadata.is_some() {
                warn!("get metadata on non-checkpoint barrier");
            }
            Ok(())
        }
    }

    async fn abort(&mut self) -> Result<()> {
        self.inner.abort().await
    }

    async fn update_vnode_bitmap(&mut self, vnode_bitmap: Bitmap) -> Result<()> {
        self.inner.update_vnode_bitmap(vnode_bitmap).await
    }
}
