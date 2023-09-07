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

use std::fmt::{Debug, Formatter};
use std::ops::{Deref, DerefMut};
use std::sync::Arc;

use async_trait::async_trait;
use risingwave_common::array::StreamChunk;
use risingwave_common::buffer::Bitmap;
use risingwave_pb::connector_service::SinkMetadata;
use risingwave_rpc_client::ConnectorClient;

use crate::sink::writer::{LogSinkerOf, SinkWriter};
use crate::sink::{Sink, SinkCommitCoordinator, SinkWriterParam};

pub type BoxWriter<CM> = Box<dyn SinkWriter<CommitMetadata = CM> + Send + 'static>;
pub type BoxCoordinator = Box<dyn SinkCommitCoordinator + Send + 'static>;
pub type BoxSink = Box<
    dyn Sink<LogSinker = LogSinkerOf<BoxWriter<()>>, Coordinator = BoxCoordinator>
        + Send
        + Sync
        + 'static,
>;

impl Debug for BoxSink {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str("BoxSink")
    }
}

#[async_trait]
impl<CM: 'static + Send> SinkWriter for BoxWriter<CM> {
    type CommitMetadata = CM;

    async fn begin_epoch(&mut self, epoch: u64) -> crate::sink::Result<()> {
        self.deref_mut().begin_epoch(epoch).await
    }

    async fn write_batch(&mut self, chunk: StreamChunk) -> crate::sink::Result<()> {
        self.deref_mut().write_batch(chunk).await
    }

    async fn barrier(&mut self, is_checkpoint: bool) -> crate::sink::Result<CM> {
        self.deref_mut().barrier(is_checkpoint).await
    }

    async fn abort(&mut self) -> crate::sink::Result<()> {
        self.deref_mut().abort().await
    }

    async fn update_vnode_bitmap(&mut self, vnode_bitmap: Arc<Bitmap>) -> crate::sink::Result<()> {
        self.deref_mut().update_vnode_bitmap(vnode_bitmap).await
    }
}

#[async_trait]
impl SinkCommitCoordinator for BoxCoordinator {
    async fn init(&mut self) -> crate::sink::Result<()> {
        self.deref_mut().init().await
    }

    async fn commit(&mut self, epoch: u64, metadata: Vec<SinkMetadata>) -> crate::sink::Result<()> {
        self.deref_mut().commit(epoch, metadata).await
    }
}

#[async_trait]
impl Sink for BoxSink {
    type Coordinator = BoxCoordinator;
    type LogSinker = LogSinkerOf<BoxWriter<()>>;

    async fn validate(&self, client: Option<ConnectorClient>) -> crate::sink::Result<()> {
        self.deref().validate(client).await
    }

    async fn new_log_sinker(
        &self,
        writer_param: SinkWriterParam,
    ) -> crate::sink::Result<Self::LogSinker> {
        self.deref().new_log_sinker(writer_param).await
    }

    async fn new_coordinator(
        &self,
        connector_client: Option<ConnectorClient>,
    ) -> crate::sink::Result<Self::Coordinator> {
        self.deref().new_coordinator(connector_client).await
    }
}
