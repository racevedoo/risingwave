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

use std::sync::Arc;

use futures_async_stream::try_stream;
use itertools::Itertools;
use risingwave_common::catalog::{Field, Schema};

use crate::executor::error::StreamExecutorError;
use crate::executor::{ExecutorInfo, Message, MessageStream, Mutation};

/// Streams wrapped by `schema_check` will check the passing stream chunk against the expected
/// schema.
#[try_stream(ok = Message, error = StreamExecutorError)]
pub async fn schema_check(mut info: Arc<ExecutorInfo>, input: impl MessageStream) {
    #[for_await]
    for message in input {
        let message = message?;

        match &message {
            Message::Chunk(chunk) => risingwave_common::util::schema_check::schema_check(
                info.schema.fields().iter().map(|f| &f.data_type),
                chunk.columns(),
            ),
            Message::Watermark(watermark) => {
                let expected = info.schema.fields()[watermark.col_idx].data_type();
                let found = &watermark.data_type;
                if &expected != found {
                    Err(format!(
                        "watermark type mismatched: expected {expected}, found {found}"
                    ))
                } else {
                    Ok(())
                }
            }
            Message::Barrier(barrier) => {
                if let Some(Mutation::Update {
                    source: Some(source),
                    ..
                }) = barrier.mutation.as_deref()
                {
                    info = Arc::new(ExecutorInfo {
                        schema: Schema {
                            fields: source
                                .columns
                                .iter()
                                .map(|c| Field::from(c.column_desc.as_ref().unwrap()))
                                .collect_vec(),
                        },
                        pk_indices: info.pk_indices.clone(),
                        identity: info.identity.clone(),
                    })
                }
                Ok(())
            }
        }
        .unwrap_or_else(|e| panic!("schema check failed on {}: {}", info.identity, e));

        yield message;
    }
}

#[cfg(test)]
mod tests {
    use assert_matches::assert_matches;
    use futures::{pin_mut, StreamExt};
    use risingwave_common::array::stream_chunk::StreamChunkTestExt;
    use risingwave_common::array::StreamChunk;
    use risingwave_common::catalog::{Field, Schema};
    use risingwave_common::types::DataType;

    use super::*;
    use crate::executor::test_utils::MockSource;
    use crate::executor::Executor;

    #[tokio::test]
    async fn test_schema_ok() {
        let schema = Schema {
            fields: vec![
                Field::unnamed(DataType::Int64),
                Field::unnamed(DataType::Float64),
            ],
        };

        let (mut tx, source) = MockSource::channel(schema, vec![1]);
        tx.push_chunk(StreamChunk::from_pretty(
            "   I     F
            + 100 200.0
            +  10  14.0
            +   4 300.0",
        ));
        tx.push_barrier(1, false);

        let checked = schema_check(source.info().into(), source.boxed().execute());
        pin_mut!(checked);

        assert_matches!(checked.next().await.unwrap().unwrap(), Message::Chunk(_));
        assert_matches!(checked.next().await.unwrap().unwrap(), Message::Barrier(_));
    }

    #[should_panic]
    #[tokio::test]
    async fn test_schema_bad() {
        let schema = Schema {
            fields: vec![
                Field::unnamed(DataType::Int64),
                Field::unnamed(DataType::Float64),
            ],
        };

        let (mut tx, source) = MockSource::channel(schema, vec![1]);
        tx.push_chunk(StreamChunk::from_pretty(
            "   I   I
            + 100 200
            +  10  14
            +   4 300",
        ));
        tx.push_barrier(1, false);

        let checked = schema_check(source.info().into(), source.boxed().execute());
        pin_mut!(checked);
        checked.next().await.unwrap().unwrap();
    }
}
