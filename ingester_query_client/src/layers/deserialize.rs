//! GRPC-based client.
//!
//! This is THE main client.

use std::{
    collections::{HashMap, HashSet},
    fmt::Debug,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use async_trait::async_trait;
use data_types::{TimestampMinMax, TransitionPartitionId};
use futures::{
    stream::{BoxStream, Fuse},
    Stream, StreamExt, TryStreamExt,
};
use ingester_query_grpc::{
    arrow_serde::{bytes_to_schema, BatchEncoder},
    influxdata::iox::ingester::v2 as proto,
};
use snafu::{whatever, OptionExt, ResultExt, Snafu};
use uuid::Uuid;

use crate::{
    error::DynError,
    interface::{ResponseMetadata, ResponsePartition, ResponsePayload},
    layer::{Layer, QueryResponse},
};

#[derive(Debug, Snafu)]
#[allow(missing_docs)]
pub enum Error {
    #[snafu(
        whatever,
        display(
            "error deserializing gRPC response: {}{}",
            message,
            source.as_ref().map(|e| format!(": {e}")).unwrap_or_default(),
        ),
    )]
    Deserialization {
        message: String,

        #[snafu(source(from(Box<dyn std::error::Error + Send + Sync>, Some)))]
        source: Option<Box<dyn std::error::Error + Send + Sync>>,
    },

    Inner {
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

/// [`Layer`] that deserializes gRPC response data.
#[derive(Debug)]
pub struct DeserializeLayer<L>
where
    L: Layer<ResponsePayload = proto::QueryResponse>,
{
    /// Inner layer.
    inner: L,
}

impl<L> DeserializeLayer<L>
where
    L: Layer<ResponsePayload = proto::QueryResponse>,
{
    /// Create new layer.
    pub fn new(inner: L) -> Self {
        Self { inner }
    }

    /// Get inner layer.
    pub fn inner(&self) -> &L {
        &self.inner
    }
}

/// Read [`proto::IngesterQueryResponseMetadata`] from response stream.
async fn read_metadata_msg(
    stream: &mut BoxStream<'static, Result<proto::QueryResponse, DynError>>,
) -> Result<proto::IngesterQueryResponseMetadata, Error> {
    let msg = stream
        .try_next()
        .await
        .map_err(|e| Error::Inner {
            source: Box::new(e),
        })?
        .whatever_context("Stream is empty but needs at least a metadata message")?;

    let proto::QueryResponse { msg } = msg;
    let msg = msg.whatever_context("msg must be set")?;

    match msg {
        proto::query_response::Msg::Metadata(md) => Ok(md),
        proto::query_response::Msg::Payload(_) => {
            whatever!("first message must be a metadata message but is a payload message")
        }
    }
}

/// Decodes metadata protobuf message.
fn decode_metadata_msg(
    msg: proto::IngesterQueryResponseMetadata,
    payload_stream: BoxStream<'static, Result<proto::QueryResponse, DynError>>,
) -> Result<QueryResponse<ResponseMetadata, ResponsePayload>, Error> {
    let proto::IngesterQueryResponseMetadata {
        ingester_uuid,
        persist_counter,
        table_schema,
        partitions,
    } = msg;

    let ingester_uuid = Uuid::parse_str(&ingester_uuid).whatever_context("parse UUID")?;
    let table_schema =
        Arc::new(bytes_to_schema(&table_schema).whatever_context("bytes to schema")?);
    let mut partition_projections = HashMap::with_capacity(partitions.len());
    let partitions = partitions
        .into_iter()
        .map(|p| {
            let proto::ingester_query_response_metadata::Partition {
                id,
                t_min,
                t_max,
                projection,
            } = p;

            let id = id.whatever_context("partition needs id")?;
            let id = TransitionPartitionId::try_from(id).whatever_context("decode partition ID")?;

            if t_min > t_max {
                whatever!("t_min ({t_min}) > t_max ({t_max})");
            }
            let t_min_max = TimestampMinMax::new(t_min, t_max);

            let projection_set = projection.iter().cloned().collect::<HashSet<_>>();
            if projection_set.len() != projection.len() {
                whatever!("partition projection contains duplicates");
            }
            let existed = partition_projections
                .insert(id.clone(), projection_set)
                .is_some();
            if existed {
                whatever!("duplicate partition");
            }

            let projection = projection
                .into_iter()
                .map(|idx| idx as usize)
                .collect::<Vec<_>>();
            let schema = Arc::new(
                table_schema
                    .project(&projection)
                    .whatever_context("project table schema")?,
            );

            Ok(ResponsePartition {
                id,
                t_min_max,
                schema,
            })
        })
        .collect::<Result<Vec<_>, Error>>()?;

    let batch_encoder = BatchEncoder::new(Arc::clone(&table_schema));

    Ok(QueryResponse {
        metadata: ResponseMetadata {
            ingester_uuid,
            persist_counter,
            table_schema,
            partitions,
        },
        payload: ResponseStream {
            payload_stream: payload_stream.fuse(),
            partition_projections,
            batch_encoder,
        }
        .boxed(),
    })
}

#[async_trait]
impl<L> Layer for DeserializeLayer<L>
where
    L: Layer<ResponsePayload = proto::QueryResponse>,
{
    type Request = L::Request;
    type ResponseMetadata = ResponseMetadata;
    type ResponsePayload = ResponsePayload;

    async fn query(
        &self,
        request: Self::Request,
    ) -> Result<QueryResponse<Self::ResponseMetadata, Self::ResponsePayload>, DynError> {
        // issue request
        let mut resp_stream = self.inner.query(request).await?.payload;

        // decode response
        let proto_md = read_metadata_msg(&mut resp_stream)
            .await
            .map_err(DynError::new)?;
        decode_metadata_msg(proto_md, resp_stream).map_err(DynError::new)
    }
}

/// Response stream of [`DeserializeLayer`].
struct ResponseStream {
    /// gRPC data stream.
    ///
    /// The metadata message was already read from this, this this should only contain payload messages.
    payload_stream: Fuse<BoxStream<'static, Result<proto::QueryResponse, DynError>>>,

    /// Projection for every partition.
    partition_projections: HashMap<TransitionPartitionId, HashSet<u64>>,

    /// Record batch encoder.
    batch_encoder: BatchEncoder,
}

impl Debug for ResponseStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ResponseStream")
            .field("payload_stream", &"<STREAM>")
            .field("partition_projections", &self.partition_projections)
            .field("batch_encoder", &self.batch_encoder)
            .finish()
    }
}

impl ResponseStream {
    /// Decode protobuf response.
    fn decode(&self, resp: proto::QueryResponse) -> Result<ResponsePayload, Error> {
        let proto::QueryResponse { msg } = resp;
        let msg = msg.whatever_context("msg must be set")?;

        let payload = match msg {
            proto::query_response::Msg::Metadata(_) => {
                whatever!("first message must be a payload message but is a metadata message");
            }
            proto::query_response::Msg::Payload(payload) => payload,
        };
        let proto::IngesterQueryResponsePayload {
            partition_id,
            projection,
            record_batch,
        } = payload;

        let partition_id = partition_id.whatever_context("payload needs partition ID")?;
        let partition_id = TransitionPartitionId::try_from(partition_id)
            .whatever_context("decode partition ID")?;

        let projection_set = projection.iter().cloned().collect::<HashSet<_>>();
        if projection_set.len() != projection.len() {
            whatever!("payload projection contains duplicates");
        }

        let partition_projection = self
            .partition_projections
            .get(&partition_id)
            .whatever_context("payload has unknown partition")?;
        if !projection
            .iter()
            .all(|idx| partition_projection.contains(idx))
        {
            whatever!("payload projection is not subset of partition projection");
        }
        let projection = projection
            .into_iter()
            .map(|idx| idx as usize)
            .collect::<Vec<_>>();
        let batch_encoder = self.batch_encoder.project(&projection).expect(
            "already checked if payload projection is subset of partition and partition schema was created, so this must succeed"
        );

        let record_batch = record_batch.whatever_context("payload needs record batch")?;
        let record_batch = batch_encoder
            .read(record_batch)
            .whatever_context("decode batch")?;

        Ok(ResponsePayload {
            partition_id,
            batch: record_batch,
        })
    }
}

impl Stream for ResponseStream {
    type Item = Result<ResponsePayload, DynError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.payload_stream.poll_next_unpin(cx).map(|resp| {
            resp.map(|res| {
                res.map_err(DynError::new)
                    .and_then(|resp| self.decode(resp).map_err(DynError::new))
            })
        })
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::{Int64Array, StringDictionaryBuilder, UInt64Array};
    use arrow::datatypes::{DataType, Field, Int32Type, Schema, SchemaRef};
    use arrow::record_batch::RecordBatch;
    use data_types::{PartitionHashId, PartitionKey, TableId};
    use ingester_query_grpc::arrow_serde::schema_to_bytes;

    use crate::layers::testing::{TestLayer, TestResponse};

    use super::*;

    #[tokio::test]
    async fn test_ok_happy_path() {
        let ingester_uuid = Uuid::from_u128(42);
        let table_schema = table_schema();
        let pid_1 = partition_id(1);
        let pid_2 = partition_id(2);
        let pid_3 = partition_id(3);

        let enc = BatchEncoder::new(Arc::clone(&table_schema));
        let table_batch = record_batch();
        let batch_2_1 = table_batch.project(&[3]).unwrap().slice(0, 2);
        let batch_1_1 = table_batch.project(&[0, 2]).unwrap().slice(2, 2);
        let batch_1_2 = table_batch.project(&[2, 3]).unwrap().slice(4, 1);
        let batch_2_2 = table_batch.project(&[3]).unwrap().slice(0, 0);

        let resp = simulate([
            proto::QueryResponse {
                msg: Some(proto::query_response::Msg::Metadata(
                    proto::IngesterQueryResponseMetadata {
                        ingester_uuid: ingester_uuid.to_string(),
                        persist_counter: 5,
                        table_schema: schema_to_bytes(&table_schema),
                        partitions: vec![
                            proto::ingester_query_response_metadata::Partition {
                                id: Some(pid_1.clone().into()),
                                t_min: 20,
                                t_max: 30,
                                projection: vec![0, 2, 3],
                            },
                            proto::ingester_query_response_metadata::Partition {
                                id: Some(pid_3.clone().into()),
                                t_min: 1,
                                t_max: 2,
                                projection: vec![0, 1, 2, 3],
                            },
                            proto::ingester_query_response_metadata::Partition {
                                id: Some(pid_2.clone().into()),
                                t_min: 22,
                                t_max: 22,
                                projection: vec![3],
                            },
                        ],
                    },
                )),
            },
            proto::QueryResponse {
                msg: Some(proto::query_response::Msg::Payload(
                    proto::IngesterQueryResponsePayload {
                        partition_id: Some(pid_2.clone().into()),
                        projection: vec![3],
                        record_batch: Some(enc.project(&[3]).unwrap().write(&batch_2_1).unwrap()),
                    },
                )),
            },
            proto::QueryResponse {
                msg: Some(proto::query_response::Msg::Payload(
                    proto::IngesterQueryResponsePayload {
                        partition_id: Some(pid_1.clone().into()),
                        projection: vec![0, 2],
                        record_batch: Some(
                            enc.project(&[0, 2]).unwrap().write(&batch_1_1).unwrap(),
                        ),
                    },
                )),
            },
            proto::QueryResponse {
                msg: Some(proto::query_response::Msg::Payload(
                    proto::IngesterQueryResponsePayload {
                        partition_id: Some(pid_1.clone().into()),
                        projection: vec![2, 3],
                        record_batch: Some(
                            enc.project(&[2, 3]).unwrap().write(&batch_1_2).unwrap(),
                        ),
                    },
                )),
            },
            proto::QueryResponse {
                msg: Some(proto::query_response::Msg::Payload(
                    proto::IngesterQueryResponsePayload {
                        partition_id: Some(pid_2.clone().into()),
                        projection: vec![3],
                        record_batch: Some(enc.project(&[3]).unwrap().write(&batch_2_2).unwrap()),
                    },
                )),
            },
        ])
        .await
        .unwrap();

        assert_eq!(
            resp.metadata,
            ResponseMetadata {
                ingester_uuid,
                persist_counter: 5,
                table_schema: Arc::clone(&table_schema),
                partitions: vec![
                    ResponsePartition {
                        id: pid_1.clone(),
                        t_min_max: TimestampMinMax::new(20, 30),
                        schema: Arc::new(table_schema.project(&[0, 2, 3]).unwrap())
                    },
                    ResponsePartition {
                        id: pid_3,
                        t_min_max: TimestampMinMax::new(1, 2),
                        schema: Arc::new(table_schema.project(&[0, 1, 2, 3]).unwrap())
                    },
                    ResponsePartition {
                        id: pid_2.clone(),
                        t_min_max: TimestampMinMax::new(22, 22),
                        schema: Arc::new(table_schema.project(&[3]).unwrap())
                    }
                ],
            },
        );
        let payload = resp.payload.try_collect::<Vec<_>>().await.unwrap();
        assert_eq!(
            payload,
            vec![
                ResponsePayload {
                    partition_id: pid_2.clone(),
                    batch: batch_2_1,
                },
                ResponsePayload {
                    partition_id: pid_1.clone(),
                    batch: batch_1_1,
                },
                ResponsePayload {
                    partition_id: pid_1,
                    batch: batch_1_2,
                },
                ResponsePayload {
                    partition_id: pid_2,
                    batch: batch_2_2,
                },
            ],
        );
    }

    #[tokio::test]
    async fn test_ok_no_partitions() {
        let ingester_uuid = Uuid::from_u128(42);
        let table_schema = table_schema();

        let resp = simulate([proto::QueryResponse {
            msg: Some(proto::query_response::Msg::Metadata(
                proto::IngesterQueryResponseMetadata {
                    ingester_uuid: ingester_uuid.to_string(),
                    persist_counter: 5,
                    table_schema: schema_to_bytes(&table_schema),
                    partitions: vec![],
                },
            )),
        }])
        .await
        .unwrap();

        assert_eq!(
            resp.metadata,
            ResponseMetadata {
                ingester_uuid,
                persist_counter: 5,
                table_schema: Arc::clone(&table_schema),
                partitions: vec![],
            },
        );

        let payload = resp.payload.try_collect::<Vec<_>>().await.unwrap();
        assert_eq!(payload, vec![],);
    }

    #[tokio::test]
    async fn test_err_empty_response() {
        let err = simulate([]).await.unwrap_err();
        assert_eq!(
            err.to_string(),
            "error deserializing gRPC response: Stream is empty but needs at least a metadata message",
        );
    }

    #[tokio::test]
    async fn test_err_ts_min_max() {
        let ingester_uuid = Uuid::from_u128(42);
        let table_schema = table_schema();
        let pid_1 = partition_id(1);

        let err = simulate([proto::QueryResponse {
            msg: Some(proto::query_response::Msg::Metadata(
                proto::IngesterQueryResponseMetadata {
                    ingester_uuid: ingester_uuid.to_string(),
                    persist_counter: 5,
                    table_schema: schema_to_bytes(&table_schema),
                    partitions: vec![proto::ingester_query_response_metadata::Partition {
                        id: Some(pid_1.clone().into()),
                        t_min: 3,
                        t_max: 2,
                        projection: vec![0, 2, 3],
                    }],
                },
            )),
        }])
        .await
        .unwrap_err();

        assert_eq!(
            err.to_string(),
            "error deserializing gRPC response: t_min (3) > t_max (2)"
        );
    }

    #[tokio::test]
    async fn test_err_duplicate_partition() {
        let ingester_uuid = Uuid::from_u128(42);
        let table_schema = table_schema();
        let pid_1 = partition_id(1);
        let pid_2 = partition_id(2);

        let err = simulate([proto::QueryResponse {
            msg: Some(proto::query_response::Msg::Metadata(
                proto::IngesterQueryResponseMetadata {
                    ingester_uuid: ingester_uuid.to_string(),
                    persist_counter: 5,
                    table_schema: schema_to_bytes(&table_schema),
                    partitions: vec![
                        proto::ingester_query_response_metadata::Partition {
                            id: Some(pid_1.clone().into()),
                            t_min: 1,
                            t_max: 1,
                            projection: vec![],
                        },
                        proto::ingester_query_response_metadata::Partition {
                            id: Some(pid_2.into()),
                            t_min: 1,
                            t_max: 1,
                            projection: vec![],
                        },
                        proto::ingester_query_response_metadata::Partition {
                            id: Some(pid_1.into()),
                            t_min: 1,
                            t_max: 1,
                            projection: vec![],
                        },
                    ],
                },
            )),
        }])
        .await
        .unwrap_err();

        assert_eq!(
            err.to_string(),
            "error deserializing gRPC response: duplicate partition"
        );
    }

    #[tokio::test]
    async fn test_err_duplicate_partition_projection() {
        let ingester_uuid = Uuid::from_u128(42);
        let table_schema = table_schema();
        let pid_1 = partition_id(1);

        let err = simulate([proto::QueryResponse {
            msg: Some(proto::query_response::Msg::Metadata(
                proto::IngesterQueryResponseMetadata {
                    ingester_uuid: ingester_uuid.to_string(),
                    persist_counter: 5,
                    table_schema: schema_to_bytes(&table_schema),
                    partitions: vec![proto::ingester_query_response_metadata::Partition {
                        id: Some(pid_1.clone().into()),
                        t_min: 1,
                        t_max: 1,
                        projection: vec![0, 1, 0],
                    }],
                },
            )),
        }])
        .await
        .unwrap_err();

        assert_eq!(
            err.to_string(),
            "error deserializing gRPC response: partition projection contains duplicates"
        );
    }

    #[tokio::test]
    async fn test_err_invalid_partition_projection() {
        let ingester_uuid = Uuid::from_u128(42);
        let table_schema = table_schema();
        let pid_1 = partition_id(1);

        let err = simulate([proto::QueryResponse {
            msg: Some(proto::query_response::Msg::Metadata(
                proto::IngesterQueryResponseMetadata {
                    ingester_uuid: ingester_uuid.to_string(),
                    persist_counter: 5,
                    table_schema: schema_to_bytes(&table_schema),
                    partitions: vec![proto::ingester_query_response_metadata::Partition {
                        id: Some(pid_1.clone().into()),
                        t_min: 1,
                        t_max: 1,
                        projection: vec![100],
                    }],
                },
            )),
        }])
        .await
        .unwrap_err();

        assert_eq!(
            err.to_string(),
            "error deserializing gRPC response: project table schema: Schema error: project index 100 out of bounds, max field 4"
        );
    }

    #[tokio::test]
    async fn test_err_invalid_payload_projection() {
        let ingester_uuid = Uuid::from_u128(42);
        let table_schema = table_schema();
        let pid_1 = partition_id(1);

        let enc = BatchEncoder::new(Arc::clone(&table_schema));
        let table_batch = record_batch();

        let resp = simulate([
            proto::QueryResponse {
                msg: Some(proto::query_response::Msg::Metadata(
                    proto::IngesterQueryResponseMetadata {
                        ingester_uuid: ingester_uuid.to_string(),
                        persist_counter: 5,
                        table_schema: schema_to_bytes(&table_schema),
                        partitions: vec![proto::ingester_query_response_metadata::Partition {
                            id: Some(pid_1.clone().into()),
                            t_min: 20,
                            t_max: 30,
                            projection: vec![0],
                        }],
                    },
                )),
            },
            proto::QueryResponse {
                msg: Some(proto::query_response::Msg::Payload(
                    proto::IngesterQueryResponsePayload {
                        partition_id: Some(pid_1.clone().into()),
                        projection: vec![0, 1],
                        record_batch: Some(
                            enc.project(&[0, 1])
                                .unwrap()
                                .write(&table_batch.project(&[0, 1]).unwrap())
                                .unwrap(),
                        ),
                    },
                )),
            },
        ])
        .await
        .unwrap();

        let err = resp.payload.try_collect::<Vec<_>>().await.unwrap_err();
        assert_eq!(
            err.to_string(),
            "error deserializing gRPC response: payload projection is not subset of partition projection",
        );
    }

    #[tokio::test]
    async fn test_err_duplicate_payload_projection() {
        let ingester_uuid = Uuid::from_u128(42);
        let table_schema = table_schema();
        let pid_1 = partition_id(1);

        let enc = BatchEncoder::new(Arc::clone(&table_schema));
        let table_batch = record_batch();

        let resp = simulate([
            proto::QueryResponse {
                msg: Some(proto::query_response::Msg::Metadata(
                    proto::IngesterQueryResponseMetadata {
                        ingester_uuid: ingester_uuid.to_string(),
                        persist_counter: 5,
                        table_schema: schema_to_bytes(&table_schema),
                        partitions: vec![proto::ingester_query_response_metadata::Partition {
                            id: Some(pid_1.clone().into()),
                            t_min: 20,
                            t_max: 30,
                            projection: vec![0, 1],
                        }],
                    },
                )),
            },
            proto::QueryResponse {
                msg: Some(proto::query_response::Msg::Payload(
                    proto::IngesterQueryResponsePayload {
                        partition_id: Some(pid_1.clone().into()),
                        projection: vec![0, 1, 0],
                        record_batch: Some(
                            enc.project(&[0, 1, 0])
                                .unwrap()
                                .write(&table_batch.project(&[0, 1, 0]).unwrap())
                                .unwrap(),
                        ),
                    },
                )),
            },
        ])
        .await
        .unwrap();

        let err = resp.payload.try_collect::<Vec<_>>().await.unwrap_err();
        assert_eq!(
            err.to_string(),
            "error deserializing gRPC response: payload projection contains duplicates",
        );
    }

    async fn simulate<const N: usize>(
        payload: [proto::QueryResponse; N],
    ) -> Result<QueryResponse<ResponseMetadata, ResponsePayload>, DynError> {
        let l = TestLayer::<(), (), proto::QueryResponse>::default();

        let mut resp = TestResponse::ok(());
        for p in payload {
            resp = resp.with_ok_payload(p);
        }
        l.mock_response(resp);

        let l = DeserializeLayer::new(l);
        l.query(()).await
    }

    fn table_schema() -> SchemaRef {
        Arc::new(Schema::new_with_metadata(
            vec![
                Field::new("f1", DataType::UInt64, true).with_metadata(HashMap::from([(
                    String::from("field_md_k"),
                    String::from("field_md_v"),
                )])),
                Field::new("f2", DataType::Int64, false),
                Field::new(
                    "f3",
                    DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                    false,
                ),
                Field::new(
                    "f4",
                    DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                    true,
                ),
            ],
            HashMap::from([(String::from("table_md_k"), String::from("table_md_v"))]),
        ))
    }

    fn record_batch() -> RecordBatch {
        const N_ROWS: usize = 10;

        let mut builder_1 = StringDictionaryBuilder::<Int32Type>::new();
        let mut builder_2 = StringDictionaryBuilder::<Int32Type>::new();
        for i in 0..N_ROWS {
            builder_1.append(format!("x_{i}")).unwrap();
            builder_2.append_option((i % 3 != 0).then(|| format!("y_{i}")));
        }

        RecordBatch::try_new(
            table_schema(),
            vec![
                Arc::new(
                    (0..N_ROWS)
                        .map(|i| (i % 2 == 0).then_some(i as u64))
                        .collect::<UInt64Array>(),
                ),
                Arc::new((0..N_ROWS).map(|i| 10 + (i as i64)).collect::<Int64Array>()),
                Arc::new(builder_1.finish()),
                Arc::new(builder_2.finish()),
            ],
        )
        .unwrap()
    }

    fn partition_id(id: i128) -> TransitionPartitionId {
        TransitionPartitionId::Deterministic(PartitionHashId::new(
            TableId::new(1),
            &PartitionKey::from(id.to_string()),
        ))
    }
}
