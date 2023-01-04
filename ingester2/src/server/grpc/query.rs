use std::{pin::Pin, sync::Arc, task::Poll};

use arrow::{error::ArrowError, record_batch::RecordBatch};
use arrow_flight::{
    flight_service_server::FlightService as Flight, Action, ActionType, Criteria, Empty,
    FlightData, FlightDescriptor, FlightInfo, HandshakeRequest, HandshakeResponse, IpcMessage,
    PutResult, SchemaAsIpc, SchemaResult, Ticket,
};
use arrow_util::optimize::{
    prepare_batch_for_flight, prepare_schema_for_flight, split_batch_for_grpc_response,
};
use data_types::{NamespaceId, PartitionId, TableId};
use flatbuffers::FlatBufferBuilder;
use futures::{Stream, StreamExt};
use generated_types::influxdata::iox::ingester::v1::{self as proto, PartitionStatus};
use metric::U64Counter;
use observability_deps::tracing::*;
use pin_project::pin_project;
use prost::Message;
use thiserror::Error;
use tokio::sync::{Semaphore, TryAcquireError};
use tonic::{Request, Response, Streaming};
use trace::{ctx::SpanContext, span::SpanExt};
use uuid::Uuid;

use crate::query::{response::QueryResponse, QueryError, QueryExec};

/// Error states for the query RPC handler.
///
/// Note that this DOES NOT include any query-time error states - those are
/// mapped directly from the [`QueryError`] itself.
///
/// Note that this isn't strictly necessary as the [`FlightService`] trait
/// expects a [`tonic::Status`] error value, but by defining the errors here
/// they serve as documentation of the potential error states (which are then
/// converted into [`tonic::Status`] for the handler).
#[derive(Debug, Error)]
enum Error {
    /// The payload within the Flight ticket cannot be deserialised into a
    /// [`proto::IngesterQueryRequest`].
    #[error("invalid flight ticket: {0}")]
    InvalidTicket(#[from] prost::DecodeError),

    /// The [`proto::IngesterQueryResponseMetadata`] response metadata being
    /// returned to the RPC caller cannot be serialised into the protobuf
    /// response format.
    #[error("failed to serialise response: {0}")]
    SerialiseResponse(#[from] prost::EncodeError),

    /// An error was observed in the [`QueryResponse`] stream.
    #[error("error streaming response: {0}")]
    Stream(#[from] ArrowError),

    /// The number of simultaneous queries being executed has been reached.
    #[error("simultaneous query limit exceeded")]
    RequestLimit,
}

/// Map a query-execution error into a [`tonic::Status`].
impl From<QueryError> for tonic::Status {
    fn from(e: QueryError) -> Self {
        use tonic::Code;

        let code = match e {
            QueryError::TableNotFound(_, _) | QueryError::NamespaceNotFound(_) => Code::NotFound,
        };

        Self::new(code, e.to_string())
    }
}

/// Map a gRPC handler error to a [`tonic::Status`].
impl From<Error> for tonic::Status {
    fn from(e: Error) -> Self {
        use tonic::Code;

        let code = match e {
            Error::InvalidTicket(_) => {
                debug!(error=%e, "invalid flight query ticket");
                Code::InvalidArgument
            }
            Error::Stream(_) | Error::SerialiseResponse(_) => {
                error!(error=%e, "flight query response error");
                Code::Internal
            }
            Error::RequestLimit => {
                warn!("simultaneous query limit exceeded");
                Code::ResourceExhausted
            }
        };

        Self::new(code, e.to_string())
    }
}

/// Concrete implementation of the gRPC Arrow Flight Service API
#[derive(Debug)]
pub(crate) struct FlightService<Q> {
    query_handler: Q,

    /// A request limiter to restrict the number of simultaneous requests this
    /// ingester services.
    ///
    /// This allows the ingester to drop a portion of requests when experiencing
    /// an unusual flood of requests
    request_sem: Semaphore,

    /// Number of queries rejected due to lack of available `request_sem`
    /// permit.
    query_request_limit_rejected: U64Counter,

    ingester_uuid: Uuid,
}

impl<Q> FlightService<Q> {
    pub(super) fn new(
        query_handler: Q,
        max_simultaneous_requests: usize,
        metrics: &metric::Registry,
    ) -> Self {
        let query_request_limit_rejected = metrics
            .register_metric::<U64Counter>(
                "query_request_limit_rejected",
                "number of query requests rejected due to exceeding parallel request limit",
            )
            .recorder(&[]);

        Self {
            query_handler,
            request_sem: Semaphore::new(max_simultaneous_requests),
            query_request_limit_rejected,
            ingester_uuid: Uuid::new_v4(),
        }
    }
}

type TonicStream<T> = Pin<Box<dyn Stream<Item = Result<T, tonic::Status>> + Send + 'static>>;

#[tonic::async_trait]
impl<Q> Flight for FlightService<Q>
where
    Q: QueryExec<Response = QueryResponse> + 'static,
{
    type HandshakeStream = TonicStream<HandshakeResponse>;
    type ListFlightsStream = TonicStream<FlightInfo>;
    type DoGetStream = TonicStream<FlightData>;
    type DoPutStream = TonicStream<PutResult>;
    type DoActionStream = TonicStream<arrow_flight::Result>;
    type ListActionsStream = TonicStream<ActionType>;
    type DoExchangeStream = TonicStream<FlightData>;

    async fn get_schema(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, tonic::Status> {
        Err(tonic::Status::unimplemented("Not yet implemented"))
    }

    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, tonic::Status> {
        let span_ctx: Option<SpanContext> = request.extensions().get().cloned();

        // Acquire and hold a permit for the duration of this request, or return
        // an error if the existing requests have already exhausted the
        // allocation.
        //
        // Our goal is to limit the number of concurrently executing queries as
        // a rough way of ensuring we don't explode memory by trying to do too
        // much at the same time.
        let _permit = match self.request_sem.try_acquire() {
            Ok(p) => p,
            Err(TryAcquireError::NoPermits) => {
                warn!("simultaneous request limit exceeded - dropping query request");
                self.query_request_limit_rejected.inc(1);
                return Err(Error::RequestLimit)?;
            }
            Err(e) => panic!("request limiter error: {}", e),
        };

        let ticket = request.into_inner();
        let request = proto::IngesterQueryRequest::decode(&*ticket.ticket).map_err(Error::from)?;

        // Extract the namespace/table identifiers
        let namespace_id = NamespaceId::new(request.namespace_id);
        let table_id = TableId::new(request.table_id);

        // Predicate pushdown is part of the API, but not implemented.
        if let Some(p) = request.predicate {
            warn!(predicate=?p, "ignoring query predicate (unsupported)");
        }

        let response = self
            .query_handler
            .query_exec(
                namespace_id,
                table_id,
                request.columns,
                span_ctx.child_span("ingester query"),
            )
            .await?;

        let output = FlightFrameCodec::new(
            FlatIngesterQueryResponseStream::from(response),
            self.ingester_uuid,
        );

        Ok(Response::new(Box::pin(output) as Self::DoGetStream))
    }

    async fn handshake(
        &self,
        request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, tonic::Status> {
        let request = request.into_inner().message().await?.unwrap();
        let response = HandshakeResponse {
            protocol_version: request.protocol_version,
            payload: request.payload,
        };
        let output = futures::stream::iter(std::iter::once(Ok(response)));
        Ok(Response::new(Box::pin(output) as Self::HandshakeStream))
    }

    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, tonic::Status> {
        Err(tonic::Status::unimplemented("Not yet implemented"))
    }

    async fn get_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, tonic::Status> {
        Err(tonic::Status::unimplemented("Not yet implemented"))
    }

    async fn do_put(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, tonic::Status> {
        Err(tonic::Status::unimplemented("Not yet implemented"))
    }

    async fn do_action(
        &self,
        _request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, tonic::Status> {
        Err(tonic::Status::unimplemented("Not yet implemented"))
    }

    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, tonic::Status> {
        Err(tonic::Status::unimplemented("Not yet implemented"))
    }

    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, tonic::Status> {
        Err(tonic::Status::unimplemented("Not yet implemented"))
    }
}

/// A stream of [`FlatIngesterQueryResponse`], itself a flattened version of
/// [`QueryResponse`].
type FlatIngesterQueryResponseStream =
    Pin<Box<dyn Stream<Item = Result<FlatIngesterQueryResponse, ArrowError>> + Send>>;

/// Element within the flat wire protocol.
#[derive(Debug, PartialEq)]
pub enum FlatIngesterQueryResponse {
    /// Start a new partition.
    StartPartition {
        /// Partition ID.
        partition_id: PartitionId,

        /// Partition persistence status.
        status: PartitionStatus,

        /// Count of persisted Parquet files for the [`PartitionData`] instance this
        /// [`PartitionResponse`] was generated from.
        ///
        /// [`PartitionData`]: crate::buffer_tree::partition::PartitionData
        /// [`PartitionResponse`]: crate::query::partition_response::PartitionResponse
        completed_persistence_count: u64,
    },

    /// Start a new snapshot.
    ///
    /// The snapshot belongs to the partition of the last [`StartPartition`](Self::StartPartition)
    /// message.
    StartSnapshot {
        /// Snapshot schema.
        schema: Arc<arrow::datatypes::Schema>,
    },

    /// Add a record batch to the snapshot that was announced by the last
    /// [`StartSnapshot`](Self::StartSnapshot) message.
    RecordBatch {
        /// Record batch.
        batch: RecordBatch,
    },
}

impl From<QueryResponse> for FlatIngesterQueryResponseStream {
    fn from(v: QueryResponse) -> Self {
        v.into_partition_stream()
            .flat_map(|partition| {
                let partition_id = partition.id();
                let completed_persistence_count = partition.completed_persistence_count();
                let head = futures::stream::once(async move {
                    Ok(FlatIngesterQueryResponse::StartPartition {
                        partition_id,
                        status: PartitionStatus {
                            parquet_max_sequence_number: None,
                        },
                        completed_persistence_count,
                    })
                });

                match partition.into_record_batch_stream() {
                    Some(stream) => {
                        let tail = stream.flat_map(|snapshot_res| match snapshot_res {
                            Ok(snapshot) => {
                                let schema =
                                    Arc::new(prepare_schema_for_flight(&snapshot.schema()));

                                let schema_captured = Arc::clone(&schema);
                                let head = futures::stream::once(async {
                                    Ok(FlatIngesterQueryResponse::StartSnapshot {
                                        schema: schema_captured,
                                    })
                                });

                                let tail = match prepare_batch_for_flight(
                                    &snapshot,
                                    Arc::clone(&schema),
                                ) {
                                    Ok(batch) => {
                                        futures::stream::iter(split_batch_for_grpc_response(batch))
                                            .map(|batch| {
                                                Ok(FlatIngesterQueryResponse::RecordBatch { batch })
                                            })
                                            .boxed()
                                    }
                                    Err(e) => futures::stream::once(async { Err(e) }).boxed(),
                                };

                                head.chain(tail).boxed()
                            }
                            Err(e) => futures::stream::once(async { Err(e) }).boxed(),
                        });

                        head.chain(tail).boxed()
                    }
                    None => head.boxed(),
                }
            })
            .boxed()
    }
}

/// A mapping decorator over a [`FlatIngesterQueryResponseStream`] that converts
/// it into [`arrow_flight`] response frames.
#[pin_project]
struct FlightFrameCodec {
    #[pin]
    inner: Pin<Box<dyn Stream<Item = Result<FlatIngesterQueryResponse, ArrowError>> + Send>>,
    done: bool,
    buffer: Vec<FlightData>,
    ingester_uuid: Uuid,
}

impl FlightFrameCodec {
    fn new(inner: FlatIngesterQueryResponseStream, ingester_uuid: Uuid) -> Self {
        Self {
            inner,
            done: false,
            buffer: vec![],
            ingester_uuid,
        }
    }
}

impl Stream for FlightFrameCodec {
    type Item = Result<FlightData, tonic::Status>;

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let this = self.project();

        if !this.buffer.is_empty() {
            let next = this.buffer.remove(0);
            return Poll::Ready(Some(Ok(next)));
        }

        if *this.done {
            Poll::Ready(None)
        } else {
            match this.inner.poll_next(cx) {
                Poll::Pending => Poll::Pending,
                Poll::Ready(None) => {
                    *this.done = true;
                    Poll::Ready(None)
                }
                Poll::Ready(Some(Err(e))) => {
                    *this.done = true;
                    let e = Error::Stream(e).into();
                    Poll::Ready(Some(Err(e)))
                }
                Poll::Ready(Some(Ok(FlatIngesterQueryResponse::StartPartition {
                    partition_id,
                    status,
                    completed_persistence_count,
                }))) => {
                    let mut bytes = bytes::BytesMut::new();
                    let app_metadata = proto::IngesterQueryResponseMetadata {
                        partition_id: partition_id.get(),
                        status: Some(proto::PartitionStatus {
                            parquet_max_sequence_number: status.parquet_max_sequence_number,
                        }),
                        ingester_uuid: this.ingester_uuid.to_string(),
                        completed_persistence_count,
                    };
                    prost::Message::encode(&app_metadata, &mut bytes).map_err(Error::from)?;

                    let flight_data = arrow_flight::FlightData::new(
                        None,
                        IpcMessage(build_none_flight_msg()),
                        bytes.to_vec(),
                        vec![],
                    );
                    Poll::Ready(Some(Ok(flight_data)))
                }
                Poll::Ready(Some(Ok(FlatIngesterQueryResponse::StartSnapshot { schema }))) => {
                    let options = arrow::ipc::writer::IpcWriteOptions::default();
                    let flight_data: FlightData = SchemaAsIpc::new(&schema, &options).into();
                    Poll::Ready(Some(Ok(flight_data)))
                }
                Poll::Ready(Some(Ok(FlatIngesterQueryResponse::RecordBatch { batch }))) => {
                    let options = arrow::ipc::writer::IpcWriteOptions::default();
                    let (mut flight_dictionaries, flight_batch) =
                        arrow_flight::utils::flight_data_from_arrow_batch(&batch, &options);
                    std::mem::swap(this.buffer, &mut flight_dictionaries);
                    this.buffer.push(flight_batch);
                    let next = this.buffer.remove(0);
                    Poll::Ready(Some(Ok(next)))
                }
            }
        }
    }
}

fn build_none_flight_msg() -> Vec<u8> {
    let mut fbb = FlatBufferBuilder::new();

    let mut message = arrow::ipc::MessageBuilder::new(&mut fbb);
    message.add_version(arrow::ipc::MetadataVersion::V5);
    message.add_header_type(arrow::ipc::MessageHeader::NONE);
    message.add_bodyLength(0);

    let data = message.finish();
    fbb.finish(data, None);

    fbb.finished_data().to_vec()
}

#[cfg(test)]
mod tests {
    use arrow::{error::ArrowError, ipc::MessageHeader};
    use data_types::PartitionId;
    use futures::StreamExt;
    use generated_types::influxdata::iox::ingester::v1::{self as proto};
    use mutable_batch_lp::test_helpers::lp_to_mutable_batch;
    use schema::Projection;
    use tonic::Code;

    use crate::query::mock_query_exec::MockQueryExec;

    use super::*;

    #[tokio::test]
    async fn test_get_stream_empty() {
        assert_get_stream(Uuid::new_v4(), vec![], vec![]).await;
    }

    #[tokio::test]
    async fn test_get_stream_all_types() {
        let batch = lp_to_mutable_batch("table z=1 0")
            .1
            .to_arrow(Projection::All)
            .unwrap();
        let schema = batch.schema();
        let ingester_uuid = Uuid::new_v4();

        assert_get_stream(
            ingester_uuid,
            vec![
                Ok(FlatIngesterQueryResponse::StartPartition {
                    partition_id: PartitionId::new(1),
                    status: PartitionStatus {
                        parquet_max_sequence_number: None,
                    },
                    completed_persistence_count: 0,
                }),
                Ok(FlatIngesterQueryResponse::StartSnapshot { schema }),
                Ok(FlatIngesterQueryResponse::RecordBatch { batch }),
            ],
            vec![
                Ok(DecodedFlightData {
                    header_type: MessageHeader::NONE,
                    app_metadata: proto::IngesterQueryResponseMetadata {
                        partition_id: 1,
                        status: Some(proto::PartitionStatus {
                            parquet_max_sequence_number: None,
                        }),
                        completed_persistence_count: 0,
                        ingester_uuid: ingester_uuid.to_string(),
                    },
                }),
                Ok(DecodedFlightData {
                    header_type: MessageHeader::Schema,
                    app_metadata: proto::IngesterQueryResponseMetadata::default(),
                }),
                Ok(DecodedFlightData {
                    header_type: MessageHeader::RecordBatch,
                    app_metadata: proto::IngesterQueryResponseMetadata::default(),
                }),
            ],
        )
        .await;
    }

    #[tokio::test]
    async fn test_get_stream_shortcuts_err() {
        let ingester_uuid = Uuid::new_v4();
        assert_get_stream(
            ingester_uuid,
            vec![
                Ok(FlatIngesterQueryResponse::StartPartition {
                    partition_id: PartitionId::new(1),
                    status: PartitionStatus {
                        parquet_max_sequence_number: None,
                    },
                    completed_persistence_count: 0,
                }),
                Err(ArrowError::IoError("foo".into())),
                Ok(FlatIngesterQueryResponse::StartPartition {
                    partition_id: PartitionId::new(1),
                    status: PartitionStatus {
                        parquet_max_sequence_number: None,
                    },
                    completed_persistence_count: 0,
                }),
            ],
            vec![
                Ok(DecodedFlightData {
                    header_type: MessageHeader::NONE,
                    app_metadata: proto::IngesterQueryResponseMetadata {
                        partition_id: 1,
                        status: Some(proto::PartitionStatus {
                            parquet_max_sequence_number: None,
                        }),
                        completed_persistence_count: 0,
                        ingester_uuid: ingester_uuid.to_string(),
                    },
                }),
                Err(tonic::Code::Internal),
            ],
        )
        .await;
    }

    #[tokio::test]
    async fn test_get_stream_dictionary_batches() {
        let batch = lp_to_mutable_batch("table,x=\"foo\",y=\"bar\" z=1 0")
            .1
            .to_arrow(Projection::All)
            .unwrap();

        assert_get_stream(
            Uuid::new_v4(),
            vec![Ok(FlatIngesterQueryResponse::RecordBatch { batch })],
            vec![
                Ok(DecodedFlightData {
                    header_type: MessageHeader::DictionaryBatch,
                    app_metadata: proto::IngesterQueryResponseMetadata::default(),
                }),
                Ok(DecodedFlightData {
                    header_type: MessageHeader::DictionaryBatch,
                    app_metadata: proto::IngesterQueryResponseMetadata::default(),
                }),
                Ok(DecodedFlightData {
                    header_type: MessageHeader::RecordBatch,
                    app_metadata: proto::IngesterQueryResponseMetadata::default(),
                }),
            ],
        )
        .await;
    }

    struct DecodedFlightData {
        header_type: MessageHeader,
        app_metadata: proto::IngesterQueryResponseMetadata,
    }

    async fn assert_get_stream(
        ingester_uuid: Uuid,
        inputs: Vec<Result<FlatIngesterQueryResponse, ArrowError>>,
        expected: Vec<Result<DecodedFlightData, tonic::Code>>,
    ) {
        let inner = Box::pin(futures::stream::iter(inputs));
        let stream = FlightFrameCodec::new(inner, ingester_uuid);
        let actual: Vec<_> = stream.collect().await;
        assert_eq!(actual.len(), expected.len());

        for (actual, expected) in actual.into_iter().zip(expected) {
            match (actual, expected) {
                (Ok(actual), Ok(expected)) => {
                    let header_type = arrow::ipc::root_as_message(&actual.data_header[..])
                        .unwrap()
                        .header_type();
                    assert_eq!(header_type, expected.header_type);

                    let app_metadata: proto::IngesterQueryResponseMetadata =
                        prost::Message::decode(&actual.app_metadata[..]).unwrap();
                    assert_eq!(app_metadata, expected.app_metadata);
                }
                (Err(actual), Err(expected)) => {
                    assert_eq!(actual.code(), expected);
                }
                (Ok(_), Err(_)) => panic!("Actual is Ok but expected is Err"),
                (Err(_), Ok(_)) => panic!("Actual is Err but expected is Ok"),
            }
        }
    }

    #[tokio::test]
    async fn limits_concurrent_queries() {
        let mut flight =
            FlightService::new(MockQueryExec::default(), 100, &metric::Registry::default());

        let req = tonic::Request::new(Ticket { ticket: vec![] });
        match flight.do_get(req).await {
            Ok(_) => panic!("expected error because of invalid ticket"),
            Err(s) => {
                assert_eq!(s.code(), Code::NotFound); // Mock response value
            }
        }

        flight.request_sem = Semaphore::new(0);

        let req = tonic::Request::new(Ticket { ticket: vec![] });
        match flight.do_get(req).await {
            Ok(_) => panic!("expected error because of request limit"),
            Err(s) => {
                assert_eq!(s.code(), Code::ResourceExhausted);
            }
        }
    }
}
