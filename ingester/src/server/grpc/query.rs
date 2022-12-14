use std::{
    pin::Pin,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    task::Poll,
};

use arrow::error::ArrowError;
use arrow_flight::{
    flight_service_server::FlightService as Flight, Action, ActionType, Criteria, Empty,
    FlightData, FlightDescriptor, FlightInfo, HandshakeRequest, HandshakeResponse, IpcMessage,
    PutResult, SchemaAsIpc, SchemaResult, Ticket,
};
use data_types::{NamespaceId, TableId};
use flatbuffers::FlatBufferBuilder;
use futures::Stream;
use generated_types::influxdata::iox::ingester::v1::{self as proto};
use observability_deps::tracing::*;
use pin_project::pin_project;
use prost::Message;
use snafu::{ResultExt, Snafu};
use tonic::{Request, Response, Streaming};
use trace::{ctx::SpanContext, span::SpanExt};

use crate::{
    handler::IngestHandler,
    querier_handler::{FlatIngesterQueryResponse, FlatIngesterQueryResponseStream},
};

#[derive(Debug, Snafu)]
#[allow(missing_docs)]
pub enum Error {
    #[snafu(display("Invalid ticket. Error: {:?} Ticket: {:?}", source, ticket))]
    InvalidTicket {
        source: prost::DecodeError,
        ticket: Vec<u8>,
    },

    #[snafu(display("Invalid query, could not convert protobuf: {}", source))]
    InvalidQuery {
        source: generated_types::google::FieldViolation,
    },

    #[snafu(display("Error while performing query: {}", source))]
    Query {
        source: Box<crate::querier_handler::Error>,
    },

    #[snafu(display("No Namespace Data found for the given namespace ID {}", namespace_id,))]
    NamespaceNotFound { namespace_id: NamespaceId },

    #[snafu(display(
        "No Table Data found for the given namespace ID {}, table ID {}",
        namespace_id,
        table_id
    ))]
    TableNotFound {
        namespace_id: NamespaceId,
        table_id: TableId,
    },

    #[snafu(display("Error while streaming query results: {}", source))]
    QueryStream { source: ArrowError },

    #[snafu(display("Error during protobuf serialization: {}", source))]
    Serialization { source: prost::EncodeError },
}

impl From<Error> for tonic::Status {
    /// Logs and converts a result from the business logic into the appropriate tonic status
    fn from(err: Error) -> Self {
        // An explicit match on the Error enum will ensure appropriate logging is handled for any
        // new error variants.
        let msg = "Error handling Flight gRPC request";
        match err {
            Error::InvalidTicket { .. }
            | Error::InvalidQuery { .. }
            | Error::Query { .. }
            | Error::NamespaceNotFound { .. }
            | Error::TableNotFound { .. } => {
                debug!(e=%err, msg)
            }
            Error::QueryStream { .. } | Error::Serialization { .. } => {
                warn!(e=%err, msg)
            }
        }
        err.to_status()
    }
}

impl Error {
    /// Converts a result from the business logic into the appropriate tonic status
    fn to_status(&self) -> tonic::Status {
        use tonic::Status;
        match self {
            Self::InvalidTicket { .. } | Self::InvalidQuery { .. } => {
                Status::invalid_argument(self.to_string())
            }
            Self::Query { .. } | Self::QueryStream { .. } | Self::Serialization { .. } => {
                Status::internal(self.to_string())
            }
            Self::NamespaceNotFound { .. } | Self::TableNotFound { .. } => {
                Status::not_found(self.to_string())
            }
        }
    }
}

/// Concrete implementation of the gRPC Arrow Flight Service API
#[derive(Debug)]
pub(super) struct FlightService<I: IngestHandler + Send + Sync + 'static> {
    ingest_handler: Arc<I>,

    /// How many `do_get` flight requests should panic for testing purposes.
    ///
    /// Every panic will decrease the counter until it reaches zero. At zero, no panics will occur.
    test_flight_do_get_panic: Arc<AtomicU64>,
}

impl<I> FlightService<I>
where
    I: IngestHandler + Send + Sync + 'static,
{
    pub(super) fn new(ingest_handler: Arc<I>, test_flight_do_get_panic: Arc<AtomicU64>) -> Self {
        Self {
            ingest_handler,
            test_flight_do_get_panic,
        }
    }

    fn maybe_panic_in_flight_do_get(&self) {
        loop {
            let current = self.test_flight_do_get_panic.load(Ordering::SeqCst);
            if current == 0 {
                return;
            }
            if self
                .test_flight_do_get_panic
                .compare_exchange(current, current - 1, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                break;
            }
        }

        panic!("Panicking in `do_get` for testing purposes.");
    }
}

type TonicStream<T> = Pin<Box<dyn Stream<Item = Result<T, tonic::Status>> + Send + 'static>>;

#[tonic::async_trait]
impl<I: IngestHandler + Send + Sync + 'static> Flight for FlightService<I> {
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
        let ticket = request.into_inner();

        let proto_query_request =
            proto::IngesterQueryRequest::decode(&*ticket.ticket).context(InvalidTicketSnafu {
                ticket: ticket.ticket,
            })?;

        let query_request = proto_query_request.try_into().context(InvalidQuerySnafu)?;

        self.maybe_panic_in_flight_do_get();

        let query_response = self
            .ingest_handler
            .query(query_request, span_ctx.child_span("ingest handler query"))
            .await
            .map_err(|e| match e {
                crate::querier_handler::Error::NamespaceNotFound { namespace_id } => {
                    Error::NamespaceNotFound { namespace_id }
                }
                crate::querier_handler::Error::TableNotFound {
                    namespace_id,
                    table_id,
                } => Error::TableNotFound {
                    namespace_id,
                    table_id,
                },
                _ => Error::Query {
                    source: Box::new(e),
                },
            })?;

        let output = GetStream::new(query_response.flatten());

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

#[pin_project]
struct GetStream {
    #[pin]
    inner: Pin<Box<dyn Stream<Item = Result<FlatIngesterQueryResponse, ArrowError>> + Send>>,
    done: bool,
    buffer: Vec<FlightData>,
}

impl GetStream {
    fn new(inner: FlatIngesterQueryResponseStream) -> Self {
        Self {
            inner,
            done: false,
            buffer: vec![],
        }
    }
}

impl Stream for GetStream {
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
                    let e = Error::QueryStream { source: e }.into();
                    Poll::Ready(Some(Err(e)))
                }
                Poll::Ready(Some(Ok(FlatIngesterQueryResponse::StartPartition {
                    partition_id,
                    status,
                }))) => {
                    let mut bytes = bytes::BytesMut::new();
                    let app_metadata = proto::IngesterQueryResponseMetadata {
                        partition_id: partition_id.get(),
                        status: Some(proto::PartitionStatus {
                            parquet_max_sequence_number: status
                                .parquet_max_sequence_number
                                .map(|x| x.get()),
                        }),
                        // These fields are only used in ingester2.
                        ingester_uuid: String::new(),
                        completed_persistence_count: 0,
                    };
                    prost::Message::encode(&app_metadata, &mut bytes)
                        .context(SerializationSnafu)?;

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

    use super::*;
    use crate::querier_handler::{FlatIngesterQueryResponse, PartitionStatus};

    #[tokio::test]
    async fn test_get_stream_empty() {
        assert_get_stream(vec![], vec![]).await;
    }

    #[tokio::test]
    async fn test_get_stream_all_types() {
        let batch = lp_to_mutable_batch("table z=1 0")
            .1
            .to_arrow(Projection::All)
            .unwrap();
        let schema = batch.schema();

        assert_get_stream(
            vec![
                Ok(FlatIngesterQueryResponse::StartPartition {
                    partition_id: PartitionId::new(1),
                    status: PartitionStatus {
                        parquet_max_sequence_number: None,
                    },
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
                        // These fields are only used in ingester2.
                        ingester_uuid: String::new(),
                        completed_persistence_count: 0,
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
        assert_get_stream(
            vec![
                Ok(FlatIngesterQueryResponse::StartPartition {
                    partition_id: PartitionId::new(1),
                    status: PartitionStatus {
                        parquet_max_sequence_number: None,
                    },
                }),
                Err(ArrowError::IoError("foo".into())),
                Ok(FlatIngesterQueryResponse::StartPartition {
                    partition_id: PartitionId::new(1),
                    status: PartitionStatus {
                        parquet_max_sequence_number: None,
                    },
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
                        // These fields are only used in ingester2.
                        ingester_uuid: String::new(),
                        completed_persistence_count: 0,
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
        inputs: Vec<Result<FlatIngesterQueryResponse, ArrowError>>,
        expected: Vec<Result<DecodedFlightData, tonic::Code>>,
    ) {
        let inner = Box::pin(futures::stream::iter(inputs));
        let stream = GetStream::new(inner);
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
}
