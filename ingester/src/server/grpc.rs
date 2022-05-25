//! gRPC service implementations for `ingester`.

use crate::{data::IngesterQueryResponse, handler::IngestHandler};
use arrow::error::ArrowError;
use arrow_flight::{
    flight_service_server::{FlightService as Flight, FlightServiceServer as FlightServer},
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, PutResult, SchemaAsIpc, SchemaResult, Ticket,
};
use arrow_util::optimize::{optimize_record_batch, optimize_schema};
use futures::{SinkExt, Stream, StreamExt};
use generated_types::influxdata::iox::ingester::v1::{
    self as proto,
    write_info_service_server::{WriteInfoService, WriteInfoServiceServer},
};
use observability_deps::tracing::{info, warn};
use pin_project::{pin_project, pinned_drop};
use prost::Message;
use snafu::{ResultExt, Snafu};
use std::{
    pin::Pin,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    task::Poll,
};
use tokio::task::JoinHandle;
use tonic::{Request, Response, Streaming};
use trace::ctx::SpanContext;
use write_summary::WriteSummary;

/// This type is responsible for managing all gRPC services exposed by
/// `ingester`.
#[derive(Debug, Default)]
pub struct GrpcDelegate<I: IngestHandler> {
    ingest_handler: Arc<I>,

    /// How many `do_get` flight requests should panic for testing purposes.
    ///
    /// Every panic will decrease the counter until it reaches zero. At zero, no panics will occur.
    test_flight_do_get_panic: Arc<AtomicU64>,
}

impl<I: IngestHandler + Send + Sync + 'static> GrpcDelegate<I> {
    /// Initialise a new [`GrpcDelegate`] passing valid requests to the
    /// specified `ingest_handler`.
    pub fn new(ingest_handler: Arc<I>, test_flight_do_get_panic: Arc<AtomicU64>) -> Self {
        Self {
            ingest_handler,
            test_flight_do_get_panic,
        }
    }

    /// Acquire an Arrow Flight gRPC service implementation.
    pub fn flight_service(&self) -> FlightServer<impl Flight> {
        FlightServer::new(FlightService {
            ingest_handler: Arc::clone(&self.ingest_handler),
            test_flight_do_get_panic: Arc::clone(&self.test_flight_do_get_panic),
        })
    }

    /// Acquire an WriteInfo gRPC service implementation.
    pub fn write_info_service(&self) -> WriteInfoServiceServer<impl WriteInfoService> {
        WriteInfoServiceServer::new(WriteInfoServiceImpl::new(
            Arc::clone(&self.ingest_handler) as _
        ))
    }
}

/// Implementation of write info
struct WriteInfoServiceImpl {
    handler: Arc<dyn IngestHandler + Send + Sync + 'static>,
}

impl WriteInfoServiceImpl {
    pub fn new(handler: Arc<dyn IngestHandler + Send + Sync + 'static>) -> Self {
        Self { handler }
    }
}

#[tonic::async_trait]
impl WriteInfoService for WriteInfoServiceImpl {
    async fn get_write_info(
        &self,
        request: Request<proto::GetWriteInfoRequest>,
    ) -> Result<Response<proto::GetWriteInfoResponse>, tonic::Status> {
        let proto::GetWriteInfoRequest { write_token } = request.into_inner();

        let write_summary =
            WriteSummary::try_from_token(&write_token).map_err(tonic::Status::invalid_argument)?;

        let progresses = self
            .handler
            .progresses(write_summary.kafka_partitions())
            .await;

        let kafka_partition_infos = progresses
            .into_iter()
            .map(|(kafka_partition_id, progress)| {
                let status = write_summary
                    .write_status(kafka_partition_id, &progress)
                    .map_err(|e| tonic::Status::invalid_argument(e.to_string()))?;

                Ok(proto::KafkaPartitionInfo {
                    kafka_partition_id: kafka_partition_id.get(),
                    status: proto::KafkaPartitionStatus::from(status).into(),
                })
            })
            .collect::<Result<Vec<_>, tonic::Status>>()?;

        Ok(tonic::Response::new(proto::GetWriteInfoResponse {
            kafka_partition_infos,
        }))
    }
}

#[derive(Debug, Snafu)]
#[allow(missing_docs)]
pub enum Error {
    #[snafu(display("Failed to optimize record batch: {}", source))]
    Optimize { source: ArrowError },

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

    #[snafu(display(
        "No Namespace Data found for the given namespace name {}",
        namespace_name,
    ))]
    NamespaceNotFound { namespace_name: String },

    #[snafu(display(
        "No Table Data found for the given namespace name {}, table name {}",
        namespace_name,
        table_name
    ))]
    TableNotFound {
        namespace_name: String,
        table_name: String,
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
                // TODO(edd): this should be `debug`. Keeping at info whilst IOx still in early
                // development
                info!(?err, msg)
            }
            Error::Optimize { .. } | Error::QueryStream { .. } | Error::Serialization { .. } => {
                warn!(?err, msg)
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
            Self::Query { .. }
            | Self::Optimize { .. }
            | Self::QueryStream { .. }
            | Self::Serialization { .. } => Status::internal(self.to_string()),
            Self::NamespaceNotFound { .. } | Self::TableNotFound { .. } => {
                Status::not_found(self.to_string())
            }
        }
    }
}

/// Concrete implementation of the gRPC Arrow Flight Service API
#[derive(Debug)]
struct FlightService<I: IngestHandler + Send + Sync + 'static> {
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

type TonicStream<T> = Pin<Box<dyn Stream<Item = Result<T, tonic::Status>> + Send + Sync + 'static>>;

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
        let _span_ctx: Option<SpanContext> = request.extensions().get().cloned();
        let ticket = request.into_inner();

        let proto_query_request =
            proto::IngesterQueryRequest::decode(&*ticket.ticket).context(InvalidTicketSnafu {
                ticket: ticket.ticket,
            })?;

        let query_request = proto_query_request.try_into().context(InvalidQuerySnafu)?;

        self.maybe_panic_in_flight_do_get();

        let query_response =
            self.ingest_handler
                .query(query_request)
                .await
                .map_err(|e| match e {
                    crate::querier_handler::Error::NamespaceNotFound { namespace_name } => {
                        Error::NamespaceNotFound { namespace_name }
                    }
                    crate::querier_handler::Error::TableNotFound {
                        namespace_name,
                        table_name,
                    } => Error::TableNotFound {
                        namespace_name,
                        table_name,
                    },
                    _ => Error::Query {
                        source: Box::new(e),
                    },
                })?;

        let output = GetStream::new(query_response).await?;

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

#[pin_project(PinnedDrop)]
struct GetStream {
    #[pin]
    rx: futures::channel::mpsc::Receiver<Result<FlightData, tonic::Status>>,
    join_handle: JoinHandle<()>,
    done: bool,
}

impl GetStream {
    async fn new(query_response: IngesterQueryResponse) -> Result<Self, tonic::Status> {
        let IngesterQueryResponse {
            mut data,
            schema,
            unpersisted_partitions,
            batch_partition_ids,
        } = query_response;

        // setup channel
        let (mut tx, rx) = futures::channel::mpsc::channel::<Result<FlightData, tonic::Status>>(1);

        // get schema
        let schema = Arc::new(optimize_schema(&schema.as_arrow()));
        let options = arrow::ipc::writer::IpcWriteOptions::default();
        let mut schema_flight_data: FlightData = SchemaAsIpc::new(&schema, &options).into();

        // Add max_sequencer_number to app metadata
        let mut bytes = bytes::BytesMut::new();
        let app_metadata = proto::IngesterQueryResponseMetadata {
            unpersisted_partitions: unpersisted_partitions
                .into_iter()
                .map(|(id, status)| {
                    (
                        id.get(),
                        proto::PartitionStatus {
                            parquet_max_sequence_number: status
                                .parquet_max_sequence_number
                                .map(|x| x.get()),
                            tombstone_max_sequence_number: status
                                .tombstone_max_sequence_number
                                .map(|x| x.get()),
                        },
                    )
                })
                .collect(),
            batch_partition_ids: batch_partition_ids.into_iter().map(|id| id.get()).collect(),
        };
        prost::Message::encode(&app_metadata, &mut bytes).context(SerializationSnafu)?;
        schema_flight_data.app_metadata = bytes.to_vec();

        let join_handle = tokio::spawn(async move {
            if tx.send(Ok(schema_flight_data)).await.is_err() {
                // receiver gone
                return;
            }

            while let Some(batch_or_err) = data.next().await {
                match batch_or_err {
                    Ok(batch) => {
                        match optimize_record_batch(&batch, Arc::clone(&schema)) {
                            Ok(batch) => {
                                let (flight_dictionaries, flight_batch) =
                                    arrow_flight::utils::flight_data_from_arrow_batch(
                                        &batch, &options,
                                    );

                                for dict in flight_dictionaries {
                                    if tx.send(Ok(dict)).await.is_err() {
                                        // receiver is gone
                                        return;
                                    }
                                }

                                if tx.send(Ok(flight_batch)).await.is_err() {
                                    // receiver is gone
                                    return;
                                }
                            }
                            Err(e) => {
                                // failure sending here is OK because we're cutting the stream anyways
                                tx.send(Err(Error::Optimize { source: e }.into()))
                                    .await
                                    .ok();

                                // end stream
                                return;
                            }
                        }
                    }
                    Err(e) => {
                        // failure sending here is OK because we're cutting the stream anyways
                        tx.send(Err(Error::QueryStream { source: e }.into()))
                            .await
                            .ok();

                        // end stream
                        return;
                    }
                }
            }
        });

        Ok(Self {
            rx,
            join_handle,
            done: false,
        })
    }
}

#[pinned_drop]
impl PinnedDrop for GetStream {
    fn drop(self: Pin<&mut Self>) {
        self.join_handle.abort();
    }
}

impl Stream for GetStream {
    type Item = Result<FlightData, tonic::Status>;

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let this = self.project();
        if *this.done {
            Poll::Ready(None)
        } else {
            match this.rx.poll_next(cx) {
                Poll::Ready(None) => {
                    *this.done = true;
                    Poll::Ready(None)
                }
                e @ Poll::Ready(Some(Err(_))) => {
                    *this.done = true;
                    e
                }
                other => other,
            }
        }
    }
}
