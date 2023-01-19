use std::{
    pin::Pin,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use arrow::error::ArrowError;
use arrow_flight::{
    flight_service_server::FlightService as Flight, Action, ActionType, Criteria, Empty,
    FlightData, FlightDescriptor, FlightInfo, HandshakeRequest, HandshakeResponse, PutResult,
    SchemaResult, Ticket,
};
use data_types::{NamespaceId, TableId};
use futures::{Stream, TryStreamExt};
use generated_types::influxdata::iox::ingester::v1::{self as proto};
use observability_deps::tracing::*;
use prost::Message;
use snafu::{ResultExt, Snafu};
use tonic::{Request, Response, Streaming};
use trace::{ctx::SpanContext, span::SpanExt};

use crate::handler::IngestHandler;

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

        let output = query_response
            .flatten()
            // map FlightError --> tonic::Status
            .map_err(|e| e.into());
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
