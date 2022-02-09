//! gRPC service implementations for `ingester`.

use crate::handler::IngestHandler;
use arrow_flight::{
    flight_service_server::{FlightService as Flight, FlightServiceServer as FlightServer},
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, PutResult, SchemaResult, Ticket,
};
use futures::Stream;
use generated_types::influxdata::iox::ingester::v1 as proto;
use observability_deps::tracing::info;
use prost::Message;
use snafu::{ResultExt, Snafu};
use std::{pin::Pin, sync::Arc};
use tonic::{Request, Response, Streaming};
use trace::ctx::SpanContext;

/// This type is responsible for managing all gRPC services exposed by
/// `ingester`.
#[derive(Debug, Default)]
pub struct GrpcDelegate<I: IngestHandler> {
    ingest_handler: Arc<I>,
}

impl<I: IngestHandler + Send + Sync + 'static> GrpcDelegate<I> {
    /// Initialise a new [`GrpcDelegate`] passing valid requests to the
    /// specified `ingest_handler`.
    pub fn new(ingest_handler: Arc<I>) -> Self {
        Self { ingest_handler }
    }

    /// Acquire an Arrow Flight gRPC service implementation.
    pub fn flight_service(&self) -> FlightServer<impl Flight> {
        FlightServer::new(FlightService {
            ingest_handler: Arc::clone(&self.ingest_handler),
        })
    }
}

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
    Query { source: crate::handler::Error },
}

impl From<Error> for tonic::Status {
    /// Logs and converts a result from the business logic into the appropriate tonic status
    fn from(err: Error) -> Self {
        // An explicit match on the Error enum will ensure appropriate logging is handled for any
        // new error variants.
        let msg = "Error handling Flight gRPC request";
        match err {
            Error::InvalidTicket { .. } | Error::InvalidQuery { .. } | Error::Query { .. } => {
                // TODO(edd): this should be `debug`. Keeping at info whilst IOx still in early
                // development
                info!(?err, msg)
            }
        }
        err.to_status()
    }
}

impl Error {
    /// Converts a result from the business logic into the appropriate tonic status
    fn to_status(&self) -> tonic::Status {
        use tonic::Status;
        match &self {
            Self::InvalidTicket { .. } | Self::InvalidQuery { .. } => {
                Status::invalid_argument(self.to_string())
            }
            Self::Query { .. } => Status::internal(self.to_string()),
        }
    }
}

/// Concrete implementation of the gRPC Arrow Flight Service API
#[derive(Debug)]
struct FlightService<I: IngestHandler + Send + Sync + 'static> {
    ingest_handler: Arc<I>,
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

        let _query_response = self
            .ingest_handler
            .query(query_request)
            .context(QuerySnafu)?;

        let output = futures::stream::empty();
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
