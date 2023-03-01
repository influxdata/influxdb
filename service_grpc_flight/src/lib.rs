//! Implements the InfluxDB IOx Flight API and Arrow FlightSQL, based
//! on Arrow Flight and gRPC. See [`FlightService`] for full detail.

mod request;

use arrow::{
    datatypes::{DataType, Field, Schema, SchemaRef},
    error::ArrowError,
    ipc::writer::IpcWriteOptions,
    record_batch::RecordBatch,
};
use arrow_flight::{
    encode::{FlightDataEncoder, FlightDataEncoderBuilder},
    flight_descriptor::DescriptorType,
    flight_service_server::{FlightService as Flight, FlightServiceServer as FlightServer},
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightEndpoint, FlightInfo,
    HandshakeRequest, HandshakeResponse, PutResult, SchemaAsIpc, SchemaResult, Ticket,
};
use bytes::Bytes;
use data_types::NamespaceNameError;
use datafusion::{error::DataFusionError, physical_plan::ExecutionPlan};
use flightsql::FlightSQLCommand;
use futures::{ready, Stream, StreamExt, TryStreamExt};
use generated_types::influxdata::iox::querier::v1 as proto;
use iox_query::{
    exec::{ExecutionContextProvider, IOxSessionContext},
    QueryCompletedToken, QueryNamespace,
};
use observability_deps::tracing::{debug, info, warn};
use prost::Message;
use request::{IoxGetRequest, RunQuery};
use service_common::{datafusion_error_to_tonic_code, planner::Planner, QueryNamespaceProvider};
use snafu::{OptionExt, ResultExt, Snafu};
use std::{fmt::Debug, pin::Pin, sync::Arc, task::Poll, time::Instant};
use tonic::{metadata::MetadataMap, Request, Response, Streaming};
use trace::{ctx::SpanContext, span::SpanExt};
use trace_http::ctx::{RequestLogContext, RequestLogContextExt};
use tracker::InstrumentedAsyncOwnedSemaphorePermit;

/// The name of the grpc header that contains the target iox namespace
/// name for FlightSQL requests.
///
/// See <https://lists.apache.org/thread/fd6r1n7vt91sg2c7fr35wcrsqz6x4645>
/// for discussion on adding support to FlightSQL itself.
const IOX_FLIGHT_SQL_NAMESPACE_HEADER: &str = "iox-namespace-name";

#[allow(clippy::enum_variant_names)]
#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Invalid ticket. Error: {}", source))]
    InvalidTicket { source: request::Error },

    #[snafu(display("Internal creating encoding ticket: {}", source))]
    InternalCreatingTicket { source: request::Error },

    #[snafu(display("Invalid handshake. No payload provided"))]
    InvalidHandshake {},

    #[snafu(display("Namespace '{}' not found", namespace_name))]
    NamespaceNotFound { namespace_name: String },

    #[snafu(display(
        "Internal error reading points from namespace {}: {}",
        namespace_name,
        source
    ))]
    Query {
        namespace_name: String,
        source: DataFusionError,
    },

    #[snafu(display("no 'iox-namespace-name' header in request"))]
    NoFlightSQLNamespace,

    #[snafu(display("Invalid 'iox-namespace-name' header in request: {}", source))]
    InvalidNamespaceHeader {
        source: tonic::metadata::errors::ToStrError,
    },

    #[snafu(display("Invalid namespace name: {}", source))]
    InvalidNamespaceName { source: NamespaceNameError },

    #[snafu(display("Failed to optimize record batch: {}", source))]
    Optimize { source: ArrowError },

    #[snafu(display("Error while planning query: {}", source))]
    Planning {
        source: service_common::planner::Error,
    },

    #[snafu(display("Error while planning Flight SQL : {}", source))]
    FlightSQL { source: flightsql::Error },

    #[snafu(display("Invalid protobuf: {}", source))]
    Deserialization { source: prost::DecodeError },

    #[snafu(display("Unsupported message type: {}", description))]
    UnsupportedMessageType { description: String },
}
pub type Result<T, E = Error> = std::result::Result<T, E>;

impl From<Error> for tonic::Status {
    /// Converts a result from the business logic into the appropriate tonic
    /// status
    fn from(err: Error) -> Self {
        // An explicit match on the Error enum will ensure appropriate
        // logging is handled for any new error variants.
        let msg = "Error handling Flight gRPC request";
        match err {
            Error::NamespaceNotFound { .. }
            | Error::InvalidTicket { .. }
            | Error::InvalidHandshake { .. }
            // TODO(edd): this should be `debug`. Keeping at info while IOx in early development
            | Error::InvalidNamespaceName { .. } => info!(e=%err, msg),
            Error::Query { .. } => info!(e=%err, msg),
            Error::Optimize { .. }
            |Error::NoFlightSQLNamespace
            |Error::InvalidNamespaceHeader { .. }
            | Error::Planning { .. }
            | Error::Deserialization { .. }
            | Error::InternalCreatingTicket { .. }
                | Error::UnsupportedMessageType { .. }
                | Error::FlightSQL { .. }
            => {
                warn!(e=%err, msg)
            }
        }
        err.into_status()
    }
}

impl Error {
    /// Converts a result from the business logic into the appropriate tonic (gRPC)
    /// status message to send back to users
    fn into_status(self) -> tonic::Status {
        let msg = self.to_string();

        let code = match self {
            Self::NamespaceNotFound { .. } => tonic::Code::NotFound,
            Self::InvalidTicket { .. }
            | Self::InvalidHandshake { .. }
            | Self::Deserialization { .. }
            | Self::NoFlightSQLNamespace
            | Self::InvalidNamespaceHeader { .. }
            | Self::InvalidNamespaceName { .. } => tonic::Code::InvalidArgument,
            Self::Planning { source, .. } | Self::Query { source, .. } => {
                datafusion_error_to_tonic_code(&source)
            }
            Self::UnsupportedMessageType { .. } => tonic::Code::Unimplemented,
            Error::FlightSQL { source } => match source {
                flightsql::Error::InvalidHandle { .. }
                | flightsql::Error::Decode { .. }
                | flightsql::Error::Protocol { .. }
                | flightsql::Error::UnsupportedMessageType { .. } => tonic::Code::InvalidArgument,
                flightsql::Error::Flight { source: e } => return tonic::Status::from(e),
                fs_err @ flightsql::Error::Arrow { .. } => {
                    // wrap in Datafusion error to walk source stacks
                    let df_error = DataFusionError::from(fs_err);
                    datafusion_error_to_tonic_code(&df_error)
                }
                flightsql::Error::DataFusion { source } => datafusion_error_to_tonic_code(&source),
            },
            Self::InternalCreatingTicket { .. } | Self::Optimize { .. } => tonic::Code::Internal,
        };

        tonic::Status::new(code, msg)
    }

    fn unsupported_message_type(description: impl Into<String>) -> Self {
        Self::UnsupportedMessageType {
            description: description.into(),
        }
    }
}

impl From<flightsql::Error> for Error {
    fn from(source: flightsql::Error) -> Self {
        Self::FlightSQL { source }
    }
}

type TonicStream<T> = Pin<Box<dyn Stream<Item = Result<T, tonic::Status>> + Send + 'static>>;

/// Concrete implementation of the IOx client protocol, implemented as
/// a gRPC [Arrow Flight] Service API
///
/// # Tickets
///
/// Creating and serializing the `Ticket` structure used in IOx Arrow
/// Flight API is handled by [`IoxGetRequest`]. See that for more
/// details.
///
/// # Native IOx API ad-hoc query
///
/// To run a query with the native IOx API, a client needs to
///
/// 1. Encode the query string as a `Ticket` (see [`IoxGetRequest`]).
///
/// 2. Call the `DoGet` method with the `Ticket`,
///
/// 2. Recieve a stream of data encoded as [`FlightData`]
///
/// ```text
///                                                      .───────.
/// ╔═══════════╗                                       (         )
/// ║           ║                                       │`───────'│
/// ║  Client   ║                                       │   IOx   │
/// ║           ║                                       │.───────.│
/// ║           ║                                       (         )
/// ╚═══════════╝                                        `───────'
///       ┃ Creates a                                        ┃
///     1 ┃ Ticket                                           ┃
///       ┃                                                  ┃
///       ┃                                                  ┃
///     2 ┃                    DoGet(Ticket)                 ┃
///       ┃━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━▶┃
///       ┃                                                  ┃
///       ┃                Stream of FightData               ┃
///     3 ┃◀ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ┃
/// ```
///
/// # FlightSQL
///
/// IOx also supports [Arrow FlightSQL]. In addition to `DoGet`,
/// FlightSQL clients call additional Arrow Flight RPC methods such as
/// `GetFlightInfo`, `GetSchema`, `DoPut`, and `DoAction`.
///
/// ## FlightSQL List tables (NOT YET IMPLEMENTED)
///
/// TODO sequence diagram for List Tables
///
/// ## FlightSQL ad-hoc query
///
/// To run an ad-hoc query, via FlightSQL, the client needs to
///
/// 1. Encode the query in a `CommandStatementQuery` FlightSQL
/// structure in a [`FlightDescriptor`]
///
/// 2. Call the `GetFlightInfo` method with the the [`FlightDescriptor`]
///
/// 3. Receive a `Ticket` in the returned [`FlightInfo`]. The Ticket is
/// opaque (uninterpreted) by the client. It contains an
/// [`IoxGetRequest`] with the `CommandStatementQuery` request.
///
/// 4. Calls the `DoGet` method with the `Ticket` from the previous step.
///
/// 5. Recieve a stream of data encoded as [`FlightData`]
///
/// ```text
///                                                      .───────.
/// ╔═══════════╗                                       (         )
/// ║           ║                                       │`───────'│
/// ║ FlightSQL ║                                       │   IOx   │
/// ║  Client   ║                                       │.───────.│
/// ║           ║                                       (         )
/// ╚═══════════╝                                        `───────'
///       ┃ Creates a                                        ┃
///     1 ┃ CommandStatementQuery                            ┃
///       ┃                                                  ┃
///       ┃                                                  ┃
///       ┃                                                  ┃
///     2 ┃       GetFlightInfo(CommandStatementQuery)       ┃
///       ┃━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━▶┃
///       ┃               FlightInfo{..Ticket{               ┃
///       ┃                CommandStatementQuery             ┃
///     3 ┃◀ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ┃
///       ┃                                                  ┃
///       ┃                                                  ┃
///       ┃                  DoGet(Ticket)                   ┃
///     4 ┃━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━▶┃
///       ┃                                                  ┃
///       ┃                Stream of FightData               ┃
///     5 ┃◀ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ┃
///       ┃                                                  ┃
/// ```
///
/// ## FlightSQL Prepared Statement (no bind parameters like $1, etc)
///
/// To run a prepared query, via FlightSQL, the client undertakes a
/// few more steps:
///
/// 1. Encode the query in a `ActionCreatePreparedStatementRequest`
/// request structure
///
/// 2. Call `DoAction` method with the the request
///
/// 3. Receive a `ActionCreatePreparedStatementResponse`, which contains
/// a prepared statement "handle".
///
/// 4. Encode the handle in a `CommandPreparedStatementQuery`
/// FlightSQL structure in a [`FlightDescriptor`] and call the
/// `GetFlightInfo` method with the the [`FlightDescriptor`]
///
/// 5. Steps 5,6,7 proceed the same as for a FlightSQL ad-hoc query
///
/// ```text
///                                                      .───────.
/// ╔═══════════╗                                       (         )
/// ║           ║                                       │`───────'│
/// ║ FlightSQL ║                                       │   IOx   │
/// ║  Client   ║                                       │.───────.│
/// ║           ║                                       (         )
/// ╚═══════════╝                                        `───────'
///       ┃ Creates                                          ┃
///     1 ┃ ActionCreatePreparedStatementRequest             ┃
///       ┃                                                  ┃
///       ┃                                                  ┃
///       ┃  DoAction(ActionCreatePreparedStatementRequest)  ┃
///     2 ┃━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━▶┃
///       ┃                                                  ┃
///       ┃  Result(ActionCreatePreparedStatementResponse)   ┃
///     3 ┃◀ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ┃
///       ┃                                                  ┃
///       ┃  GetFlightInfo(CommandPreparedStatementQuery)    ┃
///     4 ┃━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━▶┃
///       ┃  FlightInfo(..Ticket{                            ┃
///       ┃     CommandPreparedStatementQuery})              ┃
///     5 ┃◀ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ┃
///       ┃                                                  ┃
///       ┃                  DoGet(Ticket)                   ┃
///     6 ┃━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━▶┃
///       ┃                                                  ┃
///       ┃                Stream of FightData               ┃
///     7 ┃◀ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ┃
/// ```
///
/// [Arrow Flight]: https://arrow.apache.org/docs/format/Flight.html
/// [Arrow FlightSQL]: https://arrow.apache.org/docs/format/FlightSql.html
#[derive(Debug)]
struct FlightService<S>
where
    S: QueryNamespaceProvider,
{
    server: Arc<S>,
}

pub fn make_server<S>(server: Arc<S>) -> FlightServer<impl Flight>
where
    S: QueryNamespaceProvider,
{
    FlightServer::new(FlightService { server })
}

impl<S> FlightService<S>
where
    S: QueryNamespaceProvider,
{
    /// Implementation of the `DoGet` method
    async fn run_do_get(
        &self,
        span_ctx: Option<SpanContext>,
        permit: InstrumentedAsyncOwnedSemaphorePermit,
        query: &RunQuery,
        namespace: String,
    ) -> Result<Response<TonicStream<FlightData>>, tonic::Status> {
        let db = self
            .server
            .db(&namespace, span_ctx.child_span("get namespace"))
            .await
            .context(NamespaceNotFoundSnafu {
                namespace_name: &namespace,
            })?;

        let ctx = db.new_query_context(span_ctx);
        let (query_completed_token, physical_plan) = match query {
            RunQuery::Sql(sql_query) => {
                let token = db.record_query(&ctx, "sql", Box::new(sql_query.clone()));
                let plan = Planner::new(&ctx)
                    .sql(sql_query)
                    .await
                    .context(PlanningSnafu)?;
                (token, plan)
            }
            RunQuery::InfluxQL(sql_query) => {
                let token = db.record_query(&ctx, "influxql", Box::new(sql_query.clone()));
                let plan = Planner::new(&ctx)
                    .influxql(sql_query)
                    .await
                    .context(PlanningSnafu)?;
                (token, plan)
            }
            RunQuery::FlightSQL(msg) => {
                let token = db.record_query(&ctx, "flightsql", Box::new(msg.to_string()));
                let plan = Planner::new(&ctx)
                    .flight_sql_do_get(&namespace, db, msg.clone())
                    .await
                    .context(PlanningSnafu)?;
                (token, plan)
            }
        };

        let output =
            GetStream::new(ctx, physical_plan, namespace, query_completed_token, permit).await?;

        Ok(Response::new(Box::pin(output) as TonicStream<FlightData>))
    }
}

#[tonic::async_trait]
impl<S> Flight for FlightService<S>
where
    S: QueryNamespaceProvider,
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
        Err(tonic::Status::unimplemented(
            "Not yet implemented: get_schema",
        ))
    }

    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, tonic::Status> {
        let external_span_ctx: Option<RequestLogContext> = request.extensions().get().cloned();
        let trace = external_span_ctx.format_jaeger();
        let span_ctx: Option<SpanContext> = request.extensions().get().cloned();
        let ticket = request.into_inner();

        // attempt to decode ticket
        let request = IoxGetRequest::try_decode(ticket).context(InvalidTicketSnafu);

        if let Err(e) = &request {
            info!(%e, "Error decoding Flight API ticket");
        };

        let request = request?;
        let namespace_name = request.namespace_name();
        let query = request.query();

        let permit = self
            .server
            .acquire_semaphore(span_ctx.child_span("query rate limit semaphore"))
            .await;

        // Log after we acquire the permit and are about to start execution
        let start = Instant::now();
        info!(
            %namespace_name,
            %query,
            %trace,
            variant=query.variant(),
            "DoGet request",
        );

        let response = self
            .run_do_get(span_ctx, permit, query, namespace_name.to_string())
            .await;

        if let Err(e) = &response {
            info!(%namespace_name, %query, %trace, %e, "Error running DoGet");
        } else {
            let elapsed = Instant::now() - start;
            debug!(%namespace_name, %query, %trace, ?elapsed, "Completed DoGet request");
        }
        response
    }

    async fn handshake(
        &self,
        request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, tonic::Status> {
        let request = request
            .into_inner()
            .message()
            .await?
            .context(InvalidHandshakeSnafu)?;

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
        Err(tonic::Status::unimplemented(
            "Not yet implemented: list_flights",
        ))
    }

    /// Handles `GetFlightInfo` RPC requests. The [`FlightDescriptor`]
    /// is treated containing an FlightSQL command, encoded as a binary
    /// ProtoBuf message.
    ///
    /// see [`FlightService`] for more details.
    async fn get_flight_info(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, tonic::Status> {
        let external_span_ctx: Option<RequestLogContext> = request.extensions().get().cloned();
        let span_ctx: Option<SpanContext> = request.extensions().get().cloned();
        let trace = external_span_ctx.format_jaeger();

        let namespace_name = get_flightsql_namespace(request.metadata())?;
        let flight_descriptor = request.into_inner();

        // extract the FlightSQL message
        let cmd = cmd_from_descriptor(flight_descriptor.clone())?;

        info!(%namespace_name, %cmd, %trace, "GetFlightInfo request");

        let db = self
            .server
            .db(&namespace_name, span_ctx.child_span("get namespace"))
            .await
            .context(NamespaceNotFoundSnafu {
                namespace_name: &namespace_name,
            })?;

        let ctx = db.new_query_context(span_ctx);
        let schema = Planner::new(&ctx)
            .flight_sql_get_flight_info(&namespace_name, cmd.clone())
            .await
            .context(PlanningSnafu);

        if let Err(e) = &schema {
            info!(%namespace_name, %cmd, %trace, %e, "Error running GetFlightInfo");
        } else {
            debug!(%namespace_name, %cmd, %trace, "Completed GetFlightInfo request");
        };
        let schema = schema?;

        // Form the response ticket (that the client will pass back to DoGet)
        let ticket = IoxGetRequest::new(&namespace_name, RunQuery::FlightSQL(cmd))
            .try_encode()
            .context(InternalCreatingTicketSnafu)?;

        // Flight says "Set these to -1 if unknown."
        //
        // https://github.com/apache/arrow-rs/blob/a0a5880665b1836890f6843b6b8772d81c463351/format/Flight.proto#L274-L276
        let total_records = -1;
        let total_bytes = -1;

        let endpoint = vec![FlightEndpoint {
            ticket: Some(ticket),
            // "If the list is empty, the expectation is that the
            // ticket can only be redeemed on the current service
            // where the ticket was generated."
            //
            // https://github.com/apache/arrow-rs/blob/a0a5880665b1836890f6843b6b8772d81c463351/format/Flight.proto#L292-L294
            location: vec![],
        }];

        let flight_info = FlightInfo {
            schema,
            // return descriptor we were passed
            flight_descriptor: Some(flight_descriptor),
            endpoint,
            total_records,
            total_bytes,
        };

        Ok(tonic::Response::new(flight_info))
    }

    async fn do_put(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, tonic::Status> {
        info!("Handling flightsql do_put body");

        Err(tonic::Status::unimplemented("Not yet implemented: do_put"))
    }

    async fn do_action(
        &self,
        request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, tonic::Status> {
        let external_span_ctx: Option<RequestLogContext> = request.extensions().get().cloned();
        let span_ctx: Option<SpanContext> = request.extensions().get().cloned();
        let trace = external_span_ctx.format_jaeger();

        let namespace_name = get_flightsql_namespace(request.metadata())?;
        let Action {
            r#type: action_type,
            body,
        } = request.into_inner();

        // extract the FlightSQL message
        let cmd = FlightSQLCommand::try_decode(body).context(FlightSQLSnafu)?;

        info!(%namespace_name, %action_type, %cmd, %trace, "DoAction request");

        let db = self
            .server
            .db(&namespace_name, span_ctx.child_span("get namespace"))
            .await
            .context(NamespaceNotFoundSnafu {
                namespace_name: &namespace_name,
            })?;

        let ctx = db.new_query_context(span_ctx);
        let body = Planner::new(&ctx)
            .flight_sql_do_action(&namespace_name, db, cmd.clone())
            .await
            .context(PlanningSnafu);

        if let Err(e) = &body {
            info!(%namespace_name, %cmd, %trace, %e, "Error running DoAction");
        } else {
            debug!(%namespace_name, %cmd, %trace, "Completed DoAction request");
        };

        let result = arrow_flight::Result { body: body? };
        let stream = futures::stream::iter([Ok(result)]);

        Ok(Response::new(stream.boxed()))
    }

    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, tonic::Status> {
        Err(tonic::Status::unimplemented(
            "Not yet implemented: list_actions",
        ))
    }

    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, tonic::Status> {
        Err(tonic::Status::unimplemented(
            "Not yet implemented: do_exchange",
        ))
    }
}

/// Extracts an encoded Protobuf message from a [`FlightDescriptor`],
/// as used in FlightSQL.
fn cmd_from_descriptor(flight_descriptor: FlightDescriptor) -> Result<FlightSQLCommand> {
    match flight_descriptor.r#type() {
        DescriptorType::Cmd => Ok(FlightSQLCommand::try_decode(flight_descriptor.cmd)?),
        DescriptorType::Path => Err(Error::unsupported_message_type("FlightInfo with Path")),
        DescriptorType::Unknown => Err(Error::unsupported_message_type(
            "FlightInfo of unknown type",
        )),
    }
}

/// Figure out the namespace for this request by checking
/// the "iox-namespace-name=the_name";
fn get_flightsql_namespace(metadata: &MetadataMap) -> Result<String> {
    if let Some(v) = metadata.get(IOX_FLIGHT_SQL_NAMESPACE_HEADER) {
        let v = v.to_str().context(InvalidNamespaceHeaderSnafu)?;
        return Ok(v.to_string());
    }

    NoFlightSQLNamespaceSnafu.fail()
}

/// Wrapper over a FlightDataEncodeStream that adds IOx specfic
/// metadata and records completion
struct GetStream {
    inner: IOxFlightDataEncoder,
    #[allow(dead_code)]
    permit: InstrumentedAsyncOwnedSemaphorePermit,
    query_completed_token: QueryCompletedToken,
    done: bool,
}

impl GetStream {
    async fn new(
        ctx: IOxSessionContext,
        physical_plan: Arc<dyn ExecutionPlan>,
        namespace_name: String,
        query_completed_token: QueryCompletedToken,
        permit: InstrumentedAsyncOwnedSemaphorePermit,
    ) -> Result<Self, tonic::Status> {
        let app_metadata = proto::AppMetadata {};

        let schema = physical_plan.schema();

        let query_results = ctx
            .execute_stream(Arc::clone(&physical_plan))
            .await
            .context(QuerySnafu {
                namespace_name: namespace_name.clone(),
            })?
            .map_err(|e| {
                let code = datafusion_error_to_tonic_code(&e);
                tonic::Status::new(code, e.to_string()).into()
            });

        // setup inner stream
        let inner = IOxFlightDataEncoderBuilder::new(schema)
            .with_metadata(app_metadata.encode_to_vec().into())
            .build(query_results);

        Ok(Self {
            inner,
            permit,
            query_completed_token,
            done: false,
        })
    }
}

/// workaround for <https://github.com/apache/arrow-rs/issues/3591>
///
/// data encoder stream that always sends a Schema message even if the
/// underlying stream is empty
struct IOxFlightDataEncoder {
    inner: FlightDataEncoder,
    // The schema of the inner stream. Set to None when a schema
    // message has been sent.
    schema: Option<SchemaRef>,
    done: bool,
}

impl IOxFlightDataEncoder {
    fn new(inner: FlightDataEncoder, schema: SchemaRef) -> Self {
        Self {
            inner,
            schema: Some(schema),
            done: false,
        }
    }
}

#[derive(Debug)]
struct IOxFlightDataEncoderBuilder {
    inner: FlightDataEncoderBuilder,
    schema: SchemaRef,
}

impl IOxFlightDataEncoderBuilder {
    fn new(schema: SchemaRef) -> Self {
        Self {
            inner: FlightDataEncoderBuilder::new(),
            schema: prepare_schema_for_flight(schema),
        }
    }

    pub fn with_metadata(mut self, app_metadata: Bytes) -> Self {
        self.inner = self.inner.with_metadata(app_metadata);
        self
    }

    pub fn build<S>(self, input: S) -> IOxFlightDataEncoder
    where
        S: Stream<Item = arrow_flight::error::Result<RecordBatch>> + Send + 'static,
    {
        let Self { inner, schema } = self;

        IOxFlightDataEncoder::new(inner.build(input), schema)
    }
}

impl Stream for IOxFlightDataEncoder {
    type Item = arrow_flight::error::Result<FlightData>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        loop {
            if self.done {
                return Poll::Ready(None);
            }

            let res = ready!(self.inner.poll_next_unpin(cx));
            match res {
                None => {
                    self.done = true;
                    // return a schema message if we haven't sent any data
                    if let Some(schema) = self.schema.take() {
                        let options = IpcWriteOptions::default();
                        let data: FlightData = SchemaAsIpc::new(schema.as_ref(), &options).into();
                        return Poll::Ready(Some(Ok(data)));
                    }
                }
                Some(Ok(data)) => {
                    // If any data is returned from the underlying stream no need to resend schema
                    self.schema = None;
                    return Poll::Ready(Some(Ok(data)));
                }
                Some(Err(e)) => {
                    self.done = true;
                    return Poll::Ready(Some(Err(e)));
                }
            }
        }
    }
}

/// Prepare an arrow Schema for transport over the Arrow Flight protocol
///
/// Convert dictionary types to underlying types
///
/// See hydrate_dictionary for more information
fn prepare_schema_for_flight(schema: SchemaRef) -> SchemaRef {
    let fields = schema
        .fields()
        .iter()
        .map(|field| match field.data_type() {
            DataType::Dictionary(_, value_type) => Field::new(
                field.name(),
                value_type.as_ref().clone(),
                field.is_nullable(),
            )
            .with_metadata(field.metadata().clone()),
            _ => field.clone(),
        })
        .collect();

    Arc::new(Schema::new(fields))
}

impl Stream for GetStream {
    type Item = Result<FlightData, tonic::Status>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        loop {
            if self.done {
                return Poll::Ready(None);
            }

            let res = ready!(self.inner.poll_next_unpin(cx));
            match res {
                None => {
                    self.done = true;
                    // if we get here, all is good
                    self.query_completed_token.set_success();
                }
                Some(Ok(data)) => {
                    return Poll::Ready(Some(Ok(data)));
                }
                Some(Err(e)) => {
                    self.done = true;
                    return Poll::Ready(Some(Err(e.into())));
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use futures::Future;
    use metric::{Attributes, Metric, U64Gauge};
    use service_common::test_util::TestDatabaseStore;
    use tokio::pin;

    use super::*;

    #[tokio::test]
    async fn test_query_semaphore() {
        let semaphore_size = 2;
        let test_storage = Arc::new(TestDatabaseStore::new_with_semaphore_size(semaphore_size));

        assert_semaphore_metric(
            &test_storage.metric_registry,
            "iox_async_semaphore_permits_total",
            2,
        );
        assert_semaphore_metric(
            &test_storage.metric_registry,
            "iox_async_semaphore_permits_pending",
            0,
        );
        assert_semaphore_metric(
            &test_storage.metric_registry,
            "iox_async_semaphore_permits_acquired",
            0,
        );

        // add some data
        test_storage.db_or_create("my_db").await;

        let service = FlightService {
            server: Arc::clone(&test_storage),
        };
        let ticket = Ticket {
            ticket: br#"{"namespace_name": "my_db", "sql_query": "SELECT 1;"}"#
                .to_vec()
                .into(),
        };
        let streaming_resp1 = service
            .do_get(tonic::Request::new(ticket.clone()))
            .await
            .unwrap();

        assert_semaphore_metric(
            &test_storage.metric_registry,
            "iox_async_semaphore_permits_total",
            2,
        );
        assert_semaphore_metric(
            &test_storage.metric_registry,
            "iox_async_semaphore_permits_pending",
            0,
        );
        assert_semaphore_metric(
            &test_storage.metric_registry,
            "iox_async_semaphore_permits_acquired",
            1,
        );

        let streaming_resp2 = service
            .do_get(tonic::Request::new(ticket.clone()))
            .await
            .unwrap();

        assert_semaphore_metric(
            &test_storage.metric_registry,
            "iox_async_semaphore_permits_total",
            2,
        );
        assert_semaphore_metric(
            &test_storage.metric_registry,
            "iox_async_semaphore_permits_pending",
            0,
        );
        assert_semaphore_metric(
            &test_storage.metric_registry,
            "iox_async_semaphore_permits_acquired",
            2,
        );

        // 3rd request is pending
        let fut = service.do_get(tonic::Request::new(ticket.clone()));
        pin!(fut);
        assert_fut_pending(&mut fut).await;

        assert_semaphore_metric(
            &test_storage.metric_registry,
            "iox_async_semaphore_permits_total",
            2,
        );
        assert_semaphore_metric(
            &test_storage.metric_registry,
            "iox_async_semaphore_permits_pending",
            1,
        );
        assert_semaphore_metric(
            &test_storage.metric_registry,
            "iox_async_semaphore_permits_acquired",
            2,
        );

        // free permit
        drop(streaming_resp1);
        let streaming_resp3 = fut.await;

        assert_semaphore_metric(
            &test_storage.metric_registry,
            "iox_async_semaphore_permits_total",
            2,
        );
        assert_semaphore_metric(
            &test_storage.metric_registry,
            "iox_async_semaphore_permits_pending",
            0,
        );
        assert_semaphore_metric(
            &test_storage.metric_registry,
            "iox_async_semaphore_permits_acquired",
            2,
        );

        drop(streaming_resp2);
        drop(streaming_resp3);

        assert_semaphore_metric(
            &test_storage.metric_registry,
            "iox_async_semaphore_permits_total",
            2,
        );
        assert_semaphore_metric(
            &test_storage.metric_registry,
            "iox_async_semaphore_permits_pending",
            0,
        );
        assert_semaphore_metric(
            &test_storage.metric_registry,
            "iox_async_semaphore_permits_acquired",
            0,
        );
    }

    /// Assert that given future is pending.
    ///
    /// This will try to poll the future a bit to ensure that it is not stuck in tokios task preemption.
    async fn assert_fut_pending<F>(fut: &mut F)
    where
        F: Future + Send + Unpin,
    {
        tokio::select! {
            _ = fut => panic!("future is not pending, yielded"),
            _ = tokio::time::sleep(std::time::Duration::from_millis(10)) => {},
        };
    }

    fn assert_semaphore_metric(registry: &metric::Registry, name: &'static str, expected: u64) {
        let actual = registry
            .get_instrument::<Metric<U64Gauge>>(name)
            .expect("failed to read metric")
            .get_observer(&Attributes::from(&[("semaphore", "query_execution")]))
            .expect("failed to get observer")
            .fetch();
        assert_eq!(actual, expected);
    }
}
