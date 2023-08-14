//! Implements the InfluxDB IOx "Native" Flight API and Arrow
//! FlightSQL, based on Arrow Flight and gRPC. See [`FlightService`]
//! for full detail.

#![deny(rustdoc::broken_intra_doc_links, rustdoc::bare_urls, rust_2018_idioms)]
#![allow(clippy::clone_on_ref_ptr)]
#![warn(
    missing_copy_implementations,
    missing_debug_implementations,
    clippy::explicit_iter_loop,
    // See https://github.com/influxdata/influxdb_iox/pull/1671
    clippy::future_not_send,
    clippy::clone_on_ref_ptr,
    clippy::todo,
    clippy::dbg_macro,
    unused_crate_dependencies
)]

use keep_alive::KeepAliveStream;
// Workaround for "unused crate" lint false positives.
use workspace_hack as _;

mod keep_alive;
mod request;

use arrow::error::ArrowError;
use arrow_flight::{
    encode::FlightDataEncoderBuilder,
    flight_descriptor::DescriptorType,
    flight_service_server::{FlightService as Flight, FlightServiceServer as FlightServer},
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightEndpoint, FlightInfo,
    HandshakeRequest, HandshakeResponse, PutResult, SchemaResult, Ticket,
};
use authz::{extract_token, Authorizer};
use data_types::NamespaceNameError;
use datafusion::{error::DataFusionError, physical_plan::ExecutionPlan};
use flightsql::FlightSQLCommand;
use futures::{ready, Stream, StreamExt, TryStreamExt};
use generated_types::influxdata::iox::querier::v1 as proto;
use iox_query::{exec::IOxSessionContext, QueryCompletedToken, QueryNamespace};
use observability_deps::tracing::{debug, info, warn};
use prost::Message;
use request::{IoxGetRequest, RunQuery};
use service_common::{datafusion_error_to_tonic_code, planner::Planner, QueryNamespaceProvider};
use snafu::{OptionExt, ResultExt, Snafu};
use std::{
    fmt::Debug,
    pin::Pin,
    sync::Arc,
    task::Poll,
    time::{Duration, Instant},
};
use tonic::{
    metadata::{AsciiMetadataValue, MetadataMap},
    Request, Response, Streaming,
};
use trace::{ctx::SpanContext, span::SpanExt};
use trace_http::ctx::{RequestLogContext, RequestLogContextExt};
use tracker::InstrumentedAsyncOwnedSemaphorePermit;

/// The supported names of the grpc header that contain the target database
/// for FlightSQL requests.
///
/// See <https://lists.apache.org/thread/fd6r1n7vt91sg2c7fr35wcrsqz6x4645>
/// for discussion on adding support to FlightSQL itself.
const IOX_FLIGHT_SQL_DATABASE_HEADERS: [&str; 4] = [
    "database", // preferred
    "bucket",
    "bucket-name",
    "iox-namespace-name", // deprecated
];

/// In which interval should the `DoGet` stream send empty messages as keep alive markers?
const DO_GET_KEEP_ALIVE_INTERVAL: Duration = Duration::from_secs(5);

#[allow(clippy::enum_variant_names)]
#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Invalid ticket. Error: {}", source))]
    InvalidTicket { source: request::Error },

    #[snafu(display("Internal creating encoding ticket: {}", source))]
    InternalCreatingTicket { source: request::Error },

    #[snafu(display("Invalid handshake. No payload provided"))]
    InvalidHandshake {},

    #[snafu(display("Database '{}' not found", namespace_name))]
    DatabaseNotFound { namespace_name: String },

    #[snafu(display(
        "Internal error reading points from namespace {}: {}",
        namespace_name,
        source
    ))]
    Query {
        namespace_name: String,
        query: String,
        source: DataFusionError,
    },

    #[snafu(display(
        "More than one headers are found in request: {:?}. \
    Please include only one of them",
        header_names
    ))]
    TooManyFlightSQLDatabases { header_names: Vec<String> },

    #[snafu(display("no 'database' header in request"))]
    NoFlightSQLDatabase,

    #[snafu(display("Invalid 'database' header in request: {}", source))]
    InvalidDatabaseHeader {
        source: tonic::metadata::errors::ToStrError,
    },

    #[snafu(display("Invalid database name: {}", source))]
    InvalidDatabaseName { source: NamespaceNameError },

    #[snafu(display("Failed to optimize record batch: {}", source))]
    Optimize { source: ArrowError },

    #[snafu(display("Failed to encode schema: {}", source))]
    EncodeSchema { source: ArrowError },

    #[snafu(display("Error while planning query: {}", source))]
    Planning {
        namespace_name: String,
        query: String,
        source: service_common::planner::Error,
    },

    #[snafu(display("Error while planning Flight SQL : {}", source))]
    FlightSQL { source: flightsql::Error },

    #[snafu(display("Invalid protobuf: {}", source))]
    Deserialization { source: prost::DecodeError },

    #[snafu(display("Unsupported message type: {}", description))]
    UnsupportedMessageType { description: String },

    #[snafu(display("Unauthenticated"))]
    Unauthenticated,

    #[snafu(display("Permission denied"))]
    PermissionDenied,

    #[snafu(display("Authz error: {}", source))]
    Authz { source: authz::Error },
}
pub type Result<T, E = Error> = std::result::Result<T, E>;

impl From<Error> for tonic::Status {
    /// Converts a result from the business logic into the appropriate tonic
    /// status
    fn from(err: Error) -> Self {
        // An explicit match on the Error enum will ensure appropriate
        // logging is handled for any new error variants.
        let msg = "Error handling Flight gRPC request";
        let namespace = err.namespace();
        let query = err.query();
        match err {
            Error::DatabaseNotFound { .. }
            | Error::InvalidTicket { .. }
            | Error::InvalidHandshake { .. }
            | Error::Unauthenticated { .. }
            | Error::PermissionDenied { .. }
            | Error::InvalidDatabaseName { .. }
            | Error::Query { .. } => info!(e=%err, %namespace, %query, msg),
            Error::Optimize { .. }
            | Error::EncodeSchema { .. }
            | Error::TooManyFlightSQLDatabases { .. }
            | Error::NoFlightSQLDatabase
            | Error::InvalidDatabaseHeader { .. }
            | Error::Planning { .. }
            | Error::Deserialization { .. }
            | Error::InternalCreatingTicket { .. }
            | Error::UnsupportedMessageType { .. }
            | Error::FlightSQL { .. }
            | Error::Authz { .. } => {
                warn!(e=%err, %namespace, %query, msg)
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
            Self::DatabaseNotFound { .. } => tonic::Code::NotFound,
            Self::InvalidTicket { .. }
            | Self::InvalidHandshake { .. }
            | Self::Deserialization { .. }
            | Self::TooManyFlightSQLDatabases { .. }
            | Self::NoFlightSQLDatabase
            | Self::InvalidDatabaseHeader { .. }
            | Self::InvalidDatabaseName { .. } => tonic::Code::InvalidArgument,
            Self::Planning { source, .. } | Self::Query { source, .. } => {
                datafusion_error_to_tonic_code(&source)
            }
            Self::UnsupportedMessageType { .. } => tonic::Code::Unimplemented,
            Self::FlightSQL { source } => match source {
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
            Self::InternalCreatingTicket { .. }
            | Self::Optimize { .. }
            | Self::EncodeSchema { .. }
            | Self::Authz { .. } => tonic::Code::Internal,
            Self::Unauthenticated => tonic::Code::Unauthenticated,
            Self::PermissionDenied => tonic::Code::PermissionDenied,
        };

        tonic::Status::new(code, msg)
    }

    /// returns the namespace name, if known, used for logging
    fn namespace(&self) -> &str {
        match self {
            Error::InvalidTicket { .. }
            | Error::InternalCreatingTicket { .. }
            | Error::InvalidHandshake {}
            | Error::TooManyFlightSQLDatabases { .. }
            | Error::NoFlightSQLDatabase
            | Error::InvalidDatabaseHeader { .. }
            | Error::InvalidDatabaseName { .. }
            | Error::Optimize { .. }
            | Error::EncodeSchema { .. }
            | Error::FlightSQL { .. }
            | Error::Deserialization { .. }
            | Error::UnsupportedMessageType { .. }
            | Error::Unauthenticated
            | Error::PermissionDenied
            | Error::Authz { .. } => "<unknown>",
            Error::DatabaseNotFound { namespace_name } => namespace_name,
            Error::Query { namespace_name, .. } => namespace_name,
            Error::Planning { namespace_name, .. } => namespace_name,
        }
    }

    /// returns a query, if know, used for logging
    fn query(&self) -> &str {
        match self {
            Error::InvalidTicket { .. }
            | Error::InternalCreatingTicket { .. }
            | Error::InvalidHandshake {}
            | Error::TooManyFlightSQLDatabases { .. }
            | Error::NoFlightSQLDatabase
            | Error::InvalidDatabaseHeader { .. }
            | Error::InvalidDatabaseName { .. }
            | Error::Optimize { .. }
            | Error::EncodeSchema { .. }
            | Error::FlightSQL { .. }
            | Error::Deserialization { .. }
            | Error::UnsupportedMessageType { .. }
            | Error::Unauthenticated
            | Error::PermissionDenied
            | Error::Authz { .. }
            | Error::DatabaseNotFound { .. } => "NONE",
            Error::Query { query, .. } => query,
            Error::Planning { query, .. } => query,
        }
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

impl From<authz::Error> for Error {
    fn from(source: authz::Error) -> Self {
        match source {
            authz::Error::Forbidden => Self::PermissionDenied,
            authz::Error::InvalidToken => Self::PermissionDenied,
            authz::Error::NoToken => Self::Unauthenticated,
            source => Self::Authz { source },
        }
    }
}

type TonicStream<T> = Pin<Box<dyn Stream<Item = Result<T, tonic::Status>> + Send + 'static>>;

/// Concrete implementation of the IOx client protocol, implemented as
/// a gRPC [Arrow Flight] Service API
///
/// Perhaps confusingly, this service also implements [FlightSQL] in
/// addition to the IOx client protocol. This is done so clients can
/// use the same Arrow Flight endpoint for either protocol. The
/// difference between the two protocols is the specific messages
/// passed to the Flight APIs (e.g. `DoGet` or `GetFlightInfo`).
///
/// The only way to run InfluxQL queries is to use the IOx client
/// protocol. SQL queries can be run either using the IOx client
/// protocol or FlightSQL.
///
/// Because FlightSQL is SQL specific, there is no way to specify a
/// different language or dialect, and clients expect SQL semantics,
/// thus it doesn't make sense to run InfluxQL over FlightSQL.
///
/// [FlightSQL]: https://arrow.apache.org/docs/format/FlightSql.html
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
    authz: Option<Arc<dyn Authorizer>>,
}

pub fn make_server<S>(
    server: Arc<S>,
    authz: Option<Arc<dyn Authorizer>>,
) -> FlightServer<impl Flight>
where
    S: QueryNamespaceProvider,
{
    FlightServer::new(FlightService { server, authz })
}

impl<S> FlightService<S>
where
    S: QueryNamespaceProvider,
{
    /// Implementation of the `DoGet` method
    async fn run_do_get(
        &self,
        span_ctx: Option<SpanContext>,
        trace: String,
        permit: InstrumentedAsyncOwnedSemaphorePermit,
        query: RunQuery,
        namespace_name: String,
        is_debug: bool,
    ) -> Result<Response<TonicStream<FlightData>>, tonic::Status> {
        let db = self
            .server
            .db(
                &namespace_name,
                span_ctx.child_span("get namespace"),
                is_debug,
            )
            .await
            .context(DatabaseNotFoundSnafu {
                namespace_name: &namespace_name,
            })?;

        let ctx = db.new_query_context(span_ctx);
        let (query_completed_token, physical_plan) = match &query {
            RunQuery::Sql(sql_query) => {
                let token = db.record_query(&ctx, "sql", Box::new(sql_query.clone()));
                let plan = Planner::new(&ctx)
                    .sql(sql_query)
                    .await
                    .context(PlanningSnafu {
                        namespace_name: &namespace_name,
                        query: query.to_string(),
                    })?;
                (token, plan)
            }
            RunQuery::InfluxQL(sql_query) => {
                let token = db.record_query(&ctx, "influxql", Box::new(sql_query.clone()));
                let plan = Planner::new(&ctx)
                    .influxql(sql_query)
                    .await
                    .context(PlanningSnafu {
                        namespace_name: &namespace_name,
                        query: query.to_string(),
                    })?;
                (token, plan)
            }
            RunQuery::FlightSQL(msg) => {
                let token = db.record_query(&ctx, "flightsql", Box::new(msg.to_string()));
                let plan = Planner::new(&ctx)
                    .flight_sql_do_get(&namespace_name, db, msg.clone())
                    .await
                    .context(PlanningSnafu {
                        namespace_name: &namespace_name,
                        query: query.to_string(),
                    })?;
                (token, plan)
            }
        };

        let output = GetStream::new(
            ctx,
            physical_plan,
            namespace_name.to_string(),
            &query,
            query_completed_token,
            permit,
        )
        .await?;

        // Log any error that happens *during* execution (other error
        // handling in this file happen during planning)
        let output = output.map(move |res| {
            if let Err(e) = &res {
                info!(%namespace_name, %query, %trace, %e, "Error executing query via DoGet");
            }
            res
        });

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
        let authz_token = get_flight_authz(request.metadata());
        let mut is_debug = has_debug_header(request.metadata());
        let ticket = request.into_inner();

        // attempt to decode ticket
        let request = IoxGetRequest::try_decode(ticket).context(InvalidTicketSnafu);

        if let Err(e) = &request {
            info!(%e, "Error decoding Flight API ticket");
        };

        let request = request?;
        let namespace_name = request.database();
        let query = request.query();
        is_debug |= request.is_debug();

        let perms = match query {
            RunQuery::FlightSQL(cmd) => flightsql_permissions(namespace_name, cmd),
            RunQuery::Sql(_) | RunQuery::InfluxQL(_) => vec![authz::Permission::ResourceAction(
                authz::Resource::Database(namespace_name.to_string()),
                authz::Action::Read,
            )],
        };
        self.authz
            .permissions(authz_token, &perms)
            .await
            .map_err(Error::from)?;

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
            .run_do_get(
                span_ctx,
                trace.clone(),
                permit,
                query.clone(),
                namespace_name.to_string(),
                is_debug,
            )
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
        // Note that the JDBC driver doesn't send the iox-namespace-name metadata
        // in the handshake request, even if configured in the JDBC URL,
        // so we cannot actually do any access checking here.
        let authz_token = get_flight_authz(request.metadata());

        let request = request
            .into_inner()
            .message()
            .await?
            .context(InvalidHandshakeSnafu)?;

        // The handshake method is used for authentication. IOx ignores the
        // username and returns the password itself as the token to use for
        // subsequent requests
        let response_header = authz_token
            .map(|mut v| {
                let mut nv = b"Bearer ".to_vec();
                nv.append(&mut v);
                nv
            })
            .map(AsciiMetadataValue::try_from)
            .transpose()
            .map_err(|e| tonic::Status::invalid_argument(e.to_string()))?;

        let response = HandshakeResponse {
            protocol_version: request.protocol_version,
            payload: request.payload,
        };
        let output = futures::stream::iter(std::iter::once(Ok(response)));
        let mut response = Response::new(Box::pin(output) as Self::HandshakeStream);
        if let Some(header) = response_header {
            response.metadata_mut().insert("authorization", header);
        }
        Ok(response)
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
        let is_debug = has_debug_header(request.metadata());

        let namespace_name = get_flightsql_namespace(request.metadata())?;
        let authz_token = get_flight_authz(request.metadata());
        let flight_descriptor = request.into_inner();

        // extract the FlightSQL message
        let cmd = cmd_from_descriptor(flight_descriptor.clone())?;
        info!(%namespace_name, %cmd, %trace, "GetFlightInfo request");

        let perms = flightsql_permissions(&namespace_name, &cmd);
        self.authz
            .permissions(authz_token, &perms)
            .await
            .map_err(Error::from)?;

        let db = self
            .server
            .db(
                &namespace_name,
                span_ctx.child_span("get namespace"),
                is_debug,
            )
            .await
            .context(DatabaseNotFoundSnafu {
                namespace_name: &namespace_name,
            })?;

        let ctx = db.new_query_context(span_ctx);
        let schema = Planner::new(&ctx)
            .flight_sql_get_flight_info_schema(&namespace_name, cmd.clone())
            .await
            .context(PlanningSnafu {
                namespace_name: &namespace_name,
                query: format!("{cmd:?}"),
            });

        if let Err(e) = &schema {
            info!(%namespace_name, %cmd, %trace, %e, "Error running GetFlightInfo");
        } else {
            debug!(%namespace_name, %cmd, %trace, "Completed GetFlightInfo request");
        };
        let schema = schema?;

        // Form the response ticket (that the client will pass back to DoGet)
        let ticket = IoxGetRequest::new(&namespace_name, RunQuery::FlightSQL(cmd), is_debug)
            .try_encode()
            .context(InternalCreatingTicketSnafu)?;

        let endpoint = FlightEndpoint::new().with_ticket(ticket);

        let flight_info = FlightInfo::new()
            .with_endpoint(endpoint)
            // return descriptor we were passed
            .with_descriptor(flight_descriptor)
            .try_with_schema(schema.as_ref())
            .context(EncodeSchemaSnafu)?;

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
        let is_debug = has_debug_header(request.metadata());

        let namespace_name = get_flightsql_namespace(request.metadata())?;
        let authz_token = get_flight_authz(request.metadata());
        let Action {
            r#type: action_type,
            body,
        } = request.into_inner();

        // extract the FlightSQL message
        let cmd = FlightSQLCommand::try_decode(body).context(FlightSQLSnafu)?;

        info!(%namespace_name, %action_type, %cmd, %trace, "DoAction request");

        let perms = flightsql_permissions(&namespace_name, &cmd);
        self.authz
            .permissions(authz_token, &perms)
            .await
            .map_err(Error::from)?;

        let db = self
            .server
            .db(
                &namespace_name,
                span_ctx.child_span("get namespace"),
                is_debug,
            )
            .await
            .context(DatabaseNotFoundSnafu {
                namespace_name: &namespace_name,
            })?;

        let ctx = db.new_query_context(span_ctx);
        let body = Planner::new(&ctx)
            .flight_sql_do_action(&namespace_name, db, cmd.clone())
            .await
            .context(PlanningSnafu {
                namespace_name: &namespace_name,
                query: format!("{cmd:?}"),
            })?;

        let result = arrow_flight::Result { body };
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

/// Figure out the database for this request by checking
/// the "database=database_or_bucket_name" (preferred)
/// or "bucket=database_or_bucket_name"
/// or "bucket-name=database_or_bucket_name"
/// or "iox-namespace-name=the_name" (deprecated);
///
/// Only one of the keys is accepted.
///
/// Note that `iox-namespace-name` is still accepted (rather than error) for
/// some period of time until we are sure that all other software speaking
/// FlightSQL is using the new header names.
fn get_flightsql_namespace(metadata: &MetadataMap) -> Result<String> {
    let mut found_header_keys: Vec<String> = vec![];

    for key in IOX_FLIGHT_SQL_DATABASE_HEADERS {
        if metadata.contains_key(key) {
            found_header_keys.push(key.to_string());
        }
    }

    // if all the keys specify the same database name, return the name
    let mut database_name: Option<&str> = None;
    for key in &found_header_keys {
        if let Some(v) = metadata.get(key) {
            let v = v.to_str().context(InvalidDatabaseHeaderSnafu)?;
            if database_name.is_none() {
                database_name = Some(v);
            } else if let Some(database_name) = database_name {
                if database_name != v {
                    return TooManyFlightSQLDatabasesSnafu {
                        header_names: found_header_keys,
                    }
                    .fail();
                }
            }
        }
    }

    Ok(database_name.context(NoFlightSQLDatabaseSnafu)?.to_string())
}

/// Retrieve the authorization token associated with the request.
fn get_flight_authz(metadata: &MetadataMap) -> Option<Vec<u8>> {
    extract_token(metadata.get("authorization"))
}

fn flightsql_permissions(namespace_name: &str, cmd: &FlightSQLCommand) -> Vec<authz::Permission> {
    let resource = authz::Resource::Database(namespace_name.to_string());
    let action = match cmd {
        FlightSQLCommand::CommandStatementQuery(_) => authz::Action::Read,
        FlightSQLCommand::CommandPreparedStatementQuery(_) => authz::Action::Read,
        FlightSQLCommand::CommandGetSqlInfo(_) => authz::Action::ReadSchema,
        FlightSQLCommand::CommandGetCatalogs(_) => authz::Action::ReadSchema,
        FlightSQLCommand::CommandGetCrossReference(_) => authz::Action::ReadSchema,
        FlightSQLCommand::CommandGetDbSchemas(_) => authz::Action::ReadSchema,
        FlightSQLCommand::CommandGetExportedKeys(_) => authz::Action::ReadSchema,
        FlightSQLCommand::CommandGetImportedKeys(_) => authz::Action::ReadSchema,
        FlightSQLCommand::CommandGetPrimaryKeys(_) => authz::Action::ReadSchema,
        FlightSQLCommand::CommandGetTables(_) => authz::Action::ReadSchema,
        FlightSQLCommand::CommandGetTableTypes(_) => authz::Action::ReadSchema,
        FlightSQLCommand::CommandGetXdbcTypeInfo(_) => authz::Action::ReadSchema,
        FlightSQLCommand::ActionCreatePreparedStatementRequest(_) => authz::Action::Read,
        FlightSQLCommand::ActionClosePreparedStatementRequest(_) => authz::Action::Read,
    };
    vec![authz::Permission::ResourceAction(resource, action)]
}

/// Check if request has IOx debug header set.
fn has_debug_header(metadata: &MetadataMap) -> bool {
    metadata
        .get("iox-debug")
        .and_then(|s| s.to_str().ok())
        .map(|s| s.to_lowercase())
        .map(|s| matches!(s.as_str(), "1" | "on" | "yes" | "y" | "true" | "t"))
        .unwrap_or_default()
}

/// Wrapper over a FlightDataEncodeStream that adds IOx specfic
/// metadata and records completion
struct GetStream {
    inner: KeepAliveStream,
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
        query: &RunQuery,
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
                query: query.to_string(),
            })?
            .map_err(|e| {
                let code = datafusion_error_to_tonic_code(&e);
                tonic::Status::new(code, e.to_string()).into()
            });

        // setup inner stream
        let inner = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .with_metadata(app_metadata.encode_to_vec().into())
            .build(query_results);

        // add keep alive
        let inner = KeepAliveStream::new(inner, DO_GET_KEEP_ALIVE_INTERVAL);

        Ok(Self {
            inner,
            permit,
            query_completed_token,
            done: false,
        })
    }
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
    use arrow_flight::sql::ProstMessageExt;
    use async_trait::async_trait;
    use authz::Permission;
    use futures::Future;
    use metric::{Attributes, Metric, U64Gauge};
    use service_common::test_util::TestDatabaseStore;
    use tokio::pin;
    use tonic::metadata::{MetadataKey, MetadataValue};

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
            authz: Option::<Arc<dyn Authorizer>>::None,
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

    #[derive(Debug)]
    struct MockAuthorizer {}

    #[async_trait]
    impl Authorizer for MockAuthorizer {
        async fn permissions(
            &self,
            token: Option<Vec<u8>>,
            perms: &[Permission],
        ) -> Result<Vec<Permission>, authz::Error> {
            match token {
                Some(token) => match (&token as &dyn AsRef<[u8]>).as_ref() {
                    b"GOOD" => Ok(perms.to_vec()),
                    b"BAD" => Err(authz::Error::Forbidden),
                    b"INVALID" => Err(authz::Error::InvalidToken),
                    b"UGLY" => Err(authz::Error::verification("test", "test error")),
                    _ => panic!("unexpected token"),
                },
                None => Err(authz::Error::NoToken),
            }
        }
    }

    #[tokio::test]
    async fn do_get_authz() {
        let test_storage = Arc::new(TestDatabaseStore::default());
        test_storage.db_or_create("bananas").await;

        let svc = FlightService {
            server: Arc::clone(&test_storage),
            authz: Some(Arc::new(MockAuthorizer {})),
        };

        async fn assert_code(
            svc: &FlightService<TestDatabaseStore>,
            want: tonic::Code,
            request: tonic::Request<arrow_flight::Ticket>,
        ) {
            let got = match svc.do_get(request).await {
                Ok(_) => tonic::Code::Ok,
                Err(e) => e.code(),
            };
            assert_eq!(want, got);
        }

        fn request(
            query: RunQuery,
            authorization: &'static str,
        ) -> tonic::Request<arrow_flight::Ticket> {
            let mut req = tonic::Request::new(
                IoxGetRequest::new("bananas".to_string(), query, false)
                    .try_encode()
                    .unwrap(),
            );
            if !authorization.is_empty() {
                req.metadata_mut().insert(
                    MetadataKey::from_static("authorization"),
                    MetadataValue::from_static(authorization),
                );
            }
            req
        }

        fn sql_request(authorization: &'static str) -> tonic::Request<arrow_flight::Ticket> {
            request(RunQuery::Sql("SELECT 1".to_string()), authorization)
        }

        fn influxql_request(authorization: &'static str) -> tonic::Request<arrow_flight::Ticket> {
            request(
                RunQuery::InfluxQL("SHOW DATABASES".to_string()),
                authorization,
            )
        }

        fn flightsql_request(authorization: &'static str) -> tonic::Request<arrow_flight::Ticket> {
            request(
                RunQuery::FlightSQL(FlightSQLCommand::CommandGetCatalogs(
                    arrow_flight::sql::CommandGetCatalogs {},
                )),
                authorization,
            )
        }

        assert_code(&svc, tonic::Code::Unauthenticated, sql_request("")).await;
        assert_code(&svc, tonic::Code::Ok, sql_request("Bearer GOOD")).await;
        assert_code(
            &svc,
            tonic::Code::PermissionDenied,
            sql_request("Bearer BAD"),
        )
        .await;
        assert_code(
            &svc,
            tonic::Code::PermissionDenied,
            sql_request("Bearer INVALID"),
        )
        .await;
        assert_code(&svc, tonic::Code::Internal, sql_request("Bearer UGLY")).await;

        assert_code(&svc, tonic::Code::Unauthenticated, influxql_request("")).await;

        assert_code(
            &svc,
            tonic::Code::InvalidArgument, // SHOW DATABASE has not been implemented yet.
            influxql_request("Bearer GOOD"),
        )
        .await;
        assert_code(
            &svc,
            tonic::Code::PermissionDenied,
            influxql_request("Bearer BAD"),
        )
        .await;
        assert_code(&svc, tonic::Code::Internal, influxql_request("Bearer UGLY")).await;

        assert_code(&svc, tonic::Code::Unauthenticated, flightsql_request("")).await;
        assert_code(&svc, tonic::Code::Ok, flightsql_request("Bearer GOOD")).await;
        assert_code(
            &svc,
            tonic::Code::PermissionDenied,
            flightsql_request("Bearer BAD"),
        )
        .await;
        assert_code(
            &svc,
            tonic::Code::Internal,
            flightsql_request("Bearer UGLY"),
        )
        .await;
    }

    #[tokio::test]
    async fn get_flight_info_authz() {
        let test_storage = Arc::new(TestDatabaseStore::default());
        test_storage.db_or_create("bananas").await;

        let svc = FlightService {
            server: Arc::clone(&test_storage),
            authz: Some(Arc::new(MockAuthorizer {})),
        };

        async fn assert_code(
            svc: &FlightService<TestDatabaseStore>,
            want: tonic::Code,
            request: tonic::Request<FlightDescriptor>,
        ) {
            let got = match svc.get_flight_info(request).await {
                Ok(_) => tonic::Code::Ok,
                Err(e) => e.code(),
            };
            assert_eq!(want, got);
        }

        fn request(authorization: &'static str) -> tonic::Request<FlightDescriptor> {
            let cmd = arrow_flight::sql::CommandGetCatalogs {};
            let mut req =
                tonic::Request::new(FlightDescriptor::new_cmd(cmd.as_any().encode_to_vec()));
            req.metadata_mut().insert(
                MetadataKey::from_static("database"),
                MetadataValue::from_static("bananas"),
            );
            if !authorization.is_empty() {
                req.metadata_mut().insert(
                    MetadataKey::from_static("authorization"),
                    MetadataValue::from_static(authorization),
                );
            }
            req
        }

        assert_code(&svc, tonic::Code::Unauthenticated, request("")).await;
        assert_code(&svc, tonic::Code::Ok, request("Bearer GOOD")).await;
        assert_code(&svc, tonic::Code::PermissionDenied, request("Bearer BAD")).await;
        assert_code(&svc, tonic::Code::Internal, request("Bearer UGLY")).await;
    }
}
