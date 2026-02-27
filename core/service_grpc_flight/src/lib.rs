//! Implements the InfluxDB IOx "Native" Flight API and Arrow
//! FlightSQL, based on Arrow Flight and gRPC. See [`FlightService`]
//! for full detail.

use keep_alive::KeepAliveStream;
use planner::Planner;
use tower_trailer::{HeaderMap, Trailers};
// Workaround for "unused crate" lint false positives.
use workspace_hack as _;

mod keep_alive;
mod planner;
mod request;

use arrow::error::ArrowError;
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightEndpoint, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaResult, Ticket,
    encode::FlightDataEncoderBuilder,
    error::FlightError,
    flight_descriptor::DescriptorType,
    flight_service_server::{FlightService as Flight, FlightServiceServer as FlightServer},
};
use authz::{Authorizer, extract_token};
use data_types::NamespaceNameError;
use datafusion::{error::DataFusionError, physical_plan::ExecutionPlan};
use error_reporting::DisplaySourceChain;
use flightsql::{BaseTableType, FlightSQLCommand};
use futures::{Stream, StreamExt, TryStreamExt, ready, stream::BoxStream};
use generated_types::{
    Code, Request, Response, Status, Streaming,
    influxdata::iox::querier::v1 as proto,
    metadata::{AsciiMetadataValue, MetadataMap},
};
use iox_query::{
    QueryDatabase,
    exec::IOxSessionContext,
    query_log::{PermitAndToken, QueryCompletedToken, QueryLogEntry, StatePermit, StatePlanned},
};
use iox_query::{exec::QueryConfig, query_log::QueryLogEntryState};
use prost::Message;
use request::{IoxGetRequest, RunQuery};
use service_common::{datafusion_error_to_tonic_code, flight_error_to_tonic_code};
use snafu::{OptionExt, ResultExt, Snafu};
use std::{
    fmt::Debug,
    pin::Pin,
    str::FromStr,
    sync::{Arc, Mutex},
    task::Poll,
    time::Duration,
};
use trace::{ctx::SpanContext, span::SpanExt};
use trace_http::{
    ctx::{RequestLogContext, RequestLogContextExt},
    query_variant::QueryVariantExt,
};
use tracing::{debug, info, warn};

/// The supported names of the grpc header that contain the target database
/// for FlightSQL requests.
///
/// See <https://lists.apache.org/thread/fd6r1n7vt91sg2c7fr35wcrsqz6x4645>
/// for discussion on adding support to FlightSQL itself.
const IOX_FLIGHT_SQL_DATABASE_REQUEST_HEADERS: [&str; 4] = [
    "database", // preferred
    "bucket",
    "bucket-name",
    "iox-namespace-name", // deprecated
];

/// Header that sets the base table type for FlightSQL requests. `BASE TABLE` or `TABLE`
const IOX_FLIGHT_SQL_BASE_TABLE_TYPE: &str = "x-influxdata-base-table-type";

/// Header that contains a query-specific partition limit.
const IOX_FLIGHT_PARTITION_LIMIT_HEADER: &str = "x-influxdata-partition-limit";

/// Header that contains a query-specific parquet file limit.
const IOX_FLIGHT_PARQUET_FILE_LIMIT_HEADER: &str = "x-influxdata-parquet-file-limit";

/// Header that specifies the language which the provided query string should be interpreted as;
/// optionally sent as part of the GetFlightInfo request
const IOX_FLIGHT_QUERY_LANGUAGE: &str = "x-influxdata-query-language";

/// Trailer that describes the duration (in seconds) for which a query was queued due to concurrency limits.
const IOX_FLIGHT_QUEUE_DURATION_RESPONSE_TRAILER: &str = "x-influxdata-queue-duration-seconds";

/// Trailer that describes the duration (in seconds) of the planning phase of a query.
const IOX_FLIGHT_PLANNING_DURATION_RESPONSE_TRAILER: &str =
    "x-influxdata-planning-duration-seconds";

/// Trailer that describes the duration (in seconds) of the execution phase of a query.
const IOX_FLIGHT_EXECUTION_DURATION_RESPONSE_TRAILER: &str =
    "x-influxdata-execution-duration-seconds";

/// Trailer that describes the duration (in seconds) the CPU(s) took to compute the results.
const IOX_FLIGHT_COMPUTE_DURATION_RESPONSE_TRAILER: &str = "x-influxdata-compute-duration-seconds";

/// Trailer that describes the number of partitions processed by a query.
const IOX_FLIGHT_PARTITIONS_RESPONSE_TRAILER: &str = "x-influxdata-partitions";

/// Trailer that describes the number of parquet files processed by a query.
const IOX_FLIGHT_PARQUET_FILES_RESPONSE_TRAILER: &str = "x-influxdata-parquet-files";

/// Trailer that describes the peak memory usage of a query.
const IOX_FLIGHT_MAX_MEMORY_RESPONSE_TRAILER: &str = "x-influxdata-max-memory-bytes";

/// Trailer that describes the latency to planning from ingester response.
const IOX_FLIGHT_INGESTER_LATENCY_PLAN_RESPONSE_TRAILER: &str =
    "x-influxdata-ingester-latency-plan";

/// Trailer that describes the latency to all data from ingester response.
const IOX_FLIGHT_INGESTER_LATENCY_DATA_RESPONSE_TRAILER: &str =
    "x-influxdata-ingester-latency-data";

/// Trailer that describes the total response rows from the ingester.
const IOX_FLIGHT_INGESTER_RESPONSE_ROWS_RESPONSE_TRAILER: &str =
    "x-influxdata-ingester-response-rows";

/// Trailer that describes the total partition count from the ingester.
const IOX_FLIGHT_INGESTER_PATITION_COUNT_RESPONSE_TRAILER: &str =
    "x-influxdata-ingester-partition-count";

/// Trailer that describes the total response bytes from the ingester.
const IOX_FLIGHT_INGESTER_RESPONSE_BYTES_RESPONSE_TRAILER: &str =
    "x-influxdata-ingester-response-bytes";

/// In which interval should the `DoGet` stream send empty messages as keep alive markers?
const DO_GET_KEEP_ALIVE_INTERVAL: Duration = Duration::from_secs(5);

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Invalid ticket. Error: {}", source))]
    InvalidTicket { source: request::Error },

    #[snafu(display("Internal creating encoding ticket: {}", source))]
    InternalCreatingTicket { source: request::Error },

    #[snafu(display("Invalid handshake. No payload provided"))]
    InvalidHandshake {},

    #[snafu(display("Cannot retrieve database: {}", source))]
    Database { source: DataFusionError },

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

    #[snafu(display("Invalid argument: {}", source))]
    InvalidArgument { source: flightsql::Error },

    #[snafu(display("Invalid 'database' header in request: {}", source))]
    InvalidDatabaseHeader {
        source: generated_types::metadata::errors::ToStrError,
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
        source: planner::Error,
    },

    #[snafu(display("Error while planning Flight SQL : {}", source))]
    FlightSQL { source: flightsql::Error },

    #[snafu(display("Error while execution Flight SQL stream: {}", source))]
    StreamingFlightSql { source: FlightError },

    #[snafu(display("No FlightDescriptor in Flight SQL request"))]
    NoFlightDescriptor,

    #[snafu(display("Invalid protobuf: {}", source))]
    Deserialization { source: prost::DecodeError },

    #[snafu(display("Unsupported message type: {}", description))]
    UnsupportedMessageType { description: String },

    #[snafu(display("Unauthenticated"))]
    Unauthenticated,

    #[snafu(display("Permission denied"))]
    PermissionDenied,

    #[snafu(display("Authz verification error: {}: {}", msg, source))]
    AuthzVerification {
        msg: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}
pub type Result<T, E = Error> = std::result::Result<T, E>;

impl From<Error> for Status {
    /// Converts a result from the business logic into the appropriate tonic
    /// status
    fn from(err: Error) -> Self {
        // walk cause chain to display full details
        // see https://github.com/influxdata/influxdb_iox/issues/12373
        let err = DisplaySourceChain::new(err);
        let msg = "Error handling Flight gRPC request";
        let namespace = err.inner().namespace();
        let query = err.inner().query();
        // An explicit match on the Error enum will ensure appropriate
        // logging is handled for any new error variants.
        match err.inner() {
            Error::DatabaseNotFound { .. }
            | Error::InvalidTicket { .. }
            | Error::InvalidHandshake { .. }
            | Error::Unauthenticated
            | Error::PermissionDenied
            | Error::InvalidDatabaseName { .. }
            | Error::Query { .. } => {
                info!(e=%err, %namespace, %query, msg);
            }
            Error::Database { .. }
            | Error::Optimize { .. }
            | Error::EncodeSchema { .. }
            | Error::TooManyFlightSQLDatabases { .. }
            | Error::NoFlightSQLDatabase
            | Error::InvalidArgument { .. }
            | Error::InvalidDatabaseHeader { .. }
            | Error::Planning { .. }
            | Error::Deserialization { .. }
            | Error::InternalCreatingTicket { .. }
            | Error::UnsupportedMessageType { .. }
            | Error::FlightSQL { .. }
            | Error::StreamingFlightSql { .. }
            | Error::AuthzVerification { .. }
            | Error::NoFlightDescriptor => {
                warn!(e=%err, %namespace, %query, msg);
            }
        };
        err.into_inner().into_status()
    }
}

impl Error {
    /// Converts a result from the business logic into the appropriate tonic (gRPC)
    /// status message to send back to users
    fn into_status(self) -> Status {
        // walk cause chain to display full details
        // see https://github.com/influxdata/influxdb_iox/issues/12373
        let err = DisplaySourceChain::new(self);

        // limit msg length
        let mut msg = err.to_string();
        if msg.len() > 2_000 {
            msg = format!("{}...", &msg[..2_000]);
        }

        let code = match err.into_inner() {
            Self::DatabaseNotFound { .. } => Code::NotFound,
            Self::InvalidTicket { .. }
            | Self::InvalidHandshake { .. }
            | Self::Deserialization { .. }
            | Self::TooManyFlightSQLDatabases { .. }
            | Self::NoFlightSQLDatabase
            | Self::NoFlightDescriptor
            | Self::InvalidArgument { .. }
            | Self::InvalidDatabaseHeader { .. }
            | Self::InvalidDatabaseName { .. } => Code::InvalidArgument,
            Self::Database { source }
            | Self::Planning { source, .. }
            | Self::Query { source, .. } => datafusion_error_to_tonic_code(&source),
            Self::UnsupportedMessageType { .. } => Code::Unimplemented,
            Self::FlightSQL { source } => match source {
                flightsql::Error::InvalidArgument { .. }
                | flightsql::Error::InvalidHandle { .. }
                | flightsql::Error::InvalidTypeUrl { .. }
                | flightsql::Error::Decode { .. }
                | flightsql::Error::InvalidPreparedStatementParams { .. }
                | flightsql::Error::NoFlightData { .. }
                | flightsql::Error::Protocol { .. }
                | flightsql::Error::UnknownParameterType { .. }
                | flightsql::Error::UnsupportedMessageType { .. } => Code::InvalidArgument,
                flightsql::Error::Flight { source } => {
                    // only use status code, NOT the message because it does NOT contain the entire chain
                    flight_error_to_tonic_code(&source)
                }
                fs_err @ flightsql::Error::Arrow { .. } => {
                    // wrap in Datafusion error to walk source stacks
                    let df_error = DataFusionError::from(fs_err);
                    datafusion_error_to_tonic_code(&df_error)
                }
                flightsql::Error::DataFusion { source } => datafusion_error_to_tonic_code(&source),
            },
            Self::StreamingFlightSql { source } => {
                // only use status code, NOT the message because it does NOT contain the entire chain
                flight_error_to_tonic_code(&source)
            }
            Self::InternalCreatingTicket { .. }
            | Self::Optimize { .. }
            | Self::EncodeSchema { .. } => Code::Internal,
            Self::AuthzVerification { .. } => Code::Unavailable,
            Self::Unauthenticated => Code::Unauthenticated,
            Self::PermissionDenied => Code::PermissionDenied,
        };

        Status::new(code, msg)
    }

    /// returns the namespace name, if known, used for logging
    fn namespace(&self) -> &str {
        match self {
            Self::Database { .. }
            | Self::InvalidTicket { .. }
            | Self::InternalCreatingTicket { .. }
            | Self::InvalidHandshake {}
            | Self::TooManyFlightSQLDatabases { .. }
            | Self::NoFlightSQLDatabase
            | Self::NoFlightDescriptor
            | Self::InvalidArgument { .. }
            | Self::InvalidDatabaseHeader { .. }
            | Self::InvalidDatabaseName { .. }
            | Self::Optimize { .. }
            | Self::EncodeSchema { .. }
            | Self::FlightSQL { .. }
            | Self::StreamingFlightSql { .. }
            | Self::Deserialization { .. }
            | Self::UnsupportedMessageType { .. }
            | Self::Unauthenticated
            | Self::PermissionDenied
            | Self::AuthzVerification { .. } => "<unknown>",
            Self::DatabaseNotFound { namespace_name } => namespace_name,
            Self::Query { namespace_name, .. } => namespace_name,
            Self::Planning { namespace_name, .. } => namespace_name,
        }
    }

    /// returns a query, if know, used for logging
    fn query(&self) -> &str {
        match self {
            Self::Database { .. }
            | Self::InvalidTicket { .. }
            | Self::InternalCreatingTicket { .. }
            | Self::InvalidHandshake {}
            | Self::TooManyFlightSQLDatabases { .. }
            | Self::NoFlightSQLDatabase
            | Self::NoFlightDescriptor
            | Self::InvalidArgument { .. }
            | Self::InvalidDatabaseHeader { .. }
            | Self::InvalidDatabaseName { .. }
            | Self::Optimize { .. }
            | Self::EncodeSchema { .. }
            | Self::FlightSQL { .. }
            | Self::StreamingFlightSql { .. }
            | Self::Deserialization { .. }
            | Self::UnsupportedMessageType { .. }
            | Self::Unauthenticated
            | Self::PermissionDenied
            | Self::AuthzVerification { .. }
            | Self::DatabaseNotFound { .. } => "NONE",
            Self::Query { query, .. } => query,
            Self::Planning { query, .. } => query,
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
            authz::Error::Forbidden { .. } => Self::PermissionDenied,
            authz::Error::InvalidToken => Self::PermissionDenied,
            authz::Error::NoToken => Self::Unauthenticated,
            authz::Error::Verification { source, msg } => Self::AuthzVerification { msg, source },
        }
    }
}

type TonicStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send + 'static>>;

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
/// 1. Encode the query in a `CommandStatementQuery` FlightSQL structure in a [`FlightDescriptor`]
///
/// 2. Call the `GetFlightInfo` method with the the [`FlightDescriptor`]
///
/// 3. Receive a `Ticket` in the returned [`FlightInfo`]. The Ticket is opaque (uninterpreted) by
///    the client. It contains an [`IoxGetRequest`] with the `CommandStatementQuery` request.
///
/// 4. Calls the `DoGet` method with the `Ticket` from the previous step.
///
/// 5. Receive a stream of data encoded as [`FlightData`]
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
/// ## FlightSQL Prepared Statement with no bind parameters (e.g. $1, $name)
///
/// To run a prepared query, via FlightSQL, the client undertakes a
/// few more steps:
///
/// 1. Encode the query in a `ActionCreatePreparedStatementRequest` request structure
///
/// 2. Call `DoAction` method with the the request
///
/// 3. Receive a `ActionCreatePreparedStatementResponse`, which contains a prepared statement
///    "handle".
///
/// 4. Encode the handle in a `CommandPreparedStatementQuery` FlightSQL structure in a
///    [`FlightDescriptor`] and call the `GetFlightInfo` method with the the [`FlightDescriptor`]
///
/// 5. Steps 5,6,7 proceed the same as for a FlightSQL ad-hoc query
///
/// ```text
///                                                            .───────.
/// ╔═══════════╗                                             (         )
/// ║           ║                                             │`───────'│
/// ║ FlightSQL ║                                             │   IOx   │
/// ║  Client   ║                                             │.───────.│
/// ║           ║                                             (         )
/// ╚═══════════╝                                              `───────'
///     1 ┃ Creates an ActionCreatePreparedStatementRequest        ┃
///       ┃                                                        ┃
///       ┃  DoAction(ActionCreatePreparedStatementRequest)        ┃
///     2 ┃━ ━ ━ ━ ━ ━ ━ ━  ━ ━ ━━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━▶┃
///       ┃                                                        ┃
///       ┃  Result(ActionCreatePreparedStatementResult{handle})   ┃
///     3 ┃◀ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ┃
///       ┃                                                        ┃
///       ┃  GetFlightInfo(CommandPreparedStatementQuery)          ┃
///     4 ┃━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━▶┃
///       ┃                                                        ┃
///       ┃  FlightInfo(Ticket{CommandPreparedStatementQuery})     ┃
///     5 ┃◀ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ┃
///       ┃                                                        ┃
///       ┃                  DoGet(Ticket)                         ┃
///     6 ┃━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━▶┃
///       ┃                                                        ┃
///       ┃                Stream of FlightData                     ┃
///     7 ┃◀ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ┃
/// ```
///
/// ## FlightSQL Prepared Statement with bind parameters (e.g. $1, $name)
///
/// Running a prepared query with bind parameters is largely the same as
/// running a prepared query without bind parameters, but there is an additional
/// step to bind the parameter values to the prepared statement handle.
///
/// 1. Encode the query in a `ActionCreatePreparedStatementRequest` request structure
///
/// 2. Call `DoAction` method with the the request
///
/// 3. Receive a `ActionCreatePreparedStatementResponse`, which contains a prepared statement
///    "handle".
///
/// 4. Call `DoPut method with `CommandPreparedStatementQuery`
///
/// 5. Send a stream of [`FlightData`] containing the parameters
///
/// 6. Receive a `DoPutPreparedStatementResult` which contains an updated prepared statement handle
///    to use with the subsequent steps.
///
/// 7. Encode the updated handle in a `CommandPreparedStatementQuery` FlightSQL structure in a
///    [`FlightDescriptor`] and call the `GetFlightInfo` method with the the [`FlightDescriptor`]
///
/// 8. Steps 8,9,10 proceed the same as for a FlightSQL ad-hoc query
///
/// ```text
///                                                            .───────.
/// ╔═══════════╗                                             (         )
/// ║           ║                                             │`───────'│
/// ║ FlightSQL ║                                             │   IOx   │
/// ║  Client   ║                                             │.───────.│
/// ║           ║                                             (         )
/// ╚═══════════╝                                              `───────'
///     1 ┃  Creates an ActionCreatePreparedStatementRequest       ┃
///       ┃                                                        ┃
///       ┃  DoAction(ActionCreatePreparedStatementRequest)        ┃
///     2 ┃━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━  ━ ━ ━━ ━ ━▶┃
///       ┃                                                        ┃
///       ┃  Result(ActionCreatePreparedStatementResult{handle})   ┃
///     3 ┃◀ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ┃
///       ┃                                                        ┃
///       ┃  DoPut(CommandPreparedStatementQuery)                  ┃
///     4 ┃━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━▶┃
///       ┃                                                        ┃
///       ┃  parameters as stream of FlightData                    ┃
///     5 ┃━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━▶┃
///       ┃                                                        ┃
///       ┃  Result(DoPutPreparedStatementResult{handle})          ┃
///     6 ┃◀ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ┃
///       ┃                                                        ┃
///       ┃  GetFlightInfo(CommandPreparedStatementQuery)          ┃
///     7 ┃━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━▶┃
///       ┃                                                        ┃
///       ┃  FlightInfo(Ticket{CommandPreparedStatementQuery})     ┃
///     8 ┃◀ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ┃
///       ┃                                                        ┃
///       ┃                  DoGet(Ticket)                         ┃
///     9 ┃━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━▶┃
///       ┃                                                        ┃
///       ┃                Stream of FightData                     ┃
///    10 ┃◀ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ┃
/// ```
/// [Arrow Flight]: https://arrow.apache.org/docs/format/Flight.html
/// [Arrow FlightSQL]: https://arrow.apache.org/docs/format/FlightSql.html
#[derive(Debug)]
struct FlightService {
    server: Arc<dyn QueryDatabase>,
    authz: Option<Arc<dyn Authorizer>>,
}

pub fn make_server(
    server: Arc<dyn QueryDatabase>,
    authz: Option<Arc<dyn Authorizer>>,
) -> FlightServer<impl Flight> {
    FlightServer::new(FlightService { server, authz })
}

impl FlightService {
    /// Implementation of the `DoGet` method
    #[expect(clippy::too_many_arguments)]
    async fn run_do_get(
        server: Arc<dyn QueryDatabase>,
        span_ctx: Option<SpanContext>,
        external_span_ctx: Option<RequestLogContext>,
        request: IoxGetRequest,
        log_entry: &mut Option<Arc<QueryLogEntry>>,
        query_config: Option<&QueryConfig>,
        auth_id: Option<String>,
        base_table_type: BaseTableType,
    ) -> Result<TonicStream<FlightData>, Status> {
        let IoxGetRequest {
            database,
            query,
            params,
            is_debug,
        } = request;
        let namespace: Arc<str> = database.into();
        let namespace_name = Arc::clone(&namespace);
        let namespace_name = namespace_name.as_ref();

        let db = server
            .namespace(
                namespace_name,
                span_ctx.child_span("get_namespace"),
                is_debug,
            )
            .await
            .context(DatabaseSnafu)?
            .context(DatabaseNotFoundSnafu { namespace_name })?;

        let query_completed_token = db.record_query(
            external_span_ctx.as_ref().map(RequestLogContext::ctx),
            query.variant().str(),
            Box::new(query.to_string()),
            params.clone(),
            auth_id,
        );

        *log_entry = Some(Arc::clone(query_completed_token.entry()));

        // Log after we acquire the permit and are about to start execution
        info!(
            %namespace_name,
            %query,
            trace=external_span_ctx.format_jaeger().as_str(),
            variant=query.variant().str(),
            request_protocol="v3_grpc_flight",
            "DoGet request",
        );

        let ctx = db.new_query_context(span_ctx, query_config);
        let planner = Planner::new(&ctx);
        let query = Arc::new(query);

        let q = Arc::clone(&query);
        let ns = Arc::clone(&namespace);

        // Run planner on a separate threadpool, rather than the IO pool that is servicing this request
        let physical_plan_res = ctx
            .run(async move {
                match q.as_ref() {
                    RunQuery::FlightSQL(msg) => {
                        planner
                            .flight_sql_do_get(&ns, db, msg.clone(), base_table_type)
                            .await
                    }
                    RunQuery::Sql(sql_query) => planner.sql(sql_query, params).await,
                    RunQuery::InfluxQL(sql_query) => planner.influxql(sql_query, params).await,
                }
            })
            .await
            .with_context(|_| PlanningSnafu {
                namespace_name,
                query: query.to_string(),
            });

        let (physical_plan, query_completed_token) = match physical_plan_res {
            Ok(physical_plan) => {
                let query_completed_token =
                    query_completed_token.planned(&ctx, Arc::clone(&physical_plan));
                (physical_plan, query_completed_token)
            }
            Err(e) => {
                query_completed_token.fail();
                return Err(e.into());
            }
        };

        let output = GetStream::new(
            server,
            ctx,
            physical_plan,
            namespace_name.to_string(),
            &query,
            query_completed_token,
        )
        .await?;

        // Log any error that happens *during* execution (other error
        // handling in this file happen during planning)
        let output = output.map(move |res| {
            if let Err(e) = &res {
                info!(
                    %namespace,
                    %query,
                    trace=external_span_ctx.format_jaeger().as_str(),
                    %e,
                    "Error executing query via DoGet",
                );
            }
            res
        });

        Ok(Box::pin(output) as TonicStream<FlightData>)
    }
}

#[generated_types::async_trait]
impl Flight for FlightService {
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
    ) -> Result<Response<SchemaResult>, Status> {
        Err(Status::unimplemented("Not yet implemented: get_schema"))
    }

    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        let extensions = request.extensions();

        let external_span_ctx: Option<RequestLogContext> = extensions.get().cloned();
        // technically the trailers layer should always be installed but for testing this isn't always the case, so lets
        // make this optional
        let trailers: Option<Trailers> = extensions.get().cloned();
        let span_ctx: Option<SpanContext> = extensions.get().cloned();
        let query_variant_ext: Option<QueryVariantExt> = extensions.get().cloned();

        let metadata = request.metadata();
        let authz_token = get_flight_authz(metadata);
        let debug_header = has_debug_header(metadata);
        let query_config = get_query_config(metadata);
        let base_table_type = get_flightsql_base_table_type(metadata)?;
        let ticket = request.into_inner();

        // attempt to decode ticket
        let request = IoxGetRequest::try_decode(ticket)
            .context(InvalidTicketSnafu)
            .inspect_err(|e| info!(%e, "Error decoding Flight API ticket"))?
            .add_debug_header(debug_header);

        // register query variant
        if let Some(ext) = query_variant_ext {
            ext.set(request.query().variant())
        }

        let perms = match request.query() {
            RunQuery::FlightSQL(cmd) => flightsql_permissions(request.database(), cmd),
            RunQuery::Sql(_) | RunQuery::InfluxQL(_) => vec![authz::Permission::ResourceAction(
                authz::Resource::Database(authz::Target::ResourceName(
                    request.database().to_string(),
                )),
                authz::Action::Read,
            )],
        };
        let authz = self
            .authz
            .authorize(authz_token, &perms)
            .await
            .map_err(Error::from)?;

        let database = request.database.clone();
        let query = request.query.clone();
        let jaeger_trace = external_span_ctx.format_jaeger();

        // `run_do_get` may wait for the semaphore. In this case, we shall send empty "keep alive" messages already. So
        // wrap the whole implementation into the keep alive stream.
        //
        // Also note that due to the keep alive mechanism, we cannot send any headers back because they might come
        // after a keep alive message and therefore aren't headers. gRPC metadata can only be sent at the very beginning
        // (headers) or at the very end (trailers). We shall use trailers.
        let server = Arc::clone(&self.server);
        let mut log_entry = None;
        let response = Self::run_do_get(
            server,
            span_ctx,
            external_span_ctx,
            request,
            &mut log_entry,
            query_config.as_ref(),
            authz.into_subject(),
            base_table_type,
        )
        .await;

        if let Err(e) = &response {
            info!(
                %database,
                %query,
                trace=jaeger_trace.as_str(),
                %e,
                "Error running DoGet",
            );
        } else {
            debug!(
                %database,
                %query,
                trace=jaeger_trace.as_str(),
                "Planned DoGet request",
            );
        }

        let md = QueryResponseMetadata { log_entry };
        let md_captured = md.clone();
        if let Some(trailers) = trailers {
            trailers.add_callback(move |trailers| md_captured.write_trailers(trailers));
        }

        let stream = response?;

        Ok(Response::new(Box::pin(stream) as _))
    }

    async fn handshake(
        &self,
        request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
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
            .map_err(|e| Status::invalid_argument(e.to_string()))?;

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
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        Err(Status::unimplemented("Not yet implemented: list_flights"))
    }

    /// Handles `GetFlightInfo` RPC requests. The [`FlightDescriptor`]
    /// is treated containing an FlightSQL command, encoded as a binary
    /// ProtoBuf message.
    ///
    /// see [`FlightService`] for more details.
    async fn get_flight_info(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let external_span_ctx: Option<RequestLogContext> = request.extensions().get().cloned();
        let span_ctx: Option<SpanContext> = request.extensions().get().cloned();
        let trace = external_span_ctx.format_jaeger();
        let metadata = request.metadata();

        let is_debug = has_debug_header(metadata);

        let namespace_name = get_flightsql_namespace(metadata)?;
        let authz_token = get_flight_authz(metadata);
        let query_lang = parse_header_str(metadata, IOX_FLIGHT_QUERY_LANGUAGE);
        let flight_descriptor = request.into_inner();

        // extract the FlightSQL message
        let cmd = cmd_from_descriptor(flight_descriptor.clone())?;
        info!(%namespace_name, %cmd, %trace, "GetFlightInfo request");

        let perms = flightsql_permissions(&namespace_name, &cmd);
        self.authz
            .authorize(authz_token, &perms)
            .await
            .map_err(Error::from)?;

        let db = self
            .server
            .namespace(
                &namespace_name,
                span_ctx.child_span("get_namespace"),
                is_debug,
            )
            .await
            .context(DatabaseSnafu)?
            .context(DatabaseNotFoundSnafu {
                namespace_name: &namespace_name,
            })?;

        let ctx = db.new_query_context(span_ctx, None);
        let ns_name = namespace_name.clone();
        let planner = Planner::new(&ctx);
        let cmd_captured = cmd.clone();

        // invariant: schema and run_query must both correspond to the same language. This is not
        // really something that we can encode in the type system, as that would 1. require
        // changing the wire format that these tickets and such as encoded/decoded as, and 2. mess
        // with the concept of having unified types for processing something that is either an
        // InfluxQL query plan or a SQL query plan. So we just process them at the same place and
        // hope we don't mess that up in the future
        let (schema, run_query) = ctx
            // Run planner on a separate threadpool, rather than the IO pool that is servicing this request
            .run(async move {
                planner
                    .flight_sql_get_flight_info_schema(&ns_name, cmd_captured, query_lang)
                    .await
            })
            .await
            .context(PlanningSnafu {
                namespace_name: &namespace_name,
                query: format!("{cmd:?}"),
            })
            .inspect_err(
                |e| info!(%namespace_name, %cmd, %trace, %e, "Error running GetFlightInfo"),
            )
            .inspect(
                |_| debug!(%namespace_name, %cmd, %trace, "Completed GetFlightInfo request"),
            )?;

        // Form the response ticket (that the client will pass back to DoGet)
        let ticket = IoxGetRequest::new(&namespace_name, run_query, is_debug)
            .try_encode()
            .context(InternalCreatingTicketSnafu)?;

        let endpoint = FlightEndpoint::new().with_ticket(ticket);

        let flight_info = FlightInfo::new()
            .with_endpoint(endpoint)
            // return descriptor we were passed
            .with_descriptor(flight_descriptor)
            .try_with_schema(schema.as_ref())
            .context(EncodeSchemaSnafu)?;

        Ok(Response::new(flight_info))
    }

    async fn poll_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<PollInfo>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }

    async fn do_put(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        info!("Handling flightsql do_put body");
        let external_span_ctx: Option<RequestLogContext> = request.extensions().get().cloned();
        let span_ctx: Option<SpanContext> = request.extensions().get().cloned();
        let trace = external_span_ctx.format_jaeger();
        let is_debug = has_debug_header(request.metadata());

        let namespace_name = get_flightsql_namespace(request.metadata())?;
        let authz_token = get_flight_authz(request.metadata());

        // get stream from request
        let mut stream = request.into_inner().peekable();
        // peek at the first FlightData in the stream to get the FlightDescriptor
        let flight_descriptor = Pin::new(&mut stream)
            .peek()
            .await
            .cloned()
            .transpose()?
            .and_then(|data| data.flight_descriptor)
            .context(NoFlightDescriptorSnafu)?;
        // extract the FlightSQL message
        let cmd = FlightSQLCommand::try_decode(flight_descriptor.cmd).context(FlightSQLSnafu)?;

        info!(%namespace_name, %cmd, %trace, "DoPut request");

        let perms = flightsql_permissions(&namespace_name, &cmd);
        self.authz
            .authorize(authz_token, &perms)
            .await
            .map_err(Error::from)?;

        let db = self
            .server
            .namespace(
                &namespace_name,
                span_ctx.child_span("get_namespace"),
                is_debug,
            )
            .await
            .context(DatabaseSnafu)?
            .context(DatabaseNotFoundSnafu {
                namespace_name: &namespace_name,
            })?;

        let ctx = db.new_query_context(span_ctx, None);
        let planner = Planner::new(&ctx);
        let name_captured = namespace_name.clone();
        let cmd_captured = cmd.clone();
        // Run planner on a separate threadpool, rather than the IO pool that is servicing this request
        let app_metadata = ctx
            .run(async move {
                planner
                    .flight_sql_do_put(&name_captured, db, cmd_captured, stream)
                    .await
            })
            .await
            .context(PlanningSnafu {
                namespace_name: &namespace_name,
                query: format!("{cmd:?}"),
            })?;

        let result = arrow_flight::PutResult { app_metadata };
        let stream = futures::stream::iter([Ok(result)]);

        Ok(Response::new(stream.boxed()))
    }

    async fn do_action(
        &self,
        request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        let external_span_ctx: Option<RequestLogContext> = request.extensions().get().cloned();
        let span_ctx: Option<SpanContext> = request.extensions().get().cloned();
        let trace = external_span_ctx.format_jaeger();
        let is_debug = has_debug_header(request.metadata());

        let namespace_name = get_flightsql_namespace(request.metadata())?;
        let authz_token = get_flight_authz(request.metadata());
        let query_config = get_query_config(request.metadata());
        let Action {
            r#type: action_type,
            body,
        } = request.into_inner();

        // extract the FlightSQL message
        let cmd = FlightSQLCommand::try_decode(body).context(FlightSQLSnafu)?;

        info!(%namespace_name, %action_type, %cmd, %trace, "DoAction request");

        let perms = flightsql_permissions(&namespace_name, &cmd);
        self.authz
            .authorize(authz_token, &perms)
            .await
            .map_err(Error::from)?;

        let db = self
            .server
            .namespace(
                &namespace_name,
                span_ctx.child_span("get_namespace"),
                is_debug,
            )
            .await
            .context(DatabaseSnafu)?
            .context(DatabaseNotFoundSnafu {
                namespace_name: &namespace_name,
            })?;

        let ctx = db.new_query_context(span_ctx, query_config.as_ref());
        let planner = Planner::new(&ctx);
        let name_captured = namespace_name.clone();
        let cmd_captured = cmd.clone();
        // Run planner on a separate threadpool, rather than the IO pool that is servicing this request
        let body = ctx
            .run(async move {
                planner
                    .flight_sql_do_action(&name_captured, db, cmd_captured)
                    .await
            })
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
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        Err(Status::unimplemented("Not yet implemented: list_actions"))
    }

    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("Not yet implemented: do_exchange"))
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

    for key in IOX_FLIGHT_SQL_DATABASE_REQUEST_HEADERS {
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
            } else if let Some(database_name) = database_name
                && database_name != v
            {
                return TooManyFlightSQLDatabasesSnafu {
                    header_names: found_header_keys,
                }
                .fail();
            }
        }
    }

    Ok(database_name.context(NoFlightSQLDatabaseSnafu)?.to_string())
}

/// Retrieve the base table type for FlightSQL requests.
/// Only `BASE TABLE` or `TABLE` are valid.
/// If the header is not present, default to `BASE TABLE` type.
/// If invalid, return an error.
fn get_flightsql_base_table_type(metadata: &MetadataMap) -> Result<BaseTableType> {
    match metadata.get(IOX_FLIGHT_SQL_BASE_TABLE_TYPE) {
        Some(value) => {
            let value_str = value.to_str().context(InvalidDatabaseHeaderSnafu)?;
            value_str.parse().context(InvalidArgumentSnafu)
        }
        None => Ok(BaseTableType::BaseTable), // Default to `BASE TABLE` if header not present
    }
}

/// Retrieve the authorization token associated with the request.
fn get_flight_authz(metadata: &MetadataMap) -> Option<Vec<u8>> {
    extract_token(metadata.get("authorization"))
}

fn flightsql_permissions(namespace_name: &str, cmd: &FlightSQLCommand) -> Vec<authz::Permission> {
    let resource =
        authz::Resource::Database(authz::Target::ResourceName(namespace_name.to_string()));
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

fn parse_header_str<C: FromStr>(metadata: &MetadataMap, header_str: &'static str) -> Option<C> {
    metadata
        .get(header_str)
        .and_then(|s| s.to_str().ok())
        .and_then(|s| s.parse().ok())
}

/// Extract the desired per-query configuration from the request metadata.
fn get_query_config(metadata: &MetadataMap) -> Option<QueryConfig> {
    let mut config = None;

    if let Some(partition_limit) = parse_header_str(metadata, IOX_FLIGHT_PARTITION_LIMIT_HEADER) {
        config.get_or_insert(QueryConfig::default()).partition_limit = Some(partition_limit);
    }

    if let Some(parquet_file_limit) =
        parse_header_str(metadata, IOX_FLIGHT_PARQUET_FILE_LIMIT_HEADER)
    {
        config
            .get_or_insert(QueryConfig::default())
            .parquet_file_limit = Some(parquet_file_limit);
    }
    config
}

/// Wrapper over a FlightDataEncodeStream that adds IOx specific
/// metadata and records completion
struct GetStream {
    inner: BoxStream<'static, Result<FlightData, FlightError>>,
    permit_state: Arc<Mutex<Option<PermitAndToken>>>,
    done: bool,
    /// keep ctx because it contains the sticky exec
    ctx: Option<IOxSessionContext>,
}

impl GetStream {
    async fn new(
        server: Arc<dyn QueryDatabase>,
        ctx: IOxSessionContext,
        physical_plan: Arc<dyn ExecutionPlan>,
        namespace_name: String,
        query: &RunQuery,
        query_completed_token: QueryCompletedToken<StatePlanned>,
    ) -> Result<Self, Status> {
        let app_metadata = proto::AppMetadata {};

        let schema = physical_plan.schema();

        let query_results = ctx
            .execute_stream(Arc::clone(&physical_plan))
            .await
            .context(QuerySnafu {
                namespace_name: namespace_name.clone(),
                query: query.to_string(),
            })?
            .map_err(|e| FlightError::ExternalError(Box::new(e)));

        // acquire token (after planning)
        let permit_state: Arc<Mutex<Option<PermitAndToken>>> = Default::default();
        let permit_state_captured = Arc::clone(&permit_state);
        let permit_span = ctx.child_span("query_rate_limit_semaphore");
        let query_results = futures::stream::once(async move {
            let permit = server.acquire_semaphore(permit_span).await;
            let query_completed_token = query_completed_token.permit();
            *permit_state_captured.lock().expect("not poisoned") = Some(PermitAndToken {
                permit,
                query_completed_token,
            });
            query_results
        })
        .flatten();

        // setup encoding stream
        let encoded = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .with_metadata(app_metadata.encode_to_vec().into())
            .build(query_results);

        // keep-alive
        let inner = KeepAliveStream::new(encoded, DO_GET_KEEP_ALIVE_INTERVAL).boxed();

        Ok(Self {
            inner,
            permit_state,
            done: false,
            ctx: Some(ctx),
        })
    }

    #[must_use]
    fn finish_stream(&mut self) -> Option<QueryCompletedToken<StatePermit>> {
        self.ctx.take();

        self.permit_state
            .lock()
            .expect("not poisoned")
            .take()
            .map(|state| state.query_completed_token)
    }
}

impl Stream for GetStream {
    type Item = Result<FlightData, Status>;

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
                    if let Some(token) = self.finish_stream() {
                        token.success();
                    }
                }
                Some(Ok(data)) => {
                    return Poll::Ready(Some(Ok(data)));
                }
                Some(Err(e)) => {
                    self.done = true;
                    if let Some(token) = self.finish_stream() {
                        token.fail();
                    }
                    // convert error type for better formatting of the error chain
                    let e = Error::StreamingFlightSql { source: e };
                    return Poll::Ready(Some(Err(e.into())));
                }
            }
        }
    }
}

/// Header/trailer data added to query responses.
#[derive(Debug, Clone)]
struct QueryResponseMetadata {
    log_entry: Option<Arc<QueryLogEntry>>,
}

impl QueryResponseMetadata {
    fn write_trailer_count<N: ToString>(md: &mut HeaderMap, key: &'static str, count: Option<N>) {
        let Some(count) = count else { return };
        md.insert(key, count.to_string().parse().expect("always valid"));
    }

    fn write_trailer_duration(md: &mut HeaderMap, key: &'static str, d: Option<Duration>) {
        let Some(d) = d else { return };

        md.insert(
            key,
            d.as_secs_f64().to_string().parse().expect("always valid"),
        );
    }

    fn write_trailers(&self, md: &mut HeaderMap) {
        let Some(log_entry) = &self.log_entry else {
            return;
        };

        let state = log_entry.state();
        let QueryLogEntryState {
            id: _,
            namespace_id: _,
            namespace_name: _,
            query_type: _,
            query_text: _,
            query_params: _,
            auth_id: _,
            trace_id: _,
            issue_time: _,
            partitions,
            parquet_files,
            permit_duration,
            plan_duration,
            execute_duration,
            end2end_duration: _,
            compute_duration,
            max_memory,
            success: _,
            running: _,
            phase: _,
            ingester_metrics,
            deduplicated_parquet_files: _,
            deduplicated_partitions: _,
        } = state.as_ref();

        Self::write_trailer_duration(
            md,
            IOX_FLIGHT_QUEUE_DURATION_RESPONSE_TRAILER,
            *permit_duration,
        );
        Self::write_trailer_duration(
            md,
            IOX_FLIGHT_PLANNING_DURATION_RESPONSE_TRAILER,
            *plan_duration,
        );
        Self::write_trailer_duration(
            md,
            IOX_FLIGHT_EXECUTION_DURATION_RESPONSE_TRAILER,
            *execute_duration,
        );
        Self::write_trailer_duration(
            md,
            IOX_FLIGHT_COMPUTE_DURATION_RESPONSE_TRAILER,
            *compute_duration,
        );
        Self::write_trailer_count(md, IOX_FLIGHT_PARTITIONS_RESPONSE_TRAILER, *partitions);
        Self::write_trailer_count(
            md,
            IOX_FLIGHT_PARQUET_FILES_RESPONSE_TRAILER,
            *parquet_files,
        );
        Self::write_trailer_count(md, IOX_FLIGHT_MAX_MEMORY_RESPONSE_TRAILER, *max_memory);
        Self::write_trailer_duration(
            md,
            IOX_FLIGHT_INGESTER_LATENCY_PLAN_RESPONSE_TRAILER,
            ingester_metrics.map(|x| x.latency_to_plan),
        );
        Self::write_trailer_duration(
            md,
            IOX_FLIGHT_INGESTER_LATENCY_DATA_RESPONSE_TRAILER,
            ingester_metrics.map(|x| x.latency_to_full_data),
        );
        Self::write_trailer_count(
            md,
            IOX_FLIGHT_INGESTER_RESPONSE_ROWS_RESPONSE_TRAILER,
            ingester_metrics.map(|x| x.response_rows as i64),
        );
        Self::write_trailer_count(
            md,
            IOX_FLIGHT_INGESTER_PATITION_COUNT_RESPONSE_TRAILER,
            ingester_metrics.map(|x| x.partition_count as i64),
        );
        Self::write_trailer_count(
            md,
            IOX_FLIGHT_INGESTER_RESPONSE_BYTES_RESPONSE_TRAILER,
            ingester_metrics.map(|x| x.response_size as i64),
        );
    }
}

#[cfg(test)]
mod tests {
    use arrow_flight::sql::ProstMessageExt;
    use async_trait::async_trait;
    use authz::{Authorization, Permission};
    use futures::Future;
    use generated_types::metadata::{MetadataKey, MetadataValue};
    use iox_query::test::TestDatabaseStore;
    use metric::{Attributes, Metric, U64Gauge};
    use test_helpers::maybe_start_logging;
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
            server: Arc::clone(&test_storage) as _,
            authz: Option::<Arc<dyn Authorizer>>::None,
        };
        let ticket = Ticket {
            ticket: br#"{"namespace_name": "my_db", "sql_query": "SELECT 1;"}"#
                .to_vec()
                .into(),
        };
        let mut streaming_resp1 = service
            .do_get(Request::new(ticket.clone()))
            .await
            .unwrap()
            .into_inner();
        streaming_resp1.next().await.unwrap().unwrap(); // schema (planning)
        streaming_resp1.next().await.unwrap().unwrap(); // record batch (execution)

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

        let mut streaming_resp2 = service
            .do_get(Request::new(ticket.clone()))
            .await
            .unwrap()
            .into_inner();
        streaming_resp2.next().await.unwrap().unwrap(); // schema (planning)
        streaming_resp2.next().await.unwrap().unwrap(); // record batch (execution)

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
        let mut streaming_resp3 = service
            .do_get(Request::new(ticket.clone()))
            .await
            .unwrap()
            .into_inner();
        streaming_resp3.next().await.unwrap().unwrap(); // schema (planning)
        let fut = streaming_resp3.next(); // record batch (execution)
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
        fut.await.unwrap().unwrap();

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

    #[track_caller]
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
        async fn authorize(
            &self,
            token: Option<Vec<u8>>,
            perms: &[Permission],
        ) -> Result<Authorization, authz::Error> {
            match token {
                Some(token) => match (&token as &dyn AsRef<[u8]>).as_ref() {
                    b"GOOD" => Ok(Authorization::new(
                        Some("GGOD user".to_owned()),
                        perms.to_vec(),
                    )),
                    b"BAD" => Err(authz::Error::Forbidden {
                        authorization: Authorization::new(Some("BAD user".to_owned()), vec![]),
                    }),
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
        maybe_start_logging();

        let test_storage = Arc::new(TestDatabaseStore::default());
        test_storage.db_or_create("bananas").await;

        let svc = FlightService {
            server: Arc::clone(&test_storage) as _,
            authz: Some(Arc::new(MockAuthorizer {})),
        };

        async fn assert_code(svc: &FlightService, want: Code, request: Request<Ticket>) {
            let got = match svc.do_get(request).await {
                Ok(_) => Code::Ok,
                Err(e) => e.code(),
            };
            assert_eq!(want, got);
        }

        fn request(query: RunQuery, authorization: &'static str) -> Request<arrow_flight::Ticket> {
            let mut req = Request::new(
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

        fn sql_request(authorization: &'static str) -> Request<arrow_flight::Ticket> {
            request(RunQuery::Sql("SELECT 1".to_string()), authorization)
        }

        fn influxql_request(authorization: &'static str) -> Request<arrow_flight::Ticket> {
            request(
                RunQuery::InfluxQL("SHOW DATABASES".to_string()),
                authorization,
            )
        }

        fn flightsql_request(authorization: &'static str) -> Request<arrow_flight::Ticket> {
            request(
                RunQuery::FlightSQL(FlightSQLCommand::CommandGetCatalogs(
                    arrow_flight::sql::CommandGetCatalogs {},
                )),
                authorization,
            )
        }

        assert_code(&svc, Code::Unauthenticated, sql_request("")).await;
        assert_code(&svc, Code::Ok, sql_request("Bearer GOOD")).await;
        assert_code(&svc, Code::PermissionDenied, sql_request("Bearer BAD")).await;
        assert_code(&svc, Code::PermissionDenied, sql_request("Bearer INVALID")).await;
        assert_code(&svc, Code::Unavailable, sql_request("Bearer UGLY")).await;

        assert_code(&svc, Code::Unauthenticated, influxql_request("")).await;

        assert_code(
            &svc,
            Code::InvalidArgument, // SHOW DATABASE has not been implemented yet.
            influxql_request("Bearer GOOD"),
        )
        .await;
        assert_code(&svc, Code::PermissionDenied, influxql_request("Bearer BAD")).await;
        assert_code(&svc, Code::Unavailable, influxql_request("Bearer UGLY")).await;

        assert_code(&svc, Code::Unauthenticated, flightsql_request("")).await;
        assert_code(&svc, Code::Ok, flightsql_request("Bearer GOOD")).await;
        assert_code(
            &svc,
            Code::PermissionDenied,
            flightsql_request("Bearer BAD"),
        )
        .await;
        assert_code(&svc, Code::Unavailable, flightsql_request("Bearer UGLY")).await;
    }

    #[tokio::test]
    async fn get_flight_info_authz() {
        let test_storage = Arc::new(TestDatabaseStore::default());
        test_storage.db_or_create("bananas").await;

        let svc = FlightService {
            server: Arc::clone(&test_storage) as _,
            authz: Some(Arc::new(MockAuthorizer {})),
        };

        async fn assert_code(svc: &FlightService, want: Code, request: Request<FlightDescriptor>) {
            let got = match svc.get_flight_info(request).await {
                Ok(_) => Code::Ok,
                Err(e) => e.code(),
            };
            assert_eq!(want, got);
        }

        fn request(authorization: &'static str) -> Request<FlightDescriptor> {
            let cmd = arrow_flight::sql::CommandGetCatalogs {};
            let mut req = Request::new(FlightDescriptor::new_cmd(cmd.as_any().encode_to_vec()));
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

        assert_code(&svc, Code::Unauthenticated, request("")).await;
        assert_code(&svc, Code::Ok, request("Bearer GOOD")).await;
        assert_code(&svc, Code::PermissionDenied, request("Bearer BAD")).await;
        assert_code(&svc, Code::Unavailable, request("Bearer UGLY")).await;
    }
}
