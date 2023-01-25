//! FlightSQL handling
use std::{string::FromUtf8Error, sync::Arc};

use arrow::{error::ArrowError, ipc::writer::IpcWriteOptions};
use arrow_flight::{
    error::FlightError,
    sql::{
        ActionClosePreparedStatementRequest, ActionCreatePreparedStatementRequest,
        ActionCreatePreparedStatementResult, Any, CommandPreparedStatementQuery,
        CommandStatementQuery,
    },
    IpcMessage, SchemaAsIpc,
};
use bytes::Bytes;
use datafusion::{error::DataFusionError, physical_plan::ExecutionPlan};
use iox_query::{exec::IOxSessionContext, QueryNamespace};
use observability_deps::tracing::debug;
use prost::Message;
use snafu::{ResultExt, Snafu};

#[allow(clippy::enum_variant_names)]
#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Invalid protobuf for type_url '{}': {}", type_url, source))]
    DeserializationTypeKnown {
        type_url: String,
        source: prost::DecodeError,
    },

    #[snafu(display("Invalid PreparedStatement handle (invalid UTF-8:) {}", source))]
    InvalidHandle { source: FromUtf8Error },

    #[snafu(display("{}", source))]
    Flight { source: FlightError },

    #[snafu(display("{}", source))]
    DataFusion { source: DataFusionError },

    #[snafu(display("{}", source))]
    Arrow { source: ArrowError },

    #[snafu(display("Unsupported FlightSQL message type: {}", description))]
    UnsupportedMessageType { description: String },

    #[snafu(display("Protocol error. Method {} does not expect '{:?}'", method, cmd))]
    Protocol { cmd: String, method: &'static str },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

impl From<FlightError> for Error {
    fn from(source: FlightError) -> Self {
        Self::Flight { source }
    }
}

impl From<ArrowError> for Error {
    fn from(source: ArrowError) -> Self {
        Self::Arrow { source }
    }
}

impl From<Error> for DataFusionError {
    fn from(value: Error) -> Self {
        match value {
            Error::DataFusion { source } => source,
            Error::Arrow { source } => DataFusionError::ArrowError(source),
            value => DataFusionError::External(Box::new(value)),
        }
    }
}

/// Logic for creating plans for various Flight messages against a query database
#[derive(Debug, Default)]
pub struct FlightSQLPlanner {}

impl FlightSQLPlanner {
    pub fn new() -> Self {
        Self {}
    }

    /// Returns the schema, in Arrow IPC encoded form, for the request in msg.
    pub async fn get_flight_info(
        namespace_name: impl Into<String>,
        msg: Any,
        ctx: &IOxSessionContext,
    ) -> Result<Bytes> {
        let namespace_name = namespace_name.into();
        debug!(%namespace_name, type_url=%msg.type_url, "Handling flightsql get_flight_info");

        let cmd = FlightSQLCommand::try_new(&msg)?;
        match cmd {
            FlightSQLCommand::CommandStatementQuery(query) => {
                Self::get_schema_for_query(&query, ctx).await
            }
            FlightSQLCommand::CommandPreparedStatementQuery(handle) => {
                Self::get_schema_for_query(&handle.query, ctx).await
            }
            _ => ProtocolSnafu {
                cmd: format!("{cmd:?}"),
                method: "GetFlightInfo",
            }
            .fail(),
        }
    }

    /// Return the schema for the specified query
    ///
    /// returns: IPC encoded (schema_bytes) for this query
    async fn get_schema_for_query(query: &str, ctx: &IOxSessionContext) -> Result<Bytes> {
        // gather real schema, but only
        let logical_plan = ctx.plan_sql(query).await.context(DataFusionSnafu)?;
        let schema = arrow::datatypes::Schema::from(logical_plan.schema().as_ref());
        let options = IpcWriteOptions::default();

        // encode the schema into the correct form
        let IpcMessage(schema) = SchemaAsIpc::new(&schema, &options)
            .try_into()
            .context(ArrowSnafu)?;

        Ok(schema)
    }

    /// Returns a plan that computes results requested in msg
    pub async fn do_get(
        namespace_name: impl Into<String>,
        _database: Arc<dyn QueryNamespace>,
        msg: Any,
        ctx: &IOxSessionContext,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let namespace_name = namespace_name.into();
        debug!(%namespace_name, type_url=%msg.type_url, "Handling flightsql do_get");

        let cmd = FlightSQLCommand::try_new(&msg)?;
        match cmd {
            FlightSQLCommand::CommandStatementQuery(query) => {
                debug!(%query, "Planning FlightSQL query");
                ctx.prepare_sql(&query).await.context(DataFusionSnafu)
            }
            FlightSQLCommand::CommandPreparedStatementQuery(handle) => {
                let query = &handle.query;
                debug!(%query, "Planning FlightSQL prepared query");
                ctx.prepare_sql(query).await.context(DataFusionSnafu)
            }
            _ => ProtocolSnafu {
                cmd: format!("{cmd:?}"),
                method: "DoGet",
            }
            .fail(),
        }
    }

    /// Handles the action specified in `msg` and returns bytes for
    /// the [`arrow_flight::Result`] (not the same as a rust
    /// [`Result`]!)
    pub async fn do_action(
        namespace_name: impl Into<String>,
        _database: Arc<dyn QueryNamespace>,
        msg: Any,
        ctx: &IOxSessionContext,
    ) -> Result<Bytes> {
        let namespace_name = namespace_name.into();
        debug!(%namespace_name, type_url=%msg.type_url, "Handling flightsql do_action");

        let cmd = FlightSQLCommand::try_new(&msg)?;
        match cmd {
            FlightSQLCommand::ActionCreatePreparedStatementRequest(query) => {
                debug!(%query, "Creating prepared statement");

                // todo run the planner here and actually figure out parameter schemas
                // see https://github.com/apache/arrow-datafusion/pull/4701
                let parameter_schema = vec![];

                let dataset_schema = Self::get_schema_for_query(&query, ctx).await?;
                let handle = PreparedStatementHandle::new(query);

                let result = ActionCreatePreparedStatementResult {
                    prepared_statement_handle: Bytes::from(handle),
                    dataset_schema,
                    parameter_schema: Bytes::from(parameter_schema),
                };

                let msg = Any::pack(&result)?;
                Ok(msg.encode_to_vec().into())
            }
            FlightSQLCommand::ActionClosePreparedStatementRequest(handle) => {
                let query = &handle.query;
                debug!(%query, "Closing prepared statement");

                // Nothing really to do
                Ok(Bytes::new())
            }
            _ => ProtocolSnafu {
                cmd: format!("{cmd:?}"),
                method: "DoAction",
            }
            .fail(),
        }
    }
}

/// Represents a prepared statement "handle". IOx passes all state
/// required to run the prepared statement back and forth to the
/// client so any querier instance can run it
#[derive(Debug, Clone)]
struct PreparedStatementHandle {
    /// The raw SQL query text
    query: String,
}

impl PreparedStatementHandle {
    fn new(query: String) -> Self {
        Self { query }
    }
}

/// Decode bytes to a PreparedStatementHandle
impl TryFrom<Bytes> for PreparedStatementHandle {
    type Error = Error;

    fn try_from(handle: Bytes) -> Result<Self, Self::Error> {
        // Note: in IOx  handles are the entire decoded query
        let query = String::from_utf8(handle.to_vec()).context(InvalidHandleSnafu)?;
        Ok(Self { query })
    }
}

/// Encode a PreparedStatementHandle as Bytes
impl From<PreparedStatementHandle> for Bytes {
    fn from(value: PreparedStatementHandle) -> Self {
        Bytes::from(value.query.into_bytes())
    }
}

/// Decoded /  validated FlightSQL command messages
#[derive(Debug, Clone)]
enum FlightSQLCommand {
    CommandStatementQuery(String),
    /// Run a prepared statement
    CommandPreparedStatementQuery(PreparedStatementHandle),
    /// Create a prepared statement
    ActionCreatePreparedStatementRequest(String),
    /// Close a prepared statement
    ActionClosePreparedStatementRequest(PreparedStatementHandle),
}

impl FlightSQLCommand {
    /// Figure out and decode the specific FlightSQL command in `msg` and decode it to a native IOx / Rust struct
    fn try_new(msg: &Any) -> Result<Self> {
        if let Some(decoded_cmd) = Any::unpack::<CommandStatementQuery>(msg)? {
            let CommandStatementQuery { query } = decoded_cmd;
            Ok(Self::CommandStatementQuery(query))
        } else if let Some(decoded_cmd) = Any::unpack::<CommandPreparedStatementQuery>(msg)? {
            let CommandPreparedStatementQuery {
                prepared_statement_handle,
            } = decoded_cmd;

            let handle = PreparedStatementHandle::try_from(prepared_statement_handle)?;
            Ok(Self::CommandPreparedStatementQuery(handle))
        } else if let Some(decoded_cmd) = Any::unpack::<ActionCreatePreparedStatementRequest>(msg)?
        {
            let ActionCreatePreparedStatementRequest { query } = decoded_cmd;
            Ok(Self::ActionCreatePreparedStatementRequest(query))
        } else if let Some(decoded_cmd) = Any::unpack::<ActionClosePreparedStatementRequest>(msg)? {
            let ActionClosePreparedStatementRequest {
                prepared_statement_handle,
            } = decoded_cmd;
            let handle = PreparedStatementHandle::try_from(prepared_statement_handle)?;
            Ok(Self::ActionClosePreparedStatementRequest(handle))
        } else {
            UnsupportedMessageTypeSnafu {
                description: &msg.type_url,
            }
            .fail()
        }
    }
}
