//! FlightSQL handling
use std::{string::FromUtf8Error, sync::Arc};

use arrow::{error::ArrowError, ipc::writer::IpcWriteOptions};
use arrow_flight::{
    error::FlightError,
    sql::{Any, CommandPreparedStatementQuery, CommandStatementQuery, ProstMessageExt},
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

    #[snafu(display("Query was not valid UTF-8: {}", source))]
    InvalidUtf8 { source: FromUtf8Error },

    #[snafu(display("{}", source))]
    Flight { source: FlightError },

    #[snafu(display("{}", source))]
    DataFusion { source: DataFusionError },

    #[snafu(display("{}", source))]
    Arrow { source: ArrowError },

    #[snafu(display("Unsupported FlightSQL message type: {}", description))]
    UnsupportedMessageType { description: String },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

impl From<FlightError> for Error {
    fn from(value: FlightError) -> Self {
        Self::Flight { source: value }
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

        match FlightSQLCommand::try_new(&msg)? {
            FlightSQLCommand::CommandStatementQuery(query)
            | FlightSQLCommand::CommandPreparedStatementQuery(query) => {
                Self::get_schema_for_query(&query, ctx).await
            }
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
        debug!(%namespace_name, type_url=%msg.type_url, "Handling flightsql plan to run an actual query");

        match FlightSQLCommand::try_new(&msg)? {
            FlightSQLCommand::CommandStatementQuery(query) => {
                debug!(%query, "Planning FlightSQL query");
                ctx.prepare_sql(&query).await.context(DataFusionSnafu)
            }
            FlightSQLCommand::CommandPreparedStatementQuery(query) => {
                debug!(%query, "Planning FlightSQL prepared query");
                ctx.prepare_sql(&query).await.context(DataFusionSnafu)
            }
        }
    }
}

/// Decoded and validated FlightSQL command
#[derive(Debug, Clone)]
enum FlightSQLCommand {
    CommandStatementQuery(String),
    CommandPreparedStatementQuery(String),
}

impl FlightSQLCommand {
    /// Figure out and decode the specific FlightSQL command in `msg`
    fn try_new(msg: &Any) -> Result<Self> {
        if let Some(decoded_cmd) = try_unpack::<CommandStatementQuery>(msg)? {
            let CommandStatementQuery { query } = decoded_cmd;
            Ok(Self::CommandStatementQuery(query))
        } else if let Some(decoded_cmd) = try_unpack::<CommandPreparedStatementQuery>(msg)? {
            let CommandPreparedStatementQuery {
                prepared_statement_handle,
            } = decoded_cmd;

            // handle should be a decoded query
            let query =
                String::from_utf8(prepared_statement_handle.to_vec()).context(InvalidUtf8Snafu)?;
            Ok(Self::CommandPreparedStatementQuery(query))
        } else {
            UnsupportedMessageTypeSnafu {
                description: &msg.type_url,
            }
            .fail()
        }
    }
}

/// try to unpack the [`arrow_flight::sql::Any`] as type `T`, returning Ok(None) if
/// the type is wrong or Err if an error occurs
fn try_unpack<T: ProstMessageExt>(msg: &Any) -> Result<Option<T>> {
    // Does the type URL match?
    if T::type_url() != msg.type_url {
        return Ok(None);
    }
    // type matched, so try and decode
    let m = Message::decode(&*msg.value).context(DeserializationTypeKnownSnafu {
        type_url: &msg.type_url,
    })?;
    Ok(Some(m))
}
