//! IOx FlightSQL Command structures

use std::fmt::Display;

use arrow_flight::sql::{
    ActionClosePreparedStatementRequest, ActionCreatePreparedStatementRequest, Any,
    CommandGetCatalogs, CommandGetDbSchemas, CommandGetSqlInfo, CommandGetTableTypes,
    CommandGetTables, CommandPreparedStatementQuery, CommandStatementQuery,
};
use bytes::Bytes;
use prost::Message;
use snafu::ResultExt;

use crate::error::*;

/// Represents a prepared statement "handle". IOx passes all state
/// required to run the prepared statement back and forth to the
/// client, so any querier instance can run it
#[derive(Debug, Clone, PartialEq)]
pub struct PreparedStatementHandle {
    /// The raw SQL query text
    query: String,
}

impl PreparedStatementHandle {
    pub fn new(query: String) -> Self {
        Self { query }
    }

    /// return the query
    pub fn query(&self) -> &str {
        self.query.as_ref()
    }

    fn try_decode(handle: Bytes) -> Result<Self> {
        // Note: in IOx  handles are the entire decoded query
        // It will likely need to get more sophisticated as part of
        // https://github.com/influxdata/influxdb_iox/issues/6699
        let query = String::from_utf8(handle.to_vec()).context(InvalidHandleSnafu)?;
        Ok(Self { query })
    }

    fn encode(self) -> Bytes {
        Bytes::from(self.query.into_bytes())
    }
}

impl Display for PreparedStatementHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Pepared({})", self.query)
    }
}

/// Encode a PreparedStatementHandle as Bytes
impl From<PreparedStatementHandle> for Bytes {
    fn from(value: PreparedStatementHandle) -> Self {
        Bytes::from(value.query.into_bytes())
    }
}

/// Decoded / validated FlightSQL command messages
///
/// Handles encoding/decoding prost::Any messages back
/// and forth to native Rust types
///
/// TODO use / contribute upstream arrow-flight implementation, when ready:
/// <https://github.com/apache/arrow-rs/issues/3874>
#[derive(Debug, Clone, PartialEq)]
pub enum FlightSQLCommand {
    /// Run a normal query
    CommandStatementQuery(CommandStatementQuery),
    /// Run a prepared statement.
    CommandPreparedStatementQuery(PreparedStatementHandle),
    /// Get information about the SQL supported
    CommandGetSqlInfo(CommandGetSqlInfo),
    /// Get a list of the available catalogs. See [`CommandGetCatalogs`] for details.
    CommandGetCatalogs(CommandGetCatalogs),
    /// Get a list of the available schemas. See [`CommandGetDbSchemas`]
    /// for details and how to interpret the parameters.
    CommandGetDbSchemas(CommandGetDbSchemas),
    /// Get a list of the available tables
    CommandGetTables(CommandGetTables),
    /// Get a list of the available table tyypes
    CommandGetTableTypes(CommandGetTableTypes),
    /// Create a prepared statement
    ActionCreatePreparedStatementRequest(ActionCreatePreparedStatementRequest),
    /// Close a prepared statement
    ActionClosePreparedStatementRequest(PreparedStatementHandle),
}

impl Display for FlightSQLCommand {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::CommandStatementQuery(CommandStatementQuery { query }) => {
                write!(f, "CommandStatementQuery{query}")
            }
            Self::CommandPreparedStatementQuery(h) => write!(f, "CommandPreparedStatementQuery{h}"),
            Self::CommandGetSqlInfo(CommandGetSqlInfo { info: _ }) => {
                write!(f, "CommandGetSqlInfo(...)")
            }
            Self::CommandGetCatalogs(CommandGetCatalogs {}) => write!(f, "CommandGetCatalogs"),
            Self::CommandGetDbSchemas(CommandGetDbSchemas {
                catalog,
                db_schema_filter_pattern,
            }) => {
                write!(
                    f,
                    "CommandGetCatalogs(catalog={}, db_schema_filter_pattern={}",
                    catalog.as_ref().map(|c| c.as_str()).unwrap_or("<NONE>"),
                    db_schema_filter_pattern
                        .as_ref()
                        .map(|c| c.as_str())
                        .unwrap_or("<NONE>")
                )
            }
            Self::CommandGetTables(CommandGetTables {
                catalog,
                db_schema_filter_pattern,
                table_name_filter_pattern,
                table_types,
                include_schema,
            }) => {
                write!(
                    f,
                    "CommandGetTables(catalog={}, db_schema_filter_pattern={},\
                        table_name_filter_pattern={},table_types={},include_schema={})",
                    catalog.as_ref().map(|c| c.as_ref()).unwrap_or("<NONE>"),
                    db_schema_filter_pattern
                        .as_ref()
                        .map(|db| db.as_ref())
                        .unwrap_or("<NONE>"),
                    table_name_filter_pattern
                        .as_ref()
                        .map(|t| t.as_ref())
                        .unwrap_or("<NONE>"),
                    table_types.join(","),
                    include_schema,
                )
            }
            Self::CommandGetTableTypes(CommandGetTableTypes {}) => {
                write!(f, "CommandGetTableTypes")
            }
            Self::ActionCreatePreparedStatementRequest(ActionCreatePreparedStatementRequest {
                query,
            }) => {
                write!(f, "ActionCreatePreparedStatementRequest{query}")
            }
            Self::ActionClosePreparedStatementRequest(h) => {
                write!(f, "ActionClosePreparedStatementRequest{h}")
            }
        }
    }
}

impl FlightSQLCommand {
    /// Figure out and decode the specific FlightSQL command in `msg`
    /// and decode it to a native IOx / Rust struct
    pub fn try_decode(msg: Bytes) -> Result<Self> {
        let msg: Any = Message::decode(msg)?;

        if let Some(decoded_cmd) = Any::unpack::<CommandStatementQuery>(&msg)? {
            Ok(Self::CommandStatementQuery(decoded_cmd))
        } else if let Some(decoded_cmd) = Any::unpack::<CommandPreparedStatementQuery>(&msg)? {
            let CommandPreparedStatementQuery {
                prepared_statement_handle,
            } = decoded_cmd;
            // Decode to IOx specific structure
            let handle = PreparedStatementHandle::try_decode(prepared_statement_handle)?;
            Ok(Self::CommandPreparedStatementQuery(handle))
        } else if let Some(decoded_cmd) = Any::unpack::<CommandGetSqlInfo>(&msg)? {
            Ok(Self::CommandGetSqlInfo(decoded_cmd))
        } else if let Some(decoded_cmd) = Any::unpack::<CommandGetCatalogs>(&msg)? {
            Ok(Self::CommandGetCatalogs(decoded_cmd))
        } else if let Some(decoded_cmd) = Any::unpack::<CommandGetDbSchemas>(&msg)? {
            Ok(Self::CommandGetDbSchemas(decoded_cmd))
        } else if let Some(decode_cmd) = Any::unpack::<CommandGetTables>(&msg)? {
            Ok(Self::CommandGetTables(decode_cmd))
        } else if let Some(decoded_cmd) = Any::unpack::<CommandGetTableTypes>(&msg)? {
            Ok(Self::CommandGetTableTypes(decoded_cmd))
        } else if let Some(decoded_cmd) = Any::unpack::<ActionCreatePreparedStatementRequest>(&msg)?
        {
            Ok(Self::ActionCreatePreparedStatementRequest(decoded_cmd))
        } else if let Some(decoded_cmd) = Any::unpack::<ActionClosePreparedStatementRequest>(&msg)?
        {
            // Decode to IOx specific structure
            let ActionClosePreparedStatementRequest {
                prepared_statement_handle,
            } = decoded_cmd;
            let handle = PreparedStatementHandle::try_decode(prepared_statement_handle)?;
            Ok(Self::ActionClosePreparedStatementRequest(handle))
        } else {
            UnsupportedMessageTypeSnafu {
                description: &msg.type_url,
            }
            .fail()
        }
    }

    // Encode the command as a flightsql message (bytes)
    pub fn try_encode(self) -> Result<Bytes> {
        let msg = match self {
            FlightSQLCommand::CommandStatementQuery(cmd) => Any::pack(&cmd),
            FlightSQLCommand::CommandPreparedStatementQuery(handle) => {
                let prepared_statement_handle = handle.encode();
                let cmd = CommandPreparedStatementQuery {
                    prepared_statement_handle,
                };
                Any::pack(&cmd)
            }
            FlightSQLCommand::CommandGetSqlInfo(cmd) => Any::pack(&cmd),
            FlightSQLCommand::CommandGetCatalogs(cmd) => Any::pack(&cmd),
            FlightSQLCommand::CommandGetDbSchemas(cmd) => Any::pack(&cmd),
            FlightSQLCommand::CommandGetTables(cmd) => Any::pack(&cmd),
            FlightSQLCommand::CommandGetTableTypes(cmd) => Any::pack(&cmd),
            FlightSQLCommand::ActionCreatePreparedStatementRequest(cmd) => Any::pack(&cmd),
            FlightSQLCommand::ActionClosePreparedStatementRequest(handle) => {
                let prepared_statement_handle = handle.encode();
                Any::pack(&ActionClosePreparedStatementRequest {
                    prepared_statement_handle,
                })
            }
        }?;
        Ok(msg.encode_to_vec().into())
    }
}
