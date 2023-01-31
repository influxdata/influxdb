//! IOx FlightSQL Command structures

use std::fmt::Display;

use arrow_flight::sql::{
    ActionClosePreparedStatementRequest, ActionCreatePreparedStatementRequest, Any,
    CommandPreparedStatementQuery, CommandStatementQuery,
};
use bytes::Bytes;
use prost::Message;
use snafu::ResultExt;

use crate::error::*;

/// Represents a prepared statement "handle". IOx passes all state
/// required to run the prepared statement back and forth to the
/// client so any querier instance can run it
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

/// Decoded /  validated FlightSQL command messages
#[derive(Debug, Clone, PartialEq)]
pub enum FlightSQLCommand {
    CommandStatementQuery(String),
    /// Run a prepared statement
    CommandPreparedStatementQuery(PreparedStatementHandle),
    /// Create a prepared statement
    ActionCreatePreparedStatementRequest(String),
    /// Close a prepared statement
    ActionClosePreparedStatementRequest(PreparedStatementHandle),
}

impl Display for FlightSQLCommand {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::CommandStatementQuery(q) => write!(f, "CommandStatementQuery{q}"),
            Self::CommandPreparedStatementQuery(h) => write!(f, "CommandPreparedStatementQuery{h}"),
            Self::ActionCreatePreparedStatementRequest(q) => {
                write!(f, "ActionCreatePreparedStatementRequest{q}")
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
            let CommandStatementQuery { query } = decoded_cmd;
            Ok(Self::CommandStatementQuery(query))
        } else if let Some(decoded_cmd) = Any::unpack::<CommandPreparedStatementQuery>(&msg)? {
            let CommandPreparedStatementQuery {
                prepared_statement_handle,
            } = decoded_cmd;

            let handle = PreparedStatementHandle::try_decode(prepared_statement_handle)?;
            Ok(Self::CommandPreparedStatementQuery(handle))
        } else if let Some(decoded_cmd) = Any::unpack::<ActionCreatePreparedStatementRequest>(&msg)?
        {
            let ActionCreatePreparedStatementRequest { query } = decoded_cmd;
            Ok(Self::ActionCreatePreparedStatementRequest(query))
        } else if let Some(decoded_cmd) = Any::unpack::<ActionClosePreparedStatementRequest>(&msg)?
        {
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
            FlightSQLCommand::CommandStatementQuery(query) => {
                Any::pack(&CommandStatementQuery { query })
            }
            FlightSQLCommand::CommandPreparedStatementQuery(handle) => {
                let prepared_statement_handle = handle.encode();
                Any::pack(&CommandPreparedStatementQuery {
                    prepared_statement_handle,
                })
            }
            FlightSQLCommand::ActionCreatePreparedStatementRequest(query) => {
                Any::pack(&ActionCreatePreparedStatementRequest { query })
            }
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
