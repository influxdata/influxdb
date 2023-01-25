//! IOx FlightSQL Command structures

use arrow_flight::sql::{
    ActionClosePreparedStatementRequest, ActionCreatePreparedStatementRequest, Any,
    CommandPreparedStatementQuery, CommandStatementQuery,
};
use bytes::Bytes;
use snafu::ResultExt;

use crate::error::*;

/// Represents a prepared statement "handle". IOx passes all state
/// required to run the prepared statement back and forth to the
/// client so any querier instance can run it
#[derive(Debug, Clone)]
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
pub enum FlightSQLCommand {
    CommandStatementQuery(String),
    /// Run a prepared statement
    CommandPreparedStatementQuery(PreparedStatementHandle),
    /// Create a prepared statement
    ActionCreatePreparedStatementRequest(String),
    /// Close a prepared statement
    ActionClosePreparedStatementRequest(PreparedStatementHandle),
}

impl FlightSQLCommand {
    /// Figure out and decode the specific FlightSQL command in `msg`
    /// and decode it to a native IOx / Rust struct
    pub fn try_new(msg: &Any) -> Result<Self> {
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
