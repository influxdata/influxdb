//! IOx FlightSQL Command structures

use std::{collections::HashMap, fmt::Display, slice};

use arrow::{
    array::RecordBatch,
    ipc::{
        reader::StreamReader,
        writer::{IpcWriteOptions, StreamWriter},
    },
};
use arrow_flight::sql::{
    ActionClosePreparedStatementRequest, ActionCreatePreparedStatementRequest, Any,
    CommandGetCatalogs, CommandGetCrossReference, CommandGetDbSchemas, CommandGetExportedKeys,
    CommandGetImportedKeys, CommandGetPrimaryKeys, CommandGetSqlInfo, CommandGetTableTypes,
    CommandGetTables, CommandGetXdbcTypeInfo, CommandPreparedStatementQuery, CommandStatementQuery,
};
use arrow_util::display::pretty_format_batches;
use bytes::Bytes;
use datafusion::{common::ParamValues, error::DataFusionError, scalar::ScalarValue};
use generated_types::influxdata::iox::querier::v1::FlightSqlPreparedStatementHandle;
use prost::Message;
use snafu::ResultExt;

use crate::error::*;

/// Represents a prepared statement "handle". IOx passes all state
/// required to run the prepared statement back and forth to the
/// client, so any querier instance can run it
#[derive(Debug, Clone, PartialEq)]
pub struct PreparedStatementHandle {
    /// The raw SQL query text
    pub(crate) query: String,
    /// Any parameters supplied via DoPut(CommandPreparedStatementQuery)
    pub(crate) params: Option<RecordBatch>,
}

impl PreparedStatementHandle {
    const PREPARED_STATEMENT_HANDLE_TYPE_URL: &'static str =
        "type.googleapis.com/influxdata.iox.querier.v1.FlightSqlPreparedStatementHandle";

    pub(crate) fn new(query: impl Into<String>) -> Self {
        Self {
            query: query.into(),
            params: None,
        }
    }

    /// return a reference to the query
    pub fn query(&self) -> &str {
        self.query.as_ref()
    }

    /// return a reference to the params
    pub fn params(&self) -> &Option<RecordBatch> {
        &self.params
    }

    /// Converts the parameters stored in this handle into a map of DataFusion [[ScalarValue]]s
    pub fn to_df_param_values(&self) -> Result<ParamValues, DataFusionError> {
        let map = match &self.params {
            Some(params) => params
                .schema()
                .flattened_fields()
                .into_iter()
                .zip(params.columns())
                .map(|(field, col)| {
                    Ok((
                        field.name().to_owned(),
                        ScalarValue::try_from_array(col, 0)?,
                    ))
                })
                .collect::<Result<HashMap<_, _>, DataFusionError>>()?,

            None => HashMap::default(),
        };
        Ok(ParamValues::Map(map))
    }

    fn try_decode(handle: Bytes) -> Result<Self> {
        match Any::decode(&*handle) {
            Ok(any) => {
                if any.type_url == Self::PREPARED_STATEMENT_HANDLE_TYPE_URL {
                    let decoded_handle = FlightSqlPreparedStatementHandle::decode(any.value)?;
                    let query = decoded_handle.query;
                    // decode Arrow IPC encoded parameters
                    let params = match decoded_handle.params {
                        Some(params) => StreamReader::try_new(params.as_slice(), None)?.next(),
                        None => None,
                    }
                    .transpose()?;
                    Ok(Self { query, params })
                } else {
                    InvalidTypeUrlSnafu {
                        expected: Self::PREPARED_STATEMENT_HANDLE_TYPE_URL,
                        actual: any.type_url,
                    }
                    .fail()
                }
            }
            // initially the prepared statement handle was encoded as a raw UTF-8 string
            // this is a fallback case to support the legacy behavior
            Err(proto_source) => {
                let query = String::from_utf8(handle.to_vec())
                    .context(InvalidHandleSnafu { proto_source })?;
                Ok(Self::new(query))
            }
        }
    }

    fn encode(self) -> Result<Bytes> {
        let params = match self.params {
            Some(params) => {
                let buffer: Vec<u8> = Vec::new();
                let schema = params.schema();
                // encode parameters as Arrow IPC
                let mut writer = StreamWriter::try_new_with_options(
                    buffer,
                    &schema,
                    IpcWriteOptions::default(),
                )?;
                writer.write(&params)?;
                Some(writer.into_inner()?)
            }
            None => None,
        };
        let flightsql_handle = FlightSqlPreparedStatementHandle {
            query: self.query,
            params,
        };
        let any = Any {
            type_url: Self::PREPARED_STATEMENT_HANDLE_TYPE_URL.to_string(),
            value: flightsql_handle.encode_to_vec().into(),
        };
        Ok(any.encode_to_vec().into())
    }
}

impl Display for PreparedStatementHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Prepared({}", self.query)?;
        if let Some(batch) = &self.params {
            write!(
                f,
                ",{}",
                pretty_format_batches(slice::from_ref(batch)).map_err(|_| std::fmt::Error)?
            )?
        };
        write!(f, ")")
    }
}

/// Encode a PreparedStatementHandle as Bytes
impl TryFrom<PreparedStatementHandle> for Bytes {
    type Error = self::Error;
    fn try_from(value: PreparedStatementHandle) -> Result<Self> {
        value.encode()
    }
}

/// Attempt to decode a sequence of Bytes into a PreparedStatementHandle
impl TryFrom<Bytes> for PreparedStatementHandle {
    type Error = crate::Error;
    fn try_from(value: Bytes) -> Result<Self> {
        Self::try_decode(value)
    }
}

/// Decoded / validated FlightSQL command messages
///
/// Handles encoding/decoding prost::Any messages back
/// and forth to native Rust types
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
    /// Get a description of the foreign key columns in the given foreign key table
    /// that reference the primary key or the columns representing a unique constraint
    /// of the parent table (could be the same or a different table).
    /// See [`CommandGetCrossReference`] for details.
    CommandGetCrossReference(CommandGetCrossReference),
    /// Get a list of the available schemas. See [`CommandGetDbSchemas`]
    /// for details and how to interpret the parameters.
    CommandGetDbSchemas(CommandGetDbSchemas),
    /// Get a description of the foreign key columns that reference the given
    /// table's primary key columns (the foreign keys exported by a table) of a table.
    /// See [`CommandGetExportedKeys`] for details.
    CommandGetExportedKeys(CommandGetExportedKeys),
    /// Get the foreign keys of a table. See [`CommandGetImportedKeys`] for details.
    CommandGetImportedKeys(CommandGetImportedKeys),
    /// Get a list of primary keys. See [`CommandGetPrimaryKeys`] for details.
    CommandGetPrimaryKeys(CommandGetPrimaryKeys),
    /// Get a list of the available tables
    CommandGetTables(CommandGetTables),
    /// Get information about data types supported.
    /// See [`CommandGetXdbcTypeInfo`] for details.
    CommandGetXdbcTypeInfo(CommandGetXdbcTypeInfo),
    /// Get a list of the available table types
    CommandGetTableTypes(CommandGetTableTypes),
    /// Create a prepared statement
    ActionCreatePreparedStatementRequest(ActionCreatePreparedStatementRequest),
    /// Close a prepared statement
    ActionClosePreparedStatementRequest(PreparedStatementHandle),
}

impl Display for FlightSQLCommand {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::CommandStatementQuery(CommandStatementQuery { query, .. }) => {
                write!(f, "CommandStatementQuery {query}")
            }
            Self::CommandPreparedStatementQuery(h) => {
                write!(f, "CommandPreparedStatementQuery {h}")
            }
            Self::CommandGetSqlInfo(CommandGetSqlInfo { info: _ }) => {
                write!(f, "CommandGetSqlInfo(...)")
            }
            Self::CommandGetCatalogs(CommandGetCatalogs {}) => write!(f, "CommandGetCatalogs"),
            Self::CommandGetCrossReference(CommandGetCrossReference {
                pk_catalog,
                pk_db_schema,
                pk_table,
                fk_catalog,
                fk_db_schema,
                fk_table,
            }) => {
                write!(
                    f,
                    "CommandGetCrossReference(
                        pk_catalog={},
                        pk_db_schema={},
                        pk_table={},
                        fk_catalog={},
                        fk_db_schema={},
                        fk_table={}",
                    pk_catalog.as_ref().map(|c| c.as_str()).unwrap_or("<NONE>"),
                    pk_db_schema
                        .as_ref()
                        .map(|c| c.as_str())
                        .unwrap_or("<NONE>"),
                    pk_table,
                    fk_catalog.as_ref().map(|c| c.as_str()).unwrap_or("<NONE>"),
                    fk_db_schema
                        .as_ref()
                        .map(|c| c.as_str())
                        .unwrap_or("<NONE>"),
                    fk_table,
                )
            }
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
            Self::CommandGetExportedKeys(CommandGetExportedKeys {
                catalog,
                db_schema,
                table,
            }) => {
                write!(
                    f,
                    "CommandGetExportedKeys(catalog={}, db_schema={}, table={})",
                    catalog.as_ref().map(|c| c.as_str()).unwrap_or("<NONE>"),
                    db_schema.as_ref().map(|c| c.as_str()).unwrap_or("<NONE>"),
                    table
                )
            }
            Self::CommandGetImportedKeys(CommandGetImportedKeys {
                catalog,
                db_schema,
                table,
            }) => {
                write!(
                    f,
                    "CommandGetImportedKeys(catalog={}, db_schema={}, table={})",
                    catalog.as_ref().map(|c| c.as_str()).unwrap_or("<NONE>"),
                    db_schema.as_ref().map(|c| c.as_str()).unwrap_or("<NONE>"),
                    table
                )
            }
            Self::CommandGetPrimaryKeys(CommandGetPrimaryKeys {
                catalog,
                db_schema,
                table,
            }) => {
                write!(
                    f,
                    "CommandGetPrimaryKeys(catalog={}, db_schema={}, table={})",
                    catalog.as_ref().map(|c| c.as_str()).unwrap_or("<NONE>"),
                    db_schema.as_ref().map(|c| c.as_str()).unwrap_or("<NONE>"),
                    table
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
            Self::CommandGetXdbcTypeInfo(CommandGetXdbcTypeInfo { data_type }) => {
                write!(
                    f,
                    "CommandGetXdbcTypeInfo(data_type={})",
                    data_type.as_ref().copied().unwrap_or(0),
                )
            }
            Self::ActionCreatePreparedStatementRequest(ActionCreatePreparedStatementRequest {
                query,
                ..
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
        } else if let Some(decoded_cmd) = Any::unpack::<CommandGetCrossReference>(&msg)? {
            Ok(Self::CommandGetCrossReference(decoded_cmd))
        } else if let Some(decoded_cmd) = Any::unpack::<CommandGetDbSchemas>(&msg)? {
            Ok(Self::CommandGetDbSchemas(decoded_cmd))
        } else if let Some(decoded_cmd) = Any::unpack::<CommandGetExportedKeys>(&msg)? {
            Ok(Self::CommandGetExportedKeys(decoded_cmd))
        } else if let Some(decoded_cmd) = Any::unpack::<CommandGetImportedKeys>(&msg)? {
            Ok(Self::CommandGetImportedKeys(decoded_cmd))
        } else if let Some(decode_cmd) = Any::unpack::<CommandGetPrimaryKeys>(&msg)? {
            Ok(Self::CommandGetPrimaryKeys(decode_cmd))
        } else if let Some(decode_cmd) = Any::unpack::<CommandGetTables>(&msg)? {
            Ok(Self::CommandGetTables(decode_cmd))
        } else if let Some(decoded_cmd) = Any::unpack::<CommandGetTableTypes>(&msg)? {
            Ok(Self::CommandGetTableTypes(decoded_cmd))
        } else if let Some(decoded_cmd) = Any::unpack::<CommandGetXdbcTypeInfo>(&msg)? {
            Ok(Self::CommandGetXdbcTypeInfo(decoded_cmd))
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
            Self::CommandStatementQuery(cmd) => Any::pack(&cmd),
            Self::CommandPreparedStatementQuery(handle) => {
                let prepared_statement_handle = handle.encode()?;
                let cmd = CommandPreparedStatementQuery {
                    prepared_statement_handle,
                };
                Any::pack(&cmd)
            }
            Self::CommandGetSqlInfo(cmd) => Any::pack(&cmd),
            Self::CommandGetCatalogs(cmd) => Any::pack(&cmd),
            Self::CommandGetCrossReference(cmd) => Any::pack(&cmd),
            Self::CommandGetDbSchemas(cmd) => Any::pack(&cmd),
            Self::CommandGetExportedKeys(cmd) => Any::pack(&cmd),
            Self::CommandGetImportedKeys(cmd) => Any::pack(&cmd),
            Self::CommandGetPrimaryKeys(cmd) => Any::pack(&cmd),
            Self::CommandGetTables(cmd) => Any::pack(&cmd),
            Self::CommandGetTableTypes(cmd) => Any::pack(&cmd),
            Self::CommandGetXdbcTypeInfo(cmd) => Any::pack(&cmd),
            Self::ActionCreatePreparedStatementRequest(cmd) => Any::pack(&cmd),
            Self::ActionClosePreparedStatementRequest(handle) => {
                let prepared_statement_handle = handle.encode()?;
                Any::pack(&ActionClosePreparedStatementRequest {
                    prepared_statement_handle,
                })
            }
        }?;
        Ok(msg.encode_to_vec().into())
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use crate::PreparedStatementHandle;

    /// Tests that the older UTF-8 encoded format for the FlightSQL prepared
    /// statement handle can be decoded by the current implementation.
    #[test]
    fn prepared_statement_handle_decoding_compatibility() {
        let handle_bytes = Bytes::from("SELECT 1");
        let handle = PreparedStatementHandle::try_decode(handle_bytes).unwrap();
        assert_eq!(handle.query, "SELECT 1");
        assert_eq!(handle.params, None);
    }
}
