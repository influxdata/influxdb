//! Fork of the Arrow Rust FlightSQL client
//! <https://github.com/apache/arrow-rs/tree/master/arrow-flight/src/sql/client.rs>
//!
//! Plan is to upstream much/all of this to arrow-rs
//! see <https://github.com/apache/arrow-rs/issues/3301>

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::sync::Arc;

use arrow::{
    datatypes::{Schema, SchemaRef},
    error::ArrowError,
    record_batch::RecordBatch,
};
use arrow_flight::{
    Action, FlightClient, FlightDescriptor, FlightInfo, IpcMessage, PutResult, Ticket,
    decode::FlightRecordBatchStream,
    encode::FlightDataEncoderBuilder,
    error::{FlightError, Result},
    sql::{
        ActionCreatePreparedStatementRequest, ActionCreatePreparedStatementResult, Any,
        CommandGetCatalogs, CommandGetCrossReference, CommandGetDbSchemas, CommandGetExportedKeys,
        CommandGetImportedKeys, CommandGetPrimaryKeys, CommandGetSqlInfo, CommandGetTableTypes,
        CommandGetTables, CommandGetXdbcTypeInfo, CommandPreparedStatementQuery,
        CommandStatementQuery, DoPutPreparedStatementResult, ProstMessageExt,
    },
};
use bytes::Bytes;
use futures_util::{Stream, TryStreamExt, stream::BoxStream};
use generated_types::{metadata::MetadataMap, transport::Channel};
use prost::Message;

/// A FlightSQLServiceClient handles details of interacting with a
/// remote server using the FlightSQL protocol.
#[derive(Debug)]
pub struct FlightSqlClient {
    inner: FlightClient,
}

/// An [Arrow Flight SQL](https://arrow.apache.org/docs/format/FlightSql.html) client
/// that can run queries against FlightSql servers.
///
/// If you need more low level control, such as access to response
/// headers, or redirecting to different endpoints, use the lower
/// level [`FlightClient`].
///
/// This client is in the "experimental" stage. It is not guaranteed
/// to follow the spec in all instances.  Github issues are welcomed.
impl FlightSqlClient {
    /// Creates a new FlightSql client that connects to a server over an arbitrary tonic `Channel`
    pub fn new(channel: Channel) -> Self {
        Self::new_from_flight(FlightClient::new(channel))
    }

    /// Create a new client from an existing [`FlightClient`]
    pub fn new_from_flight(inner: FlightClient) -> Self {
        Self { inner }
    }

    /// Return a reference to the underlying [`FlightClient`]
    pub fn inner(&self) -> &FlightClient {
        &self.inner
    }

    /// Return a mutable reference to the underlying [`FlightClient`]
    pub fn inner_mut(&mut self) -> &mut FlightClient {
        &mut self.inner
    }

    /// Consume self and return the inner [`FlightClient`]
    pub fn into_inner(self) -> FlightClient {
        self.inner
    }

    /// Return a reference to gRPC metadata included with each request
    pub fn metadata(&self) -> &MetadataMap {
        self.inner.metadata()
    }

    /// Return a reference to gRPC metadata included with each request
    ///
    /// This can be used, for example, to include authorization or
    /// other headers with each request
    pub fn metadata_mut(&mut self) -> &mut MetadataMap {
        self.inner.metadata_mut()
    }

    /// Add the specified header with value to all subsequent requests
    pub fn add_header(&mut self, key: &str, value: &str) -> Result<()> {
        self.inner.add_header(key, value)
    }

    /// Send `cmd`, encoded as protobuf, to the FlightSQL server
    pub async fn get_flight_info_for_command(
        &mut self,
        cmd: arrow_flight::sql::Any,
    ) -> Result<FlightInfo> {
        let descriptor = FlightDescriptor::new_cmd(cmd.encode_to_vec());
        self.inner.get_flight_info(descriptor).await
    }

    /// Execute a SQL query on the server using [`CommandStatementQuery`]
    ///
    /// This involves two round trips
    ///
    /// Step 1: send a [`CommandStatementQuery`] message to the
    /// `GetFlightInfo` endpoint of the FlightSQL server to receive a
    /// FlightInfo descriptor.
    ///
    /// Step 2: Fetch the results described in the [`FlightInfo`]
    ///
    /// This implementation does not support alternate endpoints
    pub async fn query(
        &mut self,
        query: impl Into<String> + Send,
    ) -> Result<FlightRecordBatchStream> {
        let msg = CommandStatementQuery {
            query: query.into(),
            transaction_id: None,
        };
        self.do_get_with_cmd(msg.as_any()).await
    }

    /// Get information about sql compatibility from this server using [`CommandGetSqlInfo`]
    ///
    /// This implementation does not support alternate endpoints
    ///
    /// * If omitted, then all metadata will be retrieved.
    ///
    /// [`CommandGetSqlInfo`]: https://github.com/apache/arrow/blob/3a6fc1f9eedd41df2d8ffbcbdfbdab911ff6d82e/format/FlightSql.proto#L45-L68
    pub async fn get_sql_info(&mut self, info: Vec<u32>) -> Result<FlightRecordBatchStream> {
        let msg = CommandGetSqlInfo { info };
        self.do_get_with_cmd(msg.as_any()).await
    }

    /// List the catalogs on this server using a [`CommandGetCatalogs`] message.
    ///
    /// This implementation does not support alternate endpoints
    ///
    /// [`CommandGetCatalogs`]: https://github.com/apache/arrow/blob/3a6fc1f9eedd41df2d8ffbcbdfbdab911ff6d82e/format/FlightSql.proto#L1125-L1140
    pub async fn get_catalogs(&mut self) -> Result<FlightRecordBatchStream> {
        let msg = CommandGetCatalogs {};
        self.do_get_with_cmd(msg.as_any()).await
    }

    /// List a description of the foreign key columns in the given foreign key table that
    /// reference the primary key or the columns representing a unique constraint of the
    /// parent table (could be the same or a different table) on this server using a
    /// [`CommandGetCrossReference`] message.
    ///
    /// # Parameters
    ///
    /// Definition from <https://github.com/apache/arrow/blob/f0c8229f5a09fe53186df171d518430243ddf112/format/FlightSql.proto#L1405-L1477>
    ///
    /// pk_catalog: The catalog name where the parent table is.
    /// An empty string retrieves those without a catalog.
    /// If omitted the catalog name should not be used to narrow the search.
    ///
    /// pk_db_schema: The Schema name where the parent table is.
    /// An empty string retrieves those without a schema.
    /// If omitted the schema name should not be used to narrow the search.
    ///
    /// pk_table: The parent table name. It cannot be null.
    ///
    /// fk_catalog: The catalog name where the foreign table is.
    /// An empty string retrieves those without a catalog.
    /// If omitted the catalog name should not be used to narrow the search.
    ///
    /// fk_db_schema: The schema name where the foreign table is.
    /// An empty string retrieves those without a schema.
    /// If omitted the schema name should not be used to narrow the search.
    ///
    /// fk_table: The foreign table name. It cannot be null.
    ///
    /// This implementation does not support alternate endpoints
    pub async fn get_cross_reference(
        &mut self,
        pk_catalog: Option<impl Into<String> + Send>,
        pk_db_schema: Option<impl Into<String> + Send>,
        pk_table: String,
        fk_catalog: Option<impl Into<String> + Send>,
        fk_db_schema: Option<impl Into<String> + Send>,
        fk_table: String,
    ) -> Result<FlightRecordBatchStream> {
        let msg = CommandGetCrossReference {
            pk_catalog: pk_catalog.map(|s| s.into()),
            pk_db_schema: pk_db_schema.map(|s| s.into()),
            pk_table,
            fk_catalog: fk_catalog.map(|s| s.into()),
            fk_db_schema: fk_db_schema.map(|s| s.into()),
            fk_table,
        };
        self.do_get_with_cmd(msg.as_any()).await
    }

    /// List the schemas on this server
    ///
    /// # Parameters
    ///
    /// Definition from <https://github.com/apache/arrow/blob/44edc27e549d82db930421b0d4c76098941afd71/format/FlightSql.proto#L1156-L1173>
    ///
    /// catalog: Specifies the Catalog to search for the tables.
    /// An empty string retrieves those without a catalog.
    /// If omitted the catalog name should not be used to narrow the search.
    ///
    /// db_schema_filter_pattern: Specifies a filter pattern for schemas to search for.
    /// When no db_schema_filter_pattern is provided, the pattern will not be used to narrow the search.
    /// In the pattern string, two special characters can be used to denote matching rules:
    ///    - "%" means to match any substring with 0 or more characters.
    ///    - "_" means to match any one character.
    ///
    /// This implementation does not support alternate endpoints
    pub async fn get_db_schemas(
        &mut self,
        catalog: Option<impl Into<String> + Send>,
        db_schema_filter_pattern: Option<impl Into<String> + Send>,
    ) -> Result<FlightRecordBatchStream> {
        let msg = CommandGetDbSchemas {
            catalog: catalog.map(|s| s.into()),
            db_schema_filter_pattern: db_schema_filter_pattern.map(|s| s.into()),
        };
        self.do_get_with_cmd(msg.as_any()).await
    }

    /// List a description of the foreign key columns that reference the given
    /// table's primary key columns (the foreign keys exported by a table) of a
    /// table on this server using a [`CommandGetExportedKeys`] message.
    ///
    /// # Parameters
    ///
    /// Definition from <https://github.com/apache/arrow/blob/0434ab65075ecd1d2ab9245bcd7ec6038934ed29/format/FlightSql.proto#L1307-L1352>
    ///
    /// catalog: Specifies the catalog to search for the foreign key table.
    /// An empty string retrieves those without a catalog.
    /// If omitted the catalog name should not be used to narrow the search.
    ///
    /// db_schema: Specifies the schema to search for the foreign key table.
    /// An empty string retrieves those without a schema.
    /// If omitted the schema name should not be used to narrow the search.
    ///
    /// table: Specifies the foreign key table to get the foreign keys for.
    ///
    /// This implementation does not support alternate endpoints
    pub async fn get_exported_keys(
        &mut self,
        catalog: Option<impl Into<String> + Send>,
        db_schema: Option<impl Into<String> + Send>,
        table: String,
    ) -> Result<FlightRecordBatchStream> {
        let msg = CommandGetExportedKeys {
            catalog: catalog.map(|s| s.into()),
            db_schema: db_schema.map(|s| s.into()),
            table,
        };
        self.do_get_with_cmd(msg.as_any()).await
    }

    /// List the foreign keys of a table on this server using a
    /// [`CommandGetImportedKeys`] message.
    ///
    /// # Parameters
    ///
    /// Definition from <https://github.com/apache/arrow/blob/196222dbd543d6931f4a1432845add97be0db802/format/FlightSql.proto#L1354-L1403>
    ///
    /// catalog: Specifies the catalog to search for the primary key table.
    /// An empty string retrieves those without a catalog.
    /// If omitted the catalog name should not be used to narrow the search.
    ///
    /// db_schema: Specifies the schema to search for the primary key table.
    /// An empty string retrieves those without a schema.
    /// If omitted the schema name should not be used to narrow the search.
    ///
    /// table: Specifies the primary key table to get the foreign keys for.
    ///
    /// This implementation does not support alternate endpoints
    pub async fn get_imported_keys(
        &mut self,
        catalog: Option<impl Into<String> + Send>,
        db_schema: Option<impl Into<String> + Send>,
        table: String,
    ) -> Result<FlightRecordBatchStream> {
        let msg = CommandGetImportedKeys {
            catalog: catalog.map(|s| s.into()),
            db_schema: db_schema.map(|s| s.into()),
            table,
        };
        self.do_get_with_cmd(msg.as_any()).await
    }

    /// List the primary keys on this server using a [`CommandGetPrimaryKeys`] message.
    ///
    /// # Parameters
    ///
    /// Definition from <https://github.com/apache/arrow/blob/2fe17338e2d1f85d0c2685d31d2dd51f138b6b80/format/FlightSql.proto#L1261-L1297>
    ///
    /// catalog: Specifies the catalog to search for the table.
    /// An empty string retrieves those without a catalog.
    /// If omitted the catalog name should not be used to narrow the search.
    ///
    /// db_schema: Specifies the schema to search for the table.
    /// An empty string retrieves those without a schema.
    /// If omitted the schema name should not be used to narrow the search.
    ///
    /// table: Specifies the table to get the primary keys for.
    ///
    /// This implementation does not support alternate endpoints
    pub async fn get_primary_keys(
        &mut self,
        catalog: Option<impl Into<String> + Send>,
        db_schema: Option<impl Into<String> + Send>,
        table: String,
    ) -> Result<FlightRecordBatchStream> {
        let msg = CommandGetPrimaryKeys {
            catalog: catalog.map(|s| s.into()),
            db_schema: db_schema.map(|s| s.into()),
            table,
        };
        self.do_get_with_cmd(msg.as_any()).await
    }

    /// List the tables on this server using a [`CommandGetTables`] message.
    ///
    /// This implementation does not support alternate endpoints
    ///
    /// [`CommandGetTables`]: https://github.com/apache/arrow/blob/44edc27e549d82db930421b0d4c76098941afd71/format/FlightSql.proto#L1176-L1241
    pub async fn get_tables(
        &mut self,
        catalog: Option<impl Into<String> + Send>,
        db_schema_filter_pattern: Option<impl Into<String> + Send>,
        table_name_filter_pattern: Option<impl Into<String> + Send>,
        table_types: Vec<String>,
        include_schema: bool,
    ) -> Result<FlightRecordBatchStream> {
        let msg = CommandGetTables {
            catalog: catalog.map(|s| s.into()),
            db_schema_filter_pattern: db_schema_filter_pattern.map(|s| s.into()),
            table_name_filter_pattern: table_name_filter_pattern.map(|s| s.into()),
            table_types,
            include_schema,
        };
        self.do_get_with_cmd(msg.as_any()).await
    }

    /// List the table types on this server using a [`CommandGetTableTypes`] message.
    ///
    /// This implementation does not support alternate endpoints
    ///
    /// [`CommandGetTableTypes`]: https://github.com/apache/arrow/blob/44edc27e549d82db930421b0d4c76098941afd71/format/FlightSql.proto#L1243-L1259
    pub async fn get_table_types(&mut self) -> Result<FlightRecordBatchStream> {
        let msg = CommandGetTableTypes {};
        self.do_get_with_cmd(msg.as_any()).await
    }

    /// List information about data type supported on this server
    /// using a [`CommandGetXdbcTypeInfo`] message.
    ///
    /// # Parameters
    ///
    /// Definition from <https://github.com/apache/arrow/blob/9588da967c756b2923e213ccc067378ba6c90a86/format/FlightSql.proto#L1058-L1123>
    ///
    /// data_type: Specifies the data type to search for the info.
    ///
    /// This implementation does not support alternate endpoints
    pub async fn get_xdbc_type_info(
        &mut self,
        data_type: Option<impl Into<i32> + Send>,
    ) -> Result<FlightRecordBatchStream> {
        let msg = CommandGetXdbcTypeInfo {
            data_type: data_type.map(|dt| dt.into()),
        };
        self.do_get_with_cmd(msg.as_any()).await
    }

    /// Implements the canonical interaction for most FlightSQL messages:
    ///
    /// 1. Call `GetFlightInfo` with the provided message, and get a [`FlightInfo`] and embedded
    ///    ticket.
    ///
    /// 2. Call `DoGet` with the provided ticket.
    ///
    /// TODO: example calling with GetDbSchemas
    pub async fn do_get_with_cmd(
        &mut self,
        cmd: arrow_flight::sql::Any,
    ) -> Result<FlightRecordBatchStream> {
        let info = self.get_flight_info_for_command(cmd).await?;
        self.do_get_with_info(info).await
    }

    /// Calls `DoGet` with the provded FlightInfo, which is normally retrieved by calling
    /// [`Self::get_flight_info_for_command`]. This method allows one to provide a ticket (as
    /// opposed to allowing the client to get one with the default options) and then provide that
    /// to be used with a do_get.
    pub async fn do_get_with_info(&mut self, info: FlightInfo) -> Result<FlightRecordBatchStream> {
        let FlightInfo { mut endpoint, .. } = info;

        let flight_endpoint = endpoint.pop().ok_or_else(|| {
            FlightError::protocol("No endpoint specifed in CommandStatementQuery response")
        })?;

        // "If the list is empty, the expectation is that the
        // ticket can only be redeemed on the current service
        // where the ticket was generated."
        //
        // https://github.com/apache/arrow-rs/blob/a0a5880665b1836890f6843b6b8772d81c463351/format/Flight.proto#L292-L294
        if !flight_endpoint.location.is_empty() {
            return Err(FlightError::NotYetImplemented(format!(
                "FlightEndpoint with non empty 'location' not supported ({:?})",
                flight_endpoint.location,
            )));
        }

        if !endpoint.is_empty() {
            return Err(FlightError::NotYetImplemented(format!(
                "Multiple endpoints returned in CommandStatementQuery response ({})",
                endpoint.len() + 1,
            )));
        }

        // Get the underlying ticket
        let ticket = flight_endpoint
            .ticket
            .ok_or_else(|| {
                FlightError::protocol(
                    "No ticket specifed in CommandStatementQuery's FlightInfo response",
                )
            })?
            .ticket;

        self.inner.do_get(Ticket { ticket }).await
    }

    /// Implements the DoPut method for sending a stream of FlightData to the server:
    pub async fn do_put_with_cmd(
        &mut self,
        cmd: arrow_flight::sql::Any,
        record_batch_stream: impl Stream<Item = Result<RecordBatch>> + Send + 'static,
    ) -> Result<BoxStream<'static, Result<PutResult>>> {
        let descriptor = FlightDescriptor::new_cmd(cmd.encode_to_vec());
        let flight_stream_builder =
            FlightDataEncoderBuilder::new().with_flight_descriptor(Some(descriptor));
        let flight_data = flight_stream_builder.build(record_batch_stream);
        self.inner.do_put(flight_data).await
    }

    /// Create a prepared statement for execution.
    ///
    /// This involves two roundtrips:
    ///
    /// Step 1: Sends a [`ActionCreatePreparedStatementRequest`] message to
    /// the `DoAction` endpoint of the FlightSQL server, and returns
    /// the handle from the server.
    ///
    /// If params are given:
    /// Step 2: Add parameters to the prepared statement using `DoPut(` [`CommandStatementQuery`] )
    ///
    /// See [`Self::execute`] to run a previously prepared statement
    pub async fn prepare(
        &mut self,
        query: String,
        params: Option<RecordBatch>,
    ) -> Result<PreparedStatement> {
        let cmd = ActionCreatePreparedStatementRequest {
            query,
            transaction_id: None,
        };

        let request = Action {
            r#type: "CreatePreparedStatement".into(),
            body: cmd.as_any().encode_to_vec().into(),
        };

        let mut results: Vec<Bytes> = self.inner.do_action(request).await?.try_collect().await?;

        if results.len() != 1 {
            return Err(FlightError::ProtocolError(format!(
                "Expected 1 response for preparing a statement, got {}",
                results.len()
            )));
        }
        let result = results.pop().unwrap();

        // decode the response
        let response: arrow_flight::sql::Any = Message::decode(result.as_ref())
            .map_err(|e| FlightError::ExternalError(Box::new(e)))?;

        let ActionCreatePreparedStatementResult {
            mut prepared_statement_handle,
            dataset_schema,
            parameter_schema,
        } = Any::unpack(&response)?.ok_or_else(|| {
            FlightError::ProtocolError(format!(
                "Expected ActionCreatePreparedStatementResult message but got {} instead",
                response.type_url
            ))
        })?;

        // Add parameters to prepared statement and update handle if server provides a new one.
        if let Some(params) = params {
            prepared_statement_handle = self
                .write_bind_params(prepared_statement_handle, params)
                .await?;
        };
        Ok(PreparedStatement::new(
            prepared_statement_handle,
            schema_bytes_to_schema(dataset_schema)?,
            schema_bytes_to_schema(parameter_schema)?,
        ))
    }

    /// Write bind params to the server.
    /// Returns an optionally updated prepared statement, or the old handle if
    /// the server did not provide a new one.
    /// Ignores errors since not all FlightSQL servers implement returning
    /// an updated handle
    async fn write_bind_params(
        &mut self,
        prepared_statement_handle: Bytes,
        params_batch: RecordBatch,
    ) -> Result<Bytes> {
        let cmd = CommandPreparedStatementQuery {
            prepared_statement_handle,
        };
        // run DoPut and get PutResult
        let mut stream = self
            .do_put_with_cmd(cmd.as_any(), futures_util::stream::iter([Ok(params_batch)]))
            .await?;
        if let Ok(Some(put_result)) = stream.try_next().await
            && let Ok(Some(handle)) = self.unpack_prepared_statement_handle(&put_result)
        {
            return Ok(handle);
        }
        Ok(cmd.prepared_statement_handle)
    }

    /// Decodes the app_metadata stored in a [`PutResult`] as a
    /// [`DoPutPreparedStatementResult`] and then returns
    /// the inner prepared statement handle as [`Bytes`]
    fn unpack_prepared_statement_handle(
        &self,
        put_result: &PutResult,
    ) -> std::result::Result<Option<Bytes>, ArrowError> {
        let result: DoPutPreparedStatementResult = Message::decode(&*put_result.app_metadata)
            .map_err(|err| ArrowError::IpcError(err.to_string()))?;
        Ok(result.prepared_statement_handle)
    }

    /// Execute a SQL query on the server using [`CommandStatementQuery`]
    ///
    /// This involves two roundtrips
    ///
    /// Step 1: send a [`CommandStatementQuery`] message to the
    /// `GetFlightInfo` endpoint of the FlightSQL server to receive a
    /// FlightInfo descriptor.
    ///
    /// Step 2: Fetch the results described in the [`FlightInfo`]
    ///
    /// This implementation does not support alternate endpoints
    pub async fn execute(
        &mut self,
        statement: PreparedStatement,
    ) -> Result<FlightRecordBatchStream> {
        let PreparedStatement {
            prepared_statement_handle,
            dataset_schema: _,
            parameter_schema: _,
        } = statement;

        let cmd = CommandPreparedStatementQuery {
            prepared_statement_handle,
        };

        self.do_get_with_cmd(cmd.as_any()).await
    }
}

fn schema_bytes_to_schema(schema: Bytes) -> Result<SchemaRef> {
    let schema = if schema.is_empty() {
        Schema::empty()
    } else {
        Schema::try_from(IpcMessage(schema))?
    };

    Ok(Arc::new(schema))
}

/// represents a "prepared statement handle" on the server
#[derive(Debug, Clone)]
pub struct PreparedStatement {
    /// The handle returned from the server
    prepared_statement_handle: Bytes,

    /// Schema for the result of the query
    dataset_schema: SchemaRef,

    /// Schema of parameters, if any
    parameter_schema: SchemaRef,
}

impl PreparedStatement {
    /// The handle returned from the server
    /// Schema for the result of the query
    /// Schema of parameters, if any
    fn new(
        prepared_statement_handle: Bytes,
        dataset_schema: SchemaRef,
        parameter_schema: SchemaRef,
    ) -> Self {
        Self {
            prepared_statement_handle,
            dataset_schema,
            parameter_schema,
        }
    }

    /// Return the schema of the query
    pub fn get_dataset_schema(&self) -> SchemaRef {
        Arc::clone(&self.dataset_schema)
    }

    /// Return the schema needed for the parameters
    pub fn get_parameter_schema(&self) -> SchemaRef {
        Arc::clone(&self.parameter_schema)
    }
}
