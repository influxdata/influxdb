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

use arrow::datatypes::{Schema, SchemaRef};
use arrow_flight::{
    decode::FlightRecordBatchStream,
    error::{FlightError, Result},
    sql::{
        ActionCreatePreparedStatementRequest, ActionCreatePreparedStatementResult, Any,
        CommandPreparedStatementQuery, CommandStatementQuery, ProstMessageExt,
    },
    Action, FlightClient, FlightDescriptor, FlightInfo, IpcMessage, Ticket,
};
use bytes::Bytes;
use futures_util::TryStreamExt;
use prost::Message;
use tonic::metadata::MetadataMap;
use tonic::transport::Channel;

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
        FlightSqlClient { inner }
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
    async fn get_flight_info_for_command(
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
    pub async fn query(&mut self, query: String) -> Result<FlightRecordBatchStream> {
        let msg = CommandStatementQuery { query };
        self.do_get_with_cmd(msg.as_any()).await
    }

    async fn do_get_with_cmd(
        &mut self,
        cmd: arrow_flight::sql::Any,
    ) -> Result<FlightRecordBatchStream> {
        let FlightInfo {
            schema: _,
            flight_descriptor: _,
            mut endpoint,
            total_records: _,
            total_bytes: _,
        } = self.get_flight_info_for_command(cmd).await?;

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

    /// Create a prepared statement for execution.
    ///
    /// Sends a [`ActionCreatePreparedStatementRequest`] message to
    /// the `DoAction` endpoint of the FlightSQL server, and returns
    /// the handle from the server.
    ///
    /// See [`Self::execute`] to run a previously prepared statement
    pub async fn prepare(&mut self, query: String) -> Result<PreparedStatement> {
        let cmd = ActionCreatePreparedStatementRequest { query };

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
            prepared_statement_handle,
            dataset_schema,
            parameter_schema,
        } = Any::unpack(&response)?.ok_or_else(|| {
            FlightError::ProtocolError(format!(
                "Expected ActionCreatePreparedStatementResult message but got {} instead",
                response.type_url
            ))
        })?;

        Ok(PreparedStatement::new(
            prepared_statement_handle,
            schema_bytes_to_schema(dataset_schema)?,
            schema_bytes_to_schema(parameter_schema)?,
        ))
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
    pub async fn execute(
        &mut self,
        statement: PreparedStatement,
    ) -> Result<FlightRecordBatchStream> {
        let PreparedStatement {
            prepared_statement_handle,
            dataset_schema: _,
            parameter_schema: _,
        } = statement;
        // TODO handle parameters (via DoPut)

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
