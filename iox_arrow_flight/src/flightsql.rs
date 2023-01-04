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

use arrow_flight::sql::{CommandStatementQuery, ProstMessageExt};
use arrow_flight::{FlightDescriptor, FlightInfo};
use prost::Message;
use tonic::metadata::MetadataMap;
use tonic::transport::Channel;

use crate::{
    error::{FlightError, Result},
    FlightClient, FlightRecordBatchStream,
};

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
    async fn get_flight_info_for_command<M: ProstMessageExt>(
        &mut self,
        cmd: M,
    ) -> Result<FlightInfo> {
        let descriptor = FlightDescriptor::new_cmd(cmd.as_any().encode_to_vec());
        self.inner.get_flight_info(descriptor).await
    }

    /// Execute a SQL query on the server using `CommandStatementQuery.
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
        let cmd = CommandStatementQuery { query };
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

        self.inner.do_get(ticket).await
    }
}
