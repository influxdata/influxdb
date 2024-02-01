//! Client for InfluxDB IOx Flight API

use std::{pin::Pin, task::Poll};

use ::generated_types::influxdata::iox::querier::v1::{read_info::QueryType, ReadInfo};
use futures_util::{Stream, StreamExt};
use prost::Message;
use thiserror::Error;
use tonic::metadata::{MetadataKey, MetadataMap, MetadataValue};

use arrow::{
    ipc::{self},
    record_batch::RecordBatch,
};

use rand::Rng;

use arrow_flight::{decode::FlightRecordBatchStream, error::FlightError, FlightClient, Ticket};

use crate::connection::Connection;

use self::query::{NoQuery, QueryBuilder};

pub mod query;

/// Re-export generated_types
pub mod generated_types {
    pub use generated_types::influxdata::iox::querier::v1::*;
}

/// Error responses when querying an IOx namespace using the IOx Flight API.
#[derive(Debug, Error)]
pub enum Error {
    /// There were no FlightData messages returned when we expected to get one
    /// containing a Schema.
    #[error("no FlightData containing a Schema returned")]
    NoSchema,

    /// An error involving an Arrow operation occurred.
    #[error(transparent)]
    ArrowError(#[from] arrow::error::ArrowError),

    /// An error involving an Arrow Flight operation occurred.
    #[error(transparent)]
    ArrowFlightError(#[from] FlightError),

    /// The data contained invalid Flatbuffers.
    #[error("Invalid Flatbuffer: `{0}`")]
    InvalidFlatbuffer(String),

    /// The message header said it was a dictionary batch, but interpreting the
    /// message as a dictionary batch returned `None`. Indicates malformed
    /// Flight data from the server.
    #[error("Message with header of type dictionary batch could not return a dictionary batch")]
    CouldNotGetDictionaryBatch,

    /// Arrow Flight handshake failed.
    #[error("Handshake failed: {0}")]
    HandshakeFailed(String),

    /// Serializing the protobuf structs into bytes failed.
    #[error(transparent)]
    Serialization(#[from] prost::EncodeError),

    /// Deserializing the protobuf structs from bytes failed.
    #[error(transparent)]
    Deserialization(#[from] prost::DecodeError),

    /// Unknown IPC message type.
    #[error("Unknown IPC message type: {0:?}")]
    UnknownMessageType(ipc::MessageHeader),

    /// Unexpected schema change.
    #[error("Unexpected schema change")]
    UnexpectedSchemaChange,
}

impl Error {
    /// Extracts the underlying tonic status, if any
    pub fn tonic_status(&self) -> Option<&tonic::Status> {
        if let Self::ArrowFlightError(FlightError::Tonic(status)) = self {
            Some(status)
        } else {
            None
        }
    }
}

impl From<tonic::Status> for Error {
    fn from(status: tonic::Status) -> Self {
        Self::ArrowFlightError(status.into())
    }
}

/// InfluxDB IOx Flight API client.
///
/// This client can send SQL or InfluxQL queries to an IOx server
/// via IOx's native [Apache Arrow Flight](https://arrow.apache.org/blog/2019/10/13/introducing-arrow-flight/)
/// API (based on gRPC) and returns query results as streams of [`RecordBatch`].
///
/// # Protocol
///
/// For SQL queries, this client yields a stream of [`RecordBatch`]es
/// with the same schema.
///
/// # Example
///
/// ```rust,no_run
/// #[tokio::main]
/// # async fn main() {
/// use influxdb_iox_client::{
///     connection::Builder,
///     flight::{
///         Client,
///         generated_types::ReadInfo,
///         generated_types::read_info,
///     },
/// };
/// use futures_util::TryStreamExt;
///
/// let connection = Builder::default()
///     .build("http://127.0.0.1:8082")
///     .await
///     .expect("client should be valid");
///
/// let mut client = Client::new(connection);
///
/// // results is a stream of RecordBatches
/// let query_results = client
///     .sql("my_namespace", "select * from cpu_load")
///     .await
///     .expect("query request should work");
///
/// // Can use standard TryStreamExt combinators like try_collect
/// let batches: Vec<_> = query_results
///     .try_collect()
///     .await
///     .expect("valid bathes");
/// # }
/// ```
///
/// # Parameterized Queries
///
/// Use the [`Client::query`] method to create a [`QueryBuilder`] which can be used
/// to supply parameter values to a query containing `$placeholder` variables
///
/// For example:
///
/// ```rust, no_run
/// # let mut client: influxdb_iox_client::flight::Client = todo!();
/// # async {
///
/// // Use QueryBuilder to create a parameterized query
/// let query_results = client
///     .query("my_namespace")
///     .sql("select * from cpu_load where host = $host and value = $value")
///     .with_param("$host", "my.hostname")
///     .with_param("$value", 0.523)
///     .run()
///     .await?;
///
/// # Ok::<(), influxdb_iox_client::flight::Error>(())
/// # };
///```
///
/// ## Helper macro for working with query parameters
///
/// To make building queries easier in some scenarios, you can use the [`iox_query_params::params`] macro to
/// build a map of name-value pairs.
///
/// Use the `params!` macro with [`query::QueryBuilder::with_params`] to
/// supply parameters to a [`query::QueryBuilder`]
///
/// ```rust, no_run
///     # use influxdb_iox_client::flight::query::{QueryBuilder, Query};
///     # use iox_query_params::params;
///     # let mut query: QueryBuilder<Query> = todo!();
///     query.with_params(
///         params! {
///            "param1" => "a string",
///            "param2" => 1234,
///            "param3" => 1.234,
///            "param4" => true,
///            "param5" => Some(false),
///            "param6" => None::<Option<()>>
///         }
///     )
///     # ;
/// ```
#[derive(Debug)]
pub struct Client {
    inner: FlightClient,
}

impl Client {
    /// Creates a new client with the provided [`Connection`]. Panics
    /// if the metadata in connection is invalid for the underlying
    /// tonic library.
    pub fn new(connection: Connection) -> Self {
        // Extract headers to include with each request
        let (channel, headers) = connection.into_grpc_connection().into_parts();

        let mut inner = FlightClient::new(channel);

        // Copy any headers from IOx Connection
        for (name, value) in headers.iter() {
            let name = MetadataKey::<_>::from_bytes(name.as_str().as_bytes())
                .expect("Invalid metadata name");

            let value: MetadataValue<_> =
                value.as_bytes().try_into().expect("Invalid metadata value");
            inner.metadata_mut().insert(name, value);
        }

        Self { inner }
    }

    /// Return the inner arrow flight client
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
    pub fn add_header(&mut self, key: &str, value: &str) -> Result<(), Error> {
        Ok(self.inner.add_header(key, value)?)
    }

    /// Create a new [`QueryBuilder`] to construct a query, optionally with parameters, on the
    /// given namespace
    pub fn query(&mut self, database: impl Into<String> + Send) -> QueryBuilder<'_, NoQuery> {
        QueryBuilder::new(self, database)
    }

    /// Query the given database with the given SQL query, returning
    /// a struct that can stream Arrow [`RecordBatch`] results.
    pub async fn sql(
        &mut self,
        database: impl Into<String> + Send,
        sql_query: impl Into<String> + Send,
    ) -> Result<IOxRecordBatchStream, Error> {
        let request = ReadInfo {
            database: database.into(),
            sql_query: sql_query.into(),
            query_type: QueryType::Sql.into(),
            flightsql_command: vec![],
            params: vec![],
            is_debug: false,
        };

        self.do_get_with_read_info(request).await
    }

    /// Query the given database with the given InfluxQL query, returning
    /// a struct that can stream Arrow [`RecordBatch`] results.
    pub async fn influxql(
        &mut self,
        database: impl Into<String> + Send,
        influxql_query: impl Into<String> + Send,
    ) -> Result<IOxRecordBatchStream, Error> {
        let request = ReadInfo {
            database: database.into(),
            sql_query: influxql_query.into(),
            query_type: QueryType::InfluxQl.into(),
            flightsql_command: vec![],
            params: vec![],
            is_debug: false,
        };

        self.do_get_with_read_info(request).await
    }

    /// Perform a lower level client read with the `ReadInfo`
    async fn do_get_with_read_info(
        &mut self,
        read_info: ReadInfo,
    ) -> Result<IOxRecordBatchStream, Error> {
        // encode readinfo as bytes and send it
        let ticket = Ticket {
            ticket: read_info.encode_to_vec().into(),
        };
        self.inner
            .do_get(ticket)
            .await
            .map(IOxRecordBatchStream::new)
            .map_err(Error::ArrowFlightError)
    }

    /// Perform a handshake with the server, returning Ok on success
    /// and Err if the server fails the handshake.
    ///
    /// It is best practice to ensure a successful handshake with IOx
    /// prior to issuing queries.

    /// Perform a handshake with the server, as defined by the Arrow Flight API.
    pub async fn handshake(&mut self) -> Result<(), Error> {
        // handshake is an echo server. Send some random bytes and
        // expect the same back.
        let payload = rand::thread_rng().gen::<[u8; 16]>().to_vec();

        let response = self
            .inner
            .handshake(payload.clone())
            .await
            .map_err(|e| e.to_string())
            .map_err(Error::HandshakeFailed)?;

        if payload.eq(&response) {
            Ok(())
        } else {
            Err(Error::HandshakeFailed("reponse mismatch".into()))
        }
    }
}

#[derive(Debug)]
/// Translates errors from FlightErrors to IOx client errors,
/// providing access to the underyling [`FlightRecordBatchStream`]
pub struct IOxRecordBatchStream {
    inner: FlightRecordBatchStream,
}

impl IOxRecordBatchStream {
    /// create a new IOxRecordBatchStream
    pub fn new(inner: FlightRecordBatchStream) -> Self {
        Self { inner }
    }

    /// Return a reference to the inner stream
    pub fn inner(&self) -> &FlightRecordBatchStream {
        &self.inner
    }

    /// Return a mutable reference to the inner stream
    pub fn inner_mut(&mut self) -> &mut FlightRecordBatchStream {
        &mut self.inner
    }

    /// Consume self and return the wrapped [`FlightRecordBatchStream`]
    pub fn into_inner(self) -> FlightRecordBatchStream {
        self.inner
    }
}

impl Stream for IOxRecordBatchStream {
    type Item = Result<RecordBatch, Error>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Result<RecordBatch, Error>>> {
        self.inner
            .poll_next_unpin(cx)
            .map_err(Error::ArrowFlightError)
    }
}
