//! Client for InfluxDB IOx Flight API

use ::generated_types::influxdata::iox::querier::v1::{
    read_info::QueryType, AppMetadata, ReadInfo,
};
use thiserror::Error;

use arrow::{
    ipc::{self},
    record_batch::RecordBatch,
};

use crate::connection::Connection;

/// Re-export generated_types
pub mod generated_types {
    pub use generated_types::influxdata::iox::{
        ingester::v1::{IngesterQueryRequest, IngesterQueryResponseMetadata, Predicate},
        querier::v1::*,
    };
}

pub mod low_level;
pub use low_level::{Client as LowLevelClient, PerformQuery as LowLevelPerformQuery};

use self::low_level::LowLevelMessage;

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

    /// The data contained invalid Flatbuffers.
    #[error("Invalid Flatbuffer: `{0}`")]
    InvalidFlatbuffer(String),

    /// The message header said it was a dictionary batch, but interpreting the
    /// message as a dictionary batch returned `None`. Indicates malformed
    /// Flight data from the server.
    #[error("Message with header of type dictionary batch could not return a dictionary batch")]
    CouldNotGetDictionaryBatch,

    /// An unknown server error occurred. Contains the `tonic::Status` returned
    /// from the server.
    #[error("{}", .0.message())]
    GrpcError(#[from] tonic::Status),

    /// Arrow Flight handshake failed.
    #[error("Handshake failed")]
    HandshakeFailed,

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
/// Note that [low level interface](low_level), used internally for querier
/// <--> ingester communication, offers more control over the messages
/// that are sent and the batches received.
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
///
/// let connection = Builder::default()
///     .build("http://127.0.0.1:8082")
///     .await
///     .expect("client should be valid");
///
/// let mut client = Client::new(connection);
///
/// let mut query_results = client
///     .sql("my_namespace".into(), "select * from cpu_load".into())
///     .await
///     .expect("query request should work");
///
/// let mut batches = vec![];
///
/// while let Some(data) = query_results.next().await.expect("valid batches") {
///     batches.push(data);
/// }
/// # }
/// ```
#[derive(Debug)]
pub struct Client {
    inner: LowLevelClient<ReadInfo>,
}

impl Client {
    /// Creates a new client with the provided connection
    pub fn new(connection: Connection) -> Self {
        Self {
            inner: LowLevelClient::new(connection, None),
        }
    }

    /// Query the given namespace with the given SQL query, and return a
    /// [`PerformQuery`] instance that streams Arrow [`RecordBatch`] results.
    pub async fn sql(
        &mut self,
        namespace_name: String,
        sql_query: String,
    ) -> Result<PerformQuery, Error> {
        let request = ReadInfo {
            namespace_name,
            sql_query,
            query_type: QueryType::Sql.into(),
        };

        self.perform_read(request).await
    }

    /// Query the given namespace with the given InfluxQL query, and return a
    /// [`PerformQuery`] instance that streams Arrow [`RecordBatch`] results.
    pub async fn influxql(
        &mut self,
        namespace_name: String,
        influxql_query: String,
    ) -> Result<PerformQuery, Error> {
        let request = ReadInfo {
            namespace_name,
            sql_query: influxql_query,
            query_type: QueryType::InfluxQl.into(),
        };
        PerformQuery::new(self, request).await
    }

    /// Send the query request described in `request` to the IOx
    /// server, returning a [`PerformQuery`] instance that streams
    /// Arrow [`RecordBatch`] results.
    pub async fn perform_read(&mut self, request: ReadInfo) -> Result<PerformQuery, Error> {
        PerformQuery::new(self, request).await
    }

    /// Perform a handshake with the server, returning Ok on success
    /// and Err if the server fails the handshake.
    ///
    /// It is best practice to ensure a successful handshake with IOx
    /// prior to issuing queries.
    pub async fn handshake(&mut self) -> Result<(), Error> {
        self.inner.handshake().await
    }
}

/// A struct that manages the stream of Arrow [`RecordBatch`]
/// resulting from executing an IOx Flight query.
///
/// Most users will not interact with this structure directly, but
/// rather should use [`Client::sql`] or [`Client::influxql`]
/// methods.
#[derive(Debug)]
pub struct PerformQuery {
    inner: LowLevelPerformQuery<AppMetadata>,
    got_schema: bool,
}

impl PerformQuery {
    pub(crate) async fn new(flight: &mut Client, request: ReadInfo) -> Result<Self, Error> {
        let inner = flight.inner.perform_query(request).await?;

        Ok(Self {
            inner,
            got_schema: false,
        })
    }

    /// Returns the next `RecordBatch` available for this query, or `None` if
    /// there are no further results available.
    pub async fn next(&mut self) -> Result<Option<RecordBatch>, Error> {
        loop {
            match self.inner.next().await? {
                None => return Ok(None),
                Some((LowLevelMessage::Schema(_), _)) => {
                    if self.got_schema {
                        return Err(Error::UnexpectedSchemaChange);
                    }
                    self.got_schema = true;
                }
                Some((LowLevelMessage::RecordBatch(batch), _)) => return Ok(Some(batch)),
                Some((LowLevelMessage::None, _)) => (),
            }
        }
    }

    /// Collect and return all `RecordBatch`es as a `Vec`
    pub async fn collect(&mut self) -> Result<Vec<RecordBatch>, Error> {
        let mut batches = Vec::new();
        while let Some(data) = self.next().await? {
            batches.push(data);
        }

        Ok(batches)
    }
}
