use std::{convert::TryFrom, sync::Arc};

use futures_util::stream;
use futures_util::stream::StreamExt;
use serde::Serialize;
use thiserror::Error;
use tonic::Streaming;

use arrow::{
    array::Array,
    datatypes::Schema,
    ipc::{self, reader},
    record_batch::RecordBatch,
};
use arrow_flight::{
    flight_service_client::FlightServiceClient, utils::flight_data_to_arrow_batch, FlightData,
    HandshakeRequest, Ticket,
};

use crate::connection::Connection;
use rand::Rng;

/// Error responses when querying an IOx database using the Arrow Flight gRPC
/// API.
#[derive(Debug, Error)]
pub enum Error {
    /// An error occurred while serializing the query.
    #[error(transparent)]
    QuerySerializeError(#[from] serde_json::Error),

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
    #[error(transparent)]
    GrpcError(#[from] tonic::Status),

    /// Arrow Flight handshake failed.
    #[error("Handshake failed")]
    HandshakeFailed,
}

/// An IOx Arrow Flight gRPC API client.
///
/// ```rust,no_run
/// #[tokio::main]
/// # async fn main() {
/// use influxdb_iox_client::{connection::Builder, flight::Client};
///
/// let connection = Builder::default()
///     .build("http://127.0.0.1:8082")
///     .await
///     .expect("client should be valid");
///
/// let mut client = Client::new(connection);
///
/// let mut query_results = client
///     .perform_query("my_database", "select * from cpu_load")
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
    inner: FlightServiceClient<Connection>,
}

impl Client {
    /// Creates a new client with the provided connection
    pub fn new(channel: Connection) -> Self {
        Self {
            inner: FlightServiceClient::new(channel),
        }
    }

    /// Query the given database with the given SQL query, and return a
    /// [`PerformQuery`] instance that streams Arrow `RecordBatch` results.
    pub async fn perform_query(
        &mut self,
        database_name: impl Into<String> + Send,
        sql_query: impl Into<String> + Send,
    ) -> Result<PerformQuery, Error> {
        PerformQuery::new(self, database_name.into(), sql_query.into()).await
    }

    /// Perform a handshake with the server, as defined by the Arrow Flight API.
    pub async fn handshake(&mut self) -> Result<(), Error> {
        let request = HandshakeRequest {
            protocol_version: 0,
            payload: rand::thread_rng().gen::<[u8; 16]>().to_vec(),
        };
        let mut response = self
            .inner
            .handshake(stream::iter(vec![request.clone()]))
            .await?
            .into_inner();
        if request.payload.eq(&response
            .next()
            .await
            .ok_or(Error::HandshakeFailed)??
            .payload)
        {
            Result::Ok(())
        } else {
            Result::Err(Error::HandshakeFailed)
        }
    }
}

// TODO: this should be shared
#[derive(Serialize, Debug)]
struct ReadInfo {
    database_name: String,
    sql_query: String,
}

/// A struct that manages the stream of Arrow `RecordBatch` results from an
/// Arrow Flight query. Created by calling the `perform_query` method on a
/// Flight [`Client`].
#[derive(Debug)]
pub struct PerformQuery {
    schema: Arc<Schema>,
    dictionaries_by_field: Vec<Option<Arc<dyn Array>>>,
    response: Streaming<FlightData>,
}

impl PerformQuery {
    pub(crate) async fn new(
        flight: &mut Client,
        database_name: String,
        sql_query: String,
    ) -> Result<Self, Error> {
        let query = ReadInfo {
            database_name,
            sql_query,
        };

        let t = Ticket {
            ticket: serde_json::to_string(&query)?.into(),
        };
        let mut response = flight.inner.do_get(t).await?.into_inner();

        let flight_data_schema = response.next().await.ok_or(Error::NoSchema)??;
        let schema = Arc::new(Schema::try_from(&flight_data_schema)?);

        let dictionaries_by_field = vec![None; schema.fields().len()];

        Ok(Self {
            schema,
            dictionaries_by_field,
            response,
        })
    }

    /// Returns the next `RecordBatch` available for this query, or `None` if
    /// there are no further results available.
    pub async fn next(&mut self) -> Result<Option<RecordBatch>, Error> {
        let Self {
            schema,
            dictionaries_by_field,
            response,
        } = self;

        let mut data = match response.next().await {
            Some(d) => d?,
            None => return Ok(None),
        };

        let mut message = ipc::root_as_message(&data.data_header[..])
            .map_err(|e| Error::InvalidFlatbuffer(e.to_string()))?;

        while message.header_type() == ipc::MessageHeader::DictionaryBatch {
            reader::read_dictionary(
                &data.data_body,
                message
                    .header_as_dictionary_batch()
                    .ok_or(Error::CouldNotGetDictionaryBatch)?,
                schema,
                dictionaries_by_field,
            )?;

            data = match response.next().await {
                Some(d) => d?,
                None => return Ok(None),
            };

            message = ipc::root_as_message(&data.data_header[..])
                .map_err(|e| Error::InvalidFlatbuffer(e.to_string()))?;
        }

        Ok(Some(flight_data_to_arrow_batch(
            &data,
            Arc::clone(schema),
            dictionaries_by_field,
        )?))
    }

    /// Return all record batches of it
    pub async fn to_batches(&mut self) -> Result<Vec<RecordBatch>, Error> {
        let mut batches = Vec::new();
        while let Some(data) = self.next().await? {
            batches.push(data);
        }

        Ok(batches)
    }
}
