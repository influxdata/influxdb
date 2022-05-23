use std::{collections::HashMap, convert::TryFrom, marker::PhantomData, sync::Arc};

use ::generated_types::influxdata::iox::{
    ingester::v1::{IngesterQueryRequest, IngesterQueryResponseMetadata},
    querier::v1::{AppMetadata, ReadInfo},
};
use futures_util::stream;
use futures_util::stream::StreamExt;
use prost::Message;
use thiserror::Error;
use tonic::Streaming;

use arrow::{
    array::ArrayRef,
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

/// Re-export generated_types
pub mod generated_types {
    pub use generated_types::influxdata::iox::{
        ingester::v1::{IngesterQueryRequest, IngesterQueryResponseMetadata, Predicate},
        querier::v1::*,
    };
}

/// Error responses when querying an IOx database using the Arrow Flight gRPC
/// API.
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
    #[error(transparent)]
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
}

/// Metadata that can be send during flight requests.
pub trait ClientMetadata: Message {
    /// Response metadata.
    type Response: Default + Message;
}

impl ClientMetadata for ReadInfo {
    type Response = AppMetadata;
}

impl ClientMetadata for IngesterQueryRequest {
    type Response = IngesterQueryResponseMetadata;
}

/// An IOx Arrow Flight gRPC API client.
///
/// # Request and Response Metadata
/// The type parameter `T` -- which must implement [`ClientMetadata`] describes the request and response metadata that
/// is send and received during the flight request. The request is encoded as protobuf and send as the Flight "ticket",
/// the response is received via the so called "app metadata".
///
/// The default [`ReadInfo`] should work most general-purpose clients that talk the the IOx querier.
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
///     .perform_query(ReadInfo {
///         namespace_name: "my_database".to_string(),
///         sql_query: "select * from cpu_load".to_string(),
///     })
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
pub struct Client<T = ReadInfo>
where
    T: ClientMetadata,
{
    inner: FlightServiceClient<Connection>,
    _phantom: PhantomData<T>,
}

impl<T> Client<T>
where
    T: ClientMetadata,
{
    /// Creates a new client with the provided connection
    pub fn new(channel: Connection) -> Self {
        Self {
            inner: FlightServiceClient::new(channel),
            _phantom: PhantomData::default(),
        }
    }

    /// Query the given database with the given SQL query, and return a
    /// [`PerformQuery`] instance that streams Arrow `RecordBatch` results.
    pub async fn perform_query(&mut self, request: T) -> Result<PerformQuery<T::Response>, Error> {
        PerformQuery::<T::Response>::new(self, request).await
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

/// A struct that manages the stream of Arrow `RecordBatch` results from an
/// Arrow Flight query. Created by calling the `perform_query` method on a
/// Flight [`Client`].
#[derive(Debug)]
pub struct PerformQuery<T = AppMetadata>
where
    T: Default + Message,
{
    schema: Arc<Schema>,
    dictionaries_by_field: HashMap<i64, ArrayRef>,
    response: Streaming<FlightData>,
    app_metadata: T,
}

impl<T> PerformQuery<T>
where
    T: Default + Message,
{
    pub(crate) async fn new<R>(flight: &mut Client<R>, request: R) -> Result<Self, Error>
    where
        R: ClientMetadata<Response = T>,
    {
        let mut bytes = bytes::BytesMut::new();
        prost::Message::encode(&request, &mut bytes)?;
        let t = Ticket {
            ticket: bytes.to_vec(),
        };
        let mut response = flight.inner.do_get(t).await?.into_inner();

        let flight_data_schema = response.next().await.ok_or(Error::NoSchema)??;

        let app_metadata = &flight_data_schema.app_metadata[..];
        let app_metadata = prost::Message::decode(app_metadata)?;

        let schema = Arc::new(Schema::try_from(&flight_data_schema)?);

        let dictionaries_by_field = HashMap::new();

        Ok(Self {
            schema,
            dictionaries_by_field,
            response,
            app_metadata,
        })
    }

    /// Returns the next `RecordBatch` available for this query, or `None` if
    /// there are no further results available.
    pub async fn next(&mut self) -> Result<Option<RecordBatch>, Error> {
        let Self {
            schema,
            dictionaries_by_field,
            response,
            ..
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

    /// Collect and return all `RecordBatch`es into a `Vec`
    pub async fn collect(&mut self) -> Result<Vec<RecordBatch>, Error> {
        let mut batches = Vec::new();
        while let Some(data) = self.next().await? {
            batches.push(data);
        }

        Ok(batches)
    }

    /// App metadata that was part of the response.
    pub fn app_metadata(&self) -> &T {
        &self.app_metadata
    }

    /// Schema.
    pub fn schema(&self) -> Arc<Schema> {
        Arc::clone(&self.schema)
    }
}
