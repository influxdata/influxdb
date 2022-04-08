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
use client_util::connection::Connection;
use data_types2::{IngesterQueryRequest, SequenceNumber};
use futures::{stream, StreamExt};
use generated_types::influxdata::iox::ingester::v1 as proto;
use rand::Rng;
use std::{convert::TryFrom, sync::Arc};
use thiserror::Error;
use tonic::Streaming;

/// Error responses when querying an IOx ingester using the Arrow Flight gRPC API.
#[derive(Debug, Error)]
pub enum Error {
    /// An unknown server error occurred. Contains the `tonic::Status` returned
    /// from the server.
    #[error(transparent)]
    Grpc(#[from] tonic::Status),

    /// Arrow Flight handshake failed.
    #[error("Handshake failed")]
    HandshakeFailed,

    /// There were no FlightData messages returned when we expected to get one
    /// containing a Schema.
    #[error("no FlightData containing a Schema returned")]
    NoSchema,

    /// An error involving an Arrow operation occurred.
    #[error(transparent)]
    Arrow(#[from] arrow::error::ArrowError),

    /// The data contained invalid Flatbuffers.
    #[error("Invalid Flatbuffer: `{0}`")]
    InvalidFlatbuffer(String),

    /// The message header said it was a dictionary batch, but interpreting the
    /// message as a dictionary batch returned `None`. Indicates malformed
    /// Flight data from the server.
    #[error("Message with header of type dictionary batch could not return a dictionary batch")]
    CouldNotGetDictionaryBatch,

    /// Creating protobuf structs for the query failed. Indicates an invalid request
    /// is being made.
    #[error(transparent)]
    Protobuf(#[from] generated_types::google::FieldViolation),

    /// Serializing the protobuf structs into bytes failed.
    #[error(transparent)]
    Serialization(#[from] prost::EncodeError),

    /// Deserializing the protobuf structs from bytes failed.
    #[error(transparent)]
    Deserialization(#[from] prost::DecodeError),
}

/// Client for the querier to fetch not yet persisted data from the ingester
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

    /// Query an ingester with the given parameters and return a [`PerformQuery`] instance that
    /// streams Arrow `RecordBatch` results.
    pub async fn perform_query(
        &mut self,
        ingester_query_request: IngesterQueryRequest,
    ) -> Result<PerformQuery, Error> {
        PerformQuery::new(self, ingester_query_request).await
    }
}

/// A struct that manages the stream of Arrow `RecordBatch` results from an
/// Arrow Flight query. Created by calling the `perform_query` method on a
/// Flight [`Client`].
#[derive(Debug)]
pub struct PerformQuery {
    /// The schema sent back in the first FlightData message.
    pub schema: Arc<Schema>,

    /// The parquet_max_sequence_number sent back in the app_metadata of the first FlightData message.
    pub parquet_max_sequence_number: Option<SequenceNumber>,

    /// The tombstone_max_sequence_number sent back in the app_metadata of the first FlightData message.
    pub tombstone_max_sequence_number: Option<SequenceNumber>,

    /// The dictionaries as collected from the FlightData messages received
    pub dictionaries_by_field: Vec<Option<Arc<dyn Array>>>,
    response: Streaming<FlightData>,
}

impl PerformQuery {
    pub(crate) async fn new(
        flight: &mut Client,
        ingester_query_request: IngesterQueryRequest,
    ) -> Result<Self, Error> {
        let request = proto::IngesterQueryRequest::try_from(ingester_query_request)?;
        let mut bytes = bytes::BytesMut::new();
        prost::Message::encode(&request, &mut bytes)?;
        let t = Ticket {
            ticket: bytes.to_vec(),
        };
        let mut response = flight.inner.do_get(t).await?.into_inner();

        let flight_data_schema = response.next().await.ok_or(Error::NoSchema)??;

        let app_metadata = &flight_data_schema.app_metadata[..];
        let app_metadata: proto::IngesterQueryResponseMetadata =
            prost::Message::decode(app_metadata)?;

        let parquet_max_sequence_number = app_metadata
            .parquet_max_sequence_number
            .map(SequenceNumber::new);
        let tombstone_max_sequence_number = app_metadata
            .tombstone_max_sequence_number
            .map(SequenceNumber::new);

        let schema = Arc::new(Schema::try_from(&flight_data_schema)?);

        let dictionaries_by_field = vec![None; schema.fields().len()];

        Ok(Self {
            schema,
            parquet_max_sequence_number,
            tombstone_max_sequence_number,
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

    pub fn schema(&self) -> Arc<Schema> {
        Arc::clone(&self.schema)
    }
}
