//! Low-level flight client.
//!
//! This client allows more inspection of the flight messages which can be helpful to implement
//! more advanced protocols.
//!
//! # Protocol Usage
//!
//! The client handles flight messages as followes:
//!
//! - **None:** App metadata is extracted. Otherwise this message has no effect. This is useful to
//!   transmit metadata without any actual payload.
//! - **Schema:** The schema is (re-)set. Dictionaries are cleared. App metadata is extraced and
//!   both the schema and the metadata are presented to the user.
//! - **Dictionary Batch:** A new dictionary for a given column is registered. An existing
//!   dictionary for the same column will be overwritten. No app metadata is extracted. This
//!   message is NOT visible to the user.
//! - **Record Batch:** Record batch is created based on the current schema and dictionaries. This
//!   fails if no schema was transmitted yet. App metadata is extracted is is presented -- together
//!   with the record batch -- to the user.
//!
//! All other message types (at the time of writing: tensor and sparse tensor) lead to an error.

use super::Error;
use ::generated_types::influxdata::iox::{
    ingester::v1::{IngesterQueryRequest, IngesterQueryResponseMetadata},
    querier::v1::{AppMetadata, ReadInfo},
};
use arrow::{
    array::ArrayRef,
    buffer::Buffer,
    datatypes::Schema,
    ipc::{self, reader},
    record_batch::RecordBatch,
};
use arrow_flight::{
    flight_service_client::FlightServiceClient, utils::flight_data_to_arrow_batch, FlightData,
    HandshakeRequest, Ticket,
};
use client_util::connection::{Connection, GrpcConnection};
use futures_util::stream;
use futures_util::stream::StreamExt;
use prost::Message;
use rand::Rng;
use std::{collections::HashMap, convert::TryFrom, marker::PhantomData, str::FromStr, sync::Arc};
use tonic::{
    codegen::http::header::{HeaderName, HeaderValue},
    Streaming,
};
use trace::ctx::SpanContext;
use trace_http::ctx::format_jaeger_trace_context;

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

/// Low-level flight client.
///
/// # Request and Response Metadata
///
/// The type parameter `T` -- which must implement [`ClientMetadata`] -- describes the request and
/// response metadata that is sent and received during the flight request. The request is encoded
/// as protobuf and send as the Flight "ticket". The response is received via the so-called "app
/// metadata".
#[derive(Debug)]
pub struct Client<T>
where
    T: ClientMetadata,
{
    inner: FlightServiceClient<GrpcConnection>,
    _phantom: PhantomData<T>,
}

impl<T> Client<T>
where
    T: ClientMetadata,
{
    /// Creates a new client with the provided connection
    #[allow(clippy::mutable_key_type)] // https://github.com/rust-lang/rust-clippy/issues/5812
    pub fn new(connection: Connection, span_context: Option<SpanContext>) -> Self {
        let grpc_conn = connection.into_grpc_connection();

        let grpc_conn = if let Some(ctx) = span_context {
            let (service, headers) = grpc_conn.into_parts();

            let mut headers: HashMap<_, _> = headers.iter().cloned().collect();
            let key =
                HeaderName::from_str(trace_exporters::DEFAULT_JAEGER_TRACE_CONTEXT_HEADER_NAME)
                    .unwrap();
            let value = HeaderValue::from_str(&format_jaeger_trace_context(&ctx)).unwrap();
            headers.insert(key, value);

            GrpcConnection::new(service, headers.into_iter().collect())
        } else {
            grpc_conn
        };

        Self {
            inner: FlightServiceClient::new(grpc_conn),
            _phantom: PhantomData::default(),
        }
    }

    /// Query the given database with the given SQL query, and return a [`PerformQuery`] instance
    /// that streams low-level message results.
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

#[derive(Debug)]
struct PerformQueryState {
    schema: Arc<Schema>,
    dictionaries_by_field: HashMap<i64, ArrayRef>,
}

/// Low-level message returned by the flight server.
#[derive(Debug)]
pub enum LowLevelMessage {
    /// None.
    None,

    /// Schema.
    Schema(Arc<Schema>),

    /// Record batch.
    RecordBatch(RecordBatch),
}

impl LowLevelMessage {
    /// Unwrap none.
    pub fn unwrap_none(self) {
        match self {
            LowLevelMessage::None => (),
            LowLevelMessage::Schema(_) => panic!("Contains schema"),
            LowLevelMessage::RecordBatch(_) => panic!("Contains record batch"),
        }
    }

    /// Unwrap schema.
    pub fn unwrap_schema(self) -> Arc<Schema> {
        match self {
            LowLevelMessage::None => panic!("Contains none"),
            LowLevelMessage::Schema(schema) => schema,
            LowLevelMessage::RecordBatch(_) => panic!("Contains record batch"),
        }
    }

    /// Unwrap data.
    pub fn unwrap_record_batch(self) -> RecordBatch {
        match self {
            LowLevelMessage::None => panic!("Contains none"),
            LowLevelMessage::Schema(_) => panic!("Contains schema"),
            LowLevelMessage::RecordBatch(batch) => batch,
        }
    }
}

/// A struct that manages the stream of Arrow `RecordBatch` results from an Arrow Flight query.
/// Created by calling the `perform_query` method on a Flight [`Client`].
#[derive(Debug)]
pub struct PerformQuery<T>
where
    T: Default + Message,
{
    response: Streaming<FlightData>,
    state: Option<PerformQueryState>,
    _phantom: PhantomData<T>,
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
        let response = flight.inner.do_get(t).await?.into_inner();

        Ok(Self {
            state: None,
            response,
            _phantom: Default::default(),
        })
    }

    /// Returns next low-level message, or `None` if there are no further results available.
    pub async fn next(&mut self) -> Result<Option<(LowLevelMessage, T)>, Error> {
        let Self {
            state, response, ..
        } = self;

        loop {
            let data = match response.next().await {
                Some(d) => d?,
                None => return Ok(None),
            };

            let message = ipc::root_as_message(&data.data_header[..])
                .map_err(|e| Error::InvalidFlatbuffer(e.to_string()))?;

            match message.header_type() {
                ipc::MessageHeader::NONE => {
                    let app_metadata = &data.app_metadata[..];
                    let app_metadata = prost::Message::decode(app_metadata)?;

                    return Ok(Some((LowLevelMessage::None, app_metadata)));
                }
                ipc::MessageHeader::Schema => {
                    let app_metadata = &data.app_metadata[..];
                    let app_metadata = prost::Message::decode(app_metadata)?;

                    let schema = Arc::new(Schema::try_from(&data)?);

                    let dictionaries_by_field = HashMap::new();

                    *state = Some(PerformQueryState {
                        schema: Arc::clone(&schema),
                        dictionaries_by_field,
                    });
                    return Ok(Some((LowLevelMessage::Schema(schema), app_metadata)));
                }
                ipc::MessageHeader::DictionaryBatch => {
                    let state = if let Some(state) = state.as_mut() {
                        state
                    } else {
                        return Err(Error::NoSchema);
                    };

                    let buffer: Buffer = data.data_body.into();
                    reader::read_dictionary(
                        &buffer,
                        message
                            .header_as_dictionary_batch()
                            .ok_or(Error::CouldNotGetDictionaryBatch)?,
                        &state.schema,
                        &mut state.dictionaries_by_field,
                        &message.version(),
                    )?;
                }
                ipc::MessageHeader::RecordBatch => {
                    let state = if let Some(state) = state.as_ref() {
                        state
                    } else {
                        return Err(Error::NoSchema);
                    };

                    let app_metadata = &data.app_metadata[..];
                    let app_metadata = prost::Message::decode(app_metadata)?;

                    let batch = flight_data_to_arrow_batch(
                        &data,
                        Arc::clone(&state.schema),
                        &state.dictionaries_by_field,
                    )?;

                    return Ok(Some((LowLevelMessage::RecordBatch(batch), app_metadata)));
                }
                other => {
                    return Err(Error::UnknownMessageType(other));
                }
            }
        }
    }
}
