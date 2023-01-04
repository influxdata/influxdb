/// Prototype "Flight Client" that handles underlying details of the flight protocol at a higher level

/// Based on the "low level client" from IOx client:
use arrow::{array::ArrayRef, datatypes::Schema, ipc, record_batch::RecordBatch};
use arrow_flight::{
    flight_service_client::FlightServiceClient, utils::flight_data_to_arrow_batch, FlightData,
    FlightDescriptor, FlightInfo, HandshakeRequest, Ticket,
};
use futures::ready;
use futures_util::stream;
use futures_util::stream::StreamExt;
use std::{collections::HashMap, convert::TryFrom, pin::Pin, sync::Arc, task::Poll};
use tonic::{
    metadata::MetadataMap,
    transport::Channel,
    //codegen::http::header::{HeaderName, HeaderValue},
    Streaming,
};

use crate::error::{FlightError, Result};

/// [Apache Arrow Flight](https://arrow.apache.org/docs/format/Flight.html) client.
///
/// [`FlightClient`] is intended as a convenience for interactions
/// with Arrow Flight servers. If you need more direct control, such
/// as access to the response headers, you can use the
/// [`FlightServiceClient`] directly via methods such as
/// [`Self::inner`] or [`Self::into_inner`].
///
// TODO:
// docstring example
// - [ ] make this properly templated so it can take any service (not just a tonic Channel directly)
#[derive(Debug)]
pub struct FlightClient {
    /// Optional grpc header metadata to include with each request
    metadata: MetadataMap,

    /// The inner client
    inner: FlightServiceClient<Channel>,
}

impl FlightClient {
    /// Creates a client client with the provided [`Channel`](tonic::transport::Channel);
    pub fn new(channel: Channel) -> Self {
        Self::new_from_inner(FlightServiceClient::new(channel))
    }

    /// Creates a new higher level client with the provided lower level client
    pub fn new_from_inner(inner: FlightServiceClient<Channel>) -> Self {
        Self {
            metadata: MetadataMap::new(),
            inner,
        }
    }

    /// Return a reference to gRPC metadata included with each request
    pub fn metadata(&self) -> &MetadataMap {
        &self.metadata
    }

    /// Return a reference to gRPC metadata included with each request
    ///
    /// These headers can be used, for example, to include
    /// authorization or other application specific headers.
    pub fn metadata_mut(&mut self) -> &mut MetadataMap {
        &mut self.metadata
    }

    /// Add the specified header with value to all subsequent
    /// requests. See [`Self::metadata_mut`] for fine grained control.
    pub fn add_header(&mut self, key: &str, value: &str) -> Result<()> {
        let key = tonic::metadata::MetadataKey::<_>::from_bytes(key.as_bytes())
            .map_err(|e| FlightError::ExternalError(Box::new(e)))?;

        let value = value
            .parse()
            .map_err(|e| FlightError::ExternalError(Box::new(e)))?;

        // ignore previous value
        self.metadata.insert(key, value);

        Ok(())
    }

    /// Return a reference to the underlying tonic
    /// [`FlightServiceClient`]
    pub fn inner(&self) -> &FlightServiceClient<Channel> {
        &self.inner
    }

    /// Return a mutable reference to the underlying tonic
    /// [`FlightServiceClient`]
    pub fn inner_mut(&mut self) -> &mut FlightServiceClient<Channel> {
        &mut self.inner
    }

    /// Consume this client and return the underlying tonic
    /// [`FlightServiceClient`]
    pub fn into_inner(self) -> FlightServiceClient<Channel> {
        self.inner
    }

    /// Perform an Arrow Flight handshake with the server, sending
    /// `payload` as the [`HandshakeRequest`] payload and returning
    /// the [`HandshakeResponse`](arrow_flight::HandshakeResponse)
    /// bytes returned from the server
    pub async fn handshake(&mut self, payload: Vec<u8>) -> Result<Vec<u8>> {
        let request = HandshakeRequest {
            protocol_version: 0,
            payload,
        };

        let mut response_stream = self
            .inner
            .handshake(stream::iter(vec![request]))
            .await
            .map_err(FlightError::Tonic)?
            .into_inner();

        if let Some(response) = response_stream.next().await {
            let response = response.map_err(FlightError::Tonic)?;

            // check if there is another response
            if response_stream.next().await.is_some() {
                return Err(FlightError::protocol(
                    "Got unexpected second response from handshake",
                ));
            }

            Ok(response.payload)
        } else {
            Err(FlightError::protocol("No response from handshake"))
        }
    }

    /// Make a `DoGet` call to the server with the provided ticket,
    /// returning a [`FlightRecordBatchStream`] for reading
    /// [`RecordBatch`]es.
    pub async fn do_get(&mut self, ticket: Vec<u8>) -> Result<FlightRecordBatchStream> {
        let t = Ticket { ticket };
        let request = self.make_request(t);

        let response = self
            .inner
            .do_get(request)
            .await
            .map_err(FlightError::Tonic)?
            .into_inner();

        let flight_data_stream = FlightDataStream::new(response);
        Ok(FlightRecordBatchStream::new(flight_data_stream))
    }

    /// Make a `GetFlightInfo` call to the server with the provided
    /// [`FlightDescriptor`] and return the [`FlightInfo`] from the
    /// server
    pub async fn get_flight_info(&mut self, descriptor: FlightDescriptor) -> Result<FlightInfo> {
        let request = self.make_request(descriptor);

        let response = self
            .inner
            .get_flight_info(request)
            .await
            .map_err(FlightError::Tonic)?
            .into_inner();
        Ok(response)
    }

    /// return a Request, adding any configured metadata
    fn make_request<T>(&self, t: T) -> tonic::Request<T> {
        // Pass along metadata
        let mut request = tonic::Request::new(t);
        *request.metadata_mut() = self.metadata.clone();
        request
    }
}

/// A stream of [`RecordBatch`]es from from an Arrow Flight server.
///
/// To access the lower level Flight messages directly, consider
/// calling [`Self::into_inner`] and using the [`FlightDataStream`]
/// directly.
#[derive(Debug)]
pub struct FlightRecordBatchStream {
    inner: FlightDataStream,
    got_schema: bool,
}

impl FlightRecordBatchStream {
    pub fn new(inner: FlightDataStream) -> Self {
        Self {
            inner,
            got_schema: false,
        }
    }

    /// Has a message defining the schema been received yet?
    pub fn got_schema(&self) -> bool {
        self.got_schema
    }

    /// Consume self and return the wrapped [`FlightDataStream`]
    pub fn into_inner(self) -> FlightDataStream {
        self.inner
    }
}
impl futures::Stream for FlightRecordBatchStream {
    type Item = Result<RecordBatch>;

    /// Returns the next [`RecordBatch`] available in this stream, or `None` if
    /// there are no further results available.
    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Result<RecordBatch>>> {
        loop {
            let res = ready!(self.inner.poll_next_unpin(cx));
            match res {
                // Inner exhausted
                None => {
                    return Poll::Ready(None);
                }
                Some(Err(e)) => {
                    return Poll::Ready(Some(Err(e)));
                }
                // translate data
                Some(Ok(data)) => match data.payload {
                    DecodedPayload::Schema(_) if self.got_schema => {
                        return Poll::Ready(Some(Err(FlightError::protocol(
                            "Unexpectedly saw multiple Schema messages in FlightData stream",
                        ))));
                    }
                    DecodedPayload::Schema(_) => {
                        self.got_schema = true;
                        // Need next message, poll inner again
                    }
                    DecodedPayload::RecordBatch(batch) => {
                        return Poll::Ready(Some(Ok(batch)));
                    }
                    DecodedPayload::None => {
                        // Need next message
                    }
                },
            }
        }
    }
}

/// Wrapper around a stream of [`FlightData`] that handles the details
/// of decoding low level Flight messages into [`Schema`] and
/// [`RecordBatch`]es, including details such as dictionaries.
///
/// # Protocol Details
///
/// The client handles flight messages as followes:
///
/// - **None:** This message has no effect. This is useful to
///   transmit metadata without any actual payload.
///
/// - **Schema:** The schema is (re-)set. Dictionaries are cleared and
///   the decoded schema is returned.
///
/// - **Dictionary Batch:** A new dictionary for a given column is registered. An existing
///   dictionary for the same column will be overwritten. This
///   message is NOT visible.
///
/// - **Record Batch:** Record batch is created based on the current
///   schema and dictionaries. This fails if no schema was transmitted
///   yet.
///
/// All other message types (at the time of writing: e.g. tensor and
/// sparse tensor) lead to an error.
///
/// Example usecases
///
/// 1. Using this low level stream it is possible to receive a steam
/// of RecordBatches in FlightData that have different schemas by
/// handling multiple schema messages separately.
#[derive(Debug)]
pub struct FlightDataStream {
    /// Underlying data stream
    response: Streaming<FlightData>,
    /// Decoding state
    state: Option<FlightStreamState>,
    /// seen the end of the inner stream?
    done: bool,
}

impl FlightDataStream {
    /// Create a new wrapper around the stream of FlightData
    pub fn new(response: Streaming<FlightData>) -> Self {
        Self {
            state: None,
            response,
            done: false,
        }
    }

    /// Extracts flight data from the next message, updating decoding
    /// state as necessary.
    fn extract_message(&mut self, data: FlightData) -> Result<Option<DecodedFlightData>> {
        let message = ipc::root_as_message(&data.data_header[..])
            .map_err(|e| FlightError::DecodeError(format!("Error decoding root message: {e}")))?;

        match message.header_type() {
            ipc::MessageHeader::NONE => Ok(Some(DecodedFlightData::new_none(data))),
            ipc::MessageHeader::Schema => {
                let schema = Schema::try_from(&data)
                    .map_err(|e| FlightError::DecodeError(format!("Error decoding schema: {e}")))?;

                let schema = Arc::new(schema);
                let dictionaries_by_field = HashMap::new();

                self.state = Some(FlightStreamState {
                    schema: Arc::clone(&schema),
                    dictionaries_by_field,
                });
                Ok(Some(DecodedFlightData::new_schema(data, schema)))
            }
            ipc::MessageHeader::DictionaryBatch => {
                let state = if let Some(state) = self.state.as_mut() {
                    state
                } else {
                    return Err(FlightError::protocol(
                        "Received DictionaryBatch prior to Schema",
                    ));
                };

                let buffer: arrow::buffer::Buffer = data.data_body.into();
                let dictionary_batch = message.header_as_dictionary_batch().ok_or_else(|| {
                    FlightError::protocol(
                        "Could not get dictionary batch from DictionaryBatch message",
                    )
                })?;

                ipc::reader::read_dictionary(
                    &buffer,
                    dictionary_batch,
                    &state.schema,
                    &mut state.dictionaries_by_field,
                    &message.version(),
                )
                .map_err(|e| {
                    FlightError::DecodeError(format!("Error decoding ipc dictionary: {e}"))
                })?;

                // Updated internal state, but no decoded message
                Ok(None)
            }
            ipc::MessageHeader::RecordBatch => {
                let state = if let Some(state) = self.state.as_ref() {
                    state
                } else {
                    return Err(FlightError::protocol(
                        "Received RecordBatch prior to Schema",
                    ));
                };

                let batch = flight_data_to_arrow_batch(
                    &data,
                    Arc::clone(&state.schema),
                    &state.dictionaries_by_field,
                )
                .map_err(|e| {
                    FlightError::DecodeError(format!("Error decoding ipc RecordBatch: {e}"))
                })?;

                Ok(Some(DecodedFlightData::new_record_batch(data, batch)))
            }
            other => {
                let name = other.variant_name().unwrap_or("UNKNOWN");
                Err(FlightError::protocol(format!("Unexpected message: {name}")))
            }
        }
    }
}

impl futures::Stream for FlightDataStream {
    type Item = Result<DecodedFlightData>;
    /// Returns the result of decoding the next [`FlightData`] message
    /// from the server, or `None` if there are no further results
    /// available.
    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        if self.done {
            return Poll::Ready(None);
        }
        loop {
            let res = ready!(self.response.poll_next_unpin(cx));

            return Poll::Ready(match res {
                None => {
                    self.done = true;
                    None // inner is exhausted
                }
                Some(data) => Some(match data {
                    Err(e) => Err(FlightError::Tonic(e)),
                    Ok(data) => match self.extract_message(data) {
                        Ok(Some(extracted)) => Ok(extracted),
                        Ok(None) => continue, // Need next input message
                        Err(e) => Err(e),
                    },
                }),
            });
        }
    }
}

/// tracks the state needed to reconstruct [`RecordBatch`]es from a
/// streaming flight response.
#[derive(Debug)]
struct FlightStreamState {
    schema: Arc<Schema>,
    dictionaries_by_field: HashMap<i64, ArrayRef>,
}

/// FlightData and the decoded payload (Schema, RecordBatch), if any
#[derive(Debug)]
pub struct DecodedFlightData {
    pub inner: FlightData,
    pub payload: DecodedPayload,
}

impl DecodedFlightData {
    pub fn new_none(inner: FlightData) -> Self {
        Self {
            inner,
            payload: DecodedPayload::None,
        }
    }

    pub fn new_schema(inner: FlightData, schema: Arc<Schema>) -> Self {
        Self {
            inner,
            payload: DecodedPayload::Schema(schema),
        }
    }

    pub fn new_record_batch(inner: FlightData, batch: RecordBatch) -> Self {
        Self {
            inner,
            payload: DecodedPayload::RecordBatch(batch),
        }
    }

    /// return the metadata field of the inner flight data
    pub fn app_metadata(&self) -> &[u8] {
        &self.inner.app_metadata
    }
}

/// The result of decoding [`FlightData`]
#[derive(Debug)]
pub enum DecodedPayload {
    /// None (no data was sent in the corresponding FlightData)
    None,

    /// A decoded Schema message
    Schema(Arc<Schema>),

    /// A decoded Record batch.
    RecordBatch(RecordBatch),
}
