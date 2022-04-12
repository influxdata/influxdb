//! gRPC service implementations for `ingester`.

use crate::{data::IngesterQueryResponse, handler::IngestHandler};
use arrow::{
    array::{make_array, ArrayRef, MutableArrayData},
    datatypes::{DataType, Field, Schema, SchemaRef},
    error::ArrowError,
    record_batch::RecordBatch,
};
use arrow_flight::{
    flight_service_server::{FlightService as Flight, FlightServiceServer as FlightServer},
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, PutResult, SchemaAsIpc, SchemaResult, Ticket,
};
use futures::{SinkExt, Stream, StreamExt};
use generated_types::influxdata::iox::ingester::v1::{
    self as proto,
    write_info_service_server::{WriteInfoService, WriteInfoServiceServer},
    GetWriteInfoRequest, GetWriteInfoResponse,
};
use observability_deps::tracing::{info, warn};
use pin_project::{pin_project, pinned_drop};
use prost::Message;
use snafu::{ResultExt, Snafu};
use std::{pin::Pin, sync::Arc, task::Poll};
use tokio::task::JoinHandle;
use tonic::{Request, Response, Streaming};
use trace::ctx::SpanContext;
use write_summary::WriteSummary;

/// This type is responsible for managing all gRPC services exposed by
/// `ingester`.
#[derive(Debug, Default)]
pub struct GrpcDelegate<I: IngestHandler> {
    ingest_handler: Arc<I>,
}

impl<I: IngestHandler + Send + Sync + 'static> GrpcDelegate<I> {
    /// Initialise a new [`GrpcDelegate`] passing valid requests to the
    /// specified `ingest_handler`.
    pub fn new(ingest_handler: Arc<I>) -> Self {
        Self { ingest_handler }
    }

    /// Acquire an Arrow Flight gRPC service implementation.
    pub fn flight_service(&self) -> FlightServer<impl Flight> {
        FlightServer::new(FlightService {
            ingest_handler: Arc::clone(&self.ingest_handler),
        })
    }

    /// Acquire an WriteInfo gRPC service implementation.
    pub fn write_info_service(&self) -> WriteInfoServiceServer<impl WriteInfoService> {
        WriteInfoServiceServer::new(WriteInfoServiceImpl::new(
            Arc::clone(&self.ingest_handler) as _
        ))
    }
}

/// Implementation of write info
struct WriteInfoServiceImpl {
    handler: Arc<dyn IngestHandler + Send + Sync + 'static>,
}

impl WriteInfoServiceImpl {
    pub fn new(handler: Arc<dyn IngestHandler + Send + Sync + 'static>) -> Self {
        Self { handler }
    }
}

fn write_summary_error_to_status(e: write_summary::Error) -> tonic::Status {
    use write_summary::Error;

    match e {
        // treat "unknown partition error" as a failed precondition
        // (so the client can distinguish between "write isn't
        // readable" from "we can't tell if write is readable"
        e @ Error::UnknownKafkaPartition { .. } => {
            tonic::Status::failed_precondition(format!("Can not determine status of write: {}", e))
        }
    }
}

#[tonic::async_trait]
impl WriteInfoService for WriteInfoServiceImpl {
    async fn get_write_info(
        &self,
        request: Request<GetWriteInfoRequest>,
    ) -> Result<Response<GetWriteInfoResponse>, tonic::Status> {
        let GetWriteInfoRequest { write_token } = request.into_inner();

        let write_summary =
            WriteSummary::try_from_token(&write_token).map_err(tonic::Status::invalid_argument)?;

        let progress = self
            .handler
            .progresses(write_summary.kafka_partitions())
            .await
            .map_err(|e| tonic::Status::invalid_argument(e.to_string()))?;

        let readable = write_summary
            .readable(&progress)
            .map_err(write_summary_error_to_status)?;

        let persisted = write_summary
            .persisted(&progress)
            .map_err(write_summary_error_to_status)?;

        Ok(tonic::Response::new(GetWriteInfoResponse {
            readable,
            persisted,
        }))
    }
}

#[derive(Debug, Snafu)]
#[allow(missing_docs)]
pub enum Error {
    #[snafu(display("Failed to hydrate dictionary: {}", source))]
    Dictionary { source: ArrowError },

    #[snafu(display("Invalid ticket. Error: {:?} Ticket: {:?}", source, ticket))]
    InvalidTicket {
        source: prost::DecodeError,
        ticket: Vec<u8>,
    },

    #[snafu(display("Invalid query, could not convert protobuf: {}", source))]
    InvalidQuery {
        source: generated_types::google::FieldViolation,
    },

    #[snafu(display("Invalid RecordBatch: {}", source))]
    InvalidRecordBatch { source: ArrowError },

    #[snafu(display("Error while performing query: {}", source))]
    Query {
        source: Box<crate::querier_handler::Error>,
    },

    #[snafu(display("Error while streaming query results: {}", source))]
    QueryStream { source: ArrowError },

    #[snafu(display("Error during protobuf serialization: {}", source))]
    Serialization { source: prost::EncodeError },
}

impl From<Error> for tonic::Status {
    /// Logs and converts a result from the business logic into the appropriate tonic status
    fn from(err: Error) -> Self {
        // An explicit match on the Error enum will ensure appropriate logging is handled for any
        // new error variants.
        let msg = "Error handling Flight gRPC request";
        match err {
            Error::InvalidTicket { .. } | Error::InvalidQuery { .. } | Error::Query { .. } => {
                // TODO(edd): this should be `debug`. Keeping at info whilst IOx still in early
                // development
                info!(?err, msg)
            }
            Error::Dictionary { .. }
            | Error::InvalidRecordBatch { .. }
            | Error::QueryStream { .. }
            | Error::Serialization { .. } => warn!(?err, msg),
        }
        err.to_status()
    }
}

impl Error {
    /// Converts a result from the business logic into the appropriate tonic status
    fn to_status(&self) -> tonic::Status {
        use tonic::Status;
        match self {
            Self::InvalidTicket { .. } | Self::InvalidQuery { .. } => {
                Status::invalid_argument(self.to_string())
            }
            Self::Query { .. }
            | Self::InvalidRecordBatch { .. }
            | Self::Dictionary { .. }
            | Self::QueryStream { .. }
            | Self::Serialization { .. } => Status::internal(self.to_string()),
        }
    }
}

/// Concrete implementation of the gRPC Arrow Flight Service API
#[derive(Debug)]
struct FlightService<I: IngestHandler + Send + Sync + 'static> {
    ingest_handler: Arc<I>,
}

type TonicStream<T> = Pin<Box<dyn Stream<Item = Result<T, tonic::Status>> + Send + Sync + 'static>>;

#[tonic::async_trait]
impl<I: IngestHandler + Send + Sync + 'static> Flight for FlightService<I> {
    type HandshakeStream = TonicStream<HandshakeResponse>;
    type ListFlightsStream = TonicStream<FlightInfo>;
    type DoGetStream = TonicStream<FlightData>;
    type DoPutStream = TonicStream<PutResult>;
    type DoActionStream = TonicStream<arrow_flight::Result>;
    type ListActionsStream = TonicStream<ActionType>;
    type DoExchangeStream = TonicStream<FlightData>;

    async fn get_schema(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, tonic::Status> {
        Err(tonic::Status::unimplemented("Not yet implemented"))
    }

    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, tonic::Status> {
        let _span_ctx: Option<SpanContext> = request.extensions().get().cloned();
        let ticket = request.into_inner();

        let proto_query_request =
            proto::IngesterQueryRequest::decode(&*ticket.ticket).context(InvalidTicketSnafu {
                ticket: ticket.ticket,
            })?;

        let query_request = proto_query_request.try_into().context(InvalidQuerySnafu)?;

        let query_response = self
            .ingest_handler
            .query(query_request)
            .await
            .map_err(Box::new)
            .context(QuerySnafu)?;

        let output = GetStream::new(query_response).await?;

        Ok(Response::new(Box::pin(output) as Self::DoGetStream))
    }

    async fn handshake(
        &self,
        request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, tonic::Status> {
        let request = request.into_inner().message().await?.unwrap();
        let response = HandshakeResponse {
            protocol_version: request.protocol_version,
            payload: request.payload,
        };
        let output = futures::stream::iter(std::iter::once(Ok(response)));
        Ok(Response::new(Box::pin(output) as Self::HandshakeStream))
    }

    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, tonic::Status> {
        Err(tonic::Status::unimplemented("Not yet implemented"))
    }

    async fn get_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, tonic::Status> {
        Err(tonic::Status::unimplemented("Not yet implemented"))
    }

    async fn do_put(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, tonic::Status> {
        Err(tonic::Status::unimplemented("Not yet implemented"))
    }

    async fn do_action(
        &self,
        _request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, tonic::Status> {
        Err(tonic::Status::unimplemented("Not yet implemented"))
    }

    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, tonic::Status> {
        Err(tonic::Status::unimplemented("Not yet implemented"))
    }

    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, tonic::Status> {
        Err(tonic::Status::unimplemented("Not yet implemented"))
    }
}

#[pin_project(PinnedDrop)]
struct GetStream {
    #[pin]
    rx: futures::channel::mpsc::Receiver<Result<FlightData, tonic::Status>>,
    join_handle: JoinHandle<()>,
    done: bool,
}

impl GetStream {
    async fn new(query_response: IngesterQueryResponse) -> Result<Self, tonic::Status> {
        let IngesterQueryResponse {
            mut data,
            schema,
            unpersisted_partitions,
        } = query_response;

        // setup channel
        let (mut tx, rx) = futures::channel::mpsc::channel::<Result<FlightData, tonic::Status>>(1);

        // get schema
        let schema = Arc::new(optimize_schema(&schema.as_arrow()));
        let options = arrow::ipc::writer::IpcWriteOptions::default();
        let mut schema_flight_data: FlightData = SchemaAsIpc::new(&schema, &options).into();

        // Add max_sequencer_number to app metadata
        let mut bytes = bytes::BytesMut::new();
        let app_metadata = proto::IngesterQueryResponseMetadata {
            unpersisted_partitions: unpersisted_partitions
                .into_iter()
                .map(|(id, status)| {
                    (
                        id.get(),
                        proto::PartitionStatus {
                            parquet_max_sequence_number: status
                                .parquet_max_sequence_number
                                .map(|x| x.get()),
                            tombstone_max_sequence_number: status
                                .tombstone_max_sequence_number
                                .map(|x| x.get()),
                        },
                    )
                })
                .collect(),
        };
        prost::Message::encode(&app_metadata, &mut bytes).context(SerializationSnafu)?;
        schema_flight_data.app_metadata = bytes.to_vec();

        let join_handle = tokio::spawn(async move {
            if tx.send(Ok(schema_flight_data)).await.is_err() {
                // receiver gone
                return;
            }

            while let Some(batch_or_err) = data.next().await {
                match batch_or_err {
                    Ok(batch) => {
                        match optimize_record_batch(&batch, Arc::clone(&schema)) {
                            Ok(batch) => {
                                let (flight_dictionaries, flight_batch) =
                                    arrow_flight::utils::flight_data_from_arrow_batch(
                                        &batch, &options,
                                    );

                                for dict in flight_dictionaries {
                                    if tx.send(Ok(dict)).await.is_err() {
                                        // receiver is gone
                                        return;
                                    }
                                }

                                if tx.send(Ok(flight_batch)).await.is_err() {
                                    // receiver is gone
                                    return;
                                }
                            }
                            Err(e) => {
                                // failure sending here is OK because we're cutting the stream anyways
                                tx.send(Err(e.into())).await.ok();

                                // end stream
                                return;
                            }
                        }
                    }
                    Err(e) => {
                        // failure sending here is OK because we're cutting the stream anyways
                        tx.send(Err(Error::QueryStream { source: e }.into()))
                            .await
                            .ok();

                        // end stream
                        return;
                    }
                }
            }
        });

        Ok(Self {
            rx,
            join_handle,
            done: false,
        })
    }
}

#[pinned_drop]
impl PinnedDrop for GetStream {
    fn drop(self: Pin<&mut Self>) {
        self.join_handle.abort();
    }
}

impl Stream for GetStream {
    type Item = Result<FlightData, tonic::Status>;

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let this = self.project();
        if *this.done {
            Poll::Ready(None)
        } else {
            match this.rx.poll_next(cx) {
                Poll::Ready(None) => {
                    *this.done = true;
                    Poll::Ready(None)
                }
                e @ Poll::Ready(Some(Err(_))) => {
                    *this.done = true;
                    e
                }
                other => other,
            }
        }
    }
}

/// Some batches are small slices of the underlying arrays.
/// At this stage we only know the number of rows in the record batch
/// and the sizes in bytes of the backing buffers of the column arrays.
/// There is no straight-forward relationship between these two quantities,
/// since some columns can host variable length data such as strings.
///
/// However we can apply a quick&dirty heuristic:
/// if the backing buffer is two orders of magnitudes bigger
/// than the number of rows in the result set, we assume
/// that deep-copying the record batch is cheaper than the and transfer costs.
///
/// Possible improvements: take the type of the columns into consideration
/// and perhaps sample a few element sizes (taking care of not doing more work
/// than to always copying the results in the first place).
///
/// Or we just fix this upstream in
/// arrow_flight::utils::flight_data_from_arrow_batch and re-encode the array
/// into a smaller buffer while we have to copy stuff around anyway.
///
/// See rationale and discussions about future improvements on
/// <https://github.com/influxdata/influxdb_iox/issues/1133>
fn optimize_record_batch(batch: &RecordBatch, schema: SchemaRef) -> Result<RecordBatch, Error> {
    let max_buf_len = batch
        .columns()
        .iter()
        .map(|a| a.get_array_memory_size())
        .max()
        .unwrap_or_default();

    let columns: Result<Vec<_>, _> = batch
        .columns()
        .iter()
        .map(|column| {
            if matches!(column.data_type(), DataType::Dictionary(_, _)) {
                hydrate_dictionary(column)
            } else if max_buf_len > batch.num_rows() * 100 {
                Ok(deep_clone_array(column))
            } else {
                Ok(Arc::clone(column))
            }
        })
        .collect();

    RecordBatch::try_new(schema, columns?).context(InvalidRecordBatchSnafu)
}

fn deep_clone_array(array: &ArrayRef) -> ArrayRef {
    let mut mutable = MutableArrayData::new(vec![array.data()], false, 0);
    mutable.extend(0, 0, array.len());

    make_array(mutable.freeze())
}

/// Convert dictionary types to underlying types
/// See hydrate_dictionary for more information
fn optimize_schema(schema: &Schema) -> Schema {
    let fields = schema
        .fields()
        .iter()
        .map(|field| match field.data_type() {
            DataType::Dictionary(_, value_type) => Field::new(
                field.name(),
                value_type.as_ref().clone(),
                field.is_nullable(),
            ),
            _ => field.clone(),
        })
        .collect();

    Schema::new(fields)
}

/// Hydrates a dictionary to its underlying type
///
/// An IPC response, streaming or otherwise, defines its schema up front
/// which defines the mapping from dictionary IDs. It then sends these
/// dictionaries over the wire.
///
/// This requires identifying the different dictionaries in use, assigning
/// them IDs, and sending new dictionaries, delta or otherwise, when needed
///
/// This is tracked by #1318
///
/// For now we just hydrate the dictionaries to their underlying type
fn hydrate_dictionary(array: &ArrayRef) -> Result<ArrayRef, Error> {
    match array.data_type() {
        DataType::Dictionary(_, value) => {
            arrow::compute::cast(array, value).context(DictionarySnafu)
        }
        _ => unreachable!("not a dictionary"),
    }
}
