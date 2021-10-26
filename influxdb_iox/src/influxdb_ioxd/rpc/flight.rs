//! Implements the native gRPC IOx query API using Arrow Flight
use std::fmt::Debug;
use std::task::Poll;
use std::{pin::Pin, sync::Arc};

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
use datafusion::physical_plan::ExecutionPlan;
use futures::{SinkExt, Stream, StreamExt};
use pin_project::{pin_project, pinned_drop};
use serde::Deserialize;
use snafu::{ResultExt, Snafu};
use tokio::task::JoinHandle;
use tonic::{Request, Response, Streaming};

use data_types::{DatabaseName, DatabaseNameError};
use observability_deps::tracing::{info, warn};
use query::exec::{ExecutionContextProvider, IOxExecutionContext};
use server::{connection::ConnectionManager, Server};

use crate::influxdb_ioxd::rpc::error::default_server_error_handler;

use super::super::planner::Planner;

#[allow(clippy::enum_variant_names)]
#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Invalid ticket. Error: {:?} Ticket: {:?}", source, ticket))]
    InvalidTicket {
        source: std::string::FromUtf8Error,
        ticket: Vec<u8>,
    },
    #[snafu(display("Invalid query, could not parse '{}': {}", query, source))]
    InvalidQuery {
        query: String,
        source: serde_json::Error,
    },

    #[snafu(display("Database {} not found", database_name))]
    DatabaseNotFound { database_name: String },

    #[snafu(display(
        "Internal error reading points from database {}:  {}",
        database_name,
        source
    ))]
    Query {
        database_name: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Invalid database name: {}", source))]
    InvalidDatabaseName { source: DatabaseNameError },

    #[snafu(display("Invalid RecordBatch: {}", source))]
    InvalidRecordBatch { source: ArrowError },

    #[snafu(display("Failed to hydrate dictionary: {}", source))]
    DictionaryError { source: ArrowError },

    #[snafu(display("Error while planning query: {}", source))]
    Planning {
        source: super::super::planner::Error,
    },
}
pub type Result<T, E = Error> = std::result::Result<T, E>;

impl From<Error> for tonic::Status {
    /// Converts a result from the business logic into the appropriate tonic
    /// status
    fn from(err: Error) -> Self {
        // An explicit match on the Error enum will ensure appropriate
        // logging is handled for any new error variants.
        let msg = "Error handling Flight gRPC request";
        match err {
            Error::DatabaseNotFound { .. }
            | Error::InvalidTicket { .. }
            | Error::InvalidQuery { .. }
            // TODO(edd): this should be `debug`. Keeping at info whilst IOx still in early development
            | Error::InvalidDatabaseName { .. } => info!(?err, msg),
            Error::Query { .. } => info!(?err, msg),
            Error::DictionaryError { .. }
            | Error::InvalidRecordBatch { .. }
            | Error::Planning { .. } => warn!(?err, msg),
        }
        err.to_status()
    }
}

impl Error {
    /// Converts a result from the business logic into the appropriate tonic
    /// status
    fn to_status(&self) -> tonic::Status {
        use tonic::Status;
        match &self {
            Self::InvalidTicket { .. } => Status::invalid_argument(self.to_string()),
            Self::InvalidQuery { .. } => Status::invalid_argument(self.to_string()),
            Self::DatabaseNotFound { .. } => Status::not_found(self.to_string()),
            Self::Query { .. } => Status::internal(self.to_string()),
            Self::InvalidDatabaseName { .. } => Status::invalid_argument(self.to_string()),
            Self::InvalidRecordBatch { .. } => Status::internal(self.to_string()),
            Self::Planning { .. } => Status::invalid_argument(self.to_string()),
            Self::DictionaryError { .. } => Status::internal(self.to_string()),
        }
    }
}

type TonicStream<T> = Pin<Box<dyn Stream<Item = Result<T, tonic::Status>> + Send + Sync + 'static>>;

#[derive(Deserialize, Debug)]
/// Body of the `Ticket` serialized and sent to the do_get endpoint; this should
/// be shared with the read API probably...
struct ReadInfo {
    database_name: String,
    sql_query: String,
}

/// Concrete implementation of the gRPC Arrow Flight Service API
#[derive(Debug)]
struct FlightService<M: ConnectionManager> {
    server: Arc<Server<M>>,
}

pub fn make_server<M>(server: Arc<Server<M>>) -> FlightServer<impl Flight>
where
    M: ConnectionManager + Send + Sync + Debug + 'static,
{
    FlightServer::new(FlightService { server })
}

#[tonic::async_trait]
impl<M: ConnectionManager> Flight for FlightService<M>
where
    M: ConnectionManager + Send + Sync + Debug + 'static,
{
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
        let span_ctx = request.extensions().get().cloned();
        let ticket = request.into_inner();
        let json_str = String::from_utf8(ticket.ticket.to_vec()).context(InvalidTicket {
            ticket: ticket.ticket,
        })?;

        let read_info: ReadInfo =
            serde_json::from_str(&json_str).context(InvalidQuery { query: &json_str })?;

        let database = DatabaseName::new(&read_info.database_name).context(InvalidDatabaseName)?;

        let db = self
            .server
            .db(&database)
            .map_err(default_server_error_handler)?;

        let ctx = db.new_query_context(span_ctx);

        let physical_plan = Planner::new(&ctx)
            .sql(&read_info.sql_query)
            .await
            .context(Planning)?;

        let output = GetStream::new(ctx, physical_plan, read_info.database_name).await?;

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
    async fn new(
        ctx: IOxExecutionContext,
        physical_plan: Arc<dyn ExecutionPlan>,
        database_name: String,
    ) -> Result<Self, tonic::Status> {
        // setup channel
        let (mut tx, rx) = futures::channel::mpsc::channel::<Result<FlightData, tonic::Status>>(1);

        // get schema
        let schema = Arc::new(optimize_schema(&physical_plan.schema()));

        // setup stream
        let options = arrow::ipc::writer::IpcWriteOptions::default();
        let schema_flight_data = SchemaAsIpc::new(&schema, &options).into();
        let mut stream_record_batches = ctx
            .execute_stream(Arc::clone(&physical_plan))
            .await
            .map_err(|e| Box::new(e) as _)
            .context(Query {
                database_name: &database_name,
            })?;

        let join_handle = tokio::spawn(async move {
            if tx.send(Ok(schema_flight_data)).await.is_err() {
                // receiver gone
                return;
            }

            while let Some(batch_or_err) = stream_record_batches.next().await {
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
                        tx.send(Err(Error::Query {
                            database_name: database_name.clone(),
                            source: Box::new(e),
                        }
                        .into()))
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

    RecordBatch::try_new(schema, columns?).context(InvalidRecordBatch)
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
            arrow::compute::cast(array, value).context(DictionaryError)
        }
        _ => unreachable!("not a dictionary"),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::StringArray;
    use arrow::{
        array::{DictionaryArray, UInt32Array},
        datatypes::{DataType, Int32Type},
    };
    use arrow_flight::utils::flight_data_to_arrow_batch;

    use datafusion::physical_plan::limit::truncate_batch;

    use super::*;

    #[test]
    fn test_deep_clone_array() {
        let mut builder = UInt32Array::builder(1000);
        builder.append_slice(&[1, 2, 3, 4, 5, 6]).unwrap();
        let array: ArrayRef = Arc::new(builder.finish());
        assert_eq!(array.len(), 6);

        let sliced = array.slice(0, 2);
        assert_eq!(sliced.len(), 2);

        let deep_cloned = deep_clone_array(&sliced);
        assert!(sliced.data().get_array_memory_size() > deep_cloned.data().get_array_memory_size());
    }

    #[test]
    fn test_encode_flight_data() {
        let options = arrow::ipc::writer::IpcWriteOptions::default();
        let c1 = UInt32Array::from(vec![1, 2, 3, 4, 5, 6]);

        let batch = RecordBatch::try_from_iter(vec![("a", Arc::new(c1) as ArrayRef)])
            .expect("cannot create record batch");
        let schema = batch.schema();

        let (_, baseline_flight_batch) =
            arrow_flight::utils::flight_data_from_arrow_batch(&batch, &options);

        let big_batch = truncate_batch(&batch, batch.num_rows() - 1);
        let optimized_big_batch =
            optimize_record_batch(&big_batch, Arc::clone(&schema)).expect("failed to optimize");
        let (_, optimized_big_flight_batch) =
            arrow_flight::utils::flight_data_from_arrow_batch(&optimized_big_batch, &options);

        assert_eq!(
            baseline_flight_batch.data_body.len(),
            optimized_big_flight_batch.data_body.len()
        );

        let small_batch = truncate_batch(&batch, 1);
        let optimized_small_batch =
            optimize_record_batch(&small_batch, Arc::clone(&schema)).expect("failed to optimize");
        let (_, optimized_small_flight_batch) =
            arrow_flight::utils::flight_data_from_arrow_batch(&optimized_small_batch, &options);

        assert!(
            baseline_flight_batch.data_body.len() > optimized_small_flight_batch.data_body.len()
        );
    }

    #[test]
    fn test_encode_flight_data_dictionary() {
        let options = arrow::ipc::writer::IpcWriteOptions::default();

        let c1 = UInt32Array::from(vec![1, 2, 3, 4, 5, 6]);
        let c2: DictionaryArray<Int32Type> = vec![
            Some("foo"),
            Some("bar"),
            None,
            Some("fiz"),
            None,
            Some("foo"),
        ]
        .into_iter()
        .collect();

        let batch =
            RecordBatch::try_from_iter(vec![("a", Arc::new(c1) as ArrayRef), ("b", Arc::new(c2))])
                .expect("cannot create record batch");

        let original_schema = batch.schema();
        let optimized_schema = Arc::new(optimize_schema(&original_schema));

        let optimized_batch = optimize_record_batch(&batch, Arc::clone(&optimized_schema)).unwrap();

        let (_, flight_data) =
            arrow_flight::utils::flight_data_from_arrow_batch(&optimized_batch, &options);

        let batch =
            flight_data_to_arrow_batch(&flight_data, Arc::clone(&optimized_schema), &[None, None])
                .unwrap();

        // Should hydrate string dictionary for transport
        assert_eq!(optimized_schema.field(1).data_type(), &DataType::Utf8);
        let array = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        let expected = StringArray::from(vec![
            Some("foo"),
            Some("bar"),
            None,
            Some("fiz"),
            None,
            Some("foo"),
        ]);
        assert_eq!(array, &expected)
    }
}
