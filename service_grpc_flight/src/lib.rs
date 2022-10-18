//! Implements the native gRPC IOx query API using Arrow Flight

use arrow::error::ArrowError;
use arrow_flight::{
    flight_service_server::{FlightService as Flight, FlightServiceServer as FlightServer},
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, PutResult, SchemaAsIpc, SchemaResult, Ticket,
};
use arrow_util::optimize::{optimize_record_batch, optimize_schema};
use bytes::{Bytes, BytesMut};
use data_types::{DatabaseName, DatabaseNameError};
use datafusion::{error::DataFusionError, physical_plan::ExecutionPlan};
use futures::{SinkExt, Stream, StreamExt};
use generated_types::influxdata::iox::querier::v1 as proto;
use iox_query::{
    exec::{ExecutionContextProvider, IOxSessionContext},
    QueryCompletedToken, QueryDatabase,
};
use observability_deps::tracing::{debug, info, warn};
use pin_project::{pin_project, pinned_drop};
use prost::Message;
use serde::Deserialize;
use service_common::{datafusion_error_to_tonic_code, planner::Planner, QueryDatabaseProvider};
use snafu::{ResultExt, Snafu};
use std::{fmt::Debug, pin::Pin, sync::Arc, task::Poll, time::Instant};
use tokio::task::JoinHandle;
use tonic::{Request, Response, Streaming};
use trace::{ctx::SpanContext, span::SpanExt};
use trace_http::ctx::{RequestLogContext, RequestLogContextExt};
use tracker::InstrumentedAsyncOwnedSemaphorePermit;

#[allow(clippy::enum_variant_names)]
#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Invalid ticket. Error: {:?}", source))]
    InvalidTicket { source: prost::DecodeError },

    #[snafu(display("Invalid legacy ticket. Error: {:?}", source))]
    InvalidTicketLegacy { source: std::string::FromUtf8Error },

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
        source: DataFusionError,
    },

    #[snafu(display("Invalid database name: {}", source))]
    InvalidDatabaseName { source: DatabaseNameError },

    #[snafu(display("Failed to optimize record batch: {}", source))]
    Optimize { source: ArrowError },

    #[snafu(display("Error while planning query: {}", source))]
    Planning {
        source: service_common::planner::Error,
    },

    #[snafu(display("Error during protobuf serialization: {}", source))]
    Serialization { source: prost::EncodeError },
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
            | Error::InvalidTicketLegacy { .. }
            | Error::InvalidQuery { .. }
            // TODO(edd): this should be `debug`. Keeping at info whilst IOx still in early development
            | Error::InvalidDatabaseName { .. } => info!(e=%err, msg),
            Error::Query { .. } => info!(e=%err, msg),
            Error::Optimize { .. }
            | Error::Planning { .. } | Error::Serialization { .. } => warn!(e=%err, msg),
        }
        err.into_status()
    }
}

impl Error {
    /// Converts a result from the business logic into the appropriate tonic (gRPC)
    /// status message to send back to users
    fn into_status(self) -> tonic::Status {
        let msg = self.to_string();

        let code = match self {
            Self::DatabaseNotFound { .. } => tonic::Code::NotFound,
            Self::InvalidTicket { .. }
            | Self::InvalidTicketLegacy { .. }
            | Self::InvalidQuery { .. }
            | Self::InvalidDatabaseName { .. } => tonic::Code::InvalidArgument,
            Self::Planning { source, .. } | Self::Query { source, .. } => {
                datafusion_error_to_tonic_code(&source)
            }
            Self::Optimize { .. } | Self::Serialization { .. } => tonic::Code::Internal,
        };

        tonic::Status::new(code, msg)
    }
}

type TonicStream<T> = Pin<Box<dyn Stream<Item = Result<T, tonic::Status>> + Send + Sync + 'static>>;

#[derive(Deserialize, Debug)]
/// Body of the `Ticket` serialized and sent to the do_get endpoint.
struct ReadInfo {
    database_name: String,
    sql_query: String,
}

impl ReadInfo {
    fn decode_json(ticket: &[u8]) -> Result<Self> {
        let json_str = String::from_utf8(ticket.to_vec()).context(InvalidTicketLegacySnafu {})?;

        let read_info: ReadInfo =
            serde_json::from_str(&json_str).context(InvalidQuerySnafu { query: &json_str })?;

        Ok(read_info)
    }

    fn decode_protobuf(ticket: &[u8]) -> Result<Self> {
        let read_info =
            proto::ReadInfo::decode(Bytes::from(ticket.to_vec())).context(InvalidTicketSnafu {})?;

        Ok(Self {
            database_name: read_info.namespace_name,
            sql_query: read_info.sql_query,
        })
    }
}

/// Concrete implementation of the gRPC Arrow Flight Service API
#[derive(Debug)]
struct FlightService<S>
where
    S: QueryDatabaseProvider,
{
    server: Arc<S>,
}

pub fn make_server<S>(server: Arc<S>) -> FlightServer<impl Flight>
where
    S: QueryDatabaseProvider,
{
    FlightServer::new(FlightService { server })
}

impl<S> FlightService<S>
where
    S: QueryDatabaseProvider,
{
    async fn run_query(
        &self,
        span_ctx: Option<SpanContext>,
        permit: InstrumentedAsyncOwnedSemaphorePermit,
        sql_query: String,
        database_name: String,
    ) -> Result<Response<TonicStream<FlightData>>, tonic::Status> {
        let database = DatabaseName::new(&database_name).context(InvalidDatabaseNameSnafu)?;

        let db = self
            .server
            .db(&database, span_ctx.child_span("get namespace"))
            .await
            .ok_or_else(|| tonic::Status::not_found(format!("Unknown namespace: {database}")))?;

        let ctx = db.new_query_context(span_ctx);
        let query_completed_token = db.record_query(&ctx, "sql", Box::new(sql_query.clone()));

        let physical_plan = Planner::new(&ctx)
            .sql(sql_query)
            .await
            .context(PlanningSnafu)?;

        let output = GetStream::new(
            ctx,
            physical_plan,
            database_name,
            query_completed_token,
            permit,
        )
        .await?;

        Ok(Response::new(Box::pin(output) as TonicStream<FlightData>))
    }
}

#[tonic::async_trait]
impl<S> Flight for FlightService<S>
where
    S: QueryDatabaseProvider,
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
        let external_span_ctx: Option<RequestLogContext> = request.extensions().get().cloned();
        let trace = external_span_ctx.format_jaeger();
        let span_ctx: Option<SpanContext> = request.extensions().get().cloned();
        let ticket = request.into_inner();

        // decode ticket
        let read_info = ReadInfo::decode_protobuf(&ticket.ticket).or_else(|_e| {
            // try legacy json
            ReadInfo::decode_json(&ticket.ticket)
        });

        if let Err(e) = &read_info {
            info!(%e, "Error decoding database and SQL query name from flight ticket");
        };
        let ReadInfo {
            database_name,
            sql_query,
        } = read_info?;

        let permit = self
            .server
            .acquire_semaphore(span_ctx.child_span("query rate limit semaphore"))
            .await;

        // Log after we acquire the permit and are about to start execution
        let start = Instant::now();
        info!(db_name=%database_name, %sql_query, %trace, "Running SQL via flight do_get");

        let response = self
            .run_query(span_ctx, permit, sql_query.clone(), database_name.clone())
            .await;

        if let Err(e) = &response {
            info!(db_name=%database_name, %sql_query, %trace, %e, "Error running SQL query");
        } else {
            let elapsed = Instant::now() - start;
            debug!(db_name=%database_name,%sql_query,%trace, ?elapsed, "Completed SQL query successfully");
        }
        response
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
    #[allow(dead_code)]
    permit: InstrumentedAsyncOwnedSemaphorePermit,
}

impl GetStream {
    async fn new(
        ctx: IOxSessionContext,
        physical_plan: Arc<dyn ExecutionPlan>,
        database_name: String,
        mut query_completed_token: QueryCompletedToken,
        permit: InstrumentedAsyncOwnedSemaphorePermit,
    ) -> Result<Self, tonic::Status> {
        // setup channel
        let (mut tx, rx) = futures::channel::mpsc::channel::<Result<FlightData, tonic::Status>>(1);

        // get schema
        let schema = Arc::new(optimize_schema(&physical_plan.schema()));

        // setup stream
        let options = arrow::ipc::writer::IpcWriteOptions::default();
        let mut schema_flight_data: FlightData = SchemaAsIpc::new(&schema, &options).into();

        // Add response metadata
        let mut bytes = BytesMut::new();
        let app_metadata = proto::AppMetadata {};
        prost::Message::encode(&app_metadata, &mut bytes).context(SerializationSnafu)?;
        schema_flight_data.app_metadata = bytes.to_vec();

        let mut stream_record_batches = ctx
            .execute_stream(Arc::clone(&physical_plan))
            .await
            .context(QuerySnafu {
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
                                tx.send(Err(Error::Optimize { source: e }.into()))
                                    .await
                                    .ok();

                                // end stream
                                return;
                            }
                        }
                    }
                    Err(e) => {
                        // failure sending here is OK because we're cutting the stream anyways
                        tx.send(Err(Error::Query {
                            database_name: database_name.clone(),
                            source: DataFusionError::ArrowError(e),
                        }
                        .into()))
                            .await
                            .ok();

                        // end stream
                        return;
                    }
                }
            }

            // if we get here, all is good
            query_completed_token.set_success()
        });

        Ok(Self {
            rx,
            join_handle,
            done: false,
            permit,
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

#[cfg(test)]
mod tests {
    use futures::Future;
    use metric::{Attributes, Metric, U64Gauge};
    use service_common::test_util::TestDatabaseStore;
    use tokio::pin;

    use super::*;

    #[tokio::test]
    async fn test_query_semaphore() {
        let semaphore_size = 2;
        let test_storage = Arc::new(TestDatabaseStore::new_with_semaphore_size(semaphore_size));

        assert_semaphore_metric(
            &test_storage.metric_registry,
            "iox_async_semaphore_permits_total",
            2,
        );
        assert_semaphore_metric(
            &test_storage.metric_registry,
            "iox_async_semaphore_permits_pending",
            0,
        );
        assert_semaphore_metric(
            &test_storage.metric_registry,
            "iox_async_semaphore_permits_acquired",
            0,
        );

        // add some data
        test_storage.db_or_create("my_db").await;

        let service = FlightService {
            server: Arc::clone(&test_storage),
        };
        let ticket = Ticket {
            ticket: br#"{"database_name": "my_db", "sql_query": "SELECT 1;"}"#.to_vec(),
        };
        let streaming_resp1 = service
            .do_get(tonic::Request::new(ticket.clone()))
            .await
            .unwrap();

        assert_semaphore_metric(
            &test_storage.metric_registry,
            "iox_async_semaphore_permits_total",
            2,
        );
        assert_semaphore_metric(
            &test_storage.metric_registry,
            "iox_async_semaphore_permits_pending",
            0,
        );
        assert_semaphore_metric(
            &test_storage.metric_registry,
            "iox_async_semaphore_permits_acquired",
            1,
        );

        let streaming_resp2 = service
            .do_get(tonic::Request::new(ticket.clone()))
            .await
            .unwrap();

        assert_semaphore_metric(
            &test_storage.metric_registry,
            "iox_async_semaphore_permits_total",
            2,
        );
        assert_semaphore_metric(
            &test_storage.metric_registry,
            "iox_async_semaphore_permits_pending",
            0,
        );
        assert_semaphore_metric(
            &test_storage.metric_registry,
            "iox_async_semaphore_permits_acquired",
            2,
        );

        // 3rd request is pending
        let fut = service.do_get(tonic::Request::new(ticket.clone()));
        pin!(fut);
        assert_fut_pending(&mut fut).await;

        assert_semaphore_metric(
            &test_storage.metric_registry,
            "iox_async_semaphore_permits_total",
            2,
        );
        assert_semaphore_metric(
            &test_storage.metric_registry,
            "iox_async_semaphore_permits_pending",
            1,
        );
        assert_semaphore_metric(
            &test_storage.metric_registry,
            "iox_async_semaphore_permits_acquired",
            2,
        );

        // free permit
        drop(streaming_resp1);
        let streaming_resp3 = fut.await;

        assert_semaphore_metric(
            &test_storage.metric_registry,
            "iox_async_semaphore_permits_total",
            2,
        );
        assert_semaphore_metric(
            &test_storage.metric_registry,
            "iox_async_semaphore_permits_pending",
            0,
        );
        assert_semaphore_metric(
            &test_storage.metric_registry,
            "iox_async_semaphore_permits_acquired",
            2,
        );

        drop(streaming_resp2);
        drop(streaming_resp3);

        assert_semaphore_metric(
            &test_storage.metric_registry,
            "iox_async_semaphore_permits_total",
            2,
        );
        assert_semaphore_metric(
            &test_storage.metric_registry,
            "iox_async_semaphore_permits_pending",
            0,
        );
        assert_semaphore_metric(
            &test_storage.metric_registry,
            "iox_async_semaphore_permits_acquired",
            0,
        );
    }

    /// Assert that given future is pending.
    ///
    /// This will try to poll the future a bit to ensure that it is not stuck in tokios task preemption.
    async fn assert_fut_pending<F>(fut: &mut F)
    where
        F: Future + Send + Unpin,
    {
        tokio::select! {
            _ = fut => panic!("future is not pending, yielded"),
            _ = tokio::time::sleep(std::time::Duration::from_millis(10)) => {},
        };
    }

    fn assert_semaphore_metric(registry: &metric::Registry, name: &'static str, expected: u64) {
        let actual = registry
            .get_instrument::<Metric<U64Gauge>>(name)
            .expect("failed to read metric")
            .get_observer(&Attributes::from(&[("semaphore", "query_execution")]))
            .expect("failed to get observer")
            .fetch();
        assert_eq!(actual, expected);
    }
}
