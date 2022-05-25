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
use datafusion::physical_plan::ExecutionPlan;
use futures::{SinkExt, Stream, StreamExt};
use generated_types::influxdata::iox::querier::v1 as proto;
use iox_query::{
    exec::{ExecutionContextProvider, IOxSessionContext},
    QueryCompletedToken, QueryDatabase,
};
use observability_deps::tracing::{info, warn};
use pin_project::{pin_project, pinned_drop};
use prost::Message;
use serde::Deserialize;
use service_common::{planner::Planner, QueryDatabaseProvider};
use snafu::{ResultExt, Snafu};
use std::{fmt::Debug, pin::Pin, sync::Arc, task::Poll};
use tokio::task::JoinHandle;
use tonic::{Request, Response, Streaming};

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
        source: Box<dyn std::error::Error + Send + Sync>,
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
            | Error::InvalidDatabaseName { .. } => info!(?err, msg),
            Error::Query { .. } => info!(?err, msg),
            Error::Optimize { .. }
            | Error::Planning { .. } | Error::Serialization { .. } => warn!(?err, msg),
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
            Self::InvalidTicketLegacy { .. } => Status::invalid_argument(self.to_string()),
            Self::InvalidQuery { .. } => Status::invalid_argument(self.to_string()),
            Self::DatabaseNotFound { .. } => Status::not_found(self.to_string()),
            Self::Query { .. } => Status::internal(self.to_string()),
            Self::InvalidDatabaseName { .. } => Status::invalid_argument(self.to_string()),
            Self::Planning {
                source: service_common::planner::Error::External(_),
            } => Status::internal(self.to_string()),
            Self::Planning { .. } => Status::invalid_argument(self.to_string()),
            Self::Optimize { .. } => Status::internal(self.to_string()),
            Self::Serialization { .. } => Status::internal(self.to_string()),
        }
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
        let span_ctx = request.extensions().get().cloned();
        let ticket = request.into_inner();

        // decode ticket
        let read_info = match ReadInfo::decode_protobuf(&ticket.ticket) {
            Ok(read_info) => read_info,
            Err(_) => {
                // try legacy json
                ReadInfo::decode_json(&ticket.ticket)?
            }
        };

        let database =
            DatabaseName::new(&read_info.database_name).context(InvalidDatabaseNameSnafu)?;

        let db =
            self.server.db(&database).await.ok_or_else(|| {
                tonic::Status::not_found(format!("Unknown namespace: {database}"))
            })?;

        let ctx = db.new_query_context(span_ctx);
        let query_completed_token =
            db.record_query(&ctx, "sql", Box::new(read_info.sql_query.clone()));

        let physical_plan = Planner::new(&ctx)
            .sql(&read_info.sql_query)
            .await
            .context(PlanningSnafu)?;

        let output = GetStream::new(
            ctx,
            physical_plan,
            read_info.database_name,
            query_completed_token,
        )
        .await?;

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
        ctx: IOxSessionContext,
        physical_plan: Arc<dyn ExecutionPlan>,
        database_name: String,
        mut query_completed_token: QueryCompletedToken,
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
            .map_err(|e| Box::new(e) as _)
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

            // if we get here, all is good
            query_completed_token.set_success()
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
