//! Implements the InfluxDB IOx Flight API using Arrow Flight and gRPC

mod request;

use arrow::{
    datatypes::SchemaRef, error::ArrowError, ipc::writer::IpcWriteOptions,
    record_batch::RecordBatch,
};
use arrow_flight::{
    encode::{FlightDataEncoder, FlightDataEncoderBuilder},
    error::FlightError,
    flight_descriptor::DescriptorType,
    flight_service_server::{FlightService as Flight, FlightServiceServer as FlightServer},
    sql::{CommandStatementQuery, ProstMessageExt},
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightEndpoint, FlightInfo,
    HandshakeRequest, HandshakeResponse, PutResult, SchemaAsIpc, SchemaResult, Ticket,
};
use bytes::Bytes;
use data_types::NamespaceNameError;
use datafusion::{error::DataFusionError, physical_plan::ExecutionPlan};
use futures::{ready, Stream, StreamExt, TryStreamExt};
use generated_types::influxdata::iox::querier::v1 as proto;
use iox_query::{
    exec::{ExecutionContextProvider, IOxSessionContext},
    QueryCompletedToken, QueryNamespace,
};
use observability_deps::tracing::{debug, info, warn};
use prost::Message;
use request::{IoxGetRequest, RunQuery};
use service_common::{datafusion_error_to_tonic_code, planner::Planner, QueryNamespaceProvider};
use snafu::{ResultExt, Snafu};
use std::{fmt::Debug, pin::Pin, sync::Arc, task::Poll, time::Instant};
use tonic::{Request, Response, Streaming};
use trace::{ctx::SpanContext, span::SpanExt};
use trace_http::ctx::{RequestLogContext, RequestLogContextExt};
use tracker::InstrumentedAsyncOwnedSemaphorePermit;

/// The name of the grpc header that contains the target iox namespace
/// name for FlightSQL requests.
///
/// See <https://lists.apache.org/thread/fd6r1n7vt91sg2c7fr35wcrsqz6x4645>
/// for discussion on adding support to FlightSQL itself.
const IOX_FLIGHT_SQL_NAMESPACE_HEADER: &str = "iox-namespace-name";

#[allow(clippy::enum_variant_names)]
#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Invalid ticket. Error: {}", source))]
    InvalidTicket { source: request::Error },

    #[snafu(display("Internal creating encoding ticket: {}", source))]
    InternalCreatingTicket { source: request::Error },

    #[snafu(display("Invalid query, could not parse '{}': {}", query, source))]
    InvalidQuery {
        query: String,
        source: serde_json::Error,
    },

    #[snafu(display("Namespace {} not found", namespace_name))]
    NamespaceNotFound { namespace_name: String },

    #[snafu(display(
        "Internal error reading points from namespace {}: {}",
        namespace_name,
        source
    ))]
    Query {
        namespace_name: String,
        source: DataFusionError,
    },

    #[snafu(display("no 'iox-namespace-name' header in request"))]
    NoNamespaceHeader,

    #[snafu(display("Invalid 'iox-namespace-name' header in request: {}", source))]
    InvalidNamespaceHeader {
        source: tonic::metadata::errors::ToStrError,
    },

    #[snafu(display("Invalid namespace name: {}", source))]
    InvalidNamespaceName { source: NamespaceNameError },

    #[snafu(display("Failed to optimize record batch: {}", source))]
    Optimize { source: ArrowError },

    #[snafu(display("Error while planning query: {}", source))]
    Planning {
        source: service_common::planner::Error,
    },

    #[snafu(display("Error during protobuf serialization: {}", source))]
    Serialization { source: prost::EncodeError },

    #[snafu(display("Invalid protobuf: {}", source))]
    Deserialization { source: prost::DecodeError },

    #[snafu(display("Invalid protobuf for type_url'{}': {}", type_url, source))]
    DeserializationTypeKnown {
        type_url: String,
        source: prost::DecodeError,
    },

    #[snafu(display("Unsupported message type: {}", description))]
    UnsupportedMessageType { description: String },
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
            Error::NamespaceNotFound { .. }
            | Error::InvalidTicket { .. }
            | Error::InvalidQuery { .. }
            // TODO(edd): this should be `debug`. Keeping at info while IOx in early development
            | Error::InvalidNamespaceName { .. } => info!(e=%err, msg),
            Error::Query { .. } => info!(e=%err, msg),
            Error::Optimize { .. }
            |Error::NoNamespaceHeader
            |Error::InvalidNamespaceHeader { .. }
            | Error::Planning { .. }
            | Error::Serialization { .. }
            | Error::Deserialization { .. }
            | Error::DeserializationTypeKnown { .. }
            | Error::InternalCreatingTicket { .. }
                | Error::UnsupportedMessageType { .. }
            => {
                warn!(e=%err, msg)
            }
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
            Self::NamespaceNotFound { .. } => tonic::Code::NotFound,
            Self::InvalidTicket { .. }
            | Self::InvalidQuery { .. }
            | Self::Serialization { .. }
            | Self::Deserialization { .. }
            | Self::DeserializationTypeKnown { .. }
            | Self::NoNamespaceHeader
            | Self::InvalidNamespaceHeader { .. }
            | Self::InvalidNamespaceName { .. } => tonic::Code::InvalidArgument,
            Self::Planning { source, .. } | Self::Query { source, .. } => {
                datafusion_error_to_tonic_code(&source)
            }
            Self::UnsupportedMessageType { .. } => tonic::Code::Unimplemented,
            Self::InternalCreatingTicket { .. } | Self::Optimize { .. } => tonic::Code::Internal,
        };

        tonic::Status::new(code, msg)
    }

    fn unsupported_message_type(description: impl Into<String>) -> Self {
        Self::UnsupportedMessageType {
            description: description.into(),
        }
    }
}

type TonicStream<T> = Pin<Box<dyn Stream<Item = Result<T, tonic::Status>> + Send + 'static>>;

/// Concrete implementation of the gRPC Arrow Flight Service API
#[derive(Debug)]
struct FlightService<S>
where
    S: QueryNamespaceProvider,
{
    server: Arc<S>,
}

pub fn make_server<S>(server: Arc<S>) -> FlightServer<impl Flight>
where
    S: QueryNamespaceProvider,
{
    FlightServer::new(FlightService { server })
}

impl<S> FlightService<S>
where
    S: QueryNamespaceProvider,
{
    async fn run_query(
        &self,
        span_ctx: Option<SpanContext>,
        permit: InstrumentedAsyncOwnedSemaphorePermit,
        query: &RunQuery,
        namespace: String,
    ) -> Result<Response<TonicStream<FlightData>>, tonic::Status> {
        let db = self
            .server
            .db(&namespace, span_ctx.child_span("get namespace"))
            .await
            .ok_or_else(|| tonic::Status::not_found(format!("Unknown namespace: {namespace}")))?;

        let ctx = db.new_query_context(span_ctx);
        let (query_completed_token, physical_plan) = match query {
            RunQuery::Sql(sql_query) => {
                let token = db.record_query(&ctx, "sql", Box::new(sql_query.clone()));
                let plan = Planner::new(&ctx)
                    .sql(sql_query)
                    .await
                    .context(PlanningSnafu)?;
                (token, plan)
            }
            RunQuery::InfluxQL(sql_query) => {
                let token = db.record_query(&ctx, "influxql", Box::new(sql_query.clone()));
                let plan = Planner::new(&ctx)
                    .influxql(db, sql_query)
                    .await
                    .context(PlanningSnafu)?;
                (token, plan)
            }
        };

        let output =
            GetStream::new(ctx, physical_plan, namespace, query_completed_token, permit).await?;

        Ok(Response::new(Box::pin(output) as TonicStream<FlightData>))
    }
}

#[tonic::async_trait]
impl<S> Flight for FlightService<S>
where
    S: QueryNamespaceProvider,
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

        // attempt to decode ticket
        let request = IoxGetRequest::try_decode(ticket).context(InvalidTicketSnafu);

        if let Err(e) = &request {
            info!(%e, "Error decoding Flight API ticket");
        };

        let request = request?;
        let namespace_name = request.namespace_name();
        let query = request.query();

        let permit = self
            .server
            .acquire_semaphore(span_ctx.child_span("query rate limit semaphore"))
            .await;

        // Log after we acquire the permit and are about to start execution
        let start = Instant::now();
        info!(%namespace_name, %query, %trace, "Running SQL via flight do_get");

        let response = self
            .run_query(span_ctx, permit, query, namespace_name.to_string())
            .await;

        if let Err(e) = &response {
            info!(%namespace_name, %query, %trace, %e, "Error running SQL query");
        } else {
            let elapsed = Instant::now() - start;
            debug!(%namespace_name, %query,%trace, ?elapsed, "Completed SQL query successfully");
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

    /// Handles requests encoded in the FlightDescriptor
    ///
    /// IOx currently only processes "cmd" type Descriptors (not
    /// paths) and attempts to decodes the [`FlightDescriptor::cmd`]
    /// bytes as an encoded protobuf message
    ///
    ///
    async fn get_flight_info(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, tonic::Status> {
        // look for namespace information in headers
        let namespace_name = request
            .metadata()
            .get(IOX_FLIGHT_SQL_NAMESPACE_HEADER)
            .map(|v| {
                v.to_str()
                    .context(InvalidNamespaceHeaderSnafu)
                    .map(|s| s.to_string())
            })
            .ok_or(Error::NoNamespaceHeader)??;

        let request = request.into_inner();

        let cmd = match request.r#type() {
            DescriptorType::Cmd => Ok(&request.cmd),
            DescriptorType::Path => Err(Error::unsupported_message_type("FlightInfo with Path")),
            DescriptorType::Unknown => Err(Error::unsupported_message_type(
                "FlightInfo of unknown type",
            )),
        }?;

        let message: prost_types::Any =
            prost::Message::decode(cmd.as_ref()).context(DeserializationSnafu)?;

        let flight_info = self.dispatch(&namespace_name, request, message).await?;
        Ok(tonic::Response::new(flight_info))
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

impl<S> FlightService<S>
where
    S: QueryNamespaceProvider,
{
    /// Given a successfully decoded protobuf *Any* message, handles
    /// recognized messages (e.g those defined by FlightSQL) and
    /// creates the appropriate FlightData response
    ///
    /// Arguments
    ///
    /// namespace_name: is the target namespace of the request
    ///
    /// flight_descriptor: is the descriptor sent in the request (included in response)
    ///
    /// msg is the `cmd` field of the flight descriptor decoded as a protobuf  message
    async fn dispatch(
        &self,
        namespace_name: &str,
        flight_descriptor: FlightDescriptor,
        msg: prost_types::Any,
    ) -> Result<FlightInfo> {
        fn try_unpack<T: ProstMessageExt>(msg: &prost_types::Any) -> Result<Option<T>> {
            // Does the type URL match?
            if T::type_url() != msg.type_url {
                return Ok(None);
            }
            // type matched, so try and decode
            let m = prost::Message::decode(&*msg.value).context(DeserializationTypeKnownSnafu {
                type_url: &msg.type_url,
            })?;
            Ok(Some(m))
        }

        // FlightSQL CommandStatementQuery
        let (schema, ticket) = if let Some(cmd) = try_unpack::<CommandStatementQuery>(&msg)? {
            let CommandStatementQuery { query } = cmd;
            debug!(%namespace_name, %query, "Handling FlightSQL CommandStatementQuery");

            // TODO is supposed to return a schema -- if clients
            // actually expect the schema we'll have to plan the query
            // here.
            let schema = vec![];

            // Create a ticket that can be passed to do_get to run the query
            let ticket = IoxGetRequest::new(namespace_name, RunQuery::Sql(query))
                .try_encode()
                .context(InternalCreatingTicketSnafu)?;

            (schema, ticket)
        } else {
            return Err(Error::unsupported_message_type(format!(
                "Unsupported cmd message: {}",
                msg.type_url
            )));
        };

        // form the response

        // Arrow says "set to -1 if not known
        let total_records = -1;
        let total_bytes = -1;

        let endpoint = vec![FlightEndpoint {
            ticket: Some(ticket),
            // "If the list is empty, the expectation is that the
            // ticket can only be redeemed on the current service
            // where the ticket was generated."
            //
            // https://github.com/apache/arrow-rs/blob/a0a5880665b1836890f6843b6b8772d81c463351/format/Flight.proto#L292-L294
            location: vec![],
        }];

        Ok(FlightInfo {
            schema: schema.into(),
            flight_descriptor: Some(flight_descriptor),
            endpoint,
            total_records,
            total_bytes,
        })
    }
}

/// Wrapper over a FlightDataEncodeStream that adds IOx specfic
/// metadata and records completion
struct GetStream {
    inner: IOxFlightDataEncoder,
    #[allow(dead_code)]
    permit: InstrumentedAsyncOwnedSemaphorePermit,
    query_completed_token: QueryCompletedToken,
    done: bool,
}

impl GetStream {
    async fn new(
        ctx: IOxSessionContext,
        physical_plan: Arc<dyn ExecutionPlan>,
        namespace_name: String,
        query_completed_token: QueryCompletedToken,
        permit: InstrumentedAsyncOwnedSemaphorePermit,
    ) -> Result<Self, tonic::Status> {
        let app_metadata = proto::AppMetadata {};

        let schema = physical_plan.schema();

        let query_results = ctx
            .execute_stream(Arc::clone(&physical_plan))
            .await
            .context(QuerySnafu {
                namespace_name: namespace_name.clone(),
            })?
            .map_err(FlightError::from);

        // setup inner stream
        let inner = IOxFlightDataEncoderBuilder::new(schema)
            .with_metadata(app_metadata.encode_to_vec().into())
            .build(query_results);

        Ok(Self {
            inner,
            permit,
            query_completed_token,
            done: false,
        })
    }
}

/// workaround for <https://github.com/apache/arrow-rs/issues/3591>
///
/// data encoder stream that always sends a Schema message even if the
/// underlying stream is empty
struct IOxFlightDataEncoder {
    inner: FlightDataEncoder,
    // The schema of the inner stream. Set to None when a schema
    // message has been sent.
    schema: Option<SchemaRef>,
    done: bool,
}

impl IOxFlightDataEncoder {
    fn new(inner: FlightDataEncoder, schema: SchemaRef) -> Self {
        Self {
            inner,
            schema: Some(schema),
            done: false,
        }
    }
}

#[derive(Debug)]
struct IOxFlightDataEncoderBuilder {
    inner: FlightDataEncoderBuilder,
    schema: SchemaRef,
}

impl IOxFlightDataEncoderBuilder {
    fn new(schema: SchemaRef) -> Self {
        Self {
            inner: FlightDataEncoderBuilder::new(),
            schema,
        }
    }

    pub fn with_metadata(mut self, app_metadata: Bytes) -> Self {
        self.inner = self.inner.with_metadata(app_metadata);
        self
    }

    pub fn build<S>(self, input: S) -> IOxFlightDataEncoder
    where
        S: Stream<Item = arrow_flight::error::Result<RecordBatch>> + Send + 'static,
    {
        let Self { inner, schema } = self;

        IOxFlightDataEncoder::new(inner.build(input), schema)
    }
}

impl Stream for IOxFlightDataEncoder {
    type Item = arrow_flight::error::Result<FlightData>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        loop {
            if self.done {
                return Poll::Ready(None);
            }

            let res = ready!(self.inner.poll_next_unpin(cx));
            match res {
                None => {
                    self.done = true;
                    // return a schema message if we haven't sent any data
                    if let Some(schema) = self.schema.take() {
                        let options = IpcWriteOptions::default();
                        let data: FlightData = SchemaAsIpc::new(schema.as_ref(), &options).into();
                        return Poll::Ready(Some(Ok(data)));
                    }
                }
                Some(Ok(data)) => {
                    // If any data is returned from the underlying stream no need to resend schema
                    self.schema = None;
                    return Poll::Ready(Some(Ok(data)));
                }
                Some(Err(e)) => {
                    self.done = true;
                    return Poll::Ready(Some(Err(e)));
                }
            }
        }
    }
}

impl Stream for GetStream {
    type Item = Result<FlightData, tonic::Status>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        loop {
            if self.done {
                return Poll::Ready(None);
            }

            let res = ready!(self.inner.poll_next_unpin(cx));
            match res {
                None => {
                    self.done = true;
                    // if we get here, all is good
                    self.query_completed_token.set_success();
                }
                Some(Ok(data)) => {
                    return Poll::Ready(Some(Ok(data)));
                }
                Some(Err(e)) => {
                    self.done = true;
                    return Poll::Ready(Some(Err(flight_error_to_status(e))));
                }
            }
        }
    }
}

// TODO remove this when this is released:
// https://github.com/apache/arrow-rs/issues/3566
fn flight_error_to_status(e: FlightError) -> tonic::Status {
    // Special error code translation logic for finding root of chain:
    if let FlightError::Arrow(e) = e {
        let msg = e.to_string();
        let code = datafusion_error_to_tonic_code(&DataFusionError::from(e));
        tonic::Status::new(code, msg)
    } else {
        tonic::Status::from(e)
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
            ticket: br#"{"namespace_name": "my_db", "sql_query": "SELECT 1;"}"#
                .to_vec()
                .into(),
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
