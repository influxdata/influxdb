use arrow_flight::{
    decode::{DecodedFlightData, DecodedPayload, FlightDataDecoder},
    Ticket,
};
use async_trait::async_trait;
use client_util::connection::{self, Connection};
use futures::StreamExt;
use ingester_query_grpc::{influxdata::iox::ingester::v1 as proto, IngesterQueryRequest};
use observability_deps::tracing::{debug, warn};
use prost::Message;
use snafu::{ResultExt, Snafu};
use std::{collections::HashMap, fmt::Debug, ops::DerefMut, sync::Arc};
use trace::{ctx::SpanContext, span::SpanRecorder};
use trace_http::ctx::format_jaeger_trace_context;

pub use influxdb_iox_client::flight::Error as FlightError;

#[derive(Debug, Snafu)]
#[allow(missing_copy_implementations, missing_docs)]
pub enum Error {
    #[snafu(display("Failed to connect to ingester '{}': {}", ingester_address, source))]
    Connecting {
        ingester_address: String,
        source: connection::Error,
    },

    #[snafu(display("Failed ingester handshake '{}': {}", ingester_address, source))]
    Handshake {
        ingester_address: String,
        source: FlightError,
    },

    #[snafu(display("Internal error creating flight request : {}", source))]
    CreatingRequest {
        source: ingester_query_grpc::FieldViolation,
    },

    #[snafu(display("Failed to perform flight request: {}", source))]
    Flight { source: FlightError },

    #[snafu(display("Can not contact ingester. Circuit broken: {}", ingester_address))]
    CircuitBroken { ingester_address: String },
}

impl Error {
    /// Checks if this is a connection / ingester-state error.
    ///
    /// This can lead to cutting the connection or breaking/opening the circuit.
    pub fn is_upstream_error(&self) -> bool {
        match self {
            Self::Flight {
                source:
                    _source @ influxdb_iox_client::flight::Error::ArrowFlightError(
                        arrow_flight::error::FlightError::Tonic(e),
                    ),
            } => !matches!(
                e.code(),
                tonic::Code::NotFound | tonic::Code::ResourceExhausted
            ),
            Self::Connecting { .. } | Self::Handshake { .. } | Self::Flight { .. } => true,
            // do NOT break circuit for client-side errors
            Self::CreatingRequest { .. } => false,
            // circuit broken, not an upstream error
            Self::CircuitBroken { .. } => false,
        }
    }
}

/// Abstract Flight client interface for Ingester.
///
/// May use an internal connection pool.
#[async_trait]
pub trait IngesterFlightClient: Debug + Send + Sync + 'static {
    /// Invalidate connection to given ingester.
    ///
    /// This is a no-op if there is no active connection to the given ingester.
    async fn invalidate_connection(&self, ingester_address: Arc<str>);

    /// Send query to given ingester.
    async fn query(
        &self,
        ingester_address: Arc<str>,
        request: IngesterQueryRequest,
        span_context: Option<SpanContext>,
    ) -> Result<Box<dyn QueryData>, Error>;
}

/// Default [`IngesterFlightClient`] implementation that uses a real connection
#[derive(Debug, Default)]
pub struct FlightClientImpl {
    /// Cached connections
    /// key: ingester_address (e.g. "http://ingester-1:8082")
    /// value: CachedConnection
    ///
    /// Note: Use sync (parking_log) mutex because it is always held
    /// for a very short period of time, and any actual connection (and
    /// waiting) is done in CachedConnection
    connections: parking_lot::Mutex<HashMap<String, CachedConnection>>,

    /// Name of the http header that will contain the tracing context value.
    trace_context_header_name: String,
}

impl FlightClientImpl {
    /// Create new client.
    pub fn new(trace_context_header_name: &str) -> Self {
        Self {
            trace_context_header_name: trace_context_header_name.to_string(),
            ..Default::default()
        }
    }

    /// Establish connection to given addr and perform handshake.
    async fn connect(&self, ingester_address: Arc<str>) -> Result<Connection, Error> {
        let cached_connection = {
            let mut connections = self.connections.lock();
            if let Some(cached_connection) = connections.get(ingester_address.as_ref()) {
                cached_connection.clone()
            } else {
                // need to make a new one;
                let cached_connection = CachedConnection::new(&ingester_address);
                connections.insert(ingester_address.to_string(), cached_connection.clone());
                cached_connection
            }
        };
        cached_connection.connect().await
    }
}

#[async_trait]
impl IngesterFlightClient for FlightClientImpl {
    async fn invalidate_connection(&self, ingester_address: Arc<str>) {
        let maybe_conn = self.connections.lock().remove(ingester_address.as_ref());

        if let Some(conn) = maybe_conn {
            conn.close().await;
        }
    }

    async fn query(
        &self,
        ingester_addr: Arc<str>,
        request: IngesterQueryRequest,
        span_context: Option<SpanContext>,
    ) -> Result<Box<dyn QueryData>, Error> {
        let span = span_context.map(|s| s.child("ingester flight client impl"));
        let span_recorder = SpanRecorder::new(span.clone());

        let connection = {
            let _span_recorder = span_recorder.child("connect");

            self.connect(Arc::clone(&ingester_addr)).await?
        };

        debug!(%ingester_addr, ?request, "Sending request to ingester");
        let ticket = {
            let _span_recorder = span_recorder.child("serialize request");

            let request = serialize_ingester_query_request(request)?.encode_to_vec();

            Ticket {
                ticket: request.into(),
            }
        };

        let mut client = influxdb_iox_client::flight::Client::new(connection)
            // use lower level client to send a custom message type
            .into_inner();

        // Add the span context header, if any
        let span_recorder_comm = span_recorder.child("comm");
        if let Some(span) = span_recorder_comm.span() {
            client
                .add_header(
                    &self.trace_context_header_name,
                    &format_jaeger_trace_context(&span.ctx),
                )
                // wrap in client error type
                .map_err(FlightError::ArrowFlightError)
                .context(FlightSnafu)?;
        }

        let data_stream = {
            let _span_recorder = span_recorder_comm.child("initial request");

            client
                .do_get(ticket)
                .await
                // wrap in client error type
                .map_err(FlightError::ArrowFlightError)
                .context(FlightSnafu)?
                .into_inner()
        };

        Ok(Box::new(QueryDataTracer {
            inner: data_stream,
            recorder: span_recorder_comm.child("stream"),
        }))
    }
}

/// Tries to serialize the request to the ingester
///
/// Note if the predicate is too "complicated" to be serialized simply
/// ask for all the data from the ingester. More details:
/// <https://github.com/apache/arrow-datafusion/issues/3968>
fn serialize_ingester_query_request(
    mut request: IngesterQueryRequest,
) -> Result<proto::IngesterQueryRequest, Error> {
    match request.clone().try_into() {
        Ok(proto) => Ok(proto),
        Err(e) => {
            match SerializeFailureReason::extract_from_description(&e.field, &e.description) {
                Some(reason) => {
                    warn!(
                        predicate=?request.predicate,
                        reason=?reason,
                        "Cannot serialize predicate, stripping it",
                    );
                    request.predicate = None;
                    request.try_into().context(CreatingRequestSnafu)
                }
                None => Err(Error::CreatingRequest { source: e }),
            }
        }
    }
}

#[derive(Debug)]
enum SerializeFailureReason {
    RecursionLimit,
    NotSupported,
}

impl SerializeFailureReason {
    fn extract_from_description(field: &str, description: &str) -> Option<Self> {
        if field != "exprs" {
            return None;
        }

        if description.contains("recursion limit reached") {
            Some(Self::RecursionLimit)
        } else if description.contains("not supported") {
            Some(Self::NotSupported)
        } else {
            None
        }
    }
}

/// Data that is returned by an ingester gRPC query.
///
/// This is mostly the same as [`FlightDataDecoder`] but allows mocking in tests
#[async_trait]
pub trait QueryData: Debug + Send + 'static {
    /// Returns the next [`DecodedPayload`] available for this query, or `None` if
    /// there are no further results available.
    async fn next_message(
        &mut self,
    ) -> Result<Option<(DecodedPayload, proto::IngesterQueryResponseMetadata)>, FlightError>;
}

#[async_trait]
impl<T> QueryData for Box<T>
where
    T: QueryData + ?Sized,
{
    async fn next_message(
        &mut self,
    ) -> Result<Option<(DecodedPayload, proto::IngesterQueryResponseMetadata)>, FlightError> {
        self.deref_mut().next_message().await
    }
}

#[async_trait]
// Extracts the ingester metadata from the streaming FlightData
impl QueryData for FlightDataDecoder {
    async fn next_message(
        &mut self,
    ) -> Result<Option<(DecodedPayload, proto::IngesterQueryResponseMetadata)>, FlightError> {
        let decoded_data = self.next().await.transpose()?;

        Ok(decoded_data
            .map(|decoded_data| {
                let DecodedFlightData { inner, payload } = decoded_data;

                // extract the metadata from the underlying FlightData structure
                let app_metadata = &inner.app_metadata[..];
                let app_metadata: proto::IngesterQueryResponseMetadata =
                    Message::decode(app_metadata)?;

                Ok((payload, app_metadata)) as Result<_, FlightError>
            })
            .transpose()?)
    }
}

/// Wraps [`QueryData`] so that all calls to [`QueryData::next_message`] have proper tracing spans.
#[derive(Debug)]
struct QueryDataTracer<T>
where
    T: QueryData,
{
    inner: T,
    recorder: SpanRecorder,
}

#[async_trait]
impl<T> QueryData for QueryDataTracer<T>
where
    T: QueryData,
{
    async fn next_message(
        &mut self,
    ) -> Result<Option<(DecodedPayload, proto::IngesterQueryResponseMetadata)>, FlightError> {
        let mut span_recorder = self.recorder.child("next message");
        let res = self.inner.next_message().await;

        match &res {
            Ok(res) => {
                span_recorder.ok("ok");
                self.record_metadata(&mut span_recorder, res.as_ref())
            }
            Err(e) => span_recorder.error(e.to_string()),
        }

        res
    }
}

impl<T> QueryDataTracer<T>
where
    T: QueryData,
{
    /// Record additional metadata on the
    fn record_metadata(
        &self,
        span_recorder: &mut SpanRecorder,
        res: Option<&(DecodedPayload, proto::IngesterQueryResponseMetadata)>,
    ) {
        let Some((payload, _metadata)) = res else {
            return;
        };
        match payload {
            DecodedPayload::None => {
                span_recorder.set_metadata("payload_type", "none");
            }
            DecodedPayload::Schema(_) => {
                span_recorder.set_metadata("payload_type", "schema");
            }
            DecodedPayload::RecordBatch(batch) => {
                span_recorder.set_metadata("payload_type", "batch");
                span_recorder.set_metadata("num_rows", batch.num_rows() as i64);
                span_recorder.set_metadata("mem_bytes", batch.get_array_memory_size() as i64);
            }
        }
    }
}

#[derive(Debug, Clone)]
struct CachedConnection {
    ingester_address: Arc<str>,
    /// Real async mutex to
    maybe_connection: Arc<tokio::sync::Mutex<Option<Connection>>>,
}

impl CachedConnection {
    fn new(ingester_address: &Arc<str>) -> Self {
        Self {
            ingester_address: Arc::clone(ingester_address),
            maybe_connection: Arc::new(tokio::sync::Mutex::new(None)),
        }
    }

    /// Return the underlying connection, creating it if needed
    async fn connect(&self) -> Result<Connection, Error> {
        let mut maybe_connection = self.maybe_connection.lock().await;

        let ingester_address = self.ingester_address.as_ref();

        if let Some(connection) = maybe_connection.as_ref() {
            debug!(%ingester_address, "Reusing connection to ingester");

            Ok(connection.clone())
        } else {
            debug!(%ingester_address, "Connecting to ingester");

            let connection = connection::Builder::new()
                .build(ingester_address)
                .await
                .context(ConnectingSnafu { ingester_address })?;

            // sanity check w/ a handshake
            let mut client = influxdb_iox_client::flight::Client::new(connection.clone());

            // make contact with the ingester
            client
                .handshake()
                .await
                .context(HandshakeSnafu { ingester_address })?;

            *maybe_connection = Some(connection.clone());
            Ok(connection)
        }
    }

    /// Close connection
    async fn close(&self) {
        let mut maybe_connection = self.maybe_connection.lock().await;

        // dropping the channel seems to be enough to close it.
        maybe_connection.take();
    }
}

#[cfg(test)]
mod tests {
    use data_types::{NamespaceId, TableId};
    use datafusion::{
        logical_expr::LogicalPlanBuilder,
        prelude::{col, exists, lit, when, Expr},
    };
    use predicate::Predicate;

    use super::*;

    #[test]
    fn serialize_deeply_nested_and() {
        // we need more stack space so this doesn't overflow in dev builds
        std::thread::Builder::new()
            .stack_size(10_000_000)
            .spawn(|| {
                let n = 100;
                println!("testing: {n}");

                // build a deeply nested (a < 5) AND (a < 5) AND .... tree
                let expr_base = col("a").lt(lit(5i32));
                let expr = (0..n).fold(expr_base.clone(), |expr, _| expr.and(expr_base.clone()));

                let (request, request2) = serialize_roundtrip(expr);
                assert_eq!(request, request2);
            })
            .expect("spawning thread")
            .join()
            .expect("joining thread");
    }

    #[test]
    fn serialize_deeply_nested_predicate() {
        // see https://github.com/influxdata/influxdb_iox/issues/5974

        // we need more stack space so this doesn't overflow in dev builds
        std::thread::Builder::new().stack_size(10_000_000).spawn(|| {
            // don't know what "too much" is, so let's slowly try to increase complexity
            let n_max = 100;

            for n in [1, 2, n_max] {
                println!("testing: {n}");


                // build a deeply recursive nested expression:
                //
                // CASE
                //  WHEN TRUE
                //  THEN (WHEN ...)
                // ELSE FALSE
                //
                let expr = (0..n).fold(lit(false), |expr, _|{
                    when(lit(true), expr)
                        .end()
                        .unwrap()
                });

                let (request1, request2) = serialize_roundtrip(expr);

                // expect that the self preservation mechanism has
                // kicked in and the predicate has been ignored.
                if request2.predicate.is_none() {
                    assert!(n > 2, "not really deeply nested");
                    return;
                } else {
                    assert_eq!(request1, request2);
                }
            }

            panic!("did not find a 'too deeply nested' expression, tested up to a depth of {n_max}")
        }).expect("spawning thread").join().expect("joining thread");
    }

    #[test]
    fn serialize_predicate_that_is_unsupported() {
        // See https://github.com/influxdata/influxdb_iox/issues/6195

        let subquery = Arc::new(LogicalPlanBuilder::empty(true).build().unwrap());
        let expr = exists(subquery);

        let (_request1, request2) = serialize_roundtrip(expr);
        assert!(request2.predicate.is_none());
    }

    /// Creates a [`IngesterQueryRequest`] and round trips it through
    /// serialization, returning both the original and the serialized
    /// request
    fn serialize_roundtrip(expr: Expr) -> (IngesterQueryRequest, IngesterQueryRequest) {
        let predicate = Predicate {
            exprs: vec![expr],
            ..Default::default()
        };

        let request = IngesterQueryRequest {
            namespace_id: NamespaceId::new(42),
            table_id: TableId::new(1337),
            columns: vec![String::from("col1"), String::from("col2")],
            predicate: Some(predicate),
        };

        let proto = serialize_ingester_query_request(request.clone()).expect("serialization");
        let request2 = IngesterQueryRequest::try_from(proto).expect("deserialization");
        (request, request2)
    }
}
