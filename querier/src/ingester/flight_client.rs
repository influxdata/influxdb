use async_trait::async_trait;
use client_util::connection::{self, Connection};
use generated_types::ingester::IngesterQueryRequest;
use influxdb_iox_client::flight::{
    generated_types as proto,
    low_level::{Client as LowLevelFlightClient, LowLevelMessage, PerformQuery},
};
use observability_deps::tracing::{debug, warn};
use snafu::{ResultExt, Snafu};
use std::{collections::HashMap, fmt::Debug, ops::DerefMut, sync::Arc};
use trace::ctx::SpanContext;

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
        source: influxdb_iox_client::google::FieldViolation,
    },

    #[snafu(display("Failed to perform flight request: {}", source))]
    Flight { source: FlightError },

    #[snafu(display("Can not contact ingester. Circuit broken: {}", ingester_address))]
    CircuitBroken { ingester_address: String },
}

/// Abstract Flight client.
///
/// May use an internal connection pool.
#[async_trait]
pub trait FlightClient: Debug + Send + Sync + 'static {
    /// Send query to given ingester.
    async fn query(
        &self,
        ingester_address: Arc<str>,
        request: IngesterQueryRequest,
        span_context: Option<SpanContext>,
    ) -> Result<Box<dyn QueryData>, Error>;
}

/// Default [`FlightClient`] implementation that uses a real connection
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
}

impl FlightClientImpl {
    /// Create new client.
    pub fn new() -> Self {
        Self::default()
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
impl FlightClient for FlightClientImpl {
    async fn query(
        &self,
        ingester_addr: Arc<str>,
        request: IngesterQueryRequest,
        span_context: Option<SpanContext>,
    ) -> Result<Box<dyn QueryData>, Error> {
        let connection = self.connect(Arc::clone(&ingester_addr)).await?;

        let mut client =
            LowLevelFlightClient::<proto::IngesterQueryRequest>::new(connection, span_context);

        debug!(%ingester_addr, ?request, "Sending request to ingester");
        let request = serialize_ingester_query_request(request)?;

        let perform_query = client.perform_query(request).await.context(FlightSnafu)?;
        Ok(Box::new(perform_query))
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
        Err(e) if (e.field == "exprs") && (e.description.contains("recursion limit reached")) => {
            warn!(
                predicate=?request.predicate,
                "Cannot serialize predicate due to recursion limit, stripping it",
            );
            request.predicate = None;
            request.try_into().context(CreatingRequestSnafu)
        }
        Err(e) => Err(Error::CreatingRequest { source: e }),
    }
}

/// Data that is returned by an ingester gRPC query.
///
/// This is mostly the same as [`PerformQuery`] but allows some easier mocking.
#[async_trait]
pub trait QueryData: Debug + Send + 'static {
    /// Returns the next [`LowLevelMessage`] available for this query, or `None` if
    /// there are no further results available.
    async fn next(
        &mut self,
    ) -> Result<Option<(LowLevelMessage, proto::IngesterQueryResponseMetadata)>, FlightError>;
}

#[async_trait]
impl<T> QueryData for Box<T>
where
    T: QueryData + ?Sized,
{
    async fn next(
        &mut self,
    ) -> Result<Option<(LowLevelMessage, proto::IngesterQueryResponseMetadata)>, FlightError> {
        self.deref_mut().next().await
    }
}

#[async_trait]
impl QueryData for PerformQuery<proto::IngesterQueryResponseMetadata> {
    async fn next(
        &mut self,
    ) -> Result<Option<(LowLevelMessage, proto::IngesterQueryResponseMetadata)>, FlightError> {
        self.next().await
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
            let mut client =
                LowLevelFlightClient::<proto::IngesterQueryRequest>::new(connection.clone(), None);

            // make contact with the ingester
            client
                .handshake()
                .await
                .context(HandshakeSnafu { ingester_address })?;

            *maybe_connection = Some(connection.clone());
            Ok(connection)
        }
    }
}

#[cfg(test)]
mod tests {
    use data_types::{NamespaceId, TableId};
    use datafusion::prelude::{col, lit, when, Expr};
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
