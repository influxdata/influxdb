use async_trait::async_trait;
use client_util::connection::{self, Connection};
use generated_types::ingester::IngesterQueryRequest;
use influxdb_iox_client::flight::{
    generated_types as proto,
    low_level::{Client as LowLevelFlightClient, LowLevelMessage, PerformQuery},
};
use observability_deps::tracing::debug;
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
        let request: proto::IngesterQueryRequest =
            request.try_into().context(CreatingRequestSnafu)?;

        let perform_query = client.perform_query(request).await.context(FlightSnafu)?;
        Ok(Box::new(perform_query))
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
