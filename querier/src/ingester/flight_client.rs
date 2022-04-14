use std::fmt::Debug;

use async_trait::async_trait;
use client_util::connection;
use data_types2::IngesterQueryRequest;
use influxdb_iox_client::flight::{self, generated_types::IngesterQueryResponseMetadata};
use observability_deps::tracing::debug;
use snafu::{ResultExt, Snafu};

pub use flight::{Error as FlightError, PerformQuery};

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
        ingester_address: &str,
        request: IngesterQueryRequest,
    ) -> Result<PerformQuery<IngesterQueryResponseMetadata>, Error>;
}

/// Default [`FlightClient`] implemenetation that uses a real client.
#[derive(Debug, Default)]
pub struct FlightClientImpl {}

impl FlightClientImpl {
    /// Create new client.
    pub fn new() -> Self {
        Self::default()
    }

    /// Establish connection to given addr and perform handshake.
    async fn connect(
        &self,
        ingester_address: &str,
    ) -> Result<flight::Client<flight::generated_types::IngesterQueryRequest>, Error> {
        debug!(
            %ingester_address,
            "Connecting to ingester",
        );
        let connection = connection::Builder::new()
            .build(ingester_address)
            .await
            .context(ConnectingSnafu { ingester_address })?;
        let mut client =
            flight::Client::<flight::generated_types::IngesterQueryRequest>::new(connection);

        // make contact with the ingester
        client
            .handshake()
            .await
            .context(HandshakeSnafu { ingester_address })?;

        Ok(client)
    }
}

#[async_trait]
impl FlightClient for FlightClientImpl {
    async fn query(
        &self,
        ingester_addr: &str,
        request: IngesterQueryRequest,
    ) -> Result<PerformQuery<IngesterQueryResponseMetadata>, Error> {
        // TODO maybe cache this connection
        let mut client = self.connect(ingester_addr).await?;

        debug!(?request, "Sending request to ingester");
        let request: flight::generated_types::IngesterQueryRequest =
            request.try_into().context(CreatingRequestSnafu)?;

        client.perform_query(request).await.context(FlightSnafu)
    }
}
