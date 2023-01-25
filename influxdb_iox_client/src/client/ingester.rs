use self::generated_types::{persist_service_client::PersistServiceClient, *};
use crate::{connection::Connection, error::Error};
use client_util::connection::GrpcConnection;

/// Re-export generated_types
pub mod generated_types {
    pub use generated_types::influxdata::iox::ingester::v1::*;
}

/// A basic client for interacting with the ingester persist service.
#[derive(Debug, Clone)]
pub struct Client {
    inner: PersistServiceClient<GrpcConnection>,
}

impl Client {
    /// Creates a new client with the provided connection
    pub fn new(connection: Connection) -> Self {
        Self {
            inner: PersistServiceClient::new(connection.into_grpc_connection()),
        }
    }

    /// Instruct the ingester to persist its data for the specified namespace to Parquet. Useful in
    /// tests asserting on persisted data. May behave in unexpected ways if used concurrently with
    /// writes and ingester WAL rotations.
    pub async fn persist(&mut self, namespace: String) -> Result<(), Error> {
        self.inner.persist(PersistRequest { namespace }).await?;

        Ok(())
    }
}
