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

    /// Instruct the ingester to persist its data to Parquet
    pub async fn persist(&mut self) -> Result<(), Error> {
        self.inner.persist(PersistRequest {}).await?;

        Ok(())
    }
}
