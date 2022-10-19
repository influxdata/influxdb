use self::generated_types::{skipped_compaction_service_client::SkippedCompactionServiceClient, *};
use crate::{connection::Connection, error::Error};
use client_util::connection::GrpcConnection;

/// Re-export generated_types
pub mod generated_types {
    pub use generated_types::influxdata::iox::compactor::v1::*;
}

/// A basic client for fetching the Schema for a Namespace.
#[derive(Debug, Clone)]
pub struct Client {
    inner: SkippedCompactionServiceClient<GrpcConnection>,
}

impl Client {
    /// Creates a new client with the provided connection
    pub fn new(connection: Connection) -> Self {
        Self {
            inner: SkippedCompactionServiceClient::new(connection.into_grpc_connection()),
        }
    }

    /// List all skipped compactions
    pub async fn skipped_compactions(&mut self) -> Result<Vec<SkippedCompaction>, Error> {
        let response = self
            .inner
            .list_skipped_compactions(ListSkippedCompactionsRequest {})
            .await?;

        Ok(response.into_inner().skipped_compactions)
    }
}
