use self::generated_types::{compaction_service_client::CompactionServiceClient, *};
use crate::{connection::Connection, error::Error};
use client_util::connection::GrpcConnection;

/// Re-export generated_types
pub mod generated_types {
    pub use generated_types::influxdata::iox::compactor::v1::*;
    pub use generated_types::influxdata::iox::skipped_compaction::v1::*;
}

/// A basic client for interacting with the compaction service.
#[derive(Debug, Clone)]
pub struct Client {
    inner: CompactionServiceClient<GrpcConnection>,
}

impl Client {
    /// Creates a new client with the provided connection
    pub fn new(connection: Connection) -> Self {
        Self {
            inner: CompactionServiceClient::new(connection.into_grpc_connection()),
        }
    }

    /// List all skipped compactions
    pub async fn skipped_compactions(
        &mut self,
    ) -> Result<Vec<generated_types::SkippedCompaction>, Error> {
        let response = self
            .inner
            .list_skipped_compactions(ListSkippedCompactionsRequest {})
            .await?;

        Ok(response.into_inner().skipped_compactions)
    }

    /// Delete the requested skipped compaction
    pub async fn delete_skipped_compactions(
        &mut self,
        partition_id: i64,
    ) -> Result<Option<generated_types::SkippedCompaction>, Error> {
        let response = self
            .inner
            .delete_skipped_compactions(DeleteSkippedCompactionsRequest { partition_id })
            .await?;

        Ok(response.into_inner().skipped_compaction)
    }
}
