use self::generated_types::{catalog_service_client::CatalogServiceClient, *};

use crate::connection::Connection;
use crate::error::Error;

/// Re-export generated_types
pub mod generated_types {
    pub use generated_types::influxdata::iox::catalog::v1::*;
}

/// A basic client for interacting the a remote catalog.
#[derive(Debug, Clone)]
pub struct Client {
    inner: CatalogServiceClient<Connection>,
}

impl Client {
    /// Creates a new client with the provided connection
    pub fn new(channel: Connection) -> Self {
        Self {
            inner: CatalogServiceClient::new(channel),
        }
    }

    /// Get the parquet file records by their partition id
    pub async fn get_parquet_files_by_partition_id(
        &mut self,
        partition_id: i64,
    ) -> Result<Vec<ParquetFile>, Error> {
        let response = self
            .inner
            .get_parquet_files_by_partition_id(GetParquetFilesByPartitionIdRequest { partition_id })
            .await?;

        Ok(response.into_inner().parquet_files)
    }

    /// Get the partitions by table id
    pub async fn get_partitions_by_table_id(
        &mut self,
        table_id: i64,
    ) -> Result<Vec<Partition>, Error> {
        let response = self
            .inner
            .get_partitions_by_table_id(GetPartitionsByTableIdRequest { table_id })
            .await?;

        Ok(response.into_inner().partitions)
    }
}
