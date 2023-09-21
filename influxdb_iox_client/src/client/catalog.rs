use client_util::connection::GrpcConnection;

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
    inner: CatalogServiceClient<GrpcConnection>,
}

impl Client {
    /// Creates a new client with the provided connection
    pub fn new(connection: Connection) -> Self {
        Self {
            inner: CatalogServiceClient::new(connection.into_grpc_connection()),
        }
    }

    /// Get the Parquet file records by their partition id
    pub async fn get_parquet_files_by_partition_id(
        &mut self,
        partition_id: i64,
    ) -> Result<Vec<ParquetFile>, Error> {
        let partition_identifier = PartitionIdentifier {
            id: Some(partition_identifier::Id::CatalogId(partition_id)),
        };

        let response = self
            .inner
            .get_parquet_files_by_partition_id(GetParquetFilesByPartitionIdRequest {
                partition_identifier: Some(partition_identifier),
            })
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

    /// Get the Parquet file records by their namespace and table names
    pub async fn get_parquet_files_by_namespace_table(
        &mut self,
        namespace_name: impl Into<String> + Send,
        table_name: impl Into<String> + Send,
    ) -> Result<Vec<ParquetFile>, Error> {
        let namespace_name = namespace_name.into();
        let table_name = table_name.into();
        let response = self
            .inner
            .get_parquet_files_by_namespace_table(GetParquetFilesByNamespaceTableRequest {
                namespace_name,
                table_name,
            })
            .await?;

        Ok(response.into_inner().parquet_files)
    }

    /// Get the Parquet file records by their namespace
    pub async fn get_parquet_files_by_namespace(
        &mut self,
        namespace_name: impl Into<String> + Send,
    ) -> Result<Vec<ParquetFile>, Error> {
        let namespace_name = namespace_name.into();
        let response = self
            .inner
            .get_parquet_files_by_namespace(GetParquetFilesByNamespaceRequest { namespace_name })
            .await?;

        Ok(response.into_inner().parquet_files)
    }
}
