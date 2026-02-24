use self::generated_types::{object_store_service_client::ObjectStoreServiceClient, *};

use crate::connection::Connection;
use crate::error::Error;

pub use ::generated_types::Status;
use client_util::connection::GrpcConnection;
use futures_util::stream::BoxStream;

/// Re-export generated_types
pub mod generated_types {
    pub use generated_types::influxdata::iox::object_store::v1::*;
}

/// A basic client for interacting the a remote catalog.
#[derive(Debug, Clone)]
pub struct Client {
    inner: ObjectStoreServiceClient<GrpcConnection>,
}

impl Client {
    /// Creates a new client with the provided connection
    pub fn new(connection: Connection) -> Self {
        Self {
            inner: ObjectStoreServiceClient::new(connection.into_grpc_connection()),
        }
    }

    /// Get the parquet file data by its object store uuid
    pub async fn get_parquet_file_by_object_store_id(
        &mut self,
        uuid: String,
    ) -> Result<BoxStream<'static, Result<GetParquetFileByObjectStoreIdResponse, Status>>, Error>
    {
        let response = self
            .inner
            .get_parquet_file_by_object_store_id(GetParquetFileByObjectStoreIdRequest { uuid })
            .await?;

        Ok(Box::pin(response.into_inner()))
    }

    /// Get the parquet file data by its object store path, including different versions.
    ///
    /// Lookup by object_store_id only requires catalog data in order to build the path. Instead,
    /// provide the ability to request versioned objects directly.
    pub async fn get_parquet_file_by_object_store_path(
        &mut self,
        path: String,
        version: Option<String>,
    ) -> Result<BoxStream<'static, Result<GetParquetFileByObjectStorePathResponse, Status>>, Error>
    {
        let response = self
            .inner
            .get_parquet_file_by_object_store_path(GetParquetFileByObjectStorePathRequest {
                path,
                version,
            })
            .await?;

        Ok(Box::pin(response.into_inner()))
    }

    /// List the parquet files located with the path prefix.
    /// Search is recursive (e.g. searching `prefix/` includes `prefix/<other/paths>/uuid.parquet`.
    ///
    /// Returns only the current version of a file, not any other (e.g. deleted) versions
    pub async fn list_parquet_files_by_path_filter(
        &mut self,
        prefix: ParquetFilePathFilter,
    ) -> Result<BoxStream<'static, Result<ListParquetFilesByPathFilterResponse, Status>>, Error>
    {
        let response = self
            .inner
            .list_parquet_files_by_path_filter(ListParquetFilesByPathFilterRequest {
                prefix: Some(prefix),
            })
            .await?;

        Ok(Box::pin(response.into_inner()))
    }
}
