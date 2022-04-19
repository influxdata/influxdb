use self::generated_types::{object_store_service_client::ObjectStoreServiceClient, *};

use crate::connection::Connection;
use crate::error::Error;

use futures_util::stream::BoxStream;
use tonic::Status;

/// Re-export generated_types
pub mod generated_types {
    pub use generated_types::influxdata::iox::object_store::v1::*;
}

/// A basic client for interacting the a remote catalog.
#[derive(Debug, Clone)]
pub struct Client {
    inner: ObjectStoreServiceClient<Connection>,
}

impl Client {
    /// Creates a new client with the provided connection
    pub fn new(channel: Connection) -> Self {
        Self {
            inner: ObjectStoreServiceClient::new(channel),
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
}
