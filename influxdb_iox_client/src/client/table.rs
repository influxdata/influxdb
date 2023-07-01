use client_util::connection::GrpcConnection;

use self::generated_types::{table_service_client::TableServiceClient, *};
use crate::connection::Connection;
use crate::error::Error;
use ::generated_types::google::OptionalField;

/// Re-export generated_types
pub mod generated_types {
    pub use generated_types::influxdata::iox::{
        partition_template::v1::{template_part::*, *},
        table::v1::*,
    };
}

/// A basic client for working with Tables.
#[derive(Debug, Clone)]
pub struct Client {
    inner: TableServiceClient<GrpcConnection>,
}

impl Client {
    /// Creates a new client with the provided connection
    pub fn new(connection: Connection) -> Self {
        Self {
            inner: TableServiceClient::new(connection.into_grpc_connection()),
        }
    }

    /// Create a table
    pub async fn create_table(
        &mut self,
        namespace: &str,
        table: &str,
        partition_template: Option<PartitionTemplate>,
    ) -> Result<Table, Error> {
        let response = self
            .inner
            .create_table(CreateTableRequest {
                name: table.to_string(),
                namespace: namespace.to_string(),
                partition_template,
            })
            .await?;

        Ok(response.into_inner().table.unwrap_field("table")?)
    }
}
