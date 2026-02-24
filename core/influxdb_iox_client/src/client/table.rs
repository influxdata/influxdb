use client_util::connection::GrpcConnection;

use self::generated_types::{table_service_client::TableServiceClient, *};
use crate::connection::Connection;
use crate::error::Error;
use ::generated_types::google::OptionalField;
use ::generated_types::influxdata::iox::Target;

/// Re-export generated_types
pub mod generated_types {
    pub use generated_types::influxdata::iox::{
        common::v1::SoftDeleted,
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

    /// Fetch the list of tables in the given namespace
    pub async fn get_tables(
        &mut self,
        namespace: impl Into<Target> + Send,
        filters: Option<impl IntoIterator<Item = TableStatusFilter>>,
    ) -> Result<Vec<Table>, Error> {
        Ok(self
            .inner
            .get_tables(GetTablesRequest {
                target: Some(namespace.into().into()),
                filters: filters.map(|filters| TableStatusFilterList {
                    inner: filters.into_iter().map(|f| f as i32).collect(),
                }),
            })
            .await?
            .into_inner()
            .tables)
    }

    /// Get a table in the given namespace
    pub async fn get_table(
        &mut self,
        namespace: impl Into<Target> + Send,
        table: impl Into<Target> + Send,
    ) -> Result<Table, Error> {
        Ok(self
            .inner
            .get_table(GetTableRequest {
                namespace_target: Some(namespace.into().into()),
                table_target: Some(table.into().into()),
            })
            .await?
            .into_inner()
            .table
            .unwrap_field("table")?)
    }

    /// Create a table
    pub async fn create_table(
        &mut self,
        namespace: impl Into<Target> + Send,
        table: &str,
        partition_template: Option<PartitionTemplate>,
    ) -> Result<Table, Error> {
        let response = self
            .inner
            .create_table(CreateTableRequest {
                name: table.to_string(),
                namespace_target: namespace.into().into(),
                partition_template,
            })
            .await?;

        Ok(response.into_inner().table.unwrap_field("table")?)
    }

    /// Soft delete a table
    pub async fn soft_delete_table(
        &mut self,
        table_id: i64,
        namespace_id: i64,
    ) -> Result<Table, Error> {
        let response = self
            .inner
            .delete_table(DeleteTableRequest {
                table_id,
                namespace_id,
            })
            .await?;

        Ok(response.into_inner().table.unwrap_field("table")?)
    }

    /// Enable iceberg exports for a table
    pub async fn enable_iceberg(&mut self, table_id: i64, namespace_id: i64) -> Result<(), Error> {
        let _ = self
            .inner
            .enable_iceberg(EnableIcebergRequest {
                table_id,
                namespace_id,
            })
            .await?;
        Ok(())
    }

    /// Disable iceberg exports for a table
    pub async fn disable_iceberg(&mut self, table_id: i64, namespace_id: i64) -> Result<(), Error> {
        let _ = self
            .inner
            .disable_iceberg(DisableIcebergRequest {
                table_id,
                namespace_id,
            })
            .await?;
        Ok(())
    }
}
