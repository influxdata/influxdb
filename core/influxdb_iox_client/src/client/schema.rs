use self::generated_types::{schema_service_client::SchemaServiceClient, *};
use ::generated_types::google::OptionalField;
use ::generated_types::influxdata::iox::Target;
use client_util::connection::GrpcConnection;

use crate::connection::Connection;
use crate::error::Error;

/// Re-export generated_types
pub mod generated_types {
    pub use generated_types::influxdata::iox::{column_type::v1::ColumnType, schema::v1::*};
}

/// A basic client for fetching the Schema for a Namespace.
#[derive(Debug, Clone)]
pub struct Client {
    inner: SchemaServiceClient<GrpcConnection>,
}

impl Client {
    /// Creates a new client with the provided connection
    pub fn new(connection: Connection) -> Self {
        Self {
            inner: SchemaServiceClient::new(connection.into_grpc_connection()),
        }
    }

    /// Get the schema for a namespace and, optionally, one table within that namespace.
    pub async fn get_schema<N, T>(
        &mut self,
        namespace: N,
        table: Option<T>,
    ) -> Result<NamespaceSchema, Error>
    where
        Target: From<N>,
        Target: From<T>,
    {
        let response = self
            .inner
            .get_schema(GetSchemaRequest {
                namespace_target: Target::from(namespace).into(),
                table_target: table.map(|t| Target::from(t).into()),
            })
            .await?;

        Ok(response.into_inner().schema.unwrap_field("schema")?)
    }

    /// Upsert the schema for a namespace and table. Returns the schema for the specified table
    /// after applying the upsert.
    pub async fn upsert_schema(
        &mut self,
        namespace: impl Into<Target> + Send,
        table: impl Into<Target> + Send,
        columns: impl Iterator<Item = (&str, ColumnType)> + Send,
    ) -> Result<NamespaceSchema, Error> {
        let columns = columns
            .map(|(name, column_type)| (name.to_string(), column_type as i32))
            .collect();

        let response = self
            .inner
            .upsert_schema(UpsertSchemaRequest {
                namespace_target: namespace.into().into(),
                table_target: table.into().into(),
                columns,
            })
            .await?;

        Ok(response.into_inner().schema.unwrap_field("schema")?)
    }
}
