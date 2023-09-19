use self::generated_types::{schema_service_client::SchemaServiceClient, *};
use ::generated_types::google::OptionalField;
use client_util::connection::GrpcConnection;

use crate::connection::Connection;
use crate::error::Error;

/// Re-export generated_types
pub mod generated_types {
    pub use generated_types::influxdata::iox::schema::v1::*;
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
    pub async fn get_schema(
        &mut self,
        namespace: &str,
        table: Option<&str>,
    ) -> Result<NamespaceSchema, Error> {
        let response = self
            .inner
            .get_schema(GetSchemaRequest {
                namespace: namespace.to_string(),
                table: table.map(ToString::to_string),
            })
            .await?;

        Ok(response.into_inner().schema.unwrap_field("schema")?)
    }
}
