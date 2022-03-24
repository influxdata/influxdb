use self::generated_types::{schema_service_client::SchemaServiceClient, *};
use ::generated_types::google::OptionalField;

use crate::connection::Connection;
use crate::error::Error;

/// Re-export generated_types
pub mod generated_types {
    pub use generated_types::influxdata::iox::schema::v1::*;
}

/// A basic client for fetching the Schema for a Namespace.
#[derive(Debug, Clone)]
pub struct Client {
    inner: SchemaServiceClient<Connection>,
}

impl Client {
    /// Creates a new client with the provided connection
    pub fn new(channel: Connection) -> Self {
        Self {
            inner: SchemaServiceClient::new(channel),
        }
    }

    /// Get the schema for a namespace.
    pub async fn get_schema(&mut self, namespace: &str) -> Result<NamespaceSchema, Error> {
        let response = self
            .inner
            .get_schema(GetSchemaRequest {
                namespace: namespace.to_string(),
            })
            .await?;

        Ok(response.into_inner().schema.unwrap_field("schema")?)
    }
}
