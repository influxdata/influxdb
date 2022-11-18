use client_util::connection::GrpcConnection;

use self::generated_types::{namespace_service_client::NamespaceServiceClient, *};
use crate::connection::Connection;
use crate::error::Error;
use ::generated_types::google::OptionalField;

/// Re-export generated_types
pub mod generated_types {
    pub use generated_types::influxdata::iox::namespace::v1::*;
}

/// A basic client for working with Namespaces.
#[derive(Debug, Clone)]
pub struct Client {
    inner: NamespaceServiceClient<GrpcConnection>,
}

impl Client {
    /// Creates a new client with the provided connection
    pub fn new(connection: Connection) -> Self {
        Self {
            inner: NamespaceServiceClient::new(connection.into_grpc_connection()),
        }
    }

    /// Get the available namespaces
    pub async fn get_namespaces(&mut self) -> Result<Vec<Namespace>, Error> {
        let response = self.inner.get_namespaces(GetNamespacesRequest {}).await?;

        Ok(response.into_inner().namespaces)
    }

    /// Create a namespace
    pub async fn create_namespace(
        &mut self,
        namespace: &str,
        retention_period_ns: Option<i64>,
    ) -> Result<Namespace, Error> {
        let response = self
            .inner
            .create_namespace(CreateNamespaceRequest {
                name: namespace.to_string(),
                retention_period_ns,
            })
            .await?;

        Ok(response.into_inner().namespace.unwrap_field("namespace")?)
    }

    /// Update retention for a namespace
    pub async fn update_namespace_retention(
        &mut self,
        namespace: &str,
        retention_period_ns: Option<i64>,
    ) -> Result<Namespace, Error> {
        let response = self
            .inner
            .update_namespace_retention(UpdateNamespaceRetentionRequest {
                name: namespace.to_string(),
                retention_period_ns,
            })
            .await?;

        Ok(response.into_inner().namespace.unwrap_field("namespace")?)
    }
}
