use ::generated_types::influxdata::iox::Target;
use client_util::connection::GrpcConnection;

use self::generated_types::{namespace_service_client::NamespaceServiceClient, *};
use crate::connection::Connection;
use crate::error::Error;
use ::generated_types::google::OptionalField;

/// Re-export generated_types
pub mod generated_types {
    pub use generated_types::influxdata::iox::{
        common::v1::SoftDeleted,
        namespace::v1::{update_namespace_service_protection_limit_request::LimitUpdate, *},
        partition_template::v1::{template_part::*, *},
    };
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

    /// Get the list of active namespaces
    pub async fn get_namespaces(&mut self) -> Result<Vec<Namespace>, Error> {
        let response = self
            .inner
            .get_namespaces(GetNamespacesRequest {
                // we need to specify the field to fully initialize the struct, but it is
                // deprecated.
                #[expect(deprecated)]
                deleted: None,
                filters: Some(NamespaceStatusFilterList {
                    inner: vec![i32::from(NamespaceStatusFilter::Active)],
                }),
            })
            .await?;

        Ok(response.into_inner().namespaces)
    }

    /// Create a namespace
    ///
    /// `retention_period_ns` is the the retention period in nanoseconds,
    /// measured from `now()`. `None` represents infinite retention (i.e. never
    /// drop data), and 0 is also mapped to `None` on the server side.
    ///
    /// Negative retention periods are rejected, returning an error.
    pub async fn create_namespace(
        &mut self,
        namespace: &str,
        retention_period_ns: Option<i64>,
        service_protection_limits: Option<ServiceProtectionLimits>,
        partition_template: Option<PartitionTemplate>,
    ) -> Result<Namespace, Error> {
        let response = self
            .inner
            .create_namespace(CreateNamespaceRequest {
                name: namespace.to_string(),
                retention_period_ns,
                partition_template,
                service_protection_limits,
            })
            .await?;

        Ok(response.into_inner().namespace.unwrap_field("namespace")?)
    }

    /// Update retention for a namespace
    ///
    /// `retention_period_ns` is the the retention period in nanoseconds,
    /// measured from `now()`. `None` represents infinite retention (i.e. never
    /// drop data), and 0 is also mapped to `None` on the server side.
    ///
    /// Negative retention periods are rejected, returning an error.
    pub async fn update_namespace_retention(
        &mut self,
        namespace: impl Into<Target> + Send,
        retention_period_ns: Option<i64>,
    ) -> Result<Namespace, Error> {
        let response = self
            .inner
            .update_namespace_retention(UpdateNamespaceRetentionRequest {
                target: namespace.into().into(),
                retention_period_ns,
            })
            .await?;

        Ok(response.into_inner().namespace.unwrap_field("namespace")?)
    }

    /// Update one of the service protection limits for a namespace
    ///
    /// `limit_update` is the new service limit protection limit to set
    /// on the namespace.
    ///
    /// Zero-valued limits are rejected, returning an error.
    pub async fn update_namespace_service_protection_limit(
        &mut self,
        namespace: impl Into<Target> + Send,
        limit_update: LimitUpdate,
    ) -> Result<Namespace, Error> {
        let response = self
            .inner
            .update_namespace_service_protection_limit(
                UpdateNamespaceServiceProtectionLimitRequest {
                    target: namespace.into().into(),
                    limit_update: Some(limit_update),
                },
            )
            .await?;

        Ok(response.into_inner().namespace.unwrap_field("namespace")?)
    }

    /// Delete a namespace
    pub async fn delete_namespace(
        &mut self,
        namespace: impl Into<Target> + Send,
    ) -> Result<(), Error> {
        self.inner
            .delete_namespace(DeleteNamespaceRequest {
                target: namespace.into().into(),
            })
            .await?;

        Ok(())
    }

    /// Restore a soft-deleted namespace
    pub async fn undelete_namespace(
        &mut self,
        namespace_id: impl Into<i64> + Send,
    ) -> Result<Namespace, Error> {
        let response = self
            .inner
            .undelete_namespace(UndeleteNamespaceRequest {
                id: namespace_id.into(),
            })
            .await?;
        Ok(response.into_inner().namespace.unwrap_field("namespace")?)
    }

    /// Rename a namespace
    pub async fn update_namespace_name(
        &mut self,
        namespace_id: impl Into<i64> + Send,
        new_name: impl Into<String> + Send,
    ) -> Result<Namespace, Error> {
        let response = self
            .inner
            .update_namespace_name(UpdateNamespaceNameRequest {
                id: namespace_id.into(),
                new_name: new_name.into(),
            })
            .await?;
        Ok(response.into_inner().namespace.unwrap_field("namespace")?)
    }
}
