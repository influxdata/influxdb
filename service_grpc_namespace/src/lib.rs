//! Implementation of the namespace gRPC service

use std::sync::Arc;

use data_types::Namespace as CatalogNamespace;
use generated_types::influxdata::iox::namespace::v1::*;
use iox_catalog::interface::Catalog;
use observability_deps::tracing::warn;
use tonic::{Request, Response, Status};

/// Implementation of the gRPC namespace service
#[derive(Debug)]
pub struct NamespaceService {
    /// Catalog.
    catalog: Arc<dyn Catalog>,
}

impl NamespaceService {
    pub fn new(catalog: Arc<dyn Catalog>) -> Self {
        Self { catalog }
    }
}

#[tonic::async_trait]
impl namespace_service_server::NamespaceService for NamespaceService {
    async fn get_namespaces(
        &self,
        _request: Request<GetNamespacesRequest>,
    ) -> Result<Response<GetNamespacesResponse>, Status> {
        let mut repos = self.catalog.repositories().await;

        let namespaces = repos.namespaces().list().await.map_err(|e| {
            warn!(error=%e, "failed to retrieve namespaces from catalog");
            Status::not_found(e.to_string())
        })?;
        Ok(Response::new(GetNamespacesResponse {
            namespaces: namespaces.into_iter().map(namespace_to_proto).collect(),
        }))
    }

    async fn update_namespace_retention(
        &self,
        request: Request<UpdateNamespaceRetentionRequest>,
    ) -> Result<Response<UpdateNamespaceRetentionResponse>, Status> {
        let mut repos = self.catalog.repositories().await;

        let req = request.into_inner();
        let namespace = repos
            .namespaces()
            .update_retention_period(&req.name, req.retention_hours)
            .await
            .map_err(|e| {
                warn!(error=%e, %req.name, "failed to update namespace retention");
                Status::not_found(e.to_string())
            })?;
        Ok(Response::new(UpdateNamespaceRetentionResponse {
            namespace: Some(namespace_to_proto(namespace)),
        }))
    }
}

fn namespace_to_proto(namespace: CatalogNamespace) -> Namespace {
    Namespace {
        id: namespace.id.get(),
        name: namespace.name.clone(),
        retention_period_ns: namespace.retention_period_ns,
    }
}
