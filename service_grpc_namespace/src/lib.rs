//! Implementation of the namespace gRPC service

use std::sync::Arc;

use data_types::{Namespace as CatalogNamespace, QueryPoolId, TopicId};
use generated_types::influxdata::iox::namespace::v1::*;
use iox_catalog::interface::Catalog;
use observability_deps::tracing::warn;
use tonic::{Request, Response, Status};

/// Implementation of the gRPC namespace service
#[derive(Debug)]
pub struct NamespaceService {
    /// Catalog.
    catalog: Arc<dyn Catalog>,
    topic_id: Option<TopicId>,
    query_id: Option<QueryPoolId>,
}

impl NamespaceService {
    pub fn new(
        catalog: Arc<dyn Catalog>,
        topic_id: Option<TopicId>,
        query_id: Option<QueryPoolId>,
    ) -> Self {
        Self {
            catalog,
            topic_id,
            query_id,
        }
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

    // create a namespace
    async fn create_namespace(
        &self,
        request: Request<CreateNamespaceRequest>,
    ) -> Result<Response<CreateNamespaceResponse>, Status> {
        if self.topic_id.is_none() || self.query_id.is_none() {
            return Err(Status::invalid_argument("topic_id or query_id not set"));
        }

        let mut repos = self.catalog.repositories().await;

        let req = request.into_inner();
        let namespace = repos
            .namespaces()
            .create(
                &req.name,
                req.retention_period_ns,
                self.topic_id.unwrap(),
                self.query_id.unwrap(),
            )
            .await
            .map_err(|e| {
                warn!(error=%e, %req.name, "failed to create namespace");
                Status::internal(e.to_string())
            })?;

        Ok(Response::new(create_namespace_to_proto(namespace)))
    }

    async fn update_namespace_retention(
        &self,
        request: Request<UpdateNamespaceRetentionRequest>,
    ) -> Result<Response<UpdateNamespaceRetentionResponse>, Status> {
        let mut repos = self.catalog.repositories().await;

        let req = request.into_inner();
        let namespace = repos
            .namespaces()
            .update_retention_period(&req.name, req.retention_period_ns)
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

fn create_namespace_to_proto(namespace: CatalogNamespace) -> CreateNamespaceResponse {
    CreateNamespaceResponse {
        namespace: Some(Namespace {
            id: namespace.id.get(),
            name: namespace.name.clone(),
            retention_period_ns: namespace.retention_period_ns,
        }),
    }
}
