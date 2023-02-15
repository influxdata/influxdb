//! Implementation of the namespace gRPC service

use std::sync::Arc;

use data_types::{Namespace as CatalogNamespace, QueryPoolId, TopicId};
use generated_types::influxdata::iox::namespace::v1::*;
use iox_catalog::interface::{Catalog, SoftDeletedRows};
use observability_deps::tracing::{debug, info, warn};
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

        let namespaces = repos
            .namespaces()
            .list(SoftDeletedRows::ExcludeDeleted)
            .await
            .map_err(|e| {
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

        let CreateNamespaceRequest {
            name: namespace_name,
            retention_period_ns,
        } = request.into_inner();

        let retention_period_ns = map_retention_period(retention_period_ns)?;

        debug!(%namespace_name, ?retention_period_ns, "Creating namespace");

        let namespace = repos
            .namespaces()
            .create(
                &namespace_name,
                retention_period_ns,
                self.topic_id.unwrap(),
                self.query_id.unwrap(),
            )
            .await
            .map_err(|e| {
                warn!(error=%e, %namespace_name, "failed to create namespace");
                Status::internal(e.to_string())
            })?;

        info!(
            namespace_name,
            namespace_id = %namespace.id,
            "created namespace"
        );

        Ok(Response::new(create_namespace_to_proto(namespace)))
    }

    async fn delete_namespace(
        &self,
        request: Request<DeleteNamespaceRequest>,
    ) -> Result<Response<DeleteNamespaceResponse>, Status> {
        let namespace_name = request.into_inner().name;

        self.catalog
            .repositories()
            .await
            .namespaces()
            .soft_delete(&namespace_name)
            .await
            .map_err(|e| {
                warn!(error=%e, %namespace_name, "failed to soft-delete namespace");
                Status::internal(e.to_string())
            })?;

        info!(namespace_name, "soft-deleted namespace");

        Ok(Response::new(Default::default()))
    }

    async fn update_namespace_retention(
        &self,
        request: Request<UpdateNamespaceRetentionRequest>,
    ) -> Result<Response<UpdateNamespaceRetentionResponse>, Status> {
        let mut repos = self.catalog.repositories().await;

        let UpdateNamespaceRetentionRequest {
            name: namespace_name,
            retention_period_ns,
        } = request.into_inner();

        let retention_period_ns = map_retention_period(retention_period_ns)?;

        debug!(%namespace_name, ?retention_period_ns, "Updating namespace retention");

        let namespace = repos
            .namespaces()
            .update_retention_period(&namespace_name, retention_period_ns)
            .await
            .map_err(|e| {
                warn!(error=%e, %namespace_name, "failed to update namespace retention");
                Status::not_found(e.to_string())
            })?;

        info!(
            namespace_name,
            retention_period_ns,
            namespace_id = %namespace.id,
            "updated namespace retention"
        );

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

/// Map a user-submitted retention period value to the correct internal
/// encoding.
///
/// 0 is always mapped to [`None`], indicating infinite retention.
///
/// Negative retention periods are rejected with an error.
fn map_retention_period(v: Option<i64>) -> Result<Option<i64>, Status> {
    match v {
        Some(0) => Ok(None),
        Some(v @ 1..) => Ok(Some(v)),
        Some(_v @ ..=0) => Err(Status::invalid_argument(
            "invalid negative retention period",
        )),
        None => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use assert_matches::assert_matches;
    use generated_types::influxdata::iox::namespace::v1::namespace_service_server::NamespaceService as _;
    use iox_catalog::mem::MemCatalog;
    use tonic::Code;

    use super::*;

    const RETENTION: i64 = Duration::from_secs(42 * 60 * 60).as_nanos() as _;
    const NS_NAME: &str = "bananas";

    #[test]
    fn test_retention_mapping() {
        assert_matches!(map_retention_period(None), Ok(None));
        assert_matches!(map_retention_period(Some(0)), Ok(None));
        assert_matches!(map_retention_period(Some(1)), Ok(Some(1)));
        assert_matches!(map_retention_period(Some(42)), Ok(Some(42)));
        assert_matches!(map_retention_period(Some(-1)), Err(e) => {
            assert_eq!(e.code(), Code::InvalidArgument)
        });
        assert_matches!(map_retention_period(Some(-42)), Err(e) => {
            assert_eq!(e.code(), Code::InvalidArgument)
        });
    }

    #[tokio::test]
    async fn test_crud() {
        let catalog: Arc<dyn Catalog> =
            Arc::new(MemCatalog::new(Arc::new(metric::Registry::default())));

        let topic = catalog
            .repositories()
            .await
            .topics()
            .create_or_get("kafka-topic")
            .await
            .unwrap();
        let query_pool = catalog
            .repositories()
            .await
            .query_pools()
            .create_or_get("query-pool")
            .await
            .unwrap();

        let handler = NamespaceService::new(catalog, Some(topic.id), Some(query_pool.id));

        // There should be no namespaces to start with.
        {
            let current = handler
                .get_namespaces(Request::new(Default::default()))
                .await
                .expect("must return namespaces")
                .into_inner()
                .namespaces;
            assert!(current.is_empty());
        }

        let req = CreateNamespaceRequest {
            name: NS_NAME.to_string(),
            retention_period_ns: Some(RETENTION),
        };
        let created_ns = handler
            .create_namespace(Request::new(req))
            .await
            .expect("failed to create namespace")
            .into_inner()
            .namespace
            .expect("no namespace in response");
        assert_eq!(created_ns.name, NS_NAME);
        assert_eq!(created_ns.retention_period_ns, Some(RETENTION));

        // There should now be one namespace
        {
            let current = handler
                .get_namespaces(Request::new(Default::default()))
                .await
                .expect("must return namespaces")
                .into_inner()
                .namespaces;
            assert_matches!(current.as_slice(), [ns] => {
                assert_eq!(ns, &created_ns);
            })
        }

        // Update the retention period
        let updated_ns = handler
            .update_namespace_retention(Request::new(UpdateNamespaceRetentionRequest {
                name: NS_NAME.to_string(),
                retention_period_ns: Some(0), // A zero!
            }))
            .await
            .expect("failed to update namespace")
            .into_inner()
            .namespace
            .expect("no namespace in response");
        assert_eq!(updated_ns.name, created_ns.name);
        assert_eq!(updated_ns.id, created_ns.id);
        assert_eq!(created_ns.retention_period_ns, Some(RETENTION));
        assert_eq!(updated_ns.retention_period_ns, None);

        // Listing the namespaces should return the updated namespace
        {
            let current = handler
                .get_namespaces(Request::new(Default::default()))
                .await
                .expect("must return namespaces")
                .into_inner()
                .namespaces;
            assert_matches!(current.as_slice(), [ns] => {
                assert_eq!(ns, &updated_ns);
            })
        }

        // Deleting the namespace should cause it to disappear
        handler
            .delete_namespace(Request::new(DeleteNamespaceRequest {
                name: NS_NAME.to_string(),
            }))
            .await
            .expect("must delete");

        // Listing the namespaces should now return nothing.
        {
            let current = handler
                .get_namespaces(Request::new(Default::default()))
                .await
                .expect("must return namespaces")
                .into_inner()
                .namespaces;
            assert_matches!(current.as_slice(), []);
        }
    }
}
