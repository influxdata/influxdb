//! NamespaceService gRPC implementation
//!
//! NOTE: this is present here in the querier to support a debug use-case that is handy in
//! production, namely `kubectl exec`ing into the querier pod and using the REPL. the namespace API
//! belongs in the router and has been moved there, but this is kept here in partial form to
//! support `show namespaces` in the REPL.

use data_types::Namespace;
use generated_types::influxdata::iox::namespace::v1 as proto;
use querier::QuerierDatabase;
use std::sync::Arc;

/// Acquire a [`NamespaceService`](proto::namespace_service_server::NamespaceService) gRPC service implementation.
pub fn namespace_service(
    server: Arc<QuerierDatabase>,
) -> proto::namespace_service_server::NamespaceServiceServer<
    impl proto::namespace_service_server::NamespaceService,
> {
    proto::namespace_service_server::NamespaceServiceServer::new(NamespaceServiceImpl::new(server))
}

#[derive(Debug)]
struct NamespaceServiceImpl {
    server: Arc<QuerierDatabase>,
}

impl NamespaceServiceImpl {
    pub fn new(server: Arc<QuerierDatabase>) -> Self {
        Self { server }
    }
}

/// Translate a catalog Namespace object to a protobuf form
fn namespace_to_proto(namespace: Namespace) -> proto::Namespace {
    proto::Namespace {
        id: namespace.id.get(),
        name: namespace.name,
        retention_period_ns: namespace.retention_period_ns,
    }
}

#[tonic::async_trait]
impl proto::namespace_service_server::NamespaceService for NamespaceServiceImpl {
    async fn get_namespaces(
        &self,
        _request: tonic::Request<proto::GetNamespacesRequest>,
    ) -> Result<tonic::Response<proto::GetNamespacesResponse>, tonic::Status> {
        // Get catalog namespaces
        let namespaces = self.server.namespaces().await;

        // convert to proto Namespaces
        let namespaces: Vec<_> = namespaces.into_iter().map(namespace_to_proto).collect();

        Ok(tonic::Response::new(proto::GetNamespacesResponse {
            namespaces,
        }))
    }

    async fn create_namespace(
        &self,
        _request: tonic::Request<proto::CreateNamespaceRequest>,
    ) -> Result<tonic::Response<proto::CreateNamespaceResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented(
            "use router instances to manage namespaces",
        ))
    }

    async fn update_namespace_retention(
        &self,
        _request: tonic::Request<proto::UpdateNamespaceRetentionRequest>,
    ) -> Result<tonic::Response<proto::UpdateNamespaceRetentionResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented(
            "use router instances to manage namespaces",
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use generated_types::influxdata::iox::namespace::v1::namespace_service_server::NamespaceService;
    use iox_tests::util::TestCatalog;
    use querier::{create_ingester_connection_for_testing, QuerierCatalogCache};
    use tokio::runtime::Handle;

    /// Common retention period value we'll use in tests
    const TEST_RETENTION_PERIOD_NS: Option<i64> = Some(3_600 * 1_000_000_000);

    #[tokio::test]
    async fn test_get_namespaces_empty() {
        let catalog = TestCatalog::new();

        // QuerierDatabase::new returns an error if there are no shards in the catalog
        catalog.create_shard(0).await;

        let catalog_cache = Arc::new(QuerierCatalogCache::new_testing(
            catalog.catalog(),
            catalog.time_provider(),
            catalog.metric_registry(),
            catalog.object_store(),
            &Handle::current(),
        ));
        let db = Arc::new(
            QuerierDatabase::new(
                catalog_cache,
                catalog.metric_registry(),
                catalog.exec(),
                Some(create_ingester_connection_for_testing()),
                QuerierDatabase::MAX_CONCURRENT_QUERIES_MAX,
                false,
            )
            .await
            .unwrap(),
        );

        let service = NamespaceServiceImpl::new(db);

        let namespaces = get_namespaces(&service).await;
        assert_eq!(
            namespaces,
            proto::GetNamespacesResponse { namespaces: vec![] }
        );
    }

    #[tokio::test]
    async fn test_get_namespaces() {
        let catalog = TestCatalog::new();

        // QuerierDatabase::new returns an error if there are no shards in the catalog
        catalog.create_shard(0).await;

        let catalog_cache = Arc::new(QuerierCatalogCache::new_testing(
            catalog.catalog(),
            catalog.time_provider(),
            catalog.metric_registry(),
            catalog.object_store(),
            &Handle::current(),
        ));
        let db = Arc::new(
            QuerierDatabase::new(
                catalog_cache,
                catalog.metric_registry(),
                catalog.exec(),
                Some(create_ingester_connection_for_testing()),
                QuerierDatabase::MAX_CONCURRENT_QUERIES_MAX,
                false,
            )
            .await
            .unwrap(),
        );

        let service = NamespaceServiceImpl::new(db);
        catalog.create_namespace_1hr_retention("namespace2").await;
        catalog.create_namespace_1hr_retention("namespace1").await;

        let namespaces = get_namespaces(&service).await;
        assert_eq!(
            namespaces,
            proto::GetNamespacesResponse {
                namespaces: vec![
                    proto::Namespace {
                        id: 1,
                        name: "namespace2".to_string(),
                        retention_period_ns: TEST_RETENTION_PERIOD_NS,
                    },
                    proto::Namespace {
                        id: 2,
                        name: "namespace1".to_string(),
                        retention_period_ns: TEST_RETENTION_PERIOD_NS,
                    },
                ]
            }
        );
    }

    async fn get_namespaces(service: &NamespaceServiceImpl) -> proto::GetNamespacesResponse {
        let request = proto::GetNamespacesRequest {};

        let mut namespaces = service
            .get_namespaces(tonic::Request::new(request))
            .await
            .unwrap()
            .into_inner();
        namespaces.namespaces.sort_by_key(|n| n.id);
        namespaces
    }
}
