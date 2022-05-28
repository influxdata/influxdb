//! NamespaceService gRPC implementation

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
}

#[cfg(test)]
mod tests {
    use generated_types::influxdata::iox::namespace::v1::namespace_service_server::NamespaceService;
    use iox_tests::util::TestCatalog;
    use parquet_file::storage::ParquetStorage;
    use querier::{create_ingester_connection_for_testing, QuerierCatalogCache};

    use super::*;

    #[tokio::test]
    async fn test_get_namespaces_empty() {
        let catalog = TestCatalog::new();

        let catalog_cache = Arc::new(QuerierCatalogCache::new(
            catalog.catalog(),
            catalog.time_provider(),
            catalog.metric_registry(),
            usize::MAX,
        ));
        let db = Arc::new(QuerierDatabase::new(
            catalog_cache,
            catalog.metric_registry(),
            ParquetStorage::new(catalog.object_store()),
            catalog.exec(),
            create_ingester_connection_for_testing(),
        ));

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

        let catalog_cache = Arc::new(QuerierCatalogCache::new(
            catalog.catalog(),
            catalog.time_provider(),
            catalog.metric_registry(),
            usize::MAX,
        ));
        let db = Arc::new(QuerierDatabase::new(
            catalog_cache,
            catalog.metric_registry(),
            ParquetStorage::new(catalog.object_store()),
            catalog.exec(),
            create_ingester_connection_for_testing(),
        ));

        let service = NamespaceServiceImpl::new(db);
        catalog.create_namespace("namespace2").await;
        catalog.create_namespace("namespace1").await;

        let namespaces = get_namespaces(&service).await;
        assert_eq!(
            namespaces,
            proto::GetNamespacesResponse {
                namespaces: vec![
                    proto::Namespace {
                        id: 1,
                        name: "namespace2".to_string(),
                    },
                    proto::Namespace {
                        id: 2,
                        name: "namespace1".to_string(),
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
