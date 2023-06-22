//! Querier server entrypoint.

use std::sync::Arc;

use influxdb_iox_client::{
    catalog::generated_types::catalog_service_server::CatalogServiceServer,
    schema::generated_types::schema_service_server::SchemaServiceServer,
    store::generated_types::object_store_service_server::ObjectStoreServiceServer,
};
use iox_catalog::interface::Catalog;
use object_store::ObjectStore;
use observability_deps::tracing::warn;
use service_grpc_catalog::CatalogService;
use service_grpc_object_store::ObjectStoreService;
use service_grpc_schema::SchemaService;
use tokio_util::sync::CancellationToken;

use crate::QuerierDatabase;

/// The [`QuerierServer`] manages the lifecycle and contains all state for a
/// `querier` server instance.
#[derive(Debug)]
pub struct QuerierServer {
    /// Metrics (for other services)
    metrics: Arc<metric::Registry>,

    /// Catalog (for other services)
    catalog: Arc<dyn Catalog>,

    /// Database that handles query operation
    database: Arc<QuerierDatabase>,

    /// The object store
    object_store: Arc<dyn ObjectStore>,

    /// Remembers if `shutdown` was called but also blocks the `join` call.
    shutdown: CancellationToken,
}

impl QuerierServer {
    /// Initialise a new [`QuerierServer`] using the provided gRPC
    /// handlers.
    pub fn new(
        catalog: Arc<dyn Catalog>,
        database: Arc<QuerierDatabase>,
        metrics: Arc<metric::Registry>,
        object_store: Arc<dyn ObjectStore>,
    ) -> Self {
        Self {
            catalog,
            database,
            metrics,
            object_store,
            shutdown: CancellationToken::new(),
        }
    }

    /// Return the [`metric::Registry`] used by the router.
    pub fn metric_registry(&self) -> Arc<metric::Registry> {
        Arc::clone(&self.metrics)
    }

    /// Acquire a [`SchemaServiceServer`] gRPC service implementation.
    pub fn schema_service(&self) -> SchemaServiceServer<SchemaService> {
        SchemaServiceServer::new(SchemaService::new(Arc::clone(&self.catalog)))
    }

    /// Acquire a [`CatalogServiceServer`] gRPC service implementation.
    pub fn catalog_service(&self) -> CatalogServiceServer<CatalogService> {
        CatalogServiceServer::new(CatalogService::new(Arc::clone(&self.catalog)))
    }

    /// Acquire an [`ObjectStoreServiceServer`] gRPC service implementation.
    pub fn object_store_service(&self) -> ObjectStoreServiceServer<ObjectStoreService> {
        ObjectStoreServiceServer::new(ObjectStoreService::new(
            Arc::clone(&self.catalog),
            Arc::clone(&self.object_store),
        ))
    }

    /// Wait until the handler finished  to shutdown.
    ///
    /// Use [`shutdown`](Self::shutdown) to trigger a shutdown.
    pub async fn join(&self) {
        self.shutdown.cancelled().await;
        self.database.exec().join().await;
    }

    /// Shut down background workers.
    pub fn shutdown(&self) {
        self.shutdown.cancel();
        self.database.exec().shutdown();
    }
}

impl Drop for QuerierServer {
    fn drop(&mut self) {
        if !self.shutdown.is_cancelled() {
            warn!("QuerierServer dropped without calling shutdown()");
            self.shutdown();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{cache::CatalogCache, create_ingester_connection_for_testing};
    use iox_catalog::mem::MemCatalog;
    use iox_query::exec::Executor;
    use iox_time::{MockProvider, Time};
    use object_store::memory::InMemory;
    use std::{collections::HashMap, time::Duration};
    use tokio::runtime::Handle;

    #[tokio::test]
    async fn test_shutdown() {
        let querier = TestQuerier::new().await.querier;

        // does not exit w/o shutdown
        tokio::select! {
            _ = querier.join() => panic!("querier finished w/o shutdown"),
            _ = tokio::time::sleep(Duration::from_millis(10)) => {},
        };

        querier.shutdown();

        tokio::time::timeout(Duration::from_millis(1000), querier.join())
            .await
            .unwrap();
    }

    struct TestQuerier {
        querier: QuerierServer,
    }

    impl TestQuerier {
        async fn new() -> Self {
            let metric_registry = Arc::new(metric::Registry::new());
            let catalog = Arc::new(MemCatalog::new(Arc::clone(&metric_registry))) as _;
            let object_store = Arc::new(InMemory::new()) as _;

            let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
            let exec = Arc::new(Executor::new_testing());
            let catalog_cache = Arc::new(CatalogCache::new_testing(
                Arc::clone(&catalog),
                time_provider,
                Arc::clone(&metric_registry),
                Arc::clone(&object_store),
                &Handle::current(),
            ));

            let database = Arc::new(
                QuerierDatabase::new(
                    catalog_cache,
                    Arc::clone(&metric_registry),
                    exec,
                    Some(create_ingester_connection_for_testing()),
                    QuerierDatabase::MAX_CONCURRENT_QUERIES_MAX,
                    Arc::new(HashMap::default()),
                )
                .await
                .unwrap(),
            );
            let querier = QuerierServer::new(catalog, database, metric_registry, object_store);

            Self { querier }
        }
    }
}
