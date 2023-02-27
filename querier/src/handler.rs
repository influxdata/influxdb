//! Querier handler

use async_trait::async_trait;
use futures::{
    future::{BoxFuture, Shared},
    stream::FuturesUnordered,
    FutureExt, StreamExt, TryFutureExt,
};
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
use std::sync::Arc;
use thiserror::Error;
use tokio::task::{JoinError, JoinHandle};
use tokio_util::sync::CancellationToken;

use crate::{database::QuerierDatabase, poison::PoisonCabinet};

#[derive(Debug, Error)]
#[allow(missing_copy_implementations, missing_docs)]
pub enum Error {}

/// The [`QuerierHandler`] does nothing at this point
#[async_trait]
pub trait QuerierHandler: Send + Sync {
    /// Acquire a [`SchemaServiceServer`] gRPC service implementation.
    fn schema_service(&self) -> SchemaServiceServer<SchemaService>;

    /// Acquire a [`CatalogServiceServer`] gRPC service implementation.
    fn catalog_service(&self) -> CatalogServiceServer<CatalogService>;

    /// Acquire an [`ObjectStoreServiceServer`] gRPC service implementation.
    fn object_store_service(&self) -> ObjectStoreServiceServer<ObjectStoreService>;

    /// Wait until the handler finished  to shutdown.
    ///
    /// Use [`shutdown`](Self::shutdown) to trigger a shutdown.
    async fn join(&self);

    /// Shut down background workers.
    fn shutdown(&self);
}

/// A [`JoinHandle`] that can be cloned
type SharedJoinHandle = Shared<BoxFuture<'static, Result<(), Arc<JoinError>>>>;

/// Convert a [`JoinHandle`] into a [`SharedJoinHandle`].
#[allow(dead_code)]
fn shared_handle(handle: JoinHandle<()>) -> SharedJoinHandle {
    handle.map_err(Arc::new).boxed().shared()
}

/// Implementation of the `QuerierHandler` trait (that currently does nothing)
#[derive(Debug)]
pub struct QuerierHandlerImpl {
    /// Catalog (for other services)
    catalog: Arc<dyn Catalog>,

    /// Database that handles query operation
    database: Arc<QuerierDatabase>,

    /// The object store
    object_store: Arc<dyn ObjectStore>,

    /// Future that resolves when the background worker exits
    join_handles: Vec<(String, SharedJoinHandle)>,

    /// A token that is used to trigger shutdown of the background worker
    shutdown: CancellationToken,

    /// Poison pills for testing.
    #[allow(dead_code)]
    poison_cabinet: Arc<PoisonCabinet>,
}

impl QuerierHandlerImpl {
    /// Initialize the Querier
    pub fn new(
        catalog: Arc<dyn Catalog>,
        database: Arc<QuerierDatabase>,
        object_store: Arc<dyn ObjectStore>,
    ) -> Self {
        let shutdown = CancellationToken::new();
        let poison_cabinet = Arc::new(PoisonCabinet::new());

        let join_handles = vec![];
        Self {
            catalog,
            database,
            object_store,
            join_handles,
            shutdown,
            poison_cabinet,
        }
    }
}

#[async_trait]
impl QuerierHandler for QuerierHandlerImpl {
    fn schema_service(&self) -> SchemaServiceServer<SchemaService> {
        SchemaServiceServer::new(SchemaService::new(Arc::clone(&self.catalog)))
    }

    fn catalog_service(&self) -> CatalogServiceServer<CatalogService> {
        CatalogServiceServer::new(CatalogService::new(Arc::clone(&self.catalog)))
    }

    fn object_store_service(&self) -> ObjectStoreServiceServer<ObjectStoreService> {
        ObjectStoreServiceServer::new(ObjectStoreService::new(
            Arc::clone(&self.catalog),
            Arc::clone(&self.object_store),
        ))
    }

    async fn join(&self) {
        // Need to poll handlers unordered to detect early exists of any worker in the list.
        let mut unordered: FuturesUnordered<_> = self
            .join_handles
            .iter()
            .cloned()
            .map(|(name, handle)| async move { handle.await.map(|_| name) })
            .collect();

        while let Some(e) = unordered.next().await {
            let name = e.unwrap();

            if !self.shutdown.is_cancelled() {
                panic!("Background worker '{name}' exited early!");
            }
        }

        self.shutdown.cancelled().await;
        self.database.exec().join().await;
    }

    fn shutdown(&self) {
        self.shutdown.cancel();
        self.database.exec().shutdown();
    }
}

impl Drop for QuerierHandlerImpl {
    fn drop(&mut self) {
        if !self.shutdown.is_cancelled() {
            warn!("QuerierHandlerImpl dropped without calling shutdown()");
            self.shutdown.cancel();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{cache::CatalogCache, create_ingester_connection_for_testing};
    use data_types::ShardIndex;
    use iox_catalog::mem::MemCatalog;
    use iox_query::exec::Executor;
    use iox_time::{MockProvider, Time};
    use object_store::memory::InMemory;
    use std::time::Duration;
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
        querier: QuerierHandlerImpl,
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
            // QuerierDatabase::new returns an error if there are no shards in the catalog
            {
                let mut repos = catalog.repositories().await;

                let topic = repos.topics().create_or_get("topic").await.unwrap();
                let shard_index = ShardIndex::new(0);
                repos
                    .shards()
                    .create_or_get(&topic, shard_index)
                    .await
                    .unwrap();
            }

            let database = Arc::new(
                QuerierDatabase::new(
                    catalog_cache,
                    metric_registry,
                    exec,
                    Some(create_ingester_connection_for_testing()),
                    QuerierDatabase::MAX_CONCURRENT_QUERIES_MAX,
                    true,
                )
                .await
                .unwrap(),
            );
            let querier = QuerierHandlerImpl::new(catalog, database, object_store);

            Self { querier }
        }
    }
}
