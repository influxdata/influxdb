//! Querier server entrypoint.

use std::sync::Arc;

use observability_deps::tracing::warn;
use tokio_util::sync::CancellationToken;

use crate::QuerierDatabase;

/// The [`QuerierServer`] manages the lifecycle and contains all state for a
/// `querier` server instance.
#[derive(Debug)]
pub struct QuerierServer {
    /// Database that handles query operation
    database: Arc<QuerierDatabase>,

    /// Remembers if `shutdown` was called but also blocks the `join` call.
    shutdown: CancellationToken,
}

impl QuerierServer {
    /// Initialise a new [`QuerierServer`] using the provided gRPC
    /// handlers.
    pub fn new(database: Arc<QuerierDatabase>) -> Self {
        Self {
            database,
            shutdown: CancellationToken::new(),
        }
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
            let querier = QuerierServer::new(database);

            Self { querier }
        }
    }
}
