//! Database for the querier that contains all namespaces.

use crate::{
    cache::CatalogCache, chunk::ChunkAdapter, ingester::IngesterConnection,
    namespace::QuerierNamespace, query_log::QueryLog,
};
use async_trait::async_trait;
use backoff::{Backoff, BackoffConfig};
use data_types::Namespace;
use iox_query::exec::Executor;
use parquet_file::storage::ParquetStorage;
use service_common::QueryDatabaseProvider;
use std::sync::Arc;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// The number of entries to store in the circular query buffer log.
///
/// That buffer is shared between all namespaces, and filtered on query
const QUERY_LOG_SIZE: usize = 10_000;

/// Database for the querier.
///
/// Contains all namespaces.
#[derive(Debug)]
pub struct QuerierDatabase {
    /// Backoff config for IO operations.
    backoff_config: BackoffConfig,

    /// Catalog cache.
    catalog_cache: Arc<CatalogCache>,

    /// Adapter to create chunks.
    chunk_adapter: Arc<ChunkAdapter>,

    /// Metric registry
    #[allow(dead_code)]
    metric_registry: Arc<metric::Registry>,

    /// Executor for queries.
    exec: Arc<Executor>,

    /// Connection to ingester(s)
    ingester_connection: Arc<dyn IngesterConnection>,

    /// Query log.
    query_log: Arc<QueryLog>,

    /// Semaphore that limits the number of namespaces in used at the time by the query subsystem.
    ///
    /// This should be a 1-to-1 relation to the number of active queries.
    ///
    /// If the same database is requested twice for different queries, it is counted twice.
    query_execution_semaphore: Arc<Semaphore>,
}

#[async_trait]
impl QueryDatabaseProvider for QuerierDatabase {
    type Db = QuerierNamespace;

    async fn db(&self, name: &str) -> Option<Arc<Self::Db>> {
        self.namespace(name).await
    }

    async fn acquire_semaphore(&self) -> OwnedSemaphorePermit {
        Arc::clone(&self.query_execution_semaphore)
            .acquire_owned()
            .await
            .expect("Semaphore should not be closed by anyone")
    }
}

impl QuerierDatabase {
    /// The maximum value for `max_concurrent_queries` that is allowed.
    ///
    /// This limit exists because [`tokio::sync::Semaphore`] has an internal limit and semaphore creation beyond that
    /// will panic. The tokio limit is not exposed though so we pick a reasonable but smaller number.
    pub const MAX_CONCURRENT_QUERIES_MAX: usize = u16::MAX as usize;

    /// Create new database.
    pub fn new(
        catalog_cache: Arc<CatalogCache>,
        metric_registry: Arc<metric::Registry>,
        store: ParquetStorage,
        exec: Arc<Executor>,
        ingester_connection: Arc<dyn IngesterConnection>,
        max_concurrent_queries: usize,
    ) -> Self {
        assert!(
            max_concurrent_queries <= Self::MAX_CONCURRENT_QUERIES_MAX,
            "`max_concurrent_queries` ({}) > `max_concurrent_queries_MAX` ({})",
            max_concurrent_queries,
            Self::MAX_CONCURRENT_QUERIES_MAX,
        );

        let chunk_adapter = Arc::new(ChunkAdapter::new(
            Arc::clone(&catalog_cache),
            store,
            Arc::clone(&metric_registry),
            catalog_cache.time_provider(),
        ));
        let query_log = Arc::new(QueryLog::new(QUERY_LOG_SIZE, catalog_cache.time_provider()));
        let query_execution_semaphore = Arc::new(Semaphore::new(max_concurrent_queries));

        Self {
            backoff_config: BackoffConfig::default(),
            catalog_cache,
            chunk_adapter,
            metric_registry,
            exec,
            ingester_connection,
            query_log,
            query_execution_semaphore,
        }
    }

    /// Get namespace if it exists.
    ///
    /// This will await the internal namespace semaphore. Existence of namespaces is checked AFTER a semaphore permit
    /// was acquired since this lowers the chance that we obtain stale data.
    pub async fn namespace(&self, name: &str) -> Option<Arc<QuerierNamespace>> {
        let name = Arc::from(name.to_owned());
        let schema = self
            .catalog_cache
            .namespace()
            .schema(Arc::clone(&name))
            .await?;
        Some(Arc::new(QuerierNamespace::new(
            Arc::clone(&self.chunk_adapter),
            schema,
            name,
            Arc::clone(&self.exec),
            Arc::clone(&self.ingester_connection),
            Arc::clone(&self.query_log),
        )))
    }

    /// Return all namespaces this querier knows about
    pub async fn namespaces(&self) -> Vec<Namespace> {
        let catalog = &self.catalog_cache.catalog();
        Backoff::new(&self.backoff_config)
            .retry_all_errors("listing namespaces", || async {
                catalog.repositories().await.namespaces().list().await
            })
            .await
            .expect("retry forever")
    }

    /// Return connection to ingester(s) to get and aggregate information from them
    pub fn ingester_connection(&self) -> Arc<dyn IngesterConnection> {
        Arc::clone(&self.ingester_connection)
    }

    /// Executor
    pub(crate) fn exec(&self) -> &Executor {
        &self.exec
    }
}

#[cfg(test)]
mod tests {
    use iox_tests::util::TestCatalog;

    use crate::create_ingester_connection_for_testing;

    use super::*;

    #[test]
    #[should_panic(
        expected = "`max_concurrent_queries` (65536) > `max_concurrent_queries_MAX` (65535)"
    )]
    fn test_semaphore_limit_is_checked() {
        let catalog = TestCatalog::new();

        let catalog_cache = Arc::new(CatalogCache::new(
            catalog.catalog(),
            catalog.time_provider(),
            catalog.metric_registry(),
            usize::MAX,
        ));
        QuerierDatabase::new(
            catalog_cache,
            catalog.metric_registry(),
            ParquetStorage::new(catalog.object_store()),
            catalog.exec(),
            create_ingester_connection_for_testing(),
            QuerierDatabase::MAX_CONCURRENT_QUERIES_MAX.saturating_add(1),
        );
    }

    #[tokio::test]
    async fn test_namespace() {
        let catalog = TestCatalog::new();

        let catalog_cache = Arc::new(CatalogCache::new(
            catalog.catalog(),
            catalog.time_provider(),
            catalog.metric_registry(),
            usize::MAX,
        ));
        let db = QuerierDatabase::new(
            catalog_cache,
            catalog.metric_registry(),
            ParquetStorage::new(catalog.object_store()),
            catalog.exec(),
            create_ingester_connection_for_testing(),
            QuerierDatabase::MAX_CONCURRENT_QUERIES_MAX,
        );

        catalog.create_namespace("ns1").await;

        assert!(db.namespace("ns1").await.is_some());
        assert!(db.namespace("ns2").await.is_none());
    }

    #[tokio::test]
    async fn test_namespaces() {
        let catalog = TestCatalog::new();

        let catalog_cache = Arc::new(CatalogCache::new(
            catalog.catalog(),
            catalog.time_provider(),
            catalog.metric_registry(),
            usize::MAX,
        ));
        let db = QuerierDatabase::new(
            catalog_cache,
            catalog.metric_registry(),
            ParquetStorage::new(catalog.object_store()),
            catalog.exec(),
            create_ingester_connection_for_testing(),
            QuerierDatabase::MAX_CONCURRENT_QUERIES_MAX,
        );

        catalog.create_namespace("ns1").await;
        catalog.create_namespace("ns2").await;

        let mut namespaces = db.namespaces().await;
        namespaces.sort_by_key(|ns| ns.name.clone());
        assert_eq!(namespaces.len(), 2);
        assert_eq!(namespaces[0].name, "ns1");
        assert_eq!(namespaces[1].name, "ns2");
    }
}
