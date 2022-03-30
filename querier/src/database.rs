//! Database for the querier that contains all namespaces.

use crate::{
    cache::CatalogCache, chunk::ParquetChunkAdapter, namespace::QuerierNamespace,
    query_log::QueryLog,
};
use async_trait::async_trait;
use backoff::BackoffConfig;
use iox_catalog::interface::Catalog;
use object_store::DynObjectStore;
use parking_lot::RwLock;
use query::exec::Executor;
use service_common::QueryDatabaseProvider;
use std::{collections::HashMap, sync::Arc};
use time::TimeProvider;

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

    /// Catalog.
    catalog: Arc<dyn Catalog>,

    /// Catalog cache.
    catalog_cache: Arc<CatalogCache>,

    /// Adapter to create chunks.
    chunk_adapter: Arc<ParquetChunkAdapter>,

    /// Metric registry
    metric_registry: Arc<metric::Registry>,

    /// Namespaces.
    namespaces: RwLock<HashMap<Arc<str>, Arc<QuerierNamespace>>>,

    /// Object store.
    object_store: Arc<DynObjectStore>,

    /// Time provider.
    time_provider: Arc<dyn TimeProvider>,

    /// Executor for queries.
    exec: Arc<Executor>,

    /// Query log.
    query_log: Arc<QueryLog>,
}

#[async_trait]
impl QueryDatabaseProvider for QuerierDatabase {
    type Db = QuerierNamespace;

    async fn db(&self, name: &str) -> Option<Arc<Self::Db>> {
        self.namespace(name).await
    }
}

impl QuerierDatabase {
    /// Create new database.
    pub fn new(
        catalog: Arc<dyn Catalog>,
        metric_registry: Arc<metric::Registry>,
        object_store: Arc<DynObjectStore>,
        time_provider: Arc<dyn TimeProvider>,
        exec: Arc<Executor>,
    ) -> Self {
        let catalog_cache = Arc::new(CatalogCache::new(
            Arc::clone(&catalog),
            Arc::clone(&time_provider),
        ));
        let chunk_adapter = Arc::new(ParquetChunkAdapter::new(
            Arc::clone(&catalog_cache),
            Arc::clone(&object_store),
            Arc::clone(&metric_registry),
            Arc::clone(&time_provider),
        ));
        let query_log = Arc::new(QueryLog::new(QUERY_LOG_SIZE, Arc::clone(&time_provider)));

        Self {
            backoff_config: BackoffConfig::default(),
            catalog,
            catalog_cache,
            chunk_adapter,
            metric_registry,
            namespaces: RwLock::new(HashMap::new()),
            object_store,
            time_provider,
            exec,
            query_log,
        }
    }

    /// Get namespace if it exists
    pub async fn namespace(&self, name: &str) -> Option<Arc<QuerierNamespace>> {
        let name = Arc::from(name.to_owned());
        let schema = self
            .catalog_cache
            .namespace()
            .schema(Arc::clone(&name))
            .await?;
        Some(Arc::new(QuerierNamespace::new(
            self.backoff_config.clone(),
            Arc::clone(&self.chunk_adapter),
            schema,
            name,
            Arc::clone(&self.exec),
            Arc::clone(&self.query_log),
        )))
    }
}

#[cfg(test)]
mod tests {
    use iox_tests::util::TestCatalog;

    use super::*;

    #[tokio::test]
    async fn test_namespace() {
        let catalog = TestCatalog::new();

        let db = QuerierDatabase::new(
            catalog.catalog(),
            catalog.metric_registry(),
            catalog.object_store(),
            catalog.time_provider(),
            catalog.exec(),
        );

        catalog.create_namespace("ns1").await;

        assert!(db.namespace("ns1").await.is_some());
        assert!(db.namespace("ns2").await.is_none());
    }
}
