//! Database for the querier that contains all namespaces.

use crate::{
    cache::CatalogCache, chunk::ParquetChunkAdapter, ingester::IngesterConnection,
    namespace::QuerierNamespace, query_log::QueryLog,
};
use async_trait::async_trait;
use backoff::{Backoff, BackoffConfig};
use data_types::Namespace;
use object_store::DynObjectStore;
use parking_lot::RwLock;
use query::exec::Executor;
use service_common::QueryDatabaseProvider;
use std::{collections::HashMap, sync::Arc};

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
    chunk_adapter: Arc<ParquetChunkAdapter>,

    /// Metric registry
    metric_registry: Arc<metric::Registry>,

    /// Namespaces.
    namespaces: RwLock<HashMap<Arc<str>, Arc<QuerierNamespace>>>,

    /// Object store.
    object_store: Arc<DynObjectStore>,

    /// Executor for queries.
    exec: Arc<Executor>,

    /// Connection to ingester
    ingester_connection: Arc<dyn IngesterConnection>,

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
        catalog_cache: Arc<CatalogCache>,
        metric_registry: Arc<metric::Registry>,
        object_store: Arc<DynObjectStore>,
        exec: Arc<Executor>,
        ingester_connection: Arc<dyn IngesterConnection>,
    ) -> Self {
        let chunk_adapter = Arc::new(ParquetChunkAdapter::new(
            Arc::clone(&catalog_cache),
            Arc::clone(&object_store),
            Arc::clone(&metric_registry),
            catalog_cache.time_provider(),
        ));
        let query_log = Arc::new(QueryLog::new(QUERY_LOG_SIZE, catalog_cache.time_provider()));

        Self {
            backoff_config: BackoffConfig::default(),
            catalog_cache,
            chunk_adapter,
            metric_registry,
            namespaces: RwLock::new(HashMap::new()),
            object_store,
            exec,
            ingester_connection,
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

    #[tokio::test]
    async fn test_namespace() {
        let catalog = TestCatalog::new();

        let catalog_cache = Arc::new(CatalogCache::new(
            catalog.catalog(),
            catalog.time_provider(),
        ));
        let db = QuerierDatabase::new(
            catalog_cache,
            catalog.metric_registry(),
            catalog.object_store(),
            catalog.exec(),
            create_ingester_connection_for_testing(),
        );

        catalog.create_namespace("ns1").await;

        assert!(db.namespace("ns1").await.is_some());
        assert!(db.namespace("ns2").await.is_none());
    }
}
