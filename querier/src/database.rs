//! Database for the querier that contains all namespaces.

use crate::{
    cache::CatalogCache,
    ingester::IngesterConnection,
    namespace::{QuerierNamespace, QuerierNamespaceArgs},
    parquet::ChunkAdapter,
    query_log::QueryLog,
    table::PruneMetrics,
};
use async_trait::async_trait;
use backoff::{Backoff, BackoffConfig};
use data_types::Namespace;
use iox_catalog::interface::SoftDeletedRows;
use iox_query::exec::Executor;
use service_common::QueryNamespaceProvider;
use snafu::Snafu;
use std::{collections::HashMap, sync::Arc};
use trace::span::{Span, SpanRecorder};
use tracker::{
    AsyncSemaphoreMetrics, InstrumentedAsyncOwnedSemaphorePermit, InstrumentedAsyncSemaphore,
};

/// The number of entries to store in the circular query buffer log.
///
/// That buffer is shared between all namespaces, and filtered on query
const QUERY_LOG_SIZE: usize = 10_000;

#[allow(missing_docs)]
#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Catalog error: {source}"))]
    Catalog {
        source: iox_catalog::interface::Error,
    },
}

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

    /// Executor for queries.
    exec: Arc<Executor>,

    /// Connection to ingester(s)
    ingester_connection: Option<Arc<dyn IngesterConnection>>,

    /// Query log.
    query_log: Arc<QueryLog>,

    /// Semaphore that limits the number of namespaces in used at the time by the query subsystem.
    ///
    /// This should be a 1-to-1 relation to the number of active queries.
    ///
    /// If the same namespace is requested twice for different queries, it is counted twice.
    query_execution_semaphore: Arc<InstrumentedAsyncSemaphore>,

    /// Chunk prune metrics.
    prune_metrics: Arc<PruneMetrics>,

    /// DataFusion config.
    datafusion_config: Arc<HashMap<String, String>>,
}

#[async_trait]
impl QueryNamespaceProvider for QuerierDatabase {
    type Db = QuerierNamespace;

    async fn db(
        &self,
        name: &str,
        span: Option<Span>,
        include_debug_info_tables: bool,
    ) -> Option<Arc<Self::Db>> {
        self.namespace(name, span, include_debug_info_tables).await
    }

    async fn acquire_semaphore(&self, span: Option<Span>) -> InstrumentedAsyncOwnedSemaphorePermit {
        Arc::clone(&self.query_execution_semaphore)
            .acquire_owned(span)
            .await
            .expect("Semaphore should not be closed by anyone")
    }
}

impl QuerierDatabase {
    /// The maximum value for `max_concurrent_queries` that is allowed.
    ///
    /// This limit exists because [`tokio::sync::Semaphore`] has an internal limit and semaphore
    /// creation beyond that will panic. The tokio limit is not exposed though so we pick a
    /// reasonable but smaller number.
    pub const MAX_CONCURRENT_QUERIES_MAX: usize = u16::MAX as usize;

    /// Create new database.
    pub async fn new(
        catalog_cache: Arc<CatalogCache>,
        metric_registry: Arc<metric::Registry>,
        exec: Arc<Executor>,
        ingester_connection: Option<Arc<dyn IngesterConnection>>,
        max_concurrent_queries: usize,
        datafusion_config: Arc<HashMap<String, String>>,
    ) -> Result<Self, Error> {
        assert!(
            max_concurrent_queries <= Self::MAX_CONCURRENT_QUERIES_MAX,
            "`max_concurrent_queries` ({}) > `max_concurrent_queries_MAX` ({})",
            max_concurrent_queries,
            Self::MAX_CONCURRENT_QUERIES_MAX,
        );

        let backoff_config = BackoffConfig::default();

        let chunk_adapter = Arc::new(ChunkAdapter::new(
            Arc::clone(&catalog_cache),
            Arc::clone(&metric_registry),
        ));
        let query_log = Arc::new(QueryLog::new(QUERY_LOG_SIZE, catalog_cache.time_provider()));
        let semaphore_metrics = Arc::new(AsyncSemaphoreMetrics::new(
            &metric_registry,
            &[("semaphore", "query_execution")],
        ));
        let query_execution_semaphore =
            Arc::new(semaphore_metrics.new_semaphore(max_concurrent_queries));

        let prune_metrics = Arc::new(PruneMetrics::new(&metric_registry));

        Ok(Self {
            backoff_config,
            catalog_cache,
            chunk_adapter,
            exec,
            ingester_connection,
            query_log,
            query_execution_semaphore,
            prune_metrics,
            datafusion_config,
        })
    }

    /// Get namespace if it exists.
    ///
    /// This will await the internal namespace semaphore. Existence of namespaces is checked AFTER
    /// a semaphore permit was acquired since this lowers the chance that we obtain stale data.
    pub async fn namespace(
        &self,
        name: &str,
        span: Option<Span>,
        include_debug_info_tables: bool,
    ) -> Option<Arc<QuerierNamespace>> {
        let span_recorder = SpanRecorder::new(span);
        let name = Arc::from(name.to_owned());
        let ns = self
            .catalog_cache
            .namespace()
            .get(
                Arc::clone(&name),
                // we have no specific need for any tables or columns at this point, so nothing to cover
                &[],
                span_recorder.child_span("cache GET namespace schema"),
            )
            .await?;
        Some(Arc::new(QuerierNamespace::new(QuerierNamespaceArgs {
            chunk_adapter: Arc::clone(&self.chunk_adapter),
            ns,
            name,
            exec: Arc::clone(&self.exec),
            ingester_connection: self.ingester_connection.clone(),
            query_log: Arc::clone(&self.query_log),
            prune_metrics: Arc::clone(&self.prune_metrics),
            datafusion_config: Arc::clone(&self.datafusion_config),
            include_debug_info_tables,
        })))
    }

    /// Return all namespaces this querier knows about
    pub async fn namespaces(&self) -> Vec<Namespace> {
        let catalog = &self.catalog_cache.catalog();
        Backoff::new(&self.backoff_config)
            .retry_all_errors("listing namespaces", || async {
                catalog
                    .repositories()
                    .await
                    .namespaces()
                    .list(SoftDeletedRows::ExcludeDeleted)
                    .await
            })
            .await
            .expect("retry forever")
    }

    /// Return connection to ingester(s) to get and aggregate information from them
    pub fn ingester_connection(&self) -> Option<Arc<dyn IngesterConnection>> {
        self.ingester_connection.clone()
    }

    /// Executor
    pub(crate) fn exec(&self) -> &Executor {
        &self.exec
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::create_ingester_connection_for_testing;
    use iox_tests::TestCatalog;
    use tokio::runtime::Handle;

    #[tokio::test]
    #[should_panic(
        expected = "`max_concurrent_queries` (65536) > `max_concurrent_queries_MAX` (65535)"
    )]
    async fn test_semaphore_limit_is_checked() {
        let catalog = TestCatalog::new();

        let catalog_cache = Arc::new(CatalogCache::new_testing(
            catalog.catalog(),
            catalog.time_provider(),
            catalog.metric_registry(),
            catalog.object_store(),
            &Handle::current(),
        ));
        QuerierDatabase::new(
            catalog_cache,
            catalog.metric_registry(),
            catalog.exec(),
            Some(create_ingester_connection_for_testing()),
            QuerierDatabase::MAX_CONCURRENT_QUERIES_MAX.saturating_add(1),
            Arc::new(HashMap::default()),
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn test_namespace() {
        let catalog = TestCatalog::new();
        let db = new_db(&catalog).await;

        catalog.create_namespace_1hr_retention("ns1").await;

        assert!(db.namespace("ns1", None, true).await.is_some());
        assert!(db.namespace("ns2", None, true).await.is_none());
    }

    #[tokio::test]
    async fn test_namespaces() {
        let catalog = TestCatalog::new();
        let db = new_db(&catalog).await;

        catalog.create_namespace_1hr_retention("ns1").await;
        catalog.create_namespace_1hr_retention("ns2").await;

        let mut namespaces = db.namespaces().await;
        namespaces.sort_by_key(|ns| ns.name.clone());
        assert_eq!(namespaces.len(), 2);
        assert_eq!(namespaces[0].name, "ns1");
        assert_eq!(namespaces[1].name, "ns2");
    }

    async fn new_db(catalog: &Arc<TestCatalog>) -> QuerierDatabase {
        let catalog_cache = Arc::new(CatalogCache::new_testing(
            catalog.catalog(),
            catalog.time_provider(),
            catalog.metric_registry(),
            catalog.object_store(),
            &Handle::current(),
        ));
        QuerierDatabase::new(
            catalog_cache,
            catalog.metric_registry(),
            catalog.exec(),
            Some(create_ingester_connection_for_testing()),
            QuerierDatabase::MAX_CONCURRENT_QUERIES_MAX,
            Arc::new(HashMap::default()),
        )
        .await
        .unwrap()
    }
}
