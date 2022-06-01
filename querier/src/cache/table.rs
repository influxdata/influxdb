//! Table cache.

use backoff::{Backoff, BackoffConfig};
use cache_system::{
    backend::{
        lru::{LruBackend, ResourcePool},
        resource_consumption::FunctionEstimator,
        ttl::{OptionalValueTtlProvider, TtlBackend},
    },
    driver::Cache,
    loader::{metrics::MetricsLoader, FunctionLoader},
};
use data_types::{NamespaceId, Table, TableId};
use iox_catalog::interface::Catalog;
use iox_time::TimeProvider;
use std::{collections::HashMap, mem::size_of_val, sync::Arc, time::Duration};

use super::ram::RamSize;

/// Duration to keep non-existing tables.
pub const TTL_NON_EXISTING: Duration = Duration::from_secs(10);

const CACHE_ID: &str = "table";

type CacheT = Cache<TableId, Option<Arc<CachedTable>>, ()>;

/// Cache for table-related queries.
#[derive(Debug)]
pub struct TableCache {
    cache: CacheT,
}

impl TableCache {
    /// Create new empty cache.
    pub fn new(
        catalog: Arc<dyn Catalog>,
        backoff_config: BackoffConfig,
        time_provider: Arc<dyn TimeProvider>,
        metric_registry: &metric::Registry,
        ram_pool: Arc<ResourcePool<RamSize>>,
    ) -> Self {
        let loader = Box::new(FunctionLoader::new(move |table_id: TableId, _extra: ()| {
            let catalog = Arc::clone(&catalog);
            let backoff_config = backoff_config.clone();

            async move {
                let table = Backoff::new(&backoff_config)
                    .retry_all_errors("get table_name by ID", || async {
                        catalog
                            .repositories()
                            .await
                            .tables()
                            .get_by_id(table_id)
                            .await
                    })
                    .await
                    .expect("retry forever")?;

                Some(Arc::new(CachedTable::from(table)))
            }
        }));
        let loader = Arc::new(MetricsLoader::new(
            loader,
            CACHE_ID,
            Arc::clone(&time_provider),
            metric_registry,
        ));

        let backend = Box::new(TtlBackend::new(
            Box::new(HashMap::new()),
            Arc::new(OptionalValueTtlProvider::new(Some(TTL_NON_EXISTING), None)),
            Arc::clone(&time_provider),
        ));

        // add to memory pool
        let backend = Box::new(LruBackend::new(
            backend,
            Arc::clone(&ram_pool),
            CACHE_ID,
            Arc::new(FunctionEstimator::new(|k, v: &Option<Arc<CachedTable>>| {
                RamSize(
                    size_of_val(k)
                        + size_of_val(v)
                        + v.as_ref().map(|v| v.size()).unwrap_or_default(),
                )
            })),
        ));

        let cache = Cache::new(loader, backend);

        Self { cache }
    }

    /// Get the table name for the given table ID.
    ///
    /// This either uses a cached value or -- if required -- fetches the mapping from the catalog.
    pub async fn name(&self, table_id: TableId) -> Option<Arc<str>> {
        self.cache
            .get(table_id, ())
            .await
            .map(|t| Arc::clone(&t.name))
    }

    /// Get the table namespace ID for the given table ID.
    ///
    /// This either uses a cached value or -- if required -- fetches the mapping from the catalog.
    pub async fn namespace_id(&self, table_id: TableId) -> Option<NamespaceId> {
        self.cache.get(table_id, ()).await.map(|t| t.namespace_id)
    }
}

#[derive(Debug, Clone)]
struct CachedTable {
    name: Arc<str>,
    namespace_id: NamespaceId,
}

impl CachedTable {
    /// RAM-bytes EXCLUDING `self`.
    fn size(&self) -> usize {
        self.name.len()
    }
}

impl From<Table> for CachedTable {
    fn from(table: Table) -> Self {
        Self {
            name: Arc::from(table.name),
            namespace_id: table.namespace_id,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::cache::{ram::test_util::test_ram_pool, test_util::assert_histogram_metric_count};
    use iox_tests::util::TestCatalog;

    use super::*;

    #[tokio::test]
    async fn test_name() {
        let catalog = TestCatalog::new();

        let ns = catalog.create_namespace("ns").await;
        let t1 = ns.create_table("table1").await.table.clone();
        let t2 = ns.create_table("table2").await.table.clone();
        assert_ne!(t1.id, t2.id);

        let cache = TableCache::new(
            catalog.catalog(),
            BackoffConfig::default(),
            catalog.time_provider(),
            &catalog.metric_registry(),
            test_ram_pool(),
        );

        let name1_a = cache.name(t1.id).await.unwrap();
        assert_eq!(name1_a.as_ref(), t1.name.as_str());
        assert_histogram_metric_count(&catalog.metric_registry, "table_get_by_id", 1);

        let name2_a = cache.name(t2.id).await.unwrap();
        assert_eq!(name2_a.as_ref(), t2.name.as_str());
        assert_histogram_metric_count(&catalog.metric_registry, "table_get_by_id", 2);

        let name1_b = cache.name(t1.id).await.unwrap();
        assert!(Arc::ptr_eq(&name1_a, &name1_b));
        assert_histogram_metric_count(&catalog.metric_registry, "table_get_by_id", 2);

        // wait for "non-existing" TTL, this must not wipe the existing names from cache
        let name1_c = cache.name(t1.id).await.unwrap();
        assert_eq!(name1_c.as_ref(), t1.name.as_str());
        let name2_b = cache.name(t2.id).await.unwrap();
        assert_eq!(name2_b.as_ref(), t2.name.as_str());
        assert_histogram_metric_count(&catalog.metric_registry, "table_get_by_id", 2);
    }

    #[tokio::test]
    async fn test_name_non_existing() {
        let catalog = TestCatalog::new();

        let cache = TableCache::new(
            catalog.catalog(),
            BackoffConfig::default(),
            catalog.time_provider(),
            &catalog.metric_registry(),
            test_ram_pool(),
        );

        let none = cache.name(TableId::new(i64::MAX)).await;
        assert_eq!(none, None);
        assert_histogram_metric_count(&catalog.metric_registry, "table_get_by_id", 1);

        let none = cache.name(TableId::new(i64::MAX)).await;
        assert_eq!(none, None);
        assert_histogram_metric_count(&catalog.metric_registry, "table_get_by_id", 1);

        catalog.mock_time_provider().inc(TTL_NON_EXISTING);

        let none = cache.name(TableId::new(i64::MAX)).await;
        assert_eq!(none, None);
        assert_histogram_metric_count(&catalog.metric_registry, "table_get_by_id", 2);
    }

    #[tokio::test]
    async fn test_namespace_id() {
        let catalog = TestCatalog::new();

        let ns2 = catalog.create_namespace("ns1").await;
        let ns1 = catalog.create_namespace("ns2").await;
        let t1 = ns1.create_table("table1").await.table.clone();
        let t2 = ns2.create_table("table2").await.table.clone();
        assert_ne!(t1.id, t2.id);
        assert_ne!(t1.namespace_id, t2.namespace_id);

        let cache = TableCache::new(
            catalog.catalog(),
            BackoffConfig::default(),
            catalog.time_provider(),
            &catalog.metric_registry(),
            test_ram_pool(),
        );

        let id1_a = cache.namespace_id(t1.id).await.unwrap();
        assert_eq!(id1_a, t1.namespace_id);
        assert_histogram_metric_count(&catalog.metric_registry, "table_get_by_id", 1);

        let id2_a = cache.namespace_id(t2.id).await.unwrap();
        assert_eq!(id2_a, t2.namespace_id);
        assert_histogram_metric_count(&catalog.metric_registry, "table_get_by_id", 2);

        let id1_b = cache.namespace_id(t1.id).await.unwrap();
        assert_eq!(id1_b, t1.namespace_id);
        assert_histogram_metric_count(&catalog.metric_registry, "table_get_by_id", 2);

        // wait for "non-existing" TTL, this must not wipe the existing IDs from cache
        let id1_c = cache.namespace_id(t1.id).await.unwrap();
        assert_eq!(id1_c, t1.namespace_id);
        let id2_b = cache.namespace_id(t2.id).await.unwrap();
        assert_histogram_metric_count(&catalog.metric_registry, "table_get_by_id", 2);
        assert_eq!(id2_b, t2.namespace_id);
    }

    #[tokio::test]
    async fn test_namespace_id_non_existing() {
        let catalog = TestCatalog::new();

        let cache = TableCache::new(
            catalog.catalog(),
            BackoffConfig::default(),
            catalog.time_provider(),
            &catalog.metric_registry(),
            test_ram_pool(),
        );

        let none = cache.namespace_id(TableId::new(i64::MAX)).await;
        assert_eq!(none, None);
        assert_histogram_metric_count(&catalog.metric_registry, "table_get_by_id", 1);

        // "non-existing" is cached
        let none = cache.namespace_id(TableId::new(i64::MAX)).await;
        assert_eq!(none, None);
        assert_histogram_metric_count(&catalog.metric_registry, "table_get_by_id", 1);

        // let "non-existing" TTL expire
        catalog.mock_time_provider().inc(TTL_NON_EXISTING);
        let none = cache.namespace_id(TableId::new(i64::MAX)).await;
        assert_eq!(none, None);
        assert_histogram_metric_count(&catalog.metric_registry, "table_get_by_id", 2);
    }
}
