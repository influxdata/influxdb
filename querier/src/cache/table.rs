//! Table cache.
use std::{collections::HashMap, sync::Arc, time::Duration};

use backoff::{Backoff, BackoffConfig};
use data_types2::{NamespaceId, Table, TableId};
use iox_catalog::interface::Catalog;
use time::TimeProvider;

use crate::cache_system::{
    backend::{
        dual::dual_backends,
        ttl::{OptionalValueTtlProvider, TtlBackend},
    },
    driver::Cache,
    loader::FunctionLoader,
};

/// Duration to keep non-existing tables.
pub const TTL_NON_EXISTING: Duration = Duration::from_secs(10);

type CacheFromId = Cache<TableId, Option<Arc<CachedTable>>>;
type CacheFromName = Cache<(NamespaceId, Arc<str>), Option<Arc<CachedTable>>>;

/// Cache for table-related queries.
#[derive(Debug)]
pub struct TableCache {
    cache_from_id: CacheFromId,
    cache_from_name: CacheFromName,
}

impl TableCache {
    /// Create new empty cache.
    pub fn new(
        catalog: Arc<dyn Catalog>,
        backoff_config: BackoffConfig,
        time_provider: Arc<dyn TimeProvider>,
    ) -> Self {
        let catalog_captured = Arc::clone(&catalog);
        let backoff_config_captured = backoff_config.clone();
        let loader_from_id = Arc::new(FunctionLoader::new(move |table_id| {
            let catalog = Arc::clone(&catalog_captured);
            let backoff_config = backoff_config_captured.clone();

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
        let backend_from_id = Box::new(TtlBackend::new(
            Box::new(HashMap::new()),
            Arc::new(OptionalValueTtlProvider::new(Some(TTL_NON_EXISTING), None)),
            Arc::clone(&time_provider),
        ));
        let mapper_from_id = |_k: &_, maybe_table: &Option<Arc<CachedTable>>| {
            maybe_table
                .as_ref()
                .map(|t| (t.namespace_id, Arc::clone(&t.name)))
        };

        let loader_from_name = Arc::new(FunctionLoader::new(
            move |(namespace_id, name): (_, Arc<str>)| {
                let catalog = Arc::clone(&catalog);
                let backoff_config = backoff_config.clone();

                async move {
                    let table = Backoff::new(&backoff_config)
                        .retry_all_errors("get table_name by namespace and name", || async {
                            catalog
                                .repositories()
                                .await
                                .tables()
                                .get_by_namespace_and_name(namespace_id, name.as_ref())
                                .await
                        })
                        .await
                        .expect("retry forever")?;

                    Some(Arc::new(CachedTable::from(table)))
                }
            },
        ));
        let backend_from_name = Box::new(TtlBackend::new(
            Box::new(HashMap::new()),
            Arc::new(OptionalValueTtlProvider::new(Some(TTL_NON_EXISTING), None)),
            Arc::clone(&time_provider),
        ));
        let mapper_from_name =
            |_k: &_, maybe_table: &Option<Arc<CachedTable>>| maybe_table.as_ref().map(|t| t.id);

        // cross backends
        let (backend_from_id, backend_from_name) = dual_backends(
            backend_from_id,
            mapper_from_id,
            backend_from_name,
            mapper_from_name,
        );

        let cache_from_id = Cache::new(loader_from_id, Box::new(backend_from_id));
        let cache_from_name = Cache::new(loader_from_name, Box::new(backend_from_name));

        Self {
            cache_from_id,
            cache_from_name,
        }
    }

    /// Get the table name for the given table ID.
    ///
    /// This either uses a cached value or -- if required -- fetches the mapping from the catalog.
    pub async fn name(&self, table_id: TableId) -> Option<Arc<str>> {
        self.cache_from_id
            .get(table_id)
            .await
            .map(|t| Arc::clone(&t.name))
    }

    /// Get the table namespace ID for the given table ID.
    ///
    /// This either uses a cached value or -- if required -- fetches the mapping from the catalog.
    pub async fn namespace_id(&self, table_id: TableId) -> Option<NamespaceId> {
        self.cache_from_id
            .get(table_id)
            .await
            .map(|t| t.namespace_id)
    }

    /// Get table ID by namespace ID and table name.
    pub async fn id(&self, namespace_id: NamespaceId, name: Arc<str>) -> Option<TableId> {
        self.cache_from_name
            .get((namespace_id, name))
            .await
            .map(|t| t.id)
    }
}

#[derive(Debug, Clone)]
struct CachedTable {
    id: TableId,
    name: Arc<str>,
    namespace_id: NamespaceId,
}

impl From<Table> for CachedTable {
    fn from(table: Table) -> Self {
        Self {
            id: table.id,
            name: Arc::from(table.name),
            namespace_id: table.namespace_id,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::cache::test_util::assert_histogram_metric_count;
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
        );

        let none = cache.name(TableId::new(i32::MAX)).await;
        assert_eq!(none, None);
        assert_histogram_metric_count(&catalog.metric_registry, "table_get_by_id", 1);

        let none = cache.name(TableId::new(i32::MAX)).await;
        assert_eq!(none, None);
        assert_histogram_metric_count(&catalog.metric_registry, "table_get_by_id", 1);

        catalog.mock_time_provider().inc(TTL_NON_EXISTING);

        let none = cache.name(TableId::new(i32::MAX)).await;
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
        );

        let none = cache.namespace_id(TableId::new(i32::MAX)).await;
        assert_eq!(none, None);
        assert_histogram_metric_count(&catalog.metric_registry, "table_get_by_id", 1);

        // "non-existing" is cached
        let none = cache.namespace_id(TableId::new(i32::MAX)).await;
        assert_eq!(none, None);
        assert_histogram_metric_count(&catalog.metric_registry, "table_get_by_id", 1);

        // let "non-existing" TTL expire
        catalog.mock_time_provider().inc(TTL_NON_EXISTING);
        let none = cache.namespace_id(TableId::new(i32::MAX)).await;
        assert_eq!(none, None);
        assert_histogram_metric_count(&catalog.metric_registry, "table_get_by_id", 2);
    }

    #[tokio::test]
    async fn test_id() {
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
        );

        // request first ID
        let id1_a = cache
            .id(ns1.namespace.id, t1.name.clone().into())
            .await
            .unwrap();
        assert_eq!(id1_a, t1.id);
        assert_histogram_metric_count(
            &catalog.metric_registry,
            "table_get_by_namespace_and_name",
            1,
        );

        // request a different (2nd) ID
        let id2_a = cache
            .id(ns2.namespace.id, t2.name.clone().into())
            .await
            .unwrap();
        assert_eq!(id2_a, t2.id);
        assert_histogram_metric_count(
            &catalog.metric_registry,
            "table_get_by_namespace_and_name",
            2,
        );

        // first ID is still cached
        let id1_b = cache
            .id(ns1.namespace.id, t1.name.clone().into())
            .await
            .unwrap();
        assert_eq!(id1_b, t1.id);
        assert_histogram_metric_count(
            &catalog.metric_registry,
            "table_get_by_namespace_and_name",
            2,
        );

        // wait for "non-existing" TTL, this must not wipe the existing IDs from cache
        catalog.mock_time_provider().inc(TTL_NON_EXISTING);
        let id1_c = cache
            .id(ns1.namespace.id, t1.name.clone().into())
            .await
            .unwrap();
        assert_eq!(id1_c, t1.id);
        let id2_b = cache
            .id(ns2.namespace.id, t2.name.clone().into())
            .await
            .unwrap();
        assert_eq!(id2_b, t2.id);
        assert_histogram_metric_count(
            &catalog.metric_registry,
            "table_get_by_namespace_and_name",
            2,
        );
    }

    #[tokio::test]
    async fn test_id_non_existing() {
        let catalog = TestCatalog::new();

        let cache = TableCache::new(
            catalog.catalog(),
            BackoffConfig::default(),
            catalog.time_provider(),
        );

        let none = cache
            .id(NamespaceId::new(i32::MAX), Arc::from("table"))
            .await;
        assert_eq!(none, None);
        assert_histogram_metric_count(
            &catalog.metric_registry,
            "table_get_by_namespace_and_name",
            1,
        );

        let none = cache
            .id(NamespaceId::new(i32::MAX), Arc::from("table"))
            .await;
        assert_eq!(none, None);
        assert_histogram_metric_count(
            &catalog.metric_registry,
            "table_get_by_namespace_and_name",
            1,
        );

        catalog.mock_time_provider().inc(TTL_NON_EXISTING);

        let none = cache
            .id(NamespaceId::new(i32::MAX), Arc::from("table"))
            .await;
        assert_eq!(none, None);
        assert_histogram_metric_count(
            &catalog.metric_registry,
            "table_get_by_namespace_and_name",
            2,
        );
    }

    #[tokio::test]
    async fn test_shared_cache() {
        let catalog = TestCatalog::new();

        let ns = catalog.create_namespace("ns").await;
        let t1 = ns.create_table("table1").await.table.clone();
        let t2 = ns.create_table("table2").await.table.clone();
        let t3 = ns.create_table("table3").await.table.clone();
        assert_ne!(t1.id, t2.id);
        assert_ne!(t2.id, t3.id);
        assert_ne!(t3.id, t1.id);

        let cache = TableCache::new(
            catalog.catalog(),
            BackoffConfig::default(),
            catalog.time_provider(),
        );

        cache.name(t1.id).await;
        cache.namespace_id(t2.id).await;
        cache.id(ns.namespace.id, t3.name.into()).await;
        assert_histogram_metric_count(&catalog.metric_registry, "table_get_by_id", 2);
        assert_histogram_metric_count(
            &catalog.metric_registry,
            "table_get_by_namespace_and_name",
            1,
        );

        // `name`, `namespace_id`, and `id` use the same underlying cache
        cache.namespace_id(t1.id).await;
        cache.namespace_id(t3.id).await;
        cache.name(t2.id).await;
        cache.name(t3.id).await;
        cache.id(ns.namespace.id, t1.name.into()).await;
        assert_histogram_metric_count(&catalog.metric_registry, "table_get_by_id", 2);
        assert_histogram_metric_count(
            &catalog.metric_registry,
            "table_get_by_namespace_and_name",
            1,
        );
    }
}
