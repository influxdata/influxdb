use std::sync::Arc;

use backoff::{Backoff, BackoffConfig};
use data_types2::{NamespaceId, TableId};
use iox_catalog::interface::Catalog;

use crate::cache_system::{driver::Cache, loader::FunctionLoader};

/// Cache for table-related queries.
#[derive(Debug)]
pub struct TableCache {
    cache: Cache<TableId, CachedTable>,
}

impl TableCache {
    /// Create new empty cache.
    pub fn new(catalog: Arc<dyn Catalog>, backoff_config: BackoffConfig) -> Self {
        let loader = Arc::new(FunctionLoader::new(move |table_id| {
            let catalog = Arc::clone(&catalog);
            let backoff_config = backoff_config.clone();

            async move {
                let table = Backoff::new(&backoff_config)
                    .retry_all_errors("get table_name", || async {
                        catalog
                            .repositories()
                            .await
                            .tables()
                            .get_by_id(table_id)
                            .await
                    })
                    .await
                    .expect("retry forever")
                    .expect("table gone from catalog?!");

                CachedTable {
                    name: Arc::from(table.name),
                    namespace_id: table.namespace_id,
                }
            }
        }));

        Self {
            cache: Cache::new(loader),
        }
    }

    /// Get the table name for the given table ID.
    ///
    /// This either uses a cached value or -- if required -- fetches the mapping from the catalog.
    pub async fn name(&self, table_id: TableId) -> Arc<str> {
        self.cache.get(table_id).await.name
    }

    /// Get the table namespace ID for the given table ID.
    ///
    /// This either uses a cached value or -- if required -- fetches the mapping from the catalog.
    pub async fn namespace_id(&self, table_id: TableId) -> NamespaceId {
        self.cache.get(table_id).await.namespace_id
    }
}

#[derive(Debug, Clone)]
struct CachedTable {
    name: Arc<str>,
    namespace_id: NamespaceId,
}

#[cfg(test)]
mod tests {
    use crate::{cache::test_util::assert_histogram_metric_count, test_util::TestCatalog};

    use super::*;

    #[tokio::test]
    async fn test_table_name() {
        let catalog = TestCatalog::new();

        let ns = catalog.create_namespace("ns").await;
        let t1 = ns.create_table("table1").await.table.clone();
        let t2 = ns.create_table("table2").await.table.clone();
        assert_ne!(t1.id, t2.id);

        let cache = TableCache::new(catalog.catalog(), BackoffConfig::default());

        let name1_a = cache.name(t1.id).await;
        assert_eq!(name1_a.as_ref(), t1.name.as_str());
        assert_histogram_metric_count(&catalog.metric_registry, "table_get_by_id", 1);

        let name2 = cache.name(t2.id).await;
        assert_eq!(name2.as_ref(), t2.name.as_str());
        assert_histogram_metric_count(&catalog.metric_registry, "table_get_by_id", 2);

        let name1_b = cache.name(t1.id).await;
        assert!(Arc::ptr_eq(&name1_a, &name1_b));
        assert_histogram_metric_count(&catalog.metric_registry, "table_get_by_id", 2);
    }

    #[tokio::test]
    async fn test_table_namespace_id() {
        let catalog = TestCatalog::new();

        let ns2 = catalog.create_namespace("ns1").await;
        let ns1 = catalog.create_namespace("ns2").await;
        let t1 = ns1.create_table("table1").await.table.clone();
        let t2 = ns2.create_table("table2").await.table.clone();
        assert_ne!(t1.id, t2.id);
        assert_ne!(t1.namespace_id, t2.namespace_id);

        let cache = TableCache::new(catalog.catalog(), BackoffConfig::default());

        let id1_a = cache.namespace_id(t1.id).await;
        assert_eq!(id1_a, t1.namespace_id);
        assert_histogram_metric_count(&catalog.metric_registry, "table_get_by_id", 1);

        let id2 = cache.namespace_id(t2.id).await;
        assert_eq!(id2, t2.namespace_id);
        assert_histogram_metric_count(&catalog.metric_registry, "table_get_by_id", 2);

        let id1_b = cache.namespace_id(t1.id).await;
        assert_eq!(id1_b, t1.namespace_id);
        assert_histogram_metric_count(&catalog.metric_registry, "table_get_by_id", 2);
    }

    #[tokio::test]
    async fn test_table_shared_cache() {
        let catalog = TestCatalog::new();

        let ns = catalog.create_namespace("ns").await;
        let t1 = ns.create_table("table1").await.table.clone();
        let t2 = ns.create_table("table2").await.table.clone();
        assert_ne!(t1.id, t2.id);

        let cache = TableCache::new(catalog.catalog(), BackoffConfig::default());

        cache.name(t1.id).await;
        cache.namespace_id(t2.id).await;
        assert_histogram_metric_count(&catalog.metric_registry, "table_get_by_id", 2);

        // `name` and `namespace_id` use the same underlying cache
        cache.namespace_id(t1.id).await;
        cache.name(t2.id).await;
        assert_histogram_metric_count(&catalog.metric_registry, "table_get_by_id", 2);
    }
}
