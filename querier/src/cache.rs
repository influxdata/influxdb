use std::{collections::HashMap, sync::Arc};

use backoff::{Backoff, BackoffConfig};
use iox_catalog::interface::{Catalog, NamespaceId, PartitionId, SequencerId, TableId};
use parking_lot::RwLock;

/// Caches request to the [`Catalog`].
#[derive(Debug)]
pub struct CatalogCache {
    /// Backoff config for IO operations.
    backoff_config: BackoffConfig,

    /// Catalog.
    catalog: Arc<dyn Catalog>,

    /// Partition keys cache for old gen.
    old_gen_partition_key_cache: RwLock<HashMap<PartitionId, Arc<str>>>,

    /// Partition key and sequencer ID cache.
    partition_cache: RwLock<HashMap<PartitionId, (Arc<str>, SequencerId)>>,

    /// Table name and namespace cache.
    table_cache: RwLock<HashMap<TableId, (Arc<str>, NamespaceId)>>,

    /// Namespace name cache.
    namespace_cache: RwLock<HashMap<NamespaceId, Arc<str>>>,
}

impl CatalogCache {
    /// Create empty cache.
    pub fn new(catalog: Arc<dyn Catalog>) -> Self {
        Self {
            backoff_config: BackoffConfig::default(),
            catalog,
            old_gen_partition_key_cache: RwLock::new(HashMap::default()),
            partition_cache: RwLock::new(HashMap::default()),
            table_cache: RwLock::new(HashMap::default()),
            namespace_cache: RwLock::new(HashMap::default()),
        }
    }

    /// Get partition key for old gen.
    ///
    /// This either uses a cached value or -- if required -- creates a fresh string.
    pub async fn old_gen_partition_key(&self, partition_id: PartitionId) -> Arc<str> {
        if let Some(key) = self.old_gen_partition_key_cache.read().get(&partition_id) {
            return Arc::clone(key);
        }

        let (partition_key, sequencer_id) = self.cached_partition(partition_id).await;
        let og_partition_key = Arc::from(format!("{}-{}", sequencer_id.get(), partition_key));

        Arc::clone(
            self.old_gen_partition_key_cache
                .write()
                .entry(partition_id)
                .or_insert(og_partition_key),
        )
    }

    /// Get the partition key and sequencer ID for the given partition ID.
    ///
    /// This either uses a cached value or -- if required -- fetches the mapping from the catalog.
    async fn cached_partition(&self, partition_id: PartitionId) -> (Arc<str>, SequencerId) {
        if let Some((key, id)) = self.partition_cache.read().get(&partition_id) {
            return (Arc::clone(key), *id);
        }

        let partition = Backoff::new(&self.backoff_config)
            .retry_all_errors("get partition_key", || async {
                self.catalog
                    .repositories()
                    .await
                    .partitions()
                    .get_by_id(partition_id)
                    .await
            })
            .await
            .expect("retry forever")
            .expect("partition gone from catalog?!");

        let key = Arc::from(partition.partition_key);

        let mut partition_cache = self.partition_cache.write();
        let (key, id) = partition_cache
            .entry(partition_id)
            .or_insert((key, partition.sequencer_id));
        (Arc::clone(key), *id)
    }

    /// Get the table name and namespace ID for the given table ID.
    ///
    /// This either uses a cached value or -- if required -- fetches the mapping from the catalog.
    async fn cached_table(&self, table_id: TableId) -> (Arc<str>, NamespaceId) {
        if let Some((name, ns)) = self.table_cache.read().get(&table_id) {
            return (Arc::clone(name), *ns);
        }

        let table = Backoff::new(&self.backoff_config)
            .retry_all_errors("get table_name", || async {
                self.catalog
                    .repositories()
                    .await
                    .tables()
                    .get_by_id(table_id)
                    .await
            })
            .await
            .expect("retry forever")
            .expect("table gone from catalog?!");

        let name = Arc::from(table.name);

        let mut table_cache = self.table_cache.write();
        let (name, ns) = table_cache
            .entry(table_id)
            .or_insert((name, table.namespace_id));
        (Arc::clone(name), *ns)
    }

    /// Get the table name for the given table ID.
    ///
    /// This either uses a cached value or -- if required -- fetches the mapping from the catalog.
    pub async fn table_name(&self, table_id: TableId) -> Arc<str> {
        self.cached_table(table_id).await.0
    }

    /// Get the table namespace ID for the given table ID.
    ///
    /// This either uses a cached value or -- if required -- fetches the mapping from the catalog.
    pub async fn table_namespace_id(&self, table_id: TableId) -> NamespaceId {
        self.cached_table(table_id).await.1
    }

    /// Get the namespace name for the given namespace ID.
    ///
    /// This either uses a cached value or -- if required -- fetches the mapping from the catalog.
    pub async fn namespace_name(&self, namespace_id: NamespaceId) -> Arc<str> {
        if let Some(name) = self.namespace_cache.read().get(&namespace_id) {
            return Arc::clone(name);
        }

        let namespace = Backoff::new(&self.backoff_config)
            .retry_all_errors("get namespace_name", || async {
                self.catalog
                    .repositories()
                    .await
                    .namespaces()
                    .get_by_id(namespace_id)
                    .await
            })
            .await
            .expect("retry forever")
            .expect("namespace gone from catalog?!");

        let name = Arc::from(namespace.name);

        Arc::clone(
            self.namespace_cache
                .write()
                .entry(namespace_id)
                .or_insert(name),
        )
    }
}

#[cfg(test)]
mod tests {
    use metric::{Attributes, Metric, U64Histogram};

    use crate::test_util::TestCatalog;

    use super::*;

    #[tokio::test]
    async fn test_namespace_name() {
        let catalog = TestCatalog::new();

        let ns1 = catalog.create_namespace("ns1").await.namespace.clone();
        let ns2 = catalog.create_namespace("ns2").await.namespace.clone();
        assert_ne!(ns1.id, ns2.id);

        let cache = CatalogCache::new(catalog.catalog());

        let name1_a = cache.namespace_name(ns1.id).await;
        assert_eq!(name1_a.as_ref(), ns1.name.as_str());
        assert_metric(&catalog.metric_registry, "namespace_get_by_id", 1);

        let name2 = cache.namespace_name(ns2.id).await;
        assert_eq!(name2.as_ref(), ns2.name.as_str());
        assert_metric(&catalog.metric_registry, "namespace_get_by_id", 2);

        let name1_b = cache.namespace_name(ns1.id).await;
        assert!(Arc::ptr_eq(&name1_a, &name1_b));
        assert_metric(&catalog.metric_registry, "namespace_get_by_id", 2);
    }

    #[tokio::test]
    async fn test_table_name() {
        let catalog = TestCatalog::new();

        let ns = catalog.create_namespace("ns").await;
        let t1 = ns.create_table("table1").await.table.clone();
        let t2 = ns.create_table("table2").await.table.clone();
        assert_ne!(t1.id, t2.id);

        let cache = CatalogCache::new(catalog.catalog());

        let name1_a = cache.table_name(t1.id).await;
        assert_eq!(name1_a.as_ref(), t1.name.as_str());
        assert_metric(&catalog.metric_registry, "table_get_by_id", 1);

        let name2 = cache.table_name(t2.id).await;
        assert_eq!(name2.as_ref(), t2.name.as_str());
        assert_metric(&catalog.metric_registry, "table_get_by_id", 2);

        let name1_b = cache.table_name(t1.id).await;
        assert!(Arc::ptr_eq(&name1_a, &name1_b));
        assert_metric(&catalog.metric_registry, "table_get_by_id", 2);
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

        let cache = CatalogCache::new(catalog.catalog());

        let id1_a = cache.table_namespace_id(t1.id).await;
        assert_eq!(id1_a, t1.namespace_id);
        assert_metric(&catalog.metric_registry, "table_get_by_id", 1);

        let id2 = cache.table_namespace_id(t2.id).await;
        assert_eq!(id2, t2.namespace_id);
        assert_metric(&catalog.metric_registry, "table_get_by_id", 2);

        let id1_b = cache.table_namespace_id(t1.id).await;
        assert_eq!(id1_b, t1.namespace_id);
        assert_metric(&catalog.metric_registry, "table_get_by_id", 2);
    }

    #[tokio::test]
    async fn test_table_shared_cache() {
        let catalog = TestCatalog::new();

        let ns = catalog.create_namespace("ns").await;
        let t1 = ns.create_table("table1").await.table.clone();
        let t2 = ns.create_table("table2").await.table.clone();
        assert_ne!(t1.id, t2.id);

        let cache = CatalogCache::new(catalog.catalog());

        cache.table_name(t1.id).await;
        cache.table_namespace_id(t2.id).await;
        assert_metric(&catalog.metric_registry, "table_get_by_id", 2);

        // `table_name` and `table_namespace_id` use the same underlying cache
        cache.table_namespace_id(t1.id).await;
        cache.table_name(t2.id).await;
        assert_metric(&catalog.metric_registry, "table_get_by_id", 2);
    }

    #[tokio::test]
    async fn test_old_partition_key() {
        let catalog = TestCatalog::new();

        let ns = catalog.create_namespace("ns").await;
        let t = ns.create_table("table").await;
        let p11 = t.create_partition("k1", 1).await.partition.clone();
        let p12 = t.create_partition("k1", 2).await.partition.clone();
        let p21 = t.create_partition("k2", 1).await.partition.clone();
        let p22 = t.create_partition("k2", 2).await.partition.clone();

        let cache = CatalogCache::new(catalog.catalog());

        let name11_a = cache.old_gen_partition_key(p11.id).await;
        assert_eq!(name11_a.as_ref(), "1-k1");
        assert_metric(&catalog.metric_registry, "partition_get_by_id", 1);

        let name12 = cache.old_gen_partition_key(p12.id).await;
        assert_eq!(name12.as_ref(), "2-k1");
        assert_metric(&catalog.metric_registry, "partition_get_by_id", 2);

        let name21 = cache.old_gen_partition_key(p21.id).await;
        assert_eq!(name21.as_ref(), "1-k2");
        assert_metric(&catalog.metric_registry, "partition_get_by_id", 3);

        let name22 = cache.old_gen_partition_key(p22.id).await;
        assert_eq!(name22.as_ref(), "2-k2");
        assert_metric(&catalog.metric_registry, "partition_get_by_id", 4);

        let name11_b = cache.old_gen_partition_key(p11.id).await;
        assert!(Arc::ptr_eq(&name11_a, &name11_b));
        assert_metric(&catalog.metric_registry, "partition_get_by_id", 4);
    }

    fn assert_metric(metrics: &metric::Registry, name: &'static str, n: u64) {
        let histogram = metrics
            .get_instrument::<Metric<U64Histogram>>("catalog_op_duration_ms")
            .expect("failed to read metric")
            .get_observer(&Attributes::from(&[("op", name), ("result", "success")]))
            .expect("failed to get observer")
            .fetch();

        let hit_count = histogram.buckets.iter().fold(0, |acc, v| acc + v.count);
        assert_eq!(hit_count, n);
    }
}
