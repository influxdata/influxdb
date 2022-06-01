//! Partition cache.

use backoff::{Backoff, BackoffConfig};
use cache_system::{
    backend::{
        lru::{LruBackend, ResourcePool},
        resource_consumption::FunctionEstimator,
        shared::SharedBackend,
    },
    driver::Cache,
    loader::{metrics::MetricsLoader, FunctionLoader},
};
use data_types::{PartitionId, SequencerId};
use iox_catalog::interface::Catalog;
use iox_time::TimeProvider;
use schema::sort::SortKey;
use std::{
    collections::{HashMap, HashSet},
    mem::size_of_val,
    sync::Arc,
};

use super::ram::RamSize;

const CACHE_ID: &str = "partition";

/// Cache for partition-related attributes.
#[derive(Debug)]
pub struct PartitionCache {
    cache: Cache<PartitionId, CachedPartition, ()>,
    backend: SharedBackend<PartitionId, CachedPartition>,
}

impl PartitionCache {
    /// Create new empty cache.
    pub fn new(
        catalog: Arc<dyn Catalog>,
        backoff_config: BackoffConfig,
        time_provider: Arc<dyn TimeProvider>,
        metric_registry: &metric::Registry,
        ram_pool: Arc<ResourcePool<RamSize>>,
    ) -> Self {
        let loader = Box::new(FunctionLoader::new(
            move |partition_id: PartitionId, _extra: ()| {
                let catalog = Arc::clone(&catalog);
                let backoff_config = backoff_config.clone();

                async move {
                    let partition = Backoff::new(&backoff_config)
                        .retry_all_errors("get partition_key", || async {
                            catalog
                                .repositories()
                                .await
                                .partitions()
                                .get_by_id(partition_id)
                                .await
                        })
                        .await
                        .expect("retry forever")
                        .expect("partition gone from catalog?!");

                    CachedPartition {
                        sequencer_id: partition.sequencer_id,
                        sort_key: Arc::new(partition.sort_key()),
                    }
                }
            },
        ));
        let loader = Arc::new(MetricsLoader::new(
            loader,
            CACHE_ID,
            Arc::clone(&time_provider),
            metric_registry,
        ));

        let backend = Box::new(HashMap::new());
        let backend = Box::new(LruBackend::new(
            backend,
            ram_pool,
            CACHE_ID,
            Arc::new(FunctionEstimator::new(|k, v: &CachedPartition| {
                RamSize(size_of_val(k) + size_of_val(v) + v.size())
            })),
        ));
        let backend = SharedBackend::new(backend);

        Self {
            cache: Cache::new(loader, Box::new(backend.clone())),
            backend,
        }
    }

    /// Get sequencer ID.
    pub async fn sequencer_id(&self, partition_id: PartitionId) -> SequencerId {
        self.cache.get(partition_id, ()).await.sequencer_id
    }

    /// Get sort key
    pub async fn sort_key(&self, partition_id: PartitionId) -> Arc<Option<SortKey>> {
        self.cache.get(partition_id, ()).await.sort_key
    }

    /// Expire partition if the cached sort key does NOT cover the given set of columns.
    pub fn expire_if_sort_key_does_not_cover(
        &self,
        partition_id: PartitionId,
        columns: &HashSet<String>,
    ) {
        self.backend.remove_if(&partition_id, |cached_partition| {
            if let Some(sort_key) = cached_partition.sort_key.as_ref().as_ref() {
                let covered: HashSet<_> = sort_key
                    .iter()
                    .map(|(col, _options)| Arc::clone(col))
                    .collect();
                columns.iter().any(|col| !covered.contains(col.as_str()))
            } else {
                // no sort key at all => need to update
                true
            }
        });
    }
}

#[derive(Debug, Clone)]
struct CachedPartition {
    sequencer_id: SequencerId,
    sort_key: Arc<Option<SortKey>>,
}

impl CachedPartition {
    /// RAM-bytes EXCLUDING `self`.
    fn size(&self) -> usize {
        self.sort_key
            .as_ref()
            .as_ref()
            .map(|sk| sk.size() - size_of_val(sk))
            .unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cache::{ram::test_util::test_ram_pool, test_util::assert_histogram_metric_count};
    use iox_tests::util::TestCatalog;

    #[tokio::test]
    async fn test_sequencer_id() {
        let catalog = TestCatalog::new();

        let ns = catalog.create_namespace("ns").await;
        let t = ns.create_table("table").await;
        let s1 = ns.create_sequencer(1).await;
        let s2 = ns.create_sequencer(2).await;
        let p1 = t
            .with_sequencer(&s1)
            .create_partition("k1")
            .await
            .partition
            .clone();
        let p2 = t
            .with_sequencer(&s2)
            .create_partition("k2")
            .await
            .partition
            .clone();

        let cache = PartitionCache::new(
            catalog.catalog(),
            BackoffConfig::default(),
            catalog.time_provider(),
            &catalog.metric_registry(),
            test_ram_pool(),
        );

        let id1 = cache.sequencer_id(p1.id).await;
        assert_eq!(id1, s1.sequencer.id);
        assert_histogram_metric_count(&catalog.metric_registry, "partition_get_by_id", 1);

        let id2 = cache.sequencer_id(p2.id).await;
        assert_eq!(id2, s2.sequencer.id);
        assert_histogram_metric_count(&catalog.metric_registry, "partition_get_by_id", 2);

        let id1 = cache.sequencer_id(p1.id).await;
        assert_eq!(id1, s1.sequencer.id);
        assert_histogram_metric_count(&catalog.metric_registry, "partition_get_by_id", 2);
    }

    #[tokio::test]
    async fn test_sort_key() {
        let catalog = TestCatalog::new();

        let ns = catalog.create_namespace("ns").await;
        let t = ns.create_table("table").await;
        let s1 = ns.create_sequencer(1).await;
        let s2 = ns.create_sequencer(2).await;
        let p1 = t
            .with_sequencer(&s1)
            .create_partition_with_sort_key("k1", "tag,time")
            .await
            .partition
            .clone();
        let p2 = t
            .with_sequencer(&s2)
            .create_partition("k2") // no sort key
            .await
            .partition
            .clone();

        let cache = PartitionCache::new(
            catalog.catalog(),
            BackoffConfig::default(),
            catalog.time_provider(),
            &catalog.metric_registry(),
            test_ram_pool(),
        );

        let sort_key1 = cache.sort_key(p1.id).await;
        assert_eq!(sort_key1.as_ref(), &p1.sort_key());
        assert_histogram_metric_count(&catalog.metric_registry, "partition_get_by_id", 1);

        let sort_key2 = cache.sort_key(p2.id).await;
        assert_eq!(sort_key2.as_ref(), &p2.sort_key());
        assert_histogram_metric_count(&catalog.metric_registry, "partition_get_by_id", 2);

        let sort_key1 = cache.sort_key(p1.id).await;
        assert_eq!(sort_key1.as_ref(), &p1.sort_key());
        assert_histogram_metric_count(&catalog.metric_registry, "partition_get_by_id", 2);
    }

    #[tokio::test]
    async fn test_cache_sharing() {
        let catalog = TestCatalog::new();

        let ns = catalog.create_namespace("ns").await;
        let t = ns.create_table("table").await;
        let s1 = ns.create_sequencer(1).await;
        let s2 = ns.create_sequencer(2).await;
        let p1 = t
            .with_sequencer(&s1)
            .create_partition_with_sort_key("k1", "tag,time")
            .await
            .partition
            .clone();
        let p2 = t
            .with_sequencer(&s2)
            .create_partition("k2")
            .await
            .partition
            .clone();
        let p3 = t
            .with_sequencer(&s2)
            .create_partition("k3")
            .await
            .partition
            .clone();

        let cache = PartitionCache::new(
            catalog.catalog(),
            BackoffConfig::default(),
            catalog.time_provider(),
            &catalog.metric_registry(),
            test_ram_pool(),
        );

        cache.sequencer_id(p2.id).await;
        cache.sort_key(p3.id).await;
        assert_histogram_metric_count(&catalog.metric_registry, "partition_get_by_id", 2);

        cache.sequencer_id(p1.id).await;
        cache.sort_key(p2.id).await;
        assert_histogram_metric_count(&catalog.metric_registry, "partition_get_by_id", 3);

        cache.sort_key(p1.id).await;
        cache.sequencer_id(p2.id).await;
        assert_histogram_metric_count(&catalog.metric_registry, "partition_get_by_id", 3);
    }

    #[tokio::test]
    async fn test_expiration() {
        let catalog = TestCatalog::new();

        let ns = catalog.create_namespace("ns").await;
        let t = ns.create_table("table").await;
        let s = ns.create_sequencer(1).await;
        let p = t.with_sequencer(&s).create_partition("k1").await;
        let p_id = p.partition.id;
        let p_sort_key = p.partition.sort_key();

        let cache = PartitionCache::new(
            catalog.catalog(),
            BackoffConfig::default(),
            catalog.time_provider(),
            &catalog.metric_registry(),
            test_ram_pool(),
        );

        let sort_key = cache.sort_key(p_id).await;
        assert_eq!(sort_key.as_ref(), &p_sort_key);
        assert_histogram_metric_count(&catalog.metric_registry, "partition_get_by_id", 1);

        // non-existing sort keys always expire
        assert!(p_sort_key.is_none());
        cache.expire_if_sort_key_does_not_cover(p_id, &HashSet::new());
        let sort_key = cache.sort_key(p_id).await;
        assert_eq!(sort_key.as_ref(), &p_sort_key);
        assert_histogram_metric_count(&catalog.metric_registry, "partition_get_by_id", 2);

        // set sort key
        let p = p
            .update_sort_key(SortKey::from_columns(["foo", "bar"]))
            .await;

        // just changing the sort key doesn't expire, we need a signal
        let sort_key = cache.sort_key(p_id).await;
        assert_eq!(sort_key.as_ref(), &p_sort_key);
        assert_histogram_metric_count(&catalog.metric_registry, "partition_get_by_id", 2);

        // expire
        let p_sort_key = p.partition.sort_key();
        cache.expire_if_sort_key_does_not_cover(p_id, &HashSet::from([String::from("foo")]));
        let sort_key = cache.sort_key(p_id).await;
        assert_eq!(sort_key.as_ref(), &p_sort_key);
        assert_histogram_metric_count(&catalog.metric_registry, "partition_get_by_id", 3);

        // subsets and the full key don't expire
        cache.expire_if_sort_key_does_not_cover(p_id, &HashSet::new());
        cache.expire_if_sort_key_does_not_cover(p_id, &HashSet::from([String::from("foo")]));
        cache.expire_if_sort_key_does_not_cover(p_id, &HashSet::from([String::from("bar")]));
        cache.expire_if_sort_key_does_not_cover(
            p_id,
            &HashSet::from([String::from("foo"), String::from("bar")]),
        );
        let sort_key = cache.sort_key(p_id).await;
        assert_eq!(sort_key.as_ref(), &p_sort_key);
        assert_histogram_metric_count(&catalog.metric_registry, "partition_get_by_id", 3);

        // unknown columns expire
        cache.expire_if_sort_key_does_not_cover(
            p_id,
            &HashSet::from([String::from("foo"), String::from("x")]),
        );
        let sort_key = cache.sort_key(p_id).await;
        assert_eq!(sort_key.as_ref(), &p_sort_key);
        assert_histogram_metric_count(&catalog.metric_registry, "partition_get_by_id", 4);
    }
}
