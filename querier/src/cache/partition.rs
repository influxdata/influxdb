//! Partition cache.

use backoff::{Backoff, BackoffConfig};
use cache_system::{
    backend::policy::{
        lru::{LruPolicy, ResourcePool},
        remove_if::{RemoveIfHandle, RemoveIfPolicy},
        PolicyBackend,
    },
    cache::{driver::CacheDriver, metrics::CacheWithMetrics, Cache},
    loader::{metrics::MetricsLoader, FunctionLoader},
    resource_consumption::FunctionEstimator,
};
use data_types::{PartitionId, ShardId};
use iox_catalog::interface::Catalog;
use iox_time::TimeProvider;
use schema::sort::SortKey;
use std::{collections::HashMap, mem::size_of_val, sync::Arc};
use trace::span::Span;

use super::ram::RamSize;

const CACHE_ID: &str = "partition";

type CacheT = Box<
    dyn Cache<
        K = PartitionId,
        V = CachedPartition,
        GetExtra = ((), Option<Span>),
        PeekExtra = ((), Option<Span>),
    >,
>;

/// Cache for partition-related attributes.
#[derive(Debug)]
pub struct PartitionCache {
    cache: CacheT,
    remove_if_handle: RemoveIfHandle<PartitionId, CachedPartition>,
}

impl PartitionCache {
    /// Create new empty cache.
    pub fn new(
        catalog: Arc<dyn Catalog>,
        backoff_config: BackoffConfig,
        time_provider: Arc<dyn TimeProvider>,
        metric_registry: &metric::Registry,
        ram_pool: Arc<ResourcePool<RamSize>>,
        testing: bool,
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
                        shard_id: partition.shard_id,
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
            testing,
        ));

        let mut backend = PolicyBackend::new(Box::new(HashMap::new()), Arc::clone(&time_provider));
        let (policy_constructor, remove_if_handle) =
            RemoveIfPolicy::create_constructor_and_handle(CACHE_ID, metric_registry);
        backend.add_policy(policy_constructor);
        backend.add_policy(LruPolicy::new(
            ram_pool,
            CACHE_ID,
            Arc::new(FunctionEstimator::new(|k, v: &CachedPartition| {
                RamSize(size_of_val(k) + size_of_val(v) + v.size())
            })),
        ));

        let cache = CacheDriver::new(loader, backend);
        let cache = Box::new(CacheWithMetrics::new(
            cache,
            CACHE_ID,
            time_provider,
            metric_registry,
        ));

        Self {
            cache,
            remove_if_handle,
        }
    }

    /// Get shard ID.
    pub async fn shard_id(&self, partition_id: PartitionId, span: Option<Span>) -> ShardId {
        self.cache.get(partition_id, ((), span)).await.shard_id
    }

    /// Get sort key
    ///
    /// Expire partition if the cached sort key does NOT cover the given set of columns.
    pub async fn sort_key(
        &self,
        partition_id: PartitionId,
        should_cover: &[&str],
        span: Option<Span>,
    ) -> Arc<Option<SortKey>> {
        self.remove_if_handle
            .remove_if(&partition_id, |cached_partition| {
                if let Some(sort_key) = cached_partition.sort_key.as_ref().as_ref() {
                    should_cover.iter().any(|col| !sort_key.contains(col))
                } else {
                    // no sort key at all => need to update if there is anything to cover
                    !should_cover.is_empty()
                }
            });

        self.cache.get(partition_id, ((), span)).await.sort_key
    }
}

#[derive(Debug, Clone)]
struct CachedPartition {
    shard_id: ShardId,
    sort_key: Arc<Option<SortKey>>,
}

impl CachedPartition {
    /// RAM-bytes EXCLUDING `self`.
    fn size(&self) -> usize {
        // Arc heap allocation
        size_of_val(self.sort_key.as_ref()) +
        // Arc content
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
    async fn test_shard_id() {
        let catalog = TestCatalog::new();

        let ns = catalog.create_namespace("ns").await;
        let t = ns.create_table("table").await;
        let s1 = ns.create_shard(1).await;
        let s2 = ns.create_shard(2).await;
        let p1 = t
            .with_shard(&s1)
            .create_partition("k1")
            .await
            .partition
            .clone();
        let p2 = t
            .with_shard(&s2)
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
            true,
        );

        let id1 = cache.shard_id(p1.id, None).await;
        assert_eq!(id1, s1.shard.id);
        assert_histogram_metric_count(&catalog.metric_registry, "partition_get_by_id", 1);

        let id2 = cache.shard_id(p2.id, None).await;
        assert_eq!(id2, s2.shard.id);
        assert_histogram_metric_count(&catalog.metric_registry, "partition_get_by_id", 2);

        let id1 = cache.shard_id(p1.id, None).await;
        assert_eq!(id1, s1.shard.id);
        assert_histogram_metric_count(&catalog.metric_registry, "partition_get_by_id", 2);
    }

    #[tokio::test]
    async fn test_sort_key() {
        let catalog = TestCatalog::new();

        let ns = catalog.create_namespace("ns").await;
        let t = ns.create_table("table").await;
        let s1 = ns.create_shard(1).await;
        let s2 = ns.create_shard(2).await;
        let p1 = t
            .with_shard(&s1)
            .create_partition_with_sort_key("k1", &["tag", "time"])
            .await
            .partition
            .clone();
        let p2 = t
            .with_shard(&s2)
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
            true,
        );

        let sort_key1 = cache.sort_key(p1.id, &Vec::new(), None).await;
        assert_eq!(sort_key1.as_ref(), &p1.sort_key());
        assert_histogram_metric_count(&catalog.metric_registry, "partition_get_by_id", 1);

        let sort_key2 = cache.sort_key(p2.id, &Vec::new(), None).await;
        assert_eq!(sort_key2.as_ref(), &p2.sort_key());
        assert_histogram_metric_count(&catalog.metric_registry, "partition_get_by_id", 2);

        let sort_key1 = cache.sort_key(p1.id, &Vec::new(), None).await;
        assert_eq!(sort_key1.as_ref(), &p1.sort_key());
        assert_histogram_metric_count(&catalog.metric_registry, "partition_get_by_id", 2);
    }

    #[tokio::test]
    async fn test_cache_sharing() {
        let catalog = TestCatalog::new();

        let ns = catalog.create_namespace("ns").await;
        let t = ns.create_table("table").await;
        let s1 = ns.create_shard(1).await;
        let s2 = ns.create_shard(2).await;
        let p1 = t
            .with_shard(&s1)
            .create_partition_with_sort_key("k1", &["tag", "time"])
            .await
            .partition
            .clone();
        let p2 = t
            .with_shard(&s2)
            .create_partition("k2")
            .await
            .partition
            .clone();
        let p3 = t
            .with_shard(&s2)
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
            true,
        );

        cache.shard_id(p2.id, None).await;
        cache.sort_key(p3.id, &Vec::new(), None).await;
        assert_histogram_metric_count(&catalog.metric_registry, "partition_get_by_id", 2);

        cache.shard_id(p1.id, None).await;
        cache.sort_key(p2.id, &Vec::new(), None).await;
        assert_histogram_metric_count(&catalog.metric_registry, "partition_get_by_id", 3);

        cache.sort_key(p1.id, &Vec::new(), None).await;
        cache.shard_id(p2.id, None).await;
        assert_histogram_metric_count(&catalog.metric_registry, "partition_get_by_id", 3);
    }

    #[tokio::test]
    async fn test_expiration() {
        let catalog = TestCatalog::new();

        let ns = catalog.create_namespace("ns").await;
        let t = ns.create_table("table").await;
        let s = ns.create_shard(1).await;
        let p = t.with_shard(&s).create_partition("k1").await;
        let p_id = p.partition.id;
        let p_sort_key = p.partition.sort_key();

        let cache = PartitionCache::new(
            catalog.catalog(),
            BackoffConfig::default(),
            catalog.time_provider(),
            &catalog.metric_registry(),
            test_ram_pool(),
            true,
        );

        let sort_key = cache.sort_key(p_id, &Vec::new(), None).await;
        assert_eq!(sort_key.as_ref(), &p_sort_key);
        assert_histogram_metric_count(&catalog.metric_registry, "partition_get_by_id", 1);

        // requesting nother will not expire
        assert!(p_sort_key.is_none());
        let sort_key = cache.sort_key(p_id, &Vec::new(), None).await;
        assert_eq!(sort_key.as_ref(), &p_sort_key);
        assert_histogram_metric_count(&catalog.metric_registry, "partition_get_by_id", 1);

        // but requesting something will expire
        let sort_key = cache.sort_key(p_id, &["foo"], None).await;
        assert_eq!(sort_key.as_ref(), &p_sort_key);
        assert_histogram_metric_count(&catalog.metric_registry, "partition_get_by_id", 2);

        // set sort key
        let p = p
            .update_sort_key(SortKey::from_columns(["foo", "bar"]))
            .await;

        // expire & fetch
        let p_sort_key = p.partition.sort_key();
        let sort_key = cache.sort_key(p_id, &["foo"], None).await;
        assert_eq!(sort_key.as_ref(), &p_sort_key);
        assert_histogram_metric_count(&catalog.metric_registry, "partition_get_by_id", 3);

        // subsets and the full key don't expire
        for should_cover in [Vec::new(), vec!["foo"], vec!["bar"], vec!["foo", "bar"]] {
            let sort_key = cache.sort_key(p_id, &should_cover, None).await;
            assert_eq!(sort_key.as_ref(), &p_sort_key);
            assert_histogram_metric_count(&catalog.metric_registry, "partition_get_by_id", 3);
        }

        // unknown columns expire
        let sort_key = cache.sort_key(p_id, &["foo", "x"], None).await;
        assert_eq!(sort_key.as_ref(), &p_sort_key);
        assert_histogram_metric_count(&catalog.metric_registry, "partition_get_by_id", 4);
    }
}
