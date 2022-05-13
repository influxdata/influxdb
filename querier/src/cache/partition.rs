//! Partition cache.

use backoff::{Backoff, BackoffConfig};
use cache_system::{driver::Cache, loader::FunctionLoader};
use data_types::{PartitionId, SequencerId};
use iox_catalog::interface::Catalog;
use schema::sort::SortKey;
use std::{collections::HashMap, sync::Arc};

/// Cache for partition-related attributes.
#[derive(Debug)]
pub struct PartitionCache {
    cache: Cache<PartitionId, CachedPartition>,
}

impl PartitionCache {
    /// Create new empty cache.
    pub fn new(catalog: Arc<dyn Catalog>, backoff_config: BackoffConfig) -> Self {
        let loader = Arc::new(FunctionLoader::new(move |partition_id| {
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
                    sort_key: partition.sort_key(),
                }
            }
        }));
        let backend = Box::new(HashMap::new());

        Self {
            cache: Cache::new(loader, backend),
        }
    }

    /// Get sequencer ID.
    pub async fn sequencer_id(&self, partition_id: PartitionId) -> SequencerId {
        self.cache.get(partition_id).await.sequencer_id
    }

    /// Get sort key
    pub async fn sort_key(&self, partition_id: PartitionId) -> Option<SortKey> {
        self.cache.get(partition_id).await.sort_key
    }
}

#[derive(Debug, Clone)]
struct CachedPartition {
    sequencer_id: SequencerId,
    sort_key: Option<SortKey>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cache::test_util::assert_histogram_metric_count;
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

        let cache = PartitionCache::new(catalog.catalog(), BackoffConfig::default());

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

        let cache = PartitionCache::new(catalog.catalog(), BackoffConfig::default());

        let sort_key1 = cache.sort_key(p1.id).await;
        assert_eq!(sort_key1, p1.sort_key());
        assert_histogram_metric_count(&catalog.metric_registry, "partition_get_by_id", 1);

        let sort_key2 = cache.sort_key(p2.id).await;
        assert_eq!(sort_key2, p2.sort_key());
        assert_histogram_metric_count(&catalog.metric_registry, "partition_get_by_id", 2);

        let sort_key1 = cache.sort_key(p1.id).await;
        assert_eq!(sort_key1, p1.sort_key());
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

        let cache = PartitionCache::new(catalog.catalog(), BackoffConfig::default());

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
}
