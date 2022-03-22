//! Partition cache.
use std::{collections::HashMap, sync::Arc};

use backoff::{Backoff, BackoffConfig};
use data_types2::PartitionId;
use iox_catalog::interface::Catalog;

use crate::cache_system::{driver::Cache, loader::FunctionLoader};

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
                    old_gen_partition_key: Arc::from(format!(
                        "{}-{}",
                        partition.sequencer_id.get(),
                        partition.partition_key
                    )),
                }
            }
        }));
        let backend = Box::new(HashMap::new());

        Self {
            cache: Cache::new(loader, backend),
        }
    }

    /// Get partition key for old gen.
    ///
    /// This either uses a cached value or -- if required -- creates a fresh string.
    pub async fn old_gen_partition_key(&self, partition_id: PartitionId) -> Arc<str> {
        self.cache.get(partition_id).await.old_gen_partition_key
    }
}

#[derive(Debug, Clone)]
struct CachedPartition {
    old_gen_partition_key: Arc<str>,
}

#[cfg(test)]
mod tests {
    use crate::cache::test_util::assert_histogram_metric_count;
    use iox_tests::util::TestCatalog;

    use super::*;

    #[tokio::test]
    async fn test_old_partition_key() {
        let catalog = TestCatalog::new();

        let ns = catalog.create_namespace("ns").await;
        let t = ns.create_table("table").await;
        let s1 = ns.create_sequencer(1).await;
        let s2 = ns.create_sequencer(2).await;
        let p11 = t
            .with_sequencer(&s1)
            .create_partition("k1")
            .await
            .partition
            .clone();
        let p12 = t
            .with_sequencer(&s2)
            .create_partition("k1")
            .await
            .partition
            .clone();
        let p21 = t
            .with_sequencer(&s1)
            .create_partition("k2")
            .await
            .partition
            .clone();
        let p22 = t
            .with_sequencer(&s2)
            .create_partition("k2")
            .await
            .partition
            .clone();

        let cache = PartitionCache::new(catalog.catalog(), BackoffConfig::default());

        let name11_a = cache.old_gen_partition_key(p11.id).await;
        assert_eq!(name11_a.as_ref(), "1-k1");
        assert_histogram_metric_count(&catalog.metric_registry, "partition_get_by_id", 1);

        let name12 = cache.old_gen_partition_key(p12.id).await;
        assert_eq!(name12.as_ref(), "2-k1");
        assert_histogram_metric_count(&catalog.metric_registry, "partition_get_by_id", 2);

        let name21 = cache.old_gen_partition_key(p21.id).await;
        assert_eq!(name21.as_ref(), "1-k2");
        assert_histogram_metric_count(&catalog.metric_registry, "partition_get_by_id", 3);

        let name22 = cache.old_gen_partition_key(p22.id).await;
        assert_eq!(name22.as_ref(), "2-k2");
        assert_histogram_metric_count(&catalog.metric_registry, "partition_get_by_id", 4);

        let name11_b = cache.old_gen_partition_key(p11.id).await;
        assert!(Arc::ptr_eq(&name11_a, &name11_b));
        assert_histogram_metric_count(&catalog.metric_registry, "partition_get_by_id", 4);
    }
}
