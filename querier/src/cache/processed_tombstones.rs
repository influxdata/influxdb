//! Processed tombstone cache.
use std::{collections::HashMap, sync::Arc, time::Duration};

use backoff::{Backoff, BackoffConfig};
use data_types2::{ParquetFileId, TombstoneId};
use iox_catalog::interface::Catalog;
use time::TimeProvider;

use crate::cache_system::{
    backend::ttl::{TtlBackend, TtlProvider},
    driver::Cache,
    loader::FunctionLoader,
};

/// Duration to keep "tombstone is NOT processed yet".
///
/// Marking tombstones as processed is a mere optimization, so we can keep this cache entry for a while.
pub const TTL_NOT_PROCESSED: Duration = Duration::from_secs(100);

/// Cache for processed tombstones.
#[derive(Debug)]
pub struct ProcessedTombstonesCache {
    cache: Cache<(ParquetFileId, TombstoneId), bool>,
}

impl ProcessedTombstonesCache {
    /// Create new empty cache.
    pub fn new(
        catalog: Arc<dyn Catalog>,
        backoff_config: BackoffConfig,
        time_provider: Arc<dyn TimeProvider>,
    ) -> Self {
        let loader = Arc::new(FunctionLoader::new(
            move |(parquet_file_id, tombstone_id)| {
                let catalog = Arc::clone(&catalog);
                let backoff_config = backoff_config.clone();

                async move {
                    Backoff::new(&backoff_config)
                        .retry_all_errors("processed tombstone exists", || async {
                            catalog
                                .repositories()
                                .await
                                .processed_tombstones()
                                .exist(parquet_file_id, tombstone_id)
                                .await
                        })
                        .await
                        .expect("retry forever")
                }
            },
        ));

        let backend = Box::new(HashMap::new());
        let backend = Box::new(TtlBackend::new(
            backend,
            Arc::new(KeepExistsForever {}),
            time_provider,
        ));

        Self {
            cache: Cache::new(loader, backend),
        }
    }

    /// Check if the specified tombstone is mark as "processed" for the given parquet file.
    pub async fn exists(&self, parquet_file_id: ParquetFileId, tombstone_id: TombstoneId) -> bool {
        self.cache.get((parquet_file_id, tombstone_id)).await
    }
}

#[derive(Debug)]
struct KeepExistsForever;

impl TtlProvider for KeepExistsForever {
    type K = (ParquetFileId, TombstoneId);
    type V = bool;

    fn expires_in(&self, _k: &Self::K, v: &Self::V) -> Option<Duration> {
        if *v {
            // keep forever
            None
        } else {
            // marking tombstones as processed is a mere optimization, so we can keep this cache entry for a while
            Some(TTL_NOT_PROCESSED)
        }
    }
}

#[cfg(test)]
mod tests {
    use iox_tests::util::TestCatalog;
    use time::{MockProvider, Time};

    use crate::cache::test_util::assert_histogram_metric_count;

    use super::*;

    #[tokio::test]
    async fn test() {
        let catalog = TestCatalog::new();

        let ns = catalog.create_namespace("ns").await;
        let table = ns.create_table("table").await;
        let sequencer = ns.create_sequencer(1).await;
        let partition = table.with_sequencer(&sequencer).create_partition("k").await;

        let file1 = partition.create_parquet_file("table foo=1 11").await;
        let file2 = partition.create_parquet_file("table foo=1 11").await;
        let ts1 = table
            .with_sequencer(&sequencer)
            .create_tombstone(1, 1, 10, "foo=1")
            .await;
        let ts2 = table
            .with_sequencer(&sequencer)
            .create_tombstone(2, 1, 10, "foo=1")
            .await;

        ts1.mark_processed(&file1).await;

        let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
        let cache = ProcessedTombstonesCache::new(
            catalog.catalog(),
            BackoffConfig::default(),
            Arc::clone(&time_provider) as _,
        );

        assert!(cache.exists(file1.parquet_file.id, ts1.tombstone.id).await);
        assert!(!cache.exists(file1.parquet_file.id, ts2.tombstone.id).await);
        assert!(!cache.exists(file2.parquet_file.id, ts1.tombstone.id).await);
        assert!(!cache.exists(file2.parquet_file.id, ts2.tombstone.id).await);

        assert_histogram_metric_count(&catalog.metric_registry, "processed_tombstone_exist", 4);

        ts2.mark_processed(&file2).await;

        // values are cached for a while
        assert!(TTL_NOT_PROCESSED > Duration::from_millis(1));
        time_provider.inc(TTL_NOT_PROCESSED - Duration::from_millis(1));
        assert!(!cache.exists(file2.parquet_file.id, ts2.tombstone.id).await);
        assert_histogram_metric_count(&catalog.metric_registry, "processed_tombstone_exist", 4);

        time_provider.inc(Duration::from_millis(1));
        assert!(cache.exists(file2.parquet_file.id, ts2.tombstone.id).await);
        assert_histogram_metric_count(&catalog.metric_registry, "processed_tombstone_exist", 5);

        // "true" results are cached forever
        assert!(cache.exists(file1.parquet_file.id, ts1.tombstone.id).await);
        assert_histogram_metric_count(&catalog.metric_registry, "processed_tombstone_exist", 5);

        // cache key has two dimensions
        assert!(cache.exists(file1.parquet_file.id, ts1.tombstone.id).await);
        assert!(!cache.exists(file1.parquet_file.id, ts2.tombstone.id).await);
        assert!(!cache.exists(file2.parquet_file.id, ts1.tombstone.id).await);
        assert!(cache.exists(file2.parquet_file.id, ts2.tombstone.id).await);
        ts1.mark_processed(&file2).await;
        time_provider.inc(TTL_NOT_PROCESSED);
        assert!(cache.exists(file1.parquet_file.id, ts1.tombstone.id).await);
        assert!(!cache.exists(file1.parquet_file.id, ts2.tombstone.id).await);
        assert!(cache.exists(file2.parquet_file.id, ts1.tombstone.id).await);
        assert!(cache.exists(file2.parquet_file.id, ts2.tombstone.id).await);
    }
}
