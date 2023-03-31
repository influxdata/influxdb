use std::{fmt::Display, sync::Arc, time::Duration};

use async_trait::async_trait;
use backoff::{Backoff, BackoffConfig};
use data_types::PartitionId;
use iox_catalog::interface::Catalog;
use iox_time::TimeProvider;

use super::PartitionsSource;

#[derive(Debug)]
/// Returns all partitions that had a new parquet file written more than `threshold` ago.
pub struct CatalogToCompactPartitionsSource {
    backoff_config: BackoffConfig,
    catalog: Arc<dyn Catalog>,
    threshold: Duration,
    time_provider: Arc<dyn TimeProvider>,
}

impl CatalogToCompactPartitionsSource {
    pub fn new(
        backoff_config: BackoffConfig,
        catalog: Arc<dyn Catalog>,
        threshold: Duration,
        time_provider: Arc<dyn TimeProvider>,
    ) -> Self {
        Self {
            backoff_config,
            catalog,
            threshold,
            time_provider,
        }
    }
}

impl Display for CatalogToCompactPartitionsSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "catalog_to_compact")
    }
}

#[async_trait]
impl PartitionsSource for CatalogToCompactPartitionsSource {
    async fn fetch(&self) -> Vec<PartitionId> {
        let cutoff = self.time_provider.now() - self.threshold;

        Backoff::new(&self.backoff_config)
            .retry_all_errors("partitions_to_compact", || async {
                self.catalog
                    .repositories()
                    .await
                    .partitions()
                    .partitions_new_file_between(cutoff.into(), None)
                    .await
            })
            .await
            .expect("retry forever")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use data_types::Timestamp;
    use iox_catalog::mem::MemCatalog;
    use iox_tests::PartitionBuilder;

    fn partition_ids(ids: &[i64]) -> Vec<PartitionId> {
        ids.iter().cloned().map(|id| PartitionId::new(id)).collect()
    }

    async fn fetch_test(catalog: Arc<MemCatalog>, min_threshold: Duration, expected_ids: &[i64]) {
        let time_provider = catalog.time_provider();

        let partitions_source = CatalogToCompactPartitionsSource::new(
            Default::default(),
            catalog,
            min_threshold,
            time_provider,
        );

        let mut actual_partition_ids = partitions_source.fetch().await;
        actual_partition_ids.sort();

        assert_eq!(
            actual_partition_ids,
            partition_ids(expected_ids),
            "CatalogToCompact source with min_threshold {min_threshold:?} failed",
        );
    }

    #[tokio::test]
    async fn no_max_specified() {
        let catalog = Arc::new(MemCatalog::new(Default::default()));
        let time_provider = catalog.time_provider();

        let time_three_hour_ago = Timestamp::from(time_provider.hours_ago(3));
        let time_six_hour_ago = Timestamp::from(time_provider.hours_ago(6));

        for (id, time) in [(1, time_three_hour_ago), (2, time_six_hour_ago)]
            .iter()
            .cloned()
        {
            let partition = PartitionBuilder::new(id as i64)
                .with_new_file_at(time)
                .build();
            catalog.add_partition(partition).await;
        }

        let one_minute = Duration::from_secs(60);
        fetch_test(Arc::clone(&catalog), one_minute, &[]).await;

        let four_hours = Duration::from_secs(60 * 60 * 4);
        fetch_test(Arc::clone(&catalog), four_hours, &[1]).await;

        let seven_hours = Duration::from_secs(60 * 60 * 7);
        fetch_test(Arc::clone(&catalog), seven_hours, &[1, 2]).await;
    }
}
