use std::{fmt::Display, ops::Sub, sync::Arc, sync::Mutex, time::Duration};

use async_trait::async_trait;
use backoff::{Backoff, BackoffConfig};
use data_types::PartitionId;
use iox_catalog::interface::Catalog;
use iox_time::{Time, TimeProvider};

use crate::PartitionsSource;

#[derive(Debug)]
/// Returns all [`PartitionId`](data_types::PartitionId) that had a new Parquet file written after a lower bound of the current
/// time minus `min_threshold` and optionally limited only to those with Parquet files written
/// before the current time minus `max_threshold`.
///
/// If `max_threshold` is not specified, the upper bound is effectively the current time.
///
/// If `max_threshold` is specified, it must be less than `min_threshold` so that when computing
/// the range endpoints as `(now - min_threshold, now - max_threshold)`, the lower bound is lower
/// than the upper bound.
pub(crate) struct CatalogToCompactPartitionsSource {
    backoff_config: BackoffConfig,
    catalog: Arc<dyn Catalog>,

    /// min_threshold is the duration subtracted from now to determine the start range of the query.
    /// If its been a while since the last query, or this is the first query, this value will be
    /// increased up to 3x.
    min_threshold: Duration,

    /// max_threshold is the duration subtracted from now to determine the end range of the query.
    max_threshold: Option<Duration>,

    /// last_maximum_time remembers the ending time of the last query.  If previous round of compaction took
    /// too long, knowing the ending time of the last query helps us avoid skipping partitions with the next query.
    last_maximum_time: Mutex<Time>,

    time_provider: Arc<dyn TimeProvider>,
}

impl CatalogToCompactPartitionsSource {
    /// Create a new [`CatalogToCompactPartitionsSource`].
    pub(crate) fn new(
        backoff_config: BackoffConfig,
        catalog: Arc<dyn Catalog>,
        min_threshold: Duration,
        max_threshold: Option<Duration>,
        time_provider: Arc<dyn TimeProvider>,
    ) -> Self {
        Self {
            backoff_config,
            catalog,
            min_threshold,
            max_threshold,
            last_maximum_time: Mutex::new(Time::from_timestamp_nanos(0)),
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
        let mut minimum_time = self.time_provider.now() - self.min_threshold;
        let maximum_time: Option<Time>;

        // scope the locking to just maintenance of last_maximum_time, not the query
        {
            // we're going check the time range we'd like to query for against the end time of the last query.
            let mut last = self.last_maximum_time.lock().unwrap();

            // if the last query ended further back in time than this query starts, we're about to skip something.
            if *last < minimum_time {
                if minimum_time.sub(*last) < self.min_threshold * 3 {
                    // the end of the last query says we're skipping less than 3x our configured lookback, so
                    // back up and query everything since the last query.
                    minimum_time = *last;
                } else {
                    // end of the last query says we're skiping a lot.  We should limit how far we lookback to avoid
                    // returning all partitions, so we'll just backup 3x the configured lookback.
                    // this might skip something (until cold compaction), but we need a limit in how far we look back.
                    minimum_time = self.time_provider.now() - self.min_threshold * 3;
                }
            }
            maximum_time = self.max_threshold.map(|max| self.time_provider.now() - max);

            // save the maximum time used in this query to self.last_maximum_time
            *last = maximum_time.unwrap_or(self.time_provider.now());
        }

        Backoff::new(&self.backoff_config)
            .retry_all_errors("partitions_to_compact", || async {
                self.catalog
                    .repositories()
                    .await
                    .partitions()
                    .partitions_new_file_between(minimum_time.into(), maximum_time.map(Into::into))
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
        ids.iter().cloned().map(PartitionId::new).collect()
    }

    async fn fetch_test(
        catalog: Arc<MemCatalog>,
        min_threshold: Duration,
        max_threshold: Option<Duration>,
        first_expected_ids: &[i64], // expected values on first fetch, which does a 3x on min_threshold
        second_expected_ids: &[i64], // expected values on second fetch, which uses min_threshold unmodified
    ) {
        let time_provider = catalog.time_provider();

        let partitions_source = CatalogToCompactPartitionsSource::new(
            Default::default(),
            catalog,
            min_threshold,
            max_threshold,
            time_provider,
        );

        let mut actual_partition_ids = partitions_source.fetch().await;
        actual_partition_ids.sort();

        assert_eq!(
            actual_partition_ids,
            partition_ids(first_expected_ids),
            "CatalogToCompact source with min_threshold {min_threshold:?} and \
            max_threshold {max_threshold:?} failed (first fetch, 3x lookback)",
        );

        let mut actual_partition_ids = partitions_source.fetch().await;
        actual_partition_ids.sort();

        assert_eq!(
            actual_partition_ids,
            partition_ids(second_expected_ids),
            "CatalogToCompact source with min_threshold {min_threshold:?} and \
            max_threshold {max_threshold:?} failed (second fetch, specified lookback)",
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
        fetch_test(Arc::clone(&catalog), one_minute, None, &[], &[]).await;

        let four_hours = Duration::from_secs(60 * 60 * 4);
        fetch_test(Arc::clone(&catalog), four_hours, None, &[1, 2], &[1]).await;

        let seven_hours = Duration::from_secs(60 * 60 * 7);
        fetch_test(Arc::clone(&catalog), seven_hours, None, &[1, 2], &[1, 2]).await;
    }

    #[tokio::test]
    async fn max_specified() {
        let catalog = Arc::new(MemCatalog::new(Default::default()));
        let time_provider = catalog.time_provider();

        let time_now = Timestamp::from(time_provider.now());
        let time_three_hour_ago = Timestamp::from(time_provider.hours_ago(3));
        let time_six_hour_ago = Timestamp::from(time_provider.hours_ago(6));

        for (id, time) in [
            (1, time_now),
            (2, time_three_hour_ago),
            (3, time_six_hour_ago),
        ]
        .iter()
        .cloned()
        {
            let partition = PartitionBuilder::new(id as i64)
                .with_new_file_at(time)
                .build();
            catalog.add_partition(partition).await;
        }

        let one_minute = Duration::from_secs(60);
        let one_hour = Duration::from_secs(60 * 60);
        let four_hours = Duration::from_secs(60 * 60 * 4);
        let seven_hours = Duration::from_secs(60 * 60 * 7);

        fetch_test(
            Arc::clone(&catalog),
            seven_hours,
            Some(four_hours),
            &[3],
            &[3],
        )
        .await;

        fetch_test(
            Arc::clone(&catalog),
            seven_hours,
            Some(one_hour),
            &[2, 3],
            &[2, 3],
        )
        .await;

        fetch_test(
            Arc::clone(&catalog),
            seven_hours,
            Some(one_minute),
            &[2, 3],
            &[2, 3],
        )
        .await;

        fetch_test(
            Arc::clone(&catalog),
            four_hours,
            Some(one_hour),
            &[2, 3],
            &[2],
        )
        .await;

        fetch_test(
            Arc::clone(&catalog),
            four_hours,
            Some(one_minute),
            &[2, 3],
            &[2],
        )
        .await;

        fetch_test(Arc::clone(&catalog), one_hour, Some(one_minute), &[], &[]).await;
    }
}
