use std::{fmt::Display, sync::Arc, time::Duration};

use async_trait::async_trait;
use backoff::{Backoff, BackoffConfig};
use data_types::PartitionId;
use iox_catalog::interface::Catalog;
use iox_time::{Time, TimeProvider};
use observability_deps::tracing::info;
use parking_lot::Mutex;

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
            let mut last = self.last_maximum_time.lock();

            // query for partitions with activity since the last query.  We shouldn't query for a time range
            // we've already covered.  So if the prior query was 2m ago, and the query covered 10m, ending at
            // the time of that query, we just need to query for activity in the last 2m.  Asking for more than
            // that creates busy-work that will spam the catalog with more queries to determine no compaction
            // needed.  But we also don't want to query so far back in time that we get all partitions, so the
            // lookback is limited to 3x the configured threshold.
            if minimum_time < *last
                || minimum_time
                    .checked_duration_since(*last)
                    .map(|duration| duration < self.min_threshold * 3)
                    .unwrap_or_default()
            {
                // the end of the last query is less than 3x our configured lookback, so we can query everything
                // since the last query.
                minimum_time = *last;
            } else {
                // end of the last query says we're skiping a lot.  We should limit how far we lookback to avoid
                // returning all partitions, so we'll just backup 3x the configured lookback.
                // this might skip something (until cold compaction), but we need a limit in how far we look back.
                minimum_time = self.time_provider.now() - self.min_threshold * 3;
            }
            maximum_time = self.max_threshold.map(|max| self.time_provider.now() - max);

            info!(
                minimum_time = minimum_time.to_string().as_str(),
                maximum_time = maximum_time
                    .map(|mt| mt.to_string())
                    .unwrap_or(String::from(""))
                    .as_str(),
                last_maximum_time = (*last).to_string().as_str(),
                "Fetching partitions to consider for compaction",
            );

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
    use iox_time::MockProvider;

    fn partition_ids(ids: &[i64]) -> Vec<PartitionId> {
        ids.iter().cloned().map(PartitionId::new).collect()
    }

    async fn fetch_test(
        catalog: Arc<MemCatalog>,
        min_threshold: Duration,
        max_threshold: Option<Duration>,
        second_query_delta: Duration, // time between first and second query
        first_expected_ids: &[i64], // expected values on first fetch, which does a 3x on min_threshold
        second_expected_ids: &[i64], // expected values on second fetch, which uses min_threshold unmodified
    ) {
        let time_provider = Arc::new(MockProvider::new(catalog.time_provider().now()));

        let partitions_source = CatalogToCompactPartitionsSource::new(
            Default::default(),
            catalog,
            min_threshold,
            max_threshold,
            Arc::<iox_time::MockProvider>::clone(&time_provider),
        );

        let mut actual_partition_ids = partitions_source.fetch().await;
        actual_partition_ids.sort();

        assert_eq!(
            actual_partition_ids,
            partition_ids(first_expected_ids),
            "CatalogToCompact source with min_threshold {min_threshold:?} and \
            max_threshold {max_threshold:?} failed (first fetch, 3x lookback)",
        );

        time_provider.inc(second_query_delta);
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
        let time_one_min_future = Timestamp::from(time_provider.minutes_into_future(1));

        for (id, time) in [
            (1, time_three_hour_ago),
            (2, time_six_hour_ago),
            (3, time_one_min_future),
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
        let ten_minute = Duration::from_secs(60) * 10;

        // the lack of end time means it gets the future file (3) in the first query, this is an
        // oddity of a test case that has files with a future timestamp (not a real world concern).
        // the second query 10m later with a cap of 3m lookback doesn't get it.
        fetch_test(
            Arc::clone(&catalog),
            one_minute,
            None,
            ten_minute,
            &[3],
            &[],
        )
        .await;

        let four_hours = Duration::from_secs(60 * 60 * 4);
        // again the future file is included in he first query, just an oddity of the test case.
        fetch_test(
            Arc::clone(&catalog),
            four_hours,
            None,
            ten_minute,
            &[1, 2, 3],
            &[3],
        )
        .await;

        let seven_hours = Duration::from_secs(60 * 60 * 7);
        // again the future file is included in he first query, just an oddity of the test case.
        fetch_test(
            Arc::clone(&catalog),
            seven_hours,
            None,
            ten_minute,
            &[1, 2, 3],
            &[3],
        )
        .await;
    }

    #[tokio::test]
    async fn max_specified() {
        let catalog = Arc::new(MemCatalog::new(Default::default()));
        let time_provider = catalog.time_provider();

        let time_now = Timestamp::from(time_provider.now());
        let time_three_hour_ago = Timestamp::from(time_provider.hours_ago(3));
        let time_six_hour_ago = Timestamp::from(time_provider.hours_ago(6));
        let time_one_min_future = Timestamp::from(time_provider.minutes_into_future(1));

        for (id, time) in [
            (1, time_now),
            (2, time_three_hour_ago),
            (3, time_six_hour_ago),
            (4, time_one_min_future),
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
        let two_hour = Duration::from_secs(60 * 60 * 2);
        let four_hours = Duration::from_secs(60 * 60 * 4);
        let seven_hours = Duration::from_secs(60 * 60 * 7);

        // File 3 is all that falls within the 7-4h lookback window.  With 1m to the next query,
        // nothing is found with windows advanced by 1m.
        fetch_test(
            Arc::clone(&catalog),
            seven_hours,
            Some(four_hours),
            one_minute,
            &[3],
            &[],
        )
        .await;

        // With a 7-1h lookback window, files 2 and 3 are found.  With 2h to the next query, the
        // window advances to find the two newer files.
        fetch_test(
            Arc::clone(&catalog),
            seven_hours,
            Some(one_hour),
            two_hour,
            &[2, 3],
            &[1, 4],
        )
        .await;

        // With a 7h-1m lookback window, files 2 and 3 are found.  With 1m to the next query, the
        // window advances to find the one newer file.
        fetch_test(
            Arc::clone(&catalog),
            seven_hours,
            Some(one_minute),
            one_minute,
            &[2, 3],
            &[1],
        )
        .await;

        // With a 4h-1h lookback window, files 2 and 3 are found.  With 1m to the next query, there's
        // nothing new in the next window.
        fetch_test(
            Arc::clone(&catalog),
            four_hours,
            Some(one_hour),
            one_minute,
            &[2, 3],
            &[],
        )
        .await;

        // With a 4h-1m lookback window, files 2 and 3 are found.  With 4h to the next query, the
        // remaining files are found.
        fetch_test(
            Arc::clone(&catalog),
            four_hours,
            Some(one_minute),
            four_hours,
            &[2, 3],
            &[1, 4],
        )
        .await;

        // With a 1h-1m lookback window, nothing is found.  In the second query 1m later, it finds
        // the file create 'now'.
        fetch_test(
            Arc::clone(&catalog),
            one_hour,
            Some(one_minute),
            one_minute,
            &[],
            &[1],
        )
        .await;
    }
}
