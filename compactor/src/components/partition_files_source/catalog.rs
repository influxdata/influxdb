use std::{
    fmt::{Debug, Display},
    sync::Arc,
};

use async_trait::async_trait;
use backoff::{Backoff, BackoffConfig};
use data_types::{ParquetFile, PartitionId, TransitionPartitionId};
use iox_catalog::interface::Catalog;
use observability_deps::tracing::warn;

use super::{rate_limit::RateLimit, PartitionFilesSource};

#[async_trait]
pub(crate) trait CatalogQuerier: Send + Sync + Debug {
    async fn get_partitions(
        &self,
        partition_id: PartitionId,
    ) -> Result<Vec<ParquetFile>, iox_catalog::interface::Error>;
}

/// a QueryRateLimiter applies a RateLimit to a CatalogQuerier.
#[derive(Debug)]
pub struct QueryRateLimiter<T> {
    inner: T,
    rate_limit: RateLimit,
}

impl<T> QueryRateLimiter<T> {
    pub fn new(inner: T, rate_limit: RateLimit) -> Self {
        Self { inner, rate_limit }
    }
}

#[async_trait]
impl<T> CatalogQuerier for QueryRateLimiter<T>
where
    T: CatalogQuerier,
{
    async fn get_partitions(
        &self,
        partition_id: PartitionId,
    ) -> Result<Vec<ParquetFile>, iox_catalog::interface::Error> {
        while let Some(d) = self.rate_limit.can_proceed() {
            warn!(%partition_id, "partition fetch rate limited");

            // Don't busy loop - wait the fractions of a second before a retry
            // is allowed.
            tokio::time::sleep(d).await;
        }
        self.inner.get_partitions(partition_id).await
    }
}

#[async_trait]
impl CatalogQuerier for Arc<dyn Catalog> {
    async fn get_partitions(
        &self,
        partition_id: PartitionId,
    ) -> Result<Vec<ParquetFile>, iox_catalog::interface::Error> {
        self.repositories()
            .await
            .parquet_files()
            .list_by_partition_not_to_delete(&TransitionPartitionId::Deprecated(partition_id))
            .await
    }
}

#[derive(Debug)]
pub struct CatalogPartitionFilesSource<T = QueryRateLimiter<Arc<dyn Catalog>>> {
    backoff_config: BackoffConfig,
    catalog: T,
}

impl<T> CatalogPartitionFilesSource<T> {
    pub fn new(backoff_config: BackoffConfig, catalog: T) -> Self {
        Self {
            backoff_config,
            catalog,
        }
    }
}

impl<T> Display for CatalogPartitionFilesSource<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "catalog")
    }
}

#[async_trait]
impl<T> PartitionFilesSource for CatalogPartitionFilesSource<T>
where
    T: CatalogQuerier,
{
    async fn fetch(&self, partition_id: PartitionId) -> Vec<ParquetFile> {
        Backoff::new(&self.backoff_config)
            .retry_all_errors("parquet_files_of_given_partition", || async {
                self.catalog.get_partitions(partition_id).await
            })
            .await
            .expect("retry forever")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{sync::Mutex, time::Duration};
    use tokio::time::Instant;

    /// A [`CatalogQuerier`] that always returns OK, and counts the number of
    /// calls made.
    #[derive(Debug, Default)]
    struct MockInner(Mutex<usize>);
    #[async_trait]
    impl CatalogQuerier for &MockInner {
        async fn get_partitions(
            &self,
            _partition_id: PartitionId,
        ) -> Result<Vec<ParquetFile>, iox_catalog::interface::Error> {
            *self.0.lock().unwrap() += 1;
            Ok(vec![])
        }
    }

    #[tokio::test]
    async fn test_rate_limit() {
        const ALLOWED_PER_SECOND: usize = 100;

        let inner = MockInner::default();
        let r = QueryRateLimiter::new(
            &inner,
            RateLimit::new(ALLOWED_PER_SECOND, ALLOWED_PER_SECOND / 10),
        );

        let mut start = Instant::now();

        // If there are ALLOWED_PER_SECOND queries allowed per second, then it
        // should take 1 second to issue ALLOWED_PER_SECOND number of queries.
        //
        // Attempt to make 1/10th the number of permissible queries per second,
        // which should take at least 1/10th of a second due to smoothing, so
        // the test does not take so long.
        for _ in 0..(ALLOWED_PER_SECOND / 10) {
            r.get_partitions(PartitionId::new(42)).await.unwrap();
        }

        // It should have taken at least 1/10th of a second
        let duration = Instant::now() - start;
        assert!(duration > Duration::from_millis(100));

        // It should have taken less than 2/10th of a second
        // If this test is flaky, increase this a bit.
        assert!(duration < Duration::from_millis(200));

        // Exactly 1/10th the number of queries should be dispatched to the
        // inner impl.
        assert_eq!(*inner.0.lock().unwrap(), ALLOWED_PER_SECOND / 10);

        // Now repeat with a delay to fill the burst balance.
        start = Instant::now();
        for i in 0..(ALLOWED_PER_SECOND / 10) {
            r.get_partitions(PartitionId::new(42)).await.unwrap();
            if i == 0 {
                // delay until the next request, to fill the burst balance.
                tokio::time::sleep(Duration::from_millis(90)).await;
            }
        }

        // It should have taken at least 1/10th of a second
        let duration = Instant::now() - start;
        assert!(duration > Duration::from_millis(100));

        // It should have taken less than 2/10th of a second
        // If this test is flaky, increase this a bit.
        assert!(duration < Duration::from_millis(200));

        // Exactly 2/10th the number of queries should be dispatched to the
        // inner impl.
        assert_eq!(*inner.0.lock().unwrap(), 2 * ALLOWED_PER_SECOND / 10);
    }
}
