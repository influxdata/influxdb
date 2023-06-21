use std::{sync::Mutex, time::Duration};

use async_trait::async_trait;
use data_types::{ParquetFile, PartitionId};
use observability_deps::tracing::warn;
use tokio::time::Instant;

use super::catalog::CatalogQuerier;

/// A [`CatalogQuerier`] rate limiter that smooths `N` queries over a second.
#[derive(Debug)]
pub struct QueryRateLimit<T> {
    inner: T,

    last_query: Mutex<Instant>,
    min_interval: Duration,
}

impl<T> QueryRateLimit<T> {
    pub(crate) fn new(inner: T, rps: usize) -> Self {
        Self {
            inner,
            last_query: Mutex::new(Instant::now()),
            min_interval: Duration::from_secs(1) / rps as u32,
        }
    }

    fn can_proceed(&self) -> bool {
        let mut last_query = self.last_query.lock().unwrap();
        let now = Instant::now();

        // Has enough time passed since the last query was allowed?
        let next_allowed = last_query.checked_add(self.min_interval).unwrap();
        if now < next_allowed {
            return false;
        }

        *last_query = now;
        true
    }
}

#[async_trait]
impl<T> CatalogQuerier for QueryRateLimit<T>
where
    T: CatalogQuerier,
{
    async fn get_partitions(
        &self,
        partition_id: PartitionId,
    ) -> Result<Vec<ParquetFile>, iox_catalog::interface::Error> {
        while !self.can_proceed() {
            warn!(%partition_id, "partition fetch rate limited");

            // Don't busy loop - wait the fractions of a second before a retry
            // is allowed.
            tokio::time::sleep(self.min_interval).await;
        }
        self.inner.get_partitions(partition_id).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
        let r = QueryRateLimit::new(&inner, ALLOWED_PER_SECOND);

        let start = Instant::now();

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
        assert!(duration > Duration::from_millis(ALLOWED_PER_SECOND as u64 / 10));

        // Exactly 1/10th the number of queries should be dispatched to the
        // inner impl.
        assert_eq!(*inner.0.lock().unwrap(), ALLOWED_PER_SECOND / 10);
    }
}
