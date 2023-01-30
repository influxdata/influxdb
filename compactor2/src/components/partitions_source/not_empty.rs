use std::{fmt::Display, sync::Arc, time::Duration};

use async_trait::async_trait;
use data_types::PartitionId;
use iox_time::TimeProvider;

use super::PartitionsSource;

#[derive(Debug)]
pub struct NotEmptyPartitionsSourceWrapper<T>
where
    T: PartitionsSource,
{
    inner: T,
    throttle: Duration,
    time_provider: Arc<dyn TimeProvider>,
}

impl<T> NotEmptyPartitionsSourceWrapper<T>
where
    T: PartitionsSource,
{
    pub fn new(inner: T, throttle: Duration, time_provider: Arc<dyn TimeProvider>) -> Self {
        Self {
            inner,
            throttle,
            time_provider,
        }
    }
}

impl<T> Display for NotEmptyPartitionsSourceWrapper<T>
where
    T: PartitionsSource,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "not_empty({})", self.inner)
    }
}

#[async_trait]
impl<T> PartitionsSource for NotEmptyPartitionsSourceWrapper<T>
where
    T: PartitionsSource,
{
    async fn fetch(&self) -> Vec<PartitionId> {
        loop {
            let res = self.inner.fetch().await;
            if !res.is_empty() {
                return res;
            }
            self.time_provider.sleep(self.throttle).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use iox_time::{MockProvider, Time};

    use crate::{
        components::partitions_source::mock::MockPartitionsSource, test_util::AssertFutureExt,
    };

    use super::*;

    #[test]
    fn test_display() {
        let source = NotEmptyPartitionsSourceWrapper::new(
            MockPartitionsSource::new(vec![]),
            Duration::from_secs(1),
            Arc::new(MockProvider::new(Time::MIN)),
        );
        assert_eq!(source.to_string(), "not_empty(mock)",);
    }

    #[tokio::test]
    async fn test_fetch() {
        let inner = Arc::new(MockPartitionsSource::new(vec![]));
        let time_provider = Arc::new(MockProvider::new(Time::MIN));
        let source = NotEmptyPartitionsSourceWrapper::new(
            Arc::clone(&inner),
            Duration::from_secs(1),
            Arc::clone(&time_provider) as _,
        );

        // intially pending because no data
        let mut fut = source.fetch();
        fut.assert_pending().await;

        // still not data, still pending
        time_provider.inc(Duration::from_secs(10));
        fut.assert_pending().await;

        // insert data but system is still throttled
        let p = PartitionId::new(5);
        let parts = vec![p];
        inner.set(parts.clone());
        fut.assert_pending().await;

        // still throttled
        time_provider.inc(Duration::from_millis(500));
        fut.assert_pending().await;

        // finally a result
        time_provider.inc(Duration::from_millis(500));
        let res = fut.poll_timeout().await;
        assert_eq!(res, parts);

        // not empty, so data arrives immediately
        let fut = source.fetch();
        let res = fut.poll_timeout().await;
        assert_eq!(res, parts);
    }
}
