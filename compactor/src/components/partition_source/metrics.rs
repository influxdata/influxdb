use std::fmt::Display;

use async_trait::async_trait;
use data_types::{Partition, PartitionId};
use metric::{Registry, U64Counter};

use super::PartitionSource;

const METRIC_NAME_PARTITION_FETCH_COUNT: &str = "iox_compactor_partition_fetch_count";

#[derive(Debug)]
pub struct MetricsPartitionSourceWrapper<T>
where
    T: PartitionSource,
{
    fetch_found_counter: U64Counter,
    fetch_notfound_counter: U64Counter,
    inner: T,
}

impl<T> MetricsPartitionSourceWrapper<T>
where
    T: PartitionSource,
{
    pub fn new(inner: T, registry: &Registry) -> Self {
        let fetch_metric = registry.register_metric::<U64Counter>(
            METRIC_NAME_PARTITION_FETCH_COUNT,
            "Number of times the compactor fetched information for a dedicated partition",
        );
        let fetch_found_counter = fetch_metric.recorder(&[("result", "found")]);
        let fetch_notfound_counter = fetch_metric.recorder(&[("result", "not_found")]);

        Self {
            fetch_found_counter,
            fetch_notfound_counter,
            inner,
        }
    }
}

impl<T> Display for MetricsPartitionSourceWrapper<T>
where
    T: PartitionSource,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "metrics({})", self.inner)
    }
}

#[async_trait]
impl<T> PartitionSource for MetricsPartitionSourceWrapper<T>
where
    T: PartitionSource,
{
    async fn fetch_by_id(&self, partition_id: PartitionId) -> Option<Partition> {
        let res = self.inner.fetch_by_id(partition_id).await;
        match res {
            Some(_) => self.fetch_found_counter.inc(1),
            None => self.fetch_notfound_counter.inc(1),
        }
        res
    }
}

#[cfg(test)]
mod tests {
    use metric::{assert_counter, Attributes};

    use crate::components::partition_source::mock::MockPartitionSource;
    use iox_tests::PartitionBuilder;

    use super::*;

    #[test]
    fn test_display() {
        let registry = Registry::new();
        let source =
            MetricsPartitionSourceWrapper::new(MockPartitionSource::new(vec![]), &registry);
        assert_eq!(source.to_string(), "metrics(mock)",);
    }

    #[tokio::test]
    async fn test_fetch_by_id() {
        let registry = Registry::new();
        let p = PartitionBuilder::new(5).build();
        let source = MetricsPartitionSourceWrapper::new(
            MockPartitionSource::new(vec![p.clone()]),
            &registry,
        );

        assert_fetch_found_counter(&registry, 0);
        assert_fetch_notfound_counter(&registry, 0);

        assert_eq!(
            source.fetch_by_id(PartitionId::new(5)).await,
            Some(p.clone())
        );
        assert_eq!(source.fetch_by_id(PartitionId::new(5)).await, Some(p));
        assert_eq!(source.fetch_by_id(PartitionId::new(1)).await, None);

        assert_fetch_found_counter(&registry, 2);
        assert_fetch_notfound_counter(&registry, 1);
    }

    fn assert_fetch_found_counter(registry: &Registry, value: u64) {
        assert_counter!(
            registry,
            U64Counter,
            METRIC_NAME_PARTITION_FETCH_COUNT,
            labels = Attributes::from(&[("result", "found")]),
            value = value,
        );
    }

    fn assert_fetch_notfound_counter(registry: &Registry, value: u64) {
        assert_counter!(
            registry,
            U64Counter,
            METRIC_NAME_PARTITION_FETCH_COUNT,
            labels = Attributes::from(&[("result", "not_found")]),
            value = value,
        );
    }
}
