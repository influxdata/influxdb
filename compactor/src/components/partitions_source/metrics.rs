use std::fmt::Display;

use async_trait::async_trait;
use compactor_scheduler::PartitionsSource;
use data_types::PartitionId;
use metric::{Registry, U64Counter};

const METRIC_NAME_PARTITIONS_FETCH_COUNT: &str = "iox_compactor_partitions_fetch_count";
const METRIC_NAME_PARTITIONS_COUNT: &str = "iox_compactor_partitions_count";

#[derive(Debug)]
pub struct MetricsPartitionsSourceWrapper<T>
where
    T: PartitionsSource,
{
    partitions_fetch_counter: U64Counter,
    partitions_counter: U64Counter,
    inner: T,
}

impl<T> MetricsPartitionsSourceWrapper<T>
where
    T: PartitionsSource,
{
    pub fn new(inner: T, registry: &Registry) -> Self {
        let partitions_fetch_counter = registry
            .register_metric::<U64Counter>(
                METRIC_NAME_PARTITIONS_FETCH_COUNT,
                "Number of times the compactor fetched fresh partitions",
            )
            .recorder(&[]);
        let partitions_counter = registry
            .register_metric::<U64Counter>(
                METRIC_NAME_PARTITIONS_COUNT,
                "Number of partitions processed by the compactor. This contains the sum over ALL rounds (i.e. the same partition may be counted multiple times).",
            )
            .recorder(&[]);

        Self {
            partitions_fetch_counter,
            partitions_counter,
            inner,
        }
    }
}

impl<T> Display for MetricsPartitionsSourceWrapper<T>
where
    T: PartitionsSource,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "metrics({})", self.inner)
    }
}

#[async_trait]
impl<T> PartitionsSource for MetricsPartitionsSourceWrapper<T>
where
    T: PartitionsSource,
{
    async fn fetch(&self) -> Vec<PartitionId> {
        let partitions = self.inner.fetch().await;
        self.partitions_fetch_counter.inc(1);
        self.partitions_counter.inc(partitions.len() as u64);
        partitions
    }
}

#[cfg(test)]
mod tests {
    use compactor_scheduler::MockPartitionsSource;
    use metric::assert_counter;

    use super::*;

    #[test]
    fn test_display() {
        let registry = Registry::new();
        let source =
            MetricsPartitionsSourceWrapper::new(MockPartitionsSource::new(vec![]), &registry);
        assert_eq!(source.to_string(), "metrics(mock)",);
    }

    #[tokio::test]
    async fn test_fetch() {
        let registry = Registry::new();
        let partitions = vec![
            PartitionId::new(5),
            PartitionId::new(1),
            PartitionId::new(12),
        ];
        let source = MetricsPartitionsSourceWrapper::new(
            MockPartitionsSource::new(partitions.clone()),
            &registry,
        );

        assert_fetch_counter(&registry, 0);
        assert_partition_counter(&registry, 0);

        assert_eq!(source.fetch().await, partitions);

        assert_fetch_counter(&registry, 1);
        assert_partition_counter(&registry, 3);
    }

    fn assert_fetch_counter(registry: &Registry, value: u64) {
        assert_counter!(
            registry,
            U64Counter,
            METRIC_NAME_PARTITIONS_FETCH_COUNT,
            value = value,
        );
    }

    fn assert_partition_counter(registry: &Registry, value: u64) {
        assert_counter!(
            registry,
            U64Counter,
            METRIC_NAME_PARTITIONS_COUNT,
            value = value,
        );
    }
}
