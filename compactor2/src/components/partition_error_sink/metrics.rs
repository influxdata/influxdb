use std::fmt::Display;

use async_trait::async_trait;
use data_types::PartitionId;
use metric::{Registry, U64Counter};

use super::PartitionErrorSink;

#[derive(Debug)]
pub struct MetricsPartitionErrorSinkWrapper<T>
where
    T: PartitionErrorSink,
{
    error_counter: U64Counter,
    inner: T,
}

impl<T> MetricsPartitionErrorSinkWrapper<T>
where
    T: PartitionErrorSink,
{
    pub fn new(inner: T, registry: &Registry) -> Self {
        let error_counter = registry
            .register_metric::<U64Counter>(
                "iox_compactor_partition_error_count",
                "Number of errors that occurred while compacting a partition",
            )
            .recorder(&[]);
        Self {
            error_counter,
            inner,
        }
    }
}

impl<T> Display for MetricsPartitionErrorSinkWrapper<T>
where
    T: PartitionErrorSink,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "metrics({})", self.inner)
    }
}

#[async_trait]
impl<T> PartitionErrorSink for MetricsPartitionErrorSinkWrapper<T>
where
    T: PartitionErrorSink,
{
    async fn record(&self, partition: PartitionId, msg: &str) {
        self.error_counter.inc(1);
        self.inner.record(partition, msg).await;
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Arc};

    use metric::{Attributes, Metric};

    use crate::components::partition_error_sink::mock::MockPartitionErrorSink;

    use super::*;

    #[test]
    fn test_display() {
        let registry = Registry::new();
        let sink = MetricsPartitionErrorSinkWrapper::new(MockPartitionErrorSink::new(), &registry);
        assert_eq!(sink.to_string(), "metrics(mock)");
    }

    #[tokio::test]
    async fn test_record() {
        let registry = Registry::new();
        let inner = Arc::new(MockPartitionErrorSink::new());
        let sink = MetricsPartitionErrorSinkWrapper::new(Arc::clone(&inner), &registry);

        assert_eq!(error_counter(&registry), 0);

        sink.record(PartitionId::new(1), "msg 1").await;
        sink.record(PartitionId::new(2), "msg 2").await;
        sink.record(PartitionId::new(1), "msg 3").await;

        assert_eq!(error_counter(&registry), 3);

        assert_eq!(
            inner.errors(),
            HashMap::from([
                (PartitionId::new(1), String::from("msg 3")),
                (PartitionId::new(2), String::from("msg 2")),
            ]),
        );
    }

    fn error_counter(registry: &Registry) -> u64 {
        registry
            .get_instrument::<Metric<U64Counter>>("iox_compactor_partition_error_count")
            .expect("instrument not found")
            .get_observer(&Attributes::from([]))
            .expect("observer not found")
            .fetch()
    }
}
