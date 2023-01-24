use std::{collections::HashMap, fmt::Display};

use async_trait::async_trait;
use data_types::PartitionId;
use metric::{Registry, U64Counter};

use crate::error::{ErrorKind, ErrorKindExt};

use super::PartitionErrorSink;

#[derive(Debug)]
pub struct MetricsPartitionErrorSinkWrapper<T>
where
    T: PartitionErrorSink,
{
    error_counter: HashMap<ErrorKind, U64Counter>,
    inner: T,
}

impl<T> MetricsPartitionErrorSinkWrapper<T>
where
    T: PartitionErrorSink,
{
    pub fn new(inner: T, registry: &Registry) -> Self {
        let metric = registry.register_metric::<U64Counter>(
            "iox_compactor_partition_error_count",
            "Number of errors that occurred while compacting a partition",
        );
        let error_counter = ErrorKind::variants()
            .iter()
            .map(|kind| (*kind, metric.recorder(&[("kind", kind.name())])))
            .collect();

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
    async fn record(&self, partition: PartitionId, e: Box<dyn std::error::Error + Send + Sync>) {
        let kind = e.classify();
        self.error_counter
            .get(&kind)
            .expect("all kinds constructed")
            .inc(1);
        self.inner.record(partition, e).await;
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Arc};

    use metric::{Attributes, Metric};
    use object_store::Error as ObjectStoreError;

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

        assert_eq!(error_counter(&registry, "unknown"), 0);
        assert_eq!(error_counter(&registry, "object_store"), 0);

        sink.record(PartitionId::new(1), "msg 1".into()).await;
        sink.record(PartitionId::new(2), "msg 2".into()).await;
        sink.record(
            PartitionId::new(1),
            Box::new(ObjectStoreError::NotImplemented),
        )
        .await;

        assert_eq!(error_counter(&registry, "unknown"), 2);
        assert_eq!(error_counter(&registry, "object_store"), 1);

        assert_eq!(
            inner.errors(),
            HashMap::from([
                (
                    PartitionId::new(1),
                    String::from("Operation not yet implemented.")
                ),
                (PartitionId::new(2), String::from("msg 2")),
            ]),
        );
    }

    fn error_counter(registry: &Registry, kind: &'static str) -> u64 {
        registry
            .get_instrument::<Metric<U64Counter>>("iox_compactor_partition_error_count")
            .expect("instrument not found")
            .get_observer(&Attributes::from(&[("kind", kind)]))
            .expect("observer not found")
            .fetch()
    }
}
