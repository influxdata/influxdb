use std::fmt::Display;

use async_trait::async_trait;
use data_types::{Partition, PartitionId};
use metric::{Registry, U64Counter};

use super::PartitionsSource;

#[derive(Debug)]
pub struct MetricsPartitionsSourceWrapper<T>
where
    T: PartitionsSource,
{
    partitions_fetch_counter: U64Counter,
    partitions_counter: U64Counter,
    fetch_found_counter: U64Counter,
    fetch_notfound_counter: U64Counter,
    inner: T,
}

impl<T> MetricsPartitionsSourceWrapper<T>
where
    T: PartitionsSource,
{
    pub fn new(inner: T, registry: &Registry) -> Self {
        let partitions_fetch_counter = registry
            .register_metric::<U64Counter>(
                "iox_compactor_partitions_fetch_count",
                "Number of times the compactor fetched fresh partitions",
            )
            .recorder(&[]);
        let partitions_counter = registry
            .register_metric::<U64Counter>(
                "iox_compactor_partitions_count",
                "Number of partitions processed by the compactor. This contains the sum over ALL rounds (i.e. the same partition may be counted multiple times).",
            )
            .recorder(&[]);

        let fetch_metric = registry.register_metric::<U64Counter>(
            "iox_compactor_partition_fetch_count",
            "Number of times the compactor fetched information for a dedicated partition",
        );
        let fetch_found_counter = fetch_metric.recorder(&[("result", "found")]);
        let fetch_notfound_counter = fetch_metric.recorder(&[("result", "not_found")]);

        Self {
            partitions_fetch_counter,
            partitions_counter,
            fetch_found_counter,
            fetch_notfound_counter,
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
    use metric::{Attributes, Metric};

    use crate::{
        components::partitions_source::mock::MockPartitionsSource, test_util::PartitionBuilder,
    };

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
            PartitionBuilder::new(5).build(),
            PartitionBuilder::new(1).build(),
            PartitionBuilder::new(12).build(),
        ];
        let source = MetricsPartitionsSourceWrapper::new(
            MockPartitionsSource::new(partitions.clone()),
            &registry,
        );

        assert_eq!(fetch_counter(&registry), 0,);
        assert_eq!(partition_counter(&registry), 0,);

        let ids = partitions.iter().map(|p| p.id).collect::<Vec<_>>();
        assert_eq!(source.fetch().await, ids,);

        assert_eq!(fetch_counter(&registry), 1,);
        assert_eq!(partition_counter(&registry), 3,);
    }

    #[tokio::test]
    async fn test_fetch_by_id() {
        let registry = Registry::new();
        let p = PartitionBuilder::new(5).build();
        let source = MetricsPartitionsSourceWrapper::new(
            MockPartitionsSource::new(vec![p.clone()]),
            &registry,
        );

        assert_eq!(fetch_found_counter(&registry), 0,);
        assert_eq!(fetch_notfound_counter(&registry), 0,);

        assert_eq!(
            source.fetch_by_id(PartitionId::new(5)).await,
            Some(p.clone())
        );
        assert_eq!(source.fetch_by_id(PartitionId::new(5)).await, Some(p));
        assert_eq!(source.fetch_by_id(PartitionId::new(1)).await, None);

        assert_eq!(fetch_found_counter(&registry), 2,);
        assert_eq!(fetch_notfound_counter(&registry), 1,);
    }

    fn fetch_counter(registry: &Registry) -> u64 {
        registry
            .get_instrument::<Metric<U64Counter>>("iox_compactor_partitions_fetch_count")
            .expect("instrument not found")
            .get_observer(&Attributes::from([]))
            .expect("observer not found")
            .fetch()
    }

    fn partition_counter(registry: &Registry) -> u64 {
        registry
            .get_instrument::<Metric<U64Counter>>("iox_compactor_partitions_count")
            .expect("instrument not found")
            .get_observer(&Attributes::from([]))
            .expect("observer not found")
            .fetch()
    }

    fn fetch_found_counter(registry: &Registry) -> u64 {
        registry
            .get_instrument::<Metric<U64Counter>>("iox_compactor_partition_fetch_count")
            .expect("instrument not found")
            .get_observer(&Attributes::from(&[("result", "found")]))
            .expect("observer not found")
            .fetch()
    }

    fn fetch_notfound_counter(registry: &Registry) -> u64 {
        registry
            .get_instrument::<Metric<U64Counter>>("iox_compactor_partition_fetch_count")
            .expect("instrument not found")
            .get_observer(&Attributes::from(&[("result", "not_found")]))
            .expect("observer not found")
            .fetch()
    }
}
