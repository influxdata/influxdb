use std::fmt::Display;

use async_trait::async_trait;
use data_types::{Partition, PartitionId};
use observability_deps::tracing::{info, warn};

use super::PartitionSource;

#[derive(Debug)]
pub struct LoggingPartitionSourceWrapper<T>
where
    T: PartitionSource,
{
    inner: T,
}

impl<T> LoggingPartitionSourceWrapper<T>
where
    T: PartitionSource,
{
    pub fn new(inner: T) -> Self {
        Self { inner }
    }
}

impl<T> Display for LoggingPartitionSourceWrapper<T>
where
    T: PartitionSource,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "logging({})", self.inner)
    }
}

#[async_trait]
impl<T> PartitionSource for LoggingPartitionSourceWrapper<T>
where
    T: PartitionSource,
{
    async fn fetch_by_id(&self, partition_id: PartitionId) -> Option<Partition> {
        let partition = self.inner.fetch_by_id(partition_id).await;
        match &partition {
            Some(_) => {
                info!(partition_id = partition_id.get(), "Fetch a partition",);
            }
            None => {
                warn!(partition_id = partition_id.get(), "Partition not found",);
            }
        }
        partition
    }
}

#[cfg(test)]
mod tests {
    use test_helpers::tracing::TracingCapture;

    use crate::components::partition_source::mock::MockPartitionSource;
    use iox_tests::PartitionBuilder;

    use super::*;

    #[test]
    fn test_display() {
        let source = LoggingPartitionSourceWrapper::new(MockPartitionSource::new(vec![]));
        assert_eq!(source.to_string(), "logging(mock)",);
    }

    #[tokio::test]
    async fn test_fetch_by_id() {
        let p = PartitionBuilder::new(5).build();
        let source = LoggingPartitionSourceWrapper::new(MockPartitionSource::new(vec![p.clone()]));
        let capture = TracingCapture::new();

        assert_eq!(
            source.fetch_by_id(PartitionId::new(5)).await,
            Some(p.clone())
        );
        assert_eq!(source.fetch_by_id(PartitionId::new(5)).await, Some(p));
        assert_eq!(source.fetch_by_id(PartitionId::new(1)).await, None);

        assert_eq!(
            capture.to_string(),
            "level = INFO; message = Fetch a partition; partition_id = 5; \n\
level = INFO; message = Fetch a partition; partition_id = 5; \n\
level = WARN; message = Partition not found; partition_id = 1; ",
        );
    }
}
