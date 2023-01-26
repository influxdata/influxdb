use std::fmt::Display;

use async_trait::async_trait;
use data_types::PartitionId;
use observability_deps::tracing::{info, warn};

use super::PartitionsSource;

#[derive(Debug)]
pub struct LoggingPartitionsSourceWrapper<T>
where
    T: PartitionsSource,
{
    inner: T,
}

impl<T> LoggingPartitionsSourceWrapper<T>
where
    T: PartitionsSource,
{
    pub fn new(inner: T) -> Self {
        Self { inner }
    }
}

impl<T> Display for LoggingPartitionsSourceWrapper<T>
where
    T: PartitionsSource,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "logging({})", self.inner)
    }
}

#[async_trait]
impl<T> PartitionsSource for LoggingPartitionsSourceWrapper<T>
where
    T: PartitionsSource,
{
    async fn fetch(&self) -> Vec<PartitionId> {
        let partitions = self.inner.fetch().await;
        info!(n_partitions = partitions.len(), "Fetch partitions",);
        if partitions.is_empty() {
            warn!("No partition found",);
        }
        partitions
    }
}

#[cfg(test)]
mod tests {
    use test_helpers::tracing::TracingCapture;

    use crate::components::partitions_source::mock::MockPartitionsSource;

    use super::*;

    #[test]
    fn test_display() {
        let source = LoggingPartitionsSourceWrapper::new(MockPartitionsSource::new(vec![]));
        assert_eq!(source.to_string(), "logging(mock)",);
    }

    #[tokio::test]
    async fn test_fetch_empty() {
        let source = LoggingPartitionsSourceWrapper::new(MockPartitionsSource::new(vec![]));
        let capture = TracingCapture::new();
        assert_eq!(source.fetch().await, vec![],);
        // logs normal log message (so it's easy search for every single call) but also an extra warning
        assert_eq!(
            capture.to_string(),
            "level = INFO; message = Fetch partitions; n_partitions = 0; \nlevel = WARN; message = No partition found; ",
        );
    }

    #[tokio::test]
    async fn test_fetch_some() {
        let p_1 = PartitionId::new(5);
        let p_2 = PartitionId::new(1);
        let p_3 = PartitionId::new(12);
        let partitions = vec![p_1, p_2, p_3];

        let source =
            LoggingPartitionsSourceWrapper::new(MockPartitionsSource::new(partitions.clone()));
        let capture = TracingCapture::new();
        assert_eq!(source.fetch().await, partitions,);
        // just the ordinary log message, no warning
        assert_eq!(
            capture.to_string(),
            "level = INFO; message = Fetch partitions; n_partitions = 3; ",
        );
    }
}
