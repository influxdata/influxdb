use std::fmt::Display;

use async_trait::async_trait;
use data_types::{ParquetFile, PartitionId};
use observability_deps::tracing::{debug, info};

use super::PartitionFilter;

#[derive(Debug)]
pub struct LoggingPartitionFilterWrapper<T>
where
    T: PartitionFilter,
{
    inner: T,
}

impl<T> LoggingPartitionFilterWrapper<T>
where
    T: PartitionFilter,
{
    pub fn new(inner: T) -> Self {
        Self { inner }
    }
}

impl<T> Display for LoggingPartitionFilterWrapper<T>
where
    T: PartitionFilter,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "logging({})", self.inner)
    }
}

#[async_trait]
impl<T> PartitionFilter for LoggingPartitionFilterWrapper<T>
where
    T: PartitionFilter,
{
    async fn apply(&self, partition_id: PartitionId, files: &[ParquetFile]) -> bool {
        let res = self.inner.apply(partition_id, files).await;
        if !res {
            info!(partition_id = partition_id.get(), "filtered partition");
        } else {
            debug!(partition_id = partition_id.get(), "NOT filtered partition");
        }
        res
    }
}

#[cfg(test)]
mod tests {
    use test_helpers::tracing::TracingCapture;

    use crate::{
        components::partition_filter::has_files::HasFilesPartitionFilter,
        test_util::ParquetFileBuilder,
    };

    use super::*;

    #[test]
    fn test_display() {
        let filter = LoggingPartitionFilterWrapper::new(HasFilesPartitionFilter::new());
        assert_eq!(filter.to_string(), "logging(has_files)");
    }

    #[tokio::test]
    async fn test_apply() {
        let filter = LoggingPartitionFilterWrapper::new(HasFilesPartitionFilter::new());
        let f = ParquetFileBuilder::new(0).build();
        let p_id1 = PartitionId::new(1);
        let p_id2 = PartitionId::new(2);

        let capture = TracingCapture::new();

        assert!(!filter.apply(p_id1, &[]).await);
        assert!(filter.apply(p_id2, &[f]).await);

        assert_eq!(
            capture.to_string(),
            "level = INFO; message = filtered partition; partition_id = 1; \n\
level = DEBUG; message = NOT filtered partition; partition_id = 2; ",
        );
    }
}
