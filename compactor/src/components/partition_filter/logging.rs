use std::fmt::Display;

use async_trait::async_trait;
use data_types::ParquetFile;
use observability_deps::tracing::{debug, error, info};

use crate::{error::DynError, PartitionInfo};

use super::PartitionFilter;

#[derive(Debug)]
pub struct LoggingPartitionFilterWrapper<T>
where
    T: PartitionFilter,
{
    inner: T,
    filter_type: &'static str,
}

impl<T> LoggingPartitionFilterWrapper<T>
where
    T: PartitionFilter,
{
    pub fn new(inner: T, filter_type: &'static str) -> Self {
        Self { inner, filter_type }
    }
}

impl<T> Display for LoggingPartitionFilterWrapper<T>
where
    T: PartitionFilter,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "logging({}, {})", self.inner, self.filter_type)
    }
}

#[async_trait]
impl<T> PartitionFilter for LoggingPartitionFilterWrapper<T>
where
    T: PartitionFilter,
{
    async fn apply(
        &self,
        partition_info: &PartitionInfo,
        files: &[ParquetFile],
    ) -> Result<bool, DynError> {
        let res = self.inner.apply(partition_info, files).await;
        match &res {
            Ok(true) => {
                debug!(
                    partition_id = partition_info.partition_id.get(),
                    filter_type = self.filter_type,
                    "NOT filtered partition"
                );
            }
            Ok(false) => {
                info!(
                    partition_id = partition_info.partition_id.get(),
                    filter_type = self.filter_type,
                    "filtered partition"
                );
            }
            Err(e) => {
                error!(partition_id = partition_info.partition_id.get(), filter_type = self.filter_type, %e, "error filtering filtered partition");
            }
        }
        res
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use test_helpers::tracing::TracingCapture;

    use crate::{
        components::partition_filter::has_files::HasFilesPartitionFilter,
        test_utils::PartitionInfoBuilder,
    };
    use iox_tests::ParquetFileBuilder;

    use super::*;

    #[test]
    fn test_display() {
        let filter = LoggingPartitionFilterWrapper::new(HasFilesPartitionFilter::new(), "test");
        assert_eq!(filter.to_string(), "logging(has_files, test)");
    }

    #[tokio::test]
    async fn test_apply() {
        let filter = LoggingPartitionFilterWrapper::new(HasFilesPartitionFilter::new(), "test");
        let f = ParquetFileBuilder::new(0).build();
        let p_info1 = Arc::new(PartitionInfoBuilder::new().with_partition_id(1).build());
        let p_info2 = Arc::new(PartitionInfoBuilder::new().with_partition_id(2).build());

        let capture = TracingCapture::new();

        assert!(!filter.apply(&p_info1, &[]).await.unwrap());
        assert!(filter.apply(&p_info2, &[f]).await.unwrap());

        assert_eq!(
            capture.to_string(),
            "level = INFO; message = filtered partition; partition_id = 1; filter_type = \"test\"; 
level = DEBUG; message = NOT filtered partition; partition_id = 2; filter_type = \"test\"; ",
        );
    }
}
