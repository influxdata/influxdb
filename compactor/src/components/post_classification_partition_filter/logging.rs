use std::fmt::Display;

use async_trait::async_trait;
use observability_deps::tracing::{debug, error, info};

use crate::{error::DynError, file_classification::FilesForProgress, PartitionInfo};

use super::PostClassificationPartitionFilter;

#[derive(Debug)]
pub struct LoggingPostClassificationFilterWrapper<T>
where
    T: PostClassificationPartitionFilter,
{
    inner: T,
    filter_type: &'static str,
}

impl<T> LoggingPostClassificationFilterWrapper<T>
where
    T: PostClassificationPartitionFilter,
{
    pub fn new(inner: T, filter_type: &'static str) -> Self {
        Self { inner, filter_type }
    }
}

impl<T> Display for LoggingPostClassificationFilterWrapper<T>
where
    T: PostClassificationPartitionFilter,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "logging({}, {})", self.inner, self.filter_type)
    }
}

#[async_trait]
impl<T> PostClassificationPartitionFilter for LoggingPostClassificationFilterWrapper<T>
where
    T: PostClassificationPartitionFilter,
{
    async fn apply(
        &self,
        partition_info: &PartitionInfo,
        files_to_make_progress_on: &FilesForProgress,
    ) -> Result<bool, DynError> {
        let res = self
            .inner
            .apply(partition_info, files_to_make_progress_on)
            .await;
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
                error!(
                    partition_id = partition_info.partition_id.get(),
                    filter_type = self.filter_type,
                    %e,
                    "error filtering filtered partition"
                );
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
        components::post_classification_partition_filter::mock::MockPostClassificationPartitionFilter,
        test_utils::PartitionInfoBuilder,
    };

    use super::*;

    #[test]
    fn test_display() {
        let filter = LoggingPostClassificationFilterWrapper::new(
            MockPostClassificationPartitionFilter::new(vec![Ok(true)]),
            "test",
        );
        assert_eq!(filter.to_string(), "logging(mock, test)");
    }

    #[tokio::test]
    async fn test_apply() {
        let filter = LoggingPostClassificationFilterWrapper::new(
            MockPostClassificationPartitionFilter::new(vec![
                Ok(true),
                Ok(false),
                Err("problem".into()),
            ]),
            "test",
        );
        let p_info1 = Arc::new(PartitionInfoBuilder::new().with_partition_id(1).build());
        let p_info2 = Arc::new(PartitionInfoBuilder::new().with_partition_id(2).build());
        let p_info3 = Arc::new(PartitionInfoBuilder::new().with_partition_id(3).build());

        let capture = TracingCapture::new();

        assert!(filter
            .apply(&p_info1, &FilesForProgress::empty())
            .await
            .unwrap());
        assert!(!filter
            .apply(&p_info2, &FilesForProgress::empty())
            .await
            .unwrap());
        assert_eq!(
            filter
                .apply(&p_info3, &FilesForProgress::empty())
                .await
                .unwrap_err()
                .to_string(),
            "problem"
        );

        assert_eq!(
            capture.to_string(),
            "level = DEBUG; \
                 message = NOT filtered partition; \
                 partition_id = 1; \
                 filter_type = \"test\"; \n\
             level = INFO; \
                 message = filtered partition; \
                 partition_id = 2; \
                 filter_type = \"test\"; \n\
             level = ERROR; \
                 message = error filtering filtered partition; \
                 partition_id = 3; \
                 filter_type = \"test\"; \
                 e = problem; ",
        );
    }
}
