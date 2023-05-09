use std::fmt::Display;

use async_trait::async_trait;
use data_types::ParquetFile;

use crate::{components::file_filter::FileFilter, error::DynError, PartitionInfo};

use super::PartitionFilter;

#[derive(Debug)]
pub struct HasMatchingFilePartitionFilter<T>
where
    T: FileFilter,
{
    filter: T,
}

impl<T> HasMatchingFilePartitionFilter<T>
where
    T: FileFilter,
{
    pub fn new(filter: T) -> Self {
        Self { filter }
    }
}

impl<T> Display for HasMatchingFilePartitionFilter<T>
where
    T: FileFilter,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "has_matching_file({})", self.filter)
    }
}

#[async_trait]
impl<T> PartitionFilter for HasMatchingFilePartitionFilter<T>
where
    T: FileFilter,
{
    async fn apply(
        &self,
        _partition_info: &PartitionInfo,
        files: &[ParquetFile],
    ) -> Result<bool, DynError> {
        Ok(files.iter().any(|file| self.filter.apply(file)))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use data_types::CompactionLevel;

    use crate::{
        components::file_filter::level_range::LevelRangeFileFilter,
        test_utils::PartitionInfoBuilder,
    };
    use iox_tests::ParquetFileBuilder;

    use super::*;

    #[test]
    fn test_display() {
        let filter = HasMatchingFilePartitionFilter::new(LevelRangeFileFilter::new(
            CompactionLevel::Initial..=CompactionLevel::FileNonOverlapped,
        ));
        assert_eq!(filter.to_string(), "has_matching_file(level_range(0..=1))");
    }

    #[tokio::test]
    async fn test_apply() {
        let filter = HasMatchingFilePartitionFilter::new(LevelRangeFileFilter::new(
            CompactionLevel::Initial..=CompactionLevel::FileNonOverlapped,
        ));
        let f1 = ParquetFileBuilder::new(0)
            .with_compaction_level(CompactionLevel::FileNonOverlapped)
            .build();
        let f2 = ParquetFileBuilder::new(1)
            .with_compaction_level(CompactionLevel::Final)
            .build();

        let p_info = Arc::new(PartitionInfoBuilder::new().build());

        // empty
        assert!(!filter.apply(&p_info, &[]).await.unwrap());

        // all matching
        assert!(filter.apply(&p_info, &[f1.clone()]).await.unwrap());

        // none matching
        assert!(!filter.apply(&p_info, &[f2.clone()]).await.unwrap());

        // some matching
        assert!(filter.apply(&p_info, &[f1, f2]).await.unwrap());
    }
}
