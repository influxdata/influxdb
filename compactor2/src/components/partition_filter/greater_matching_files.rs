use std::fmt::Display;

use async_trait::async_trait;
use data_types::{ParquetFile, PartitionId};

use crate::{components::file_filter::FileFilter, error::DynError};

use super::PartitionFilter;

/// A partition filter that matches partitions that have more than `min_num_files` files
/// matching the given file filter.
#[derive(Debug)]
pub struct GreaterMatchingFilesPartitionFilter<T>
where
    T: FileFilter,
{
    filter: T,
    min_num_files: usize,
}

impl<T> GreaterMatchingFilesPartitionFilter<T>
where
    T: FileFilter,
{
    pub fn new(filter: T, min_num_files: usize) -> Self {
        Self {
            filter,
            min_num_files,
        }
    }
}

impl<T> Display for GreaterMatchingFilesPartitionFilter<T>
where
    T: FileFilter,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "greater_matching_file({}, {})",
            self.filter, self.min_num_files
        )
    }
}

#[async_trait]
impl<T> PartitionFilter for GreaterMatchingFilesPartitionFilter<T>
where
    T: FileFilter,
{
    async fn apply(
        &self,
        _partition_id: PartitionId,
        files: &[ParquetFile],
    ) -> Result<bool, DynError> {
        Ok(files.iter().filter(|file| self.filter.apply(file)).count() >= self.min_num_files)
    }
}

#[cfg(test)]
mod tests {
    use data_types::CompactionLevel;

    use crate::{
        components::file_filter::level_range::LevelRangeFileFilter, test_util::ParquetFileBuilder,
    };

    use super::*;

    #[test]
    fn test_display() {
        let filter = GreaterMatchingFilesPartitionFilter::new(
            LevelRangeFileFilter::new(
                CompactionLevel::FileNonOverlapped..=CompactionLevel::FileNonOverlapped,
            ),
            1,
        );
        assert_eq!(
            filter.to_string(),
            "greater_matching_file(level_range(1..=1), 1)"
        );
    }

    #[tokio::test]
    async fn test_apply() {
        let filter = GreaterMatchingFilesPartitionFilter::new(
            LevelRangeFileFilter::new(
                CompactionLevel::FileNonOverlapped..=CompactionLevel::FileNonOverlapped,
            ),
            2,
        );
        let f1 = ParquetFileBuilder::new(0)
            .with_compaction_level(CompactionLevel::FileNonOverlapped)
            .build();
        let f2 = ParquetFileBuilder::new(1)
            .with_compaction_level(CompactionLevel::FileNonOverlapped)
            .build();
        let f3 = ParquetFileBuilder::new(2)
            .with_compaction_level(CompactionLevel::FileNonOverlapped)
            .build();

        let p_id = PartitionId::new(1);

        // empty, not enough matching
        assert!(!filter.apply(p_id, &[]).await.unwrap());

        // Not enough matching
        assert!(!filter.apply(p_id, &[f1.clone()]).await.unwrap());

        // enough matching
        assert!(filter.apply(p_id, &[f1.clone(), f2.clone()]).await.unwrap());

        // enough matching
        assert!(filter.apply(p_id, &[f1, f2, f3]).await.unwrap());
    }
}
