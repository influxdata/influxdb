use std::fmt::Display;

use async_trait::async_trait;
use data_types::ParquetFile;

use crate::{components::file_filter::FileFilter, error::DynError, PartitionInfo};

use super::PartitionFilter;

/// A partition filter that matches partitions that have files
/// matching the given file filter and their total size > max_desired_file_bytes
/// The idea for doing this:
///  1. Not to compact large input size to avoid hitting OOM/crash.
///  2. Not to compact too-large input size that lead to unecessary split into many files.
///     - Because we limit the size of a file. If the compacted result is too large, we will split them into many files.
///     - Because Level-1 files do not overlap, it is a waste to compact too-large size and then split.
#[derive(Debug)]
pub struct GreaterSizeMatchingFilesPartitionFilter<T>
where
    T: FileFilter,
{
    filter: T,
    max_desired_file_bytes: u64,
}

impl<T> GreaterSizeMatchingFilesPartitionFilter<T>
where
    T: FileFilter,
{
    pub fn new(filter: T, max_desired_file_bytes: u64) -> Self {
        Self {
            filter,
            max_desired_file_bytes,
        }
    }
}

impl<T> Display for GreaterSizeMatchingFilesPartitionFilter<T>
where
    T: FileFilter,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "greater_size_matching_file({}, {})",
            self.filter, self.max_desired_file_bytes
        )
    }
}

#[async_trait]
impl<T> PartitionFilter for GreaterSizeMatchingFilesPartitionFilter<T>
where
    T: FileFilter,
{
    async fn apply(
        &self,
        _partition_info: &PartitionInfo,
        files: &[ParquetFile],
    ) -> Result<bool, DynError> {
        // Matching files
        let matching_files: Vec<&ParquetFile> = files
            .iter()
            .filter(|file| self.filter.apply(file))
            .collect();

        // Sum of file_size_bytes matching files
        let sum: i64 = matching_files.iter().map(|file| file.file_size_bytes).sum();
        Ok(sum >= self.max_desired_file_bytes as i64)
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
        let filter = GreaterSizeMatchingFilesPartitionFilter::new(
            LevelRangeFileFilter::new(
                CompactionLevel::FileNonOverlapped..=CompactionLevel::FileNonOverlapped,
            ),
            1,
        );
        assert_eq!(
            filter.to_string(),
            "greater_size_matching_file(level_range(1..=1), 1)"
        );
    }

    #[tokio::test]
    async fn test_apply() {
        let filter = GreaterSizeMatchingFilesPartitionFilter::new(
            LevelRangeFileFilter::new(
                CompactionLevel::FileNonOverlapped..=CompactionLevel::FileNonOverlapped,
            ),
            15,
        );
        let f1 = ParquetFileBuilder::new(0)
            .with_compaction_level(CompactionLevel::FileNonOverlapped)
            .with_file_size_bytes(10)
            .build();
        let f2 = ParquetFileBuilder::new(1)
            .with_compaction_level(CompactionLevel::FileNonOverlapped)
            .with_file_size_bytes(14)
            .build();
        let f3 = ParquetFileBuilder::new(2)
            .with_compaction_level(CompactionLevel::FileNonOverlapped)
            .with_file_size_bytes(15)
            .build();

        let p_info = Arc::new(PartitionInfoBuilder::new().build());

        // empty, not large enough
        assert!(!filter.apply(&p_info, &[]).await.unwrap());

        // Not large enough
        assert!(!filter.apply(&p_info, &[f1.clone()]).await.unwrap());
        assert!(!filter.apply(&p_info, &[f2.clone()]).await.unwrap());

        // large enough
        assert!(filter
            .apply(&p_info, &[f1.clone(), f2.clone()])
            .await
            .unwrap());
        assert!(filter.apply(&p_info, &[f3.clone()]).await.unwrap());
        assert!(filter.apply(&p_info, &[f1, f2, f3]).await.unwrap());
    }
}
