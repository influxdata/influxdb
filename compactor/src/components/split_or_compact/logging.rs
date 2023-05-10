use std::fmt::Display;

use data_types::{CompactionLevel, ParquetFile};
use observability_deps::tracing::info;

use crate::{file_classification::FilesToSplitOrCompact, partition_info::PartitionInfo};

use super::SplitOrCompact;

#[derive(Debug)]
pub struct LoggingSplitOrCompactWrapper<T>
where
    T: SplitOrCompact,
{
    inner: T,
}

impl<T> LoggingSplitOrCompactWrapper<T>
where
    T: SplitOrCompact,
{
    pub fn new(inner: T) -> Self {
        Self { inner }
    }
}

impl<T> Display for LoggingSplitOrCompactWrapper<T>
where
    T: SplitOrCompact,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "display({})", self.inner)
    }
}

impl<T> SplitOrCompact for LoggingSplitOrCompactWrapper<T>
where
    T: SplitOrCompact,
{
    fn apply(
        &self,
        partition_info: &PartitionInfo,
        files: Vec<ParquetFile>,
        target_level: CompactionLevel,
    ) -> (FilesToSplitOrCompact, Vec<ParquetFile>) {
        let (files_to_split_or_compact, files_to_keep) =
            self.inner.apply(partition_info, files, target_level);

        info!(
            partition_id = partition_info.partition_id.get(),
            target_level = %target_level,
            files_to_compact = files_to_split_or_compact.num_files_to_compact(),
            files_to_split = files_to_split_or_compact.num_files_to_split(),
            files_to_keep = files_to_keep.len(),
            "split or compact"
        );

        (files_to_split_or_compact, files_to_keep)
    }
}
