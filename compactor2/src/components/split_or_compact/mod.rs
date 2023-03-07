use std::fmt::{Debug, Display};

use data_types::{CompactionLevel, ParquetFile};

use crate::{file_classification::FilesToCompactOrSplit, PartitionInfo};

pub mod files_to_compact;
pub mod files_to_split;
pub mod logging;
pub mod split_compact;

pub trait SplitOrCompact: Debug + Display + Send + Sync {
    /// Return (`[files_to_split_or_compact]`, `[files_to_keep]`) of given files
    /// `files_to_keep` are files that are not part of the compaction of this round but they
    /// are kept to get compacted in the next round
    fn apply(
        &self,
        partition_info: &PartitionInfo,
        files: Vec<ParquetFile>,
        target_level: CompactionLevel,
    ) -> (FilesToCompactOrSplit, Vec<ParquetFile>);
}
