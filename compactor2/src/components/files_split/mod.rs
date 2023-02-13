use std::fmt::{Debug, Display};

use data_types::{CompactionLevel, ParquetFile};

pub mod target_level_non_overlap_split;
pub mod target_level_target_level_split;
pub mod target_level_upgrade_split;

pub trait FilesSplit: Debug + Display + Send + Sync {
    /// Split provided files into 2 groups of files:
    /// (files_to_compact, files_to_keep)
    ///
    /// Only files in files_to_compact are considered for compaction this round
    ///
    /// There will be different split needs:
    ///  . `[files <= target_level]` and `[files > target_level]`
    ///  . `[overlapping_files]` and `[non_overlapping_files]`
    ///  . `[files_to_upgrade]` and `[files_to_compact]`
    ///
    /// Note that for AllAtOnce version, we do not split anything and compact all files at once
    /// This split is mainly for version after the naive AllAtOnce. For the AllAtOnce, we will
    /// create dummy modules to return all files
    fn apply(
        &self,
        files: Vec<data_types::ParquetFile>,
        target_level: CompactionLevel,
    ) -> (Vec<ParquetFile>, Vec<ParquetFile>);
}
