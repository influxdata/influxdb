use std::fmt::{Debug, Display};

use data_types::{CompactionLevel, ParquetFile};

pub mod all_at_once_target_level_split;
pub mod target_level_target_level_split;

pub trait FilesSplit: Debug + Display + Send + Sync {
    /// Split provided files into 2 groups of files. There will be different split needs:
    ///  . `[files <= target_level]` and `[files > target_level]`
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
