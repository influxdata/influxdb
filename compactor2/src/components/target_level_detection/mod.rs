use std::fmt::{Debug, Display};

use data_types::CompactionLevel;

pub mod all_at_once;
pub mod target_level;

pub trait TargetLevelDetection: Debug + Display + Send + Sync {
    /// Return the compaction level the given files are suitable to get compacted to
    fn detect(&self, files: &[data_types::ParquetFile]) -> CompactionLevel;
}
