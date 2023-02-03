use std::fmt::{Debug, Display};

use data_types::CompactionLevel;

pub mod hot_cold;
pub mod naive;

pub trait TargetLevelDetection: Debug + Display + Send + Sync {
    /// return the compaction level the given files are suitable to get compacted to
    fn detect(&self, files: &[data_types::ParquetFile]) -> Option<CompactionLevel>;
}
