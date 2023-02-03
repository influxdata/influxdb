use std::fmt::{Debug, Display};

use data_types::CompactionLevel;

pub mod one_level;

pub trait LevelFilter: Debug + Display + Send + Sync {
    /// return true if this filter has the given level
    fn apply(&self, files: &[data_types::ParquetFile], level: CompactionLevel) -> bool;
}
