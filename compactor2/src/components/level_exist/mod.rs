use std::fmt::{Debug, Display};

use data_types::CompactionLevel;

pub mod one_level;

pub trait LevelExist: Debug + Display + Send + Sync {
    /// return true if at least one file in the given level
    fn apply(&self, files: &[data_types::ParquetFile], level: CompactionLevel) -> bool;
}
