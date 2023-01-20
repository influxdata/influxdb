use std::fmt::{Debug, Display};

use data_types::ParquetFile;

pub mod and;
pub mod level_range;

pub trait FileFilter: Debug + Display + Send + Sync {
    fn apply(&self, file: &ParquetFile) -> bool;
}
