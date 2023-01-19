use std::fmt::{Debug, Display};

use data_types::ParquetFile;

pub mod and;
pub mod has_files;

pub trait PartitionFilter: Debug + Display + Send + Sync {
    fn apply(&self, files: &[ParquetFile]) -> bool;
}
