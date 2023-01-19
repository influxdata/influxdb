use std::fmt::{Debug, Display};

use data_types::ParquetFile;

pub trait PartitionFilter: Debug + Display + Send + Sync {
    fn apply(&self, files: &[ParquetFile]) -> bool;
}
