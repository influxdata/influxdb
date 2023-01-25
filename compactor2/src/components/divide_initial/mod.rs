use std::fmt::{Debug, Display};

use data_types::ParquetFile;

pub mod single_branch;

pub trait DivideInitial: Debug + Display + Send + Sync {
    fn divide(&self, files: Vec<ParquetFile>) -> Vec<Vec<ParquetFile>>;
}
