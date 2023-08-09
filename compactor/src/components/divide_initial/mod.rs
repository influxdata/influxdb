use std::fmt::{Debug, Display};

use data_types::ParquetFile;

use crate::RoundInfo;

pub mod multiple_branches;

pub trait DivideInitial: Debug + Display + Send + Sync {
    /// Divides a group of files that should be compacted into
    /// potentially smaller groups called "branches",
    ///
    /// Each branch is compacted together in a single plan, and each
    /// compact plan may produce one or more parquet files.
    fn divide(
        &self,
        files: Vec<ParquetFile>,
        round_info: RoundInfo,
    ) -> (Vec<Vec<ParquetFile>>, Vec<ParquetFile>);
}
