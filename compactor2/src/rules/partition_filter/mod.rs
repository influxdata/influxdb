use std::fmt::Debug;

use data_types::ParquetFile;

pub trait PartitionFilter: Debug + Send + Sync {
    fn apply(&self, files: &[ParquetFile]) -> bool;

    fn name(&self) -> &'static str;
}
