use std::fmt::Debug;

use data_types::ParquetFile;

pub trait FileFilter: Debug + Send + Sync {
    fn apply(&self, file: &ParquetFile) -> bool;

    fn name(&self) -> &'static str;
}
