use std::fmt::{Debug, Display};

use data_types::ParquetFile;

pub mod chain;
pub mod per_file;

/// Filter that is applied to all files of a single partition at the same time.
///
/// Use should only be used if you need a global view on all files
/// within a partition. For per-file filtering, use
/// [`PerFileFilesFilter`] in combination with [`FileFilter`].
///
/// [`FileFilter`]: crate::components::file_filter::FileFilter
/// [`PerFileFilesFilter`]: crate::components::files_filter::per_file::PerFileFilesFilter
pub trait FilesFilter: Debug + Display + Send + Sync {
    fn apply(&self, files: Vec<ParquetFile>) -> Vec<ParquetFile>;
}
