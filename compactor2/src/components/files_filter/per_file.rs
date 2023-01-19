use std::fmt::Display;

use crate::components::file_filter::FileFilter;

use super::FilesFilter;

#[derive(Debug)]
pub struct PerFileFilesFilter<T>
where
    T: FileFilter,
{
    inner: T,
}

impl<T> PerFileFilesFilter<T>
where
    T: FileFilter,
{
    #[allow(dead_code)] // not used anywhere
    pub fn new(inner: T) -> Self {
        Self { inner }
    }
}

impl<T> Display for PerFileFilesFilter<T>
where
    T: FileFilter,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "per_file({})", self.inner)
    }
}

impl<T> FilesFilter for PerFileFilesFilter<T>
where
    T: FileFilter,
{
    fn apply(&self, files: Vec<data_types::ParquetFile>) -> Vec<data_types::ParquetFile> {
        files
            .into_iter()
            .filter(|file| self.inner.apply(file))
            .collect()
    }
}
