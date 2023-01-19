use std::{fmt::Display, sync::Arc};

use super::FilesFilter;

#[derive(Debug)]
pub struct FilesFilterChain {
    filters: Vec<Arc<dyn FilesFilter>>,
}

impl FilesFilterChain {
    pub fn new(filters: Vec<Arc<dyn FilesFilter>>) -> Self {
        Self { filters }
    }
}

impl Display for FilesFilterChain {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "chain([")?;
        for (i, sub) in self.filters.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{sub}")?;
        }
        write!(f, "])")
    }
}

impl FilesFilter for FilesFilterChain {
    fn apply(&self, mut files: Vec<data_types::ParquetFile>) -> Vec<data_types::ParquetFile> {
        for filter in &self.filters {
            files = filter.apply(files);
        }

        files
    }
}
