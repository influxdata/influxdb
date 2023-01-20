use std::{fmt::Display, sync::Arc};

use data_types::ParquetFile;

use super::FileFilter;

#[derive(Debug)]
pub struct AndFileFilter {
    filters: Vec<Arc<dyn FileFilter>>,
}

impl AndFileFilter {
    pub fn new(filters: Vec<Arc<dyn FileFilter>>) -> Self {
        Self { filters }
    }
}

impl Display for AndFileFilter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "and([")?;
        for (i, sub) in self.filters.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{sub}")?;
        }
        write!(f, "])")
    }
}

impl FileFilter for AndFileFilter {
    fn apply(&self, file: &ParquetFile) -> bool {
        self.filters.iter().all(|filter| filter.apply(file))
    }
}
