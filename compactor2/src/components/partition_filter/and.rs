use std::{fmt::Display, sync::Arc};

use data_types::ParquetFile;

use super::PartitionFilter;

#[derive(Debug)]
pub struct AndPartitionFilter {
    filters: Vec<Arc<dyn PartitionFilter>>,
}

impl AndPartitionFilter {
    pub fn new(filters: Vec<Arc<dyn PartitionFilter>>) -> Self {
        Self { filters }
    }
}

impl Display for AndPartitionFilter {
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

impl PartitionFilter for AndPartitionFilter {
    fn apply(&self, files: &[ParquetFile]) -> bool {
        self.filters.iter().all(|filter| filter.apply(files))
    }
}
