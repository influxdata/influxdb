use std::fmt::Display;

use super::PartitionFilter;

#[derive(Debug, Default)]
pub struct HasFilesPartitionFilter;

impl HasFilesPartitionFilter {
    pub fn new() -> Self {
        Self::default()
    }
}

impl Display for HasFilesPartitionFilter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "has_files")
    }
}

impl PartitionFilter for HasFilesPartitionFilter {
    fn apply(&self, files: &[data_types::ParquetFile]) -> bool {
        !files.is_empty()
    }
}
