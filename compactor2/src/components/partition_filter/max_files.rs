use std::fmt::Display;

use async_trait::async_trait;
use data_types::{ParquetFile, PartitionId};

use super::PartitionFilter;

#[derive(Debug)]
pub struct MaxFilesPartitionFilter {
    max_files: usize,
}

impl MaxFilesPartitionFilter {
    pub fn new(max_files: usize) -> Self {
        Self { max_files }
    }
}

impl Display for MaxFilesPartitionFilter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "max_files")
    }
}

#[async_trait]
impl PartitionFilter for MaxFilesPartitionFilter {
    async fn apply(&self, _partition_id: PartitionId, files: &[ParquetFile]) -> bool {
        files.len() <= self.max_files
    }
}

#[cfg(test)]
mod tests {
    use crate::test_util::ParquetFileBuilder;

    use super::*;

    #[test]
    fn test_display() {
        assert_eq!(MaxFilesPartitionFilter::new(10).to_string(), "max_files");
    }

    #[tokio::test]
    async fn test_apply() {
        let filter = MaxFilesPartitionFilter::new(2);
        let f1 = ParquetFileBuilder::new(1).build();
        let f2 = ParquetFileBuilder::new(2).build();
        let f3 = ParquetFileBuilder::new(3).build();
        let p_id = PartitionId::new(1);

        assert!(filter.apply(p_id, &[]).await);
        assert!(filter.apply(p_id, &[f1.clone()]).await);
        assert!(filter.apply(p_id, &[f1.clone(), f2.clone()]).await);
        assert!(!filter.apply(p_id, &[f1, f2, f3]).await);
    }
}
