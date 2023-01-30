use std::fmt::Display;

use async_trait::async_trait;
use data_types::{ParquetFile, PartitionId};

use super::PartitionFilter;

#[derive(Debug)]
pub struct MaxParquetBytesPartitionFilter {
    max_parquet_bytes: usize,
}

impl MaxParquetBytesPartitionFilter {
    pub fn new(max_parquet_bytes: usize) -> Self {
        Self { max_parquet_bytes }
    }
}

impl Display for MaxParquetBytesPartitionFilter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "max_parquet_bytes")
    }
}

#[async_trait]
impl PartitionFilter for MaxParquetBytesPartitionFilter {
    async fn apply(&self, _partition_id: PartitionId, files: &[ParquetFile]) -> bool {
        files
            .iter()
            .map(|f| usize::try_from(f.file_size_bytes).unwrap_or(0))
            .sum::<usize>()
            <= self.max_parquet_bytes
    }
}

#[cfg(test)]
mod tests {
    use crate::test_util::ParquetFileBuilder;

    use super::*;

    #[test]
    fn test_display() {
        assert_eq!(
            MaxParquetBytesPartitionFilter::new(10).to_string(),
            "max_parquet_bytes"
        );
    }

    #[tokio::test]
    async fn test_apply() {
        let filter = MaxParquetBytesPartitionFilter::new(10);
        let f1 = ParquetFileBuilder::new(1).with_file_size_bytes(7).build();
        let f2 = ParquetFileBuilder::new(2).with_file_size_bytes(4).build();
        let f3 = ParquetFileBuilder::new(3).with_file_size_bytes(3).build();
        let p_id = PartitionId::new(1);

        assert!(filter.apply(p_id, &[]).await);
        assert!(filter.apply(p_id, &[f1.clone()]).await);
        assert!(filter.apply(p_id, &[f1.clone(), f3.clone()]).await);
        assert!(!filter.apply(p_id, &[f1, f2]).await);
    }
}
