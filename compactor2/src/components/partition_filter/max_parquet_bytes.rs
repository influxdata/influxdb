use std::fmt::Display;

use async_trait::async_trait;
use data_types::{ParquetFile, PartitionId};

use crate::error::{DynError, ErrorKind, SimpleError};

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
    async fn apply(
        &self,
        partition_id: PartitionId,
        files: &[ParquetFile],
    ) -> Result<bool, DynError> {
        let sum = files
            .iter()
            .map(|f| usize::try_from(f.file_size_bytes).unwrap_or(0))
            .sum::<usize>();

        if sum <= self.max_parquet_bytes {
            Ok(true)
        } else {
            Err(SimpleError::new(
                ErrorKind::OutOfMemory,
                format!(
                    "partition {} has {} parquet file bytes, limit is {}",
                    partition_id, sum, self.max_parquet_bytes
                ),
            )
            .into())
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{error::ErrorKindExt, test_util::ParquetFileBuilder};

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

        assert!(filter.apply(p_id, &[]).await.unwrap());
        assert!(filter.apply(p_id, &[f1.clone()]).await.unwrap());
        assert!(filter.apply(p_id, &[f1.clone(), f3.clone()]).await.unwrap());

        let err = filter.apply(p_id, &[f1, f2]).await.unwrap_err();
        assert_eq!(err.classify(), ErrorKind::OutOfMemory);
        assert_eq!(
            err.to_string(),
            "partition 1 has 11 parquet file bytes, limit is 10"
        );
    }
}
