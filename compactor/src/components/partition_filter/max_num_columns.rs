use std::fmt::Display;

use async_trait::async_trait;
use data_types::ParquetFile;

use crate::{
    error::{DynError, ErrorKind, SimpleError},
    PartitionInfo,
};

use super::PartitionFilter;

#[derive(Debug)]
pub struct MaxNumColumnsPartitionFilter {
    max_num_columns: usize,
}

impl MaxNumColumnsPartitionFilter {
    pub fn new(max_num_columns: usize) -> Self {
        Self { max_num_columns }
    }
}

impl Display for MaxNumColumnsPartitionFilter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "max_num_columns")
    }
}

#[async_trait]
impl PartitionFilter for MaxNumColumnsPartitionFilter {
    async fn apply(
        &self,
        partition_info: &PartitionInfo,
        _files: &[ParquetFile],
    ) -> Result<bool, DynError> {
        let col_count = partition_info.column_count();
        if col_count <= self.max_num_columns {
            Ok(true)
        } else {
            Err(SimpleError::new(
                ErrorKind::OutOfMemory,
                format!(
                    "table of partition {} has {} number of columns, limit is {}",
                    partition_info.partition_id, col_count, self.max_num_columns
                ),
            )
            .into())
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::{error::ErrorKindExt, test_utils::PartitionInfoBuilder};
    use iox_tests::ParquetFileBuilder;

    use super::*;

    #[test]
    fn test_display() {
        assert_eq!(
            MaxNumColumnsPartitionFilter::new(1).to_string(),
            "max_num_columns"
        );
    }

    #[tokio::test]
    async fn test_apply_skip() {
        let filter = MaxNumColumnsPartitionFilter::new(2);
        let f1 = ParquetFileBuilder::new(1).with_file_size_bytes(7).build();
        let f2 = ParquetFileBuilder::new(2).with_file_size_bytes(4).build();

        let p_info = Arc::new(
            PartitionInfoBuilder::new()
                .with_partition_id(1)
                .with_num_columns(3)
                .build(),
        );

        let err = filter.apply(&p_info, &[f1, f2]).await.unwrap_err();
        assert_eq!(err.classify(), ErrorKind::OutOfMemory);
        assert_eq!(
            err.to_string(),
            "table of partition 1 has 3 number of columns, limit is 2"
        );

        // empty files
        // This filter doea not look into the file set, so it should not fail
        let err = filter.apply(&p_info, &[]).await.unwrap_err();
        assert_eq!(err.classify(), ErrorKind::OutOfMemory);
        assert_eq!(
            err.to_string(),
            "table of partition 1 has 3 number of columns, limit is 2"
        );
    }

    #[tokio::test]
    async fn test_apply() {
        let filter = MaxNumColumnsPartitionFilter::new(5);
        let f1 = ParquetFileBuilder::new(1).with_file_size_bytes(7).build();
        let f2 = ParquetFileBuilder::new(2).with_file_size_bytes(4).build();

        let p_info = Arc::new(
            PartitionInfoBuilder::new()
                .with_partition_id(1)
                .with_num_columns(3)
                .build(),
        );

        assert!(filter.apply(&p_info, &[f1, f2]).await.unwrap());
        assert!(filter.apply(&p_info, &[]).await.unwrap());
    }
}
