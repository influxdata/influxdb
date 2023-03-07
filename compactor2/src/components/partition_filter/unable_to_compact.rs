use std::fmt::Display;

use async_trait::async_trait;
use data_types::ParquetFile;

use crate::{
    error::{DynError, ErrorKind, SimpleError},
    PartitionInfo,
};

use super::PartitionFilter;

#[derive(Debug)]
pub struct UnableToCompactPartitionFilter {
    max_parquet_bytes: usize,
}

impl UnableToCompactPartitionFilter {
    pub fn new(max_parquet_bytes: usize) -> Self {
        Self { max_parquet_bytes }
    }
}

impl Display for UnableToCompactPartitionFilter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "unable_to_compact")
    }
}

#[async_trait]
impl PartitionFilter for UnableToCompactPartitionFilter {
    async fn apply(
        &self,
        partition_info: &PartitionInfo,
        files: &[ParquetFile],
    ) -> Result<bool, DynError> {
        if !files.is_empty() {
            // There is some files to compact or split
            Ok(true)
        } else {
            // No files means the split_compact cannot find any reasonable set of files to compact or split
            // TODO: after https://github.com/influxdata/idpe/issues/17208 that renames the size limit and
            // https://github.com/influxdata/idpe/issues/17209 for modifying the knobs, this message should be modified accordingly
            Err(SimpleError::new(
                ErrorKind::OutOfMemory,
                format!(
                    "partition {} has overlapped files that exceed max compact size limit {}. The may happen if a large amount of data has the same timestamp",
                    partition_info.partition_id, self.max_parquet_bytes
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
            UnableToCompactPartitionFilter::new(10).to_string(),
            "unable_to_compact"
        );
    }

    #[tokio::test]
    async fn test_apply_empty() {
        let filter = UnableToCompactPartitionFilter::new(10);
        let p_info = Arc::new(PartitionInfoBuilder::new().with_partition_id(1).build());
        let err = filter.apply(&p_info, &[]).await.unwrap_err();
        assert_eq!(err.classify(), ErrorKind::OutOfMemory);
        assert_eq!(
            err.to_string(),
            "partition 1 has overlapped files that exceed max compact size limit 10. The may happen if a large amount of data has the same timestamp"
        );
    }

    #[tokio::test]
    async fn test_apply_not_empty() {
        let filter = UnableToCompactPartitionFilter::new(10);
        let p_info = Arc::new(PartitionInfoBuilder::new().with_partition_id(1).build());
        let f1 = ParquetFileBuilder::new(1).with_file_size_bytes(7).build();
        assert!(filter.apply(&p_info, &[f1]).await.unwrap());
    }
}
