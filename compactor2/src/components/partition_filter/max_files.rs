use std::fmt::Display;

use async_trait::async_trait;
use data_types::{ParquetFile, PartitionId};

use crate::error::{DynError, ErrorKind, SimpleError};

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
    async fn apply(
        &self,
        partition_id: PartitionId,
        files: &[ParquetFile],
    ) -> Result<bool, DynError> {
        if files.len() <= self.max_files {
            Ok(true)
        } else {
            Err(SimpleError::new(
                ErrorKind::OutOfMemory,
                format!(
                    "partition {} has {} files, limit is {}",
                    partition_id,
                    files.len(),
                    self.max_files
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
        assert_eq!(MaxFilesPartitionFilter::new(10).to_string(), "max_files");
    }

    #[tokio::test]
    async fn test_apply() {
        let filter = MaxFilesPartitionFilter::new(2);
        let f1 = ParquetFileBuilder::new(1).build();
        let f2 = ParquetFileBuilder::new(2).build();
        let f3 = ParquetFileBuilder::new(3).build();
        let p_id = PartitionId::new(1);

        assert!(filter.apply(p_id, &[]).await.unwrap());
        assert!(filter.apply(p_id, &[f1.clone()]).await.unwrap());
        assert!(filter.apply(p_id, &[f1.clone(), f2.clone()]).await.unwrap());

        let e = filter.apply(p_id, &[f1, f2, f3]).await.unwrap_err();
        assert_eq!(e.classify(), ErrorKind::OutOfMemory);
        assert_eq!(e.to_string(), "partition 1 has 3 files, limit is 2");
    }
}
