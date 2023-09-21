use std::fmt::Display;

use async_trait::async_trait;

use crate::{error::DynError, PartitionInfo};

use super::PartitionFilter;

#[derive(Debug, Default)]
pub struct HasFilesPartitionFilter;

impl HasFilesPartitionFilter {
    pub fn new() -> Self {
        Self
    }
}

impl Display for HasFilesPartitionFilter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "has_files")
    }
}

#[async_trait]
impl PartitionFilter for HasFilesPartitionFilter {
    async fn apply(
        &self,
        _partition_info: &PartitionInfo,
        files: &[data_types::ParquetFile],
    ) -> Result<bool, DynError> {
        Ok(!files.is_empty())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use iox_tests::ParquetFileBuilder;

    use crate::test_utils::PartitionInfoBuilder;

    use super::*;

    #[test]
    fn test_display() {
        assert_eq!(HasFilesPartitionFilter::new().to_string(), "has_files");
    }

    #[tokio::test]
    async fn test_apply() {
        let filter = HasFilesPartitionFilter::new();
        let f = ParquetFileBuilder::new(0).build();
        let p_info = Arc::new(PartitionInfoBuilder::new().build());

        assert!(!filter.apply(&p_info, &[]).await.unwrap());
        assert!(filter.apply(&p_info, &[f]).await.unwrap());
    }
}
