use std::fmt::Display;

use async_trait::async_trait;
use data_types::PartitionId;

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

#[async_trait]
impl PartitionFilter for HasFilesPartitionFilter {
    async fn apply(&self, _partition_id: PartitionId, files: &[data_types::ParquetFile]) -> bool {
        !files.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use crate::test_util::ParquetFileBuilder;

    use super::*;

    #[test]
    fn test_display() {
        assert_eq!(HasFilesPartitionFilter::new().to_string(), "has_files");
    }

    #[tokio::test]
    async fn test_apply() {
        let filter = HasFilesPartitionFilter::new();
        let f = ParquetFileBuilder::new(0).build();
        let p_id = PartitionId::new(1);

        assert!(!filter.apply(p_id, &[]).await);
        assert!(filter.apply(p_id, &[f]).await);
    }
}
