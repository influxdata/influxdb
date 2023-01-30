use std::{collections::HashSet, fmt::Display};

use async_trait::async_trait;
use data_types::{ParquetFile, PartitionId};

use super::PartitionFilter;

#[derive(Debug)]
pub struct ByIdPartitionFilter {
    ids: HashSet<PartitionId>,
}

impl ByIdPartitionFilter {
    pub fn new(ids: HashSet<PartitionId>) -> Self {
        Self { ids }
    }
}

impl Display for ByIdPartitionFilter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "by_id")
    }
}

#[async_trait]
impl PartitionFilter for ByIdPartitionFilter {
    async fn apply(&self, partition_id: PartitionId, _files: &[ParquetFile]) -> bool {
        self.ids.contains(&partition_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_display() {
        assert_eq!(
            ByIdPartitionFilter::new(HashSet::default()).to_string(),
            "by_id"
        );
    }

    #[tokio::test]
    async fn test_apply() {
        let filter =
            ByIdPartitionFilter::new(HashSet::from([PartitionId::new(1), PartitionId::new(10)]));

        assert!(filter.apply(PartitionId::new(1), &[]).await);
        assert!(filter.apply(PartitionId::new(10), &[]).await);
        assert!(!filter.apply(PartitionId::new(2), &[]).await);
    }
}
