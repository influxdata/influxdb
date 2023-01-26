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
