use std::{fmt::Display, sync::Arc};

use async_trait::async_trait;
use data_types::{ParquetFile, PartitionId};

use super::PartitionFilter;

#[derive(Debug)]
pub struct AndPartitionFilter {
    filters: Vec<Arc<dyn PartitionFilter>>,
}

impl AndPartitionFilter {
    pub fn new(filters: Vec<Arc<dyn PartitionFilter>>) -> Self {
        Self { filters }
    }
}

impl Display for AndPartitionFilter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "and([")?;
        for (i, sub) in self.filters.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{sub}")?;
        }
        write!(f, "])")
    }
}

#[async_trait]
impl PartitionFilter for AndPartitionFilter {
    async fn apply(&self, partition_id: PartitionId, files: &[ParquetFile]) -> bool {
        for filter in &self.filters {
            if !filter.apply(partition_id, files).await {
                return false;
            }
        }
        true
    }
}
