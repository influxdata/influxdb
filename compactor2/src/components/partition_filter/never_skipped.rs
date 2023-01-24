use std::fmt::Display;

use async_trait::async_trait;
use data_types::{ParquetFile, PartitionId};

use crate::components::skipped_compactions_source::SkippedCompactionsSource;

use super::PartitionFilter;

#[derive(Debug)]
pub struct NeverSkippedPartitionFilter<T>
where
    T: SkippedCompactionsSource,
{
    source: T,
}

impl<T> NeverSkippedPartitionFilter<T>
where
    T: SkippedCompactionsSource,
{
    pub fn new(source: T) -> Self {
        Self { source }
    }
}

impl<T> Display for NeverSkippedPartitionFilter<T>
where
    T: SkippedCompactionsSource,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "never_skipped({})", self.source)
    }
}

#[async_trait]
impl<T> PartitionFilter for NeverSkippedPartitionFilter<T>
where
    T: SkippedCompactionsSource,
{
    async fn apply(&self, partition_id: PartitionId, _files: &[ParquetFile]) -> bool {
        self.source.fetch(partition_id).await.is_none()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use crate::components::skipped_compactions_source::mock::MockSkippedCompactionsSource;

    use super::*;

    #[test]
    fn test_display() {
        let filter =
            NeverSkippedPartitionFilter::new(MockSkippedCompactionsSource::new(HashMap::default()));
        assert_eq!(filter.to_string(), "never_skipped(mock)");
    }
}
