use std::fmt::Display;

use async_trait::async_trait;
use data_types::{Partition, PartitionId};

use super::PartitionSource;

#[derive(Debug)]
pub struct MockPartitionSource {
    partitions: Vec<Partition>,
}

impl MockPartitionSource {
    #[allow(dead_code)] // not used anywhere
    pub fn new(partitions: Vec<Partition>) -> Self {
        Self { partitions }
    }
}

impl Display for MockPartitionSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "mock")
    }
}

#[async_trait]
impl PartitionSource for MockPartitionSource {
    async fn fetch_by_id(&self, partition_id: PartitionId) -> Option<Partition> {
        self.partitions
            .iter()
            .find(|p| p.id == partition_id)
            .cloned()
    }
}

#[cfg(test)]
mod tests {
    use crate::test_util::PartitionBuilder;

    use super::*;

    #[test]
    fn test_display() {
        assert_eq!(MockPartitionSource::new(vec![]).to_string(), "mock",);
    }

    #[tokio::test]
    async fn test_fetch_by_id() {
        let p_1 = PartitionBuilder::new(5).build();
        let p_2 = PartitionBuilder::new(1).build();
        let p_3 = PartitionBuilder::new(12).build();
        let partitions = vec![p_1.clone(), p_2.clone(), p_3.clone()];
        let source = MockPartitionSource::new(partitions);

        assert_eq!(
            source.fetch_by_id(PartitionId::new(5)).await,
            Some(p_1.clone())
        );
        assert_eq!(
            source.fetch_by_id(PartitionId::new(1)).await,
            Some(p_2.clone())
        );

        // fetching does not drain
        assert_eq!(
            source.fetch_by_id(PartitionId::new(5)).await,
            Some(p_1.clone())
        );

        // unknown table => None result
        assert_eq!(source.fetch_by_id(PartitionId::new(3)).await, None,);
    }
}
