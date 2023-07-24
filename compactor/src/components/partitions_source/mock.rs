use async_trait::async_trait;
use data_types::PartitionId;
use parking_lot::Mutex;

use super::PartitionsSource;

/// A mock structure for providing [partitions](PartitionId).
#[derive(Debug)]
pub struct MockPartitionsSource {
    partitions: Mutex<Vec<PartitionId>>,
}

impl MockPartitionsSource {
    #[allow(dead_code)]
    /// Create a new MockPartitionsSource.
    pub fn new(partitions: Vec<PartitionId>) -> Self {
        Self {
            partitions: Mutex::new(partitions),
        }
    }

    /// Set PartitionIds for MockPartitionsSource.
    #[allow(dead_code)] // not used anywhere
    pub fn set(&self, partitions: Vec<PartitionId>) {
        *self.partitions.lock() = partitions;
    }
}

impl std::fmt::Display for MockPartitionsSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "mock")
    }
}

#[async_trait]
impl PartitionsSource for MockPartitionsSource {
    async fn fetch(&self) -> Vec<PartitionId> {
        self.partitions.lock().clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_display() {
        assert_eq!(MockPartitionsSource::new(vec![]).to_string(), "mock",);
    }

    #[tokio::test]
    async fn test_fetch() {
        let source = MockPartitionsSource::new(vec![]);
        assert_eq!(source.fetch().await, vec![],);

        let p_1 = PartitionId::new(5);
        let p_2 = PartitionId::new(1);
        let p_3 = PartitionId::new(12);
        let parts = vec![p_1, p_2, p_3];
        source.set(parts.clone());
        assert_eq!(source.fetch().await, parts,);
    }
}
