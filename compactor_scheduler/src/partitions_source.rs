use std::{
    fmt::{Debug, Display},
    sync::{Arc, Mutex},
};

use async_trait::async_trait;
use data_types::PartitionId;

/// A source of partitions, noted by [`PartitionId`](data_types::PartitionId), that may potentially need compacting.
#[async_trait]
pub trait PartitionsSource: Debug + Display + Send + Sync {
    /// Get partition IDs.
    ///
    /// This method performs retries.
    ///
    /// This should only perform basic, efficient filtering. It MUST NOT inspect individual parquet files.
    async fn fetch(&self) -> Vec<PartitionId>;
}

#[async_trait]
impl<T> PartitionsSource for Arc<T>
where
    T: PartitionsSource + ?Sized,
{
    async fn fetch(&self) -> Vec<PartitionId> {
        self.as_ref().fetch().await
    }
}

pub use mock::MockPartitionsSource;
mod mock {
    use super::*;

    /// A mock structure for providing [partitions](PartitionId).
    #[derive(Debug)]
    pub struct MockPartitionsSource {
        partitions: Mutex<Vec<PartitionId>>,
    }

    impl MockPartitionsSource {
        /// Create a new MockPartitionsSource.
        pub fn new(partitions: Vec<PartitionId>) -> Self {
            Self {
                partitions: Mutex::new(partitions),
            }
        }

        /// Set PartitionIds for MockPartitionsSource.
        #[allow(dead_code)] // not used anywhere
        pub fn set(&self, partitions: Vec<PartitionId>) {
            *self.partitions.lock().expect("not poisoned") = partitions;
        }
    }

    impl Display for MockPartitionsSource {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "mock")
        }
    }

    #[async_trait]
    impl PartitionsSource for MockPartitionsSource {
        async fn fetch(&self) -> Vec<PartitionId> {
            self.partitions.lock().expect("not poisoned").clone()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::mock::*;
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
