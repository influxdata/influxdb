use std::{collections::HashSet, fmt::Display};

use async_trait::async_trait;
use data_types::PartitionId;

use super::PartitionsSubsetSource;

/// A mock of [`PartitionsSubsetSource`], which returns based on subset inclusion.
#[derive(Debug)]
pub(crate) struct MockInclusionPartitionsSubsetSource {
    partitions: HashSet<PartitionId>,
}

impl MockInclusionPartitionsSubsetSource {
    #[cfg(test)]
    pub(crate) fn new(partitions: HashSet<PartitionId>) -> Self {
        Self { partitions }
    }
}

impl Display for MockInclusionPartitionsSubsetSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "mock")
    }
}

#[async_trait]
impl PartitionsSubsetSource for MockInclusionPartitionsSubsetSource {
    async fn fetch(&self, partitions: &[PartitionId]) -> Vec<PartitionId> {
        partitions
            .iter()
            .filter(|p| self.partitions.contains(p))
            .cloned()
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use assert_matches::assert_matches;

    use super::*;

    #[test]
    fn test_display() {
        assert_eq!(
            MockInclusionPartitionsSubsetSource::new(HashSet::default()).to_string(),
            "mock",
        )
    }

    #[tokio::test]
    async fn test_fetch() {
        let p_1 = PartitionId::new(1);
        let p_2 = PartitionId::new(2);
        let p_3 = PartitionId::new(3);

        let set = HashSet::from([p_1, p_2]);
        let source = MockInclusionPartitionsSubsetSource::new(set);

        //  subset containing 1
        assert_matches!(
            source.fetch(&[p_1]).await[..],
            [subset] if subset == p_1
        );
        // subset containing multiple
        assert_matches!(
            source.fetch(&[p_1, p_2]).await[..],
            [subset_1, subset_2] if subset_1 == p_1 && subset_2 == p_2
        );
        // does not return not-included
        assert_matches!(
            source.fetch(&[p_1, p_3]).await[..],
            [subset] if subset == p_1
        );
        // None included = empty
        assert_matches!(source.fetch(&[p_3]).await[..], []);
    }
}
