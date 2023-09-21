use std::fmt::Display;

use async_trait::async_trait;
use data_types::PartitionId;

use crate::PartitionsSource;

use super::super::partitions_subset_source::PartitionsSubsetSource;

#[derive(Debug)]
pub(crate) struct NeverSkippedPartitionsSource<T, I>
where
    T: PartitionsSubsetSource,
    I: PartitionsSource,
{
    /// The source of skipped partitions to filter.
    skipped_source: T,
    // The inner source of partitions for compaction.
    inner: I,
}

impl<T, I> NeverSkippedPartitionsSource<T, I>
where
    T: PartitionsSubsetSource,
    I: PartitionsSource,
{
    pub(crate) fn new(inner: I, skipped_source: T) -> Self {
        Self {
            inner,
            skipped_source,
        }
    }
}

impl<T, I> Display for NeverSkippedPartitionsSource<T, I>
where
    T: PartitionsSubsetSource,
    I: PartitionsSource,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "never_skipped({})", self.skipped_source)
    }
}

#[async_trait]
impl<T, I> PartitionsSource for NeverSkippedPartitionsSource<T, I>
where
    T: PartitionsSubsetSource,
    I: PartitionsSource,
{
    async fn fetch(&self) -> Vec<PartitionId> {
        let partitions = self.inner.fetch().await;
        let skipped = self.skipped_source.fetch(&partitions).await;

        partitions
            .into_iter()
            .filter(|p| !skipped.contains(p))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use crate::MockPartitionsSource;

    use super::{
        super::super::partitions_subset_source::mock::MockInclusionPartitionsSubsetSource, *,
    };

    #[test]
    fn test_display() {
        let filter = NeverSkippedPartitionsSource::new(
            MockPartitionsSource::new(vec![]),
            MockInclusionPartitionsSubsetSource::new(HashSet::default()),
        );
        assert_eq!(filter.to_string(), "never_skipped(mock)");
    }

    #[tokio::test]
    async fn test_removes_skipped_partitions() {
        let p_1 = PartitionId::new(1);
        let p_2 = PartitionId::new(2);
        let p_3 = PartitionId::new(3);
        let p_4 = PartitionId::new(4);

        let skipped = HashSet::from([p_1, p_2]);
        let set_transformer = NeverSkippedPartitionsSource::new(
            MockPartitionsSource::new(vec![p_1, p_2, p_3, p_4]),
            MockInclusionPartitionsSubsetSource::new(skipped),
        );

        assert_eq!(set_transformer.fetch().await[..], [p_3, p_4],);
    }
}
