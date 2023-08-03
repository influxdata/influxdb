use std::{collections::HashSet, fmt::Display};

use data_types::PartitionId;

use super::IdOnlyPartitionFilter;

/// Apply a containment [`IdOnlyPartitionFilter`].
/// PartitionId must be contained within the set.
#[derive(Debug)]
pub(crate) struct ByIdPartitionFilter {
    ids: HashSet<PartitionId>,
}

impl ByIdPartitionFilter {
    #[cfg(test)]
    pub(crate) fn new(ids: HashSet<PartitionId>) -> Self {
        Self { ids }
    }
}

impl Display for ByIdPartitionFilter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "by_id")
    }
}

impl IdOnlyPartitionFilter for ByIdPartitionFilter {
    fn apply(&self, partition_id: PartitionId) -> bool {
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

    #[test]
    fn test_apply() {
        let filter =
            ByIdPartitionFilter::new(HashSet::from([PartitionId::new(1), PartitionId::new(10)]));

        assert!(filter.apply(PartitionId::new(1)));
        assert!(filter.apply(PartitionId::new(10)));
        assert!(!filter.apply(PartitionId::new(2)));
    }
}
