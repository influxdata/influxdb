use std::{fmt::Display, sync::Arc};

use data_types::PartitionId;

use super::IdOnlyPartitionFilter;

/// Apply a series of ANDed [`IdOnlyPartitionFilter`].
#[derive(Debug)]
pub(crate) struct AndIdOnlyPartitionFilter {
    filters: Vec<Arc<dyn IdOnlyPartitionFilter>>,
}

impl AndIdOnlyPartitionFilter {
    /// Create a new [`AndIdOnlyPartitionFilter`] from a series of IdOnlyPartitionFilters
    pub(crate) fn new(filters: Vec<Arc<dyn IdOnlyPartitionFilter>>) -> Self {
        Self { filters }
    }
}

impl Display for AndIdOnlyPartitionFilter {
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

impl IdOnlyPartitionFilter for AndIdOnlyPartitionFilter {
    fn apply(&self, partition_id: PartitionId) -> bool {
        self.filters.iter().all(|filter| filter.apply(partition_id))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use crate::local_scheduler::id_only_partition_filter::by_id::ByIdPartitionFilter;

    use super::*;

    #[test]
    fn test_display() {
        assert_eq!(AndIdOnlyPartitionFilter::new(vec![]).to_string(), "and([])",);
        assert_eq!(
            AndIdOnlyPartitionFilter::new(vec![Arc::new(ByIdPartitionFilter::new(
                Default::default()
            ))])
            .to_string(),
            "and([by_id])",
        );
        assert_eq!(
            AndIdOnlyPartitionFilter::new(vec![
                Arc::new(ByIdPartitionFilter::new(Default::default())),
                Arc::new(ByIdPartitionFilter::new(Default::default()))
            ])
            .to_string(),
            "and([by_id, by_id])",
        );
    }

    #[test]
    fn tets_apply_empty() {
        let filter = AndIdOnlyPartitionFilter::new(vec![]);
        assert!(filter.apply(PartitionId::new(1)));
    }

    #[test]
    fn tets_apply_and() {
        let filter = AndIdOnlyPartitionFilter::new(vec![
            Arc::new(ByIdPartitionFilter::new(HashSet::from([
                PartitionId::new(1),
                PartitionId::new(2),
            ]))),
            Arc::new(ByIdPartitionFilter::new(HashSet::from([
                PartitionId::new(1),
                PartitionId::new(3),
            ]))),
        ]);
        assert!(filter.apply(PartitionId::new(1)));
        assert!(!filter.apply(PartitionId::new(2)));
        assert!(!filter.apply(PartitionId::new(3)));
        assert!(!filter.apply(PartitionId::new(4)));
    }
}
