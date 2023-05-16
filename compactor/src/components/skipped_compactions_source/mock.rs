use std::{collections::HashMap, fmt::Display};

use async_trait::async_trait;
use data_types::{PartitionId, SkippedCompaction};

use super::SkippedCompactionsSource;

#[derive(Debug)]
pub struct MockSkippedCompactionsSource {
    skipped_compactions: HashMap<PartitionId, SkippedCompaction>,
}

impl MockSkippedCompactionsSource {
    #[allow(dead_code)] // not used anywhere
    pub fn new(skipped_compactions: HashMap<PartitionId, SkippedCompaction>) -> Self {
        Self {
            skipped_compactions,
        }
    }
}

impl Display for MockSkippedCompactionsSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "mock")
    }
}

#[async_trait]
impl SkippedCompactionsSource for MockSkippedCompactionsSource {
    async fn fetch(&self, partition: PartitionId) -> Option<SkippedCompaction> {
        self.skipped_compactions.get(&partition).cloned()
    }
}

#[cfg(test)]
mod tests {
    use iox_tests::SkippedCompactionBuilder;

    use super::*;

    #[test]
    fn test_display() {
        assert_eq!(
            MockSkippedCompactionsSource::new(HashMap::default()).to_string(),
            "mock",
        )
    }

    #[tokio::test]
    async fn test_fetch() {
        let sc_1 = SkippedCompactionBuilder::new(1)
            .with_reason("reason1")
            .build();
        let sc_2 = SkippedCompactionBuilder::new(2)
            .with_reason("reason2")
            .build();

        let scs = HashMap::from([
            (PartitionId::new(1), sc_1.clone()),
            (PartitionId::new(2), sc_2.clone()),
        ]);
        let source = MockSkippedCompactionsSource::new(scs);

        // different tables
        assert_eq!(source.fetch(PartitionId::new(1)).await, Some(sc_1.clone()),);
        assert_eq!(source.fetch(PartitionId::new(2)).await, Some(sc_2),);

        // fetching does not drain
        assert_eq!(source.fetch(PartitionId::new(1)).await, Some(sc_1),);

        // unknown partition => None result
        assert_eq!(source.fetch(PartitionId::new(3)).await, None);
    }
}
