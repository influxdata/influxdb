use std::{collections::HashMap, fmt::Display};

use super::PartitionFilesSource;
use async_trait::async_trait;
use data_types::{ParquetFile, PartitionId, TransitionPartitionId};

#[derive(Debug)]
pub struct MockPartitionFilesSource {
    // This complexity is because we're in the process of moving to partition hash IDs rather than
    // partition catalog IDs, and Parquet files might only have the partition hash ID on their
    // record, but the compactor deals with partition catalog IDs because we haven't transitioned
    // it yet. This should become simpler when the transition is complete.
    partition_lookup: HashMap<PartitionId, TransitionPartitionId>,
    file_lookup: HashMap<TransitionPartitionId, Vec<ParquetFile>>,
}

impl MockPartitionFilesSource {
    #[cfg(test)]
    pub fn new(
        partition_lookup: HashMap<PartitionId, TransitionPartitionId>,
        parquet_files: Vec<ParquetFile>,
    ) -> Self {
        let mut file_lookup: HashMap<TransitionPartitionId, Vec<ParquetFile>> = HashMap::new();
        for file in parquet_files {
            let files = file_lookup.entry(file.partition_id.clone()).or_default();
            files.push(file);
        }

        Self {
            partition_lookup,
            file_lookup,
        }
    }
}

impl Display for MockPartitionFilesSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "mock")
    }
}

#[async_trait]
impl PartitionFilesSource for MockPartitionFilesSource {
    async fn fetch(&self, partition_id: PartitionId) -> Vec<ParquetFile> {
        self.partition_lookup
            .get(&partition_id)
            .and_then(|partition_hash_id| self.file_lookup.get(partition_hash_id).cloned())
            .unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use iox_tests::{partition_identifier, ParquetFileBuilder};

    #[test]
    fn test_display() {
        assert_eq!(
            MockPartitionFilesSource::new(Default::default(), Default::default()).to_string(),
            "mock",
        )
    }

    #[tokio::test]
    async fn test_fetch() {
        let partition_id_1 = PartitionId::new(1);
        let partition_id_2 = PartitionId::new(2);
        let partition_identifier_1 = partition_identifier(1);
        let partition_identifier_2 = partition_identifier(2);
        let f_1_1 = ParquetFileBuilder::new(1)
            .with_partition(partition_identifier_1.clone())
            .build();
        let f_1_2 = ParquetFileBuilder::new(2)
            .with_partition(partition_identifier_1.clone())
            .build();
        let f_2_1 = ParquetFileBuilder::new(3)
            .with_partition(partition_identifier_2.clone())
            .build();

        let partition_lookup = HashMap::from([
            (partition_id_1, partition_identifier_1.clone()),
            (partition_id_2, partition_identifier_2.clone()),
        ]);

        let files = vec![f_1_1.clone(), f_1_2.clone(), f_2_1.clone()];
        let source = MockPartitionFilesSource::new(partition_lookup, files);

        // different partitions
        assert_eq!(
            source.fetch(partition_id_1).await,
            vec![f_1_1.clone(), f_1_2.clone()],
        );
        assert_eq!(source.fetch(partition_id_2).await, vec![f_2_1],);

        // fetching does not drain
        assert_eq!(source.fetch(partition_id_1).await, vec![f_1_1, f_1_2],);

        // unknown partition => empty result
        assert_eq!(source.fetch(PartitionId::new(3)).await, vec![],);
    }
}
