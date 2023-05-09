use std::{collections::HashMap, fmt::Display};

use async_trait::async_trait;
use data_types::{ParquetFile, PartitionId};

use super::PartitionFilesSource;

#[derive(Debug)]
pub struct MockPartitionFilesSource {
    files: HashMap<PartitionId, Vec<ParquetFile>>,
}

impl MockPartitionFilesSource {
    #[allow(dead_code)] // not used anywhere
    pub fn new(files: HashMap<PartitionId, Vec<ParquetFile>>) -> Self {
        Self { files }
    }
}

impl Display for MockPartitionFilesSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "mock")
    }
}

#[async_trait]
impl PartitionFilesSource for MockPartitionFilesSource {
    async fn fetch(&self, partition: PartitionId) -> Vec<ParquetFile> {
        self.files.get(&partition).cloned().unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use iox_tests::ParquetFileBuilder;

    use super::*;

    #[test]
    fn test_display() {
        assert_eq!(
            MockPartitionFilesSource::new(HashMap::default()).to_string(),
            "mock",
        )
    }

    #[tokio::test]
    async fn test_fetch() {
        let f_1_1 = ParquetFileBuilder::new(1).with_partition(1).build();
        let f_1_2 = ParquetFileBuilder::new(2).with_partition(1).build();
        let f_2_1 = ParquetFileBuilder::new(3).with_partition(2).build();

        let files = HashMap::from([
            (PartitionId::new(1), vec![f_1_1.clone(), f_1_2.clone()]),
            (PartitionId::new(2), vec![f_2_1.clone()]),
        ]);
        let source = MockPartitionFilesSource::new(files);

        // different partitions
        assert_eq!(
            source.fetch(PartitionId::new(1)).await,
            vec![f_1_1.clone(), f_1_2.clone()],
        );
        assert_eq!(source.fetch(PartitionId::new(2)).await, vec![f_2_1],);

        // fetching does not drain
        assert_eq!(source.fetch(PartitionId::new(1)).await, vec![f_1_1, f_1_2],);

        // unknown partition => empty result
        assert_eq!(source.fetch(PartitionId::new(3)).await, vec![],);
    }
}
