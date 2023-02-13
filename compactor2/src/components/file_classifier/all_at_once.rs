use std::fmt::Display;

use data_types::{CompactionLevel, ParquetFile};

use crate::{file_classification::FileClassification, partition_info::PartitionInfo};

use super::FileClassifier;

/// All files of level 0 and level 1 will be classified in one group to get compacted to level 1.
#[derive(Debug, Default)]
pub struct AllAtOnceFileClassifier;

impl AllAtOnceFileClassifier {
    pub fn new() -> Self {
        Self::default()
    }
}

impl Display for AllAtOnceFileClassifier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "all_at_once")
    }
}

impl FileClassifier for AllAtOnceFileClassifier {
    fn classify(
        &self,
        _partition_info: &PartitionInfo,
        files: Vec<ParquetFile>,
    ) -> FileClassification {
        // Check if there are files in Compaction::Initial level
        if !files
            .iter()
            .any(|file| file.compaction_level == CompactionLevel::Initial)
        {
            panic!("Level-0 file not found in target level detection");
        }

        FileClassification {
            target_level: CompactionLevel::FileNonOverlapped,
            files_to_compact: files,
            files_to_upgrade: vec![],
            files_to_keep: vec![],
        }
    }
}

#[cfg(test)]
mod tests {
    use compactor2_test_utils::create_overlapped_files;
    use iox_tests::ParquetFileBuilder;

    use crate::test_utils::partition_info;

    use super::*;

    #[test]
    fn test_display() {
        assert_eq!(AllAtOnceFileClassifier::new().to_string(), "all_at_once",);
    }

    #[test]
    #[should_panic(expected = "Level-0 file not found in target level detection")]
    fn test_apply_empty() {
        let classifier = AllAtOnceFileClassifier::new();

        classifier.classify(&partition_info(), vec![]);
    }

    #[test]
    #[should_panic(expected = "Level-0 file not found in target level detection")]
    fn test_only_l1() {
        let classifier = AllAtOnceFileClassifier::new();

        let f1 = ParquetFileBuilder::new(1)
            .with_compaction_level(CompactionLevel::FileNonOverlapped)
            .build();

        classifier.classify(&partition_info(), vec![f1]);
    }

    #[test]
    #[should_panic(expected = "Level-0 file not found in target level detection")]
    fn test_only_l2() {
        let classifier = AllAtOnceFileClassifier::new();

        let f2 = ParquetFileBuilder::new(2)
            .with_compaction_level(CompactionLevel::Final)
            .build();

        classifier.classify(&partition_info(), vec![f2]);
    }

    #[test]
    #[should_panic(expected = "Level-0 file not found in target level detection")]
    fn test_only_l1_l2() {
        let classifier = AllAtOnceFileClassifier::new();

        let f1 = ParquetFileBuilder::new(1)
            .with_compaction_level(CompactionLevel::FileNonOverlapped)
            .build();

        let f2 = ParquetFileBuilder::new(2)
            .with_compaction_level(CompactionLevel::Final)
            .build();

        classifier.classify(&partition_info(), vec![f1, f2]);
    }

    #[test]
    fn test_apply() {
        let classifier = AllAtOnceFileClassifier::new();
        let files = create_overlapped_files();
        let classification = classifier.classify(&partition_info(), files.clone());
        assert_eq!(
            classification,
            FileClassification {
                target_level: CompactionLevel::FileNonOverlapped,
                files_to_compact: files,
                files_to_keep: vec![],
                files_to_upgrade: vec![],
            }
        );
    }
}
