use std::fmt::Display;

use data_types::CompactionLevel;

use super::TargetLevelDetection;

#[derive(Debug)]
pub struct AllAtOnceTargetLevelDetection {}

impl AllAtOnceTargetLevelDetection {
    pub fn new() -> Self {
        Self {}
    }
}

impl Display for AllAtOnceTargetLevelDetection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Target level detection for AllAtOnce version",)
    }
}

impl TargetLevelDetection for AllAtOnceTargetLevelDetection {
    // For AllAtOnce version, we only compact (L0s + L1s) to L1s
    // The target level is always 1 and there must be at least one file in L0
    fn detect(&self, files: &[data_types::ParquetFile]) -> CompactionLevel {
        // Check if there are files in Compaction::Initial level
        if files
            .iter()
            .any(|file| file.compaction_level == CompactionLevel::Initial)
        {
            return CompactionLevel::FileNonOverlapped;
        }

        panic!("Level-0 file not found in target level detection");
    }
}

#[cfg(test)]
mod tests {
    use crate::test_util::ParquetFileBuilder;

    use super::*;

    #[test]
    fn test_display() {
        assert_eq!(
            AllAtOnceTargetLevelDetection::new().to_string(),
            "Target level detection for AllAtOnce version"
        );
    }

    #[test]
    #[should_panic(expected = "Level-0 file not found in target level detection")]
    fn test_apply_empty() {
        let target_level_detection = AllAtOnceTargetLevelDetection::new();

        target_level_detection.detect(&[]);
    }

    #[test]
    #[should_panic(expected = "Level-0 file not found in target level detection")]
    fn test_only_l1() {
        let target_level_detection = AllAtOnceTargetLevelDetection::new();

        let f1 = ParquetFileBuilder::new(1)
            .with_compaction_level(CompactionLevel::FileNonOverlapped)
            .build();

        target_level_detection.detect(&[f1]);
    }

    #[test]
    #[should_panic(expected = "Level-0 file not found in target level detection")]
    fn test_only_l2() {
        let target_level_detection = AllAtOnceTargetLevelDetection::new();

        let f2 = ParquetFileBuilder::new(2)
            .with_compaction_level(CompactionLevel::Final)
            .build();

        target_level_detection.detect(&[f2]);
    }

    #[test]
    #[should_panic(expected = "Level-0 file not found in target level detection")]
    fn test_only_l1_l2() {
        let target_level_detection = AllAtOnceTargetLevelDetection::new();

        let f1 = ParquetFileBuilder::new(1)
            .with_compaction_level(CompactionLevel::FileNonOverlapped)
            .build();

        let f2 = ParquetFileBuilder::new(2)
            .with_compaction_level(CompactionLevel::Final)
            .build();

        target_level_detection.detect(&[f1, f2]);
    }

    #[test]
    fn test_apply() {
        let target_level_detection = AllAtOnceTargetLevelDetection::new();

        let f0 = ParquetFileBuilder::new(0)
            .with_compaction_level(CompactionLevel::Initial)
            .build();
        let f1 = ParquetFileBuilder::new(1)
            .with_compaction_level(CompactionLevel::FileNonOverlapped)
            .build();
        let f2 = ParquetFileBuilder::new(2)
            .with_compaction_level(CompactionLevel::Final)
            .build();

        // list of one
        assert_eq!(
            target_level_detection.detect(&[f0.clone()]),
            CompactionLevel::FileNonOverlapped
        );

        // list of many
        assert_eq!(
            target_level_detection.detect(&[f1.clone(), f0.clone()]),
            CompactionLevel::FileNonOverlapped
        );
        assert_eq!(
            target_level_detection.detect(&[f2.clone(), f0.clone()]),
            CompactionLevel::FileNonOverlapped
        );
        assert_eq!(
            target_level_detection.detect(&[f2, f0, f1]),
            CompactionLevel::FileNonOverlapped
        );
    }
}
