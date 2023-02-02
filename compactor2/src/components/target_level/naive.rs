use std::fmt::Display;

use data_types::CompactionLevel;

use super::TargetLevelDetection;

#[derive(Debug)]
pub struct NaiveTargetLevelDetection {}

impl NaiveTargetLevelDetection {
    pub fn new() -> Self {
        Self {}
    }
}

impl Display for NaiveTargetLevelDetection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "naive target level detection",)
    }
}

impl TargetLevelDetection for NaiveTargetLevelDetection {
    // For naive version, we only compact (L0s + L1s) to L1s
    // The target level is always 1 and there must be at least one file in L0
    fn detect(&self, files: &[data_types::ParquetFile]) -> Option<CompactionLevel> {
        // Check if there are files in Compaction::Initial level
        if files
            .iter()
            .any(|file| file.compaction_level == CompactionLevel::Initial)
        {
            return Some(CompactionLevel::FileNonOverlapped);
        }

        None
    }
}

#[cfg(test)]
mod tests {
    use crate::test_util::ParquetFileBuilder;

    use super::*;

    #[test]
    fn test_display() {
        assert_eq!(
            NaiveTargetLevelDetection::new().to_string(),
            "naive target level detection"
        );
    }

    #[test]
    fn test_apply() {
        let target_level_detection = NaiveTargetLevelDetection::new();

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
            Some(CompactionLevel::FileNonOverlapped)
        );
        assert_eq!(target_level_detection.detect(&[f1.clone()]), None);
        assert_eq!(target_level_detection.detect(&[f2.clone()]), None);
        // list of many
        assert_eq!(
            target_level_detection.detect(&[f1.clone(), f0.clone()]),
            Some(CompactionLevel::FileNonOverlapped)
        );
        assert_eq!(
            target_level_detection.detect(&[f2.clone(), f0.clone()]),
            Some(CompactionLevel::FileNonOverlapped)
        );
        assert_eq!(
            target_level_detection.detect(&[f2.clone(), f0, f1.clone()]),
            Some(CompactionLevel::FileNonOverlapped)
        );
        assert_eq!(target_level_detection.detect(&[f2, f1]), None);
    }
}
