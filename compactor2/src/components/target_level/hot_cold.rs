use std::fmt::Display;

use data_types::CompactionLevel;

use crate::components::level_filter::{one_level::OneLevelFilter, LevelFilter};

use super::TargetLevelDetection;

#[derive(Debug)]
pub struct HotColdTargetLevelDetection<T> where T: LevelFilter {
    level_filter: T,
}

impl HotColdTargetLevelDetection {
    pub fn new() -> Self {
        Self {
            level_filter: OneLevelFilter::new(),
        }
    }
}

impl Display for HotColdTargetLevelDetection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "hot cold target level detection",)
    }
}

impl TargetLevelDetection for HotColdTargetLevelDetection {
    // For HotCold version, we support compact (L0s + L1s) to L1s and (L1s + L2s) to L2s
    // Target is the next level of the lowest level that has files
    fn detect(&self, files: &[data_types::ParquetFile]) -> Option<CompactionLevel> {
        // Start with initial level
        // If there are files in  this level, the compaction's target level will be the next level.
        // Otherwise repeat until reaching the final level.
        let mut level = CompactionLevel::Initial;
        while level != CompactionLevel::Final {
            if self.level_filter.apply(files, level) {
                return Some(level.next());
            }

            level = level.next();
        }

        // All files are in final level, nothing to compact
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
            HotColdTargetLevelDetection::new().to_string(),
            "hot cold target level detection"
        );
    }

    #[test]
    fn test_apply() {
        let target_level_detection = HotColdTargetLevelDetection::new();

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
        assert_eq!(
            target_level_detection.detect(&[f1.clone()]),
            Some(CompactionLevel::Final)
        );
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
        assert_eq!(
            target_level_detection.detect(&[f2, f1]),
            Some(CompactionLevel::Final)
        );
    }
}
