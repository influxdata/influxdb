use std::fmt::Display;

use data_types::CompactionLevel;

use crate::components::level_exist::LevelExist;

use super::TargetLevelChooser;

/// For TargetLevel version, we support compact (L0s + L1s) to L1s and (L1s + L2s) to L2s
/// Target is the next level of the lowest level that has files
#[derive(Debug)]
pub struct TargetLevelTargetLevelChooser<T>
where
    T: LevelExist,
{
    inner: T,
}

impl<T> TargetLevelTargetLevelChooser<T>
where
    T: LevelExist,
{
    pub fn new(inner: T) -> Self {
        Self { inner }
    }
}

impl<T> Display for TargetLevelTargetLevelChooser<T>
where
    T: LevelExist,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Target level detection for TargetLevel version",)
    }
}

impl<T> TargetLevelChooser for TargetLevelTargetLevelChooser<T>
where
    T: LevelExist,
{
    fn detect(&self, files: &[data_types::ParquetFile]) -> CompactionLevel {
        // Start with initial level
        // If there are files in  this level, the compaction's target level will be the next level.
        // Otherwise repeat until reaching the final level.
        let mut level = CompactionLevel::Initial;
        while level != CompactionLevel::Final {
            if self.inner.apply(files, level) {
                return level.next();
            }

            level = level.next();
        }

        // All files are in final level and should have been filtered out earlier
        panic!("Neither level-0 nor level-1 found in target level detection");
    }
}

#[cfg(test)]
mod tests {
    use crate::components::level_exist::one_level::OneLevelExist;
    use iox_tests::ParquetFileBuilder;

    use super::*;

    #[test]
    fn test_display() {
        assert_eq!(
            TargetLevelTargetLevelChooser::new(OneLevelExist::new()).to_string(),
            "Target level detection for TargetLevel version"
        );
    }

    #[test]
    #[should_panic(expected = "Neither level-0 nor level-1 found in target level detection")]
    fn test_apply_empty() {
        let target_level_chooser = TargetLevelTargetLevelChooser::new(OneLevelExist::new());

        target_level_chooser.detect(&[]);
    }

    #[test]
    fn test_apply_only_l0() {
        let target_level_chooser = TargetLevelTargetLevelChooser::new(OneLevelExist::new());

        let f0 = ParquetFileBuilder::new(0)
            .with_compaction_level(CompactionLevel::Initial)
            .build();

        assert_eq!(
            target_level_chooser.detect(&[f0]),
            CompactionLevel::FileNonOverlapped
        );
    }

    #[test]
    fn test_apply_only_l1() {
        let target_level_chooser = TargetLevelTargetLevelChooser::new(OneLevelExist::new());

        let f1 = ParquetFileBuilder::new(1)
            .with_compaction_level(CompactionLevel::FileNonOverlapped)
            .build();

        assert_eq!(target_level_chooser.detect(&[f1]), CompactionLevel::Final);
    }

    #[test]
    #[should_panic(expected = "Neither level-0 nor level-1 found in target level detection")]
    fn test_apply_only_l2() {
        let target_level_chooser = TargetLevelTargetLevelChooser::new(OneLevelExist::new());

        let f2 = ParquetFileBuilder::new(2)
            .with_compaction_level(CompactionLevel::Final)
            .build();

        target_level_chooser.detect(&[f2]);
    }

    #[test]
    fn test_apply_many_files() {
        let target_level_chooser = TargetLevelTargetLevelChooser::new(OneLevelExist::new());

        let f0 = ParquetFileBuilder::new(0)
            .with_compaction_level(CompactionLevel::Initial)
            .build();
        let f1 = ParquetFileBuilder::new(1)
            .with_compaction_level(CompactionLevel::FileNonOverlapped)
            .build();
        let f2 = ParquetFileBuilder::new(2)
            .with_compaction_level(CompactionLevel::Final)
            .build();

        assert_eq!(
            target_level_chooser.detect(&[f1.clone(), f0.clone()]),
            CompactionLevel::FileNonOverlapped
        );
        assert_eq!(
            target_level_chooser.detect(&[f2.clone(), f0.clone()]),
            CompactionLevel::FileNonOverlapped
        );
        assert_eq!(
            target_level_chooser.detect(&[f2.clone(), f0, f1.clone()]),
            CompactionLevel::FileNonOverlapped
        );
        assert_eq!(
            target_level_chooser.detect(&[f2, f1]),
            CompactionLevel::Final
        );
    }
}
