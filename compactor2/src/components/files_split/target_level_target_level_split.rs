use std::fmt::Display;

use data_types::{CompactionLevel, ParquetFile};

use super::FilesSplit;

/// Split given files into 2 groups of files: `[<= target_level]` and `[> target_level]`
#[derive(Debug)]
pub struct TargetLevelTargetLevelSplit {}

impl TargetLevelTargetLevelSplit {
    pub fn new() -> Self {
        Self {}
    }
}

impl Display for TargetLevelTargetLevelSplit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Target level split for TargetLevel version")
    }
}

impl FilesSplit for TargetLevelTargetLevelSplit {
    fn apply(
        &self,
        files: Vec<data_types::ParquetFile>,
        target_level: CompactionLevel,
    ) -> (Vec<ParquetFile>, Vec<ParquetFile>) {
        files
            .into_iter()
            .partition(|f| f.compaction_level <= target_level)
    }
}

#[cfg(test)]
mod tests {

    use crate::test_util::create_overlapped_files;

    use super::*;

    #[test]
    fn test_display() {
        assert_eq!(
            TargetLevelTargetLevelSplit::new().to_string(),
            "Target level split for TargetLevel version"
        );
    }

    #[test]
    fn test_apply_empty_files() {
        let files = vec![];
        let split = TargetLevelTargetLevelSplit::new();

        let (lower, higher) = split.apply(files, CompactionLevel::FileNonOverlapped);
        assert_eq!(lower.len(), 0);
        assert_eq!(higher.len(), 0);
    }

    #[test]
    fn test_apply_partial_empty_files_l0() {
        // ------------------------------
        // Create 8 files with all levels
        let files = create_overlapped_files();
        assert_eq!(files.len(), 8);
        //
        // Only keep files of CompactionLevel::Initial
        let files = files
            .into_iter()
            .filter(|f| f.compaction_level == CompactionLevel::Initial)
            .collect::<Vec<_>>();
        assert_eq!(files.len(), 3);

        let split = TargetLevelTargetLevelSplit::new();
        let (lower, higher) = split.apply(files.clone(), CompactionLevel::Initial);
        assert_eq!(lower.len(), 3);
        assert_eq!(higher.len(), 0);

        let (lower, higher) = split.apply(files.clone(), CompactionLevel::FileNonOverlapped);
        assert_eq!(lower.len(), 3);
        assert_eq!(higher.len(), 0);

        let (lower, higher) = split.apply(files, CompactionLevel::Final);
        assert_eq!(lower.len(), 3);
        assert_eq!(higher.len(), 0);
    }

    #[test]
    fn test_apply_partial_empty_files_l1() {
        // ------------------------------
        // Create 8 files with all levels
        let files = create_overlapped_files();
        assert_eq!(files.len(), 8);
        //
        // Only keep files of CompactionLevel::FileNonOverlapped
        let files = files
            .into_iter()
            .filter(|f| f.compaction_level == CompactionLevel::FileNonOverlapped)
            .collect::<Vec<_>>();
        assert_eq!(files.len(), 3);

        let split = TargetLevelTargetLevelSplit::new();
        let (lower, higher) = split.apply(files.clone(), CompactionLevel::Initial);
        assert_eq!(lower.len(), 0);
        assert_eq!(higher.len(), 3);
        //
        let (lower, higher) = split.apply(files.clone(), CompactionLevel::FileNonOverlapped);
        assert_eq!(lower.len(), 3);
        assert_eq!(higher.len(), 0);
        //
        let (lower, higher) = split.apply(files, CompactionLevel::Final);
        assert_eq!(lower.len(), 3);
        assert_eq!(higher.len(), 0);
    }

    #[test]
    fn test_apply_partial_empty_files_l2() {
        // ------------------------------
        // Create 8 files with all levels
        let files = create_overlapped_files();
        assert_eq!(files.len(), 8);
        //
        // Only keep files of CompactionLevel::Final
        let files = files
            .into_iter()
            .filter(|f| f.compaction_level == CompactionLevel::Final)
            .collect::<Vec<_>>();
        assert_eq!(files.len(), 2);

        let split = TargetLevelTargetLevelSplit::new();
        let (lower, higher) = split.apply(files.clone(), CompactionLevel::Initial);
        assert_eq!(lower.len(), 0);
        assert_eq!(higher.len(), 2);

        let (lower, higher) = split.apply(files.clone(), CompactionLevel::FileNonOverlapped);
        assert_eq!(lower.len(), 0);
        assert_eq!(higher.len(), 2);

        let (lower, higher) = split.apply(files, CompactionLevel::Final);
        assert_eq!(lower.len(), 2);
        assert_eq!(higher.len(), 0);
    }

    #[test]
    fn test_apply_target_level_0() {
        // Test target level Initial
        let files = create_overlapped_files();
        assert_eq!(files.len(), 8);

        let split = TargetLevelTargetLevelSplit::new();
        let (lower, higher) = split.apply(files, CompactionLevel::Initial);
        // verify number of files
        assert_eq!(lower.len(), 3);
        assert_eq!(higher.len(), 5);
        // verify compaction level of files
        assert!(lower
            .iter()
            .all(|f| f.compaction_level == CompactionLevel::Initial));
        assert!(higher
            .iter()
            .all(|f| f.compaction_level == CompactionLevel::FileNonOverlapped
                || f.compaction_level == CompactionLevel::Final));
    }

    #[test]
    fn test_apply_terget_level_l1() {
        // ------------------------------
        // Test target level is FileNonOverlapped
        let files = create_overlapped_files();
        assert_eq!(files.len(), 8);

        let split = TargetLevelTargetLevelSplit::new();
        let (lower, higher) = split.apply(files, CompactionLevel::FileNonOverlapped);
        // verify number of files
        assert_eq!(lower.len(), 6);
        assert_eq!(higher.len(), 2);
        // verify compaction level of files
        assert!(lower
            .iter()
            .all(|f| f.compaction_level == CompactionLevel::Initial
                || f.compaction_level == CompactionLevel::FileNonOverlapped));
        assert!(higher
            .iter()
            .all(|f| f.compaction_level == CompactionLevel::Final));
    }

    #[test]
    fn test_apply_taget_level_l2() {
        // ------------------------------
        // Test target level is Final
        let files = create_overlapped_files();
        assert_eq!(files.len(), 8);

        let split = TargetLevelTargetLevelSplit::new();
        let (lower, higher) = split.apply(files, CompactionLevel::Final);
        // verify number of files
        assert_eq!(lower.len(), 8);
        assert_eq!(higher.len(), 0);
        // verify compaction level of files
        assert!(lower
            .iter()
            .all(|f| f.compaction_level == CompactionLevel::Initial
                || f.compaction_level == CompactionLevel::FileNonOverlapped
                || f.compaction_level == CompactionLevel::Final));
    }
}
