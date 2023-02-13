use std::fmt::Display;

use data_types::{CompactionLevel, ParquetFile};

use super::FilesSplit;

#[derive(Debug)]
/// A [`FilesSplit`] that considers compacting all files each round
pub struct AllAtOnceUpgradeSplit {}

impl AllAtOnceUpgradeSplit {
    pub fn new() -> Self {
        Self {}
    }
}

impl Display for AllAtOnceUpgradeSplit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Upgrade split for AllAtOnce version")
    }
}

impl FilesSplit for AllAtOnceUpgradeSplit {
    fn apply(
        &self,
        files: Vec<data_types::ParquetFile>,
        _target_level: CompactionLevel,
    ) -> (Vec<ParquetFile>, Vec<ParquetFile>) {
        (files, vec![])
    }
}

#[cfg(test)]
mod tests {

    use compactor2_test_utils::create_overlapped_files;

    use super::*;

    #[test]
    fn test_display() {
        assert_eq!(
            AllAtOnceUpgradeSplit::new().to_string(),
            "Upgrade split for AllAtOnce version"
        );
    }

    #[test]
    fn test_apply_empty_files() {
        let files = vec![];
        let split = AllAtOnceUpgradeSplit::new();

        let (compact, upgrade) = split.apply(files, CompactionLevel::FileNonOverlapped);
        assert_eq!(compact.len(), 0);
        assert_eq!(upgrade.len(), 0);
    }

    #[test]
    fn test_apply() {
        // Create 8 files with all levels
        let files = create_overlapped_files();
        assert_eq!(files.len(), 8);

        let split = AllAtOnceUpgradeSplit::new();
        let (compact, upgrade) = split.apply(files.clone(), CompactionLevel::Initial);
        assert_eq!(compact.len(), 8);
        assert_eq!(upgrade.len(), 0);

        let (compact, upgrade) = split.apply(files.clone(), CompactionLevel::FileNonOverlapped);
        assert_eq!(compact.len(), 8);
        assert_eq!(upgrade.len(), 0);

        let (compact, upgrade) = split.apply(files, CompactionLevel::Final);
        assert_eq!(compact.len(), 8);
        assert_eq!(upgrade.len(), 0);
    }
}
