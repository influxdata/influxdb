use std::fmt::Display;

use data_types::CompactionLevel;

use super::LevelFilter;

#[derive(Debug)]
pub struct OneLevelFilter {}

impl OneLevelFilter {
    pub fn new() -> Self {
        Self {}
    }
}

impl Display for OneLevelFilter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "one level",)
    }
}

impl LevelFilter for OneLevelFilter {
    fn apply(&self, files: &[data_types::ParquetFile], level: CompactionLevel) -> bool {
        files.iter().any(|f| f.compaction_level == level)
    }
}

#[cfg(test)]
mod tests {
    use crate::test_util::ParquetFileBuilder;

    use super::*;

    #[test]
    fn test_display() {
        assert_eq!(OneLevelFilter::new().to_string(), "one level");
    }

    #[test]
    fn test_apply() {
        let filter = OneLevelFilter::new();

        let f0 = ParquetFileBuilder::new(0)
            .with_compaction_level(CompactionLevel::Initial)
            .build();
        let f1 = ParquetFileBuilder::new(1)
            .with_compaction_level(CompactionLevel::FileNonOverlapped)
            .build();
        let f2 = ParquetFileBuilder::new(2)
            .with_compaction_level(CompactionLevel::Final)
            .build();

        // empty list
        assert!(!filter.apply(&[], CompactionLevel::Initial));

        // list of one
        assert!(filter.apply(&[f0.clone()], CompactionLevel::Initial));
        assert!(filter.apply(&[f1.clone()], CompactionLevel::FileNonOverlapped));
        assert!(filter.apply(&[f2.clone()], CompactionLevel::Final));
        assert!(!filter.apply(&[f0.clone()], CompactionLevel::FileNonOverlapped));
        assert!(!filter.apply(&[f1.clone()], CompactionLevel::Initial));
        assert!(!filter.apply(&[f2.clone()], CompactionLevel::Initial));
        // list of many
        assert!(filter.apply(&[f2.clone(), f0.clone()], CompactionLevel::Initial));
        assert!(filter.apply(
            &vec![f2.clone(), f0, f1.clone()],
            CompactionLevel::FileNonOverlapped
        ));
        assert!(!filter.apply(&[f2, f1], CompactionLevel::Initial));
    }
}
