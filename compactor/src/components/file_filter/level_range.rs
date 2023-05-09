use std::{fmt::Display, ops::RangeInclusive};

use data_types::CompactionLevel;

use super::FileFilter;

#[derive(Debug)]
pub struct LevelRangeFileFilter {
    range: RangeInclusive<CompactionLevel>,
}

impl LevelRangeFileFilter {
    pub fn new(range: RangeInclusive<CompactionLevel>) -> Self {
        Self { range }
    }
}

impl Display for LevelRangeFileFilter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "level_range({}..={})",
            *self.range.start() as i32,
            *self.range.end() as i32
        )
    }
}

impl FileFilter for LevelRangeFileFilter {
    fn apply(&self, file: &data_types::ParquetFile) -> bool {
        self.range.contains(&file.compaction_level)
    }
}

#[cfg(test)]
mod tests {
    use iox_tests::ParquetFileBuilder;

    use super::*;

    #[test]
    fn test_display() {
        assert_eq!(
            LevelRangeFileFilter::new(
                CompactionLevel::Initial..=CompactionLevel::FileNonOverlapped
            )
            .to_string(),
            "level_range(0..=1)"
        );
    }

    #[test]
    fn test_apply() {
        let filter_a = LevelRangeFileFilter::new(
            CompactionLevel::Initial..=CompactionLevel::FileNonOverlapped,
        );
        let filter_b =
            LevelRangeFileFilter::new(CompactionLevel::FileNonOverlapped..=CompactionLevel::Final);

        let f0 = ParquetFileBuilder::new(0)
            .with_compaction_level(CompactionLevel::Initial)
            .build();
        let f1 = ParquetFileBuilder::new(0)
            .with_compaction_level(CompactionLevel::FileNonOverlapped)
            .build();
        let f2 = ParquetFileBuilder::new(0)
            .with_compaction_level(CompactionLevel::Final)
            .build();

        assert!(filter_a.apply(&f0));
        assert!(filter_a.apply(&f1));
        assert!(!filter_a.apply(&f2));
        assert!(!filter_b.apply(&f0));
        assert!(filter_b.apply(&f1));
        assert!(filter_b.apply(&f2));
    }
}
