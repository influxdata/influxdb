use std::fmt::Display;

use data_types::{CompactionLevel, ParquetFile};

use crate::RoundInfo;

use super::RoundSplit;

#[derive(Debug, Default)]
pub struct ManyFilesRoundSplit;

impl ManyFilesRoundSplit {
    pub fn new() -> Self {
        Self
    }
}

impl Display for ManyFilesRoundSplit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "many_files")
    }
}

impl RoundSplit for ManyFilesRoundSplit {
    fn split(
        &self,
        files: Vec<ParquetFile>,
        round_info: RoundInfo,
    ) -> (Vec<ParquetFile>, Vec<ParquetFile>) {
        // Scpecify specific arms to avoid missing any new variants
        match round_info {
            RoundInfo::ManySmallFiles { start_level, .. } => {
                // Split start_level from the rest
                let (start_level_files, rest) = files
                    .into_iter()
                    .partition(|f| f.compaction_level == start_level);
                (start_level_files, rest)
            }

            // A TargetLevel round only needs its start (source) and target (destination) levels.
            // All other files are a distraction that should wait for another round.
            RoundInfo::TargetLevel { target_level } => {
                // Split start_level & target level from the rest
                let start_level = target_level.prev();
                let (start_files, rest) = files.into_iter().partition(|f| {
                    f.compaction_level == start_level
                        || f.compaction_level == target_level
                        || f.compaction_level == CompactionLevel::Final
                });
                (start_files, rest)
            }

            RoundInfo::SimulatedLeadingEdge { .. } => {
                // Split first two levels from the rest
                let (start_files, rest) = files.into_iter().partition(|f| {
                    f.compaction_level == CompactionLevel::Initial
                        || f.compaction_level == CompactionLevel::FileNonOverlapped
                });

                (start_files, rest)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use data_types::CompactionLevel;
    use iox_tests::ParquetFileBuilder;

    use crate::RoundInfo;

    use super::*;

    #[test]
    fn test_display() {
        assert_eq!(ManyFilesRoundSplit::new().to_string(), "many_files");
    }

    #[test]
    fn test_split_many_files() {
        let round_info = RoundInfo::ManySmallFiles {
            start_level: CompactionLevel::Initial,
            max_num_files_to_group: 2,
            max_total_file_size_to_group: 100,
        };
        let split = ManyFilesRoundSplit::new();

        // empty input
        assert_eq!(split.split(vec![], round_info), (vec![], vec![]));

        // all L0
        let f1 = ParquetFileBuilder::new(1)
            .with_compaction_level(CompactionLevel::Initial)
            .build();
        let f2 = ParquetFileBuilder::new(2)
            .with_compaction_level(CompactionLevel::Initial)
            .build();
        assert_eq!(
            split.split(vec![f1.clone(), f2.clone()], round_info),
            (vec![f1.clone(), f2.clone()], vec![])
        );

        // some L0 some L1 and some l2
        let f3 = ParquetFileBuilder::new(3)
            .with_compaction_level(CompactionLevel::FileNonOverlapped)
            .build();
        let f4 = ParquetFileBuilder::new(4)
            .with_compaction_level(CompactionLevel::Final)
            .build();
        assert_eq!(
            split.split(
                vec![f1.clone(), f2.clone(), f3.clone(), f4.clone()],
                round_info
            ),
            (vec![f1, f2], vec![f3, f4])
        );
    }

    #[test]
    fn test_split_target_level() {
        let round_info = RoundInfo::TargetLevel {
            target_level: CompactionLevel::Final,
        };
        let split = ManyFilesRoundSplit::new();

        // empty input
        assert_eq!(split.split(vec![], round_info), (vec![], vec![]));

        // non empty
        let f1 = ParquetFileBuilder::new(1).build();
        let f2 = ParquetFileBuilder::new(2).build();
        assert_eq!(
            split.split(vec![f1.clone(), f2.clone()], round_info),
            (vec![f1, f2], vec![])
        );
    }
}
