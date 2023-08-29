use std::fmt::Display;

use data_types::{CompactionLevel, ParquetFile, TransitionPartitionId};

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

// TODO(joe): maintain this comment through the next few PRs; see how true the comment is/remains.
// split is the first of three file list manipulation layers.  Based on the RoundInfo, split will do
// some simple filtering to remove files easily identifiable as not relevant to this round.
impl RoundSplit for ManyFilesRoundSplit {
    fn split(
        &self,
        files: Vec<ParquetFile>,
        round_info: RoundInfo,
        partition: TransitionPartitionId,
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
            RoundInfo::TargetLevel { target_level, .. } => {
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

            RoundInfo::VerticalSplit { split_times } => {
                // We're splitting L0 files at split_times.  So any L0 that overlaps a split_time needs processed, and all other files are ignored until later.
                let (split_files, rest): (Vec<ParquetFile>, Vec<ParquetFile>) =
                    files.into_iter().partition(|f| {
                        f.compaction_level != CompactionLevel::Final && f.needs_split(&split_times)
                    });

                assert!(
                    !split_files.is_empty(),
                    "if we decided to split, there should be something to split, instead found no split_files for partition {}",
                    partition
                );
                (split_files, rest)
            }

            RoundInfo::CompactRanges { ranges, .. } => {
                // We're compacting L0 & L1s in the specified ranges.  Files outside these ranges are
                // ignored until a later round.
                let (compact_files, rest): (Vec<ParquetFile>, Vec<ParquetFile>) =
                    files.into_iter().partition(|f| {
                        f.compaction_level != CompactionLevel::Final && f.overlaps_ranges(&ranges)
                    });

                (compact_files, rest)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use data_types::{CompactionLevel, PartitionId};
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
        let default_partition = TransitionPartitionId::Deprecated(PartitionId::new(0));

        // empty input
        assert_eq!(
            split.split(vec![], round_info.clone(), default_partition.clone()),
            (vec![], vec![])
        );

        // all L0
        let f1 = ParquetFileBuilder::new(1)
            .with_compaction_level(CompactionLevel::Initial)
            .build();
        let f2 = ParquetFileBuilder::new(2)
            .with_compaction_level(CompactionLevel::Initial)
            .build();
        assert_eq!(
            split.split(
                vec![f1.clone(), f2.clone()],
                round_info.clone(),
                default_partition.clone()
            ),
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
                round_info.clone(),
                default_partition,
            ),
            (vec![f1, f2], vec![f3, f4])
        );
    }

    #[test]
    fn test_split_target_level() {
        let round_info = RoundInfo::TargetLevel {
            target_level: CompactionLevel::Final,
            max_total_file_size_to_group: 100 * 1024 * 1024,
        };
        let split = ManyFilesRoundSplit::new();
        let default_partition = TransitionPartitionId::Deprecated(PartitionId::new(0));

        // empty input
        assert_eq!(
            split.split(vec![], round_info.clone(), default_partition.clone()),
            (vec![], vec![])
        );

        // non empty
        let f1 = ParquetFileBuilder::new(1).build();
        let f2 = ParquetFileBuilder::new(2).build();
        assert_eq!(
            split.split(
                vec![f1.clone(), f2.clone()],
                round_info.clone(),
                default_partition
            ),
            (vec![f1, f2], vec![])
        );
    }
}
