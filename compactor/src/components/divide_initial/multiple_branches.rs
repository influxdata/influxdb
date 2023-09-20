use std::fmt::Display;

use data_types::{CompactionLevel, ParquetFile, Timestamp, TransitionPartitionId};
use observability_deps::tracing::warn;

use crate::round_info::CompactType;

use super::DivideInitial;

#[derive(Debug, Default)]
pub struct MultipleBranchesDivideInitial;

impl MultipleBranchesDivideInitial {
    pub fn new() -> Self {
        Self
    }
}

impl Display for MultipleBranchesDivideInitial {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "multiple_branches")
    }
}

// TODO(joe): maintain this comment through the next few PRs; see how true the comment is/remains.
// divide is the second of three file list manipluation layers.  split has already filtered most of the
// files not relevant to this round of compaction.  Now divide will group files into branches which can
// be concurrently operated on.  As of this comment, dividing the files into branches is fairly simple,
// except for:
//   - ManySmallFiles, which is cluttered from previous challenges when RoundInfo didn't have as much influence.
//     this clutter will either be going away, or maybe ManySmallFiles goes away entirely
//   - SimulatedLeadingEdge which is likley a temporary workaround during the refactoring (i.e. likely go go away)
//
// Over time this function should work towards being a simpler grouping into batches for concurrent operations.
// If that happens (and this layer remains), this should probably be renamed to emphasize its role in branch creation
// rather than the current abigious 'divide' which sounds potentially overlapping with divisions happening in the
// other layers.
impl DivideInitial for MultipleBranchesDivideInitial {
    fn divide(
        &self,
        files: Vec<ParquetFile>,
        op: CompactType,
        partition: TransitionPartitionId,
    ) -> (Vec<Vec<ParquetFile>>, Vec<ParquetFile>) {
        let mut more_for_later = vec![];
        match op {
            CompactType::ManySmallFiles {
                start_level,
                max_num_files_to_group,
                max_total_file_size_to_group,
            } => {
                // Since its ManySmallFiles, we know the files are L0s, and the total bytes is under our limit.
                // We just need to split them up into branches for compaction.
                // TODO: it would be nice to pick some good split times, store them in the ManySmallFiles op, and use them consistently across all the branches.  That should make the later round more efficient.
                let mut branches = Vec::with_capacity(files.len() / max_num_files_to_group);
                let files = order_files(files, start_level);
                let capacity = files.len();

                let mut current_branch = Vec::with_capacity(capacity.min(max_num_files_to_group));
                let mut current_branch_size = 0;
                for f in files {
                    if current_branch.len() == max_num_files_to_group
                        || current_branch_size + f.file_size_bytes as usize
                            > max_total_file_size_to_group
                    {
                        if current_branch.is_empty() {
                            warn!(
                                "Size of a file {} is larger than the max size limit to compact on partition {}.",
                                f.file_size_bytes,
                                partition
                            );
                        }

                        if current_branch.len() == 1 {
                            // Compacting a branch of 1 won't help us reduce the L0 file count.  Put it on the ignore list.
                            more_for_later.push(current_branch.pop().unwrap());
                        } else if !current_branch.is_empty() {
                            branches.push(current_branch);
                        }
                        current_branch = Vec::with_capacity(capacity);
                        current_branch_size = 0;
                    }
                    current_branch_size += f.file_size_bytes as usize;
                    current_branch.push(f);
                }

                // push the last branch
                if !current_branch.is_empty() {
                    if current_branch.len() == 1 {
                        // Compacting a branch of 1 won't help us reduce the L0 file count.  Put it on the ignore list.
                        more_for_later.push(current_branch.pop().unwrap());
                    } else {
                        branches.push(current_branch);
                    }
                }

                (branches, more_for_later)
            }

            CompactType::TargetLevel {
                target_level,
                max_total_file_size_to_group,
            } => {
                let start_level = target_level.prev();

                let total_bytes: usize = files.iter().map(|f| f.file_size_bytes as usize).sum();
                let start_file_cnt = files
                    .iter()
                    .filter(|f| f.compaction_level == start_level)
                    .count();

                if start_file_cnt == 0 {
                    // No files to compact
                    (vec![], files)
                } else if total_bytes < max_total_file_size_to_group {
                    (vec![files], more_for_later)
                } else {
                    let (mut for_now, rest): (Vec<ParquetFile>, Vec<ParquetFile>) = files
                        .into_iter()
                        .partition(|f| f.compaction_level == start_level);

                    let min_time = for_now.iter().map(|f| f.min_time).min().unwrap();
                    let max_time = for_now.iter().map(|f| f.max_time).max().unwrap();

                    let (overlaps, for_later): (Vec<ParquetFile>, Vec<ParquetFile>) = rest
                        .into_iter()
                        .partition(|f2| f2.overlaps_time_range(min_time, max_time));

                    for_now.extend(overlaps);

                    (vec![for_now], for_later)
                }
            }

            CompactType::SimulatedLeadingEdge {
                max_num_files_to_group,
                max_total_file_size_to_group,
            } => {
                // There may be a lot of L0s, but we're going to keep it simple and just look at the first (few).
                let start_level = CompactionLevel::Initial;

                // Separate start_level_files (L0s) from target_level_files (L1), and `more_for_later` which is all the rest of the files (L2).
                // We'll then with the first L0 and see how many L1s it needs to drag into its compaction, and then look at the next L0
                // and see how many L1s it needs, etc.  We'll end up with 1 or more L0s, and whatever L1s they need to compact with.
                // When we can't add any more without getting "too big", we'll consider that our branch, and all remaining L0s, L1s & L2s
                // are returned as

                let (start_level_files, rest): (Vec<ParquetFile>, Vec<ParquetFile>) = files
                    .into_iter()
                    .partition(|f| f.compaction_level == start_level);

                let (mut target_level_files, mut more_for_later): (
                    Vec<ParquetFile>,
                    Vec<ParquetFile>,
                ) = rest
                    .into_iter()
                    .partition(|f| f.compaction_level == start_level.next());

                let mut start_level_files = order_files(start_level_files, start_level);
                let mut current_branch = Vec::with_capacity(start_level_files.len());
                let mut current_branch_size = 0;
                let mut min_time = Timestamp::new(i64::MAX);
                let mut max_time = Timestamp::new(0);

                // Until we run out of L0s or return early with a branch to compact, keep adding L0s & their overlapping L1s to the current branch.
                while !start_level_files.is_empty() {
                    let f = start_level_files.remove(0);

                    // overlaps of this new file isn't enough - we must look for overlaps of this + all previously added L0s, because the result of
                    // compacting them will be that whole time range.  Therefore, we must include all L1s overlapping that entire time range.
                    min_time = min_time.min(f.min_time);
                    max_time = max_time.max(f.max_time);

                    let (mut overlaps, remainder): (Vec<ParquetFile>, Vec<ParquetFile>) =
                        target_level_files
                            .into_iter()
                            .partition(|f2| f2.overlaps_time_range(min_time, max_time));

                    target_level_files = remainder;

                    if current_branch_size == 0 || // minimum 1 start level file
                    (current_branch.len() + overlaps.len() < max_num_files_to_group && current_branch_size + f.size() < max_total_file_size_to_group)
                    {
                        // This L0 & its overlapping L1s fit in the current branch.
                        current_branch_size += f.size();
                        current_branch.push(f);
                        current_branch.append(&mut overlaps);
                    } else {
                        // This L0 & its overlapping L1s would make the current branch too big.
                        // We're done - what we previously added to the branch will be compacted, everything else goes in more_for_later.
                        more_for_later.push(f);
                        more_for_later.append(&mut overlaps);
                        more_for_later.append(&mut start_level_files);
                        more_for_later.append(&mut target_level_files);
                        return (vec![current_branch], more_for_later);
                    }
                }
                (vec![current_branch], more_for_later)
            }

            // RoundSplit already eliminated all the files we don't need to work on.
            CompactType::VerticalSplit { .. } => (vec![files], more_for_later),

            // Deferred does nothing now, everything is for later
            CompactType::Deferred { .. } => (vec![], files),
        }
    }
}

/// Return a sorted files of the given ones.
/// The order is used to split the files and form the right groups of files to compact
/// and deduplicate correctly to fewer and larger but same level files
///
/// All given files are in the same given start_level.
/// They will be sorted on their `max_l0_created_at` (then `min_time`) if the start_level is 0,
/// otherwise on their `min_time`
pub fn order_files(files: Vec<ParquetFile>, start_level: CompactionLevel) -> Vec<ParquetFile> {
    let mut files = files;
    if start_level == CompactionLevel::Initial {
        files.sort_by(|a, b| {
            if a.max_l0_created_at == b.max_l0_created_at {
                a.min_time.cmp(&b.min_time)
            } else {
                a.max_l0_created_at.cmp(&b.max_l0_created_at)
            }
        })
    } else {
        files.sort_by(|a, b| a.min_time.cmp(&b.min_time));
    }
    files
}

#[cfg(test)]
mod tests {
    use data_types::{CompactionLevel, PartitionId};
    use iox_tests::ParquetFileBuilder;

    use super::*;

    #[test]
    fn test_display() {
        assert_eq!(
            MultipleBranchesDivideInitial::new().to_string(),
            "multiple_branches"
        );
    }

    #[test]
    fn test_divide_num_file() {
        let op = CompactType::ManySmallFiles {
            start_level: CompactionLevel::Initial,
            max_num_files_to_group: 2,
            max_total_file_size_to_group: 100,
        };
        let divide = MultipleBranchesDivideInitial::new();

        // empty input
        assert_eq!(
            divide.divide(
                vec![],
                op.clone(),
                TransitionPartitionId::Deprecated(PartitionId::new(0))
            ),
            (Vec::<Vec<_>>::new(), Vec::new())
        );

        // not empty
        let f1 = ParquetFileBuilder::new(1)
            .with_compaction_level(CompactionLevel::Initial)
            .with_max_l0_created_at(1)
            .build();
        let f2 = ParquetFileBuilder::new(2)
            .with_compaction_level(CompactionLevel::Initial)
            .with_max_l0_created_at(5)
            .build();
        let f3 = ParquetFileBuilder::new(3)
            .with_compaction_level(CompactionLevel::Initial)
            .with_max_l0_created_at(10)
            .build();

        // files in random order of max_l0_created_at
        let files = vec![f2.clone(), f3.clone(), f1.clone()];

        let (branches, more_for_later) = divide.divide(
            files,
            op.clone(),
            TransitionPartitionId::Deprecated(PartitionId::new(0)),
        );
        // output must be split into their max_l0_created_at
        assert_eq!(branches.len(), 1);
        assert_eq!(more_for_later.len(), 1);
        assert_eq!(branches[0], vec![f1, f2]);
    }

    #[test]
    fn test_divide_size_limit() {
        let op = CompactType::ManySmallFiles {
            start_level: CompactionLevel::Initial,
            max_num_files_to_group: 10,
            max_total_file_size_to_group: 100,
        };
        let divide = MultipleBranchesDivideInitial::new();

        let f1 = ParquetFileBuilder::new(1)
            .with_compaction_level(CompactionLevel::Initial)
            .with_max_l0_created_at(1)
            .with_file_size_bytes(90)
            .build();
        let f2 = ParquetFileBuilder::new(2)
            .with_compaction_level(CompactionLevel::Initial)
            .with_max_l0_created_at(5)
            .with_file_size_bytes(20)
            .build();
        let f3 = ParquetFileBuilder::new(3)
            .with_compaction_level(CompactionLevel::Initial)
            .with_max_l0_created_at(10)
            .with_file_size_bytes(30)
            .build();

        // files in random order of max_l0_created_at
        let files = vec![f2.clone(), f3.clone(), f1.clone()];

        let (branches, more_for_later) = divide.divide(
            files,
            op,
            TransitionPartitionId::Deprecated(PartitionId::new(0)),
        );
        // output must be split into their max_l0_created_at
        assert_eq!(branches.len(), 1);
        assert_eq!(more_for_later.len(), 1);
        assert_eq!(branches[0], vec![f2, f3]);
    }
}
