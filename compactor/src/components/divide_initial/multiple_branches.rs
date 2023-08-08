use std::fmt::Display;

use data_types::{CompactionLevel, ParquetFile, Timestamp};

use crate::{
    components::split_or_compact::start_level_files_to_split::{
        merge_small_l0_chains, split_into_chains,
    },
    RoundInfo,
};

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

impl DivideInitial for MultipleBranchesDivideInitial {
    fn divide(
        &self,
        files: Vec<ParquetFile>,
        round_info: RoundInfo,
    ) -> (Vec<Vec<ParquetFile>>, Vec<ParquetFile>) {
        let mut more_for_later = vec![];
        match round_info {
            RoundInfo::ManySmallFiles {
                start_level,
                max_num_files_to_group,
                max_total_file_size_to_group,
            } => {
                // Files must be sorted by `max_l0_created_at` when there are overlaps to resolve.
                // If the `start_level` is greater than 0, there cannot be overlaps within the level,
                // so sorting by `max_l0_created_at` is not necessary (however, sorting by `min_time`
                // is needed to avoid introducing overlaps within their levels).  When the `start_level`
                // is 0, we have to sort by `max_l0_created_at` if a chain of overlaps is too big for
                // a single compaction.
                //
                // See tests many_l0_files_different_created_order and many_l1_files_different_created_order for examples

                let start_level_files = files
                    .into_iter()
                    .filter(|f| f.compaction_level == start_level)
                    .collect::<Vec<_>>();
                let mut branches = Vec::with_capacity(start_level_files.len());

                let mut chains = Vec::with_capacity(start_level_files.len());
                if start_level == CompactionLevel::Initial {
                    // L0 files can be highly overlapping, requiring 'vertical splitting' (see high_l0_overlap_split).
                    // Achieving `vertical splitting` requires we tweak the grouping here for two reasons:
                    //  1) Allow the large highly overlapped groups of L0s to remain in a single branch, so they trigger the split
                    //  2) Prevent the output of a prior split from being grouped together to undo the previous veritacal split.

                    // Both of these objectives need to consider the L0s as a chains of overlapping files.  The chains are
                    // each a set of L0s that overlap each other, but do not overlap the other chains.
                    // Chains can be created based on min_time/max_time without regard for max_l0_created_at because there
                    // are no overlaps between chains.
                    let initial_chains = split_into_chains(start_level_files);

                    // Reason 1) above - keep the large groups of L0s in a single branch to facilitate later splitting.
                    for chain in initial_chains {
                        let this_chain_bytes: usize =
                            chain.iter().map(|f| f.file_size_bytes as usize).sum();
                        if this_chain_bytes > 2 * max_total_file_size_to_group {
                            // This is a very large set of overlapping L0s, its needs vertical splitting, so keep the branch intact
                            // to trigger the split.
                            branches.push(chain);
                        } else {
                            chains.push(chain);
                        }
                    }

                    // If the chains are smaller than the max compact size, combine them to get better compaction group sizes.
                    // This combining of chains must happen based on max_l0_created_at (it can only join adjacent chains, when
                    // sorted by max_l0_created_at).
                    chains = merge_small_l0_chains(chains, max_total_file_size_to_group);
                } else {
                    chains = vec![start_level_files];
                }

                // Reason 2) above - ensure the grouping in branches doesn't undo the vertical splitting.
                // Assume we start with 30 files (A,B,C,...), that were each split into 3 files (A1, A2, A3, B1, ..).  If we create branches
                // from sorting all files by max_l0_created_at we'd undo the vertical splitting (A1-A3 would get compacted back into one file).
                // Currently the contents of each chain is more like A1, B1, C1, so by grouping chains together we can preserve the previous
                // vertical splitting.
                for chain in chains {
                    let start_level_files = order_files(chain, start_level);

                    let capacity = start_level_files.len();

                    // Split L0s into many small groups, each has max_num_files_to_group but not exceed max_total_file_size_to_group
                    // Collect files until either limit is reached
                    let mut current_branch = Vec::with_capacity(capacity);
                    let mut current_branch_size = 0;
                    for f in start_level_files {
                        if current_branch.len() == max_num_files_to_group
                            || current_branch_size + f.file_size_bytes as usize
                                > max_total_file_size_to_group
                        {
                            // panic if current_branch is empty
                            if current_branch.is_empty() {
                                panic!("Size of a file {} is larger than the max size limit to compact. Please adjust the settings. See ticket https://github.com/influxdata/idpe/issues/17209" , f.file_size_bytes);
                            }

                            if current_branch.len() == 1 {
                                // Compacting a branch of 1 won't help us reduce the L0 file count.  Put it on the ignore list.
                                more_for_later.push(current_branch.pop().unwrap());
                            } else {
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
                }

                (branches, more_for_later)
            }

            RoundInfo::TargetLevel { .. } => (vec![files], more_for_later),

            RoundInfo::SimulatedLeadingEdge {
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
    use data_types::CompactionLevel;
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
        let round_info = RoundInfo::ManySmallFiles {
            start_level: CompactionLevel::Initial,
            max_num_files_to_group: 2,
            max_total_file_size_to_group: 100,
        };
        let divide = MultipleBranchesDivideInitial::new();

        // empty input
        assert_eq!(
            divide.divide(vec![], round_info),
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

        let (branches, more_for_later) = divide.divide(files, round_info);
        // output must be split into their max_l0_created_at
        assert_eq!(branches.len(), 1);
        assert_eq!(more_for_later.len(), 1);
        assert_eq!(branches[0], vec![f1, f2]);
    }

    #[test]
    #[should_panic(
        expected = "Size of a file 50 is larger than the max size limit to compact. Please adjust the settings"
    )]
    fn test_divide_size_limit_too_small() {
        let round_info = RoundInfo::ManySmallFiles {
            start_level: CompactionLevel::Initial,
            max_num_files_to_group: 10,
            max_total_file_size_to_group: 40,
        };
        let divide = MultipleBranchesDivideInitial::new();

        let f1 = ParquetFileBuilder::new(1)
            .with_compaction_level(CompactionLevel::Initial)
            .with_max_l0_created_at(1)
            .with_file_size_bytes(50)
            .build();
        let f2 = ParquetFileBuilder::new(2)
            .with_compaction_level(CompactionLevel::Initial)
            .with_max_l0_created_at(5)
            .with_file_size_bytes(5)
            .build();

        // files in random order of max_l0_created_at
        let files = vec![f2, f1];

        // panic
        let (_branches, _more_for_later) = divide.divide(files, round_info);
    }

    #[test]
    fn test_divide_size_limit() {
        let round_info = RoundInfo::ManySmallFiles {
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

        let (branches, more_for_later) = divide.divide(files, round_info);
        // output must be split into their max_l0_created_at
        assert_eq!(branches.len(), 1);
        assert_eq!(more_for_later.len(), 1);
        assert_eq!(branches[0], vec![f2, f3]);
    }
}
