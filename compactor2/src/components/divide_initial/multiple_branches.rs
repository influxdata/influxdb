use std::fmt::Display;

use data_types::{CompactionLevel, ParquetFile};

use crate::RoundInfo;

use super::DivideInitial;

#[derive(Debug, Default)]
pub struct MultipleBranchesDivideInitial;

impl MultipleBranchesDivideInitial {
    pub fn new() -> Self {
        Self::default()
    }
}

impl Display for MultipleBranchesDivideInitial {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "multiple_branches")
    }
}

impl DivideInitial for MultipleBranchesDivideInitial {
    fn divide(&self, files: Vec<ParquetFile>, round_info: &RoundInfo) -> Vec<Vec<ParquetFile>> {
        match round_info {
            RoundInfo::ManySmallFiles {
                start_level,
                max_num_files_to_group,
                max_total_file_size_to_group,
            } => {
                // To split the start_level files correctly so they can be compacted in the right order,
                // the files must be sorted on `max_l0_created_at` if start_level is 0 or `min_time` otherwise.
                //
                // Since L0s can overlap, they can contain duplicate data, which can only be resolved by
                // using `max_l0_created_at` time, so the `max_l0_created_at` time must be used to sort so that it is
                // preserved.  Since L1s & L2s cannot overlap within their own level, they cannot contain
                // duplicate data within their own level, so they do not need to preserve their `max_l0_created_at`
                // time, so they do not need to be sorted based on `max_l0_created_at`.  However, sorting by
                // `min_time` is needed to avoid introducing overlaps within their levels.
                //
                // See tests many_l0_files_different_created_order and many_l1_files_different_created_order for examples
                let start_level_files = files
                    .into_iter()
                    .filter(|f| f.compaction_level == *start_level)
                    .collect::<Vec<_>>();
                let start_level_files = order_files(start_level_files, start_level);

                // Split L0s into many small groups, each has max_num_files_to_group but not exceed max_total_file_size_to_group
                // Collect files until either limit is reached
                let mut branches = vec![];
                let mut current_branch = vec![];
                let mut current_branch_size = 0;
                for f in start_level_files {
                    if current_branch.len() == *max_num_files_to_group
                        || current_branch_size + f.file_size_bytes as usize
                            > *max_total_file_size_to_group
                    {
                        // panic if current_branch is empty
                        if current_branch.is_empty() {
                            panic!("Size of a file {} is larger than the max size limit to compact. Please adjust the settings. See ticket https://github.com/influxdata/idpe/issues/17209" , f.file_size_bytes);
                        }

                        branches.push(current_branch);
                        current_branch = vec![];
                        current_branch_size = 0;
                    }
                    current_branch_size += f.file_size_bytes as usize;
                    current_branch.push(f);
                }

                // push the last branch
                if !current_branch.is_empty() {
                    branches.push(current_branch);
                }

                branches
            }
            RoundInfo::TargetLevel { .. } => vec![files],
        }
    }
}

/// Return a sorted files of the given ones.
/// The order is used to split the files and form the right groups of files to compact
// and deduplcate correctly to fewer and larger but same level files
///
/// All given files are in the same given start_level.
/// They will be sorted on their `max_l0_created_at` if the start_level is 0,
/// otherwise on their `min_time`
pub fn order_files(files: Vec<ParquetFile>, start_level: &CompactionLevel) -> Vec<ParquetFile> {
    let mut files = files;
    if *start_level == CompactionLevel::Initial {
        files.sort_by(|a, b| a.max_l0_created_at.cmp(&b.max_l0_created_at));
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
        assert_eq!(divide.divide(vec![], &round_info), Vec::<Vec<_>>::new());

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

        let branches = divide.divide(files, &round_info);
        // output must be split into their max_l0_created_at
        assert_eq!(branches.len(), 2);
        assert_eq!(branches[0], vec![f1, f2]);
        assert_eq!(branches[1], vec![f3]);
    }

    #[test]
    #[should_panic(
        expected = "Size of a file 50 is larger than the max size limit to compact. Please adjust the settings"
    )]
    fn test_divide_size_limit_too_sall() {
        let round_info = RoundInfo::ManySmallFiles {
            start_level: CompactionLevel::Initial,
            max_num_files_to_group: 10,
            max_total_file_size_to_group: 10,
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
        let _branches = divide.divide(files, &round_info);
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

        let branches = divide.divide(files, &round_info);
        // output must be split into their max_l0_created_at
        assert_eq!(branches.len(), 2);
        assert_eq!(branches[0], vec![f1]);
        assert_eq!(branches[1], vec![f2, f3]);
    }
}
