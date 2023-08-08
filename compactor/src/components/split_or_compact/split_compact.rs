use std::fmt::Display;

use data_types::{CompactionLevel, ParquetFile};

use crate::{
    file_classification::{CompactReason, FilesToSplitOrCompact, NoneReason, SplitReason},
    partition_info::PartitionInfo,
};

use super::{
    files_to_compact::limit_files_to_compact,
    large_files_to_split::compute_split_times_for_large_files,
    start_level_files_to_split::{high_l0_overlap_split, identify_start_level_files_to_split},
    SplitOrCompact,
};

#[derive(Debug)]
pub struct SplitCompact {
    max_compact_files: usize,
    max_compact_size: usize,
    max_desired_file_size: u64,
}

impl SplitCompact {
    pub fn new(
        max_compact_files: usize,
        max_compact_size: usize,
        max_desired_file_size: u64,
    ) -> Self {
        Self {
            max_compact_files,
            max_compact_size,
            max_desired_file_size,
        }
    }
}

impl Display for SplitCompact {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "split_or_compact({}, {})",
            self.max_compact_size, self.max_desired_file_size
        )
    }
}

impl SplitOrCompact for SplitCompact {
    /// Return (`[files_to_split_or_compact]`, `[files_to_keep]`) of given files
    ///
    /// Verify if the the given files are over the max_compact_size or file count limit, then:
    /// (1).If >max_compact_size files overlap each other (i.e. they all overlap, not just each one
    ///     overlapping its neighbors), perform a 'vertical split' on all overlapping files.
    /// (2).Find start-level files that can be split to reduce the number of overlapped
    ///     files that must be compact in one run. This split will align the time ranges of
    ///    start_level files with target level files.
    /// (3).If split is not needed which also means the split was needed and done in previous round,
    ///     pick files to compact that under max_compact_size limit. Mostly after the split above
    ///     done in previous round, we will be able to do this because start level and
    ///     target level time ranges are aligned
    /// (4).If the smallest possible set to compact is still over size limit, split over-size files.
    ///     This will be any large files of start-level or target-level. We expect this split is very rare
    ///     and the goal is to reduce the size for us to move forward, hence the split time will make e
    ///     ach output file soft max file size. If this split is not rare and/or created many non-aligned
    ///    files that will lead to more splits in next round, it won't be efficient
    fn apply(
        &self,
        _partition_info: &PartitionInfo,
        files: Vec<ParquetFile>,
        target_level: CompactionLevel,
    ) -> (FilesToSplitOrCompact, Vec<ParquetFile>) {
        if files.is_empty() {
            return (
                FilesToSplitOrCompact::None(NoneReason::NoInputFiles),
                vec![],
            );
        }

        // Compact all in one run if total size and file count are under the limit.
        let total_size: i64 = files.iter().map(|f| f.file_size_bytes).sum();
        let start_level_files: usize = files
            .iter()
            .filter(|f| f.compaction_level == target_level.prev())
            .collect::<Vec<&ParquetFile>>()
            .len();

        if total_size as usize <= self.max_compact_size
            && start_level_files < self.max_compact_files
        {
            return (
                FilesToSplitOrCompact::Compact(
                    files,
                    CompactReason::TotalSizeLessThanMaxCompactSize,
                ),
                vec![],
            );
        }

        // (1) this function checks for a highly overlapped L0s
        let (files_to_split, remaining_files, reason) =
            high_l0_overlap_split(self.max_compact_size, files, target_level);

        if !files_to_split.is_empty() {
            // These files must be split before further compaction
            return (
                FilesToSplitOrCompact::Split(files_to_split, reason),
                remaining_files,
            );
        }

        // (2) This function identifies all start-level files that overlap with more than one target-level files
        let (files_to_split, files_not_to_split) =
            identify_start_level_files_to_split(remaining_files, target_level);

        if !files_to_split.is_empty() {
            // These files must be split before further compaction
            return (
                FilesToSplitOrCompact::Split(files_to_split, SplitReason::ReduceOverlap),
                files_not_to_split,
            );
        }

        // (3) No start level split is needed, which means every start-level file overlaps with at most one target-level file
        // Need to limit number of files to compact to stay under compact size limit
        let keep_and_split_or_compact = limit_files_to_compact(
            self.max_compact_files,
            self.max_compact_size,
            files_not_to_split,
            target_level,
        );

        let files_to_compact = keep_and_split_or_compact.files_to_compact();
        let files_to_further_split = keep_and_split_or_compact.files_to_further_split();
        let specific_splits = keep_and_split_or_compact.specific_splits();
        let mut files_to_keep = keep_and_split_or_compact.files_to_keep();

        if !files_to_compact.is_empty() {
            return (
                FilesToSplitOrCompact::Compact(
                    files_to_compact,
                    CompactReason::FoundSubsetLessThanMaxCompactSize,
                ),
                files_to_keep,
            );
        }

        if !specific_splits.is_empty() {
            // limit_files_to_compact picked start level files, then the target level files that overlap them, but those
            // target level files overlap more start level files, and that was too much.
            return (
                FilesToSplitOrCompact::Split(
                    specific_splits,
                    SplitReason::StartLevelOverlapsTooBig,
                ),
                files_to_keep,
            );
        }

        // (4) Not able to compact the smallest set, split the large files
        let (files_to_split, files_not_to_split) = compute_split_times_for_large_files(
            files_to_further_split,
            self.max_desired_file_size,
            self.max_compact_size,
        );

        files_to_keep.extend(files_not_to_split);

        if files_to_split.is_empty() {
            (
                FilesToSplitOrCompact::None(NoneReason::NoFilesToSplitFound),
                files_to_keep,
            )
        } else {
            (
                FilesToSplitOrCompact::Split(files_to_split, SplitReason::ReduceLargeFileSize),
                files_to_keep,
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use compactor_test_utils::{
        create_overlapped_l0_l1_files_2, create_overlapped_l1_l2_files_2, format_files,
        format_files_split,
    };
    use data_types::CompactionLevel;

    use crate::{
        components::split_or_compact::{
            large_files_to_split::PERCENTAGE_OF_SOFT_EXCEEDED, split_compact::SplitCompact,
            SplitOrCompact,
        },
        file_classification::{FilesToSplitOrCompact, NoneReason},
        test_utils::PartitionInfoBuilder,
    };

    const FILE_SIZE: usize = 100;
    const FILE_COUNT: usize = 20;

    #[test]
    fn test_empty() {
        let files = vec![];
        let p_info = Arc::new(PartitionInfoBuilder::new().build());
        let split_compact = SplitCompact::new(FILE_COUNT, FILE_SIZE, FILE_SIZE as u64);
        let (files_to_split_or_compact, files_to_keep) =
            split_compact.apply(&p_info, files, CompactionLevel::Initial);

        assert!(matches!(
            files_to_split_or_compact,
            FilesToSplitOrCompact::None(NoneReason::NoInputFiles),
        ));
        assert!(files_to_keep.is_empty());
    }

    #[test]
    #[should_panic(
        expected = "max_compact_size 100 must be at least 2 times larger than max_desired_file_size 100"
    )]
    fn test_compact_invalid_max_compact_size() {
        let files = create_overlapped_l1_l2_files_2(FILE_SIZE as i64);
        insta::assert_yaml_snapshot!(
            format_files("initial", &files),
            @r###"
        ---
        - initial
        - "L1, all files 100b                                                                                                 "
        - "L1.13[600,700] 4ns                                                                               |-----L1.13------|"
        - "L1.12[400,500] 3ns                                           |-----L1.12------|                                    "
        - "L1.11[250,350] 2ns                |-----L1.11------|                                                               "
        - "L2, all files 100b                                                                                                 "
        - "L2.22[200,300] 1ns       |-----L2.22------|                                                                        "
        "###
        );

        let p_info = Arc::new(PartitionInfoBuilder::new().build());
        let split_compact = SplitCompact::new(FILE_COUNT, FILE_SIZE, FILE_SIZE as u64);
        let (_files_to_split_or_compact, _files_to_keep) =
            split_compact.apply(&p_info, files, CompactionLevel::Final);
    }

    #[test]
    fn test_compact_too_large_to_compact() {
        let files = create_overlapped_l1_l2_files_2(FILE_SIZE as i64);
        insta::assert_yaml_snapshot!(
            format_files("initial", &files),
            @r###"
        ---
        - initial
        - "L1, all files 100b                                                                                                 "
        - "L1.13[600,700] 4ns                                                                               |-----L1.13------|"
        - "L1.12[400,500] 3ns                                           |-----L1.12------|                                    "
        - "L1.11[250,350] 2ns                |-----L1.11------|                                                               "
        - "L2, all files 100b                                                                                                 "
        - "L2.22[200,300] 1ns       |-----L2.22------|                                                                        "
        "###
        );

        let p_info = Arc::new(PartitionInfoBuilder::new().build());
        let max_desired_file_size =
            FILE_SIZE - (FILE_SIZE as f64 * PERCENTAGE_OF_SOFT_EXCEEDED) as usize - 30;
        let max_compact_size = 3 * max_desired_file_size;
        let split_compact =
            SplitCompact::new(FILE_COUNT, max_compact_size, max_desired_file_size as u64);
        let (files_to_split_or_compact, files_to_keep) =
            split_compact.apply(&p_info, files, CompactionLevel::Final);

        // need to split files
        assert_eq!(files_to_split_or_compact.num_files_to_compact(), 0);
        let files_to_split = if let FilesToSplitOrCompact::Split(ref files_to_split, ..) =
            files_to_split_or_compact
        {
            files_to_split
        } else {
            panic!("Expected files to split, instead got {files_to_split_or_compact:?}");
        };
        assert_eq!(files_to_split.len(), 2);
        assert_eq!(files_to_keep.len(), 2);

        // both L1.11 and L2.22 are just a bit larger than max_desired_file_size
        // so they are split into 2 files each. This means the split_times of each includes one time where it is split into 2 files
        for times in files_to_split.iter().map(|f| &f.split_times) {
            assert_eq!(times.len(), 1);
        }

        let parquet_files_to_split = files_to_split_or_compact.into_files();

        // See layout of 2 set of files
        insta::assert_yaml_snapshot!(
            format_files_split("files to split", &parquet_files_to_split, "files to keep:", &files_to_keep),
            @r###"
        ---
        - files to split
        - "L1, all files 100b                                                                                                 "
        - "L1.11[250,350] 2ns                                     |--------------------------L1.11---------------------------|"
        - "L2, all files 100b                                                                                                 "
        - "L2.22[200,300] 1ns       |--------------------------L2.22---------------------------|                              "
        - "files to keep:"
        - "L1, all files 100b                                                                                                 "
        - "L1.12[400,500] 3ns       |-----------L1.12------------|                                                            "
        - "L1.13[600,700] 4ns                                                                   |-----------L1.13------------|"
        "###
        );
    }

    #[test]
    fn test_compact_files_no_limit() {
        let files = create_overlapped_l0_l1_files_2(FILE_SIZE as i64);
        insta::assert_yaml_snapshot!(
            format_files("initial", &files),
            @r###"
        ---
        - initial
        - "L0, all files 100b                                                                                                 "
        - "L0.2[650,750] 180s                                                    |------L0.2------|                           "
        - "L0.1[450,620] 120s                |------------L0.1------------|                                                   "
        - "L0.3[800,900] 300s                                                                               |------L0.3------|"
        - "L1, all files 100b                                                                                                 "
        - "L1.13[600,700] 60s                                           |-----L1.13------|                                    "
        - "L1.12[400,500] 60s       |-----L1.12------|                                                                        "
        "###
        );

        // size limit > total size --> compact all
        let p_info = Arc::new(PartitionInfoBuilder::new().build());
        let split_compact = SplitCompact::new(FILE_COUNT, FILE_SIZE * 6 + 1, FILE_SIZE as u64);
        let (files_to_split_or_compact, files_to_keep) =
            split_compact.apply(&p_info, files, CompactionLevel::FileNonOverlapped);

        assert_eq!(files_to_split_or_compact.num_files_to_compact(), 5);
        assert!(files_to_keep.is_empty());

        let files_to_compact = files_to_split_or_compact.into_files();

        // See layout of 2 set of files
        insta::assert_yaml_snapshot!(
            format_files_split("files to compact", &files_to_compact, "files to keep:", &files_to_keep),
            @r###"
        ---
        - files to compact
        - "L0, all files 100b                                                                                                 "
        - "L0.2[650,750] 180s                                                    |------L0.2------|                           "
        - "L0.1[450,620] 120s                |------------L0.1------------|                                                   "
        - "L0.3[800,900] 300s                                                                               |------L0.3------|"
        - "L1, all files 100b                                                                                                 "
        - "L1.13[600,700] 60s                                           |-----L1.13------|                                    "
        - "L1.12[400,500] 60s       |-----L1.12------|                                                                        "
        - "files to keep:"
        "###
        );
    }

    #[test]
    fn test_split_files() {
        let files = create_overlapped_l0_l1_files_2(FILE_SIZE as i64 - 1);
        insta::assert_yaml_snapshot!(
            format_files("initial", &files),
            @r###"
        ---
        - initial
        - "L0, all files 99b                                                                                                  "
        - "L0.2[650,750] 180s                                                    |------L0.2------|                           "
        - "L0.1[450,620] 120s                |------------L0.1------------|                                                   "
        - "L0.3[800,900] 300s                                                                               |------L0.3------|"
        - "L1, all files 99b                                                                                                  "
        - "L1.13[600,700] 60s                                           |-----L1.13------|                                    "
        - "L1.12[400,500] 60s       |-----L1.12------|                                                                        "
        "###
        );

        // hit size limit -> split start_level files that overlap with more than 1 target_level files
        let p_info = Arc::new(PartitionInfoBuilder::new().build());
        let split_compact = SplitCompact::new(FILE_COUNT, FILE_SIZE * 3, FILE_SIZE as u64);
        let (files_to_split_or_compact, files_to_keep) =
            split_compact.apply(&p_info, files, CompactionLevel::FileNonOverlapped);

        assert_eq!(files_to_split_or_compact.num_files_to_split(), 1);
        assert_eq!(files_to_keep.len(), 4);

        let files_to_split = files_to_split_or_compact.into_files();

        // See layout of 2 set of files
        insta::assert_yaml_snapshot!(
            format_files_split("files to compact or split:", &files_to_split, "files to keep:", &files_to_keep),
            @r###"
        ---
        - "files to compact or split:"
        - "L0, all files 99b                                                                                                  "
        - "L0.1[450,620] 120s       |------------------------------------------L0.1------------------------------------------|"
        - "files to keep:"
        - "L0, all files 99b                                                                                                  "
        - "L0.2[650,750] 180s                                                    |------L0.2------|                           "
        - "L0.3[800,900] 300s                                                                               |------L0.3------|"
        - "L1, all files 99b                                                                                                  "
        - "L1.12[400,500] 60s       |-----L1.12------|                                                                        "
        - "L1.13[600,700] 60s                                           |-----L1.13------|                                    "
        "###
        );
    }

    #[test]
    fn test_split_files_high_capacity() {
        let files = create_overlapped_l0_l1_files_2(FILE_SIZE as i64);
        insta::assert_yaml_snapshot!(
            format_files("initial", &files),
            @r###"
        ---
        - initial
        - "L0, all files 100b                                                                                                 "
        - "L0.2[650,750] 180s                                                    |------L0.2------|                           "
        - "L0.1[450,620] 120s                |------------L0.1------------|                                                   "
        - "L0.3[800,900] 300s                                                                               |------L0.3------|"
        - "L1, all files 100b                                                                                                 "
        - "L1.13[600,700] 60s                                           |-----L1.13------|                                    "
        - "L1.12[400,500] 60s       |-----L1.12------|                                                                        "
        "###
        );

        // hit max_compact_size limit on files that overlap just L0.1, and split that set of files into something manageable.
        let p_info = Arc::new(PartitionInfoBuilder::new().build());
        let split_compact = SplitCompact::new(FILE_COUNT, FILE_SIZE, FILE_SIZE as u64);
        let (files_to_split_or_compact, files_to_keep) =
            split_compact.apply(&p_info, files, CompactionLevel::FileNonOverlapped);

        assert_eq!(files_to_split_or_compact.num_files_to_split(), 4);
        assert_eq!(files_to_keep.len(), 1);

        let files_to_split = files_to_split_or_compact.into_files();

        // See layout of 2 set of files
        insta::assert_yaml_snapshot!(
            format_files_split("files to compact or split:", &files_to_split, "files to keep:", &files_to_keep),
            @r###"
        ---
        - "files to compact or split:"
        - "L0, all files 100b                                                                                                 "
        - "L0.1[450,620] 120s                   |------------------L0.1-------------------|                                   "
        - "L0.2[650,750] 180s                                                                       |---------L0.2----------| "
        - "L1, all files 100b                                                                                                 "
        - "L1.12[400,500] 60s       |---------L1.12---------|                                                                 "
        - "L1.13[600,700] 60s                                                          |---------L1.13---------|              "
        - "files to keep:"
        - "L0, all files 100b                                                                                                 "
        - "L0.3[800,900] 300s       |------------------------------------------L0.3------------------------------------------|"
        "###
        );
    }

    #[test]
    fn test_compact_files() {
        let files = create_overlapped_l1_l2_files_2(FILE_SIZE as i64);
        insta::assert_yaml_snapshot!(
            format_files("initial", &files),
            @r###"
        ---
        - initial
        - "L1, all files 100b                                                                                                 "
        - "L1.13[600,700] 4ns                                                                               |-----L1.13------|"
        - "L1.12[400,500] 3ns                                           |-----L1.12------|                                    "
        - "L1.11[250,350] 2ns                |-----L1.11------|                                                               "
        - "L2, all files 100b                                                                                                 "
        - "L2.22[200,300] 1ns       |-----L2.22------|                                                                        "
        "###
        );

        // hit size limit and nthign to split --> limit number if files to compact
        let p_info = Arc::new(PartitionInfoBuilder::new().build());
        let split_compact = SplitCompact::new(FILE_COUNT, FILE_SIZE * 3, FILE_SIZE as u64);
        let (files_to_split_or_compact, files_to_keep) =
            split_compact.apply(&p_info, files, CompactionLevel::Final);

        assert_eq!(files_to_split_or_compact.num_files_to_compact(), 3);
        assert_eq!(files_to_keep.len(), 1);

        let files_to_compact = files_to_split_or_compact.into_files();

        // See layout of 2 set of files
        insta::assert_yaml_snapshot!(
            format_files_split("files to compact or split:", &files_to_compact, "files to keep:", &files_to_keep),
            @r###"
        ---
        - "files to compact or split:"
        - "L1, all files 100b                                                                                                 "
        - "L1.11[250,350] 2ns                      |-----------L1.11------------|                                             "
        - "L1.12[400,500] 3ns                                                                   |-----------L1.12------------|"
        - "L2, all files 100b                                                                                                 "
        - "L2.22[200,300] 1ns       |-----------L2.22------------|                                                            "
        - "files to keep:"
        - "L1, all files 100b                                                                                                 "
        - "L1.13[600,700] 4ns       |-----------------------------------------L1.13------------------------------------------|"
        "###
        );
    }
}
