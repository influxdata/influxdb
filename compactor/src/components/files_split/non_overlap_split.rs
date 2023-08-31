use std::{collections::VecDeque, fmt::Display};

use data_types::{CompactionLevel, ParquetFile, TransitionPartitionId};

use crate::file_group::{split_by_level, FilesTimeRange};

use super::FilesSplit;

#[derive(Debug)]

/// Split files into `[compact_files]` and `[non_overlapping_files]`
/// To have better and efficient compaction performance, eligible non-overlapped files
/// should not be compacted.
pub struct NonOverlapSplit {
    /// undersized_threshold is the threshold for unnecessarily including & rewriting adjacent
    /// small files.  This does increase write amplification, so it shouldn't be too high, but
    /// it also prevents leaving tiny L1/L2 files that will never be compacted, so it shouldn't
    /// be too low.
    undersized_threshold: u64,
}

impl NonOverlapSplit {
    pub fn new(undersized_threshold: u64) -> Self {
        Self {
            undersized_threshold,
        }
    }
}

impl Display for NonOverlapSplit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Non-overlapping  split for TargetLevel version")
    }
}

impl FilesSplit for NonOverlapSplit {
    /// Return (`[compact_files]`, `[non_overlapping_files]`) of given files
    /// such that after combining all `compact_files` into a new file, the new file will
    /// have no overlap with any file in `non_overlapping_files`.
    /// The non_overlapping_files must be in the target_level
    ///
    /// Eligible non-overlapping files are files of the target level that do not
    /// overlap on time range of all files in lower-level. All files in target level
    /// are assumed to not overlap with each other and do not need to check
    ///
    /// Example:
    ///  . Input:
    ///                          |--L0.1--|             |--L0.2--|
    ///            |--L1.1--| |--L1.2--|    |--L1.3--|              |--L1.4--|
    ///
    ///    (L1.1, L1.3, L1.4) do not overlap with any L0s but only (L1.1, L1.4) are eligible non-overlapping files.
    ///    (L1.2, L1.3) must be compacted with L0s to produce the right non-overlapping L1s.
    ///
    ///  . Output:
    ///     . compact_files: [L0.1, L0.2, L1.2, L1.3]
    ///     . non_overlapping_files: [L1.1, L1.4]
    ///
    /// Algorithm:
    ///    The non-overlappings files are files from 2 ends of the target level files that are
    ///    completely ouside the time range of all lower level files
    ///
    ///   L0s                      |--L0.1--|             |--L0.2--|
    ///   ==> L0s' time range:     |-------L0's time range --------|
    ///
    ///   L1s      |--L1.1--| |--L1.2--|    |--L1.3--|              |--L1.4--|
    ///   ==> Only L1.1 and L1.4 are completely outside the time range of L0s.
    ///       So L1.1 and L1.4 are usually not included in compact_files.  However, if either of L1.1 or L1.4
    ///       are small (below undersized_threshold), they will be included in compact_files to avoid leaving
    ///       tiny L1 files behind.  Note that the application of undersized_threshold can only contiguously
    ///       expand the set of min_time sorted target level files.  So if there was a small L1.5 file, we
    ///       could not skip over a large L1.4 file to include L1.5 in compact_files.
    ///
    fn apply(
        &self,
        files: Vec<ParquetFile>,
        target_level: CompactionLevel,
        partition: TransitionPartitionId,
    ) -> (Vec<ParquetFile>, Vec<ParquetFile>) {
        assert_ne!(
            target_level,
            CompactionLevel::Initial,
            "unexpected compaction target_level, should not be L0, partition_id={}",
            partition
        );
        let num_files = files.len();

        // Split files into levels
        let prev_level = target_level.prev();
        let (mut target_level_files, prev_level_files) =
            split_by_level(files, target_level, prev_level, partition);

        // compute time range of prev_level_files
        let prev_level_range = if let Some(r) = FilesTimeRange::try_new(&prev_level_files) {
            r
        } else {
            // No prev_level_files, all target_level_files are non_overlapping_files
            return (vec![], target_level_files);
        };

        // Split target_level_files into 3 parts, those before, during and after prev_level_files.
        // Since target level files during the time range of prev_level_files must be compacted,
        // they are the start of compact_files.
        let mut before: Vec<ParquetFile>;
        let mut compact_files: Vec<ParquetFile>;
        let mut after: Vec<ParquetFile>;
        let mut non_overlapping_files = Vec::with_capacity(num_files);
        (before, compact_files, after) =
            three_range_split(&mut target_level_files, prev_level_range);

        // Closure that checks if a file is under the size threshold and adds it to compact_files
        let mut check_undersize_and_add = |file: ParquetFile| -> bool {
            let mut under = false;

            // Check if file overlaps with (min_time, max_time)
            if file.file_size_bytes <= self.undersized_threshold as i64 {
                under = true;
                compact_files.push(file);
            } else {
                non_overlapping_files.push(file);
            }

            under
        };

        // Contiguously add `before` files to the list to compact, so long as they're under the size threshold.
        before.sort_by_key(|f| f.min_time);
        let mut before = before.into_iter().collect::<VecDeque<_>>();
        while let Some(file) = before.pop_back() {
            if !check_undersize_and_add(file) {
                break;
            }
        }

        // Contiguously add `after` files to the list to compact, so long as they're under the size threshold.
        after.sort_by_key(|f| f.min_time);
        let mut after = after.into_iter().collect::<VecDeque<_>>();
        while let Some(file) = after.pop_front() {
            if !check_undersize_and_add(file) {
                break;
            }
        }

        compact_files.extend(prev_level_files);

        // Add remaining files to non_overlapping_files.
        non_overlapping_files.extend(before);
        non_overlapping_files.extend(after);

        (compact_files, non_overlapping_files)
    }
}

// three_range_split splits the files into 3 vectors: before, during, after the specified time range.
pub fn three_range_split(
    files: &mut Vec<ParquetFile>,
    range: FilesTimeRange,
) -> (Vec<ParquetFile>, Vec<ParquetFile>, Vec<ParquetFile>) {
    let num_files = files.len();
    let mut before = Vec::with_capacity(num_files);
    let mut during = Vec::with_capacity(num_files);
    let mut after = Vec::with_capacity(num_files);

    while let Some(file) = files.pop() {
        if range.contains(&file) {
            during.push(file);
        } else if range.before(&file) {
            before.push(file);
        } else {
            after.push(file);
        }
    }

    (before, during, after)
}

#[cfg(test)]
mod tests {

    use compactor_test_utils::{
        create_fake_partition_id, create_l1_files, create_overlapped_files,
        create_overlapped_files_2, create_overlapped_files_mix_sizes_1,
        create_overlapped_l0_l1_files, create_overlapped_l1_l2_files, format_files,
        format_files_split,
    };

    use super::*;

    #[test]
    fn test_display() {
        assert_eq!(
            NonOverlapSplit::new(1024 * 1024).to_string(),
            "Non-overlapping  split for TargetLevel version"
        );
    }

    #[test]
    #[should_panic]
    fn test_wrong_target_level() {
        let files = create_overlapped_files();
        let split = NonOverlapSplit::new(1024 * 1024);
        split.apply(files, CompactionLevel::Initial, create_fake_partition_id());
    }

    #[test]
    #[should_panic(
        expected = "unexpected compaction level for partition 0, expected CompactionLevel::L1 or CompactionLevel::L0 but got CompactionLevel::L2"
    )]
    fn test_unexpected_compaction_level_2() {
        let files = create_overlapped_files();
        let split = NonOverlapSplit::new(1024 * 1024);
        // There are L2 files and will panic
        split.apply(
            files,
            CompactionLevel::FileNonOverlapped,
            create_fake_partition_id(),
        );
    }

    #[test]
    #[should_panic(
        expected = "unexpected compaction level for partition 0, expected CompactionLevel::L2 or CompactionLevel::L1 but got CompactionLevel::L0"
    )]
    fn test_unexpected_compaction_level_0() {
        let files = create_overlapped_files();
        let split = NonOverlapSplit::new(1024 * 1024);
        // There are L0 files and will panic
        split.apply(files, CompactionLevel::Final, create_fake_partition_id());
    }

    #[test]
    fn test_apply_empty_files() {
        let files = vec![];
        let split = NonOverlapSplit::new(1024 * 1024);

        let (overlap, non_overlap) = split.apply(
            files,
            CompactionLevel::FileNonOverlapped,
            create_fake_partition_id(),
        );
        assert_eq!(overlap.len(), 0);
        assert_eq!(non_overlap.len(), 0);
    }

    #[test]
    fn test_apply_one_level_empty() {
        let files = create_l1_files(1);
        let fake_partition_id = create_fake_partition_id();

        insta::assert_yaml_snapshot!(
            format_files("initial", &files),
            @r###"
        ---
        - initial
        - "L1, all files 1b                                                                                                   "
        - "L1.13[600,700] 0ns                                                                             |------L1.13-------|"
        - "L1.12[400,500] 0ns                                     |------L1.12-------|                                        "
        - "L1.11[250,350] 0ns       |------L1.11-------|                                                                      "
        "###
        );

        let split = NonOverlapSplit::new(0);

        // Lower level is empty  -> all files will be in non_overlapping_files
        let (overlap, non_overlap) = split.apply(
            files.clone(),
            CompactionLevel::FileNonOverlapped,
            fake_partition_id.clone(),
        );
        assert_eq!(overlap.len(), 0);
        assert_eq!(non_overlap.len(), 3);

        // target level is empty -> all files will be in compact_files
        let (overlap, non_overlap) = split.apply(files, CompactionLevel::Final, fake_partition_id);
        assert_eq!(overlap.len(), 3);
        assert_eq!(non_overlap.len(), 0);
    }

    #[test]
    fn test_apply_mix_1() {
        let files = create_overlapped_l0_l1_files(1);
        insta::assert_yaml_snapshot!(
            format_files("initial", &files),
            @r###"
        ---
        - initial
        - "L0, all files 1b                                                                                                   "
        - "L0.2[650,750] 180s                                                              |---L0.2----|                      "
        - "L0.1[450,620] 120s                                  |--------L0.1---------|                                        "
        - "L0.3[800,900] 300s                                                                                   |---L0.3----| "
        - "L1, all files 1b                                                                                                   "
        - "L1.13[600,700] 60s                                                       |---L1.13---|                             "
        - "L1.12[400,500] 60s                           |---L1.12---|                                                         "
        - "L1.11[250,350] 60s       |---L1.11---|                                                                             "
        "###
        );

        let split = NonOverlapSplit::new(0);
        let (overlap, non_overlap) = split.apply(
            files,
            CompactionLevel::FileNonOverlapped,
            create_fake_partition_id(),
        );
        insta::assert_yaml_snapshot!(
            format_files_split("overlap", &overlap, "non_overlap", &non_overlap),
            @r###"
        ---
        - overlap
        - "L0, all files 1b                                                                                                   "
        - "L0.2[650,750] 180s                                                    |------L0.2------|                           "
        - "L0.1[450,620] 120s                |------------L0.1------------|                                                   "
        - "L0.3[800,900] 300s                                                                               |------L0.3------|"
        - "L1, all files 1b                                                                                                   "
        - "L1.12[400,500] 60s       |-----L1.12------|                                                                        "
        - "L1.13[600,700] 60s                                           |-----L1.13------|                                    "
        - non_overlap
        - "L1, all files 1b                                                                                                   "
        - "L1.11[250,350] 60s       |-----------------------------------------L1.11------------------------------------------|"
        "###
        );
    }

    //    |--L2.1--|  |--L2.2--|
    //                  |--L1.1--|  |--L1.2--|  |--L1.3--|
    //                                  |--L0.1--|   |--L0.2--| |--L0.3--|

    #[test]
    fn test_apply_mix_2() {
        let files = create_overlapped_l1_l2_files(1);
        insta::assert_yaml_snapshot!(
            format_files("initial", &files),
            @r###"
        ---
        - initial
        - "L1, all files 1b                                                                                                   "
        - "L1.13[600,700] 0ns                                                                                    |--L1.13---| "
        - "L1.12[400,500] 0ns                                                          |--L1.12---|                           "
        - "L1.11[250,350] 0ns                                       |--L1.11---|                                              "
        - "L2, all files 1b                                                                                                   "
        - "L2.21[0,100] 0ns         |--L2.21---|                                                                              "
        - "L2.22[200,300] 0ns                                |--L2.22---|                                                     "
        "###
        );

        let split = NonOverlapSplit::new(0);
        let (overlap, non_overlap) =
            split.apply(files, CompactionLevel::Final, create_fake_partition_id());
        insta::assert_yaml_snapshot!(
            format_files_split("overlap", &overlap, "non_overlap", &non_overlap),
            @r###"
        ---
        - overlap
        - "L1, all files 1b                                                                                                   "
        - "L1.13[600,700] 0ns                                                                               |-----L1.13------|"
        - "L1.12[400,500] 0ns                                           |-----L1.12------|                                    "
        - "L1.11[250,350] 0ns                |-----L1.11------|                                                               "
        - "L2, all files 1b                                                                                                   "
        - "L2.22[200,300] 0ns       |-----L2.22------|                                                                        "
        - non_overlap
        - "L2, all files 1b                                                                                                   "
        - "L2.21[0,100] 0ns         |-----------------------------------------L2.21------------------------------------------|"
        "###
        );
    }

    #[test]
    fn test_apply_mix_3() {
        // Create files with levels and time ranges
        //  . Input:
        //                          |--L0.1--|             |--L0.2--|
        //            |--L1.1--| |--L1.2--|    |--L1.3--|              |--L1.4--|
        //
        //  . Output: (overlap, non_overlap) = ( [L0.1, L0.2, L1.2, L1.3] , [L1.1, L1.4] )
        let files = create_overlapped_files_2(1);
        insta::assert_yaml_snapshot!(
            format_files("initial", &files),
            @r###"
        ---
        - initial
        - "L0, all files 1b                                                                                                   "
        - "L0.2[520,550] 0ns                                                                          |L0.2|                  "
        - "L0.1[250,350] 0ns                                        |---L0.1---|                                              "
        - "L1, all files 1b                                                                                                   "
        - "L1.13[400,500] 0ns                                                          |--L1.13---|                           "
        - "L1.12[200,300] 0ns                                |--L1.12---|                                                     "
        - "L1.11[0,100] 0ns         |--L1.11---|                                                                              "
        - "L1.14[600,700] 0ns                                                                                    |--L1.14---| "
        "###
        );

        let split = NonOverlapSplit::new(0);
        let (overlap, non_overlap) = split.apply(
            files,
            CompactionLevel::FileNonOverlapped,
            create_fake_partition_id(),
        );
        insta::assert_yaml_snapshot!(
            format_files_split("overlap", &overlap, "non_overlap", &non_overlap),
            @r###"
        ---
        - overlap
        - "L0, all files 1b                                                                                                   "
        - "L0.2[520,550] 0ns                                                                                          |L0.2-| "
        - "L0.1[250,350] 0ns                    |---------L0.1----------|                                                     "
        - "L1, all files 1b                                                                                                   "
        - "L1.12[200,300] 0ns       |---------L1.12---------|                                                                 "
        - "L1.13[400,500] 0ns                                                          |---------L1.13---------|              "
        - non_overlap
        - "L1, all files 1b                                                                                                   "
        - "L1.11[0,100] 0ns         |--L1.11---|                                                                              "
        - "L1.14[600,700] 0ns                                                                                    |--L1.14---| "
        "###
        );
    }

    #[test]
    fn test_undersized_1() {
        // Create files with levels and time ranges, where all L1 files are undersized.
        //    Input:
        //                          |--L0.1--|             |--L0.2--|
        //            |--L1.1--| |--L1.2--|    |--L1.3--|              |--L1.4--|
        //
        //    Output: (compact, non_overlap) = ( [L0.1, L0.2, L1.1, L1.2, L1.3, L1.4] , [] )
        //             Since all the files are below the undersized threshold, they all get compacted, even though some
        //             don't overlap.
        let files = create_overlapped_files_2(10);
        insta::assert_yaml_snapshot!(
            format_files("initial", &files),
            @r###"
        ---
        - initial
        - "L0, all files 10b                                                                                                  "
        - "L0.2[520,550] 0ns                                                                          |L0.2|                  "
        - "L0.1[250,350] 0ns                                        |---L0.1---|                                              "
        - "L1, all files 10b                                                                                                  "
        - "L1.13[400,500] 0ns                                                          |--L1.13---|                           "
        - "L1.12[200,300] 0ns                                |--L1.12---|                                                     "
        - "L1.11[0,100] 0ns         |--L1.11---|                                                                              "
        - "L1.14[600,700] 0ns                                                                                    |--L1.14---| "
        "###
        );

        let split = NonOverlapSplit::new(11);
        let (compact_files, non_overlap) = split.apply(
            files,
            CompactionLevel::FileNonOverlapped,
            create_fake_partition_id(),
        );
        insta::assert_yaml_snapshot!(
            format_files_split("compact_files", &compact_files, "non_overlap", &non_overlap),
            @r###"
        ---
        - compact_files
        - "L0, all files 10b                                                                                                  "
        - "L0.2[520,550] 0ns                                                                          |L0.2|                  "
        - "L0.1[250,350] 0ns                                        |---L0.1---|                                              "
        - "L1, all files 10b                                                                                                  "
        - "L1.12[200,300] 0ns                                |--L1.12---|                                                     "
        - "L1.13[400,500] 0ns                                                          |--L1.13---|                           "
        - "L1.11[0,100] 0ns         |--L1.11---|                                                                              "
        - "L1.14[600,700] 0ns                                                                                    |--L1.14---| "
        - non_overlap
        "###
        );
    }

    #[test]
    fn test_undersized_2() {
        // Create files with levels and time ranges, where some non-overlapping L1 files are undersized.
        //  . Input:
        //                                              |--L0.1--|    |-L0.2-|
        //            |--L1.1--| |--L1.2--| |--L1.3--| |--L1.4--|    |--L1.5--|              |--L1.6--| |--L1.7--|  |--L1.8--|
        // underized:    no          no         yes        x             x                      yes         no          no
        // Results:
        //   L1.4 and L1.5 are compacted regardless of their size because they overlap L0s.
        //   L1.3 and L1.6 are compacted because they are undersized and contiguous to the overlapping files.
        //   L1.1, L1.2, L1.7, and L1.8 are not compacted because they are not undersized.
        //
        let files = create_overlapped_files_mix_sizes_1(10, 20, 30);
        insta::assert_yaml_snapshot!(
            format_files("initial", &files),
            @r###"
        ---
        - initial
        - "L0                                                                                                                 "
        - "L0.2[820,850] 0ns 10b                                                     |L0.2|                                   "
        - "L0.1[650,750] 0ns 10b                                           |L0.1|                                             "
        - "L1                                                                                                                 "
        - "L1.13[400,500] 0ns 10b                           |L1.13|                                                           "
        - "L1.17[1200,1300] 0ns 30b                                                                         |L1.17|           "
        - "L1.12[200,300] 0ns 30b               |L1.12|                                                                       "
        - "L1.11[0,100] 0ns 20b     |L1.11|                                                                                   "
        - "L1.14[600,700] 0ns 20b                                       |L1.14|                                               "
        - "L1.15[800,900] 0ns 20b                                                   |L1.15|                                   "
        - "L1.16[1000,1100] 0ns 10b                                                            |L1.16|                        "
        - "L1.18[1400,1500] 0ns 20b                                                                                     |L1.18|"
        "###
        );

        let split = NonOverlapSplit::new(11);
        let (compact_files, non_overlap) = split.apply(
            files,
            CompactionLevel::FileNonOverlapped,
            create_fake_partition_id(),
        );
        insta::assert_yaml_snapshot!(
            format_files_split("compact_files", &compact_files, "non_overlap", &non_overlap),
            @r###"
        ---
        - compact_files
        - "L0                                                                                                                 "
        - "L0.2[820,850] 0ns 10b                                                          |L0.2|                              "
        - "L0.1[650,750] 0ns 10b                                    |---L0.1---|                                              "
        - "L1                                                                                                                 "
        - "L1.15[800,900] 0ns 20b                                                      |--L1.15---|                           "
        - "L1.14[600,700] 0ns 20b                            |--L1.14---|                                                     "
        - "L1.13[400,500] 0ns 10b   |--L1.13---|                                                                              "
        - "L1.16[1000,1100] 0ns 10b                                                                              |--L1.16---| "
        - non_overlap
        - "L1                                                                                                                 "
        - "L1.12[200,300] 0ns 30b               |L1.12|                                                                       "
        - "L1.17[1200,1300] 0ns 30b                                                                         |L1.17|           "
        - "L1.11[0,100] 0ns 20b     |L1.11|                                                                                   "
        - "L1.18[1400,1500] 0ns 20b                                                                                     |L1.18|"
        "###
        );
    }

    #[test]
    fn test_undersized_3() {
        // This case is like undersized 3, except that the outermost L1 files (L1 & L18) are also undersized.  But since
        // they're separated from the overlapping files by oversized files, they are not compacted.
        //    Input:
        //                                              |--L0.1--|    |-L0.2-|
        //            |--L1.1--| |--L1.2--| |--L1.3--| |--L1.4--|    |--L1.5--|              |--L1.6--| |--L1.7--|  |--L1.8--|
        // underized:    yes         no         yes        x             x                      yes         no          yes
        // Results:
        //   L1.4 and L1.5 are compacted regardless of their size because they overlap.
        //   L1.3 and L1.6 are compacted because they are undersized and contiguous to the overlapping files.
        //   L1.2 and L1.7 are not compacted because they are not undersized.
        //   L1.1 and L1.8 are not compacted because even though they are undersized, they are not contiguous to the
        //   overlapping files.
        //
        let files = create_overlapped_files_mix_sizes_1(10, 20, 30);
        insta::assert_yaml_snapshot!(
            format_files("initial", &files),
            @r###"
        ---
        - initial
        - "L0                                                                                                                 "
        - "L0.2[820,850] 0ns 10b                                                     |L0.2|                                   "
        - "L0.1[650,750] 0ns 10b                                           |L0.1|                                             "
        - "L1                                                                                                                 "
        - "L1.13[400,500] 0ns 10b                           |L1.13|                                                           "
        - "L1.17[1200,1300] 0ns 30b                                                                         |L1.17|           "
        - "L1.12[200,300] 0ns 30b               |L1.12|                                                                       "
        - "L1.11[0,100] 0ns 20b     |L1.11|                                                                                   "
        - "L1.14[600,700] 0ns 20b                                       |L1.14|                                               "
        - "L1.15[800,900] 0ns 20b                                                   |L1.15|                                   "
        - "L1.16[1000,1100] 0ns 10b                                                            |L1.16|                        "
        - "L1.18[1400,1500] 0ns 20b                                                                                     |L1.18|"
        "###
        );

        let split = NonOverlapSplit::new(21);
        let (compact_files, non_overlap) = split.apply(
            files,
            CompactionLevel::FileNonOverlapped,
            create_fake_partition_id(),
        );
        insta::assert_yaml_snapshot!(
            format_files_split("compact_files", &compact_files, "non_overlap", &non_overlap),
            @r###"
        ---
        - compact_files
        - "L0                                                                                                                 "
        - "L0.2[820,850] 0ns 10b                                                          |L0.2|                              "
        - "L0.1[650,750] 0ns 10b                                    |---L0.1---|                                              "
        - "L1                                                                                                                 "
        - "L1.15[800,900] 0ns 20b                                                      |--L1.15---|                           "
        - "L1.14[600,700] 0ns 20b                            |--L1.14---|                                                     "
        - "L1.13[400,500] 0ns 10b   |--L1.13---|                                                                              "
        - "L1.16[1000,1100] 0ns 10b                                                                              |--L1.16---| "
        - non_overlap
        - "L1                                                                                                                 "
        - "L1.12[200,300] 0ns 30b               |L1.12|                                                                       "
        - "L1.17[1200,1300] 0ns 30b                                                                         |L1.17|           "
        - "L1.11[0,100] 0ns 20b     |L1.11|                                                                                   "
        - "L1.18[1400,1500] 0ns 20b                                                                                     |L1.18|"
        "###
        );
    }
}
