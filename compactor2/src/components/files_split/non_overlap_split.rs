use std::{collections::VecDeque, fmt::Display};

use data_types::{CompactionLevel, ParquetFile};

use crate::file_group::{split_by_level, FilesTimeRange};

use super::FilesSplit;

#[derive(Debug)]

/// Split files into `[overlapping_files]` and `[non_overlapping_files]`
/// To have better and efficient compaction performance, eligible non-overlapped files
/// should not be compacted.
pub struct NonOverlapSplit {}

impl NonOverlapSplit {
    pub fn new() -> Self {
        Self {}
    }
}

impl Display for NonOverlapSplit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Non-overlapping  split for TargetLevel version")
    }
}

impl FilesSplit for NonOverlapSplit {
    /// Return (`[overlapping_files]`, `[non_overlapping_files]`) of given files
    /// such that after combining all `overlapping_files` into a new file, the new file will
    /// have no overlap with any file in `non_overlapping_files`.
    /// The non_overlapping_files must in the target_level
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
    ///     . overlapping_files: [L0.1, L0.2, L1.2, L1.3]
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
    ///   ==> Only L1.1 and L1.4 are completely outside the time range of L0s
    ///
    fn apply(
        &self,
        files: Vec<data_types::ParquetFile>,
        target_level: CompactionLevel,
    ) -> (Vec<ParquetFile>, Vec<ParquetFile>) {
        // Panic if given wrong target level, L0
        assert_ne!(target_level, CompactionLevel::Initial);
        let num_files = files.len();

        // Split files into levels
        let prev_level = target_level.prev();
        let (mut target_level_files, prev_level_files) =
            split_by_level(files, target_level, prev_level);

        // compute time range of prev_level_files
        let prev_level_range = if let Some(r) = FilesTimeRange::try_new(&prev_level_files) {
            r
        } else {
            // No prev_level_files, all target_level_files are non_overlapping_files
            return (vec![], target_level_files);
        };

        // Sort files of target level by min_time
        target_level_files.sort_by_key(|f| f.min_time);

        // Convert target_level_files to VecDeque
        let mut target_level_files = target_level_files.into_iter().collect::<VecDeque<_>>();

        // Closure that checks if a file overlaps with any prev_level_files and add it
        // to overlapping_files or non_overlapping_files accordingly. Return true if ovelapping
        let mut non_overlapping_files = Vec::with_capacity(num_files);
        let mut overlapping_files = Vec::with_capacity(num_files);
        let mut check_overlapping_and_add = |file: ParquetFile| -> bool {
            let mut overlapping = false;

            // Check if file overlaps with (min_time, max_time)
            if prev_level_range.contains(&file) {
                overlapping = true;
                overlapping_files.push(file);
            } else {
                non_overlapping_files.push(file);
            }

            overlapping
        };

        // Find non-overlapping files which are only at either end of their min-time sorted target_level_files
        // pop_front() until hitting the first overlapping file
        while let Some(file) = target_level_files.pop_front() {
            if check_overlapping_and_add(file) {
                break;
            }
        }
        // pop_back() until hitting the first overlapping file
        while let Some(file) = target_level_files.pop_back() {
            if check_overlapping_and_add(file) {
                break;
            }
        }

        // Add remaining files to overlapping_files
        overlapping_files.extend(target_level_files);
        overlapping_files.extend(prev_level_files);

        (overlapping_files, non_overlapping_files)
    }
}

#[cfg(test)]
mod tests {

    use compactor2_test_utils::{
        create_l1_files, create_overlapped_files, create_overlapped_files_2,
        create_overlapped_l0_l1_files, create_overlapped_l1_l2_files, format_files,
        format_files_split,
    };

    use super::*;

    #[test]
    fn test_display() {
        assert_eq!(
            NonOverlapSplit::new().to_string(),
            "Non-overlapping  split for TargetLevel version"
        );
    }

    #[test]
    #[should_panic]
    fn test_wrong_target_level() {
        let files = create_overlapped_files();
        let split = NonOverlapSplit::new();
        split.apply(files, CompactionLevel::Initial);
    }

    #[test]
    #[should_panic(
        expected = "Unexpected compaction level. Expected CompactionLevel::L1 or CompactionLevel::L0 but got CompactionLevel::L2."
    )]
    fn test_unexpected_compaction_level_2() {
        let files = create_overlapped_files();
        let split = NonOverlapSplit::new();
        // There are L2 files and will panic
        split.apply(files, CompactionLevel::FileNonOverlapped);
    }

    #[test]
    #[should_panic(
        expected = "Unexpected compaction level. Expected CompactionLevel::L2 or CompactionLevel::L1 but got CompactionLevel::L0."
    )]
    fn test_unexpected_compaction_level_0() {
        let files = create_overlapped_files();
        let split = NonOverlapSplit::new();
        // There are L0 files and will panic
        split.apply(files, CompactionLevel::Final);
    }

    #[test]
    fn test_apply_empty_files() {
        let files = vec![];
        let split = NonOverlapSplit::new();

        let (overlap, non_overlap) = split.apply(files, CompactionLevel::FileNonOverlapped);
        assert_eq!(overlap.len(), 0);
        assert_eq!(non_overlap.len(), 0);
    }

    #[test]
    fn test_apply_one_level_empty() {
        let files = create_l1_files(1);
        insta::assert_yaml_snapshot!(
            format_files("initial", &files),
            @r###"
        ---
        - initial
        - "L1, all files 1b                                                                                    "
        - "L1.13[600,700]                                                                    |-----L1.13-----| "
        - "L1.12[400,500]                                |-----L1.12-----|                                     "
        - "L1.11[250,350]      |-----L1.11-----|                                                               "
        "###
        );

        let split = NonOverlapSplit::new();

        // Lower level is empty  -> all files will be in non_overlapping_files
        let (overlap, non_overlap) = split.apply(files.clone(), CompactionLevel::FileNonOverlapped);
        assert_eq!(overlap.len(), 0);
        assert_eq!(non_overlap.len(), 3);

        // target level is empty -> all files will be in overlapping_files
        let (overlap, non_overlap) = split.apply(files, CompactionLevel::Final);
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
        - "L0, all files 1b                                                                                    "
        - "L0.2[650,750]                                                        |---L0.2---|                   "
        - "L0.1[450,620]                               |-------L0.1-------|                                    "
        - "L0.3[800,900]                                                                          |---L0.3---| "
        - "L1, all files 1b                                                                                    "
        - "L1.13[600,700]                                                 |--L1.13---|                         "
        - "L1.12[400,500]                        |--L1.12---|                                                  "
        - "L1.11[250,350]      |--L1.11---|                                                                    "
        "###
        );

        let split = NonOverlapSplit::new();
        let (overlap, non_overlap) = split.apply(files, CompactionLevel::FileNonOverlapped);
        insta::assert_yaml_snapshot!(
            format_files_split("overlap", &overlap, "non_overlap", &non_overlap),
            @r###"
        ---
        - overlap
        - "L0, all files 1b                                                                                    "
        - "L0.2[650,750]                                               |-----L0.2-----|                        "
        - "L0.1[450,620]               |----------L0.1-----------|                                             "
        - "L0.3[800,900]                                                                       |-----L0.3-----|"
        - "L1, all files 1b                                                                                    "
        - "L1.12[400,500]      |----L1.12-----|                                                                "
        - "L1.13[600,700]                                      |----L1.13-----|                                "
        - non_overlap
        - "L1, all files 1b                                                                                    "
        - "L1.11[250,350]      |------------------------------------L1.11-------------------------------------|"
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
        - "L1, all files 1b                                                                                    "
        - "L1.13[600,700]                                                                          |--L1.13--| "
        - "L1.12[400,500]                                                   |--L1.12--|                        "
        - "L1.11[250,350]                                  |--L1.11--|                                         "
        - "L2, all files 1b                                                                                    "
        - "L2.21[0,100]        |--L2.21--|                                                                     "
        - "L2.22[200,300]                            |--L2.22--|                                               "
        "###
        );

        let split = NonOverlapSplit::new();
        let (overlap, non_overlap) = split.apply(files, CompactionLevel::Final);
        insta::assert_yaml_snapshot!(
            format_files_split("overlap", &overlap, "non_overlap", &non_overlap),
            @r###"
        ---
        - overlap
        - "L1, all files 1b                                                                                    "
        - "L1.13[600,700]                                                                      |----L1.13-----|"
        - "L1.12[400,500]                                      |----L1.12-----|                                "
        - "L1.11[250,350]              |----L1.11-----|                                                        "
        - "L2, all files 1b                                                                                    "
        - "L2.22[200,300]      |----L2.22-----|                                                                "
        - non_overlap
        - "L2, all files 1b                                                                                    "
        - "L2.21[0,100]        |------------------------------------L2.21-------------------------------------|"
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
        - "L0, all files 1b                                                                                    "
        - "L0.2[520,550]                                                                  |L0.2|               "
        - "L0.1[250,350]                                   |--L0.1---|                                         "
        - "L1, all files 1b                                                                                    "
        - "L1.13[400,500]                                                   |--L1.13--|                        "
        - "L1.12[200,300]                            |--L1.12--|                                               "
        - "L1.11[0,100]        |--L1.11--|                                                                     "
        - "L1.14[600,700]                                                                          |--L1.14--| "
        "###
        );

        let split = NonOverlapSplit::new();
        let (overlap, non_overlap) = split.apply(files, CompactionLevel::FileNonOverlapped);
        insta::assert_yaml_snapshot!(
            format_files_split("overlap", &overlap, "non_overlap", &non_overlap),
            @r###"
        ---
        - overlap
        - "L0, all files 1b                                                                                    "
        - "L0.2[520,550]                                                                                |L0.2| "
        - "L0.1[250,350]                  |--------L0.1--------|                                               "
        - "L1, all files 1b                                                                                    "
        - "L1.12[200,300]      |-------L1.12--------|                                                          "
        - "L1.13[400,500]                                                   |-------L1.13--------|             "
        - non_overlap
        - "L1, all files 1b                                                                                    "
        - "L1.11[0,100]        |--L1.11--|                                                                     "
        - "L1.14[600,700]                                                                          |--L1.14--| "
        "###
        );
    }
}
