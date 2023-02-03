use std::{
    cmp::{max, min},
    collections::VecDeque,
    fmt::Display,
};

use data_types::{CompactionLevel, ParquetFile};

use super::FilesSplit;

#[derive(Debug)]

/// Split files into `[overlapping_files]` and `[non_overlapping_files]`
/// To have better and efficient compaction performance, eligible non-overlapped files
/// should not be compacted.
pub struct TargetLevelNonOverlapSplit {}

impl TargetLevelNonOverlapSplit {
    pub fn new() -> Self {
        Self {}
    }
}

impl Display for TargetLevelNonOverlapSplit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Non-overlapping  split for TargetLevel version")
    }
}

impl FilesSplit for TargetLevelNonOverlapSplit {
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

        let mut non_overlapping_files = Vec::with_capacity(files.len());
        let mut overlapping_files = Vec::with_capacity(files.len());

        // Split files into levels
        let mut target_level_files = Vec::with_capacity(files.len());
        let mut prev_level_files = Vec::with_capacity(files.len());
        let prev_level = target_level.prev();
        for file in files {
            if file.compaction_level == target_level {
                target_level_files.push(file);
            } else if file.compaction_level == prev_level {
                prev_level_files.push(file);
            } else {
                panic!("Unexpected compaction level: {}", file.compaction_level);
            }
        }

        if prev_level_files.is_empty() {
            // No prev_level_files, all target_level_files are non_overlapping_files
            return (vec![], target_level_files);
        }

        // compute time range of prev_level_files
        // split prev_level_files into one and the rest
        let prev_level_files_tail = prev_level_files.split_off(1);
        let mut min_time = prev_level_files[0].min_time;
        let mut max_time = prev_level_files[0].max_time;
        for file in &prev_level_files_tail {
            min_time = min(min_time, file.min_time);
            max_time = max(max_time, file.max_time);
        }

        // Sort files of target level by min_time
        target_level_files.sort_by_key(|f| f.min_time);

        // Convert target_level_files to VecDeque
        let mut target_level_files = target_level_files.into_iter().collect::<VecDeque<_>>();

        // Closure that checks if a file overlaps with any prev_level_files and add it
        // to overlapping_files or non_overlapping_files accordingly. Return true if ovelapping
        let mut check_overlapping_and_add = |file: ParquetFile| -> bool {
            let mut overlapping = false;

            // Check if file overlaps with (min_time, max_time)
            if file.min_time <= max_time && file.max_time >= min_time {
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
        overlapping_files.extend(prev_level_files_tail);

        (overlapping_files, non_overlapping_files)
    }
}

#[cfg(test)]
mod tests {

    use crate::test_util::{
        create_l1_files, create_overlapped_files, create_overlapped_files_2,
        create_overlapped_l0_l1_files, create_overlapped_l1_l2_files,
    };

    use super::*;

    #[test]
    fn test_display() {
        assert_eq!(
            TargetLevelNonOverlapSplit::new().to_string(),
            "Non-overlapping  split for TargetLevel version"
        );
    }

    #[test]
    #[should_panic]
    fn test_wrong_target_level() {
        let files = create_overlapped_files();
        let split = TargetLevelNonOverlapSplit::new();
        split.apply(files, CompactionLevel::Initial);
    }

    #[test]
    #[should_panic(expected = "Unexpected compaction level: CompactionLevel::L2")]
    fn test_unexpected_compaction_level_2() {
        let files = create_overlapped_files();
        let split = TargetLevelNonOverlapSplit::new();
        // There are L2 files and will panic
        split.apply(files, CompactionLevel::FileNonOverlapped);
    }

    #[test]
    #[should_panic(expected = "Unexpected compaction level: CompactionLevel::L0")]
    fn test_unexpected_compaction_level_0() {
        let files = create_overlapped_files();
        let split = TargetLevelNonOverlapSplit::new();
        // There are L0 files and will panic
        split.apply(files, CompactionLevel::Final);
    }

    #[test]
    fn test_apply_empty_files() {
        let files = vec![];
        let split = TargetLevelNonOverlapSplit::new();

        let (overlap, non_overlap) = split.apply(files, CompactionLevel::FileNonOverlapped);
        assert_eq!(overlap.len(), 0);
        assert_eq!(non_overlap.len(), 0);
    }

    #[test]
    fn test_apply_one_level_empty() {
        let files = create_l1_files();
        assert_eq!(files.len(), 3);

        let split = TargetLevelNonOverlapSplit::new();

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
        let files = create_overlapped_l0_l1_files();
        assert_eq!(files.len(), 6);

        // Input files:
        //                  |--L1.1--|  |--L1.2--|  |--L1.3--|
        //                                  |--L0.1--|   |--L0.2--| |--L0.3--|
        // Output files: (overlap, non_overlap) = ( [L0.1, L0.2, L0.3, L1.2, L1.3] , L1.1] )

        let split = TargetLevelNonOverlapSplit::new();
        let (overlap, non_overlap) = split.apply(files, CompactionLevel::FileNonOverlapped);
        assert_eq!(overlap.len(), 5);
        assert_eq!(non_overlap.len(), 1);

        // Verify overlapping files
        // sort by id
        let mut overlap = overlap;
        overlap.sort_by(|a, b| a.id.cmp(&b.id));
        assert_eq!(overlap[0].id.get(), 1);
        assert_eq!(overlap[1].id.get(), 2);
        assert_eq!(overlap[2].id.get(), 3);
        assert_eq!(overlap[3].id.get(), 12);
        assert_eq!(overlap[4].id.get(), 13);
        // verify non-overlapping files
        assert_eq!(non_overlap[0].id.get(), 11);
    }

    //    |--L2.1--|  |--L2.2--|
    //                  |--L1.1--|  |--L1.2--|  |--L1.3--|
    //                                  |--L0.1--|   |--L0.2--| |--L0.3--|

    #[test]
    fn test_apply_mix_2() {
        let files = create_overlapped_l1_l2_files();
        assert_eq!(files.len(), 5);

        // Input files:
        //    |--L2.1--|  |--L2.2--|
        //                  |--L1.1--|  |--L1.2--|  |--L1.3--|
        // Output files: (overlap, non_overlap) = ( [L1.1, L1.2, L1.3, L2.2] , L2.1] )

        let split = TargetLevelNonOverlapSplit::new();
        let (overlap, non_overlap) = split.apply(files, CompactionLevel::Final);
        assert_eq!(overlap.len(), 4);
        assert_eq!(non_overlap.len(), 1);

        // Verify overlapping files
        // sort by id
        let mut overlap = overlap;
        overlap.sort_by(|a, b| a.id.cmp(&b.id));
        assert_eq!(overlap[0].id.get(), 11);
        assert_eq!(overlap[1].id.get(), 12);
        assert_eq!(overlap[2].id.get(), 13);
        assert_eq!(overlap[3].id.get(), 22);
        // verify non-overlapping files
        assert_eq!(non_overlap[0].id.get(), 21);
    }

    #[test]
    fn test_apply_mix_3() {
        // Create files with levels and time ranges
        //  . Input:
        //                          |--L0.1--|             |--L0.2--|
        //            |--L1.1--| |--L1.2--|    |--L1.3--|              |--L1.4--|
        //
        //  . Output: (overlap, non_overlap) = ( [L0.1, L0.2, L1.2, L1.3] , [L1.1, L1.4] )
        let files = create_overlapped_files_2();
        assert_eq!(files.len(), 6);

        let split = TargetLevelNonOverlapSplit::new();
        let (overlap, non_overlap) = split.apply(files, CompactionLevel::FileNonOverlapped);
        assert_eq!(overlap.len(), 4);
        assert_eq!(non_overlap.len(), 2);

        // Verify overlapping files
        // sort by id
        let mut overlap = overlap;
        overlap.sort_by(|a, b| a.id.cmp(&b.id));
        assert_eq!(overlap[0].id.get(), 1);
        assert_eq!(overlap[1].id.get(), 2);
        assert_eq!(overlap[2].id.get(), 12);
        assert_eq!(overlap[3].id.get(), 13);
        // verify non-overlapping files
        // sort by id
        let mut non_overlap = non_overlap;
        non_overlap.sort_by(|a, b| a.id.cmp(&b.id));
        assert_eq!(non_overlap[0].id.get(), 11);
        assert_eq!(non_overlap[1].id.get(), 14);
    }
}
