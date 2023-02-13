use std::fmt::Display;

use data_types::{CompactionLevel, ParquetFile};

use super::FilesSplit;
use crate::file_group::{overlaps_in_time, split_by_level, FilesTimeRange};

#[derive(Debug)]
/// Split files into `[files_to_compact]` and `[files_to_upgrade]`
/// To have better and efficient compaction performance, eligible upgradable files
/// should not be compacted but only need to update its compaction_level to the target_level
pub struct TargetLevelUpgradeSplit {
    // Maximum desired file size (try and avoid compacting files above this size)
    max_desired_file_size_bytes: u64,
}

impl TargetLevelUpgradeSplit {
    pub fn new(size: u64) -> Self {
        Self {
            max_desired_file_size_bytes: size,
        }
    }
}

impl Display for TargetLevelUpgradeSplit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Upgrade split for TargetLevel version - Size: {}",
            self.max_desired_file_size_bytes
        )
    }
}

impl FilesSplit for TargetLevelUpgradeSplit {
    /// Return (`[files_to_compact]`, `[files_to_upgrade]`) of the given files
    /// so that `files_to_upgrade` does not overlap with any files in previous level
    ///
    /// The files_to_upgrade must in the (target_level - 1)
    ///
    /// Eligible upgradable files are large-enough-file (>= max_desired_file_size) files of the previous level of
    /// the target level that do not overlap on time range with any files in its level and higher-level files.
    /// Note: we always have to stick the the invariance that the outout files must not overlap
    ///
    /// Example:
    ///             |--L0.1--| |--L0.2--| |--L0.3--|
    ///                                              |--L0.4--|     |--L0.5--| |--L0.6--|
    ///                        |--L1.1--|              |--L1.2--|
    ///
    /// . There are 4 L0 files that do not overlap with any L1s and L0s: [L0.1, L0.3, L0.5, L0.6]. However,
    ///   file L0.3 is in the middle of L0.2 and L0.4 and is not eligible.
    /// . Even if L0.5 is large enough, it in the midle of L0.6 and the rest. L0.5 is only
    ///    eligible to upgrade if L0.6 is eligible (large enough), too.
    ///
    /// Algorithm:
    ///    The non-overlappings files are files of the (target_level -1) files that:
    ///      1. Size >= max_desire_file_size
    ///      2. Completely outside the time range of all higher level files
    ///      3. Not overlap with any files in the same level
    ///      4. Not overlap with the time range of the files not meet 3 conditions above
    ///         This is the case that L0.5 is large but L0.6 is small
    ///
    fn apply(
        &self,
        files: Vec<data_types::ParquetFile>,
        target_level: CompactionLevel,
    ) -> (Vec<ParquetFile>, Vec<ParquetFile>) {
        // Panic if given wrong target level, L0
        assert_ne!(target_level, CompactionLevel::Initial);

        let mut files_to_upgrade = Vec::with_capacity(files.len());
        let mut files_to_compact = Vec::with_capacity(files.len());

        // Split files into levels
        let prev_level = target_level.prev();
        let (target_level_files, mut prev_level_files) =
            split_by_level(files, target_level, prev_level);

        // compute time range of target_level_files, if any
        let target_time_range = FilesTimeRange::try_new(&target_level_files);

        // Go go over all files of previous level and check if they are NOT eligible to upgrade
        // by hit one of this conditions
        //  . Size < max_desire_file_size
        //  . Overlap with time range of target_level_files
        //  . Overlap with any files in the same level
        // Otherwise, they are large and not overlap. Put them in the potential upgradable list
        // to check if they are actually upgradable or not. If they are in the middle of the
        // non-eligible files above, they are not upgradable.
        let mut potential_upgradable_files = Vec::with_capacity(prev_level_files.len());
        while let Some(file) = prev_level_files.pop() {
            // size is small
            if file.file_size_bytes < self.max_desired_file_size_bytes as i64 {
                files_to_compact.push(file);
            } else if let Some(target_time_range) = target_time_range {
                // overlap with target_level_files
                if target_time_range.contains(&file) ||
                // overlap with files in the same level
                    overlaps_in_time(&file, &prev_level_files) ||
                    overlaps_in_time(&file, &files_to_compact)
                {
                    files_to_compact.push(file);
                } else {
                    potential_upgradable_files.push(file);
                }
            } else if prev_level_files.iter().any(|f| f.overlaps(&file))
                || files_to_compact.iter().any(|f| f.overlaps(&file))
            {
                // overlap with files in the same level
                files_to_compact.push(file);
            } else {
                potential_upgradable_files.push(file);
            }
        }

        // Add target_level_files to files_to_compact
        files_to_compact.extend(target_level_files);

        // Compute time range of files_to_compact again to check if the potential upgradable files
        let to_compact_time_range = FilesTimeRange::try_new(&files_to_compact);

        // Go over all potential upgradable files and check if they are actually upgradable
        //  by not overlapping with the min_max_range of files_to_compact
        while let Some(file) = potential_upgradable_files.pop() {
            if let Some(to_compact_time_range) = to_compact_time_range {
                if !to_compact_time_range.contains(&file) {
                    files_to_upgrade.push(file);
                } else {
                    files_to_compact.push(file);
                }
            } else {
                files_to_upgrade.push(file);
            }
        }

        (files_to_compact, files_to_upgrade)
    }
}

#[cfg(test)]
mod tests {

    use compactor2_test_utils::{
        create_l0_files, create_l1_files, create_l1_files_mix_size, create_overlapped_files,
        create_overlapped_files_2, create_overlapped_files_3, create_overlapped_files_3_mix_size,
        create_overlapped_l0_l1_files, create_overlapped_l1_l2_files,
        create_overlapped_l1_l2_files_mix_size, create_overlapped_l1_l2_files_mix_size_2,
        create_overlapping_l0_files, format_files, format_files_split,
    };

    use super::*;
    use data_types::CompactionLevel;

    const MAX_SIZE: u64 = 100;

    #[test]
    fn test_display() {
        assert_eq!(
            TargetLevelUpgradeSplit::new(MAX_SIZE).to_string(),
            "Upgrade split for TargetLevel version - Size: 100"
        );
    }

    #[test]
    #[should_panic]
    fn test_wrong_target_level() {
        let split = TargetLevelUpgradeSplit::new(MAX_SIZE);
        let (_files_to_compact, _files_to_upgrade) = split.apply(vec![], CompactionLevel::Initial);
    }

    #[test]
    #[should_panic(
        expected = "Unexpected compaction level. Expected CompactionLevel::L1 or CompactionLevel::L0 but got CompactionLevel::L2."
    )]
    fn test_unexpected_compaction_level_2() {
        let files = create_overlapped_files();
        let split = TargetLevelUpgradeSplit::new(MAX_SIZE);
        // There are L2 files and will panic
        split.apply(files, CompactionLevel::FileNonOverlapped);
    }

    #[test]
    #[should_panic(
        expected = "Unexpected compaction level. Expected CompactionLevel::L2 or CompactionLevel::L1 but got CompactionLevel::L0."
    )]
    fn test_unexpected_compaction_level_0() {
        let files = create_overlapped_files();
        let split = TargetLevelUpgradeSplit::new(MAX_SIZE);
        // There are L0 files and will panic
        split.apply(files, CompactionLevel::Final);
    }

    #[test]
    fn test_apply_empty_files() {
        let split = TargetLevelUpgradeSplit::new(MAX_SIZE);
        let (files_to_compact, files_to_upgrade) =
            split.apply(vec![], CompactionLevel::FileNonOverlapped);
        assert_eq!((files_to_compact, files_to_upgrade), (vec![], vec![]));

        let (files_to_compact, files_to_upgrade) = split.apply(vec![], CompactionLevel::Final);
        assert_eq!((files_to_compact, files_to_upgrade), (vec![], vec![]));
    }

    #[test]
    fn test_apply_one_level_overlap_small_l0() {
        let files = create_overlapping_l0_files((MAX_SIZE - 1) as i64);
        insta::assert_yaml_snapshot!(
            format_files("initial", &files),
            @r###"
        ---
        - initial
        - "L0, all files 99b                                                                                   "
        - "L0.2[150,180]            |L0.2|                                                                     "
        - "L0.1[100,200]       |--L0.1--|                                                                      "
        - "L0.3[800,900]                                                                             |--L0.3--|"
        "###
        );

        let split = TargetLevelUpgradeSplit::new(MAX_SIZE);
        let (files_to_compact, files_to_upgrade) =
            split.apply(files, CompactionLevel::FileNonOverlapped);

        // All files are small --> nothing to upgrade
        insta::assert_yaml_snapshot!(
            format_files_split("files_to_compact", &files_to_compact, "files_to_upgrade", &files_to_upgrade),
            @r###"
        ---
        - files_to_compact
        - "L0, all files 99b                                                                                   "
        - "L0.3[800,900]                                                                             |--L0.3--|"
        - "L0.1[100,200]       |--L0.1--|                                                                      "
        - "L0.2[150,180]            |L0.2|                                                                     "
        - files_to_upgrade
        "###
        );
    }

    #[test]
    fn test_apply_one_level_overlap_large_l0() {
        let files = create_overlapping_l0_files((MAX_SIZE + 1) as i64);
        insta::assert_yaml_snapshot!(
            format_files("initial", &files),
            @r###"
        ---
        - initial
        - "L0, all files 101b                                                                                  "
        - "L0.2[150,180]            |L0.2|                                                                     "
        - "L0.1[100,200]       |--L0.1--|                                                                      "
        - "L0.3[800,900]                                                                             |--L0.3--|"
        "###
        );

        let split = TargetLevelUpgradeSplit::new(MAX_SIZE);
        let (files_to_compact, files_to_upgrade) =
            split.apply(files, CompactionLevel::FileNonOverlapped);

        // All files are large but only one eligible for upgrade
        // files_to_compact = [L0.1, L0.2]
        // files_to_upgrade = [L0.3]
        insta::assert_yaml_snapshot!(
            format_files_split("files_to_compact", &files_to_compact, "files_to_upgrade", &files_to_upgrade),
            @r###"
        ---
        - files_to_compact
        - "L0, all files 101b                                                                                  "
        - "L0.1[100,200]       |-------------------------------------L0.1-------------------------------------|"
        - "L0.2[150,180]                                               |---------L0.2---------|                "
        - files_to_upgrade
        - "L0, all files 101b                                                                                  "
        - "L0.3[800,900]       |-------------------------------------L0.3-------------------------------------|"
        "###
        );
    }

    #[test]
    fn test_apply_one_level_small_l0() {
        let files = create_l0_files((MAX_SIZE - 1) as i64);
        insta::assert_yaml_snapshot!(
            format_files("initial", &files),
            @r###"
        ---
        - initial
        - "L0, all files 99b                                                                                   "
        - "L0.2[650,750]                                          |-----L0.2------|                            "
        - "L0.1[450,620]       |------------L0.1------------|                                                  "
        - "L0.3[800,900]                                                                     |-----L0.3------| "
        "###
        );
        let split = TargetLevelUpgradeSplit::new(MAX_SIZE);
        let (files_to_compact, files_to_upgrade) =
            split.apply(files, CompactionLevel::FileNonOverlapped);

        // All files are small --> nothing to upgrade
        insta::assert_yaml_snapshot!(
            format_files_split("files_to_compact", &files_to_compact, "files_to_upgrade", &files_to_upgrade),
            @r###"
        ---
        - files_to_compact
        - "L0, all files 99b                                                                                   "
        - "L0.3[800,900]                                                                     |-----L0.3------| "
        - "L0.1[450,620]       |------------L0.1------------|                                                  "
        - "L0.2[650,750]                                          |-----L0.2------|                            "
        - files_to_upgrade
        "###
        );
    }

    #[test]
    fn test_apply_one_level_large_l0() {
        let files = create_l0_files((MAX_SIZE + 1) as i64);
        insta::assert_yaml_snapshot!(
            format_files("initial", &files),
            @r###"
        ---
        - initial
        - "L0, all files 101b                                                                                  "
        - "L0.2[650,750]                                          |-----L0.2------|                            "
        - "L0.1[450,620]       |------------L0.1------------|                                                  "
        - "L0.3[800,900]                                                                     |-----L0.3------| "
        "###
        );
        let split = TargetLevelUpgradeSplit::new(MAX_SIZE);
        let (files_to_compact, files_to_upgrade) =
            split.apply(files, CompactionLevel::FileNonOverlapped);

        // All files are large and eligible for upgrade
        insta::assert_yaml_snapshot!(
            format_files_split("files_to_compact", &files_to_compact, "files_to_upgrade", &files_to_upgrade),
            @r###"
        ---
        - files_to_compact
        - files_to_upgrade
        - "L0, all files 101b                                                                                  "
        - "L0.2[650,750]                                          |-----L0.2------|                            "
        - "L0.1[450,620]       |------------L0.1------------|                                                  "
        - "L0.3[800,900]                                                                     |-----L0.3------| "
        "###
        );
    }

    #[test]
    fn test_apply_one_level_small_l1() {
        let files = create_l1_files((MAX_SIZE - 1) as i64);
        insta::assert_yaml_snapshot!(
            format_files("initial", &files),
            @r###"
        ---
        - initial
        - "L1, all files 99b                                                                                   "
        - "L1.13[600,700]                                                                    |-----L1.13-----| "
        - "L1.12[400,500]                                |-----L1.12-----|                                     "
        - "L1.11[250,350]      |-----L1.11-----|                                                               "
        "###
        );
        let split = TargetLevelUpgradeSplit::new(MAX_SIZE);
        let (files_to_compact, files_to_upgrade) =
            split.apply(files, CompactionLevel::FileNonOverlapped);

        // All files are small --> nothing to upgrade
        insta::assert_yaml_snapshot!(
            format_files_split("files_to_compact", &files_to_compact, "files_to_upgrade", &files_to_upgrade),
            @r###"
        ---
        - files_to_compact
        - "L1, all files 99b                                                                                   "
        - "L1.13[600,700]                                                                    |-----L1.13-----| "
        - "L1.12[400,500]                                |-----L1.12-----|                                     "
        - "L1.11[250,350]      |-----L1.11-----|                                                               "
        - files_to_upgrade
        "###
        );
    }

    #[test]
    fn test_apply_one_level_large_l1() {
        let files = create_l1_files((MAX_SIZE + 1) as i64);
        let split = TargetLevelUpgradeSplit::new(MAX_SIZE);
        let (files_to_compact, files_to_upgrade) = split.apply(files, CompactionLevel::Final);

        // All files are large and eligible for upgrade
        insta::assert_yaml_snapshot!(
            format_files_split("files_to_compact", &files_to_compact, "files_to_upgrade", &files_to_upgrade),
            @r###"
        ---
        - files_to_compact
        - files_to_upgrade
        - "L1, all files 101b                                                                                  "
        - "L1.13[600,700]                                                                    |-----L1.13-----| "
        - "L1.12[400,500]                                |-----L1.12-----|                                     "
        - "L1.11[250,350]      |-----L1.11-----|                                                               "
        "###
        );
    }

    #[test]
    fn test_apply_one_level_l1_mix_size() {
        let files = create_l1_files_mix_size(MAX_SIZE as i64);

        //  . small files (< size ): L1.1, L1.3
        //  . Large files (.= size): L1.2, L1.4, L1.5
        //
        //  . files_to_compact = [L1.1, L1.2, L1.3]
        //  . files_to_upgrade = [L1.4, L1.5]
        insta::assert_yaml_snapshot!(
            format_files("initial", &files),
            @r###"
        ---
        - initial
        - "L1                                                                                                  "
        - "L1.15[1000,1100] 200b                                                                      |-L1.15-| "
        - "L1.13[600,700] 90b                                  |-L1.13-|                                       "
        - "L1.12[400,500] 101b               |-L1.12-|                                                         "
        - "L1.11[250,350] 99b  |-L1.11-|                                                                       "
        - "L1.14[800,900] 100b                                                    |-L1.14-|                    "
        "###
        );

        let split = TargetLevelUpgradeSplit::new(MAX_SIZE);
        let (files_to_compact, files_to_upgrade) = split.apply(files, CompactionLevel::Final);

        // Some files are large and eligible for upgrade
        insta::assert_yaml_snapshot!(
            format_files_split("files_to_compact", &files_to_compact, "files_to_upgrade", &files_to_upgrade),
            @r###"
        ---
        - files_to_compact
        - "L1                                                                                                  "
        - "L1.11[250,350] 99b  |-----L1.11-----|                                                               "
        - "L1.13[600,700] 90b                                                                |-----L1.13-----| "
        - "L1.12[400,500] 101b                           |-----L1.12-----|                                     "
        - files_to_upgrade
        - "L1                                                                                                  "
        - "L1.15[1000,1100] 200b                                                     |---------L1.15----------| "
        - "L1.14[800,900] 100b |---------L1.14----------|                                                      "
        "###
        );
    }

    #[test]
    fn test_apply_all_small_target_l1() {
        let files = create_overlapped_l0_l1_files((MAX_SIZE - 1) as i64);
        insta::assert_yaml_snapshot!(
            format_files("initial", &files),
            @r###"
        ---
        - initial
        - "L0, all files 99b                                                                                   "
        - "L0.2[650,750]                                                        |---L0.2---|                   "
        - "L0.1[450,620]                               |-------L0.1-------|                                    "
        - "L0.3[800,900]                                                                          |---L0.3---| "
        - "L1, all files 99b                                                                                   "
        - "L1.13[600,700]                                                 |--L1.13---|                         "
        - "L1.12[400,500]                        |--L1.12---|                                                  "
        - "L1.11[250,350]      |--L1.11---|                                                                    "
        "###
        );

        let split = TargetLevelUpgradeSplit::new(MAX_SIZE);
        let (files_to_compact, files_to_upgrade) =
            split.apply(files, CompactionLevel::FileNonOverlapped);

        // All files are small --> nothing to upgrade
        insta::assert_yaml_snapshot!(
            format_files_split("files_to_compact", &files_to_compact, "files_to_upgrade", &files_to_upgrade),
            @r###"
        ---
        - files_to_compact
        - "L0, all files 99b                                                                                   "
        - "L0.3[800,900]                                                                          |---L0.3---| "
        - "L0.1[450,620]                               |-------L0.1-------|                                    "
        - "L0.2[650,750]                                                        |---L0.2---|                   "
        - "L1, all files 99b                                                                                   "
        - "L1.13[600,700]                                                 |--L1.13---|                         "
        - "L1.12[400,500]                        |--L1.12---|                                                  "
        - "L1.11[250,350]      |--L1.11---|                                                                    "
        - files_to_upgrade
        "###
        );
    }

    #[test]
    fn test_apply_all_large_target_l1() {
        let files = create_overlapped_l0_l1_files((MAX_SIZE) as i64);
        insta::assert_yaml_snapshot!(
            format_files("initial", &files),
            @r###"
        ---
        - initial
        - "L0, all files 100b                                                                                  "
        - "L0.2[650,750]                                                        |---L0.2---|                   "
        - "L0.1[450,620]                               |-------L0.1-------|                                    "
        - "L0.3[800,900]                                                                          |---L0.3---| "
        - "L1, all files 100b                                                                                  "
        - "L1.13[600,700]                                                 |--L1.13---|                         "
        - "L1.12[400,500]                        |--L1.12---|                                                  "
        - "L1.11[250,350]      |--L1.11---|                                                                    "
        "###
        );

        let split = TargetLevelUpgradeSplit::new(MAX_SIZE);
        let (files_to_compact, files_to_upgrade) =
            split.apply(files, CompactionLevel::FileNonOverlapped);

        // All files are large --> L0.3 is eligible for upgrade
        insta::assert_yaml_snapshot!(
            format_files_split("files_to_compact", &files_to_compact, "files_to_upgrade", &files_to_upgrade),
            @r###"
        ---
        - files_to_compact
        - "L0, all files 100b                                                                                  "
        - "L0.1[450,620]                                       |----------L0.1-----------|                     "
        - "L0.2[650,750]                                                                       |-----L0.2-----|"
        - "L1, all files 100b                                                                                  "
        - "L1.13[600,700]                                                              |----L1.13-----|        "
        - "L1.12[400,500]                              |----L1.12-----|                                        "
        - "L1.11[250,350]      |----L1.11-----|                                                                "
        - files_to_upgrade
        - "L0, all files 100b                                                                                  "
        - "L0.3[800,900]       |-------------------------------------L0.3-------------------------------------|"
        "###
        );
    }

    #[test]
    fn test_apply_all_small_target_l2() {
        let files = create_overlapped_l1_l2_files((MAX_SIZE - 1) as i64);
        let split = TargetLevelUpgradeSplit::new(MAX_SIZE);
        let (files_to_compact, files_to_upgrade) = split.apply(files, CompactionLevel::Final);

        // All files are small --> nothing to upgrade
        insta::assert_yaml_snapshot!(
            format_files_split("files_to_compact", &files_to_compact, "files_to_upgrade", &files_to_upgrade),
            @r###"
        ---
        - files_to_compact
        - "L1, all files 99b                                                                                   "
        - "L1.11[250,350]                                  |--L1.11--|                                         "
        - "L1.12[400,500]                                                   |--L1.12--|                        "
        - "L1.13[600,700]                                                                          |--L1.13--| "
        - "L2, all files 99b                                                                                   "
        - "L2.21[0,100]        |--L2.21--|                                                                     "
        - "L2.22[200,300]                            |--L2.22--|                                               "
        - files_to_upgrade
        "###
        );
    }

    #[test]
    fn test_apply_all_large_target_l2() {
        let files = create_overlapped_l1_l2_files(MAX_SIZE as i64);
        insta::assert_yaml_snapshot!(
            format_files("initial", &files),
            @r###"
        ---
        - initial
        - "L1, all files 100b                                                                                  "
        - "L1.13[600,700]                                                                          |--L1.13--| "
        - "L1.12[400,500]                                                   |--L1.12--|                        "
        - "L1.11[250,350]                                  |--L1.11--|                                         "
        - "L2, all files 100b                                                                                  "
        - "L2.21[0,100]        |--L2.21--|                                                                     "
        - "L2.22[200,300]                            |--L2.22--|                                               "
        "###
        );

        let split = TargetLevelUpgradeSplit::new(MAX_SIZE);
        let (files_to_compact, files_to_upgrade) = split.apply(files, CompactionLevel::Final);

        // All files are large --> L1.2 and L1.3 are eligible for upgrade
        insta::assert_yaml_snapshot!(
            format_files_split("files_to_compact", &files_to_compact, "files_to_upgrade", &files_to_upgrade),
            @r###"
        ---
        - files_to_compact
        - "L1, all files 100b                                                                                  "
        - "L1.11[250,350]                                                               |-------L1.11--------| "
        - "L2, all files 100b                                                                                  "
        - "L2.21[0,100]        |-------L2.21--------|                                                          "
        - "L2.22[200,300]                                                   |-------L2.22--------|             "
        - files_to_upgrade
        - "L1, all files 100b                                                                                  "
        - "L1.13[600,700]                                                           |---------L1.13----------| "
        - "L1.12[400,500]      |---------L1.12----------|                                                      "
        "###
        );
    }

    #[test]
    fn test_apply_all_small_target_l2_mix_size() {
        let files = create_overlapped_l1_l2_files_mix_size(MAX_SIZE as i64);
        //  Small files (< size): [L1.3]
        //  Large files: [L2.1, L2.2, L1.1, L1.2]
        // ==> nothing to upgrade
        insta::assert_yaml_snapshot!(
            format_files("initial", &files),
            @r###"
        ---
        - initial
        - "L1                                                                                                  "
        - "L1.13[600,700] 99b                                                                      |--L1.13--| "
        - "L1.12[400,500] 100b                                              |--L1.12--|                        "
        - "L1.11[250,350] 100b                             |--L1.11--|                                         "
        - "L2                                                                                                  "
        - "L2.21[0,100] 100b   |--L2.21--|                                                                     "
        - "L2.22[200,300] 100b                       |--L2.22--|                                               "
        "###
        );

        let split = TargetLevelUpgradeSplit::new(MAX_SIZE);
        let (files_to_compact, files_to_upgrade) = split.apply(files, CompactionLevel::Final);

        insta::assert_yaml_snapshot!(
            format_files_split("files_to_compact", &files_to_compact, "files_to_upgrade", &files_to_upgrade),
            @r###"
        ---
        - files_to_compact
        - "L1                                                                                                  "
        - "L1.11[250,350] 100b                             |--L1.11--|                                         "
        - "L1.13[600,700] 99b                                                                      |--L1.13--| "
        - "L1.12[400,500] 100b                                              |--L1.12--|                        "
        - "L2                                                                                                  "
        - "L2.21[0,100] 100b   |--L2.21--|                                                                     "
        - "L2.22[200,300] 100b                       |--L2.22--|                                               "
        - files_to_upgrade
        "###
        );
    }

    #[test]
    fn test_apply_all_small_target_l2_mix_size_2() {
        let files = create_overlapped_l1_l2_files_mix_size_2(MAX_SIZE as i64);
        //  Small files (< size): [L1.2]
        //  Large files: [L2.1, L2.2, L1.1, L1.3]
        //  ==> L1.3 is eligible for upgrade
        insta::assert_yaml_snapshot!(
            format_files("initial", &files),
            @r###"
        ---
        - initial
        - "L1                                                                                                  "
        - "L1.13[600,700] 100b                                                                     |--L1.13--| "
        - "L1.12[400,500] 99b                                               |--L1.12--|                        "
        - "L1.11[250,350] 100b                             |--L1.11--|                                         "
        - "L2                                                                                                  "
        - "L2.21[0,100] 100b   |--L2.21--|                                                                     "
        - "L2.22[200,300] 100b                       |--L2.22--|                                               "
        "###
        );

        let split = TargetLevelUpgradeSplit::new(MAX_SIZE);
        let (files_to_compact, files_to_upgrade) = split.apply(files, CompactionLevel::Final);
        insta::assert_yaml_snapshot!(
            format_files_split("files_to_compact", &files_to_compact, "files_to_upgrade", &files_to_upgrade),
            @r###"
        ---
        - files_to_compact
        - "L1                                                                                                  "
        - "L1.11[250,350] 100b                                         |----L1.11-----|                        "
        - "L1.12[400,500] 99b                                                                  |----L1.12-----|"
        - "L2                                                                                                  "
        - "L2.21[0,100] 100b   |----L2.21-----|                                                                "
        - "L2.22[200,300] 100b                                 |----L2.22-----|                                "
        - files_to_upgrade
        - "L1, all files 100b                                                                                  "
        - "L1.13[600,700]      |------------------------------------L1.13-------------------------------------|"
        "###
        );
    }

    #[test]
    fn test_apply_all_large_but_no_upragde() {
        let files = create_overlapped_files_2(MAX_SIZE as i64);
        // L0s in the time range of L1 ==> nothing to upgrade
        insta::assert_yaml_snapshot!(
            format_files("initial", &files),
            @r###"
        ---
        - initial
        - "L0, all files 100b                                                                                  "
        - "L0.2[520,550]                                                                  |L0.2|               "
        - "L0.1[250,350]                                   |--L0.1---|                                         "
        - "L1, all files 100b                                                                                  "
        - "L1.13[400,500]                                                   |--L1.13--|                        "
        - "L1.12[200,300]                            |--L1.12--|                                               "
        - "L1.11[0,100]        |--L1.11--|                                                                     "
        - "L1.14[600,700]                                                                          |--L1.14--| "
        "###
        );

        let split = TargetLevelUpgradeSplit::new(MAX_SIZE);
        let (files_to_compact, files_to_upgrade) =
            split.apply(files, CompactionLevel::FileNonOverlapped);

        insta::assert_yaml_snapshot!(
            format_files_split("files_to_compact", &files_to_compact, "files_to_upgrade", &files_to_upgrade),
            @r###"
        ---
        - files_to_compact
        - "L0, all files 100b                                                                                  "
        - "L0.1[250,350]                                   |--L0.1---|                                         "
        - "L0.2[520,550]                                                                  |L0.2|               "
        - "L1, all files 100b                                                                                  "
        - "L1.13[400,500]                                                   |--L1.13--|                        "
        - "L1.12[200,300]                            |--L1.12--|                                               "
        - "L1.11[0,100]        |--L1.11--|                                                                     "
        - "L1.14[600,700]                                                                          |--L1.14--| "
        - files_to_upgrade
        "###
        );
    }

    #[test]
    fn test_apply_all_small_target_l1_2() {
        let files = create_overlapped_files_3((MAX_SIZE - 1) as i64);
        // All small ==> nothing to upgrade
        insta::assert_yaml_snapshot!(
            format_files("initial", &files),
            @r###"
        ---
        - initial
        - "L0, all files 99b                                                                                   "
        - "L0.3[400,500]                                    |L0.3-|                                            "
        - "L0.2[200,300]                     |L0.2-|                                                           "
        - "L0.1[0,100]         |L0.1-|                                                                         "
        - "L0.4[600,700]                                                  |L0.4-|                              "
        - "L0.5[800,900]                                                                 |L0.5-|               "
        - "L0.6[1000,1100]                                                                             |L0.6-| "
        - "L1, all files 99b                                                                                   "
        - "L1.11[250,350]                        |L1.11|                                                       "
        - "L1.12[650,750]                                                     |L1.12|                          "
        "###
        );

        let split = TargetLevelUpgradeSplit::new(MAX_SIZE);
        let (files_to_compact, files_to_upgrade) =
            split.apply(files, CompactionLevel::FileNonOverlapped);

        insta::assert_yaml_snapshot!(
            format_files_split("files_to_compact", &files_to_compact, "files_to_upgrade", &files_to_upgrade),
            @r###"
        ---
        - files_to_compact
        - "L0, all files 99b                                                                                   "
        - "L0.6[1000,1100]                                                                             |L0.6-| "
        - "L0.5[800,900]                                                                 |L0.5-|               "
        - "L0.4[600,700]                                                  |L0.4-|                              "
        - "L0.1[0,100]         |L0.1-|                                                                         "
        - "L0.2[200,300]                     |L0.2-|                                                           "
        - "L0.3[400,500]                                    |L0.3-|                                            "
        - "L1, all files 99b                                                                                   "
        - "L1.11[250,350]                        |L1.11|                                                       "
        - "L1.12[650,750]                                                     |L1.12|                          "
        - files_to_upgrade
        "###
        );
    }

    #[test]
    fn test_apply_all_large_target_l1_2() {
        let files = create_overlapped_files_3((MAX_SIZE + 10) as i64);
        // All large ==> L0.1, L0.5, L0.6 are eligible for upgrade
        //   files_to_compact: [L0.2, L0.3, L0.4, L1.1, L1.2]
        //   files_to_upgrade: [L0.1, L0.5, L0.6]
        insta::assert_yaml_snapshot!(
            format_files("initial", &files),
            @r###"
        ---
        - initial
        - "L0, all files 110b                                                                                  "
        - "L0.3[400,500]                                    |L0.3-|                                            "
        - "L0.2[200,300]                     |L0.2-|                                                           "
        - "L0.1[0,100]         |L0.1-|                                                                         "
        - "L0.4[600,700]                                                  |L0.4-|                              "
        - "L0.5[800,900]                                                                 |L0.5-|               "
        - "L0.6[1000,1100]                                                                             |L0.6-| "
        - "L1, all files 110b                                                                                  "
        - "L1.11[250,350]                        |L1.11|                                                       "
        - "L1.12[650,750]                                                     |L1.12|                          "
        "###
        );

        let split = TargetLevelUpgradeSplit::new(MAX_SIZE);
        let (files_to_compact, files_to_upgrade) =
            split.apply(files, CompactionLevel::FileNonOverlapped);

        insta::assert_yaml_snapshot!(
            format_files_split("files_to_compact", &files_to_compact, "files_to_upgrade", &files_to_upgrade),
            @r###"
        ---
        - files_to_compact
        - "L0, all files 110b                                                                                  "
        - "L0.4[600,700]                                                                 |----L0.4----|        "
        - "L0.2[200,300]       |----L0.2----|                                                                  "
        - "L0.3[400,500]                                    |----L0.3----|                                     "
        - "L1, all files 110b                                                                                  "
        - "L1.11[250,350]             |---L1.11----|                                                           "
        - "L1.12[650,750]                                                                       |---L1.12----| "
        - files_to_upgrade
        - "L0, all files 110b                                                                                  "
        - "L0.1[0,100]         |L0.1-|                                                                         "
        - "L0.5[800,900]                                                                 |L0.5-|               "
        - "L0.6[1000,1100]                                                                             |L0.6-| "
        "###
        );
    }

    #[test]
    fn test_apply_mix_size_target_l1_2() {
        let files = create_overlapped_files_3_mix_size(MAX_SIZE as i64);
        // Small files (< size): L0.6
        // Large files: the rest
        // ==> only L0.1 is eligible for upgrade
        insta::assert_yaml_snapshot!(
            format_files("initial", &files),
            @r###"
        ---
        - initial
        - "L0                                                                                                  "
        - "L0.3[400,500] 100b                               |L0.3-|                                            "
        - "L0.2[200,300] 100b                |L0.2-|                                                           "
        - "L0.1[0,100] 100b    |L0.1-|                                                                         "
        - "L0.4[600,700] 100b                                             |L0.4-|                              "
        - "L0.5[800,900] 100b                                                            |L0.5-|               "
        - "L0.6[1000,1100] 99b                                                                         |L0.6-| "
        - "L1                                                                                                  "
        - "L1.11[250,350] 100b                   |L1.11|                                                       "
        - "L1.12[650,750] 100b                                                |L1.12|                          "
        "###
        );

        let split = TargetLevelUpgradeSplit::new(MAX_SIZE);
        let (files_to_compact, files_to_upgrade) =
            split.apply(files, CompactionLevel::FileNonOverlapped);

        insta::assert_yaml_snapshot!(
            format_files_split("files_to_compact", &files_to_compact, "files_to_upgrade", &files_to_upgrade),
            @r###"
        ---
        - files_to_compact
        - "L0                                                                                                  "
        - "L0.6[1000,1100] 99b                                                                        |-L0.6-| "
        - "L0.4[600,700] 100b                                     |-L0.4-|                                     "
        - "L0.2[200,300] 100b  |-L0.2-|                                                                        "
        - "L0.3[400,500] 100b                   |-L0.3-|                                                       "
        - "L0.5[800,900] 100b                                                       |-L0.5-|                   "
        - "L1                                                                                                  "
        - "L1.11[250,350] 100b     |L1.11-|                                                                    "
        - "L1.12[650,750] 100b                                         |L1.12-|                                "
        - files_to_upgrade
        - "L0, all files 100b                                                                                  "
        - "L0.1[0,100]         |-------------------------------------L0.1-------------------------------------|"
        "###
        );
    }
}
