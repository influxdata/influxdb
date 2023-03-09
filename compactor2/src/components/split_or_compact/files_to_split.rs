use data_types::{CompactionLevel, ParquetFile};
use itertools::Itertools;
use observability_deps::tracing::debug;

use crate::{
    components::files_split::{target_level_split::TargetLevelSplit, FilesSplit},
    file_classification::FileToSplit,
};

/// Return (`[files_to_split]`, `[files_not_to_split]`) of given files
/// such that `files_to_split` are files  in start-level that overlaps with more than one file in target_level.
///
/// Only split files in start-level if the total size greater than max_compact_size
///
/// Example:
///  . Input:
///                          |---L0.1---|   |--L0.2--|
///            |--L1.1--| |--L1.2--| |--L1.3--|
///
///    L0.1 overlaps with 2 level-1 files (L1.2, L1.3) and should be split into 2 files, one overlaps with L1.2
///    and one oerlaps with L1.3
///
///  . Output:
///     . files_to_split = [L0.1]
///     . files_not_to_split = [L1.1, L1.2, L1.3, L0.2] which is the rest of the files
///
/// Since a start-level file needs to compact with all of its overlapped target-level files to retain the invariant that
/// all files in target level are non-overlapped, splitting start-level files is to reduce the number of overlapped files
/// at the target level and avoid compacting too many files in the next compaction cycle.
/// To achieve this goal, a start-level file should be split to overlap with at most one target-level file. This enables the
/// minimum set of compacting files to 2 files: a start-level file and an overlapped target-level file.
pub fn identify_files_to_split(
    files: Vec<ParquetFile>,
    target_level: CompactionLevel,
) -> (Vec<FileToSplit>, Vec<ParquetFile>) {
    // panic if not all files are either in target level or start level
    let start_level = target_level.prev();
    assert!(files
        .iter()
        .all(|f| f.compaction_level == target_level || f.compaction_level == start_level));

    // Get start-level and target-level files
    let len = files.len();
    let split = TargetLevelSplit::new();
    let (mut start_level_files, mut target_level_files) = split.apply(files, start_level);

    // sort start_level files in their max_l0_created_at
    start_level_files.sort_by_key(|f| f.max_l0_created_at);
    // sort target level files in their min_time
    target_level_files.sort_by_key(|f| f.min_time);

    // Get files in start level that overlap with any file in target level
    let mut files_to_split = Vec::with_capacity(len);
    let mut files_not_to_split = Vec::with_capacity(len);
    for file in start_level_files {
        // Get target_level files that overlaps with this file
        let overlapped_target_level_files: Vec<&ParquetFile> = target_level_files
            .iter()
            .filter(|f| file.overlaps(f))
            .collect();

        // Neither split file that overlaps with only one file in target level
        // nor has a single timestamp (splitting this will lead to the same file and as a result infinite loop)
        // nor has time range = 1 (splitting this will cause panic because split_time will be min_tim/max_time which is disallowed)
        if overlapped_target_level_files.len() < 2
            || file.min_time == file.max_time
            || file.min_time == file.max_time - 1
        {
            files_not_to_split.push(file);
        } else {
            debug!(?file.min_time, ?file.max_time, ?file.compaction_level, "time range of file to split");
            overlapped_target_level_files
                .iter()
                .for_each(|f| debug!(?f.min_time, ?f.max_time, ?f.compaction_level, "time range of overlap file"));

            // this files will be split, add its max time
            let split_times: Vec<i64> = overlapped_target_level_files
                .iter()
                .filter(|f| f.max_time < file.max_time)
                .map(|f| f.max_time.get())
                .dedup()
                .collect();

            debug!(?split_times);

            files_to_split.push(FileToSplit { file, split_times });
        }
    }

    // keep the rest of the files for next round
    files_not_to_split.extend(target_level_files);

    assert_eq!(files_to_split.len() + files_not_to_split.len(), len);

    (files_to_split, files_not_to_split)
}

#[cfg(test)]
mod tests {
    use compactor2_test_utils::{
        create_l1_files, create_overlapped_files, create_overlapped_l0_l1_files_2, format_files,
        format_files_split,
    };
    use data_types::CompactionLevel;

    #[test]
    fn test_split_empty() {
        let files = vec![];
        let (files_to_split, files_not_to_split) =
            super::identify_files_to_split(files, CompactionLevel::Initial);
        assert!(files_to_split.is_empty());
        assert!(files_not_to_split.is_empty());
    }

    #[test]
    #[should_panic]
    fn test_split_files_wrong_target_level() {
        // all L1 files
        let files = create_l1_files(1);

        // Target is L0 while all files are in L1 --> panic
        let (_files_to_split, _files_not_to_split) =
            super::identify_files_to_split(files, CompactionLevel::Initial);
    }

    #[test]
    #[should_panic]
    fn test_split_files_three_level_files() {
        // Three level files
        let files = create_overlapped_files();
        insta::assert_yaml_snapshot!(
            format_files("initial", &files),
            @r###"
        ---
        - initial
        - "L0                                                                                                  "
        - "L0.2[650,750] 1b                                                             |-L0.2-|               "
        - "L0.1[450,620] 1b                                            |----L0.1-----|                         "
        - "L0.3[800,900] 100b                                                                         |-L0.3-| "
        - "L1                                                                                                  "
        - "L1.13[600,700] 100b                                                      |L1.13-|                   "
        - "L1.12[400,500] 1b                                      |L1.12-|                                     "
        - "L1.11[250,350] 1b                         |L1.11-|                                                  "
        - "L2                                                                                                  "
        - "L2.21[0,100] 1b     |L2.21-|                                                                        "
        - "L2.22[200,300] 1b                    |L2.22-|                                                       "
        "###
        );

        // panic because it only handle at most 2 levels next to each other
        let (_files_to_split, _files_not_to_split) =
            super::identify_files_to_split(files, CompactionLevel::FileNonOverlapped);
    }

    #[test]
    fn test_split_files_no_split() {
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

        let (files_to_split, files_not_to_split) =
            super::identify_files_to_split(files, CompactionLevel::FileNonOverlapped);
        assert!(files_to_split.is_empty());
        assert_eq!(files_not_to_split.len(), 3);
    }

    #[test]
    fn test_split_files_split() {
        let files = create_overlapped_l0_l1_files_2(1);
        insta::assert_yaml_snapshot!(
            format_files("initial", &files),
            @r###"
        ---
        - initial
        - "L0, all files 1b                                                                                    "
        - "L0.2[650,750]                                               |-----L0.2-----|                        "
        - "L0.1[450,620]               |----------L0.1-----------|                                             "
        - "L0.3[800,900]                                                                       |-----L0.3-----|"
        - "L1, all files 1b                                                                                    "
        - "L1.13[600,700]                                      |----L1.13-----|                                "
        - "L1.12[400,500]      |----L1.12-----|                                                                "
        "###
        );

        let (files_to_split, files_not_to_split) =
            super::identify_files_to_split(files, CompactionLevel::FileNonOverlapped);

        // L0.1 that overlaps with 2 level-1 files will be split
        assert_eq!(files_to_split.len(), 1);

        // L0.1 [450, 620] will be split at 500 (max of its overlapped L1.12)
        // The spit_times [500] means after we execute the split (in later steps), L0.1 will
        // be split into 2 files with time ranges: [450, 500] and [501, 620]. This means the first file will
        // overlap with L1.12 and the second file will overlap with L1.13
        assert_eq!(files_to_split[0].file.id.get(), 1);
        assert_eq!(files_to_split[0].split_times, vec![500]);

        // The rest is in not-split
        assert_eq!(files_not_to_split.len(), 4);

        // See layout of 2 set of files
        insta::assert_yaml_snapshot!(
            format_files_split("files to split:", &files_to_split.iter().map(|f| f.file.clone()).collect::<Vec<_>>(), "files not to split:", &files_not_to_split),
            @r###"
        ---
        - "files to split:"
        - "L0, all files 1b                                                                                    "
        - "L0.1[450,620]       |-------------------------------------L0.1-------------------------------------|"
        - "files not to split:"
        - "L0, all files 1b                                                                                    "
        - "L0.2[650,750]                                               |-----L0.2-----|                        "
        - "L0.3[800,900]                                                                       |-----L0.3-----|"
        - "L1, all files 1b                                                                                    "
        - "L1.12[400,500]      |----L1.12-----|                                                                "
        - "L1.13[600,700]                                      |----L1.13-----|                                "
        "###
        );
    }
}
