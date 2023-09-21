use data_types::{ParquetFile, TimestampMinMax, TransitionPartitionId};

use crate::{components::ir_planner::planner_v1::V1IRPlanner, file_classification::FileToSplit};

// max number of files in a minimum possible compacting set
const MAX_FILE_NUM: usize = 2;
// percentage of soft limit of max desired file size allowed to be exceeded when splitting
pub const PERCENTAGE_OF_SOFT_EXCEEDED: f64 = 0.1;

/// Return `[files_to_split]` and `[files_not_to_split]` of the given files.
/// files_to_split are the files that are larger than max_desired_file_size.
/// files_not_to_split are the files that are smaller than max_desired_file_size.
pub fn compute_split_times_for_large_files(
    files: Vec<ParquetFile>,
    max_desired_file_size: u64,
    max_compact_size: usize,
    partition: TransitionPartitionId,
) -> (Vec<FileToSplit>, Vec<ParquetFile>) {
    // Sanity checks
    // There must be at most 2 files
    assert!(
        files.len() <= MAX_FILE_NUM && !files.is_empty(),
        "there must be at least one file and at most {MAX_FILE_NUM} files, instead found {} files, partition_id={}",
        files.len(),
        partition,
    );
    // max compact size must at least MAX_FILE_NUM  times larger then max desired file size to ensure the split works
    assert!(
        max_compact_size >= MAX_FILE_NUM * max_desired_file_size as usize,
        "max_compact_size {max_compact_size} must be at least {MAX_FILE_NUM} times larger than max_desired_file_size {max_desired_file_size}, partition_id={partition}",
    );
    // Total size of files must be larger than max_compact_size
    let total_size: i64 = files.iter().map(|f| f.file_size_bytes).sum();
    assert!(
        total_size as usize > max_compact_size,
        "total size of files {total_size} must be larger than max_compact_size {max_compact_size}, partition_id={partition}",
    );

    // Split files over max_desired_file_size into multiple files each is softly around max_desired_file_size
    let mut files_to_split = Vec::with_capacity(files.len());
    let mut files_not_to_split = Vec::with_capacity(files.len());
    for file in files.into_iter() {
        let file_size = file.file_size_bytes as u64;
        let min_time = file.min_time.get();
        let max_time = file.max_time.get();

        // TODO: it would be nice to check if these files overlap (e.g. if they're multiple levels), and
        // coordinate the split time across all the files, rather than deciding the split time for each file
        // as if its the only file under consideration.
        // only split files that are larger than max_desired_file_size and have time range at least 2
        let max_file_size =
            (max_desired_file_size as f64 * (1.0 + PERCENTAGE_OF_SOFT_EXCEEDED)) as u64;
        if file_size > max_file_size && file.min_time < file.max_time {
            if file.min_time < file.max_time - 1 {
                // The time range of the file is big enough we have choices for split time(s), so compute them.
                let file_times = vec![TimestampMinMax {
                    min: min_time,
                    max: max_time,
                }];
                let split_times = V1IRPlanner::compute_split_time(
                    file_times,
                    min_time,
                    max_time,
                    file_size,
                    max_desired_file_size,
                );
                files_to_split.push(FileToSplit { file, split_times });
            } else {
                // The file covers 2ns.  There's nothing to compute, split it the only place possible.
                // When splitting, split time is the last ns included in the 'left' file on the split.
                // So setting `min` as the split time means `min` goes to the left, and `max` goes to the right.
                let split_times = vec![file.min_time.get()];
                files_to_split.push(FileToSplit { file, split_times });
            }
        } else {
            files_not_to_split.push(file);
        }
    }

    (files_to_split, files_not_to_split)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use compactor_test_utils::{
        create_fake_partition_id, create_overlapped_l0_l1_files_3,
        create_overlapped_two_overlapped_files, format_files, format_files_split, TestTimes,
    };
    use data_types::CompactionLevel;
    use iox_tests::ParquetFileBuilder;
    use iox_time::{MockProvider, Time};

    use crate::components::split_or_compact::large_files_to_split::compute_split_times_for_large_files;

    const FILE_SIZE: i64 = 100;

    // empty input
    #[test]
    #[should_panic(
        expected = "there must be at least one file and at most 2 files, instead found 0 files, partition_id=0"
    )]
    fn test_empty_input() {
        let (_files_to_split, _files_not_to_split) = compute_split_times_for_large_files(
            vec![],
            (FILE_SIZE + 1) as u64,
            ((FILE_SIZE + 1) * 3) as usize,
            create_fake_partition_id(),
        );
    }

    // more than 2 files
    #[test]
    #[should_panic(
        expected = "there must be at least one file and at most 2 files, instead found 5 files, partition_id=0"
    )]
    fn test_too_many_files() {
        let files = create_overlapped_l0_l1_files_3(FILE_SIZE);
        insta::assert_yaml_snapshot!(
            format_files("initial", &files),
            @r###"
        ---
        - initial
        - "L0, all files 100b                                                                                                 "
        - "L0.2[650,750] 180s                                                    |------L0.2------|                           "
        - "L0.1[450,550] 120s                |------L0.1------|                                                               "
        - "L0.3[800,900] 300s                                                                               |------L0.3------|"
        - "L1, all files 100b                                                                                                 "
        - "L1.13[600,700] 60s                                           |-----L1.13------|                                    "
        - "L1.12[400,500] 60s       |-----L1.12------|                                                                        "
        "###
        );

        let (_files_to_split, _files_not_to_split) = compute_split_times_for_large_files(
            files,
            (FILE_SIZE + 1) as u64,
            ((FILE_SIZE + 1) * 3) as usize,
            create_fake_partition_id(),
        );
    }

    // invalid max compact size
    #[test]
    #[should_panic(
        expected = "max_compact_size 111 must be at least 2 times larger than max_desired_file_size 101"
    )]
    fn test_invalid_max_compact_size() {
        let files = create_overlapped_two_overlapped_files(FILE_SIZE);
        insta::assert_yaml_snapshot!(
            format_files("initial", &files),
            @r###"
        ---
        - initial
        - "L0, all files 100b                                                                                                 "
        - "L0.1[450,620] 120s                           |-------------------------------L0.1--------------------------------| "
        - "L1, all files 100b                                                                                                 "
        - "L1.11[400,500] 60s       |----------------L1.11-----------------|                                                  "
        "###
        );

        let (_files_to_split, _files_not_to_split) = compute_split_times_for_large_files(
            files,
            (FILE_SIZE + 1) as u64,
            ((FILE_SIZE + 1) + 10) as usize,
            create_fake_partition_id(),
        );
    }

    // invalid total size
    #[test]
    #[should_panic(
        expected = "total size of files 200 must be larger than max_compact_size 300, partition_id=0"
    )]
    fn test_invalid_total_size() {
        let files = create_overlapped_two_overlapped_files(FILE_SIZE);
        insta::assert_yaml_snapshot!(
            format_files("initial", &files),
            @r###"
        ---
        - initial
        - "L0, all files 100b                                                                                                 "
        - "L0.1[450,620] 120s                           |-------------------------------L0.1--------------------------------| "
        - "L1, all files 100b                                                                                                 "
        - "L1.11[400,500] 60s       |----------------L1.11-----------------|                                                  "
        "###
        );

        let (_files_to_split, _files_not_to_split) = compute_split_times_for_large_files(
            files,
            FILE_SIZE as u64,
            (FILE_SIZE * 3) as usize,
            create_fake_partition_id(),
        );
    }

    // split both large files
    #[test]
    fn test_split_both_large_files() {
        let file_size = FILE_SIZE;
        let max_desired_file_size = (FILE_SIZE / 4) as u64;
        let max_compact_size = (max_desired_file_size * 3) as usize;

        let files = create_overlapped_two_overlapped_files(file_size);
        insta::assert_yaml_snapshot!(
            format_files("initial", &files),
            @r###"
        ---
        - initial
        - "L0, all files 100b                                                                                                 "
        - "L0.1[450,620] 120s                           |-------------------------------L0.1--------------------------------| "
        - "L1, all files 100b                                                                                                 "
        - "L1.11[400,500] 60s       |----------------L1.11-----------------|                                                  "
        "###
        );

        let (files_to_split, files_not_to_split) = compute_split_times_for_large_files(
            files,
            max_desired_file_size,
            max_compact_size,
            create_fake_partition_id(),
        );

        // See layout of 2 set of files
        let files_to_split = files_to_split
            .into_iter()
            .map(|f| f.file)
            .collect::<Vec<_>>();
        insta::assert_yaml_snapshot!(
            format_files_split("files to split", &files_to_split , "files not to split:", &files_not_to_split),
            @r###"
        ---
        - files to split
        - "L0, all files 100b                                                                                                 "
        - "L0.1[450,620] 120s                           |-------------------------------L0.1--------------------------------| "
        - "L1, all files 100b                                                                                                 "
        - "L1.11[400,500] 60s       |----------------L1.11-----------------|                                                  "
        - "files not to split:"
        "###
        );
    }

    // split only the large file start level file
    #[test]
    fn test_split_large_start_level() {
        let time_provider = Arc::new(MockProvider::new(Time::from_timestamp(0, 0).unwrap()));
        let time = TestTimes::new(&time_provider);

        let large_size = FILE_SIZE * 3;
        let small_size = FILE_SIZE / 2;
        let max_desired_file_size = FILE_SIZE as u64;
        let max_compact_size = (max_desired_file_size * 3) as usize;

        let l1_1 = ParquetFileBuilder::new(11)
            .with_compaction_level(CompactionLevel::FileNonOverlapped)
            .with_time_range(400, 500)
            .with_file_size_bytes(small_size)
            .with_max_l0_created_at(time.time_1_minute_future)
            .build();

        // L0_1 overlaps with L1_1
        let l0_1 = ParquetFileBuilder::new(1)
            .with_compaction_level(CompactionLevel::Initial)
            .with_time_range(450, 620)
            .with_file_size_bytes(large_size)
            .with_max_l0_created_at(time.time_2_minutes_future)
            .build();

        let files = vec![l1_1, l0_1];

        insta::assert_yaml_snapshot!(
            format_files("initial", &files),
            @r###"
        ---
        - initial
        - "L0                                                                                                                 "
        - "L0.1[450,620] 120s 300b                      |-------------------------------L0.1--------------------------------| "
        - "L1                                                                                                                 "
        - "L1.11[400,500] 60s 50b   |----------------L1.11-----------------|                                                  "
        "###
        );

        let (files_to_split, files_not_to_split) = compute_split_times_for_large_files(
            files,
            max_desired_file_size,
            max_compact_size,
            create_fake_partition_id(),
        );
        // The split files should be L0_1 with 2 split times to split the file into 3 smaller files
        assert_eq!(files_to_split.len(), 1);
        assert_eq!(files_to_split[0].split_times.len(), 2);

        // See layout of 2 set of files
        let files_to_split = files_to_split
            .into_iter()
            .map(|f| f.file)
            .collect::<Vec<_>>();
        insta::assert_yaml_snapshot!(
            format_files_split("files to split", &files_to_split , "files not to split:", &files_not_to_split),
            @r###"
        ---
        - files to split
        - "L0, all files 300b                                                                                                 "
        - "L0.1[450,620] 120s       |------------------------------------------L0.1------------------------------------------|"
        - "files not to split:"
        - "L1, all files 50b                                                                                                  "
        - "L1.11[400,500] 60s       |-----------------------------------------L1.11------------------------------------------|"
        "###
        );
    }

    // split only the large file target level file
    #[test]
    fn test_split_large_target_level() {
        let time_provider = Arc::new(MockProvider::new(Time::from_timestamp(0, 0).unwrap()));
        let time = TestTimes::new(&time_provider);

        let large_size = FILE_SIZE * 3;
        let small_size = FILE_SIZE / 2;
        let max_desired_file_size = FILE_SIZE as u64;
        let max_compact_size = (max_desired_file_size * 3) as usize;

        let l1_1 = ParquetFileBuilder::new(11)
            .with_compaction_level(CompactionLevel::FileNonOverlapped)
            .with_time_range(400, 500)
            .with_file_size_bytes(large_size)
            .with_max_l0_created_at(time.time_1_minute_future)
            .build();

        // L0_1 overlaps with L1_1
        let l0_1 = ParquetFileBuilder::new(1)
            .with_compaction_level(CompactionLevel::Initial)
            .with_time_range(450, 620)
            .with_file_size_bytes(small_size)
            .with_max_l0_created_at(time.time_2_minutes_future)
            .build();

        let files = vec![l1_1, l0_1];

        insta::assert_yaml_snapshot!(
            format_files("initial", &files),
            @r###"
        ---
        - initial
        - "L0                                                                                                                 "
        - "L0.1[450,620] 120s 50b                       |-------------------------------L0.1--------------------------------| "
        - "L1                                                                                                                 "
        - "L1.11[400,500] 60s 300b  |----------------L1.11-----------------|                                                  "
        "###
        );

        let (files_to_split, files_not_to_split) = compute_split_times_for_large_files(
            files,
            max_desired_file_size,
            max_compact_size,
            create_fake_partition_id(),
        );
        // The split files should be L1_1 with 2 split times to split the file into 3 smaller files
        assert_eq!(files_to_split.len(), 1);
        assert_eq!(files_to_split[0].split_times.len(), 2);

        // See layout of 2 set of files
        let files_to_split = files_to_split
            .into_iter()
            .map(|f| f.file)
            .collect::<Vec<_>>();
        insta::assert_yaml_snapshot!(
            format_files_split("files to split", &files_to_split , "files not to split:", &files_not_to_split),
            @r###"
        ---
        - files to split
        - "L1, all files 300b                                                                                                 "
        - "L1.11[400,500] 60s       |-----------------------------------------L1.11------------------------------------------|"
        - "files not to split:"
        - "L0, all files 50b                                                                                                  "
        - "L0.1[450,620] 120s       |------------------------------------------L0.1------------------------------------------|"
        "###
        );
    }

    // tiny time-range on one file
    #[test]
    fn test_one_file_with_tiny_time_range() {
        let time_provider = Arc::new(MockProvider::new(Time::from_timestamp(0, 0).unwrap()));
        let time = TestTimes::new(&time_provider);

        let large_size = FILE_SIZE * 3;
        let max_desired_file_size = FILE_SIZE as u64;
        let max_compact_size = (max_desired_file_size * 3) as usize;

        let l1_1 = ParquetFileBuilder::new(11)
            .with_compaction_level(CompactionLevel::FileNonOverlapped)
            .with_time_range(400, 401)
            .with_file_size_bytes(large_size)
            .with_max_l0_created_at(time.time_1_minute_future)
            .build();

        // L0_1 overlaps with L1_1
        let l0_1 = ParquetFileBuilder::new(1)
            .with_compaction_level(CompactionLevel::Initial)
            .with_time_range(400, 620)
            .with_file_size_bytes(large_size)
            .with_max_l0_created_at(time.time_2_minutes_future)
            .build();

        let files = vec![l1_1, l0_1];

        insta::assert_yaml_snapshot!(
            format_files("initial", &files),
            @r###"
        ---
        - initial
        - "L0, all files 300b                                                                                                 "
        - "L0.1[400,620] 120s       |-----------------------------------------L0.1------------------------------------------| "
        - "L1, all files 300b                                                                                                 "
        - "L1.11[400,401] 60s       |L1.11|                                                                                   "
        "###
        );

        let (files_to_split, files_not_to_split) = compute_split_times_for_large_files(
            files,
            max_desired_file_size,
            max_compact_size,
            create_fake_partition_id(),
        );
        // The split files should be L1_1 with 2 split times to split the file into 3 smaller files, and the L1 split at the only
        // time possible (since its a 2ns file, there is only one choice)
        assert_eq!(files_to_split.len(), 2);
        assert_eq!(files_to_split[0].split_times.len(), 1);
        assert_eq!(files_to_split[1].split_times.len(), 2);

        // See layout of 2 set of files
        let files_to_split = files_to_split
            .into_iter()
            .map(|f| f.file)
            .collect::<Vec<_>>();
        insta::assert_yaml_snapshot!(
            format_files_split("files to split", &files_to_split , "files not to split:", &files_not_to_split),
            @r###"
        ---
        - files to split
        - "L0, all files 300b                                                                                                 "
        - "L0.1[400,620] 120s       |-----------------------------------------L0.1------------------------------------------| "
        - "L1, all files 300b                                                                                                 "
        - "L1.11[400,401] 60s       |L1.11|                                                                                   "
        - "files not to split:"
        "###
        );
    }

    // tiny time-range on both files
    #[test]
    fn test_two_files_with_tiny_time_range() {
        let time_provider = Arc::new(MockProvider::new(Time::from_timestamp(0, 0).unwrap()));
        let time = TestTimes::new(&time_provider);

        let large_size = FILE_SIZE * 3;
        let max_desired_file_size = FILE_SIZE as u64;
        let max_compact_size = (max_desired_file_size * 3) as usize;

        let l1_1 = ParquetFileBuilder::new(11)
            .with_compaction_level(CompactionLevel::FileNonOverlapped)
            .with_time_range(400, 401)
            .with_file_size_bytes(large_size)
            .with_max_l0_created_at(time.time_1_minute_future)
            .build();

        // L0_1 overlaps with L1_1
        let l0_1 = ParquetFileBuilder::new(1)
            .with_compaction_level(CompactionLevel::Initial)
            .with_time_range(400, 400)
            .with_file_size_bytes(large_size)
            .with_max_l0_created_at(time.time_2_minutes_future)
            .build();

        let files = vec![l1_1, l0_1];

        insta::assert_yaml_snapshot!(
            format_files("initial", &files),
            @r###"
        ---
        - initial
        - "L0, all files 300b                                                                                                 "
        - "L0.1[400,400] 120s       |L0.1|                                                                                    "
        - "L1, all files 300b                                                                                                 "
        - "L1.11[400,401] 60s       |-----------------------------------------L1.11------------------------------------------|"
        "###
        );

        let (files_to_split, files_not_to_split) = compute_split_times_for_large_files(
            files,
            max_desired_file_size,
            max_compact_size,
            create_fake_partition_id(),
        );
        // The split files should be L1_11, split at the only time possible (since its a 2ns file, there is only one choice)
        assert_eq!(files_to_split.len(), 1);
        assert_eq!(files_to_split[0].split_times.len(), 1);
        assert_eq!(files_to_split[0].split_times[0], 400);

        // See layout of 2 set of files
        let files_to_split = files_to_split
            .into_iter()
            .map(|f| f.file)
            .collect::<Vec<_>>();
        insta::assert_yaml_snapshot!(
            format_files_split("files to split", &files_to_split , "files not to split:", &files_not_to_split),
            @r###"
        ---
        - files to split
        - "L1, all files 300b                                                                                                 "
        - "L1.11[400,401] 60s       |-----------------------------------------L1.11------------------------------------------|"
        - "files not to split:"
        - "L0, all files 300b                                                                                                 "
        - "L0.1[400,400] 120s       |------------------------------------------L0.1------------------------------------------|"
        "###
        );
    }
}
