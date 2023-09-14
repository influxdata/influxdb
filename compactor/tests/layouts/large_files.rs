//! layout tests for scenarios with large input files
//!
//! See [crate::layout] module for detailed documentation

use data_types::CompactionLevel;
use iox_time::Time;
use std::time::Duration;

use crate::layouts::{layout_setup_builder, parquet_builder, run_layout_scenario, ONE_MB};

const MAX_DESIRED_FILE_SIZE: u64 = 100 * ONE_MB;

// One l1 file that is larger than max desired file size
#[tokio::test]
async fn one_larger_max_file_size() {
    test_helpers::maybe_start_logging();

    let setup = layout_setup_builder()
        .await
        .with_max_desired_file_size_bytes(MAX_DESIRED_FILE_SIZE)
        .build()
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1)
                .with_max_time(1000)
                .with_compaction_level(CompactionLevel::FileNonOverlapped)
                // file > max_desired_file_size_bytes
                .with_file_size_bytes(MAX_DESIRED_FILE_SIZE + 1),
        )
        .await;

    // Large L1 is upgraded to L2
    insta::assert_yaml_snapshot!(
        run_layout_scenario(&setup).await,
        @r###"
    ---
    - "**** Input Files "
    - "L1, all files 100mb                                                                                                "
    - "L1.1[1,1000] 1ns         |------------------------------------------L1.1------------------------------------------|"
    - "Committing partition 1:"
    - "  Upgrading 1 files level to CompactionLevel::L2: L1.1"
    - "**** Final Output Files (0b written)"
    - "L2, all files 100mb                                                                                                "
    - "L2.1[1,1000] 1ns         |------------------------------------------L2.1------------------------------------------|"
    "###
    );
}

// One l0 file that is larger than max desired file size
#[tokio::test]
async fn one_l0_larger_max_file_size() {
    test_helpers::maybe_start_logging();

    let setup = layout_setup_builder()
        .await
        .with_max_desired_file_size_bytes(MAX_DESIRED_FILE_SIZE)
        .build()
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1)
                .with_max_time(1000)
                .with_compaction_level(CompactionLevel::Initial)
                // file size > max_desired_file_size_bytes
                .with_file_size_bytes(MAX_DESIRED_FILE_SIZE + 1),
        )
        .await;

    // Large L0 is upgraded to L1 and then L2
    // Note: a parquet file including level 0 does not include duplicated data and no need to go over compaction
    insta::assert_yaml_snapshot!(
        run_layout_scenario(&setup).await,
        @r###"
    ---
    - "**** Input Files "
    - "L0, all files 100mb                                                                                                "
    - "L0.1[1,1000] 1ns         |------------------------------------------L0.1------------------------------------------|"
    - "Committing partition 1:"
    - "  Upgrading 1 files level to CompactionLevel::L1: L0.1"
    - "Committing partition 1:"
    - "  Upgrading 1 files level to CompactionLevel::L2: L1.1"
    - "**** Final Output Files (0b written)"
    - "L2, all files 100mb                                                                                                "
    - "L2.1[1,1000] 1ns         |------------------------------------------L2.1------------------------------------------|"
    "###
    );
}

// One l1 file that is larger than max compact size
#[tokio::test]
async fn one_larger_max_compact_size() {
    test_helpers::maybe_start_logging();

    let setup = layout_setup_builder()
        .await
        .with_max_desired_file_size_bytes(MAX_DESIRED_FILE_SIZE)
        .build()
        .await;

    let max_compact_size = setup.config.max_compact_size_bytes();

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1)
                .with_max_time(1000)
                .with_compaction_level(CompactionLevel::FileNonOverlapped)
                .with_max_l0_created_at(Time::from_timestamp_nanos(5))
                // file > max_desired_file_size_bytes
                .with_file_size_bytes((max_compact_size + 1) as u64),
        )
        .await;

    // Large L1 is upgraded to L2
    insta::assert_yaml_snapshot!(
        run_layout_scenario(&setup).await,
        @r###"
    ---
    - "**** Input Files "
    - "L1, all files 300mb                                                                                                "
    - "L1.1[1,1000] 5ns         |------------------------------------------L1.1------------------------------------------|"
    - "WARNING: file L1.1[1,1000] 5ns 300mb exceeds soft limit 100mb by more than 50%"
    - "Committing partition 1:"
    - "  Upgrading 1 files level to CompactionLevel::L2: L1.1"
    - "**** Final Output Files (0b written)"
    - "L2, all files 300mb                                                                                                "
    - "L2.1[1,1000] 5ns         |------------------------------------------L2.1------------------------------------------|"
    - "WARNING: file L2.1[1,1000] 5ns 300mb exceeds soft limit 100mb by more than 50%"
    "###
    );
}

// One l0 file that is larger than max compact size
#[tokio::test]
async fn one_l0_larger_max_compact_size() {
    test_helpers::maybe_start_logging();

    let setup = layout_setup_builder()
        .await
        .with_max_desired_file_size_bytes(MAX_DESIRED_FILE_SIZE)
        .build()
        .await;

    let max_compact_size = setup.config.max_compact_size_bytes();

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1)
                .with_max_time(1000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_max_l0_created_at(Time::from_timestamp_nanos(5))
                // file > max_desired_file_size_bytes
                .with_file_size_bytes((max_compact_size + 1) as u64),
        )
        .await;

    // Large L0 will be upgraded to L1 and then L2
    insta::assert_yaml_snapshot!(
        run_layout_scenario(&setup).await,
        @r###"
    ---
    - "**** Input Files "
    - "L0, all files 300mb                                                                                                "
    - "L0.1[1,1000] 5ns         |------------------------------------------L0.1------------------------------------------|"
    - "WARNING: file L0.1[1,1000] 5ns 300mb exceeds soft limit 100mb by more than 50%"
    - "Committing partition 1:"
    - "  Upgrading 1 files level to CompactionLevel::L1: L0.1"
    - "Committing partition 1:"
    - "  Upgrading 1 files level to CompactionLevel::L2: L1.1"
    - "**** Final Output Files (0b written)"
    - "L2, all files 300mb                                                                                                "
    - "L2.1[1,1000] 5ns         |------------------------------------------L2.1------------------------------------------|"
    - "WARNING: file L2.1[1,1000] 5ns 300mb exceeds soft limit 100mb by more than 50%"
    "###
    );
}

// Two files that are under max compact size
#[tokio::test]
async fn two_large_files_total_under_max_compact_size() {
    test_helpers::maybe_start_logging();

    let setup = layout_setup_builder()
        .await
        .with_max_desired_file_size_bytes(MAX_DESIRED_FILE_SIZE)
        .build()
        .await;

    // each file size over max_desired_file_size_bytes but total size under max_compact_size
    let size = MAX_DESIRED_FILE_SIZE + 10;

    for i in 1..=2 {
        setup
            .partition
            .create_parquet_file(
                parquet_builder()
                    .with_min_time(i)
                    .with_max_time(1000)
                    .with_compaction_level(CompactionLevel::try_from(i as i32).unwrap())
                    // max_l0_created_at of larger level is set to be smaller
                    .with_max_l0_created_at(Time::from_timestamp_nanos(10 - i))
                    .with_file_size_bytes(size),
            )
            .await;
    }

    insta::assert_yaml_snapshot!(
        run_layout_scenario(&setup).await,
        @r###"
    ---
    - "**** Input Files "
    - "L1, all files 100mb                                                                                                "
    - "L1.1[1,1000] 9ns         |------------------------------------------L1.1------------------------------------------|"
    - "L2, all files 100mb                                                                                                "
    - "L2.2[2,1000] 8ns         |-----------------------------------------L2.2------------------------------------------| "
    - "**** Simulation run 0, type=split(CompactAndSplitOutput(TotalSizeLessThanMaxCompactSize))(split_times=[501]). 2 Input Files, 200mb total:"
    - "L1, all files 100mb                                                                                                "
    - "L1.1[1,1000] 9ns         |------------------------------------------L1.1------------------------------------------|"
    - "L2, all files 100mb                                                                                                "
    - "L2.2[2,1000] 8ns         |-----------------------------------------L2.2------------------------------------------| "
    - "**** 2 Output Files (parquet_file_id not yet assigned), 200mb total:"
    - "L2                                                                                                                 "
    - "L2.?[1,501] 9ns 100mb    |-------------------L2.?--------------------|                                             "
    - "L2.?[502,1000] 9ns 100mb                                              |-------------------L2.?-------------------| "
    - "Committing partition 1:"
    - "  Soft Deleting 2 files: L1.1, L2.2"
    - "  Creating 2 files"
    - "**** Final Output Files (200mb written)"
    - "L2                                                                                                                 "
    - "L2.3[1,501] 9ns 100mb    |-------------------L2.3--------------------|                                             "
    - "L2.4[502,1000] 9ns 100mb                                              |-------------------L2.4-------------------| "
    "###
    );
}

// Two similar size and time range files with total size larger than max compact size
#[tokio::test]
async fn two_large_files_total_over_max_compact_size() {
    test_helpers::maybe_start_logging();

    let setup = layout_setup_builder()
        .await
        .with_max_desired_file_size_bytes(MAX_DESIRED_FILE_SIZE)
        .build()
        .await;

    let max_compact_size = setup.config.max_compact_size_bytes();
    let size = max_compact_size / 2 + 10;

    for i in 1..=2 {
        setup
            .partition
            .create_parquet_file(
                parquet_builder()
                    .with_min_time(i)
                    .with_max_time(1000)
                    // L1.1 or  L2.2
                    .with_compaction_level(CompactionLevel::try_from(i as i32).unwrap())
                    // max_l0_created_at of larger level is set to be smaller
                    .with_max_l0_created_at(Time::from_timestamp_nanos(10 - i))
                    .with_file_size_bytes(size as u64),
            )
            .await;
    }

    insta::assert_yaml_snapshot!(
        run_layout_scenario(&setup).await,
        @r###"
    ---
    - "**** Input Files "
    - "L1, all files 150mb                                                                                                "
    - "L1.1[1,1000] 9ns         |------------------------------------------L1.1------------------------------------------|"
    - "L2, all files 150mb                                                                                                "
    - "L2.2[2,1000] 8ns         |-----------------------------------------L2.2------------------------------------------| "
    - "WARNING: file L1.1[1,1000] 9ns 150mb exceeds soft limit 100mb by more than 50%"
    - "WARNING: file L2.2[2,1000] 8ns 150mb exceeds soft limit 100mb by more than 50%"
    - "**** Simulation run 0, type=split(ReduceLargeFileSize)(split_times=[667]). 1 Input Files, 150mb total:"
    - "L1, all files 150mb                                                                                                "
    - "L1.1[1,1000] 9ns         |------------------------------------------L1.1------------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 150mb total:"
    - "L1                                                                                                                 "
    - "L1.?[1,667] 9ns 100mb    |---------------------------L1.?---------------------------|                              "
    - "L1.?[668,1000] 9ns 50mb                                                              |-----------L1.?------------| "
    - "**** Simulation run 1, type=split(ReduceLargeFileSize)(split_times=[668]). 1 Input Files, 150mb total:"
    - "L2, all files 150mb                                                                                                "
    - "L2.2[2,1000] 8ns         |------------------------------------------L2.2------------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 150mb total:"
    - "L2                                                                                                                 "
    - "L2.?[2,668] 8ns 100mb    |---------------------------L2.?---------------------------|                              "
    - "L2.?[669,1000] 8ns 50mb                                                              |-----------L2.?------------| "
    - "Committing partition 1:"
    - "  Soft Deleting 2 files: L1.1, L2.2"
    - "  Creating 4 files"
    - "**** Simulation run 2, type=split(ReduceOverlap)(split_times=[668]). 1 Input Files, 50mb total:"
    - "L1, all files 50mb                                                                                                 "
    - "L1.4[668,1000] 9ns       |------------------------------------------L1.4------------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 50mb total:"
    - "L1                                                                                                                 "
    - "L1.?[668,668] 9ns 154kb  |L1.?|                                                                                    "
    - "L1.?[669,1000] 9ns 50mb  |-----------------------------------------L1.?------------------------------------------| "
    - "Committing partition 1:"
    - "  Soft Deleting 1 files: L1.4"
    - "  Creating 2 files"
    - "**** Simulation run 3, type=split(CompactAndSplitOutput(FoundSubsetLessThanMaxCompactSize))(split_times=[334, 667]). 3 Input Files, 200mb total:"
    - "L1                                                                                                                 "
    - "L1.3[1,667] 9ns 100mb    |-----------------------------------------L1.3------------------------------------------| "
    - "L1.7[668,668] 9ns 154kb                                                                                            |L1.7|"
    - "L2                                                                                                                 "
    - "L2.5[2,668] 8ns 100mb    |-----------------------------------------L2.5------------------------------------------| "
    - "**** 3 Output Files (parquet_file_id not yet assigned), 200mb total:"
    - "L2                                                                                                                 "
    - "L2.?[1,334] 9ns 100mb    |-------------------L2.?-------------------|                                              "
    - "L2.?[335,667] 9ns 100mb                                               |-------------------L2.?-------------------| "
    - "L2.?[668,668] 9ns 307kb                                                                                            |L2.?|"
    - "Committing partition 1:"
    - "  Soft Deleting 3 files: L1.3, L2.5, L1.7"
    - "  Creating 3 files"
    - "**** Final Output Files (550mb written)"
    - "L1                                                                                                                 "
    - "L1.8[669,1000] 9ns 50mb                                                              |-----------L1.8------------| "
    - "L2                                                                                                                 "
    - "L2.6[669,1000] 8ns 50mb                                                              |-----------L2.6------------| "
    - "L2.9[1,334] 9ns 100mb    |------------L2.9------------|                                                            "
    - "L2.10[335,667] 9ns 100mb                               |-----------L2.10-----------|                               "
    - "L2.11[668,668] 9ns 307kb                                                             |L2.11|                       "
    "###
    );
}

// Two similar size files with total size larger than max compact size with small overlap range
// The time range of target level file is much smaller and at the end range of the start level file
#[tokio::test]
async fn two_large_files_total_over_max_compact_size_small_overlap_range() {
    test_helpers::maybe_start_logging();

    let setup = layout_setup_builder()
        .await
        .with_max_desired_file_size_bytes(MAX_DESIRED_FILE_SIZE)
        .build()
        .await;

    let max_compact_size = setup.config.max_compact_size_bytes();
    let size = max_compact_size / 2 + 10;

    for i in 1..=2 {
        setup
            .partition
            .create_parquet_file(
                parquet_builder()
                    .with_min_time((i - 1) * 800)
                    .with_max_time(1000)
                    // L1.1 or  L2.2
                    .with_compaction_level(CompactionLevel::try_from(i as i32).unwrap())
                    // max_l0_created_at of larger level is set to be smaller
                    .with_max_l0_created_at(Time::from_timestamp_nanos(10 - i))
                    .with_file_size_bytes(size as u64),
            )
            .await;
    }

    insta::assert_yaml_snapshot!(
        run_layout_scenario(&setup).await,
        @r###"
    ---
    - "**** Input Files "
    - "L1, all files 150mb                                                                                                "
    - "L1.1[0,1000] 9ns         |------------------------------------------L1.1------------------------------------------|"
    - "L2, all files 150mb                                                                                                "
    - "L2.2[800,1000] 8ns                                                                               |------L2.2------|"
    - "WARNING: file L1.1[0,1000] 9ns 150mb exceeds soft limit 100mb by more than 50%"
    - "WARNING: file L2.2[800,1000] 8ns 150mb exceeds soft limit 100mb by more than 50%"
    - "**** Simulation run 0, type=split(ReduceLargeFileSize)(split_times=[667]). 1 Input Files, 150mb total:"
    - "L1, all files 150mb                                                                                                "
    - "L1.1[0,1000] 9ns         |------------------------------------------L1.1------------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 150mb total:"
    - "L1                                                                                                                 "
    - "L1.?[0,667] 9ns 100mb    |---------------------------L1.?---------------------------|                              "
    - "L1.?[668,1000] 9ns 50mb                                                              |-----------L1.?------------| "
    - "**** Simulation run 1, type=split(ReduceLargeFileSize)(split_times=[934]). 1 Input Files, 150mb total:"
    - "L2, all files 150mb                                                                                                "
    - "L2.2[800,1000] 8ns       |------------------------------------------L2.2------------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 150mb total:"
    - "L2                                                                                                                 "
    - "L2.?[800,934] 8ns 101mb  |---------------------------L2.?---------------------------|                              "
    - "L2.?[935,1000] 8ns 49mb                                                              |-----------L2.?------------| "
    - "Committing partition 1:"
    - "  Soft Deleting 2 files: L1.1, L2.2"
    - "  Creating 4 files"
    - "**** Simulation run 2, type=split(CompactAndSplitOutput(TotalSizeLessThanMaxCompactSize))(split_times=[835]). 3 Input Files, 200mb total:"
    - "L1                                                                                                                 "
    - "L1.4[668,1000] 9ns 50mb  |------------------------------------------L1.4------------------------------------------|"
    - "L2                                                                                                                 "
    - "L2.6[935,1000] 8ns 49mb                                                                          |-----L2.6------| "
    - "L2.5[800,934] 8ns 101mb                                     |---------------L2.5---------------|                   "
    - "**** 2 Output Files (parquet_file_id not yet assigned), 200mb total:"
    - "L2                                                                                                                 "
    - "L2.?[668,835] 9ns 101mb  |-------------------L2.?--------------------|                                             "
    - "L2.?[836,1000] 9ns 99mb                                               |-------------------L2.?-------------------| "
    - "Committing partition 1:"
    - "  Soft Deleting 3 files: L1.4, L2.5, L2.6"
    - "  Upgrading 1 files level to CompactionLevel::L2: L1.3"
    - "  Creating 2 files"
    - "**** Final Output Files (500mb written)"
    - "L2                                                                                                                 "
    - "L2.3[0,667] 9ns 100mb    |---------------------------L2.3---------------------------|                              "
    - "L2.7[668,835] 9ns 101mb                                                              |----L2.7-----|               "
    - "L2.8[836,1000] 9ns 99mb                                                                             |----L2.8----| "
    "###
    );
}

// Two similar size files with total size larger than max compact size with small overlap range
// The overlapped range is at the end range of start_level file and start of target level file
// Two files have similar length of time range
#[tokio::test]
async fn two_large_files_total_over_max_compact_size_small_overlap_range_2() {
    test_helpers::maybe_start_logging();

    let setup = layout_setup_builder()
        .await
        .with_max_desired_file_size_bytes(MAX_DESIRED_FILE_SIZE)
        .build()
        .await;

    let max_compact_size = setup.config.max_compact_size_bytes();
    let size = max_compact_size / 2 + 10;

    for i in 1..=2 {
        setup
            .partition
            .create_parquet_file(
                parquet_builder()
                    .with_min_time(i * 800)
                    .with_max_time((i + 1) * 1000)
                    // L1.1 or  L2.2
                    .with_compaction_level(CompactionLevel::try_from(i as i32).unwrap())
                    // max_l0_created_at of larger level is set to be smaller
                    .with_max_l0_created_at(Time::from_timestamp_nanos(10 - i))
                    .with_file_size_bytes(size as u64),
            )
            .await;
    }

    insta::assert_yaml_snapshot!(
        run_layout_scenario(&setup).await,
        @r###"
    ---
    - "**** Input Files "
    - "L1, all files 150mb                                                                                                "
    - "L1.1[800,2000] 9ns       |---------------------L1.1----------------------|                                         "
    - "L2, all files 150mb                                                                                                "
    - "L2.2[1600,3000] 8ns                                      |-------------------------L2.2--------------------------| "
    - "WARNING: file L1.1[800,2000] 9ns 150mb exceeds soft limit 100mb by more than 50%"
    - "WARNING: file L2.2[1600,3000] 8ns 150mb exceeds soft limit 100mb by more than 50%"
    - "**** Simulation run 0, type=split(ReduceLargeFileSize)(split_times=[1600]). 1 Input Files, 150mb total:"
    - "L1, all files 150mb                                                                                                "
    - "L1.1[800,2000] 9ns       |------------------------------------------L1.1------------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 150mb total:"
    - "L1                                                                                                                 "
    - "L1.?[800,1600] 9ns 100mb |---------------------------L1.?---------------------------|                              "
    - "L1.?[1601,2000] 9ns 50mb                                                             |-----------L1.?------------| "
    - "**** Simulation run 1, type=split(ReduceLargeFileSize)(split_times=[2534]). 1 Input Files, 150mb total:"
    - "L2, all files 150mb                                                                                                "
    - "L2.2[1600,3000] 8ns      |------------------------------------------L2.2------------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 150mb total:"
    - "L2                                                                                                                 "
    - "L2.?[1600,2534] 8ns 100mb|---------------------------L2.?---------------------------|                              "
    - "L2.?[2535,3000] 8ns 50mb                                                             |-----------L2.?------------| "
    - "Committing partition 1:"
    - "  Soft Deleting 2 files: L1.1, L2.2"
    - "  Creating 4 files"
    - "**** Simulation run 2, type=split(CompactAndSplitOutput(TotalSizeLessThanMaxCompactSize))(split_times=[1494, 2188]). 3 Input Files, 250mb total:"
    - "L1                                                                                                                 "
    - "L1.4[1601,2000] 9ns 50mb                                          |-------L1.4-------|                             "
    - "L1.3[800,1600] 9ns 100mb |-----------------L1.3------------------|                                                 "
    - "L2                                                                                                                 "
    - "L2.5[1600,2534] 8ns 100mb                                         |---------------------L2.5---------------------| "
    - "**** 3 Output Files (parquet_file_id not yet assigned), 250mb total:"
    - "L2                                                                                                                 "
    - "L2.?[800,1494] 9ns 100mb |---------------L2.?---------------|                                                      "
    - "L2.?[1495,2188] 9ns 100mb                                    |--------------L2.?---------------|                   "
    - "L2.?[2189,2534] 9ns 50mb                                                                         |-----L2.?------| "
    - "Committing partition 1:"
    - "  Soft Deleting 3 files: L1.3, L1.4, L2.5"
    - "  Creating 3 files"
    - "**** Final Output Files (550mb written)"
    - "L2                                                                                                                 "
    - "L2.6[2535,3000] 8ns 50mb                                                                       |------L2.6-------| "
    - "L2.7[800,1494] 9ns 100mb |-----------L2.7-----------|                                                              "
    - "L2.8[1495,2188] 9ns 100mb                            |-----------L2.8-----------|                                  "
    - "L2.9[2189,2534] 9ns 50mb                                                         |----L2.9----|                    "
    "###
    );
}

// Two similar size files with total size larger than max compact size with small overlap range
// The overlapped range is at the end range of start_level file and start of target level file
// Time range of the start level file is much smaller than the one of target level file
#[tokio::test]
async fn two_large_files_total_over_max_compact_size_small_overlap_range_3() {
    test_helpers::maybe_start_logging();

    let setup = layout_setup_builder()
        .await
        .with_max_desired_file_size_bytes(MAX_DESIRED_FILE_SIZE)
        .build()
        .await;

    let max_compact_size = setup.config.max_compact_size_bytes();
    let size = max_compact_size / 2 + 10;

    for i in 1..=2 {
        setup
            .partition
            .create_parquet_file(
                parquet_builder()
                    .with_min_time((i - 1) * 200)
                    .with_max_time((i - 1) * 1000 + 300)
                    // L1.1 or  L2.2
                    .with_compaction_level(CompactionLevel::try_from(i as i32).unwrap())
                    // max_l0_created_at of larger level is set to be smaller
                    .with_max_l0_created_at(Time::from_timestamp_nanos(10 - i))
                    .with_file_size_bytes(size as u64),
            )
            .await;
    }

    insta::assert_yaml_snapshot!(
        run_layout_scenario(&setup).await,
        @r###"
    ---
    - "**** Input Files "
    - "L1, all files 150mb                                                                                                "
    - "L1.1[0,300] 9ns          |-------L1.1-------|                                                                      "
    - "L2, all files 150mb                                                                                                "
    - "L2.2[200,1300] 8ns                    |-----------------------------------L2.2-----------------------------------| "
    - "WARNING: file L1.1[0,300] 9ns 150mb exceeds soft limit 100mb by more than 50%"
    - "WARNING: file L2.2[200,1300] 8ns 150mb exceeds soft limit 100mb by more than 50%"
    - "**** Simulation run 0, type=split(ReduceLargeFileSize)(split_times=[200]). 1 Input Files, 150mb total:"
    - "L1, all files 150mb                                                                                                "
    - "L1.1[0,300] 9ns          |------------------------------------------L1.1------------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 150mb total:"
    - "L1                                                                                                                 "
    - "L1.?[0,200] 9ns 100mb    |---------------------------L1.?---------------------------|                              "
    - "L1.?[201,300] 9ns 50mb                                                               |-----------L1.?------------| "
    - "**** Simulation run 1, type=split(ReduceLargeFileSize)(split_times=[934]). 1 Input Files, 150mb total:"
    - "L2, all files 150mb                                                                                                "
    - "L2.2[200,1300] 8ns       |------------------------------------------L2.2------------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 150mb total:"
    - "L2                                                                                                                 "
    - "L2.?[200,934] 8ns 100mb  |---------------------------L2.?---------------------------|                              "
    - "L2.?[935,1300] 8ns 50mb                                                              |-----------L2.?------------| "
    - "Committing partition 1:"
    - "  Soft Deleting 2 files: L1.1, L2.2"
    - "  Creating 4 files"
    - "**** Simulation run 2, type=split(CompactAndSplitOutput(TotalSizeLessThanMaxCompactSize))(split_times=[374, 748]). 3 Input Files, 250mb total:"
    - "L1                                                                                                                 "
    - "L1.4[201,300] 9ns 50mb                      |-L1.4--|                                                              "
    - "L1.3[0,200] 9ns 100mb    |------L1.3-------|                                                                       "
    - "L2                                                                                                                 "
    - "L2.5[200,934] 8ns 100mb                     |--------------------------------L2.5--------------------------------| "
    - "**** 3 Output Files (parquet_file_id not yet assigned), 250mb total:"
    - "L2                                                                                                                 "
    - "L2.?[0,374] 9ns 100mb    |---------------L2.?---------------|                                                      "
    - "L2.?[375,748] 9ns 100mb                                      |--------------L2.?---------------|                   "
    - "L2.?[749,934] 9ns 50mb                                                                           |-----L2.?------| "
    - "Committing partition 1:"
    - "  Soft Deleting 3 files: L1.3, L1.4, L2.5"
    - "  Creating 3 files"
    - "**** Final Output Files (550mb written)"
    - "L2                                                                                                                 "
    - "L2.6[935,1300] 8ns 50mb                                                                  |---------L2.6----------| "
    - "L2.7[0,374] 9ns 100mb    |---------L2.7----------|                                                                 "
    - "L2.8[375,748] 9ns 100mb                           |---------L2.8----------|                                        "
    - "L2.9[749,934] 9ns 50mb                                                      |---L2.9---|                           "
    "###
    );
}

// Two similar size files with total size larger than max compact size and similar time range
// Start level is 0
#[tokio::test]
async fn two_large_files_total_over_max_compact_size_start_l0() {
    test_helpers::maybe_start_logging();

    let setup = layout_setup_builder()
        .await
        .with_max_desired_file_size_bytes(MAX_DESIRED_FILE_SIZE)
        .build()
        .await;

    let max_compact_size = setup.config.max_compact_size_bytes();
    let size = max_compact_size / 2 + 10;

    for i in 0..=1 {
        setup
            .partition
            .create_parquet_file(
                parquet_builder()
                    .with_min_time(i)
                    .with_max_time(1000)
                    // time of L0 larger than time of L1
                    .with_max_l0_created_at(Time::from_timestamp_nanos(10 - i))
                    // L0.1 or  L1.2
                    .with_compaction_level(CompactionLevel::try_from(i as i32).unwrap())
                    .with_file_size_bytes(size as u64),
            )
            .await;
    }

    insta::assert_yaml_snapshot!(
        run_layout_scenario(&setup).await,
        @r###"
    ---
    - "**** Input Files "
    - "L0, all files 150mb                                                                                                "
    - "L0.1[0,1000] 10ns        |------------------------------------------L0.1------------------------------------------|"
    - "L1, all files 150mb                                                                                                "
    - "L1.2[1,1000] 9ns         |-----------------------------------------L1.2------------------------------------------| "
    - "WARNING: file L0.1[0,1000] 10ns 150mb exceeds soft limit 100mb by more than 50%"
    - "WARNING: file L1.2[1,1000] 9ns 150mb exceeds soft limit 100mb by more than 50%"
    - "**** Simulation run 0, type=split(ReduceLargeFileSize)(split_times=[667]). 1 Input Files, 150mb total:"
    - "L0, all files 150mb                                                                                                "
    - "L0.1[0,1000] 10ns        |------------------------------------------L0.1------------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 150mb total:"
    - "L0                                                                                                                 "
    - "L0.?[0,667] 10ns 100mb   |---------------------------L0.?---------------------------|                              "
    - "L0.?[668,1000] 10ns 50mb                                                             |-----------L0.?------------| "
    - "**** Simulation run 1, type=split(ReduceLargeFileSize)(split_times=[667]). 1 Input Files, 150mb total:"
    - "L1, all files 150mb                                                                                                "
    - "L1.2[1,1000] 9ns         |------------------------------------------L1.2------------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 150mb total:"
    - "L1                                                                                                                 "
    - "L1.?[1,667] 9ns 100mb    |---------------------------L1.?---------------------------|                              "
    - "L1.?[668,1000] 9ns 50mb                                                              |-----------L1.?------------| "
    - "Committing partition 1:"
    - "  Soft Deleting 2 files: L0.1, L1.2"
    - "  Creating 4 files"
    - "**** Simulation run 2, type=split(CompactAndSplitOutput(TotalSizeLessThanMaxCompactSize))(split_times=[334]). 2 Input Files, 200mb total:"
    - "L0                                                                                                                 "
    - "L0.3[0,667] 10ns 100mb   |------------------------------------------L0.3------------------------------------------|"
    - "L1                                                                                                                 "
    - "L1.5[1,667] 9ns 100mb    |-----------------------------------------L1.5------------------------------------------| "
    - "**** 2 Output Files (parquet_file_id not yet assigned), 200mb total:"
    - "L1                                                                                                                 "
    - "L1.?[0,334] 10ns 100mb   |-------------------L1.?--------------------|                                             "
    - "L1.?[335,667] 10ns 100mb                                              |-------------------L1.?-------------------| "
    - "Committing partition 1:"
    - "  Soft Deleting 2 files: L0.3, L1.5"
    - "  Creating 2 files"
    - "**** Simulation run 3, type=split(CompactAndSplitOutput(TotalSizeLessThanMaxCompactSize))(split_times=[933]). 2 Input Files, 100mb total:"
    - "L0                                                                                                                 "
    - "L0.4[668,1000] 10ns 50mb |------------------------------------------L0.4------------------------------------------|"
    - "L1                                                                                                                 "
    - "L1.6[668,1000] 9ns 50mb  |------------------------------------------L1.6------------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 100mb total:"
    - "L1                                                                                                                 "
    - "L1.?[668,933] 10ns 80mb  |--------------------------------L1.?---------------------------------|                   "
    - "L1.?[934,1000] 10ns 20mb                                                                         |-----L1.?------| "
    - "Committing partition 1:"
    - "  Soft Deleting 2 files: L0.4, L1.6"
    - "  Creating 2 files"
    - "**** Simulation run 4, type=split(CompactAndSplitOutput(TotalSizeLessThanMaxCompactSize))(split_times=[986]). 1 Input Files, 20mb total:"
    - "L1, all files 20mb                                                                                                 "
    - "L1.10[934,1000] 10ns     |-----------------------------------------L1.10------------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 20mb total:"
    - "L2                                                                                                                 "
    - "L2.?[934,986] 10ns 16mb  |--------------------------------L2.?--------------------------------|                    "
    - "L2.?[987,1000] 10ns 4mb                                                                          |-----L2.?------| "
    - "Committing partition 1:"
    - "  Soft Deleting 1 files: L1.10"
    - "  Upgrading 3 files level to CompactionLevel::L2: L1.7, L1.8, L1.9"
    - "  Creating 2 files"
    - "**** Final Output Files (620mb written)"
    - "L2                                                                                                                 "
    - "L2.7[0,334] 10ns 100mb   |------------L2.7------------|                                                            "
    - "L2.8[335,667] 10ns 100mb                               |-----------L2.8------------|                               "
    - "L2.9[668,933] 10ns 80mb                                                              |--------L2.9---------|       "
    - "L2.11[934,986] 10ns 16mb                                                                                     |L2.11|"
    - "L2.12[987,1000] 10ns 4mb                                                                                         |L2.12|"
    "###
    );

    // Read all files including the soft deleted ones
    let output_files = setup.list_by_table().await;
    assert_eq!(output_files.len(), 12);

    // Sort the files by id
    let mut output_files = output_files;
    output_files.sort_by(|a, b| a.id.cmp(&b.id));

    // Verify all L0 files created by splitting must have  value of max_l0_created_at 10 which is the value of the original L0
    // Note: this test make created_test deterministic and 1 which we do not care much about
    for file in &output_files {
        if file.compaction_level == CompactionLevel::Initial {
            assert_eq!(file.max_l0_created_at.get(), 10);
        }
    }
}

// Real-life case with three good size L1s and one very large L2
#[tokio::test]
async fn target_too_large_1() {
    test_helpers::maybe_start_logging();

    let setup = layout_setup_builder()
        .await
        .with_max_desired_file_size_bytes(MAX_DESIRED_FILE_SIZE)
        .build()
        .await;

    // Real-life case 1
    //   . three L1s with total > max_desired_file_size_bytes to trigger compaction
    //   . one very large overlapped L2

    // size of l1s & l2
    let l1_sizes = [53 * ONE_MB, 45 * ONE_MB, 5 * ONE_MB];
    let l2_size = 253 * ONE_MB;

    // L2 overlapped with the first L1
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1)
                .with_max_time(1000)
                .with_compaction_level(CompactionLevel::Final)
                // level-2 file has small max_l0_created_at, 5
                .with_max_l0_created_at(Time::from_timestamp_nanos(5))
                .with_file_size_bytes(l2_size),
        )
        .await;

    // L1s
    for i in 0..=2 {
        setup
            .partition
            .create_parquet_file(
                parquet_builder()
                    .with_min_time(i * 1000 + 1)
                    .with_max_time(i * 1000 + 1000)
                    .with_compaction_level(CompactionLevel::FileNonOverlapped)
                    // level-1 files, each has different max_l0_created_at and >= 10
                    // simulate to have L1 with larger time range having smaller max_l0_created_at which is a rare use case
                    .with_max_l0_created_at(Time::from_timestamp_nanos(10 + (10 - i)))
                    .with_file_size_bytes(l1_sizes[i as usize]),
            )
            .await;
    }

    insta::assert_yaml_snapshot!(
        run_layout_scenario(&setup).await,
        @r###"
    ---
    - "**** Input Files "
    - "L1                                                                                                                 "
    - "L1.2[1,1000] 20ns 53mb   |-----------L1.2------------|                                                             "
    - "L1.3[1001,2000] 19ns 45mb                              |-----------L1.3------------|                               "
    - "L1.4[2001,3000] 18ns 5mb                                                             |-----------L1.4------------| "
    - "L2                                                                                                                 "
    - "L2.1[1,1000] 5ns 253mb   |-----------L2.1------------|                                                             "
    - "WARNING: file L2.1[1,1000] 5ns 253mb exceeds soft limit 100mb by more than 50%"
    - "**** Simulation run 0, type=split(ReduceLargeFileSize)(split_times=[396, 791]). 1 Input Files, 253mb total:"
    - "L2, all files 253mb                                                                                                "
    - "L2.1[1,1000] 5ns         |------------------------------------------L2.1------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 253mb total:"
    - "L2                                                                                                                 "
    - "L2.?[1,396] 5ns 100mb    |--------------L2.?---------------|                                                       "
    - "L2.?[397,791] 5ns 100mb                                     |--------------L2.?---------------|                    "
    - "L2.?[792,1000] 5ns 53mb                                                                         |------L2.?------| "
    - "Committing partition 1:"
    - "  Soft Deleting 1 files: L2.1"
    - "  Creating 3 files"
    - "**** Simulation run 1, type=split(ReduceOverlap)(split_times=[396, 791]). 1 Input Files, 53mb total:"
    - "L1, all files 53mb                                                                                                 "
    - "L1.2[1,1000] 20ns        |------------------------------------------L1.2------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 53mb total:"
    - "L1                                                                                                                 "
    - "L1.?[1,396] 20ns 21mb    |--------------L1.?---------------|                                                       "
    - "L1.?[397,791] 20ns 21mb                                     |--------------L1.?---------------|                    "
    - "L1.?[792,1000] 20ns 11mb                                                                        |------L1.?------| "
    - "Committing partition 1:"
    - "  Soft Deleting 1 files: L1.2"
    - "  Creating 3 files"
    - "**** Simulation run 2, type=split(CompactAndSplitOutput(FoundSubsetLessThanMaxCompactSize))(split_times=[328, 655]). 4 Input Files, 242mb total:"
    - "L1                                                                                                                 "
    - "L1.8[1,396] 20ns 21mb    |-------------------L1.8-------------------|                                              "
    - "L1.9[397,791] 20ns 21mb                                               |-------------------L1.9-------------------| "
    - "L2                                                                                                                 "
    - "L2.5[1,396] 5ns 100mb    |-------------------L2.5-------------------|                                              "
    - "L2.6[397,791] 5ns 100mb                                               |-------------------L2.6-------------------| "
    - "**** 3 Output Files (parquet_file_id not yet assigned), 242mb total:"
    - "L2                                                                                                                 "
    - "L2.?[1,328] 20ns 100mb   |---------------L2.?----------------|                                                     "
    - "L2.?[329,655] 20ns 100mb                                      |---------------L2.?----------------|                "
    - "L2.?[656,791] 20ns 42mb                                                                            |----L2.?-----| "
    - "Committing partition 1:"
    - "  Soft Deleting 4 files: L2.5, L2.6, L1.8, L1.9"
    - "  Creating 3 files"
    - "**** Final Output Files (548mb written)"
    - "L1                                                                                                                 "
    - "L1.3[1001,2000] 19ns 45mb                              |-----------L1.3------------|                               "
    - "L1.4[2001,3000] 18ns 5mb                                                             |-----------L1.4------------| "
    - "L1.10[792,1000] 20ns 11mb                       |L1.10|                                                            "
    - "L2                                                                                                                 "
    - "L2.7[792,1000] 5ns 53mb                         |L2.7|                                                             "
    - "L2.11[1,328] 20ns 100mb  |-L2.11-|                                                                                 "
    - "L2.12[329,655] 20ns 100mb         |-L2.12-|                                                                        "
    - "L2.13[656,791] 20ns 42mb                    |L2.13|                                                                "
    "###
    );
}

// Real-life case with two good size L1s and one very large L2
#[tokio::test]
async fn target_too_large_2() {
    test_helpers::maybe_start_logging();

    let setup = layout_setup_builder()
        .await
        .with_max_desired_file_size_bytes(MAX_DESIRED_FILE_SIZE)
        .build()
        .await;

    // Real-life case 2
    //   . two L1s with total > max_desired_file_size_bytes to trigger compaction
    //   . one very large overlapped L2

    // size of l1s & l2
    let l1_sizes = [69 * ONE_MB, 50 * ONE_MB];
    let l2_size = 232 * ONE_MB;

    // L2 overlapped with both L1s
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1)
                .with_max_time(3000)
                .with_compaction_level(CompactionLevel::Final)
                // level-2 file has small max_l0_created_at
                .with_max_l0_created_at(Time::from_timestamp_nanos(5))
                .with_file_size_bytes(l2_size),
        )
        .await;

    // L1s
    for i in 0..=1 {
        setup
            .partition
            .create_parquet_file(
                parquet_builder()
                    .with_min_time(i * 1000 + 1)
                    .with_max_time(i * 1000 + 1000)
                    .with_compaction_level(CompactionLevel::FileNonOverlapped)
                    // level-1 each has different max_l0_created_at and larger than level-2 one
                    // set smaller time range wiht smaller max_l0_created_at which is the common use case
                    .with_max_l0_created_at(Time::from_timestamp_nanos(10 + i))
                    .with_file_size_bytes(l1_sizes[i as usize]),
            )
            .await;
    }

    insta::assert_yaml_snapshot!(
        run_layout_scenario(&setup).await,
        @r###"
    ---
    - "**** Input Files "
    - "L1                                                                                                                 "
    - "L1.2[1,1000] 10ns 69mb   |-----------L1.2------------|                                                             "
    - "L1.3[1001,2000] 11ns 50mb                              |-----------L1.3------------|                               "
    - "L2                                                                                                                 "
    - "L2.1[1,3000] 5ns 232mb   |------------------------------------------L2.1------------------------------------------|"
    - "WARNING: file L2.1[1,3000] 5ns 232mb exceeds soft limit 100mb by more than 50%"
    - "**** Simulation run 0, type=split(StartLevelOverlapsTooBig)(split_times=[1001]). 1 Input Files, 232mb total:"
    - "L2, all files 232mb                                                                                                "
    - "L2.1[1,3000] 5ns         |------------------------------------------L2.1------------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 232mb total:"
    - "L2                                                                                                                 "
    - "L2.?[1,1001] 5ns 77mb    |------------L2.?------------|                                                            "
    - "L2.?[1002,3000] 5ns 155mb                              |--------------------------L2.?---------------------------| "
    - "Committing partition 1:"
    - "  Soft Deleting 1 files: L2.1"
    - "  Creating 2 files"
    - "**** Simulation run 1, type=split(ReduceOverlap)(split_times=[1001]). 1 Input Files, 50mb total:"
    - "L1, all files 50mb                                                                                                 "
    - "L1.3[1001,2000] 11ns     |------------------------------------------L1.3------------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 50mb total:"
    - "L1                                                                                                                 "
    - "L1.?[1001,1001] 11ns 51kb|L1.?|                                                                                    "
    - "L1.?[1002,2000] 11ns 50mb|-----------------------------------------L1.?------------------------------------------| "
    - "Committing partition 1:"
    - "  Soft Deleting 1 files: L1.3"
    - "  Creating 2 files"
    - "**** Simulation run 2, type=split(CompactAndSplitOutput(FoundSubsetLessThanMaxCompactSize))(split_times=[684]). 3 Input Files, 146mb total:"
    - "L1                                                                                                                 "
    - "L1.2[1,1000] 10ns 69mb   |-----------------------------------------L1.2------------------------------------------| "
    - "L1.6[1001,1001] 11ns 51kb                                                                                          |L1.6|"
    - "L2                                                                                                                 "
    - "L2.4[1,1001] 5ns 77mb    |------------------------------------------L2.4------------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 146mb total:"
    - "L2                                                                                                                 "
    - "L2.?[1,684] 11ns 100mb   |---------------------------L2.?----------------------------|                             "
    - "L2.?[685,1001] 11ns 46mb                                                              |-----------L2.?-----------| "
    - "Committing partition 1:"
    - "  Soft Deleting 3 files: L1.2, L2.4, L1.6"
    - "  Creating 2 files"
    - "**** Final Output Files (428mb written)"
    - "L1                                                                                                                 "
    - "L1.7[1002,2000] 11ns 50mb                              |-----------L1.7------------|                               "
    - "L2                                                                                                                 "
    - "L2.5[1002,3000] 5ns 155mb                              |--------------------------L2.5---------------------------| "
    - "L2.8[1,684] 11ns 100mb   |-------L2.8-------|                                                                      "
    - "L2.9[685,1001] 11ns 46mb                     |-L2.9--|                                                             "
    - "WARNING: file L2.5[1002,3000] 5ns 155mb exceeds soft limit 100mb by more than 50%"
    "###
    );
}

// One very large start level file with one good size overlapped target level file
// Two have similar time range
#[tokio::test]
async fn start_too_large_similar_time_range() {
    test_helpers::maybe_start_logging();

    let setup = layout_setup_builder()
        .await
        .with_max_desired_file_size_bytes(MAX_DESIRED_FILE_SIZE)
        .build()
        .await;

    //   . one L1 >>  max_desired_file_size_bytes to trigger compaction
    //   . one good size overlapped L2
    //   . total size = L1 & L2 > max_compact_size

    // size of l1 & l2 respectively
    let sizes = [250 * ONE_MB, 52 * ONE_MB];

    for i in 1..=2 {
        setup
            .partition
            .create_parquet_file(
                parquet_builder()
                    .with_min_time(i)
                    .with_max_time(1000)
                    // L1.1 or L2.2
                    .with_compaction_level(CompactionLevel::try_from(i as i32).unwrap())
                    // max_l0_created_at of larger level is set to be smaller
                    .with_max_l0_created_at(Time::from_timestamp_nanos(10 - i))
                    .with_file_size_bytes(sizes[(i - 1) as usize]),
            )
            .await;
    }

    insta::assert_yaml_snapshot!(
        run_layout_scenario(&setup).await,
        @r###"
    ---
    - "**** Input Files "
    - "L1                                                                                                                 "
    - "L1.1[1,1000] 9ns 250mb   |------------------------------------------L1.1------------------------------------------|"
    - "L2                                                                                                                 "
    - "L2.2[2,1000] 8ns 52mb    |-----------------------------------------L2.2------------------------------------------| "
    - "WARNING: file L1.1[1,1000] 9ns 250mb exceeds soft limit 100mb by more than 50%"
    - "**** Simulation run 0, type=split(ReduceLargeFileSize)(split_times=[401, 801]). 1 Input Files, 250mb total:"
    - "L1, all files 250mb                                                                                                "
    - "L1.1[1,1000] 9ns         |------------------------------------------L1.1------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 250mb total:"
    - "L1                                                                                                                 "
    - "L1.?[1,401] 9ns 100mb    |---------------L1.?---------------|                                                      "
    - "L1.?[402,801] 9ns 100mb                                      |--------------L1.?---------------|                   "
    - "L1.?[802,1000] 9ns 50mb                                                                          |-----L1.?------| "
    - "Committing partition 1:"
    - "  Soft Deleting 1 files: L1.1"
    - "  Creating 3 files"
    - "**** Simulation run 1, type=split(StartLevelOverlapsTooBig)(split_times=[402, 802]). 1 Input Files, 52mb total:"
    - "L2, all files 52mb                                                                                                 "
    - "L2.2[2,1000] 8ns         |------------------------------------------L2.2------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 52mb total:"
    - "L2                                                                                                                 "
    - "L2.?[2,402] 8ns 21mb     |---------------L2.?---------------|                                                      "
    - "L2.?[403,802] 8ns 21mb                                       |--------------L2.?---------------|                   "
    - "L2.?[803,1000] 8ns 10mb                                                                          |-----L2.?------| "
    - "Committing partition 1:"
    - "  Soft Deleting 1 files: L2.2"
    - "  Creating 3 files"
    - "**** Simulation run 2, type=split(ReduceOverlap)(split_times=[802]). 1 Input Files, 50mb total:"
    - "L1, all files 50mb                                                                                                 "
    - "L1.5[802,1000] 9ns       |-----------------------------------------L1.5------------------------------------------| "
    - "**** 2 Output Files (parquet_file_id not yet assigned), 50mb total:"
    - "L1                                                                                                                 "
    - "L1.?[802,802] 9ns 256kb  |L1.?|                                                                                    "
    - "L1.?[803,1000] 9ns 50mb  |-----------------------------------------L1.?------------------------------------------| "
    - "**** Simulation run 3, type=split(ReduceOverlap)(split_times=[402]). 1 Input Files, 100mb total:"
    - "L1, all files 100mb                                                                                                "
    - "L1.4[402,801] 9ns        |------------------------------------------L1.4------------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 100mb total:"
    - "L1                                                                                                                 "
    - "L1.?[402,402] 9ns 256kb  |L1.?|                                                                                    "
    - "L1.?[403,801] 9ns 100mb  |-----------------------------------------L1.?------------------------------------------| "
    - "Committing partition 1:"
    - "  Soft Deleting 2 files: L1.4, L1.5"
    - "  Creating 4 files"
    - "**** Simulation run 4, type=split(CompactAndSplitOutput(FoundSubsetLessThanMaxCompactSize))(split_times=[332, 663]). 6 Input Files, 242mb total:"
    - "L1                                                                                                                 "
    - "L1.3[1,401] 9ns 100mb    |-------------------L1.3-------------------|                                              "
    - "L1.11[402,402] 9ns 256kb                                              |L1.11|                                      "
    - "L1.12[403,801] 9ns 100mb                                              |------------------L1.12-------------------| "
    - "L1.9[802,802] 9ns 256kb                                                                                            |L1.9|"
    - "L2                                                                                                                 "
    - "L2.6[2,402] 8ns 21mb     |-------------------L2.6-------------------|                                              "
    - "L2.7[403,802] 8ns 21mb                                                |-------------------L2.7-------------------| "
    - "**** 3 Output Files (parquet_file_id not yet assigned), 242mb total:"
    - "L2                                                                                                                 "
    - "L2.?[1,332] 9ns 100mb    |---------------L2.?----------------|                                                     "
    - "L2.?[333,663] 9ns 100mb                                       |---------------L2.?----------------|                "
    - "L2.?[664,802] 9ns 42mb                                                                             |----L2.?-----| "
    - "Committing partition 1:"
    - "  Soft Deleting 6 files: L1.3, L2.6, L2.7, L1.9, L1.11, L1.12"
    - "  Creating 3 files"
    - "**** Final Output Files (694mb written)"
    - "L1                                                                                                                 "
    - "L1.10[803,1000] 9ns 50mb                                                                         |-----L1.10-----| "
    - "L2                                                                                                                 "
    - "L2.8[803,1000] 8ns 10mb                                                                          |-----L2.8------| "
    - "L2.13[1,332] 9ns 100mb   |-----------L2.13-----------|                                                             "
    - "L2.14[333,663] 9ns 100mb                              |-----------L2.14-----------|                                "
    - "L2.15[664,802] 9ns 42mb                                                             |--L2.15---|                   "
    "###
    );
}

// One very large start level file with one good size overlapped target level file
// Overlapped range is small
// The overlapped range is at the end of both start_level file and target level file
#[tokio::test]
async fn start_too_large_small_time_range() {
    test_helpers::maybe_start_logging();

    let setup = layout_setup_builder()
        .await
        .with_max_desired_file_size_bytes(MAX_DESIRED_FILE_SIZE)
        .build()
        .await;

    //   . one L1 >>  max_desired_file_size_bytes to trigger compaction
    //   . one good size overlapped L2
    //   . total size = L1 & L2 > max_compact_size

    // size of l1 & l2 respectively
    let sizes = [250 * ONE_MB, 52 * ONE_MB];

    for i in 1..=2 {
        setup
            .partition
            .create_parquet_file(
                parquet_builder()
                    .with_min_time((i - 1) * 800)
                    .with_max_time(1000)
                    // L1.1 or L2.2
                    .with_compaction_level(CompactionLevel::try_from(i as i32).unwrap())
                    // max_l0_created_at of larger level is set to be smaller
                    .with_max_l0_created_at(Time::from_timestamp_nanos(10 - i))
                    .with_file_size_bytes(sizes[(i - 1) as usize]),
            )
            .await;
    }

    insta::assert_yaml_snapshot!(
        run_layout_scenario(&setup).await,
        @r###"
    ---
    - "**** Input Files "
    - "L1                                                                                                                 "
    - "L1.1[0,1000] 9ns 250mb   |------------------------------------------L1.1------------------------------------------|"
    - "L2                                                                                                                 "
    - "L2.2[800,1000] 8ns 52mb                                                                          |------L2.2------|"
    - "WARNING: file L1.1[0,1000] 9ns 250mb exceeds soft limit 100mb by more than 50%"
    - "**** Simulation run 0, type=split(ReduceLargeFileSize)(split_times=[400, 800]). 1 Input Files, 250mb total:"
    - "L1, all files 250mb                                                                                                "
    - "L1.1[0,1000] 9ns         |------------------------------------------L1.1------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 250mb total:"
    - "L1                                                                                                                 "
    - "L1.?[0,400] 9ns 100mb    |---------------L1.?---------------|                                                      "
    - "L1.?[401,800] 9ns 100mb                                      |--------------L1.?---------------|                   "
    - "L1.?[801,1000] 9ns 50mb                                                                          |-----L1.?------| "
    - "Committing partition 1:"
    - "  Soft Deleting 1 files: L1.1"
    - "  Creating 3 files"
    - "**** Simulation run 1, type=split(CompactAndSplitOutput(TotalSizeLessThanMaxCompactSize))(split_times=[698, 995]). 3 Input Files, 202mb total:"
    - "L1                                                                                                                 "
    - "L1.5[801,1000] 9ns 50mb                                                              |-----------L1.5------------| "
    - "L1.4[401,800] 9ns 100mb  |--------------------------L1.4---------------------------|                               "
    - "L2                                                                                                                 "
    - "L2.2[800,1000] 8ns 52mb                                                             |------------L2.2------------| "
    - "**** 3 Output Files (parquet_file_id not yet assigned), 202mb total:"
    - "L2                                                                                                                 "
    - "L2.?[401,698] 9ns 100mb  |-------------------L2.?-------------------|                                              "
    - "L2.?[699,995] 9ns 100mb                                              |-------------------L2.?-------------------|  "
    - "L2.?[996,1000] 9ns 2mb                                                                                            |L2.?|"
    - "Committing partition 1:"
    - "  Soft Deleting 3 files: L2.2, L1.4, L1.5"
    - "  Upgrading 1 files level to CompactionLevel::L2: L1.3"
    - "  Creating 3 files"
    - "**** Final Output Files (452mb written)"
    - "L2                                                                                                                 "
    - "L2.3[0,400] 9ns 100mb    |---------------L2.3---------------|                                                      "
    - "L2.6[401,698] 9ns 100mb                                      |----------L2.6----------|                            "
    - "L2.7[699,995] 9ns 100mb                                                                |----------L2.7----------|  "
    - "L2.8[996,1000] 9ns 2mb                                                                                            |L2.8|"
    "###
    );
}

// One very large start level file with one good size overlapped target level file
// Overlapped range is small
// The overlapped range is at the end of start_level file and start of target level file
#[tokio::test]
async fn start_too_large_small_time_range_2() {
    test_helpers::maybe_start_logging();

    let setup = layout_setup_builder()
        .await
        .with_max_desired_file_size_bytes(MAX_DESIRED_FILE_SIZE)
        .build()
        .await;

    //   . one L1 >>  max_desired_file_size_bytes to trigger compaction
    //   . one good size overlapped L2
    //   . total size = L1 & L2 > max_compact_size

    // size of l1 & l2 respectively
    let sizes = [250 * ONE_MB, 52 * ONE_MB];

    for i in 1..=2 {
        setup
            .partition
            .create_parquet_file(
                parquet_builder()
                    .with_min_time(i * 800)
                    .with_max_time((i + 1) * 1000)
                    // L1.1 or L2.2
                    .with_compaction_level(CompactionLevel::try_from(i as i32).unwrap())
                    .with_file_size_bytes(sizes[(i - 1) as usize]),
            )
            .await;
    }

    insta::assert_yaml_snapshot!(
        run_layout_scenario(&setup).await,
        @r###"
    ---
    - "**** Input Files "
    - "L1                                                                                                                 "
    - "L1.1[800,2000] 1ns 250mb |---------------------L1.1----------------------|                                         "
    - "L2                                                                                                                 "
    - "L2.2[1600,3000] 1ns 52mb                                 |-------------------------L2.2--------------------------| "
    - "WARNING: file L1.1[800,2000] 1ns 250mb exceeds soft limit 100mb by more than 50%"
    - "**** Simulation run 0, type=split(ReduceLargeFileSize)(split_times=[1280, 1760]). 1 Input Files, 250mb total:"
    - "L1, all files 250mb                                                                                                "
    - "L1.1[800,2000] 1ns       |------------------------------------------L1.1------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 250mb total:"
    - "L1                                                                                                                 "
    - "L1.?[800,1280] 1ns 100mb |---------------L1.?---------------|                                                      "
    - "L1.?[1281,1760] 1ns 100mb                                    |--------------L1.?---------------|                   "
    - "L1.?[1761,2000] 1ns 50mb                                                                         |-----L1.?------| "
    - "Committing partition 1:"
    - "  Soft Deleting 1 files: L1.1"
    - "  Creating 3 files"
    - "**** Simulation run 1, type=split(CompactAndSplitOutput(TotalSizeLessThanMaxCompactSize))(split_times=[2133, 2985]). 3 Input Files, 202mb total:"
    - "L1                                                                                                                 "
    - "L1.5[1761,2000] 1ns 50mb                          |---L1.5---|                                                     "
    - "L1.4[1281,1760] 1ns 100mb|---------L1.4----------|                                                                 "
    - "L2                                                                                                                 "
    - "L2.2[1600,3000] 1ns 52mb                 |---------------------------------L2.2----------------------------------| "
    - "**** 3 Output Files (parquet_file_id not yet assigned), 202mb total:"
    - "L2                                                                                                                 "
    - "L2.?[1281,2133] 1ns 100mb|-------------------L2.?-------------------|                                              "
    - "L2.?[2134,2985] 1ns 100mb                                            |-------------------L2.?-------------------|  "
    - "L2.?[2986,3000] 1ns 2mb                                                                                           |L2.?|"
    - "Committing partition 1:"
    - "  Soft Deleting 3 files: L2.2, L1.4, L1.5"
    - "  Upgrading 1 files level to CompactionLevel::L2: L1.3"
    - "  Creating 3 files"
    - "**** Final Output Files (452mb written)"
    - "L2                                                                                                                 "
    - "L2.3[800,1280] 1ns 100mb |------L2.3-------|                                                                       "
    - "L2.6[1281,2133] 1ns 100mb                   |--------------L2.6--------------|                                     "
    - "L2.7[2134,2985] 1ns 100mb                                                      |--------------L2.7--------------|  "
    - "L2.8[2986,3000] 1ns 2mb                                                                                           |L2.8|"
    "###
    );
}

// One very large start level file with one good size overlapped target level file
// Overlapped range is small
// The overlapped range is at the end of start_level file and start of target level file
// Time range of start level is much larger than target level
#[tokio::test]
async fn start_too_large_small_time_range_3() {
    test_helpers::maybe_start_logging();

    let setup = layout_setup_builder()
        .await
        .with_max_desired_file_size_bytes(MAX_DESIRED_FILE_SIZE)
        .build()
        .await;

    //   . one L1 >>  max_desired_file_size_bytes to trigger compaction
    //   . one good size overlapped L2
    //   . total size = L1 & L2 > max_compact_size

    // size of l1 & l2 respectively
    let sizes = [250 * ONE_MB, 52 * ONE_MB];

    for i in 1..=2 {
        setup
            .partition
            .create_parquet_file(
                parquet_builder()
                    .with_min_time((i - 1) * 200)
                    .with_max_time((i - 1) * 1000 + 300)
                    // L1.1 or L2.2
                    .with_compaction_level(CompactionLevel::try_from(i as i32).unwrap())
                    // max_l0_created_at of larger level is set to be smaller
                    .with_max_l0_created_at(Time::from_timestamp_nanos(10 - i))
                    .with_file_size_bytes(sizes[(i - 1) as usize]),
            )
            .await;
    }

    insta::assert_yaml_snapshot!(
        run_layout_scenario(&setup).await,
        @r###"
    ---
    - "**** Input Files "
    - "L1                                                                                                                 "
    - "L1.1[0,300] 9ns 250mb    |-------L1.1-------|                                                                      "
    - "L2                                                                                                                 "
    - "L2.2[200,1300] 8ns 52mb               |-----------------------------------L2.2-----------------------------------| "
    - "WARNING: file L1.1[0,300] 9ns 250mb exceeds soft limit 100mb by more than 50%"
    - "**** Simulation run 0, type=split(ReduceLargeFileSize)(split_times=[120, 240]). 1 Input Files, 250mb total:"
    - "L1, all files 250mb                                                                                                "
    - "L1.1[0,300] 9ns          |------------------------------------------L1.1------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 250mb total:"
    - "L1                                                                                                                 "
    - "L1.?[0,120] 9ns 100mb    |---------------L1.?---------------|                                                      "
    - "L1.?[121,240] 9ns 100mb                                      |--------------L1.?---------------|                   "
    - "L1.?[241,300] 9ns 50mb                                                                           |-----L1.?------| "
    - "Committing partition 1:"
    - "  Soft Deleting 1 files: L1.1"
    - "  Creating 3 files"
    - "**** Simulation run 1, type=split(CompactAndSplitOutput(TotalSizeLessThanMaxCompactSize))(split_times=[707, 1293]). 3 Input Files, 202mb total:"
    - "L1                                                                                                                 "
    - "L1.5[241,300] 9ns 50mb            |L1.5|                                                                           "
    - "L1.4[121,240] 9ns 100mb  |-L1.4--|                                                                                 "
    - "L2                                                                                                                 "
    - "L2.2[200,1300] 8ns 52mb        |--------------------------------------L2.2---------------------------------------| "
    - "**** 3 Output Files (parquet_file_id not yet assigned), 202mb total:"
    - "L2                                                                                                                 "
    - "L2.?[121,707] 9ns 100mb  |-------------------L2.?-------------------|                                              "
    - "L2.?[708,1293] 9ns 100mb                                             |-------------------L2.?-------------------|  "
    - "L2.?[1294,1300] 9ns 1mb                                                                                           |L2.?|"
    - "Committing partition 1:"
    - "  Soft Deleting 3 files: L2.2, L1.4, L1.5"
    - "  Upgrading 1 files level to CompactionLevel::L2: L1.3"
    - "  Creating 3 files"
    - "**** Final Output Files (452mb written)"
    - "L2                                                                                                                 "
    - "L2.3[0,120] 9ns 100mb    |-L2.3-|                                                                                  "
    - "L2.6[121,707] 9ns 100mb          |-----------------L2.6-----------------|                                          "
    - "L2.7[708,1293] 9ns 100mb                                                  |-----------------L2.7-----------------| "
    - "L2.8[1294,1300] 9ns 1mb                                                                                           |L2.8|"
    "###
    );
}

// tiny time range and cannot split --> skip
#[tokio::test]
async fn tiny_time_range() {
    test_helpers::maybe_start_logging();

    let setup = layout_setup_builder()
        .await
        .with_max_desired_file_size_bytes(MAX_DESIRED_FILE_SIZE)
        .build()
        .await;

    //   . one L1 >>  max_desired_file_size_bytes to trigger compaction
    //   . L1 is tiny time range --> won't be split
    //   . one good size overlapped L2 --> won't be split
    //   . total size = L1 & L2 > max_compact_size

    // size of l1 & l2 respectively
    let l1_size = 250 * ONE_MB;
    let l2_size = 52 * ONE_MB;

    // l1
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1)
                .with_max_time(2)
                .with_compaction_level(CompactionLevel::FileNonOverlapped)
                // L1 file with larger max_l0_created_at
                .with_max_l0_created_at(Time::from_timestamp_nanos(10))
                .with_file_size_bytes(l1_size),
        )
        .await;

    // l2
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1)
                .with_max_time(1000)
                .with_compaction_level(CompactionLevel::Final)
                // L2 file with smaller max_l0_created_at
                .with_max_l0_created_at(Time::from_timestamp_nanos(5))
                .with_file_size_bytes(l2_size),
        )
        .await;

    // Neither L1 nor L2 will be split and lead to skipping compaction
    insta::assert_yaml_snapshot!(
        run_layout_scenario(&setup).await,
        @r###"
    ---
    - "**** Input Files "
    - "L1                                                                                                                 "
    - "L1.1[1,2] 10ns 250mb     |L1.1|                                                                                    "
    - "L2                                                                                                                 "
    - "L2.2[1,1000] 5ns 52mb    |------------------------------------------L2.2------------------------------------------|"
    - "WARNING: file L1.1[1,2] 10ns 250mb exceeds soft limit 100mb by more than 50%"
    - "**** Simulation run 0, type=split(ReduceLargeFileSize)(split_times=[1]). 1 Input Files, 250mb total:"
    - "L1, all files 250mb                                                                                                "
    - "L1.1[1,2] 10ns           |------------------------------------------L1.1------------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 250mb total:"
    - "L1, all files 125mb                                                                                                "
    - "L1.?[1,1] 10ns           |L1.?|                                                                                    "
    - "L1.?[2,2] 10ns                                                                                                     |L1.?|"
    - "Committing partition 1:"
    - "  Soft Deleting 1 files: L1.1"
    - "  Creating 2 files"
    - "**** Simulation run 1, type=split(StartLevelOverlapsTooBig)(split_times=[2]). 1 Input Files, 52mb total:"
    - "L2, all files 52mb                                                                                                 "
    - "L2.2[1,1000] 5ns         |------------------------------------------L2.2------------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 52mb total:"
    - "L2                                                                                                                 "
    - "L2.?[1,2] 5ns 106kb      |L2.?|                                                                                    "
    - "L2.?[3,1000] 5ns 52mb    |-----------------------------------------L2.?------------------------------------------| "
    - "Committing partition 1:"
    - "  Soft Deleting 1 files: L2.2"
    - "  Creating 2 files"
    - "**** Simulation run 2, type=compact(TotalSizeLessThanMaxCompactSize). 3 Input Files, 250mb total:"
    - "L1                                                                                                                 "
    - "L1.4[2,2] 10ns 125mb                                                                                               |L1.4|"
    - "L1.3[1,1] 10ns 125mb     |L1.3|                                                                                    "
    - "L2                                                                                                                 "
    - "L2.5[1,2] 5ns 106kb      |------------------------------------------L2.5------------------------------------------|"
    - "**** 1 Output Files (parquet_file_id not yet assigned), 250mb total:"
    - "L2, all files 250mb                                                                                                "
    - "L2.?[1,2] 10ns           |------------------------------------------L2.?------------------------------------------|"
    - "Committing partition 1:"
    - "  Soft Deleting 3 files: L1.3, L1.4, L2.5"
    - "  Creating 1 files"
    - "**** Final Output Files (552mb written)"
    - "L2                                                                                                                 "
    - "L2.6[3,1000] 5ns 52mb    |-----------------------------------------L2.6------------------------------------------| "
    - "L2.7[1,2] 10ns 250mb     |L2.7|                                                                                    "
    - "WARNING: file L2.7[1,2] 10ns 250mb exceeds soft limit 100mb by more than 50%"
    "###
    );
}

// This test simulates a situation where we vertically split 11 large L0s, into 7 files each, then another L0 file is written
// which shifts the default split points.  The purpose of this test is to see if the compactor will adjust its split points
// to the previusly used times.  Otherwise, write amplification will be significantly impacted.
#[tokio::test]
async fn pre_split_large_l0() {
    test_helpers::maybe_start_logging();

    let setup = layout_setup_builder()
        .await
        .with_max_desired_file_size_bytes(MAX_DESIRED_FILE_SIZE)
        .with_required_split_times(vec![3568, 4852, 6136, 7420, 8704])
        .build()
        .await;

    // L0s - assume they started as timestamp 1000 - 10000, with MAX_DESIRED_FILE_SIZE bytes and were split
    for i in 0..=10 {
        setup
            .partition
            .create_parquet_file(
                parquet_builder()
                    .with_min_time(1000)
                    .with_max_time(2284)
                    .with_compaction_level(CompactionLevel::Initial)
                    .with_max_l0_created_at(Time::from_timestamp_nanos(10000 + i))
                    .with_file_size_bytes(MAX_DESIRED_FILE_SIZE / 7),
            )
            .await;
        setup
            .partition
            .create_parquet_file(
                parquet_builder()
                    .with_min_time(2285)
                    .with_max_time(3568)
                    .with_compaction_level(CompactionLevel::Initial)
                    .with_max_l0_created_at(Time::from_timestamp_nanos(10000 + i))
                    .with_file_size_bytes(MAX_DESIRED_FILE_SIZE / 7),
            )
            .await;
        setup
            .partition
            .create_parquet_file(
                parquet_builder()
                    .with_min_time(3569)
                    .with_max_time(4852)
                    .with_compaction_level(CompactionLevel::Initial)
                    .with_max_l0_created_at(Time::from_timestamp_nanos(10000 + i))
                    .with_file_size_bytes(MAX_DESIRED_FILE_SIZE / 7),
            )
            .await;
        setup
            .partition
            .create_parquet_file(
                parquet_builder()
                    .with_min_time(4853)
                    .with_max_time(6136)
                    .with_compaction_level(CompactionLevel::Initial)
                    .with_max_l0_created_at(Time::from_timestamp_nanos(10000 + i))
                    .with_file_size_bytes(MAX_DESIRED_FILE_SIZE / 7),
            )
            .await;
        setup
            .partition
            .create_parquet_file(
                parquet_builder()
                    .with_min_time(6137)
                    .with_max_time(7420)
                    .with_compaction_level(CompactionLevel::Initial)
                    .with_max_l0_created_at(Time::from_timestamp_nanos(10000 + i))
                    .with_file_size_bytes(MAX_DESIRED_FILE_SIZE / 7),
            )
            .await;
        setup
            .partition
            .create_parquet_file(
                parquet_builder()
                    .with_min_time(7421)
                    .with_max_time(8704)
                    .with_compaction_level(CompactionLevel::Initial)
                    .with_max_l0_created_at(Time::from_timestamp_nanos(10000 + i))
                    .with_file_size_bytes(MAX_DESIRED_FILE_SIZE / 7),
            )
            .await;
        setup
            .partition
            .create_parquet_file(
                parquet_builder()
                    .with_min_time(8705)
                    .with_max_time(10000)
                    .with_compaction_level(CompactionLevel::Initial)
                    .with_max_l0_created_at(Time::from_timestamp_nanos(10000 + i))
                    .with_file_size_bytes(MAX_DESIRED_FILE_SIZE / 7),
            )
            .await;
    }

    // after we split those, another L0 arrived with different time range, and shifted the default split points when resuming compaction
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(3000)
                .with_max_time(13000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_max_l0_created_at(Time::from_timestamp_nanos(11000))
                .with_file_size_bytes(MAX_DESIRED_FILE_SIZE),
        )
        .await;

    insta::assert_yaml_snapshot!(
        run_layout_scenario(&setup).await,
        @r###"
    ---
    - "**** Input Files "
    - "L0                                                                                                                 "
    - "L0.1[1000,2284] 10us 14mb|-L0.1--|                                                                                 "
    - "L0.2[2285,3568] 10us 14mb         |-L0.2--|                                                                        "
    - "L0.3[3569,4852] 10us 14mb                   |-L0.3--|                                                              "
    - "L0.4[4853,6136] 10us 14mb                            |-L0.4--|                                                     "
    - "L0.5[6137,7420] 10us 14mb                                      |-L0.5--|                                           "
    - "L0.6[7421,8704] 10us 14mb                                                |-L0.6--|                                 "
    - "L0.7[8705,10000] 10us 14mb                                                         |-L0.7--|                        "
    - "L0.8[1000,2284] 10us 14mb|-L0.8--|                                                                                 "
    - "L0.9[2285,3568] 10us 14mb         |-L0.9--|                                                                        "
    - "L0.10[3569,4852] 10us 14mb                   |-L0.10-|                                                              "
    - "L0.11[4853,6136] 10us 14mb                            |-L0.11-|                                                     "
    - "L0.12[6137,7420] 10us 14mb                                      |-L0.12-|                                           "
    - "L0.13[7421,8704] 10us 14mb                                                |-L0.13-|                                 "
    - "L0.14[8705,10000] 10us 14mb                                                         |-L0.14-|                        "
    - "L0.15[1000,2284] 10us 14mb|-L0.15-|                                                                                 "
    - "L0.16[2285,3568] 10us 14mb         |-L0.16-|                                                                        "
    - "L0.17[3569,4852] 10us 14mb                   |-L0.17-|                                                              "
    - "L0.18[4853,6136] 10us 14mb                            |-L0.18-|                                                     "
    - "L0.19[6137,7420] 10us 14mb                                      |-L0.19-|                                           "
    - "L0.20[7421,8704] 10us 14mb                                                |-L0.20-|                                 "
    - "L0.21[8705,10000] 10us 14mb                                                         |-L0.21-|                        "
    - "L0.22[1000,2284] 10us 14mb|-L0.22-|                                                                                 "
    - "L0.23[2285,3568] 10us 14mb         |-L0.23-|                                                                        "
    - "L0.24[3569,4852] 10us 14mb                   |-L0.24-|                                                              "
    - "L0.25[4853,6136] 10us 14mb                            |-L0.25-|                                                     "
    - "L0.26[6137,7420] 10us 14mb                                      |-L0.26-|                                           "
    - "L0.27[7421,8704] 10us 14mb                                                |-L0.27-|                                 "
    - "L0.28[8705,10000] 10us 14mb                                                         |-L0.28-|                        "
    - "L0.29[1000,2284] 10us 14mb|-L0.29-|                                                                                 "
    - "L0.30[2285,3568] 10us 14mb         |-L0.30-|                                                                        "
    - "L0.31[3569,4852] 10us 14mb                   |-L0.31-|                                                              "
    - "L0.32[4853,6136] 10us 14mb                            |-L0.32-|                                                     "
    - "L0.33[6137,7420] 10us 14mb                                      |-L0.33-|                                           "
    - "L0.34[7421,8704] 10us 14mb                                                |-L0.34-|                                 "
    - "L0.35[8705,10000] 10us 14mb                                                         |-L0.35-|                        "
    - "L0.36[1000,2284] 10.01us 14mb|-L0.36-|                                                                                 "
    - "L0.37[2285,3568] 10.01us 14mb         |-L0.37-|                                                                        "
    - "L0.38[3569,4852] 10.01us 14mb                   |-L0.38-|                                                              "
    - "L0.39[4853,6136] 10.01us 14mb                            |-L0.39-|                                                     "
    - "L0.40[6137,7420] 10.01us 14mb                                      |-L0.40-|                                           "
    - "L0.41[7421,8704] 10.01us 14mb                                                |-L0.41-|                                 "
    - "L0.42[8705,10000] 10.01us 14mb                                                         |-L0.42-|                        "
    - "L0.43[1000,2284] 10.01us 14mb|-L0.43-|                                                                                 "
    - "L0.44[2285,3568] 10.01us 14mb         |-L0.44-|                                                                        "
    - "L0.45[3569,4852] 10.01us 14mb                   |-L0.45-|                                                              "
    - "L0.46[4853,6136] 10.01us 14mb                            |-L0.46-|                                                     "
    - "L0.47[6137,7420] 10.01us 14mb                                      |-L0.47-|                                           "
    - "L0.48[7421,8704] 10.01us 14mb                                                |-L0.48-|                                 "
    - "L0.49[8705,10000] 10.01us 14mb                                                         |-L0.49-|                        "
    - "L0.50[1000,2284] 10.01us 14mb|-L0.50-|                                                                                 "
    - "L0.51[2285,3568] 10.01us 14mb         |-L0.51-|                                                                        "
    - "L0.52[3569,4852] 10.01us 14mb                   |-L0.52-|                                                              "
    - "L0.53[4853,6136] 10.01us 14mb                            |-L0.53-|                                                     "
    - "L0.54[6137,7420] 10.01us 14mb                                      |-L0.54-|                                           "
    - "L0.55[7421,8704] 10.01us 14mb                                                |-L0.55-|                                 "
    - "L0.56[8705,10000] 10.01us 14mb                                                         |-L0.56-|                        "
    - "L0.57[1000,2284] 10.01us 14mb|-L0.57-|                                                                                 "
    - "L0.58[2285,3568] 10.01us 14mb         |-L0.58-|                                                                        "
    - "L0.59[3569,4852] 10.01us 14mb                   |-L0.59-|                                                              "
    - "L0.60[4853,6136] 10.01us 14mb                            |-L0.60-|                                                     "
    - "L0.61[6137,7420] 10.01us 14mb                                      |-L0.61-|                                           "
    - "L0.62[7421,8704] 10.01us 14mb                                                |-L0.62-|                                 "
    - "L0.63[8705,10000] 10.01us 14mb                                                         |-L0.63-|                        "
    - "L0.64[1000,2284] 10.01us 14mb|-L0.64-|                                                                                 "
    - "L0.65[2285,3568] 10.01us 14mb         |-L0.65-|                                                                        "
    - "L0.66[3569,4852] 10.01us 14mb                   |-L0.66-|                                                              "
    - "L0.67[4853,6136] 10.01us 14mb                            |-L0.67-|                                                     "
    - "L0.68[6137,7420] 10.01us 14mb                                      |-L0.68-|                                           "
    - "L0.69[7421,8704] 10.01us 14mb                                                |-L0.69-|                                 "
    - "L0.70[8705,10000] 10.01us 14mb                                                         |-L0.70-|                        "
    - "L0.71[1000,2284] 10.01us 14mb|-L0.71-|                                                                                 "
    - "L0.72[2285,3568] 10.01us 14mb         |-L0.72-|                                                                        "
    - "L0.73[3569,4852] 10.01us 14mb                   |-L0.73-|                                                              "
    - "L0.74[4853,6136] 10.01us 14mb                            |-L0.74-|                                                     "
    - "L0.75[6137,7420] 10.01us 14mb                                      |-L0.75-|                                           "
    - "L0.76[7421,8704] 10.01us 14mb                                                |-L0.76-|                                 "
    - "L0.77[8705,10000] 10.01us 14mb                                                         |-L0.77-|                        "
    - "L0.78[3000,13000] 11us 100mb              |----------------------------------L0.78----------------------------------| "
    - "**** Simulation run 0, type=split(VerticalSplit)(split_times=[3568, 4852, 6136, 7420, 8704, 10312]). 1 Input Files, 100mb total:"
    - "L0, all files 100mb                                                                                                "
    - "L0.78[3000,13000] 11us   |-----------------------------------------L0.78------------------------------------------|"
    - "**** 7 Output Files (parquet_file_id not yet assigned), 100mb total:"
    - "L0                                                                                                                 "
    - "L0.?[3000,3568] 11us 6mb |L0.?|                                                                                    "
    - "L0.?[3569,4852] 11us 13mb     |--L0.?---|                                                                          "
    - "L0.?[4853,6136] 11us 13mb                |--L0.?---|                                                               "
    - "L0.?[6137,7420] 11us 13mb                            |--L0.?---|                                                   "
    - "L0.?[7421,8704] 11us 13mb                                       |--L0.?---|                                        "
    - "L0.?[8705,10312] 11us 16mb                                                   |----L0.?----|                         "
    - "L0.?[10313,13000] 11us 27mb                                                                 |---------L0.?---------| "
    - "Committing partition 1:"
    - "  Soft Deleting 1 files: L0.78"
    - "  Creating 7 files"
    - "**** Simulation run 1, type=split(CompactAndSplitOutput(TotalSizeLessThanMaxCompactSize))(split_times=[1818]). 11 Input Files, 157mb total:"
    - "L0, all files 14mb                                                                                                 "
    - "L0.71[1000,2284] 10.01us |-----------------------------------------L0.71------------------------------------------|"
    - "L0.64[1000,2284] 10.01us |-----------------------------------------L0.64------------------------------------------|"
    - "L0.57[1000,2284] 10.01us |-----------------------------------------L0.57------------------------------------------|"
    - "L0.50[1000,2284] 10.01us |-----------------------------------------L0.50------------------------------------------|"
    - "L0.43[1000,2284] 10.01us |-----------------------------------------L0.43------------------------------------------|"
    - "L0.36[1000,2284] 10.01us |-----------------------------------------L0.36------------------------------------------|"
    - "L0.29[1000,2284] 10us    |-----------------------------------------L0.29------------------------------------------|"
    - "L0.22[1000,2284] 10us    |-----------------------------------------L0.22------------------------------------------|"
    - "L0.15[1000,2284] 10us    |-----------------------------------------L0.15------------------------------------------|"
    - "L0.8[1000,2284] 10us     |------------------------------------------L0.8------------------------------------------|"
    - "L0.1[1000,2284] 10us     |------------------------------------------L0.1------------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 157mb total:"
    - "L1                                                                                                                 "
    - "L1.?[1000,1818] 10.01us 100mb|-------------------------L1.?--------------------------|                                 "
    - "L1.?[1819,2284] 10.01us 57mb                                                         |-------------L1.?-------------| "
    - "Committing partition 1:"
    - "  Soft Deleting 11 files: L0.1, L0.8, L0.15, L0.22, L0.29, L0.36, L0.43, L0.50, L0.57, L0.64, L0.71"
    - "  Creating 2 files"
    - "**** Simulation run 2, type=split(CompactAndSplitOutput(TotalSizeLessThanMaxCompactSize))(split_times=[3073]). 12 Input Files, 163mb total:"
    - "L0                                                                                                                 "
    - "L0.79[3000,3568] 11us 6mb                                                  |----------------L0.79----------------| "
    - "L0.72[2285,3568] 10.01us 14mb|-----------------------------------------L0.72------------------------------------------|"
    - "L0.65[2285,3568] 10.01us 14mb|-----------------------------------------L0.65------------------------------------------|"
    - "L0.58[2285,3568] 10.01us 14mb|-----------------------------------------L0.58------------------------------------------|"
    - "L0.51[2285,3568] 10.01us 14mb|-----------------------------------------L0.51------------------------------------------|"
    - "L0.44[2285,3568] 10.01us 14mb|-----------------------------------------L0.44------------------------------------------|"
    - "L0.37[2285,3568] 10.01us 14mb|-----------------------------------------L0.37------------------------------------------|"
    - "L0.30[2285,3568] 10us 14mb|-----------------------------------------L0.30------------------------------------------|"
    - "L0.23[2285,3568] 10us 14mb|-----------------------------------------L0.23------------------------------------------|"
    - "L0.16[2285,3568] 10us 14mb|-----------------------------------------L0.16------------------------------------------|"
    - "L0.9[2285,3568] 10us 14mb|------------------------------------------L0.9------------------------------------------|"
    - "L0.2[2285,3568] 10us 14mb|------------------------------------------L0.2------------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 163mb total:"
    - "L1                                                                                                                 "
    - "L1.?[2285,3073] 11us 100mb|------------------------L1.?-------------------------|                                   "
    - "L1.?[3074,3568] 11us 63mb                                                       |--------------L1.?--------------| "
    - "Committing partition 1:"
    - "  Soft Deleting 12 files: L0.2, L0.9, L0.16, L0.23, L0.30, L0.37, L0.44, L0.51, L0.58, L0.65, L0.72, L0.79"
    - "  Creating 2 files"
    - "**** Simulation run 3, type=split(CompactAndSplitOutput(TotalSizeLessThanMaxCompactSize))(split_times=[4324]). 12 Input Files, 170mb total:"
    - "L0                                                                                                                 "
    - "L0.80[3569,4852] 11us 13mb|-----------------------------------------L0.80------------------------------------------|"
    - "L0.73[3569,4852] 10.01us 14mb|-----------------------------------------L0.73------------------------------------------|"
    - "L0.66[3569,4852] 10.01us 14mb|-----------------------------------------L0.66------------------------------------------|"
    - "L0.59[3569,4852] 10.01us 14mb|-----------------------------------------L0.59------------------------------------------|"
    - "L0.52[3569,4852] 10.01us 14mb|-----------------------------------------L0.52------------------------------------------|"
    - "L0.45[3569,4852] 10.01us 14mb|-----------------------------------------L0.45------------------------------------------|"
    - "L0.38[3569,4852] 10.01us 14mb|-----------------------------------------L0.38------------------------------------------|"
    - "L0.31[3569,4852] 10us 14mb|-----------------------------------------L0.31------------------------------------------|"
    - "L0.24[3569,4852] 10us 14mb|-----------------------------------------L0.24------------------------------------------|"
    - "L0.17[3569,4852] 10us 14mb|-----------------------------------------L0.17------------------------------------------|"
    - "L0.10[3569,4852] 10us 14mb|-----------------------------------------L0.10------------------------------------------|"
    - "L0.3[3569,4852] 10us 14mb|------------------------------------------L0.3------------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 170mb total:"
    - "L1                                                                                                                 "
    - "L1.?[3569,4324] 11us 100mb|-----------------------L1.?-----------------------|                                      "
    - "L1.?[4325,4852] 11us 70mb                                                     |---------------L1.?---------------| "
    - "Committing partition 1:"
    - "  Soft Deleting 12 files: L0.3, L0.10, L0.17, L0.24, L0.31, L0.38, L0.45, L0.52, L0.59, L0.66, L0.73, L0.80"
    - "  Creating 2 files"
    - "**** Simulation run 4, type=split(CompactAndSplitOutput(TotalSizeLessThanMaxCompactSize))(split_times=[5608]). 12 Input Files, 170mb total:"
    - "L0                                                                                                                 "
    - "L0.81[4853,6136] 11us 13mb|-----------------------------------------L0.81------------------------------------------|"
    - "L0.74[4853,6136] 10.01us 14mb|-----------------------------------------L0.74------------------------------------------|"
    - "L0.67[4853,6136] 10.01us 14mb|-----------------------------------------L0.67------------------------------------------|"
    - "L0.60[4853,6136] 10.01us 14mb|-----------------------------------------L0.60------------------------------------------|"
    - "L0.53[4853,6136] 10.01us 14mb|-----------------------------------------L0.53------------------------------------------|"
    - "L0.46[4853,6136] 10.01us 14mb|-----------------------------------------L0.46------------------------------------------|"
    - "L0.39[4853,6136] 10.01us 14mb|-----------------------------------------L0.39------------------------------------------|"
    - "L0.32[4853,6136] 10us 14mb|-----------------------------------------L0.32------------------------------------------|"
    - "L0.25[4853,6136] 10us 14mb|-----------------------------------------L0.25------------------------------------------|"
    - "L0.18[4853,6136] 10us 14mb|-----------------------------------------L0.18------------------------------------------|"
    - "L0.11[4853,6136] 10us 14mb|-----------------------------------------L0.11------------------------------------------|"
    - "L0.4[4853,6136] 10us 14mb|------------------------------------------L0.4------------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 170mb total:"
    - "L1                                                                                                                 "
    - "L1.?[4853,5608] 11us 100mb|-----------------------L1.?-----------------------|                                      "
    - "L1.?[5609,6136] 11us 70mb                                                     |---------------L1.?---------------| "
    - "Committing partition 1:"
    - "  Soft Deleting 12 files: L0.4, L0.11, L0.18, L0.25, L0.32, L0.39, L0.46, L0.53, L0.60, L0.67, L0.74, L0.81"
    - "  Creating 2 files"
    - "**** Simulation run 5, type=split(CompactAndSplitOutput(TotalSizeLessThanMaxCompactSize))(split_times=[6892]). 12 Input Files, 170mb total:"
    - "L0                                                                                                                 "
    - "L0.82[6137,7420] 11us 13mb|-----------------------------------------L0.82------------------------------------------|"
    - "L0.75[6137,7420] 10.01us 14mb|-----------------------------------------L0.75------------------------------------------|"
    - "L0.68[6137,7420] 10.01us 14mb|-----------------------------------------L0.68------------------------------------------|"
    - "L0.61[6137,7420] 10.01us 14mb|-----------------------------------------L0.61------------------------------------------|"
    - "L0.54[6137,7420] 10.01us 14mb|-----------------------------------------L0.54------------------------------------------|"
    - "L0.47[6137,7420] 10.01us 14mb|-----------------------------------------L0.47------------------------------------------|"
    - "L0.40[6137,7420] 10.01us 14mb|-----------------------------------------L0.40------------------------------------------|"
    - "L0.33[6137,7420] 10us 14mb|-----------------------------------------L0.33------------------------------------------|"
    - "L0.26[6137,7420] 10us 14mb|-----------------------------------------L0.26------------------------------------------|"
    - "L0.19[6137,7420] 10us 14mb|-----------------------------------------L0.19------------------------------------------|"
    - "L0.12[6137,7420] 10us 14mb|-----------------------------------------L0.12------------------------------------------|"
    - "L0.5[6137,7420] 10us 14mb|------------------------------------------L0.5------------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 170mb total:"
    - "L1                                                                                                                 "
    - "L1.?[6137,6892] 11us 100mb|-----------------------L1.?-----------------------|                                      "
    - "L1.?[6893,7420] 11us 70mb                                                     |---------------L1.?---------------| "
    - "Committing partition 1:"
    - "  Soft Deleting 12 files: L0.5, L0.12, L0.19, L0.26, L0.33, L0.40, L0.47, L0.54, L0.61, L0.68, L0.75, L0.82"
    - "  Creating 2 files"
    - "**** Simulation run 6, type=split(CompactAndSplitOutput(TotalSizeLessThanMaxCompactSize))(split_times=[8176]). 12 Input Files, 170mb total:"
    - "L0                                                                                                                 "
    - "L0.83[7421,8704] 11us 13mb|-----------------------------------------L0.83------------------------------------------|"
    - "L0.76[7421,8704] 10.01us 14mb|-----------------------------------------L0.76------------------------------------------|"
    - "L0.69[7421,8704] 10.01us 14mb|-----------------------------------------L0.69------------------------------------------|"
    - "L0.62[7421,8704] 10.01us 14mb|-----------------------------------------L0.62------------------------------------------|"
    - "L0.55[7421,8704] 10.01us 14mb|-----------------------------------------L0.55------------------------------------------|"
    - "L0.48[7421,8704] 10.01us 14mb|-----------------------------------------L0.48------------------------------------------|"
    - "L0.41[7421,8704] 10.01us 14mb|-----------------------------------------L0.41------------------------------------------|"
    - "L0.34[7421,8704] 10us 14mb|-----------------------------------------L0.34------------------------------------------|"
    - "L0.27[7421,8704] 10us 14mb|-----------------------------------------L0.27------------------------------------------|"
    - "L0.20[7421,8704] 10us 14mb|-----------------------------------------L0.20------------------------------------------|"
    - "L0.13[7421,8704] 10us 14mb|-----------------------------------------L0.13------------------------------------------|"
    - "L0.6[7421,8704] 10us 14mb|------------------------------------------L0.6------------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 170mb total:"
    - "L1                                                                                                                 "
    - "L1.?[7421,8176] 11us 100mb|-----------------------L1.?-----------------------|                                      "
    - "L1.?[8177,8704] 11us 70mb                                                     |---------------L1.?---------------| "
    - "Committing partition 1:"
    - "  Soft Deleting 12 files: L0.6, L0.13, L0.20, L0.27, L0.34, L0.41, L0.48, L0.55, L0.62, L0.69, L0.76, L0.83"
    - "  Creating 2 files"
    - "**** Simulation run 7, type=split(CompactAndSplitOutput(TotalSizeLessThanMaxCompactSize))(split_times=[9633]). 12 Input Files, 173mb total:"
    - "L0                                                                                                                 "
    - "L0.84[8705,10312] 11us 16mb|-----------------------------------------L0.84------------------------------------------|"
    - "L0.77[8705,10000] 10.01us 14mb|--------------------------------L0.77---------------------------------|                  "
    - "L0.70[8705,10000] 10.01us 14mb|--------------------------------L0.70---------------------------------|                  "
    - "L0.63[8705,10000] 10.01us 14mb|--------------------------------L0.63---------------------------------|                  "
    - "L0.56[8705,10000] 10.01us 14mb|--------------------------------L0.56---------------------------------|                  "
    - "L0.49[8705,10000] 10.01us 14mb|--------------------------------L0.49---------------------------------|                  "
    - "L0.42[8705,10000] 10.01us 14mb|--------------------------------L0.42---------------------------------|                  "
    - "L0.35[8705,10000] 10us 14mb|--------------------------------L0.35---------------------------------|                  "
    - "L0.28[8705,10000] 10us 14mb|--------------------------------L0.28---------------------------------|                  "
    - "L0.21[8705,10000] 10us 14mb|--------------------------------L0.21---------------------------------|                  "
    - "L0.14[8705,10000] 10us 14mb|--------------------------------L0.14---------------------------------|                  "
    - "L0.7[8705,10000] 10us 14mb|---------------------------------L0.7---------------------------------|                  "
    - "**** 2 Output Files (parquet_file_id not yet assigned), 173mb total:"
    - "L1                                                                                                                 "
    - "L1.?[8705,9633] 11us 100mb|----------------------L1.?-----------------------|                                       "
    - "L1.?[9634,10312] 11us 73mb                                                    |---------------L1.?----------------| "
    - "Committing partition 1:"
    - "  Soft Deleting 12 files: L0.7, L0.14, L0.21, L0.28, L0.35, L0.42, L0.49, L0.56, L0.63, L0.70, L0.77, L0.84"
    - "  Creating 2 files"
    - "**** Simulation run 8, type=split(CompactAndSplitOutput(TotalSizeLessThanMaxCompactSize))(split_times=[12462]). 1 Input Files, 27mb total:"
    - "L0, all files 27mb                                                                                                 "
    - "L0.85[10313,13000] 11us  |-----------------------------------------L0.85------------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 27mb total:"
    - "L1                                                                                                                 "
    - "L1.?[10313,12462] 11us 21mb|--------------------------------L1.?---------------------------------|                   "
    - "L1.?[12463,13000] 11us 5mb                                                                        |-----L1.?------| "
    - "Committing partition 1:"
    - "  Soft Deleting 1 files: L0.85"
    - "  Creating 2 files"
    - "**** Simulation run 9, type=split(CompactAndSplitOutput(TotalSizeLessThanMaxCompactSize))(split_times=[12462]). 2 Input Files, 27mb total:"
    - "L1                                                                                                                 "
    - "L1.101[12463,13000] 11us 5mb                                                                        |----L1.101-----| "
    - "L1.100[10313,12462] 11us 21mb|-------------------------------L1.100--------------------------------|                   "
    - "**** 2 Output Files (parquet_file_id not yet assigned), 27mb total:"
    - "L2                                                                                                                 "
    - "L2.?[10313,12462] 11us 21mb|--------------------------------L2.?---------------------------------|                   "
    - "L2.?[12463,13000] 11us 5mb                                                                        |-----L2.?------| "
    - "Committing partition 1:"
    - "  Soft Deleting 2 files: L1.100, L1.101"
    - "  Upgrading 14 files level to CompactionLevel::L2: L1.86, L1.87, L1.88, L1.89, L1.90, L1.91, L1.92, L1.93, L1.94, L1.95, L1.96, L1.97, L1.98, L1.99"
    - "  Creating 2 files"
    - "**** Final Output Files (1.3gb written)"
    - "L2                                                                                                                 "
    - "L2.86[1000,1818] 10.01us 100mb|L2.86|                                                                                   "
    - "L2.87[1819,2284] 10.01us 57mb      |L2.87|                                                                             "
    - "L2.88[2285,3073] 11us 100mb         |L2.88|                                                                          "
    - "L2.89[3074,3568] 11us 63mb               |L2.89|                                                                    "
    - "L2.90[3569,4324] 11us 100mb                   |L2.90|                                                                "
    - "L2.91[4325,4852] 11us 70mb                        |L2.91|                                                           "
    - "L2.92[4853,5608] 11us 100mb                            |L2.92|                                                       "
    - "L2.93[5609,6136] 11us 70mb                                  |L2.93|                                                 "
    - "L2.94[6137,6892] 11us 100mb                                      |L2.94|                                             "
    - "L2.95[6893,7420] 11us 70mb                                            |L2.95|                                       "
    - "L2.96[7421,8176] 11us 100mb                                                |L2.96|                                   "
    - "L2.97[8177,8704] 11us 70mb                                                     |L2.97|                              "
    - "L2.98[8705,9633] 11us 100mb                                                         |L2.98|                          "
    - "L2.99[9634,10312] 11us 73mb                                                                |L2.99|                   "
    - "L2.102[10313,12462] 11us 21mb                                                                     |----L2.102----|     "
    - "L2.103[12463,13000] 11us 5mb                                                                                     |L2.103|"
    "###
    );
}

#[tokio::test]
async fn file_over_max_size() {
    test_helpers::maybe_start_logging();

    const MAX_DESIRED_FILE_SIZE: u64 = 10 * ONE_MB;
    let setup = layout_setup_builder()
        .await
        .with_max_num_files_per_plan(20)
        .with_max_desired_file_size_bytes(MAX_DESIRED_FILE_SIZE)
        .with_partition_timeout(Duration::from_millis(100000))
        .with_suppress_run_output() // remove this to debug
        .with_writes_breakdown()
        .build()
        .await;

    let file_count = 100;
    for i in 0..file_count {
        setup
            .partition
            .create_parquet_file(
                parquet_builder()
                    .with_min_time(i * 10)
                    .with_max_time(i * 10 + 1)
                    .with_compaction_level(CompactionLevel::Initial)
                    .with_max_l0_created_at(Time::from_timestamp_nanos(i * 10 + 2))
                    .with_file_size_bytes(1),
            )
            .await;
    }

    // One oversized file that overlaps the above files.  But there's so many small ones we trigger ManySmallFiles.
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(9)
                .with_max_time(file_count * 10 + 1)
                .with_compaction_level(CompactionLevel::Initial)
                .with_max_l0_created_at(Time::from_timestamp_nanos(2002))
                .with_file_size_bytes(MAX_DESIRED_FILE_SIZE * 10 / 3),
        )
        .await;

    // Also test a separate situation - a single file that's too big to compact.  This one does not trigger ManySmallFiles.
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(file_count * 10 + 100)
                .with_max_time(file_count * 10 + 200)
                .with_compaction_level(CompactionLevel::Initial)
                .with_max_l0_created_at(Time::from_timestamp_nanos(2002))
                .with_file_size_bytes(MAX_DESIRED_FILE_SIZE * 10 / 3),
        )
        .await;

    insta::assert_yaml_snapshot!(
        run_layout_scenario(&setup).await,
        @r###"
    ---
    - "**** Input Files "
    - "L0                                                                                                                 "
    - "L0.1[0,1] 2ns 1b         |L0.1|                                                                                    "
    - "L0.2[10,11] 12ns 1b      |L0.2|                                                                                    "
    - "L0.3[20,21] 22ns 1b       |L0.3|                                                                                   "
    - "L0.4[30,31] 32ns 1b        |L0.4|                                                                                  "
    - "L0.5[40,41] 42ns 1b         |L0.5|                                                                                 "
    - "L0.6[50,51] 52ns 1b         |L0.6|                                                                                 "
    - "L0.7[60,61] 62ns 1b          |L0.7|                                                                                "
    - "L0.8[70,71] 72ns 1b           |L0.8|                                                                               "
    - "L0.9[80,81] 82ns 1b            |L0.9|                                                                              "
    - "L0.10[90,91] 92ns 1b           |L0.10|                                                                             "
    - "L0.11[100,101] 102ns 1b         |L0.11|                                                                            "
    - "L0.12[110,111] 112ns 1b          |L0.12|                                                                           "
    - "L0.13[120,121] 122ns 1b           |L0.13|                                                                          "
    - "L0.14[130,131] 132ns 1b           |L0.14|                                                                          "
    - "L0.15[140,141] 142ns 1b            |L0.15|                                                                         "
    - "L0.16[150,151] 152ns 1b             |L0.16|                                                                        "
    - "L0.17[160,161] 162ns 1b              |L0.17|                                                                       "
    - "L0.18[170,171] 172ns 1b              |L0.18|                                                                       "
    - "L0.19[180,181] 182ns 1b               |L0.19|                                                                      "
    - "L0.20[190,191] 192ns 1b                |L0.20|                                                                     "
    - "L0.21[200,201] 202ns 1b                 |L0.21|                                                                    "
    - "L0.22[210,211] 212ns 1b                 |L0.22|                                                                    "
    - "L0.23[220,221] 222ns 1b                  |L0.23|                                                                   "
    - "L0.24[230,231] 232ns 1b                   |L0.24|                                                                  "
    - "L0.25[240,241] 242ns 1b                    |L0.25|                                                                 "
    - "L0.26[250,251] 252ns 1b                    |L0.26|                                                                 "
    - "L0.27[260,261] 262ns 1b                     |L0.27|                                                                "
    - "L0.28[270,271] 272ns 1b                      |L0.28|                                                               "
    - "L0.29[280,281] 282ns 1b                       |L0.29|                                                              "
    - "L0.30[290,291] 292ns 1b                       |L0.30|                                                              "
    - "L0.31[300,301] 302ns 1b                        |L0.31|                                                             "
    - "L0.32[310,311] 312ns 1b                         |L0.32|                                                            "
    - "L0.33[320,321] 322ns 1b                          |L0.33|                                                           "
    - "L0.34[330,331] 332ns 1b                          |L0.34|                                                           "
    - "L0.35[340,341] 342ns 1b                           |L0.35|                                                          "
    - "L0.36[350,351] 352ns 1b                            |L0.36|                                                         "
    - "L0.37[360,361] 362ns 1b                             |L0.37|                                                        "
    - "L0.38[370,371] 372ns 1b                             |L0.38|                                                        "
    - "L0.39[380,381] 382ns 1b                              |L0.39|                                                       "
    - "L0.40[390,391] 392ns 1b                               |L0.40|                                                      "
    - "L0.41[400,401] 402ns 1b                                |L0.41|                                                     "
    - "L0.42[410,411] 412ns 1b                                |L0.42|                                                     "
    - "L0.43[420,421] 422ns 1b                                 |L0.43|                                                    "
    - "L0.44[430,431] 432ns 1b                                  |L0.44|                                                   "
    - "L0.45[440,441] 442ns 1b                                   |L0.45|                                                  "
    - "L0.46[450,451] 452ns 1b                                   |L0.46|                                                  "
    - "L0.47[460,461] 462ns 1b                                    |L0.47|                                                 "
    - "L0.48[470,471] 472ns 1b                                     |L0.48|                                                "
    - "L0.49[480,481] 482ns 1b                                      |L0.49|                                               "
    - "L0.50[490,491] 492ns 1b                                      |L0.50|                                               "
    - "L0.51[500,501] 502ns 1b                                       |L0.51|                                              "
    - "L0.52[510,511] 512ns 1b                                        |L0.52|                                             "
    - "L0.53[520,521] 522ns 1b                                         |L0.53|                                            "
    - "L0.54[530,531] 532ns 1b                                         |L0.54|                                            "
    - "L0.55[540,541] 542ns 1b                                          |L0.55|                                           "
    - "L0.56[550,551] 552ns 1b                                           |L0.56|                                          "
    - "L0.57[560,561] 562ns 1b                                            |L0.57|                                         "
    - "L0.58[570,571] 572ns 1b                                            |L0.58|                                         "
    - "L0.59[580,581] 582ns 1b                                             |L0.59|                                        "
    - "L0.60[590,591] 592ns 1b                                              |L0.60|                                       "
    - "L0.61[600,601] 602ns 1b                                               |L0.61|                                      "
    - "L0.62[610,611] 612ns 1b                                               |L0.62|                                      "
    - "L0.63[620,621] 622ns 1b                                                |L0.63|                                     "
    - "L0.64[630,631] 632ns 1b                                                 |L0.64|                                    "
    - "L0.65[640,641] 642ns 1b                                                  |L0.65|                                   "
    - "L0.66[650,651] 652ns 1b                                                  |L0.66|                                   "
    - "L0.67[660,661] 662ns 1b                                                   |L0.67|                                  "
    - "L0.68[670,671] 672ns 1b                                                    |L0.68|                                 "
    - "L0.69[680,681] 682ns 1b                                                     |L0.69|                                "
    - "L0.70[690,691] 692ns 1b                                                     |L0.70|                                "
    - "L0.71[700,701] 702ns 1b                                                      |L0.71|                               "
    - "L0.72[710,711] 712ns 1b                                                       |L0.72|                              "
    - "L0.73[720,721] 722ns 1b                                                        |L0.73|                             "
    - "L0.74[730,731] 732ns 1b                                                        |L0.74|                             "
    - "L0.75[740,741] 742ns 1b                                                         |L0.75|                            "
    - "L0.76[750,751] 752ns 1b                                                          |L0.76|                           "
    - "L0.77[760,761] 762ns 1b                                                           |L0.77|                          "
    - "L0.78[770,771] 772ns 1b                                                           |L0.78|                          "
    - "L0.79[780,781] 782ns 1b                                                            |L0.79|                         "
    - "L0.80[790,791] 792ns 1b                                                             |L0.80|                        "
    - "L0.81[800,801] 802ns 1b                                                              |L0.81|                       "
    - "L0.82[810,811] 812ns 1b                                                              |L0.82|                       "
    - "L0.83[820,821] 822ns 1b                                                               |L0.83|                      "
    - "L0.84[830,831] 832ns 1b                                                                |L0.84|                     "
    - "L0.85[840,841] 842ns 1b                                                                 |L0.85|                    "
    - "L0.86[850,851] 852ns 1b                                                                 |L0.86|                    "
    - "L0.87[860,861] 862ns 1b                                                                  |L0.87|                   "
    - "L0.88[870,871] 872ns 1b                                                                   |L0.88|                  "
    - "L0.89[880,881] 882ns 1b                                                                    |L0.89|                 "
    - "L0.90[890,891] 892ns 1b                                                                    |L0.90|                 "
    - "L0.91[900,901] 902ns 1b                                                                     |L0.91|                "
    - "L0.92[910,911] 912ns 1b                                                                      |L0.92|               "
    - "L0.93[920,921] 922ns 1b                                                                       |L0.93|              "
    - "L0.94[930,931] 932ns 1b                                                                       |L0.94|              "
    - "L0.95[940,941] 942ns 1b                                                                        |L0.95|             "
    - "L0.96[950,951] 952ns 1b                                                                         |L0.96|            "
    - "L0.97[960,961] 962ns 1b                                                                          |L0.97|           "
    - "L0.98[970,971] 972ns 1b                                                                          |L0.98|           "
    - "L0.99[980,981] 982ns 1b                                                                           |L0.99|          "
    - "L0.100[990,991] 992ns 1b                                                                           |L0.100|        "
    - "L0.101[9,1001] 2us 33mb  |---------------------------------L0.101---------------------------------|                "
    - "L0.102[1100,1200] 2us 33mb                                                                                  |L0.102|"
    - "WARNING: file L0.101[9,1001] 2us 33mb exceeds soft limit 10mb by more than 50%"
    - "WARNING: file L0.102[1100,1200] 2us 33mb exceeds soft limit 10mb by more than 50%"
    - "**** Final Output Files (91mb written)"
    - "L2                                                                                                                 "
    - "L2.102[1100,1200] 2us 33mb                                                                                  |L2.102|"
    - "L2.109[0,271] 2us 9mb    |------L2.109------|                                                                      "
    - "L2.115[272,571] 2us 10mb                     |-------L2.115-------|                                                "
    - "L2.116[572,870] 2us 10mb                                           |-------L2.116-------|                          "
    - "L2.117[871,1001] 2us 4mb                                                                  |L2.117-|                "
    - "**** Breakdown of where bytes were written"
    - 33mb written by split(VerticalSplit)
    - 58mb written by split(CompactAndSplitOutput(TotalSizeLessThanMaxCompactSize))
    - 60b written by compact(ManySmallFiles)
    - "WARNING: file L2.102[1100,1200] 2us 33mb exceeds soft limit 10mb by more than 50%"
    "###
    );
}
