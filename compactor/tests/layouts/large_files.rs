//! layout tests for scenarios with large input files
//!
//! See [crate::layout] module for detailed documentation

use data_types::CompactionLevel;
use iox_time::Time;

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
    - "L1.?[668,668] 9ns 0b     |L1.?|                                                                                    "
    - "L1.?[669,1000] 9ns 50mb  |-----------------------------------------L1.?------------------------------------------| "
    - "Committing partition 1:"
    - "  Soft Deleting 1 files: L1.4"
    - "  Creating 2 files"
    - "**** Simulation run 3, type=split(CompactAndSplitOutput(FoundSubsetLessThanMaxCompactSize))(split_times=[335]). 3 Input Files, 200mb total:"
    - "L1                                                                                                                 "
    - "L1.3[1,667] 9ns 100mb    |-----------------------------------------L1.3------------------------------------------| "
    - "L1.7[668,668] 9ns 0b                                                                                               |L1.7|"
    - "L2                                                                                                                 "
    - "L2.5[2,668] 8ns 100mb    |-----------------------------------------L2.5------------------------------------------| "
    - "**** 2 Output Files (parquet_file_id not yet assigned), 200mb total:"
    - "L2                                                                                                                 "
    - "L2.?[1,335] 9ns 100mb    |-------------------L2.?--------------------|                                             "
    - "L2.?[336,668] 9ns 100mb                                               |-------------------L2.?-------------------| "
    - "Committing partition 1:"
    - "  Soft Deleting 3 files: L1.3, L2.5, L1.7"
    - "  Creating 2 files"
    - "**** Final Output Files (550mb written)"
    - "L1                                                                                                                 "
    - "L1.8[669,1000] 9ns 50mb                                                              |-----------L1.8------------| "
    - "L2                                                                                                                 "
    - "L2.6[669,1000] 8ns 50mb                                                              |-----------L2.6------------| "
    - "L2.9[1,335] 9ns 100mb    |------------L2.9------------|                                                            "
    - "L2.10[336,668] 9ns 100mb                               |-----------L2.10-----------|                               "
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
    - "L2.?[935,1000] 8ns 50mb                                                              |-----------L2.?------------| "
    - "Committing partition 1:"
    - "  Soft Deleting 2 files: L1.1, L2.2"
    - "  Creating 4 files"
    - "**** Simulation run 2, type=split(CompactAndSplitOutput(TotalSizeLessThanMaxCompactSize))(split_times=[835]). 3 Input Files, 200mb total:"
    - "L1                                                                                                                 "
    - "L1.4[668,1000] 9ns 50mb  |------------------------------------------L1.4------------------------------------------|"
    - "L2                                                                                                                 "
    - "L2.6[935,1000] 8ns 50mb                                                                          |-----L2.6------| "
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
    - "**** Simulation run 0, type=split(HighL0OverlapSingleFile)(split_times=[333, 666]). 1 Input Files, 150mb total:"
    - "L1, all files 150mb                                                                                                "
    - "L1.2[1,1000] 9ns         |------------------------------------------L1.2------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 150mb total:"
    - "L1                                                                                                                 "
    - "L1.?[1,333] 9ns 50mb     |-----------L1.?------------|                                                             "
    - "L1.?[334,666] 9ns 50mb                                 |-----------L1.?------------|                               "
    - "L1.?[667,1000] 9ns 50mb                                                              |------------L1.?------------|"
    - "**** Simulation run 1, type=split(HighL0OverlapSingleFile)(split_times=[333, 666]). 1 Input Files, 150mb total:"
    - "L0, all files 150mb                                                                                                "
    - "L0.1[0,1000] 10ns        |------------------------------------------L0.1------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 150mb total:"
    - "L0                                                                                                                 "
    - "L0.?[0,333] 10ns 50mb    |-----------L0.?------------|                                                             "
    - "L0.?[334,666] 10ns 50mb                                |-----------L0.?------------|                               "
    - "L0.?[667,1000] 10ns 50mb                                                             |-----------L0.?------------| "
    - "Committing partition 1:"
    - "  Soft Deleting 2 files: L0.1, L1.2"
    - "  Creating 6 files"
    - "**** Simulation run 2, type=split(CompactAndSplitOutput(FoundSubsetLessThanMaxCompactSize))(split_times=[334]). 4 Input Files, 199mb total:"
    - "L0                                                                                                                 "
    - "L0.6[0,333] 10ns 50mb    |-------------------L0.6--------------------|                                             "
    - "L0.7[334,666] 10ns 50mb                                               |-------------------L0.7-------------------| "
    - "L1                                                                                                                 "
    - "L1.3[1,333] 9ns 50mb     |-------------------L1.3-------------------|                                              "
    - "L1.4[334,666] 9ns 50mb                                                |-------------------L1.4-------------------| "
    - "**** 2 Output Files (parquet_file_id not yet assigned), 199mb total:"
    - "L1                                                                                                                 "
    - "L1.?[0,334] 10ns 100mb   |-------------------L1.?--------------------|                                             "
    - "L1.?[335,666] 10ns 99mb                                               |-------------------L1.?-------------------| "
    - "Committing partition 1:"
    - "  Soft Deleting 4 files: L1.3, L1.4, L0.6, L0.7"
    - "  Creating 2 files"
    - "**** Simulation run 3, type=split(CompactAndSplitOutput(TotalSizeLessThanMaxCompactSize))(split_times=[933]). 2 Input Files, 101mb total:"
    - "L0                                                                                                                 "
    - "L0.8[667,1000] 10ns 50mb |------------------------------------------L0.8------------------------------------------|"
    - "L1                                                                                                                 "
    - "L1.5[667,1000] 9ns 50mb  |------------------------------------------L1.5------------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 101mb total:"
    - "L1                                                                                                                 "
    - "L1.?[667,933] 10ns 80mb  |--------------------------------L1.?---------------------------------|                   "
    - "L1.?[934,1000] 10ns 20mb                                                                         |-----L1.?------| "
    - "Committing partition 1:"
    - "  Soft Deleting 2 files: L1.5, L0.8"
    - "  Creating 2 files"
    - "**** Simulation run 4, type=split(CompactAndSplitOutput(TotalSizeLessThanMaxCompactSize))(split_times=[668]). 3 Input Files, 200mb total:"
    - "L1                                                                                                                 "
    - "L1.10[335,666] 10ns 99mb |------------------L1.10-------------------|                                              "
    - "L1.12[934,1000] 10ns 20mb                                                                                 |L1.12-| "
    - "L1.11[667,933] 10ns 80mb                                             |--------------L1.11---------------|          "
    - "**** 2 Output Files (parquet_file_id not yet assigned), 200mb total:"
    - "L2                                                                                                                 "
    - "L2.?[335,668] 10ns 100mb |-------------------L2.?--------------------|                                             "
    - "L2.?[669,1000] 10ns 100mb                                             |-------------------L2.?-------------------| "
    - "Committing partition 1:"
    - "  Soft Deleting 3 files: L1.10, L1.11, L1.12"
    - "  Upgrading 1 files level to CompactionLevel::L2: L1.9"
    - "  Creating 2 files"
    - "**** Final Output Files (800mb written)"
    - "L2                                                                                                                 "
    - "L2.9[0,334] 10ns 100mb   |------------L2.9------------|                                                            "
    - "L2.13[335,668] 10ns 100mb                              |-----------L2.13-----------|                               "
    - "L2.14[669,1000] 10ns 100mb                                                            |-----------L2.14-----------| "
    "###
    );

    // Read all files including the soft deleted ones
    let output_files = setup.list_by_table().await;
    assert_eq!(output_files.len(), 14);

    // Sort the files by id
    let mut output_files = output_files;
    output_files.sort_by(|a, b| a.id.cmp(&b.id));

    // Verify all L0 files created by splitting must have  value of max_l0_created_at 10 which is the value of the riginal L0
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
    let l1_sizes = vec![53 * ONE_MB, 45 * ONE_MB, 5 * ONE_MB];
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
    let l1_sizes = vec![69 * ONE_MB, 50 * ONE_MB];
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
    - "**** Simulation run 0, type=split(ReduceLargeFileSize)(split_times=[1294, 2587]). 1 Input Files, 232mb total:"
    - "L2, all files 232mb                                                                                                "
    - "L2.1[1,3000] 5ns         |------------------------------------------L2.1------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 232mb total:"
    - "L2                                                                                                                 "
    - "L2.?[1,1294] 5ns 100mb   |----------------L2.?----------------|                                                    "
    - "L2.?[1295,2587] 5ns 100mb                                      |----------------L2.?----------------|              "
    - "L2.?[2588,3000] 5ns 32mb                                                                              |---L2.?---| "
    - "Committing partition 1:"
    - "  Soft Deleting 1 files: L2.1"
    - "  Creating 3 files"
    - "**** Simulation run 1, type=split(ReduceOverlap)(split_times=[1294]). 1 Input Files, 50mb total:"
    - "L1, all files 50mb                                                                                                 "
    - "L1.3[1001,2000] 11ns     |------------------------------------------L1.3------------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 50mb total:"
    - "L1                                                                                                                 "
    - "L1.?[1001,1294] 11ns 15mb|----------L1.?----------|                                                                "
    - "L1.?[1295,2000] 11ns 35mb                          |----------------------------L1.?-----------------------------| "
    - "Committing partition 1:"
    - "  Soft Deleting 1 files: L1.3"
    - "  Creating 2 files"
    - "**** Simulation run 2, type=split(CompactAndSplitOutput(FoundSubsetLessThanMaxCompactSize))(split_times=[705]). 3 Input Files, 184mb total:"
    - "L1                                                                                                                 "
    - "L1.2[1,1000] 10ns 69mb   |-------------------------------L1.2--------------------------------|                     "
    - "L1.7[1001,1294] 11ns 15mb                                                                     |-------L1.7-------| "
    - "L2                                                                                                                 "
    - "L2.4[1,1294] 5ns 100mb   |------------------------------------------L2.4------------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 184mb total:"
    - "L2                                                                                                                 "
    - "L2.?[1,705] 11ns 100mb   |---------------------L2.?----------------------|                                         "
    - "L2.?[706,1294] 11ns 84mb                                                  |-----------------L2.?-----------------| "
    - "Committing partition 1:"
    - "  Soft Deleting 3 files: L1.2, L2.4, L1.7"
    - "  Creating 2 files"
    - "**** Final Output Files (466mb written)"
    - "L1                                                                                                                 "
    - "L1.8[1295,2000] 11ns 35mb                                      |-------L1.8--------|                               "
    - "L2                                                                                                                 "
    - "L2.5[1295,2587] 5ns 100mb                                      |----------------L2.5----------------|              "
    - "L2.6[2588,3000] 5ns 32mb                                                                              |---L2.6---| "
    - "L2.9[1,705] 11ns 100mb   |-------L2.9--------|                                                                     "
    - "L2.10[706,1294] 11ns 84mb                     |-----L2.10-----|                                                    "
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
    let sizes = vec![250 * ONE_MB, 52 * ONE_MB];

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
    - "**** Simulation run 1, type=split(CompactAndSplitOutput(FoundSubsetLessThanMaxCompactSize))(split_times=[398, 795]). 3 Input Files, 252mb total:"
    - "L1                                                                                                                 "
    - "L1.3[1,401] 9ns 100mb    |---------------L1.3---------------|                                                      "
    - "L1.4[402,801] 9ns 100mb                                      |--------------L1.4---------------|                   "
    - "L2                                                                                                                 "
    - "L2.2[2,1000] 8ns 52mb    |-----------------------------------------L2.2------------------------------------------| "
    - "**** 3 Output Files (parquet_file_id not yet assigned), 252mb total:"
    - "L2                                                                                                                 "
    - "L2.?[1,398] 9ns 100mb    |--------------L2.?---------------|                                                       "
    - "L2.?[399,795] 9ns 100mb                                     |--------------L2.?---------------|                    "
    - "L2.?[796,1000] 9ns 52mb                                                                         |------L2.?------| "
    - "Committing partition 1:"
    - "  Soft Deleting 3 files: L2.2, L1.3, L1.4"
    - "  Creating 3 files"
    - "**** Final Output Files (502mb written)"
    - "L1                                                                                                                 "
    - "L1.5[802,1000] 9ns 50mb                                                                          |-----L1.5------| "
    - "L2                                                                                                                 "
    - "L2.6[1,398] 9ns 100mb    |--------------L2.6---------------|                                                       "
    - "L2.7[399,795] 9ns 100mb                                     |--------------L2.7---------------|                    "
    - "L2.8[796,1000] 9ns 52mb                                                                         |------L2.8------| "
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
    let sizes = vec![250 * ONE_MB, 52 * ONE_MB];

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
    let sizes = vec![250 * ONE_MB, 52 * ONE_MB];

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
    - "**** Simulation run 1, type=split(CompactAndSplitOutput(TotalSizeLessThanMaxCompactSize))(split_times=[2132, 2983]). 3 Input Files, 202mb total:"
    - "L1                                                                                                                 "
    - "L1.5[1761,2000] 1ns 50mb                          |---L1.5---|                                                     "
    - "L1.4[1281,1760] 1ns 100mb|---------L1.4----------|                                                                 "
    - "L2                                                                                                                 "
    - "L2.2[1600,3000] 1ns 52mb                 |---------------------------------L2.2----------------------------------| "
    - "**** 3 Output Files (parquet_file_id not yet assigned), 202mb total:"
    - "L2                                                                                                                 "
    - "L2.?[1281,2132] 1ns 100mb|-------------------L2.?-------------------|                                              "
    - "L2.?[2133,2983] 1ns 100mb                                            |-------------------L2.?-------------------|  "
    - "L2.?[2984,3000] 1ns 2mb                                                                                           |L2.?|"
    - "Committing partition 1:"
    - "  Soft Deleting 3 files: L2.2, L1.4, L1.5"
    - "  Upgrading 1 files level to CompactionLevel::L2: L1.3"
    - "  Creating 3 files"
    - "**** Final Output Files (452mb written)"
    - "L2                                                                                                                 "
    - "L2.3[800,1280] 1ns 100mb |------L2.3-------|                                                                       "
    - "L2.6[1281,2132] 1ns 100mb                   |--------------L2.6--------------|                                     "
    - "L2.7[2133,2983] 1ns 100mb                                                      |--------------L2.7--------------|  "
    - "L2.8[2984,3000] 1ns 2mb                                                                                           |L2.8|"
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
    let sizes = vec![250 * ONE_MB, 52 * ONE_MB];

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
    - "L1.?[121,240] 9ns 99mb                                       |--------------L1.?---------------|                   "
    - "L1.?[241,300] 9ns 51mb                                                                           |-----L1.?------| "
    - "Committing partition 1:"
    - "  Soft Deleting 1 files: L1.1"
    - "  Creating 3 files"
    - "**** Simulation run 1, type=split(CompactAndSplitOutput(TotalSizeLessThanMaxCompactSize))(split_times=[705, 1289]). 3 Input Files, 202mb total:"
    - "L1                                                                                                                 "
    - "L1.5[241,300] 9ns 51mb            |L1.5|                                                                           "
    - "L1.4[121,240] 9ns 99mb   |-L1.4--|                                                                                 "
    - "L2                                                                                                                 "
    - "L2.2[200,1300] 8ns 52mb        |--------------------------------------L2.2---------------------------------------| "
    - "**** 3 Output Files (parquet_file_id not yet assigned), 202mb total:"
    - "L2                                                                                                                 "
    - "L2.?[121,705] 9ns 100mb  |-------------------L2.?-------------------|                                              "
    - "L2.?[706,1289] 9ns 100mb                                             |-------------------L2.?-------------------|  "
    - "L2.?[1290,1300] 9ns 2mb                                                                                           |L2.?|"
    - "Committing partition 1:"
    - "  Soft Deleting 3 files: L2.2, L1.4, L1.5"
    - "  Upgrading 1 files level to CompactionLevel::L2: L1.3"
    - "  Creating 3 files"
    - "**** Final Output Files (452mb written)"
    - "L2                                                                                                                 "
    - "L2.3[0,120] 9ns 100mb    |-L2.3-|                                                                                  "
    - "L2.6[121,705] 9ns 100mb          |-----------------L2.6-----------------|                                          "
    - "L2.7[706,1289] 9ns 100mb                                                 |-----------------L2.7-----------------|  "
    - "L2.8[1290,1300] 9ns 2mb                                                                                           |L2.8|"
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
    - "SKIPPED COMPACTION for PartitionId(1): partition 1 has overlapped files that exceed max compact size limit 314572800. This may happen if a large amount of data has the same timestamp"
    - "**** Final Output Files (0b written)"
    - "L1                                                                                                                 "
    - "L1.1[1,2] 10ns 250mb     |L1.1|                                                                                    "
    - "L2                                                                                                                 "
    - "L2.2[1,1000] 5ns 52mb    |------------------------------------------L2.2------------------------------------------|"
    - "WARNING: file L1.1[1,2] 10ns 250mb exceeds soft limit 100mb by more than 50%"
    "###
    );
}
