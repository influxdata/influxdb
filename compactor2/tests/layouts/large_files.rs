//! layout tests for scenarios with large input files
//!
//! See [crate::layout] module for detailed documentation

use data_types::CompactionLevel;

use crate::layouts::{layout_setup_builder, parquet_builder, run_layout_scenario, ONE_MB};

const MAX_COMPACT_SIZE: usize = 300 * ONE_MB as usize;
const MAX_DESIRED_FILE_SIZE: u64 = 100 * ONE_MB;

// This file should be upgraded after https://github.com/influxdata/idpe/issues/17246
// One l1 file that is larger than max desired file size
#[tokio::test]
async fn one_larger_max_file_size() {
    test_helpers::maybe_start_logging();

    let setup = layout_setup_builder()
        .await
        .with_max_compact_size(MAX_COMPACT_SIZE)
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

    insta::assert_yaml_snapshot!(
        run_layout_scenario(&setup).await,
        @r###"
    ---
    - "**** Input Files "
    - "L1, all files 100mb                                                                                 "
    - "L1.1[1,1000]        |-------------------------------------L1.1-------------------------------------|"
    - "SKIPPED COMPACTION for PartitionId(1): partition 1 has overlapped files that exceed max compact size limit 314572800. The may happen if a large amount of data has the same timestamp"
    - "**** Final Output Files "
    - "L1, all files 100mb                                                                                 "
    - "L1.1[1,1000]        |-------------------------------------L1.1-------------------------------------|"
    "###
    );
}

// One l0 file that is larger than max desired file size
#[tokio::test]
async fn one_l0_larger_max_file_size() {
    test_helpers::maybe_start_logging();

    let setup = layout_setup_builder()
        .await
        .with_max_compact_size(MAX_COMPACT_SIZE)
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

    insta::assert_yaml_snapshot!(
        run_layout_scenario(&setup).await,
        @r###"
    ---
    - "**** Input Files "
    - "L0, all files 100mb                                                                                 "
    - "L0.1[1,1000]        |-------------------------------------L0.1-------------------------------------|"
    - "SKIPPED COMPACTION for PartitionId(1): partition 1 has overlapped files that exceed max compact size limit 314572800. The may happen if a large amount of data has the same timestamp"
    - "**** Final Output Files "
    - "L0, all files 100mb                                                                                 "
    - "L0.1[1,1000]        |-------------------------------------L0.1-------------------------------------|"
    "###
    );
}

// This file should be upgraded after https://github.com/influxdata/idpe/issues/17246
// One l1 file that is larger than max compact size
#[tokio::test]
async fn one_larger_max_compact_size() {
    test_helpers::maybe_start_logging();

    let setup = layout_setup_builder()
        .await
        .with_max_compact_size(MAX_COMPACT_SIZE)
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
                .with_file_size_bytes((MAX_COMPACT_SIZE + 1) as u64),
        )
        .await;

    insta::assert_yaml_snapshot!(
        run_layout_scenario(&setup).await,
        @r###"
    ---
    - "**** Input Files "
    - "L1, all files 300mb                                                                                 "
    - "L1.1[1,1000]        |-------------------------------------L1.1-------------------------------------|"
    - "WARNING: file L1.1[1,1000] 300mb exceeds soft limit 100mb by more than 50%"
    - "SKIPPED COMPACTION for PartitionId(1): partition 1 has overlapped files that exceed max compact size limit 314572800. The may happen if a large amount of data has the same timestamp"
    - "**** Final Output Files "
    - "L1, all files 300mb                                                                                 "
    - "L1.1[1,1000]        |-------------------------------------L1.1-------------------------------------|"
    - "WARNING: file L1.1[1,1000] 300mb exceeds soft limit 100mb by more than 50%"
    "###
    );
}

// This file should be upgraded after https://github.com/influxdata/idpe/issues/17246
// One l0 file that is larger than max compact size
#[tokio::test]
async fn one_l0_larger_max_compact_size() {
    test_helpers::maybe_start_logging();

    let setup = layout_setup_builder()
        .await
        .with_max_compact_size(MAX_COMPACT_SIZE)
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
                // file > max_desired_file_size_bytes
                .with_file_size_bytes((MAX_COMPACT_SIZE + 1) as u64),
        )
        .await;

    insta::assert_yaml_snapshot!(
        run_layout_scenario(&setup).await,
        @r###"
    ---
    - "**** Input Files "
    - "L0, all files 300mb                                                                                 "
    - "L0.1[1,1000]        |-------------------------------------L0.1-------------------------------------|"
    - "WARNING: file L0.1[1,1000] 300mb exceeds soft limit 100mb by more than 50%"
    - "SKIPPED COMPACTION for PartitionId(1): partition 1 has overlapped files that exceed max compact size limit 314572800. The may happen if a large amount of data has the same timestamp"
    - "**** Final Output Files "
    - "L0, all files 300mb                                                                                 "
    - "L0.1[1,1000]        |-------------------------------------L0.1-------------------------------------|"
    - "WARNING: file L0.1[1,1000] 300mb exceeds soft limit 100mb by more than 50%"
    "###
    );
}

// This is working as expected and should stay after https://github.com/influxdata/idpe/issues/17246
// Two files that are under max compact size
#[tokio::test]
async fn two_large_files_total_under_max_compact_size() {
    test_helpers::maybe_start_logging();

    let setup = layout_setup_builder()
        .await
        .with_max_compact_size(MAX_COMPACT_SIZE)
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
                    .with_file_size_bytes(size),
            )
            .await;
    }

    insta::assert_yaml_snapshot!(
        run_layout_scenario(&setup).await,
        @r###"
    ---
    - "**** Input Files "
    - "L1, all files 100mb                                                                                 "
    - "L1.1[1,1000]        |-------------------------------------L1.1-------------------------------------|"
    - "L2, all files 100mb                                                                                 "
    - "L2.2[2,1000]        |------------------------------------L2.2-------------------------------------| "
    - "**** Simulation run 0, type=split(split_times=[501]). 2 Input Files, 200mb total:"
    - "L1, all files 100mb                                                                                 "
    - "L1.1[1,1000]        |-------------------------------------L1.1-------------------------------------|"
    - "L2, all files 100mb                                                                                 "
    - "L2.2[2,1000]        |------------------------------------L2.2-------------------------------------| "
    - "**** 2 Output Files (parquet_file_id not yet assigned), 199.8mb total:"
    - "L2                                                                                                  "
    - "L2.?[1,501] 100.1mb |-----------------L2.?-----------------|                                        "
    - "L2.?[502,1000] 99.7mb                                        |----------------L2.?-----------------| "
    - "Committing partition 1:"
    - "  Soft Deleting 2 files: L1.1, L2.2"
    - "  Creating 2 files at level CompactionLevel::L2"
    - "**** Final Output Files "
    - "L2                                                                                                  "
    - "L2.3[1,501] 100.1mb |-----------------L2.3-----------------|                                        "
    - "L2.4[502,1000] 99.7mb                                        |----------------L2.4-----------------| "
    "###
    );
}

// These files should be split and then compacted after https://github.com/influxdata/idpe/issues/17246
// Two similar size and time range files with total size larger than max compact size
#[tokio::test]
async fn two_large_files_total_over_max_compact_size() {
    test_helpers::maybe_start_logging();

    let setup = layout_setup_builder()
        .await
        .with_max_compact_size(MAX_COMPACT_SIZE)
        .with_max_desired_file_size_bytes(MAX_DESIRED_FILE_SIZE)
        .build()
        .await;

    let size = MAX_COMPACT_SIZE / 2 + 10;

    for i in 1..=2 {
        setup
            .partition
            .create_parquet_file(
                parquet_builder()
                    .with_min_time(i)
                    .with_max_time(1000)
                    // L1.1 or  L2.2
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
    - "L1, all files 150mb                                                                                 "
    - "L1.1[1,1000]        |-------------------------------------L1.1-------------------------------------|"
    - "L2, all files 150mb                                                                                 "
    - "L2.2[2,1000]        |------------------------------------L2.2-------------------------------------| "
    - "WARNING: file L1.1[1,1000] 150mb exceeds soft limit 100mb by more than 50%"
    - "WARNING: file L2.2[2,1000] 150mb exceeds soft limit 100mb by more than 50%"
    - "SKIPPED COMPACTION for PartitionId(1): partition 1 has overlapped files that exceed max compact size limit 314572800. The may happen if a large amount of data has the same timestamp"
    - "**** Final Output Files "
    - "L1, all files 150mb                                                                                 "
    - "L1.1[1,1000]        |-------------------------------------L1.1-------------------------------------|"
    - "L2, all files 150mb                                                                                 "
    - "L2.2[2,1000]        |------------------------------------L2.2-------------------------------------| "
    - "WARNING: file L1.1[1,1000] 150mb exceeds soft limit 100mb by more than 50%"
    - "WARNING: file L2.2[2,1000] 150mb exceeds soft limit 100mb by more than 50%"
    "###
    );
}

// These files should be split and then compacted after https://github.com/influxdata/idpe/issues/17246
// Two similar size files with total size larger than max compact size with small overlap range
// The time range of target level file is much smaller and at the end range of the start level file
#[tokio::test]
async fn two_large_files_total_over_max_compact_size_small_overlap_range() {
    test_helpers::maybe_start_logging();

    let setup = layout_setup_builder()
        .await
        .with_max_compact_size(MAX_COMPACT_SIZE)
        .with_max_desired_file_size_bytes(MAX_DESIRED_FILE_SIZE)
        .build()
        .await;

    let size = MAX_COMPACT_SIZE / 2 + 10;

    for i in 1..=2 {
        setup
            .partition
            .create_parquet_file(
                parquet_builder()
                    .with_min_time((i - 1) * 800)
                    .with_max_time(1000)
                    // L1.1 or  L2.2
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
    - "L1, all files 150mb                                                                                 "
    - "L1.1[0,1000]        |-------------------------------------L1.1-------------------------------------|"
    - "L2, all files 150mb                                                                                 "
    - "L2.2[800,1000]                                                                      |-----L2.2-----|"
    - "WARNING: file L1.1[0,1000] 150mb exceeds soft limit 100mb by more than 50%"
    - "WARNING: file L2.2[800,1000] 150mb exceeds soft limit 100mb by more than 50%"
    - "SKIPPED COMPACTION for PartitionId(1): partition 1 has overlapped files that exceed max compact size limit 314572800. The may happen if a large amount of data has the same timestamp"
    - "**** Final Output Files "
    - "L1, all files 150mb                                                                                 "
    - "L1.1[0,1000]        |-------------------------------------L1.1-------------------------------------|"
    - "L2, all files 150mb                                                                                 "
    - "L2.2[800,1000]                                                                      |-----L2.2-----|"
    - "WARNING: file L1.1[0,1000] 150mb exceeds soft limit 100mb by more than 50%"
    - "WARNING: file L2.2[800,1000] 150mb exceeds soft limit 100mb by more than 50%"
    "###
    );
}

// These files should be split and then compacted after https://github.com/influxdata/idpe/issues/17246
// Two similar size files with total size larger than max compact size with small overlap range
// The overlapped range is at the end range of start_level file and start of target level file
// Two files have similar length of time range
#[tokio::test]
async fn two_large_files_total_over_max_compact_size_small_overlap_range_2() {
    test_helpers::maybe_start_logging();

    let setup = layout_setup_builder()
        .await
        .with_max_compact_size(MAX_COMPACT_SIZE)
        .with_max_desired_file_size_bytes(MAX_DESIRED_FILE_SIZE)
        .build()
        .await;

    let size = MAX_COMPACT_SIZE / 2 + 10;

    for i in 1..=2 {
        setup
            .partition
            .create_parquet_file(
                parquet_builder()
                    .with_min_time(i * 800)
                    .with_max_time((i + 1) * 1000)
                    // L1.1 or  L2.2
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
    - "L1, all files 150mb                                                                                 "
    - "L1.1[800,2000]      |------------------L1.1-------------------|                                     "
    - "L2, all files 150mb                                                                                 "
    - "L2.2[1600,3000]                                  |----------------------L2.2----------------------| "
    - "WARNING: file L1.1[800,2000] 150mb exceeds soft limit 100mb by more than 50%"
    - "WARNING: file L2.2[1600,3000] 150mb exceeds soft limit 100mb by more than 50%"
    - "SKIPPED COMPACTION for PartitionId(1): partition 1 has overlapped files that exceed max compact size limit 314572800. The may happen if a large amount of data has the same timestamp"
    - "**** Final Output Files "
    - "L1, all files 150mb                                                                                 "
    - "L1.1[800,2000]      |------------------L1.1-------------------|                                     "
    - "L2, all files 150mb                                                                                 "
    - "L2.2[1600,3000]                                  |----------------------L2.2----------------------| "
    - "WARNING: file L1.1[800,2000] 150mb exceeds soft limit 100mb by more than 50%"
    - "WARNING: file L2.2[1600,3000] 150mb exceeds soft limit 100mb by more than 50%"
    "###
    );
}

// These files should be split and then compacted after https://github.com/influxdata/idpe/issues/17246
// Two similar size files with total size larger than max compact size with small overlap range
// The overlapped range is at the end range of start_level file and start of target level file
// Time range of the start level file is much smaller than the one of target level file
#[tokio::test]
async fn two_large_files_total_over_max_compact_size_small_overlap_range_3() {
    test_helpers::maybe_start_logging();

    let setup = layout_setup_builder()
        .await
        .with_max_compact_size(MAX_COMPACT_SIZE)
        .with_max_desired_file_size_bytes(MAX_DESIRED_FILE_SIZE)
        .build()
        .await;

    let size = MAX_COMPACT_SIZE / 2 + 10;

    for i in 1..=2 {
        setup
            .partition
            .create_parquet_file(
                parquet_builder()
                    .with_min_time((i - 1) * 200)
                    .with_max_time((i - 1) * 1000 + 300)
                    // L1.1 or  L2.2
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
    - "L1, all files 150mb                                                                                 "
    - "L1.1[0,300]         |------L1.1------|                                                              "
    - "L2, all files 150mb                                                                                 "
    - "L2.2[200,1300]                  |------------------------------L2.2-------------------------------| "
    - "WARNING: file L1.1[0,300] 150mb exceeds soft limit 100mb by more than 50%"
    - "WARNING: file L2.2[200,1300] 150mb exceeds soft limit 100mb by more than 50%"
    - "SKIPPED COMPACTION for PartitionId(1): partition 1 has overlapped files that exceed max compact size limit 314572800. The may happen if a large amount of data has the same timestamp"
    - "**** Final Output Files "
    - "L1, all files 150mb                                                                                 "
    - "L1.1[0,300]         |------L1.1------|                                                              "
    - "L2, all files 150mb                                                                                 "
    - "L2.2[200,1300]                  |------------------------------L2.2-------------------------------| "
    - "WARNING: file L1.1[0,300] 150mb exceeds soft limit 100mb by more than 50%"
    - "WARNING: file L2.2[200,1300] 150mb exceeds soft limit 100mb by more than 50%"
    "###
    );
}

// These files should be split and then compacted after https://github.com/influxdata/idpe/issues/17246
// Two similar size files with total size larger than max compact size and similar time range
// Start level is 0
#[tokio::test]
async fn two_large_files_total_over_max_compact_size_start_l0() {
    test_helpers::maybe_start_logging();

    let setup = layout_setup_builder()
        .await
        .with_max_compact_size(MAX_COMPACT_SIZE)
        .with_max_desired_file_size_bytes(MAX_DESIRED_FILE_SIZE)
        .build()
        .await;

    let size = MAX_COMPACT_SIZE / 2 + 10;

    for i in 0..=1 {
        setup
            .partition
            .create_parquet_file(
                parquet_builder()
                    .with_min_time(i)
                    .with_max_time(1000)
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
    - "L0, all files 150mb                                                                                 "
    - "L0.1[0,1000]        |-------------------------------------L0.1-------------------------------------|"
    - "L1, all files 150mb                                                                                 "
    - "L1.2[1,1000]        |------------------------------------L1.2-------------------------------------| "
    - "WARNING: file L0.1[0,1000] 150mb exceeds soft limit 100mb by more than 50%"
    - "WARNING: file L1.2[1,1000] 150mb exceeds soft limit 100mb by more than 50%"
    - "SKIPPED COMPACTION for PartitionId(1): partition 1 has overlapped files that exceed max compact size limit 314572800. The may happen if a large amount of data has the same timestamp"
    - "**** Final Output Files "
    - "L0, all files 150mb                                                                                 "
    - "L0.1[0,1000]        |-------------------------------------L0.1-------------------------------------|"
    - "L1, all files 150mb                                                                                 "
    - "L1.2[1,1000]        |------------------------------------L1.2-------------------------------------| "
    - "WARNING: file L0.1[0,1000] 150mb exceeds soft limit 100mb by more than 50%"
    - "WARNING: file L1.2[1,1000] 150mb exceeds soft limit 100mb by more than 50%"
    "###
    );
}

// These files should be split and then compacted after https://github.com/influxdata/idpe/issues/17246
// Real-life case with three good size L1s and one very large L2
#[tokio::test]
async fn target_too_large_1() {
    test_helpers::maybe_start_logging();

    let setup = layout_setup_builder()
        .await
        .with_max_compact_size(MAX_COMPACT_SIZE)
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
                    .with_file_size_bytes(l1_sizes[i as usize]),
            )
            .await;
    }

    insta::assert_yaml_snapshot!(
        run_layout_scenario(&setup).await,
        @r###"
    ---
    - "**** Input Files "
    - "L1                                                                                                  "
    - "L1.2[1,1000] 53mb   |----------L1.2----------|                                                      "
    - "L1.3[1001,2000] 45mb                          |----------L1.3----------|                            "
    - "L1.4[2001,3000] 5mb                                                      |----------L1.4----------| "
    - "L2                                                                                                  "
    - "L2.1[1,1000] 253mb  |----------L2.1----------|                                                      "
    - "WARNING: file L2.1[1,1000] 253mb exceeds soft limit 100mb by more than 50%"
    - "SKIPPED COMPACTION for PartitionId(1): partition 1 has overlapped files that exceed max compact size limit 314572800. The may happen if a large amount of data has the same timestamp"
    - "**** Final Output Files "
    - "L1                                                                                                  "
    - "L1.2[1,1000] 53mb   |----------L1.2----------|                                                      "
    - "L1.3[1001,2000] 45mb                          |----------L1.3----------|                            "
    - "L1.4[2001,3000] 5mb                                                      |----------L1.4----------| "
    - "L2                                                                                                  "
    - "L2.1[1,1000] 253mb  |----------L2.1----------|                                                      "
    - "WARNING: file L2.1[1,1000] 253mb exceeds soft limit 100mb by more than 50%"
    "###
    );
}

// These files should be split and then compacted after https://github.com/influxdata/idpe/issues/17246
// Real-life case with two good size L1s and one very large L2
#[tokio::test]
async fn target_too_large_2() {
    test_helpers::maybe_start_logging();

    let setup = layout_setup_builder()
        .await
        .with_max_compact_size(MAX_COMPACT_SIZE)
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
                    .with_file_size_bytes(l1_sizes[i as usize]),
            )
            .await;
    }

    insta::assert_yaml_snapshot!(
        run_layout_scenario(&setup).await,
        @r###"
    ---
    - "**** Input Files "
    - "L1                                                                                                  "
    - "L1.2[1,1000] 69mb   |----------L1.2----------|                                                      "
    - "L1.3[1001,2000] 50mb                          |----------L1.3----------|                            "
    - "L2                                                                                                  "
    - "L2.1[1,3000] 232mb  |-------------------------------------L2.1-------------------------------------|"
    - "WARNING: file L2.1[1,3000] 232mb exceeds soft limit 100mb by more than 50%"
    - "SKIPPED COMPACTION for PartitionId(1): partition 1 has overlapped files that exceed max compact size limit 314572800. The may happen if a large amount of data has the same timestamp"
    - "**** Final Output Files "
    - "L1                                                                                                  "
    - "L1.2[1,1000] 69mb   |----------L1.2----------|                                                      "
    - "L1.3[1001,2000] 50mb                          |----------L1.3----------|                            "
    - "L2                                                                                                  "
    - "L2.1[1,3000] 232mb  |-------------------------------------L2.1-------------------------------------|"
    - "WARNING: file L2.1[1,3000] 232mb exceeds soft limit 100mb by more than 50%"
    "###
    );
}

// These files should be split and then compacted after https://github.com/influxdata/idpe/issues/17246
// One very large start level file with one good size overlapped target level file
// Two have similar time range
#[tokio::test]
async fn start_too_large_similar_time_range() {
    test_helpers::maybe_start_logging();

    let setup = layout_setup_builder()
        .await
        .with_max_compact_size(MAX_COMPACT_SIZE)
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
                    .with_file_size_bytes(sizes[(i - 1) as usize]),
            )
            .await;
    }

    insta::assert_yaml_snapshot!(
        run_layout_scenario(&setup).await,
        @r###"
    ---
    - "**** Input Files "
    - "L1                                                                                                  "
    - "L1.1[1,1000] 250mb  |-------------------------------------L1.1-------------------------------------|"
    - "L2                                                                                                  "
    - "L2.2[2,1000] 52mb   |------------------------------------L2.2-------------------------------------| "
    - "WARNING: file L1.1[1,1000] 250mb exceeds soft limit 100mb by more than 50%"
    - "SKIPPED COMPACTION for PartitionId(1): partition 1 has overlapped files that exceed max compact size limit 314572800. The may happen if a large amount of data has the same timestamp"
    - "**** Final Output Files "
    - "L1                                                                                                  "
    - "L1.1[1,1000] 250mb  |-------------------------------------L1.1-------------------------------------|"
    - "L2                                                                                                  "
    - "L2.2[2,1000] 52mb   |------------------------------------L2.2-------------------------------------| "
    - "WARNING: file L1.1[1,1000] 250mb exceeds soft limit 100mb by more than 50%"
    "###
    );
}

// These files should be split and then compacted after https://github.com/influxdata/idpe/issues/17246
// One very large start level file with one good size overlapped target level file
// Overlapped range is small
// The overlapped range is at the end of both start_level file and target level file
#[tokio::test]
async fn start_too_large_small_time_range() {
    test_helpers::maybe_start_logging();

    let setup = layout_setup_builder()
        .await
        .with_max_compact_size(MAX_COMPACT_SIZE)
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
                    .with_file_size_bytes(sizes[(i - 1) as usize]),
            )
            .await;
    }

    insta::assert_yaml_snapshot!(
        run_layout_scenario(&setup).await,
        @r###"
    ---
    - "**** Input Files "
    - "L1                                                                                                  "
    - "L1.1[0,1000] 250mb  |-------------------------------------L1.1-------------------------------------|"
    - "L2                                                                                                  "
    - "L2.2[800,1000] 52mb                                                                 |-----L2.2-----|"
    - "WARNING: file L1.1[0,1000] 250mb exceeds soft limit 100mb by more than 50%"
    - "SKIPPED COMPACTION for PartitionId(1): partition 1 has overlapped files that exceed max compact size limit 314572800. The may happen if a large amount of data has the same timestamp"
    - "**** Final Output Files "
    - "L1                                                                                                  "
    - "L1.1[0,1000] 250mb  |-------------------------------------L1.1-------------------------------------|"
    - "L2                                                                                                  "
    - "L2.2[800,1000] 52mb                                                                 |-----L2.2-----|"
    - "WARNING: file L1.1[0,1000] 250mb exceeds soft limit 100mb by more than 50%"
    "###
    );
}

// These files should be split and then compacted after https://github.com/influxdata/idpe/issues/17246
// One very large start level file with one good size overlapped target level file
// Overlapped range is small
// The overlapped range is at the end of start_level file and start of target level file
#[tokio::test]
async fn start_too_large_small_time_range_2() {
    test_helpers::maybe_start_logging();

    let setup = layout_setup_builder()
        .await
        .with_max_compact_size(MAX_COMPACT_SIZE)
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
    - "L1                                                                                                  "
    - "L1.1[800,2000] 250mb|------------------L1.1-------------------|                                     "
    - "L2                                                                                                  "
    - "L2.2[1600,3000] 52mb                             |----------------------L2.2----------------------| "
    - "WARNING: file L1.1[800,2000] 250mb exceeds soft limit 100mb by more than 50%"
    - "SKIPPED COMPACTION for PartitionId(1): partition 1 has overlapped files that exceed max compact size limit 314572800. The may happen if a large amount of data has the same timestamp"
    - "**** Final Output Files "
    - "L1                                                                                                  "
    - "L1.1[800,2000] 250mb|------------------L1.1-------------------|                                     "
    - "L2                                                                                                  "
    - "L2.2[1600,3000] 52mb                             |----------------------L2.2----------------------| "
    - "WARNING: file L1.1[800,2000] 250mb exceeds soft limit 100mb by more than 50%"
    "###
    );
}

// These files should be split and then compacted after https://github.com/influxdata/idpe/issues/17246
// One very large start level file with one good size overlapped target level file
// Overlapped range is small
// The overlapped range is at the end of start_level file and start of target level file
// Time range of start level is much larger than target level
#[tokio::test]
async fn start_too_large_small_time_range_3() {
    test_helpers::maybe_start_logging();

    let setup = layout_setup_builder()
        .await
        .with_max_compact_size(MAX_COMPACT_SIZE)
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
                    .with_file_size_bytes(sizes[(i - 1) as usize]),
            )
            .await;
    }

    insta::assert_yaml_snapshot!(
        run_layout_scenario(&setup).await,
        @r###"
    ---
    - "**** Input Files "
    - "L1                                                                                                  "
    - "L1.1[0,300] 250mb   |------L1.1------|                                                              "
    - "L2                                                                                                  "
    - "L2.2[200,1300] 52mb             |------------------------------L2.2-------------------------------| "
    - "WARNING: file L1.1[0,300] 250mb exceeds soft limit 100mb by more than 50%"
    - "SKIPPED COMPACTION for PartitionId(1): partition 1 has overlapped files that exceed max compact size limit 314572800. The may happen if a large amount of data has the same timestamp"
    - "**** Final Output Files "
    - "L1                                                                                                  "
    - "L1.1[0,300] 250mb   |------L1.1------|                                                              "
    - "L2                                                                                                  "
    - "L2.2[200,1300] 52mb             |------------------------------L2.2-------------------------------| "
    - "WARNING: file L1.1[0,300] 250mb exceeds soft limit 100mb by more than 50%"
    "###
    );
}
