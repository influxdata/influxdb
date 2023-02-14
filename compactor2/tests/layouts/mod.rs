//! IOx Compactor Layout tests
//!
//! These tests do almost everything the compactor would do in a
//! production system *except* for reading/writing parquet data.
//!
//! The input to each test is the parquet file layout of a partition.
//!
//! The output is a representation of the steps the compactor chose to
//! take and the final layout of the parquet files in the partition.
//!
//! # Interpreting test lines
//!
//! This test uses `insta` to compare inlined string represetention of
//! what the compactor did.
//!
//! Each line in the representation represents either some metadata or
//! a parquet file, with a visual depiction of its `min_time` and
//! `max_time` (the minimum timestamp and maximum timestamp for data
//! in the file).
//!
//! For example:
//!
//! ```text
//!     - L0.3[300,350] 5kb                                           |-L0.3-|
//! ```
//!
//! Represents the following [`ParquetFile`]:
//!
//! ```text
//! ParquetFile {
//!  id: 3,
//!  compaction_level: L0
//!  min_time: 300,
//!  max_time: 350
//!  file_size_bytes: 5*1024
//! }
//! ```
//!
//! The `|-L0.3-|` shows the relative location of `min_time` (`|-`)
//! and `max_time (`-|`) on a time line to help visualize the output
use std::time::Duration;

use compactor2::config::AlgoVersion;
use compactor2_test_utils::{format_files, TestSetup, TestSetupBuilder};
use data_types::{CompactionLevel, ParquetFile};
use iox_tests::TestParquetFileBuilder;

const ONE_MB: u64 = 1024 * 1024;

/// creates a TestParquetFileBuilder setup for layout tests
fn parquet_builder() -> TestParquetFileBuilder {
    TestParquetFileBuilder::default()
        .with_compaction_level(CompactionLevel::Initial)
        // need some LP to generate the schema
        .with_line_protocol("table,tag1=A,tag2=B,tag3=C field_int=1i 100")
}

/// Creates the default TestSetupBuilder for layout tests
///
/// NOTE: The builder is configured with parameters that are intended
/// to be as close as possible to what is configured on production
/// systems so that we can predict and reason about what the compactor
/// will do in production.
async fn layout_setup_builder() -> TestSetupBuilder<false> {
    TestSetup::builder()
        .await
        .with_compact_version(AlgoVersion::TargetLevel)
        .with_percentage_max_file_size(20)
        .with_split_percentage(80)
        .with_max_input_files_per_partition(200)
        .with_max_input_parquet_bytes_per_partition(256 * ONE_MB as usize)
        .with_min_num_l1_files_to_compact(10)
        .with_max_desired_file_size_bytes(100 * ONE_MB)
        .simulate_without_object_store()
}

#[tokio::test]
async fn all_overlapping_l0() {
    test_helpers::maybe_start_logging();

    let setup = layout_setup_builder().await.build().await;

    // create virtual files
    for _ in 0..10 {
        setup
            .partition
            .create_parquet_file(
                parquet_builder()
                    .with_min_time(100)
                    .with_max_time(200)
                    .with_file_size_bytes(9 * ONE_MB),
            )
            .await;
    }

    insta::assert_yaml_snapshot!(
        run_layout_scenario(&setup).await,
        @r###"
    ---
    - "**** Input Files "
    - "L0, all files 9mb                                                                                   "
    - "L0.1[100,200]       |-------------------------------------L0.1-------------------------------------|"
    - "L0.2[100,200]       |-------------------------------------L0.2-------------------------------------|"
    - "L0.3[100,200]       |-------------------------------------L0.3-------------------------------------|"
    - "L0.4[100,200]       |-------------------------------------L0.4-------------------------------------|"
    - "L0.5[100,200]       |-------------------------------------L0.5-------------------------------------|"
    - "L0.6[100,200]       |-------------------------------------L0.6-------------------------------------|"
    - "L0.7[100,200]       |-------------------------------------L0.7-------------------------------------|"
    - "L0.8[100,200]       |-------------------------------------L0.8-------------------------------------|"
    - "L0.9[100,200]       |-------------------------------------L0.9-------------------------------------|"
    - "L0.10[100,200]      |------------------------------------L0.10-------------------------------------|"
    - "**** Simulation run 0, type=split(split_times=[180]). 10 Input Files, 90mb total:"
    - "L0, all files 9mb                                                                                   "
    - "L0.10[100,200]      |------------------------------------L0.10-------------------------------------|"
    - "L0.9[100,200]       |-------------------------------------L0.9-------------------------------------|"
    - "L0.8[100,200]       |-------------------------------------L0.8-------------------------------------|"
    - "L0.7[100,200]       |-------------------------------------L0.7-------------------------------------|"
    - "L0.6[100,200]       |-------------------------------------L0.6-------------------------------------|"
    - "L0.5[100,200]       |-------------------------------------L0.5-------------------------------------|"
    - "L0.4[100,200]       |-------------------------------------L0.4-------------------------------------|"
    - "L0.3[100,200]       |-------------------------------------L0.3-------------------------------------|"
    - "L0.2[100,200]       |-------------------------------------L0.2-------------------------------------|"
    - "L0.1[100,200]       |-------------------------------------L0.1-------------------------------------|"
    - "**** Final Output Files "
    - "L1                                                                                                  "
    - "L1.11[100,180] 72mb |----------------------------L1.11-----------------------------|                "
    - "L1.12[180,200] 18mb                                                                 |----L1.12-----|"
    "###
    );
}

#[tokio::test]
async fn all_non_overlapping_l0() {
    test_helpers::maybe_start_logging();

    let setup = layout_setup_builder().await.build().await;

    // create virtual files
    for i in 0..10 {
        setup
            .partition
            .create_parquet_file(
                parquet_builder()
                    .with_min_time(100 * i)
                    .with_max_time(100 * i + 1)
                    .with_file_size_bytes(10 * ONE_MB),
            )
            .await;
    }

    insta::assert_yaml_snapshot!(
        run_layout_scenario(&setup).await,
        @r###"
    ---
    - "**** Input Files "
    - "L0, all files 10mb                                                                                  "
    - "L0.1[0,1]           |L0.1|                                                                          "
    - "L0.2[100,101]               |L0.2|                                                                  "
    - "L0.3[200,201]                        |L0.3|                                                         "
    - "L0.4[300,301]                                 |L0.4|                                                "
    - "L0.5[400,401]                                          |L0.5|                                       "
    - "L0.6[500,501]                                                   |L0.6|                              "
    - "L0.7[600,601]                                                            |L0.7|                     "
    - "L0.8[700,701]                                                                     |L0.8|            "
    - "L0.9[800,801]                                                                              |L0.9|   "
    - "L0.10[900,901]                                                                                     |L0.10|"
    - "**** Simulation run 0, type=split(split_times=[720]). 10 Input Files, 100mb total:"
    - "L0, all files 10mb                                                                                  "
    - "L0.10[900,901]                                                                                     |L0.10|"
    - "L0.9[800,801]                                                                              |L0.9|   "
    - "L0.8[700,701]                                                                     |L0.8|            "
    - "L0.7[600,601]                                                            |L0.7|                     "
    - "L0.6[500,501]                                                   |L0.6|                              "
    - "L0.5[400,401]                                          |L0.5|                                       "
    - "L0.4[300,301]                                 |L0.4|                                                "
    - "L0.3[200,201]                        |L0.3|                                                         "
    - "L0.2[100,101]               |L0.2|                                                                  "
    - "L0.1[0,1]           |L0.1|                                                                          "
    - "**** Final Output Files "
    - "L1                                                                                                  "
    - "L1.11[0,720] 79.91mb|----------------------------L1.11----------------------------|                 "
    - "L1.12[720,901] 20.09mb                                                               |----L1.12-----| "
    "###
    );
}

#[tokio::test]
async fn l1_with_overlapping_l0() {
    test_helpers::maybe_start_logging();
    let five_kb = 5 * 1024;

    let setup = layout_setup_builder().await.build().await;

    // Model the expected common case where new data is being written
    // at the end of the partition's time range, in small L0 files.
    //
    // There have been previous compaction rounds which created L1
    // files, and each new L0 file is written that partially overlap
    // with the end L1 and with each other
    for i in 0..2 {
        setup
            .partition
            .create_parquet_file(
                parquet_builder()
                    .with_min_time(50 + i * 50)
                    .with_max_time(100 + i * 50)
                    .with_compaction_level(CompactionLevel::FileNonOverlapped)
                    .with_file_size_bytes(10 * ONE_MB),
            )
            .await;
    }
    // L1 files
    for i in 0..5 {
        setup
            .partition
            .create_parquet_file(
                parquet_builder()
                    .with_min_time(140 + i * 30)
                    .with_max_time(160 + (i + 1) * 30)
                    .with_file_size_bytes(five_kb),
            )
            .await;
    }

    insta::assert_yaml_snapshot!(
        run_layout_scenario(&setup).await,
        @r###"
    ---
    - "**** Input Files "
    - "L0                                                                                                  "
    - "L0.3[140,190] 5kb                              |----L0.3-----|                                      "
    - "L0.4[170,220] 5kb                                       |----L0.4-----|                             "
    - "L0.5[200,250] 5kb                                                 |----L0.5-----|                   "
    - "L0.6[230,280] 5kb                                                          |----L0.6-----|          "
    - "L0.7[260,310] 5kb                                                                   |----L0.7-----| "
    - "L1                                                                                                  "
    - "L1.1[50,100] 10mb   |----L1.1-----|                                                                 "
    - "L1.2[100,150] 10mb                 |----L1.2-----|                                                  "
    - "**** Simulation run 0, type=compact. 6 Input Files, 10.02mb total:"
    - "L0                                                                                                  "
    - "L0.7[260,310] 5kb                                                               |------L0.7-------| "
    - "L0.6[230,280] 5kb                                                    |------L0.6-------|            "
    - "L0.5[200,250] 5kb                                         |------L0.5-------|                       "
    - "L0.4[170,220] 5kb                             |------L0.4-------|                                   "
    - "L0.3[140,190] 5kb                  |------L0.3-------|                                              "
    - "L1                                                                                                  "
    - "L1.2[100,150] 10mb  |------L1.2-------|                                                             "
    - "**** Final Output Files "
    - "L1                                                                                                  "
    - "L1.1[50,100] 10mb   |----L1.1-----|                                                                 "
    - "L1.8[100,310] 10.02mb               |-----------------------------L1.8-----------------------------| "
    "###
    );
}

#[tokio::test]
async fn l1_with_non_overlapping_l0() {
    test_helpers::maybe_start_logging();
    let five_kb = 5 * 1024;

    let setup = layout_setup_builder().await.build().await;

    // Model several non overlapping L1 file and new L0 files written
    // that are not overlapping
    //
    // L1: 10MB, 10MB
    // L0: 5k, 5k, 5k, 5k, 5k (all non overlapping with the L1 files)
    for i in 0..2 {
        setup
            .partition
            .create_parquet_file(
                parquet_builder()
                    .with_min_time(50 + i * 50)
                    .with_max_time(100 + i * 50)
                    .with_compaction_level(CompactionLevel::FileNonOverlapped)
                    .with_file_size_bytes(10 * ONE_MB),
            )
            .await;
    }
    for i in 0..5 {
        setup
            .partition
            .create_parquet_file(
                parquet_builder()
                    .with_min_time(300 + i * 50)
                    .with_max_time(350 + i * 50)
                    .with_file_size_bytes(five_kb),
            )
            .await;
    }

    insta::assert_yaml_snapshot!(
        run_layout_scenario(&setup).await,
        @r###"
    ---
    - "**** Input Files "
    - "L0                                                                                                  "
    - "L0.3[300,350] 5kb                                           |-L0.3-|                                "
    - "L0.4[350,400] 5kb                                                   |-L0.4-|                        "
    - "L0.5[400,450] 5kb                                                           |-L0.5-|                "
    - "L0.6[450,500] 5kb                                                                   |-L0.6-|        "
    - "L0.7[500,550] 5kb                                                                           |-L0.7-|"
    - "L1                                                                                                  "
    - "L1.1[50,100] 10mb   |-L1.1-|                                                                        "
    - "L1.2[100,150] 10mb          |-L1.2-|                                                                "
    - "**** Simulation run 0, type=compact. 5 Input Files, 25kb total:"
    - "L0, all files 5kb                                                                                   "
    - "L0.7[500,550]                                                                       |-----L0.7-----|"
    - "L0.6[450,500]                                                       |-----L0.6-----|                "
    - "L0.5[400,450]                                       |-----L0.5-----|                                "
    - "L0.4[350,400]                       |-----L0.4-----|                                                "
    - "L0.3[300,350]       |-----L0.3-----|                                                                "
    - "**** Final Output Files "
    - "L1                                                                                                  "
    - "L1.1[50,100] 10mb   |-L1.1-|                                                                        "
    - "L1.2[100,150] 10mb          |-L1.2-|                                                                "
    - "L1.8[300,550] 25kb                                          |-----------------L1.8-----------------|"
    "###
    );
}

#[tokio::test]
async fn l1_with_non_overlapping_l0_larger() {
    test_helpers::maybe_start_logging();

    let setup = layout_setup_builder().await.build().await;

    // Model several non overlapping L1 file and new L0 files written
    // that are also not overlapping
    //
    // L1: 20MB, 50MB, 20MB, 3MB
    // L0: 5MB, 5MB, 5MB
    for (i, sz) in [20, 50, 20, 3].iter().enumerate() {
        let i = i as i64;
        setup
            .partition
            .create_parquet_file(
                parquet_builder()
                    .with_min_time(50 + i * 50)
                    .with_max_time(100 + i * 50)
                    .with_compaction_level(CompactionLevel::FileNonOverlapped)
                    .with_file_size_bytes(sz * ONE_MB),
            )
            .await;
    }
    for i in 0..3 {
        setup
            .partition
            .create_parquet_file(
                parquet_builder()
                    .with_min_time(300 + i * 50)
                    .with_max_time(350 + i * 50)
                    .with_file_size_bytes(5 * ONE_MB),
            )
            .await;
    }

    insta::assert_yaml_snapshot!(
        run_layout_scenario(&setup).await,
        @r###"
    ---
    - "**** Input Files "
    - "L0                                                                                                  "
    - "L0.5[300,350] 5mb                                                     |--L0.5--|                    "
    - "L0.6[350,400] 5mb                                                               |--L0.6--|          "
    - "L0.7[400,450] 5mb                                                                         |--L0.7--|"
    - "L1                                                                                                  "
    - "L1.1[50,100] 20mb   |--L1.1--|                                                                      "
    - "L1.2[100,150] 50mb            |--L1.2--|                                                            "
    - "L1.3[150,200] 20mb                      |--L1.3--|                                                  "
    - "L1.4[200,250] 3mb                                 |--L1.4--|                                        "
    - "**** Simulation run 0, type=compact. 3 Input Files, 15mb total:"
    - "L0, all files 5mb                                                                                   "
    - "L0.7[400,450]                                                            |----------L0.7----------| "
    - "L0.6[350,400]                                 |----------L0.6----------|                            "
    - "L0.5[300,350]       |----------L0.5----------|                                                      "
    - "**** Simulation run 1, type=split(split_times=[370]). 5 Input Files, 108mb total:"
    - "L1                                                                                                  "
    - "L1.4[200,250] 3mb                                 |--L1.4--|                                        "
    - "L1.3[150,200] 20mb                      |--L1.3--|                                                  "
    - "L1.2[100,150] 50mb            |--L1.2--|                                                            "
    - "L1.1[50,100] 20mb   |--L1.1--|                                                                      "
    - "L1.8[300,450] 15mb                                                    |------------L1.8------------|"
    - "**** Final Output Files "
    - "L2                                                                                                  "
    - "L2.9[50,370] 86.4mb |-----------------------------L2.9-----------------------------|                "
    - "L2.10[370,450] 21.6mb                                                                |----L2.10-----|"
    "###
    );
}

#[tokio::test]
async fn l1_too_much_with_non_overlapping_l0() {
    test_helpers::maybe_start_logging();

    let setup = layout_setup_builder().await.build().await;

    // If we wait until we have 10 L1 files each is not large
    // enough to upgrade, the total size will be > 256MB and we will
    // skip the partition
    //
    // L1: 90MB, 80MB, 70MB, ..., 70MB
    // L0: ..

    for (i, sz) in [90, 80, 70, 70, 70, 70, 70, 70, 70, 70].iter().enumerate() {
        let i = i as i64;
        setup
            .partition
            .create_parquet_file(
                parquet_builder()
                    .with_min_time(50 + i * 50)
                    .with_max_time(100 + i * 50)
                    .with_compaction_level(CompactionLevel::FileNonOverlapped)
                    .with_file_size_bytes(sz * ONE_MB),
            )
            .await;
    }
    // note these overlap with each other, but not the L1 files
    for _ in 0..3 {
        setup
            .partition
            .create_parquet_file(
                parquet_builder()
                    .with_min_time(600)
                    .with_max_time(650)
                    .with_file_size_bytes(5 * ONE_MB),
            )
            .await;
    }

    insta::assert_yaml_snapshot!(
        run_layout_scenario(&setup).await,
        @r###"
    ---
    - "**** Input Files "
    - "L0                                                                                                  "
    - "L0.11[600,650] 5mb                                                                           |L0.11|"
    - "L0.12[600,650] 5mb                                                                           |L0.12|"
    - "L0.13[600,650] 5mb                                                                           |L0.13|"
    - "L1                                                                                                  "
    - "L1.1[50,100] 90mb   |L1.1|                                                                          "
    - "L1.2[100,150] 80mb        |L1.2|                                                                    "
    - "L1.3[150,200] 70mb               |L1.3|                                                             "
    - "L1.4[200,250] 70mb                      |L1.4|                                                      "
    - "L1.5[250,300] 70mb                            |L1.5|                                                "
    - "L1.6[300,350] 70mb                                   |L1.6|                                         "
    - "L1.7[350,400] 70mb                                          |L1.7|                                  "
    - "L1.8[400,450] 70mb                                                |L1.8|                            "
    - "L1.9[450,500] 70mb                                                       |L1.9|                     "
    - "L1.10[500,550] 70mb                                                             |L1.10|             "
    - "**** Simulation run 0, type=compact. 3 Input Files, 15mb total:"
    - "L0, all files 5mb                                                                                   "
    - "L0.13[600,650]      |------------------------------------L0.13-------------------------------------|"
    - "L0.12[600,650]      |------------------------------------L0.12-------------------------------------|"
    - "L0.11[600,650]      |------------------------------------L0.11-------------------------------------|"
    - "SKIPPED COMPACTION for PartitionId(1): partition 1 has 781189120 parquet file bytes, limit is 268435456"
    - "**** Final Output Files "
    - "L1                                                                                                  "
    - "L1.1[50,100] 90mb   |L1.1|                                                                          "
    - "L1.2[100,150] 80mb        |L1.2|                                                                    "
    - "L1.3[150,200] 70mb               |L1.3|                                                             "
    - "L1.4[200,250] 70mb                      |L1.4|                                                      "
    - "L1.5[250,300] 70mb                            |L1.5|                                                "
    - "L1.6[300,350] 70mb                                   |L1.6|                                         "
    - "L1.7[350,400] 70mb                                          |L1.7|                                  "
    - "L1.8[400,450] 70mb                                                |L1.8|                            "
    - "L1.9[450,500] 70mb                                                       |L1.9|                     "
    - "L1.10[500,550] 70mb                                                             |L1.10|             "
    - "L1.14[600,650] 15mb                                                                          |L1.14|"
    "###
    );
}

#[tokio::test]
// Test that compacts L1 files in second round if their number of files >= min_num_l1_files_to_compact
async fn man_l1_with_non_overlapping_l0() {
    test_helpers::maybe_start_logging();

    let setup = layout_setup_builder().await.build().await;

    // Create 10 L1 files so they will be compacted to L2 because they exceed min_num_l1_files_to_compact
    //
    // L1: 9MB, 8MB, 7MB, ..., 7MB
    // L0: ..

    for (i, sz) in [9, 8, 7, 7, 7, 7, 7, 7, 7, 7].iter().enumerate() {
        let i = i as i64;
        setup
            .partition
            .create_parquet_file(
                parquet_builder()
                    .with_min_time(50 + i * 50)
                    .with_max_time(100 + i * 50)
                    .with_compaction_level(CompactionLevel::FileNonOverlapped)
                    .with_file_size_bytes(sz * ONE_MB),
            )
            .await;
    }
    // note these overlap with each other, but not the L1 files
    for _ in 0..3 {
        setup
            .partition
            .create_parquet_file(
                parquet_builder()
                    .with_min_time(600)
                    .with_max_time(650)
                    .with_file_size_bytes(5 * ONE_MB),
            )
            .await;
    }

    insta::assert_yaml_snapshot!(
        run_layout_scenario(&setup).await,
        @r###"
    ---
    - "**** Input Files "
    - "L0                                                                                                  "
    - "L0.11[600,650] 5mb                                                                           |L0.11|"
    - "L0.12[600,650] 5mb                                                                           |L0.12|"
    - "L0.13[600,650] 5mb                                                                           |L0.13|"
    - "L1                                                                                                  "
    - "L1.1[50,100] 9mb    |L1.1|                                                                          "
    - "L1.2[100,150] 8mb         |L1.2|                                                                    "
    - "L1.3[150,200] 7mb                |L1.3|                                                             "
    - "L1.4[200,250] 7mb                       |L1.4|                                                      "
    - "L1.5[250,300] 7mb                             |L1.5|                                                "
    - "L1.6[300,350] 7mb                                    |L1.6|                                         "
    - "L1.7[350,400] 7mb                                           |L1.7|                                  "
    - "L1.8[400,450] 7mb                                                 |L1.8|                            "
    - "L1.9[450,500] 7mb                                                        |L1.9|                     "
    - "L1.10[500,550] 7mb                                                              |L1.10|             "
    - "**** Simulation run 0, type=compact. 3 Input Files, 15mb total:"
    - "L0, all files 5mb                                                                                   "
    - "L0.13[600,650]      |------------------------------------L0.13-------------------------------------|"
    - "L0.12[600,650]      |------------------------------------L0.12-------------------------------------|"
    - "L0.11[600,650]      |------------------------------------L0.11-------------------------------------|"
    - "**** Simulation run 1, type=split(split_times=[530]). 11 Input Files, 88mb total:"
    - "L1                                                                                                  "
    - "L1.10[500,550] 7mb                                                              |L1.10|             "
    - "L1.9[450,500] 7mb                                                        |L1.9|                     "
    - "L1.8[400,450] 7mb                                                 |L1.8|                            "
    - "L1.7[350,400] 7mb                                           |L1.7|                                  "
    - "L1.6[300,350] 7mb                                    |L1.6|                                         "
    - "L1.5[250,300] 7mb                             |L1.5|                                                "
    - "L1.4[200,250] 7mb                       |L1.4|                                                      "
    - "L1.3[150,200] 7mb                |L1.3|                                                             "
    - "L1.2[100,150] 8mb         |L1.2|                                                                    "
    - "L1.1[50,100] 9mb    |L1.1|                                                                          "
    - "L1.14[600,650] 15mb                                                                          |L1.14|"
    - "**** Final Output Files "
    - "L2                                                                                                  "
    - "L2.15[50,530] 70.4mb|----------------------------L2.15-----------------------------|                "
    - "L2.16[530,650] 17.6mb                                                                |----L2.16-----|"
    "###
    );
}

#[tokio::test]
// Test that compacts L1 files in second round if their total size > max_desired_file_size
async fn large_l1_with_non_overlapping_l0() {
    test_helpers::maybe_start_logging();

    let setup = layout_setup_builder().await.build().await;

    // L1 files with total size > 100MB will get compacted after in round 2
    // after the L0 files are compacted in round 1
    // L1: 90MB, 80MB
    // L0: ..

    for (i, sz) in [90, 80].iter().enumerate() {
        let i = i as i64;
        setup
            .partition
            .create_parquet_file(
                parquet_builder()
                    .with_min_time(50 + i * 50)
                    .with_max_time(100 + i * 50)
                    .with_compaction_level(CompactionLevel::FileNonOverlapped)
                    .with_file_size_bytes(sz * ONE_MB),
            )
            .await;
    }
    // note these overlap with each other, but not the L1 files
    for _ in 0..3 {
        setup
            .partition
            .create_parquet_file(
                parquet_builder()
                    .with_min_time(600)
                    .with_max_time(650)
                    .with_file_size_bytes(5 * ONE_MB),
            )
            .await;
    }

    insta::assert_yaml_snapshot!(
        run_layout_scenario(&setup).await,
        @r###"
    ---
    - "**** Input Files "
    - "L0                                                                                                  "
    - "L0.3[600,650] 5mb                                                                            |L0.3| "
    - "L0.4[600,650] 5mb                                                                            |L0.4| "
    - "L0.5[600,650] 5mb                                                                            |L0.5| "
    - "L1                                                                                                  "
    - "L1.1[50,100] 90mb   |L1.1|                                                                          "
    - "L1.2[100,150] 80mb        |L1.2|                                                                    "
    - "**** Simulation run 0, type=compact. 3 Input Files, 15mb total:"
    - "L0, all files 5mb                                                                                   "
    - "L0.5[600,650]       |-------------------------------------L0.5-------------------------------------|"
    - "L0.4[600,650]       |-------------------------------------L0.4-------------------------------------|"
    - "L0.3[600,650]       |-------------------------------------L0.3-------------------------------------|"
    - "**** Simulation run 1, type=split(split_times=[375]). 3 Input Files, 185mb total:"
    - "L1                                                                                                  "
    - "L1.2[100,150] 80mb        |L1.2|                                                                    "
    - "L1.1[50,100] 90mb   |L1.1|                                                                          "
    - "L1.6[600,650] 15mb                                                                           |L1.6| "
    - "**** Final Output Files "
    - "L2                                                                                                  "
    - "L2.7[50,375] 100.21mb|------------------L2.7-------------------|                                     "
    - "L2.8[375,650] 84.79mb                                           |---------------L2.8---------------| "
    "###
    );
}

#[tokio::test]
async fn many_tiny_l0_files() {
    test_helpers::maybe_start_logging();

    // Observed size on production
    let seven_kb = 7 * 1024;

    let setup = layout_setup_builder().await.build().await;

    // models what happens if the compactor can't keep up for some
    // reason and gets to a partition where the ingster persisted
    // every 5 minutes for an entire day with telegraf data, for
    // example
    let num_tiny_files = (24 * 60) / 5;

    for i in 0..num_tiny_files {
        let i = i as i64;
        setup
            .partition
            .create_parquet_file(
                parquet_builder()
                    .with_min_time(i)
                    .with_max_time(i + 1)
                    .with_compaction_level(CompactionLevel::FileNonOverlapped)
                    .with_file_size_bytes(seven_kb),
            )
            .await;
    }

    insta::assert_yaml_snapshot!(
        run_layout_scenario(&setup).await,
        @r###"
    ---
    - "**** Input Files "
    - "L1, all files 7kb                                                                                   "
    - "L1.1[0,1]           |L1.1|                                                                          "
    - "L1.2[1,2]           |L1.2|                                                                          "
    - "L1.3[2,3]           |L1.3|                                                                          "
    - "L1.4[3,4]           |L1.4|                                                                          "
    - "L1.5[4,5]            |L1.5|                                                                         "
    - "L1.6[5,6]            |L1.6|                                                                         "
    - "L1.7[6,7]            |L1.7|                                                                         "
    - "L1.8[7,8]            |L1.8|                                                                         "
    - "L1.9[8,9]             |L1.9|                                                                        "
    - "L1.10[9,10]           |L1.10|                                                                       "
    - "L1.11[10,11]          |L1.11|                                                                       "
    - "L1.12[11,12]           |L1.12|                                                                      "
    - "L1.13[12,13]           |L1.13|                                                                      "
    - "L1.14[13,14]           |L1.14|                                                                      "
    - "L1.15[14,15]           |L1.15|                                                                      "
    - "L1.16[15,16]            |L1.16|                                                                     "
    - "L1.17[16,17]            |L1.17|                                                                     "
    - "L1.18[17,18]            |L1.18|                                                                     "
    - "L1.19[18,19]             |L1.19|                                                                    "
    - "L1.20[19,20]             |L1.20|                                                                    "
    - "L1.21[20,21]             |L1.21|                                                                    "
    - "L1.22[21,22]             |L1.22|                                                                    "
    - "L1.23[22,23]              |L1.23|                                                                   "
    - "L1.24[23,24]              |L1.24|                                                                   "
    - "L1.25[24,25]              |L1.25|                                                                   "
    - "L1.26[25,26]              |L1.26|                                                                   "
    - "L1.27[26,27]               |L1.27|                                                                  "
    - "L1.28[27,28]               |L1.28|                                                                  "
    - "L1.29[28,29]               |L1.29|                                                                  "
    - "L1.30[29,30]                |L1.30|                                                                 "
    - "L1.31[30,31]                |L1.31|                                                                 "
    - "L1.32[31,32]                |L1.32|                                                                 "
    - "L1.33[32,33]                |L1.33|                                                                 "
    - "L1.34[33,34]                 |L1.34|                                                                "
    - "L1.35[34,35]                 |L1.35|                                                                "
    - "L1.36[35,36]                 |L1.36|                                                                "
    - "L1.37[36,37]                  |L1.37|                                                               "
    - "L1.38[37,38]                  |L1.38|                                                               "
    - "L1.39[38,39]                  |L1.39|                                                               "
    - "L1.40[39,40]                  |L1.40|                                                               "
    - "L1.41[40,41]                   |L1.41|                                                              "
    - "L1.42[41,42]                   |L1.42|                                                              "
    - "L1.43[42,43]                   |L1.43|                                                              "
    - "L1.44[43,44]                   |L1.44|                                                              "
    - "L1.45[44,45]                    |L1.45|                                                             "
    - "L1.46[45,46]                    |L1.46|                                                             "
    - "L1.47[46,47]                    |L1.47|                                                             "
    - "L1.48[47,48]                     |L1.48|                                                            "
    - "L1.49[48,49]                     |L1.49|                                                            "
    - "L1.50[49,50]                     |L1.50|                                                            "
    - "L1.51[50,51]                     |L1.51|                                                            "
    - "L1.52[51,52]                      |L1.52|                                                           "
    - "L1.53[52,53]                      |L1.53|                                                           "
    - "L1.54[53,54]                      |L1.54|                                                           "
    - "L1.55[54,55]                       |L1.55|                                                          "
    - "L1.56[55,56]                       |L1.56|                                                          "
    - "L1.57[56,57]                       |L1.57|                                                          "
    - "L1.58[57,58]                       |L1.58|                                                          "
    - "L1.59[58,59]                        |L1.59|                                                         "
    - "L1.60[59,60]                        |L1.60|                                                         "
    - "L1.61[60,61]                        |L1.61|                                                         "
    - "L1.62[61,62]                        |L1.62|                                                         "
    - "L1.63[62,63]                         |L1.63|                                                        "
    - "L1.64[63,64]                         |L1.64|                                                        "
    - "L1.65[64,65]                         |L1.65|                                                        "
    - "L1.66[65,66]                          |L1.66|                                                       "
    - "L1.67[66,67]                          |L1.67|                                                       "
    - "L1.68[67,68]                          |L1.68|                                                       "
    - "L1.69[68,69]                          |L1.69|                                                       "
    - "L1.70[69,70]                           |L1.70|                                                      "
    - "L1.71[70,71]                           |L1.71|                                                      "
    - "L1.72[71,72]                           |L1.72|                                                      "
    - "L1.73[72,73]                            |L1.73|                                                     "
    - "L1.74[73,74]                            |L1.74|                                                     "
    - "L1.75[74,75]                            |L1.75|                                                     "
    - "L1.76[75,76]                            |L1.76|                                                     "
    - "L1.77[76,77]                             |L1.77|                                                    "
    - "L1.78[77,78]                             |L1.78|                                                    "
    - "L1.79[78,79]                             |L1.79|                                                    "
    - "L1.80[79,80]                             |L1.80|                                                    "
    - "L1.81[80,81]                              |L1.81|                                                   "
    - "L1.82[81,82]                              |L1.82|                                                   "
    - "L1.83[82,83]                              |L1.83|                                                   "
    - "L1.84[83,84]                               |L1.84|                                                  "
    - "L1.85[84,85]                               |L1.85|                                                  "
    - "L1.86[85,86]                               |L1.86|                                                  "
    - "L1.87[86,87]                               |L1.87|                                                  "
    - "L1.88[87,88]                                |L1.88|                                                 "
    - "L1.89[88,89]                                |L1.89|                                                 "
    - "L1.90[89,90]                                |L1.90|                                                 "
    - "L1.91[90,91]                                 |L1.91|                                                "
    - "L1.92[91,92]                                 |L1.92|                                                "
    - "L1.93[92,93]                                 |L1.93|                                                "
    - "L1.94[93,94]                                 |L1.94|                                                "
    - "L1.95[94,95]                                  |L1.95|                                               "
    - "L1.96[95,96]                                  |L1.96|                                               "
    - "L1.97[96,97]                                  |L1.97|                                               "
    - "L1.98[97,98]                                  |L1.98|                                               "
    - "L1.99[98,99]                                   |L1.99|                                              "
    - "L1.100[99,100]                                 |L1.100|                                             "
    - "L1.101[100,101]                                |L1.101|                                             "
    - "L1.102[101,102]                                 |L1.102|                                            "
    - "L1.103[102,103]                                 |L1.103|                                            "
    - "L1.104[103,104]                                 |L1.104|                                            "
    - "L1.105[104,105]                                 |L1.105|                                            "
    - "L1.106[105,106]                                  |L1.106|                                           "
    - "L1.107[106,107]                                  |L1.107|                                           "
    - "L1.108[107,108]                                  |L1.108|                                           "
    - "L1.109[108,109]                                   |L1.109|                                          "
    - "L1.110[109,110]                                   |L1.110|                                          "
    - "L1.111[110,111]                                   |L1.111|                                          "
    - "L1.112[111,112]                                   |L1.112|                                          "
    - "L1.113[112,113]                                    |L1.113|                                         "
    - "L1.114[113,114]                                    |L1.114|                                         "
    - "L1.115[114,115]                                    |L1.115|                                         "
    - "L1.116[115,116]                                    |L1.116|                                         "
    - "L1.117[116,117]                                     |L1.117|                                        "
    - "L1.118[117,118]                                     |L1.118|                                        "
    - "L1.119[118,119]                                     |L1.119|                                        "
    - "L1.120[119,120]                                      |L1.120|                                       "
    - "L1.121[120,121]                                      |L1.121|                                       "
    - "L1.122[121,122]                                      |L1.122|                                       "
    - "L1.123[122,123]                                      |L1.123|                                       "
    - "L1.124[123,124]                                       |L1.124|                                      "
    - "L1.125[124,125]                                       |L1.125|                                      "
    - "L1.126[125,126]                                       |L1.126|                                      "
    - "L1.127[126,127]                                        |L1.127|                                     "
    - "L1.128[127,128]                                        |L1.128|                                     "
    - "L1.129[128,129]                                        |L1.129|                                     "
    - "L1.130[129,130]                                        |L1.130|                                     "
    - "L1.131[130,131]                                         |L1.131|                                    "
    - "L1.132[131,132]                                         |L1.132|                                    "
    - "L1.133[132,133]                                         |L1.133|                                    "
    - "L1.134[133,134]                                         |L1.134|                                    "
    - "L1.135[134,135]                                          |L1.135|                                   "
    - "L1.136[135,136]                                          |L1.136|                                   "
    - "L1.137[136,137]                                          |L1.137|                                   "
    - "L1.138[137,138]                                           |L1.138|                                  "
    - "L1.139[138,139]                                           |L1.139|                                  "
    - "L1.140[139,140]                                           |L1.140|                                  "
    - "L1.141[140,141]                                           |L1.141|                                  "
    - "L1.142[141,142]                                            |L1.142|                                 "
    - "L1.143[142,143]                                            |L1.143|                                 "
    - "L1.144[143,144]                                            |L1.144|                                 "
    - "L1.145[144,145]                                             |L1.145|                                "
    - "L1.146[145,146]                                             |L1.146|                                "
    - "L1.147[146,147]                                             |L1.147|                                "
    - "L1.148[147,148]                                             |L1.148|                                "
    - "L1.149[148,149]                                              |L1.149|                               "
    - "L1.150[149,150]                                              |L1.150|                               "
    - "L1.151[150,151]                                              |L1.151|                               "
    - "L1.152[151,152]                                              |L1.152|                               "
    - "L1.153[152,153]                                               |L1.153|                              "
    - "L1.154[153,154]                                               |L1.154|                              "
    - "L1.155[154,155]                                               |L1.155|                              "
    - "L1.156[155,156]                                                |L1.156|                             "
    - "L1.157[156,157]                                                |L1.157|                             "
    - "L1.158[157,158]                                                |L1.158|                             "
    - "L1.159[158,159]                                                |L1.159|                             "
    - "L1.160[159,160]                                                 |L1.160|                            "
    - "L1.161[160,161]                                                 |L1.161|                            "
    - "L1.162[161,162]                                                 |L1.162|                            "
    - "L1.163[162,163]                                                  |L1.163|                           "
    - "L1.164[163,164]                                                  |L1.164|                           "
    - "L1.165[164,165]                                                  |L1.165|                           "
    - "L1.166[165,166]                                                  |L1.166|                           "
    - "L1.167[166,167]                                                   |L1.167|                          "
    - "L1.168[167,168]                                                   |L1.168|                          "
    - "L1.169[168,169]                                                   |L1.169|                          "
    - "L1.170[169,170]                                                   |L1.170|                          "
    - "L1.171[170,171]                                                    |L1.171|                         "
    - "L1.172[171,172]                                                    |L1.172|                         "
    - "L1.173[172,173]                                                    |L1.173|                         "
    - "L1.174[173,174]                                                     |L1.174|                        "
    - "L1.175[174,175]                                                     |L1.175|                        "
    - "L1.176[175,176]                                                     |L1.176|                        "
    - "L1.177[176,177]                                                     |L1.177|                        "
    - "L1.178[177,178]                                                      |L1.178|                       "
    - "L1.179[178,179]                                                      |L1.179|                       "
    - "L1.180[179,180]                                                      |L1.180|                       "
    - "L1.181[180,181]                                                       |L1.181|                      "
    - "L1.182[181,182]                                                       |L1.182|                      "
    - "L1.183[182,183]                                                       |L1.183|                      "
    - "L1.184[183,184]                                                       |L1.184|                      "
    - "L1.185[184,185]                                                        |L1.185|                     "
    - "L1.186[185,186]                                                        |L1.186|                     "
    - "L1.187[186,187]                                                        |L1.187|                     "
    - "L1.188[187,188]                                                        |L1.188|                     "
    - "L1.189[188,189]                                                         |L1.189|                    "
    - "L1.190[189,190]                                                         |L1.190|                    "
    - "L1.191[190,191]                                                         |L1.191|                    "
    - "L1.192[191,192]                                                          |L1.192|                   "
    - "L1.193[192,193]                                                          |L1.193|                   "
    - "L1.194[193,194]                                                          |L1.194|                   "
    - "L1.195[194,195]                                                          |L1.195|                   "
    - "L1.196[195,196]                                                           |L1.196|                  "
    - "L1.197[196,197]                                                           |L1.197|                  "
    - "L1.198[197,198]                                                           |L1.198|                  "
    - "L1.199[198,199]                                                            |L1.199|                 "
    - "L1.200[199,200]                                                            |L1.200|                 "
    - "L1.201[200,201]                                                            |L1.201|                 "
    - "L1.202[201,202]                                                            |L1.202|                 "
    - "L1.203[202,203]                                                             |L1.203|                "
    - "L1.204[203,204]                                                             |L1.204|                "
    - "L1.205[204,205]                                                             |L1.205|                "
    - "L1.206[205,206]                                                             |L1.206|                "
    - "L1.207[206,207]                                                              |L1.207|               "
    - "L1.208[207,208]                                                              |L1.208|               "
    - "L1.209[208,209]                                                              |L1.209|               "
    - "L1.210[209,210]                                                               |L1.210|              "
    - "L1.211[210,211]                                                               |L1.211|              "
    - "L1.212[211,212]                                                               |L1.212|              "
    - "L1.213[212,213]                                                               |L1.213|              "
    - "L1.214[213,214]                                                                |L1.214|             "
    - "L1.215[214,215]                                                                |L1.215|             "
    - "L1.216[215,216]                                                                |L1.216|             "
    - "L1.217[216,217]                                                                 |L1.217|            "
    - "L1.218[217,218]                                                                 |L1.218|            "
    - "L1.219[218,219]                                                                 |L1.219|            "
    - "L1.220[219,220]                                                                 |L1.220|            "
    - "L1.221[220,221]                                                                  |L1.221|           "
    - "L1.222[221,222]                                                                  |L1.222|           "
    - "L1.223[222,223]                                                                  |L1.223|           "
    - "L1.224[223,224]                                                                  |L1.224|           "
    - "L1.225[224,225]                                                                   |L1.225|          "
    - "L1.226[225,226]                                                                   |L1.226|          "
    - "L1.227[226,227]                                                                   |L1.227|          "
    - "L1.228[227,228]                                                                    |L1.228|         "
    - "L1.229[228,229]                                                                    |L1.229|         "
    - "L1.230[229,230]                                                                    |L1.230|         "
    - "L1.231[230,231]                                                                    |L1.231|         "
    - "L1.232[231,232]                                                                     |L1.232|        "
    - "L1.233[232,233]                                                                     |L1.233|        "
    - "L1.234[233,234]                                                                     |L1.234|        "
    - "L1.235[234,235]                                                                      |L1.235|       "
    - "L1.236[235,236]                                                                      |L1.236|       "
    - "L1.237[236,237]                                                                      |L1.237|       "
    - "L1.238[237,238]                                                                      |L1.238|       "
    - "L1.239[238,239]                                                                       |L1.239|      "
    - "L1.240[239,240]                                                                       |L1.240|      "
    - "L1.241[240,241]                                                                       |L1.241|      "
    - "L1.242[241,242]                                                                       |L1.242|      "
    - "L1.243[242,243]                                                                        |L1.243|     "
    - "L1.244[243,244]                                                                        |L1.244|     "
    - "L1.245[244,245]                                                                        |L1.245|     "
    - "L1.246[245,246]                                                                         |L1.246|    "
    - "L1.247[246,247]                                                                         |L1.247|    "
    - "L1.248[247,248]                                                                         |L1.248|    "
    - "L1.249[248,249]                                                                         |L1.249|    "
    - "L1.250[249,250]                                                                          |L1.250|   "
    - "L1.251[250,251]                                                                          |L1.251|   "
    - "L1.252[251,252]                                                                          |L1.252|   "
    - "L1.253[252,253]                                                                           |L1.253|  "
    - "L1.254[253,254]                                                                           |L1.254|  "
    - "L1.255[254,255]                                                                           |L1.255|  "
    - "L1.256[255,256]                                                                           |L1.256|  "
    - "L1.257[256,257]                                                                            |L1.257| "
    - "L1.258[257,258]                                                                            |L1.258| "
    - "L1.259[258,259]                                                                            |L1.259| "
    - "L1.260[259,260]                                                                            |L1.260| "
    - "L1.261[260,261]                                                                             |L1.261|"
    - "L1.262[261,262]                                                                             |L1.262|"
    - "L1.263[262,263]                                                                             |L1.263|"
    - "L1.264[263,264]                                                                              |L1.264|"
    - "L1.265[264,265]                                                                              |L1.265|"
    - "L1.266[265,266]                                                                              |L1.266|"
    - "L1.267[266,267]                                                                              |L1.267|"
    - "L1.268[267,268]                                                                               |L1.268|"
    - "L1.269[268,269]                                                                               |L1.269|"
    - "L1.270[269,270]                                                                               |L1.270|"
    - "L1.271[270,271]                                                                                |L1.271|"
    - "L1.272[271,272]                                                                                |L1.272|"
    - "L1.273[272,273]                                                                                |L1.273|"
    - "L1.274[273,274]                                                                                |L1.274|"
    - "L1.275[274,275]                                                                                 |L1.275|"
    - "L1.276[275,276]                                                                                 |L1.276|"
    - "L1.277[276,277]                                                                                 |L1.277|"
    - "L1.278[277,278]                                                                                 |L1.278|"
    - "L1.279[278,279]                                                                                  |L1.279|"
    - "L1.280[279,280]                                                                                  |L1.280|"
    - "L1.281[280,281]                                                                                  |L1.281|"
    - "L1.282[281,282]                                                                                   |L1.282|"
    - "L1.283[282,283]                                                                                   |L1.283|"
    - "L1.284[283,284]                                                                                   |L1.284|"
    - "L1.285[284,285]                                                                                   |L1.285|"
    - "L1.286[285,286]                                                                                    |L1.286|"
    - "L1.287[286,287]                                                                                    |L1.287|"
    - "L1.288[287,288]                                                                                    |L1.288|"
    - "SKIPPED COMPACTION for PartitionId(1): partition 1 has 288 files, limit is 200"
    - "**** Final Output Files "
    - "L1, all files 7kb                                                                                   "
    - "L1.1[0,1]           |L1.1|                                                                          "
    - "L1.2[1,2]           |L1.2|                                                                          "
    - "L1.3[2,3]           |L1.3|                                                                          "
    - "L1.4[3,4]           |L1.4|                                                                          "
    - "L1.5[4,5]            |L1.5|                                                                         "
    - "L1.6[5,6]            |L1.6|                                                                         "
    - "L1.7[6,7]            |L1.7|                                                                         "
    - "L1.8[7,8]            |L1.8|                                                                         "
    - "L1.9[8,9]             |L1.9|                                                                        "
    - "L1.10[9,10]           |L1.10|                                                                       "
    - "L1.11[10,11]          |L1.11|                                                                       "
    - "L1.12[11,12]           |L1.12|                                                                      "
    - "L1.13[12,13]           |L1.13|                                                                      "
    - "L1.14[13,14]           |L1.14|                                                                      "
    - "L1.15[14,15]           |L1.15|                                                                      "
    - "L1.16[15,16]            |L1.16|                                                                     "
    - "L1.17[16,17]            |L1.17|                                                                     "
    - "L1.18[17,18]            |L1.18|                                                                     "
    - "L1.19[18,19]             |L1.19|                                                                    "
    - "L1.20[19,20]             |L1.20|                                                                    "
    - "L1.21[20,21]             |L1.21|                                                                    "
    - "L1.22[21,22]             |L1.22|                                                                    "
    - "L1.23[22,23]              |L1.23|                                                                   "
    - "L1.24[23,24]              |L1.24|                                                                   "
    - "L1.25[24,25]              |L1.25|                                                                   "
    - "L1.26[25,26]              |L1.26|                                                                   "
    - "L1.27[26,27]               |L1.27|                                                                  "
    - "L1.28[27,28]               |L1.28|                                                                  "
    - "L1.29[28,29]               |L1.29|                                                                  "
    - "L1.30[29,30]                |L1.30|                                                                 "
    - "L1.31[30,31]                |L1.31|                                                                 "
    - "L1.32[31,32]                |L1.32|                                                                 "
    - "L1.33[32,33]                |L1.33|                                                                 "
    - "L1.34[33,34]                 |L1.34|                                                                "
    - "L1.35[34,35]                 |L1.35|                                                                "
    - "L1.36[35,36]                 |L1.36|                                                                "
    - "L1.37[36,37]                  |L1.37|                                                               "
    - "L1.38[37,38]                  |L1.38|                                                               "
    - "L1.39[38,39]                  |L1.39|                                                               "
    - "L1.40[39,40]                  |L1.40|                                                               "
    - "L1.41[40,41]                   |L1.41|                                                              "
    - "L1.42[41,42]                   |L1.42|                                                              "
    - "L1.43[42,43]                   |L1.43|                                                              "
    - "L1.44[43,44]                   |L1.44|                                                              "
    - "L1.45[44,45]                    |L1.45|                                                             "
    - "L1.46[45,46]                    |L1.46|                                                             "
    - "L1.47[46,47]                    |L1.47|                                                             "
    - "L1.48[47,48]                     |L1.48|                                                            "
    - "L1.49[48,49]                     |L1.49|                                                            "
    - "L1.50[49,50]                     |L1.50|                                                            "
    - "L1.51[50,51]                     |L1.51|                                                            "
    - "L1.52[51,52]                      |L1.52|                                                           "
    - "L1.53[52,53]                      |L1.53|                                                           "
    - "L1.54[53,54]                      |L1.54|                                                           "
    - "L1.55[54,55]                       |L1.55|                                                          "
    - "L1.56[55,56]                       |L1.56|                                                          "
    - "L1.57[56,57]                       |L1.57|                                                          "
    - "L1.58[57,58]                       |L1.58|                                                          "
    - "L1.59[58,59]                        |L1.59|                                                         "
    - "L1.60[59,60]                        |L1.60|                                                         "
    - "L1.61[60,61]                        |L1.61|                                                         "
    - "L1.62[61,62]                        |L1.62|                                                         "
    - "L1.63[62,63]                         |L1.63|                                                        "
    - "L1.64[63,64]                         |L1.64|                                                        "
    - "L1.65[64,65]                         |L1.65|                                                        "
    - "L1.66[65,66]                          |L1.66|                                                       "
    - "L1.67[66,67]                          |L1.67|                                                       "
    - "L1.68[67,68]                          |L1.68|                                                       "
    - "L1.69[68,69]                          |L1.69|                                                       "
    - "L1.70[69,70]                           |L1.70|                                                      "
    - "L1.71[70,71]                           |L1.71|                                                      "
    - "L1.72[71,72]                           |L1.72|                                                      "
    - "L1.73[72,73]                            |L1.73|                                                     "
    - "L1.74[73,74]                            |L1.74|                                                     "
    - "L1.75[74,75]                            |L1.75|                                                     "
    - "L1.76[75,76]                            |L1.76|                                                     "
    - "L1.77[76,77]                             |L1.77|                                                    "
    - "L1.78[77,78]                             |L1.78|                                                    "
    - "L1.79[78,79]                             |L1.79|                                                    "
    - "L1.80[79,80]                             |L1.80|                                                    "
    - "L1.81[80,81]                              |L1.81|                                                   "
    - "L1.82[81,82]                              |L1.82|                                                   "
    - "L1.83[82,83]                              |L1.83|                                                   "
    - "L1.84[83,84]                               |L1.84|                                                  "
    - "L1.85[84,85]                               |L1.85|                                                  "
    - "L1.86[85,86]                               |L1.86|                                                  "
    - "L1.87[86,87]                               |L1.87|                                                  "
    - "L1.88[87,88]                                |L1.88|                                                 "
    - "L1.89[88,89]                                |L1.89|                                                 "
    - "L1.90[89,90]                                |L1.90|                                                 "
    - "L1.91[90,91]                                 |L1.91|                                                "
    - "L1.92[91,92]                                 |L1.92|                                                "
    - "L1.93[92,93]                                 |L1.93|                                                "
    - "L1.94[93,94]                                 |L1.94|                                                "
    - "L1.95[94,95]                                  |L1.95|                                               "
    - "L1.96[95,96]                                  |L1.96|                                               "
    - "L1.97[96,97]                                  |L1.97|                                               "
    - "L1.98[97,98]                                  |L1.98|                                               "
    - "L1.99[98,99]                                   |L1.99|                                              "
    - "L1.100[99,100]                                 |L1.100|                                             "
    - "L1.101[100,101]                                |L1.101|                                             "
    - "L1.102[101,102]                                 |L1.102|                                            "
    - "L1.103[102,103]                                 |L1.103|                                            "
    - "L1.104[103,104]                                 |L1.104|                                            "
    - "L1.105[104,105]                                 |L1.105|                                            "
    - "L1.106[105,106]                                  |L1.106|                                           "
    - "L1.107[106,107]                                  |L1.107|                                           "
    - "L1.108[107,108]                                  |L1.108|                                           "
    - "L1.109[108,109]                                   |L1.109|                                          "
    - "L1.110[109,110]                                   |L1.110|                                          "
    - "L1.111[110,111]                                   |L1.111|                                          "
    - "L1.112[111,112]                                   |L1.112|                                          "
    - "L1.113[112,113]                                    |L1.113|                                         "
    - "L1.114[113,114]                                    |L1.114|                                         "
    - "L1.115[114,115]                                    |L1.115|                                         "
    - "L1.116[115,116]                                    |L1.116|                                         "
    - "L1.117[116,117]                                     |L1.117|                                        "
    - "L1.118[117,118]                                     |L1.118|                                        "
    - "L1.119[118,119]                                     |L1.119|                                        "
    - "L1.120[119,120]                                      |L1.120|                                       "
    - "L1.121[120,121]                                      |L1.121|                                       "
    - "L1.122[121,122]                                      |L1.122|                                       "
    - "L1.123[122,123]                                      |L1.123|                                       "
    - "L1.124[123,124]                                       |L1.124|                                      "
    - "L1.125[124,125]                                       |L1.125|                                      "
    - "L1.126[125,126]                                       |L1.126|                                      "
    - "L1.127[126,127]                                        |L1.127|                                     "
    - "L1.128[127,128]                                        |L1.128|                                     "
    - "L1.129[128,129]                                        |L1.129|                                     "
    - "L1.130[129,130]                                        |L1.130|                                     "
    - "L1.131[130,131]                                         |L1.131|                                    "
    - "L1.132[131,132]                                         |L1.132|                                    "
    - "L1.133[132,133]                                         |L1.133|                                    "
    - "L1.134[133,134]                                         |L1.134|                                    "
    - "L1.135[134,135]                                          |L1.135|                                   "
    - "L1.136[135,136]                                          |L1.136|                                   "
    - "L1.137[136,137]                                          |L1.137|                                   "
    - "L1.138[137,138]                                           |L1.138|                                  "
    - "L1.139[138,139]                                           |L1.139|                                  "
    - "L1.140[139,140]                                           |L1.140|                                  "
    - "L1.141[140,141]                                           |L1.141|                                  "
    - "L1.142[141,142]                                            |L1.142|                                 "
    - "L1.143[142,143]                                            |L1.143|                                 "
    - "L1.144[143,144]                                            |L1.144|                                 "
    - "L1.145[144,145]                                             |L1.145|                                "
    - "L1.146[145,146]                                             |L1.146|                                "
    - "L1.147[146,147]                                             |L1.147|                                "
    - "L1.148[147,148]                                             |L1.148|                                "
    - "L1.149[148,149]                                              |L1.149|                               "
    - "L1.150[149,150]                                              |L1.150|                               "
    - "L1.151[150,151]                                              |L1.151|                               "
    - "L1.152[151,152]                                              |L1.152|                               "
    - "L1.153[152,153]                                               |L1.153|                              "
    - "L1.154[153,154]                                               |L1.154|                              "
    - "L1.155[154,155]                                               |L1.155|                              "
    - "L1.156[155,156]                                                |L1.156|                             "
    - "L1.157[156,157]                                                |L1.157|                             "
    - "L1.158[157,158]                                                |L1.158|                             "
    - "L1.159[158,159]                                                |L1.159|                             "
    - "L1.160[159,160]                                                 |L1.160|                            "
    - "L1.161[160,161]                                                 |L1.161|                            "
    - "L1.162[161,162]                                                 |L1.162|                            "
    - "L1.163[162,163]                                                  |L1.163|                           "
    - "L1.164[163,164]                                                  |L1.164|                           "
    - "L1.165[164,165]                                                  |L1.165|                           "
    - "L1.166[165,166]                                                  |L1.166|                           "
    - "L1.167[166,167]                                                   |L1.167|                          "
    - "L1.168[167,168]                                                   |L1.168|                          "
    - "L1.169[168,169]                                                   |L1.169|                          "
    - "L1.170[169,170]                                                   |L1.170|                          "
    - "L1.171[170,171]                                                    |L1.171|                         "
    - "L1.172[171,172]                                                    |L1.172|                         "
    - "L1.173[172,173]                                                    |L1.173|                         "
    - "L1.174[173,174]                                                     |L1.174|                        "
    - "L1.175[174,175]                                                     |L1.175|                        "
    - "L1.176[175,176]                                                     |L1.176|                        "
    - "L1.177[176,177]                                                     |L1.177|                        "
    - "L1.178[177,178]                                                      |L1.178|                       "
    - "L1.179[178,179]                                                      |L1.179|                       "
    - "L1.180[179,180]                                                      |L1.180|                       "
    - "L1.181[180,181]                                                       |L1.181|                      "
    - "L1.182[181,182]                                                       |L1.182|                      "
    - "L1.183[182,183]                                                       |L1.183|                      "
    - "L1.184[183,184]                                                       |L1.184|                      "
    - "L1.185[184,185]                                                        |L1.185|                     "
    - "L1.186[185,186]                                                        |L1.186|                     "
    - "L1.187[186,187]                                                        |L1.187|                     "
    - "L1.188[187,188]                                                        |L1.188|                     "
    - "L1.189[188,189]                                                         |L1.189|                    "
    - "L1.190[189,190]                                                         |L1.190|                    "
    - "L1.191[190,191]                                                         |L1.191|                    "
    - "L1.192[191,192]                                                          |L1.192|                   "
    - "L1.193[192,193]                                                          |L1.193|                   "
    - "L1.194[193,194]                                                          |L1.194|                   "
    - "L1.195[194,195]                                                          |L1.195|                   "
    - "L1.196[195,196]                                                           |L1.196|                  "
    - "L1.197[196,197]                                                           |L1.197|                  "
    - "L1.198[197,198]                                                           |L1.198|                  "
    - "L1.199[198,199]                                                            |L1.199|                 "
    - "L1.200[199,200]                                                            |L1.200|                 "
    - "L1.201[200,201]                                                            |L1.201|                 "
    - "L1.202[201,202]                                                            |L1.202|                 "
    - "L1.203[202,203]                                                             |L1.203|                "
    - "L1.204[203,204]                                                             |L1.204|                "
    - "L1.205[204,205]                                                             |L1.205|                "
    - "L1.206[205,206]                                                             |L1.206|                "
    - "L1.207[206,207]                                                              |L1.207|               "
    - "L1.208[207,208]                                                              |L1.208|               "
    - "L1.209[208,209]                                                              |L1.209|               "
    - "L1.210[209,210]                                                               |L1.210|              "
    - "L1.211[210,211]                                                               |L1.211|              "
    - "L1.212[211,212]                                                               |L1.212|              "
    - "L1.213[212,213]                                                               |L1.213|              "
    - "L1.214[213,214]                                                                |L1.214|             "
    - "L1.215[214,215]                                                                |L1.215|             "
    - "L1.216[215,216]                                                                |L1.216|             "
    - "L1.217[216,217]                                                                 |L1.217|            "
    - "L1.218[217,218]                                                                 |L1.218|            "
    - "L1.219[218,219]                                                                 |L1.219|            "
    - "L1.220[219,220]                                                                 |L1.220|            "
    - "L1.221[220,221]                                                                  |L1.221|           "
    - "L1.222[221,222]                                                                  |L1.222|           "
    - "L1.223[222,223]                                                                  |L1.223|           "
    - "L1.224[223,224]                                                                  |L1.224|           "
    - "L1.225[224,225]                                                                   |L1.225|          "
    - "L1.226[225,226]                                                                   |L1.226|          "
    - "L1.227[226,227]                                                                   |L1.227|          "
    - "L1.228[227,228]                                                                    |L1.228|         "
    - "L1.229[228,229]                                                                    |L1.229|         "
    - "L1.230[229,230]                                                                    |L1.230|         "
    - "L1.231[230,231]                                                                    |L1.231|         "
    - "L1.232[231,232]                                                                     |L1.232|        "
    - "L1.233[232,233]                                                                     |L1.233|        "
    - "L1.234[233,234]                                                                     |L1.234|        "
    - "L1.235[234,235]                                                                      |L1.235|       "
    - "L1.236[235,236]                                                                      |L1.236|       "
    - "L1.237[236,237]                                                                      |L1.237|       "
    - "L1.238[237,238]                                                                      |L1.238|       "
    - "L1.239[238,239]                                                                       |L1.239|      "
    - "L1.240[239,240]                                                                       |L1.240|      "
    - "L1.241[240,241]                                                                       |L1.241|      "
    - "L1.242[241,242]                                                                       |L1.242|      "
    - "L1.243[242,243]                                                                        |L1.243|     "
    - "L1.244[243,244]                                                                        |L1.244|     "
    - "L1.245[244,245]                                                                        |L1.245|     "
    - "L1.246[245,246]                                                                         |L1.246|    "
    - "L1.247[246,247]                                                                         |L1.247|    "
    - "L1.248[247,248]                                                                         |L1.248|    "
    - "L1.249[248,249]                                                                         |L1.249|    "
    - "L1.250[249,250]                                                                          |L1.250|   "
    - "L1.251[250,251]                                                                          |L1.251|   "
    - "L1.252[251,252]                                                                          |L1.252|   "
    - "L1.253[252,253]                                                                           |L1.253|  "
    - "L1.254[253,254]                                                                           |L1.254|  "
    - "L1.255[254,255]                                                                           |L1.255|  "
    - "L1.256[255,256]                                                                           |L1.256|  "
    - "L1.257[256,257]                                                                            |L1.257| "
    - "L1.258[257,258]                                                                            |L1.258| "
    - "L1.259[258,259]                                                                            |L1.259| "
    - "L1.260[259,260]                                                                            |L1.260| "
    - "L1.261[260,261]                                                                             |L1.261|"
    - "L1.262[261,262]                                                                             |L1.262|"
    - "L1.263[262,263]                                                                             |L1.263|"
    - "L1.264[263,264]                                                                              |L1.264|"
    - "L1.265[264,265]                                                                              |L1.265|"
    - "L1.266[265,266]                                                                              |L1.266|"
    - "L1.267[266,267]                                                                              |L1.267|"
    - "L1.268[267,268]                                                                               |L1.268|"
    - "L1.269[268,269]                                                                               |L1.269|"
    - "L1.270[269,270]                                                                               |L1.270|"
    - "L1.271[270,271]                                                                                |L1.271|"
    - "L1.272[271,272]                                                                                |L1.272|"
    - "L1.273[272,273]                                                                                |L1.273|"
    - "L1.274[273,274]                                                                                |L1.274|"
    - "L1.275[274,275]                                                                                 |L1.275|"
    - "L1.276[275,276]                                                                                 |L1.276|"
    - "L1.277[276,277]                                                                                 |L1.277|"
    - "L1.278[277,278]                                                                                 |L1.278|"
    - "L1.279[278,279]                                                                                  |L1.279|"
    - "L1.280[279,280]                                                                                  |L1.280|"
    - "L1.281[280,281]                                                                                  |L1.281|"
    - "L1.282[281,282]                                                                                   |L1.282|"
    - "L1.283[282,283]                                                                                   |L1.283|"
    - "L1.284[283,284]                                                                                   |L1.284|"
    - "L1.285[284,285]                                                                                   |L1.285|"
    - "L1.286[285,286]                                                                                    |L1.286|"
    - "L1.287[286,287]                                                                                    |L1.287|"
    - "L1.288[287,288]                                                                                    |L1.288|"
    "###
    );
}

/// runs the scenario and returns a string based output for comparison
async fn run_layout_scenario(setup: &TestSetup) -> Vec<String> {
    setup.catalog.time_provider.inc(Duration::from_nanos(200));

    let input_files = setup.list_by_table_not_to_delete().await;
    let mut output = format_files("**** Input Files ", &sort_files(input_files));

    // run the actual compaction
    let compact_result = setup.run_compact().await;

    // record what the compactor actually did
    output.extend(compact_result.simulator_runs);

    // Record any skipped compactions (is after what the compactor actually did)
    output.extend(get_skipped_compactions(setup).await);

    // record the final state of the catalog
    let output_files = setup.list_by_table_not_to_delete().await;
    output.extend(format_files(
        "**** Final Output Files ",
        &sort_files(output_files),
    ));

    output
}

fn sort_files(mut files: Vec<ParquetFile>) -> Vec<ParquetFile> {
    // sort by ascending parquet file id for more consistent display
    files.sort_by(|f1, f2| f1.id.cmp(&f2.id));
    files
}

async fn get_skipped_compactions(setup: &TestSetup) -> Vec<String> {
    let skipped = setup
        .catalog
        .catalog
        .repositories()
        .await
        .partitions()
        .list_skipped_compactions()
        .await
        .unwrap();

    skipped
        .iter()
        .map(|skipped| {
            format!(
                "SKIPPED COMPACTION for {:?}: {}",
                skipped.partition_id, skipped.reason
            )
        })
        .collect()
}
