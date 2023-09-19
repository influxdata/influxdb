//! layout tests for the pathalogical cases of large amounts of data
//! with a single timestamp.
//!
//! The compactor doesn't necessarily have to handle actually
//! splitting such files, but it shouldn't crash/ panic / etc either.
//!
//! See [crate::layout] module for detailed documentation

use data_types::CompactionLevel;
use iox_time::Time;

use crate::layouts::{layout_setup_builder, parquet_builder, run_layout_scenario, ONE_MB};

#[tokio::test]
async fn single_giant_file() {
    test_helpers::maybe_start_logging();
    let setup = layout_setup_builder().await.build().await;

    // This test a single file that is too large for the compactor to
    // split with a single timestamp. The compactor should not panic
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(100)
                .with_max_time(100)
                .with_file_size_bytes(5 * 1000 * ONE_MB)
                .with_compaction_level(CompactionLevel::Initial),
        )
        .await;

    // L0 file is upgraded to L1 and then L2
    insta::assert_yaml_snapshot!(
        run_layout_scenario(&setup).await,
        @r###"
    ---
    - "**** Input Files "
    - "L0, all files 4.88gb                                                                                               "
    - "L0.1[100,100] 1ns        |------------------------------------------L0.1------------------------------------------|"
    - "WARNING: file L0.1[100,100] 1ns 4.88gb exceeds soft limit 100mb by more than 50%"
    - "Committing partition 1:"
    - "  Upgrading 1 files level to CompactionLevel::L1: L0.1"
    - "Committing partition 1:"
    - "  Upgrading 1 files level to CompactionLevel::L2: L1.1"
    - "**** Final Output Files (0b written)"
    - "L2, all files 4.88gb                                                                                               "
    - "L2.1[100,100] 1ns        |------------------------------------------L2.1------------------------------------------|"
    - "WARNING: file L2.1[100,100] 1ns 4.88gb exceeds soft limit 100mb by more than 50%"
    "###
    );
}

#[tokio::test]
async fn two_giant_files() {
    test_helpers::maybe_start_logging();
    let setup = layout_setup_builder().await.build().await;

    // This has two large overlapping files that the compactor can't
    // split as they have a single timestamp. The compactor should not
    // panic
    for i in 0..2 {
        setup
            .partition
            .create_parquet_file(
                parquet_builder()
                    .with_min_time(100)
                    .with_max_time(100)
                    .with_file_size_bytes(5 * 1000 * ONE_MB)
                    .with_compaction_level(CompactionLevel::Initial)
                    .with_max_l0_created_at(Time::from_timestamp_nanos(i + 1)),
            )
            .await;
    }

    insta::assert_yaml_snapshot!(
        run_layout_scenario(&setup).await,
        @r###"
    ---
    - "**** Input Files "
    - "L0, all files 4.88gb                                                                                               "
    - "L0.1[100,100] 1ns        |------------------------------------------L0.1------------------------------------------|"
    - "L0.2[100,100] 2ns        |------------------------------------------L0.2------------------------------------------|"
    - "WARNING: file L0.1[100,100] 1ns 4.88gb exceeds soft limit 100mb by more than 50%"
    - "WARNING: file L0.2[100,100] 2ns 4.88gb exceeds soft limit 100mb by more than 50%"
    - "SKIPPED COMPACTION for PartitionId(1): partition 1 has overlapped files that exceed max compact size limit 314572800. This may happen if a large amount of data has the same timestamp"
    - "**** Final Output Files (0b written)"
    - "L0, all files 4.88gb                                                                                               "
    - "L0.1[100,100] 1ns        |------------------------------------------L0.1------------------------------------------|"
    - "L0.2[100,100] 2ns        |------------------------------------------L0.2------------------------------------------|"
    - "WARNING: file L0.1[100,100] 1ns 4.88gb exceeds soft limit 100mb by more than 50%"
    - "WARNING: file L0.2[100,100] 2ns 4.88gb exceeds soft limit 100mb by more than 50%"
    "###
    );
}

#[tokio::test]
async fn two_giant_files_time_range_1() {
    test_helpers::maybe_start_logging();
    let setup = layout_setup_builder().await.build().await;

    // This has two large overlapping files that the compactor can't
    // split as they have a single timestamp. The compactor should not
    // panic
    for i in 0..2 {
        setup
            .partition
            .create_parquet_file(
                parquet_builder()
                    .with_min_time(100)
                    .with_max_time(101)
                    .with_file_size_bytes(5 * 1000 * ONE_MB)
                    .with_compaction_level(CompactionLevel::Initial)
                    .with_max_l0_created_at(Time::from_timestamp_nanos(i + 1)),
            )
            .await;
    }

    insta::assert_yaml_snapshot!(
        run_layout_scenario(&setup).await,
        @r###"
    ---
    - "**** Input Files "
    - "L0, all files 4.88gb                                                                                               "
    - "L0.1[100,101] 1ns        |------------------------------------------L0.1------------------------------------------|"
    - "L0.2[100,101] 2ns        |------------------------------------------L0.2------------------------------------------|"
    - "WARNING: file L0.1[100,101] 1ns 4.88gb exceeds soft limit 100mb by more than 50%"
    - "WARNING: file L0.2[100,101] 2ns 4.88gb exceeds soft limit 100mb by more than 50%"
    - "**** Simulation run 0, type=split(VerticalSplit)(split_times=[100]). 1 Input Files, 4.88gb total:"
    - "L0, all files 4.88gb                                                                                               "
    - "L0.1[100,101] 1ns        |------------------------------------------L0.1------------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 4.88gb total:"
    - "L0, all files 2.44gb                                                                                               "
    - "L0.?[100,100] 1ns        |L0.?|                                                                                    "
    - "L0.?[101,101] 1ns                                                                                                  |L0.?|"
    - "**** Simulation run 1, type=split(VerticalSplit)(split_times=[100]). 1 Input Files, 4.88gb total:"
    - "L0, all files 4.88gb                                                                                               "
    - "L0.2[100,101] 2ns        |------------------------------------------L0.2------------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 4.88gb total:"
    - "L0, all files 2.44gb                                                                                               "
    - "L0.?[100,100] 2ns        |L0.?|                                                                                    "
    - "L0.?[101,101] 2ns                                                                                                  |L0.?|"
    - "Committing partition 1:"
    - "  Soft Deleting 2 files: L0.1, L0.2"
    - "  Creating 4 files"
    - "SKIPPED COMPACTION for PartitionId(1): partition 1 has overlapped files that exceed max compact size limit 314572800. This may happen if a large amount of data has the same timestamp"
    - "**** Final Output Files (9.77gb written)"
    - "L0, all files 2.44gb                                                                                               "
    - "L0.3[100,100] 1ns        |L0.3|                                                                                    "
    - "L0.4[101,101] 1ns                                                                                                  |L0.4|"
    - "L0.5[100,100] 2ns        |L0.5|                                                                                    "
    - "L0.6[101,101] 2ns                                                                                                  |L0.6|"
    - "WARNING: file L0.3[100,100] 1ns 2.44gb exceeds soft limit 100mb by more than 50%"
    - "WARNING: file L0.4[101,101] 1ns 2.44gb exceeds soft limit 100mb by more than 50%"
    - "WARNING: file L0.5[100,100] 2ns 2.44gb exceeds soft limit 100mb by more than 50%"
    - "WARNING: file L0.6[101,101] 2ns 2.44gb exceeds soft limit 100mb by more than 50%"
    "###
    );
}

#[tokio::test]
async fn many_medium_files() {
    test_helpers::maybe_start_logging();
    let setup = layout_setup_builder().await.build().await;

    // The compactor has 20 files overlapping files with a single
    // timestamp that indivdually are small enough to be processed,
    // but when compacted together are too large and can't be split by
    // timestamp
    for i in 0..20 {
        setup
            .partition
            .create_parquet_file(
                parquet_builder()
                    .with_min_time(100)
                    .with_max_time(100)
                    .with_file_size_bytes(30 * ONE_MB)
                    .with_compaction_level(CompactionLevel::Initial)
                    .with_max_l0_created_at(Time::from_timestamp_nanos(i + 1)),
            )
            .await;
    }

    insta::assert_yaml_snapshot!(
        run_layout_scenario(&setup).await,
        @r###"
    ---
    - "**** Input Files "
    - "L0, all files 30mb                                                                                                 "
    - "L0.1[100,100] 1ns        |------------------------------------------L0.1------------------------------------------|"
    - "L0.2[100,100] 2ns        |------------------------------------------L0.2------------------------------------------|"
    - "L0.3[100,100] 3ns        |------------------------------------------L0.3------------------------------------------|"
    - "L0.4[100,100] 4ns        |------------------------------------------L0.4------------------------------------------|"
    - "L0.5[100,100] 5ns        |------------------------------------------L0.5------------------------------------------|"
    - "L0.6[100,100] 6ns        |------------------------------------------L0.6------------------------------------------|"
    - "L0.7[100,100] 7ns        |------------------------------------------L0.7------------------------------------------|"
    - "L0.8[100,100] 8ns        |------------------------------------------L0.8------------------------------------------|"
    - "L0.9[100,100] 9ns        |------------------------------------------L0.9------------------------------------------|"
    - "L0.10[100,100] 10ns      |-----------------------------------------L0.10------------------------------------------|"
    - "L0.11[100,100] 11ns      |-----------------------------------------L0.11------------------------------------------|"
    - "L0.12[100,100] 12ns      |-----------------------------------------L0.12------------------------------------------|"
    - "L0.13[100,100] 13ns      |-----------------------------------------L0.13------------------------------------------|"
    - "L0.14[100,100] 14ns      |-----------------------------------------L0.14------------------------------------------|"
    - "L0.15[100,100] 15ns      |-----------------------------------------L0.15------------------------------------------|"
    - "L0.16[100,100] 16ns      |-----------------------------------------L0.16------------------------------------------|"
    - "L0.17[100,100] 17ns      |-----------------------------------------L0.17------------------------------------------|"
    - "L0.18[100,100] 18ns      |-----------------------------------------L0.18------------------------------------------|"
    - "L0.19[100,100] 19ns      |-----------------------------------------L0.19------------------------------------------|"
    - "L0.20[100,100] 20ns      |-----------------------------------------L0.20------------------------------------------|"
    - "**** Simulation run 0, type=compact(FoundSubsetLessThanMaxCompactSize). 10 Input Files, 300mb total:"
    - "L0, all files 30mb                                                                                                 "
    - "L0.1[100,100] 1ns        |------------------------------------------L0.1------------------------------------------|"
    - "L0.2[100,100] 2ns        |------------------------------------------L0.2------------------------------------------|"
    - "L0.3[100,100] 3ns        |------------------------------------------L0.3------------------------------------------|"
    - "L0.4[100,100] 4ns        |------------------------------------------L0.4------------------------------------------|"
    - "L0.5[100,100] 5ns        |------------------------------------------L0.5------------------------------------------|"
    - "L0.6[100,100] 6ns        |------------------------------------------L0.6------------------------------------------|"
    - "L0.7[100,100] 7ns        |------------------------------------------L0.7------------------------------------------|"
    - "L0.8[100,100] 8ns        |------------------------------------------L0.8------------------------------------------|"
    - "L0.9[100,100] 9ns        |------------------------------------------L0.9------------------------------------------|"
    - "L0.10[100,100] 10ns      |-----------------------------------------L0.10------------------------------------------|"
    - "**** 1 Output Files (parquet_file_id not yet assigned), 300mb total:"
    - "L1, all files 300mb                                                                                                "
    - "L1.?[100,100] 10ns       |------------------------------------------L1.?------------------------------------------|"
    - "Committing partition 1:"
    - "  Soft Deleting 10 files: L0.1, L0.2, L0.3, L0.4, L0.5, L0.6, L0.7, L0.8, L0.9, L0.10"
    - "  Creating 1 files"
    - "SKIPPED COMPACTION for PartitionId(1): partition 1 has overlapped files that exceed max compact size limit 314572800. This may happen if a large amount of data has the same timestamp"
    - "**** Final Output Files (300mb written)"
    - "L0                                                                                                                 "
    - "L0.11[100,100] 11ns 30mb |-----------------------------------------L0.11------------------------------------------|"
    - "L0.12[100,100] 12ns 30mb |-----------------------------------------L0.12------------------------------------------|"
    - "L0.13[100,100] 13ns 30mb |-----------------------------------------L0.13------------------------------------------|"
    - "L0.14[100,100] 14ns 30mb |-----------------------------------------L0.14------------------------------------------|"
    - "L0.15[100,100] 15ns 30mb |-----------------------------------------L0.15------------------------------------------|"
    - "L0.16[100,100] 16ns 30mb |-----------------------------------------L0.16------------------------------------------|"
    - "L0.17[100,100] 17ns 30mb |-----------------------------------------L0.17------------------------------------------|"
    - "L0.18[100,100] 18ns 30mb |-----------------------------------------L0.18------------------------------------------|"
    - "L0.19[100,100] 19ns 30mb |-----------------------------------------L0.19------------------------------------------|"
    - "L0.20[100,100] 20ns 30mb |-----------------------------------------L0.20------------------------------------------|"
    - "L1                                                                                                                 "
    - "L1.21[100,100] 10ns 300mb|-----------------------------------------L1.21------------------------------------------|"
    - "WARNING: file L1.21[100,100] 10ns 300mb exceeds soft limit 100mb by more than 50%"
    "###
    );
}

#[tokio::test]
async fn many_medium_files_time_range_1() {
    test_helpers::maybe_start_logging();
    let setup = layout_setup_builder().await.build().await;

    // The compactor has 20 files overlapping files with a single
    // timestamp that indivdually are small enough to be processed,
    // but when compacted together are too large and can't be split by
    // timestamp
    for i in 0..20 {
        setup
            .partition
            .create_parquet_file(
                parquet_builder()
                    .with_min_time(100)
                    .with_max_time(101)
                    .with_file_size_bytes(30 * ONE_MB)
                    .with_compaction_level(CompactionLevel::Initial)
                    .with_max_l0_created_at(Time::from_timestamp_nanos(i + 1)),
            )
            .await;
    }

    insta::assert_yaml_snapshot!(
        run_layout_scenario(&setup).await,
        @r###"
    ---
    - "**** Input Files "
    - "L0, all files 30mb                                                                                                 "
    - "L0.1[100,101] 1ns        |------------------------------------------L0.1------------------------------------------|"
    - "L0.2[100,101] 2ns        |------------------------------------------L0.2------------------------------------------|"
    - "L0.3[100,101] 3ns        |------------------------------------------L0.3------------------------------------------|"
    - "L0.4[100,101] 4ns        |------------------------------------------L0.4------------------------------------------|"
    - "L0.5[100,101] 5ns        |------------------------------------------L0.5------------------------------------------|"
    - "L0.6[100,101] 6ns        |------------------------------------------L0.6------------------------------------------|"
    - "L0.7[100,101] 7ns        |------------------------------------------L0.7------------------------------------------|"
    - "L0.8[100,101] 8ns        |------------------------------------------L0.8------------------------------------------|"
    - "L0.9[100,101] 9ns        |------------------------------------------L0.9------------------------------------------|"
    - "L0.10[100,101] 10ns      |-----------------------------------------L0.10------------------------------------------|"
    - "L0.11[100,101] 11ns      |-----------------------------------------L0.11------------------------------------------|"
    - "L0.12[100,101] 12ns      |-----------------------------------------L0.12------------------------------------------|"
    - "L0.13[100,101] 13ns      |-----------------------------------------L0.13------------------------------------------|"
    - "L0.14[100,101] 14ns      |-----------------------------------------L0.14------------------------------------------|"
    - "L0.15[100,101] 15ns      |-----------------------------------------L0.15------------------------------------------|"
    - "L0.16[100,101] 16ns      |-----------------------------------------L0.16------------------------------------------|"
    - "L0.17[100,101] 17ns      |-----------------------------------------L0.17------------------------------------------|"
    - "L0.18[100,101] 18ns      |-----------------------------------------L0.18------------------------------------------|"
    - "L0.19[100,101] 19ns      |-----------------------------------------L0.19------------------------------------------|"
    - "L0.20[100,101] 20ns      |-----------------------------------------L0.20------------------------------------------|"
    - "**** Simulation run 0, type=split(VerticalSplit)(split_times=[100]). 1 Input Files, 30mb total:"
    - "L0, all files 30mb                                                                                                 "
    - "L0.1[100,101] 1ns        |------------------------------------------L0.1------------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 30mb total:"
    - "L0, all files 15mb                                                                                                 "
    - "L0.?[100,100] 1ns        |L0.?|                                                                                    "
    - "L0.?[101,101] 1ns                                                                                                  |L0.?|"
    - "**** Simulation run 1, type=split(VerticalSplit)(split_times=[100]). 1 Input Files, 30mb total:"
    - "L0, all files 30mb                                                                                                 "
    - "L0.2[100,101] 2ns        |------------------------------------------L0.2------------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 30mb total:"
    - "L0, all files 15mb                                                                                                 "
    - "L0.?[100,100] 2ns        |L0.?|                                                                                    "
    - "L0.?[101,101] 2ns                                                                                                  |L0.?|"
    - "**** Simulation run 2, type=split(VerticalSplit)(split_times=[100]). 1 Input Files, 30mb total:"
    - "L0, all files 30mb                                                                                                 "
    - "L0.3[100,101] 3ns        |------------------------------------------L0.3------------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 30mb total:"
    - "L0, all files 15mb                                                                                                 "
    - "L0.?[100,100] 3ns        |L0.?|                                                                                    "
    - "L0.?[101,101] 3ns                                                                                                  |L0.?|"
    - "**** Simulation run 3, type=split(VerticalSplit)(split_times=[100]). 1 Input Files, 30mb total:"
    - "L0, all files 30mb                                                                                                 "
    - "L0.4[100,101] 4ns        |------------------------------------------L0.4------------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 30mb total:"
    - "L0, all files 15mb                                                                                                 "
    - "L0.?[100,100] 4ns        |L0.?|                                                                                    "
    - "L0.?[101,101] 4ns                                                                                                  |L0.?|"
    - "**** Simulation run 4, type=split(VerticalSplit)(split_times=[100]). 1 Input Files, 30mb total:"
    - "L0, all files 30mb                                                                                                 "
    - "L0.5[100,101] 5ns        |------------------------------------------L0.5------------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 30mb total:"
    - "L0, all files 15mb                                                                                                 "
    - "L0.?[100,100] 5ns        |L0.?|                                                                                    "
    - "L0.?[101,101] 5ns                                                                                                  |L0.?|"
    - "**** Simulation run 5, type=split(VerticalSplit)(split_times=[100]). 1 Input Files, 30mb total:"
    - "L0, all files 30mb                                                                                                 "
    - "L0.6[100,101] 6ns        |------------------------------------------L0.6------------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 30mb total:"
    - "L0, all files 15mb                                                                                                 "
    - "L0.?[100,100] 6ns        |L0.?|                                                                                    "
    - "L0.?[101,101] 6ns                                                                                                  |L0.?|"
    - "**** Simulation run 6, type=split(VerticalSplit)(split_times=[100]). 1 Input Files, 30mb total:"
    - "L0, all files 30mb                                                                                                 "
    - "L0.7[100,101] 7ns        |------------------------------------------L0.7------------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 30mb total:"
    - "L0, all files 15mb                                                                                                 "
    - "L0.?[100,100] 7ns        |L0.?|                                                                                    "
    - "L0.?[101,101] 7ns                                                                                                  |L0.?|"
    - "**** Simulation run 7, type=split(VerticalSplit)(split_times=[100]). 1 Input Files, 30mb total:"
    - "L0, all files 30mb                                                                                                 "
    - "L0.8[100,101] 8ns        |------------------------------------------L0.8------------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 30mb total:"
    - "L0, all files 15mb                                                                                                 "
    - "L0.?[100,100] 8ns        |L0.?|                                                                                    "
    - "L0.?[101,101] 8ns                                                                                                  |L0.?|"
    - "**** Simulation run 8, type=split(VerticalSplit)(split_times=[100]). 1 Input Files, 30mb total:"
    - "L0, all files 30mb                                                                                                 "
    - "L0.9[100,101] 9ns        |------------------------------------------L0.9------------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 30mb total:"
    - "L0, all files 15mb                                                                                                 "
    - "L0.?[100,100] 9ns        |L0.?|                                                                                    "
    - "L0.?[101,101] 9ns                                                                                                  |L0.?|"
    - "**** Simulation run 9, type=split(VerticalSplit)(split_times=[100]). 1 Input Files, 30mb total:"
    - "L0, all files 30mb                                                                                                 "
    - "L0.10[100,101] 10ns      |-----------------------------------------L0.10------------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 30mb total:"
    - "L0, all files 15mb                                                                                                 "
    - "L0.?[100,100] 10ns       |L0.?|                                                                                    "
    - "L0.?[101,101] 10ns                                                                                                 |L0.?|"
    - "**** Simulation run 10, type=split(VerticalSplit)(split_times=[100]). 1 Input Files, 30mb total:"
    - "L0, all files 30mb                                                                                                 "
    - "L0.11[100,101] 11ns      |-----------------------------------------L0.11------------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 30mb total:"
    - "L0, all files 15mb                                                                                                 "
    - "L0.?[100,100] 11ns       |L0.?|                                                                                    "
    - "L0.?[101,101] 11ns                                                                                                 |L0.?|"
    - "**** Simulation run 11, type=split(VerticalSplit)(split_times=[100]). 1 Input Files, 30mb total:"
    - "L0, all files 30mb                                                                                                 "
    - "L0.12[100,101] 12ns      |-----------------------------------------L0.12------------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 30mb total:"
    - "L0, all files 15mb                                                                                                 "
    - "L0.?[100,100] 12ns       |L0.?|                                                                                    "
    - "L0.?[101,101] 12ns                                                                                                 |L0.?|"
    - "**** Simulation run 12, type=split(VerticalSplit)(split_times=[100]). 1 Input Files, 30mb total:"
    - "L0, all files 30mb                                                                                                 "
    - "L0.13[100,101] 13ns      |-----------------------------------------L0.13------------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 30mb total:"
    - "L0, all files 15mb                                                                                                 "
    - "L0.?[100,100] 13ns       |L0.?|                                                                                    "
    - "L0.?[101,101] 13ns                                                                                                 |L0.?|"
    - "**** Simulation run 13, type=split(VerticalSplit)(split_times=[100]). 1 Input Files, 30mb total:"
    - "L0, all files 30mb                                                                                                 "
    - "L0.14[100,101] 14ns      |-----------------------------------------L0.14------------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 30mb total:"
    - "L0, all files 15mb                                                                                                 "
    - "L0.?[100,100] 14ns       |L0.?|                                                                                    "
    - "L0.?[101,101] 14ns                                                                                                 |L0.?|"
    - "**** Simulation run 14, type=split(VerticalSplit)(split_times=[100]). 1 Input Files, 30mb total:"
    - "L0, all files 30mb                                                                                                 "
    - "L0.15[100,101] 15ns      |-----------------------------------------L0.15------------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 30mb total:"
    - "L0, all files 15mb                                                                                                 "
    - "L0.?[100,100] 15ns       |L0.?|                                                                                    "
    - "L0.?[101,101] 15ns                                                                                                 |L0.?|"
    - "**** Simulation run 15, type=split(VerticalSplit)(split_times=[100]). 1 Input Files, 30mb total:"
    - "L0, all files 30mb                                                                                                 "
    - "L0.16[100,101] 16ns      |-----------------------------------------L0.16------------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 30mb total:"
    - "L0, all files 15mb                                                                                                 "
    - "L0.?[100,100] 16ns       |L0.?|                                                                                    "
    - "L0.?[101,101] 16ns                                                                                                 |L0.?|"
    - "**** Simulation run 16, type=split(VerticalSplit)(split_times=[100]). 1 Input Files, 30mb total:"
    - "L0, all files 30mb                                                                                                 "
    - "L0.17[100,101] 17ns      |-----------------------------------------L0.17------------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 30mb total:"
    - "L0, all files 15mb                                                                                                 "
    - "L0.?[100,100] 17ns       |L0.?|                                                                                    "
    - "L0.?[101,101] 17ns                                                                                                 |L0.?|"
    - "**** Simulation run 17, type=split(VerticalSplit)(split_times=[100]). 1 Input Files, 30mb total:"
    - "L0, all files 30mb                                                                                                 "
    - "L0.18[100,101] 18ns      |-----------------------------------------L0.18------------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 30mb total:"
    - "L0, all files 15mb                                                                                                 "
    - "L0.?[100,100] 18ns       |L0.?|                                                                                    "
    - "L0.?[101,101] 18ns                                                                                                 |L0.?|"
    - "**** Simulation run 18, type=split(VerticalSplit)(split_times=[100]). 1 Input Files, 30mb total:"
    - "L0, all files 30mb                                                                                                 "
    - "L0.19[100,101] 19ns      |-----------------------------------------L0.19------------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 30mb total:"
    - "L0, all files 15mb                                                                                                 "
    - "L0.?[100,100] 19ns       |L0.?|                                                                                    "
    - "L0.?[101,101] 19ns                                                                                                 |L0.?|"
    - "**** Simulation run 19, type=split(VerticalSplit)(split_times=[100]). 1 Input Files, 30mb total:"
    - "L0, all files 30mb                                                                                                 "
    - "L0.20[100,101] 20ns      |-----------------------------------------L0.20------------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 30mb total:"
    - "L0, all files 15mb                                                                                                 "
    - "L0.?[100,100] 20ns       |L0.?|                                                                                    "
    - "L0.?[101,101] 20ns                                                                                                 |L0.?|"
    - "Committing partition 1:"
    - "  Soft Deleting 20 files: L0.1, L0.2, L0.3, L0.4, L0.5, L0.6, L0.7, L0.8, L0.9, L0.10, L0.11, L0.12, L0.13, L0.14, L0.15, L0.16, L0.17, L0.18, L0.19, L0.20"
    - "  Creating 40 files"
    - "**** Simulation run 20, type=compact(TotalSizeLessThanMaxCompactSize). 20 Input Files, 300mb total:"
    - "L0, all files 15mb                                                                                                 "
    - "L0.59[100,100] 20ns      |-----------------------------------------L0.59------------------------------------------|"
    - "L0.57[100,100] 19ns      |-----------------------------------------L0.57------------------------------------------|"
    - "L0.55[100,100] 18ns      |-----------------------------------------L0.55------------------------------------------|"
    - "L0.53[100,100] 17ns      |-----------------------------------------L0.53------------------------------------------|"
    - "L0.51[100,100] 16ns      |-----------------------------------------L0.51------------------------------------------|"
    - "L0.49[100,100] 15ns      |-----------------------------------------L0.49------------------------------------------|"
    - "L0.47[100,100] 14ns      |-----------------------------------------L0.47------------------------------------------|"
    - "L0.45[100,100] 13ns      |-----------------------------------------L0.45------------------------------------------|"
    - "L0.43[100,100] 12ns      |-----------------------------------------L0.43------------------------------------------|"
    - "L0.41[100,100] 11ns      |-----------------------------------------L0.41------------------------------------------|"
    - "L0.39[100,100] 10ns      |-----------------------------------------L0.39------------------------------------------|"
    - "L0.37[100,100] 9ns       |-----------------------------------------L0.37------------------------------------------|"
    - "L0.35[100,100] 8ns       |-----------------------------------------L0.35------------------------------------------|"
    - "L0.33[100,100] 7ns       |-----------------------------------------L0.33------------------------------------------|"
    - "L0.31[100,100] 6ns       |-----------------------------------------L0.31------------------------------------------|"
    - "L0.29[100,100] 5ns       |-----------------------------------------L0.29------------------------------------------|"
    - "L0.27[100,100] 4ns       |-----------------------------------------L0.27------------------------------------------|"
    - "L0.25[100,100] 3ns       |-----------------------------------------L0.25------------------------------------------|"
    - "L0.23[100,100] 2ns       |-----------------------------------------L0.23------------------------------------------|"
    - "L0.21[100,100] 1ns       |-----------------------------------------L0.21------------------------------------------|"
    - "**** 1 Output Files (parquet_file_id not yet assigned), 300mb total:"
    - "L1, all files 300mb                                                                                                "
    - "L1.?[100,100] 20ns       |------------------------------------------L1.?------------------------------------------|"
    - "Committing partition 1:"
    - "  Soft Deleting 20 files: L0.21, L0.23, L0.25, L0.27, L0.29, L0.31, L0.33, L0.35, L0.37, L0.39, L0.41, L0.43, L0.45, L0.47, L0.49, L0.51, L0.53, L0.55, L0.57, L0.59"
    - "  Creating 1 files"
    - "**** Simulation run 21, type=compact(TotalSizeLessThanMaxCompactSize). 20 Input Files, 300mb total:"
    - "L0, all files 15mb                                                                                                 "
    - "L0.60[101,101] 20ns      |-----------------------------------------L0.60------------------------------------------|"
    - "L0.58[101,101] 19ns      |-----------------------------------------L0.58------------------------------------------|"
    - "L0.56[101,101] 18ns      |-----------------------------------------L0.56------------------------------------------|"
    - "L0.54[101,101] 17ns      |-----------------------------------------L0.54------------------------------------------|"
    - "L0.52[101,101] 16ns      |-----------------------------------------L0.52------------------------------------------|"
    - "L0.50[101,101] 15ns      |-----------------------------------------L0.50------------------------------------------|"
    - "L0.48[101,101] 14ns      |-----------------------------------------L0.48------------------------------------------|"
    - "L0.46[101,101] 13ns      |-----------------------------------------L0.46------------------------------------------|"
    - "L0.44[101,101] 12ns      |-----------------------------------------L0.44------------------------------------------|"
    - "L0.42[101,101] 11ns      |-----------------------------------------L0.42------------------------------------------|"
    - "L0.40[101,101] 10ns      |-----------------------------------------L0.40------------------------------------------|"
    - "L0.38[101,101] 9ns       |-----------------------------------------L0.38------------------------------------------|"
    - "L0.36[101,101] 8ns       |-----------------------------------------L0.36------------------------------------------|"
    - "L0.34[101,101] 7ns       |-----------------------------------------L0.34------------------------------------------|"
    - "L0.32[101,101] 6ns       |-----------------------------------------L0.32------------------------------------------|"
    - "L0.30[101,101] 5ns       |-----------------------------------------L0.30------------------------------------------|"
    - "L0.28[101,101] 4ns       |-----------------------------------------L0.28------------------------------------------|"
    - "L0.26[101,101] 3ns       |-----------------------------------------L0.26------------------------------------------|"
    - "L0.24[101,101] 2ns       |-----------------------------------------L0.24------------------------------------------|"
    - "L0.22[101,101] 1ns       |-----------------------------------------L0.22------------------------------------------|"
    - "**** 1 Output Files (parquet_file_id not yet assigned), 300mb total:"
    - "L1, all files 300mb                                                                                                "
    - "L1.?[101,101] 20ns       |------------------------------------------L1.?------------------------------------------|"
    - "Committing partition 1:"
    - "  Soft Deleting 20 files: L0.22, L0.24, L0.26, L0.28, L0.30, L0.32, L0.34, L0.36, L0.38, L0.40, L0.42, L0.44, L0.46, L0.48, L0.50, L0.52, L0.54, L0.56, L0.58, L0.60"
    - "  Creating 1 files"
    - "Committing partition 1:"
    - "  Upgrading 2 files level to CompactionLevel::L2: L1.61, L1.62"
    - "**** Final Output Files (1.17gb written)"
    - "L2, all files 300mb                                                                                                "
    - "L2.61[100,100] 20ns      |L2.61|                                                                                   "
    - "L2.62[101,101] 20ns                                                                                                |L2.62|"
    - "WARNING: file L2.61[100,100] 20ns 300mb exceeds soft limit 100mb by more than 50%"
    - "WARNING: file L2.62[101,101] 20ns 300mb exceeds soft limit 100mb by more than 50%"
    "###
    );
}

#[tokio::test]
async fn many_small_files() {
    test_helpers::maybe_start_logging();
    let setup = layout_setup_builder().await.build().await;

    // The compactor has 20 files overlapping files that together are
    // below the partition limit (256MB) but above the target file
    // size limit (100MB) but can't be split by timestamp
    for i in 0..20 {
        setup
            .partition
            .create_parquet_file(
                parquet_builder()
                    .with_min_time(100)
                    .with_max_time(100)
                    .with_file_size_bytes(10 * ONE_MB)
                    .with_compaction_level(CompactionLevel::Initial)
                    .with_max_l0_created_at(Time::from_timestamp_nanos(i + 1)),
            )
            .await;
    }

    // L0s are compacted into a single L1 file. It can't be split because of single timestamp
    // Then the L1 is large enough to get upgraded to L2
    insta::assert_yaml_snapshot!(
        run_layout_scenario(&setup).await,
        @r###"
    ---
    - "**** Input Files "
    - "L0, all files 10mb                                                                                                 "
    - "L0.1[100,100] 1ns        |------------------------------------------L0.1------------------------------------------|"
    - "L0.2[100,100] 2ns        |------------------------------------------L0.2------------------------------------------|"
    - "L0.3[100,100] 3ns        |------------------------------------------L0.3------------------------------------------|"
    - "L0.4[100,100] 4ns        |------------------------------------------L0.4------------------------------------------|"
    - "L0.5[100,100] 5ns        |------------------------------------------L0.5------------------------------------------|"
    - "L0.6[100,100] 6ns        |------------------------------------------L0.6------------------------------------------|"
    - "L0.7[100,100] 7ns        |------------------------------------------L0.7------------------------------------------|"
    - "L0.8[100,100] 8ns        |------------------------------------------L0.8------------------------------------------|"
    - "L0.9[100,100] 9ns        |------------------------------------------L0.9------------------------------------------|"
    - "L0.10[100,100] 10ns      |-----------------------------------------L0.10------------------------------------------|"
    - "L0.11[100,100] 11ns      |-----------------------------------------L0.11------------------------------------------|"
    - "L0.12[100,100] 12ns      |-----------------------------------------L0.12------------------------------------------|"
    - "L0.13[100,100] 13ns      |-----------------------------------------L0.13------------------------------------------|"
    - "L0.14[100,100] 14ns      |-----------------------------------------L0.14------------------------------------------|"
    - "L0.15[100,100] 15ns      |-----------------------------------------L0.15------------------------------------------|"
    - "L0.16[100,100] 16ns      |-----------------------------------------L0.16------------------------------------------|"
    - "L0.17[100,100] 17ns      |-----------------------------------------L0.17------------------------------------------|"
    - "L0.18[100,100] 18ns      |-----------------------------------------L0.18------------------------------------------|"
    - "L0.19[100,100] 19ns      |-----------------------------------------L0.19------------------------------------------|"
    - "L0.20[100,100] 20ns      |-----------------------------------------L0.20------------------------------------------|"
    - "**** Simulation run 0, type=compact(TotalSizeLessThanMaxCompactSize). 20 Input Files, 200mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.20[100,100] 20ns      |-----------------------------------------L0.20------------------------------------------|"
    - "L0.19[100,100] 19ns      |-----------------------------------------L0.19------------------------------------------|"
    - "L0.18[100,100] 18ns      |-----------------------------------------L0.18------------------------------------------|"
    - "L0.17[100,100] 17ns      |-----------------------------------------L0.17------------------------------------------|"
    - "L0.16[100,100] 16ns      |-----------------------------------------L0.16------------------------------------------|"
    - "L0.15[100,100] 15ns      |-----------------------------------------L0.15------------------------------------------|"
    - "L0.14[100,100] 14ns      |-----------------------------------------L0.14------------------------------------------|"
    - "L0.13[100,100] 13ns      |-----------------------------------------L0.13------------------------------------------|"
    - "L0.12[100,100] 12ns      |-----------------------------------------L0.12------------------------------------------|"
    - "L0.11[100,100] 11ns      |-----------------------------------------L0.11------------------------------------------|"
    - "L0.10[100,100] 10ns      |-----------------------------------------L0.10------------------------------------------|"
    - "L0.9[100,100] 9ns        |------------------------------------------L0.9------------------------------------------|"
    - "L0.8[100,100] 8ns        |------------------------------------------L0.8------------------------------------------|"
    - "L0.7[100,100] 7ns        |------------------------------------------L0.7------------------------------------------|"
    - "L0.6[100,100] 6ns        |------------------------------------------L0.6------------------------------------------|"
    - "L0.5[100,100] 5ns        |------------------------------------------L0.5------------------------------------------|"
    - "L0.4[100,100] 4ns        |------------------------------------------L0.4------------------------------------------|"
    - "L0.3[100,100] 3ns        |------------------------------------------L0.3------------------------------------------|"
    - "L0.2[100,100] 2ns        |------------------------------------------L0.2------------------------------------------|"
    - "L0.1[100,100] 1ns        |------------------------------------------L0.1------------------------------------------|"
    - "**** 1 Output Files (parquet_file_id not yet assigned), 200mb total:"
    - "L1, all files 200mb                                                                                                "
    - "L1.?[100,100] 20ns       |------------------------------------------L1.?------------------------------------------|"
    - "Committing partition 1:"
    - "  Soft Deleting 20 files: L0.1, L0.2, L0.3, L0.4, L0.5, L0.6, L0.7, L0.8, L0.9, L0.10, L0.11, L0.12, L0.13, L0.14, L0.15, L0.16, L0.17, L0.18, L0.19, L0.20"
    - "  Creating 1 files"
    - "Committing partition 1:"
    - "  Upgrading 1 files level to CompactionLevel::L2: L1.21"
    - "**** Final Output Files (200mb written)"
    - "L2, all files 200mb                                                                                                "
    - "L2.21[100,100] 20ns      |-----------------------------------------L2.21------------------------------------------|"
    - "WARNING: file L2.21[100,100] 20ns 200mb exceeds soft limit 100mb by more than 50%"
    "###
    );
}
