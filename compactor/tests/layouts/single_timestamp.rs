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
    - "**** Simulation run 0, type=split(ReduceLargeFileSize)(split_times=[100]). 1 Input Files, 4.88gb total:"
    - "L0, all files 4.88gb                                                                                               "
    - "L0.1[100,101] 1ns        |------------------------------------------L0.1------------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 4.88gb total:"
    - "L0, all files 2.44gb                                                                                               "
    - "L0.?[100,100] 1ns        |L0.?|                                                                                    "
    - "L0.?[101,101] 1ns                                                                                                  |L0.?|"
    - "Committing partition 1:"
    - "  Soft Deleting 1 files: L0.1"
    - "  Creating 2 files"
    - "SKIPPED COMPACTION for PartitionId(1): partition 1 has overlapped files that exceed max compact size limit 314572800. This may happen if a large amount of data has the same timestamp"
    - "**** Final Output Files (4.88gb written)"
    - "L0                                                                                                                 "
    - "L0.2[100,101] 2ns 4.88gb |------------------------------------------L0.2------------------------------------------|"
    - "L0.3[100,100] 1ns 2.44gb |L0.3|                                                                                    "
    - "L0.4[101,101] 1ns 2.44gb                                                                                           |L0.4|"
    - "WARNING: file L0.2[100,101] 2ns 4.88gb exceeds soft limit 100mb by more than 50%"
    - "WARNING: file L0.3[100,100] 1ns 2.44gb exceeds soft limit 100mb by more than 50%"
    - "WARNING: file L0.4[101,101] 1ns 2.44gb exceeds soft limit 100mb by more than 50%"
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
    - "**** Simulation run 0, type=compact(FoundSubsetLessThanMaxCompactSize). 10 Input Files, 300mb total:"
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
    - "**** 1 Output Files (parquet_file_id not yet assigned), 300mb total:"
    - "L1, all files 300mb                                                                                                "
    - "L1.?[100,101] 10ns       |------------------------------------------L1.?------------------------------------------|"
    - "Committing partition 1:"
    - "  Soft Deleting 10 files: L0.1, L0.2, L0.3, L0.4, L0.5, L0.6, L0.7, L0.8, L0.9, L0.10"
    - "  Creating 1 files"
    - "**** Simulation run 1, type=split(ReduceLargeFileSize)(split_times=[100]). 1 Input Files, 300mb total:"
    - "L1, all files 300mb                                                                                                "
    - "L1.21[100,101] 10ns      |-----------------------------------------L1.21------------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 300mb total:"
    - "L1, all files 150mb                                                                                                "
    - "L1.?[100,100] 10ns       |L1.?|                                                                                    "
    - "L1.?[101,101] 10ns                                                                                                 |L1.?|"
    - "Committing partition 1:"
    - "  Soft Deleting 1 files: L1.21"
    - "  Creating 2 files"
    - "**** Simulation run 2, type=split(ReduceOverlap)(split_times=[100]). 1 Input Files, 30mb total:"
    - "L0, all files 30mb                                                                                                 "
    - "L0.11[100,101] 11ns      |-----------------------------------------L0.11------------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 30mb total:"
    - "L0, all files 15mb                                                                                                 "
    - "L0.?[100,100] 11ns       |L0.?|                                                                                    "
    - "L0.?[101,101] 11ns                                                                                                 |L0.?|"
    - "**** Simulation run 3, type=split(ReduceOverlap)(split_times=[100]). 1 Input Files, 30mb total:"
    - "L0, all files 30mb                                                                                                 "
    - "L0.12[100,101] 12ns      |-----------------------------------------L0.12------------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 30mb total:"
    - "L0, all files 15mb                                                                                                 "
    - "L0.?[100,100] 12ns       |L0.?|                                                                                    "
    - "L0.?[101,101] 12ns                                                                                                 |L0.?|"
    - "**** Simulation run 4, type=split(ReduceOverlap)(split_times=[100]). 1 Input Files, 30mb total:"
    - "L0, all files 30mb                                                                                                 "
    - "L0.13[100,101] 13ns      |-----------------------------------------L0.13------------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 30mb total:"
    - "L0, all files 15mb                                                                                                 "
    - "L0.?[100,100] 13ns       |L0.?|                                                                                    "
    - "L0.?[101,101] 13ns                                                                                                 |L0.?|"
    - "**** Simulation run 5, type=split(ReduceOverlap)(split_times=[100]). 1 Input Files, 30mb total:"
    - "L0, all files 30mb                                                                                                 "
    - "L0.14[100,101] 14ns      |-----------------------------------------L0.14------------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 30mb total:"
    - "L0, all files 15mb                                                                                                 "
    - "L0.?[100,100] 14ns       |L0.?|                                                                                    "
    - "L0.?[101,101] 14ns                                                                                                 |L0.?|"
    - "**** Simulation run 6, type=split(ReduceOverlap)(split_times=[100]). 1 Input Files, 30mb total:"
    - "L0, all files 30mb                                                                                                 "
    - "L0.15[100,101] 15ns      |-----------------------------------------L0.15------------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 30mb total:"
    - "L0, all files 15mb                                                                                                 "
    - "L0.?[100,100] 15ns       |L0.?|                                                                                    "
    - "L0.?[101,101] 15ns                                                                                                 |L0.?|"
    - "**** Simulation run 7, type=split(ReduceOverlap)(split_times=[100]). 1 Input Files, 30mb total:"
    - "L0, all files 30mb                                                                                                 "
    - "L0.16[100,101] 16ns      |-----------------------------------------L0.16------------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 30mb total:"
    - "L0, all files 15mb                                                                                                 "
    - "L0.?[100,100] 16ns       |L0.?|                                                                                    "
    - "L0.?[101,101] 16ns                                                                                                 |L0.?|"
    - "**** Simulation run 8, type=split(ReduceOverlap)(split_times=[100]). 1 Input Files, 30mb total:"
    - "L0, all files 30mb                                                                                                 "
    - "L0.17[100,101] 17ns      |-----------------------------------------L0.17------------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 30mb total:"
    - "L0, all files 15mb                                                                                                 "
    - "L0.?[100,100] 17ns       |L0.?|                                                                                    "
    - "L0.?[101,101] 17ns                                                                                                 |L0.?|"
    - "**** Simulation run 9, type=split(ReduceOverlap)(split_times=[100]). 1 Input Files, 30mb total:"
    - "L0, all files 30mb                                                                                                 "
    - "L0.18[100,101] 18ns      |-----------------------------------------L0.18------------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 30mb total:"
    - "L0, all files 15mb                                                                                                 "
    - "L0.?[100,100] 18ns       |L0.?|                                                                                    "
    - "L0.?[101,101] 18ns                                                                                                 |L0.?|"
    - "**** Simulation run 10, type=split(ReduceOverlap)(split_times=[100]). 1 Input Files, 30mb total:"
    - "L0, all files 30mb                                                                                                 "
    - "L0.19[100,101] 19ns      |-----------------------------------------L0.19------------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 30mb total:"
    - "L0, all files 15mb                                                                                                 "
    - "L0.?[100,100] 19ns       |L0.?|                                                                                    "
    - "L0.?[101,101] 19ns                                                                                                 |L0.?|"
    - "**** Simulation run 11, type=split(ReduceOverlap)(split_times=[100]). 1 Input Files, 30mb total:"
    - "L0, all files 30mb                                                                                                 "
    - "L0.20[100,101] 20ns      |-----------------------------------------L0.20------------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 30mb total:"
    - "L0, all files 15mb                                                                                                 "
    - "L0.?[100,100] 20ns       |L0.?|                                                                                    "
    - "L0.?[101,101] 20ns                                                                                                 |L0.?|"
    - "Committing partition 1:"
    - "  Soft Deleting 10 files: L0.11, L0.12, L0.13, L0.14, L0.15, L0.16, L0.17, L0.18, L0.19, L0.20"
    - "  Creating 20 files"
    - "**** Simulation run 12, type=compact(FoundSubsetLessThanMaxCompactSize). 2 Input Files, 165mb total:"
    - "L0                                                                                                                 "
    - "L0.24[100,100] 11ns 15mb |-----------------------------------------L0.24------------------------------------------|"
    - "L1                                                                                                                 "
    - "L1.22[100,100] 10ns 150mb|-----------------------------------------L1.22------------------------------------------|"
    - "**** 1 Output Files (parquet_file_id not yet assigned), 165mb total:"
    - "L1, all files 165mb                                                                                                "
    - "L1.?[100,100] 11ns       |------------------------------------------L1.?------------------------------------------|"
    - "Committing partition 1:"
    - "  Soft Deleting 2 files: L1.22, L0.24"
    - "  Creating 1 files"
    - "**** Simulation run 13, type=compact(FoundSubsetLessThanMaxCompactSize). 2 Input Files, 165mb total:"
    - "L0                                                                                                                 "
    - "L0.25[101,101] 11ns 15mb |-----------------------------------------L0.25------------------------------------------|"
    - "L1                                                                                                                 "
    - "L1.23[101,101] 10ns 150mb|-----------------------------------------L1.23------------------------------------------|"
    - "**** 1 Output Files (parquet_file_id not yet assigned), 165mb total:"
    - "L1, all files 165mb                                                                                                "
    - "L1.?[101,101] 11ns       |------------------------------------------L1.?------------------------------------------|"
    - "Committing partition 1:"
    - "  Soft Deleting 2 files: L1.23, L0.25"
    - "  Creating 1 files"
    - "**** Simulation run 14, type=compact(FoundSubsetLessThanMaxCompactSize). 2 Input Files, 180mb total:"
    - "L0                                                                                                                 "
    - "L0.26[100,100] 12ns 15mb |-----------------------------------------L0.26------------------------------------------|"
    - "L1                                                                                                                 "
    - "L1.44[100,100] 11ns 165mb|-----------------------------------------L1.44------------------------------------------|"
    - "**** 1 Output Files (parquet_file_id not yet assigned), 180mb total:"
    - "L1, all files 180mb                                                                                                "
    - "L1.?[100,100] 12ns       |------------------------------------------L1.?------------------------------------------|"
    - "Committing partition 1:"
    - "  Soft Deleting 2 files: L0.26, L1.44"
    - "  Creating 1 files"
    - "**** Simulation run 15, type=compact(FoundSubsetLessThanMaxCompactSize). 2 Input Files, 180mb total:"
    - "L0                                                                                                                 "
    - "L0.27[101,101] 12ns 15mb |-----------------------------------------L0.27------------------------------------------|"
    - "L1                                                                                                                 "
    - "L1.45[101,101] 11ns 165mb|-----------------------------------------L1.45------------------------------------------|"
    - "**** 1 Output Files (parquet_file_id not yet assigned), 180mb total:"
    - "L1, all files 180mb                                                                                                "
    - "L1.?[101,101] 12ns       |------------------------------------------L1.?------------------------------------------|"
    - "Committing partition 1:"
    - "  Soft Deleting 2 files: L0.27, L1.45"
    - "  Creating 1 files"
    - "**** Simulation run 16, type=compact(FoundSubsetLessThanMaxCompactSize). 2 Input Files, 195mb total:"
    - "L0                                                                                                                 "
    - "L0.28[100,100] 13ns 15mb |-----------------------------------------L0.28------------------------------------------|"
    - "L1                                                                                                                 "
    - "L1.46[100,100] 12ns 180mb|-----------------------------------------L1.46------------------------------------------|"
    - "**** 1 Output Files (parquet_file_id not yet assigned), 195mb total:"
    - "L1, all files 195mb                                                                                                "
    - "L1.?[100,100] 13ns       |------------------------------------------L1.?------------------------------------------|"
    - "Committing partition 1:"
    - "  Soft Deleting 2 files: L0.28, L1.46"
    - "  Creating 1 files"
    - "**** Simulation run 17, type=compact(FoundSubsetLessThanMaxCompactSize). 2 Input Files, 195mb total:"
    - "L0                                                                                                                 "
    - "L0.29[101,101] 13ns 15mb |-----------------------------------------L0.29------------------------------------------|"
    - "L1                                                                                                                 "
    - "L1.47[101,101] 12ns 180mb|-----------------------------------------L1.47------------------------------------------|"
    - "**** 1 Output Files (parquet_file_id not yet assigned), 195mb total:"
    - "L1, all files 195mb                                                                                                "
    - "L1.?[101,101] 13ns       |------------------------------------------L1.?------------------------------------------|"
    - "Committing partition 1:"
    - "  Soft Deleting 2 files: L0.29, L1.47"
    - "  Creating 1 files"
    - "**** Simulation run 18, type=compact(FoundSubsetLessThanMaxCompactSize). 2 Input Files, 210mb total:"
    - "L0                                                                                                                 "
    - "L0.30[100,100] 14ns 15mb |-----------------------------------------L0.30------------------------------------------|"
    - "L1                                                                                                                 "
    - "L1.48[100,100] 13ns 195mb|-----------------------------------------L1.48------------------------------------------|"
    - "**** 1 Output Files (parquet_file_id not yet assigned), 210mb total:"
    - "L1, all files 210mb                                                                                                "
    - "L1.?[100,100] 14ns       |------------------------------------------L1.?------------------------------------------|"
    - "Committing partition 1:"
    - "  Soft Deleting 2 files: L0.30, L1.48"
    - "  Creating 1 files"
    - "**** Simulation run 19, type=compact(FoundSubsetLessThanMaxCompactSize). 2 Input Files, 210mb total:"
    - "L0                                                                                                                 "
    - "L0.31[101,101] 14ns 15mb |-----------------------------------------L0.31------------------------------------------|"
    - "L1                                                                                                                 "
    - "L1.49[101,101] 13ns 195mb|-----------------------------------------L1.49------------------------------------------|"
    - "**** 1 Output Files (parquet_file_id not yet assigned), 210mb total:"
    - "L1, all files 210mb                                                                                                "
    - "L1.?[101,101] 14ns       |------------------------------------------L1.?------------------------------------------|"
    - "Committing partition 1:"
    - "  Soft Deleting 2 files: L0.31, L1.49"
    - "  Creating 1 files"
    - "**** Simulation run 20, type=compact(FoundSubsetLessThanMaxCompactSize). 2 Input Files, 225mb total:"
    - "L0                                                                                                                 "
    - "L0.32[100,100] 15ns 15mb |-----------------------------------------L0.32------------------------------------------|"
    - "L1                                                                                                                 "
    - "L1.50[100,100] 14ns 210mb|-----------------------------------------L1.50------------------------------------------|"
    - "**** 1 Output Files (parquet_file_id not yet assigned), 225mb total:"
    - "L1, all files 225mb                                                                                                "
    - "L1.?[100,100] 15ns       |------------------------------------------L1.?------------------------------------------|"
    - "Committing partition 1:"
    - "  Soft Deleting 2 files: L0.32, L1.50"
    - "  Creating 1 files"
    - "**** Simulation run 21, type=compact(FoundSubsetLessThanMaxCompactSize). 2 Input Files, 225mb total:"
    - "L0                                                                                                                 "
    - "L0.33[101,101] 15ns 15mb |-----------------------------------------L0.33------------------------------------------|"
    - "L1                                                                                                                 "
    - "L1.51[101,101] 14ns 210mb|-----------------------------------------L1.51------------------------------------------|"
    - "**** 1 Output Files (parquet_file_id not yet assigned), 225mb total:"
    - "L1, all files 225mb                                                                                                "
    - "L1.?[101,101] 15ns       |------------------------------------------L1.?------------------------------------------|"
    - "Committing partition 1:"
    - "  Soft Deleting 2 files: L0.33, L1.51"
    - "  Creating 1 files"
    - "**** Simulation run 22, type=compact(FoundSubsetLessThanMaxCompactSize). 2 Input Files, 240mb total:"
    - "L0                                                                                                                 "
    - "L0.34[100,100] 16ns 15mb |-----------------------------------------L0.34------------------------------------------|"
    - "L1                                                                                                                 "
    - "L1.52[100,100] 15ns 225mb|-----------------------------------------L1.52------------------------------------------|"
    - "**** 1 Output Files (parquet_file_id not yet assigned), 240mb total:"
    - "L1, all files 240mb                                                                                                "
    - "L1.?[100,100] 16ns       |------------------------------------------L1.?------------------------------------------|"
    - "Committing partition 1:"
    - "  Soft Deleting 2 files: L0.34, L1.52"
    - "  Creating 1 files"
    - "**** Simulation run 23, type=compact(FoundSubsetLessThanMaxCompactSize). 2 Input Files, 240mb total:"
    - "L0                                                                                                                 "
    - "L0.35[101,101] 16ns 15mb |-----------------------------------------L0.35------------------------------------------|"
    - "L1                                                                                                                 "
    - "L1.53[101,101] 15ns 225mb|-----------------------------------------L1.53------------------------------------------|"
    - "**** 1 Output Files (parquet_file_id not yet assigned), 240mb total:"
    - "L1, all files 240mb                                                                                                "
    - "L1.?[101,101] 16ns       |------------------------------------------L1.?------------------------------------------|"
    - "Committing partition 1:"
    - "  Soft Deleting 2 files: L0.35, L1.53"
    - "  Creating 1 files"
    - "**** Simulation run 24, type=compact(FoundSubsetLessThanMaxCompactSize). 2 Input Files, 255mb total:"
    - "L0                                                                                                                 "
    - "L0.36[100,100] 17ns 15mb |-----------------------------------------L0.36------------------------------------------|"
    - "L1                                                                                                                 "
    - "L1.54[100,100] 16ns 240mb|-----------------------------------------L1.54------------------------------------------|"
    - "**** 1 Output Files (parquet_file_id not yet assigned), 255mb total:"
    - "L1, all files 255mb                                                                                                "
    - "L1.?[100,100] 17ns       |------------------------------------------L1.?------------------------------------------|"
    - "Committing partition 1:"
    - "  Soft Deleting 2 files: L0.36, L1.54"
    - "  Creating 1 files"
    - "**** Simulation run 25, type=compact(FoundSubsetLessThanMaxCompactSize). 2 Input Files, 255mb total:"
    - "L0                                                                                                                 "
    - "L0.37[101,101] 17ns 15mb |-----------------------------------------L0.37------------------------------------------|"
    - "L1                                                                                                                 "
    - "L1.55[101,101] 16ns 240mb|-----------------------------------------L1.55------------------------------------------|"
    - "**** 1 Output Files (parquet_file_id not yet assigned), 255mb total:"
    - "L1, all files 255mb                                                                                                "
    - "L1.?[101,101] 17ns       |------------------------------------------L1.?------------------------------------------|"
    - "Committing partition 1:"
    - "  Soft Deleting 2 files: L0.37, L1.55"
    - "  Creating 1 files"
    - "**** Simulation run 26, type=compact(FoundSubsetLessThanMaxCompactSize). 2 Input Files, 270mb total:"
    - "L0                                                                                                                 "
    - "L0.38[100,100] 18ns 15mb |-----------------------------------------L0.38------------------------------------------|"
    - "L1                                                                                                                 "
    - "L1.56[100,100] 17ns 255mb|-----------------------------------------L1.56------------------------------------------|"
    - "**** 1 Output Files (parquet_file_id not yet assigned), 270mb total:"
    - "L1, all files 270mb                                                                                                "
    - "L1.?[100,100] 18ns       |------------------------------------------L1.?------------------------------------------|"
    - "Committing partition 1:"
    - "  Soft Deleting 2 files: L0.38, L1.56"
    - "  Creating 1 files"
    - "**** Simulation run 27, type=compact(FoundSubsetLessThanMaxCompactSize). 2 Input Files, 270mb total:"
    - "L0                                                                                                                 "
    - "L0.39[101,101] 18ns 15mb |-----------------------------------------L0.39------------------------------------------|"
    - "L1                                                                                                                 "
    - "L1.57[101,101] 17ns 255mb|-----------------------------------------L1.57------------------------------------------|"
    - "**** 1 Output Files (parquet_file_id not yet assigned), 270mb total:"
    - "L1, all files 270mb                                                                                                "
    - "L1.?[101,101] 18ns       |------------------------------------------L1.?------------------------------------------|"
    - "Committing partition 1:"
    - "  Soft Deleting 2 files: L0.39, L1.57"
    - "  Creating 1 files"
    - "**** Simulation run 28, type=compact(FoundSubsetLessThanMaxCompactSize). 2 Input Files, 285mb total:"
    - "L0                                                                                                                 "
    - "L0.40[100,100] 19ns 15mb |-----------------------------------------L0.40------------------------------------------|"
    - "L1                                                                                                                 "
    - "L1.58[100,100] 18ns 270mb|-----------------------------------------L1.58------------------------------------------|"
    - "**** 1 Output Files (parquet_file_id not yet assigned), 285mb total:"
    - "L1, all files 285mb                                                                                                "
    - "L1.?[100,100] 19ns       |------------------------------------------L1.?------------------------------------------|"
    - "Committing partition 1:"
    - "  Soft Deleting 2 files: L0.40, L1.58"
    - "  Creating 1 files"
    - "**** Simulation run 29, type=compact(FoundSubsetLessThanMaxCompactSize). 2 Input Files, 285mb total:"
    - "L0                                                                                                                 "
    - "L0.41[101,101] 19ns 15mb |-----------------------------------------L0.41------------------------------------------|"
    - "L1                                                                                                                 "
    - "L1.59[101,101] 18ns 270mb|-----------------------------------------L1.59------------------------------------------|"
    - "**** 1 Output Files (parquet_file_id not yet assigned), 285mb total:"
    - "L1, all files 285mb                                                                                                "
    - "L1.?[101,101] 19ns       |------------------------------------------L1.?------------------------------------------|"
    - "Committing partition 1:"
    - "  Soft Deleting 2 files: L0.41, L1.59"
    - "  Creating 1 files"
    - "**** Simulation run 30, type=compact(FoundSubsetLessThanMaxCompactSize). 2 Input Files, 300mb total:"
    - "L0                                                                                                                 "
    - "L0.42[100,100] 20ns 15mb |-----------------------------------------L0.42------------------------------------------|"
    - "L1                                                                                                                 "
    - "L1.60[100,100] 19ns 285mb|-----------------------------------------L1.60------------------------------------------|"
    - "**** 1 Output Files (parquet_file_id not yet assigned), 300mb total:"
    - "L1, all files 300mb                                                                                                "
    - "L1.?[100,100] 20ns       |------------------------------------------L1.?------------------------------------------|"
    - "Committing partition 1:"
    - "  Soft Deleting 2 files: L0.42, L1.60"
    - "  Creating 1 files"
    - "**** Simulation run 31, type=compact(TotalSizeLessThanMaxCompactSize). 2 Input Files, 300mb total:"
    - "L0                                                                                                                 "
    - "L0.43[101,101] 20ns 15mb |-----------------------------------------L0.43------------------------------------------|"
    - "L1                                                                                                                 "
    - "L1.61[101,101] 19ns 285mb|-----------------------------------------L1.61------------------------------------------|"
    - "**** 1 Output Files (parquet_file_id not yet assigned), 300mb total:"
    - "L1, all files 300mb                                                                                                "
    - "L1.?[101,101] 20ns       |------------------------------------------L1.?------------------------------------------|"
    - "Committing partition 1:"
    - "  Soft Deleting 2 files: L0.43, L1.61"
    - "  Creating 1 files"
    - "Committing partition 1:"
    - "  Upgrading 2 files level to CompactionLevel::L2: L1.62, L1.63"
    - "**** Final Output Files (5.42gb written)"
    - "L2, all files 300mb                                                                                                "
    - "L2.62[100,100] 20ns      |L2.62|                                                                                   "
    - "L2.63[101,101] 20ns                                                                                                |L2.63|"
    - "WARNING: file L2.62[100,100] 20ns 300mb exceeds soft limit 100mb by more than 50%"
    - "WARNING: file L2.63[101,101] 20ns 300mb exceeds soft limit 100mb by more than 50%"
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
