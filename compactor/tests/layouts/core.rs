//! layout tests for the "core" design scenarios for compactor
//!
//! See [crate::layout] module for detailed documentation

use std::sync::Arc;

use data_types::CompactionLevel;
use iox_time::{MockProvider, Time, TimeProvider};

use crate::layouts::{
    all_overlapping_l0_files, layout_setup_builder, parquet_builder, run_layout_scenario, ONE_MB,
};

#[tokio::test]
#[should_panic(expected = "Found overlapping files at L1/L2 target level!")]
async fn test_panic_l1_overlap() {
    test_helpers::maybe_start_logging();
    let setup = layout_setup_builder().await.build().await;

    // This test verifies the simulator, rather than real code. Input files that violate
    // an invariant are given to the simulator, and this test requires that the simulator
    // panic. The invariant violated in this test is:
    //    L1 files, that overlap slightly (the max of the first matches the min of the next)
    for i in 0..2 {
        setup
            .partition
            .create_parquet_file(
                parquet_builder()
                    .with_min_time(10 + i * 30)
                    .with_max_time(10 + (i + 1) * 30)
                    .with_compaction_level(CompactionLevel::FileNonOverlapped),
            )
            .await;
    }

    run_layout_scenario(&setup).await;
}

#[tokio::test]
#[should_panic(expected = "Found overlapping files at L1/L2 target level!")]
async fn test_panic_l2_overlap() {
    test_helpers::maybe_start_logging();

    let setup = layout_setup_builder().await.build().await;

    // This test verifies the simulator, rather than real code. Input files that violate
    // an invariant are given to the simulator, and this test requires that the simulator
    // panic. The invariant violated in this test is:
    //    L2 files, that overlap slightly (the max of the first matches the min of the next)
    for i in 0..2 {
        setup
            .partition
            .create_parquet_file(
                parquet_builder()
                    .with_min_time(10 + i * 30)
                    .with_max_time(10 + (i + 1) * 30)
                    .with_compaction_level(CompactionLevel::Final),
            )
            .await;
    }

    run_layout_scenario(&setup).await;
}

#[tokio::test]
async fn all_overlapping_l0() {
    test_helpers::maybe_start_logging();

    let setup = layout_setup_builder().await.build().await;
    let setup = all_overlapping_l0_files(setup).await;

    insta::assert_yaml_snapshot!(
        run_layout_scenario(&setup).await,
        @r###"
    ---
    - "**** Input Files "
    - "L0, all files 9mb                                                                                                  "
    - "L0.1[100,200000] 1ns     |-----------------------------------------L0.1------------------------------------------| "
    - "L0.2[100,200000] 2ns     |-----------------------------------------L0.2------------------------------------------| "
    - "L0.3[100,200000] 3ns     |-----------------------------------------L0.3------------------------------------------| "
    - "L0.4[100,200000] 4ns     |-----------------------------------------L0.4------------------------------------------| "
    - "L0.5[100,200000] 5ns     |-----------------------------------------L0.5------------------------------------------| "
    - "L0.6[100,200000] 6ns     |-----------------------------------------L0.6------------------------------------------| "
    - "L0.7[100,200000] 7ns     |-----------------------------------------L0.7------------------------------------------| "
    - "L0.8[100,200000] 8ns     |-----------------------------------------L0.8------------------------------------------| "
    - "L0.9[100,200000] 9ns     |-----------------------------------------L0.9------------------------------------------| "
    - "L0.10[100,200000] 10ns   |-----------------------------------------L0.10-----------------------------------------| "
    - "**** Simulation run 0, type=split(CompactAndSplitOutput(TotalSizeLessThanMaxCompactSize))(split_times=[160020]). 10 Input Files, 90mb total:"
    - "L0, all files 9mb                                                                                                  "
    - "L0.10[100,200000] 10ns   |-----------------------------------------L0.10-----------------------------------------| "
    - "L0.9[100,200000] 9ns     |-----------------------------------------L0.9------------------------------------------| "
    - "L0.8[100,200000] 8ns     |-----------------------------------------L0.8------------------------------------------| "
    - "L0.7[100,200000] 7ns     |-----------------------------------------L0.7------------------------------------------| "
    - "L0.6[100,200000] 6ns     |-----------------------------------------L0.6------------------------------------------| "
    - "L0.5[100,200000] 5ns     |-----------------------------------------L0.5------------------------------------------| "
    - "L0.4[100,200000] 4ns     |-----------------------------------------L0.4------------------------------------------| "
    - "L0.3[100,200000] 3ns     |-----------------------------------------L0.3------------------------------------------| "
    - "L0.2[100,200000] 2ns     |-----------------------------------------L0.2------------------------------------------| "
    - "L0.1[100,200000] 1ns     |-----------------------------------------L0.1------------------------------------------| "
    - "**** 2 Output Files (parquet_file_id not yet assigned), 90mb total:"
    - "L1                                                                                                                 "
    - "L1.?[100,160020] 10ns 72mb|---------------------------------L1.?---------------------------------|                  "
    - "L1.?[160021,200000] 10ns 18mb                                                                        |-----L1.?------| "
    - "Committing partition 1:"
    - "  Soft Deleting 10 files: L0.1, L0.2, L0.3, L0.4, L0.5, L0.6, L0.7, L0.8, L0.9, L0.10"
    - "  Creating 2 files"
    - "**** Final Output Files (90mb written)"
    - "L1                                                                                                                 "
    - "L1.11[100,160020] 10ns 72mb|--------------------------------L1.11---------------------------------|                  "
    - "L1.12[160021,200000] 10ns 18mb                                                                        |-----L1.12-----| "
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
                    .with_max_l0_created_at(Time::from_timestamp_nanos(i))
                    .with_file_size_bytes(10 * ONE_MB),
            )
            .await;
    }

    insta::assert_yaml_snapshot!(
        run_layout_scenario(&setup).await,
        @r###"
    ---
    - "**** Input Files "
    - "L0, all files 10mb                                                                                                 "
    - "L0.1[0,1] 0ns            |L0.1|                                                                                    "
    - "L0.2[100,101] 1ns                 |L0.2|                                                                           "
    - "L0.3[200,201] 2ns                           |L0.3|                                                                 "
    - "L0.4[300,301] 3ns                                     |L0.4|                                                       "
    - "L0.5[400,401] 4ns                                               |L0.5|                                             "
    - "L0.6[500,501] 5ns                                                         |L0.6|                                   "
    - "L0.7[600,601] 6ns                                                                   |L0.7|                         "
    - "L0.8[700,701] 7ns                                                                             |L0.8|               "
    - "L0.9[800,801] 8ns                                                                                       |L0.9|     "
    - "L0.10[900,901] 9ns                                                                                                |L0.10|"
    - "**** Simulation run 0, type=split(CompactAndSplitOutput(TotalSizeLessThanMaxCompactSize))(split_times=[720]). 10 Input Files, 100mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.10[900,901] 9ns                                                                                                |L0.10|"
    - "L0.9[800,801] 8ns                                                                                       |L0.9|     "
    - "L0.8[700,701] 7ns                                                                             |L0.8|               "
    - "L0.7[600,601] 6ns                                                                   |L0.7|                         "
    - "L0.6[500,501] 5ns                                                         |L0.6|                                   "
    - "L0.5[400,401] 4ns                                               |L0.5|                                             "
    - "L0.4[300,301] 3ns                                     |L0.4|                                                       "
    - "L0.3[200,201] 2ns                           |L0.3|                                                                 "
    - "L0.2[100,101] 1ns                 |L0.2|                                                                           "
    - "L0.1[0,1] 0ns            |L0.1|                                                                                    "
    - "**** 2 Output Files (parquet_file_id not yet assigned), 100mb total:"
    - "L1                                                                                                                 "
    - "L1.?[0,720] 9ns 80mb     |--------------------------------L1.?---------------------------------|                   "
    - "L1.?[721,901] 9ns 20mb                                                                           |-----L1.?------| "
    - "Committing partition 1:"
    - "  Soft Deleting 10 files: L0.1, L0.2, L0.3, L0.4, L0.5, L0.6, L0.7, L0.8, L0.9, L0.10"
    - "  Creating 2 files"
    - "**** Simulation run 1, type=split(CompactAndSplitOutput(TotalSizeLessThanMaxCompactSize))(split_times=[720]). 2 Input Files, 100mb total:"
    - "L1                                                                                                                 "
    - "L1.12[721,901] 9ns 20mb                                                                          |-----L1.12-----| "
    - "L1.11[0,720] 9ns 80mb    |--------------------------------L1.11--------------------------------|                   "
    - "**** 2 Output Files (parquet_file_id not yet assigned), 100mb total:"
    - "L2                                                                                                                 "
    - "L2.?[0,720] 9ns 80mb     |--------------------------------L2.?---------------------------------|                   "
    - "L2.?[721,901] 9ns 20mb                                                                           |-----L2.?------| "
    - "Committing partition 1:"
    - "  Soft Deleting 2 files: L1.11, L1.12"
    - "  Creating 2 files"
    - "**** Final Output Files (200mb written)"
    - "L2                                                                                                                 "
    - "L2.13[0,720] 9ns 80mb    |--------------------------------L2.13--------------------------------|                   "
    - "L2.14[721,901] 9ns 20mb                                                                          |-----L2.14-----| "
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
                    .with_max_time(99 + i * 50)
                    .with_compaction_level(CompactionLevel::FileNonOverlapped)
                    // L1 files with smaller max_l0_created_at to indicate they inlcude data created before the L0 files
                    .with_max_l0_created_at(Time::from_timestamp_nanos(i + 1))
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
                    // specify L0 explicitly
                    .with_compaction_level(CompactionLevel::Initial)
                    // L0 files with larger max_l0_created_at to indicate they are created after data comapcted in the L1 files
                    .with_max_l0_created_at(Time::from_timestamp_nanos(10 + i + 1))
                    .with_file_size_bytes(five_kb),
            )
            .await;
    }

    insta::assert_yaml_snapshot!(
        run_layout_scenario(&setup).await,
        @r###"
    ---
    - "**** Input Files "
    - "L0                                                                                                                 "
    - "L0.3[140,190] 11ns 5kb                                  |-----L0.3------|                                          "
    - "L0.4[170,220] 12ns 5kb                                            |-----L0.4------|                                "
    - "L0.5[200,250] 13ns 5kb                                                      |-----L0.5------|                      "
    - "L0.6[230,280] 14ns 5kb                                                                 |-----L0.6------|           "
    - "L0.7[260,310] 15ns 5kb                                                                           |-----L0.7------| "
    - "L1                                                                                                                 "
    - "L1.1[50,99] 1ns 10mb     |-----L1.1-----|                                                                          "
    - "L1.2[100,149] 2ns 10mb                    |-----L1.2-----|                                                         "
    - "**** Simulation run 0, type=compact(TotalSizeLessThanMaxCompactSize). 6 Input Files, 10mb total:"
    - "L0                                                                                                                 "
    - "L0.7[260,310] 15ns 5kb                                                                       |-------L0.7--------| "
    - "L0.6[230,280] 14ns 5kb                                                          |-------L0.6--------|              "
    - "L0.5[200,250] 13ns 5kb                                             |-------L0.5--------|                           "
    - "L0.4[170,220] 12ns 5kb                                |-------L0.4--------|                                        "
    - "L0.3[140,190] 11ns 5kb                    |-------L0.3--------|                                                    "
    - "L1                                                                                                                 "
    - "L1.2[100,149] 2ns 10mb   |-------L1.2--------|                                                                     "
    - "**** 1 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L1, all files 10mb                                                                                                 "
    - "L1.?[100,310] 15ns       |------------------------------------------L1.?------------------------------------------|"
    - "Committing partition 1:"
    - "  Soft Deleting 6 files: L1.2, L0.3, L0.4, L0.5, L0.6, L0.7"
    - "  Creating 1 files"
    - "**** Final Output Files (10mb written)"
    - "L1                                                                                                                 "
    - "L1.1[50,99] 1ns 10mb     |-----L1.1-----|                                                                          "
    - "L1.8[100,310] 15ns 10mb                   |---------------------------------L1.8---------------------------------| "
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
                    .with_max_time(99 + i * 50)
                    .with_compaction_level(CompactionLevel::FileNonOverlapped)
                    // L1 files with smaller max_l0_created_at to indicate they inlcude data created before the L0 files
                    .with_max_l0_created_at(Time::from_timestamp_nanos(i + 1))
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
                    // specify L0 explicitly
                    .with_compaction_level(CompactionLevel::Initial)
                    // L0 files with larger max_l0_created_at to indicate they are created after data compacted in the L1 files
                    .with_max_l0_created_at(Time::from_timestamp_nanos(10 + i + 1))
                    .with_file_size_bytes(five_kb),
            )
            .await;
    }

    insta::assert_yaml_snapshot!(
        run_layout_scenario(&setup).await,
        @r###"
    ---
    - "**** Input Files "
    - "L0                                                                                                                 "
    - "L0.3[300,350] 11ns 5kb                                                |-L0.3--|                                    "
    - "L0.4[350,400] 12ns 5kb                                                         |-L0.4--|                           "
    - "L0.5[400,450] 13ns 5kb                                                                  |-L0.5--|                  "
    - "L0.6[450,500] 14ns 5kb                                                                           |-L0.6--|         "
    - "L0.7[500,550] 15ns 5kb                                                                                    |-L0.7--|"
    - "L1                                                                                                                 "
    - "L1.1[50,99] 1ns 10mb     |-L1.1-|                                                                                  "
    - "L1.2[100,149] 2ns 10mb            |-L1.2-|                                                                         "
    - "**** Simulation run 0, type=compact(TotalSizeLessThanMaxCompactSize). 5 Input Files, 25kb total:"
    - "L0, all files 5kb                                                                                                  "
    - "L0.7[500,550] 15ns                                                                               |------L0.7------|"
    - "L0.6[450,500] 14ns                                                             |------L0.6------|                  "
    - "L0.5[400,450] 13ns                                           |------L0.5------|                                    "
    - "L0.4[350,400] 12ns                         |------L0.4------|                                                      "
    - "L0.3[300,350] 11ns       |------L0.3------|                                                                        "
    - "**** 1 Output Files (parquet_file_id not yet assigned), 25kb total:"
    - "L1, all files 25kb                                                                                                 "
    - "L1.?[300,550] 15ns       |------------------------------------------L1.?------------------------------------------|"
    - "Committing partition 1:"
    - "  Soft Deleting 5 files: L0.3, L0.4, L0.5, L0.6, L0.7"
    - "  Creating 1 files"
    - "**** Final Output Files (25kb written)"
    - "L1                                                                                                                 "
    - "L1.1[50,99] 1ns 10mb     |-L1.1-|                                                                                  "
    - "L1.2[100,149] 2ns 10mb            |-L1.2-|                                                                         "
    - "L1.8[300,550] 15ns 25kb                                               |-------------------L1.8--------------------|"
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
                    .with_max_time(99 + i * 50)
                    .with_compaction_level(CompactionLevel::FileNonOverlapped)
                    // L1 files with smaller max_l0_created_at to indicate they inlcude data created before the L0 files
                    .with_max_l0_created_at(Time::from_timestamp_nanos(i + 1))
                    .with_file_size_bytes(sz * ONE_MB),
            )
            .await;
    }
    // L0 overlapping
    for i in 0..3 {
        setup
            .partition
            .create_parquet_file(
                parquet_builder()
                    .with_min_time(300 + i * 50)
                    .with_max_time(350 + i * 50)
                    // specify L0 explicitly
                    .with_compaction_level(CompactionLevel::Initial)
                    // L0 files with larger max_l0_created_at to indicate they are created after data compacted in the L1 files
                    .with_max_l0_created_at(Time::from_timestamp_nanos(10 + i + 1))
                    .with_file_size_bytes(5 * ONE_MB),
            )
            .await;
    }

    insta::assert_yaml_snapshot!(
        run_layout_scenario(&setup).await,
        @r###"
    ---
    - "**** Input Files "
    - "L0                                                                                                                 "
    - "L0.5[300,350] 11ns 5mb                                                           |--L0.5---|                       "
    - "L0.6[350,400] 12ns 5mb                                                                      |--L0.6---|            "
    - "L0.7[400,450] 13ns 5mb                                                                                 |--L0.7---| "
    - "L1                                                                                                                 "
    - "L1.1[50,99] 1ns 20mb     |--L1.1---|                                                                               "
    - "L1.2[100,149] 2ns 50mb              |--L1.2---|                                                                    "
    - "L1.3[150,199] 3ns 20mb                         |--L1.3---|                                                         "
    - "L1.4[200,249] 4ns 3mb                                     |--L1.4---|                                              "
    - "**** Simulation run 0, type=compact(TotalSizeLessThanMaxCompactSize). 4 Input Files, 18mb total:"
    - "L0                                                                                                                 "
    - "L0.7[400,450] 13ns 5mb                                                                           |------L0.7------|"
    - "L0.6[350,400] 12ns 5mb                                                         |------L0.6------|                  "
    - "L0.5[300,350] 11ns 5mb                                       |------L0.5------|                                    "
    - "L1                                                                                                                 "
    - "L1.4[200,249] 4ns 3mb    |-----L1.4------|                                                                         "
    - "**** 1 Output Files (parquet_file_id not yet assigned), 18mb total:"
    - "L1, all files 18mb                                                                                                 "
    - "L1.?[200,450] 13ns       |------------------------------------------L1.?------------------------------------------|"
    - "Committing partition 1:"
    - "  Soft Deleting 4 files: L1.4, L0.5, L0.6, L0.7"
    - "  Creating 1 files"
    - "**** Simulation run 1, type=split(CompactAndSplitOutput(TotalSizeLessThanMaxCompactSize))(split_times=[370]). 4 Input Files, 108mb total:"
    - "L1                                                                                                                 "
    - "L1.2[100,149] 2ns 50mb              |--L1.2---|                                                                    "
    - "L1.1[50,99] 1ns 20mb     |--L1.1---|                                                                               "
    - "L1.3[150,199] 3ns 20mb                         |--L1.3---|                                                         "
    - "L1.8[200,450] 13ns 18mb                                   |-------------------------L1.8-------------------------| "
    - "**** 2 Output Files (parquet_file_id not yet assigned), 108mb total:"
    - "L2                                                                                                                 "
    - "L2.?[50,370] 13ns 86mb   |---------------------------------L2.?---------------------------------|                  "
    - "L2.?[371,450] 13ns 22mb                                                                          |-----L2.?------| "
    - "Committing partition 1:"
    - "  Soft Deleting 4 files: L1.1, L1.2, L1.3, L1.8"
    - "  Creating 2 files"
    - "**** Final Output Files (126mb written)"
    - "L2                                                                                                                 "
    - "L2.9[50,370] 13ns 86mb   |---------------------------------L2.9---------------------------------|                  "
    - "L2.10[371,450] 13ns 22mb                                                                         |-----L2.10-----| "
    "###
    );
}

#[tokio::test]
async fn l1_too_much_with_non_overlapping_l0() {
    test_helpers::maybe_start_logging();

    let setup = layout_setup_builder().await.build().await;

    let time_provider = Arc::new(MockProvider::new(Time::from_timestamp(0, 0).unwrap()));

    // If we wait until we have 10 L1 files each is not large
    // enough to upgrade, the total size will be > 256MB and we will
    // skip the partition
    //
    // L1: 90MB, 80MB, 70MB, ..., 70MB
    // L0: ..

    let mut num_l1_files = 0;
    for (i, sz) in [90, 80, 70, 70, 70, 70, 70, 70, 70, 70].iter().enumerate() {
        let i = i as i64;
        setup
            .partition
            .create_parquet_file(
                parquet_builder()
                    .with_min_time(50 + i * 50)
                    .with_max_time(99 + i * 50)
                    .with_compaction_level(CompactionLevel::FileNonOverlapped)
                    .with_file_size_bytes(sz * ONE_MB)
                    .with_max_l0_created_at(time_provider.minutes_into_future(i as u64)),
            )
            .await;
        num_l1_files += 1;
    }
    // note these overlap with each other, but not the L1 files
    for i in 0..3 {
        setup
            .partition
            .create_parquet_file(
                parquet_builder()
                    .with_min_time(600)
                    .with_max_time(649)
                    .with_file_size_bytes(5 * ONE_MB)
                    .with_max_l0_created_at(
                        time_provider.minutes_into_future(num_l1_files + i + 1),
                    ),
            )
            .await;
    }

    insta::assert_yaml_snapshot!(
        run_layout_scenario(&setup).await,
        @r###"
    ---
    - "**** Input Files "
    - "L0                                                                                                                 "
    - "L0.11[600,649] 660s 5mb                                                                                    |L0.11| "
    - "L0.12[600,649] 720s 5mb                                                                                    |L0.12| "
    - "L0.13[600,649] 780s 5mb                                                                                    |L0.13| "
    - "L1                                                                                                                 "
    - "L1.1[50,99] 0ns 90mb     |L1.1-|                                                                                   "
    - "L1.2[100,149] 60s 80mb          |L1.2-|                                                                            "
    - "L1.3[150,199] 120s 70mb                 |L1.3-|                                                                    "
    - "L1.4[200,249] 180s 70mb                        |L1.4-|                                                             "
    - "L1.5[250,299] 240s 70mb                                |L1.5-|                                                     "
    - "L1.6[300,349] 300s 70mb                                       |L1.6-|                                              "
    - "L1.7[350,399] 360s 70mb                                               |L1.7-|                                      "
    - "L1.8[400,449] 420s 70mb                                                      |L1.8-|                               "
    - "L1.9[450,499] 480s 70mb                                                              |L1.9-|                       "
    - "L1.10[500,549] 540s 70mb                                                                    |L1.10|                "
    - "**** Simulation run 0, type=compact(TotalSizeLessThanMaxCompactSize). 3 Input Files, 15mb total:"
    - "L0, all files 5mb                                                                                                  "
    - "L0.13[600,649] 780s      |-----------------------------------------L0.13------------------------------------------|"
    - "L0.12[600,649] 720s      |-----------------------------------------L0.12------------------------------------------|"
    - "L0.11[600,649] 660s      |-----------------------------------------L0.11------------------------------------------|"
    - "**** 1 Output Files (parquet_file_id not yet assigned), 15mb total:"
    - "L1, all files 15mb                                                                                                 "
    - "L1.?[600,649] 780s       |------------------------------------------L1.?------------------------------------------|"
    - "Committing partition 1:"
    - "  Soft Deleting 3 files: L0.11, L0.12, L0.13"
    - "  Creating 1 files"
    - "**** Simulation run 1, type=split(CompactAndSplitOutput(FoundSubsetLessThanMaxCompactSize))(split_times=[113, 176]). 3 Input Files, 240mb total:"
    - "L1                                                                                                                 "
    - "L1.1[50,99] 0ns 90mb     |-----------L1.1------------|                                                             "
    - "L1.2[100,149] 60s 80mb                                 |-----------L1.2------------|                               "
    - "L1.3[150,199] 120s 70mb                                                              |-----------L1.3------------| "
    - "**** 3 Output Files (parquet_file_id not yet assigned), 240mb total:"
    - "L2                                                                                                                 "
    - "L2.?[50,113] 120s 101mb  |----------------L2.?----------------|                                                    "
    - "L2.?[114,176] 120s 100mb                                       |---------------L2.?----------------|               "
    - "L2.?[177,199] 120s 39mb                                                                              |---L2.?----| "
    - "Committing partition 1:"
    - "  Soft Deleting 3 files: L1.1, L1.2, L1.3"
    - "  Creating 3 files"
    - "**** Simulation run 2, type=split(CompactAndSplitOutput(FoundSubsetLessThanMaxCompactSize))(split_times=[272, 344]). 4 Input Files, 280mb total:"
    - "L1, all files 70mb                                                                                                 "
    - "L1.4[200,249] 180s       |--------L1.4--------|                                                                    "
    - "L1.5[250,299] 240s                             |--------L1.5--------|                                              "
    - "L1.6[300,349] 300s                                                    |--------L1.6--------|                       "
    - "L1.7[350,399] 360s                                                                          |--------L1.7--------| "
    - "**** 3 Output Files (parquet_file_id not yet assigned), 280mb total:"
    - "L2                                                                                                                 "
    - "L2.?[200,272] 360s 101mb |-------------L2.?-------------|                                                          "
    - "L2.?[273,344] 360s 100mb                                  |-------------L2.?-------------|                         "
    - "L2.?[345,399] 360s 79mb                                                                   |---------L2.?---------| "
    - "Committing partition 1:"
    - "  Soft Deleting 4 files: L1.4, L1.5, L1.6, L1.7"
    - "  Creating 3 files"
    - "**** Simulation run 3, type=split(CompactAndSplitOutput(TotalSizeLessThanMaxCompactSize))(split_times=[511, 622]). 4 Input Files, 225mb total:"
    - "L1                                                                                                                 "
    - "L1.14[600,649] 780s 15mb                                                                         |-----L1.14-----| "
    - "L1.10[500,549] 540s 70mb                                     |-----L1.10-----|                                     "
    - "L1.9[450,499] 480s 70mb                    |-----L1.9------|                                                       "
    - "L1.8[400,449] 420s 70mb  |-----L1.8------|                                                                         "
    - "**** 3 Output Files (parquet_file_id not yet assigned), 225mb total:"
    - "L2                                                                                                                 "
    - "L2.?[400,511] 780s 100mb |-----------------L2.?-----------------|                                                  "
    - "L2.?[512,622] 780s 99mb                                          |----------------L2.?-----------------|           "
    - "L2.?[623,649] 780s 25mb                                                                                  |-L2.?--| "
    - "Committing partition 1:"
    - "  Soft Deleting 4 files: L1.8, L1.9, L1.10, L1.14"
    - "  Creating 3 files"
    - "**** Final Output Files (760mb written)"
    - "L2                                                                                                                 "
    - "L2.15[50,113] 120s 101mb |-L2.15-|                                                                                 "
    - "L2.16[114,176] 120s 100mb         |-L2.16-|                                                                        "
    - "L2.17[177,199] 120s 39mb                    |L2.17|                                                                "
    - "L2.18[200,272] 360s 101mb                      |-L2.18--|                                                          "
    - "L2.19[273,344] 360s 100mb                                 |-L2.19--|                                               "
    - "L2.20[345,399] 360s 79mb                                             |L2.20-|                                      "
    - "L2.21[400,511] 780s 100mb                                                    |----L2.21-----|                      "
    - "L2.22[512,622] 780s 99mb                                                                      |----L2.22-----|     "
    - "L2.23[623,649] 780s 25mb                                                                                       |L2.23|"
    "###
    );
}

#[tokio::test]
/// compacts L1 files in second round if their number of files >= min_num_l1_files_to_compact
async fn many_l1_with_non_overlapping_l0() {
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
                    .with_max_time(99 + i * 50)
                    .with_compaction_level(CompactionLevel::FileNonOverlapped)
                    // L1 files with smaller max_l0_created_at to indicate they inlcude data created before the L0 files
                    .with_max_l0_created_at(Time::from_timestamp_nanos(i + 1))
                    .with_file_size_bytes(sz * ONE_MB),
            )
            .await;
    }
    // note these overlap with each other, but not the L1 files
    for i in 0..3 {
        setup
            .partition
            .create_parquet_file(
                parquet_builder()
                    .with_min_time(600)
                    .with_max_time(650)
                    // specify L0 explicitly
                    .with_compaction_level(CompactionLevel::Initial)
                    // L0 files with larger max_l0_created_at to indicate they are created after data compacted in the L1 files
                    .with_max_l0_created_at(Time::from_timestamp_nanos(20 + i + 1))
                    .with_file_size_bytes(5 * ONE_MB),
            )
            .await;
    }

    insta::assert_yaml_snapshot!(
        run_layout_scenario(&setup).await,
        @r###"
    ---
    - "**** Input Files "
    - "L0                                                                                                                 "
    - "L0.11[600,650] 21ns 5mb                                                                                    |L0.11| "
    - "L0.12[600,650] 22ns 5mb                                                                                    |L0.12| "
    - "L0.13[600,650] 23ns 5mb                                                                                    |L0.13| "
    - "L1                                                                                                                 "
    - "L1.1[50,99] 1ns 9mb      |L1.1-|                                                                                   "
    - "L1.2[100,149] 2ns 8mb           |L1.2-|                                                                            "
    - "L1.3[150,199] 3ns 7mb                   |L1.3-|                                                                    "
    - "L1.4[200,249] 4ns 7mb                          |L1.4-|                                                             "
    - "L1.5[250,299] 5ns 7mb                                  |L1.5-|                                                     "
    - "L1.6[300,349] 6ns 7mb                                         |L1.6-|                                              "
    - "L1.7[350,399] 7ns 7mb                                                 |L1.7-|                                      "
    - "L1.8[400,449] 8ns 7mb                                                        |L1.8-|                               "
    - "L1.9[450,499] 9ns 7mb                                                                |L1.9-|                       "
    - "L1.10[500,549] 10ns 7mb                                                                     |L1.10|                "
    - "**** Simulation run 0, type=compact(TotalSizeLessThanMaxCompactSize). 3 Input Files, 15mb total:"
    - "L0, all files 5mb                                                                                                  "
    - "L0.13[600,650] 23ns      |-----------------------------------------L0.13------------------------------------------|"
    - "L0.12[600,650] 22ns      |-----------------------------------------L0.12------------------------------------------|"
    - "L0.11[600,650] 21ns      |-----------------------------------------L0.11------------------------------------------|"
    - "**** 1 Output Files (parquet_file_id not yet assigned), 15mb total:"
    - "L1, all files 15mb                                                                                                 "
    - "L1.?[600,650] 23ns       |------------------------------------------L1.?------------------------------------------|"
    - "Committing partition 1:"
    - "  Soft Deleting 3 files: L0.11, L0.12, L0.13"
    - "  Creating 1 files"
    - "**** Simulation run 1, type=split(CompactAndSplitOutput(TotalSizeLessThanMaxCompactSize))(split_times=[530]). 11 Input Files, 88mb total:"
    - "L1                                                                                                                 "
    - "L1.9[450,499] 9ns 7mb                                                                |L1.9-|                       "
    - "L1.8[400,449] 8ns 7mb                                                        |L1.8-|                               "
    - "L1.7[350,399] 7ns 7mb                                                 |L1.7-|                                      "
    - "L1.6[300,349] 6ns 7mb                                         |L1.6-|                                              "
    - "L1.5[250,299] 5ns 7mb                                  |L1.5-|                                                     "
    - "L1.4[200,249] 4ns 7mb                          |L1.4-|                                                             "
    - "L1.3[150,199] 3ns 7mb                   |L1.3-|                                                                    "
    - "L1.2[100,149] 2ns 8mb           |L1.2-|                                                                            "
    - "L1.1[50,99] 1ns 9mb      |L1.1-|                                                                                   "
    - "L1.10[500,549] 10ns 7mb                                                                     |L1.10|                "
    - "L1.14[600,650] 23ns 15mb                                                                                   |L1.14| "
    - "**** 2 Output Files (parquet_file_id not yet assigned), 88mb total:"
    - "L2                                                                                                                 "
    - "L2.?[50,530] 23ns 70mb   |---------------------------------L2.?---------------------------------|                  "
    - "L2.?[531,650] 23ns 18mb                                                                          |-----L2.?------| "
    - "Committing partition 1:"
    - "  Soft Deleting 11 files: L1.1, L1.2, L1.3, L1.4, L1.5, L1.6, L1.7, L1.8, L1.9, L1.10, L1.14"
    - "  Creating 2 files"
    - "**** Final Output Files (103mb written)"
    - "L2                                                                                                                 "
    - "L2.15[50,530] 23ns 70mb  |--------------------------------L2.15---------------------------------|                  "
    - "L2.16[531,650] 23ns 18mb                                                                         |-----L2.16-----| "
    "###
    );
}

#[tokio::test]
/// Compacts L1 files in second round if their total size > max_desired_file_size
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
                    .with_max_time(99 + i * 50)
                    .with_compaction_level(CompactionLevel::FileNonOverlapped)
                    // L1 files with smaller max_l0_created_at to indicate they inlcude data created before the L0 files
                    .with_max_l0_created_at(Time::from_timestamp_nanos(i + 1))
                    .with_file_size_bytes(sz * ONE_MB),
            )
            .await;
    }
    // note these overlap with each other, but not the L1 files
    for i in 0..3 {
        setup
            .partition
            .create_parquet_file(
                parquet_builder()
                    .with_min_time(600)
                    .with_max_time(650)
                    // specify L0 explicitly
                    .with_compaction_level(CompactionLevel::Initial)
                    // L0 files with larger max_l0_created_at to indicate they are created after data compacted in the L1 files
                    .with_max_l0_created_at(Time::from_timestamp_nanos(20 + i + 1))
                    .with_file_size_bytes(5 * ONE_MB),
            )
            .await;
    }

    insta::assert_yaml_snapshot!(
        run_layout_scenario(&setup).await,
        @r###"
    ---
    - "**** Input Files "
    - "L0                                                                                                                 "
    - "L0.3[600,650] 21ns 5mb                                                                                     |L0.3-| "
    - "L0.4[600,650] 22ns 5mb                                                                                     |L0.4-| "
    - "L0.5[600,650] 23ns 5mb                                                                                     |L0.5-| "
    - "L1                                                                                                                 "
    - "L1.1[50,99] 1ns 90mb     |L1.1-|                                                                                   "
    - "L1.2[100,149] 2ns 80mb          |L1.2-|                                                                            "
    - "**** Simulation run 0, type=compact(TotalSizeLessThanMaxCompactSize). 3 Input Files, 15mb total:"
    - "L0, all files 5mb                                                                                                  "
    - "L0.5[600,650] 23ns       |------------------------------------------L0.5------------------------------------------|"
    - "L0.4[600,650] 22ns       |------------------------------------------L0.4------------------------------------------|"
    - "L0.3[600,650] 21ns       |------------------------------------------L0.3------------------------------------------|"
    - "**** 1 Output Files (parquet_file_id not yet assigned), 15mb total:"
    - "L1, all files 15mb                                                                                                 "
    - "L1.?[600,650] 23ns       |------------------------------------------L1.?------------------------------------------|"
    - "Committing partition 1:"
    - "  Soft Deleting 3 files: L0.3, L0.4, L0.5"
    - "  Creating 1 files"
    - "**** Simulation run 1, type=split(CompactAndSplitOutput(TotalSizeLessThanMaxCompactSize))(split_times=[375]). 3 Input Files, 185mb total:"
    - "L1                                                                                                                 "
    - "L1.1[50,99] 1ns 90mb     |L1.1-|                                                                                   "
    - "L1.2[100,149] 2ns 80mb          |L1.2-|                                                                            "
    - "L1.6[600,650] 23ns 15mb                                                                                    |L1.6-| "
    - "**** 2 Output Files (parquet_file_id not yet assigned), 185mb total:"
    - "L2                                                                                                                 "
    - "L2.?[50,375] 23ns 100mb  |---------------------L2.?---------------------|                                          "
    - "L2.?[376,650] 23ns 85mb                                                  |-----------------L2.?------------------| "
    - "Committing partition 1:"
    - "  Soft Deleting 3 files: L1.1, L1.2, L1.6"
    - "  Creating 2 files"
    - "**** Final Output Files (200mb written)"
    - "L2                                                                                                                 "
    - "L2.7[50,375] 23ns 100mb  |---------------------L2.7---------------------|                                          "
    - "L2.8[376,650] 23ns 85mb                                                  |-----------------L2.8------------------| "
    "###
    );
}
