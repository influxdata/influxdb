//! layout tests for the "core" design scenarios for compactor
//!
//! See [crate::layout] module for detailed documentation

use std::sync::Arc;

use data_types::CompactionLevel;
use iox_time::{MockProvider, Time, TimeProvider};
use std::time::Duration;

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
    - "**** Simulation run 1, type=split(CompactAndSplitOutput(TotalSizeLessThanMaxCompactSize))(split_times=[865]). 1 Input Files, 20mb total:"
    - "L1, all files 20mb                                                                                                 "
    - "L1.12[721,901] 9ns       |-----------------------------------------L1.12------------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 20mb total:"
    - "L2                                                                                                                 "
    - "L2.?[721,865] 9ns 16mb   |---------------------------------L2.?---------------------------------|                  "
    - "L2.?[866,901] 9ns 4mb                                                                            |-----L2.?------| "
    - "Committing partition 1:"
    - "  Soft Deleting 1 files: L1.12"
    - "  Upgrading 1 files level to CompactionLevel::L2: L1.11"
    - "  Creating 2 files"
    - "**** Final Output Files (120mb written)"
    - "L2                                                                                                                 "
    - "L2.11[0,720] 9ns 80mb    |--------------------------------L2.11--------------------------------|                   "
    - "L2.13[721,865] 9ns 16mb                                                                          |---L2.13----|    "
    - "L2.14[866,901] 9ns 4mb                                                                                         |L2.14|"
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
    - "L1.1[50,99] 1ns 20mb     |--L1.1---|                                                                               "
    - "L1.3[150,199] 3ns 20mb                         |--L1.3---|                                                         "
    - "L1.8[200,450] 13ns 18mb                                   |-------------------------L1.8-------------------------| "
    - "L1.2[100,149] 2ns 50mb              |--L1.2---|                                                                    "
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
    // L1: 45MB, 40MB, 35MB, ..., 35MB
    // L0: ..

    let mut num_l1_files = 0;
    for (i, sz) in [45, 40, 35, 35, 35, 35, 35, 35, 35, 35].iter().enumerate() {
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
    - "L1.1[50,99] 0ns 45mb     |L1.1-|                                                                                   "
    - "L1.2[100,149] 60s 40mb          |L1.2-|                                                                            "
    - "L1.3[150,199] 120s 35mb                 |L1.3-|                                                                    "
    - "L1.4[200,249] 180s 35mb                        |L1.4-|                                                             "
    - "L1.5[250,299] 240s 35mb                                |L1.5-|                                                     "
    - "L1.6[300,349] 300s 35mb                                       |L1.6-|                                              "
    - "L1.7[350,399] 360s 35mb                                               |L1.7-|                                      "
    - "L1.8[400,449] 420s 35mb                                                      |L1.8-|                               "
    - "L1.9[450,499] 480s 35mb                                                              |L1.9-|                       "
    - "L1.10[500,549] 540s 35mb                                                                    |L1.10|                "
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
    - "**** Simulation run 1, type=split(CompactAndSplitOutput(FoundSubsetLessThanMaxCompactSize))(split_times=[186, 322]). 8 Input Files, 295mb total:"
    - "L1                                                                                                                 "
    - "L1.1[50,99] 0ns 45mb     |--L1.1---|                                                                               "
    - "L1.2[100,149] 60s 40mb              |--L1.2---|                                                                    "
    - "L1.3[150,199] 120s 35mb                        |--L1.3---|                                                         "
    - "L1.4[200,249] 180s 35mb                                   |--L1.4---|                                              "
    - "L1.5[250,299] 240s 35mb                                               |--L1.5---|                                  "
    - "L1.6[300,349] 300s 35mb                                                          |--L1.6---|                       "
    - "L1.7[350,399] 360s 35mb                                                                     |--L1.7---|            "
    - "L1.8[400,449] 420s 35mb                                                                                |--L1.8---| "
    - "**** 3 Output Files (parquet_file_id not yet assigned), 295mb total:"
    - "L2                                                                                                                 "
    - "L2.?[50,186] 420s 101mb  |------------L2.?------------|                                                            "
    - "L2.?[187,322] 420s 100mb                               |------------L2.?------------|                              "
    - "L2.?[323,449] 420s 94mb                                                               |-----------L2.?-----------| "
    - "Committing partition 1:"
    - "  Soft Deleting 8 files: L1.1, L1.2, L1.3, L1.4, L1.5, L1.6, L1.7, L1.8"
    - "  Creating 3 files"
    - "**** Final Output Files (310mb written)"
    - "L1                                                                                                                 "
    - "L1.9[450,499] 480s 35mb                                                              |L1.9-|                       "
    - "L1.10[500,549] 540s 35mb                                                                    |L1.10|                "
    - "L1.14[600,649] 780s 15mb                                                                                   |L1.14| "
    - "L2                                                                                                                 "
    - "L2.15[50,186] 420s 101mb |------L2.15-------|                                                                      "
    - "L2.16[187,322] 420s 100mb                    |------L2.16-------|                                                  "
    - "L2.17[323,449] 420s 94mb                                          |-----L2.17------|                               "
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
    // L1: 45MB, 40MB
    // L0: ..

    for (i, sz) in [45, 40].iter().enumerate() {
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
    - "L1.1[50,99] 1ns 45mb     |L1.1-|                                                                                   "
    - "L1.2[100,149] 2ns 40mb          |L1.2-|                                                                            "
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
    - "**** Simulation run 1, type=split(CompactAndSplitOutput(TotalSizeLessThanMaxCompactSize))(split_times=[530]). 3 Input Files, 100mb total:"
    - "L1                                                                                                                 "
    - "L1.1[50,99] 1ns 45mb     |L1.1-|                                                                                   "
    - "L1.2[100,149] 2ns 40mb          |L1.2-|                                                                            "
    - "L1.6[600,650] 23ns 15mb                                                                                    |L1.6-| "
    - "**** 2 Output Files (parquet_file_id not yet assigned), 100mb total:"
    - "L2                                                                                                                 "
    - "L2.?[50,530] 23ns 80mb   |---------------------------------L2.?---------------------------------|                  "
    - "L2.?[531,650] 23ns 20mb                                                                          |-----L2.?------| "
    - "Committing partition 1:"
    - "  Soft Deleting 3 files: L1.1, L1.2, L1.6"
    - "  Creating 2 files"
    - "**** Final Output Files (115mb written)"
    - "L2                                                                                                                 "
    - "L2.7[50,530] 23ns 80mb   |---------------------------------L2.7---------------------------------|                  "
    - "L2.8[531,650] 23ns 20mb                                                                          |-----L2.8------| "
    "###
    );
}

#[tokio::test]
async fn overlapping_out_of_order_l0() {
    test_helpers::maybe_start_logging();

    let max_files_per_plan = 20;
    let max_compact_size = 300 * ONE_MB;
    let max_file_size = max_compact_size / 3;

    let setup = layout_setup_builder()
        .await
        .with_max_num_files_per_plan(max_files_per_plan as usize)
        .with_max_desired_file_size_bytes(max_file_size)
        .with_partition_timeout(Duration::from_millis(10000))
        .with_suppress_run_output() // remove this to debug
        .build()
        .await;

    // create 10 chains of 10 files each, with gaps between each chain
    for i in 0..10 {
        for j in 0..10 {
            setup
                .partition
                .create_parquet_file(
                    parquet_builder()
                        .with_min_time(100 * i + j * 5)
                        .with_max_time(100 * i + (j + 1) * 5)
                        .with_max_l0_created_at(Time::from_timestamp_nanos(
                            100 * i + (j + 1) * 5 + 1,
                        ))
                        .with_file_size_bytes(max_file_size),
                )
                .await;
        }
    }

    // create 10 files to fit in the gaps between the above chains, with newer max_l0_created_at
    for i in 0..10 {
        setup
            .partition
            .create_parquet_file(
                parquet_builder()
                    .with_min_time(100 * i + 60)
                    .with_max_time(100 * (i + 1) - 25)
                    .with_max_l0_created_at(Time::from_timestamp_nanos(1000 + 100 * (i + 1)))
                    .with_file_size_bytes(max_file_size),
            )
            .await;
    }

    insta::assert_yaml_snapshot!(
        run_layout_scenario(&setup).await,
        @r###"
    ---
    - "**** Input Files "
    - "L0, all files 100mb                                                                                                "
    - "L0.1[0,5] 6ns            |L0.1|                                                                                    "
    - "L0.2[5,10] 11ns          |L0.2|                                                                                    "
    - "L0.3[10,15] 16ns         |L0.3|                                                                                    "
    - "L0.4[15,20] 21ns          |L0.4|                                                                                   "
    - "L0.5[20,25] 26ns          |L0.5|                                                                                   "
    - "L0.6[25,30] 31ns           |L0.6|                                                                                  "
    - "L0.7[30,35] 36ns           |L0.7|                                                                                  "
    - "L0.8[35,40] 41ns            |L0.8|                                                                                 "
    - "L0.9[40,45] 46ns            |L0.9|                                                                                 "
    - "L0.10[45,50] 51ns            |L0.10|                                                                               "
    - "L0.11[100,105] 106ns              |L0.11|                                                                          "
    - "L0.12[105,110] 111ns              |L0.12|                                                                          "
    - "L0.13[110,115] 116ns               |L0.13|                                                                         "
    - "L0.14[115,120] 121ns               |L0.14|                                                                         "
    - "L0.15[120,125] 126ns                |L0.15|                                                                        "
    - "L0.16[125,130] 131ns                |L0.16|                                                                        "
    - "L0.17[130,135] 136ns                 |L0.17|                                                                       "
    - "L0.18[135,140] 141ns                 |L0.18|                                                                       "
    - "L0.19[140,145] 146ns                 |L0.19|                                                                       "
    - "L0.20[145,150] 151ns                  |L0.20|                                                                      "
    - "L0.21[200,205] 206ns                       |L0.21|                                                                 "
    - "L0.22[205,210] 211ns                       |L0.22|                                                                 "
    - "L0.23[210,215] 216ns                        |L0.23|                                                                "
    - "L0.24[215,220] 221ns                        |L0.24|                                                                "
    - "L0.25[220,225] 226ns                         |L0.25|                                                               "
    - "L0.26[225,230] 231ns                         |L0.26|                                                               "
    - "L0.27[230,235] 236ns                          |L0.27|                                                              "
    - "L0.28[235,240] 241ns                          |L0.28|                                                              "
    - "L0.29[240,245] 246ns                           |L0.29|                                                             "
    - "L0.30[245,250] 251ns                           |L0.30|                                                             "
    - "L0.31[300,305] 306ns                                |L0.31|                                                        "
    - "L0.32[305,310] 311ns                                 |L0.32|                                                       "
    - "L0.33[310,315] 316ns                                 |L0.33|                                                       "
    - "L0.34[315,320] 321ns                                  |L0.34|                                                      "
    - "L0.35[320,325] 326ns                                  |L0.35|                                                      "
    - "L0.36[325,330] 331ns                                   |L0.36|                                                     "
    - "L0.37[330,335] 336ns                                   |L0.37|                                                     "
    - "L0.38[335,340] 341ns                                   |L0.38|                                                     "
    - "L0.39[340,345] 346ns                                    |L0.39|                                                    "
    - "L0.40[345,350] 351ns                                    |L0.40|                                                    "
    - "L0.41[400,405] 406ns                                         |L0.41|                                               "
    - "L0.42[405,410] 411ns                                          |L0.42|                                              "
    - "L0.43[410,415] 416ns                                          |L0.43|                                              "
    - "L0.44[415,420] 421ns                                           |L0.44|                                             "
    - "L0.45[420,425] 426ns                                           |L0.45|                                             "
    - "L0.46[425,430] 431ns                                            |L0.46|                                            "
    - "L0.47[430,435] 436ns                                            |L0.47|                                            "
    - "L0.48[435,440] 441ns                                             |L0.48|                                           "
    - "L0.49[440,445] 446ns                                             |L0.49|                                           "
    - "L0.50[445,450] 451ns                                              |L0.50|                                          "
    - "L0.51[500,505] 506ns                                                   |L0.51|                                     "
    - "L0.52[505,510] 511ns                                                   |L0.52|                                     "
    - "L0.53[510,515] 516ns                                                    |L0.53|                                    "
    - "L0.54[515,520] 521ns                                                    |L0.54|                                    "
    - "L0.55[520,525] 526ns                                                     |L0.55|                                   "
    - "L0.56[525,530] 531ns                                                     |L0.56|                                   "
    - "L0.57[530,535] 536ns                                                     |L0.57|                                   "
    - "L0.58[535,540] 541ns                                                      |L0.58|                                  "
    - "L0.59[540,545] 546ns                                                      |L0.59|                                  "
    - "L0.60[545,550] 551ns                                                       |L0.60|                                 "
    - "L0.61[600,605] 606ns                                                            |L0.61|                            "
    - "L0.62[605,610] 611ns                                                            |L0.62|                            "
    - "L0.63[610,615] 616ns                                                             |L0.63|                           "
    - "L0.64[615,620] 621ns                                                             |L0.64|                           "
    - "L0.65[620,625] 626ns                                                              |L0.65|                          "
    - "L0.66[625,630] 631ns                                                              |L0.66|                          "
    - "L0.67[630,635] 636ns                                                               |L0.67|                         "
    - "L0.68[635,640] 641ns                                                               |L0.68|                         "
    - "L0.69[640,645] 646ns                                                                |L0.69|                        "
    - "L0.70[645,650] 651ns                                                                |L0.70|                        "
    - "L0.71[700,705] 706ns                                                                     |L0.71|                   "
    - "L0.72[705,710] 711ns                                                                      |L0.72|                  "
    - "L0.73[710,715] 716ns                                                                      |L0.73|                  "
    - "L0.74[715,720] 721ns                                                                       |L0.74|                 "
    - "L0.75[720,725] 726ns                                                                       |L0.75|                 "
    - "L0.76[725,730] 731ns                                                                       |L0.76|                 "
    - "L0.77[730,735] 736ns                                                                        |L0.77|                "
    - "L0.78[735,740] 741ns                                                                        |L0.78|                "
    - "L0.79[740,745] 746ns                                                                         |L0.79|               "
    - "L0.80[745,750] 751ns                                                                         |L0.80|               "
    - "L0.81[800,805] 806ns                                                                              |L0.81|          "
    - "L0.82[805,810] 811ns                                                                               |L0.82|         "
    - "L0.83[810,815] 816ns                                                                               |L0.83|         "
    - "L0.84[815,820] 821ns                                                                                |L0.84|        "
    - "L0.85[820,825] 826ns                                                                                |L0.85|        "
    - "L0.86[825,830] 831ns                                                                                 |L0.86|       "
    - "L0.87[830,835] 836ns                                                                                 |L0.87|       "
    - "L0.88[835,840] 841ns                                                                                  |L0.88|      "
    - "L0.89[840,845] 846ns                                                                                  |L0.89|      "
    - "L0.90[845,850] 851ns                                                                                   |L0.90|     "
    - "L0.91[900,905] 906ns                                                                                        |L0.91|"
    - "L0.92[905,910] 911ns                                                                                        |L0.92|"
    - "L0.93[910,915] 916ns                                                                                         |L0.93|"
    - "L0.94[915,920] 921ns                                                                                         |L0.94|"
    - "L0.95[920,925] 926ns                                                                                         |L0.95|"
    - "L0.96[925,930] 931ns                                                                                          |L0.96|"
    - "L0.97[930,935] 936ns                                                                                          |L0.97|"
    - "L0.98[935,940] 941ns                                                                                           |L0.98|"
    - "L0.99[940,945] 946ns                                                                                           |L0.99|"
    - "L0.100[945,950] 951ns                                                                                           |L0.100|"
    - "L0.101[60,75] 1.1us           |L0.101|                                                                             "
    - "L0.102[160,175] 1.2us                  |L0.102|                                                                    "
    - "L0.103[260,275] 1.3us                            |L0.103|                                                          "
    - "L0.104[360,375] 1.4us                                     |L0.104|                                                 "
    - "L0.105[460,475] 1.5us                                              |L0.105|                                        "
    - "L0.106[560,575] 1.6us                                                       |L0.106|                               "
    - "L0.107[660,675] 1.7us                                                                |L0.107|                      "
    - "L0.108[760,775] 1.8us                                                                          |L0.108|            "
    - "L0.109[860,875] 1.9us                                                                                   |L0.109|   "
    - "L0.110[960,975] 2us                                                                                              |L0.110|"
    - "**** Final Output Files (24.23gb written)"
    - "L2                                                                                                                 "
    - "L2.110[960,975] 2us 100mb                                                                                        |L2.110|"
    - "L2.251[0,5] 11ns 114mb   |L2.251|                                                                                  "
    - "L2.257[19,23] 26ns 97mb   |L2.257|                                                                                 "
    - "L2.263[37,41] 46ns 97mb     |L2.263|                                                                               "
    - "L2.267[100,105] 111ns 114mb         |L2.267|                                                                         "
    - "L2.273[119,123] 126ns 97mb          |L2.273|                                                                        "
    - "L2.279[137,141] 146ns 97mb            |L2.279|                                                                      "
    - "L2.281[200,205] 211ns 114mb                  |L2.281|                                                                "
    - "L2.287[219,223] 226ns 97mb                    |L2.287|                                                              "
    - "L2.293[237,241] 246ns 97mb                     |L2.293|                                                             "
    - "L2.299[300,305] 311ns 114mb                           |L2.299|                                                       "
    - "L2.305[319,323] 326ns 97mb                             |L2.305|                                                     "
    - "L2.311[337,341] 346ns 97mb                               |L2.311|                                                   "
    - "L2.315[400,405] 411ns 114mb                                    |L2.315|                                              "
    - "L2.321[419,423] 426ns 97mb                                      |L2.321|                                            "
    - "L2.327[437,441] 446ns 97mb                                        |L2.327|                                          "
    - "L2.329[500,505] 511ns 114mb                                              |L2.329|                                    "
    - "L2.335[519,523] 526ns 97mb                                               |L2.335|                                   "
    - "L2.341[537,541] 546ns 97mb                                                 |L2.341|                                 "
    - "L2.347[600,605] 611ns 114mb                                                       |L2.347|                           "
    - "L2.353[619,623] 626ns 97mb                                                         |L2.353|                         "
    - "L2.359[637,641] 646ns 97mb                                                          |L2.359|                        "
    - "L2.363[700,705] 711ns 114mb                                                                |L2.363|                  "
    - "L2.369[719,723] 726ns 97mb                                                                  |L2.369|                "
    - "L2.375[800,805] 811ns 114mb                                                                         |L2.375|         "
    - "L2.381[819,823] 826ns 97mb                                                                           |L2.381|       "
    - "L2.387[837,841] 846ns 97mb                                                                             |L2.387|     "
    - "L2.389[737,741] 746ns 97mb                                                                    |L2.389|              "
    - "L2.395[900,905] 911ns 114mb                                                                                   |L2.395|"
    - "L2.401[919,923] 926ns 97mb                                                                                    |L2.401|"
    - "L2.407[937,941] 946ns 97mb                                                                                      |L2.407|"
    - "L2.411[6,11] 21ns 116mb  |L2.411|                                                                                  "
    - "L2.412[12,16] 21ns 97mb   |L2.412|                                                                                 "
    - "L2.413[17,18] 21ns 39mb   |L2.413|                                                                                 "
    - "L2.414[24,29] 41ns 124mb   |L2.414|                                                                                "
    - "L2.415[30,34] 41ns 104mb   |L2.415|                                                                                "
    - "L2.416[35,36] 41ns 41mb     |L2.416|                                                                               "
    - "L2.417[42,55] 1.1us 111mb   |L2.417|                                                                               "
    - "L2.418[56,68] 1.1us 103mb     |L2.418|                                                                             "
    - "L2.419[69,75] 1.1us 55mb       |L2.419|                                                                            "
    - "L2.420[106,111] 121ns 116mb         |L2.420|                                                                         "
    - "L2.421[112,116] 121ns 97mb          |L2.421|                                                                        "
    - "L2.422[117,118] 121ns 39mb          |L2.422|                                                                        "
    - "L2.423[124,129] 141ns 124mb           |L2.423|                                                                       "
    - "L2.424[130,134] 141ns 104mb            |L2.424|                                                                      "
    - "L2.425[135,136] 141ns 41mb            |L2.425|                                                                      "
    - "L2.426[142,155] 1.2us 111mb             |L2.426|                                                                     "
    - "L2.427[156,168] 1.2us 103mb              |L2.427|                                                                    "
    - "L2.428[169,175] 1.2us 55mb               |L2.428|                                                                   "
    - "L2.429[206,211] 221ns 116mb                   |L2.429|                                                               "
    - "L2.430[212,216] 221ns 97mb                   |L2.430|                                                               "
    - "L2.431[217,218] 221ns 39mb                    |L2.431|                                                              "
    - "L2.432[224,229] 241ns 124mb                    |L2.432|                                                              "
    - "L2.433[230,234] 241ns 104mb                     |L2.433|                                                             "
    - "L2.434[235,236] 241ns 41mb                     |L2.434|                                                             "
    - "L2.435[242,255] 1.3us 111mb                      |L2.435|                                                            "
    - "L2.436[256,268] 1.3us 103mb                       |L2.436|                                                           "
    - "L2.437[269,275] 1.3us 55mb                        |L2.437|                                                          "
    - "L2.438[306,311] 321ns 116mb                            |L2.438|                                                      "
    - "L2.439[312,316] 321ns 97mb                            |L2.439|                                                      "
    - "L2.440[317,318] 321ns 39mb                             |L2.440|                                                     "
    - "L2.441[324,329] 341ns 124mb                             |L2.441|                                                     "
    - "L2.442[330,334] 341ns 104mb                              |L2.442|                                                    "
    - "L2.443[335,336] 341ns 41mb                              |L2.443|                                                    "
    - "L2.444[342,355] 1.4us 111mb                               |L2.444|                                                   "
    - "L2.445[356,368] 1.4us 103mb                                |L2.445|                                                  "
    - "L2.446[369,375] 1.4us 55mb                                  |L2.446|                                                "
    - "L2.447[406,411] 421ns 116mb                                     |L2.447|                                             "
    - "L2.448[412,416] 421ns 97mb                                      |L2.448|                                            "
    - "L2.449[417,418] 421ns 39mb                                      |L2.449|                                            "
    - "L2.450[424,429] 441ns 124mb                                       |L2.450|                                           "
    - "L2.451[430,434] 441ns 104mb                                       |L2.451|                                           "
    - "L2.452[435,436] 441ns 41mb                                        |L2.452|                                          "
    - "L2.453[442,455] 1.5us 111mb                                        |L2.453|                                          "
    - "L2.454[456,468] 1.5us 103mb                                          |L2.454|                                        "
    - "L2.455[469,475] 1.5us 55mb                                           |L2.455|                                       "
    - "L2.456[506,511] 521ns 116mb                                              |L2.456|                                    "
    - "L2.457[512,516] 521ns 97mb                                               |L2.457|                                   "
    - "L2.458[517,518] 521ns 39mb                                               |L2.458|                                   "
    - "L2.459[524,529] 541ns 124mb                                                |L2.459|                                  "
    - "L2.460[530,534] 541ns 104mb                                                |L2.460|                                  "
    - "L2.461[535,536] 541ns 41mb                                                 |L2.461|                                 "
    - "L2.462[542,555] 1.6us 111mb                                                  |L2.462|                                "
    - "L2.463[556,568] 1.6us 103mb                                                   |L2.463|                               "
    - "L2.464[569,575] 1.6us 55mb                                                    |L2.464|                              "
    - "L2.465[606,611] 621ns 116mb                                                       |L2.465|                           "
    - "L2.466[612,616] 621ns 97mb                                                        |L2.466|                          "
    - "L2.467[617,618] 621ns 39mb                                                        |L2.467|                          "
    - "L2.468[624,629] 641ns 124mb                                                         |L2.468|                         "
    - "L2.469[630,634] 641ns 104mb                                                          |L2.469|                        "
    - "L2.470[635,636] 641ns 41mb                                                          |L2.470|                        "
    - "L2.471[642,655] 1.7us 111mb                                                           |L2.471|                       "
    - "L2.472[656,668] 1.7us 103mb                                                            |L2.472|                      "
    - "L2.473[669,675] 1.7us 55mb                                                             |L2.473|                     "
    - "L2.474[706,711] 721ns 116mb                                                                 |L2.474|                 "
    - "L2.475[712,716] 721ns 97mb                                                                 |L2.475|                 "
    - "L2.476[717,718] 721ns 39mb                                                                  |L2.476|                "
    - "L2.477[724,729] 741ns 124mb                                                                  |L2.477|                "
    - "L2.478[730,734] 741ns 104mb                                                                   |L2.478|               "
    - "L2.479[735,736] 741ns 41mb                                                                   |L2.479|               "
    - "L2.480[742,755] 1.8us 111mb                                                                    |L2.480|              "
    - "L2.481[756,768] 1.8us 103mb                                                                     |L2.481|             "
    - "L2.482[769,775] 1.8us 55mb                                                                      |L2.482|            "
    - "L2.483[806,811] 821ns 116mb                                                                          |L2.483|        "
    - "L2.484[812,816] 821ns 97mb                                                                          |L2.484|        "
    - "L2.485[817,818] 821ns 39mb                                                                           |L2.485|       "
    - "L2.486[824,829] 841ns 124mb                                                                            |L2.486|      "
    - "L2.487[830,834] 841ns 104mb                                                                            |L2.487|      "
    - "L2.488[835,836] 841ns 41mb                                                                             |L2.488|     "
    - "L2.489[842,855] 1.9us 111mb                                                                             |L2.489|     "
    - "L2.490[856,868] 1.9us 103mb                                                                               |L2.490|   "
    - "L2.491[869,875] 1.9us 55mb                                                                                |L2.491|  "
    - "L2.492[906,911] 921ns 116mb                                                                                   |L2.492|"
    - "L2.493[912,916] 921ns 97mb                                                                                    |L2.493|"
    - "L2.494[917,918] 921ns 39mb                                                                                    |L2.494|"
    - "L2.495[924,929] 941ns 124mb                                                                                     |L2.495|"
    - "L2.496[930,934] 941ns 104mb                                                                                     |L2.496|"
    - "L2.497[935,936] 941ns 41mb                                                                                      |L2.497|"
    - "L2.498[942,947] 951ns 113mb                                                                                      |L2.498|"
    - "L2.499[948,950] 951ns 56mb                                                                                       |L2.499|"
    "###
    );
}

#[tokio::test]
async fn overlapping_out_of_order_l0_small() {
    test_helpers::maybe_start_logging();

    let max_files_per_plan = 20;
    let max_compact_size = 300 * ONE_MB;
    let max_file_size = max_compact_size / 3;

    let setup = layout_setup_builder()
        .await
        .with_max_num_files_per_plan(max_files_per_plan as usize)
        .with_max_desired_file_size_bytes(max_file_size)
        .with_partition_timeout(Duration::from_millis(10000))
        .with_suppress_run_output() // remove this to debug
        .build()
        .await;

    // create 10 chains of 10 files each, with gaps between each chain
    for i in 0..10 {
        for j in 0..10 {
            setup
                .partition
                .create_parquet_file(
                    parquet_builder()
                        .with_min_time(100 * i + j * 5)
                        .with_max_time(100 * i + (j + 1) * 5)
                        .with_max_l0_created_at(Time::from_timestamp_nanos(
                            100 * i + (j + 1) * 5 + 1,
                        ))
                        .with_file_size_bytes(max_file_size / 10),
                )
                .await;
        }
    }

    // create 10 files to fit in the gaps between the above chains, with newer max_l0_created_at
    for i in 0..10 {
        setup
            .partition
            .create_parquet_file(
                parquet_builder()
                    .with_min_time(100 * i + 60)
                    .with_max_time(100 * (i + 1) - 25)
                    .with_max_l0_created_at(Time::from_timestamp_nanos(1000 + 100 * (i + 1)))
                    .with_file_size_bytes(max_file_size / 10),
            )
            .await;
    }

    insta::assert_yaml_snapshot!(
        run_layout_scenario(&setup).await,
        @r###"
    ---
    - "**** Input Files "
    - "L0, all files 10mb                                                                                                 "
    - "L0.1[0,5] 6ns            |L0.1|                                                                                    "
    - "L0.2[5,10] 11ns          |L0.2|                                                                                    "
    - "L0.3[10,15] 16ns         |L0.3|                                                                                    "
    - "L0.4[15,20] 21ns          |L0.4|                                                                                   "
    - "L0.5[20,25] 26ns          |L0.5|                                                                                   "
    - "L0.6[25,30] 31ns           |L0.6|                                                                                  "
    - "L0.7[30,35] 36ns           |L0.7|                                                                                  "
    - "L0.8[35,40] 41ns            |L0.8|                                                                                 "
    - "L0.9[40,45] 46ns            |L0.9|                                                                                 "
    - "L0.10[45,50] 51ns            |L0.10|                                                                               "
    - "L0.11[100,105] 106ns              |L0.11|                                                                          "
    - "L0.12[105,110] 111ns              |L0.12|                                                                          "
    - "L0.13[110,115] 116ns               |L0.13|                                                                         "
    - "L0.14[115,120] 121ns               |L0.14|                                                                         "
    - "L0.15[120,125] 126ns                |L0.15|                                                                        "
    - "L0.16[125,130] 131ns                |L0.16|                                                                        "
    - "L0.17[130,135] 136ns                 |L0.17|                                                                       "
    - "L0.18[135,140] 141ns                 |L0.18|                                                                       "
    - "L0.19[140,145] 146ns                 |L0.19|                                                                       "
    - "L0.20[145,150] 151ns                  |L0.20|                                                                      "
    - "L0.21[200,205] 206ns                       |L0.21|                                                                 "
    - "L0.22[205,210] 211ns                       |L0.22|                                                                 "
    - "L0.23[210,215] 216ns                        |L0.23|                                                                "
    - "L0.24[215,220] 221ns                        |L0.24|                                                                "
    - "L0.25[220,225] 226ns                         |L0.25|                                                               "
    - "L0.26[225,230] 231ns                         |L0.26|                                                               "
    - "L0.27[230,235] 236ns                          |L0.27|                                                              "
    - "L0.28[235,240] 241ns                          |L0.28|                                                              "
    - "L0.29[240,245] 246ns                           |L0.29|                                                             "
    - "L0.30[245,250] 251ns                           |L0.30|                                                             "
    - "L0.31[300,305] 306ns                                |L0.31|                                                        "
    - "L0.32[305,310] 311ns                                 |L0.32|                                                       "
    - "L0.33[310,315] 316ns                                 |L0.33|                                                       "
    - "L0.34[315,320] 321ns                                  |L0.34|                                                      "
    - "L0.35[320,325] 326ns                                  |L0.35|                                                      "
    - "L0.36[325,330] 331ns                                   |L0.36|                                                     "
    - "L0.37[330,335] 336ns                                   |L0.37|                                                     "
    - "L0.38[335,340] 341ns                                   |L0.38|                                                     "
    - "L0.39[340,345] 346ns                                    |L0.39|                                                    "
    - "L0.40[345,350] 351ns                                    |L0.40|                                                    "
    - "L0.41[400,405] 406ns                                         |L0.41|                                               "
    - "L0.42[405,410] 411ns                                          |L0.42|                                              "
    - "L0.43[410,415] 416ns                                          |L0.43|                                              "
    - "L0.44[415,420] 421ns                                           |L0.44|                                             "
    - "L0.45[420,425] 426ns                                           |L0.45|                                             "
    - "L0.46[425,430] 431ns                                            |L0.46|                                            "
    - "L0.47[430,435] 436ns                                            |L0.47|                                            "
    - "L0.48[435,440] 441ns                                             |L0.48|                                           "
    - "L0.49[440,445] 446ns                                             |L0.49|                                           "
    - "L0.50[445,450] 451ns                                              |L0.50|                                          "
    - "L0.51[500,505] 506ns                                                   |L0.51|                                     "
    - "L0.52[505,510] 511ns                                                   |L0.52|                                     "
    - "L0.53[510,515] 516ns                                                    |L0.53|                                    "
    - "L0.54[515,520] 521ns                                                    |L0.54|                                    "
    - "L0.55[520,525] 526ns                                                     |L0.55|                                   "
    - "L0.56[525,530] 531ns                                                     |L0.56|                                   "
    - "L0.57[530,535] 536ns                                                     |L0.57|                                   "
    - "L0.58[535,540] 541ns                                                      |L0.58|                                  "
    - "L0.59[540,545] 546ns                                                      |L0.59|                                  "
    - "L0.60[545,550] 551ns                                                       |L0.60|                                 "
    - "L0.61[600,605] 606ns                                                            |L0.61|                            "
    - "L0.62[605,610] 611ns                                                            |L0.62|                            "
    - "L0.63[610,615] 616ns                                                             |L0.63|                           "
    - "L0.64[615,620] 621ns                                                             |L0.64|                           "
    - "L0.65[620,625] 626ns                                                              |L0.65|                          "
    - "L0.66[625,630] 631ns                                                              |L0.66|                          "
    - "L0.67[630,635] 636ns                                                               |L0.67|                         "
    - "L0.68[635,640] 641ns                                                               |L0.68|                         "
    - "L0.69[640,645] 646ns                                                                |L0.69|                        "
    - "L0.70[645,650] 651ns                                                                |L0.70|                        "
    - "L0.71[700,705] 706ns                                                                     |L0.71|                   "
    - "L0.72[705,710] 711ns                                                                      |L0.72|                  "
    - "L0.73[710,715] 716ns                                                                      |L0.73|                  "
    - "L0.74[715,720] 721ns                                                                       |L0.74|                 "
    - "L0.75[720,725] 726ns                                                                       |L0.75|                 "
    - "L0.76[725,730] 731ns                                                                       |L0.76|                 "
    - "L0.77[730,735] 736ns                                                                        |L0.77|                "
    - "L0.78[735,740] 741ns                                                                        |L0.78|                "
    - "L0.79[740,745] 746ns                                                                         |L0.79|               "
    - "L0.80[745,750] 751ns                                                                         |L0.80|               "
    - "L0.81[800,805] 806ns                                                                              |L0.81|          "
    - "L0.82[805,810] 811ns                                                                               |L0.82|         "
    - "L0.83[810,815] 816ns                                                                               |L0.83|         "
    - "L0.84[815,820] 821ns                                                                                |L0.84|        "
    - "L0.85[820,825] 826ns                                                                                |L0.85|        "
    - "L0.86[825,830] 831ns                                                                                 |L0.86|       "
    - "L0.87[830,835] 836ns                                                                                 |L0.87|       "
    - "L0.88[835,840] 841ns                                                                                  |L0.88|      "
    - "L0.89[840,845] 846ns                                                                                  |L0.89|      "
    - "L0.90[845,850] 851ns                                                                                   |L0.90|     "
    - "L0.91[900,905] 906ns                                                                                        |L0.91|"
    - "L0.92[905,910] 911ns                                                                                        |L0.92|"
    - "L0.93[910,915] 916ns                                                                                         |L0.93|"
    - "L0.94[915,920] 921ns                                                                                         |L0.94|"
    - "L0.95[920,925] 926ns                                                                                         |L0.95|"
    - "L0.96[925,930] 931ns                                                                                          |L0.96|"
    - "L0.97[930,935] 936ns                                                                                          |L0.97|"
    - "L0.98[935,940] 941ns                                                                                           |L0.98|"
    - "L0.99[940,945] 946ns                                                                                           |L0.99|"
    - "L0.100[945,950] 951ns                                                                                           |L0.100|"
    - "L0.101[60,75] 1.1us           |L0.101|                                                                             "
    - "L0.102[160,175] 1.2us                  |L0.102|                                                                    "
    - "L0.103[260,275] 1.3us                            |L0.103|                                                          "
    - "L0.104[360,375] 1.4us                                     |L0.104|                                                 "
    - "L0.105[460,475] 1.5us                                              |L0.105|                                        "
    - "L0.106[560,575] 1.6us                                                       |L0.106|                               "
    - "L0.107[660,675] 1.7us                                                                |L0.107|                      "
    - "L0.108[760,775] 1.8us                                                                          |L0.108|            "
    - "L0.109[860,875] 1.9us                                                                                   |L0.109|   "
    - "L0.110[960,975] 2us                                                                                              |L0.110|"
    - "**** Final Output Files (1.83gb written)"
    - "L2                                                                                                                 "
    - "L2.111[0,40] 51ns 80mb   |L2.111|                                                                                  "
    - "L2.126[600,640] 651ns 80mb                                                       |L2.126|                           "
    - "L2.132[300,340] 351ns 80mb                           |L2.132|                                                       "
    - "L2.138[900,940] 951ns 80mb                                                                                   |L2.138|"
    - "L2.141[41,135] 1.3us 101mb   |L2.141|                                                                               "
    - "L2.142[136,229] 1.3us 100mb            |L2.142|                                                                      "
    - "L2.143[230,275] 1.3us 49mb                     |L2.143|                                                             "
    - "L2.144[341,435] 1.6us 101mb                               |L2.144|                                                   "
    - "L2.145[436,529] 1.6us 100mb                                        |L2.145|                                          "
    - "L2.146[530,575] 1.6us 49mb                                                |L2.146|                                  "
    - "L2.147[641,735] 1.9us 101mb                                                           |L2.147|                       "
    - "L2.148[736,829] 1.9us 100mb                                                                   |L2.148|               "
    - "L2.149[830,875] 1.9us 49mb                                                                            |L2.149|      "
    - "L2.150[941,968] 2us 24mb                                                                                       |L2.150|"
    - "L2.151[969,975] 2us 6mb                                                                                           |L2.151|"
    "###
    );
}
