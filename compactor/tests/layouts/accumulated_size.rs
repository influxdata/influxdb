//! layout tests related to the size L1/L2 files achieve when the L0 size is small.
//!
//! The intent of these tests is to ensure that when L0s are arriving in a normal/leading edge pattern,
//! even if they're quite small (10KB) the L1 & L2 files should still be accumulated to a reasonable size.
//!
//! Accumulating large L1/L2 is generally easier when cleaning up a backlogged partition with many L0s,
//! so these test try to mimic the more challenging scenario of a steady stream of small L0s.
//! The steady stream of L0s can be partially simulated by setting the max files per plan to a small number,
//! and putting just a few files in the test case.

use data_types::CompactionLevel;
use iox_time::Time;

use crate::layouts::{layout_setup_builder, parquet_builder, run_layout_scenario, ONE_MB};

const MAX_DESIRED_FILE_SIZE: u64 = 100 * ONE_MB;

// Mimic small L0 files trickling when they overlap in time (by a minor amount, as is common)
// In this case, all L1 and L0 files can fit in a single compaction run.
#[tokio::test]
async fn small_l1_plus_overlapping_l0s_single_run() {
    test_helpers::maybe_start_logging();

    let setup = layout_setup_builder()
        .await
        .with_max_desired_file_size_bytes(MAX_DESIRED_FILE_SIZE)
        .with_max_num_files_per_plan(4) // artificially limit to 4 / plan to similuate a steady stream of small files compacted as they come in.
        .build()
        .await;

    let size = 10 * 1024;

    // Create 1 L1 file that mimics the output from a previous L0 compaction
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(0)
                .with_max_time(10)
                .with_compaction_level(CompactionLevel::FileNonOverlapped)
                .with_max_l0_created_at(Time::from_timestamp_nanos(11))
                .with_file_size_bytes(size * 4_u64),
        )
        .await;

    // Create 3 L0 files, slighly overlapping in time.
    // note the first L0 slightly overlaps the L1, as would happen if this slightly overlapping pattern occured
    // in the files that (we're pretending) were compacted into that L1.
    for i in 1..=3 {
        setup
            .partition
            .create_parquet_file(
                parquet_builder()
                    .with_min_time(i * 10)
                    .with_max_time((i + 1) * 10)
                    .with_compaction_level(CompactionLevel::Initial)
                    .with_max_l0_created_at(Time::from_timestamp_nanos((i + 1) * 10 + 1))
                    .with_file_size_bytes(size),
            )
            .await;
    }

    // Required behavior:
    //    1. (achieved) all files compacted to a single L1 file
    // Desired behavior:
    //    1. (achieved) only one compaction is performed to compact them
    insta::assert_yaml_snapshot!(
        run_layout_scenario(&setup).await,
        @r###"
    ---
    - "**** Input Files "
    - "L0                                                                                                                 "
    - "L0.2[10,20] 21ns 10kb                          |--------L0.2--------|                                              "
    - "L0.3[20,30] 31ns 10kb                                                 |--------L0.3--------|                       "
    - "L0.4[30,40] 41ns 10kb                                                                       |--------L0.4--------| "
    - "L1                                                                                                                 "
    - "L1.1[0,10] 11ns 40kb     |--------L1.1--------|                                                                    "
    - "**** Simulation run 0, type=compact(TotalSizeLessThanMaxCompactSize). 4 Input Files, 70kb total:"
    - "L0                                                                                                                 "
    - "L0.4[30,40] 41ns 10kb                                                                       |--------L0.4--------| "
    - "L0.3[20,30] 31ns 10kb                                                 |--------L0.3--------|                       "
    - "L0.2[10,20] 21ns 10kb                          |--------L0.2--------|                                              "
    - "L1                                                                                                                 "
    - "L1.1[0,10] 11ns 40kb     |--------L1.1--------|                                                                    "
    - "**** 1 Output Files (parquet_file_id not yet assigned), 70kb total:"
    - "L1, all files 70kb                                                                                                 "
    - "L1.?[0,40] 41ns          |------------------------------------------L1.?------------------------------------------|"
    - "Committing partition 1:"
    - "  Soft Deleting 4 files: L1.1, L0.2, L0.3, L0.4"
    - "  Creating 1 files"
    - "**** Final Output Files (70kb written)"
    - "L1, all files 70kb                                                                                                 "
    - "L1.5[0,40] 41ns          |------------------------------------------L1.5------------------------------------------|"
    "###
    );
}

// Mimic small L0 files trickling when they overlap in time (by a minor amount, as is common)
// In this case, all L1 and L0 files do not fit in a single compaction run.
#[tokio::test]
async fn small_l1_plus_overlapping_l0s_two_runs() {
    test_helpers::maybe_start_logging();

    let setup = layout_setup_builder()
        .await
        .with_max_desired_file_size_bytes(MAX_DESIRED_FILE_SIZE)
        .with_max_num_files_per_plan(4) // artificially limit to 4 / plan to similuate a steady stream of small files compacted as they come in.
        .build()
        .await;

    let size = 10 * 1024;

    // Create 1 L1 file that mimics the output from a previous L0 compaction
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(0)
                .with_max_time(10)
                .with_compaction_level(CompactionLevel::FileNonOverlapped)
                .with_max_l0_created_at(Time::from_timestamp_nanos(11))
                .with_file_size_bytes(size * 4_u64),
        )
        .await;

    // Create 4 L0 files, slighly overlapping in time
    // note the first L0 slightly overlaps the L1, as would happen if this slightly overlapping pattern occured
    // in the files that (we're pretending) were compacted into that L1.
    for i in 1..=4 {
        setup
            .partition
            .create_parquet_file(
                parquet_builder()
                    .with_min_time(i * 10)
                    .with_max_time((i + 1) * 10)
                    .with_compaction_level(CompactionLevel::Initial)
                    .with_max_l0_created_at(Time::from_timestamp_nanos((i + 1) * 10 + 1))
                    .with_file_size_bytes(size),
            )
            .await;
    }

    // Required behavior:
    //    1. (achieved) all files compacted to a single L1 file
    // Desired behavior:
    //    1. (not achieved) It may be preferable that the first compaction include the last L1 and as many L0s as are allowed (3).
    //       This does not happen.  Instead, the first compaction is the four L0's that are later combined with the L1.
    //       This is not necessarily bad, actually, its better for write amplification.  But might hint at the possibility of
    //       compaction sequences that never get around to coming back and picking up the L1.
    //       So the current behavior is noteworthy and unclear if its 'good' or 'bad'.
    insta::assert_yaml_snapshot!(
        run_layout_scenario(&setup).await,
        @r###"
    ---
    - "**** Input Files "
    - "L0                                                                                                                 "
    - "L0.2[10,20] 21ns 10kb                      |------L0.2------|                                                      "
    - "L0.3[20,30] 31ns 10kb                                        |------L0.3------|                                    "
    - "L0.4[30,40] 41ns 10kb                                                          |------L0.4------|                  "
    - "L0.5[40,50] 51ns 10kb                                                                            |------L0.5------|"
    - "L1                                                                                                                 "
    - "L1.1[0,10] 11ns 40kb     |------L1.1------|                                                                        "
    - "**** Simulation run 0, type=compact(ManySmallFiles). 4 Input Files, 40kb total:"
    - "L0, all files 10kb                                                                                                 "
    - "L0.2[10,20] 21ns         |--------L0.2--------|                                                                    "
    - "L0.3[20,30] 31ns                               |--------L0.3--------|                                              "
    - "L0.4[30,40] 41ns                                                      |--------L0.4--------|                       "
    - "L0.5[40,50] 51ns                                                                            |--------L0.5--------| "
    - "**** 1 Output Files (parquet_file_id not yet assigned), 40kb total:"
    - "L0, all files 40kb                                                                                                 "
    - "L0.?[10,50] 51ns         |------------------------------------------L0.?------------------------------------------|"
    - "Committing partition 1:"
    - "  Soft Deleting 4 files: L0.2, L0.3, L0.4, L0.5"
    - "  Creating 1 files"
    - "**** Simulation run 1, type=compact(TotalSizeLessThanMaxCompactSize). 2 Input Files, 80kb total:"
    - "L0, all files 40kb                                                                                                 "
    - "L0.6[10,50] 51ns                           |---------------------------------L0.6---------------------------------|"
    - "L1, all files 40kb                                                                                                 "
    - "L1.1[0,10] 11ns          |------L1.1------|                                                                        "
    - "**** 1 Output Files (parquet_file_id not yet assigned), 80kb total:"
    - "L1, all files 80kb                                                                                                 "
    - "L1.?[0,50] 51ns          |------------------------------------------L1.?------------------------------------------|"
    - "Committing partition 1:"
    - "  Soft Deleting 2 files: L1.1, L0.6"
    - "  Creating 1 files"
    - "**** Final Output Files (120kb written)"
    - "L1, all files 80kb                                                                                                 "
    - "L1.7[0,50] 51ns          |------------------------------------------L1.7------------------------------------------|"
    "###
    );
}

// Mimic small L0 files trickling when they do NOT overlap in time (i.e. they have gaps between them)
// In this case, all L1 and L0 files can fit in a single compaction run.
#[tokio::test]
async fn small_l1_plus_nonoverlapping_l0s_single_run() {
    test_helpers::maybe_start_logging();

    let setup = layout_setup_builder()
        .await
        .with_max_desired_file_size_bytes(MAX_DESIRED_FILE_SIZE)
        .with_max_num_files_per_plan(4) // artificially limit to 4 / plan to similuate a steady stream of small files compacted as they come in.
        .build()
        .await;

    let size = 10 * 1024;

    // Create 1 L1 file that mimics the output from a previous L0 compaction
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(0)
                .with_max_time(9)
                .with_compaction_level(CompactionLevel::FileNonOverlapped)
                .with_max_l0_created_at(Time::from_timestamp_nanos(11))
                .with_file_size_bytes(size * 4_u64),
        )
        .await;

    // Create 3 L0 files, not overlapping in time, and not overlapping the L1.
    for i in 1..=3 {
        setup
            .partition
            .create_parquet_file(
                parquet_builder()
                    .with_min_time(i * 10 + 1)
                    .with_max_time((i + 1) * 10 - 1)
                    .with_compaction_level(CompactionLevel::Initial)
                    .with_max_l0_created_at(Time::from_timestamp_nanos((i + 1) * 10 + 1))
                    .with_file_size_bytes(size),
            )
            .await;
    }

    // Required behavior:
    //    1. (not achieved) all files compacted to a single L1 file
    //       The assumption is: since it didn't combine the old L1 with the new one, it will never grow L1.1.
    //       It will eventually compact L1.1 with other L1s to make an L2, but if the write pattern continues
    //       with tiny L0s, the resulting L2 will be N * the L1 size (where N is the number of L1s compacted
    //       into the L2).
    insta::assert_yaml_snapshot!(
        run_layout_scenario(&setup).await,
        @r###"
    ---
    - "**** Input Files "
    - "L0                                                                                                                 "
    - "L0.2[11,19] 21ns 10kb                             |------L0.2------|                                               "
    - "L0.3[21,29] 31ns 10kb                                                    |------L0.3------|                        "
    - "L0.4[31,39] 41ns 10kb                                                                           |------L0.4------| "
    - "L1                                                                                                                 "
    - "L1.1[0,9] 11ns 40kb      |-------L1.1-------|                                                                      "
    - "**** Simulation run 0, type=compact(TotalSizeLessThanMaxCompactSize). 4 Input Files, 70kb total:"
    - "L0                                                                                                                 "
    - "L0.4[31,39] 41ns 10kb                                                                           |------L0.4------| "
    - "L0.3[21,29] 31ns 10kb                                                    |------L0.3------|                        "
    - "L0.2[11,19] 21ns 10kb                             |------L0.2------|                                               "
    - "L1                                                                                                                 "
    - "L1.1[0,9] 11ns 40kb      |-------L1.1-------|                                                                      "
    - "**** 1 Output Files (parquet_file_id not yet assigned), 70kb total:"
    - "L1, all files 70kb                                                                                                 "
    - "L1.?[0,39] 41ns          |------------------------------------------L1.?------------------------------------------|"
    - "Committing partition 1:"
    - "  Soft Deleting 4 files: L1.1, L0.2, L0.3, L0.4"
    - "  Creating 1 files"
    - "**** Final Output Files (70kb written)"
    - "L1, all files 70kb                                                                                                 "
    - "L1.5[0,39] 41ns          |------------------------------------------L1.5------------------------------------------|"
    "###
    );
}

// Mimic small L0 files trickling when they do NOT overlap in time (i.e. they have gaps between them)
// In this case, all L1 and L0 files do not fit in a single compaction run.
#[tokio::test]
async fn small_l1_plus_nonoverlapping_l0s_two_runs() {
    test_helpers::maybe_start_logging();

    let setup = layout_setup_builder()
        .await
        .with_max_desired_file_size_bytes(MAX_DESIRED_FILE_SIZE)
        .with_max_num_files_per_plan(4) // artificially limit to 4 / plan to similuate a steady stream of small files compacted as they come in.
        .build()
        .await;

    let size = 10 * 1024;

    // Create 1 L1 file that mimics the output from a previous L0 compaction
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(0)
                .with_max_time(10)
                .with_compaction_level(CompactionLevel::FileNonOverlapped)
                .with_max_l0_created_at(Time::from_timestamp_nanos(11))
                .with_file_size_bytes(size * 4_u64),
        )
        .await;

    // Create 4 L0 files, not overlapping in time, and not overlapping the L1.
    for i in 1..=4 {
        setup
            .partition
            .create_parquet_file(
                parquet_builder()
                    .with_min_time(i * 10 + 1)
                    .with_max_time((i + 1) * 10 - 1)
                    .with_compaction_level(CompactionLevel::Initial)
                    .with_max_l0_created_at(Time::from_timestamp_nanos((i + 1) * 10 + 1))
                    .with_file_size_bytes(size),
            )
            .await;
    }

    // Required behavior:
    //    1. (not achieved) all files compacted to a single L1 file
    //       The assumption is: since it didn't combine the old L1 with the new one, it will never grow L1.1.
    //       It will eventually compact L1.1 with other L1s to make an L2, but if the write pattern continues
    //       with tiny L0s, the resulting L2 will be N * the L1 size (where N is the number of L1s compacted
    //       into the L2).
    insta::assert_yaml_snapshot!(
        run_layout_scenario(&setup).await,
        @r###"
    ---
    - "**** Input Files "
    - "L0                                                                                                                 "
    - "L0.2[11,19] 21ns 10kb                        |----L0.2----|                                                        "
    - "L0.3[21,29] 31ns 10kb                                          |----L0.3----|                                      "
    - "L0.4[31,39] 41ns 10kb                                                            |----L0.4----|                    "
    - "L0.5[41,49] 51ns 10kb                                                                               |----L0.5----| "
    - "L1                                                                                                                 "
    - "L1.1[0,10] 11ns 40kb     |------L1.1------|                                                                        "
    - "**** Simulation run 0, type=compact(FoundSubsetLessThanMaxCompactSize). 4 Input Files, 40kb total:"
    - "L0, all files 10kb                                                                                                 "
    - "L0.2[11,19] 21ns         |------L0.2------|                                                                        "
    - "L0.3[21,29] 31ns                                |------L0.3------|                                                 "
    - "L0.4[31,39] 41ns                                                        |------L0.4------|                         "
    - "L0.5[41,49] 51ns                                                                                |------L0.5------| "
    - "**** 1 Output Files (parquet_file_id not yet assigned), 40kb total:"
    - "L1, all files 40kb                                                                                                 "
    - "L1.?[11,49] 51ns         |------------------------------------------L1.?------------------------------------------|"
    - "Committing partition 1:"
    - "  Soft Deleting 4 files: L0.2, L0.3, L0.4, L0.5"
    - "  Creating 1 files"
    - "**** Final Output Files (40kb written)"
    - "L1, all files 40kb                                                                                                 "
    - "L1.1[0,10] 11ns          |------L1.1------|                                                                        "
    - "L1.6[11,49] 51ns                             |-------------------------------L1.6--------------------------------| "
    "###
    );
}
