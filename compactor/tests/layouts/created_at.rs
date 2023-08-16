//! Tests for scenarios with subtle max_l0_created_at combinations.
//! Correct handling and preservation of the max_l0_created_at is required
//! for the compactor to correctly dedupe overwritten points.
//!
//! Whether or not the compactor does actually correctly dedupe the points is a separate
//! question.  The tests in this file evaluate if max_l0_created_at is correctly handled
//! so the compactor has the opportunity to correctly dedupe.
//!
//! The approach taken in these tests is to identify a base case with some potential
//! vulnerability for mishandling. Then various follow the base case permuting complications
//! to the base case that might confuse the comnpactor into mishandling presumed vulnerability
//! of the base case.

use data_types::CompactionLevel;
use iox_time::Time;

use crate::layouts::{layout_setup_builder, parquet_builder, run_layout_scenario, ONE_MB};

#[tokio::test]
async fn base1_level1_level2_overlaps() {
    // |------------L1.2------------|                |------------L1.3------------|
    //                      |--------------L2.1--------------|

    // Base case 1:
    // This is a base case of two L1s and an L2 overlapping, that will be further complicated
    // in subsequent test cases.  Assume that each L1 has at least 1 duplicate point with the L2.

    // All variations of base case 1 honor the invariant that when overlaps exist between levels,
    // the lower level file must have a newer (>=) max_l0_created_at.  In the above diagram, both L1.2
    // and L1.3 must have a newer max_l0_created_at than L2.1.

    // Note that the L2's max_l0_created_at time is older than that of the two L1s.
    // The duplicates in the L1s should prevail.

    // Mishandling the max_l0_created_at in L1.2 is the key vulnerability targeted by this case case.
    // Subsequent test cases will add various complications trying to trick the compactor into
    // mishandling L1.2's max_l0_created_at.

    test_helpers::maybe_start_logging();

    let setup = layout_setup_builder().await.build().await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(4)
                .with_max_time(7)
                .with_compaction_level(CompactionLevel::Final)
                .with_file_size_bytes(100 * ONE_MB)
                .with_max_l0_created_at(Time::from_timestamp_nanos(7)),
        )
        .await;

    // Non-overlapping L1 files but they overlap with L2 files as in the diagram above
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1)
                .with_max_time(5)
                .with_compaction_level(CompactionLevel::FileNonOverlapped)
                .with_file_size_bytes(50 * ONE_MB)
                .with_max_l0_created_at(Time::from_timestamp_nanos(8)), // Newer than L2
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(6)
                .with_max_time(10)
                .with_compaction_level(CompactionLevel::FileNonOverlapped)
                .with_file_size_bytes(50 * ONE_MB)
                .with_max_l0_created_at(Time::from_timestamp_nanos(10)), // Newer than L2
        )
        .await;

    // All file are compacted in a single compactions, so the compactor has the opportunity to properly resolve the duplicates.
    insta::assert_yaml_snapshot!(
        run_layout_scenario(&setup).await,
        @r###"
    ---
    - "**** Input Files "
    - "L1                                                                                                                 "
    - "L1.2[1,5] 8ns 50mb       |-----------------L1.2-----------------|                                                  "
    - "L1.3[6,10] 10ns 50mb                                                       |-----------------L1.3-----------------|"
    - "L2                                                                                                                 "
    - "L2.1[4,7] 7ns 100mb                                    |------------L2.1------------|                              "
    - "**** Simulation run 0, type=split(CompactAndSplitOutput(TotalSizeLessThanMaxCompactSize))(split_times=[6]). 3 Input Files, 200mb total:"
    - "L1                                                                                                                 "
    - "L1.3[6,10] 10ns 50mb                                                       |-----------------L1.3-----------------|"
    - "L1.2[1,5] 8ns 50mb       |-----------------L1.2-----------------|                                                  "
    - "L2                                                                                                                 "
    - "L2.1[4,7] 7ns 100mb                                    |------------L2.1------------|                              "
    - "**** 2 Output Files (parquet_file_id not yet assigned), 200mb total:"
    - "L2                                                                                                                 "
    - "L2.?[1,6] 10ns 120mb     |----------------------L2.?----------------------|                                        "
    - "L2.?[7,10] 10ns 80mb                                                                 |------------L2.?------------|"
    - "Committing partition 1:"
    - "  Soft Deleting 3 files: L2.1, L1.2, L1.3"
    - "  Creating 2 files"
    - "**** Final Output Files (200mb written)"
    - "L2                                                                                                                 "
    - "L2.4[1,6] 10ns 120mb     |----------------------L2.4----------------------|                                        "
    - "L2.5[7,10] 10ns 80mb                                                                 |------------L2.5------------|"
    "###
    );
}

#[tokio::test]
// base case 1 + an L0 overlapping the l1
async fn base1_level0_level1() {
    // This modifies base case by adding an L0 that overlaps only the first L1
    //       |--L0.2-|
    // |------------L1.2------------|                |------------L1.3------------|
    //                      |--------------L2.1--------------|

    test_helpers::maybe_start_logging();

    let setup = layout_setup_builder().await.build().await;

    // Start of base base 1
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(4)
                .with_max_time(7)
                .with_compaction_level(CompactionLevel::Final)
                .with_file_size_bytes(100 * ONE_MB)
                .with_max_l0_created_at(Time::from_timestamp_nanos(7)),
        )
        .await;

    // Non-overlapping L1 files but they overlap with L2 files as in the diagram above
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1)
                .with_max_time(5)
                .with_compaction_level(CompactionLevel::FileNonOverlapped)
                .with_file_size_bytes(50 * ONE_MB)
                .with_max_l0_created_at(Time::from_timestamp_nanos(8)), // Newer than L2
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(6)
                .with_max_time(10)
                .with_compaction_level(CompactionLevel::FileNonOverlapped)
                .with_file_size_bytes(50 * ONE_MB)
                .with_max_l0_created_at(Time::from_timestamp_nanos(10)), // Newer than L2
        )
        .await;
    // End of base base 1

    // Additions to base case:
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(2)
                .with_max_time(4)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(50 * ONE_MB)
                .with_max_l0_created_at(Time::from_timestamp_nanos(11)), // Newer than the L2; could add more permutations (older, equal)
        )
        .await;

    // run 0 compacts the L0 with L1.2 but since L1.2 already had a newer max_l0_created_at than L2.1, that's ok.
    insta::assert_yaml_snapshot!(
        run_layout_scenario(&setup).await,
        @r###"
    ---
    - "**** Input Files "
    - "L0                                                                                                                 "
    - "L0.4[2,4] 11ns 50mb                |-------L0.4-------|                                                            "
    - "L1                                                                                                                 "
    - "L1.2[1,5] 8ns 50mb       |-----------------L1.2-----------------|                                                  "
    - "L1.3[6,10] 10ns 50mb                                                       |-----------------L1.3-----------------|"
    - "L2                                                                                                                 "
    - "L2.1[4,7] 7ns 100mb                                    |------------L2.1------------|                              "
    - "**** Simulation run 0, type=split(CompactAndSplitOutput(TotalSizeLessThanMaxCompactSize))(split_times=[4]). 2 Input Files, 100mb total:"
    - "L0, all files 50mb                                                                                                 "
    - "L0.4[2,4] 11ns                                 |-------------------L0.4--------------------|                       "
    - "L1, all files 50mb                                                                                                 "
    - "L1.2[1,5] 8ns            |------------------------------------------L1.2------------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 100mb total:"
    - "L1                                                                                                                 "
    - "L1.?[1,4] 11ns 80mb      |------------------------------L1.?-------------------------------|                       "
    - "L1.?[5,5] 11ns 20mb                                                                                                |L1.?|"
    - "Committing partition 1:"
    - "  Soft Deleting 2 files: L1.2, L0.4"
    - "  Creating 2 files"
    - "**** Simulation run 1, type=split(CompactAndSplitOutput(TotalSizeLessThanMaxCompactSize))(split_times=[5, 9]). 4 Input Files, 250mb total:"
    - "L1                                                                                                                 "
    - "L1.3[6,10] 10ns 50mb                                                       |-----------------L1.3-----------------|"
    - "L1.6[5,5] 11ns 20mb                                              |L1.6|                                            "
    - "L1.5[1,4] 11ns 80mb      |------------L1.5------------|                                                            "
    - "L2                                                                                                                 "
    - "L2.1[4,7] 7ns 100mb                                    |------------L2.1------------|                              "
    - "**** 3 Output Files (parquet_file_id not yet assigned), 250mb total:"
    - "L2                                                                                                                 "
    - "L2.?[1,5] 11ns 125mb     |-----------------L2.?-----------------|                                                  "
    - "L2.?[6,9] 11ns 100mb                                                       |------------L2.?------------|          "
    - "L2.?[10,10] 11ns 25mb                                                                                              |L2.?|"
    - "Committing partition 1:"
    - "  Soft Deleting 4 files: L2.1, L1.3, L1.5, L1.6"
    - "  Creating 3 files"
    - "**** Final Output Files (350mb written)"
    - "L2                                                                                                                 "
    - "L2.7[1,5] 11ns 125mb     |-----------------L2.7-----------------|                                                  "
    - "L2.8[6,9] 11ns 100mb                                                       |------------L2.8------------|          "
    - "L2.9[10,10] 11ns 25mb                                                                                              |L2.9|"
    "###
    );
}

#[tokio::test]
#[should_panic(expected = "Found overlapping files with illegal max_l0_created_at order")]
async fn base1_level1_level2_invariant_violation() {
    // |------------L1.2------------|                |------------L1.3------------|
    //                      |--------------L2.1--------------|

    // This case modifies base case 1 by making the max_l0_created_at of L1.2 older than L2.1,
    // which violates an invariant and causes a panic in the simulator.

    // This test verifies the simulator's ability to detect violations of the invariant.

    test_helpers::maybe_start_logging();

    let setup = layout_setup_builder().await.build().await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(4)
                .with_max_time(7)
                .with_compaction_level(CompactionLevel::Final)
                .with_file_size_bytes(100 * ONE_MB)
                .with_max_l0_created_at(Time::from_timestamp_nanos(7)),
        )
        .await;

    // Non-overlapping L1 files but they overlap with L2 files as in the diagram above
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1)
                .with_max_time(5)
                .with_compaction_level(CompactionLevel::FileNonOverlapped)
                .with_file_size_bytes(50 * ONE_MB)
                .with_max_l0_created_at(Time::from_timestamp_nanos(5)), // Older than L2 - That violates an invariant
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(6)
                .with_max_time(10)
                .with_compaction_level(CompactionLevel::FileNonOverlapped)
                .with_file_size_bytes(50 * ONE_MB)
                .with_max_l0_created_at(Time::from_timestamp_nanos(10)), // Newer than L2
        )
        .await;

    // All file are compacted in a single compactions, so the compactor has the opportunity to properly resolve the duplicates.
    insta::assert_yaml_snapshot!(
        run_layout_scenario(&setup).await,
        @r###"
    "###
    );
}

#[tokio::test]
#[should_panic(expected = "Found overlapping files with illegal max_l0_created_at order")]
async fn base2_level0_level1_invariant_violation() {
    // |------------L0.2------------|                |------------L0.3------------|
    //                      |--------------L1.1--------------|

    // This case modifies base case 1 by changing the levels from L1,L2 to L0,L1, and by
    // making the max_l0_created_at of L0.2 older than L1.1, which violates an invariant
    // and causes a panic in the simulator.

    // This test verifies the simulator's ability to detect violations of the invariant.

    test_helpers::maybe_start_logging();

    let setup = layout_setup_builder().await.build().await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(4)
                .with_max_time(7)
                .with_compaction_level(CompactionLevel::FileNonOverlapped)
                .with_file_size_bytes(100 * ONE_MB)
                .with_max_l0_created_at(Time::from_timestamp_nanos(7)),
        )
        .await;

    // Non-overlapping L0 files but they overlap with L2 files as in the diagram above
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1)
                .with_max_time(5)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(50 * ONE_MB)
                .with_max_l0_created_at(Time::from_timestamp_nanos(5)), // Older than L1 - That violates an invariant
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(6)
                .with_max_time(10)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(50 * ONE_MB)
                .with_max_l0_created_at(Time::from_timestamp_nanos(10)), // Newer than L1
        )
        .await;

    // All file are compacted in a single compactions, so the compactor has the opportunity to properly resolve the duplicates.
    insta::assert_yaml_snapshot!(
        run_layout_scenario(&setup).await,
        @r###"
    "###
    );
}

#[tokio::test]
#[should_panic(expected = "Found overlapping files with illegal max_l0_created_at order")]
async fn base3_level0_level2_invariant_violation() {
    // |------------L0.2------------|                |------------L0.3------------|
    //                      |--------------L2.1--------------|

    // This case modifies base case 1 by changing the levels from L1,L2 to L0,L2, and by
    // making the max_l0_created_at of L0.2 older than L2.1, which violates an invariant
    // and causes a panic in the simulator.

    // This test verifies the simulator's ability to detect violations of the invariant.
    // This differs from the prior test in that this test is verifing the illegal
    // max_l0_created_at scenario is still detected when it skips a level.

    test_helpers::maybe_start_logging();

    let setup = layout_setup_builder().await.build().await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(4)
                .with_max_time(7)
                .with_compaction_level(CompactionLevel::Final)
                .with_file_size_bytes(100 * ONE_MB)
                .with_max_l0_created_at(Time::from_timestamp_nanos(7)),
        )
        .await;

    // Non-overlapping L0 files but they overlap with L2 files as in the diagram above
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1)
                .with_max_time(5)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(50 * ONE_MB)
                .with_max_l0_created_at(Time::from_timestamp_nanos(5)), // Older than L2 - That violates an invariant
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(6)
                .with_max_time(10)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(50 * ONE_MB)
                .with_max_l0_created_at(Time::from_timestamp_nanos(10)), // Newer than L2
        )
        .await;

    // All file are compacted in a single compactions, so the compactor has the opportunity to properly resolve the duplicates.
    insta::assert_yaml_snapshot!(
        run_layout_scenario(&setup).await,
        @r###"
    "###
    );
}
