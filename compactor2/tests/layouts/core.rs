//! layout tests for the "core" design scenarios for compactor
//!
//! See [crate::layout] module for detailed documentation

use data_types::CompactionLevel;

use crate::layouts::{
    all_overlapping_l0_files, layout_setup_builder, parquet_builder, run_layout_scenario, ONE_MB,
};

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
    - "**** 2 Output Files (parquet_file_id not yet assigned), 90mb total:"
    - "L1                                                                                                  "
    - "L1.?[100,180] 72mb  |-----------------------------L1.?-----------------------------|                "
    - "L1.?[180,200] 18mb                                                                  |-----L1.?-----|"
    - "Committing partition 1:"
    - "  Soft Deleting 10 files: L0.1, L0.2, L0.3, L0.4, L0.5, L0.6, L0.7, L0.8, L0.9, L0.10"
    - "  Creating 2 files at level CompactionLevel::L1"
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
    - "**** 2 Output Files (parquet_file_id not yet assigned), 100mb total:"
    - "L1                                                                                                  "
    - "L1.?[0,720] 79.91mb |----------------------------L1.?-----------------------------|                 "
    - "L1.?[720,901] 20.09mb                                                               |-----L1.?-----| "
    - "Committing partition 1:"
    - "  Soft Deleting 10 files: L0.1, L0.2, L0.3, L0.4, L0.5, L0.6, L0.7, L0.8, L0.9, L0.10"
    - "  Creating 2 files at level CompactionLevel::L1"
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
    - "**** 1 Output Files (parquet_file_id not yet assigned), 10.02mb total:"
    - "L1, all files 10.02mb                                                                               "
    - "L1.?[100,310]       |-------------------------------------L1.?-------------------------------------|"
    - "Committing partition 1:"
    - "  Soft Deleting 6 files: L1.2, L0.3, L0.4, L0.5, L0.6, L0.7"
    - "  Creating 1 files at level CompactionLevel::L1"
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
    - "**** 1 Output Files (parquet_file_id not yet assigned), 25kb total:"
    - "L1, all files 25kb                                                                                  "
    - "L1.?[300,550]       |-------------------------------------L1.?-------------------------------------|"
    - "Committing partition 1:"
    - "  Soft Deleting 5 files: L0.3, L0.4, L0.5, L0.6, L0.7"
    - "  Creating 1 files at level CompactionLevel::L1"
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
    - "**** 1 Output Files (parquet_file_id not yet assigned), 15mb total:"
    - "L1, all files 15mb                                                                                  "
    - "L1.?[300,450]       |-------------------------------------L1.?-------------------------------------|"
    - "Committing partition 1:"
    - "  Soft Deleting 3 files: L0.5, L0.6, L0.7"
    - "  Creating 1 files at level CompactionLevel::L1"
    - "**** Simulation run 1, type=split(split_times=[370]). 5 Input Files, 108mb total:"
    - "L1                                                                                                  "
    - "L1.4[200,250] 3mb                                 |--L1.4--|                                        "
    - "L1.3[150,200] 20mb                      |--L1.3--|                                                  "
    - "L1.2[100,150] 50mb            |--L1.2--|                                                            "
    - "L1.1[50,100] 20mb   |--L1.1--|                                                                      "
    - "L1.8[300,450] 15mb                                                    |------------L1.8------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 108mb total:"
    - "L2                                                                                                  "
    - "L2.?[50,370] 86.4mb |-----------------------------L2.?-----------------------------|                "
    - "L2.?[370,450] 21.6mb                                                                |-----L2.?-----|"
    - "Committing partition 1:"
    - "  Soft Deleting 5 files: L1.1, L1.2, L1.3, L1.4, L1.8"
    - "  Creating 2 files at level CompactionLevel::L2"
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
    - "**** 1 Output Files (parquet_file_id not yet assigned), 15mb total:"
    - "L1, all files 15mb                                                                                  "
    - "L1.?[600,650]       |-------------------------------------L1.?-------------------------------------|"
    - "Committing partition 1:"
    - "  Soft Deleting 3 files: L0.11, L0.12, L0.13"
    - "  Creating 1 files at level CompactionLevel::L1"
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
    - "**** 1 Output Files (parquet_file_id not yet assigned), 15mb total:"
    - "L1, all files 15mb                                                                                  "
    - "L1.?[600,650]       |-------------------------------------L1.?-------------------------------------|"
    - "Committing partition 1:"
    - "  Soft Deleting 3 files: L0.11, L0.12, L0.13"
    - "  Creating 1 files at level CompactionLevel::L1"
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
    - "**** 2 Output Files (parquet_file_id not yet assigned), 88mb total:"
    - "L2                                                                                                  "
    - "L2.?[50,530] 70.4mb |-----------------------------L2.?-----------------------------|                "
    - "L2.?[530,650] 17.6mb                                                                |-----L2.?-----|"
    - "Committing partition 1:"
    - "  Soft Deleting 11 files: L1.1, L1.2, L1.3, L1.4, L1.5, L1.6, L1.7, L1.8, L1.9, L1.10, L1.14"
    - "  Creating 2 files at level CompactionLevel::L2"
    - "**** Final Output Files "
    - "L2                                                                                                  "
    - "L2.15[50,530] 70.4mb|----------------------------L2.15-----------------------------|                "
    - "L2.16[530,650] 17.6mb                                                                |----L2.16-----|"
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
    - "**** 1 Output Files (parquet_file_id not yet assigned), 15mb total:"
    - "L1, all files 15mb                                                                                  "
    - "L1.?[600,650]       |-------------------------------------L1.?-------------------------------------|"
    - "Committing partition 1:"
    - "  Soft Deleting 3 files: L0.3, L0.4, L0.5"
    - "  Creating 1 files at level CompactionLevel::L1"
    - "**** Simulation run 1, type=split(split_times=[375]). 3 Input Files, 185mb total:"
    - "L1                                                                                                  "
    - "L1.2[100,150] 80mb        |L1.2|                                                                    "
    - "L1.1[50,100] 90mb   |L1.1|                                                                          "
    - "L1.6[600,650] 15mb                                                                           |L1.6| "
    - "**** 2 Output Files (parquet_file_id not yet assigned), 185mb total:"
    - "L2                                                                                                  "
    - "L2.?[50,375] 100.21mb|------------------L2.?-------------------|                                     "
    - "L2.?[375,650] 84.79mb                                           |---------------L2.?---------------| "
    - "Committing partition 1:"
    - "  Soft Deleting 3 files: L1.1, L1.2, L1.6"
    - "  Creating 2 files at level CompactionLevel::L2"
    - "**** Final Output Files "
    - "L2                                                                                                  "
    - "L2.7[50,375] 100.21mb|------------------L2.7-------------------|                                     "
    - "L2.8[375,650] 84.79mb                                           |---------------L2.8---------------| "
    "###
    );
}
