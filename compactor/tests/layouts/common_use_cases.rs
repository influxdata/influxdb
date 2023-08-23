//! layout tests for common scenarios for compactor.
//!
//! These scenarios are the "best case" for the compactor and the ones for which its algorithm
//! is designed to work best with.
//!
//! See [crate::layout] module for detailed documentation

use compactor_test_utils::format_files;
use data_types::CompactionLevel;
use iox_time::Time;

use crate::layouts::{layout_setup_builder, parquet_builder, run_layout_scenario, ONE_MB};

// Each L0 file overlaps around 20% time range  with its previously created L0 file.
// The setup has final files of level 2 only
#[tokio::test]
async fn test_keep_ingesting_l0_files_20_percent_overlap() {
    test_helpers::maybe_start_logging();
    let setup = layout_setup_builder().await.build().await;

    // This test simulates the case where the ingester creates new L0 files
    // with the most recent data and the compactor runs continuously keeping
    // the partition ideally configured
    //
    // The tests compacts N times, each time new M number of L0 files are created.
    // Each L0 file is 5MB and only overlaps 20% with the previously created L0 file.

    let n = 100;
    let m = 5;
    let mut idx = 0;
    for _i in 0..n {
        for _j in 0..m {
            let min = idx * 10;
            let max = min + 11;
            setup
                .partition
                .create_parquet_file(
                    parquet_builder()
                        .with_min_time(min)
                        .with_max_time(max)
                        .with_file_size_bytes(5 * ONE_MB)
                        .with_max_l0_created_at(Time::from_timestamp_nanos(idx))
                        .with_compaction_level(CompactionLevel::Initial),
                )
                .await;
            idx += 1;
        }
        run_layout_scenario(&setup).await;
    }

    // Add three L0 file during last compacting
    for _ in 0..3 {
        let min = idx * 10;
        let max = min + 14;
        setup
            .partition
            .create_parquet_file(
                parquet_builder()
                    .with_min_time(min)
                    .with_max_time(max)
                    .with_file_size_bytes(5 * ONE_MB)
                    .with_max_l0_created_at(Time::from_timestamp_nanos(idx))
                    .with_compaction_level(CompactionLevel::Initial),
            )
            .await;
        idx += 1;
    }

    let files = setup.list_by_table_not_to_delete().await;

    // Only the earliest avaialble L0 overlaps with the latest L2 file
    insta::assert_yaml_snapshot!(
        format_files("final output", &files),
        @r###"
    ---
    - final output
    - "L0                                                                                                                 "
    - "L0.751[5000,5014] 500ns 5mb                                                                                         |L0.751|"
    - "L0.752[5010,5024] 501ns 5mb                                                                                         |L0.752|"
    - "L0.753[5020,5034] 502ns 5mb                                                                                         |L0.753|"
    - "L2                                                                                                                 "
    - "L2.29[0,160] 19ns 80mb   |L2.29|                                                                                   "
    - "L2.59[161,361] 39ns 100mb  |L2.59|                                                                                 "
    - "L2.89[362,553] 59ns 96mb       |L2.89|                                                                             "
    - "L2.119[554,754] 79ns 100mb         |L2.119|                                                                         "
    - "L2.149[755,955] 99ns 100mb             |L2.149|                                                                     "
    - "L2.179[956,1156] 119ns 100mb                 |L2.179|                                                                 "
    - "L2.209[1157,1357] 139ns 100mb                    |L2.209|                                                              "
    - "L2.239[1358,1558] 159ns 100mb                        |L2.239|                                                          "
    - "L2.269[1559,1759] 179ns 100mb                           |L2.269|                                                       "
    - "L2.299[1760,1960] 199ns 101mb                               |L2.299|                                                   "
    - "L2.329[1961,2161] 219ns 101mb                                   |L2.329|                                               "
    - "L2.359[2162,2353] 239ns 96mb                                      |L2.359|                                            "
    - "L2.389[2354,2554] 259ns 101mb                                          |L2.389|                                        "
    - "L2.419[2555,2755] 279ns 101mb                                             |L2.419|                                     "
    - "L2.449[2756,2956] 299ns 101mb                                                 |L2.449|                                 "
    - "L2.479[2957,3157] 319ns 101mb                                                    |L2.479|                              "
    - "L2.509[3158,3358] 339ns 101mb                                                        |L2.509|                          "
    - "L2.539[3359,3559] 359ns 101mb                                                            |L2.539|                      "
    - "L2.569[3560,3760] 379ns 101mb                                                               |L2.569|                   "
    - "L2.599[3761,3961] 399ns 101mb                                                                   |L2.599|               "
    - "L2.629[3962,4153] 419ns 96mb                                                                      |L2.629|            "
    - "L2.659[4154,4354] 439ns 101mb                                                                          |L2.659|        "
    - "L2.689[4355,4555] 459ns 101mb                                                                             |L2.689|     "
    - "L2.719[4556,4756] 479ns 101mb                                                                                 |L2.719| "
    - "L2.749[4757,4957] 499ns 101mb                                                                                     |L2.749|"
    - "L2.750[4958,5001] 499ns 22mb                                                                                        |L2.750|"
    "###
    );
}

// Each L0 file overlaps ~40% time range  with its previously created L0 file.
// The setup has final files of level 2 only
#[tokio::test]
async fn test_keep_ingesting_l0_files_40_percent_overlap() {
    test_helpers::maybe_start_logging();
    let setup = layout_setup_builder().await.build().await;

    // This test simulates the case where the ingester creates new L0 files
    // with the most recent data but there is a larger delay in new data arriving
    // and thus there is more overlap (40%) with the existing files
    //
    // This test simulates the case where we loop to compact N times, each time new M number of L0 files are created.
    // Each L0 file is 5MB and only overlaps 40% with the previously created L0 file.

    let n = 100;
    let m = 5;
    let mut idx = 0;
    for _i in 0..n {
        for _j in 0..m {
            let min = idx * 10;
            let max = min + 14;
            setup
                .partition
                .create_parquet_file(
                    parquet_builder()
                        .with_min_time(min)
                        .with_max_time(max)
                        .with_file_size_bytes(5 * ONE_MB)
                        .with_max_l0_created_at(Time::from_timestamp_nanos(idx))
                        .with_compaction_level(CompactionLevel::Initial),
                )
                .await;
            idx += 1;
        }
        run_layout_scenario(&setup).await;
    }

    // Add three L0 file during last compacting
    for _ in 0..3 {
        let min = idx * 10;
        let max = min + 14;
        setup
            .partition
            .create_parquet_file(
                parquet_builder()
                    .with_min_time(min)
                    .with_max_time(max)
                    .with_file_size_bytes(5 * ONE_MB)
                    .with_max_l0_created_at(Time::from_timestamp_nanos(idx))
                    .with_compaction_level(CompactionLevel::Initial),
            )
            .await;
        idx += 1;
    }

    let files = setup.list_by_table_not_to_delete().await;

    // Only the earliest avaialble L0 overlaps with the latest L2 file
    insta::assert_yaml_snapshot!(
        format_files("final output", &files),
        @r###"
    ---
    - final output
    - "L0                                                                                                                 "
    - "L0.751[5000,5014] 500ns 5mb                                                                                         |L0.751|"
    - "L0.752[5010,5024] 501ns 5mb                                                                                         |L0.752|"
    - "L0.753[5020,5034] 502ns 5mb                                                                                         |L0.753|"
    - "L2                                                                                                                 "
    - "L2.29[0,163] 19ns 80mb   |L2.29|                                                                                   "
    - "L2.59[164,356] 39ns 96mb   |L2.59|                                                                                 "
    - "L2.89[357,557] 59ns 100mb      |L2.89|                                                                             "
    - "L2.119[558,758] 79ns 100mb         |L2.119|                                                                         "
    - "L2.149[759,959] 99ns 100mb             |L2.149|                                                                     "
    - "L2.179[960,1160] 119ns 100mb                 |L2.179|                                                                 "
    - "L2.209[1161,1361] 139ns 100mb                    |L2.209|                                                              "
    - "L2.239[1362,1562] 159ns 100mb                        |L2.239|                                                          "
    - "L2.269[1563,1763] 179ns 100mb                           |L2.269|                                                       "
    - "L2.299[1764,1964] 199ns 101mb                               |L2.299|                                                   "
    - "L2.329[1965,2156] 219ns 96mb                                   |L2.329|                                               "
    - "L2.359[2157,2357] 239ns 101mb                                      |L2.359|                                            "
    - "L2.389[2358,2558] 259ns 101mb                                          |L2.389|                                        "
    - "L2.419[2559,2759] 279ns 101mb                                             |L2.419|                                     "
    - "L2.449[2760,2960] 299ns 101mb                                                 |L2.449|                                 "
    - "L2.479[2961,3161] 319ns 101mb                                                    |L2.479|                              "
    - "L2.509[3162,3362] 339ns 101mb                                                        |L2.509|                          "
    - "L2.539[3363,3563] 359ns 101mb                                                            |L2.539|                      "
    - "L2.569[3564,3764] 379ns 101mb                                                               |L2.569|                   "
    - "L2.599[3765,3956] 399ns 96mb                                                                   |L2.599|               "
    - "L2.629[3957,4157] 419ns 101mb                                                                      |L2.629|            "
    - "L2.659[4158,4358] 439ns 101mb                                                                          |L2.659|        "
    - "L2.689[4359,4559] 459ns 101mb                                                                             |L2.689|     "
    - "L2.719[4560,4760] 479ns 101mb                                                                                 |L2.719| "
    - "L2.749[4761,4961] 499ns 101mb                                                                                     |L2.749|"
    - "L2.750[4962,5004] 499ns 22mb                                                                                        |L2.750|"
    "###
    );
}

// Each L0 file overlaps ~40% time range  with its previously created L0 file.
// The setup has final files of level 2, level 1, and level 0.
// The level-1 files are not large enough to get compacted into L2 files
// The level-0 files are ingested during the last compaction
#[tokio::test]
async fn test_keep_ingesting_l0_files_40_percent_overlap_l1_left() {
    test_helpers::maybe_start_logging();
    let setup = layout_setup_builder().await.build().await;

    // This test simulates the case where we loop to compact N times, each time new M number of L0 files are created.
    // Each L0 file is 5MB and only overlaps 40% with the previously created L0 file.

    let n = 101;
    let m = 5;
    let mut idx = 0;
    let show_intermediate_result_runs = [0, 28, 45, 67, 89, 99];
    for i in 0..n {
        for _ in 0..m {
            let min = idx * 10;
            let max = min + 14;
            setup
                .partition
                .create_parquet_file(
                    parquet_builder()
                        .with_min_time(min)
                        .with_max_time(max)
                        .with_file_size_bytes(5 * ONE_MB)
                        .with_max_l0_created_at(Time::from_timestamp_nanos(idx))
                        .with_compaction_level(CompactionLevel::Initial),
                )
                .await;
            idx += 1;
        }

        // show intermediate reults for index i in show_intermediate_result_runs
        if i == show_intermediate_result_runs[0] {
            insta::assert_yaml_snapshot!(
                run_layout_scenario(&setup).await,
                @r###"
            ---
            - "**** Input Files "
            - "L0, all files 5mb                                                                                                  "
            - "L0.1[0,14] 0ns           |--------L0.1---------|                                                                   "
            - "L0.2[10,24] 1ns                          |--------L0.2---------|                                                   "
            - "L0.3[20,34] 2ns                                           |--------L0.3---------|                                  "
            - "L0.4[30,44] 3ns                                                            |--------L0.4---------|                 "
            - "L0.5[40,54] 4ns                                                                            |--------L0.5---------| "
            - "**** Simulation run 0, type=split(CompactAndSplitOutput(TotalSizeLessThanMaxCompactSize))(split_times=[43]). 5 Input Files, 25mb total:"
            - "L0, all files 5mb                                                                                                  "
            - "L0.5[40,54] 4ns                                                                            |--------L0.5---------| "
            - "L0.4[30,44] 3ns                                                            |--------L0.4---------|                 "
            - "L0.3[20,34] 2ns                                           |--------L0.3---------|                                  "
            - "L0.2[10,24] 1ns                          |--------L0.2---------|                                                   "
            - "L0.1[0,14] 0ns           |--------L0.1---------|                                                                   "
            - "**** 2 Output Files (parquet_file_id not yet assigned), 25mb total:"
            - "L1                                                                                                                 "
            - "L1.?[0,43] 4ns 20mb      |--------------------------------L1.?---------------------------------|                   "
            - "L1.?[44,54] 4ns 5mb                                                                               |-----L1.?-----| "
            - "Committing partition 1:"
            - "  Soft Deleting 5 files: L0.1, L0.2, L0.3, L0.4, L0.5"
            - "  Creating 2 files"
            - "**** Final Output Files (25mb written)"
            - "L1                                                                                                                 "
            - "L1.6[0,43] 4ns 20mb      |--------------------------------L1.6---------------------------------|                   "
            - "L1.7[44,54] 4ns 5mb                                                                               |-----L1.7-----| "
            "###
            );
        } else if i == show_intermediate_result_runs[1] {
            insta::assert_yaml_snapshot!(
                run_layout_scenario(&setup).await,
                @r###"
            ---
            - "**** Input Files "
            - "L0                                                                                                                 "
            - "L0.211[1400,1414] 140ns 5mb                                                                                      |L0.211|"
            - "L0.212[1410,1424] 141ns 5mb                                                                                       |L0.212|"
            - "L0.213[1420,1434] 142ns 5mb                                                                                       |L0.213|"
            - "L0.214[1430,1444] 143ns 5mb                                                                                        |L0.214|"
            - "L0.215[1440,1454] 144ns 5mb                                                                                         |L0.215|"
            - "L2                                                                                                                 "
            - "L2.29[0,163] 19ns 80mb   |-L2.29--|                                                                                "
            - "L2.59[164,356] 39ns 96mb           |--L2.59--|                                                                     "
            - "L2.89[357,557] 59ns 100mb                      |--L2.89---|                                                        "
            - "L2.119[558,758] 79ns 100mb                                  |--L2.119--|                                            "
            - "L2.149[759,959] 99ns 100mb                                              |--L2.149--|                                "
            - "L2.179[960,1160] 119ns 100mb                                                           |--L2.179--|                   "
            - "L2.209[1161,1361] 139ns 100mb                                                                       |--L2.209--|       "
            - "L2.210[1362,1404] 139ns 21mb                                                                                    |L2.210|"
            - "**** Simulation run 35, type=split(CompactAndSplitOutput(TotalSizeLessThanMaxCompactSize))(split_times=[1443]). 5 Input Files, 25mb total:"
            - "L0, all files 5mb                                                                                                  "
            - "L0.215[1440,1454] 144ns                                                                    |-------L0.215--------| "
            - "L0.214[1430,1444] 143ns                                                    |-------L0.214--------|                 "
            - "L0.213[1420,1434] 142ns                                   |-------L0.213--------|                                  "
            - "L0.212[1410,1424] 141ns                  |-------L0.212--------|                                                   "
            - "L0.211[1400,1414] 140ns  |-------L0.211--------|                                                                   "
            - "**** 2 Output Files (parquet_file_id not yet assigned), 25mb total:"
            - "L1                                                                                                                 "
            - "L1.?[1400,1443] 144ns 20mb|--------------------------------L1.?---------------------------------|                   "
            - "L1.?[1444,1454] 144ns 5mb                                                                         |-----L1.?-----| "
            - "Committing partition 1:"
            - "  Soft Deleting 5 files: L0.211, L0.212, L0.213, L0.214, L0.215"
            - "  Creating 2 files"
            - "**** Final Output Files (1.64gb written)"
            - "L1                                                                                                                 "
            - "L1.216[1400,1443] 144ns 20mb                                                                                      |L1.216|"
            - "L1.217[1444,1454] 144ns 5mb                                                                                         |L1.217|"
            - "L2                                                                                                                 "
            - "L2.29[0,163] 19ns 80mb   |-L2.29--|                                                                                "
            - "L2.59[164,356] 39ns 96mb           |--L2.59--|                                                                     "
            - "L2.89[357,557] 59ns 100mb                      |--L2.89---|                                                        "
            - "L2.119[558,758] 79ns 100mb                                  |--L2.119--|                                            "
            - "L2.149[759,959] 99ns 100mb                                              |--L2.149--|                                "
            - "L2.179[960,1160] 119ns 100mb                                                           |--L2.179--|                   "
            - "L2.209[1161,1361] 139ns 100mb                                                                       |--L2.209--|       "
            - "L2.210[1362,1404] 139ns 21mb                                                                                    |L2.210|"
            "###
            );
        } else if i == show_intermediate_result_runs[2] {
            insta::assert_yaml_snapshot!(
                run_layout_scenario(&setup).await,
                @r###"
            ---
            - "**** Input Files "
            - "L0                                                                                                                 "
            - "L0.338[2250,2264] 225ns 5mb                                                                                       |L0.338|"
            - "L0.339[2260,2274] 226ns 5mb                                                                                        |L0.339|"
            - "L0.340[2270,2284] 227ns 5mb                                                                                        |L0.340|"
            - "L0.341[2280,2294] 228ns 5mb                                                                                         |L0.341|"
            - "L0.342[2290,2304] 229ns 5mb                                                                                         |L0.342|"
            - "L1                                                                                                                 "
            - "L1.336[2200,2243] 224ns 20mb                                                                                     |L1.336|"
            - "L1.337[2244,2254] 224ns 5mb                                                                                       |L1.337|"
            - "L2                                                                                                                 "
            - "L2.29[0,163] 19ns 80mb   |L2.29|                                                                                   "
            - "L2.59[164,356] 39ns 96mb       |L2.59|                                                                             "
            - "L2.89[357,557] 59ns 100mb             |L2.89|                                                                      "
            - "L2.119[558,758] 79ns 100mb                     |L2.119|                                                             "
            - "L2.149[759,959] 99ns 100mb                             |L2.149|                                                     "
            - "L2.179[960,1160] 119ns 100mb                                     |L2.179|                                             "
            - "L2.209[1161,1361] 139ns 100mb                                             |L2.209|                                     "
            - "L2.239[1362,1562] 159ns 100mb                                                     |L2.239|                             "
            - "L2.269[1563,1763] 179ns 100mb                                                             |L2.269|                     "
            - "L2.299[1764,1964] 199ns 101mb                                                                    |L2.299|              "
            - "L2.329[1965,2156] 219ns 96mb                                                                            |L2.329|      "
            - "L2.330[2157,2204] 219ns 24mb                                                                                    |L2.330|"
            - "**** Simulation run 56, type=split(CompactAndSplitOutput(TotalSizeLessThanMaxCompactSize))(split_times=[2292]). 6 Input Files, 30mb total:"
            - "L0, all files 5mb                                                                                                  "
            - "L0.342[2290,2304] 229ns                                                                       |------L0.342-------|"
            - "L0.341[2280,2294] 228ns                                                        |------L0.341-------|               "
            - "L0.340[2270,2284] 227ns                                         |------L0.340-------|                              "
            - "L0.339[2260,2274] 226ns                          |------L0.339-------|                                             "
            - "L0.338[2250,2264] 225ns           |------L0.338-------|                                                            "
            - "L1, all files 5mb                                                                                                  "
            - "L1.337[2244,2254] 224ns  |---L1.337----|                                                                           "
            - "**** 2 Output Files (parquet_file_id not yet assigned), 30mb total:"
            - "L1                                                                                                                 "
            - "L1.?[2244,2292] 229ns 24mb|---------------------------------L1.?---------------------------------|                  "
            - "L1.?[2293,2304] 229ns 6mb                                                                         |-----L1.?-----| "
            - "Committing partition 1:"
            - "  Soft Deleting 6 files: L1.337, L0.338, L0.339, L0.340, L0.341, L0.342"
            - "  Creating 2 files"
            - "**** Final Output Files (2.6gb written)"
            - "L1                                                                                                                 "
            - "L1.336[2200,2243] 224ns 20mb                                                                                     |L1.336|"
            - "L1.343[2244,2292] 229ns 24mb                                                                                       |L1.343|"
            - "L1.344[2293,2304] 229ns 6mb                                                                                         |L1.344|"
            - "L2                                                                                                                 "
            - "L2.29[0,163] 19ns 80mb   |L2.29|                                                                                   "
            - "L2.59[164,356] 39ns 96mb       |L2.59|                                                                             "
            - "L2.89[357,557] 59ns 100mb             |L2.89|                                                                      "
            - "L2.119[558,758] 79ns 100mb                     |L2.119|                                                             "
            - "L2.149[759,959] 99ns 100mb                             |L2.149|                                                     "
            - "L2.179[960,1160] 119ns 100mb                                     |L2.179|                                             "
            - "L2.209[1161,1361] 139ns 100mb                                             |L2.209|                                     "
            - "L2.239[1362,1562] 159ns 100mb                                                     |L2.239|                             "
            - "L2.269[1563,1763] 179ns 100mb                                                             |L2.269|                     "
            - "L2.299[1764,1964] 199ns 101mb                                                                    |L2.299|              "
            - "L2.329[1965,2156] 219ns 96mb                                                                            |L2.329|      "
            - "L2.330[2157,2204] 219ns 24mb                                                                                    |L2.330|"
            "###
            );
        } else if i == show_intermediate_result_runs[3] {
            insta::assert_yaml_snapshot!(
                run_layout_scenario(&setup).await,
                @r###"
            ---
            - "**** Input Files "
            - "L0                                                                                                                 "
            - "L0.502[3350,3364] 335ns 5mb                                                                                        |L0.502|"
            - "L0.503[3360,3374] 336ns 5mb                                                                                        |L0.503|"
            - "L0.504[3370,3384] 337ns 5mb                                                                                         |L0.504|"
            - "L0.505[3380,3394] 338ns 5mb                                                                                         |L0.505|"
            - "L0.506[3390,3404] 339ns 5mb                                                                                         |L0.506|"
            - "L1                                                                                                                 "
            - "L1.486[3200,3243] 324ns 20mb                                                                                    |L1.486|"
            - "L1.493[3244,3292] 329ns 24mb                                                                                     |L1.493|"
            - "L1.500[3293,3341] 334ns 24mb                                                                                       |L1.500|"
            - "L1.501[3342,3354] 334ns 6mb                                                                                        |L1.501|"
            - "L2                                                                                                                 "
            - "L2.29[0,163] 19ns 80mb   |L2.29|                                                                                   "
            - "L2.59[164,356] 39ns 96mb     |L2.59|                                                                               "
            - "L2.89[357,557] 59ns 100mb         |L2.89|                                                                          "
            - "L2.119[558,758] 79ns 100mb              |L2.119|                                                                    "
            - "L2.149[759,959] 99ns 100mb                    |L2.149|                                                              "
            - "L2.179[960,1160] 119ns 100mb                         |L2.179|                                                         "
            - "L2.209[1161,1361] 139ns 100mb                              |L2.209|                                                    "
            - "L2.239[1362,1562] 159ns 100mb                                    |L2.239|                                              "
            - "L2.269[1563,1763] 179ns 100mb                                         |L2.269|                                         "
            - "L2.299[1764,1964] 199ns 101mb                                              |L2.299|                                    "
            - "L2.329[1965,2156] 219ns 96mb                                                   |L2.329|                               "
            - "L2.359[2157,2357] 239ns 101mb                                                         |L2.359|                         "
            - "L2.389[2358,2558] 259ns 101mb                                                              |L2.389|                    "
            - "L2.419[2559,2759] 279ns 101mb                                                                   |L2.419|               "
            - "L2.449[2760,2960] 299ns 101mb                                                                        |L2.449|          "
            - "L2.479[2961,3161] 319ns 101mb                                                                              |L2.479|    "
            - "L2.480[3162,3204] 319ns 22mb                                                                                   |L2.480|"
            - "**** Simulation run 83, type=split(CompactAndSplitOutput(TotalSizeLessThanMaxCompactSize))(split_times=[3391]). 6 Input Files, 31mb total:"
            - "L0                                                                                                                 "
            - "L0.506[3390,3404] 339ns 5mb                                                                     |------L0.506------| "
            - "L0.505[3380,3394] 338ns 5mb                                                       |------L0.505------|               "
            - "L0.504[3370,3384] 337ns 5mb                                        |------L0.504------|                              "
            - "L0.503[3360,3374] 336ns 5mb                          |------L0.503------|                                            "
            - "L0.502[3350,3364] 335ns 5mb           |------L0.502------|                                                           "
            - "L1                                                                                                                 "
            - "L1.501[3342,3354] 334ns 6mb|----L1.501-----|                                                                         "
            - "**** 2 Output Files (parquet_file_id not yet assigned), 31mb total:"
            - "L1                                                                                                                 "
            - "L1.?[3342,3391] 339ns 25mb|--------------------------------L1.?---------------------------------|                   "
            - "L1.?[3392,3404] 339ns 6mb                                                                        |-----L1.?------| "
            - "Committing partition 1:"
            - "  Soft Deleting 6 files: L1.501, L0.502, L0.503, L0.504, L0.505, L0.506"
            - "  Creating 2 files"
            - "**** Simulation run 84, type=split(CompactAndSplitOutput(TotalSizeLessThanMaxCompactSize))(split_times=[3362]). 6 Input Files, 122mb total:"
            - "L1                                                                                                                 "
            - "L1.508[3392,3404] 339ns 6mb                                                                                     |L1.508|"
            - "L1.507[3342,3391] 339ns 25mb                                                                  |-----L1.507-----|      "
            - "L1.500[3293,3341] 334ns 24mb                                                |----L1.500-----|                         "
            - "L1.493[3244,3292] 329ns 24mb                              |----L1.493-----|                                           "
            - "L1.486[3200,3243] 324ns 20mb              |---L1.486----|                                                             "
            - "L2                                                                                                                 "
            - "L2.480[3162,3204] 319ns 22mb|---L2.480----|                                                                           "
            - "**** 2 Output Files (parquet_file_id not yet assigned), 122mb total:"
            - "L2                                                                                                                 "
            - "L2.?[3162,3362] 339ns 101mb|----------------------------------L2.?----------------------------------|                "
            - "L2.?[3363,3404] 339ns 21mb                                                                          |----L2.?-----| "
            - "Committing partition 1:"
            - "  Soft Deleting 6 files: L2.480, L1.486, L1.493, L1.500, L1.507, L1.508"
            - "  Creating 2 files"
            - "**** Final Output Files (3.95gb written)"
            - "L2                                                                                                                 "
            - "L2.29[0,163] 19ns 80mb   |L2.29|                                                                                   "
            - "L2.59[164,356] 39ns 96mb     |L2.59|                                                                               "
            - "L2.89[357,557] 59ns 100mb         |L2.89|                                                                          "
            - "L2.119[558,758] 79ns 100mb              |L2.119|                                                                    "
            - "L2.149[759,959] 99ns 100mb                    |L2.149|                                                              "
            - "L2.179[960,1160] 119ns 100mb                         |L2.179|                                                         "
            - "L2.209[1161,1361] 139ns 100mb                              |L2.209|                                                    "
            - "L2.239[1362,1562] 159ns 100mb                                    |L2.239|                                              "
            - "L2.269[1563,1763] 179ns 100mb                                         |L2.269|                                         "
            - "L2.299[1764,1964] 199ns 101mb                                              |L2.299|                                    "
            - "L2.329[1965,2156] 219ns 96mb                                                   |L2.329|                               "
            - "L2.359[2157,2357] 239ns 101mb                                                         |L2.359|                         "
            - "L2.389[2358,2558] 259ns 101mb                                                              |L2.389|                    "
            - "L2.419[2559,2759] 279ns 101mb                                                                   |L2.419|               "
            - "L2.449[2760,2960] 299ns 101mb                                                                        |L2.449|          "
            - "L2.479[2961,3161] 319ns 101mb                                                                              |L2.479|    "
            - "L2.509[3162,3362] 339ns 101mb                                                                                   |L2.509|"
            - "L2.510[3363,3404] 339ns 21mb                                                                                        |L2.510|"
            "###
            );
        } else if i == show_intermediate_result_runs[4] {
            insta::assert_yaml_snapshot!(
                run_layout_scenario(&setup).await,
                @r###"
            ---
            - "**** Input Files "
            - "L0                                                                                                                 "
            - "L0.668[4450,4464] 445ns 5mb                                                                                        |L0.668|"
            - "L0.669[4460,4474] 446ns 5mb                                                                                         |L0.669|"
            - "L0.670[4470,4484] 447ns 5mb                                                                                         |L0.670|"
            - "L0.671[4480,4494] 448ns 5mb                                                                                         |L0.671|"
            - "L0.672[4490,4504] 449ns 5mb                                                                                         |L0.672|"
            - "L1                                                                                                                 "
            - "L1.666[4400,4443] 444ns 20mb                                                                                       |L1.666|"
            - "L1.667[4444,4454] 444ns 5mb                                                                                        |L1.667|"
            - "L2                                                                                                                 "
            - "L2.29[0,163] 19ns 80mb   |L2.29|                                                                                   "
            - "L2.59[164,356] 39ns 96mb    |L2.59|                                                                                "
            - "L2.89[357,557] 59ns 100mb       |L2.89|                                                                            "
            - "L2.119[558,758] 79ns 100mb           |L2.119|                                                                       "
            - "L2.149[759,959] 99ns 100mb               |L2.149|                                                                   "
            - "L2.179[960,1160] 119ns 100mb                   |L2.179|                                                               "
            - "L2.209[1161,1361] 139ns 100mb                       |L2.209|                                                           "
            - "L2.239[1362,1562] 159ns 100mb                           |L2.239|                                                       "
            - "L2.269[1563,1763] 179ns 100mb                               |L2.269|                                                   "
            - "L2.299[1764,1964] 199ns 101mb                                   |L2.299|                                               "
            - "L2.329[1965,2156] 219ns 96mb                                       |L2.329|                                           "
            - "L2.359[2157,2357] 239ns 101mb                                           |L2.359|                                       "
            - "L2.389[2358,2558] 259ns 101mb                                               |L2.389|                                   "
            - "L2.419[2559,2759] 279ns 101mb                                                   |L2.419|                               "
            - "L2.449[2760,2960] 299ns 101mb                                                       |L2.449|                           "
            - "L2.479[2961,3161] 319ns 101mb                                                           |L2.479|                       "
            - "L2.509[3162,3362] 339ns 101mb                                                               |L2.509|                   "
            - "L2.539[3363,3563] 359ns 101mb                                                                   |L2.539|               "
            - "L2.569[3564,3764] 379ns 101mb                                                                       |L2.569|           "
            - "L2.599[3765,3956] 399ns 96mb                                                                           |L2.599|       "
            - "L2.629[3957,4157] 419ns 101mb                                                                               |L2.629|   "
            - "L2.659[4158,4358] 439ns 101mb                                                                                   |L2.659|"
            - "L2.660[4359,4404] 439ns 23mb                                                                                       |L2.660|"
            - "**** Simulation run 111, type=split(CompactAndSplitOutput(TotalSizeLessThanMaxCompactSize))(split_times=[4492]). 6 Input Files, 30mb total:"
            - "L0, all files 5mb                                                                                                  "
            - "L0.672[4490,4504] 449ns                                                                       |------L0.672-------|"
            - "L0.671[4480,4494] 448ns                                                        |------L0.671-------|               "
            - "L0.670[4470,4484] 447ns                                         |------L0.670-------|                              "
            - "L0.669[4460,4474] 446ns                          |------L0.669-------|                                             "
            - "L0.668[4450,4464] 445ns           |------L0.668-------|                                                            "
            - "L1, all files 5mb                                                                                                  "
            - "L1.667[4444,4454] 444ns  |---L1.667----|                                                                           "
            - "**** 2 Output Files (parquet_file_id not yet assigned), 30mb total:"
            - "L1                                                                                                                 "
            - "L1.?[4444,4492] 449ns 24mb|---------------------------------L1.?---------------------------------|                  "
            - "L1.?[4493,4504] 449ns 6mb                                                                         |-----L1.?-----| "
            - "Committing partition 1:"
            - "  Soft Deleting 6 files: L1.667, L0.668, L0.669, L0.670, L0.671, L0.672"
            - "  Creating 2 files"
            - "**** Final Output Files (5.18gb written)"
            - "L1                                                                                                                 "
            - "L1.666[4400,4443] 444ns 20mb                                                                                       |L1.666|"
            - "L1.673[4444,4492] 449ns 24mb                                                                                        |L1.673|"
            - "L1.674[4493,4504] 449ns 6mb                                                                                         |L1.674|"
            - "L2                                                                                                                 "
            - "L2.29[0,163] 19ns 80mb   |L2.29|                                                                                   "
            - "L2.59[164,356] 39ns 96mb    |L2.59|                                                                                "
            - "L2.89[357,557] 59ns 100mb       |L2.89|                                                                            "
            - "L2.119[558,758] 79ns 100mb           |L2.119|                                                                       "
            - "L2.149[759,959] 99ns 100mb               |L2.149|                                                                   "
            - "L2.179[960,1160] 119ns 100mb                   |L2.179|                                                               "
            - "L2.209[1161,1361] 139ns 100mb                       |L2.209|                                                           "
            - "L2.239[1362,1562] 159ns 100mb                           |L2.239|                                                       "
            - "L2.269[1563,1763] 179ns 100mb                               |L2.269|                                                   "
            - "L2.299[1764,1964] 199ns 101mb                                   |L2.299|                                               "
            - "L2.329[1965,2156] 219ns 96mb                                       |L2.329|                                           "
            - "L2.359[2157,2357] 239ns 101mb                                           |L2.359|                                       "
            - "L2.389[2358,2558] 259ns 101mb                                               |L2.389|                                   "
            - "L2.419[2559,2759] 279ns 101mb                                                   |L2.419|                               "
            - "L2.449[2760,2960] 299ns 101mb                                                       |L2.449|                           "
            - "L2.479[2961,3161] 319ns 101mb                                                           |L2.479|                       "
            - "L2.509[3162,3362] 339ns 101mb                                                               |L2.509|                   "
            - "L2.539[3363,3563] 359ns 101mb                                                                   |L2.539|               "
            - "L2.569[3564,3764] 379ns 101mb                                                                       |L2.569|           "
            - "L2.599[3765,3956] 399ns 96mb                                                                           |L2.599|       "
            - "L2.629[3957,4157] 419ns 101mb                                                                               |L2.629|   "
            - "L2.659[4158,4358] 439ns 101mb                                                                                   |L2.659|"
            - "L2.660[4359,4404] 439ns 23mb                                                                                       |L2.660|"
            "###
            );
        } else if i == show_intermediate_result_runs[5] {
            insta::assert_yaml_snapshot!(
                run_layout_scenario(&setup).await,
                @r###"
            ---
            - "**** Input Files "
            - "L0                                                                                                                 "
            - "L0.742[4950,4964] 495ns 5mb                                                                                         |L0.742|"
            - "L0.743[4960,4974] 496ns 5mb                                                                                         |L0.743|"
            - "L0.744[4970,4984] 497ns 5mb                                                                                         |L0.744|"
            - "L0.745[4980,4994] 498ns 5mb                                                                                         |L0.745|"
            - "L0.746[4990,5004] 499ns 5mb                                                                                         |L0.746|"
            - "L1                                                                                                                 "
            - "L1.726[4800,4843] 484ns 20mb                                                                                      |L1.726|"
            - "L1.733[4844,4892] 489ns 24mb                                                                                       |L1.733|"
            - "L1.740[4893,4941] 494ns 24mb                                                                                        |L1.740|"
            - "L1.741[4942,4954] 494ns 6mb                                                                                        |L1.741|"
            - "L2                                                                                                                 "
            - "L2.29[0,163] 19ns 80mb   |L2.29|                                                                                   "
            - "L2.59[164,356] 39ns 96mb   |L2.59|                                                                                 "
            - "L2.89[357,557] 59ns 100mb      |L2.89|                                                                             "
            - "L2.119[558,758] 79ns 100mb          |L2.119|                                                                        "
            - "L2.149[759,959] 99ns 100mb             |L2.149|                                                                     "
            - "L2.179[960,1160] 119ns 100mb                 |L2.179|                                                                 "
            - "L2.209[1161,1361] 139ns 100mb                    |L2.209|                                                              "
            - "L2.239[1362,1562] 159ns 100mb                        |L2.239|                                                          "
            - "L2.269[1563,1763] 179ns 100mb                            |L2.269|                                                      "
            - "L2.299[1764,1964] 199ns 101mb                               |L2.299|                                                   "
            - "L2.329[1965,2156] 219ns 96mb                                   |L2.329|                                               "
            - "L2.359[2157,2357] 239ns 101mb                                      |L2.359|                                            "
            - "L2.389[2358,2558] 259ns 101mb                                          |L2.389|                                        "
            - "L2.419[2559,2759] 279ns 101mb                                              |L2.419|                                    "
            - "L2.449[2760,2960] 299ns 101mb                                                 |L2.449|                                 "
            - "L2.479[2961,3161] 319ns 101mb                                                     |L2.479|                             "
            - "L2.509[3162,3362] 339ns 101mb                                                        |L2.509|                          "
            - "L2.539[3363,3563] 359ns 101mb                                                            |L2.539|                      "
            - "L2.569[3564,3764] 379ns 101mb                                                                |L2.569|                  "
            - "L2.599[3765,3956] 399ns 96mb                                                                   |L2.599|               "
            - "L2.629[3957,4157] 419ns 101mb                                                                       |L2.629|           "
            - "L2.659[4158,4358] 439ns 101mb                                                                          |L2.659|        "
            - "L2.689[4359,4559] 459ns 101mb                                                                              |L2.689|    "
            - "L2.719[4560,4760] 479ns 101mb                                                                                  |L2.719|"
            - "L2.720[4761,4804] 479ns 22mb                                                                                     |L2.720|"
            - "**** Simulation run 123, type=split(CompactAndSplitOutput(TotalSizeLessThanMaxCompactSize))(split_times=[4991]). 6 Input Files, 31mb total:"
            - "L0                                                                                                                 "
            - "L0.746[4990,5004] 499ns 5mb                                                                     |------L0.746------| "
            - "L0.745[4980,4994] 498ns 5mb                                                       |------L0.745------|               "
            - "L0.744[4970,4984] 497ns 5mb                                        |------L0.744------|                              "
            - "L0.743[4960,4974] 496ns 5mb                          |------L0.743------|                                            "
            - "L0.742[4950,4964] 495ns 5mb           |------L0.742------|                                                           "
            - "L1                                                                                                                 "
            - "L1.741[4942,4954] 494ns 6mb|----L1.741-----|                                                                         "
            - "**** 2 Output Files (parquet_file_id not yet assigned), 31mb total:"
            - "L1                                                                                                                 "
            - "L1.?[4942,4991] 499ns 25mb|--------------------------------L1.?---------------------------------|                   "
            - "L1.?[4992,5004] 499ns 6mb                                                                        |-----L1.?------| "
            - "Committing partition 1:"
            - "  Soft Deleting 6 files: L1.741, L0.742, L0.743, L0.744, L0.745, L0.746"
            - "  Creating 2 files"
            - "**** Simulation run 124, type=split(CompactAndSplitOutput(TotalSizeLessThanMaxCompactSize))(split_times=[4961]). 6 Input Files, 122mb total:"
            - "L1                                                                                                                 "
            - "L1.748[4992,5004] 499ns 6mb                                                                                     |L1.748|"
            - "L1.747[4942,4991] 499ns 25mb                                                                   |-----L1.747-----|     "
            - "L1.740[4893,4941] 494ns 24mb                                                |----L1.740-----|                         "
            - "L1.733[4844,4892] 489ns 24mb                              |----L1.733-----|                                           "
            - "L1.726[4800,4843] 484ns 20mb              |---L1.726----|                                                             "
            - "L2                                                                                                                 "
            - "L2.720[4761,4804] 479ns 22mb|---L2.720----|                                                                           "
            - "**** 2 Output Files (parquet_file_id not yet assigned), 122mb total:"
            - "L2                                                                                                                 "
            - "L2.?[4761,4961] 499ns 101mb|----------------------------------L2.?----------------------------------|                "
            - "L2.?[4962,5004] 499ns 22mb                                                                          |----L2.?-----| "
            - "Committing partition 1:"
            - "  Soft Deleting 6 files: L2.720, L1.726, L1.733, L1.740, L1.747, L1.748"
            - "  Creating 2 files"
            - "**** Final Output Files (5.83gb written)"
            - "L2                                                                                                                 "
            - "L2.29[0,163] 19ns 80mb   |L2.29|                                                                                   "
            - "L2.59[164,356] 39ns 96mb   |L2.59|                                                                                 "
            - "L2.89[357,557] 59ns 100mb      |L2.89|                                                                             "
            - "L2.119[558,758] 79ns 100mb          |L2.119|                                                                        "
            - "L2.149[759,959] 99ns 100mb             |L2.149|                                                                     "
            - "L2.179[960,1160] 119ns 100mb                 |L2.179|                                                                 "
            - "L2.209[1161,1361] 139ns 100mb                    |L2.209|                                                              "
            - "L2.239[1362,1562] 159ns 100mb                        |L2.239|                                                          "
            - "L2.269[1563,1763] 179ns 100mb                            |L2.269|                                                      "
            - "L2.299[1764,1964] 199ns 101mb                               |L2.299|                                                   "
            - "L2.329[1965,2156] 219ns 96mb                                   |L2.329|                                               "
            - "L2.359[2157,2357] 239ns 101mb                                      |L2.359|                                            "
            - "L2.389[2358,2558] 259ns 101mb                                          |L2.389|                                        "
            - "L2.419[2559,2759] 279ns 101mb                                              |L2.419|                                    "
            - "L2.449[2760,2960] 299ns 101mb                                                 |L2.449|                                 "
            - "L2.479[2961,3161] 319ns 101mb                                                     |L2.479|                             "
            - "L2.509[3162,3362] 339ns 101mb                                                        |L2.509|                          "
            - "L2.539[3363,3563] 359ns 101mb                                                            |L2.539|                      "
            - "L2.569[3564,3764] 379ns 101mb                                                                |L2.569|                  "
            - "L2.599[3765,3956] 399ns 96mb                                                                   |L2.599|               "
            - "L2.629[3957,4157] 419ns 101mb                                                                       |L2.629|           "
            - "L2.659[4158,4358] 439ns 101mb                                                                          |L2.659|        "
            - "L2.689[4359,4559] 459ns 101mb                                                                              |L2.689|    "
            - "L2.719[4560,4760] 479ns 101mb                                                                                  |L2.719|"
            - "L2.749[4761,4961] 499ns 101mb                                                                                     |L2.749|"
            - "L2.750[4962,5004] 499ns 22mb                                                                                         |L2.750|"
            "###
            );
        } else {
            run_layout_scenario(&setup).await;
        }
    }

    // Add three L0 file during last compacting
    for _ in 0..3 {
        let min = idx * 10;
        let max = min + 14;
        setup
            .partition
            .create_parquet_file(
                parquet_builder()
                    .with_min_time(min)
                    .with_max_time(max)
                    .with_file_size_bytes(5 * ONE_MB)
                    .with_max_l0_created_at(Time::from_timestamp_nanos(idx))
                    .with_compaction_level(CompactionLevel::Initial),
            )
            .await;
        idx += 1;
    }

    let files = setup.list_by_table_not_to_delete().await;

    // Final results
    // With time overlapped setup (common use case), there is always:
    //   . Only the earliest avaialble L0 overlaps with the latest L1 file
    //   . Only the earliest avaialble L1 overlaps with the latest L2 file
    insta::assert_yaml_snapshot!(
        format_files("final output", &files),
        @r###"
    ---
    - final output
    - "L0                                                                                                                 "
    - "L0.758[5050,5064] 505ns 5mb                                                                                         |L0.758|"
    - "L0.759[5060,5074] 506ns 5mb                                                                                         |L0.759|"
    - "L0.760[5070,5084] 507ns 5mb                                                                                         |L0.760|"
    - "L1                                                                                                                 "
    - "L1.756[5000,5043] 504ns 20mb                                                                                        |L1.756|"
    - "L1.757[5044,5054] 504ns 5mb                                                                                         |L1.757|"
    - "L2                                                                                                                 "
    - "L2.29[0,163] 19ns 80mb   |L2.29|                                                                                   "
    - "L2.59[164,356] 39ns 96mb   |L2.59|                                                                                 "
    - "L2.89[357,557] 59ns 100mb      |L2.89|                                                                             "
    - "L2.119[558,758] 79ns 100mb         |L2.119|                                                                         "
    - "L2.149[759,959] 99ns 100mb             |L2.149|                                                                     "
    - "L2.179[960,1160] 119ns 100mb                |L2.179|                                                                  "
    - "L2.209[1161,1361] 139ns 100mb                    |L2.209|                                                              "
    - "L2.239[1362,1562] 159ns 100mb                        |L2.239|                                                          "
    - "L2.269[1563,1763] 179ns 100mb                           |L2.269|                                                       "
    - "L2.299[1764,1964] 199ns 101mb                               |L2.299|                                                   "
    - "L2.329[1965,2156] 219ns 96mb                                  |L2.329|                                                "
    - "L2.359[2157,2357] 239ns 101mb                                      |L2.359|                                            "
    - "L2.389[2358,2558] 259ns 101mb                                         |L2.389|                                         "
    - "L2.419[2559,2759] 279ns 101mb                                             |L2.419|                                     "
    - "L2.449[2760,2960] 299ns 101mb                                                |L2.449|                                  "
    - "L2.479[2961,3161] 319ns 101mb                                                    |L2.479|                              "
    - "L2.509[3162,3362] 339ns 101mb                                                       |L2.509|                           "
    - "L2.539[3363,3563] 359ns 101mb                                                           |L2.539|                       "
    - "L2.569[3564,3764] 379ns 101mb                                                               |L2.569|                   "
    - "L2.599[3765,3956] 399ns 96mb                                                                  |L2.599|                "
    - "L2.629[3957,4157] 419ns 101mb                                                                      |L2.629|            "
    - "L2.659[4158,4358] 439ns 101mb                                                                         |L2.659|         "
    - "L2.689[4359,4559] 459ns 101mb                                                                             |L2.689|     "
    - "L2.719[4560,4760] 479ns 101mb                                                                                |L2.719|  "
    - "L2.749[4761,4961] 499ns 101mb                                                                                    |L2.749|"
    - "L2.750[4962,5004] 499ns 22mb                                                                                       |L2.750|"
    "###
    );
}

// Each L0 file overlaps ~40% time range  with its previously created L0 file.
// The setup has final files of level 2, level 1, and level 0.
// The level-1 files are not large enough to get compacted into L2 files
// The level-0 files are ingested during the last compaction
#[tokio::test]
async fn test_keep_ingesting_l0_files_40_percent_overlap_output_250mb() {
    test_helpers::maybe_start_logging();
    let setup = layout_setup_builder().await.build().await;

    // This test simulates the case where the ingester creates new L0 files
    // with the most recent data but there is a larger delay in new data arriving
    // and thus there is more overlap (40%) with the existing files

    // Loop to compact N times, each time new M number of L0 files are created.
    // Each L0 file is 5MB and only overlaps 40% with the previously created L0 file.

    let n = 10;
    let m = 5;
    let mut idx = 0;
    for _i in 0..n {
        for _j in 0..m {
            let min = idx * 10;
            let max = min + 14;
            setup
                .partition
                .create_parquet_file(
                    parquet_builder()
                        .with_min_time(min)
                        .with_max_time(max)
                        .with_file_size_bytes(5 * ONE_MB)
                        .with_max_l0_created_at(Time::from_timestamp_nanos(idx))
                        .with_compaction_level(CompactionLevel::Initial),
                )
                .await;
            idx += 1;
        }
        run_layout_scenario(&setup).await;
    }

    // Add three L0 file during last compacting
    for _ in 0..3 {
        let min = idx * 10;
        let max = min + 14;
        setup
            .partition
            .create_parquet_file(
                parquet_builder()
                    .with_min_time(min)
                    .with_max_time(max)
                    .with_file_size_bytes(5 * ONE_MB)
                    .with_max_l0_created_at(Time::from_timestamp_nanos(idx))
                    .with_compaction_level(CompactionLevel::Initial),
            )
            .await;
        idx += 1;
    }

    let files = setup.list_by_table_not_to_delete().await;

    // Only the earliest avaialble L0 overlaps with the latest L2 file
    insta::assert_yaml_snapshot!(
        format_files("final output", &files),
        @r###"
    ---
    - final output
    - "L0                                                                                                                 "
    - "L0.75[500,514] 50ns 5mb                                                                                      |L0.75|"
    - "L0.76[510,524] 51ns 5mb                                                                                       |L0.76|"
    - "L0.77[520,534] 52ns 5mb                                                                                         |L0.77|"
    - "L1                                                                                                                 "
    - "L1.66[400,443] 44ns 20mb                                                                    |L1.66|                "
    - "L1.73[444,492] 49ns 24mb                                                                           |L1.73-|        "
    - "L1.74[493,504] 49ns 6mb                                                                                     |L1.74|"
    - "L2                                                                                                                 "
    - "L2.29[0,163] 19ns 80mb   |----------L2.29----------|                                                               "
    - "L2.59[164,356] 39ns 96mb                            |------------L2.59-------------|                               "
    - "L2.60[357,404] 39ns 24mb                                                             |L2.60|                       "
    "###
    );
}

// The (assumed) most common ingest pattern is a leading edge, overlapping pattern. All data is written approximately
// at the time of the data, but with enough variance to cause overlaps between L0s.  Those overlaps mean the first in
// any sequence of L0s overlaps the L1 from the previously compacted L0s.
// This test case covers a slightly different scenario.
// Instead, imagine if the incoming data's timestamps have a courrse granularity (every few seconds, minute, etc).
// This can produce a leading edge non-overlapping pattern in the L0s, which means the L0s won't overlap their prior
// L1.  This ingest pattern has a tendency to leave small L1s.  If the partition gets enough data, most of them will
// eventually get compacted.  But if the partition is small, it may be left with several small L1s that should be
// compacted together. This test case is an incremental simulation of that scenario.
// We can't just create a big backlog of non-overlapping L0s because they'll be compacted all at once and not relfect
// behavior from a compactor that keeps up with the non-overlapping L0s as they arrive.  So this test case creates one
// small L1 representing a prior compaction's output, and just enough L0s to trigger a compaction.  The purpose of
// this test is to ensure the prior small L1 is not left behind.
#[tokio::test]
async fn test_tiny_ingest_non_overlapping() {
    test_helpers::maybe_start_logging();
    let setup = layout_setup_builder().await.build().await;

    // Small L1 simumlating output from prior compaction
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(0)
                .with_max_time(5)
                .with_file_size_bytes(20000)
                .with_max_l0_created_at(Time::from_timestamp_nanos(6))
                .with_compaction_level(CompactionLevel::FileNonOverlapped),
        )
        .await;

    // Add two L0 file
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(10)
                .with_max_time(19)
                .with_file_size_bytes(2000)
                .with_max_l0_created_at(Time::from_timestamp_nanos(20))
                .with_compaction_level(CompactionLevel::Initial),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(20)
                .with_max_time(29)
                .with_file_size_bytes(2000)
                .with_max_l0_created_at(Time::from_timestamp_nanos(30))
                .with_compaction_level(CompactionLevel::Initial),
        )
        .await;

    run_layout_scenario(&setup).await;
    let files = setup.list_by_table_not_to_delete().await;

    insta::assert_yaml_snapshot!(
        format_files("final output", &files),
        @r###"
    ---
    - final output
    - "L1, all files 23kb                                                                                                 "
    - "L1.4[0,29] 30ns          |------------------------------------------L1.4------------------------------------------|"
    "###
    );
}
