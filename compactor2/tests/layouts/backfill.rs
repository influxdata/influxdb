//! layout tests for scenarios with large numbers of files
//!
//! See [crate::layout] module for detailed documentation

use data_types::CompactionLevel;
use iox_time::Time;
use std::time::Duration;

use crate::layouts::{layout_setup_builder, parquet_builder, run_layout_scenario, ONE_MB};

const MAX_DESIRED_FILE_SIZE: u64 = 100 * ONE_MB;

// This case simulates a backfill scenario with no existing data prior to the start of backfill.
//   - the customer starts backfilling yesterday's data, writing at random times spread across the day.
// The result:
//   - We get many L0s that each cover much of the day.
#[tokio::test]
async fn random_backfill_empty_partition() {
    test_helpers::maybe_start_logging();

    let setup = layout_setup_builder()
        .await
        .with_max_desired_file_size_bytes(MAX_DESIRED_FILE_SIZE)
        .with_max_num_files_per_plan(200)
        .build()
        .await;

    let num_tiny_l0_files = 50;
    let l0_size = MAX_DESIRED_FILE_SIZE / 10;

    // Assume the "day" is 1000 units of time, and spread the L0s across that
    for i in 0..num_tiny_l0_files {
        let i = i as i64;

        // Create a bit of variety in the start/stop times, but mostly they cover most of the day.
        let mut start_time = 50;
        let mut end_time = 950;
        match i % 4 {
            0 => {
                start_time += 26;
                end_time -= 18;
            }
            1 => {
                start_time -= 8;
                end_time += 36;
            }
            2 => {
                start_time += 123;
            }
            3 => {
                end_time -= 321;
            }
            _ => {}
        }

        setup
            .partition
            .create_parquet_file(
                parquet_builder()
                    .with_min_time(start_time)
                    .with_max_time(end_time)
                    .with_compaction_level(CompactionLevel::Initial)
                    .with_file_size_bytes(l0_size)
                    .with_max_l0_created_at(Time::from_timestamp_nanos(i + 1000)), // These files are created sequentially "today" with "yesterday's" data
            )
            .await;
    }

    // Add an extra file that doesn't overlap anything. Since this test case exercises high_l0_overlap_split, including an l0 file that overlaps nothing,
    // exercises a code path in high_l0_overlap_split where a file has no overlaps.
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(0)
                .with_max_time(1)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(l0_size)
                .with_max_l0_created_at(Time::from_timestamp_nanos(999)),
        )
        .await;

    insta::assert_yaml_snapshot!(
        run_layout_scenario(&setup).await,
        @r###"
    ---
    - "**** Input Files "
    - "L0, all files 10mb                                                                                                 "
    - "L0.1[76,932] 1us               |------------------------------------L0.1------------------------------------|      "
    - "L0.2[42,986] 1us            |----------------------------------------L0.2----------------------------------------| "
    - "L0.3[173,950] 1us                       |--------------------------------L0.3--------------------------------|     "
    - "L0.4[50,629] 1us             |-----------------------L0.4-----------------------|                                  "
    - "L0.5[76,932] 1us               |------------------------------------L0.5------------------------------------|      "
    - "L0.6[42,986] 1us            |----------------------------------------L0.6----------------------------------------| "
    - "L0.7[173,950] 1.01us                    |--------------------------------L0.7--------------------------------|     "
    - "L0.8[50,629] 1.01us          |-----------------------L0.8-----------------------|                                  "
    - "L0.9[76,932] 1.01us            |------------------------------------L0.9------------------------------------|      "
    - "L0.10[42,986] 1.01us        |---------------------------------------L0.10----------------------------------------| "
    - "L0.11[173,950] 1.01us                   |-------------------------------L0.11--------------------------------|     "
    - "L0.12[50,629] 1.01us         |----------------------L0.12-----------------------|                                  "
    - "L0.13[76,932] 1.01us           |-----------------------------------L0.13------------------------------------|      "
    - "L0.14[42,986] 1.01us        |---------------------------------------L0.14----------------------------------------| "
    - "L0.15[173,950] 1.01us                   |-------------------------------L0.15--------------------------------|     "
    - "L0.16[50,629] 1.01us         |----------------------L0.16-----------------------|                                  "
    - "L0.17[76,932] 1.02us           |-----------------------------------L0.17------------------------------------|      "
    - "L0.18[42,986] 1.02us        |---------------------------------------L0.18----------------------------------------| "
    - "L0.19[173,950] 1.02us                   |-------------------------------L0.19--------------------------------|     "
    - "L0.20[50,629] 1.02us         |----------------------L0.20-----------------------|                                  "
    - "L0.21[76,932] 1.02us           |-----------------------------------L0.21------------------------------------|      "
    - "L0.22[42,986] 1.02us        |---------------------------------------L0.22----------------------------------------| "
    - "L0.23[173,950] 1.02us                   |-------------------------------L0.23--------------------------------|     "
    - "L0.24[50,629] 1.02us         |----------------------L0.24-----------------------|                                  "
    - "L0.25[76,932] 1.02us           |-----------------------------------L0.25------------------------------------|      "
    - "L0.26[42,986] 1.02us        |---------------------------------------L0.26----------------------------------------| "
    - "L0.27[173,950] 1.03us                   |-------------------------------L0.27--------------------------------|     "
    - "L0.28[50,629] 1.03us         |----------------------L0.28-----------------------|                                  "
    - "L0.29[76,932] 1.03us           |-----------------------------------L0.29------------------------------------|      "
    - "L0.30[42,986] 1.03us        |---------------------------------------L0.30----------------------------------------| "
    - "L0.31[173,950] 1.03us                   |-------------------------------L0.31--------------------------------|     "
    - "L0.32[50,629] 1.03us         |----------------------L0.32-----------------------|                                  "
    - "L0.33[76,932] 1.03us           |-----------------------------------L0.33------------------------------------|      "
    - "L0.34[42,986] 1.03us        |---------------------------------------L0.34----------------------------------------| "
    - "L0.35[173,950] 1.03us                   |-------------------------------L0.35--------------------------------|     "
    - "L0.36[50,629] 1.03us         |----------------------L0.36-----------------------|                                  "
    - "L0.37[76,932] 1.04us           |-----------------------------------L0.37------------------------------------|      "
    - "L0.38[42,986] 1.04us        |---------------------------------------L0.38----------------------------------------| "
    - "L0.39[173,950] 1.04us                   |-------------------------------L0.39--------------------------------|     "
    - "L0.40[50,629] 1.04us         |----------------------L0.40-----------------------|                                  "
    - "L0.41[76,932] 1.04us           |-----------------------------------L0.41------------------------------------|      "
    - "L0.42[42,986] 1.04us        |---------------------------------------L0.42----------------------------------------| "
    - "L0.43[173,950] 1.04us                   |-------------------------------L0.43--------------------------------|     "
    - "L0.44[50,629] 1.04us         |----------------------L0.44-----------------------|                                  "
    - "L0.45[76,932] 1.04us           |-----------------------------------L0.45------------------------------------|      "
    - "L0.46[42,986] 1.05us        |---------------------------------------L0.46----------------------------------------| "
    - "L0.47[173,950] 1.05us                   |-------------------------------L0.47--------------------------------|     "
    - "L0.48[50,629] 1.05us         |----------------------L0.48-----------------------|                                  "
    - "L0.49[76,932] 1.05us           |-----------------------------------L0.49------------------------------------|      "
    - "L0.50[42,986] 1.05us        |---------------------------------------L0.50----------------------------------------| "
    - "L0.51[0,1] 999ns         |L0.51|                                                                                   "
    - "**** Simulation run 0, type=split(HighL0OverlapSingleFile)(split_times=[356, 670]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.1[76,932] 1us         |------------------------------------------L0.1------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[76,356] 1us 3.27mb  |-----------L0.?------------|                                                             "
    - "L0.?[357,670] 1us 3.66mb                              |-------------L0.?-------------|                             "
    - "L0.?[671,932] 1us 3.07mb                                                               |----------L0.?-----------| "
    - "**** Simulation run 1, type=split(HighL0OverlapSingleFile)(split_times=[356, 670]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.2[42,986] 1us         |------------------------------------------L0.2------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[42,356] 1us 3.33mb  |-----------L0.?------------|                                                             "
    - "L0.?[357,670] 1us 3.32mb                               |-----------L0.?------------|                               "
    - "L0.?[671,986] 1us 3.36mb                                                            |------------L0.?------------| "
    - "**** Simulation run 2, type=split(HighL0OverlapSingleFile)(split_times=[356, 670]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.3[173,950] 1us        |------------------------------------------L0.3------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[173,356] 1us 2.36mb |-------L0.?--------|                                                                     "
    - "L0.?[357,670] 1us 4.03mb                      |---------------L0.?---------------|                                 "
    - "L0.?[671,950] 1us 3.62mb                                                          |-------------L0.?-------------| "
    - "**** Simulation run 3, type=split(HighL0OverlapSingleFile)(split_times=[356]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.4[50,629] 1us         |------------------------------------------L0.4------------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[50,356] 1us 5.28mb  |--------------------L0.?---------------------|                                           "
    - "L0.?[357,629] 1us 4.72mb                                                |------------------L0.?------------------| "
    - "**** Simulation run 4, type=split(HighL0OverlapSingleFile)(split_times=[356, 670]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.5[76,932] 1us         |------------------------------------------L0.5------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[76,356] 1us 3.27mb  |-----------L0.?------------|                                                             "
    - "L0.?[357,670] 1us 3.66mb                              |-------------L0.?-------------|                             "
    - "L0.?[671,932] 1us 3.07mb                                                               |----------L0.?-----------| "
    - "**** Simulation run 5, type=split(HighL0OverlapSingleFile)(split_times=[356, 670]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.6[42,986] 1us         |------------------------------------------L0.6------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[42,356] 1us 3.33mb  |-----------L0.?------------|                                                             "
    - "L0.?[357,670] 1us 3.32mb                               |-----------L0.?------------|                               "
    - "L0.?[671,986] 1us 3.36mb                                                            |------------L0.?------------| "
    - "**** Simulation run 6, type=split(HighL0OverlapSingleFile)(split_times=[356, 670]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.7[173,950] 1.01us     |------------------------------------------L0.7------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[173,356] 1.01us 2.36mb|-------L0.?--------|                                                                     "
    - "L0.?[357,670] 1.01us 4.03mb                     |---------------L0.?---------------|                                 "
    - "L0.?[671,950] 1.01us 3.62mb                                                         |-------------L0.?-------------| "
    - "**** Simulation run 7, type=split(HighL0OverlapSingleFile)(split_times=[356]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.8[50,629] 1.01us      |------------------------------------------L0.8------------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[50,356] 1.01us 5.28mb|--------------------L0.?---------------------|                                           "
    - "L0.?[357,629] 1.01us 4.72mb                                               |------------------L0.?------------------| "
    - "**** Simulation run 8, type=split(HighL0OverlapSingleFile)(split_times=[356, 670]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.9[76,932] 1.01us      |------------------------------------------L0.9------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[76,356] 1.01us 3.27mb|-----------L0.?------------|                                                             "
    - "L0.?[357,670] 1.01us 3.66mb                             |-------------L0.?-------------|                             "
    - "L0.?[671,932] 1.01us 3.07mb                                                              |----------L0.?-----------| "
    - "**** Simulation run 9, type=split(HighL0OverlapSingleFile)(split_times=[356, 670]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.10[42,986] 1.01us     |-----------------------------------------L0.10------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[42,356] 1.01us 3.33mb|-----------L0.?------------|                                                             "
    - "L0.?[357,670] 1.01us 3.32mb                              |-----------L0.?------------|                               "
    - "L0.?[671,986] 1.01us 3.36mb                                                           |------------L0.?------------| "
    - "**** Simulation run 10, type=split(HighL0OverlapSingleFile)(split_times=[356, 670]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.11[173,950] 1.01us    |-----------------------------------------L0.11------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[173,356] 1.01us 2.36mb|-------L0.?--------|                                                                     "
    - "L0.?[357,670] 1.01us 4.03mb                     |---------------L0.?---------------|                                 "
    - "L0.?[671,950] 1.01us 3.62mb                                                         |-------------L0.?-------------| "
    - "**** Simulation run 11, type=split(HighL0OverlapSingleFile)(split_times=[356]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.12[50,629] 1.01us     |-----------------------------------------L0.12------------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[50,356] 1.01us 5.28mb|--------------------L0.?---------------------|                                           "
    - "L0.?[357,629] 1.01us 4.72mb                                               |------------------L0.?------------------| "
    - "**** Simulation run 12, type=split(HighL0OverlapSingleFile)(split_times=[356, 670]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.13[76,932] 1.01us     |-----------------------------------------L0.13------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[76,356] 1.01us 3.27mb|-----------L0.?------------|                                                             "
    - "L0.?[357,670] 1.01us 3.66mb                             |-------------L0.?-------------|                             "
    - "L0.?[671,932] 1.01us 3.07mb                                                              |----------L0.?-----------| "
    - "**** Simulation run 13, type=split(HighL0OverlapSingleFile)(split_times=[356, 670]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.14[42,986] 1.01us     |-----------------------------------------L0.14------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[42,356] 1.01us 3.33mb|-----------L0.?------------|                                                             "
    - "L0.?[357,670] 1.01us 3.32mb                              |-----------L0.?------------|                               "
    - "L0.?[671,986] 1.01us 3.36mb                                                           |------------L0.?------------| "
    - "**** Simulation run 14, type=split(HighL0OverlapSingleFile)(split_times=[356, 670]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.15[173,950] 1.01us    |-----------------------------------------L0.15------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[173,356] 1.01us 2.36mb|-------L0.?--------|                                                                     "
    - "L0.?[357,670] 1.01us 4.03mb                     |---------------L0.?---------------|                                 "
    - "L0.?[671,950] 1.01us 3.62mb                                                         |-------------L0.?-------------| "
    - "**** Simulation run 15, type=split(HighL0OverlapSingleFile)(split_times=[356]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.16[50,629] 1.01us     |-----------------------------------------L0.16------------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[50,356] 1.01us 5.28mb|--------------------L0.?---------------------|                                           "
    - "L0.?[357,629] 1.01us 4.72mb                                               |------------------L0.?------------------| "
    - "**** Simulation run 16, type=split(HighL0OverlapSingleFile)(split_times=[356, 670]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.17[76,932] 1.02us     |-----------------------------------------L0.17------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[76,356] 1.02us 3.27mb|-----------L0.?------------|                                                             "
    - "L0.?[357,670] 1.02us 3.66mb                             |-------------L0.?-------------|                             "
    - "L0.?[671,932] 1.02us 3.07mb                                                              |----------L0.?-----------| "
    - "**** Simulation run 17, type=split(HighL0OverlapSingleFile)(split_times=[356, 670]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.18[42,986] 1.02us     |-----------------------------------------L0.18------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[42,356] 1.02us 3.33mb|-----------L0.?------------|                                                             "
    - "L0.?[357,670] 1.02us 3.32mb                              |-----------L0.?------------|                               "
    - "L0.?[671,986] 1.02us 3.36mb                                                           |------------L0.?------------| "
    - "**** Simulation run 18, type=split(HighL0OverlapSingleFile)(split_times=[356, 670]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.19[173,950] 1.02us    |-----------------------------------------L0.19------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[173,356] 1.02us 2.36mb|-------L0.?--------|                                                                     "
    - "L0.?[357,670] 1.02us 4.03mb                     |---------------L0.?---------------|                                 "
    - "L0.?[671,950] 1.02us 3.62mb                                                         |-------------L0.?-------------| "
    - "**** Simulation run 19, type=split(HighL0OverlapSingleFile)(split_times=[356]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.20[50,629] 1.02us     |-----------------------------------------L0.20------------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[50,356] 1.02us 5.28mb|--------------------L0.?---------------------|                                           "
    - "L0.?[357,629] 1.02us 4.72mb                                               |------------------L0.?------------------| "
    - "**** Simulation run 20, type=split(HighL0OverlapSingleFile)(split_times=[356, 670]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.21[76,932] 1.02us     |-----------------------------------------L0.21------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[76,356] 1.02us 3.27mb|-----------L0.?------------|                                                             "
    - "L0.?[357,670] 1.02us 3.66mb                             |-------------L0.?-------------|                             "
    - "L0.?[671,932] 1.02us 3.07mb                                                              |----------L0.?-----------| "
    - "**** Simulation run 21, type=split(HighL0OverlapSingleFile)(split_times=[356, 670]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.22[42,986] 1.02us     |-----------------------------------------L0.22------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[42,356] 1.02us 3.33mb|-----------L0.?------------|                                                             "
    - "L0.?[357,670] 1.02us 3.32mb                              |-----------L0.?------------|                               "
    - "L0.?[671,986] 1.02us 3.36mb                                                           |------------L0.?------------| "
    - "**** Simulation run 22, type=split(HighL0OverlapSingleFile)(split_times=[356, 670]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.23[173,950] 1.02us    |-----------------------------------------L0.23------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[173,356] 1.02us 2.36mb|-------L0.?--------|                                                                     "
    - "L0.?[357,670] 1.02us 4.03mb                     |---------------L0.?---------------|                                 "
    - "L0.?[671,950] 1.02us 3.62mb                                                         |-------------L0.?-------------| "
    - "**** Simulation run 23, type=split(HighL0OverlapSingleFile)(split_times=[356]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.24[50,629] 1.02us     |-----------------------------------------L0.24------------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[50,356] 1.02us 5.28mb|--------------------L0.?---------------------|                                           "
    - "L0.?[357,629] 1.02us 4.72mb                                               |------------------L0.?------------------| "
    - "**** Simulation run 24, type=split(HighL0OverlapSingleFile)(split_times=[356, 670]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.25[76,932] 1.02us     |-----------------------------------------L0.25------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[76,356] 1.02us 3.27mb|-----------L0.?------------|                                                             "
    - "L0.?[357,670] 1.02us 3.66mb                             |-------------L0.?-------------|                             "
    - "L0.?[671,932] 1.02us 3.07mb                                                              |----------L0.?-----------| "
    - "**** Simulation run 25, type=split(HighL0OverlapSingleFile)(split_times=[356, 670]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.26[42,986] 1.02us     |-----------------------------------------L0.26------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[42,356] 1.02us 3.33mb|-----------L0.?------------|                                                             "
    - "L0.?[357,670] 1.02us 3.32mb                              |-----------L0.?------------|                               "
    - "L0.?[671,986] 1.02us 3.36mb                                                           |------------L0.?------------| "
    - "**** Simulation run 26, type=split(HighL0OverlapSingleFile)(split_times=[356, 670]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.27[173,950] 1.03us    |-----------------------------------------L0.27------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[173,356] 1.03us 2.36mb|-------L0.?--------|                                                                     "
    - "L0.?[357,670] 1.03us 4.03mb                     |---------------L0.?---------------|                                 "
    - "L0.?[671,950] 1.03us 3.62mb                                                         |-------------L0.?-------------| "
    - "**** Simulation run 27, type=split(HighL0OverlapSingleFile)(split_times=[356]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.28[50,629] 1.03us     |-----------------------------------------L0.28------------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[50,356] 1.03us 5.28mb|--------------------L0.?---------------------|                                           "
    - "L0.?[357,629] 1.03us 4.72mb                                               |------------------L0.?------------------| "
    - "**** Simulation run 28, type=split(HighL0OverlapSingleFile)(split_times=[356, 670]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.29[76,932] 1.03us     |-----------------------------------------L0.29------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[76,356] 1.03us 3.27mb|-----------L0.?------------|                                                             "
    - "L0.?[357,670] 1.03us 3.66mb                             |-------------L0.?-------------|                             "
    - "L0.?[671,932] 1.03us 3.07mb                                                              |----------L0.?-----------| "
    - "**** Simulation run 29, type=split(HighL0OverlapSingleFile)(split_times=[356, 670]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.30[42,986] 1.03us     |-----------------------------------------L0.30------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[42,356] 1.03us 3.33mb|-----------L0.?------------|                                                             "
    - "L0.?[357,670] 1.03us 3.32mb                              |-----------L0.?------------|                               "
    - "L0.?[671,986] 1.03us 3.36mb                                                           |------------L0.?------------| "
    - "**** Simulation run 30, type=split(HighL0OverlapSingleFile)(split_times=[356, 670]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.31[173,950] 1.03us    |-----------------------------------------L0.31------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[173,356] 1.03us 2.36mb|-------L0.?--------|                                                                     "
    - "L0.?[357,670] 1.03us 4.03mb                     |---------------L0.?---------------|                                 "
    - "L0.?[671,950] 1.03us 3.62mb                                                         |-------------L0.?-------------| "
    - "**** Simulation run 31, type=split(HighL0OverlapSingleFile)(split_times=[356]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.32[50,629] 1.03us     |-----------------------------------------L0.32------------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[50,356] 1.03us 5.28mb|--------------------L0.?---------------------|                                           "
    - "L0.?[357,629] 1.03us 4.72mb                                               |------------------L0.?------------------| "
    - "**** Simulation run 32, type=split(HighL0OverlapSingleFile)(split_times=[356, 670]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.33[76,932] 1.03us     |-----------------------------------------L0.33------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[76,356] 1.03us 3.27mb|-----------L0.?------------|                                                             "
    - "L0.?[357,670] 1.03us 3.66mb                             |-------------L0.?-------------|                             "
    - "L0.?[671,932] 1.03us 3.07mb                                                              |----------L0.?-----------| "
    - "**** Simulation run 33, type=split(HighL0OverlapSingleFile)(split_times=[356, 670]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.34[42,986] 1.03us     |-----------------------------------------L0.34------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[42,356] 1.03us 3.33mb|-----------L0.?------------|                                                             "
    - "L0.?[357,670] 1.03us 3.32mb                              |-----------L0.?------------|                               "
    - "L0.?[671,986] 1.03us 3.36mb                                                           |------------L0.?------------| "
    - "**** Simulation run 34, type=split(HighL0OverlapSingleFile)(split_times=[356, 670]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.35[173,950] 1.03us    |-----------------------------------------L0.35------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[173,356] 1.03us 2.36mb|-------L0.?--------|                                                                     "
    - "L0.?[357,670] 1.03us 4.03mb                     |---------------L0.?---------------|                                 "
    - "L0.?[671,950] 1.03us 3.62mb                                                         |-------------L0.?-------------| "
    - "**** Simulation run 35, type=split(HighL0OverlapSingleFile)(split_times=[356]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.36[50,629] 1.03us     |-----------------------------------------L0.36------------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[50,356] 1.03us 5.28mb|--------------------L0.?---------------------|                                           "
    - "L0.?[357,629] 1.03us 4.72mb                                               |------------------L0.?------------------| "
    - "**** Simulation run 36, type=split(HighL0OverlapSingleFile)(split_times=[356, 670]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.37[76,932] 1.04us     |-----------------------------------------L0.37------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[76,356] 1.04us 3.27mb|-----------L0.?------------|                                                             "
    - "L0.?[357,670] 1.04us 3.66mb                             |-------------L0.?-------------|                             "
    - "L0.?[671,932] 1.04us 3.07mb                                                              |----------L0.?-----------| "
    - "**** Simulation run 37, type=split(HighL0OverlapSingleFile)(split_times=[356, 670]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.38[42,986] 1.04us     |-----------------------------------------L0.38------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[42,356] 1.04us 3.33mb|-----------L0.?------------|                                                             "
    - "L0.?[357,670] 1.04us 3.32mb                              |-----------L0.?------------|                               "
    - "L0.?[671,986] 1.04us 3.36mb                                                           |------------L0.?------------| "
    - "**** Simulation run 38, type=split(HighL0OverlapSingleFile)(split_times=[356, 670]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.39[173,950] 1.04us    |-----------------------------------------L0.39------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[173,356] 1.04us 2.36mb|-------L0.?--------|                                                                     "
    - "L0.?[357,670] 1.04us 4.03mb                     |---------------L0.?---------------|                                 "
    - "L0.?[671,950] 1.04us 3.62mb                                                         |-------------L0.?-------------| "
    - "**** Simulation run 39, type=split(HighL0OverlapSingleFile)(split_times=[356]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.40[50,629] 1.04us     |-----------------------------------------L0.40------------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[50,356] 1.04us 5.28mb|--------------------L0.?---------------------|                                           "
    - "L0.?[357,629] 1.04us 4.72mb                                               |------------------L0.?------------------| "
    - "**** Simulation run 40, type=split(HighL0OverlapSingleFile)(split_times=[356, 670]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.41[76,932] 1.04us     |-----------------------------------------L0.41------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[76,356] 1.04us 3.27mb|-----------L0.?------------|                                                             "
    - "L0.?[357,670] 1.04us 3.66mb                             |-------------L0.?-------------|                             "
    - "L0.?[671,932] 1.04us 3.07mb                                                              |----------L0.?-----------| "
    - "**** Simulation run 41, type=split(HighL0OverlapSingleFile)(split_times=[356, 670]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.42[42,986] 1.04us     |-----------------------------------------L0.42------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[42,356] 1.04us 3.33mb|-----------L0.?------------|                                                             "
    - "L0.?[357,670] 1.04us 3.32mb                              |-----------L0.?------------|                               "
    - "L0.?[671,986] 1.04us 3.36mb                                                           |------------L0.?------------| "
    - "**** Simulation run 42, type=split(HighL0OverlapSingleFile)(split_times=[356, 670]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.43[173,950] 1.04us    |-----------------------------------------L0.43------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[173,356] 1.04us 2.36mb|-------L0.?--------|                                                                     "
    - "L0.?[357,670] 1.04us 4.03mb                     |---------------L0.?---------------|                                 "
    - "L0.?[671,950] 1.04us 3.62mb                                                         |-------------L0.?-------------| "
    - "**** Simulation run 43, type=split(HighL0OverlapSingleFile)(split_times=[356]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.44[50,629] 1.04us     |-----------------------------------------L0.44------------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[50,356] 1.04us 5.28mb|--------------------L0.?---------------------|                                           "
    - "L0.?[357,629] 1.04us 4.72mb                                               |------------------L0.?------------------| "
    - "**** Simulation run 44, type=split(HighL0OverlapSingleFile)(split_times=[356, 670]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.45[76,932] 1.04us     |-----------------------------------------L0.45------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[76,356] 1.04us 3.27mb|-----------L0.?------------|                                                             "
    - "L0.?[357,670] 1.04us 3.66mb                             |-------------L0.?-------------|                             "
    - "L0.?[671,932] 1.04us 3.07mb                                                              |----------L0.?-----------| "
    - "**** Simulation run 45, type=split(HighL0OverlapSingleFile)(split_times=[356, 670]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.46[42,986] 1.05us     |-----------------------------------------L0.46------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[42,356] 1.05us 3.33mb|-----------L0.?------------|                                                             "
    - "L0.?[357,670] 1.05us 3.32mb                              |-----------L0.?------------|                               "
    - "L0.?[671,986] 1.05us 3.36mb                                                           |------------L0.?------------| "
    - "**** Simulation run 46, type=split(HighL0OverlapSingleFile)(split_times=[356, 670]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.47[173,950] 1.05us    |-----------------------------------------L0.47------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[173,356] 1.05us 2.36mb|-------L0.?--------|                                                                     "
    - "L0.?[357,670] 1.05us 4.03mb                     |---------------L0.?---------------|                                 "
    - "L0.?[671,950] 1.05us 3.62mb                                                         |-------------L0.?-------------| "
    - "**** Simulation run 47, type=split(HighL0OverlapSingleFile)(split_times=[356]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.48[50,629] 1.05us     |-----------------------------------------L0.48------------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[50,356] 1.05us 5.28mb|--------------------L0.?---------------------|                                           "
    - "L0.?[357,629] 1.05us 4.72mb                                               |------------------L0.?------------------| "
    - "**** Simulation run 48, type=split(HighL0OverlapSingleFile)(split_times=[356, 670]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.49[76,932] 1.05us     |-----------------------------------------L0.49------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[76,356] 1.05us 3.27mb|-----------L0.?------------|                                                             "
    - "L0.?[357,670] 1.05us 3.66mb                             |-------------L0.?-------------|                             "
    - "L0.?[671,932] 1.05us 3.07mb                                                              |----------L0.?-----------| "
    - "**** Simulation run 49, type=split(HighL0OverlapSingleFile)(split_times=[356, 670]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.50[42,986] 1.05us     |-----------------------------------------L0.50------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[42,356] 1.05us 3.33mb|-----------L0.?------------|                                                             "
    - "L0.?[357,670] 1.05us 3.32mb                              |-----------L0.?------------|                               "
    - "L0.?[671,986] 1.05us 3.36mb                                                           |------------L0.?------------| "
    - "Committing partition 1:"
    - "  Soft Deleting 50 files: L0.1, L0.2, L0.3, L0.4, L0.5, L0.6, L0.7, L0.8, L0.9, L0.10, L0.11, L0.12, L0.13, L0.14, L0.15, L0.16, L0.17, L0.18, L0.19, L0.20, L0.21, L0.22, L0.23, L0.24, L0.25, L0.26, L0.27, L0.28, L0.29, L0.30, L0.31, L0.32, L0.33, L0.34, L0.35, L0.36, L0.37, L0.38, L0.39, L0.40, L0.41, L0.42, L0.43, L0.44, L0.45, L0.46, L0.47, L0.48, L0.49, L0.50"
    - "  Creating 138 files"
    - "**** Simulation run 50, type=split(CompactAndSplitOutput(FoundSubsetLessThanMaxCompactSize))(split_times=[329, 658]). 81 Input Files, 300mb total:"
    - "L0                                                                                                                 "
    - "L0.51[0,1] 999ns 10mb    |L0.51|                                                                                   "
    - "L0.54[671,932] 1us 3.07mb                                                             |--------L0.54--------|      "
    - "L0.53[357,670] 1us 3.66mb                                |----------L0.53-----------|                              "
    - "L0.52[76,356] 1us 3.27mb       |---------L0.52---------|                                                           "
    - "L0.57[671,986] 1us 3.36mb                                                             |----------L0.57-----------| "
    - "L0.56[357,670] 1us 3.32mb                                |----------L0.56-----------|                              "
    - "L0.55[42,356] 1us 3.33mb    |----------L0.55-----------|                                                           "
    - "L0.60[671,950] 1us 3.62mb                                                             |---------L0.60---------|    "
    - "L0.59[357,670] 1us 4.03mb                                |----------L0.59-----------|                              "
    - "L0.58[173,356] 1us 2.36mb               |----L0.58-----|                                                           "
    - "L0.62[357,629] 1us 4.72mb                                |--------L0.62---------|                                  "
    - "L0.61[50,356] 1us 5.28mb     |----------L0.61----------|                                                           "
    - "L0.65[671,932] 1us 3.07mb                                                             |--------L0.65--------|      "
    - "L0.64[357,670] 1us 3.66mb                                |----------L0.64-----------|                              "
    - "L0.63[76,356] 1us 3.27mb       |---------L0.63---------|                                                           "
    - "L0.68[671,986] 1us 3.36mb                                                             |----------L0.68-----------| "
    - "L0.67[357,670] 1us 3.32mb                                |----------L0.67-----------|                              "
    - "L0.66[42,356] 1us 3.33mb    |----------L0.66-----------|                                                           "
    - "L0.71[671,950] 1.01us 3.62mb                                                             |---------L0.71---------|    "
    - "L0.70[357,670] 1.01us 4.03mb                                |----------L0.70-----------|                              "
    - "L0.69[173,356] 1.01us 2.36mb               |----L0.69-----|                                                           "
    - "L0.73[357,629] 1.01us 4.72mb                                |--------L0.73---------|                                  "
    - "L0.72[50,356] 1.01us 5.28mb    |----------L0.72----------|                                                           "
    - "L0.76[671,932] 1.01us 3.07mb                                                             |--------L0.76--------|      "
    - "L0.75[357,670] 1.01us 3.66mb                                |----------L0.75-----------|                              "
    - "L0.74[76,356] 1.01us 3.27mb      |---------L0.74---------|                                                           "
    - "L0.79[671,986] 1.01us 3.36mb                                                             |----------L0.79-----------| "
    - "L0.78[357,670] 1.01us 3.32mb                                |----------L0.78-----------|                              "
    - "L0.77[42,356] 1.01us 3.33mb   |----------L0.77-----------|                                                           "
    - "L0.82[671,950] 1.01us 3.62mb                                                             |---------L0.82---------|    "
    - "L0.81[357,670] 1.01us 4.03mb                                |----------L0.81-----------|                              "
    - "L0.80[173,356] 1.01us 2.36mb               |----L0.80-----|                                                           "
    - "L0.84[357,629] 1.01us 4.72mb                                |--------L0.84---------|                                  "
    - "L0.83[50,356] 1.01us 5.28mb    |----------L0.83----------|                                                           "
    - "L0.87[671,932] 1.01us 3.07mb                                                             |--------L0.87--------|      "
    - "L0.86[357,670] 1.01us 3.66mb                                |----------L0.86-----------|                              "
    - "L0.85[76,356] 1.01us 3.27mb      |---------L0.85---------|                                                           "
    - "L0.90[671,986] 1.01us 3.36mb                                                             |----------L0.90-----------| "
    - "L0.89[357,670] 1.01us 3.32mb                                |----------L0.89-----------|                              "
    - "L0.88[42,356] 1.01us 3.33mb   |----------L0.88-----------|                                                           "
    - "L0.93[671,950] 1.01us 3.62mb                                                             |---------L0.93---------|    "
    - "L0.92[357,670] 1.01us 4.03mb                                |----------L0.92-----------|                              "
    - "L0.91[173,356] 1.01us 2.36mb               |----L0.91-----|                                                           "
    - "L0.95[357,629] 1.01us 4.72mb                                |--------L0.95---------|                                  "
    - "L0.94[50,356] 1.01us 5.28mb    |----------L0.94----------|                                                           "
    - "L0.98[671,932] 1.02us 3.07mb                                                             |--------L0.98--------|      "
    - "L0.97[357,670] 1.02us 3.66mb                                |----------L0.97-----------|                              "
    - "L0.96[76,356] 1.02us 3.27mb      |---------L0.96---------|                                                           "
    - "L0.101[671,986] 1.02us 3.36mb                                                             |----------L0.101----------| "
    - "L0.100[357,670] 1.02us 3.32mb                                |----------L0.100----------|                              "
    - "L0.99[42,356] 1.02us 3.33mb   |----------L0.99-----------|                                                           "
    - "L0.104[671,950] 1.02us 3.62mb                                                             |--------L0.104---------|    "
    - "L0.103[357,670] 1.02us 4.03mb                                |----------L0.103----------|                              "
    - "L0.102[173,356] 1.02us 2.36mb               |----L0.102----|                                                           "
    - "L0.106[357,629] 1.02us 4.72mb                                |--------L0.106--------|                                  "
    - "L0.105[50,356] 1.02us 5.28mb    |---------L0.105----------|                                                           "
    - "L0.109[671,932] 1.02us 3.07mb                                                             |-------L0.109--------|      "
    - "L0.108[357,670] 1.02us 3.66mb                                |----------L0.108----------|                              "
    - "L0.107[76,356] 1.02us 3.27mb      |--------L0.107---------|                                                           "
    - "L0.112[671,986] 1.02us 3.36mb                                                             |----------L0.112----------| "
    - "L0.111[357,670] 1.02us 3.32mb                                |----------L0.111----------|                              "
    - "L0.110[42,356] 1.02us 3.33mb   |----------L0.110----------|                                                           "
    - "L0.115[671,950] 1.02us 3.62mb                                                             |--------L0.115---------|    "
    - "L0.114[357,670] 1.02us 4.03mb                                |----------L0.114----------|                              "
    - "L0.113[173,356] 1.02us 2.36mb               |----L0.113----|                                                           "
    - "L0.117[357,629] 1.02us 4.72mb                                |--------L0.117--------|                                  "
    - "L0.116[50,356] 1.02us 5.28mb    |---------L0.116----------|                                                           "
    - "L0.120[671,932] 1.02us 3.07mb                                                             |-------L0.120--------|      "
    - "L0.119[357,670] 1.02us 3.66mb                                |----------L0.119----------|                              "
    - "L0.118[76,356] 1.02us 3.27mb      |--------L0.118---------|                                                           "
    - "L0.123[671,986] 1.02us 3.36mb                                                             |----------L0.123----------| "
    - "L0.122[357,670] 1.02us 3.32mb                                |----------L0.122----------|                              "
    - "L0.121[42,356] 1.02us 3.33mb   |----------L0.121----------|                                                           "
    - "L0.126[671,950] 1.03us 3.62mb                                                             |--------L0.126---------|    "
    - "L0.125[357,670] 1.03us 4.03mb                                |----------L0.125----------|                              "
    - "L0.124[173,356] 1.03us 2.36mb               |----L0.124----|                                                           "
    - "L0.128[357,629] 1.03us 4.72mb                                |--------L0.128--------|                                  "
    - "L0.127[50,356] 1.03us 5.28mb    |---------L0.127----------|                                                           "
    - "L0.131[671,932] 1.03us 3.07mb                                                             |-------L0.131--------|      "
    - "L0.130[357,670] 1.03us 3.66mb                                |----------L0.130----------|                              "
    - "L0.129[76,356] 1.03us 3.27mb      |--------L0.129---------|                                                           "
    - "**** 3 Output Files (parquet_file_id not yet assigned), 300mb total:"
    - "L1                                                                                                                 "
    - "L1.?[0,329] 1.03us 100.1mb|------------L1.?------------|                                                            "
    - "L1.?[330,658] 1.03us 99.8mb                              |-----------L1.?------------|                               "
    - "L1.?[659,986] 1.03us 100.1mb                                                            |-----------L1.?------------| "
    - "Committing partition 1:"
    - "  Soft Deleting 81 files: L0.51, L0.52, L0.53, L0.54, L0.55, L0.56, L0.57, L0.58, L0.59, L0.60, L0.61, L0.62, L0.63, L0.64, L0.65, L0.66, L0.67, L0.68, L0.69, L0.70, L0.71, L0.72, L0.73, L0.74, L0.75, L0.76, L0.77, L0.78, L0.79, L0.80, L0.81, L0.82, L0.83, L0.84, L0.85, L0.86, L0.87, L0.88, L0.89, L0.90, L0.91, L0.92, L0.93, L0.94, L0.95, L0.96, L0.97, L0.98, L0.99, L0.100, L0.101, L0.102, L0.103, L0.104, L0.105, L0.106, L0.107, L0.108, L0.109, L0.110, L0.111, L0.112, L0.113, L0.114, L0.115, L0.116, L0.117, L0.118, L0.119, L0.120, L0.121, L0.122, L0.123, L0.124, L0.125, L0.126, L0.127, L0.128, L0.129, L0.130, L0.131"
    - "  Creating 3 files"
    - "**** Simulation run 51, type=split(ReduceOverlap)(split_times=[329]). 1 Input Files, 3.33mb total:"
    - "L0, all files 3.33mb                                                                                               "
    - "L0.132[42,356] 1.03us    |-----------------------------------------L0.132-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3.33mb total:"
    - "L0                                                                                                                 "
    - "L0.?[42,329] 1.03us 3.04mb|--------------------------------------L0.?--------------------------------------|        "
    - "L0.?[330,356] 1.03us 292.88kb                                                                                  |L0.?-| "
    - "**** Simulation run 52, type=split(ReduceOverlap)(split_times=[658]). 1 Input Files, 3.32mb total:"
    - "L0, all files 3.32mb                                                                                               "
    - "L0.133[357,670] 1.03us   |-----------------------------------------L0.133-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3.32mb total:"
    - "L0                                                                                                                 "
    - "L0.?[357,658] 1.03us 3.19mb|----------------------------------------L0.?----------------------------------------|    "
    - "L0.?[659,670] 1.03us 130.17kb                                                                                      |L0.?|"
    - "**** Simulation run 53, type=split(ReduceOverlap)(split_times=[329]). 1 Input Files, 2.36mb total:"
    - "L0, all files 2.36mb                                                                                               "
    - "L0.135[173,356] 1.03us   |-----------------------------------------L0.135-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 2.36mb total:"
    - "L0                                                                                                                 "
    - "L0.?[173,329] 1.03us 2.01mb|-----------------------------------L0.?-----------------------------------|              "
    - "L0.?[330,356] 1.03us 355.83kb                                                                             |---L0.?---| "
    - "**** Simulation run 54, type=split(ReduceOverlap)(split_times=[658]). 1 Input Files, 4.03mb total:"
    - "L0, all files 4.03mb                                                                                               "
    - "L0.136[357,670] 1.03us   |-----------------------------------------L0.136-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 4.03mb total:"
    - "L0                                                                                                                 "
    - "L0.?[357,658] 1.03us 3.87mb|----------------------------------------L0.?----------------------------------------|    "
    - "L0.?[659,670] 1.03us 158.15kb                                                                                      |L0.?|"
    - "**** Simulation run 55, type=split(ReduceOverlap)(split_times=[329]). 1 Input Files, 5.28mb total:"
    - "L0, all files 5.28mb                                                                                               "
    - "L0.138[50,356] 1.03us    |-----------------------------------------L0.138-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 5.28mb total:"
    - "L0                                                                                                                 "
    - "L0.?[50,329] 1.03us 4.82mb|--------------------------------------L0.?--------------------------------------|        "
    - "L0.?[330,356] 1.03us 477.51kb                                                                                  |L0.?-| "
    - "**** Simulation run 56, type=split(ReduceOverlap)(split_times=[329]). 1 Input Files, 3.27mb total:"
    - "L0, all files 3.27mb                                                                                               "
    - "L0.140[76,356] 1.03us    |-----------------------------------------L0.140-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3.27mb total:"
    - "L0                                                                                                                 "
    - "L0.?[76,329] 1.03us 2.96mb|-------------------------------------L0.?--------------------------------------|         "
    - "L0.?[330,356] 1.03us 322.99kb                                                                                 |-L0.?-| "
    - "**** Simulation run 57, type=split(ReduceOverlap)(split_times=[658]). 1 Input Files, 3.66mb total:"
    - "L0, all files 3.66mb                                                                                               "
    - "L0.141[357,670] 1.03us   |-----------------------------------------L0.141-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3.66mb total:"
    - "L0                                                                                                                 "
    - "L0.?[357,658] 1.03us 3.52mb|----------------------------------------L0.?----------------------------------------|    "
    - "L0.?[659,670] 1.03us 143.55kb                                                                                      |L0.?|"
    - "**** Simulation run 58, type=split(ReduceOverlap)(split_times=[329]). 1 Input Files, 3.33mb total:"
    - "L0, all files 3.33mb                                                                                               "
    - "L0.143[42,356] 1.03us    |-----------------------------------------L0.143-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3.33mb total:"
    - "L0                                                                                                                 "
    - "L0.?[42,329] 1.03us 3.04mb|--------------------------------------L0.?--------------------------------------|        "
    - "L0.?[330,356] 1.03us 292.88kb                                                                                  |L0.?-| "
    - "**** Simulation run 59, type=split(ReduceOverlap)(split_times=[658]). 1 Input Files, 3.32mb total:"
    - "L0, all files 3.32mb                                                                                               "
    - "L0.144[357,670] 1.03us   |-----------------------------------------L0.144-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3.32mb total:"
    - "L0                                                                                                                 "
    - "L0.?[357,658] 1.03us 3.19mb|----------------------------------------L0.?----------------------------------------|    "
    - "L0.?[659,670] 1.03us 130.17kb                                                                                      |L0.?|"
    - "**** Simulation run 60, type=split(ReduceOverlap)(split_times=[329]). 1 Input Files, 2.36mb total:"
    - "L0, all files 2.36mb                                                                                               "
    - "L0.146[173,356] 1.03us   |-----------------------------------------L0.146-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 2.36mb total:"
    - "L0                                                                                                                 "
    - "L0.?[173,329] 1.03us 2.01mb|-----------------------------------L0.?-----------------------------------|              "
    - "L0.?[330,356] 1.03us 355.83kb                                                                             |---L0.?---| "
    - "**** Simulation run 61, type=split(ReduceOverlap)(split_times=[658]). 1 Input Files, 4.03mb total:"
    - "L0, all files 4.03mb                                                                                               "
    - "L0.147[357,670] 1.03us   |-----------------------------------------L0.147-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 4.03mb total:"
    - "L0                                                                                                                 "
    - "L0.?[357,658] 1.03us 3.87mb|----------------------------------------L0.?----------------------------------------|    "
    - "L0.?[659,670] 1.03us 158.15kb                                                                                      |L0.?|"
    - "**** Simulation run 62, type=split(ReduceOverlap)(split_times=[329]). 1 Input Files, 5.28mb total:"
    - "L0, all files 5.28mb                                                                                               "
    - "L0.149[50,356] 1.03us    |-----------------------------------------L0.149-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 5.28mb total:"
    - "L0                                                                                                                 "
    - "L0.?[50,329] 1.03us 4.82mb|--------------------------------------L0.?--------------------------------------|        "
    - "L0.?[330,356] 1.03us 477.51kb                                                                                  |L0.?-| "
    - "**** Simulation run 63, type=split(ReduceOverlap)(split_times=[329]). 1 Input Files, 3.27mb total:"
    - "L0, all files 3.27mb                                                                                               "
    - "L0.151[76,356] 1.04us    |-----------------------------------------L0.151-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3.27mb total:"
    - "L0                                                                                                                 "
    - "L0.?[76,329] 1.04us 2.96mb|-------------------------------------L0.?--------------------------------------|         "
    - "L0.?[330,356] 1.04us 322.99kb                                                                                 |-L0.?-| "
    - "**** Simulation run 64, type=split(ReduceOverlap)(split_times=[658]). 1 Input Files, 3.66mb total:"
    - "L0, all files 3.66mb                                                                                               "
    - "L0.152[357,670] 1.04us   |-----------------------------------------L0.152-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3.66mb total:"
    - "L0                                                                                                                 "
    - "L0.?[357,658] 1.04us 3.52mb|----------------------------------------L0.?----------------------------------------|    "
    - "L0.?[659,670] 1.04us 143.55kb                                                                                      |L0.?|"
    - "**** Simulation run 65, type=split(ReduceOverlap)(split_times=[329]). 1 Input Files, 3.33mb total:"
    - "L0, all files 3.33mb                                                                                               "
    - "L0.154[42,356] 1.04us    |-----------------------------------------L0.154-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3.33mb total:"
    - "L0                                                                                                                 "
    - "L0.?[42,329] 1.04us 3.04mb|--------------------------------------L0.?--------------------------------------|        "
    - "L0.?[330,356] 1.04us 292.88kb                                                                                  |L0.?-| "
    - "**** Simulation run 66, type=split(ReduceOverlap)(split_times=[658]). 1 Input Files, 3.32mb total:"
    - "L0, all files 3.32mb                                                                                               "
    - "L0.155[357,670] 1.04us   |-----------------------------------------L0.155-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3.32mb total:"
    - "L0                                                                                                                 "
    - "L0.?[357,658] 1.04us 3.19mb|----------------------------------------L0.?----------------------------------------|    "
    - "L0.?[659,670] 1.04us 130.17kb                                                                                      |L0.?|"
    - "**** Simulation run 67, type=split(ReduceOverlap)(split_times=[329]). 1 Input Files, 2.36mb total:"
    - "L0, all files 2.36mb                                                                                               "
    - "L0.157[173,356] 1.04us   |-----------------------------------------L0.157-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 2.36mb total:"
    - "L0                                                                                                                 "
    - "L0.?[173,329] 1.04us 2.01mb|-----------------------------------L0.?-----------------------------------|              "
    - "L0.?[330,356] 1.04us 355.83kb                                                                             |---L0.?---| "
    - "**** Simulation run 68, type=split(ReduceOverlap)(split_times=[658]). 1 Input Files, 4.03mb total:"
    - "L0, all files 4.03mb                                                                                               "
    - "L0.158[357,670] 1.04us   |-----------------------------------------L0.158-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 4.03mb total:"
    - "L0                                                                                                                 "
    - "L0.?[357,658] 1.04us 3.87mb|----------------------------------------L0.?----------------------------------------|    "
    - "L0.?[659,670] 1.04us 158.15kb                                                                                      |L0.?|"
    - "**** Simulation run 69, type=split(ReduceOverlap)(split_times=[329]). 1 Input Files, 5.28mb total:"
    - "L0, all files 5.28mb                                                                                               "
    - "L0.160[50,356] 1.04us    |-----------------------------------------L0.160-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 5.28mb total:"
    - "L0                                                                                                                 "
    - "L0.?[50,329] 1.04us 4.82mb|--------------------------------------L0.?--------------------------------------|        "
    - "L0.?[330,356] 1.04us 477.51kb                                                                                  |L0.?-| "
    - "**** Simulation run 70, type=split(ReduceOverlap)(split_times=[329]). 1 Input Files, 3.27mb total:"
    - "L0, all files 3.27mb                                                                                               "
    - "L0.162[76,356] 1.04us    |-----------------------------------------L0.162-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3.27mb total:"
    - "L0                                                                                                                 "
    - "L0.?[76,329] 1.04us 2.96mb|-------------------------------------L0.?--------------------------------------|         "
    - "L0.?[330,356] 1.04us 322.99kb                                                                                 |-L0.?-| "
    - "**** Simulation run 71, type=split(ReduceOverlap)(split_times=[658]). 1 Input Files, 3.66mb total:"
    - "L0, all files 3.66mb                                                                                               "
    - "L0.163[357,670] 1.04us   |-----------------------------------------L0.163-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3.66mb total:"
    - "L0                                                                                                                 "
    - "L0.?[357,658] 1.04us 3.52mb|----------------------------------------L0.?----------------------------------------|    "
    - "L0.?[659,670] 1.04us 143.55kb                                                                                      |L0.?|"
    - "**** Simulation run 72, type=split(ReduceOverlap)(split_times=[329]). 1 Input Files, 3.33mb total:"
    - "L0, all files 3.33mb                                                                                               "
    - "L0.165[42,356] 1.04us    |-----------------------------------------L0.165-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3.33mb total:"
    - "L0                                                                                                                 "
    - "L0.?[42,329] 1.04us 3.04mb|--------------------------------------L0.?--------------------------------------|        "
    - "L0.?[330,356] 1.04us 292.88kb                                                                                  |L0.?-| "
    - "**** Simulation run 73, type=split(ReduceOverlap)(split_times=[658]). 1 Input Files, 3.32mb total:"
    - "L0, all files 3.32mb                                                                                               "
    - "L0.166[357,670] 1.04us   |-----------------------------------------L0.166-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3.32mb total:"
    - "L0                                                                                                                 "
    - "L0.?[357,658] 1.04us 3.19mb|----------------------------------------L0.?----------------------------------------|    "
    - "L0.?[659,670] 1.04us 130.17kb                                                                                      |L0.?|"
    - "**** Simulation run 74, type=split(ReduceOverlap)(split_times=[329]). 1 Input Files, 2.36mb total:"
    - "L0, all files 2.36mb                                                                                               "
    - "L0.168[173,356] 1.04us   |-----------------------------------------L0.168-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 2.36mb total:"
    - "L0                                                                                                                 "
    - "L0.?[173,329] 1.04us 2.01mb|-----------------------------------L0.?-----------------------------------|              "
    - "L0.?[330,356] 1.04us 355.83kb                                                                             |---L0.?---| "
    - "**** Simulation run 75, type=split(ReduceOverlap)(split_times=[658]). 1 Input Files, 4.03mb total:"
    - "L0, all files 4.03mb                                                                                               "
    - "L0.169[357,670] 1.04us   |-----------------------------------------L0.169-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 4.03mb total:"
    - "L0                                                                                                                 "
    - "L0.?[357,658] 1.04us 3.87mb|----------------------------------------L0.?----------------------------------------|    "
    - "L0.?[659,670] 1.04us 158.15kb                                                                                      |L0.?|"
    - "**** Simulation run 76, type=split(ReduceOverlap)(split_times=[329]). 1 Input Files, 5.28mb total:"
    - "L0, all files 5.28mb                                                                                               "
    - "L0.171[50,356] 1.04us    |-----------------------------------------L0.171-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 5.28mb total:"
    - "L0                                                                                                                 "
    - "L0.?[50,329] 1.04us 4.82mb|--------------------------------------L0.?--------------------------------------|        "
    - "L0.?[330,356] 1.04us 477.51kb                                                                                  |L0.?-| "
    - "**** Simulation run 77, type=split(ReduceOverlap)(split_times=[329]). 1 Input Files, 3.27mb total:"
    - "L0, all files 3.27mb                                                                                               "
    - "L0.173[76,356] 1.04us    |-----------------------------------------L0.173-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3.27mb total:"
    - "L0                                                                                                                 "
    - "L0.?[76,329] 1.04us 2.96mb|-------------------------------------L0.?--------------------------------------|         "
    - "L0.?[330,356] 1.04us 322.99kb                                                                                 |-L0.?-| "
    - "**** Simulation run 78, type=split(ReduceOverlap)(split_times=[658]). 1 Input Files, 3.66mb total:"
    - "L0, all files 3.66mb                                                                                               "
    - "L0.174[357,670] 1.04us   |-----------------------------------------L0.174-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3.66mb total:"
    - "L0                                                                                                                 "
    - "L0.?[357,658] 1.04us 3.52mb|----------------------------------------L0.?----------------------------------------|    "
    - "L0.?[659,670] 1.04us 143.55kb                                                                                      |L0.?|"
    - "**** Simulation run 79, type=split(ReduceOverlap)(split_times=[329]). 1 Input Files, 3.33mb total:"
    - "L0, all files 3.33mb                                                                                               "
    - "L0.176[42,356] 1.05us    |-----------------------------------------L0.176-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3.33mb total:"
    - "L0                                                                                                                 "
    - "L0.?[42,329] 1.05us 3.04mb|--------------------------------------L0.?--------------------------------------|        "
    - "L0.?[330,356] 1.05us 292.88kb                                                                                  |L0.?-| "
    - "**** Simulation run 80, type=split(ReduceOverlap)(split_times=[658]). 1 Input Files, 3.32mb total:"
    - "L0, all files 3.32mb                                                                                               "
    - "L0.177[357,670] 1.05us   |-----------------------------------------L0.177-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3.32mb total:"
    - "L0                                                                                                                 "
    - "L0.?[357,658] 1.05us 3.19mb|----------------------------------------L0.?----------------------------------------|    "
    - "L0.?[659,670] 1.05us 130.17kb                                                                                      |L0.?|"
    - "**** Simulation run 81, type=split(ReduceOverlap)(split_times=[329]). 1 Input Files, 2.36mb total:"
    - "L0, all files 2.36mb                                                                                               "
    - "L0.179[173,356] 1.05us   |-----------------------------------------L0.179-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 2.36mb total:"
    - "L0                                                                                                                 "
    - "L0.?[173,329] 1.05us 2.01mb|-----------------------------------L0.?-----------------------------------|              "
    - "L0.?[330,356] 1.05us 355.83kb                                                                             |---L0.?---| "
    - "**** Simulation run 82, type=split(ReduceOverlap)(split_times=[658]). 1 Input Files, 4.03mb total:"
    - "L0, all files 4.03mb                                                                                               "
    - "L0.180[357,670] 1.05us   |-----------------------------------------L0.180-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 4.03mb total:"
    - "L0                                                                                                                 "
    - "L0.?[357,658] 1.05us 3.87mb|----------------------------------------L0.?----------------------------------------|    "
    - "L0.?[659,670] 1.05us 158.15kb                                                                                      |L0.?|"
    - "**** Simulation run 83, type=split(ReduceOverlap)(split_times=[329]). 1 Input Files, 5.28mb total:"
    - "L0, all files 5.28mb                                                                                               "
    - "L0.182[50,356] 1.05us    |-----------------------------------------L0.182-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 5.28mb total:"
    - "L0                                                                                                                 "
    - "L0.?[50,329] 1.05us 4.82mb|--------------------------------------L0.?--------------------------------------|        "
    - "L0.?[330,356] 1.05us 477.51kb                                                                                  |L0.?-| "
    - "**** Simulation run 84, type=split(ReduceOverlap)(split_times=[329]). 1 Input Files, 3.27mb total:"
    - "L0, all files 3.27mb                                                                                               "
    - "L0.184[76,356] 1.05us    |-----------------------------------------L0.184-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3.27mb total:"
    - "L0                                                                                                                 "
    - "L0.?[76,329] 1.05us 2.96mb|-------------------------------------L0.?--------------------------------------|         "
    - "L0.?[330,356] 1.05us 322.99kb                                                                                 |-L0.?-| "
    - "**** Simulation run 85, type=split(ReduceOverlap)(split_times=[658]). 1 Input Files, 3.66mb total:"
    - "L0, all files 3.66mb                                                                                               "
    - "L0.185[357,670] 1.05us   |-----------------------------------------L0.185-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3.66mb total:"
    - "L0                                                                                                                 "
    - "L0.?[357,658] 1.05us 3.52mb|----------------------------------------L0.?----------------------------------------|    "
    - "L0.?[659,670] 1.05us 143.55kb                                                                                      |L0.?|"
    - "**** Simulation run 86, type=split(ReduceOverlap)(split_times=[329]). 1 Input Files, 3.33mb total:"
    - "L0, all files 3.33mb                                                                                               "
    - "L0.187[42,356] 1.05us    |-----------------------------------------L0.187-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3.33mb total:"
    - "L0                                                                                                                 "
    - "L0.?[42,329] 1.05us 3.04mb|--------------------------------------L0.?--------------------------------------|        "
    - "L0.?[330,356] 1.05us 292.88kb                                                                                  |L0.?-| "
    - "**** Simulation run 87, type=split(ReduceOverlap)(split_times=[658]). 1 Input Files, 3.32mb total:"
    - "L0, all files 3.32mb                                                                                               "
    - "L0.188[357,670] 1.05us   |-----------------------------------------L0.188-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3.32mb total:"
    - "L0                                                                                                                 "
    - "L0.?[357,658] 1.05us 3.19mb|----------------------------------------L0.?----------------------------------------|    "
    - "L0.?[659,670] 1.05us 130.17kb                                                                                      |L0.?|"
    - "Committing partition 1:"
    - "  Soft Deleting 37 files: L0.132, L0.133, L0.135, L0.136, L0.138, L0.140, L0.141, L0.143, L0.144, L0.146, L0.147, L0.149, L0.151, L0.152, L0.154, L0.155, L0.157, L0.158, L0.160, L0.162, L0.163, L0.165, L0.166, L0.168, L0.169, L0.171, L0.173, L0.174, L0.176, L0.177, L0.179, L0.180, L0.182, L0.184, L0.185, L0.187, L0.188"
    - "  Creating 74 files"
    - "**** Simulation run 88, type=split(CompactAndSplitOutput(FoundSubsetLessThanMaxCompactSize))(split_times=[648, 966]). 6 Input Files, 206.86mb total:"
    - "L0                                                                                                                 "
    - "L0.134[671,986] 1.03us 3.36mb                                              |-----------------L0.134------------------| "
    - "L0.196[659,670] 1.03us 130.17kb                                             |L0.196|                                     "
    - "L0.195[357,658] 1.03us 3.19mb   |----------------L0.195-----------------|                                              "
    - "L0.194[330,356] 1.03us 292.88kb|L0.194|                                                                                  "
    - "L1                                                                                                                 "
    - "L1.192[659,986] 1.03us 100.1mb                                             |------------------L1.192------------------| "
    - "L1.191[330,658] 1.03us 99.8mb|------------------L1.191-------------------|                                             "
    - "**** 3 Output Files (parquet_file_id not yet assigned), 206.86mb total:"
    - "L1                                                                                                                 "
    - "L1.?[330,648] 1.03us 100.28mb|------------------L1.?-------------------|                                               "
    - "L1.?[649,966] 1.03us 99.96mb                                           |------------------L1.?-------------------|    "
    - "L1.?[967,986] 1.03us 6.62mb                                                                                       |L1.?|"
    - "Committing partition 1:"
    - "  Soft Deleting 6 files: L0.134, L1.191, L1.192, L0.194, L0.195, L0.196"
    - "  Creating 3 files"
    - "**** Simulation run 89, type=split(ReduceOverlap)(split_times=[648]). 1 Input Files, 3.87mb total:"
    - "L0, all files 3.87mb                                                                                               "
    - "L0.199[357,658] 1.03us   |-----------------------------------------L0.199-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3.87mb total:"
    - "L0                                                                                                                 "
    - "L0.?[357,648] 1.03us 3.75mb|----------------------------------------L0.?-----------------------------------------|   "
    - "L0.?[649,658] 1.03us 131.79kb                                                                                       |L0.?|"
    - "**** Simulation run 90, type=split(ReduceOverlap)(split_times=[648]). 1 Input Files, 3.52mb total:"
    - "L0, all files 3.52mb                                                                                               "
    - "L0.205[357,658] 1.03us   |-----------------------------------------L0.205-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3.52mb total:"
    - "L0                                                                                                                 "
    - "L0.?[357,648] 1.03us 3.4mb|----------------------------------------L0.?-----------------------------------------|   "
    - "L0.?[649,658] 1.03us 119.63kb                                                                                       |L0.?|"
    - "**** Simulation run 91, type=split(ReduceOverlap)(split_times=[648]). 1 Input Files, 3.19mb total:"
    - "L0, all files 3.19mb                                                                                               "
    - "L0.209[357,658] 1.03us   |-----------------------------------------L0.209-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3.19mb total:"
    - "L0                                                                                                                 "
    - "L0.?[357,648] 1.03us 3.08mb|----------------------------------------L0.?-----------------------------------------|   "
    - "L0.?[649,658] 1.03us 108.47kb                                                                                       |L0.?|"
    - "**** Simulation run 92, type=split(ReduceOverlap)(split_times=[966]). 1 Input Files, 3.36mb total:"
    - "L0, all files 3.36mb                                                                                               "
    - "L0.145[671,986] 1.03us   |-----------------------------------------L0.145-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3.36mb total:"
    - "L0                                                                                                                 "
    - "L0.?[671,966] 1.03us 3.14mb|---------------------------------------L0.?---------------------------------------|      "
    - "L0.?[967,986] 1.03us 218.33kb                                                                                    |L0.?|"
    - "**** Simulation run 93, type=split(ReduceOverlap)(split_times=[648]). 1 Input Files, 3.87mb total:"
    - "L0, all files 3.87mb                                                                                               "
    - "L0.213[357,658] 1.03us   |-----------------------------------------L0.213-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3.87mb total:"
    - "L0                                                                                                                 "
    - "L0.?[357,648] 1.03us 3.75mb|----------------------------------------L0.?-----------------------------------------|   "
    - "L0.?[649,658] 1.03us 131.79kb                                                                                       |L0.?|"
    - "**** Simulation run 94, type=split(ReduceOverlap)(split_times=[648]). 1 Input Files, 3.52mb total:"
    - "L0, all files 3.52mb                                                                                               "
    - "L0.219[357,658] 1.04us   |-----------------------------------------L0.219-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3.52mb total:"
    - "L0                                                                                                                 "
    - "L0.?[357,648] 1.04us 3.4mb|----------------------------------------L0.?-----------------------------------------|   "
    - "L0.?[649,658] 1.04us 119.63kb                                                                                       |L0.?|"
    - "**** Simulation run 95, type=split(ReduceOverlap)(split_times=[648]). 1 Input Files, 3.19mb total:"
    - "L0, all files 3.19mb                                                                                               "
    - "L0.223[357,658] 1.04us   |-----------------------------------------L0.223-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3.19mb total:"
    - "L0                                                                                                                 "
    - "L0.?[357,648] 1.04us 3.08mb|----------------------------------------L0.?-----------------------------------------|   "
    - "L0.?[649,658] 1.04us 108.47kb                                                                                       |L0.?|"
    - "**** Simulation run 96, type=split(ReduceOverlap)(split_times=[966]). 1 Input Files, 3.36mb total:"
    - "L0, all files 3.36mb                                                                                               "
    - "L0.156[671,986] 1.04us   |-----------------------------------------L0.156-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3.36mb total:"
    - "L0                                                                                                                 "
    - "L0.?[671,966] 1.04us 3.14mb|---------------------------------------L0.?---------------------------------------|      "
    - "L0.?[967,986] 1.04us 218.33kb                                                                                    |L0.?|"
    - "**** Simulation run 97, type=split(ReduceOverlap)(split_times=[648]). 1 Input Files, 3.87mb total:"
    - "L0, all files 3.87mb                                                                                               "
    - "L0.227[357,658] 1.04us   |-----------------------------------------L0.227-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3.87mb total:"
    - "L0                                                                                                                 "
    - "L0.?[357,648] 1.04us 3.75mb|----------------------------------------L0.?-----------------------------------------|   "
    - "L0.?[649,658] 1.04us 131.79kb                                                                                       |L0.?|"
    - "**** Simulation run 98, type=split(ReduceOverlap)(split_times=[648]). 1 Input Files, 3.52mb total:"
    - "L0, all files 3.52mb                                                                                               "
    - "L0.233[357,658] 1.04us   |-----------------------------------------L0.233-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3.52mb total:"
    - "L0                                                                                                                 "
    - "L0.?[357,648] 1.04us 3.4mb|----------------------------------------L0.?-----------------------------------------|   "
    - "L0.?[649,658] 1.04us 119.63kb                                                                                       |L0.?|"
    - "**** Simulation run 99, type=split(ReduceOverlap)(split_times=[648]). 1 Input Files, 3.19mb total:"
    - "L0, all files 3.19mb                                                                                               "
    - "L0.237[357,658] 1.04us   |-----------------------------------------L0.237-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3.19mb total:"
    - "L0                                                                                                                 "
    - "L0.?[357,648] 1.04us 3.08mb|----------------------------------------L0.?-----------------------------------------|   "
    - "L0.?[649,658] 1.04us 108.47kb                                                                                       |L0.?|"
    - "**** Simulation run 100, type=split(ReduceOverlap)(split_times=[966]). 1 Input Files, 3.36mb total:"
    - "L0, all files 3.36mb                                                                                               "
    - "L0.167[671,986] 1.04us   |-----------------------------------------L0.167-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3.36mb total:"
    - "L0                                                                                                                 "
    - "L0.?[671,966] 1.04us 3.14mb|---------------------------------------L0.?---------------------------------------|      "
    - "L0.?[967,986] 1.04us 218.33kb                                                                                    |L0.?|"
    - "**** Simulation run 101, type=split(ReduceOverlap)(split_times=[648]). 1 Input Files, 3.87mb total:"
    - "L0, all files 3.87mb                                                                                               "
    - "L0.241[357,658] 1.04us   |-----------------------------------------L0.241-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3.87mb total:"
    - "L0                                                                                                                 "
    - "L0.?[357,648] 1.04us 3.75mb|----------------------------------------L0.?-----------------------------------------|   "
    - "L0.?[649,658] 1.04us 131.79kb                                                                                       |L0.?|"
    - "**** Simulation run 102, type=split(ReduceOverlap)(split_times=[648]). 1 Input Files, 3.52mb total:"
    - "L0, all files 3.52mb                                                                                               "
    - "L0.247[357,658] 1.04us   |-----------------------------------------L0.247-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3.52mb total:"
    - "L0                                                                                                                 "
    - "L0.?[357,648] 1.04us 3.4mb|----------------------------------------L0.?-----------------------------------------|   "
    - "L0.?[649,658] 1.04us 119.63kb                                                                                       |L0.?|"
    - "**** Simulation run 103, type=split(ReduceOverlap)(split_times=[648]). 1 Input Files, 3.19mb total:"
    - "L0, all files 3.19mb                                                                                               "
    - "L0.251[357,658] 1.05us   |-----------------------------------------L0.251-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3.19mb total:"
    - "L0                                                                                                                 "
    - "L0.?[357,648] 1.05us 3.08mb|----------------------------------------L0.?-----------------------------------------|   "
    - "L0.?[649,658] 1.05us 108.47kb                                                                                       |L0.?|"
    - "**** Simulation run 104, type=split(ReduceOverlap)(split_times=[966]). 1 Input Files, 3.36mb total:"
    - "L0, all files 3.36mb                                                                                               "
    - "L0.178[671,986] 1.05us   |-----------------------------------------L0.178-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3.36mb total:"
    - "L0                                                                                                                 "
    - "L0.?[671,966] 1.05us 3.14mb|---------------------------------------L0.?---------------------------------------|      "
    - "L0.?[967,986] 1.05us 218.33kb                                                                                    |L0.?|"
    - "**** Simulation run 105, type=split(ReduceOverlap)(split_times=[648]). 1 Input Files, 3.87mb total:"
    - "L0, all files 3.87mb                                                                                               "
    - "L0.255[357,658] 1.05us   |-----------------------------------------L0.255-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3.87mb total:"
    - "L0                                                                                                                 "
    - "L0.?[357,648] 1.05us 3.75mb|----------------------------------------L0.?-----------------------------------------|   "
    - "L0.?[649,658] 1.05us 131.79kb                                                                                       |L0.?|"
    - "**** Simulation run 106, type=split(ReduceOverlap)(split_times=[648]). 1 Input Files, 3.52mb total:"
    - "L0, all files 3.52mb                                                                                               "
    - "L0.261[357,658] 1.05us   |-----------------------------------------L0.261-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3.52mb total:"
    - "L0                                                                                                                 "
    - "L0.?[357,648] 1.05us 3.4mb|----------------------------------------L0.?-----------------------------------------|   "
    - "L0.?[649,658] 1.05us 119.63kb                                                                                       |L0.?|"
    - "**** Simulation run 107, type=split(ReduceOverlap)(split_times=[648]). 1 Input Files, 3.19mb total:"
    - "L0, all files 3.19mb                                                                                               "
    - "L0.265[357,658] 1.05us   |-----------------------------------------L0.265-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3.19mb total:"
    - "L0                                                                                                                 "
    - "L0.?[357,648] 1.05us 3.08mb|----------------------------------------L0.?-----------------------------------------|   "
    - "L0.?[649,658] 1.05us 108.47kb                                                                                       |L0.?|"
    - "**** Simulation run 108, type=split(ReduceOverlap)(split_times=[966]). 1 Input Files, 3.36mb total:"
    - "L0, all files 3.36mb                                                                                               "
    - "L0.189[671,986] 1.05us   |-----------------------------------------L0.189-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3.36mb total:"
    - "L0                                                                                                                 "
    - "L0.?[671,966] 1.05us 3.14mb|---------------------------------------L0.?---------------------------------------|      "
    - "L0.?[967,986] 1.05us 218.33kb                                                                                    |L0.?|"
    - "Committing partition 1:"
    - "  Soft Deleting 20 files: L0.145, L0.156, L0.167, L0.178, L0.189, L0.199, L0.205, L0.209, L0.213, L0.219, L0.223, L0.227, L0.233, L0.237, L0.241, L0.247, L0.251, L0.255, L0.261, L0.265"
    - "  Creating 40 files"
    - "**** Simulation run 109, type=split(CompactAndSplitOutput(FoundSubsetLessThanMaxCompactSize))(split_times=[263]). 2 Input Files, 103.14mb total:"
    - "L0                                                                                                                 "
    - "L0.193[42,329] 1.03us 3.04mb           |-----------------------------------L0.193-----------------------------------| "
    - "L1                                                                                                                 "
    - "L1.190[0,329] 1.03us 100.1mb|-----------------------------------------L1.190-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 103.14mb total:"
    - "L1                                                                                                                 "
    - "L1.?[0,263] 1.03us 82.45mb|--------------------------------L1.?---------------------------------|                   "
    - "L1.?[264,329] 1.03us 20.69mb                                                                        |-----L1.?------| "
    - "Committing partition 1:"
    - "  Soft Deleting 2 files: L1.190, L0.193"
    - "  Creating 2 files"
    - "**** Simulation run 110, type=split(ReduceOverlap)(split_times=[263]). 1 Input Files, 2.01mb total:"
    - "L0, all files 2.01mb                                                                                               "
    - "L0.197[173,329] 1.03us   |-----------------------------------------L0.197-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 2.01mb total:"
    - "L0                                                                                                                 "
    - "L0.?[173,263] 1.03us 1.16mb|----------------------L0.?-----------------------|                                       "
    - "L0.?[264,329] 1.03us 869.81kb                                                    |---------------L0.?----------------| "
    - "**** Simulation run 111, type=split(ReduceOverlap)(split_times=[263]). 1 Input Files, 4.82mb total:"
    - "L0, all files 4.82mb                                                                                               "
    - "L0.201[50,329] 1.03us    |-----------------------------------------L0.201-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 4.82mb total:"
    - "L0                                                                                                                 "
    - "L0.?[50,263] 1.03us 3.68mb|-------------------------------L0.?-------------------------------|                      "
    - "L0.?[264,329] 1.03us 1.14mb                                                                     |-------L0.?-------| "
    - "**** Simulation run 112, type=split(ReduceOverlap)(split_times=[263]). 1 Input Files, 2.96mb total:"
    - "L0, all files 2.96mb                                                                                               "
    - "L0.203[76,329] 1.03us    |-----------------------------------------L0.203-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 2.96mb total:"
    - "L0                                                                                                                 "
    - "L0.?[76,263] 1.03us 2.18mb|------------------------------L0.?------------------------------|                        "
    - "L0.?[264,329] 1.03us 789.53kb                                                                  |--------L0.?---------| "
    - "**** Simulation run 113, type=split(ReduceOverlap)(split_times=[263]). 1 Input Files, 3.04mb total:"
    - "L0, all files 3.04mb                                                                                               "
    - "L0.207[42,329] 1.03us    |-----------------------------------------L0.207-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3.04mb total:"
    - "L0                                                                                                                 "
    - "L0.?[42,263] 1.03us 2.34mb|-------------------------------L0.?--------------------------------|                     "
    - "L0.?[264,329] 1.03us 715.93kb                                                                     |-------L0.?-------| "
    - "**** Simulation run 114, type=split(ReduceOverlap)(split_times=[263]). 1 Input Files, 2.01mb total:"
    - "L0, all files 2.01mb                                                                                               "
    - "L0.211[173,329] 1.03us   |-----------------------------------------L0.211-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 2.01mb total:"
    - "L0                                                                                                                 "
    - "L0.?[173,263] 1.03us 1.16mb|----------------------L0.?-----------------------|                                       "
    - "L0.?[264,329] 1.03us 869.81kb                                                    |---------------L0.?----------------| "
    - "**** Simulation run 115, type=split(ReduceOverlap)(split_times=[263]). 1 Input Files, 4.82mb total:"
    - "L0, all files 4.82mb                                                                                               "
    - "L0.215[50,329] 1.03us    |-----------------------------------------L0.215-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 4.82mb total:"
    - "L0                                                                                                                 "
    - "L0.?[50,263] 1.03us 3.68mb|-------------------------------L0.?-------------------------------|                      "
    - "L0.?[264,329] 1.03us 1.14mb                                                                     |-------L0.?-------| "
    - "**** Simulation run 116, type=split(ReduceOverlap)(split_times=[263]). 1 Input Files, 2.96mb total:"
    - "L0, all files 2.96mb                                                                                               "
    - "L0.217[76,329] 1.04us    |-----------------------------------------L0.217-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 2.96mb total:"
    - "L0                                                                                                                 "
    - "L0.?[76,263] 1.04us 2.18mb|------------------------------L0.?------------------------------|                        "
    - "L0.?[264,329] 1.04us 789.53kb                                                                  |--------L0.?---------| "
    - "**** Simulation run 117, type=split(ReduceOverlap)(split_times=[263]). 1 Input Files, 3.04mb total:"
    - "L0, all files 3.04mb                                                                                               "
    - "L0.221[42,329] 1.04us    |-----------------------------------------L0.221-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3.04mb total:"
    - "L0                                                                                                                 "
    - "L0.?[42,263] 1.04us 2.34mb|-------------------------------L0.?--------------------------------|                     "
    - "L0.?[264,329] 1.04us 715.93kb                                                                     |-------L0.?-------| "
    - "**** Simulation run 118, type=split(ReduceOverlap)(split_times=[263]). 1 Input Files, 2.01mb total:"
    - "L0, all files 2.01mb                                                                                               "
    - "L0.225[173,329] 1.04us   |-----------------------------------------L0.225-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 2.01mb total:"
    - "L0                                                                                                                 "
    - "L0.?[173,263] 1.04us 1.16mb|----------------------L0.?-----------------------|                                       "
    - "L0.?[264,329] 1.04us 869.81kb                                                    |---------------L0.?----------------| "
    - "**** Simulation run 119, type=split(ReduceOverlap)(split_times=[263]). 1 Input Files, 4.82mb total:"
    - "L0, all files 4.82mb                                                                                               "
    - "L0.229[50,329] 1.04us    |-----------------------------------------L0.229-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 4.82mb total:"
    - "L0                                                                                                                 "
    - "L0.?[50,263] 1.04us 3.68mb|-------------------------------L0.?-------------------------------|                      "
    - "L0.?[264,329] 1.04us 1.14mb                                                                     |-------L0.?-------| "
    - "**** Simulation run 120, type=split(ReduceOverlap)(split_times=[263]). 1 Input Files, 2.96mb total:"
    - "L0, all files 2.96mb                                                                                               "
    - "L0.231[76,329] 1.04us    |-----------------------------------------L0.231-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 2.96mb total:"
    - "L0                                                                                                                 "
    - "L0.?[76,263] 1.04us 2.18mb|------------------------------L0.?------------------------------|                        "
    - "L0.?[264,329] 1.04us 789.53kb                                                                  |--------L0.?---------| "
    - "**** Simulation run 121, type=split(ReduceOverlap)(split_times=[263]). 1 Input Files, 3.04mb total:"
    - "L0, all files 3.04mb                                                                                               "
    - "L0.235[42,329] 1.04us    |-----------------------------------------L0.235-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3.04mb total:"
    - "L0                                                                                                                 "
    - "L0.?[42,263] 1.04us 2.34mb|-------------------------------L0.?--------------------------------|                     "
    - "L0.?[264,329] 1.04us 715.93kb                                                                     |-------L0.?-------| "
    - "**** Simulation run 122, type=split(ReduceOverlap)(split_times=[263]). 1 Input Files, 2.01mb total:"
    - "L0, all files 2.01mb                                                                                               "
    - "L0.239[173,329] 1.04us   |-----------------------------------------L0.239-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 2.01mb total:"
    - "L0                                                                                                                 "
    - "L0.?[173,263] 1.04us 1.16mb|----------------------L0.?-----------------------|                                       "
    - "L0.?[264,329] 1.04us 869.81kb                                                    |---------------L0.?----------------| "
    - "**** Simulation run 123, type=split(ReduceOverlap)(split_times=[263]). 1 Input Files, 4.82mb total:"
    - "L0, all files 4.82mb                                                                                               "
    - "L0.243[50,329] 1.04us    |-----------------------------------------L0.243-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 4.82mb total:"
    - "L0                                                                                                                 "
    - "L0.?[50,263] 1.04us 3.68mb|-------------------------------L0.?-------------------------------|                      "
    - "L0.?[264,329] 1.04us 1.14mb                                                                     |-------L0.?-------| "
    - "**** Simulation run 124, type=split(ReduceOverlap)(split_times=[263]). 1 Input Files, 2.96mb total:"
    - "L0, all files 2.96mb                                                                                               "
    - "L0.245[76,329] 1.04us    |-----------------------------------------L0.245-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 2.96mb total:"
    - "L0                                                                                                                 "
    - "L0.?[76,263] 1.04us 2.18mb|------------------------------L0.?------------------------------|                        "
    - "L0.?[264,329] 1.04us 789.53kb                                                                  |--------L0.?---------| "
    - "**** Simulation run 125, type=split(ReduceOverlap)(split_times=[263]). 1 Input Files, 3.04mb total:"
    - "L0, all files 3.04mb                                                                                               "
    - "L0.249[42,329] 1.05us    |-----------------------------------------L0.249-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3.04mb total:"
    - "L0                                                                                                                 "
    - "L0.?[42,263] 1.05us 2.34mb|-------------------------------L0.?--------------------------------|                     "
    - "L0.?[264,329] 1.05us 715.93kb                                                                     |-------L0.?-------| "
    - "**** Simulation run 126, type=split(ReduceOverlap)(split_times=[263]). 1 Input Files, 2.01mb total:"
    - "L0, all files 2.01mb                                                                                               "
    - "L0.253[173,329] 1.05us   |-----------------------------------------L0.253-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 2.01mb total:"
    - "L0                                                                                                                 "
    - "L0.?[173,263] 1.05us 1.16mb|----------------------L0.?-----------------------|                                       "
    - "L0.?[264,329] 1.05us 869.81kb                                                    |---------------L0.?----------------| "
    - "**** Simulation run 127, type=split(ReduceOverlap)(split_times=[263]). 1 Input Files, 4.82mb total:"
    - "L0, all files 4.82mb                                                                                               "
    - "L0.257[50,329] 1.05us    |-----------------------------------------L0.257-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 4.82mb total:"
    - "L0                                                                                                                 "
    - "L0.?[50,263] 1.05us 3.68mb|-------------------------------L0.?-------------------------------|                      "
    - "L0.?[264,329] 1.05us 1.14mb                                                                     |-------L0.?-------| "
    - "**** Simulation run 128, type=split(ReduceOverlap)(split_times=[263]). 1 Input Files, 2.96mb total:"
    - "L0, all files 2.96mb                                                                                               "
    - "L0.259[76,329] 1.05us    |-----------------------------------------L0.259-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 2.96mb total:"
    - "L0                                                                                                                 "
    - "L0.?[76,263] 1.05us 2.18mb|------------------------------L0.?------------------------------|                        "
    - "L0.?[264,329] 1.05us 789.53kb                                                                  |--------L0.?---------| "
    - "**** Simulation run 129, type=split(ReduceOverlap)(split_times=[263]). 1 Input Files, 3.04mb total:"
    - "L0, all files 3.04mb                                                                                               "
    - "L0.263[42,329] 1.05us    |-----------------------------------------L0.263-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3.04mb total:"
    - "L0                                                                                                                 "
    - "L0.?[42,263] 1.05us 2.34mb|-------------------------------L0.?--------------------------------|                     "
    - "L0.?[264,329] 1.05us 715.93kb                                                                     |-------L0.?-------| "
    - "Committing partition 1:"
    - "  Soft Deleting 20 files: L0.197, L0.201, L0.203, L0.207, L0.211, L0.215, L0.217, L0.221, L0.225, L0.229, L0.231, L0.235, L0.239, L0.243, L0.245, L0.249, L0.253, L0.257, L0.259, L0.263"
    - "  Creating 40 files"
    - "**** Simulation run 130, type=split(CompactAndSplitOutput(FoundSubsetLessThanMaxCompactSize))(split_times=[570, 876]). 9 Input Files, 229.77mb total:"
    - "L0                                                                                                                 "
    - "L0.137[671,950] 1.03us 3.62mb                                                    |-------------L0.137--------------|   "
    - "L0.200[659,670] 1.03us 158.15kb                                                  |L0.200|                                "
    - "L0.198[330,356] 1.03us 355.83kb        |L0.198|                                                                          "
    - "L0.271[649,658] 1.03us 131.79kb                                                 |L0.271|                                 "
    - "L0.270[357,648] 1.03us 3.75mb           |--------------L0.270---------------|                                          "
    - "L0.313[264,329] 1.03us 869.81kb|L0.313|                                                                                  "
    - "L1                                                                                                                 "
    - "L1.268[649,966] 1.03us 99.96mb                                                 |----------------L1.268----------------| "
    - "L1.267[330,648] 1.03us 100.28mb        |----------------L1.267----------------|                                          "
    - "L1.311[264,329] 1.03us 20.69mb|L1.311|                                                                                  "
    - "**** 3 Output Files (parquet_file_id not yet assigned), 229.77mb total:"
    - "L1                                                                                                                 "
    - "L1.?[264,570] 1.03us 100.16mb|----------------L1.?-----------------|                                                   "
    - "L1.?[571,876] 1.03us 99.83mb                                       |----------------L1.?-----------------|            "
    - "L1.?[877,966] 1.03us 29.78mb                                                                              |--L1.?---| "
    - "Committing partition 1:"
    - "  Soft Deleting 9 files: L0.137, L0.198, L0.200, L1.267, L1.268, L0.270, L0.271, L1.311, L0.313"
    - "  Creating 3 files"
    - "**** Simulation run 131, type=split(ReduceOverlap)(split_times=[570]). 1 Input Files, 4.72mb total:"
    - "L0, all files 4.72mb                                                                                               "
    - "L0.139[357,629] 1.03us   |-----------------------------------------L0.139-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 4.72mb total:"
    - "L0                                                                                                                 "
    - "L0.?[357,570] 1.03us 3.69mb|--------------------------------L0.?--------------------------------|                    "
    - "L0.?[571,629] 1.03us 1.02mb                                                                      |------L0.?-------| "
    - "**** Simulation run 132, type=split(ReduceOverlap)(split_times=[570]). 1 Input Files, 3.4mb total:"
    - "L0, all files 3.4mb                                                                                                "
    - "L0.272[357,648] 1.03us   |-----------------------------------------L0.272-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3.4mb total:"
    - "L0                                                                                                                 "
    - "L0.?[357,570] 1.03us 2.49mb|-----------------------------L0.?------------------------------|                         "
    - "L0.?[571,648] 1.03us 933.08kb                                                                  |--------L0.?---------| "
    - "**** Simulation run 133, type=split(ReduceOverlap)(split_times=[876]). 1 Input Files, 3.07mb total:"
    - "L0, all files 3.07mb                                                                                               "
    - "L0.142[671,932] 1.03us   |-----------------------------------------L0.142-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3.07mb total:"
    - "L0                                                                                                                 "
    - "L0.?[671,876] 1.03us 2.41mb|--------------------------------L0.?--------------------------------|                    "
    - "L0.?[877,932] 1.03us 675.04kb                                                                       |------L0.?------| "
    - "**** Simulation run 134, type=split(ReduceOverlap)(split_times=[570]). 1 Input Files, 3.08mb total:"
    - "L0, all files 3.08mb                                                                                               "
    - "L0.274[357,648] 1.03us   |-----------------------------------------L0.274-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3.08mb total:"
    - "L0                                                                                                                 "
    - "L0.?[357,570] 1.03us 2.26mb|-----------------------------L0.?------------------------------|                         "
    - "L0.?[571,648] 1.03us 846.1kb                                                                  |--------L0.?---------| "
    - "**** Simulation run 135, type=split(ReduceOverlap)(split_times=[876]). 1 Input Files, 3.14mb total:"
    - "L0, all files 3.14mb                                                                                               "
    - "L0.276[671,966] 1.03us   |-----------------------------------------L0.276-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3.14mb total:"
    - "L0                                                                                                                 "
    - "L0.?[671,876] 1.03us 2.19mb|----------------------------L0.?----------------------------|                            "
    - "L0.?[877,966] 1.03us 982.47kb                                                              |----------L0.?-----------| "
    - "**** Simulation run 136, type=split(ReduceOverlap)(split_times=[570]). 1 Input Files, 3.75mb total:"
    - "L0, all files 3.75mb                                                                                               "
    - "L0.278[357,648] 1.03us   |-----------------------------------------L0.278-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3.75mb total:"
    - "L0                                                                                                                 "
    - "L0.?[357,570] 1.03us 2.74mb|-----------------------------L0.?------------------------------|                         "
    - "L0.?[571,648] 1.03us 1mb                                                                   |--------L0.?---------| "
    - "**** Simulation run 137, type=split(ReduceOverlap)(split_times=[876]). 1 Input Files, 3.62mb total:"
    - "L0, all files 3.62mb                                                                                               "
    - "L0.148[671,950] 1.03us   |-----------------------------------------L0.148-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3.62mb total:"
    - "L0                                                                                                                 "
    - "L0.?[671,876] 1.03us 2.66mb|------------------------------L0.?------------------------------|                        "
    - "L0.?[877,950] 1.03us 982.23kb                                                                  |--------L0.?---------| "
    - "**** Simulation run 138, type=split(ReduceOverlap)(split_times=[570]). 1 Input Files, 4.72mb total:"
    - "L0, all files 4.72mb                                                                                               "
    - "L0.150[357,629] 1.03us   |-----------------------------------------L0.150-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 4.72mb total:"
    - "L0                                                                                                                 "
    - "L0.?[357,570] 1.03us 3.69mb|--------------------------------L0.?--------------------------------|                    "
    - "L0.?[571,629] 1.03us 1.02mb                                                                      |------L0.?-------| "
    - "**** Simulation run 139, type=split(ReduceOverlap)(split_times=[570]). 1 Input Files, 3.4mb total:"
    - "L0, all files 3.4mb                                                                                                "
    - "L0.280[357,648] 1.04us   |-----------------------------------------L0.280-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3.4mb total:"
    - "L0                                                                                                                 "
    - "L0.?[357,570] 1.04us 2.49mb|-----------------------------L0.?------------------------------|                         "
    - "L0.?[571,648] 1.04us 933.08kb                                                                  |--------L0.?---------| "
    - "**** Simulation run 140, type=split(ReduceOverlap)(split_times=[876]). 1 Input Files, 3.07mb total:"
    - "L0, all files 3.07mb                                                                                               "
    - "L0.153[671,932] 1.04us   |-----------------------------------------L0.153-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3.07mb total:"
    - "L0                                                                                                                 "
    - "L0.?[671,876] 1.04us 2.41mb|--------------------------------L0.?--------------------------------|                    "
    - "L0.?[877,932] 1.04us 675.04kb                                                                       |------L0.?------| "
    - "**** Simulation run 141, type=split(ReduceOverlap)(split_times=[570]). 1 Input Files, 3.08mb total:"
    - "L0, all files 3.08mb                                                                                               "
    - "L0.282[357,648] 1.04us   |-----------------------------------------L0.282-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3.08mb total:"
    - "L0                                                                                                                 "
    - "L0.?[357,570] 1.04us 2.26mb|-----------------------------L0.?------------------------------|                         "
    - "L0.?[571,648] 1.04us 846.1kb                                                                  |--------L0.?---------| "
    - "**** Simulation run 142, type=split(ReduceOverlap)(split_times=[876]). 1 Input Files, 3.14mb total:"
    - "L0, all files 3.14mb                                                                                               "
    - "L0.284[671,966] 1.04us   |-----------------------------------------L0.284-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3.14mb total:"
    - "L0                                                                                                                 "
    - "L0.?[671,876] 1.04us 2.19mb|----------------------------L0.?----------------------------|                            "
    - "L0.?[877,966] 1.04us 982.47kb                                                              |----------L0.?-----------| "
    - "**** Simulation run 143, type=split(ReduceOverlap)(split_times=[570]). 1 Input Files, 3.75mb total:"
    - "L0, all files 3.75mb                                                                                               "
    - "L0.286[357,648] 1.04us   |-----------------------------------------L0.286-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3.75mb total:"
    - "L0                                                                                                                 "
    - "L0.?[357,570] 1.04us 2.74mb|-----------------------------L0.?------------------------------|                         "
    - "L0.?[571,648] 1.04us 1mb                                                                   |--------L0.?---------| "
    - "**** Simulation run 144, type=split(ReduceOverlap)(split_times=[876]). 1 Input Files, 3.62mb total:"
    - "L0, all files 3.62mb                                                                                               "
    - "L0.159[671,950] 1.04us   |-----------------------------------------L0.159-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3.62mb total:"
    - "L0                                                                                                                 "
    - "L0.?[671,876] 1.04us 2.66mb|------------------------------L0.?------------------------------|                        "
    - "L0.?[877,950] 1.04us 982.23kb                                                                  |--------L0.?---------| "
    - "**** Simulation run 145, type=split(ReduceOverlap)(split_times=[570]). 1 Input Files, 4.72mb total:"
    - "L0, all files 4.72mb                                                                                               "
    - "L0.161[357,629] 1.04us   |-----------------------------------------L0.161-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 4.72mb total:"
    - "L0                                                                                                                 "
    - "L0.?[357,570] 1.04us 3.69mb|--------------------------------L0.?--------------------------------|                    "
    - "L0.?[571,629] 1.04us 1.02mb                                                                      |------L0.?-------| "
    - "**** Simulation run 146, type=split(ReduceOverlap)(split_times=[570]). 1 Input Files, 3.4mb total:"
    - "L0, all files 3.4mb                                                                                                "
    - "L0.288[357,648] 1.04us   |-----------------------------------------L0.288-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3.4mb total:"
    - "L0                                                                                                                 "
    - "L0.?[357,570] 1.04us 2.49mb|-----------------------------L0.?------------------------------|                         "
    - "L0.?[571,648] 1.04us 933.08kb                                                                  |--------L0.?---------| "
    - "**** Simulation run 147, type=split(ReduceOverlap)(split_times=[876]). 1 Input Files, 3.07mb total:"
    - "L0, all files 3.07mb                                                                                               "
    - "L0.164[671,932] 1.04us   |-----------------------------------------L0.164-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3.07mb total:"
    - "L0                                                                                                                 "
    - "L0.?[671,876] 1.04us 2.41mb|--------------------------------L0.?--------------------------------|                    "
    - "L0.?[877,932] 1.04us 675.04kb                                                                       |------L0.?------| "
    - "**** Simulation run 148, type=split(ReduceOverlap)(split_times=[570]). 1 Input Files, 3.08mb total:"
    - "L0, all files 3.08mb                                                                                               "
    - "L0.290[357,648] 1.04us   |-----------------------------------------L0.290-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3.08mb total:"
    - "L0                                                                                                                 "
    - "L0.?[357,570] 1.04us 2.26mb|-----------------------------L0.?------------------------------|                         "
    - "L0.?[571,648] 1.04us 846.1kb                                                                  |--------L0.?---------| "
    - "**** Simulation run 149, type=split(ReduceOverlap)(split_times=[876]). 1 Input Files, 3.14mb total:"
    - "L0, all files 3.14mb                                                                                               "
    - "L0.292[671,966] 1.04us   |-----------------------------------------L0.292-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3.14mb total:"
    - "L0                                                                                                                 "
    - "L0.?[671,876] 1.04us 2.19mb|----------------------------L0.?----------------------------|                            "
    - "L0.?[877,966] 1.04us 982.47kb                                                              |----------L0.?-----------| "
    - "**** Simulation run 150, type=split(ReduceOverlap)(split_times=[570]). 1 Input Files, 3.75mb total:"
    - "L0, all files 3.75mb                                                                                               "
    - "L0.294[357,648] 1.04us   |-----------------------------------------L0.294-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3.75mb total:"
    - "L0                                                                                                                 "
    - "L0.?[357,570] 1.04us 2.74mb|-----------------------------L0.?------------------------------|                         "
    - "L0.?[571,648] 1.04us 1mb                                                                   |--------L0.?---------| "
    - "**** Simulation run 151, type=split(ReduceOverlap)(split_times=[876]). 1 Input Files, 3.62mb total:"
    - "L0, all files 3.62mb                                                                                               "
    - "L0.170[671,950] 1.04us   |-----------------------------------------L0.170-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3.62mb total:"
    - "L0                                                                                                                 "
    - "L0.?[671,876] 1.04us 2.66mb|------------------------------L0.?------------------------------|                        "
    - "L0.?[877,950] 1.04us 982.23kb                                                                  |--------L0.?---------| "
    - "**** Simulation run 152, type=split(ReduceOverlap)(split_times=[570]). 1 Input Files, 4.72mb total:"
    - "L0, all files 4.72mb                                                                                               "
    - "L0.172[357,629] 1.04us   |-----------------------------------------L0.172-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 4.72mb total:"
    - "L0                                                                                                                 "
    - "L0.?[357,570] 1.04us 3.69mb|--------------------------------L0.?--------------------------------|                    "
    - "L0.?[571,629] 1.04us 1.02mb                                                                      |------L0.?-------| "
    - "**** Simulation run 153, type=split(ReduceOverlap)(split_times=[570]). 1 Input Files, 3.4mb total:"
    - "L0, all files 3.4mb                                                                                                "
    - "L0.296[357,648] 1.04us   |-----------------------------------------L0.296-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3.4mb total:"
    - "L0                                                                                                                 "
    - "L0.?[357,570] 1.04us 2.49mb|-----------------------------L0.?------------------------------|                         "
    - "L0.?[571,648] 1.04us 933.08kb                                                                  |--------L0.?---------| "
    - "**** Simulation run 154, type=split(ReduceOverlap)(split_times=[876]). 1 Input Files, 3.07mb total:"
    - "L0, all files 3.07mb                                                                                               "
    - "L0.175[671,932] 1.04us   |-----------------------------------------L0.175-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3.07mb total:"
    - "L0                                                                                                                 "
    - "L0.?[671,876] 1.04us 2.41mb|--------------------------------L0.?--------------------------------|                    "
    - "L0.?[877,932] 1.04us 675.04kb                                                                       |------L0.?------| "
    - "**** Simulation run 155, type=split(ReduceOverlap)(split_times=[570]). 1 Input Files, 3.08mb total:"
    - "L0, all files 3.08mb                                                                                               "
    - "L0.298[357,648] 1.05us   |-----------------------------------------L0.298-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3.08mb total:"
    - "L0                                                                                                                 "
    - "L0.?[357,570] 1.05us 2.26mb|-----------------------------L0.?------------------------------|                         "
    - "L0.?[571,648] 1.05us 846.1kb                                                                  |--------L0.?---------| "
    - "**** Simulation run 156, type=split(ReduceOverlap)(split_times=[876]). 1 Input Files, 3.14mb total:"
    - "L0, all files 3.14mb                                                                                               "
    - "L0.300[671,966] 1.05us   |-----------------------------------------L0.300-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3.14mb total:"
    - "L0                                                                                                                 "
    - "L0.?[671,876] 1.05us 2.19mb|----------------------------L0.?----------------------------|                            "
    - "L0.?[877,966] 1.05us 982.47kb                                                              |----------L0.?-----------| "
    - "**** Simulation run 157, type=split(ReduceOverlap)(split_times=[570]). 1 Input Files, 3.75mb total:"
    - "L0, all files 3.75mb                                                                                               "
    - "L0.302[357,648] 1.05us   |-----------------------------------------L0.302-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3.75mb total:"
    - "L0                                                                                                                 "
    - "L0.?[357,570] 1.05us 2.74mb|-----------------------------L0.?------------------------------|                         "
    - "L0.?[571,648] 1.05us 1mb                                                                   |--------L0.?---------| "
    - "**** Simulation run 158, type=split(ReduceOverlap)(split_times=[876]). 1 Input Files, 3.62mb total:"
    - "L0, all files 3.62mb                                                                                               "
    - "L0.181[671,950] 1.05us   |-----------------------------------------L0.181-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3.62mb total:"
    - "L0                                                                                                                 "
    - "L0.?[671,876] 1.05us 2.66mb|------------------------------L0.?------------------------------|                        "
    - "L0.?[877,950] 1.05us 982.23kb                                                                  |--------L0.?---------| "
    - "**** Simulation run 159, type=split(ReduceOverlap)(split_times=[570]). 1 Input Files, 4.72mb total:"
    - "L0, all files 4.72mb                                                                                               "
    - "L0.183[357,629] 1.05us   |-----------------------------------------L0.183-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 4.72mb total:"
    - "L0                                                                                                                 "
    - "L0.?[357,570] 1.05us 3.69mb|--------------------------------L0.?--------------------------------|                    "
    - "L0.?[571,629] 1.05us 1.02mb                                                                      |------L0.?-------| "
    - "**** Simulation run 160, type=split(ReduceOverlap)(split_times=[570]). 1 Input Files, 3.4mb total:"
    - "L0, all files 3.4mb                                                                                                "
    - "L0.304[357,648] 1.05us   |-----------------------------------------L0.304-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3.4mb total:"
    - "L0                                                                                                                 "
    - "L0.?[357,570] 1.05us 2.49mb|-----------------------------L0.?------------------------------|                         "
    - "L0.?[571,648] 1.05us 933.08kb                                                                  |--------L0.?---------| "
    - "**** Simulation run 161, type=split(ReduceOverlap)(split_times=[876]). 1 Input Files, 3.07mb total:"
    - "L0, all files 3.07mb                                                                                               "
    - "L0.186[671,932] 1.05us   |-----------------------------------------L0.186-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3.07mb total:"
    - "L0                                                                                                                 "
    - "L0.?[671,876] 1.05us 2.41mb|--------------------------------L0.?--------------------------------|                    "
    - "L0.?[877,932] 1.05us 675.04kb                                                                       |------L0.?------| "
    - "**** Simulation run 162, type=split(ReduceOverlap)(split_times=[570]). 1 Input Files, 3.08mb total:"
    - "L0, all files 3.08mb                                                                                               "
    - "L0.306[357,648] 1.05us   |-----------------------------------------L0.306-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3.08mb total:"
    - "L0                                                                                                                 "
    - "L0.?[357,570] 1.05us 2.26mb|-----------------------------L0.?------------------------------|                         "
    - "L0.?[571,648] 1.05us 846.1kb                                                                  |--------L0.?---------| "
    - "**** Simulation run 163, type=split(ReduceOverlap)(split_times=[876]). 1 Input Files, 3.14mb total:"
    - "L0, all files 3.14mb                                                                                               "
    - "L0.308[671,966] 1.05us   |-----------------------------------------L0.308-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3.14mb total:"
    - "L0                                                                                                                 "
    - "L0.?[671,876] 1.05us 2.19mb|----------------------------L0.?----------------------------|                            "
    - "L0.?[877,966] 1.05us 982.47kb                                                              |----------L0.?-----------| "
    - "Committing partition 1:"
    - "  Soft Deleting 33 files: L0.139, L0.142, L0.148, L0.150, L0.153, L0.159, L0.161, L0.164, L0.170, L0.172, L0.175, L0.181, L0.183, L0.186, L0.272, L0.274, L0.276, L0.278, L0.280, L0.282, L0.284, L0.286, L0.288, L0.290, L0.292, L0.294, L0.296, L0.298, L0.300, L0.302, L0.304, L0.306, L0.308"
    - "  Creating 66 files"
    - "**** Simulation run 164, type=split(CompactAndSplitOutput(FoundSubsetLessThanMaxCompactSize))(split_times=[295, 590]). 14 Input Files, 297.12mb total:"
    - "L0                                                                                                                 "
    - "L0.312[173,263] 1.03us 1.16mb                 |L0.312-|                                                                "
    - "L0.202[330,356] 1.03us 477.51kb                                 |L0.202|                                                 "
    - "L0.315[264,329] 1.03us 1.14mb                           |L0.315|                                                       "
    - "L0.314[50,263] 1.03us 3.68mb     |------L0.314-------|                                                                "
    - "L0.356[571,629] 1.03us 1.02mb                                                          |L0.356|                        "
    - "L0.355[357,570] 1.03us 3.69mb                                    |------L0.355-------|                                 "
    - "L0.206[659,670] 1.03us 143.55kb                                                                   |L0.206|               "
    - "L0.204[330,356] 1.03us 322.99kb                                 |L0.204|                                                 "
    - "L0.273[649,658] 1.03us 119.63kb                                                                  |L0.273|                "
    - "L0.317[264,329] 1.03us 789.53kb                           |L0.317|                                                       "
    - "L0.316[76,263] 1.03us 2.18mb       |-----L0.316------|                                                                "
    - "L1                                                                                                                 "
    - "L1.310[0,263] 1.03us 82.45mb|---------L1.310----------|                                                               "
    - "L1.352[264,570] 1.03us 100.16mb                           |-----------L1.352------------|                                "
    - "L1.353[571,876] 1.03us 99.83mb                                                          |-----------L1.353------------| "
    - "**** 3 Output Files (parquet_file_id not yet assigned), 297.12mb total:"
    - "L1                                                                                                                 "
    - "L1.?[0,295] 1.03us 100.06mb|------------L1.?------------|                                                            "
    - "L1.?[296,590] 1.03us 99.72mb                              |------------L1.?------------|                              "
    - "L1.?[591,876] 1.03us 97.34mb                                                            |-----------L1.?------------| "
    - "Committing partition 1:"
    - "  Soft Deleting 14 files: L0.202, L0.204, L0.206, L0.273, L1.310, L0.312, L0.314, L0.315, L0.316, L0.317, L1.352, L1.353, L0.355, L0.356"
    - "  Creating 3 files"
    - "**** Simulation run 165, type=split(ReduceOverlap)(split_times=[590]). 1 Input Files, 933.08kb total:"
    - "L0, all files 933.08kb                                                                                             "
    - "L0.358[571,648] 1.03us   |-----------------------------------------L0.358-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 933.08kb total:"
    - "L0                                                                                                                 "
    - "L0.?[571,590] 1.03us 230.24kb|--------L0.?--------|                                                                    "
    - "L0.?[591,648] 1.03us 702.84kb                       |------------------------------L0.?------------------------------| "
    - "**** Simulation run 166, type=split(ReduceOverlap)(split_times=[590]). 1 Input Files, 846.1kb total:"
    - "L0, all files 846.1kb                                                                                              "
    - "L0.362[571,648] 1.03us   |-----------------------------------------L0.362-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 846.1kb total:"
    - "L0                                                                                                                 "
    - "L0.?[571,590] 1.03us 208.78kb|--------L0.?--------|                                                                    "
    - "L0.?[591,648] 1.03us 637.32kb                       |------------------------------L0.?------------------------------| "
    - "**** Simulation run 167, type=split(ReduceOverlap)(split_times=[295]). 1 Input Files, 715.93kb total:"
    - "L0, all files 715.93kb                                                                                             "
    - "L0.319[264,329] 1.03us   |-----------------------------------------L0.319-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 715.93kb total:"
    - "L0                                                                                                                 "
    - "L0.?[264,295] 1.03us 341.44kb|------------------L0.?------------------|                                                "
    - "L0.?[296,329] 1.03us 374.49kb                                            |-------------------L0.?--------------------| "
    - "**** Simulation run 168, type=split(ReduceOverlap)(split_times=[590]). 1 Input Files, 1mb total:"
    - "L0, all files 1mb                                                                                                  "
    - "L0.366[571,648] 1.03us   |-----------------------------------------L0.366-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 1mb total:"
    - "L0                                                                                                                 "
    - "L0.?[571,590] 1.03us 253.65kb|--------L0.?--------|                                                                    "
    - "L0.?[591,648] 1.03us 774.3kb                       |------------------------------L0.?------------------------------| "
    - "**** Simulation run 169, type=split(ReduceOverlap)(split_times=[295]). 1 Input Files, 869.81kb total:"
    - "L0, all files 869.81kb                                                                                             "
    - "L0.321[264,329] 1.03us   |-----------------------------------------L0.321-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 869.81kb total:"
    - "L0                                                                                                                 "
    - "L0.?[264,295] 1.03us 414.83kb|------------------L0.?------------------|                                                "
    - "L0.?[296,329] 1.03us 454.98kb                                            |-------------------L0.?--------------------| "
    - "**** Simulation run 170, type=split(ReduceOverlap)(split_times=[590]). 1 Input Files, 1.02mb total:"
    - "L0, all files 1.02mb                                                                                               "
    - "L0.370[571,629] 1.03us   |-----------------------------------------L0.370-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 1.02mb total:"
    - "L0                                                                                                                 "
    - "L0.?[571,590] 1.03us 343.08kb|-----------L0.?------------|                                                             "
    - "L0.?[591,629] 1.03us 704.21kb                               |--------------------------L0.?--------------------------| "
    - "**** Simulation run 171, type=split(ReduceOverlap)(split_times=[295]). 1 Input Files, 1.14mb total:"
    - "L0, all files 1.14mb                                                                                               "
    - "L0.323[264,329] 1.03us   |-----------------------------------------L0.323-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 1.14mb total:"
    - "L0                                                                                                                 "
    - "L0.?[264,295] 1.03us 556.69kb|------------------L0.?------------------|                                                "
    - "L0.?[296,329] 1.03us 610.56kb                                            |-------------------L0.?--------------------| "
    - "**** Simulation run 172, type=split(ReduceOverlap)(split_times=[590]). 1 Input Files, 933.08kb total:"
    - "L0, all files 933.08kb                                                                                             "
    - "L0.372[571,648] 1.04us   |-----------------------------------------L0.372-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 933.08kb total:"
    - "L0                                                                                                                 "
    - "L0.?[571,590] 1.04us 230.24kb|--------L0.?--------|                                                                    "
    - "L0.?[591,648] 1.04us 702.84kb                       |------------------------------L0.?------------------------------| "
    - "**** Simulation run 173, type=split(ReduceOverlap)(split_times=[295]). 1 Input Files, 789.53kb total:"
    - "L0, all files 789.53kb                                                                                             "
    - "L0.325[264,329] 1.04us   |-----------------------------------------L0.325-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 789.53kb total:"
    - "L0                                                                                                                 "
    - "L0.?[264,295] 1.04us 376.55kb|------------------L0.?------------------|                                                "
    - "L0.?[296,329] 1.04us 412.99kb                                            |-------------------L0.?--------------------| "
    - "**** Simulation run 174, type=split(ReduceOverlap)(split_times=[590]). 1 Input Files, 846.1kb total:"
    - "L0, all files 846.1kb                                                                                              "
    - "L0.376[571,648] 1.04us   |-----------------------------------------L0.376-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 846.1kb total:"
    - "L0                                                                                                                 "
    - "L0.?[571,590] 1.04us 208.78kb|--------L0.?--------|                                                                    "
    - "L0.?[591,648] 1.04us 637.32kb                       |------------------------------L0.?------------------------------| "
    - "**** Simulation run 175, type=split(ReduceOverlap)(split_times=[295]). 1 Input Files, 715.93kb total:"
    - "L0, all files 715.93kb                                                                                             "
    - "L0.327[264,329] 1.04us   |-----------------------------------------L0.327-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 715.93kb total:"
    - "L0                                                                                                                 "
    - "L0.?[264,295] 1.04us 341.44kb|------------------L0.?------------------|                                                "
    - "L0.?[296,329] 1.04us 374.49kb                                            |-------------------L0.?--------------------| "
    - "**** Simulation run 176, type=split(ReduceOverlap)(split_times=[590]). 1 Input Files, 1mb total:"
    - "L0, all files 1mb                                                                                                  "
    - "L0.380[571,648] 1.04us   |-----------------------------------------L0.380-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 1mb total:"
    - "L0                                                                                                                 "
    - "L0.?[571,590] 1.04us 253.65kb|--------L0.?--------|                                                                    "
    - "L0.?[591,648] 1.04us 774.3kb                       |------------------------------L0.?------------------------------| "
    - "**** Simulation run 177, type=split(ReduceOverlap)(split_times=[295]). 1 Input Files, 869.81kb total:"
    - "L0, all files 869.81kb                                                                                             "
    - "L0.329[264,329] 1.04us   |-----------------------------------------L0.329-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 869.81kb total:"
    - "L0                                                                                                                 "
    - "L0.?[264,295] 1.04us 414.83kb|------------------L0.?------------------|                                                "
    - "L0.?[296,329] 1.04us 454.98kb                                            |-------------------L0.?--------------------| "
    - "**** Simulation run 178, type=split(ReduceOverlap)(split_times=[590]). 1 Input Files, 1.02mb total:"
    - "L0, all files 1.02mb                                                                                               "
    - "L0.384[571,629] 1.04us   |-----------------------------------------L0.384-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 1.02mb total:"
    - "L0                                                                                                                 "
    - "L0.?[571,590] 1.04us 343.08kb|-----------L0.?------------|                                                             "
    - "L0.?[591,629] 1.04us 704.21kb                               |--------------------------L0.?--------------------------| "
    - "**** Simulation run 179, type=split(ReduceOverlap)(split_times=[295]). 1 Input Files, 1.14mb total:"
    - "L0, all files 1.14mb                                                                                               "
    - "L0.331[264,329] 1.04us   |-----------------------------------------L0.331-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 1.14mb total:"
    - "L0                                                                                                                 "
    - "L0.?[264,295] 1.04us 556.69kb|------------------L0.?------------------|                                                "
    - "L0.?[296,329] 1.04us 610.56kb                                            |-------------------L0.?--------------------| "
    - "**** Simulation run 180, type=split(ReduceOverlap)(split_times=[590]). 1 Input Files, 933.08kb total:"
    - "L0, all files 933.08kb                                                                                             "
    - "L0.386[571,648] 1.04us   |-----------------------------------------L0.386-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 933.08kb total:"
    - "L0                                                                                                                 "
    - "L0.?[571,590] 1.04us 230.24kb|--------L0.?--------|                                                                    "
    - "L0.?[591,648] 1.04us 702.84kb                       |------------------------------L0.?------------------------------| "
    - "**** Simulation run 181, type=split(ReduceOverlap)(split_times=[295]). 1 Input Files, 789.53kb total:"
    - "L0, all files 789.53kb                                                                                             "
    - "L0.333[264,329] 1.04us   |-----------------------------------------L0.333-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 789.53kb total:"
    - "L0                                                                                                                 "
    - "L0.?[264,295] 1.04us 376.55kb|------------------L0.?------------------|                                                "
    - "L0.?[296,329] 1.04us 412.99kb                                            |-------------------L0.?--------------------| "
    - "**** Simulation run 182, type=split(ReduceOverlap)(split_times=[590]). 1 Input Files, 846.1kb total:"
    - "L0, all files 846.1kb                                                                                              "
    - "L0.390[571,648] 1.04us   |-----------------------------------------L0.390-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 846.1kb total:"
    - "L0                                                                                                                 "
    - "L0.?[571,590] 1.04us 208.78kb|--------L0.?--------|                                                                    "
    - "L0.?[591,648] 1.04us 637.32kb                       |------------------------------L0.?------------------------------| "
    - "**** Simulation run 183, type=split(ReduceOverlap)(split_times=[295]). 1 Input Files, 715.93kb total:"
    - "L0, all files 715.93kb                                                                                             "
    - "L0.335[264,329] 1.04us   |-----------------------------------------L0.335-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 715.93kb total:"
    - "L0                                                                                                                 "
    - "L0.?[264,295] 1.04us 341.44kb|------------------L0.?------------------|                                                "
    - "L0.?[296,329] 1.04us 374.49kb                                            |-------------------L0.?--------------------| "
    - "**** Simulation run 184, type=split(ReduceOverlap)(split_times=[590]). 1 Input Files, 1mb total:"
    - "L0, all files 1mb                                                                                                  "
    - "L0.394[571,648] 1.04us   |-----------------------------------------L0.394-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 1mb total:"
    - "L0                                                                                                                 "
    - "L0.?[571,590] 1.04us 253.65kb|--------L0.?--------|                                                                    "
    - "L0.?[591,648] 1.04us 774.3kb                       |------------------------------L0.?------------------------------| "
    - "**** Simulation run 185, type=split(ReduceOverlap)(split_times=[295]). 1 Input Files, 869.81kb total:"
    - "L0, all files 869.81kb                                                                                             "
    - "L0.337[264,329] 1.04us   |-----------------------------------------L0.337-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 869.81kb total:"
    - "L0                                                                                                                 "
    - "L0.?[264,295] 1.04us 414.83kb|------------------L0.?------------------|                                                "
    - "L0.?[296,329] 1.04us 454.98kb                                            |-------------------L0.?--------------------| "
    - "**** Simulation run 186, type=split(ReduceOverlap)(split_times=[590]). 1 Input Files, 1.02mb total:"
    - "L0, all files 1.02mb                                                                                               "
    - "L0.398[571,629] 1.04us   |-----------------------------------------L0.398-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 1.02mb total:"
    - "L0                                                                                                                 "
    - "L0.?[571,590] 1.04us 343.08kb|-----------L0.?------------|                                                             "
    - "L0.?[591,629] 1.04us 704.21kb                               |--------------------------L0.?--------------------------| "
    - "**** Simulation run 187, type=split(ReduceOverlap)(split_times=[295]). 1 Input Files, 1.14mb total:"
    - "L0, all files 1.14mb                                                                                               "
    - "L0.339[264,329] 1.04us   |-----------------------------------------L0.339-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 1.14mb total:"
    - "L0                                                                                                                 "
    - "L0.?[264,295] 1.04us 556.69kb|------------------L0.?------------------|                                                "
    - "L0.?[296,329] 1.04us 610.56kb                                            |-------------------L0.?--------------------| "
    - "**** Simulation run 188, type=split(ReduceOverlap)(split_times=[590]). 1 Input Files, 933.08kb total:"
    - "L0, all files 933.08kb                                                                                             "
    - "L0.400[571,648] 1.04us   |-----------------------------------------L0.400-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 933.08kb total:"
    - "L0                                                                                                                 "
    - "L0.?[571,590] 1.04us 230.24kb|--------L0.?--------|                                                                    "
    - "L0.?[591,648] 1.04us 702.84kb                       |------------------------------L0.?------------------------------| "
    - "**** Simulation run 189, type=split(ReduceOverlap)(split_times=[295]). 1 Input Files, 789.53kb total:"
    - "L0, all files 789.53kb                                                                                             "
    - "L0.341[264,329] 1.04us   |-----------------------------------------L0.341-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 789.53kb total:"
    - "L0                                                                                                                 "
    - "L0.?[264,295] 1.04us 376.55kb|------------------L0.?------------------|                                                "
    - "L0.?[296,329] 1.04us 412.99kb                                            |-------------------L0.?--------------------| "
    - "**** Simulation run 190, type=split(ReduceOverlap)(split_times=[590]). 1 Input Files, 846.1kb total:"
    - "L0, all files 846.1kb                                                                                              "
    - "L0.404[571,648] 1.05us   |-----------------------------------------L0.404-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 846.1kb total:"
    - "L0                                                                                                                 "
    - "L0.?[571,590] 1.05us 208.78kb|--------L0.?--------|                                                                    "
    - "L0.?[591,648] 1.05us 637.32kb                       |------------------------------L0.?------------------------------| "
    - "**** Simulation run 191, type=split(ReduceOverlap)(split_times=[295]). 1 Input Files, 715.93kb total:"
    - "L0, all files 715.93kb                                                                                             "
    - "L0.343[264,329] 1.05us   |-----------------------------------------L0.343-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 715.93kb total:"
    - "L0                                                                                                                 "
    - "L0.?[264,295] 1.05us 341.44kb|------------------L0.?------------------|                                                "
    - "L0.?[296,329] 1.05us 374.49kb                                            |-------------------L0.?--------------------| "
    - "**** Simulation run 192, type=split(ReduceOverlap)(split_times=[590]). 1 Input Files, 1mb total:"
    - "L0, all files 1mb                                                                                                  "
    - "L0.408[571,648] 1.05us   |-----------------------------------------L0.408-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 1mb total:"
    - "L0                                                                                                                 "
    - "L0.?[571,590] 1.05us 253.65kb|--------L0.?--------|                                                                    "
    - "L0.?[591,648] 1.05us 774.3kb                       |------------------------------L0.?------------------------------| "
    - "**** Simulation run 193, type=split(ReduceOverlap)(split_times=[295]). 1 Input Files, 869.81kb total:"
    - "L0, all files 869.81kb                                                                                             "
    - "L0.345[264,329] 1.05us   |-----------------------------------------L0.345-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 869.81kb total:"
    - "L0                                                                                                                 "
    - "L0.?[264,295] 1.05us 414.83kb|------------------L0.?------------------|                                                "
    - "L0.?[296,329] 1.05us 454.98kb                                            |-------------------L0.?--------------------| "
    - "**** Simulation run 194, type=split(ReduceOverlap)(split_times=[590]). 1 Input Files, 1.02mb total:"
    - "L0, all files 1.02mb                                                                                               "
    - "L0.412[571,629] 1.05us   |-----------------------------------------L0.412-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 1.02mb total:"
    - "L0                                                                                                                 "
    - "L0.?[571,590] 1.05us 343.08kb|-----------L0.?------------|                                                             "
    - "L0.?[591,629] 1.05us 704.21kb                               |--------------------------L0.?--------------------------| "
    - "**** Simulation run 195, type=split(ReduceOverlap)(split_times=[295]). 1 Input Files, 1.14mb total:"
    - "L0, all files 1.14mb                                                                                               "
    - "L0.347[264,329] 1.05us   |-----------------------------------------L0.347-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 1.14mb total:"
    - "L0                                                                                                                 "
    - "L0.?[264,295] 1.05us 556.69kb|------------------L0.?------------------|                                                "
    - "L0.?[296,329] 1.05us 610.56kb                                            |-------------------L0.?--------------------| "
    - "**** Simulation run 196, type=split(ReduceOverlap)(split_times=[590]). 1 Input Files, 933.08kb total:"
    - "L0, all files 933.08kb                                                                                             "
    - "L0.414[571,648] 1.05us   |-----------------------------------------L0.414-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 933.08kb total:"
    - "L0                                                                                                                 "
    - "L0.?[571,590] 1.05us 230.24kb|--------L0.?--------|                                                                    "
    - "L0.?[591,648] 1.05us 702.84kb                       |------------------------------L0.?------------------------------| "
    - "**** Simulation run 197, type=split(ReduceOverlap)(split_times=[295]). 1 Input Files, 789.53kb total:"
    - "L0, all files 789.53kb                                                                                             "
    - "L0.349[264,329] 1.05us   |-----------------------------------------L0.349-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 789.53kb total:"
    - "L0                                                                                                                 "
    - "L0.?[264,295] 1.05us 376.55kb|------------------L0.?------------------|                                                "
    - "L0.?[296,329] 1.05us 412.99kb                                            |-------------------L0.?--------------------| "
    - "**** Simulation run 198, type=split(ReduceOverlap)(split_times=[590]). 1 Input Files, 846.1kb total:"
    - "L0, all files 846.1kb                                                                                              "
    - "L0.418[571,648] 1.05us   |-----------------------------------------L0.418-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 846.1kb total:"
    - "L0                                                                                                                 "
    - "L0.?[571,590] 1.05us 208.78kb|--------L0.?--------|                                                                    "
    - "L0.?[591,648] 1.05us 637.32kb                       |------------------------------L0.?------------------------------| "
    - "**** Simulation run 199, type=split(ReduceOverlap)(split_times=[295]). 1 Input Files, 715.93kb total:"
    - "L0, all files 715.93kb                                                                                             "
    - "L0.351[264,329] 1.05us   |-----------------------------------------L0.351-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 715.93kb total:"
    - "L0                                                                                                                 "
    - "L0.?[264,295] 1.05us 341.44kb|------------------L0.?------------------|                                                "
    - "L0.?[296,329] 1.05us 374.49kb                                            |-------------------L0.?--------------------| "
    - "Committing partition 1:"
    - "  Soft Deleting 35 files: L0.319, L0.321, L0.323, L0.325, L0.327, L0.329, L0.331, L0.333, L0.335, L0.337, L0.339, L0.341, L0.343, L0.345, L0.347, L0.349, L0.351, L0.358, L0.362, L0.366, L0.370, L0.372, L0.376, L0.380, L0.384, L0.386, L0.390, L0.394, L0.398, L0.400, L0.404, L0.408, L0.412, L0.414, L0.418"
    - "  Creating 70 files"
    - "**** Simulation run 200, type=split(CompactAndSplitOutput(FoundSubsetLessThanMaxCompactSize))(split_times=[583, 870]). 13 Input Files, 240.67mb total:"
    - "L0                                                                                                                 "
    - "L0.360[877,932] 1.03us 675.04kb                                                                           |L0.360|       "
    - "L0.359[671,876] 1.03us 2.41mb                                                |---------L0.359---------|                "
    - "L0.357[357,570] 1.03us 2.49mb       |---------L0.357----------|                                                        "
    - "L0.425[591,648] 1.03us 702.84kb                                      |L0.425|                                            "
    - "L0.424[571,590] 1.03us 230.24kb                                   |L0.424|                                               "
    - "L0.210[659,670] 1.03us 130.17kb                                               |L0.210|                                   "
    - "L0.208[330,356] 1.03us 292.88kb    |L0.208|                                                                              "
    - "L0.277[967,986] 1.03us 218.33kb                                                                                       |L0.277|"
    - "L0.275[649,658] 1.03us 108.47kb                                              |L0.275|                                    "
    - "L1                                                                                                                 "
    - "L1.354[877,966] 1.03us 29.78mb                                                                           |-L1.354--|    "
    - "L1.423[591,876] 1.03us 97.34mb                                      |--------------L1.423---------------|               "
    - "L1.422[296,590] 1.03us 99.72mb|---------------L1.422---------------|                                                    "
    - "L1.269[967,986] 1.03us 6.62mb                                                                                       |L1.269|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 240.67mb total:"
    - "L1                                                                                                                 "
    - "L1.?[296,583] 1.03us 100.11mb|---------------L1.?----------------|                                                     "
    - "L1.?[584,870] 1.03us 99.76mb                                     |---------------L1.?----------------|                "
    - "L1.?[871,986] 1.03us 40.81mb                                                                           |----L1.?-----|"
    - "Committing partition 1:"
    - "  Soft Deleting 13 files: L0.208, L0.210, L1.269, L0.275, L0.277, L1.354, L0.357, L0.359, L0.360, L1.422, L1.423, L0.424, L0.425"
    - "  Creating 3 files"
    - "**** Simulation run 201, type=split(ReduceOverlap)(split_times=[583]). 1 Input Files, 208.78kb total:"
    - "L0, all files 208.78kb                                                                                             "
    - "L0.426[571,590] 1.03us   |-----------------------------------------L0.426-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 208.78kb total:"
    - "L0                                                                                                                 "
    - "L0.?[571,583] 1.03us 131.86kb|-------------------------L0.?-------------------------|                                  "
    - "L0.?[584,590] 1.03us 76.92kb                                                             |-----------L0.?-----------| "
    - "**** Simulation run 202, type=split(ReduceOverlap)(split_times=[870]). 1 Input Files, 2.19mb total:"
    - "L0, all files 2.19mb                                                                                               "
    - "L0.363[671,876] 1.03us   |-----------------------------------------L0.363-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 2.19mb total:"
    - "L0                                                                                                                 "
    - "L0.?[671,870] 1.03us 2.12mb|----------------------------------------L0.?-----------------------------------------|   "
    - "L0.?[871,876] 1.03us 65.5kb                                                                                       |L0.?|"
    - "**** Simulation run 203, type=split(ReduceOverlap)(split_times=[583]). 1 Input Files, 253.65kb total:"
    - "L0, all files 253.65kb                                                                                             "
    - "L0.430[571,590] 1.03us   |-----------------------------------------L0.430-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 253.65kb total:"
    - "L0                                                                                                                 "
    - "L0.?[571,583] 1.03us 160.2kb|-------------------------L0.?-------------------------|                                  "
    - "L0.?[584,590] 1.03us 93.45kb                                                             |-----------L0.?-----------| "
    - "**** Simulation run 204, type=split(ReduceOverlap)(split_times=[870]). 1 Input Files, 2.66mb total:"
    - "L0, all files 2.66mb                                                                                               "
    - "L0.367[671,876] 1.03us   |-----------------------------------------L0.367-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 2.66mb total:"
    - "L0                                                                                                                 "
    - "L0.?[671,870] 1.03us 2.58mb|----------------------------------------L0.?-----------------------------------------|   "
    - "L0.?[871,876] 1.03us 79.64kb                                                                                       |L0.?|"
    - "**** Simulation run 205, type=split(ReduceOverlap)(split_times=[583]). 1 Input Files, 343.08kb total:"
    - "L0, all files 343.08kb                                                                                             "
    - "L0.434[571,590] 1.03us   |-----------------------------------------L0.434-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 343.08kb total:"
    - "L0                                                                                                                 "
    - "L0.?[571,583] 1.03us 216.68kb|-------------------------L0.?-------------------------|                                  "
    - "L0.?[584,590] 1.03us 126.4kb                                                             |-----------L0.?-----------| "
    - "**** Simulation run 206, type=split(ReduceOverlap)(split_times=[583]). 1 Input Files, 230.24kb total:"
    - "L0, all files 230.24kb                                                                                             "
    - "L0.438[571,590] 1.04us   |-----------------------------------------L0.438-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 230.24kb total:"
    - "L0                                                                                                                 "
    - "L0.?[571,583] 1.04us 145.42kb|-------------------------L0.?-------------------------|                                  "
    - "L0.?[584,590] 1.04us 84.83kb                                                             |-----------L0.?-----------| "
    - "**** Simulation run 207, type=split(ReduceOverlap)(split_times=[870]). 1 Input Files, 2.41mb total:"
    - "L0, all files 2.41mb                                                                                               "
    - "L0.373[671,876] 1.04us   |-----------------------------------------L0.373-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 2.41mb total:"
    - "L0                                                                                                                 "
    - "L0.?[671,870] 1.04us 2.34mb|----------------------------------------L0.?-----------------------------------------|   "
    - "L0.?[871,876] 1.04us 72.33kb                                                                                       |L0.?|"
    - "**** Simulation run 208, type=split(ReduceOverlap)(split_times=[583]). 1 Input Files, 208.78kb total:"
    - "L0, all files 208.78kb                                                                                             "
    - "L0.442[571,590] 1.04us   |-----------------------------------------L0.442-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 208.78kb total:"
    - "L0                                                                                                                 "
    - "L0.?[571,583] 1.04us 131.86kb|-------------------------L0.?-------------------------|                                  "
    - "L0.?[584,590] 1.04us 76.92kb                                                             |-----------L0.?-----------| "
    - "**** Simulation run 209, type=split(ReduceOverlap)(split_times=[870]). 1 Input Files, 2.19mb total:"
    - "L0, all files 2.19mb                                                                                               "
    - "L0.377[671,876] 1.04us   |-----------------------------------------L0.377-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 2.19mb total:"
    - "L0                                                                                                                 "
    - "L0.?[671,870] 1.04us 2.12mb|----------------------------------------L0.?-----------------------------------------|   "
    - "L0.?[871,876] 1.04us 65.5kb                                                                                       |L0.?|"
    - "**** Simulation run 210, type=split(ReduceOverlap)(split_times=[583]). 1 Input Files, 253.65kb total:"
    - "L0, all files 253.65kb                                                                                             "
    - "L0.446[571,590] 1.04us   |-----------------------------------------L0.446-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 253.65kb total:"
    - "L0                                                                                                                 "
    - "L0.?[571,583] 1.04us 160.2kb|-------------------------L0.?-------------------------|                                  "
    - "L0.?[584,590] 1.04us 93.45kb                                                             |-----------L0.?-----------| "
    - "**** Simulation run 211, type=split(ReduceOverlap)(split_times=[870]). 1 Input Files, 2.66mb total:"
    - "L0, all files 2.66mb                                                                                               "
    - "L0.381[671,876] 1.04us   |-----------------------------------------L0.381-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 2.66mb total:"
    - "L0                                                                                                                 "
    - "L0.?[671,870] 1.04us 2.58mb|----------------------------------------L0.?-----------------------------------------|   "
    - "L0.?[871,876] 1.04us 79.64kb                                                                                       |L0.?|"
    - "**** Simulation run 212, type=split(ReduceOverlap)(split_times=[583]). 1 Input Files, 343.08kb total:"
    - "L0, all files 343.08kb                                                                                             "
    - "L0.450[571,590] 1.04us   |-----------------------------------------L0.450-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 343.08kb total:"
    - "L0                                                                                                                 "
    - "L0.?[571,583] 1.04us 216.68kb|-------------------------L0.?-------------------------|                                  "
    - "L0.?[584,590] 1.04us 126.4kb                                                             |-----------L0.?-----------| "
    - "**** Simulation run 213, type=split(ReduceOverlap)(split_times=[583]). 1 Input Files, 230.24kb total:"
    - "L0, all files 230.24kb                                                                                             "
    - "L0.454[571,590] 1.04us   |-----------------------------------------L0.454-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 230.24kb total:"
    - "L0                                                                                                                 "
    - "L0.?[571,583] 1.04us 145.42kb|-------------------------L0.?-------------------------|                                  "
    - "L0.?[584,590] 1.04us 84.83kb                                                             |-----------L0.?-----------| "
    - "**** Simulation run 214, type=split(ReduceOverlap)(split_times=[870]). 1 Input Files, 2.41mb total:"
    - "L0, all files 2.41mb                                                                                               "
    - "L0.387[671,876] 1.04us   |-----------------------------------------L0.387-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 2.41mb total:"
    - "L0                                                                                                                 "
    - "L0.?[671,870] 1.04us 2.34mb|----------------------------------------L0.?-----------------------------------------|   "
    - "L0.?[871,876] 1.04us 72.33kb                                                                                       |L0.?|"
    - "**** Simulation run 215, type=split(ReduceOverlap)(split_times=[583]). 1 Input Files, 208.78kb total:"
    - "L0, all files 208.78kb                                                                                             "
    - "L0.458[571,590] 1.04us   |-----------------------------------------L0.458-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 208.78kb total:"
    - "L0                                                                                                                 "
    - "L0.?[571,583] 1.04us 131.86kb|-------------------------L0.?-------------------------|                                  "
    - "L0.?[584,590] 1.04us 76.92kb                                                             |-----------L0.?-----------| "
    - "**** Simulation run 216, type=split(ReduceOverlap)(split_times=[870]). 1 Input Files, 2.19mb total:"
    - "L0, all files 2.19mb                                                                                               "
    - "L0.391[671,876] 1.04us   |-----------------------------------------L0.391-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 2.19mb total:"
    - "L0                                                                                                                 "
    - "L0.?[671,870] 1.04us 2.12mb|----------------------------------------L0.?-----------------------------------------|   "
    - "L0.?[871,876] 1.04us 65.5kb                                                                                       |L0.?|"
    - "**** Simulation run 217, type=split(ReduceOverlap)(split_times=[583]). 1 Input Files, 253.65kb total:"
    - "L0, all files 253.65kb                                                                                             "
    - "L0.462[571,590] 1.04us   |-----------------------------------------L0.462-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 253.65kb total:"
    - "L0                                                                                                                 "
    - "L0.?[571,583] 1.04us 160.2kb|-------------------------L0.?-------------------------|                                  "
    - "L0.?[584,590] 1.04us 93.45kb                                                             |-----------L0.?-----------| "
    - "**** Simulation run 218, type=split(ReduceOverlap)(split_times=[870]). 1 Input Files, 2.66mb total:"
    - "L0, all files 2.66mb                                                                                               "
    - "L0.395[671,876] 1.04us   |-----------------------------------------L0.395-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 2.66mb total:"
    - "L0                                                                                                                 "
    - "L0.?[671,870] 1.04us 2.58mb|----------------------------------------L0.?-----------------------------------------|   "
    - "L0.?[871,876] 1.04us 79.64kb                                                                                       |L0.?|"
    - "**** Simulation run 219, type=split(ReduceOverlap)(split_times=[583]). 1 Input Files, 343.08kb total:"
    - "L0, all files 343.08kb                                                                                             "
    - "L0.466[571,590] 1.04us   |-----------------------------------------L0.466-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 343.08kb total:"
    - "L0                                                                                                                 "
    - "L0.?[571,583] 1.04us 216.68kb|-------------------------L0.?-------------------------|                                  "
    - "L0.?[584,590] 1.04us 126.4kb                                                             |-----------L0.?-----------| "
    - "**** Simulation run 220, type=split(ReduceOverlap)(split_times=[583]). 1 Input Files, 230.24kb total:"
    - "L0, all files 230.24kb                                                                                             "
    - "L0.470[571,590] 1.04us   |-----------------------------------------L0.470-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 230.24kb total:"
    - "L0                                                                                                                 "
    - "L0.?[571,583] 1.04us 145.42kb|-------------------------L0.?-------------------------|                                  "
    - "L0.?[584,590] 1.04us 84.83kb                                                             |-----------L0.?-----------| "
    - "**** Simulation run 221, type=split(ReduceOverlap)(split_times=[870]). 1 Input Files, 2.41mb total:"
    - "L0, all files 2.41mb                                                                                               "
    - "L0.401[671,876] 1.04us   |-----------------------------------------L0.401-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 2.41mb total:"
    - "L0                                                                                                                 "
    - "L0.?[671,870] 1.04us 2.34mb|----------------------------------------L0.?-----------------------------------------|   "
    - "L0.?[871,876] 1.04us 72.33kb                                                                                       |L0.?|"
    - "**** Simulation run 222, type=split(ReduceOverlap)(split_times=[583]). 1 Input Files, 208.78kb total:"
    - "L0, all files 208.78kb                                                                                             "
    - "L0.474[571,590] 1.05us   |-----------------------------------------L0.474-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 208.78kb total:"
    - "L0                                                                                                                 "
    - "L0.?[571,583] 1.05us 131.86kb|-------------------------L0.?-------------------------|                                  "
    - "L0.?[584,590] 1.05us 76.92kb                                                             |-----------L0.?-----------| "
    - "**** Simulation run 223, type=split(ReduceOverlap)(split_times=[870]). 1 Input Files, 2.19mb total:"
    - "L0, all files 2.19mb                                                                                               "
    - "L0.405[671,876] 1.05us   |-----------------------------------------L0.405-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 2.19mb total:"
    - "L0                                                                                                                 "
    - "L0.?[671,870] 1.05us 2.12mb|----------------------------------------L0.?-----------------------------------------|   "
    - "L0.?[871,876] 1.05us 65.5kb                                                                                       |L0.?|"
    - "**** Simulation run 224, type=split(ReduceOverlap)(split_times=[583]). 1 Input Files, 253.65kb total:"
    - "L0, all files 253.65kb                                                                                             "
    - "L0.478[571,590] 1.05us   |-----------------------------------------L0.478-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 253.65kb total:"
    - "L0                                                                                                                 "
    - "L0.?[571,583] 1.05us 160.2kb|-------------------------L0.?-------------------------|                                  "
    - "L0.?[584,590] 1.05us 93.45kb                                                             |-----------L0.?-----------| "
    - "**** Simulation run 225, type=split(ReduceOverlap)(split_times=[870]). 1 Input Files, 2.66mb total:"
    - "L0, all files 2.66mb                                                                                               "
    - "L0.409[671,876] 1.05us   |-----------------------------------------L0.409-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 2.66mb total:"
    - "L0                                                                                                                 "
    - "L0.?[671,870] 1.05us 2.58mb|----------------------------------------L0.?-----------------------------------------|   "
    - "L0.?[871,876] 1.05us 79.64kb                                                                                       |L0.?|"
    - "**** Simulation run 226, type=split(ReduceOverlap)(split_times=[583]). 1 Input Files, 343.08kb total:"
    - "L0, all files 343.08kb                                                                                             "
    - "L0.482[571,590] 1.05us   |-----------------------------------------L0.482-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 343.08kb total:"
    - "L0                                                                                                                 "
    - "L0.?[571,583] 1.05us 216.68kb|-------------------------L0.?-------------------------|                                  "
    - "L0.?[584,590] 1.05us 126.4kb                                                             |-----------L0.?-----------| "
    - "**** Simulation run 227, type=split(ReduceOverlap)(split_times=[583]). 1 Input Files, 230.24kb total:"
    - "L0, all files 230.24kb                                                                                             "
    - "L0.486[571,590] 1.05us   |-----------------------------------------L0.486-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 230.24kb total:"
    - "L0                                                                                                                 "
    - "L0.?[571,583] 1.05us 145.42kb|-------------------------L0.?-------------------------|                                  "
    - "L0.?[584,590] 1.05us 84.83kb                                                             |-----------L0.?-----------| "
    - "**** Simulation run 228, type=split(ReduceOverlap)(split_times=[870]). 1 Input Files, 2.41mb total:"
    - "L0, all files 2.41mb                                                                                               "
    - "L0.415[671,876] 1.05us   |-----------------------------------------L0.415-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 2.41mb total:"
    - "L0                                                                                                                 "
    - "L0.?[671,870] 1.05us 2.34mb|----------------------------------------L0.?-----------------------------------------|   "
    - "L0.?[871,876] 1.05us 72.33kb                                                                                       |L0.?|"
    - "**** Simulation run 229, type=split(ReduceOverlap)(split_times=[583]). 1 Input Files, 208.78kb total:"
    - "L0, all files 208.78kb                                                                                             "
    - "L0.490[571,590] 1.05us   |-----------------------------------------L0.490-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 208.78kb total:"
    - "L0                                                                                                                 "
    - "L0.?[571,583] 1.05us 131.86kb|-------------------------L0.?-------------------------|                                  "
    - "L0.?[584,590] 1.05us 76.92kb                                                             |-----------L0.?-----------| "
    - "**** Simulation run 230, type=split(ReduceOverlap)(split_times=[870]). 1 Input Files, 2.19mb total:"
    - "L0, all files 2.19mb                                                                                               "
    - "L0.419[671,876] 1.05us   |-----------------------------------------L0.419-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 2.19mb total:"
    - "L0                                                                                                                 "
    - "L0.?[671,870] 1.05us 2.12mb|----------------------------------------L0.?-----------------------------------------|   "
    - "L0.?[871,876] 1.05us 65.5kb                                                                                       |L0.?|"
    - "Committing partition 1:"
    - "  Soft Deleting 30 files: L0.363, L0.367, L0.373, L0.377, L0.381, L0.387, L0.391, L0.395, L0.401, L0.405, L0.409, L0.415, L0.419, L0.426, L0.430, L0.434, L0.438, L0.442, L0.446, L0.450, L0.454, L0.458, L0.462, L0.466, L0.470, L0.474, L0.478, L0.482, L0.486, L0.490"
    - "  Creating 60 files"
    - "**** Simulation run 231, type=split(CompactAndSplitOutput(ManySmallFiles))(split_times=[604]). 200 Input Files, 168.1mb total:"
    - "L0                                                                                                                 "
    - "L0.318[42,263] 1.03us 2.34mb|------L0.318-------|                                                                     "
    - "L0.428[264,295] 1.03us 341.44kb                     |L0.428|                                                             "
    - "L0.429[296,329] 1.03us 374.49kb                        |L0.429|                                                          "
    - "L0.361[357,570] 1.03us 2.26mb                              |------L0.361------|                                        "
    - "L0.497[571,583] 1.03us 131.86kb                                                  |L0.497|                                "
    - "L0.498[584,590] 1.03us 76.92kb                                                   |L0.498|                               "
    - "L0.427[591,648] 1.03us 637.32kb                                                    |L0.427|                              "
    - "L0.499[671,870] 1.03us 2.12mb                                                           |-----L0.499-----|             "
    - "L0.500[871,876] 1.03us 65.5kb                                                                               |L0.500|   "
    - "L0.364[877,966] 1.03us 982.47kb                                                                               |L0.364|   "
    - "L0.320[173,263] 1.03us 1.16mb            |L0.320|                                                                      "
    - "L0.432[264,295] 1.03us 414.83kb                     |L0.432|                                                             "
    - "L0.433[296,329] 1.03us 454.98kb                        |L0.433|                                                          "
    - "L0.212[330,356] 1.03us 355.83kb                           |L0.212|                                                       "
    - "L0.365[357,570] 1.03us 2.74mb                              |------L0.365------|                                        "
    - "L0.501[571,583] 1.03us 160.2kb                                                  |L0.501|                                "
    - "L0.502[584,590] 1.03us 93.45kb                                                   |L0.502|                               "
    - "L0.431[591,648] 1.03us 774.3kb                                                    |L0.431|                              "
    - "L0.279[649,658] 1.03us 131.79kb                                                         |L0.279|                         "
    - "L0.214[659,670] 1.03us 158.15kb                                                          |L0.214|                        "
    - "L0.503[671,870] 1.03us 2.58mb                                                           |-----L0.503-----|             "
    - "L0.504[871,876] 1.03us 79.64kb                                                                               |L0.504|   "
    - "L0.368[877,950] 1.03us 982.23kb                                                                               |L0.368|   "
    - "L0.322[50,263] 1.03us 3.68mb|------L0.322------|                                                                      "
    - "L0.436[264,295] 1.03us 556.69kb                     |L0.436|                                                             "
    - "L0.437[296,329] 1.03us 610.56kb                        |L0.437|                                                          "
    - "L0.216[330,356] 1.03us 477.51kb                           |L0.216|                                                       "
    - "L0.369[357,570] 1.03us 3.69mb                              |------L0.369------|                                        "
    - "L0.505[571,583] 1.03us 216.68kb                                                  |L0.505|                                "
    - "L0.506[584,590] 1.03us 126.4kb                                                   |L0.506|                               "
    - "L0.435[591,629] 1.03us 704.21kb                                                    |L0.435|                              "
    - "L0.324[76,263] 1.04us 2.18mb   |----L0.324-----|                                                                      "
    - "L0.440[264,295] 1.04us 376.55kb                     |L0.440|                                                             "
    - "L0.441[296,329] 1.04us 412.99kb                        |L0.441|                                                          "
    - "L0.218[330,356] 1.04us 322.99kb                           |L0.218|                                                       "
    - "L0.371[357,570] 1.04us 2.49mb                              |------L0.371------|                                        "
    - "L0.507[571,583] 1.04us 145.42kb                                                  |L0.507|                                "
    - "L0.508[584,590] 1.04us 84.83kb                                                   |L0.508|                               "
    - "L0.439[591,648] 1.04us 702.84kb                                                    |L0.439|                              "
    - "L0.281[649,658] 1.04us 119.63kb                                                         |L0.281|                         "
    - "L0.220[659,670] 1.04us 143.55kb                                                          |L0.220|                        "
    - "L0.509[671,870] 1.04us 2.34mb                                                           |-----L0.509-----|             "
    - "L0.510[871,876] 1.04us 72.33kb                                                                               |L0.510|   "
    - "L0.374[877,932] 1.04us 675.04kb                                                                               |L0.374|   "
    - "L0.326[42,263] 1.04us 2.34mb|------L0.326-------|                                                                     "
    - "L0.444[264,295] 1.04us 341.44kb                     |L0.444|                                                             "
    - "L0.445[296,329] 1.04us 374.49kb                        |L0.445|                                                          "
    - "L0.222[330,356] 1.04us 292.88kb                           |L0.222|                                                       "
    - "L0.375[357,570] 1.04us 2.26mb                              |------L0.375------|                                        "
    - "L0.511[571,583] 1.04us 131.86kb                                                  |L0.511|                                "
    - "L0.512[584,590] 1.04us 76.92kb                                                   |L0.512|                               "
    - "L0.443[591,648] 1.04us 637.32kb                                                    |L0.443|                              "
    - "L0.283[649,658] 1.04us 108.47kb                                                         |L0.283|                         "
    - "L0.224[659,670] 1.04us 130.17kb                                                          |L0.224|                        "
    - "L0.513[671,870] 1.04us 2.12mb                                                           |-----L0.513-----|             "
    - "L0.514[871,876] 1.04us 65.5kb                                                                               |L0.514|   "
    - "L0.378[877,966] 1.04us 982.47kb                                                                               |L0.378|   "
    - "L0.285[967,986] 1.04us 218.33kb                                                                                        |L0.285|"
    - "L0.328[173,263] 1.04us 1.16mb            |L0.328|                                                                      "
    - "L0.448[264,295] 1.04us 414.83kb                     |L0.448|                                                             "
    - "L0.449[296,329] 1.04us 454.98kb                        |L0.449|                                                          "
    - "L0.226[330,356] 1.04us 355.83kb                           |L0.226|                                                       "
    - "L0.379[357,570] 1.04us 2.74mb                              |------L0.379------|                                        "
    - "L0.515[571,583] 1.04us 160.2kb                                                  |L0.515|                                "
    - "L0.516[584,590] 1.04us 93.45kb                                                   |L0.516|                               "
    - "L0.447[591,648] 1.04us 774.3kb                                                    |L0.447|                              "
    - "L0.287[649,658] 1.04us 131.79kb                                                         |L0.287|                         "
    - "L0.228[659,670] 1.04us 158.15kb                                                          |L0.228|                        "
    - "L0.517[671,870] 1.04us 2.58mb                                                           |-----L0.517-----|             "
    - "L0.518[871,876] 1.04us 79.64kb                                                                               |L0.518|   "
    - "L0.382[877,950] 1.04us 982.23kb                                                                               |L0.382|   "
    - "L0.330[50,263] 1.04us 3.68mb|------L0.330------|                                                                      "
    - "L0.452[264,295] 1.04us 556.69kb                     |L0.452|                                                             "
    - "L0.453[296,329] 1.04us 610.56kb                        |L0.453|                                                          "
    - "L0.230[330,356] 1.04us 477.51kb                           |L0.230|                                                       "
    - "L0.383[357,570] 1.04us 3.69mb                              |------L0.383------|                                        "
    - "L0.519[571,583] 1.04us 216.68kb                                                  |L0.519|                                "
    - "L0.520[584,590] 1.04us 126.4kb                                                   |L0.520|                               "
    - "L0.451[591,629] 1.04us 704.21kb                                                    |L0.451|                              "
    - "L0.332[76,263] 1.04us 2.18mb   |----L0.332-----|                                                                      "
    - "L0.456[264,295] 1.04us 376.55kb                     |L0.456|                                                             "
    - "L0.457[296,329] 1.04us 412.99kb                        |L0.457|                                                          "
    - "L0.232[330,356] 1.04us 322.99kb                           |L0.232|                                                       "
    - "L0.385[357,570] 1.04us 2.49mb                              |------L0.385------|                                        "
    - "L0.521[571,583] 1.04us 145.42kb                                                  |L0.521|                                "
    - "L0.522[584,590] 1.04us 84.83kb                                                   |L0.522|                               "
    - "L0.455[591,648] 1.04us 702.84kb                                                    |L0.455|                              "
    - "L0.289[649,658] 1.04us 119.63kb                                                         |L0.289|                         "
    - "L0.234[659,670] 1.04us 143.55kb                                                          |L0.234|                        "
    - "L0.523[671,870] 1.04us 2.34mb                                                           |-----L0.523-----|             "
    - "L0.524[871,876] 1.04us 72.33kb                                                                               |L0.524|   "
    - "L0.388[877,932] 1.04us 675.04kb                                                                               |L0.388|   "
    - "L0.334[42,263] 1.04us 2.34mb|------L0.334-------|                                                                     "
    - "L0.460[264,295] 1.04us 341.44kb                     |L0.460|                                                             "
    - "L0.461[296,329] 1.04us 374.49kb                        |L0.461|                                                          "
    - "L0.236[330,356] 1.04us 292.88kb                           |L0.236|                                                       "
    - "L0.389[357,570] 1.04us 2.26mb                              |------L0.389------|                                        "
    - "L0.525[571,583] 1.04us 131.86kb                                                  |L0.525|                                "
    - "L0.526[584,590] 1.04us 76.92kb                                                   |L0.526|                               "
    - "L0.459[591,648] 1.04us 637.32kb                                                    |L0.459|                              "
    - "L0.291[649,658] 1.04us 108.47kb                                                         |L0.291|                         "
    - "L0.238[659,670] 1.04us 130.17kb                                                          |L0.238|                        "
    - "L0.527[671,870] 1.04us 2.12mb                                                           |-----L0.527-----|             "
    - "L0.528[871,876] 1.04us 65.5kb                                                                               |L0.528|   "
    - "L0.392[877,966] 1.04us 982.47kb                                                                               |L0.392|   "
    - "L0.293[967,986] 1.04us 218.33kb                                                                                        |L0.293|"
    - "L0.336[173,263] 1.04us 1.16mb            |L0.336|                                                                      "
    - "L0.464[264,295] 1.04us 414.83kb                     |L0.464|                                                             "
    - "L0.465[296,329] 1.04us 454.98kb                        |L0.465|                                                          "
    - "L0.240[330,356] 1.04us 355.83kb                           |L0.240|                                                       "
    - "L0.393[357,570] 1.04us 2.74mb                              |------L0.393------|                                        "
    - "L0.529[571,583] 1.04us 160.2kb                                                  |L0.529|                                "
    - "L0.530[584,590] 1.04us 93.45kb                                                   |L0.530|                               "
    - "L0.463[591,648] 1.04us 774.3kb                                                    |L0.463|                              "
    - "L0.295[649,658] 1.04us 131.79kb                                                         |L0.295|                         "
    - "L0.242[659,670] 1.04us 158.15kb                                                          |L0.242|                        "
    - "L0.531[671,870] 1.04us 2.58mb                                                           |-----L0.531-----|             "
    - "L0.532[871,876] 1.04us 79.64kb                                                                               |L0.532|   "
    - "L0.396[877,950] 1.04us 982.23kb                                                                               |L0.396|   "
    - "L0.338[50,263] 1.04us 3.68mb|------L0.338------|                                                                      "
    - "L0.468[264,295] 1.04us 556.69kb                     |L0.468|                                                             "
    - "L0.469[296,329] 1.04us 610.56kb                        |L0.469|                                                          "
    - "L0.244[330,356] 1.04us 477.51kb                           |L0.244|                                                       "
    - "L0.397[357,570] 1.04us 3.69mb                              |------L0.397------|                                        "
    - "L0.533[571,583] 1.04us 216.68kb                                                  |L0.533|                                "
    - "L0.534[584,590] 1.04us 126.4kb                                                   |L0.534|                               "
    - "L0.467[591,629] 1.04us 704.21kb                                                    |L0.467|                              "
    - "L0.340[76,263] 1.04us 2.18mb   |----L0.340-----|                                                                      "
    - "L0.472[264,295] 1.04us 376.55kb                     |L0.472|                                                             "
    - "L0.473[296,329] 1.04us 412.99kb                        |L0.473|                                                          "
    - "L0.246[330,356] 1.04us 322.99kb                           |L0.246|                                                       "
    - "L0.399[357,570] 1.04us 2.49mb                              |------L0.399------|                                        "
    - "L0.535[571,583] 1.04us 145.42kb                                                  |L0.535|                                "
    - "L0.536[584,590] 1.04us 84.83kb                                                   |L0.536|                               "
    - "L0.471[591,648] 1.04us 702.84kb                                                    |L0.471|                              "
    - "L0.297[649,658] 1.04us 119.63kb                                                         |L0.297|                         "
    - "L0.248[659,670] 1.04us 143.55kb                                                          |L0.248|                        "
    - "L0.537[671,870] 1.04us 2.34mb                                                           |-----L0.537-----|             "
    - "L0.538[871,876] 1.04us 72.33kb                                                                               |L0.538|   "
    - "L0.402[877,932] 1.04us 675.04kb                                                                               |L0.402|   "
    - "L0.342[42,263] 1.05us 2.34mb|------L0.342-------|                                                                     "
    - "L0.476[264,295] 1.05us 341.44kb                     |L0.476|                                                             "
    - "L0.477[296,329] 1.05us 374.49kb                        |L0.477|                                                          "
    - "L0.250[330,356] 1.05us 292.88kb                           |L0.250|                                                       "
    - "L0.403[357,570] 1.05us 2.26mb                              |------L0.403------|                                        "
    - "L0.539[571,583] 1.05us 131.86kb                                                  |L0.539|                                "
    - "L0.540[584,590] 1.05us 76.92kb                                                   |L0.540|                               "
    - "L0.475[591,648] 1.05us 637.32kb                                                    |L0.475|                              "
    - "L0.299[649,658] 1.05us 108.47kb                                                         |L0.299|                         "
    - "L0.252[659,670] 1.05us 130.17kb                                                          |L0.252|                        "
    - "L0.541[671,870] 1.05us 2.12mb                                                           |-----L0.541-----|             "
    - "L0.542[871,876] 1.05us 65.5kb                                                                               |L0.542|   "
    - "L0.406[877,966] 1.05us 982.47kb                                                                               |L0.406|   "
    - "L0.301[967,986] 1.05us 218.33kb                                                                                        |L0.301|"
    - "L0.344[173,263] 1.05us 1.16mb            |L0.344|                                                                      "
    - "L0.480[264,295] 1.05us 414.83kb                     |L0.480|                                                             "
    - "L0.481[296,329] 1.05us 454.98kb                        |L0.481|                                                          "
    - "L0.254[330,356] 1.05us 355.83kb                           |L0.254|                                                       "
    - "L0.407[357,570] 1.05us 2.74mb                              |------L0.407------|                                        "
    - "L0.543[571,583] 1.05us 160.2kb                                                  |L0.543|                                "
    - "L0.544[584,590] 1.05us 93.45kb                                                   |L0.544|                               "
    - "L0.479[591,648] 1.05us 774.3kb                                                    |L0.479|                              "
    - "L0.303[649,658] 1.05us 131.79kb                                                         |L0.303|                         "
    - "L0.256[659,670] 1.05us 158.15kb                                                          |L0.256|                        "
    - "L0.545[671,870] 1.05us 2.58mb                                                           |-----L0.545-----|             "
    - "L0.546[871,876] 1.05us 79.64kb                                                                               |L0.546|   "
    - "L0.410[877,950] 1.05us 982.23kb                                                                               |L0.410|   "
    - "L0.346[50,263] 1.05us 3.68mb|------L0.346------|                                                                      "
    - "L0.484[264,295] 1.05us 556.69kb                     |L0.484|                                                             "
    - "L0.485[296,329] 1.05us 610.56kb                        |L0.485|                                                          "
    - "L0.258[330,356] 1.05us 477.51kb                           |L0.258|                                                       "
    - "L0.411[357,570] 1.05us 3.69mb                              |------L0.411------|                                        "
    - "L0.547[571,583] 1.05us 216.68kb                                                  |L0.547|                                "
    - "L0.548[584,590] 1.05us 126.4kb                                                   |L0.548|                               "
    - "L0.483[591,629] 1.05us 704.21kb                                                    |L0.483|                              "
    - "L0.348[76,263] 1.05us 2.18mb   |----L0.348-----|                                                                      "
    - "L0.488[264,295] 1.05us 376.55kb                     |L0.488|                                                             "
    - "L0.489[296,329] 1.05us 412.99kb                        |L0.489|                                                          "
    - "L0.260[330,356] 1.05us 322.99kb                           |L0.260|                                                       "
    - "L0.413[357,570] 1.05us 2.49mb                              |------L0.413------|                                        "
    - "L0.549[571,583] 1.05us 145.42kb                                                  |L0.549|                                "
    - "L0.550[584,590] 1.05us 84.83kb                                                   |L0.550|                               "
    - "L0.487[591,648] 1.05us 702.84kb                                                    |L0.487|                              "
    - "L0.305[649,658] 1.05us 119.63kb                                                         |L0.305|                         "
    - "L0.262[659,670] 1.05us 143.55kb                                                          |L0.262|                        "
    - "L0.551[671,870] 1.05us 2.34mb                                                           |-----L0.551-----|             "
    - "L0.552[871,876] 1.05us 72.33kb                                                                               |L0.552|   "
    - "L0.416[877,932] 1.05us 675.04kb                                                                               |L0.416|   "
    - "L0.350[42,263] 1.05us 2.34mb|------L0.350-------|                                                                     "
    - "L0.492[264,295] 1.05us 341.44kb                     |L0.492|                                                             "
    - "L0.493[296,329] 1.05us 374.49kb                        |L0.493|                                                          "
    - "L0.264[330,356] 1.05us 292.88kb                           |L0.264|                                                       "
    - "L0.417[357,570] 1.05us 2.26mb                              |------L0.417------|                                        "
    - "L0.553[571,583] 1.05us 131.86kb                                                  |L0.553|                                "
    - "L0.554[584,590] 1.05us 76.92kb                                                   |L0.554|                               "
    - "L0.491[591,648] 1.05us 637.32kb                                                    |L0.491|                              "
    - "L0.307[649,658] 1.05us 108.47kb                                                         |L0.307|                         "
    - "L0.266[659,670] 1.05us 130.17kb                                                          |L0.266|                        "
    - "L0.555[671,870] 1.05us 2.12mb                                                           |-----L0.555-----|             "
    - "L0.556[871,876] 1.05us 65.5kb                                                                               |L0.556|   "
    - "**** 2 Output Files (parquet_file_id not yet assigned), 168.1mb total:"
    - "L0                                                                                                                 "
    - "L0.?[42,604] 1.05us 100.07mb|-----------------------L0.?------------------------|                                     "
    - "L0.?[605,986] 1.05us 68.02mb                                                     |---------------L0.?---------------| "
    - "Committing partition 1:"
    - "  Soft Deleting 200 files: L0.212, L0.214, L0.216, L0.218, L0.220, L0.222, L0.224, L0.226, L0.228, L0.230, L0.232, L0.234, L0.236, L0.238, L0.240, L0.242, L0.244, L0.246, L0.248, L0.250, L0.252, L0.254, L0.256, L0.258, L0.260, L0.262, L0.264, L0.266, L0.279, L0.281, L0.283, L0.285, L0.287, L0.289, L0.291, L0.293, L0.295, L0.297, L0.299, L0.301, L0.303, L0.305, L0.307, L0.318, L0.320, L0.322, L0.324, L0.326, L0.328, L0.330, L0.332, L0.334, L0.336, L0.338, L0.340, L0.342, L0.344, L0.346, L0.348, L0.350, L0.361, L0.364, L0.365, L0.368, L0.369, L0.371, L0.374, L0.375, L0.378, L0.379, L0.382, L0.383, L0.385, L0.388, L0.389, L0.392, L0.393, L0.396, L0.397, L0.399, L0.402, L0.403, L0.406, L0.407, L0.410, L0.411, L0.413, L0.416, L0.417, L0.427, L0.428, L0.429, L0.431, L0.432, L0.433, L0.435, L0.436, L0.437, L0.439, L0.440, L0.441, L0.443, L0.444, L0.445, L0.447, L0.448, L0.449, L0.451, L0.452, L0.453, L0.455, L0.456, L0.457, L0.459, L0.460, L0.461, L0.463, L0.464, L0.465, L0.467, L0.468, L0.469, L0.471, L0.472, L0.473, L0.475, L0.476, L0.477, L0.479, L0.480, L0.481, L0.483, L0.484, L0.485, L0.487, L0.488, L0.489, L0.491, L0.492, L0.493, L0.497, L0.498, L0.499, L0.500, L0.501, L0.502, L0.503, L0.504, L0.505, L0.506, L0.507, L0.508, L0.509, L0.510, L0.511, L0.512, L0.513, L0.514, L0.515, L0.516, L0.517, L0.518, L0.519, L0.520, L0.521, L0.522, L0.523, L0.524, L0.525, L0.526, L0.527, L0.528, L0.529, L0.530, L0.531, L0.532, L0.533, L0.534, L0.535, L0.536, L0.537, L0.538, L0.539, L0.540, L0.541, L0.542, L0.543, L0.544, L0.545, L0.546, L0.547, L0.548, L0.549, L0.550, L0.551, L0.552, L0.553, L0.554, L0.555, L0.556"
    - "  Creating 2 files"
    - "**** Simulation run 232, type=compact(ManySmallFiles). 2 Input Files, 1.17mb total:"
    - "L0                                                                                                                 "
    - "L0.420[877,966] 1.05us 982.47kb|--------------------------------L0.420---------------------------------|                 "
    - "L0.309[967,986] 1.05us 218.33kb                                                                          |---L0.309----| "
    - "**** 1 Output Files (parquet_file_id not yet assigned), 1.17mb total:"
    - "L0, all files 1.17mb                                                                                               "
    - "L0.?[877,986] 1.05us     |------------------------------------------L0.?------------------------------------------|"
    - "Committing partition 1:"
    - "  Soft Deleting 2 files: L0.309, L0.420"
    - "  Creating 1 files"
    - "**** Simulation run 233, type=split(HighL0OverlapSingleFile)(split_times=[290]). 1 Input Files, 100.06mb total:"
    - "L1, all files 100.06mb                                                                                             "
    - "L1.421[0,295] 1.03us     |-----------------------------------------L1.421-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 100.06mb total:"
    - "L1                                                                                                                 "
    - "L1.?[0,290] 1.03us 98.36mb|-----------------------------------------L1.?-----------------------------------------|  "
    - "L1.?[291,295] 1.03us 1.7mb                                                                                        |L1.?|"
    - "**** Simulation run 234, type=split(HighL0OverlapSingleFile)(split_times=[580]). 1 Input Files, 100.11mb total:"
    - "L1, all files 100.11mb                                                                                             "
    - "L1.494[296,583] 1.03us   |-----------------------------------------L1.494-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 100.11mb total:"
    - "L1                                                                                                                 "
    - "L1.?[296,580] 1.03us 99.06mb|-----------------------------------------L1.?------------------------------------------| "
    - "L1.?[581,583] 1.03us 1.05mb                                                                                         |L1.?|"
    - "**** Simulation run 235, type=split(HighL0OverlapSingleFile)(split_times=[290, 580]). 1 Input Files, 100.07mb total:"
    - "L0, all files 100.07mb                                                                                             "
    - "L0.557[42,604] 1.05us    |-----------------------------------------L0.557-----------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 100.07mb total:"
    - "L0                                                                                                                 "
    - "L0.?[42,290] 1.05us 44.16mb|----------------L0.?-----------------|                                                   "
    - "L0.?[291,580] 1.05us 51.46mb                                       |--------------------L0.?--------------------|     "
    - "L0.?[581,604] 1.05us 4.45mb                                                                                      |L0.?|"
    - "Committing partition 1:"
    - "  Soft Deleting 3 files: L1.421, L1.494, L0.557"
    - "  Creating 7 files"
    - "**** Simulation run 236, type=split(ReduceOverlap)(split_times=[870]). 1 Input Files, 68.02mb total:"
    - "L0, all files 68.02mb                                                                                              "
    - "L0.558[605,986] 1.05us   |-----------------------------------------L0.558-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 68.02mb total:"
    - "L0                                                                                                                 "
    - "L0.?[605,870] 1.05us 47.31mb|----------------------------L0.?----------------------------|                            "
    - "L0.?[871,986] 1.05us 20.71mb                                                              |----------L0.?-----------| "
    - "**** Simulation run 237, type=split(ReduceOverlap)(split_times=[583]). 1 Input Files, 4.45mb total:"
    - "L0, all files 4.45mb                                                                                               "
    - "L0.566[581,604] 1.05us   |-----------------------------------------L0.566-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 4.45mb total:"
    - "L0                                                                                                                 "
    - "L0.?[581,583] 1.05us 396.39kb|L0.?-|                                                                                   "
    - "L0.?[584,604] 1.05us 4.06mb           |------------------------------------L0.?------------------------------------| "
    - "**** Simulation run 238, type=split(ReduceOverlap)(split_times=[295]). 1 Input Files, 51.46mb total:"
    - "L0, all files 51.46mb                                                                                              "
    - "L0.565[291,580] 1.05us   |-----------------------------------------L0.565-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 51.46mb total:"
    - "L0                                                                                                                 "
    - "L0.?[291,295] 1.05us 729.36kb|L0.?|                                                                                    "
    - "L0.?[296,580] 1.05us 50.75mb |-----------------------------------------L0.?-----------------------------------------| "
    - "Committing partition 1:"
    - "  Soft Deleting 3 files: L0.558, L0.565, L0.566"
    - "  Creating 6 files"
    - "**** Simulation run 239, type=split(CompactAndSplitOutput(FoundSubsetLessThanMaxCompactSize))(split_times=[204]). 2 Input Files, 142.52mb total:"
    - "L0                                                                                                                 "
    - "L0.564[42,290] 1.05us 44.16mb             |----------------------------------L0.564----------------------------------| "
    - "L1                                                                                                                 "
    - "L1.560[0,290] 1.03us 98.36mb|-----------------------------------------L1.560-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 142.52mb total:"
    - "L1                                                                                                                 "
    - "L1.?[0,204] 1.05us 100.26mb|----------------------------L1.?-----------------------------|                           "
    - "L1.?[205,290] 1.05us 42.27mb                                                               |----------L1.?----------| "
    - "Committing partition 1:"
    - "  Soft Deleting 2 files: L1.560, L0.564"
    - "  Creating 2 files"
    - "**** Simulation run 240, type=split(CompactAndSplitOutput(FoundSubsetLessThanMaxCompactSize))(split_times=[771, 961]). 7 Input Files, 214.09mb total:"
    - "L0                                                                                                                 "
    - "L0.567[605,870] 1.05us 47.31mb     |-------------------------L0.567-------------------------|                           "
    - "L0.568[871,986] 1.05us 20.71mb                                                                |--------L0.568---------| "
    - "L0.569[581,583] 1.05us 396.39kb|L0.569|                                                                                  "
    - "L0.570[584,604] 1.05us 4.06mb|L0.570|                                                                                  "
    - "L1                                                                                                                 "
    - "L1.495[584,870] 1.03us 99.76mb|---------------------------L1.495----------------------------|                           "
    - "L1.496[871,986] 1.03us 40.81mb                                                                |--------L1.496---------| "
    - "L1.563[581,583] 1.03us 1.05mb|L1.563|                                                                                  "
    - "**** 3 Output Files (parquet_file_id not yet assigned), 214.09mb total:"
    - "L1                                                                                                                 "
    - "L1.?[581,771] 1.05us 100.44mb|------------------L1.?------------------|                                                "
    - "L1.?[772,961] 1.05us 99.91mb                                          |------------------L1.?------------------|      "
    - "L1.?[962,986] 1.05us 13.74mb                                                                                    |L1.?|"
    - "Committing partition 1:"
    - "  Soft Deleting 7 files: L1.495, L1.496, L1.563, L0.567, L0.568, L0.569, L0.570"
    - "  Creating 3 files"
    - "**** Simulation run 241, type=split(ReduceOverlap)(split_times=[961]). 1 Input Files, 1.17mb total:"
    - "L0, all files 1.17mb                                                                                               "
    - "L0.559[877,986] 1.05us   |-----------------------------------------L0.559-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 1.17mb total:"
    - "L0                                                                                                                 "
    - "L0.?[877,961] 1.05us 925.38kb|-------------------------------L0.?--------------------------------|                     "
    - "L0.?[962,986] 1.05us 275.41kb                                                                      |------L0.?-------| "
    - "Committing partition 1:"
    - "  Soft Deleting 1 files: L0.559"
    - "  Creating 2 files"
    - "**** Simulation run 242, type=split(CompactAndSplitOutput(FoundSubsetLessThanMaxCompactSize))(split_times=[481]). 4 Input Files, 152.22mb total:"
    - "L0                                                                                                                 "
    - "L0.571[291,295] 1.05us 729.36kb|L0.571|                                                                                  "
    - "L0.572[296,580] 1.05us 50.75mb |----------------------------------------L0.572----------------------------------------| "
    - "L1                                                                                                                 "
    - "L1.561[291,295] 1.03us 1.7mb|L1.561|                                                                                  "
    - "L1.562[296,580] 1.03us 99.06mb |----------------------------------------L1.562----------------------------------------| "
    - "**** 2 Output Files (parquet_file_id not yet assigned), 152.22mb total:"
    - "L1                                                                                                                 "
    - "L1.?[291,481] 1.05us 100.07mb|--------------------------L1.?---------------------------|                               "
    - "L1.?[482,580] 1.05us 52.14mb                                                           |------------L1.?------------| "
    - "Committing partition 1:"
    - "  Soft Deleting 4 files: L1.561, L1.562, L0.571, L0.572"
    - "  Creating 2 files"
    - "**** Simulation run 243, type=split(CompactAndSplitOutput(TotalSizeLessThanMaxCompactSize))(split_times=[943]). 4 Input Files, 114.82mb total:"
    - "L0                                                                                                                 "
    - "L0.578[877,961] 1.05us 925.38kb                                            |-------------L0.578--------------|           "
    - "L0.579[962,986] 1.05us 275.41kb                                                                               |-L0.579-| "
    - "L1                                                                                                                 "
    - "L1.576[772,961] 1.05us 99.91mb|-----------------------------------L1.576------------------------------------|           "
    - "L1.577[962,986] 1.05us 13.74mb                                                                               |-L1.577-| "
    - "**** 2 Output Files (parquet_file_id not yet assigned), 114.82mb total:"
    - "L1                                                                                                                 "
    - "L1.?[772,943] 1.05us 91.75mb|--------------------------------L1.?---------------------------------|                   "
    - "L1.?[944,986] 1.05us 23.07mb                                                                        |-----L1.?------| "
    - "Committing partition 1:"
    - "  Soft Deleting 4 files: L1.576, L1.577, L0.578, L0.579"
    - "  Creating 2 files"
    - "**** Simulation run 244, type=split(CompactAndSplitOutput(FoundSubsetLessThanMaxCompactSize))(split_times=[397, 589]). 4 Input Files, 294.92mb total:"
    - "L1                                                                                                                 "
    - "L1.574[205,290] 1.05us 42.27mb|--L1.574---|                                                                             "
    - "L1.580[291,481] 1.05us 100.07mb             |-----------L1.580-----------|                                               "
    - "L1.581[482,580] 1.05us 52.14mb                                            |---L1.581----|                               "
    - "L1.575[581,771] 1.05us 100.44mb                                                           |-----------L1.575-----------| "
    - "**** 3 Output Files (parquet_file_id not yet assigned), 294.92mb total:"
    - "L2                                                                                                                 "
    - "L2.?[205,397] 1.05us 100.04mb|------------L2.?------------|                                                            "
    - "L2.?[398,589] 1.05us 99.52mb                              |------------L2.?------------|                              "
    - "L2.?[590,771] 1.05us 95.35mb                                                             |-----------L2.?-----------| "
    - "Committing partition 1:"
    - "  Soft Deleting 4 files: L1.574, L1.575, L1.580, L1.581"
    - "  Upgrading 1 files level to CompactionLevel::L2: L1.573"
    - "  Creating 3 files"
    - "**** Simulation run 245, type=split(CompactAndSplitOutput(TotalSizeLessThanMaxCompactSize))(split_times=[943]). 2 Input Files, 114.82mb total:"
    - "L1                                                                                                                 "
    - "L1.583[944,986] 1.05us 23.07mb                                                                        |----L1.583-----| "
    - "L1.582[772,943] 1.05us 91.75mb|-------------------------------L1.582--------------------------------|                   "
    - "**** 2 Output Files (parquet_file_id not yet assigned), 114.82mb total:"
    - "L2                                                                                                                 "
    - "L2.?[772,943] 1.05us 91.75mb|--------------------------------L2.?---------------------------------|                   "
    - "L2.?[944,986] 1.05us 23.07mb                                                                        |-----L2.?------| "
    - "Committing partition 1:"
    - "  Soft Deleting 2 files: L1.582, L1.583"
    - "  Creating 2 files"
    - "**** Final Output Files (3.86gb written)"
    - "L2                                                                                                                 "
    - "L2.573[0,204] 1.05us 100.26mb|-----L2.573-----|                                                                        "
    - "L2.584[205,397] 1.05us 100.04mb                  |----L2.584-----|                                                       "
    - "L2.585[398,589] 1.05us 99.52mb                                    |----L2.585-----|                                     "
    - "L2.586[590,771] 1.05us 95.35mb                                                     |----L2.586----|                     "
    - "L2.587[772,943] 1.05us 91.75mb                                                                      |---L2.587----|     "
    - "L2.588[944,986] 1.05us 23.07mb                                                                                      |L2.588|"
    "###
    );
}

// This case simulates a backfill scenario with existing data prior to the start of backfill.
//   - we have L2s covering the whole time range of yesterday
//   - the customer starts backfilling more of yesterday's data, writing at random times spread across the day.
// The result:
//   - We start with compacted L2s covering the day, then get many L0s that each cover much of the day.
#[tokio::test]
async fn random_backfill_over_l2s() {
    test_helpers::maybe_start_logging();

    let setup = layout_setup_builder()
        .await
        // compact at most 10 L0 files per plan
        .with_max_num_files_per_plan(10)
        .with_max_desired_file_size_bytes(MAX_DESIRED_FILE_SIZE)
        .with_partition_timeout(Duration::from_secs(10))
        .with_max_num_files_per_plan(200)
        .build()
        .await;

    let day = 1000;
    let num_l2_files = 10;
    let l2_time = day / num_l2_files;
    let num_tiny_l0_files = 50;
    let l2_size = MAX_DESIRED_FILE_SIZE;
    let l0_size = MAX_DESIRED_FILE_SIZE / 10;

    for i in 0..num_l2_files {
        setup
            .partition
            .create_parquet_file(
                parquet_builder()
                    .with_min_time(i * l2_time)
                    .with_max_time((i + 1) * l2_time - 1)
                    .with_compaction_level(CompactionLevel::Final)
                    .with_file_size_bytes(l2_size)
                    .with_max_l0_created_at(Time::from_timestamp_nanos((i + 1) * l2_time - 1)), // These files are created sequentially "yesterday" with "yesterday's" data
            )
            .await;
    }

    // Assume the "day" is 1000 units of time, make the L0s span most of the day, with a little variability.
    for i in 0..num_tiny_l0_files {
        let i = i as i64;

        let mut start_time = 50;
        let mut end_time = 950;
        match i % 4 {
            0 => {
                start_time += 26;
                end_time -= 18;
            }
            1 => {
                start_time -= 8;
                end_time += 36;
            }
            2 => {
                start_time += 123;
            }
            3 => {
                end_time -= 321;
            }
            _ => {}
        }

        setup
            .partition
            .create_parquet_file(
                parquet_builder()
                    .with_min_time(start_time)
                    .with_max_time(end_time)
                    .with_compaction_level(CompactionLevel::Initial)
                    .with_file_size_bytes(l0_size)
                    .with_max_l0_created_at(Time::from_timestamp_nanos(i + 1000)), // These files are created sequentially "today" with "yesterday's" data
            )
            .await;
    }

    insta::assert_yaml_snapshot!(
        run_layout_scenario(&setup).await,
        @r###"
    ---
    - "**** Input Files "
    - "L0                                                                                                                 "
    - "L0.11[76,932] 1us 10mb         |-----------------------------------L0.11-----------------------------------|       "
    - "L0.12[42,986] 1us 10mb      |---------------------------------------L0.12---------------------------------------|  "
    - "L0.13[173,950] 1us 10mb                 |-------------------------------L0.13--------------------------------|     "
    - "L0.14[50,629] 1us 10mb       |----------------------L0.14-----------------------|                                  "
    - "L0.15[76,932] 1us 10mb         |-----------------------------------L0.15-----------------------------------|       "
    - "L0.16[42,986] 1us 10mb      |---------------------------------------L0.16---------------------------------------|  "
    - "L0.17[173,950] 1.01us 10mb               |-------------------------------L0.17--------------------------------|     "
    - "L0.18[50,629] 1.01us 10mb    |----------------------L0.18-----------------------|                                  "
    - "L0.19[76,932] 1.01us 10mb      |-----------------------------------L0.19-----------------------------------|       "
    - "L0.20[42,986] 1.01us 10mb   |---------------------------------------L0.20---------------------------------------|  "
    - "L0.21[173,950] 1.01us 10mb               |-------------------------------L0.21--------------------------------|     "
    - "L0.22[50,629] 1.01us 10mb    |----------------------L0.22-----------------------|                                  "
    - "L0.23[76,932] 1.01us 10mb      |-----------------------------------L0.23-----------------------------------|       "
    - "L0.24[42,986] 1.01us 10mb   |---------------------------------------L0.24---------------------------------------|  "
    - "L0.25[173,950] 1.01us 10mb               |-------------------------------L0.25--------------------------------|     "
    - "L0.26[50,629] 1.01us 10mb    |----------------------L0.26-----------------------|                                  "
    - "L0.27[76,932] 1.02us 10mb      |-----------------------------------L0.27-----------------------------------|       "
    - "L0.28[42,986] 1.02us 10mb   |---------------------------------------L0.28---------------------------------------|  "
    - "L0.29[173,950] 1.02us 10mb               |-------------------------------L0.29--------------------------------|     "
    - "L0.30[50,629] 1.02us 10mb    |----------------------L0.30-----------------------|                                  "
    - "L0.31[76,932] 1.02us 10mb      |-----------------------------------L0.31-----------------------------------|       "
    - "L0.32[42,986] 1.02us 10mb   |---------------------------------------L0.32---------------------------------------|  "
    - "L0.33[173,950] 1.02us 10mb               |-------------------------------L0.33--------------------------------|     "
    - "L0.34[50,629] 1.02us 10mb    |----------------------L0.34-----------------------|                                  "
    - "L0.35[76,932] 1.02us 10mb      |-----------------------------------L0.35-----------------------------------|       "
    - "L0.36[42,986] 1.02us 10mb   |---------------------------------------L0.36---------------------------------------|  "
    - "L0.37[173,950] 1.03us 10mb               |-------------------------------L0.37--------------------------------|     "
    - "L0.38[50,629] 1.03us 10mb    |----------------------L0.38-----------------------|                                  "
    - "L0.39[76,932] 1.03us 10mb      |-----------------------------------L0.39-----------------------------------|       "
    - "L0.40[42,986] 1.03us 10mb   |---------------------------------------L0.40---------------------------------------|  "
    - "L0.41[173,950] 1.03us 10mb               |-------------------------------L0.41--------------------------------|     "
    - "L0.42[50,629] 1.03us 10mb    |----------------------L0.42-----------------------|                                  "
    - "L0.43[76,932] 1.03us 10mb      |-----------------------------------L0.43-----------------------------------|       "
    - "L0.44[42,986] 1.03us 10mb   |---------------------------------------L0.44---------------------------------------|  "
    - "L0.45[173,950] 1.03us 10mb               |-------------------------------L0.45--------------------------------|     "
    - "L0.46[50,629] 1.03us 10mb    |----------------------L0.46-----------------------|                                  "
    - "L0.47[76,932] 1.04us 10mb      |-----------------------------------L0.47-----------------------------------|       "
    - "L0.48[42,986] 1.04us 10mb   |---------------------------------------L0.48---------------------------------------|  "
    - "L0.49[173,950] 1.04us 10mb               |-------------------------------L0.49--------------------------------|     "
    - "L0.50[50,629] 1.04us 10mb    |----------------------L0.50-----------------------|                                  "
    - "L0.51[76,932] 1.04us 10mb      |-----------------------------------L0.51-----------------------------------|       "
    - "L0.52[42,986] 1.04us 10mb   |---------------------------------------L0.52---------------------------------------|  "
    - "L0.53[173,950] 1.04us 10mb               |-------------------------------L0.53--------------------------------|     "
    - "L0.54[50,629] 1.04us 10mb    |----------------------L0.54-----------------------|                                  "
    - "L0.55[76,932] 1.04us 10mb      |-----------------------------------L0.55-----------------------------------|       "
    - "L0.56[42,986] 1.05us 10mb   |---------------------------------------L0.56---------------------------------------|  "
    - "L0.57[173,950] 1.05us 10mb               |-------------------------------L0.57--------------------------------|     "
    - "L0.58[50,629] 1.05us 10mb    |----------------------L0.58-----------------------|                                  "
    - "L0.59[76,932] 1.05us 10mb      |-----------------------------------L0.59-----------------------------------|       "
    - "L0.60[42,986] 1.05us 10mb   |---------------------------------------L0.60---------------------------------------|  "
    - "L2                                                                                                                 "
    - "L2.1[0,99] 99ns 100mb    |-L2.1-|                                                                                  "
    - "L2.2[100,199] 199ns 100mb         |-L2.2-|                                                                         "
    - "L2.3[200,299] 299ns 100mb                  |-L2.3-|                                                                "
    - "L2.4[300,399] 399ns 100mb                           |-L2.4-|                                                       "
    - "L2.5[400,499] 499ns 100mb                                    |-L2.5-|                                              "
    - "L2.6[500,599] 599ns 100mb                                             |-L2.6-|                                     "
    - "L2.7[600,699] 699ns 100mb                                                      |-L2.7-|                            "
    - "L2.8[700,799] 799ns 100mb                                                               |-L2.8-|                   "
    - "L2.9[800,899] 899ns 100mb                                                                        |-L2.9-|          "
    - "L2.10[900,999] 999ns 100mb                                                                                 |L2.10-| "
    - "**** Simulation run 0, type=split(HighL0OverlapSingleFile)(split_times=[356, 670]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.11[76,932] 1us        |-----------------------------------------L0.11------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[76,356] 1us 3.27mb  |-----------L0.?------------|                                                             "
    - "L0.?[357,670] 1us 3.66mb                              |-------------L0.?-------------|                             "
    - "L0.?[671,932] 1us 3.07mb                                                               |----------L0.?-----------| "
    - "**** Simulation run 1, type=split(HighL0OverlapSingleFile)(split_times=[356, 670]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.12[42,986] 1us        |-----------------------------------------L0.12------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[42,356] 1us 3.33mb  |-----------L0.?------------|                                                             "
    - "L0.?[357,670] 1us 3.32mb                               |-----------L0.?------------|                               "
    - "L0.?[671,986] 1us 3.36mb                                                            |------------L0.?------------| "
    - "**** Simulation run 2, type=split(HighL0OverlapSingleFile)(split_times=[356, 670]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.13[173,950] 1us       |-----------------------------------------L0.13------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[173,356] 1us 2.36mb |-------L0.?--------|                                                                     "
    - "L0.?[357,670] 1us 4.03mb                      |---------------L0.?---------------|                                 "
    - "L0.?[671,950] 1us 3.62mb                                                          |-------------L0.?-------------| "
    - "**** Simulation run 3, type=split(HighL0OverlapSingleFile)(split_times=[356]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.14[50,629] 1us        |-----------------------------------------L0.14------------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[50,356] 1us 5.28mb  |--------------------L0.?---------------------|                                           "
    - "L0.?[357,629] 1us 4.72mb                                                |------------------L0.?------------------| "
    - "**** Simulation run 4, type=split(HighL0OverlapSingleFile)(split_times=[356, 670]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.15[76,932] 1us        |-----------------------------------------L0.15------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[76,356] 1us 3.27mb  |-----------L0.?------------|                                                             "
    - "L0.?[357,670] 1us 3.66mb                              |-------------L0.?-------------|                             "
    - "L0.?[671,932] 1us 3.07mb                                                               |----------L0.?-----------| "
    - "**** Simulation run 5, type=split(HighL0OverlapSingleFile)(split_times=[356, 670]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.16[42,986] 1us        |-----------------------------------------L0.16------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[42,356] 1us 3.33mb  |-----------L0.?------------|                                                             "
    - "L0.?[357,670] 1us 3.32mb                               |-----------L0.?------------|                               "
    - "L0.?[671,986] 1us 3.36mb                                                            |------------L0.?------------| "
    - "**** Simulation run 6, type=split(HighL0OverlapSingleFile)(split_times=[356, 670]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.17[173,950] 1.01us    |-----------------------------------------L0.17------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[173,356] 1.01us 2.36mb|-------L0.?--------|                                                                     "
    - "L0.?[357,670] 1.01us 4.03mb                     |---------------L0.?---------------|                                 "
    - "L0.?[671,950] 1.01us 3.62mb                                                         |-------------L0.?-------------| "
    - "**** Simulation run 7, type=split(HighL0OverlapSingleFile)(split_times=[356]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.18[50,629] 1.01us     |-----------------------------------------L0.18------------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[50,356] 1.01us 5.28mb|--------------------L0.?---------------------|                                           "
    - "L0.?[357,629] 1.01us 4.72mb                                               |------------------L0.?------------------| "
    - "**** Simulation run 8, type=split(HighL0OverlapSingleFile)(split_times=[356, 670]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.19[76,932] 1.01us     |-----------------------------------------L0.19------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[76,356] 1.01us 3.27mb|-----------L0.?------------|                                                             "
    - "L0.?[357,670] 1.01us 3.66mb                             |-------------L0.?-------------|                             "
    - "L0.?[671,932] 1.01us 3.07mb                                                              |----------L0.?-----------| "
    - "**** Simulation run 9, type=split(HighL0OverlapSingleFile)(split_times=[356, 670]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.20[42,986] 1.01us     |-----------------------------------------L0.20------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[42,356] 1.01us 3.33mb|-----------L0.?------------|                                                             "
    - "L0.?[357,670] 1.01us 3.32mb                              |-----------L0.?------------|                               "
    - "L0.?[671,986] 1.01us 3.36mb                                                           |------------L0.?------------| "
    - "**** Simulation run 10, type=split(HighL0OverlapSingleFile)(split_times=[356, 670]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.21[173,950] 1.01us    |-----------------------------------------L0.21------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[173,356] 1.01us 2.36mb|-------L0.?--------|                                                                     "
    - "L0.?[357,670] 1.01us 4.03mb                     |---------------L0.?---------------|                                 "
    - "L0.?[671,950] 1.01us 3.62mb                                                         |-------------L0.?-------------| "
    - "**** Simulation run 11, type=split(HighL0OverlapSingleFile)(split_times=[356]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.22[50,629] 1.01us     |-----------------------------------------L0.22------------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[50,356] 1.01us 5.28mb|--------------------L0.?---------------------|                                           "
    - "L0.?[357,629] 1.01us 4.72mb                                               |------------------L0.?------------------| "
    - "**** Simulation run 12, type=split(HighL0OverlapSingleFile)(split_times=[356, 670]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.23[76,932] 1.01us     |-----------------------------------------L0.23------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[76,356] 1.01us 3.27mb|-----------L0.?------------|                                                             "
    - "L0.?[357,670] 1.01us 3.66mb                             |-------------L0.?-------------|                             "
    - "L0.?[671,932] 1.01us 3.07mb                                                              |----------L0.?-----------| "
    - "**** Simulation run 13, type=split(HighL0OverlapSingleFile)(split_times=[356, 670]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.24[42,986] 1.01us     |-----------------------------------------L0.24------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[42,356] 1.01us 3.33mb|-----------L0.?------------|                                                             "
    - "L0.?[357,670] 1.01us 3.32mb                              |-----------L0.?------------|                               "
    - "L0.?[671,986] 1.01us 3.36mb                                                           |------------L0.?------------| "
    - "**** Simulation run 14, type=split(HighL0OverlapSingleFile)(split_times=[356, 670]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.25[173,950] 1.01us    |-----------------------------------------L0.25------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[173,356] 1.01us 2.36mb|-------L0.?--------|                                                                     "
    - "L0.?[357,670] 1.01us 4.03mb                     |---------------L0.?---------------|                                 "
    - "L0.?[671,950] 1.01us 3.62mb                                                         |-------------L0.?-------------| "
    - "**** Simulation run 15, type=split(HighL0OverlapSingleFile)(split_times=[356]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.26[50,629] 1.01us     |-----------------------------------------L0.26------------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[50,356] 1.01us 5.28mb|--------------------L0.?---------------------|                                           "
    - "L0.?[357,629] 1.01us 4.72mb                                               |------------------L0.?------------------| "
    - "**** Simulation run 16, type=split(HighL0OverlapSingleFile)(split_times=[356, 670]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.27[76,932] 1.02us     |-----------------------------------------L0.27------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[76,356] 1.02us 3.27mb|-----------L0.?------------|                                                             "
    - "L0.?[357,670] 1.02us 3.66mb                             |-------------L0.?-------------|                             "
    - "L0.?[671,932] 1.02us 3.07mb                                                              |----------L0.?-----------| "
    - "**** Simulation run 17, type=split(HighL0OverlapSingleFile)(split_times=[356, 670]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.28[42,986] 1.02us     |-----------------------------------------L0.28------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[42,356] 1.02us 3.33mb|-----------L0.?------------|                                                             "
    - "L0.?[357,670] 1.02us 3.32mb                              |-----------L0.?------------|                               "
    - "L0.?[671,986] 1.02us 3.36mb                                                           |------------L0.?------------| "
    - "**** Simulation run 18, type=split(HighL0OverlapSingleFile)(split_times=[356, 670]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.29[173,950] 1.02us    |-----------------------------------------L0.29------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[173,356] 1.02us 2.36mb|-------L0.?--------|                                                                     "
    - "L0.?[357,670] 1.02us 4.03mb                     |---------------L0.?---------------|                                 "
    - "L0.?[671,950] 1.02us 3.62mb                                                         |-------------L0.?-------------| "
    - "**** Simulation run 19, type=split(HighL0OverlapSingleFile)(split_times=[356]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.30[50,629] 1.02us     |-----------------------------------------L0.30------------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[50,356] 1.02us 5.28mb|--------------------L0.?---------------------|                                           "
    - "L0.?[357,629] 1.02us 4.72mb                                               |------------------L0.?------------------| "
    - "**** Simulation run 20, type=split(HighL0OverlapSingleFile)(split_times=[356, 670]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.31[76,932] 1.02us     |-----------------------------------------L0.31------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[76,356] 1.02us 3.27mb|-----------L0.?------------|                                                             "
    - "L0.?[357,670] 1.02us 3.66mb                             |-------------L0.?-------------|                             "
    - "L0.?[671,932] 1.02us 3.07mb                                                              |----------L0.?-----------| "
    - "**** Simulation run 21, type=split(HighL0OverlapSingleFile)(split_times=[356, 670]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.32[42,986] 1.02us     |-----------------------------------------L0.32------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[42,356] 1.02us 3.33mb|-----------L0.?------------|                                                             "
    - "L0.?[357,670] 1.02us 3.32mb                              |-----------L0.?------------|                               "
    - "L0.?[671,986] 1.02us 3.36mb                                                           |------------L0.?------------| "
    - "**** Simulation run 22, type=split(HighL0OverlapSingleFile)(split_times=[356, 670]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.33[173,950] 1.02us    |-----------------------------------------L0.33------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[173,356] 1.02us 2.36mb|-------L0.?--------|                                                                     "
    - "L0.?[357,670] 1.02us 4.03mb                     |---------------L0.?---------------|                                 "
    - "L0.?[671,950] 1.02us 3.62mb                                                         |-------------L0.?-------------| "
    - "**** Simulation run 23, type=split(HighL0OverlapSingleFile)(split_times=[356]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.34[50,629] 1.02us     |-----------------------------------------L0.34------------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[50,356] 1.02us 5.28mb|--------------------L0.?---------------------|                                           "
    - "L0.?[357,629] 1.02us 4.72mb                                               |------------------L0.?------------------| "
    - "**** Simulation run 24, type=split(HighL0OverlapSingleFile)(split_times=[356, 670]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.35[76,932] 1.02us     |-----------------------------------------L0.35------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[76,356] 1.02us 3.27mb|-----------L0.?------------|                                                             "
    - "L0.?[357,670] 1.02us 3.66mb                             |-------------L0.?-------------|                             "
    - "L0.?[671,932] 1.02us 3.07mb                                                              |----------L0.?-----------| "
    - "**** Simulation run 25, type=split(HighL0OverlapSingleFile)(split_times=[356, 670]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.36[42,986] 1.02us     |-----------------------------------------L0.36------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[42,356] 1.02us 3.33mb|-----------L0.?------------|                                                             "
    - "L0.?[357,670] 1.02us 3.32mb                              |-----------L0.?------------|                               "
    - "L0.?[671,986] 1.02us 3.36mb                                                           |------------L0.?------------| "
    - "**** Simulation run 26, type=split(HighL0OverlapSingleFile)(split_times=[356, 670]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.37[173,950] 1.03us    |-----------------------------------------L0.37------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[173,356] 1.03us 2.36mb|-------L0.?--------|                                                                     "
    - "L0.?[357,670] 1.03us 4.03mb                     |---------------L0.?---------------|                                 "
    - "L0.?[671,950] 1.03us 3.62mb                                                         |-------------L0.?-------------| "
    - "**** Simulation run 27, type=split(HighL0OverlapSingleFile)(split_times=[356]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.38[50,629] 1.03us     |-----------------------------------------L0.38------------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[50,356] 1.03us 5.28mb|--------------------L0.?---------------------|                                           "
    - "L0.?[357,629] 1.03us 4.72mb                                               |------------------L0.?------------------| "
    - "**** Simulation run 28, type=split(HighL0OverlapSingleFile)(split_times=[356, 670]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.39[76,932] 1.03us     |-----------------------------------------L0.39------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[76,356] 1.03us 3.27mb|-----------L0.?------------|                                                             "
    - "L0.?[357,670] 1.03us 3.66mb                             |-------------L0.?-------------|                             "
    - "L0.?[671,932] 1.03us 3.07mb                                                              |----------L0.?-----------| "
    - "**** Simulation run 29, type=split(HighL0OverlapSingleFile)(split_times=[356, 670]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.40[42,986] 1.03us     |-----------------------------------------L0.40------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[42,356] 1.03us 3.33mb|-----------L0.?------------|                                                             "
    - "L0.?[357,670] 1.03us 3.32mb                              |-----------L0.?------------|                               "
    - "L0.?[671,986] 1.03us 3.36mb                                                           |------------L0.?------------| "
    - "**** Simulation run 30, type=split(HighL0OverlapSingleFile)(split_times=[356, 670]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.41[173,950] 1.03us    |-----------------------------------------L0.41------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[173,356] 1.03us 2.36mb|-------L0.?--------|                                                                     "
    - "L0.?[357,670] 1.03us 4.03mb                     |---------------L0.?---------------|                                 "
    - "L0.?[671,950] 1.03us 3.62mb                                                         |-------------L0.?-------------| "
    - "**** Simulation run 31, type=split(HighL0OverlapSingleFile)(split_times=[356]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.42[50,629] 1.03us     |-----------------------------------------L0.42------------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[50,356] 1.03us 5.28mb|--------------------L0.?---------------------|                                           "
    - "L0.?[357,629] 1.03us 4.72mb                                               |------------------L0.?------------------| "
    - "**** Simulation run 32, type=split(HighL0OverlapSingleFile)(split_times=[356, 670]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.43[76,932] 1.03us     |-----------------------------------------L0.43------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[76,356] 1.03us 3.27mb|-----------L0.?------------|                                                             "
    - "L0.?[357,670] 1.03us 3.66mb                             |-------------L0.?-------------|                             "
    - "L0.?[671,932] 1.03us 3.07mb                                                              |----------L0.?-----------| "
    - "**** Simulation run 33, type=split(HighL0OverlapSingleFile)(split_times=[356, 670]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.44[42,986] 1.03us     |-----------------------------------------L0.44------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[42,356] 1.03us 3.33mb|-----------L0.?------------|                                                             "
    - "L0.?[357,670] 1.03us 3.32mb                              |-----------L0.?------------|                               "
    - "L0.?[671,986] 1.03us 3.36mb                                                           |------------L0.?------------| "
    - "**** Simulation run 34, type=split(HighL0OverlapSingleFile)(split_times=[356, 670]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.45[173,950] 1.03us    |-----------------------------------------L0.45------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[173,356] 1.03us 2.36mb|-------L0.?--------|                                                                     "
    - "L0.?[357,670] 1.03us 4.03mb                     |---------------L0.?---------------|                                 "
    - "L0.?[671,950] 1.03us 3.62mb                                                         |-------------L0.?-------------| "
    - "**** Simulation run 35, type=split(HighL0OverlapSingleFile)(split_times=[356]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.46[50,629] 1.03us     |-----------------------------------------L0.46------------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[50,356] 1.03us 5.28mb|--------------------L0.?---------------------|                                           "
    - "L0.?[357,629] 1.03us 4.72mb                                               |------------------L0.?------------------| "
    - "**** Simulation run 36, type=split(HighL0OverlapSingleFile)(split_times=[356, 670]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.47[76,932] 1.04us     |-----------------------------------------L0.47------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[76,356] 1.04us 3.27mb|-----------L0.?------------|                                                             "
    - "L0.?[357,670] 1.04us 3.66mb                             |-------------L0.?-------------|                             "
    - "L0.?[671,932] 1.04us 3.07mb                                                              |----------L0.?-----------| "
    - "**** Simulation run 37, type=split(HighL0OverlapSingleFile)(split_times=[356, 670]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.48[42,986] 1.04us     |-----------------------------------------L0.48------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[42,356] 1.04us 3.33mb|-----------L0.?------------|                                                             "
    - "L0.?[357,670] 1.04us 3.32mb                              |-----------L0.?------------|                               "
    - "L0.?[671,986] 1.04us 3.36mb                                                           |------------L0.?------------| "
    - "**** Simulation run 38, type=split(HighL0OverlapSingleFile)(split_times=[356, 670]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.49[173,950] 1.04us    |-----------------------------------------L0.49------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[173,356] 1.04us 2.36mb|-------L0.?--------|                                                                     "
    - "L0.?[357,670] 1.04us 4.03mb                     |---------------L0.?---------------|                                 "
    - "L0.?[671,950] 1.04us 3.62mb                                                         |-------------L0.?-------------| "
    - "**** Simulation run 39, type=split(HighL0OverlapSingleFile)(split_times=[356]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.50[50,629] 1.04us     |-----------------------------------------L0.50------------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[50,356] 1.04us 5.28mb|--------------------L0.?---------------------|                                           "
    - "L0.?[357,629] 1.04us 4.72mb                                               |------------------L0.?------------------| "
    - "**** Simulation run 40, type=split(HighL0OverlapSingleFile)(split_times=[356, 670]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.51[76,932] 1.04us     |-----------------------------------------L0.51------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[76,356] 1.04us 3.27mb|-----------L0.?------------|                                                             "
    - "L0.?[357,670] 1.04us 3.66mb                             |-------------L0.?-------------|                             "
    - "L0.?[671,932] 1.04us 3.07mb                                                              |----------L0.?-----------| "
    - "**** Simulation run 41, type=split(HighL0OverlapSingleFile)(split_times=[356, 670]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.52[42,986] 1.04us     |-----------------------------------------L0.52------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[42,356] 1.04us 3.33mb|-----------L0.?------------|                                                             "
    - "L0.?[357,670] 1.04us 3.32mb                              |-----------L0.?------------|                               "
    - "L0.?[671,986] 1.04us 3.36mb                                                           |------------L0.?------------| "
    - "**** Simulation run 42, type=split(HighL0OverlapSingleFile)(split_times=[356, 670]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.53[173,950] 1.04us    |-----------------------------------------L0.53------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[173,356] 1.04us 2.36mb|-------L0.?--------|                                                                     "
    - "L0.?[357,670] 1.04us 4.03mb                     |---------------L0.?---------------|                                 "
    - "L0.?[671,950] 1.04us 3.62mb                                                         |-------------L0.?-------------| "
    - "**** Simulation run 43, type=split(HighL0OverlapSingleFile)(split_times=[356]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.54[50,629] 1.04us     |-----------------------------------------L0.54------------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[50,356] 1.04us 5.28mb|--------------------L0.?---------------------|                                           "
    - "L0.?[357,629] 1.04us 4.72mb                                               |------------------L0.?------------------| "
    - "**** Simulation run 44, type=split(HighL0OverlapSingleFile)(split_times=[356, 670]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.55[76,932] 1.04us     |-----------------------------------------L0.55------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[76,356] 1.04us 3.27mb|-----------L0.?------------|                                                             "
    - "L0.?[357,670] 1.04us 3.66mb                             |-------------L0.?-------------|                             "
    - "L0.?[671,932] 1.04us 3.07mb                                                              |----------L0.?-----------| "
    - "**** Simulation run 45, type=split(HighL0OverlapSingleFile)(split_times=[356, 670]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.56[42,986] 1.05us     |-----------------------------------------L0.56------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[42,356] 1.05us 3.33mb|-----------L0.?------------|                                                             "
    - "L0.?[357,670] 1.05us 3.32mb                              |-----------L0.?------------|                               "
    - "L0.?[671,986] 1.05us 3.36mb                                                           |------------L0.?------------| "
    - "**** Simulation run 46, type=split(HighL0OverlapSingleFile)(split_times=[356, 670]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.57[173,950] 1.05us    |-----------------------------------------L0.57------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[173,356] 1.05us 2.36mb|-------L0.?--------|                                                                     "
    - "L0.?[357,670] 1.05us 4.03mb                     |---------------L0.?---------------|                                 "
    - "L0.?[671,950] 1.05us 3.62mb                                                         |-------------L0.?-------------| "
    - "**** Simulation run 47, type=split(HighL0OverlapSingleFile)(split_times=[356]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.58[50,629] 1.05us     |-----------------------------------------L0.58------------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[50,356] 1.05us 5.28mb|--------------------L0.?---------------------|                                           "
    - "L0.?[357,629] 1.05us 4.72mb                                               |------------------L0.?------------------| "
    - "**** Simulation run 48, type=split(HighL0OverlapSingleFile)(split_times=[356, 670]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.59[76,932] 1.05us     |-----------------------------------------L0.59------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[76,356] 1.05us 3.27mb|-----------L0.?------------|                                                             "
    - "L0.?[357,670] 1.05us 3.66mb                             |-------------L0.?-------------|                             "
    - "L0.?[671,932] 1.05us 3.07mb                                                              |----------L0.?-----------| "
    - "**** Simulation run 49, type=split(HighL0OverlapSingleFile)(split_times=[356, 670]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.60[42,986] 1.05us     |-----------------------------------------L0.60------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[42,356] 1.05us 3.33mb|-----------L0.?------------|                                                             "
    - "L0.?[357,670] 1.05us 3.32mb                              |-----------L0.?------------|                               "
    - "L0.?[671,986] 1.05us 3.36mb                                                           |------------L0.?------------| "
    - "Committing partition 1:"
    - "  Soft Deleting 50 files: L0.11, L0.12, L0.13, L0.14, L0.15, L0.16, L0.17, L0.18, L0.19, L0.20, L0.21, L0.22, L0.23, L0.24, L0.25, L0.26, L0.27, L0.28, L0.29, L0.30, L0.31, L0.32, L0.33, L0.34, L0.35, L0.36, L0.37, L0.38, L0.39, L0.40, L0.41, L0.42, L0.43, L0.44, L0.45, L0.46, L0.47, L0.48, L0.49, L0.50, L0.51, L0.52, L0.53, L0.54, L0.55, L0.56, L0.57, L0.58, L0.59, L0.60"
    - "  Creating 138 files"
    - "**** Simulation run 50, type=split(CompactAndSplitOutput(FoundSubsetLessThanMaxCompactSize))(split_times=[357, 672]). 83 Input Files, 300mb total:"
    - "L0                                                                                                                 "
    - "L0.63[671,932] 1us 3.07mb                                                           |--------L0.63---------|       "
    - "L0.62[357,670] 1us 3.66mb                              |-----------L0.62-----------|                               "
    - "L0.61[76,356] 1us 3.27mb    |---------L0.61----------|                                                             "
    - "L0.66[671,986] 1us 3.36mb                                                           |-----------L0.66------------| "
    - "L0.65[357,670] 1us 3.32mb                              |-----------L0.65-----------|                               "
    - "L0.64[42,356] 1us 3.33mb |-----------L0.64-----------|                                                             "
    - "L0.69[671,950] 1us 3.62mb                                                           |---------L0.69----------|     "
    - "L0.68[357,670] 1us 4.03mb                              |-----------L0.68-----------|                               "
    - "L0.67[173,356] 1us 2.36mb            |-----L0.67-----|                                                             "
    - "L0.71[357,629] 1us 4.72mb                              |---------L0.71---------|                                   "
    - "L0.70[50,356] 1us 5.28mb |-----------L0.70-----------|                                                             "
    - "L0.74[671,932] 1us 3.07mb                                                           |--------L0.74---------|       "
    - "L0.73[357,670] 1us 3.66mb                              |-----------L0.73-----------|                               "
    - "L0.72[76,356] 1us 3.27mb    |---------L0.72----------|                                                             "
    - "L0.77[671,986] 1us 3.36mb                                                           |-----------L0.77------------| "
    - "L0.76[357,670] 1us 3.32mb                              |-----------L0.76-----------|                               "
    - "L0.75[42,356] 1us 3.33mb |-----------L0.75-----------|                                                             "
    - "L0.80[671,950] 1.01us 3.62mb                                                           |---------L0.80----------|     "
    - "L0.79[357,670] 1.01us 4.03mb                              |-----------L0.79-----------|                               "
    - "L0.78[173,356] 1.01us 2.36mb            |-----L0.78-----|                                                             "
    - "L0.82[357,629] 1.01us 4.72mb                              |---------L0.82---------|                                   "
    - "L0.81[50,356] 1.01us 5.28mb|-----------L0.81-----------|                                                             "
    - "L0.85[671,932] 1.01us 3.07mb                                                           |--------L0.85---------|       "
    - "L0.84[357,670] 1.01us 3.66mb                              |-----------L0.84-----------|                               "
    - "L0.83[76,356] 1.01us 3.27mb   |---------L0.83----------|                                                             "
    - "L0.88[671,986] 1.01us 3.36mb                                                           |-----------L0.88------------| "
    - "L0.87[357,670] 1.01us 3.32mb                              |-----------L0.87-----------|                               "
    - "L0.86[42,356] 1.01us 3.33mb|-----------L0.86-----------|                                                             "
    - "L0.91[671,950] 1.01us 3.62mb                                                           |---------L0.91----------|     "
    - "L0.90[357,670] 1.01us 4.03mb                              |-----------L0.90-----------|                               "
    - "L0.89[173,356] 1.01us 2.36mb            |-----L0.89-----|                                                             "
    - "L0.93[357,629] 1.01us 4.72mb                              |---------L0.93---------|                                   "
    - "L0.92[50,356] 1.01us 5.28mb|-----------L0.92-----------|                                                             "
    - "L0.96[671,932] 1.01us 3.07mb                                                           |--------L0.96---------|       "
    - "L0.95[357,670] 1.01us 3.66mb                              |-----------L0.95-----------|                               "
    - "L0.94[76,356] 1.01us 3.27mb   |---------L0.94----------|                                                             "
    - "L0.99[671,986] 1.01us 3.36mb                                                           |-----------L0.99------------| "
    - "L0.98[357,670] 1.01us 3.32mb                              |-----------L0.98-----------|                               "
    - "L0.97[42,356] 1.01us 3.33mb|-----------L0.97-----------|                                                             "
    - "L0.102[671,950] 1.01us 3.62mb                                                           |---------L0.102---------|     "
    - "L0.101[357,670] 1.01us 4.03mb                              |----------L0.101-----------|                               "
    - "L0.100[173,356] 1.01us 2.36mb            |----L0.100-----|                                                             "
    - "L0.104[357,629] 1.01us 4.72mb                              |--------L0.104---------|                                   "
    - "L0.103[50,356] 1.01us 5.28mb|----------L0.103-----------|                                                             "
    - "L0.107[671,932] 1.02us 3.07mb                                                           |--------L0.107--------|       "
    - "L0.106[357,670] 1.02us 3.66mb                              |----------L0.106-----------|                               "
    - "L0.105[76,356] 1.02us 3.27mb   |---------L0.105---------|                                                             "
    - "L0.110[671,986] 1.02us 3.36mb                                                           |-----------L0.110-----------| "
    - "L0.109[357,670] 1.02us 3.32mb                              |----------L0.109-----------|                               "
    - "L0.108[42,356] 1.02us 3.33mb|----------L0.108-----------|                                                             "
    - "L0.113[671,950] 1.02us 3.62mb                                                           |---------L0.113---------|     "
    - "L0.112[357,670] 1.02us 4.03mb                              |----------L0.112-----------|                               "
    - "L0.111[173,356] 1.02us 2.36mb            |----L0.111-----|                                                             "
    - "L0.115[357,629] 1.02us 4.72mb                              |--------L0.115---------|                                   "
    - "L0.114[50,356] 1.02us 5.28mb|----------L0.114-----------|                                                             "
    - "L0.118[671,932] 1.02us 3.07mb                                                           |--------L0.118--------|       "
    - "L0.117[357,670] 1.02us 3.66mb                              |----------L0.117-----------|                               "
    - "L0.116[76,356] 1.02us 3.27mb   |---------L0.116---------|                                                             "
    - "L0.121[671,986] 1.02us 3.36mb                                                           |-----------L0.121-----------| "
    - "L0.120[357,670] 1.02us 3.32mb                              |----------L0.120-----------|                               "
    - "L0.119[42,356] 1.02us 3.33mb|----------L0.119-----------|                                                             "
    - "L0.124[671,950] 1.02us 3.62mb                                                           |---------L0.124---------|     "
    - "L0.123[357,670] 1.02us 4.03mb                              |----------L0.123-----------|                               "
    - "L0.122[173,356] 1.02us 2.36mb            |----L0.122-----|                                                             "
    - "L0.126[357,629] 1.02us 4.72mb                              |--------L0.126---------|                                   "
    - "L0.125[50,356] 1.02us 5.28mb|----------L0.125-----------|                                                             "
    - "L0.129[671,932] 1.02us 3.07mb                                                           |--------L0.129--------|       "
    - "L0.128[357,670] 1.02us 3.66mb                              |----------L0.128-----------|                               "
    - "L0.127[76,356] 1.02us 3.27mb   |---------L0.127---------|                                                             "
    - "L0.132[671,986] 1.02us 3.36mb                                                           |-----------L0.132-----------| "
    - "L0.131[357,670] 1.02us 3.32mb                              |----------L0.131-----------|                               "
    - "L0.130[42,356] 1.02us 3.33mb|----------L0.130-----------|                                                             "
    - "L0.135[671,950] 1.03us 3.62mb                                                           |---------L0.135---------|     "
    - "L0.134[357,670] 1.03us 4.03mb                              |----------L0.134-----------|                               "
    - "L0.133[173,356] 1.03us 2.36mb            |----L0.133-----|                                                             "
    - "L0.137[357,629] 1.03us 4.72mb                              |--------L0.137---------|                                   "
    - "L0.136[50,356] 1.03us 5.28mb|----------L0.136-----------|                                                             "
    - "L0.140[671,932] 1.03us 3.07mb                                                           |--------L0.140--------|       "
    - "L0.139[357,670] 1.03us 3.66mb                              |----------L0.139-----------|                               "
    - "L0.138[76,356] 1.03us 3.27mb   |---------L0.138---------|                                                             "
    - "L0.143[671,986] 1.03us 3.36mb                                                           |-----------L0.143-----------| "
    - "L0.142[357,670] 1.03us 3.32mb                              |----------L0.142-----------|                               "
    - "L0.141[42,356] 1.03us 3.33mb|----------L0.141-----------|                                                             "
    - "**** 3 Output Files (parquet_file_id not yet assigned), 300mb total:"
    - "L1                                                                                                                 "
    - "L1.?[42,357] 1.03us 100.11mb|------------L1.?------------|                                                            "
    - "L1.?[358,672] 1.03us 99.79mb                              |-----------L1.?------------|                               "
    - "L1.?[673,986] 1.03us 100.11mb                                                            |-----------L1.?------------| "
    - "Committing partition 1:"
    - "  Soft Deleting 83 files: L0.61, L0.62, L0.63, L0.64, L0.65, L0.66, L0.67, L0.68, L0.69, L0.70, L0.71, L0.72, L0.73, L0.74, L0.75, L0.76, L0.77, L0.78, L0.79, L0.80, L0.81, L0.82, L0.83, L0.84, L0.85, L0.86, L0.87, L0.88, L0.89, L0.90, L0.91, L0.92, L0.93, L0.94, L0.95, L0.96, L0.97, L0.98, L0.99, L0.100, L0.101, L0.102, L0.103, L0.104, L0.105, L0.106, L0.107, L0.108, L0.109, L0.110, L0.111, L0.112, L0.113, L0.114, L0.115, L0.116, L0.117, L0.118, L0.119, L0.120, L0.121, L0.122, L0.123, L0.124, L0.125, L0.126, L0.127, L0.128, L0.129, L0.130, L0.131, L0.132, L0.133, L0.134, L0.135, L0.136, L0.137, L0.138, L0.139, L0.140, L0.141, L0.142, L0.143"
    - "  Creating 3 files"
    - "**** Simulation run 51, type=split(ReduceOverlap)(split_times=[357]). 1 Input Files, 4.03mb total:"
    - "L0, all files 4.03mb                                                                                               "
    - "L0.145[357,670] 1.03us   |-----------------------------------------L0.145-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 4.03mb total:"
    - "L0                                                                                                                 "
    - "L0.?[357,357] 1.03us 0b  |L0.?|                                                                                    "
    - "L0.?[358,670] 1.03us 4.03mb|-----------------------------------------L0.?------------------------------------------| "
    - "**** Simulation run 52, type=split(ReduceOverlap)(split_times=[672]). 1 Input Files, 3.62mb total:"
    - "L0, all files 3.62mb                                                                                               "
    - "L0.146[671,950] 1.03us   |-----------------------------------------L0.146-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3.62mb total:"
    - "L0                                                                                                                 "
    - "L0.?[671,672] 1.03us 13.27kb|L0.?|                                                                                    "
    - "L0.?[673,950] 1.03us 3.6mb|-----------------------------------------L0.?------------------------------------------| "
    - "**** Simulation run 53, type=split(ReduceOverlap)(split_times=[357]). 1 Input Files, 4.72mb total:"
    - "L0, all files 4.72mb                                                                                               "
    - "L0.148[357,629] 1.03us   |-----------------------------------------L0.148-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 4.72mb total:"
    - "L0                                                                                                                 "
    - "L0.?[357,357] 1.03us 0b  |L0.?|                                                                                    "
    - "L0.?[358,629] 1.03us 4.72mb|-----------------------------------------L0.?------------------------------------------| "
    - "**** Simulation run 54, type=split(ReduceOverlap)(split_times=[357]). 1 Input Files, 3.66mb total:"
    - "L0, all files 3.66mb                                                                                               "
    - "L0.150[357,670] 1.03us   |-----------------------------------------L0.150-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3.66mb total:"
    - "L0                                                                                                                 "
    - "L0.?[357,357] 1.03us 0b  |L0.?|                                                                                    "
    - "L0.?[358,670] 1.03us 3.66mb|-----------------------------------------L0.?------------------------------------------| "
    - "**** Simulation run 55, type=split(ReduceOverlap)(split_times=[672]). 1 Input Files, 3.07mb total:"
    - "L0, all files 3.07mb                                                                                               "
    - "L0.151[671,932] 1.03us   |-----------------------------------------L0.151-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3.07mb total:"
    - "L0                                                                                                                 "
    - "L0.?[671,672] 1.03us 12.05kb|L0.?|                                                                                    "
    - "L0.?[673,932] 1.03us 3.06mb|-----------------------------------------L0.?------------------------------------------| "
    - "**** Simulation run 56, type=split(ReduceOverlap)(split_times=[357]). 1 Input Files, 3.32mb total:"
    - "L0, all files 3.32mb                                                                                               "
    - "L0.153[357,670] 1.03us   |-----------------------------------------L0.153-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3.32mb total:"
    - "L0                                                                                                                 "
    - "L0.?[357,357] 1.03us 0b  |L0.?|                                                                                    "
    - "L0.?[358,670] 1.03us 3.32mb|-----------------------------------------L0.?------------------------------------------| "
    - "**** Simulation run 57, type=split(ReduceOverlap)(split_times=[672]). 1 Input Files, 3.36mb total:"
    - "L0, all files 3.36mb                                                                                               "
    - "L0.154[671,986] 1.03us   |-----------------------------------------L0.154-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3.36mb total:"
    - "L0                                                                                                                 "
    - "L0.?[671,672] 1.03us 10.92kb|L0.?|                                                                                    "
    - "L0.?[673,986] 1.03us 3.35mb|-----------------------------------------L0.?------------------------------------------| "
    - "**** Simulation run 58, type=split(ReduceOverlap)(split_times=[357]). 1 Input Files, 4.03mb total:"
    - "L0, all files 4.03mb                                                                                               "
    - "L0.156[357,670] 1.03us   |-----------------------------------------L0.156-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 4.03mb total:"
    - "L0                                                                                                                 "
    - "L0.?[357,357] 1.03us 0b  |L0.?|                                                                                    "
    - "L0.?[358,670] 1.03us 4.03mb|-----------------------------------------L0.?------------------------------------------| "
    - "**** Simulation run 59, type=split(ReduceOverlap)(split_times=[672]). 1 Input Files, 3.62mb total:"
    - "L0, all files 3.62mb                                                                                               "
    - "L0.157[671,950] 1.03us   |-----------------------------------------L0.157-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3.62mb total:"
    - "L0                                                                                                                 "
    - "L0.?[671,672] 1.03us 13.27kb|L0.?|                                                                                    "
    - "L0.?[673,950] 1.03us 3.6mb|-----------------------------------------L0.?------------------------------------------| "
    - "**** Simulation run 60, type=split(ReduceOverlap)(split_times=[357]). 1 Input Files, 4.72mb total:"
    - "L0, all files 4.72mb                                                                                               "
    - "L0.159[357,629] 1.03us   |-----------------------------------------L0.159-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 4.72mb total:"
    - "L0                                                                                                                 "
    - "L0.?[357,357] 1.03us 0b  |L0.?|                                                                                    "
    - "L0.?[358,629] 1.03us 4.72mb|-----------------------------------------L0.?------------------------------------------| "
    - "**** Simulation run 61, type=split(ReduceOverlap)(split_times=[357]). 1 Input Files, 3.66mb total:"
    - "L0, all files 3.66mb                                                                                               "
    - "L0.161[357,670] 1.04us   |-----------------------------------------L0.161-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3.66mb total:"
    - "L0                                                                                                                 "
    - "L0.?[357,357] 1.04us 0b  |L0.?|                                                                                    "
    - "L0.?[358,670] 1.04us 3.66mb|-----------------------------------------L0.?------------------------------------------| "
    - "**** Simulation run 62, type=split(ReduceOverlap)(split_times=[672]). 1 Input Files, 3.07mb total:"
    - "L0, all files 3.07mb                                                                                               "
    - "L0.162[671,932] 1.04us   |-----------------------------------------L0.162-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3.07mb total:"
    - "L0                                                                                                                 "
    - "L0.?[671,672] 1.04us 12.05kb|L0.?|                                                                                    "
    - "L0.?[673,932] 1.04us 3.06mb|-----------------------------------------L0.?------------------------------------------| "
    - "**** Simulation run 63, type=split(ReduceOverlap)(split_times=[357]). 1 Input Files, 3.32mb total:"
    - "L0, all files 3.32mb                                                                                               "
    - "L0.164[357,670] 1.04us   |-----------------------------------------L0.164-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3.32mb total:"
    - "L0                                                                                                                 "
    - "L0.?[357,357] 1.04us 0b  |L0.?|                                                                                    "
    - "L0.?[358,670] 1.04us 3.32mb|-----------------------------------------L0.?------------------------------------------| "
    - "**** Simulation run 64, type=split(ReduceOverlap)(split_times=[672]). 1 Input Files, 3.36mb total:"
    - "L0, all files 3.36mb                                                                                               "
    - "L0.165[671,986] 1.04us   |-----------------------------------------L0.165-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3.36mb total:"
    - "L0                                                                                                                 "
    - "L0.?[671,672] 1.04us 10.92kb|L0.?|                                                                                    "
    - "L0.?[673,986] 1.04us 3.35mb|-----------------------------------------L0.?------------------------------------------| "
    - "**** Simulation run 65, type=split(ReduceOverlap)(split_times=[357]). 1 Input Files, 4.03mb total:"
    - "L0, all files 4.03mb                                                                                               "
    - "L0.167[357,670] 1.04us   |-----------------------------------------L0.167-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 4.03mb total:"
    - "L0                                                                                                                 "
    - "L0.?[357,357] 1.04us 0b  |L0.?|                                                                                    "
    - "L0.?[358,670] 1.04us 4.03mb|-----------------------------------------L0.?------------------------------------------| "
    - "**** Simulation run 66, type=split(ReduceOverlap)(split_times=[672]). 1 Input Files, 3.62mb total:"
    - "L0, all files 3.62mb                                                                                               "
    - "L0.168[671,950] 1.04us   |-----------------------------------------L0.168-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3.62mb total:"
    - "L0                                                                                                                 "
    - "L0.?[671,672] 1.04us 13.27kb|L0.?|                                                                                    "
    - "L0.?[673,950] 1.04us 3.6mb|-----------------------------------------L0.?------------------------------------------| "
    - "**** Simulation run 67, type=split(ReduceOverlap)(split_times=[357]). 1 Input Files, 4.72mb total:"
    - "L0, all files 4.72mb                                                                                               "
    - "L0.170[357,629] 1.04us   |-----------------------------------------L0.170-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 4.72mb total:"
    - "L0                                                                                                                 "
    - "L0.?[357,357] 1.04us 0b  |L0.?|                                                                                    "
    - "L0.?[358,629] 1.04us 4.72mb|-----------------------------------------L0.?------------------------------------------| "
    - "**** Simulation run 68, type=split(ReduceOverlap)(split_times=[357]). 1 Input Files, 3.66mb total:"
    - "L0, all files 3.66mb                                                                                               "
    - "L0.172[357,670] 1.04us   |-----------------------------------------L0.172-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3.66mb total:"
    - "L0                                                                                                                 "
    - "L0.?[357,357] 1.04us 0b  |L0.?|                                                                                    "
    - "L0.?[358,670] 1.04us 3.66mb|-----------------------------------------L0.?------------------------------------------| "
    - "**** Simulation run 69, type=split(ReduceOverlap)(split_times=[672]). 1 Input Files, 3.07mb total:"
    - "L0, all files 3.07mb                                                                                               "
    - "L0.173[671,932] 1.04us   |-----------------------------------------L0.173-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3.07mb total:"
    - "L0                                                                                                                 "
    - "L0.?[671,672] 1.04us 12.05kb|L0.?|                                                                                    "
    - "L0.?[673,932] 1.04us 3.06mb|-----------------------------------------L0.?------------------------------------------| "
    - "**** Simulation run 70, type=split(ReduceOverlap)(split_times=[357]). 1 Input Files, 3.32mb total:"
    - "L0, all files 3.32mb                                                                                               "
    - "L0.175[357,670] 1.04us   |-----------------------------------------L0.175-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3.32mb total:"
    - "L0                                                                                                                 "
    - "L0.?[357,357] 1.04us 0b  |L0.?|                                                                                    "
    - "L0.?[358,670] 1.04us 3.32mb|-----------------------------------------L0.?------------------------------------------| "
    - "**** Simulation run 71, type=split(ReduceOverlap)(split_times=[672]). 1 Input Files, 3.36mb total:"
    - "L0, all files 3.36mb                                                                                               "
    - "L0.176[671,986] 1.04us   |-----------------------------------------L0.176-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3.36mb total:"
    - "L0                                                                                                                 "
    - "L0.?[671,672] 1.04us 10.92kb|L0.?|                                                                                    "
    - "L0.?[673,986] 1.04us 3.35mb|-----------------------------------------L0.?------------------------------------------| "
    - "**** Simulation run 72, type=split(ReduceOverlap)(split_times=[357]). 1 Input Files, 4.03mb total:"
    - "L0, all files 4.03mb                                                                                               "
    - "L0.178[357,670] 1.04us   |-----------------------------------------L0.178-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 4.03mb total:"
    - "L0                                                                                                                 "
    - "L0.?[357,357] 1.04us 0b  |L0.?|                                                                                    "
    - "L0.?[358,670] 1.04us 4.03mb|-----------------------------------------L0.?------------------------------------------| "
    - "**** Simulation run 73, type=split(ReduceOverlap)(split_times=[672]). 1 Input Files, 3.62mb total:"
    - "L0, all files 3.62mb                                                                                               "
    - "L0.179[671,950] 1.04us   |-----------------------------------------L0.179-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3.62mb total:"
    - "L0                                                                                                                 "
    - "L0.?[671,672] 1.04us 13.27kb|L0.?|                                                                                    "
    - "L0.?[673,950] 1.04us 3.6mb|-----------------------------------------L0.?------------------------------------------| "
    - "**** Simulation run 74, type=split(ReduceOverlap)(split_times=[357]). 1 Input Files, 4.72mb total:"
    - "L0, all files 4.72mb                                                                                               "
    - "L0.181[357,629] 1.04us   |-----------------------------------------L0.181-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 4.72mb total:"
    - "L0                                                                                                                 "
    - "L0.?[357,357] 1.04us 0b  |L0.?|                                                                                    "
    - "L0.?[358,629] 1.04us 4.72mb|-----------------------------------------L0.?------------------------------------------| "
    - "**** Simulation run 75, type=split(ReduceOverlap)(split_times=[357]). 1 Input Files, 3.66mb total:"
    - "L0, all files 3.66mb                                                                                               "
    - "L0.183[357,670] 1.04us   |-----------------------------------------L0.183-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3.66mb total:"
    - "L0                                                                                                                 "
    - "L0.?[357,357] 1.04us 0b  |L0.?|                                                                                    "
    - "L0.?[358,670] 1.04us 3.66mb|-----------------------------------------L0.?------------------------------------------| "
    - "**** Simulation run 76, type=split(ReduceOverlap)(split_times=[672]). 1 Input Files, 3.07mb total:"
    - "L0, all files 3.07mb                                                                                               "
    - "L0.184[671,932] 1.04us   |-----------------------------------------L0.184-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3.07mb total:"
    - "L0                                                                                                                 "
    - "L0.?[671,672] 1.04us 12.05kb|L0.?|                                                                                    "
    - "L0.?[673,932] 1.04us 3.06mb|-----------------------------------------L0.?------------------------------------------| "
    - "**** Simulation run 77, type=split(ReduceOverlap)(split_times=[357]). 1 Input Files, 3.32mb total:"
    - "L0, all files 3.32mb                                                                                               "
    - "L0.186[357,670] 1.05us   |-----------------------------------------L0.186-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3.32mb total:"
    - "L0                                                                                                                 "
    - "L0.?[357,357] 1.05us 0b  |L0.?|                                                                                    "
    - "L0.?[358,670] 1.05us 3.32mb|-----------------------------------------L0.?------------------------------------------| "
    - "**** Simulation run 78, type=split(ReduceOverlap)(split_times=[672]). 1 Input Files, 3.36mb total:"
    - "L0, all files 3.36mb                                                                                               "
    - "L0.187[671,986] 1.05us   |-----------------------------------------L0.187-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3.36mb total:"
    - "L0                                                                                                                 "
    - "L0.?[671,672] 1.05us 10.92kb|L0.?|                                                                                    "
    - "L0.?[673,986] 1.05us 3.35mb|-----------------------------------------L0.?------------------------------------------| "
    - "**** Simulation run 79, type=split(ReduceOverlap)(split_times=[357]). 1 Input Files, 4.03mb total:"
    - "L0, all files 4.03mb                                                                                               "
    - "L0.189[357,670] 1.05us   |-----------------------------------------L0.189-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 4.03mb total:"
    - "L0                                                                                                                 "
    - "L0.?[357,357] 1.05us 0b  |L0.?|                                                                                    "
    - "L0.?[358,670] 1.05us 4.03mb|-----------------------------------------L0.?------------------------------------------| "
    - "**** Simulation run 80, type=split(ReduceOverlap)(split_times=[672]). 1 Input Files, 3.62mb total:"
    - "L0, all files 3.62mb                                                                                               "
    - "L0.190[671,950] 1.05us   |-----------------------------------------L0.190-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3.62mb total:"
    - "L0                                                                                                                 "
    - "L0.?[671,672] 1.05us 13.27kb|L0.?|                                                                                    "
    - "L0.?[673,950] 1.05us 3.6mb|-----------------------------------------L0.?------------------------------------------| "
    - "**** Simulation run 81, type=split(ReduceOverlap)(split_times=[357]). 1 Input Files, 4.72mb total:"
    - "L0, all files 4.72mb                                                                                               "
    - "L0.192[357,629] 1.05us   |-----------------------------------------L0.192-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 4.72mb total:"
    - "L0                                                                                                                 "
    - "L0.?[357,357] 1.05us 0b  |L0.?|                                                                                    "
    - "L0.?[358,629] 1.05us 4.72mb|-----------------------------------------L0.?------------------------------------------| "
    - "**** Simulation run 82, type=split(ReduceOverlap)(split_times=[357]). 1 Input Files, 3.66mb total:"
    - "L0, all files 3.66mb                                                                                               "
    - "L0.194[357,670] 1.05us   |-----------------------------------------L0.194-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3.66mb total:"
    - "L0                                                                                                                 "
    - "L0.?[357,357] 1.05us 0b  |L0.?|                                                                                    "
    - "L0.?[358,670] 1.05us 3.66mb|-----------------------------------------L0.?------------------------------------------| "
    - "**** Simulation run 83, type=split(ReduceOverlap)(split_times=[672]). 1 Input Files, 3.07mb total:"
    - "L0, all files 3.07mb                                                                                               "
    - "L0.195[671,932] 1.05us   |-----------------------------------------L0.195-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3.07mb total:"
    - "L0                                                                                                                 "
    - "L0.?[671,672] 1.05us 12.05kb|L0.?|                                                                                    "
    - "L0.?[673,932] 1.05us 3.06mb|-----------------------------------------L0.?------------------------------------------| "
    - "**** Simulation run 84, type=split(ReduceOverlap)(split_times=[357]). 1 Input Files, 3.32mb total:"
    - "L0, all files 3.32mb                                                                                               "
    - "L0.197[357,670] 1.05us   |-----------------------------------------L0.197-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3.32mb total:"
    - "L0                                                                                                                 "
    - "L0.?[357,357] 1.05us 0b  |L0.?|                                                                                    "
    - "L0.?[358,670] 1.05us 3.32mb|-----------------------------------------L0.?------------------------------------------| "
    - "**** Simulation run 85, type=split(ReduceOverlap)(split_times=[672]). 1 Input Files, 3.36mb total:"
    - "L0, all files 3.36mb                                                                                               "
    - "L0.198[671,986] 1.05us   |-----------------------------------------L0.198-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3.36mb total:"
    - "L0                                                                                                                 "
    - "L0.?[671,672] 1.05us 10.92kb|L0.?|                                                                                    "
    - "L0.?[673,986] 1.05us 3.35mb|-----------------------------------------L0.?------------------------------------------| "
    - "Committing partition 1:"
    - "  Soft Deleting 35 files: L0.145, L0.146, L0.148, L0.150, L0.151, L0.153, L0.154, L0.156, L0.157, L0.159, L0.161, L0.162, L0.164, L0.165, L0.167, L0.168, L0.170, L0.172, L0.173, L0.175, L0.176, L0.178, L0.179, L0.181, L0.183, L0.184, L0.186, L0.187, L0.189, L0.190, L0.192, L0.194, L0.195, L0.197, L0.198"
    - "  Creating 70 files"
    - "**** Simulation run 86, type=split(CompactAndSplitOutput(FoundSubsetLessThanMaxCompactSize))(split_times=[294]). 2 Input Files, 102.46mb total:"
    - "L0                                                                                                                 "
    - "L0.144[173,356] 1.03us 2.36mb                                     |----------------------L0.144----------------------| "
    - "L1                                                                                                                 "
    - "L1.199[42,357] 1.03us 100.11mb|-----------------------------------------L1.199-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 102.46mb total:"
    - "L1                                                                                                                 "
    - "L1.?[42,294] 1.03us 81.97mb|---------------------------------L1.?---------------------------------|                  "
    - "L1.?[295,357] 1.03us 20.49mb                                                                        |-----L1.?------| "
    - "Committing partition 1:"
    - "  Soft Deleting 2 files: L0.144, L1.199"
    - "  Creating 2 files"
    - "**** Simulation run 87, type=split(ReduceOverlap)(split_times=[294]). 1 Input Files, 5.28mb total:"
    - "L0, all files 5.28mb                                                                                               "
    - "L0.147[50,356] 1.03us    |-----------------------------------------L0.147-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 5.28mb total:"
    - "L0                                                                                                                 "
    - "L0.?[50,294] 1.03us 4.21mb|--------------------------------L0.?---------------------------------|                   "
    - "L0.?[295,356] 1.03us 1.07mb                                                                        |-----L0.?------| "
    - "**** Simulation run 88, type=split(ReduceOverlap)(split_times=[294]). 1 Input Files, 3.27mb total:"
    - "L0, all files 3.27mb                                                                                               "
    - "L0.149[76,356] 1.03us    |-----------------------------------------L0.149-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3.27mb total:"
    - "L0                                                                                                                 "
    - "L0.?[76,294] 1.03us 2.55mb|--------------------------------L0.?--------------------------------|                    "
    - "L0.?[295,356] 1.03us 741.68kb                                                                      |------L0.?-------| "
    - "**** Simulation run 89, type=split(ReduceOverlap)(split_times=[294]). 1 Input Files, 3.33mb total:"
    - "L0, all files 3.33mb                                                                                               "
    - "L0.152[42,356] 1.03us    |-----------------------------------------L0.152-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3.33mb total:"
    - "L0                                                                                                                 "
    - "L0.?[42,294] 1.03us 2.67mb|---------------------------------L0.?---------------------------------|                  "
    - "L0.?[295,356] 1.03us 672.54kb                                                                        |-----L0.?------| "
    - "**** Simulation run 90, type=split(ReduceOverlap)(split_times=[294]). 1 Input Files, 2.36mb total:"
    - "L0, all files 2.36mb                                                                                               "
    - "L0.155[173,356] 1.03us   |-----------------------------------------L0.155-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 2.36mb total:"
    - "L0                                                                                                                 "
    - "L0.?[173,294] 1.03us 1.56mb|--------------------------L0.?---------------------------|                               "
    - "L0.?[295,356] 1.03us 817.09kb                                                            |------------L0.?------------|"
    - "**** Simulation run 91, type=split(ReduceOverlap)(split_times=[294]). 1 Input Files, 5.28mb total:"
    - "L0, all files 5.28mb                                                                                               "
    - "L0.158[50,356] 1.03us    |-----------------------------------------L0.158-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 5.28mb total:"
    - "L0                                                                                                                 "
    - "L0.?[50,294] 1.03us 4.21mb|--------------------------------L0.?---------------------------------|                   "
    - "L0.?[295,356] 1.03us 1.07mb                                                                        |-----L0.?------| "
    - "**** Simulation run 92, type=split(ReduceOverlap)(split_times=[294]). 1 Input Files, 3.27mb total:"
    - "L0, all files 3.27mb                                                                                               "
    - "L0.160[76,356] 1.04us    |-----------------------------------------L0.160-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3.27mb total:"
    - "L0                                                                                                                 "
    - "L0.?[76,294] 1.04us 2.55mb|--------------------------------L0.?--------------------------------|                    "
    - "L0.?[295,356] 1.04us 741.68kb                                                                      |------L0.?-------| "
    - "**** Simulation run 93, type=split(ReduceOverlap)(split_times=[294]). 1 Input Files, 3.33mb total:"
    - "L0, all files 3.33mb                                                                                               "
    - "L0.163[42,356] 1.04us    |-----------------------------------------L0.163-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3.33mb total:"
    - "L0                                                                                                                 "
    - "L0.?[42,294] 1.04us 2.67mb|---------------------------------L0.?---------------------------------|                  "
    - "L0.?[295,356] 1.04us 672.54kb                                                                        |-----L0.?------| "
    - "**** Simulation run 94, type=split(ReduceOverlap)(split_times=[294]). 1 Input Files, 2.36mb total:"
    - "L0, all files 2.36mb                                                                                               "
    - "L0.166[173,356] 1.04us   |-----------------------------------------L0.166-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 2.36mb total:"
    - "L0                                                                                                                 "
    - "L0.?[173,294] 1.04us 1.56mb|--------------------------L0.?---------------------------|                               "
    - "L0.?[295,356] 1.04us 817.09kb                                                            |------------L0.?------------|"
    - "**** Simulation run 95, type=split(ReduceOverlap)(split_times=[294]). 1 Input Files, 5.28mb total:"
    - "L0, all files 5.28mb                                                                                               "
    - "L0.169[50,356] 1.04us    |-----------------------------------------L0.169-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 5.28mb total:"
    - "L0                                                                                                                 "
    - "L0.?[50,294] 1.04us 4.21mb|--------------------------------L0.?---------------------------------|                   "
    - "L0.?[295,356] 1.04us 1.07mb                                                                        |-----L0.?------| "
    - "**** Simulation run 96, type=split(ReduceOverlap)(split_times=[294]). 1 Input Files, 3.27mb total:"
    - "L0, all files 3.27mb                                                                                               "
    - "L0.171[76,356] 1.04us    |-----------------------------------------L0.171-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3.27mb total:"
    - "L0                                                                                                                 "
    - "L0.?[76,294] 1.04us 2.55mb|--------------------------------L0.?--------------------------------|                    "
    - "L0.?[295,356] 1.04us 741.68kb                                                                      |------L0.?-------| "
    - "**** Simulation run 97, type=split(ReduceOverlap)(split_times=[294]). 1 Input Files, 3.33mb total:"
    - "L0, all files 3.33mb                                                                                               "
    - "L0.174[42,356] 1.04us    |-----------------------------------------L0.174-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3.33mb total:"
    - "L0                                                                                                                 "
    - "L0.?[42,294] 1.04us 2.67mb|---------------------------------L0.?---------------------------------|                  "
    - "L0.?[295,356] 1.04us 672.54kb                                                                        |-----L0.?------| "
    - "**** Simulation run 98, type=split(ReduceOverlap)(split_times=[294]). 1 Input Files, 2.36mb total:"
    - "L0, all files 2.36mb                                                                                               "
    - "L0.177[173,356] 1.04us   |-----------------------------------------L0.177-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 2.36mb total:"
    - "L0                                                                                                                 "
    - "L0.?[173,294] 1.04us 1.56mb|--------------------------L0.?---------------------------|                               "
    - "L0.?[295,356] 1.04us 817.09kb                                                            |------------L0.?------------|"
    - "**** Simulation run 99, type=split(ReduceOverlap)(split_times=[294]). 1 Input Files, 5.28mb total:"
    - "L0, all files 5.28mb                                                                                               "
    - "L0.180[50,356] 1.04us    |-----------------------------------------L0.180-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 5.28mb total:"
    - "L0                                                                                                                 "
    - "L0.?[50,294] 1.04us 4.21mb|--------------------------------L0.?---------------------------------|                   "
    - "L0.?[295,356] 1.04us 1.07mb                                                                        |-----L0.?------| "
    - "**** Simulation run 100, type=split(ReduceOverlap)(split_times=[294]). 1 Input Files, 3.27mb total:"
    - "L0, all files 3.27mb                                                                                               "
    - "L0.182[76,356] 1.04us    |-----------------------------------------L0.182-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3.27mb total:"
    - "L0                                                                                                                 "
    - "L0.?[76,294] 1.04us 2.55mb|--------------------------------L0.?--------------------------------|                    "
    - "L0.?[295,356] 1.04us 741.68kb                                                                      |------L0.?-------| "
    - "**** Simulation run 101, type=split(ReduceOverlap)(split_times=[294]). 1 Input Files, 3.33mb total:"
    - "L0, all files 3.33mb                                                                                               "
    - "L0.185[42,356] 1.05us    |-----------------------------------------L0.185-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3.33mb total:"
    - "L0                                                                                                                 "
    - "L0.?[42,294] 1.05us 2.67mb|---------------------------------L0.?---------------------------------|                  "
    - "L0.?[295,356] 1.05us 672.54kb                                                                        |-----L0.?------| "
    - "**** Simulation run 102, type=split(ReduceOverlap)(split_times=[294]). 1 Input Files, 2.36mb total:"
    - "L0, all files 2.36mb                                                                                               "
    - "L0.188[173,356] 1.05us   |-----------------------------------------L0.188-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 2.36mb total:"
    - "L0                                                                                                                 "
    - "L0.?[173,294] 1.05us 1.56mb|--------------------------L0.?---------------------------|                               "
    - "L0.?[295,356] 1.05us 817.09kb                                                            |------------L0.?------------|"
    - "**** Simulation run 103, type=split(ReduceOverlap)(split_times=[294]). 1 Input Files, 5.28mb total:"
    - "L0, all files 5.28mb                                                                                               "
    - "L0.191[50,356] 1.05us    |-----------------------------------------L0.191-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 5.28mb total:"
    - "L0                                                                                                                 "
    - "L0.?[50,294] 1.05us 4.21mb|--------------------------------L0.?---------------------------------|                   "
    - "L0.?[295,356] 1.05us 1.07mb                                                                        |-----L0.?------| "
    - "**** Simulation run 104, type=split(ReduceOverlap)(split_times=[294]). 1 Input Files, 3.27mb total:"
    - "L0, all files 3.27mb                                                                                               "
    - "L0.193[76,356] 1.05us    |-----------------------------------------L0.193-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3.27mb total:"
    - "L0                                                                                                                 "
    - "L0.?[76,294] 1.05us 2.55mb|--------------------------------L0.?--------------------------------|                    "
    - "L0.?[295,356] 1.05us 741.68kb                                                                      |------L0.?-------| "
    - "**** Simulation run 105, type=split(ReduceOverlap)(split_times=[294]). 1 Input Files, 3.33mb total:"
    - "L0, all files 3.33mb                                                                                               "
    - "L0.196[42,356] 1.05us    |-----------------------------------------L0.196-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3.33mb total:"
    - "L0                                                                                                                 "
    - "L0.?[42,294] 1.05us 2.67mb|---------------------------------L0.?---------------------------------|                  "
    - "L0.?[295,356] 1.05us 672.54kb                                                                        |-----L0.?------| "
    - "Committing partition 1:"
    - "  Soft Deleting 19 files: L0.147, L0.149, L0.152, L0.155, L0.158, L0.160, L0.163, L0.166, L0.169, L0.171, L0.174, L0.177, L0.180, L0.182, L0.185, L0.188, L0.191, L0.193, L0.196"
    - "  Creating 38 files"
    - "**** Simulation run 106, type=split(CompactAndSplitOutput(FoundSubsetLessThanMaxCompactSize))(split_times=[591, 887]). 10 Input Files, 233.82mb total:"
    - "L0                                                                                                                 "
    - "L0.205[673,950] 1.03us 3.6mb                                                 |--------------L0.205--------------|     "
    - "L0.204[671,672] 1.03us 13.27kb                                                |L0.204|                                  "
    - "L0.203[358,670] 1.03us 4.03mb        |----------------L0.203----------------|                                          "
    - "L0.202[357,357] 1.03us 0b        |L0.202|                                                                          "
    - "L0.207[358,629] 1.03us 4.72mb        |-------------L0.207--------------|                                               "
    - "L0.206[357,357] 1.03us 0b        |L0.206|                                                                          "
    - "L0.275[295,356] 1.03us 1.07mb|L0.275|                                                                                  "
    - "L1                                                                                                                 "
    - "L1.201[673,986] 1.03us 100.11mb                                                 |----------------L1.201----------------| "
    - "L1.200[358,672] 1.03us 99.79mb        |----------------L1.200----------------|                                          "
    - "L1.273[295,357] 1.03us 20.49mb|L1.273|                                                                                  "
    - "**** 3 Output Files (parquet_file_id not yet assigned), 233.82mb total:"
    - "L1                                                                                                                 "
    - "L1.?[295,591] 1.03us 100.16mb|----------------L1.?----------------|                                                    "
    - "L1.?[592,887] 1.03us 99.82mb                                      |----------------L1.?----------------|              "
    - "L1.?[888,986] 1.03us 33.84mb                                                                             |---L1.?---| "
    - "Committing partition 1:"
    - "  Soft Deleting 10 files: L1.200, L1.201, L0.202, L0.203, L0.204, L0.205, L0.206, L0.207, L1.273, L0.275"
    - "  Creating 3 files"
    - "**** Simulation run 107, type=split(ReduceOverlap)(split_times=[591]). 1 Input Files, 3.66mb total:"
    - "L0, all files 3.66mb                                                                                               "
    - "L0.209[358,670] 1.03us   |-----------------------------------------L0.209-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3.66mb total:"
    - "L0                                                                                                                 "
    - "L0.?[358,591] 1.03us 2.73mb|------------------------------L0.?-------------------------------|                       "
    - "L0.?[592,670] 1.03us 948.08kb                                                                   |--------L0.?--------| "
    - "**** Simulation run 108, type=split(ReduceOverlap)(split_times=[887]). 1 Input Files, 3.06mb total:"
    - "L0, all files 3.06mb                                                                                               "
    - "L0.211[673,932] 1.03us   |-----------------------------------------L0.211-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3.06mb total:"
    - "L0                                                                                                                 "
    - "L0.?[673,887] 1.03us 2.53mb|----------------------------------L0.?----------------------------------|                "
    - "L0.?[888,932] 1.03us 544.54kb                                                                          |----L0.?-----| "
    - "**** Simulation run 109, type=split(ReduceOverlap)(split_times=[591]). 1 Input Files, 3.32mb total:"
    - "L0, all files 3.32mb                                                                                               "
    - "L0.213[358,670] 1.03us   |-----------------------------------------L0.213-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3.32mb total:"
    - "L0                                                                                                                 "
    - "L0.?[358,591] 1.03us 2.48mb|------------------------------L0.?-------------------------------|                       "
    - "L0.?[592,670] 1.03us 859.7kb                                                                   |--------L0.?--------| "
    - "**** Simulation run 110, type=split(ReduceOverlap)(split_times=[887]). 1 Input Files, 3.35mb total:"
    - "L0, all files 3.35mb                                                                                               "
    - "L0.215[673,986] 1.03us   |-----------------------------------------L0.215-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3.35mb total:"
    - "L0                                                                                                                 "
    - "L0.?[673,887] 1.03us 2.29mb|---------------------------L0.?----------------------------|                             "
    - "L0.?[888,986] 1.03us 1.06mb                                                             |-----------L0.?-----------| "
    - "**** Simulation run 111, type=split(ReduceOverlap)(split_times=[591]). 1 Input Files, 4.03mb total:"
    - "L0, all files 4.03mb                                                                                               "
    - "L0.217[358,670] 1.03us   |-----------------------------------------L0.217-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 4.03mb total:"
    - "L0                                                                                                                 "
    - "L0.?[358,591] 1.03us 3.01mb|------------------------------L0.?-------------------------------|                       "
    - "L0.?[592,670] 1.03us 1.02mb                                                                   |--------L0.?--------| "
    - "**** Simulation run 112, type=split(ReduceOverlap)(split_times=[887]). 1 Input Files, 3.6mb total:"
    - "L0, all files 3.6mb                                                                                                "
    - "L0.219[673,950] 1.03us   |-----------------------------------------L0.219-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3.6mb total:"
    - "L0                                                                                                                 "
    - "L0.?[673,887] 1.03us 2.78mb|-------------------------------L0.?--------------------------------|                     "
    - "L0.?[888,950] 1.03us 839.24kb                                                                     |-------L0.?-------| "
    - "**** Simulation run 113, type=split(ReduceOverlap)(split_times=[591]). 1 Input Files, 4.72mb total:"
    - "L0, all files 4.72mb                                                                                               "
    - "L0.221[358,629] 1.03us   |-----------------------------------------L0.221-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 4.72mb total:"
    - "L0                                                                                                                 "
    - "L0.?[358,591] 1.03us 4.05mb|-----------------------------------L0.?------------------------------------|             "
    - "L0.?[592,629] 1.03us 677.02kb                                                                             |---L0.?---| "
    - "**** Simulation run 114, type=split(ReduceOverlap)(split_times=[591]). 1 Input Files, 3.66mb total:"
    - "L0, all files 3.66mb                                                                                               "
    - "L0.223[358,670] 1.04us   |-----------------------------------------L0.223-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3.66mb total:"
    - "L0                                                                                                                 "
    - "L0.?[358,591] 1.04us 2.73mb|------------------------------L0.?-------------------------------|                       "
    - "L0.?[592,670] 1.04us 948.08kb                                                                   |--------L0.?--------| "
    - "**** Simulation run 115, type=split(ReduceOverlap)(split_times=[887]). 1 Input Files, 3.06mb total:"
    - "L0, all files 3.06mb                                                                                               "
    - "L0.225[673,932] 1.04us   |-----------------------------------------L0.225-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3.06mb total:"
    - "L0                                                                                                                 "
    - "L0.?[673,887] 1.04us 2.53mb|----------------------------------L0.?----------------------------------|                "
    - "L0.?[888,932] 1.04us 544.54kb                                                                          |----L0.?-----| "
    - "**** Simulation run 116, type=split(ReduceOverlap)(split_times=[591]). 1 Input Files, 3.32mb total:"
    - "L0, all files 3.32mb                                                                                               "
    - "L0.227[358,670] 1.04us   |-----------------------------------------L0.227-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3.32mb total:"
    - "L0                                                                                                                 "
    - "L0.?[358,591] 1.04us 2.48mb|------------------------------L0.?-------------------------------|                       "
    - "L0.?[592,670] 1.04us 859.7kb                                                                   |--------L0.?--------| "
    - "**** Simulation run 117, type=split(ReduceOverlap)(split_times=[887]). 1 Input Files, 3.35mb total:"
    - "L0, all files 3.35mb                                                                                               "
    - "L0.229[673,986] 1.04us   |-----------------------------------------L0.229-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3.35mb total:"
    - "L0                                                                                                                 "
    - "L0.?[673,887] 1.04us 2.29mb|---------------------------L0.?----------------------------|                             "
    - "L0.?[888,986] 1.04us 1.06mb                                                             |-----------L0.?-----------| "
    - "**** Simulation run 118, type=split(ReduceOverlap)(split_times=[591]). 1 Input Files, 4.03mb total:"
    - "L0, all files 4.03mb                                                                                               "
    - "L0.231[358,670] 1.04us   |-----------------------------------------L0.231-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 4.03mb total:"
    - "L0                                                                                                                 "
    - "L0.?[358,591] 1.04us 3.01mb|------------------------------L0.?-------------------------------|                       "
    - "L0.?[592,670] 1.04us 1.02mb                                                                   |--------L0.?--------| "
    - "**** Simulation run 119, type=split(ReduceOverlap)(split_times=[887]). 1 Input Files, 3.6mb total:"
    - "L0, all files 3.6mb                                                                                                "
    - "L0.233[673,950] 1.04us   |-----------------------------------------L0.233-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3.6mb total:"
    - "L0                                                                                                                 "
    - "L0.?[673,887] 1.04us 2.78mb|-------------------------------L0.?--------------------------------|                     "
    - "L0.?[888,950] 1.04us 839.24kb                                                                     |-------L0.?-------| "
    - "**** Simulation run 120, type=split(ReduceOverlap)(split_times=[591]). 1 Input Files, 4.72mb total:"
    - "L0, all files 4.72mb                                                                                               "
    - "L0.235[358,629] 1.04us   |-----------------------------------------L0.235-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 4.72mb total:"
    - "L0                                                                                                                 "
    - "L0.?[358,591] 1.04us 4.05mb|-----------------------------------L0.?------------------------------------|             "
    - "L0.?[592,629] 1.04us 677.02kb                                                                             |---L0.?---| "
    - "**** Simulation run 121, type=split(ReduceOverlap)(split_times=[591]). 1 Input Files, 3.66mb total:"
    - "L0, all files 3.66mb                                                                                               "
    - "L0.237[358,670] 1.04us   |-----------------------------------------L0.237-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3.66mb total:"
    - "L0                                                                                                                 "
    - "L0.?[358,591] 1.04us 2.73mb|------------------------------L0.?-------------------------------|                       "
    - "L0.?[592,670] 1.04us 948.08kb                                                                   |--------L0.?--------| "
    - "**** Simulation run 122, type=split(ReduceOverlap)(split_times=[887]). 1 Input Files, 3.06mb total:"
    - "L0, all files 3.06mb                                                                                               "
    - "L0.239[673,932] 1.04us   |-----------------------------------------L0.239-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3.06mb total:"
    - "L0                                                                                                                 "
    - "L0.?[673,887] 1.04us 2.53mb|----------------------------------L0.?----------------------------------|                "
    - "L0.?[888,932] 1.04us 544.54kb                                                                          |----L0.?-----| "
    - "**** Simulation run 123, type=split(ReduceOverlap)(split_times=[591]). 1 Input Files, 3.32mb total:"
    - "L0, all files 3.32mb                                                                                               "
    - "L0.241[358,670] 1.04us   |-----------------------------------------L0.241-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3.32mb total:"
    - "L0                                                                                                                 "
    - "L0.?[358,591] 1.04us 2.48mb|------------------------------L0.?-------------------------------|                       "
    - "L0.?[592,670] 1.04us 859.7kb                                                                   |--------L0.?--------| "
    - "**** Simulation run 124, type=split(ReduceOverlap)(split_times=[887]). 1 Input Files, 3.35mb total:"
    - "L0, all files 3.35mb                                                                                               "
    - "L0.243[673,986] 1.04us   |-----------------------------------------L0.243-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3.35mb total:"
    - "L0                                                                                                                 "
    - "L0.?[673,887] 1.04us 2.29mb|---------------------------L0.?----------------------------|                             "
    - "L0.?[888,986] 1.04us 1.06mb                                                             |-----------L0.?-----------| "
    - "**** Simulation run 125, type=split(ReduceOverlap)(split_times=[591]). 1 Input Files, 4.03mb total:"
    - "L0, all files 4.03mb                                                                                               "
    - "L0.245[358,670] 1.04us   |-----------------------------------------L0.245-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 4.03mb total:"
    - "L0                                                                                                                 "
    - "L0.?[358,591] 1.04us 3.01mb|------------------------------L0.?-------------------------------|                       "
    - "L0.?[592,670] 1.04us 1.02mb                                                                   |--------L0.?--------| "
    - "**** Simulation run 126, type=split(ReduceOverlap)(split_times=[887]). 1 Input Files, 3.6mb total:"
    - "L0, all files 3.6mb                                                                                                "
    - "L0.247[673,950] 1.04us   |-----------------------------------------L0.247-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3.6mb total:"
    - "L0                                                                                                                 "
    - "L0.?[673,887] 1.04us 2.78mb|-------------------------------L0.?--------------------------------|                     "
    - "L0.?[888,950] 1.04us 839.24kb                                                                     |-------L0.?-------| "
    - "**** Simulation run 127, type=split(ReduceOverlap)(split_times=[591]). 1 Input Files, 4.72mb total:"
    - "L0, all files 4.72mb                                                                                               "
    - "L0.249[358,629] 1.04us   |-----------------------------------------L0.249-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 4.72mb total:"
    - "L0                                                                                                                 "
    - "L0.?[358,591] 1.04us 4.05mb|-----------------------------------L0.?------------------------------------|             "
    - "L0.?[592,629] 1.04us 677.02kb                                                                             |---L0.?---| "
    - "**** Simulation run 128, type=split(ReduceOverlap)(split_times=[591]). 1 Input Files, 3.66mb total:"
    - "L0, all files 3.66mb                                                                                               "
    - "L0.251[358,670] 1.04us   |-----------------------------------------L0.251-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3.66mb total:"
    - "L0                                                                                                                 "
    - "L0.?[358,591] 1.04us 2.73mb|------------------------------L0.?-------------------------------|                       "
    - "L0.?[592,670] 1.04us 948.08kb                                                                   |--------L0.?--------| "
    - "**** Simulation run 129, type=split(ReduceOverlap)(split_times=[887]). 1 Input Files, 3.06mb total:"
    - "L0, all files 3.06mb                                                                                               "
    - "L0.253[673,932] 1.04us   |-----------------------------------------L0.253-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3.06mb total:"
    - "L0                                                                                                                 "
    - "L0.?[673,887] 1.04us 2.53mb|----------------------------------L0.?----------------------------------|                "
    - "L0.?[888,932] 1.04us 544.54kb                                                                          |----L0.?-----| "
    - "**** Simulation run 130, type=split(ReduceOverlap)(split_times=[591]). 1 Input Files, 3.32mb total:"
    - "L0, all files 3.32mb                                                                                               "
    - "L0.255[358,670] 1.05us   |-----------------------------------------L0.255-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3.32mb total:"
    - "L0                                                                                                                 "
    - "L0.?[358,591] 1.05us 2.48mb|------------------------------L0.?-------------------------------|                       "
    - "L0.?[592,670] 1.05us 859.7kb                                                                   |--------L0.?--------| "
    - "**** Simulation run 131, type=split(ReduceOverlap)(split_times=[887]). 1 Input Files, 3.35mb total:"
    - "L0, all files 3.35mb                                                                                               "
    - "L0.257[673,986] 1.05us   |-----------------------------------------L0.257-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3.35mb total:"
    - "L0                                                                                                                 "
    - "L0.?[673,887] 1.05us 2.29mb|---------------------------L0.?----------------------------|                             "
    - "L0.?[888,986] 1.05us 1.06mb                                                             |-----------L0.?-----------| "
    - "**** Simulation run 132, type=split(ReduceOverlap)(split_times=[591]). 1 Input Files, 4.03mb total:"
    - "L0, all files 4.03mb                                                                                               "
    - "L0.259[358,670] 1.05us   |-----------------------------------------L0.259-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 4.03mb total:"
    - "L0                                                                                                                 "
    - "L0.?[358,591] 1.05us 3.01mb|------------------------------L0.?-------------------------------|                       "
    - "L0.?[592,670] 1.05us 1.02mb                                                                   |--------L0.?--------| "
    - "**** Simulation run 133, type=split(ReduceOverlap)(split_times=[887]). 1 Input Files, 3.6mb total:"
    - "L0, all files 3.6mb                                                                                                "
    - "L0.261[673,950] 1.05us   |-----------------------------------------L0.261-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3.6mb total:"
    - "L0                                                                                                                 "
    - "L0.?[673,887] 1.05us 2.78mb|-------------------------------L0.?--------------------------------|                     "
    - "L0.?[888,950] 1.05us 839.24kb                                                                     |-------L0.?-------| "
    - "**** Simulation run 134, type=split(ReduceOverlap)(split_times=[591]). 1 Input Files, 4.72mb total:"
    - "L0, all files 4.72mb                                                                                               "
    - "L0.263[358,629] 1.05us   |-----------------------------------------L0.263-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 4.72mb total:"
    - "L0                                                                                                                 "
    - "L0.?[358,591] 1.05us 4.05mb|-----------------------------------L0.?------------------------------------|             "
    - "L0.?[592,629] 1.05us 677.02kb                                                                             |---L0.?---| "
    - "**** Simulation run 135, type=split(ReduceOverlap)(split_times=[591]). 1 Input Files, 3.66mb total:"
    - "L0, all files 3.66mb                                                                                               "
    - "L0.265[358,670] 1.05us   |-----------------------------------------L0.265-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3.66mb total:"
    - "L0                                                                                                                 "
    - "L0.?[358,591] 1.05us 2.73mb|------------------------------L0.?-------------------------------|                       "
    - "L0.?[592,670] 1.05us 948.08kb                                                                   |--------L0.?--------| "
    - "**** Simulation run 136, type=split(ReduceOverlap)(split_times=[887]). 1 Input Files, 3.06mb total:"
    - "L0, all files 3.06mb                                                                                               "
    - "L0.267[673,932] 1.05us   |-----------------------------------------L0.267-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3.06mb total:"
    - "L0                                                                                                                 "
    - "L0.?[673,887] 1.05us 2.53mb|----------------------------------L0.?----------------------------------|                "
    - "L0.?[888,932] 1.05us 544.54kb                                                                          |----L0.?-----| "
    - "**** Simulation run 137, type=split(ReduceOverlap)(split_times=[591]). 1 Input Files, 3.32mb total:"
    - "L0, all files 3.32mb                                                                                               "
    - "L0.269[358,670] 1.05us   |-----------------------------------------L0.269-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3.32mb total:"
    - "L0                                                                                                                 "
    - "L0.?[358,591] 1.05us 2.48mb|------------------------------L0.?-------------------------------|                       "
    - "L0.?[592,670] 1.05us 859.7kb                                                                   |--------L0.?--------| "
    - "**** Simulation run 138, type=split(ReduceOverlap)(split_times=[887]). 1 Input Files, 3.35mb total:"
    - "L0, all files 3.35mb                                                                                               "
    - "L0.271[673,986] 1.05us   |-----------------------------------------L0.271-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3.35mb total:"
    - "L0                                                                                                                 "
    - "L0.?[673,887] 1.05us 2.29mb|---------------------------L0.?----------------------------|                             "
    - "L0.?[888,986] 1.05us 1.06mb                                                             |-----------L0.?-----------| "
    - "Committing partition 1:"
    - "  Soft Deleting 32 files: L0.209, L0.211, L0.213, L0.215, L0.217, L0.219, L0.221, L0.223, L0.225, L0.227, L0.229, L0.231, L0.233, L0.235, L0.237, L0.239, L0.241, L0.243, L0.245, L0.247, L0.249, L0.251, L0.253, L0.255, L0.257, L0.259, L0.261, L0.263, L0.265, L0.267, L0.269, L0.271"
    - "  Creating 64 files"
    - "**** Simulation run 139, type=split(CompactAndSplitOutput(FoundSubsetLessThanMaxCompactSize))(split_times=[334, 626]). 8 Input Files, 289.45mb total:"
    - "L0                                                                                                                 "
    - "L0.274[50,294] 1.03us 4.21mb|--------L0.274---------|                                                                 "
    - "L0.210[671,672] 1.03us 12.05kb                                                                  |L0.210|                "
    - "L0.208[357,357] 1.03us 0b                                 |L0.208|                                                 "
    - "L0.277[295,356] 1.03us 741.68kb                          |L0.277|                                                        "
    - "L0.276[76,294] 1.03us 2.55mb   |-------L0.276--------|                                                                "
    - "L1                                                                                                                 "
    - "L1.272[42,294] 1.03us 81.97mb|---------L1.272---------|                                                                "
    - "L1.312[295,591] 1.03us 100.16mb                          |-----------L1.312------------|                                 "
    - "L1.313[592,887] 1.03us 99.82mb                                                          |-----------L1.313------------| "
    - "**** 3 Output Files (parquet_file_id not yet assigned), 289.45mb total:"
    - "L1                                                                                                                 "
    - "L1.?[42,334] 1.03us 100.02mb|------------L1.?-------------|                                                           "
    - "L1.?[335,626] 1.03us 99.68mb                               |------------L1.?------------|                             "
    - "L1.?[627,887] 1.03us 89.75mb                                                              |----------L1.?-----------| "
    - "Committing partition 1:"
    - "  Soft Deleting 8 files: L0.208, L0.210, L1.272, L0.274, L0.276, L0.277, L1.312, L1.313"
    - "  Creating 3 files"
    - "**** Simulation run 140, type=split(ReduceOverlap)(split_times=[626]). 1 Input Files, 948.08kb total:"
    - "L0, all files 948.08kb                                                                                             "
    - "L0.316[592,670] 1.03us   |-----------------------------------------L0.316-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 948.08kb total:"
    - "L0                                                                                                                 "
    - "L0.?[592,626] 1.03us 413.26kb|----------------L0.?-----------------|                                                   "
    - "L0.?[627,670] 1.03us 534.81kb                                        |---------------------L0.?----------------------| "
    - "**** Simulation run 141, type=split(ReduceOverlap)(split_times=[626]). 1 Input Files, 859.7kb total:"
    - "L0, all files 859.7kb                                                                                              "
    - "L0.320[592,670] 1.03us   |-----------------------------------------L0.320-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 859.7kb total:"
    - "L0                                                                                                                 "
    - "L0.?[592,626] 1.03us 374.74kb|----------------L0.?-----------------|                                                   "
    - "L0.?[627,670] 1.03us 484.96kb                                        |---------------------L0.?----------------------| "
    - "**** Simulation run 142, type=split(ReduceOverlap)(split_times=[334]). 1 Input Files, 672.54kb total:"
    - "L0, all files 672.54kb                                                                                             "
    - "L0.279[295,356] 1.03us   |-----------------------------------------L0.279-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 672.54kb total:"
    - "L0                                                                                                                 "
    - "L0.?[295,334] 1.03us 429.99kb|-------------------------L0.?--------------------------|                                 "
    - "L0.?[335,356] 1.03us 242.56kb                                                           |------------L0.?------------| "
    - "**** Simulation run 143, type=split(ReduceOverlap)(split_times=[626]). 1 Input Files, 1.02mb total:"
    - "L0, all files 1.02mb                                                                                               "
    - "L0.324[592,670] 1.03us   |-----------------------------------------L0.324-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 1.02mb total:"
    - "L0                                                                                                                 "
    - "L0.?[592,626] 1.03us 455.28kb|----------------L0.?-----------------|                                                   "
    - "L0.?[627,670] 1.03us 589.19kb                                        |---------------------L0.?----------------------| "
    - "**** Simulation run 144, type=split(ReduceOverlap)(split_times=[334]). 1 Input Files, 817.09kb total:"
    - "L0, all files 817.09kb                                                                                             "
    - "L0.281[295,356] 1.03us   |-----------------------------------------L0.281-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 817.09kb total:"
    - "L0                                                                                                                 "
    - "L0.?[295,334] 1.03us 522.4kb|-------------------------L0.?--------------------------|                                 "
    - "L0.?[335,356] 1.03us 294.69kb                                                           |------------L0.?------------| "
    - "**** Simulation run 145, type=split(ReduceOverlap)(split_times=[626]). 1 Input Files, 677.02kb total:"
    - "L0, all files 677.02kb                                                                                             "
    - "L0.328[592,629] 1.03us   |-----------------------------------------L0.328-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 677.02kb total:"
    - "L0                                                                                                                 "
    - "L0.?[592,626] 1.03us 622.12kb|--------------------------------------L0.?--------------------------------------|        "
    - "L0.?[627,629] 1.03us 54.89kb                                                                                     |L0.?|"
    - "**** Simulation run 146, type=split(ReduceOverlap)(split_times=[334]). 1 Input Files, 1.07mb total:"
    - "L0, all files 1.07mb                                                                                               "
    - "L0.283[295,356] 1.03us   |-----------------------------------------L0.283-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 1.07mb total:"
    - "L0                                                                                                                 "
    - "L0.?[295,334] 1.03us 701.05kb|-------------------------L0.?--------------------------|                                 "
    - "L0.?[335,356] 1.03us 395.46kb                                                           |------------L0.?------------| "
    - "**** Simulation run 147, type=split(ReduceOverlap)(split_times=[626]). 1 Input Files, 948.08kb total:"
    - "L0, all files 948.08kb                                                                                             "
    - "L0.330[592,670] 1.04us   |-----------------------------------------L0.330-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 948.08kb total:"
    - "L0                                                                                                                 "
    - "L0.?[592,626] 1.04us 413.26kb|----------------L0.?-----------------|                                                   "
    - "L0.?[627,670] 1.04us 534.81kb                                        |---------------------L0.?----------------------| "
    - "**** Simulation run 148, type=split(ReduceOverlap)(split_times=[334]). 1 Input Files, 741.68kb total:"
    - "L0, all files 741.68kb                                                                                             "
    - "L0.285[295,356] 1.04us   |-----------------------------------------L0.285-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 741.68kb total:"
    - "L0                                                                                                                 "
    - "L0.?[295,334] 1.04us 474.19kb|-------------------------L0.?--------------------------|                                 "
    - "L0.?[335,356] 1.04us 267.49kb                                                           |------------L0.?------------| "
    - "**** Simulation run 149, type=split(ReduceOverlap)(split_times=[626]). 1 Input Files, 859.7kb total:"
    - "L0, all files 859.7kb                                                                                              "
    - "L0.334[592,670] 1.04us   |-----------------------------------------L0.334-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 859.7kb total:"
    - "L0                                                                                                                 "
    - "L0.?[592,626] 1.04us 374.74kb|----------------L0.?-----------------|                                                   "
    - "L0.?[627,670] 1.04us 484.96kb                                        |---------------------L0.?----------------------| "
    - "**** Simulation run 150, type=split(ReduceOverlap)(split_times=[334]). 1 Input Files, 672.54kb total:"
    - "L0, all files 672.54kb                                                                                             "
    - "L0.287[295,356] 1.04us   |-----------------------------------------L0.287-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 672.54kb total:"
    - "L0                                                                                                                 "
    - "L0.?[295,334] 1.04us 429.99kb|-------------------------L0.?--------------------------|                                 "
    - "L0.?[335,356] 1.04us 242.56kb                                                           |------------L0.?------------| "
    - "**** Simulation run 151, type=split(ReduceOverlap)(split_times=[626]). 1 Input Files, 1.02mb total:"
    - "L0, all files 1.02mb                                                                                               "
    - "L0.338[592,670] 1.04us   |-----------------------------------------L0.338-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 1.02mb total:"
    - "L0                                                                                                                 "
    - "L0.?[592,626] 1.04us 455.28kb|----------------L0.?-----------------|                                                   "
    - "L0.?[627,670] 1.04us 589.19kb                                        |---------------------L0.?----------------------| "
    - "**** Simulation run 152, type=split(ReduceOverlap)(split_times=[334]). 1 Input Files, 817.09kb total:"
    - "L0, all files 817.09kb                                                                                             "
    - "L0.289[295,356] 1.04us   |-----------------------------------------L0.289-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 817.09kb total:"
    - "L0                                                                                                                 "
    - "L0.?[295,334] 1.04us 522.4kb|-------------------------L0.?--------------------------|                                 "
    - "L0.?[335,356] 1.04us 294.69kb                                                           |------------L0.?------------| "
    - "**** Simulation run 153, type=split(ReduceOverlap)(split_times=[626]). 1 Input Files, 677.02kb total:"
    - "L0, all files 677.02kb                                                                                             "
    - "L0.342[592,629] 1.04us   |-----------------------------------------L0.342-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 677.02kb total:"
    - "L0                                                                                                                 "
    - "L0.?[592,626] 1.04us 622.12kb|--------------------------------------L0.?--------------------------------------|        "
    - "L0.?[627,629] 1.04us 54.89kb                                                                                     |L0.?|"
    - "**** Simulation run 154, type=split(ReduceOverlap)(split_times=[334]). 1 Input Files, 1.07mb total:"
    - "L0, all files 1.07mb                                                                                               "
    - "L0.291[295,356] 1.04us   |-----------------------------------------L0.291-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 1.07mb total:"
    - "L0                                                                                                                 "
    - "L0.?[295,334] 1.04us 701.05kb|-------------------------L0.?--------------------------|                                 "
    - "L0.?[335,356] 1.04us 395.46kb                                                           |------------L0.?------------| "
    - "**** Simulation run 155, type=split(ReduceOverlap)(split_times=[626]). 1 Input Files, 948.08kb total:"
    - "L0, all files 948.08kb                                                                                             "
    - "L0.344[592,670] 1.04us   |-----------------------------------------L0.344-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 948.08kb total:"
    - "L0                                                                                                                 "
    - "L0.?[592,626] 1.04us 413.26kb|----------------L0.?-----------------|                                                   "
    - "L0.?[627,670] 1.04us 534.81kb                                        |---------------------L0.?----------------------| "
    - "**** Simulation run 156, type=split(ReduceOverlap)(split_times=[334]). 1 Input Files, 741.68kb total:"
    - "L0, all files 741.68kb                                                                                             "
    - "L0.293[295,356] 1.04us   |-----------------------------------------L0.293-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 741.68kb total:"
    - "L0                                                                                                                 "
    - "L0.?[295,334] 1.04us 474.19kb|-------------------------L0.?--------------------------|                                 "
    - "L0.?[335,356] 1.04us 267.49kb                                                           |------------L0.?------------| "
    - "**** Simulation run 157, type=split(ReduceOverlap)(split_times=[626]). 1 Input Files, 859.7kb total:"
    - "L0, all files 859.7kb                                                                                              "
    - "L0.348[592,670] 1.04us   |-----------------------------------------L0.348-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 859.7kb total:"
    - "L0                                                                                                                 "
    - "L0.?[592,626] 1.04us 374.74kb|----------------L0.?-----------------|                                                   "
    - "L0.?[627,670] 1.04us 484.96kb                                        |---------------------L0.?----------------------| "
    - "**** Simulation run 158, type=split(ReduceOverlap)(split_times=[334]). 1 Input Files, 672.54kb total:"
    - "L0, all files 672.54kb                                                                                             "
    - "L0.295[295,356] 1.04us   |-----------------------------------------L0.295-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 672.54kb total:"
    - "L0                                                                                                                 "
    - "L0.?[295,334] 1.04us 429.99kb|-------------------------L0.?--------------------------|                                 "
    - "L0.?[335,356] 1.04us 242.56kb                                                           |------------L0.?------------| "
    - "**** Simulation run 159, type=split(ReduceOverlap)(split_times=[626]). 1 Input Files, 1.02mb total:"
    - "L0, all files 1.02mb                                                                                               "
    - "L0.352[592,670] 1.04us   |-----------------------------------------L0.352-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 1.02mb total:"
    - "L0                                                                                                                 "
    - "L0.?[592,626] 1.04us 455.28kb|----------------L0.?-----------------|                                                   "
    - "L0.?[627,670] 1.04us 589.19kb                                        |---------------------L0.?----------------------| "
    - "**** Simulation run 160, type=split(ReduceOverlap)(split_times=[334]). 1 Input Files, 817.09kb total:"
    - "L0, all files 817.09kb                                                                                             "
    - "L0.297[295,356] 1.04us   |-----------------------------------------L0.297-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 817.09kb total:"
    - "L0                                                                                                                 "
    - "L0.?[295,334] 1.04us 522.4kb|-------------------------L0.?--------------------------|                                 "
    - "L0.?[335,356] 1.04us 294.69kb                                                           |------------L0.?------------| "
    - "**** Simulation run 161, type=split(ReduceOverlap)(split_times=[626]). 1 Input Files, 677.02kb total:"
    - "L0, all files 677.02kb                                                                                             "
    - "L0.356[592,629] 1.04us   |-----------------------------------------L0.356-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 677.02kb total:"
    - "L0                                                                                                                 "
    - "L0.?[592,626] 1.04us 622.12kb|--------------------------------------L0.?--------------------------------------|        "
    - "L0.?[627,629] 1.04us 54.89kb                                                                                     |L0.?|"
    - "**** Simulation run 162, type=split(ReduceOverlap)(split_times=[334]). 1 Input Files, 1.07mb total:"
    - "L0, all files 1.07mb                                                                                               "
    - "L0.299[295,356] 1.04us   |-----------------------------------------L0.299-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 1.07mb total:"
    - "L0                                                                                                                 "
    - "L0.?[295,334] 1.04us 701.05kb|-------------------------L0.?--------------------------|                                 "
    - "L0.?[335,356] 1.04us 395.46kb                                                           |------------L0.?------------| "
    - "**** Simulation run 163, type=split(ReduceOverlap)(split_times=[626]). 1 Input Files, 948.08kb total:"
    - "L0, all files 948.08kb                                                                                             "
    - "L0.358[592,670] 1.04us   |-----------------------------------------L0.358-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 948.08kb total:"
    - "L0                                                                                                                 "
    - "L0.?[592,626] 1.04us 413.26kb|----------------L0.?-----------------|                                                   "
    - "L0.?[627,670] 1.04us 534.81kb                                        |---------------------L0.?----------------------| "
    - "**** Simulation run 164, type=split(ReduceOverlap)(split_times=[334]). 1 Input Files, 741.68kb total:"
    - "L0, all files 741.68kb                                                                                             "
    - "L0.301[295,356] 1.04us   |-----------------------------------------L0.301-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 741.68kb total:"
    - "L0                                                                                                                 "
    - "L0.?[295,334] 1.04us 474.19kb|-------------------------L0.?--------------------------|                                 "
    - "L0.?[335,356] 1.04us 267.49kb                                                           |------------L0.?------------| "
    - "**** Simulation run 165, type=split(ReduceOverlap)(split_times=[626]). 1 Input Files, 859.7kb total:"
    - "L0, all files 859.7kb                                                                                              "
    - "L0.362[592,670] 1.05us   |-----------------------------------------L0.362-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 859.7kb total:"
    - "L0                                                                                                                 "
    - "L0.?[592,626] 1.05us 374.74kb|----------------L0.?-----------------|                                                   "
    - "L0.?[627,670] 1.05us 484.96kb                                        |---------------------L0.?----------------------| "
    - "**** Simulation run 166, type=split(ReduceOverlap)(split_times=[334]). 1 Input Files, 672.54kb total:"
    - "L0, all files 672.54kb                                                                                             "
    - "L0.303[295,356] 1.05us   |-----------------------------------------L0.303-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 672.54kb total:"
    - "L0                                                                                                                 "
    - "L0.?[295,334] 1.05us 429.99kb|-------------------------L0.?--------------------------|                                 "
    - "L0.?[335,356] 1.05us 242.56kb                                                           |------------L0.?------------| "
    - "**** Simulation run 167, type=split(ReduceOverlap)(split_times=[626]). 1 Input Files, 1.02mb total:"
    - "L0, all files 1.02mb                                                                                               "
    - "L0.366[592,670] 1.05us   |-----------------------------------------L0.366-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 1.02mb total:"
    - "L0                                                                                                                 "
    - "L0.?[592,626] 1.05us 455.28kb|----------------L0.?-----------------|                                                   "
    - "L0.?[627,670] 1.05us 589.19kb                                        |---------------------L0.?----------------------| "
    - "**** Simulation run 168, type=split(ReduceOverlap)(split_times=[334]). 1 Input Files, 817.09kb total:"
    - "L0, all files 817.09kb                                                                                             "
    - "L0.305[295,356] 1.05us   |-----------------------------------------L0.305-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 817.09kb total:"
    - "L0                                                                                                                 "
    - "L0.?[295,334] 1.05us 522.4kb|-------------------------L0.?--------------------------|                                 "
    - "L0.?[335,356] 1.05us 294.69kb                                                           |------------L0.?------------| "
    - "**** Simulation run 169, type=split(ReduceOverlap)(split_times=[626]). 1 Input Files, 677.02kb total:"
    - "L0, all files 677.02kb                                                                                             "
    - "L0.370[592,629] 1.05us   |-----------------------------------------L0.370-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 677.02kb total:"
    - "L0                                                                                                                 "
    - "L0.?[592,626] 1.05us 622.12kb|--------------------------------------L0.?--------------------------------------|        "
    - "L0.?[627,629] 1.05us 54.89kb                                                                                     |L0.?|"
    - "**** Simulation run 170, type=split(ReduceOverlap)(split_times=[334]). 1 Input Files, 1.07mb total:"
    - "L0, all files 1.07mb                                                                                               "
    - "L0.307[295,356] 1.05us   |-----------------------------------------L0.307-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 1.07mb total:"
    - "L0                                                                                                                 "
    - "L0.?[295,334] 1.05us 701.05kb|-------------------------L0.?--------------------------|                                 "
    - "L0.?[335,356] 1.05us 395.46kb                                                           |------------L0.?------------| "
    - "**** Simulation run 171, type=split(ReduceOverlap)(split_times=[626]). 1 Input Files, 948.08kb total:"
    - "L0, all files 948.08kb                                                                                             "
    - "L0.372[592,670] 1.05us   |-----------------------------------------L0.372-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 948.08kb total:"
    - "L0                                                                                                                 "
    - "L0.?[592,626] 1.05us 413.26kb|----------------L0.?-----------------|                                                   "
    - "L0.?[627,670] 1.05us 534.81kb                                        |---------------------L0.?----------------------| "
    - "**** Simulation run 172, type=split(ReduceOverlap)(split_times=[334]). 1 Input Files, 741.68kb total:"
    - "L0, all files 741.68kb                                                                                             "
    - "L0.309[295,356] 1.05us   |-----------------------------------------L0.309-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 741.68kb total:"
    - "L0                                                                                                                 "
    - "L0.?[295,334] 1.05us 474.19kb|-------------------------L0.?--------------------------|                                 "
    - "L0.?[335,356] 1.05us 267.49kb                                                           |------------L0.?------------| "
    - "**** Simulation run 173, type=split(ReduceOverlap)(split_times=[626]). 1 Input Files, 859.7kb total:"
    - "L0, all files 859.7kb                                                                                              "
    - "L0.376[592,670] 1.05us   |-----------------------------------------L0.376-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 859.7kb total:"
    - "L0                                                                                                                 "
    - "L0.?[592,626] 1.05us 374.74kb|----------------L0.?-----------------|                                                   "
    - "L0.?[627,670] 1.05us 484.96kb                                        |---------------------L0.?----------------------| "
    - "**** Simulation run 174, type=split(ReduceOverlap)(split_times=[334]). 1 Input Files, 672.54kb total:"
    - "L0, all files 672.54kb                                                                                             "
    - "L0.311[295,356] 1.05us   |-----------------------------------------L0.311-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 672.54kb total:"
    - "L0                                                                                                                 "
    - "L0.?[295,334] 1.05us 429.99kb|-------------------------L0.?--------------------------|                                 "
    - "L0.?[335,356] 1.05us 242.56kb                                                           |------------L0.?------------| "
    - "Committing partition 1:"
    - "  Soft Deleting 35 files: L0.279, L0.281, L0.283, L0.285, L0.287, L0.289, L0.291, L0.293, L0.295, L0.297, L0.299, L0.301, L0.303, L0.305, L0.307, L0.309, L0.311, L0.316, L0.320, L0.324, L0.328, L0.330, L0.334, L0.338, L0.342, L0.344, L0.348, L0.352, L0.356, L0.358, L0.362, L0.366, L0.370, L0.372, L0.376"
    - "  Creating 70 files"
    - "**** Simulation run 175, type=split(CompactAndSplitOutput(FoundSubsetLessThanMaxCompactSize))(split_times=[619, 903]). 10 Input Files, 229.99mb total:"
    - "L0                                                                                                                 "
    - "L0.318[888,932] 1.03us 544.54kb                                                                            |L0.318|      "
    - "L0.317[673,887] 1.03us 2.53mb                                              |----------L0.317-----------|               "
    - "L0.315[358,591] 1.03us 2.73mb   |------------L0.315------------|                                                       "
    - "L0.383[627,670] 1.03us 534.81kb                                        |L0.383|                                          "
    - "L0.382[592,626] 1.03us 413.26kb                                   |L0.382|                                               "
    - "L0.214[671,672] 1.03us 10.92kb                                              |L0.214|                                    "
    - "L0.212[357,357] 1.03us 0b   |L0.212|                                                                               "
    - "L1                                                                                                                 "
    - "L1.314[888,986] 1.03us 33.84mb                                                                            |--L1.314---| "
    - "L1.381[627,887] 1.03us 89.75mb                                        |-------------L1.381--------------|               "
    - "L1.380[335,626] 1.03us 99.68mb|----------------L1.380----------------|                                                  "
    - "**** 3 Output Files (parquet_file_id not yet assigned), 229.99mb total:"
    - "L1                                                                                                                 "
    - "L1.?[335,619] 1.03us 100.33mb|----------------L1.?-----------------|                                                   "
    - "L1.?[620,903] 1.03us 99.98mb                                       |----------------L1.?-----------------|            "
    - "L1.?[904,986] 1.03us 29.68mb                                                                              |--L1.?---| "
    - "Committing partition 1:"
    - "  Soft Deleting 10 files: L0.212, L0.214, L1.314, L0.315, L0.317, L0.318, L1.380, L1.381, L0.382, L0.383"
    - "  Creating 3 files"
    - "**** Simulation run 176, type=split(ReduceOverlap)(split_times=[619]). 1 Input Files, 374.74kb total:"
    - "L0, all files 374.74kb                                                                                             "
    - "L0.384[592,626] 1.03us   |-----------------------------------------L0.384-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 374.74kb total:"
    - "L0                                                                                                                 "
    - "L0.?[592,619] 1.03us 297.59kb|--------------------------------L0.?---------------------------------|                   "
    - "L0.?[620,626] 1.03us 77.15kb                                                                          |----L0.?-----| "
    - "**** Simulation run 177, type=split(ReduceOverlap)(split_times=[903]). 1 Input Files, 1.06mb total:"
    - "L0, all files 1.06mb                                                                                               "
    - "L0.322[888,986] 1.03us   |-----------------------------------------L0.322-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 1.06mb total:"
    - "L0                                                                                                                 "
    - "L0.?[888,903] 1.03us 165.94kb|---L0.?----|                                                                             "
    - "L0.?[904,986] 1.03us 918.23kb              |----------------------------------L0.?-----------------------------------| "
    - "**** Simulation run 178, type=split(ReduceOverlap)(split_times=[619]). 1 Input Files, 455.28kb total:"
    - "L0, all files 455.28kb                                                                                             "
    - "L0.388[592,626] 1.03us   |-----------------------------------------L0.388-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 455.28kb total:"
    - "L0                                                                                                                 "
    - "L0.?[592,619] 1.03us 361.55kb|--------------------------------L0.?---------------------------------|                   "
    - "L0.?[620,626] 1.03us 93.73kb                                                                          |----L0.?-----| "
    - "**** Simulation run 179, type=split(ReduceOverlap)(split_times=[903]). 1 Input Files, 839.24kb total:"
    - "L0, all files 839.24kb                                                                                             "
    - "L0.326[888,950] 1.03us   |-----------------------------------------L0.326-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 839.24kb total:"
    - "L0                                                                                                                 "
    - "L0.?[888,903] 1.03us 203.04kb|-------L0.?--------|                                                                     "
    - "L0.?[904,950] 1.03us 636.2kb                       |------------------------------L0.?------------------------------| "
    - "**** Simulation run 180, type=split(ReduceOverlap)(split_times=[619]). 1 Input Files, 622.12kb total:"
    - "L0, all files 622.12kb                                                                                             "
    - "L0.392[592,626] 1.03us   |-----------------------------------------L0.392-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 622.12kb total:"
    - "L0                                                                                                                 "
    - "L0.?[592,619] 1.03us 494.04kb|--------------------------------L0.?---------------------------------|                   "
    - "L0.?[620,626] 1.03us 128.08kb                                                                          |----L0.?-----| "
    - "**** Simulation run 181, type=split(ReduceOverlap)(split_times=[619]). 1 Input Files, 413.26kb total:"
    - "L0, all files 413.26kb                                                                                             "
    - "L0.396[592,626] 1.04us   |-----------------------------------------L0.396-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 413.26kb total:"
    - "L0                                                                                                                 "
    - "L0.?[592,619] 1.04us 328.18kb|--------------------------------L0.?---------------------------------|                   "
    - "L0.?[620,626] 1.04us 85.08kb                                                                          |----L0.?-----| "
    - "**** Simulation run 182, type=split(ReduceOverlap)(split_times=[903]). 1 Input Files, 544.54kb total:"
    - "L0, all files 544.54kb                                                                                             "
    - "L0.332[888,932] 1.04us   |-----------------------------------------L0.332-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 544.54kb total:"
    - "L0                                                                                                                 "
    - "L0.?[888,903] 1.04us 185.64kb|------------L0.?------------|                                                            "
    - "L0.?[904,932] 1.04us 358.9kb                                |-------------------------L0.?--------------------------| "
    - "**** Simulation run 183, type=split(ReduceOverlap)(split_times=[619]). 1 Input Files, 374.74kb total:"
    - "L0, all files 374.74kb                                                                                             "
    - "L0.400[592,626] 1.04us   |-----------------------------------------L0.400-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 374.74kb total:"
    - "L0                                                                                                                 "
    - "L0.?[592,619] 1.04us 297.59kb|--------------------------------L0.?---------------------------------|                   "
    - "L0.?[620,626] 1.04us 77.15kb                                                                          |----L0.?-----| "
    - "**** Simulation run 184, type=split(ReduceOverlap)(split_times=[903]). 1 Input Files, 1.06mb total:"
    - "L0, all files 1.06mb                                                                                               "
    - "L0.336[888,986] 1.04us   |-----------------------------------------L0.336-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 1.06mb total:"
    - "L0                                                                                                                 "
    - "L0.?[888,903] 1.04us 165.94kb|---L0.?----|                                                                             "
    - "L0.?[904,986] 1.04us 918.23kb              |----------------------------------L0.?-----------------------------------| "
    - "**** Simulation run 185, type=split(ReduceOverlap)(split_times=[619]). 1 Input Files, 455.28kb total:"
    - "L0, all files 455.28kb                                                                                             "
    - "L0.404[592,626] 1.04us   |-----------------------------------------L0.404-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 455.28kb total:"
    - "L0                                                                                                                 "
    - "L0.?[592,619] 1.04us 361.55kb|--------------------------------L0.?---------------------------------|                   "
    - "L0.?[620,626] 1.04us 93.73kb                                                                          |----L0.?-----| "
    - "**** Simulation run 186, type=split(ReduceOverlap)(split_times=[903]). 1 Input Files, 839.24kb total:"
    - "L0, all files 839.24kb                                                                                             "
    - "L0.340[888,950] 1.04us   |-----------------------------------------L0.340-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 839.24kb total:"
    - "L0                                                                                                                 "
    - "L0.?[888,903] 1.04us 203.04kb|-------L0.?--------|                                                                     "
    - "L0.?[904,950] 1.04us 636.2kb                       |------------------------------L0.?------------------------------| "
    - "**** Simulation run 187, type=split(ReduceOverlap)(split_times=[619]). 1 Input Files, 622.12kb total:"
    - "L0, all files 622.12kb                                                                                             "
    - "L0.408[592,626] 1.04us   |-----------------------------------------L0.408-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 622.12kb total:"
    - "L0                                                                                                                 "
    - "L0.?[592,619] 1.04us 494.04kb|--------------------------------L0.?---------------------------------|                   "
    - "L0.?[620,626] 1.04us 128.08kb                                                                          |----L0.?-----| "
    - "**** Simulation run 188, type=split(ReduceOverlap)(split_times=[619]). 1 Input Files, 413.26kb total:"
    - "L0, all files 413.26kb                                                                                             "
    - "L0.412[592,626] 1.04us   |-----------------------------------------L0.412-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 413.26kb total:"
    - "L0                                                                                                                 "
    - "L0.?[592,619] 1.04us 328.18kb|--------------------------------L0.?---------------------------------|                   "
    - "L0.?[620,626] 1.04us 85.08kb                                                                          |----L0.?-----| "
    - "**** Simulation run 189, type=split(ReduceOverlap)(split_times=[903]). 1 Input Files, 544.54kb total:"
    - "L0, all files 544.54kb                                                                                             "
    - "L0.346[888,932] 1.04us   |-----------------------------------------L0.346-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 544.54kb total:"
    - "L0                                                                                                                 "
    - "L0.?[888,903] 1.04us 185.64kb|------------L0.?------------|                                                            "
    - "L0.?[904,932] 1.04us 358.9kb                                |-------------------------L0.?--------------------------| "
    - "**** Simulation run 190, type=split(ReduceOverlap)(split_times=[619]). 1 Input Files, 374.74kb total:"
    - "L0, all files 374.74kb                                                                                             "
    - "L0.416[592,626] 1.04us   |-----------------------------------------L0.416-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 374.74kb total:"
    - "L0                                                                                                                 "
    - "L0.?[592,619] 1.04us 297.59kb|--------------------------------L0.?---------------------------------|                   "
    - "L0.?[620,626] 1.04us 77.15kb                                                                          |----L0.?-----| "
    - "**** Simulation run 191, type=split(ReduceOverlap)(split_times=[903]). 1 Input Files, 1.06mb total:"
    - "L0, all files 1.06mb                                                                                               "
    - "L0.350[888,986] 1.04us   |-----------------------------------------L0.350-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 1.06mb total:"
    - "L0                                                                                                                 "
    - "L0.?[888,903] 1.04us 165.94kb|---L0.?----|                                                                             "
    - "L0.?[904,986] 1.04us 918.23kb              |----------------------------------L0.?-----------------------------------| "
    - "**** Simulation run 192, type=split(ReduceOverlap)(split_times=[619]). 1 Input Files, 455.28kb total:"
    - "L0, all files 455.28kb                                                                                             "
    - "L0.420[592,626] 1.04us   |-----------------------------------------L0.420-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 455.28kb total:"
    - "L0                                                                                                                 "
    - "L0.?[592,619] 1.04us 361.55kb|--------------------------------L0.?---------------------------------|                   "
    - "L0.?[620,626] 1.04us 93.73kb                                                                          |----L0.?-----| "
    - "**** Simulation run 193, type=split(ReduceOverlap)(split_times=[903]). 1 Input Files, 839.24kb total:"
    - "L0, all files 839.24kb                                                                                             "
    - "L0.354[888,950] 1.04us   |-----------------------------------------L0.354-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 839.24kb total:"
    - "L0                                                                                                                 "
    - "L0.?[888,903] 1.04us 203.04kb|-------L0.?--------|                                                                     "
    - "L0.?[904,950] 1.04us 636.2kb                       |------------------------------L0.?------------------------------| "
    - "**** Simulation run 194, type=split(ReduceOverlap)(split_times=[619]). 1 Input Files, 622.12kb total:"
    - "L0, all files 622.12kb                                                                                             "
    - "L0.424[592,626] 1.04us   |-----------------------------------------L0.424-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 622.12kb total:"
    - "L0                                                                                                                 "
    - "L0.?[592,619] 1.04us 494.04kb|--------------------------------L0.?---------------------------------|                   "
    - "L0.?[620,626] 1.04us 128.08kb                                                                          |----L0.?-----| "
    - "**** Simulation run 195, type=split(ReduceOverlap)(split_times=[619]). 1 Input Files, 413.26kb total:"
    - "L0, all files 413.26kb                                                                                             "
    - "L0.428[592,626] 1.04us   |-----------------------------------------L0.428-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 413.26kb total:"
    - "L0                                                                                                                 "
    - "L0.?[592,619] 1.04us 328.18kb|--------------------------------L0.?---------------------------------|                   "
    - "L0.?[620,626] 1.04us 85.08kb                                                                          |----L0.?-----| "
    - "**** Simulation run 196, type=split(ReduceOverlap)(split_times=[903]). 1 Input Files, 544.54kb total:"
    - "L0, all files 544.54kb                                                                                             "
    - "L0.360[888,932] 1.04us   |-----------------------------------------L0.360-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 544.54kb total:"
    - "L0                                                                                                                 "
    - "L0.?[888,903] 1.04us 185.64kb|------------L0.?------------|                                                            "
    - "L0.?[904,932] 1.04us 358.9kb                                |-------------------------L0.?--------------------------| "
    - "**** Simulation run 197, type=split(ReduceOverlap)(split_times=[619]). 1 Input Files, 374.74kb total:"
    - "L0, all files 374.74kb                                                                                             "
    - "L0.432[592,626] 1.05us   |-----------------------------------------L0.432-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 374.74kb total:"
    - "L0                                                                                                                 "
    - "L0.?[592,619] 1.05us 297.59kb|--------------------------------L0.?---------------------------------|                   "
    - "L0.?[620,626] 1.05us 77.15kb                                                                          |----L0.?-----| "
    - "**** Simulation run 198, type=split(ReduceOverlap)(split_times=[903]). 1 Input Files, 1.06mb total:"
    - "L0, all files 1.06mb                                                                                               "
    - "L0.364[888,986] 1.05us   |-----------------------------------------L0.364-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 1.06mb total:"
    - "L0                                                                                                                 "
    - "L0.?[888,903] 1.05us 165.94kb|---L0.?----|                                                                             "
    - "L0.?[904,986] 1.05us 918.23kb              |----------------------------------L0.?-----------------------------------| "
    - "**** Simulation run 199, type=split(ReduceOverlap)(split_times=[619]). 1 Input Files, 455.28kb total:"
    - "L0, all files 455.28kb                                                                                             "
    - "L0.436[592,626] 1.05us   |-----------------------------------------L0.436-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 455.28kb total:"
    - "L0                                                                                                                 "
    - "L0.?[592,619] 1.05us 361.55kb|--------------------------------L0.?---------------------------------|                   "
    - "L0.?[620,626] 1.05us 93.73kb                                                                          |----L0.?-----| "
    - "**** Simulation run 200, type=split(ReduceOverlap)(split_times=[903]). 1 Input Files, 839.24kb total:"
    - "L0, all files 839.24kb                                                                                             "
    - "L0.368[888,950] 1.05us   |-----------------------------------------L0.368-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 839.24kb total:"
    - "L0                                                                                                                 "
    - "L0.?[888,903] 1.05us 203.04kb|-------L0.?--------|                                                                     "
    - "L0.?[904,950] 1.05us 636.2kb                       |------------------------------L0.?------------------------------| "
    - "**** Simulation run 201, type=split(ReduceOverlap)(split_times=[619]). 1 Input Files, 622.12kb total:"
    - "L0, all files 622.12kb                                                                                             "
    - "L0.440[592,626] 1.05us   |-----------------------------------------L0.440-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 622.12kb total:"
    - "L0                                                                                                                 "
    - "L0.?[592,619] 1.05us 494.04kb|--------------------------------L0.?---------------------------------|                   "
    - "L0.?[620,626] 1.05us 128.08kb                                                                          |----L0.?-----| "
    - "**** Simulation run 202, type=split(ReduceOverlap)(split_times=[619]). 1 Input Files, 413.26kb total:"
    - "L0, all files 413.26kb                                                                                             "
    - "L0.444[592,626] 1.05us   |-----------------------------------------L0.444-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 413.26kb total:"
    - "L0                                                                                                                 "
    - "L0.?[592,619] 1.05us 328.18kb|--------------------------------L0.?---------------------------------|                   "
    - "L0.?[620,626] 1.05us 85.08kb                                                                          |----L0.?-----| "
    - "**** Simulation run 203, type=split(ReduceOverlap)(split_times=[903]). 1 Input Files, 544.54kb total:"
    - "L0, all files 544.54kb                                                                                             "
    - "L0.374[888,932] 1.05us   |-----------------------------------------L0.374-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 544.54kb total:"
    - "L0                                                                                                                 "
    - "L0.?[888,903] 1.05us 185.64kb|------------L0.?------------|                                                            "
    - "L0.?[904,932] 1.05us 358.9kb                                |-------------------------L0.?--------------------------| "
    - "**** Simulation run 204, type=split(ReduceOverlap)(split_times=[619]). 1 Input Files, 374.74kb total:"
    - "L0, all files 374.74kb                                                                                             "
    - "L0.448[592,626] 1.05us   |-----------------------------------------L0.448-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 374.74kb total:"
    - "L0                                                                                                                 "
    - "L0.?[592,619] 1.05us 297.59kb|--------------------------------L0.?---------------------------------|                   "
    - "L0.?[620,626] 1.05us 77.15kb                                                                          |----L0.?-----| "
    - "**** Simulation run 205, type=split(ReduceOverlap)(split_times=[903]). 1 Input Files, 1.06mb total:"
    - "L0, all files 1.06mb                                                                                               "
    - "L0.378[888,986] 1.05us   |-----------------------------------------L0.378-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 1.06mb total:"
    - "L0                                                                                                                 "
    - "L0.?[888,903] 1.05us 165.94kb|---L0.?----|                                                                             "
    - "L0.?[904,986] 1.05us 918.23kb              |----------------------------------L0.?-----------------------------------| "
    - "Committing partition 1:"
    - "  Soft Deleting 30 files: L0.322, L0.326, L0.332, L0.336, L0.340, L0.346, L0.350, L0.354, L0.360, L0.364, L0.368, L0.374, L0.378, L0.384, L0.388, L0.392, L0.396, L0.400, L0.404, L0.408, L0.412, L0.416, L0.420, L0.424, L0.428, L0.432, L0.436, L0.440, L0.444, L0.448"
    - "  Creating 60 files"
    - "**** Simulation run 206, type=split(CompactAndSplitOutput(FoundSubsetLessThanMaxCompactSize))(split_times=[275]). 2 Input Files, 102.69mb total:"
    - "L0                                                                                                                 "
    - "L0.278[42,294] 1.03us 2.67mb|----------------------------------L0.278-----------------------------------|             "
    - "L1                                                                                                                 "
    - "L1.379[42,334] 1.03us 100.02mb|-----------------------------------------L1.379-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 102.69mb total:"
    - "L1                                                                                                                 "
    - "L1.?[42,275] 1.03us 81.94mb|--------------------------------L1.?---------------------------------|                   "
    - "L1.?[276,334] 1.03us 20.75mb                                                                        |-----L1.?------| "
    - "Committing partition 1:"
    - "  Soft Deleting 2 files: L0.278, L1.379"
    - "  Creating 2 files"
    - "**** Simulation run 207, type=split(ReduceOverlap)(split_times=[275]). 1 Input Files, 1.56mb total:"
    - "L0, all files 1.56mb                                                                                               "
    - "L0.280[173,294] 1.03us   |----------------------------------------L0.280-----------------------------------------| "
    - "**** 2 Output Files (parquet_file_id not yet assigned), 1.56mb total:"
    - "L0                                                                                                                 "
    - "L0.?[173,275] 1.03us 1.31mb|----------------------------------L0.?-----------------------------------|               "
    - "L0.?[276,294] 1.03us 250.4kb                                                                            |---L0.?----| "
    - "**** Simulation run 208, type=split(ReduceOverlap)(split_times=[275]). 1 Input Files, 4.21mb total:"
    - "L0, all files 4.21mb                                                                                               "
    - "L0.282[50,294] 1.03us    |-----------------------------------------L0.282-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 4.21mb total:"
    - "L0                                                                                                                 "
    - "L0.?[50,275] 1.03us 3.89mb|--------------------------------------L0.?--------------------------------------|        "
    - "L0.?[276,294] 1.03us 336.03kb                                                                                   |L0.?| "
    - "**** Simulation run 209, type=split(ReduceOverlap)(split_times=[275]). 1 Input Files, 2.55mb total:"
    - "L0, all files 2.55mb                                                                                               "
    - "L0.284[76,294] 1.04us    |-----------------------------------------L0.284-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 2.55mb total:"
    - "L0                                                                                                                 "
    - "L0.?[76,275] 1.04us 2.32mb|--------------------------------------L0.?--------------------------------------|        "
    - "L0.?[276,294] 1.04us 227.29kb                                                                                  |L0.?-| "
    - "**** Simulation run 210, type=split(ReduceOverlap)(split_times=[275]). 1 Input Files, 2.67mb total:"
    - "L0, all files 2.67mb                                                                                               "
    - "L0.286[42,294] 1.04us    |-----------------------------------------L0.286-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 2.67mb total:"
    - "L0                                                                                                                 "
    - "L0.?[42,275] 1.04us 2.47mb|--------------------------------------L0.?---------------------------------------|       "
    - "L0.?[276,294] 1.04us 206.1kb                                                                                   |L0.?| "
    - "**** Simulation run 211, type=split(ReduceOverlap)(split_times=[275]). 1 Input Files, 1.56mb total:"
    - "L0, all files 1.56mb                                                                                               "
    - "L0.288[173,294] 1.04us   |----------------------------------------L0.288-----------------------------------------| "
    - "**** 2 Output Files (parquet_file_id not yet assigned), 1.56mb total:"
    - "L0                                                                                                                 "
    - "L0.?[173,275] 1.04us 1.31mb|----------------------------------L0.?-----------------------------------|               "
    - "L0.?[276,294] 1.04us 250.4kb                                                                            |---L0.?----| "
    - "**** Simulation run 212, type=split(ReduceOverlap)(split_times=[275]). 1 Input Files, 4.21mb total:"
    - "L0, all files 4.21mb                                                                                               "
    - "L0.290[50,294] 1.04us    |-----------------------------------------L0.290-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 4.21mb total:"
    - "L0                                                                                                                 "
    - "L0.?[50,275] 1.04us 3.89mb|--------------------------------------L0.?--------------------------------------|        "
    - "L0.?[276,294] 1.04us 336.03kb                                                                                   |L0.?| "
    - "**** Simulation run 213, type=split(ReduceOverlap)(split_times=[275]). 1 Input Files, 2.55mb total:"
    - "L0, all files 2.55mb                                                                                               "
    - "L0.292[76,294] 1.04us    |-----------------------------------------L0.292-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 2.55mb total:"
    - "L0                                                                                                                 "
    - "L0.?[76,275] 1.04us 2.32mb|--------------------------------------L0.?--------------------------------------|        "
    - "L0.?[276,294] 1.04us 227.29kb                                                                                  |L0.?-| "
    - "**** Simulation run 214, type=split(ReduceOverlap)(split_times=[275]). 1 Input Files, 2.67mb total:"
    - "L0, all files 2.67mb                                                                                               "
    - "L0.294[42,294] 1.04us    |-----------------------------------------L0.294-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 2.67mb total:"
    - "L0                                                                                                                 "
    - "L0.?[42,275] 1.04us 2.47mb|--------------------------------------L0.?---------------------------------------|       "
    - "L0.?[276,294] 1.04us 206.1kb                                                                                   |L0.?| "
    - "**** Simulation run 215, type=split(ReduceOverlap)(split_times=[275]). 1 Input Files, 1.56mb total:"
    - "L0, all files 1.56mb                                                                                               "
    - "L0.296[173,294] 1.04us   |----------------------------------------L0.296-----------------------------------------| "
    - "**** 2 Output Files (parquet_file_id not yet assigned), 1.56mb total:"
    - "L0                                                                                                                 "
    - "L0.?[173,275] 1.04us 1.31mb|----------------------------------L0.?-----------------------------------|               "
    - "L0.?[276,294] 1.04us 250.4kb                                                                            |---L0.?----| "
    - "**** Simulation run 216, type=split(ReduceOverlap)(split_times=[275]). 1 Input Files, 4.21mb total:"
    - "L0, all files 4.21mb                                                                                               "
    - "L0.298[50,294] 1.04us    |-----------------------------------------L0.298-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 4.21mb total:"
    - "L0                                                                                                                 "
    - "L0.?[50,275] 1.04us 3.89mb|--------------------------------------L0.?--------------------------------------|        "
    - "L0.?[276,294] 1.04us 336.03kb                                                                                   |L0.?| "
    - "**** Simulation run 217, type=split(ReduceOverlap)(split_times=[275]). 1 Input Files, 2.55mb total:"
    - "L0, all files 2.55mb                                                                                               "
    - "L0.300[76,294] 1.04us    |-----------------------------------------L0.300-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 2.55mb total:"
    - "L0                                                                                                                 "
    - "L0.?[76,275] 1.04us 2.32mb|--------------------------------------L0.?--------------------------------------|        "
    - "L0.?[276,294] 1.04us 227.29kb                                                                                  |L0.?-| "
    - "**** Simulation run 218, type=split(ReduceOverlap)(split_times=[275]). 1 Input Files, 2.67mb total:"
    - "L0, all files 2.67mb                                                                                               "
    - "L0.302[42,294] 1.05us    |-----------------------------------------L0.302-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 2.67mb total:"
    - "L0                                                                                                                 "
    - "L0.?[42,275] 1.05us 2.47mb|--------------------------------------L0.?---------------------------------------|       "
    - "L0.?[276,294] 1.05us 206.1kb                                                                                   |L0.?| "
    - "**** Simulation run 219, type=split(ReduceOverlap)(split_times=[275]). 1 Input Files, 1.56mb total:"
    - "L0, all files 1.56mb                                                                                               "
    - "L0.304[173,294] 1.05us   |----------------------------------------L0.304-----------------------------------------| "
    - "**** 2 Output Files (parquet_file_id not yet assigned), 1.56mb total:"
    - "L0                                                                                                                 "
    - "L0.?[173,275] 1.05us 1.31mb|----------------------------------L0.?-----------------------------------|               "
    - "L0.?[276,294] 1.05us 250.4kb                                                                            |---L0.?----| "
    - "**** Simulation run 220, type=split(ReduceOverlap)(split_times=[275]). 1 Input Files, 4.21mb total:"
    - "L0, all files 4.21mb                                                                                               "
    - "L0.306[50,294] 1.05us    |-----------------------------------------L0.306-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 4.21mb total:"
    - "L0                                                                                                                 "
    - "L0.?[50,275] 1.05us 3.89mb|--------------------------------------L0.?--------------------------------------|        "
    - "L0.?[276,294] 1.05us 336.03kb                                                                                   |L0.?| "
    - "**** Simulation run 221, type=split(ReduceOverlap)(split_times=[275]). 1 Input Files, 2.55mb total:"
    - "L0, all files 2.55mb                                                                                               "
    - "L0.308[76,294] 1.05us    |-----------------------------------------L0.308-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 2.55mb total:"
    - "L0                                                                                                                 "
    - "L0.?[76,275] 1.05us 2.32mb|--------------------------------------L0.?--------------------------------------|        "
    - "L0.?[276,294] 1.05us 227.29kb                                                                                  |L0.?-| "
    - "**** Simulation run 222, type=split(ReduceOverlap)(split_times=[275]). 1 Input Files, 2.67mb total:"
    - "L0, all files 2.67mb                                                                                               "
    - "L0.310[42,294] 1.05us    |-----------------------------------------L0.310-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 2.67mb total:"
    - "L0                                                                                                                 "
    - "L0.?[42,275] 1.05us 2.47mb|--------------------------------------L0.?---------------------------------------|       "
    - "L0.?[276,294] 1.05us 206.1kb                                                                                   |L0.?| "
    - "Committing partition 1:"
    - "  Soft Deleting 16 files: L0.280, L0.282, L0.284, L0.286, L0.288, L0.290, L0.292, L0.294, L0.296, L0.298, L0.300, L0.302, L0.304, L0.306, L0.308, L0.310"
    - "  Creating 32 files"
    - "**** Simulation run 223, type=split(CompactAndSplitOutput(ManySmallFiles))(split_times=[610]). 200 Input Files, 166.42mb total:"
    - "L0                                                                                                                 "
    - "L0.386[295,334] 1.03us 429.99kb                        |L0.386|                                                          "
    - "L0.387[335,356] 1.03us 242.56kb                           |L0.387|                                                       "
    - "L0.319[358,591] 1.03us 2.48mb                              |-------L0.319-------|                                      "
    - "L0.455[592,619] 1.03us 297.59kb                                                    |L0.455|                              "
    - "L0.456[620,626] 1.03us 77.15kb                                                       |L0.456|                           "
    - "L0.385[627,670] 1.03us 484.96kb                                                       |L0.385|                           "
    - "L0.321[673,887] 1.03us 2.29mb                                                            |------L0.321------|          "
    - "L0.457[888,903] 1.03us 165.94kb                                                                                |L0.457|  "
    - "L0.458[904,986] 1.03us 918.23kb                                                                                  |L0.458|"
    - "L0.517[173,275] 1.03us 1.31mb            |L0.517-|                                                                     "
    - "L0.518[276,294] 1.03us 250.4kb                      |L0.518|                                                            "
    - "L0.390[295,334] 1.03us 522.4kb                        |L0.390|                                                          "
    - "L0.391[335,356] 1.03us 294.69kb                           |L0.391|                                                       "
    - "L0.216[357,357] 1.03us 0b                              |L0.216|                                                    "
    - "L0.323[358,591] 1.03us 3.01mb                              |-------L0.323-------|                                      "
    - "L0.459[592,619] 1.03us 361.55kb                                                    |L0.459|                              "
    - "L0.460[620,626] 1.03us 93.73kb                                                       |L0.460|                           "
    - "L0.389[627,670] 1.03us 589.19kb                                                       |L0.389|                           "
    - "L0.218[671,672] 1.03us 13.27kb                                                           |L0.218|                       "
    - "L0.325[673,887] 1.03us 2.78mb                                                            |------L0.325------|          "
    - "L0.461[888,903] 1.03us 203.04kb                                                                                |L0.461|  "
    - "L0.462[904,950] 1.03us 636.2kb                                                                                  |L0.462|"
    - "L0.519[50,275] 1.03us 3.89mb|------L0.519-------|                                                                     "
    - "L0.520[276,294] 1.03us 336.03kb                      |L0.520|                                                            "
    - "L0.394[295,334] 1.03us 701.05kb                        |L0.394|                                                          "
    - "L0.395[335,356] 1.03us 395.46kb                           |L0.395|                                                       "
    - "L0.220[357,357] 1.03us 0b                              |L0.220|                                                    "
    - "L0.327[358,591] 1.03us 4.05mb                              |-------L0.327-------|                                      "
    - "L0.463[592,619] 1.03us 494.04kb                                                    |L0.463|                              "
    - "L0.464[620,626] 1.03us 128.08kb                                                       |L0.464|                           "
    - "L0.393[627,629] 1.03us 54.89kb                                                       |L0.393|                           "
    - "L0.521[76,275] 1.04us 2.32mb   |-----L0.521-----|                                                                     "
    - "L0.522[276,294] 1.04us 227.29kb                      |L0.522|                                                            "
    - "L0.398[295,334] 1.04us 474.19kb                        |L0.398|                                                          "
    - "L0.399[335,356] 1.04us 267.49kb                           |L0.399|                                                       "
    - "L0.222[357,357] 1.04us 0b                              |L0.222|                                                    "
    - "L0.329[358,591] 1.04us 2.73mb                              |-------L0.329-------|                                      "
    - "L0.465[592,619] 1.04us 328.18kb                                                    |L0.465|                              "
    - "L0.466[620,626] 1.04us 85.08kb                                                       |L0.466|                           "
    - "L0.397[627,670] 1.04us 534.81kb                                                       |L0.397|                           "
    - "L0.224[671,672] 1.04us 12.05kb                                                           |L0.224|                       "
    - "L0.331[673,887] 1.04us 2.53mb                                                            |------L0.331------|          "
    - "L0.467[888,903] 1.04us 185.64kb                                                                                |L0.467|  "
    - "L0.468[904,932] 1.04us 358.9kb                                                                                  |L0.468|"
    - "L0.523[42,275] 1.04us 2.47mb|-------L0.523-------|                                                                    "
    - "L0.524[276,294] 1.04us 206.1kb                      |L0.524|                                                            "
    - "L0.402[295,334] 1.04us 429.99kb                        |L0.402|                                                          "
    - "L0.403[335,356] 1.04us 242.56kb                           |L0.403|                                                       "
    - "L0.226[357,357] 1.04us 0b                              |L0.226|                                                    "
    - "L0.333[358,591] 1.04us 2.48mb                              |-------L0.333-------|                                      "
    - "L0.469[592,619] 1.04us 297.59kb                                                    |L0.469|                              "
    - "L0.470[620,626] 1.04us 77.15kb                                                       |L0.470|                           "
    - "L0.401[627,670] 1.04us 484.96kb                                                       |L0.401|                           "
    - "L0.228[671,672] 1.04us 10.92kb                                                           |L0.228|                       "
    - "L0.335[673,887] 1.04us 2.29mb                                                            |------L0.335------|          "
    - "L0.471[888,903] 1.04us 165.94kb                                                                                |L0.471|  "
    - "L0.472[904,986] 1.04us 918.23kb                                                                                  |L0.472|"
    - "L0.525[173,275] 1.04us 1.31mb            |L0.525-|                                                                     "
    - "L0.526[276,294] 1.04us 250.4kb                      |L0.526|                                                            "
    - "L0.406[295,334] 1.04us 522.4kb                        |L0.406|                                                          "
    - "L0.407[335,356] 1.04us 294.69kb                           |L0.407|                                                       "
    - "L0.230[357,357] 1.04us 0b                              |L0.230|                                                    "
    - "L0.337[358,591] 1.04us 3.01mb                              |-------L0.337-------|                                      "
    - "L0.473[592,619] 1.04us 361.55kb                                                    |L0.473|                              "
    - "L0.474[620,626] 1.04us 93.73kb                                                       |L0.474|                           "
    - "L0.405[627,670] 1.04us 589.19kb                                                       |L0.405|                           "
    - "L0.232[671,672] 1.04us 13.27kb                                                           |L0.232|                       "
    - "L0.339[673,887] 1.04us 2.78mb                                                            |------L0.339------|          "
    - "L0.475[888,903] 1.04us 203.04kb                                                                                |L0.475|  "
    - "L0.476[904,950] 1.04us 636.2kb                                                                                  |L0.476|"
    - "L0.527[50,275] 1.04us 3.89mb|------L0.527-------|                                                                     "
    - "L0.528[276,294] 1.04us 336.03kb                      |L0.528|                                                            "
    - "L0.410[295,334] 1.04us 701.05kb                        |L0.410|                                                          "
    - "L0.411[335,356] 1.04us 395.46kb                           |L0.411|                                                       "
    - "L0.234[357,357] 1.04us 0b                              |L0.234|                                                    "
    - "L0.341[358,591] 1.04us 4.05mb                              |-------L0.341-------|                                      "
    - "L0.477[592,619] 1.04us 494.04kb                                                    |L0.477|                              "
    - "L0.478[620,626] 1.04us 128.08kb                                                       |L0.478|                           "
    - "L0.409[627,629] 1.04us 54.89kb                                                       |L0.409|                           "
    - "L0.529[76,275] 1.04us 2.32mb   |-----L0.529-----|                                                                     "
    - "L0.530[276,294] 1.04us 227.29kb                      |L0.530|                                                            "
    - "L0.414[295,334] 1.04us 474.19kb                        |L0.414|                                                          "
    - "L0.415[335,356] 1.04us 267.49kb                           |L0.415|                                                       "
    - "L0.236[357,357] 1.04us 0b                              |L0.236|                                                    "
    - "L0.343[358,591] 1.04us 2.73mb                              |-------L0.343-------|                                      "
    - "L0.479[592,619] 1.04us 328.18kb                                                    |L0.479|                              "
    - "L0.480[620,626] 1.04us 85.08kb                                                       |L0.480|                           "
    - "L0.413[627,670] 1.04us 534.81kb                                                       |L0.413|                           "
    - "L0.238[671,672] 1.04us 12.05kb                                                           |L0.238|                       "
    - "L0.345[673,887] 1.04us 2.53mb                                                            |------L0.345------|          "
    - "L0.481[888,903] 1.04us 185.64kb                                                                                |L0.481|  "
    - "L0.482[904,932] 1.04us 358.9kb                                                                                  |L0.482|"
    - "L0.531[42,275] 1.04us 2.47mb|-------L0.531-------|                                                                    "
    - "L0.532[276,294] 1.04us 206.1kb                      |L0.532|                                                            "
    - "L0.418[295,334] 1.04us 429.99kb                        |L0.418|                                                          "
    - "L0.419[335,356] 1.04us 242.56kb                           |L0.419|                                                       "
    - "L0.240[357,357] 1.04us 0b                              |L0.240|                                                    "
    - "L0.347[358,591] 1.04us 2.48mb                              |-------L0.347-------|                                      "
    - "L0.483[592,619] 1.04us 297.59kb                                                    |L0.483|                              "
    - "L0.484[620,626] 1.04us 77.15kb                                                       |L0.484|                           "
    - "L0.417[627,670] 1.04us 484.96kb                                                       |L0.417|                           "
    - "L0.242[671,672] 1.04us 10.92kb                                                           |L0.242|                       "
    - "L0.349[673,887] 1.04us 2.29mb                                                            |------L0.349------|          "
    - "L0.485[888,903] 1.04us 165.94kb                                                                                |L0.485|  "
    - "L0.486[904,986] 1.04us 918.23kb                                                                                  |L0.486|"
    - "L0.533[173,275] 1.04us 1.31mb            |L0.533-|                                                                     "
    - "L0.534[276,294] 1.04us 250.4kb                      |L0.534|                                                            "
    - "L0.422[295,334] 1.04us 522.4kb                        |L0.422|                                                          "
    - "L0.423[335,356] 1.04us 294.69kb                           |L0.423|                                                       "
    - "L0.244[357,357] 1.04us 0b                              |L0.244|                                                    "
    - "L0.351[358,591] 1.04us 3.01mb                              |-------L0.351-------|                                      "
    - "L0.487[592,619] 1.04us 361.55kb                                                    |L0.487|                              "
    - "L0.488[620,626] 1.04us 93.73kb                                                       |L0.488|                           "
    - "L0.421[627,670] 1.04us 589.19kb                                                       |L0.421|                           "
    - "L0.246[671,672] 1.04us 13.27kb                                                           |L0.246|                       "
    - "L0.353[673,887] 1.04us 2.78mb                                                            |------L0.353------|          "
    - "L0.489[888,903] 1.04us 203.04kb                                                                                |L0.489|  "
    - "L0.490[904,950] 1.04us 636.2kb                                                                                  |L0.490|"
    - "L0.535[50,275] 1.04us 3.89mb|------L0.535-------|                                                                     "
    - "L0.536[276,294] 1.04us 336.03kb                      |L0.536|                                                            "
    - "L0.426[295,334] 1.04us 701.05kb                        |L0.426|                                                          "
    - "L0.427[335,356] 1.04us 395.46kb                           |L0.427|                                                       "
    - "L0.248[357,357] 1.04us 0b                              |L0.248|                                                    "
    - "L0.355[358,591] 1.04us 4.05mb                              |-------L0.355-------|                                      "
    - "L0.491[592,619] 1.04us 494.04kb                                                    |L0.491|                              "
    - "L0.492[620,626] 1.04us 128.08kb                                                       |L0.492|                           "
    - "L0.425[627,629] 1.04us 54.89kb                                                       |L0.425|                           "
    - "L0.537[76,275] 1.04us 2.32mb   |-----L0.537-----|                                                                     "
    - "L0.538[276,294] 1.04us 227.29kb                      |L0.538|                                                            "
    - "L0.430[295,334] 1.04us 474.19kb                        |L0.430|                                                          "
    - "L0.431[335,356] 1.04us 267.49kb                           |L0.431|                                                       "
    - "L0.250[357,357] 1.04us 0b                              |L0.250|                                                    "
    - "L0.357[358,591] 1.04us 2.73mb                              |-------L0.357-------|                                      "
    - "L0.493[592,619] 1.04us 328.18kb                                                    |L0.493|                              "
    - "L0.494[620,626] 1.04us 85.08kb                                                       |L0.494|                           "
    - "L0.429[627,670] 1.04us 534.81kb                                                       |L0.429|                           "
    - "L0.252[671,672] 1.04us 12.05kb                                                           |L0.252|                       "
    - "L0.359[673,887] 1.04us 2.53mb                                                            |------L0.359------|          "
    - "L0.495[888,903] 1.04us 185.64kb                                                                                |L0.495|  "
    - "L0.496[904,932] 1.04us 358.9kb                                                                                  |L0.496|"
    - "L0.539[42,275] 1.05us 2.47mb|-------L0.539-------|                                                                    "
    - "L0.540[276,294] 1.05us 206.1kb                      |L0.540|                                                            "
    - "L0.434[295,334] 1.05us 429.99kb                        |L0.434|                                                          "
    - "L0.435[335,356] 1.05us 242.56kb                           |L0.435|                                                       "
    - "L0.254[357,357] 1.05us 0b                              |L0.254|                                                    "
    - "L0.361[358,591] 1.05us 2.48mb                              |-------L0.361-------|                                      "
    - "L0.497[592,619] 1.05us 297.59kb                                                    |L0.497|                              "
    - "L0.498[620,626] 1.05us 77.15kb                                                       |L0.498|                           "
    - "L0.433[627,670] 1.05us 484.96kb                                                       |L0.433|                           "
    - "L0.256[671,672] 1.05us 10.92kb                                                           |L0.256|                       "
    - "L0.363[673,887] 1.05us 2.29mb                                                            |------L0.363------|          "
    - "L0.499[888,903] 1.05us 165.94kb                                                                                |L0.499|  "
    - "L0.500[904,986] 1.05us 918.23kb                                                                                  |L0.500|"
    - "L0.541[173,275] 1.05us 1.31mb            |L0.541-|                                                                     "
    - "L0.542[276,294] 1.05us 250.4kb                      |L0.542|                                                            "
    - "L0.438[295,334] 1.05us 522.4kb                        |L0.438|                                                          "
    - "L0.439[335,356] 1.05us 294.69kb                           |L0.439|                                                       "
    - "L0.258[357,357] 1.05us 0b                              |L0.258|                                                    "
    - "L0.365[358,591] 1.05us 3.01mb                              |-------L0.365-------|                                      "
    - "L0.501[592,619] 1.05us 361.55kb                                                    |L0.501|                              "
    - "L0.502[620,626] 1.05us 93.73kb                                                       |L0.502|                           "
    - "L0.437[627,670] 1.05us 589.19kb                                                       |L0.437|                           "
    - "L0.260[671,672] 1.05us 13.27kb                                                           |L0.260|                       "
    - "L0.367[673,887] 1.05us 2.78mb                                                            |------L0.367------|          "
    - "L0.503[888,903] 1.05us 203.04kb                                                                                |L0.503|  "
    - "L0.504[904,950] 1.05us 636.2kb                                                                                  |L0.504|"
    - "L0.543[50,275] 1.05us 3.89mb|------L0.543-------|                                                                     "
    - "L0.544[276,294] 1.05us 336.03kb                      |L0.544|                                                            "
    - "L0.442[295,334] 1.05us 701.05kb                        |L0.442|                                                          "
    - "L0.443[335,356] 1.05us 395.46kb                           |L0.443|                                                       "
    - "L0.262[357,357] 1.05us 0b                              |L0.262|                                                    "
    - "L0.369[358,591] 1.05us 4.05mb                              |-------L0.369-------|                                      "
    - "L0.505[592,619] 1.05us 494.04kb                                                    |L0.505|                              "
    - "L0.506[620,626] 1.05us 128.08kb                                                       |L0.506|                           "
    - "L0.441[627,629] 1.05us 54.89kb                                                       |L0.441|                           "
    - "L0.545[76,275] 1.05us 2.32mb   |-----L0.545-----|                                                                     "
    - "L0.546[276,294] 1.05us 227.29kb                      |L0.546|                                                            "
    - "L0.446[295,334] 1.05us 474.19kb                        |L0.446|                                                          "
    - "L0.447[335,356] 1.05us 267.49kb                           |L0.447|                                                       "
    - "L0.264[357,357] 1.05us 0b                              |L0.264|                                                    "
    - "L0.371[358,591] 1.05us 2.73mb                              |-------L0.371-------|                                      "
    - "L0.507[592,619] 1.05us 328.18kb                                                    |L0.507|                              "
    - "L0.508[620,626] 1.05us 85.08kb                                                       |L0.508|                           "
    - "L0.445[627,670] 1.05us 534.81kb                                                       |L0.445|                           "
    - "L0.266[671,672] 1.05us 12.05kb                                                           |L0.266|                       "
    - "L0.373[673,887] 1.05us 2.53mb                                                            |------L0.373------|          "
    - "L0.509[888,903] 1.05us 185.64kb                                                                                |L0.509|  "
    - "L0.510[904,932] 1.05us 358.9kb                                                                                  |L0.510|"
    - "L0.547[42,275] 1.05us 2.47mb|-------L0.547-------|                                                                    "
    - "L0.548[276,294] 1.05us 206.1kb                      |L0.548|                                                            "
    - "L0.450[295,334] 1.05us 429.99kb                        |L0.450|                                                          "
    - "L0.451[335,356] 1.05us 242.56kb                           |L0.451|                                                       "
    - "L0.268[357,357] 1.05us 0b                              |L0.268|                                                    "
    - "L0.375[358,591] 1.05us 2.48mb                              |-------L0.375-------|                                      "
    - "L0.511[592,619] 1.05us 297.59kb                                                    |L0.511|                              "
    - "L0.512[620,626] 1.05us 77.15kb                                                       |L0.512|                           "
    - "L0.449[627,670] 1.05us 484.96kb                                                       |L0.449|                           "
    - "L0.270[671,672] 1.05us 10.92kb                                                           |L0.270|                       "
    - "L0.377[673,887] 1.05us 2.29mb                                                            |------L0.377------|          "
    - "L0.513[888,903] 1.05us 165.94kb                                                                                |L0.513|  "
    - "**** 2 Output Files (parquet_file_id not yet assigned), 166.42mb total:"
    - "L0                                                                                                                 "
    - "L0.?[42,610] 1.05us 100.14mb|------------------------L0.?------------------------|                                    "
    - "L0.?[611,986] 1.05us 66.29mb                                                      |--------------L0.?---------------| "
    - "Committing partition 1:"
    - "  Soft Deleting 200 files: L0.216, L0.218, L0.220, L0.222, L0.224, L0.226, L0.228, L0.230, L0.232, L0.234, L0.236, L0.238, L0.240, L0.242, L0.244, L0.246, L0.248, L0.250, L0.252, L0.254, L0.256, L0.258, L0.260, L0.262, L0.264, L0.266, L0.268, L0.270, L0.319, L0.321, L0.323, L0.325, L0.327, L0.329, L0.331, L0.333, L0.335, L0.337, L0.339, L0.341, L0.343, L0.345, L0.347, L0.349, L0.351, L0.353, L0.355, L0.357, L0.359, L0.361, L0.363, L0.365, L0.367, L0.369, L0.371, L0.373, L0.375, L0.377, L0.385, L0.386, L0.387, L0.389, L0.390, L0.391, L0.393, L0.394, L0.395, L0.397, L0.398, L0.399, L0.401, L0.402, L0.403, L0.405, L0.406, L0.407, L0.409, L0.410, L0.411, L0.413, L0.414, L0.415, L0.417, L0.418, L0.419, L0.421, L0.422, L0.423, L0.425, L0.426, L0.427, L0.429, L0.430, L0.431, L0.433, L0.434, L0.435, L0.437, L0.438, L0.439, L0.441, L0.442, L0.443, L0.445, L0.446, L0.447, L0.449, L0.450, L0.451, L0.455, L0.456, L0.457, L0.458, L0.459, L0.460, L0.461, L0.462, L0.463, L0.464, L0.465, L0.466, L0.467, L0.468, L0.469, L0.470, L0.471, L0.472, L0.473, L0.474, L0.475, L0.476, L0.477, L0.478, L0.479, L0.480, L0.481, L0.482, L0.483, L0.484, L0.485, L0.486, L0.487, L0.488, L0.489, L0.490, L0.491, L0.492, L0.493, L0.494, L0.495, L0.496, L0.497, L0.498, L0.499, L0.500, L0.501, L0.502, L0.503, L0.504, L0.505, L0.506, L0.507, L0.508, L0.509, L0.510, L0.511, L0.512, L0.513, L0.517, L0.518, L0.519, L0.520, L0.521, L0.522, L0.523, L0.524, L0.525, L0.526, L0.527, L0.528, L0.529, L0.530, L0.531, L0.532, L0.533, L0.534, L0.535, L0.536, L0.537, L0.538, L0.539, L0.540, L0.541, L0.542, L0.543, L0.544, L0.545, L0.546, L0.547, L0.548"
    - "  Creating 2 files"
    - "**** Simulation run 224, type=compact(ManySmallFiles). 1 Input Files, 918.23kb total:"
    - "L0, all files 918.23kb                                                                                             "
    - "L0.514[904,986] 1.05us   |-----------------------------------------L0.514-----------------------------------------|"
    - "**** 1 Output Files (parquet_file_id not yet assigned), 918.23kb total:"
    - "L0, all files 918.23kb                                                                                             "
    - "L0.?[904,986] 1.05us     |------------------------------------------L0.?------------------------------------------|"
    - "Committing partition 1:"
    - "  Soft Deleting 1 files: L0.514"
    - "  Creating 1 files"
    - "**** Simulation run 225, type=split(HighL0OverlapSingleFile)(split_times=[234]). 1 Input Files, 81.94mb total:"
    - "L1, all files 81.94mb                                                                                              "
    - "L1.515[42,275] 1.03us    |-----------------------------------------L1.515-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 81.94mb total:"
    - "L1                                                                                                                 "
    - "L1.?[42,234] 1.03us 67.52mb|----------------------------------L1.?----------------------------------|                "
    - "L1.?[235,275] 1.03us 14.42mb                                                                          |----L1.?-----| "
    - "**** Simulation run 226, type=split(HighL0OverlapSingleFile)(split_times=[426]). 1 Input Files, 100.33mb total:"
    - "L1, all files 100.33mb                                                                                             "
    - "L1.452[335,619] 1.03us   |-----------------------------------------L1.452-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 100.33mb total:"
    - "L1                                                                                                                 "
    - "L1.?[335,426] 1.03us 32.15mb|-----------L1.?-----------|                                                              "
    - "L1.?[427,619] 1.03us 68.18mb                             |---------------------------L1.?---------------------------| "
    - "**** Simulation run 227, type=split(HighL0OverlapSingleFile)(split_times=[234, 426]). 1 Input Files, 100.14mb total:"
    - "L0, all files 100.14mb                                                                                             "
    - "L0.549[42,610] 1.05us    |-----------------------------------------L0.549-----------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 100.14mb total:"
    - "L0                                                                                                                 "
    - "L0.?[42,234] 1.05us 33.85mb|------------L0.?------------|                                                            "
    - "L0.?[235,426] 1.05us 33.67mb                              |------------L0.?------------|                              "
    - "L0.?[427,610] 1.05us 32.61mb                                                             |-----------L0.?-----------| "
    - "Committing partition 1:"
    - "  Soft Deleting 3 files: L1.452, L1.515, L0.549"
    - "  Creating 7 files"
    - "**** Simulation run 228, type=split(ReduceOverlap)(split_times=[619, 903]). 1 Input Files, 66.29mb total:"
    - "L0, all files 66.29mb                                                                                              "
    - "L0.550[611,986] 1.05us   |-----------------------------------------L0.550-----------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 66.29mb total:"
    - "L0                                                                                                                 "
    - "L0.?[611,619] 1.05us 1.41mb|L0.?|                                                                                    "
    - "L0.?[620,903] 1.05us 50.02mb  |------------------------------L0.?-------------------------------|                     "
    - "L0.?[904,986] 1.05us 14.85mb                                                                      |------L0.?-------| "
    - "**** Simulation run 229, type=split(ReduceOverlap)(split_times=[275, 334]). 1 Input Files, 33.67mb total:"
    - "L0, all files 33.67mb                                                                                              "
    - "L0.557[235,426] 1.05us   |-----------------------------------------L0.557-----------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 33.67mb total:"
    - "L0                                                                                                                 "
    - "L0.?[235,275] 1.05us 7.05mb|------L0.?------|                                                                        "
    - "L0.?[276,334] 1.05us 10.23mb                   |----------L0.?-----------|                                            "
    - "L0.?[335,426] 1.05us 16.4mb                                               |------------------L0.?------------------| "
    - "Committing partition 1:"
    - "  Soft Deleting 2 files: L0.550, L0.557"
    - "  Creating 6 files"
    - "**** Simulation run 230, type=split(CompactAndSplitOutput(FoundSubsetLessThanMaxCompactSize))(split_times=[257, 472]). 7 Input Files, 269.49mb total:"
    - "L0                                                                                                                 "
    - "L0.556[42,234] 1.05us 33.85mb|----------L0.556-----------|                                                             "
    - "L0.558[427,610] 1.05us 32.61mb                                                            |----------L0.558----------|  "
    - "L1                                                                                                                 "
    - "L1.552[42,234] 1.03us 67.52mb|----------L1.552-----------|                                                             "
    - "L1.553[235,275] 1.03us 14.42mb                              |L1.553|                                                    "
    - "L1.516[276,334] 1.03us 20.75mb                                    |L1.516-|                                             "
    - "L1.554[335,426] 1.03us 32.15mb                                             |---L1.554---|                               "
    - "L1.555[427,619] 1.03us 68.18mb                                                            |----------L1.555-----------| "
    - "**** 3 Output Files (parquet_file_id not yet assigned), 269.49mb total:"
    - "L1                                                                                                                 "
    - "L1.?[42,257] 1.05us 100.42mb|-------------L1.?--------------|                                                         "
    - "L1.?[258,472] 1.05us 99.95mb                                 |-------------L1.?--------------|                        "
    - "L1.?[473,619] 1.05us 69.12mb                                                                   |--------L1.?--------| "
    - "Committing partition 1:"
    - "  Soft Deleting 7 files: L1.516, L1.552, L1.553, L1.554, L1.555, L0.556, L0.558"
    - "  Creating 3 files"
    - "**** Simulation run 231, type=split(ReduceOverlap)(split_times=[257]). 1 Input Files, 7.05mb total:"
    - "L0, all files 7.05mb                                                                                               "
    - "L0.562[235,275] 1.05us   |-----------------------------------------L0.562-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 7.05mb total:"
    - "L0                                                                                                                 "
    - "L0.?[235,257] 1.05us 3.88mb|---------------------L0.?----------------------|                                         "
    - "L0.?[258,275] 1.05us 3.17mb                                                   |----------------L0.?----------------| "
    - "Committing partition 1:"
    - "  Soft Deleting 1 files: L0.562"
    - "  Creating 2 files"
    - "**** Simulation run 232, type=split(CompactAndSplitOutput(FoundSubsetLessThanMaxCompactSize))(split_times=[969]). 2 Input Files, 30.57mb total:"
    - "L0                                                                                                                 "
    - "L0.551[904,986] 1.05us 918.23kb|-----------------------------------------L0.551-----------------------------------------|"
    - "L1                                                                                                                 "
    - "L1.454[904,986] 1.03us 29.68mb|-----------------------------------------L1.454-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 30.57mb total:"
    - "L1                                                                                                                 "
    - "L1.?[904,969] 1.05us 24.23mb|--------------------------------L1.?---------------------------------|                   "
    - "L1.?[970,986] 1.05us 6.34mb                                                                        |-----L1.?------| "
    - "Committing partition 1:"
    - "  Soft Deleting 2 files: L1.454, L0.551"
    - "  Creating 2 files"
    - "**** Simulation run 233, type=split(ReduceOverlap)(split_times=[969]). 1 Input Files, 14.85mb total:"
    - "L0, all files 14.85mb                                                                                              "
    - "L0.561[904,986] 1.05us   |-----------------------------------------L0.561-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 14.85mb total:"
    - "L0                                                                                                                 "
    - "L0.?[904,969] 1.05us 11.77mb|--------------------------------L0.?---------------------------------|                   "
    - "L0.?[970,986] 1.05us 3.08mb                                                                        |-----L0.?------| "
    - "Committing partition 1:"
    - "  Soft Deleting 1 files: L0.561"
    - "  Creating 2 files"
    - "**** Simulation run 234, type=split(CompactAndSplitOutput(FoundSubsetLessThanMaxCompactSize))(split_times=[428]). 3 Input Files, 126.57mb total:"
    - "L0                                                                                                                 "
    - "L0.564[335,426] 1.05us 16.4mb                                |---------------L0.564---------------|                    "
    - "L0.563[276,334] 1.05us 10.23mb       |--------L0.563--------|                                                           "
    - "L1                                                                                                                 "
    - "L1.566[258,472] 1.05us 99.95mb|-----------------------------------------L1.566-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 126.57mb total:"
    - "L1                                                                                                                 "
    - "L1.?[258,428] 1.05us 100.55mb|--------------------------------L1.?---------------------------------|                   "
    - "L1.?[429,472] 1.05us 26.02mb                                                                       |------L1.?------| "
    - "Committing partition 1:"
    - "  Soft Deleting 3 files: L0.563, L0.564, L1.566"
    - "  Creating 2 files"
    - "**** Simulation run 235, type=split(CompactAndSplitOutput(FoundSubsetLessThanMaxCompactSize))(split_times=[969]). 4 Input Files, 45.42mb total:"
    - "L0                                                                                                                 "
    - "L0.572[904,969] 1.05us 11.77mb|-------------------------------L0.572--------------------------------|                   "
    - "L0.573[970,986] 1.05us 3.08mb                                                                        |----L0.573-----| "
    - "L1                                                                                                                 "
    - "L1.570[904,969] 1.05us 24.23mb|-------------------------------L1.570--------------------------------|                   "
    - "L1.571[970,986] 1.05us 6.34mb                                                                        |----L1.571-----| "
    - "**** 2 Output Files (parquet_file_id not yet assigned), 45.42mb total:"
    - "L1                                                                                                                 "
    - "L1.?[904,969] 1.05us 36mb|--------------------------------L1.?---------------------------------|                   "
    - "L1.?[970,986] 1.05us 9.42mb                                                                        |-----L1.?------| "
    - "Committing partition 1:"
    - "  Soft Deleting 4 files: L1.570, L1.571, L0.572, L0.573"
    - "  Creating 2 files"
    - "**** Simulation run 236, type=split(CompactAndSplitOutput(FoundSubsetLessThanMaxCompactSize))(split_times=[668, 863]). 4 Input Files, 220.54mb total:"
    - "L0                                                                                                                 "
    - "L0.560[620,903] 1.05us 50.02mb                              |-------------------------L0.560--------------------------| "
    - "L0.559[611,619] 1.05us 1.41mb                            |L0.559|                                                      "
    - "L1                                                                                                                 "
    - "L1.453[620,903] 1.03us 99.98mb                              |-------------------------L1.453--------------------------| "
    - "L1.567[473,619] 1.05us 69.12mb|-----------L1.567-----------|                                                            "
    - "**** 3 Output Files (parquet_file_id not yet assigned), 220.54mb total:"
    - "L1                                                                                                                 "
    - "L1.?[473,668] 1.05us 100.01mb|-----------------L1.?-----------------|                                                  "
    - "L1.?[669,863] 1.05us 99.5mb                                         |-----------------L1.?-----------------|         "
    - "L1.?[864,903] 1.05us 21.03mb                                                                                 |-L1.?-| "
    - "Committing partition 1:"
    - "  Soft Deleting 4 files: L1.453, L0.559, L0.560, L1.567"
    - "  Creating 3 files"
    - "**** Simulation run 237, type=split(CompactAndSplitOutput(TotalSizeLessThanMaxCompactSize))(split_times=[228, 414]). 4 Input Files, 208.01mb total:"
    - "L0                                                                                                                 "
    - "L0.568[235,257] 1.05us 3.88mb                                             |L0.568|                                     "
    - "L0.569[258,275] 1.05us 3.17mb                                                  |L0.569|                                "
    - "L1                                                                                                                 "
    - "L1.565[42,257] 1.05us 100.42mb|---------------------L1.565---------------------|                                        "
    - "L1.574[258,428] 1.05us 100.55mb                                                  |---------------L1.574----------------| "
    - "**** 3 Output Files (parquet_file_id not yet assigned), 208.01mb total:"
    - "L1                                                                                                                 "
    - "L1.?[42,228] 1.05us 100.23mb|------------------L1.?-------------------|                                               "
    - "L1.?[229,414] 1.05us 99.7mb                                           |------------------L1.?-------------------|    "
    - "L1.?[415,428] 1.05us 8.08mb                                                                                      |L1.?|"
    - "Committing partition 1:"
    - "  Soft Deleting 4 files: L1.565, L0.568, L0.569, L1.574"
    - "  Creating 3 files"
    - "**** Simulation run 238, type=split(ReduceOverlap)(split_times=[499, 599]). 1 Input Files, 100.01mb total:"
    - "L1, all files 100.01mb                                                                                             "
    - "L1.578[473,668] 1.05us   |-----------------------------------------L1.578-----------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 100.01mb total:"
    - "L1                                                                                                                 "
    - "L1.?[473,499] 1.05us 13.34mb|---L1.?---|                                                                              "
    - "L1.?[500,599] 1.05us 50.78mb            |-------------------L1.?--------------------|                                 "
    - "L1.?[600,668] 1.05us 35.9mb                                                          |------------L1.?-------------| "
    - "**** Simulation run 239, type=split(ReduceOverlap)(split_times=[699, 799]). 1 Input Files, 99.5mb total:"
    - "L1, all files 99.5mb                                                                                               "
    - "L1.579[669,863] 1.05us   |-----------------------------------------L1.579-----------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 99.5mb total:"
    - "L1                                                                                                                 "
    - "L1.?[669,699] 1.05us 15.39mb|---L1.?----|                                                                             "
    - "L1.?[700,799] 1.05us 50.78mb              |-------------------L1.?--------------------|                               "
    - "L1.?[800,863] 1.05us 33.34mb                                                            |-----------L1.?------------| "
    - "**** Simulation run 240, type=split(ReduceOverlap)(split_times=[899]). 1 Input Files, 21.03mb total:"
    - "L1, all files 21.03mb                                                                                              "
    - "L1.580[864,903] 1.05us   |-----------------------------------------L1.580-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 21.03mb total:"
    - "L1                                                                                                                 "
    - "L1.?[864,899] 1.05us 18.87mb|-------------------------------------L1.?-------------------------------------|          "
    - "L1.?[900,903] 1.05us 2.16mb                                                                                   |L1.?| "
    - "**** Simulation run 241, type=split(ReduceOverlap)(split_times=[299, 399]). 1 Input Files, 99.7mb total:"
    - "L1, all files 99.7mb                                                                                               "
    - "L1.582[229,414] 1.05us   |-----------------------------------------L1.582-----------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 99.7mb total:"
    - "L1                                                                                                                 "
    - "L1.?[229,299] 1.05us 37.72mb|--------------L1.?--------------|                                                        "
    - "L1.?[300,399] 1.05us 53.35mb                                  |---------------------L1.?---------------------|        "
    - "L1.?[400,414] 1.05us 8.62mb                                                                                   |L1.?| "
    - "**** Simulation run 242, type=split(ReduceOverlap)(split_times=[99, 199]). 1 Input Files, 100.23mb total:"
    - "L1, all files 100.23mb                                                                                             "
    - "L1.581[42,228] 1.05us    |----------------------------------------L1.581-----------------------------------------| "
    - "**** 3 Output Files (parquet_file_id not yet assigned), 100.23mb total:"
    - "L1                                                                                                                 "
    - "L1.?[42,99] 1.05us 30.72mb|----------L1.?-----------|                                                               "
    - "L1.?[100,199] 1.05us 53.35mb                            |--------------------L1.?---------------------|               "
    - "L1.?[200,228] 1.05us 16.17mb                                                                            |---L1.?----| "
    - "Committing partition 1:"
    - "  Soft Deleting 5 files: L1.578, L1.579, L1.580, L1.581, L1.582"
    - "  Creating 14 files"
    - "**** Simulation run 243, type=split(CompactAndSplitOutput(FoundSubsetLessThanMaxCompactSize))(split_times=[71, 142]). 4 Input Files, 284.07mb total:"
    - "L1                                                                                                                 "
    - "L1.595[42,99] 1.05us 30.72mb                  |--------L1.595---------|                                               "
    - "L1.596[100,199] 1.05us 53.35mb                                             |------------------L1.596------------------| "
    - "L2                                                                                                                 "
    - "L2.1[0,99] 99ns 100mb    |-------------------L2.1-------------------|                                              "
    - "L2.2[100,199] 199ns 100mb                                             |-------------------L2.2-------------------| "
    - "**** 3 Output Files (parquet_file_id not yet assigned), 284.07mb total:"
    - "L2                                                                                                                 "
    - "L2.?[0,71] 1.05us 101.35mb|-------------L2.?-------------|                                                          "
    - "L2.?[72,142] 1.05us 99.92mb                                |------------L2.?-------------|                           "
    - "L2.?[143,199] 1.05us 82.79mb                                                                |---------L2.?----------| "
    - "Committing partition 1:"
    - "  Soft Deleting 4 files: L2.1, L2.2, L1.595, L1.596"
    - "  Creating 3 files"
    - "**** Simulation run 244, type=split(CompactAndSplitOutput(FoundSubsetLessThanMaxCompactSize))(split_times=[265]). 3 Input Files, 153.89mb total:"
    - "L1                                                                                                                 "
    - "L1.597[200,228] 1.05us 16.17mb|--------L1.597---------|                                                                 "
    - "L1.592[229,299] 1.05us 37.72mb                          |---------------------------L1.592----------------------------| "
    - "L2                                                                                                                 "
    - "L2.3[200,299] 299ns 100mb|-----------------------------------------L2.3------------------------------------------| "
    - "**** 2 Output Files (parquet_file_id not yet assigned), 153.89mb total:"
    - "L2                                                                                                                 "
    - "L2.?[200,265] 1.05us 101.04mb|--------------------------L2.?---------------------------|                               "
    - "L2.?[266,299] 1.05us 52.85mb                                                           |-----------L2.?------------|  "
    - "Committing partition 1:"
    - "  Soft Deleting 3 files: L2.3, L1.592, L1.597"
    - "  Creating 2 files"
    - "**** Simulation run 245, type=split(CompactAndSplitOutput(FoundSubsetLessThanMaxCompactSize))(split_times=[368, 436]). 6 Input Files, 296.08mb total:"
    - "L1                                                                                                                 "
    - "L1.593[300,399] 1.05us 53.35mb|------------------L1.593------------------|                                              "
    - "L1.594[400,414] 1.05us 8.62mb                                             |L1.594|                                     "
    - "L1.583[415,428] 1.05us 8.08mb                                                    |L1.583|                              "
    - "L1.575[429,472] 1.05us 26.02mb                                                          |-----L1.575------|             "
    - "L2                                                                                                                 "
    - "L2.4[300,399] 399ns 100mb|-------------------L2.4-------------------|                                              "
    - "L2.5[400,499] 499ns 100mb                                             |-------------------L2.5-------------------| "
    - "**** 3 Output Files (parquet_file_id not yet assigned), 296.08mb total:"
    - "L2                                                                                                                 "
    - "L2.?[300,368] 1.05us 101.17mb|------------L2.?------------|                                                            "
    - "L2.?[369,436] 1.05us 99.69mb                               |------------L2.?------------|                             "
    - "L2.?[437,499] 1.05us 95.22mb                                                             |-----------L2.?-----------| "
    - "Committing partition 1:"
    - "  Soft Deleting 6 files: L2.4, L2.5, L1.575, L1.583, L1.593, L1.594"
    - "  Creating 3 files"
    - "**** Simulation run 246, type=split(CompactAndSplitOutput(FoundSubsetLessThanMaxCompactSize))(split_times=[500, 563]). 4 Input Files, 259.33mb total:"
    - "L1                                                                                                                 "
    - "L1.584[473,499] 1.05us 13.34mb                    |---L1.584---|                                                        "
    - "L1.585[500,599] 1.05us 50.78mb                                   |-----------------------L1.585------------------------|"
    - "L2                                                                                                                 "
    - "L2.605[437,499] 1.05us 95.22mb|-------------L2.605-------------|                                                        "
    - "L2.6[500,599] 599ns 100mb                                   |------------------------L2.6-------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 259.33mb total:"
    - "L2                                                                                                                 "
    - "L2.?[437,500] 1.05us 100.85mb|--------------L2.?---------------|                                                       "
    - "L2.?[501,563] 1.05us 99.25mb                                   |--------------L2.?--------------|                     "
    - "L2.?[564,599] 1.05us 59.23mb                                                                      |------L2.?-------| "
    - "Committing partition 1:"
    - "  Soft Deleting 4 files: L2.6, L1.584, L1.585, L2.605"
    - "  Creating 3 files"
    - "**** Simulation run 247, type=split(CompactAndSplitOutput(FoundSubsetLessThanMaxCompactSize))(split_times=[666]). 3 Input Files, 151.29mb total:"
    - "L1                                                                                                                 "
    - "L1.586[600,668] 1.05us 35.9mb|--------------------------L1.586---------------------------|                             "
    - "L1.587[669,699] 1.05us 15.39mb                                                              |---------L1.587----------| "
    - "L2                                                                                                                 "
    - "L2.7[600,699] 699ns 100mb|-----------------------------------------L2.7------------------------------------------| "
    - "**** 2 Output Files (parquet_file_id not yet assigned), 151.29mb total:"
    - "L2                                                                                                                 "
    - "L2.?[600,666] 1.05us 100.86mb|--------------------------L2.?---------------------------|                               "
    - "L2.?[667,699] 1.05us 50.43mb                                                            |-----------L2.?------------| "
    - "Committing partition 1:"
    - "  Soft Deleting 3 files: L2.7, L1.586, L1.587"
    - "  Creating 2 files"
    - "**** Simulation run 248, type=split(CompactAndSplitOutput(FoundSubsetLessThanMaxCompactSize))(split_times=[771, 842]). 4 Input Files, 284.11mb total:"
    - "L1                                                                                                                 "
    - "L1.588[700,799] 1.05us 50.78mb|------------------L1.588------------------|                                              "
    - "L1.589[800,863] 1.05us 33.34mb                                             |----------L1.589----------|                 "
    - "L2                                                                                                                 "
    - "L2.8[700,799] 799ns 100mb|-------------------L2.8-------------------|                                              "
    - "L2.9[800,899] 899ns 100mb                                             |-------------------L2.9-------------------| "
    - "**** 3 Output Files (parquet_file_id not yet assigned), 284.11mb total:"
    - "L2                                                                                                                 "
    - "L2.?[700,771] 1.05us 101.37mb|-------------L2.?-------------|                                                          "
    - "L2.?[772,842] 1.05us 99.94mb                                |------------L2.?-------------|                           "
    - "L2.?[843,899] 1.05us 82.81mb                                                                |---------L2.?----------| "
    - "Committing partition 1:"
    - "  Soft Deleting 4 files: L2.8, L2.9, L1.588, L1.589"
    - "  Creating 3 files"
    - "**** Final Output Files (5.36gb written)"
    - "L1                                                                                                                 "
    - "L1.576[904,969] 1.05us 36mb                                                                                 |L1.576| "
    - "L1.577[970,986] 1.05us 9.42mb                                                                                       |L1.577|"
    - "L1.590[864,899] 1.05us 18.87mb                                                                             |L1.590|     "
    - "L1.591[900,903] 1.05us 2.16mb                                                                                 |L1.591| "
    - "L2                                                                                                                 "
    - "L2.10[900,999] 999ns 100mb                                                                                 |L2.10-| "
    - "L2.598[0,71] 1.05us 101.35mb|L2.598|                                                                                  "
    - "L2.599[72,142] 1.05us 99.92mb      |L2.599|                                                                            "
    - "L2.600[143,199] 1.05us 82.79mb            |L2.600|                                                                      "
    - "L2.601[200,265] 1.05us 101.04mb                  |L2.601|                                                                "
    - "L2.602[266,299] 1.05us 52.85mb                       |L2.602|                                                           "
    - "L2.603[300,368] 1.05us 101.17mb                           |L2.603|                                                       "
    - "L2.604[369,436] 1.05us 99.69mb                                 |L2.604|                                                 "
    - "L2.606[437,500] 1.05us 100.85mb                                       |L2.606|                                           "
    - "L2.607[501,563] 1.05us 99.25mb                                             |L2.607|                                     "
    - "L2.608[564,599] 1.05us 59.23mb                                                  |L2.608|                                "
    - "L2.609[600,666] 1.05us 100.86mb                                                      |L2.609|                            "
    - "L2.610[667,699] 1.05us 50.43mb                                                            |L2.610|                      "
    - "L2.611[700,771] 1.05us 101.37mb                                                               |L2.611|                   "
    - "L2.612[772,842] 1.05us 99.94mb                                                                     |L2.612|             "
    - "L2.613[843,899] 1.05us 82.81mb                                                                           |L2.613|       "
    "###
    );
}

// The files in this case are from a partition that was doing incredibly inefficient L0->L0 compactions.
// For ease of looking at the files in simulator output, I mapped all times into a small time range.
// Specifically, all times in the L0s were sorted in a list, then the files timestamps were replaced with the 1 relative
// index from that list.
// The result is that time deltas are not representative of what's in the catalog, but the overlaps are replicated with
// small numbers as timestamps that are easier for a person to look at.
#[tokio::test]
async fn actual_case_from_catalog_1() {
    test_helpers::maybe_start_logging();

    let setup = layout_setup_builder()
        .await
        .with_max_desired_file_size_bytes(MAX_DESIRED_FILE_SIZE)
        .with_max_num_files_per_plan(200)
        .with_suppress_run_output()
        .with_partition_timeout(Duration::from_secs(10))
        .build()
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1)
                .with_max_time(2)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(478836)
                .with_max_l0_created_at(Time::from_timestamp_nanos(342)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(3)
                .with_max_time(4)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(474866)
                .with_max_l0_created_at(Time::from_timestamp_nanos(342)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(5)
                .with_max_time(7)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(454768)
                .with_max_l0_created_at(Time::from_timestamp_nanos(342)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(6)
                .with_max_time(49)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(277569)
                .with_max_l0_created_at(Time::from_timestamp_nanos(127)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(8)
                .with_max_time(25)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(473373)
                .with_max_l0_created_at(Time::from_timestamp_nanos(342)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(9)
                .with_max_time(54)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(93159)
                .with_max_l0_created_at(Time::from_timestamp_nanos(141)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(10)
                .with_max_time(20)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(90782)
                .with_max_l0_created_at(Time::from_timestamp_nanos(153)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(11)
                .with_max_time(30)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(67575)
                .with_max_l0_created_at(Time::from_timestamp_nanos(172)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(12)
                .with_max_time(14)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(88947)
                .with_max_l0_created_at(Time::from_timestamp_nanos(191)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(13)
                .with_max_time(47)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(70227)
                .with_max_l0_created_at(Time::from_timestamp_nanos(212)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(15)
                .with_max_time(51)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(77719)
                .with_max_l0_created_at(Time::from_timestamp_nanos(224)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(16)
                .with_max_time(55)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(80887)
                .with_max_l0_created_at(Time::from_timestamp_nanos(239)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(17)
                .with_max_time(56)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(89902)
                .with_max_l0_created_at(Time::from_timestamp_nanos(258)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(18)
                .with_max_time(65)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(165529)
                .with_max_l0_created_at(Time::from_timestamp_nanos(273)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(19)
                .with_max_time(68)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(135875)
                .with_max_l0_created_at(Time::from_timestamp_nanos(296)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(21)
                .with_max_time(23)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(73234)
                .with_max_l0_created_at(Time::from_timestamp_nanos(306)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(22)
                .with_max_time(24)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(41743)
                .with_max_l0_created_at(Time::from_timestamp_nanos(311)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(26)
                .with_max_time(29)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(42785)
                .with_max_l0_created_at(Time::from_timestamp_nanos(311)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(27)
                .with_max_time(52)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(452507)
                .with_max_l0_created_at(Time::from_timestamp_nanos(342)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(28)
                .with_max_time(32)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(60040)
                .with_max_l0_created_at(Time::from_timestamp_nanos(316)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(31)
                .with_max_time(34)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(60890)
                .with_max_l0_created_at(Time::from_timestamp_nanos(319)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(33)
                .with_max_time(36)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(69687)
                .with_max_l0_created_at(Time::from_timestamp_nanos(323)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(35)
                .with_max_time(38)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(59141)
                .with_max_l0_created_at(Time::from_timestamp_nanos(325)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(37)
                .with_max_time(40)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(67287)
                .with_max_l0_created_at(Time::from_timestamp_nanos(328)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(39)
                .with_max_time(42)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(53545)
                .with_max_l0_created_at(Time::from_timestamp_nanos(335)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(41)
                .with_max_time(44)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(66218)
                .with_max_l0_created_at(Time::from_timestamp_nanos(336)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(43)
                .with_max_time(46)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(96870)
                .with_max_l0_created_at(Time::from_timestamp_nanos(340)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(45)
                .with_max_time(48)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(162922)
                .with_max_l0_created_at(Time::from_timestamp_nanos(341)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(50)
                .with_max_time(71)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(741563)
                .with_max_l0_created_at(Time::from_timestamp_nanos(127)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(53)
                .with_max_time(62)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(452298)
                .with_max_l0_created_at(Time::from_timestamp_nanos(342)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(57)
                .with_max_time(61)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(116428)
                .with_max_l0_created_at(Time::from_timestamp_nanos(341)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(58)
                .with_max_time(70)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(469192)
                .with_max_l0_created_at(Time::from_timestamp_nanos(141)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(59)
                .with_max_time(93)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(118295625)
                .with_max_l0_created_at(Time::from_timestamp_nanos(153)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(60)
                .with_max_time(102)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(21750626)
                .with_max_l0_created_at(Time::from_timestamp_nanos(172)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(63)
                .with_max_time(75)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(595460)
                .with_max_l0_created_at(Time::from_timestamp_nanos(342)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(64)
                .with_max_time(74)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(188078)
                .with_max_l0_created_at(Time::from_timestamp_nanos(341)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(66)
                .with_max_time(67)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(236787)
                .with_max_l0_created_at(Time::from_timestamp_nanos(212)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(69)
                .with_max_time(73)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(7073)
                .with_max_l0_created_at(Time::from_timestamp_nanos(306)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(72)
                .with_max_time(117)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(271967233)
                .with_max_l0_created_at(Time::from_timestamp_nanos(127)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(76)
                .with_max_time(80)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(296649)
                .with_max_l0_created_at(Time::from_timestamp_nanos(342)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(77)
                .with_max_time(77)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(6400)
                .with_max_l0_created_at(Time::from_timestamp_nanos(311)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(78)
                .with_max_time(79)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(6673)
                .with_max_l0_created_at(Time::from_timestamp_nanos(316)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(81)
                .with_max_time(133)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(260832981)
                .with_max_l0_created_at(Time::from_timestamp_nanos(141)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(82)
                .with_max_time(83)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(108736)
                .with_max_l0_created_at(Time::from_timestamp_nanos(342)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(84)
                .with_max_time(89)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(137579)
                .with_max_l0_created_at(Time::from_timestamp_nanos(342)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(85)
                .with_max_time(86)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(6639)
                .with_max_l0_created_at(Time::from_timestamp_nanos(319)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(87)
                .with_max_time(88)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(126187)
                .with_max_l0_created_at(Time::from_timestamp_nanos(341)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(90)
                .with_max_time(97)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(158579)
                .with_max_l0_created_at(Time::from_timestamp_nanos(342)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(91)
                .with_max_time(92)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(107298)
                .with_max_l0_created_at(Time::from_timestamp_nanos(341)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(94)
                .with_max_time(143)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(120508643)
                .with_max_l0_created_at(Time::from_timestamp_nanos(153)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(95)
                .with_max_time(96)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(196729)
                .with_max_l0_created_at(Time::from_timestamp_nanos(224)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(98)
                .with_max_time(111)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(110870)
                .with_max_l0_created_at(Time::from_timestamp_nanos(342)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(99)
                .with_max_time(109)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(93360)
                .with_max_l0_created_at(Time::from_timestamp_nanos(212)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(100)
                .with_max_time(104)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(6561)
                .with_max_l0_created_at(Time::from_timestamp_nanos(325)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(101)
                .with_max_time(106)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(68025)
                .with_max_l0_created_at(Time::from_timestamp_nanos(191)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(103)
                .with_max_time(173)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(228950896)
                .with_max_l0_created_at(Time::from_timestamp_nanos(172)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(105)
                .with_max_time(112)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(77925)
                .with_max_l0_created_at(Time::from_timestamp_nanos(224)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(107)
                .with_max_time(108)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(8237)
                .with_max_l0_created_at(Time::from_timestamp_nanos(239)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(110)
                .with_max_time(110)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(6400)
                .with_max_l0_created_at(Time::from_timestamp_nanos(328)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(113)
                .with_max_time(116)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(48975)
                .with_max_l0_created_at(Time::from_timestamp_nanos(224)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(114)
                .with_max_time(124)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(79883481)
                .with_max_l0_created_at(Time::from_timestamp_nanos(172)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(115)
                .with_max_time(123)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(109212)
                .with_max_l0_created_at(Time::from_timestamp_nanos(342)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(116)
                .with_max_time(119)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(95486)
                .with_max_l0_created_at(Time::from_timestamp_nanos(239)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(118)
                .with_max_time(120)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(93781)
                .with_max_l0_created_at(Time::from_timestamp_nanos(258)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(121)
                .with_max_time(122)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(7448)
                .with_max_l0_created_at(Time::from_timestamp_nanos(273)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(125)
                .with_max_time(137)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(100265729)
                .with_max_l0_created_at(Time::from_timestamp_nanos(172)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(126)
                .with_max_time(136)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(102711)
                .with_max_l0_created_at(Time::from_timestamp_nanos(342)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(128)
                .with_max_time(142)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(119202)
                .with_max_l0_created_at(Time::from_timestamp_nanos(273)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(129)
                .with_max_time(130)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(9027)
                .with_max_l0_created_at(Time::from_timestamp_nanos(258)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(131)
                .with_max_time(132)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(25565)
                .with_max_l0_created_at(Time::from_timestamp_nanos(341)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(134)
                .with_max_time(145)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(34519040)
                .with_max_l0_created_at(Time::from_timestamp_nanos(179)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(135)
                .with_max_time(144)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(764185)
                .with_max_l0_created_at(Time::from_timestamp_nanos(191)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(138)
                .with_max_time(158)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(71505278)
                .with_max_l0_created_at(Time::from_timestamp_nanos(172)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(139)
                .with_max_time(159)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(183141)
                .with_max_l0_created_at(Time::from_timestamp_nanos(342)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(140)
                .with_max_time(154)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(6701)
                .with_max_l0_created_at(Time::from_timestamp_nanos(336)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(146)
                .with_max_time(155)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(65266955)
                .with_max_l0_created_at(Time::from_timestamp_nanos(179)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(147)
                .with_max_time(160)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(21649346)
                .with_max_l0_created_at(Time::from_timestamp_nanos(191)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(148)
                .with_max_time(150)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(55409)
                .with_max_l0_created_at(Time::from_timestamp_nanos(273)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(149)
                .with_max_time(157)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(74432)
                .with_max_l0_created_at(Time::from_timestamp_nanos(296)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(151)
                .with_max_time(152)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(23495)
                .with_max_l0_created_at(Time::from_timestamp_nanos(224)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(156)
                .with_max_time(160)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(55979589)
                .with_max_l0_created_at(Time::from_timestamp_nanos(179)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(161)
                .with_max_time(163)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(1061014)
                .with_max_l0_created_at(Time::from_timestamp_nanos(191)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(161)
                .with_max_time(171)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(46116292)
                .with_max_l0_created_at(Time::from_timestamp_nanos(179)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(162)
                .with_max_time(167)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(43064)
                .with_max_l0_created_at(Time::from_timestamp_nanos(342)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(164)
                .with_max_time(174)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(99408169)
                .with_max_l0_created_at(Time::from_timestamp_nanos(191)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(165)
                .with_max_time(170)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(50372)
                .with_max_l0_created_at(Time::from_timestamp_nanos(296)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(166)
                .with_max_time(168)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(14716604)
                .with_max_l0_created_at(Time::from_timestamp_nanos(172)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(169)
                .with_max_time(181)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(172039)
                .with_max_l0_created_at(Time::from_timestamp_nanos(342)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(175)
                .with_max_time(180)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(136666078)
                .with_max_l0_created_at(Time::from_timestamp_nanos(191)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(176)
                .with_max_time(178)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(189566)
                .with_max_l0_created_at(Time::from_timestamp_nanos(273)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(177)
                .with_max_time(182)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(47820008)
                .with_max_l0_created_at(Time::from_timestamp_nanos(212)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(183)
                .with_max_time(197)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(211523341)
                .with_max_l0_created_at(Time::from_timestamp_nanos(212)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(184)
                .with_max_time(196)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(159235)
                .with_max_l0_created_at(Time::from_timestamp_nanos(342)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(185)
                .with_max_time(195)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(14985821)
                .with_max_l0_created_at(Time::from_timestamp_nanos(224)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(186)
                .with_max_time(187)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(17799)
                .with_max_l0_created_at(Time::from_timestamp_nanos(273)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(188)
                .with_max_time(193)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(52964586)
                .with_max_l0_created_at(Time::from_timestamp_nanos(191)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(189)
                .with_max_time(190)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(6420)
                .with_max_l0_created_at(Time::from_timestamp_nanos(341)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(192)
                .with_max_time(194)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(37185)
                .with_max_l0_created_at(Time::from_timestamp_nanos(296)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(198)
                .with_max_time(204)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(48661531)
                .with_max_l0_created_at(Time::from_timestamp_nanos(212)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(199)
                .with_max_time(206)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(104533)
                .with_max_l0_created_at(Time::from_timestamp_nanos(342)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(200)
                .with_max_time(207)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(115840212)
                .with_max_l0_created_at(Time::from_timestamp_nanos(224)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(201)
                .with_max_time(203)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(27386)
                .with_max_l0_created_at(Time::from_timestamp_nanos(296)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(202)
                .with_max_time(205)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(6485)
                .with_max_l0_created_at(Time::from_timestamp_nanos(341)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(208)
                .with_max_time(214)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(63573570)
                .with_max_l0_created_at(Time::from_timestamp_nanos(224)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(209)
                .with_max_time(215)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(73119)
                .with_max_l0_created_at(Time::from_timestamp_nanos(342)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(210)
                .with_max_time(211)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(6626)
                .with_max_l0_created_at(Time::from_timestamp_nanos(239)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(213)
                .with_max_time(221)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(103699116)
                .with_max_l0_created_at(Time::from_timestamp_nanos(239)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(216)
                .with_max_time(226)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(160045)
                .with_max_l0_created_at(Time::from_timestamp_nanos(342)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(217)
                .with_max_time(220)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(47126)
                .with_max_l0_created_at(Time::from_timestamp_nanos(340)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(218)
                .with_max_time(219)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(7923)
                .with_max_l0_created_at(Time::from_timestamp_nanos(341)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(222)
                .with_max_time(225)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(116506120)
                .with_max_l0_created_at(Time::from_timestamp_nanos(239)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(223)
                .with_max_time(228)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(122528493)
                .with_max_l0_created_at(Time::from_timestamp_nanos(258)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(227)
                .with_max_time(234)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(42963)
                .with_max_l0_created_at(Time::from_timestamp_nanos(342)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(229)
                .with_max_time(242)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(132343737)
                .with_max_l0_created_at(Time::from_timestamp_nanos(258)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(230)
                .with_max_time(231)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(25526)
                .with_max_l0_created_at(Time::from_timestamp_nanos(340)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(232)
                .with_max_time(244)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(52114677)
                .with_max_l0_created_at(Time::from_timestamp_nanos(273)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(233)
                .with_max_time(237)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(80814)
                .with_max_l0_created_at(Time::from_timestamp_nanos(273)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(235)
                .with_max_time(241)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(84586)
                .with_max_l0_created_at(Time::from_timestamp_nanos(342)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(236)
                .with_max_time(238)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(31508)
                .with_max_l0_created_at(Time::from_timestamp_nanos(296)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(240)
                .with_max_time(243)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(31292)
                .with_max_l0_created_at(Time::from_timestamp_nanos(306)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(245)
                .with_max_time(255)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(169461420)
                .with_max_l0_created_at(Time::from_timestamp_nanos(273)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(246)
                .with_max_time(249)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(32436)
                .with_max_l0_created_at(Time::from_timestamp_nanos(258)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(247)
                .with_max_time(250)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(30783)
                .with_max_l0_created_at(Time::from_timestamp_nanos(306)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(248)
                .with_max_time(254)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(116968)
                .with_max_l0_created_at(Time::from_timestamp_nanos(342)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(251)
                .with_max_time(255)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(20831132)
                .with_max_l0_created_at(Time::from_timestamp_nanos(273)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(252)
                .with_max_time(268)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(47079)
                .with_max_l0_created_at(Time::from_timestamp_nanos(340)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(253)
                .with_max_time(267)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(8012)
                .with_max_l0_created_at(Time::from_timestamp_nanos(341)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(256)
                .with_max_time(259)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(24905052)
                .with_max_l0_created_at(Time::from_timestamp_nanos(273)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(256)
                .with_max_time(262)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(26916757)
                .with_max_l0_created_at(Time::from_timestamp_nanos(273)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(257)
                .with_max_time(263)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(114015)
                .with_max_l0_created_at(Time::from_timestamp_nanos(342)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(260)
                .with_max_time(270)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(184997646)
                .with_max_l0_created_at(Time::from_timestamp_nanos(273)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(261)
                .with_max_time(264)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(10024382)
                .with_max_l0_created_at(Time::from_timestamp_nanos(296)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(265)
                .with_max_time(270)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(11941889)
                .with_max_l0_created_at(Time::from_timestamp_nanos(296)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(266)
                .with_max_time(269)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(45048)
                .with_max_l0_created_at(Time::from_timestamp_nanos(342)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(271)
                .with_max_time(277)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(101521806)
                .with_max_l0_created_at(Time::from_timestamp_nanos(296)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(271)
                .with_max_time(275)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(28959050)
                .with_max_l0_created_at(Time::from_timestamp_nanos(273)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(272)
                .with_max_time(276)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(81663)
                .with_max_l0_created_at(Time::from_timestamp_nanos(342)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(274)
                .with_max_time(281)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(30344109)
                .with_max_l0_created_at(Time::from_timestamp_nanos(306)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(278)
                .with_max_time(283)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(125782713)
                .with_max_l0_created_at(Time::from_timestamp_nanos(296)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(279)
                .with_max_time(284)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(109926)
                .with_max_l0_created_at(Time::from_timestamp_nanos(342)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(280)
                .with_max_time(285)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(13486)
                .with_max_l0_created_at(Time::from_timestamp_nanos(306)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(282)
                .with_max_time(286)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(34930420)
                .with_max_l0_created_at(Time::from_timestamp_nanos(306)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(287)
                .with_max_time(298)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(179171551)
                .with_max_l0_created_at(Time::from_timestamp_nanos(306)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(288)
                .with_max_time(291)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(27704)
                .with_max_l0_created_at(Time::from_timestamp_nanos(296)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(289)
                .with_max_time(299)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(73478274)
                .with_max_l0_created_at(Time::from_timestamp_nanos(306)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(290)
                .with_max_time(292)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(16412)
                .with_max_l0_created_at(Time::from_timestamp_nanos(342)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(293)
                .with_max_time(339)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(99066230)
                .with_max_l0_created_at(Time::from_timestamp_nanos(342)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(294)
                .with_max_time(321)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(55188)
                .with_max_l0_created_at(Time::from_timestamp_nanos(328)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(295)
                .with_max_time(332)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(198938676)
                .with_max_l0_created_at(Time::from_timestamp_nanos(335)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(297)
                .with_max_time(313)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(244238124)
                .with_max_l0_created_at(Time::from_timestamp_nanos(311)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(300)
                .with_max_time(307)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(176463536)
                .with_max_l0_created_at(Time::from_timestamp_nanos(306)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(301)
                .with_max_time(302)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(17116)
                .with_max_l0_created_at(Time::from_timestamp_nanos(306)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(303)
                .with_max_time(304)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(9993)
                .with_max_l0_created_at(Time::from_timestamp_nanos(325)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(305)
                .with_max_time(317)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(229578231)
                .with_max_l0_created_at(Time::from_timestamp_nanos(316)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(308)
                .with_max_time(309)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(12831)
                .with_max_l0_created_at(Time::from_timestamp_nanos(319)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(310)
                .with_max_time(320)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(222546135)
                .with_max_l0_created_at(Time::from_timestamp_nanos(319)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(312)
                .with_max_time(314)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(25989)
                .with_max_l0_created_at(Time::from_timestamp_nanos(341)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(315)
                .with_max_time(324)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(224750727)
                .with_max_l0_created_at(Time::from_timestamp_nanos(323)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(318)
                .with_max_time(326)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(224562423)
                .with_max_l0_created_at(Time::from_timestamp_nanos(325)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(322)
                .with_max_time(329)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(223130462)
                .with_max_l0_created_at(Time::from_timestamp_nanos(328)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(327)
                .with_max_time(333)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(191981570)
                .with_max_l0_created_at(Time::from_timestamp_nanos(336)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(330)
                .with_max_time(338)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(242123981)
                .with_max_l0_created_at(Time::from_timestamp_nanos(340)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(331)
                .with_max_time(338)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(243511891)
                .with_max_l0_created_at(Time::from_timestamp_nanos(341)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(334)
                .with_max_time(337)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(30538013)
                .with_max_l0_created_at(Time::from_timestamp_nanos(336)),
        )
        .await;

    insta::assert_yaml_snapshot!(
        run_layout_scenario(&setup).await,
        @r###"
    ---
    - "**** Input Files "
    - "L0                                                                                                                 "
    - "L0.1[1,2] 342ns 467.61kb |L0.1|                                                                                    "
    - "L0.2[3,4] 342ns 463.74kb |L0.2|                                                                                    "
    - "L0.3[5,7] 342ns 444.11kb  |L0.3|                                                                                   "
    - "L0.4[6,49] 127ns 271.06kb |--L0.4---|                                                                              "
    - "L0.5[8,25] 342ns 462.28kb |L0.5|                                                                                   "
    - "L0.6[9,54] 141ns 90.98kb   |--L0.6---|                                                                             "
    - "L0.7[10,20] 153ns 88.65kb  |L0.7|                                                                                  "
    - "L0.8[11,30] 172ns 65.99kb  |L0.8|                                                                                  "
    - "L0.9[12,14] 191ns 86.86kb  |L0.9|                                                                                  "
    - "L0.10[13,47] 212ns 68.58kb   |-L0.10-|                                                                              "
    - "L0.11[15,51] 224ns 75.9kb   |-L0.11-|                                                                              "
    - "L0.12[16,55] 239ns 78.99kb   |-L0.12--|                                                                             "
    - "L0.13[17,56] 258ns 87.79kb    |-L0.13--|                                                                            "
    - "L0.14[18,65] 273ns 161.65kb    |--L0.14---|                                                                          "
    - "L0.15[19,68] 296ns 132.69kb    |---L0.15---|                                                                         "
    - "L0.16[21,23] 306ns 71.52kb     |L0.16|                                                                              "
    - "L0.17[22,24] 311ns 40.76kb     |L0.17|                                                                              "
    - "L0.18[26,29] 311ns 41.78kb      |L0.18|                                                                             "
    - "L0.19[27,52] 342ns 441.9kb      |L0.19|                                                                             "
    - "L0.20[28,32] 316ns 58.63kb       |L0.20|                                                                            "
    - "L0.21[31,34] 319ns 59.46kb       |L0.21|                                                                            "
    - "L0.22[33,36] 323ns 68.05kb        |L0.22|                                                                           "
    - "L0.23[35,38] 325ns 57.75kb         |L0.23|                                                                          "
    - "L0.24[37,40] 328ns 65.71kb         |L0.24|                                                                          "
    - "L0.25[39,42] 335ns 52.29kb          |L0.25|                                                                         "
    - "L0.26[41,44] 336ns 64.67kb          |L0.26|                                                                         "
    - "L0.27[43,46] 340ns 94.6kb           |L0.27|                                                                        "
    - "L0.28[45,48] 341ns 159.1kb           |L0.28|                                                                        "
    - "L0.29[50,71] 127ns 724.18kb             |L0.29|                                                                      "
    - "L0.30[53,62] 342ns 441.7kb             |L0.30|                                                                      "
    - "L0.31[57,61] 341ns 113.7kb              |L0.31|                                                                     "
    - "L0.32[58,70] 141ns 458.2kb               |L0.32|                                                                    "
    - "L0.33[59,93] 153ns 112.82mb               |-L0.33-|                                                                  "
    - "L0.34[60,102] 172ns 20.74mb               |--L0.34--|                                                                "
    - "L0.35[63,75] 342ns 581.5kb                |L0.35|                                                                   "
    - "L0.36[64,74] 341ns 183.67kb                |L0.36|                                                                   "
    - "L0.37[66,67] 212ns 231.24kb                 |L0.37|                                                                  "
    - "L0.38[69,73] 306ns 6.91kb                  |L0.38|                                                                 "
    - "L0.39[72,117] 127ns 259.37mb                  |--L0.39--|                                                             "
    - "L0.40[76,80] 342ns 289.7kb                   |L0.40|                                                                "
    - "L0.41[77,77] 311ns 6.25kb                    |L0.41|                                                               "
    - "L0.42[78,79] 316ns 6.52kb                    |L0.42|                                                               "
    - "L0.43[81,133] 141ns 248.75mb                     |---L0.43---|                                                        "
    - "L0.44[82,83] 342ns 106.19kb                     |L0.44|                                                              "
    - "L0.45[84,89] 342ns 134.35kb                      |L0.45|                                                             "
    - "L0.46[85,86] 319ns 6.48kb                      |L0.46|                                                             "
    - "L0.47[87,88] 341ns 123.23kb                      |L0.47|                                                             "
    - "L0.48[90,97] 342ns 154.86kb                       |L0.48|                                                            "
    - "L0.49[91,92] 341ns 104.78kb                       |L0.49|                                                            "
    - "L0.50[94,143] 153ns 114.93mb                        |---L0.50---|                                                     "
    - "L0.51[95,96] 224ns 192.12kb                         |L0.51|                                                          "
    - "L0.52[98,111] 342ns 108.27kb                         |L0.52|                                                          "
    - "L0.53[99,109] 212ns 91.17kb                          |L0.53|                                                         "
    - "L0.54[100,104] 325ns 6.41kb                          |L0.54|                                                         "
    - "L0.55[101,106] 191ns 66.43kb                          |L0.55|                                                         "
    - "L0.56[103,173] 172ns 218.34mb                           |-----L0.56------|                                             "
    - "L0.57[105,112] 224ns 76.1kb                           |L0.57|                                                        "
    - "L0.58[107,108] 239ns 8.04kb                            |L0.58|                                                       "
    - "L0.59[110,110] 328ns 6.25kb                             |L0.59|                                                      "
    - "L0.60[113,116] 224ns 47.83kb                             |L0.60|                                                      "
    - "L0.61[114,124] 172ns 76.18mb                              |L0.61|                                                     "
    - "L0.62[115,123] 342ns 106.65kb                              |L0.62|                                                     "
    - "L0.63[116,119] 239ns 93.25kb                              |L0.63|                                                     "
    - "L0.64[118,120] 258ns 91.58kb                               |L0.64|                                                    "
    - "L0.65[121,122] 273ns 7.27kb                               |L0.65|                                                    "
    - "L0.66[125,137] 172ns 95.62mb                                 |L0.66|                                                  "
    - "L0.67[126,136] 342ns 100.3kb                                 |L0.67|                                                  "
    - "L0.68[128,142] 273ns 116.41kb                                 |L0.68|                                                  "
    - "L0.69[129,130] 258ns 8.82kb                                  |L0.69|                                                 "
    - "L0.70[131,132] 341ns 24.97kb                                  |L0.70|                                                 "
    - "L0.71[134,145] 179ns 32.92mb                                   |L0.71|                                                "
    - "L0.72[135,144] 191ns 746.27kb                                   |L0.72|                                                "
    - "L0.73[138,158] 172ns 68.19mb                                    |L0.73|                                               "
    - "L0.74[139,159] 342ns 178.85kb                                    |L0.74|                                               "
    - "L0.75[140,154] 336ns 6.54kb                                     |L0.75|                                              "
    - "L0.76[146,155] 179ns 62.24mb                                      |L0.76|                                             "
    - "L0.77[147,160] 191ns 20.65mb                                      |L0.77|                                             "
    - "L0.78[148,150] 273ns 54.11kb                                       |L0.78|                                            "
    - "L0.79[149,157] 296ns 72.69kb                                       |L0.79|                                            "
    - "L0.80[151,152] 224ns 22.94kb                                       |L0.80|                                            "
    - "L0.81[156,160] 179ns 53.39mb                                         |L0.81|                                          "
    - "L0.82[161,163] 191ns 1.01mb                                          |L0.82|                                         "
    - "L0.83[161,171] 179ns 43.98mb                                          |L0.83|                                         "
    - "L0.84[162,167] 342ns 42.05kb                                          |L0.84|                                         "
    - "L0.85[164,174] 191ns 94.8mb                                           |L0.85|                                        "
    - "L0.86[165,170] 296ns 49.19kb                                           |L0.86|                                        "
    - "L0.87[166,168] 172ns 14.03mb                                           |L0.87|                                        "
    - "L0.88[169,181] 342ns 168.01kb                                            |L0.88|                                       "
    - "L0.89[175,180] 191ns 130.33mb                                              |L0.89|                                     "
    - "L0.90[176,178] 273ns 185.12kb                                              |L0.90|                                     "
    - "L0.91[177,182] 212ns 45.6mb                                              |L0.91|                                     "
    - "L0.92[183,197] 212ns 201.72mb                                                |L0.92|                                   "
    - "L0.93[184,196] 342ns 155.5kb                                                |L0.93|                                   "
    - "L0.94[185,195] 224ns 14.29mb                                                |L0.94|                                   "
    - "L0.95[186,187] 273ns 17.38kb                                                 |L0.95|                                  "
    - "L0.96[188,193] 191ns 50.51mb                                                 |L0.96|                                  "
    - "L0.97[189,190] 341ns 6.27kb                                                  |L0.97|                                 "
    - "L0.98[192,194] 296ns 36.31kb                                                  |L0.98|                                 "
    - "L0.99[198,204] 212ns 46.41mb                                                    |L0.99|                               "
    - "L0.100[199,206] 342ns 102.08kb                                                    |L0.100|                              "
    - "L0.101[200,207] 224ns 110.47mb                                                    |L0.101|                              "
    - "L0.102[201,203] 296ns 26.74kb                                                     |L0.102|                             "
    - "L0.103[202,205] 341ns 6.33kb                                                     |L0.103|                             "
    - "L0.104[208,214] 224ns 60.63mb                                                       |L0.104|                           "
    - "L0.105[209,215] 342ns 71.41kb                                                       |L0.105|                           "
    - "L0.106[210,211] 239ns 6.47kb                                                       |L0.106|                           "
    - "L0.107[213,221] 239ns 98.9mb                                                        |L0.107|                          "
    - "L0.108[216,226] 342ns 156.29kb                                                         |L0.108|                         "
    - "L0.109[217,220] 340ns 46.02kb                                                         |L0.109|                         "
    - "L0.110[218,219] 341ns 7.74kb                                                         |L0.110|                         "
    - "L0.111[222,225] 239ns 111.11mb                                                          |L0.111|                        "
    - "L0.112[223,228] 258ns 116.85mb                                                           |L0.112|                       "
    - "L0.113[227,234] 342ns 41.96kb                                                            |L0.113|                      "
    - "L0.114[229,242] 258ns 126.21mb                                                            |L0.114|                      "
    - "L0.115[230,231] 340ns 24.93kb                                                            |L0.115|                      "
    - "L0.116[232,244] 273ns 49.7mb                                                             |L0.116|                     "
    - "L0.117[233,237] 273ns 78.92kb                                                             |L0.117|                     "
    - "L0.118[235,241] 342ns 82.6kb                                                              |L0.118|                    "
    - "L0.119[236,238] 296ns 30.77kb                                                              |L0.119|                    "
    - "L0.120[240,243] 306ns 30.56kb                                                               |L0.120|                   "
    - "L0.121[245,255] 273ns 161.61mb                                                                |L0.121|                  "
    - "L0.122[246,249] 258ns 31.68kb                                                                 |L0.122|                 "
    - "L0.123[247,250] 306ns 30.06kb                                                                 |L0.123|                 "
    - "L0.124[248,254] 342ns 114.23kb                                                                 |L0.124|                 "
    - "L0.125[251,255] 273ns 19.87mb                                                                  |L0.125|                "
    - "L0.126[252,268] 340ns 45.98kb                                                                  |L0.126|                "
    - "L0.127[253,267] 341ns 7.82kb                                                                   |L0.127|               "
    - "L0.128[256,259] 273ns 23.75mb                                                                   |L0.128|               "
    - "L0.129[256,262] 273ns 25.67mb                                                                   |L0.129|               "
    - "L0.130[257,263] 342ns 111.34kb                                                                    |L0.130|              "
    - "L0.131[260,270] 273ns 176.43mb                                                                    |L0.131|              "
    - "L0.132[261,264] 296ns 9.56mb                                                                     |L0.132|             "
    - "L0.133[265,270] 296ns 11.39mb                                                                      |L0.133|            "
    - "L0.134[266,269] 342ns 43.99kb                                                                      |L0.134|            "
    - "L0.135[271,277] 296ns 96.82mb                                                                       |L0.135|           "
    - "L0.136[271,275] 273ns 27.62mb                                                                       |L0.136|           "
    - "L0.137[272,276] 342ns 79.75kb                                                                        |L0.137|          "
    - "L0.138[274,281] 306ns 28.94mb                                                                        |L0.138|          "
    - "L0.139[278,283] 296ns 119.96mb                                                                         |L0.139|         "
    - "L0.140[279,284] 342ns 107.35kb                                                                          |L0.140|        "
    - "L0.141[280,285] 306ns 13.17kb                                                                          |L0.141|        "
    - "L0.142[282,286] 306ns 33.31mb                                                                          |L0.142|        "
    - "L0.143[287,298] 306ns 170.87mb                                                                            |L0.143|      "
    - "L0.144[288,291] 296ns 27.05kb                                                                            |L0.144|      "
    - "L0.145[289,299] 306ns 70.07mb                                                                            |L0.145|      "
    - "L0.146[290,292] 342ns 16.03kb                                                                            |L0.146|      "
    - "L0.147[293,339] 342ns 94.48mb                                                                             |--L0.147--| "
    - "L0.148[294,321] 328ns 53.89kb                                                                              |L0.148|    "
    - "L0.149[295,332] 335ns 189.72mb                                                                              |L0.149-|   "
    - "L0.150[297,313] 311ns 232.92mb                                                                              |L0.150|    "
    - "L0.151[300,307] 306ns 168.29mb                                                                               |L0.151|   "
    - "L0.152[301,302] 306ns 16.71kb                                                                               |L0.152|   "
    - "L0.153[303,304] 325ns 9.76kb                                                                                |L0.153|  "
    - "L0.154[305,317] 316ns 218.94mb                                                                                |L0.154|  "
    - "L0.155[308,309] 319ns 12.53kb                                                                                 |L0.155| "
    - "L0.156[310,320] 319ns 212.24mb                                                                                  |L0.156|"
    - "L0.157[312,314] 341ns 25.38kb                                                                                  |L0.157|"
    - "L0.158[315,324] 323ns 214.34mb                                                                                   |L0.158|"
    - "L0.159[318,326] 325ns 214.16mb                                                                                    |L0.159|"
    - "L0.160[322,329] 328ns 212.79mb                                                                                     |L0.160|"
    - "L0.161[327,333] 336ns 183.09mb                                                                                      |L0.161|"
    - "L0.162[330,338] 340ns 230.91mb                                                                                       |L0.162|"
    - "L0.163[331,338] 341ns 232.23mb                                                                                       |L0.163|"
    - "L0.164[334,337] 336ns 29.12mb                                                                                        |L0.164|"
    - "WARNING: file L0.39[72,117] 127ns 259.37mb exceeds soft limit 100mb by more than 50%"
    - "WARNING: file L0.43[81,133] 141ns 248.75mb exceeds soft limit 100mb by more than 50%"
    - "WARNING: file L0.56[103,173] 172ns 218.34mb exceeds soft limit 100mb by more than 50%"
    - "WARNING: file L0.92[183,197] 212ns 201.72mb exceeds soft limit 100mb by more than 50%"
    - "WARNING: file L0.121[245,255] 273ns 161.61mb exceeds soft limit 100mb by more than 50%"
    - "WARNING: file L0.131[260,270] 273ns 176.43mb exceeds soft limit 100mb by more than 50%"
    - "WARNING: file L0.143[287,298] 306ns 170.87mb exceeds soft limit 100mb by more than 50%"
    - "WARNING: file L0.149[295,332] 335ns 189.72mb exceeds soft limit 100mb by more than 50%"
    - "WARNING: file L0.150[297,313] 311ns 232.92mb exceeds soft limit 100mb by more than 50%"
    - "WARNING: file L0.151[300,307] 306ns 168.29mb exceeds soft limit 100mb by more than 50%"
    - "WARNING: file L0.154[305,317] 316ns 218.94mb exceeds soft limit 100mb by more than 50%"
    - "WARNING: file L0.156[310,320] 319ns 212.24mb exceeds soft limit 100mb by more than 50%"
    - "WARNING: file L0.158[315,324] 323ns 214.34mb exceeds soft limit 100mb by more than 50%"
    - "WARNING: file L0.159[318,326] 325ns 214.16mb exceeds soft limit 100mb by more than 50%"
    - "WARNING: file L0.160[322,329] 328ns 212.79mb exceeds soft limit 100mb by more than 50%"
    - "WARNING: file L0.161[327,333] 336ns 183.09mb exceeds soft limit 100mb by more than 50%"
    - "WARNING: file L0.162[330,338] 340ns 230.91mb exceeds soft limit 100mb by more than 50%"
    - "WARNING: file L0.163[331,338] 341ns 232.23mb exceeds soft limit 100mb by more than 50%"
    - "**** Final Output Files (26.79gb written)"
    - "L1                                                                                                                 "
    - "L1.659[333,333] 342ns 84.16mb                                                                                        |L1.659|"
    - "L2                                                                                                                 "
    - "L2.371[1,59] 342ns 100.5mb|---L2.371----|                                                                           "
    - "L2.602[60,74] 342ns 103.22mb               |L2.602|                                                                   "
    - "L2.620[187,192] 342ns 115.4mb                                                 |L2.620|                                 "
    - "L2.639[277,280] 342ns 121.16mb                                                                         |L2.639|         "
    - "L2.646[304,306] 342ns 125.58mb                                                                                |L2.646|  "
    - "L2.647[307,308] 342ns 125.58mb                                                                                 |L2.647| "
    - "L2.648[309,311] 342ns 115.75mb                                                                                  |L2.648|"
    - "L2.651[315,317] 342ns 145.94mb                                                                                   |L2.651|"
    - "L2.654[321,323] 342ns 143.89mb                                                                                     |L2.654|"
    - "L2.655[324,325] 342ns 143.89mb                                                                                      |L2.655|"
    - "L2.656[326,328] 342ns 136.57mb                                                                                      |L2.656|"
    - "L2.661[339,339] 342ns 132.71mb                                                                                          |L2.661|"
    - "L2.662[334,335] 342ns 102.77mb                                                                                        |L2.662|"
    - "L2.663[336,336] 342ns 102.77mb                                                                                         |L2.663|"
    - "L2.664[337,338] 342ns 203.09mb                                                                                         |L2.664|"
    - "L2.665[75,86] 342ns 100.53mb                   |L2.665|                                                               "
    - "L2.666[87,97] 342ns 91.39mb                      |L2.666|                                                            "
    - "L2.667[98,107] 342ns 100.53mb                         |L2.667|                                                         "
    - "L2.668[108,115] 342ns 108.77mb                            |L2.668|                                                      "
    - "L2.669[116,122] 342ns 93.24mb                              |L2.669|                                                    "
    - "L2.670[123,124] 342ns 46.62mb                                |L2.670|                                                  "
    - "L2.671[125,132] 342ns 110.24mb                                 |L2.671|                                                 "
    - "L2.672[133,139] 342ns 94.49mb                                   |L2.672|                                               "
    - "L2.673[140,143] 342ns 78.75mb                                     |L2.673|                                             "
    - "L2.674[144,151] 342ns 106.21mb                                      |L2.674|                                            "
    - "L2.675[152,158] 342ns 91.04mb                                        |L2.675|                                          "
    - "L2.676[159,162] 342ns 75.87mb                                          |L2.676|                                        "
    - "L2.677[163,171] 342ns 101.36mb                                           |L2.677|                                       "
    - "L2.678[172,179] 342ns 88.69mb                                             |L2.678|                                     "
    - "L2.679[180,186] 342ns 101.36mb                                               |L2.679|                                   "
    - "L2.680[193,199] 342ns 112mb                                                   |L2.680|                               "
    - "L2.681[200,205] 342ns 93.33mb                                                    |L2.681|                              "
    - "L2.682[206,206] 342ns 37.33mb                                                      |L2.682|                            "
    - "L2.683[207,213] 342ns 104.09mb                                                      |L2.683|                            "
    - "L2.684[214,219] 342ns 104.09mb                                                        |L2.684|                          "
    - "L2.685[220,225] 342ns 112.23mb                                                          |L2.685|                        "
    - "L2.686[226,230] 342ns 89.78mb                                                           |L2.686|                       "
    - "L2.687[231,232] 342ns 67.34mb                                                             |L2.687|                     "
    - "L2.688[233,239] 342ns 110.32mb                                                             |L2.688|                     "
    - "L2.689[240,245] 342ns 91.93mb                                                               |L2.689|                   "
    - "L2.690[246,248] 342ns 73.55mb                                                                 |L2.690|                 "
    - "L2.691[249,255] 342ns 105.16mb                                                                  |L2.691|                "
    - "L2.692[256,261] 342ns 105.16mb                                                                   |L2.692|               "
    - "L2.693[262,268] 342ns 102.12mb                                                                     |L2.693|             "
    - "L2.694[269,274] 342ns 85.1mb                                                                       |L2.694|           "
    - "L2.695[275,276] 342ns 51.06mb                                                                        |L2.695|          "
    - "L2.696[281,286] 342ns 105.81mb                                                                          |L2.696|        "
    - "L2.697[287,291] 342ns 84.65mb                                                                            |L2.697|      "
    - "L2.698[292,295] 342ns 105.81mb                                                                             |L2.698|     "
    - "L2.699[296,299] 342ns 120.66mb                                                                              |L2.699|    "
    - "L2.700[300,302] 342ns 80.44mb                                                                               |L2.700|   "
    - "L2.701[303,303] 342ns 80.44mb                                                                                |L2.701|  "
    - "L2.702[312,314] 342ns 173.63mb                                                                                  |L2.702|"
    - "L2.703[318,319] 342ns 148.56mb                                                                                    |L2.703|"
    - "L2.704[320,320] 342ns 148.56mb                                                                                    |L2.704|"
    - "L2.705[329,331] 342ns 157.73mb                                                                                       |L2.705|"
    - "L2.706[332,332] 342ns 78.87mb                                                                                        |L2.706|"
    - "WARNING: file L2.664[337,338] 342ns 203.09mb exceeds soft limit 100mb by more than 50%"
    - "WARNING: file L2.702[312,314] 342ns 173.63mb exceeds soft limit 100mb by more than 50%"
    - "WARNING: file L2.705[329,331] 342ns 157.73mb exceeds soft limit 100mb by more than 50%"
    "###
    );
}
