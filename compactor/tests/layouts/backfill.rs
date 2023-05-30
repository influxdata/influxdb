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
    - "L0.?[76,356] 1us 3mb     |-----------L0.?------------|                                                             "
    - "L0.?[357,670] 1us 4mb                                 |-------------L0.?-------------|                             "
    - "L0.?[671,932] 1us 3mb                                                                  |----------L0.?-----------| "
    - "**** Simulation run 1, type=split(HighL0OverlapSingleFile)(split_times=[356, 670]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.2[42,986] 1us         |------------------------------------------L0.2------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[42,356] 1us 3mb     |-----------L0.?------------|                                                             "
    - "L0.?[357,670] 1us 3mb                                  |-----------L0.?------------|                               "
    - "L0.?[671,986] 1us 3mb                                                               |------------L0.?------------| "
    - "**** Simulation run 2, type=split(HighL0OverlapSingleFile)(split_times=[356, 670]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.3[173,950] 1us        |------------------------------------------L0.3------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[173,356] 1us 2mb    |-------L0.?--------|                                                                     "
    - "L0.?[357,670] 1us 4mb                         |---------------L0.?---------------|                                 "
    - "L0.?[671,950] 1us 4mb                                                             |-------------L0.?-------------| "
    - "**** Simulation run 3, type=split(HighL0OverlapSingleFile)(split_times=[356]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.4[50,629] 1us         |------------------------------------------L0.4------------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[50,356] 1us 5mb     |--------------------L0.?---------------------|                                           "
    - "L0.?[357,629] 1us 5mb                                                   |------------------L0.?------------------| "
    - "**** Simulation run 4, type=split(HighL0OverlapSingleFile)(split_times=[356, 670]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.5[76,932] 1us         |------------------------------------------L0.5------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[76,356] 1us 3mb     |-----------L0.?------------|                                                             "
    - "L0.?[357,670] 1us 4mb                                 |-------------L0.?-------------|                             "
    - "L0.?[671,932] 1us 3mb                                                                  |----------L0.?-----------| "
    - "**** Simulation run 5, type=split(HighL0OverlapSingleFile)(split_times=[356, 670]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.6[42,986] 1us         |------------------------------------------L0.6------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[42,356] 1us 3mb     |-----------L0.?------------|                                                             "
    - "L0.?[357,670] 1us 3mb                                  |-----------L0.?------------|                               "
    - "L0.?[671,986] 1us 3mb                                                               |------------L0.?------------| "
    - "**** Simulation run 6, type=split(HighL0OverlapSingleFile)(split_times=[356, 670]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.7[173,950] 1.01us     |------------------------------------------L0.7------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[173,356] 1.01us 2mb |-------L0.?--------|                                                                     "
    - "L0.?[357,670] 1.01us 4mb                      |---------------L0.?---------------|                                 "
    - "L0.?[671,950] 1.01us 4mb                                                          |-------------L0.?-------------| "
    - "**** Simulation run 7, type=split(HighL0OverlapSingleFile)(split_times=[356]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.8[50,629] 1.01us      |------------------------------------------L0.8------------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[50,356] 1.01us 5mb  |--------------------L0.?---------------------|                                           "
    - "L0.?[357,629] 1.01us 5mb                                                |------------------L0.?------------------| "
    - "**** Simulation run 8, type=split(HighL0OverlapSingleFile)(split_times=[356, 670]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.9[76,932] 1.01us      |------------------------------------------L0.9------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[76,356] 1.01us 3mb  |-----------L0.?------------|                                                             "
    - "L0.?[357,670] 1.01us 4mb                              |-------------L0.?-------------|                             "
    - "L0.?[671,932] 1.01us 3mb                                                               |----------L0.?-----------| "
    - "**** Simulation run 9, type=split(HighL0OverlapSingleFile)(split_times=[356, 670]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.10[42,986] 1.01us     |-----------------------------------------L0.10------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[42,356] 1.01us 3mb  |-----------L0.?------------|                                                             "
    - "L0.?[357,670] 1.01us 3mb                               |-----------L0.?------------|                               "
    - "L0.?[671,986] 1.01us 3mb                                                            |------------L0.?------------| "
    - "**** Simulation run 10, type=split(HighL0OverlapSingleFile)(split_times=[356, 670]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.11[173,950] 1.01us    |-----------------------------------------L0.11------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[173,356] 1.01us 2mb |-------L0.?--------|                                                                     "
    - "L0.?[357,670] 1.01us 4mb                      |---------------L0.?---------------|                                 "
    - "L0.?[671,950] 1.01us 4mb                                                          |-------------L0.?-------------| "
    - "**** Simulation run 11, type=split(HighL0OverlapSingleFile)(split_times=[356]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.12[50,629] 1.01us     |-----------------------------------------L0.12------------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[50,356] 1.01us 5mb  |--------------------L0.?---------------------|                                           "
    - "L0.?[357,629] 1.01us 5mb                                                |------------------L0.?------------------| "
    - "**** Simulation run 12, type=split(HighL0OverlapSingleFile)(split_times=[356, 670]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.13[76,932] 1.01us     |-----------------------------------------L0.13------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[76,356] 1.01us 3mb  |-----------L0.?------------|                                                             "
    - "L0.?[357,670] 1.01us 4mb                              |-------------L0.?-------------|                             "
    - "L0.?[671,932] 1.01us 3mb                                                               |----------L0.?-----------| "
    - "**** Simulation run 13, type=split(HighL0OverlapSingleFile)(split_times=[356, 670]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.14[42,986] 1.01us     |-----------------------------------------L0.14------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[42,356] 1.01us 3mb  |-----------L0.?------------|                                                             "
    - "L0.?[357,670] 1.01us 3mb                               |-----------L0.?------------|                               "
    - "L0.?[671,986] 1.01us 3mb                                                            |------------L0.?------------| "
    - "**** Simulation run 14, type=split(HighL0OverlapSingleFile)(split_times=[356, 670]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.15[173,950] 1.01us    |-----------------------------------------L0.15------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[173,356] 1.01us 2mb |-------L0.?--------|                                                                     "
    - "L0.?[357,670] 1.01us 4mb                      |---------------L0.?---------------|                                 "
    - "L0.?[671,950] 1.01us 4mb                                                          |-------------L0.?-------------| "
    - "**** Simulation run 15, type=split(HighL0OverlapSingleFile)(split_times=[356]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.16[50,629] 1.01us     |-----------------------------------------L0.16------------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[50,356] 1.01us 5mb  |--------------------L0.?---------------------|                                           "
    - "L0.?[357,629] 1.01us 5mb                                                |------------------L0.?------------------| "
    - "**** Simulation run 16, type=split(HighL0OverlapSingleFile)(split_times=[356, 670]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.17[76,932] 1.02us     |-----------------------------------------L0.17------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[76,356] 1.02us 3mb  |-----------L0.?------------|                                                             "
    - "L0.?[357,670] 1.02us 4mb                              |-------------L0.?-------------|                             "
    - "L0.?[671,932] 1.02us 3mb                                                               |----------L0.?-----------| "
    - "**** Simulation run 17, type=split(HighL0OverlapSingleFile)(split_times=[356, 670]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.18[42,986] 1.02us     |-----------------------------------------L0.18------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[42,356] 1.02us 3mb  |-----------L0.?------------|                                                             "
    - "L0.?[357,670] 1.02us 3mb                               |-----------L0.?------------|                               "
    - "L0.?[671,986] 1.02us 3mb                                                            |------------L0.?------------| "
    - "**** Simulation run 18, type=split(HighL0OverlapSingleFile)(split_times=[356, 670]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.19[173,950] 1.02us    |-----------------------------------------L0.19------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[173,356] 1.02us 2mb |-------L0.?--------|                                                                     "
    - "L0.?[357,670] 1.02us 4mb                      |---------------L0.?---------------|                                 "
    - "L0.?[671,950] 1.02us 4mb                                                          |-------------L0.?-------------| "
    - "**** Simulation run 19, type=split(HighL0OverlapSingleFile)(split_times=[356]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.20[50,629] 1.02us     |-----------------------------------------L0.20------------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[50,356] 1.02us 5mb  |--------------------L0.?---------------------|                                           "
    - "L0.?[357,629] 1.02us 5mb                                                |------------------L0.?------------------| "
    - "**** Simulation run 20, type=split(HighL0OverlapSingleFile)(split_times=[356, 670]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.21[76,932] 1.02us     |-----------------------------------------L0.21------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[76,356] 1.02us 3mb  |-----------L0.?------------|                                                             "
    - "L0.?[357,670] 1.02us 4mb                              |-------------L0.?-------------|                             "
    - "L0.?[671,932] 1.02us 3mb                                                               |----------L0.?-----------| "
    - "**** Simulation run 21, type=split(HighL0OverlapSingleFile)(split_times=[356, 670]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.22[42,986] 1.02us     |-----------------------------------------L0.22------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[42,356] 1.02us 3mb  |-----------L0.?------------|                                                             "
    - "L0.?[357,670] 1.02us 3mb                               |-----------L0.?------------|                               "
    - "L0.?[671,986] 1.02us 3mb                                                            |------------L0.?------------| "
    - "**** Simulation run 22, type=split(HighL0OverlapSingleFile)(split_times=[356, 670]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.23[173,950] 1.02us    |-----------------------------------------L0.23------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[173,356] 1.02us 2mb |-------L0.?--------|                                                                     "
    - "L0.?[357,670] 1.02us 4mb                      |---------------L0.?---------------|                                 "
    - "L0.?[671,950] 1.02us 4mb                                                          |-------------L0.?-------------| "
    - "**** Simulation run 23, type=split(HighL0OverlapSingleFile)(split_times=[356]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.24[50,629] 1.02us     |-----------------------------------------L0.24------------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[50,356] 1.02us 5mb  |--------------------L0.?---------------------|                                           "
    - "L0.?[357,629] 1.02us 5mb                                                |------------------L0.?------------------| "
    - "**** Simulation run 24, type=split(HighL0OverlapSingleFile)(split_times=[356, 670]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.25[76,932] 1.02us     |-----------------------------------------L0.25------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[76,356] 1.02us 3mb  |-----------L0.?------------|                                                             "
    - "L0.?[357,670] 1.02us 4mb                              |-------------L0.?-------------|                             "
    - "L0.?[671,932] 1.02us 3mb                                                               |----------L0.?-----------| "
    - "**** Simulation run 25, type=split(HighL0OverlapSingleFile)(split_times=[356, 670]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.26[42,986] 1.02us     |-----------------------------------------L0.26------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[42,356] 1.02us 3mb  |-----------L0.?------------|                                                             "
    - "L0.?[357,670] 1.02us 3mb                               |-----------L0.?------------|                               "
    - "L0.?[671,986] 1.02us 3mb                                                            |------------L0.?------------| "
    - "**** Simulation run 26, type=split(HighL0OverlapSingleFile)(split_times=[356, 670]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.27[173,950] 1.03us    |-----------------------------------------L0.27------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[173,356] 1.03us 2mb |-------L0.?--------|                                                                     "
    - "L0.?[357,670] 1.03us 4mb                      |---------------L0.?---------------|                                 "
    - "L0.?[671,950] 1.03us 4mb                                                          |-------------L0.?-------------| "
    - "**** Simulation run 27, type=split(HighL0OverlapSingleFile)(split_times=[356]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.28[50,629] 1.03us     |-----------------------------------------L0.28------------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[50,356] 1.03us 5mb  |--------------------L0.?---------------------|                                           "
    - "L0.?[357,629] 1.03us 5mb                                                |------------------L0.?------------------| "
    - "**** Simulation run 28, type=split(HighL0OverlapSingleFile)(split_times=[356, 670]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.29[76,932] 1.03us     |-----------------------------------------L0.29------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[76,356] 1.03us 3mb  |-----------L0.?------------|                                                             "
    - "L0.?[357,670] 1.03us 4mb                              |-------------L0.?-------------|                             "
    - "L0.?[671,932] 1.03us 3mb                                                               |----------L0.?-----------| "
    - "**** Simulation run 29, type=split(HighL0OverlapSingleFile)(split_times=[356, 670]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.30[42,986] 1.03us     |-----------------------------------------L0.30------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[42,356] 1.03us 3mb  |-----------L0.?------------|                                                             "
    - "L0.?[357,670] 1.03us 3mb                               |-----------L0.?------------|                               "
    - "L0.?[671,986] 1.03us 3mb                                                            |------------L0.?------------| "
    - "**** Simulation run 30, type=split(HighL0OverlapSingleFile)(split_times=[356, 670]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.31[173,950] 1.03us    |-----------------------------------------L0.31------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[173,356] 1.03us 2mb |-------L0.?--------|                                                                     "
    - "L0.?[357,670] 1.03us 4mb                      |---------------L0.?---------------|                                 "
    - "L0.?[671,950] 1.03us 4mb                                                          |-------------L0.?-------------| "
    - "**** Simulation run 31, type=split(HighL0OverlapSingleFile)(split_times=[356]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.32[50,629] 1.03us     |-----------------------------------------L0.32------------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[50,356] 1.03us 5mb  |--------------------L0.?---------------------|                                           "
    - "L0.?[357,629] 1.03us 5mb                                                |------------------L0.?------------------| "
    - "**** Simulation run 32, type=split(HighL0OverlapSingleFile)(split_times=[356, 670]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.33[76,932] 1.03us     |-----------------------------------------L0.33------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[76,356] 1.03us 3mb  |-----------L0.?------------|                                                             "
    - "L0.?[357,670] 1.03us 4mb                              |-------------L0.?-------------|                             "
    - "L0.?[671,932] 1.03us 3mb                                                               |----------L0.?-----------| "
    - "**** Simulation run 33, type=split(HighL0OverlapSingleFile)(split_times=[356, 670]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.34[42,986] 1.03us     |-----------------------------------------L0.34------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[42,356] 1.03us 3mb  |-----------L0.?------------|                                                             "
    - "L0.?[357,670] 1.03us 3mb                               |-----------L0.?------------|                               "
    - "L0.?[671,986] 1.03us 3mb                                                            |------------L0.?------------| "
    - "**** Simulation run 34, type=split(HighL0OverlapSingleFile)(split_times=[356, 670]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.35[173,950] 1.03us    |-----------------------------------------L0.35------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[173,356] 1.03us 2mb |-------L0.?--------|                                                                     "
    - "L0.?[357,670] 1.03us 4mb                      |---------------L0.?---------------|                                 "
    - "L0.?[671,950] 1.03us 4mb                                                          |-------------L0.?-------------| "
    - "**** Simulation run 35, type=split(HighL0OverlapSingleFile)(split_times=[356]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.36[50,629] 1.03us     |-----------------------------------------L0.36------------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[50,356] 1.03us 5mb  |--------------------L0.?---------------------|                                           "
    - "L0.?[357,629] 1.03us 5mb                                                |------------------L0.?------------------| "
    - "**** Simulation run 36, type=split(HighL0OverlapSingleFile)(split_times=[356, 670]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.37[76,932] 1.04us     |-----------------------------------------L0.37------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[76,356] 1.04us 3mb  |-----------L0.?------------|                                                             "
    - "L0.?[357,670] 1.04us 4mb                              |-------------L0.?-------------|                             "
    - "L0.?[671,932] 1.04us 3mb                                                               |----------L0.?-----------| "
    - "**** Simulation run 37, type=split(HighL0OverlapSingleFile)(split_times=[356, 670]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.38[42,986] 1.04us     |-----------------------------------------L0.38------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[42,356] 1.04us 3mb  |-----------L0.?------------|                                                             "
    - "L0.?[357,670] 1.04us 3mb                               |-----------L0.?------------|                               "
    - "L0.?[671,986] 1.04us 3mb                                                            |------------L0.?------------| "
    - "**** Simulation run 38, type=split(HighL0OverlapSingleFile)(split_times=[356, 670]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.39[173,950] 1.04us    |-----------------------------------------L0.39------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[173,356] 1.04us 2mb |-------L0.?--------|                                                                     "
    - "L0.?[357,670] 1.04us 4mb                      |---------------L0.?---------------|                                 "
    - "L0.?[671,950] 1.04us 4mb                                                          |-------------L0.?-------------| "
    - "**** Simulation run 39, type=split(HighL0OverlapSingleFile)(split_times=[356]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.40[50,629] 1.04us     |-----------------------------------------L0.40------------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[50,356] 1.04us 5mb  |--------------------L0.?---------------------|                                           "
    - "L0.?[357,629] 1.04us 5mb                                                |------------------L0.?------------------| "
    - "**** Simulation run 40, type=split(HighL0OverlapSingleFile)(split_times=[356, 670]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.41[76,932] 1.04us     |-----------------------------------------L0.41------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[76,356] 1.04us 3mb  |-----------L0.?------------|                                                             "
    - "L0.?[357,670] 1.04us 4mb                              |-------------L0.?-------------|                             "
    - "L0.?[671,932] 1.04us 3mb                                                               |----------L0.?-----------| "
    - "**** Simulation run 41, type=split(HighL0OverlapSingleFile)(split_times=[356, 670]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.42[42,986] 1.04us     |-----------------------------------------L0.42------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[42,356] 1.04us 3mb  |-----------L0.?------------|                                                             "
    - "L0.?[357,670] 1.04us 3mb                               |-----------L0.?------------|                               "
    - "L0.?[671,986] 1.04us 3mb                                                            |------------L0.?------------| "
    - "**** Simulation run 42, type=split(HighL0OverlapSingleFile)(split_times=[356, 670]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.43[173,950] 1.04us    |-----------------------------------------L0.43------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[173,356] 1.04us 2mb |-------L0.?--------|                                                                     "
    - "L0.?[357,670] 1.04us 4mb                      |---------------L0.?---------------|                                 "
    - "L0.?[671,950] 1.04us 4mb                                                          |-------------L0.?-------------| "
    - "**** Simulation run 43, type=split(HighL0OverlapSingleFile)(split_times=[356]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.44[50,629] 1.04us     |-----------------------------------------L0.44------------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[50,356] 1.04us 5mb  |--------------------L0.?---------------------|                                           "
    - "L0.?[357,629] 1.04us 5mb                                                |------------------L0.?------------------| "
    - "**** Simulation run 44, type=split(HighL0OverlapSingleFile)(split_times=[356, 670]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.45[76,932] 1.04us     |-----------------------------------------L0.45------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[76,356] 1.04us 3mb  |-----------L0.?------------|                                                             "
    - "L0.?[357,670] 1.04us 4mb                              |-------------L0.?-------------|                             "
    - "L0.?[671,932] 1.04us 3mb                                                               |----------L0.?-----------| "
    - "**** Simulation run 45, type=split(HighL0OverlapSingleFile)(split_times=[356, 670]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.46[42,986] 1.05us     |-----------------------------------------L0.46------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[42,356] 1.05us 3mb  |-----------L0.?------------|                                                             "
    - "L0.?[357,670] 1.05us 3mb                               |-----------L0.?------------|                               "
    - "L0.?[671,986] 1.05us 3mb                                                            |------------L0.?------------| "
    - "**** Simulation run 46, type=split(HighL0OverlapSingleFile)(split_times=[356, 670]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.47[173,950] 1.05us    |-----------------------------------------L0.47------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[173,356] 1.05us 2mb |-------L0.?--------|                                                                     "
    - "L0.?[357,670] 1.05us 4mb                      |---------------L0.?---------------|                                 "
    - "L0.?[671,950] 1.05us 4mb                                                          |-------------L0.?-------------| "
    - "**** Simulation run 47, type=split(HighL0OverlapSingleFile)(split_times=[356]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.48[50,629] 1.05us     |-----------------------------------------L0.48------------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[50,356] 1.05us 5mb  |--------------------L0.?---------------------|                                           "
    - "L0.?[357,629] 1.05us 5mb                                                |------------------L0.?------------------| "
    - "**** Simulation run 48, type=split(HighL0OverlapSingleFile)(split_times=[356, 670]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.49[76,932] 1.05us     |-----------------------------------------L0.49------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[76,356] 1.05us 3mb  |-----------L0.?------------|                                                             "
    - "L0.?[357,670] 1.05us 4mb                              |-------------L0.?-------------|                             "
    - "L0.?[671,932] 1.05us 3mb                                                               |----------L0.?-----------| "
    - "**** Simulation run 49, type=split(HighL0OverlapSingleFile)(split_times=[356, 670]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.50[42,986] 1.05us     |-----------------------------------------L0.50------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[42,356] 1.05us 3mb  |-----------L0.?------------|                                                             "
    - "L0.?[357,670] 1.05us 3mb                               |-----------L0.?------------|                               "
    - "L0.?[671,986] 1.05us 3mb                                                            |------------L0.?------------| "
    - "Committing partition 1:"
    - "  Soft Deleting 50 files: L0.1, L0.2, L0.3, L0.4, L0.5, L0.6, L0.7, L0.8, L0.9, L0.10, L0.11, L0.12, L0.13, L0.14, L0.15, L0.16, L0.17, L0.18, L0.19, L0.20, L0.21, L0.22, L0.23, L0.24, L0.25, L0.26, L0.27, L0.28, L0.29, L0.30, L0.31, L0.32, L0.33, L0.34, L0.35, L0.36, L0.37, L0.38, L0.39, L0.40, L0.41, L0.42, L0.43, L0.44, L0.45, L0.46, L0.47, L0.48, L0.49, L0.50"
    - "  Creating 138 files"
    - "**** Simulation run 50, type=split(CompactAndSplitOutput(FoundSubsetLessThanMaxCompactSize))(split_times=[329, 658]). 81 Input Files, 300mb total:"
    - "L0                                                                                                                 "
    - "L0.51[0,1] 999ns 10mb    |L0.51|                                                                                   "
    - "L0.52[76,356] 1us 3mb          |---------L0.52---------|                                                           "
    - "L0.53[357,670] 1us 4mb                                   |----------L0.53-----------|                              "
    - "L0.54[671,932] 1us 3mb                                                                |--------L0.54--------|      "
    - "L0.55[42,356] 1us 3mb       |----------L0.55-----------|                                                           "
    - "L0.56[357,670] 1us 3mb                                   |----------L0.56-----------|                              "
    - "L0.57[671,986] 1us 3mb                                                                |----------L0.57-----------| "
    - "L0.58[173,356] 1us 2mb                  |----L0.58-----|                                                           "
    - "L0.59[357,670] 1us 4mb                                   |----------L0.59-----------|                              "
    - "L0.60[671,950] 1us 4mb                                                                |---------L0.60---------|    "
    - "L0.61[50,356] 1us 5mb        |----------L0.61----------|                                                           "
    - "L0.62[357,629] 1us 5mb                                   |--------L0.62---------|                                  "
    - "L0.63[76,356] 1us 3mb          |---------L0.63---------|                                                           "
    - "L0.64[357,670] 1us 4mb                                   |----------L0.64-----------|                              "
    - "L0.65[671,932] 1us 3mb                                                                |--------L0.65--------|      "
    - "L0.66[42,356] 1us 3mb       |----------L0.66-----------|                                                           "
    - "L0.67[357,670] 1us 3mb                                   |----------L0.67-----------|                              "
    - "L0.68[671,986] 1us 3mb                                                                |----------L0.68-----------| "
    - "L0.69[173,356] 1.01us 2mb               |----L0.69-----|                                                           "
    - "L0.70[357,670] 1.01us 4mb                                |----------L0.70-----------|                              "
    - "L0.71[671,950] 1.01us 4mb                                                             |---------L0.71---------|    "
    - "L0.72[50,356] 1.01us 5mb     |----------L0.72----------|                                                           "
    - "L0.73[357,629] 1.01us 5mb                                |--------L0.73---------|                                  "
    - "L0.74[76,356] 1.01us 3mb       |---------L0.74---------|                                                           "
    - "L0.75[357,670] 1.01us 4mb                                |----------L0.75-----------|                              "
    - "L0.76[671,932] 1.01us 3mb                                                             |--------L0.76--------|      "
    - "L0.77[42,356] 1.01us 3mb    |----------L0.77-----------|                                                           "
    - "L0.78[357,670] 1.01us 3mb                                |----------L0.78-----------|                              "
    - "L0.79[671,986] 1.01us 3mb                                                             |----------L0.79-----------| "
    - "L0.80[173,356] 1.01us 2mb               |----L0.80-----|                                                           "
    - "L0.81[357,670] 1.01us 4mb                                |----------L0.81-----------|                              "
    - "L0.82[671,950] 1.01us 4mb                                                             |---------L0.82---------|    "
    - "L0.83[50,356] 1.01us 5mb     |----------L0.83----------|                                                           "
    - "L0.84[357,629] 1.01us 5mb                                |--------L0.84---------|                                  "
    - "L0.85[76,356] 1.01us 3mb       |---------L0.85---------|                                                           "
    - "L0.86[357,670] 1.01us 4mb                                |----------L0.86-----------|                              "
    - "L0.87[671,932] 1.01us 3mb                                                             |--------L0.87--------|      "
    - "L0.88[42,356] 1.01us 3mb    |----------L0.88-----------|                                                           "
    - "L0.89[357,670] 1.01us 3mb                                |----------L0.89-----------|                              "
    - "L0.90[671,986] 1.01us 3mb                                                             |----------L0.90-----------| "
    - "L0.91[173,356] 1.01us 2mb               |----L0.91-----|                                                           "
    - "L0.92[357,670] 1.01us 4mb                                |----------L0.92-----------|                              "
    - "L0.93[671,950] 1.01us 4mb                                                             |---------L0.93---------|    "
    - "L0.94[50,356] 1.01us 5mb     |----------L0.94----------|                                                           "
    - "L0.95[357,629] 1.01us 5mb                                |--------L0.95---------|                                  "
    - "L0.96[76,356] 1.02us 3mb       |---------L0.96---------|                                                           "
    - "L0.97[357,670] 1.02us 4mb                                |----------L0.97-----------|                              "
    - "L0.98[671,932] 1.02us 3mb                                                             |--------L0.98--------|      "
    - "L0.99[42,356] 1.02us 3mb    |----------L0.99-----------|                                                           "
    - "L0.100[357,670] 1.02us 3mb                                |----------L0.100----------|                              "
    - "L0.101[671,986] 1.02us 3mb                                                             |----------L0.101----------| "
    - "L0.102[173,356] 1.02us 2mb               |----L0.102----|                                                           "
    - "L0.103[357,670] 1.02us 4mb                                |----------L0.103----------|                              "
    - "L0.104[671,950] 1.02us 4mb                                                             |--------L0.104---------|    "
    - "L0.105[50,356] 1.02us 5mb    |---------L0.105----------|                                                           "
    - "L0.106[357,629] 1.02us 5mb                                |--------L0.106--------|                                  "
    - "L0.107[76,356] 1.02us 3mb      |--------L0.107---------|                                                           "
    - "L0.108[357,670] 1.02us 4mb                                |----------L0.108----------|                              "
    - "L0.109[671,932] 1.02us 3mb                                                             |-------L0.109--------|      "
    - "L0.110[42,356] 1.02us 3mb   |----------L0.110----------|                                                           "
    - "L0.111[357,670] 1.02us 3mb                                |----------L0.111----------|                              "
    - "L0.112[671,986] 1.02us 3mb                                                             |----------L0.112----------| "
    - "L0.113[173,356] 1.02us 2mb               |----L0.113----|                                                           "
    - "L0.114[357,670] 1.02us 4mb                                |----------L0.114----------|                              "
    - "L0.115[671,950] 1.02us 4mb                                                             |--------L0.115---------|    "
    - "L0.116[50,356] 1.02us 5mb    |---------L0.116----------|                                                           "
    - "L0.117[357,629] 1.02us 5mb                                |--------L0.117--------|                                  "
    - "L0.118[76,356] 1.02us 3mb      |--------L0.118---------|                                                           "
    - "L0.119[357,670] 1.02us 4mb                                |----------L0.119----------|                              "
    - "L0.120[671,932] 1.02us 3mb                                                             |-------L0.120--------|      "
    - "L0.121[42,356] 1.02us 3mb   |----------L0.121----------|                                                           "
    - "L0.122[357,670] 1.02us 3mb                                |----------L0.122----------|                              "
    - "L0.123[671,986] 1.02us 3mb                                                             |----------L0.123----------| "
    - "L0.124[173,356] 1.03us 2mb               |----L0.124----|                                                           "
    - "L0.125[357,670] 1.03us 4mb                                |----------L0.125----------|                              "
    - "L0.126[671,950] 1.03us 4mb                                                             |--------L0.126---------|    "
    - "L0.127[50,356] 1.03us 5mb    |---------L0.127----------|                                                           "
    - "L0.128[357,629] 1.03us 5mb                                |--------L0.128--------|                                  "
    - "L0.129[76,356] 1.03us 3mb      |--------L0.129---------|                                                           "
    - "L0.130[357,670] 1.03us 4mb                                |----------L0.130----------|                              "
    - "L0.131[671,932] 1.03us 3mb                                                             |-------L0.131--------|      "
    - "**** 3 Output Files (parquet_file_id not yet assigned), 300mb total:"
    - "L1                                                                                                                 "
    - "L1.?[0,329] 1.03us 100mb |------------L1.?------------|                                                            "
    - "L1.?[330,658] 1.03us 100mb                              |-----------L1.?------------|                               "
    - "L1.?[659,986] 1.03us 100mb                                                            |-----------L1.?------------| "
    - "Committing partition 1:"
    - "  Soft Deleting 81 files: L0.51, L0.52, L0.53, L0.54, L0.55, L0.56, L0.57, L0.58, L0.59, L0.60, L0.61, L0.62, L0.63, L0.64, L0.65, L0.66, L0.67, L0.68, L0.69, L0.70, L0.71, L0.72, L0.73, L0.74, L0.75, L0.76, L0.77, L0.78, L0.79, L0.80, L0.81, L0.82, L0.83, L0.84, L0.85, L0.86, L0.87, L0.88, L0.89, L0.90, L0.91, L0.92, L0.93, L0.94, L0.95, L0.96, L0.97, L0.98, L0.99, L0.100, L0.101, L0.102, L0.103, L0.104, L0.105, L0.106, L0.107, L0.108, L0.109, L0.110, L0.111, L0.112, L0.113, L0.114, L0.115, L0.116, L0.117, L0.118, L0.119, L0.120, L0.121, L0.122, L0.123, L0.124, L0.125, L0.126, L0.127, L0.128, L0.129, L0.130, L0.131"
    - "  Creating 3 files"
    - "**** Simulation run 51, type=split(ReduceOverlap)(split_times=[658]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.133[357,670] 1.03us   |-----------------------------------------L0.133-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[357,658] 1.03us 3mb |----------------------------------------L0.?----------------------------------------|    "
    - "L0.?[659,670] 1.03us 130kb                                                                                      |L0.?|"
    - "**** Simulation run 52, type=split(ReduceOverlap)(split_times=[329]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.132[42,356] 1.03us    |-----------------------------------------L0.132-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[42,329] 1.03us 3mb  |--------------------------------------L0.?--------------------------------------|        "
    - "L0.?[330,356] 1.03us 293kb                                                                                  |L0.?-| "
    - "**** Simulation run 53, type=split(ReduceOverlap)(split_times=[658]). 1 Input Files, 4mb total:"
    - "L0, all files 4mb                                                                                                  "
    - "L0.136[357,670] 1.03us   |-----------------------------------------L0.136-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 4mb total:"
    - "L0                                                                                                                 "
    - "L0.?[357,658] 1.03us 4mb |----------------------------------------L0.?----------------------------------------|    "
    - "L0.?[659,670] 1.03us 158kb                                                                                      |L0.?|"
    - "**** Simulation run 54, type=split(ReduceOverlap)(split_times=[329]). 1 Input Files, 2mb total:"
    - "L0, all files 2mb                                                                                                  "
    - "L0.135[173,356] 1.03us   |-----------------------------------------L0.135-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 2mb total:"
    - "L0                                                                                                                 "
    - "L0.?[173,329] 1.03us 2mb |-----------------------------------L0.?-----------------------------------|              "
    - "L0.?[330,356] 1.03us 356kb                                                                             |---L0.?---| "
    - "**** Simulation run 55, type=split(ReduceOverlap)(split_times=[329]). 1 Input Files, 5mb total:"
    - "L0, all files 5mb                                                                                                  "
    - "L0.138[50,356] 1.03us    |-----------------------------------------L0.138-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 5mb total:"
    - "L0                                                                                                                 "
    - "L0.?[50,329] 1.03us 5mb  |--------------------------------------L0.?--------------------------------------|        "
    - "L0.?[330,356] 1.03us 478kb                                                                                  |L0.?-| "
    - "**** Simulation run 56, type=split(ReduceOverlap)(split_times=[658]). 1 Input Files, 4mb total:"
    - "L0, all files 4mb                                                                                                  "
    - "L0.141[357,670] 1.03us   |-----------------------------------------L0.141-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 4mb total:"
    - "L0                                                                                                                 "
    - "L0.?[357,658] 1.03us 4mb |----------------------------------------L0.?----------------------------------------|    "
    - "L0.?[659,670] 1.03us 144kb                                                                                      |L0.?|"
    - "**** Simulation run 57, type=split(ReduceOverlap)(split_times=[329]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.140[76,356] 1.03us    |-----------------------------------------L0.140-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[76,329] 1.03us 3mb  |-------------------------------------L0.?--------------------------------------|         "
    - "L0.?[330,356] 1.03us 323kb                                                                                 |-L0.?-| "
    - "**** Simulation run 58, type=split(ReduceOverlap)(split_times=[658]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.144[357,670] 1.03us   |-----------------------------------------L0.144-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[357,658] 1.03us 3mb |----------------------------------------L0.?----------------------------------------|    "
    - "L0.?[659,670] 1.03us 130kb                                                                                      |L0.?|"
    - "**** Simulation run 59, type=split(ReduceOverlap)(split_times=[329]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.143[42,356] 1.03us    |-----------------------------------------L0.143-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[42,329] 1.03us 3mb  |--------------------------------------L0.?--------------------------------------|        "
    - "L0.?[330,356] 1.03us 293kb                                                                                  |L0.?-| "
    - "**** Simulation run 60, type=split(ReduceOverlap)(split_times=[658]). 1 Input Files, 4mb total:"
    - "L0, all files 4mb                                                                                                  "
    - "L0.147[357,670] 1.03us   |-----------------------------------------L0.147-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 4mb total:"
    - "L0                                                                                                                 "
    - "L0.?[357,658] 1.03us 4mb |----------------------------------------L0.?----------------------------------------|    "
    - "L0.?[659,670] 1.03us 158kb                                                                                      |L0.?|"
    - "**** Simulation run 61, type=split(ReduceOverlap)(split_times=[329]). 1 Input Files, 2mb total:"
    - "L0, all files 2mb                                                                                                  "
    - "L0.146[173,356] 1.03us   |-----------------------------------------L0.146-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 2mb total:"
    - "L0                                                                                                                 "
    - "L0.?[173,329] 1.03us 2mb |-----------------------------------L0.?-----------------------------------|              "
    - "L0.?[330,356] 1.03us 356kb                                                                             |---L0.?---| "
    - "**** Simulation run 62, type=split(ReduceOverlap)(split_times=[329]). 1 Input Files, 5mb total:"
    - "L0, all files 5mb                                                                                                  "
    - "L0.149[50,356] 1.03us    |-----------------------------------------L0.149-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 5mb total:"
    - "L0                                                                                                                 "
    - "L0.?[50,329] 1.03us 5mb  |--------------------------------------L0.?--------------------------------------|        "
    - "L0.?[330,356] 1.03us 478kb                                                                                  |L0.?-| "
    - "**** Simulation run 63, type=split(ReduceOverlap)(split_times=[658]). 1 Input Files, 4mb total:"
    - "L0, all files 4mb                                                                                                  "
    - "L0.152[357,670] 1.04us   |-----------------------------------------L0.152-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 4mb total:"
    - "L0                                                                                                                 "
    - "L0.?[357,658] 1.04us 4mb |----------------------------------------L0.?----------------------------------------|    "
    - "L0.?[659,670] 1.04us 144kb                                                                                      |L0.?|"
    - "**** Simulation run 64, type=split(ReduceOverlap)(split_times=[329]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.151[76,356] 1.04us    |-----------------------------------------L0.151-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[76,329] 1.04us 3mb  |-------------------------------------L0.?--------------------------------------|         "
    - "L0.?[330,356] 1.04us 323kb                                                                                 |-L0.?-| "
    - "**** Simulation run 65, type=split(ReduceOverlap)(split_times=[658]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.155[357,670] 1.04us   |-----------------------------------------L0.155-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[357,658] 1.04us 3mb |----------------------------------------L0.?----------------------------------------|    "
    - "L0.?[659,670] 1.04us 130kb                                                                                      |L0.?|"
    - "**** Simulation run 66, type=split(ReduceOverlap)(split_times=[329]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.154[42,356] 1.04us    |-----------------------------------------L0.154-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[42,329] 1.04us 3mb  |--------------------------------------L0.?--------------------------------------|        "
    - "L0.?[330,356] 1.04us 293kb                                                                                  |L0.?-| "
    - "**** Simulation run 67, type=split(ReduceOverlap)(split_times=[658]). 1 Input Files, 4mb total:"
    - "L0, all files 4mb                                                                                                  "
    - "L0.158[357,670] 1.04us   |-----------------------------------------L0.158-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 4mb total:"
    - "L0                                                                                                                 "
    - "L0.?[357,658] 1.04us 4mb |----------------------------------------L0.?----------------------------------------|    "
    - "L0.?[659,670] 1.04us 158kb                                                                                      |L0.?|"
    - "**** Simulation run 68, type=split(ReduceOverlap)(split_times=[329]). 1 Input Files, 2mb total:"
    - "L0, all files 2mb                                                                                                  "
    - "L0.157[173,356] 1.04us   |-----------------------------------------L0.157-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 2mb total:"
    - "L0                                                                                                                 "
    - "L0.?[173,329] 1.04us 2mb |-----------------------------------L0.?-----------------------------------|              "
    - "L0.?[330,356] 1.04us 356kb                                                                             |---L0.?---| "
    - "**** Simulation run 69, type=split(ReduceOverlap)(split_times=[329]). 1 Input Files, 5mb total:"
    - "L0, all files 5mb                                                                                                  "
    - "L0.160[50,356] 1.04us    |-----------------------------------------L0.160-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 5mb total:"
    - "L0                                                                                                                 "
    - "L0.?[50,329] 1.04us 5mb  |--------------------------------------L0.?--------------------------------------|        "
    - "L0.?[330,356] 1.04us 478kb                                                                                  |L0.?-| "
    - "**** Simulation run 70, type=split(ReduceOverlap)(split_times=[658]). 1 Input Files, 4mb total:"
    - "L0, all files 4mb                                                                                                  "
    - "L0.163[357,670] 1.04us   |-----------------------------------------L0.163-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 4mb total:"
    - "L0                                                                                                                 "
    - "L0.?[357,658] 1.04us 4mb |----------------------------------------L0.?----------------------------------------|    "
    - "L0.?[659,670] 1.04us 144kb                                                                                      |L0.?|"
    - "**** Simulation run 71, type=split(ReduceOverlap)(split_times=[329]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.162[76,356] 1.04us    |-----------------------------------------L0.162-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[76,329] 1.04us 3mb  |-------------------------------------L0.?--------------------------------------|         "
    - "L0.?[330,356] 1.04us 323kb                                                                                 |-L0.?-| "
    - "**** Simulation run 72, type=split(ReduceOverlap)(split_times=[658]). 1 Input Files, 4mb total:"
    - "L0, all files 4mb                                                                                                  "
    - "L0.169[357,670] 1.04us   |-----------------------------------------L0.169-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 4mb total:"
    - "L0                                                                                                                 "
    - "L0.?[357,658] 1.04us 4mb |----------------------------------------L0.?----------------------------------------|    "
    - "L0.?[659,670] 1.04us 158kb                                                                                      |L0.?|"
    - "**** Simulation run 73, type=split(ReduceOverlap)(split_times=[329]). 1 Input Files, 2mb total:"
    - "L0, all files 2mb                                                                                                  "
    - "L0.168[173,356] 1.04us   |-----------------------------------------L0.168-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 2mb total:"
    - "L0                                                                                                                 "
    - "L0.?[173,329] 1.04us 2mb |-----------------------------------L0.?-----------------------------------|              "
    - "L0.?[330,356] 1.04us 356kb                                                                             |---L0.?---| "
    - "**** Simulation run 74, type=split(ReduceOverlap)(split_times=[329]). 1 Input Files, 5mb total:"
    - "L0, all files 5mb                                                                                                  "
    - "L0.171[50,356] 1.04us    |-----------------------------------------L0.171-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 5mb total:"
    - "L0                                                                                                                 "
    - "L0.?[50,329] 1.04us 5mb  |--------------------------------------L0.?--------------------------------------|        "
    - "L0.?[330,356] 1.04us 478kb                                                                                  |L0.?-| "
    - "**** Simulation run 75, type=split(ReduceOverlap)(split_times=[658]). 1 Input Files, 4mb total:"
    - "L0, all files 4mb                                                                                                  "
    - "L0.174[357,670] 1.04us   |-----------------------------------------L0.174-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 4mb total:"
    - "L0                                                                                                                 "
    - "L0.?[357,658] 1.04us 4mb |----------------------------------------L0.?----------------------------------------|    "
    - "L0.?[659,670] 1.04us 144kb                                                                                      |L0.?|"
    - "**** Simulation run 76, type=split(ReduceOverlap)(split_times=[329]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.173[76,356] 1.04us    |-----------------------------------------L0.173-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[76,329] 1.04us 3mb  |-------------------------------------L0.?--------------------------------------|         "
    - "L0.?[330,356] 1.04us 323kb                                                                                 |-L0.?-| "
    - "**** Simulation run 77, type=split(ReduceOverlap)(split_times=[658]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.177[357,670] 1.05us   |-----------------------------------------L0.177-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[357,658] 1.05us 3mb |----------------------------------------L0.?----------------------------------------|    "
    - "L0.?[659,670] 1.05us 130kb                                                                                      |L0.?|"
    - "**** Simulation run 78, type=split(ReduceOverlap)(split_times=[329]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.176[42,356] 1.05us    |-----------------------------------------L0.176-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[42,329] 1.05us 3mb  |--------------------------------------L0.?--------------------------------------|        "
    - "L0.?[330,356] 1.05us 293kb                                                                                  |L0.?-| "
    - "**** Simulation run 79, type=split(ReduceOverlap)(split_times=[658]). 1 Input Files, 4mb total:"
    - "L0, all files 4mb                                                                                                  "
    - "L0.180[357,670] 1.05us   |-----------------------------------------L0.180-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 4mb total:"
    - "L0                                                                                                                 "
    - "L0.?[357,658] 1.05us 4mb |----------------------------------------L0.?----------------------------------------|    "
    - "L0.?[659,670] 1.05us 158kb                                                                                      |L0.?|"
    - "**** Simulation run 80, type=split(ReduceOverlap)(split_times=[658]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.166[357,670] 1.04us   |-----------------------------------------L0.166-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[357,658] 1.04us 3mb |----------------------------------------L0.?----------------------------------------|    "
    - "L0.?[659,670] 1.04us 130kb                                                                                      |L0.?|"
    - "**** Simulation run 81, type=split(ReduceOverlap)(split_times=[329]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.165[42,356] 1.04us    |-----------------------------------------L0.165-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[42,329] 1.04us 3mb  |--------------------------------------L0.?--------------------------------------|        "
    - "L0.?[330,356] 1.04us 293kb                                                                                  |L0.?-| "
    - "**** Simulation run 82, type=split(ReduceOverlap)(split_times=[329]). 1 Input Files, 2mb total:"
    - "L0, all files 2mb                                                                                                  "
    - "L0.179[173,356] 1.05us   |-----------------------------------------L0.179-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 2mb total:"
    - "L0                                                                                                                 "
    - "L0.?[173,329] 1.05us 2mb |-----------------------------------L0.?-----------------------------------|              "
    - "L0.?[330,356] 1.05us 356kb                                                                             |---L0.?---| "
    - "**** Simulation run 83, type=split(ReduceOverlap)(split_times=[329]). 1 Input Files, 5mb total:"
    - "L0, all files 5mb                                                                                                  "
    - "L0.182[50,356] 1.05us    |-----------------------------------------L0.182-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 5mb total:"
    - "L0                                                                                                                 "
    - "L0.?[50,329] 1.05us 5mb  |--------------------------------------L0.?--------------------------------------|        "
    - "L0.?[330,356] 1.05us 478kb                                                                                  |L0.?-| "
    - "**** Simulation run 84, type=split(ReduceOverlap)(split_times=[658]). 1 Input Files, 4mb total:"
    - "L0, all files 4mb                                                                                                  "
    - "L0.185[357,670] 1.05us   |-----------------------------------------L0.185-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 4mb total:"
    - "L0                                                                                                                 "
    - "L0.?[357,658] 1.05us 4mb |----------------------------------------L0.?----------------------------------------|    "
    - "L0.?[659,670] 1.05us 144kb                                                                                      |L0.?|"
    - "**** Simulation run 85, type=split(ReduceOverlap)(split_times=[329]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.184[76,356] 1.05us    |-----------------------------------------L0.184-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[76,329] 1.05us 3mb  |-------------------------------------L0.?--------------------------------------|         "
    - "L0.?[330,356] 1.05us 323kb                                                                                 |-L0.?-| "
    - "**** Simulation run 86, type=split(ReduceOverlap)(split_times=[658]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.188[357,670] 1.05us   |-----------------------------------------L0.188-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[357,658] 1.05us 3mb |----------------------------------------L0.?----------------------------------------|    "
    - "L0.?[659,670] 1.05us 130kb                                                                                      |L0.?|"
    - "**** Simulation run 87, type=split(ReduceOverlap)(split_times=[329]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.187[42,356] 1.05us    |-----------------------------------------L0.187-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[42,329] 1.05us 3mb  |--------------------------------------L0.?--------------------------------------|        "
    - "L0.?[330,356] 1.05us 293kb                                                                                  |L0.?-| "
    - "Committing partition 1:"
    - "  Soft Deleting 37 files: L0.132, L0.133, L0.135, L0.136, L0.138, L0.140, L0.141, L0.143, L0.144, L0.146, L0.147, L0.149, L0.151, L0.152, L0.154, L0.155, L0.157, L0.158, L0.160, L0.162, L0.163, L0.165, L0.166, L0.168, L0.169, L0.171, L0.173, L0.174, L0.176, L0.177, L0.179, L0.180, L0.182, L0.184, L0.185, L0.187, L0.188"
    - "  Creating 74 files"
    - "**** Simulation run 88, type=split(CompactAndSplitOutput(FoundSubsetLessThanMaxCompactSize))(split_times=[319, 638]). 5 Input Files, 206mb total:"
    - "L0                                                                                                                 "
    - "L0.195[42,329] 1.03us 3mb     |---------------L0.195----------------|                                              "
    - "L0.196[330,356] 1.03us 293kb                                             |L0.196|                                     "
    - "L0.193[357,658] 1.03us 3mb                                                |----------------L0.193-----------------| "
    - "L1                                                                                                                 "
    - "L1.190[0,329] 1.03us 100mb|------------------L1.190-------------------|                                             "
    - "L1.191[330,658] 1.03us 100mb                                             |------------------L1.191------------------| "
    - "**** 3 Output Files (parquet_file_id not yet assigned), 206mb total:"
    - "L1                                                                                                                 "
    - "L1.?[0,319] 1.03us 100mb |------------------L1.?-------------------|                                               "
    - "L1.?[320,638] 1.03us 100mb                                           |------------------L1.?-------------------|    "
    - "L1.?[639,658] 1.03us 7mb                                                                                        |L1.?|"
    - "Committing partition 1:"
    - "  Soft Deleting 5 files: L1.190, L1.191, L0.193, L0.195, L0.196"
    - "  Creating 3 files"
    - "**** Simulation run 89, type=split(ReduceOverlap)(split_times=[638]). 1 Input Files, 4mb total:"
    - "L0, all files 4mb                                                                                                  "
    - "L0.197[357,658] 1.03us   |-----------------------------------------L0.197-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 4mb total:"
    - "L0                                                                                                                 "
    - "L0.?[357,638] 1.03us 4mb |---------------------------------------L0.?---------------------------------------|      "
    - "L0.?[639,658] 1.03us 264kb                                                                                    |L0.?|"
    - "**** Simulation run 90, type=split(ReduceOverlap)(split_times=[319]). 1 Input Files, 2mb total:"
    - "L0, all files 2mb                                                                                                  "
    - "L0.199[173,329] 1.03us   |-----------------------------------------L0.199-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 2mb total:"
    - "L0                                                                                                                 "
    - "L0.?[173,319] 1.03us 2mb |---------------------------------------L0.?---------------------------------------|      "
    - "L0.?[320,329] 1.03us 132kb                                                                                    |L0.?|"
    - "**** Simulation run 91, type=split(ReduceOverlap)(split_times=[319]). 1 Input Files, 5mb total:"
    - "L0, all files 5mb                                                                                                  "
    - "L0.201[50,329] 1.03us    |-----------------------------------------L0.201-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 5mb total:"
    - "L0                                                                                                                 "
    - "L0.?[50,319] 1.03us 5mb  |----------------------------------------L0.?----------------------------------------|    "
    - "L0.?[320,329] 1.03us 177kb                                                                                       |L0.?|"
    - "**** Simulation run 92, type=split(ReduceOverlap)(split_times=[638]). 1 Input Files, 4mb total:"
    - "L0, all files 4mb                                                                                                  "
    - "L0.203[357,658] 1.03us   |-----------------------------------------L0.203-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 4mb total:"
    - "L0                                                                                                                 "
    - "L0.?[357,638] 1.03us 3mb |---------------------------------------L0.?---------------------------------------|      "
    - "L0.?[639,658] 1.03us 239kb                                                                                    |L0.?|"
    - "**** Simulation run 93, type=split(ReduceOverlap)(split_times=[319]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.205[76,329] 1.03us    |-----------------------------------------L0.205-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[76,319] 1.03us 3mb  |----------------------------------------L0.?----------------------------------------|    "
    - "L0.?[320,329] 1.03us 120kb                                                                                      |L0.?|"
    - "**** Simulation run 94, type=split(ReduceOverlap)(split_times=[638]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.207[357,658] 1.03us   |-----------------------------------------L0.207-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[357,638] 1.03us 3mb |---------------------------------------L0.?---------------------------------------|      "
    - "L0.?[639,658] 1.03us 217kb                                                                                    |L0.?|"
    - "**** Simulation run 95, type=split(ReduceOverlap)(split_times=[319]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.209[42,329] 1.03us    |-----------------------------------------L0.209-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[42,319] 1.03us 3mb  |----------------------------------------L0.?----------------------------------------|    "
    - "L0.?[320,329] 1.03us 108kb                                                                                       |L0.?|"
    - "**** Simulation run 96, type=split(ReduceOverlap)(split_times=[638]). 1 Input Files, 4mb total:"
    - "L0, all files 4mb                                                                                                  "
    - "L0.211[357,658] 1.03us   |-----------------------------------------L0.211-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 4mb total:"
    - "L0                                                                                                                 "
    - "L0.?[357,638] 1.03us 4mb |---------------------------------------L0.?---------------------------------------|      "
    - "L0.?[639,658] 1.03us 264kb                                                                                    |L0.?|"
    - "**** Simulation run 97, type=split(ReduceOverlap)(split_times=[319]). 1 Input Files, 2mb total:"
    - "L0, all files 2mb                                                                                                  "
    - "L0.213[173,329] 1.03us   |-----------------------------------------L0.213-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 2mb total:"
    - "L0                                                                                                                 "
    - "L0.?[173,319] 1.03us 2mb |---------------------------------------L0.?---------------------------------------|      "
    - "L0.?[320,329] 1.03us 132kb                                                                                    |L0.?|"
    - "**** Simulation run 98, type=split(ReduceOverlap)(split_times=[319]). 1 Input Files, 5mb total:"
    - "L0, all files 5mb                                                                                                  "
    - "L0.215[50,329] 1.03us    |-----------------------------------------L0.215-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 5mb total:"
    - "L0                                                                                                                 "
    - "L0.?[50,319] 1.03us 5mb  |----------------------------------------L0.?----------------------------------------|    "
    - "L0.?[320,329] 1.03us 177kb                                                                                       |L0.?|"
    - "**** Simulation run 99, type=split(ReduceOverlap)(split_times=[638]). 1 Input Files, 4mb total:"
    - "L0, all files 4mb                                                                                                  "
    - "L0.217[357,658] 1.04us   |-----------------------------------------L0.217-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 4mb total:"
    - "L0                                                                                                                 "
    - "L0.?[357,638] 1.04us 3mb |---------------------------------------L0.?---------------------------------------|      "
    - "L0.?[639,658] 1.04us 239kb                                                                                    |L0.?|"
    - "**** Simulation run 100, type=split(ReduceOverlap)(split_times=[319]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.219[76,329] 1.04us    |-----------------------------------------L0.219-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[76,319] 1.04us 3mb  |----------------------------------------L0.?----------------------------------------|    "
    - "L0.?[320,329] 1.04us 120kb                                                                                      |L0.?|"
    - "**** Simulation run 101, type=split(ReduceOverlap)(split_times=[638]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.221[357,658] 1.04us   |-----------------------------------------L0.221-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[357,638] 1.04us 3mb |---------------------------------------L0.?---------------------------------------|      "
    - "L0.?[639,658] 1.04us 217kb                                                                                    |L0.?|"
    - "**** Simulation run 102, type=split(ReduceOverlap)(split_times=[319]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.223[42,329] 1.04us    |-----------------------------------------L0.223-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[42,319] 1.04us 3mb  |----------------------------------------L0.?----------------------------------------|    "
    - "L0.?[320,329] 1.04us 108kb                                                                                       |L0.?|"
    - "**** Simulation run 103, type=split(ReduceOverlap)(split_times=[638]). 1 Input Files, 4mb total:"
    - "L0, all files 4mb                                                                                                  "
    - "L0.225[357,658] 1.04us   |-----------------------------------------L0.225-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 4mb total:"
    - "L0                                                                                                                 "
    - "L0.?[357,638] 1.04us 4mb |---------------------------------------L0.?---------------------------------------|      "
    - "L0.?[639,658] 1.04us 264kb                                                                                    |L0.?|"
    - "**** Simulation run 104, type=split(ReduceOverlap)(split_times=[319]). 1 Input Files, 2mb total:"
    - "L0, all files 2mb                                                                                                  "
    - "L0.227[173,329] 1.04us   |-----------------------------------------L0.227-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 2mb total:"
    - "L0                                                                                                                 "
    - "L0.?[173,319] 1.04us 2mb |---------------------------------------L0.?---------------------------------------|      "
    - "L0.?[320,329] 1.04us 132kb                                                                                    |L0.?|"
    - "**** Simulation run 105, type=split(ReduceOverlap)(split_times=[319]). 1 Input Files, 5mb total:"
    - "L0, all files 5mb                                                                                                  "
    - "L0.229[50,329] 1.04us    |-----------------------------------------L0.229-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 5mb total:"
    - "L0                                                                                                                 "
    - "L0.?[50,319] 1.04us 5mb  |----------------------------------------L0.?----------------------------------------|    "
    - "L0.?[320,329] 1.04us 177kb                                                                                       |L0.?|"
    - "**** Simulation run 106, type=split(ReduceOverlap)(split_times=[638]). 1 Input Files, 4mb total:"
    - "L0, all files 4mb                                                                                                  "
    - "L0.231[357,658] 1.04us   |-----------------------------------------L0.231-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 4mb total:"
    - "L0                                                                                                                 "
    - "L0.?[357,638] 1.04us 3mb |---------------------------------------L0.?---------------------------------------|      "
    - "L0.?[639,658] 1.04us 239kb                                                                                    |L0.?|"
    - "**** Simulation run 107, type=split(ReduceOverlap)(split_times=[319]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.233[76,329] 1.04us    |-----------------------------------------L0.233-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[76,319] 1.04us 3mb  |----------------------------------------L0.?----------------------------------------|    "
    - "L0.?[320,329] 1.04us 120kb                                                                                      |L0.?|"
    - "**** Simulation run 108, type=split(ReduceOverlap)(split_times=[638]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.251[357,658] 1.04us   |-----------------------------------------L0.251-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[357,638] 1.04us 3mb |---------------------------------------L0.?---------------------------------------|      "
    - "L0.?[639,658] 1.04us 217kb                                                                                    |L0.?|"
    - "**** Simulation run 109, type=split(ReduceOverlap)(split_times=[319]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.253[42,329] 1.04us    |-----------------------------------------L0.253-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[42,319] 1.04us 3mb  |----------------------------------------L0.?----------------------------------------|    "
    - "L0.?[320,329] 1.04us 108kb                                                                                       |L0.?|"
    - "**** Simulation run 110, type=split(ReduceOverlap)(split_times=[638]). 1 Input Files, 4mb total:"
    - "L0, all files 4mb                                                                                                  "
    - "L0.235[357,658] 1.04us   |-----------------------------------------L0.235-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 4mb total:"
    - "L0                                                                                                                 "
    - "L0.?[357,638] 1.04us 4mb |---------------------------------------L0.?---------------------------------------|      "
    - "L0.?[639,658] 1.04us 264kb                                                                                    |L0.?|"
    - "**** Simulation run 111, type=split(ReduceOverlap)(split_times=[319]). 1 Input Files, 2mb total:"
    - "L0, all files 2mb                                                                                                  "
    - "L0.237[173,329] 1.04us   |-----------------------------------------L0.237-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 2mb total:"
    - "L0                                                                                                                 "
    - "L0.?[173,319] 1.04us 2mb |---------------------------------------L0.?---------------------------------------|      "
    - "L0.?[320,329] 1.04us 132kb                                                                                    |L0.?|"
    - "**** Simulation run 112, type=split(ReduceOverlap)(split_times=[319]). 1 Input Files, 5mb total:"
    - "L0, all files 5mb                                                                                                  "
    - "L0.239[50,329] 1.04us    |-----------------------------------------L0.239-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 5mb total:"
    - "L0                                                                                                                 "
    - "L0.?[50,319] 1.04us 5mb  |----------------------------------------L0.?----------------------------------------|    "
    - "L0.?[320,329] 1.04us 177kb                                                                                       |L0.?|"
    - "**** Simulation run 113, type=split(ReduceOverlap)(split_times=[638]). 1 Input Files, 4mb total:"
    - "L0, all files 4mb                                                                                                  "
    - "L0.241[357,658] 1.04us   |-----------------------------------------L0.241-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 4mb total:"
    - "L0                                                                                                                 "
    - "L0.?[357,638] 1.04us 3mb |---------------------------------------L0.?---------------------------------------|      "
    - "L0.?[639,658] 1.04us 239kb                                                                                    |L0.?|"
    - "**** Simulation run 114, type=split(ReduceOverlap)(split_times=[319]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.243[76,329] 1.04us    |-----------------------------------------L0.243-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[76,319] 1.04us 3mb  |----------------------------------------L0.?----------------------------------------|    "
    - "L0.?[320,329] 1.04us 120kb                                                                                      |L0.?|"
    - "**** Simulation run 115, type=split(ReduceOverlap)(split_times=[638]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.245[357,658] 1.05us   |-----------------------------------------L0.245-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[357,638] 1.05us 3mb |---------------------------------------L0.?---------------------------------------|      "
    - "L0.?[639,658] 1.05us 217kb                                                                                    |L0.?|"
    - "**** Simulation run 116, type=split(ReduceOverlap)(split_times=[319]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.247[42,329] 1.05us    |-----------------------------------------L0.247-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[42,319] 1.05us 3mb  |----------------------------------------L0.?----------------------------------------|    "
    - "L0.?[320,329] 1.05us 108kb                                                                                       |L0.?|"
    - "**** Simulation run 117, type=split(ReduceOverlap)(split_times=[638]). 1 Input Files, 4mb total:"
    - "L0, all files 4mb                                                                                                  "
    - "L0.249[357,658] 1.05us   |-----------------------------------------L0.249-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 4mb total:"
    - "L0                                                                                                                 "
    - "L0.?[357,638] 1.05us 4mb |---------------------------------------L0.?---------------------------------------|      "
    - "L0.?[639,658] 1.05us 264kb                                                                                    |L0.?|"
    - "**** Simulation run 118, type=split(ReduceOverlap)(split_times=[319]). 1 Input Files, 2mb total:"
    - "L0, all files 2mb                                                                                                  "
    - "L0.255[173,329] 1.05us   |-----------------------------------------L0.255-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 2mb total:"
    - "L0                                                                                                                 "
    - "L0.?[173,319] 1.05us 2mb |---------------------------------------L0.?---------------------------------------|      "
    - "L0.?[320,329] 1.05us 132kb                                                                                    |L0.?|"
    - "**** Simulation run 119, type=split(ReduceOverlap)(split_times=[319]). 1 Input Files, 5mb total:"
    - "L0, all files 5mb                                                                                                  "
    - "L0.257[50,329] 1.05us    |-----------------------------------------L0.257-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 5mb total:"
    - "L0                                                                                                                 "
    - "L0.?[50,319] 1.05us 5mb  |----------------------------------------L0.?----------------------------------------|    "
    - "L0.?[320,329] 1.05us 177kb                                                                                       |L0.?|"
    - "**** Simulation run 120, type=split(ReduceOverlap)(split_times=[638]). 1 Input Files, 4mb total:"
    - "L0, all files 4mb                                                                                                  "
    - "L0.259[357,658] 1.05us   |-----------------------------------------L0.259-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 4mb total:"
    - "L0                                                                                                                 "
    - "L0.?[357,638] 1.05us 3mb |---------------------------------------L0.?---------------------------------------|      "
    - "L0.?[639,658] 1.05us 239kb                                                                                    |L0.?|"
    - "**** Simulation run 121, type=split(ReduceOverlap)(split_times=[319]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.261[76,329] 1.05us    |-----------------------------------------L0.261-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[76,319] 1.05us 3mb  |----------------------------------------L0.?----------------------------------------|    "
    - "L0.?[320,329] 1.05us 120kb                                                                                      |L0.?|"
    - "**** Simulation run 122, type=split(ReduceOverlap)(split_times=[638]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.263[357,658] 1.05us   |-----------------------------------------L0.263-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[357,638] 1.05us 3mb |---------------------------------------L0.?---------------------------------------|      "
    - "L0.?[639,658] 1.05us 217kb                                                                                    |L0.?|"
    - "**** Simulation run 123, type=split(ReduceOverlap)(split_times=[319]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.265[42,329] 1.05us    |-----------------------------------------L0.265-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[42,319] 1.05us 3mb  |----------------------------------------L0.?----------------------------------------|    "
    - "L0.?[320,329] 1.05us 108kb                                                                                       |L0.?|"
    - "Committing partition 1:"
    - "  Soft Deleting 35 files: L0.197, L0.199, L0.201, L0.203, L0.205, L0.207, L0.209, L0.211, L0.213, L0.215, L0.217, L0.219, L0.221, L0.223, L0.225, L0.227, L0.229, L0.231, L0.233, L0.235, L0.237, L0.239, L0.241, L0.243, L0.245, L0.247, L0.249, L0.251, L0.253, L0.255, L0.257, L0.259, L0.261, L0.263, L0.265"
    - "  Creating 70 files"
    - "**** Simulation run 124, type=split(CompactAndSplitOutput(FoundSubsetLessThanMaxCompactSize))(split_times=[920]). 3 Input Files, 104mb total:"
    - "L0                                                                                                                 "
    - "L0.194[659,670] 1.03us 130kb|L0.194|                                                                                  "
    - "L0.134[671,986] 1.03us 3mb   |---------------------------------------L0.134---------------------------------------| "
    - "L1                                                                                                                 "
    - "L1.192[659,986] 1.03us 100mb|-----------------------------------------L1.192-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 104mb total:"
    - "L1                                                                                                                 "
    - "L1.?[659,920] 1.03us 83mb|--------------------------------L1.?---------------------------------|                   "
    - "L1.?[921,986] 1.03us 21mb                                                                        |-----L1.?------| "
    - "Committing partition 1:"
    - "  Soft Deleting 3 files: L0.134, L1.192, L0.194"
    - "  Creating 2 files"
    - "**** Simulation run 125, type=split(ReduceOverlap)(split_times=[920]). 1 Input Files, 4mb total:"
    - "L0, all files 4mb                                                                                                  "
    - "L0.137[671,950] 1.03us   |-----------------------------------------L0.137-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 4mb total:"
    - "L0                                                                                                                 "
    - "L0.?[671,920] 1.03us 3mb |-------------------------------------L0.?-------------------------------------|          "
    - "L0.?[921,950] 1.03us 398kb                                                                                |-L0.?--| "
    - "**** Simulation run 126, type=split(ReduceOverlap)(split_times=[920]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.142[671,932] 1.03us   |-----------------------------------------L0.142-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[671,920] 1.03us 3mb |---------------------------------------L0.?----------------------------------------|     "
    - "L0.?[921,932] 1.03us 145kb                                                                                      |L0.?|"
    - "**** Simulation run 127, type=split(ReduceOverlap)(split_times=[920]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.145[671,986] 1.03us   |-----------------------------------------L0.145-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[671,920] 1.03us 3mb |--------------------------------L0.?---------------------------------|                   "
    - "L0.?[921,986] 1.03us 720kb                                                                       |------L0.?------| "
    - "**** Simulation run 128, type=split(ReduceOverlap)(split_times=[920]). 1 Input Files, 4mb total:"
    - "L0, all files 4mb                                                                                                  "
    - "L0.148[671,950] 1.03us   |-----------------------------------------L0.148-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 4mb total:"
    - "L0                                                                                                                 "
    - "L0.?[671,920] 1.03us 3mb |-------------------------------------L0.?-------------------------------------|          "
    - "L0.?[921,950] 1.03us 398kb                                                                                |-L0.?--| "
    - "**** Simulation run 129, type=split(ReduceOverlap)(split_times=[920]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.153[671,932] 1.04us   |-----------------------------------------L0.153-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[671,920] 1.04us 3mb |---------------------------------------L0.?----------------------------------------|     "
    - "L0.?[921,932] 1.04us 145kb                                                                                      |L0.?|"
    - "**** Simulation run 130, type=split(ReduceOverlap)(split_times=[920]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.156[671,986] 1.04us   |-----------------------------------------L0.156-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[671,920] 1.04us 3mb |--------------------------------L0.?---------------------------------|                   "
    - "L0.?[921,986] 1.04us 720kb                                                                       |------L0.?------| "
    - "**** Simulation run 131, type=split(ReduceOverlap)(split_times=[920]). 1 Input Files, 4mb total:"
    - "L0, all files 4mb                                                                                                  "
    - "L0.159[671,950] 1.04us   |-----------------------------------------L0.159-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 4mb total:"
    - "L0                                                                                                                 "
    - "L0.?[671,920] 1.04us 3mb |-------------------------------------L0.?-------------------------------------|          "
    - "L0.?[921,950] 1.04us 398kb                                                                                |-L0.?--| "
    - "**** Simulation run 132, type=split(ReduceOverlap)(split_times=[920]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.164[671,932] 1.04us   |-----------------------------------------L0.164-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[671,920] 1.04us 3mb |---------------------------------------L0.?----------------------------------------|     "
    - "L0.?[921,932] 1.04us 145kb                                                                                      |L0.?|"
    - "**** Simulation run 133, type=split(ReduceOverlap)(split_times=[920]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.167[671,986] 1.04us   |-----------------------------------------L0.167-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[671,920] 1.04us 3mb |--------------------------------L0.?---------------------------------|                   "
    - "L0.?[921,986] 1.04us 720kb                                                                       |------L0.?------| "
    - "**** Simulation run 134, type=split(ReduceOverlap)(split_times=[920]). 1 Input Files, 4mb total:"
    - "L0, all files 4mb                                                                                                  "
    - "L0.170[671,950] 1.04us   |-----------------------------------------L0.170-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 4mb total:"
    - "L0                                                                                                                 "
    - "L0.?[671,920] 1.04us 3mb |-------------------------------------L0.?-------------------------------------|          "
    - "L0.?[921,950] 1.04us 398kb                                                                                |-L0.?--| "
    - "**** Simulation run 135, type=split(ReduceOverlap)(split_times=[920]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.175[671,932] 1.04us   |-----------------------------------------L0.175-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[671,920] 1.04us 3mb |---------------------------------------L0.?----------------------------------------|     "
    - "L0.?[921,932] 1.04us 145kb                                                                                      |L0.?|"
    - "**** Simulation run 136, type=split(ReduceOverlap)(split_times=[920]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.178[671,986] 1.05us   |-----------------------------------------L0.178-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[671,920] 1.05us 3mb |--------------------------------L0.?---------------------------------|                   "
    - "L0.?[921,986] 1.05us 720kb                                                                       |------L0.?------| "
    - "**** Simulation run 137, type=split(ReduceOverlap)(split_times=[920]). 1 Input Files, 4mb total:"
    - "L0, all files 4mb                                                                                                  "
    - "L0.181[671,950] 1.05us   |-----------------------------------------L0.181-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 4mb total:"
    - "L0                                                                                                                 "
    - "L0.?[671,920] 1.05us 3mb |-------------------------------------L0.?-------------------------------------|          "
    - "L0.?[921,950] 1.05us 398kb                                                                                |-L0.?--| "
    - "**** Simulation run 138, type=split(ReduceOverlap)(split_times=[920]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.186[671,932] 1.05us   |-----------------------------------------L0.186-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[671,920] 1.05us 3mb |---------------------------------------L0.?----------------------------------------|     "
    - "L0.?[921,932] 1.05us 145kb                                                                                      |L0.?|"
    - "**** Simulation run 139, type=split(ReduceOverlap)(split_times=[920]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.189[671,986] 1.05us   |-----------------------------------------L0.189-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[671,920] 1.05us 3mb |--------------------------------L0.?---------------------------------|                   "
    - "L0.?[921,986] 1.05us 720kb                                                                       |------L0.?------| "
    - "Committing partition 1:"
    - "  Soft Deleting 15 files: L0.137, L0.142, L0.145, L0.148, L0.153, L0.156, L0.159, L0.164, L0.167, L0.170, L0.175, L0.178, L0.181, L0.186, L0.189"
    - "  Creating 30 files"
    - "**** Simulation run 140, type=split(CompactAndSplitOutput(FoundSubsetLessThanMaxCompactSize))(split_times=[308, 616]). 11 Input Files, 299mb total:"
    - "L0                                                                                                                 "
    - "L0.272[173,319] 1.03us 2mb                |---L0.272---|                                                            "
    - "L0.273[320,329] 1.03us 132kb                               |L0.273|                                                   "
    - "L0.200[330,356] 1.03us 356kb                                |L0.200|                                                  "
    - "L0.270[357,638] 1.03us 4mb                                  |---------L0.270----------|                             "
    - "L0.271[639,658] 1.03us 264kb                                                              |L0.271|                    "
    - "L0.198[659,670] 1.03us 158kb                                                                |L0.198|                  "
    - "L0.342[671,920] 1.03us 3mb                                                                 |--------L0.342--------| "
    - "L1                                                                                                                 "
    - "L1.267[0,319] 1.03us 100mb|-----------L1.267------------|                                                           "
    - "L1.268[320,638] 1.03us 100mb                               |-----------L1.268------------|                            "
    - "L1.269[639,658] 1.03us 7mb                                                              |L1.269|                    "
    - "L1.340[659,920] 1.03us 83mb                                                                |--------L1.340---------| "
    - "**** 3 Output Files (parquet_file_id not yet assigned), 299mb total:"
    - "L1                                                                                                                 "
    - "L1.?[0,308] 1.03us 100mb |------------L1.?------------|                                                            "
    - "L1.?[309,616] 1.03us 100mb                              |------------L1.?------------|                              "
    - "L1.?[617,920] 1.03us 99mb                                                            |-----------L1.?------------| "
    - "Committing partition 1:"
    - "  Soft Deleting 11 files: L0.198, L0.200, L1.267, L1.268, L1.269, L0.270, L0.271, L0.272, L0.273, L1.340, L0.342"
    - "  Creating 3 files"
    - "**** Simulation run 141, type=split(ReduceOverlap)(split_times=[616]). 1 Input Files, 5mb total:"
    - "L0, all files 5mb                                                                                                  "
    - "L0.139[357,629] 1.03us   |-----------------------------------------L0.139-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 5mb total:"
    - "L0                                                                                                                 "
    - "L0.?[357,616] 1.03us 4mb |---------------------------------------L0.?----------------------------------------|     "
    - "L0.?[617,629] 1.03us 231kb                                                                                      |L0.?|"
    - "**** Simulation run 142, type=split(ReduceOverlap)(split_times=[308]). 1 Input Files, 5mb total:"
    - "L0, all files 5mb                                                                                                  "
    - "L0.274[50,319] 1.03us    |-----------------------------------------L0.274-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 5mb total:"
    - "L0                                                                                                                 "
    - "L0.?[50,308] 1.03us 4mb  |----------------------------------------L0.?----------------------------------------|    "
    - "L0.?[309,319] 1.03us 195kb                                                                                      |L0.?|"
    - "**** Simulation run 143, type=split(ReduceOverlap)(split_times=[616]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.276[357,638] 1.03us   |-----------------------------------------L0.276-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[357,616] 1.03us 3mb |--------------------------------------L0.?--------------------------------------|        "
    - "L0.?[617,638] 1.03us 263kb                                                                                   |L0.?| "
    - "**** Simulation run 144, type=split(ReduceOverlap)(split_times=[308]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.278[76,319] 1.03us    |-----------------------------------------L0.278-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[76,308] 1.03us 3mb  |---------------------------------------L0.?----------------------------------------|     "
    - "L0.?[309,319] 1.03us 132kb                                                                                      |L0.?|"
    - "**** Simulation run 145, type=split(ReduceOverlap)(split_times=[616]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.280[357,638] 1.03us   |-----------------------------------------L0.280-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[357,616] 1.03us 3mb |--------------------------------------L0.?--------------------------------------|        "
    - "L0.?[617,638] 1.03us 239kb                                                                                   |L0.?| "
    - "**** Simulation run 146, type=split(ReduceOverlap)(split_times=[308]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.282[42,319] 1.03us    |-----------------------------------------L0.282-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[42,308] 1.03us 3mb  |----------------------------------------L0.?----------------------------------------|    "
    - "L0.?[309,319] 1.03us 119kb                                                                                      |L0.?|"
    - "**** Simulation run 147, type=split(ReduceOverlap)(split_times=[616]). 1 Input Files, 4mb total:"
    - "L0, all files 4mb                                                                                                  "
    - "L0.284[357,638] 1.03us   |-----------------------------------------L0.284-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 4mb total:"
    - "L0                                                                                                                 "
    - "L0.?[357,616] 1.03us 3mb |--------------------------------------L0.?--------------------------------------|        "
    - "L0.?[617,638] 1.03us 290kb                                                                                   |L0.?| "
    - "**** Simulation run 148, type=split(ReduceOverlap)(split_times=[308]). 1 Input Files, 2mb total:"
    - "L0, all files 2mb                                                                                                  "
    - "L0.286[173,319] 1.03us   |-----------------------------------------L0.286-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 2mb total:"
    - "L0                                                                                                                 "
    - "L0.?[173,308] 1.03us 2mb |--------------------------------------L0.?---------------------------------------|       "
    - "L0.?[309,319] 1.03us 145kb                                                                                   |L0.?| "
    - "**** Simulation run 149, type=split(ReduceOverlap)(split_times=[616]). 1 Input Files, 5mb total:"
    - "L0, all files 5mb                                                                                                  "
    - "L0.150[357,629] 1.03us   |-----------------------------------------L0.150-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 5mb total:"
    - "L0                                                                                                                 "
    - "L0.?[357,616] 1.03us 4mb |---------------------------------------L0.?----------------------------------------|     "
    - "L0.?[617,629] 1.03us 231kb                                                                                      |L0.?|"
    - "**** Simulation run 150, type=split(ReduceOverlap)(split_times=[308]). 1 Input Files, 5mb total:"
    - "L0, all files 5mb                                                                                                  "
    - "L0.288[50,319] 1.03us    |-----------------------------------------L0.288-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 5mb total:"
    - "L0                                                                                                                 "
    - "L0.?[50,308] 1.03us 4mb  |----------------------------------------L0.?----------------------------------------|    "
    - "L0.?[309,319] 1.03us 195kb                                                                                      |L0.?|"
    - "**** Simulation run 151, type=split(ReduceOverlap)(split_times=[616]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.290[357,638] 1.04us   |-----------------------------------------L0.290-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[357,616] 1.04us 3mb |--------------------------------------L0.?--------------------------------------|        "
    - "L0.?[617,638] 1.04us 263kb                                                                                   |L0.?| "
    - "**** Simulation run 152, type=split(ReduceOverlap)(split_times=[308]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.292[76,319] 1.04us    |-----------------------------------------L0.292-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[76,308] 1.04us 3mb  |---------------------------------------L0.?----------------------------------------|     "
    - "L0.?[309,319] 1.04us 132kb                                                                                      |L0.?|"
    - "**** Simulation run 153, type=split(ReduceOverlap)(split_times=[616]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.294[357,638] 1.04us   |-----------------------------------------L0.294-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[357,616] 1.04us 3mb |--------------------------------------L0.?--------------------------------------|        "
    - "L0.?[617,638] 1.04us 239kb                                                                                   |L0.?| "
    - "**** Simulation run 154, type=split(ReduceOverlap)(split_times=[308]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.296[42,319] 1.04us    |-----------------------------------------L0.296-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[42,308] 1.04us 3mb  |----------------------------------------L0.?----------------------------------------|    "
    - "L0.?[309,319] 1.04us 119kb                                                                                      |L0.?|"
    - "**** Simulation run 155, type=split(ReduceOverlap)(split_times=[616]). 1 Input Files, 4mb total:"
    - "L0, all files 4mb                                                                                                  "
    - "L0.298[357,638] 1.04us   |-----------------------------------------L0.298-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 4mb total:"
    - "L0                                                                                                                 "
    - "L0.?[357,616] 1.04us 3mb |--------------------------------------L0.?--------------------------------------|        "
    - "L0.?[617,638] 1.04us 290kb                                                                                   |L0.?| "
    - "**** Simulation run 156, type=split(ReduceOverlap)(split_times=[308]). 1 Input Files, 2mb total:"
    - "L0, all files 2mb                                                                                                  "
    - "L0.300[173,319] 1.04us   |-----------------------------------------L0.300-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 2mb total:"
    - "L0                                                                                                                 "
    - "L0.?[173,308] 1.04us 2mb |--------------------------------------L0.?---------------------------------------|       "
    - "L0.?[309,319] 1.04us 145kb                                                                                   |L0.?| "
    - "**** Simulation run 157, type=split(ReduceOverlap)(split_times=[616]). 1 Input Files, 5mb total:"
    - "L0, all files 5mb                                                                                                  "
    - "L0.161[357,629] 1.04us   |-----------------------------------------L0.161-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 5mb total:"
    - "L0                                                                                                                 "
    - "L0.?[357,616] 1.04us 4mb |---------------------------------------L0.?----------------------------------------|     "
    - "L0.?[617,629] 1.04us 231kb                                                                                      |L0.?|"
    - "**** Simulation run 158, type=split(ReduceOverlap)(split_times=[308]). 1 Input Files, 5mb total:"
    - "L0, all files 5mb                                                                                                  "
    - "L0.302[50,319] 1.04us    |-----------------------------------------L0.302-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 5mb total:"
    - "L0                                                                                                                 "
    - "L0.?[50,308] 1.04us 4mb  |----------------------------------------L0.?----------------------------------------|    "
    - "L0.?[309,319] 1.04us 195kb                                                                                      |L0.?|"
    - "**** Simulation run 159, type=split(ReduceOverlap)(split_times=[616]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.304[357,638] 1.04us   |-----------------------------------------L0.304-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[357,616] 1.04us 3mb |--------------------------------------L0.?--------------------------------------|        "
    - "L0.?[617,638] 1.04us 263kb                                                                                   |L0.?| "
    - "**** Simulation run 160, type=split(ReduceOverlap)(split_times=[308]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.306[76,319] 1.04us    |-----------------------------------------L0.306-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[76,308] 1.04us 3mb  |---------------------------------------L0.?----------------------------------------|     "
    - "L0.?[309,319] 1.04us 132kb                                                                                      |L0.?|"
    - "**** Simulation run 161, type=split(ReduceOverlap)(split_times=[616]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.308[357,638] 1.04us   |-----------------------------------------L0.308-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[357,616] 1.04us 3mb |--------------------------------------L0.?--------------------------------------|        "
    - "L0.?[617,638] 1.04us 239kb                                                                                   |L0.?| "
    - "**** Simulation run 162, type=split(ReduceOverlap)(split_times=[308]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.310[42,319] 1.04us    |-----------------------------------------L0.310-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[42,308] 1.04us 3mb  |----------------------------------------L0.?----------------------------------------|    "
    - "L0.?[309,319] 1.04us 119kb                                                                                      |L0.?|"
    - "**** Simulation run 163, type=split(ReduceOverlap)(split_times=[616]). 1 Input Files, 4mb total:"
    - "L0, all files 4mb                                                                                                  "
    - "L0.312[357,638] 1.04us   |-----------------------------------------L0.312-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 4mb total:"
    - "L0                                                                                                                 "
    - "L0.?[357,616] 1.04us 3mb |--------------------------------------L0.?--------------------------------------|        "
    - "L0.?[617,638] 1.04us 290kb                                                                                   |L0.?| "
    - "**** Simulation run 164, type=split(ReduceOverlap)(split_times=[308]). 1 Input Files, 2mb total:"
    - "L0, all files 2mb                                                                                                  "
    - "L0.314[173,319] 1.04us   |-----------------------------------------L0.314-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 2mb total:"
    - "L0                                                                                                                 "
    - "L0.?[173,308] 1.04us 2mb |--------------------------------------L0.?---------------------------------------|       "
    - "L0.?[309,319] 1.04us 145kb                                                                                   |L0.?| "
    - "**** Simulation run 165, type=split(ReduceOverlap)(split_times=[616]). 1 Input Files, 5mb total:"
    - "L0, all files 5mb                                                                                                  "
    - "L0.172[357,629] 1.04us   |-----------------------------------------L0.172-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 5mb total:"
    - "L0                                                                                                                 "
    - "L0.?[357,616] 1.04us 4mb |---------------------------------------L0.?----------------------------------------|     "
    - "L0.?[617,629] 1.04us 231kb                                                                                      |L0.?|"
    - "**** Simulation run 166, type=split(ReduceOverlap)(split_times=[308]). 1 Input Files, 5mb total:"
    - "L0, all files 5mb                                                                                                  "
    - "L0.316[50,319] 1.04us    |-----------------------------------------L0.316-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 5mb total:"
    - "L0                                                                                                                 "
    - "L0.?[50,308] 1.04us 4mb  |----------------------------------------L0.?----------------------------------------|    "
    - "L0.?[309,319] 1.04us 195kb                                                                                      |L0.?|"
    - "**** Simulation run 167, type=split(ReduceOverlap)(split_times=[616]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.318[357,638] 1.04us   |-----------------------------------------L0.318-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[357,616] 1.04us 3mb |--------------------------------------L0.?--------------------------------------|        "
    - "L0.?[617,638] 1.04us 263kb                                                                                   |L0.?| "
    - "**** Simulation run 168, type=split(ReduceOverlap)(split_times=[308]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.320[76,319] 1.04us    |-----------------------------------------L0.320-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[76,308] 1.04us 3mb  |---------------------------------------L0.?----------------------------------------|     "
    - "L0.?[309,319] 1.04us 132kb                                                                                      |L0.?|"
    - "**** Simulation run 169, type=split(ReduceOverlap)(split_times=[616]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.322[357,638] 1.05us   |-----------------------------------------L0.322-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[357,616] 1.05us 3mb |--------------------------------------L0.?--------------------------------------|        "
    - "L0.?[617,638] 1.05us 239kb                                                                                   |L0.?| "
    - "**** Simulation run 170, type=split(ReduceOverlap)(split_times=[308]). 1 Input Files, 2mb total:"
    - "L0, all files 2mb                                                                                                  "
    - "L0.328[173,319] 1.05us   |-----------------------------------------L0.328-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 2mb total:"
    - "L0                                                                                                                 "
    - "L0.?[173,308] 1.05us 2mb |--------------------------------------L0.?---------------------------------------|       "
    - "L0.?[309,319] 1.05us 145kb                                                                                   |L0.?| "
    - "**** Simulation run 171, type=split(ReduceOverlap)(split_times=[616]). 1 Input Files, 5mb total:"
    - "L0, all files 5mb                                                                                                  "
    - "L0.183[357,629] 1.05us   |-----------------------------------------L0.183-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 5mb total:"
    - "L0                                                                                                                 "
    - "L0.?[357,616] 1.05us 4mb |---------------------------------------L0.?----------------------------------------|     "
    - "L0.?[617,629] 1.05us 231kb                                                                                      |L0.?|"
    - "**** Simulation run 172, type=split(ReduceOverlap)(split_times=[308]). 1 Input Files, 5mb total:"
    - "L0, all files 5mb                                                                                                  "
    - "L0.330[50,319] 1.05us    |-----------------------------------------L0.330-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 5mb total:"
    - "L0                                                                                                                 "
    - "L0.?[50,308] 1.05us 4mb  |----------------------------------------L0.?----------------------------------------|    "
    - "L0.?[309,319] 1.05us 195kb                                                                                      |L0.?|"
    - "**** Simulation run 173, type=split(ReduceOverlap)(split_times=[616]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.332[357,638] 1.05us   |-----------------------------------------L0.332-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[357,616] 1.05us 3mb |--------------------------------------L0.?--------------------------------------|        "
    - "L0.?[617,638] 1.05us 263kb                                                                                   |L0.?| "
    - "**** Simulation run 174, type=split(ReduceOverlap)(split_times=[308]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.334[76,319] 1.05us    |-----------------------------------------L0.334-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[76,308] 1.05us 3mb  |---------------------------------------L0.?----------------------------------------|     "
    - "L0.?[309,319] 1.05us 132kb                                                                                      |L0.?|"
    - "**** Simulation run 175, type=split(ReduceOverlap)(split_times=[616]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.336[357,638] 1.05us   |-----------------------------------------L0.336-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[357,616] 1.05us 3mb |--------------------------------------L0.?--------------------------------------|        "
    - "L0.?[617,638] 1.05us 239kb                                                                                   |L0.?| "
    - "**** Simulation run 176, type=split(ReduceOverlap)(split_times=[308]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.338[42,319] 1.05us    |-----------------------------------------L0.338-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[42,308] 1.05us 3mb  |----------------------------------------L0.?----------------------------------------|    "
    - "L0.?[309,319] 1.05us 119kb                                                                                      |L0.?|"
    - "**** Simulation run 177, type=split(ReduceOverlap)(split_times=[308]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.324[42,319] 1.05us    |-----------------------------------------L0.324-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[42,308] 1.05us 3mb  |----------------------------------------L0.?----------------------------------------|    "
    - "L0.?[309,319] 1.05us 119kb                                                                                      |L0.?|"
    - "**** Simulation run 178, type=split(ReduceOverlap)(split_times=[616]). 1 Input Files, 4mb total:"
    - "L0, all files 4mb                                                                                                  "
    - "L0.326[357,638] 1.05us   |-----------------------------------------L0.326-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 4mb total:"
    - "L0                                                                                                                 "
    - "L0.?[357,616] 1.05us 3mb |--------------------------------------L0.?--------------------------------------|        "
    - "L0.?[617,638] 1.05us 290kb                                                                                   |L0.?| "
    - "Committing partition 1:"
    - "  Soft Deleting 38 files: L0.139, L0.150, L0.161, L0.172, L0.183, L0.274, L0.276, L0.278, L0.280, L0.282, L0.284, L0.286, L0.288, L0.290, L0.292, L0.294, L0.296, L0.298, L0.300, L0.302, L0.304, L0.306, L0.308, L0.310, L0.312, L0.314, L0.316, L0.318, L0.320, L0.322, L0.324, L0.326, L0.328, L0.330, L0.332, L0.334, L0.336, L0.338"
    - "  Creating 76 files"
    - "**** Simulation run 179, type=split(CompactAndSplitOutput(FoundSubsetLessThanMaxCompactSize))(split_times=[973]). 2 Input Files, 21mb total:"
    - "L0                                                                                                                 "
    - "L0.343[921,950] 1.03us 398kb|----------------L0.343----------------|                                                  "
    - "L1                                                                                                                 "
    - "L1.341[921,986] 1.03us 21mb|-----------------------------------------L1.341-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 21mb total:"
    - "L1                                                                                                                 "
    - "L1.?[921,973] 1.03us 17mb|---------------------------------L1.?---------------------------------|                  "
    - "L1.?[974,986] 1.03us 4mb                                                                          |-----L1.?-----| "
    - "Committing partition 1:"
    - "  Soft Deleting 2 files: L1.341, L0.343"
    - "  Creating 2 files"
    - "**** Simulation run 180, type=split(ReduceOverlap)(split_times=[973]). 1 Input Files, 720kb total:"
    - "L0, all files 720kb                                                                                                "
    - "L0.347[921,986] 1.03us   |-----------------------------------------L0.347-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 720kb total:"
    - "L0                                                                                                                 "
    - "L0.?[921,973] 1.03us 576kb|---------------------------------L0.?---------------------------------|                  "
    - "L0.?[974,986] 1.03us 144kb                                                                         |-----L0.?-----| "
    - "**** Simulation run 181, type=split(ReduceOverlap)(split_times=[973]). 1 Input Files, 720kb total:"
    - "L0, all files 720kb                                                                                                "
    - "L0.353[921,986] 1.04us   |-----------------------------------------L0.353-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 720kb total:"
    - "L0                                                                                                                 "
    - "L0.?[921,973] 1.04us 576kb|---------------------------------L0.?---------------------------------|                  "
    - "L0.?[974,986] 1.04us 144kb                                                                         |-----L0.?-----| "
    - "**** Simulation run 182, type=split(ReduceOverlap)(split_times=[973]). 1 Input Files, 720kb total:"
    - "L0, all files 720kb                                                                                                "
    - "L0.359[921,986] 1.04us   |-----------------------------------------L0.359-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 720kb total:"
    - "L0                                                                                                                 "
    - "L0.?[921,973] 1.04us 576kb|---------------------------------L0.?---------------------------------|                  "
    - "L0.?[974,986] 1.04us 144kb                                                                         |-----L0.?-----| "
    - "**** Simulation run 183, type=split(ReduceOverlap)(split_times=[973]). 1 Input Files, 720kb total:"
    - "L0, all files 720kb                                                                                                "
    - "L0.365[921,986] 1.05us   |-----------------------------------------L0.365-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 720kb total:"
    - "L0                                                                                                                 "
    - "L0.?[921,973] 1.05us 576kb|---------------------------------L0.?---------------------------------|                  "
    - "L0.?[974,986] 1.05us 144kb                                                                         |-----L0.?-----| "
    - "**** Simulation run 184, type=split(ReduceOverlap)(split_times=[973]). 1 Input Files, 720kb total:"
    - "L0, all files 720kb                                                                                                "
    - "L0.371[921,986] 1.05us   |-----------------------------------------L0.371-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 720kb total:"
    - "L0                                                                                                                 "
    - "L0.?[921,973] 1.05us 576kb|---------------------------------L0.?---------------------------------|                  "
    - "L0.?[974,986] 1.05us 144kb                                                                         |-----L0.?-----| "
    - "Committing partition 1:"
    - "  Soft Deleting 5 files: L0.347, L0.353, L0.359, L0.365, L0.371"
    - "  Creating 10 files"
    - "**** Simulation run 185, type=split(CompactAndSplitOutput(FoundSubsetLessThanMaxCompactSize))(split_times=[295, 590]). 7 Input Files, 209mb total:"
    - "L0                                                                                                                 "
    - "L0.377[50,308] 1.03us 4mb       |--------------L0.377---------------|                                              "
    - "L0.378[309,319] 1.03us 195kb                                             |L0.378|                                     "
    - "L0.275[320,329] 1.03us 177kb                                              |L0.275|                                    "
    - "L0.202[330,356] 1.03us 478kb                                                |L0.202|                                  "
    - "L0.375[357,616] 1.03us 4mb                                                    |--------------L0.375---------------| "
    - "L1                                                                                                                 "
    - "L1.372[0,308] 1.03us 100mb|------------------L1.372-------------------|                                             "
    - "L1.373[309,616] 1.03us 100mb                                             |------------------L1.373------------------| "
    - "**** 3 Output Files (parquet_file_id not yet assigned), 209mb total:"
    - "L1                                                                                                                 "
    - "L1.?[0,295] 1.03us 100mb |------------------L1.?-------------------|                                               "
    - "L1.?[296,590] 1.03us 100mb                                           |------------------L1.?------------------|     "
    - "L1.?[591,616] 1.03us 9mb                                                                                       |L1.?|"
    - "Committing partition 1:"
    - "  Soft Deleting 7 files: L0.202, L0.275, L1.372, L1.373, L0.375, L0.377, L0.378"
    - "  Creating 3 files"
    - "**** Simulation run 186, type=split(ReduceOverlap)(split_times=[590]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.379[357,616] 1.03us   |-----------------------------------------L0.379-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[357,590] 1.03us 3mb |-------------------------------------L0.?-------------------------------------|          "
    - "L0.?[591,616] 1.03us 311kb                                                                                 |-L0.?-| "
    - "**** Simulation run 187, type=split(ReduceOverlap)(split_times=[295]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.381[76,308] 1.03us    |-----------------------------------------L0.381-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[76,295] 1.03us 3mb  |---------------------------------------L0.?---------------------------------------|      "
    - "L0.?[296,308] 1.03us 156kb                                                                                     |L0.?|"
    - "**** Simulation run 188, type=split(ReduceOverlap)(split_times=[590]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.383[357,616] 1.03us   |-----------------------------------------L0.383-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[357,590] 1.03us 2mb |-------------------------------------L0.?-------------------------------------|          "
    - "L0.?[591,616] 1.03us 282kb                                                                                 |-L0.?-| "
    - "**** Simulation run 189, type=split(ReduceOverlap)(split_times=[295]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.385[42,308] 1.03us    |-----------------------------------------L0.385-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[42,295] 1.03us 3mb  |---------------------------------------L0.?----------------------------------------|     "
    - "L0.?[296,308] 1.03us 141kb                                                                                     |L0.?|"
    - "**** Simulation run 190, type=split(ReduceOverlap)(split_times=[590]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.387[357,616] 1.03us   |-----------------------------------------L0.387-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[357,590] 1.03us 3mb |-------------------------------------L0.?-------------------------------------|          "
    - "L0.?[591,616] 1.03us 343kb                                                                                 |-L0.?-| "
    - "**** Simulation run 191, type=split(ReduceOverlap)(split_times=[295]). 1 Input Files, 2mb total:"
    - "L0, all files 2mb                                                                                                  "
    - "L0.389[173,308] 1.03us   |-----------------------------------------L0.389-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 2mb total:"
    - "L0                                                                                                                 "
    - "L0.?[173,295] 1.03us 2mb |-------------------------------------L0.?--------------------------------------|         "
    - "L0.?[296,308] 1.03us 171kb                                                                                  |-L0.?-|"
    - "**** Simulation run 192, type=split(ReduceOverlap)(split_times=[590]). 1 Input Files, 4mb total:"
    - "L0, all files 4mb                                                                                                  "
    - "L0.391[357,616] 1.03us   |-----------------------------------------L0.391-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 4mb total:"
    - "L0                                                                                                                 "
    - "L0.?[357,590] 1.03us 4mb |-------------------------------------L0.?-------------------------------------|          "
    - "L0.?[591,616] 1.03us 462kb                                                                                 |-L0.?-| "
    - "**** Simulation run 193, type=split(ReduceOverlap)(split_times=[295]). 1 Input Files, 4mb total:"
    - "L0, all files 4mb                                                                                                  "
    - "L0.393[50,308] 1.03us    |-----------------------------------------L0.393-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 4mb total:"
    - "L0                                                                                                                 "
    - "L0.?[50,295] 1.03us 4mb  |---------------------------------------L0.?----------------------------------------|     "
    - "L0.?[296,308] 1.03us 230kb                                                                                     |L0.?|"
    - "**** Simulation run 194, type=split(ReduceOverlap)(split_times=[590]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.395[357,616] 1.04us   |-----------------------------------------L0.395-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[357,590] 1.04us 3mb |-------------------------------------L0.?-------------------------------------|          "
    - "L0.?[591,616] 1.04us 311kb                                                                                 |-L0.?-| "
    - "**** Simulation run 195, type=split(ReduceOverlap)(split_times=[295]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.397[76,308] 1.04us    |-----------------------------------------L0.397-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[76,295] 1.04us 3mb  |---------------------------------------L0.?---------------------------------------|      "
    - "L0.?[296,308] 1.04us 156kb                                                                                     |L0.?|"
    - "**** Simulation run 196, type=split(ReduceOverlap)(split_times=[590]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.399[357,616] 1.04us   |-----------------------------------------L0.399-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[357,590] 1.04us 2mb |-------------------------------------L0.?-------------------------------------|          "
    - "L0.?[591,616] 1.04us 282kb                                                                                 |-L0.?-| "
    - "**** Simulation run 197, type=split(ReduceOverlap)(split_times=[295]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.401[42,308] 1.04us    |-----------------------------------------L0.401-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[42,295] 1.04us 3mb  |---------------------------------------L0.?----------------------------------------|     "
    - "L0.?[296,308] 1.04us 141kb                                                                                     |L0.?|"
    - "**** Simulation run 198, type=split(ReduceOverlap)(split_times=[590]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.403[357,616] 1.04us   |-----------------------------------------L0.403-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[357,590] 1.04us 3mb |-------------------------------------L0.?-------------------------------------|          "
    - "L0.?[591,616] 1.04us 343kb                                                                                 |-L0.?-| "
    - "**** Simulation run 199, type=split(ReduceOverlap)(split_times=[295]). 1 Input Files, 2mb total:"
    - "L0, all files 2mb                                                                                                  "
    - "L0.405[173,308] 1.04us   |-----------------------------------------L0.405-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 2mb total:"
    - "L0                                                                                                                 "
    - "L0.?[173,295] 1.04us 2mb |-------------------------------------L0.?--------------------------------------|         "
    - "L0.?[296,308] 1.04us 171kb                                                                                  |-L0.?-|"
    - "**** Simulation run 200, type=split(ReduceOverlap)(split_times=[590]). 1 Input Files, 4mb total:"
    - "L0, all files 4mb                                                                                                  "
    - "L0.407[357,616] 1.04us   |-----------------------------------------L0.407-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 4mb total:"
    - "L0                                                                                                                 "
    - "L0.?[357,590] 1.04us 4mb |-------------------------------------L0.?-------------------------------------|          "
    - "L0.?[591,616] 1.04us 462kb                                                                                 |-L0.?-| "
    - "**** Simulation run 201, type=split(ReduceOverlap)(split_times=[295]). 1 Input Files, 4mb total:"
    - "L0, all files 4mb                                                                                                  "
    - "L0.409[50,308] 1.04us    |-----------------------------------------L0.409-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 4mb total:"
    - "L0                                                                                                                 "
    - "L0.?[50,295] 1.04us 4mb  |---------------------------------------L0.?----------------------------------------|     "
    - "L0.?[296,308] 1.04us 230kb                                                                                     |L0.?|"
    - "**** Simulation run 202, type=split(ReduceOverlap)(split_times=[590]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.411[357,616] 1.04us   |-----------------------------------------L0.411-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[357,590] 1.04us 3mb |-------------------------------------L0.?-------------------------------------|          "
    - "L0.?[591,616] 1.04us 311kb                                                                                 |-L0.?-| "
    - "**** Simulation run 203, type=split(ReduceOverlap)(split_times=[295]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.413[76,308] 1.04us    |-----------------------------------------L0.413-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[76,295] 1.04us 3mb  |---------------------------------------L0.?---------------------------------------|      "
    - "L0.?[296,308] 1.04us 156kb                                                                                     |L0.?|"
    - "**** Simulation run 204, type=split(ReduceOverlap)(split_times=[590]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.415[357,616] 1.04us   |-----------------------------------------L0.415-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[357,590] 1.04us 2mb |-------------------------------------L0.?-------------------------------------|          "
    - "L0.?[591,616] 1.04us 282kb                                                                                 |-L0.?-| "
    - "**** Simulation run 205, type=split(ReduceOverlap)(split_times=[295]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.417[42,308] 1.04us    |-----------------------------------------L0.417-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[42,295] 1.04us 3mb  |---------------------------------------L0.?----------------------------------------|     "
    - "L0.?[296,308] 1.04us 141kb                                                                                     |L0.?|"
    - "**** Simulation run 206, type=split(ReduceOverlap)(split_times=[590]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.419[357,616] 1.04us   |-----------------------------------------L0.419-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[357,590] 1.04us 3mb |-------------------------------------L0.?-------------------------------------|          "
    - "L0.?[591,616] 1.04us 343kb                                                                                 |-L0.?-| "
    - "**** Simulation run 207, type=split(ReduceOverlap)(split_times=[295]). 1 Input Files, 2mb total:"
    - "L0, all files 2mb                                                                                                  "
    - "L0.421[173,308] 1.04us   |-----------------------------------------L0.421-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 2mb total:"
    - "L0                                                                                                                 "
    - "L0.?[173,295] 1.04us 2mb |-------------------------------------L0.?--------------------------------------|         "
    - "L0.?[296,308] 1.04us 171kb                                                                                  |-L0.?-|"
    - "**** Simulation run 208, type=split(ReduceOverlap)(split_times=[590]). 1 Input Files, 4mb total:"
    - "L0, all files 4mb                                                                                                  "
    - "L0.423[357,616] 1.04us   |-----------------------------------------L0.423-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 4mb total:"
    - "L0                                                                                                                 "
    - "L0.?[357,590] 1.04us 4mb |-------------------------------------L0.?-------------------------------------|          "
    - "L0.?[591,616] 1.04us 462kb                                                                                 |-L0.?-| "
    - "**** Simulation run 209, type=split(ReduceOverlap)(split_times=[295]). 1 Input Files, 4mb total:"
    - "L0, all files 4mb                                                                                                  "
    - "L0.425[50,308] 1.04us    |-----------------------------------------L0.425-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 4mb total:"
    - "L0                                                                                                                 "
    - "L0.?[50,295] 1.04us 4mb  |---------------------------------------L0.?----------------------------------------|     "
    - "L0.?[296,308] 1.04us 230kb                                                                                     |L0.?|"
    - "**** Simulation run 210, type=split(ReduceOverlap)(split_times=[590]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.427[357,616] 1.04us   |-----------------------------------------L0.427-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[357,590] 1.04us 3mb |-------------------------------------L0.?-------------------------------------|          "
    - "L0.?[591,616] 1.04us 311kb                                                                                 |-L0.?-| "
    - "**** Simulation run 211, type=split(ReduceOverlap)(split_times=[295]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.429[76,308] 1.04us    |-----------------------------------------L0.429-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[76,295] 1.04us 3mb  |---------------------------------------L0.?---------------------------------------|      "
    - "L0.?[296,308] 1.04us 156kb                                                                                     |L0.?|"
    - "**** Simulation run 212, type=split(ReduceOverlap)(split_times=[590]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.431[357,616] 1.05us   |-----------------------------------------L0.431-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[357,590] 1.05us 2mb |-------------------------------------L0.?-------------------------------------|          "
    - "L0.?[591,616] 1.05us 282kb                                                                                 |-L0.?-| "
    - "**** Simulation run 213, type=split(ReduceOverlap)(split_times=[295]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.447[42,308] 1.05us    |-----------------------------------------L0.447-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[42,295] 1.05us 3mb  |---------------------------------------L0.?----------------------------------------|     "
    - "L0.?[296,308] 1.05us 141kb                                                                                     |L0.?|"
    - "**** Simulation run 214, type=split(ReduceOverlap)(split_times=[590]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.449[357,616] 1.05us   |-----------------------------------------L0.449-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[357,590] 1.05us 3mb |-------------------------------------L0.?-------------------------------------|          "
    - "L0.?[591,616] 1.05us 343kb                                                                                 |-L0.?-| "
    - "**** Simulation run 215, type=split(ReduceOverlap)(split_times=[295]). 1 Input Files, 2mb total:"
    - "L0, all files 2mb                                                                                                  "
    - "L0.433[173,308] 1.05us   |-----------------------------------------L0.433-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 2mb total:"
    - "L0                                                                                                                 "
    - "L0.?[173,295] 1.05us 2mb |-------------------------------------L0.?--------------------------------------|         "
    - "L0.?[296,308] 1.05us 171kb                                                                                  |-L0.?-|"
    - "**** Simulation run 216, type=split(ReduceOverlap)(split_times=[590]). 1 Input Files, 4mb total:"
    - "L0, all files 4mb                                                                                                  "
    - "L0.435[357,616] 1.05us   |-----------------------------------------L0.435-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 4mb total:"
    - "L0                                                                                                                 "
    - "L0.?[357,590] 1.05us 4mb |-------------------------------------L0.?-------------------------------------|          "
    - "L0.?[591,616] 1.05us 462kb                                                                                 |-L0.?-| "
    - "**** Simulation run 217, type=split(ReduceOverlap)(split_times=[295]). 1 Input Files, 4mb total:"
    - "L0, all files 4mb                                                                                                  "
    - "L0.437[50,308] 1.05us    |-----------------------------------------L0.437-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 4mb total:"
    - "L0                                                                                                                 "
    - "L0.?[50,295] 1.05us 4mb  |---------------------------------------L0.?----------------------------------------|     "
    - "L0.?[296,308] 1.05us 230kb                                                                                     |L0.?|"
    - "**** Simulation run 218, type=split(ReduceOverlap)(split_times=[590]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.439[357,616] 1.05us   |-----------------------------------------L0.439-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[357,590] 1.05us 3mb |-------------------------------------L0.?-------------------------------------|          "
    - "L0.?[591,616] 1.05us 311kb                                                                                 |-L0.?-| "
    - "**** Simulation run 219, type=split(ReduceOverlap)(split_times=[295]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.441[76,308] 1.05us    |-----------------------------------------L0.441-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[76,295] 1.05us 3mb  |---------------------------------------L0.?---------------------------------------|      "
    - "L0.?[296,308] 1.05us 156kb                                                                                     |L0.?|"
    - "**** Simulation run 220, type=split(ReduceOverlap)(split_times=[590]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.443[357,616] 1.05us   |-----------------------------------------L0.443-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[357,590] 1.05us 2mb |-------------------------------------L0.?-------------------------------------|          "
    - "L0.?[591,616] 1.05us 282kb                                                                                 |-L0.?-| "
    - "**** Simulation run 221, type=split(ReduceOverlap)(split_times=[295]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.445[42,308] 1.05us    |-----------------------------------------L0.445-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[42,295] 1.05us 3mb  |---------------------------------------L0.?----------------------------------------|     "
    - "L0.?[296,308] 1.05us 141kb                                                                                     |L0.?|"
    - "Committing partition 1:"
    - "  Soft Deleting 36 files: L0.379, L0.381, L0.383, L0.385, L0.387, L0.389, L0.391, L0.393, L0.395, L0.397, L0.399, L0.401, L0.403, L0.405, L0.407, L0.409, L0.411, L0.413, L0.415, L0.417, L0.419, L0.421, L0.423, L0.425, L0.427, L0.429, L0.431, L0.433, L0.435, L0.437, L0.439, L0.441, L0.443, L0.445, L0.447, L0.449"
    - "  Creating 72 files"
    - "**** Simulation run 222, type=split(CompactAndSplitOutput(ManySmallFiles))(split_times=[578]). 200 Input Files, 176mb total:"
    - "L0                                                                                                                 "
    - "L0.376[617,629] 1.03us 231kb                                                      |L0.376|                            "
    - "L0.468[76,295] 1.03us 3mb   |------L0.468------|                                                                   "
    - "L0.469[296,308] 1.03us 156kb                        |L0.469|                                                          "
    - "L0.382[309,319] 1.03us 132kb                         |L0.382|                                                         "
    - "L0.279[320,329] 1.03us 120kb                          |L0.279|                                                        "
    - "L0.206[330,356] 1.03us 323kb                           |L0.206|                                                       "
    - "L0.466[357,590] 1.03us 3mb                              |-------L0.466-------|                                      "
    - "L0.467[591,616] 1.03us 311kb                                                    |L0.467|                              "
    - "L0.380[617,638] 1.03us 263kb                                                      |L0.380|                            "
    - "L0.277[639,658] 1.03us 239kb                                                        |L0.277|                          "
    - "L0.204[659,670] 1.03us 144kb                                                          |L0.204|                        "
    - "L0.344[671,920] 1.03us 3mb                                                           |-------L0.344--------|        "
    - "L0.345[921,932] 1.03us 145kb                                                                                   |L0.345|"
    - "L0.472[42,295] 1.03us 3mb|--------L0.472--------|                                                                  "
    - "L0.473[296,308] 1.03us 141kb                        |L0.473|                                                          "
    - "L0.386[309,319] 1.03us 119kb                         |L0.386|                                                         "
    - "L0.283[320,329] 1.03us 108kb                          |L0.283|                                                        "
    - "L0.210[330,356] 1.03us 293kb                           |L0.210|                                                       "
    - "L0.470[357,590] 1.03us 2mb                              |-------L0.470-------|                                      "
    - "L0.471[591,616] 1.03us 282kb                                                    |L0.471|                              "
    - "L0.384[617,638] 1.03us 239kb                                                      |L0.384|                            "
    - "L0.281[639,658] 1.03us 217kb                                                        |L0.281|                          "
    - "L0.208[659,670] 1.03us 130kb                                                          |L0.208|                        "
    - "L0.346[671,920] 1.03us 3mb                                                           |-------L0.346--------|        "
    - "L0.453[921,973] 1.03us 576kb                                                                                   |L0.453|"
    - "L0.454[974,986] 1.03us 144kb                                                                                        |L0.454|"
    - "L0.476[173,295] 1.03us 2mb            |-L0.476--|                                                                   "
    - "L0.477[296,308] 1.03us 171kb                        |L0.477|                                                          "
    - "L0.390[309,319] 1.03us 145kb                         |L0.390|                                                         "
    - "L0.287[320,329] 1.03us 132kb                          |L0.287|                                                        "
    - "L0.214[330,356] 1.03us 356kb                           |L0.214|                                                       "
    - "L0.474[357,590] 1.03us 3mb                              |-------L0.474-------|                                      "
    - "L0.475[591,616] 1.03us 343kb                                                    |L0.475|                              "
    - "L0.388[617,638] 1.03us 290kb                                                      |L0.388|                            "
    - "L0.285[639,658] 1.03us 264kb                                                        |L0.285|                          "
    - "L0.212[659,670] 1.03us 158kb                                                          |L0.212|                        "
    - "L0.348[671,920] 1.03us 3mb                                                           |-------L0.348--------|        "
    - "L0.349[921,950] 1.03us 398kb                                                                                   |L0.349|"
    - "L0.480[50,295] 1.03us 4mb|-------L0.480--------|                                                                   "
    - "L0.481[296,308] 1.03us 230kb                        |L0.481|                                                          "
    - "L0.394[309,319] 1.03us 195kb                         |L0.394|                                                         "
    - "L0.289[320,329] 1.03us 177kb                          |L0.289|                                                        "
    - "L0.216[330,356] 1.03us 478kb                           |L0.216|                                                       "
    - "L0.478[357,590] 1.03us 4mb                              |-------L0.478-------|                                      "
    - "L0.479[591,616] 1.03us 462kb                                                    |L0.479|                              "
    - "L0.392[617,629] 1.03us 231kb                                                      |L0.392|                            "
    - "L0.484[76,295] 1.04us 3mb   |------L0.484------|                                                                   "
    - "L0.485[296,308] 1.04us 156kb                        |L0.485|                                                          "
    - "L0.398[309,319] 1.04us 132kb                         |L0.398|                                                         "
    - "L0.293[320,329] 1.04us 120kb                          |L0.293|                                                        "
    - "L0.220[330,356] 1.04us 323kb                           |L0.220|                                                       "
    - "L0.482[357,590] 1.04us 3mb                              |-------L0.482-------|                                      "
    - "L0.483[591,616] 1.04us 311kb                                                    |L0.483|                              "
    - "L0.396[617,638] 1.04us 263kb                                                      |L0.396|                            "
    - "L0.291[639,658] 1.04us 239kb                                                        |L0.291|                          "
    - "L0.218[659,670] 1.04us 144kb                                                          |L0.218|                        "
    - "L0.350[671,920] 1.04us 3mb                                                           |-------L0.350--------|        "
    - "L0.351[921,932] 1.04us 145kb                                                                                   |L0.351|"
    - "L0.488[42,295] 1.04us 3mb|--------L0.488--------|                                                                  "
    - "L0.489[296,308] 1.04us 141kb                        |L0.489|                                                          "
    - "L0.402[309,319] 1.04us 119kb                         |L0.402|                                                         "
    - "L0.297[320,329] 1.04us 108kb                          |L0.297|                                                        "
    - "L0.224[330,356] 1.04us 293kb                           |L0.224|                                                       "
    - "L0.486[357,590] 1.04us 2mb                              |-------L0.486-------|                                      "
    - "L0.487[591,616] 1.04us 282kb                                                    |L0.487|                              "
    - "L0.400[617,638] 1.04us 239kb                                                      |L0.400|                            "
    - "L0.295[639,658] 1.04us 217kb                                                        |L0.295|                          "
    - "L0.222[659,670] 1.04us 130kb                                                          |L0.222|                        "
    - "L0.352[671,920] 1.04us 3mb                                                           |-------L0.352--------|        "
    - "L0.455[921,973] 1.04us 576kb                                                                                   |L0.455|"
    - "L0.456[974,986] 1.04us 144kb                                                                                        |L0.456|"
    - "L0.492[173,295] 1.04us 2mb            |-L0.492--|                                                                   "
    - "L0.493[296,308] 1.04us 171kb                        |L0.493|                                                          "
    - "L0.406[309,319] 1.04us 145kb                         |L0.406|                                                         "
    - "L0.301[320,329] 1.04us 132kb                          |L0.301|                                                        "
    - "L0.228[330,356] 1.04us 356kb                           |L0.228|                                                       "
    - "L0.490[357,590] 1.04us 3mb                              |-------L0.490-------|                                      "
    - "L0.491[591,616] 1.04us 343kb                                                    |L0.491|                              "
    - "L0.404[617,638] 1.04us 290kb                                                      |L0.404|                            "
    - "L0.299[639,658] 1.04us 264kb                                                        |L0.299|                          "
    - "L0.226[659,670] 1.04us 158kb                                                          |L0.226|                        "
    - "L0.354[671,920] 1.04us 3mb                                                           |-------L0.354--------|        "
    - "L0.355[921,950] 1.04us 398kb                                                                                   |L0.355|"
    - "L0.496[50,295] 1.04us 4mb|-------L0.496--------|                                                                   "
    - "L0.497[296,308] 1.04us 230kb                        |L0.497|                                                          "
    - "L0.410[309,319] 1.04us 195kb                         |L0.410|                                                         "
    - "L0.303[320,329] 1.04us 177kb                          |L0.303|                                                        "
    - "L0.230[330,356] 1.04us 478kb                           |L0.230|                                                       "
    - "L0.494[357,590] 1.04us 4mb                              |-------L0.494-------|                                      "
    - "L0.495[591,616] 1.04us 462kb                                                    |L0.495|                              "
    - "L0.408[617,629] 1.04us 231kb                                                      |L0.408|                            "
    - "L0.500[76,295] 1.04us 3mb   |------L0.500------|                                                                   "
    - "L0.501[296,308] 1.04us 156kb                        |L0.501|                                                          "
    - "L0.414[309,319] 1.04us 132kb                         |L0.414|                                                         "
    - "L0.307[320,329] 1.04us 120kb                          |L0.307|                                                        "
    - "L0.234[330,356] 1.04us 323kb                           |L0.234|                                                       "
    - "L0.498[357,590] 1.04us 3mb                              |-------L0.498-------|                                      "
    - "L0.499[591,616] 1.04us 311kb                                                    |L0.499|                              "
    - "L0.412[617,638] 1.04us 263kb                                                      |L0.412|                            "
    - "L0.305[639,658] 1.04us 239kb                                                        |L0.305|                          "
    - "L0.232[659,670] 1.04us 144kb                                                          |L0.232|                        "
    - "L0.356[671,920] 1.04us 3mb                                                           |-------L0.356--------|        "
    - "L0.357[921,932] 1.04us 145kb                                                                                   |L0.357|"
    - "L0.504[42,295] 1.04us 3mb|--------L0.504--------|                                                                  "
    - "L0.505[296,308] 1.04us 141kb                        |L0.505|                                                          "
    - "L0.418[309,319] 1.04us 119kb                         |L0.418|                                                         "
    - "L0.311[320,329] 1.04us 108kb                          |L0.311|                                                        "
    - "L0.254[330,356] 1.04us 293kb                           |L0.254|                                                       "
    - "L0.502[357,590] 1.04us 2mb                              |-------L0.502-------|                                      "
    - "L0.503[591,616] 1.04us 282kb                                                    |L0.503|                              "
    - "L0.416[617,638] 1.04us 239kb                                                      |L0.416|                            "
    - "L0.309[639,658] 1.04us 217kb                                                        |L0.309|                          "
    - "L0.252[659,670] 1.04us 130kb                                                          |L0.252|                        "
    - "L0.358[671,920] 1.04us 3mb                                                           |-------L0.358--------|        "
    - "L0.457[921,973] 1.04us 576kb                                                                                   |L0.457|"
    - "L0.458[974,986] 1.04us 144kb                                                                                        |L0.458|"
    - "L0.508[173,295] 1.04us 2mb            |-L0.508--|                                                                   "
    - "L0.509[296,308] 1.04us 171kb                        |L0.509|                                                          "
    - "L0.422[309,319] 1.04us 145kb                         |L0.422|                                                         "
    - "L0.315[320,329] 1.04us 132kb                          |L0.315|                                                        "
    - "L0.238[330,356] 1.04us 356kb                           |L0.238|                                                       "
    - "L0.506[357,590] 1.04us 3mb                              |-------L0.506-------|                                      "
    - "L0.507[591,616] 1.04us 343kb                                                    |L0.507|                              "
    - "L0.420[617,638] 1.04us 290kb                                                      |L0.420|                            "
    - "L0.313[639,658] 1.04us 264kb                                                        |L0.313|                          "
    - "L0.236[659,670] 1.04us 158kb                                                          |L0.236|                        "
    - "L0.360[671,920] 1.04us 3mb                                                           |-------L0.360--------|        "
    - "L0.361[921,950] 1.04us 398kb                                                                                   |L0.361|"
    - "L0.512[50,295] 1.04us 4mb|-------L0.512--------|                                                                   "
    - "L0.513[296,308] 1.04us 230kb                        |L0.513|                                                          "
    - "L0.426[309,319] 1.04us 195kb                         |L0.426|                                                         "
    - "L0.317[320,329] 1.04us 177kb                          |L0.317|                                                        "
    - "L0.240[330,356] 1.04us 478kb                           |L0.240|                                                       "
    - "L0.510[357,590] 1.04us 4mb                              |-------L0.510-------|                                      "
    - "L0.511[591,616] 1.04us 462kb                                                    |L0.511|                              "
    - "L0.424[617,629] 1.04us 231kb                                                      |L0.424|                            "
    - "L0.516[76,295] 1.04us 3mb   |------L0.516------|                                                                   "
    - "L0.517[296,308] 1.04us 156kb                        |L0.517|                                                          "
    - "L0.430[309,319] 1.04us 132kb                         |L0.430|                                                         "
    - "L0.321[320,329] 1.04us 120kb                          |L0.321|                                                        "
    - "L0.244[330,356] 1.04us 323kb                           |L0.244|                                                       "
    - "L0.514[357,590] 1.04us 3mb                              |-------L0.514-------|                                      "
    - "L0.515[591,616] 1.04us 311kb                                                    |L0.515|                              "
    - "L0.428[617,638] 1.04us 263kb                                                      |L0.428|                            "
    - "L0.319[639,658] 1.04us 239kb                                                        |L0.319|                          "
    - "L0.242[659,670] 1.04us 144kb                                                          |L0.242|                        "
    - "L0.362[671,920] 1.04us 3mb                                                           |-------L0.362--------|        "
    - "L0.363[921,932] 1.04us 145kb                                                                                   |L0.363|"
    - "L0.520[42,295] 1.05us 3mb|--------L0.520--------|                                                                  "
    - "L0.521[296,308] 1.05us 141kb                        |L0.521|                                                          "
    - "L0.448[309,319] 1.05us 119kb                         |L0.448|                                                         "
    - "L0.325[320,329] 1.05us 108kb                          |L0.325|                                                        "
    - "L0.248[330,356] 1.05us 293kb                           |L0.248|                                                       "
    - "L0.518[357,590] 1.05us 2mb                              |-------L0.518-------|                                      "
    - "L0.519[591,616] 1.05us 282kb                                                    |L0.519|                              "
    - "L0.432[617,638] 1.05us 239kb                                                      |L0.432|                            "
    - "L0.323[639,658] 1.05us 217kb                                                        |L0.323|                          "
    - "L0.246[659,670] 1.05us 130kb                                                          |L0.246|                        "
    - "L0.364[671,920] 1.05us 3mb                                                           |-------L0.364--------|        "
    - "L0.459[921,973] 1.05us 576kb                                                                                   |L0.459|"
    - "L0.460[974,986] 1.05us 144kb                                                                                        |L0.460|"
    - "L0.524[173,295] 1.05us 2mb            |-L0.524--|                                                                   "
    - "L0.525[296,308] 1.05us 171kb                        |L0.525|                                                          "
    - "L0.434[309,319] 1.05us 145kb                         |L0.434|                                                         "
    - "L0.329[320,329] 1.05us 132kb                          |L0.329|                                                        "
    - "L0.256[330,356] 1.05us 356kb                           |L0.256|                                                       "
    - "L0.522[357,590] 1.05us 3mb                              |-------L0.522-------|                                      "
    - "L0.523[591,616] 1.05us 343kb                                                    |L0.523|                              "
    - "L0.450[617,638] 1.05us 290kb                                                      |L0.450|                            "
    - "L0.327[639,658] 1.05us 264kb                                                        |L0.327|                          "
    - "L0.250[659,670] 1.05us 158kb                                                          |L0.250|                        "
    - "L0.366[671,920] 1.05us 3mb                                                           |-------L0.366--------|        "
    - "L0.367[921,950] 1.05us 398kb                                                                                   |L0.367|"
    - "L0.528[50,295] 1.05us 4mb|-------L0.528--------|                                                                   "
    - "L0.529[296,308] 1.05us 230kb                        |L0.529|                                                          "
    - "L0.438[309,319] 1.05us 195kb                         |L0.438|                                                         "
    - "L0.331[320,329] 1.05us 177kb                          |L0.331|                                                        "
    - "L0.258[330,356] 1.05us 478kb                           |L0.258|                                                       "
    - "L0.526[357,590] 1.05us 4mb                              |-------L0.526-------|                                      "
    - "L0.527[591,616] 1.05us 462kb                                                    |L0.527|                              "
    - "L0.436[617,629] 1.05us 231kb                                                      |L0.436|                            "
    - "L0.532[76,295] 1.05us 3mb   |------L0.532------|                                                                   "
    - "L0.533[296,308] 1.05us 156kb                        |L0.533|                                                          "
    - "L0.442[309,319] 1.05us 132kb                         |L0.442|                                                         "
    - "L0.335[320,329] 1.05us 120kb                          |L0.335|                                                        "
    - "L0.262[330,356] 1.05us 323kb                           |L0.262|                                                       "
    - "L0.530[357,590] 1.05us 3mb                              |-------L0.530-------|                                      "
    - "L0.531[591,616] 1.05us 311kb                                                    |L0.531|                              "
    - "L0.440[617,638] 1.05us 263kb                                                      |L0.440|                            "
    - "L0.333[639,658] 1.05us 239kb                                                        |L0.333|                          "
    - "L0.260[659,670] 1.05us 144kb                                                          |L0.260|                        "
    - "L0.368[671,920] 1.05us 3mb                                                           |-------L0.368--------|        "
    - "L0.369[921,932] 1.05us 145kb                                                                                   |L0.369|"
    - "L0.536[42,295] 1.05us 3mb|--------L0.536--------|                                                                  "
    - "L0.537[296,308] 1.05us 141kb                        |L0.537|                                                          "
    - "L0.446[309,319] 1.05us 119kb                         |L0.446|                                                         "
    - "L0.339[320,329] 1.05us 108kb                          |L0.339|                                                        "
    - "L0.266[330,356] 1.05us 293kb                           |L0.266|                                                       "
    - "L0.534[357,590] 1.05us 2mb                              |-------L0.534-------|                                      "
    - "L0.535[591,616] 1.05us 282kb                                                    |L0.535|                              "
    - "**** 2 Output Files (parquet_file_id not yet assigned), 176mb total:"
    - "L0                                                                                                                 "
    - "L0.?[42,578] 1.05us 100mb|----------------------L0.?-----------------------|                                       "
    - "L0.?[579,986] 1.05us 76mb                                                   |----------------L0.?----------------| "
    - "Committing partition 1:"
    - "  Soft Deleting 200 files: L0.204, L0.206, L0.208, L0.210, L0.212, L0.214, L0.216, L0.218, L0.220, L0.222, L0.224, L0.226, L0.228, L0.230, L0.232, L0.234, L0.236, L0.238, L0.240, L0.242, L0.244, L0.246, L0.248, L0.250, L0.252, L0.254, L0.256, L0.258, L0.260, L0.262, L0.266, L0.277, L0.279, L0.281, L0.283, L0.285, L0.287, L0.289, L0.291, L0.293, L0.295, L0.297, L0.299, L0.301, L0.303, L0.305, L0.307, L0.309, L0.311, L0.313, L0.315, L0.317, L0.319, L0.321, L0.323, L0.325, L0.327, L0.329, L0.331, L0.333, L0.335, L0.339, L0.344, L0.345, L0.346, L0.348, L0.349, L0.350, L0.351, L0.352, L0.354, L0.355, L0.356, L0.357, L0.358, L0.360, L0.361, L0.362, L0.363, L0.364, L0.366, L0.367, L0.368, L0.369, L0.376, L0.380, L0.382, L0.384, L0.386, L0.388, L0.390, L0.392, L0.394, L0.396, L0.398, L0.400, L0.402, L0.404, L0.406, L0.408, L0.410, L0.412, L0.414, L0.416, L0.418, L0.420, L0.422, L0.424, L0.426, L0.428, L0.430, L0.432, L0.434, L0.436, L0.438, L0.440, L0.442, L0.446, L0.448, L0.450, L0.453, L0.454, L0.455, L0.456, L0.457, L0.458, L0.459, L0.460, L0.466, L0.467, L0.468, L0.469, L0.470, L0.471, L0.472, L0.473, L0.474, L0.475, L0.476, L0.477, L0.478, L0.479, L0.480, L0.481, L0.482, L0.483, L0.484, L0.485, L0.486, L0.487, L0.488, L0.489, L0.490, L0.491, L0.492, L0.493, L0.494, L0.495, L0.496, L0.497, L0.498, L0.499, L0.500, L0.501, L0.502, L0.503, L0.504, L0.505, L0.506, L0.507, L0.508, L0.509, L0.510, L0.511, L0.512, L0.513, L0.514, L0.515, L0.516, L0.517, L0.518, L0.519, L0.520, L0.521, L0.522, L0.523, L0.524, L0.525, L0.526, L0.527, L0.528, L0.529, L0.530, L0.531, L0.532, L0.533, L0.534, L0.535, L0.536, L0.537"
    - "  Creating 2 files"
    - "**** Simulation run 223, type=compact(ManySmallFiles). 6 Input Files, 4mb total:"
    - "L0                                                                                                                 "
    - "L0.444[617,638] 1.05us 239kb|L0.444|                                                                                  "
    - "L0.337[639,658] 1.05us 217kb     |L0.337|                                                                             "
    - "L0.264[659,670] 1.05us 130kb          |L0.264|                                                                        "
    - "L0.370[671,920] 1.05us 3mb             |--------------------------L0.370--------------------------|                 "
    - "L0.461[921,973] 1.05us 576kb                                                                          |--L0.461--|    "
    - "L0.462[974,986] 1.05us 144kb                                                                                       |L0.462|"
    - "**** 1 Output Files (parquet_file_id not yet assigned), 4mb total:"
    - "L0, all files 4mb                                                                                                  "
    - "L0.?[617,986] 1.05us     |------------------------------------------L0.?------------------------------------------|"
    - "Committing partition 1:"
    - "  Soft Deleting 6 files: L0.264, L0.337, L0.370, L0.444, L0.461, L0.462"
    - "  Creating 1 files"
    - "**** Simulation run 224, type=split(HighL0OverlapSingleFile)(split_times=[756]). 1 Input Files, 99mb total:"
    - "L1, all files 99mb                                                                                                 "
    - "L1.374[617,920] 1.03us   |-----------------------------------------L1.374-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 99mb total:"
    - "L1                                                                                                                 "
    - "L1.?[617,756] 1.03us 45mb|-----------------L1.?------------------|                                                 "
    - "L1.?[757,920] 1.03us 54mb                                         |---------------------L1.?---------------------| "
    - "**** Simulation run 225, type=split(HighL0OverlapSingleFile)(split_times=[526]). 1 Input Files, 100mb total:"
    - "L1, all files 100mb                                                                                                "
    - "L1.464[296,590] 1.03us   |-----------------------------------------L1.464-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 100mb total:"
    - "L1                                                                                                                 "
    - "L1.?[296,526] 1.03us 78mb|--------------------------------L1.?--------------------------------|                    "
    - "L1.?[527,590] 1.03us 22mb                                                                      |------L1.?-------| "
    - "**** Simulation run 226, type=split(HighL0OverlapSingleFile)(split_times=[756]). 1 Input Files, 4mb total:"
    - "L0, all files 4mb                                                                                                  "
    - "L0.540[617,986] 1.05us   |-----------------------------------------L0.540-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 4mb total:"
    - "L0                                                                                                                 "
    - "L0.?[617,756] 1.05us 1mb |-------------L0.?--------------|                                                         "
    - "L0.?[757,986] 1.05us 2mb                                   |------------------------L0.?-------------------------| "
    - "**** Simulation run 227, type=split(HighL0OverlapSingleFile)(split_times=[756]). 1 Input Files, 76mb total:"
    - "L0, all files 76mb                                                                                                 "
    - "L0.539[579,986] 1.05us   |-----------------------------------------L0.539-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 76mb total:"
    - "L0                                                                                                                 "
    - "L0.?[579,756] 1.05us 33mb|----------------L0.?-----------------|                                                   "
    - "L0.?[757,986] 1.05us 43mb                                       |----------------------L0.?----------------------| "
    - "Committing partition 1:"
    - "  Soft Deleting 4 files: L1.374, L1.464, L0.539, L0.540"
    - "  Creating 8 files"
    - "**** Simulation run 228, type=split(HighL0OverlapSingleFile)(split_times=[196]). 1 Input Files, 100mb total:"
    - "L1, all files 100mb                                                                                                "
    - "L1.463[0,295] 1.03us     |-----------------------------------------L1.463-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 100mb total:"
    - "L1                                                                                                                 "
    - "L1.?[0,196] 1.03us 67mb  |--------------------------L1.?---------------------------|                               "
    - "L1.?[197,295] 1.03us 34mb                                                            |-----------L1.?------------| "
    - "**** Simulation run 229, type=split(HighL0OverlapSingleFile)(split_times=[392]). 1 Input Files, 78mb total:"
    - "L1, all files 78mb                                                                                                 "
    - "L1.543[296,526] 1.03us   |-----------------------------------------L1.543-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 78mb total:"
    - "L1                                                                                                                 "
    - "L1.?[296,392] 1.03us 33mb|---------------L1.?----------------|                                                     "
    - "L1.?[393,526] 1.03us 46mb                                     |-----------------------L1.?-----------------------| "
    - "**** Simulation run 230, type=split(HighL0OverlapSingleFile)(split_times=[196, 392]). 1 Input Files, 100mb total:"
    - "L0, all files 100mb                                                                                                "
    - "L0.538[42,578] 1.05us    |-----------------------------------------L0.538-----------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 100mb total:"
    - "L0                                                                                                                 "
    - "L0.?[42,196] 1.05us 29mb |---------L0.?----------|                                                                 "
    - "L0.?[197,392] 1.05us 36mb                          |-------------L0.?-------------|                                "
    - "L0.?[393,578] 1.05us 35mb                                                          |------------L0.?-------------| "
    - "Committing partition 1:"
    - "  Soft Deleting 3 files: L1.463, L0.538, L1.543"
    - "  Creating 7 files"
    - "**** Simulation run 231, type=split(ReduceOverlap)(split_times=[920, 973]). 1 Input Files, 2mb total:"
    - "L0, all files 2mb                                                                                                  "
    - "L0.546[757,986] 1.05us   |-----------------------------------------L0.546-----------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 2mb total:"
    - "L0                                                                                                                 "
    - "L0.?[757,920] 1.05us 2mb |-----------------------------L0.?-----------------------------|                          "
    - "L0.?[921,973] 1.05us 570kb                                                                |-------L0.?-------|      "
    - "L0.?[974,986] 1.05us 153kb                                                                                     |L0.?|"
    - "**** Simulation run 232, type=split(ReduceOverlap)(split_times=[590, 616]). 1 Input Files, 33mb total:"
    - "L0, all files 33mb                                                                                                 "
    - "L0.547[579,756] 1.05us   |-----------------------------------------L0.547-----------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 33mb total:"
    - "L0                                                                                                                 "
    - "L0.?[579,590] 1.05us 2mb |L0.?|                                                                                    "
    - "L0.?[591,616] 1.05us 5mb       |---L0.?---|                                                                        "
    - "L0.?[617,756] 1.05us 26mb                   |--------------------------------L0.?--------------------------------| "
    - "**** Simulation run 233, type=split(ReduceOverlap)(split_times=[920, 973]). 1 Input Files, 43mb total:"
    - "L0, all files 43mb                                                                                                 "
    - "L0.548[757,986] 1.05us   |-----------------------------------------L0.548-----------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 43mb total:"
    - "L0                                                                                                                 "
    - "L0.?[757,920] 1.05us 31mb|-----------------------------L0.?-----------------------------|                          "
    - "L0.?[921,973] 1.05us 10mb                                                                |-------L0.?-------|      "
    - "L0.?[974,986] 1.05us 3mb                                                                                      |L0.?|"
    - "**** Simulation run 234, type=split(ReduceOverlap)(split_times=[526]). 1 Input Files, 35mb total:"
    - "L0, all files 35mb                                                                                                 "
    - "L0.555[393,578] 1.05us   |-----------------------------------------L0.555-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 35mb total:"
    - "L0                                                                                                                 "
    - "L0.?[393,526] 1.05us 25mb|-----------------------------L0.?-----------------------------|                          "
    - "L0.?[527,578] 1.05us 10mb                                                                 |---------L0.?---------| "
    - "**** Simulation run 235, type=split(ReduceOverlap)(split_times=[295]). 1 Input Files, 36mb total:"
    - "L0, all files 36mb                                                                                                 "
    - "L0.554[197,392] 1.05us   |-----------------------------------------L0.554-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 36mb total:"
    - "L0                                                                                                                 "
    - "L0.?[197,295] 1.05us 18mb|-------------------L0.?--------------------|                                             "
    - "L0.?[296,392] 1.05us 18mb                                             |-------------------L0.?-------------------| "
    - "Committing partition 1:"
    - "  Soft Deleting 5 files: L0.546, L0.547, L0.548, L0.554, L0.555"
    - "  Creating 13 files"
    - "**** Simulation run 236, type=split(CompactAndSplitOutput(FoundSubsetLessThanMaxCompactSize))(split_times=[196, 392]). 8 Input Files, 269mb total:"
    - "L0                                                                                                                 "
    - "L0.553[42,196] 1.05us 29mb       |---------L0.553---------|                                                         "
    - "L0.567[197,295] 1.05us 18mb                                 |----L0.567----|                                         "
    - "L0.568[296,392] 1.05us 18mb                                                  |----L0.568----|                        "
    - "L0.565[393,526] 1.05us 25mb                                                                   |-------L0.565-------| "
    - "L1                                                                                                                 "
    - "L1.549[0,196] 1.03us 67mb|------------L1.549-------------|                                                         "
    - "L1.550[197,295] 1.03us 34mb                                 |----L1.550----|                                         "
    - "L1.551[296,392] 1.03us 33mb                                                  |----L1.551----|                        "
    - "L1.552[393,526] 1.03us 46mb                                                                   |-------L1.552-------| "
    - "**** 3 Output Files (parquet_file_id not yet assigned), 269mb total:"
    - "L1                                                                                                                 "
    - "L1.?[0,196] 1.05us 100mb |-------------L1.?--------------|                                                         "
    - "L1.?[197,392] 1.05us 100mb                                 |-------------L1.?--------------|                        "
    - "L1.?[393,526] 1.05us 69mb                                                                   |--------L1.?--------| "
    - "Committing partition 1:"
    - "  Soft Deleting 8 files: L1.549, L1.550, L1.551, L1.552, L0.553, L0.565, L0.567, L0.568"
    - "  Creating 3 files"
    - "**** Simulation run 237, type=split(CompactAndSplitOutput(TotalSizeLessThanMaxCompactSize))(split_times=[718, 909]). 17 Input Files, 241mb total:"
    - "L0                                                                                                                 "
    - "L0.558[974,986] 1.05us 153kb                                                                                       |L0.558|"
    - "L0.564[974,986] 1.05us 3mb                                                                                       |L0.564|"
    - "L0.557[921,973] 1.05us 570kb                                                                             |-L0.557-|   "
    - "L0.563[921,973] 1.05us 10mb                                                                             |-L0.563-|   "
    - "L0.556[757,920] 1.05us 2mb                                             |-----------L0.556------------|              "
    - "L0.562[757,920] 1.05us 31mb                                             |-----------L0.562------------|              "
    - "L0.561[617,756] 1.05us 26mb                 |---------L0.561----------|                                              "
    - "L0.545[617,756] 1.05us 1mb                 |---------L0.545----------|                                              "
    - "L0.560[591,616] 1.05us 5mb            |L0.560|                                                                      "
    - "L0.559[579,590] 1.05us 2mb          |L0.559|                                                                        "
    - "L0.566[527,578] 1.05us 10mb|-L0.566-|                                                                                "
    - "L1                                                                                                                 "
    - "L1.544[527,590] 1.03us 22mb|--L1.544--|                                                                              "
    - "L1.452[974,986] 1.03us 4mb                                                                                       |L1.452|"
    - "L1.465[591,616] 1.03us 9mb            |L1.465|                                                                      "
    - "L1.541[617,756] 1.03us 45mb                 |---------L1.541----------|                                              "
    - "L1.542[757,920] 1.03us 54mb                                             |-----------L1.542------------|              "
    - "L1.451[921,973] 1.03us 17mb                                                                             |-L1.451-|   "
    - "**** 3 Output Files (parquet_file_id not yet assigned), 241mb total:"
    - "L1                                                                                                                 "
    - "L1.?[527,718] 1.05us 100mb|---------------L1.?----------------|                                                     "
    - "L1.?[719,909] 1.05us 100mb                                     |---------------L1.?----------------|                "
    - "L1.?[910,986] 1.05us 41mb                                                                           |----L1.?----| "
    - "Committing partition 1:"
    - "  Soft Deleting 17 files: L1.451, L1.452, L1.465, L1.541, L1.542, L1.544, L0.545, L0.556, L0.557, L0.558, L0.559, L0.560, L0.561, L0.562, L0.563, L0.564, L0.566"
    - "  Creating 3 files"
    - "**** Simulation run 238, type=split(CompactAndSplitOutput(FoundSubsetLessThanMaxCompactSize))(split_times=[391, 585]). 3 Input Files, 269mb total:"
    - "L1                                                                                                                 "
    - "L1.570[197,392] 1.05us 100mb|------------L1.570-------------|                                                         "
    - "L1.571[393,526] 1.05us 69mb                                 |-------L1.571-------|                                   "
    - "L1.572[527,718] 1.05us 100mb                                                         |------------L1.572------------| "
    - "**** 3 Output Files (parquet_file_id not yet assigned), 269mb total:"
    - "L2                                                                                                                 "
    - "L2.?[197,391] 1.05us 100mb|-------------L2.?--------------|                                                         "
    - "L2.?[392,585] 1.05us 100mb                                 |-------------L2.?--------------|                        "
    - "L2.?[586,718] 1.05us 69mb                                                                   |--------L2.?--------| "
    - "Committing partition 1:"
    - "  Soft Deleting 3 files: L1.570, L1.571, L1.572"
    - "  Upgrading 1 files level to CompactionLevel::L2: L1.569"
    - "  Creating 3 files"
    - "**** Simulation run 239, type=split(CompactAndSplitOutput(TotalSizeLessThanMaxCompactSize))(split_times=[909]). 2 Input Files, 141mb total:"
    - "L1                                                                                                                 "
    - "L1.574[910,986] 1.05us 41mb                                                                |--------L1.574---------| "
    - "L1.573[719,909] 1.05us 100mb|----------------------------L1.573----------------------------|                          "
    - "**** 2 Output Files (parquet_file_id not yet assigned), 141mb total:"
    - "L2                                                                                                                 "
    - "L2.?[719,909] 1.05us 100mb|-----------------------------L2.?-----------------------------|                          "
    - "L2.?[910,986] 1.05us 41mb                                                                |---------L2.?----------| "
    - "Committing partition 1:"
    - "  Soft Deleting 2 files: L1.573, L1.574"
    - "  Creating 2 files"
    - "**** Final Output Files (3.9gb written)"
    - "L2                                                                                                                 "
    - "L2.569[0,196] 1.05us 100mb|----L2.569-----|                                                                         "
    - "L2.575[197,391] 1.05us 100mb                 |----L2.575-----|                                                        "
    - "L2.576[392,585] 1.05us 100mb                                   |----L2.576-----|                                      "
    - "L2.577[586,718] 1.05us 69mb                                                     |--L2.577--|                         "
    - "L2.578[719,909] 1.05us 100mb                                                                 |----L2.578-----|        "
    - "L2.579[910,986] 1.05us 41mb                                                                                   |L2.579|"
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
    - "L0.?[76,356] 1us 3mb     |-----------L0.?------------|                                                             "
    - "L0.?[357,670] 1us 4mb                                 |-------------L0.?-------------|                             "
    - "L0.?[671,932] 1us 3mb                                                                  |----------L0.?-----------| "
    - "**** Simulation run 1, type=split(HighL0OverlapSingleFile)(split_times=[356, 670]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.12[42,986] 1us        |-----------------------------------------L0.12------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[42,356] 1us 3mb     |-----------L0.?------------|                                                             "
    - "L0.?[357,670] 1us 3mb                                  |-----------L0.?------------|                               "
    - "L0.?[671,986] 1us 3mb                                                               |------------L0.?------------| "
    - "**** Simulation run 2, type=split(HighL0OverlapSingleFile)(split_times=[356, 670]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.13[173,950] 1us       |-----------------------------------------L0.13------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[173,356] 1us 2mb    |-------L0.?--------|                                                                     "
    - "L0.?[357,670] 1us 4mb                         |---------------L0.?---------------|                                 "
    - "L0.?[671,950] 1us 4mb                                                             |-------------L0.?-------------| "
    - "**** Simulation run 3, type=split(HighL0OverlapSingleFile)(split_times=[356]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.14[50,629] 1us        |-----------------------------------------L0.14------------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[50,356] 1us 5mb     |--------------------L0.?---------------------|                                           "
    - "L0.?[357,629] 1us 5mb                                                   |------------------L0.?------------------| "
    - "**** Simulation run 4, type=split(HighL0OverlapSingleFile)(split_times=[356, 670]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.15[76,932] 1us        |-----------------------------------------L0.15------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[76,356] 1us 3mb     |-----------L0.?------------|                                                             "
    - "L0.?[357,670] 1us 4mb                                 |-------------L0.?-------------|                             "
    - "L0.?[671,932] 1us 3mb                                                                  |----------L0.?-----------| "
    - "**** Simulation run 5, type=split(HighL0OverlapSingleFile)(split_times=[356, 670]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.16[42,986] 1us        |-----------------------------------------L0.16------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[42,356] 1us 3mb     |-----------L0.?------------|                                                             "
    - "L0.?[357,670] 1us 3mb                                  |-----------L0.?------------|                               "
    - "L0.?[671,986] 1us 3mb                                                               |------------L0.?------------| "
    - "**** Simulation run 6, type=split(HighL0OverlapSingleFile)(split_times=[356, 670]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.17[173,950] 1.01us    |-----------------------------------------L0.17------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[173,356] 1.01us 2mb |-------L0.?--------|                                                                     "
    - "L0.?[357,670] 1.01us 4mb                      |---------------L0.?---------------|                                 "
    - "L0.?[671,950] 1.01us 4mb                                                          |-------------L0.?-------------| "
    - "**** Simulation run 7, type=split(HighL0OverlapSingleFile)(split_times=[356]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.18[50,629] 1.01us     |-----------------------------------------L0.18------------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[50,356] 1.01us 5mb  |--------------------L0.?---------------------|                                           "
    - "L0.?[357,629] 1.01us 5mb                                                |------------------L0.?------------------| "
    - "**** Simulation run 8, type=split(HighL0OverlapSingleFile)(split_times=[356, 670]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.19[76,932] 1.01us     |-----------------------------------------L0.19------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[76,356] 1.01us 3mb  |-----------L0.?------------|                                                             "
    - "L0.?[357,670] 1.01us 4mb                              |-------------L0.?-------------|                             "
    - "L0.?[671,932] 1.01us 3mb                                                               |----------L0.?-----------| "
    - "**** Simulation run 9, type=split(HighL0OverlapSingleFile)(split_times=[356, 670]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.20[42,986] 1.01us     |-----------------------------------------L0.20------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[42,356] 1.01us 3mb  |-----------L0.?------------|                                                             "
    - "L0.?[357,670] 1.01us 3mb                               |-----------L0.?------------|                               "
    - "L0.?[671,986] 1.01us 3mb                                                            |------------L0.?------------| "
    - "**** Simulation run 10, type=split(HighL0OverlapSingleFile)(split_times=[356, 670]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.21[173,950] 1.01us    |-----------------------------------------L0.21------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[173,356] 1.01us 2mb |-------L0.?--------|                                                                     "
    - "L0.?[357,670] 1.01us 4mb                      |---------------L0.?---------------|                                 "
    - "L0.?[671,950] 1.01us 4mb                                                          |-------------L0.?-------------| "
    - "**** Simulation run 11, type=split(HighL0OverlapSingleFile)(split_times=[356]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.22[50,629] 1.01us     |-----------------------------------------L0.22------------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[50,356] 1.01us 5mb  |--------------------L0.?---------------------|                                           "
    - "L0.?[357,629] 1.01us 5mb                                                |------------------L0.?------------------| "
    - "**** Simulation run 12, type=split(HighL0OverlapSingleFile)(split_times=[356, 670]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.23[76,932] 1.01us     |-----------------------------------------L0.23------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[76,356] 1.01us 3mb  |-----------L0.?------------|                                                             "
    - "L0.?[357,670] 1.01us 4mb                              |-------------L0.?-------------|                             "
    - "L0.?[671,932] 1.01us 3mb                                                               |----------L0.?-----------| "
    - "**** Simulation run 13, type=split(HighL0OverlapSingleFile)(split_times=[356, 670]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.24[42,986] 1.01us     |-----------------------------------------L0.24------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[42,356] 1.01us 3mb  |-----------L0.?------------|                                                             "
    - "L0.?[357,670] 1.01us 3mb                               |-----------L0.?------------|                               "
    - "L0.?[671,986] 1.01us 3mb                                                            |------------L0.?------------| "
    - "**** Simulation run 14, type=split(HighL0OverlapSingleFile)(split_times=[356, 670]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.25[173,950] 1.01us    |-----------------------------------------L0.25------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[173,356] 1.01us 2mb |-------L0.?--------|                                                                     "
    - "L0.?[357,670] 1.01us 4mb                      |---------------L0.?---------------|                                 "
    - "L0.?[671,950] 1.01us 4mb                                                          |-------------L0.?-------------| "
    - "**** Simulation run 15, type=split(HighL0OverlapSingleFile)(split_times=[356]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.26[50,629] 1.01us     |-----------------------------------------L0.26------------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[50,356] 1.01us 5mb  |--------------------L0.?---------------------|                                           "
    - "L0.?[357,629] 1.01us 5mb                                                |------------------L0.?------------------| "
    - "**** Simulation run 16, type=split(HighL0OverlapSingleFile)(split_times=[356, 670]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.27[76,932] 1.02us     |-----------------------------------------L0.27------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[76,356] 1.02us 3mb  |-----------L0.?------------|                                                             "
    - "L0.?[357,670] 1.02us 4mb                              |-------------L0.?-------------|                             "
    - "L0.?[671,932] 1.02us 3mb                                                               |----------L0.?-----------| "
    - "**** Simulation run 17, type=split(HighL0OverlapSingleFile)(split_times=[356, 670]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.28[42,986] 1.02us     |-----------------------------------------L0.28------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[42,356] 1.02us 3mb  |-----------L0.?------------|                                                             "
    - "L0.?[357,670] 1.02us 3mb                               |-----------L0.?------------|                               "
    - "L0.?[671,986] 1.02us 3mb                                                            |------------L0.?------------| "
    - "**** Simulation run 18, type=split(HighL0OverlapSingleFile)(split_times=[356, 670]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.29[173,950] 1.02us    |-----------------------------------------L0.29------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[173,356] 1.02us 2mb |-------L0.?--------|                                                                     "
    - "L0.?[357,670] 1.02us 4mb                      |---------------L0.?---------------|                                 "
    - "L0.?[671,950] 1.02us 4mb                                                          |-------------L0.?-------------| "
    - "**** Simulation run 19, type=split(HighL0OverlapSingleFile)(split_times=[356]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.30[50,629] 1.02us     |-----------------------------------------L0.30------------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[50,356] 1.02us 5mb  |--------------------L0.?---------------------|                                           "
    - "L0.?[357,629] 1.02us 5mb                                                |------------------L0.?------------------| "
    - "**** Simulation run 20, type=split(HighL0OverlapSingleFile)(split_times=[356, 670]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.31[76,932] 1.02us     |-----------------------------------------L0.31------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[76,356] 1.02us 3mb  |-----------L0.?------------|                                                             "
    - "L0.?[357,670] 1.02us 4mb                              |-------------L0.?-------------|                             "
    - "L0.?[671,932] 1.02us 3mb                                                               |----------L0.?-----------| "
    - "**** Simulation run 21, type=split(HighL0OverlapSingleFile)(split_times=[356, 670]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.32[42,986] 1.02us     |-----------------------------------------L0.32------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[42,356] 1.02us 3mb  |-----------L0.?------------|                                                             "
    - "L0.?[357,670] 1.02us 3mb                               |-----------L0.?------------|                               "
    - "L0.?[671,986] 1.02us 3mb                                                            |------------L0.?------------| "
    - "**** Simulation run 22, type=split(HighL0OverlapSingleFile)(split_times=[356, 670]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.33[173,950] 1.02us    |-----------------------------------------L0.33------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[173,356] 1.02us 2mb |-------L0.?--------|                                                                     "
    - "L0.?[357,670] 1.02us 4mb                      |---------------L0.?---------------|                                 "
    - "L0.?[671,950] 1.02us 4mb                                                          |-------------L0.?-------------| "
    - "**** Simulation run 23, type=split(HighL0OverlapSingleFile)(split_times=[356]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.34[50,629] 1.02us     |-----------------------------------------L0.34------------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[50,356] 1.02us 5mb  |--------------------L0.?---------------------|                                           "
    - "L0.?[357,629] 1.02us 5mb                                                |------------------L0.?------------------| "
    - "**** Simulation run 24, type=split(HighL0OverlapSingleFile)(split_times=[356, 670]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.35[76,932] 1.02us     |-----------------------------------------L0.35------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[76,356] 1.02us 3mb  |-----------L0.?------------|                                                             "
    - "L0.?[357,670] 1.02us 4mb                              |-------------L0.?-------------|                             "
    - "L0.?[671,932] 1.02us 3mb                                                               |----------L0.?-----------| "
    - "**** Simulation run 25, type=split(HighL0OverlapSingleFile)(split_times=[356, 670]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.36[42,986] 1.02us     |-----------------------------------------L0.36------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[42,356] 1.02us 3mb  |-----------L0.?------------|                                                             "
    - "L0.?[357,670] 1.02us 3mb                               |-----------L0.?------------|                               "
    - "L0.?[671,986] 1.02us 3mb                                                            |------------L0.?------------| "
    - "**** Simulation run 26, type=split(HighL0OverlapSingleFile)(split_times=[356, 670]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.37[173,950] 1.03us    |-----------------------------------------L0.37------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[173,356] 1.03us 2mb |-------L0.?--------|                                                                     "
    - "L0.?[357,670] 1.03us 4mb                      |---------------L0.?---------------|                                 "
    - "L0.?[671,950] 1.03us 4mb                                                          |-------------L0.?-------------| "
    - "**** Simulation run 27, type=split(HighL0OverlapSingleFile)(split_times=[356]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.38[50,629] 1.03us     |-----------------------------------------L0.38------------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[50,356] 1.03us 5mb  |--------------------L0.?---------------------|                                           "
    - "L0.?[357,629] 1.03us 5mb                                                |------------------L0.?------------------| "
    - "**** Simulation run 28, type=split(HighL0OverlapSingleFile)(split_times=[356, 670]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.39[76,932] 1.03us     |-----------------------------------------L0.39------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[76,356] 1.03us 3mb  |-----------L0.?------------|                                                             "
    - "L0.?[357,670] 1.03us 4mb                              |-------------L0.?-------------|                             "
    - "L0.?[671,932] 1.03us 3mb                                                               |----------L0.?-----------| "
    - "**** Simulation run 29, type=split(HighL0OverlapSingleFile)(split_times=[356, 670]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.40[42,986] 1.03us     |-----------------------------------------L0.40------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[42,356] 1.03us 3mb  |-----------L0.?------------|                                                             "
    - "L0.?[357,670] 1.03us 3mb                               |-----------L0.?------------|                               "
    - "L0.?[671,986] 1.03us 3mb                                                            |------------L0.?------------| "
    - "**** Simulation run 30, type=split(HighL0OverlapSingleFile)(split_times=[356, 670]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.41[173,950] 1.03us    |-----------------------------------------L0.41------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[173,356] 1.03us 2mb |-------L0.?--------|                                                                     "
    - "L0.?[357,670] 1.03us 4mb                      |---------------L0.?---------------|                                 "
    - "L0.?[671,950] 1.03us 4mb                                                          |-------------L0.?-------------| "
    - "**** Simulation run 31, type=split(HighL0OverlapSingleFile)(split_times=[356]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.42[50,629] 1.03us     |-----------------------------------------L0.42------------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[50,356] 1.03us 5mb  |--------------------L0.?---------------------|                                           "
    - "L0.?[357,629] 1.03us 5mb                                                |------------------L0.?------------------| "
    - "**** Simulation run 32, type=split(HighL0OverlapSingleFile)(split_times=[356, 670]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.43[76,932] 1.03us     |-----------------------------------------L0.43------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[76,356] 1.03us 3mb  |-----------L0.?------------|                                                             "
    - "L0.?[357,670] 1.03us 4mb                              |-------------L0.?-------------|                             "
    - "L0.?[671,932] 1.03us 3mb                                                               |----------L0.?-----------| "
    - "**** Simulation run 33, type=split(HighL0OverlapSingleFile)(split_times=[356, 670]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.44[42,986] 1.03us     |-----------------------------------------L0.44------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[42,356] 1.03us 3mb  |-----------L0.?------------|                                                             "
    - "L0.?[357,670] 1.03us 3mb                               |-----------L0.?------------|                               "
    - "L0.?[671,986] 1.03us 3mb                                                            |------------L0.?------------| "
    - "**** Simulation run 34, type=split(HighL0OverlapSingleFile)(split_times=[356, 670]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.45[173,950] 1.03us    |-----------------------------------------L0.45------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[173,356] 1.03us 2mb |-------L0.?--------|                                                                     "
    - "L0.?[357,670] 1.03us 4mb                      |---------------L0.?---------------|                                 "
    - "L0.?[671,950] 1.03us 4mb                                                          |-------------L0.?-------------| "
    - "**** Simulation run 35, type=split(HighL0OverlapSingleFile)(split_times=[356]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.46[50,629] 1.03us     |-----------------------------------------L0.46------------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[50,356] 1.03us 5mb  |--------------------L0.?---------------------|                                           "
    - "L0.?[357,629] 1.03us 5mb                                                |------------------L0.?------------------| "
    - "**** Simulation run 36, type=split(HighL0OverlapSingleFile)(split_times=[356, 670]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.47[76,932] 1.04us     |-----------------------------------------L0.47------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[76,356] 1.04us 3mb  |-----------L0.?------------|                                                             "
    - "L0.?[357,670] 1.04us 4mb                              |-------------L0.?-------------|                             "
    - "L0.?[671,932] 1.04us 3mb                                                               |----------L0.?-----------| "
    - "**** Simulation run 37, type=split(HighL0OverlapSingleFile)(split_times=[356, 670]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.48[42,986] 1.04us     |-----------------------------------------L0.48------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[42,356] 1.04us 3mb  |-----------L0.?------------|                                                             "
    - "L0.?[357,670] 1.04us 3mb                               |-----------L0.?------------|                               "
    - "L0.?[671,986] 1.04us 3mb                                                            |------------L0.?------------| "
    - "**** Simulation run 38, type=split(HighL0OverlapSingleFile)(split_times=[356, 670]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.49[173,950] 1.04us    |-----------------------------------------L0.49------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[173,356] 1.04us 2mb |-------L0.?--------|                                                                     "
    - "L0.?[357,670] 1.04us 4mb                      |---------------L0.?---------------|                                 "
    - "L0.?[671,950] 1.04us 4mb                                                          |-------------L0.?-------------| "
    - "**** Simulation run 39, type=split(HighL0OverlapSingleFile)(split_times=[356]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.50[50,629] 1.04us     |-----------------------------------------L0.50------------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[50,356] 1.04us 5mb  |--------------------L0.?---------------------|                                           "
    - "L0.?[357,629] 1.04us 5mb                                                |------------------L0.?------------------| "
    - "**** Simulation run 40, type=split(HighL0OverlapSingleFile)(split_times=[356, 670]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.51[76,932] 1.04us     |-----------------------------------------L0.51------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[76,356] 1.04us 3mb  |-----------L0.?------------|                                                             "
    - "L0.?[357,670] 1.04us 4mb                              |-------------L0.?-------------|                             "
    - "L0.?[671,932] 1.04us 3mb                                                               |----------L0.?-----------| "
    - "**** Simulation run 41, type=split(HighL0OverlapSingleFile)(split_times=[356, 670]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.52[42,986] 1.04us     |-----------------------------------------L0.52------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[42,356] 1.04us 3mb  |-----------L0.?------------|                                                             "
    - "L0.?[357,670] 1.04us 3mb                               |-----------L0.?------------|                               "
    - "L0.?[671,986] 1.04us 3mb                                                            |------------L0.?------------| "
    - "**** Simulation run 42, type=split(HighL0OverlapSingleFile)(split_times=[356, 670]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.53[173,950] 1.04us    |-----------------------------------------L0.53------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[173,356] 1.04us 2mb |-------L0.?--------|                                                                     "
    - "L0.?[357,670] 1.04us 4mb                      |---------------L0.?---------------|                                 "
    - "L0.?[671,950] 1.04us 4mb                                                          |-------------L0.?-------------| "
    - "**** Simulation run 43, type=split(HighL0OverlapSingleFile)(split_times=[356]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.54[50,629] 1.04us     |-----------------------------------------L0.54------------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[50,356] 1.04us 5mb  |--------------------L0.?---------------------|                                           "
    - "L0.?[357,629] 1.04us 5mb                                                |------------------L0.?------------------| "
    - "**** Simulation run 44, type=split(HighL0OverlapSingleFile)(split_times=[356, 670]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.55[76,932] 1.04us     |-----------------------------------------L0.55------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[76,356] 1.04us 3mb  |-----------L0.?------------|                                                             "
    - "L0.?[357,670] 1.04us 4mb                              |-------------L0.?-------------|                             "
    - "L0.?[671,932] 1.04us 3mb                                                               |----------L0.?-----------| "
    - "**** Simulation run 45, type=split(HighL0OverlapSingleFile)(split_times=[356, 670]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.56[42,986] 1.05us     |-----------------------------------------L0.56------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[42,356] 1.05us 3mb  |-----------L0.?------------|                                                             "
    - "L0.?[357,670] 1.05us 3mb                               |-----------L0.?------------|                               "
    - "L0.?[671,986] 1.05us 3mb                                                            |------------L0.?------------| "
    - "**** Simulation run 46, type=split(HighL0OverlapSingleFile)(split_times=[356, 670]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.57[173,950] 1.05us    |-----------------------------------------L0.57------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[173,356] 1.05us 2mb |-------L0.?--------|                                                                     "
    - "L0.?[357,670] 1.05us 4mb                      |---------------L0.?---------------|                                 "
    - "L0.?[671,950] 1.05us 4mb                                                          |-------------L0.?-------------| "
    - "**** Simulation run 47, type=split(HighL0OverlapSingleFile)(split_times=[356]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.58[50,629] 1.05us     |-----------------------------------------L0.58------------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[50,356] 1.05us 5mb  |--------------------L0.?---------------------|                                           "
    - "L0.?[357,629] 1.05us 5mb                                                |------------------L0.?------------------| "
    - "**** Simulation run 48, type=split(HighL0OverlapSingleFile)(split_times=[356, 670]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.59[76,932] 1.05us     |-----------------------------------------L0.59------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[76,356] 1.05us 3mb  |-----------L0.?------------|                                                             "
    - "L0.?[357,670] 1.05us 4mb                              |-------------L0.?-------------|                             "
    - "L0.?[671,932] 1.05us 3mb                                                               |----------L0.?-----------| "
    - "**** Simulation run 49, type=split(HighL0OverlapSingleFile)(split_times=[356, 670]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.60[42,986] 1.05us     |-----------------------------------------L0.60------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[42,356] 1.05us 3mb  |-----------L0.?------------|                                                             "
    - "L0.?[357,670] 1.05us 3mb                               |-----------L0.?------------|                               "
    - "L0.?[671,986] 1.05us 3mb                                                            |------------L0.?------------| "
    - "Committing partition 1:"
    - "  Soft Deleting 50 files: L0.11, L0.12, L0.13, L0.14, L0.15, L0.16, L0.17, L0.18, L0.19, L0.20, L0.21, L0.22, L0.23, L0.24, L0.25, L0.26, L0.27, L0.28, L0.29, L0.30, L0.31, L0.32, L0.33, L0.34, L0.35, L0.36, L0.37, L0.38, L0.39, L0.40, L0.41, L0.42, L0.43, L0.44, L0.45, L0.46, L0.47, L0.48, L0.49, L0.50, L0.51, L0.52, L0.53, L0.54, L0.55, L0.56, L0.57, L0.58, L0.59, L0.60"
    - "  Creating 138 files"
    - "**** Simulation run 50, type=split(CompactAndSplitOutput(FoundSubsetLessThanMaxCompactSize))(split_times=[357, 672]). 83 Input Files, 300mb total:"
    - "L0                                                                                                                 "
    - "L0.61[76,356] 1us 3mb       |---------L0.61----------|                                                             "
    - "L0.62[357,670] 1us 4mb                                 |-----------L0.62-----------|                               "
    - "L0.63[671,932] 1us 3mb                                                              |--------L0.63---------|       "
    - "L0.64[42,356] 1us 3mb    |-----------L0.64-----------|                                                             "
    - "L0.65[357,670] 1us 3mb                                 |-----------L0.65-----------|                               "
    - "L0.66[671,986] 1us 3mb                                                              |-----------L0.66------------| "
    - "L0.67[173,356] 1us 2mb               |-----L0.67-----|                                                             "
    - "L0.68[357,670] 1us 4mb                                 |-----------L0.68-----------|                               "
    - "L0.69[671,950] 1us 4mb                                                              |---------L0.69----------|     "
    - "L0.70[50,356] 1us 5mb    |-----------L0.70-----------|                                                             "
    - "L0.71[357,629] 1us 5mb                                 |---------L0.71---------|                                   "
    - "L0.72[76,356] 1us 3mb       |---------L0.72----------|                                                             "
    - "L0.73[357,670] 1us 4mb                                 |-----------L0.73-----------|                               "
    - "L0.74[671,932] 1us 3mb                                                              |--------L0.74---------|       "
    - "L0.75[42,356] 1us 3mb    |-----------L0.75-----------|                                                             "
    - "L0.76[357,670] 1us 3mb                                 |-----------L0.76-----------|                               "
    - "L0.77[671,986] 1us 3mb                                                              |-----------L0.77------------| "
    - "L0.78[173,356] 1.01us 2mb            |-----L0.78-----|                                                             "
    - "L0.79[357,670] 1.01us 4mb                              |-----------L0.79-----------|                               "
    - "L0.80[671,950] 1.01us 4mb                                                           |---------L0.80----------|     "
    - "L0.81[50,356] 1.01us 5mb |-----------L0.81-----------|                                                             "
    - "L0.82[357,629] 1.01us 5mb                              |---------L0.82---------|                                   "
    - "L0.83[76,356] 1.01us 3mb    |---------L0.83----------|                                                             "
    - "L0.84[357,670] 1.01us 4mb                              |-----------L0.84-----------|                               "
    - "L0.85[671,932] 1.01us 3mb                                                           |--------L0.85---------|       "
    - "L0.86[42,356] 1.01us 3mb |-----------L0.86-----------|                                                             "
    - "L0.87[357,670] 1.01us 3mb                              |-----------L0.87-----------|                               "
    - "L0.88[671,986] 1.01us 3mb                                                           |-----------L0.88------------| "
    - "L0.89[173,356] 1.01us 2mb            |-----L0.89-----|                                                             "
    - "L0.90[357,670] 1.01us 4mb                              |-----------L0.90-----------|                               "
    - "L0.91[671,950] 1.01us 4mb                                                           |---------L0.91----------|     "
    - "L0.92[50,356] 1.01us 5mb |-----------L0.92-----------|                                                             "
    - "L0.93[357,629] 1.01us 5mb                              |---------L0.93---------|                                   "
    - "L0.94[76,356] 1.01us 3mb    |---------L0.94----------|                                                             "
    - "L0.95[357,670] 1.01us 4mb                              |-----------L0.95-----------|                               "
    - "L0.96[671,932] 1.01us 3mb                                                           |--------L0.96---------|       "
    - "L0.97[42,356] 1.01us 3mb |-----------L0.97-----------|                                                             "
    - "L0.98[357,670] 1.01us 3mb                              |-----------L0.98-----------|                               "
    - "L0.99[671,986] 1.01us 3mb                                                           |-----------L0.99------------| "
    - "L0.100[173,356] 1.01us 2mb            |----L0.100-----|                                                             "
    - "L0.101[357,670] 1.01us 4mb                              |----------L0.101-----------|                               "
    - "L0.102[671,950] 1.01us 4mb                                                           |---------L0.102---------|     "
    - "L0.103[50,356] 1.01us 5mb|----------L0.103-----------|                                                             "
    - "L0.104[357,629] 1.01us 5mb                              |--------L0.104---------|                                   "
    - "L0.105[76,356] 1.02us 3mb   |---------L0.105---------|                                                             "
    - "L0.106[357,670] 1.02us 4mb                              |----------L0.106-----------|                               "
    - "L0.107[671,932] 1.02us 3mb                                                           |--------L0.107--------|       "
    - "L0.108[42,356] 1.02us 3mb|----------L0.108-----------|                                                             "
    - "L0.109[357,670] 1.02us 3mb                              |----------L0.109-----------|                               "
    - "L0.110[671,986] 1.02us 3mb                                                           |-----------L0.110-----------| "
    - "L0.111[173,356] 1.02us 2mb            |----L0.111-----|                                                             "
    - "L0.112[357,670] 1.02us 4mb                              |----------L0.112-----------|                               "
    - "L0.113[671,950] 1.02us 4mb                                                           |---------L0.113---------|     "
    - "L0.114[50,356] 1.02us 5mb|----------L0.114-----------|                                                             "
    - "L0.115[357,629] 1.02us 5mb                              |--------L0.115---------|                                   "
    - "L0.116[76,356] 1.02us 3mb   |---------L0.116---------|                                                             "
    - "L0.117[357,670] 1.02us 4mb                              |----------L0.117-----------|                               "
    - "L0.118[671,932] 1.02us 3mb                                                           |--------L0.118--------|       "
    - "L0.119[42,356] 1.02us 3mb|----------L0.119-----------|                                                             "
    - "L0.120[357,670] 1.02us 3mb                              |----------L0.120-----------|                               "
    - "L0.121[671,986] 1.02us 3mb                                                           |-----------L0.121-----------| "
    - "L0.122[173,356] 1.02us 2mb            |----L0.122-----|                                                             "
    - "L0.123[357,670] 1.02us 4mb                              |----------L0.123-----------|                               "
    - "L0.124[671,950] 1.02us 4mb                                                           |---------L0.124---------|     "
    - "L0.125[50,356] 1.02us 5mb|----------L0.125-----------|                                                             "
    - "L0.126[357,629] 1.02us 5mb                              |--------L0.126---------|                                   "
    - "L0.127[76,356] 1.02us 3mb   |---------L0.127---------|                                                             "
    - "L0.128[357,670] 1.02us 4mb                              |----------L0.128-----------|                               "
    - "L0.129[671,932] 1.02us 3mb                                                           |--------L0.129--------|       "
    - "L0.130[42,356] 1.02us 3mb|----------L0.130-----------|                                                             "
    - "L0.131[357,670] 1.02us 3mb                              |----------L0.131-----------|                               "
    - "L0.132[671,986] 1.02us 3mb                                                           |-----------L0.132-----------| "
    - "L0.133[173,356] 1.03us 2mb            |----L0.133-----|                                                             "
    - "L0.134[357,670] 1.03us 4mb                              |----------L0.134-----------|                               "
    - "L0.135[671,950] 1.03us 4mb                                                           |---------L0.135---------|     "
    - "L0.136[50,356] 1.03us 5mb|----------L0.136-----------|                                                             "
    - "L0.137[357,629] 1.03us 5mb                              |--------L0.137---------|                                   "
    - "L0.138[76,356] 1.03us 3mb   |---------L0.138---------|                                                             "
    - "L0.139[357,670] 1.03us 4mb                              |----------L0.139-----------|                               "
    - "L0.140[671,932] 1.03us 3mb                                                           |--------L0.140--------|       "
    - "L0.141[42,356] 1.03us 3mb|----------L0.141-----------|                                                             "
    - "L0.142[357,670] 1.03us 3mb                              |----------L0.142-----------|                               "
    - "L0.143[671,986] 1.03us 3mb                                                           |-----------L0.143-----------| "
    - "**** 3 Output Files (parquet_file_id not yet assigned), 300mb total:"
    - "L1                                                                                                                 "
    - "L1.?[42,357] 1.03us 100mb|------------L1.?------------|                                                            "
    - "L1.?[358,672] 1.03us 100mb                              |-----------L1.?------------|                               "
    - "L1.?[673,986] 1.03us 100mb                                                            |-----------L1.?------------| "
    - "Committing partition 1:"
    - "  Soft Deleting 83 files: L0.61, L0.62, L0.63, L0.64, L0.65, L0.66, L0.67, L0.68, L0.69, L0.70, L0.71, L0.72, L0.73, L0.74, L0.75, L0.76, L0.77, L0.78, L0.79, L0.80, L0.81, L0.82, L0.83, L0.84, L0.85, L0.86, L0.87, L0.88, L0.89, L0.90, L0.91, L0.92, L0.93, L0.94, L0.95, L0.96, L0.97, L0.98, L0.99, L0.100, L0.101, L0.102, L0.103, L0.104, L0.105, L0.106, L0.107, L0.108, L0.109, L0.110, L0.111, L0.112, L0.113, L0.114, L0.115, L0.116, L0.117, L0.118, L0.119, L0.120, L0.121, L0.122, L0.123, L0.124, L0.125, L0.126, L0.127, L0.128, L0.129, L0.130, L0.131, L0.132, L0.133, L0.134, L0.135, L0.136, L0.137, L0.138, L0.139, L0.140, L0.141, L0.142, L0.143"
    - "  Creating 3 files"
    - "**** Simulation run 51, type=split(ReduceOverlap)(split_times=[672]). 1 Input Files, 4mb total:"
    - "L0, all files 4mb                                                                                                  "
    - "L0.146[671,950] 1.03us   |-----------------------------------------L0.146-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 4mb total:"
    - "L0                                                                                                                 "
    - "L0.?[671,672] 1.03us 13kb|L0.?|                                                                                    "
    - "L0.?[673,950] 1.03us 4mb |-----------------------------------------L0.?------------------------------------------| "
    - "**** Simulation run 52, type=split(ReduceOverlap)(split_times=[357]). 1 Input Files, 4mb total:"
    - "L0, all files 4mb                                                                                                  "
    - "L0.145[357,670] 1.03us   |-----------------------------------------L0.145-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 4mb total:"
    - "L0                                                                                                                 "
    - "L0.?[357,357] 1.03us 0b  |L0.?|                                                                                    "
    - "L0.?[358,670] 1.03us 4mb |-----------------------------------------L0.?------------------------------------------| "
    - "**** Simulation run 53, type=split(ReduceOverlap)(split_times=[357]). 1 Input Files, 5mb total:"
    - "L0, all files 5mb                                                                                                  "
    - "L0.148[357,629] 1.03us   |-----------------------------------------L0.148-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 5mb total:"
    - "L0                                                                                                                 "
    - "L0.?[357,357] 1.03us 0b  |L0.?|                                                                                    "
    - "L0.?[358,629] 1.03us 5mb |-----------------------------------------L0.?------------------------------------------| "
    - "**** Simulation run 54, type=split(ReduceOverlap)(split_times=[672]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.151[671,932] 1.03us   |-----------------------------------------L0.151-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[671,672] 1.03us 12kb|L0.?|                                                                                    "
    - "L0.?[673,932] 1.03us 3mb |-----------------------------------------L0.?------------------------------------------| "
    - "**** Simulation run 55, type=split(ReduceOverlap)(split_times=[357]). 1 Input Files, 4mb total:"
    - "L0, all files 4mb                                                                                                  "
    - "L0.150[357,670] 1.03us   |-----------------------------------------L0.150-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 4mb total:"
    - "L0                                                                                                                 "
    - "L0.?[357,357] 1.03us 0b  |L0.?|                                                                                    "
    - "L0.?[358,670] 1.03us 4mb |-----------------------------------------L0.?------------------------------------------| "
    - "**** Simulation run 56, type=split(ReduceOverlap)(split_times=[672]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.154[671,986] 1.03us   |-----------------------------------------L0.154-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[671,672] 1.03us 11kb|L0.?|                                                                                    "
    - "L0.?[673,986] 1.03us 3mb |-----------------------------------------L0.?------------------------------------------| "
    - "**** Simulation run 57, type=split(ReduceOverlap)(split_times=[357]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.153[357,670] 1.03us   |-----------------------------------------L0.153-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[357,357] 1.03us 0b  |L0.?|                                                                                    "
    - "L0.?[358,670] 1.03us 3mb |-----------------------------------------L0.?------------------------------------------| "
    - "**** Simulation run 58, type=split(ReduceOverlap)(split_times=[672]). 1 Input Files, 4mb total:"
    - "L0, all files 4mb                                                                                                  "
    - "L0.157[671,950] 1.03us   |-----------------------------------------L0.157-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 4mb total:"
    - "L0                                                                                                                 "
    - "L0.?[671,672] 1.03us 13kb|L0.?|                                                                                    "
    - "L0.?[673,950] 1.03us 4mb |-----------------------------------------L0.?------------------------------------------| "
    - "**** Simulation run 59, type=split(ReduceOverlap)(split_times=[357]). 1 Input Files, 4mb total:"
    - "L0, all files 4mb                                                                                                  "
    - "L0.156[357,670] 1.03us   |-----------------------------------------L0.156-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 4mb total:"
    - "L0                                                                                                                 "
    - "L0.?[357,357] 1.03us 0b  |L0.?|                                                                                    "
    - "L0.?[358,670] 1.03us 4mb |-----------------------------------------L0.?------------------------------------------| "
    - "**** Simulation run 60, type=split(ReduceOverlap)(split_times=[357]). 1 Input Files, 5mb total:"
    - "L0, all files 5mb                                                                                                  "
    - "L0.159[357,629] 1.03us   |-----------------------------------------L0.159-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 5mb total:"
    - "L0                                                                                                                 "
    - "L0.?[357,357] 1.03us 0b  |L0.?|                                                                                    "
    - "L0.?[358,629] 1.03us 5mb |-----------------------------------------L0.?------------------------------------------| "
    - "**** Simulation run 61, type=split(ReduceOverlap)(split_times=[672]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.162[671,932] 1.04us   |-----------------------------------------L0.162-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[671,672] 1.04us 12kb|L0.?|                                                                                    "
    - "L0.?[673,932] 1.04us 3mb |-----------------------------------------L0.?------------------------------------------| "
    - "**** Simulation run 62, type=split(ReduceOverlap)(split_times=[357]). 1 Input Files, 4mb total:"
    - "L0, all files 4mb                                                                                                  "
    - "L0.161[357,670] 1.04us   |-----------------------------------------L0.161-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 4mb total:"
    - "L0                                                                                                                 "
    - "L0.?[357,357] 1.04us 0b  |L0.?|                                                                                    "
    - "L0.?[358,670] 1.04us 4mb |-----------------------------------------L0.?------------------------------------------| "
    - "**** Simulation run 63, type=split(ReduceOverlap)(split_times=[672]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.165[671,986] 1.04us   |-----------------------------------------L0.165-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[671,672] 1.04us 11kb|L0.?|                                                                                    "
    - "L0.?[673,986] 1.04us 3mb |-----------------------------------------L0.?------------------------------------------| "
    - "**** Simulation run 64, type=split(ReduceOverlap)(split_times=[357]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.164[357,670] 1.04us   |-----------------------------------------L0.164-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[357,357] 1.04us 0b  |L0.?|                                                                                    "
    - "L0.?[358,670] 1.04us 3mb |-----------------------------------------L0.?------------------------------------------| "
    - "**** Simulation run 65, type=split(ReduceOverlap)(split_times=[672]). 1 Input Files, 4mb total:"
    - "L0, all files 4mb                                                                                                  "
    - "L0.168[671,950] 1.04us   |-----------------------------------------L0.168-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 4mb total:"
    - "L0                                                                                                                 "
    - "L0.?[671,672] 1.04us 13kb|L0.?|                                                                                    "
    - "L0.?[673,950] 1.04us 4mb |-----------------------------------------L0.?------------------------------------------| "
    - "**** Simulation run 66, type=split(ReduceOverlap)(split_times=[357]). 1 Input Files, 4mb total:"
    - "L0, all files 4mb                                                                                                  "
    - "L0.167[357,670] 1.04us   |-----------------------------------------L0.167-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 4mb total:"
    - "L0                                                                                                                 "
    - "L0.?[357,357] 1.04us 0b  |L0.?|                                                                                    "
    - "L0.?[358,670] 1.04us 4mb |-----------------------------------------L0.?------------------------------------------| "
    - "**** Simulation run 67, type=split(ReduceOverlap)(split_times=[357]). 1 Input Files, 5mb total:"
    - "L0, all files 5mb                                                                                                  "
    - "L0.170[357,629] 1.04us   |-----------------------------------------L0.170-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 5mb total:"
    - "L0                                                                                                                 "
    - "L0.?[357,357] 1.04us 0b  |L0.?|                                                                                    "
    - "L0.?[358,629] 1.04us 5mb |-----------------------------------------L0.?------------------------------------------| "
    - "**** Simulation run 68, type=split(ReduceOverlap)(split_times=[672]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.173[671,932] 1.04us   |-----------------------------------------L0.173-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[671,672] 1.04us 12kb|L0.?|                                                                                    "
    - "L0.?[673,932] 1.04us 3mb |-----------------------------------------L0.?------------------------------------------| "
    - "**** Simulation run 69, type=split(ReduceOverlap)(split_times=[357]). 1 Input Files, 4mb total:"
    - "L0, all files 4mb                                                                                                  "
    - "L0.172[357,670] 1.04us   |-----------------------------------------L0.172-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 4mb total:"
    - "L0                                                                                                                 "
    - "L0.?[357,357] 1.04us 0b  |L0.?|                                                                                    "
    - "L0.?[358,670] 1.04us 4mb |-----------------------------------------L0.?------------------------------------------| "
    - "**** Simulation run 70, type=split(ReduceOverlap)(split_times=[672]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.176[671,986] 1.04us   |-----------------------------------------L0.176-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[671,672] 1.04us 11kb|L0.?|                                                                                    "
    - "L0.?[673,986] 1.04us 3mb |-----------------------------------------L0.?------------------------------------------| "
    - "**** Simulation run 71, type=split(ReduceOverlap)(split_times=[357]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.175[357,670] 1.04us   |-----------------------------------------L0.175-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[357,357] 1.04us 0b  |L0.?|                                                                                    "
    - "L0.?[358,670] 1.04us 3mb |-----------------------------------------L0.?------------------------------------------| "
    - "**** Simulation run 72, type=split(ReduceOverlap)(split_times=[672]). 1 Input Files, 4mb total:"
    - "L0, all files 4mb                                                                                                  "
    - "L0.179[671,950] 1.04us   |-----------------------------------------L0.179-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 4mb total:"
    - "L0                                                                                                                 "
    - "L0.?[671,672] 1.04us 13kb|L0.?|                                                                                    "
    - "L0.?[673,950] 1.04us 4mb |-----------------------------------------L0.?------------------------------------------| "
    - "**** Simulation run 73, type=split(ReduceOverlap)(split_times=[357]). 1 Input Files, 4mb total:"
    - "L0, all files 4mb                                                                                                  "
    - "L0.178[357,670] 1.04us   |-----------------------------------------L0.178-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 4mb total:"
    - "L0                                                                                                                 "
    - "L0.?[357,357] 1.04us 0b  |L0.?|                                                                                    "
    - "L0.?[358,670] 1.04us 4mb |-----------------------------------------L0.?------------------------------------------| "
    - "**** Simulation run 74, type=split(ReduceOverlap)(split_times=[357]). 1 Input Files, 5mb total:"
    - "L0, all files 5mb                                                                                                  "
    - "L0.181[357,629] 1.04us   |-----------------------------------------L0.181-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 5mb total:"
    - "L0                                                                                                                 "
    - "L0.?[357,357] 1.04us 0b  |L0.?|                                                                                    "
    - "L0.?[358,629] 1.04us 5mb |-----------------------------------------L0.?------------------------------------------| "
    - "**** Simulation run 75, type=split(ReduceOverlap)(split_times=[672]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.184[671,932] 1.04us   |-----------------------------------------L0.184-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[671,672] 1.04us 12kb|L0.?|                                                                                    "
    - "L0.?[673,932] 1.04us 3mb |-----------------------------------------L0.?------------------------------------------| "
    - "**** Simulation run 76, type=split(ReduceOverlap)(split_times=[357]). 1 Input Files, 4mb total:"
    - "L0, all files 4mb                                                                                                  "
    - "L0.183[357,670] 1.04us   |-----------------------------------------L0.183-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 4mb total:"
    - "L0                                                                                                                 "
    - "L0.?[357,357] 1.04us 0b  |L0.?|                                                                                    "
    - "L0.?[358,670] 1.04us 4mb |-----------------------------------------L0.?------------------------------------------| "
    - "**** Simulation run 77, type=split(ReduceOverlap)(split_times=[672]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.187[671,986] 1.05us   |-----------------------------------------L0.187-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[671,672] 1.05us 11kb|L0.?|                                                                                    "
    - "L0.?[673,986] 1.05us 3mb |-----------------------------------------L0.?------------------------------------------| "
    - "**** Simulation run 78, type=split(ReduceOverlap)(split_times=[357]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.186[357,670] 1.05us   |-----------------------------------------L0.186-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[357,357] 1.05us 0b  |L0.?|                                                                                    "
    - "L0.?[358,670] 1.05us 3mb |-----------------------------------------L0.?------------------------------------------| "
    - "**** Simulation run 79, type=split(ReduceOverlap)(split_times=[672]). 1 Input Files, 4mb total:"
    - "L0, all files 4mb                                                                                                  "
    - "L0.190[671,950] 1.05us   |-----------------------------------------L0.190-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 4mb total:"
    - "L0                                                                                                                 "
    - "L0.?[671,672] 1.05us 13kb|L0.?|                                                                                    "
    - "L0.?[673,950] 1.05us 4mb |-----------------------------------------L0.?------------------------------------------| "
    - "**** Simulation run 80, type=split(ReduceOverlap)(split_times=[357]). 1 Input Files, 4mb total:"
    - "L0, all files 4mb                                                                                                  "
    - "L0.189[357,670] 1.05us   |-----------------------------------------L0.189-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 4mb total:"
    - "L0                                                                                                                 "
    - "L0.?[357,357] 1.05us 0b  |L0.?|                                                                                    "
    - "L0.?[358,670] 1.05us 4mb |-----------------------------------------L0.?------------------------------------------| "
    - "**** Simulation run 81, type=split(ReduceOverlap)(split_times=[357]). 1 Input Files, 5mb total:"
    - "L0, all files 5mb                                                                                                  "
    - "L0.192[357,629] 1.05us   |-----------------------------------------L0.192-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 5mb total:"
    - "L0                                                                                                                 "
    - "L0.?[357,357] 1.05us 0b  |L0.?|                                                                                    "
    - "L0.?[358,629] 1.05us 5mb |-----------------------------------------L0.?------------------------------------------| "
    - "**** Simulation run 82, type=split(ReduceOverlap)(split_times=[672]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.195[671,932] 1.05us   |-----------------------------------------L0.195-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[671,672] 1.05us 12kb|L0.?|                                                                                    "
    - "L0.?[673,932] 1.05us 3mb |-----------------------------------------L0.?------------------------------------------| "
    - "**** Simulation run 83, type=split(ReduceOverlap)(split_times=[357]). 1 Input Files, 4mb total:"
    - "L0, all files 4mb                                                                                                  "
    - "L0.194[357,670] 1.05us   |-----------------------------------------L0.194-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 4mb total:"
    - "L0                                                                                                                 "
    - "L0.?[357,357] 1.05us 0b  |L0.?|                                                                                    "
    - "L0.?[358,670] 1.05us 4mb |-----------------------------------------L0.?------------------------------------------| "
    - "**** Simulation run 84, type=split(ReduceOverlap)(split_times=[672]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.198[671,986] 1.05us   |-----------------------------------------L0.198-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[671,672] 1.05us 11kb|L0.?|                                                                                    "
    - "L0.?[673,986] 1.05us 3mb |-----------------------------------------L0.?------------------------------------------| "
    - "**** Simulation run 85, type=split(ReduceOverlap)(split_times=[357]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.197[357,670] 1.05us   |-----------------------------------------L0.197-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[357,357] 1.05us 0b  |L0.?|                                                                                    "
    - "L0.?[358,670] 1.05us 3mb |-----------------------------------------L0.?------------------------------------------| "
    - "Committing partition 1:"
    - "  Soft Deleting 35 files: L0.145, L0.146, L0.148, L0.150, L0.151, L0.153, L0.154, L0.156, L0.157, L0.159, L0.161, L0.162, L0.164, L0.165, L0.167, L0.168, L0.170, L0.172, L0.173, L0.175, L0.176, L0.178, L0.179, L0.181, L0.183, L0.184, L0.186, L0.187, L0.189, L0.190, L0.192, L0.194, L0.195, L0.197, L0.198"
    - "  Creating 70 files"
    - "**** Simulation run 86, type=split(CompactAndSplitOutput(FoundSubsetLessThanMaxCompactSize))(split_times=[348, 654]). 6 Input Files, 206mb total:"
    - "L0                                                                                                                 "
    - "L0.144[173,356] 1.03us 2mb                  |---------L0.144---------|                                              "
    - "L0.204[357,357] 1.03us 0b                                             |L0.204|                                     "
    - "L0.205[358,670] 1.03us 4mb                                             |------------------L0.205------------------| "
    - "L0.202[671,672] 1.03us 13kb                                                                                         |L0.202|"
    - "L1                                                                                                                 "
    - "L1.199[42,357] 1.03us 100mb|------------------L1.199-------------------|                                             "
    - "L1.200[358,672] 1.03us 100mb                                             |------------------L1.200------------------| "
    - "**** 3 Output Files (parquet_file_id not yet assigned), 206mb total:"
    - "L1                                                                                                                 "
    - "L1.?[42,348] 1.03us 100mb|------------------L1.?-------------------|                                               "
    - "L1.?[349,654] 1.03us 100mb                                           |------------------L1.?-------------------|    "
    - "L1.?[655,672] 1.03us 6mb                                                                                        |L1.?|"
    - "Committing partition 1:"
    - "  Soft Deleting 6 files: L0.144, L1.199, L1.200, L0.202, L0.204, L0.205"
    - "  Creating 3 files"
    - "**** Simulation run 87, type=split(ReduceOverlap)(split_times=[348]). 1 Input Files, 5mb total:"
    - "L0, all files 5mb                                                                                                  "
    - "L0.147[50,356] 1.03us    |-----------------------------------------L0.147-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 5mb total:"
    - "L0                                                                                                                 "
    - "L0.?[50,348] 1.03us 5mb  |----------------------------------------L0.?-----------------------------------------|   "
    - "L0.?[349,356] 1.03us 141kb                                                                                       |L0.?|"
    - "**** Simulation run 88, type=split(ReduceOverlap)(split_times=[654]). 1 Input Files, 4mb total:"
    - "L0, all files 4mb                                                                                                  "
    - "L0.211[358,670] 1.03us   |-----------------------------------------L0.211-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 4mb total:"
    - "L0                                                                                                                 "
    - "L0.?[358,654] 1.03us 3mb |---------------------------------------L0.?----------------------------------------|     "
    - "L0.?[655,670] 1.03us 192kb                                                                                     |L0.?|"
    - "**** Simulation run 89, type=split(ReduceOverlap)(split_times=[348]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.149[76,356] 1.03us    |-----------------------------------------L0.149-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[76,348] 1.03us 3mb  |----------------------------------------L0.?-----------------------------------------|   "
    - "L0.?[349,356] 1.03us 96kb                                                                                       |L0.?|"
    - "**** Simulation run 90, type=split(ReduceOverlap)(split_times=[654]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.215[358,670] 1.03us   |-----------------------------------------L0.215-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[358,654] 1.03us 3mb |---------------------------------------L0.?----------------------------------------|     "
    - "L0.?[655,670] 1.03us 174kb                                                                                     |L0.?|"
    - "**** Simulation run 91, type=split(ReduceOverlap)(split_times=[348]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.152[42,356] 1.03us    |-----------------------------------------L0.152-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[42,348] 1.03us 3mb  |----------------------------------------L0.?-----------------------------------------|   "
    - "L0.?[349,356] 1.03us 87kb                                                                                       |L0.?|"
    - "**** Simulation run 92, type=split(ReduceOverlap)(split_times=[654]). 1 Input Files, 4mb total:"
    - "L0, all files 4mb                                                                                                  "
    - "L0.219[358,670] 1.03us   |-----------------------------------------L0.219-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 4mb total:"
    - "L0                                                                                                                 "
    - "L0.?[358,654] 1.03us 4mb |---------------------------------------L0.?----------------------------------------|     "
    - "L0.?[655,670] 1.03us 212kb                                                                                     |L0.?|"
    - "**** Simulation run 93, type=split(ReduceOverlap)(split_times=[348]). 1 Input Files, 2mb total:"
    - "L0, all files 2mb                                                                                                  "
    - "L0.155[173,356] 1.03us   |-----------------------------------------L0.155-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 2mb total:"
    - "L0                                                                                                                 "
    - "L0.?[173,348] 1.03us 2mb |----------------------------------------L0.?----------------------------------------|    "
    - "L0.?[349,356] 1.03us 105kb                                                                                      |L0.?|"
    - "**** Simulation run 94, type=split(ReduceOverlap)(split_times=[348]). 1 Input Files, 5mb total:"
    - "L0, all files 5mb                                                                                                  "
    - "L0.158[50,356] 1.03us    |-----------------------------------------L0.158-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 5mb total:"
    - "L0                                                                                                                 "
    - "L0.?[50,348] 1.03us 5mb  |----------------------------------------L0.?-----------------------------------------|   "
    - "L0.?[349,356] 1.03us 141kb                                                                                       |L0.?|"
    - "**** Simulation run 95, type=split(ReduceOverlap)(split_times=[654]). 1 Input Files, 4mb total:"
    - "L0, all files 4mb                                                                                                  "
    - "L0.225[358,670] 1.04us   |-----------------------------------------L0.225-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 4mb total:"
    - "L0                                                                                                                 "
    - "L0.?[358,654] 1.04us 3mb |---------------------------------------L0.?----------------------------------------|     "
    - "L0.?[655,670] 1.04us 192kb                                                                                     |L0.?|"
    - "**** Simulation run 96, type=split(ReduceOverlap)(split_times=[348]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.160[76,356] 1.04us    |-----------------------------------------L0.160-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[76,348] 1.04us 3mb  |----------------------------------------L0.?-----------------------------------------|   "
    - "L0.?[349,356] 1.04us 96kb                                                                                       |L0.?|"
    - "**** Simulation run 97, type=split(ReduceOverlap)(split_times=[654]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.229[358,670] 1.04us   |-----------------------------------------L0.229-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[358,654] 1.04us 3mb |---------------------------------------L0.?----------------------------------------|     "
    - "L0.?[655,670] 1.04us 174kb                                                                                     |L0.?|"
    - "**** Simulation run 98, type=split(ReduceOverlap)(split_times=[348]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.163[42,356] 1.04us    |-----------------------------------------L0.163-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[42,348] 1.04us 3mb  |----------------------------------------L0.?-----------------------------------------|   "
    - "L0.?[349,356] 1.04us 87kb                                                                                       |L0.?|"
    - "**** Simulation run 99, type=split(ReduceOverlap)(split_times=[654]). 1 Input Files, 4mb total:"
    - "L0, all files 4mb                                                                                                  "
    - "L0.233[358,670] 1.04us   |-----------------------------------------L0.233-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 4mb total:"
    - "L0                                                                                                                 "
    - "L0.?[358,654] 1.04us 4mb |---------------------------------------L0.?----------------------------------------|     "
    - "L0.?[655,670] 1.04us 212kb                                                                                     |L0.?|"
    - "**** Simulation run 100, type=split(ReduceOverlap)(split_times=[348]). 1 Input Files, 2mb total:"
    - "L0, all files 2mb                                                                                                  "
    - "L0.166[173,356] 1.04us   |-----------------------------------------L0.166-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 2mb total:"
    - "L0                                                                                                                 "
    - "L0.?[173,348] 1.04us 2mb |----------------------------------------L0.?----------------------------------------|    "
    - "L0.?[349,356] 1.04us 105kb                                                                                      |L0.?|"
    - "**** Simulation run 101, type=split(ReduceOverlap)(split_times=[348]). 1 Input Files, 5mb total:"
    - "L0, all files 5mb                                                                                                  "
    - "L0.169[50,356] 1.04us    |-----------------------------------------L0.169-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 5mb total:"
    - "L0                                                                                                                 "
    - "L0.?[50,348] 1.04us 5mb  |----------------------------------------L0.?-----------------------------------------|   "
    - "L0.?[349,356] 1.04us 141kb                                                                                       |L0.?|"
    - "**** Simulation run 102, type=split(ReduceOverlap)(split_times=[654]). 1 Input Files, 4mb total:"
    - "L0, all files 4mb                                                                                                  "
    - "L0.239[358,670] 1.04us   |-----------------------------------------L0.239-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 4mb total:"
    - "L0                                                                                                                 "
    - "L0.?[358,654] 1.04us 3mb |---------------------------------------L0.?----------------------------------------|     "
    - "L0.?[655,670] 1.04us 192kb                                                                                     |L0.?|"
    - "**** Simulation run 103, type=split(ReduceOverlap)(split_times=[348]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.171[76,356] 1.04us    |-----------------------------------------L0.171-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[76,348] 1.04us 3mb  |----------------------------------------L0.?-----------------------------------------|   "
    - "L0.?[349,356] 1.04us 96kb                                                                                       |L0.?|"
    - "**** Simulation run 104, type=split(ReduceOverlap)(split_times=[654]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.243[358,670] 1.04us   |-----------------------------------------L0.243-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[358,654] 1.04us 3mb |---------------------------------------L0.?----------------------------------------|     "
    - "L0.?[655,670] 1.04us 174kb                                                                                     |L0.?|"
    - "**** Simulation run 105, type=split(ReduceOverlap)(split_times=[348]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.174[42,356] 1.04us    |-----------------------------------------L0.174-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[42,348] 1.04us 3mb  |----------------------------------------L0.?-----------------------------------------|   "
    - "L0.?[349,356] 1.04us 87kb                                                                                       |L0.?|"
    - "**** Simulation run 106, type=split(ReduceOverlap)(split_times=[654]). 1 Input Files, 4mb total:"
    - "L0, all files 4mb                                                                                                  "
    - "L0.247[358,670] 1.04us   |-----------------------------------------L0.247-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 4mb total:"
    - "L0                                                                                                                 "
    - "L0.?[358,654] 1.04us 4mb |---------------------------------------L0.?----------------------------------------|     "
    - "L0.?[655,670] 1.04us 212kb                                                                                     |L0.?|"
    - "**** Simulation run 107, type=split(ReduceOverlap)(split_times=[348]). 1 Input Files, 2mb total:"
    - "L0, all files 2mb                                                                                                  "
    - "L0.177[173,356] 1.04us   |-----------------------------------------L0.177-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 2mb total:"
    - "L0                                                                                                                 "
    - "L0.?[173,348] 1.04us 2mb |----------------------------------------L0.?----------------------------------------|    "
    - "L0.?[349,356] 1.04us 105kb                                                                                      |L0.?|"
    - "**** Simulation run 108, type=split(ReduceOverlap)(split_times=[348]). 1 Input Files, 5mb total:"
    - "L0, all files 5mb                                                                                                  "
    - "L0.180[50,356] 1.04us    |-----------------------------------------L0.180-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 5mb total:"
    - "L0                                                                                                                 "
    - "L0.?[50,348] 1.04us 5mb  |----------------------------------------L0.?-----------------------------------------|   "
    - "L0.?[349,356] 1.04us 141kb                                                                                       |L0.?|"
    - "**** Simulation run 109, type=split(ReduceOverlap)(split_times=[654]). 1 Input Files, 4mb total:"
    - "L0, all files 4mb                                                                                                  "
    - "L0.253[358,670] 1.04us   |-----------------------------------------L0.253-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 4mb total:"
    - "L0                                                                                                                 "
    - "L0.?[358,654] 1.04us 3mb |---------------------------------------L0.?----------------------------------------|     "
    - "L0.?[655,670] 1.04us 192kb                                                                                     |L0.?|"
    - "**** Simulation run 110, type=split(ReduceOverlap)(split_times=[348]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.182[76,356] 1.04us    |-----------------------------------------L0.182-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[76,348] 1.04us 3mb  |----------------------------------------L0.?-----------------------------------------|   "
    - "L0.?[349,356] 1.04us 96kb                                                                                       |L0.?|"
    - "**** Simulation run 111, type=split(ReduceOverlap)(split_times=[654]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.257[358,670] 1.05us   |-----------------------------------------L0.257-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[358,654] 1.05us 3mb |---------------------------------------L0.?----------------------------------------|     "
    - "L0.?[655,670] 1.05us 174kb                                                                                     |L0.?|"
    - "**** Simulation run 112, type=split(ReduceOverlap)(split_times=[348]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.185[42,356] 1.05us    |-----------------------------------------L0.185-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[42,348] 1.05us 3mb  |----------------------------------------L0.?-----------------------------------------|   "
    - "L0.?[349,356] 1.05us 87kb                                                                                       |L0.?|"
    - "**** Simulation run 113, type=split(ReduceOverlap)(split_times=[654]). 1 Input Files, 4mb total:"
    - "L0, all files 4mb                                                                                                  "
    - "L0.261[358,670] 1.05us   |-----------------------------------------L0.261-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 4mb total:"
    - "L0                                                                                                                 "
    - "L0.?[358,654] 1.05us 4mb |---------------------------------------L0.?----------------------------------------|     "
    - "L0.?[655,670] 1.05us 212kb                                                                                     |L0.?|"
    - "**** Simulation run 114, type=split(ReduceOverlap)(split_times=[348]). 1 Input Files, 2mb total:"
    - "L0, all files 2mb                                                                                                  "
    - "L0.188[173,356] 1.05us   |-----------------------------------------L0.188-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 2mb total:"
    - "L0                                                                                                                 "
    - "L0.?[173,348] 1.05us 2mb |----------------------------------------L0.?----------------------------------------|    "
    - "L0.?[349,356] 1.05us 105kb                                                                                      |L0.?|"
    - "**** Simulation run 115, type=split(ReduceOverlap)(split_times=[348]). 1 Input Files, 5mb total:"
    - "L0, all files 5mb                                                                                                  "
    - "L0.191[50,356] 1.05us    |-----------------------------------------L0.191-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 5mb total:"
    - "L0                                                                                                                 "
    - "L0.?[50,348] 1.05us 5mb  |----------------------------------------L0.?-----------------------------------------|   "
    - "L0.?[349,356] 1.05us 141kb                                                                                       |L0.?|"
    - "**** Simulation run 116, type=split(ReduceOverlap)(split_times=[654]). 1 Input Files, 4mb total:"
    - "L0, all files 4mb                                                                                                  "
    - "L0.267[358,670] 1.05us   |-----------------------------------------L0.267-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 4mb total:"
    - "L0                                                                                                                 "
    - "L0.?[358,654] 1.05us 3mb |---------------------------------------L0.?----------------------------------------|     "
    - "L0.?[655,670] 1.05us 192kb                                                                                     |L0.?|"
    - "**** Simulation run 117, type=split(ReduceOverlap)(split_times=[348]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.193[76,356] 1.05us    |-----------------------------------------L0.193-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[76,348] 1.05us 3mb  |----------------------------------------L0.?-----------------------------------------|   "
    - "L0.?[349,356] 1.05us 96kb                                                                                       |L0.?|"
    - "**** Simulation run 118, type=split(ReduceOverlap)(split_times=[654]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.271[358,670] 1.05us   |-----------------------------------------L0.271-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[358,654] 1.05us 3mb |---------------------------------------L0.?----------------------------------------|     "
    - "L0.?[655,670] 1.05us 174kb                                                                                     |L0.?|"
    - "**** Simulation run 119, type=split(ReduceOverlap)(split_times=[348]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.196[42,356] 1.05us    |-----------------------------------------L0.196-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[42,348] 1.05us 3mb  |----------------------------------------L0.?-----------------------------------------|   "
    - "L0.?[349,356] 1.05us 87kb                                                                                       |L0.?|"
    - "Committing partition 1:"
    - "  Soft Deleting 33 files: L0.147, L0.149, L0.152, L0.155, L0.158, L0.160, L0.163, L0.166, L0.169, L0.171, L0.174, L0.177, L0.180, L0.182, L0.185, L0.188, L0.191, L0.193, L0.196, L0.211, L0.215, L0.219, L0.225, L0.229, L0.233, L0.239, L0.243, L0.247, L0.253, L0.257, L0.261, L0.267, L0.271"
    - "  Creating 66 files"
    - "**** Simulation run 120, type=split(CompactAndSplitOutput(FoundSubsetLessThanMaxCompactSize))(split_times=[923]). 2 Input Files, 104mb total:"
    - "L0                                                                                                                 "
    - "L0.203[673,950] 1.03us 4mb|-----------------------------------L0.203------------------------------------|           "
    - "L1                                                                                                                 "
    - "L1.201[673,986] 1.03us 100mb|-----------------------------------------L1.201-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 104mb total:"
    - "L1                                                                                                                 "
    - "L1.?[673,923] 1.03us 83mb|--------------------------------L1.?---------------------------------|                   "
    - "L1.?[924,986] 1.03us 21mb                                                                        |-----L1.?------| "
    - "Committing partition 1:"
    - "  Soft Deleting 2 files: L1.201, L0.203"
    - "  Creating 2 files"
    - "**** Simulation run 121, type=split(ReduceOverlap)(split_times=[923]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.209[673,932] 1.03us   |-----------------------------------------L0.209-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[673,923] 1.03us 3mb |----------------------------------------L0.?----------------------------------------|    "
    - "L0.?[924,932] 1.03us 109kb                                                                                       |L0.?|"
    - "**** Simulation run 122, type=split(ReduceOverlap)(split_times=[923]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.213[673,986] 1.03us   |-----------------------------------------L0.213-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[673,923] 1.03us 3mb |--------------------------------L0.?---------------------------------|                   "
    - "L0.?[924,986] 1.03us 690kb                                                                        |-----L0.?------| "
    - "**** Simulation run 123, type=split(ReduceOverlap)(split_times=[923]). 1 Input Files, 4mb total:"
    - "L0, all files 4mb                                                                                                  "
    - "L0.217[673,950] 1.03us   |-----------------------------------------L0.217-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 4mb total:"
    - "L0                                                                                                                 "
    - "L0.?[673,923] 1.03us 3mb |-------------------------------------L0.?--------------------------------------|         "
    - "L0.?[924,950] 1.03us 360kb                                                                                 |-L0.?-| "
    - "**** Simulation run 124, type=split(ReduceOverlap)(split_times=[923]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.223[673,932] 1.04us   |-----------------------------------------L0.223-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[673,923] 1.04us 3mb |----------------------------------------L0.?----------------------------------------|    "
    - "L0.?[924,932] 1.04us 109kb                                                                                       |L0.?|"
    - "**** Simulation run 125, type=split(ReduceOverlap)(split_times=[923]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.227[673,986] 1.04us   |-----------------------------------------L0.227-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[673,923] 1.04us 3mb |--------------------------------L0.?---------------------------------|                   "
    - "L0.?[924,986] 1.04us 690kb                                                                        |-----L0.?------| "
    - "**** Simulation run 126, type=split(ReduceOverlap)(split_times=[923]). 1 Input Files, 4mb total:"
    - "L0, all files 4mb                                                                                                  "
    - "L0.231[673,950] 1.04us   |-----------------------------------------L0.231-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 4mb total:"
    - "L0                                                                                                                 "
    - "L0.?[673,923] 1.04us 3mb |-------------------------------------L0.?--------------------------------------|         "
    - "L0.?[924,950] 1.04us 360kb                                                                                 |-L0.?-| "
    - "**** Simulation run 127, type=split(ReduceOverlap)(split_times=[923]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.237[673,932] 1.04us   |-----------------------------------------L0.237-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[673,923] 1.04us 3mb |----------------------------------------L0.?----------------------------------------|    "
    - "L0.?[924,932] 1.04us 109kb                                                                                       |L0.?|"
    - "**** Simulation run 128, type=split(ReduceOverlap)(split_times=[923]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.241[673,986] 1.04us   |-----------------------------------------L0.241-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[673,923] 1.04us 3mb |--------------------------------L0.?---------------------------------|                   "
    - "L0.?[924,986] 1.04us 690kb                                                                        |-----L0.?------| "
    - "**** Simulation run 129, type=split(ReduceOverlap)(split_times=[923]). 1 Input Files, 4mb total:"
    - "L0, all files 4mb                                                                                                  "
    - "L0.245[673,950] 1.04us   |-----------------------------------------L0.245-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 4mb total:"
    - "L0                                                                                                                 "
    - "L0.?[673,923] 1.04us 3mb |-------------------------------------L0.?--------------------------------------|         "
    - "L0.?[924,950] 1.04us 360kb                                                                                 |-L0.?-| "
    - "**** Simulation run 130, type=split(ReduceOverlap)(split_times=[923]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.251[673,932] 1.04us   |-----------------------------------------L0.251-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[673,923] 1.04us 3mb |----------------------------------------L0.?----------------------------------------|    "
    - "L0.?[924,932] 1.04us 109kb                                                                                       |L0.?|"
    - "**** Simulation run 131, type=split(ReduceOverlap)(split_times=[923]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.255[673,986] 1.05us   |-----------------------------------------L0.255-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[673,923] 1.05us 3mb |--------------------------------L0.?---------------------------------|                   "
    - "L0.?[924,986] 1.05us 690kb                                                                        |-----L0.?------| "
    - "**** Simulation run 132, type=split(ReduceOverlap)(split_times=[923]). 1 Input Files, 4mb total:"
    - "L0, all files 4mb                                                                                                  "
    - "L0.259[673,950] 1.05us   |-----------------------------------------L0.259-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 4mb total:"
    - "L0                                                                                                                 "
    - "L0.?[673,923] 1.05us 3mb |-------------------------------------L0.?--------------------------------------|         "
    - "L0.?[924,950] 1.05us 360kb                                                                                 |-L0.?-| "
    - "**** Simulation run 133, type=split(ReduceOverlap)(split_times=[923]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.265[673,932] 1.05us   |-----------------------------------------L0.265-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[673,923] 1.05us 3mb |----------------------------------------L0.?----------------------------------------|    "
    - "L0.?[924,932] 1.05us 109kb                                                                                       |L0.?|"
    - "**** Simulation run 134, type=split(ReduceOverlap)(split_times=[923]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.269[673,986] 1.05us   |-----------------------------------------L0.269-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[673,923] 1.05us 3mb |--------------------------------L0.?---------------------------------|                   "
    - "L0.?[924,986] 1.05us 690kb                                                                        |-----L0.?------| "
    - "Committing partition 1:"
    - "  Soft Deleting 14 files: L0.209, L0.213, L0.217, L0.223, L0.227, L0.231, L0.237, L0.241, L0.245, L0.251, L0.255, L0.259, L0.265, L0.269"
    - "  Creating 28 files"
    - "**** Simulation run 135, type=split(CompactAndSplitOutput(FoundSubsetLessThanMaxCompactSize))(split_times=[325, 608]). 13 Input Files, 223mb total:"
    - "L0                                                                                                                 "
    - "L0.275[50,348] 1.03us 5mb |-----------------L0.275-----------------|                                               "
    - "L0.276[349,356] 1.03us 141kb                                           |L0.276|                                       "
    - "L0.206[357,357] 1.03us 0b                                             |L0.206|                                     "
    - "L0.207[358,629] 1.03us 5mb                                             |---------------L0.207---------------|       "
    - "L0.279[76,348] 1.03us 3mb    |---------------L0.279---------------|                                                "
    - "L0.280[349,356] 1.03us 96kb                                           |L0.280|                                       "
    - "L0.210[357,357] 1.03us 0b                                             |L0.210|                                     "
    - "L0.277[358,654] 1.03us 3mb                                             |-----------------L0.277-----------------|   "
    - "L0.278[655,670] 1.03us 192kb                                                                                       |L0.278|"
    - "L0.208[671,672] 1.03us 12kb                                                                                         |L0.208|"
    - "L1                                                                                                                 "
    - "L1.272[42,348] 1.03us 100mb|-----------------L1.272------------------|                                               "
    - "L1.273[349,654] 1.03us 100mb                                           |-----------------L1.273------------------|    "
    - "L1.274[655,672] 1.03us 6mb                                                                                       |L1.274|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 223mb total:"
    - "L1                                                                                                                 "
    - "L1.?[42,325] 1.03us 100mb|-----------------L1.?-----------------|                                                  "
    - "L1.?[326,608] 1.03us 100mb                                        |-----------------L1.?-----------------|          "
    - "L1.?[609,672] 1.03us 23mb                                                                                 |-L1.?--|"
    - "Committing partition 1:"
    - "  Soft Deleting 13 files: L0.206, L0.207, L0.208, L0.210, L1.272, L1.273, L1.274, L0.275, L0.276, L0.277, L0.278, L0.279, L0.280"
    - "  Creating 3 files"
    - "**** Simulation run 136, type=split(ReduceOverlap)(split_times=[608]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.281[358,654] 1.03us   |-----------------------------------------L0.281-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[358,608] 1.03us 3mb |-----------------------------------L0.?-----------------------------------|              "
    - "L0.?[609,654] 1.03us 501kb                                                                            |---L0.?----| "
    - "**** Simulation run 137, type=split(ReduceOverlap)(split_times=[325]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.283[42,348] 1.03us    |-----------------------------------------L0.283-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[42,325] 1.03us 3mb  |--------------------------------------L0.?---------------------------------------|       "
    - "L0.?[326,348] 1.03us 249kb                                                                                   |L0.?| "
    - "**** Simulation run 138, type=split(ReduceOverlap)(split_times=[608]). 1 Input Files, 4mb total:"
    - "L0, all files 4mb                                                                                                  "
    - "L0.285[358,654] 1.03us   |-----------------------------------------L0.285-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 4mb total:"
    - "L0                                                                                                                 "
    - "L0.?[358,608] 1.03us 3mb |-----------------------------------L0.?-----------------------------------|              "
    - "L0.?[609,654] 1.03us 608kb                                                                            |---L0.?----| "
    - "**** Simulation run 139, type=split(ReduceOverlap)(split_times=[325]). 1 Input Files, 2mb total:"
    - "L0, all files 2mb                                                                                                  "
    - "L0.287[173,348] 1.03us   |-----------------------------------------L0.287-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 2mb total:"
    - "L0                                                                                                                 "
    - "L0.?[173,325] 1.03us 2mb |------------------------------------L0.?------------------------------------|            "
    - "L0.?[326,348] 1.03us 303kb                                                                              |--L0.?---| "
    - "**** Simulation run 140, type=split(ReduceOverlap)(split_times=[608]). 1 Input Files, 5mb total:"
    - "L0, all files 5mb                                                                                                  "
    - "L0.221[358,629] 1.03us   |-----------------------------------------L0.221-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 5mb total:"
    - "L0                                                                                                                 "
    - "L0.?[358,608] 1.03us 4mb |--------------------------------------L0.?---------------------------------------|       "
    - "L0.?[609,629] 1.03us 374kb                                                                                   |L0.?| "
    - "**** Simulation run 141, type=split(ReduceOverlap)(split_times=[325]). 1 Input Files, 5mb total:"
    - "L0, all files 5mb                                                                                                  "
    - "L0.289[50,348] 1.03us    |-----------------------------------------L0.289-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 5mb total:"
    - "L0                                                                                                                 "
    - "L0.?[50,325] 1.03us 5mb  |--------------------------------------L0.?---------------------------------------|       "
    - "L0.?[326,348] 1.03us 407kb                                                                                   |L0.?| "
    - "**** Simulation run 142, type=split(ReduceOverlap)(split_times=[608]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.291[358,654] 1.04us   |-----------------------------------------L0.291-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[358,608] 1.04us 3mb |-----------------------------------L0.?-----------------------------------|              "
    - "L0.?[609,654] 1.04us 552kb                                                                            |---L0.?----| "
    - "**** Simulation run 143, type=split(ReduceOverlap)(split_times=[325]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.297[42,348] 1.04us    |-----------------------------------------L0.297-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[42,325] 1.04us 3mb  |--------------------------------------L0.?---------------------------------------|       "
    - "L0.?[326,348] 1.04us 249kb                                                                                   |L0.?| "
    - "**** Simulation run 144, type=split(ReduceOverlap)(split_times=[608]). 1 Input Files, 4mb total:"
    - "L0, all files 4mb                                                                                                  "
    - "L0.299[358,654] 1.04us   |-----------------------------------------L0.299-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 4mb total:"
    - "L0                                                                                                                 "
    - "L0.?[358,608] 1.04us 3mb |-----------------------------------L0.?-----------------------------------|              "
    - "L0.?[609,654] 1.04us 608kb                                                                            |---L0.?----| "
    - "**** Simulation run 145, type=split(ReduceOverlap)(split_times=[325]). 1 Input Files, 2mb total:"
    - "L0, all files 2mb                                                                                                  "
    - "L0.301[173,348] 1.04us   |-----------------------------------------L0.301-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 2mb total:"
    - "L0                                                                                                                 "
    - "L0.?[173,325] 1.04us 2mb |------------------------------------L0.?------------------------------------|            "
    - "L0.?[326,348] 1.04us 303kb                                                                              |--L0.?---| "
    - "**** Simulation run 146, type=split(ReduceOverlap)(split_times=[608]). 1 Input Files, 5mb total:"
    - "L0, all files 5mb                                                                                                  "
    - "L0.235[358,629] 1.04us   |-----------------------------------------L0.235-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 5mb total:"
    - "L0                                                                                                                 "
    - "L0.?[358,608] 1.04us 4mb |--------------------------------------L0.?---------------------------------------|       "
    - "L0.?[609,629] 1.04us 374kb                                                                                   |L0.?| "
    - "**** Simulation run 147, type=split(ReduceOverlap)(split_times=[325]). 1 Input Files, 5mb total:"
    - "L0, all files 5mb                                                                                                  "
    - "L0.303[50,348] 1.04us    |-----------------------------------------L0.303-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 5mb total:"
    - "L0                                                                                                                 "
    - "L0.?[50,325] 1.04us 5mb  |--------------------------------------L0.?---------------------------------------|       "
    - "L0.?[326,348] 1.04us 407kb                                                                                   |L0.?| "
    - "**** Simulation run 148, type=split(ReduceOverlap)(split_times=[608]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.305[358,654] 1.04us   |-----------------------------------------L0.305-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[358,608] 1.04us 3mb |-----------------------------------L0.?-----------------------------------|              "
    - "L0.?[609,654] 1.04us 552kb                                                                            |---L0.?----| "
    - "**** Simulation run 149, type=split(ReduceOverlap)(split_times=[325]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.307[76,348] 1.04us    |-----------------------------------------L0.307-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[76,325] 1.04us 3mb  |--------------------------------------L0.?--------------------------------------|        "
    - "L0.?[326,348] 1.04us 275kb                                                                                  |L0.?-| "
    - "**** Simulation run 150, type=split(ReduceOverlap)(split_times=[608]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.309[358,654] 1.04us   |-----------------------------------------L0.309-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[358,608] 1.04us 3mb |-----------------------------------L0.?-----------------------------------|              "
    - "L0.?[609,654] 1.04us 501kb                                                                            |---L0.?----| "
    - "**** Simulation run 151, type=split(ReduceOverlap)(split_times=[325]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.293[76,348] 1.04us    |-----------------------------------------L0.293-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[76,325] 1.04us 3mb  |--------------------------------------L0.?--------------------------------------|        "
    - "L0.?[326,348] 1.04us 275kb                                                                                  |L0.?-| "
    - "**** Simulation run 152, type=split(ReduceOverlap)(split_times=[608]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.295[358,654] 1.04us   |-----------------------------------------L0.295-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[358,608] 1.04us 3mb |-----------------------------------L0.?-----------------------------------|              "
    - "L0.?[609,654] 1.04us 501kb                                                                            |---L0.?----| "
    - "**** Simulation run 153, type=split(ReduceOverlap)(split_times=[325]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.311[42,348] 1.04us    |-----------------------------------------L0.311-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[42,325] 1.04us 3mb  |--------------------------------------L0.?---------------------------------------|       "
    - "L0.?[326,348] 1.04us 249kb                                                                                   |L0.?| "
    - "**** Simulation run 154, type=split(ReduceOverlap)(split_times=[608]). 1 Input Files, 4mb total:"
    - "L0, all files 4mb                                                                                                  "
    - "L0.313[358,654] 1.04us   |-----------------------------------------L0.313-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 4mb total:"
    - "L0                                                                                                                 "
    - "L0.?[358,608] 1.04us 3mb |-----------------------------------L0.?-----------------------------------|              "
    - "L0.?[609,654] 1.04us 608kb                                                                            |---L0.?----| "
    - "**** Simulation run 155, type=split(ReduceOverlap)(split_times=[325]). 1 Input Files, 2mb total:"
    - "L0, all files 2mb                                                                                                  "
    - "L0.315[173,348] 1.04us   |-----------------------------------------L0.315-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 2mb total:"
    - "L0                                                                                                                 "
    - "L0.?[173,325] 1.04us 2mb |------------------------------------L0.?------------------------------------|            "
    - "L0.?[326,348] 1.04us 303kb                                                                              |--L0.?---| "
    - "**** Simulation run 156, type=split(ReduceOverlap)(split_times=[608]). 1 Input Files, 5mb total:"
    - "L0, all files 5mb                                                                                                  "
    - "L0.249[358,629] 1.04us   |-----------------------------------------L0.249-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 5mb total:"
    - "L0                                                                                                                 "
    - "L0.?[358,608] 1.04us 4mb |--------------------------------------L0.?---------------------------------------|       "
    - "L0.?[609,629] 1.04us 374kb                                                                                   |L0.?| "
    - "**** Simulation run 157, type=split(ReduceOverlap)(split_times=[325]). 1 Input Files, 5mb total:"
    - "L0, all files 5mb                                                                                                  "
    - "L0.317[50,348] 1.04us    |-----------------------------------------L0.317-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 5mb total:"
    - "L0                                                                                                                 "
    - "L0.?[50,325] 1.04us 5mb  |--------------------------------------L0.?---------------------------------------|       "
    - "L0.?[326,348] 1.04us 407kb                                                                                   |L0.?| "
    - "**** Simulation run 158, type=split(ReduceOverlap)(split_times=[608]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.319[358,654] 1.04us   |-----------------------------------------L0.319-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[358,608] 1.04us 3mb |-----------------------------------L0.?-----------------------------------|              "
    - "L0.?[609,654] 1.04us 552kb                                                                            |---L0.?----| "
    - "**** Simulation run 159, type=split(ReduceOverlap)(split_times=[325]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.321[76,348] 1.04us    |-----------------------------------------L0.321-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[76,325] 1.04us 3mb  |--------------------------------------L0.?--------------------------------------|        "
    - "L0.?[326,348] 1.04us 275kb                                                                                  |L0.?-| "
    - "**** Simulation run 160, type=split(ReduceOverlap)(split_times=[608]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.323[358,654] 1.05us   |-----------------------------------------L0.323-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[358,608] 1.05us 3mb |-----------------------------------L0.?-----------------------------------|              "
    - "L0.?[609,654] 1.05us 501kb                                                                            |---L0.?----| "
    - "**** Simulation run 161, type=split(ReduceOverlap)(split_times=[325]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.325[42,348] 1.05us    |-----------------------------------------L0.325-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[42,325] 1.05us 3mb  |--------------------------------------L0.?---------------------------------------|       "
    - "L0.?[326,348] 1.05us 249kb                                                                                   |L0.?| "
    - "**** Simulation run 162, type=split(ReduceOverlap)(split_times=[608]). 1 Input Files, 4mb total:"
    - "L0, all files 4mb                                                                                                  "
    - "L0.327[358,654] 1.05us   |-----------------------------------------L0.327-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 4mb total:"
    - "L0                                                                                                                 "
    - "L0.?[358,608] 1.05us 3mb |-----------------------------------L0.?-----------------------------------|              "
    - "L0.?[609,654] 1.05us 608kb                                                                            |---L0.?----| "
    - "**** Simulation run 163, type=split(ReduceOverlap)(split_times=[325]). 1 Input Files, 2mb total:"
    - "L0, all files 2mb                                                                                                  "
    - "L0.329[173,348] 1.05us   |-----------------------------------------L0.329-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 2mb total:"
    - "L0                                                                                                                 "
    - "L0.?[173,325] 1.05us 2mb |------------------------------------L0.?------------------------------------|            "
    - "L0.?[326,348] 1.05us 303kb                                                                              |--L0.?---| "
    - "**** Simulation run 164, type=split(ReduceOverlap)(split_times=[608]). 1 Input Files, 5mb total:"
    - "L0, all files 5mb                                                                                                  "
    - "L0.263[358,629] 1.05us   |-----------------------------------------L0.263-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 5mb total:"
    - "L0                                                                                                                 "
    - "L0.?[358,608] 1.05us 4mb |--------------------------------------L0.?---------------------------------------|       "
    - "L0.?[609,629] 1.05us 374kb                                                                                   |L0.?| "
    - "**** Simulation run 165, type=split(ReduceOverlap)(split_times=[325]). 1 Input Files, 5mb total:"
    - "L0, all files 5mb                                                                                                  "
    - "L0.331[50,348] 1.05us    |-----------------------------------------L0.331-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 5mb total:"
    - "L0                                                                                                                 "
    - "L0.?[50,325] 1.05us 5mb  |--------------------------------------L0.?---------------------------------------|       "
    - "L0.?[326,348] 1.05us 407kb                                                                                   |L0.?| "
    - "**** Simulation run 166, type=split(ReduceOverlap)(split_times=[608]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.333[358,654] 1.05us   |-----------------------------------------L0.333-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[358,608] 1.05us 3mb |-----------------------------------L0.?-----------------------------------|              "
    - "L0.?[609,654] 1.05us 552kb                                                                            |---L0.?----| "
    - "**** Simulation run 167, type=split(ReduceOverlap)(split_times=[325]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.335[76,348] 1.05us    |-----------------------------------------L0.335-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[76,325] 1.05us 3mb  |--------------------------------------L0.?--------------------------------------|        "
    - "L0.?[326,348] 1.05us 275kb                                                                                  |L0.?-| "
    - "**** Simulation run 168, type=split(ReduceOverlap)(split_times=[608]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.337[358,654] 1.05us   |-----------------------------------------L0.337-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[358,608] 1.05us 3mb |-----------------------------------L0.?-----------------------------------|              "
    - "L0.?[609,654] 1.05us 501kb                                                                            |---L0.?----| "
    - "**** Simulation run 169, type=split(ReduceOverlap)(split_times=[325]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.339[42,348] 1.05us    |-----------------------------------------L0.339-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[42,325] 1.05us 3mb  |--------------------------------------L0.?---------------------------------------|       "
    - "L0.?[326,348] 1.05us 249kb                                                                                   |L0.?| "
    - "Committing partition 1:"
    - "  Soft Deleting 34 files: L0.221, L0.235, L0.249, L0.263, L0.281, L0.283, L0.285, L0.287, L0.289, L0.291, L0.293, L0.295, L0.297, L0.299, L0.301, L0.303, L0.305, L0.307, L0.309, L0.311, L0.313, L0.315, L0.317, L0.319, L0.321, L0.323, L0.325, L0.327, L0.329, L0.331, L0.333, L0.335, L0.337, L0.339"
    - "  Creating 68 files"
    - "**** Simulation run 170, type=split(CompactAndSplitOutput(FoundSubsetLessThanMaxCompactSize))(split_times=[923]). 4 Input Files, 107mb total:"
    - "L0                                                                                                                 "
    - "L0.343[673,923] 1.03us 3mb|-------------------------------L0.343--------------------------------|                   "
    - "L0.344[924,932] 1.03us 109kb                                                                        |L0.344|          "
    - "L1                                                                                                                 "
    - "L1.341[673,923] 1.03us 83mb|-------------------------------L1.341--------------------------------|                   "
    - "L1.342[924,986] 1.03us 21mb                                                                        |----L1.342-----| "
    - "**** 2 Output Files (parquet_file_id not yet assigned), 107mb total:"
    - "L1                                                                                                                 "
    - "L1.?[673,923] 1.03us 85mb|--------------------------------L1.?---------------------------------|                   "
    - "L1.?[924,986] 1.03us 21mb                                                                        |-----L1.?------| "
    - "Committing partition 1:"
    - "  Soft Deleting 4 files: L1.341, L1.342, L0.343, L0.344"
    - "  Creating 2 files"
    - "**** Simulation run 171, type=split(CompactAndSplitOutput(FoundSubsetLessThanMaxCompactSize))(split_times=[317, 592]). 11 Input Files, 230mb total:"
    - "L0                                                                                                                 "
    - "L0.376[42,325] 1.03us 3mb|----------------L0.376----------------|                                                  "
    - "L0.377[326,348] 1.03us 249kb                                        |L0.377|                                          "
    - "L0.284[349,356] 1.03us 87kb                                           |L0.284|                                       "
    - "L0.214[357,357] 1.03us 0b                                             |L0.214|                                     "
    - "L0.374[358,608] 1.03us 3mb                                             |-------------L0.374--------------|          "
    - "L0.375[609,654] 1.03us 501kb                                                                                 |L0.375| "
    - "L0.282[655,670] 1.03us 174kb                                                                                       |L0.282|"
    - "L0.212[671,672] 1.03us 11kb                                                                                         |L0.212|"
    - "L1                                                                                                                 "
    - "L1.371[42,325] 1.03us 100mb|----------------L1.371----------------|                                                  "
    - "L1.372[326,608] 1.03us 100mb                                        |----------------L1.372----------------|          "
    - "L1.373[609,672] 1.03us 23mb                                                                                 |L1.373-|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 230mb total:"
    - "L1                                                                                                                 "
    - "L1.?[42,317] 1.03us 100mb|----------------L1.?-----------------|                                                   "
    - "L1.?[318,592] 1.03us 100mb                                       |----------------L1.?-----------------|            "
    - "L1.?[593,672] 1.03us 30mb                                                                              |--L1.?---| "
    - "Committing partition 1:"
    - "  Soft Deleting 11 files: L0.212, L0.214, L0.282, L0.284, L1.371, L1.372, L1.373, L0.374, L0.375, L0.376, L0.377"
    - "  Creating 3 files"
    - "**** Simulation run 172, type=split(ReduceOverlap)(split_times=[592]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.378[358,608] 1.03us   |-----------------------------------------L0.378-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[358,592] 1.03us 3mb |---------------------------------------L0.?---------------------------------------|      "
    - "L0.?[593,608] 1.03us 212kb                                                                                    |L0.?|"
    - "**** Simulation run 173, type=split(ReduceOverlap)(split_times=[317]). 1 Input Files, 2mb total:"
    - "L0, all files 2mb                                                                                                  "
    - "L0.380[173,325] 1.03us   |-----------------------------------------L0.380-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 2mb total:"
    - "L0                                                                                                                 "
    - "L0.?[173,317] 1.03us 2mb |---------------------------------------L0.?----------------------------------------|     "
    - "L0.?[318,325] 1.03us 105kb                                                                                     |L0.?|"
    - "**** Simulation run 174, type=split(ReduceOverlap)(split_times=[592]). 1 Input Files, 4mb total:"
    - "L0, all files 4mb                                                                                                  "
    - "L0.382[358,608] 1.03us   |-----------------------------------------L0.382-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 4mb total:"
    - "L0                                                                                                                 "
    - "L0.?[358,592] 1.03us 4mb |---------------------------------------L0.?---------------------------------------|      "
    - "L0.?[593,608] 1.03us 285kb                                                                                    |L0.?|"
    - "**** Simulation run 175, type=split(ReduceOverlap)(split_times=[317]). 1 Input Files, 5mb total:"
    - "L0, all files 5mb                                                                                                  "
    - "L0.384[50,325] 1.03us    |-----------------------------------------L0.384-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 5mb total:"
    - "L0                                                                                                                 "
    - "L0.?[50,317] 1.03us 5mb  |----------------------------------------L0.?-----------------------------------------|   "
    - "L0.?[318,325] 1.03us 141kb                                                                                       |L0.?|"
    - "**** Simulation run 176, type=split(ReduceOverlap)(split_times=[592]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.386[358,608] 1.04us   |-----------------------------------------L0.386-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[358,592] 1.04us 3mb |---------------------------------------L0.?---------------------------------------|      "
    - "L0.?[593,608] 1.04us 192kb                                                                                    |L0.?|"
    - "**** Simulation run 177, type=split(ReduceOverlap)(split_times=[317]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.404[76,325] 1.04us    |-----------------------------------------L0.404-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[76,317] 1.04us 3mb  |----------------------------------------L0.?-----------------------------------------|   "
    - "L0.?[318,325] 1.04us 96kb                                                                                       |L0.?|"
    - "**** Simulation run 178, type=split(ReduceOverlap)(split_times=[592]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.406[358,608] 1.04us   |-----------------------------------------L0.406-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[358,592] 1.04us 2mb |---------------------------------------L0.?---------------------------------------|      "
    - "L0.?[593,608] 1.04us 174kb                                                                                    |L0.?|"
    - "**** Simulation run 179, type=split(ReduceOverlap)(split_times=[317]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.388[42,325] 1.04us    |-----------------------------------------L0.388-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[42,317] 1.04us 3mb  |----------------------------------------L0.?-----------------------------------------|   "
    - "L0.?[318,325] 1.04us 87kb                                                                                       |L0.?|"
    - "**** Simulation run 180, type=split(ReduceOverlap)(split_times=[592]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.390[358,608] 1.04us   |-----------------------------------------L0.390-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[358,592] 1.04us 3mb |---------------------------------------L0.?---------------------------------------|      "
    - "L0.?[593,608] 1.04us 212kb                                                                                    |L0.?|"
    - "**** Simulation run 181, type=split(ReduceOverlap)(split_times=[317]). 1 Input Files, 2mb total:"
    - "L0, all files 2mb                                                                                                  "
    - "L0.392[173,325] 1.04us   |-----------------------------------------L0.392-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 2mb total:"
    - "L0                                                                                                                 "
    - "L0.?[173,317] 1.04us 2mb |---------------------------------------L0.?----------------------------------------|     "
    - "L0.?[318,325] 1.04us 105kb                                                                                     |L0.?|"
    - "**** Simulation run 182, type=split(ReduceOverlap)(split_times=[592]). 1 Input Files, 4mb total:"
    - "L0, all files 4mb                                                                                                  "
    - "L0.394[358,608] 1.04us   |-----------------------------------------L0.394-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 4mb total:"
    - "L0                                                                                                                 "
    - "L0.?[358,592] 1.04us 4mb |---------------------------------------L0.?---------------------------------------|      "
    - "L0.?[593,608] 1.04us 285kb                                                                                    |L0.?|"
    - "**** Simulation run 183, type=split(ReduceOverlap)(split_times=[317]). 1 Input Files, 5mb total:"
    - "L0, all files 5mb                                                                                                  "
    - "L0.396[50,325] 1.04us    |-----------------------------------------L0.396-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 5mb total:"
    - "L0                                                                                                                 "
    - "L0.?[50,317] 1.04us 5mb  |----------------------------------------L0.?-----------------------------------------|   "
    - "L0.?[318,325] 1.04us 141kb                                                                                       |L0.?|"
    - "**** Simulation run 184, type=split(ReduceOverlap)(split_times=[592]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.398[358,608] 1.04us   |-----------------------------------------L0.398-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[358,592] 1.04us 3mb |---------------------------------------L0.?---------------------------------------|      "
    - "L0.?[593,608] 1.04us 192kb                                                                                    |L0.?|"
    - "**** Simulation run 185, type=split(ReduceOverlap)(split_times=[317]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.400[76,325] 1.04us    |-----------------------------------------L0.400-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[76,317] 1.04us 3mb  |----------------------------------------L0.?-----------------------------------------|   "
    - "L0.?[318,325] 1.04us 96kb                                                                                       |L0.?|"
    - "**** Simulation run 186, type=split(ReduceOverlap)(split_times=[592]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.402[358,608] 1.04us   |-----------------------------------------L0.402-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[358,592] 1.04us 2mb |---------------------------------------L0.?---------------------------------------|      "
    - "L0.?[593,608] 1.04us 174kb                                                                                    |L0.?|"
    - "**** Simulation run 187, type=split(ReduceOverlap)(split_times=[317]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.408[42,325] 1.04us    |-----------------------------------------L0.408-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[42,317] 1.04us 3mb  |----------------------------------------L0.?-----------------------------------------|   "
    - "L0.?[318,325] 1.04us 87kb                                                                                       |L0.?|"
    - "**** Simulation run 188, type=split(ReduceOverlap)(split_times=[592]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.410[358,608] 1.04us   |-----------------------------------------L0.410-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[358,592] 1.04us 3mb |---------------------------------------L0.?---------------------------------------|      "
    - "L0.?[593,608] 1.04us 212kb                                                                                    |L0.?|"
    - "**** Simulation run 189, type=split(ReduceOverlap)(split_times=[317]). 1 Input Files, 2mb total:"
    - "L0, all files 2mb                                                                                                  "
    - "L0.412[173,325] 1.04us   |-----------------------------------------L0.412-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 2mb total:"
    - "L0                                                                                                                 "
    - "L0.?[173,317] 1.04us 2mb |---------------------------------------L0.?----------------------------------------|     "
    - "L0.?[318,325] 1.04us 105kb                                                                                     |L0.?|"
    - "**** Simulation run 190, type=split(ReduceOverlap)(split_times=[592]). 1 Input Files, 4mb total:"
    - "L0, all files 4mb                                                                                                  "
    - "L0.414[358,608] 1.04us   |-----------------------------------------L0.414-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 4mb total:"
    - "L0                                                                                                                 "
    - "L0.?[358,592] 1.04us 4mb |---------------------------------------L0.?---------------------------------------|      "
    - "L0.?[593,608] 1.04us 285kb                                                                                    |L0.?|"
    - "**** Simulation run 191, type=split(ReduceOverlap)(split_times=[317]). 1 Input Files, 5mb total:"
    - "L0, all files 5mb                                                                                                  "
    - "L0.416[50,325] 1.04us    |-----------------------------------------L0.416-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 5mb total:"
    - "L0                                                                                                                 "
    - "L0.?[50,317] 1.04us 5mb  |----------------------------------------L0.?-----------------------------------------|   "
    - "L0.?[318,325] 1.04us 141kb                                                                                       |L0.?|"
    - "**** Simulation run 192, type=split(ReduceOverlap)(split_times=[592]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.418[358,608] 1.04us   |-----------------------------------------L0.418-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[358,592] 1.04us 3mb |---------------------------------------L0.?---------------------------------------|      "
    - "L0.?[593,608] 1.04us 192kb                                                                                    |L0.?|"
    - "**** Simulation run 193, type=split(ReduceOverlap)(split_times=[317]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.420[76,325] 1.04us    |-----------------------------------------L0.420-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[76,317] 1.04us 3mb  |----------------------------------------L0.?-----------------------------------------|   "
    - "L0.?[318,325] 1.04us 96kb                                                                                       |L0.?|"
    - "**** Simulation run 194, type=split(ReduceOverlap)(split_times=[592]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.422[358,608] 1.05us   |-----------------------------------------L0.422-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[358,592] 1.05us 2mb |---------------------------------------L0.?---------------------------------------|      "
    - "L0.?[593,608] 1.05us 174kb                                                                                    |L0.?|"
    - "**** Simulation run 195, type=split(ReduceOverlap)(split_times=[317]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.424[42,325] 1.05us    |-----------------------------------------L0.424-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[42,317] 1.05us 3mb  |----------------------------------------L0.?-----------------------------------------|   "
    - "L0.?[318,325] 1.05us 87kb                                                                                       |L0.?|"
    - "**** Simulation run 196, type=split(ReduceOverlap)(split_times=[592]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.426[358,608] 1.05us   |-----------------------------------------L0.426-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[358,592] 1.05us 3mb |---------------------------------------L0.?---------------------------------------|      "
    - "L0.?[593,608] 1.05us 212kb                                                                                    |L0.?|"
    - "**** Simulation run 197, type=split(ReduceOverlap)(split_times=[317]). 1 Input Files, 2mb total:"
    - "L0, all files 2mb                                                                                                  "
    - "L0.428[173,325] 1.05us   |-----------------------------------------L0.428-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 2mb total:"
    - "L0                                                                                                                 "
    - "L0.?[173,317] 1.05us 2mb |---------------------------------------L0.?----------------------------------------|     "
    - "L0.?[318,325] 1.05us 105kb                                                                                     |L0.?|"
    - "**** Simulation run 198, type=split(ReduceOverlap)(split_times=[592]). 1 Input Files, 4mb total:"
    - "L0, all files 4mb                                                                                                  "
    - "L0.430[358,608] 1.05us   |-----------------------------------------L0.430-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 4mb total:"
    - "L0                                                                                                                 "
    - "L0.?[358,592] 1.05us 4mb |---------------------------------------L0.?---------------------------------------|      "
    - "L0.?[593,608] 1.05us 285kb                                                                                    |L0.?|"
    - "**** Simulation run 199, type=split(ReduceOverlap)(split_times=[317]). 1 Input Files, 5mb total:"
    - "L0, all files 5mb                                                                                                  "
    - "L0.432[50,325] 1.05us    |-----------------------------------------L0.432-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 5mb total:"
    - "L0                                                                                                                 "
    - "L0.?[50,317] 1.05us 5mb  |----------------------------------------L0.?-----------------------------------------|   "
    - "L0.?[318,325] 1.05us 141kb                                                                                       |L0.?|"
    - "**** Simulation run 200, type=split(ReduceOverlap)(split_times=[592]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.434[358,608] 1.05us   |-----------------------------------------L0.434-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[358,592] 1.05us 3mb |---------------------------------------L0.?---------------------------------------|      "
    - "L0.?[593,608] 1.05us 192kb                                                                                    |L0.?|"
    - "**** Simulation run 201, type=split(ReduceOverlap)(split_times=[317]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.436[76,325] 1.05us    |-----------------------------------------L0.436-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[76,317] 1.05us 3mb  |----------------------------------------L0.?-----------------------------------------|   "
    - "L0.?[318,325] 1.05us 96kb                                                                                       |L0.?|"
    - "**** Simulation run 202, type=split(ReduceOverlap)(split_times=[592]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.438[358,608] 1.05us   |-----------------------------------------L0.438-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[358,592] 1.05us 2mb |---------------------------------------L0.?---------------------------------------|      "
    - "L0.?[593,608] 1.05us 174kb                                                                                    |L0.?|"
    - "**** Simulation run 203, type=split(ReduceOverlap)(split_times=[317]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.440[42,325] 1.05us    |-----------------------------------------L0.440-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[42,317] 1.05us 3mb  |----------------------------------------L0.?-----------------------------------------|   "
    - "L0.?[318,325] 1.05us 87kb                                                                                       |L0.?|"
    - "Committing partition 1:"
    - "  Soft Deleting 32 files: L0.378, L0.380, L0.382, L0.384, L0.386, L0.388, L0.390, L0.392, L0.394, L0.396, L0.398, L0.400, L0.402, L0.404, L0.406, L0.408, L0.410, L0.412, L0.414, L0.416, L0.418, L0.420, L0.422, L0.424, L0.426, L0.428, L0.430, L0.432, L0.434, L0.436, L0.438, L0.440"
    - "  Creating 64 files"
    - "**** Simulation run 204, type=split(CompactAndSplitOutput(FoundSubsetLessThanMaxCompactSize))(split_times=[923]). 4 Input Files, 110mb total:"
    - "L0                                                                                                                 "
    - "L0.345[673,923] 1.03us 3mb|-------------------------------L0.345--------------------------------|                   "
    - "L0.346[924,986] 1.03us 690kb                                                                        |----L0.346-----| "
    - "L1                                                                                                                 "
    - "L1.442[673,923] 1.03us 85mb|-------------------------------L1.442--------------------------------|                   "
    - "L1.443[924,986] 1.03us 21mb                                                                        |----L1.443-----| "
    - "**** 2 Output Files (parquet_file_id not yet assigned), 110mb total:"
    - "L1                                                                                                                 "
    - "L1.?[673,923] 1.03us 88mb|--------------------------------L1.?---------------------------------|                   "
    - "L1.?[924,986] 1.03us 22mb                                                                        |-----L1.?------| "
    - "Committing partition 1:"
    - "  Soft Deleting 4 files: L0.345, L0.346, L1.442, L1.443"
    - "  Creating 2 files"
    - "**** Simulation run 205, type=split(CompactAndSplitOutput(FoundSubsetLessThanMaxCompactSize))(split_times=[309, 576]). 13 Input Files, 236mb total:"
    - "L0                                                                                                                 "
    - "L0.449[173,317] 1.03us 2mb                  |------L0.449------|                                                    "
    - "L0.450[318,325] 1.03us 105kb                                       |L0.450|                                           "
    - "L0.381[326,348] 1.03us 303kb                                        |L0.381|                                          "
    - "L0.288[349,356] 1.03us 105kb                                           |L0.288|                                       "
    - "L0.218[357,357] 1.03us 0b                                             |L0.218|                                     "
    - "L0.447[358,592] 1.03us 3mb                                             |------------L0.447-------------|            "
    - "L0.448[593,608] 1.03us 212kb                                                                              |L0.448|    "
    - "L0.379[609,654] 1.03us 608kb                                                                                 |L0.379| "
    - "L0.286[655,670] 1.03us 212kb                                                                                       |L0.286|"
    - "L0.216[671,672] 1.03us 13kb                                                                                         |L0.216|"
    - "L1                                                                                                                 "
    - "L1.444[42,317] 1.03us 100mb|---------------L1.444----------------|                                                   "
    - "L1.445[318,592] 1.03us 100mb                                       |---------------L1.445----------------|            "
    - "L1.446[593,672] 1.03us 30mb                                                                              |-L1.446--| "
    - "**** 3 Output Files (parquet_file_id not yet assigned), 236mb total:"
    - "L1                                                                                                                 "
    - "L1.?[42,309] 1.03us 100mb|----------------L1.?----------------|                                                    "
    - "L1.?[310,576] 1.03us 100mb                                      |----------------L1.?----------------|              "
    - "L1.?[577,672] 1.03us 36mb                                                                            |---L1.?----| "
    - "Committing partition 1:"
    - "  Soft Deleting 13 files: L0.216, L0.218, L0.286, L0.288, L0.379, L0.381, L1.444, L1.445, L1.446, L0.447, L0.448, L0.449, L0.450"
    - "  Creating 3 files"
    - "**** Simulation run 206, type=split(ReduceOverlap)(split_times=[576]). 1 Input Files, 4mb total:"
    - "L0, all files 4mb                                                                                                  "
    - "L0.451[358,592] 1.03us   |-----------------------------------------L0.451-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 4mb total:"
    - "L0                                                                                                                 "
    - "L0.?[358,576] 1.03us 4mb |--------------------------------------L0.?---------------------------------------|       "
    - "L0.?[577,592] 1.03us 285kb                                                                                    |L0.?|"
    - "**** Simulation run 207, type=split(ReduceOverlap)(split_times=[309]). 1 Input Files, 5mb total:"
    - "L0, all files 5mb                                                                                                  "
    - "L0.453[50,317] 1.03us    |-----------------------------------------L0.453-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 5mb total:"
    - "L0                                                                                                                 "
    - "L0.?[50,309] 1.03us 4mb  |----------------------------------------L0.?-----------------------------------------|   "
    - "L0.?[310,317] 1.03us 141kb                                                                                       |L0.?|"
    - "**** Simulation run 208, type=split(ReduceOverlap)(split_times=[576]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.455[358,592] 1.04us   |-----------------------------------------L0.455-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[358,576] 1.04us 3mb |--------------------------------------L0.?---------------------------------------|       "
    - "L0.?[577,592] 1.04us 192kb                                                                                    |L0.?|"
    - "**** Simulation run 209, type=split(ReduceOverlap)(split_times=[309]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.457[76,317] 1.04us    |-----------------------------------------L0.457-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[76,309] 1.04us 3mb  |----------------------------------------L0.?-----------------------------------------|   "
    - "L0.?[310,317] 1.04us 96kb                                                                                       |L0.?|"
    - "**** Simulation run 210, type=split(ReduceOverlap)(split_times=[576]). 1 Input Files, 2mb total:"
    - "L0, all files 2mb                                                                                                  "
    - "L0.459[358,592] 1.04us   |-----------------------------------------L0.459-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 2mb total:"
    - "L0                                                                                                                 "
    - "L0.?[358,576] 1.04us 2mb |--------------------------------------L0.?---------------------------------------|       "
    - "L0.?[577,592] 1.04us 174kb                                                                                    |L0.?|"
    - "**** Simulation run 211, type=split(ReduceOverlap)(split_times=[309]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.461[42,317] 1.04us    |-----------------------------------------L0.461-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[42,309] 1.04us 3mb  |----------------------------------------L0.?-----------------------------------------|   "
    - "L0.?[310,317] 1.04us 87kb                                                                                       |L0.?|"
    - "**** Simulation run 212, type=split(ReduceOverlap)(split_times=[576]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.463[358,592] 1.04us   |-----------------------------------------L0.463-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[358,576] 1.04us 3mb |--------------------------------------L0.?---------------------------------------|       "
    - "L0.?[577,592] 1.04us 212kb                                                                                    |L0.?|"
    - "**** Simulation run 213, type=split(ReduceOverlap)(split_times=[309]). 1 Input Files, 2mb total:"
    - "L0, all files 2mb                                                                                                  "
    - "L0.465[173,317] 1.04us   |-----------------------------------------L0.465-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 2mb total:"
    - "L0                                                                                                                 "
    - "L0.?[173,309] 1.04us 2mb |---------------------------------------L0.?----------------------------------------|     "
    - "L0.?[310,317] 1.04us 105kb                                                                                     |L0.?|"
    - "**** Simulation run 214, type=split(ReduceOverlap)(split_times=[576]). 1 Input Files, 4mb total:"
    - "L0, all files 4mb                                                                                                  "
    - "L0.467[358,592] 1.04us   |-----------------------------------------L0.467-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 4mb total:"
    - "L0                                                                                                                 "
    - "L0.?[358,576] 1.04us 4mb |--------------------------------------L0.?---------------------------------------|       "
    - "L0.?[577,592] 1.04us 285kb                                                                                    |L0.?|"
    - "**** Simulation run 215, type=split(ReduceOverlap)(split_times=[309]). 1 Input Files, 5mb total:"
    - "L0, all files 5mb                                                                                                  "
    - "L0.469[50,317] 1.04us    |-----------------------------------------L0.469-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 5mb total:"
    - "L0                                                                                                                 "
    - "L0.?[50,309] 1.04us 4mb  |----------------------------------------L0.?-----------------------------------------|   "
    - "L0.?[310,317] 1.04us 141kb                                                                                       |L0.?|"
    - "**** Simulation run 216, type=split(ReduceOverlap)(split_times=[576]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.471[358,592] 1.04us   |-----------------------------------------L0.471-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[358,576] 1.04us 3mb |--------------------------------------L0.?---------------------------------------|       "
    - "L0.?[577,592] 1.04us 192kb                                                                                    |L0.?|"
    - "**** Simulation run 217, type=split(ReduceOverlap)(split_times=[309]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.473[76,317] 1.04us    |-----------------------------------------L0.473-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[76,309] 1.04us 3mb  |----------------------------------------L0.?-----------------------------------------|   "
    - "L0.?[310,317] 1.04us 96kb                                                                                       |L0.?|"
    - "**** Simulation run 218, type=split(ReduceOverlap)(split_times=[576]). 1 Input Files, 2mb total:"
    - "L0, all files 2mb                                                                                                  "
    - "L0.475[358,592] 1.04us   |-----------------------------------------L0.475-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 2mb total:"
    - "L0                                                                                                                 "
    - "L0.?[358,576] 1.04us 2mb |--------------------------------------L0.?---------------------------------------|       "
    - "L0.?[577,592] 1.04us 174kb                                                                                    |L0.?|"
    - "**** Simulation run 219, type=split(ReduceOverlap)(split_times=[309]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.477[42,317] 1.04us    |-----------------------------------------L0.477-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[42,309] 1.04us 3mb  |----------------------------------------L0.?-----------------------------------------|   "
    - "L0.?[310,317] 1.04us 87kb                                                                                       |L0.?|"
    - "**** Simulation run 220, type=split(ReduceOverlap)(split_times=[576]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.479[358,592] 1.04us   |-----------------------------------------L0.479-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[358,576] 1.04us 3mb |--------------------------------------L0.?---------------------------------------|       "
    - "L0.?[577,592] 1.04us 212kb                                                                                    |L0.?|"
    - "**** Simulation run 221, type=split(ReduceOverlap)(split_times=[309]). 1 Input Files, 2mb total:"
    - "L0, all files 2mb                                                                                                  "
    - "L0.481[173,317] 1.04us   |-----------------------------------------L0.481-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 2mb total:"
    - "L0                                                                                                                 "
    - "L0.?[173,309] 1.04us 2mb |---------------------------------------L0.?----------------------------------------|     "
    - "L0.?[310,317] 1.04us 105kb                                                                                     |L0.?|"
    - "**** Simulation run 222, type=split(ReduceOverlap)(split_times=[576]). 1 Input Files, 4mb total:"
    - "L0, all files 4mb                                                                                                  "
    - "L0.483[358,592] 1.04us   |-----------------------------------------L0.483-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 4mb total:"
    - "L0                                                                                                                 "
    - "L0.?[358,576] 1.04us 4mb |--------------------------------------L0.?---------------------------------------|       "
    - "L0.?[577,592] 1.04us 285kb                                                                                    |L0.?|"
    - "**** Simulation run 223, type=split(ReduceOverlap)(split_times=[309]). 1 Input Files, 5mb total:"
    - "L0, all files 5mb                                                                                                  "
    - "L0.485[50,317] 1.04us    |-----------------------------------------L0.485-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 5mb total:"
    - "L0                                                                                                                 "
    - "L0.?[50,309] 1.04us 4mb  |----------------------------------------L0.?-----------------------------------------|   "
    - "L0.?[310,317] 1.04us 141kb                                                                                       |L0.?|"
    - "**** Simulation run 224, type=split(ReduceOverlap)(split_times=[576]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.487[358,592] 1.04us   |-----------------------------------------L0.487-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[358,576] 1.04us 3mb |--------------------------------------L0.?---------------------------------------|       "
    - "L0.?[577,592] 1.04us 192kb                                                                                    |L0.?|"
    - "**** Simulation run 225, type=split(ReduceOverlap)(split_times=[309]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.489[76,317] 1.04us    |-----------------------------------------L0.489-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[76,309] 1.04us 3mb  |----------------------------------------L0.?-----------------------------------------|   "
    - "L0.?[310,317] 1.04us 96kb                                                                                       |L0.?|"
    - "**** Simulation run 226, type=split(ReduceOverlap)(split_times=[576]). 1 Input Files, 2mb total:"
    - "L0, all files 2mb                                                                                                  "
    - "L0.491[358,592] 1.05us   |-----------------------------------------L0.491-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 2mb total:"
    - "L0                                                                                                                 "
    - "L0.?[358,576] 1.05us 2mb |--------------------------------------L0.?---------------------------------------|       "
    - "L0.?[577,592] 1.05us 174kb                                                                                    |L0.?|"
    - "**** Simulation run 227, type=split(ReduceOverlap)(split_times=[309]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.493[42,317] 1.05us    |-----------------------------------------L0.493-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[42,309] 1.05us 3mb  |----------------------------------------L0.?-----------------------------------------|   "
    - "L0.?[310,317] 1.05us 87kb                                                                                       |L0.?|"
    - "**** Simulation run 228, type=split(ReduceOverlap)(split_times=[576]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.495[358,592] 1.05us   |-----------------------------------------L0.495-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[358,576] 1.05us 3mb |--------------------------------------L0.?---------------------------------------|       "
    - "L0.?[577,592] 1.05us 212kb                                                                                    |L0.?|"
    - "**** Simulation run 229, type=split(ReduceOverlap)(split_times=[309]). 1 Input Files, 2mb total:"
    - "L0, all files 2mb                                                                                                  "
    - "L0.497[173,317] 1.05us   |-----------------------------------------L0.497-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 2mb total:"
    - "L0                                                                                                                 "
    - "L0.?[173,309] 1.05us 2mb |---------------------------------------L0.?----------------------------------------|     "
    - "L0.?[310,317] 1.05us 105kb                                                                                     |L0.?|"
    - "**** Simulation run 230, type=split(ReduceOverlap)(split_times=[576]). 1 Input Files, 4mb total:"
    - "L0, all files 4mb                                                                                                  "
    - "L0.499[358,592] 1.05us   |-----------------------------------------L0.499-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 4mb total:"
    - "L0                                                                                                                 "
    - "L0.?[358,576] 1.05us 4mb |--------------------------------------L0.?---------------------------------------|       "
    - "L0.?[577,592] 1.05us 285kb                                                                                    |L0.?|"
    - "**** Simulation run 231, type=split(ReduceOverlap)(split_times=[309]). 1 Input Files, 5mb total:"
    - "L0, all files 5mb                                                                                                  "
    - "L0.501[50,317] 1.05us    |-----------------------------------------L0.501-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 5mb total:"
    - "L0                                                                                                                 "
    - "L0.?[50,309] 1.05us 4mb  |----------------------------------------L0.?-----------------------------------------|   "
    - "L0.?[310,317] 1.05us 141kb                                                                                       |L0.?|"
    - "**** Simulation run 232, type=split(ReduceOverlap)(split_times=[576]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.503[358,592] 1.05us   |-----------------------------------------L0.503-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[358,576] 1.05us 3mb |--------------------------------------L0.?---------------------------------------|       "
    - "L0.?[577,592] 1.05us 192kb                                                                                    |L0.?|"
    - "**** Simulation run 233, type=split(ReduceOverlap)(split_times=[309]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.505[76,317] 1.05us    |-----------------------------------------L0.505-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[76,309] 1.05us 3mb  |----------------------------------------L0.?-----------------------------------------|   "
    - "L0.?[310,317] 1.05us 96kb                                                                                       |L0.?|"
    - "**** Simulation run 234, type=split(ReduceOverlap)(split_times=[576]). 1 Input Files, 2mb total:"
    - "L0, all files 2mb                                                                                                  "
    - "L0.507[358,592] 1.05us   |-----------------------------------------L0.507-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 2mb total:"
    - "L0                                                                                                                 "
    - "L0.?[358,576] 1.05us 2mb |--------------------------------------L0.?---------------------------------------|       "
    - "L0.?[577,592] 1.05us 174kb                                                                                    |L0.?|"
    - "**** Simulation run 235, type=split(ReduceOverlap)(split_times=[309]). 1 Input Files, 3mb total:"
    - "L0, all files 3mb                                                                                                  "
    - "L0.509[42,317] 1.05us    |-----------------------------------------L0.509-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 3mb total:"
    - "L0                                                                                                                 "
    - "L0.?[42,309] 1.05us 3mb  |----------------------------------------L0.?-----------------------------------------|   "
    - "L0.?[310,317] 1.05us 87kb                                                                                       |L0.?|"
    - "Committing partition 1:"
    - "  Soft Deleting 30 files: L0.451, L0.453, L0.455, L0.457, L0.459, L0.461, L0.463, L0.465, L0.467, L0.469, L0.471, L0.473, L0.475, L0.477, L0.479, L0.481, L0.483, L0.485, L0.487, L0.489, L0.491, L0.493, L0.495, L0.497, L0.499, L0.501, L0.503, L0.505, L0.507, L0.509"
    - "  Creating 60 files"
    - "**** Simulation run 236, type=split(CompactAndSplitOutput(ManySmallFiles))(split_times=[657]). 196 Input Files, 154mb total:"
    - "L0                                                                                                                 "
    - "L0.347[673,923] 1.03us 3mb                                                            |-------L0.347--------|       "
    - "L0.348[924,950] 1.03us 360kb                                                                                    |L0.348|"
    - "L0.518[50,309] 1.03us 4mb|--------L0.518--------|                                                                  "
    - "L0.519[310,317] 1.03us 141kb                         |L0.519|                                                         "
    - "L0.454[318,325] 1.03us 141kb                          |L0.454|                                                        "
    - "L0.385[326,348] 1.03us 407kb                           |L0.385|                                                       "
    - "L0.290[349,356] 1.03us 141kb                             |L0.290|                                                     "
    - "L0.220[357,357] 1.03us 0b                              |L0.220|                                                    "
    - "L0.516[358,576] 1.03us 4mb                              |------L0.516------|                                        "
    - "L0.517[577,592] 1.03us 285kb                                                   |L0.517|                               "
    - "L0.452[593,608] 1.03us 285kb                                                    |L0.452|                              "
    - "L0.383[609,629] 1.03us 374kb                                                      |L0.383|                            "
    - "L0.522[76,309] 1.04us 3mb   |-------L0.522-------|                                                                 "
    - "L0.523[310,317] 1.04us 96kb                         |L0.523|                                                         "
    - "L0.458[318,325] 1.04us 96kb                          |L0.458|                                                        "
    - "L0.405[326,348] 1.04us 275kb                           |L0.405|                                                       "
    - "L0.294[349,356] 1.04us 96kb                             |L0.294|                                                     "
    - "L0.224[357,357] 1.04us 0b                              |L0.224|                                                    "
    - "L0.520[358,576] 1.04us 3mb                              |------L0.520------|                                        "
    - "L0.521[577,592] 1.04us 192kb                                                   |L0.521|                               "
    - "L0.456[593,608] 1.04us 192kb                                                    |L0.456|                              "
    - "L0.387[609,654] 1.04us 552kb                                                      |L0.387|                            "
    - "L0.292[655,670] 1.04us 192kb                                                          |L0.292|                        "
    - "L0.222[671,672] 1.04us 12kb                                                           |L0.222|                       "
    - "L0.349[673,923] 1.04us 3mb                                                            |-------L0.349--------|       "
    - "L0.350[924,932] 1.04us 109kb                                                                                    |L0.350|"
    - "L0.526[42,309] 1.04us 3mb|--------L0.526---------|                                                                 "
    - "L0.527[310,317] 1.04us 87kb                         |L0.527|                                                         "
    - "L0.462[318,325] 1.04us 87kb                          |L0.462|                                                        "
    - "L0.389[326,348] 1.04us 249kb                           |L0.389|                                                       "
    - "L0.298[349,356] 1.04us 87kb                             |L0.298|                                                     "
    - "L0.228[357,357] 1.04us 0b                              |L0.228|                                                    "
    - "L0.524[358,576] 1.04us 2mb                              |------L0.524------|                                        "
    - "L0.525[577,592] 1.04us 174kb                                                   |L0.525|                               "
    - "L0.460[593,608] 1.04us 174kb                                                    |L0.460|                              "
    - "L0.407[609,654] 1.04us 501kb                                                      |L0.407|                            "
    - "L0.296[655,670] 1.04us 174kb                                                          |L0.296|                        "
    - "L0.226[671,672] 1.04us 11kb                                                           |L0.226|                       "
    - "L0.351[673,923] 1.04us 3mb                                                            |-------L0.351--------|       "
    - "L0.352[924,986] 1.04us 690kb                                                                                    |L0.352|"
    - "L0.530[173,309] 1.04us 2mb            |--L0.530--|                                                                  "
    - "L0.531[310,317] 1.04us 105kb                         |L0.531|                                                         "
    - "L0.466[318,325] 1.04us 105kb                          |L0.466|                                                        "
    - "L0.393[326,348] 1.04us 303kb                           |L0.393|                                                       "
    - "L0.302[349,356] 1.04us 105kb                             |L0.302|                                                     "
    - "L0.232[357,357] 1.04us 0b                              |L0.232|                                                    "
    - "L0.528[358,576] 1.04us 3mb                              |------L0.528------|                                        "
    - "L0.529[577,592] 1.04us 212kb                                                   |L0.529|                               "
    - "L0.464[593,608] 1.04us 212kb                                                    |L0.464|                              "
    - "L0.391[609,654] 1.04us 608kb                                                      |L0.391|                            "
    - "L0.300[655,670] 1.04us 212kb                                                          |L0.300|                        "
    - "L0.230[671,672] 1.04us 13kb                                                           |L0.230|                       "
    - "L0.353[673,923] 1.04us 3mb                                                            |-------L0.353--------|       "
    - "L0.354[924,950] 1.04us 360kb                                                                                    |L0.354|"
    - "L0.534[50,309] 1.04us 4mb|--------L0.534--------|                                                                  "
    - "L0.535[310,317] 1.04us 141kb                         |L0.535|                                                         "
    - "L0.470[318,325] 1.04us 141kb                          |L0.470|                                                        "
    - "L0.397[326,348] 1.04us 407kb                           |L0.397|                                                       "
    - "L0.304[349,356] 1.04us 141kb                             |L0.304|                                                     "
    - "L0.234[357,357] 1.04us 0b                              |L0.234|                                                    "
    - "L0.532[358,576] 1.04us 4mb                              |------L0.532------|                                        "
    - "L0.533[577,592] 1.04us 285kb                                                   |L0.533|                               "
    - "L0.468[593,608] 1.04us 285kb                                                    |L0.468|                              "
    - "L0.395[609,629] 1.04us 374kb                                                      |L0.395|                            "
    - "L0.538[76,309] 1.04us 3mb   |-------L0.538-------|                                                                 "
    - "L0.539[310,317] 1.04us 96kb                         |L0.539|                                                         "
    - "L0.474[318,325] 1.04us 96kb                          |L0.474|                                                        "
    - "L0.401[326,348] 1.04us 275kb                           |L0.401|                                                       "
    - "L0.308[349,356] 1.04us 96kb                             |L0.308|                                                     "
    - "L0.238[357,357] 1.04us 0b                              |L0.238|                                                    "
    - "L0.536[358,576] 1.04us 3mb                              |------L0.536------|                                        "
    - "L0.537[577,592] 1.04us 192kb                                                   |L0.537|                               "
    - "L0.472[593,608] 1.04us 192kb                                                    |L0.472|                              "
    - "L0.399[609,654] 1.04us 552kb                                                      |L0.399|                            "
    - "L0.306[655,670] 1.04us 192kb                                                          |L0.306|                        "
    - "L0.236[671,672] 1.04us 12kb                                                           |L0.236|                       "
    - "L0.355[673,923] 1.04us 3mb                                                            |-------L0.355--------|       "
    - "L0.356[924,932] 1.04us 109kb                                                                                    |L0.356|"
    - "L0.542[42,309] 1.04us 3mb|--------L0.542---------|                                                                 "
    - "L0.543[310,317] 1.04us 87kb                         |L0.543|                                                         "
    - "L0.478[318,325] 1.04us 87kb                          |L0.478|                                                        "
    - "L0.409[326,348] 1.04us 249kb                           |L0.409|                                                       "
    - "L0.312[349,356] 1.04us 87kb                             |L0.312|                                                     "
    - "L0.242[357,357] 1.04us 0b                              |L0.242|                                                    "
    - "L0.540[358,576] 1.04us 2mb                              |------L0.540------|                                        "
    - "L0.541[577,592] 1.04us 174kb                                                   |L0.541|                               "
    - "L0.476[593,608] 1.04us 174kb                                                    |L0.476|                              "
    - "L0.403[609,654] 1.04us 501kb                                                      |L0.403|                            "
    - "L0.310[655,670] 1.04us 174kb                                                          |L0.310|                        "
    - "L0.240[671,672] 1.04us 11kb                                                           |L0.240|                       "
    - "L0.357[673,923] 1.04us 3mb                                                            |-------L0.357--------|       "
    - "L0.358[924,986] 1.04us 690kb                                                                                    |L0.358|"
    - "L0.546[173,309] 1.04us 2mb            |--L0.546--|                                                                  "
    - "L0.547[310,317] 1.04us 105kb                         |L0.547|                                                         "
    - "L0.482[318,325] 1.04us 105kb                          |L0.482|                                                        "
    - "L0.413[326,348] 1.04us 303kb                           |L0.413|                                                       "
    - "L0.316[349,356] 1.04us 105kb                             |L0.316|                                                     "
    - "L0.246[357,357] 1.04us 0b                              |L0.246|                                                    "
    - "L0.544[358,576] 1.04us 3mb                              |------L0.544------|                                        "
    - "L0.545[577,592] 1.04us 212kb                                                   |L0.545|                               "
    - "L0.480[593,608] 1.04us 212kb                                                    |L0.480|                              "
    - "L0.411[609,654] 1.04us 608kb                                                      |L0.411|                            "
    - "L0.314[655,670] 1.04us 212kb                                                          |L0.314|                        "
    - "L0.244[671,672] 1.04us 13kb                                                           |L0.244|                       "
    - "L0.359[673,923] 1.04us 3mb                                                            |-------L0.359--------|       "
    - "L0.360[924,950] 1.04us 360kb                                                                                    |L0.360|"
    - "L0.550[50,309] 1.04us 4mb|--------L0.550--------|                                                                  "
    - "L0.551[310,317] 1.04us 141kb                         |L0.551|                                                         "
    - "L0.486[318,325] 1.04us 141kb                          |L0.486|                                                        "
    - "L0.417[326,348] 1.04us 407kb                           |L0.417|                                                       "
    - "L0.318[349,356] 1.04us 141kb                             |L0.318|                                                     "
    - "L0.248[357,357] 1.04us 0b                              |L0.248|                                                    "
    - "L0.548[358,576] 1.04us 4mb                              |------L0.548------|                                        "
    - "L0.549[577,592] 1.04us 285kb                                                   |L0.549|                               "
    - "L0.484[593,608] 1.04us 285kb                                                    |L0.484|                              "
    - "L0.415[609,629] 1.04us 374kb                                                      |L0.415|                            "
    - "L0.554[76,309] 1.04us 3mb   |-------L0.554-------|                                                                 "
    - "L0.555[310,317] 1.04us 96kb                         |L0.555|                                                         "
    - "L0.490[318,325] 1.04us 96kb                          |L0.490|                                                        "
    - "L0.421[326,348] 1.04us 275kb                           |L0.421|                                                       "
    - "L0.322[349,356] 1.04us 96kb                             |L0.322|                                                     "
    - "L0.252[357,357] 1.04us 0b                              |L0.252|                                                    "
    - "L0.552[358,576] 1.04us 3mb                              |------L0.552------|                                        "
    - "L0.553[577,592] 1.04us 192kb                                                   |L0.553|                               "
    - "L0.488[593,608] 1.04us 192kb                                                    |L0.488|                              "
    - "L0.419[609,654] 1.04us 552kb                                                      |L0.419|                            "
    - "L0.320[655,670] 1.04us 192kb                                                          |L0.320|                        "
    - "L0.250[671,672] 1.04us 12kb                                                           |L0.250|                       "
    - "L0.361[673,923] 1.04us 3mb                                                            |-------L0.361--------|       "
    - "L0.362[924,932] 1.04us 109kb                                                                                    |L0.362|"
    - "L0.558[42,309] 1.05us 3mb|--------L0.558---------|                                                                 "
    - "L0.559[310,317] 1.05us 87kb                         |L0.559|                                                         "
    - "L0.494[318,325] 1.05us 87kb                          |L0.494|                                                        "
    - "L0.425[326,348] 1.05us 249kb                           |L0.425|                                                       "
    - "L0.326[349,356] 1.05us 87kb                             |L0.326|                                                     "
    - "L0.256[357,357] 1.05us 0b                              |L0.256|                                                    "
    - "L0.556[358,576] 1.05us 2mb                              |------L0.556------|                                        "
    - "L0.557[577,592] 1.05us 174kb                                                   |L0.557|                               "
    - "L0.492[593,608] 1.05us 174kb                                                    |L0.492|                              "
    - "L0.423[609,654] 1.05us 501kb                                                      |L0.423|                            "
    - "L0.324[655,670] 1.05us 174kb                                                          |L0.324|                        "
    - "L0.254[671,672] 1.05us 11kb                                                           |L0.254|                       "
    - "L0.363[673,923] 1.05us 3mb                                                            |-------L0.363--------|       "
    - "L0.364[924,986] 1.05us 690kb                                                                                    |L0.364|"
    - "L0.562[173,309] 1.05us 2mb            |--L0.562--|                                                                  "
    - "L0.563[310,317] 1.05us 105kb                         |L0.563|                                                         "
    - "L0.498[318,325] 1.05us 105kb                          |L0.498|                                                        "
    - "L0.429[326,348] 1.05us 303kb                           |L0.429|                                                       "
    - "L0.330[349,356] 1.05us 105kb                             |L0.330|                                                     "
    - "L0.260[357,357] 1.05us 0b                              |L0.260|                                                    "
    - "L0.560[358,576] 1.05us 3mb                              |------L0.560------|                                        "
    - "L0.561[577,592] 1.05us 212kb                                                   |L0.561|                               "
    - "L0.496[593,608] 1.05us 212kb                                                    |L0.496|                              "
    - "L0.427[609,654] 1.05us 608kb                                                      |L0.427|                            "
    - "L0.328[655,670] 1.05us 212kb                                                          |L0.328|                        "
    - "L0.258[671,672] 1.05us 13kb                                                           |L0.258|                       "
    - "L0.365[673,923] 1.05us 3mb                                                            |-------L0.365--------|       "
    - "L0.366[924,950] 1.05us 360kb                                                                                    |L0.366|"
    - "L0.566[50,309] 1.05us 4mb|--------L0.566--------|                                                                  "
    - "L0.567[310,317] 1.05us 141kb                         |L0.567|                                                         "
    - "L0.502[318,325] 1.05us 141kb                          |L0.502|                                                        "
    - "L0.433[326,348] 1.05us 407kb                           |L0.433|                                                       "
    - "L0.332[349,356] 1.05us 141kb                             |L0.332|                                                     "
    - "L0.262[357,357] 1.05us 0b                              |L0.262|                                                    "
    - "L0.564[358,576] 1.05us 4mb                              |------L0.564------|                                        "
    - "L0.565[577,592] 1.05us 285kb                                                   |L0.565|                               "
    - "L0.500[593,608] 1.05us 285kb                                                    |L0.500|                              "
    - "L0.431[609,629] 1.05us 374kb                                                      |L0.431|                            "
    - "L0.570[76,309] 1.05us 3mb   |-------L0.570-------|                                                                 "
    - "L0.571[310,317] 1.05us 96kb                         |L0.571|                                                         "
    - "L0.506[318,325] 1.05us 96kb                          |L0.506|                                                        "
    - "L0.437[326,348] 1.05us 275kb                           |L0.437|                                                       "
    - "L0.336[349,356] 1.05us 96kb                             |L0.336|                                                     "
    - "L0.266[357,357] 1.05us 0b                              |L0.266|                                                    "
    - "L0.568[358,576] 1.05us 3mb                              |------L0.568------|                                        "
    - "L0.569[577,592] 1.05us 192kb                                                   |L0.569|                               "
    - "L0.504[593,608] 1.05us 192kb                                                    |L0.504|                              "
    - "L0.435[609,654] 1.05us 552kb                                                      |L0.435|                            "
    - "L0.334[655,670] 1.05us 192kb                                                          |L0.334|                        "
    - "L0.264[671,672] 1.05us 12kb                                                           |L0.264|                       "
    - "L0.367[673,923] 1.05us 3mb                                                            |-------L0.367--------|       "
    - "L0.368[924,932] 1.05us 109kb                                                                                    |L0.368|"
    - "L0.574[42,309] 1.05us 3mb|--------L0.574---------|                                                                 "
    - "L0.575[310,317] 1.05us 87kb                         |L0.575|                                                         "
    - "L0.510[318,325] 1.05us 87kb                          |L0.510|                                                        "
    - "L0.441[326,348] 1.05us 249kb                           |L0.441|                                                       "
    - "L0.340[349,356] 1.05us 87kb                             |L0.340|                                                     "
    - "L0.270[357,357] 1.05us 0b                              |L0.270|                                                    "
    - "L0.572[358,576] 1.05us 2mb                              |------L0.572------|                                        "
    - "L0.573[577,592] 1.05us 174kb                                                   |L0.573|                               "
    - "L0.508[593,608] 1.05us 174kb                                                    |L0.508|                              "
    - "L0.439[609,654] 1.05us 501kb                                                      |L0.439|                            "
    - "L0.338[655,670] 1.05us 174kb                                                          |L0.338|                        "
    - "L0.268[671,672] 1.05us 11kb                                                           |L0.268|                       "
    - "L0.369[673,923] 1.05us 3mb                                                            |-------L0.369--------|       "
    - "L0.370[924,986] 1.05us 690kb                                                                                    |L0.370|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 154mb total:"
    - "L0                                                                                                                 "
    - "L0.?[42,657] 1.05us 100mb|--------------------------L0.?--------------------------|                                "
    - "L0.?[658,986] 1.05us 54mb                                                          |------------L0.?-------------| "
    - "Committing partition 1:"
    - "  Soft Deleting 196 files: L0.220, L0.222, L0.224, L0.226, L0.228, L0.230, L0.232, L0.234, L0.236, L0.238, L0.240, L0.242, L0.244, L0.246, L0.248, L0.250, L0.252, L0.254, L0.256, L0.258, L0.260, L0.262, L0.264, L0.266, L0.268, L0.270, L0.290, L0.292, L0.294, L0.296, L0.298, L0.300, L0.302, L0.304, L0.306, L0.308, L0.310, L0.312, L0.314, L0.316, L0.318, L0.320, L0.322, L0.324, L0.326, L0.328, L0.330, L0.332, L0.334, L0.336, L0.338, L0.340, L0.347, L0.348, L0.349, L0.350, L0.351, L0.352, L0.353, L0.354, L0.355, L0.356, L0.357, L0.358, L0.359, L0.360, L0.361, L0.362, L0.363, L0.364, L0.365, L0.366, L0.367, L0.368, L0.369, L0.370, L0.383, L0.385, L0.387, L0.389, L0.391, L0.393, L0.395, L0.397, L0.399, L0.401, L0.403, L0.405, L0.407, L0.409, L0.411, L0.413, L0.415, L0.417, L0.419, L0.421, L0.423, L0.425, L0.427, L0.429, L0.431, L0.433, L0.435, L0.437, L0.439, L0.441, L0.452, L0.454, L0.456, L0.458, L0.460, L0.462, L0.464, L0.466, L0.468, L0.470, L0.472, L0.474, L0.476, L0.478, L0.480, L0.482, L0.484, L0.486, L0.488, L0.490, L0.492, L0.494, L0.496, L0.498, L0.500, L0.502, L0.504, L0.506, L0.508, L0.510, L0.516, L0.517, L0.518, L0.519, L0.520, L0.521, L0.522, L0.523, L0.524, L0.525, L0.526, L0.527, L0.528, L0.529, L0.530, L0.531, L0.532, L0.533, L0.534, L0.535, L0.536, L0.537, L0.538, L0.539, L0.540, L0.541, L0.542, L0.543, L0.544, L0.545, L0.546, L0.547, L0.548, L0.549, L0.550, L0.551, L0.552, L0.553, L0.554, L0.555, L0.556, L0.557, L0.558, L0.559, L0.560, L0.561, L0.562, L0.563, L0.564, L0.565, L0.566, L0.567, L0.568, L0.569, L0.570, L0.571, L0.572, L0.573, L0.574, L0.575"
    - "  Creating 2 files"
    - "**** Simulation run 237, type=split(HighL0OverlapSingleFile)(split_times=[252]). 1 Input Files, 100mb total:"
    - "L1, all files 100mb                                                                                                "
    - "L1.513[42,309] 1.03us    |-----------------------------------------L1.513-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 100mb total:"
    - "L1                                                                                                                 "
    - "L1.?[42,252] 1.03us 79mb |--------------------------------L1.?--------------------------------|                    "
    - "L1.?[253,309] 1.03us 21mb                                                                       |------L1.?------| "
    - "**** Simulation run 238, type=split(HighL0OverlapSingleFile)(split_times=[462]). 1 Input Files, 100mb total:"
    - "L1, all files 100mb                                                                                                "
    - "L1.514[310,576] 1.03us   |-----------------------------------------L1.514-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 100mb total:"
    - "L1                                                                                                                 "
    - "L1.?[310,462] 1.03us 57mb|----------------------L1.?-----------------------|                                       "
    - "L1.?[463,576] 1.03us 43mb                                                   |----------------L1.?----------------| "
    - "**** Simulation run 239, type=split(HighL0OverlapSingleFile)(split_times=[252, 462]). 1 Input Files, 100mb total:"
    - "L0, all files 100mb                                                                                                "
    - "L0.576[42,657] 1.05us    |-----------------------------------------L0.576-----------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 100mb total:"
    - "L0                                                                                                                 "
    - "L0.?[42,252] 1.05us 34mb |------------L0.?------------|                                                            "
    - "L0.?[253,462] 1.05us 34mb                              |------------L0.?------------|                              "
    - "L0.?[463,657] 1.05us 32mb                                                             |-----------L0.?-----------| "
    - "Committing partition 1:"
    - "  Soft Deleting 3 files: L1.513, L1.514, L0.576"
    - "  Creating 7 files"
    - "**** Simulation run 240, type=split(ReduceOverlap)(split_times=[672, 923]). 1 Input Files, 54mb total:"
    - "L0, all files 54mb                                                                                                 "
    - "L0.577[658,986] 1.05us   |-----------------------------------------L0.577-----------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 54mb total:"
    - "L0                                                                                                                 "
    - "L0.?[658,672] 1.05us 2mb |L0.?|                                                                                    "
    - "L0.?[673,923] 1.05us 41mb    |-------------------------------L0.?-------------------------------|                  "
    - "L0.?[924,986] 1.05us 10mb                                                                        |-----L0.?------| "
    - "**** Simulation run 241, type=split(ReduceOverlap)(split_times=[576]). 1 Input Files, 32mb total:"
    - "L0, all files 32mb                                                                                                 "
    - "L0.584[463,657] 1.05us   |-----------------------------------------L0.584-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 32mb total:"
    - "L0                                                                                                                 "
    - "L0.?[463,576] 1.05us 19mb|-----------------------L0.?-----------------------|                                      "
    - "L0.?[577,657] 1.05us 13mb                                                    |---------------L0.?----------------| "
    - "**** Simulation run 242, type=split(ReduceOverlap)(split_times=[309]). 1 Input Files, 34mb total:"
    - "L0, all files 34mb                                                                                                 "
    - "L0.583[253,462] 1.05us   |----------------------------------------L0.583-----------------------------------------| "
    - "**** 2 Output Files (parquet_file_id not yet assigned), 34mb total:"
    - "L0                                                                                                                 "
    - "L0.?[253,309] 1.05us 9mb |---------L0.?---------|                                                                  "
    - "L0.?[310,462] 1.05us 25mb                        |-----------------------------L0.?------------------------------| "
    - "Committing partition 1:"
    - "  Soft Deleting 3 files: L0.577, L0.583, L0.584"
    - "  Creating 7 files"
    - "**** Simulation run 243, type=split(CompactAndSplitOutput(FoundSubsetLessThanMaxCompactSize))(split_times=[229, 416]). 8 Input Files, 287mb total:"
    - "L0                                                                                                                 "
    - "L0.582[42,252] 1.05us 34mb|-------------L0.582--------------|                                                       "
    - "L0.590[253,309] 1.05us 9mb                                   |L0.590-|                                              "
    - "L0.591[310,462] 1.05us 25mb                                             |--------L0.591---------|                    "
    - "L0.588[463,576] 1.05us 19mb                                                                      |-----L0.588------| "
    - "L1                                                                                                                 "
    - "L1.578[42,252] 1.03us 79mb|-------------L1.578--------------|                                                       "
    - "L1.579[253,309] 1.03us 21mb                                   |L1.579-|                                              "
    - "L1.580[310,462] 1.03us 57mb                                             |--------L1.580---------|                    "
    - "L1.581[463,576] 1.03us 43mb                                                                      |-----L1.581------| "
    - "**** 3 Output Files (parquet_file_id not yet assigned), 287mb total:"
    - "L1                                                                                                                 "
    - "L1.?[42,229] 1.05us 100mb|------------L1.?-------------|                                                           "
    - "L1.?[230,416] 1.05us 100mb                               |------------L1.?-------------|                            "
    - "L1.?[417,576] 1.05us 86mb                                                               |----------L1.?----------| "
    - "Committing partition 1:"
    - "  Soft Deleting 8 files: L1.578, L1.579, L1.580, L1.581, L0.582, L0.588, L0.590, L0.591"
    - "  Creating 3 files"
    - "**** Simulation run 244, type=split(CompactAndSplitOutput(TotalSizeLessThanMaxCompactSize))(split_times=[769, 961]). 7 Input Files, 213mb total:"
    - "L0                                                                                                                 "
    - "L0.587[924,986] 1.05us 10mb                                                                            |--L0.587---| "
    - "L0.586[673,923] 1.05us 41mb                     |-----------------------L0.586------------------------|              "
    - "L0.585[658,672] 1.05us 2mb                 |L0.585|                                                                 "
    - "L0.589[577,657] 1.05us 13mb|----L0.589-----|                                                                         "
    - "L1                                                                                                                 "
    - "L1.515[577,672] 1.03us 36mb|------L1.515------|                                                                      "
    - "L1.512[924,986] 1.03us 22mb                                                                            |--L1.512---| "
    - "L1.511[673,923] 1.03us 88mb                     |-----------------------L1.511------------------------|              "
    - "**** 3 Output Files (parquet_file_id not yet assigned), 213mb total:"
    - "L1                                                                                                                 "
    - "L1.?[577,769] 1.05us 100mb|------------------L1.?------------------|                                                "
    - "L1.?[770,961] 1.05us 100mb                                          |------------------L1.?------------------|      "
    - "L1.?[962,986] 1.05us 14mb                                                                                    |L1.?|"
    - "Committing partition 1:"
    - "  Soft Deleting 7 files: L1.511, L1.512, L1.515, L0.585, L0.586, L0.587, L0.589"
    - "  Creating 3 files"
    - "**** Simulation run 245, type=split(ReduceOverlap)(split_times=[499]). 1 Input Files, 86mb total:"
    - "L1, all files 86mb                                                                                                 "
    - "L1.594[417,576] 1.05us   |-----------------------------------------L1.594-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 86mb total:"
    - "L1                                                                                                                 "
    - "L1.?[417,499] 1.05us 45mb|--------------------L1.?--------------------|                                            "
    - "L1.?[500,576] 1.05us 42mb                                              |------------------L1.?-------------------| "
    - "**** Simulation run 246, type=split(ReduceOverlap)(split_times=[299, 399]). 1 Input Files, 100mb total:"
    - "L1, all files 100mb                                                                                                "
    - "L1.593[230,416] 1.05us   |----------------------------------------L1.593-----------------------------------------| "
    - "**** 3 Output Files (parquet_file_id not yet assigned), 100mb total:"
    - "L1                                                                                                                 "
    - "L1.?[230,299] 1.05us 37mb|-------------L1.?--------------|                                                         "
    - "L1.?[300,399] 1.05us 53mb                                 |--------------------L1.?---------------------|          "
    - "L1.?[400,416] 1.05us 10mb                                                                                  |L1.?-| "
    - "**** Simulation run 247, type=split(ReduceOverlap)(split_times=[99, 199]). 1 Input Files, 100mb total:"
    - "L1, all files 100mb                                                                                                "
    - "L1.592[42,229] 1.05us    |----------------------------------------L1.592-----------------------------------------| "
    - "**** 3 Output Files (parquet_file_id not yet assigned), 100mb total:"
    - "L1                                                                                                                 "
    - "L1.?[42,99] 1.05us 31mb  |----------L1.?-----------|                                                               "
    - "L1.?[100,199] 1.05us 53mb                           |--------------------L1.?---------------------|                "
    - "L1.?[200,229] 1.05us 17mb                                                                            |---L1.?----| "
    - "**** Simulation run 248, type=split(ReduceOverlap)(split_times=[799, 899]). 1 Input Files, 100mb total:"
    - "L1, all files 100mb                                                                                                "
    - "L1.596[770,961] 1.05us   |-----------------------------------------L1.596-----------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 100mb total:"
    - "L1                                                                                                                 "
    - "L1.?[770,799] 1.05us 15mb|---L1.?----|                                                                             "
    - "L1.?[800,899] 1.05us 52mb              |--------------------L1.?--------------------|                              "
    - "L1.?[900,961] 1.05us 33mb                                                             |-----------L1.?-----------| "
    - "**** Simulation run 249, type=split(ReduceOverlap)(split_times=[599, 699]). 1 Input Files, 100mb total:"
    - "L1, all files 100mb                                                                                                "
    - "L1.595[577,769] 1.05us   |-----------------------------------------L1.595-----------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 100mb total:"
    - "L1                                                                                                                 "
    - "L1.?[577,599] 1.05us 11mb|--L1.?--|                                                                                "
    - "L1.?[600,699] 1.05us 52mb          |--------------------L1.?--------------------|                                  "
    - "L1.?[700,769] 1.05us 37mb                                                         |-------------L1.?-------------| "
    - "Committing partition 1:"
    - "  Soft Deleting 5 files: L1.592, L1.593, L1.594, L1.595, L1.596"
    - "  Creating 14 files"
    - "**** Simulation run 250, type=split(CompactAndSplitOutput(FoundSubsetLessThanMaxCompactSize))(split_times=[71, 142]). 4 Input Files, 284mb total:"
    - "L1                                                                                                                 "
    - "L1.603[42,99] 1.05us 31mb                  |--------L1.603---------|                                               "
    - "L1.604[100,199] 1.05us 53mb                                             |------------------L1.604------------------| "
    - "L2                                                                                                                 "
    - "L2.1[0,99] 99ns 100mb    |-------------------L2.1-------------------|                                              "
    - "L2.2[100,199] 199ns 100mb                                             |-------------------L2.2-------------------| "
    - "**** 3 Output Files (parquet_file_id not yet assigned), 284mb total:"
    - "L2                                                                                                                 "
    - "L2.?[0,71] 1.05us 101mb  |-------------L2.?-------------|                                                          "
    - "L2.?[72,142] 1.05us 100mb                                |------------L2.?-------------|                           "
    - "L2.?[143,199] 1.05us 83mb                                                                |---------L2.?----------| "
    - "Committing partition 1:"
    - "  Soft Deleting 4 files: L2.1, L2.2, L1.603, L1.604"
    - "  Creating 3 files"
    - "**** Simulation run 251, type=split(CompactAndSplitOutput(FoundSubsetLessThanMaxCompactSize))(split_times=[265]). 3 Input Files, 154mb total:"
    - "L1                                                                                                                 "
    - "L1.605[200,229] 1.05us 17mb|---------L1.605---------|                                                                "
    - "L1.600[230,299] 1.05us 37mb                           |---------------------------L1.600---------------------------| "
    - "L2                                                                                                                 "
    - "L2.3[200,299] 299ns 100mb|-----------------------------------------L2.3------------------------------------------| "
    - "**** 2 Output Files (parquet_file_id not yet assigned), 154mb total:"
    - "L2                                                                                                                 "
    - "L2.?[200,265] 1.05us 101mb|--------------------------L2.?---------------------------|                               "
    - "L2.?[266,299] 1.05us 53mb                                                           |-----------L2.?------------|  "
    - "Committing partition 1:"
    - "  Soft Deleting 3 files: L2.3, L1.600, L1.605"
    - "  Creating 2 files"
    - "**** Simulation run 252, type=split(CompactAndSplitOutput(FoundSubsetLessThanMaxCompactSize))(split_times=[376, 452]). 4 Input Files, 263mb total:"
    - "L1                                                                                                                 "
    - "L1.601[300,399] 1.05us 53mb|------------------L1.601------------------|                                              "
    - "L1.602[400,416] 1.05us 10mb                                             |L1.602|                                     "
    - "L2                                                                                                                 "
    - "L2.4[300,399] 399ns 100mb|-------------------L2.4-------------------|                                              "
    - "L2.5[400,499] 499ns 100mb                                             |-------------------L2.5-------------------| "
    - "**** 3 Output Files (parquet_file_id not yet assigned), 263mb total:"
    - "L2                                                                                                                 "
    - "L2.?[300,376] 1.05us 100mb|--------------L2.?--------------|                                                        "
    - "L2.?[377,452] 1.05us 99mb                                  |-------------L2.?--------------|                       "
    - "L2.?[453,499] 1.05us 63mb                                                                     |-------L2.?-------| "
    - "Committing partition 1:"
    - "  Soft Deleting 4 files: L2.4, L2.5, L1.601, L1.602"
    - "  Creating 3 files"
    - "**** Simulation run 253, type=split(ReduceOverlap)(split_times=[452]). 1 Input Files, 45mb total:"
    - "L1, all files 45mb                                                                                                 "
    - "L1.598[417,499] 1.05us   |-----------------------------------------L1.598-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 45mb total:"
    - "L1                                                                                                                 "
    - "L1.?[417,452] 1.05us 19mb|----------------L1.?----------------|                                                    "
    - "L1.?[453,499] 1.05us 26mb                                       |----------------------L1.?----------------------| "
    - "Committing partition 1:"
    - "  Soft Deleting 1 files: L1.598"
    - "  Creating 2 files"
    - "**** Simulation run 254, type=split(CompactAndSplitOutput(FoundSubsetLessThanMaxCompactSize))(split_times=[436, 495]). 4 Input Files, 207mb total:"
    - "L1                                                                                                                 "
    - "L1.620[417,452] 1.05us 19mb                             |--------L1.620---------|                                    "
    - "L1.621[453,499] 1.05us 26mb                                                        |------------L1.621-------------| "
    - "L2                                                                                                                 "
    - "L2.618[377,452] 1.05us 99mb|-----------------------L2.618------------------------|                                   "
    - "L2.619[453,499] 1.05us 63mb                                                        |------------L2.619-------------| "
    - "**** 3 Output Files (parquet_file_id not yet assigned), 207mb total:"
    - "L2                                                                                                                 "
    - "L2.?[377,436] 1.05us 100mb|------------------L2.?-------------------|                                               "
    - "L2.?[437,495] 1.05us 98mb                                            |------------------L2.?------------------|    "
    - "L2.?[496,499] 1.05us 8mb                                                                                        |L2.?|"
    - "Committing partition 1:"
    - "  Soft Deleting 4 files: L2.618, L2.619, L1.620, L1.621"
    - "  Creating 3 files"
    - "**** Simulation run 255, type=split(CompactAndSplitOutput(FoundSubsetLessThanMaxCompactSize))(split_times=[565]). 3 Input Files, 153mb total:"
    - "L1                                                                                                                 "
    - "L1.599[500,576] 1.05us 42mb|------------------------------L1.599-------------------------------|                     "
    - "L1.609[577,599] 1.05us 11mb                                                                      |------L1.609------|"
    - "L2                                                                                                                 "
    - "L2.6[500,599] 599ns 100mb|-----------------------------------------L2.6------------------------------------------| "
    - "**** 2 Output Files (parquet_file_id not yet assigned), 153mb total:"
    - "L2                                                                                                                 "
    - "L2.?[500,565] 1.05us 101mb|--------------------------L2.?---------------------------|                               "
    - "L2.?[566,599] 1.05us 53mb                                                           |-----------L2.?------------|  "
    - "Committing partition 1:"
    - "  Soft Deleting 3 files: L2.6, L1.599, L1.609"
    - "  Creating 2 files"
    - "**** Simulation run 256, type=split(CompactAndSplitOutput(FoundSubsetLessThanMaxCompactSize))(split_times=[669, 738]). 4 Input Files, 289mb total:"
    - "L1                                                                                                                 "
    - "L1.610[600,699] 1.05us 52mb|------------------L1.610------------------|                                              "
    - "L1.611[700,769] 1.05us 37mb                                             |-----------L1.611------------|              "
    - "L2                                                                                                                 "
    - "L2.7[600,699] 699ns 100mb|-------------------L2.7-------------------|                                              "
    - "L2.8[700,799] 799ns 100mb                                             |-------------------L2.8-------------------| "
    - "**** 3 Output Files (parquet_file_id not yet assigned), 289mb total:"
    - "L2                                                                                                                 "
    - "L2.?[600,669] 1.05us 100mb|------------L2.?-------------|                                                           "
    - "L2.?[670,738] 1.05us 99mb                               |------------L2.?------------|                             "
    - "L2.?[739,799] 1.05us 90mb                                                              |----------L2.?-----------| "
    - "Committing partition 1:"
    - "  Soft Deleting 4 files: L2.7, L2.8, L1.610, L1.611"
    - "  Creating 3 files"
    - "**** Simulation run 257, type=split(CompactAndSplitOutput(FoundSubsetLessThanMaxCompactSize))(split_times=[802, 865]). 4 Input Files, 257mb total:"
    - "L1                                                                                                                 "
    - "L1.606[770,799] 1.05us 15mb                 |----L1.606----|                                                         "
    - "L1.607[800,899] 1.05us 52mb                                  |-----------------------L1.607------------------------| "
    - "L2                                                                                                                 "
    - "L2.629[739,799] 1.05us 90mb|------------L2.629-------------|                                                         "
    - "L2.9[800,899] 899ns 100mb                                  |------------------------L2.9-------------------------| "
    - "**** 3 Output Files (parquet_file_id not yet assigned), 257mb total:"
    - "L2                                                                                                                 "
    - "L2.?[739,802] 1.05us 101mb|--------------L2.?---------------|                                                       "
    - "L2.?[803,865] 1.05us 99mb                                    |--------------L2.?--------------|                    "
    - "L2.?[866,899] 1.05us 56mb                                                                       |------L2.?------| "
    - "Committing partition 1:"
    - "  Soft Deleting 4 files: L2.9, L1.606, L1.607, L2.629"
    - "  Creating 3 files"
    - "**** Final Output Files (5.7gb written)"
    - "L1                                                                                                                 "
    - "L1.597[962,986] 1.05us 14mb                                                                                      |L1.597|"
    - "L1.608[900,961] 1.05us 33mb                                                                                 |L1.608| "
    - "L2                                                                                                                 "
    - "L2.10[900,999] 999ns 100mb                                                                                 |L2.10-| "
    - "L2.612[0,71] 1.05us 101mb|L2.612|                                                                                  "
    - "L2.613[72,142] 1.05us 100mb      |L2.613|                                                                            "
    - "L2.614[143,199] 1.05us 83mb            |L2.614|                                                                      "
    - "L2.615[200,265] 1.05us 101mb                  |L2.615|                                                                "
    - "L2.616[266,299] 1.05us 53mb                       |L2.616|                                                           "
    - "L2.617[300,376] 1.05us 100mb                           |L2.617|                                                       "
    - "L2.622[377,436] 1.05us 100mb                                 |L2.622|                                                 "
    - "L2.623[437,495] 1.05us 98mb                                       |L2.623|                                           "
    - "L2.624[496,499] 1.05us 8mb                                            |L2.624|                                      "
    - "L2.625[500,565] 1.05us 101mb                                             |L2.625|                                     "
    - "L2.626[566,599] 1.05us 53mb                                                  |L2.626|                                "
    - "L2.627[600,669] 1.05us 100mb                                                      |L2.627|                            "
    - "L2.628[670,738] 1.05us 99mb                                                            |L2.628|                      "
    - "L2.630[739,802] 1.05us 101mb                                                                  |L2.630|                "
    - "L2.631[803,865] 1.05us 99mb                                                                        |L2.631|          "
    - "L2.632[866,899] 1.05us 56mb                                                                              |L2.632|    "
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
    - "L0.1[1,2] 342ns 468kb    |L0.1|                                                                                    "
    - "L0.2[3,4] 342ns 464kb    |L0.2|                                                                                    "
    - "L0.3[5,7] 342ns 444kb     |L0.3|                                                                                   "
    - "L0.4[6,49] 127ns 271kb    |--L0.4---|                                                                              "
    - "L0.5[8,25] 342ns 462kb    |L0.5|                                                                                   "
    - "L0.6[9,54] 141ns 91kb      |--L0.6---|                                                                             "
    - "L0.7[10,20] 153ns 89kb     |L0.7|                                                                                  "
    - "L0.8[11,30] 172ns 66kb     |L0.8|                                                                                  "
    - "L0.9[12,14] 191ns 87kb     |L0.9|                                                                                  "
    - "L0.10[13,47] 212ns 69kb     |-L0.10-|                                                                              "
    - "L0.11[15,51] 224ns 76kb     |-L0.11-|                                                                              "
    - "L0.12[16,55] 239ns 79kb     |-L0.12--|                                                                             "
    - "L0.13[17,56] 258ns 88kb      |-L0.13--|                                                                            "
    - "L0.14[18,65] 273ns 162kb     |--L0.14---|                                                                          "
    - "L0.15[19,68] 296ns 133kb     |---L0.15---|                                                                         "
    - "L0.16[21,23] 306ns 72kb       |L0.16|                                                                              "
    - "L0.17[22,24] 311ns 41kb       |L0.17|                                                                              "
    - "L0.18[26,29] 311ns 42kb        |L0.18|                                                                             "
    - "L0.19[27,52] 342ns 442kb       |L0.19|                                                                             "
    - "L0.20[28,32] 316ns 59kb         |L0.20|                                                                            "
    - "L0.21[31,34] 319ns 59kb         |L0.21|                                                                            "
    - "L0.22[33,36] 323ns 68kb          |L0.22|                                                                           "
    - "L0.23[35,38] 325ns 58kb           |L0.23|                                                                          "
    - "L0.24[37,40] 328ns 66kb           |L0.24|                                                                          "
    - "L0.25[39,42] 335ns 52kb            |L0.25|                                                                         "
    - "L0.26[41,44] 336ns 65kb            |L0.26|                                                                         "
    - "L0.27[43,46] 340ns 95kb             |L0.27|                                                                        "
    - "L0.28[45,48] 341ns 159kb            |L0.28|                                                                        "
    - "L0.29[50,71] 127ns 724kb              |L0.29|                                                                      "
    - "L0.30[53,62] 342ns 442kb              |L0.30|                                                                      "
    - "L0.31[57,61] 341ns 114kb               |L0.31|                                                                     "
    - "L0.32[58,70] 141ns 458kb                |L0.32|                                                                    "
    - "L0.33[59,93] 153ns 113mb                |-L0.33-|                                                                  "
    - "L0.34[60,102] 172ns 21mb                |--L0.34--|                                                                "
    - "L0.35[63,75] 342ns 582kb                 |L0.35|                                                                   "
    - "L0.36[64,74] 341ns 184kb                 |L0.36|                                                                   "
    - "L0.37[66,67] 212ns 231kb                  |L0.37|                                                                  "
    - "L0.38[69,73] 306ns 7kb                     |L0.38|                                                                 "
    - "L0.39[72,117] 127ns 259mb                  |--L0.39--|                                                             "
    - "L0.40[76,80] 342ns 290kb                    |L0.40|                                                                "
    - "L0.41[77,77] 311ns 6kb                       |L0.41|                                                               "
    - "L0.42[78,79] 316ns 7kb                       |L0.42|                                                               "
    - "L0.43[81,133] 141ns 249mb                     |---L0.43---|                                                        "
    - "L0.44[82,83] 342ns 106kb                      |L0.44|                                                              "
    - "L0.45[84,89] 342ns 134kb                       |L0.45|                                                             "
    - "L0.46[85,86] 319ns 6kb                         |L0.46|                                                             "
    - "L0.47[87,88] 341ns 123kb                       |L0.47|                                                             "
    - "L0.48[90,97] 342ns 155kb                        |L0.48|                                                            "
    - "L0.49[91,92] 341ns 105kb                        |L0.49|                                                            "
    - "L0.50[94,143] 153ns 115mb                        |---L0.50---|                                                     "
    - "L0.51[95,96] 224ns 192kb                          |L0.51|                                                          "
    - "L0.52[98,111] 342ns 108kb                         |L0.52|                                                          "
    - "L0.53[99,109] 212ns 91kb                           |L0.53|                                                         "
    - "L0.54[100,104] 325ns 6kb                           |L0.54|                                                         "
    - "L0.55[101,106] 191ns 66kb                          |L0.55|                                                         "
    - "L0.56[103,173] 172ns 218mb                           |-----L0.56------|                                             "
    - "L0.57[105,112] 224ns 76kb                           |L0.57|                                                        "
    - "L0.58[107,108] 239ns 8kb                             |L0.58|                                                       "
    - "L0.59[110,110] 328ns 6kb                              |L0.59|                                                      "
    - "L0.60[113,116] 224ns 48kb                             |L0.60|                                                      "
    - "L0.61[114,124] 172ns 76mb                              |L0.61|                                                     "
    - "L0.62[115,123] 342ns 107kb                              |L0.62|                                                     "
    - "L0.63[116,119] 239ns 93kb                              |L0.63|                                                     "
    - "L0.64[118,120] 258ns 92kb                               |L0.64|                                                    "
    - "L0.65[121,122] 273ns 7kb                                |L0.65|                                                    "
    - "L0.66[125,137] 172ns 96mb                                 |L0.66|                                                  "
    - "L0.67[126,136] 342ns 100kb                                 |L0.67|                                                  "
    - "L0.68[128,142] 273ns 116kb                                 |L0.68|                                                  "
    - "L0.69[129,130] 258ns 9kb                                   |L0.69|                                                 "
    - "L0.70[131,132] 341ns 25kb                                  |L0.70|                                                 "
    - "L0.71[134,145] 179ns 33mb                                   |L0.71|                                                "
    - "L0.72[135,144] 191ns 746kb                                   |L0.72|                                                "
    - "L0.73[138,158] 172ns 68mb                                    |L0.73|                                               "
    - "L0.74[139,159] 342ns 179kb                                    |L0.74|                                               "
    - "L0.75[140,154] 336ns 7kb                                      |L0.75|                                              "
    - "L0.76[146,155] 179ns 62mb                                      |L0.76|                                             "
    - "L0.77[147,160] 191ns 21mb                                      |L0.77|                                             "
    - "L0.78[148,150] 273ns 54kb                                       |L0.78|                                            "
    - "L0.79[149,157] 296ns 73kb                                       |L0.79|                                            "
    - "L0.80[151,152] 224ns 23kb                                       |L0.80|                                            "
    - "L0.81[156,160] 179ns 53mb                                         |L0.81|                                          "
    - "L0.82[161,163] 191ns 1mb                                           |L0.82|                                         "
    - "L0.83[161,171] 179ns 44mb                                          |L0.83|                                         "
    - "L0.84[162,167] 342ns 42kb                                          |L0.84|                                         "
    - "L0.85[164,174] 191ns 95mb                                           |L0.85|                                        "
    - "L0.86[165,170] 296ns 49kb                                           |L0.86|                                        "
    - "L0.87[166,168] 172ns 14mb                                           |L0.87|                                        "
    - "L0.88[169,181] 342ns 168kb                                            |L0.88|                                       "
    - "L0.89[175,180] 191ns 130mb                                              |L0.89|                                     "
    - "L0.90[176,178] 273ns 185kb                                              |L0.90|                                     "
    - "L0.91[177,182] 212ns 46mb                                              |L0.91|                                     "
    - "L0.92[183,197] 212ns 202mb                                                |L0.92|                                   "
    - "L0.93[184,196] 342ns 156kb                                                |L0.93|                                   "
    - "L0.94[185,195] 224ns 14mb                                                |L0.94|                                   "
    - "L0.95[186,187] 273ns 17kb                                                 |L0.95|                                  "
    - "L0.96[188,193] 191ns 51mb                                                 |L0.96|                                  "
    - "L0.97[189,190] 341ns 6kb                                                   |L0.97|                                 "
    - "L0.98[192,194] 296ns 36kb                                                  |L0.98|                                 "
    - "L0.99[198,204] 212ns 46mb                                                    |L0.99|                               "
    - "L0.100[199,206] 342ns 102kb                                                    |L0.100|                              "
    - "L0.101[200,207] 224ns 110mb                                                    |L0.101|                              "
    - "L0.102[201,203] 296ns 27kb                                                     |L0.102|                             "
    - "L0.103[202,205] 341ns 6kb                                                     |L0.103|                             "
    - "L0.104[208,214] 224ns 61mb                                                       |L0.104|                           "
    - "L0.105[209,215] 342ns 71kb                                                       |L0.105|                           "
    - "L0.106[210,211] 239ns 6kb                                                       |L0.106|                           "
    - "L0.107[213,221] 239ns 99mb                                                        |L0.107|                          "
    - "L0.108[216,226] 342ns 156kb                                                         |L0.108|                         "
    - "L0.109[217,220] 340ns 46kb                                                         |L0.109|                         "
    - "L0.110[218,219] 341ns 8kb                                                         |L0.110|                         "
    - "L0.111[222,225] 239ns 111mb                                                          |L0.111|                        "
    - "L0.112[223,228] 258ns 117mb                                                           |L0.112|                       "
    - "L0.113[227,234] 342ns 42kb                                                            |L0.113|                      "
    - "L0.114[229,242] 258ns 126mb                                                            |L0.114|                      "
    - "L0.115[230,231] 340ns 25kb                                                            |L0.115|                      "
    - "L0.116[232,244] 273ns 50mb                                                             |L0.116|                     "
    - "L0.117[233,237] 273ns 79kb                                                             |L0.117|                     "
    - "L0.118[235,241] 342ns 83kb                                                              |L0.118|                    "
    - "L0.119[236,238] 296ns 31kb                                                              |L0.119|                    "
    - "L0.120[240,243] 306ns 31kb                                                               |L0.120|                   "
    - "L0.121[245,255] 273ns 162mb                                                                |L0.121|                  "
    - "L0.122[246,249] 258ns 32kb                                                                 |L0.122|                 "
    - "L0.123[247,250] 306ns 30kb                                                                 |L0.123|                 "
    - "L0.124[248,254] 342ns 114kb                                                                 |L0.124|                 "
    - "L0.125[251,255] 273ns 20mb                                                                  |L0.125|                "
    - "L0.126[252,268] 340ns 46kb                                                                  |L0.126|                "
    - "L0.127[253,267] 341ns 8kb                                                                   |L0.127|               "
    - "L0.128[256,259] 273ns 24mb                                                                   |L0.128|               "
    - "L0.129[256,262] 273ns 26mb                                                                   |L0.129|               "
    - "L0.130[257,263] 342ns 111kb                                                                    |L0.130|              "
    - "L0.131[260,270] 273ns 176mb                                                                    |L0.131|              "
    - "L0.132[261,264] 296ns 10mb                                                                     |L0.132|             "
    - "L0.133[265,270] 296ns 11mb                                                                      |L0.133|            "
    - "L0.134[266,269] 342ns 44kb                                                                      |L0.134|            "
    - "L0.135[271,277] 296ns 97mb                                                                       |L0.135|           "
    - "L0.136[271,275] 273ns 28mb                                                                       |L0.136|           "
    - "L0.137[272,276] 342ns 80kb                                                                        |L0.137|          "
    - "L0.138[274,281] 306ns 29mb                                                                        |L0.138|          "
    - "L0.139[278,283] 296ns 120mb                                                                         |L0.139|         "
    - "L0.140[279,284] 342ns 107kb                                                                          |L0.140|        "
    - "L0.141[280,285] 306ns 13kb                                                                          |L0.141|        "
    - "L0.142[282,286] 306ns 33mb                                                                          |L0.142|        "
    - "L0.143[287,298] 306ns 171mb                                                                            |L0.143|      "
    - "L0.144[288,291] 296ns 27kb                                                                            |L0.144|      "
    - "L0.145[289,299] 306ns 70mb                                                                            |L0.145|      "
    - "L0.146[290,292] 342ns 16kb                                                                            |L0.146|      "
    - "L0.147[293,339] 342ns 94mb                                                                             |--L0.147--| "
    - "L0.148[294,321] 328ns 54kb                                                                              |L0.148|    "
    - "L0.149[295,332] 335ns 190mb                                                                              |L0.149-|   "
    - "L0.150[297,313] 311ns 233mb                                                                              |L0.150|    "
    - "L0.151[300,307] 306ns 168mb                                                                               |L0.151|   "
    - "L0.152[301,302] 306ns 17kb                                                                               |L0.152|   "
    - "L0.153[303,304] 325ns 10kb                                                                                |L0.153|  "
    - "L0.154[305,317] 316ns 219mb                                                                                |L0.154|  "
    - "L0.155[308,309] 319ns 13kb                                                                                 |L0.155| "
    - "L0.156[310,320] 319ns 212mb                                                                                  |L0.156|"
    - "L0.157[312,314] 341ns 25kb                                                                                  |L0.157|"
    - "L0.158[315,324] 323ns 214mb                                                                                   |L0.158|"
    - "L0.159[318,326] 325ns 214mb                                                                                    |L0.159|"
    - "L0.160[322,329] 328ns 213mb                                                                                     |L0.160|"
    - "L0.161[327,333] 336ns 183mb                                                                                      |L0.161|"
    - "L0.162[330,338] 340ns 231mb                                                                                       |L0.162|"
    - "L0.163[331,338] 341ns 232mb                                                                                       |L0.163|"
    - "L0.164[334,337] 336ns 29mb                                                                                        |L0.164|"
    - "WARNING: file L0.39[72,117] 127ns 259mb exceeds soft limit 100mb by more than 50%"
    - "WARNING: file L0.43[81,133] 141ns 249mb exceeds soft limit 100mb by more than 50%"
    - "WARNING: file L0.56[103,173] 172ns 218mb exceeds soft limit 100mb by more than 50%"
    - "WARNING: file L0.92[183,197] 212ns 202mb exceeds soft limit 100mb by more than 50%"
    - "WARNING: file L0.121[245,255] 273ns 162mb exceeds soft limit 100mb by more than 50%"
    - "WARNING: file L0.131[260,270] 273ns 176mb exceeds soft limit 100mb by more than 50%"
    - "WARNING: file L0.143[287,298] 306ns 171mb exceeds soft limit 100mb by more than 50%"
    - "WARNING: file L0.149[295,332] 335ns 190mb exceeds soft limit 100mb by more than 50%"
    - "WARNING: file L0.150[297,313] 311ns 233mb exceeds soft limit 100mb by more than 50%"
    - "WARNING: file L0.151[300,307] 306ns 168mb exceeds soft limit 100mb by more than 50%"
    - "WARNING: file L0.154[305,317] 316ns 219mb exceeds soft limit 100mb by more than 50%"
    - "WARNING: file L0.156[310,320] 319ns 212mb exceeds soft limit 100mb by more than 50%"
    - "WARNING: file L0.158[315,324] 323ns 214mb exceeds soft limit 100mb by more than 50%"
    - "WARNING: file L0.159[318,326] 325ns 214mb exceeds soft limit 100mb by more than 50%"
    - "WARNING: file L0.160[322,329] 328ns 213mb exceeds soft limit 100mb by more than 50%"
    - "WARNING: file L0.161[327,333] 336ns 183mb exceeds soft limit 100mb by more than 50%"
    - "WARNING: file L0.162[330,338] 340ns 231mb exceeds soft limit 100mb by more than 50%"
    - "WARNING: file L0.163[331,338] 341ns 232mb exceeds soft limit 100mb by more than 50%"
    - "**** Final Output Files (18.14gb written)"
    - "L2                                                                                                                 "
    - "L2.420[1,44] 342ns 100mb |-L2.420--|                                                                               "
    - "L2.453[282,288] 342ns 100mb                                                                          |L2.453|        "
    - "L2.460[183,189] 342ns 114mb                                                |L2.460|                                  "
    - "L2.467[314,314] 342ns 113mb                                                                                   |L2.467|"
    - "L2.468[315,316] 342ns 107mb                                                                                   |L2.468|"
    - "L2.469[317,317] 342ns 107mb                                                                                    |L2.469|"
    - "L2.470[318,319] 342ns 111mb                                                                                    |L2.470|"
    - "L2.471[320,320] 342ns 111mb                                                                                    |L2.471|"
    - "L2.472[321,323] 342ns 146mb                                                                                     |L2.472|"
    - "L2.473[324,325] 342ns 127mb                                                                                      |L2.473|"
    - "L2.474[326,326] 342ns 127mb                                                                                      |L2.474|"
    - "L2.475[327,329] 342ns 197mb                                                                                      |L2.475|"
    - "L2.476[330,331] 342ns 114mb                                                                                       |L2.476|"
    - "L2.477[332,332] 342ns 114mb                                                                                        |L2.477|"
    - "L2.478[333,335] 342ns 199mb                                                                                        |L2.478|"
    - "L2.493[45,69] 342ns 102mb           |L2.493|                                                                       "
    - "L2.499[115,121] 342ns 113mb                              |L2.499|                                                    "
    - "L2.505[158,164] 342ns 108mb                                         |L2.505|                                         "
    - "L2.509[178,182] 342ns 109mb                                               |L2.509|                                   "
    - "L2.510[190,196] 342ns 110mb                                                  |L2.510|                                "
    - "L2.515[227,233] 342ns 114mb                                                            |L2.515|                      "
    - "L2.521[261,267] 342ns 117mb                                                                     |L2.521|             "
    - "L2.524[289,294] 342ns 124mb                                                                            |L2.524|      "
    - "L2.530[311,313] 342ns 133mb                                                                                  |L2.530|"
    - "L2.531[336,337] 342ns 146mb                                                                                         |L2.531|"
    - "L2.532[338,338] 342ns 146mb                                                                                         |L2.532|"
    - "L2.534[70,85] 342ns 102mb                  |L2.534|                                                                "
    - "L2.535[86,100] 342ns 95mb                      |L2.535|                                                            "
    - "L2.536[101,102] 342ns 20mb                          |L2.536|                                                        "
    - "L2.537[103,109] 342ns 103mb                           |L2.537|                                                       "
    - "L2.538[110,114] 342ns 86mb                             |L2.538|                                                     "
    - "L2.539[122,128] 342ns 104mb                                |L2.539|                                                  "
    - "L2.540[129,134] 342ns 87mb                                  |L2.540|                                                "
    - "L2.541[135,138] 342ns 87mb                                   |L2.541|                                               "
    - "L2.542[139,146] 342ns 106mb                                    |L2.542|                                              "
    - "L2.543[147,153] 342ns 91mb                                      |L2.543|                                            "
    - "L2.544[154,157] 342ns 76mb                                        |L2.544|                                          "
    - "L2.545[165,171] 342ns 118mb                                           |L2.545|                                       "
    - "L2.546[172,177] 342ns 118mb                                             |L2.546|                                     "
    - "L2.547[197,204] 342ns 111mb                                                    |L2.547|                              "
    - "L2.548[205,211] 342ns 95mb                                                      |L2.548|                            "
    - "L2.549[212,213] 342ns 48mb                                                        |L2.549|                          "
    - "L2.550[214,220] 342ns 108mb                                                        |L2.550|                          "
    - "L2.551[221,226] 342ns 108mb                                                          |L2.551|                        "
    - "L2.552[234,240] 342ns 104mb                                                              |L2.552|                    "
    - "L2.553[241,246] 342ns 86mb                                                               |L2.553|                   "
    - "L2.554[247,248] 342ns 52mb                                                                 |L2.554|                 "
    - "L2.555[249,255] 342ns 102mb                                                                  |L2.555|                "
    - "L2.556[256,260] 342ns 85mb                                                                   |L2.556|               "
    - "L2.557[268,273] 342ns 111mb                                                                       |L2.557|           "
    - "L2.558[274,278] 342ns 89mb                                                                        |L2.558|          "
    - "L2.559[279,281] 342ns 89mb                                                                          |L2.559|        "
    - "L2.560[295,299] 342ns 130mb                                                                              |L2.560|    "
    - "L2.561[300,303] 342ns 98mb                                                                               |L2.561|   "
    - "L2.562[304,304] 342ns 65mb                                                                                |L2.562|  "
    - "L2.563[305,308] 342ns 137mb                                                                                |L2.563|  "
    - "L2.564[309,310] 342ns 92mb                                                                                  |L2.564|"
    - "L2.565[339,339] 342ns 12mb                                                                                          |L2.565|"
    - "WARNING: file L2.475[327,329] 342ns 197mb exceeds soft limit 100mb by more than 50%"
    - "WARNING: file L2.478[333,335] 342ns 199mb exceeds soft limit 100mb by more than 50%"
    "###
    );
}
