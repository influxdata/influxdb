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
        .with_max_num_files_per_plan(20)
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
    - "**** Simulation run 0, type=compact(TotalSizeLessThanMaxCompactSize). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.51[0,1] 999ns         |-----------------------------------------L0.51------------------------------------------|"
    - "**** 1 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L1, all files 10mb                                                                                                 "
    - "L1.?[0,1] 999ns          |------------------------------------------L1.?------------------------------------------|"
    - "Committing partition 1:"
    - "  Soft Deleting 1 files: L0.51"
    - "  Creating 1 files"
    - "**** Simulation run 1, type=split(VerticalSplit)(split_times=[355, 629]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.2[42,986] 1us         |------------------------------------------L0.2------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[42,355] 1us 3mb     |-----------L0.?------------|                                                             "
    - "L0.?[356,629] 1us 3mb                                 |----------L0.?----------|                                   "
    - "L0.?[630,986] 1us 4mb                                                            |-------------L0.?--------------| "
    - "**** Simulation run 2, type=split(VerticalSplit)(split_times=[355, 629]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.6[42,986] 1us         |------------------------------------------L0.6------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[42,355] 1us 3mb     |-----------L0.?------------|                                                             "
    - "L0.?[356,629] 1us 3mb                                 |----------L0.?----------|                                   "
    - "L0.?[630,986] 1us 4mb                                                            |-------------L0.?--------------| "
    - "**** Simulation run 3, type=split(VerticalSplit)(split_times=[355, 629]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.10[42,986] 1.01us     |-----------------------------------------L0.10------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[42,355] 1.01us 3mb  |-----------L0.?------------|                                                             "
    - "L0.?[356,629] 1.01us 3mb                              |----------L0.?----------|                                   "
    - "L0.?[630,986] 1.01us 4mb                                                         |-------------L0.?--------------| "
    - "**** Simulation run 4, type=split(VerticalSplit)(split_times=[355, 629]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.14[42,986] 1.01us     |-----------------------------------------L0.14------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[42,355] 1.01us 3mb  |-----------L0.?------------|                                                             "
    - "L0.?[356,629] 1.01us 3mb                              |----------L0.?----------|                                   "
    - "L0.?[630,986] 1.01us 4mb                                                         |-------------L0.?--------------| "
    - "**** Simulation run 5, type=split(VerticalSplit)(split_times=[355, 629]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.18[42,986] 1.02us     |-----------------------------------------L0.18------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[42,355] 1.02us 3mb  |-----------L0.?------------|                                                             "
    - "L0.?[356,629] 1.02us 3mb                              |----------L0.?----------|                                   "
    - "L0.?[630,986] 1.02us 4mb                                                         |-------------L0.?--------------| "
    - "**** Simulation run 6, type=split(VerticalSplit)(split_times=[355, 629]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.22[42,986] 1.02us     |-----------------------------------------L0.22------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[42,355] 1.02us 3mb  |-----------L0.?------------|                                                             "
    - "L0.?[356,629] 1.02us 3mb                              |----------L0.?----------|                                   "
    - "L0.?[630,986] 1.02us 4mb                                                         |-------------L0.?--------------| "
    - "**** Simulation run 7, type=split(VerticalSplit)(split_times=[355, 629]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.26[42,986] 1.02us     |-----------------------------------------L0.26------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[42,355] 1.02us 3mb  |-----------L0.?------------|                                                             "
    - "L0.?[356,629] 1.02us 3mb                              |----------L0.?----------|                                   "
    - "L0.?[630,986] 1.02us 4mb                                                         |-------------L0.?--------------| "
    - "**** Simulation run 8, type=split(VerticalSplit)(split_times=[355, 629]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.30[42,986] 1.03us     |-----------------------------------------L0.30------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[42,355] 1.03us 3mb  |-----------L0.?------------|                                                             "
    - "L0.?[356,629] 1.03us 3mb                              |----------L0.?----------|                                   "
    - "L0.?[630,986] 1.03us 4mb                                                         |-------------L0.?--------------| "
    - "**** Simulation run 9, type=split(VerticalSplit)(split_times=[355, 629]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.34[42,986] 1.03us     |-----------------------------------------L0.34------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[42,355] 1.03us 3mb  |-----------L0.?------------|                                                             "
    - "L0.?[356,629] 1.03us 3mb                              |----------L0.?----------|                                   "
    - "L0.?[630,986] 1.03us 4mb                                                         |-------------L0.?--------------| "
    - "**** Simulation run 10, type=split(VerticalSplit)(split_times=[355, 629]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.38[42,986] 1.04us     |-----------------------------------------L0.38------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[42,355] 1.04us 3mb  |-----------L0.?------------|                                                             "
    - "L0.?[356,629] 1.04us 3mb                              |----------L0.?----------|                                   "
    - "L0.?[630,986] 1.04us 4mb                                                         |-------------L0.?--------------| "
    - "**** Simulation run 11, type=split(VerticalSplit)(split_times=[355, 629]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.42[42,986] 1.04us     |-----------------------------------------L0.42------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[42,355] 1.04us 3mb  |-----------L0.?------------|                                                             "
    - "L0.?[356,629] 1.04us 3mb                              |----------L0.?----------|                                   "
    - "L0.?[630,986] 1.04us 4mb                                                         |-------------L0.?--------------| "
    - "**** Simulation run 12, type=split(VerticalSplit)(split_times=[355, 629]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.46[42,986] 1.05us     |-----------------------------------------L0.46------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[42,355] 1.05us 3mb  |-----------L0.?------------|                                                             "
    - "L0.?[356,629] 1.05us 3mb                              |----------L0.?----------|                                   "
    - "L0.?[630,986] 1.05us 4mb                                                         |-------------L0.?--------------| "
    - "**** Simulation run 13, type=split(VerticalSplit)(split_times=[355, 629]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.50[42,986] 1.05us     |-----------------------------------------L0.50------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[42,355] 1.05us 3mb  |-----------L0.?------------|                                                             "
    - "L0.?[356,629] 1.05us 3mb                              |----------L0.?----------|                                   "
    - "L0.?[630,986] 1.05us 4mb                                                         |-------------L0.?--------------| "
    - "**** Simulation run 14, type=split(VerticalSplit)(split_times=[355]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.4[50,629] 1us         |------------------------------------------L0.4------------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[50,355] 1us 5mb     |--------------------L0.?---------------------|                                           "
    - "L0.?[356,629] 1us 5mb                                                   |------------------L0.?------------------| "
    - "**** Simulation run 15, type=split(VerticalSplit)(split_times=[355]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.8[50,629] 1.01us      |------------------------------------------L0.8------------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[50,355] 1.01us 5mb  |--------------------L0.?---------------------|                                           "
    - "L0.?[356,629] 1.01us 5mb                                                |------------------L0.?------------------| "
    - "**** Simulation run 16, type=split(VerticalSplit)(split_times=[355]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.12[50,629] 1.01us     |-----------------------------------------L0.12------------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[50,355] 1.01us 5mb  |--------------------L0.?---------------------|                                           "
    - "L0.?[356,629] 1.01us 5mb                                                |------------------L0.?------------------| "
    - "**** Simulation run 17, type=split(VerticalSplit)(split_times=[355]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.16[50,629] 1.01us     |-----------------------------------------L0.16------------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[50,355] 1.01us 5mb  |--------------------L0.?---------------------|                                           "
    - "L0.?[356,629] 1.01us 5mb                                                |------------------L0.?------------------| "
    - "**** Simulation run 18, type=split(VerticalSplit)(split_times=[355]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.20[50,629] 1.02us     |-----------------------------------------L0.20------------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[50,355] 1.02us 5mb  |--------------------L0.?---------------------|                                           "
    - "L0.?[356,629] 1.02us 5mb                                                |------------------L0.?------------------| "
    - "**** Simulation run 19, type=split(VerticalSplit)(split_times=[355]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.24[50,629] 1.02us     |-----------------------------------------L0.24------------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[50,355] 1.02us 5mb  |--------------------L0.?---------------------|                                           "
    - "L0.?[356,629] 1.02us 5mb                                                |------------------L0.?------------------| "
    - "**** Simulation run 20, type=split(VerticalSplit)(split_times=[355]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.28[50,629] 1.03us     |-----------------------------------------L0.28------------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[50,355] 1.03us 5mb  |--------------------L0.?---------------------|                                           "
    - "L0.?[356,629] 1.03us 5mb                                                |------------------L0.?------------------| "
    - "**** Simulation run 21, type=split(VerticalSplit)(split_times=[355]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.32[50,629] 1.03us     |-----------------------------------------L0.32------------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[50,355] 1.03us 5mb  |--------------------L0.?---------------------|                                           "
    - "L0.?[356,629] 1.03us 5mb                                                |------------------L0.?------------------| "
    - "**** Simulation run 22, type=split(VerticalSplit)(split_times=[355]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.36[50,629] 1.03us     |-----------------------------------------L0.36------------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[50,355] 1.03us 5mb  |--------------------L0.?---------------------|                                           "
    - "L0.?[356,629] 1.03us 5mb                                                |------------------L0.?------------------| "
    - "**** Simulation run 23, type=split(VerticalSplit)(split_times=[355]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.40[50,629] 1.04us     |-----------------------------------------L0.40------------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[50,355] 1.04us 5mb  |--------------------L0.?---------------------|                                           "
    - "L0.?[356,629] 1.04us 5mb                                                |------------------L0.?------------------| "
    - "**** Simulation run 24, type=split(VerticalSplit)(split_times=[355]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.44[50,629] 1.04us     |-----------------------------------------L0.44------------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[50,355] 1.04us 5mb  |--------------------L0.?---------------------|                                           "
    - "L0.?[356,629] 1.04us 5mb                                                |------------------L0.?------------------| "
    - "**** Simulation run 25, type=split(VerticalSplit)(split_times=[355]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.48[50,629] 1.05us     |-----------------------------------------L0.48------------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[50,355] 1.05us 5mb  |--------------------L0.?---------------------|                                           "
    - "L0.?[356,629] 1.05us 5mb                                                |------------------L0.?------------------| "
    - "**** Simulation run 26, type=split(VerticalSplit)(split_times=[355, 629]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.1[76,932] 1us         |------------------------------------------L0.1------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[76,355] 1us 3mb     |-----------L0.?------------|                                                             "
    - "L0.?[356,629] 1us 3mb                                 |-----------L0.?-----------|                                 "
    - "L0.?[630,932] 1us 4mb                                                              |------------L0.?-------------| "
    - "**** Simulation run 27, type=split(VerticalSplit)(split_times=[355, 629]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.5[76,932] 1us         |------------------------------------------L0.5------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[76,355] 1us 3mb     |-----------L0.?------------|                                                             "
    - "L0.?[356,629] 1us 3mb                                 |-----------L0.?-----------|                                 "
    - "L0.?[630,932] 1us 4mb                                                              |------------L0.?-------------| "
    - "**** Simulation run 28, type=split(VerticalSplit)(split_times=[355, 629]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.9[76,932] 1.01us      |------------------------------------------L0.9------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[76,355] 1.01us 3mb  |-----------L0.?------------|                                                             "
    - "L0.?[356,629] 1.01us 3mb                              |-----------L0.?-----------|                                 "
    - "L0.?[630,932] 1.01us 4mb                                                           |------------L0.?-------------| "
    - "**** Simulation run 29, type=split(VerticalSplit)(split_times=[355, 629]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.13[76,932] 1.01us     |-----------------------------------------L0.13------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[76,355] 1.01us 3mb  |-----------L0.?------------|                                                             "
    - "L0.?[356,629] 1.01us 3mb                              |-----------L0.?-----------|                                 "
    - "L0.?[630,932] 1.01us 4mb                                                           |------------L0.?-------------| "
    - "**** Simulation run 30, type=split(VerticalSplit)(split_times=[355, 629]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.17[76,932] 1.02us     |-----------------------------------------L0.17------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[76,355] 1.02us 3mb  |-----------L0.?------------|                                                             "
    - "L0.?[356,629] 1.02us 3mb                              |-----------L0.?-----------|                                 "
    - "L0.?[630,932] 1.02us 4mb                                                           |------------L0.?-------------| "
    - "**** Simulation run 31, type=split(VerticalSplit)(split_times=[355, 629]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.21[76,932] 1.02us     |-----------------------------------------L0.21------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[76,355] 1.02us 3mb  |-----------L0.?------------|                                                             "
    - "L0.?[356,629] 1.02us 3mb                              |-----------L0.?-----------|                                 "
    - "L0.?[630,932] 1.02us 4mb                                                           |------------L0.?-------------| "
    - "**** Simulation run 32, type=split(VerticalSplit)(split_times=[355, 629]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.25[76,932] 1.02us     |-----------------------------------------L0.25------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[76,355] 1.02us 3mb  |-----------L0.?------------|                                                             "
    - "L0.?[356,629] 1.02us 3mb                              |-----------L0.?-----------|                                 "
    - "L0.?[630,932] 1.02us 4mb                                                           |------------L0.?-------------| "
    - "**** Simulation run 33, type=split(VerticalSplit)(split_times=[355, 629]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.29[76,932] 1.03us     |-----------------------------------------L0.29------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[76,355] 1.03us 3mb  |-----------L0.?------------|                                                             "
    - "L0.?[356,629] 1.03us 3mb                              |-----------L0.?-----------|                                 "
    - "L0.?[630,932] 1.03us 4mb                                                           |------------L0.?-------------| "
    - "**** Simulation run 34, type=split(VerticalSplit)(split_times=[355, 629]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.33[76,932] 1.03us     |-----------------------------------------L0.33------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[76,355] 1.03us 3mb  |-----------L0.?------------|                                                             "
    - "L0.?[356,629] 1.03us 3mb                              |-----------L0.?-----------|                                 "
    - "L0.?[630,932] 1.03us 4mb                                                           |------------L0.?-------------| "
    - "**** Simulation run 35, type=split(VerticalSplit)(split_times=[355, 629]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.37[76,932] 1.04us     |-----------------------------------------L0.37------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[76,355] 1.04us 3mb  |-----------L0.?------------|                                                             "
    - "L0.?[356,629] 1.04us 3mb                              |-----------L0.?-----------|                                 "
    - "L0.?[630,932] 1.04us 4mb                                                           |------------L0.?-------------| "
    - "**** Simulation run 36, type=split(VerticalSplit)(split_times=[355, 629]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.41[76,932] 1.04us     |-----------------------------------------L0.41------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[76,355] 1.04us 3mb  |-----------L0.?------------|                                                             "
    - "L0.?[356,629] 1.04us 3mb                              |-----------L0.?-----------|                                 "
    - "L0.?[630,932] 1.04us 4mb                                                           |------------L0.?-------------| "
    - "**** Simulation run 37, type=split(VerticalSplit)(split_times=[355, 629]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.45[76,932] 1.04us     |-----------------------------------------L0.45------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[76,355] 1.04us 3mb  |-----------L0.?------------|                                                             "
    - "L0.?[356,629] 1.04us 3mb                              |-----------L0.?-----------|                                 "
    - "L0.?[630,932] 1.04us 4mb                                                           |------------L0.?-------------| "
    - "**** Simulation run 38, type=split(VerticalSplit)(split_times=[355, 629]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.49[76,932] 1.05us     |-----------------------------------------L0.49------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[76,355] 1.05us 3mb  |-----------L0.?------------|                                                             "
    - "L0.?[356,629] 1.05us 3mb                              |-----------L0.?-----------|                                 "
    - "L0.?[630,932] 1.05us 4mb                                                           |------------L0.?-------------| "
    - "**** Simulation run 39, type=split(VerticalSplit)(split_times=[355, 629]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.3[173,950] 1us        |------------------------------------------L0.3------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[173,355] 1us 2mb    |-------L0.?--------|                                                                     "
    - "L0.?[356,629] 1us 4mb                         |------------L0.?-------------|                                      "
    - "L0.?[630,950] 1us 4mb                                                        |---------------L0.?----------------| "
    - "**** Simulation run 40, type=split(VerticalSplit)(split_times=[355, 629]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.7[173,950] 1.01us     |------------------------------------------L0.7------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[173,355] 1.01us 2mb |-------L0.?--------|                                                                     "
    - "L0.?[356,629] 1.01us 4mb                      |------------L0.?-------------|                                      "
    - "L0.?[630,950] 1.01us 4mb                                                     |---------------L0.?----------------| "
    - "Committing partition 1:"
    - "  Soft Deleting 40 files: L0.1, L0.2, L0.3, L0.4, L0.5, L0.6, L0.7, L0.8, L0.9, L0.10, L0.12, L0.13, L0.14, L0.16, L0.17, L0.18, L0.20, L0.21, L0.22, L0.24, L0.25, L0.26, L0.28, L0.29, L0.30, L0.32, L0.33, L0.34, L0.36, L0.37, L0.38, L0.40, L0.41, L0.42, L0.44, L0.45, L0.46, L0.48, L0.49, L0.50"
    - "  Creating 108 files"
    - "**** Simulation run 41, type=split(VerticalSplit)(split_times=[355, 629]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.11[173,950] 1.01us    |-----------------------------------------L0.11------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[173,355] 1.01us 2mb |-------L0.?--------|                                                                     "
    - "L0.?[356,629] 1.01us 4mb                      |------------L0.?-------------|                                      "
    - "L0.?[630,950] 1.01us 4mb                                                     |---------------L0.?----------------| "
    - "**** Simulation run 42, type=split(VerticalSplit)(split_times=[355, 629]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.15[173,950] 1.01us    |-----------------------------------------L0.15------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[173,355] 1.01us 2mb |-------L0.?--------|                                                                     "
    - "L0.?[356,629] 1.01us 4mb                      |------------L0.?-------------|                                      "
    - "L0.?[630,950] 1.01us 4mb                                                     |---------------L0.?----------------| "
    - "**** Simulation run 43, type=split(VerticalSplit)(split_times=[355, 629]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.19[173,950] 1.02us    |-----------------------------------------L0.19------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[173,355] 1.02us 2mb |-------L0.?--------|                                                                     "
    - "L0.?[356,629] 1.02us 4mb                      |------------L0.?-------------|                                      "
    - "L0.?[630,950] 1.02us 4mb                                                     |---------------L0.?----------------| "
    - "**** Simulation run 44, type=split(VerticalSplit)(split_times=[355, 629]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.23[173,950] 1.02us    |-----------------------------------------L0.23------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[173,355] 1.02us 2mb |-------L0.?--------|                                                                     "
    - "L0.?[356,629] 1.02us 4mb                      |------------L0.?-------------|                                      "
    - "L0.?[630,950] 1.02us 4mb                                                     |---------------L0.?----------------| "
    - "**** Simulation run 45, type=split(VerticalSplit)(split_times=[355, 629]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.27[173,950] 1.03us    |-----------------------------------------L0.27------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[173,355] 1.03us 2mb |-------L0.?--------|                                                                     "
    - "L0.?[356,629] 1.03us 4mb                      |------------L0.?-------------|                                      "
    - "L0.?[630,950] 1.03us 4mb                                                     |---------------L0.?----------------| "
    - "**** Simulation run 46, type=split(VerticalSplit)(split_times=[355, 629]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.31[173,950] 1.03us    |-----------------------------------------L0.31------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[173,355] 1.03us 2mb |-------L0.?--------|                                                                     "
    - "L0.?[356,629] 1.03us 4mb                      |------------L0.?-------------|                                      "
    - "L0.?[630,950] 1.03us 4mb                                                     |---------------L0.?----------------| "
    - "**** Simulation run 47, type=split(VerticalSplit)(split_times=[355, 629]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.35[173,950] 1.03us    |-----------------------------------------L0.35------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[173,355] 1.03us 2mb |-------L0.?--------|                                                                     "
    - "L0.?[356,629] 1.03us 4mb                      |------------L0.?-------------|                                      "
    - "L0.?[630,950] 1.03us 4mb                                                     |---------------L0.?----------------| "
    - "**** Simulation run 48, type=split(VerticalSplit)(split_times=[355, 629]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.39[173,950] 1.04us    |-----------------------------------------L0.39------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[173,355] 1.04us 2mb |-------L0.?--------|                                                                     "
    - "L0.?[356,629] 1.04us 4mb                      |------------L0.?-------------|                                      "
    - "L0.?[630,950] 1.04us 4mb                                                     |---------------L0.?----------------| "
    - "**** Simulation run 49, type=split(VerticalSplit)(split_times=[355, 629]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.43[173,950] 1.04us    |-----------------------------------------L0.43------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[173,355] 1.04us 2mb |-------L0.?--------|                                                                     "
    - "L0.?[356,629] 1.04us 4mb                      |------------L0.?-------------|                                      "
    - "L0.?[630,950] 1.04us 4mb                                                     |---------------L0.?----------------| "
    - "**** Simulation run 50, type=split(VerticalSplit)(split_times=[355, 629]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.47[173,950] 1.05us    |-----------------------------------------L0.47------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[173,355] 1.05us 2mb |-------L0.?--------|                                                                     "
    - "L0.?[356,629] 1.05us 4mb                      |------------L0.?-------------|                                      "
    - "L0.?[630,950] 1.05us 4mb                                                     |---------------L0.?----------------| "
    - "Committing partition 1:"
    - "  Soft Deleting 10 files: L0.11, L0.15, L0.19, L0.23, L0.27, L0.31, L0.35, L0.39, L0.43, L0.47"
    - "  Creating 30 files"
    - "**** Simulation run 51, type=compact(ManySmallFiles). 20 Input Files, 71mb total:"
    - "L0                                                                                                                 "
    - "L0.116[76,355] 1us 3mb            |------------------------------------L0.116------------------------------------| "
    - "L0.53[42,355] 1us 3mb    |-----------------------------------------L0.53------------------------------------------|"
    - "L0.155[173,355] 1us 2mb                                       |----------------------L0.155----------------------| "
    - "L0.92[50,355] 1us 5mb      |----------------------------------------L0.92----------------------------------------| "
    - "L0.119[76,355] 1us 3mb            |------------------------------------L0.119------------------------------------| "
    - "L0.56[42,355] 1us 3mb    |-----------------------------------------L0.56------------------------------------------|"
    - "L0.158[173,355] 1.01us 2mb                                     |----------------------L0.158----------------------| "
    - "L0.94[50,355] 1.01us 5mb   |----------------------------------------L0.94----------------------------------------| "
    - "L0.122[76,355] 1.01us 3mb         |------------------------------------L0.122------------------------------------| "
    - "L0.59[42,355] 1.01us 3mb |-----------------------------------------L0.59------------------------------------------|"
    - "L0.161[173,355] 1.01us 2mb                                     |----------------------L0.161----------------------| "
    - "L0.96[50,355] 1.01us 5mb   |----------------------------------------L0.96----------------------------------------| "
    - "L0.125[76,355] 1.01us 3mb         |------------------------------------L0.125------------------------------------| "
    - "L0.62[42,355] 1.01us 3mb |-----------------------------------------L0.62------------------------------------------|"
    - "L0.164[173,355] 1.01us 2mb                                     |----------------------L0.164----------------------| "
    - "L0.98[50,355] 1.01us 5mb   |----------------------------------------L0.98----------------------------------------| "
    - "L0.128[76,355] 1.02us 3mb         |------------------------------------L0.128------------------------------------| "
    - "L0.65[42,355] 1.02us 3mb |-----------------------------------------L0.65------------------------------------------|"
    - "L0.167[173,355] 1.02us 2mb                                     |----------------------L0.167----------------------| "
    - "L0.100[50,355] 1.02us 5mb  |---------------------------------------L0.100----------------------------------------| "
    - "**** 1 Output Files (parquet_file_id not yet assigned), 71mb total:"
    - "L0, all files 71mb                                                                                                 "
    - "L0.?[42,355] 1.02us      |------------------------------------------L0.?------------------------------------------|"
    - "Committing partition 1:"
    - "  Soft Deleting 20 files: L0.53, L0.56, L0.59, L0.62, L0.65, L0.92, L0.94, L0.96, L0.98, L0.100, L0.116, L0.119, L0.122, L0.125, L0.128, L0.155, L0.158, L0.161, L0.164, L0.167"
    - "  Creating 1 files"
    - "**** Simulation run 52, type=compact(ManySmallFiles). 20 Input Files, 71mb total:"
    - "L0                                                                                                                 "
    - "L0.131[76,355] 1.02us 3mb         |------------------------------------L0.131------------------------------------| "
    - "L0.68[42,355] 1.02us 3mb |-----------------------------------------L0.68------------------------------------------|"
    - "L0.170[173,355] 1.02us 2mb                                     |----------------------L0.170----------------------| "
    - "L0.102[50,355] 1.02us 5mb  |---------------------------------------L0.102----------------------------------------| "
    - "L0.134[76,355] 1.02us 3mb         |------------------------------------L0.134------------------------------------| "
    - "L0.71[42,355] 1.02us 3mb |-----------------------------------------L0.71------------------------------------------|"
    - "L0.173[173,355] 1.03us 2mb                                     |----------------------L0.173----------------------| "
    - "L0.104[50,355] 1.03us 5mb  |---------------------------------------L0.104----------------------------------------| "
    - "L0.137[76,355] 1.03us 3mb         |------------------------------------L0.137------------------------------------| "
    - "L0.74[42,355] 1.03us 3mb |-----------------------------------------L0.74------------------------------------------|"
    - "L0.176[173,355] 1.03us 2mb                                     |----------------------L0.176----------------------| "
    - "L0.106[50,355] 1.03us 5mb  |---------------------------------------L0.106----------------------------------------| "
    - "L0.140[76,355] 1.03us 3mb         |------------------------------------L0.140------------------------------------| "
    - "L0.77[42,355] 1.03us 3mb |-----------------------------------------L0.77------------------------------------------|"
    - "L0.179[173,355] 1.03us 2mb                                     |----------------------L0.179----------------------| "
    - "L0.108[50,355] 1.03us 5mb  |---------------------------------------L0.108----------------------------------------| "
    - "L0.143[76,355] 1.04us 3mb         |------------------------------------L0.143------------------------------------| "
    - "L0.80[42,355] 1.04us 3mb |-----------------------------------------L0.80------------------------------------------|"
    - "L0.182[173,355] 1.04us 2mb                                     |----------------------L0.182----------------------| "
    - "L0.110[50,355] 1.04us 5mb  |---------------------------------------L0.110----------------------------------------| "
    - "**** 1 Output Files (parquet_file_id not yet assigned), 71mb total:"
    - "L0, all files 71mb                                                                                                 "
    - "L0.?[42,355] 1.04us      |------------------------------------------L0.?------------------------------------------|"
    - "Committing partition 1:"
    - "  Soft Deleting 20 files: L0.68, L0.71, L0.74, L0.77, L0.80, L0.102, L0.104, L0.106, L0.108, L0.110, L0.131, L0.134, L0.137, L0.140, L0.143, L0.170, L0.173, L0.176, L0.179, L0.182"
    - "  Creating 1 files"
    - "**** Simulation run 53, type=compact(ManySmallFiles). 10 Input Files, 35mb total:"
    - "L0                                                                                                                 "
    - "L0.146[76,355] 1.04us 3mb         |------------------------------------L0.146------------------------------------| "
    - "L0.83[42,355] 1.04us 3mb |-----------------------------------------L0.83------------------------------------------|"
    - "L0.185[173,355] 1.04us 2mb                                     |----------------------L0.185----------------------| "
    - "L0.112[50,355] 1.04us 5mb  |---------------------------------------L0.112----------------------------------------| "
    - "L0.149[76,355] 1.04us 3mb         |------------------------------------L0.149------------------------------------| "
    - "L0.86[42,355] 1.05us 3mb |-----------------------------------------L0.86------------------------------------------|"
    - "L0.188[173,355] 1.05us 2mb                                     |----------------------L0.188----------------------| "
    - "L0.114[50,355] 1.05us 5mb  |---------------------------------------L0.114----------------------------------------| "
    - "L0.152[76,355] 1.05us 3mb         |------------------------------------L0.152------------------------------------| "
    - "L0.89[42,355] 1.05us 3mb |-----------------------------------------L0.89------------------------------------------|"
    - "**** 1 Output Files (parquet_file_id not yet assigned), 35mb total:"
    - "L0, all files 35mb                                                                                                 "
    - "L0.?[42,355] 1.05us      |------------------------------------------L0.?------------------------------------------|"
    - "Committing partition 1:"
    - "  Soft Deleting 10 files: L0.83, L0.86, L0.89, L0.112, L0.114, L0.146, L0.149, L0.152, L0.185, L0.188"
    - "  Creating 1 files"
    - "**** Simulation run 54, type=compact(ManySmallFiles). 20 Input Files, 72mb total:"
    - "L0                                                                                                                 "
    - "L0.117[356,629] 1us 3mb  |-----------------------------------------L0.117-----------------------------------------|"
    - "L0.54[356,629] 1us 3mb   |-----------------------------------------L0.54------------------------------------------|"
    - "L0.156[356,629] 1us 4mb  |-----------------------------------------L0.156-----------------------------------------|"
    - "L0.93[356,629] 1us 5mb   |-----------------------------------------L0.93------------------------------------------|"
    - "L0.120[356,629] 1us 3mb  |-----------------------------------------L0.120-----------------------------------------|"
    - "L0.57[356,629] 1us 3mb   |-----------------------------------------L0.57------------------------------------------|"
    - "L0.159[356,629] 1.01us 4mb|-----------------------------------------L0.159-----------------------------------------|"
    - "L0.95[356,629] 1.01us 5mb|-----------------------------------------L0.95------------------------------------------|"
    - "L0.123[356,629] 1.01us 3mb|-----------------------------------------L0.123-----------------------------------------|"
    - "L0.60[356,629] 1.01us 3mb|-----------------------------------------L0.60------------------------------------------|"
    - "L0.162[356,629] 1.01us 4mb|-----------------------------------------L0.162-----------------------------------------|"
    - "L0.97[356,629] 1.01us 5mb|-----------------------------------------L0.97------------------------------------------|"
    - "L0.126[356,629] 1.01us 3mb|-----------------------------------------L0.126-----------------------------------------|"
    - "L0.63[356,629] 1.01us 3mb|-----------------------------------------L0.63------------------------------------------|"
    - "L0.165[356,629] 1.01us 4mb|-----------------------------------------L0.165-----------------------------------------|"
    - "L0.99[356,629] 1.01us 5mb|-----------------------------------------L0.99------------------------------------------|"
    - "L0.129[356,629] 1.02us 3mb|-----------------------------------------L0.129-----------------------------------------|"
    - "L0.66[356,629] 1.02us 3mb|-----------------------------------------L0.66------------------------------------------|"
    - "L0.168[356,629] 1.02us 4mb|-----------------------------------------L0.168-----------------------------------------|"
    - "L0.101[356,629] 1.02us 5mb|-----------------------------------------L0.101-----------------------------------------|"
    - "**** 1 Output Files (parquet_file_id not yet assigned), 72mb total:"
    - "L0, all files 72mb                                                                                                 "
    - "L0.?[356,629] 1.02us     |------------------------------------------L0.?------------------------------------------|"
    - "Committing partition 1:"
    - "  Soft Deleting 20 files: L0.54, L0.57, L0.60, L0.63, L0.66, L0.93, L0.95, L0.97, L0.99, L0.101, L0.117, L0.120, L0.123, L0.126, L0.129, L0.156, L0.159, L0.162, L0.165, L0.168"
    - "  Creating 1 files"
    - "**** Simulation run 55, type=compact(ManySmallFiles). 10 Input Files, 35mb total:"
    - "L0                                                                                                                 "
    - "L0.147[356,629] 1.04us 3mb|-----------------------------------------L0.147-----------------------------------------|"
    - "L0.84[356,629] 1.04us 3mb|-----------------------------------------L0.84------------------------------------------|"
    - "L0.186[356,629] 1.04us 4mb|-----------------------------------------L0.186-----------------------------------------|"
    - "L0.113[356,629] 1.04us 5mb|-----------------------------------------L0.113-----------------------------------------|"
    - "L0.150[356,629] 1.04us 3mb|-----------------------------------------L0.150-----------------------------------------|"
    - "L0.87[356,629] 1.05us 3mb|-----------------------------------------L0.87------------------------------------------|"
    - "L0.189[356,629] 1.05us 4mb|-----------------------------------------L0.189-----------------------------------------|"
    - "L0.115[356,629] 1.05us 5mb|-----------------------------------------L0.115-----------------------------------------|"
    - "L0.153[356,629] 1.05us 3mb|-----------------------------------------L0.153-----------------------------------------|"
    - "L0.90[356,629] 1.05us 3mb|-----------------------------------------L0.90------------------------------------------|"
    - "**** 1 Output Files (parquet_file_id not yet assigned), 35mb total:"
    - "L0, all files 35mb                                                                                                 "
    - "L0.?[356,629] 1.05us     |------------------------------------------L0.?------------------------------------------|"
    - "Committing partition 1:"
    - "  Soft Deleting 10 files: L0.84, L0.87, L0.90, L0.113, L0.115, L0.147, L0.150, L0.153, L0.186, L0.189"
    - "  Creating 1 files"
    - "**** Simulation run 56, type=compact(ManySmallFiles). 20 Input Files, 72mb total:"
    - "L0                                                                                                                 "
    - "L0.132[356,629] 1.02us 3mb|-----------------------------------------L0.132-----------------------------------------|"
    - "L0.69[356,629] 1.02us 3mb|-----------------------------------------L0.69------------------------------------------|"
    - "L0.171[356,629] 1.02us 4mb|-----------------------------------------L0.171-----------------------------------------|"
    - "L0.103[356,629] 1.02us 5mb|-----------------------------------------L0.103-----------------------------------------|"
    - "L0.135[356,629] 1.02us 3mb|-----------------------------------------L0.135-----------------------------------------|"
    - "L0.72[356,629] 1.02us 3mb|-----------------------------------------L0.72------------------------------------------|"
    - "L0.174[356,629] 1.03us 4mb|-----------------------------------------L0.174-----------------------------------------|"
    - "L0.105[356,629] 1.03us 5mb|-----------------------------------------L0.105-----------------------------------------|"
    - "L0.138[356,629] 1.03us 3mb|-----------------------------------------L0.138-----------------------------------------|"
    - "L0.75[356,629] 1.03us 3mb|-----------------------------------------L0.75------------------------------------------|"
    - "L0.177[356,629] 1.03us 4mb|-----------------------------------------L0.177-----------------------------------------|"
    - "L0.107[356,629] 1.03us 5mb|-----------------------------------------L0.107-----------------------------------------|"
    - "L0.141[356,629] 1.03us 3mb|-----------------------------------------L0.141-----------------------------------------|"
    - "L0.78[356,629] 1.03us 3mb|-----------------------------------------L0.78------------------------------------------|"
    - "L0.180[356,629] 1.03us 4mb|-----------------------------------------L0.180-----------------------------------------|"
    - "L0.109[356,629] 1.03us 5mb|-----------------------------------------L0.109-----------------------------------------|"
    - "L0.144[356,629] 1.04us 3mb|-----------------------------------------L0.144-----------------------------------------|"
    - "L0.81[356,629] 1.04us 3mb|-----------------------------------------L0.81------------------------------------------|"
    - "L0.183[356,629] 1.04us 4mb|-----------------------------------------L0.183-----------------------------------------|"
    - "L0.111[356,629] 1.04us 5mb|-----------------------------------------L0.111-----------------------------------------|"
    - "**** 1 Output Files (parquet_file_id not yet assigned), 72mb total:"
    - "L0, all files 72mb                                                                                                 "
    - "L0.?[356,629] 1.04us     |------------------------------------------L0.?------------------------------------------|"
    - "Committing partition 1:"
    - "  Soft Deleting 20 files: L0.69, L0.72, L0.75, L0.78, L0.81, L0.103, L0.105, L0.107, L0.109, L0.111, L0.132, L0.135, L0.138, L0.141, L0.144, L0.171, L0.174, L0.177, L0.180, L0.183"
    - "  Creating 1 files"
    - "**** Simulation run 57, type=compact(ManySmallFiles). 20 Input Files, 76mb total:"
    - "L0                                                                                                                 "
    - "L0.118[630,932] 1us 4mb  |----------------------------------L0.118----------------------------------|              "
    - "L0.55[630,986] 1us 4mb   |-----------------------------------------L0.55------------------------------------------|"
    - "L0.157[630,950] 1us 4mb  |------------------------------------L0.157------------------------------------|          "
    - "L0.121[630,932] 1us 4mb  |----------------------------------L0.121----------------------------------|              "
    - "L0.58[630,986] 1us 4mb   |-----------------------------------------L0.58------------------------------------------|"
    - "L0.160[630,950] 1.01us 4mb|------------------------------------L0.160------------------------------------|          "
    - "L0.124[630,932] 1.01us 4mb|----------------------------------L0.124----------------------------------|              "
    - "L0.61[630,986] 1.01us 4mb|-----------------------------------------L0.61------------------------------------------|"
    - "L0.163[630,950] 1.01us 4mb|------------------------------------L0.163------------------------------------|          "
    - "L0.127[630,932] 1.01us 4mb|----------------------------------L0.127----------------------------------|              "
    - "L0.64[630,986] 1.01us 4mb|-----------------------------------------L0.64------------------------------------------|"
    - "L0.166[630,950] 1.01us 4mb|------------------------------------L0.166------------------------------------|          "
    - "L0.130[630,932] 1.02us 4mb|----------------------------------L0.130----------------------------------|              "
    - "L0.67[630,986] 1.02us 4mb|-----------------------------------------L0.67------------------------------------------|"
    - "L0.169[630,950] 1.02us 4mb|------------------------------------L0.169------------------------------------|          "
    - "L0.133[630,932] 1.02us 4mb|----------------------------------L0.133----------------------------------|              "
    - "L0.70[630,986] 1.02us 4mb|-----------------------------------------L0.70------------------------------------------|"
    - "L0.172[630,950] 1.02us 4mb|------------------------------------L0.172------------------------------------|          "
    - "L0.136[630,932] 1.02us 4mb|----------------------------------L0.136----------------------------------|              "
    - "L0.73[630,986] 1.02us 4mb|-----------------------------------------L0.73------------------------------------------|"
    - "**** 1 Output Files (parquet_file_id not yet assigned), 76mb total:"
    - "L0, all files 76mb                                                                                                 "
    - "L0.?[630,986] 1.02us     |------------------------------------------L0.?------------------------------------------|"
    - "Committing partition 1:"
    - "  Soft Deleting 20 files: L0.55, L0.58, L0.61, L0.64, L0.67, L0.70, L0.73, L0.118, L0.121, L0.124, L0.127, L0.130, L0.133, L0.136, L0.157, L0.160, L0.163, L0.166, L0.169, L0.172"
    - "  Creating 1 files"
    - "**** Simulation run 58, type=compact(ManySmallFiles). 18 Input Files, 69mb total:"
    - "L0                                                                                                                 "
    - "L0.175[630,950] 1.03us 4mb|------------------------------------L0.175------------------------------------|          "
    - "L0.139[630,932] 1.03us 4mb|----------------------------------L0.139----------------------------------|              "
    - "L0.76[630,986] 1.03us 4mb|-----------------------------------------L0.76------------------------------------------|"
    - "L0.178[630,950] 1.03us 4mb|------------------------------------L0.178------------------------------------|          "
    - "L0.142[630,932] 1.03us 4mb|----------------------------------L0.142----------------------------------|              "
    - "L0.79[630,986] 1.03us 4mb|-----------------------------------------L0.79------------------------------------------|"
    - "L0.181[630,950] 1.03us 4mb|------------------------------------L0.181------------------------------------|          "
    - "L0.145[630,932] 1.04us 4mb|----------------------------------L0.145----------------------------------|              "
    - "L0.82[630,986] 1.04us 4mb|-----------------------------------------L0.82------------------------------------------|"
    - "L0.184[630,950] 1.04us 4mb|------------------------------------L0.184------------------------------------|          "
    - "L0.148[630,932] 1.04us 4mb|----------------------------------L0.148----------------------------------|              "
    - "L0.85[630,986] 1.04us 4mb|-----------------------------------------L0.85------------------------------------------|"
    - "L0.187[630,950] 1.04us 4mb|------------------------------------L0.187------------------------------------|          "
    - "L0.151[630,932] 1.04us 4mb|----------------------------------L0.151----------------------------------|              "
    - "L0.88[630,986] 1.05us 4mb|-----------------------------------------L0.88------------------------------------------|"
    - "L0.190[630,950] 1.05us 4mb|------------------------------------L0.190------------------------------------|          "
    - "L0.154[630,932] 1.05us 4mb|----------------------------------L0.154----------------------------------|              "
    - "L0.91[630,986] 1.05us 4mb|-----------------------------------------L0.91------------------------------------------|"
    - "**** 1 Output Files (parquet_file_id not yet assigned), 69mb total:"
    - "L0, all files 69mb                                                                                                 "
    - "L0.?[630,986] 1.05us     |------------------------------------------L0.?------------------------------------------|"
    - "Committing partition 1:"
    - "  Soft Deleting 18 files: L0.76, L0.79, L0.82, L0.85, L0.88, L0.91, L0.139, L0.142, L0.145, L0.148, L0.151, L0.154, L0.175, L0.178, L0.181, L0.184, L0.187, L0.190"
    - "  Creating 1 files"
    - "**** Simulation run 59, type=split(CompactAndSplitOutput(TotalSizeLessThanMaxCompactSize))(split_times=[219]). 3 Input Files, 177mb total:"
    - "L0                                                                                                                 "
    - "L0.193[42,355] 1.05us 35mb|-----------------------------------------L0.193-----------------------------------------|"
    - "L0.192[42,355] 1.04us 71mb|-----------------------------------------L0.192-----------------------------------------|"
    - "L0.191[42,355] 1.02us 71mb|-----------------------------------------L0.191-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 177mb total:"
    - "L1                                                                                                                 "
    - "L1.?[42,219] 1.05us 100mb|----------------------L1.?----------------------|                                        "
    - "L1.?[220,355] 1.05us 77mb                                                   |----------------L1.?----------------| "
    - "Committing partition 1:"
    - "  Soft Deleting 3 files: L0.191, L0.192, L0.193"
    - "  Creating 2 files"
    - "**** Simulation run 60, type=split(CompactAndSplitOutput(TotalSizeLessThanMaxCompactSize))(split_times=[510]). 3 Input Files, 178mb total:"
    - "L0                                                                                                                 "
    - "L0.196[356,629] 1.04us 72mb|-----------------------------------------L0.196-----------------------------------------|"
    - "L0.195[356,629] 1.02us 72mb|-----------------------------------------L0.195-----------------------------------------|"
    - "L0.194[356,629] 1.05us 35mb|-----------------------------------------L0.194-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 178mb total:"
    - "L1                                                                                                                 "
    - "L1.?[356,510] 1.05us 101mb|----------------------L1.?----------------------|                                        "
    - "L1.?[511,629] 1.05us 77mb                                                   |----------------L1.?----------------| "
    - "Committing partition 1:"
    - "  Soft Deleting 3 files: L0.194, L0.195, L0.196"
    - "  Creating 2 files"
    - "**** Simulation run 61, type=split(CompactAndSplitOutput(TotalSizeLessThanMaxCompactSize))(split_times=[877]). 2 Input Files, 145mb total:"
    - "L0                                                                                                                 "
    - "L0.198[630,986] 1.05us 69mb|-----------------------------------------L0.198-----------------------------------------|"
    - "L0.197[630,986] 1.02us 76mb|-----------------------------------------L0.197-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 145mb total:"
    - "L1                                                                                                                 "
    - "L1.?[630,877] 1.05us 100mb|----------------------------L1.?----------------------------|                            "
    - "L1.?[878,986] 1.05us 44mb                                                              |----------L1.?-----------| "
    - "Committing partition 1:"
    - "  Soft Deleting 2 files: L0.197, L0.198"
    - "  Creating 2 files"
    - "**** Simulation run 62, type=split(CompactAndSplitOutput(FoundSubsetLessThanMaxCompactSize))(split_times=[178, 356]). 4 Input Files, 288mb total:"
    - "L1                                                                                                                 "
    - "L1.52[0,1] 999ns 10mb    |L1.52|                                                                                   "
    - "L1.199[42,219] 1.05us 100mb       |-----------L1.199------------|                                                    "
    - "L1.200[220,355] 1.05us 77mb                                      |-------L1.200--------|                             "
    - "L1.201[356,510] 1.05us 101mb                                                              |---------L1.201----------| "
    - "**** 3 Output Files (parquet_file_id not yet assigned), 288mb total:"
    - "L2                                                                                                                 "
    - "L2.?[0,178] 1.05us 101mb |------------L2.?-------------|                                                           "
    - "L2.?[179,356] 1.05us 100mb                               |------------L2.?-------------|                            "
    - "L2.?[357,510] 1.05us 87mb                                                               |----------L2.?-----------|"
    - "Committing partition 1:"
    - "  Soft Deleting 4 files: L1.52, L1.199, L1.200, L1.201"
    - "  Creating 3 files"
    - "**** Simulation run 63, type=split(CompactAndSplitOutput(TotalSizeLessThanMaxCompactSize))(split_times=[964]). 1 Input Files, 44mb total:"
    - "L1, all files 44mb                                                                                                 "
    - "L1.204[878,986] 1.05us   |-----------------------------------------L1.204-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 44mb total:"
    - "L2                                                                                                                 "
    - "L2.?[878,964] 1.05us 35mb|--------------------------------L2.?---------------------------------|                   "
    - "L2.?[965,986] 1.05us 9mb                                                                         |-----L2.?------| "
    - "Committing partition 1:"
    - "  Soft Deleting 1 files: L1.204"
    - "  Upgrading 2 files level to CompactionLevel::L2: L1.202, L1.203"
    - "  Creating 2 files"
    - "**** Final Output Files (1.8gb written)"
    - "L2                                                                                                                 "
    - "L2.202[511,629] 1.05us 77mb                                              |-L2.202-|                                  "
    - "L2.203[630,877] 1.05us 100mb                                                         |-------L2.203-------|           "
    - "L2.205[0,178] 1.05us 101mb|----L2.205----|                                                                          "
    - "L2.206[179,356] 1.05us 100mb                |----L2.206----|                                                          "
    - "L2.207[357,510] 1.05us 87mb                                |--L2.207---|                                             "
    - "L2.208[878,964] 1.05us 35mb                                                                                |L2.208|  "
    - "L2.209[965,986] 1.05us 9mb                                                                                        |L2.209|"
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
        .with_writes_breakdown()
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
    - "**** Simulation run 0, type=split(VerticalSplit)(split_times=[355, 629]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.12[42,986] 1us        |-----------------------------------------L0.12------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[42,355] 1us 3mb     |-----------L0.?------------|                                                             "
    - "L0.?[356,629] 1us 3mb                                 |----------L0.?----------|                                   "
    - "L0.?[630,986] 1us 4mb                                                            |-------------L0.?--------------| "
    - "**** Simulation run 1, type=split(VerticalSplit)(split_times=[355, 629]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.16[42,986] 1us        |-----------------------------------------L0.16------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[42,355] 1us 3mb     |-----------L0.?------------|                                                             "
    - "L0.?[356,629] 1us 3mb                                 |----------L0.?----------|                                   "
    - "L0.?[630,986] 1us 4mb                                                            |-------------L0.?--------------| "
    - "**** Simulation run 2, type=split(VerticalSplit)(split_times=[355, 629]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.20[42,986] 1.01us     |-----------------------------------------L0.20------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[42,355] 1.01us 3mb  |-----------L0.?------------|                                                             "
    - "L0.?[356,629] 1.01us 3mb                              |----------L0.?----------|                                   "
    - "L0.?[630,986] 1.01us 4mb                                                         |-------------L0.?--------------| "
    - "**** Simulation run 3, type=split(VerticalSplit)(split_times=[355, 629]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.24[42,986] 1.01us     |-----------------------------------------L0.24------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[42,355] 1.01us 3mb  |-----------L0.?------------|                                                             "
    - "L0.?[356,629] 1.01us 3mb                              |----------L0.?----------|                                   "
    - "L0.?[630,986] 1.01us 4mb                                                         |-------------L0.?--------------| "
    - "**** Simulation run 4, type=split(VerticalSplit)(split_times=[355, 629]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.28[42,986] 1.02us     |-----------------------------------------L0.28------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[42,355] 1.02us 3mb  |-----------L0.?------------|                                                             "
    - "L0.?[356,629] 1.02us 3mb                              |----------L0.?----------|                                   "
    - "L0.?[630,986] 1.02us 4mb                                                         |-------------L0.?--------------| "
    - "**** Simulation run 5, type=split(VerticalSplit)(split_times=[355, 629]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.32[42,986] 1.02us     |-----------------------------------------L0.32------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[42,355] 1.02us 3mb  |-----------L0.?------------|                                                             "
    - "L0.?[356,629] 1.02us 3mb                              |----------L0.?----------|                                   "
    - "L0.?[630,986] 1.02us 4mb                                                         |-------------L0.?--------------| "
    - "**** Simulation run 6, type=split(VerticalSplit)(split_times=[355, 629]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.36[42,986] 1.02us     |-----------------------------------------L0.36------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[42,355] 1.02us 3mb  |-----------L0.?------------|                                                             "
    - "L0.?[356,629] 1.02us 3mb                              |----------L0.?----------|                                   "
    - "L0.?[630,986] 1.02us 4mb                                                         |-------------L0.?--------------| "
    - "**** Simulation run 7, type=split(VerticalSplit)(split_times=[355, 629]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.40[42,986] 1.03us     |-----------------------------------------L0.40------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[42,355] 1.03us 3mb  |-----------L0.?------------|                                                             "
    - "L0.?[356,629] 1.03us 3mb                              |----------L0.?----------|                                   "
    - "L0.?[630,986] 1.03us 4mb                                                         |-------------L0.?--------------| "
    - "**** Simulation run 8, type=split(VerticalSplit)(split_times=[355, 629]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.44[42,986] 1.03us     |-----------------------------------------L0.44------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[42,355] 1.03us 3mb  |-----------L0.?------------|                                                             "
    - "L0.?[356,629] 1.03us 3mb                              |----------L0.?----------|                                   "
    - "L0.?[630,986] 1.03us 4mb                                                         |-------------L0.?--------------| "
    - "**** Simulation run 9, type=split(VerticalSplit)(split_times=[355, 629]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.48[42,986] 1.04us     |-----------------------------------------L0.48------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[42,355] 1.04us 3mb  |-----------L0.?------------|                                                             "
    - "L0.?[356,629] 1.04us 3mb                              |----------L0.?----------|                                   "
    - "L0.?[630,986] 1.04us 4mb                                                         |-------------L0.?--------------| "
    - "**** Simulation run 10, type=split(VerticalSplit)(split_times=[355, 629]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.52[42,986] 1.04us     |-----------------------------------------L0.52------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[42,355] 1.04us 3mb  |-----------L0.?------------|                                                             "
    - "L0.?[356,629] 1.04us 3mb                              |----------L0.?----------|                                   "
    - "L0.?[630,986] 1.04us 4mb                                                         |-------------L0.?--------------| "
    - "**** Simulation run 11, type=split(VerticalSplit)(split_times=[355, 629]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.56[42,986] 1.05us     |-----------------------------------------L0.56------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[42,355] 1.05us 3mb  |-----------L0.?------------|                                                             "
    - "L0.?[356,629] 1.05us 3mb                              |----------L0.?----------|                                   "
    - "L0.?[630,986] 1.05us 4mb                                                         |-------------L0.?--------------| "
    - "**** Simulation run 12, type=split(VerticalSplit)(split_times=[355, 629]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.60[42,986] 1.05us     |-----------------------------------------L0.60------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[42,355] 1.05us 3mb  |-----------L0.?------------|                                                             "
    - "L0.?[356,629] 1.05us 3mb                              |----------L0.?----------|                                   "
    - "L0.?[630,986] 1.05us 4mb                                                         |-------------L0.?--------------| "
    - "**** Simulation run 13, type=split(VerticalSplit)(split_times=[355]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.14[50,629] 1us        |-----------------------------------------L0.14------------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[50,355] 1us 5mb     |--------------------L0.?---------------------|                                           "
    - "L0.?[356,629] 1us 5mb                                                   |------------------L0.?------------------| "
    - "**** Simulation run 14, type=split(VerticalSplit)(split_times=[355]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.18[50,629] 1.01us     |-----------------------------------------L0.18------------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[50,355] 1.01us 5mb  |--------------------L0.?---------------------|                                           "
    - "L0.?[356,629] 1.01us 5mb                                                |------------------L0.?------------------| "
    - "**** Simulation run 15, type=split(VerticalSplit)(split_times=[355]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.22[50,629] 1.01us     |-----------------------------------------L0.22------------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[50,355] 1.01us 5mb  |--------------------L0.?---------------------|                                           "
    - "L0.?[356,629] 1.01us 5mb                                                |------------------L0.?------------------| "
    - "**** Simulation run 16, type=split(VerticalSplit)(split_times=[355]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.26[50,629] 1.01us     |-----------------------------------------L0.26------------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[50,355] 1.01us 5mb  |--------------------L0.?---------------------|                                           "
    - "L0.?[356,629] 1.01us 5mb                                                |------------------L0.?------------------| "
    - "**** Simulation run 17, type=split(VerticalSplit)(split_times=[355]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.30[50,629] 1.02us     |-----------------------------------------L0.30------------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[50,355] 1.02us 5mb  |--------------------L0.?---------------------|                                           "
    - "L0.?[356,629] 1.02us 5mb                                                |------------------L0.?------------------| "
    - "**** Simulation run 18, type=split(VerticalSplit)(split_times=[355]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.34[50,629] 1.02us     |-----------------------------------------L0.34------------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[50,355] 1.02us 5mb  |--------------------L0.?---------------------|                                           "
    - "L0.?[356,629] 1.02us 5mb                                                |------------------L0.?------------------| "
    - "**** Simulation run 19, type=split(VerticalSplit)(split_times=[355]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.38[50,629] 1.03us     |-----------------------------------------L0.38------------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[50,355] 1.03us 5mb  |--------------------L0.?---------------------|                                           "
    - "L0.?[356,629] 1.03us 5mb                                                |------------------L0.?------------------| "
    - "**** Simulation run 20, type=split(VerticalSplit)(split_times=[355]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.42[50,629] 1.03us     |-----------------------------------------L0.42------------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[50,355] 1.03us 5mb  |--------------------L0.?---------------------|                                           "
    - "L0.?[356,629] 1.03us 5mb                                                |------------------L0.?------------------| "
    - "**** Simulation run 21, type=split(VerticalSplit)(split_times=[355]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.46[50,629] 1.03us     |-----------------------------------------L0.46------------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[50,355] 1.03us 5mb  |--------------------L0.?---------------------|                                           "
    - "L0.?[356,629] 1.03us 5mb                                                |------------------L0.?------------------| "
    - "**** Simulation run 22, type=split(VerticalSplit)(split_times=[355]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.50[50,629] 1.04us     |-----------------------------------------L0.50------------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[50,355] 1.04us 5mb  |--------------------L0.?---------------------|                                           "
    - "L0.?[356,629] 1.04us 5mb                                                |------------------L0.?------------------| "
    - "**** Simulation run 23, type=split(VerticalSplit)(split_times=[355]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.54[50,629] 1.04us     |-----------------------------------------L0.54------------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[50,355] 1.04us 5mb  |--------------------L0.?---------------------|                                           "
    - "L0.?[356,629] 1.04us 5mb                                                |------------------L0.?------------------| "
    - "**** Simulation run 24, type=split(VerticalSplit)(split_times=[355]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.58[50,629] 1.05us     |-----------------------------------------L0.58------------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[50,355] 1.05us 5mb  |--------------------L0.?---------------------|                                           "
    - "L0.?[356,629] 1.05us 5mb                                                |------------------L0.?------------------| "
    - "**** Simulation run 25, type=split(VerticalSplit)(split_times=[355, 629]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.11[76,932] 1us        |-----------------------------------------L0.11------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[76,355] 1us 3mb     |-----------L0.?------------|                                                             "
    - "L0.?[356,629] 1us 3mb                                 |-----------L0.?-----------|                                 "
    - "L0.?[630,932] 1us 4mb                                                              |------------L0.?-------------| "
    - "**** Simulation run 26, type=split(VerticalSplit)(split_times=[355, 629]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.15[76,932] 1us        |-----------------------------------------L0.15------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[76,355] 1us 3mb     |-----------L0.?------------|                                                             "
    - "L0.?[356,629] 1us 3mb                                 |-----------L0.?-----------|                                 "
    - "L0.?[630,932] 1us 4mb                                                              |------------L0.?-------------| "
    - "**** Simulation run 27, type=split(VerticalSplit)(split_times=[355, 629]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.19[76,932] 1.01us     |-----------------------------------------L0.19------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[76,355] 1.01us 3mb  |-----------L0.?------------|                                                             "
    - "L0.?[356,629] 1.01us 3mb                              |-----------L0.?-----------|                                 "
    - "L0.?[630,932] 1.01us 4mb                                                           |------------L0.?-------------| "
    - "**** Simulation run 28, type=split(VerticalSplit)(split_times=[355, 629]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.23[76,932] 1.01us     |-----------------------------------------L0.23------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[76,355] 1.01us 3mb  |-----------L0.?------------|                                                             "
    - "L0.?[356,629] 1.01us 3mb                              |-----------L0.?-----------|                                 "
    - "L0.?[630,932] 1.01us 4mb                                                           |------------L0.?-------------| "
    - "**** Simulation run 29, type=split(VerticalSplit)(split_times=[355, 629]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.27[76,932] 1.02us     |-----------------------------------------L0.27------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[76,355] 1.02us 3mb  |-----------L0.?------------|                                                             "
    - "L0.?[356,629] 1.02us 3mb                              |-----------L0.?-----------|                                 "
    - "L0.?[630,932] 1.02us 4mb                                                           |------------L0.?-------------| "
    - "**** Simulation run 30, type=split(VerticalSplit)(split_times=[355, 629]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.31[76,932] 1.02us     |-----------------------------------------L0.31------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[76,355] 1.02us 3mb  |-----------L0.?------------|                                                             "
    - "L0.?[356,629] 1.02us 3mb                              |-----------L0.?-----------|                                 "
    - "L0.?[630,932] 1.02us 4mb                                                           |------------L0.?-------------| "
    - "**** Simulation run 31, type=split(VerticalSplit)(split_times=[355, 629]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.35[76,932] 1.02us     |-----------------------------------------L0.35------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[76,355] 1.02us 3mb  |-----------L0.?------------|                                                             "
    - "L0.?[356,629] 1.02us 3mb                              |-----------L0.?-----------|                                 "
    - "L0.?[630,932] 1.02us 4mb                                                           |------------L0.?-------------| "
    - "**** Simulation run 32, type=split(VerticalSplit)(split_times=[355, 629]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.39[76,932] 1.03us     |-----------------------------------------L0.39------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[76,355] 1.03us 3mb  |-----------L0.?------------|                                                             "
    - "L0.?[356,629] 1.03us 3mb                              |-----------L0.?-----------|                                 "
    - "L0.?[630,932] 1.03us 4mb                                                           |------------L0.?-------------| "
    - "**** Simulation run 33, type=split(VerticalSplit)(split_times=[355, 629]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.43[76,932] 1.03us     |-----------------------------------------L0.43------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[76,355] 1.03us 3mb  |-----------L0.?------------|                                                             "
    - "L0.?[356,629] 1.03us 3mb                              |-----------L0.?-----------|                                 "
    - "L0.?[630,932] 1.03us 4mb                                                           |------------L0.?-------------| "
    - "**** Simulation run 34, type=split(VerticalSplit)(split_times=[355, 629]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.47[76,932] 1.04us     |-----------------------------------------L0.47------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[76,355] 1.04us 3mb  |-----------L0.?------------|                                                             "
    - "L0.?[356,629] 1.04us 3mb                              |-----------L0.?-----------|                                 "
    - "L0.?[630,932] 1.04us 4mb                                                           |------------L0.?-------------| "
    - "**** Simulation run 35, type=split(VerticalSplit)(split_times=[355, 629]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.51[76,932] 1.04us     |-----------------------------------------L0.51------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[76,355] 1.04us 3mb  |-----------L0.?------------|                                                             "
    - "L0.?[356,629] 1.04us 3mb                              |-----------L0.?-----------|                                 "
    - "L0.?[630,932] 1.04us 4mb                                                           |------------L0.?-------------| "
    - "**** Simulation run 36, type=split(VerticalSplit)(split_times=[355, 629]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.55[76,932] 1.04us     |-----------------------------------------L0.55------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[76,355] 1.04us 3mb  |-----------L0.?------------|                                                             "
    - "L0.?[356,629] 1.04us 3mb                              |-----------L0.?-----------|                                 "
    - "L0.?[630,932] 1.04us 4mb                                                           |------------L0.?-------------| "
    - "**** Simulation run 37, type=split(VerticalSplit)(split_times=[355, 629]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.59[76,932] 1.05us     |-----------------------------------------L0.59------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[76,355] 1.05us 3mb  |-----------L0.?------------|                                                             "
    - "L0.?[356,629] 1.05us 3mb                              |-----------L0.?-----------|                                 "
    - "L0.?[630,932] 1.05us 4mb                                                           |------------L0.?-------------| "
    - "**** Simulation run 38, type=split(VerticalSplit)(split_times=[355, 629]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.13[173,950] 1us       |-----------------------------------------L0.13------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[173,355] 1us 2mb    |-------L0.?--------|                                                                     "
    - "L0.?[356,629] 1us 4mb                         |------------L0.?-------------|                                      "
    - "L0.?[630,950] 1us 4mb                                                        |---------------L0.?----------------| "
    - "**** Simulation run 39, type=split(VerticalSplit)(split_times=[355, 629]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.17[173,950] 1.01us    |-----------------------------------------L0.17------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[173,355] 1.01us 2mb |-------L0.?--------|                                                                     "
    - "L0.?[356,629] 1.01us 4mb                      |------------L0.?-------------|                                      "
    - "L0.?[630,950] 1.01us 4mb                                                     |---------------L0.?----------------| "
    - "Committing partition 1:"
    - "  Soft Deleting 40 files: L0.11, L0.12, L0.13, L0.14, L0.15, L0.16, L0.17, L0.18, L0.19, L0.20, L0.22, L0.23, L0.24, L0.26, L0.27, L0.28, L0.30, L0.31, L0.32, L0.34, L0.35, L0.36, L0.38, L0.39, L0.40, L0.42, L0.43, L0.44, L0.46, L0.47, L0.48, L0.50, L0.51, L0.52, L0.54, L0.55, L0.56, L0.58, L0.59, L0.60"
    - "  Creating 108 files"
    - "**** Simulation run 40, type=split(VerticalSplit)(split_times=[355, 629]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.21[173,950] 1.01us    |-----------------------------------------L0.21------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[173,355] 1.01us 2mb |-------L0.?--------|                                                                     "
    - "L0.?[356,629] 1.01us 4mb                      |------------L0.?-------------|                                      "
    - "L0.?[630,950] 1.01us 4mb                                                     |---------------L0.?----------------| "
    - "**** Simulation run 41, type=split(VerticalSplit)(split_times=[355, 629]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.25[173,950] 1.01us    |-----------------------------------------L0.25------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[173,355] 1.01us 2mb |-------L0.?--------|                                                                     "
    - "L0.?[356,629] 1.01us 4mb                      |------------L0.?-------------|                                      "
    - "L0.?[630,950] 1.01us 4mb                                                     |---------------L0.?----------------| "
    - "**** Simulation run 42, type=split(VerticalSplit)(split_times=[355, 629]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.29[173,950] 1.02us    |-----------------------------------------L0.29------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[173,355] 1.02us 2mb |-------L0.?--------|                                                                     "
    - "L0.?[356,629] 1.02us 4mb                      |------------L0.?-------------|                                      "
    - "L0.?[630,950] 1.02us 4mb                                                     |---------------L0.?----------------| "
    - "**** Simulation run 43, type=split(VerticalSplit)(split_times=[355, 629]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.33[173,950] 1.02us    |-----------------------------------------L0.33------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[173,355] 1.02us 2mb |-------L0.?--------|                                                                     "
    - "L0.?[356,629] 1.02us 4mb                      |------------L0.?-------------|                                      "
    - "L0.?[630,950] 1.02us 4mb                                                     |---------------L0.?----------------| "
    - "**** Simulation run 44, type=split(VerticalSplit)(split_times=[355, 629]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.37[173,950] 1.03us    |-----------------------------------------L0.37------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[173,355] 1.03us 2mb |-------L0.?--------|                                                                     "
    - "L0.?[356,629] 1.03us 4mb                      |------------L0.?-------------|                                      "
    - "L0.?[630,950] 1.03us 4mb                                                     |---------------L0.?----------------| "
    - "**** Simulation run 45, type=split(VerticalSplit)(split_times=[355, 629]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.41[173,950] 1.03us    |-----------------------------------------L0.41------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[173,355] 1.03us 2mb |-------L0.?--------|                                                                     "
    - "L0.?[356,629] 1.03us 4mb                      |------------L0.?-------------|                                      "
    - "L0.?[630,950] 1.03us 4mb                                                     |---------------L0.?----------------| "
    - "**** Simulation run 46, type=split(VerticalSplit)(split_times=[355, 629]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.45[173,950] 1.03us    |-----------------------------------------L0.45------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[173,355] 1.03us 2mb |-------L0.?--------|                                                                     "
    - "L0.?[356,629] 1.03us 4mb                      |------------L0.?-------------|                                      "
    - "L0.?[630,950] 1.03us 4mb                                                     |---------------L0.?----------------| "
    - "**** Simulation run 47, type=split(VerticalSplit)(split_times=[355, 629]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.49[173,950] 1.04us    |-----------------------------------------L0.49------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[173,355] 1.04us 2mb |-------L0.?--------|                                                                     "
    - "L0.?[356,629] 1.04us 4mb                      |------------L0.?-------------|                                      "
    - "L0.?[630,950] 1.04us 4mb                                                     |---------------L0.?----------------| "
    - "**** Simulation run 48, type=split(VerticalSplit)(split_times=[355, 629]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.53[173,950] 1.04us    |-----------------------------------------L0.53------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[173,355] 1.04us 2mb |-------L0.?--------|                                                                     "
    - "L0.?[356,629] 1.04us 4mb                      |------------L0.?-------------|                                      "
    - "L0.?[630,950] 1.04us 4mb                                                     |---------------L0.?----------------| "
    - "**** Simulation run 49, type=split(VerticalSplit)(split_times=[355, 629]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.57[173,950] 1.05us    |-----------------------------------------L0.57------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[173,355] 1.05us 2mb |-------L0.?--------|                                                                     "
    - "L0.?[356,629] 1.05us 4mb                      |------------L0.?-------------|                                      "
    - "L0.?[630,950] 1.05us 4mb                                                     |---------------L0.?----------------| "
    - "Committing partition 1:"
    - "  Soft Deleting 10 files: L0.21, L0.25, L0.29, L0.33, L0.37, L0.41, L0.45, L0.49, L0.53, L0.57"
    - "  Creating 30 files"
    - "**** Simulation run 50, type=compact(ManySmallFiles). 10 Input Files, 35mb total:"
    - "L0                                                                                                                 "
    - "L0.124[76,355] 1us 3mb            |------------------------------------L0.124------------------------------------| "
    - "L0.61[42,355] 1us 3mb    |-----------------------------------------L0.61------------------------------------------|"
    - "L0.163[173,355] 1us 2mb                                       |----------------------L0.163----------------------| "
    - "L0.100[50,355] 1us 5mb     |---------------------------------------L0.100----------------------------------------| "
    - "L0.127[76,355] 1us 3mb            |------------------------------------L0.127------------------------------------| "
    - "L0.64[42,355] 1us 3mb    |-----------------------------------------L0.64------------------------------------------|"
    - "L0.166[173,355] 1.01us 2mb                                     |----------------------L0.166----------------------| "
    - "L0.102[50,355] 1.01us 5mb  |---------------------------------------L0.102----------------------------------------| "
    - "L0.130[76,355] 1.01us 3mb         |------------------------------------L0.130------------------------------------| "
    - "L0.67[42,355] 1.01us 3mb |-----------------------------------------L0.67------------------------------------------|"
    - "**** 1 Output Files (parquet_file_id not yet assigned), 35mb total:"
    - "L0, all files 35mb                                                                                                 "
    - "L0.?[42,355] 1.01us      |------------------------------------------L0.?------------------------------------------|"
    - "Committing partition 1:"
    - "  Soft Deleting 10 files: L0.61, L0.64, L0.67, L0.100, L0.102, L0.124, L0.127, L0.130, L0.163, L0.166"
    - "  Creating 1 files"
    - "**** Simulation run 51, type=compact(ManySmallFiles). 10 Input Files, 36mb total:"
    - "L0                                                                                                                 "
    - "L0.169[173,355] 1.01us 2mb                                     |----------------------L0.169----------------------| "
    - "L0.104[50,355] 1.01us 5mb  |---------------------------------------L0.104----------------------------------------| "
    - "L0.133[76,355] 1.01us 3mb         |------------------------------------L0.133------------------------------------| "
    - "L0.70[42,355] 1.01us 3mb |-----------------------------------------L0.70------------------------------------------|"
    - "L0.172[173,355] 1.01us 2mb                                     |----------------------L0.172----------------------| "
    - "L0.106[50,355] 1.01us 5mb  |---------------------------------------L0.106----------------------------------------| "
    - "L0.136[76,355] 1.02us 3mb         |------------------------------------L0.136------------------------------------| "
    - "L0.73[42,355] 1.02us 3mb |-----------------------------------------L0.73------------------------------------------|"
    - "L0.175[173,355] 1.02us 2mb                                     |----------------------L0.175----------------------| "
    - "L0.108[50,355] 1.02us 5mb  |---------------------------------------L0.108----------------------------------------| "
    - "**** 1 Output Files (parquet_file_id not yet assigned), 36mb total:"
    - "L0, all files 36mb                                                                                                 "
    - "L0.?[42,355] 1.02us      |------------------------------------------L0.?------------------------------------------|"
    - "Committing partition 1:"
    - "  Soft Deleting 10 files: L0.70, L0.73, L0.104, L0.106, L0.108, L0.133, L0.136, L0.169, L0.172, L0.175"
    - "  Creating 1 files"
    - "**** Simulation run 52, type=compact(ManySmallFiles). 10 Input Files, 35mb total:"
    - "L0                                                                                                                 "
    - "L0.139[76,355] 1.02us 3mb         |------------------------------------L0.139------------------------------------| "
    - "L0.76[42,355] 1.02us 3mb |-----------------------------------------L0.76------------------------------------------|"
    - "L0.178[173,355] 1.02us 2mb                                     |----------------------L0.178----------------------| "
    - "L0.110[50,355] 1.02us 5mb  |---------------------------------------L0.110----------------------------------------| "
    - "L0.142[76,355] 1.02us 3mb         |------------------------------------L0.142------------------------------------| "
    - "L0.79[42,355] 1.02us 3mb |-----------------------------------------L0.79------------------------------------------|"
    - "L0.181[173,355] 1.03us 2mb                                     |----------------------L0.181----------------------| "
    - "L0.112[50,355] 1.03us 5mb  |---------------------------------------L0.112----------------------------------------| "
    - "L0.145[76,355] 1.03us 3mb         |------------------------------------L0.145------------------------------------| "
    - "L0.82[42,355] 1.03us 3mb |-----------------------------------------L0.82------------------------------------------|"
    - "**** 1 Output Files (parquet_file_id not yet assigned), 35mb total:"
    - "L0, all files 35mb                                                                                                 "
    - "L0.?[42,355] 1.03us      |------------------------------------------L0.?------------------------------------------|"
    - "Committing partition 1:"
    - "  Soft Deleting 10 files: L0.76, L0.79, L0.82, L0.110, L0.112, L0.139, L0.142, L0.145, L0.178, L0.181"
    - "  Creating 1 files"
    - "**** Simulation run 53, type=compact(ManySmallFiles). 10 Input Files, 36mb total:"
    - "L0                                                                                                                 "
    - "L0.184[173,355] 1.03us 2mb                                     |----------------------L0.184----------------------| "
    - "L0.114[50,355] 1.03us 5mb  |---------------------------------------L0.114----------------------------------------| "
    - "L0.148[76,355] 1.03us 3mb         |------------------------------------L0.148------------------------------------| "
    - "L0.85[42,355] 1.03us 3mb |-----------------------------------------L0.85------------------------------------------|"
    - "L0.187[173,355] 1.03us 2mb                                     |----------------------L0.187----------------------| "
    - "L0.116[50,355] 1.03us 5mb  |---------------------------------------L0.116----------------------------------------| "
    - "L0.151[76,355] 1.04us 3mb         |------------------------------------L0.151------------------------------------| "
    - "L0.88[42,355] 1.04us 3mb |-----------------------------------------L0.88------------------------------------------|"
    - "L0.190[173,355] 1.04us 2mb                                     |----------------------L0.190----------------------| "
    - "L0.118[50,355] 1.04us 5mb  |---------------------------------------L0.118----------------------------------------| "
    - "**** 1 Output Files (parquet_file_id not yet assigned), 36mb total:"
    - "L0, all files 36mb                                                                                                 "
    - "L0.?[42,355] 1.04us      |------------------------------------------L0.?------------------------------------------|"
    - "Committing partition 1:"
    - "  Soft Deleting 10 files: L0.85, L0.88, L0.114, L0.116, L0.118, L0.148, L0.151, L0.184, L0.187, L0.190"
    - "  Creating 1 files"
    - "**** Simulation run 54, type=compact(ManySmallFiles). 10 Input Files, 35mb total:"
    - "L0                                                                                                                 "
    - "L0.154[76,355] 1.04us 3mb         |------------------------------------L0.154------------------------------------| "
    - "L0.91[42,355] 1.04us 3mb |-----------------------------------------L0.91------------------------------------------|"
    - "L0.193[173,355] 1.04us 2mb                                     |----------------------L0.193----------------------| "
    - "L0.120[50,355] 1.04us 5mb  |---------------------------------------L0.120----------------------------------------| "
    - "L0.157[76,355] 1.04us 3mb         |------------------------------------L0.157------------------------------------| "
    - "L0.94[42,355] 1.05us 3mb |-----------------------------------------L0.94------------------------------------------|"
    - "L0.196[173,355] 1.05us 2mb                                     |----------------------L0.196----------------------| "
    - "L0.122[50,355] 1.05us 5mb  |---------------------------------------L0.122----------------------------------------| "
    - "L0.160[76,355] 1.05us 3mb         |------------------------------------L0.160------------------------------------| "
    - "L0.97[42,355] 1.05us 3mb |-----------------------------------------L0.97------------------------------------------|"
    - "**** 1 Output Files (parquet_file_id not yet assigned), 35mb total:"
    - "L0, all files 35mb                                                                                                 "
    - "L0.?[42,355] 1.05us      |------------------------------------------L0.?------------------------------------------|"
    - "Committing partition 1:"
    - "  Soft Deleting 10 files: L0.91, L0.94, L0.97, L0.120, L0.122, L0.154, L0.157, L0.160, L0.193, L0.196"
    - "  Creating 1 files"
    - "**** Simulation run 55, type=compact(ManySmallFiles). 10 Input Files, 35mb total:"
    - "L0                                                                                                                 "
    - "L0.125[356,629] 1us 3mb  |-----------------------------------------L0.125-----------------------------------------|"
    - "L0.62[356,629] 1us 3mb   |-----------------------------------------L0.62------------------------------------------|"
    - "L0.164[356,629] 1us 4mb  |-----------------------------------------L0.164-----------------------------------------|"
    - "L0.101[356,629] 1us 5mb  |-----------------------------------------L0.101-----------------------------------------|"
    - "L0.128[356,629] 1us 3mb  |-----------------------------------------L0.128-----------------------------------------|"
    - "L0.65[356,629] 1us 3mb   |-----------------------------------------L0.65------------------------------------------|"
    - "L0.167[356,629] 1.01us 4mb|-----------------------------------------L0.167-----------------------------------------|"
    - "L0.103[356,629] 1.01us 5mb|-----------------------------------------L0.103-----------------------------------------|"
    - "L0.131[356,629] 1.01us 3mb|-----------------------------------------L0.131-----------------------------------------|"
    - "L0.68[356,629] 1.01us 3mb|-----------------------------------------L0.68------------------------------------------|"
    - "**** 1 Output Files (parquet_file_id not yet assigned), 35mb total:"
    - "L0, all files 35mb                                                                                                 "
    - "L0.?[356,629] 1.01us     |------------------------------------------L0.?------------------------------------------|"
    - "Committing partition 1:"
    - "  Soft Deleting 10 files: L0.62, L0.65, L0.68, L0.101, L0.103, L0.125, L0.128, L0.131, L0.164, L0.167"
    - "  Creating 1 files"
    - "**** Simulation run 56, type=compact(ManySmallFiles). 10 Input Files, 37mb total:"
    - "L0                                                                                                                 "
    - "L0.170[356,629] 1.01us 4mb|-----------------------------------------L0.170-----------------------------------------|"
    - "L0.105[356,629] 1.01us 5mb|-----------------------------------------L0.105-----------------------------------------|"
    - "L0.134[356,629] 1.01us 3mb|-----------------------------------------L0.134-----------------------------------------|"
    - "L0.71[356,629] 1.01us 3mb|-----------------------------------------L0.71------------------------------------------|"
    - "L0.173[356,629] 1.01us 4mb|-----------------------------------------L0.173-----------------------------------------|"
    - "L0.107[356,629] 1.01us 5mb|-----------------------------------------L0.107-----------------------------------------|"
    - "L0.137[356,629] 1.02us 3mb|-----------------------------------------L0.137-----------------------------------------|"
    - "L0.74[356,629] 1.02us 3mb|-----------------------------------------L0.74------------------------------------------|"
    - "L0.176[356,629] 1.02us 4mb|-----------------------------------------L0.176-----------------------------------------|"
    - "L0.109[356,629] 1.02us 5mb|-----------------------------------------L0.109-----------------------------------------|"
    - "**** 1 Output Files (parquet_file_id not yet assigned), 37mb total:"
    - "L0, all files 37mb                                                                                                 "
    - "L0.?[356,629] 1.02us     |------------------------------------------L0.?------------------------------------------|"
    - "Committing partition 1:"
    - "  Soft Deleting 10 files: L0.71, L0.74, L0.105, L0.107, L0.109, L0.134, L0.137, L0.170, L0.173, L0.176"
    - "  Creating 1 files"
    - "**** Simulation run 57, type=compact(ManySmallFiles). 10 Input Files, 35mb total:"
    - "L0                                                                                                                 "
    - "L0.140[356,629] 1.02us 3mb|-----------------------------------------L0.140-----------------------------------------|"
    - "L0.77[356,629] 1.02us 3mb|-----------------------------------------L0.77------------------------------------------|"
    - "L0.179[356,629] 1.02us 4mb|-----------------------------------------L0.179-----------------------------------------|"
    - "L0.111[356,629] 1.02us 5mb|-----------------------------------------L0.111-----------------------------------------|"
    - "L0.143[356,629] 1.02us 3mb|-----------------------------------------L0.143-----------------------------------------|"
    - "L0.80[356,629] 1.02us 3mb|-----------------------------------------L0.80------------------------------------------|"
    - "L0.182[356,629] 1.03us 4mb|-----------------------------------------L0.182-----------------------------------------|"
    - "L0.113[356,629] 1.03us 5mb|-----------------------------------------L0.113-----------------------------------------|"
    - "L0.146[356,629] 1.03us 3mb|-----------------------------------------L0.146-----------------------------------------|"
    - "L0.83[356,629] 1.03us 3mb|-----------------------------------------L0.83------------------------------------------|"
    - "**** 1 Output Files (parquet_file_id not yet assigned), 35mb total:"
    - "L0, all files 35mb                                                                                                 "
    - "L0.?[356,629] 1.03us     |------------------------------------------L0.?------------------------------------------|"
    - "Committing partition 1:"
    - "  Soft Deleting 10 files: L0.77, L0.80, L0.83, L0.111, L0.113, L0.140, L0.143, L0.146, L0.179, L0.182"
    - "  Creating 1 files"
    - "**** Simulation run 58, type=compact(ManySmallFiles). 10 Input Files, 37mb total:"
    - "L0                                                                                                                 "
    - "L0.185[356,629] 1.03us 4mb|-----------------------------------------L0.185-----------------------------------------|"
    - "L0.115[356,629] 1.03us 5mb|-----------------------------------------L0.115-----------------------------------------|"
    - "L0.149[356,629] 1.03us 3mb|-----------------------------------------L0.149-----------------------------------------|"
    - "L0.86[356,629] 1.03us 3mb|-----------------------------------------L0.86------------------------------------------|"
    - "L0.188[356,629] 1.03us 4mb|-----------------------------------------L0.188-----------------------------------------|"
    - "L0.117[356,629] 1.03us 5mb|-----------------------------------------L0.117-----------------------------------------|"
    - "L0.152[356,629] 1.04us 3mb|-----------------------------------------L0.152-----------------------------------------|"
    - "L0.89[356,629] 1.04us 3mb|-----------------------------------------L0.89------------------------------------------|"
    - "L0.191[356,629] 1.04us 4mb|-----------------------------------------L0.191-----------------------------------------|"
    - "L0.119[356,629] 1.04us 5mb|-----------------------------------------L0.119-----------------------------------------|"
    - "**** 1 Output Files (parquet_file_id not yet assigned), 37mb total:"
    - "L0, all files 37mb                                                                                                 "
    - "L0.?[356,629] 1.04us     |------------------------------------------L0.?------------------------------------------|"
    - "Committing partition 1:"
    - "  Soft Deleting 10 files: L0.86, L0.89, L0.115, L0.117, L0.119, L0.149, L0.152, L0.185, L0.188, L0.191"
    - "  Creating 1 files"
    - "**** Simulation run 59, type=compact(ManySmallFiles). 10 Input Files, 35mb total:"
    - "L0                                                                                                                 "
    - "L0.155[356,629] 1.04us 3mb|-----------------------------------------L0.155-----------------------------------------|"
    - "L0.92[356,629] 1.04us 3mb|-----------------------------------------L0.92------------------------------------------|"
    - "L0.194[356,629] 1.04us 4mb|-----------------------------------------L0.194-----------------------------------------|"
    - "L0.121[356,629] 1.04us 5mb|-----------------------------------------L0.121-----------------------------------------|"
    - "L0.158[356,629] 1.04us 3mb|-----------------------------------------L0.158-----------------------------------------|"
    - "L0.95[356,629] 1.05us 3mb|-----------------------------------------L0.95------------------------------------------|"
    - "L0.197[356,629] 1.05us 4mb|-----------------------------------------L0.197-----------------------------------------|"
    - "L0.123[356,629] 1.05us 5mb|-----------------------------------------L0.123-----------------------------------------|"
    - "L0.161[356,629] 1.05us 3mb|-----------------------------------------L0.161-----------------------------------------|"
    - "L0.98[356,629] 1.05us 3mb|-----------------------------------------L0.98------------------------------------------|"
    - "**** 1 Output Files (parquet_file_id not yet assigned), 35mb total:"
    - "L0, all files 35mb                                                                                                 "
    - "L0.?[356,629] 1.05us     |------------------------------------------L0.?------------------------------------------|"
    - "Committing partition 1:"
    - "  Soft Deleting 10 files: L0.92, L0.95, L0.98, L0.121, L0.123, L0.155, L0.158, L0.161, L0.194, L0.197"
    - "  Creating 1 files"
    - "**** Simulation run 60, type=compact(ManySmallFiles). 10 Input Files, 38mb total:"
    - "L0                                                                                                                 "
    - "L0.126[630,932] 1us 4mb  |----------------------------------L0.126----------------------------------|              "
    - "L0.63[630,986] 1us 4mb   |-----------------------------------------L0.63------------------------------------------|"
    - "L0.165[630,950] 1us 4mb  |------------------------------------L0.165------------------------------------|          "
    - "L0.129[630,932] 1us 4mb  |----------------------------------L0.129----------------------------------|              "
    - "L0.66[630,986] 1us 4mb   |-----------------------------------------L0.66------------------------------------------|"
    - "L0.168[630,950] 1.01us 4mb|------------------------------------L0.168------------------------------------|          "
    - "L0.132[630,932] 1.01us 4mb|----------------------------------L0.132----------------------------------|              "
    - "L0.69[630,986] 1.01us 4mb|-----------------------------------------L0.69------------------------------------------|"
    - "L0.171[630,950] 1.01us 4mb|------------------------------------L0.171------------------------------------|          "
    - "L0.135[630,932] 1.01us 4mb|----------------------------------L0.135----------------------------------|              "
    - "**** 1 Output Files (parquet_file_id not yet assigned), 38mb total:"
    - "L0, all files 38mb                                                                                                 "
    - "L0.?[630,986] 1.01us     |------------------------------------------L0.?------------------------------------------|"
    - "Committing partition 1:"
    - "  Soft Deleting 10 files: L0.63, L0.66, L0.69, L0.126, L0.129, L0.132, L0.135, L0.165, L0.168, L0.171"
    - "  Creating 1 files"
    - "**** Simulation run 61, type=compact(ManySmallFiles). 10 Input Files, 38mb total:"
    - "L0                                                                                                                 "
    - "L0.72[630,986] 1.01us 4mb|-----------------------------------------L0.72------------------------------------------|"
    - "L0.174[630,950] 1.01us 4mb|------------------------------------L0.174------------------------------------|          "
    - "L0.138[630,932] 1.02us 4mb|----------------------------------L0.138----------------------------------|              "
    - "L0.75[630,986] 1.02us 4mb|-----------------------------------------L0.75------------------------------------------|"
    - "L0.177[630,950] 1.02us 4mb|------------------------------------L0.177------------------------------------|          "
    - "L0.141[630,932] 1.02us 4mb|----------------------------------L0.141----------------------------------|              "
    - "L0.78[630,986] 1.02us 4mb|-----------------------------------------L0.78------------------------------------------|"
    - "L0.180[630,950] 1.02us 4mb|------------------------------------L0.180------------------------------------|          "
    - "L0.144[630,932] 1.02us 4mb|----------------------------------L0.144----------------------------------|              "
    - "L0.81[630,986] 1.02us 4mb|-----------------------------------------L0.81------------------------------------------|"
    - "**** 1 Output Files (parquet_file_id not yet assigned), 38mb total:"
    - "L0, all files 38mb                                                                                                 "
    - "L0.?[630,986] 1.02us     |------------------------------------------L0.?------------------------------------------|"
    - "Committing partition 1:"
    - "  Soft Deleting 10 files: L0.72, L0.75, L0.78, L0.81, L0.138, L0.141, L0.144, L0.174, L0.177, L0.180"
    - "  Creating 1 files"
    - "**** Simulation run 62, type=compact(ManySmallFiles). 10 Input Files, 38mb total:"
    - "L0                                                                                                                 "
    - "L0.183[630,950] 1.03us 4mb|------------------------------------L0.183------------------------------------|          "
    - "L0.147[630,932] 1.03us 4mb|----------------------------------L0.147----------------------------------|              "
    - "L0.84[630,986] 1.03us 4mb|-----------------------------------------L0.84------------------------------------------|"
    - "L0.186[630,950] 1.03us 4mb|------------------------------------L0.186------------------------------------|          "
    - "L0.150[630,932] 1.03us 4mb|----------------------------------L0.150----------------------------------|              "
    - "L0.87[630,986] 1.03us 4mb|-----------------------------------------L0.87------------------------------------------|"
    - "L0.189[630,950] 1.03us 4mb|------------------------------------L0.189------------------------------------|          "
    - "L0.153[630,932] 1.04us 4mb|----------------------------------L0.153----------------------------------|              "
    - "L0.90[630,986] 1.04us 4mb|-----------------------------------------L0.90------------------------------------------|"
    - "L0.192[630,950] 1.04us 4mb|------------------------------------L0.192------------------------------------|          "
    - "**** 1 Output Files (parquet_file_id not yet assigned), 38mb total:"
    - "L0, all files 38mb                                                                                                 "
    - "L0.?[630,986] 1.04us     |------------------------------------------L0.?------------------------------------------|"
    - "Committing partition 1:"
    - "  Soft Deleting 10 files: L0.84, L0.87, L0.90, L0.147, L0.150, L0.153, L0.183, L0.186, L0.189, L0.192"
    - "  Creating 1 files"
    - "**** Simulation run 63, type=compact(ManySmallFiles). 8 Input Files, 30mb total:"
    - "L0                                                                                                                 "
    - "L0.156[630,932] 1.04us 4mb|----------------------------------L0.156----------------------------------|              "
    - "L0.93[630,986] 1.04us 4mb|-----------------------------------------L0.93------------------------------------------|"
    - "L0.195[630,950] 1.04us 4mb|------------------------------------L0.195------------------------------------|          "
    - "L0.159[630,932] 1.04us 4mb|----------------------------------L0.159----------------------------------|              "
    - "L0.96[630,986] 1.05us 4mb|-----------------------------------------L0.96------------------------------------------|"
    - "L0.198[630,950] 1.05us 4mb|------------------------------------L0.198------------------------------------|          "
    - "L0.162[630,932] 1.05us 4mb|----------------------------------L0.162----------------------------------|              "
    - "L0.99[630,986] 1.05us 4mb|-----------------------------------------L0.99------------------------------------------|"
    - "**** 1 Output Files (parquet_file_id not yet assigned), 30mb total:"
    - "L0, all files 30mb                                                                                                 "
    - "L0.?[630,986] 1.05us     |------------------------------------------L0.?------------------------------------------|"
    - "Committing partition 1:"
    - "  Soft Deleting 8 files: L0.93, L0.96, L0.99, L0.156, L0.159, L0.162, L0.195, L0.198"
    - "  Creating 1 files"
    - "**** Simulation run 64, type=split(CompactAndSplitOutput(TotalSizeLessThanMaxCompactSize))(split_times=[219]). 5 Input Files, 177mb total:"
    - "L0                                                                                                                 "
    - "L0.203[42,355] 1.05us 35mb|-----------------------------------------L0.203-----------------------------------------|"
    - "L0.202[42,355] 1.04us 36mb|-----------------------------------------L0.202-----------------------------------------|"
    - "L0.201[42,355] 1.03us 35mb|-----------------------------------------L0.201-----------------------------------------|"
    - "L0.200[42,355] 1.02us 36mb|-----------------------------------------L0.200-----------------------------------------|"
    - "L0.199[42,355] 1.01us 35mb|-----------------------------------------L0.199-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 177mb total:"
    - "L1                                                                                                                 "
    - "L1.?[42,219] 1.05us 100mb|----------------------L1.?----------------------|                                        "
    - "L1.?[220,355] 1.05us 77mb                                                   |----------------L1.?----------------| "
    - "Committing partition 1:"
    - "  Soft Deleting 5 files: L0.199, L0.200, L0.201, L0.202, L0.203"
    - "  Creating 2 files"
    - "**** Simulation run 65, type=split(CompactAndSplitOutput(TotalSizeLessThanMaxCompactSize))(split_times=[510]). 5 Input Files, 178mb total:"
    - "L0                                                                                                                 "
    - "L0.208[356,629] 1.05us 35mb|-----------------------------------------L0.208-----------------------------------------|"
    - "L0.207[356,629] 1.04us 37mb|-----------------------------------------L0.207-----------------------------------------|"
    - "L0.206[356,629] 1.03us 35mb|-----------------------------------------L0.206-----------------------------------------|"
    - "L0.205[356,629] 1.02us 37mb|-----------------------------------------L0.205-----------------------------------------|"
    - "L0.204[356,629] 1.01us 35mb|-----------------------------------------L0.204-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 178mb total:"
    - "L1                                                                                                                 "
    - "L1.?[356,510] 1.05us 101mb|----------------------L1.?----------------------|                                        "
    - "L1.?[511,629] 1.05us 77mb                                                   |----------------L1.?----------------| "
    - "Committing partition 1:"
    - "  Soft Deleting 5 files: L0.204, L0.205, L0.206, L0.207, L0.208"
    - "  Creating 2 files"
    - "**** Simulation run 66, type=split(CompactAndSplitOutput(TotalSizeLessThanMaxCompactSize))(split_times=[877]). 4 Input Files, 145mb total:"
    - "L0                                                                                                                 "
    - "L0.212[630,986] 1.05us 30mb|-----------------------------------------L0.212-----------------------------------------|"
    - "L0.211[630,986] 1.04us 38mb|-----------------------------------------L0.211-----------------------------------------|"
    - "L0.210[630,986] 1.02us 38mb|-----------------------------------------L0.210-----------------------------------------|"
    - "L0.209[630,986] 1.01us 38mb|-----------------------------------------L0.209-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 145mb total:"
    - "L1                                                                                                                 "
    - "L1.?[630,877] 1.05us 100mb|----------------------------L1.?----------------------------|                            "
    - "L1.?[878,986] 1.05us 44mb                                                              |----------L1.?-----------| "
    - "Committing partition 1:"
    - "  Soft Deleting 4 files: L0.209, L0.210, L0.211, L0.212"
    - "  Creating 2 files"
    - "**** Simulation run 67, type=split(ReduceOverlap)(split_times=[899]). 1 Input Files, 44mb total:"
    - "L1, all files 44mb                                                                                                 "
    - "L1.218[878,986] 1.05us   |-----------------------------------------L1.218-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 44mb total:"
    - "L1                                                                                                                 "
    - "L1.?[878,899] 1.05us 9mb |-----L1.?------|                                                                         "
    - "L1.?[900,986] 1.05us 35mb                  |--------------------------------L1.?---------------------------------| "
    - "**** Simulation run 68, type=split(ReduceOverlap)(split_times=[699, 799]). 1 Input Files, 100mb total:"
    - "L1, all files 100mb                                                                                                "
    - "L1.217[630,877] 1.05us   |-----------------------------------------L1.217-----------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 100mb total:"
    - "L1                                                                                                                 "
    - "L1.?[630,699] 1.05us 28mb|---------L1.?----------|                                                                 "
    - "L1.?[700,799] 1.05us 41mb                         |---------------L1.?---------------|                             "
    - "L1.?[800,877] 1.05us 32mb                                                             |-----------L1.?-----------| "
    - "**** Simulation run 69, type=split(ReduceOverlap)(split_times=[599]). 1 Input Files, 77mb total:"
    - "L1, all files 77mb                                                                                                 "
    - "L1.216[511,629] 1.05us   |-----------------------------------------L1.216-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 77mb total:"
    - "L1                                                                                                                 "
    - "L1.?[511,599] 1.05us 58mb|------------------------------L1.?-------------------------------|                       "
    - "L1.?[600,629] 1.05us 20mb                                                                   |--------L1.?--------| "
    - "**** Simulation run 70, type=split(ReduceOverlap)(split_times=[399, 499]). 1 Input Files, 101mb total:"
    - "L1, all files 101mb                                                                                                "
    - "L1.215[356,510] 1.05us   |-----------------------------------------L1.215-----------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 101mb total:"
    - "L1                                                                                                                 "
    - "L1.?[356,399] 1.05us 29mb|---------L1.?----------|                                                                 "
    - "L1.?[400,499] 1.05us 65mb                         |-------------------------L1.?--------------------------|        "
    - "L1.?[500,510] 1.05us 7mb                                                                                     |L1.?|"
    - "**** Simulation run 71, type=split(ReduceOverlap)(split_times=[299]). 1 Input Files, 77mb total:"
    - "L1, all files 77mb                                                                                                 "
    - "L1.214[220,355] 1.05us   |-----------------------------------------L1.214-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 77mb total:"
    - "L1                                                                                                                 "
    - "L1.?[220,299] 1.05us 45mb|-----------------------L1.?-----------------------|                                      "
    - "L1.?[300,355] 1.05us 32mb                                                     |---------------L1.?---------------| "
    - "**** Simulation run 72, type=split(ReduceOverlap)(split_times=[99, 199]). 1 Input Files, 100mb total:"
    - "L1, all files 100mb                                                                                                "
    - "L1.213[42,219] 1.05us    |-----------------------------------------L1.213-----------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 100mb total:"
    - "L1                                                                                                                 "
    - "L1.?[42,99] 1.05us 33mb  |-----------L1.?-----------|                                                              "
    - "L1.?[100,199] 1.05us 56mb                             |----------------------L1.?----------------------|           "
    - "L1.?[200,219] 1.05us 11mb                                                                                |-L1.?--| "
    - "Committing partition 1:"
    - "  Soft Deleting 6 files: L1.213, L1.214, L1.215, L1.216, L1.217, L1.218"
    - "  Creating 15 files"
    - "**** Simulation run 73, type=split(CompactAndSplitOutput(FoundSubsetLessThanMaxCompactSize))(split_times=[69, 138]). 4 Input Files, 289mb total:"
    - "L1                                                                                                                 "
    - "L1.231[42,99] 1.05us 33mb                  |--------L1.231---------|                                               "
    - "L1.232[100,199] 1.05us 56mb                                             |------------------L1.232------------------| "
    - "L2                                                                                                                 "
    - "L2.1[0,99] 99ns 100mb    |-------------------L2.1-------------------|                                              "
    - "L2.2[100,199] 199ns 100mb                                             |-------------------L2.2-------------------| "
    - "**** 3 Output Files (parquet_file_id not yet assigned), 289mb total:"
    - "L2                                                                                                                 "
    - "L2.?[0,69] 1.05us 101mb  |------------L2.?-------------|                                                           "
    - "L2.?[70,138] 1.05us 100mb                               |------------L2.?------------|                             "
    - "L2.?[139,199] 1.05us 88mb                                                              |----------L2.?-----------| "
    - "Committing partition 1:"
    - "  Soft Deleting 4 files: L2.1, L2.2, L1.231, L1.232"
    - "  Creating 3 files"
    - "**** Simulation run 74, type=split(CompactAndSplitOutput(FoundSubsetLessThanMaxCompactSize))(split_times=[264]). 3 Input Files, 156mb total:"
    - "L1                                                                                                                 "
    - "L1.233[200,219] 1.05us 11mb|----L1.233-----|                                                                         "
    - "L1.229[220,299] 1.05us 45mb                  |-------------------------------L1.229--------------------------------| "
    - "L2                                                                                                                 "
    - "L2.3[200,299] 299ns 100mb|-----------------------------------------L2.3------------------------------------------| "
    - "**** 2 Output Files (parquet_file_id not yet assigned), 156mb total:"
    - "L2                                                                                                                 "
    - "L2.?[200,264] 1.05us 102mb|--------------------------L2.?--------------------------|                                "
    - "L2.?[265,299] 1.05us 55mb                                                           |------------L2.?------------| "
    - "Committing partition 1:"
    - "  Soft Deleting 3 files: L2.3, L1.229, L1.233"
    - "  Creating 2 files"
    - "**** Simulation run 75, type=split(CompactAndSplitOutput(FoundSubsetLessThanMaxCompactSize))(split_times=[362]). 3 Input Files, 160mb total:"
    - "L1                                                                                                                 "
    - "L1.230[300,355] 1.05us 32mb|--------------------L1.230---------------------|                                         "
    - "L1.226[356,399] 1.05us 29mb                                                  |---------------L1.226----------------| "
    - "L2                                                                                                                 "
    - "L2.4[300,399] 399ns 100mb|-----------------------------------------L2.4------------------------------------------| "
    - "**** 2 Output Files (parquet_file_id not yet assigned), 160mb total:"
    - "L2                                                                                                                 "
    - "L2.?[300,362] 1.05us 101mb|-------------------------L2.?-------------------------|                                  "
    - "L2.?[363,399] 1.05us 59mb                                                         |-------------L2.?-------------| "
    - "Committing partition 1:"
    - "  Soft Deleting 3 files: L2.4, L1.226, L1.230"
    - "  Creating 2 files"
    - "**** Simulation run 76, type=split(CompactAndSplitOutput(FoundSubsetLessThanMaxCompactSize))(split_times=[460]). 2 Input Files, 165mb total:"
    - "L1                                                                                                                 "
    - "L1.227[400,499] 1.05us 65mb|----------------------------------------L1.227-----------------------------------------| "
    - "L2                                                                                                                 "
    - "L2.5[400,499] 499ns 100mb|-----------------------------------------L2.5------------------------------------------| "
    - "**** 2 Output Files (parquet_file_id not yet assigned), 165mb total:"
    - "L2                                                                                                                 "
    - "L2.?[400,460] 1.05us 101mb|------------------------L2.?------------------------|                                    "
    - "L2.?[461,499] 1.05us 64mb                                                       |--------------L2.?--------------| "
    - "Committing partition 1:"
    - "  Soft Deleting 2 files: L2.5, L1.227"
    - "  Creating 2 files"
    - "**** Simulation run 77, type=split(CompactAndSplitOutput(FoundSubsetLessThanMaxCompactSize))(split_times=[560]). 3 Input Files, 165mb total:"
    - "L1                                                                                                                 "
    - "L1.228[500,510] 1.05us 7mb|L1.228-|                                                                                 "
    - "L1.224[511,599] 1.05us 58mb          |------------------------------------L1.224------------------------------------|"
    - "L2                                                                                                                 "
    - "L2.6[500,599] 599ns 100mb|-----------------------------------------L2.6------------------------------------------| "
    - "**** 2 Output Files (parquet_file_id not yet assigned), 165mb total:"
    - "L2                                                                                                                 "
    - "L2.?[500,560] 1.05us 101mb|------------------------L2.?------------------------|                                    "
    - "L2.?[561,599] 1.05us 64mb                                                       |--------------L2.?--------------| "
    - "Committing partition 1:"
    - "  Soft Deleting 3 files: L2.6, L1.224, L1.228"
    - "  Creating 2 files"
    - "**** Simulation run 78, type=split(CompactAndSplitOutput(FoundSubsetLessThanMaxCompactSize))(split_times=[670, 740]). 5 Input Files, 288mb total:"
    - "L1                                                                                                                 "
    - "L1.225[600,629] 1.05us 20mb|--L1.225---|                                                                             "
    - "L1.221[630,699] 1.05us 28mb             |-----------L1.221------------|                                              "
    - "L1.222[700,799] 1.05us 41mb                                             |------------------L1.222------------------| "
    - "L2                                                                                                                 "
    - "L2.7[600,699] 699ns 100mb|-------------------L2.7-------------------|                                              "
    - "L2.8[700,799] 799ns 100mb                                             |-------------------L2.8-------------------| "
    - "**** 3 Output Files (parquet_file_id not yet assigned), 288mb total:"
    - "L2                                                                                                                 "
    - "L2.?[600,670] 1.05us 102mb|------------L2.?-------------|                                                           "
    - "L2.?[671,740] 1.05us 101mb                                |------------L2.?-------------|                           "
    - "L2.?[741,799] 1.05us 85mb                                                               |----------L2.?----------| "
    - "Committing partition 1:"
    - "  Soft Deleting 5 files: L2.7, L2.8, L1.221, L1.222, L1.225"
    - "  Creating 3 files"
    - "**** Final Output Files (3.15gb written)"
    - "L1                                                                                                                 "
    - "L1.219[878,899] 1.05us 9mb                                                                               |L1.219|   "
    - "L1.220[900,986] 1.05us 35mb                                                                                 |L1.220| "
    - "L1.223[800,877] 1.05us 32mb                                                                        |L1.223|          "
    - "L2                                                                                                                 "
    - "L2.9[800,899] 899ns 100mb                                                                        |-L2.9-|          "
    - "L2.10[900,999] 999ns 100mb                                                                                 |L2.10-| "
    - "L2.234[0,69] 1.05us 101mb|L2.234|                                                                                  "
    - "L2.235[70,138] 1.05us 100mb      |L2.235|                                                                            "
    - "L2.236[139,199] 1.05us 88mb            |L2.236|                                                                      "
    - "L2.237[200,264] 1.05us 102mb                  |L2.237|                                                                "
    - "L2.238[265,299] 1.05us 55mb                       |L2.238|                                                           "
    - "L2.239[300,362] 1.05us 101mb                           |L2.239|                                                       "
    - "L2.240[363,399] 1.05us 59mb                                |L2.240|                                                  "
    - "L2.241[400,460] 1.05us 101mb                                    |L2.241|                                              "
    - "L2.242[461,499] 1.05us 64mb                                         |L2.242|                                         "
    - "L2.243[500,560] 1.05us 101mb                                             |L2.243|                                     "
    - "L2.244[561,599] 1.05us 64mb                                                  |L2.244|                                "
    - "L2.245[600,670] 1.05us 102mb                                                      |L2.245|                            "
    - "L2.246[671,740] 1.05us 101mb                                                            |L2.246|                      "
    - "L2.247[741,799] 1.05us 85mb                                                                  |L2.247|                "
    - "**** Breakdown of where bytes were written"
    - 1.2gb written by split(CompactAndSplitOutput(FoundSubsetLessThanMaxCompactSize))
    - 500mb written by compact(ManySmallFiles)
    - 500mb written by split(CompactAndSplitOutput(TotalSizeLessThanMaxCompactSize))
    - 500mb written by split(ReduceOverlap)
    - 500mb written by split(VerticalSplit)
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
        .with_max_num_files_per_plan(20)
        .with_suppress_run_output()
        .with_writes_breakdown()
        .with_partition_timeout(Duration::from_secs(10))
        .build()
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(10000)
                .with_max_time(20000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(478836)
                .with_max_l0_created_at(Time::from_timestamp_nanos(3420000)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(30000)
                .with_max_time(40000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(474866)
                .with_max_l0_created_at(Time::from_timestamp_nanos(3420000)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(50000)
                .with_max_time(70000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(454768)
                .with_max_l0_created_at(Time::from_timestamp_nanos(3420000)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(60000)
                .with_max_time(490000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(277569)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1270000)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(80000)
                .with_max_time(250000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(473373)
                .with_max_l0_created_at(Time::from_timestamp_nanos(3420000)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(90000)
                .with_max_time(540000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(93159)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1410000)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(100000)
                .with_max_time(200000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(90782)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1530000)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(110000)
                .with_max_time(300000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(67575)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1720000)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(120000)
                .with_max_time(140000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(88947)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1910000)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(130000)
                .with_max_time(470000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(70227)
                .with_max_l0_created_at(Time::from_timestamp_nanos(2120000)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(150000)
                .with_max_time(510000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(77719)
                .with_max_l0_created_at(Time::from_timestamp_nanos(2240000)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(160000)
                .with_max_time(550000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(80887)
                .with_max_l0_created_at(Time::from_timestamp_nanos(2390000)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(170000)
                .with_max_time(560000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(89902)
                .with_max_l0_created_at(Time::from_timestamp_nanos(2580000)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(180000)
                .with_max_time(650000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(165529)
                .with_max_l0_created_at(Time::from_timestamp_nanos(2730000)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(190000)
                .with_max_time(680000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(135875)
                .with_max_l0_created_at(Time::from_timestamp_nanos(2960000)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(210000)
                .with_max_time(230000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(73234)
                .with_max_l0_created_at(Time::from_timestamp_nanos(3060000)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(220000)
                .with_max_time(240000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(41743)
                .with_max_l0_created_at(Time::from_timestamp_nanos(3110000)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(260000)
                .with_max_time(290000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(42785)
                .with_max_l0_created_at(Time::from_timestamp_nanos(3110000)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(270000)
                .with_max_time(520000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(452507)
                .with_max_l0_created_at(Time::from_timestamp_nanos(3420000)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(280000)
                .with_max_time(320000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(60040)
                .with_max_l0_created_at(Time::from_timestamp_nanos(3160000)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(310000)
                .with_max_time(340000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(60890)
                .with_max_l0_created_at(Time::from_timestamp_nanos(3190000)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(330000)
                .with_max_time(360000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(69687)
                .with_max_l0_created_at(Time::from_timestamp_nanos(3230000)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(350000)
                .with_max_time(380000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(59141)
                .with_max_l0_created_at(Time::from_timestamp_nanos(3250000)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(370000)
                .with_max_time(400000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(67287)
                .with_max_l0_created_at(Time::from_timestamp_nanos(3280000)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(390000)
                .with_max_time(420000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(53545)
                .with_max_l0_created_at(Time::from_timestamp_nanos(3350000)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(410000)
                .with_max_time(440000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(66218)
                .with_max_l0_created_at(Time::from_timestamp_nanos(3360000)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(430000)
                .with_max_time(460000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(96870)
                .with_max_l0_created_at(Time::from_timestamp_nanos(3400000)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(450000)
                .with_max_time(480000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(162922)
                .with_max_l0_created_at(Time::from_timestamp_nanos(3410000)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(500000)
                .with_max_time(710000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(741563)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1270000)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(530000)
                .with_max_time(620000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(452298)
                .with_max_l0_created_at(Time::from_timestamp_nanos(3420000)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(570000)
                .with_max_time(610000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(116428)
                .with_max_l0_created_at(Time::from_timestamp_nanos(3410000)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(580000)
                .with_max_time(700000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(469192)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1410000)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(590000)
                .with_max_time(930000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(118295625)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1530000)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(600000)
                .with_max_time(1020000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(21750626)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1720000)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(630000)
                .with_max_time(750000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(595460)
                .with_max_l0_created_at(Time::from_timestamp_nanos(3420000)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(640000)
                .with_max_time(740000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(188078)
                .with_max_l0_created_at(Time::from_timestamp_nanos(3410000)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(660000)
                .with_max_time(670000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(236787)
                .with_max_l0_created_at(Time::from_timestamp_nanos(2120000)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(690000)
                .with_max_time(730000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(7073)
                .with_max_l0_created_at(Time::from_timestamp_nanos(3060000)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(720000)
                .with_max_time(1170000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(271967233)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1270000)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(760000)
                .with_max_time(800000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(296649)
                .with_max_l0_created_at(Time::from_timestamp_nanos(3420000)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(770000)
                .with_max_time(770000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(6400)
                .with_max_l0_created_at(Time::from_timestamp_nanos(3110000)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(780000)
                .with_max_time(790000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(6673)
                .with_max_l0_created_at(Time::from_timestamp_nanos(3160000)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(810000)
                .with_max_time(1330000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(260832981)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1410000)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(820000)
                .with_max_time(830000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(108736)
                .with_max_l0_created_at(Time::from_timestamp_nanos(3420000)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(840000)
                .with_max_time(890000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(137579)
                .with_max_l0_created_at(Time::from_timestamp_nanos(3420000)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(850000)
                .with_max_time(860000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(6639)
                .with_max_l0_created_at(Time::from_timestamp_nanos(3190000)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(870000)
                .with_max_time(880000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(126187)
                .with_max_l0_created_at(Time::from_timestamp_nanos(3410000)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(900000)
                .with_max_time(970000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(158579)
                .with_max_l0_created_at(Time::from_timestamp_nanos(3420000)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(910000)
                .with_max_time(920000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(107298)
                .with_max_l0_created_at(Time::from_timestamp_nanos(3410000)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(940000)
                .with_max_time(1430000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(120508643)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1530000)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(950000)
                .with_max_time(960000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(196729)
                .with_max_l0_created_at(Time::from_timestamp_nanos(2240000)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(980000)
                .with_max_time(1110000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(110870)
                .with_max_l0_created_at(Time::from_timestamp_nanos(3420000)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(990000)
                .with_max_time(1090000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(93360)
                .with_max_l0_created_at(Time::from_timestamp_nanos(2120000)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1000000)
                .with_max_time(1040000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(6561)
                .with_max_l0_created_at(Time::from_timestamp_nanos(3250000)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1010000)
                .with_max_time(1060000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(68025)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1910000)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1030000)
                .with_max_time(1730000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(228950896)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1720000)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1050000)
                .with_max_time(1120000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(77925)
                .with_max_l0_created_at(Time::from_timestamp_nanos(2240000)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1070000)
                .with_max_time(1080000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(8237)
                .with_max_l0_created_at(Time::from_timestamp_nanos(2390000)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1100000)
                .with_max_time(1100000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(6400)
                .with_max_l0_created_at(Time::from_timestamp_nanos(3280000)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1130000)
                .with_max_time(1160000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(48975)
                .with_max_l0_created_at(Time::from_timestamp_nanos(2240000)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1140000)
                .with_max_time(1240000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(79883481)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1720000)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1150000)
                .with_max_time(1230000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(109212)
                .with_max_l0_created_at(Time::from_timestamp_nanos(3420000)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1160000)
                .with_max_time(1190000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(95486)
                .with_max_l0_created_at(Time::from_timestamp_nanos(2390000)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1180000)
                .with_max_time(1200000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(93781)
                .with_max_l0_created_at(Time::from_timestamp_nanos(2580000)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1210000)
                .with_max_time(1220000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(7448)
                .with_max_l0_created_at(Time::from_timestamp_nanos(2730000)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1250000)
                .with_max_time(1370000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(100265729)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1720000)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1260000)
                .with_max_time(1360000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(102711)
                .with_max_l0_created_at(Time::from_timestamp_nanos(3420000)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1280000)
                .with_max_time(1420000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(119202)
                .with_max_l0_created_at(Time::from_timestamp_nanos(2730000)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1290000)
                .with_max_time(1300000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(9027)
                .with_max_l0_created_at(Time::from_timestamp_nanos(2580000)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1310000)
                .with_max_time(1320000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(25565)
                .with_max_l0_created_at(Time::from_timestamp_nanos(3410000)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1340000)
                .with_max_time(1450000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(34519040)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1790000)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1350000)
                .with_max_time(1440000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(764185)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1910000)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1380000)
                .with_max_time(1580000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(71505278)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1720000)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1390000)
                .with_max_time(1590000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(183141)
                .with_max_l0_created_at(Time::from_timestamp_nanos(3420000)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1400000)
                .with_max_time(1540000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(6701)
                .with_max_l0_created_at(Time::from_timestamp_nanos(3360000)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1460000)
                .with_max_time(1550000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(65266955)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1790000)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1470000)
                .with_max_time(1600000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(21649346)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1910000)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1480000)
                .with_max_time(1500000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(55409)
                .with_max_l0_created_at(Time::from_timestamp_nanos(2730000)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1490000)
                .with_max_time(1570000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(74432)
                .with_max_l0_created_at(Time::from_timestamp_nanos(2960000)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1510000)
                .with_max_time(1520000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(23495)
                .with_max_l0_created_at(Time::from_timestamp_nanos(2240000)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1560000)
                .with_max_time(1600000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(55979589)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1790000)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1610000)
                .with_max_time(1630000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(1061014)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1910000)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1610000)
                .with_max_time(1710000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(46116292)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1790000)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1620000)
                .with_max_time(1670000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(43064)
                .with_max_l0_created_at(Time::from_timestamp_nanos(3420000)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1640000)
                .with_max_time(1740000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(99408169)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1910000)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1650000)
                .with_max_time(1700000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(50372)
                .with_max_l0_created_at(Time::from_timestamp_nanos(2960000)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1660000)
                .with_max_time(1680000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(14716604)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1720000)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1690000)
                .with_max_time(1810000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(172039)
                .with_max_l0_created_at(Time::from_timestamp_nanos(3420000)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1750000)
                .with_max_time(1800000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(136666078)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1910000)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1760000)
                .with_max_time(1780000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(189566)
                .with_max_l0_created_at(Time::from_timestamp_nanos(2730000)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1770000)
                .with_max_time(1820000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(47820008)
                .with_max_l0_created_at(Time::from_timestamp_nanos(2120000)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1830000)
                .with_max_time(1970000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(211523341)
                .with_max_l0_created_at(Time::from_timestamp_nanos(2120000)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1840000)
                .with_max_time(1960000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(159235)
                .with_max_l0_created_at(Time::from_timestamp_nanos(3420000)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1850000)
                .with_max_time(1950000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(14985821)
                .with_max_l0_created_at(Time::from_timestamp_nanos(2240000)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1860000)
                .with_max_time(1870000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(17799)
                .with_max_l0_created_at(Time::from_timestamp_nanos(2730000)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1880000)
                .with_max_time(1930000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(52964586)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1910000)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1890000)
                .with_max_time(1900000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(6420)
                .with_max_l0_created_at(Time::from_timestamp_nanos(3410000)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1920000)
                .with_max_time(1940000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(37185)
                .with_max_l0_created_at(Time::from_timestamp_nanos(2960000)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1980000)
                .with_max_time(2040000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(48661531)
                .with_max_l0_created_at(Time::from_timestamp_nanos(2120000)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1990000)
                .with_max_time(2060000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(104533)
                .with_max_l0_created_at(Time::from_timestamp_nanos(3420000)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(2000000)
                .with_max_time(2070000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(115840212)
                .with_max_l0_created_at(Time::from_timestamp_nanos(2240000)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(2010000)
                .with_max_time(2030000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(27386)
                .with_max_l0_created_at(Time::from_timestamp_nanos(2960000)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(2020000)
                .with_max_time(2050000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(6485)
                .with_max_l0_created_at(Time::from_timestamp_nanos(3410000)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(2080000)
                .with_max_time(2140000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(63573570)
                .with_max_l0_created_at(Time::from_timestamp_nanos(2240000)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(2090000)
                .with_max_time(2150000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(73119)
                .with_max_l0_created_at(Time::from_timestamp_nanos(3420000)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(2100000)
                .with_max_time(2110000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(6626)
                .with_max_l0_created_at(Time::from_timestamp_nanos(2390000)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(2130000)
                .with_max_time(2210000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(103699116)
                .with_max_l0_created_at(Time::from_timestamp_nanos(2390000)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(2160000)
                .with_max_time(2260000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(160045)
                .with_max_l0_created_at(Time::from_timestamp_nanos(3420000)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(2170000)
                .with_max_time(2200000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(47126)
                .with_max_l0_created_at(Time::from_timestamp_nanos(3400000)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(2180000)
                .with_max_time(2190000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(7923)
                .with_max_l0_created_at(Time::from_timestamp_nanos(3410000)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(2220000)
                .with_max_time(2250000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(116506120)
                .with_max_l0_created_at(Time::from_timestamp_nanos(2390000)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(2230000)
                .with_max_time(2280000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(122528493)
                .with_max_l0_created_at(Time::from_timestamp_nanos(2580000)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(2270000)
                .with_max_time(2340000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(42963)
                .with_max_l0_created_at(Time::from_timestamp_nanos(3420000)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(2290000)
                .with_max_time(2420000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(132343737)
                .with_max_l0_created_at(Time::from_timestamp_nanos(2580000)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(2300000)
                .with_max_time(2310000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(25526)
                .with_max_l0_created_at(Time::from_timestamp_nanos(3400000)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(2320000)
                .with_max_time(2440000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(52114677)
                .with_max_l0_created_at(Time::from_timestamp_nanos(2730000)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(2330000)
                .with_max_time(2370000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(80814)
                .with_max_l0_created_at(Time::from_timestamp_nanos(2730000)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(2350000)
                .with_max_time(2410000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(84586)
                .with_max_l0_created_at(Time::from_timestamp_nanos(3420000)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(2360000)
                .with_max_time(2380000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(31508)
                .with_max_l0_created_at(Time::from_timestamp_nanos(2960000)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(2400000)
                .with_max_time(2430000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(31292)
                .with_max_l0_created_at(Time::from_timestamp_nanos(3060000)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(2450000)
                .with_max_time(2550000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(169461420)
                .with_max_l0_created_at(Time::from_timestamp_nanos(2730000)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(2460000)
                .with_max_time(2490000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(32436)
                .with_max_l0_created_at(Time::from_timestamp_nanos(2580000)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(2470000)
                .with_max_time(2500000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(30783)
                .with_max_l0_created_at(Time::from_timestamp_nanos(3060000)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(2480000)
                .with_max_time(2540000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(116968)
                .with_max_l0_created_at(Time::from_timestamp_nanos(3420000)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(2510000)
                .with_max_time(2550000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(20831132)
                .with_max_l0_created_at(Time::from_timestamp_nanos(2730000)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(2520000)
                .with_max_time(2680000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(47079)
                .with_max_l0_created_at(Time::from_timestamp_nanos(3400000)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(2530000)
                .with_max_time(2670000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(8012)
                .with_max_l0_created_at(Time::from_timestamp_nanos(3410000)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(2560000)
                .with_max_time(2590000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(24905052)
                .with_max_l0_created_at(Time::from_timestamp_nanos(2730000)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(2560000)
                .with_max_time(2620000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(26916757)
                .with_max_l0_created_at(Time::from_timestamp_nanos(2730000)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(2570000)
                .with_max_time(2630000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(114015)
                .with_max_l0_created_at(Time::from_timestamp_nanos(3420000)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(2600000)
                .with_max_time(2700000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(184997646)
                .with_max_l0_created_at(Time::from_timestamp_nanos(2730000)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(2610000)
                .with_max_time(2640000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(10024382)
                .with_max_l0_created_at(Time::from_timestamp_nanos(2960000)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(2650000)
                .with_max_time(2700000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(11941889)
                .with_max_l0_created_at(Time::from_timestamp_nanos(2960000)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(2660000)
                .with_max_time(2690000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(45048)
                .with_max_l0_created_at(Time::from_timestamp_nanos(3420000)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(2710000)
                .with_max_time(2770000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(101521806)
                .with_max_l0_created_at(Time::from_timestamp_nanos(2960000)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(2710000)
                .with_max_time(2750000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(28959050)
                .with_max_l0_created_at(Time::from_timestamp_nanos(2730000)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(2720000)
                .with_max_time(2760000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(81663)
                .with_max_l0_created_at(Time::from_timestamp_nanos(3420000)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(2740000)
                .with_max_time(2810000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(30344109)
                .with_max_l0_created_at(Time::from_timestamp_nanos(3060000)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(2780000)
                .with_max_time(2830000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(125782713)
                .with_max_l0_created_at(Time::from_timestamp_nanos(2960000)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(2790000)
                .with_max_time(2840000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(109926)
                .with_max_l0_created_at(Time::from_timestamp_nanos(3420000)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(2800000)
                .with_max_time(2850000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(13486)
                .with_max_l0_created_at(Time::from_timestamp_nanos(3060000)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(2820000)
                .with_max_time(2860000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(34930420)
                .with_max_l0_created_at(Time::from_timestamp_nanos(3060000)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(2870000)
                .with_max_time(2980000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(179171551)
                .with_max_l0_created_at(Time::from_timestamp_nanos(3060000)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(2880000)
                .with_max_time(2910000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(27704)
                .with_max_l0_created_at(Time::from_timestamp_nanos(2960000)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(2890000)
                .with_max_time(2990000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(73478274)
                .with_max_l0_created_at(Time::from_timestamp_nanos(3060000)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(2900000)
                .with_max_time(2920000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(16412)
                .with_max_l0_created_at(Time::from_timestamp_nanos(3420000)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(2930000)
                .with_max_time(3390000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(99066230)
                .with_max_l0_created_at(Time::from_timestamp_nanos(3420000)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(2940000)
                .with_max_time(3210000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(55188)
                .with_max_l0_created_at(Time::from_timestamp_nanos(3280000)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(2950000)
                .with_max_time(3320000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(198938676)
                .with_max_l0_created_at(Time::from_timestamp_nanos(3350000)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(2970000)
                .with_max_time(3130000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(244238124)
                .with_max_l0_created_at(Time::from_timestamp_nanos(3110000)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(3000000)
                .with_max_time(3070000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(176463536)
                .with_max_l0_created_at(Time::from_timestamp_nanos(3060000)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(3010000)
                .with_max_time(3020000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(17116)
                .with_max_l0_created_at(Time::from_timestamp_nanos(3060000)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(3030000)
                .with_max_time(3040000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(9993)
                .with_max_l0_created_at(Time::from_timestamp_nanos(3250000)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(3050000)
                .with_max_time(3170000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(229578231)
                .with_max_l0_created_at(Time::from_timestamp_nanos(3160000)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(3080000)
                .with_max_time(3090000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(12831)
                .with_max_l0_created_at(Time::from_timestamp_nanos(3190000)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(3100000)
                .with_max_time(3200000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(222546135)
                .with_max_l0_created_at(Time::from_timestamp_nanos(3190000)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(3120000)
                .with_max_time(3140000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(25989)
                .with_max_l0_created_at(Time::from_timestamp_nanos(3410000)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(3150000)
                .with_max_time(3240000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(224750727)
                .with_max_l0_created_at(Time::from_timestamp_nanos(3230000)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(3180000)
                .with_max_time(3260000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(224562423)
                .with_max_l0_created_at(Time::from_timestamp_nanos(3250000)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(3220000)
                .with_max_time(3290000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(223130462)
                .with_max_l0_created_at(Time::from_timestamp_nanos(3280000)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(3270000)
                .with_max_time(3330000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(191981570)
                .with_max_l0_created_at(Time::from_timestamp_nanos(3360000)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(3300000)
                .with_max_time(3380000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(242123981)
                .with_max_l0_created_at(Time::from_timestamp_nanos(3400000)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(3310000)
                .with_max_time(3380000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(243511891)
                .with_max_l0_created_at(Time::from_timestamp_nanos(3410000)),
        )
        .await;
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(3340000)
                .with_max_time(3370000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_file_size_bytes(30538013)
                .with_max_l0_created_at(Time::from_timestamp_nanos(3360000)),
        )
        .await;

    insta::assert_yaml_snapshot!(
        run_layout_scenario(&setup).await,
        @r###"
    ---
    - "**** Input Files "
    - "L0                                                                                                                 "
    - "L0.1[10000,20000] 3.42ms 468kb|L0.1|                                                                                    "
    - "L0.2[30000,40000] 3.42ms 464kb|L0.2|                                                                                    "
    - "L0.3[50000,70000] 3.42ms 444kb |L0.3|                                                                                   "
    - "L0.4[60000,490000] 1.27ms 271kb |--L0.4---|                                                                              "
    - "L0.5[80000,250000] 3.42ms 462kb |L0.5|                                                                                   "
    - "L0.6[90000,540000] 1.41ms 91kb  |--L0.6---|                                                                             "
    - "L0.7[100000,200000] 1.53ms 89kb  |L0.7|                                                                                  "
    - "L0.8[110000,300000] 1.72ms 66kb  |L0.8|                                                                                  "
    - "L0.9[120000,140000] 1.91ms 87kb  |L0.9|                                                                                  "
    - "L0.10[130000,470000] 2.12ms 69kb   |-L0.10-|                                                                              "
    - "L0.11[150000,510000] 2.24ms 76kb   |-L0.11-|                                                                              "
    - "L0.12[160000,550000] 2.39ms 79kb   |-L0.12--|                                                                             "
    - "L0.13[170000,560000] 2.58ms 88kb    |-L0.13--|                                                                            "
    - "L0.14[180000,650000] 2.73ms 162kb    |--L0.14---|                                                                          "
    - "L0.15[190000,680000] 2.96ms 133kb    |---L0.15---|                                                                         "
    - "L0.16[210000,230000] 3.06ms 72kb     |L0.16|                                                                              "
    - "L0.17[220000,240000] 3.11ms 41kb     |L0.17|                                                                              "
    - "L0.18[260000,290000] 3.11ms 42kb      |L0.18|                                                                             "
    - "L0.19[270000,520000] 3.42ms 442kb      |L0.19|                                                                             "
    - "L0.20[280000,320000] 3.16ms 59kb       |L0.20|                                                                            "
    - "L0.21[310000,340000] 3.19ms 59kb       |L0.21|                                                                            "
    - "L0.22[330000,360000] 3.23ms 68kb        |L0.22|                                                                           "
    - "L0.23[350000,380000] 3.25ms 58kb         |L0.23|                                                                          "
    - "L0.24[370000,400000] 3.28ms 66kb         |L0.24|                                                                          "
    - "L0.25[390000,420000] 3.35ms 52kb          |L0.25|                                                                         "
    - "L0.26[410000,440000] 3.36ms 65kb          |L0.26|                                                                         "
    - "L0.27[430000,460000] 3.4ms 95kb           |L0.27|                                                                        "
    - "L0.28[450000,480000] 3.41ms 159kb           |L0.28|                                                                        "
    - "L0.29[500000,710000] 1.27ms 724kb             |L0.29|                                                                      "
    - "L0.30[530000,620000] 3.42ms 442kb             |L0.30|                                                                      "
    - "L0.31[570000,610000] 3.41ms 114kb              |L0.31|                                                                     "
    - "L0.32[580000,700000] 1.41ms 458kb               |L0.32|                                                                    "
    - "L0.33[590000,930000] 1.53ms 113mb               |-L0.33-|                                                                  "
    - "L0.34[600000,1020000] 1.72ms 21mb               |--L0.34--|                                                                "
    - "L0.35[630000,750000] 3.42ms 582kb                |L0.35|                                                                   "
    - "L0.36[640000,740000] 3.41ms 184kb                |L0.36|                                                                   "
    - "L0.37[660000,670000] 2.12ms 231kb                 |L0.37|                                                                  "
    - "L0.38[690000,730000] 3.06ms 7kb                  |L0.38|                                                                 "
    - "L0.39[720000,1170000] 1.27ms 259mb                  |--L0.39--|                                                             "
    - "L0.40[760000,800000] 3.42ms 290kb                   |L0.40|                                                                "
    - "L0.41[770000,770000] 3.11ms 6kb                    |L0.41|                                                               "
    - "L0.42[780000,790000] 3.16ms 7kb                    |L0.42|                                                               "
    - "L0.43[810000,1330000] 1.41ms 249mb                     |---L0.43---|                                                        "
    - "L0.44[820000,830000] 3.42ms 106kb                     |L0.44|                                                              "
    - "L0.45[840000,890000] 3.42ms 134kb                      |L0.45|                                                             "
    - "L0.46[850000,860000] 3.19ms 6kb                      |L0.46|                                                             "
    - "L0.47[870000,880000] 3.41ms 123kb                      |L0.47|                                                             "
    - "L0.48[900000,970000] 3.42ms 155kb                       |L0.48|                                                            "
    - "L0.49[910000,920000] 3.41ms 105kb                       |L0.49|                                                            "
    - "L0.50[940000,1430000] 1.53ms 115mb                        |---L0.50---|                                                     "
    - "L0.51[950000,960000] 2.24ms 192kb                         |L0.51|                                                          "
    - "L0.52[980000,1110000] 3.42ms 108kb                         |L0.52|                                                          "
    - "L0.53[990000,1090000] 2.12ms 91kb                          |L0.53|                                                         "
    - "L0.54[1000000,1040000] 3.25ms 6kb                          |L0.54|                                                         "
    - "L0.55[1010000,1060000] 1.91ms 66kb                          |L0.55|                                                         "
    - "L0.56[1030000,1730000] 1.72ms 218mb                           |-----L0.56------|                                             "
    - "L0.57[1050000,1120000] 2.24ms 76kb                           |L0.57|                                                        "
    - "L0.58[1070000,1080000] 2.39ms 8kb                            |L0.58|                                                       "
    - "L0.59[1100000,1100000] 3.28ms 6kb                             |L0.59|                                                      "
    - "L0.60[1130000,1160000] 2.24ms 48kb                             |L0.60|                                                      "
    - "L0.61[1140000,1240000] 1.72ms 76mb                              |L0.61|                                                     "
    - "L0.62[1150000,1230000] 3.42ms 107kb                              |L0.62|                                                     "
    - "L0.63[1160000,1190000] 2.39ms 93kb                              |L0.63|                                                     "
    - "L0.64[1180000,1200000] 2.58ms 92kb                               |L0.64|                                                    "
    - "L0.65[1210000,1220000] 2.73ms 7kb                               |L0.65|                                                    "
    - "L0.66[1250000,1370000] 1.72ms 96mb                                 |L0.66|                                                  "
    - "L0.67[1260000,1360000] 3.42ms 100kb                                 |L0.67|                                                  "
    - "L0.68[1280000,1420000] 2.73ms 116kb                                 |L0.68|                                                  "
    - "L0.69[1290000,1300000] 2.58ms 9kb                                  |L0.69|                                                 "
    - "L0.70[1310000,1320000] 3.41ms 25kb                                  |L0.70|                                                 "
    - "L0.71[1340000,1450000] 1.79ms 33mb                                   |L0.71|                                                "
    - "L0.72[1350000,1440000] 1.91ms 746kb                                   |L0.72|                                                "
    - "L0.73[1380000,1580000] 1.72ms 68mb                                    |L0.73|                                               "
    - "L0.74[1390000,1590000] 3.42ms 179kb                                    |L0.74|                                               "
    - "L0.75[1400000,1540000] 3.36ms 7kb                                     |L0.75|                                              "
    - "L0.76[1460000,1550000] 1.79ms 62mb                                      |L0.76|                                             "
    - "L0.77[1470000,1600000] 1.91ms 21mb                                      |L0.77|                                             "
    - "L0.78[1480000,1500000] 2.73ms 54kb                                       |L0.78|                                            "
    - "L0.79[1490000,1570000] 2.96ms 73kb                                       |L0.79|                                            "
    - "L0.80[1510000,1520000] 2.24ms 23kb                                       |L0.80|                                            "
    - "L0.81[1560000,1600000] 1.79ms 53mb                                         |L0.81|                                          "
    - "L0.82[1610000,1630000] 1.91ms 1mb                                          |L0.82|                                         "
    - "L0.83[1610000,1710000] 1.79ms 44mb                                          |L0.83|                                         "
    - "L0.84[1620000,1670000] 3.42ms 42kb                                          |L0.84|                                         "
    - "L0.85[1640000,1740000] 1.91ms 95mb                                           |L0.85|                                        "
    - "L0.86[1650000,1700000] 2.96ms 49kb                                           |L0.86|                                        "
    - "L0.87[1660000,1680000] 1.72ms 14mb                                           |L0.87|                                        "
    - "L0.88[1690000,1810000] 3.42ms 168kb                                            |L0.88|                                       "
    - "L0.89[1750000,1800000] 1.91ms 130mb                                              |L0.89|                                     "
    - "L0.90[1760000,1780000] 2.73ms 185kb                                              |L0.90|                                     "
    - "L0.91[1770000,1820000] 2.12ms 46mb                                              |L0.91|                                     "
    - "L0.92[1830000,1970000] 2.12ms 202mb                                                |L0.92|                                   "
    - "L0.93[1840000,1960000] 3.42ms 156kb                                                |L0.93|                                   "
    - "L0.94[1850000,1950000] 2.24ms 14mb                                                |L0.94|                                   "
    - "L0.95[1860000,1870000] 2.73ms 17kb                                                 |L0.95|                                  "
    - "L0.96[1880000,1930000] 1.91ms 51mb                                                 |L0.96|                                  "
    - "L0.97[1890000,1900000] 3.41ms 6kb                                                  |L0.97|                                 "
    - "L0.98[1920000,1940000] 2.96ms 36kb                                                  |L0.98|                                 "
    - "L0.99[1980000,2040000] 2.12ms 46mb                                                    |L0.99|                               "
    - "L0.100[1990000,2060000] 3.42ms 102kb                                                    |L0.100|                              "
    - "L0.101[2000000,2070000] 2.24ms 110mb                                                    |L0.101|                              "
    - "L0.102[2010000,2030000] 2.96ms 27kb                                                     |L0.102|                             "
    - "L0.103[2020000,2050000] 3.41ms 6kb                                                     |L0.103|                             "
    - "L0.104[2080000,2140000] 2.24ms 61mb                                                       |L0.104|                           "
    - "L0.105[2090000,2150000] 3.42ms 71kb                                                       |L0.105|                           "
    - "L0.106[2100000,2110000] 2.39ms 6kb                                                       |L0.106|                           "
    - "L0.107[2130000,2210000] 2.39ms 99mb                                                        |L0.107|                          "
    - "L0.108[2160000,2260000] 3.42ms 156kb                                                         |L0.108|                         "
    - "L0.109[2170000,2200000] 3.4ms 46kb                                                         |L0.109|                         "
    - "L0.110[2180000,2190000] 3.41ms 8kb                                                         |L0.110|                         "
    - "L0.111[2220000,2250000] 2.39ms 111mb                                                          |L0.111|                        "
    - "L0.112[2230000,2280000] 2.58ms 117mb                                                           |L0.112|                       "
    - "L0.113[2270000,2340000] 3.42ms 42kb                                                            |L0.113|                      "
    - "L0.114[2290000,2420000] 2.58ms 126mb                                                            |L0.114|                      "
    - "L0.115[2300000,2310000] 3.4ms 25kb                                                            |L0.115|                      "
    - "L0.116[2320000,2440000] 2.73ms 50mb                                                             |L0.116|                     "
    - "L0.117[2330000,2370000] 2.73ms 79kb                                                             |L0.117|                     "
    - "L0.118[2350000,2410000] 3.42ms 83kb                                                              |L0.118|                    "
    - "L0.119[2360000,2380000] 2.96ms 31kb                                                              |L0.119|                    "
    - "L0.120[2400000,2430000] 3.06ms 31kb                                                               |L0.120|                   "
    - "L0.121[2450000,2550000] 2.73ms 162mb                                                                |L0.121|                  "
    - "L0.122[2460000,2490000] 2.58ms 32kb                                                                 |L0.122|                 "
    - "L0.123[2470000,2500000] 3.06ms 30kb                                                                 |L0.123|                 "
    - "L0.124[2480000,2540000] 3.42ms 114kb                                                                 |L0.124|                 "
    - "L0.125[2510000,2550000] 2.73ms 20mb                                                                  |L0.125|                "
    - "L0.126[2520000,2680000] 3.4ms 46kb                                                                  |L0.126|                "
    - "L0.127[2530000,2670000] 3.41ms 8kb                                                                   |L0.127|               "
    - "L0.128[2560000,2590000] 2.73ms 24mb                                                                   |L0.128|               "
    - "L0.129[2560000,2620000] 2.73ms 26mb                                                                   |L0.129|               "
    - "L0.130[2570000,2630000] 3.42ms 111kb                                                                    |L0.130|              "
    - "L0.131[2600000,2700000] 2.73ms 176mb                                                                    |L0.131|              "
    - "L0.132[2610000,2640000] 2.96ms 10mb                                                                     |L0.132|             "
    - "L0.133[2650000,2700000] 2.96ms 11mb                                                                      |L0.133|            "
    - "L0.134[2660000,2690000] 3.42ms 44kb                                                                      |L0.134|            "
    - "L0.135[2710000,2770000] 2.96ms 97mb                                                                       |L0.135|           "
    - "L0.136[2710000,2750000] 2.73ms 28mb                                                                       |L0.136|           "
    - "L0.137[2720000,2760000] 3.42ms 80kb                                                                        |L0.137|          "
    - "L0.138[2740000,2810000] 3.06ms 29mb                                                                        |L0.138|          "
    - "L0.139[2780000,2830000] 2.96ms 120mb                                                                         |L0.139|         "
    - "L0.140[2790000,2840000] 3.42ms 107kb                                                                          |L0.140|        "
    - "L0.141[2800000,2850000] 3.06ms 13kb                                                                          |L0.141|        "
    - "L0.142[2820000,2860000] 3.06ms 33mb                                                                          |L0.142|        "
    - "L0.143[2870000,2980000] 3.06ms 171mb                                                                            |L0.143|      "
    - "L0.144[2880000,2910000] 2.96ms 27kb                                                                            |L0.144|      "
    - "L0.145[2890000,2990000] 3.06ms 70mb                                                                            |L0.145|      "
    - "L0.146[2900000,2920000] 3.42ms 16kb                                                                            |L0.146|      "
    - "L0.147[2930000,3390000] 3.42ms 94mb                                                                             |--L0.147--| "
    - "L0.148[2940000,3210000] 3.28ms 54kb                                                                              |L0.148|    "
    - "L0.149[2950000,3320000] 3.35ms 190mb                                                                              |L0.149-|   "
    - "L0.150[2970000,3130000] 3.11ms 233mb                                                                              |L0.150|    "
    - "L0.151[3000000,3070000] 3.06ms 168mb                                                                               |L0.151|   "
    - "L0.152[3010000,3020000] 3.06ms 17kb                                                                               |L0.152|   "
    - "L0.153[3030000,3040000] 3.25ms 10kb                                                                                |L0.153|  "
    - "L0.154[3050000,3170000] 3.16ms 219mb                                                                                |L0.154|  "
    - "L0.155[3080000,3090000] 3.19ms 13kb                                                                                 |L0.155| "
    - "L0.156[3100000,3200000] 3.19ms 212mb                                                                                  |L0.156|"
    - "L0.157[3120000,3140000] 3.41ms 25kb                                                                                  |L0.157|"
    - "L0.158[3150000,3240000] 3.23ms 214mb                                                                                   |L0.158|"
    - "L0.159[3180000,3260000] 3.25ms 214mb                                                                                    |L0.159|"
    - "L0.160[3220000,3290000] 3.28ms 213mb                                                                                     |L0.160|"
    - "L0.161[3270000,3330000] 3.36ms 183mb                                                                                      |L0.161|"
    - "L0.162[3300000,3380000] 3.4ms 231mb                                                                                       |L0.162|"
    - "L0.163[3310000,3380000] 3.41ms 232mb                                                                                       |L0.163|"
    - "L0.164[3340000,3370000] 3.36ms 29mb                                                                                        |L0.164|"
    - "WARNING: file L0.39[720000,1170000] 1.27ms 259mb exceeds soft limit 100mb by more than 50%"
    - "WARNING: file L0.43[810000,1330000] 1.41ms 249mb exceeds soft limit 100mb by more than 50%"
    - "WARNING: file L0.56[1030000,1730000] 1.72ms 218mb exceeds soft limit 100mb by more than 50%"
    - "WARNING: file L0.92[1830000,1970000] 2.12ms 202mb exceeds soft limit 100mb by more than 50%"
    - "WARNING: file L0.121[2450000,2550000] 2.73ms 162mb exceeds soft limit 100mb by more than 50%"
    - "WARNING: file L0.131[2600000,2700000] 2.73ms 176mb exceeds soft limit 100mb by more than 50%"
    - "WARNING: file L0.143[2870000,2980000] 3.06ms 171mb exceeds soft limit 100mb by more than 50%"
    - "WARNING: file L0.149[2950000,3320000] 3.35ms 190mb exceeds soft limit 100mb by more than 50%"
    - "WARNING: file L0.150[2970000,3130000] 3.11ms 233mb exceeds soft limit 100mb by more than 50%"
    - "WARNING: file L0.151[3000000,3070000] 3.06ms 168mb exceeds soft limit 100mb by more than 50%"
    - "WARNING: file L0.154[3050000,3170000] 3.16ms 219mb exceeds soft limit 100mb by more than 50%"
    - "WARNING: file L0.156[3100000,3200000] 3.19ms 212mb exceeds soft limit 100mb by more than 50%"
    - "WARNING: file L0.158[3150000,3240000] 3.23ms 214mb exceeds soft limit 100mb by more than 50%"
    - "WARNING: file L0.159[3180000,3260000] 3.25ms 214mb exceeds soft limit 100mb by more than 50%"
    - "WARNING: file L0.160[3220000,3290000] 3.28ms 213mb exceeds soft limit 100mb by more than 50%"
    - "WARNING: file L0.161[3270000,3330000] 3.36ms 183mb exceeds soft limit 100mb by more than 50%"
    - "WARNING: file L0.162[3300000,3380000] 3.4ms 231mb exceeds soft limit 100mb by more than 50%"
    - "WARNING: file L0.163[3310000,3380000] 3.41ms 232mb exceeds soft limit 100mb by more than 50%"
    - "**** Final Output Files (13.84gb written)"
    - "L2                                                                                                                 "
    - "L2.244[1980000,2037321] 3.42ms 100mb                                                    |L2.244|                              "
    - "L2.245[2037322,2070000] 3.42ms 57mb                                                     |L2.245|                             "
    - "L2.377[894767,968639] 3.42ms 100mb                       |L2.377|                                                           "
    - "L2.382[1153380,1203173] 3.42ms 94mb                              |L2.382|                                                    "
    - "L2.383[1203174,1260886] 3.42ms 100mb                               |L2.383|                                                   "
    - "L2.384[1260887,1305975] 3.42ms 78mb                                 |L2.384|                                                 "
    - "L2.385[1305976,1374631] 3.42ms 100mb                                  |L2.385|                                                "
    - "L2.394[1755317,1807829] 3.42ms 100mb                                              |L2.394|                                    "
    - "L2.396[2080000,2156222] 3.42ms 100mb                                                       |L2.396|                           "
    - "L2.397[2156223,2229999] 3.42ms 97mb                                                         |L2.397|                         "
    - "L2.398[2230000,2264878] 3.42ms 100mb                                                           |L2.398|                       "
    - "L2.399[2264879,2299756] 3.42ms 100mb                                                            |L2.399|                      "
    - "L2.406[2505050,2550000] 3.42ms 82mb                                                                  |L2.406|                "
    - "L2.407[2550001,2616666] 3.42ms 93mb                                                                   |L2.407|               "
    - "L2.413[2760000,2799998] 3.42ms 87mb                                                                         |L2.413|         "
    - "L2.419[2931176,2964443] 3.42ms 100mb                                                                             |L2.419|     "
    - "L2.420[2964444,2992350] 3.42ms 84mb                                                                              |L2.420|    "
    - "L2.421[2992351,3015165] 3.42ms 100mb                                                                               |L2.421|   "
    - "L2.422[3015166,3037979] 3.42ms 100mb                                                                                |L2.422|  "
    - "L2.423[3037980,3053527] 3.42ms 68mb                                                                                |L2.423|  "
    - "L2.424[3053528,3071615] 3.42ms 100mb                                                                                 |L2.424| "
    - "L2.429[3121808,3131174] 3.42ms 57mb                                                                                  |L2.429|"
    - "L2.430[3131175,3149990] 3.42ms 100mb                                                                                   |L2.430|"
    - "L2.434[3182939,3196874] 3.42ms 100mb                                                                                    |L2.434|"
    - "L2.435[3196875,3208820] 3.42ms 86mb                                                                                    |L2.435|"
    - "L2.436[3208821,3222148] 3.42ms 100mb                                                                                     |L2.436|"
    - "L2.437[3222149,3234702] 3.42ms 94mb                                                                                     |L2.437|"
    - "L2.438[3234703,3249276] 3.42ms 100mb                                                                                     |L2.438|"
    - "L2.439[3249277,3260584] 3.42ms 78mb                                                                                      |L2.439|"
    - "L2.440[3260585,3278130] 3.42ms 100mb                                                                                      |L2.440|"
    - "L2.445[3323922,3335493] 3.42ms 100mb                                                                                        |L2.445|"
    - "L2.449[3364113,3384822] 3.42ms 88mb                                                                                         |L2.449|"
    - "L2.455[10000,344482] 3.42ms 100mb|L2.455|                                                                                  "
    - "L2.456[344483,678964] 3.42ms 100mb        |L2.456|                                                                          "
    - "L2.457[678965,894766] 3.42ms 65mb                 |L2.457|                                                                 "
    - "L2.458[968640,1031250] 3.42ms 100mb                         |L2.458|                                                         "
    - "L2.459[1031251,1093860] 3.42ms 100mb                           |L2.459|                                                       "
    - "L2.460[1093861,1153379] 3.42ms 95mb                            |L2.460|                                                      "
    - "L2.461[1374632,1446111] 3.42ms 100mb                                    |L2.461|                                              "
    - "L2.462[1446112,1517590] 3.42ms 100mb                                      |L2.462|                                            "
    - "L2.463[1517591,1572539] 3.42ms 77mb                                        |L2.463|                                          "
    - "L2.464[1572540,1635002] 3.42ms 100mb                                         |L2.464|                                         "
    - "L2.465[1635003,1697464] 3.42ms 100mb                                           |L2.465|                                       "
    - "L2.466[1697465,1755316] 3.42ms 93mb                                            |L2.466|                                      "
    - "L2.467[1807830,1863768] 3.42ms 100mb                                               |L2.467|                                   "
    - "L2.468[1863769,1919706] 3.42ms 100mb                                                 |L2.468|                                 "
    - "L2.469[1919707,1970000] 3.42ms 90mb                                                  |L2.469|                                "
    - "L2.470[2299757,2376597] 3.42ms 100mb                                                            |L2.470|                      "
    - "L2.471[2376598,2453437] 3.42ms 100mb                                                               |L2.471|                   "
    - "L2.472[2453438,2505049] 3.42ms 67mb                                                                 |L2.472|                 "
    - "L2.473[2616667,2669559] 3.42ms 100mb                                                                     |L2.473|             "
    - "L2.474[2669560,2722451] 3.42ms 100mb                                                                      |L2.474|            "
    - "L2.475[2722452,2759999] 3.42ms 71mb                                                                        |L2.475|          "
    - "L2.476[2799999,2857709] 3.42ms 100mb                                                                          |L2.476|        "
    - "L2.477[2857710,2915419] 3.42ms 100mb                                                                           |L2.477|       "
    - "L2.478[2915420,2931175] 3.42ms 27mb                                                                             |L2.478|     "
    - "L2.479[3071616,3091082] 3.42ms 100mb                                                                                 |L2.479| "
    - "L2.480[3091083,3110548] 3.42ms 100mb                                                                                  |L2.480|"
    - "L2.481[3110549,3121807] 3.42ms 58mb                                                                                  |L2.481|"
    - "L2.482[3149991,3166126] 3.42ms 100mb                                                                                   |L2.482|"
    - "L2.483[3166127,3182261] 3.42ms 100mb                                                                                    |L2.483|"
    - "L2.484[3182262,3182938] 3.42ms 4mb                                                                                    |L2.484|"
    - "L2.485[3278131,3293432] 3.42ms 100mb                                                                                       |L2.485|"
    - "L2.486[3293433,3308733] 3.42ms 100mb                                                                                       |L2.486|"
    - "L2.487[3308734,3323921] 3.42ms 99mb                                                                                       |L2.487|"
    - "L2.488[3335494,3348934] 3.42ms 100mb                                                                                        |L2.488|"
    - "L2.489[3348935,3362374] 3.42ms 100mb                                                                                        |L2.489|"
    - "L2.490[3362375,3364112] 3.42ms 13mb                                                                                         |L2.490|"
    - "L2.491[3384823,3388964] 3.42ms 18mb                                                                                         |L2.491|"
    - "L2.492[3388965,3390000] 3.42ms 4mb                                                                                         |L2.492|"
    - "**** Breakdown of where bytes were written"
    - 3.34gb written by split(CompactAndSplitOutput(FoundSubsetLessThanMaxCompactSize))
    - 4.75gb written by split(VerticalSplit)
    - 5.74gb written by split(CompactAndSplitOutput(TotalSizeLessThanMaxCompactSize))
    - 6mb written by compact(ManySmallFiles)
    - 931kb written by compact(TotalSizeLessThanMaxCompactSize)
    "###
    );
}
