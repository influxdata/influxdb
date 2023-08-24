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
    - "**** Simulation run 0, type=split(VerticalSplit)(split_times=[355, 668]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.1[76,932] 1us         |------------------------------------------L0.1------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[76,355] 1us 3mb     |-----------L0.?------------|                                                             "
    - "L0.?[356,668] 1us 4mb                                 |-------------L0.?-------------|                             "
    - "L0.?[669,932] 1us 3mb                                                                  |----------L0.?-----------| "
    - "**** Simulation run 1, type=split(VerticalSplit)(split_times=[355, 668]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.2[42,986] 1us         |------------------------------------------L0.2------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[42,355] 1us 3mb     |-----------L0.?------------|                                                             "
    - "L0.?[356,668] 1us 3mb                                 |-----------L0.?------------|                                "
    - "L0.?[669,986] 1us 3mb                                                               |------------L0.?------------| "
    - "**** Simulation run 2, type=split(VerticalSplit)(split_times=[355, 668]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.3[173,950] 1us        |------------------------------------------L0.3------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[173,355] 1us 2mb    |-------L0.?--------|                                                                     "
    - "L0.?[356,668] 1us 4mb                         |---------------L0.?---------------|                                 "
    - "L0.?[669,950] 1us 4mb                                                             |-------------L0.?-------------| "
    - "**** Simulation run 3, type=split(VerticalSplit)(split_times=[355]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.4[50,629] 1us         |------------------------------------------L0.4------------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[50,355] 1us 5mb     |--------------------L0.?---------------------|                                           "
    - "L0.?[356,629] 1us 5mb                                                   |------------------L0.?------------------| "
    - "**** Simulation run 4, type=split(VerticalSplit)(split_times=[355, 668]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.5[76,932] 1us         |------------------------------------------L0.5------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[76,355] 1us 3mb     |-----------L0.?------------|                                                             "
    - "L0.?[356,668] 1us 4mb                                 |-------------L0.?-------------|                             "
    - "L0.?[669,932] 1us 3mb                                                                  |----------L0.?-----------| "
    - "**** Simulation run 5, type=split(VerticalSplit)(split_times=[355, 668]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.6[42,986] 1us         |------------------------------------------L0.6------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[42,355] 1us 3mb     |-----------L0.?------------|                                                             "
    - "L0.?[356,668] 1us 3mb                                 |-----------L0.?------------|                                "
    - "L0.?[669,986] 1us 3mb                                                               |------------L0.?------------| "
    - "**** Simulation run 6, type=split(VerticalSplit)(split_times=[355, 668]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.7[173,950] 1.01us     |------------------------------------------L0.7------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[173,355] 1.01us 2mb |-------L0.?--------|                                                                     "
    - "L0.?[356,668] 1.01us 4mb                      |---------------L0.?---------------|                                 "
    - "L0.?[669,950] 1.01us 4mb                                                          |-------------L0.?-------------| "
    - "**** Simulation run 7, type=split(VerticalSplit)(split_times=[355]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.8[50,629] 1.01us      |------------------------------------------L0.8------------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[50,355] 1.01us 5mb  |--------------------L0.?---------------------|                                           "
    - "L0.?[356,629] 1.01us 5mb                                                |------------------L0.?------------------| "
    - "**** Simulation run 8, type=split(VerticalSplit)(split_times=[355, 668]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.9[76,932] 1.01us      |------------------------------------------L0.9------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[76,355] 1.01us 3mb  |-----------L0.?------------|                                                             "
    - "L0.?[356,668] 1.01us 4mb                              |-------------L0.?-------------|                             "
    - "L0.?[669,932] 1.01us 3mb                                                               |----------L0.?-----------| "
    - "**** Simulation run 9, type=split(VerticalSplit)(split_times=[355, 668]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.10[42,986] 1.01us     |-----------------------------------------L0.10------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[42,355] 1.01us 3mb  |-----------L0.?------------|                                                             "
    - "L0.?[356,668] 1.01us 3mb                              |-----------L0.?------------|                                "
    - "L0.?[669,986] 1.01us 3mb                                                            |------------L0.?------------| "
    - "**** Simulation run 10, type=split(VerticalSplit)(split_times=[355, 668]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.11[173,950] 1.01us    |-----------------------------------------L0.11------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[173,355] 1.01us 2mb |-------L0.?--------|                                                                     "
    - "L0.?[356,668] 1.01us 4mb                      |---------------L0.?---------------|                                 "
    - "L0.?[669,950] 1.01us 4mb                                                          |-------------L0.?-------------| "
    - "**** Simulation run 11, type=split(VerticalSplit)(split_times=[355]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.12[50,629] 1.01us     |-----------------------------------------L0.12------------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[50,355] 1.01us 5mb  |--------------------L0.?---------------------|                                           "
    - "L0.?[356,629] 1.01us 5mb                                                |------------------L0.?------------------| "
    - "**** Simulation run 12, type=split(VerticalSplit)(split_times=[355, 668]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.13[76,932] 1.01us     |-----------------------------------------L0.13------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[76,355] 1.01us 3mb  |-----------L0.?------------|                                                             "
    - "L0.?[356,668] 1.01us 4mb                              |-------------L0.?-------------|                             "
    - "L0.?[669,932] 1.01us 3mb                                                               |----------L0.?-----------| "
    - "**** Simulation run 13, type=split(VerticalSplit)(split_times=[355, 668]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.14[42,986] 1.01us     |-----------------------------------------L0.14------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[42,355] 1.01us 3mb  |-----------L0.?------------|                                                             "
    - "L0.?[356,668] 1.01us 3mb                              |-----------L0.?------------|                                "
    - "L0.?[669,986] 1.01us 3mb                                                            |------------L0.?------------| "
    - "**** Simulation run 14, type=split(VerticalSplit)(split_times=[355, 668]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.15[173,950] 1.01us    |-----------------------------------------L0.15------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[173,355] 1.01us 2mb |-------L0.?--------|                                                                     "
    - "L0.?[356,668] 1.01us 4mb                      |---------------L0.?---------------|                                 "
    - "L0.?[669,950] 1.01us 4mb                                                          |-------------L0.?-------------| "
    - "**** Simulation run 15, type=split(VerticalSplit)(split_times=[355]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.16[50,629] 1.01us     |-----------------------------------------L0.16------------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[50,355] 1.01us 5mb  |--------------------L0.?---------------------|                                           "
    - "L0.?[356,629] 1.01us 5mb                                                |------------------L0.?------------------| "
    - "**** Simulation run 16, type=split(VerticalSplit)(split_times=[355, 668]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.17[76,932] 1.02us     |-----------------------------------------L0.17------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[76,355] 1.02us 3mb  |-----------L0.?------------|                                                             "
    - "L0.?[356,668] 1.02us 4mb                              |-------------L0.?-------------|                             "
    - "L0.?[669,932] 1.02us 3mb                                                               |----------L0.?-----------| "
    - "**** Simulation run 17, type=split(VerticalSplit)(split_times=[355, 668]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.18[42,986] 1.02us     |-----------------------------------------L0.18------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[42,355] 1.02us 3mb  |-----------L0.?------------|                                                             "
    - "L0.?[356,668] 1.02us 3mb                              |-----------L0.?------------|                                "
    - "L0.?[669,986] 1.02us 3mb                                                            |------------L0.?------------| "
    - "**** Simulation run 18, type=split(VerticalSplit)(split_times=[355, 668]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.19[173,950] 1.02us    |-----------------------------------------L0.19------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[173,355] 1.02us 2mb |-------L0.?--------|                                                                     "
    - "L0.?[356,668] 1.02us 4mb                      |---------------L0.?---------------|                                 "
    - "L0.?[669,950] 1.02us 4mb                                                          |-------------L0.?-------------| "
    - "**** Simulation run 19, type=split(VerticalSplit)(split_times=[355]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.20[50,629] 1.02us     |-----------------------------------------L0.20------------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[50,355] 1.02us 5mb  |--------------------L0.?---------------------|                                           "
    - "L0.?[356,629] 1.02us 5mb                                                |------------------L0.?------------------| "
    - "**** Simulation run 20, type=split(VerticalSplit)(split_times=[355, 668]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.21[76,932] 1.02us     |-----------------------------------------L0.21------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[76,355] 1.02us 3mb  |-----------L0.?------------|                                                             "
    - "L0.?[356,668] 1.02us 4mb                              |-------------L0.?-------------|                             "
    - "L0.?[669,932] 1.02us 3mb                                                               |----------L0.?-----------| "
    - "**** Simulation run 21, type=split(VerticalSplit)(split_times=[355, 668]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.22[42,986] 1.02us     |-----------------------------------------L0.22------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[42,355] 1.02us 3mb  |-----------L0.?------------|                                                             "
    - "L0.?[356,668] 1.02us 3mb                              |-----------L0.?------------|                                "
    - "L0.?[669,986] 1.02us 3mb                                                            |------------L0.?------------| "
    - "**** Simulation run 22, type=split(VerticalSplit)(split_times=[355, 668]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.23[173,950] 1.02us    |-----------------------------------------L0.23------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[173,355] 1.02us 2mb |-------L0.?--------|                                                                     "
    - "L0.?[356,668] 1.02us 4mb                      |---------------L0.?---------------|                                 "
    - "L0.?[669,950] 1.02us 4mb                                                          |-------------L0.?-------------| "
    - "**** Simulation run 23, type=split(VerticalSplit)(split_times=[355]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.24[50,629] 1.02us     |-----------------------------------------L0.24------------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[50,355] 1.02us 5mb  |--------------------L0.?---------------------|                                           "
    - "L0.?[356,629] 1.02us 5mb                                                |------------------L0.?------------------| "
    - "**** Simulation run 24, type=split(VerticalSplit)(split_times=[355, 668]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.25[76,932] 1.02us     |-----------------------------------------L0.25------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[76,355] 1.02us 3mb  |-----------L0.?------------|                                                             "
    - "L0.?[356,668] 1.02us 4mb                              |-------------L0.?-------------|                             "
    - "L0.?[669,932] 1.02us 3mb                                                               |----------L0.?-----------| "
    - "**** Simulation run 25, type=split(VerticalSplit)(split_times=[355, 668]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.26[42,986] 1.02us     |-----------------------------------------L0.26------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[42,355] 1.02us 3mb  |-----------L0.?------------|                                                             "
    - "L0.?[356,668] 1.02us 3mb                              |-----------L0.?------------|                                "
    - "L0.?[669,986] 1.02us 3mb                                                            |------------L0.?------------| "
    - "**** Simulation run 26, type=split(VerticalSplit)(split_times=[355, 668]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.27[173,950] 1.03us    |-----------------------------------------L0.27------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[173,355] 1.03us 2mb |-------L0.?--------|                                                                     "
    - "L0.?[356,668] 1.03us 4mb                      |---------------L0.?---------------|                                 "
    - "L0.?[669,950] 1.03us 4mb                                                          |-------------L0.?-------------| "
    - "**** Simulation run 27, type=split(VerticalSplit)(split_times=[355]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.28[50,629] 1.03us     |-----------------------------------------L0.28------------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[50,355] 1.03us 5mb  |--------------------L0.?---------------------|                                           "
    - "L0.?[356,629] 1.03us 5mb                                                |------------------L0.?------------------| "
    - "**** Simulation run 28, type=split(VerticalSplit)(split_times=[355, 668]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.29[76,932] 1.03us     |-----------------------------------------L0.29------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[76,355] 1.03us 3mb  |-----------L0.?------------|                                                             "
    - "L0.?[356,668] 1.03us 4mb                              |-------------L0.?-------------|                             "
    - "L0.?[669,932] 1.03us 3mb                                                               |----------L0.?-----------| "
    - "**** Simulation run 29, type=split(VerticalSplit)(split_times=[355, 668]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.30[42,986] 1.03us     |-----------------------------------------L0.30------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[42,355] 1.03us 3mb  |-----------L0.?------------|                                                             "
    - "L0.?[356,668] 1.03us 3mb                              |-----------L0.?------------|                                "
    - "L0.?[669,986] 1.03us 3mb                                                            |------------L0.?------------| "
    - "**** Simulation run 30, type=split(VerticalSplit)(split_times=[355, 668]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.31[173,950] 1.03us    |-----------------------------------------L0.31------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[173,355] 1.03us 2mb |-------L0.?--------|                                                                     "
    - "L0.?[356,668] 1.03us 4mb                      |---------------L0.?---------------|                                 "
    - "L0.?[669,950] 1.03us 4mb                                                          |-------------L0.?-------------| "
    - "**** Simulation run 31, type=split(VerticalSplit)(split_times=[355]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.32[50,629] 1.03us     |-----------------------------------------L0.32------------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[50,355] 1.03us 5mb  |--------------------L0.?---------------------|                                           "
    - "L0.?[356,629] 1.03us 5mb                                                |------------------L0.?------------------| "
    - "**** Simulation run 32, type=split(VerticalSplit)(split_times=[355, 668]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.33[76,932] 1.03us     |-----------------------------------------L0.33------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[76,355] 1.03us 3mb  |-----------L0.?------------|                                                             "
    - "L0.?[356,668] 1.03us 4mb                              |-------------L0.?-------------|                             "
    - "L0.?[669,932] 1.03us 3mb                                                               |----------L0.?-----------| "
    - "**** Simulation run 33, type=split(VerticalSplit)(split_times=[355, 668]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.34[42,986] 1.03us     |-----------------------------------------L0.34------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[42,355] 1.03us 3mb  |-----------L0.?------------|                                                             "
    - "L0.?[356,668] 1.03us 3mb                              |-----------L0.?------------|                                "
    - "L0.?[669,986] 1.03us 3mb                                                            |------------L0.?------------| "
    - "**** Simulation run 34, type=split(VerticalSplit)(split_times=[355, 668]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.35[173,950] 1.03us    |-----------------------------------------L0.35------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[173,355] 1.03us 2mb |-------L0.?--------|                                                                     "
    - "L0.?[356,668] 1.03us 4mb                      |---------------L0.?---------------|                                 "
    - "L0.?[669,950] 1.03us 4mb                                                          |-------------L0.?-------------| "
    - "**** Simulation run 35, type=split(VerticalSplit)(split_times=[355]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.36[50,629] 1.03us     |-----------------------------------------L0.36------------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[50,355] 1.03us 5mb  |--------------------L0.?---------------------|                                           "
    - "L0.?[356,629] 1.03us 5mb                                                |------------------L0.?------------------| "
    - "**** Simulation run 36, type=split(VerticalSplit)(split_times=[355, 668]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.37[76,932] 1.04us     |-----------------------------------------L0.37------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[76,355] 1.04us 3mb  |-----------L0.?------------|                                                             "
    - "L0.?[356,668] 1.04us 4mb                              |-------------L0.?-------------|                             "
    - "L0.?[669,932] 1.04us 3mb                                                               |----------L0.?-----------| "
    - "**** Simulation run 37, type=split(VerticalSplit)(split_times=[355, 668]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.38[42,986] 1.04us     |-----------------------------------------L0.38------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[42,355] 1.04us 3mb  |-----------L0.?------------|                                                             "
    - "L0.?[356,668] 1.04us 3mb                              |-----------L0.?------------|                                "
    - "L0.?[669,986] 1.04us 3mb                                                            |------------L0.?------------| "
    - "**** Simulation run 38, type=split(VerticalSplit)(split_times=[355, 668]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.39[173,950] 1.04us    |-----------------------------------------L0.39------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[173,355] 1.04us 2mb |-------L0.?--------|                                                                     "
    - "L0.?[356,668] 1.04us 4mb                      |---------------L0.?---------------|                                 "
    - "L0.?[669,950] 1.04us 4mb                                                          |-------------L0.?-------------| "
    - "**** Simulation run 39, type=split(VerticalSplit)(split_times=[355]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.40[50,629] 1.04us     |-----------------------------------------L0.40------------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[50,355] 1.04us 5mb  |--------------------L0.?---------------------|                                           "
    - "L0.?[356,629] 1.04us 5mb                                                |------------------L0.?------------------| "
    - "Committing partition 1:"
    - "  Soft Deleting 40 files: L0.1, L0.2, L0.3, L0.4, L0.5, L0.6, L0.7, L0.8, L0.9, L0.10, L0.11, L0.12, L0.13, L0.14, L0.15, L0.16, L0.17, L0.18, L0.19, L0.20, L0.21, L0.22, L0.23, L0.24, L0.25, L0.26, L0.27, L0.28, L0.29, L0.30, L0.31, L0.32, L0.33, L0.34, L0.35, L0.36, L0.37, L0.38, L0.39, L0.40"
    - "  Creating 110 files"
    - "**** Simulation run 40, type=split(VerticalSplit)(split_times=[355, 668]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.41[76,932] 1.04us     |-----------------------------------------L0.41------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[76,355] 1.04us 3mb  |-----------L0.?------------|                                                             "
    - "L0.?[356,668] 1.04us 4mb                              |-------------L0.?-------------|                             "
    - "L0.?[669,932] 1.04us 3mb                                                               |----------L0.?-----------| "
    - "**** Simulation run 41, type=split(VerticalSplit)(split_times=[355, 668]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.42[42,986] 1.04us     |-----------------------------------------L0.42------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[42,355] 1.04us 3mb  |-----------L0.?------------|                                                             "
    - "L0.?[356,668] 1.04us 3mb                              |-----------L0.?------------|                                "
    - "L0.?[669,986] 1.04us 3mb                                                            |------------L0.?------------| "
    - "**** Simulation run 42, type=split(VerticalSplit)(split_times=[355, 668]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.43[173,950] 1.04us    |-----------------------------------------L0.43------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[173,355] 1.04us 2mb |-------L0.?--------|                                                                     "
    - "L0.?[356,668] 1.04us 4mb                      |---------------L0.?---------------|                                 "
    - "L0.?[669,950] 1.04us 4mb                                                          |-------------L0.?-------------| "
    - "**** Simulation run 43, type=split(VerticalSplit)(split_times=[355]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.44[50,629] 1.04us     |-----------------------------------------L0.44------------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[50,355] 1.04us 5mb  |--------------------L0.?---------------------|                                           "
    - "L0.?[356,629] 1.04us 5mb                                                |------------------L0.?------------------| "
    - "**** Simulation run 44, type=split(VerticalSplit)(split_times=[355, 668]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.45[76,932] 1.04us     |-----------------------------------------L0.45------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[76,355] 1.04us 3mb  |-----------L0.?------------|                                                             "
    - "L0.?[356,668] 1.04us 4mb                              |-------------L0.?-------------|                             "
    - "L0.?[669,932] 1.04us 3mb                                                               |----------L0.?-----------| "
    - "**** Simulation run 45, type=split(VerticalSplit)(split_times=[355, 668]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.46[42,986] 1.05us     |-----------------------------------------L0.46------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[42,355] 1.05us 3mb  |-----------L0.?------------|                                                             "
    - "L0.?[356,668] 1.05us 3mb                              |-----------L0.?------------|                                "
    - "L0.?[669,986] 1.05us 3mb                                                            |------------L0.?------------| "
    - "**** Simulation run 46, type=split(VerticalSplit)(split_times=[355, 668]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.47[173,950] 1.05us    |-----------------------------------------L0.47------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[173,355] 1.05us 2mb |-------L0.?--------|                                                                     "
    - "L0.?[356,668] 1.05us 4mb                      |---------------L0.?---------------|                                 "
    - "L0.?[669,950] 1.05us 4mb                                                          |-------------L0.?-------------| "
    - "**** Simulation run 47, type=split(VerticalSplit)(split_times=[355]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.48[50,629] 1.05us     |-----------------------------------------L0.48------------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[50,355] 1.05us 5mb  |--------------------L0.?---------------------|                                           "
    - "L0.?[356,629] 1.05us 5mb                                                |------------------L0.?------------------| "
    - "**** Simulation run 48, type=split(VerticalSplit)(split_times=[355, 668]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.49[76,932] 1.05us     |-----------------------------------------L0.49------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[76,355] 1.05us 3mb  |-----------L0.?------------|                                                             "
    - "L0.?[356,668] 1.05us 4mb                              |-------------L0.?-------------|                             "
    - "L0.?[669,932] 1.05us 3mb                                                               |----------L0.?-----------| "
    - "**** Simulation run 49, type=split(VerticalSplit)(split_times=[355, 668]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.50[42,986] 1.05us     |-----------------------------------------L0.50------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[42,355] 1.05us 3mb  |-----------L0.?------------|                                                             "
    - "L0.?[356,668] 1.05us 3mb                              |-----------L0.?------------|                                "
    - "L0.?[669,986] 1.05us 3mb                                                            |------------L0.?------------| "
    - "Committing partition 1:"
    - "  Soft Deleting 10 files: L0.41, L0.42, L0.43, L0.44, L0.45, L0.46, L0.47, L0.48, L0.49, L0.50"
    - "  Creating 28 files"
    - "**** Simulation run 50, type=compact(ManySmallFiles). 20 Input Files, 76mb total:"
    - "L0                                                                                                                 "
    - "L0.51[0,1] 999ns 10mb    |L0.51|                                                                                   "
    - "L0.52[76,355] 1us 3mb                       |-------------------------------L0.52--------------------------------| "
    - "L0.55[42,355] 1us 3mb              |------------------------------------L0.55------------------------------------| "
    - "L0.58[173,355] 1us 2mb                                              |-------------------L0.58--------------------| "
    - "L0.61[50,355] 1us 5mb                |-----------------------------------L0.61-----------------------------------| "
    - "L0.63[76,355] 1us 3mb                       |-------------------------------L0.63--------------------------------| "
    - "L0.66[42,355] 1us 3mb              |------------------------------------L0.66------------------------------------| "
    - "L0.69[173,355] 1.01us 2mb                                           |-------------------L0.69--------------------| "
    - "L0.72[50,355] 1.01us 5mb             |-----------------------------------L0.72-----------------------------------| "
    - "L0.74[76,355] 1.01us 3mb                    |-------------------------------L0.74--------------------------------| "
    - "L0.77[42,355] 1.01us 3mb           |------------------------------------L0.77------------------------------------| "
    - "L0.80[173,355] 1.01us 2mb                                           |-------------------L0.80--------------------| "
    - "L0.83[50,355] 1.01us 5mb             |-----------------------------------L0.83-----------------------------------| "
    - "L0.85[76,355] 1.01us 3mb                    |-------------------------------L0.85--------------------------------| "
    - "L0.88[42,355] 1.01us 3mb           |------------------------------------L0.88------------------------------------| "
    - "L0.91[173,355] 1.01us 2mb                                           |-------------------L0.91--------------------| "
    - "L0.94[50,355] 1.01us 5mb             |-----------------------------------L0.94-----------------------------------| "
    - "L0.96[76,355] 1.02us 3mb                    |-------------------------------L0.96--------------------------------| "
    - "L0.99[42,355] 1.02us 3mb           |------------------------------------L0.99------------------------------------| "
    - "L0.102[173,355] 1.02us 2mb                                           |-------------------L0.102-------------------| "
    - "**** 1 Output Files (parquet_file_id not yet assigned), 76mb total:"
    - "L0, all files 76mb                                                                                                 "
    - "L0.?[0,355] 1.02us       |------------------------------------------L0.?------------------------------------------|"
    - "Committing partition 1:"
    - "  Soft Deleting 20 files: L0.51, L0.52, L0.55, L0.58, L0.61, L0.63, L0.66, L0.69, L0.72, L0.74, L0.77, L0.80, L0.83, L0.85, L0.88, L0.91, L0.94, L0.96, L0.99, L0.102"
    - "  Creating 1 files"
    - "**** Simulation run 51, type=compact(ManySmallFiles). 20 Input Files, 71mb total:"
    - "L0                                                                                                                 "
    - "L0.105[50,355] 1.02us 5mb  |---------------------------------------L0.105----------------------------------------| "
    - "L0.107[76,355] 1.02us 3mb         |------------------------------------L0.107------------------------------------| "
    - "L0.110[42,355] 1.02us 3mb|-----------------------------------------L0.110-----------------------------------------|"
    - "L0.113[173,355] 1.02us 2mb                                     |----------------------L0.113----------------------| "
    - "L0.116[50,355] 1.02us 5mb  |---------------------------------------L0.116----------------------------------------| "
    - "L0.118[76,355] 1.02us 3mb         |------------------------------------L0.118------------------------------------| "
    - "L0.121[42,355] 1.02us 3mb|-----------------------------------------L0.121-----------------------------------------|"
    - "L0.124[173,355] 1.03us 2mb                                     |----------------------L0.124----------------------| "
    - "L0.127[50,355] 1.03us 5mb  |---------------------------------------L0.127----------------------------------------| "
    - "L0.129[76,355] 1.03us 3mb         |------------------------------------L0.129------------------------------------| "
    - "L0.132[42,355] 1.03us 3mb|-----------------------------------------L0.132-----------------------------------------|"
    - "L0.135[173,355] 1.03us 2mb                                     |----------------------L0.135----------------------| "
    - "L0.138[50,355] 1.03us 5mb  |---------------------------------------L0.138----------------------------------------| "
    - "L0.140[76,355] 1.03us 3mb         |------------------------------------L0.140------------------------------------| "
    - "L0.143[42,355] 1.03us 3mb|-----------------------------------------L0.143-----------------------------------------|"
    - "L0.146[173,355] 1.03us 2mb                                     |----------------------L0.146----------------------| "
    - "L0.149[50,355] 1.03us 5mb  |---------------------------------------L0.149----------------------------------------| "
    - "L0.151[76,355] 1.04us 3mb         |------------------------------------L0.151------------------------------------| "
    - "L0.154[42,355] 1.04us 3mb|-----------------------------------------L0.154-----------------------------------------|"
    - "L0.157[173,355] 1.04us 2mb                                     |----------------------L0.157----------------------| "
    - "**** 1 Output Files (parquet_file_id not yet assigned), 71mb total:"
    - "L0, all files 71mb                                                                                                 "
    - "L0.?[42,355] 1.04us      |------------------------------------------L0.?------------------------------------------|"
    - "Committing partition 1:"
    - "  Soft Deleting 20 files: L0.105, L0.107, L0.110, L0.113, L0.116, L0.118, L0.121, L0.124, L0.127, L0.129, L0.132, L0.135, L0.138, L0.140, L0.143, L0.146, L0.149, L0.151, L0.154, L0.157"
    - "  Creating 1 files"
    - "**** Simulation run 52, type=compact(ManySmallFiles). 11 Input Files, 40mb total:"
    - "L0                                                                                                                 "
    - "L0.160[50,355] 1.04us 5mb  |---------------------------------------L0.160----------------------------------------| "
    - "L0.162[76,355] 1.04us 3mb         |------------------------------------L0.162------------------------------------| "
    - "L0.165[42,355] 1.04us 3mb|-----------------------------------------L0.165-----------------------------------------|"
    - "L0.168[173,355] 1.04us 2mb                                     |----------------------L0.168----------------------| "
    - "L0.171[50,355] 1.04us 5mb  |---------------------------------------L0.171----------------------------------------| "
    - "L0.173[76,355] 1.04us 3mb         |------------------------------------L0.173------------------------------------| "
    - "L0.176[42,355] 1.05us 3mb|-----------------------------------------L0.176-----------------------------------------|"
    - "L0.179[173,355] 1.05us 2mb                                     |----------------------L0.179----------------------| "
    - "L0.182[50,355] 1.05us 5mb  |---------------------------------------L0.182----------------------------------------| "
    - "L0.184[76,355] 1.05us 3mb         |------------------------------------L0.184------------------------------------| "
    - "L0.187[42,355] 1.05us 3mb|-----------------------------------------L0.187-----------------------------------------|"
    - "**** 1 Output Files (parquet_file_id not yet assigned), 40mb total:"
    - "L0, all files 40mb                                                                                                 "
    - "L0.?[42,355] 1.05us      |------------------------------------------L0.?------------------------------------------|"
    - "Committing partition 1:"
    - "  Soft Deleting 11 files: L0.160, L0.162, L0.165, L0.168, L0.171, L0.173, L0.176, L0.179, L0.182, L0.184, L0.187"
    - "  Creating 1 files"
    - "**** Simulation run 53, type=compact(ManySmallFiles). 20 Input Files, 79mb total:"
    - "L0                                                                                                                 "
    - "L0.53[356,668] 1us 4mb   |-----------------------------------------L0.53------------------------------------------|"
    - "L0.56[356,668] 1us 3mb   |-----------------------------------------L0.56------------------------------------------|"
    - "L0.59[356,668] 1us 4mb   |-----------------------------------------L0.59------------------------------------------|"
    - "L0.62[356,629] 1us 5mb   |-----------------------------------L0.62------------------------------------|            "
    - "L0.64[356,668] 1us 4mb   |-----------------------------------------L0.64------------------------------------------|"
    - "L0.67[356,668] 1us 3mb   |-----------------------------------------L0.67------------------------------------------|"
    - "L0.70[356,668] 1.01us 4mb|-----------------------------------------L0.70------------------------------------------|"
    - "L0.73[356,629] 1.01us 5mb|-----------------------------------L0.73------------------------------------|            "
    - "L0.75[356,668] 1.01us 4mb|-----------------------------------------L0.75------------------------------------------|"
    - "L0.78[356,668] 1.01us 3mb|-----------------------------------------L0.78------------------------------------------|"
    - "L0.81[356,668] 1.01us 4mb|-----------------------------------------L0.81------------------------------------------|"
    - "L0.84[356,629] 1.01us 5mb|-----------------------------------L0.84------------------------------------|            "
    - "L0.86[356,668] 1.01us 4mb|-----------------------------------------L0.86------------------------------------------|"
    - "L0.89[356,668] 1.01us 3mb|-----------------------------------------L0.89------------------------------------------|"
    - "L0.92[356,668] 1.01us 4mb|-----------------------------------------L0.92------------------------------------------|"
    - "L0.95[356,629] 1.01us 5mb|-----------------------------------L0.95------------------------------------|            "
    - "L0.97[356,668] 1.02us 4mb|-----------------------------------------L0.97------------------------------------------|"
    - "L0.100[356,668] 1.02us 3mb|-----------------------------------------L0.100-----------------------------------------|"
    - "L0.103[356,668] 1.02us 4mb|-----------------------------------------L0.103-----------------------------------------|"
    - "L0.106[356,629] 1.02us 5mb|-----------------------------------L0.106-----------------------------------|            "
    - "**** 1 Output Files (parquet_file_id not yet assigned), 79mb total:"
    - "L0, all files 79mb                                                                                                 "
    - "L0.?[356,668] 1.02us     |------------------------------------------L0.?------------------------------------------|"
    - "Committing partition 1:"
    - "  Soft Deleting 20 files: L0.53, L0.56, L0.59, L0.62, L0.64, L0.67, L0.70, L0.73, L0.75, L0.78, L0.81, L0.84, L0.86, L0.89, L0.92, L0.95, L0.97, L0.100, L0.103, L0.106"
    - "  Creating 1 files"
    - "**** Simulation run 54, type=compact(ManySmallFiles). 20 Input Files, 79mb total:"
    - "L0                                                                                                                 "
    - "L0.108[356,668] 1.02us 4mb|-----------------------------------------L0.108-----------------------------------------|"
    - "L0.111[356,668] 1.02us 3mb|-----------------------------------------L0.111-----------------------------------------|"
    - "L0.114[356,668] 1.02us 4mb|-----------------------------------------L0.114-----------------------------------------|"
    - "L0.117[356,629] 1.02us 5mb|-----------------------------------L0.117-----------------------------------|            "
    - "L0.119[356,668] 1.02us 4mb|-----------------------------------------L0.119-----------------------------------------|"
    - "L0.122[356,668] 1.02us 3mb|-----------------------------------------L0.122-----------------------------------------|"
    - "L0.125[356,668] 1.03us 4mb|-----------------------------------------L0.125-----------------------------------------|"
    - "L0.128[356,629] 1.03us 5mb|-----------------------------------L0.128-----------------------------------|            "
    - "L0.130[356,668] 1.03us 4mb|-----------------------------------------L0.130-----------------------------------------|"
    - "L0.133[356,668] 1.03us 3mb|-----------------------------------------L0.133-----------------------------------------|"
    - "L0.136[356,668] 1.03us 4mb|-----------------------------------------L0.136-----------------------------------------|"
    - "L0.139[356,629] 1.03us 5mb|-----------------------------------L0.139-----------------------------------|            "
    - "L0.141[356,668] 1.03us 4mb|-----------------------------------------L0.141-----------------------------------------|"
    - "L0.144[356,668] 1.03us 3mb|-----------------------------------------L0.144-----------------------------------------|"
    - "L0.147[356,668] 1.03us 4mb|-----------------------------------------L0.147-----------------------------------------|"
    - "L0.150[356,629] 1.03us 5mb|-----------------------------------L0.150-----------------------------------|            "
    - "L0.152[356,668] 1.04us 4mb|-----------------------------------------L0.152-----------------------------------------|"
    - "L0.155[356,668] 1.04us 3mb|-----------------------------------------L0.155-----------------------------------------|"
    - "L0.158[356,668] 1.04us 4mb|-----------------------------------------L0.158-----------------------------------------|"
    - "L0.161[356,629] 1.04us 5mb|-----------------------------------L0.161-----------------------------------|            "
    - "**** 1 Output Files (parquet_file_id not yet assigned), 79mb total:"
    - "L0, all files 79mb                                                                                                 "
    - "L0.?[356,668] 1.04us     |------------------------------------------L0.?------------------------------------------|"
    - "Committing partition 1:"
    - "  Soft Deleting 20 files: L0.108, L0.111, L0.114, L0.117, L0.119, L0.122, L0.125, L0.128, L0.130, L0.133, L0.136, L0.139, L0.141, L0.144, L0.147, L0.150, L0.152, L0.155, L0.158, L0.161"
    - "  Creating 1 files"
    - "**** Simulation run 55, type=compact(ManySmallFiles). 20 Input Files, 67mb total:"
    - "L0                                                                                                                 "
    - "L0.54[669,932] 1us 3mb   |---------------------------------L0.54----------------------------------|                "
    - "L0.57[669,986] 1us 3mb   |-----------------------------------------L0.57------------------------------------------|"
    - "L0.60[669,950] 1us 4mb   |------------------------------------L0.60------------------------------------|           "
    - "L0.65[669,932] 1us 3mb   |---------------------------------L0.65----------------------------------|                "
    - "L0.68[669,986] 1us 3mb   |-----------------------------------------L0.68------------------------------------------|"
    - "L0.71[669,950] 1.01us 4mb|------------------------------------L0.71------------------------------------|           "
    - "L0.76[669,932] 1.01us 3mb|---------------------------------L0.76----------------------------------|                "
    - "L0.79[669,986] 1.01us 3mb|-----------------------------------------L0.79------------------------------------------|"
    - "L0.82[669,950] 1.01us 4mb|------------------------------------L0.82------------------------------------|           "
    - "L0.87[669,932] 1.01us 3mb|---------------------------------L0.87----------------------------------|                "
    - "L0.90[669,986] 1.01us 3mb|-----------------------------------------L0.90------------------------------------------|"
    - "L0.93[669,950] 1.01us 4mb|------------------------------------L0.93------------------------------------|           "
    - "L0.98[669,932] 1.02us 3mb|---------------------------------L0.98----------------------------------|                "
    - "L0.101[669,986] 1.02us 3mb|-----------------------------------------L0.101-----------------------------------------|"
    - "L0.104[669,950] 1.02us 4mb|-----------------------------------L0.104------------------------------------|           "
    - "L0.109[669,932] 1.02us 3mb|---------------------------------L0.109---------------------------------|                "
    - "L0.112[669,986] 1.02us 3mb|-----------------------------------------L0.112-----------------------------------------|"
    - "L0.115[669,950] 1.02us 4mb|-----------------------------------L0.115------------------------------------|           "
    - "L0.120[669,932] 1.02us 3mb|---------------------------------L0.120---------------------------------|                "
    - "L0.123[669,986] 1.02us 3mb|-----------------------------------------L0.123-----------------------------------------|"
    - "**** 1 Output Files (parquet_file_id not yet assigned), 67mb total:"
    - "L0, all files 67mb                                                                                                 "
    - "L0.?[669,986] 1.02us     |------------------------------------------L0.?------------------------------------------|"
    - "Committing partition 1:"
    - "  Soft Deleting 20 files: L0.54, L0.57, L0.60, L0.65, L0.68, L0.71, L0.76, L0.79, L0.82, L0.87, L0.90, L0.93, L0.98, L0.101, L0.104, L0.109, L0.112, L0.115, L0.120, L0.123"
    - "  Creating 1 files"
    - "**** Simulation run 56, type=compact(ManySmallFiles). 18 Input Files, 60mb total:"
    - "L0                                                                                                                 "
    - "L0.126[669,950] 1.03us 4mb|-----------------------------------L0.126------------------------------------|           "
    - "L0.131[669,932] 1.03us 3mb|---------------------------------L0.131---------------------------------|                "
    - "L0.134[669,986] 1.03us 3mb|-----------------------------------------L0.134-----------------------------------------|"
    - "L0.137[669,950] 1.03us 4mb|-----------------------------------L0.137------------------------------------|           "
    - "L0.142[669,932] 1.03us 3mb|---------------------------------L0.142---------------------------------|                "
    - "L0.145[669,986] 1.03us 3mb|-----------------------------------------L0.145-----------------------------------------|"
    - "L0.148[669,950] 1.03us 4mb|-----------------------------------L0.148------------------------------------|           "
    - "L0.153[669,932] 1.04us 3mb|---------------------------------L0.153---------------------------------|                "
    - "L0.156[669,986] 1.04us 3mb|-----------------------------------------L0.156-----------------------------------------|"
    - "L0.159[669,950] 1.04us 4mb|-----------------------------------L0.159------------------------------------|           "
    - "L0.164[669,932] 1.04us 3mb|---------------------------------L0.164---------------------------------|                "
    - "L0.167[669,986] 1.04us 3mb|-----------------------------------------L0.167-----------------------------------------|"
    - "L0.170[669,950] 1.04us 4mb|-----------------------------------L0.170------------------------------------|           "
    - "L0.175[669,932] 1.04us 3mb|---------------------------------L0.175---------------------------------|                "
    - "L0.178[669,986] 1.05us 3mb|-----------------------------------------L0.178-----------------------------------------|"
    - "L0.181[669,950] 1.05us 4mb|-----------------------------------L0.181------------------------------------|           "
    - "L0.186[669,932] 1.05us 3mb|---------------------------------L0.186---------------------------------|                "
    - "L0.189[669,986] 1.05us 3mb|-----------------------------------------L0.189-----------------------------------------|"
    - "**** 1 Output Files (parquet_file_id not yet assigned), 60mb total:"
    - "L0, all files 60mb                                                                                                 "
    - "L0.?[669,986] 1.05us     |------------------------------------------L0.?------------------------------------------|"
    - "Committing partition 1:"
    - "  Soft Deleting 18 files: L0.126, L0.131, L0.134, L0.137, L0.142, L0.145, L0.148, L0.153, L0.156, L0.159, L0.164, L0.167, L0.170, L0.175, L0.178, L0.181, L0.186, L0.189"
    - "  Creating 1 files"
    - "**** Simulation run 57, type=compact(ManySmallFiles). 10 Input Files, 38mb total:"
    - "L0                                                                                                                 "
    - "L0.163[356,668] 1.04us 4mb|-----------------------------------------L0.163-----------------------------------------|"
    - "L0.166[356,668] 1.04us 3mb|-----------------------------------------L0.166-----------------------------------------|"
    - "L0.169[356,668] 1.04us 4mb|-----------------------------------------L0.169-----------------------------------------|"
    - "L0.172[356,629] 1.04us 5mb|-----------------------------------L0.172-----------------------------------|            "
    - "L0.174[356,668] 1.04us 4mb|-----------------------------------------L0.174-----------------------------------------|"
    - "L0.177[356,668] 1.05us 3mb|-----------------------------------------L0.177-----------------------------------------|"
    - "L0.180[356,668] 1.05us 4mb|-----------------------------------------L0.180-----------------------------------------|"
    - "L0.183[356,629] 1.05us 5mb|-----------------------------------L0.183-----------------------------------|            "
    - "L0.185[356,668] 1.05us 4mb|-----------------------------------------L0.185-----------------------------------------|"
    - "L0.188[356,668] 1.05us 3mb|-----------------------------------------L0.188-----------------------------------------|"
    - "**** 1 Output Files (parquet_file_id not yet assigned), 38mb total:"
    - "L0, all files 38mb                                                                                                 "
    - "L0.?[356,668] 1.05us     |------------------------------------------L0.?------------------------------------------|"
    - "Committing partition 1:"
    - "  Soft Deleting 10 files: L0.163, L0.166, L0.169, L0.172, L0.174, L0.177, L0.180, L0.183, L0.185, L0.188"
    - "  Creating 1 files"
    - "**** Simulation run 58, type=split(CompactAndSplitOutput(FoundSubsetLessThanMaxCompactSize))(split_times=[338, 676]). 4 Input Files, 292mb total:"
    - "L0                                                                                                                 "
    - "L0.190[0,355] 1.02us 76mb|------------L0.190------------|                                                          "
    - "L0.193[356,668] 1.02us 79mb                                |----------L0.193----------|                              "
    - "L0.194[669,986] 1.02us 67mb                                                             |----------L0.194----------| "
    - "L0.191[42,355] 1.04us 71mb   |----------L0.191----------|                                                           "
    - "**** 3 Output Files (parquet_file_id not yet assigned), 292mb total:"
    - "L1                                                                                                                 "
    - "L1.?[0,338] 1.04us 100mb |------------L1.?------------|                                                            "
    - "L1.?[339,676] 1.04us 100mb                              |------------L1.?------------|                              "
    - "L1.?[677,986] 1.04us 92mb                                                             |-----------L1.?-----------| "
    - "Committing partition 1:"
    - "  Soft Deleting 4 files: L0.190, L0.191, L0.193, L0.194"
    - "  Creating 3 files"
    - "**** Simulation run 59, type=split(ReduceOverlap)(split_times=[676]). 1 Input Files, 60mb total:"
    - "L0, all files 60mb                                                                                                 "
    - "L0.195[669,986] 1.05us   |-----------------------------------------L0.195-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 60mb total:"
    - "L0                                                                                                                 "
    - "L0.?[669,676] 1.05us 2mb |L0.?|                                                                                    "
    - "L0.?[677,986] 1.05us 59mb  |----------------------------------------L0.?-----------------------------------------| "
    - "**** Simulation run 60, type=split(ReduceOverlap)(split_times=[338]). 1 Input Files, 40mb total:"
    - "L0, all files 40mb                                                                                                 "
    - "L0.192[42,355] 1.05us    |-----------------------------------------L0.192-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 40mb total:"
    - "L0                                                                                                                 "
    - "L0.?[42,338] 1.05us 38mb |---------------------------------------L0.?----------------------------------------|     "
    - "L0.?[339,355] 1.05us 2mb                                                                                      |L0.?|"
    - "Committing partition 1:"
    - "  Soft Deleting 2 files: L0.192, L0.195"
    - "  Creating 4 files"
    - "**** Simulation run 61, type=split(CompactAndSplitOutput(FoundSubsetLessThanMaxCompactSize))(split_times=[528]). 2 Input Files, 179mb total:"
    - "L0                                                                                                                 "
    - "L0.196[356,668] 1.04us 79mb    |-------------------------------------L0.196--------------------------------------|   "
    - "L1                                                                                                                 "
    - "L1.199[339,676] 1.04us 100mb|-----------------------------------------L1.199-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 179mb total:"
    - "L1                                                                                                                 "
    - "L1.?[339,528] 1.04us 100mb|----------------------L1.?----------------------|                                        "
    - "L1.?[529,676] 1.04us 78mb                                                  |----------------L1.?-----------------| "
    - "Committing partition 1:"
    - "  Soft Deleting 2 files: L0.196, L1.199"
    - "  Creating 2 files"
    - "**** Simulation run 62, type=split(ReduceOverlap)(split_times=[528]). 1 Input Files, 38mb total:"
    - "L0, all files 38mb                                                                                                 "
    - "L0.197[356,668] 1.05us   |-----------------------------------------L0.197-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 38mb total:"
    - "L0                                                                                                                 "
    - "L0.?[356,528] 1.05us 21mb|---------------------L0.?----------------------|                                         "
    - "L0.?[529,668] 1.05us 17mb                                                 |-----------------L0.?-----------------| "
    - "Committing partition 1:"
    - "  Soft Deleting 1 files: L0.197"
    - "  Creating 2 files"
    - "**** Simulation run 63, type=split(CompactAndSplitOutput(FoundSubsetLessThanMaxCompactSize))(split_times=[202, 404]). 5 Input Files, 262mb total:"
    - "L0                                                                                                                 "
    - "L0.203[42,338] 1.05us 38mb       |---------------------L0.203---------------------|                                 "
    - "L0.204[339,355] 1.05us 2mb                                                         |L0.204|                         "
    - "L0.207[356,528] 1.05us 21mb                                                            |----------L0.207-----------| "
    - "L1                                                                                                                 "
    - "L1.198[0,338] 1.04us 100mb|------------------------L1.198-------------------------|                                 "
    - "L1.205[339,528] 1.04us 100mb                                                         |------------L1.205------------| "
    - "**** 3 Output Files (parquet_file_id not yet assigned), 262mb total:"
    - "L1                                                                                                                 "
    - "L1.?[0,202] 1.05us 101mb |--------------L1.?--------------|                                                        "
    - "L1.?[203,404] 1.05us 100mb                                  |--------------L1.?--------------|                      "
    - "L1.?[405,528] 1.05us 61mb                                                                     |-------L1.?-------| "
    - "Committing partition 1:"
    - "  Soft Deleting 5 files: L1.198, L0.203, L0.204, L1.205, L0.207"
    - "  Creating 3 files"
    - "**** Simulation run 64, type=split(CompactAndSplitOutput(TotalSizeLessThanMaxCompactSize))(split_times=[714, 899]). 5 Input Files, 248mb total:"
    - "L0                                                                                                                 "
    - "L0.202[677,986] 1.05us 59mb                             |--------------------------L0.202--------------------------| "
    - "L0.201[669,676] 1.05us 2mb                           |L0.201|                                                       "
    - "L0.208[529,668] 1.05us 17mb|---------L0.208----------|                                                               "
    - "L1                                                                                                                 "
    - "L1.200[677,986] 1.04us 92mb                             |--------------------------L1.200--------------------------| "
    - "L1.206[529,676] 1.04us 78mb|----------L1.206----------|                                                              "
    - "**** 3 Output Files (parquet_file_id not yet assigned), 248mb total:"
    - "L1                                                                                                                 "
    - "L1.?[529,714] 1.05us 101mb|---------------L1.?---------------|                                                      "
    - "L1.?[715,899] 1.05us 100mb                                    |---------------L1.?---------------|                  "
    - "L1.?[900,986] 1.05us 47mb                                                                         |-----L1.?-----| "
    - "Committing partition 1:"
    - "  Soft Deleting 5 files: L1.200, L0.201, L0.202, L1.206, L0.208"
    - "  Creating 3 files"
    - "**** Simulation run 65, type=split(CompactAndSplitOutput(FoundSubsetLessThanMaxCompactSize))(split_times=[594, 783]). 3 Input Files, 262mb total:"
    - "L1                                                                                                                 "
    - "L1.211[405,528] 1.05us 61mb|-------L1.211-------|                                                                    "
    - "L1.212[529,714] 1.05us 101mb                      |------------L1.212-------------|                                   "
    - "L1.213[715,899] 1.05us 100mb                                                        |------------L1.213-------------| "
    - "**** 3 Output Files (parquet_file_id not yet assigned), 262mb total:"
    - "L2                                                                                                                 "
    - "L2.?[405,594] 1.05us 101mb|--------------L2.?--------------|                                                        "
    - "L2.?[595,783] 1.05us 100mb                                  |--------------L2.?--------------|                      "
    - "L2.?[784,899] 1.05us 61mb                                                                     |-------L2.?-------| "
    - "Committing partition 1:"
    - "  Soft Deleting 3 files: L1.211, L1.212, L1.213"
    - "  Upgrading 2 files level to CompactionLevel::L2: L1.209, L1.210"
    - "  Creating 3 files"
    - "**** Final Output Files (2.34gb written)"
    - "L1                                                                                                                 "
    - "L1.214[900,986] 1.05us 47mb                                                                                  |L1.214|"
    - "L2                                                                                                                 "
    - "L2.209[0,202] 1.05us 101mb|-----L2.209-----|                                                                        "
    - "L2.210[203,404] 1.05us 100mb                  |-----L2.210-----|                                                      "
    - "L2.215[405,594] 1.05us 101mb                                    |----L2.215-----|                                     "
    - "L2.216[595,783] 1.05us 100mb                                                      |----L2.216-----|                   "
    - "L2.217[784,899] 1.05us 61mb                                                                       |-L2.217-|         "
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
    - "**** Simulation run 0, type=split(VerticalSplit)(split_times=[355, 668]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.11[76,932] 1us        |-----------------------------------------L0.11------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[76,355] 1us 3mb     |-----------L0.?------------|                                                             "
    - "L0.?[356,668] 1us 4mb                                 |-------------L0.?-------------|                             "
    - "L0.?[669,932] 1us 3mb                                                                  |----------L0.?-----------| "
    - "**** Simulation run 1, type=split(VerticalSplit)(split_times=[355, 668]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.12[42,986] 1us        |-----------------------------------------L0.12------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[42,355] 1us 3mb     |-----------L0.?------------|                                                             "
    - "L0.?[356,668] 1us 3mb                                 |-----------L0.?------------|                                "
    - "L0.?[669,986] 1us 3mb                                                               |------------L0.?------------| "
    - "**** Simulation run 2, type=split(VerticalSplit)(split_times=[355, 668]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.13[173,950] 1us       |-----------------------------------------L0.13------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[173,355] 1us 2mb    |-------L0.?--------|                                                                     "
    - "L0.?[356,668] 1us 4mb                         |---------------L0.?---------------|                                 "
    - "L0.?[669,950] 1us 4mb                                                             |-------------L0.?-------------| "
    - "**** Simulation run 3, type=split(VerticalSplit)(split_times=[355]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.14[50,629] 1us        |-----------------------------------------L0.14------------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[50,355] 1us 5mb     |--------------------L0.?---------------------|                                           "
    - "L0.?[356,629] 1us 5mb                                                   |------------------L0.?------------------| "
    - "**** Simulation run 4, type=split(VerticalSplit)(split_times=[355, 668]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.15[76,932] 1us        |-----------------------------------------L0.15------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[76,355] 1us 3mb     |-----------L0.?------------|                                                             "
    - "L0.?[356,668] 1us 4mb                                 |-------------L0.?-------------|                             "
    - "L0.?[669,932] 1us 3mb                                                                  |----------L0.?-----------| "
    - "**** Simulation run 5, type=split(VerticalSplit)(split_times=[355, 668]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.16[42,986] 1us        |-----------------------------------------L0.16------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[42,355] 1us 3mb     |-----------L0.?------------|                                                             "
    - "L0.?[356,668] 1us 3mb                                 |-----------L0.?------------|                                "
    - "L0.?[669,986] 1us 3mb                                                               |------------L0.?------------| "
    - "**** Simulation run 6, type=split(VerticalSplit)(split_times=[355, 668]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.17[173,950] 1.01us    |-----------------------------------------L0.17------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[173,355] 1.01us 2mb |-------L0.?--------|                                                                     "
    - "L0.?[356,668] 1.01us 4mb                      |---------------L0.?---------------|                                 "
    - "L0.?[669,950] 1.01us 4mb                                                          |-------------L0.?-------------| "
    - "**** Simulation run 7, type=split(VerticalSplit)(split_times=[355]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.18[50,629] 1.01us     |-----------------------------------------L0.18------------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[50,355] 1.01us 5mb  |--------------------L0.?---------------------|                                           "
    - "L0.?[356,629] 1.01us 5mb                                                |------------------L0.?------------------| "
    - "**** Simulation run 8, type=split(VerticalSplit)(split_times=[355, 668]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.19[76,932] 1.01us     |-----------------------------------------L0.19------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[76,355] 1.01us 3mb  |-----------L0.?------------|                                                             "
    - "L0.?[356,668] 1.01us 4mb                              |-------------L0.?-------------|                             "
    - "L0.?[669,932] 1.01us 3mb                                                               |----------L0.?-----------| "
    - "**** Simulation run 9, type=split(VerticalSplit)(split_times=[355, 668]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.20[42,986] 1.01us     |-----------------------------------------L0.20------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[42,355] 1.01us 3mb  |-----------L0.?------------|                                                             "
    - "L0.?[356,668] 1.01us 3mb                              |-----------L0.?------------|                                "
    - "L0.?[669,986] 1.01us 3mb                                                            |------------L0.?------------| "
    - "**** Simulation run 10, type=split(VerticalSplit)(split_times=[355, 668]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.21[173,950] 1.01us    |-----------------------------------------L0.21------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[173,355] 1.01us 2mb |-------L0.?--------|                                                                     "
    - "L0.?[356,668] 1.01us 4mb                      |---------------L0.?---------------|                                 "
    - "L0.?[669,950] 1.01us 4mb                                                          |-------------L0.?-------------| "
    - "**** Simulation run 11, type=split(VerticalSplit)(split_times=[355]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.22[50,629] 1.01us     |-----------------------------------------L0.22------------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[50,355] 1.01us 5mb  |--------------------L0.?---------------------|                                           "
    - "L0.?[356,629] 1.01us 5mb                                                |------------------L0.?------------------| "
    - "**** Simulation run 12, type=split(VerticalSplit)(split_times=[355, 668]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.23[76,932] 1.01us     |-----------------------------------------L0.23------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[76,355] 1.01us 3mb  |-----------L0.?------------|                                                             "
    - "L0.?[356,668] 1.01us 4mb                              |-------------L0.?-------------|                             "
    - "L0.?[669,932] 1.01us 3mb                                                               |----------L0.?-----------| "
    - "**** Simulation run 13, type=split(VerticalSplit)(split_times=[355, 668]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.24[42,986] 1.01us     |-----------------------------------------L0.24------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[42,355] 1.01us 3mb  |-----------L0.?------------|                                                             "
    - "L0.?[356,668] 1.01us 3mb                              |-----------L0.?------------|                                "
    - "L0.?[669,986] 1.01us 3mb                                                            |------------L0.?------------| "
    - "**** Simulation run 14, type=split(VerticalSplit)(split_times=[355, 668]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.25[173,950] 1.01us    |-----------------------------------------L0.25------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[173,355] 1.01us 2mb |-------L0.?--------|                                                                     "
    - "L0.?[356,668] 1.01us 4mb                      |---------------L0.?---------------|                                 "
    - "L0.?[669,950] 1.01us 4mb                                                          |-------------L0.?-------------| "
    - "**** Simulation run 15, type=split(VerticalSplit)(split_times=[355]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.26[50,629] 1.01us     |-----------------------------------------L0.26------------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[50,355] 1.01us 5mb  |--------------------L0.?---------------------|                                           "
    - "L0.?[356,629] 1.01us 5mb                                                |------------------L0.?------------------| "
    - "**** Simulation run 16, type=split(VerticalSplit)(split_times=[355, 668]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.27[76,932] 1.02us     |-----------------------------------------L0.27------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[76,355] 1.02us 3mb  |-----------L0.?------------|                                                             "
    - "L0.?[356,668] 1.02us 4mb                              |-------------L0.?-------------|                             "
    - "L0.?[669,932] 1.02us 3mb                                                               |----------L0.?-----------| "
    - "**** Simulation run 17, type=split(VerticalSplit)(split_times=[355, 668]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.28[42,986] 1.02us     |-----------------------------------------L0.28------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[42,355] 1.02us 3mb  |-----------L0.?------------|                                                             "
    - "L0.?[356,668] 1.02us 3mb                              |-----------L0.?------------|                                "
    - "L0.?[669,986] 1.02us 3mb                                                            |------------L0.?------------| "
    - "**** Simulation run 18, type=split(VerticalSplit)(split_times=[355, 668]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.29[173,950] 1.02us    |-----------------------------------------L0.29------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[173,355] 1.02us 2mb |-------L0.?--------|                                                                     "
    - "L0.?[356,668] 1.02us 4mb                      |---------------L0.?---------------|                                 "
    - "L0.?[669,950] 1.02us 4mb                                                          |-------------L0.?-------------| "
    - "**** Simulation run 19, type=split(VerticalSplit)(split_times=[355]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.30[50,629] 1.02us     |-----------------------------------------L0.30------------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[50,355] 1.02us 5mb  |--------------------L0.?---------------------|                                           "
    - "L0.?[356,629] 1.02us 5mb                                                |------------------L0.?------------------| "
    - "**** Simulation run 20, type=split(VerticalSplit)(split_times=[355, 668]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.31[76,932] 1.02us     |-----------------------------------------L0.31------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[76,355] 1.02us 3mb  |-----------L0.?------------|                                                             "
    - "L0.?[356,668] 1.02us 4mb                              |-------------L0.?-------------|                             "
    - "L0.?[669,932] 1.02us 3mb                                                               |----------L0.?-----------| "
    - "**** Simulation run 21, type=split(VerticalSplit)(split_times=[355, 668]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.32[42,986] 1.02us     |-----------------------------------------L0.32------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[42,355] 1.02us 3mb  |-----------L0.?------------|                                                             "
    - "L0.?[356,668] 1.02us 3mb                              |-----------L0.?------------|                                "
    - "L0.?[669,986] 1.02us 3mb                                                            |------------L0.?------------| "
    - "**** Simulation run 22, type=split(VerticalSplit)(split_times=[355, 668]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.33[173,950] 1.02us    |-----------------------------------------L0.33------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[173,355] 1.02us 2mb |-------L0.?--------|                                                                     "
    - "L0.?[356,668] 1.02us 4mb                      |---------------L0.?---------------|                                 "
    - "L0.?[669,950] 1.02us 4mb                                                          |-------------L0.?-------------| "
    - "**** Simulation run 23, type=split(VerticalSplit)(split_times=[355]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.34[50,629] 1.02us     |-----------------------------------------L0.34------------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[50,355] 1.02us 5mb  |--------------------L0.?---------------------|                                           "
    - "L0.?[356,629] 1.02us 5mb                                                |------------------L0.?------------------| "
    - "**** Simulation run 24, type=split(VerticalSplit)(split_times=[355, 668]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.35[76,932] 1.02us     |-----------------------------------------L0.35------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[76,355] 1.02us 3mb  |-----------L0.?------------|                                                             "
    - "L0.?[356,668] 1.02us 4mb                              |-------------L0.?-------------|                             "
    - "L0.?[669,932] 1.02us 3mb                                                               |----------L0.?-----------| "
    - "**** Simulation run 25, type=split(VerticalSplit)(split_times=[355, 668]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.36[42,986] 1.02us     |-----------------------------------------L0.36------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[42,355] 1.02us 3mb  |-----------L0.?------------|                                                             "
    - "L0.?[356,668] 1.02us 3mb                              |-----------L0.?------------|                                "
    - "L0.?[669,986] 1.02us 3mb                                                            |------------L0.?------------| "
    - "**** Simulation run 26, type=split(VerticalSplit)(split_times=[355, 668]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.37[173,950] 1.03us    |-----------------------------------------L0.37------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[173,355] 1.03us 2mb |-------L0.?--------|                                                                     "
    - "L0.?[356,668] 1.03us 4mb                      |---------------L0.?---------------|                                 "
    - "L0.?[669,950] 1.03us 4mb                                                          |-------------L0.?-------------| "
    - "**** Simulation run 27, type=split(VerticalSplit)(split_times=[355]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.38[50,629] 1.03us     |-----------------------------------------L0.38------------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[50,355] 1.03us 5mb  |--------------------L0.?---------------------|                                           "
    - "L0.?[356,629] 1.03us 5mb                                                |------------------L0.?------------------| "
    - "**** Simulation run 28, type=split(VerticalSplit)(split_times=[355, 668]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.39[76,932] 1.03us     |-----------------------------------------L0.39------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[76,355] 1.03us 3mb  |-----------L0.?------------|                                                             "
    - "L0.?[356,668] 1.03us 4mb                              |-------------L0.?-------------|                             "
    - "L0.?[669,932] 1.03us 3mb                                                               |----------L0.?-----------| "
    - "**** Simulation run 29, type=split(VerticalSplit)(split_times=[355, 668]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.40[42,986] 1.03us     |-----------------------------------------L0.40------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[42,355] 1.03us 3mb  |-----------L0.?------------|                                                             "
    - "L0.?[356,668] 1.03us 3mb                              |-----------L0.?------------|                                "
    - "L0.?[669,986] 1.03us 3mb                                                            |------------L0.?------------| "
    - "**** Simulation run 30, type=split(VerticalSplit)(split_times=[355, 668]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.41[173,950] 1.03us    |-----------------------------------------L0.41------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[173,355] 1.03us 2mb |-------L0.?--------|                                                                     "
    - "L0.?[356,668] 1.03us 4mb                      |---------------L0.?---------------|                                 "
    - "L0.?[669,950] 1.03us 4mb                                                          |-------------L0.?-------------| "
    - "**** Simulation run 31, type=split(VerticalSplit)(split_times=[355]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.42[50,629] 1.03us     |-----------------------------------------L0.42------------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[50,355] 1.03us 5mb  |--------------------L0.?---------------------|                                           "
    - "L0.?[356,629] 1.03us 5mb                                                |------------------L0.?------------------| "
    - "**** Simulation run 32, type=split(VerticalSplit)(split_times=[355, 668]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.43[76,932] 1.03us     |-----------------------------------------L0.43------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[76,355] 1.03us 3mb  |-----------L0.?------------|                                                             "
    - "L0.?[356,668] 1.03us 4mb                              |-------------L0.?-------------|                             "
    - "L0.?[669,932] 1.03us 3mb                                                               |----------L0.?-----------| "
    - "**** Simulation run 33, type=split(VerticalSplit)(split_times=[355, 668]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.44[42,986] 1.03us     |-----------------------------------------L0.44------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[42,355] 1.03us 3mb  |-----------L0.?------------|                                                             "
    - "L0.?[356,668] 1.03us 3mb                              |-----------L0.?------------|                                "
    - "L0.?[669,986] 1.03us 3mb                                                            |------------L0.?------------| "
    - "**** Simulation run 34, type=split(VerticalSplit)(split_times=[355, 668]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.45[173,950] 1.03us    |-----------------------------------------L0.45------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[173,355] 1.03us 2mb |-------L0.?--------|                                                                     "
    - "L0.?[356,668] 1.03us 4mb                      |---------------L0.?---------------|                                 "
    - "L0.?[669,950] 1.03us 4mb                                                          |-------------L0.?-------------| "
    - "**** Simulation run 35, type=split(VerticalSplit)(split_times=[355]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.46[50,629] 1.03us     |-----------------------------------------L0.46------------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[50,355] 1.03us 5mb  |--------------------L0.?---------------------|                                           "
    - "L0.?[356,629] 1.03us 5mb                                                |------------------L0.?------------------| "
    - "**** Simulation run 36, type=split(VerticalSplit)(split_times=[355, 668]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.47[76,932] 1.04us     |-----------------------------------------L0.47------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[76,355] 1.04us 3mb  |-----------L0.?------------|                                                             "
    - "L0.?[356,668] 1.04us 4mb                              |-------------L0.?-------------|                             "
    - "L0.?[669,932] 1.04us 3mb                                                               |----------L0.?-----------| "
    - "**** Simulation run 37, type=split(VerticalSplit)(split_times=[355, 668]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.48[42,986] 1.04us     |-----------------------------------------L0.48------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[42,355] 1.04us 3mb  |-----------L0.?------------|                                                             "
    - "L0.?[356,668] 1.04us 3mb                              |-----------L0.?------------|                                "
    - "L0.?[669,986] 1.04us 3mb                                                            |------------L0.?------------| "
    - "**** Simulation run 38, type=split(VerticalSplit)(split_times=[355, 668]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.49[173,950] 1.04us    |-----------------------------------------L0.49------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[173,355] 1.04us 2mb |-------L0.?--------|                                                                     "
    - "L0.?[356,668] 1.04us 4mb                      |---------------L0.?---------------|                                 "
    - "L0.?[669,950] 1.04us 4mb                                                          |-------------L0.?-------------| "
    - "**** Simulation run 39, type=split(VerticalSplit)(split_times=[355]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.50[50,629] 1.04us     |-----------------------------------------L0.50------------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[50,355] 1.04us 5mb  |--------------------L0.?---------------------|                                           "
    - "L0.?[356,629] 1.04us 5mb                                                |------------------L0.?------------------| "
    - "Committing partition 1:"
    - "  Soft Deleting 40 files: L0.11, L0.12, L0.13, L0.14, L0.15, L0.16, L0.17, L0.18, L0.19, L0.20, L0.21, L0.22, L0.23, L0.24, L0.25, L0.26, L0.27, L0.28, L0.29, L0.30, L0.31, L0.32, L0.33, L0.34, L0.35, L0.36, L0.37, L0.38, L0.39, L0.40, L0.41, L0.42, L0.43, L0.44, L0.45, L0.46, L0.47, L0.48, L0.49, L0.50"
    - "  Creating 110 files"
    - "**** Simulation run 40, type=split(VerticalSplit)(split_times=[355, 668]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.51[76,932] 1.04us     |-----------------------------------------L0.51------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[76,355] 1.04us 3mb  |-----------L0.?------------|                                                             "
    - "L0.?[356,668] 1.04us 4mb                              |-------------L0.?-------------|                             "
    - "L0.?[669,932] 1.04us 3mb                                                               |----------L0.?-----------| "
    - "**** Simulation run 41, type=split(VerticalSplit)(split_times=[355, 668]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.52[42,986] 1.04us     |-----------------------------------------L0.52------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[42,355] 1.04us 3mb  |-----------L0.?------------|                                                             "
    - "L0.?[356,668] 1.04us 3mb                              |-----------L0.?------------|                                "
    - "L0.?[669,986] 1.04us 3mb                                                            |------------L0.?------------| "
    - "**** Simulation run 42, type=split(VerticalSplit)(split_times=[355, 668]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.53[173,950] 1.04us    |-----------------------------------------L0.53------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[173,355] 1.04us 2mb |-------L0.?--------|                                                                     "
    - "L0.?[356,668] 1.04us 4mb                      |---------------L0.?---------------|                                 "
    - "L0.?[669,950] 1.04us 4mb                                                          |-------------L0.?-------------| "
    - "**** Simulation run 43, type=split(VerticalSplit)(split_times=[355]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.54[50,629] 1.04us     |-----------------------------------------L0.54------------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[50,355] 1.04us 5mb  |--------------------L0.?---------------------|                                           "
    - "L0.?[356,629] 1.04us 5mb                                                |------------------L0.?------------------| "
    - "**** Simulation run 44, type=split(VerticalSplit)(split_times=[355, 668]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.55[76,932] 1.04us     |-----------------------------------------L0.55------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[76,355] 1.04us 3mb  |-----------L0.?------------|                                                             "
    - "L0.?[356,668] 1.04us 4mb                              |-------------L0.?-------------|                             "
    - "L0.?[669,932] 1.04us 3mb                                                               |----------L0.?-----------| "
    - "**** Simulation run 45, type=split(VerticalSplit)(split_times=[355, 668]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.56[42,986] 1.05us     |-----------------------------------------L0.56------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[42,355] 1.05us 3mb  |-----------L0.?------------|                                                             "
    - "L0.?[356,668] 1.05us 3mb                              |-----------L0.?------------|                                "
    - "L0.?[669,986] 1.05us 3mb                                                            |------------L0.?------------| "
    - "**** Simulation run 46, type=split(VerticalSplit)(split_times=[355, 668]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.57[173,950] 1.05us    |-----------------------------------------L0.57------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[173,355] 1.05us 2mb |-------L0.?--------|                                                                     "
    - "L0.?[356,668] 1.05us 4mb                      |---------------L0.?---------------|                                 "
    - "L0.?[669,950] 1.05us 4mb                                                          |-------------L0.?-------------| "
    - "**** Simulation run 47, type=split(VerticalSplit)(split_times=[355]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.58[50,629] 1.05us     |-----------------------------------------L0.58------------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[50,355] 1.05us 5mb  |--------------------L0.?---------------------|                                           "
    - "L0.?[356,629] 1.05us 5mb                                                |------------------L0.?------------------| "
    - "**** Simulation run 48, type=split(VerticalSplit)(split_times=[355, 668]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.59[76,932] 1.05us     |-----------------------------------------L0.59------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[76,355] 1.05us 3mb  |-----------L0.?------------|                                                             "
    - "L0.?[356,668] 1.05us 4mb                              |-------------L0.?-------------|                             "
    - "L0.?[669,932] 1.05us 3mb                                                               |----------L0.?-----------| "
    - "**** Simulation run 49, type=split(VerticalSplit)(split_times=[355, 668]). 1 Input Files, 10mb total:"
    - "L0, all files 10mb                                                                                                 "
    - "L0.60[42,986] 1.05us     |-----------------------------------------L0.60------------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 10mb total:"
    - "L0                                                                                                                 "
    - "L0.?[42,355] 1.05us 3mb  |-----------L0.?------------|                                                             "
    - "L0.?[356,668] 1.05us 3mb                              |-----------L0.?------------|                                "
    - "L0.?[669,986] 1.05us 3mb                                                            |------------L0.?------------| "
    - "Committing partition 1:"
    - "  Soft Deleting 10 files: L0.51, L0.52, L0.53, L0.54, L0.55, L0.56, L0.57, L0.58, L0.59, L0.60"
    - "  Creating 28 files"
    - "**** Simulation run 50, type=compact(ManySmallFiles). 10 Input Files, 35mb total:"
    - "L0                                                                                                                 "
    - "L0.61[76,355] 1us 3mb             |------------------------------------L0.61-------------------------------------| "
    - "L0.64[42,355] 1us 3mb    |-----------------------------------------L0.64------------------------------------------|"
    - "L0.67[173,355] 1us 2mb                                        |----------------------L0.67-----------------------| "
    - "L0.70[50,355] 1us 5mb      |----------------------------------------L0.70----------------------------------------| "
    - "L0.72[76,355] 1us 3mb             |------------------------------------L0.72-------------------------------------| "
    - "L0.75[42,355] 1us 3mb    |-----------------------------------------L0.75------------------------------------------|"
    - "L0.78[173,355] 1.01us 2mb                                     |----------------------L0.78-----------------------| "
    - "L0.81[50,355] 1.01us 5mb   |----------------------------------------L0.81----------------------------------------| "
    - "L0.83[76,355] 1.01us 3mb          |------------------------------------L0.83-------------------------------------| "
    - "L0.86[42,355] 1.01us 3mb |-----------------------------------------L0.86------------------------------------------|"
    - "**** 1 Output Files (parquet_file_id not yet assigned), 35mb total:"
    - "L0, all files 35mb                                                                                                 "
    - "L0.?[42,355] 1.01us      |------------------------------------------L0.?------------------------------------------|"
    - "Committing partition 1:"
    - "  Soft Deleting 10 files: L0.61, L0.64, L0.67, L0.70, L0.72, L0.75, L0.78, L0.81, L0.83, L0.86"
    - "  Creating 1 files"
    - "**** Simulation run 51, type=compact(ManySmallFiles). 10 Input Files, 36mb total:"
    - "L0                                                                                                                 "
    - "L0.89[173,355] 1.01us 2mb                                     |----------------------L0.89-----------------------| "
    - "L0.92[50,355] 1.01us 5mb   |----------------------------------------L0.92----------------------------------------| "
    - "L0.94[76,355] 1.01us 3mb          |------------------------------------L0.94-------------------------------------| "
    - "L0.97[42,355] 1.01us 3mb |-----------------------------------------L0.97------------------------------------------|"
    - "L0.100[173,355] 1.01us 2mb                                     |----------------------L0.100----------------------| "
    - "L0.103[50,355] 1.01us 5mb  |---------------------------------------L0.103----------------------------------------| "
    - "L0.105[76,355] 1.02us 3mb         |------------------------------------L0.105------------------------------------| "
    - "L0.108[42,355] 1.02us 3mb|-----------------------------------------L0.108-----------------------------------------|"
    - "L0.111[173,355] 1.02us 2mb                                     |----------------------L0.111----------------------| "
    - "L0.114[50,355] 1.02us 5mb  |---------------------------------------L0.114----------------------------------------| "
    - "**** 1 Output Files (parquet_file_id not yet assigned), 36mb total:"
    - "L0, all files 36mb                                                                                                 "
    - "L0.?[42,355] 1.02us      |------------------------------------------L0.?------------------------------------------|"
    - "Committing partition 1:"
    - "  Soft Deleting 10 files: L0.89, L0.92, L0.94, L0.97, L0.100, L0.103, L0.105, L0.108, L0.111, L0.114"
    - "  Creating 1 files"
    - "**** Simulation run 52, type=compact(ManySmallFiles). 10 Input Files, 35mb total:"
    - "L0                                                                                                                 "
    - "L0.116[76,355] 1.02us 3mb         |------------------------------------L0.116------------------------------------| "
    - "L0.119[42,355] 1.02us 3mb|-----------------------------------------L0.119-----------------------------------------|"
    - "L0.122[173,355] 1.02us 2mb                                     |----------------------L0.122----------------------| "
    - "L0.125[50,355] 1.02us 5mb  |---------------------------------------L0.125----------------------------------------| "
    - "L0.127[76,355] 1.02us 3mb         |------------------------------------L0.127------------------------------------| "
    - "L0.130[42,355] 1.02us 3mb|-----------------------------------------L0.130-----------------------------------------|"
    - "L0.133[173,355] 1.03us 2mb                                     |----------------------L0.133----------------------| "
    - "L0.136[50,355] 1.03us 5mb  |---------------------------------------L0.136----------------------------------------| "
    - "L0.138[76,355] 1.03us 3mb         |------------------------------------L0.138------------------------------------| "
    - "L0.141[42,355] 1.03us 3mb|-----------------------------------------L0.141-----------------------------------------|"
    - "**** 1 Output Files (parquet_file_id not yet assigned), 35mb total:"
    - "L0, all files 35mb                                                                                                 "
    - "L0.?[42,355] 1.03us      |------------------------------------------L0.?------------------------------------------|"
    - "Committing partition 1:"
    - "  Soft Deleting 10 files: L0.116, L0.119, L0.122, L0.125, L0.127, L0.130, L0.133, L0.136, L0.138, L0.141"
    - "  Creating 1 files"
    - "**** Simulation run 53, type=compact(ManySmallFiles). 10 Input Files, 36mb total:"
    - "L0                                                                                                                 "
    - "L0.144[173,355] 1.03us 2mb                                     |----------------------L0.144----------------------| "
    - "L0.147[50,355] 1.03us 5mb  |---------------------------------------L0.147----------------------------------------| "
    - "L0.149[76,355] 1.03us 3mb         |------------------------------------L0.149------------------------------------| "
    - "L0.152[42,355] 1.03us 3mb|-----------------------------------------L0.152-----------------------------------------|"
    - "L0.155[173,355] 1.03us 2mb                                     |----------------------L0.155----------------------| "
    - "L0.158[50,355] 1.03us 5mb  |---------------------------------------L0.158----------------------------------------| "
    - "L0.160[76,355] 1.04us 3mb         |------------------------------------L0.160------------------------------------| "
    - "L0.163[42,355] 1.04us 3mb|-----------------------------------------L0.163-----------------------------------------|"
    - "L0.166[173,355] 1.04us 2mb                                     |----------------------L0.166----------------------| "
    - "L0.169[50,355] 1.04us 5mb  |---------------------------------------L0.169----------------------------------------| "
    - "**** 1 Output Files (parquet_file_id not yet assigned), 36mb total:"
    - "L0, all files 36mb                                                                                                 "
    - "L0.?[42,355] 1.04us      |------------------------------------------L0.?------------------------------------------|"
    - "Committing partition 1:"
    - "  Soft Deleting 10 files: L0.144, L0.147, L0.149, L0.152, L0.155, L0.158, L0.160, L0.163, L0.166, L0.169"
    - "  Creating 1 files"
    - "**** Simulation run 54, type=compact(ManySmallFiles). 10 Input Files, 35mb total:"
    - "L0                                                                                                                 "
    - "L0.171[76,355] 1.04us 3mb         |------------------------------------L0.171------------------------------------| "
    - "L0.174[42,355] 1.04us 3mb|-----------------------------------------L0.174-----------------------------------------|"
    - "L0.177[173,355] 1.04us 2mb                                     |----------------------L0.177----------------------| "
    - "L0.180[50,355] 1.04us 5mb  |---------------------------------------L0.180----------------------------------------| "
    - "L0.182[76,355] 1.04us 3mb         |------------------------------------L0.182------------------------------------| "
    - "L0.185[42,355] 1.05us 3mb|-----------------------------------------L0.185-----------------------------------------|"
    - "L0.188[173,355] 1.05us 2mb                                     |----------------------L0.188----------------------| "
    - "L0.191[50,355] 1.05us 5mb  |---------------------------------------L0.191----------------------------------------| "
    - "L0.193[76,355] 1.05us 3mb         |------------------------------------L0.193------------------------------------| "
    - "L0.196[42,355] 1.05us 3mb|-----------------------------------------L0.196-----------------------------------------|"
    - "**** 1 Output Files (parquet_file_id not yet assigned), 35mb total:"
    - "L0, all files 35mb                                                                                                 "
    - "L0.?[42,355] 1.05us      |------------------------------------------L0.?------------------------------------------|"
    - "Committing partition 1:"
    - "  Soft Deleting 10 files: L0.171, L0.174, L0.177, L0.180, L0.182, L0.185, L0.188, L0.191, L0.193, L0.196"
    - "  Creating 1 files"
    - "**** Simulation run 55, type=compact(ManySmallFiles). 10 Input Files, 38mb total:"
    - "L0                                                                                                                 "
    - "L0.62[356,668] 1us 4mb   |-----------------------------------------L0.62------------------------------------------|"
    - "L0.65[356,668] 1us 3mb   |-----------------------------------------L0.65------------------------------------------|"
    - "L0.68[356,668] 1us 4mb   |-----------------------------------------L0.68------------------------------------------|"
    - "L0.71[356,629] 1us 5mb   |-----------------------------------L0.71------------------------------------|            "
    - "L0.73[356,668] 1us 4mb   |-----------------------------------------L0.73------------------------------------------|"
    - "L0.76[356,668] 1us 3mb   |-----------------------------------------L0.76------------------------------------------|"
    - "L0.79[356,668] 1.01us 4mb|-----------------------------------------L0.79------------------------------------------|"
    - "L0.82[356,629] 1.01us 5mb|-----------------------------------L0.82------------------------------------|            "
    - "L0.84[356,668] 1.01us 4mb|-----------------------------------------L0.84------------------------------------------|"
    - "L0.87[356,668] 1.01us 3mb|-----------------------------------------L0.87------------------------------------------|"
    - "**** 1 Output Files (parquet_file_id not yet assigned), 38mb total:"
    - "L0, all files 38mb                                                                                                 "
    - "L0.?[356,668] 1.01us     |------------------------------------------L0.?------------------------------------------|"
    - "Committing partition 1:"
    - "  Soft Deleting 10 files: L0.62, L0.65, L0.68, L0.71, L0.73, L0.76, L0.79, L0.82, L0.84, L0.87"
    - "  Creating 1 files"
    - "**** Simulation run 56, type=compact(ManySmallFiles). 10 Input Files, 40mb total:"
    - "L0                                                                                                                 "
    - "L0.90[356,668] 1.01us 4mb|-----------------------------------------L0.90------------------------------------------|"
    - "L0.93[356,629] 1.01us 5mb|-----------------------------------L0.93------------------------------------|            "
    - "L0.95[356,668] 1.01us 4mb|-----------------------------------------L0.95------------------------------------------|"
    - "L0.98[356,668] 1.01us 3mb|-----------------------------------------L0.98------------------------------------------|"
    - "L0.101[356,668] 1.01us 4mb|-----------------------------------------L0.101-----------------------------------------|"
    - "L0.104[356,629] 1.01us 5mb|-----------------------------------L0.104-----------------------------------|            "
    - "L0.106[356,668] 1.02us 4mb|-----------------------------------------L0.106-----------------------------------------|"
    - "L0.109[356,668] 1.02us 3mb|-----------------------------------------L0.109-----------------------------------------|"
    - "L0.112[356,668] 1.02us 4mb|-----------------------------------------L0.112-----------------------------------------|"
    - "L0.115[356,629] 1.02us 5mb|-----------------------------------L0.115-----------------------------------|            "
    - "**** 1 Output Files (parquet_file_id not yet assigned), 40mb total:"
    - "L0, all files 40mb                                                                                                 "
    - "L0.?[356,668] 1.02us     |------------------------------------------L0.?------------------------------------------|"
    - "Committing partition 1:"
    - "  Soft Deleting 10 files: L0.90, L0.93, L0.95, L0.98, L0.101, L0.104, L0.106, L0.109, L0.112, L0.115"
    - "  Creating 1 files"
    - "**** Simulation run 57, type=compact(ManySmallFiles). 10 Input Files, 38mb total:"
    - "L0                                                                                                                 "
    - "L0.117[356,668] 1.02us 4mb|-----------------------------------------L0.117-----------------------------------------|"
    - "L0.120[356,668] 1.02us 3mb|-----------------------------------------L0.120-----------------------------------------|"
    - "L0.123[356,668] 1.02us 4mb|-----------------------------------------L0.123-----------------------------------------|"
    - "L0.126[356,629] 1.02us 5mb|-----------------------------------L0.126-----------------------------------|            "
    - "L0.128[356,668] 1.02us 4mb|-----------------------------------------L0.128-----------------------------------------|"
    - "L0.131[356,668] 1.02us 3mb|-----------------------------------------L0.131-----------------------------------------|"
    - "L0.134[356,668] 1.03us 4mb|-----------------------------------------L0.134-----------------------------------------|"
    - "L0.137[356,629] 1.03us 5mb|-----------------------------------L0.137-----------------------------------|            "
    - "L0.139[356,668] 1.03us 4mb|-----------------------------------------L0.139-----------------------------------------|"
    - "L0.142[356,668] 1.03us 3mb|-----------------------------------------L0.142-----------------------------------------|"
    - "**** 1 Output Files (parquet_file_id not yet assigned), 38mb total:"
    - "L0, all files 38mb                                                                                                 "
    - "L0.?[356,668] 1.03us     |------------------------------------------L0.?------------------------------------------|"
    - "Committing partition 1:"
    - "  Soft Deleting 10 files: L0.117, L0.120, L0.123, L0.126, L0.128, L0.131, L0.134, L0.137, L0.139, L0.142"
    - "  Creating 1 files"
    - "**** Simulation run 58, type=compact(ManySmallFiles). 10 Input Files, 40mb total:"
    - "L0                                                                                                                 "
    - "L0.145[356,668] 1.03us 4mb|-----------------------------------------L0.145-----------------------------------------|"
    - "L0.148[356,629] 1.03us 5mb|-----------------------------------L0.148-----------------------------------|            "
    - "L0.150[356,668] 1.03us 4mb|-----------------------------------------L0.150-----------------------------------------|"
    - "L0.153[356,668] 1.03us 3mb|-----------------------------------------L0.153-----------------------------------------|"
    - "L0.156[356,668] 1.03us 4mb|-----------------------------------------L0.156-----------------------------------------|"
    - "L0.159[356,629] 1.03us 5mb|-----------------------------------L0.159-----------------------------------|            "
    - "L0.161[356,668] 1.04us 4mb|-----------------------------------------L0.161-----------------------------------------|"
    - "L0.164[356,668] 1.04us 3mb|-----------------------------------------L0.164-----------------------------------------|"
    - "L0.167[356,668] 1.04us 4mb|-----------------------------------------L0.167-----------------------------------------|"
    - "L0.170[356,629] 1.04us 5mb|-----------------------------------L0.170-----------------------------------|            "
    - "**** 1 Output Files (parquet_file_id not yet assigned), 40mb total:"
    - "L0, all files 40mb                                                                                                 "
    - "L0.?[356,668] 1.04us     |------------------------------------------L0.?------------------------------------------|"
    - "Committing partition 1:"
    - "  Soft Deleting 10 files: L0.145, L0.148, L0.150, L0.153, L0.156, L0.159, L0.161, L0.164, L0.167, L0.170"
    - "  Creating 1 files"
    - "**** Simulation run 59, type=compact(ManySmallFiles). 10 Input Files, 38mb total:"
    - "L0                                                                                                                 "
    - "L0.172[356,668] 1.04us 4mb|-----------------------------------------L0.172-----------------------------------------|"
    - "L0.175[356,668] 1.04us 3mb|-----------------------------------------L0.175-----------------------------------------|"
    - "L0.178[356,668] 1.04us 4mb|-----------------------------------------L0.178-----------------------------------------|"
    - "L0.181[356,629] 1.04us 5mb|-----------------------------------L0.181-----------------------------------|            "
    - "L0.183[356,668] 1.04us 4mb|-----------------------------------------L0.183-----------------------------------------|"
    - "L0.186[356,668] 1.05us 3mb|-----------------------------------------L0.186-----------------------------------------|"
    - "L0.189[356,668] 1.05us 4mb|-----------------------------------------L0.189-----------------------------------------|"
    - "L0.192[356,629] 1.05us 5mb|-----------------------------------L0.192-----------------------------------|            "
    - "L0.194[356,668] 1.05us 4mb|-----------------------------------------L0.194-----------------------------------------|"
    - "L0.197[356,668] 1.05us 3mb|-----------------------------------------L0.197-----------------------------------------|"
    - "**** 1 Output Files (parquet_file_id not yet assigned), 38mb total:"
    - "L0, all files 38mb                                                                                                 "
    - "L0.?[356,668] 1.05us     |------------------------------------------L0.?------------------------------------------|"
    - "Committing partition 1:"
    - "  Soft Deleting 10 files: L0.172, L0.175, L0.178, L0.181, L0.183, L0.186, L0.189, L0.192, L0.194, L0.197"
    - "  Creating 1 files"
    - "**** Simulation run 60, type=compact(ManySmallFiles). 10 Input Files, 33mb total:"
    - "L0                                                                                                                 "
    - "L0.63[669,932] 1us 3mb   |---------------------------------L0.63----------------------------------|                "
    - "L0.66[669,986] 1us 3mb   |-----------------------------------------L0.66------------------------------------------|"
    - "L0.69[669,950] 1us 4mb   |------------------------------------L0.69------------------------------------|           "
    - "L0.74[669,932] 1us 3mb   |---------------------------------L0.74----------------------------------|                "
    - "L0.77[669,986] 1us 3mb   |-----------------------------------------L0.77------------------------------------------|"
    - "L0.80[669,950] 1.01us 4mb|------------------------------------L0.80------------------------------------|           "
    - "L0.85[669,932] 1.01us 3mb|---------------------------------L0.85----------------------------------|                "
    - "L0.88[669,986] 1.01us 3mb|-----------------------------------------L0.88------------------------------------------|"
    - "L0.91[669,950] 1.01us 4mb|------------------------------------L0.91------------------------------------|           "
    - "L0.96[669,932] 1.01us 3mb|---------------------------------L0.96----------------------------------|                "
    - "**** 1 Output Files (parquet_file_id not yet assigned), 33mb total:"
    - "L0, all files 33mb                                                                                                 "
    - "L0.?[669,986] 1.01us     |------------------------------------------L0.?------------------------------------------|"
    - "Committing partition 1:"
    - "  Soft Deleting 10 files: L0.63, L0.66, L0.69, L0.74, L0.77, L0.80, L0.85, L0.88, L0.91, L0.96"
    - "  Creating 1 files"
    - "**** Simulation run 61, type=compact(ManySmallFiles). 10 Input Files, 34mb total:"
    - "L0                                                                                                                 "
    - "L0.99[669,986] 1.01us 3mb|-----------------------------------------L0.99------------------------------------------|"
    - "L0.102[669,950] 1.01us 4mb|-----------------------------------L0.102------------------------------------|           "
    - "L0.107[669,932] 1.02us 3mb|---------------------------------L0.107---------------------------------|                "
    - "L0.110[669,986] 1.02us 3mb|-----------------------------------------L0.110-----------------------------------------|"
    - "L0.113[669,950] 1.02us 4mb|-----------------------------------L0.113------------------------------------|           "
    - "L0.118[669,932] 1.02us 3mb|---------------------------------L0.118---------------------------------|                "
    - "L0.121[669,986] 1.02us 3mb|-----------------------------------------L0.121-----------------------------------------|"
    - "L0.124[669,950] 1.02us 4mb|-----------------------------------L0.124------------------------------------|           "
    - "L0.129[669,932] 1.02us 3mb|---------------------------------L0.129---------------------------------|                "
    - "L0.132[669,986] 1.02us 3mb|-----------------------------------------L0.132-----------------------------------------|"
    - "**** 1 Output Files (parquet_file_id not yet assigned), 34mb total:"
    - "L0, all files 34mb                                                                                                 "
    - "L0.?[669,986] 1.02us     |------------------------------------------L0.?------------------------------------------|"
    - "Committing partition 1:"
    - "  Soft Deleting 10 files: L0.99, L0.102, L0.107, L0.110, L0.113, L0.118, L0.121, L0.124, L0.129, L0.132"
    - "  Creating 1 files"
    - "**** Simulation run 62, type=compact(ManySmallFiles). 10 Input Files, 34mb total:"
    - "L0                                                                                                                 "
    - "L0.135[669,950] 1.03us 4mb|-----------------------------------L0.135------------------------------------|           "
    - "L0.140[669,932] 1.03us 3mb|---------------------------------L0.140---------------------------------|                "
    - "L0.143[669,986] 1.03us 3mb|-----------------------------------------L0.143-----------------------------------------|"
    - "L0.146[669,950] 1.03us 4mb|-----------------------------------L0.146------------------------------------|           "
    - "L0.151[669,932] 1.03us 3mb|---------------------------------L0.151---------------------------------|                "
    - "L0.154[669,986] 1.03us 3mb|-----------------------------------------L0.154-----------------------------------------|"
    - "L0.157[669,950] 1.03us 4mb|-----------------------------------L0.157------------------------------------|           "
    - "L0.162[669,932] 1.04us 3mb|---------------------------------L0.162---------------------------------|                "
    - "L0.165[669,986] 1.04us 3mb|-----------------------------------------L0.165-----------------------------------------|"
    - "L0.168[669,950] 1.04us 4mb|-----------------------------------L0.168------------------------------------|           "
    - "**** 1 Output Files (parquet_file_id not yet assigned), 34mb total:"
    - "L0, all files 34mb                                                                                                 "
    - "L0.?[669,986] 1.04us     |------------------------------------------L0.?------------------------------------------|"
    - "Committing partition 1:"
    - "  Soft Deleting 10 files: L0.135, L0.140, L0.143, L0.146, L0.151, L0.154, L0.157, L0.162, L0.165, L0.168"
    - "  Creating 1 files"
    - "**** Simulation run 63, type=compact(ManySmallFiles). 8 Input Files, 27mb total:"
    - "L0                                                                                                                 "
    - "L0.173[669,932] 1.04us 3mb|---------------------------------L0.173---------------------------------|                "
    - "L0.176[669,986] 1.04us 3mb|-----------------------------------------L0.176-----------------------------------------|"
    - "L0.179[669,950] 1.04us 4mb|-----------------------------------L0.179------------------------------------|           "
    - "L0.184[669,932] 1.04us 3mb|---------------------------------L0.184---------------------------------|                "
    - "L0.187[669,986] 1.05us 3mb|-----------------------------------------L0.187-----------------------------------------|"
    - "L0.190[669,950] 1.05us 4mb|-----------------------------------L0.190------------------------------------|           "
    - "L0.195[669,932] 1.05us 3mb|---------------------------------L0.195---------------------------------|                "
    - "L0.198[669,986] 1.05us 3mb|-----------------------------------------L0.198-----------------------------------------|"
    - "**** 1 Output Files (parquet_file_id not yet assigned), 27mb total:"
    - "L0, all files 27mb                                                                                                 "
    - "L0.?[669,986] 1.05us     |------------------------------------------L0.?------------------------------------------|"
    - "Committing partition 1:"
    - "  Soft Deleting 8 files: L0.173, L0.176, L0.179, L0.184, L0.187, L0.190, L0.195, L0.198"
    - "  Creating 1 files"
    - "**** Simulation run 64, type=split(CompactAndSplitOutput(FoundSubsetLessThanMaxCompactSize))(split_times=[368, 694]). 8 Input Files, 290mb total:"
    - "L0                                                                                                                 "
    - "L0.199[42,355] 1.01us 35mb|----------L0.199-----------|                                                             "
    - "L0.204[356,668] 1.01us 38mb                             |----------L0.204-----------|                                "
    - "L0.209[669,986] 1.01us 33mb                                                           |-----------L0.209-----------| "
    - "L0.200[42,355] 1.02us 36mb|----------L0.200-----------|                                                             "
    - "L0.205[356,668] 1.02us 40mb                             |----------L0.205-----------|                                "
    - "L0.210[669,986] 1.02us 34mb                                                           |-----------L0.210-----------| "
    - "L0.201[42,355] 1.03us 35mb|----------L0.201-----------|                                                             "
    - "L0.206[356,668] 1.03us 38mb                             |----------L0.206-----------|                                "
    - "**** 3 Output Files (parquet_file_id not yet assigned), 290mb total:"
    - "L1                                                                                                                 "
    - "L1.?[42,368] 1.03us 100mb|------------L1.?-------------|                                                           "
    - "L1.?[369,694] 1.03us 100mb                               |------------L1.?------------|                             "
    - "L1.?[695,986] 1.03us 90mb                                                              |----------L1.?-----------| "
    - "Committing partition 1:"
    - "  Soft Deleting 8 files: L0.199, L0.200, L0.201, L0.204, L0.205, L0.206, L0.209, L0.210"
    - "  Creating 3 files"
    - "**** Simulation run 65, type=split(ReduceOverlap)(split_times=[694]). 1 Input Files, 34mb total:"
    - "L0, all files 34mb                                                                                                 "
    - "L0.211[669,986] 1.04us   |-----------------------------------------L0.211-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 34mb total:"
    - "L0                                                                                                                 "
    - "L0.?[669,694] 1.04us 3mb |L0.?-|                                                                                   "
    - "L0.?[695,986] 1.04us 31mb       |--------------------------------------L0.?--------------------------------------| "
    - "**** Simulation run 66, type=split(ReduceOverlap)(split_times=[368]). 1 Input Files, 40mb total:"
    - "L0, all files 40mb                                                                                                 "
    - "L0.207[356,668] 1.04us   |-----------------------------------------L0.207-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 40mb total:"
    - "L0                                                                                                                 "
    - "L0.?[356,368] 1.04us 2mb |L0.?|                                                                                    "
    - "L0.?[369,668] 1.04us 39mb   |----------------------------------------L0.?----------------------------------------| "
    - "**** Simulation run 67, type=split(ReduceOverlap)(split_times=[694]). 1 Input Files, 27mb total:"
    - "L0, all files 27mb                                                                                                 "
    - "L0.212[669,986] 1.05us   |-----------------------------------------L0.212-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 27mb total:"
    - "L0                                                                                                                 "
    - "L0.?[669,694] 1.05us 2mb |L0.?-|                                                                                   "
    - "L0.?[695,986] 1.05us 24mb       |--------------------------------------L0.?--------------------------------------| "
    - "**** Simulation run 68, type=split(ReduceOverlap)(split_times=[368]). 1 Input Files, 38mb total:"
    - "L0, all files 38mb                                                                                                 "
    - "L0.208[356,668] 1.05us   |-----------------------------------------L0.208-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 38mb total:"
    - "L0                                                                                                                 "
    - "L0.?[356,368] 1.05us 2mb |L0.?|                                                                                    "
    - "L0.?[369,668] 1.05us 37mb   |----------------------------------------L0.?----------------------------------------| "
    - "Committing partition 1:"
    - "  Soft Deleting 4 files: L0.207, L0.208, L0.211, L0.212"
    - "  Creating 8 files"
    - "**** Simulation run 69, type=split(CompactAndSplitOutput(TotalSizeLessThanMaxCompactSize))(split_times=[259, 476, 693, 910]). 10 Input Files, 437mb total:"
    - "L0                                                                                                                 "
    - "L0.216[669,694] 1.04us 3mb                                                           |L0.216|                       "
    - "L0.217[695,986] 1.04us 31mb                                                              |---------L0.217----------| "
    - "L0.202[42,355] 1.04us 36mb|----------L0.202-----------|                                                             "
    - "L0.218[356,368] 1.04us 2mb                             |L0.218|                                                     "
    - "L0.219[369,668] 1.04us 39mb                               |----------L0.219----------|                               "
    - "L0.203[42,355] 1.05us 35mb|----------L0.203-----------|                                                             "
    - "L0.222[356,368] 1.05us 2mb                             |L0.222|                                                     "
    - "L1                                                                                                                 "
    - "L1.214[369,694] 1.03us 100mb                               |-----------L1.214-----------|                             "
    - "L1.215[695,986] 1.03us 90mb                                                              |---------L1.215----------| "
    - "L1.213[42,368] 1.03us 100mb|-----------L1.213------------|                                                           "
    - "**** 5 Output Files (parquet_file_id not yet assigned), 437mb total:"
    - "L1                                                                                                                 "
    - "L1.?[42,259] 1.05us 101mb|-------L1.?-------|                                                                      "
    - "L1.?[260,476] 1.05us 100mb                    |-------L1.?-------|                                                  "
    - "L1.?[477,693] 1.05us 100mb                                         |-------L1.?-------|                             "
    - "L1.?[694,910] 1.05us 100mb                                                              |-------L1.?-------|        "
    - "L1.?[911,986] 1.05us 35mb                                                                                  |L1.?-| "
    - "Committing partition 1:"
    - "  Soft Deleting 10 files: L0.202, L0.203, L1.213, L1.214, L1.215, L0.216, L0.217, L0.218, L0.219, L0.222"
    - "  Creating 5 files"
    - "**** Simulation run 70, type=split(ReduceOverlap)(split_times=[910]). 1 Input Files, 24mb total:"
    - "L0, all files 24mb                                                                                                 "
    - "L0.221[695,986] 1.05us   |-----------------------------------------L0.221-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 24mb total:"
    - "L0                                                                                                                 "
    - "L0.?[695,910] 1.05us 18mb|------------------------------L0.?------------------------------|                        "
    - "L0.?[911,986] 1.05us 6mb                                                                   |--------L0.?---------| "
    - "**** Simulation run 71, type=split(ReduceOverlap)(split_times=[693]). 1 Input Files, 2mb total:"
    - "L0, all files 2mb                                                                                                  "
    - "L0.220[669,694] 1.05us   |-----------------------------------------L0.220-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 2mb total:"
    - "L0                                                                                                                 "
    - "L0.?[669,693] 1.05us 2mb |----------------------------------------L0.?----------------------------------------|    "
    - "L0.?[694,694] 1.05us 86kb                                                                                          |L0.?|"
    - "**** Simulation run 72, type=split(ReduceOverlap)(split_times=[476]). 1 Input Files, 37mb total:"
    - "L0, all files 37mb                                                                                                 "
    - "L0.223[369,668] 1.05us   |-----------------------------------------L0.223-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 37mb total:"
    - "L0                                                                                                                 "
    - "L0.?[369,476] 1.05us 13mb|-------------L0.?-------------|                                                          "
    - "L0.?[477,668] 1.05us 24mb                                |-------------------------L0.?--------------------------| "
    - "Committing partition 1:"
    - "  Soft Deleting 3 files: L0.220, L0.221, L0.223"
    - "  Creating 6 files"
    - "**** Simulation run 73, type=split(CompactAndSplitOutput(FoundSubsetLessThanMaxCompactSize))(split_times=[441, 622]). 5 Input Files, 239mb total:"
    - "L0                                                                                                                 "
    - "L0.233[369,476] 1.05us 13mb                      |-------L0.233-------|                                              "
    - "L0.234[477,668] 1.05us 24mb                                             |---------------L0.234----------------|      "
    - "L0.231[669,693] 1.05us 2mb                                                                                     |L0.231|"
    - "L1                                                                                                                 "
    - "L1.225[260,476] 1.05us 100mb|------------------L1.225------------------|                                              "
    - "L1.226[477,693] 1.05us 100mb                                             |------------------L1.226------------------| "
    - "**** 3 Output Files (parquet_file_id not yet assigned), 239mb total:"
    - "L1                                                                                                                 "
    - "L1.?[260,441] 1.05us 100mb|---------------L1.?----------------|                                                     "
    - "L1.?[442,622] 1.05us 100mb                                     |---------------L1.?----------------|                "
    - "L1.?[623,693] 1.05us 39mb                                                                           |----L1.?----| "
    - "Committing partition 1:"
    - "  Soft Deleting 5 files: L1.225, L1.226, L0.231, L0.233, L0.234"
    - "  Creating 3 files"
    - "**** Simulation run 74, type=split(CompactAndSplitOutput(TotalSizeLessThanMaxCompactSize))(split_times=[877]). 5 Input Files, 160mb total:"
    - "L0                                                                                                                 "
    - "L0.230[911,986] 1.05us 6mb                                                                  |-------L0.230--------| "
    - "L0.229[695,910] 1.05us 18mb|-----------------------------L0.229-----------------------------|                        "
    - "L0.232[694,694] 1.05us 86kb|L0.232|                                                                                  "
    - "L1                                                                                                                 "
    - "L1.228[911,986] 1.05us 35mb                                                                  |-------L1.228--------| "
    - "L1.227[694,910] 1.05us 100mb|-----------------------------L1.227-----------------------------|                        "
    - "**** 2 Output Files (parquet_file_id not yet assigned), 160mb total:"
    - "L1                                                                                                                 "
    - "L1.?[694,877] 1.05us 100mb|-------------------------L1.?-------------------------|                                  "
    - "L1.?[878,986] 1.05us 59mb                                                        |-------------L1.?--------------| "
    - "Committing partition 1:"
    - "  Soft Deleting 5 files: L1.227, L1.228, L0.229, L0.230, L0.232"
    - "  Creating 2 files"
    - "**** Simulation run 75, type=split(ReduceOverlap)(split_times=[899]). 1 Input Files, 59mb total:"
    - "L1, all files 59mb                                                                                                 "
    - "L1.239[878,986] 1.05us   |-----------------------------------------L1.239-----------------------------------------|"
    - "**** 2 Output Files (parquet_file_id not yet assigned), 59mb total:"
    - "L1                                                                                                                 "
    - "L1.?[878,899] 1.05us 12mb|-----L1.?------|                                                                         "
    - "L1.?[900,986] 1.05us 47mb                  |--------------------------------L1.?---------------------------------| "
    - "**** Simulation run 76, type=split(ReduceOverlap)(split_times=[699, 799]). 1 Input Files, 100mb total:"
    - "L1, all files 100mb                                                                                                "
    - "L1.238[694,877] 1.05us   |-----------------------------------------L1.238-----------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 100mb total:"
    - "L1                                                                                                                 "
    - "L1.?[694,699] 1.05us 3mb |L1.?|                                                                                    "
    - "L1.?[700,799] 1.05us 55mb  |---------------------L1.?---------------------|                                        "
    - "L1.?[800,877] 1.05us 43mb                                                    |---------------L1.?----------------| "
    - "**** Simulation run 77, type=split(ReduceOverlap)(split_times=[499, 599]). 1 Input Files, 100mb total:"
    - "L1, all files 100mb                                                                                                "
    - "L1.236[442,622] 1.05us   |-----------------------------------------L1.236-----------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 100mb total:"
    - "L1                                                                                                                 "
    - "L1.?[442,499] 1.05us 32mb|-----------L1.?-----------|                                                              "
    - "L1.?[500,599] 1.05us 55mb                             |---------------------L1.?----------------------|            "
    - "L1.?[600,622] 1.05us 13mb                                                                               |--L1.?---|"
    - "**** Simulation run 78, type=split(ReduceOverlap)(split_times=[299, 399]). 1 Input Files, 100mb total:"
    - "L1, all files 100mb                                                                                                "
    - "L1.235[260,441] 1.05us   |-----------------------------------------L1.235-----------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 100mb total:"
    - "L1                                                                                                                 "
    - "L1.?[260,299] 1.05us 22mb|------L1.?-------|                                                                       "
    - "L1.?[300,399] 1.05us 55mb                   |---------------------L1.?----------------------|                      "
    - "L1.?[400,441] 1.05us 23mb                                                                     |-------L1.?-------| "
    - "**** Simulation run 79, type=split(ReduceOverlap)(split_times=[99, 199]). 1 Input Files, 101mb total:"
    - "L1, all files 101mb                                                                                                "
    - "L1.224[42,259] 1.05us    |-----------------------------------------L1.224-----------------------------------------|"
    - "**** 3 Output Files (parquet_file_id not yet assigned), 101mb total:"
    - "L1                                                                                                                 "
    - "L1.?[42,99] 1.05us 27mb  |--------L1.?---------|                                                                   "
    - "L1.?[100,199] 1.05us 46mb                        |-----------------L1.?------------------|                         "
    - "L1.?[200,259] 1.05us 28mb                                                                 |---------L1.?---------| "
    - "Committing partition 1:"
    - "  Soft Deleting 5 files: L1.224, L1.235, L1.236, L1.238, L1.239"
    - "  Creating 14 files"
    - "**** Simulation run 80, type=split(CompactAndSplitOutput(FoundSubsetLessThanMaxCompactSize))(split_times=[73, 146]). 4 Input Files, 273mb total:"
    - "L1                                                                                                                 "
    - "L1.251[42,99] 1.05us 27mb                  |--------L1.251---------|                                               "
    - "L1.252[100,199] 1.05us 46mb                                             |------------------L1.252------------------| "
    - "L2                                                                                                                 "
    - "L2.1[0,99] 99ns 100mb    |-------------------L2.1-------------------|                                              "
    - "L2.2[100,199] 199ns 100mb                                             |-------------------L2.2-------------------| "
    - "**** 3 Output Files (parquet_file_id not yet assigned), 273mb total:"
    - "L2                                                                                                                 "
    - "L2.?[0,73] 1.05us 101mb  |-------------L2.?--------------|                                                         "
    - "L2.?[74,146] 1.05us 100mb                                 |-------------L2.?-------------|                         "
    - "L2.?[147,199] 1.05us 72mb                                                                  |--------L2.?---------| "
    - "Committing partition 1:"
    - "  Soft Deleting 4 files: L2.1, L2.2, L1.251, L1.252"
    - "  Creating 3 files"
    - "**** Simulation run 81, type=split(CompactAndSplitOutput(FoundSubsetLessThanMaxCompactSize))(split_times=[267]). 3 Input Files, 150mb total:"
    - "L1                                                                                                                 "
    - "L1.253[200,259] 1.05us 28mb|----------------------L1.253-----------------------|                                     "
    - "L1.248[260,299] 1.05us 22mb                                                      |-------------L1.248--------------| "
    - "L2                                                                                                                 "
    - "L2.3[200,299] 299ns 100mb|-----------------------------------------L2.3------------------------------------------| "
    - "**** 2 Output Files (parquet_file_id not yet assigned), 150mb total:"
    - "L2                                                                                                                 "
    - "L2.?[200,267] 1.05us 102mb|---------------------------L2.?---------------------------|                              "
    - "L2.?[268,299] 1.05us 48mb                                                             |-----------L2.?-----------| "
    - "Committing partition 1:"
    - "  Soft Deleting 3 files: L2.3, L1.248, L1.253"
    - "  Creating 2 files"
    - "**** Simulation run 82, type=split(CompactAndSplitOutput(FoundSubsetLessThanMaxCompactSize))(split_times=[364]). 2 Input Files, 155mb total:"
    - "L1                                                                                                                 "
    - "L1.249[300,399] 1.05us 55mb|----------------------------------------L1.249-----------------------------------------| "
    - "L2                                                                                                                 "
    - "L2.4[300,399] 399ns 100mb|-----------------------------------------L2.4------------------------------------------| "
    - "**** 2 Output Files (parquet_file_id not yet assigned), 155mb total:"
    - "L2                                                                                                                 "
    - "L2.?[300,364] 1.05us 101mb|--------------------------L2.?--------------------------|                                "
    - "L2.?[365,399] 1.05us 54mb                                                           |------------L2.?------------| "
    - "Committing partition 1:"
    - "  Soft Deleting 2 files: L2.4, L1.249"
    - "  Creating 2 files"
    - "**** Simulation run 83, type=split(CompactAndSplitOutput(FoundSubsetLessThanMaxCompactSize))(split_times=[464]). 3 Input Files, 155mb total:"
    - "L1                                                                                                                 "
    - "L1.250[400,441] 1.05us 23mb|--------------L1.250---------------|                                                     "
    - "L1.245[442,499] 1.05us 32mb                                      |---------------------L1.245----------------------| "
    - "L2                                                                                                                 "
    - "L2.5[400,499] 499ns 100mb|-----------------------------------------L2.5------------------------------------------| "
    - "**** 2 Output Files (parquet_file_id not yet assigned), 155mb total:"
    - "L2                                                                                                                 "
    - "L2.?[400,464] 1.05us 101mb|--------------------------L2.?--------------------------|                                "
    - "L2.?[465,499] 1.05us 54mb                                                           |------------L2.?------------| "
    - "Committing partition 1:"
    - "  Soft Deleting 3 files: L2.5, L1.245, L1.250"
    - "  Creating 2 files"
    - "**** Simulation run 84, type=split(CompactAndSplitOutput(FoundSubsetLessThanMaxCompactSize))(split_times=[564]). 2 Input Files, 155mb total:"
    - "L1                                                                                                                 "
    - "L1.246[500,599] 1.05us 55mb|----------------------------------------L1.246-----------------------------------------| "
    - "L2                                                                                                                 "
    - "L2.6[500,599] 599ns 100mb|-----------------------------------------L2.6------------------------------------------| "
    - "**** 2 Output Files (parquet_file_id not yet assigned), 155mb total:"
    - "L2                                                                                                                 "
    - "L2.?[500,564] 1.05us 101mb|--------------------------L2.?--------------------------|                                "
    - "L2.?[565,599] 1.05us 54mb                                                           |------------L2.?------------| "
    - "Committing partition 1:"
    - "  Soft Deleting 2 files: L2.6, L1.246"
    - "  Creating 2 files"
    - "**** Simulation run 85, type=split(CompactAndSplitOutput(FoundSubsetLessThanMaxCompactSize))(split_times=[664]). 4 Input Files, 155mb total:"
    - "L1                                                                                                                 "
    - "L1.247[600,622] 1.05us 13mb|------L1.247------|                                                                      "
    - "L1.237[623,693] 1.05us 39mb                    |---------------------------L1.237----------------------------|       "
    - "L1.242[694,699] 1.05us 3mb                                                                                     |L1.242|"
    - "L2                                                                                                                 "
    - "L2.7[600,699] 699ns 100mb|-----------------------------------------L2.7------------------------------------------| "
    - "**** 2 Output Files (parquet_file_id not yet assigned), 155mb total:"
    - "L2                                                                                                                 "
    - "L2.?[600,664] 1.05us 101mb|--------------------------L2.?--------------------------|                                "
    - "L2.?[665,699] 1.05us 54mb                                                           |------------L2.?------------| "
    - "Committing partition 1:"
    - "  Soft Deleting 4 files: L2.7, L1.237, L1.242, L1.247"
    - "  Creating 2 files"
    - "**** Simulation run 86, type=split(CompactAndSplitOutput(FoundSubsetLessThanMaxCompactSize))(split_times=[765]). 2 Input Files, 155mb total:"
    - "L1                                                                                                                 "
    - "L1.243[700,799] 1.05us 55mb|----------------------------------------L1.243-----------------------------------------| "
    - "L2                                                                                                                 "
    - "L2.8[700,799] 799ns 100mb|-----------------------------------------L2.8------------------------------------------| "
    - "**** 2 Output Files (parquet_file_id not yet assigned), 155mb total:"
    - "L2                                                                                                                 "
    - "L2.?[700,765] 1.05us 102mb|--------------------------L2.?---------------------------|                               "
    - "L2.?[766,799] 1.05us 53mb                                                           |-----------L2.?------------|  "
    - "Committing partition 1:"
    - "  Soft Deleting 2 files: L2.8, L1.243"
    - "  Creating 2 files"
    - "**** Simulation run 87, type=split(CompactAndSplitOutput(FoundSubsetLessThanMaxCompactSize))(split_times=[865]). 3 Input Files, 155mb total:"
    - "L1                                                                                                                 "
    - "L1.244[800,877] 1.05us 43mb|-------------------------------L1.244-------------------------------|                    "
    - "L1.240[878,899] 1.05us 12mb                                                                      |-----L1.240------| "
    - "L2                                                                                                                 "
    - "L2.9[800,899] 899ns 100mb|-----------------------------------------L2.9------------------------------------------| "
    - "**** 2 Output Files (parquet_file_id not yet assigned), 155mb total:"
    - "L2                                                                                                                 "
    - "L2.?[800,865] 1.05us 102mb|--------------------------L2.?---------------------------|                               "
    - "L2.?[866,899] 1.05us 53mb                                                           |-----------L2.?------------|  "
    - "Committing partition 1:"
    - "  Soft Deleting 3 files: L2.9, L1.240, L1.244"
    - "  Creating 2 files"
    - "**** Final Output Files (4.04gb written)"
    - "L1                                                                                                                 "
    - "L1.241[900,986] 1.05us 47mb                                                                                 |L1.241| "
    - "L2                                                                                                                 "
    - "L2.10[900,999] 999ns 100mb                                                                                 |L2.10-| "
    - "L2.254[0,73] 1.05us 101mb|L2.254|                                                                                  "
    - "L2.255[74,146] 1.05us 100mb      |L2.255|                                                                            "
    - "L2.256[147,199] 1.05us 72mb             |L2.256|                                                                     "
    - "L2.257[200,267] 1.05us 102mb                  |L2.257|                                                                "
    - "L2.258[268,299] 1.05us 48mb                        |L2.258|                                                          "
    - "L2.259[300,364] 1.05us 101mb                           |L2.259|                                                       "
    - "L2.260[365,399] 1.05us 54mb                                |L2.260|                                                  "
    - "L2.261[400,464] 1.05us 101mb                                    |L2.261|                                              "
    - "L2.262[465,499] 1.05us 54mb                                         |L2.262|                                         "
    - "L2.263[500,564] 1.05us 101mb                                             |L2.263|                                     "
    - "L2.264[565,599] 1.05us 54mb                                                  |L2.264|                                "
    - "L2.265[600,664] 1.05us 101mb                                                      |L2.265|                            "
    - "L2.266[665,699] 1.05us 54mb                                                           |L2.266|                       "
    - "L2.267[700,765] 1.05us 102mb                                                               |L2.267|                   "
    - "L2.268[766,799] 1.05us 53mb                                                                     |L2.268|             "
    - "L2.269[800,865] 1.05us 102mb                                                                        |L2.269|          "
    - "L2.270[866,899] 1.05us 53mb                                                                              |L2.270|    "
    - "**** Breakdown of where bytes were written"
    - 1.84gb written by split(CompactAndSplitOutput(FoundSubsetLessThanMaxCompactSize))
    - 500mb written by compact(ManySmallFiles)
    - 500mb written by split(VerticalSplit)
    - 596mb written by split(CompactAndSplitOutput(TotalSizeLessThanMaxCompactSize))
    - 663mb written by split(ReduceOverlap)
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
    - "**** Final Output Files (16.17gb written)"
    - "L2                                                                                                                 "
    - "L2.621[1614382,1717183] 3.42ms 162mb                                          |L2.621|                                        "
    - "L2.622[1717184,1820000] 3.42ms 202mb                                             |L2.622|                                     "
    - "L2.623[1830000,1970000] 3.42ms 267mb                                                |L2.623|                                  "
    - "L2.624[1980000,2070000] 3.42ms 157mb                                                    |L2.624|                              "
    - "L2.625[2080000,2229999] 3.42ms 197mb                                                       |L2.625|                           "
    - "L2.626[2230000,2299999] 3.42ms 201mb                                                           |L2.626|                       "
    - "L2.629[2450000,2533333] 3.42ms 146mb                                                                |L2.629|                  "
    - "L2.630[2533334,2616666] 3.42ms 115mb                                                                   |L2.630|               "
    - "L2.631[2616667,2700000] 3.42ms 167mb                                                                     |L2.631|             "
    - "L2.632[2710000,2759999] 3.42ms 117mb                                                                       |L2.632|           "
    - "L2.633[2760000,2809998] 3.42ms 109mb                                                                         |L2.633|         "
    - "L2.635[2870000,2931175] 3.42ms 124mb                                                                            |L2.635|      "
    - "L2.636[3053528,3079410] 3.42ms 143mb                                                                                 |L2.636| "
    - "L2.637[3079411,3105292] 3.42ms 115mb                                                                                 |L2.637| "
    - "L2.638[3105293,3131174] 3.42ms 157mb                                                                                  |L2.638|"
    - "L2.639[3131175,3157056] 3.42ms 138mb                                                                                   |L2.639|"
    - "L2.640[3157057,3182938] 3.42ms 167mb                                                                                   |L2.640|"
    - "L2.641[3182939,3208820] 3.42ms 186mb                                                                                    |L2.641|"
    - "L2.642[3208821,3234702] 3.42ms 194mb                                                                                     |L2.642|"
    - "L2.643[3234703,3260584] 3.42ms 178mb                                                                                     |L2.643|"
    - "L2.644[2931176,2992350] 3.42ms 184mb                                                                             |L2.644|     "
    - "L2.645[2992351,3053527] 3.42ms 268mb                                                                               |L2.645|   "
    - "L2.646[3260585,3286466] 3.42ms 148mb                                                                                      |L2.646|"
    - "L2.647[3286467,3312348] 3.42ms 152mb                                                                                       |L2.647|"
    - "L2.648[3312349,3338230] 3.42ms 224mb                                                                                       |L2.648|"
    - "L2.649[3338231,3364112] 3.42ms 189mb                                                                                        |L2.649|"
    - "L2.650[3364113,3390000] 3.42ms 110mb                                                                                         |L2.650|"
    - "L2.657[983237,1076038] 3.42ms 100mb                         |L2.657|                                                         "
    - "L2.658[1076039,1168839] 3.42ms 100mb                            |L2.658|                                                      "
    - "L2.663[1304127,1361580] 3.42ms 100mb                                  |L2.663|                                                "
    - "L2.669[1361581,1423756] 3.42ms 100mb                                   |L2.669|                                               "
    - "L2.672[2300000,2384100] 3.42ms 100mb                                                            |L2.672|                      "
    - "L2.674[10000,208052] 3.42ms 100mb|L2.674|                                                                                  "
    - "L2.675[208053,406104] 3.42ms 100mb     |L2.675|                                                                             "
    - "L2.676[406105,533587] 3.42ms 64mb          |L2.676|                                                                        "
    - "L2.677[533588,756033] 3.42ms 100mb             |L2.677|                                                                     "
    - "L2.678[756034,978478] 3.42ms 100mb                   |L2.678|                                                               "
    - "L2.679[978479,983236] 3.42ms 2mb                         |L2.679|                                                         "
    - "L2.680[1168840,1236225] 3.42ms 100mb                              |L2.680|                                                    "
    - "L2.681[1236226,1303610] 3.42ms 100mb                                |L2.681|                                                  "
    - "L2.682[1303611,1304126] 3.42ms 784kb                                  |L2.682|                                                "
    - "L2.683[1423757,1488831] 3.42ms 100mb                                     |L2.683|                                             "
    - "L2.684[1488832,1553905] 3.42ms 100mb                                       |L2.684|                                           "
    - "L2.685[1553906,1614381] 3.42ms 93mb                                         |L2.685|                                         "
    - "L2.686[2384101,2428820] 3.42ms 53mb                                                               |L2.686|                   "
    - "L2.687[2428821,2440000] 3.42ms 13mb                                                                |L2.687|                  "
    - "L2.690[2809999,2849999] 3.42ms 65mb                                                                          |L2.690|        "
    - "L2.691[2850000,2860000] 3.42ms 16mb                                                                           |L2.691|       "
    - "**** Breakdown of where bytes were written"
    - 229mb written by split(CompactAndSplitOutput(TotalSizeLessThanMaxCompactSize))
    - 4.84gb written by split(VerticalSplit)
    - 4.94gb written by compact(ManySmallFiles)
    - 5.23gb written by split(CompactAndSplitOutput(FoundSubsetLessThanMaxCompactSize))
    - 955mb written by split(ReduceOverlap)
    - "WARNING: file L2.621[1614382,1717183] 3.42ms 162mb exceeds soft limit 100mb by more than 50%"
    - "WARNING: file L2.622[1717184,1820000] 3.42ms 202mb exceeds soft limit 100mb by more than 50%"
    - "WARNING: file L2.623[1830000,1970000] 3.42ms 267mb exceeds soft limit 100mb by more than 50%"
    - "WARNING: file L2.624[1980000,2070000] 3.42ms 157mb exceeds soft limit 100mb by more than 50%"
    - "WARNING: file L2.625[2080000,2229999] 3.42ms 197mb exceeds soft limit 100mb by more than 50%"
    - "WARNING: file L2.626[2230000,2299999] 3.42ms 201mb exceeds soft limit 100mb by more than 50%"
    - "WARNING: file L2.631[2616667,2700000] 3.42ms 167mb exceeds soft limit 100mb by more than 50%"
    - "WARNING: file L2.638[3105293,3131174] 3.42ms 157mb exceeds soft limit 100mb by more than 50%"
    - "WARNING: file L2.640[3157057,3182938] 3.42ms 167mb exceeds soft limit 100mb by more than 50%"
    - "WARNING: file L2.641[3182939,3208820] 3.42ms 186mb exceeds soft limit 100mb by more than 50%"
    - "WARNING: file L2.642[3208821,3234702] 3.42ms 194mb exceeds soft limit 100mb by more than 50%"
    - "WARNING: file L2.643[3234703,3260584] 3.42ms 178mb exceeds soft limit 100mb by more than 50%"
    - "WARNING: file L2.644[2931176,2992350] 3.42ms 184mb exceeds soft limit 100mb by more than 50%"
    - "WARNING: file L2.645[2992351,3053527] 3.42ms 268mb exceeds soft limit 100mb by more than 50%"
    - "WARNING: file L2.647[3286467,3312348] 3.42ms 152mb exceeds soft limit 100mb by more than 50%"
    - "WARNING: file L2.648[3312349,3338230] 3.42ms 224mb exceeds soft limit 100mb by more than 50%"
    - "WARNING: file L2.649[3338231,3364112] 3.42ms 189mb exceeds soft limit 100mb by more than 50%"
    "###
    );
}
