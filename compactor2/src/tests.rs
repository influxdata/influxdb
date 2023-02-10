use std::time::Duration;

use arrow_util::assert_batches_sorted_eq;
use data_types::{CompactionLevel, ParquetFile, PartitionId};
use iox_tests::TestParquetFileBuilder;

use crate::{
    config::AlgoVersion,
    test_util::{format_files, list_object_store, TestSetup},
};

#[tokio::test]
async fn test_compact_no_file() {
    test_helpers::maybe_start_logging();

    // no files
    let setup = TestSetup::builder().await.build().await;

    let files = setup.list_by_table_not_to_delete().await;
    assert!(files.is_empty());

    // compact
    setup.run_compact().await;

    // verify catalog is still empty
    let files = setup.list_by_table_not_to_delete().await;
    assert!(files.is_empty());
}

#[tokio::test]
async fn test_num_files_over_limit() {
    test_helpers::maybe_start_logging();

    for version in [AlgoVersion::AllAtOnce, AlgoVersion::TargetLevel] {
        // Create a test setup with 6 files
        let setup = TestSetup::builder()
            .await
            .with_files()
            .await
            .with_compact_version(version)
            // Set max num file to 4 (< num files) --> it won't get comapcted
            .with_max_input_files_per_partition(4)
            .build()
            .await;

        // verify 6 files
        let files = setup.list_by_table_not_to_delete().await;
        assert_eq!(files.len(), 6);
        // verify ID and compaction level of the files
        assert_levels(
            &files,
            vec![
                (1, CompactionLevel::FileNonOverlapped),
                (2, CompactionLevel::Initial),
                (3, CompactionLevel::Initial),
                (4, CompactionLevel::FileNonOverlapped),
                (5, CompactionLevel::Initial),
                (6, CompactionLevel::Initial),
            ],
        );

        setup.run_compact().await;
        //
        // read files and verify they are not compacted
        let files = setup.list_by_table_not_to_delete().await;
        assert_eq!(files.len(), 6);
        //
        // verify ID and compaction level of the files
        assert_levels(
            &files,
            vec![
                (1, CompactionLevel::FileNonOverlapped),
                (2, CompactionLevel::Initial),
                (3, CompactionLevel::Initial),
                (4, CompactionLevel::FileNonOverlapped),
                (5, CompactionLevel::Initial),
                (6, CompactionLevel::Initial),
            ],
        );
    }
}

#[tokio::test]
async fn test_total_file_size_over_limit() {
    test_helpers::maybe_start_logging();

    for version in [AlgoVersion::AllAtOnce, AlgoVersion::TargetLevel] {
        // Create a test setup with 6 files
        let setup = TestSetup::builder()
            .await
            .with_files()
            .await
            // Set max size < the input file size  --> it won't get compacted
            .with_max_input_parquet_bytes_per_partition_relative_to_total_size(-1)
            .with_compact_version(version)
            .build()
            .await;

        // verify 6 files
        let files = setup.list_by_table_not_to_delete().await;
        assert_eq!(files.len(), 6);

        // verify ID and compaction level of the files
        assert_levels(
            &files,
            vec![
                (1, CompactionLevel::FileNonOverlapped),
                (2, CompactionLevel::Initial),
                (3, CompactionLevel::Initial),
                (4, CompactionLevel::FileNonOverlapped),
                (5, CompactionLevel::Initial),
                (6, CompactionLevel::Initial),
            ],
        );

        setup.run_compact().await;

        // read files and verify they are not compacted
        let files = setup.list_by_table_not_to_delete().await;
        assert_eq!(files.len(), 6);

        // verify ID and compaction level of the files
        assert_levels(
            &files,
            vec![
                (1, CompactionLevel::FileNonOverlapped),
                (2, CompactionLevel::Initial),
                (3, CompactionLevel::Initial),
                (4, CompactionLevel::FileNonOverlapped),
                (5, CompactionLevel::Initial),
                (6, CompactionLevel::Initial),
            ],
        );
    }
}

#[tokio::test]
async fn test_compact_all_at_once() {
    test_helpers::maybe_start_logging();

    // Create a test setup with 6 files
    let setup = TestSetup::builder()
        .await
        .with_files()
        .await
        // Ensure we have enough resource to compact the files
        .with_max_input_files_per_partition_relative_to_n_files(10)
        .with_max_input_parquet_bytes_per_partition_relative_to_total_size(1000)
        .with_compact_version(AlgoVersion::AllAtOnce)
        .build()
        .await;

    // verify 6 files
    // verify ID and compaction level of the files
    let files = setup.list_by_table_not_to_delete().await;
    assert_levels(
        &files,
        vec![
            (1, CompactionLevel::FileNonOverlapped),
            (2, CompactionLevel::Initial),
            (3, CompactionLevel::Initial),
            (4, CompactionLevel::FileNonOverlapped),
            (5, CompactionLevel::Initial),
            (6, CompactionLevel::Initial),
        ],
    );

    // verify ID and max_l0_created_at
    let times = setup.test_times();
    assert_max_l0_created_at(
        &files,
        vec![
            (1, times.time_1_minute_future),
            (2, times.time_2_minutes_future),
            (3, times.time_5_minutes_future),
            (4, times.time_3_minutes_future),
            (5, times.time_5_minutes_future),
            (6, times.time_2_minutes_future),
        ],
    );

    // compact
    setup.run_compact().await;

    // verify number of files: 6 files are compacted into 2 files
    let files = setup.list_by_table_not_to_delete().await;
    assert_levels(
        &files,
        vec![
            (7, CompactionLevel::FileNonOverlapped),
            (8, CompactionLevel::FileNonOverlapped),
        ],
    );
    assert_max_l0_created_at(
        &files,
        // both files have max_l0_created time_5_minutes_future
        // which is the max of all L0 input's max_l0_created_at
        vec![
            (7, times.time_5_minutes_future),
            (8, times.time_5_minutes_future),
        ],
    );

    // verify the content of files
    // Compacted smaller file with the later data
    let mut files = setup.list_by_table_not_to_delete().await;
    let file1 = files.pop().unwrap();
    let batches = setup.read_parquet_file(file1).await;
    assert_batches_sorted_eq!(
        &[
            "+-----------+------+------+------+-----------------------------+",
            "| field_int | tag1 | tag2 | tag3 | time                        |",
            "+-----------+------+------+------+-----------------------------+",
            "| 210       |      | OH   | 21   | 1970-01-01T00:00:00.000136Z |",
            "+-----------+------+------+------+-----------------------------+",
        ],
        &batches
    );

    // Compacted larger file with the earlier data
    let file0 = files.pop().unwrap();
    let batches = setup.read_parquet_file(file0).await;
    assert_batches_sorted_eq!(
        [
            "+-----------+------+------+------+-----------------------------+",
            "| field_int | tag1 | tag2 | tag3 | time                        |",
            "+-----------+------+------+------+-----------------------------+",
            "| 10        | VT   |      |      | 1970-01-01T00:00:00.000006Z |",
            "| 10        | VT   |      |      | 1970-01-01T00:00:00.000010Z |",
            "| 10        | VT   |      |      | 1970-01-01T00:00:00.000068Z |",
            "| 1500      | WA   |      |      | 1970-01-01T00:00:00.000008Z |",
            "| 1601      |      | PA   | 15   | 1970-01-01T00:00:00.000030Z |",
            "| 22        |      | OH   | 21   | 1970-01-01T00:00:00.000036Z |",
            "| 270       | UT   |      |      | 1970-01-01T00:00:00.000025Z |",
            "| 70        | UT   |      |      | 1970-01-01T00:00:00.000020Z |",
            "| 99        | OR   |      |      | 1970-01-01T00:00:00.000012Z |",
            "+-----------+------+------+------+-----------------------------+",
        ],
        &batches
    );
}

#[tokio::test]
async fn test_compact_target_level() {
    test_helpers::maybe_start_logging();

    // Create a test setup with 6 files
    let setup = TestSetup::builder()
        .await
        .with_files()
        .await
        // Ensure we have enough resource to compact the files
        .with_max_input_files_per_partition_relative_to_n_files(10)
        .with_max_input_parquet_bytes_per_partition_relative_to_total_size(1000)
        .with_compact_version(AlgoVersion::TargetLevel)
        .with_min_num_l1_files_to_compact(2)
        .build()
        .await;

    // verify 6 files
    let files = setup.list_by_table_not_to_delete().await;
    assert_levels(
        &files,
        vec![
            (1, CompactionLevel::FileNonOverlapped),
            (2, CompactionLevel::Initial),
            (3, CompactionLevel::Initial),
            (4, CompactionLevel::FileNonOverlapped),
            (5, CompactionLevel::Initial),
            (6, CompactionLevel::Initial),
        ],
    );

    // verify ID and max_l0_created_at
    let times = setup.test_times();
    assert_max_l0_created_at(
        &files,
        vec![
            (1, times.time_1_minute_future),
            (2, times.time_2_minutes_future),
            (3, times.time_5_minutes_future),
            (4, times.time_3_minutes_future),
            (5, times.time_5_minutes_future),
            (6, times.time_2_minutes_future),
        ],
    );

    // compact
    setup.run_compact().await;

    // verify number of files: 6 files are compacted into 2 files
    let files = setup.list_by_table_not_to_delete().await;
    assert_eq!(files.len(), 2);

    assert_levels(
        &files,
        // This is the result of 2-round compaction fomr L0s -> L1s and then L1s -> L2s
        // The first round will create two L1 files IDs 7 and 8
        // The second round will create tow L2 file IDs 9 and 10
        vec![(9, CompactionLevel::Final), (10, CompactionLevel::Final)],
    );

    assert_max_l0_created_at(
        &files,
        // both files have max_l0_created time_5_minutes_future
        // which is the max of all L0 input's max_l0_created_at
        vec![
            (9, times.time_5_minutes_future),
            (10, times.time_5_minutes_future),
        ],
    );

    // verify the content of files
    // Compacted smaller file with the later data
    let mut files = setup.list_by_table_not_to_delete().await;
    let file1 = files.pop().unwrap();
    let batches = setup.read_parquet_file(file1).await;
    assert_batches_sorted_eq!(
        &[
            "+-----------+------+------+------+-----------------------------+",
            "| field_int | tag1 | tag2 | tag3 | time                        |",
            "+-----------+------+------+------+-----------------------------+",
            "| 210       |      | OH   | 21   | 1970-01-01T00:00:00.000136Z |",
            "+-----------+------+------+------+-----------------------------+",
        ],
        &batches
    );

    // Compacted larger file with the earlier data
    let file0 = files.pop().unwrap();
    let batches = setup.read_parquet_file(file0).await;
    assert_batches_sorted_eq!(
        [
            "+-----------+------+------+------+-----------------------------+",
            "| field_int | tag1 | tag2 | tag3 | time                        |",
            "+-----------+------+------+------+-----------------------------+",
            "| 10        | VT   |      |      | 1970-01-01T00:00:00.000006Z |",
            "| 10        | VT   |      |      | 1970-01-01T00:00:00.000010Z |",
            "| 10        | VT   |      |      | 1970-01-01T00:00:00.000068Z |",
            "| 1500      | WA   |      |      | 1970-01-01T00:00:00.000008Z |",
            "| 1601      |      | PA   | 15   | 1970-01-01T00:00:00.000030Z |",
            "| 22        |      | OH   | 21   | 1970-01-01T00:00:00.000036Z |",
            "| 270       | UT   |      |      | 1970-01-01T00:00:00.000025Z |",
            "| 70        | UT   |      |      | 1970-01-01T00:00:00.000020Z |",
            "| 99        | OR   |      |      | 1970-01-01T00:00:00.000012Z |",
            "+-----------+------+------+------+-----------------------------+",
        ],
        &batches
    );
}

#[tokio::test]
async fn test_skip_compact() {
    test_helpers::maybe_start_logging();

    // Create a test setup with 6 files
    let setup = TestSetup::builder().await.with_files().await.build().await;

    let expected_files_and_levels = vec![
        (1, CompactionLevel::FileNonOverlapped),
        (2, CompactionLevel::Initial),
        (3, CompactionLevel::Initial),
        (4, CompactionLevel::FileNonOverlapped),
        (5, CompactionLevel::Initial),
        (6, CompactionLevel::Initial),
    ];

    // verify 6 files
    let files = setup.list_by_table_not_to_delete().await;
    assert_levels(&files, expected_files_and_levels.clone());

    // add the partition into skipped compaction
    setup
        .catalog
        .add_to_skipped_compaction(setup.partition_info.partition_id, "test reason")
        .await;

    // compact but nothing will be compacted because the partition is skipped
    setup.run_compact().await;

    // verify still 6 files
    let files = setup.list_by_table_not_to_delete().await;
    assert_levels(&files, expected_files_and_levels.clone());
}

#[tokio::test]
async fn test_partition_fail() {
    test_helpers::maybe_start_logging();

    // Create a test setup with 6 files
    let setup = TestSetup::builder().await.with_files().await.build().await;

    let catalog_files_pre = setup.list_by_table_not_to_delete().await;
    assert!(!catalog_files_pre.is_empty());

    let object_store_files_pre = list_object_store(&setup.catalog.object_store).await;
    assert!(!object_store_files_pre.is_empty());

    setup.run_compact_failing().await;

    let catalog_files_post = setup.list_by_table_not_to_delete().await;
    assert_eq!(catalog_files_pre, catalog_files_post);

    let object_store_files_post = list_object_store(&setup.catalog.object_store).await;
    assert_eq!(object_store_files_pre, object_store_files_post);

    assert_skipped_compactions(
        &setup,
        [(
            setup.partition_info.partition_id,
            "serialize\ncaused by\nJoin Error (panic)\ncaused by\nExternal error: foo",
        )],
    )
    .await;
}

#[tokio::test]
async fn test_shadow_mode() {
    test_helpers::maybe_start_logging();

    // Create a test setup with 6 files
    let setup = TestSetup::builder()
        .await
        .with_files()
        .await
        .with_shadow_mode()
        .build()
        .await;

    let catalog_files_pre = setup.list_by_table_not_to_delete().await;
    assert!(!catalog_files_pre.is_empty());

    let object_store_files_pre = list_object_store(&setup.catalog.object_store).await;
    assert!(!object_store_files_pre.is_empty());

    setup.run_compact().await;

    let catalog_files_post = setup.list_by_table_not_to_delete().await;
    assert_eq!(catalog_files_pre, catalog_files_post);

    let object_store_files_post = list_object_store(&setup.catalog.object_store).await;
    assert_eq!(object_store_files_pre, object_store_files_post);
}

#[tokio::test]
async fn test_shadow_mode_partition_fail() {
    test_helpers::maybe_start_logging();

    // Create a test setup with 6 files
    let setup = TestSetup::builder()
        .await
        .with_files()
        .await
        .with_shadow_mode()
        .build()
        .await;

    let catalog_files_pre = setup.list_by_table_not_to_delete().await;
    assert!(!catalog_files_pre.is_empty());

    let object_store_files_pre = list_object_store(&setup.catalog.object_store).await;
    assert!(!object_store_files_pre.is_empty());

    setup.run_compact_failing().await;

    let catalog_files_post = setup.list_by_table_not_to_delete().await;
    assert_eq!(catalog_files_pre, catalog_files_post);

    let object_store_files_post = list_object_store(&setup.catalog.object_store).await;
    assert_eq!(object_store_files_pre, object_store_files_post);

    assert_skipped_compactions(&setup, []).await;
}

#[track_caller]
fn assert_levels<'a>(
    files: impl IntoIterator<Item = &'a ParquetFile>,
    expected_files_and_levels: impl IntoIterator<Item = (i64, CompactionLevel)>,
) {
    let files_and_levels: Vec<_> = files
        .into_iter()
        .map(|f| (f.id.get(), f.compaction_level))
        .collect();

    let expected_files_and_levels: Vec<_> = expected_files_and_levels.into_iter().collect();

    assert_eq!(files_and_levels, expected_files_and_levels);
}

#[track_caller]
/// Asserts each parquet file has (id, max_l0_created_at)
fn assert_max_l0_created_at<'a>(
    files: impl IntoIterator<Item = &'a ParquetFile>,
    expected_files_and_max_l0_created_ats: impl IntoIterator<Item = (i64, i64)>,
) {
    let files_and_max_l0_created_ats: Vec<_> = files
        .into_iter()
        .map(|f| (f.id.get(), f.max_l0_created_at.get()))
        .collect();

    let expected_files_and_max_l0_created_ats: Vec<_> =
        expected_files_and_max_l0_created_ats.into_iter().collect();

    assert_eq!(
        files_and_max_l0_created_ats,
        expected_files_and_max_l0_created_ats
    );
}

async fn assert_skipped_compactions<const N: usize>(
    setup: &TestSetup,
    expected: [(PartitionId, &'static str); N],
) {
    let skipped = setup
        .catalog
        .catalog
        .repositories()
        .await
        .partitions()
        .list_skipped_compactions()
        .await
        .unwrap();

    let actual = skipped
        .iter()
        .map(|skipped| (skipped.partition_id, skipped.reason.as_str()))
        .collect::<Vec<_>>();

    assert_eq!(actual, expected);
}

// ----------------------------
// ----- Begin Layout Tests ----
// (TODO move these to a separate module)
// ----------------------------

/// creates a TestParquetFileBuilder setup for layout tests
fn parquet_builder() -> TestParquetFileBuilder {
    TestParquetFileBuilder::default()
        .with_compaction_level(CompactionLevel::Initial)
        // need some LP to generate the schema
        .with_line_protocol("table,tag1=A,tag2=B,tag3=C field_int=1i 100")
}

/// runs the scenario and returns a string based output for comparison
async fn run_layout_scenario(setup: &TestSetup, input_files: Vec<ParquetFile>) -> Vec<String> {
    setup.catalog.time_provider.inc(Duration::from_nanos(200));

    // record the input files
    let mut output = format_files("**** Input Files", &input_files);

    // run the actual compaction
    let compact_result = setup.run_compact().await;
    assert_skipped_compactions(setup, []).await;

    // record what the compactor actually did
    output.extend(compact_result.simulator_runs);

    // record the output files
    let output_files = setup.list_by_table_not_to_delete().await;
    output.extend(format_files("**** Output Files", &output_files));

    output
}

#[tokio::test]
async fn layout_all_overlapping() {
    test_helpers::maybe_start_logging();
    let one_mb = 1024 * 1024;

    let setup = TestSetup::builder()
        .await
        .simulate_without_object_store()
        .with_max_desired_file_size_bytes(20 * one_mb)
        .build()
        .await;

    // create virtual files
    let mut input_files = vec![];
    for _ in 0..10 {
        let file = setup
            .partition
            .create_parquet_file(
                parquet_builder()
                    .with_min_time(100)
                    .with_max_time(200)
                    .with_file_size_bytes(one_mb),
            )
            .await
            .parquet_file;
        input_files.push(file);
    }

    insta::assert_yaml_snapshot!(
        run_layout_scenario(&setup, input_files).await,
        @r###"
    ---
    - "**** Input Files"
    - "L0, all files 1mb                                                                                   "
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
    - "**** Simulation Run 0, type=split(split_times=[180])"
    - "Input, 10 files: 1, 2, 3, 4, 5, 6, 7, 8, 9, 10"
    - "**** Output Files"
    - "L1                                                                                                  "
    - "L1.11[100,180] 8mb  |----------------------------L1.11-----------------------------|                "
    - "L1.12[180,200] 2mb                                                                  |----L1.12-----|"
    "###
    );
}

#[tokio::test]
async fn layout_l1_with_new_non_overlapping_l0() {
    test_helpers::maybe_start_logging();
    let one_hundred_mb = 100 * 1024 * 1024;
    let five_kb = 5 * 1024;

    let setup = TestSetup::builder()
        .await
        .simulate_without_object_store()
        .with_max_desired_file_size_bytes(one_hundred_mb)
        .build()
        .await;

    // Model several non overlapping L1 file and new L0 files written
    // that are not overlapping
    //
    // L1: 100MB, 100MB, 100MB, 100MB
    // L0: 5k, 5k, 5k, 5k, 5k (all non overlapping with the L1 files)
    let mut input_files = vec![];
    for i in 0..4 {
        let file = setup
            .partition
            .create_parquet_file(
                parquet_builder()
                    .with_min_time(50 + i * 50)
                    .with_max_time(100 + i * 50)
                    .with_compaction_level(CompactionLevel::FileNonOverlapped)
                    .with_file_size_bytes(one_hundred_mb),
            )
            .await
            .parquet_file;
        input_files.push(file);
    }
    for i in 0..5 {
        let file = setup
            .partition
            .create_parquet_file(
                parquet_builder()
                    .with_min_time(300 + i * 50)
                    .with_max_time(350 + i * 50)
                    .with_file_size_bytes(five_kb),
            )
            .await
            .parquet_file;
        input_files.push(file);
    }

    setup.catalog.time_provider.inc(Duration::from_nanos(200));

    insta::assert_yaml_snapshot!(
        run_layout_scenario(&setup, input_files).await,
        @r###"
    ---
    - "**** Input Files"
    - "L0                                                                                                  "
    - "L0.5[300,350] 5kb                                           |-L0.5-|                                "
    - "L0.6[350,400] 5kb                                                   |-L0.6-|                        "
    - "L0.7[400,450] 5kb                                                           |-L0.7-|                "
    - "L0.8[450,500] 5kb                                                                   |-L0.8-|        "
    - "L0.9[500,550] 5kb                                                                           |-L0.9-|"
    - "L1                                                                                                  "
    - "L1.1[50,100] 100mb  |-L1.1-|                                                                        "
    - "L1.2[100,150] 100mb         |-L1.2-|                                                                "
    - "L1.3[150,200] 100mb                 |-L1.3-|                                                        "
    - "L1.4[200,250] 100mb                         |-L1.4-|                                                "
    - "**** Simulation Run 0, type=split(split_times=[175, 300, 425])"
    - "Input, 9 files: 1, 2, 3, 4, 5, 6, 7, 8, 9"
    - "**** Output Files"
    - "L1, all files 100.01mb                                                                              "
    - "L1.10[50,175]       |------L1.10-------|                                                            "
    - "L1.11[175,300]                          |------L1.11-------|                                        "
    - "L1.12[300,425]                                              |------L1.12-------|                    "
    - "L1.13[425,550]                                                                  |------L1.13-------|"
    "###
    );
}

#[tokio::test]
async fn layout_l1_with_new_non_overlapping_l0_larger() {
    test_helpers::maybe_start_logging();
    let one_mb = 1024 * 1024;

    let setup = TestSetup::builder()
        .await
        .simulate_without_object_store()
        .with_max_desired_file_size_bytes(100 * one_mb)
        .build()
        .await;

    // Model several non overlapping L1 file and new L0 files written
    // that are also not overlapping
    //
    // L1: 20MB, 50MB, 20MB, 3MB
    // L0: 5MB, 5MB, 5MB
    let mut input_files = vec![];
    for (i, sz) in [20, 50, 20, 3].iter().enumerate() {
        let i = i as i64;
        let file = setup
            .partition
            .create_parquet_file(
                parquet_builder()
                    .with_min_time(50 + i * 50)
                    .with_max_time(100 + i * 50)
                    .with_compaction_level(CompactionLevel::FileNonOverlapped)
                    .with_file_size_bytes(sz * one_mb),
            )
            .await
            .parquet_file;
        input_files.push(file);
    }
    for i in 0..3 {
        let file = setup
            .partition
            .create_parquet_file(
                parquet_builder()
                    .with_min_time(300 + i * 50)
                    .with_max_time(350 + i * 50)
                    .with_file_size_bytes(5 * one_mb),
            )
            .await
            .parquet_file;
        input_files.push(file);
    }

    setup.catalog.time_provider.inc(Duration::from_nanos(200));

    insta::assert_yaml_snapshot!(
        run_layout_scenario(&setup, input_files).await,
        @r###"
    ---
    - "**** Input Files"
    - "L0                                                                                                  "
    - "L0.5[300,350] 5mb                                                     |--L0.5--|                    "
    - "L0.6[350,400] 5mb                                                               |--L0.6--|          "
    - "L0.7[400,450] 5mb                                                                         |--L0.7--|"
    - "L1                                                                                                  "
    - "L1.1[50,100] 20mb   |--L1.1--|                                                                      "
    - "L1.2[100,150] 50mb            |--L1.2--|                                                            "
    - "L1.3[150,200] 20mb                      |--L1.3--|                                                  "
    - "L1.4[200,250] 3mb                                 |--L1.4--|                                        "
    - "**** Simulation Run 0, type=split(split_times=[421])"
    - "Input, 7 files: 1, 2, 3, 4, 5, 6, 7"
    - "**** Output Files"
    - "L1                                                                                                  "
    - "L1.8[50,421] 100.17mb|----------------------------------L1.8----------------------------------|      "
    - "L1.9[421,450] 7.83mb                                                                          |L1.9|"
    "###
    );
}
