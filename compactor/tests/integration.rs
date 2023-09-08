use arrow_util::assert_batches_sorted_eq;
use compactor_test_utils::{format_files, list_object_store, TestSetup};
use data_types::{CompactionLevel, ParquetFile, PartitionId};

mod layouts;

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

    // Create a test setup with 6 files
    let setup = TestSetup::builder()
        .await
        .with_files()
        .await
        // Set max num file to 4 (< num files) --> many L0s files, comppact 4 L0s into 2 L0s
        .with_max_num_files_per_plan(4)
        // Not compact L1s into L2s because tnumber of L1s < 5
        .with_min_num_l1_files_to_compact(5)
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
    // read files and verify 3 files
    let files = setup.list_by_table_not_to_delete().await;
    assert_eq!(files.len(), 3);

    //
    // verify ID and compaction level of the files
    // Original IDs of files: 1, 2, 3, 4, 5, 6
    // 4 L0s files are compacted into 2 new L0s files with IDs 7, 8
    // Then these 2 new L0s files are compacted with overlapped L1 files into 2 new L1s files with IDs 9, 10
    assert_levels(
        &files,
        vec![
            (7, CompactionLevel::FileNonOverlapped),
            (8, CompactionLevel::FileNonOverlapped),
            (9, CompactionLevel::FileNonOverlapped),
        ],
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
        .with_max_num_files_per_plan(10)
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
            (4, times.time_1_minute_future),
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
        vec![(10, CompactionLevel::Final), (11, CompactionLevel::Final)],
    );

    assert_max_l0_created_at(
        &files,
        // both files have max_l0_created time_5_minutes_future
        // which is the max of all L0 input's max_l0_created_at
        vec![
            (10, times.time_5_minutes_future),
            (11, times.time_5_minutes_future),
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
async fn test_compact_large_overlapes() {
    test_helpers::maybe_start_logging();

    // Simulate a production scenario in which there are two L1 files but one overlaps with three L2 files
    // and their total size > limit 256MB
    // |----------L2.1----------||----------L2.2----------||-----L2.3----|
    // |----------------------------------------L1.1---------------------------||--L1.2--|

    let setup = TestSetup::builder()
        .await
        .with_3_l2_2_l1_scenario_1()
        .await
        // the test setup does not exceed number of files limit
        .with_max_num_files_per_plan(10)
        .with_min_num_l1_files_to_compact(2)
        // the test setup to have total file size exceed max compact size limit
        .with_max_desired_file_size_bytes((4 * 1024) as u64)
        .build()
        .await;

    let files = setup.list_by_table_not_to_delete().await;
    // verify 5 files
    insta::assert_yaml_snapshot!(
        format_files("initial", &files),
        @r###"
    ---
    - initial
    - "L1                                                                                                                 "
    - "L1.4[6000,68000] 240s 3kb|------------------L1.4------------------|                                                "
    - "L1.5[136000,136000] 300s 2kb                                                                                          |L1.5|"
    - "L2                                                                                                                 "
    - "L2.1[8000,12000] 60s 2kb  |L2.1|                                                                                   "
    - "L2.2[20000,30000] 120s 3kb         |L2.2|                                                                           "
    - "L2.3[36000,36000] 180s 2kb                    |L2.3|                                                                "
    "###
    );

    // compact
    setup.run_compact().await;

    let mut files = setup.list_by_table_not_to_delete().await;
    insta::assert_yaml_snapshot!(
        format_files("initial", &files),
        @r###"
    ---
    - initial
    - "L2                                                                                                                 "
    - "L2.5[136000,136000] 300s 2kb                                                                                          |L2.5|"
    - "L2.6[6000,30000] 240s 3kb|-----L2.6-----|                                                                          "
    - "L2.7[36000,36000] 240s 3kb                    |L2.7|                                                                "
    - "L2.8[68000,68000] 240s 3kb                                          |L2.8|                                          "
    "###
    );

    assert_eq!(files.len(), 4);

    // order files on their min_time
    files.sort_by_key(|f| f.min_time);

    // time range: [6000,36000]
    let file = files[0].clone();
    let batches = setup.read_parquet_file(file).await;
    assert_batches_sorted_eq!(
        &[
            "+-----------+------+------+------+-----------------------------+",
            "| field_int | tag1 | tag2 | tag3 | time                        |",
            "+-----------+------+------+------+-----------------------------+",
            "| 10        | VT   |      |      | 1970-01-01T00:00:00.000006Z |",
            "| 10        | VT   |      |      | 1970-01-01T00:00:00.000010Z |",
            "| 1500      | WA   |      |      | 1970-01-01T00:00:00.000008Z |",
            "| 1601      |      | PA   | 15   | 1970-01-01T00:00:00.000028Z |",
            "| 1601      |      | PA   | 15   | 1970-01-01T00:00:00.000030Z |",
            "| 270       | UT   |      |      | 1970-01-01T00:00:00.000025Z |",
            "| 70        | UT   |      |      | 1970-01-01T00:00:00.000020Z |",
            "| 99        | OR   |      |      | 1970-01-01T00:00:00.000012Z |",
            "+-----------+------+------+------+-----------------------------+",
        ],
        &batches
    );

    // time range: [68000,68000]
    let file = files[1].clone();
    let batches = setup.read_parquet_file(file).await;
    assert_batches_sorted_eq!(
        &[
            "+-----------+------+------+------+-----------------------------+",
            "| field_int | tag1 | tag2 | tag3 | time                        |",
            "+-----------+------+------+------+-----------------------------+",
            "| 21        |      | OH   | 21   | 1970-01-01T00:00:00.000036Z |",
            "+-----------+------+------+------+-----------------------------+",
        ],
        &batches
    );

    // time range: [136000,136000]
    let file = files[2].clone();
    let batches = setup.read_parquet_file(file).await;
    assert_batches_sorted_eq!(
        &[
            "+-----------+------+------+------+-----------------------------+",
            "| field_int | tag1 | tag2 | tag3 | time                        |",
            "+-----------+------+------+------+-----------------------------+",
            "| 10        | VT   |      |      | 1970-01-01T00:00:00.000068Z |",
            "+-----------+------+------+------+-----------------------------+",
        ],
        &batches
    );

    let file = files[3].clone();
    let batches = setup.read_parquet_file(file).await;
    assert_batches_sorted_eq!(
        &[
            "+-----------+------+------+-----------------------------+",
            "| field_int | tag2 | tag3 | time                        |",
            "+-----------+------+------+-----------------------------+",
            "| 210       | OH   | 21   | 1970-01-01T00:00:00.000136Z |",
            "+-----------+------+------+-----------------------------+",
        ],
        &batches
    );
}

#[tokio::test]
async fn test_compact_large_overlape_2() {
    test_helpers::maybe_start_logging();

    // Simulate a production scenario in which there are two L1 files that overlap with more than 1 L2 file
    // Scenario 2: two L1 files each overlaps with at least 2 L2 files
    // |----------L2.1----------||----------L2.2----------||-----L2.3----|
    // |----------------------------------------L1.1----||------L1.2--------|

    let setup = TestSetup::builder()
        .await
        .with_3_l2_2_l1_scenario_2()
        .await
        // the test setup does not exceed number of files limit
        .with_max_num_files_per_plan(10)
        .with_min_num_l1_files_to_compact(2)
        // the test setup to have total file size exceed max compact size limit
        .with_max_desired_file_size_bytes((4 * 1024) as u64)
        .build()
        .await;

    // verify 5 files
    let files = setup.list_by_table_not_to_delete().await;
    insta::assert_yaml_snapshot!(
        format_files("initial", &files),
        @r###"
    ---
    - initial
    - "L1                                                                                                                 "
    - "L1.4[6000,25000] 240s 2kb|---L1.4----|                                                                             "
    - "L1.5[28000,136000] 300s 3kb               |----------------------------------L1.5----------------------------------| "
    - "L2                                                                                                                 "
    - "L2.1[8000,12000] 60s 2kb  |L2.1|                                                                                   "
    - "L2.2[20000,30000] 120s 3kb         |L2.2|                                                                           "
    - "L2.3[36000,36000] 180s 2kb                    |L2.3|                                                                "
    "###
    );

    // compact
    setup.run_compact().await;

    let mut files = setup.list_by_table_not_to_delete().await;
    insta::assert_yaml_snapshot!(
        format_files("initial", &files),
        @r###"
    ---
    - initial
    - "L2                                                                                                                 "
    - "L2.6[6000,36000] 300s 3kb|-------L2.6-------|                                                                      "
    - "L2.7[68000,68000] 300s 3kb                                          |L2.7|                                          "
    - "L2.8[136000,136000] 300s 3kb                                                                                          |L2.8|"
    "###
    );

    assert_eq!(files.len(), 3);

    // order files on their min_time
    files.sort_by_key(|f| f.min_time);

    // time range: [6000,36000]
    let file = files[0].clone();
    let batches = setup.read_parquet_file(file).await;
    assert_batches_sorted_eq!(
        &[
            "+-----------+------+------+------+-----------------------------+",
            "| field_int | tag1 | tag2 | tag3 | time                        |",
            "+-----------+------+------+------+-----------------------------+",
            "| 10        | VT   |      |      | 1970-01-01T00:00:00.000006Z |",
            "| 10        | VT   |      |      | 1970-01-01T00:00:00.000010Z |",
            "| 1500      | WA   |      |      | 1970-01-01T00:00:00.000008Z |",
            "| 1601      |      | PA   | 15   | 1970-01-01T00:00:00.000028Z |",
            "| 1601      |      | PA   | 15   | 1970-01-01T00:00:00.000030Z |",
            "| 21        |      | OH   | 21   | 1970-01-01T00:00:00.000036Z |",
            "| 270       | UT   |      |      | 1970-01-01T00:00:00.000025Z |",
            "| 70        | UT   |      |      | 1970-01-01T00:00:00.000020Z |",
            "| 99        | OR   |      |      | 1970-01-01T00:00:00.000012Z |",
            "+-----------+------+------+------+-----------------------------+",
        ],
        &batches
    );

    // time range: [68000,68000]
    let file = files[1].clone();
    let batches = setup.read_parquet_file(file).await;
    assert_batches_sorted_eq!(
        &[
            "+-----------+------+------+------+-----------------------------+",
            "| field_int | tag1 | tag2 | tag3 | time                        |",
            "+-----------+------+------+------+-----------------------------+",
            "| 10        | VT   |      |      | 1970-01-01T00:00:00.000068Z |",
            "+-----------+------+------+------+-----------------------------+",
        ],
        &batches
    );

    // time range: [136000,136000]
    let file = files[2].clone();
    let batches = setup.read_parquet_file(file).await;
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
            "serialize\ncaused by\nJoin Error (panic)\ncaused by\nExternal error: Panic: foo",
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
