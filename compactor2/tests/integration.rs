use arrow_util::assert_batches_sorted_eq;
use data_types::{CompactionLevel, ParquetFile, PartitionId};

use compactor2::config::AlgoVersion;
use compactor2_test_utils::{list_object_store, TestSetup};

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
        .with_compact_version(AlgoVersion::TargetLevel)
        // Set max num file to 4 (< num files) --> many L0s files, comppact 4 L0s into 2 L0s
        .with_max_num_files_per_plan(4)
        // Not compact L1s into L2s becasue tnumber of L1s < 5
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
    // read files and verify 4 files: 2 original L1s and 2 new L1s after L0s are split and compacted into
    let files = setup.list_by_table_not_to_delete().await;
    assert_eq!(files.len(), 4);

    //
    // verify ID and compaction level of the files
    // Original IDs of files: 1, 2, 3, 4, 5, 6
    // 4 L0s files are splitted into 2 groups and compacted into 2 new L0s files with IDs 7, 8
    // Then these 2 new L0s files are compacted into 2 new L1s files with IDs 9, 10
    assert_levels(
        &files,
        vec![
            (1, CompactionLevel::FileNonOverlapped),
            (4, CompactionLevel::FileNonOverlapped),
            (9, CompactionLevel::FileNonOverlapped),
            (10, CompactionLevel::FileNonOverlapped),
        ],
    );
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
        .with_max_num_files_per_plan(10)
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
