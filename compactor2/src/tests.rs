use std::{num::NonZeroUsize, sync::Arc, time::Duration};

use arrow_util::assert_batches_sorted_eq;
use data_types::{CompactionLevel, ParquetFile, PartitionId};
use iox_query::exec::ExecutorType;
use iox_tests::util::TestParquetFileBuilder;
use tracker::AsyncSemaphoreMetrics;

use crate::{
    components::{
        df_planner::panic::PanicDataFusionPlanner, hardcoded::hardcoded_components, Components,
    },
    config::AlgoVersion,
    driver::compact,
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
    run_compact(&setup).await;

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

        run_compact(&setup).await;
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

        run_compact(&setup).await;

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
    run_compact(&setup).await;

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
    run_compact(&setup).await;

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
    run_compact(&setup).await;

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

    run_compact_failing(&setup).await;

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

    run_compact(&setup).await;

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

    run_compact_failing(&setup).await;

    let catalog_files_post = setup.list_by_table_not_to_delete().await;
    assert_eq!(catalog_files_pre, catalog_files_post);

    let object_store_files_post = list_object_store(&setup.catalog.object_store).await;
    assert_eq!(object_store_files_pre, object_store_files_post);

    assert_skipped_compactions(&setup, []).await;
}

#[tokio::test]
async fn test_pr6890() {
    test_helpers::maybe_start_logging();

    let setup = TestSetup::builder()
        .await
        .simulate_without_object_store()
        .build()
        .await;

    // create virtual files
    let mut input_files = vec![];
    for _ in 0..10 {
        let file = setup
            .partition
            .create_parquet_file(
                TestParquetFileBuilder::default()
                    .with_min_time(100)
                    .with_max_time(200)
                    .with_file_size_bytes(1_000_000) // 1MB
                    .with_compaction_level(CompactionLevel::Initial)
                    // need some LP to generate the schema
                    .with_line_protocol("table,tag1=A,tag2=B,tag3=C field_int=1i 100"),
            )
            .await
            .parquet_file;
        input_files.push(file);
    }

    setup.catalog.time_provider.inc(Duration::from_nanos(200));

    insta::assert_yaml_snapshot!(
        format_files("input", &input_files),
        @r###"
    ---
    - input
    - "L0                                                                                                  "
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
    "###
    );

    run_compact(&setup).await;
    assert_skipped_compactions(&setup, []).await;

    let output_files = setup.list_by_table_not_to_delete().await;
    insta::assert_yaml_snapshot!(
        format_files("input", &output_files),
        @r###"
    ---
    - input
    - "L1                                                                                                  "
    - "L1.11[0,0]          |------------------------------------L1.11-------------------------------------|"
    - "L1.12[0,0]          |------------------------------------L1.12-------------------------------------|"
    - "L1.13[0,0]          |------------------------------------L1.13-------------------------------------|"
    - "L1.14[0,0]          |------------------------------------L1.14-------------------------------------|"
    - "L1.15[0,0]          |------------------------------------L1.15-------------------------------------|"
    - "L1.16[0,0]          |------------------------------------L1.16-------------------------------------|"
    - "L1.17[0,0]          |------------------------------------L1.17-------------------------------------|"
    - "L1.18[0,0]          |------------------------------------L1.18-------------------------------------|"
    - "L1.19[0,0]          |------------------------------------L1.19-------------------------------------|"
    - "L1.20[0,0]          |------------------------------------L1.20-------------------------------------|"
    - "L1.21[0,0]          |------------------------------------L1.21-------------------------------------|"
    - "L1.22[0,0]          |------------------------------------L1.22-------------------------------------|"
    - "L1.23[0,0]          |------------------------------------L1.23-------------------------------------|"
    - "L1.24[0,0]          |------------------------------------L1.24-------------------------------------|"
    - "L1.25[0,0]          |------------------------------------L1.25-------------------------------------|"
    - "L1.26[0,0]          |------------------------------------L1.26-------------------------------------|"
    - "L1.27[0,0]          |------------------------------------L1.27-------------------------------------|"
    - "L1.28[0,0]          |------------------------------------L1.28-------------------------------------|"
    - "L1.29[0,0]          |------------------------------------L1.29-------------------------------------|"
    - "L1.30[0,0]          |------------------------------------L1.30-------------------------------------|"
    - "L1.31[0,0]          |------------------------------------L1.31-------------------------------------|"
    - "L1.32[0,0]          |------------------------------------L1.32-------------------------------------|"
    - "L1.33[0,0]          |------------------------------------L1.33-------------------------------------|"
    - "L1.34[0,0]          |------------------------------------L1.34-------------------------------------|"
    - "L1.35[0,0]          |------------------------------------L1.35-------------------------------------|"
    - "L1.36[0,0]          |------------------------------------L1.36-------------------------------------|"
    - "L1.37[0,0]          |------------------------------------L1.37-------------------------------------|"
    - "L1.38[0,0]          |------------------------------------L1.38-------------------------------------|"
    - "L1.39[0,0]          |------------------------------------L1.39-------------------------------------|"
    - "L1.40[0,0]          |------------------------------------L1.40-------------------------------------|"
    - "L1.41[0,0]          |------------------------------------L1.41-------------------------------------|"
    - "L1.42[0,0]          |------------------------------------L1.42-------------------------------------|"
    - "L1.43[0,0]          |------------------------------------L1.43-------------------------------------|"
    - "L1.44[0,0]          |------------------------------------L1.44-------------------------------------|"
    - "L1.45[0,0]          |------------------------------------L1.45-------------------------------------|"
    - "L1.46[0,0]          |------------------------------------L1.46-------------------------------------|"
    - "L1.47[0,0]          |------------------------------------L1.47-------------------------------------|"
    - "L1.48[0,0]          |------------------------------------L1.48-------------------------------------|"
    - "L1.49[0,0]          |------------------------------------L1.49-------------------------------------|"
    - "L1.50[0,0]          |------------------------------------L1.50-------------------------------------|"
    - "L1.51[0,0]          |------------------------------------L1.51-------------------------------------|"
    - "L1.52[0,0]          |------------------------------------L1.52-------------------------------------|"
    - "L1.53[0,0]          |------------------------------------L1.53-------------------------------------|"
    - "L1.54[0,0]          |------------------------------------L1.54-------------------------------------|"
    - "L1.55[0,0]          |------------------------------------L1.55-------------------------------------|"
    - "L1.56[0,0]          |------------------------------------L1.56-------------------------------------|"
    - "L1.57[0,0]          |------------------------------------L1.57-------------------------------------|"
    - "L1.58[0,0]          |------------------------------------L1.58-------------------------------------|"
    - "L1.59[0,0]          |------------------------------------L1.59-------------------------------------|"
    - "L1.60[0,0]          |------------------------------------L1.60-------------------------------------|"
    "###
    );
}

async fn run_compact(setup: &TestSetup) {
    let components = hardcoded_components(&setup.config);
    run_compact_impl(setup, components).await;
}

async fn run_compact_failing(setup: &TestSetup) {
    let components = hardcoded_components(&setup.config);
    let components = Arc::new(Components {
        df_planner: Arc::new(PanicDataFusionPlanner::new()),
        ..components.as_ref().clone()
    });
    run_compact_impl(setup, components).await;
}

async fn run_compact_impl(setup: &TestSetup, components: Arc<Components>) {
    let config = Arc::clone(&setup.config);
    let job_semaphore = Arc::new(
        Arc::new(AsyncSemaphoreMetrics::new(&config.metric_registry, [])).new_semaphore(10),
    );

    // register scratchpad store
    setup
        .catalog
        .exec()
        .new_context(ExecutorType::Reorg)
        .inner()
        .runtime_env()
        .register_object_store(
            "iox",
            config.parquet_store_scratchpad.id(),
            Arc::clone(config.parquet_store_scratchpad.object_store()),
        );

    compact(
        NonZeroUsize::new(10).unwrap(),
        Duration::from_secs(3_6000),
        job_semaphore,
        &components,
    )
    .await;
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
