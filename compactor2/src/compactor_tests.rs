#[cfg(test)]
mod tests {
    use std::{num::NonZeroUsize, sync::Arc, time::Duration};

    use arrow_util::assert_batches_sorted_eq;
    use data_types::{CompactionLevel, ParquetFile};
    use iox_query::exec::ExecutorType;
    use tracker::AsyncSemaphoreMetrics;

    use crate::{
        components::{
            df_planner::panic::PanicDataFusionPlanner, hardcoded::hardcoded_components, Components,
        },
        config::AlgoVersion,
        driver::compact,
        test_util::{list_object_store, AssertFutureExt, TestSetup},
    };

    #[tokio::test]
    async fn test_compact_no_file() {
        test_helpers::maybe_start_logging();

        // no files
        let setup = TestSetup::builder().build().await;

        let files = setup.list_by_table_not_to_delete().await;
        assert!(files.is_empty());

        // compact
        // This wil wait for files forever.
        let fut = run_compact(&setup);
        tokio::pin!(fut);
        fut.assert_pending().await;

        // verify catalog is still empty
        let files = setup.list_by_table_not_to_delete().await;
        assert!(files.is_empty());
    }

    #[tokio::test]
    async fn test_num_files_over_limit() {
        test_helpers::maybe_start_logging();

        // Create a test setup with 6 files
        let mut setup = TestSetup::builder().with_files().build().await;

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

        // Set max num file to 4 (< num files) --> it won't get comapcted
        setup.set_max_input_files_per_partition(4);

        // ----------------- For AllAtOnce version -----------------
        setup.set_compact_version(AlgoVersion::AllAtOnce);
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

        // ----------------- For TargetLevel version -----------------
        setup.set_compact_version(AlgoVersion::TargetLevel);
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

    #[tokio::test]
    async fn test_total_file_size_over_limit() {
        test_helpers::maybe_start_logging();

        // Create a test setup with 6 files
        let mut setup = TestSetup::builder().with_files().build().await;

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

        // get total size of the files
        let total_size = files.iter().map(|f| f.file_size_bytes).sum::<i64>();

        // Set max size < the input fie size
        setup.set_max_input_parquet_bytes_per_partition((total_size - 1) as usize);

        // ----------------- For AllAtOnce version -----------------
        setup.set_compact_version(AlgoVersion::AllAtOnce);
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

        // ----------------- For TargetLevel version -----------------
        setup.set_compact_version(AlgoVersion::TargetLevel);
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

    #[tokio::test]
    async fn test_compact_all_at_once() {
        test_helpers::maybe_start_logging();

        // Create a test setup with 6 files
        let mut setup = TestSetup::builder().with_files().build().await;
        setup.set_compact_version(AlgoVersion::AllAtOnce);

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
        let mut setup = TestSetup::builder().with_files().build().await;
        setup.set_compact_version(AlgoVersion::TargetLevel);
        setup.set_min_num_l1_files_to_compact(2);


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

        // Ensure we have enough resource to compact the files
        setup.set_max_input_files_per_partition(files.len() + 10);
        let total_size = files.iter().map(|f| f.file_size_bytes).sum::<i64>();
        println!("=======  total_size: {}", total_size);
        setup.set_max_input_parquet_bytes_per_partition((total_size + 1000) as usize);

        // compact
        run_compact(&setup).await;

        // verify number of files: 6 files are compacted into 2 files
        let files = setup.list_by_table_not_to_delete().await;
        // assert_eq!(files.len(), 2);

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
        let setup = TestSetup::builder().with_files().build().await;

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
        let setup = TestSetup::builder().with_files().build().await;

        let catalog_files_pre = setup.list_by_table_not_to_delete().await;
        assert!(!catalog_files_pre.is_empty());

        let object_store_files_pre = list_object_store(&setup.catalog.object_store).await;
        assert!(!object_store_files_pre.is_empty());

        run_compact_failing(&setup).await;

        let catalog_files_post = setup.list_by_table_not_to_delete().await;
        assert_eq!(catalog_files_pre, catalog_files_post);

        let object_store_files_post = list_object_store(&setup.catalog.object_store).await;
        assert_eq!(object_store_files_pre, object_store_files_post);

        let skipped = setup
            .catalog
            .catalog
            .repositories()
            .await
            .partitions()
            .list_skipped_compactions()
            .await
            .unwrap();
        assert_eq!(skipped.len(), 1);
    }

    #[tokio::test]
    async fn test_shadow_mode() {
        test_helpers::maybe_start_logging();

        // Create a test setup with 6 files
        let setup = TestSetup::builder()
            .with_files()
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
            .with_files()
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

        let skipped = setup
            .catalog
            .catalog
            .repositories()
            .await
            .partitions()
            .list_skipped_compactions()
            .await
            .unwrap();
        assert_eq!(skipped, vec![]);
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
}
