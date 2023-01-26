#[cfg(test)]
mod tests {
    use std::{num::NonZeroUsize, sync::Arc, time::Duration};

    use arrow_util::assert_batches_sorted_eq;
    use data_types::CompactionLevel;
    use iox_query::exec::ExecutorType;
    use tracker::AsyncSemaphoreMetrics;

    use crate::{
        components::hardcoded::hardcoded_components, driver::compact, test_util::TestSetup,
    };

    #[tokio::test]
    async fn test_compact_no_file() {
        test_helpers::maybe_start_logging();

        // no files
        let setup = TestSetup::new(false).await;

        let files = setup.list_by_table_not_to_delete().await;
        assert!(files.is_empty());

        // compact
        run_compact(&setup).await;

        // verify catalog is still empty
        let files = setup.list_by_table_not_to_delete().await;
        assert!(files.is_empty());
    }

    #[tokio::test]
    async fn test_compact() {
        test_helpers::maybe_start_logging();

        // Create a test setup with 6 files
        let setup = TestSetup::new(true).await;

        // verify 6 files
        let files = setup.list_by_table_not_to_delete().await;
        assert_eq!(files.len(), 6);
        //
        // verify ID and compaction level of the files
        let files_and_levels: Vec<_> = files
            .iter()
            .map(|f| (f.id.get(), f.compaction_level))
            .collect();
        assert_eq!(
            files_and_levels,
            vec![
                (1, CompactionLevel::FileNonOverlapped),
                (2, CompactionLevel::Initial),
                (3, CompactionLevel::Initial),
                (4, CompactionLevel::FileNonOverlapped),
                (5, CompactionLevel::Initial),
                (6, CompactionLevel::Initial),
            ]
        );
        // verify ID and max_l0_created_at
        let time_provider = Arc::clone(&setup.config.time_provider);

        let time_1_minute_future = time_provider.minutes_into_future(1).timestamp_nanos();
        let time_2_minutes_future = time_provider.minutes_into_future(2).timestamp_nanos();
        let time_3_minutes_future = time_provider.minutes_into_future(3).timestamp_nanos();
        let time_5_minutes_future = time_provider.minutes_into_future(5).timestamp_nanos();

        let files_and_max_l0_created_ats: Vec<_> = files
            .iter()
            .map(|f| (f.id.get(), f.max_l0_created_at.get()))
            .collect();
        assert_eq!(
            files_and_max_l0_created_ats,
            vec![
                (1, time_1_minute_future),
                (2, time_2_minutes_future),
                (3, time_5_minutes_future),
                (4, time_3_minutes_future),
                (5, time_5_minutes_future),
                (6, time_2_minutes_future),
            ]
        );

        // compact
        run_compact(&setup).await;

        // verify number of files: 6 files are compacted into 2 files
        let files = setup.list_by_table_not_to_delete().await;
        assert_eq!(files.len(), 2);
        //
        // verify ID and compaction level of the files
        let files_and_levels: Vec<_> = files
            .iter()
            .map(|f| (f.id.get(), f.compaction_level))
            .collect();
        println!("{:?}", files_and_levels);
        assert_eq!(
            files_and_levels,
            vec![
                (7, CompactionLevel::FileNonOverlapped),
                (8, CompactionLevel::FileNonOverlapped),
            ]
        );
        // verify ID and max_l0_created_at
        let files_and_max_l0_created_ats: Vec<_> = files
            .iter()
            .map(|f| (f.id.get(), f.max_l0_created_at.get()))
            .collect();
        // both files have max_l0_created time_5_minutes_future which is the max of all L0 input's max_l0_created_at
        assert_eq!(
            files_and_max_l0_created_ats,
            vec![(7, time_5_minutes_future), (8, time_5_minutes_future),]
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
        let setup = TestSetup::new(true).await;

        // verify 6 files
        let files = setup.list_by_table_not_to_delete().await;
        assert_eq!(files.len(), 6);
        //
        // verify ID and compaction level of the files
        let files_and_levels: Vec<_> = files
            .iter()
            .map(|f| (f.id.get(), f.compaction_level))
            .collect();
        assert_eq!(
            files_and_levels,
            vec![
                (1, CompactionLevel::FileNonOverlapped),
                (2, CompactionLevel::Initial),
                (3, CompactionLevel::Initial),
                (4, CompactionLevel::FileNonOverlapped),
                (5, CompactionLevel::Initial),
                (6, CompactionLevel::Initial),
            ]
        );

        // add the partition into skipped compaction
        setup
            .catalog
            .add_to_skipped_compaction(setup.partition_info.partition_id, "test reason")
            .await;

        // compact but nothing will be compacted because the partition is skipped
        run_compact(&setup).await;

        // verify still 6 files
        let files = setup.list_by_table_not_to_delete().await;
        assert_eq!(files.len(), 6);
        //
        // verify ID and compaction level of the files
        let files_and_levels: Vec<_> = files
            .iter()
            .map(|f| (f.id.get(), f.compaction_level))
            .collect();
        assert_eq!(
            files_and_levels,
            vec![
                (1, CompactionLevel::FileNonOverlapped),
                (2, CompactionLevel::Initial),
                (3, CompactionLevel::Initial),
                (4, CompactionLevel::FileNonOverlapped),
                (5, CompactionLevel::Initial),
                (6, CompactionLevel::Initial),
            ]
        );
    }

    async fn run_compact(setup: &TestSetup) {
        let config = Arc::clone(&setup.config);
        let components = hardcoded_components(&config);
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
}
