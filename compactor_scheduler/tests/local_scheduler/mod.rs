use std::sync::Arc;

use assert_matches::assert_matches;
use compactor_scheduler::{
    create_scheduler, CompactionJob, LocalSchedulerConfig, Scheduler, SchedulerConfig,
};
use data_types::{ColumnType, ParquetFile, ParquetFileParams, PartitionId, TransitionPartitionId};
use iox_tests::{ParquetFileBuilder, TestCatalog, TestParquetFileBuilder, TestPartition};

mod end_job;
mod get_jobs;
mod update_job_status;

/// Test local_scheduler, with seeded files, used in these integration tests
#[derive(Debug)]
pub struct TestLocalScheduler {
    pub scheduler: Arc<dyn Scheduler>,
    pub catalog: Arc<TestCatalog>,
    test_partition: Arc<TestPartition>,
    seeded_files: (ParquetFile, ParquetFile),
}

impl TestLocalScheduler {
    pub async fn builder() -> Self {
        // create a catalog with a table with one partition
        let catalog = TestCatalog::new();
        let ns = catalog.create_namespace_with_retention("ns", None).await;
        let table = ns.create_table("table1").await;
        table.create_column("time", ColumnType::Time).await;
        table.create_column("load", ColumnType::F64).await;
        let partition = table.create_partition("k").await;

        // file builder
        let file_builder = TestParquetFileBuilder::default().with_line_protocol("table1 load=1 11");
        // seed with two existing parquet files
        let seeded_files = (
            partition
                .create_parquet_file(file_builder.clone())
                .await
                .into(),
            partition.create_parquet_file(file_builder).await.into(),
        );

        // local scheduler in default config
        // created partitions are "hot" and should be returned with `get_jobs()`
        let scheduler = create_scheduler(
            SchedulerConfig::Local(LocalSchedulerConfig::default()),
            catalog.catalog(),
            Arc::clone(&catalog.time_provider()),
            Arc::new(metric::Registry::default()),
            false,
        );

        Self {
            scheduler,
            catalog,
            test_partition: partition,
            seeded_files,
        }
    }

    pub fn get_seeded_files(&self) -> (ParquetFile, ParquetFile) {
        self.seeded_files.clone()
    }

    pub async fn create_params_for_new_parquet_file(&self) -> ParquetFileParams {
        ParquetFileBuilder::new(42)
            .with_partition(self.get_transition_partition_id())
            .build()
            .into()
    }

    pub fn assert_matches_seeded_hot_partition(&self, jobs: &[CompactionJob]) {
        assert_matches!(
            jobs[..],
            [CompactionJob { partition_id, ..}] if partition_id == self.test_partition.partition.id
        );
    }

    /// currently has only 1 partition seeded (by default)
    pub fn get_partition_id(&self) -> PartitionId {
        self.test_partition.partition.id
    }

    pub fn get_transition_partition_id(&self) -> TransitionPartitionId {
        self.test_partition.partition.transition_partition_id()
    }
}
