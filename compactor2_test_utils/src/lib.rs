//! IOx test utils and tests

#![deny(rustdoc::broken_intra_doc_links, rust_2018_idioms)]
#![warn(
    missing_copy_implementations,
    missing_docs,
    clippy::explicit_iter_loop,
    clippy::future_not_send,
    clippy::use_self,
    clippy::clone_on_ref_ptr,
    clippy::todo,
    clippy::dbg_macro
)]

mod display;
mod simulator;
pub use display::{display_size, format_files, format_files_split};
use iox_query::exec::ExecutorType;
use simulator::ParquetFileSimulator;
use tracker::AsyncSemaphoreMetrics;

use std::{collections::HashSet, future::Future, num::NonZeroUsize, sync::Arc, time::Duration};

use async_trait::async_trait;
use backoff::BackoffConfig;
use data_types::{ColumnType, CompactionLevel, ParquetFile, TRANSITION_SHARD_NUMBER};
use datafusion::arrow::record_batch::RecordBatch;
use futures::TryStreamExt;
use iox_tests::{
    ParquetFileBuilder, TestCatalog, TestNamespace, TestParquetFileBuilder, TestPartition,
    TestShard, TestTable,
};
use iox_time::TimeProvider;
use object_store::{path::Path, DynObjectStore};
use parquet_file::storage::{ParquetStorage, StorageId};
use schema::sort::SortKey;

use compactor2::{
    compact,
    config::{AlgoVersion, Config, PartitionsSourceConfig},
    hardcoded_components, Components, PanicDataFusionPlanner, PartitionInfo,
};

// Default values for the test setup builder
const SHARD_INDEX: i32 = TRANSITION_SHARD_NUMBER;
const PARTITION_THRESHOLD: Duration = Duration::from_secs(10 * 60); // 10min
const MAX_DESIRE_FILE_SIZE: u64 = 100 * 1024;
const PERCENTAGE_MAX_FILE_SIZE: u16 = 5;
const SPLIT_PERCENTAGE: u16 = 80;
const MIN_NUM_L1_FILES_TO_COMPACT: usize = 2;

/// Creates [`TestSetup`]s
#[derive(Debug)]
pub struct TestSetupBuilder<const WITH_FILES: bool> {
    config: Config,
    catalog: Arc<TestCatalog>,
    ns: Arc<TestNamespace>,
    shard: Arc<TestShard>,
    table: Arc<TestTable>,
    partition: Arc<TestPartition>,
    files: Vec<ParquetFile>,
}

impl TestSetupBuilder<false> {
    /// Create a new builder
    pub async fn new() -> Self {
        let catalog = TestCatalog::new();
        let ns = catalog.create_namespace_1hr_retention("ns").await;
        let shard = ns.create_shard(SHARD_INDEX).await;
        let table = ns.create_table("table").await;
        table.create_column("field_int", ColumnType::I64).await;
        table.create_column("tag1", ColumnType::Tag).await;
        table.create_column("tag2", ColumnType::Tag).await;
        table.create_column("tag3", ColumnType::Tag).await;
        table.create_column("time", ColumnType::Time).await;

        let partition = table
            .with_shard(&shard)
            .create_partition("2022-07-13")
            .await;

        // The sort key comes from the catalog and should be the union of all tags the
        // ingester has seen
        let sort_key = SortKey::from_columns(["tag1", "tag2", "tag3", "time"]);
        let partition = partition.update_sort_key(sort_key.clone()).await;

        let config = Config {
            shard_id: shard.shard.id,
            metric_registry: catalog.metric_registry(),
            catalog: catalog.catalog(),
            parquet_store_real: catalog.parquet_store.clone(),
            parquet_store_scratchpad: ParquetStorage::new(
                Arc::new(object_store::memory::InMemory::new()),
                StorageId::from("scratchpad"),
            ),
            time_provider: catalog.time_provider(),
            exec: Arc::clone(&catalog.exec),
            backoff_config: BackoffConfig::default(),
            partition_concurrency: NonZeroUsize::new(1).unwrap(),
            job_concurrency: NonZeroUsize::new(1).unwrap(),
            partition_scratchpad_concurrency: NonZeroUsize::new(1).unwrap(),
            partition_threshold: PARTITION_THRESHOLD,
            max_desired_file_size_bytes: MAX_DESIRE_FILE_SIZE,
            percentage_max_file_size: PERCENTAGE_MAX_FILE_SIZE,
            split_percentage: SPLIT_PERCENTAGE,
            partition_timeout: Duration::from_secs(3_600),
            partitions_source: PartitionsSourceConfig::CatalogRecentWrites,
            shadow_mode: false,
            ignore_partition_skip_marker: false,
            max_input_parquet_bytes_per_partition: usize::MAX,
            shard_config: None,
            compact_version: AlgoVersion::AllAtOnce,
            min_num_l1_files_to_compact: MIN_NUM_L1_FILES_TO_COMPACT,
            process_once: true,
            simulate_without_object_store: false,
            parquet_files_sink_override: None,
            all_errors_are_fatal: true,
            max_num_columns_per_table: 200,
            max_num_files_per_plan: 200,
        };

        Self {
            config,
            catalog,
            ns,
            shard,
            table,
            partition,
            files: vec![],
        }
    }

    /// Create a buidler with some pre-cooked files
    pub async fn with_files(self) -> TestSetupBuilder<true> {
        let time_provider = self.catalog.time_provider();
        let time_1_minute_future = time_provider.minutes_into_future(1);
        let time_2_minutes_future = time_provider.minutes_into_future(2);
        let time_3_minutes_future = time_provider.minutes_into_future(3);
        let time_5_minutes_future = time_provider.minutes_into_future(5);

        // L1 file
        let lp = vec![
            "table,tag2=PA,tag3=15 field_int=1601i 30000",
            "table,tag2=OH,tag3=21 field_int=21i 36000", // will be eliminated due to duplicate
        ]
        .join("\n");
        let builder = TestParquetFileBuilder::default()
            .with_line_protocol(&lp)
            .with_creation_time(time_3_minutes_future)
            .with_max_l0_created_at(time_1_minute_future)
            .with_compaction_level(CompactionLevel::FileNonOverlapped); // Prev compaction
        let level_1_file_1_minute_ago = self.partition.create_parquet_file(builder).await.into();

        // L0 file
        let lp = vec![
            "table,tag1=WA field_int=1000i 8000", // will be eliminated due to duplicate
            "table,tag1=VT field_int=10i 10000", // latest L0 compared with duplicate in level_1_file_1_minute_ago_with_duplicates
            // keep it
            "table,tag1=UT field_int=70i 20000",
        ]
        .join("\n");
        let builder = TestParquetFileBuilder::default()
            .with_line_protocol(&lp)
            .with_creation_time(time_2_minutes_future)
            .with_max_l0_created_at(time_2_minutes_future)
            .with_compaction_level(CompactionLevel::Initial);
        let level_0_file_16_minutes_ago = self.partition.create_parquet_file(builder).await.into();

        // L0 file
        let lp = vec![
            "table,tag1=WA field_int=1500i 8000", // latest duplicate and kept
            "table,tag1=VT field_int=10i 6000",
            "table,tag1=UT field_int=270i 25000",
        ]
        .join("\n");
        let builder = TestParquetFileBuilder::default()
            .with_line_protocol(&lp)
            .with_creation_time(time_5_minutes_future)
            .with_max_l0_created_at(time_5_minutes_future)
            .with_compaction_level(CompactionLevel::Initial);
        let level_0_file_5_minutes_ago = self.partition.create_parquet_file(builder).await.into();

        // L1 file
        let lp = vec![
            "table,tag1=VT field_int=88i 10000", //  will be eliminated due to duplicate.
            // Note: created time more recent than level_0_file_16_minutes_ago
            // but always considered older ingested data
            "table,tag1=OR field_int=99i 12000",
        ]
        .join("\n");
        let builder = TestParquetFileBuilder::default()
            .with_line_protocol(&lp)
            .with_creation_time(time_5_minutes_future)
            .with_max_l0_created_at(time_3_minutes_future)
            .with_compaction_level(CompactionLevel::FileNonOverlapped); // Prev compaction
        let level_1_file_1_minute_ago_with_duplicates: ParquetFile =
            self.partition.create_parquet_file(builder).await.into();

        // L0 file
        let lp = vec!["table,tag2=OH,tag3=21 field_int=22i 36000"].join("\n");
        let builder = TestParquetFileBuilder::default()
            .with_line_protocol(&lp)
            .with_min_time(0)
            .with_max_time(36000)
            .with_creation_time(time_5_minutes_future)
            .with_max_l0_created_at(time_5_minutes_future)
            // Will put the group size between "small" and "large"
            .with_size_override(50 * 1024 * 1024)
            .with_compaction_level(CompactionLevel::Initial);
        let medium_level_0_file_time_now = self.partition.create_parquet_file(builder).await.into();

        // L0 file
        let lp = vec![
            "table,tag1=VT field_int=10i 68000",
            "table,tag2=OH,tag3=21 field_int=210i 136000",
        ]
        .join("\n");
        let builder = TestParquetFileBuilder::default()
            .with_line_protocol(&lp)
            .with_min_time(36001)
            .with_max_time(136000)
            .with_creation_time(time_2_minutes_future)
            .with_max_l0_created_at(time_2_minutes_future)
            // Will put the group size two multiples over "large"
            .with_size_override(180 * 1024 * 1024)
            .with_compaction_level(CompactionLevel::Initial);
        let large_level_0_file_2_2_minutes_ago =
            self.partition.create_parquet_file(builder).await.into();

        // Order here isn't relevant; the chunk order should ensure the level 1 files are ordered
        // first, then the other files by max seq num.
        let files = vec![
            level_1_file_1_minute_ago,
            level_0_file_16_minutes_ago,
            level_0_file_5_minutes_ago,
            level_1_file_1_minute_ago_with_duplicates,
            medium_level_0_file_time_now,
            large_level_0_file_2_2_minutes_ago,
        ];

        TestSetupBuilder::<true> {
            config: self.config,
            catalog: self.catalog,
            ns: self.ns,
            shard: self.shard,
            table: self.table,
            partition: self.partition,
            files,
        }
    }
}

impl TestSetupBuilder<true> {
    /// Set max_input_parquet_bytes_per_partition
    pub fn with_max_input_parquet_bytes_per_partition_relative_to_total_size(
        self,
        delta: isize,
    ) -> Self {
        let total_size = self.files.iter().map(|f| f.file_size_bytes).sum::<i64>();
        Self {
            config: Config {
                max_input_parquet_bytes_per_partition: (total_size as isize + delta) as usize,
                ..self.config
            },
            ..self
        }
    }
}

impl<const WITH_FILES: bool> TestSetupBuilder<WITH_FILES> {
    /// Use shadow mode
    pub fn with_shadow_mode(mut self) -> Self {
        self.config.shadow_mode = true;
        self
    }

    /// Set compact version
    pub fn with_compact_version(mut self, compact_version: AlgoVersion) -> Self {
        self.config.compact_version = compact_version;
        self
    }

    /// set min_num_l1_files_to_compact
    pub fn with_min_num_l1_files_to_compact(mut self, min_num_l1_files_to_compact: usize) -> Self {
        self.config.min_num_l1_files_to_compact = min_num_l1_files_to_compact;
        self
    }

    /// Set max_num_files_per_plan;
    pub fn with_max_num_files_per_plan(mut self, max_num_files_per_plan: usize) -> Self {
        self.config.max_num_files_per_plan = max_num_files_per_plan;
        self
    }

    /// set simulate_without_object_store
    pub fn simulate_without_object_store(mut self) -> Self {
        self.config.simulate_without_object_store = true;
        self.config.parquet_files_sink_override = Some(Arc::new(ParquetFileSimulator::new()));
        self
    }

    /// Set max_desired_file_size_bytes
    pub fn with_max_desired_file_size_bytes(mut self, max_desired_file_size_bytes: u64) -> Self {
        self.config.max_desired_file_size_bytes = max_desired_file_size_bytes;
        self
    }

    /// Set percentage_max_file_size
    pub fn with_percentage_max_file_size(mut self, percentage_max_file_size: u16) -> Self {
        self.config.percentage_max_file_size = percentage_max_file_size;
        self
    }

    /// Set split_percentage
    pub fn with_split_percentage(mut self, split_percentage: u16) -> Self {
        self.config.split_percentage = split_percentage;
        self
    }

    /// Set max_input_parquet_bytes_per_partition
    pub fn with_max_input_parquet_bytes_per_partition(
        mut self,
        max_input_parquet_bytes_per_partition: usize,
    ) -> Self {
        self.config.max_input_parquet_bytes_per_partition = max_input_parquet_bytes_per_partition;
        self
    }

    /// Create a [`TestSetup`]
    pub async fn build(self) -> TestSetup {
        let candidate_partition = Arc::new(PartitionInfo {
            partition_id: self.partition.partition.id,
            namespace_id: self.ns.namespace.id,
            namespace_name: self.ns.namespace.name.clone(),
            table: Arc::new(self.table.table.clone()),
            table_schema: Arc::new(self.table.catalog_schema().await),
            sort_key: self.partition.partition.sort_key(),
            partition_key: self.partition.partition.partition_key.clone(),
        });

        TestSetup {
            files: Arc::new(self.files),
            partition_info: candidate_partition,
            catalog: self.catalog,
            table: self.table,
            partition: self.partition,
            config: Arc::new(self.config),
        }
    }
}

/// Contains state for running compactor2 integration tests with a
/// single partition full of files.
pub struct TestSetup {
    /// The parquet files in the partition
    pub files: Arc<Vec<ParquetFile>>,
    /// the information about the partition
    pub partition_info: Arc<PartitionInfo>,
    /// The catalog
    pub catalog: Arc<TestCatalog>,
    /// a test table
    pub table: Arc<TestTable>,
    /// a test partition
    pub partition: Arc<TestPartition>,
    /// The compactor2 configuration
    pub config: Arc<Config>,
}

impl TestSetup {
    /// Create a builder for creating [`TestSetup`]s
    pub async fn builder() -> TestSetupBuilder<false> {
        TestSetupBuilder::new().await
    }

    /// Get the catalog files stored in the catalog
    pub async fn list_by_table_not_to_delete(&self) -> Vec<ParquetFile> {
        self.catalog
            .list_by_table_not_to_delete(self.table.table.id)
            .await
    }

    /// Reads the specified parquet file out of object store
    pub async fn read_parquet_file(&self, file: ParquetFile) -> Vec<RecordBatch> {
        assert_eq!(file.table_id, self.table.table.id);
        self.table.read_parquet_file(file).await
    }

    /// return a set of times relative to config.time_provider.now()
    pub fn test_times(&self) -> TestTimes {
        TestTimes::new(self.config.time_provider.as_ref())
    }

    /// Run compaction job saving simulator state, if any
    pub async fn run_compact(&self) -> CompactResult {
        let components = hardcoded_components(&self.config);
        self.run_compact_impl(Arc::clone(&components)).await
    }

    /// run a compaction plan where the df planner will panic
    pub async fn run_compact_failing(&self) -> CompactResult {
        let components = hardcoded_components(&self.config);
        let components = Arc::new(Components {
            df_planner: Arc::new(PanicDataFusionPlanner::new()),
            ..components.as_ref().clone()
        });
        self.run_compact_impl(components).await
    }

    async fn run_compact_impl(&self, components: Arc<Components>) -> CompactResult {
        let config = Arc::clone(&self.config);
        let job_semaphore = Arc::new(
            Arc::new(AsyncSemaphoreMetrics::new(&config.metric_registry, [])).new_semaphore(10),
        );

        // register scratchpad store
        self.catalog
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

        // get the results
        let simulator_runs = if let Some(simulator) = components
            .parquet_files_sink
            .as_any()
            .downcast_ref::<ParquetFileSimulator>()
        {
            simulator.runs()
        } else {
            vec![]
        };

        CompactResult { simulator_runs }
    }
}

/// Information about the compaction that was run
pub struct CompactResult {
    /// [`ParquetFileSimulator`] output, if enabled
    pub simulator_runs: Vec<String>,
}

/// A collection of nanosecond timestamps relative to now
#[derive(Debug, Clone, Copy)]
pub struct TestTimes {
    /// 1 minute in the future
    pub time_1_minute_future: i64,
    /// 2 minutes in the future
    pub time_2_minutes_future: i64,
    /// 3 minutes in the future
    pub time_3_minutes_future: i64,
    /// 5 minutes in the future
    pub time_5_minutes_future: i64,
}

impl TestTimes {
    fn new(time_provider: &dyn TimeProvider) -> Self {
        let time_1_minute_future = time_provider.minutes_into_future(1).timestamp_nanos();
        let time_2_minutes_future = time_provider.minutes_into_future(2).timestamp_nanos();
        let time_3_minutes_future = time_provider.minutes_into_future(3).timestamp_nanos();
        let time_5_minutes_future = time_provider.minutes_into_future(5).timestamp_nanos();
        Self {
            time_1_minute_future,
            time_2_minutes_future,
            time_3_minutes_future,
            time_5_minutes_future,
        }
    }
}

/// List all paths in the store
pub async fn list_object_store(store: &Arc<DynObjectStore>) -> HashSet<Path> {
    store
        .list(None)
        .await
        .unwrap()
        .map_ok(|f| f.location)
        .try_collect::<HashSet<_>>()
        .await
        .unwrap()
}

#[async_trait]
/// Helper trait for asserting state of a future
pub trait AssertFutureExt {
    /// The output type
    type Output;

    /// Panic's if the future does not return `Poll::Pending`
    async fn assert_pending(&mut self);
    /// Pols with a timeout
    async fn poll_timeout(self) -> Self::Output;
}

#[async_trait]
impl<F> AssertFutureExt for F
where
    F: Future + Send + Unpin,
{
    type Output = F::Output;

    async fn assert_pending(&mut self) {
        tokio::select! {
            biased;
            _ = self => {
                panic!("not pending")
            }
            _ = tokio::time::sleep(Duration::from_millis(10)) => {}
        }
    }

    async fn poll_timeout(self) -> Self::Output {
        tokio::time::timeout(Duration::from_millis(10), self)
            .await
            .expect("timeout")
    }
}

/// This setup will return files with ranges as follows:
///                                  |--L0.1--|   |--L0.2--| |--L0.3--|
pub fn create_l0_files(size: i64) -> Vec<ParquetFile> {
    let l0_1 = ParquetFileBuilder::new(1)
        .with_compaction_level(CompactionLevel::Initial)
        .with_time_range(450, 620)
        .with_file_size_bytes(size)
        .build();
    let l0_2 = ParquetFileBuilder::new(2)
        .with_compaction_level(CompactionLevel::Initial)
        .with_time_range(650, 750)
        .with_file_size_bytes(size)
        .build();
    let l0_3 = ParquetFileBuilder::new(3)
        .with_compaction_level(CompactionLevel::Initial)
        .with_time_range(800, 900)
        .with_file_size_bytes(size)
        .build();

    // Put the files in random order
    vec![l0_2, l0_1, l0_3]
}

/// This setup will return files with ranges as follows:
///              |--L0.1-----|
///                |--L0.2--|   |--L0.3--|
/// L0.1 and L0.2 overlap, L0.3 is not overlapping
pub fn create_overlapping_l0_files(size: i64) -> Vec<ParquetFile> {
    let l0_1 = ParquetFileBuilder::new(1)
        .with_compaction_level(CompactionLevel::Initial)
        .with_time_range(100, 200)
        .with_file_size_bytes(size)
        .build();
    let l0_2 = ParquetFileBuilder::new(2)
        .with_compaction_level(CompactionLevel::Initial)
        .with_time_range(150, 180)
        .with_file_size_bytes(size)
        .build();
    let l0_3 = ParquetFileBuilder::new(3)
        .with_compaction_level(CompactionLevel::Initial)
        .with_time_range(800, 900)
        .with_file_size_bytes(size)
        .build();

    // Put the files in random order
    vec![l0_2, l0_1, l0_3]
}

/// This setup will return files with ranges as follows:
///                  |--L1.1--|  |--L1.2--|  |--L1.3--|
pub fn create_l1_files(size: i64) -> Vec<ParquetFile> {
    let l1_1 = ParquetFileBuilder::new(11)
        .with_compaction_level(CompactionLevel::FileNonOverlapped)
        .with_time_range(250, 350)
        .with_file_size_bytes(size)
        .build();
    let l1_2 = ParquetFileBuilder::new(12)
        .with_compaction_level(CompactionLevel::FileNonOverlapped)
        .with_time_range(400, 500)
        .with_file_size_bytes(size)
        .build();
    let l1_3 = ParquetFileBuilder::new(13)
        .with_compaction_level(CompactionLevel::FileNonOverlapped)
        .with_time_range(600, 700)
        .with_file_size_bytes(size)
        .build();
    // Put the files in random order
    vec![l1_3, l1_2, l1_1]
}

/// This setup will return files with ranges as follows:
///           |--L1.1--|  |--L1.2--|  |--L1.3--| |--L1.4--|  |--L1.5--|
///  . small files (< size ): L1.1, L1.3
///  . Large files (.= size): L1.2, L1.4, L1.5
pub fn create_l1_files_mix_size(size: i64) -> Vec<ParquetFile> {
    let l1_1 = ParquetFileBuilder::new(11)
        .with_compaction_level(CompactionLevel::FileNonOverlapped)
        .with_time_range(250, 350)
        .with_file_size_bytes(size - 1)
        .build();
    let l1_2 = ParquetFileBuilder::new(12)
        .with_compaction_level(CompactionLevel::FileNonOverlapped)
        .with_time_range(400, 500)
        .with_file_size_bytes(size + 1)
        .build();
    let l1_3 = ParquetFileBuilder::new(13)
        .with_compaction_level(CompactionLevel::FileNonOverlapped)
        .with_time_range(600, 700)
        .with_file_size_bytes(size - 10)
        .build();
    let l1_4 = ParquetFileBuilder::new(14)
        .with_compaction_level(CompactionLevel::FileNonOverlapped)
        .with_time_range(800, 900)
        .with_file_size_bytes(size)
        .build();
    let l1_5 = ParquetFileBuilder::new(15)
        .with_compaction_level(CompactionLevel::FileNonOverlapped)
        .with_time_range(1000, 1100)
        .with_file_size_bytes(size + 100)
        .build();

    // Put the files in random order
    vec![l1_5, l1_3, l1_2, l1_1, l1_4]
}

/// This setup will return files with ranges as follows:
///    |--L2.1--|  |--L2.2--|
pub fn create_l2_files() -> Vec<ParquetFile> {
    let l2_1 = ParquetFileBuilder::new(21)
        .with_compaction_level(CompactionLevel::Final)
        .with_time_range(0, 100)
        .build();
    let l2_2 = ParquetFileBuilder::new(22)
        .with_compaction_level(CompactionLevel::Final)
        .with_time_range(200, 300)
        .build();

    // Put the files in random order
    vec![l2_1, l2_2]
}

/// This setup will return files with ranges as follows:
///                  |--L1.1--|  |--L1.2--|  |--L1.3--|
///                                  |--L0.1--|   |--L0.2--| |--L0.3--|
pub fn create_overlapped_l0_l1_files(size: i64) -> Vec<ParquetFile> {
    let l1_1 = ParquetFileBuilder::new(11)
        .with_compaction_level(CompactionLevel::FileNonOverlapped)
        .with_time_range(250, 350)
        .with_file_size_bytes(size)
        .build();
    let l1_2 = ParquetFileBuilder::new(12)
        .with_compaction_level(CompactionLevel::FileNonOverlapped)
        .with_time_range(400, 500)
        .with_file_size_bytes(size)
        .build();
    let l1_3 = ParquetFileBuilder::new(13)
        .with_compaction_level(CompactionLevel::FileNonOverlapped)
        .with_time_range(600, 700)
        .with_file_size_bytes(size)
        .build();

    // L0_1 overlaps with L1_2 and L1_3
    let l0_1 = ParquetFileBuilder::new(1)
        .with_compaction_level(CompactionLevel::Initial)
        .with_time_range(450, 620)
        .with_file_size_bytes(size)
        .build();
    // L0_2 overlaps with L1_3
    let l0_2 = ParquetFileBuilder::new(2)
        .with_compaction_level(CompactionLevel::Initial)
        .with_time_range(650, 750)
        .with_file_size_bytes(size)
        .build();
    // L0_3 overlaps with nothing
    let l0_3 = ParquetFileBuilder::new(3)
        .with_compaction_level(CompactionLevel::Initial)
        .with_time_range(800, 900)
        .with_file_size_bytes(size)
        .build();

    // Put the files in random order
    vec![l1_3, l1_2, l0_2, l1_1, l0_1, l0_3]
}

/// This setup will return files with ranges as follows:
///    |--L2.1--|  |--L2.2--|
///                  |--L1.1--|  |--L1.2--|  |--L1.3--|
pub fn create_overlapped_l1_l2_files(size: i64) -> Vec<ParquetFile> {
    let l2_1 = ParquetFileBuilder::new(21)
        .with_compaction_level(CompactionLevel::Final)
        .with_time_range(0, 100)
        .with_file_size_bytes(size)
        .build();
    let l2_2 = ParquetFileBuilder::new(22)
        .with_compaction_level(CompactionLevel::Final)
        .with_time_range(200, 300)
        .with_file_size_bytes(size)
        .build();

    // L1_1 overlaps with L2_1
    let l1_1 = ParquetFileBuilder::new(11)
        .with_compaction_level(CompactionLevel::FileNonOverlapped)
        .with_time_range(250, 350)
        .with_file_size_bytes(size)
        .build();
    let l1_2 = ParquetFileBuilder::new(12)
        .with_compaction_level(CompactionLevel::FileNonOverlapped)
        .with_time_range(400, 500)
        .with_file_size_bytes(size)
        .build();
    let l1_3 = ParquetFileBuilder::new(13)
        .with_compaction_level(CompactionLevel::FileNonOverlapped)
        .with_time_range(600, 700)
        .with_file_size_bytes(size)
        .build();

    // Put the files in random order
    vec![l1_3, l1_2, l2_1, l2_2, l1_1]
}

/// This setup will return files with ranges as follows with mixed sizes:
///    |--L2.1--|  |--L2.2--|
///                  |--L1.1--|  |--L1.2--|  |--L1.3--|
///  Small files (< size): [L1.3]
///  Large files: [L2.1, L2.2, L1.1, L1.2]
pub fn create_overlapped_l1_l2_files_mix_size(size: i64) -> Vec<ParquetFile> {
    let l2_1 = ParquetFileBuilder::new(21)
        .with_compaction_level(CompactionLevel::Final)
        .with_time_range(0, 100)
        .with_file_size_bytes(size)
        .build();
    let l2_2 = ParquetFileBuilder::new(22)
        .with_compaction_level(CompactionLevel::Final)
        .with_time_range(200, 300)
        .with_file_size_bytes(size)
        .build();

    // L1_1 overlaps with L2_1
    let l1_1 = ParquetFileBuilder::new(11)
        .with_compaction_level(CompactionLevel::FileNonOverlapped)
        .with_time_range(250, 350)
        .with_file_size_bytes(size)
        .build();
    let l1_2 = ParquetFileBuilder::new(12)
        .with_compaction_level(CompactionLevel::FileNonOverlapped)
        .with_time_range(400, 500)
        .with_file_size_bytes(size)
        .build();
    let l1_3 = ParquetFileBuilder::new(13)
        .with_compaction_level(CompactionLevel::FileNonOverlapped)
        .with_time_range(600, 700)
        .with_file_size_bytes(size - 1)
        .build();

    // Put the files in random order
    vec![l1_3, l1_2, l2_1, l2_2, l1_1]
}

/// This setup will return files with ranges as follows with mixed sizes:
///    |--L2.1--|  |--L2.2--|
///                  |--L1.1--|  |--L1.2--|  |--L1.3--|
///  Small files (< size): [L1.2]
///  Large files: [L2.1, L2.2, L1.1, L1.3]
pub fn create_overlapped_l1_l2_files_mix_size_2(size: i64) -> Vec<ParquetFile> {
    let l2_1 = ParquetFileBuilder::new(21)
        .with_compaction_level(CompactionLevel::Final)
        .with_time_range(0, 100)
        .with_file_size_bytes(size)
        .build();
    let l2_2 = ParquetFileBuilder::new(22)
        .with_compaction_level(CompactionLevel::Final)
        .with_time_range(200, 300)
        .with_file_size_bytes(size)
        .build();

    // L1_1 overlaps with L2_1
    let l1_1 = ParquetFileBuilder::new(11)
        .with_compaction_level(CompactionLevel::FileNonOverlapped)
        .with_time_range(250, 350)
        .with_file_size_bytes(size)
        .build();
    let l1_2 = ParquetFileBuilder::new(12)
        .with_compaction_level(CompactionLevel::FileNonOverlapped)
        .with_time_range(400, 500)
        .with_file_size_bytes(size - 1)
        .build();
    let l1_3 = ParquetFileBuilder::new(13)
        .with_compaction_level(CompactionLevel::FileNonOverlapped)
        .with_time_range(600, 700)
        .with_file_size_bytes(size)
        .build();

    // Put the files in random order
    vec![l1_3, l1_2, l2_1, l2_2, l1_1]
}

/// This setup will return files with ranges as follows:
///    |--L2.1--|  |--L2.2--|
///                  |--L1.1--|  |--L1.2--|  |--L1.3--|
///                                  |--L0.1--|   |--L0.2--| |--L0.3--|
/// Sizes of L1.3 and L0.3 are set large (100), the rest is default (1)
pub fn create_overlapped_files() -> Vec<ParquetFile> {
    let l2_1 = ParquetFileBuilder::new(21)
        .with_compaction_level(CompactionLevel::Final)
        .with_time_range(0, 100)
        .build();
    let l2_2 = ParquetFileBuilder::new(22)
        .with_compaction_level(CompactionLevel::Final)
        .with_time_range(200, 300)
        .build();

    // L1_1 overlaps with L2_1
    let l1_1 = ParquetFileBuilder::new(11)
        .with_compaction_level(CompactionLevel::FileNonOverlapped)
        .with_time_range(250, 350)
        .build();
    let l1_2 = ParquetFileBuilder::new(12)
        .with_compaction_level(CompactionLevel::FileNonOverlapped)
        .with_time_range(400, 500)
        .build();
    let l1_3 = ParquetFileBuilder::new(13)
        .with_compaction_level(CompactionLevel::FileNonOverlapped)
        .with_time_range(600, 700)
        .with_file_size_bytes(100)
        .build();

    // L0_1 overlaps with L1_2 and L1_3
    let l0_1 = ParquetFileBuilder::new(1)
        .with_compaction_level(CompactionLevel::Initial)
        .with_time_range(450, 620)
        .build();
    // L0_2 overlaps with L1_3
    let l0_2 = ParquetFileBuilder::new(2)
        .with_compaction_level(CompactionLevel::Initial)
        .with_time_range(650, 750)
        .build();
    // L0_3 overlaps with nothing
    let l0_3 = ParquetFileBuilder::new(3)
        .with_compaction_level(CompactionLevel::Initial)
        .with_time_range(800, 900)
        .with_file_size_bytes(100)
        .build();

    // Put the files in random order
    vec![l1_3, l1_2, l2_1, l2_2, l0_2, l1_1, l0_1, l0_3]
}

/// This setup will return files with ranges as follows:
///                          |--L0.1--|             |--L0.2--|
///            |--L1.1--| |--L1.2--|    |--L1.3--|              |--L1.4--|
pub fn create_overlapped_files_2(size: i64) -> Vec<ParquetFile> {
    let l1_1 = ParquetFileBuilder::new(11)
        .with_compaction_level(CompactionLevel::FileNonOverlapped)
        .with_time_range(0, 100)
        .with_file_size_bytes(size)
        .build();
    let l1_2 = ParquetFileBuilder::new(12)
        .with_compaction_level(CompactionLevel::FileNonOverlapped)
        .with_time_range(200, 300)
        .with_file_size_bytes(size)
        .build();
    let l1_3 = ParquetFileBuilder::new(13)
        .with_compaction_level(CompactionLevel::FileNonOverlapped)
        .with_time_range(400, 500)
        .with_file_size_bytes(size)
        .build();
    let l1_4 = ParquetFileBuilder::new(14)
        .with_compaction_level(CompactionLevel::FileNonOverlapped)
        .with_time_range(600, 700)
        .with_file_size_bytes(size)
        .build();
    // L0_1 overlaps with L1_2
    let l0_1 = ParquetFileBuilder::new(1)
        .with_compaction_level(CompactionLevel::Initial)
        .with_time_range(250, 350)
        .with_file_size_bytes(size)
        .build();
    // L0_2 not overlap but in the middle of L1_3 and L1_4
    let l0_2 = ParquetFileBuilder::new(2)
        .with_compaction_level(CompactionLevel::Initial)
        .with_time_range(520, 550)
        .with_file_size_bytes(size)
        .build();

    // Put the files in random order
    vec![l1_3, l1_2, l1_1, l1_4, l0_2, l0_1]
}

/// This setup will return files with ranges as follows:
///             |--L0.1--| |--L0.2--| |--L0.3--|
///                                              |--L0.4--|     |--L0.5--| |--L0.6--|
///                        |--L1.1--|              |--L1.2--|
pub fn create_overlapped_files_3(size: i64) -> Vec<ParquetFile> {
    let l0_1 = ParquetFileBuilder::new(1)
        .with_compaction_level(CompactionLevel::Initial)
        .with_time_range(0, 100)
        .with_file_size_bytes(size)
        .build();
    let l0_2 = ParquetFileBuilder::new(2)
        .with_compaction_level(CompactionLevel::Initial)
        .with_time_range(200, 300)
        .with_file_size_bytes(size)
        .build();
    let l0_3 = ParquetFileBuilder::new(3)
        .with_compaction_level(CompactionLevel::Initial)
        .with_time_range(400, 500)
        .with_file_size_bytes(size)
        .build();
    let l0_4 = ParquetFileBuilder::new(4)
        .with_compaction_level(CompactionLevel::Initial)
        .with_time_range(600, 700)
        .with_file_size_bytes(size)
        .build();
    let l0_5 = ParquetFileBuilder::new(5)
        .with_compaction_level(CompactionLevel::Initial)
        .with_time_range(800, 900)
        .with_file_size_bytes(size)
        .build();
    let l0_6 = ParquetFileBuilder::new(6)
        .with_compaction_level(CompactionLevel::Initial)
        .with_time_range(1000, 1100)
        .with_file_size_bytes(size)
        .build();
    // L1_1 overlaps with L0_2
    let l1_1 = ParquetFileBuilder::new(11)
        .with_compaction_level(CompactionLevel::FileNonOverlapped)
        .with_time_range(250, 350)
        .with_file_size_bytes(size)
        .build();
    // L1_2 overlaps with L0_4
    let l1_2 = ParquetFileBuilder::new(12)
        .with_compaction_level(CompactionLevel::FileNonOverlapped)
        .with_time_range(650, 750)
        .with_file_size_bytes(size)
        .build();

    // Put the files in random order
    vec![l0_3, l0_2, l0_1, l0_4, l0_5, l0_6, l1_1, l1_2]
}

/// This setup will return files with ranges as follows:
///             |--L0.1--| |--L0.2--| |--L0.3--|
///                                              |--L0.4--|     |--L0.5--| |--L0.6--|
///                        |--L1.1--|              |--L1.2--|
/// Small files (< size): L0.6
/// Large files: the rest
pub fn create_overlapped_files_3_mix_size(size: i64) -> Vec<ParquetFile> {
    let l0_1 = ParquetFileBuilder::new(1)
        .with_compaction_level(CompactionLevel::Initial)
        .with_time_range(0, 100)
        .with_file_size_bytes(size)
        .build();
    let l0_2 = ParquetFileBuilder::new(2)
        .with_compaction_level(CompactionLevel::Initial)
        .with_time_range(200, 300)
        .with_file_size_bytes(size)
        .build();
    let l0_3 = ParquetFileBuilder::new(3)
        .with_compaction_level(CompactionLevel::Initial)
        .with_time_range(400, 500)
        .with_file_size_bytes(size)
        .build();
    let l0_4 = ParquetFileBuilder::new(4)
        .with_compaction_level(CompactionLevel::Initial)
        .with_time_range(600, 700)
        .with_file_size_bytes(size)
        .build();
    let l0_5 = ParquetFileBuilder::new(5)
        .with_compaction_level(CompactionLevel::Initial)
        .with_time_range(800, 900)
        .with_file_size_bytes(size)
        .build();
    let l0_6 = ParquetFileBuilder::new(6)
        .with_compaction_level(CompactionLevel::Initial)
        .with_time_range(1000, 1100)
        .with_file_size_bytes(size - 1)
        .build();
    // L1_1 overlaps with L0_2
    let l1_1 = ParquetFileBuilder::new(11)
        .with_compaction_level(CompactionLevel::FileNonOverlapped)
        .with_time_range(250, 350)
        .with_file_size_bytes(size)
        .build();
    // L1_2 overlaps with L0_4
    let l1_2 = ParquetFileBuilder::new(12)
        .with_compaction_level(CompactionLevel::FileNonOverlapped)
        .with_time_range(650, 750)
        .with_file_size_bytes(size)
        .build();

    // Put the files in random order
    vec![l0_3, l0_2, l0_1, l0_4, l0_5, l0_6, l1_1, l1_2]
}
