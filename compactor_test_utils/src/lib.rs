//! IOx test utils and tests

#![deny(rustdoc::broken_intra_doc_links, rust_2018_idioms)]
#![warn(
    missing_copy_implementations,
    missing_docs,
    clippy::explicit_iter_loop,
    // See https://github.com/influxdata/influxdb_iox/pull/1671
    clippy::future_not_send,
    clippy::use_self,
    clippy::clone_on_ref_ptr,
    clippy::todo,
    clippy::dbg_macro,
    unused_crate_dependencies
)]

// Workaround for "unused crate" lint false positives.
use workspace_hack as _;

mod commit_wrapper;
mod display;
mod simulator;

pub use display::{display_format, display_size, format_files, format_files_split, format_ranges};

use std::{
    collections::{HashMap, HashSet},
    future::Future,
    num::NonZeroUsize,
    sync::{atomic::AtomicUsize, Arc, Mutex},
    time::Duration,
};

use crate::{
    commit_wrapper::{CommitRecorderBuilder, InvariantCheck},
    simulator::ParquetFileSimulator,
};
use async_trait::async_trait;
use backoff::BackoffConfig;
use compactor::{
    compact, config::Config, hardcoded_components, Components, PanicDataFusionPlanner,
    PartitionInfo,
};
use compactor_scheduler::SchedulerConfig;
use data_types::{
    ColumnType, CompactionLevel, ParquetFile, PartitionId, SortedColumnSet, TableId,
    TransitionPartitionId,
};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion_util::config::register_iox_object_store;
use futures::TryStreamExt;
use iox_catalog::interface::Catalog;
use iox_query::exec::ExecutorType;
use iox_tests::{
    ParquetFileBuilder, TestCatalog, TestNamespace, TestParquetFileBuilder, TestPartition,
    TestTable,
};
use iox_time::{MockProvider, Time, TimeProvider};
use object_store::{path::Path, DynObjectStore};
use parquet_file::storage::{ParquetStorage, StorageId};
use schema::sort::SortKey;
use trace::{RingBufferTraceCollector, TraceCollector};
use tracker::AsyncSemaphoreMetrics;

// Default values for the test setup builder
const MAX_DESIRE_FILE_SIZE: u64 = 100 * 1024;
const PERCENTAGE_MAX_FILE_SIZE: u16 = 5;
const SPLIT_PERCENTAGE: u16 = 80;
const MIN_NUM_L1_FILES_TO_COMPACT: usize = 2;

// Warning thresholds
const MAX_DESIRE_FILE_SIZE_OVERAGE_PERCENT: i64 = 50;

/// Creates [`TestSetup`]s
#[derive(Debug)]
pub struct TestSetupBuilder<const WITH_FILES: bool> {
    config: Config,
    catalog: Arc<TestCatalog>,
    ns: Arc<TestNamespace>,
    table: Arc<TestTable>,
    partition: Arc<TestPartition>,
    files: Vec<ParquetFile>,
    /// a shared log of what happened during the test
    run_log: Arc<Mutex<Vec<String>>>,
    /// Checker that catalog invariant are not violated
    invariant_check: Arc<dyn InvariantCheck>,
    /// Split times required to occur during the simulation
    /// This starts as the full list of what we need to see, and they're removed as they occur.
    required_split_times: Arc<Mutex<Vec<i64>>>,
    /// A shared count of total bytes written during test
    bytes_written: Arc<AtomicUsize>,
    /// A shared count of the breakdown of where bytes were written
    bytes_written_per_plan: Arc<Mutex<HashMap<String, usize>>>,
    /// Suppresses showing detailed output of where bytes are written
    suppress_writes_breakdown: bool,
    /// Suppresses showing each 'run' (compact|split) output
    suppress_run_output: bool,
}

impl TestSetupBuilder<false> {
    /// Create a new builder
    pub async fn new() -> Self {
        let catalog = TestCatalog::new();
        let ns = catalog.create_namespace_1hr_retention("ns").await;
        let table = ns.create_table("table").await;
        table.create_column("field_int", ColumnType::I64).await;
        let tag1 = table.create_column("tag1", ColumnType::Tag).await;
        let tag2 = table.create_column("tag2", ColumnType::Tag).await;
        let tag3 = table.create_column("tag3", ColumnType::Tag).await;
        let col_time = table.create_column("time", ColumnType::Time).await;

        let partition = table.create_partition("2022-07-13").await;

        // The sort key comes from the catalog and should be the union of all tags the
        // ingester has seen
        let sort_key = SortKey::from_columns(["tag1", "tag2", "tag3", "time"]);
        let sort_key_col_ids =
            SortedColumnSet::from([tag1.id(), tag2.id(), tag3.id(), col_time.id()]);
        let partition = partition
            .update_sort_key(sort_key.clone(), &sort_key_col_ids)
            .await;

        // Ensure the input scenario conforms to the expected invariants.
        let invariant_check = Arc::new(CatalogInvariants {
            table_id: table.table.id,
            catalog: Arc::clone(&catalog.catalog),
        });

        let suppress_run_output = false;
        let suppress_writes_breakdown = true;

        // Intercept all catalog commit calls to record them in
        // `run_log` as well as ensuring the invariants still hold
        let run_log = Arc::new(Mutex::new(vec![]));
        let commit_wrapper = CommitRecorderBuilder::new(Arc::clone(&run_log))
            .with_invariant_check(Arc::clone(&invariant_check) as _);

        let ring_buffer = Arc::new(RingBufferTraceCollector::new(5));
        let trace_collector: Option<Arc<dyn TraceCollector>> =
            Some(Arc::new(Arc::clone(&ring_buffer)));

        let config = Config {
            metric_registry: catalog.metric_registry(),
            trace_collector,
            catalog: catalog.catalog(),
            scheduler_config: SchedulerConfig::new_local_with_wrapper(Arc::new(commit_wrapper)),
            parquet_store_real: catalog.parquet_store.clone(),
            parquet_store_scratchpad: ParquetStorage::new(
                Arc::new(object_store::memory::InMemory::new()),
                StorageId::from("scratchpad"),
            ),
            time_provider: catalog.time_provider(),
            exec: Arc::clone(&catalog.exec),
            backoff_config: BackoffConfig::default(),
            partition_concurrency: NonZeroUsize::new(1).unwrap(),
            df_concurrency: NonZeroUsize::new(1).unwrap(),
            partition_scratchpad_concurrency: NonZeroUsize::new(1).unwrap(),
            max_desired_file_size_bytes: MAX_DESIRE_FILE_SIZE,
            percentage_max_file_size: PERCENTAGE_MAX_FILE_SIZE,
            split_percentage: SPLIT_PERCENTAGE,
            partition_timeout: Duration::from_secs(3_600),
            shadow_mode: false,
            enable_scratchpad: true,
            min_num_l1_files_to_compact: MIN_NUM_L1_FILES_TO_COMPACT,
            process_once: true,
            simulate_without_object_store: false,
            parquet_files_sink_override: None,
            all_errors_are_fatal: true,
            max_num_columns_per_table: 200,
            max_num_files_per_plan: 200,
            max_partition_fetch_queries_per_second: None,
            gossip_bind_address: None,
            gossip_seeds: vec![],
        };

        let bytes_written = Arc::new(AtomicUsize::new(0));
        let bytes_written_per_plan: Arc<Mutex<HashMap<String, usize>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let required_split_times: Arc<Mutex<Vec<i64>>> = Arc::new(Mutex::new(vec![]));

        Self {
            config,
            catalog,
            ns,
            table,
            partition,
            files: vec![],
            run_log,
            invariant_check,
            required_split_times,
            bytes_written,
            bytes_written_per_plan,
            suppress_writes_breakdown,
            suppress_run_output,
        }
    }

    /// Create a builder with some pre-cooked files
    pub async fn with_files(self) -> TestSetupBuilder<true> {
        let time_provider = self.catalog.time_provider();
        let time_1_minute_future = time_provider.minutes_into_future(1);
        let time_2_minutes_future = time_provider.minutes_into_future(2);
        let time_3_minutes_future = time_provider.minutes_into_future(3);
        let time_5_minutes_future = time_provider.minutes_into_future(5);

        // L1 file
        let lp = [
            "table,tag2=PA,tag3=15 field_int=1601i 30000",
            "table,tag2=OH,tag3=21 field_int=21i 36000", // will be eliminated due to duplicate
        ]
        .join("\n");
        let builder = TestParquetFileBuilder::default()
            .with_line_protocol(&lp)
            .with_min_time(30000)
            .with_max_time(36000)
            .with_creation_time(time_3_minutes_future)
            .with_max_l0_created_at(time_1_minute_future)
            .with_compaction_level(CompactionLevel::FileNonOverlapped); // Prev compaction
        let level_1_file_1_minute_ago = self.partition.create_parquet_file(builder).await.into();

        // L0 file
        let lp = [
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
            .with_min_time(8000)
            .with_max_time(20000)
            .with_compaction_level(CompactionLevel::Initial);
        let level_0_file_16_minutes_ago = self.partition.create_parquet_file(builder).await.into();

        // L0 file
        let lp = [
            "table,tag1=WA field_int=1500i 8000", // latest duplicate and kept
            "table,tag1=VT field_int=10i 6000",
            "table,tag1=UT field_int=270i 25000",
        ]
        .join("\n");
        let builder = TestParquetFileBuilder::default()
            .with_line_protocol(&lp)
            .with_creation_time(time_5_minutes_future)
            .with_max_l0_created_at(time_5_minutes_future)
            .with_min_time(8000)
            .with_max_time(25000)
            .with_compaction_level(CompactionLevel::Initial);
        let level_0_file_5_minutes_ago = self.partition.create_parquet_file(builder).await.into();

        // L1 file
        let lp = [
            "table,tag1=VT field_int=88i 10000", //  will be eliminated due to duplicate.
            // Note: created time more recent than level_0_file_16_minutes_ago
            // but always considered older ingested data
            "table,tag1=OR field_int=99i 12000",
        ]
        .join("\n");
        let builder = TestParquetFileBuilder::default()
            .with_line_protocol(&lp)
            .with_creation_time(time_5_minutes_future)
            .with_min_time(10000)
            .with_max_time(12000)
            .with_max_l0_created_at(time_1_minute_future)
            .with_compaction_level(CompactionLevel::FileNonOverlapped); // Prev compaction
        let level_1_file_1_minute_ago_with_duplicates: ParquetFile =
            self.partition.create_parquet_file(builder).await.into();

        // L0 file
        let lp = ["table,tag2=OH,tag3=21 field_int=22i 36000"].join("\n");
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
        let lp = [
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

        // ensure the catalog still looks good
        let invariant_check = Arc::clone(&self.invariant_check);
        invariant_check.check().await;

        let bytes_written = Arc::new(AtomicUsize::new(0));
        let bytes_written_per_plan: Arc<Mutex<HashMap<String, usize>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let required_split_times: Arc<Mutex<Vec<i64>>> = Arc::new(Mutex::new(vec![]));

        TestSetupBuilder::<true> {
            config: self.config,
            catalog: self.catalog,
            ns: self.ns,
            table: self.table,
            partition: self.partition,
            files,
            run_log: Arc::new(Mutex::new(vec![])),
            invariant_check,
            required_split_times,
            bytes_written,
            bytes_written_per_plan,
            suppress_writes_breakdown: true,
            suppress_run_output: false,
        }
    }

    /// Simulate a production scenario in which there are two L1 files that overlap with more than 1 L2 file
    /// Secnario 1: one L1 file overlaps with three L3 files
    /// |----------L2.1----------||----------L2.2----------||-----L2.3----|
    /// |----------------------------------------L1.1---------------------------||--L1.2--|
    pub async fn with_3_l2_2_l1_scenario_1(&self) -> TestSetupBuilder<true> {
        let time = TestTimes::new(self.catalog.time_provider().as_ref());
        let l2_files = self.create_three_l2_files(time).await;
        let l1_files = self
            .create_two_l1_and_one_overlaps_with_three_l2_files(time)
            .await;

        let files = l2_files.into_iter().chain(l1_files.into_iter()).collect();

        // make sure the catalog still looks good
        let invariant_check = Arc::clone(&self.invariant_check);
        invariant_check.check().await;

        let bytes_written = Arc::new(AtomicUsize::new(0));
        let bytes_written_per_plan: Arc<Mutex<HashMap<String, usize>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let required_split_times: Arc<Mutex<Vec<i64>>> = Arc::new(Mutex::new(vec![]));

        TestSetupBuilder::<true> {
            config: self.config.clone(),
            catalog: Arc::clone(&self.catalog),
            ns: Arc::clone(&self.ns),
            table: Arc::clone(&self.table),
            partition: Arc::clone(&self.partition),
            files,
            run_log: Arc::new(Mutex::new(vec![])),
            invariant_check,
            required_split_times,
            bytes_written,
            bytes_written_per_plan,
            suppress_writes_breakdown: true,
            suppress_run_output: false,
        }
    }

    /// Simulate a production scenario in which there are two L1 files that overlap with more than 1 L2 file
    /// Scenario 2: two L1 files each overlaps with at least 2 L2 files
    /// |----------L2.1----------||----------L2.2----------||-----L2.3----|
    /// |----------------------------------------L1.1----||------L1.2--------|
    pub async fn with_3_l2_2_l1_scenario_2(&self) -> TestSetupBuilder<true> {
        let time = TestTimes::new(self.catalog.time_provider().as_ref());

        let l2_files = self.create_three_l2_files(time).await;
        let l1_files = self
            .create_two_l1_and_both_overlap_with_two_l2_files(time)
            .await;

        let files = l2_files.into_iter().chain(l1_files.into_iter()).collect();

        // ensure the catalog still looks good
        let invariant_check = Arc::clone(&self.invariant_check);
        invariant_check.check().await;

        let bytes_written = Arc::new(AtomicUsize::new(0));
        let bytes_written_per_plan: Arc<Mutex<HashMap<String, usize>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let required_split_times: Arc<Mutex<Vec<i64>>> = Arc::new(Mutex::new(vec![]));

        TestSetupBuilder::<true> {
            config: self.config.clone(),
            catalog: Arc::clone(&self.catalog),
            ns: Arc::clone(&self.ns),
            table: Arc::clone(&self.table),
            partition: Arc::clone(&self.partition),
            files,
            run_log: Arc::new(Mutex::new(vec![])),
            invariant_check,
            required_split_times,
            bytes_written,
            bytes_written_per_plan,
            suppress_writes_breakdown: true,
            suppress_run_output: false,
        }
    }

    /// Create 3 L2 files
    pub async fn create_three_l2_files(&self, time: TestTimes) -> Vec<ParquetFile> {
        // L2.1 file
        let lp = [
            "table,tag1=WA field_int=1000i 8000", // will be eliminated due to duplicate
            "table,tag1=VT field_int=88i 10000",  //  will be eliminated due to duplicate.
            "table,tag1=OR field_int=99i 12000",
        ]
        .join("\n");
        let builder = TestParquetFileBuilder::default()
            .with_line_protocol(&lp)
            .with_creation_time(Time::from_timestamp_nanos(time.time_1_minute_future))
            .with_max_l0_created_at(Time::from_timestamp_nanos(time.time_1_minute_future))
            .with_min_time(8000)
            .with_max_time(12000)
            .with_compaction_level(CompactionLevel::Final);
        let l2_1 = self.partition.create_parquet_file(builder).await.into();

        // L2.2 file
        let lp = [
            "table,tag1=UT field_int=70i 20000",
            "table,tag2=PA,tag3=15 field_int=1601i 30000",
        ]
        .join("\n");
        let builder = TestParquetFileBuilder::default()
            .with_line_protocol(&lp)
            .with_creation_time(Time::from_timestamp_nanos(time.time_2_minutes_future))
            .with_max_l0_created_at(Time::from_timestamp_nanos(time.time_2_minutes_future))
            .with_min_time(20000)
            .with_max_time(30000)
            .with_compaction_level(CompactionLevel::Final);
        let l2_2 = self.partition.create_parquet_file(builder).await.into();

        // L2.3 file
        let lp = ["table,tag2=OH,tag3=21 field_int=21i 36000"].join("\n");
        let builder = TestParquetFileBuilder::default()
            .with_line_protocol(&lp)
            .with_creation_time(Time::from_timestamp_nanos(time.time_3_minutes_future))
            .with_max_l0_created_at(Time::from_timestamp_nanos(time.time_3_minutes_future))
            // the file includes one row of data. min_time and max_time are the same
            .with_min_time(36000)
            .with_max_time(36000)
            .with_compaction_level(CompactionLevel::Final);
        let l2_3 = self.partition.create_parquet_file(builder).await.into();

        // return the files in random order
        vec![l2_2, l2_3, l2_1]
    }

    /// Create 2 L1 files and only one overlaps with 3 L2 files
    pub async fn create_two_l1_and_one_overlaps_with_three_l2_files(
        &self,
        time: TestTimes,
    ) -> Vec<ParquetFile> {
        // L1.1 file
        let lp = [
            "table,tag1=WA field_int=1500i 8000", // latest duplicate and kept
            "table,tag1=VT field_int=10i 10000",  // latest duplicate and kept
            "table,tag1=VT field_int=10i 6000",
            "table,tag1=UT field_int=270i 25000",
            "table,tag2=PA,tag3=15 field_int=1601i 28000",
            "table,tag1=VT field_int=10i 68000",
        ]
        .join("\n");
        let builder = TestParquetFileBuilder::default()
            .with_line_protocol(&lp)
            .with_creation_time(Time::from_timestamp_nanos(time.time_4_minutes_future))
            .with_max_l0_created_at(Time::from_timestamp_nanos(time.time_4_minutes_future))
            .with_min_time(6000)
            .with_max_time(68000)
            .with_compaction_level(CompactionLevel::FileNonOverlapped);
        let l1_1 = self.partition.create_parquet_file(builder).await.into();

        // L1.2 file
        let lp = ["table,tag2=OH,tag3=21 field_int=210i 136000"].join("\n");
        let builder = TestParquetFileBuilder::default()
            .with_line_protocol(&lp)
            .with_min_time(136000)
            .with_max_time(136000)
            .with_creation_time(Time::from_timestamp_nanos(time.time_5_minutes_future))
            .with_max_l0_created_at(Time::from_timestamp_nanos(time.time_5_minutes_future))
            .with_compaction_level(CompactionLevel::FileNonOverlapped);
        let l1_2 = self.partition.create_parquet_file(builder).await.into();

        // return l1 files in random order
        vec![l1_2, l1_1]
    }

    /// Create 2 L1 files and both overlap with at least one L2 file
    pub async fn create_two_l1_and_both_overlap_with_two_l2_files(
        &self,
        time: TestTimes,
    ) -> Vec<ParquetFile> {
        // L1.1 file
        let lp = [
            "table,tag1=WA field_int=1500i 8000", // latest duplicate and kept
            "table,tag1=VT field_int=10i 10000",  // latest duplicate and kept
            "table,tag1=VT field_int=10i 6000",
            "table,tag1=UT field_int=270i 25000",
        ]
        .join("\n");
        let builder = TestParquetFileBuilder::default()
            .with_line_protocol(&lp)
            .with_creation_time(Time::from_timestamp_nanos(time.time_5_minutes_future))
            .with_max_l0_created_at(Time::from_timestamp_nanos(time.time_4_minutes_future))
            .with_min_time(6000)
            .with_max_time(25000)
            .with_compaction_level(CompactionLevel::FileNonOverlapped);
        let l1_1 = self.partition.create_parquet_file(builder).await.into();

        // L1.2 file
        let lp = [
            "table,tag2=PA,tag3=15 field_int=1601i 28000",
            "table,tag1=VT field_int=10i 68000",
            "table,tag2=OH,tag3=21 field_int=210i 136000",
        ]
        .join("\n");
        let builder = TestParquetFileBuilder::default()
            .with_line_protocol(&lp)
            .with_min_time(28000)
            .with_max_time(136000)
            .with_creation_time(Time::from_timestamp_nanos(time.time_5_minutes_future))
            .with_max_l0_created_at(Time::from_timestamp_nanos(time.time_5_minutes_future))
            .with_compaction_level(CompactionLevel::FileNonOverlapped);
        let l1_2 = self.partition.create_parquet_file(builder).await.into();

        // return l1 files in random order
        vec![l1_2, l1_1]
    }
}

impl<const WITH_FILES: bool> TestSetupBuilder<WITH_FILES> {
    /// Use shadow mode
    pub fn with_shadow_mode(mut self) -> Self {
        self.config.shadow_mode = true;
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

    /// Set option to suppress output of compaction runs;
    pub fn with_suppress_run_output(mut self) -> Self {
        self.suppress_run_output = true;
        self
    }

    /// Set option to show detailed output of where bytes are written
    pub fn with_writes_breakdown(mut self) -> Self {
        self.suppress_writes_breakdown = false;
        self
    }

    /// set simulate_without_object_store
    pub fn simulate_without_object_store(mut self) -> Self {
        let run_log = Arc::clone(&self.run_log);
        let bytes_written = Arc::clone(&self.bytes_written);
        let bytes_written_per_plan = Arc::clone(&self.bytes_written_per_plan);
        let required_split_times = Arc::clone(&self.required_split_times);

        self.config.simulate_without_object_store = true;
        self.config.parquet_files_sink_override = Some(Arc::new(ParquetFileSimulator::new(
            run_log,
            bytes_written,
            bytes_written_per_plan,
            required_split_times,
        )));
        self
    }

    /// Set max_desired_file_size_bytes
    pub fn with_max_desired_file_size_bytes(mut self, max_desired_file_size_bytes: u64) -> Self {
        self.config.max_desired_file_size_bytes = max_desired_file_size_bytes;
        self
    }

    /// Set split times required to be used
    pub fn with_required_split_times(self, required_split_times: Vec<i64>) -> Self {
        self.required_split_times
            .lock()
            .unwrap()
            .extend(required_split_times);
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

    /// Set the compaction timeout
    pub fn with_partition_timeout(mut self, partition_timeout: Duration) -> Self {
        self.config.partition_timeout = partition_timeout;
        self
    }

    /// Create a [`TestSetup`]
    pub async fn build(self) -> TestSetup {
        let candidate_partition = Arc::new(PartitionInfo {
            partition_id: self.partition.partition.id,
            partition_hash_id: self.partition.partition.hash_id().cloned(),
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
            run_log: self.run_log,
            bytes_written: self.bytes_written,
            bytes_written_per_plan: self.bytes_written_per_plan,
            invariant_check: self.invariant_check,
            suppress_writes_breakdown: self.suppress_writes_breakdown,
            suppress_run_output: self.suppress_run_output,
            required_split_times: self.required_split_times,
        }
    }
}

/// Contains state for running compactor integration tests with a
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
    /// The compactor configuration
    pub config: Arc<Config>,
    /// allows optionally suppressing detailed output of where bytes are written
    pub suppress_writes_breakdown: bool,
    /// allows optionally suppressing output of running the test
    pub suppress_run_output: bool,
    /// a shared log of what happened during a simulated run
    run_log: Arc<Mutex<Vec<String>>>,
    /// A total of all bytes written during test.
    pub bytes_written: Arc<AtomicUsize>,
    /// A total of bytes written during test per operation.
    pub bytes_written_per_plan: Arc<Mutex<HashMap<String, usize>>>,
    /// Checker that catalog invariant are not violated
    invariant_check: Arc<dyn InvariantCheck>,
    /// Split times required to be used during simulation.
    pub required_split_times: Arc<Mutex<Vec<i64>>>,
}

impl TestSetup {
    /// Create a builder for creating [`TestSetup`]s
    pub async fn builder() -> TestSetupBuilder<false> {
        TestSetupBuilder::new().await
    }

    /// Get the parquet files stored in the catalog
    pub async fn list_by_table_not_to_delete(&self) -> Vec<ParquetFile> {
        self.catalog
            .list_by_table_not_to_delete(self.table.table.id)
            .await
    }

    /// Get the parquet files including the soft-deleted stored in the catalog
    pub async fn list_by_table(&self) -> Vec<ParquetFile> {
        self.catalog
            .catalog
            .repositories()
            .await
            .parquet_files()
            .list_all()
            .await
            .unwrap()
            .into_iter()
            .filter(|f| f.table_id == self.table.table.id)
            .collect()
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
        // clear any existing log entries, if any
        self.run_log.lock().unwrap().clear();

        let config = Arc::clone(&self.config);
        let df_semaphore = Arc::new(
            Arc::new(AsyncSemaphoreMetrics::new(&config.metric_registry, [])).new_semaphore(10),
        );
        let trace_collector = config.trace_collector.clone();

        // register scratchpad store
        let runtime_env = self
            .catalog
            .exec()
            .new_context(ExecutorType::Reorg)
            .inner()
            .runtime_env();
        register_iox_object_store(
            runtime_env,
            config.parquet_store_scratchpad.id(),
            Arc::clone(config.parquet_store_scratchpad.object_store()),
        );

        compact(
            trace_collector,
            NonZeroUsize::new(10).unwrap(),
            config.partition_timeout,
            df_semaphore,
            &components,
            None,
        )
        .await;

        // get the results
        CompactResult {
            run_log: self.run_log.lock().unwrap().clone(),
        }
    }

    /// Checks the catalog contents of this test setup for invariant violations.
    pub async fn verify_invariants(&self) {
        self.invariant_check.check().await
    }

    /// Checks the catalog contents of this test setup for abnormal characteristics.
    ///
    /// Currently checks:
    /// 1. No file sizes exceed max_desired_file_size_bytes by more than MAX_DESIRE_FILE_SIZE_OVERAGE_PERCENT
    pub async fn generate_warnings(&self) -> Vec<String> {
        let warnings: Vec<_> = self
            .list_by_table_not_to_delete()
            .await
            .into_iter()
            .filter(|f| {
                f.file_size_bytes
                    > self.config.max_desired_file_size_bytes as i64
                        * (100 + MAX_DESIRE_FILE_SIZE_OVERAGE_PERCENT)
                        / 100
            })
            .map(|f| {
                format!(
                    "WARNING: file {} exceeds soft limit {} by more than {}%",
                    display_format(&f, true),
                    display_size(self.config.max_desired_file_size_bytes as i64),
                    MAX_DESIRE_FILE_SIZE_OVERAGE_PERCENT
                )
            })
            .collect();

        warnings
    }
}

#[derive(Debug)]
struct CatalogInvariants {
    catalog: Arc<dyn Catalog>,
    table_id: TableId,
}

#[async_trait]
impl InvariantCheck for CatalogInvariants {
    async fn check(&self) {
        verify_catalog_invariants(self.catalog.as_ref(), self.table_id).await
    }
}

/// Checks the catalog contents for invariant violations of the table
///
/// Currently checks:
/// 1. There are no overlapping files (the compactor should never create overlapping L1 or L2 files)
pub async fn verify_catalog_invariants(catalog: &dyn Catalog, table_id: TableId) {
    let files: Vec<_> = catalog
        .repositories()
        .await
        .parquet_files()
        .list_by_table_not_to_delete(table_id)
        .await
        .unwrap()
        .into_iter()
        // ignore files that are deleted
        .filter(|f| f.to_delete.is_none())
        .collect();

    for f1 in &files {
        for f2 in &files {
            assert_no_intra_level_overlap(f1, f2);
            assert_no_inter_level_misordered_overlap(f1, f2);
        }
    }
}

/// Panics if f1 and f2 are different, overlapping files in the
/// (same) L1 or L2 levels (the compactor should never create such files
fn assert_no_intra_level_overlap(f1: &ParquetFile, f2: &ParquetFile) {
    if f1.id != f2.id
        && (f1.compaction_level == CompactionLevel::FileNonOverlapped
            || f1.compaction_level == CompactionLevel::Final)
        && f1.compaction_level == f2.compaction_level
        && f1.overlaps(f2)
    {
        panic!("Found overlapping files at L1/L2 target level!\nf1 = {}\nf2 = {}\n\n{f1:#?}\n\n{f2:#?}",
               display_format(f1, true),
               display_format(f2, true),
        );
    }
}

/// Panics if F2 is higher level than F1, and they overlap, and their max_l0_created_at
/// times are out of order (F1 must be newer than F2)
fn assert_no_inter_level_misordered_overlap(f1: &ParquetFile, f2: &ParquetFile) {
    if f1.id != f2.id
        && f1.compaction_level < f2.compaction_level
        && f1.overlaps(f2)
        && f1.max_l0_created_at < f2.max_l0_created_at
    {
        panic!("Found overlapping files with illegal max_l0_created_at order\nf1 = {}\nf2 = {}\n\n{f1:#?}\n\n{f2:#?}",
               display_format(f1, true),
               display_format(f2, true),
        );
    }
}

/// Information about the compaction that was run
pub struct CompactResult {
    /// [`ParquetFileSimulator`] output, if enabled
    pub run_log: Vec<String>,
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
    /// 4 minutes in the future
    pub time_4_minutes_future: i64,
    /// 5 minutes in the future
    pub time_5_minutes_future: i64,
}

impl TestTimes {
    /// Create a new instance
    pub fn new(time_provider: &dyn TimeProvider) -> Self {
        let time_1_minute_future = time_provider.minutes_into_future(1).timestamp_nanos();
        let time_2_minutes_future = time_provider.minutes_into_future(2).timestamp_nanos();
        let time_3_minutes_future = time_provider.minutes_into_future(3).timestamp_nanos();
        let time_4_minutes_future = time_provider.minutes_into_future(4).timestamp_nanos();
        let time_5_minutes_future = time_provider.minutes_into_future(5).timestamp_nanos();
        Self {
            time_1_minute_future,
            time_2_minutes_future,
            time_3_minutes_future,
            time_4_minutes_future,
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

/// This setup provides the fake partition_id needed for test cases.
///
/// the TransitionPartitionId is to be iterated upon, so this is a single location for updates
pub fn create_fake_partition_id() -> TransitionPartitionId {
    TransitionPartitionId::Deprecated(PartitionId::new(0))
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
///                   |--L1.1--|
///                       |--L0.1--|
pub fn create_overlapped_two_overlapped_files(size: i64) -> Vec<ParquetFile> {
    let time_provider = Arc::new(MockProvider::new(Time::from_timestamp(0, 0).unwrap()));
    let time = TestTimes::new(&time_provider);

    let l1_1 = ParquetFileBuilder::new(11)
        .with_compaction_level(CompactionLevel::FileNonOverlapped)
        .with_time_range(400, 500)
        .with_file_size_bytes(size)
        .with_max_l0_created_at(time.time_1_minute_future)
        .build();

    // L0_1 overlaps with L1_1
    let l0_1 = ParquetFileBuilder::new(1)
        .with_compaction_level(CompactionLevel::Initial)
        .with_time_range(450, 620)
        .with_file_size_bytes(size)
        .with_max_l0_created_at(time.time_2_minutes_future)
        .build();

    // Put the files in random order
    vec![l1_1, l0_1]
}

/// This setup will return files with ranges as follows:
///                  |--L1.1--|  |--L1.2--|  |--L1.3--|
///                                  |--L0.1--|   |--L0.2--| |--L0.3--|
pub fn create_overlapped_l0_l1_files(size: i64) -> Vec<ParquetFile> {
    let time_provider = Arc::new(MockProvider::new(Time::from_timestamp(0, 0).unwrap()));
    let time = TestTimes::new(&time_provider);

    let l1_1 = ParquetFileBuilder::new(11)
        .with_compaction_level(CompactionLevel::FileNonOverlapped)
        .with_time_range(250, 350)
        .with_file_size_bytes(size)
        .with_max_l0_created_at(time.time_1_minute_future)
        .build();
    let l1_2 = ParquetFileBuilder::new(12)
        .with_compaction_level(CompactionLevel::FileNonOverlapped)
        .with_time_range(400, 500)
        .with_file_size_bytes(size)
        .with_max_l0_created_at(time.time_1_minute_future)
        .build();
    let l1_3 = ParquetFileBuilder::new(13)
        .with_compaction_level(CompactionLevel::FileNonOverlapped)
        .with_time_range(600, 700)
        .with_file_size_bytes(size)
        .with_max_l0_created_at(time.time_1_minute_future)
        .build();

    // L0_1 overlaps with L1_2 and L1_3
    let l0_1 = ParquetFileBuilder::new(1)
        .with_compaction_level(CompactionLevel::Initial)
        .with_time_range(450, 620)
        .with_file_size_bytes(size)
        .with_max_l0_created_at(time.time_2_minutes_future)
        .build();
    // L0_2 overlaps with L1_3
    let l0_2 = ParquetFileBuilder::new(2)
        .with_compaction_level(CompactionLevel::Initial)
        .with_time_range(650, 750)
        .with_file_size_bytes(size)
        .with_max_l0_created_at(time.time_3_minutes_future)
        .build();
    // L0_3 overlaps with nothing
    let l0_3 = ParquetFileBuilder::new(3)
        .with_compaction_level(CompactionLevel::Initial)
        .with_time_range(800, 900)
        .with_file_size_bytes(size)
        .with_max_l0_created_at(time.time_5_minutes_future)
        .build();

    // Put the files in random order
    vec![l1_3, l1_2, l0_2, l1_1, l0_1, l0_3]
}

/// This setup will return files with ranges as follows:
///                   |--L1.1--|  |--L1.2--|
///                       |--L0.1--|   |--L0.2--| |--L0.3--|
pub fn create_overlapped_l0_l1_files_2(size: i64) -> Vec<ParquetFile> {
    let time_provider = Arc::new(MockProvider::new(Time::from_timestamp(0, 0).unwrap()));
    let time = TestTimes::new(&time_provider);

    let l1_1 = ParquetFileBuilder::new(12)
        .with_compaction_level(CompactionLevel::FileNonOverlapped)
        .with_time_range(400, 500)
        .with_file_size_bytes(size)
        .with_max_l0_created_at(time.time_1_minute_future)
        .build();
    let l1_2 = ParquetFileBuilder::new(13)
        .with_compaction_level(CompactionLevel::FileNonOverlapped)
        .with_time_range(600, 700)
        .with_file_size_bytes(size)
        .with_max_l0_created_at(time.time_1_minute_future)
        .build();

    // L0_1 overlaps with L1_1 and L1_2
    let l0_1 = ParquetFileBuilder::new(1)
        .with_compaction_level(CompactionLevel::Initial)
        .with_time_range(450, 620)
        .with_file_size_bytes(size)
        .with_max_l0_created_at(time.time_2_minutes_future)
        .build();
    // L0_2 overlaps with L1_2
    let l0_2 = ParquetFileBuilder::new(2)
        .with_compaction_level(CompactionLevel::Initial)
        .with_time_range(650, 750)
        .with_file_size_bytes(size)
        .with_max_l0_created_at(time.time_3_minutes_future)
        .build();
    // L0_3 overlaps with nothing
    let l0_3 = ParquetFileBuilder::new(3)
        .with_compaction_level(CompactionLevel::Initial)
        .with_time_range(800, 900)
        .with_file_size_bytes(size)
        .with_max_l0_created_at(time.time_5_minutes_future)
        .build();

    // Put the files in random order
    vec![l1_2, l0_2, l1_1, l0_1, l0_3]
}

/// Each level-0 file overlaps with at most one level-1 file
///                   |--L1.1--|     |--L1.2--|
///                       |--L0.1--|    |--L0.2--| |--L0.3--|
pub fn create_overlapped_l0_l1_files_3(size: i64) -> Vec<ParquetFile> {
    let time_provider = Arc::new(MockProvider::new(Time::from_timestamp(0, 0).unwrap()));
    let time = TestTimes::new(&time_provider);

    let l1_1 = ParquetFileBuilder::new(12)
        .with_compaction_level(CompactionLevel::FileNonOverlapped)
        .with_time_range(400, 500)
        .with_file_size_bytes(size)
        .with_max_l0_created_at(time.time_1_minute_future)
        .build();
    let l1_2 = ParquetFileBuilder::new(13)
        .with_compaction_level(CompactionLevel::FileNonOverlapped)
        .with_time_range(600, 700)
        .with_file_size_bytes(size)
        .with_max_l0_created_at(time.time_1_minute_future)
        .build();

    // L0_1 overlaps with L1_1
    let l0_1 = ParquetFileBuilder::new(1)
        .with_compaction_level(CompactionLevel::Initial)
        .with_time_range(450, 550)
        .with_file_size_bytes(size)
        .with_max_l0_created_at(time.time_2_minutes_future)
        .build();
    // L0_2 overlaps with L1_2
    let l0_2 = ParquetFileBuilder::new(2)
        .with_compaction_level(CompactionLevel::Initial)
        .with_time_range(650, 750)
        .with_file_size_bytes(size)
        .with_max_l0_created_at(time.time_3_minutes_future)
        .build();
    // L0_3 overlaps with nothing
    let l0_3 = ParquetFileBuilder::new(3)
        .with_compaction_level(CompactionLevel::Initial)
        .with_time_range(800, 900)
        .with_file_size_bytes(size)
        .with_max_l0_created_at(time.time_5_minutes_future)
        .build();

    // Put the files in random order
    vec![l1_2, l0_2, l1_1, l0_1, l0_3]
}

/// This setup will return files with ranges as follows:
///   |--L1.1--|   |--L1.2--|  |--L1.3--|   : target_level files
///     |--L0.1--|   |--L0.3--| |--L0.2--|  : start_level files
/// Note that L0.2 is created before L0.3 but has later time range
pub fn create_overlapped_start_target_files(
    size: i64,
    start_level: CompactionLevel,
) -> Vec<ParquetFile> {
    let time_provider = Arc::new(MockProvider::new(Time::from_timestamp(0, 0).unwrap()));
    let time = TestTimes::new(&time_provider);

    let target_level = start_level.next();

    let l1_1 = ParquetFileBuilder::new(11)
        .with_compaction_level(target_level)
        .with_time_range(100, 200)
        .with_file_size_bytes(size)
        .with_max_l0_created_at(time.time_1_minute_future)
        .build();
    let l1_2 = ParquetFileBuilder::new(12)
        .with_compaction_level(target_level)
        .with_time_range(300, 400)
        .with_file_size_bytes(size)
        .with_max_l0_created_at(time.time_1_minute_future)
        .build();
    let l1_3 = ParquetFileBuilder::new(13)
        .with_compaction_level(target_level)
        .with_time_range(500, 600)
        .with_file_size_bytes(size)
        .with_max_l0_created_at(time.time_1_minute_future)
        .build();

    // L0_1 overlaps with L1_1
    let l0_1 = ParquetFileBuilder::new(1)
        .with_compaction_level(start_level)
        .with_time_range(150, 250)
        .with_file_size_bytes(size)
        .with_max_l0_created_at(time.time_2_minutes_future)
        .build();
    // L0_2 overlaps L1_3
    let l0_2 = ParquetFileBuilder::new(2)
        .with_compaction_level(start_level)
        .with_time_range(550, 650)
        .with_file_size_bytes(size)
        .with_max_l0_created_at(time.time_3_minutes_future)
        .build();
    // L0_3 overlaps with L1_2
    let l0_3 = ParquetFileBuilder::new(3)
        .with_compaction_level(start_level)
        .with_time_range(350, 450)
        .with_file_size_bytes(size)
        .with_max_l0_created_at(time.time_5_minutes_future)
        .build();

    // Put the files in random order
    vec![l1_2, l1_3, l0_2, l1_1, l0_1, l0_3]
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

/// This setup will return files with ranges as follows:
///     |--L2.2--|
///           |--L1.1--|  |--L1.2--|  |--L1.3--|
pub fn create_overlapped_l1_l2_files_2(size: i64) -> Vec<ParquetFile> {
    let l2_2 = ParquetFileBuilder::new(22)
        .with_compaction_level(CompactionLevel::Final)
        .with_time_range(200, 300)
        .with_file_size_bytes(size)
        .with_max_l0_created_at(1)
        .build();

    // L1_1 overlaps with L2_1
    let l1_1 = ParquetFileBuilder::new(11)
        .with_compaction_level(CompactionLevel::FileNonOverlapped)
        .with_time_range(250, 350)
        .with_file_size_bytes(size)
        .with_max_l0_created_at(2)
        .build();
    let l1_2 = ParquetFileBuilder::new(12)
        .with_compaction_level(CompactionLevel::FileNonOverlapped)
        .with_time_range(400, 500)
        .with_file_size_bytes(size)
        .with_max_l0_created_at(3)
        .build();
    let l1_3 = ParquetFileBuilder::new(13)
        .with_compaction_level(CompactionLevel::FileNonOverlapped)
        .with_time_range(600, 700)
        .with_file_size_bytes(size)
        .with_max_l0_created_at(4)
        .build();

    // Put the files in random order
    vec![l1_3, l1_2, l2_2, l1_1]
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

/// This setup will return files with ranges as follows:
///  Input:
///                                              |--L0.1--|    |-L0.2-|
///            |--L1.1--| |--L1.2--| |--L1.3--| |--L1.4--|    |--L1.5--|              |--L1.6--| |--L1.7--|  |--L1.8--|
/// l1 size :     med        large      small       med           med                    small      large       med
pub fn create_overlapped_files_mix_sizes_1(small: i64, med: i64, large: i64) -> Vec<ParquetFile> {
    let l1_1 = ParquetFileBuilder::new(11)
        .with_compaction_level(CompactionLevel::FileNonOverlapped)
        .with_time_range(0, 100)
        .with_file_size_bytes(med)
        .build();
    let l1_2 = ParquetFileBuilder::new(12)
        .with_compaction_level(CompactionLevel::FileNonOverlapped)
        .with_time_range(200, 300)
        .with_file_size_bytes(large)
        .build();
    let l1_3 = ParquetFileBuilder::new(13)
        .with_compaction_level(CompactionLevel::FileNonOverlapped)
        .with_time_range(400, 500)
        .with_file_size_bytes(small)
        .build();
    let l1_4 = ParquetFileBuilder::new(14)
        .with_compaction_level(CompactionLevel::FileNonOverlapped)
        .with_time_range(600, 700)
        .with_file_size_bytes(med)
        .build();
    let l1_5 = ParquetFileBuilder::new(15)
        .with_compaction_level(CompactionLevel::FileNonOverlapped)
        .with_time_range(800, 900)
        .with_file_size_bytes(med)
        .build();
    let l1_6 = ParquetFileBuilder::new(16)
        .with_compaction_level(CompactionLevel::FileNonOverlapped)
        .with_time_range(1000, 1100)
        .with_file_size_bytes(small)
        .build();
    let l1_7 = ParquetFileBuilder::new(17)
        .with_compaction_level(CompactionLevel::FileNonOverlapped)
        .with_time_range(1200, 1300)
        .with_file_size_bytes(large)
        .build();
    let l1_8 = ParquetFileBuilder::new(18)
        .with_compaction_level(CompactionLevel::FileNonOverlapped)
        .with_time_range(1400, 1500)
        .with_file_size_bytes(med)
        .build();

    // L0_1 overlaps with L1_4
    let l0_1 = ParquetFileBuilder::new(1)
        .with_compaction_level(CompactionLevel::Initial)
        .with_time_range(650, 750)
        .with_file_size_bytes(small)
        .build();
    // L0_2 overlaps with L1_5
    let l0_2 = ParquetFileBuilder::new(2)
        .with_compaction_level(CompactionLevel::Initial)
        .with_time_range(820, 850)
        .with_file_size_bytes(small)
        .build();

    vec![l1_3, l1_7, l1_2, l1_1, l1_4, l1_5, l1_6, l0_2, l0_1, l1_8]
}

#[cfg(test)]
mod test {
    use super::*;

    #[tokio::test]
    async fn good_setup_overlapping_l0() {
        let builder = TestSetup::builder().await;

        // two overlapping L0 Files
        builder
            .partition
            .create_parquet_file(
                parquet_builder()
                    .with_compaction_level(CompactionLevel::Initial)
                    .with_min_time(100)
                    .with_max_time(200),
            )
            .await;

        builder
            .partition
            .create_parquet_file(
                parquet_builder()
                    .with_compaction_level(CompactionLevel::Initial)
                    .with_min_time(50)
                    .with_max_time(200),
            )
            .await;

        // expect no panic
        builder.build().await.verify_invariants().await;
    }

    #[tokio::test]
    #[should_panic(expected = "Found overlapping files at L1/L2 target level")]
    async fn bad_setup_overlapping_l1() {
        let builder = TestSetup::builder().await;

        // two overlapping L1 Files

        builder
            .partition
            .create_parquet_file(
                parquet_builder()
                    .with_compaction_level(CompactionLevel::FileNonOverlapped)
                    .with_min_time(50)
                    .with_max_time(200),
            )
            .await;

        builder
            .partition
            .create_parquet_file(
                parquet_builder()
                    .with_compaction_level(CompactionLevel::FileNonOverlapped)
                    .with_min_time(100)
                    .with_max_time(200),
            )
            .await;

        builder.build().await.verify_invariants().await;
    }

    #[tokio::test]
    #[should_panic(expected = "Found overlapping files at L1/L2 target level")]
    async fn bad_setup_overlapping_l1_leading_edge() {
        let builder = TestSetup::builder().await;

        // non overlapping l1 with
        // two overlapping L1 files but right on edge (max time == min time)

        builder
            .partition
            .create_parquet_file(
                parquet_builder()
                    .with_compaction_level(CompactionLevel::FileNonOverlapped)
                    .with_min_time(100)
                    .with_max_time(200),
            )
            .await;

        builder
            .partition
            .create_parquet_file(
                parquet_builder()
                    .with_compaction_level(CompactionLevel::FileNonOverlapped)
                    .with_min_time(50)
                    .with_max_time(75),
            )
            .await;

        builder
            .partition
            .create_parquet_file(
                parquet_builder()
                    .with_compaction_level(CompactionLevel::FileNonOverlapped)
                    .with_min_time(200)
                    .with_max_time(300),
            )
            .await;

        builder.build().await.verify_invariants().await;
    }

    #[tokio::test]
    #[should_panic(expected = "Found overlapping files at L1/L2 target level")]
    async fn bad_setup_overlapping_l2() {
        let builder = TestSetup::builder().await;

        // two overlapping L1 Files
        builder
            .partition
            .create_parquet_file(
                parquet_builder()
                    .with_compaction_level(CompactionLevel::Final)
                    .with_min_time(100)
                    .with_max_time(200),
            )
            .await;

        builder
            .partition
            .create_parquet_file(
                parquet_builder()
                    .with_compaction_level(CompactionLevel::Final)
                    .with_min_time(50)
                    .with_max_time(200),
            )
            .await;

        builder.build().await.verify_invariants().await;
    }

    /// creates a TestParquetFileBuilder setup for layout tests
    pub fn parquet_builder() -> TestParquetFileBuilder {
        TestParquetFileBuilder::default()
            // need some LP to generate the schema
            .with_line_protocol("table,tag1=A,tag2=B,tag3=C field_int=1i 100")
            .with_file_size_bytes(300)
    }
}
