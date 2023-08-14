//! This module handles the manipulation / execution of storage
//! plans. This is currently implemented using DataFusion, and this
//! interface abstracts away many of the details
pub(crate) mod context;
pub mod field;
pub mod fieldlist;
pub mod gapfill;
mod non_null_checker;
pub mod query_tracing;
mod schema_pivot;
pub mod seriesset;
pub(crate) mod split;
pub mod stringset;
use datafusion_util::config::register_iox_object_store;
use executor::DedicatedExecutor;
use metric::Registry;
use object_store::DynObjectStore;
use parquet_file::storage::StorageId;
mod cross_rt_stream;

use std::{collections::HashMap, fmt::Display, num::NonZeroUsize, sync::Arc};

use datafusion::{
    self,
    execution::{
        disk_manager::DiskManagerConfig,
        memory_pool::MemoryPool,
        runtime_env::{RuntimeConfig, RuntimeEnv},
    },
    logical_expr::{expr_rewriter::normalize_col, Extension},
    logical_expr::{Expr, LogicalPlan},
};

pub use context::{IOxSessionConfig, IOxSessionContext, SessionContextIOxExt};
use schema_pivot::SchemaPivotNode;

use self::{non_null_checker::NonNullCheckerNode, split::StreamSplitNode};

/// Configuration for an Executor
#[derive(Debug, Clone)]
pub struct ExecutorConfig {
    /// Number of threads per thread pool
    pub num_threads: NonZeroUsize,

    /// Target parallelism for query execution
    pub target_query_partitions: NonZeroUsize,

    /// Object stores
    pub object_stores: HashMap<StorageId, Arc<DynObjectStore>>,

    /// Metric registry
    pub metric_registry: Arc<Registry>,

    /// Memory pool size in bytes.
    pub mem_pool_size: usize,
}

impl Display for ExecutorConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "num_threads={}, target_query_partitions={}, mem_pool_size={}",
            self.num_threads, self.target_query_partitions, self.mem_pool_size
        )
    }
}

#[derive(Debug)]
pub struct DedicatedExecutors {
    /// Executor for running user queries
    query_exec: DedicatedExecutor,

    /// Executor for running system/reorganization tasks such as
    /// compact
    reorg_exec: DedicatedExecutor,

    /// Number of threads per thread pool
    num_threads: NonZeroUsize,
}

impl DedicatedExecutors {
    pub fn new(num_threads: NonZeroUsize, metric_registry: Arc<Registry>) -> Self {
        let query_exec =
            DedicatedExecutor::new("IOx Query", num_threads, Arc::clone(&metric_registry));
        let reorg_exec = DedicatedExecutor::new("IOx Reorg", num_threads, metric_registry);

        Self {
            query_exec,
            reorg_exec,
            num_threads,
        }
    }

    pub fn new_testing() -> Self {
        let query_exec = DedicatedExecutor::new_testing();
        let reorg_exec = DedicatedExecutor::new_testing();
        assert_eq!(query_exec.num_threads(), reorg_exec.num_threads());
        let num_threads = query_exec.num_threads();
        Self {
            query_exec,
            reorg_exec,
            num_threads,
        }
    }

    pub fn num_threads(&self) -> NonZeroUsize {
        self.num_threads
    }
}

/// Handles executing DataFusion plans, and marshalling the results into rust
/// native structures.
#[derive(Debug)]
pub struct Executor {
    /// Executors
    executors: Arc<DedicatedExecutors>,

    /// The default configuration options with which to create contexts
    config: ExecutorConfig,

    /// The DataFusion [RuntimeEnv] (including memory manager and disk
    /// manager) used for all executions
    runtime: Arc<RuntimeEnv>,
}

impl Display for Executor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Executor({})", self.config)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutorType {
    /// Run using the pool for queries
    Query,

    /// Run using the pool for system / reorganization tasks
    Reorg,
}

impl Executor {
    /// Creates a new executor with a two dedicated thread pools, each
    /// with num_threads
    pub fn new(
        num_threads: NonZeroUsize,
        mem_pool_size: usize,
        metric_registry: Arc<Registry>,
    ) -> Self {
        Self::new_with_config(ExecutorConfig {
            num_threads,
            target_query_partitions: num_threads,
            object_stores: HashMap::default(),
            metric_registry,
            mem_pool_size,
        })
    }

    /// Create new executor based on a specific config.
    pub fn new_with_config(config: ExecutorConfig) -> Self {
        let executors = Arc::new(DedicatedExecutors::new(
            config.num_threads,
            Arc::clone(&config.metric_registry),
        ));
        Self::new_with_config_and_executors(config, executors)
    }

    /// Get testing executor that runs a on single thread and a low memory bound
    /// to preserve resources.
    pub fn new_testing() -> Self {
        let config = ExecutorConfig {
            num_threads: NonZeroUsize::new(1).unwrap(),
            target_query_partitions: NonZeroUsize::new(1).unwrap(),
            object_stores: HashMap::default(),
            metric_registry: Arc::new(Registry::default()),
            mem_pool_size: 1024 * 1024 * 1024, // 1GB
        };
        let executors = Arc::new(DedicatedExecutors::new_testing());
        Self::new_with_config_and_executors(config, executors)
    }

    /// Low-level constructor.
    ///
    /// This is mostly useful if you wanna keep the executors (because they are quiet expensive to create) but need a fresh IOx runtime.
    ///
    /// # Panic
    /// Panics if the number of threads in `executors` is different from `config`.
    pub fn new_with_config_and_executors(
        config: ExecutorConfig,
        executors: Arc<DedicatedExecutors>,
    ) -> Self {
        assert_eq!(config.num_threads, executors.num_threads);

        let runtime_config = RuntimeConfig::new()
            .with_disk_manager(DiskManagerConfig::Disabled)
            .with_memory_limit(config.mem_pool_size, 1.0);

        let runtime = Arc::new(RuntimeEnv::new(runtime_config).expect("creating runtime"));
        for (id, store) in &config.object_stores {
            register_iox_object_store(&runtime, id, Arc::clone(store));
        }

        Self {
            executors,
            config,
            runtime,
        }
    }

    /// Return a new execution config, suitable for executing a new query or system task.
    ///
    /// Note that this context (and all its clones) will be shut down once `Executor` is dropped.
    pub fn new_execution_config(&self, executor_type: ExecutorType) -> IOxSessionConfig {
        let exec = self.executor(executor_type).clone();
        IOxSessionConfig::new(exec, Arc::clone(&self.runtime))
            .with_target_partitions(self.config.target_query_partitions)
    }

    /// Create a new execution context, suitable for executing a new query or system task
    ///
    /// Note that this context (and all its clones) will be shut down once `Executor` is dropped.
    pub fn new_context(&self, executor_type: ExecutorType) -> IOxSessionContext {
        self.new_execution_config(executor_type).build()
    }

    /// Return the execution pool  of the specified type
    pub fn executor(&self, executor_type: ExecutorType) -> &DedicatedExecutor {
        match executor_type {
            ExecutorType::Query => &self.executors.query_exec,
            ExecutorType::Reorg => &self.executors.reorg_exec,
        }
    }

    /// Initializes shutdown.
    pub fn shutdown(&self) {
        self.executors.query_exec.shutdown();
        self.executors.reorg_exec.shutdown();
    }

    /// Stops all subsequent task executions, and waits for the worker
    /// thread to complete. Note this will shutdown all created contexts.
    ///
    /// Only the first all to `join` will actually wait for the
    /// executing thread to complete. All other calls to join will
    /// complete immediately.
    pub async fn join(&self) {
        self.executors.query_exec.join().await;
        self.executors.reorg_exec.join().await;
    }

    /// Returns the memory pool associated with this `Executor`
    pub fn pool(&self) -> Arc<dyn MemoryPool> {
        Arc::clone(&self.runtime.memory_pool)
    }
}

// No need to implement `Drop` because this is done by DedicatedExecutor already

/// Create a SchemaPivot node which  an arbitrary input like
///  ColA | ColB | ColC
/// ------+------+------
///   1   | NULL | NULL
///   2   | 2    | NULL
///   3   | 2    | NULL
///
/// And pivots it to a table with a single string column for any
/// columns that had non null values.
///
///   non_null_column
///  -----------------
///   "ColA"
///   "ColB"
pub fn make_schema_pivot(input: LogicalPlan) -> LogicalPlan {
    let node = Arc::new(SchemaPivotNode::new(input));

    LogicalPlan::Extension(Extension { node })
}

/// Make a NonNullChecker node takes an arbitrary input array and
/// produces a single string output column that contains
///
/// 1. the single `table_name` string if any of the input columns are non-null
/// 2. zero rows if all of the input columns are null
///
/// For this input:
///
///  ColA | ColB | ColC
/// ------+------+------
///   1   | NULL | NULL
///   2   | 2    | NULL
///   3   | 2    | NULL
///
/// The output would be (given 'the_table_name' was the table name)
///
///   non_null_column
///  -----------------
///   the_table_name
///
/// However, for this input (All NULL)
///
///  ColA | ColB | ColC
/// ------+------+------
///  NULL | NULL | NULL
///  NULL | NULL | NULL
///  NULL | NULL | NULL
///
/// There would be no output rows
///
///   non_null_column
///  -----------------
pub fn make_non_null_checker(table_name: &str, input: LogicalPlan) -> LogicalPlan {
    let node = Arc::new(NonNullCheckerNode::new(table_name, input));

    LogicalPlan::Extension(Extension { node })
}

/// Create a StreamSplit node which takes an input stream of record
/// batches and produces multiple output streams based on  a list of `N` predicates.
/// The output will have `N+1` streams, and each row is sent to the stream
/// corresponding to the first predicate that evaluates to true, or the last stream if none do.
///
/// For example, if the input looks like:
/// ```text
///  X | time
/// ---+-----
///  a | 1000
///  b | 4000
///  c | 2000
/// ```
///
/// A StreamSplit with split_exprs = [`time <= 1000`, `1000 < time <=2000`] will produce the
/// following three output streams (output DataFusion Partitions):
///
///
/// ```text
///  X | time
/// ---+-----
///  a | 1000
/// ```
///
/// ```text
///  X | time
/// ---+-----
///  b | 2000
/// ```
/// and
/// ```text
///  X | time
/// ---+-----
///  b | 4000
/// ```
pub fn make_stream_split(input: LogicalPlan, split_exprs: Vec<Expr>) -> LogicalPlan {
    // rewrite the input expression so that it is fully qualified with the input schema
    let split_exprs = split_exprs
        .into_iter()
        .map(|split_expr| normalize_col(split_expr, &input).expect("normalize is infallable"))
        .collect::<Vec<_>>();

    let node = Arc::new(StreamSplitNode::new(input, split_exprs));
    LogicalPlan::Extension(Extension { node })
}

#[cfg(test)]
mod tests {
    use arrow::{
        array::{ArrayRef, Int64Array, StringArray},
        datatypes::{DataType, Field, Schema, SchemaRef},
    };
    use datafusion::{
        datasource::{provider_as_source, MemTable},
        logical_expr::LogicalPlanBuilder,
    };
    use stringset::StringSet;

    use super::*;
    use crate::exec::stringset::StringSetRef;
    use crate::plan::stringset::StringSetPlan;
    use arrow::record_batch::RecordBatch;

    #[tokio::test]
    async fn executor_known_string_set_plan_ok() {
        let expected_strings = to_set(&["Foo", "Bar"]);
        let plan = StringSetPlan::Known(Arc::clone(&expected_strings));

        let exec = Executor::new_testing();
        let ctx = exec.new_context(ExecutorType::Query);
        let result_strings = ctx.to_string_set(plan).await.unwrap();
        assert_eq!(result_strings, expected_strings);
    }

    #[tokio::test]
    async fn executor_datafusion_string_set_single_plan_no_batches() {
        // Test with a single plan that produces no batches
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Utf8, true)]));
        let scan = make_plan(schema, vec![]);
        let plan: StringSetPlan = vec![scan].into();

        let exec = Executor::new_testing();
        let ctx = exec.new_context(ExecutorType::Query);
        let results = ctx.to_string_set(plan).await.unwrap();

        assert_eq!(results, StringSetRef::new(StringSet::new()));
    }

    #[tokio::test]
    async fn executor_datafusion_string_set_single_plan_one_batch() {
        // Test with a single plan that produces one record batch
        let data = to_string_array(&["foo", "bar", "baz", "foo"]);
        let batch = RecordBatch::try_from_iter_with_nullable(vec![("a", data, true)])
            .expect("created new record batch");
        let scan = make_plan(batch.schema(), vec![batch]);
        let plan: StringSetPlan = vec![scan].into();

        let exec = Executor::new_testing();
        let ctx = exec.new_context(ExecutorType::Query);
        let results = ctx.to_string_set(plan).await.unwrap();

        assert_eq!(results, to_set(&["foo", "bar", "baz"]));
    }

    #[tokio::test]
    async fn executor_datafusion_string_set_single_plan_two_batch() {
        // Test with a single plan that produces multiple record batches
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Utf8, true)]));
        let data1 = to_string_array(&["foo", "bar"]);
        let batch1 = RecordBatch::try_new(Arc::clone(&schema), vec![data1])
            .expect("created new record batch");
        let data2 = to_string_array(&["baz", "foo"]);
        let batch2 = RecordBatch::try_new(Arc::clone(&schema), vec![data2])
            .expect("created new record batch");
        let scan = make_plan(schema, vec![batch1, batch2]);
        let plan: StringSetPlan = vec![scan].into();

        let exec = Executor::new_testing();
        let ctx = exec.new_context(ExecutorType::Query);
        let results = ctx.to_string_set(plan).await.unwrap();

        assert_eq!(results, to_set(&["foo", "bar", "baz"]));
    }

    #[tokio::test]
    async fn executor_datafusion_string_set_multi_plan() {
        // Test with multiple datafusion logical plans
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Utf8, true)]));

        let data1 = to_string_array(&["foo", "bar"]);
        let batch1 = RecordBatch::try_new(Arc::clone(&schema), vec![data1])
            .expect("created new record batch");
        let scan1 = make_plan(Arc::clone(&schema), vec![batch1]);

        let data2 = to_string_array(&["baz", "foo"]);
        let batch2 = RecordBatch::try_new(Arc::clone(&schema), vec![data2])
            .expect("created new record batch");
        let scan2 = make_plan(schema, vec![batch2]);

        let plan: StringSetPlan = vec![scan1, scan2].into();

        let exec = Executor::new_testing();
        let ctx = exec.new_context(ExecutorType::Query);
        let results = ctx.to_string_set(plan).await.unwrap();

        assert_eq!(results, to_set(&["foo", "bar", "baz"]));
    }

    #[tokio::test]
    async fn executor_datafusion_string_set_nulls() {
        // Ensure that nulls in the output set are handled reasonably
        // (error, rather than silently ignored)
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Utf8, true)]));
        let array = StringArray::from_iter(vec![Some("foo"), None]);
        let data = Arc::new(array);
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![data])
            .expect("created new record batch");
        let scan = make_plan(schema, vec![batch]);
        let plan: StringSetPlan = vec![scan].into();

        let exec = Executor::new_testing();
        let ctx = exec.new_context(ExecutorType::Query);
        let results = ctx.to_string_set(plan).await;

        let actual_error = match results {
            Ok(_) => "Unexpected Ok".into(),
            Err(e) => format!("{e}"),
        };
        let expected_error = "unexpected null value";
        assert!(
            actual_error.contains(expected_error),
            "expected error '{expected_error}' not found in '{actual_error:?}'",
        );
    }

    #[tokio::test]
    async fn executor_datafusion_string_set_bad_schema() {
        // Ensure that an incorect schema (an int) gives a reasonable error
        let data: ArrayRef = Arc::new(Int64Array::from(vec![1]));
        let batch =
            RecordBatch::try_from_iter(vec![("a", data)]).expect("created new record batch");
        let scan = make_plan(batch.schema(), vec![batch]);
        let plan: StringSetPlan = vec![scan].into();

        let exec = Executor::new_testing();
        let ctx = exec.new_context(ExecutorType::Query);
        let results = ctx.to_string_set(plan).await;

        let actual_error = match results {
            Ok(_) => "Unexpected Ok".into(),
            Err(e) => format!("{e}"),
        };

        let expected_error = "schema not a single Utf8";
        assert!(
            actual_error.contains(expected_error),
            "expected error '{expected_error}' not found in '{actual_error:?}'"
        );
    }

    #[tokio::test]
    async fn make_schema_pivot_is_planned() {
        // Test that all the planning logic is wired up and that we
        // can make a plan using a SchemaPivot node
        let batch = RecordBatch::try_from_iter_with_nullable(vec![
            ("f1", to_string_array(&["foo", "bar"]), true),
            ("f2", to_string_array(&["baz", "bzz"]), true),
        ])
        .expect("created new record batch");

        let scan = make_plan(batch.schema(), vec![batch]);
        let pivot = make_schema_pivot(scan);
        let plan = vec![pivot].into();

        let exec = Executor::new_testing();
        let ctx = exec.new_context(ExecutorType::Query);
        let results = ctx.to_string_set(plan).await.expect("Executed plan");

        assert_eq!(results, to_set(&["f1", "f2"]));
    }

    /// return a set for testing
    fn to_set(strs: &[&str]) -> StringSetRef {
        StringSetRef::new(strs.iter().map(|s| s.to_string()).collect::<StringSet>())
    }

    fn to_string_array(strs: &[&str]) -> ArrayRef {
        let array: StringArray = strs.iter().map(|s| Some(*s)).collect();
        Arc::new(array)
    }

    // creates a DataFusion plan that reads the RecordBatches into memory
    fn make_plan(schema: SchemaRef, data: Vec<RecordBatch>) -> LogicalPlan {
        let partitions = vec![data];

        let projection = None;

        // model one partition,
        let table = MemTable::try_new(schema, partitions).unwrap();
        let source = provider_as_source(Arc::new(table));

        LogicalPlanBuilder::scan("memtable", source, projection)
            .unwrap()
            .build()
            .unwrap()
    }
}
