//! This module handles the manipulation / execution of storage
//! plans. This is currently implemented using DataFusion, and this
//! interface abstracts away many of the details
pub(crate) mod context;
pub mod field;
pub mod fieldlist;
mod schema_pivot;
pub mod seriesset;
pub(crate) mod split;
pub mod stringset;
mod task;
pub use context::{DEFAULT_CATALOG, DEFAULT_SCHEMA};

use std::sync::Arc;

use datafusion::{
    self,
    logical_plan::{normalize_col, Expr, LogicalPlan},
};

pub use context::{IOxExecutionConfig, IOxExecutionContext};
use schema_pivot::SchemaPivotNode;

use self::{split::StreamSplitNode, task::DedicatedExecutor};

/// Configuration for an Executor
#[derive(Debug, Clone)]
pub struct ExecutorConfig {
    /// Number of threads per thread pool
    pub num_threads: usize,

    /// Target parallelism for query execution
    pub target_query_partitions: usize,
}

/// Handles executing DataFusion plans, and marshalling the results into rust
/// native structures.
///
/// TODO: Have a resource manager that would limit how many plans are
/// running, based on a policy
#[derive(Debug)]
pub struct Executor {
    /// Executor for running user queries
    query_exec: DedicatedExecutor,

    /// Executor for running system/reorganization tasks such as
    /// compact
    reorg_exec: DedicatedExecutor,

    /// The default configuration options with which to create contexts
    config: ExecutorConfig,
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
    pub fn new(num_threads: usize) -> Self {
        Self::new_with_config(ExecutorConfig {
            num_threads,
            target_query_partitions: num_threads,
        })
    }

    pub fn new_with_config(config: ExecutorConfig) -> Self {
        let query_exec = DedicatedExecutor::new("IOx Query Executor Thread", config.num_threads);
        let reorg_exec = DedicatedExecutor::new("IOx Reorg Executor Thread", config.num_threads);

        Self {
            query_exec,
            reorg_exec,
            config,
        }
    }

    /// Return a new execution config, suitable for executing a new query or system task.
    ///
    /// Note that this context (and all its clones) will be shut down once `Executor` is dropped.
    pub fn new_execution_config(&self, executor_type: ExecutorType) -> IOxExecutionConfig {
        let exec = self.executor(executor_type).clone();
        IOxExecutionConfig::new(exec).with_target_partitions(self.config.target_query_partitions)
    }

    /// Create a new execution context, suitable for executing a new query or system task
    ///
    /// Note that this context (and all its clones) will be shut down once `Executor` is dropped.
    pub fn new_context(&self, executor_type: ExecutorType) -> IOxExecutionContext {
        self.new_execution_config(executor_type).build()
    }

    /// Return the execution pool  of the specified type
    fn executor(&self, executor_type: ExecutorType) -> &DedicatedExecutor {
        match executor_type {
            ExecutorType::Query => &self.query_exec,
            ExecutorType::Reorg => &self.reorg_exec,
        }
    }

    /// Stops all subsequent task executions, and waits for the worker
    /// thread to complete. Note this will shutdown all created contexts.
    ///
    /// Only the first all to `join` will actually wait for the
    /// executing thread to complete. All other calls to join will
    /// complete immediately.
    pub fn join(&self) {
        self.query_exec.join();
        self.reorg_exec.join();
    }
}

impl Drop for Executor {
    fn drop(&mut self) {
        self.join();
    }
}

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

    LogicalPlan::Extension { node }
}

/// Create a StreamSplit node which takes an input stream of record
/// batches and produces two output streams based on a predicate
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
/// A StreamSplit with split_expr = `time <= 2000` will produce the
/// following two output streams (output DataFusion Partitions):
///
///
/// ```text
///  X | time
/// ---+-----
///  a | 1000
///  c | 2000
/// ```
/// and
/// ```text
///  X | time
/// ---+-----
///  b | 4000
/// ```
pub fn make_stream_split(input: LogicalPlan, split_expr: Expr) -> LogicalPlan {
    // rewrite the input expression so that it is fully qualified with the input schema
    let split_expr = normalize_col(split_expr, &input).expect("normalize is infallable");

    let node = Arc::new(StreamSplitNode::new(input, split_expr));
    LogicalPlan::Extension { node }
}

/// A type that can provide `IOxExecutionContext` for query
pub trait ExecutionContextProvider {
    /// Returns a new execution context suitable for running queries
    fn new_query_context(
        self: &Arc<Self>,
        span_ctx: Option<trace::ctx::SpanContext>,
    ) -> IOxExecutionContext;
}

#[cfg(test)]
mod tests {
    use arrow::{
        array::{ArrayRef, Int64Array, StringBuilder},
        datatypes::{DataType, Field, Schema, SchemaRef},
    };
    use datafusion::logical_plan::LogicalPlanBuilder;
    use stringset::StringSet;

    use super::*;
    use crate::exec::stringset::StringSetRef;
    use crate::plan::stringset::StringSetPlan;
    use arrow::record_batch::RecordBatch;

    #[tokio::test]
    async fn executor_known_string_set_plan_ok() {
        let expected_strings = to_set(&["Foo", "Bar"]);
        let plan = StringSetPlan::Known(Arc::clone(&expected_strings));

        let exec = Executor::new(1);
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

        let exec = Executor::new(1);
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

        let exec = Executor::new(1);
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

        let exec = Executor::new(1);
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

        let exec = Executor::new(1);
        let ctx = exec.new_context(ExecutorType::Query);
        let results = ctx.to_string_set(plan).await.unwrap();

        assert_eq!(results, to_set(&["foo", "bar", "baz"]));
    }

    #[tokio::test]
    async fn executor_datafusion_string_set_nulls() {
        // Ensure that nulls in the output set are handled reasonably
        // (error, rather than silently ignored)
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Utf8, true)]));
        let mut builder = StringBuilder::new(2);
        builder.append_value("foo").unwrap();
        builder.append_null().unwrap();
        let data = Arc::new(builder.finish());
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![data])
            .expect("created new record batch");
        let scan = make_plan(schema, vec![batch]);
        let plan: StringSetPlan = vec![scan].into();

        let exec = Executor::new(1);
        let ctx = exec.new_context(ExecutorType::Query);
        let results = ctx.to_string_set(plan).await;

        let actual_error = match results {
            Ok(_) => "Unexpected Ok".into(),
            Err(e) => format!("{}", e),
        };
        let expected_error = "unexpected null value";
        assert!(
            actual_error.contains(expected_error),
            "expected error '{}' not found in '{:?}'",
            expected_error,
            actual_error,
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

        let exec = Executor::new(1);
        let ctx = exec.new_context(ExecutorType::Query);
        let results = ctx.to_string_set(plan).await;

        let actual_error = match results {
            Ok(_) => "Unexpected Ok".into(),
            Err(e) => format!("{}", e),
        };

        let expected_error = "schema not a single Utf8";
        assert!(
            actual_error.contains(expected_error),
            "expected error '{}' not found in '{:?}'",
            expected_error,
            actual_error
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

        let exec = Executor::new(1);
        let ctx = exec.new_context(ExecutorType::Query);
        let results = ctx.to_string_set(plan).await.expect("Executed plan");

        assert_eq!(results, to_set(&["f1", "f2"]));
    }

    /// return a set for testing
    fn to_set(strs: &[&str]) -> StringSetRef {
        StringSetRef::new(strs.iter().map(|s| s.to_string()).collect::<StringSet>())
    }

    fn to_string_array(strs: &[&str]) -> ArrayRef {
        let mut builder = StringBuilder::new(strs.len());
        for s in strs {
            builder.append_value(s).expect("appending string");
        }
        Arc::new(builder.finish())
    }

    // creates a DataFusion plan that reads the RecordBatches into memory
    fn make_plan(schema: SchemaRef, data: Vec<RecordBatch>) -> LogicalPlan {
        let projection = None;
        LogicalPlanBuilder::scan_memory(
            vec![data], // model one partition,
            schema,
            projection,
        )
        .unwrap()
        .build()
        .unwrap()
    }
}
