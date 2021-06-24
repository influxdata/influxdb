//! This module handles the manipulation / execution of storage
//! plans. This is currently implemented using DataFusion, and this
//! interface abstracts away many of the details
pub(crate) mod context;
mod counters;
pub mod field;
pub mod fieldlist;
mod schema_pivot;
pub mod seriesset;
mod split;
pub mod stream;
pub mod stringset;
mod task;
pub use context::{DEFAULT_CATALOG, DEFAULT_SCHEMA};
use futures::{future, Future};

use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use counters::ExecutionCounters;
use datafusion::{
    self,
    logical_plan::{Expr, LogicalPlan},
    physical_plan::ExecutionPlan,
};

use context::IOxExecutionContext;
use schema_pivot::SchemaPivotNode;

use fieldlist::{FieldList, IntoFieldList};
use seriesset::{Error as SeriesSetError, SeriesSetConverter, SeriesSetItem};
use stringset::{IntoStringSet, StringSetRef};
use tokio::sync::mpsc::error::SendError;

use snafu::{ResultExt, Snafu};

use crate::plan::{
    fieldlist::FieldListPlan,
    seriesset::{SeriesSetPlan, SeriesSetPlans},
    stringset::StringSetPlan,
};

use self::{
    split::StreamSplitNode,
    task::{DedicatedExecutor, Error as ExecutorError},
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Plan Execution Error: {}", source))]
    Execution {
        source: Box<dyn std::error::Error + Send + Sync + 'static>,
    },

    #[snafu(display("Internal error optimizing plan: {}", source))]
    DataFusionOptimization {
        source: datafusion::error::DataFusionError,
    },

    #[snafu(display("Internal error during physical planning: {}", source))]
    DataFusionPhysicalPlanning {
        source: datafusion::error::DataFusionError,
    },

    #[snafu(display("Internal error executing plan: {}", source))]
    DataFusionExecution {
        source: datafusion::error::DataFusionError,
    },

    #[snafu(display("Internal error executing series set set plan: {}", source))]
    SeriesSetExecution {
        source: datafusion::error::DataFusionError,
    },

    #[snafu(display("Internal error executing field set plan: {}", source))]
    FieldListExectuor {
        source: datafusion::error::DataFusionError,
    },

    #[snafu(display("Internal error extracting results from Record Batches: {}", message))]
    InternalResultsExtraction { message: String },

    #[snafu(display("Internal error creating StringSet: {}", source))]
    StringSetConversion { source: stringset::Error },

    #[snafu(display("Error converting results to SeriesSet: {}", source))]
    SeriesSetConversion { source: seriesset::Error },

    #[snafu(display("Internal error creating FieldList: {}", source))]
    FieldListConversion { source: fieldlist::Error },

    #[snafu(display("Sending series set results during conversion: {:?}", source))]
    SendingDuringConversion {
        source: Box<SendError<Result<SeriesSetItem, SeriesSetError>>>,
    },

    #[snafu(display("Error joining execution task: {}", source))]
    TaskJoinError { source: ExecutorError },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Handles executing DataFusion plans, and marshalling the results into rust
/// native structures.
#[derive(Debug)]
pub struct Executor {
    counters: Arc<ExecutionCounters>,
    exec: DedicatedExecutor,
}

impl Executor {
    /// Creates a new executor with a single dedicated thread pool with
    /// num_threads
    pub fn new(num_threads: usize) -> Self {
        let exec = DedicatedExecutor::new("IOx Executor Thread", num_threads);

        Self {
            exec,
            counters: Arc::new(ExecutionCounters::default()),
        }
    }

    /// Executes this plan and returns the resulting set of strings
    pub async fn to_string_set(&self, plan: StringSetPlan) -> Result<StringSetRef> {
        match plan {
            StringSetPlan::Known(ss) => Ok(ss),
            StringSetPlan::Plan(plans) => self
                .run_logical_plans(plans)
                .await?
                .into_stringset()
                .context(StringSetConversion),
        }
    }

    /// Executes the embedded plans, each as separate tasks combining the results
    /// into the returned collection of items.
    ///
    /// The SeriesSets are guaranteed to come back ordered by table_name.
    pub async fn to_series_set(
        &self,
        series_set_plans: SeriesSetPlans,
    ) -> Result<Vec<SeriesSetItem>, Error> {
        let SeriesSetPlans { mut plans } = series_set_plans;

        if plans.is_empty() {
            return Ok(vec![]);
        }

        // sort plans by table name
        plans.sort_by(|a, b| a.table_name.cmp(&b.table_name));

        // Run the plans in parallel
        let handles = plans
            .into_iter()
            .map(|plan| {
                // TODO run these on some executor other than the main tokio pool (maybe?)
                let ctx = self.new_context();

                self.exec.spawn(async move {
                    let SeriesSetPlan {
                        table_name,
                        plan,
                        tag_columns,
                        field_columns,
                        num_prefix_tag_group_columns,
                    } = plan;

                    let tag_columns = Arc::new(tag_columns);

                    let physical_plan = ctx
                        .prepare_plan(&plan)
                        .context(DataFusionPhysicalPlanning)?;

                    let it = ctx
                        .execute(physical_plan)
                        .await
                        .context(SeriesSetExecution)?;

                    SeriesSetConverter::default()
                        .convert(
                            table_name,
                            tag_columns,
                            field_columns,
                            num_prefix_tag_group_columns,
                            it,
                        )
                        .await
                        .context(SeriesSetConversion)
                })
            })
            .collect::<Vec<_>>();

        // join_all ensures that the results are consumed in the same order they
        // were spawned maintaining the guarantee to return results ordered
        // by the plan sort order.
        let handles = future::try_join_all(handles).await.context(TaskJoinError)?;
        let mut results = vec![];
        for handle in handles {
            results.extend(handle?.into_iter());
        }

        Ok(results)
    }

    /// Executes `plan` and return the resulting FieldList
    pub async fn to_field_list(&self, plan: FieldListPlan) -> Result<FieldList> {
        let FieldListPlan { plans } = plan;

        // Run the plans in parallel
        let handles = plans
            .into_iter()
            .map(|plan| {
                let ctx = self.new_context();
                self.exec.spawn(async move {
                    let physical_plan = ctx
                        .prepare_plan(&plan)
                        .context(DataFusionPhysicalPlanning)?;

                    // TODO: avoid this buffering
                    let fieldlist = ctx
                        .collect(physical_plan)
                        .await
                        .context(FieldListExectuor)?
                        .into_fieldlist()
                        .context(FieldListConversion);

                    Ok(fieldlist)
                })
            })
            .collect::<Vec<_>>();

        // collect them all up and combine them
        let mut results = Vec::new();
        for join_handle in handles {
            let fieldlist = join_handle.await.context(TaskJoinError)???;

            results.push(fieldlist);
        }

        results.into_fieldlist().context(FieldListConversion)
    }

    /// Run the plan and return a record batch reader for reading the results
    pub async fn run_logical_plan(&self, plan: LogicalPlan) -> Result<Vec<RecordBatch>> {
        self.run_logical_plans(vec![plan]).await
    }

    /// Executes the logical plan using DataFusion on a separate
    /// thread pool and produces RecordBatches
    pub async fn collect(&self, physical_plan: Arc<dyn ExecutionPlan>) -> Result<Vec<RecordBatch>> {
        self.new_context()
            .collect(physical_plan)
            .await
            .context(DataFusionExecution)
    }

    /// Create a new execution context, suitable for executing a new query
    pub fn new_context(&self) -> IOxExecutionContext {
        IOxExecutionContext::new(self.exec.clone(), Arc::clone(&self.counters))
    }

    /// plans and runs the plans in parallel and collects the results
    /// run each plan in parallel and collect the results
    async fn run_logical_plans(&self, plans: Vec<LogicalPlan>) -> Result<Vec<RecordBatch>> {
        let value_futures = plans
            .into_iter()
            .map(|plan| {
                let ctx = self.new_context();

                self.exec.spawn(async move {
                    let physical_plan = ctx
                        .prepare_plan(&plan)
                        .context(DataFusionPhysicalPlanning)?;

                    // TODO: avoid this buffering
                    ctx.collect(physical_plan)
                        .await
                        .context(DataFusionExecution)
                })
            })
            .collect::<Vec<_>>();

        // now, wait for all the values to resolve and collect them together
        let mut results = Vec::new();
        for join_handle in value_futures {
            let mut plan_result = join_handle.await.context(TaskJoinError)??;
            results.append(&mut plan_result);
        }
        Ok(results)
    }

    /// Runs the specified Future (and any tasks it spawns) on the
    /// worker pool for this executor, returning the result of the
    /// computation.
    pub async fn run<T>(&self, task: T) -> Result<T::Output>
    where
        T: Future + Send + 'static,
        T::Output: Send + 'static,
    {
        // run on the dedicated executor
        self.exec
            .spawn(task)
            // wait on the *current* tokio executor
            .await
            .context(TaskJoinError)
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
    let node = Arc::new(StreamSplitNode::new(input, split_expr));
    LogicalPlan::Extension { node }
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

    #[tokio::test]
    async fn executor_known_string_set_plan_ok() {
        let expected_strings = to_set(&["Foo", "Bar"]);
        let plan = StringSetPlan::Known(Arc::clone(&expected_strings));

        let executor = Executor::new(1);
        let result_strings = executor.to_string_set(plan).await.unwrap();
        assert_eq!(result_strings, expected_strings);
    }

    #[tokio::test]
    async fn executor_datafusion_string_set_single_plan_no_batches() {
        // Test with a single plan that produces no batches
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Utf8, true)]));
        let scan = make_plan(schema, vec![]);
        let plan: StringSetPlan = vec![scan].into();

        let executor = Executor::new(1);
        let results = executor.to_string_set(plan).await.unwrap();

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

        let executor = Executor::new(1);
        let results = executor.to_string_set(plan).await.unwrap();

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

        let executor = Executor::new(1);
        let results = executor.to_string_set(plan).await.unwrap();

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

        let executor = Executor::new(1);
        let results = executor.to_string_set(plan).await.unwrap();

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

        let executor = Executor::new(1);
        let results = executor.to_string_set(plan).await;

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

        let executor = Executor::new(1);
        let results = executor.to_string_set(plan).await;

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

        let executor = Executor::new(1);
        let results = executor.to_string_set(plan).await.expect("Executed plan");

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
