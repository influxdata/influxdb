//! This module handles the manipulation / execution of storage
//! plans. This is currently implemented using DataFusion, and this
//! interface abstracts away many of the details
mod planning;
mod schema_pivot;
mod stringset;

use std::{sync::atomic::AtomicU64, sync::atomic::Ordering, sync::Arc};

use delorean_arrow::{
    arrow::record_batch::RecordBatch,
    datafusion::{
        self,
        logical_plan::{Expr, LogicalPlan},
        prelude::ExecutionConfig,
    },
};

use planning::make_exec_context;
use schema_pivot::SchemaPivotNode;
use stringset::{IntoStringSet, StringSetRef};

use tracing::debug;

use snafu::{ResultExt, Snafu};

#[derive(Debug, Snafu)]
/// Opaque error type
pub enum Error {
    #[snafu(display("Plan Execution Error: {}", source))]
    Execution {
        source: Box<dyn std::error::Error + Send + Sync + 'static>,
    },

    #[snafu(display("Internal error optimizing plan: {}", source))]
    DataFusionOptimization {
        source: datafusion::error::ExecutionError,
    },

    #[snafu(display("Internal error during physical planning: {}", source))]
    DataFusionPhysicalPlanning {
        source: datafusion::error::ExecutionError,
    },

    #[snafu(display("Internal error executing plan: {}", source))]
    DataFusionExecution {
        source: datafusion::error::ExecutionError,
    },

    #[snafu(display("Internal error extracting results from Record Batches: {}", message))]
    InternalResultsExtraction { message: String },

    #[snafu(display("Internal error creating StringSet: {}", source))]
    StringSetConversion { source: stringset::Error },

    #[snafu(display("Joining execution task: {}", source))]
    JoinError { source: tokio::task::JoinError },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Represents a general purpose predicate for evaluation.
///
/// TBD can this predicate represent predicates for multiple tables?
#[derive(Clone, Debug)]
pub struct Predicate {
    /// An expresson using the DataFusion expression operations.
    pub expr: Expr,
}

/// A plan which produces a logical set of Strings (e.g. tag
/// values). This includes variants with pre-calculated results as
/// well a variant that runs a full on DataFusion plan.
#[derive(Debug)]
pub enum StringSetPlan {
    // If the results are known without having to run an actual datafusion plan
    Known(Result<StringSetRef>),
    // A datafusion plan(s) to execute. Each plan must produce
    // RecordBatches with exactly one String column
    Plan(Vec<LogicalPlan>),
}

impl<E> From<Result<StringSetRef, E>> for StringSetPlan
where
    E: std::error::Error + Send + Sync + 'static,
{
    /// Create a plan from a known result, wrapping the error type
    /// appropriately
    fn from(result: Result<StringSetRef, E>) -> Self {
        match result {
            Ok(set) => Self::Known(Ok(set)),
            Err(e) => Self::Known(Err(Error::Execution {
                source: Box::new(e),
            })),
        }
    }
}

impl From<Vec<LogicalPlan>> for StringSetPlan {
    /// Create a DataFusion LogicalPlan node, each if which must
    /// produce a single output Utf8 column. The output of each plan
    /// will be included into the final set.
    fn from(plans: Vec<LogicalPlan>) -> Self {
        Self::Plan(plans)
    }
}

/// Handles executing plans, and marshalling the results into rust
/// native structures.
#[derive(Debug, Default)]
pub struct Executor {
    counters: Arc<ExecutionCounters>,
}

impl Executor {
    pub fn new() -> Self {
        Self::default()
    }
    /// Executes this plan and returns the resulting set of strings
    pub async fn to_string_set(&self, plan: StringSetPlan) -> Result<StringSetRef> {
        match plan {
            StringSetPlan::Known(res) => res,
            StringSetPlan::Plan(plans) => run_logical_plans(self.counters.clone(), plans)
                .await?
                .into_stringset()
                .context(StringSetConversion),
        }
    }
}

// Various statistics for execution
#[derive(Debug, Default)]
pub struct ExecutionCounters {
    pub plans_run: AtomicU64,
}

impl ExecutionCounters {
    fn inc_plans_run(&self) {
        self.plans_run.fetch_add(1, Ordering::Relaxed);
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

/// plans and runs the plans in parallel and collects the results
/// run each plan in parallel and collect the results
async fn run_logical_plans(
    counters: Arc<ExecutionCounters>,
    plans: Vec<LogicalPlan>,
) -> Result<Vec<RecordBatch>> {
    let value_futures = plans
        .into_iter()
        .map(|plan| {
            let counters = counters.clone();
            // TODO run these on some executor other than the main tokio pool
            tokio::task::spawn(async move { run_logical_plan(counters, plan).await })
        })
        .collect::<Vec<_>>();

    // now, wait for all the values to resolve and collect them together
    let mut results = Vec::new();
    for join_handle in value_futures.into_iter() {
        let mut plan_result = join_handle.await.context(JoinError)??;

        results.append(&mut plan_result);
    }
    Ok(results)
}

/// Executes the logical plan using DataFusion and produces RecordBatches
async fn run_logical_plan(
    counters: Arc<ExecutionCounters>,
    plan: LogicalPlan,
) -> Result<Vec<RecordBatch>> {
    counters.inc_plans_run();

    const BATCH_SIZE: usize = 1000;

    // TBD: Should we be reusing an execution context across all executions?
    let config = ExecutionConfig::new().with_batch_size(BATCH_SIZE);
    let ctx = make_exec_context(config);

    debug!("Running plan, input:\n{:?}", plan);
    // TODO the datafusion optimizer was removing filters..
    //let logical_plan = ctx.optimize(&plan).context(DataFusionOptimization)?;
    let logical_plan = plan;
    debug!("Running plan, optimized:\n{:?}", logical_plan);

    let physical_plan = ctx
        .create_physical_plan(&logical_plan)
        .context(DataFusionPhysicalPlanning)?;

    debug!("Running plan, physical:\n{:?}", physical_plan);

    // This executes the query, using its own threads
    // internally. TODO figure out a better way to control
    // concurrency / plan admission
    ctx.collect(physical_plan)
        .await
        .context(DataFusionExecution)
}

#[cfg(test)]
mod tests {
    use delorean_arrow::arrow::{
        array::Int64Array,
        array::StringArray,
        array::StringBuilder,
        datatypes::DataType,
        datatypes::{Field, Schema, SchemaRef},
    };
    use stringset::StringSet;

    use super::*;

    #[tokio::test]
    async fn executor_known_string_set_plan_ok() -> Result<()> {
        let expected_strings = to_set(&["Foo", "Bar"]);
        let result: Result<_> = Ok(expected_strings.clone());
        let plan = result.into();

        let executor = Executor::default();
        let result_strings = executor.to_string_set(plan).await?;
        assert_eq!(result_strings, expected_strings);
        Ok(())
    }

    #[tokio::test]
    async fn executor_known_string_set_plan_err() -> Result<()> {
        let result = InternalResultsExtraction {
            message: "this is a test",
        }
        .fail();

        let plan = result.into();

        let executor = Executor::default();
        let actual_result = executor.to_string_set(plan).await;
        assert!(actual_result.is_err());
        assert!(
            format!("{:?}", actual_result).contains("this is a test"),
            "Actual result: '{:?}'",
            actual_result
        );
        Ok(())
    }

    #[tokio::test]
    async fn executor_datafusion_string_set_single_plan_no_batches() -> Result<()> {
        // Test with a single plan that produces no batches
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Utf8, true)]));
        let scan = make_plan(schema, vec![]);
        let plan: StringSetPlan = vec![scan].into();

        let executor = Executor::new();
        let results = executor.to_string_set(plan).await?;

        assert_eq!(results, StringSetRef::new(StringSet::new()));

        Ok(())
    }

    #[tokio::test]
    async fn executor_datafusion_string_set_single_plan_one_batch() -> Result<()> {
        // Test with a single plan that produces one record batch
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Utf8, true)]));
        let data = to_string_array(&["foo", "bar", "baz", "foo"]);
        let batch =
            RecordBatch::try_new(schema.clone(), vec![data]).expect("created new record batch");
        let scan = make_plan(schema, vec![batch]);
        let plan: StringSetPlan = vec![scan].into();

        let executor = Executor::new();
        let results = executor.to_string_set(plan).await?;

        assert_eq!(results, to_set(&["foo", "bar", "baz"]));

        Ok(())
    }

    #[tokio::test]
    async fn executor_datafusion_string_set_single_plan_two_batch() -> Result<()> {
        // Test with a single plan that produces multiple record batches
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Utf8, true)]));
        let data1 = to_string_array(&["foo", "bar"]);
        let batch1 =
            RecordBatch::try_new(schema.clone(), vec![data1]).expect("created new record batch");
        let data2 = to_string_array(&["baz", "foo"]);
        let batch2 =
            RecordBatch::try_new(schema.clone(), vec![data2]).expect("created new record batch");
        let scan = make_plan(schema, vec![batch1, batch2]);
        let plan: StringSetPlan = vec![scan].into();

        let executor = Executor::new();
        let results = executor.to_string_set(plan).await?;

        assert_eq!(results, to_set(&["foo", "bar", "baz"]));

        Ok(())
    }

    #[tokio::test]
    async fn executor_datafusion_string_set_multi_plan() -> Result<()> {
        // Test with multiple datafusion logical plans
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Utf8, true)]));

        let data1 = to_string_array(&["foo", "bar"]);
        let batch1 =
            RecordBatch::try_new(schema.clone(), vec![data1]).expect("created new record batch");
        let scan1 = make_plan(schema.clone(), vec![batch1]);

        let data2 = to_string_array(&["baz", "foo"]);
        let batch2 =
            RecordBatch::try_new(schema.clone(), vec![data2]).expect("created new record batch");
        let scan2 = make_plan(schema, vec![batch2]);

        let plan: StringSetPlan = vec![scan1, scan2].into();

        let executor = Executor::new();
        let results = executor.to_string_set(plan).await?;

        assert_eq!(results, to_set(&["foo", "bar", "baz"]));

        Ok(())
    }

    #[tokio::test]
    async fn executor_datafusion_string_set_nulls() -> Result<()> {
        // Ensure that nulls in the output set are handled reasonably
        // (error, rather than silently ignored)
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Utf8, true)]));
        let mut builder = StringBuilder::new(2);
        builder.append_value("foo").unwrap();
        builder.append_null().unwrap();
        let data = Arc::new(builder.finish());
        let batch =
            RecordBatch::try_new(schema.clone(), vec![data]).expect("created new record batch");
        let scan = make_plan(schema, vec![batch]);
        let plan: StringSetPlan = vec![scan].into();

        let executor = Executor::new();
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

        Ok(())
    }

    #[tokio::test]
    async fn executor_datafusion_string_set_bad_schema() -> Result<()> {
        // Ensure that an incorect schema (an int) gives a reasonable error
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, true)]));
        let data = Arc::new(Int64Array::from(vec![1]));
        let batch =
            RecordBatch::try_new(schema.clone(), vec![data]).expect("created new record batch");
        let scan = make_plan(schema, vec![batch]);
        let plan: StringSetPlan = vec![scan].into();

        let executor = Executor::new();
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

        Ok(())
    }

    #[tokio::test]
    async fn make_schema_pivot_is_planned() -> Result<()> {
        // Test that all the planning logic is wired up and that we
        // can make a plan using a SchemaPivot node
        let schema = Arc::new(Schema::new(vec![
            Field::new("f1", DataType::Utf8, true),
            Field::new("f2", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                to_string_array(&["foo", "bar"]),
                to_string_array(&["baz", "bzz"]),
            ],
        )
        .expect("created new record batch");

        let scan = make_plan(schema, vec![batch]);
        let pivot = make_schema_pivot(scan);
        let plan = vec![pivot].into();

        let executor = Executor::new();
        let results = executor.to_string_set(plan).await.expect("Executed plan");

        assert_eq!(results, to_set(&["f1", "f2"]));

        Ok(())
    }

    /// return a set for testing
    fn to_set(strs: &[&str]) -> StringSetRef {
        StringSetRef::new(strs.iter().map(|s| s.to_string()).collect::<StringSet>())
    }

    fn to_string_array(strs: &[&str]) -> Arc<StringArray> {
        let mut builder = StringBuilder::new(strs.len());
        for s in strs {
            builder.append_value(s).expect("appending string");
        }
        Arc::new(builder.finish())
    }

    // creates a DataFusion plan that reads the RecordBatches into memory
    fn make_plan(schema: SchemaRef, data: Vec<RecordBatch>) -> LogicalPlan {
        let projected_schema = schema.clone();

        LogicalPlan::InMemoryScan {
            data: vec![data], // model one partition
            schema,
            projection: None,
            projected_schema,
        }
    }
}
