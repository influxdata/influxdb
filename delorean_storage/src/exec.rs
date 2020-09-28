//! This module handles the manipulation / execution of storage
//! plans. This is currently implemented using DataFusion, and this
//! interface abstracts away many of the details

use std::{collections::BTreeSet, sync::atomic::AtomicU64, sync::atomic::Ordering, sync::Arc};

use datafusion::prelude::{ExecutionConfig, ExecutionContext};
use delorean_arrow::{
    arrow::{
        array::{Array, StringArray, StringArrayOps},
        datatypes::DataType,
        record_batch::RecordBatch,
    },
    datafusion::{
        self,
        logical_plan::{Expr, LogicalPlan},
    },
};

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
    DataFusionOptimizationError {
        source: datafusion::error::ExecutionError,
    },

    #[snafu(display("Internal error during physical planning: {}", source))]
    DataFusionPhysicalPlanningError {
        source: datafusion::error::ExecutionError,
    },

    #[snafu(display("Internal error executing plan: {}", source))]
    DataFusionExecutionError {
        source: datafusion::error::ExecutionError,
    },

    #[snafu(display("Internal error extracting results from Record Batches: {}", message))]
    InternalResultsExtraction { message: String },

    #[snafu(display("Joining execution task: {}", source))]
    JoinError { source: tokio::task::JoinError },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

pub type StringSet = BTreeSet<String>;
pub type StringSetRef = Arc<StringSet>;

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
                .into_stringset(),
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
            tokio::task::spawn(async move { run_logical_plan(counters, plan) })
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
fn run_logical_plan(
    counters: Arc<ExecutionCounters>,
    plan: LogicalPlan,
) -> Result<Vec<RecordBatch>> {
    counters.inc_plans_run();

    const BATCH_SIZE: usize = 1000;

    // TBD: Should we be reusing an execution context across all executions?
    let config = ExecutionConfig::new().with_batch_size(BATCH_SIZE);
    //let ctx = make_exec_context(config); // TODO (With the next chunk)
    let ctx = ExecutionContext::with_config(config);

    debug!("Running plan, input:\n{:?}", plan);
    // TODO the datafusion optimizer was removing filters..
    //let logical_plan = ctx.optimize(&plan).context(DataFusionOptimizationError)?;
    let logical_plan = plan;
    debug!("Running plan, optimized:\n{:?}", logical_plan);

    let physical_plan = ctx
        .create_physical_plan(&logical_plan)
        .context(DataFusionPhysicalPlanningError)?;

    debug!("Running plan, physical:\n{:?}", physical_plan);

    // This executes the query, using its own threads
    // internally. TODO figure out a better way to control
    // concurrency / plan admission
    ctx.collect(physical_plan).context(DataFusionExecutionError)
}

trait IntoStringSet {
    fn into_stringset(self) -> Result<StringSetRef>;
}

/// Converts record batches into StringSets. Assumes that the record
/// batches each have a single string column
impl IntoStringSet for Vec<RecordBatch> {
    fn into_stringset(self) -> Result<StringSetRef> {
        let mut strings = StringSet::new();

        // process the record batches one by one
        for record_batch in self.into_iter() {
            let num_rows = record_batch.num_rows();
            let schema = record_batch.schema();
            let fields = schema.fields();
            if fields.len() != 1 {
                return InternalResultsExtraction {
                    message: format!(
                        "Expected exactly 1 field in StringSet schema, found {} field in {:?}",
                        fields.len(),
                        schema
                    ),
                }
                .fail();
            }
            let field = &fields[0];

            if *field.data_type() != DataType::Utf8 {
                return InternalResultsExtraction {
                    message: format!(
                        "Expected StringSet schema field to be Utf8, instead it was {:?}",
                        field.data_type()
                    ),
                }
                .fail();
            }

            let array = record_batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>();

            match array {
                Some(array) => add_utf8_array_to_stringset(&mut strings, array, num_rows)?,
                None => {
                    return InternalResultsExtraction {
                        message: format!("Failed to downcast field {:?} to StringArray", field),
                    }
                    .fail()
                }
            }
        }
        Ok(StringSetRef::new(strings))
    }
}

fn add_utf8_array_to_stringset(
    dest: &mut StringSet,
    src: &StringArray,
    num_rows: usize,
) -> Result<()> {
    for i in 0..num_rows {
        // Not sure how to handle a NULL -- StringSet contains
        // Strings, not Option<String>
        if src.is_null(i) {
            return InternalResultsExtraction {
                message: "Unexpected null value",
            }
            .fail();
        } else {
            let src_value = src.value(i);
            if !dest.contains(src_value) {
                dest.insert(src_value.into());
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use delorean_arrow::arrow::{
        array::Int64Array,
        datatypes::{Field, Schema, SchemaRef},
    };

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
        let mut builder = StringArray::builder(2);
        builder.append_value("foo").unwrap();
        builder.append_null().unwrap();
        let data = Arc::new(builder.finish());
        let batch =
            RecordBatch::try_new(schema.clone(), vec![data]).expect("created new record batch");
        let scan = make_plan(schema, vec![batch]);
        let plan: StringSetPlan = vec![scan].into();

        let executor = Executor::new();
        let results = executor.to_string_set(plan).await;

        assert!(results.is_err(), "result is {:?}", results);
        let expected_error = "Unexpected null value";
        assert!(
            format!("{:?}", results).contains(expected_error),
            "expected error '{}' not found in '{:?}'",
            expected_error,
            results
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

        assert!(results.is_err(), "result is {:?}", results);
        let expected_error = "Expected StringSet schema field to be Utf8, instead it was Int64";
        assert!(
            format!("{:?}", results).contains(expected_error),
            "expected error '{}' not found in '{:?}'",
            expected_error,
            results
        );

        Ok(())
    }

    /// return a set for testing
    fn to_set(strs: &[&str]) -> StringSetRef {
        StringSetRef::new(strs.iter().map(|s| s.to_string()).collect::<StringSet>())
    }

    fn to_string_array(strs: &[&str]) -> Arc<StringArray> {
        let mut builder = StringArray::builder(strs.len());
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
