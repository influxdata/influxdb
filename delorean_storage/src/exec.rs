//! This module handles the manipulation / execution of storage
//! plans. This is currently implemented using DataFusion, and this
//! interface abstracts away many of the details
mod counters;
mod planning;
mod schema_pivot;
mod seriesset;
mod stringset;

use std::sync::Arc;

use counters::ExecutionCounters;
use delorean_arrow::{
    arrow::record_batch::RecordBatch,
    datafusion::{self, logical_plan::LogicalPlan},
};

use planning::DeloreanExecutionContext;
use schema_pivot::SchemaPivotNode;

// Publically export StringSets
pub use seriesset::{Error as SeriesSetError, SeriesSet, SeriesSetConverter, SeriesSetRef};
pub use stringset::{IntoStringSet, StringSet, StringSetRef};
use tokio::sync::mpsc;

use snafu::{ResultExt, Snafu};

#[derive(Debug, Snafu)]
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

    #[snafu(display("Error converting results to SeriesSet: {}", source))]
    SeriesSetConversion { source: seriesset::Error },

    #[snafu(display("Joining execution task: {}", source))]
    JoinError { source: tokio::task::JoinError },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

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

impl From<StringSetRef> for StringSetPlan {
    /// Create a StringSetPlan from a StringSetRef
    fn from(set: StringSetRef) -> Self {
        Self::Known(Ok(set))
    }
}

impl From<StringSet> for StringSetPlan {
    /// Create a StringSetPlan from a StringSet result, wrapping the error type
    /// appropriately
    fn from(set: StringSet) -> Self {
        Self::Known(Ok(StringSetRef::new(set)))
    }
}

impl<E> From<Result<StringSetRef, E>> for StringSetPlan
where
    E: std::error::Error + Send + Sync + 'static,
{
    /// Create a StringSetPlan from a Result<StringSetRef> result, wrapping the error type
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

/// A plan which produces a logical stream of time series, and can be
/// executed to produce `SeriesSet`s.
#[derive(Debug)]
pub struct SeriesSetPlan {
    /// Datafusion plan(s) to execute. Each plan must produce
    /// RecordBatches that have:
    ///
    /// * fields with matching names for each value of `tag_columns` and `field_columns`
    /// * include the timestamp column
    /// * each column named in tag_columns must be a String (Utf8)
    ///
    /// The plans are provided as tuples of "(Table Name, LogicalPlan)";
    pub plans: Vec<(Arc<String>, LogicalPlan)>,

    /// The names of the columns that define tags.
    ///
    /// Note these are `Arc` strings because they are duplicated for
    /// *each* resulting `SeriesSet` that is produced when this type
    /// of plan is executed.
    pub tag_columns: Vec<Arc<String>>,

    /// The names of the columns which are "fields"
    ///
    /// Note these are `Arc` strings because they are duplicated for
    /// *each* resulting `SeriesSet` that is produced when this type
    /// of plan is executed.
    pub field_columns: Vec<Arc<String>>,
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

    /// Executes this plan, sending the resulting `SeriesSet`s one by one
    /// via `tx`
    pub async fn to_series_set(
        &self,
        series_set_plan: SeriesSetPlan,
        tx: mpsc::Sender<Result<SeriesSet, SeriesSetError>>,
    ) -> Result<()> {
        if series_set_plan.plans.is_empty() {
            return Ok(());
        }

        let SeriesSetPlan {
            plans,
            tag_columns,
            field_columns,
        } = series_set_plan;

        // wrap them in Arcs so we can share them among threads
        let tag_columns = Arc::new(tag_columns);
        let field_columns = Arc::new(field_columns);

        let value_futures = plans
            .into_iter()
            .map(|(table_name, plan)| {
                // Clone Arc's for transmission to threads
                let counters = self.counters.clone();
                let tx = tx.clone();
                let tag_columns = tag_columns.clone();
                let field_columns = field_columns.clone();

                // TODO run these on some executor other than the main tokio pool
                tokio::task::spawn(async move {
                    let ctx = DeloreanExecutionContext::new(counters);
                    let physical_plan = ctx
                        .make_plan(&plan)
                        .await
                        .context(DataFusionPhysicalPlanning)?;

                    let it = ctx
                        .execute(physical_plan)
                        .await
                        .context(DataFusionExecution)?;

                    SeriesSetConverter::new(tx)
                        .convert(table_name, tag_columns, field_columns, it)
                        .await
                        .context(SeriesSetConversion)
                })
            })
            .collect::<Vec<_>>();

        // now, wait for all the values to resolve and reprot any errors
        for join_handle in value_futures.into_iter() {
            join_handle.await.context(JoinError)??;
        }
        Ok(())
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
            tokio::task::spawn(async move {
                let ctx = DeloreanExecutionContext::new(counters);
                let physical_plan = ctx
                    .make_plan(&plan)
                    .await
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
    for join_handle in value_futures.into_iter() {
        let mut plan_result = join_handle.await.context(JoinError)??;
        results.append(&mut plan_result);
    }
    Ok(results)
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
