//! This module handles the manipulation / execution of storage
//! plans. This is currently implemented using DataFusion, and this
//! interface abstracts away many of the details
pub(crate) mod context;
mod counters;
pub mod field;
pub mod fieldlist;
mod schema_pivot;
pub mod seriesset;
pub mod stringset;

use std::sync::Arc;

use arrow_deps::{
    arrow::record_batch::RecordBatch,
    datafusion::{self, logical_plan::LogicalPlan},
};
use counters::ExecutionCounters;

use context::IOxExecutionContext;
use field::FieldColumns;
use schema_pivot::SchemaPivotNode;

use fieldlist::{FieldList, IntoFieldList};
use seriesset::{Error as SeriesSetError, SeriesSetConverter, SeriesSetItem};
use stringset::{IntoStringSet, StringSet, StringSetRef};
use tokio::sync::mpsc::{self, error::SendError};

use snafu::{ResultExt, Snafu};

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
    FieldListExectuon {
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
    /// Create a StringSetPlan from a Result<StringSetRef> result, wrapping the
    /// error type appropriately
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

/// A plan that can be run to produce a logical stream of time series,
/// as represented as sequence of SeriesSets from a single DataFusion
/// plan, optionally grouped in some way.
#[derive(Debug)]
pub struct SeriesSetPlan {
    /// The table name this came from
    pub table_name: Arc<String>,

    /// Datafusion plan to execute. The plan must produce
    /// RecordBatches that have:
    ///
    /// * fields for each name in `tag_columns` and `field_columns`
    /// * a timestamp column called 'time'
    /// * each column in tag_columns must be a String (Utf8)
    pub plan: LogicalPlan,

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
    pub field_columns: FieldColumns,

    /// If present, how many of the series_set_plan::tag_columns
    /// should be used to compute the group
    pub num_prefix_tag_group_columns: Option<usize>,
}

impl SeriesSetPlan {
    /// Create a SeriesSetPlan that will not produce any Group items
    pub fn new_from_shared_timestamp(
        table_name: Arc<String>,
        plan: LogicalPlan,
        tag_columns: Vec<Arc<String>>,
        field_columns: Vec<Arc<String>>,
    ) -> Self {
        Self::new(table_name, plan, tag_columns, field_columns.into())
    }

    /// Create a SeriesSetPlan that will not produce any Group items
    pub fn new(
        table_name: Arc<String>,
        plan: LogicalPlan,
        tag_columns: Vec<Arc<String>>,
        field_columns: FieldColumns,
    ) -> Self {
        let num_prefix_tag_group_columns = None;

        Self {
            table_name,
            plan,
            tag_columns,
            field_columns,
            num_prefix_tag_group_columns,
        }
    }

    /// Create a SeriesSetPlan that will produce Group items, according to
    /// num_prefix_tag_group_columns.
    pub fn grouped(mut self, num_prefix_tag_group_columns: usize) -> Self {
        self.num_prefix_tag_group_columns = Some(num_prefix_tag_group_columns);
        self
    }
}

/// A container for plans which each produce a logical stream of
/// timeseries (from across many potential tables).
#[derive(Debug, Default)]
pub struct SeriesSetPlans {
    pub plans: Vec<SeriesSetPlan>,
}

impl From<Vec<SeriesSetPlan>> for SeriesSetPlans {
    fn from(plans: Vec<SeriesSetPlan>) -> Self {
        Self { plans }
    }
}

/// A plan that can be run to produce a sequence of FieldLists
/// DataFusion plans or a known set of results
#[derive(Debug)]
pub enum FieldListPlan {
    Known(Result<FieldList>),
    Plans(Vec<LogicalPlan>),
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
            StringSetPlan::Plan(plans) => self
                .run_logical_plans(plans)
                .await?
                .into_stringset()
                .context(StringSetConversion),
        }
    }

    /// Executes the embedded plans, each as separate tasks, sending
    /// the resulting `SeriesSet`s one by one to the `tx` chanel.
    ///
    /// The SeriesSets are guaranteed to come back ordered by table_name
    ///
    /// Note that the returned future resolves (e.g. "returns") once
    /// all plans have been sent to `tx`. This means that the future
    /// will not resolve if there is nothing hooked up receiving
    /// results from the other end of the channel and the channel
    /// can't hold all the resulting series.
    pub async fn to_series_set(
        &self,
        series_set_plans: SeriesSetPlans,
        mut tx: mpsc::Sender<Result<SeriesSetItem, SeriesSetError>>,
    ) -> Result<()> {
        let SeriesSetPlans { mut plans } = series_set_plans;

        if plans.is_empty() {
            return Ok(());
        }

        // sort by table name and send the results to separate
        // channels
        plans.sort_by(|a, b| a.table_name.cmp(&b.table_name));
        let mut rx_channels = Vec::new(); // sorted by table names

        // Run the plans in parallel
        let handles = plans
            .into_iter()
            .map(|plan| {
                // TODO run these on some executor other than the main tokio pool (maybe?)
                let ctx = self.new_context();
                let (plan_tx, plan_rx) = mpsc::channel(1);
                rx_channels.push(plan_rx);

                tokio::task::spawn(async move {
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
                        .await
                        .context(DataFusionPhysicalPlanning)?;

                    let it = ctx
                        .execute(physical_plan)
                        .await
                        .context(SeriesSetExecution)?;

                    SeriesSetConverter::new(plan_tx)
                        .convert(
                            table_name,
                            tag_columns,
                            field_columns,
                            num_prefix_tag_group_columns,
                            it,
                        )
                        .await
                        .context(SeriesSetConversion)?;

                    Ok(())
                })
            })
            .collect::<Vec<_>>();

        // transfer data from the rx streams in order
        for mut rx in rx_channels {
            while let Some(r) = rx.recv().await {
                tx.send(r)
                    .await
                    .map_err(|e| Error::SendingDuringConversion {
                        source: Box::new(e),
                    })?
            }
        }

        // now, wait for all the values to resolve so we can report
        // any errors
        for join_handle in handles {
            join_handle.await.context(JoinError)??;
        }
        Ok(())
    }

    /// Executes `plan` and return the resulting FieldList
    pub async fn to_fieldlist(&self, plan: FieldListPlan) -> Result<FieldList> {
        match plan {
            FieldListPlan::Known(res) => res,
            FieldListPlan::Plans(plans) => {
                // Run the plans in parallel
                let handles = plans
                    .into_iter()
                    .map(|plan| {
                        let counters = self.counters.clone();

                        tokio::task::spawn(async move {
                            let ctx = IOxExecutionContext::new(counters);
                            let physical_plan = ctx
                                .prepare_plan(&plan)
                                .await
                                .context(DataFusionPhysicalPlanning)?;

                            // TODO: avoid this buffering
                            let fieldlist = ctx
                                .collect(physical_plan)
                                .await
                                .context(FieldListExectuon)?
                                .into_fieldlist()
                                .context(FieldListConversion);

                            Ok(fieldlist)
                        })
                    })
                    .collect::<Vec<_>>();

                // collect them all up and combine them
                let mut results = Vec::new();
                for join_handle in handles {
                    let fieldlist = join_handle.await.context(JoinError)???;

                    results.push(fieldlist);
                }

                results.into_fieldlist().context(FieldListConversion)
            }
        }
    }

    /// Run the plan and return a record batch reader for reading the results
    pub async fn run_logical_plan(&self, plan: LogicalPlan) -> Result<Vec<RecordBatch>> {
        self.run_logical_plans(vec![plan]).await
    }

    /// Create a new execution context, suitable for executing a new query
    pub fn new_context(&self) -> IOxExecutionContext {
        IOxExecutionContext::new(self.counters.clone())
    }

    /// plans and runs the plans in parallel and collects the results
    /// run each plan in parallel and collect the results
    async fn run_logical_plans(&self, plans: Vec<LogicalPlan>) -> Result<Vec<RecordBatch>> {
        let value_futures = plans
            .into_iter()
            .map(|plan| {
                let ctx = self.new_context();
                // TODO run these on some executor other than the main tokio pool
                tokio::task::spawn(async move {
                    let physical_plan = ctx.prepare_plan(&plan).await.expect("making logical plan");

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
            let mut plan_result = join_handle.await.context(JoinError)??;
            results.append(&mut plan_result);
        }
        Ok(results)
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

#[cfg(test)]
mod tests {
    use arrow_deps::{
        arrow::{
            array::Int64Array,
            array::StringArray,
            array::StringBuilder,
            datatypes::DataType,
            datatypes::{Field, Schema, SchemaRef},
        },
        datafusion::logical_plan::LogicalPlanBuilder,
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
