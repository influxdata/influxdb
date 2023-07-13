//! This module contains code for the "NonNullChecker" DataFusion
//! extension plan node
//!
//! A NonNullChecker node takes an arbitrary input array and produces
//! a single string output column that contains
//!
//! 1. A single string if any of the input columns are non-null
//! 2. zero rows if all of the input columns are null
//!
//! For this input:
//!
//!  ColA | ColB | ColC
//! ------+------+------
//!   1   | NULL | NULL
//!   2   | 2    | NULL
//!   3   | 2    | NULL
//!
//! The output would be (given 'the_value' was provided to `NonNullChecker` node)
//!
//!   non_null_column
//!  -----------------
//!   the_value
//!
//! However, for this input (All NULL)
//!
//!  ColA | ColB | ColC
//! ------+------+------
//!  NULL | NULL | NULL
//!  NULL | NULL | NULL
//!  NULL | NULL | NULL
//!
//! There would be no output rows
//!
//!   non_null_column
//!  -----------------
//!
//! This operation can be used to implement the table_name metadata query

use std::{
    fmt::{self, Debug},
    sync::Arc,
};

use arrow::{
    array::{new_empty_array, StringArray},
    datatypes::{DataType, Field, Schema, SchemaRef},
    record_batch::RecordBatch,
};
use datafusion::{
    common::{DFSchemaRef, ToDFSchema},
    error::{DataFusionError, Result},
    execution::context::TaskContext,
    logical_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore},
    physical_plan::{
        expressions::PhysicalSortExpr,
        metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet},
        DisplayAs, DisplayFormatType, Distribution, ExecutionPlan, Partitioning,
        SendableRecordBatchStream, Statistics,
    },
};

use datafusion_util::{watch::WatchedTask, AdapterStream};
use observability_deps::tracing::debug;
use tokio::sync::mpsc;
use tokio_stream::StreamExt;

/// Implements the NonNullChecker operation as described in this module's documentation
#[derive(Hash, PartialEq, Eq)]
pub struct NonNullCheckerNode {
    input: LogicalPlan,
    schema: DFSchemaRef,
    /// these expressions represent what columns are "used" by this
    /// node (in this case all of them) -- columns that are not used
    /// are optimzied away by datafusion.
    exprs: Vec<Expr>,

    /// The value to produce if there are any non null Inputs
    value: Arc<str>,
}

impl NonNullCheckerNode {
    pub fn new(value: &str, input: LogicalPlan) -> Self {
        let schema = make_non_null_checker_output_schema();

        // Form exprs that refer to all of our input columns (so that
        // datafusion knows not to opimize them away)
        let exprs = input
            .schema()
            .fields()
            .iter()
            .map(|field| Expr::Column(field.qualified_column()))
            .collect::<Vec<_>>();

        Self {
            input,
            schema,
            exprs,
            value: value.into(),
        }
    }

    /// Return the value associated with this checker
    pub fn value(&self) -> Arc<str> {
        Arc::clone(&self.value)
    }
}

impl Debug for NonNullCheckerNode {
    /// Use explain format for the Debug format.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.fmt_for_explain(f)
    }
}

impl UserDefinedLogicalNodeCore for NonNullCheckerNode {
    fn name(&self) -> &str {
        "NonNullChecker"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    /// Schema for Pivot is a single string
    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        self.exprs.clone()
    }

    /// For example: `NonNullChecker('the_value')`
    fn fmt_for_explain(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}('{}')", self.name(), self.value)
    }

    fn from_template(&self, exprs: &[Expr], inputs: &[LogicalPlan]) -> Self {
        assert_eq!(inputs.len(), 1, "NonNullChecker: input sizes inconistent");
        assert_eq!(
            exprs.len(),
            self.exprs.len(),
            "NonNullChecker: expression sizes inconistent"
        );
        Self::new(self.value.as_ref(), inputs[0].clone())
    }
}

// ------ The implementation of NonNullChecker code follows -----

/// Create the schema describing the output
pub fn make_non_null_checker_output_schema() -> DFSchemaRef {
    let nullable = false;
    Schema::new(vec![Field::new(
        "non_null_column",
        DataType::Utf8,
        nullable,
    )])
    .to_dfschema_ref()
    .unwrap()
}

/// Physical operator that implements the NonNullChecker operation aginst
/// data types
pub struct NonNullCheckerExec {
    input: Arc<dyn ExecutionPlan>,
    /// Output schema
    schema: SchemaRef,
    /// The value to produce if there are any non null Inputs
    value: Arc<str>,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
}

impl NonNullCheckerExec {
    pub fn new(input: Arc<dyn ExecutionPlan>, schema: SchemaRef, value: Arc<str>) -> Self {
        Self {
            input,
            schema,
            value,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }
}

impl Debug for NonNullCheckerExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "NonNullCheckerExec")
    }
}

impl ExecutionPlan for NonNullCheckerExec {
    fn as_any(&self) -> &(dyn std::any::Any + 'static) {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn output_partitioning(&self) -> Partitioning {
        use Partitioning::*;
        match self.input.output_partitioning() {
            RoundRobinBatch(num_partitions) => RoundRobinBatch(num_partitions),
            // as this node transforms the output schema,  whatever partitioning
            // was present on the input is lost on the output
            Hash(_, num_partitions) => UnknownPartitioning(num_partitions),
            UnknownPartitioning(num_partitions) => UnknownPartitioning(num_partitions),
        }
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![Distribution::UnspecifiedDistribution]
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![Arc::clone(&self.input)]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        match children.len() {
            1 => Ok(Arc::new(Self {
                input: Arc::clone(&children[0]),
                schema: Arc::clone(&self.schema),
                metrics: ExecutionPlanMetricsSet::new(),
                value: Arc::clone(&self.value),
            })),
            _ => Err(DataFusionError::Internal(
                "NonNullCheckerExec wrong number of children".to_string(),
            )),
        }
    }

    /// Execute one partition and return an iterator over RecordBatch
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        debug!(partition, "Start NonNullCheckerExec::execute");
        if self.output_partitioning().partition_count() <= partition {
            return Err(DataFusionError::Internal(format!(
                "NonNullCheckerExec invalid partition {partition}"
            )));
        }

        let baseline_metrics = BaselineMetrics::new(&self.metrics, partition);
        let input_stream = self.input.execute(partition, context)?;

        let (tx, rx) = mpsc::channel(1);

        let fut = check_for_nulls(
            input_stream,
            Arc::clone(&self.schema),
            baseline_metrics,
            Arc::clone(&self.value),
            tx.clone(),
        );

        // A second task watches the output of the worker task and
        // reports errors
        let handle = WatchedTask::new(fut, vec![tx], "non_null_checker");

        debug!(partition, "End NonNullCheckerExec::execute");
        Ok(AdapterStream::adapt(self.schema(), rx, handle))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn statistics(&self) -> Statistics {
        // don't know anything about the statistics
        Statistics::default()
    }
}

impl DisplayAs for NonNullCheckerExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "NonNullCheckerExec")
            }
        }
    }
}

async fn check_for_nulls(
    mut input_stream: SendableRecordBatchStream,
    schema: SchemaRef,
    baseline_metrics: BaselineMetrics,
    value: Arc<str>,
    tx: mpsc::Sender<Result<RecordBatch, DataFusionError>>,
) -> Result<(), DataFusionError> {
    while let Some(input_batch) = input_stream.next().await.transpose()? {
        let timer = baseline_metrics.elapsed_compute().timer();

        if input_batch
            .columns()
            .iter()
            .any(|arr| arr.null_count() != arr.len())
        {
            // found a non null in input, return value
            let arr: StringArray = vec![Some(value.as_ref())].into();

            let output_batch = RecordBatch::try_new(schema, vec![Arc::new(arr)])?;
            // ignore errors on sending (means receiver hung up)
            std::mem::drop(timer);
            tx.send(Ok(output_batch)).await.ok();
            return Ok(());
        }
        // else keep looking
    }
    // if we got here, did not see any non null values. So
    // send back an empty record batch
    let output_batch = RecordBatch::try_new(schema, vec![new_empty_array(&DataType::Utf8)])?;

    // ignore errors on sending (means receiver hung up)
    tx.send(Ok(output_batch)).await.ok();
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{ArrayRef, StringArray};
    use arrow_util::assert_batches_eq;
    use datafusion::physical_plan::memory::MemoryExec;
    use datafusion_util::test_collect;

    #[tokio::test]
    async fn test_single_column_non_null() {
        let t1 = StringArray::from(vec![Some("a"), Some("c"), Some("c")]);
        let batch = RecordBatch::try_from_iter(vec![("t1", Arc::new(t1) as ArrayRef)]).unwrap();

        let results = check("the_value", vec![batch]).await;

        let expected = vec![
            "+-----------------+",
            "| non_null_column |",
            "+-----------------+",
            "| the_value       |",
            "+-----------------+",
        ];
        assert_batches_eq!(&expected, &results);
    }

    #[tokio::test]
    async fn test_single_column_null() {
        let t1 = StringArray::from(vec![None::<&str>, None, None]);
        let batch = RecordBatch::try_from_iter(vec![("t1", Arc::new(t1) as ArrayRef)]).unwrap();

        let results = check("the_value", vec![batch]).await;

        let expected = vec![
            "+-----------------+",
            "| non_null_column |",
            "+-----------------+",
            "+-----------------+",
        ];
        assert_batches_eq!(&expected, &results);
    }

    #[tokio::test]
    async fn test_multi_column_non_null() {
        let t1 = StringArray::from(vec![None::<&str>, None, None]);
        let t2 = StringArray::from(vec![None::<&str>, None, Some("c")]);
        let batch = RecordBatch::try_from_iter(vec![
            ("t1", Arc::new(t1) as ArrayRef),
            ("t2", Arc::new(t2) as ArrayRef),
        ])
        .unwrap();

        let results = check("the_value", vec![batch]).await;

        let expected = vec![
            "+-----------------+",
            "| non_null_column |",
            "+-----------------+",
            "| the_value       |",
            "+-----------------+",
        ];
        assert_batches_eq!(&expected, &results);
    }

    #[tokio::test]
    async fn test_multi_column_null() {
        let t1 = StringArray::from(vec![None::<&str>, None, None]);
        let t2 = StringArray::from(vec![None::<&str>, None, None]);
        let batch = RecordBatch::try_from_iter(vec![
            ("t1", Arc::new(t1) as ArrayRef),
            ("t2", Arc::new(t2) as ArrayRef),
        ])
        .unwrap();

        let results = check("the_value", vec![batch]).await;

        let expected = vec![
            "+-----------------+",
            "| non_null_column |",
            "+-----------------+",
            "+-----------------+",
        ];
        assert_batches_eq!(&expected, &results);
    }

    #[tokio::test]
    async fn test_multi_column_second_batch_non_null() {
        // this time only the second batch has a non null value
        let t1 = StringArray::from(vec![None::<&str>, None, None]);
        let t2 = StringArray::from(vec![None::<&str>, None, None]);

        let batch1 = RecordBatch::try_from_iter(vec![
            ("t1", Arc::new(t1) as ArrayRef),
            ("t2", Arc::new(t2) as ArrayRef),
        ])
        .unwrap();

        let t1 = StringArray::from(vec![None::<&str>]);
        let t2 = StringArray::from(vec![Some("f")]);

        let batch2 = RecordBatch::try_from_iter(vec![
            ("t1", Arc::new(t1) as ArrayRef),
            ("t2", Arc::new(t2) as ArrayRef),
        ])
        .unwrap();

        let results = check("another_value", vec![batch1, batch2]).await;

        let expected = vec![
            "+-----------------+",
            "| non_null_column |",
            "+-----------------+",
            "| another_value   |",
            "+-----------------+",
        ];
        assert_batches_eq!(&expected, &results);
    }

    /// Run the input through the checker and return results
    async fn check(value: &str, input: Vec<RecordBatch>) -> Vec<RecordBatch> {
        test_helpers::maybe_start_logging();

        // Setup in memory stream
        let schema = input[0].schema();
        let projection = None;
        let input = Arc::new(MemoryExec::try_new(&[input], schema, projection).unwrap());

        // Create and run the checker
        let schema: Schema = make_non_null_checker_output_schema().as_ref().into();
        let exec = Arc::new(NonNullCheckerExec::new(
            input,
            Arc::new(schema),
            value.into(),
        ));

        test_collect(exec as Arc<dyn ExecutionPlan>).await
    }
}
