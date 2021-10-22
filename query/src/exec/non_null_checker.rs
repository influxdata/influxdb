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
    any::Any,
    fmt::{self, Debug},
    sync::Arc,
};

use async_trait::async_trait;

use arrow::{
    array::{new_empty_array, StringArray},
    datatypes::{DataType, Field, Schema, SchemaRef},
    error::{ArrowError, Result as ArrowResult},
    record_batch::RecordBatch,
};
use datafusion::{
    error::{DataFusionError as Error, Result},
    logical_plan::{DFSchemaRef, Expr, LogicalPlan, ToDFSchema, UserDefinedLogicalNode},
    physical_plan::{
        metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet},
        DisplayFormatType, Distribution, ExecutionPlan, Partitioning, SendableRecordBatchStream,
        Statistics,
    },
};

use datafusion_util::AdapterStream;
use observability_deps::tracing::debug;
use tokio::sync::mpsc;
use tokio_stream::StreamExt;

/// Implements the NonNullChecker operation as described in this module's documentation
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

impl UserDefinedLogicalNode for NonNullCheckerNode {
    fn as_any(&self) -> &dyn Any {
        self
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
        write!(f, "NonNullChecker('{}')", self.value)
    }

    fn from_template(
        &self,
        exprs: &[Expr],
        inputs: &[LogicalPlan],
    ) -> Arc<dyn UserDefinedLogicalNode + Send + Sync> {
        assert_eq!(inputs.len(), 1, "NonNullChecker: input sizes inconistent");
        assert_eq!(
            exprs.len(),
            self.exprs.len(),
            "NonNullChecker: expression sizes inconistent"
        );
        Arc::new(Self::new(self.value.as_ref(), inputs[0].clone()))
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

#[async_trait]
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

    fn required_child_distribution(&self) -> Distribution {
        Distribution::UnspecifiedDistribution
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![Arc::clone(&self.input)]
    }

    fn with_new_children(
        &self,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        match children.len() {
            1 => Ok(Arc::new(Self {
                input: Arc::clone(&children[0]),
                schema: Arc::clone(&self.schema),
                metrics: ExecutionPlanMetricsSet::new(),
                value: Arc::clone(&self.value),
            })),
            _ => Err(Error::Internal(
                "NonNullCheckerExec wrong number of children".to_string(),
            )),
        }
    }

    /// Execute one partition and return an iterator over RecordBatch
    async fn execute(&self, partition: usize) -> Result<SendableRecordBatchStream> {
        if self.output_partitioning().partition_count() <= partition {
            return Err(Error::Internal(format!(
                "NonNullCheckerExec invalid partition {}",
                partition
            )));
        }

        let baseline_metrics = BaselineMetrics::new(&self.metrics, partition);
        let input_stream = self.input.execute(partition).await?;

        let (tx, rx) = mpsc::channel(1);

        let task = tokio::task::spawn(check_for_nulls(
            input_stream,
            Arc::clone(&self.schema),
            baseline_metrics,
            Arc::clone(&self.value),
            tx.clone(),
        ));

        // A second task watches the output of the worker task (TODO refactor into datafusion_util)
        tokio::task::spawn(async move {
            let task_result = task.await;

            let msg = match task_result {
                Err(join_err) => {
                    debug!(e=%join_err, "Error joining null_check task");
                    Some(ArrowError::ExternalError(Box::new(join_err)))
                }
                Ok(Err(e)) => {
                    debug!(%e, "Error in null_check task itself");
                    Some(e)
                }
                Ok(Ok(())) => {
                    // successful
                    None
                }
            };

            if let Some(e) = msg {
                // try and tell the receiver something went
                // wrong. Note we ignore errors sending this message
                // as that means the receiver has already been
                // shutdown and no one cares anymore lol
                if tx.send(Err(e)).await.is_err() {
                    debug!("null_check receiver hung up");
                }
            }
        });

        Ok(AdapterStream::adapt(self.schema(), rx))
    }

    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match t {
            DisplayFormatType::Default => {
                write!(f, "NonNullCheckerExec")
            }
        }
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn statistics(&self) -> Statistics {
        // don't know anything about the statistics
        Statistics::default()
    }
}

async fn check_for_nulls(
    mut input_stream: SendableRecordBatchStream,
    schema: SchemaRef,
    baseline_metrics: BaselineMetrics,
    value: Arc<str>,
    tx: mpsc::Sender<ArrowResult<RecordBatch>>,
) -> ArrowResult<()> {
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
    use datafusion::physical_plan::{collect, memory::MemoryExec};

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
        let t1 = StringArray::from(vec![None, None, None]);
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
        let t1 = StringArray::from(vec![None, None, None]);
        let t2 = StringArray::from(vec![None, None, Some("c")]);
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
        let t1 = StringArray::from(vec![None, None, None]);
        let t2 = StringArray::from(vec![None, None, None]);
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
        let t1 = StringArray::from(vec![None, None, None]);
        let t2 = StringArray::from(vec![None, None, None]);

        let batch1 = RecordBatch::try_from_iter(vec![
            ("t1", Arc::new(t1) as ArrayRef),
            ("t2", Arc::new(t2) as ArrayRef),
        ])
        .unwrap();

        let t1 = StringArray::from(vec![None]);
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
        let output = collect(Arc::clone(&exec) as Arc<dyn ExecutionPlan>)
            .await
            .unwrap();

        output
    }
}
