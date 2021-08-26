//! This module contains a DataFusion extension node to "split" schemas

use std::{
    any::Any,
    fmt::{self, Debug},
    sync::Arc,
};

use async_trait::async_trait;

use arrow::{
    array::{Array, ArrayRef, BooleanArray},
    compute::{self, filter_record_batch},
    datatypes::SchemaRef,
    error::Result as ArrowResult,
    record_batch::RecordBatch,
};
use datafusion::{
    error::{DataFusionError, Result},
    logical_plan::{DFSchemaRef, Expr, LogicalPlan, UserDefinedLogicalNode},
    physical_plan::{
        metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet, RecordOutput},
        ColumnarValue, DisplayFormatType, Distribution, ExecutionPlan, Partitioning, PhysicalExpr,
        SendableRecordBatchStream,
    },
    scalar::ScalarValue,
};

use futures::StreamExt;
use observability_deps::tracing::{debug, trace};
use tokio::sync::{mpsc::Sender, Mutex};

use crate::exec::stream::AdapterStream;

/// Implements stream splitting described in `make_stream_split`
///
/// The resulting execution plan always produces exactly 2 partitions:
///
/// * partition 0 are the rows for which the split_expr evaluates to true
/// * partition 1 are the rows for which the split_expr does not evaluate to true (e.g. Null or false)
pub struct StreamSplitNode {
    input: LogicalPlan,
    split_expr: Expr,
}

impl StreamSplitNode {
    /// Create a new `StreamSplitNode` using `split_expr` to divide the
    /// rows. `split_expr` must evaluate to a boolean otherwise a
    /// runtime error will occur.
    pub fn new(input: LogicalPlan, split_expr: Expr) -> Self {
        Self { input, split_expr }
    }

    pub fn split_expr(&self) -> &Expr {
        &self.split_expr
    }
}

impl Debug for StreamSplitNode {
    /// Use explain format for the Debug format.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.fmt_for_explain(f)
    }
}

impl UserDefinedLogicalNode for StreamSplitNode {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    /// Schema is the same as the input schema
    fn schema(&self) -> &DFSchemaRef {
        self.input.schema()
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![self.split_expr.clone()]
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "StreamSplit split_expr={:?}", self.split_expr)
    }

    fn from_template(
        &self,
        exprs: &[Expr],
        inputs: &[LogicalPlan],
    ) -> Arc<dyn UserDefinedLogicalNode + Send + Sync> {
        assert_eq!(inputs.len(), 1, "StreamSplitNode: input sizes inconistent");
        assert_eq!(
            exprs.len(),
            1,
            "StreamSplitNode: expression sizes inconistent"
        );
        Arc::new(Self {
            input: inputs[0].clone(),
            split_expr: exprs[0].clone(),
        })
    }
}

/// Tracks the state of the physical operator
enum State {
    New,
    Running {
        /// Stream for which split_expr is true. Set to `None` after
        /// execute(0) is called
        stream0: Option<SendableRecordBatchStream>,
        /// Stream for which split_expr is not true (false or NONE).
        /// Set to `None` after execute(0) is called
        stream1: Option<SendableRecordBatchStream>,
    },
}

/// Physical operator that implements steam splitting operation
pub struct StreamSplitExec {
    state: Mutex<State>,
    input: Arc<dyn ExecutionPlan>,
    split_expr: Arc<dyn PhysicalExpr>,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
}

impl StreamSplitExec {
    pub fn new(input: Arc<dyn ExecutionPlan>, split_expr: Arc<dyn PhysicalExpr>) -> Self {
        let state = Mutex::new(State::New);
        Self {
            state,
            input,
            split_expr,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }
}

impl Debug for StreamSplitExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "StreamSplitExec {:?}", self.split_expr)
    }
}

#[async_trait]
impl ExecutionPlan for StreamSplitExec {
    fn as_any(&self) -> &(dyn std::any::Any + 'static) {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.input.schema()
    }

    /// Always produces exactly two outputs
    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(2)
    }

    /// Always require a single input (eventually we might imagine
    /// running this on multiple partitions concurrently to compute
    /// the splits in parallel, but not now)
    fn required_child_distribution(&self) -> Distribution {
        Distribution::SinglePartition
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![Arc::clone(&self.input)]
    }

    fn with_new_children(
        &self,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        match children.len() {
            1 => Ok(Arc::new(Self::new(
                Arc::clone(&children[0]),
                Arc::clone(&self.split_expr),
            ))),
            _ => Err(DataFusionError::Internal(
                "StreamSplitExec wrong number of children".to_string(),
            )),
        }
    }

    /// Stream split has two partitions:
    ///
    /// * partition 0 are the rows for which the split_expr evaluates to true
    /// * partition 1 are the rows for which the split_expr does not evaluate to true (e.g. Null or false)
    async fn execute(&self, partition: usize) -> Result<SendableRecordBatchStream> {
        trace!(partition, "SplitExec::execute");
        self.start_if_needed().await?;

        let mut state = self.state.lock().await;
        match &mut (*state) {
            State::New => panic!("should have been initialized"),
            State::Running { stream0, stream1 } => {
                let stream = match partition {
                    0 => stream0.take().expect("execute previously called with 0"),
                    1 => stream1.take().expect("execute previously called with 1"),
                    _ => panic!(
                        "Only partition 0 or partition 1 are valid. Got {}",
                        partition
                    ),
                };
                Ok(stream)
            }
        }
    }

    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match t {
            DisplayFormatType::Default => {
                write!(f, "StreamSplitExec")
            }
        }
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

impl StreamSplitExec {
    /// if in State::New, sets up the output running and sets self.state --> `Running`
    async fn start_if_needed(&self) -> Result<()> {
        let mut state = self.state.lock().await;
        if matches!(*state, State::Running { .. }) {
            return Ok(());
        }

        let num_input_streams = self.input.output_partitioning().partition_count();
        assert_eq!(
            num_input_streams, 1,
            "need exactly one input partition for stream split exec"
        );

        let baseline_metrics0 = BaselineMetrics::new(&self.metrics, 0);
        let baseline_metrics1 = BaselineMetrics::new(&self.metrics, 1);

        trace!("Setting up SplitStreamExec state");

        let input_stream = self.input.execute(0).await?;
        let (tx0, rx0) = tokio::sync::mpsc::channel(2);
        let (tx1, rx1) = tokio::sync::mpsc::channel(2);
        let split_expr = Arc::clone(&self.split_expr);

        // launch the work on a different task, with a task to handle its output values
        tokio::task::spawn(async move {
            // wait for completion, and propagate errors
            // note we ignore errors on send (.ok) as that means the receiver has already shutdown.
            let worker = tokio::task::spawn(split_the_stream(
                input_stream,
                split_expr,
                tx0.clone(),
                tx1.clone(),
                baseline_metrics0,
                baseline_metrics1,
            ));

            let txs = [tx0, tx1];
            match worker.await {
                Err(e) => {
                    debug!(%e, "error joining task");
                    for tx in &txs {
                        let err = DataFusionError::Execution(format!("Join Error: {}", e));
                        let err = Err(err.into_arrow_external_error());
                        tx.send(err).await.ok();
                    }
                }
                Ok(Err(e)) => {
                    debug!(%e, "error in work function");
                    for tx in &txs {
                        let err = DataFusionError::Execution(e.to_string());
                        let err = Err(err.into_arrow_external_error());
                        tx.send(err).await.ok();
                    }
                }
                // Input task completed successfully
                Ok(Ok(())) => {
                    trace!("All input tasks completed successfully");
                }
            }
        });

        *state = State::Running {
            stream0: Some(AdapterStream::adapt(self.input.schema(), rx0)),
            stream1: Some(AdapterStream::adapt(self.input.schema(), rx1)),
        };
        Ok(())
    }
}

/// This function does the actual splitting: evaluates `split_expr` on
/// each input [`RecordBatch`], and then sends the rows to the correct
/// output: tx0 (when split_expr is true) and tx0 otherwise
async fn split_the_stream(
    mut input_stream: SendableRecordBatchStream,
    split_expr: Arc<dyn PhysicalExpr>,
    tx0: Sender<ArrowResult<RecordBatch>>,
    tx1: Sender<ArrowResult<RecordBatch>>,
    baseline_metrics0: BaselineMetrics,
    baseline_metrics1: BaselineMetrics,
) -> Result<()> {
    let elapsed_compute0 = baseline_metrics0.elapsed_compute();
    let elapsed_compute1 = baseline_metrics1.elapsed_compute();
    while let Some(batch) = input_stream.next().await {
        // charge initial evaluation to output 0 (not fair, but I
        // can't think of anything else that makes this reasonable)
        let timer = elapsed_compute0.timer();

        let batch = batch?;
        trace!(num_rows = batch.num_rows(), "Processing batch");
        let mut tx0_done = false;
        let mut tx1_done = false;

        let true_indices = split_expr.evaluate(&batch)?;
        let not_true_indices = negate(&true_indices)?;
        timer.done();

        let (true_batch, not_true_batch) = match (true_indices, not_true_indices) {
            (ColumnarValue::Array(true_indices), ColumnarValue::Array(not_true_indices)) => {
                let timer = elapsed_compute0.timer();
                let true_indices = true_indices
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .unwrap();
                let true_batch = filter_record_batch(&batch, true_indices)?;
                timer.done();

                let timer = elapsed_compute1.timer();
                let not_true_indices = not_true_indices
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .unwrap();

                let not_true_batch = if not_true_indices.null_count() > 0 {
                    // since !Null --> Null, but we want all the
                    // remaining rows, that are not in true_indicies,
                    // transform any nulls into true for this one
                    let mapped_indicies = not_true_indices
                        .iter()
                        .map(|v| v.or(Some(true)))
                        .collect::<Vec<_>>();

                    filter_record_batch(&batch, &BooleanArray::from(mapped_indicies))
                } else {
                    filter_record_batch(&batch, not_true_indices)
                }?;
                timer.done();

                (true_batch, not_true_batch)
            }
            // handle when the user provided a constant predicate
            (
                ColumnarValue::Scalar(ScalarValue::Boolean(val0)),
                ColumnarValue::Scalar(ScalarValue::Boolean(val1)),
            ) => {
                let empty_record_batch = RecordBatch::new_empty(batch.schema());
                match (val0, val1) {
                    (Some(true), Some(false)) => (batch, empty_record_batch),
                    (Some(false), Some(true)) => (empty_record_batch, batch),
                    _ => panic!("mismatched boolean values: {:?}, {:?}", val0, val1),
                }
            }
            _ => {
                panic!("mismatched array types");
            }
        };

        // record output counts
        let true_batch = true_batch.record_output(&baseline_metrics0);
        let not_true_batch = not_true_batch.record_output(&baseline_metrics1);

        // don't treat a hangup as an error, as it can also be caused
        // by a LIMIT operation where the entire stream is not
        // consumed)
        if let Err(e) = tx0.send(Ok(true_batch)).await {
            debug!(%e, "Split tx0 hung up, ignoring");
            tx0_done = true;
        }
        if let Err(e) = tx1.send(Ok(not_true_batch)).await {
            debug!(%e, "Split tx1 hung up, ignoring");
            tx1_done = true;
        }

        if tx0_done && tx1_done {
            debug!("Split both tx ends have hung up, stopping loop");
            return Ok(());
        }
    }

    trace!("Splitting done successfully");
    Ok(())
}

/// compute the boolean compliment of the columnar value (which must be boolean)
fn negate(v: &ColumnarValue) -> Result<ColumnarValue> {
    match v {
        ColumnarValue::Array(arr) => {
            let arr = arr.as_any().downcast_ref::<BooleanArray>().ok_or_else(|| {
                let msg = format!("Expected boolean array, but had type {:?}", arr.data_type());
                DataFusionError::Internal(msg)
            })?;
            let neg_array = Arc::new(compute::not(arr)?) as ArrayRef;
            Ok(ColumnarValue::Array(neg_array))
        }
        ColumnarValue::Scalar(val) => {
            if let ScalarValue::Boolean(v) = val {
                let not_v = v.map(|v| !v);
                Ok(ColumnarValue::Scalar(ScalarValue::Boolean(not_v)))
            } else {
                let msg = format!(
                    "Expected boolean literal, but got type {:?}",
                    val.get_datatype()
                );
                Err(DataFusionError::Internal(msg))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::convert::TryInto;

    use arrow::array::{Int64Array, StringArray};
    use arrow_util::assert_batches_sorted_eq;
    use datafusion::{
        logical_plan::{col, lit, DFSchema},
        physical_plan::{memory::MemoryExec, planner::DefaultPhysicalPlanner},
    };

    use super::*;

    #[tokio::test]
    async fn test_basic_split() {
        test_helpers::maybe_start_logging();
        let batch0 = RecordBatch::try_from_iter(vec![
            (
                "int_col",
                Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef,
            ),
            (
                "str_col",
                Arc::new(StringArray::from(vec!["one", "two", "three"])) as ArrayRef,
            ),
        ])
        .unwrap();

        let batch1 = RecordBatch::try_from_iter(vec![
            (
                "int_col",
                Arc::new(Int64Array::from(vec![4, -2])) as ArrayRef,
            ),
            (
                "str_col",
                Arc::new(StringArray::from(vec!["four", "negative 2"])) as ArrayRef,
            ),
        ])
        .unwrap();

        let input = make_input(vec![vec![batch0, batch1]]);
        // int_col < 3
        let split_expr = compile_expr(input.as_ref(), col("int_col").lt(lit(3)));
        let split_exec = StreamSplitExec::new(input, split_expr);

        let output0 = run_and_get_output(&split_exec, 0).await.unwrap();
        let expected = vec![
            "+---------+------------+",
            "| int_col | str_col    |",
            "+---------+------------+",
            "| -2      | negative 2 |",
            "| 1       | one        |",
            "| 2       | two        |",
            "+---------+------------+",
        ];
        assert_batches_sorted_eq!(&expected, &output0);

        let output1 = run_and_get_output(&split_exec, 1).await.unwrap();
        let expected = vec![
            "+---------+---------+",
            "| int_col | str_col |",
            "+---------+---------+",
            "| 3       | three   |",
            "| 4       | four    |",
            "+---------+---------+",
        ];
        assert_batches_sorted_eq!(&expected, &output1);
    }

    #[tokio::test]
    async fn test_constant_split() {
        // test that it works with a constant expression
        test_helpers::maybe_start_logging();
        let batch0 = RecordBatch::try_from_iter(vec![(
            "int_col",
            Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef,
        )])
        .unwrap();

        let input = make_input(vec![vec![batch0]]);
        // use `false` to send all outputs to second stream
        let split_expr = compile_expr(input.as_ref(), lit(false));
        let split_exec = StreamSplitExec::new(input, split_expr);

        let output0 = run_and_get_output(&split_exec, 0).await.unwrap();
        let expected = vec!["++", "||", "++", "++"];
        assert_batches_sorted_eq!(&expected, &output0);

        let output1 = run_and_get_output(&split_exec, 1).await.unwrap();
        let expected = vec![
            "+---------+",
            "| int_col |",
            "+---------+",
            "| 1       |",
            "| 2       |",
            "| 3       |",
            "+---------+",
        ];
        assert_batches_sorted_eq!(&expected, &output1);
    }

    #[tokio::test]
    async fn test_nulls() {
        // test with null inputs (so rows evaluate to null)

        test_helpers::maybe_start_logging();
        let batch0 = RecordBatch::try_from_iter(vec![(
            "int_col",
            Arc::new(Int64Array::from(vec![Some(1), None, Some(2), Some(3)])) as ArrayRef,
        )])
        .unwrap();

        let input = make_input(vec![vec![batch0]]);
        // int_col < 3
        let split_expr = compile_expr(input.as_ref(), col("int_col").lt(lit(3)));
        let split_exec = StreamSplitExec::new(input, split_expr);

        let output0 = run_and_get_output(&split_exec, 0).await.unwrap();
        let expected = vec![
            "+---------+",
            "| int_col |",
            "+---------+",
            "| 1       |",
            "| 2       |",
            "+---------+",
        ];
        assert_batches_sorted_eq!(&expected, &output0);

        let output1 = run_and_get_output(&split_exec, 1).await.unwrap();
        let expected = vec![
            "+---------+",
            "| int_col |",
            "+---------+",
            "|         |",
            "| 3       |",
            "+---------+",
        ];
        assert_batches_sorted_eq!(&expected, &output1);
    }

    #[tokio::test]
    async fn test_non_bool() {
        // test non boolean expression (expect error)

        test_helpers::maybe_start_logging();
        let batch0 = RecordBatch::try_from_iter(vec![(
            "int_col",
            Arc::new(Int64Array::from(vec![Some(1), None, Some(2), Some(3)])) as ArrayRef,
        )])
        .unwrap();

        let input = make_input(vec![vec![batch0]]);
        // int_col (not a boolean)
        let split_expr = compile_expr(input.as_ref(), col("int_col"));
        let split_exec = StreamSplitExec::new(input, split_expr);

        let output0 = run_and_get_output(&split_exec, 0)
            .await
            .unwrap_err()
            .to_string();
        assert!(
            output0.contains("Expected boolean array, but had type Int64"),
            "error was: {}",
            output0
        );
    }

    fn make_input(partitions: Vec<Vec<RecordBatch>>) -> Arc<dyn ExecutionPlan> {
        let schema = partitions
            .iter()
            .flat_map(|p| p.iter())
            .map(|batch| batch.schema())
            .next()
            .expect("must be at least one batch");

        let projection = None;
        let input =
            MemoryExec::try_new(&partitions, schema, projection).expect("Created MemoryExec");
        Arc::new(input)
    }

    fn compile_expr(input: &dyn ExecutionPlan, expr: Expr) -> Arc<dyn PhysicalExpr> {
        let physical_planner = DefaultPhysicalPlanner::default();
        // we should probably get a handle to the one in the default
        // physical planner somehow,..
        let ctx_state = datafusion::execution::context::ExecutionContextState::new();

        let input_physical_schema = input.schema();
        let input_logical_schema: DFSchema = input_physical_schema
            .as_ref()
            .clone()
            .try_into()
            .expect("could not make logical schema");

        physical_planner
            .create_physical_expr(
                &expr,
                &input_logical_schema,
                &input_physical_schema,
                &ctx_state,
            )
            .expect("creating physical expression")
    }

    /// Runs the `output_num` output of the stream and returns the results
    async fn run_and_get_output(
        split_exec: &StreamSplitExec,
        output_num: usize,
    ) -> Result<Vec<RecordBatch>> {
        let stream = split_exec.execute(output_num).await?;
        datafusion::physical_plan::common::collect(stream).await
    }
}
