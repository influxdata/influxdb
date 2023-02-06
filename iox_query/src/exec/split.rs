//! This module contains a DataFusion extension node to "split" a
//! stream based on an expression.
//!
//! All rows for which the expression are true are sent to partition
//! `0` and all other rows are sent to partition `1`.
//!
//! There are corresponding [`LogicalPlan`] ([`StreamSplitNode`]) and
//! [`ExecutionPlan`] ([`StreamSplitExec`]) implementations, which are
//! typically used as shown in the following diagram:
//!
//!
//! ```text
//!                                               partition 0            partition 1
//!                                                   ▲                       ▲
//!                                                   │                       │
//!                                                   └────────────┬──────────┘
//!                                                                │
//!                                                                │
//!                                                   ┌ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─
//!                                                        StreamSplitExec     │
//!                                                   │          expr
//!                                                    ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┘
//!                                                                ▲
//!                                                                │
//!                                                   ┌────────────────────────┐
//!                                                   │         Union          │
//!                                                   │                        │
//!                                                   └────────────────────────┘
//!                                                                ▲
//!                                                                │
//!
//!                                                       Other IOxScan code
//!       ┌────────────────────────┐                     (Filter, Dedup, etc)
//!       │ ┌ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─  │                             ...
//!       │      StreamSplit     │ │
//!       │ └ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─  │                               ▲
//!       │       Extension        │                               │
//!       └────────────────────────┘                               │
//!                    ▲                              ┌────────────────────────┐
//!                    │                              │     TableProvider      │
//!       ┌────────────────────────┐                  │                        │
//!       │       TableScan        │                  └────────────────────────┘
//!       │                        │
//!       └────────────────────────┘
//!
//!                                                          Execution Plan
//!             Logical Plan                                (Physical Plan)
//! ```

use std::{
    any::Any,
    fmt::{self, Debug},
    sync::Arc,
};

use arrow::{
    array::{as_boolean_array, Array, ArrayRef, BooleanArray},
    compute::{self, filter_record_batch},
    datatypes::SchemaRef,
    record_batch::RecordBatch,
};
use datafusion::{
    common::DFSchemaRef,
    error::{DataFusionError, Result},
    execution::context::TaskContext,
    logical_expr::{Expr, LogicalPlan, UserDefinedLogicalNode},
    physical_plan::{
        expressions::PhysicalSortExpr,
        metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet, RecordOutput},
        ColumnarValue, DisplayFormatType, Distribution, ExecutionPlan, Partitioning, PhysicalExpr,
        SendableRecordBatchStream, Statistics,
    },
    scalar::ScalarValue,
};

use datafusion_util::{watch::WatchedTask, AdapterStream};
use futures::StreamExt;
use observability_deps::tracing::*;
use parking_lot::Mutex;
use tokio::sync::mpsc::Sender;

/// Implements stream splitting described in `make_stream_split`
///
/// The resulting execution plan always produces exactly split_exprs's length + 1 partitions:
///
/// * partition i (i < split_exprs.len()) are the rows for which the `split_expr[i]`
///   evaluates to true. If the rows are evaluated true for both `split_expr[i]` and
///   `split_expr[j]`, where i < j, the rows will be sent to partition i. However,
///   this will be mostly used in the use case of range expressions (e.g: [2 <= x, 2= x <= 5])
///   in which rows are only evaluated to true in at most one of the expressions.
/// * partition n (n = partition split_exprs.len())  are the rows for which all split_exprs
///   do not evaluate to true (e.g. Null or false)
pub struct StreamSplitNode {
    input: LogicalPlan,
    split_exprs: Vec<Expr>,
}

impl StreamSplitNode {
    /// Create a new `StreamSplitNode` using `split_exprs` to divide the
    /// rows. All `split_exprs` must evaluate to a boolean otherwise a
    /// runtime error will occur.
    pub fn new(input: LogicalPlan, split_exprs: Vec<Expr>) -> Self {
        Self { input, split_exprs }
    }

    pub fn split_exprs(&self) -> &Vec<Expr> {
        &self.split_exprs
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
        self.split_exprs.clone()
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "StreamSplit split_expr={:?}", self.split_exprs)
    }

    fn from_template(
        &self,
        exprs: &[Expr],
        inputs: &[LogicalPlan],
    ) -> Arc<dyn UserDefinedLogicalNode> {
        assert_eq!(inputs.len(), 1, "StreamSplitNode: input sizes inconistent");
        Arc::new(Self {
            input: inputs[0].clone(),
            split_exprs: (*exprs).to_vec(),
        })
    }
}

/// Tracks the state of the physical operator
enum State {
    New,
    Running {
        streams: Vec<Option<SendableRecordBatchStream>>,
    },
}

/// Physical operator that implements steam splitting operation
pub struct StreamSplitExec {
    state: Mutex<State>,
    input: Arc<dyn ExecutionPlan>,
    split_exprs: Vec<Arc<dyn PhysicalExpr>>,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
}

impl StreamSplitExec {
    pub fn new(input: Arc<dyn ExecutionPlan>, split_exprs: Vec<Arc<dyn PhysicalExpr>>) -> Self {
        let state = Mutex::new(State::New);
        Self {
            state,
            input,
            split_exprs,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }
}

impl Debug for StreamSplitExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "StreamSplitExec {:?}", self.split_exprs)
    }
}

impl ExecutionPlan for StreamSplitExec {
    fn as_any(&self) -> &(dyn std::any::Any + 'static) {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.input.schema()
    }

    /// Always produces exactly two outputs
    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(self.split_exprs.len() + 1)
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        self.input.output_ordering()
    }

    /// Always require a single input (eventually we might imagine
    /// running this on multiple partitions concurrently to compute
    /// the splits in parallel, but not now)
    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![Distribution::SinglePartition]
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![Arc::clone(&self.input)]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        match children.len() {
            1 => Ok(Arc::new(Self::new(
                Arc::clone(&children[0]),
                self.split_exprs.clone(),
            ))),
            _ => Err(DataFusionError::Internal(
                "StreamSplitExec wrong number of children".to_string(),
            )),
        }
    }

    /// Stream split has multiple partitions from 0 to n
    /// Each partition i includes rows for which `split_exprs[i]` evaluate to true
    ///
    /// # Deadlock
    ///
    /// This will deadlock unless all partitions are consumed from
    /// concurrently. Failing to consume from one partition blocks the other
    /// partitions from progressing.
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        trace!(partition, "Start SplitExec::execute");
        self.start_if_needed(context)?;

        let mut state = self.state.lock();
        match &mut (*state) {
            State::New => panic!("should have been initialized"),
            State::Running { streams } => {
                assert!(partition < streams.len());
                let stream = streams[partition].take().unwrap_or_else(|| {
                    panic!("Error executing stream #{partition} of StreamSplitExec");
                });
                trace!(partition, "End SplitExec::execute");
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

    fn statistics(&self) -> Statistics {
        // For now, don't return any statistics (in the future we
        // could potentially estimate the output cardinalities)
        Statistics::default()
    }
}

impl StreamSplitExec {
    /// if in State::New, sets up the output running and sets self.state --> `Running`
    fn start_if_needed(&self, context: Arc<TaskContext>) -> Result<()> {
        let mut state = self.state.lock();
        if matches!(*state, State::Running { .. }) {
            return Ok(());
        }

        let num_input_streams = self.input.output_partitioning().partition_count();
        assert_eq!(
            num_input_streams, 1,
            "need exactly one input partition for stream split exec"
        );

        trace!("Setting up SplitStreamExec state");
        let input_stream = self.input.execute(0, context)?;

        let split_exprs = self.split_exprs.clone();

        let num_streams = split_exprs.len() + 1;
        let mut baseline_metrics = Vec::with_capacity(num_streams);
        let mut txs = Vec::with_capacity(num_streams);
        let mut rxs = Vec::with_capacity(num_streams);
        for i in 0..num_streams {
            baseline_metrics.push(BaselineMetrics::new(&self.metrics, i));
            let (tx, rx) = tokio::sync::mpsc::channel(2);
            txs.push(tx);
            rxs.push(rx);
        }

        // launch the work on a different task, with a task to handle its output values
        let fut = split_the_stream(input_stream, split_exprs, txs.clone(), baseline_metrics);
        let handle = WatchedTask::new(fut, txs, "split");

        let streams = rxs
            .into_iter()
            .map(|rx| {
                Some(AdapterStream::adapt(
                    self.input.schema(),
                    rx,
                    Arc::clone(&handle),
                ))
            })
            .collect::<Vec<_>>();

        *state = State::Running { streams };

        Ok(())
    }
}

/// This function does the actual splitting: evaluates `split_exprs` on
/// each input [`RecordBatch`], and then sends the rows to the correct
/// output `tx[i]`
async fn split_the_stream(
    mut input_stream: SendableRecordBatchStream,
    split_exprs: Vec<Arc<dyn PhysicalExpr>>,
    tx: Vec<Sender<Result<RecordBatch, DataFusionError>>>,
    baseline_metrics: Vec<BaselineMetrics>,
) -> std::result::Result<(), DataFusionError> {
    assert_eq!(split_exprs.len() + 1, tx.len());
    assert_eq!(tx.len(), baseline_metrics.len());

    let elapsed_computes = baseline_metrics
        .iter()
        .map(|b| b.elapsed_compute())
        .collect::<Vec<_>>();

    while let Some(batch) = input_stream.next().await {
        let batch = batch?;
        trace!(num_rows = batch.num_rows(), "Processing batch");

        // All streams are not done yet
        let mut tx_done = tx.iter().map(|_| false).collect::<Vec<_>>();

        // Get data from the current batch for each stream
        let mut remaining_indices: Option<ColumnarValue> = None;
        for i in 0..split_exprs.len() {
            let timer = elapsed_computes[i].timer();
            let expr = &split_exprs[i];

            // Compute indices that meets this expr
            let true_indices = expr.evaluate(&batch)?;
            // Indices that does not meet this expr
            let not_true_indices = negate(&true_indices)?;

            // Indices that do not meet all exprs
            if let Some(not_true) = remaining_indices {
                remaining_indices = Some(
                    and(&not_true, &not_true_indices)
                        .expect("Error computing combining negating indices"),
                );
            } else {
                remaining_indices = Some(not_true_indices);
            };

            // data that meets expr
            let true_batch = compute_batch(&batch, true_indices, false)?;
            timer.done();

            // record output counts
            let true_batch = true_batch.record_output(&baseline_metrics[i]);

            // don't treat a hangup as an error, as it can also be caused
            // by a LIMIT operation where the entire stream is not
            // consumed)
            if let Err(e) = tx[i].send(Ok(true_batch)).await {
                debug!(%e, "Split tx[{}] hung up, ignoring", i);
                tx_done[i] = true;
            }
        }

        // last stream of data gets values that did not get routed to other streams
        let timer = elapsed_computes[elapsed_computes.len() - 1].timer();
        let remaining_indices =
            remaining_indices.expect("The last set of indices of the split should have values");
        let final_not_true_batch = compute_batch(&batch, remaining_indices, true)?;
        timer.done();

        // record output counts
        let final_not_true_batch =
            final_not_true_batch.record_output(&baseline_metrics[elapsed_computes.len() - 1]);

        // don't treat a hangup as an error, as it can also be caused
        // by a LIMIT operation where the entire stream is not
        // consumed)
        if let Err(e) = tx[elapsed_computes.len() - 1]
            .send(Ok(final_not_true_batch))
            .await
        {
            debug!(%e, "Split tx[{}] hung up, ignoring", elapsed_computes.len()-1);
            tx_done[elapsed_computes.len() - 1] = true;
        }

        if tx_done.iter().all(|x| *x) {
            debug!("All split tx ends have hung up, stopping loop");
            return Ok(());
        }
    }

    trace!("Splitting done successfully");
    Ok(())
}

fn compute_batch(
    input_batch: &RecordBatch,
    indices: ColumnarValue,
    last_batch: bool,
) -> Result<RecordBatch> {
    let batch = match indices {
        ColumnarValue::Array(indices) => {
            let indices = indices.as_any().downcast_ref::<BooleanArray>().unwrap();

            // include null for last batch
            if last_batch && indices.null_count() > 0 {
                // since !Null --> Null, but we want all the
                // remaining rows, that are not in true_indicies,
                // transform any nulls into true for this one
                let mapped_indicies = indices.iter().map(|v| v.or(Some(true))).collect::<Vec<_>>();

                filter_record_batch(input_batch, &BooleanArray::from(mapped_indicies))
            } else {
                filter_record_batch(input_batch, indices)
            }?
        }
        ColumnarValue::Scalar(ScalarValue::Boolean(val)) => {
            let empty_record_batch = RecordBatch::new_empty(input_batch.schema());
            match val {
                Some(true) => input_batch.clone(),
                Some(false) => empty_record_batch,
                _ => panic!("mismatched boolean values: {val:?}"),
            }
        }
        _ => {
            panic!("mismatched array types");
        }
    };

    Ok(batch)
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

fn and(left: &ColumnarValue, right: &ColumnarValue) -> Result<ColumnarValue> {
    match (left, right) {
        (ColumnarValue::Array(arr_left), ColumnarValue::Array(arr_right)) => {
            let arr_left = as_boolean_array(arr_left);
            let arr_right = as_boolean_array(arr_right);
            let and_array = Arc::new(compute::and(arr_left, arr_right)?) as ArrayRef;
            Ok(ColumnarValue::Array(and_array))
        }
        (ColumnarValue::Scalar(val_left), ColumnarValue::Scalar(val_right)) => {
            if let (ScalarValue::Boolean(Some(v_left)), ScalarValue::Boolean(Some(v_right))) =
                (val_left, val_right)
            {
                let and_val = v_left & v_right;
                Ok(ColumnarValue::Scalar(ScalarValue::Boolean(Some(and_val))))
            } else {
                let msg = format!(
                    "Expected two boolean literals, but got type {:?} and type {:?}",
                    val_left.get_datatype(),
                    val_right.get_datatype()
                );
                Err(DataFusionError::Internal(msg))
            }
        }
        _ => {
            panic!("Expected either two boolean arrays or two boolean scalars, but had type {:?} and type {:?}", left.data_type(), right.data_type());
        }
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::{Int64Array, StringArray};
    use arrow_util::assert_batches_sorted_eq;
    use datafusion::{
        physical_plan::memory::MemoryExec,
        prelude::{col, lit},
    };
    use datafusion_util::test_collect_partition;

    use crate::util::df_physical_expr;

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
        let split_expr = df_physical_expr(input.as_ref(), col("int_col").lt(lit(3))).unwrap();
        let split_exec: Arc<dyn ExecutionPlan> =
            Arc::new(StreamSplitExec::new(input, vec![split_expr]));

        let output0 = test_collect_partition(Arc::clone(&split_exec), 0).await;
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

        let output1 = test_collect_partition(split_exec, 1).await;
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
    async fn test_basic_split_multi_exprs() {
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
        // int_col < 2
        let split_expr1 =
            df_physical_expr(input.as_ref(), col("int_col").lt(lit::<i16>(2))).unwrap();
        // 2 <= int_col < 3
        let expr = col("int_col")
            .gt_eq(lit::<i16>(2))
            .and(col("int_col").lt(lit::<i16>(3)));
        let split_expr2 = df_physical_expr(input.as_ref(), expr).unwrap();
        let split_exec: Arc<dyn ExecutionPlan> =
            Arc::new(StreamSplitExec::new(input, vec![split_expr1, split_expr2]));

        let output0 = test_collect_partition(Arc::clone(&split_exec), 0).await;
        let expected = vec![
            "+---------+------------+",
            "| int_col | str_col    |",
            "+---------+------------+",
            "| -2      | negative 2 |",
            "| 1       | one        |",
            "+---------+------------+",
        ];
        assert_batches_sorted_eq!(&expected, &output0);

        let output1 = test_collect_partition(Arc::clone(&split_exec), 1).await;
        let expected = vec![
            "+---------+---------+",
            "| int_col | str_col |",
            "+---------+---------+",
            "| 2       | two     |",
            "+---------+---------+",
        ];
        assert_batches_sorted_eq!(&expected, &output1);

        let output2 = test_collect_partition(split_exec, 2).await;
        let expected = vec![
            "+---------+---------+",
            "| int_col | str_col |",
            "+---------+---------+",
            "| 3       | three   |",
            "| 4       | four    |",
            "+---------+---------+",
        ];
        assert_batches_sorted_eq!(&expected, &output2);
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
        let split_expr = df_physical_expr(input.as_ref(), lit(false)).unwrap();
        let split_exec: Arc<dyn ExecutionPlan> =
            Arc::new(StreamSplitExec::new(input, vec![split_expr]));

        let output0 = test_collect_partition(Arc::clone(&split_exec), 0).await;
        let expected = vec!["+---------+", "| int_col |", "+---------+", "+---------+"];
        assert_batches_sorted_eq!(&expected, &output0);

        let output1 = test_collect_partition(split_exec, 1).await;
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
    async fn test_constant_split_multi_exprs() {
        // test that it works with a constant expression
        test_helpers::maybe_start_logging();
        let batch0 = RecordBatch::try_from_iter(vec![(
            "int_col",
            Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef,
        )])
        .unwrap();

        // Test 1: 3 streams but all data is sent to the second one
        let input = make_input(vec![vec![batch0.clone()]]);
        // use `false` & `true` to send all outputs to second stream
        let split_expr1 = df_physical_expr(input.as_ref(), lit(false)).unwrap();
        let split_expr2 = df_physical_expr(input.as_ref(), lit(true)).unwrap();
        let split_exec: Arc<dyn ExecutionPlan> =
            Arc::new(StreamSplitExec::new(input, vec![split_expr1, split_expr2]));

        let output0 = test_collect_partition(Arc::clone(&split_exec), 0).await;
        let expected = vec!["+---------+", "| int_col |", "+---------+", "+---------+"];
        assert_batches_sorted_eq!(&expected, &output0);

        let output1 = test_collect_partition(Arc::clone(&split_exec), 1).await;
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

        let output2 = test_collect_partition(split_exec, 2).await;
        let expected = vec!["+---------+", "| int_col |", "+---------+", "+---------+"];
        assert_batches_sorted_eq!(&expected, &output2);

        // -----------------------
        // Test 2: 3 streams but all data is sent to the last one
        let input = make_input(vec![vec![batch0.clone()]]);

        // use `false` & `false` to send all outputs to third stream
        let split_expr1 = df_physical_expr(input.as_ref(), lit(false)).unwrap();
        let split_expr2 = df_physical_expr(input.as_ref(), lit(false)).unwrap();
        let split_exec: Arc<dyn ExecutionPlan> =
            Arc::new(StreamSplitExec::new(input, vec![split_expr1, split_expr2]));

        let output0 = test_collect_partition(Arc::clone(&split_exec), 0).await;
        let expected = vec!["+---------+", "| int_col |", "+---------+", "+---------+"];
        assert_batches_sorted_eq!(&expected, &output0);

        let output1 = test_collect_partition(Arc::clone(&split_exec), 1).await;
        let expected = vec!["+---------+", "| int_col |", "+---------+", "+---------+"];
        assert_batches_sorted_eq!(&expected, &output1);

        let output2 = test_collect_partition(Arc::clone(&split_exec), 2).await;
        let expected = vec![
            "+---------+",
            "| int_col |",
            "+---------+",
            "| 1       |",
            "| 2       |",
            "| 3       |",
            "+---------+",
        ];
        assert_batches_sorted_eq!(&expected, &output2);

        // -----------------------
        // Test 3: 3 streams but all data is sent to the first
        let input = make_input(vec![vec![batch0]]);

        // use `true` & `false` to send all outputs to first stream
        let split_expr1 = df_physical_expr(input.as_ref(), lit(true)).unwrap();
        let split_expr2 = df_physical_expr(input.as_ref(), lit(false)).unwrap();
        let split_exec: Arc<dyn ExecutionPlan> =
            Arc::new(StreamSplitExec::new(input, vec![split_expr1, split_expr2]));

        let output0 = test_collect_partition(Arc::clone(&split_exec), 0).await;
        let expected = vec![
            "+---------+",
            "| int_col |",
            "+---------+",
            "| 1       |",
            "| 2       |",
            "| 3       |",
            "+---------+",
        ];
        assert_batches_sorted_eq!(&expected, &output0);

        let output1 = test_collect_partition(Arc::clone(&split_exec), 1).await;
        let expected = vec!["+---------+", "| int_col |", "+---------+", "+---------+"];
        assert_batches_sorted_eq!(&expected, &output1);

        let output2 = test_collect_partition(Arc::clone(&split_exec), 2).await;
        let expected = vec!["+---------+", "| int_col |", "+---------+", "+---------+"];
        assert_batches_sorted_eq!(&expected, &output2);
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
        let split_expr = df_physical_expr(input.as_ref(), col("int_col").lt(lit(3))).unwrap();
        let split_exec: Arc<dyn ExecutionPlan> =
            Arc::new(StreamSplitExec::new(input, vec![split_expr]));

        let output0 = test_collect_partition(Arc::clone(&split_exec), 0).await;
        let expected = vec![
            "+---------+",
            "| int_col |",
            "+---------+",
            "| 1       |",
            "| 2       |",
            "+---------+",
        ];
        assert_batches_sorted_eq!(&expected, &output0);

        let output1 = test_collect_partition(split_exec, 1).await;
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
    async fn test_nulls_multi_exprs() {
        // test with null inputs (so rows evaluate to null)

        test_helpers::maybe_start_logging();
        let batch0 = RecordBatch::try_from_iter(vec![(
            "int_col",
            Arc::new(Int64Array::from(vec![Some(1), None, Some(2), Some(3)])) as ArrayRef,
        )])
        .unwrap();

        let input = make_input(vec![vec![batch0]]);
        // int_col < 2
        let split_expr1 =
            df_physical_expr(input.as_ref(), col("int_col").lt(lit::<i16>(2))).unwrap();
        // 2 <= int_col < 3
        let expr = col("int_col")
            .gt_eq(lit::<i16>(2))
            .and(col("int_col").lt(lit::<i16>(3)));
        let split_expr2 = df_physical_expr(input.as_ref(), expr).unwrap();
        let split_exec: Arc<dyn ExecutionPlan> =
            Arc::new(StreamSplitExec::new(input, vec![split_expr1, split_expr2]));

        let output0 = test_collect_partition(Arc::clone(&split_exec), 0).await;
        let expected = vec![
            "+---------+",
            "| int_col |",
            "+---------+",
            "| 1       |",
            "+---------+",
        ];
        assert_batches_sorted_eq!(&expected, &output0);

        let output1 = test_collect_partition(Arc::clone(&split_exec), 1).await;
        let expected = vec![
            "+---------+",
            "| int_col |",
            "+---------+",
            "| 2       |",
            "+---------+",
        ];
        assert_batches_sorted_eq!(&expected, &output1);

        let output2 = test_collect_partition(split_exec, 2).await;
        let expected = vec![
            "+---------+",
            "| int_col |",
            "+---------+",
            "|         |",
            "| 3       |",
            "+---------+",
        ];
        assert_batches_sorted_eq!(&expected, &output2);
    }

    #[tokio::test]
    #[should_panic(expected = "Expected boolean array, but had type Int64")]
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
        let split_expr = df_physical_expr(input.as_ref(), col("int_col")).unwrap();
        let split_exec: Arc<dyn ExecutionPlan> =
            Arc::new(StreamSplitExec::new(input, vec![split_expr]));

        test_collect_partition(split_exec, 0).await;
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
}
