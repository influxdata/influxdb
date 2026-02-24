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
    fmt::{self, Debug},
    sync::Arc,
};

use arrow::{
    array::{Array, ArrayRef, BooleanArray},
    compute::{self, filter_record_batch},
    datatypes::SchemaRef,
    record_batch::RecordBatch,
};
use datafusion::physical_expr::{LexRequirement, OrderingRequirements};
use datafusion::{
    common::DFSchemaRef,
    error::{DataFusionError, Result},
    execution::context::TaskContext,
    logical_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore},
    physical_plan::{
        ColumnarValue, DisplayAs, DisplayFormatType, Distribution, ExecutionPlan,
        ExecutionPlanProperties, Partitioning, PhysicalExpr, PlanProperties,
        SendableRecordBatchStream, Statistics,
        metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet, RecordOutput},
    },
    scalar::ScalarValue,
};
use datafusion_util::{AdapterStream, watch::WatchedTask};
use futures::StreamExt;
use parking_lot::Mutex;
use tokio::sync::mpsc::Sender;
use tracing::*;

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
#[derive(Hash, PartialEq, Eq, PartialOrd)]
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

impl UserDefinedLogicalNodeCore for StreamSplitNode {
    fn name(&self) -> &str {
        "StreamSplit"
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
        write!(f, "{} split_expr={:?}", self.name(), self.split_exprs)
    }

    fn with_exprs_and_inputs(&self, exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> Result<Self> {
        assert_eq!(inputs.len(), 1, "StreamSplitNode: input sizes inconsistent");
        Ok(Self {
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
    /// Cache holding plan properties like equivalences, output partitioning, output ordering etc.
    cache: PlanProperties,
}

impl StreamSplitExec {
    pub fn new(input: Arc<dyn ExecutionPlan>, split_exprs: Vec<Arc<dyn PhysicalExpr>>) -> Self {
        let state = Mutex::new(State::New);
        let cache = Self::compute_properties(&input, &split_exprs);
        Self {
            state,
            input,
            split_exprs,
            metrics: ExecutionPlanMetricsSet::new(),
            cache,
        }
    }

    /// This function creates the cache object that stores the plan properties such as equivalence properties, partitioning, ordering, etc.
    fn compute_properties(
        input: &Arc<dyn ExecutionPlan>,
        split_exprs: &[Arc<dyn PhysicalExpr>],
    ) -> PlanProperties {
        let eq_properties = input.equivalence_properties().clone();

        // Always produces exactly two outputs
        let output_partitioning = Partitioning::UnknownPartitioning(split_exprs.len() + 1);

        PlanProperties::new(
            eq_properties,
            output_partitioning,
            input.pipeline_behavior(),
            input.boundedness(),
        )
    }
}

impl Debug for StreamSplitExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "StreamSplitExec {:?}", self.split_exprs)
    }
}

impl ExecutionPlan for StreamSplitExec {
    fn name(&self) -> &str {
        Self::static_name()
    }

    fn as_any(&self) -> &(dyn std::any::Any + 'static) {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.input.schema()
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    /// Always require a single input (eventually we might imagine
    /// running this on multiple partitions concurrently to compute
    /// the splits in parallel, but not now)
    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![Distribution::SinglePartition]
    }

    fn required_input_ordering(&self) -> Vec<Option<OrderingRequirements>> {
        // require that the output ordering of the child is preserved
        // (so that this node logically splits what was desired)
        let requirement = self
            .input
            .properties()
            .output_ordering()
            .cloned()
            .map(LexRequirement::from)
            .map(OrderingRequirements::new);

        vec![requirement]
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
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

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn statistics(&self) -> Result<Statistics> {
        // For now, don't return any statistics (in the future we
        // could potentially estimate the output cardinalities)
        Ok(Statistics::new_unknown(&self.schema()))
    }
}

impl DisplayAs for StreamSplitExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match t {
            DisplayFormatType::Default
            | DisplayFormatType::Verbose
            | DisplayFormatType::TreeRender => {
                write!(f, "StreamSplitExec")
            }
        }
    }
}

impl StreamSplitExec {
    /// if in State::New, sets up the output running and sets self.state --> `Running`
    fn start_if_needed(&self, context: Arc<TaskContext>) -> Result<()> {
        let mut state = self.state.lock();
        if matches!(*state, State::Running { .. }) {
            return Ok(());
        }

        let num_input_streams = self
            .input
            .properties()
            .output_partitioning()
            .partition_count();
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
                    val.data_type()
                );
                Err(DataFusionError::Internal(msg))
            }
        }
    }
}

/// Extract `Option<bool>` from `ScalarValue::Boolean` with a descriptive error on type mismatch.
fn try_as_boolean_scalar(scalar: &ScalarValue) -> Result<Option<bool>> {
    match scalar {
        ScalarValue::Boolean(v) => Ok(*v),
        other => Err(DataFusionError::Internal(format!(
            "Expected boolean scalar, but got type {:?}",
            other.data_type()
        ))),
    }
}

/// Downcast an ArrayRef to BooleanArray, returning an error if it's not a boolean array.
fn try_as_boolean_array(arr: &ArrayRef) -> Result<&BooleanArray> {
    arr.as_any().downcast_ref::<BooleanArray>().ok_or_else(|| {
        DataFusionError::Internal(format!(
            "Expected boolean array, but got type {:?}",
            arr.data_type()
        ))
    })
}

/// Boolean AND on ColumnarValues. Handles all combinations of Array/Scalar ColumnarValue.
fn and(left: &ColumnarValue, right: &ColumnarValue) -> Result<ColumnarValue> {
    match (left, right) {
        // Array/Array: use Arrow kernel
        (ColumnarValue::Array(l), ColumnarValue::Array(r)) => {
            let result = compute::and(try_as_boolean_array(l)?, try_as_boolean_array(r)?)?;
            Ok(ColumnarValue::Array(Arc::new(result)))
        }
        // Scalar/Scalar: boolean logic with null handling
        (ColumnarValue::Scalar(l), ColumnarValue::Scalar(r)) => {
            let result = match (try_as_boolean_scalar(l)?, try_as_boolean_scalar(r)?) {
                (Some(l), Some(r)) => Some(l & r),
                // Arrow AND semantics: null propagates
                _ => None,
            };
            Ok(ColumnarValue::Scalar(ScalarValue::Boolean(result)))
        }
        // Mixed Array/Scalar: short-circuit based on scalar value
        (ColumnarValue::Scalar(scalar), ColumnarValue::Array(arr))
        | (ColumnarValue::Array(arr), ColumnarValue::Scalar(scalar)) => {
            match try_as_boolean_scalar(scalar)? {
                // scalar true: return the array unchanged
                Some(true) => Ok(ColumnarValue::Array(Arc::clone(arr))),
                // scalar false: return scalar false
                Some(false) => Ok(ColumnarValue::Scalar(ScalarValue::Boolean(Some(false)))),
                // Arrow AND semantics: null propagates
                None => Ok(ColumnarValue::Scalar(ScalarValue::Boolean(None))),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::{Int64Array, StringArray};
    use arrow_util::assert_batches_sorted_eq;
    use datafusion::common::DFSchema;
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::datasource::source::DataSourceExec;
    use datafusion::{
        execution::context::SessionContext,
        prelude::{col, lit},
    };
    use datafusion_util::test_collect_partition;

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
        let split_expr = df_physical_expr(&input, col("int_col").lt(lit(3)));
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
        let split_expr1 = df_physical_expr(&input, col("int_col").lt(lit::<i16>(2)));
        // 2 <= int_col < 3
        let expr = col("int_col")
            .gt_eq(lit::<i16>(2))
            .and(col("int_col").lt(lit::<i16>(3)));
        let split_expr2 = df_physical_expr(&input, expr);
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
        let split_expr = df_physical_expr(&input, lit(false));
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
        let split_expr1 = df_physical_expr(&input, lit(false));
        let split_expr2 = df_physical_expr(&input, lit(true));
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
        let split_expr1 = df_physical_expr(&input, lit(false));
        let split_expr2 = df_physical_expr(&input, lit(false));
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
        let split_expr1 = df_physical_expr(&input, lit(true));
        let split_expr2 = df_physical_expr(&input, lit(false));
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
        let split_expr = df_physical_expr(&input, col("int_col").lt(lit(3)));
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
        let split_expr1 = df_physical_expr(&input, col("int_col").lt(lit::<i16>(2)));
        // 2 <= int_col < 3
        let expr = col("int_col")
            .gt_eq(lit::<i16>(2))
            .and(col("int_col").lt(lit::<i16>(3)));
        let split_expr2 = df_physical_expr(&input, expr);
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
        let split_expr = df_physical_expr(&input, col("int_col"));
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

        Arc::new(DataSourceExec::new(Arc::new(
            MemorySourceConfig::try_new(&partitions, schema, projection)
                .expect("Created MemorySourceConfig"),
        )))
    }

    /// Return a `PhysicalExpr` from a logical `Expr`
    fn df_physical_expr(input: &Arc<dyn ExecutionPlan>, expr: Expr) -> Arc<dyn PhysicalExpr> {
        let ctx = SessionContext::new();
        let df_schema = DFSchema::try_from(input.schema()).unwrap();
        ctx.create_physical_expr(expr, &df_schema)
            .expect("Created PhysicalExpr")
    }

    /// Test that mixed Scalar/Array ColumnarValue combinations work correctly. This is a
    /// regression test for the DataFusion 50.3 upgrade where `PhysicalExpr::evaluate` sometimes
    /// returns Scalar values.
    /// Related Issues:
    /// - https://github.com/influxdata/influxdb_iox/issues/15755
    /// - https://github.com/apache/datafusion/pull/16930
    #[tokio::test]
    async fn test_mixed_scalar_array_values() {
        test_helpers::maybe_start_logging();

        let batch = RecordBatch::try_from_iter(vec![(
            "value",
            Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10])) as ArrayRef,
        )])
        .unwrap();

        let input = make_input(vec![vec![batch]]);

        // Split expr 1: value < 3
        // Returns Array with mixed true/false results
        let split_expr1 = df_physical_expr(&input, col("value").lt(lit(3)));

        // Split expr 2: value == 10 AND value > 100
        // This expression is an AND expression that triggers pre-selection.
        // - LHS: `value == 10` has only 10% true -> triggers pre-selection (threshold is <20%)
        // - RHS input filtered to just [10], RHS `value > 100` evaluated
        // - After DF 50.3 upgrade: RHS returns Scalar(false) instead of Array([false])
        let split_expr2 = df_physical_expr(
            &input,
            col("value").eq(lit(10)).and(col("value").gt(lit(100))),
        );

        let split_exec: Arc<dyn ExecutionPlan> =
            Arc::new(StreamSplitExec::new(input, vec![split_expr1, split_expr2]));

        // Partition 0: value < 3
        let output0 = test_collect_partition(Arc::clone(&split_exec), 0).await;
        let expected = vec![
            "+-------+",
            "| value |",
            "+-------+",
            "| 1     |",
            "| 2     |",
            "+-------+",
        ];
        assert_batches_sorted_eq!(&expected, &output0);

        // Partition 1: value == 10 AND value > 100 (empty)
        let output1 = test_collect_partition(Arc::clone(&split_exec), 1).await;
        let expected = vec!["+-------+", "| value |", "+-------+", "+-------+"];
        assert_batches_sorted_eq!(&expected, &output1);

        // Partition 2 (remainder): values 3-10
        let output2 = test_collect_partition(split_exec, 2).await;
        let expected = vec![
            "+-------+",
            "| value |",
            "+-------+",
            "| 10    |",
            "| 3     |",
            "| 4     |",
            "| 5     |",
            "| 6     |",
            "| 7     |",
            "| 8     |",
            "| 9     |",
            "+-------+",
        ];
        assert_batches_sorted_eq!(&expected, &output2);
    }

    /// Unit tests for the [`super::and`] function
    mod and {
        use super::*;

        /// Construct a boolean ColumnarValue::Array
        fn arr(values: &[Option<bool>]) -> ColumnarValue {
            ColumnarValue::from(Arc::new(BooleanArray::from(values.to_vec())) as ArrayRef)
        }

        /// Construct a boolean ColumnarValue::Scalar
        fn scalar(value: Option<bool>) -> ColumnarValue {
            ColumnarValue::from(ScalarValue::from(value))
        }

        /// ColumnarValue doesn't have a PartialEq impl, so we need a custom assertion
        fn assert_columnar_eq(result: &ColumnarValue, expected: &ColumnarValue) {
            match (result, expected) {
                (ColumnarValue::Array(r), ColumnarValue::Array(e)) => {
                    assert_eq!(r.as_ref(), e.as_ref());
                }
                (ColumnarValue::Scalar(r), ColumnarValue::Scalar(e)) => {
                    assert_eq!(r, e);
                }
                _ => panic!("result type mismatch: {result} vs {expected}"),
            }
        }

        #[test]
        /// Valid combinations for AND
        fn truth_table() {
            struct Case {
                left: ColumnarValue,
                right: ColumnarValue,
                expected: ColumnarValue,
            }

            let cases = vec![
                // Array & Array: covers all 9 element-wise combinations
                Case {
                    left: arr(&[
                        Some(true),
                        Some(true),
                        Some(true),
                        Some(false),
                        Some(false),
                        Some(false),
                        None,
                        None,
                        None,
                    ]),
                    right: arr(&[
                        Some(true),
                        Some(false),
                        None,
                        Some(true),
                        Some(false),
                        None,
                        Some(true),
                        Some(false),
                        None,
                    ]),
                    expected: arr(&[
                        Some(true),
                        Some(false),
                        None,
                        Some(false),
                        Some(false),
                        None,
                        None,
                        None,
                        None,
                    ]),
                },
                // Scalar & Scalar: Covers all left/right combinations
                Case {
                    left: scalar(Some(true)),
                    right: scalar(Some(true)),
                    expected: scalar(Some(true)),
                },
                Case {
                    left: scalar(Some(true)),
                    right: scalar(Some(false)),
                    expected: scalar(Some(false)),
                },
                Case {
                    left: scalar(Some(false)),
                    right: scalar(Some(true)),
                    expected: scalar(Some(false)),
                },
                Case {
                    left: scalar(Some(false)),
                    right: scalar(Some(false)),
                    expected: scalar(Some(false)),
                },
                Case {
                    left: scalar(None),
                    right: scalar(Some(true)),
                    expected: scalar(None),
                },
                Case {
                    left: scalar(Some(true)),
                    right: scalar(None),
                    expected: scalar(None),
                },
                Case {
                    left: scalar(Some(false)),
                    right: scalar(None),
                    expected: scalar(None),
                },
                Case {
                    left: scalar(None),
                    right: scalar(Some(false)),
                    expected: scalar(None),
                },
                Case {
                    left: scalar(None),
                    right: scalar(None),
                    expected: scalar(None),
                },
                // Mixed: scalar(true) & array -> returns array unchanged
                Case {
                    left: scalar(Some(true)),
                    right: arr(&[Some(true), Some(false), None]),
                    expected: arr(&[Some(true), Some(false), None]),
                },
                Case {
                    left: arr(&[Some(true), Some(false), None]),
                    right: scalar(Some(true)),
                    expected: arr(&[Some(true), Some(false), None]),
                },
                // Mixed: scalar(false) & array -> short-circuits to scalar(false)
                Case {
                    left: scalar(Some(false)),
                    right: arr(&[Some(true), Some(false), None]),
                    expected: scalar(Some(false)),
                },
                Case {
                    left: arr(&[Some(true), Some(false), None]),
                    right: scalar(Some(false)),
                    expected: scalar(Some(false)),
                },
                // Mixed: scalar(null) & array -> returns scalar null
                Case {
                    left: scalar(None),
                    right: arr(&[Some(true), Some(false), None]),
                    expected: scalar(None),
                },
                Case {
                    left: arr(&[Some(true), Some(false), None]),
                    right: scalar(None),
                    expected: scalar(None),
                },
                // Empty arrays
                Case {
                    left: arr(&[]),
                    right: arr(&[]),
                    expected: arr(&[]),
                },
                Case {
                    left: arr(&[]),
                    right: scalar(Some(true)),
                    expected: arr(&[]),
                },
                Case {
                    left: scalar(Some(true)),
                    right: arr(&[]),
                    expected: arr(&[]),
                },
                Case {
                    left: arr(&[]),
                    right: scalar(Some(false)),
                    expected: scalar(Some(false)),
                },
                Case {
                    left: scalar(Some(false)),
                    right: arr(&[]),
                    expected: scalar(Some(false)),
                },
                Case {
                    left: arr(&[]),
                    right: scalar(None),
                    expected: scalar(None),
                },
                Case {
                    left: scalar(None),
                    right: arr(&[]),
                    expected: scalar(None),
                },
            ];

            for Case {
                left,
                right,
                expected,
            } in cases
            {
                let result = and(&left, &right).expect("and() should succeed");
                assert_columnar_eq(&result, &expected);
            }
        }

        #[test]
        fn type_errors() {
            struct Case {
                left: ColumnarValue,
                right: ColumnarValue,
                expected: &'static str,
            }

            let int_arr = ColumnarValue::from(Arc::new(Int64Array::from(vec![1])) as ArrayRef);
            let int_scalar = ColumnarValue::from(ScalarValue::Int64(Some(42)));
            let bool_arr = arr(&[Some(true)]);
            let bool_scalar = scalar(Some(true));

            let cases = vec![
                Case {
                    left: int_scalar.clone(),
                    right: bool_scalar.clone(),
                    expected: "Expected boolean scalar",
                },
                Case {
                    left: bool_scalar.clone(),
                    right: int_scalar.clone(),
                    expected: "Expected boolean scalar",
                },
                Case {
                    left: int_arr.clone(),
                    right: bool_arr.clone(),
                    expected: "Expected boolean array",
                },
                Case {
                    left: bool_arr.clone(),
                    right: int_arr.clone(),
                    expected: "Expected boolean array",
                },
            ];

            for Case {
                left,
                right,
                expected,
            } in cases
            {
                let result = and(&left, &right);
                assert!(result.is_err(), "{left} & {right}");
                assert!(
                    result.unwrap_err().to_string().contains(expected),
                    "{left} & {right}"
                );
            }
        }

        #[test]
        fn length_mismatch() {
            let short = arr(&[Some(true)]);
            let long = arr(&[Some(true), Some(false)]);
            let result = and(&short, &long);
            assert!(result.is_err(), "mismatched lengths should error");
            let err_msg = result.unwrap_err().to_string();
            assert!(
                err_msg.contains("Cannot perform bitwise operation on arrays of different length"),
            );
        }

        /// Property-based tests for AND
        mod proptests {
            use super::*;
            use proptest::prelude::*;

            /// Strategy for generating arbitrary Option<bool> values
            fn arb_opt_bool() -> impl Strategy<Value = Option<bool>> {
                prop_oneof![Just(None), Just(Some(true)), Just(Some(false)),]
            }

            /// Strategy for generating ColumnarValue::Scalar with boolean values
            fn arb_scalar() -> impl Strategy<Value = ColumnarValue> {
                arb_opt_bool().prop_map(scalar)
            }

            /// Strategy for generating ColumnarValue::Array with boolean values of given length
            fn arb_array(len: usize) -> impl Strategy<Value = ColumnarValue> {
                prop::collection::vec(arb_opt_bool(), len).prop_map(|v| arr(&v))
            }

            /// Strategy for generating any ColumnarValue (scalar or array of given length)
            fn arb_columnar_value_with_len(len: usize) -> impl Strategy<Value = ColumnarValue> {
                prop_oneof![arb_scalar(), arb_array(len),]
            }

            /// Strategy for generating any ColumnarValue with default array length of 8
            fn arb_columnar_value() -> impl Strategy<Value = ColumnarValue> {
                arb_columnar_value_with_len(8)
            }

            /// Returns true if the value is a scalar null
            fn is_scalar_null(cv: &ColumnarValue) -> bool {
                matches!(cv, ColumnarValue::Scalar(ScalarValue::Boolean(None)))
            }

            // Commutativity: a & b == b & a
            proptest! {
                #[test]
                fn commutativity(a in arb_columnar_value(), b in arb_columnar_value()) {
                    let ab = and(&a, &b).unwrap();
                    let ba = and(&b, &a).unwrap();
                    assert_columnar_eq(&ab, &ba);
                }
            }

            // Identity: a & true == a
            proptest! {
                #[test]
                fn identity(a in arb_columnar_value()) {
                    let result = and(&a, &scalar(Some(true))).unwrap();
                    assert_columnar_eq(&result, &a);

                    let result = and(&scalar(Some(true)), &a).unwrap();
                    assert_columnar_eq(&result, &a);
                }
            }

            // Annihilator: a & false == scalar(false) (except for scalar null where null propagation wins)
            proptest! {
                #[test]
                fn annihilator_false(a in arb_columnar_value().prop_filter("except scalar null", |v| !is_scalar_null(v))) {
                    let result = and(&a, &scalar(Some(false))).unwrap();
                    assert_columnar_eq(&result, &scalar(Some(false)));

                    let result = and(&scalar(Some(false)), &a).unwrap();
                    assert_columnar_eq(&result, &scalar(Some(false)));
                }
            }

            // Null propagation: a & null == null
            proptest! {
                #[test]
                fn null_propagation(a in arb_columnar_value()) {
                    let result = and(&a, &scalar(None)).unwrap();
                    assert_columnar_eq(&result, &scalar(None));

                    let result = and(&scalar(None), &a).unwrap();
                    assert_columnar_eq(&result, &scalar(None));
                }
            }

            // Idempotence: a & a == a
            proptest! {
                #[test]
                fn idempotence(a in arb_columnar_value()) {
                    let result = and(&a, &a).unwrap();
                    assert_columnar_eq(&result, &a);
                }
            }

            // Associativity: (a & b) & c == a & (b & c)
            // Note: while intermediate types may differ (scalar vs array), the final values are equal
            proptest! {
                #[test]
                fn associativity(a in arb_columnar_value(), b in arb_columnar_value(), c in arb_columnar_value()) {
                    let ab = and(&a, &b).unwrap();
                    let ab_c = and(&ab, &c).unwrap();

                    let bc = and(&b, &c).unwrap();
                    let a_bc = and(&a, &bc).unwrap();

                    assert_columnar_eq(&ab_c, &a_bc);
                }
            }
        }
    }
}
