// ProgressiveEvalExec (step 1 in https://docs.google.com/document/d/1x1yf9ggyxD4JPT8Gf9YlIKxUawqoKTJ1HFyTbGin9xY/edit)
// This will be moved to DF once it is ready

//! Defines the progressive eval plan

use std::any::Any;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion::common::{internal_err, DataFusionError, Result};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::{EquivalenceProperties, PhysicalSortExpr, PhysicalSortRequirement};
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::stream::RecordBatchReceiverStream;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, Distribution, ExecutionPlan, Partitioning, RecordBatchStream,
    SendableRecordBatchStream, Statistics,
};
use datafusion::scalar::ScalarValue;
use futures::{ready, Stream, StreamExt};
use std::pin::Pin;
use std::task::{Context, Poll};

use observability_deps::tracing::{debug, trace};

/// ProgressiveEval return a stream of record batches in the order of its inputs.
/// It will stop when the number of output rows reach the given limit.
///
/// This takes an input execution plan and a number n, and provided each partition of
/// the input plan is in an expected order, this operator will return top record batches that covers the top n rows
/// in the order of the input plan.
///
/// ```text
/// ┌─────────────────────────┐
/// │ ┌───┬───┬───┬───┐       │
/// │ │ A │ B │ C │ D │       │──┐
/// │ └───┴───┴───┴───┘       │  │
/// └─────────────────────────┘  │  ┌───────────────────┐    ┌───────────────────────────────┐
///   Stream 1                   │  │                   │    │ ┌───┬───╦═══╦───┬───╦═══╗     │
///                              ├─▶│  ProgressiveEval  │───▶│ │ A │ B ║ C ║ D │ M ║ N ║ ... │
///                              │  │                   │    │ └───┴─▲─╩═══╩───┴───╩═══╝     │
/// ┌─────────────────────────┐  │  └───────────────────┘    └─┬─────┴───────────────────────┘
/// │ ╔═══╦═══╗               │  │
/// │ ║ M ║ N ║               │──┘                             │
/// │ ╚═══╩═══╝               │                Output only include top record batches that cover top N rows
/// └─────────────────────────┘                
///   Stream 2
///
///
///  Input Streams                                             Output stream
///  (in some order)                                           (in same order)
/// ```
#[derive(Debug)]
pub(crate) struct ProgressiveEvalExec {
    /// Input plan
    input: Arc<dyn ExecutionPlan>,

    /// Corresponding value ranges of the input plan
    /// None if the value ranges are not available
    value_ranges: Option<Vec<(ScalarValue, ScalarValue)>>,

    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,

    /// Optional number of rows to fetch. Stops producing rows after this fetch
    fetch: Option<usize>,
}

impl ProgressiveEvalExec {
    /// Create a new progressive execution plan
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        value_ranges: Option<Vec<(ScalarValue, ScalarValue)>>,
        fetch: Option<usize>,
    ) -> Self {
        Self {
            input,
            value_ranges,
            metrics: ExecutionPlanMetricsSet::new(),
            fetch,
        }
    }

    /// Input schema
    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }
}

impl DisplayAs for ProgressiveEvalExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "ProgressiveEvalExec: ")?;
                if let Some(fetch) = self.fetch {
                    write!(f, "fetch={fetch}, ")?;
                };
                if let Some(value_ranges) = &self.value_ranges {
                    write!(f, "input_ranges={value_ranges:?}")?;
                };

                Ok(())
            }
        }
    }
}

impl ExecutionPlan for ProgressiveEvalExec {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.input.schema()
    }

    /// Get the output partitioning of this plan
    fn output_partitioning(&self) -> Partitioning {
        // This node serializes all the data to a single partition
        Partitioning::UnknownPartitioning(1)
    }

    /// Specifies whether this plan generates an infinite stream of records.
    /// If the plan does not support pipelining, but its input(s) are
    /// infinite, returns an error to indicate this.
    fn unbounded_output(&self, children: &[bool]) -> Result<bool> {
        Ok(children[0])
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![Distribution::UnspecifiedDistribution]
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
    }

    fn required_input_ordering(&self) -> Vec<Option<Vec<PhysicalSortRequirement>>> {
        self.input()
            .output_ordering()
            .map(|_| None)
            .into_iter()
            .collect()
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        self.input.output_ordering()
    }

    /// ProgressiveEvalExec will only accept sorted input
    /// and will maintain the input order
    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![Arc::<dyn ExecutionPlan>::clone(&self.input)]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self::new(
            Arc::<dyn ExecutionPlan>::clone(&children[0]),
            self.value_ranges.clone(),
            self.fetch,
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        trace!(
            "Start ProgressiveEvalExec::execute for partition: {}",
            partition
        );
        if 0 != partition {
            return internal_err!("ProgressiveEvalExec invalid partition {partition}");
        }

        let input_partitions = self.input.output_partitioning().partition_count();
        trace!(
            "Number of input partitions of  ProgressiveEvalExec::execute: {}",
            input_partitions
        );
        let schema = self.schema();

        // Have the input streams run in parallel
        // todo: maybe in the future we do not need this parallelism if number of fecthed rows is in the fitst stream
        let receivers = (0..input_partitions)
            .map(|partition| {
                let stream = self
                    .input
                    .execute(partition, Arc::<TaskContext>::clone(&context))?;

                Ok(spawn_buffered(stream, 1))
            })
            .collect::<Result<_>>()?;

        debug!("Done setting up sender-receiver for ProgressiveEvalExec::execute");

        let result = ProgressiveEvalStream::new(
            receivers,
            schema,
            BaselineMetrics::new(&self.metrics, partition),
            self.fetch,
        )?;

        debug!("Got stream result from ProgressiveEvalStream::new_from_receivers");

        Ok(Box::pin(result))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn statistics(&self) -> Result<Statistics, DataFusionError> {
        self.input.statistics()
    }

    fn equivalence_properties(&self) -> EquivalenceProperties {
        // progressive eval does not change the equivalence properties of its input
        self.input.equivalence_properties()
    }
}

/// Concat input streams until reaching the fetch limit
struct ProgressiveEvalStream {
    /// input streams
    input_streams: Vec<SendableRecordBatchStream>,

    /// The schema of the input and output.
    schema: SchemaRef,

    /// used to record execution metrics
    metrics: BaselineMetrics,

    /// Index of current stream
    current_stream_idx: usize,

    /// If the stream has encountered an error
    aborted: bool,

    /// Optional number of rows to fetch
    fetch: Option<usize>,

    /// number of rows produced
    produced: usize,
}

impl ProgressiveEvalStream {
    fn new(
        input_streams: Vec<SendableRecordBatchStream>,
        schema: SchemaRef,
        metrics: BaselineMetrics,
        fetch: Option<usize>,
    ) -> Result<Self> {
        Ok(Self {
            input_streams,
            schema,
            metrics,
            current_stream_idx: 0,
            aborted: false,
            fetch,
            produced: 0,
        })
    }
}

impl Stream for ProgressiveEvalStream {
    type Item = Result<RecordBatch>;

    // Return the next record batch until reaching the fetch limit or the end of all input streams
    // Return pending if the next record batch is not ready
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // Error in previous poll
        if self.aborted {
            return Poll::Ready(None);
        }

        // Have reached the fetch limit
        if self.produced >= self.fetch.unwrap_or(std::usize::MAX) {
            return Poll::Ready(None);
        }

        // Have reached the end of all input streams
        if self.current_stream_idx >= self.input_streams.len() {
            return Poll::Ready(None);
        }

        // Get next record batch
        let mut poll;
        loop {
            let idx = self.current_stream_idx;
            poll = self.input_streams[idx].poll_next_unpin(cx);
            match poll {
                // This input stream no longer has data, move to next stream
                Poll::Ready(None) => {
                    self.current_stream_idx += 1;
                    if self.current_stream_idx >= self.input_streams.len() {
                        break;
                    }
                }
                _ => break,
            }
        }

        let poll = match ready!(poll) {
            // This input stream has data, return its next record batch
            Some(Ok(batch)) => {
                self.produced += batch.num_rows();
                Poll::Ready(Some(Ok(batch)))
            }
            // This input stream has an error, return the error and set aborted to true to stop polling next round
            Some(Err(e)) => {
                self.aborted = true;
                Poll::Ready(Some(Err(e)))
            }
            // This input stream has no more data, return None (aka finished)
            None => {
                // Reaching here means data of all streams have read
                assert!(
                    self.current_stream_idx >= self.input_streams.len(),
                    "ProgressiveEvalStream::poll_next should not return None before all input streams are read",);

                Poll::Ready(None)
            }
        };

        self.metrics.record_poll(poll)
    }
}

impl RecordBatchStream for ProgressiveEvalStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

// todo: this is a copy from DF code. When this ProgressiveEval operator is moved to DF, this can be removed
/// If running in a tokio context spawns the execution of `stream` to a separate task
/// allowing it to execute in parallel with an intermediate buffer of size `buffer`
pub(crate) fn spawn_buffered(
    mut input: SendableRecordBatchStream,
    buffer: usize,
) -> SendableRecordBatchStream {
    // Use tokio only if running from a multi-thread tokio context
    match tokio::runtime::Handle::try_current() {
        Ok(handle) if handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread => {
            let mut builder = RecordBatchReceiverStream::builder(input.schema(), buffer);

            let sender = builder.tx();

            builder.spawn(async move {
                while let Some(item) = input.next().await {
                    if sender.send(item).await.is_err() {
                        // receiver dropped when query is shutdown early (e.g., limit) or error,
                        // no need to return propagate the send error.
                        return Ok(());
                    }
                }

                Ok(())
            });

            builder.build()
        }
        _ => input,
    }
}

#[cfg(test)]
mod tests {
    use std::iter::FromIterator;
    use std::sync::Weak;

    use arrow::array::ArrayRef;
    use arrow::array::{Int32Array, StringArray, TimestampNanosecondArray};
    use arrow::datatypes::Schema;
    use arrow::datatypes::{DataType, Field};
    use arrow::record_batch::RecordBatch;
    use datafusion::assert_batches_eq;
    use datafusion::physical_plan::collect;
    use datafusion::physical_plan::memory::MemoryExec;
    use datafusion::physical_plan::metrics::{MetricValue, Timestamp};
    use futures::{Future, FutureExt};

    use super::*;

    #[tokio::test]
    async fn test_no_input_stream() {
        let task_ctx = Arc::new(TaskContext::default());
        _test_progressive_eval(
            &[],
            None,
            None, // no fetch limit --> return all rows
            &["++", "++"],
            task_ctx,
        )
        .await;
    }

    #[tokio::test]
    async fn test_one_input_stream() {
        let task_ctx = Arc::new(TaskContext::default());
        let a: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 7, 9, 3]));
        let b: ArrayRef = Arc::new(StringArray::from_iter(vec![
            Some("a"),
            Some("c"),
            Some("e"),
            Some("g"),
            Some("j"),
        ]));
        let c: ArrayRef = Arc::new(TimestampNanosecondArray::from(vec![8, 7, 6, 5, 8]));
        let b1 = RecordBatch::try_from_iter(vec![("a", a), ("b", b), ("c", c)]).unwrap();

        // return all
        _test_progressive_eval(
            &[vec![b1.clone()]],
            None,
            None, // no fetch limit --> return all rows
            &[
                "+---+---+-------------------------------+",
                "| a | b | c                             |",
                "+---+---+-------------------------------+",
                "| 1 | a | 1970-01-01T00:00:00.000000008 |",
                "| 2 | c | 1970-01-01T00:00:00.000000007 |",
                "| 7 | e | 1970-01-01T00:00:00.000000006 |",
                "| 9 | g | 1970-01-01T00:00:00.000000005 |",
                "| 3 | j | 1970-01-01T00:00:00.000000008 |",
                "+---+---+-------------------------------+",
            ],
            Arc::clone(&task_ctx),
        )
        .await;

        // fetch no rows
        _test_progressive_eval(
            &[vec![b1.clone()]],
            None,
            Some(0),
            &["++", "++"],
            Arc::clone(&task_ctx),
        )
        .await;

        // still return all even select 3 rows becasue first record batch is returned
        _test_progressive_eval(
            &[vec![b1.clone()]],
            None,
            Some(3),
            &[
                "+---+---+-------------------------------+",
                "| a | b | c                             |",
                "+---+---+-------------------------------+",
                "| 1 | a | 1970-01-01T00:00:00.000000008 |",
                "| 2 | c | 1970-01-01T00:00:00.000000007 |",
                "| 7 | e | 1970-01-01T00:00:00.000000006 |",
                "| 9 | g | 1970-01-01T00:00:00.000000005 |",
                "| 3 | j | 1970-01-01T00:00:00.000000008 |",
                "+---+---+-------------------------------+",
            ],
            Arc::clone(&task_ctx),
        )
        .await;

        // return all because fetch limit is larger
        _test_progressive_eval(
            &[vec![b1.clone()]],
            None,
            Some(7),
            &[
                "+---+---+-------------------------------+",
                "| a | b | c                             |",
                "+---+---+-------------------------------+",
                "| 1 | a | 1970-01-01T00:00:00.000000008 |",
                "| 2 | c | 1970-01-01T00:00:00.000000007 |",
                "| 7 | e | 1970-01-01T00:00:00.000000006 |",
                "| 9 | g | 1970-01-01T00:00:00.000000005 |",
                "| 3 | j | 1970-01-01T00:00:00.000000008 |",
                "+---+---+-------------------------------+",
            ],
            Arc::clone(&task_ctx),
        )
        .await;
    }

    #[tokio::test]
    async fn test_return_all() {
        let task_ctx = Arc::new(TaskContext::default());
        let a: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 7, 9, 3]));
        let b: ArrayRef = Arc::new(StringArray::from_iter(vec![
            Some("a"),
            Some("c"),
            Some("e"),
            Some("g"),
            Some("j"),
        ]));
        let c: ArrayRef = Arc::new(TimestampNanosecondArray::from(vec![8, 7, 6, 5, 8]));
        let b1 = RecordBatch::try_from_iter(vec![("a", a), ("b", b), ("c", c)]).unwrap();

        let a: ArrayRef = Arc::new(Int32Array::from(vec![10, 20, 70, 90, 30]));
        let b: ArrayRef = Arc::new(StringArray::from_iter(vec![
            Some("b"),
            Some("d"),
            Some("f"),
            Some("h"),
            Some("j"),
        ]));
        let c: ArrayRef = Arc::new(TimestampNanosecondArray::from(vec![4, 6, 2, 2, 6]));
        let b2 = RecordBatch::try_from_iter(vec![("a", a), ("b", b), ("c", c)]).unwrap();

        // [b1, b2]
        _test_progressive_eval(
            &[vec![b1.clone()], vec![b2.clone()]],
            None,
            None, // no fetch limit --> return all rows
            &[
                "+----+---+-------------------------------+",
                "| a  | b | c                             |",
                "+----+---+-------------------------------+",
                "| 1  | a | 1970-01-01T00:00:00.000000008 |",
                "| 2  | c | 1970-01-01T00:00:00.000000007 |",
                "| 7  | e | 1970-01-01T00:00:00.000000006 |",
                "| 9  | g | 1970-01-01T00:00:00.000000005 |",
                "| 3  | j | 1970-01-01T00:00:00.000000008 |",
                "| 10 | b | 1970-01-01T00:00:00.000000004 |",
                "| 20 | d | 1970-01-01T00:00:00.000000006 |",
                "| 70 | f | 1970-01-01T00:00:00.000000002 |",
                "| 90 | h | 1970-01-01T00:00:00.000000002 |",
                "| 30 | j | 1970-01-01T00:00:00.000000006 |",
                "+----+---+-------------------------------+",
            ],
            Arc::clone(&task_ctx),
        )
        .await;

        // [b2, b1]
        _test_progressive_eval(
            &[vec![b2], vec![b1]],
            None,
            None,
            &[
                "+----+---+-------------------------------+",
                "| a  | b | c                             |",
                "+----+---+-------------------------------+",
                "| 10 | b | 1970-01-01T00:00:00.000000004 |",
                "| 20 | d | 1970-01-01T00:00:00.000000006 |",
                "| 70 | f | 1970-01-01T00:00:00.000000002 |",
                "| 90 | h | 1970-01-01T00:00:00.000000002 |",
                "| 30 | j | 1970-01-01T00:00:00.000000006 |",
                "| 1  | a | 1970-01-01T00:00:00.000000008 |",
                "| 2  | c | 1970-01-01T00:00:00.000000007 |",
                "| 7  | e | 1970-01-01T00:00:00.000000006 |",
                "| 9  | g | 1970-01-01T00:00:00.000000005 |",
                "| 3  | j | 1970-01-01T00:00:00.000000008 |",
                "+----+---+-------------------------------+",
            ],
            task_ctx,
        )
        .await;
    }

    #[tokio::test]
    async fn test_return_all_on_different_length_batches() {
        let task_ctx = Arc::new(TaskContext::default());
        let a: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 7, 9, 3]));
        let b: ArrayRef = Arc::new(StringArray::from_iter(vec![
            Some("a"),
            Some("b"),
            Some("c"),
            Some("d"),
            Some("e"),
        ]));
        let c: ArrayRef = Arc::new(TimestampNanosecondArray::from(vec![8, 7, 6, 5, 8]));
        let b1 = RecordBatch::try_from_iter(vec![("a", a), ("b", b), ("c", c)]).unwrap();

        let a: ArrayRef = Arc::new(Int32Array::from(vec![70, 90, 30]));
        let b: ArrayRef = Arc::new(StringArray::from_iter(vec![
            Some("c"),
            Some("d"),
            Some("e"),
        ]));
        let c: ArrayRef = Arc::new(TimestampNanosecondArray::from(vec![4, 6, 2]));
        let b2 = RecordBatch::try_from_iter(vec![("a", a), ("b", b), ("c", c)]).unwrap();

        // [b1, b2]
        _test_progressive_eval(
            &[vec![b1.clone()], vec![b2.clone()]],
            None,
            None,
            &[
                "+----+---+-------------------------------+",
                "| a  | b | c                             |",
                "+----+---+-------------------------------+",
                "| 1  | a | 1970-01-01T00:00:00.000000008 |",
                "| 2  | b | 1970-01-01T00:00:00.000000007 |",
                "| 7  | c | 1970-01-01T00:00:00.000000006 |",
                "| 9  | d | 1970-01-01T00:00:00.000000005 |",
                "| 3  | e | 1970-01-01T00:00:00.000000008 |",
                "| 70 | c | 1970-01-01T00:00:00.000000004 |",
                "| 90 | d | 1970-01-01T00:00:00.000000006 |",
                "| 30 | e | 1970-01-01T00:00:00.000000002 |",
                "+----+---+-------------------------------+",
            ],
            Arc::clone(&task_ctx),
        )
        .await;

        // [b2, b1]
        _test_progressive_eval(
            &[vec![b2], vec![b1]],
            None,
            None,
            &[
                "+----+---+-------------------------------+",
                "| a  | b | c                             |",
                "+----+---+-------------------------------+",
                "| 70 | c | 1970-01-01T00:00:00.000000004 |",
                "| 90 | d | 1970-01-01T00:00:00.000000006 |",
                "| 30 | e | 1970-01-01T00:00:00.000000002 |",
                "| 1  | a | 1970-01-01T00:00:00.000000008 |",
                "| 2  | b | 1970-01-01T00:00:00.000000007 |",
                "| 7  | c | 1970-01-01T00:00:00.000000006 |",
                "| 9  | d | 1970-01-01T00:00:00.000000005 |",
                "| 3  | e | 1970-01-01T00:00:00.000000008 |",
                "+----+---+-------------------------------+",
            ],
            task_ctx,
        )
        .await;
    }

    #[tokio::test]
    async fn test_fetch_limit_1() {
        let task_ctx = Arc::new(TaskContext::default());
        let a: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 7, 9, 3]));
        let b: ArrayRef = Arc::new(StringArray::from_iter(vec![
            Some("a"),
            Some("b"),
            Some("c"),
            Some("d"),
            Some("e"),
        ]));
        let c: ArrayRef = Arc::new(TimestampNanosecondArray::from(vec![8, 7, 6, 5, 8]));
        let b1 = RecordBatch::try_from_iter(vec![("a", a), ("b", b), ("c", c)]).unwrap();

        let a: ArrayRef = Arc::new(Int32Array::from(vec![70, 90, 30]));
        let b: ArrayRef = Arc::new(StringArray::from_iter(vec![
            Some("c"),
            Some("d"),
            Some("e"),
        ]));
        let c: ArrayRef = Arc::new(TimestampNanosecondArray::from(vec![4, 6, 2]));
        let b2 = RecordBatch::try_from_iter(vec![("a", a), ("b", b), ("c", c)]).unwrap();

        // [b2, b1]
        // b2 has 3 rows. b1 has 5 rows
        // Fetch limit is 1 --> return all 3 rows of the first batch (b2) that covers that limit
        _test_progressive_eval(
            &[vec![b2.clone()], vec![b1.clone()]],
            None,
            Some(1),
            &[
                "+----+---+-------------------------------+",
                "| a  | b | c                             |",
                "+----+---+-------------------------------+",
                "| 70 | c | 1970-01-01T00:00:00.000000004 |",
                "| 90 | d | 1970-01-01T00:00:00.000000006 |",
                "| 30 | e | 1970-01-01T00:00:00.000000002 |",
                "+----+---+-------------------------------+",
            ],
            Arc::clone(&task_ctx),
        )
        .await;

        // [b1, b2]
        // b1 has 5 rows. b2 has 3 rows
        // Fetch limit is 1 --> return all 5 rows of the first batch (b1) that covers that limit
        _test_progressive_eval(
            &[vec![b1], vec![b2]],
            None,
            Some(1),
            &[
                "+---+---+-------------------------------+",
                "| a | b | c                             |",
                "+---+---+-------------------------------+",
                "| 1 | a | 1970-01-01T00:00:00.000000008 |",
                "| 2 | b | 1970-01-01T00:00:00.000000007 |",
                "| 7 | c | 1970-01-01T00:00:00.000000006 |",
                "| 9 | d | 1970-01-01T00:00:00.000000005 |",
                "| 3 | e | 1970-01-01T00:00:00.000000008 |",
                "+---+---+-------------------------------+",
            ],
            task_ctx,
        )
        .await;
    }

    #[tokio::test]
    async fn test_fetch_limit_equal_first_batch_size() {
        let task_ctx = Arc::new(TaskContext::default());
        let a: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 7, 9, 3]));
        let b: ArrayRef = Arc::new(StringArray::from_iter(vec![
            Some("a"),
            Some("b"),
            Some("c"),
            Some("d"),
            Some("e"),
        ]));
        let c: ArrayRef = Arc::new(TimestampNanosecondArray::from(vec![8, 7, 6, 5, 8]));
        let b1 = RecordBatch::try_from_iter(vec![("a", a), ("b", b), ("c", c)]).unwrap();

        let a: ArrayRef = Arc::new(Int32Array::from(vec![70, 90, 30]));
        let b: ArrayRef = Arc::new(StringArray::from_iter(vec![
            Some("c"),
            Some("d"),
            Some("e"),
        ]));
        let c: ArrayRef = Arc::new(TimestampNanosecondArray::from(vec![4, 6, 2]));
        let b2 = RecordBatch::try_from_iter(vec![("a", a), ("b", b), ("c", c)]).unwrap();

        // [b2, b1]
        // b2 has 3 rows. b1 has 5 rows
        // Fetch limit is 3 --> return all 3 rows of the first batch (b2) that covers that limit
        _test_progressive_eval(
            &[vec![b2.clone()], vec![b1.clone()]],
            None,
            Some(3),
            &[
                "+----+---+-------------------------------+",
                "| a  | b | c                             |",
                "+----+---+-------------------------------+",
                "| 70 | c | 1970-01-01T00:00:00.000000004 |",
                "| 90 | d | 1970-01-01T00:00:00.000000006 |",
                "| 30 | e | 1970-01-01T00:00:00.000000002 |",
                "+----+---+-------------------------------+",
            ],
            Arc::clone(&task_ctx),
        )
        .await;

        // [b1, b2]
        // b1 has 5 rows. b2 has 3 rows
        // Fetch limit is 5 --> return all 5 rows of first batch (b1) that covers that limit
        _test_progressive_eval(
            &[vec![b1], vec![b2]],
            None,
            Some(5),
            &[
                "+---+---+-------------------------------+",
                "| a | b | c                             |",
                "+---+---+-------------------------------+",
                "| 1 | a | 1970-01-01T00:00:00.000000008 |",
                "| 2 | b | 1970-01-01T00:00:00.000000007 |",
                "| 7 | c | 1970-01-01T00:00:00.000000006 |",
                "| 9 | d | 1970-01-01T00:00:00.000000005 |",
                "| 3 | e | 1970-01-01T00:00:00.000000008 |",
                "+---+---+-------------------------------+",
            ],
            task_ctx,
        )
        .await;
    }

    #[tokio::test]
    async fn test_fetch_limit_over_first_batch_size() {
        let task_ctx = Arc::new(TaskContext::default());
        let a: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 7, 9, 3]));
        let b: ArrayRef = Arc::new(StringArray::from_iter(vec![
            Some("a"),
            Some("b"),
            Some("c"),
            Some("d"),
            Some("e"),
        ]));
        let c: ArrayRef = Arc::new(TimestampNanosecondArray::from(vec![8, 7, 6, 5, 8]));
        let b1 = RecordBatch::try_from_iter(vec![("a", a), ("b", b), ("c", c)]).unwrap();

        let a: ArrayRef = Arc::new(Int32Array::from(vec![70, 90, 30]));
        let b: ArrayRef = Arc::new(StringArray::from_iter(vec![
            Some("c"),
            Some("d"),
            Some("e"),
        ]));
        let c: ArrayRef = Arc::new(TimestampNanosecondArray::from(vec![4, 6, 2]));
        let b2 = RecordBatch::try_from_iter(vec![("a", a), ("b", b), ("c", c)]).unwrap();

        // [b2, b1]
        // b2 has 3 rows. b1 has 5 rows
        // Fetch limit is 4 --> return all rows of both batches in the order of b2, b1
        _test_progressive_eval(
            &[vec![b2.clone()], vec![b1.clone()]],
            None,
            Some(4),
            &[
                "+----+---+-------------------------------+",
                "| a  | b | c                             |",
                "+----+---+-------------------------------+",
                "| 70 | c | 1970-01-01T00:00:00.000000004 |",
                "| 90 | d | 1970-01-01T00:00:00.000000006 |",
                "| 30 | e | 1970-01-01T00:00:00.000000002 |",
                "| 1  | a | 1970-01-01T00:00:00.000000008 |",
                "| 2  | b | 1970-01-01T00:00:00.000000007 |",
                "| 7  | c | 1970-01-01T00:00:00.000000006 |",
                "| 9  | d | 1970-01-01T00:00:00.000000005 |",
                "| 3  | e | 1970-01-01T00:00:00.000000008 |",
                "+----+---+-------------------------------+",
            ],
            Arc::clone(&task_ctx),
        )
        .await;

        // [b1, b2]
        // b1 has 5 rows. b2 has 3 rows
        // Fetch limit is 6 --> return all rows of both batches in the order of b1, b2
        _test_progressive_eval(
            &[vec![b1], vec![b2]],
            None,
            Some(6),
            &[
                "+----+---+-------------------------------+",
                "| a  | b | c                             |",
                "+----+---+-------------------------------+",
                "| 1  | a | 1970-01-01T00:00:00.000000008 |",
                "| 2  | b | 1970-01-01T00:00:00.000000007 |",
                "| 7  | c | 1970-01-01T00:00:00.000000006 |",
                "| 9  | d | 1970-01-01T00:00:00.000000005 |",
                "| 3  | e | 1970-01-01T00:00:00.000000008 |",
                "| 70 | c | 1970-01-01T00:00:00.000000004 |",
                "| 90 | d | 1970-01-01T00:00:00.000000006 |",
                "| 30 | e | 1970-01-01T00:00:00.000000002 |",
                "+----+---+-------------------------------+",
            ],
            task_ctx,
        )
        .await;
    }

    #[tokio::test]
    async fn test_three_partitions_with_nulls() {
        let task_ctx = Arc::new(TaskContext::default());
        let a: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 7, 9, 3]));
        let b: ArrayRef = Arc::new(StringArray::from_iter(vec![
            Some("a"),
            Some("b"),
            Some("c"),
            None,
            Some("f"),
        ]));
        let c: ArrayRef = Arc::new(TimestampNanosecondArray::from(vec![8, 7, 6, 5, 8]));
        let b1 = RecordBatch::try_from_iter(vec![("a", a), ("b", b), ("c", c)]).unwrap();

        let a: ArrayRef = Arc::new(Int32Array::from(vec![10, 20, 70]));
        let b: ArrayRef = Arc::new(StringArray::from_iter(vec![
            Some("e"),
            Some("g"),
            Some("h"),
        ]));
        let c: ArrayRef = Arc::new(TimestampNanosecondArray::from(vec![40, 60, 20]));
        let b2 = RecordBatch::try_from_iter(vec![("a", a), ("b", b), ("c", c)]).unwrap();

        let a: ArrayRef = Arc::new(Int32Array::from(vec![100, 200, 700, 900]));
        let b: ArrayRef = Arc::new(StringArray::from_iter(vec![
            None,
            Some("g"),
            Some("h"),
            Some("i"),
        ]));
        let c: ArrayRef = Arc::new(TimestampNanosecondArray::from(vec![4, 6, 2, 2]));
        let b3 = RecordBatch::try_from_iter(vec![("a", a), ("b", b), ("c", c)]).unwrap();

        // [b1, b2, b3]
        // b1 has 5 rows. b2 has 3 rows. b3 has 4 rows
        // Fetch limit is 1 --> return all rows of the b1
        _test_progressive_eval(
            &[vec![b1.clone()], vec![b2.clone()], vec![b3.clone()]],
            None,
            Some(1),
            &[
                "+---+---+-------------------------------+",
                "| a | b | c                             |",
                "+---+---+-------------------------------+",
                "| 1 | a | 1970-01-01T00:00:00.000000008 |",
                "| 2 | b | 1970-01-01T00:00:00.000000007 |",
                "| 7 | c | 1970-01-01T00:00:00.000000006 |",
                "| 9 |   | 1970-01-01T00:00:00.000000005 |",
                "| 3 | f | 1970-01-01T00:00:00.000000008 |",
                "+---+---+-------------------------------+",
            ],
            Arc::clone(&task_ctx),
        )
        .await;

        // [b1, b2, b3]
        // b1 has 5 rows. b2 has 3 rows. b3 has 4 rows
        // Fetch limit is 7 --> return all rows of the b1 & b2 in the order of b1, b2
        _test_progressive_eval(
            &[vec![b1.clone()], vec![b2.clone()], vec![b3.clone()]],
            None,
            Some(7),
            &[
                "+----+---+-------------------------------+",
                "| a  | b | c                             |",
                "+----+---+-------------------------------+",
                "| 1  | a | 1970-01-01T00:00:00.000000008 |",
                "| 2  | b | 1970-01-01T00:00:00.000000007 |",
                "| 7  | c | 1970-01-01T00:00:00.000000006 |",
                "| 9  |   | 1970-01-01T00:00:00.000000005 |",
                "| 3  | f | 1970-01-01T00:00:00.000000008 |",
                "| 10 | e | 1970-01-01T00:00:00.000000040 |",
                "| 20 | g | 1970-01-01T00:00:00.000000060 |",
                "| 70 | h | 1970-01-01T00:00:00.000000020 |",
                "+----+---+-------------------------------+",
            ],
            Arc::clone(&task_ctx),
        )
        .await;

        // [b1, b2, b3]
        // b1 has 5 rows. b2 has 3 rows. b3 has 4 rows
        // Fetch limit is 50 --> return all rows of all batches in the order of b1, b2, b3
        _test_progressive_eval(
            &[vec![b1], vec![b2], vec![b3]],
            None,
            Some(50),
            &[
                "+-----+---+-------------------------------+",
                "| a   | b | c                             |",
                "+-----+---+-------------------------------+",
                "| 1   | a | 1970-01-01T00:00:00.000000008 |",
                "| 2   | b | 1970-01-01T00:00:00.000000007 |",
                "| 7   | c | 1970-01-01T00:00:00.000000006 |",
                "| 9   |   | 1970-01-01T00:00:00.000000005 |",
                "| 3   | f | 1970-01-01T00:00:00.000000008 |",
                "| 10  | e | 1970-01-01T00:00:00.000000040 |",
                "| 20  | g | 1970-01-01T00:00:00.000000060 |",
                "| 70  | h | 1970-01-01T00:00:00.000000020 |",
                "| 100 |   | 1970-01-01T00:00:00.000000004 |",
                "| 200 | g | 1970-01-01T00:00:00.000000006 |",
                "| 700 | h | 1970-01-01T00:00:00.000000002 |",
                "| 900 | i | 1970-01-01T00:00:00.000000002 |",
                "+-----+---+-------------------------------+",
            ],
            task_ctx,
        )
        .await;
    }

    async fn _test_progressive_eval(
        partitions: &[Vec<RecordBatch>],
        value_ranges: Option<Vec<(ScalarValue, ScalarValue)>>,
        fetch: Option<usize>,
        exp: &[&str],
        context: Arc<TaskContext>,
    ) {
        let schema = if partitions.is_empty() {
            // just whatwever schema
            let a: ArrayRef = Arc::new(Int32Array::from(vec![1, 2]));
            let batch = RecordBatch::try_from_iter(vec![("a", a)]).unwrap();
            batch.schema()
        } else {
            partitions[0][0].schema()
        };

        let exec = MemoryExec::try_new(partitions, schema, None).unwrap();
        let progressive = Arc::new(ProgressiveEvalExec::new(
            Arc::new(exec),
            value_ranges,
            fetch,
        ));

        let collected = collect(progressive, context).await.unwrap();
        assert_batches_eq!(exp, collected.as_slice());
    }

    #[tokio::test]
    async fn test_merge_metrics() {
        let task_ctx = Arc::new(TaskContext::default());
        let a: ArrayRef = Arc::new(Int32Array::from(vec![1, 2]));
        let b: ArrayRef = Arc::new(StringArray::from_iter(vec![Some("a"), Some("c")]));
        let b1 = RecordBatch::try_from_iter(vec![("a", a), ("b", b)]).unwrap();

        let a: ArrayRef = Arc::new(Int32Array::from(vec![10, 20]));
        let b: ArrayRef = Arc::new(StringArray::from_iter(vec![Some("b"), Some("d")]));
        let b2 = RecordBatch::try_from_iter(vec![("a", a), ("b", b)]).unwrap();

        let schema = b1.schema();
        let exec = MemoryExec::try_new(&[vec![b1], vec![b2]], schema, None).unwrap();
        let progressive = Arc::new(ProgressiveEvalExec::new(Arc::new(exec), None, None));

        let collected = collect(Arc::<ProgressiveEvalExec>::clone(&progressive), task_ctx)
            .await
            .unwrap();
        let expected = [
            "+----+---+",
            "| a  | b |",
            "+----+---+",
            "| 1  | a |",
            "| 2  | c |",
            "| 10 | b |",
            "| 20 | d |",
            "+----+---+",
        ];
        assert_batches_eq!(expected, collected.as_slice());

        // Now, validate metrics
        let metrics = progressive.metrics().unwrap();

        assert_eq!(metrics.output_rows().unwrap(), 4);
        assert!(metrics.elapsed_compute().unwrap() > 0);

        let mut saw_start = false;
        let mut saw_end = false;
        metrics.iter().for_each(|m| match m.value() {
            MetricValue::StartTimestamp(ts) => {
                saw_start = true;
                assert!(nanos_from_timestamp(ts) > 0);
            }
            MetricValue::EndTimestamp(ts) => {
                saw_end = true;
                assert!(nanos_from_timestamp(ts) > 0);
            }
            _ => {}
        });

        assert!(saw_start);
        assert!(saw_end);
    }

    fn nanos_from_timestamp(ts: &Timestamp) -> i64 {
        ts.value().unwrap().timestamp_nanos_opt().unwrap()
    }

    #[tokio::test]
    async fn test_drop_cancel() -> Result<()> {
        let task_ctx = Arc::new(TaskContext::default());
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Float32, true)]));

        let blocking_exec = Arc::new(BlockingExec::new(Arc::clone(&schema), 2));
        let refs = blocking_exec.refs();
        let progressive_exec = Arc::new(ProgressiveEvalExec::new(blocking_exec, None, None));

        let fut = collect(progressive_exec, task_ctx);
        let mut fut = fut.boxed();

        assert_is_pending(&mut fut);
        drop(fut);
        assert_strong_count_converges_to_zero(refs).await;

        Ok(())
    }

    // todo: this is copied from DF. When we move ProgressiveEval to DF, this will be removed
    /// Asserts that the strong count of the given [`Weak`] pointer converges to zero.
    ///
    /// This might take a while but has a timeout.
    pub async fn assert_strong_count_converges_to_zero<T>(refs: Weak<T>) {
        #![allow(clippy::future_not_send)]
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            loop {
                if Weak::strong_count(&refs) == 0 {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
    }

    // todo: this is copied from DF. When we move ProgressiveEval to DF, this will be removed
    /// Asserts that given future is pending.
    pub fn assert_is_pending<'a, T>(fut: &mut Pin<Box<dyn Future<Output = T> + Send + 'a>>) {
        let waker = futures::task::noop_waker();
        let mut cx = futures::task::Context::from_waker(&waker);
        let poll = fut.poll_unpin(&mut cx);

        assert!(poll.is_pending());
    }

    // todo: this is copied from DF. When we move ProgressiveEval to DF, this will be removed
    /// Execution plan that emits streams that block forever.
    ///
    /// This is useful to test shutdown / cancelation behavior of certain execution plans.
    #[derive(Debug)]
    pub struct BlockingExec {
        /// Schema that is mocked by this plan.
        schema: SchemaRef,

        /// Number of output partitions.
        n_partitions: usize,

        /// Ref-counting helper to check if the plan and the produced stream are still in memory.
        refs: Arc<()>,
    }

    impl BlockingExec {
        /// Create new [`BlockingExec`] with a give schema and number of partitions.
        pub fn new(schema: SchemaRef, n_partitions: usize) -> Self {
            Self {
                schema,
                n_partitions,
                refs: Default::default(),
            }
        }

        /// Weak pointer that can be used for ref-counting this execution plan and its streams.
        ///
        /// Use [`Weak::strong_count`] to determine if the plan itself and its streams are dropped (should be 0 in that
        /// case). Note that tokio might take some time to cancel spawned tasks, so you need to wrap this check into a retry
        /// loop. Use [`assert_strong_count_converges_to_zero`] to archive this.
        pub fn refs(&self) -> Weak<()> {
            Arc::downgrade(&self.refs)
        }
    }

    impl DisplayAs for BlockingExec {
        fn fmt_as(
            &self,
            t: DisplayFormatType,
            f: &mut std::fmt::Formatter<'_>,
        ) -> std::fmt::Result {
            match t {
                DisplayFormatType::Default | DisplayFormatType::Verbose => {
                    write!(f, "BlockingExec",)
                }
            }
        }
    }

    impl ExecutionPlan for BlockingExec {
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn schema(&self) -> SchemaRef {
            Arc::clone(&self.schema)
        }

        fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
            // this is a leaf node and has no children
            vec![]
        }

        fn output_partitioning(&self) -> Partitioning {
            Partitioning::UnknownPartitioning(self.n_partitions)
        }

        fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
            None
        }

        fn with_new_children(
            self: Arc<Self>,
            _: Vec<Arc<dyn ExecutionPlan>>,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            internal_err!("Children cannot be replaced in {self:?}")
        }

        fn execute(
            &self,
            _partition: usize,
            _context: Arc<TaskContext>,
        ) -> Result<SendableRecordBatchStream> {
            Ok(Box::pin(BlockingStream {
                schema: Arc::clone(&self.schema),
                _refs: Arc::clone(&self.refs),
            }))
        }

        fn statistics(&self) -> Result<Statistics> {
            unimplemented!()
        }
    }

    /// A [`RecordBatchStream`] that is pending forever.
    #[derive(Debug)]
    pub struct BlockingStream {
        /// Schema mocked by this stream.
        schema: SchemaRef,

        /// Ref-counting helper to check if the stream are still in memory.
        _refs: Arc<()>,
    }

    impl Stream for BlockingStream {
        type Item = Result<RecordBatch>;

        fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            Poll::Pending
        }
    }

    impl RecordBatchStream for BlockingStream {
        fn schema(&self) -> SchemaRef {
            Arc::clone(&self.schema)
        }
    }
}
