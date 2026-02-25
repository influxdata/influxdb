// ProgressiveEvalExec (step 1 in https://docs.google.com/document/d/1x1yf9ggyxD4JPT8Gf9YlIKxUawqoKTJ1HFyTbGin9xY/edit)
// This will be moved to DF once it is ready

//! Defines the progressive eval plan

use std::any::Any;
use std::borrow::Cow::Borrowed;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion::common::{DataFusionError, Result, internal_err};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::{LexRequirement, OrderingRequirements};
use datafusion::physical_plan::metrics::{
    BaselineMetrics, Count, ExecutionPlanMetricsSet, MetricBuilder, MetricValue, MetricsSet,
};
use datafusion::physical_plan::stream::RecordBatchReceiverStream;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, Distribution, ExecutionPlan, ExecutionPlanProperties, Metric,
    Partitioning, PlanProperties, RecordBatchStream, SendableRecordBatchStream, Statistics,
};
use futures::{Stream, StreamExt, ready};
use std::pin::Pin;
use std::task::{Context, Poll};

use tracing::{debug, trace};

use crate::config::IoxConfigExt;
use crate::physical_optimizer::sort::lexical_range::LexicalRange;

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
#[derive(Debug, Clone)]
pub(crate) struct ProgressiveEvalExec {
    /// Input plan
    input: Arc<dyn ExecutionPlan>,

    /// Corresponding value ranges of the input plan, per partition.
    /// None if the value ranges are not available
    value_ranges: Option<Vec<LexicalRange>>,

    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,

    /// Optional number of rows to fetch. Stops producing rows after this fetch
    fetch: Option<usize>,

    /// Cache holding plan properties like equivalences, output partitioning, output ordering etc.
    cache: PlanProperties,
}

impl ProgressiveEvalExec {
    /// Create a new progressive execution plan
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        value_ranges: Option<Vec<LexicalRange>>,
        fetch: Option<usize>,
    ) -> Self {
        let cache = Self::compute_properties(&input);
        Self {
            input,
            metrics: ExecutionPlanMetricsSet::new(),
            value_ranges,
            fetch,
            cache,
        }
    }

    /// Input schema
    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }

    /// This function creates the cache object that stores the plan properties such as equivalence properties, partitioning, ordering, etc.
    fn compute_properties(input: &Arc<dyn ExecutionPlan>) -> PlanProperties {
        // progressive eval does not change the equivalence properties of its input
        let eq_properties = input.equivalence_properties().clone();

        // This node serializes all the data to a single partition
        let output_partitioning = Partitioning::UnknownPartitioning(1);

        PlanProperties::new(
            eq_properties,
            output_partitioning,
            input.pipeline_behavior(),
            input.boundedness(),
        )
    }
}

impl DisplayAs for ProgressiveEvalExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default
            | DisplayFormatType::Verbose
            | DisplayFormatType::TreeRender => {
                write!(f, "ProgressiveEvalExec: ")?;
                if let Some(fetch) = self.fetch {
                    write!(f, "fetch={fetch}, ")?;
                };

                if let Some(lexical_ranges) = &self.value_ranges {
                    write!(
                        f,
                        "input_ranges=[{}]",
                        lexical_ranges
                            .iter()
                            .map(|r| r.to_string())
                            .collect::<Vec<_>>()
                            .join(", ")
                    )?;
                };

                Ok(())
            }
        }
    }
}

impl ExecutionPlan for ProgressiveEvalExec {
    fn name(&self) -> &str {
        Self::static_name()
    }

    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.input.schema()
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![Distribution::UnspecifiedDistribution]
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
    }

    fn required_input_ordering(&self) -> Vec<Option<OrderingRequirements>> {
        let input_ordering = self
            .input()
            .properties()
            .output_ordering()
            .cloned()
            .map(LexRequirement::from)
            .map(OrderingRequirements::new);

        vec![input_ordering]
    }

    /// ProgressiveEvalExec will only accept sorted input
    /// and will maintain the input order
    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
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

        let input_partitions = self
            .input
            .properties()
            .output_partitioning()
            .partition_count();
        trace!(
            "Number of input partitions of  ProgressiveEvalExec::execute: {}",
            input_partitions
        );
        let schema = self.schema();

        // Add a metric to record the number of inputs
        let num_inputs = Count::new();
        num_inputs.add(
            self.input
                .properties()
                .output_partitioning()
                .partition_count(),
        );
        self.metrics.register(Arc::new(Metric::new(
            MetricValue::Count {
                name: Borrowed("num_inputs"),
                count: num_inputs,
            },
            None,
        )));
        // Add a metric to record the number of inputs that are actually read which is <= num_inputs
        let num_read_inputs_counter =
            MetricBuilder::new(&self.metrics).global_counter("num_read_inputs");
        // Add other base line metrics
        let baseline_metrics = BaselineMetrics::new(&self.metrics, partition);

        let result = ProgressiveEvalStream::new(
            Arc::clone(&self.input),
            Arc::clone(&context),
            schema,
            baseline_metrics,
            num_read_inputs_counter,
            self.fetch,
        )?;

        debug!("Got stream result from ProgressiveEvalStream::new_from_receivers");

        Ok(Box::pin(result))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn statistics(&self) -> Result<Statistics, DataFusionError> {
        self.input.partition_statistics(None)
    }
}

/// Handle when to prefetch input streams and how to poll next record batch
struct InputStreams {
    /// Input plan of the progressive eval exec
    input_plan: Arc<dyn ExecutionPlan>,

    /// Context of the progressive eval exec
    context: Arc<TaskContext>,

    /// Total input streams
    input_stream_count: usize,

    /// Number of input streams to prefetch
    num_input_streams_to_prefetch: usize,

    /// Index of current stream
    current_stream_idx: usize,

    /// Input stream to poll data
    current_input_stream: Option<SendableRecordBatchStream>,

    /// Prefetched Input streams
    prefetched_input_streams: Vec<SendableRecordBatchStream>,

    /// Used to record number of actually read input streams
    num_read_inputs_counter: Count,
}

impl InputStreams {
    fn new(
        input_plan: Arc<dyn ExecutionPlan>,
        context: Arc<TaskContext>,
        num_input_streams_to_prefetch: usize,
        num_read_inputs_counter: Count,
    ) -> Result<Self> {
        let input_stream_count = input_plan
            .properties()
            .output_partitioning()
            .partition_count();

        let current_stream_idx = 0;
        let mut current_input_stream = None;
        let mut capacity = 0;
        // Can be one if there is a single input stream
        if num_input_streams_to_prefetch > 1 {
            capacity = num_input_streams_to_prefetch - 1;
        }
        let mut prefetched_input_streams = Vec::with_capacity(capacity);

        for i in 0..num_input_streams_to_prefetch {
            if i >= input_stream_count {
                break;
            }

            let input_stream = spawn_buffered(
                input_plan.execute(i, Arc::<TaskContext>::clone(&context))?,
                1,
            );
            num_read_inputs_counter.add(1);
            trace!(
                "+1 to num_read_inputs_counter: {}",
                num_read_inputs_counter.value()
            );

            if i == 0 {
                current_input_stream = Some(input_stream);
            } else {
                prefetched_input_streams.push(input_stream);
            }
        }

        Ok(Self {
            input_plan,
            context,
            input_stream_count,
            num_input_streams_to_prefetch,
            current_stream_idx,
            current_input_stream,
            prefetched_input_streams,
            num_read_inputs_counter,
        })
    }

    /// Set next available stream to current_input_stream
    /// Also prefetch one more input stream if not all of them are prefetched yet
    fn next_stream(&mut self) {
        // No more input stream
        if self.current_stream_idx >= self.input_stream_count {
            // panic if we have not reached the end of all input streams
            assert!(
                self.prefetched_input_streams.is_empty(),
                "Internal error in ProgressiveEvalStream: There should not have input streams left to read",
            );

            self.current_input_stream = None;
        } else {
            // prefetch one more input stream before setting next stream to the current input stream
            if self.current_stream_idx + self.num_input_streams_to_prefetch
                < self.input_stream_count
            {
                self.num_read_inputs_counter.add(1);
                trace!(
                    "+1 to num_read_inputs_counter: {}",
                    self.num_read_inputs_counter.value()
                );

                self.prefetched_input_streams.push(spawn_buffered(
                    self.input_plan
                        .execute(
                            self.current_stream_idx + self.num_input_streams_to_prefetch,
                            Arc::<TaskContext>::clone(&self.context),
                        )
                        .unwrap(),
                    1,
                ));
            }

            self.current_stream_idx += 1;
            if self.prefetched_input_streams.is_empty() {
                self.current_input_stream = None;
            } else {
                self.current_input_stream = Some(self.prefetched_input_streams.remove(0));
            }
        }
    }

    fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<RecordBatch>>> {
        // All input streams have been read
        if self.current_input_stream.is_none() {
            return Poll::Ready(None);
        }

        // Get next record batch
        let mut poll;
        loop {
            poll = self
                .current_input_stream
                .as_mut()
                .unwrap()
                .poll_next_unpin(cx);
            match poll {
                // This input stream no longer has data, move to next stream
                Poll::Ready(None) => {
                    self.next_stream();
                    if self.current_input_stream.is_none() {
                        // Have reached the end of all input streams
                        return Poll::Ready(None);
                    }
                }
                _ => break,
            }
        }

        poll
    }
}

/// Concat input streams until reaching the fetch limit
struct ProgressiveEvalStream {
    /// Input streams
    input_streams: InputStreams,

    /// The schema of the input and output.
    schema: SchemaRef,

    /// used to record execution baseline metrics
    baseline_metrics: BaselineMetrics,

    /// If the stream has encountered an error
    aborted: bool,

    /// Optional number of rows to fetch
    fetch: Option<usize>,

    /// number of rows produced
    produced: usize,
}

impl ProgressiveEvalStream {
    fn new(
        input_plan: Arc<dyn ExecutionPlan>,
        context: Arc<TaskContext>,
        schema: SchemaRef,
        baseline_metrics: BaselineMetrics,
        num_read_inputs_counter: Count,
        fetch: Option<usize>,
    ) -> Result<Self> {
        // Use config param to set number of prefetch stream
        let mut num_input_streams_to_prefetch = context
            .session_config()
            .get_extension::<IoxConfigExt>()
            .unwrap_or_default()
            .progressive_eval_num_prefetch_input_streams;

        // If there is no limit of number of rows to fecth, prefetch all input streams
        if fetch.is_none() {
            num_input_streams_to_prefetch = input_plan
                .properties()
                .output_partitioning()
                .partition_count()
        }

        let input_streams = InputStreams::new(
            input_plan,
            context,
            num_input_streams_to_prefetch,
            num_read_inputs_counter,
        )?;

        Ok(Self {
            input_streams,
            schema,
            baseline_metrics,
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
        if self.produced >= self.fetch.unwrap_or(usize::MAX) {
            return Poll::Ready(None);
        }

        let poll = self.input_streams.poll_next(cx);

        let poll = match ready!(poll) {
            // This input stream has data, return its next record batch
            Some(Ok(mut batch)) => {
                self.produced += batch.num_rows();
                if let Some(fetch) = self.fetch
                    && self.produced > fetch
                {
                    batch = batch.slice(0, batch.num_rows() - (self.produced - fetch));
                    self.produced = fetch;
                }
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
                Poll::Ready(None)
            }
        };

        self.baseline_metrics.record_poll(poll)
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
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::datasource::source::DataSourceExec;
    use datafusion::physical_expr::EquivalenceProperties;
    use datafusion::physical_plan::collect;
    use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
    use datafusion::physical_plan::metrics::{MetricValue, Timestamp};
    use datafusion::physical_plan::{Partitioning, PlanProperties};
    use futures::{Future, FutureExt};

    use super::*;

    #[tokio::test]
    async fn test_no_input_stream() {
        let task_ctx = Arc::new(TaskContext::default());

        // no fetch limit --> return all rows
        _test_progressive_eval(
            &[],
            None,
            &["++", "++"],
            0, // 0 input stream
            0, // 0 input stream is prefetched and polled
            Arc::clone(&task_ctx),
        )
        .await;

        // limit = 0 means select nothing
        _test_progressive_eval(
            &[],
            Some(0),
            &["++", "++"],
            0, // 0 input stream
            0, // 0 input stream is prefetched and polled
            Arc::clone(&task_ctx),
        )
        .await;

        // limit = 1 on no data
        _test_progressive_eval(
            &[],
            Some(1),
            &["++", "++"],
            0, // 0 input stream
            0, // 0 input stream is prefetched and polled
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
            1, // 1 input stream
            1, // 1 input stream is prefetched and polled
            Arc::clone(&task_ctx),
        )
        .await;

        // fetch no rows
        _test_progressive_eval(
            &[vec![b1.clone()]],
            Some(0),
            &["++", "++"],
            1,
            1,
            Arc::clone(&task_ctx),
        )
        .await;

        // return only 3 rows from the first record batch
        _test_progressive_eval(
            &[vec![b1.clone()]],
            Some(3),
            &[
                "+---+---+-------------------------------+",
                "| a | b | c                             |",
                "+---+---+-------------------------------+",
                "| 1 | a | 1970-01-01T00:00:00.000000008 |",
                "| 2 | c | 1970-01-01T00:00:00.000000007 |",
                "| 7 | e | 1970-01-01T00:00:00.000000006 |",
                "+---+---+-------------------------------+",
            ],
            1, // 1 input stream
            1, // 1 input stream is prefetched and polled
            Arc::clone(&task_ctx),
        )
        .await;

        // return all because fetch limit is larger
        _test_progressive_eval(
            &[vec![b1.clone()]],
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
            1, // 1 input stream
            1, // 1 input stream is prefetched and polled
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
        // return all by not specifying fetch limit
        _test_progressive_eval(
            &[vec![b1.clone()], vec![b2.clone()]],
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
            2, // 2 input streams
            2, // all 2 input streams are prefetched and polled
            Arc::clone(&task_ctx),
        )
        .await;

        // [b1, b2]
        // return all by specifying large limit
        _test_progressive_eval(
            &[vec![b1.clone()], vec![b2.clone()]],
            Some(10), // limit = max num rows --> return all rows
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
            2, // 2 input streams
            2, // all 2 input streams are prefetched and polled
            Arc::clone(&task_ctx),
        )
        .await;

        // [b2, b1]
        // return all by not specifying fetch limit
        _test_progressive_eval(
            &[vec![b2.clone()], vec![b1.clone()]],
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
            2, // 2 input streams
            2, // all 2 input streams are prefetched and polled
            Arc::clone(&task_ctx),
        )
        .await;

        // [b2, b1]
        // return all by specifying large limit
        _test_progressive_eval(
            &[vec![b2], vec![b1]],
            Some(20),
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
            2, // 2 input streams
            2, // all 2 input streams are prefetched and polled
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
            2, // 2 input streams
            2, // all 2 input streams are prefetched and polled
            Arc::clone(&task_ctx),
        )
        .await;

        // [b2, b1]
        _test_progressive_eval(
            &[vec![b2], vec![b1]],
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
            2, // 2 input streams
            2, // all 2 input streams are prefetched and polled
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
        // Fetch limit is 1 --> return only 1 row of the first batch (b2)
        _test_progressive_eval(
            &[vec![b2.clone()], vec![b1.clone()]],
            Some(1),
            &[
                "+----+---+-------------------------------+",
                "| a  | b | c                             |",
                "+----+---+-------------------------------+",
                "| 70 | c | 1970-01-01T00:00:00.000000004 |",
                "+----+---+-------------------------------+",
            ],
            2, // 2 input streams
            2, // all 2 input streams are prefetched by default even thouggh only the first one is actally polled
            Arc::clone(&task_ctx),
        )
        .await;

        // [b1, b2]
        // b1 has 5 rows. b2 has 3 rows
        // Fetch limit is 1 --> return only 1 row of the first batch (b1)
        _test_progressive_eval(
            &[vec![b1], vec![b2]],
            Some(1),
            &[
                "+---+---+-------------------------------+",
                "| a | b | c                             |",
                "+---+---+-------------------------------+",
                "| 1 | a | 1970-01-01T00:00:00.000000008 |",
                "+---+---+-------------------------------+",
            ],
            2, // 2 input streams
            2, // all 2 input streams are prefetched by default even thouggh only the first one is actally polled
            task_ctx,
        )
        .await;
    }

    /// Progressive Eval should be only used for non-overlapping batches.
    /// This use of ProgressiveEval is decided at plan-time.
    #[rustfmt::skip]
    #[tokio::test]
    async fn test_two_streams_does_not_interleave() {
        let task_ctx = Arc::new(TaskContext::default());
        let stream1: ArrayRef = Arc::new(Int32Array::from(vec![1, 3, 5,]));
        let b1 = RecordBatch::try_from_iter(vec![("a", stream1)]).unwrap();

        let stream2: ArrayRef = Arc::new(Int32Array::from(vec![2, 4, 6]));
        let b2 = RecordBatch::try_from_iter(vec![("a", stream2)]).unwrap();

        // Does not interleave. Will return all of first batch (b1), then second batch (b2).
        _test_progressive_eval(
            &[vec![b1.clone()], vec![b2.clone()]],
            None,
            &[
                "+---+",
                "| a |",
                "+---+",
                "| 1 |",
                "| 3 |",
                "| 5 |",
                "| 2 |",
                "| 4 |",
                "| 6 |",
                "+---+",
            ],
            2, // 2 input streams
            2, // all 2 input streams are prefetched and polled
            Arc::clone(&task_ctx),
        )
        .await;

        // The first batch is based upon the order provided, not the minimum value.
        _test_progressive_eval(
            &[vec![b2], vec![b1]], // inverse the order, b2 first
            None,
            &[
                "+---+",
                "| a |",
                "+---+",
                "| 2 |",
                "| 4 |",
                "| 6 |",
                "| 1 |",
                "| 3 |",
                "| 5 |",
                "+---+",
            ],
            2, // 2 input streams
            2, // all 2 input streams are prefetched and polled
            Arc::clone(&task_ctx),
        )
        .await;
    }

    /// Progressive Eval fetches as many batches are sufficient to cover the limit requested.
    #[rustfmt::skip]
    #[tokio::test]
    async fn test_two_streams_limit() {
        let task_ctx = Arc::new(TaskContext::default());
        let stream1: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3,]));
        let b1 = RecordBatch::try_from_iter(vec![("a", stream1)]).unwrap();

        let stream2: ArrayRef = Arc::new(Int32Array::from(vec![4, 5, 6]));
        let b2 = RecordBatch::try_from_iter(vec![("a", stream2)]).unwrap();

        // Limit 4 --> should slice the second batch seen
        _test_progressive_eval(
            &[vec![b1.clone()], vec![b2.clone()]],
            Some(4),
            &[
                "+---+",
                "| a |",
                "+---+",
                "| 1 |",
                "| 2 |",
                "| 3 |",
                "| 4 |",
                "+---+",
            ],
            2, // 2 input streams
            2, // all 2 input streams are prefetched and polled
            Arc::clone(&task_ctx),
        )
        .await;

        // The first batch is based upon the order provided, not the minimum value.
        // Limit 4 --> should slice the second batch seen
        _test_progressive_eval(
            &[vec![b2.clone()], vec![b1.clone()]],  // inverse the order, b2 first
            Some(4),
            &[
                "+---+",
                "| a |",
                "+---+",
                "| 4 |",
                "| 5 |",
                "| 6 |",
                "| 1 |",
                "+---+",
            ],
            2, // 2 input streams
            2, // all 2 input streams are prefetched and polled
            Arc::clone(&task_ctx),
        )
        .await;

        // The first batch is based upon the order provided, not the minimum value.
        // Limit 2 --> should slice the first batch seen
        _test_progressive_eval(
            &[vec![b2], vec![b1]],
            Some(2),
            &[
                "+---+",
                "| a |",
                "+---+",
                "| 4 |",
                "| 5 |",
                "+---+",
            ],
            2, // 2 input streams
            2, // all 2 input streams are prefetched and polled
            Arc::clone(&task_ctx),
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
            2, // 2 input streams
            2, // all 2 input streams are prefetched by default even thouggh only the first one is actally polled
            Arc::clone(&task_ctx),
        )
        .await;

        // [b1, b2]
        // b1 has 5 rows. b2 has 3 rows
        // Fetch limit is 5 --> return all 5 rows of first batch (b1) that covers that limit
        _test_progressive_eval(
            &[vec![b1], vec![b2]],
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
            2, // 2 input streams
            2, // all 2 input streams are prefetched by default even thouggh only the first one is actally polled
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
        // Fetch limit is 4 --> return all rows of first batch b2 and part of b1
        _test_progressive_eval(
            &[vec![b2.clone()], vec![b1.clone()]],
            Some(4),
            &[
                "+----+---+-------------------------------+",
                "| a  | b | c                             |",
                "+----+---+-------------------------------+",
                "| 70 | c | 1970-01-01T00:00:00.000000004 |",
                "| 90 | d | 1970-01-01T00:00:00.000000006 |",
                "| 30 | e | 1970-01-01T00:00:00.000000002 |",
                "| 1  | a | 1970-01-01T00:00:00.000000008 |",
                "+----+---+-------------------------------+",
            ],
            2, // 2 input streams
            2, // all 2 input streams are prefetched and polled
            Arc::clone(&task_ctx),
        )
        .await;

        // [b1, b2]
        // b1 has 5 rows. b2 has 3 rows
        // Fetch limit is 6 --> return all rows of first batch b1 and part of b2
        _test_progressive_eval(
            &[vec![b1], vec![b2]],
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
                "+----+---+-------------------------------+",
            ],
            2, // 2 input streams
            2, // all 2 input streams are prefetched and polled
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
        // Fetch limit is 1 --> return first row of the b1
        _test_progressive_eval(
            &[vec![b1.clone()], vec![b2.clone()], vec![b3.clone()]],
            Some(1),
            &[
                "+---+---+-------------------------------+",
                "| a | b | c                             |",
                "+---+---+-------------------------------+",
                "| 1 | a | 1970-01-01T00:00:00.000000008 |",
                "+---+---+-------------------------------+",
            ],
            3, // 3 input streams
            2, // 2 input streams are prefetched by default even though only the first one is polled
            Arc::clone(&task_ctx),
        )
        .await;

        // [b1, b2, b3]
        // b1 has 5 rows. b2 has 3 rows. b3 has 4 rows
        // Fetch limit is 7 --> return all rows of the b1 & part of b2, in the order of b1, b2
        _test_progressive_eval(
            &[vec![b1.clone()], vec![b2.clone()], vec![b3.clone()]],
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
                "+----+---+-------------------------------+",
            ],
            3, // 3 input streams
            3, // since we need to poll 2 input streams, 3 streams are prefetched. Always one extra stream is prefetched
            Arc::clone(&task_ctx),
        )
        .await;

        // [b1, b2, b3]
        // b1 has 5 rows. b2 has 3 rows. b3 has 4 rows
        // Fetch limit is 50 --> return all rows of all batches in the order of b1, b2, b3
        _test_progressive_eval(
            &[vec![b1], vec![b2], vec![b3]],
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
            3, // 3 input streams
            3, // 3 input streams are prefetched and polled
            task_ctx,
        )
        .await;
    }

    #[tokio::test]
    async fn test_four_partitions_with_nulls() {
        let task_ctx = Arc::new(TaskContext::default());

        // partition 1
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

        // partition 2
        let a: ArrayRef = Arc::new(Int32Array::from(vec![10, 20, 70]));
        let b: ArrayRef = Arc::new(StringArray::from_iter(vec![
            Some("e"),
            Some("g"),
            Some("h"),
        ]));
        let c: ArrayRef = Arc::new(TimestampNanosecondArray::from(vec![40, 60, 20]));
        let b2 = RecordBatch::try_from_iter(vec![("a", a), ("b", b), ("c", c)]).unwrap();

        // partition 3
        let a: ArrayRef = Arc::new(Int32Array::from(vec![100, 200, 700, 900]));
        let b: ArrayRef = Arc::new(StringArray::from_iter(vec![
            None,
            Some("g"),
            Some("h"),
            Some("i"),
        ]));
        let c: ArrayRef = Arc::new(TimestampNanosecondArray::from(vec![4, 6, 2, 2]));
        let b3 = RecordBatch::try_from_iter(vec![("a", a), ("b", b), ("c", c)]).unwrap();

        // partition 4
        let a: ArrayRef = Arc::new(Int32Array::from(vec![1000, 2000]));
        let b: ArrayRef = Arc::new(StringArray::from_iter(vec![None, Some("x")]));
        let c: ArrayRef = Arc::new(TimestampNanosecondArray::from(vec![40, 60]));
        let b4 = RecordBatch::try_from_iter(vec![("a", a), ("b", b), ("c", c)]).unwrap();

        // [b1, b2, b3, b4]
        // b1 has 5 rows. b2 has 3 rows. b3 has 4 rows. b4 has 2 rows
        // Fetch limit is 0 --> return nothing.
        // Prefetch minum 2 input streams
        _test_progressive_eval(
            &[
                vec![b1.clone()],
                vec![b2.clone()],
                vec![b3.clone()],
                vec![b4.clone()],
            ],
            Some(0),
            &["++", "++"],
            4, // 4 input streams
            2, // 2 input streams are prefetched by default even though nothing is polled
            Arc::clone(&task_ctx),
        )
        .await;

        // [b1, b2, b3, b4]
        // b1 has 5 rows. b2 has 3 rows. b3 has 4 rows. b4 has 2 rows
        // Fetch limit is 4 --> return 4 rows of b1
        // Prefetch minum 2 input streams
        _test_progressive_eval(
            &[
                vec![b1.clone()],
                vec![b2.clone()],
                vec![b3.clone()],
                vec![b4.clone()],
            ],
            Some(4),
            &[
                "+---+---+-------------------------------+",
                "| a | b | c                             |",
                "+---+---+-------------------------------+",
                "| 1 | a | 1970-01-01T00:00:00.000000008 |",
                "| 2 | b | 1970-01-01T00:00:00.000000007 |",
                "| 7 | c | 1970-01-01T00:00:00.000000006 |",
                "| 9 |   | 1970-01-01T00:00:00.000000005 |",
                "+---+---+-------------------------------+",
            ],
            4, // 4 input streams
            2, // 2 input streams are prefetched  and one stream is polled
            Arc::clone(&task_ctx),
        )
        .await;

        // [b1, b2, b3, b4]
        // b1 has 5 rows. b2 has 3 rows. b3 has 4 rows. b4 has 2 rows
        // Fetch limit is 5 --> return all 5 rows of b1
        // Prefetch minum 2 input streams
        _test_progressive_eval(
            &[
                vec![b1.clone()],
                vec![b2.clone()],
                vec![b3.clone()],
                vec![b4.clone()],
            ],
            Some(5),
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
            4, // 4 input streams
            2, // 2 input streams are prefetched and one stream is polled
            Arc::clone(&task_ctx),
        )
        .await;

        // [b1, b2, b3, b4]
        // b1 has 5 rows. b2 has 3 rows. b3 has 4 rows. b4 has 2 rows
        // Fetch limit is 8 --> return all 8 rows of b1 and b2
        // Prefetched 3 input streams since we will always prefetch one extra one
        _test_progressive_eval(
            &[
                vec![b1.clone()],
                vec![b2.clone()],
                vec![b3.clone()],
                vec![b4.clone()],
            ],
            Some(8),
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
            4, // 4 input streams
            3, // 3 input streams are prefetched and 2 streams are polled
            Arc::clone(&task_ctx),
        )
        .await;

        // [b1, b2, b3, b4]
        // b1 has 5 rows. b2 has 3 rows. b3 has 4 rows. b4 has 2 rows
        // Fetch limit is 12 --> return all 12 rows of b1, b2 and b3
        // Prefetch 4 input streams since we will always prefetch one extra one
        _test_progressive_eval(
            &[
                vec![b1.clone()],
                vec![b2.clone()],
                vec![b3.clone()],
                vec![b4.clone()],
            ],
            Some(12),
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
            4, // 4 input streams
            4, // 4 input streams are prefetched and 3 streams are polled
            Arc::clone(&task_ctx),
        )
        .await;

        // [b1, b2, b3, b4]
        // b1 has 5 rows. b2 has 3 rows. b3 has 4 rows. b4 has 2 rows
        // Fetch limit is 15 --> return all 15 rows of b1, b2, b3 and b4
        // Prefetched all 4 input streams
        _test_progressive_eval(
            &[
                vec![b1.clone()],
                vec![b2.clone()],
                vec![b3.clone()],
                vec![b4.clone()],
            ],
            Some(15),
            &[
                "+------+---+-------------------------------+",
                "| a    | b | c                             |",
                "+------+---+-------------------------------+",
                "| 1    | a | 1970-01-01T00:00:00.000000008 |",
                "| 2    | b | 1970-01-01T00:00:00.000000007 |",
                "| 7    | c | 1970-01-01T00:00:00.000000006 |",
                "| 9    |   | 1970-01-01T00:00:00.000000005 |",
                "| 3    | f | 1970-01-01T00:00:00.000000008 |",
                "| 10   | e | 1970-01-01T00:00:00.000000040 |",
                "| 20   | g | 1970-01-01T00:00:00.000000060 |",
                "| 70   | h | 1970-01-01T00:00:00.000000020 |",
                "| 100  |   | 1970-01-01T00:00:00.000000004 |",
                "| 200  | g | 1970-01-01T00:00:00.000000006 |",
                "| 700  | h | 1970-01-01T00:00:00.000000002 |",
                "| 900  | i | 1970-01-01T00:00:00.000000002 |",
                "| 1000 |   | 1970-01-01T00:00:00.000000040 |",
                "| 2000 | x | 1970-01-01T00:00:00.000000060 |",
                "+------+---+-------------------------------+",
            ],
            4, // 4 input streams
            4, // 4 input streams are prefetched and polled
            Arc::clone(&task_ctx),
        )
        .await;

        // [b1, b2, b3, b4]
        // b1 has 5 rows. b2 has 3 rows. b3 has 4 rows. b4 has 2 rows
        // No fetch limit--> return all 15 rows of b1, b2, b3 and b4
        // Prefetched all 4 input streams
        _test_progressive_eval(
            &[
                vec![b1.clone()],
                vec![b2.clone()],
                vec![b3.clone()],
                vec![b4.clone()],
            ],
            None, // No fetch limit
            &[
                "+------+---+-------------------------------+",
                "| a    | b | c                             |",
                "+------+---+-------------------------------+",
                "| 1    | a | 1970-01-01T00:00:00.000000008 |",
                "| 2    | b | 1970-01-01T00:00:00.000000007 |",
                "| 7    | c | 1970-01-01T00:00:00.000000006 |",
                "| 9    |   | 1970-01-01T00:00:00.000000005 |",
                "| 3    | f | 1970-01-01T00:00:00.000000008 |",
                "| 10   | e | 1970-01-01T00:00:00.000000040 |",
                "| 20   | g | 1970-01-01T00:00:00.000000060 |",
                "| 70   | h | 1970-01-01T00:00:00.000000020 |",
                "| 100  |   | 1970-01-01T00:00:00.000000004 |",
                "| 200  | g | 1970-01-01T00:00:00.000000006 |",
                "| 700  | h | 1970-01-01T00:00:00.000000002 |",
                "| 900  | i | 1970-01-01T00:00:00.000000002 |",
                "| 1000 |   | 1970-01-01T00:00:00.000000040 |",
                "| 2000 | x | 1970-01-01T00:00:00.000000060 |",
                "+------+---+-------------------------------+",
            ],
            4, // 4 input streams
            4, // 4 input streams are prefetched and polled
            Arc::clone(&task_ctx),
        )
        .await;
    }

    async fn _test_progressive_eval(
        partitions: &[Vec<RecordBatch>],
        fetch: Option<usize>,
        expected_result: &[&str],
        expected_num_input_streams: usize,
        expected_num_read_input_streams: usize,
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

        let exec = Arc::new(DataSourceExec::new(Arc::new(
            MemorySourceConfig::try_new(partitions, schema, None).unwrap(),
        )));
        let progressive = Arc::new(ProgressiveEvalExec::new(exec, None, fetch));

        let progressive_clone = Arc::clone(&progressive);

        let collected = collect(progressive, context).await.unwrap();
        assert_batches_eq!(expected_result, collected.as_slice());

        // verify metrics
        let metrics = progressive_clone.metrics().unwrap();
        let num_input_streams = Count::new();
        num_input_streams.add(expected_num_input_streams);
        let num_read_input_streams = Count::new();
        num_read_input_streams.add(expected_num_read_input_streams);

        assert_eq!(
            metrics.sum_by_name("num_inputs"),
            Some(MetricValue::Count {
                name: Borrowed("num_inputs"),
                count: num_input_streams
            })
        );
        assert_eq!(
            metrics.sum_by_name("num_read_inputs"),
            Some(MetricValue::Count {
                name: Borrowed("num_read_inputs"),
                count: num_read_input_streams
            })
        );
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
        let exec = Arc::new(DataSourceExec::new(Arc::new(
            MemorySourceConfig::try_new(&[vec![b1], vec![b2]], schema, None).unwrap(),
        )));
        let progressive = Arc::new(ProgressiveEvalExec::new(exec, None, None));

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

        let num_input_streams = Count::new();
        num_input_streams.add(2);
        assert_eq!(
            metrics.sum_by_name("num_inputs"),
            Some(MetricValue::Count {
                name: Borrowed("num_inputs"),
                count: num_input_streams
            })
        );

        let num_read_input_streams = Count::new();
        num_read_input_streams.add(2);
        assert_eq!(
            metrics.sum_by_name("num_read_inputs"),
            Some(MetricValue::Count {
                name: Borrowed("num_read_inputs"),
                count: num_read_input_streams
            })
        );

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
        #![expect(clippy::future_not_send)]
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

        /// Ref-counting helper to check if the plan and the produced stream are still in memory.
        refs: Arc<()>,

        /// Cache holding plan properties like equivalences, output partitioning etc.
        cache: PlanProperties,
    }

    impl BlockingExec {
        /// Create new [`BlockingExec`] with a give schema and number of partitions.
        pub fn new(schema: SchemaRef, n_partitions: usize) -> Self {
            let cache = Self::compute_properties(Arc::clone(&schema), n_partitions);
            Self {
                schema,
                refs: Default::default(),
                cache,
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

        /// This function creates the cache object that stores the plan properties such as schema, equivalence properties, ordering, partitioning, etc.
        fn compute_properties(schema: SchemaRef, n_partitions: usize) -> PlanProperties {
            let eq_properties = EquivalenceProperties::new(schema);

            PlanProperties::new(
                eq_properties,
                Partitioning::UnknownPartitioning(n_partitions),
                EmissionType::Incremental,
                Boundedness::Bounded,
            )
        }
    }

    impl DisplayAs for BlockingExec {
        fn fmt_as(
            &self,
            t: DisplayFormatType,
            f: &mut std::fmt::Formatter<'_>,
        ) -> std::fmt::Result {
            match t {
                DisplayFormatType::Default
                | DisplayFormatType::Verbose
                | DisplayFormatType::TreeRender => {
                    write!(f, "BlockingExec",)
                }
            }
        }
    }

    impl ExecutionPlan for BlockingExec {
        fn name(&self) -> &str {
            Self::static_name()
        }

        fn as_any(&self) -> &dyn Any {
            self
        }

        fn properties(&self) -> &PlanProperties {
            &self.cache
        }

        fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
            // this is a leaf node and has no children
            vec![]
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
