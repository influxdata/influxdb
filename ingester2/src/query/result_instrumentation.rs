//! Instrumentation of query results streamed from a [`QueryExec`]
//! implementation.
//!
//! The top-level [`QueryResultInstrumentation`] decorator implements the
//! [`QueryExec`] trait, wrapping the response of the inner implementation with
//! instrumentation.
//!
//! ```text
//!                      ┌ QueryResultInstrumentation ───┐
//!                      │                               │
//!                      │     ╔═══════════════════╗     │
//!            ┌─────────│     ║  Inner QueryExec  ║     │
//!            │         │     ╚═══════════════════╝     │
//!            │         └───────────────║───────────────┘
//!       Injects                        ║
//!            │                         ║
//!            │                         ║
//!            ▼             ┌ Observe ─ ▼ ─ ─ ─ ─ ─ ┐
//!  ┌──────────────────┐      ╔═══════════════════╗
//!  │QueryMetricContext│─ ─ ┤ ║   QueryResponse   ║ │
//!  └──────────────────┘      ╚═══════════════════╝
//!            │             └ ─ ─ ─ ─ ─ ┬ ─ ─ ─ ─ ─ ┘
//!                                      │
//!            │                         │
//!                                      ┼
//!            │             ┌ Observe ─╱┴╲─ ─ ─ ─ ─ ┐
//!                            ╔═══════════════════╗
//!            └ ─ ─ ─ ─ ─ ─ ▶ ║ PartitionResponse ║ │──────────────┐
//!                            ╚═══════════════════╝             Injects
//!                          └ ─ ─ ─ ─ ─ ┬ ─ ─ ─ ─ ─ ┘              │
//!                                      │                          ▼
//!                                      │                ┌───────────────────┐
//!                                      ┼                │BatchStreamRecorder│
//!                          ┌ Observe ─╱┴╲─ ─ ─ ─ ─ ┐    └───────────────────┘
//!                            ╔═══════════════════╗                │
//!                          │ ║ RecordBatchStream ║ ├ ─ ─ ─ ─ ─ ─ ─
//!                            ╚═══════════════════╝
//!                          └ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┘
//! ```
//!
//! The [`QueryMetricContext`] is injected into the [`QueryResponse`], recording
//! the lifetime of the [`QueryResponse`] itself, and further injecting
//! instances of [`BatchStreamRecorder`] into each [`PartitionResponse`] to
//! observe the per-partition stream of [`RecordBatch`] that are yielded from
//! it.

use std::{
    pin::Pin,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
    task::{Context, Poll},
};

use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use data_types::{NamespaceId, TableId};
use datafusion::{
    error::DataFusionError,
    physical_plan::{RecordBatchStream, SendableRecordBatchStream},
};
use futures::Stream;
use iox_time::{SystemProvider, Time, TimeProvider};
use metric::{DurationHistogram, Metric, U64Histogram, U64HistogramOptions};
use observability_deps::tracing::debug;
use pin_project::{pin_project, pinned_drop};
use trace::span::Span;

use crate::query::{
    partition_response::PartitionResponse,
    response::{PartitionStream, QueryResponse},
    QueryError, QueryExec,
};

/// A [`QueryExec`] decorator adding instrumentation to the [`QueryResponse`]
/// returned by the inner implementation.
///
/// The wall-clock duration of time taken for the caller to consume or drop the
/// query results is recorded, faceted by success/error and completion state
/// (fully consumed all [`RecordBatch`], or dropped before the stream ended).
///
/// Additionally the distribution of row, partition and [`RecordBatch`] counts
/// are recorded.
#[derive(Debug, Clone)]
pub(crate) struct QueryResultInstrumentation<T, P = SystemProvider> {
    inner: T,
    time_provider: P,

    /// A histogram to capture the consume time for a stream that was entirely
    /// consumed (yielded [`Poll::Ready(None)`]) without ever observing an
    /// [`Err`].
    completed_ok: DurationHistogram,

    /// As above but the stream returned at least one [`Err`] item; the stream
    /// was still consumed to completion.
    completed_err: DurationHistogram,

    /// Like [`Self::completed_ok`], but for a stream that was not consumed to
    /// completion (dropped before returning [`Poll::Ready(None)`])]).
    aborted_ok: DurationHistogram,
    aborted_err: DurationHistogram,

    // Histograms to capture the distribution of row/batch/partition
    // counts per query at the end of the query.
    row_hist: U64Histogram,
    record_batch_hist: U64Histogram,
    partition_hist: U64Histogram,
}

impl<T> QueryResultInstrumentation<T> {
    pub(crate) fn new(inner: T, metrics: &metric::Registry) -> Self {
        let duration: Metric<DurationHistogram> = metrics.register_metric(
            "ingester_query_stream_duration",
            "duration of time RPC clients take to stream results for a single query",
        );

        // A wide range of bucket values to capture the highly variable row
        // count.
        let row_hist: U64Histogram = metrics
            .register_metric_with_options::<U64Histogram, _>(
                "ingester_query_result_row",
                "distribution of query result row count sent to the client",
                || {
                    U64HistogramOptions::new([
                        1 << 5,  // 32
                        1 << 6,  // 64
                        1 << 7,  // 128
                        1 << 8,  // 256
                        1 << 9,  // 512
                        1 << 10, // 1,024
                        1 << 11, // 2,048
                        1 << 12, // 4,096
                        1 << 13, // 8,192
                        1 << 14, // 16,384
                        1 << 15, // 32,768
                        1 << 16, // 65,536
                        1 << 17, // 131,072
                        1 << 18, // 262,144
                    ])
                },
            )
            .recorder(&[]);

        let record_batch_hist: U64Histogram = metrics
            .register_metric_with_options::<U64Histogram, _>(
                "ingester_query_result_record_batch",
                "distribution of query result record batch count sent to the client",
                || {
                    U64HistogramOptions::new([
                        1 << 1, // 2
                        1 << 2, // 4
                        1 << 3, // 8
                        1 << 4, // 16
                        1 << 5, // 32
                        1 << 6, // 64
                        1 << 7, // 128
                        1 << 8, // 256
                    ])
                },
            )
            .recorder(&[]);

        // And finally, the number of partitions
        let partition_hist: U64Histogram = metrics
            .register_metric_with_options::<U64Histogram, _>(
                "ingester_query_result_partition",
                "distribution of query result partition count sent to the client",
                || U64HistogramOptions::new([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]),
            )
            .recorder(&[]);

        Self {
            inner,
            time_provider: Default::default(),
            completed_ok: duration.recorder(&[("request", "complete"), ("has_error", "false")]),
            completed_err: duration.recorder(&[("request", "complete"), ("has_error", "true")]),
            aborted_ok: duration.recorder(&[("request", "incomplete"), ("has_error", "false")]),
            aborted_err: duration.recorder(&[("request", "incomplete"), ("has_error", "true")]),
            row_hist,
            record_batch_hist,
            partition_hist,
        }
    }
}

impl<T, P> QueryResultInstrumentation<T, P> {
    #[cfg(test)]
    fn with_time_provider<U>(self, time_provider: U) -> QueryResultInstrumentation<T, U>
    where
        U: TimeProvider,
    {
        QueryResultInstrumentation {
            inner: self.inner,
            time_provider,
            completed_ok: self.completed_ok,
            completed_err: self.completed_err,
            aborted_ok: self.aborted_ok,
            aborted_err: self.aborted_err,
            row_hist: self.row_hist,
            record_batch_hist: self.record_batch_hist,
            partition_hist: self.partition_hist,
        }
    }
}

#[async_trait]
impl<T, P> QueryExec for QueryResultInstrumentation<T, P>
where
    T: QueryExec<Response = QueryResponse>,
    P: TimeProvider + Clone,
{
    type Response = QueryResponse;

    async fn query_exec(
        &self,
        namespace_id: NamespaceId,
        table_id: TableId,
        columns: Vec<String>,
        span: Option<Span>,
    ) -> Result<Self::Response, QueryError> {
        let started_at = self.time_provider.now();

        let stream = self
            .inner
            .query_exec(namespace_id, table_id, columns, span)
            .await?;

        let stream = QueryMetricContext::new(
            stream.into_partition_stream(),
            started_at,
            self.time_provider.clone(),
            self.completed_ok.clone(),
            self.completed_err.clone(),
            self.aborted_ok.clone(),
            self.aborted_err.clone(),
            self.row_hist.clone(),
            self.record_batch_hist.clone(),
            self.partition_hist.clone(),
        );

        Ok(QueryResponse::new(PartitionStream::new(stream)))
    }
}

/// A metric context for the lifetime of a [`QueryResponse`].
///
/// Once the last [`PartitionResponse`] is consumed to completion, this type is
/// dropped and the metrics it has gathered are emitted at drop time.
///
/// This type is responsible for decorating all [`PartitionResponse`] yielded
/// from the result stream with [`BatchStreamRecorder`] instances, in turn
/// capturing the statistics of each [`RecordBatch`] in the
/// [`PartitionResponse`].
#[pin_project(PinnedDrop)]
#[derive(Debug)]
struct QueryMetricContext<S, P = SystemProvider>
where
    P: TimeProvider,
{
    time_provider: P,

    /// The instrumented stream.
    #[pin]
    inner: S,

    /// The metric state shared with child [`BatchStreamRecorder`] instances.
    state: Arc<MetricState>,

    /// The timestamp at which the read request began, inclusive of the work
    /// required to acquire the inner stream (which may involve fetching all the
    /// data if the result is only pretending to be a stream).
    started_at: Time,
    /// The timestamp at which the stream completed (yielding
    /// [`Poll::Ready(None)`]).
    ///
    /// [`None`] if the stream has not yet completed.
    completed_at: Option<Time>,

    /// The running count of partitions yielded by this query.
    partition_count: usize,

    /// The latency histograms faceted by completion/error state.
    completed_ok: DurationHistogram,
    completed_err: DurationHistogram,
    aborted_ok: DurationHistogram,
    aborted_err: DurationHistogram,

    /// Row/record batch/partition count distribution histograms.
    row_hist: U64Histogram,
    record_batch_hist: U64Histogram,
    partition_hist: U64Histogram,
}

impl<S, P> QueryMetricContext<S, P>
where
    P: TimeProvider,
{
    #[allow(clippy::too_many_arguments)]
    fn new(
        stream: S,
        started_at: Time,
        time_provider: P,
        completed_ok: DurationHistogram,
        completed_err: DurationHistogram,
        aborted_ok: DurationHistogram,
        aborted_err: DurationHistogram,
        row_hist: U64Histogram,
        record_batch_hist: U64Histogram,
        partition_hist: U64Histogram,
    ) -> Self {
        Self {
            inner: stream,
            time_provider,
            started_at,
            completed_at: None,
            completed_ok,
            completed_err,
            aborted_ok,
            aborted_err,
            row_hist,
            record_batch_hist,
            partition_hist,
            partition_count: 0,
            state: Default::default(),
        }
    }
}

impl<S, P> Stream for QueryMetricContext<S, P>
where
    S: Stream<Item = PartitionResponse> + Send,
    P: TimeProvider,
{
    type Item = S::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();

        match this.inner.poll_next(cx) {
            Poll::Ready(Some(p)) => {
                // Instrument the RecordBatch stream in this partition.
                *this.partition_count += 1;

                // Extract all the fields of the PartitionResponse
                let id = p.id();
                let persist_count = p.completed_persistence_count();

                // And wrap the underlying stream of RecordBatch for this
                // partition with a metric observer.
                let record_stream = p.into_record_batch_stream().map(|s| {
                    Box::pin(BatchStreamRecorder::new(s, Arc::clone(this.state)))
                        as SendableRecordBatchStream
                });

                Poll::Ready(Some(PartitionResponse::new(
                    record_stream,
                    id,
                    persist_count,
                )))
            }
            Poll::Ready(None) => {
                // Record the wall clock timestamp of the stream end.
                *this.completed_at = Some(this.time_provider.now());
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

#[pinned_drop]
impl<S, P> PinnedDrop for QueryMetricContext<S, P>
where
    P: TimeProvider,
{
    fn drop(self: Pin<&mut Self>) {
        // Record the captured metrics.
        let did_observe_error = self.state.did_observe_error.load(Ordering::Relaxed);
        let row_count = self.state.row_count.load(Ordering::Relaxed) as u64;
        let record_batch_count = self.state.record_batch_count.load(Ordering::Relaxed) as u64;
        let partition_count = self.partition_count;

        // Record the row/record batch/partition counts for this query.
        self.row_hist.record(row_count);
        self.record_batch_hist.record(record_batch_count);
        self.partition_hist.record(partition_count as _);

        // Select the appropriate histogram based off of the completion & error
        // state.
        //
        // If completed_at is None, the stream was aborted before completion.
        let hist = match self.completed_at {
            Some(_) if !did_observe_error => &self.completed_ok,
            Some(_) => &self.completed_err,
            None if !did_observe_error => &self.aborted_ok,
            None => &self.aborted_err,
        };

        // Record the duration, either up to the time of stream completion, or
        // now if the stream did not complete.
        let duration = self
            .completed_at
            .unwrap_or_else(|| self.time_provider.now())
            .checked_duration_since(self.started_at);
        if let Some(d) = duration {
            hist.record(d)
        }

        // Log a helpful debug message for query correlation purposes.
        match self.completed_at {
            Some(_) => debug!(
                ?duration,
                did_observe_error,
                row_count,
                record_batch_count,
                partition_count,
                "completed streaming query results",
            ),
            None => debug!(
                ?duration,
                did_observe_error,
                row_count,
                record_batch_count,
                partition_count,
                "aborted streaming query results",
            ),
        };
    }
}

/// State shared between the parent [`QueryMetricContext`] and all of the child
/// [`BatchStreamRecorder`] it has instantiated.
#[derive(Debug, Default)]
struct MetricState {
    /// True if at least one [`Result`] yielded by this result stream so far has
    /// been an [`Err`].
    //
    /// This is used to select the correct success/error histogram which records
    /// the operation duration.
    did_observe_error: AtomicBool,

    /// Running counts of row, partition, and [`RecordBatch`]
    /// returned for this query so far.
    row_count: AtomicUsize,
    record_batch_count: AtomicUsize,
}

/// Capture row/[`RecordBatch`]/error statistics.
///
/// Inspects each [`RecordBatch`] yielded in the result stream, scoped to a
/// single [`PartitionResponse`].
#[pin_project]
struct BatchStreamRecorder {
    #[pin]
    inner: SendableRecordBatchStream,
    shared_state: Arc<MetricState>,
}

impl BatchStreamRecorder {
    fn new(stream: SendableRecordBatchStream, shared_state: Arc<MetricState>) -> Self {
        Self {
            inner: stream,
            shared_state,
        }
    }
}

impl RecordBatchStream for BatchStreamRecorder {
    fn schema(&self) -> arrow::datatypes::SchemaRef {
        self.inner.schema()
    }
}

impl Stream for BatchStreamRecorder {
    type Item = Result<RecordBatch, DataFusionError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();

        let res = this.inner.poll_next(cx);
        match &res {
            Poll::Ready(Some(Ok(batch))) => {
                // Record the count statistics in this batch.
                this.shared_state
                    .row_count
                    .fetch_add(batch.num_rows(), Ordering::Relaxed);
                this.shared_state
                    .record_batch_count
                    .fetch_add(1, Ordering::Relaxed);
            }
            Poll::Ready(Some(Err(_e))) => {
                // Record that at least one poll returned an error.
                this.shared_state
                    .did_observe_error
                    .store(true, Ordering::Relaxed);
            }
            Poll::Ready(None) => {}
            Poll::Pending => {}
        }

        res
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        // Impl the default size_hint() so this wrapper doesn't mask the size
        // hint from the inner stream, if any.
        self.inner.size_hint()
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use crate::{make_batch, make_partition_stream, query::mock_query_exec::MockQueryExec};

    use super::*;

    use arrow::array::{Float32Array, Int64Array};
    use data_types::PartitionId;
    use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
    use futures::{stream, StreamExt};
    use iox_time::MockProvider;
    use metric::Attributes;

    const NAMESPACE_ID: NamespaceId = NamespaceId::new(42);
    const TABLE_ID: TableId = TableId::new(42);
    const TIME_STEP: Duration = Duration::from_secs(42);

    /// A concise helper to assert the value of a metric histogram, regardless
    /// of underlying type.
    macro_rules! assert_histogram {
        (
            $metrics:ident,
            $hist:ty,
            $name:literal,
            $(labels = $attr:expr,)*
            $(samples = $samples:expr,)*
            $(sum = $sum:expr,)*
        ) => {
            // Default to an empty set of attributes if not specified.
            #[allow(unused)]
            let mut attr = None;
            $(attr = Some($attr);)*
            let attr = attr.unwrap_or_else(|| Attributes::from(&[]));

            let hist = $metrics
                .get_instrument::<Metric<$hist>>($name)
                .expect("failed to find metric with provided name")
                .get_observer(&attr)
                .expect("failed to find metric with provided attributes")
                .fetch();

            $(assert_eq!(hist.sample_count(), $samples, "sample count mismatch");)*
            $(assert_eq!(hist.total, $sum, "sum value mismatch");)*
        };
    }

    /// A query against a table that has been persisted / no longer contains any
    /// data (only metadata).
    #[tokio::test]
    async fn test_multi_partition_stream_no_batches() {
        let metrics = metric::Registry::default();

        // Construct a stream with no batches.
        let stream = PartitionStream::new(stream::iter([PartitionResponse::new(
            None,
            PartitionId::new(42),
            42,
        )]));

        let mock_time = Arc::new(MockProvider::new(Time::MIN));
        let mock_inner = MockQueryExec::default().with_result(Ok(QueryResponse::new(stream)));
        let layer = QueryResultInstrumentation::new(mock_inner, &metrics)
            .with_time_provider(Arc::clone(&mock_time));

        let response = layer
            .query_exec(NAMESPACE_ID, TABLE_ID, vec![], None)
            .await
            .expect("query should succeed");

        // Now the response has been created, advance the clock
        mock_time.inc(TIME_STEP);

        // Drain the query results, moving past any errors, and collecting the
        // final set of all Ok record batches for comparison.
        let _batches = response
            .into_record_batches()
            .filter_map(|v| async { v.ok() })
            .collect::<Vec<_>>()
            .await;

        assert_histogram!(
            metrics,
            U64Histogram,
            "ingester_query_result_row",
            samples = 1,
            sum = 0,
        );
        assert_histogram!(
            metrics,
            U64Histogram,
            "ingester_query_result_record_batch",
            samples = 1,
            sum = 0,
        );
        assert_histogram!(
            metrics,
            U64Histogram,
            "ingester_query_result_partition",
            samples = 1,
            sum = 1, // A partition was yielded, but contained no data
        );
        assert_histogram!(
            metrics,
            DurationHistogram,
            "ingester_query_stream_duration",
            labels = Attributes::from(&[("request", "complete"), ("has_error", "false")]),
            samples = 1,
            sum = TIME_STEP,
        );
        assert_histogram!(
            metrics,
            DurationHistogram,
            "ingester_query_stream_duration",
            labels = Attributes::from(&[("request", "complete"), ("has_error", "true")]),
            samples = 0,
        );
        assert_histogram!(
            metrics,
            DurationHistogram,
            "ingester_query_stream_duration",
            labels = Attributes::from(&[("request", "incomplete"), ("has_error", "false")]),
            samples = 0,
        );
        assert_histogram!(
            metrics,
            DurationHistogram,
            "ingester_query_stream_duration",
            labels = Attributes::from(&[("request", "incomplete"), ("has_error", "true")]),
            samples = 0,
        );
    }

    /// A happy path test - a stream is initialised and read to completion.
    ///
    /// The response includes data from multiple partitions, with multiple
    /// record batches.
    #[tokio::test]
    async fn test_multi_partition_stream_ok() {
        let metrics = metric::Registry::default();

        // Construct the set of partitions and their record batches
        let stream = make_partition_stream!(
            PartitionId::new(1) => [
                make_batch!(
                    Int64Array("a" => vec![1, 2, 3, 4, 5]),
                    Float32Array("b" => vec![4.1, 4.2, 4.3, 4.4, 5.0]),
                ),
                make_batch!(
                    Int64Array("c" => vec![1, 2, 3, 4, 5]),
                ),
            ],
            PartitionId::new(2) => [
                make_batch!(
                    Float32Array("d" => vec![1.1]),
                ),
            ],
        );

        let mock_time = Arc::new(MockProvider::new(Time::MIN));
        let mock_inner = MockQueryExec::default().with_result(Ok(QueryResponse::new(stream)));
        let layer = QueryResultInstrumentation::new(mock_inner, &metrics)
            .with_time_provider(Arc::clone(&mock_time));

        let response = layer
            .query_exec(NAMESPACE_ID, TABLE_ID, vec![], None)
            .await
            .expect("query should succeed");

        // Now the response has been created, advance the clock
        mock_time.inc(TIME_STEP);

        // Drain the query results, moving past any errors, and collecting the
        // final set of all Ok record batches for comparison.
        let _batches = response
            .into_record_batches()
            .filter_map(|v| async { v.ok() })
            .collect::<Vec<_>>()
            .await;

        assert_histogram!(
            metrics,
            U64Histogram,
            "ingester_query_result_row",
            samples = 1,
            sum = 11, // 5 + 5 + 1
        );
        assert_histogram!(
            metrics,
            U64Histogram,
            "ingester_query_result_record_batch",
            samples = 1,
            sum = 3,
        );
        assert_histogram!(
            metrics,
            U64Histogram,
            "ingester_query_result_partition",
            samples = 1,
            sum = 2,
        );
        assert_histogram!(
            metrics,
            DurationHistogram,
            "ingester_query_stream_duration",
            labels = Attributes::from(&[("request", "complete"), ("has_error", "false")]),
            samples = 1,
            sum = TIME_STEP,
        );
        assert_histogram!(
            metrics,
            DurationHistogram,
            "ingester_query_stream_duration",
            labels = Attributes::from(&[("request", "complete"), ("has_error", "true")]),
            samples = 0,
        );
        assert_histogram!(
            metrics,
            DurationHistogram,
            "ingester_query_stream_duration",
            labels = Attributes::from(&[("request", "incomplete"), ("has_error", "false")]),
            samples = 0,
        );
        assert_histogram!(
            metrics,
            DurationHistogram,
            "ingester_query_stream_duration",
            labels = Attributes::from(&[("request", "incomplete"), ("has_error", "true")]),
            samples = 0,
        );
    }

    /// A query result which is dropped immediately does not record any
    /// rows/batches/etc (the client did not see them) but DOES record the wall
    /// clock duration between obtaining the query result and aborting the read.
    #[tokio::test]
    async fn test_multi_partition_stream_aborted_immediately() {
        let metrics = metric::Registry::default();

        // Construct the set of partitions and their record batches
        let stream = make_partition_stream!(
            PartitionId::new(1) => [
                make_batch!(
                    Int64Array("a" => vec![1, 2, 3, 4, 5]),
                    Float32Array("b" => vec![4.1, 4.2, 4.3, 4.4, 5.0]),
                ),
                make_batch!(
                    Int64Array("c" => vec![1, 2, 3, 4, 5]),
                ),
            ],
            PartitionId::new(2) => [
                make_batch!(
                    Float32Array("d" => vec![1.1]),
                ),
            ],
        );

        let mock_time = Arc::new(MockProvider::new(Time::MIN));
        let mock_inner = MockQueryExec::default().with_result(Ok(QueryResponse::new(stream)));
        let layer = QueryResultInstrumentation::new(mock_inner, &metrics)
            .with_time_provider(Arc::clone(&mock_time));

        let response = layer
            .query_exec(NAMESPACE_ID, TABLE_ID, vec![], None)
            .await
            .expect("query should succeed");

        // Now the response has been created, advance the clock
        mock_time.inc(TIME_STEP);

        // Drop the response without reading it to completion (or at all,
        // really...)
        drop(response);

        assert_histogram!(
            metrics,
            U64Histogram,
            "ingester_query_result_row",
            samples = 1,
            sum = 0,
        );
        assert_histogram!(
            metrics,
            U64Histogram,
            "ingester_query_result_record_batch",
            samples = 1,
            sum = 0,
        );
        assert_histogram!(
            metrics,
            U64Histogram,
            "ingester_query_result_partition",
            samples = 1,
            sum = 0,
        );
        assert_histogram!(
            metrics,
            DurationHistogram,
            "ingester_query_stream_duration",
            labels = Attributes::from(&[("request", "complete"), ("has_error", "false")]),
            samples = 0,
        );
        assert_histogram!(
            metrics,
            DurationHistogram,
            "ingester_query_stream_duration",
            labels = Attributes::from(&[("request", "complete"), ("has_error", "true")]),
            samples = 0,
        );
        assert_histogram!(
            metrics,
            DurationHistogram,
            "ingester_query_stream_duration",
            labels = Attributes::from(&[("request", "incomplete"), ("has_error", "false")]),
            samples = 1,
            sum = TIME_STEP, // It was recorded as an incomplete request
        );
        assert_histogram!(
            metrics,
            DurationHistogram,
            "ingester_query_stream_duration",
            labels = Attributes::from(&[("request", "incomplete"), ("has_error", "true")]),
            samples = 0,
        );
    }

    /// A query result which is dropped after partially reading the data should
    /// record rows/batches/etc (as many as the client saw) and record the wall
    /// clock duration between obtaining the query result and aborting the read.
    #[tokio::test]
    async fn test_multi_partition_stream_aborted_after_read() {
        let metrics = metric::Registry::default();

        // Construct the set of partitions and their record batches
        let stream = make_partition_stream!(
            PartitionId::new(1) => [
                make_batch!(
                    Int64Array("a" => vec![1, 2, 3, 4, 5]),
                    Float32Array("b" => vec![4.1, 4.2, 4.3, 4.4, 5.0]),
                ),
                make_batch!(
                    Int64Array("c" => vec![1, 2, 3, 4, 5]),
                ),
            ],
            PartitionId::new(2) => [
                make_batch!(
                    Float32Array("d" => vec![1.1]),
                ),
            ],
        );

        let mock_time = Arc::new(MockProvider::new(Time::MIN));
        let mock_inner = MockQueryExec::default().with_result(Ok(QueryResponse::new(stream)));
        let layer = QueryResultInstrumentation::new(mock_inner, &metrics)
            .with_time_provider(Arc::clone(&mock_time));

        let response = layer
            .query_exec(NAMESPACE_ID, TABLE_ID, vec![], None)
            .await
            .expect("query should succeed");

        // Now the response has been created, advance the clock
        mock_time.inc(TIME_STEP);

        let mut response = response.into_record_batches();
        let got = response
            .next()
            .await
            .expect("should yield first batch")
            .expect("mock doesn't return error");
        drop(response);

        assert_histogram!(
            metrics,
            U64Histogram,
            "ingester_query_result_row",
            samples = 1,
            sum = got.num_rows() as u64,
        );
        assert_histogram!(
            metrics,
            U64Histogram,
            "ingester_query_result_record_batch",
            samples = 1,
            sum = 1,
        );
        assert_histogram!(
            metrics,
            U64Histogram,
            "ingester_query_result_partition",
            samples = 1,
            sum = 1,
        );
        assert_histogram!(
            metrics,
            DurationHistogram,
            "ingester_query_stream_duration",
            labels = Attributes::from(&[("request", "complete"), ("has_error", "false")]),
            samples = 0,
        );
        assert_histogram!(
            metrics,
            DurationHistogram,
            "ingester_query_stream_duration",
            labels = Attributes::from(&[("request", "complete"), ("has_error", "true")]),
            samples = 0,
        );
        assert_histogram!(
            metrics,
            DurationHistogram,
            "ingester_query_stream_duration",
            labels = Attributes::from(&[("request", "incomplete"), ("has_error", "false")]),
            samples = 1,
            sum = TIME_STEP, // It was recorded as an incomplete request
        );
        assert_histogram!(
            metrics,
            DurationHistogram,
            "ingester_query_stream_duration",
            labels = Attributes::from(&[("request", "incomplete"), ("has_error", "true")]),
            samples = 0,
        );
    }

    /// A query result which is dropped when observing an error should record
    /// the various count statistics from any yielded batches and categorise the
    /// result as having observed an error.
    #[tokio::test]
    async fn test_multi_partition_stream_with_error_abort() {
        let metrics = metric::Registry::default();

        // Construct the set of partitions and their record batches
        let (ok_batch, schema) = make_batch!(
            Int64Array("c" => vec![1, 2, 3, 4, 5]),
        );

        let stream = Box::pin(RecordBatchStreamAdapter::new(
            schema,
            stream::iter([
                Ok(ok_batch.clone()),
                Err(DataFusionError::Internal("bananas".to_string())),
                Ok(ok_batch),
            ]),
        )) as SendableRecordBatchStream;

        let stream = PartitionStream::new(stream::iter([PartitionResponse::new(
            Some(stream),
            PartitionId::new(1),
            42,
        )]));

        let mock_time = Arc::new(MockProvider::new(Time::MIN));
        let mock_inner = MockQueryExec::default().with_result(Ok(QueryResponse::new(stream)));
        let layer = QueryResultInstrumentation::new(mock_inner, &metrics)
            .with_time_provider(Arc::clone(&mock_time));

        let response = layer
            .query_exec(NAMESPACE_ID, TABLE_ID, vec![], None)
            .await
            .expect("query should succeed");

        // Now the response has been created, advance the clock
        mock_time.inc(TIME_STEP);

        let mut response = response.into_record_batches();
        let got = response
            .next()
            .await
            .expect("should yield first batch")
            .expect("mock doesn't return error");

        response
            .next()
            .await
            .expect("more results should be available")
            .expect_err("this batch should be an error");

        // Drop the rest of the batches after observing an error.
        drop(response);

        assert_histogram!(
            metrics,
            U64Histogram,
            "ingester_query_result_row",
            samples = 1,
            sum = got.num_rows() as u64,
        );
        assert_histogram!(
            metrics,
            U64Histogram,
            "ingester_query_result_record_batch",
            samples = 1,
            sum = 1,
        );
        assert_histogram!(
            metrics,
            U64Histogram,
            "ingester_query_result_partition",
            samples = 1,
            sum = 1,
        );
        assert_histogram!(
            metrics,
            DurationHistogram,
            "ingester_query_stream_duration",
            labels = Attributes::from(&[("request", "complete"), ("has_error", "false")]),
            samples = 0,
        );
        assert_histogram!(
            metrics,
            DurationHistogram,
            "ingester_query_stream_duration",
            labels = Attributes::from(&[("request", "complete"), ("has_error", "true")]),
            samples = 0,
        );
        assert_histogram!(
            metrics,
            DurationHistogram,
            "ingester_query_stream_duration",
            labels = Attributes::from(&[("request", "incomplete"), ("has_error", "false")]),
            samples = 0,
        );
        assert_histogram!(
            metrics,
            DurationHistogram,
            "ingester_query_stream_duration",
            labels = Attributes::from(&[("request", "incomplete"), ("has_error", "true")]),
            samples = 1,
            sum = TIME_STEP, // Recorded as an incomplete request with error
        );
    }

    /// A query result which is consumed to completion even after observing an
    /// error should be correctly catagorised.
    #[tokio::test]
    async fn test_multi_partition_stream_with_error_completion() {
        let metrics = metric::Registry::default();

        // Construct the set of partitions and their record batches
        let (ok_batch, schema) = make_batch!(
            Int64Array("c" => vec![1, 2, 3, 4, 5]),
        );

        let stream = Box::pin(RecordBatchStreamAdapter::new(
            schema,
            stream::iter([
                Ok(ok_batch.clone()),
                Err(DataFusionError::Internal("bananas".to_string())),
                Ok(ok_batch),
            ]),
        )) as SendableRecordBatchStream;

        let stream = PartitionStream::new(stream::iter([PartitionResponse::new(
            Some(stream),
            PartitionId::new(1),
            42,
        )]));

        let mock_time = Arc::new(MockProvider::new(Time::MIN));
        let mock_inner = MockQueryExec::default().with_result(Ok(QueryResponse::new(stream)));
        let layer = QueryResultInstrumentation::new(mock_inner, &metrics)
            .with_time_provider(Arc::clone(&mock_time));

        let response = layer
            .query_exec(NAMESPACE_ID, TABLE_ID, vec![], None)
            .await
            .expect("query should succeed");

        // Now the response has been created, advance the clock
        mock_time.inc(TIME_STEP);

        // Drain the query results, moving past any errors, and collecting the
        // final set of all Ok record batches for comparison.
        let _batches = response
            .into_record_batches()
            .filter_map(|v| async { v.ok() })
            .collect::<Vec<_>>()
            .await;

        assert_histogram!(
            metrics,
            U64Histogram,
            "ingester_query_result_row",
            samples = 1,
            sum = 10, // 5 + 5
        );
        assert_histogram!(
            metrics,
            U64Histogram,
            "ingester_query_result_record_batch",
            samples = 1,
            sum = 2,
        );
        assert_histogram!(
            metrics,
            U64Histogram,
            "ingester_query_result_partition",
            samples = 1,
            sum = 1,
        );
        assert_histogram!(
            metrics,
            DurationHistogram,
            "ingester_query_stream_duration",
            labels = Attributes::from(&[("request", "complete"), ("has_error", "false")]),
            samples = 0,
        );
        assert_histogram!(
            metrics,
            DurationHistogram,
            "ingester_query_stream_duration",
            labels = Attributes::from(&[("request", "complete"), ("has_error", "true")]),
            samples = 1,
            sum = TIME_STEP, // Recorded as a complete request with error
        );
        assert_histogram!(
            metrics,
            DurationHistogram,
            "ingester_query_stream_duration",
            labels = Attributes::from(&[("request", "incomplete"), ("has_error", "false")]),
            samples = 0,
        );
        assert_histogram!(
            metrics,
            DurationHistogram,
            "ingester_query_stream_duration",
            labels = Attributes::from(&[("request", "incomplete"), ("has_error", "true")]),
            samples = 0,
        );
    }
}
