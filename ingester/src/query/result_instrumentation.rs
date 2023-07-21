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
//!            └ ─ ─ ─ ─ ─ ─ ▶ ║ PartitionResponse ║ │
//!                            ╚═══════════════════╝
//!                          └ ─ ─ ─ ─ ─ ┬ ─ ─ ─ ─ ─ ┘
//!                                      │
//!                                      │
//!                                      ┼
//!                          ┌ Observe ─╱┴╲─ ─ ─ ─ ─ ┐
//!                            ╔═══════════════════╗
//!                          │ ║ RecordBatchStream ║ │
//!                            ╚═══════════════════╝
//!                          └ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┘
//! ```
//!
//! The [`QueryMetricContext`] is injected into the [`QueryResponse`], recording
//! the lifetime of the [`QueryResponse`] itself, and observes the [`RecordBatch`]es
//! produced by each [`PartitionResponse`].
//!
//!
//! [`RecordBatch`]: arrow::record_batch::RecordBatch

use std::{
    pin::Pin,
    sync::atomic::{AtomicUsize, Ordering},
    task::{Context, Poll},
};

use async_trait::async_trait;
use data_types::{NamespaceId, TableId};
use futures::Stream;
use iox_time::{SystemProvider, Time, TimeProvider};
use metric::{DurationHistogram, Metric, U64Histogram, U64HistogramOptions};
use observability_deps::tracing::debug;
use pin_project::{pin_project, pinned_drop};
use predicate::Predicate;
use trace::span::Span;

use crate::query::{
    partition_response::PartitionResponse,
    response::{PartitionStream, QueryResponse},
    QueryError, QueryExec,
};

use super::projection::OwnedProjection;

/// A [`QueryExec`] decorator adding instrumentation to the [`QueryResponse`]
/// returned by the inner implementation.
///
/// The wall-clock duration of time taken for the caller to consume or drop the
/// query results is recorded, faceted by success/error and completion state
/// (fully consumed all [`RecordBatch`], or dropped before the stream ended).
///
/// Additionally the distribution of row, partition and [`RecordBatch`] counts
/// are recorded.
///
///
/// [`RecordBatch`]: arrow::record_batch::RecordBatch
#[derive(Debug, Clone)]
pub(crate) struct QueryResultInstrumentation<T, P = SystemProvider> {
    inner: T,
    time_provider: P,

    /// A histogram to capture the consume time for a stream that was entirely
    /// consumed (yielded [`Poll::Ready(None)`]).
    completed: DurationHistogram,

    /// Like [`Self::completed`], but for a stream that was not consumed to
    /// completion (dropped before returning [`Poll::Ready(None)`])]).
    aborted: DurationHistogram,

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
            completed: duration.recorder(&[("request", "complete")]),
            aborted: duration.recorder(&[("request", "incomplete")]),
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
            completed: self.completed,
            aborted: self.aborted,
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
        projection: OwnedProjection,
        span: Option<Span>,
        predicate: Option<Predicate>,
    ) -> Result<Self::Response, QueryError> {
        let started_at = self.time_provider.now();

        let stream = self
            .inner
            .query_exec(namespace_id, table_id, projection, span, predicate)
            .await?;

        let stream = QueryMetricContext::new(
            stream.into_partition_stream(),
            started_at,
            self.time_provider.clone(),
            self.completed.clone(),
            self.aborted.clone(),
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
/// This type is responsible for capturing the statistics of each [`RecordBatch`]
/// in the [`PartitionResponse`].
///
///
/// [`RecordBatch`]: arrow::record_batch::RecordBatch
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

    /// Running counts of row, partition, and [`RecordBatch`]
    /// returned for this query so far.
    ///
    ///
    /// [`RecordBatch`]: arrow::record_batch::RecordBatch
    row_count: AtomicUsize,
    record_batch_count: AtomicUsize,

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
    completed: DurationHistogram,
    aborted: DurationHistogram,

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
        completed: DurationHistogram,
        aborted: DurationHistogram,
        row_hist: U64Histogram,
        record_batch_hist: U64Histogram,
        partition_hist: U64Histogram,
    ) -> Self {
        Self {
            inner: stream,
            time_provider,
            started_at,
            completed_at: None,
            completed,
            aborted,
            row_hist,
            record_batch_hist,
            partition_hist,
            partition_count: 0,
            row_count: Default::default(),
            record_batch_count: Default::default(),
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
                let id = p.id().clone();
                let persist_count = p.completed_persistence_count();

                // And wrap the underlying stream of RecordBatch for this
                // partition with a metric observer.
                let data = p.into_record_batches();
                this.row_count.fetch_add(
                    data.iter().map(|batch| batch.num_rows()).sum::<usize>(),
                    Ordering::Relaxed,
                );
                this.record_batch_count
                    .fetch_add(data.len(), Ordering::Relaxed);

                Poll::Ready(Some(PartitionResponse::new(data, id, persist_count)))
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
        let row_count = self.row_count.load(Ordering::Relaxed) as u64;
        let record_batch_count = self.record_batch_count.load(Ordering::Relaxed) as u64;
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
            Some(_) => &self.completed,
            None => &self.aborted,
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
                row_count, record_batch_count, partition_count, "completed streaming query results",
            ),
            None => debug!(
                ?duration,
                row_count, record_batch_count, partition_count, "aborted streaming query results",
            ),
        };
    }
}

/// State for every call query (used to aggregate data that will later be written into histograms).
#[derive(Debug, Default)]
struct MetricState {
    /// Running counts of row, partition, and [`RecordBatch`]
    /// returned for this query so far.
    ///
    ///
    /// [`RecordBatch`]: arrow::record_batch::RecordBatch
    row_count: AtomicUsize,
    record_batch_count: AtomicUsize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        make_batch, make_partition_stream,
        query::mock_query_exec::MockQueryExec,
        test_util::{
            ARBITRARY_NAMESPACE_ID, ARBITRARY_TABLE_ID, ARBITRARY_TRANSITION_PARTITION_ID,
        },
    };
    use arrow::array::{Float32Array, Int64Array};
    use futures::{stream, StreamExt};
    use iox_time::MockProvider;
    use metric::{assert_histogram, Attributes};
    use std::{sync::Arc, time::Duration};

    const TIME_STEP: Duration = Duration::from_secs(42);

    /// A query against a table that has been persisted / no longer contains any
    /// data (only metadata).
    #[tokio::test]
    async fn test_multi_partition_stream_no_batches() {
        let metrics = metric::Registry::default();

        // Construct a stream with no batches.
        let stream = PartitionStream::new(stream::iter([PartitionResponse::new(
            vec![],
            ARBITRARY_TRANSITION_PARTITION_ID.clone(),
            42,
        )]));

        let mock_time = Arc::new(MockProvider::new(Time::MIN));
        let mock_inner = MockQueryExec::default().with_result(Ok(QueryResponse::new(stream)));
        let layer = QueryResultInstrumentation::new(mock_inner, &metrics)
            .with_time_provider(Arc::clone(&mock_time));

        let response = layer
            .query_exec(
                ARBITRARY_NAMESPACE_ID,
                ARBITRARY_TABLE_ID,
                OwnedProjection::default(),
                None,
                None,
            )
            .await
            .expect("query should succeed");

        // Now the response has been created, advance the clock
        mock_time.inc(TIME_STEP);

        // Drain the query results, moving past any errors, and collecting the
        // final set of all Ok record batches for comparison.
        let _partitions = response.into_partition_stream().collect::<Vec<_>>().await;

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
            labels = Attributes::from(&[("request", "complete")]),
            samples = 1,
            sum = TIME_STEP,
        );
        assert_histogram!(
            metrics,
            DurationHistogram,
            "ingester_query_stream_duration",
            labels = Attributes::from(&[("request", "incomplete")]),
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
            1 => [
                make_batch!(
                    Int64Array("a" => vec![1, 2, 3, 4, 5]),
                    Float32Array("b" => vec![4.1, 4.2, 4.3, 4.4, 5.0]),
                ),
                make_batch!(
                    Int64Array("c" => vec![1, 2, 3, 4, 5]),
                ),
            ],
            2 => [
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
            .query_exec(
                ARBITRARY_NAMESPACE_ID,
                ARBITRARY_TABLE_ID,
                OwnedProjection::default(),
                None,
                None,
            )
            .await
            .expect("query should succeed");

        // Now the response has been created, advance the clock
        mock_time.inc(TIME_STEP);

        // Drain the query results, moving past any errors, and collecting the
        // final set of all Ok record batches for comparison.
        let _partitions = response.into_partition_stream().collect::<Vec<_>>().await;

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
            labels = Attributes::from(&[("request", "complete")]),
            samples = 1,
            sum = TIME_STEP,
        );
        assert_histogram!(
            metrics,
            DurationHistogram,
            "ingester_query_stream_duration",
            labels = Attributes::from(&[("request", "incomplete")]),
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
            1 => [
                make_batch!(
                    Int64Array("a" => vec![1, 2, 3, 4, 5]),
                    Float32Array("b" => vec![4.1, 4.2, 4.3, 4.4, 5.0]),
                ),
                make_batch!(
                    Int64Array("c" => vec![1, 2, 3, 4, 5]),
                ),
            ],
            2 => [
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
            .query_exec(
                ARBITRARY_NAMESPACE_ID,
                ARBITRARY_TABLE_ID,
                OwnedProjection::default(),
                None,
                None,
            )
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
            labels = Attributes::from(&[("request", "complete")]),
            samples = 0,
        );
        assert_histogram!(
            metrics,
            DurationHistogram,
            "ingester_query_stream_duration",
            labels = Attributes::from(&[("request", "incomplete")]),
            samples = 1,
            sum = TIME_STEP, // It was recorded as an incomplete request
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
            1 => [
                make_batch!(
                    Int64Array("a" => vec![1, 2, 3, 4, 5]),
                    Float32Array("b" => vec![4.1, 4.2, 4.3, 4.4, 5.0]),
                ),
                make_batch!(
                    Int64Array("c" => vec![1, 2, 3, 4, 5]),
                ),
            ],
            2 => [
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
            .query_exec(
                ARBITRARY_NAMESPACE_ID,
                ARBITRARY_TABLE_ID,
                OwnedProjection::default(),
                None,
                None,
            )
            .await
            .expect("query should succeed");

        // Now the response has been created, advance the clock
        mock_time.inc(TIME_STEP);

        let mut response = response.into_partition_stream();
        let got = response.next().await.expect("should yield first batch");
        drop(response);

        let batches = got.into_record_batches();

        assert_histogram!(
            metrics,
            U64Histogram,
            "ingester_query_result_row",
            samples = 1,
            sum = batches.iter().map(|batch| batch.num_rows()).sum::<usize>() as u64,
        );
        assert_histogram!(
            metrics,
            U64Histogram,
            "ingester_query_result_record_batch",
            samples = 1,
            sum = batches.len() as u64,
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
            labels = Attributes::from(&[("request", "complete")]),
            samples = 0,
        );
        assert_histogram!(
            metrics,
            DurationHistogram,
            "ingester_query_stream_duration",
            labels = Attributes::from(&[("request", "incomplete")]),
            samples = 1,
            sum = TIME_STEP, // It was recorded as an incomplete request
        );
    }
}
