use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use arrow_flight::{encode::FlightDataEncoder, error::Result as FlightResult, FlightData};
use futures::Stream;
use metric::DurationHistogram;
use pin_project::pin_project;
use trace::span::Span;
use trace::span::SpanRecorder;

#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
/// State machine nodes for the [`FlightFrameEncodeInstrumentation`]
enum FrameEncodeInstrumentationState {
    /// The caller is not awaiting a frame.
    Unpolled,

    /// The caller is currently awaiting a frame, and the embedded
    /// [`SpanRecorder`] and [`Instant`] was created when the wait began.
    Polled(SpanRecorder, Instant),
}

impl FrameEncodeInstrumentationState {
    /// Unwrap `self`, measuring duration, returning the [`SpanRecorder`] and
    /// setting `self` to [`FrameEncodeInstrumentationState::Unpolled`].
    ///
    /// # Panics
    ///
    /// Panics if `self` is not [`FrameEncodeInstrumentationState::Polled`].
    fn finish(&mut self, subtotaled_sum: &mut Duration) -> SpanRecorder {
        match std::mem::replace(self, Self::Unpolled) {
            Self::Unpolled => panic!("unwrapping incorrect state"),
            Self::Polled(recorder, start) => {
                *subtotaled_sum = subtotaled_sum
                    .checked_add(start.elapsed())
                    .expect("invalid subtotaled_sum");
                recorder
            }
        }
    }
}

/// An instrumentation wrapper around a [`FlightDataEncoder`], emitting tracing
/// spans that cover the duration of time a caller spends waiting (usually
/// asynchronously) for the underlying codec implementation to obtain and covert
/// a [`RecordBatch`] to a Flight protocol frame and return it.
///
/// Effectively, these spans record the amount of time a caller is waiting to
/// make progress when streaming a Flight response.
///
/// [`RecordBatch`]: arrow::record_batch::RecordBatch
#[pin_project]
pub(crate) struct FlightFrameEncodeInstrumentation {
    #[pin]
    inner: FlightDataEncoder,
    parent_span: Option<Span>,
    state: FrameEncodeInstrumentationState,
    encoding_duration_metric: Arc<DurationHistogram>,
    current_duration_subtotal: Duration,
}

impl FlightFrameEncodeInstrumentation {
    /// Creates a [`FlightFrameEncodeInstrumentation`].
    ///
    /// When a `parent_span` is provided, it adds a trace span as a child of the parent.
    pub fn new(
        inner: FlightDataEncoder,
        parent_span: Option<Span>,
        encoding_duration_metric: Arc<DurationHistogram>,
    ) -> Self {
        Self {
            inner,
            parent_span,
            state: FrameEncodeInstrumentationState::Unpolled,
            encoding_duration_metric,
            current_duration_subtotal: Duration::ZERO,
        }
    }
}

impl Stream for FlightFrameEncodeInstrumentation {
    type Item = FlightResult<FlightData>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();

        if let FrameEncodeInstrumentationState::Unpolled = this.state {
            let recorder =
                SpanRecorder::new(this.parent_span.as_ref().map(|s| s.child("frame encoding")));
            *this.state = FrameEncodeInstrumentationState::Polled(recorder, Instant::now());
        };
        let poll = this.inner.poll_next(cx);

        match poll {
            Poll::Pending => {}
            Poll::Ready(None) => {
                this.state
                    .finish(this.current_duration_subtotal)
                    .ok("complete");
                this.encoding_duration_metric
                    .record(*this.current_duration_subtotal);
            }
            Poll::Ready(Some(Ok(_))) => {
                this.state
                    .finish(this.current_duration_subtotal)
                    .ok("data emitted");
            }
            Poll::Ready(Some(Err(_))) => {
                this.state
                    .finish(this.current_duration_subtotal)
                    .error("error");
            }
        }
        poll
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::Arc,
        time::{Duration, Instant},
    };

    use arrow::{array::Int64Array, record_batch::RecordBatch};
    use arrow_flight::{encode::FlightDataEncoderBuilder, error::FlightError};
    use assert_matches::assert_matches;
    use futures::StreamExt;
    use parking_lot::Mutex;
    use trace::{ctx::SpanContext, RingBufferTraceCollector, TraceCollector};

    use crate::make_batch;

    use super::*;

    /// The lower bound on how long a MockStream poll takes.
    ///
    /// Actual time may be longer due to CPU scheduling.
    const POLL_BLOCK_TIME: Duration = Duration::from_millis(100);

    /// A manual implementation of the Stream trait that can be configured to
    /// return pending/ready responses when polled.]
    ///
    /// It simulates [`POLL_BLOCK_TIME`] of computation occurring each time the
    /// mock is polled by blocking the caller thread before returning.
    struct MockStream {
        #[allow(clippy::type_complexity)]
        ret: Mutex<
            Box<dyn Iterator<Item = Poll<Option<Result<RecordBatch, FlightError>>>> + Send + Sync>,
        >,
    }

    impl MockStream {
        fn new<I, T>(iter: I) -> Self
        where
            I: IntoIterator<IntoIter = T> + 'static,
            T: Iterator<Item = Poll<Option<Result<RecordBatch, FlightError>>>>
                + Send
                + Sync
                + 'static,
        {
            Self {
                ret: Mutex::new(Box::new(iter.into_iter())),
            }
        }
    }

    impl Stream for MockStream {
        type Item = Result<RecordBatch, FlightError>;

        fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            // Simulate computation during the poll.
            std::thread::sleep(POLL_BLOCK_TIME);

            // Signal that this future is ready to be polled again immediately.
            cx.waker().wake_by_ref();

            // Return the configured poll result.
            self.ret
                .lock()
                .next()
                .expect("mock stream has no more values to yield")
        }
    }

    /// A test that validates the correctness of the FlightFrameEncodeInstrumentation
    /// across all permissible Stream behaviours, by simulating a query and
    /// asserting the resulting trace spans durations approximately match the
    /// desired measurements though bounds checking.
    #[tokio::test]
    async fn test_frame_encoding_latency_trace() {
        // A dummy batch of data.
        let (batch, _schema) = make_batch!(
            Int64Array("a" => vec![42, 42, 42, 42]),
        );

        // Configure the data source to respond to polls with the following
        // sequence:
        //
        //   1. (first poll) -> Pending
        //   2. Pending
        //   3. Ready(Some(data))
        //   4. Pending
        //   5. Ready(Some(data))
        //   6. Ready(Some(data))
        //   7. (end of stream) -> Ready(None)
        //
        // This simulates a data source that must be asynchronously driven to be
        // able to yield a RecordBatch (such as a contended async mutex), taking
        // at least 7 * POLL_BLOCK_TIME to yield all the data.
        let data_source = MockStream::new([
            Poll::Pending,
            Poll::Pending,
            Poll::Ready(Some(Ok(batch.clone()))),
            Poll::Pending,
            Poll::Ready(Some(Ok(batch.clone()))),
            Poll::Ready(Some(Ok(batch))),
            Poll::Ready(None),
        ]);

        // Initialise a tracing backend to capture the emitted traces.
        let trace_collector = Arc::new(RingBufferTraceCollector::new(5));
        let trace_observer: Arc<dyn TraceCollector> = Arc::new(Arc::clone(&trace_collector));
        let span_ctx = SpanContext::new(Arc::clone(&trace_observer));
        let query_span = span_ctx.child("query span");

        // Initialise metric
        let histogram = Arc::new(
            metric::Registry::default()
                .register_metric::<DurationHistogram>("test", "")
                .recorder([]),
        );

        // Construct the frame encoder, providing it with the dummy data source,
        // and wrap it the encoder in the metric decorator.
        let call_chain = FlightFrameEncodeInstrumentation::new(
            FlightDataEncoderBuilder::new().build(data_source.boxed()),
            Some(query_span.clone()),
            Arc::clone(&histogram),
        );

        // Wait before using the encoder stack to simulate a delay between
        // construction, and usage.
        std::thread::sleep(2 * POLL_BLOCK_TIME);

        // Record the starting time.
        let started_at = Instant::now();

        // Drive the call chain by collecting all the encoded frames from
        // through the stack.
        let encoded = call_chain.collect::<Vec<_>>().await;

        // Record the finish time.
        let ended_at = Instant::now();

        // Assert that at least three frames of encoded data were yielded through
        // the call chain (data frames + arbitrary protocol frames).
        assert!(encoded.len() >= 3);

        // Assert that the entire encoding call took AT LEAST as long as the
        // lower bound time.
        //
        // The encoding call will have required polling the mock data source at
        // least 7 times, causing 7 sleeps of POLL_BLOCK_TIME, plus additional
        // scheduling/execution overhead.
        let call_duration = ended_at.duration_since(started_at);
        assert!(call_duration >= (7 * POLL_BLOCK_TIME));

        // Look for give spans that capture the time a client had to wait for an
        // encoded frame; the first two spans cover this part of the sequence:
        //
        //   1. (first poll) -> Pending
        //   2. Pending
        //   3. Ready(Some(data))
        //
        // This initial data batch produces two protocol frames - a schema
        // frame, and a data payload frame, leading to a span each. Both
        // combined should cover a duration of at least 3 * POLL_BLOCK_TIME.
        //
        // The third span covers the second batch of data:
        //
        //   4. Pending
        //   5. Ready(Some(data))
        //
        // Which should take at least 2 * POLL_BLOCK_TIME.
        //
        // The fourth and last data span covers the final batch of data:
        //
        //   6. Ready(Some(data))
        //
        // Which should take at least POLL_BLOCK_TIME.
        //
        // The fifth and final span emitted covers the last poll which observes
        // the completion of the stream:
        //
        //  7. Ready(None)
        //
        // Which should take at least POLL_BLOCK_TIME.
        //
        let spans = trace_collector.spans();
        assert_matches!(spans.as_slice(), [schema_span, span1, span2, span3, end_span] => {
            // The first span should have a duration of at least 3 *
            // POLL_BLOCK_TIME, and is the schema descriptor protocol frame.
            assert!(span_duration(schema_span) >= (3 * POLL_BLOCK_TIME));
            assert_eq!(schema_span.name, "frame encoding");

            // The underlying encoder has access to the data from the above
            // poll, and is capable of emitting a data frame without polling the
            // data source again, so no sleep occurs and the next frame is
            // emitted quickly.

            // The second data span should have a duration of at least 2 *
            // POLL_BLOCK_TIME
            assert!(span_duration(span2) >= (2 * POLL_BLOCK_TIME));
            assert_eq!(span2.name, "frame encoding");

            // The third data span should have a duration of at least
            // POLL_BLOCK_TIME
            assert!(span_duration(span3) >= POLL_BLOCK_TIME);
            assert_eq!(span3.name, "frame encoding");

            // The final span is emitted when the last poll yields None, which
            // should take at least one POLL_BLOCK_TIME to pull from the
            // underlying data source.
            assert_eq!(end_span.name, "frame encoding");
            assert!(span_duration(span3) >= POLL_BLOCK_TIME);

            // All spans should be child spans of the input span.
            assert_eq!(schema_span.ctx.parent_span_id, Some(query_span.ctx.span_id));
            assert_eq!(span1.ctx.parent_span_id, Some(query_span.ctx.span_id));
            assert_eq!(span2.ctx.parent_span_id, Some(query_span.ctx.span_id));
            assert_eq!(span3.ctx.parent_span_id, Some(query_span.ctx.span_id));
            assert_eq!(end_span.ctx.parent_span_id, Some(query_span.ctx.span_id));

            // And when summed up, the duration of all spans (frame generation)
            // should be less than the duration of the entire call itself.
            let span_total = span_duration(schema_span)
                + span_duration(span1)
                + span_duration(span2)
                + span_duration(span3)
                + span_duration(end_span);
            assert!(span_total <= call_duration);

            // assert the cumulative duration
            assert!(histogram.fetch().total <= call_duration);
            assert!(histogram.fetch().total >= (7 * POLL_BLOCK_TIME));
        });
    }

    /// Helper function to return the duration of time covered by `s`.
    ///
    /// # Panics
    ///
    /// Panics if `s` does not have a start and/or end timestamp, or the range
    /// exceeds representable values (very very large time difference).
    fn span_duration(s: &Span) -> Duration {
        let start = s.start.expect("no start timestamp");
        let end = s.end.expect("no end timestamp");
        end.signed_duration_since(start).to_std().unwrap()
    }
}
