//! Instrumentation for [`DmlSink`] implementations.

use std::fmt::Debug;

use async_trait::async_trait;
use data_types::ShardIndex;
use dml::DmlOperation;
use iox_time::{SystemProvider, TimeProvider};
use metric::{Attributes, DurationHistogram, U64Counter, U64Gauge};
use trace::span::{SpanExt, SpanRecorder};

use crate::{data::DmlApplyAction, dml_sink::DmlSink};

/// A [`WatermarkFetcher`] abstracts a source of the write buffer high watermark
/// (max known offset).
///
/// # Caching
///
/// Implementations may cache the watermark and return inaccurate values.
pub(crate) trait WatermarkFetcher: Debug + Send + Sync {
    /// Return a watermark if available.
    fn watermark(&self) -> Option<i64>;
}

/// A [`SinkInstrumentation`] decorates a [`DmlSink`] implementation and records
/// write buffer metrics and the latency of the decorated [`DmlSink::apply()`]
/// call, and emits a tracing span covering the call duration.
///
/// # Panics
///
/// A [`SinkInstrumentation`] is instantiated for a specific shard index, and
/// panics if it observes a [`DmlOperation`] from a different shard.
///
/// # Wall Clocks
///
/// Some metrics emitted depend on the wall clocks of both the machine this
/// instance is running on, and the routers. If either of these clocks are
/// incorrect/skewed/drifting the metrics emitted may be incorrect.
#[derive(Debug)]
pub(crate) struct SinkInstrumentation<F, T, P = SystemProvider> {
    /// The [`DmlSink`] impl this layer decorates.
    ///
    /// All ops this impl is called with are passed into `inner` for processing.
    /// The value returned from `inner` is inspected and returned unchanged.
    inner: T,

    /// A high watermark oracle.
    ///
    /// Used to derive ingest lag - tolerant of caching / old values.
    watermark_fetcher: F,

    /// The shard index this instrumentation is recording op metrics for.
    shard_index: ShardIndex,

    /// Op application success/failure call latency histograms (which include
    /// counters)
    op_apply_success: DurationHistogram,
    op_apply_error: DurationHistogram,

    /// Write buffer metrics
    write_buffer_bytes_read: U64Counter,
    write_buffer_last_sequence_number: U64Gauge,
    write_buffer_sequence_number_lag: U64Gauge,
    write_buffer_last_ingest_ts: U64Gauge,

    time_provider: P,
}

impl<F, T> SinkInstrumentation<F, T>
where
    F: WatermarkFetcher,
    T: DmlSink,
{
    /// Construct a new [`SinkInstrumentation`] layer that decorates `inner`
    /// with logic that records metrics from the [`DmlOperation`] instances it
    /// observes.
    ///
    /// The current high watermark is read from `watermark_fetcher` and used to
    /// derive some metric values (such as lag). This impl is tolerant of
    /// cached/stale watermark values being returned by `watermark_fetcher`.
    pub(crate) fn new(
        inner: T,
        watermark_fetcher: F,
        topic_name: String,
        shard_index: ShardIndex,
        metrics: &metric::Registry,
    ) -> Self {
        let attr = Attributes::from([
            ("kafka_partition", shard_index.to_string().into()),
            ("kafka_topic", topic_name.into()),
        ]);

        let write_buffer_bytes_read = metrics
            .register_metric::<U64Counter>(
                "ingester_write_buffer_read_bytes",
                "Total number of bytes read from shard",
            )
            .recorder(attr.clone());
        let write_buffer_last_sequence_number = metrics
            .register_metric::<U64Gauge>(
                "ingester_write_buffer_last_sequence_number",
                "Last consumed sequence number (e.g. Kafka offset)",
            )
            .recorder(attr.clone());
        let write_buffer_sequence_number_lag = metrics
            .register_metric::<U64Gauge>(
                "ingester_write_buffer_sequence_number_lag",
                "The difference between the the last sequence number available (e.g. Kafka \
                 offset) and (= minus) last consumed sequence number",
            )
            .recorder(attr.clone());
        let write_buffer_last_ingest_ts = metrics
            .register_metric::<U64Gauge>(
                "ingester_write_buffer_last_ingest_ts",
                "Last seen ingest timestamp as unix timestamp in nanoseconds",
            )
            .recorder(attr.clone());

        let op_apply = metrics.register_metric::<DurationHistogram>(
            "ingester_op_apply_duration",
            "The duration of time taken to process an operation read from the shard",
        );
        let op_apply_success = op_apply.recorder({
            let mut attr = attr.clone();
            attr.insert("result", "success");
            attr
        });
        let op_apply_error = op_apply.recorder({
            let mut attr = attr;
            attr.insert("result", "error");
            attr
        });

        Self {
            inner,
            watermark_fetcher,
            shard_index,

            op_apply_success,
            op_apply_error,

            write_buffer_bytes_read,
            write_buffer_last_sequence_number,
            write_buffer_sequence_number_lag,
            write_buffer_last_ingest_ts,
            time_provider: SystemProvider::default(),
        }
    }
}

#[async_trait]
impl<F, T, P> DmlSink for SinkInstrumentation<F, T, P>
where
    F: WatermarkFetcher,
    T: DmlSink,
    P: TimeProvider,
{
    type Error = T::Error;

    async fn apply(&self, op: DmlOperation) -> Result<DmlApplyAction, Self::Error> {
        let meta = op.meta();

        // Immediately increment the "bytes read" metric as it records the
        // number of bytes read from the shard, irrespective of the op
        // apply call.
        self.write_buffer_bytes_read.inc(
            meta.bytes_read()
                .expect("entry from write buffer should have size") as u64,
        );

        // Record the producer's wall clock timestamp added to this op.
        //
        // For obvious reasons this timestamp cannot be relied upon to be
        // accurate.
        self.write_buffer_last_ingest_ts.set(
            meta.producer_ts()
                .expect("entry from write buffer must have a producer wallclock time")
                .timestamp_nanos() as u64,
        );

        // Extract the sequence number from the op before giving up ownership
        // to the inner DmlSink (avoiding a clone of the large op).
        let sequence = meta
            .sequence()
            .expect("entry from write buffer must be sequenced");
        assert_eq!(
            sequence.shard_index, self.shard_index,
            "instrumentation for shard index {} saw op from shard index {}",
            self.shard_index, sequence.shard_index,
        );

        // Record the "last read sequence number" write buffer metric.
        self.write_buffer_last_sequence_number
            .set(sequence.sequence_number.get() as u64);

        // If it is possible to obtain the sequence number of the most recent op
        // inserted into the queue, record how far behind the op is.
        if let Some(watermark) = self.watermark_fetcher.watermark() {
            let watermark = watermark as u64;
            self.write_buffer_sequence_number_lag.set(
                watermark
                    .saturating_sub(sequence.sequence_number.get() as u64)
                    .saturating_sub(1),
            );
        }

        // Create a tracing span covering the inner DmlSink call.
        let mut span_recorder =
            SpanRecorder::new(meta.span_context().child_span("DmlSink::apply()"));

        // Call into the inner handler to process the op and calculate the call
        // latency.
        let started_at = self.time_provider.now();
        let res = self.inner.apply(op).await;

        // If the clocks go backwards, skip recording the (nonsense) call
        // latency.
        if let Some(delta) = self.time_provider.now().checked_duration_since(started_at) {
            let metric = match &res {
                Ok(_) => {
                    span_recorder.ok("success");
                    &self.op_apply_success
                }
                Err(e) => {
                    span_recorder.error(e.to_string());
                    &self.op_apply_error
                }
            };
            metric.record(delta);
        }

        // Return the result from the inner handler unmodified
        res
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use assert_matches::assert_matches;
    use data_types::{NamespaceId, Sequence, SequenceNumber, ShardId, TableId};
    use dml::{DmlMeta, DmlWrite};
    use iox_time::Time;
    use metric::{Metric, MetricObserver, Observation};
    use mutable_batch_lp::lines_to_batches;
    use once_cell::sync::Lazy;
    use trace::{ctx::SpanContext, span::SpanStatus, RingBufferTraceCollector, TraceCollector};

    use super::*;
    use crate::{
        dml_sink::{mock_sink::MockDmlSink, DmlError},
        stream_handler::mock_watermark_fetcher::MockWatermarkFetcher,
    };

    /// The shard index the [`SinkInstrumentation`] under test is configured to
    /// be observing for.
    const SHARD_INDEX: ShardIndex = ShardIndex::new(42);

    static TEST_TOPIC_NAME: &str = "topic_name";

    static TEST_TIME: Lazy<Time> = Lazy::new(|| SystemProvider::default().now());

    /// The attributes assigned to the metrics emitted by the
    /// instrumentation when using the above shard / topic values.
    static DEFAULT_ATTRS: Lazy<Attributes> = Lazy::new(|| {
        Attributes::from([
            ("kafka_partition", SHARD_INDEX.to_string().into()),
            ("kafka_topic", TEST_TOPIC_NAME.into()),
        ])
    });

    /// Return a DmlWrite with the given metadata and a single table.
    fn make_write(meta: DmlMeta) -> DmlWrite {
        let tables = lines_to_batches("bananas level=42 4242", 0).unwrap();
        let tables_by_ids = tables
            .into_iter()
            .enumerate()
            .map(|(i, (_k, v))| (TableId::new(i as _), v))
            .collect();

        DmlWrite::new(
            NamespaceId::new(42),
            tables_by_ids,
            "1970-01-01".into(),
            meta,
        )
    }

    /// Extract the metric with the given name from `metrics`.
    fn get_metric<T: MetricObserver>(
        metrics: &metric::Registry,
        name: &'static str,
        attrs: &Attributes,
    ) -> Observation {
        metrics
            .get_instrument::<Metric<T>>(name)
            .unwrap_or_else(|| panic!("did not find metric {}", name))
            .get_observer(attrs)
            .unwrap_or_else(|| panic!("failed to match {} attributes", name))
            .observe()
    }

    /// Initialise a [`SinkInstrumentation`] and drive it with the given
    /// parameters.
    async fn test(
        op: impl Into<DmlOperation> + Send,
        metrics: &metric::Registry,
        with_sink_return: Result<DmlApplyAction, DmlError>,
        with_fetcher_return: Option<i64>,
    ) -> Result<DmlApplyAction, DmlError> {
        let op = op.into();
        let inner = MockDmlSink::default().with_apply_return([with_sink_return]);
        let instrumentation = SinkInstrumentation::new(
            inner,
            MockWatermarkFetcher::new(with_fetcher_return),
            TEST_TOPIC_NAME.to_string(),
            SHARD_INDEX,
            metrics,
        );

        instrumentation.apply(op).await
    }

    fn assert_trace(traces: Arc<dyn TraceCollector>, status: SpanStatus) {
        let traces = traces
            .as_any()
            .downcast_ref::<RingBufferTraceCollector>()
            .expect("unexpected collector impl");

        let span = traces
            .spans()
            .into_iter()
            .find(|s| s.name == "DmlSink::apply()")
            .expect("tracing span not found");

        assert_eq!(
            span.status, status,
            "span status does not match expected value"
        );
    }

    // This test asserts the various metrics are set in the happy path.
    #[tokio::test]
    async fn test_call_inner_ok() {
        let metrics = metric::Registry::default();
        let traces: Arc<dyn TraceCollector> = Arc::new(RingBufferTraceCollector::new(5));
        let span = SpanContext::new(Arc::clone(&traces));

        let meta = DmlMeta::sequenced(
            // Op is offset 100 for shard 42
            Sequence::new(SHARD_INDEX, SequenceNumber::new(100)),
            *TEST_TIME,
            Some(span),
            4242,
        );
        let op = make_write(meta);

        let got = test(op, &metrics, Ok(DmlApplyAction::Applied(true)), Some(12345)).await;
        assert_matches!(got, Ok(DmlApplyAction::Applied(true)));

        // Validate the various write buffer metrics
        assert_matches!(
            get_metric::<U64Counter>(&metrics, "ingester_write_buffer_read_bytes", &DEFAULT_ATTRS),
            Observation::U64Counter(4242)
        );
        assert_matches!(
            get_metric::<U64Gauge>(
                &metrics,
                "ingester_write_buffer_last_sequence_number",
                &DEFAULT_ATTRS
            ),
            Observation::U64Gauge(100)
        );
        assert_matches!(
            get_metric::<U64Gauge>(
                &metrics,
                "ingester_write_buffer_sequence_number_lag",
                &DEFAULT_ATTRS
            ),
            // 12345 - 100 - 1
            Observation::U64Gauge(12_244)
        );
        assert_matches!(
            get_metric::<U64Gauge>(&metrics, "ingester_write_buffer_last_ingest_ts", &DEFAULT_ATTRS),
            // 12345 - 100 - 1
            Observation::U64Gauge(t) => {
                assert_eq!(t, TEST_TIME.timestamp_nanos() as u64);
            }
        );

        // Validate the success histogram was hit
        let hist = get_metric::<DurationHistogram>(&metrics, "ingester_op_apply_duration", &{
            let mut attrs = DEFAULT_ATTRS.clone();
            attrs.insert("result", "success");
            attrs
        });
        assert_matches!(hist, Observation::DurationHistogram(h) => {
            let hits: u64 = h.buckets.iter().map(|b| b.count).sum();
            assert_eq!(hits, 1);
        });

        // Assert the trace span was recorded
        assert_trace(traces, SpanStatus::Ok);
    }

    // This test asserts the various metrics are set when the inner handler
    // returns an error.
    #[tokio::test]
    async fn test_call_inner_error() {
        let metrics = metric::Registry::default();
        let traces: Arc<dyn TraceCollector> = Arc::new(RingBufferTraceCollector::new(5));
        let span = SpanContext::new(Arc::clone(&traces));

        let meta = DmlMeta::sequenced(
            // Op is offset 100 for shard 42
            Sequence::new(SHARD_INDEX, SequenceNumber::new(100)),
            *TEST_TIME,
            Some(span),
            4242,
        );
        let op = make_write(meta);

        let got = test(
            op,
            &metrics,
            Err(DmlError::Data(crate::data::Error::ShardNotFound {
                shard_id: ShardId::new(42),
            })),
            Some(12345),
        )
        .await;
        assert_matches!(
            got,
            Err(DmlError::Data(crate::data::Error::ShardNotFound { .. }))
        );

        // Validate the various write buffer metrics
        assert_matches!(
            get_metric::<U64Counter>(&metrics, "ingester_write_buffer_read_bytes", &DEFAULT_ATTRS),
            Observation::U64Counter(4242)
        );
        assert_matches!(
            get_metric::<U64Gauge>(
                &metrics,
                "ingester_write_buffer_last_sequence_number",
                &DEFAULT_ATTRS
            ),
            Observation::U64Gauge(100)
        );
        assert_matches!(
            get_metric::<U64Gauge>(
                &metrics,
                "ingester_write_buffer_sequence_number_lag",
                &DEFAULT_ATTRS
            ),
            // 12345 - 100 - 1
            Observation::U64Gauge(12_244)
        );
        assert_matches!(
            get_metric::<U64Gauge>(&metrics, "ingester_write_buffer_last_ingest_ts", &DEFAULT_ATTRS),
            // 12345 - 100 - 1
            Observation::U64Gauge(t) => {
                assert_eq!(t, TEST_TIME.timestamp_nanos() as u64);
            }
        );

        // Validate the histogram was hit even on error
        let hist = get_metric::<DurationHistogram>(&metrics, "ingester_op_apply_duration", &{
            let mut attrs = DEFAULT_ATTRS.clone();
            attrs.insert("result", "error");
            attrs
        });
        assert_matches!(hist, Observation::DurationHistogram(h) => {
            let hits: u64 = h.buckets.iter().map(|b| b.count).sum();
            assert_eq!(hits, 1);
        });

        // Assert the trace span was recorded
        assert_trace(traces, SpanStatus::Err);
    }

    // If there's no high watermark available, the write should still succeed.
    #[tokio::test]
    async fn test_no_high_watermark() {
        let metrics = metric::Registry::default();
        let traces: Arc<dyn TraceCollector> = Arc::new(RingBufferTraceCollector::new(5));
        let span = SpanContext::new(Arc::clone(&traces));

        let meta = DmlMeta::sequenced(
            // Op is offset 100 for shard 42
            Sequence::new(SHARD_INDEX, SequenceNumber::new(100)),
            *TEST_TIME,
            Some(span),
            4242,
        );
        let op = make_write(meta);

        let got = test(op, &metrics, Ok(DmlApplyAction::Applied(true)), None).await;
        assert_matches!(got, Ok(DmlApplyAction::Applied(true)));

        // Validate the various write buffer metrics
        assert_matches!(
            get_metric::<U64Counter>(&metrics, "ingester_write_buffer_read_bytes", &DEFAULT_ATTRS),
            Observation::U64Counter(4242)
        );
        assert_matches!(
            get_metric::<U64Gauge>(
                &metrics,
                "ingester_write_buffer_last_sequence_number",
                &DEFAULT_ATTRS
            ),
            Observation::U64Gauge(100)
        );
        assert_matches!(
            get_metric::<U64Gauge>(
                &metrics,
                "ingester_write_buffer_sequence_number_lag",
                &DEFAULT_ATTRS
            ),
            // No value recorded because no watermark was available
            Observation::U64Gauge(0)
        );
        assert_matches!(
            get_metric::<U64Gauge>(&metrics, "ingester_write_buffer_last_ingest_ts", &DEFAULT_ATTRS),
            // 12345 - 100 - 1
            Observation::U64Gauge(t) => {
                assert_eq!(t, TEST_TIME.timestamp_nanos() as u64);
            }
        );

        // Validate the success histogram was hit
        let hist = get_metric::<DurationHistogram>(&metrics, "ingester_op_apply_duration", &{
            let mut attrs = DEFAULT_ATTRS.clone();
            attrs.insert("result", "success");
            attrs
        });
        assert_matches!(hist, Observation::DurationHistogram(h) => {
            let hits: u64 = h.buckets.iter().map(|b| b.count).sum();
            assert_eq!(hits, 1);
        });

        // Assert the trace span was recorded
        assert_trace(traces, SpanStatus::Ok);
    }

    // If the high watermark is less than the current sequence number (for
    // example, due to caching) nothing bad should happen.
    #[tokio::test]
    async fn test_high_watermark_less_than_current_op() {
        let metrics = metric::Registry::default();
        let traces: Arc<dyn TraceCollector> = Arc::new(RingBufferTraceCollector::new(5));
        let span = SpanContext::new(Arc::clone(&traces));

        let meta = DmlMeta::sequenced(
            // Op is offset 100 for shard 42
            Sequence::new(SHARD_INDEX, SequenceNumber::new(100)),
            *TEST_TIME,
            Some(span),
            4242,
        );
        let op = make_write(meta);

        let got = test(op, &metrics, Ok(DmlApplyAction::Applied(true)), Some(1)).await;
        assert_matches!(got, Ok(DmlApplyAction::Applied(true)));

        // Validate the various write buffer metrics
        assert_matches!(
            get_metric::<U64Counter>(&metrics, "ingester_write_buffer_read_bytes", &DEFAULT_ATTRS),
            Observation::U64Counter(4242)
        );
        assert_matches!(
            get_metric::<U64Gauge>(
                &metrics,
                "ingester_write_buffer_last_sequence_number",
                &DEFAULT_ATTRS
            ),
            Observation::U64Gauge(100)
        );
        assert_matches!(
            get_metric::<U64Gauge>(
                &metrics,
                "ingester_write_buffer_sequence_number_lag",
                &DEFAULT_ATTRS
            ),
            // The current sequence number is not behind the high watermark
            Observation::U64Gauge(0)
        );
        assert_matches!(
            get_metric::<U64Gauge>(&metrics, "ingester_write_buffer_last_ingest_ts", &DEFAULT_ATTRS),
            // 12345 - 100 - 1
            Observation::U64Gauge(t) => {
                assert_eq!(t, TEST_TIME.timestamp_nanos() as u64);
            }
        );

        // Validate the success histogram was hit
        let hist = get_metric::<DurationHistogram>(&metrics, "ingester_op_apply_duration", &{
            let mut attrs = DEFAULT_ATTRS.clone();
            attrs.insert("result", "success");
            attrs
        });
        assert_matches!(hist, Observation::DurationHistogram(h) => {
            let hits: u64 = h.buckets.iter().map(|b| b.count).sum();
            assert_eq!(hits, 1);
        });

        // Assert the trace span was recorded
        assert_trace(traces, SpanStatus::Ok);
    }

    // The missing metadata can cause various panics, but the bytes_read is the
    // first one hit.
    #[should_panic = "entry from write buffer should have size"]
    #[tokio::test]
    async fn test_missing_metadata() {
        let metrics = metric::Registry::default();
        let meta = DmlMeta::unsequenced(None);
        let op = make_write(meta);

        let _ = test(op, &metrics, Ok(DmlApplyAction::Applied(true)), Some(12345)).await;
    }

    // The instrumentation emits per-shard metrics, so upon observing an op
    // for a different shard it should panic.
    #[should_panic = "instrumentation for shard index 42 saw op from shard index 52"]
    #[tokio::test]
    async fn test_op_different_shard_index() {
        let metrics = metric::Registry::default();
        let meta = DmlMeta::sequenced(
            // A different shard index from what the handler is configured to
            // be instrumenting
            Sequence::new(
                ShardIndex::new(SHARD_INDEX.get() + 10),
                SequenceNumber::new(100),
            ),
            *TEST_TIME,
            None,
            4242,
        );
        let op = make_write(meta);

        let _ = test(op, &metrics, Ok(DmlApplyAction::Applied(true)), Some(12345)).await;
    }
}
