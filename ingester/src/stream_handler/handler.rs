use std::{fmt::Debug, time::Duration};

use data_types2::KafkaPartition;
use dml::DmlOperation;
use futures::{pin_mut, FutureExt, Stream, StreamExt};
use metric::{Attributes, U64Counter, U64Gauge};
use observability_deps::tracing::*;
use time::{SystemProvider, TimeProvider};
use tokio_util::sync::CancellationToken;
use write_buffer::core::{WriteBufferError, WriteBufferErrorKind};

use crate::lifecycle::LifecycleHandle;

use super::DmlSink;

/// When the [`LifecycleManager`] indicates that ingest should be paused because
/// of memory pressure, the sequencer will loop, sleeping this long between
/// calls to [`LifecycleHandle::can_resume_ingest()`] with the manager if it
/// can resume ingest.
///
/// [`LifecycleManager`]: crate::lifecycle::LifecycleManager
/// [`LifecycleHandle::can_resume_ingest()`]: crate::lifecycle::LifecycleHandle::can_resume_ingest()
const INGEST_POLL_INTERVAL: Duration = Duration::from_millis(100);

/// A [`SequencedStreamHandler`] consumes a sequence of [`DmlOperation`] from a
/// sequencer stream and pushes them into the configured [`DmlSink`].
///
/// Ingest reads are rate limited by the [`LifecycleManager`] it is initialised
/// by, pausing until the [`LifecycleHandle::can_resume_ingest()`] obtained from
/// it returns true, and TTBR / error metrics are emitted on a per-sequencer
/// basis.
///
/// [`LifecycleManager`]: crate::lifecycle::LifecycleManager
/// [`LifecycleHandle::can_resume_ingest()`]: crate::lifecycle::LifecycleHandle::can_resume_ingest()
#[derive(Debug)]
pub struct SequencedStreamHandler<I, O, T = SystemProvider> {
    /// A input stream of DML ops
    stream: I,
    /// An output sink that processes DML operations and applies them to
    /// in-memory state.
    sink: O,

    /// A handle to the [`LifecycleManager`] singleton that may periodically
    /// request ingest be paused to control memory pressure.
    ///
    /// [`LifecycleManager`]: crate::lifecycle::LifecycleManager
    lifecycle_handle: LifecycleHandle,

    // Metrics
    time_provider: T,
    time_to_be_readable_ms: U64Gauge,
    /// Duration of time ingest is paused at the request of the LifecycleManager
    pause_duration_ms: U64Counter,
    /// Errors during op stream reading
    seq_unknown_sequence_number_count: U64Counter,
    seq_invalid_data_count: U64Counter,
    seq_unknown_error_count: U64Counter,
    sink_apply_error_count: U64Counter,

    /// Log context fields - otherwise unused.
    kafka_topic_name: String,
    kafka_partition: KafkaPartition,
}

impl<I, O> SequencedStreamHandler<I, O> {
    /// Initialise a new [`SequencedStreamHandler`], consuming from `stream` and
    /// dispatching successfully decoded [`DmlOperation`] instances to `sink`.
    ///
    /// A [`SequencedStreamHandler`] starts actively consuming items from
    /// `stream` once [`SequencedStreamHandler::run()`] is called, and
    /// gracefully stops when `shutdown` is cancelled.
    pub fn new(
        stream: I,
        sink: O,
        lifecycle_handle: LifecycleHandle,
        kafka_topic_name: String,
        kafka_partition: KafkaPartition,
        metrics: &metric::Registry,
    ) -> Self {
        // TTBR
        let time_to_be_readable_ms = metrics.register_metric::<U64Gauge>(
            "ingester_ttbr_ms",
            "duration of time between producer writing to consumer putting into queryable cache in milliseconds",
        ).recorder(metric_attrs(kafka_partition, &kafka_topic_name, None, false));

        // Lifecycle-driven ingest pause duration
        let pause_duration_ms = metrics.register_metric::<U64Counter>(
            "ingest_paused_duration_ms_total",
            "duration of time ingestion has been paused by the lifecycle manager in milliseconds",
        ).recorder(&[]);

        // Error count metrics
        let ingest_errors = metrics.register_metric::<U64Counter>(
            "ingest_stream_handler_error",
            "ingester op fetching and buffering errors",
        );
        let seq_unknown_sequence_number_count = ingest_errors.recorder(metric_attrs(
            kafka_partition,
            &kafka_topic_name,
            Some("sequencer_unknown_sequence_number"),
            true,
        ));
        let seq_invalid_data_count = ingest_errors.recorder(metric_attrs(
            kafka_partition,
            &kafka_topic_name,
            Some("sequencer_invalid_data"),
            true,
        ));
        let seq_unknown_error_count = ingest_errors.recorder(metric_attrs(
            kafka_partition,
            &kafka_topic_name,
            Some("sequencer_unknown_error"),
            true,
        ));
        let sink_apply_error_count = ingest_errors.recorder(metric_attrs(
            kafka_partition,
            &kafka_topic_name,
            Some("sink_apply_error"),
            true,
        ));

        Self {
            stream,
            sink,
            lifecycle_handle,
            time_provider: SystemProvider::default(),
            time_to_be_readable_ms,
            pause_duration_ms,
            seq_unknown_sequence_number_count,
            seq_invalid_data_count,
            seq_unknown_error_count,
            sink_apply_error_count,
            kafka_topic_name,
            kafka_partition,
        }
    }

    /// Switch to the specified [`TimeProvider`] implementation.
    #[cfg(test)]
    pub(crate) fn with_time_provider<T>(self, provider: T) -> SequencedStreamHandler<I, O, T> {
        SequencedStreamHandler {
            stream: self.stream,
            sink: self.sink,
            lifecycle_handle: self.lifecycle_handle,
            time_provider: provider,
            time_to_be_readable_ms: self.time_to_be_readable_ms,
            pause_duration_ms: self.pause_duration_ms,
            seq_unknown_sequence_number_count: self.seq_unknown_sequence_number_count,
            seq_invalid_data_count: self.seq_invalid_data_count,
            seq_unknown_error_count: self.seq_unknown_error_count,
            sink_apply_error_count: self.sink_apply_error_count,
            kafka_topic_name: self.kafka_topic_name,
            kafka_partition: self.kafka_partition,
        }
    }
}

impl<I, O, T> SequencedStreamHandler<I, O, T>
where
    I: Stream<Item = Result<DmlOperation, WriteBufferError>> + Unpin + Send + Sync,
    O: DmlSink,
    T: TimeProvider,
{
    /// Run the stream handler, consuming items from [`Stream`] and applying
    /// them to the [`DmlSink`].
    ///
    /// This method blocks until gracefully shutdown by cancelling the
    /// `shutdown` [`CancellationToken`]. Once cancelled, this handler will
    /// complete the current operation it is processing before this method
    /// returns.
    ///
    /// # Panics
    ///
    /// This method panics if the input stream ends (yields a `None`).
    pub async fn run(mut self, shutdown: CancellationToken) {
        let shutdown_fut = shutdown.cancelled().fuse();
        pin_mut!(shutdown_fut);

        loop {
            // Wait for a DML operation from the sequencer, or a graceful stop
            // signal.
            let maybe_op = futures::select!(
                next = self.stream.next().fuse() => next,
                _ = shutdown_fut => {
                    info!(
                        kafka_topic=%self.kafka_topic_name,
                        kafka_partition=%self.kafka_partition,
                        "stream handler shutdown",
                    );
                    return;
                }
            );

            // Read a DML op from the write buffer, logging and emitting metrics
            // for any potential errors to enable alerting on potential data
            // loss.
            //
            // If this evaluation results in no viable DML op to apply to the
            // DmlSink, return None rather than continuing the loop to ensure
            // ingest pauses are respected.
            let maybe_op = match maybe_op {
                Some(Ok(op)) => Some(op),
                Some(Err(e)) if e.kind() == WriteBufferErrorKind::UnknownSequenceNumber => {
                    error!(
                        error=%e,
                        kafka_topic=%self.kafka_topic_name,
                        kafka_partition=%self.kafka_partition,
                        potential_data_loss=true,
                        "unable to read from desired sequencer offset"
                    );
                    self.seq_unknown_sequence_number_count.inc(1);
                    None
                }
                Some(Err(e)) if e.kind() == WriteBufferErrorKind::IO => {
                    warn!(
                        error=%e,
                        kafka_topic=%self.kafka_topic_name,
                        kafka_partition=%self.kafka_partition,
                        "I/O error reading from sequencer"
                    );
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    None
                }
                Some(Err(e)) if e.kind() == WriteBufferErrorKind::InvalidData => {
                    // The DmlOperation could not be de-serialised from the
                    // kafka message.
                    //
                    // This is almost certainly data loss as the write will not
                    // be applied/persisted.
                    error!(
                        error=%e,
                        kafka_topic=%self.kafka_topic_name,
                        kafka_partition=%self.kafka_partition,
                        potential_data_loss=true,
                        "unable to deserialise dml operation"
                    );

                    self.seq_invalid_data_count.inc(1);
                    None
                }
                Some(Err(e)) => {
                    error!(
                        error=%e,
                        kafka_topic=%self.kafka_topic_name,
                        kafka_partition=%self.kafka_partition,
                        potential_data_loss=true,
                        "unhandled error converting write buffer data to DmlOperation",
                    );
                    self.seq_unknown_error_count.inc(1);
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    None
                }
                None => {
                    panic!(
                        "sequencer {:?} stream for topic {} ended without graceful shutdown",
                        self.kafka_partition, self.kafka_topic_name
                    );
                }
            };

            // If a DML operation was successfully decoded, push it into the
            // DmlSink.
            self.maybe_apply_op(maybe_op).await;
        }
    }

    async fn maybe_apply_op(&self, op: Option<DmlOperation>) {
        if let Some(op) = op {
            // Extract the producer timestamp (added in the router when
            // dispatching the request).
            let produced_at = op.meta().producer_ts();

            let should_pause = match self.sink.apply(op).await {
                Ok(should_pause) => {
                    trace!(
                        kafka_topic=%self.kafka_topic_name,
                        kafka_partition=%self.kafka_partition,
                        "successfully applied dml operation"
                    );
                    should_pause
                }
                Err(e) => {
                    error!(
                        error=%e,
                        kafka_topic=%self.kafka_topic_name,
                        kafka_partition=%self.kafka_partition,
                        potential_data_loss=true,
                        "failed to apply dml operation"
                    );
                    self.sink_apply_error_count.inc(1);
                    return;
                }
            };

            // Update the TTBR metric before potentially sleeping.
            if let Some(delta) =
                produced_at.and_then(|ts| self.time_provider.now().checked_duration_since(ts))
            {
                self.time_to_be_readable_ms.set(delta.as_millis() as u64);
            }

            if should_pause {
                // The lifecycle manager may temporarily pause ingest - wait for
                // persist operations to shed memory pressure if needed.
                self.pause_ingest().await;
            }
        }
    }

    async fn pause_ingest(&self) {
        // Record how long this pause is, for logging purposes.
        let started_at = self.time_provider.now();

        warn!(
            kafka_topic=%self.kafka_topic_name,
            kafka_partition=%self.kafka_partition,
            "pausing ingest until persistence has run"
        );
        while !self.lifecycle_handle.can_resume_ingest() {
            // Incrementally report on the sleeps (as opposed to
            // measuring the start/end duration) in order to report
            // a blocked ingester _before_ it recovers.
            //
            // While the actual sleep may be slightly longer than
            // INGEST_POLL_INTERVAL, it's not likely to be a useful
            // distinction in the metrics.
            self.pause_duration_ms
                .inc(INGEST_POLL_INTERVAL.as_millis() as _);

            tokio::time::sleep(INGEST_POLL_INTERVAL).await;
        }

        let duration_str = self
            .time_provider
            .now()
            .checked_duration_since(started_at)
            .map(|v| format!("{}ms", v.as_millis()))
            .unwrap_or_else(|| "unknown".to_string());

        info!(
            kafka_topic=%self.kafka_topic_name,
            kafka_partition=%self.kafka_partition,
            pause_duration=%duration_str,
            "resuming ingest"
        );
    }
}

fn metric_attrs(
    partition: KafkaPartition,
    topic: &str,
    err: Option<&'static str>,
    data_loss: bool,
) -> Attributes {
    let mut attr = Attributes::from([
        ("sequencer_id", partition.to_string().into()),
        ("kafka_topic", topic.to_string().into()),
    ]);

    if let Some(err) = err {
        attr.insert("error", err)
    }

    if data_loss {
        attr.insert("potential_data_loss", "true");
    }

    attr
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::{
        lifecycle::{LifecycleConfig, LifecycleManager},
        stream_handler::mock_sink::MockDmlSink,
    };
    use assert_matches::assert_matches;
    use data_types2::{DeletePredicate, Sequence, TimestampRange};
    use dml::{DmlDelete, DmlMeta, DmlWrite};
    use futures::stream;
    use metric::Metric;
    use mutable_batch_lp::lines_to_batches;
    use test_helpers::timeout::FutureTimeout;
    use time::{SystemProvider, Time};
    use tokio::sync::mpsc;
    use tokio_stream::wrappers::ReceiverStream;

    lazy_static::lazy_static! {
        static ref TEST_TIME: Time = SystemProvider::default().now();
        static ref TEST_KAFKA_PARTITION: KafkaPartition = KafkaPartition::new(42);
    }
    static TEST_KAFKA_TOPIC: &str = "kafka_topic_name";

    // Return a DmlWrite with the given namespace and a single table.
    fn make_write(name: impl Into<String>, write_time: u64) -> DmlWrite {
        let tables = lines_to_batches("bananas level=42 4242", 0).unwrap();
        let sequence = DmlMeta::sequenced(
            Sequence::new(1, 2),
            TEST_TIME
                .checked_sub(Duration::from_millis(write_time))
                .unwrap(),
            None,
            42,
        );
        DmlWrite::new(name, tables, sequence)
    }

    // Return a DmlDelete with the given namespace.
    fn make_delete(name: impl Into<String>, write_time: u64) -> DmlDelete {
        let pred = DeletePredicate {
            range: TimestampRange::new(1, 2),
            exprs: vec![],
        };
        let sequence = DmlMeta::sequenced(
            Sequence::new(1, 2),
            TEST_TIME
                .checked_sub(Duration::from_millis(write_time))
                .unwrap(),
            None,
            42,
        );
        DmlDelete::new(name, pred, None, sequence)
    }

    // Generates a test that ensures that the handler given $stream_ops makes
    // $want_sink calls.
    //
    // Additionally all test cases assert the handler does not panic, and the
    // handler gracefully shuts down after the test input sequence is exhausted.
    macro_rules! test_stream_handler {
        (
            $name:ident,
            stream_ops = $stream_ops:expr,  // An ordered set of stream items to feed to the handler
            sink_rets = $sink_ret:expr,     // An ordered set of values to return from the mock op sink
            want_ttbr = $want_ttbr:literal, // Desired TTBR value in milliseconds
            // Optional set of ingest error metric label / values to assert
            want_err_metrics = [$($metric_name:literal => $metric_count:literal),*],
            want_sink = $($want_sink:tt)+   // A pattern to match against the calls made to the op sink
        ) => {
            paste::paste! {
                #[tokio::test]
                async fn [<test_stream_handler_ $name>]() {
                    let metrics = Arc::new(metric::Registry::default());
                    let time_provider: Arc< dyn TimeProvider> = Arc::new(SystemProvider::default());
                    let lifecycle = LifecycleManager::new(
                        LifecycleConfig::new(100, 2, 3, Duration::from_secs(4), Duration::from_secs(5)),
                        Arc::clone(&metrics),
                        time_provider,
                    );

                    // The DML sink that records ops.
                    let sink = Arc::new(
                        MockDmlSink::default()
                            .with_apply_return($sink_ret)
                    );

                    // Create an channel to pass input to the handler, with a
                    // buffer capacity of 1 (used below).
                    let (tx, rx) = mpsc::channel(1);

                    let handler = SequencedStreamHandler::new(
                        ReceiverStream::new(rx),
                        Arc::clone(&sink),
                        lifecycle.handle(),
                        TEST_KAFKA_TOPIC.to_string(),
                        *TEST_KAFKA_PARTITION,
                        &*metrics,
                    ).with_time_provider(time::MockProvider::new(*TEST_TIME));

                    // Run the handler in the background and push inputs to it
                    let shutdown = CancellationToken::default();
                    let handler_shutdown = shutdown.child_token();
                    let handler = tokio::spawn(async move {
                        handler.run(handler_shutdown).await;
                    });

                    // Push the input one at a time, wait for the the last
                    // message to be consumed by the handler (channel capacity
                    // increases to 1 once the message is read) and then request
                    // a graceful shutdown.
                    for op in $stream_ops {
                        tx.send(op)
                            .with_timeout_panic(Duration::from_secs(5))
                            .await
                            .expect("early handler exit");
                    }
                    // Wait for the handler to read the last op, restoring the
                    // capacity to 1.
                    let _permit = tx.reserve()
                        .with_timeout_panic(Duration::from_secs(5))
                        .await
                        .expect("early handler exit");

                    // Trigger graceful shutdown
                    shutdown.cancel();

                    // And wait for the handler to stop.
                    handler.with_timeout_panic(Duration::from_secs(5))
                        .await
                        .expect("handler did not shutdown");

                    // Assert the calls into the DML sink are as expected
                    let calls = sink.get_calls();
                    assert_matches!(calls.as_slice(), $($want_sink)+);

                    // Assert the TTBR metric value
                    let ttbr = metrics
                        .get_instrument::<Metric<U64Gauge>>("ingester_ttbr_ms")
                        .expect("did not find ttbr metric")
                        .get_observer(&Attributes::from([
                            ("kafka_topic", TEST_KAFKA_TOPIC.into()),
                            ("sequencer_id", TEST_KAFKA_PARTITION.to_string().into()),
                        ]))
                        .expect("did not match metric attributes")
                        .fetch();
                    assert_eq!(ttbr, $want_ttbr);

                    // Assert any error metrics in the macro call
                    $(
                        let got = metrics
                            .get_instrument::<Metric<U64Counter>>("ingest_stream_handler_error")
                            .expect("did not find error metric")
                            .get_observer(&metric_attrs(
                                *TEST_KAFKA_PARTITION,
                                TEST_KAFKA_TOPIC,
                                Some($metric_name),
                                true,
                            ))
                            .expect("did not match metric attributes")
                            .fetch();
                        assert_eq!(got, $metric_count, $metric_name);
                    )*
                }
            }
        };
    }

    test_stream_handler!(
        immediate_shutdown,
        stream_ops = [],
        sink_rets = [],
        want_ttbr = 0, // No ops, no TTBR
        want_err_metrics = [],
        want_sink = []
    );

    // Single write op applies OK, then shutdown.
    test_stream_handler!(
        write_ok,
        stream_ops = [
            Ok(DmlOperation::Write(make_write("bananas", 42)))
        ],
        sink_rets = [Ok(true)],
        want_ttbr = 42,
        want_err_metrics = [],
        want_sink = [DmlOperation::Write(op)] => {
            assert_eq!(op.namespace(), "bananas");
        }
    );

    // Single delete op applies OK, then shutdown.
    test_stream_handler!(
        delete_ok,
        stream_ops = [
            Ok(DmlOperation::Delete(make_delete("platanos", 24)))
        ],
        sink_rets = [Ok(true)],
        want_ttbr = 24,
        want_err_metrics = [],
        want_sink = [DmlOperation::Delete(op)] => {
            assert_eq!(op.namespace(), "platanos");
        }
    );

    // An error reading from the sequencer stream is processed and does not
    // affect the next op in the stream.
    test_stream_handler!(
        non_fatal_stream_io_error,
        stream_ops = [
            Err(WriteBufferError::new(WriteBufferErrorKind::IO, "explosions")),
            Ok(DmlOperation::Write(make_write("bananas", 13)))
        ],
        sink_rets = [Ok(true)],
        want_ttbr = 13,
        want_err_metrics = [
            // No error metrics for I/O errors
            "sequencer_unknown_sequence_number" => 0,
            "sequencer_invalid_data" => 0,
            "sequencer_unknown_error" => 0,
            "sink_apply_error" => 0
        ],
        want_sink = [DmlOperation::Write(op)] => {
            assert_eq!(op.namespace(), "bananas");
        }
    );
    test_stream_handler!(
        non_fatal_stream_offset_error,
        stream_ops = [
            Err(WriteBufferError::new(WriteBufferErrorKind::UnknownSequenceNumber, "explosions")),
            Ok(DmlOperation::Write(make_write("bananas", 31)))
        ],
        sink_rets = [Ok(true)],
        want_ttbr = 31,
        want_err_metrics = [
            "sequencer_unknown_sequence_number" => 1,
            "sequencer_invalid_data" => 0,
            "sequencer_unknown_error" => 0,
            "sink_apply_error" => 0
        ],
        want_sink = [DmlOperation::Write(op)] => {
            assert_eq!(op.namespace(), "bananas");
        }
    );
    test_stream_handler!(
        non_fatal_stream_invalid_data,
        stream_ops = [
            Err(WriteBufferError::new(WriteBufferErrorKind::InvalidData, "explosions")),
            Ok(DmlOperation::Write(make_write("bananas", 50)))
        ],
        sink_rets = [Ok(true)],
        want_ttbr = 50,
        want_err_metrics = [
            "sequencer_unknown_sequence_number" => 0,
            "sequencer_invalid_data" => 1,
            "sequencer_unknown_error" => 0,
            "sink_apply_error" => 0
        ],
        want_sink = [DmlOperation::Write(op)] => {
            assert_eq!(op.namespace(), "bananas");
        }
    );
    test_stream_handler!(
        non_fatal_stream_unknown_error,
        stream_ops = [
            Err(WriteBufferError::new(WriteBufferErrorKind::Unknown, "explosions")),
            Ok(DmlOperation::Write(make_write("bananas", 60)))
        ],
        sink_rets = [Ok(true)],
        want_ttbr = 60,
        want_err_metrics = [
            "sequencer_unknown_sequence_number" => 0,
            "sequencer_invalid_data" => 0,
            "sequencer_unknown_error" => 1,
            "sink_apply_error" => 0
        ],
        want_sink = [DmlOperation::Write(op)] => {
            assert_eq!(op.namespace(), "bananas");
        }
    );

    // Asserts the TTBR is not set unless an op is successfully sunk.
    test_stream_handler!(
        no_success_no_ttbr,
        stream_ops = [Err(WriteBufferError::new(
            WriteBufferErrorKind::IO,
            "explosions"
        )),],
        sink_rets = [],
        want_ttbr = 0,
        want_err_metrics = [],
        want_sink = []
    );

    // Asserts the TTBR is uses the last value in the stream.
    test_stream_handler!(
        reports_last_ttbr,
        stream_ops = [
            Ok(DmlOperation::Write(make_write("bananas", 1))),
            Ok(DmlOperation::Write(make_write("bananas", 2))),
            Ok(DmlOperation::Write(make_write("bananas", 3))),
            Ok(DmlOperation::Write(make_write("bananas", 42))),
        ],
        sink_rets = [Ok(true), Ok(false), Ok(true), Ok(false),],
        want_ttbr = 42,
        want_err_metrics = [
            // No errors!
            "sequencer_unknown_sequence_number" => 0,
            "sequencer_invalid_data" => 0,
            "sequencer_unknown_error" => 0,
            "sink_apply_error" => 0
        ],
        want_sink = _
    );

    // An error applying an op to the DmlSink is non-fatal and does not prevent
    // the next op in the stream from being processed.
    test_stream_handler!(
        non_fatal_sink_error,
        stream_ops = [
            Ok(DmlOperation::Write(make_write("bad_op", 1))),
            Ok(DmlOperation::Write(make_write("good_op", 2)))
        ],
        sink_rets = [
            Err(crate::data::Error::TimeColumnNotPresent),
            Ok(true),
        ],
        want_ttbr = 2,
        want_err_metrics = [
            "sequencer_unknown_sequence_number" => 0,
            "sequencer_invalid_data" => 0,
            "sequencer_unknown_error" => 0,
            "sink_apply_error" => 1
        ],
        want_sink = [
            DmlOperation::Write(_),  // First call into sink is bad_op, returning an error
            DmlOperation::Write(op), // Second call succeeds
        ] => {
            assert_eq!(op.namespace(), "good_op");
        }
    );

    // An abnormal end to the steam causes a panic, rather than a silent stream
    // reader exit.
    #[tokio::test]
    #[should_panic = "sequencer KafkaPartition(42) stream for topic kafka_topic_name ended without graceful shutdown"]
    async fn test_early_stream_end_panic() {
        let metrics = Arc::new(metric::Registry::default());
        let time_provider = Arc::new(SystemProvider::default());
        let lifecycle = LifecycleManager::new(
            LifecycleConfig::new(100, 2, 3, Duration::from_secs(4), Duration::from_secs(5)),
            Arc::clone(&metrics),
            time_provider,
        );

        // An empty stream iter immediately yields none.
        let stream = stream::iter([]);
        let sink = MockDmlSink::default();

        let handler = SequencedStreamHandler::new(
            stream,
            sink,
            lifecycle.handle(),
            "kafka_topic_name".to_string(),
            KafkaPartition::new(42),
            &*metrics,
        );

        handler
            .run(Default::default())
            .with_timeout_panic(Duration::from_secs(1))
            .await;
    }
}
