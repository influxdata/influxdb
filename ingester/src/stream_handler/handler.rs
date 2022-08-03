use super::DmlSink;
use crate::lifecycle::{LifecycleHandle, LifecycleHandleImpl};
use data_types::{KafkaPartition, SequenceNumber};
use dml::DmlOperation;
use futures::{pin_mut, FutureExt, StreamExt};
use iox_time::{SystemProvider, TimeProvider};
use metric::{Attributes, DurationCounter, DurationGauge, U64Counter};
use observability_deps::tracing::*;
use std::{fmt::Debug, time::Duration};
use tokio_util::sync::CancellationToken;
use write_buffer::core::{WriteBufferErrorKind, WriteBufferStreamHandler};

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
    /// Creator/manager of the stream of DML ops
    write_buffer_stream_handler: I,

    current_sequence_number: SequenceNumber,

    /// An output sink that processes DML operations and applies them to
    /// in-memory state.
    sink: O,

    /// A handle to the [`LifecycleManager`] singleton that may periodically
    /// request ingest be paused to control memory pressure.
    ///
    /// [`LifecycleManager`]: crate::lifecycle::LifecycleManager
    lifecycle_handle: LifecycleHandleImpl,

    // Metrics
    time_provider: T,
    time_to_be_readable: DurationGauge,

    /// Duration of time ingest is paused at the request of the LifecycleManager
    pause_duration: DurationCounter,

    /// Errors during op stream reading
    seq_unknown_sequence_number_count: U64Counter,
    seq_invalid_data_count: U64Counter,
    seq_unknown_error_count: U64Counter,
    sink_apply_error_count: U64Counter,
    skipped_sequence_number_amount: U64Counter,

    /// Reset count
    sequencer_reset_count: U64Counter,

    /// Log context fields - otherwise unused.
    kafka_topic_name: String,
    kafka_partition: KafkaPartition,

    skip_to_oldest_available: bool,
}

impl<I, O> SequencedStreamHandler<I, O> {
    /// Initialise a new [`SequencedStreamHandler`], consuming from `stream` and
    /// dispatching successfully decoded [`DmlOperation`] instances to `sink`.
    ///
    /// A [`SequencedStreamHandler`] starts actively consuming items from
    /// `stream` once [`SequencedStreamHandler::run()`] is called, and
    /// gracefully stops when `shutdown` is cancelled.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        write_buffer_stream_handler: I,
        current_sequence_number: SequenceNumber,
        sink: O,
        lifecycle_handle: LifecycleHandleImpl,
        kafka_topic_name: String,
        kafka_partition: KafkaPartition,
        metrics: &metric::Registry,
        skip_to_oldest_available: bool,
    ) -> Self {
        // TTBR
        let time_to_be_readable = metrics.register_metric::<DurationGauge>(
            "ingester_ttbr",
            "duration of time between producer writing to consumer putting into queryable cache",
        ).recorder(metric_attrs(kafka_partition, &kafka_topic_name, None, false));

        // Lifecycle-driven ingest pause duration
        let pause_duration = metrics
            .register_metric::<DurationCounter>(
                "ingester_paused_duration_total",
                "duration of time ingestion has been paused by the lifecycle manager",
            )
            .recorder(&[]);

        // Error count metrics
        let ingest_errors = metrics.register_metric::<U64Counter>(
            "ingester_stream_handler_error",
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
        let skipped_sequence_number_amount = ingest_errors.recorder(metric_attrs(
            kafka_partition,
            &kafka_topic_name,
            Some("skipped_sequence_number_amount"),
            true,
        ));

        // reset count
        let sequencer_reset_count = metrics
            .register_metric::<U64Counter>(
                "sequencer_reset_count",
                "how often a sequencer was already reset",
            )
            .recorder(metric_attrs(kafka_partition, &kafka_topic_name, None, true));

        Self {
            write_buffer_stream_handler,
            current_sequence_number,
            sink,
            lifecycle_handle,
            time_provider: SystemProvider::default(),
            time_to_be_readable,
            pause_duration,
            seq_unknown_sequence_number_count,
            seq_invalid_data_count,
            seq_unknown_error_count,
            sink_apply_error_count,
            skipped_sequence_number_amount,
            sequencer_reset_count,
            kafka_topic_name,
            kafka_partition,
            skip_to_oldest_available,
        }
    }

    /// Switch to the specified [`TimeProvider`] implementation.
    #[cfg(test)]
    pub(crate) fn with_time_provider<T>(self, provider: T) -> SequencedStreamHandler<I, O, T> {
        SequencedStreamHandler {
            write_buffer_stream_handler: self.write_buffer_stream_handler,
            current_sequence_number: self.current_sequence_number,
            sink: self.sink,
            lifecycle_handle: self.lifecycle_handle,
            time_provider: provider,
            time_to_be_readable: self.time_to_be_readable,
            pause_duration: self.pause_duration,
            seq_unknown_sequence_number_count: self.seq_unknown_sequence_number_count,
            seq_invalid_data_count: self.seq_invalid_data_count,
            seq_unknown_error_count: self.seq_unknown_error_count,
            sink_apply_error_count: self.sink_apply_error_count,
            skipped_sequence_number_amount: self.skipped_sequence_number_amount,
            sequencer_reset_count: self.sequencer_reset_count,
            kafka_topic_name: self.kafka_topic_name,
            kafka_partition: self.kafka_partition,
            skip_to_oldest_available: self.skip_to_oldest_available,
        }
    }
}

impl<I, O, T> SequencedStreamHandler<I, O, T>
where
    I: WriteBufferStreamHandler,
    O: DmlSink,
    T: TimeProvider,
{
    /// Run the stream handler, consuming items from the stream provided by the
    /// [`WriteBufferStreamHandler`] and applying them to the [`DmlSink`].
    ///
    /// This method blocks until gracefully shutdown by cancelling the
    /// `shutdown` [`CancellationToken`]. Once cancelled, this handler will
    /// complete the current operation it is processing before this method
    /// returns.
    ///
    /// # Panics
    ///
    /// This method panics if the input stream ends (yields a `None`).
    pub async fn run(mut self, shutdown: CancellationToken) {
        let shutdown_fut = shutdown.cancelled().fuse();
        pin_mut!(shutdown_fut);

        let mut stream = self.write_buffer_stream_handler.stream().await;
        let mut sequence_number_before_reset: Option<SequenceNumber> = None;

        loop {
            // Wait for a DML operation from the sequencer, or a graceful stop signal.
            let maybe_op = futures::select!(
                next = stream.next().fuse() => next,
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
                Some(Ok(op)) => {
                    if let Some(sequence_number) = op.meta().sequence().map(|s| s.sequence_number) {
                        if let Some(before_reset) = sequence_number_before_reset {
                            // We've requested the stream to be reset and we've skipped this many
                            // sequence numbers. Store in a metric once.
                            if before_reset != sequence_number {
                                let difference = sequence_number.get() - before_reset.get();
                                self.skipped_sequence_number_amount.inc(difference as u64);
                            }
                            sequence_number_before_reset = None;
                        }
                        self.current_sequence_number = sequence_number;
                    }

                    Some(op)
                }
                Some(Err(e)) if e.kind() == WriteBufferErrorKind::UnknownSequenceNumber => {
                    // If we get an unknown sequence number, and we're fine potentially having
                    // missed writes that were too old to be retained, try resetting the stream
                    // once and getting the next operation again.
                    // Keep the current sequence number to compare with the sequence number
                    if self.skip_to_oldest_available && sequence_number_before_reset.is_none() {
                        warn!(
                            error=%e,
                            kafka_topic=%self.kafka_topic_name,
                            kafka_partition=%self.kafka_partition,
                            potential_data_loss=true,
                            "reset stream"
                        );
                        self.sequencer_reset_count.inc(1);
                        sequence_number_before_reset = Some(self.current_sequence_number);
                        self.write_buffer_stream_handler.reset_to_earliest();
                        stream = self.write_buffer_stream_handler.stream().await;
                        continue;
                    } else {
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
                    // The DmlOperation could not be de-serialized from the
                    // kafka message.
                    //
                    // This is almost certainly data loss as the write will not
                    // be applied/persisted.
                    error!(
                        error=%e,
                        kafka_topic=%self.kafka_topic_name,
                        kafka_partition=%self.kafka_partition,
                        potential_data_loss=true,
                        "unable to deserialize dml operation"
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

    async fn maybe_apply_op(&mut self, op: Option<DmlOperation>) {
        if let Some(op) = op {
            // Emit per-op debug info.
            trace!(
                kafka_topic=%self.kafka_topic_name,
                kafka_partition=%self.kafka_partition,
                op_size=op.size(),
                op_namespace=op.namespace(),
                op_sequence_number=?op.meta().sequence().map(|s| s.sequence_number),
                "decoded dml operation"
            );

            // Calculate how long it has been since production by
            // checking the producer timestamp (added in the router
            // when dispatching the request).
            let duration_since_production =
                op.meta().duration_since_production(&self.time_provider);

            let should_pause = match self.sink.apply(op).await {
                Ok(should_pause) => {
                    trace!(
                        kafka_topic=%self.kafka_topic_name,
                        kafka_partition=%self.kafka_partition,
                        %should_pause,
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

            if let Some(delta) = duration_since_production {
                // Update the TTBR metric before potentially sleeping.
                self.time_to_be_readable.set(delta);
            }

            if should_pause {
                // The lifecycle manager may temporarily pause ingest - wait for
                // persist operations to shed memory pressure if needed.
                self.pause_ingest().await;
            }
        }
    }

    async fn pause_ingest(&mut self) {
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
            self.pause_duration.inc(INGEST_POLL_INTERVAL);

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
        ("kafka_partition", partition.to_string().into()),
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
    use super::*;
    use crate::{
        lifecycle::{LifecycleConfig, LifecycleManager},
        stream_handler::mock_sink::MockDmlSink,
    };
    use assert_matches::assert_matches;
    use async_trait::async_trait;
    use data_types::{DeletePredicate, Sequence, TimestampRange};
    use dml::{DmlDelete, DmlMeta, DmlWrite};
    use futures::stream::{self, BoxStream};
    use iox_time::{SystemProvider, Time};
    use metric::Metric;
    use mutable_batch_lp::lines_to_batches;
    use once_cell::sync::Lazy;
    use std::sync::Arc;
    use test_helpers::timeout::FutureTimeout;
    use tokio::sync::{mpsc, oneshot};
    use tokio_stream::wrappers::ReceiverStream;
    use write_buffer::core::WriteBufferError;

    static TEST_TIME: Lazy<Time> = Lazy::new(|| SystemProvider::default().now());
    static TEST_KAFKA_PARTITION: Lazy<KafkaPartition> = Lazy::new(|| KafkaPartition::new(42));
    static TEST_KAFKA_TOPIC: &str = "kafka_topic_name";

    // Return a DmlWrite with the given namespace and a single table.
    fn make_write(name: impl Into<String>, write_time: u64) -> DmlWrite {
        let tables = lines_to_batches("bananas level=42 4242", 0).unwrap();
        let sequence = DmlMeta::sequenced(
            Sequence::new(1, SequenceNumber::new(2)),
            TEST_TIME
                .checked_sub(Duration::from_millis(write_time))
                .unwrap(),
            None,
            42,
        );
        DmlWrite::new(name, tables, Some("1970-01-01".into()), sequence)
    }

    // Return a DmlDelete with the given namespace.
    fn make_delete(name: impl Into<String>, write_time: u64) -> DmlDelete {
        let pred = DeletePredicate {
            range: TimestampRange::new(1, 2),
            exprs: vec![],
        };
        let sequence = DmlMeta::sequenced(
            Sequence::new(1, SequenceNumber::new(2)),
            TEST_TIME
                .checked_sub(Duration::from_millis(write_time))
                .unwrap(),
            None,
            42,
        );
        DmlDelete::new(name, pred, None, sequence)
    }

    #[derive(Debug)]
    struct TestWriteBufferStreamHandler {
        stream_ops: Vec<Vec<Result<DmlOperation, WriteBufferError>>>,
        #[allow(clippy::type_complexity)]
        completed_tx:
            Option<oneshot::Sender<(mpsc::Sender<Result<DmlOperation, WriteBufferError>>, usize)>>,
    }

    impl TestWriteBufferStreamHandler {
        fn new(
            stream_ops: Vec<Vec<Result<DmlOperation, WriteBufferError>>>,
            completed_tx: oneshot::Sender<(
                mpsc::Sender<Result<DmlOperation, WriteBufferError>>,
                usize,
            )>,
        ) -> Self {
            Self {
                // reverse the order so we can pop off the end
                stream_ops: stream_ops.into_iter().rev().collect(),
                completed_tx: Some(completed_tx),
            }
        }
    }

    #[async_trait]
    impl WriteBufferStreamHandler for TestWriteBufferStreamHandler {
        async fn stream(&mut self) -> BoxStream<'static, Result<DmlOperation, WriteBufferError>> {
            let stream_ops = self.stream_ops.pop().unwrap();

            // Create a channel to pass input to the handler, with a
            // buffer capacity of the number of operations to send (used to tell if all
            // values have been received in the test thread).
            let capacity = if stream_ops.is_empty() {
                1 // channels can't have capacity 0, even if we're never sending anything
            } else {
                stream_ops.len()
            };
            let (tx, rx) = mpsc::channel(capacity);

            // Push all inputs
            for op in stream_ops {
                tx.send(op)
                    .with_timeout_panic(Duration::from_secs(5))
                    .await
                    .expect("early handler exit");
            }

            // If this is the last expected call to stream,
            // Send the transmitter and the capacity back to the test thread to wait for completion.
            if self.stream_ops.is_empty() {
                self.completed_tx
                    .take()
                    .unwrap()
                    .send((tx, capacity))
                    .unwrap();
            }

            ReceiverStream::new(rx).boxed()
        }

        async fn seek(&mut self, _sequence_number: SequenceNumber) -> Result<(), WriteBufferError> {
            Ok(())
        }

        fn reset_to_earliest(&mut self) {
            // Intentionally left blank
        }
    }

    // Generates a test that ensures that the handler given $stream_ops makes
    // $want_sink calls.
    //
    // Additionally all test cases assert the handler does not panic, and the
    // handler gracefully shuts down after the test input sequence is exhausted.
    macro_rules! test_stream_handler {
        (
            $name:ident,
            // Whether to skip to the oldest available sequence number if UnknownSequenceNumber
            skip_to_oldest_available = $skip_to_oldest_available:expr,
            stream_ops = $stream_ops:expr,  // Ordered set of stream items to feed to the handler
            sink_rets = $sink_ret:expr,     // Ordered set of values to return from the mock op sink
            want_ttbr = $want_ttbr:literal, // Desired TTBR value in milliseconds
            want_reset = $want_reset:literal,  // Desired reset counter value
            // Optional set of ingest error metric label / values to assert
            want_err_metrics = [$($metric_name:literal => $metric_count:literal),*],
            want_sink = $($want_sink:tt)+   // Pattern to match against calls made to the op sink
        ) => {
            paste::paste! {
                #[tokio::test]
                async fn [<test_stream_handler_ $name>]() {
                    let metrics = Arc::new(metric::Registry::default());
                    let time_provider: Arc<dyn TimeProvider> = Arc::new(SystemProvider::default());
                    let lifecycle = LifecycleManager::new(
                        LifecycleConfig::new(
                            100, 2, 3, Duration::from_secs(4), Duration::from_secs(5)
                        ),
                        Arc::clone(&metrics),
                        time_provider,
                    );

                    // The DML sink that records ops.
                    let sink = Arc::new(
                        MockDmlSink::default()
                            .with_apply_return($sink_ret)
                    );

                    let (completed_tx, completed_rx) = oneshot::channel();
                    let write_buffer_stream_handler = TestWriteBufferStreamHandler::new(
                        $stream_ops,
                        completed_tx
                    );

                    let handler = SequencedStreamHandler::new(
                        write_buffer_stream_handler,
                        SequenceNumber::new(0),
                        Arc::clone(&sink),
                        lifecycle.handle(),
                        TEST_KAFKA_TOPIC.to_string(),
                        *TEST_KAFKA_PARTITION,
                        &*metrics,
                        $skip_to_oldest_available,
                    ).with_time_provider(iox_time::MockProvider::new(*TEST_TIME));

                    // Run the handler in the background and push inputs to it
                    let shutdown = CancellationToken::default();
                    let handler_shutdown = shutdown.child_token();
                    let handler = tokio::spawn(async move {
                        handler.run(handler_shutdown).await;
                    });

                    // When all operations have been read through the TestWriteBufferStreamHandler,
                    let (tx, capacity) = completed_rx.await.unwrap();

                    // Wait for the handler to read the last op,
                    async {
                        loop {
                            tokio::time::sleep(Duration::from_millis(10)).await;
                            if tx.capacity() == capacity {
                                return;
                            }
                        }
                    }.with_timeout_panic(Duration::from_secs(5))
                        .await;


                    // Then trigger graceful shutdown
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
                        .get_instrument::<Metric<DurationGauge>>("ingester_ttbr")
                        .expect("did not find ttbr metric")
                        .get_observer(&Attributes::from([
                            ("kafka_topic", TEST_KAFKA_TOPIC.into()),
                            ("kafka_partition", TEST_KAFKA_PARTITION.to_string().into()),
                        ]))
                        .expect("did not match metric attributes")
                        .fetch();
                    assert_eq!(ttbr, Duration::from_millis($want_ttbr));

                    // assert reset counter
                    let reset = metrics
                        .get_instrument::<Metric<U64Counter>>("sequencer_reset_count")
                        .expect("did not find reset count metric")
                        .get_observer(&Attributes::from([
                            ("kafka_topic", TEST_KAFKA_TOPIC.into()),
                            ("kafka_partition", TEST_KAFKA_PARTITION.to_string().into()),
                            ("potential_data_loss", "true".into()),
                        ]))
                        .expect("did not match metric attributes")
                        .fetch();
                    assert_eq!(reset, $want_reset);

                    // Assert any error metrics in the macro call
                    $(
                        let got = metrics
                            .get_instrument::<Metric<U64Counter>>("ingester_stream_handler_error")
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
        skip_to_oldest_available = false,
        stream_ops = vec![vec![]],
        sink_rets = [],
        want_ttbr = 0, // No ops, no TTBR
        want_reset = 0,
        want_err_metrics = [],
        want_sink = []
    );

    // Single write op applies OK, then shutdown.
    test_stream_handler!(
        write_ok,
        skip_to_oldest_available = false,
        stream_ops = vec![
            vec![Ok(DmlOperation::Write(make_write("bananas", 42)))]
        ],
        sink_rets = [Ok(true)],
        want_ttbr = 42,
        want_reset = 0,
        want_err_metrics = [],
        want_sink = [DmlOperation::Write(op)] => {
            assert_eq!(op.namespace(), "bananas");
        }
    );

    // Single delete op applies OK, then shutdown.
    test_stream_handler!(
        delete_ok,
        skip_to_oldest_available = false,
        stream_ops = vec![
            vec![Ok(DmlOperation::Delete(make_delete("platanos", 24)))]
        ],
        sink_rets = [Ok(true)],
        want_ttbr = 24,
        want_reset = 0,
        want_err_metrics = [],
        want_sink = [DmlOperation::Delete(op)] => {
            assert_eq!(op.namespace(), "platanos");
        }
    );

    // An error reading from the sequencer stream is processed and does not
    // affect the next op in the stream.
    test_stream_handler!(
        non_fatal_stream_io_error,
        skip_to_oldest_available = false,
        stream_ops = vec![vec![
            Err(WriteBufferError::new(WriteBufferErrorKind::IO, "explosions")),
            Ok(DmlOperation::Write(make_write("bananas", 13)))
        ]],
        sink_rets = [Ok(true)],
        want_ttbr = 13,
        want_reset = 0,
        want_err_metrics = [
            // No error metrics for I/O errors
            "sequencer_unknown_sequence_number" => 0,
            "sequencer_invalid_data" => 0,
            "sequencer_unknown_error" => 0,
            "sink_apply_error" => 0,
            "skipped_sequence_number_amount" => 0
        ],
        want_sink = [DmlOperation::Write(op)] => {
            assert_eq!(op.namespace(), "bananas");
        }
    );
    test_stream_handler!(
        non_fatal_stream_offset_error,
        skip_to_oldest_available = false,
        stream_ops = vec![vec![
            Err(WriteBufferError::new(WriteBufferErrorKind::UnknownSequenceNumber, "explosions")),
            Ok(DmlOperation::Write(make_write("bananas", 31)))
        ]],
        sink_rets = [Ok(true)],
        want_ttbr = 31,
        want_reset = 0,
        want_err_metrics = [
            "sequencer_unknown_sequence_number" => 1,
            "sequencer_invalid_data" => 0,
            "sequencer_unknown_error" => 0,
            "sink_apply_error" => 0,
            "skipped_sequence_number_amount" => 0
        ],
        want_sink = [DmlOperation::Write(op)] => {
            assert_eq!(op.namespace(), "bananas");
        }
    );
    test_stream_handler!(
        skip_to_oldest_on_unknown_sequence_number,
        skip_to_oldest_available = true,
        stream_ops = vec![
            vec![
                Err(
                    WriteBufferError::new(
                        WriteBufferErrorKind::UnknownSequenceNumber,
                        "explosions"
                    )
                )
            ],
            vec![Ok(DmlOperation::Write(make_write("bananas", 31)))],
        ],
        sink_rets = [Ok(true)],
        want_ttbr = 31,
        want_reset = 1,
        want_err_metrics = [
            "sequencer_unknown_sequence_number" => 0,
            "sequencer_invalid_data" => 0,
            "sequencer_unknown_error" => 0,
            "sink_apply_error" => 0,
            "skipped_sequence_number_amount" => 2
        ],
        want_sink = [DmlOperation::Write(op)] => {
            assert_eq!(op.namespace(), "bananas");
        }
    );
    test_stream_handler!(
        non_fatal_stream_invalid_data,
        skip_to_oldest_available = false,
        stream_ops = vec![vec![
            Err(WriteBufferError::new(WriteBufferErrorKind::InvalidData, "explosions")),
            Ok(DmlOperation::Write(make_write("bananas", 50)))
        ]],
        sink_rets = [Ok(true)],
        want_ttbr = 50,
        want_reset = 0,
        want_err_metrics = [
            "sequencer_unknown_sequence_number" => 0,
            "sequencer_invalid_data" => 1,
            "sequencer_unknown_error" => 0,
            "sink_apply_error" => 0,
            "skipped_sequence_number_amount" => 0
        ],
        want_sink = [DmlOperation::Write(op)] => {
            assert_eq!(op.namespace(), "bananas");
        }
    );
    test_stream_handler!(
        non_fatal_stream_unknown_error,
        skip_to_oldest_available = false,
        stream_ops = vec![vec![
            Err(WriteBufferError::new(WriteBufferErrorKind::Unknown, "explosions")),
            Ok(DmlOperation::Write(make_write("bananas", 60)))
        ]],
        sink_rets = [Ok(true)],
        want_ttbr = 60,
        want_reset = 0,
        want_err_metrics = [
            "sequencer_unknown_sequence_number" => 0,
            "sequencer_invalid_data" => 0,
            "sequencer_unknown_error" => 1,
            "sink_apply_error" => 0,
            "skipped_sequence_number_amount" => 0
        ],
        want_sink = [DmlOperation::Write(op)] => {
            assert_eq!(op.namespace(), "bananas");
        }
    );

    // Asserts the TTBR is not set unless an op is successfully sunk.
    test_stream_handler!(
        no_success_no_ttbr,
        skip_to_oldest_available = false,
        stream_ops = vec![vec![Err(WriteBufferError::new(
            WriteBufferErrorKind::IO,
            "explosions"
        ))]],
        sink_rets = [],
        want_ttbr = 0,
        want_reset = 0,
        want_err_metrics = [],
        want_sink = []
    );

    // Asserts the TTBR is uses the last value in the stream.
    test_stream_handler!(
        reports_last_ttbr,
        skip_to_oldest_available = false,
        stream_ops = vec![vec![
            Ok(DmlOperation::Write(make_write("bananas", 1))),
            Ok(DmlOperation::Write(make_write("bananas", 2))),
            Ok(DmlOperation::Write(make_write("bananas", 3))),
            Ok(DmlOperation::Write(make_write("bananas", 42))),
        ]],
        sink_rets = [Ok(true), Ok(false), Ok(true), Ok(false),],
        want_ttbr = 42,
        want_reset = 0,
        want_err_metrics = [
            // No errors!
            "sequencer_unknown_sequence_number" => 0,
            "sequencer_invalid_data" => 0,
            "sequencer_unknown_error" => 0,
            "sink_apply_error" => 0,
            "skipped_sequence_number_amount" => 0
        ],
        want_sink = _
    );

    // An error applying an op to the DmlSink is non-fatal and does not prevent
    // the next op in the stream from being processed.
    test_stream_handler!(
        non_fatal_sink_error,
        skip_to_oldest_available = false,
        stream_ops = vec![vec![
            Ok(DmlOperation::Write(make_write("bad_op", 1))),
            Ok(DmlOperation::Write(make_write("good_op", 2)))
        ]],
        sink_rets = [
            Err(crate::data::Error::TableNotPresent),
            Ok(true),
        ],
        want_ttbr = 2,
        want_reset = 0,
        want_err_metrics = [
            "sequencer_unknown_sequence_number" => 0,
            "sequencer_invalid_data" => 0,
            "sequencer_unknown_error" => 0,
            "sink_apply_error" => 1,
            "skipped_sequence_number_amount" => 0
        ],
        want_sink = [
            DmlOperation::Write(_),  // First call into sink is bad_op, returning an error
            DmlOperation::Write(op), // Second call succeeds
        ] => {
            assert_eq!(op.namespace(), "good_op");
        }
    );

    #[derive(Debug)]
    struct EmptyWriteBufferStreamHandler {}

    #[async_trait]
    impl WriteBufferStreamHandler for EmptyWriteBufferStreamHandler {
        async fn stream(&mut self) -> BoxStream<'static, Result<DmlOperation, WriteBufferError>> {
            stream::iter([]).boxed()
        }

        async fn seek(&mut self, _sequence_number: SequenceNumber) -> Result<(), WriteBufferError> {
            Ok(())
        }

        fn reset_to_earliest(&mut self) {
            // Intentionally left blank
        }
    }

    // An abnormal end to the steam causes a panic, rather than a silent stream reader exit.
    #[tokio::test]
    #[should_panic(
        expected = "sequencer KafkaPartition(42) stream for topic kafka_topic_name ended without \
                    graceful shutdown"
    )]
    async fn test_early_stream_end_panic() {
        let metrics = Arc::new(metric::Registry::default());
        let time_provider = Arc::new(SystemProvider::default());
        let lifecycle = LifecycleManager::new(
            LifecycleConfig::new(100, 2, 3, Duration::from_secs(4), Duration::from_secs(5)),
            Arc::clone(&metrics),
            time_provider,
        );

        // An empty stream iter immediately yields none.
        let write_buffer_stream_handler = EmptyWriteBufferStreamHandler {};
        let sink = MockDmlSink::default();

        let handler = SequencedStreamHandler::new(
            write_buffer_stream_handler,
            SequenceNumber::new(0),
            sink,
            lifecycle.handle(),
            "kafka_topic_name".to_string(),
            KafkaPartition::new(42),
            &*metrics,
            false,
        );

        handler
            .run(Default::default())
            .with_timeout_panic(Duration::from_secs(1))
            .await;
    }
}
