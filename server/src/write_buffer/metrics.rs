use dml::DmlOperation;
use metric::{Attributes, DurationHistogram, Metric, ResultMetric, U64Counter, U64Gauge};
use std::time::Instant;

/// Metrics for data ingest via write buffer.
#[derive(Debug)]
pub struct WriteBufferIngestMetrics {
    db_name: String,

    ingest_count: Metric<U64Counter>,

    ingest_duration: Metric<DurationHistogram>,

    bytes_read: Metric<U64Counter>,

    last_sequence_number: Metric<U64Gauge>,

    sequence_number_lag: Metric<U64Gauge>,

    last_min_ts: Metric<U64Gauge>,

    last_max_ts: Metric<U64Gauge>,

    last_ingest_ts: Metric<U64Gauge>,
}

impl WriteBufferIngestMetrics {
    pub fn new(registry: &metric::Registry, db_name: impl Into<String>) -> Self {
        let ingest_count = registry.register_metric(
            "write_buffer_ingest_requests",
            "The total number of entries consumed from the sequencer",
        );
        let ingest_duration = registry.register_metric(
            "write_buffer_ingest_request_duration",
            "The distribution of latencies for ingesting data from the sequencer",
        );
        let bytes_read = registry.register_metric(
            "write_buffer_read_bytes",
            "Total number of bytes read from sequencer",
        );
        let last_sequence_number = registry.register_metric(
            "write_buffer_last_sequence_number",
            "Last consumed sequence number (e.g. Kafka offset)",
        );
        let sequence_number_lag = registry.register_metric(
            "write_buffer_sequence_number_lag",
            "The difference between the the last sequence number available (e.g. Kafka offset) and (= minus) last consumed sequence number",
        );
        let last_min_ts = registry.register_metric(
            "write_buffer_last_min_ts",
            "Minimum timestamp of last write as unix timestamp in nanoseconds",
        );
        let last_max_ts = registry.register_metric(
            "write_buffer_last_max_ts",
            "Maximum timestamp of last write as unix timestamp in nanoseconds",
        );
        let last_ingest_ts = registry.register_metric(
            "write_buffer_last_ingest_ts",
            "Last seen ingest timestamp as unix timestamp in nanoseconds",
        );

        Self {
            db_name: db_name.into(),
            ingest_count,
            ingest_duration,
            bytes_read,
            last_sequence_number,
            sequence_number_lag,
            last_min_ts,
            last_max_ts,
            last_ingest_ts,
        }
    }

    pub fn new_sequencer_metrics(&self, sequencer_id: u32) -> SequencerMetrics {
        let attributes = Attributes::from([
            ("db_name", self.db_name.clone().into()),
            ("sequencer_id", sequencer_id.to_string().into()),
        ]);

        SequencerMetrics {
            ingest_count: ResultMetric::new(&self.ingest_count, attributes.clone()),
            ingest_duration: ResultMetric::new(&self.ingest_duration, attributes.clone()),
            bytes_read: self.bytes_read.recorder(attributes.clone()),
            last_sequence_number: self.last_sequence_number.recorder(attributes.clone()),
            sequence_number_lag: self.sequence_number_lag.recorder(attributes.clone()),
            last_min_ts: self.last_min_ts.recorder(attributes.clone()),
            last_max_ts: self.last_max_ts.recorder(attributes.clone()),
            last_ingest_ts: self.last_ingest_ts.recorder(attributes),
        }
    }
}

/// Metrics for a single sequencer.
#[derive(Debug)]
pub struct SequencerMetrics {
    ingest_count: ResultMetric<U64Counter>,

    ingest_duration: ResultMetric<DurationHistogram>,

    /// Bytes read from sequencer.
    bytes_read: U64Counter,

    /// Last consumed sequence number (e.g. Kafka offset).
    last_sequence_number: U64Gauge,

    // The difference between the the last sequence number available (e.g. Kafka offset) and (= minus) last consumed
    // sequence number.
    sequence_number_lag: U64Gauge,

    /// Minimum timestamp of last write as unix timestamp in nanoseconds.
    last_min_ts: U64Gauge,

    /// Maximum timestamp of last write as unix timestamp in nanoseconds.
    last_max_ts: U64Gauge,

    /// Last seen ingest timestamp as unix timestamp in nanoseconds.
    last_ingest_ts: U64Gauge,
}

impl SequencerMetrics {
    /// Get a recorder that automatically records an error on drop
    pub fn recorder(&mut self, watermark: u64) -> IngestRecorder<'_> {
        IngestRecorder {
            operation: None,
            metrics: Some(self),
            watermark,
            start_time: Instant::now(),
        }
    }
}

/// A helper abstraction that records a failed ingest on Drop unless a call
/// has been made to `IngestRecorder::success`
///
/// Records a client_error if dropped before a call to `IngestRecorder::entry`, as this
/// indicates the write buffer contents were invalid, otherwise records a server_error
pub struct IngestRecorder<'a> {
    watermark: u64,
    start_time: Instant,

    /// The `IngestRecorder` is initially created without an operation in case of decode error
    operation: Option<&'a DmlOperation>,

    /// The SequencerMetrics are taken out of this on record to both avoid duplicate
    /// recording and also work around lifetime shenanigans
    metrics: Option<&'a mut SequencerMetrics>,
}

impl<'a> IngestRecorder<'a> {
    pub fn operation(mut self, operation: &'a DmlOperation) -> IngestRecorder<'a> {
        assert!(self.operation.is_none());
        Self {
            operation: Some(operation),
            metrics: self.metrics.take(),
            watermark: self.watermark,
            start_time: self.start_time,
        }
    }

    pub fn success(mut self) {
        self.record(true)
    }

    fn record(&mut self, success: bool) {
        let duration = self.start_time.elapsed();
        let metrics = self.metrics.take().expect("record called twice");

        if let Some(operation) = self.operation.as_ref() {
            let meta = operation.meta();
            let producer_ts = meta
                .producer_ts()
                .expect("entry from write buffer must have a producer wallclock time");

            let sequence = meta
                .sequence()
                .expect("entry from write buffer must be sequenced");

            let bytes_read = meta
                .bytes_read()
                .expect("entry from write buffer should have size");

            metrics.bytes_read.inc(bytes_read as u64);
            metrics.last_sequence_number.set(sequence.number);
            metrics.sequence_number_lag.set(
                self.watermark
                    .saturating_sub(sequence.number)
                    .saturating_sub(1),
            );

            match operation {
                DmlOperation::Write(write) => {
                    metrics.last_min_ts.set(write.min_timestamp() as u64);
                    metrics.last_max_ts.set(write.max_timestamp() as u64);
                }
            }

            metrics
                .last_ingest_ts
                .set(producer_ts.timestamp_nanos() as u64);
        }

        match (success, self.operation.is_some()) {
            (true, true) => {
                // Successfully ingested entry
                metrics.ingest_duration.ok.record(duration);
                metrics.ingest_count.ok.inc(1);
            }
            (false, false) => {
                // Invalid sequenced entry
                metrics.ingest_duration.client_error.record(duration);
                metrics.ingest_count.client_error.inc(1);
            }
            (false, true) => {
                // Failed to ingest entry
                metrics.ingest_duration.server_error.record(duration);
                metrics.ingest_count.server_error.inc(1);
            }
            _ => panic!("succeeded with no entry!"),
        }
    }
}

impl<'a> Drop for IngestRecorder<'a> {
    fn drop(&mut self) {
        if self.metrics.is_some() {
            self.record(false)
        }
    }
}
