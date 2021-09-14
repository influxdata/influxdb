use metrics::KeyValue;
use std::sync::Arc;

/// Metrics for data ingest via write buffer.
#[derive(Debug)]
pub struct WriteBufferIngestMetrics {
    /// Metrics domain
    domain: Arc<metrics::Domain>,
}

impl WriteBufferIngestMetrics {
    pub fn new(domain: Arc<metrics::Domain>) -> Self {
        Self { domain }
    }

    pub fn new_sequencer_metrics(&self, sequencer_id: u32) -> SequencerMetrics {
        let attributes = vec![KeyValue::new("sequencer_id", sequencer_id.to_string())];

        let red = self
            .domain
            .register_red_metric_with_attributes(Some("ingest"), attributes.clone());
        let bytes_read = self.domain.register_counter_metric_with_attributes(
            "read",
            Some("bytes"),
            "Bytes read from sequencer",
            attributes.clone(),
        );
        let last_sequence_number = self.domain.register_gauge_metric_with_attributes(
            "last_sequence_number",
            None,
            "Last consumed sequence number (e.g. Kafka offset)",
            &attributes,
        );
        let sequence_number_lag = self.domain.register_gauge_metric_with_attributes(
            "sequence_number_lag",
            None,
            "The difference between the the last sequence number available (e.g. Kafka offset) and (= minus) last consumed sequence number",
            &attributes,
        );
        let last_min_ts = self.domain.register_gauge_metric_with_attributes(
            "last_min_ts",
            None,
            "Minimum timestamp of last write as unix timestamp in nanoseconds",
            &attributes,
        );
        let last_max_ts = self.domain.register_gauge_metric_with_attributes(
            "last_max_ts",
            None,
            "Maximum timestamp of last write as unix timestamp in nanoseconds",
            &attributes,
        );
        let last_ingest_ts = self.domain.register_gauge_metric_with_attributes(
            "last_ingest_ts",
            None,
            "Last seen ingest timestamp as unix timestamp in nanoseconds",
            &attributes,
        );

        SequencerMetrics {
            red,
            bytes_read,
            last_sequence_number,
            sequence_number_lag,
            last_min_ts,
            last_max_ts,
            last_ingest_ts,
        }
    }
}

/// Metrics for a single sequencer.
#[derive(Debug)]
pub struct SequencerMetrics {
    /// Metrics for tracking ingest.
    pub(super) red: metrics::RedMetric,

    /// Bytes read from sequencer.
    ///
    /// This metrics is independent of the success / error state of the entries.
    bytes_read: metrics::Counter,

    /// Last consumed sequence number (e.g. Kafka offset).
    last_sequence_number: metrics::Gauge,

    // The difference between the the last sequence number available (e.g. Kafka offset) and (= minus) last consumed
    // sequence number.
    sequence_number_lag: metrics::Gauge,

    /// Minimum timestamp of last write as unix timestamp in nanoseconds.
    last_min_ts: metrics::Gauge,

    /// Maximum timestamp of last write as unix timestamp in nanoseconds.
    last_max_ts: metrics::Gauge,

    /// Last seen ingest timestamp as unix timestamp in nanoseconds.
    last_ingest_ts: metrics::Gauge,
}

impl SequencerMetrics {
    /// Record a succesful write
    ///
    /// Updates:
    ///
    /// - bytes read
    /// - last sequence number
    /// - lag
    /// - min ts
    /// - max ts
    /// - ingest ts
    pub fn record_write(&mut self, sequenced_entry: &entry::SequencedEntry, watermark: u64) {
        let entry = sequenced_entry.entry();
        let producer_wallclock_timestamp = sequenced_entry
            .producer_wallclock_timestamp()
            .expect("entry from write buffer must have a producer wallclock time");

        let sequence = sequenced_entry
            .sequence()
            .expect("entry from write buffer must be sequenced");

        self.bytes_read.add(entry.data().len() as u64);
        self.last_sequence_number.set(sequence.number as usize, &[]);
        self.sequence_number_lag.set(
            watermark.saturating_sub(sequence.number).saturating_sub(1) as usize,
            &[],
        );
        if let Some(min_ts) = entry
            .partition_writes()
            .map(|partition_writes| {
                partition_writes
                    .iter()
                    .filter_map(|partition_write| {
                        partition_write
                            .table_batches()
                            .iter()
                            .filter_map(|table_batch| table_batch.min_max_time().ok())
                            .map(|(min, _max)| min)
                            .max()
                    })
                    .min()
            })
            .flatten()
        {
            self.last_min_ts.set(min_ts.timestamp_nanos() as usize, &[]);
        }
        if let Some(max_ts) = entry
            .partition_writes()
            .map(|partition_writes| {
                partition_writes
                    .iter()
                    .filter_map(|partition_write| {
                        partition_write
                            .table_batches()
                            .iter()
                            .filter_map(|table_batch| table_batch.min_max_time().ok())
                            .map(|(_min, max)| max)
                            .max()
                    })
                    .max()
            })
            .flatten()
        {
            self.last_max_ts.set(max_ts.timestamp_nanos() as usize, &[]);
        }
        self.last_ingest_ts
            .set(producer_wallclock_timestamp.timestamp_nanos() as usize, &[]);
    }
}
