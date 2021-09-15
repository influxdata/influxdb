use metric::U64Counter;

#[derive(Debug)]
pub struct Metrics {
    ingest_entries_bytes_ok: U64Counter,

    ingest_entries_bytes_error: U64Counter,
}

impl Metrics {
    pub fn new(registry: &metric::Registry, db_name: impl Into<String>) -> Self {
        let db_name = db_name.into();
        let metric = registry
            .register_metric::<U64Counter>("ingest_entries_bytes", "total ingested entry bytes");

        Self {
            ingest_entries_bytes_ok: metric
                .recorder([("db_name", db_name.clone().into()), ("status", "ok".into())]),
            ingest_entries_bytes_error: metric
                .recorder([("db_name", db_name.into()), ("status", "error".into())]),
        }
    }

    /// Get a recorder for reporting entry ingest
    pub fn entry_ingest(&self, bytes: usize) -> EntryIngestRecorder<'_> {
        EntryIngestRecorder {
            metrics: self,
            recorded: false,
            bytes,
        }
    }
}

/// An RAII handle that records metrics for ingest
///
/// Records an error on drop unless `EntryIngestRecorder::success` invoked
pub struct EntryIngestRecorder<'a> {
    metrics: &'a Metrics,
    bytes: usize,
    recorded: bool,
}

impl<'a> EntryIngestRecorder<'a> {
    pub fn success(mut self) {
        self.recorded = true;
        self.metrics.ingest_entries_bytes_ok.inc(self.bytes as u64)
    }
}

impl<'a> Drop for EntryIngestRecorder<'a> {
    fn drop(&mut self) {
        if !self.recorded {
            self.metrics
                .ingest_entries_bytes_error
                .inc(self.bytes as u64);
        }
    }
}
