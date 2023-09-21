use hashbrown::HashMap;
use metric::{Attributes, Metric, U64Counter, U64Histogram, U64HistogramOptions};
use parking_lot::{MappedMutexGuard, Mutex, MutexGuard};

/// Line protocol ingest metrics
#[derive(Debug)]
pub struct LineProtocolMetrics {
    /// The number of LP lines ingested
    ingest_lines: Metric<U64Counter>,

    /// The number of LP fields ingested
    ingest_fields: Metric<U64Counter>,

    /// The number of LP bytes ingested
    ingest_bytes: Metric<U64Counter>,

    /// Distribution of LP batch sizes.
    ingest_batch_size_bytes: Metric<U64Histogram>,

    /// Database metrics keyed by database name
    databases: Mutex<HashMap<String, LineProtocolDatabaseMetrics>>,
}

/// Line protocol metrics for a given database
#[derive(Debug)]
struct LineProtocolDatabaseMetrics {
    /// The number of LP lines ingested successfully
    ingest_lines_ok: U64Counter,

    /// The number of LP lines ingested unsuccessfully
    ingest_lines_error: U64Counter,

    /// The number of LP fields ingested successfully
    ingest_fields_ok: U64Counter,

    /// The number of LP fields ingested unsuccessfully
    ingest_fields_error: U64Counter,

    /// The number of LP bytes ingested successfully
    ingest_bytes_ok: U64Counter,

    /// The number of LP bytes ingested unsuccessfully
    ingest_bytes_error: U64Counter,

    /// Distribution of LP batch sizes ingested successfully
    ingest_batch_size_bytes_ok: U64Histogram,

    /// Distribution of LP batch sizes ingested unsuccessfully
    ingest_batch_size_bytes_error: U64Histogram,
}

impl LineProtocolMetrics {
    pub fn new(registry: &metric::Registry) -> Self {
        Self {
            ingest_lines: registry.register_metric("ingest_lines", "total LP points ingested"),
            ingest_fields: registry
                .register_metric("ingest_fields", "total LP field values ingested"),
            ingest_bytes: registry.register_metric("ingest_bytes", "total LP bytes ingested"),
            ingest_batch_size_bytes: registry.register_metric_with_options(
                "ingest_batch_size_bytes",
                "distribution of ingested LP batch sizes",
                || {
                    U64HistogramOptions::new([
                        1024,
                        16 * 1024,
                        32 * 1024,
                        128 * 1024,
                        256 * 1024,
                        512 * 1024,
                        768 * 1024,
                        1024 * 1024,
                        4 * 1024 * 1024,
                        8 * 1024 * 1024,
                        16 * 1024 * 1024,
                        24 * 1024 * 1024,
                        32 * 1024 * 1024,
                        u64::MAX,
                    ])
                },
            ),
            databases: Default::default(),
        }
    }

    pub fn record_write(
        &self,
        db_name: &str,
        lines: usize,
        fields: usize,
        bytes: usize,
        success: bool,
    ) {
        let metrics = self.database_metrics(db_name);

        match success {
            true => {
                metrics.ingest_lines_ok.inc(lines as u64);
                metrics.ingest_fields_ok.inc(fields as u64);
                metrics.ingest_bytes_ok.inc(bytes as u64);
                metrics.ingest_batch_size_bytes_ok.record(bytes as u64);
            }
            false => {
                metrics.ingest_lines_error.inc(lines as u64);
                metrics.ingest_fields_error.inc(fields as u64);
                metrics.ingest_bytes_error.inc(bytes as u64);
                metrics.ingest_batch_size_bytes_error.record(bytes as u64);
            }
        }
    }

    fn database_metrics(&self, db_name: &str) -> MappedMutexGuard<'_, LineProtocolDatabaseMetrics> {
        MutexGuard::map(self.databases.lock(), |databases| {
            let (_, metrics) = databases
                .raw_entry_mut()
                .from_key(db_name)
                .or_insert_with(|| {
                    let metrics = LineProtocolDatabaseMetrics::new(self, db_name);
                    (db_name.to_string(), metrics)
                });
            metrics
        })
    }
}

impl LineProtocolDatabaseMetrics {
    fn new(metrics: &LineProtocolMetrics, db_name: &str) -> Self {
        let mut attributes = Attributes::from([("db_name", db_name.to_string().into())]);

        attributes.insert("status", "ok");
        let ingest_lines_ok = metrics.ingest_lines.recorder(attributes.clone());
        let ingest_fields_ok = metrics.ingest_fields.recorder(attributes.clone());
        let ingest_bytes_ok = metrics.ingest_bytes.recorder(attributes.clone());
        let ingest_batch_size_bytes_ok =
            metrics.ingest_batch_size_bytes.recorder(attributes.clone());

        attributes.insert("status", "error");
        let ingest_lines_error = metrics.ingest_lines.recorder(attributes.clone());
        let ingest_fields_error = metrics.ingest_fields.recorder(attributes.clone());
        let ingest_bytes_error = metrics.ingest_bytes.recorder(attributes.clone());
        let ingest_batch_size_bytes_error =
            metrics.ingest_batch_size_bytes.recorder(attributes.clone());

        Self {
            ingest_lines_ok,
            ingest_lines_error,
            ingest_fields_ok,
            ingest_fields_error,
            ingest_bytes_ok,
            ingest_bytes_error,
            ingest_batch_size_bytes_ok,
            ingest_batch_size_bytes_error,
        }
    }
}
