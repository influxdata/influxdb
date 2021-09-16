use hashbrown::HashMap;
use metric::{Attributes, Metric, U64Counter};
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
}

impl LineProtocolMetrics {
    pub fn new(registry: &metric::Registry) -> Self {
        Self {
            ingest_lines: registry.register_metric("ingest_lines", "total LP points ingested"),
            ingest_fields: registry
                .register_metric("ingest_fields", "total LP field values ingested"),
            ingest_bytes: registry.register_metric("ingest_bytes", "total LP bytes ingested"),
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
            }
            false => {
                metrics.ingest_lines_error.inc(lines as u64);
                metrics.ingest_fields_error.inc(fields as u64);
                metrics.ingest_bytes_error.inc(bytes as u64);
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

        attributes.insert("status", "error");
        let ingest_lines_error = metrics.ingest_lines.recorder(attributes.clone());
        let ingest_fields_error = metrics.ingest_fields.recorder(attributes.clone());
        let ingest_bytes_error = metrics.ingest_bytes.recorder(attributes.clone());

        Self {
            ingest_lines_ok,
            ingest_lines_error,
            ingest_fields_ok,
            ingest_fields_error,
            ingest_bytes_ok,
            ingest_bytes_error,
        }
    }
}
