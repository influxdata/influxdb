use super::run::Config;
use observability_deps::{
    opentelemetry::{self, metrics::Counter},
    opentelemetry_prometheus,
    prometheus::{Encoder, TextEncoder},
    tracing::log::warn,
};
use once_cell::sync::Lazy;
use parking_lot::{const_rwlock, RwLock};

///! IOx metrics interface

/// Contains the counters for system wide metrics.
///
/// These are gouped into a single crate-wide reference to keep all
/// metrics definitions close together, so it is be clear what the
/// realm of possibilities for IOx metrics is.
///
/// We think this structure will also make it easier to document and
/// refactor metrics easier.
pub struct SystemMetrics {
    /// How many line protocol lines were parsed but rejected
    pub lp_lines_errors: Counter<u64>,
    /// How many line protocol lines were parsed and successfully loaded
    pub lp_lines_success: Counter<u64>,
    /// How many bytes of protocol line were parsed and successfully loaded
    pub lp_bytes_success: Counter<u64>,
}

/// crate wide metrics
///
/// To use:
///
/// ```
/// use metrics::IOXD_METRICS;
///
/// // Record that 64 lines of line protocol were processed
/// let metric_kv = [
///   KeyValue::new("db_name", "my awesome db name"),
/// ];
///
/// IOXD_METRICS.lp_lines_success.add(lines.len() as u64, &metric_kv);
/// ```
pub static IOXD_METRICS: Lazy<SystemMetrics> = Lazy::new(SystemMetrics::new);

impl SystemMetrics {
    fn new() -> Self {
        let meter = opentelemetry::global::meter("iox");
        Self {
            lp_lines_errors: meter
                .u64_counter("ingest.lp.lines.errors")
                .with_description("line protocol formatted lines which were rejected")
                .init(),

            lp_lines_success: meter
                .u64_counter("ingest.lp.lines.success")
                .with_description("line protocol formatted lines which were successfully loaded")
                .init(),

            lp_bytes_success: meter
                .u64_counter("ingest.lp.bytes.success")
                .with_description("line protocol formatted bytes which were successfully loaded")
                .init(),
        }
    }
}

// TODO(jacobmarble): better way to write-once-read-many without a lock
// TODO(jacobmarble): generic OTel exporter, rather than just prometheus
static PROMETHEUS_EXPORTER: RwLock<Option<opentelemetry_prometheus::PrometheusExporter>> =
    const_rwlock(None);

/// Initializes metrics. See [`meter`] for example usage
pub fn init_metrics(_config: &Config) {
    // TODO add flags to config, to configure the OpenTelemetry exporter (OTLP)
    // This sets the global meter provider, for other code to use
    let exporter = opentelemetry_prometheus::exporter().init();
    init_metrics_internal(exporter)
}

#[cfg(test)]
pub fn init_metrics_for_test() {
    // Plan to skip all config flags
    let exporter = opentelemetry_prometheus::exporter().init();
    init_metrics_internal(exporter)
}

pub fn init_metrics_internal(exporter: opentelemetry_prometheus::PrometheusExporter) {
    let mut guard = PROMETHEUS_EXPORTER.write();
    if guard.is_some() {
        warn!("metrics were already initialized, overwriting configuration");
    }
    *guard = Some(exporter);
}

/// Gets current metrics state, in UTF-8 encoded Prometheus Exposition Format.
/// https://prometheus.io/docs/instrumenting/exposition_formats/
///
/// # Example
///
/// ```
/// # HELP a_counter Counts things
/// # TYPE a_counter counter
/// a_counter{key="value"} 100
/// # HELP a_value_recorder Records values
/// # TYPE a_value_recorder histogram
/// a_value_recorder_bucket{key="value",le="0.5"} 0
/// a_value_recorder_bucket{key="value",le="0.9"} 0
/// a_value_recorder_bucket{key="value",le="0.99"} 0
/// a_value_recorder_bucket{key="value",le="+Inf"} 1
/// a_value_recorder_sum{key="value"} 100
/// a_value_recorder_count{key="value"} 1
/// ```
pub fn metrics_as_text() -> Vec<u8> {
    let metric_families = PROMETHEUS_EXPORTER
        .read()
        .as_ref()
        .unwrap()
        .registry()
        .gather();
    let mut result = Vec::new();
    TextEncoder::new()
        .encode(&metric_families, &mut result)
        .unwrap();
    result
}
