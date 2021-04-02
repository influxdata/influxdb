use super::run::Config;
use parking_lot::{const_rwlock, RwLock};
use tracing_deps::{
    opentelemetry_prometheus,
    prometheus::{Encoder, TextEncoder},
};

// TODO(jacobmarble): better way to write-once-read-many without a lock
// TODO(jacobmarble): generic OTel exporter, rather than just prometheus
static PROMETHEUS_EXPORTER: RwLock<Option<opentelemetry_prometheus::PrometheusExporter>> =
    const_rwlock(None);

/// Initializes metrics.
///
/// # Example
///
/// ```
/// let meter = opentelemetry::global::meter("iox");
/// let counter = meter
///     .u64_counter("a.counter")
///     .with_description("Counts things")
///     .init();
/// let recorder = meter
///     .i64_value_recorder("a.value_recorder")
///     .with_description("Records values")
///     .init();
///
/// counter.add(100, &[KeyValue::new("key", "value")]);
/// recorder.record(100, &[KeyValue::new("key", "value")]);
/// ```
pub fn init_metrics(_config: &Config) {
    // TODO add flags to config, to configure the OpenTelemetry exporter (OTLP)
    // This sets the global meter provider, for other code to use
    let exporter = opentelemetry_prometheus::exporter().init();
    let mut guard = PROMETHEUS_EXPORTER.write();
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
