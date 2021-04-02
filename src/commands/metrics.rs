use parking_lot::{const_rwlock, RwLock};
use tracing_deps::{
    opentelemetry::{sdk::Resource, KeyValue},
    opentelemetry_prometheus,
    prometheus::{Encoder, TextEncoder},
};

static PROMETHEUS_EXPORTER: RwLock<Option<opentelemetry_prometheus::PrometheusExporter>> =
    const_rwlock(None);

pub fn init_metrics() {
    let exporter = opentelemetry_prometheus::exporter()
        .with_resource(Resource::new(vec![KeyValue::new(
            "service.name",
            "influxdb-iox",
        )]))
        .init();
    let mut guard = PROMETHEUS_EXPORTER.write();
    *guard = Some(exporter);
}

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
