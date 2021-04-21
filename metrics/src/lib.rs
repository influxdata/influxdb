#![deny(rust_2018_idioms)]
#![warn(
    missing_debug_implementations,
    clippy::explicit_iter_loop,
    clippy::use_self,
    clippy::clone_on_ref_ptr
)]
use std::sync::Arc;

use observability_deps::{
    opentelemetry::metrics::Meter as OTMeter,
    opentelemetry::{self},
    opentelemetry_prometheus::{self, ExporterBuilder},
    prometheus::{Encoder, Registry, TextEncoder},
    tracing::log::warn,
};
use once_cell::sync::Lazy;
use parking_lot::{const_rwlock, RwLock};

pub mod metrics;
pub use crate::metrics::*;

/// global metrics
pub static IOXD_METRICS_REGISTRY: Lazy<GlobalRegistry> = Lazy::new(GlobalRegistry::new);

// // TODO(jacobmarble): better way to write-once-read-many without a lock
// // TODO(jacobmarble): generic OTel exporter, rather than just prometheus
// static PROMETHEUS_EXPORTER: RwLock<Option<opentelemetry_prometheus::PrometheusExporter>> =
//     const_rwlock(None);

// /// Returns the data in the Prom exposition format.
// pub fn metrics_as_text() -> Vec<u8> {
//     let metric_families = PROMETHEUS_EXPORTER
//         .read()
//         .as_ref()
//         .unwrap()
//         .registry()
//         .gather();
//     let mut result = Vec::new();
//     TextEncoder::new()
//         .encode(&metric_families, &mut result)
//         .unwrap();
//     result
// }

/// Configuration options for the global registry.
///
///
/// TODO add flags to config, to configure the OpenTelemetry exporter (OTLP)
/// This sets the global meter provider, for other code to use
#[derive(Debug)]
pub struct Config {}

#[derive(Debug)]
pub struct GlobalRegistry {
    meter: Arc<OTMeter>,
    exporter: RwLock<opentelemetry_prometheus::PrometheusExporter>,
}

// TODO(jacobmarble): better way to write-once-read-many without a lock
// // TODO(jacobmarble): generic OTel exporter, rather than just prometheus
// static PROMETHEUS_EXPORTER: RwLock<Option<opentelemetry_prometheus::PrometheusExporter>> =
//     const_rwlock(None);

impl GlobalRegistry {
    // `new` is private because this is only initialised via
    // `IOXD_METRICS_REGISTRY`.
    fn new() -> Self {
        // Self::init();
        let prom_reg = Registry::new();
        let exporter = RwLock::new(ExporterBuilder::default().with_registry(prom_reg).init());

        // let exporter = RwLock::new(opentelemetry_prometheus::exporter().init());
        Self {
            meter: Arc::new(opentelemetry::global::meter("iox")),
            exporter,
        }
    }

    /// Returns the data in the Prom exposition format.
    pub fn metrics_as_text(&self) -> Vec<u8> {
        let metric_families = self.exporter.read().registry().gather();
        let mut result = Vec::new();
        TextEncoder::new()
            .encode(&metric_families, &mut result)
            .unwrap();
        result
    }

    // /// Initializes global registry
    // pub fn init() {
    //     let exporter = opentelemetry_prometheus::exporter().init();
    //     let mut guard = PROMETHEUS_EXPORTER.write();
    //     if guard.is_some() {
    //         warn!("metrics were already initialized, overwriting configuration");
    //     }
    //     *guard = Some(exporter);
    // }

    /// Initializes global registry with configuration options
    // pub fn init_with_config(_config: &Config) {
    //     let exporter = opentelemetry_prometheus::exporter().init();
    //     let mut guard = PROMETHEUS_EXPORTER.write();
    //     if guard.is_some() {
    //         warn!("metrics were already initialized, overwriting configuration");
    //     }
    //     *guard = Some(exporter);
    // }

    /// This method should be used to register a new domain such as a
    /// sub-system, crate or similar.
    pub fn register_domain(&self, name: &'static str) -> Domain {
        Domain::new(name, Arc::clone(&self.meter))
    }
}

/// A `Domain` provides a namespace that describes a collection of related
/// metrics. Within the domain all metrics' names will be prefixed by the name
/// of the domain; for example "http", "read_buffer", "wal".
///
/// Domains should be registered on the global registry. The returned domain
/// allows individual metrics to then be registered and used.
#[derive(Debug)]
pub struct Domain {
    name: &'static str,
    meter: Arc<OTMeter>,
}

impl Domain {
    pub(crate) fn new(name: &'static str, meter: Arc<OTMeter>) -> Self {
        Self { name, meter }
    }

    // Creates an appropriate metric name prefix based on the Domain's name and
    // an optional suffix.
    fn build_metric_prefix(&self, suffix: Option<String>) -> String {
        let suffix = match suffix {
            Some(name) => format!(".{}", name),
            None => "".to_string(),
        };

        format!("{}{}", self.name, suffix)
    }

    /// Registers a new metric following the RED methodology.
    ///
    /// By default, two distinct metrics will be created. One will be a counter
    /// that will track the number of total requests and failed requests. The
    /// second will be a metric that tracks a latency distributions of all
    /// requests in seconds.
    ///
    /// If `name` is not provided then the metrics registered on the `mydomain`
    /// domain will be named:
    ///
    /// `mydomain.requests.total` (tracks total and failed requests)
    /// `mydomain.requests.duration.seconds` (tracks a distribution of
    /// latencies)
    ///
    /// If `name` is provided then the metric names become:
    //
    /// `mydomain.somename.requests.total`
    /// `mydomain.somename.requests.duration.seconds`
    pub fn register_red_metric(&self, name: Option<String>) -> metrics::RedMetric {
        self.register_red_metric_with_labels(name, &[])
    }

    /// As `register_red_metric` but with a set of default labels. These labels
    /// will be associated with each observation given to the metric.
    pub fn register_red_metric_with_labels(
        &self,
        name: Option<String>,
        default_labels: &[KeyValue],
    ) -> metrics::RedMetric {
        let requests = self
            .meter
            .u64_counter(format!(
                "{}.requests.total",
                self.build_metric_prefix(name.clone())
            ))
            .with_description("accumulated total requests")
            .init();

        let duration = self
            .meter
            .f64_value_recorder(format!(
                "{}.request.duration.seconds",
                self.build_metric_prefix(name)
            ))
            .with_description("distribution of request latencies")
            .init();

        metrics::RedMetric::new(requests, duration, &default_labels)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::time::Duration;

    #[test]
    fn red_metric() {
        let reg = GlobalRegistry::new();
        let domain = reg.register_domain("http");

        // create a RED metrics
        let metric = domain.register_red_metric(None);

        // Get an observation to start measuring something.
        let ob = metric.observation();

        // do some "work"
        let duration = Duration::from_millis(100);

        // finish observation with success using explicit duration for easier
        // testing.
        //
        // Usually caller would call `ob.ok()`.
        ob.observe(RedRequestStatus::Ok, duration, vec![]);

        assert_eq!(
            String::from_utf8(reg.metrics_as_text()).unwrap(),
            vec![
                "# HELP http_request_duration_seconds distribution of request latencies",
                "# TYPE http_request_duration_seconds histogram",
                r#"http_request_duration_seconds_bucket{status="ok",le="0.5"} 1"#,
                r#"http_request_duration_seconds_bucket{status="ok",le="0.9"} 1"#,
                r#"http_request_duration_seconds_bucket{status="ok",le="0.99"} 1"#,
                r#"http_request_duration_seconds_bucket{status="ok",le="+Inf"} 1"#,
                r#"http_request_duration_seconds_sum{status="ok"} 0.1"#,
                r#"http_request_duration_seconds_count{status="ok"} 1"#,
                "# HELP http_requests_total accumulated total requests",
                "# TYPE http_requests_total counter",
                r#"http_requests_total{status="ok"} 1"#,
                ""
            ]
            .join("\n")
        );

        // report some other observations
        let ob = metric.observation();
        ob.observe(
            RedRequestStatus::OkError,
            Duration::from_millis(2000),
            vec![],
        );

        let ob = metric.observation();
        ob.observe(RedRequestStatus::Error, Duration::from_millis(350), vec![]);

        assert_eq!(
            String::from_utf8(reg.metrics_as_text()).unwrap(),
            vec![
                "# HELP http_request_duration_seconds distribution of request latencies",
                "# TYPE http_request_duration_seconds histogram",
                r#"http_request_duration_seconds_bucket{status="error",le="0.5"} 1"#,
                r#"http_request_duration_seconds_bucket{status="error",le="0.9"} 1"#,
                r#"http_request_duration_seconds_bucket{status="error",le="0.99"} 1"#,
                r#"http_request_duration_seconds_bucket{status="error",le="+Inf"} 1"#,
                r#"http_request_duration_seconds_sum{status="error"} 0.35"#,
                r#"http_request_duration_seconds_count{status="error"} 1"#,
                r#"http_request_duration_seconds_bucket{status="ok",le="0.5"} 1"#,
                r#"http_request_duration_seconds_bucket{status="ok",le="0.9"} 1"#,
                r#"http_request_duration_seconds_bucket{status="ok",le="0.99"} 1"#,
                r#"http_request_duration_seconds_bucket{status="ok",le="+Inf"} 1"#,
                r#"http_request_duration_seconds_sum{status="ok"} 0.1"#,
                r#"http_request_duration_seconds_count{status="ok"} 1"#,
                r#"http_request_duration_seconds_bucket{status="ok_error",le="0.5"} 0"#,
                r#"http_request_duration_seconds_bucket{status="ok_error",le="0.9"} 0"#,
                r#"http_request_duration_seconds_bucket{status="ok_error",le="0.99"} 0"#,
                r#"http_request_duration_seconds_bucket{status="ok_error",le="+Inf"} 1"#,
                r#"http_request_duration_seconds_sum{status="ok_error"} 2"#,
                r#"http_request_duration_seconds_count{status="ok_error"} 1"#,
                "# HELP http_requests_total accumulated total requests",
                "# TYPE http_requests_total counter",
                r#"http_requests_total{status="error"} 1"#,
                r#"http_requests_total{status="ok"} 1"#,
                r#"http_requests_total{status="ok_error"} 1"#,
                "",
            ]
            .join("\n")
        );
    }

    #[test]
    fn red_metric_labels() {
        let reg = GlobalRegistry::new();
        let domain = reg.register_domain("ftp");

        // create a RED metrics
        let metric = domain.register_red_metric(None);

        // Get an observation to start measuring something.
        let ob = metric.observation();

        // Usually a caller would use `ob.ok_with_labels(labels)`;
        ob.observe(
            RedRequestStatus::Ok,
            Duration::from_millis(100),
            vec![KeyValue::new("account", "abc123")],
        );

        metric.observation().observe(
            RedRequestStatus::OkError,
            Duration::from_millis(200),
            vec![KeyValue::new("account", "other")],
        );

        metric.observation().observe(
            RedRequestStatus::Error,
            Duration::from_millis(203),
            vec![KeyValue::new("account", "abc123")],
        );

        assert_eq!(
            String::from_utf8(reg.metrics_as_text()).unwrap(),
            vec![
                "# HELP ftp_request_duration_seconds distribution of request latencies",
"# TYPE ftp_request_duration_seconds histogram",
r#"ftp_request_duration_seconds_bucket{account="abc123",status="error",le="0.5"} 1"#,
r#"ftp_request_duration_seconds_bucket{account="abc123",status="error",le="0.9"} 1"#,
r#"ftp_request_duration_seconds_bucket{account="abc123",status="error",le="0.99"} 1"#,
r#"ftp_request_duration_seconds_bucket{account="abc123",status="error",le="+Inf"} 1"#,
r#"ftp_request_duration_seconds_sum{account="abc123",status="error"} 0.203"#,
r#"ftp_request_duration_seconds_count{account="abc123",status="error"} 1"#,
r#"ftp_request_duration_seconds_bucket{account="abc123",status="ok",le="0.5"} 1"#,
r#"ftp_request_duration_seconds_bucket{account="abc123",status="ok",le="0.9"} 1"#,
r#"ftp_request_duration_seconds_bucket{account="abc123",status="ok",le="0.99"} 1"#,
r#"ftp_request_duration_seconds_bucket{account="abc123",status="ok",le="+Inf"} 1"#,
r#"ftp_request_duration_seconds_sum{account="abc123",status="ok"} 0.1"#,
r#"ftp_request_duration_seconds_count{account="abc123",status="ok"} 1"#,
r#"ftp_request_duration_seconds_bucket{account="other",status="ok_error",le="0.5"} 1"#,
r#"ftp_request_duration_seconds_bucket{account="other",status="ok_error",le="0.9"} 1"#,
r#"ftp_request_duration_seconds_bucket{account="other",status="ok_error",le="0.99"} 1"#,
r#"ftp_request_duration_seconds_bucket{account="other",status="ok_error",le="+Inf"} 1"#,
r#"ftp_request_duration_seconds_sum{account="other",status="ok_error"} 0.2"#,
r#"ftp_request_duration_seconds_count{account="other",status="ok_error"} 1"#,
r#"# HELP ftp_requests_total accumulated total requests"#,
r#"# TYPE ftp_requests_total counter"#,
r#"ftp_requests_total{account="abc123",status="error"} 1"#,
r#"ftp_requests_total{account="abc123",status="ok"} 1"#,
r#"ftp_requests_total{account="other",status="ok_error"} 1"#,
""
            ]
            .join("\n")
        );
    }
}
