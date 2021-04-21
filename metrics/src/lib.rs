#![deny(rust_2018_idioms)]
#![warn(
    missing_debug_implementations,
    clippy::explicit_iter_loop,
    clippy::use_self,
    clippy::clone_on_ref_ptr
)]

use observability_deps::{
    opentelemetry::metrics::Meter as OTMeter,
    opentelemetry::{
        metrics::{registry::RegistryMeterProvider, MeterProvider},
        sdk::{
            export::metrics::ExportKindSelector,
            metrics::{controllers, selectors::simple::Selector},
        },
    },
    opentelemetry_prometheus::PrometheusExporter,
    prometheus::{Encoder, Registry, TextEncoder},
    tracing::*,
};
use once_cell::sync::Lazy;

pub mod metrics;
pub use crate::metrics::*;

/// global metrics
pub static IOXD_METRICS_REGISTRY: Lazy<MetricRegistry> = Lazy::new(MetricRegistry::new);

#[derive(Debug)]
pub struct MetricRegistry {
    provider: RegistryMeterProvider,
    exporter: PrometheusExporter,
}

impl MetricRegistry {
    /// initialise a new metrics registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns the data in the Prom exposition format.
    pub fn metrics_as_text(&self) -> Vec<u8> {
        let metric_families = self.exporter.registry().gather();
        let mut result = Vec::new();
        TextEncoder::new()
            .encode(&metric_families, &mut result)
            .unwrap();
        result
    }

    /// This method should be used to register a new domain such as a
    /// sub-system, crate or similar.
    pub fn register_domain(&self, name: &'static str) -> Domain {
        let meter = self.provider.meter(name, None);
        Domain::new(name, meter)
    }
}

impl Default for MetricRegistry {
    fn default() -> Self {
        let registry = Registry::new();
        let default_histogram_boundaries = vec![0.5, 0.9, 0.99];
        let selector = Box::new(Selector::Histogram(default_histogram_boundaries.clone()));
        let controller = controllers::pull(selector, Box::new(ExportKindSelector::Cumulative))
            // Disables caching
            .with_cache_period(std::time::Duration::from_secs(0))
            // Remember all metrics observed, not just recently updated
            .with_memory(true)
            .build();

        // Initialise the prometheus exporter
        let default_summary_quantiles = vec![0.5, 0.9, 0.99];

        let exporter = PrometheusExporter::new(
            registry,
            controller,
            default_summary_quantiles,
            default_histogram_boundaries,
        )
        .unwrap();

        let provider = exporter.provider().unwrap();

        Self { provider, exporter }
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
    meter: OTMeter,
}

impl Domain {
    pub(crate) fn new(name: &'static str, meter: OTMeter) -> Self {
        Self { name, meter }
    }

    // Creates an appropriate metric name based on the Domain's name and an
    // optional subsystem name.
    fn build_metric_prefix(&self, metric_name: &str, subname: Option<&str>) -> String {
        match subname {
            Some(subname) => format!("{}.{}.{}", self.name, subname, metric_name),
            None => format!("{}.{}", self.name, metric_name),
        }
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
    pub fn register_red_metric(&self, subname: Option<&str>) -> metrics::RedMetric {
        self.register_red_metric_with_labels(subname, vec![])
    }

    /// As `register_red_metric` but with a set of default labels. These labels
    /// will be associated with each observation given to the metric.
    pub fn register_red_metric_with_labels(
        &self,
        name: Option<&str>,
        default_labels: Vec<KeyValue>,
    ) -> metrics::RedMetric {
        let requests = self
            .meter
            .u64_counter(self.build_metric_prefix("requests.total", name))
            .with_description("accumulated total requests")
            .init();

        let duration = self
            .meter
            .f64_value_recorder(self.build_metric_prefix("request.duration.seconds", name))
            .with_description("distribution of request latencies")
            .init();

        metrics::RedMetric::new(requests, duration, default_labels)
    }

    /// Registers a new counter metric.
    ///
    /// `name` should be a noun that describes the thing being counted, e.g.,
    /// lines, payloads, accesses, compactions etc.
    ///
    /// `unit` is optional and will appear in the metric name before a final
    /// `total` suffix. Consider reviewing
    /// https://prometheus.io/docs/practices/naming/#base-units for appropriate
    /// units.
    pub fn register_counter_metric(
        &self,
        name: &str,
        unit: Option<String>,
        description: impl Into<String>,
    ) -> metrics::Counter {
        self.register_counter_metric_with_labels(name, unit, description, vec![])
    }

    /// Registers a new counter metric with default labels.
    pub fn register_counter_metric_with_labels(
        &self,
        name: &str,
        unit: Option<String>,
        description: impl Into<String>,
        default_labels: Vec<KeyValue>,
    ) -> metrics::Counter {
        let counter = self
            .meter
            .u64_counter(format!(
                "{}{}.total",
                self.build_metric_prefix(name, None),
                match unit {
                    Some(unit) => format!(".{}", unit),
                    None => "".to_string(),
                }
            ))
            .with_description(description)
            .init();

        metrics::Counter::new(counter, default_labels)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::time::Duration;

    #[test]
    fn red_metric() {
        let reg = MetricRegistry::new();
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
        ob.observe(RedRequestStatus::Ok, duration, &[]);

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
        ob.observe(RedRequestStatus::OkError, Duration::from_millis(2000), &[]);

        let ob = metric.observation();
        ob.observe(RedRequestStatus::Error, Duration::from_millis(350), &[]);

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
        let reg = MetricRegistry::new();
        let domain = reg.register_domain("ftp");

        // create a RED metrics
        let metric = domain.register_red_metric(None);

        // Get an observation to start measuring something.
        let ob = metric.observation();

        // Usually a caller would use `ob.ok_with_labels(labels)`;
        ob.observe(
            RedRequestStatus::Ok,
            Duration::from_millis(100),
            &[KeyValue::new("account", "abc123")],
        );

        metric.observation().observe(
            RedRequestStatus::OkError,
            Duration::from_millis(200),
            &[KeyValue::new("account", "other")],
        );

        metric.observation().observe(
            RedRequestStatus::Error,
            Duration::from_millis(203),
            &[KeyValue::new("account", "abc123")],
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

    #[test]
    fn counter_metric() {
        let reg = MetricRegistry::new();
        let domain = reg.register_domain("http");

        // create a counter metric
        let metric = domain.register_counter_metric_with_labels(
            "mem",
            Some("bytes".to_string()),
            "total bytes consumed",
            vec![KeyValue::new("tier", "a")],
        );

        metric.inc();
        metric.add(22);
        metric.inc_with_labels(&[KeyValue::new("tier", "b")]);

        assert_eq!(
            String::from_utf8(reg.metrics_as_text()).unwrap(),
            vec![
                "# HELP http_mem_bytes_total total bytes consumed",
                "# TYPE http_mem_bytes_total counter",
                r#"http_mem_bytes_total{tier="a"} 23"#,
                r#"http_mem_bytes_total{tier="b"} 1"#, // tier is overridden
                ""
            ]
            .join("\n")
        );
    }
}
