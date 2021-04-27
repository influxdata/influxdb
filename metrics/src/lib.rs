#![deny(broken_intra_doc_links, rust_2018_idioms)]
#![warn(
    missing_debug_implementations,
    clippy::explicit_iter_loop,
    clippy::use_self,
    clippy::clone_on_ref_ptr
)]
mod metrics;
mod tests;

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

pub use crate::metrics::{Counter, KeyValue, RedMetric};
pub use crate::tests::*;

/// A registry responsible for initialising IOx metrics and exposing their
/// current state in Prometheus exposition format.
///
/// Typically when IOx is running there should be a single `MetricRegistry`
/// initialised very close to program startup. This registry should then be
/// passed to every sub-system that needs to be instrumented.
///
/// Each sub-system should register a _domain_, which will result in all metrics
/// for that domain having a common name prefix. Domains allow callers to
/// register individual metrics.
///
/// To test your metrics in isolation simply provide a new `TestMetricRegistry`
/// during test setup. As well as ensuring isolation of your metrics it also
/// provides handy builder to easily assert conditions on your metrics.
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

    /// Returns the current metrics state, in UTF-8 encoded Prometheus
    /// Exposition Format.
    ///
    /// https://prometheus.io/docs/instrumenting/exposition_formats/
    ///
    /// For example:
    ///
    ///
    /// ```text
    /// # HELP a_counter Counts things
    /// # TYPE a_counter counter
    /// a_counter{key="value"} 100
    /// # HELP a_value_recorder Records values
    /// # TYPE a_value_recorder histogram
    /// a_value_recorder_bucket{key="value",le="0.01"} 99
    /// a_value_recorder_bucket{key="value",le="0.1"} 0
    /// a_value_recorder_bucket{key="value",le="1.0"} 1
    /// a_value_recorder_bucket{key="value",le="+Inf"} 1
    /// a_value_recorder_sum{key="value"} 1.99
    /// a_value_recorder_count{key="value"} 100
    /// ```
    ///
    pub fn metrics_as_text(&self) -> Vec<u8> {
        let metric_families = self.exporter.registry().gather();
        let mut result = Vec::new();
        TextEncoder::new()
            .encode(&metric_families, &mut result)
            .unwrap();
        result
    }

    pub(crate) fn metrics_as_str(&self) -> String {
        let metric_families = self.exporter.registry().gather();
        let mut result = Vec::new();
        TextEncoder::new()
            .encode(&metric_families, &mut result)
            .unwrap();
        String::from_utf8(result).unwrap()
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
        let default_histogram_boundaries = vec![
            0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
        ];
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
/// of the domain; for example "http", "read_buffer", "wb".
///
/// Domains should be registered on a `MetricRegistry`. The returned domain
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
    /// Two distinct metrics will be created. One will be a counter tracking
    /// the number of total requests and failed requests. The second will be a
    /// metric that tracks a latency distributions of all requests in seconds.
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
        unit: Option<&str>,
        description: impl Into<String>,
    ) -> metrics::Counter {
        self.register_counter_metric_with_labels(name, unit, description, vec![])
    }

    /// Registers a new counter metric with default labels.
    pub fn register_counter_metric_with_labels(
        &self,
        name: &str,
        unit: Option<&str>,
        description: impl Into<String>,
        default_labels: Vec<KeyValue>,
    ) -> metrics::Counter {
        let counter = self
            .meter
            .u64_counter(match unit {
                Some(unit) => format!("{}.{}.total", self.build_metric_prefix(name, None), unit),
                None => format!("{}.total", self.build_metric_prefix(name, None)),
            })
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
        let reg = TestMetricRegistry::default();
        let domain = reg.registry().register_domain("http");

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
        ob.observe(metrics::RedRequestStatus::Ok, duration, &[]);

        reg.has_metric_family("http_requests_total")
            .with_labels(&[("status", "ok")])
            .counter()
            .eq(1.0)
            .unwrap();

        reg.has_metric_family("http_request_duration_seconds")
            .with_labels(&[("status", "ok")])
            .histogram()
            .sample_count_eq(1)
            .unwrap();

        reg.has_metric_family("http_request_duration_seconds")
            .with_labels(&[("status", "ok")])
            .histogram()
            .sample_sum_eq(0.1)
            .unwrap();

        // report some other observations
        let ob = metric.observation();
        ob.observe(
            metrics::RedRequestStatus::ClientError,
            Duration::from_millis(2000),
            &[],
        );

        let ob = metric.observation();
        ob.observe(
            metrics::RedRequestStatus::Error,
            Duration::from_millis(350),
            &[],
        );

        // There are too many buckets (and we could change the defaults in the future) for a direct assertion
        // of the exposition format.
        let should_contain_lines = vec![
            "# HELP http_request_duration_seconds distribution of request latencies",
            "# TYPE http_request_duration_seconds histogram",
            r#"http_request_duration_seconds_bucket{status="client_error",le="0.005"} 0"#,
            r#"http_request_duration_seconds_bucket{status="client_error",le="0.01"} 0"#,
            r#"http_request_duration_seconds_bucket{status="client_error",le="10"} 1"#,
            r#"http_request_duration_seconds_bucket{status="client_error",le="+Inf"} 1"#,
            r#"http_request_duration_seconds_sum{status="client_error"} 2"#,
            r#"http_request_duration_seconds_count{status="client_error"} 1"#,
            r#"http_request_duration_seconds_bucket{status="error",le="0.5"} 1"#,
            r#"http_request_duration_seconds_bucket{status="error",le="1"} 1"#,
            r#"http_request_duration_seconds_bucket{status="error",le="10"} 1"#,
            r#"http_request_duration_seconds_bucket{status="error",le="+Inf"} 1"#,
            r#"http_request_duration_seconds_sum{status="error"} 0.35"#,
            r#"http_request_duration_seconds_count{status="error"} 1"#,
            r#"http_request_duration_seconds_bucket{status="ok",le="0.5"} 1"#,
            r#"http_request_duration_seconds_bucket{status="ok",le="1"} 1"#,
            r#"http_request_duration_seconds_bucket{status="ok",le="10"} 1"#,
            r#"http_request_duration_seconds_bucket{status="ok",le="+Inf"} 1"#,
            r#"http_request_duration_seconds_sum{status="ok"} 0.1"#,
            r#"http_request_duration_seconds_count{status="ok"} 1"#,
            "# HELP http_requests_total accumulated total requests",
            "# TYPE http_requests_total counter",
            r#"http_requests_total{status="client_error"} 1"#,
            r#"http_requests_total{status="error"} 1"#,
            r#"http_requests_total{status="ok"} 1"#,
            "",
        ];
        let metrics_response = String::from_utf8(reg.registry().metrics_as_text()).unwrap();
        for line in should_contain_lines {
            assert!(
                metrics_response.contains(line),
                "line: {}\nshould be contained in: {}",
                line,
                &metrics_response,
            );
        }
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
            metrics::RedRequestStatus::Ok,
            Duration::from_millis(100),
            &[KeyValue::new("account", "abc123")],
        );

        metric.observation().observe(
            metrics::RedRequestStatus::ClientError,
            Duration::from_millis(200),
            &[KeyValue::new("account", "other")],
        );

        metric.observation().observe(
            metrics::RedRequestStatus::Error,
            Duration::from_millis(203),
            &[KeyValue::new("account", "abc123")],
        );

        // There are too many buckets (and we could change the defaults in the future) for a direct assertion
        // of the exposition format.
        let should_contain_lines = vec![
            "# HELP ftp_request_duration_seconds distribution of request latencies",
            "# TYPE ftp_request_duration_seconds histogram",
            r#"ftp_request_duration_seconds_bucket{account="abc123",status="error",le="0.01"} 0"#,
            r#"ftp_request_duration_seconds_bucket{account="abc123",status="error",le="1"} 1"#,
            r#"ftp_request_duration_seconds_bucket{account="abc123",status="error",le="10"} 1"#,
            r#"ftp_request_duration_seconds_bucket{account="abc123",status="error",le="+Inf"} 1"#,
            r#"ftp_request_duration_seconds_sum{account="abc123",status="error"} 0.203"#,
            r#"ftp_request_duration_seconds_count{account="abc123",status="error"} 1"#,
            r#"ftp_request_duration_seconds_bucket{account="abc123",status="ok",le="0.01"} 0"#,
            r#"ftp_request_duration_seconds_bucket{account="abc123",status="ok",le="1"} 1"#,
            r#"ftp_request_duration_seconds_bucket{account="abc123",status="ok",le="10"} 1"#,
            r#"ftp_request_duration_seconds_bucket{account="abc123",status="ok",le="+Inf"} 1"#,
            r#"ftp_request_duration_seconds_sum{account="abc123",status="ok"} 0.1"#,
            r#"ftp_request_duration_seconds_count{account="abc123",status="ok"} 1"#,
            r#"ftp_request_duration_seconds_bucket{account="other",status="client_error",le="0.01"} 0"#,
            r#"ftp_request_duration_seconds_bucket{account="other",status="client_error",le="1"} 1"#,
            r#"ftp_request_duration_seconds_bucket{account="other",status="client_error",le="10"} 1"#,
            r#"ftp_request_duration_seconds_bucket{account="other",status="client_error",le="+Inf"} 1"#,
            r#"ftp_request_duration_seconds_sum{account="other",status="client_error"} 0.2"#,
            r#"ftp_request_duration_seconds_count{account="other",status="client_error"} 1"#,
            r#"# HELP ftp_requests_total accumulated total requests"#,
            r#"# TYPE ftp_requests_total counter"#,
            r#"ftp_requests_total{account="abc123",status="error"} 1"#,
            r#"ftp_requests_total{account="abc123",status="ok"} 1"#,
            r#"ftp_requests_total{account="other",status="client_error"} 1"#,
        ];
        let metrics_response = String::from_utf8(reg.metrics_as_text()).unwrap();
        for line in should_contain_lines {
            assert!(
                metrics_response.contains(line),
                "line: {}\nshould be contained in: {}",
                line,
                &metrics_response,
            );
        }
    }

    #[test]
    fn counter_metric() {
        let reg = TestMetricRegistry::default();
        let domain = reg.registry().register_domain("http");

        // create a counter metric
        let metric = domain.register_counter_metric_with_labels(
            "mem",
            Some("bytes"),
            "total bytes consumed",
            vec![KeyValue::new("tier", "a")],
        );

        metric.inc();
        metric.add(22);
        metric.inc_with_labels(&[KeyValue::new("tier", "b")]);

        reg.has_metric_family("http_mem_bytes_total")
            .with_labels(&[("tier", "a")])
            .counter()
            .eq(23.0)
            .unwrap();

        reg.has_metric_family("http_mem_bytes_total")
            .with_labels(&[("tier", "b")])
            .counter()
            .eq(1.0)
            .unwrap();
    }
}
