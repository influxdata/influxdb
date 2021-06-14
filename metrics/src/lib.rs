#![deny(broken_intra_doc_links, rust_2018_idioms)]
#![warn(
    missing_debug_implementations,
    clippy::explicit_iter_loop,
    clippy::use_self,
    clippy::clone_on_ref_ptr
)]

use hashbrown::hash_map::Entry;
use hashbrown::HashMap;
use std::sync::Arc;

use opentelemetry_prometheus::PrometheusExporter;
use parking_lot::Mutex;
use prometheus::{Encoder, Registry, TextEncoder};

use observability_deps::{
    opentelemetry::metrics::Meter as OTMeter,
    opentelemetry::{
        metrics::{
            registry::RegistryMeterProvider, MeterProvider, ObserverResult, ValueRecorderBuilder,
        },
        sdk::{
            export::metrics::ExportKindSelector,
            metrics::{controllers, selectors::simple::Selector},
        },
    },
    tracing::*,
};

pub use crate::gauge::*;
pub use crate::metrics::{Counter, Histogram, KeyValue, RedMetric};
use crate::observer::ObserverCollection;
pub use crate::tests::*;

mod gauge;
mod metrics;
mod observer;
mod tests;

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
    observers: Arc<ObserverCollection>,

    /// Provides a mapping between metric name and Gauge
    ///
    /// These are registered on a single shared map to ensure if gauges with the
    /// the same metric name are registered on different metrics domains they
    /// both get the same underlying Gauge. If this didn't occur and both gauges
    /// were used to publish to the same label set, they wouldn't be able to see
    /// each other contributions, and only one would be reported to OT
    gauges: Arc<Mutex<HashMap<String, Arc<GaugeState>>>>,
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
        Domain::new(
            name,
            meter,
            Arc::clone(&self.observers),
            vec![],
            Arc::clone(&self.gauges),
        )
    }

    /// This method should be used to register a new domain such as a
    /// sub-system, crate or similar.
    pub fn register_domain_with_labels(&self, name: &'static str, labels: Vec<KeyValue>) -> Domain {
        let meter = self.provider.meter(name, None);
        Domain::new(
            name,
            meter,
            Arc::clone(&self.observers),
            labels,
            Arc::clone(&self.gauges),
        )
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
        let observers = Arc::new(ObserverCollection::new(provider.meter("observers", None)));

        Self {
            provider,
            exporter,
            observers,
            gauges: Default::default(),
        }
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
    default_labels: Vec<KeyValue>,

    /// A copy of the observer collection from the owning `MetricRegistry`
    observers: Arc<ObserverCollection>,

    /// A copy of the gauge map from the owning `MetricRegistry`
    gauges: Arc<Mutex<HashMap<String, Arc<GaugeState>>>>,
}

impl Domain {
    pub(crate) fn new(
        name: &'static str,
        meter: OTMeter,
        observers: Arc<ObserverCollection>,
        default_labels: Vec<KeyValue>,
        gauges: Arc<Mutex<HashMap<String, Arc<GaugeState>>>>,
    ) -> Self {
        Self {
            name,
            meter,
            default_labels,
            observers,
            gauges,
        }
    }

    // Creates an appropriate metric name based on the Domain's name, and
    // an optional subsystem name, unit and suffix
    fn build_metric_name(
        &self,
        metric_name: &str,
        subname: Option<&str>,
        unit: Option<&str>,
        suffix: Option<&str>,
    ) -> String {
        let mut ret = self.name.to_string();
        for s in [subname, Some(metric_name), unit, suffix].iter().flatten() {
            ret.push('.');
            ret.push_str(s);
        }
        ret
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
            .u64_counter(self.build_metric_name("requests", name, None, Some("total")))
            .with_description("accumulated total requests")
            .init();

        let duration = self
            .meter
            .f64_value_recorder(self.build_metric_name(
                "request.duration",
                name,
                Some("seconds"),
                None,
            ))
            .with_description("distribution of request latencies")
            .init();

        metrics::RedMetric::new(
            requests,
            duration,
            [&self.default_labels[..], &default_labels[..]].concat(),
        )
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
            .u64_counter(self.build_metric_name(name, None, unit, Some("total")))
            .with_description(description)
            .init();

        metrics::Counter::new(
            counter,
            [&self.default_labels[..], &default_labels[..]].concat(),
        )
    }

    /// Registers a new histogram metric.
    ///
    /// `name` in the common case should be a noun describing the thing being
    /// observed. For example: "request", "migration", "conversion".
    ///
    /// `attribute` should be the attribute that is being observed. Good examples
    /// of attributes are: "duration" or "usage".
    ///
    /// `unit` is required and will appear at the end of the metric name.
    /// Consider reviewing
    /// https://prometheus.io/docs/practices/naming/#base-units for appropriate
    /// units. Examples include "bytes", "seconds", "celsius"
    ///
    pub fn register_histogram_metric(
        &self,
        name: &str,
        attribute: &str,
        unit: &str,
        description: impl Into<String>,
    ) -> HistogramBuilder<'_> {
        let histogram = self
            .meter
            .f64_value_recorder(self.build_metric_name(attribute, Some(name), Some(unit), None))
            .with_description(description);

        HistogramBuilder::new(histogram).with_labels(self.default_labels.clone())
    }

    /// Registers a new gauge metric.
    ///
    /// A gauge is a metric that represents a single numerical value that can arbitrarily go up and down.
    /// Gauges are typically used for measured values like temperatures or current memory usage, but also
    /// "counts" that can go up and down, like the number of concurrent requests.
    ///
    /// `name` should be a noun that describes the thing being observed, e.g.,
    /// threads, buffers, ...
    ///
    /// `unit` is optional and will appear in the metric name before a final
    /// `total` suffix. Consider reviewing
    /// https://prometheus.io/docs/practices/naming/#base-units for appropriate
    pub fn register_gauge_metric(
        &self,
        name: &str,
        unit: Option<&str>,
        description: impl Into<String>,
    ) -> Gauge {
        self.register_gauge_metric_with_labels(name, unit, description, &[])
    }

    /// Registers a new gauge metric with default labels.
    pub fn register_gauge_metric_with_labels(
        &self,
        name: &str,
        unit: Option<&str>,
        description: impl Into<String>,
        default_labels: &[KeyValue],
    ) -> Gauge {
        let name = self.build_metric_name(name, None, unit, None);

        let gauge = match self.gauges.lock().entry(name) {
            Entry::Occupied(occupied) => Arc::clone(occupied.get()),
            Entry::Vacant(vacant) => {
                let key = vacant.key().clone();
                let gauge = Arc::new(GaugeState::default());
                let captured = Arc::clone(&gauge);
                self.observers
                    .u64_value_observer(key, description, move |observer| {
                        captured.visit_values(|data, labels| observer.observe(data as u64, labels));
                    });

                Arc::clone(vacant.insert(gauge))
            }
        };
        Gauge::new(gauge, [&self.default_labels[..], &default_labels].concat())
    }

    /// An observer can be used to provide asynchronous fetching of values from an object
    ///
    /// NB: If you register multiple observers with the same measurement name, they MUST
    /// publish to disjoint label sets. If two or more observers publish to the same
    /// measurement name and label set only one will be reported
    pub fn register_observer(
        &self,
        subname: Option<&str>,
        default_labels: &[KeyValue],
        observer: impl MetricObserver,
    ) {
        observer.register(MetricObserverBuilder {
            domain: self,
            subname,
            labels: [&self.default_labels[..], default_labels].concat(),
        });
    }
}

/// A trait that can be implemented by an object that wishes to expose
/// its internal counters as OpenTelemetry metrics
///
/// A default implementation is provided for a closure
pub trait MetricObserver {
    fn register(self, builder: MetricObserverBuilder<'_>);
}

impl<T: FnOnce(MetricObserverBuilder<'_>)> MetricObserver for T {
    fn register(self, builder: MetricObserverBuilder<'_>) {
        self(builder)
    }
}

#[derive(Debug)]
pub struct MetricObserverBuilder<'a> {
    domain: &'a Domain,
    subname: Option<&'a str>,
    labels: Vec<KeyValue>,
}

impl<'a> MetricObserverBuilder<'a> {
    /// Register a u64 gauge
    pub fn register_gauge_u64<F>(
        &self,
        name: &str,
        unit: Option<&str>,
        description: impl Into<String>,
        callback: F,
    ) where
        F: Fn(TaggedObserverResult<'_, u64>) + Send + Sync + 'static,
    {
        self.domain.observers.u64_value_observer(
            self.domain
                .build_metric_name(name, self.subname, unit, None),
            description,
            TaggedObserverResult::with_callback(self.labels.clone(), callback),
        )
    }

    /// Register a f64 gauge
    pub fn register_gauge_f64<F>(
        &self,
        name: &str,
        unit: Option<&str>,
        description: impl Into<String>,
        callback: F,
    ) where
        F: Fn(TaggedObserverResult<'_, f64>) + Send + Sync + 'static,
    {
        self.domain.observers.f64_value_observer(
            self.domain
                .build_metric_name(name, self.subname, unit, None),
            description,
            TaggedObserverResult::with_callback(self.labels.to_owned(), callback),
        )
    }

    /// Register a u64 counter
    pub fn register_counter_u64<F>(
        &self,
        name: &str,
        unit: Option<&str>,
        description: impl Into<String>,
        callback: F,
    ) where
        F: Fn(TaggedObserverResult<'_, u64>) + Send + Sync + 'static,
    {
        self.domain.observers.u64_sum_observer(
            self.domain
                .build_metric_name(name, self.subname, unit, Some("total")),
            description,
            TaggedObserverResult::with_callback(self.labels.to_owned(), callback),
        )
    }

    /// Register a f64 counter
    pub fn register_counter_f64<F>(
        &self,
        name: &str,
        unit: Option<&str>,
        description: impl Into<String>,
        callback: F,
    ) where
        F: Fn(TaggedObserverResult<'_, f64>) + Send + Sync + 'static,
    {
        self.domain.observers.f64_sum_observer(
            self.domain
                .build_metric_name(name, self.subname, unit, Some("total")),
            description,
            TaggedObserverResult::with_callback(self.labels.to_owned(), callback),
        )
    }
}

#[derive(Debug)]
pub struct TaggedObserverResult<'a, T> {
    observer: &'a ObserverResult<T>,
    labels: &'a [KeyValue],
}

impl<'a, T> TaggedObserverResult<'a, T>
where
    T: Into<observability_deps::opentelemetry::metrics::Number>,
{
    fn with_callback<F>(
        labels: Vec<KeyValue>,
        callback: F,
    ) -> impl Fn(&ObserverResult<T>) + Send + Sync + 'static
    where
        F: Fn(TaggedObserverResult<'_, T>) + Send + Sync + 'static,
    {
        move |observer| {
            callback(TaggedObserverResult {
                observer,
                labels: labels.as_slice(),
            })
        }
    }

    pub fn observe(&self, value: T, labels: &[KeyValue]) {
        // This does not handle duplicates, it is unclear that it should
        let labels: Vec<_> = self.labels.iter().chain(labels).cloned().collect();
        self.observer.observe(value, &labels)
    }
}

#[derive(Debug)]
pub struct HistogramBuilder<'a> {
    histogram: ValueRecorderBuilder<'a, f64>,
    labels: Option<Vec<KeyValue>>,
    boundaries: Option<Vec<f64>>,
}

impl<'a> HistogramBuilder<'a> {
    fn new(histogram: ValueRecorderBuilder<'a, f64>) -> Self {
        Self {
            histogram,
            labels: None,
            boundaries: None,
        }
    }

    /// Set some default labels on the Histogram metric.
    pub fn with_labels(self, labels: Vec<KeyValue>) -> Self {
        let labels = match self.labels {
            Some(existing_labels) => [&existing_labels[..], &labels[..]].concat(),
            None => labels,
        };

        Self {
            histogram: self.histogram,
            labels: Some(labels),
            boundaries: None,
        }
    }

    /// Set some bucket boundaries on the Histogram metric.
    pub fn with_bucket_boundaries(self, boundaries: Vec<f64>) -> Self {
        Self {
            histogram: self.histogram,
            labels: self.labels,
            boundaries: Some(boundaries),
        }
    }

    /// Initialise the Histogram metric ready for observations.
    pub fn init(self) -> metrics::Histogram {
        let labels = self.labels.unwrap_or_default();
        let boundaries = self.boundaries.unwrap_or_default();

        if !boundaries.is_empty() {
            todo!("histogram boundaries not yet configurable.");
        }

        metrics::Histogram::new(self.histogram.init(), labels)
    }
}

#[cfg(test)]
mod test {
    use std::sync::Mutex;
    use std::time::Duration;

    use super::*;

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
        let reg = TestMetricRegistry::default();
        let domain = reg.registry().register_domain("ftp");

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

    #[test]
    fn histogram_metric() {
        let reg = TestMetricRegistry::default();
        let domain = reg.registry().register_domain("chunker");

        // create a histogram metric using the builder
        let metric = domain
            .register_histogram_metric(
                "conversion",
                "duration",
                "seconds",
                "The distribution of chunk conversion latencies",
            )
            .with_labels(vec![KeyValue::new("db", "mydb")]) // Optional
            // .with_bucket_boundaries(vec![1,2,3]) <- this is a future TODO
            .init();

        // Get an observation to start measuring something.
        metric.observe(22.32); // manual observation

        // There is also a timer that will handle durations for you.
        let ob = metric.timer();
        std::thread::sleep(Duration::from_millis(20));

        // will record a duration >= 20ms
        ob.record_with_labels(&[KeyValue::new("stage", "beta")]);

        reg.has_metric_family("chunker_conversion_duration_seconds")
            .with_labels(&[("db", "mydb")])
            .histogram()
            .sample_count_eq(1)
            .unwrap();

        reg.has_metric_family("chunker_conversion_duration_seconds")
            .with_labels(&[("db", "mydb")])
            .histogram()
            .sample_sum_eq(22.32)
            .unwrap();

        reg.has_metric_family("chunker_conversion_duration_seconds")
            .with_labels(&[("db", "mydb"), ("stage", "beta")])
            .histogram()
            .sample_count_eq(1)
            .unwrap();

        reg.has_metric_family("chunker_conversion_duration_seconds")
            .with_labels(&[("db", "mydb"), ("stage", "beta")])
            .histogram()
            .sample_sum_gte(0.02) // 20ms
            .unwrap();

        // TODO(edd): need to figure out how to set custom buckets then
        // these assertions can be tested.
        // reg.has_metric_family("chunker_conversion_duration_seconds")
        //     .with_labels(&[("db", "mydb"), ("stage", "beta")])
        //     .histogram()
        //     .bucket_cumulative_count_eq(11.2, 1)
        //     .unwrap();

        // reg.has_metric_family("chunker_conversion_duration_seconds")
        //     .with_labels(&[("db", "mydb"), ("stage", "beta")])
        //     .histogram()
        //     .bucket_cumulative_count_eq(0.12, 0)
        //     .unwrap();
    }

    #[test]
    fn gauge_metric() {
        let reg = TestMetricRegistry::default();
        let domain = reg.registry().register_domain("http");

        // create a gauge metric
        let mut metric = domain.register_gauge_metric_with_labels(
            "mem",
            Some("bytes"),
            "currently used bytes",
            &[KeyValue::new("tier", "a")],
        );

        metric.set(40, &[]);
        metric.set(41, &[KeyValue::new("tier", "b")]);
        metric.set(
            42,
            &[KeyValue::new("tier", "c"), KeyValue::new("tier", "b")],
        );
        metric.set(43, &[KeyValue::new("beer", "c")]);

        reg.has_metric_family("http_mem_bytes")
            .with_labels(&[("tier", "a")])
            .gauge()
            .eq(40.0)
            .unwrap();

        reg.has_metric_family("http_mem_bytes")
            .with_labels(&[("tier", "b")])
            .gauge()
            .eq(42.0)
            .unwrap();

        reg.has_metric_family("http_mem_bytes")
            .with_labels(&[("tier", "a"), ("beer", "c")])
            .gauge()
            .eq(43.0)
            .unwrap();

        let rendered = reg.registry().metrics_as_str();
        assert_eq!(
            rendered,
            vec![
                r#"# HELP http_mem_bytes currently used bytes"#,
                r#"# TYPE http_mem_bytes gauge"#,
                r#"http_mem_bytes{tier="a"} 40"#,
                r#"http_mem_bytes{tier="b"} 42"#,
                r#"http_mem_bytes{beer="c",tier="a"} 43"#,
                "",
            ]
            .join("\n")
        );

        metric.inc(100, &[KeyValue::new("me", "new")]);
        reg.has_metric_family("http_mem_bytes")
            .with_labels(&[("tier", "a"), ("me", "new")])
            .gauge()
            .eq(100.0)
            .unwrap();

        metric.inc(11, &[KeyValue::new("me", "new")]);
        reg.has_metric_family("http_mem_bytes")
            .with_labels(&[("tier", "a"), ("me", "new")])
            .gauge()
            .eq(111.0)
            .unwrap();

        metric.decr(11, &[]);
        reg.has_metric_family("http_mem_bytes")
            .with_labels(&[("tier", "a")])
            .gauge()
            .eq(29.0)
            .unwrap();
    }

    #[test]
    fn metric_observer() {
        let reg = TestMetricRegistry::default();
        let domain = reg.registry().register_domain("http");

        let value = Arc::new(Mutex::new(40_u64));

        // create a gauge metric
        domain.register_observer(
            None,
            &[KeyValue::new("tier", "a")],
            |observer: MetricObserverBuilder<'_>| {
                let value_captured = Arc::clone(&value);
                observer.register_gauge_u64(
                    "mem",
                    Some("bytes"),
                    "currently used bytes",
                    move |observer| observer.observe(*value_captured.lock().unwrap(), &[]),
                )
            },
        );

        domain.register_observer(
            None,
            &[KeyValue::new("tier", "a")],
            |observer: MetricObserverBuilder<'_>| {
                observer.register_gauge_u64(
                    "disk",
                    Some("bytes"),
                    "currently used bytes",
                    move |observer| observer.observe(43, &[KeyValue::new("beer", "b")]),
                )
            },
        );

        reg.has_metric_family("http_mem_bytes")
            .with_labels(&[("tier", "a")])
            .gauge()
            .eq(40.0)
            .unwrap();

        {
            let mut value = value.lock().unwrap();
            *value = 42;
        }

        reg.has_metric_family("http_mem_bytes")
            .with_labels(&[("tier", "a")])
            .gauge()
            .eq(42.0)
            .unwrap();

        reg.has_metric_family("http_disk_bytes")
            .with_labels(&[("beer", "b"), ("tier", "a")])
            .gauge()
            .eq(43.0)
            .unwrap();
    }

    #[test]
    fn test_basic() {
        let reg = TestMetricRegistry::default();
        let d1 = reg.registry().register_domain("A");
        let d2 = reg.registry().register_domain("A");

        d1.meter
            .u64_sum_observer("mem", |observer| {
                observer.observe(23, &[KeyValue::new("a", "b")])
            })
            .init();

        d2.meter
            .u64_sum_observer("mem", |observer| {
                observer.observe(232, &[KeyValue::new("c", "d")])
            })
            .init();

        reg.has_metric_family("mem")
            .with_labels(&[("a", "b")])
            .counter()
            .eq(23.)
            .unwrap();

        // If this passes it implies the upstream SDK has been fixed and the hacky
        // observer shim can be removed
        reg.has_metric_family("mem")
            .with_labels(&[("c", "d")])
            .counter()
            .eq(232.)
            .expect_err("expected default SDK to not handle correctly");
    }

    #[test]
    fn test_duplicate() {
        let reg = TestMetricRegistry::default();
        let domain = reg.registry().register_domain("test");

        domain.register_observer(None, &[], |builder: MetricObserverBuilder<'_>| {
            builder.register_gauge_u64("mem", None, "", |observer| {
                observer.observe(1, &[KeyValue::new("a", "b")]);
                observer.observe(2, &[KeyValue::new("c", "d")]);
            });

            builder.register_gauge_u64("mem", None, "", |observer| {
                observer.observe(3, &[KeyValue::new("e", "f")]);
            });

            builder.register_counter_u64("disk", None, "", |observer| {
                observer.observe(1, &[KeyValue::new("a", "b")]);
            });

            builder.register_counter_f64("float", None, "", |observer| {
                observer.observe(1., &[KeyValue::new("a", "b")]);
            });

            builder.register_gauge_f64("gauge", None, "", |observer| {
                observer.observe(1., &[KeyValue::new("a", "b")]);
            });
        });

        domain.register_observer(None, &[], |builder: MetricObserverBuilder<'_>| {
            builder.register_gauge_u64("mem", None, "", |observer| {
                observer.observe(4, &[KeyValue::new("g", "h")]);
            });

            builder.register_gauge_f64("gauge", None, "", |observer| {
                observer.observe(2., &[KeyValue::new("c", "d")]);
            });
        });

        reg.has_metric_family("test_mem")
            .with_labels(&[("a", "b")])
            .gauge()
            .eq(1.)
            .unwrap();

        reg.has_metric_family("test_mem")
            .with_labels(&[("c", "d")])
            .gauge()
            .eq(2.)
            .unwrap();

        reg.has_metric_family("test_mem")
            .with_labels(&[("e", "f")])
            .gauge()
            .eq(3.)
            .unwrap();

        reg.has_metric_family("test_mem")
            .with_labels(&[("g", "h")])
            .gauge()
            .eq(4.)
            .unwrap();

        reg.has_metric_family("test_disk_total")
            .with_labels(&[("a", "b")])
            .counter()
            .eq(1.)
            .unwrap();

        reg.has_metric_family("test_float_total")
            .with_labels(&[("a", "b")])
            .counter()
            .eq(1.)
            .unwrap();

        reg.has_metric_family("test_gauge")
            .with_labels(&[("a", "b")])
            .gauge()
            .eq(1.)
            .unwrap();

        reg.has_metric_family("test_gauge")
            .with_labels(&[("c", "d")])
            .gauge()
            .eq(2.)
            .unwrap();
    }

    #[test]
    fn test_default_labels() {
        let reg = TestMetricRegistry::default();
        let registry = reg.registry();
        let domain =
            registry.register_domain_with_labels("test", vec![KeyValue::new("foo", "bar")]);

        // Test Gauge

        let mut guage = domain.register_gauge_metric_with_labels(
            "mem",
            Some("bytes"),
            "currently used bytes",
            &[KeyValue::new("tier", "a")],
        );
        guage.set(2, &[]);

        reg.has_metric_family("test_mem_bytes")
            .with_labels(&[("foo", "bar"), ("tier", "a")])
            .gauge()
            .eq(2.)
            .unwrap();

        let mut guage = domain.register_gauge_metric_with_labels(
            "override",
            Some("bytes"),
            "currently used bytes",
            &[KeyValue::new("foo", "ooh")],
        );
        guage.set(2, &[]);

        reg.has_metric_family("test_override_bytes")
            .with_labels(&[("foo", "ooh")])
            .gauge()
            .eq(2.)
            .unwrap();

        let mut guage = domain.register_gauge_metric_with_labels(
            "override2",
            Some("bytes"),
            "currently used bytes",
            &[KeyValue::new("foo", "ooh")],
        );
        guage.set(2, &[KeyValue::new("foo", "heh")]);

        reg.has_metric_family("test_override2_bytes")
            .with_labels(&[("foo", "heh")])
            .gauge()
            .eq(2.)
            .unwrap();

        // Test Counter

        let counter = domain.register_counter_metric_with_labels(
            "mem",
            Some("bytes"),
            "currently used bytes",
            vec![KeyValue::new("tier", "a")],
        );
        counter.add(2);

        reg.has_metric_family("test_mem_bytes_total")
            .with_labels(&[("foo", "bar"), ("tier", "a")])
            .counter()
            .eq(2.)
            .unwrap();

        let counter = domain.register_counter_metric_with_labels(
            "override",
            Some("bytes"),
            "currently used bytes",
            vec![KeyValue::new("foo", "ooh")],
        );
        counter.add(2);

        reg.has_metric_family("test_override_bytes_total")
            .with_labels(&[("foo", "ooh")])
            .counter()
            .eq(2.)
            .unwrap();

        let counter = domain.register_counter_metric_with_labels(
            "override2",
            Some("bytes"),
            "currently used bytes",
            vec![KeyValue::new("foo", "ooh")],
        );
        counter.inc_with_labels(&[KeyValue::new("foo", "heh")]);

        reg.has_metric_family("test_override2_bytes_total")
            .with_labels(&[("foo", "heh")])
            .counter()
            .eq(1.)
            .unwrap();

        // Test Observer

        domain.register_observer(None, &[], |builder: MetricObserverBuilder<'_>| {
            builder.register_gauge_f64("observer_f64", None, "", |result| result.observe(2., &[]))
        });

        reg.has_metric_family("test_observer_f64")
            .with_labels(&[("foo", "bar")])
            .gauge()
            .eq(2.)
            .unwrap();

        domain.register_observer(None, &[], |builder: MetricObserverBuilder<'_>| {
            builder.register_gauge_f64("observer_f64", None, "", |result| {
                result.observe(2., &[KeyValue::new("inner", "foo")])
            })
        });

        reg.has_metric_family("test_observer_f64")
            .with_labels(&[("foo", "bar"), ("inner", "foo")])
            .gauge()
            .eq(2.)
            .unwrap();

        domain.register_observer(
            None,
            &[KeyValue::new("bingo", "bongo")],
            |builder: MetricObserverBuilder<'_>| {
                builder.register_gauge_u64("observer_u64", None, "", |result| {
                    result.observe(9, &[KeyValue::new("inner", "foo")])
                })
            },
        );

        reg.has_metric_family("test_observer_u64")
            .with_labels(&[("foo", "bar"), ("inner", "foo"), ("bingo", "bongo")])
            .gauge()
            .eq(9.)
            .unwrap();

        domain.register_observer(
            None,
            &[KeyValue::new("bingo", "bongo")],
            |builder: MetricObserverBuilder<'_>| {
                builder.register_gauge_f64("observer_f64", None, "", |result| {
                    result.observe(
                        4.,
                        &[KeyValue::new("inner", "foo"), KeyValue::new("foo", "boo")],
                    )
                })
            },
        );

        reg.has_metric_family("test_observer_f64")
            .with_labels(&[("foo", "boo"), ("inner", "foo"), ("bingo", "bongo")])
            .gauge()
            .eq(4.)
            .unwrap();

        domain.register_observer(
            None,
            &[KeyValue::new("bingo", "bongo"), KeyValue::new("foo", "baz")],
            |builder: MetricObserverBuilder<'_>| {
                builder.register_gauge_f64("observer_f64", None, "", |result| {
                    result.observe(
                        6.,
                        &[KeyValue::new("inner", "foo"), KeyValue::new("foo", "foo")],
                    )
                })
            },
        );

        reg.has_metric_family("test_observer_f64")
            .with_labels(&[("foo", "foo"), ("inner", "foo"), ("bingo", "bongo")])
            .gauge()
            .eq(6.)
            .unwrap();

        domain.register_observer(
            None,
            &[KeyValue::new("bingo", "bongo"), KeyValue::new("foo", "woo")],
            |builder: MetricObserverBuilder<'_>| {
                builder.register_counter_u64("observer_u64", None, "", |result| {
                    result.observe(6, &[KeyValue::new("inner", "foo")])
                })
            },
        );

        reg.has_metric_family("test_observer_u64_total")
            .with_labels(&[("foo", "woo"), ("inner", "foo"), ("bingo", "bongo")])
            .counter()
            .eq(6.)
            .unwrap();
    }

    #[test]
    fn test_gauge_value() {
        let reg = TestMetricRegistry::default();
        let registry = reg.registry();
        let domain =
            registry.register_domain_with_labels("catalog", vec![KeyValue::new("foo", "bar")]);

        let gauge = domain.register_gauge_metric_with_labels(
            "chunk_mem",
            Some("bytes"),
            "bytes tracker",
            &[KeyValue::new("state", "mub")],
        );

        let mut a = gauge.gauge_value(&[KeyValue::new("state", "mub")]);
        let mut b = gauge.gauge_value(&[KeyValue::new("state", "mub")]);

        reg.has_metric_family("catalog_chunk_mem_bytes")
            .with_labels(&[("foo", "bar"), ("state", "mub")])
            .gauge()
            .eq(0.)
            .unwrap();

        a.set(2);

        reg.has_metric_family("catalog_chunk_mem_bytes")
            .with_labels(&[("foo", "bar"), ("state", "mub")])
            .gauge()
            .eq(2.)
            .unwrap();

        b.inc(5);
        a.inc(2);

        reg.has_metric_family("catalog_chunk_mem_bytes")
            .with_labels(&[("foo", "bar"), ("state", "mub")])
            .gauge()
            .eq(9.)
            .unwrap();

        std::mem::drop(b);

        reg.has_metric_family("catalog_chunk_mem_bytes")
            .with_labels(&[("foo", "bar"), ("state", "mub")])
            .gauge()
            .eq(4.)
            .unwrap();

        std::mem::drop(a);

        reg.has_metric_family("catalog_chunk_mem_bytes")
            .with_labels(&[("foo", "bar"), ("state", "mub")])
            .gauge()
            .eq(0.)
            .unwrap();
    }

    #[test]
    fn test_gauge_multiple() {
        let reg = TestMetricRegistry::default();
        let registry = reg.registry();
        let domain =
            registry.register_domain_with_labels("catalog", vec![KeyValue::new("foo", "bar")]);

        let mut gauge1 = domain.register_gauge_metric("read_buffer", None, "testy");

        let mut gauge2 = domain.register_gauge_metric("read_buffer", None, "testy");

        gauge1.inc(5, &[KeyValue::new("enc", "foo")]);
        reg.has_metric_family("catalog_read_buffer")
            .with_labels(&[("foo", "bar"), ("enc", "foo")])
            .gauge()
            .eq(5.)
            .unwrap();

        gauge2.inc(7, &[KeyValue::new("enc", "foo")]);

        reg.has_metric_family("catalog_read_buffer")
            .with_labels(&[("foo", "bar"), ("enc", "foo")])
            .gauge()
            .eq(12.)
            .unwrap();

        gauge2.inc(7, &[KeyValue::new("enc", "bar")]);

        reg.has_metric_family("catalog_read_buffer")
            .with_labels(&[("foo", "bar"), ("enc", "bar")])
            .gauge()
            .eq(7.)
            .unwrap();

        std::mem::drop(gauge2);

        reg.has_metric_family("catalog_read_buffer")
            .with_labels(&[("foo", "bar"), ("enc", "bar")])
            .gauge()
            .eq(0.)
            .unwrap();

        reg.has_metric_family("catalog_read_buffer")
            .with_labels(&[("foo", "bar"), ("enc", "foo")])
            .gauge()
            .eq(5.)
            .unwrap();

        std::mem::drop(gauge1);

        reg.has_metric_family("catalog_read_buffer")
            .with_labels(&[("foo", "bar"), ("enc", "foo")])
            .gauge()
            .eq(0.)
            .unwrap();
    }
}
