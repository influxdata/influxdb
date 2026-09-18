use crate::classify::Classification;
use crate::query_variant::{QueryVariant, QueryVariantExt};
use hashbrown::HashMap;
use http::Method;
use metric::{
    Attributes, DurationHistogram, Metric, ResultMetric, U64Counter, U64Histogram,
    U64HistogramOptions,
};
use parking_lot::{MappedMutexGuard, Mutex, MutexGuard};
use std::sync::Arc;
use std::time::Instant;

/// The family of [`RequestMetrics`] to publish
#[derive(Debug, Copy, Clone)]
pub enum MetricFamily {
    HttpServer,
    GrpcServer,
    HttpClient,
    GrpcClient,
}

/// Maps a raw request path to a bounded route-template label value.
struct PathNormalizer(Box<dyn Fn(&str) -> &'static str + Send + Sync>);

impl std::fmt::Debug for PathNormalizer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("PathNormalizer(..)")
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct MetricsKey {
    /// request path or None for 404 responses
    path: Option<String>,

    /// method or None for invalid methods
    method: Option<Method>,

    /// query variant
    query_variant: Option<QueryVariant>,
}

/// Metrics collected for HTTP/gRPC requests
#[derive(Debug)]
pub struct RequestMetrics {
    /// Whether this `MetricCollection`
    family: MetricFamily,

    /// Metric registry for registering new metrics
    metric_registry: Arc<metric::Registry>,

    /// Metrics.
    metrics: Mutex<HashMap<MetricsKey, Metrics>>,

    /// Optional hook mapping raw request paths to bounded label values.
    path_normalizer: Option<PathNormalizer>,
}

impl RequestMetrics {
    pub fn new(metric_registry: Arc<metric::Registry>, family: MetricFamily) -> Self {
        Self {
            family,
            metric_registry,
            metrics: Default::default(),
            path_normalizer: None,
        }
    }

    /// Map every recorded path through `normalizer`, bounding the `path` and
    /// `method_path` label values to the normalizer's (static) output set.
    /// This bounds the number of distinct label values by construction.
    pub fn with_path_normalizer(
        mut self,
        normalizer: impl Fn(&str) -> &'static str + Send + Sync + 'static,
    ) -> Self {
        self.path_normalizer = Some(PathNormalizer(Box::new(normalizer)));
        self
    }

    /// Gets the `MetricsRecorder` for a given http request
    pub(crate) fn recorder<B>(
        self: &Arc<Self>,
        request: &http::Request<B>,
        query_variant: QueryVariantExt,
    ) -> MetricsRecorder {
        MetricsRecorder {
            metrics: Arc::clone(self),
            start_instant: Instant::now(),
            path: Some(request.uri().path().to_string()),
            method: Some(request.method().clone()),
            classification: None,
            response_body_size: 0,
            query_variant,
        }
    }

    fn request_metrics(
        &self,
        path: Option<String>,
        method: Option<Method>,
        query_variant: Option<QueryVariant>,
    ) -> MappedMutexGuard<'_, Metrics> {
        // method is only important for HTTP / non-gRPC
        let method = match self.family {
            MetricFamily::HttpServer | MetricFamily::HttpClient => method,
            MetricFamily::GrpcServer | MetricFamily::GrpcClient => None,
        };

        let method = method.filter(is_standard_method);

        let path = path.map(|p| match &self.path_normalizer {
            Some(normalizer) => (normalizer.0)(&p).to_string(),
            None => p,
        });

        MutexGuard::map(self.metrics.lock(), |metrics| {
            let key = MetricsKey {
                path,
                method,
                query_variant,
            };
            let (_, request_metrics) =
                metrics.raw_entry_mut().from_key(&key).or_insert_with(|| {
                    let mut attributes = Attributes::from([]);
                    if let Some(path) = &key.path {
                        attributes.insert("path", path.to_string());
                    }
                    if let Some(method) = &key.method {
                        attributes.insert("method", method.to_string());
                    }
                    if let Some(query_variant) = &key.query_variant {
                        attributes.insert("query_variant", query_variant.str());
                    }
                    if let (Some(path), Some(method)) = (&key.path, &key.method) {
                        // help Grafana because you can only repeat a single variable, not a cross-product of the two
                        attributes.insert("method_path", format!("{method} {path}"));
                    }

                    let metrics =
                        Metrics::new(self.metric_registry.as_ref(), attributes, self.family);

                    (key, metrics)
                });
            request_metrics
        })
    }
}

/// Custom (extension) HTTP methods are client-controlled strings; treat them
/// like invalid methods so they cannot mint per-method label values.
fn is_standard_method(method: &Method) -> bool {
    const STANDARD: &[Method] = &[
        Method::GET,
        Method::POST,
        Method::PUT,
        Method::DELETE,
        Method::HEAD,
        Method::OPTIONS,
        Method::PATCH,
        Method::CONNECT,
        Method::TRACE,
    ];
    STANDARD.contains(method)
}

/// The request metrics for a specific set of attributes (e.g. path)
#[derive(Debug)]
struct Metrics {
    /// Counts of un-aborted requests
    request_count: ResultMetric<U64Counter>,

    /// Count of aborted requests
    aborted_count: U64Counter,

    /// Latency distribution of non-aborted requests
    request_duration: ResultMetric<DurationHistogram>,

    /// Response body size distribution for non-aborted requests.
    response_body_size: ResultMetric<U64Histogram>,
}

impl Metrics {
    fn new(
        registry: &metric::Registry,
        attributes: impl Into<Attributes>,
        family: MetricFamily,
    ) -> Self {
        let (counter, duration, response_body_size) = match family {
            MetricFamily::GrpcServer => (
                "grpc_requests",
                "grpc_request_duration",
                "grpc_response_body_size_bytes",
            ),
            MetricFamily::HttpServer => (
                "http_requests",
                "http_request_duration",
                "http_response_body_size_bytes",
            ),
            MetricFamily::GrpcClient => (
                "grpc_client_requests",
                "grpc_client_request_duration",
                "grpc_client_response_body_size_bytes",
            ),
            MetricFamily::HttpClient => (
                "http_client_requests",
                "http_client_request_duration",
                "http_client_response_body_size_bytes",
            ),
        };

        let counter: Metric<U64Counter> =
            registry.register_metric(counter, "accumulated total requests");

        let duration: Metric<DurationHistogram> =
            registry.register_metric(duration, "distribution of request latencies");

        let response_body_size: Metric<U64Histogram> = registry.register_metric_with_options(
            response_body_size,
            "distribution of response body size in bytes",
            || {
                U64HistogramOptions::new([
                    100,
                    1_000, // 1KiB
                    10_000,
                    100_000,
                    1_000_000, // 1MiB
                    10_000_000,
                    u64::MAX,
                ])
            },
        );

        let mut attributes = attributes.into();
        let count = ResultMetric::new(&counter, attributes.clone());
        let duration = ResultMetric::new(&duration, attributes.clone());
        let response_body_size = ResultMetric::new(&response_body_size, attributes.clone());

        attributes.insert("status", "aborted");
        let aborted_count = counter.recorder(attributes);

        Self {
            request_count: count,
            request_duration: duration,
            aborted_count,
            response_body_size,
        }
    }
}

/// A `MetricsRecorder` is used to record metrics for a given http request
#[derive(Debug)]
pub(crate) struct MetricsRecorder {
    metrics: Arc<RequestMetrics>,
    start_instant: Instant,
    path: Option<String>,
    method: Option<Method>,
    classification: Option<Classification>,
    response_body_size: u64,
    query_variant: QueryVariantExt,
}

impl MetricsRecorder {
    /// Sets the classification of this request if not already set
    pub(crate) fn set_classification(&mut self, classification: Classification) {
        if matches!(classification, Classification::PathNotFound) {
            // Don't want to pollute metrics with invalid paths
            self.path = None
        }
        if matches!(classification, Classification::MethodNotAllowed) {
            // Don't want to pollute metrics with invalid methods
            self.method = None
        }

        self.classification = Some(match self.classification {
            Some(existing) => existing.max(classification),
            None => classification,
        });
    }

    /// Register additional response body size.
    pub(crate) fn add_response_body_size(&mut self, bytes: u64) {
        self.response_body_size += bytes;
    }
}

impl Drop for MetricsRecorder {
    fn drop(&mut self) {
        if self.classification.is_none() && matches!(self.metrics.family, MetricFamily::GrpcServer)
        {
            // An aborted gRPC server request never had its path validated by
            // service routing (unknown methods normally classify as
            // PathNotFound via grpc-status 12), so arbitrary client-supplied
            // paths could mint unbounded series; do not record the path.
            // Client-side paths come from the application's own outgoing
            // calls, a bounded set with no spray risk, so they keep per-method
            // abort attribution.
            self.path = None;
        }

        let metrics = self.metrics.request_metrics(
            self.path.take(),
            self.method.take(),
            self.query_variant.get(),
        );

        let duration = self.start_instant.elapsed();
        match self.classification {
            Some(Classification::Ok) => {
                metrics.request_count.ok.inc(1);
                metrics.request_duration.ok.record(duration);
                metrics
                    .response_body_size
                    .ok
                    .record(self.response_body_size);
            }
            Some(Classification::ClientErr)
            | Some(Classification::PathNotFound)
            | Some(Classification::MethodNotAllowed) => {
                metrics.request_count.client_error.inc(1);
                metrics.request_duration.client_error.record(duration);
                metrics
                    .response_body_size
                    .client_error
                    .record(self.response_body_size);
            }
            Some(Classification::ServerErr) => {
                metrics.request_count.server_error.inc(1);
                metrics.request_duration.server_error.record(duration);
                metrics
                    .response_body_size
                    .server_error
                    .record(self.response_body_size);
            }
            Some(Classification::ResourceExhausted) => {
                metrics.request_count.resource_exhausted.inc(1);
                metrics.request_duration.resource_exhausted.record(duration);
                metrics
                    .response_body_size
                    .resource_exhausted
                    .record(self.response_body_size);
            }
            Some(Classification::UnexpectedResponse) => {
                metrics.request_count.unexpected_response.inc(1);
                metrics
                    .request_duration
                    .unexpected_response
                    .record(duration);
                metrics
                    .response_body_size
                    .unexpected_response
                    .record(self.response_body_size);
            }
            None => metrics.aborted_count.inc(1),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use metric::{Observation, RawReporter};

    #[test]
    fn aborted_grpc_requests_do_not_mint_path_series() {
        let registry = Arc::new(metric::Registry::new());
        let metrics = Arc::new(RequestMetrics::new(
            Arc::clone(&registry),
            MetricFamily::GrpcServer,
        ));
        let request = http::Request::builder()
            .method(Method::POST)
            .uri("/sprayed.Garbage/Path")
            .body(())
            .unwrap();
        // dropped without classification = aborted
        drop(metrics.recorder(&request, QueryVariantExt::default()));

        let mut reporter = RawReporter::default();
        registry.report(&mut reporter);
        let observations = reporter.metric("grpc_requests").unwrap();
        for (attributes, _) in &observations.observations {
            assert!(
                attributes.iter().all(|(key, _)| *key != "path"),
                "aborted gRPC request must not retain its path: {attributes:?}"
            );
        }

        let aborted: Vec<_> = observations
            .observations
            .iter()
            .filter(|(attributes, _)| {
                attributes
                    .iter()
                    .any(|(key, value)| *key == "status" && value.as_ref() == "aborted")
                    && attributes.iter().all(|(key, _)| *key != "path")
            })
            .collect();
        assert_eq!(
            aborted.len(),
            1,
            "expected exactly one path-less aborted series: {observations:?}"
        );
        assert_eq!(aborted[0].1, Observation::U64Counter(1));
    }

    #[test]
    fn aborted_grpc_client_requests_keep_their_path() {
        let registry = Arc::new(metric::Registry::new());
        let metrics = Arc::new(RequestMetrics::new(
            Arc::clone(&registry),
            MetricFamily::GrpcClient,
        ));
        let request = http::Request::builder()
            .method(Method::POST)
            .uri("/ingester.WriteService/Write")
            .body(())
            .unwrap();
        drop(metrics.recorder(&request, QueryVariantExt::default()));

        let mut reporter = RawReporter::default();
        registry.report(&mut reporter);
        let observations = reporter.metric("grpc_client_requests").unwrap();
        assert!(
            observations
                .observations
                .iter()
                .any(|(attributes, _)| attributes.iter().any(|(key, value)| {
                    *key == "path" && value.as_ref() == "/ingester.WriteService/Write"
                })),
            "aborted gRPC client requests keep their path (bounded by the application's own outgoing calls)"
        );
    }

    #[test]
    fn aborted_http_requests_keep_their_path() {
        let registry = Arc::new(metric::Registry::new());
        let metrics = Arc::new(RequestMetrics::new(
            Arc::clone(&registry),
            MetricFamily::HttpServer,
        ));
        let request = http::Request::builder()
            .method(Method::GET)
            .uri("/api/v3/query_sql")
            .body(())
            .unwrap();
        drop(metrics.recorder(&request, QueryVariantExt::default()));

        let mut reporter = RawReporter::default();
        registry.report(&mut reporter);
        let observations = reporter.metric("http_requests").unwrap();
        assert!(
            observations
                .observations
                .iter()
                .any(|(attributes, _)| attributes.iter().any(|(key, value)| {
                    *key == "path" && value.as_ref() == "/api/v3/query_sql"
                })),
            "aborted HTTP requests keep their path; the record-time path normalizer bounds its cardinality when one is configured"
        );
    }

    #[test]
    fn aborted_http_requests_fold_path_through_normalizer() {
        let registry = Arc::new(metric::Registry::new());
        let metrics = Arc::new(
            RequestMetrics::new(Arc::clone(&registry), MetricFamily::HttpServer)
                .with_path_normalizer(|path| {
                    if path == "/health" {
                        "/health"
                    } else {
                        "other"
                    }
                }),
        );
        let request = http::Request::builder()
            .method(Method::GET)
            .uri("/wp-admin/setup.php")
            .body(())
            .unwrap();
        // dropped without classification = aborted
        drop(metrics.recorder(&request, QueryVariantExt::default()));

        let mut reporter = RawReporter::default();
        registry.report(&mut reporter);
        let observations = reporter.metric("http_requests").unwrap();
        for (attributes, _) in &observations.observations {
            assert!(
                attributes
                    .iter()
                    .all(|(key, value)| *key != "path" || value.as_ref() == "other"),
                "aborted HTTP request leaked its raw path: {attributes:?}"
            );
        }

        let aborted: Vec<_> = observations
            .observations
            .iter()
            .filter(|(attributes, _)| {
                attributes
                    .iter()
                    .any(|(key, value)| *key == "status" && value.as_ref() == "aborted")
            })
            .collect();
        assert_eq!(
            aborted.len(),
            1,
            "expected exactly one aborted series: {observations:?}"
        );
        assert!(
            aborted[0]
                .0
                .iter()
                .any(|(key, value)| *key == "path" && value.as_ref() == "other"),
            "aborted series must carry the normalized path: {:?}",
            aborted[0].0
        );
        assert_eq!(aborted[0].1, Observation::U64Counter(1));
    }

    fn record_ok(metrics: &Arc<RequestMetrics>, method: Method, uri: &str) {
        let request = http::Request::builder()
            .method(method)
            .uri(uri)
            .body(())
            .unwrap();
        let mut recorder = metrics.recorder(&request, QueryVariantExt::default());
        recorder.set_classification(Classification::Ok);
        drop(recorder);
    }

    fn observed_attributes(registry: &metric::Registry, metric_name: &str) -> String {
        let mut reporter = RawReporter::default();
        registry.report(&mut reporter);
        format!("{:?}", reporter.metric(metric_name).unwrap().observations)
    }

    #[test]
    fn path_normalizer_bounds_label_values() {
        let registry = Arc::new(metric::Registry::new());
        let metrics = Arc::new(
            RequestMetrics::new(Arc::clone(&registry), MetricFamily::HttpServer)
                .with_path_normalizer(|path| {
                    if path == "/health" {
                        "/health"
                    } else {
                        "other"
                    }
                }),
        );
        record_ok(&metrics, Method::GET, "/health");
        record_ok(&metrics, Method::GET, "/wp-admin/setup.php");

        let attrs = observed_attributes(&registry, "http_requests");
        assert!(attrs.contains("/health"));
        assert!(attrs.contains("other"));
        assert!(!attrs.contains("wp-admin"), "raw path leaked: {attrs}");
    }

    #[test]
    fn non_standard_methods_are_not_label_values() {
        let registry = Arc::new(metric::Registry::new());
        let metrics = Arc::new(RequestMetrics::new(
            Arc::clone(&registry),
            MetricFamily::HttpServer,
        ));
        record_ok(&metrics, Method::from_bytes(b"SPRAYED").unwrap(), "/health");

        let attrs = observed_attributes(&registry, "http_requests");
        assert!(!attrs.contains("SPRAYED"), "custom method leaked: {attrs}");
        assert!(
            attrs.contains("/health"),
            "request must still be counted with its path: {attrs}"
        );
    }
}
