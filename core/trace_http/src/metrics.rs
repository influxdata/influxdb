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

    /// Maximum path segments.
    max_path_segments: Option<usize>,
}

impl RequestMetrics {
    pub fn new(metric_registry: Arc<metric::Registry>, family: MetricFamily) -> Self {
        Self {
            family,
            metric_registry,
            metrics: Default::default(),
            max_path_segments: None,
        }
    }

    /// Restrict metric paths to `segments`
    pub fn with_max_path_segments(mut self, segments: usize) -> Self {
        self.max_path_segments = Some(segments);
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

        let path = path.map(|p| truncate_path(&p, self.max_path_segments));

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

fn truncate_path(path: &str, segments: Option<usize>) -> String {
    let search = || {
        let s = segments?;
        let mut indices = path.match_indices('/');
        for _ in 0..s {
            indices.next();
        }
        let end = indices.next()?.0;
        if end + 1 == path.len() {
            return None;
        }
        Some(format!("{}/*", &path[..end]))
    };
    search().unwrap_or_else(|| path.to_string())
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
    #[test]
    fn test_truncate() {
        assert_eq!(truncate_path("/health", Some(1)), "/health");
        assert_eq!(truncate_path("/api/v2/write", Some(3)), "/api/v2/write");
        assert_eq!(truncate_path("/api/v2/write/", Some(3)), "/api/v2/write/");
        assert_eq!(truncate_path("/api/v2/write", Some(2)), "/api/v2/*");
        assert_eq!(truncate_path("/v1/p/000000000000053e", Some(2)), "/v1/p/*");
        assert_eq!(truncate_path("/a/b/c/d/e/f", None), "/a/b/c/d/e/f");
        assert_eq!(truncate_path("/a/b/c/d/e/f/", None), "/a/b/c/d/e/f/");
        assert_eq!(truncate_path("/v1/p/", Some(2)), "/v1/p/");
    }
}
