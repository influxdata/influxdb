use crate::classify::Classification;
use hashbrown::HashMap;
use metric::{Attributes, DurationHistogram, Metric, ResultMetric, U64Counter};
use parking_lot::{MappedMutexGuard, Mutex, MutexGuard};
use std::sync::Arc;
use std::time::Instant;

/// `MetricsCollection` is used to retrieve `MetricsRecorder` for instrumenting http requests
#[derive(Debug)]
pub struct MetricsCollection {
    /// Whether this `MetricCollection` should publish to grpc_request* or http_request*
    is_grpc: bool,

    /// Metric registry for registering new metrics
    metric_registry: Arc<metric::Registry>,

    /// Metrics keyed by request path or None for 404 responses
    metrics: Mutex<HashMap<Option<String>, Metrics>>,
}

impl MetricsCollection {
    pub fn new(metric_registry: Arc<metric::Registry>, is_grpc: bool) -> Self {
        Self {
            is_grpc,
            metric_registry,
            metrics: Default::default(),
        }
    }

    /// Gets the `MetricsRecorder` for a given http request
    pub fn recorder<B>(self: &Arc<Self>, request: &http::Request<B>) -> MetricsRecorder {
        MetricsRecorder {
            metrics: Arc::clone(self),
            start_instant: Instant::now(),
            path: Some(request.uri().path().to_string()),
            classification: None,
        }
    }

    fn request_metrics(&self, path: Option<String>) -> MappedMutexGuard<'_, Metrics> {
        MutexGuard::map(self.metrics.lock(), |metrics| {
            let (_, request_metrics) =
                metrics.raw_entry_mut().from_key(&path).or_insert_with(|| {
                    let attributes = match path.as_ref() {
                        Some(path) => Attributes::from([("path", path.clone().into())]),
                        None => Attributes::from([]),
                    };

                    let metrics =
                        Metrics::new(self.metric_registry.as_ref(), attributes, self.is_grpc);
                    (path, metrics)
                });
            request_metrics
        })
    }
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
}

impl Metrics {
    fn new(registry: &metric::Registry, attributes: impl Into<Attributes>, is_grpc: bool) -> Self {
        let (counter, duration) = match is_grpc {
            true => ("grpc_requests", "grpc_request_duration"),
            false => ("http_requests", "http_request_duration"),
        };

        let counter: Metric<U64Counter> =
            registry.register_metric(counter, "accumulated total requests");

        let duration: Metric<DurationHistogram> =
            registry.register_metric(duration, "distribution of request latencies");

        let mut attributes = attributes.into();
        let count = ResultMetric::new(&counter, attributes.clone());
        let duration = ResultMetric::new(&duration, attributes.clone());

        attributes.insert("status", "aborted");
        let aborted_count = counter.recorder(attributes);

        Self {
            request_count: count,
            request_duration: duration,
            aborted_count,
        }
    }
}

/// A `MetricsRecorder` is used to record metrics for a given http request
#[derive(Debug)]
pub struct MetricsRecorder {
    metrics: Arc<MetricsCollection>,
    start_instant: Instant,
    path: Option<String>,
    classification: Option<Classification>,
}

impl MetricsRecorder {
    /// Sets the classification of this request if not already set
    pub fn set_classification(&mut self, classification: Classification) {
        if matches!(classification, Classification::PathNotFound) {
            // Don't want to pollute metrics with invalid paths
            self.path = None
        }

        self.classification = Some(match self.classification {
            Some(existing) => existing.max(classification),
            None => classification,
        });
    }
}

impl Drop for MetricsRecorder {
    fn drop(&mut self) {
        let metrics = self.metrics.request_metrics(self.path.take());

        let duration = self.start_instant.elapsed();
        match self.classification {
            Some(Classification::Ok) => {
                metrics.request_count.ok.inc(1);
                metrics.request_duration.ok.record(duration);
            }
            Some(Classification::ClientErr) | Some(Classification::PathNotFound) => {
                metrics.request_count.client_error.inc(1);
                metrics.request_duration.client_error.record(duration);
            }
            Some(Classification::ServerErr) => {
                metrics.request_count.server_error.inc(1);
                metrics.request_duration.server_error.record(duration);
            }
            None => metrics.aborted_count.inc(1),
        }
    }
}
