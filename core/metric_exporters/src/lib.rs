use reqwest::Client;
// Workaround for "unused crate" lint false positives.
use workspace_hack as _;

use metric::{Attributes, MetricKind, Observation, Registry};
use std::io::Write;
use std::sync::Arc;

use prometheus::proto::{Bucket, Histogram};
use prometheus::{
    Encoder, TextEncoder,
    proto::{Counter, Gauge, LabelPair, Metric, MetricFamily, MetricType},
};
use tracing::{debug, error, warn};

/// A `metric::Reporter` that writes data in the prometheus text exposition format
///
/// In order to comply with the prometheus naming best-practices, certain metrics may have
/// a unit and/or "_total" suffix applied - <https://prometheus.io/docs/practices/naming/>
///
/// Note: this is done after the metric sort order is established - this means the output
/// order is guaranteed to be stable, but not necessarily sorted.
///
/// For example a counter named "metric" and a gauge named "metric_a" will be exported as
/// "metric_total" and "metric_a" in that order
///
#[derive(Debug)]
pub struct PrometheusTextEncoder<'a, W: Write> {
    /// metric family together with a flag indicating that it was used
    metric: Option<(MetricFamily, bool)>,

    encoder: TextEncoder,
    writer: &'a mut W,
}

impl<'a, W: Write> PrometheusTextEncoder<'a, W> {
    pub fn new(writer: &'a mut W) -> Self {
        Self {
            metric: None,
            encoder: TextEncoder::new(),
            writer,
        }
    }
}

impl<W: Write> metric::Reporter for PrometheusTextEncoder<'_, W> {
    fn start_metric(
        &mut self,
        metric_name: &'static str,
        description: &'static str,
        kind: MetricKind,
    ) {
        assert!(self.metric.is_none(), "metric already in progress");

        let (name, metric_type) = match kind {
            MetricKind::U64Counter => (format!("{metric_name}_total"), MetricType::COUNTER),
            MetricKind::U64Gauge => (metric_name.to_string(), MetricType::GAUGE),
            MetricKind::U64Histogram => (metric_name.to_string(), MetricType::HISTOGRAM),
            MetricKind::DurationCounter => {
                (format!("{metric_name}_seconds_total"), MetricType::COUNTER)
            }
            MetricKind::DurationGauge => (format!("{metric_name}_seconds"), MetricType::GAUGE),
            MetricKind::DurationHistogram => {
                (format!("{metric_name}_seconds"), MetricType::HISTOGRAM)
            }
        };

        let mut metric = MetricFamily::default();
        metric.set_name(name);
        metric.set_help(description.to_string());
        metric.set_field_type(metric_type);

        self.metric = Some((metric, false))
    }

    fn report_observation(&mut self, attributes: &Attributes, observation: Observation) {
        let (metrics, used) = self.metric.as_mut().expect("no metric in progress");

        let metrics = metrics.mut_metric();

        let mut metric = Metric::default();

        metric.set_label(
            attributes
                .iter()
                .map(|(name, value)| {
                    let mut pair = LabelPair::default();
                    pair.set_name(name.to_string());
                    pair.set_value(value.to_string());
                    pair
                })
                .collect(),
        );

        match observation {
            Observation::U64Counter(v) => {
                let mut counter = Counter::default();
                counter.set_value(v as f64);
                metric.set_counter(counter)
            }
            Observation::U64Gauge(v) => {
                let mut gauge = Gauge::default();
                gauge.set_value(v as f64);
                metric.set_gauge(gauge)
            }
            Observation::DurationCounter(v) => {
                let mut counter = Counter::default();
                counter.set_value(v.as_secs_f64());
                metric.set_counter(counter)
            }
            Observation::DurationGauge(v) => {
                let mut gauge = Gauge::default();
                gauge.set_value(v.as_secs_f64());
                metric.set_gauge(gauge)
            }
            Observation::U64Histogram(v) => {
                let mut histogram = Histogram::default();
                let mut cumulative_count = 0;

                histogram.set_bucket(
                    v.buckets
                        .into_iter()
                        .map(|observation| {
                            cumulative_count += observation.count;

                            let mut bucket = Bucket::default();
                            let le = match observation.le {
                                u64::MAX => f64::INFINITY,
                                v => v as f64,
                            };

                            bucket.set_upper_bound(le);
                            bucket.set_cumulative_count(cumulative_count);
                            bucket
                        })
                        .collect(),
                );

                histogram.set_sample_count(cumulative_count);
                histogram.set_sample_sum(v.total as f64);
                metric.set_histogram(histogram)
            }
            Observation::DurationHistogram(v) => {
                let mut histogram = Histogram::default();
                let mut cumulative_count = 0;

                histogram.set_bucket(
                    v.buckets
                        .into_iter()
                        .map(|observation| {
                            cumulative_count += observation.count;

                            let mut bucket = Bucket::default();
                            let le = match observation.le {
                                metric::DURATION_MAX => f64::INFINITY,
                                v => v.as_secs_f64(),
                            };

                            bucket.set_upper_bound(le);
                            bucket.set_cumulative_count(cumulative_count);
                            bucket
                        })
                        .collect(),
                );

                histogram.set_sample_count(cumulative_count);
                histogram.set_sample_sum(v.total.as_secs_f64());
                metric.set_histogram(histogram)
            }
        };
        metrics.push(metric);

        *used = true;
    }

    fn finish_metric(&mut self) {
        if let Some((family, used)) = self.metric.take() {
            if !used {
                // just don't report the metric
                return;
            }

            match self.encoder.encode(&[family], self.writer) {
                Ok(_) => {}
                Err(e) => error!(%e, "error encoding metric family"),
            }
        }
    }
}

/// Implementation of a client which can be used to send metrics stored
/// within a [`Registry`] to a remote pushgateway server.
#[derive(Debug)]
pub struct PushGatewayClient {
    client: Client,
    metric_registry: Arc<Registry>,
    address: String,
    job_name: String,
}

impl PushGatewayClient {
    /// Construct a new [`PushGatewayClient`].
    pub fn new(address: &str, job_name: &str, metric_registry: Arc<Registry>) -> Self {
        Self {
            client: Client::new(),
            metric_registry,
            address: address.to_string(),
            job_name: job_name.to_string(),
        }
    }

    /// Get the full path used for the Prometheus PushGateway, containing
    /// the interpolated server address and the job name.
    fn pushgateway_path(&self) -> String {
        format!("{}/metrics/job/{}", self.address, self.job_name)
    }

    /// Get the address of the PushGateway server.
    pub fn address(&self) -> &str {
        &self.address
    }

    /// Push the metrics stored in the internal [`Registry`] to the configured server.
    pub async fn push_metrics(&self) -> Result<(), Box<dyn std::error::Error>> {
        let mut buf = Vec::new();
        let mut encoder = PrometheusTextEncoder::new(&mut buf);

        self.metric_registry.report(&mut encoder);

        let pushgateway_path = self.pushgateway_path();
        let resp = self
            .client
            .put(reqwest::Url::parse(&pushgateway_path)?)
            .body(buf)
            .send()
            .await?;

        match resp.error_for_status() {
            Ok(success) => {
                debug!(
                    status_code = success.status().as_str(),
                    pushgateway_path,
                    job_name = self.job_name,
                    "metrics pushed to gateway"
                );
                Ok(())
            }
            Err(e) => {
                warn!(
                    %e,
                    pushgateway_path,
                    job_name = self.job_name,
                    "unable to push metrics"
                );
                Err(Box::new(e))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use metric::{
        DurationCounter, DurationGauge, DurationHistogram, Metric, Registry, U64Counter,
        U64Histogram, U64HistogramOptions,
    };
    use std::time::Duration;
    use test_helpers::assert_not_contains;

    use mockito::Server;

    #[test]
    fn test_encode() {
        // tap tracing to check for errors
        let tracing_capture = test_helpers::tracing::TracingCapture::new();

        let registry = Registry::new();

        let counter: Metric<U64Counter> = registry.register_metric("foo", "a counter metric");

        let counter_value = counter.recorder(&[("tag1", "value"), ("tag2", "value")]);
        counter_value.inc(5);

        let counter_value2 = counter.recorder(&[("tag1", "value"), ("tag2", "value2")]);
        counter_value2.inc(7);

        let histogram: Metric<U64Histogram> =
            registry.register_metric_with_options("bar", "a histogram metric", || {
                U64HistogramOptions::new([5, 10, 50, u64::MAX])
            });

        let histogram_r1 = histogram.recorder(&[("tag1", "value1")]);
        let histogram_r2 = histogram.recorder(&[("tag1", "value1")]);
        let histogram_r3 = histogram.recorder(&[("tag1", "value2")]);

        histogram_r1.record(10);
        histogram_r2.record(3);
        histogram_r2.record(40);
        histogram_r3.record(8);
        histogram_r3.record(40);

        let duration: Metric<DurationGauge> =
            registry.register_metric("duration_gauge", "a duration gauge");

        duration
            .recorder(&[("tag1", "value1")])
            .set(Duration::from_millis(100));

        let duration_counter: Metric<DurationCounter> =
            registry.register_metric("duration_counter", "a duration counter");

        duration_counter
            .recorder(&[("tag1", "value1")])
            .inc(Duration::from_millis(1200));

        // unused metrics must not result in an error
        let _unused: Metric<DurationHistogram> = registry.register_metric("unused", "unused");

        let mut buffer = Vec::new();
        let mut encoder = PrometheusTextEncoder::new(&mut buffer);
        registry.report(&mut encoder);

        let buffer = String::from_utf8(buffer).unwrap();

        let expected = r#"
# HELP bar a histogram metric
# TYPE bar histogram
bar_bucket{tag1="value1",le="5"} 1
bar_bucket{tag1="value1",le="10"} 2
bar_bucket{tag1="value1",le="50"} 3
bar_bucket{tag1="value1",le="inf"} 3
bar_sum{tag1="value1"} 53
bar_count{tag1="value1"} 3
bar_bucket{tag1="value2",le="5"} 0
bar_bucket{tag1="value2",le="10"} 1
bar_bucket{tag1="value2",le="50"} 2
bar_bucket{tag1="value2",le="inf"} 2
bar_sum{tag1="value2"} 48
bar_count{tag1="value2"} 2
# HELP duration_counter_seconds_total a duration counter
# TYPE duration_counter_seconds_total counter
duration_counter_seconds_total{tag1="value1"} 1.2
# HELP duration_gauge_seconds a duration gauge
# TYPE duration_gauge_seconds gauge
duration_gauge_seconds{tag1="value1"} 0.1
# HELP foo_total a counter metric
# TYPE foo_total counter
foo_total{tag1="value",tag2="value"} 5
foo_total{tag1="value",tag2="value2"} 7
"#
        .trim_start();

        assert_eq!(&buffer, expected, "{buffer}");

        // no errors
        assert_not_contains!(tracing_capture.to_string(), "error");
    }

    #[test]
    fn pushgateway_path_behaviour() {
        let client = PushGatewayClient::new(
            "http://127.0.0.1:9091",
            "my_amazing_job",
            Arc::new(Registry::default()),
        );
        assert_eq!(
            client.pushgateway_path(),
            format!("{}/metrics/job/{}", client.address, client.job_name),
            "Unexpected change in pushgateway path usage behaviour"
        );
    }

    #[tokio::test]
    async fn pushgateway_client() {
        let mut server = Server::new_async().await;
        let job_name = "my_metrics_job";

        let mock = server
            .mock("PUT", format!("/metrics/job/{job_name}").as_str())
            .with_status(200)
            .create();

        let client = PushGatewayClient::new(&server.url(), job_name, Arc::new(Registry::default()));
        client.push_metrics().await.expect("Sending works in test");

        // Assert the client called the setup mocked path
        mock.assert();
    }
}
