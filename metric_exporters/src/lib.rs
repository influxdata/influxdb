#![deny(rustdoc::broken_intra_doc_links, rustdoc::bare_urls, rust_2018_idioms)]
#![warn(
    missing_debug_implementations,
    clippy::explicit_iter_loop,
    clippy::use_self,
    clippy::clone_on_ref_ptr
)]

use metric::{Attributes, MetricKind, Observation};
use std::io::Write;

use observability_deps::tracing::error;
use prometheus::proto::{Bucket, Histogram};
use prometheus::{
    proto::{Counter, Gauge, LabelPair, Metric, MetricFamily, MetricType},
    Encoder, TextEncoder,
};

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
    metric: Option<MetricFamily>,
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

impl<'a, W: Write> metric::Reporter for PrometheusTextEncoder<'a, W> {
    fn start_metric(
        &mut self,
        metric_name: &'static str,
        description: &'static str,
        kind: MetricKind,
    ) {
        assert!(self.metric.is_none(), "metric already in progress");

        let (name, metric_type) = match kind {
            MetricKind::U64Counter => (format!("{}_total", metric_name), MetricType::COUNTER),
            MetricKind::U64Gauge => (metric_name.to_string(), MetricType::GAUGE),
            MetricKind::U64Histogram => (metric_name.to_string(), MetricType::HISTOGRAM),
            MetricKind::DurationCounter => (
                format!("{}_seconds_total", metric_name),
                MetricType::COUNTER,
            ),
            MetricKind::DurationGauge => (format!("{}_seconds", metric_name), MetricType::GAUGE),
            MetricKind::DurationHistogram => {
                (format!("{}_seconds", metric_name), MetricType::HISTOGRAM)
            }
        };

        let mut metric = MetricFamily::new();
        metric.set_name(name);
        metric.set_help(description.to_string());
        metric.set_field_type(metric_type);

        self.metric = Some(metric)
    }

    fn report_observation(&mut self, attributes: &Attributes, observation: Observation) {
        let metrics = self
            .metric
            .as_mut()
            .expect("no metric in progress")
            .mut_metric();

        let mut metric = Metric::new();

        for (name, value) in attributes.iter() {
            let mut pair = LabelPair::new();
            pair.set_name(name.to_string());
            pair.set_value(value.to_string());
            metric.mut_label().push(pair)
        }

        match observation {
            Observation::U64Counter(v) => {
                let mut counter = Counter::new();
                counter.set_value(v as f64);
                metric.set_counter(counter)
            }
            Observation::U64Gauge(v) => {
                let mut gauge = Gauge::new();
                gauge.set_value(v as f64);
                metric.set_gauge(gauge)
            }
            Observation::DurationCounter(v) => {
                let mut counter = Counter::new();
                counter.set_value(v.as_secs_f64());
                metric.set_counter(counter)
            }
            Observation::DurationGauge(v) => {
                let mut gauge = Gauge::new();
                gauge.set_value(v.as_secs_f64());
                metric.set_gauge(gauge)
            }
            Observation::U64Histogram(v) => {
                let mut histogram = Histogram::new();
                let mut cumulative_count = 0;

                for observation in v.buckets {
                    cumulative_count += observation.count;

                    let mut bucket = Bucket::new();
                    let le = match observation.le {
                        u64::MAX => f64::INFINITY,
                        v => v as f64,
                    };

                    bucket.set_upper_bound(le);
                    bucket.set_cumulative_count(cumulative_count);
                    histogram.mut_bucket().push(bucket)
                }

                histogram.set_sample_count(cumulative_count);
                histogram.set_sample_sum(v.total as f64);
                metric.set_histogram(histogram)
            }
            Observation::DurationHistogram(v) => {
                let mut histogram = Histogram::new();
                let mut cumulative_count = 0;

                for observation in v.buckets {
                    cumulative_count += observation.count;

                    let mut bucket = Bucket::new();
                    let le = match observation.le {
                        metric::DURATION_MAX => f64::INFINITY,
                        v => v.as_secs_f64(),
                    };

                    bucket.set_upper_bound(le);
                    bucket.set_cumulative_count(cumulative_count);
                    histogram.mut_bucket().push(bucket)
                }

                histogram.set_sample_count(cumulative_count);
                histogram.set_sample_sum(v.total.as_secs_f64());
                metric.set_histogram(histogram)
            }
        };
        metrics.push(metric)
    }

    fn finish_metric(&mut self) {
        if let Some(family) = self.metric.take() {
            match self.encoder.encode(&[family], self.writer) {
                Ok(_) => {}
                Err(e) => error!(%e, "error encoding metric family"),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use metric::{
        DurationCounter, DurationGauge, Metric, Registry, U64Counter, U64Histogram,
        U64HistogramOptions,
    };
    use std::time::Duration;

    #[test]
    fn test_encode() {
        let registry = Registry::new();

        let counter: Metric<U64Counter> = registry.register_metric("foo", "a counter metric");

        let counter_value = counter.recorder(&[("tag1", "value"), ("tag2", "value")]);
        counter_value.inc(5);

        let counter_value2 = counter.recorder(&[("tag1", "value"), ("tag2", "value2")]);
        counter_value2.inc(7);

        let histogram: Metric<U64Histogram> =
            registry.register_metric_with_options("bar", "a histogram metric", || {
                U64HistogramOptions::new([5, 10, 50])
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
bar_bucket{tag1="value1",le="+Inf"} 3
bar_sum{tag1="value1"} 53
bar_count{tag1="value1"} 3
bar_bucket{tag1="value2",le="5"} 0
bar_bucket{tag1="value2",le="10"} 1
bar_bucket{tag1="value2",le="50"} 2
bar_bucket{tag1="value2",le="+Inf"} 2
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

        assert_eq!(&buffer, expected, "{}", buffer)
    }
}
