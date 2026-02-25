//! Prometheus metric endpoint parser.
//!
//! This module provides a [`scrape()`] function to query a Prometheus HTTP
//! endpoint and parse the returned body into a [`MetricSet`] for inspection.
//!
#![expect(missing_copy_implementations)]

use std::collections::BTreeMap;

use ordered_float::OrderedFloat;
use reqwest::Url;
use thiserror::Error;

/// Metric scrape errors.
#[derive(Debug, Error)]
pub enum Error {
    #[error("invalid metrics url")]
    BadUrl,

    #[error("failed to scrape metrics endpoint: {0}")]
    Scrape(reqwest::Error),

    #[error("metrics read: {0}")]
    ReadBody(reqwest::Error),

    #[error("metrics parse: {0}")]
    Parse(std::io::Error),

    #[error("scrape returned failed status code: {0}")]
    StatusCode(reqwest::StatusCode),
}

/// A set of metrics scraped from a Prometheus text endpoint.
///
/// # Usage
///
/// ```rust
/// # use test_helpers::prometheus::*;
/// #
/// # let metric_set = parse(r#"bananas{path="/write",status="ok"} 42"#).unwrap();
/// #
/// let count = metric_set.metric("bananas").labels([
///     ("path", "/write"),
///     ("status", "ok"),
/// ]).unwrap_counter();
///
/// println!("{:?}", count);
/// ```
#[derive(Debug, Default, PartialOrd, Ord, PartialEq, Eq)]
pub struct MetricSet(BTreeMap<String, LabelSet>);

impl MetricSet {
    /// Extract the metric named `metric`, or panic.
    ///
    /// # Panics
    ///
    /// Panics if `metric` does not appear in the [`MetricSet`].
    pub fn metric(&self, metric: impl AsRef<str>) -> &LabelSet {
        self.0.get(metric.as_ref()).unwrap_or_else(|| {
            panic!(
                "metric {} does not appear in scrape, have: {:#?}",
                metric.as_ref(),
                self
            )
        })
    }
}

/// A [`LabelSet`] contains all sets of key=value labels and their mapped
/// values.
#[derive(Debug, Default, PartialOrd, Ord, PartialEq, Eq)]
pub struct LabelSet(BTreeMap<BTreeMap<String, String>, Value>);

impl LabelSet {
    /// Extract the value for the given label set, or panic.
    ///
    /// # Usage
    ///
    /// ```rust
    /// # use test_helpers::prometheus::*;
    /// #
    /// # let m = parse(r#"bananas{path="/write",status="ok"} 42"#).unwrap();
    /// # let label_set = m.metric("bananas");
    /// #
    /// let value = label_set.labels([
    ///     ("path", "/write"),
    ///     ("status", "ok"),
    /// ]);
    ///
    /// println!("{:?}", value);
    /// ```
    ///
    /// # Panics
    ///
    /// Panics if the metric does not have an value for the provided label set.
    pub fn labels<T, K, V>(&self, labels: T) -> &Value
    where
        T: IntoIterator<Item = (K, V)>,
        K: Into<String>,
        V: Into<String>,
    {
        let labels = labels
            .into_iter()
            .map(|(k, v)| (k.into(), v.into()))
            .collect::<BTreeMap<String, String>>();

        self.0.get(&labels).unwrap_or_else(|| {
            panic!("metric exists, but no entry for label set {labels:?}, have: {self:#?}")
        })
    }

    /// Returns the number of label sets in the parent metric.
    pub fn label_count(&self) -> usize {
        self.0.len()
    }
}

/// A typed value associated with a labelled metric.
#[derive(Debug, PartialOrd, Ord, PartialEq, Eq)]
pub enum Value {
    Counter(OrderedFloat<f64>),
    Gauge(OrderedFloat<f64>),
    Histogram(Histogram),
    Untyped(OrderedFloat<f64>),
}

impl Value {
    /// Extract a counter typed value.
    ///
    /// # Panics
    ///
    /// Panics if the value is not a counter.
    pub fn unwrap_counter(&self) -> f64 {
        match self {
            // Treat Untyped as a counter, as it's returned (only?) for
            // histogram count & sum types.
            Self::Counter(v) | Self::Untyped(v) => **v,
            _ => panic!("{self:?} is not a counter"),
        }
    }

    /// Extract a gauge typed value.
    ///
    /// # Panics
    ///
    /// Panics if the value is not a gauge.
    pub fn unwrap_gauge(&self) -> f64 {
        match self {
            Self::Gauge(v) => **v,
            _ => panic!("{self:?} is not a gauge"),
        }
    }

    /// Extract a histogram typed value.
    ///
    /// # Panics
    ///
    /// Panics if the value is not a histogram.
    pub fn unwrap_histogram(&self) -> &Histogram {
        match self {
            Self::Histogram(v) => v,
            _ => panic!("{self:?} is not a histogram"),
        }
    }
}

impl From<prometheus_parse::Value> for Value {
    fn from(value: prometheus_parse::Value) -> Self {
        match value {
            prometheus_parse::Value::Counter(v) => Self::Counter(OrderedFloat(v)),
            prometheus_parse::Value::Gauge(v) => Self::Gauge(OrderedFloat(v)),
            prometheus_parse::Value::Untyped(v) => Self::Untyped(OrderedFloat(v)),
            prometheus_parse::Value::Histogram(v) => {
                Self::Histogram(Histogram(v.into_iter().map(Into::into).collect()))
            }
            prometheus_parse::Value::Summary(_) => {
                unimplemented!("iox doesn't use summary types")
            }
        }
    }
}

/// A bucketed histogram summary type.
#[derive(Debug, Ord, PartialOrd, PartialEq, Eq)]
pub struct Histogram(Vec<HistogramCount>);

impl Histogram {
    /// Return all buckets in the histogram.
    pub fn buckets(&self) -> &[HistogramCount] {
        &self.0
    }

    /// Return the bucket count for the bucket that would hold `v`.
    pub fn bucket_count(&self, v: f64) -> u64 {
        self.0
            .iter()
            .find(|b| v <= *b.less_than_eq)
            .map(|b| b.count)
            .unwrap_or(0)
    }
}

/// A [`Histogram`] bucket.
#[derive(Debug, Ord, PartialOrd, PartialEq, Eq)]
pub struct HistogramCount {
    pub less_than_eq: OrderedFloat<f64>,
    pub count: u64,
}

impl From<prometheus_parse::HistogramCount> for HistogramCount {
    fn from(value: prometheus_parse::HistogramCount) -> Self {
        Self {
            less_than_eq: OrderedFloat::from(value.less_than),
            count: value.count as u64,
        }
    }
}

/// Make a HTTP GET request to the Prometheus metric endpoint at `url` and parse
/// the result into a [`MetricSet`].
///
/// The provided URL should include any path component (typically `/metrics`).
///
/// # Panics
///
/// Panics if a duplicate value is observed for the same `(name, labels)` tuple.
pub async fn scrape(url: impl TryInto<Url> + Send) -> Result<MetricSet, Error> {
    let url = url.try_into().map_err(|_| Error::BadUrl)?;

    // Send the HTTP request.
    let resp = reqwest::get(url).await.map_err(Error::Scrape)?;

    // Return a useful error when the endpoint returns a non-200 response.
    if !resp.status().is_success() {
        return Err(Error::StatusCode(resp.status()));
    }

    // Read the entire response body.
    let body = resp.text().await.map_err(Error::ReadBody)?;

    parse(&body)
}

/// Parse the body of a Prometheus metric endpoint.
///
/// # Panics
///
/// Panics if a duplicate value is observed for the same `(name, labels)` tuple.
pub fn parse(s: &str) -> Result<MetricSet, Error> {
    // Map it into the form the parser wants.
    let body = s.lines().map(|v| Ok(v.to_owned()));

    // Let it parse the metrics text.
    let metrics = prometheus_parse::Scrape::parse(body).map_err(Error::Parse)?;

    // And convert it into an indexed, point-in-time structure.
    let mut set = BTreeMap::<String, LabelSet>::default();

    for v in metrics.samples {
        // Extract the key=value labels for this observation.
        let scrape_labels = v
            .labels
            .iter()
            .map(|(k, v)| (k.to_owned(), v.to_owned()))
            .collect::<BTreeMap<_, _>>();

        // Insert the metric label + value into the metric set.
        let old = set
            .entry(v.metric)
            .or_default()
            .0
            .insert(scrape_labels, Value::from(v.value));

        // There should never be multiple entries for a given (metric,
        // labels) tuple.
        assert!(old.is_none(), "duplicate name + label tuple in metrics");
    }

    Ok(MetricSet(set))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Parse sample prometheus text metric output and assert the metrics are
    /// collated correctly.
    #[test]
    fn test_parse() {
        let s = std::fs::read_to_string("fixtures/prometheus.txt").expect("failed to read fixture");
        let metrics = parse(&s).expect("must parse example scrape");

        // Simple counter
        let got = metrics
            .metric("grpc_requests_total")
            .labels([
                ("path", "/arrow.flight.protocol.FlightService/DoGet"),
                ("status", "ok"),
            ])
            .unwrap_counter();

        assert_eq!(got, 7987.0);

        let got = metrics
            .metric("grpc_requests_total")
            .labels([
                ("path", "/arrow.flight.protocol.FlightService/DoGet"),
                ("status", "aborted"), // Different label
            ])
            .unwrap_counter();

        assert_eq!(got, 42.0);

        // Gauge
        let got = metrics
            .metric("jemalloc_memstats_bytes")
            .labels([("stat", "alloc")])
            .unwrap_gauge();

        assert_eq!(got, 104448.0);

        let got = metrics
            .metric("jemalloc_memstats_bytes")
            .labels([("stat", "mapped")]) // Different label
            .unwrap_gauge();

        assert_eq!(got, 8536064.0);

        // Histogram
        let got = metrics
            .metric("grpc_request_duration_seconds_count")
            .labels([
                ("path", "/arrow.flight.protocol.FlightService/DoGet"),
                ("status", "client_error"),
            ])
            .unwrap_counter();

        assert_eq!(got, 2.0);

        let got = metrics
            .metric("grpc_request_duration_seconds")
            .labels([
                ("path", "/arrow.flight.protocol.FlightService/DoGet"),
                ("status", "ok"),
            ])
            .unwrap_histogram();

        let want_buckets = [
            HistogramCount {
                less_than_eq: OrderedFloat(0.001),
                count: 0,
            },
            HistogramCount {
                less_than_eq: OrderedFloat(0.0025),
                count: 0,
            },
            HistogramCount {
                less_than_eq: OrderedFloat(0.005),
                count: 7411,
            },
            HistogramCount {
                less_than_eq: OrderedFloat(0.01),
                count: 7876,
            },
            HistogramCount {
                less_than_eq: OrderedFloat(0.025),
                count: 7986,
            },
            HistogramCount {
                less_than_eq: OrderedFloat(0.05),
                count: 7987,
            },
            HistogramCount {
                less_than_eq: OrderedFloat(0.1),
                count: 7987,
            },
            HistogramCount {
                less_than_eq: OrderedFloat(0.25),
                count: 7987,
            },
            HistogramCount {
                less_than_eq: OrderedFloat(0.5),
                count: 7987,
            },
            HistogramCount {
                less_than_eq: OrderedFloat(1.0),
                count: 7987,
            },
            HistogramCount {
                less_than_eq: OrderedFloat(2.5),
                count: 7987,
            },
            HistogramCount {
                less_than_eq: OrderedFloat(5.0),
                count: 7987,
            },
            HistogramCount {
                less_than_eq: OrderedFloat(10.0),
                count: 7987,
            },
            HistogramCount {
                less_than_eq: OrderedFloat(f64::INFINITY),
                count: 7987,
            },
        ];

        // Assert the set of buckets together.
        assert_eq!(got.buckets(), want_buckets);

        // Inspect each bucket individually.
        for b in want_buckets {
            // Check the bucket boundary.
            assert_eq!(got.bucket_count(*b.less_than_eq), b.count);

            // And find the count for a value that isn't a bucket boundary.
            assert_eq!(got.bucket_count(*b.less_than_eq - 0.000001), b.count);
        }

        // Float counter / histogram sum value.
        let got = metrics
            .metric("grpc_request_duration_seconds_sum")
            .labels([
                ("path", "/arrow.flight.protocol.FlightService/DoGet"),
                ("status", "client_error"),
            ])
            .unwrap_counter();

        assert_eq!(got, 0.000095542);
    }

    #[test]
    fn test_label_count() {
        let s = std::fs::read_to_string("fixtures/prometheus.txt").expect("failed to read fixture");
        let metrics = parse(&s).expect("must parse example scrape");

        assert_eq!(metrics.metric("jemalloc_memstats_bytes").label_count(), 6);
        assert_eq!(metrics.metric("grpc_requests_total").label_count(), 5);
        assert_eq!(
            metrics
                .metric("grpc_request_duration_seconds")
                .label_count(),
            1
        );
        assert_eq!(
            metrics
                .metric("grpc_request_duration_seconds_count")
                .label_count(),
            1
        );
        assert_eq!(
            metrics
                .metric("grpc_request_duration_seconds_sum")
                .label_count(),
            1
        );
    }
}
