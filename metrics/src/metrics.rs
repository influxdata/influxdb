use std::{
    borrow::Cow,
    fmt::Display,
    time::{Duration, Instant},
};

use opentelemetry::metrics::{Counter as OTCounter, ValueRecorder as OTHistogram};

pub use opentelemetry::KeyValue;

const RED_REQUEST_STATUS_ATTRIBUTE: &str = "status";

/// Possible types of RED metric observation status.
///
/// Ok          - an observed request was successful.
/// ClientError - an observed request was unsuccessful but it was not the fault
///               of the observed service.
/// Error       - an observed request failed and it was the fault of the
///               service.
///
/// What is the difference between `ClientError` and `Error`? The difference is
/// to do where the failure occurred. When thinking about measuring SLOs like
/// availability it's necessary to calculate things like:
///
///    Availability = 1 - (failed_requests / all_valid_requests)
///
/// `all_valid_requests` includes successful requests and any requests that
/// failed but not due to the fault of the service (e.g., client errors).
///
/// It is useful to track the components of `all_valid_requests` separately so
/// operators can also monitor external errors (client_error) errors to help
/// improve their APIs or other systems.
#[derive(Debug)]
pub enum RedRequestStatus {
    Ok,
    ClientError,
    Error,
}

impl RedRequestStatus {
    fn as_str(&self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::ClientError => "client_error",
            Self::Error => "error",
        }
    }
}

impl Display for RedRequestStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// A REDMetric is a metric that tracks requests to some resource.
///
/// The [RED methodology](https://www.weave.works/blog/the-red-method-key-metrics-for-microservices-architecture/)
/// stipulates you should track three key measures:
///
///  - Request Rate: (total number of requests / second);
///  - Error Rate: (total number of failed requests / second);
///  - Duration: (latency distributions for the various requests)
///
/// Using a `REDMetric` makes following this methodology easy because it handles
/// updating the three components for you.
pub struct RedMetric {
    requests: OTCounter<u64>,
    duration: OTHistogram<f64>,
    default_attributes: Vec<KeyValue>,
}

/// Workaround self-recursive OT instruments
/// <https://github.com/open-telemetry/opentelemetry-rust/issues/550>
impl std::fmt::Debug for RedMetric {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedMetric")
            .field("default_attributes", &self.default_attributes)
            .finish()
    }
}

impl RedMetric {
    pub(crate) fn new(
        requests: OTCounter<u64>,
        duration: OTHistogram<f64>,
        mut default_attributes: Vec<KeyValue>,
    ) -> Self {
        // TODO(edd): decide what to do if `attributes` contains
        // RED_REQUEST_STATUS_ATTRIBUTE.
        // RedMetric always has a status attribute.
        default_attributes.insert(
            0,
            KeyValue::new(RED_REQUEST_STATUS_ATTRIBUTE, RedRequestStatus::Ok.as_str()),
        );

        Self {
            requests,
            duration,
            default_attributes,
        }
    }

    /// Returns a new observation that will handle timing and recording an
    /// observation the metric is tracking.
    pub fn observation(
        &self,
    ) -> RedObservation<impl Fn(RedRequestStatus, Duration, &[KeyValue]) + '_> {
        // The recording call-back
        let record =
            move |status: RedRequestStatus, duration: Duration, attributes: &[KeyValue]| {
                let attributes = if attributes.is_empty() {
                    // If there are no attributes specified just borrow defaults
                    Cow::Borrowed(&self.default_attributes)
                } else {
                    // Otherwise merge the provided attributes and the defaults.
                    // Note: provided attributes need to go last so that they overwrite
                    // any default attributes.
                    //
                    // PERF(edd): this seems expensive to me.
                    let mut new_attributes: Vec<KeyValue> = self.default_attributes.clone();
                    new_attributes.extend_from_slice(attributes);
                    Cow::Owned(new_attributes)
                };

                match status {
                    RedRequestStatus::Ok => {
                        self.requests.add(1, &attributes);
                        self.duration.record(duration.as_secs_f64(), &attributes);
                    }
                    RedRequestStatus::ClientError => {
                        let mut attributes = attributes.into_owned();
                        attributes[0] = KeyValue::new(
                            RED_REQUEST_STATUS_ATTRIBUTE,
                            RedRequestStatus::ClientError.as_str(),
                        );

                        self.requests.add(1, &attributes);
                        self.duration.record(duration.as_secs_f64(), &attributes);
                    }
                    RedRequestStatus::Error => {
                        let mut attributes = attributes.into_owned();
                        attributes[0] = KeyValue::new(
                            RED_REQUEST_STATUS_ATTRIBUTE,
                            RedRequestStatus::Error.as_str(),
                        );

                        self.requests.add(1, &attributes);
                        self.duration.record(duration.as_secs_f64(), &attributes);
                    }
                };
            };

        RedObservation::new(record)
    }
}

#[derive(Debug, Clone)]
pub struct RedObservation<T>
where
    T: Fn(RedRequestStatus, Duration, &[KeyValue]),
{
    start: Instant,
    record: T, // a call-back that records the observation on the metric.
}

impl<T> RedObservation<T>
where
    T: Fn(RedRequestStatus, Duration, &[KeyValue]),
{
    pub(crate) fn new(record: T) -> Self {
        Self {
            start: std::time::Instant::now(),
            record,
        }
    }

    /// Record that an observation was successful. The duration of the
    /// observation should be provided. Callers might prefer `ok` where the
    /// timing will be handled for them.
    pub fn observe(
        &self,
        observation: RedRequestStatus,
        duration: Duration,
        attributes: &[KeyValue],
    ) {
        (self.record)(observation, duration, attributes);
    }

    /// Record that the observation was successful. Timing of observation is
    /// handled automatically.
    pub fn ok(&self) {
        self.ok_with_attributes(&[])
    }

    /// Record that the observation was successful with provided attributes.
    /// Timing of observation is handled automatically.
    pub fn ok_with_attributes(&self, attributes: &[KeyValue]) {
        let duration = self.start.elapsed();
        self.observe(RedRequestStatus::Ok, duration, attributes);
    }

    /// Record that the observation was not successful but was still valid.
    /// `client_error` is the right thing to choose when the request failed perhaps
    /// due to client error. Timing of observation is handled automatically.
    pub fn client_error(&self) {
        self.client_error_with_attributes(&[])
    }

    /// Record with attributes that the observation was not successful but was still
    /// valid. `client_error` is the right thing to choose when the request failed
    /// perhaps due to client error. Timing of observation is handled
    /// automatically.
    pub fn client_error_with_attributes(&self, attributes: &[KeyValue]) {
        let duration = self.start.elapsed();
        self.observe(RedRequestStatus::ClientError, duration, attributes);
    }

    /// Record that the observation was not successful and results in an error
    /// caused by the service under observation. Timing of observation is
    /// handled automatically.
    pub fn error(&self) {
        self.error_with_attributes(&[]);
    }

    /// Record with attributes that the observation was not successful and results
    /// in an error caused by the service under observation. Timing of
    /// observation is handled automatically.
    pub fn error_with_attributes(&self, attributes: &[KeyValue]) {
        let duration = self.start.elapsed();
        self.observe(RedRequestStatus::Error, duration, attributes);
    }
}

/// A Counter is a metric exposing a monotonically increasing counter.
/// It is best used to track increases in something over time.
///
/// If you want to track some notion of success, failure and latency consider
/// using a `REDMetric` instead rather than expressing that with attributes on a
/// `Counter`.
#[derive(Clone)]
pub struct Counter {
    counter: Option<OTCounter<u64>>,
    default_attributes: Vec<KeyValue>,
}

/// Workaround self-recursive OT instruments
/// <https://github.com/open-telemetry/opentelemetry-rust/issues/550>
impl std::fmt::Debug for Counter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Counter")
            .field("default_attributes", &self.default_attributes)
            .finish()
    }
}

impl Counter {
    /// Creates a new Counter that isn't registered with and
    /// consequently won't report to any metrics registry
    pub fn new_unregistered() -> Self {
        Self {
            counter: None,
            default_attributes: vec![],
        }
    }

    pub(crate) fn new(counter: OTCounter<u64>, default_attributes: Vec<KeyValue>) -> Self {
        Self {
            counter: Some(counter),
            default_attributes,
        }
    }

    // Increase the count by `value`.
    pub fn add(&self, value: u64) {
        self.add_with_attributes(value, &[]);
    }

    /// Increase the count by `value` and associate the observation with the
    /// provided attributes.
    pub fn add_with_attributes(&self, value: u64, attributes: &[KeyValue]) {
        let counter = match self.counter.as_ref() {
            Some(counter) => counter,
            None => return,
        };

        let attributes = match attributes.is_empty() {
            // If there are no attributes specified just borrow defaults
            true => Cow::Borrowed(&self.default_attributes),
            false => {
                // Otherwise merge the provided attributes and the defaults.
                // Note: provided attributes need to go last so that they overwrite
                // any default attributes.
                //
                // PERF(edd): this seems expensive to me.
                let mut new_attributes: Vec<KeyValue> = self.default_attributes.clone();
                new_attributes.extend(attributes.iter().cloned());
                Cow::Owned(new_attributes)
            }
        };

        counter.add(value, &attributes);
    }

    // Increase the count by 1.
    pub fn inc(&self) {
        self.add_with_attributes(1, &[]);
    }

    /// Increase the count by 1 and associate the observation with the provided
    /// attributes.
    pub fn inc_with_attributes(&self, attributes: &[KeyValue]) {
        self.add_with_attributes(1, attributes)
    }
}

/// A Histogram is a metric exposing a distribution of observations.
#[derive(Clone)]
pub struct Histogram {
    histogram: Option<OTHistogram<f64>>,
    default_attributes: Vec<KeyValue>,
}

/// Workaround self-recursive OT instruments
/// <https://github.com/open-telemetry/opentelemetry-rust/issues/550>
impl std::fmt::Debug for Histogram {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Histogram")
            .field("default_attributes", &self.default_attributes)
            .finish()
    }
}

impl Histogram {
    /// Creates a new Histogram that isn't registered with and
    /// consequently won't report to any metrics registry
    pub fn new_unregistered() -> Self {
        Self {
            histogram: None,
            default_attributes: vec![],
        }
    }

    pub(crate) fn new(histogram: OTHistogram<f64>, default_attributes: Vec<KeyValue>) -> Self {
        Self {
            histogram: Some(histogram),
            default_attributes,
        }
    }

    /// Add a new observation to the histogram including the provided attributes.
    pub fn observe_with_attributes(&self, observation: f64, attributes: &[KeyValue]) {
        let histogram = match self.histogram.as_ref() {
            Some(histogram) => histogram,
            None => return,
        };

        // merge attributes
        let attributes = if attributes.is_empty() {
            // If there are no attributes specified just borrow defaults
            Cow::Borrowed(&self.default_attributes)
        } else {
            // Otherwise merge the provided attributes and the defaults.
            // Note: provided attributes need to go last so that they overwrite
            // any default attributes.
            //
            // PERF(edd): this seems expensive to me.
            let mut new_attributes: Vec<KeyValue> = self.default_attributes.clone();
            new_attributes.extend_from_slice(attributes);
            Cow::Owned(new_attributes)
        };

        histogram.record(observation, &attributes);
    }

    /// Add a new observation to the histogram
    pub fn observe(&self, observation: f64) {
        self.observe_with_attributes(observation, &[]);
    }

    /// A helper method for observing latencies. Returns a new timing instrument
    /// which will handle submitting an observation containing a duration.
    pub fn timer(&self) -> HistogramTimer<'_> {
        HistogramTimer::new(self)
    }
}
#[derive(Debug)]
pub struct HistogramTimer<'a> {
    start: Instant,
    histogram: &'a Histogram,
}

impl<'a> HistogramTimer<'a> {
    pub fn new(histogram: &'a Histogram) -> Self {
        Self {
            start: Instant::now(),
            histogram,
        }
    }

    pub fn record(self) {
        self.record_with_attributes(&[]);
    }

    pub fn record_with_attributes(self, attributes: &[KeyValue]) {
        self.histogram
            .observe_with_attributes(self.start.elapsed().as_secs_f64(), attributes);
    }
}
