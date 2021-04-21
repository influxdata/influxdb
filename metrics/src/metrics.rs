use std::{
    borrow::Cow,
    fmt::Display,
    time::{Duration, Instant},
};

use observability_deps::opentelemetry::metrics::{
    Counter as OTCounter, ValueRecorder as OTHistorgram,
};

pub use observability_deps::opentelemetry::KeyValue;

const RED_REQUEST_STATUS_LABEL: &str = "status";

/// Possible types of RED metric observation status.
///
/// Ok      - an observed request was successful.
/// OkError - an observed request was unsuccessful but it was not the fault of
///           the observed service.
/// Error   - an observed request failed and it was the fault of the service.
///
/// What is the difference between OkError and Error? The difference is to do
/// where the failure occurred. When thinking about measuring SLOs like
/// availability it's necessary to calculate things like:
///
///    Availability = 1 - (failed_requests / all_valid_requests)
///
/// `all_valid_requests` includes successful requests and any requests that
/// failed but not due to the fault of the service (e.g., client errors).
///
/// It is useful to track the components of `all_valid_requests` separately so
/// operators can also monitor external errors (ok_error) errors to help
/// improve their APIs or other systems.
#[derive(Debug)]
pub enum RedRequestStatus {
    Ok,
    OkError,
    Error,
}

impl Display for RedRequestStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Ok => write!(f, "ok"),
            Self::OkError => write!(f, "ok_error"),
            Self::Error => write!(f, "error"),
        }
    }
}

#[derive(Debug)]
/// A REDMetric is a metric that tracks requests to some resource.
///
/// The RED methodology stipulates you should track three key measures:
///
///  - Request Rate: (total number of requests / second);
///  - Error Rate: (total number of failed requests / second);
///  - Duration: (latency distributions for the various requests)
///
/// Using a `REDMetric` makes following this methodology easy because it handles
/// updating the three components for you.
pub struct RedMetric {
    default_labels: Vec<KeyValue>,
    requests: OTCounter<u64>,
    duration: OTHistorgram<f64>,
}

impl RedMetric {
    pub(crate) fn new(
        requests: OTCounter<u64>,
        duration: OTHistorgram<f64>,
        labels: &[KeyValue],
    ) -> Self {
        let mut default_labels = vec![KeyValue::new(
            RED_REQUEST_STATUS_LABEL,
            RedRequestStatus::Ok.to_string(),
        )];

        // TODO(edd): decide what to do if `labels` contains
        // RED_REQUEST_STATUS_LABEL.
        default_labels.extend(labels.iter().cloned());

        Self {
            requests,
            duration,
            default_labels,
        }
    }

    /// Returns a new observation that will handle timing and recording an
    /// observation the metric is tracking.
    pub fn observation(
        &'_ self,
    ) -> RedObservation<impl Fn(RedRequestStatus, Duration, Vec<KeyValue>) + '_> {
        // The recording call-back
        let record =
            move |status: RedRequestStatus, duration: Duration, mut labels: Vec<KeyValue>| {
                let status_idx = labels.len(); // status label will be located at end of labels vec.
                let labels = match labels.is_empty() {
                    // If there are no labels specified just borrow defaults
                    true => Cow::Borrowed(&self.default_labels),
                    false => {
                        // Otherwise merge the provided labels and the defaults.
                        labels.extend(self.default_labels.iter().cloned());
                        Cow::Owned(labels)
                    }
                };

                match status {
                    RedRequestStatus::Ok => {
                        self.requests.add(1, &labels);
                        self.duration.record(duration.as_secs_f64(), &labels);
                    }
                    RedRequestStatus::OkError => {
                        let mut labels = labels.into_owned();
                        labels[status_idx] = KeyValue::new(
                            RED_REQUEST_STATUS_LABEL,
                            RedRequestStatus::OkError.to_string(),
                        );

                        self.requests.add(1, &labels);
                        self.duration.record(duration.as_secs_f64(), &labels);
                    }
                    RedRequestStatus::Error => {
                        let mut labels = labels.into_owned();
                        labels[status_idx] = KeyValue::new(
                            RED_REQUEST_STATUS_LABEL,
                            RedRequestStatus::Error.to_string(),
                        );

                        self.requests.add(1, &labels);
                        self.duration.record(duration.as_secs_f64(), &labels);
                    }
                };
            };

        RedObservation::new(record)
    }
}

#[derive(Debug)]
pub struct RedObservation<T>
where
    T: Fn(RedRequestStatus, Duration, Vec<KeyValue>),
{
    start: Instant,
    record: T, // a call-back that records the observation on the metric.
}

impl<T> RedObservation<T>
where
    T: Fn(RedRequestStatus, Duration, Vec<KeyValue>),
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
        self,
        observation: RedRequestStatus,
        duration: Duration,
        labels_itr: Vec<KeyValue>,
    ) {
        (self.record)(observation, duration, labels_itr);
    }

    /// Record that the observation was successful. Timing of observation is
    /// handled automatically.
    pub fn ok(self) {
        let duration = self.start.elapsed();

        // PERF: allocation here but not convinced worth worrying about yet.
        self.observe(RedRequestStatus::Ok, duration, vec![]);
    }

    /// Record that the observation was not successful but was still valid.
    /// `ok_error` is the right thing to choose when the request failed perhaps
    /// due to client error. Timing of observation is handled automatically.
    pub fn ok_error(self) {
        let duration = self.start.elapsed();

        // PERF: allocation here but not convinced worth worrying about yet.
        self.observe(RedRequestStatus::OkError, duration, vec![]);
    }

    /// Record that the observation was not successful and results in an error
    /// caused by the service under observation. Timing of observation is
    /// handled automatically.
    pub fn error(self) {
        let duration = self.start.elapsed();

        // PERF: allocation here but not convinced worth worrying about yet.
        self.observe(RedRequestStatus::Error, duration, vec![]);
    }
}
