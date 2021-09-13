use std::time::Duration;

use crate::{
    HistogramObservation, MakeMetricObserver, MetricKind, MetricObserver, Observation,
    ObservationBucket, U64Counter, U64Gauge, U64Histogram,
};

use std::convert::TryInto;

/// The maximum duration that can be stored in the duration measurements
pub const DURATION_MAX: Duration = Duration::from_nanos(u64::MAX);

/// A monotonic counter of `std::time::Duration`
#[derive(Debug, Clone, Default)]
pub struct DurationCounter {
    inner: U64Counter,
}

impl DurationCounter {
    pub fn inc(&self, duration: Duration) {
        self.inner.inc(
            duration
                .as_nanos()
                .try_into()
                .expect("cannot fit duration into u64"),
        )
    }

    pub fn fetch(&self) -> Duration {
        Duration::from_nanos(self.inner.fetch())
    }
}

impl MetricObserver for DurationCounter {
    type Recorder = Self;

    fn kind() -> MetricKind {
        MetricKind::DurationCounter
    }

    fn recorder(&self) -> Self::Recorder {
        self.clone()
    }

    fn observe(&self) -> Observation {
        Observation::DurationCounter(self.fetch())
    }
}

/// An observation of a single `std::time::Duration`
///
/// NOTE: If the same `DurationGauge` is used in multiple locations, e.g. a non-unique set
/// of attributes is provided to `Metric<DurationGauge>::recorder`, the reported value
/// will oscillate between those reported by the separate locations
#[derive(Debug, Clone, Default)]
pub struct DurationGauge {
    inner: U64Gauge,
}

impl DurationGauge {
    pub fn set(&self, value: Duration) {
        self.inner.set(
            value
                .as_nanos()
                .try_into()
                .expect("cannot fit duration into u64"),
        )
    }

    pub fn fetch(&self) -> Duration {
        Duration::from_nanos(self.inner.fetch())
    }
}

impl MetricObserver for DurationGauge {
    type Recorder = Self;

    fn kind() -> MetricKind {
        MetricKind::DurationGauge
    }

    fn recorder(&self) -> Self::Recorder {
        self.clone()
    }

    fn observe(&self) -> Observation {
        Observation::DurationGauge(self.fetch())
    }
}

/// A `DurationHistogram` provides bucketed observations of `Durations`
///
/// This provides insight into the distribution beyond a simple count or total
#[derive(Debug, Clone)]
pub struct DurationHistogram {
    inner: U64Histogram,
}

impl DurationHistogram {
    pub fn fetch(&self) -> HistogramObservation<Duration> {
        let inner = self.inner.fetch();

        HistogramObservation {
            total: Duration::from_nanos(inner.total),
            buckets: inner
                .buckets
                .into_iter()
                .map(|bucket| ObservationBucket {
                    le: Duration::from_nanos(bucket.le),
                    count: bucket.count,
                })
                .collect(),
        }
    }

    pub fn record(&self, value: Duration) {
        self.record_multiple(value, 1)
    }

    pub fn record_multiple(&self, value: Duration, count: u64) {
        self.inner.record_multiple(
            value
                .as_nanos()
                .try_into()
                .expect("cannot fit duration into u64"),
            count,
        )
    }
}

/// `DurationHistogramOptions` allows configuring the buckets used by `DurationHistogram`
#[derive(Debug, Clone)]
pub struct DurationHistogramOptions {
    buckets: Vec<Duration>,
}

impl DurationHistogramOptions {
    /// Create a new `DurationHistogramOptions` with a list of thresholds to delimit the buckets
    pub fn new(thresholds: impl IntoIterator<Item = Duration>) -> Self {
        let mut buckets: Vec<_> = thresholds.into_iter().collect();
        buckets.sort_unstable();
        Self { buckets }
    }
}

impl Default for DurationHistogramOptions {
    fn default() -> Self {
        Self {
            buckets: vec![
                Duration::from_millis(5),
                Duration::from_millis(10),
                Duration::from_millis(25),
                Duration::from_millis(50),
                Duration::from_millis(100),
                Duration::from_millis(250),
                Duration::from_millis(500),
                Duration::from_millis(1000),
                Duration::from_millis(2500),
                Duration::from_millis(5000),
                Duration::from_millis(10000),
                DURATION_MAX,
            ],
        }
    }
}

impl MakeMetricObserver for DurationHistogram {
    type Options = DurationHistogramOptions;

    fn create(options: &DurationHistogramOptions) -> Self {
        Self {
            inner: U64Histogram::new(options.buckets.iter().map(|duration| {
                duration
                    .as_nanos()
                    .try_into()
                    .expect("cannot fit duration into u64")
            })),
        }
    }
}

impl MetricObserver for DurationHistogram {
    type Recorder = Self;

    fn kind() -> MetricKind {
        MetricKind::DurationHistogram
    }

    fn recorder(&self) -> Self::Recorder {
        self.clone()
    }

    fn observe(&self) -> Observation {
        Observation::DurationHistogram(self.fetch())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_gauge() {
        let gauge = DurationGauge::default();

        assert_eq!(
            gauge.observe(),
            Observation::DurationGauge(Duration::from_nanos(0))
        );

        gauge.set(Duration::from_nanos(10002));
        assert_eq!(
            gauge.observe(),
            Observation::DurationGauge(Duration::from_nanos(10002))
        );

        gauge.set(Duration::from_nanos(12));
        assert_eq!(
            gauge.observe(),
            Observation::DurationGauge(Duration::from_nanos(12))
        );

        let r2 = gauge.recorder();

        gauge.set(Duration::from_secs(12));
        assert_eq!(
            gauge.observe(),
            Observation::DurationGauge(Duration::from_secs(12))
        );

        std::mem::drop(r2);

        assert_eq!(
            gauge.observe(),
            Observation::DurationGauge(Duration::from_secs(12))
        );
    }

    #[test]
    fn test_counter() {
        let counter = DurationCounter::default();
        assert_eq!(counter.fetch(), Duration::from_nanos(0));
        counter.inc(Duration::from_nanos(120));
        assert_eq!(counter.fetch(), Duration::from_nanos(120));
        counter.inc(Duration::from_secs(1));
        assert_eq!(counter.fetch(), Duration::from_nanos(1_000_000_120));

        assert_eq!(
            counter.observe(),
            Observation::DurationCounter(Duration::from_nanos(1_000_000_120))
        )
    }

    #[test]
    #[should_panic(expected = "cannot fit duration into u64: TryFromIntError(())")]
    fn test_bucket_overflow() {
        let options = DurationHistogramOptions::new([Duration::MAX]);
        DurationHistogram::create(&options);
    }

    #[test]
    #[should_panic(expected = "cannot fit duration into u64: TryFromIntError(())")]
    fn test_record_overflow() {
        let histogram = DurationHistogram::create(&Default::default());
        histogram.record(Duration::MAX);
    }

    #[test]
    fn test_histogram() {
        let buckets = [
            Duration::from_millis(10),
            Duration::from_millis(15),
            Duration::from_millis(100),
            DURATION_MAX,
        ];

        let options = DurationHistogramOptions::new(buckets);
        let histogram = DurationHistogram::create(&options);

        let buckets = |expected: &[u64; 4], total| -> Observation {
            Observation::DurationHistogram(HistogramObservation {
                total,
                buckets: expected
                    .iter()
                    .cloned()
                    .zip(buckets)
                    .map(|(count, le)| ObservationBucket { le, count })
                    .collect(),
            })
        };

        assert_eq!(
            histogram.observe(),
            buckets(&[0, 0, 0, 0], Duration::from_millis(0))
        );

        histogram.record(Duration::from_millis(20));
        assert_eq!(
            histogram.observe(),
            buckets(&[0, 0, 1, 0], Duration::from_millis(20))
        );

        histogram.record(Duration::from_millis(0));
        assert_eq!(
            histogram.observe(),
            buckets(&[1, 0, 1, 0], Duration::from_millis(20))
        );

        histogram.record(DURATION_MAX);

        // Expect total to overflow and wrap around
        assert_eq!(
            histogram.observe(),
            buckets(
                &[1, 0, 1, 1],
                Duration::from_millis(20) - Duration::from_nanos(1)
            )
        );
    }
}
