use crate::{
    HistogramObservation, MakeMetricObserver, MetricKind, MetricObserver, Observation,
    ObservationBucket,
};
use parking_lot::Mutex;
use std::sync::Arc;

/// Determines the bucketing used by the `U64Histogram`
#[derive(Debug, Clone)]
pub struct U64HistogramOptions {
    buckets: Vec<u64>,
}

impl U64HistogramOptions {
    /// Create a new `U64HistogramOptions` with a list of thresholds to delimit the buckets
    pub fn new(thresholds: impl IntoIterator<Item = u64>) -> Self {
        let mut buckets: Vec<_> = thresholds.into_iter().collect();
        buckets.sort_unstable();
        Self { buckets }
    }
}

/// A `U64Histogram` provides bucketed observations of u64 values
///
/// This provides insight into the distribution of values beyond a simple count or total
#[derive(Debug, Clone)]
pub struct U64Histogram {
    shared: Arc<Mutex<HistogramObservation<u64>>>,
}

impl U64Histogram {
    pub(crate) fn new(sorted_buckets: impl Iterator<Item = u64>) -> Self {
        let buckets = sorted_buckets
            .map(|le| ObservationBucket {
                le,
                count: Default::default(),
            })
            .collect();

        Self {
            shared: Arc::new(Mutex::new(HistogramObservation {
                total: Default::default(),
                buckets,
            })),
        }
    }

    pub fn fetch(&self) -> HistogramObservation<u64> {
        self.shared.lock().clone()
    }

    pub fn record(&self, value: u64) {
        self.record_multiple(value, 1)
    }

    pub fn record_multiple(&self, value: u64, count: u64) {
        let mut state = self.shared.lock();
        if let Some(bucket) = state
            .buckets
            .iter_mut()
            .find(|bucket| value <= bucket.le)
            .as_mut()
        {
            bucket.count = bucket.count.wrapping_add(count);
            state.total = state.total.wrapping_add(value * count);
        }
    }
}

impl MakeMetricObserver for U64Histogram {
    type Options = U64HistogramOptions;

    fn create(options: &U64HistogramOptions) -> Self {
        Self::new(options.buckets.iter().cloned())
    }
}

impl MetricObserver for U64Histogram {
    type Recorder = Self;

    fn kind() -> MetricKind {
        MetricKind::U64Histogram
    }

    fn recorder(&self) -> Self::Recorder {
        self.clone()
    }

    fn observe(&self) -> Observation {
        Observation::U64Histogram(self.fetch())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::HistogramObservation;

    #[test]
    fn test_histogram() {
        let buckets = [20, 40, 50];
        let options = U64HistogramOptions::new(buckets);
        let histogram = U64Histogram::create(&options);

        let buckets = |expected: &[u64; 3], total: u64| -> Observation {
            Observation::U64Histogram(HistogramObservation {
                total,
                buckets: expected
                    .iter()
                    .cloned()
                    .zip(buckets)
                    .map(|(count, le)| ObservationBucket { le, count })
                    .collect(),
            })
        };

        assert_eq!(histogram.observe(), buckets(&[0, 0, 0], 0));

        histogram.record(30);

        assert_eq!(histogram.observe(), buckets(&[0, 1, 0], 30));

        histogram.record(50);

        assert_eq!(histogram.observe(), buckets(&[0, 1, 1], 80));

        histogram.record(51);

        // Exceeds max bucket - ignored
        assert_eq!(histogram.observe(), buckets(&[0, 1, 1], 80));

        histogram.record(0);
        histogram.record(0);

        assert_eq!(histogram.observe(), buckets(&[2, 1, 1], 80));
    }
}
