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

    pub fn reset(&self) {
        let mut state = self.shared.lock();
        for bucket in &mut state.buckets {
            bucket.count = 0;
        }
        state.total = 0;
    }

    /// percentile returns the bucket threshold for the given percentile.
    /// For example, if you want the median value, percentile(50) will return the 'le' threshold
    /// for the histogram bucket that contains the median sample.
    ///
    /// A use case for for this function is:
    ///     Use a histogram tracks the load placed on a system.
    ///     Set the buckets so they represent load levels of idle/low/medium/high/overloaded.
    ///     Then use percentile to determine how much of the time is spent at various load levels.
    ///  e.g. if percentile(50) comes come back with the low load threshold, the median load on the system is low
    pub fn percentile(&self, percentile: u64) -> u64 {
        let state = self.shared.lock();

        // we need the total quantity of samples, not the sum of samples.
        let total: u64 = state.buckets.iter().map(|bucket| bucket.count).sum();

        let target = total * percentile / 100;

        let mut sum = 0;
        for bucket in &state.buckets {
            sum += bucket.count;
            if sum >= target {
                return bucket.le;
            }
        }
        0
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

/// A concise helper to assert the value of a metric histogram, regardless of underlying type.
#[macro_export]
macro_rules! assert_histogram {
    (
        $metrics:ident,
        $hist:ty,
        $name:expr,
        $(labels = $attr:expr,)*
        $(samples = $samples:expr,)*
        $(sum = $sum:expr,)*
    ) => {
        // Default to an empty set of attributes if not specified.
        #[allow(unused)]
        let mut attr = None;
        $(attr = Some($attr);)*
        let attr = attr.unwrap_or_else(|| metric::Attributes::from(&[]));

        let hist = $metrics
            .get_instrument::<metric::Metric<$hist>>($name)
            .expect("failed to find metric with provided name")
            .get_observer(&attr)
            .expect("failed to find metric with provided attributes")
            .fetch();

        $(assert_eq!(hist.sample_count(), $samples, "sample count mismatch");)*
        $(assert_eq!(hist.total, $sum, "sum value mismatch");)*
    };
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

        // Now test the percentile reporting function
        let options = U64HistogramOptions::new(vec![0, 1, 2, 4, 8, 16, 32, u64::MAX]);
        let histogram = U64Histogram::create(&options);

        histogram.record(0); // bucket 0, le 0
        histogram.record(2); // bucket 2, le 2
        histogram.record(3); // bucket 3, le 4
        histogram.record(3); // bucket 3, le 4
        histogram.record(20); // bucket 6, le 32
        histogram.record(20000); // bucket 7, le u64::MAX
        histogram.record(20000); // bucket 7, le u64::MAX
        histogram.record(20000); // bucket 7, le u64::MAX
        histogram.record(20000); // bucket 7, le u64::MAX
        histogram.record(20000); // bucket 7, le u64::MAX

        // Of the 10 samples above:
        // 1 (10%) is in bucket 0, le 0
        // 1 (10%) is in bucket 2, le 2
        // 2 (20%) are in bucket 3, le 4
        // 1 (10%) is in bucket 6, le 32
        // 5 (50%) are in bucket 7, le u64::MAX

        // request percentiles falling in bucket 0, le 0
        assert_eq!(histogram.percentile(3), 0);
        assert_eq!(histogram.percentile(10), 0);
        assert_eq!(histogram.percentile(19), 0);

        // request percentiles falling in bucket 2, le 2
        assert_eq!(histogram.percentile(20), 2);
        assert_eq!(histogram.percentile(29), 2);

        // requests percentiles falling in bucket 3, le 4
        assert_eq!(histogram.percentile(30), 4);
        assert_eq!(histogram.percentile(49), 4);

        // requests percentiles falling in bucket 6, le 32
        assert_eq!(histogram.percentile(50), 32);
        assert_eq!(histogram.percentile(59), 32);

        // requests percentiles falling in bucket 6, le 32
        assert_eq!(histogram.percentile(60), u64::MAX);
        assert_eq!(histogram.percentile(80), u64::MAX);
        assert_eq!(histogram.percentile(100), u64::MAX);

        // test reset
        histogram.reset();
        assert_eq!(histogram.percentile(100), 0);
        histogram.record(1); // bucket 1, le 1
        histogram.record(2); // bucket 2, le 2
        histogram.record(3); // bucket 3, le 4
        histogram.record(3); // bucket 3, le 4
        assert_eq!(histogram.percentile(0), 0);
        assert_eq!(histogram.percentile(25), 1);
        assert_eq!(histogram.percentile(49), 1);
        assert_eq!(histogram.percentile(50), 2);
    }
}
