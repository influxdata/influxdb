//! Metrics for [`Loader`].

use std::sync::Arc;

use async_trait::async_trait;
use iox_time::TimeProvider;
use metric::{DurationHistogram, U64Counter};
use parking_lot::Mutex;
use pdatastructs::filters::{bloomfilter::BloomFilter, Filter};

use super::Loader;

/// Wraps a [`Loader`] and adds metrics.
pub struct MetricsLoader<L>
where
    L: Loader,
{
    inner: L,
    time_provider: Arc<dyn TimeProvider>,
    metric_calls_new: U64Counter,
    metric_calls_probably_reloaded: U64Counter,
    metric_duration: DurationHistogram,
    seen: Mutex<BloomFilter<L::K>>,
}

impl<L> MetricsLoader<L>
where
    L: Loader,
{
    /// Create new wrapper.
    ///
    /// # Testing
    /// If `testing` is set, the "seen" metrics will NOT be processed correctly because the underlying data structure is
    /// too expensive to create many times a second in an un-optimized debug build.
    pub fn new(
        inner: L,
        name: &'static str,
        time_provider: Arc<dyn TimeProvider>,
        metric_registry: &metric::Registry,
        testing: bool,
    ) -> Self {
        let metric_calls = metric_registry.register_metric::<U64Counter>(
            "cache_load_function_calls",
            "Count how often a cache loader was called.",
        );
        let metric_calls_new = metric_calls.recorder(&[("name", name), ("status", "new")]);
        let metric_calls_probably_reloaded =
            metric_calls.recorder(&[("name", name), ("status", "probably_reloaded")]);
        let metric_duration = metric_registry
            .register_metric::<DurationHistogram>(
                "cache_load_function_duration",
                "Time taken by cache load function calls",
            )
            .recorder(&[("name", name)]);

        let seen = if testing {
            BloomFilter::with_params(1, 1)
        } else {
            // Set up bloom filter for "probably reloaded" test:
            //
            // - input size: we expect 10M elements
            // - reliability: probability of false positives should be <= 1%
            // - CPU efficiency: number of hash functions should be <= 10
            // - RAM efficiency: size should be <= 15MB
            //
            //
            // A bloom filter was chosen here because of the following properties:
            //
            // - memory bound: The storage size is bound even when the set of "probably reloaded" entries approaches
            //   infinite sizes.
            // - memory efficiency: We do not need to store the actual keys.
            // - infallible: Inserting new data into the filter never fails (in contrast to for example a CuckooFilter or
            //   QuotientFilter).
            //
            // The fact that a filter can produce false positives (i.e. it classifies an actual new entry as "probably
            // reloaded") is considered to be OK since the metric is more of an estimate and a guide for cache tuning. We
            // might want to use a more efficient (i.e. more modern) filter design at one point though.
            let seen = BloomFilter::with_properties(10_000_000, 1.0 / 100.0);
            const BOUND_HASH_FUNCTIONS: usize = 10;
            assert!(
                seen.k() <= BOUND_HASH_FUNCTIONS,
                "number of hash functions for bloom filter should be <= {} but is {}",
                BOUND_HASH_FUNCTIONS,
                seen.k(),
            );
            const BOUND_SIZE_BYTES: usize = 15_000_000;
            let size_bytes = (seen.m() + 7) / 8;
            assert!(
                size_bytes <= BOUND_SIZE_BYTES,
                "size of bloom filter should be <= {} bytes but is {} bytes",
                BOUND_SIZE_BYTES,
                size_bytes,
            );

            seen
        };

        Self {
            inner,
            time_provider,
            metric_calls_new,
            metric_calls_probably_reloaded,
            metric_duration,
            seen: Mutex::new(seen),
        }
    }
}

impl<L> std::fmt::Debug for MetricsLoader<L>
where
    L: Loader,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MetricsLoader").finish_non_exhaustive()
    }
}

#[async_trait]
impl<L> Loader for MetricsLoader<L>
where
    L: Loader,
{
    type K = L::K;
    type V = L::V;
    type Extra = L::Extra;

    async fn load(&self, k: Self::K, extra: Self::Extra) -> Self::V {
        {
            let mut seen_guard = self.seen.lock();

            if seen_guard.insert(&k).expect("bloom filter cannot fail") {
                &self.metric_calls_new
            } else {
                &self.metric_calls_probably_reloaded
            }
            .inc(1);
        }

        let t_start = self.time_provider.now();
        let v = self.inner.load(k, extra).await;
        let t_end = self.time_provider.now();

        self.metric_duration.record(t_end - t_start);

        v
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use iox_time::{MockProvider, Time};
    use metric::{Observation, RawReporter};

    use crate::loader::FunctionLoader;

    use super::*;

    #[tokio::test]
    async fn test_metrics() {
        let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_millis(0).unwrap()));
        let metric_registry = Arc::new(metric::Registry::new());

        let time_provider_captured = Arc::clone(&time_provider);
        let d = Duration::from_secs(10);
        let inner_loader = FunctionLoader::new(move |x: u64, _extra: ()| {
            let time_provider_captured = Arc::clone(&time_provider_captured);
            async move {
                time_provider_captured.inc(d);
                x.to_string()
            }
        });

        let loader = MetricsLoader::new(
            inner_loader,
            "my_loader",
            time_provider,
            &metric_registry,
            false,
        );

        let mut reporter = RawReporter::default();
        metric_registry.report(&mut reporter);
        for status in ["new", "probably_reloaded"] {
            assert_eq!(
                reporter
                    .metric("cache_load_function_calls")
                    .unwrap()
                    .observation(&[("name", "my_loader"), ("status", status)])
                    .unwrap(),
                &Observation::U64Counter(0)
            );
        }
        if let Observation::DurationHistogram(hist) = reporter
            .metric("cache_load_function_duration")
            .unwrap()
            .observation(&[("name", "my_loader")])
            .unwrap()
        {
            assert_eq!(hist.sample_count(), 0);
            assert_eq!(hist.total, Duration::from_secs(0));
        } else {
            panic!("Wrong observation type");
        }

        assert_eq!(loader.load(42, ()).await, String::from("42"));
        assert_eq!(loader.load(42, ()).await, String::from("42"));
        assert_eq!(loader.load(1337, ()).await, String::from("1337"));

        let mut reporter = RawReporter::default();
        metric_registry.report(&mut reporter);
        assert_eq!(
            reporter
                .metric("cache_load_function_calls")
                .unwrap()
                .observation(&[("name", "my_loader"), ("status", "new")])
                .unwrap(),
            &Observation::U64Counter(2)
        );
        assert_eq!(
            reporter
                .metric("cache_load_function_calls")
                .unwrap()
                .observation(&[("name", "my_loader"), ("status", "probably_reloaded")])
                .unwrap(),
            &Observation::U64Counter(1)
        );
        if let Observation::DurationHistogram(hist) = reporter
            .metric("cache_load_function_duration")
            .unwrap()
            .observation(&[("name", "my_loader")])
            .unwrap()
        {
            assert_eq!(hist.sample_count(), 3);
            assert_eq!(hist.total, 3 * d);
        } else {
            panic!("Wrong observation type");
        }
    }
}
