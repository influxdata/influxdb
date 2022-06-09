//! Metrics instrumentation for [`Cache`]s.
use std::{fmt::Debug, hash::Hash, sync::Arc};

use async_trait::async_trait;
use iox_time::{Time, TimeProvider};
use metric::{Attributes, DurationHistogram, U64Counter};

use super::{Cache, CacheGetStatus};

/// Struct containing all the metrics
#[derive(Debug)]
struct Metrics {
    time_provider: Arc<dyn TimeProvider>,
    metric_get_hit: DurationHistogram,
    metric_get_miss: DurationHistogram,
    metric_get_miss_already_loading: DurationHistogram,
    metric_get_cancelled: DurationHistogram,
    metric_set: U64Counter,
}

impl Metrics {
    fn new(
        name: &'static str,
        time_provider: Arc<dyn TimeProvider>,
        metric_registry: &metric::Registry,
    ) -> Self {
        let attributes = Attributes::from(&[("name", name)]);

        let mut attributes_get = attributes.clone();
        let metric_get = metric_registry
            .register_metric::<DurationHistogram>("iox_cache_get", "Cache GET requests");

        attributes_get.insert("status", "hit");
        let metric_get_hit = metric_get.recorder(attributes_get.clone());

        attributes_get.insert("status", "miss");
        let metric_get_miss = metric_get.recorder(attributes_get.clone());

        attributes_get.insert("status", "miss_already_loading");
        let metric_get_miss_already_loading = metric_get.recorder(attributes_get.clone());

        attributes_get.insert("status", "cancelled");
        let metric_get_cancelled = metric_get.recorder(attributes_get);

        let metric_set = metric_registry
            .register_metric::<U64Counter>("iox_cache_set", "Cache SET requests.")
            .recorder(attributes);

        Self {
            time_provider,
            metric_get_hit,
            metric_get_miss,
            metric_get_miss_already_loading,
            metric_get_cancelled,
            metric_set,
        }
    }
}

/// Wraps given cache with metrics.
#[derive(Debug)]
pub struct CacheWithMetrics<K, V, Extra>
where
    K: Clone + Eq + Hash + Debug + Ord + Send + 'static,
    V: Clone + Debug + Send + 'static,
    Extra: Debug + Send + 'static,
{
    inner: Box<dyn Cache<K = K, V = V, Extra = Extra>>,
    metrics: Metrics,
}

impl<K, V, Extra> CacheWithMetrics<K, V, Extra>
where
    K: Clone + Eq + Hash + Debug + Ord + Send + 'static,
    V: Clone + Debug + Send + 'static,
    Extra: Debug + Send + 'static,
{
    /// Create new metrics wrapper around given cache.
    pub fn new(
        inner: Box<dyn Cache<K = K, V = V, Extra = Extra>>,
        name: &'static str,
        time_provider: Arc<dyn TimeProvider>,
        metric_registry: &metric::Registry,
    ) -> Self {
        Self {
            inner,
            metrics: Metrics::new(name, time_provider, metric_registry),
        }
    }
}

#[async_trait]
impl<K, V, Extra> Cache for CacheWithMetrics<K, V, Extra>
where
    K: Clone + Eq + Hash + Debug + Ord + Send + 'static,
    V: Clone + Debug + Send + 'static,
    Extra: Debug + Send + 'static,
{
    type K = K;
    type V = V;
    type Extra = Extra;

    async fn get_with_status(&self, k: Self::K, extra: Self::Extra) -> (Self::V, CacheGetStatus) {
        let mut set_on_drop = SetGetMetricOnDrop::new(&self.metrics);
        let (v, status) = self.inner.get_with_status(k, extra).await;
        set_on_drop.status = Some(status);

        (v, status)
    }

    async fn set(&self, k: Self::K, v: Self::V) {
        self.inner.set(k, v).await;
        self.metrics.metric_set.inc(1);
    }
}

/// Helper that set's GET metrics on drop depending on the `status`.
///
/// A drop might happen due to completion (in which case the `status` should be set) or if the future is cancelled (in
/// which case the `status` is `None`).
struct SetGetMetricOnDrop<'a> {
    metrics: &'a Metrics,
    t_start: Time,
    status: Option<CacheGetStatus>,
}

impl<'a> SetGetMetricOnDrop<'a> {
    fn new(metrics: &'a Metrics) -> Self {
        let t_start = metrics.time_provider.now();

        Self {
            metrics,
            t_start,
            status: None,
        }
    }
}

impl<'a> Drop for SetGetMetricOnDrop<'a> {
    fn drop(&mut self) {
        let t_end = self.metrics.time_provider.now();

        match self.status {
            Some(CacheGetStatus::Hit) => &self.metrics.metric_get_hit,
            Some(CacheGetStatus::Miss) => &self.metrics.metric_get_miss,
            Some(CacheGetStatus::MissAlreadyLoading) => {
                &self.metrics.metric_get_miss_already_loading
            }
            None => &self.metrics.metric_get_cancelled,
        }
        .record(t_end - self.t_start);
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, time::Duration};

    use futures::{stream::FuturesUnordered, StreamExt};
    use iox_time::{MockProvider, Time};
    use metric::{HistogramObservation, Observation, RawReporter};
    use tokio::sync::Barrier;

    use crate::cache::{
        driver::CacheDriver,
        test_util::{AbortAndWaitExt, EnsurePendingExt, TestLoader},
    };

    use super::*;

    #[tokio::test]
    async fn test_generic() {
        use crate::cache::test_util::test_generic;

        test_generic(|loader| TestMetricsCache::new_with_loader(loader).cache).await;
    }

    #[tokio::test]
    async fn test_get() {
        let test_cache = TestMetricsCache::new();

        let mut reporter = RawReporter::default();
        test_cache.metric_registry.report(&mut reporter);

        for status in ["hit", "miss", "miss_already_loading", "cancelled"] {
            let hist = get_metric_cache_get(&reporter, status);
            assert_eq!(hist.sample_count(), 0);
            assert_eq!(hist.total, Duration::from_secs(0));
        }

        test_cache.loader.block();

        let barrier_pending_1 = Arc::new(Barrier::new(2));
        let barrier_pending_1_captured = Arc::clone(&barrier_pending_1);
        let cache_captured = Arc::clone(&test_cache.cache);
        let join_handle_1 = tokio::task::spawn(async move {
            cache_captured
                .get(1, true)
                .ensure_pending(barrier_pending_1_captured)
                .await
        });

        barrier_pending_1.wait().await;
        let d1 = Duration::from_secs(1);
        test_cache.time_provider.inc(d1);
        let barrier_pending_2 = Arc::new(Barrier::new(2));
        let barrier_pending_2_captured = Arc::clone(&barrier_pending_2);
        let cache_captured = Arc::clone(&test_cache.cache);
        let n_miss_already_loading = 10;
        let join_handle_2 = tokio::task::spawn(async move {
            (0..n_miss_already_loading)
                .map(|_| cache_captured.get(1, true))
                .collect::<FuturesUnordered<_>>()
                .collect::<Vec<_>>()
                .ensure_pending(barrier_pending_2_captured)
                .await
        });

        barrier_pending_2.wait().await;
        let d2 = Duration::from_secs(3);
        test_cache.time_provider.inc(d2);
        test_cache.loader.unblock();

        join_handle_1.await.unwrap();
        join_handle_2.await.unwrap();

        test_cache.loader.block();
        test_cache.time_provider.inc(Duration::from_secs(10));
        let n_hit = 100;
        for _ in 0..n_hit {
            test_cache.cache.get(1, true).await;
        }

        let n_cancelled = 200;
        let barrier_pending_3 = Arc::new(Barrier::new(2));
        let barrier_pending_3_captured = Arc::clone(&barrier_pending_3);
        let cache_captured = Arc::clone(&test_cache.cache);
        let join_handle_3 = tokio::task::spawn(async move {
            (0..n_cancelled)
                .map(|_| cache_captured.get(2, true))
                .collect::<FuturesUnordered<_>>()
                .collect::<Vec<_>>()
                .ensure_pending(barrier_pending_3_captured)
                .await
        });

        barrier_pending_3.wait().await;
        let d3 = Duration::from_secs(20);
        test_cache.time_provider.inc(d3);
        join_handle_3.abort_and_wait().await;

        let mut reporter = RawReporter::default();
        test_cache.metric_registry.report(&mut reporter);

        let hist = get_metric_cache_get(&reporter, "hit");
        assert_eq!(hist.sample_count(), n_hit);
        // "hit"s are instant because there's no lock contention
        assert_eq!(hist.total, Duration::from_secs(0));

        let hist = get_metric_cache_get(&reporter, "miss");
        let n = 1;
        assert_eq!(hist.sample_count(), n);
        assert_eq!(hist.total, (n as u32) * (d1 + d2));

        let hist = get_metric_cache_get(&reporter, "miss_already_loading");
        assert_eq!(hist.sample_count(), n_miss_already_loading);
        assert_eq!(hist.total, (n_miss_already_loading as u32) * d2);

        let hist = get_metric_cache_get(&reporter, "cancelled");
        assert_eq!(hist.sample_count(), n_cancelled);
        assert_eq!(hist.total, (n_cancelled as u32) * d3);
    }

    #[tokio::test]
    async fn test_set() {
        let test_cache = TestMetricsCache::new();

        let mut reporter = RawReporter::default();
        test_cache.metric_registry.report(&mut reporter);
        assert_eq!(
            reporter
                .metric("iox_cache_set")
                .unwrap()
                .observation(&[("name", "test")])
                .unwrap(),
            &Observation::U64Counter(0)
        );

        test_cache.cache.set(1, String::from("foo")).await;

        let mut reporter = RawReporter::default();
        test_cache.metric_registry.report(&mut reporter);
        assert_eq!(
            reporter
                .metric("iox_cache_set")
                .unwrap()
                .observation(&[("name", "test")])
                .unwrap(),
            &Observation::U64Counter(1)
        );
    }

    struct TestMetricsCache {
        loader: Arc<TestLoader>,
        time_provider: Arc<MockProvider>,
        metric_registry: metric::Registry,
        cache: Arc<CacheWithMetrics<u8, String, bool>>,
    }

    impl TestMetricsCache {
        fn new() -> Self {
            Self::new_with_loader(Arc::new(TestLoader::default()))
        }

        fn new_with_loader(loader: Arc<TestLoader>) -> Self {
            let inner = Box::new(CacheDriver::new(
                Arc::clone(&loader) as _,
                Box::new(HashMap::new()),
            ));
            let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_millis(0)));
            let metric_registry = metric::Registry::new();
            let cache = Arc::new(CacheWithMetrics::new(
                inner,
                "test",
                Arc::clone(&time_provider) as _,
                &metric_registry,
            ));

            Self {
                loader,
                time_provider,
                metric_registry,
                cache,
            }
        }
    }

    fn get_metric_cache_get(
        reporter: &RawReporter,
        status: &'static str,
    ) -> HistogramObservation<Duration> {
        if let Observation::DurationHistogram(hist) = reporter
            .metric("iox_cache_get")
            .unwrap()
            .observation(&[("name", "test"), ("status", status)])
            .unwrap()
        {
            hist.clone()
        } else {
            panic!("Wrong observation type");
        }
    }
}
