//! Metrics instrumentation for [`Cache`]s.
use std::{fmt::Debug, sync::Arc};

use async_trait::async_trait;
use iox_time::{Time, TimeProvider};
use metric::{Attributes, DurationHistogram, U64Counter};
use trace::span::{Span, SpanRecorder};

use super::{Cache, CacheGetStatus, CachePeekStatus};

/// Struct containing all the metrics
#[derive(Debug)]
struct Metrics {
    time_provider: Arc<dyn TimeProvider>,
    metric_get_hit: DurationHistogram,
    metric_get_miss: DurationHistogram,
    metric_get_miss_already_loading: DurationHistogram,
    metric_get_cancelled: DurationHistogram,
    metric_peek_hit: DurationHistogram,
    metric_peek_miss: DurationHistogram,
    metric_peek_miss_already_loading: DurationHistogram,
    metric_peek_cancelled: DurationHistogram,
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

        let mut attributes_peek = attributes.clone();
        let metric_peek = metric_registry
            .register_metric::<DurationHistogram>("iox_cache_peek", "Cache PEEK requests");

        attributes_peek.insert("status", "hit");
        let metric_peek_hit = metric_peek.recorder(attributes_peek.clone());

        attributes_peek.insert("status", "miss");
        let metric_peek_miss = metric_peek.recorder(attributes_peek.clone());

        attributes_peek.insert("status", "miss_already_loading");
        let metric_peek_miss_already_loading = metric_peek.recorder(attributes_peek.clone());

        attributes_peek.insert("status", "cancelled");
        let metric_peek_cancelled = metric_peek.recorder(attributes_peek);

        let metric_set = metric_registry
            .register_metric::<U64Counter>("iox_cache_set", "Cache SET requests.")
            .recorder(attributes);

        Self {
            time_provider,
            metric_get_hit,
            metric_get_miss,
            metric_get_miss_already_loading,
            metric_get_cancelled,
            metric_peek_hit,
            metric_peek_miss,
            metric_peek_miss_already_loading,
            metric_peek_cancelled,
            metric_set,
        }
    }
}

/// Wraps given cache with metrics.
#[derive(Debug)]
pub struct CacheWithMetrics<C>
where
    C: Cache,
{
    inner: C,
    metrics: Metrics,
}

impl<C> CacheWithMetrics<C>
where
    C: Cache,
{
    /// Create new metrics wrapper around given cache.
    pub fn new(
        inner: C,
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
impl<C> Cache for CacheWithMetrics<C>
where
    C: Cache,
{
    type K = C::K;
    type V = C::V;
    type GetExtra = (C::GetExtra, Option<Span>);
    type PeekExtra = (C::PeekExtra, Option<Span>);

    async fn get_with_status(
        &self,
        k: Self::K,
        extra: Self::GetExtra,
    ) -> (Self::V, CacheGetStatus) {
        let (extra, span) = extra;
        let mut set_on_drop = SetGetMetricOnDrop::new(&self.metrics, span);
        let (v, status) = self.inner.get_with_status(k, extra).await;
        set_on_drop.status = Some(status);

        (v, status)
    }

    async fn peek_with_status(
        &self,
        k: Self::K,
        extra: Self::PeekExtra,
    ) -> Option<(Self::V, CachePeekStatus)> {
        let (extra, span) = extra;
        let mut set_on_drop = SetPeekMetricOnDrop::new(&self.metrics, span);
        let res = self.inner.peek_with_status(k, extra).await;
        set_on_drop.status = Some(res.as_ref().map(|(_v, status)| *status));

        res
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
    span_recorder: SpanRecorder,
}

impl<'a> SetGetMetricOnDrop<'a> {
    fn new(metrics: &'a Metrics, span: Option<Span>) -> Self {
        let t_start = metrics.time_provider.now();

        Self {
            metrics,
            t_start,
            status: None,
            span_recorder: SpanRecorder::new(span),
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

        if let Some(status) = self.status {
            self.span_recorder.ok(status.name());
        }
    }
}

/// Helper that set's PEEK metrics on drop depending on the `status`.
///
/// A drop might happen due to completion (in which case the `status` should be set) or if the future is cancelled (in
/// which case the `status` is `None`).
struct SetPeekMetricOnDrop<'a> {
    metrics: &'a Metrics,
    t_start: Time,
    status: Option<Option<CachePeekStatus>>,
    span_recorder: SpanRecorder,
}

impl<'a> SetPeekMetricOnDrop<'a> {
    fn new(metrics: &'a Metrics, span: Option<Span>) -> Self {
        let t_start = metrics.time_provider.now();

        Self {
            metrics,
            t_start,
            status: None,
            span_recorder: SpanRecorder::new(span),
        }
    }
}

impl<'a> Drop for SetPeekMetricOnDrop<'a> {
    fn drop(&mut self) {
        let t_end = self.metrics.time_provider.now();

        match self.status {
            Some(Some(CachePeekStatus::Hit)) => &self.metrics.metric_peek_hit,
            Some(Some(CachePeekStatus::MissAlreadyLoading)) => {
                &self.metrics.metric_peek_miss_already_loading
            }
            Some(None) => &self.metrics.metric_peek_miss,
            None => &self.metrics.metric_peek_cancelled,
        }
        .record(t_end - self.t_start);

        if let Some(status) = self.status {
            self.span_recorder
                .ok(status.map(|status| status.name()).unwrap_or("miss"));
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, time::Duration};

    use futures::{stream::FuturesUnordered, StreamExt};
    use iox_time::{MockProvider, Time};
    use metric::{HistogramObservation, Observation, RawReporter};
    use tokio::sync::Barrier;
    use trace::{span::SpanStatus, RingBufferTraceCollector};

    use crate::cache::{
        driver::CacheDriver,
        test_util::{run_test_generic, AbortAndWaitExt, EnsurePendingExt, TestAdapter, TestLoader},
    };

    use super::*;

    #[tokio::test]
    async fn test_generic() {
        run_test_generic(MyTestAdapter).await;
    }

    struct MyTestAdapter;

    impl TestAdapter for MyTestAdapter {
        type GetExtra = (bool, Option<Span>);
        type PeekExtra = ((), Option<Span>);
        type Cache = CacheWithMetrics<CacheDriver<HashMap<u8, String>, TestLoader>>;

        fn construct(&self, loader: Arc<TestLoader>) -> Arc<Self::Cache> {
            TestMetricsCache::new_with_loader(loader).cache
        }

        fn get_extra(&self, inner: bool) -> Self::GetExtra {
            (inner, None)
        }

        fn peek_extra(&self) -> Self::PeekExtra {
            ((), None)
        }
    }

    #[tokio::test]
    async fn test_get() {
        let test_cache = TestMetricsCache::new();

        let traces = Arc::new(RingBufferTraceCollector::new(1_000));

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
        let traces_captured = Arc::clone(&traces);
        let cache_captured = Arc::clone(&test_cache.cache);
        let join_handle_1 = tokio::task::spawn(async move {
            cache_captured
                .get(
                    1,
                    (
                        true,
                        Some(Span::root("miss", Arc::clone(&traces_captured) as _)),
                    ),
                )
                .ensure_pending(barrier_pending_1_captured)
                .await
        });

        barrier_pending_1.wait().await;
        let d1 = Duration::from_secs(1);
        test_cache.time_provider.inc(d1);
        let barrier_pending_2 = Arc::new(Barrier::new(2));
        let barrier_pending_2_captured = Arc::clone(&barrier_pending_2);
        let traces_captured = Arc::clone(&traces);
        let cache_captured = Arc::clone(&test_cache.cache);
        let n_miss_already_loading = 10;
        let join_handle_2 = tokio::task::spawn(async move {
            (0..n_miss_already_loading)
                .map(|_| {
                    cache_captured.get(
                        1,
                        (
                            true,
                            Some(Span::root(
                                "miss_already_loading",
                                Arc::clone(&traces_captured) as _,
                            )),
                        ),
                    )
                })
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
            test_cache
                .cache
                .get(1, (true, Some(Span::root("hit", Arc::clone(&traces) as _))))
                .await;
        }

        let n_cancelled = 200;
        let barrier_pending_3 = Arc::new(Barrier::new(2));
        let barrier_pending_3_captured = Arc::clone(&barrier_pending_3);
        let traces_captured = Arc::clone(&traces);
        let cache_captured = Arc::clone(&test_cache.cache);
        let join_handle_3 = tokio::task::spawn(async move {
            (0..n_cancelled)
                .map(|_| {
                    cache_captured.get(
                        2,
                        (
                            true,
                            Some(Span::root("cancelled", Arc::clone(&traces_captured) as _)),
                        ),
                    )
                })
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

        // check spans
        assert_n_spans(&traces, "hit", SpanStatus::Ok, n_hit as usize);
        assert_n_spans(&traces, "miss", SpanStatus::Ok, 1);
        assert_n_spans(
            &traces,
            "miss_already_loading",
            SpanStatus::Ok,
            n_miss_already_loading as usize,
        );
        assert_n_spans(
            &traces,
            "cancelled",
            SpanStatus::Unknown,
            n_cancelled as usize,
        );
    }

    #[tokio::test]
    async fn test_peek() {
        let test_cache = TestMetricsCache::new();

        let traces = Arc::new(RingBufferTraceCollector::new(1_000));

        let mut reporter = RawReporter::default();
        test_cache.metric_registry.report(&mut reporter);

        for status in ["hit", "miss", "miss_already_loading", "cancelled"] {
            let hist = get_metric_cache_peek(&reporter, status);
            assert_eq!(hist.sample_count(), 0);
            assert_eq!(hist.total, Duration::from_secs(0));
        }

        test_cache.loader.block();

        test_cache
            .cache
            .peek(1, ((), Some(Span::root("miss", Arc::clone(&traces) as _))))
            .await;

        let barrier_pending_1 = Arc::new(Barrier::new(2));
        let barrier_pending_1_captured = Arc::clone(&barrier_pending_1);
        let cache_captured = Arc::clone(&test_cache.cache);
        let join_handle_1 = tokio::task::spawn(async move {
            cache_captured
                .get(1, (true, None))
                .ensure_pending(barrier_pending_1_captured)
                .await
        });

        barrier_pending_1.wait().await;
        let d1 = Duration::from_secs(1);
        test_cache.time_provider.inc(d1);
        let barrier_pending_2 = Arc::new(Barrier::new(2));
        let barrier_pending_2_captured = Arc::clone(&barrier_pending_2);
        let traces_captured = Arc::clone(&traces);
        let cache_captured = Arc::clone(&test_cache.cache);
        let n_miss_already_loading = 10;
        let join_handle_2 = tokio::task::spawn(async move {
            (0..n_miss_already_loading)
                .map(|_| {
                    cache_captured.peek(
                        1,
                        (
                            (),
                            Some(Span::root(
                                "miss_already_loading",
                                Arc::clone(&traces_captured) as _,
                            )),
                        ),
                    )
                })
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
            test_cache
                .cache
                .peek(1, ((), Some(Span::root("hit", Arc::clone(&traces) as _))))
                .await;
        }

        let n_cancelled = 200;
        let barrier_pending_3 = Arc::new(Barrier::new(2));
        let barrier_pending_3_captured = Arc::clone(&barrier_pending_3);
        let cache_captured = Arc::clone(&test_cache.cache);
        tokio::task::spawn(async move {
            cache_captured
                .get(2, (true, None))
                .ensure_pending(barrier_pending_3_captured)
                .await
        });
        barrier_pending_3.wait().await;
        let barrier_pending_4 = Arc::new(Barrier::new(2));
        let barrier_pending_4_captured = Arc::clone(&barrier_pending_4);
        let traces_captured = Arc::clone(&traces);
        let cache_captured = Arc::clone(&test_cache.cache);
        let join_handle_3 = tokio::task::spawn(async move {
            (0..n_cancelled)
                .map(|_| {
                    cache_captured.peek(
                        2,
                        (
                            (),
                            Some(Span::root("cancelled", Arc::clone(&traces_captured) as _)),
                        ),
                    )
                })
                .collect::<FuturesUnordered<_>>()
                .collect::<Vec<_>>()
                .ensure_pending(barrier_pending_4_captured)
                .await
        });

        barrier_pending_4.wait().await;
        let d3 = Duration::from_secs(20);
        test_cache.time_provider.inc(d3);
        join_handle_3.abort_and_wait().await;

        let mut reporter = RawReporter::default();
        test_cache.metric_registry.report(&mut reporter);

        let hist = get_metric_cache_peek(&reporter, "hit");
        assert_eq!(hist.sample_count(), n_hit);
        // "hit"s are instant because there's no lock contention
        assert_eq!(hist.total, Duration::from_secs(0));

        let hist = get_metric_cache_peek(&reporter, "miss");
        let n = 1;
        assert_eq!(hist.sample_count(), n);
        // "miss"es are instant
        assert_eq!(hist.total, Duration::from_secs(0));

        let hist = get_metric_cache_peek(&reporter, "miss_already_loading");
        assert_eq!(hist.sample_count(), n_miss_already_loading);
        assert_eq!(hist.total, (n_miss_already_loading as u32) * d2);

        let hist = get_metric_cache_peek(&reporter, "cancelled");
        assert_eq!(hist.sample_count(), n_cancelled);
        assert_eq!(hist.total, (n_cancelled as u32) * d3);

        // check spans
        assert_n_spans(&traces, "hit", SpanStatus::Ok, n_hit as usize);
        assert_n_spans(&traces, "miss", SpanStatus::Ok, 1);
        assert_n_spans(
            &traces,
            "miss_already_loading",
            SpanStatus::Ok,
            n_miss_already_loading as usize,
        );
        assert_n_spans(
            &traces,
            "cancelled",
            SpanStatus::Unknown,
            n_cancelled as usize,
        );
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
        cache: Arc<CacheWithMetrics<CacheDriver<HashMap<u8, String>, TestLoader>>>,
    }

    impl TestMetricsCache {
        fn new() -> Self {
            Self::new_with_loader(Arc::new(TestLoader::default()))
        }

        fn new_with_loader(loader: Arc<TestLoader>) -> Self {
            let inner = CacheDriver::new(Arc::clone(&loader) as _, HashMap::new());
            let time_provider =
                Arc::new(MockProvider::new(Time::from_timestamp_millis(0).unwrap()));
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

    fn get_metric_cache_peek(
        reporter: &RawReporter,
        status: &'static str,
    ) -> HistogramObservation<Duration> {
        if let Observation::DurationHistogram(hist) = reporter
            .metric("iox_cache_peek")
            .unwrap()
            .observation(&[("name", "test"), ("status", status)])
            .unwrap()
        {
            hist.clone()
        } else {
            panic!("Wrong observation type");
        }
    }

    fn assert_n_spans(
        traces: &RingBufferTraceCollector,
        name: &'static str,
        status: SpanStatus,
        expected: usize,
    ) {
        let actual = traces
            .spans()
            .into_iter()
            .filter(|span| (span.name == name) && (span.status == status))
            .count();
        assert_eq!(actual, expected);
    }
}
