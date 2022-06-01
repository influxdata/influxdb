//! Metrics for [`Loader`].

use std::sync::Arc;

use async_trait::async_trait;
use iox_time::TimeProvider;
use metric::{DurationHistogram, U64Counter};

use super::Loader;

/// Wraps a [`Loader`] and adds metrics.
pub struct MetricsLoader<K, V, Extra>
where
    K: Send + 'static,
    V: Send + 'static,
    Extra: Send + 'static,
{
    inner: Box<dyn Loader<K = K, V = V, Extra = Extra>>,
    time_provider: Arc<dyn TimeProvider>,
    metric_calls: U64Counter,
    metric_duration: DurationHistogram,
}

impl<K, V, Extra> MetricsLoader<K, V, Extra>
where
    K: Send + 'static,
    V: Send + 'static,
    Extra: Send + 'static,
{
    /// Create new wrapper.
    pub fn new(
        inner: Box<dyn Loader<K = K, V = V, Extra = Extra>>,
        name: &'static str,
        time_provider: Arc<dyn TimeProvider>,
        metric_registry: &metric::Registry,
    ) -> Self {
        let metric_calls = metric_registry
            .register_metric::<U64Counter>(
                "cache_load_function_calls",
                "Count how often a cache loader was called.",
            )
            .recorder(&[("name", name)]);
        let metric_duration = metric_registry
            .register_metric::<DurationHistogram>(
                "cache_load_function_duration",
                "Time taken by cache load function calls",
            )
            .recorder(&[("name", name)]);

        Self {
            inner,
            time_provider,
            metric_calls,
            metric_duration,
        }
    }
}

impl<K, V, Extra> std::fmt::Debug for MetricsLoader<K, V, Extra>
where
    K: Send + 'static,
    V: Send + 'static,
    Extra: Send + 'static,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MetricsLoader").finish_non_exhaustive()
    }
}

#[async_trait]
impl<K, V, Extra> Loader for MetricsLoader<K, V, Extra>
where
    K: Send + 'static,
    V: Send + 'static,
    Extra: Send + 'static,
{
    type K = K;
    type V = V;
    type Extra = Extra;

    async fn load(&self, k: Self::K, extra: Self::Extra) -> Self::V {
        self.metric_calls.inc(1);

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
        let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_millis(0)));
        let metric_registry = Arc::new(metric::Registry::new());

        let time_provider_captured = Arc::clone(&time_provider);
        let inner_loader = Box::new(FunctionLoader::new(move |x: u64, _extra: ()| {
            let time_provider_captured = Arc::clone(&time_provider_captured);
            async move {
                time_provider_captured.inc(Duration::from_secs(10));
                x.to_string()
            }
        }));

        let loader = MetricsLoader::new(inner_loader, "my_loader", time_provider, &metric_registry);

        let mut reporter = RawReporter::default();
        metric_registry.report(&mut reporter);
        assert_eq!(
            reporter
                .metric("cache_load_function_calls")
                .unwrap()
                .observation(&[("name", "my_loader")])
                .unwrap(),
            &Observation::U64Counter(0)
        );
        if let Observation::DurationHistogram(hist) = reporter
            .metric("cache_load_function_duration")
            .unwrap()
            .observation(&[("name", "my_loader")])
            .unwrap()
        {
            assert_eq!(hist.total, Duration::from_secs(0));
        } else {
            panic!("Wrong observation type");
        }

        assert_eq!(loader.load(42, ()).await, String::from("42"));

        let mut reporter = RawReporter::default();
        metric_registry.report(&mut reporter);
        assert_eq!(
            reporter
                .metric("cache_load_function_calls")
                .unwrap()
                .observation(&[("name", "my_loader")])
                .unwrap(),
            &Observation::U64Counter(1)
        );
        if let Observation::DurationHistogram(hist) = reporter
            .metric("cache_load_function_duration")
            .unwrap()
            .observation(&[("name", "my_loader")])
            .unwrap()
        {
            assert_eq!(hist.total, Duration::from_secs(10));
        } else {
            panic!("Wrong observation type");
        }
    }
}
