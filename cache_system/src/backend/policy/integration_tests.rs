//! Test integration between different policies.

use std::{collections::HashMap, sync::Arc, time::Duration};

use iox_time::{MockProvider, Time};
use parking_lot::Mutex;
use tokio::{runtime::Handle, sync::Notify};

use crate::{
    backend::{policy::refresh::test_util::NotifyExt, CacheBackend},
    resource_consumption::ResourceEstimator,
};

use super::{
    lru::{test_util::TestSize, LruPolicy, ResourcePool},
    refresh::{
        test_util::{TestLoader, TestRefreshDurationProvider},
        RefreshPolicy,
    },
    ttl::{test_util::TestTtlProvider, TtlPolicy},
    PolicyBackend,
};

#[tokio::test]
async fn test_refresh_can_prevent_expiration() {
    let TestStateTtlAndRefresh {
        mut backend,
        refresh_duration_provider,
        ttl_provider,
        time_provider,
        loader,
        notify_idle,
        ..
    } = TestStateTtlAndRefresh::new();

    loader.mock_next(1, String::from("foo"));
    refresh_duration_provider.set_refresh_in(1, String::from("a"), Some(Duration::from_secs(1)));
    ttl_provider.set_expires_in(1, String::from("a"), Some(Duration::from_secs(2)));
    refresh_duration_provider.set_refresh_in(1, String::from("foo"), None);
    ttl_provider.set_expires_in(1, String::from("foo"), Some(Duration::from_secs(2)));
    backend.set(1, String::from("a"));

    // perform refresh
    time_provider.inc(Duration::from_secs(1));
    assert_eq!(backend.get(&1), Some(String::from("a")));
    notify_idle.notified_with_timeout().await;
    assert_eq!(backend.get(&1), Some(String::from("foo")));

    // no expired because refresh resets the timer
    time_provider.inc(Duration::from_secs(1));
    assert_eq!(backend.get(&1), Some(String::from("foo")));

    // we don't request a 2nd refresh (refresh duration is None), so this finally expires
    time_provider.inc(Duration::from_secs(1));
    assert_eq!(backend.get(&1), None);
}

#[tokio::test]
async fn test_refresh_sets_new_expiration_after_it_finishes() {
    let TestStateTtlAndRefresh {
        mut backend,
        refresh_duration_provider,
        ttl_provider,
        time_provider,
        loader,
        notify_idle,
        ..
    } = TestStateTtlAndRefresh::new();

    let barrier = loader.block_next(1, String::from("foo"));
    refresh_duration_provider.set_refresh_in(1, String::from("a"), Some(Duration::from_secs(1)));
    ttl_provider.set_expires_in(1, String::from("a"), Some(Duration::from_secs(3)));
    refresh_duration_provider.set_refresh_in(1, String::from("foo"), None);
    ttl_provider.set_expires_in(1, String::from("foo"), Some(Duration::from_secs(3)));
    backend.set(1, String::from("a"));

    // perform refresh
    time_provider.inc(Duration::from_secs(1));
    assert_eq!(backend.get(&1), Some(String::from("a")));
    notify_idle.notified_with_timeout().await;
    assert_eq!(backend.get(&1), Some(String::from("a")));

    time_provider.inc(Duration::from_secs(1));
    barrier.wait().await;
    notify_idle.notified_with_timeout().await;
    assert_eq!(backend.get(&1), Some(String::from("foo")));

    // no expired because refresh resets the timer after it was ready (now), not when it started (1s ago)
    time_provider.inc(Duration::from_secs(2));
    assert_eq!(backend.get(&1), Some(String::from("foo")));

    // we don't request a 2nd refresh (refresh duration is None), so this finally expires
    time_provider.inc(Duration::from_secs(1));
    assert_eq!(backend.get(&1), None);
}

#[tokio::test]
async fn test_if_refresh_to_slow_then_expire() {
    let TestStateTtlAndRefresh {
        mut backend,
        refresh_duration_provider,
        ttl_provider,
        time_provider,
        loader,
        notify_idle,
        ..
    } = TestStateTtlAndRefresh::new();

    let _barrier = loader.block_next(1, String::from("foo"));
    refresh_duration_provider.set_refresh_in(1, String::from("a"), Some(Duration::from_secs(1)));
    ttl_provider.set_expires_in(1, String::from("a"), Some(Duration::from_secs(2)));
    backend.set(1, String::from("a"));

    // perform refresh
    time_provider.inc(Duration::from_secs(1));
    assert_eq!(backend.get(&1), Some(String::from("a")));

    notify_idle.notified_with_timeout().await;
    assert_eq!(backend.get(&1), Some(String::from("a")));

    time_provider.inc(Duration::from_secs(1));
    notify_idle.not_notified().await;
    assert_eq!(backend.get(&1), None);
}

#[tokio::test]
async fn test_refresh_can_trigger_lru_eviction() {
    let TestStateLRUAndRefresh {
        mut backend,
        refresh_duration_provider,
        loader,
        size_estimator,
        time_provider,
        notify_idle,
        pool,
        ..
    } = TestStateLRUAndRefresh::new();

    assert_eq!(pool.limit(), TestSize(10));

    loader.mock_next(1, String::from("b"));

    refresh_duration_provider.set_refresh_in(1, String::from("a"), Some(Duration::from_secs(1)));
    refresh_duration_provider.set_refresh_in(1, String::from("b"), None);
    refresh_duration_provider.set_refresh_in(2, String::from("c"), None);
    refresh_duration_provider.set_refresh_in(3, String::from("d"), None);

    size_estimator.mock_size(1, String::from("a"), TestSize(1));
    size_estimator.mock_size(1, String::from("b"), TestSize(9));
    size_estimator.mock_size(2, String::from("c"), TestSize(1));
    size_estimator.mock_size(3, String::from("d"), TestSize(1));

    backend.set(1, String::from("a"));
    backend.set(2, String::from("c"));
    backend.set(3, String::from("d"));
    assert_eq!(backend.get(&1), Some(String::from("a")));
    assert_eq!(backend.get(&2), Some(String::from("c")));
    assert_eq!(backend.get(&3), Some(String::from("d")));

    // refresh
    time_provider.inc(Duration::from_secs(1));
    assert_eq!(backend.get(&1), Some(String::from("a")));
    notify_idle.notified_with_timeout().await;

    // needed to evict 2->"c"
    assert_eq!(backend.get(&1), Some(String::from("b")));
    assert_eq!(backend.get(&2), None);
    assert_eq!(backend.get(&3), Some(String::from("d")));
}

#[tokio::test]
async fn test_lru_learns_about_ttl_evictions() {
    let TestStateTtlAndLRU {
        mut backend,
        ttl_provider,
        size_estimator,
        time_provider,
        pool,
        ..
    } = TestStateTtlAndLRU::new().await;

    assert_eq!(pool.limit(), TestSize(10));

    ttl_provider.set_expires_in(1, String::from("a"), Some(Duration::from_secs(1)));
    ttl_provider.set_expires_in(2, String::from("b"), None);
    ttl_provider.set_expires_in(3, String::from("c"), None);

    size_estimator.mock_size(1, String::from("a"), TestSize(4));
    size_estimator.mock_size(2, String::from("b"), TestSize(4));
    size_estimator.mock_size(3, String::from("c"), TestSize(4));

    backend.set(1, String::from("a"));
    backend.set(2, String::from("b"));

    assert_eq!(pool.current(), TestSize(8));

    // evict
    time_provider.inc(Duration::from_secs(1));
    assert_eq!(backend.get(&1), None);

    // now there's space for 3->"c"
    assert_eq!(pool.current(), TestSize(4));
    backend.set(3, String::from("c"));

    assert_eq!(pool.current(), TestSize(8));
    assert_eq!(backend.get(&1), None);
    assert_eq!(backend.get(&2), Some(String::from("b")));
    assert_eq!(backend.get(&3), Some(String::from("c")));
}

/// Test setup that integrates the TTL policy with a refresh.
struct TestStateTtlAndRefresh {
    backend: PolicyBackend<u8, String>,
    ttl_provider: Arc<TestTtlProvider>,
    refresh_duration_provider: Arc<TestRefreshDurationProvider>,
    time_provider: Arc<MockProvider>,
    loader: Arc<TestLoader>,
    notify_idle: Arc<Notify>,
}

impl TestStateTtlAndRefresh {
    fn new() -> Self {
        let refresh_duration_provider = Arc::new(TestRefreshDurationProvider::new());
        let ttl_provider = Arc::new(TestTtlProvider::new());
        let time_provider = Arc::new(MockProvider::new(Time::MIN));
        let metric_registry = metric::Registry::new();
        let loader = Arc::new(TestLoader::default());
        let notify_idle = Arc::new(Notify::new());

        let mut backend = PolicyBackend::new(
            Box::new(HashMap::<u8, String>::new()),
            Arc::clone(&time_provider) as _,
        );
        backend.add_policy(RefreshPolicy::new_inner(
            Arc::clone(&refresh_duration_provider) as _,
            Arc::clone(&loader) as _,
            "my_cache",
            &metric_registry,
            Arc::clone(&notify_idle),
            &Handle::current(),
        ));
        backend.add_policy(TtlPolicy::new(
            Arc::clone(&ttl_provider) as _,
            "my_cache",
            &metric_registry,
        ));

        Self {
            backend,
            refresh_duration_provider,
            ttl_provider,
            time_provider,
            loader,
            notify_idle,
        }
    }
}

/// Test setup that integrates the LRU policy with a refresh.
struct TestStateLRUAndRefresh {
    backend: PolicyBackend<u8, String>,
    size_estimator: Arc<TestSizeEstimator>,
    refresh_duration_provider: Arc<TestRefreshDurationProvider>,
    time_provider: Arc<MockProvider>,
    loader: Arc<TestLoader>,
    pool: Arc<ResourcePool<TestSize>>,
    notify_idle: Arc<Notify>,
}

impl TestStateLRUAndRefresh {
    fn new() -> Self {
        let refresh_duration_provider = Arc::new(TestRefreshDurationProvider::new());
        let size_estimator = Arc::new(TestSizeEstimator::default());
        let time_provider = Arc::new(MockProvider::new(Time::MIN));
        let metric_registry = Arc::new(metric::Registry::new());
        let loader = Arc::new(TestLoader::default());
        let notify_idle = Arc::new(Notify::new());

        let mut backend = PolicyBackend::new(
            Box::new(HashMap::<u8, String>::new()),
            Arc::clone(&time_provider) as _,
        );
        backend.add_policy(RefreshPolicy::new_inner(
            Arc::clone(&refresh_duration_provider) as _,
            Arc::clone(&loader) as _,
            "my_cache",
            &metric_registry,
            Arc::clone(&notify_idle),
            &Handle::current(),
        ));
        let pool = Arc::new(ResourcePool::new(
            "my_pool",
            TestSize(10),
            Arc::clone(&metric_registry),
        ));
        backend.add_policy(LruPolicy::new(
            Arc::clone(&pool),
            "my_cache",
            Arc::clone(&size_estimator) as _,
        ));

        Self {
            backend,
            refresh_duration_provider,
            size_estimator,
            time_provider,
            loader,
            pool,
            notify_idle,
        }
    }
}

/// Test setup that integrates the TTL policy with LRU.
struct TestStateTtlAndLRU {
    backend: PolicyBackend<u8, String>,
    ttl_provider: Arc<TestTtlProvider>,
    time_provider: Arc<MockProvider>,
    size_estimator: Arc<TestSizeEstimator>,
    pool: Arc<ResourcePool<TestSize>>,
}

impl TestStateTtlAndLRU {
    async fn new() -> Self {
        let ttl_provider = Arc::new(TestTtlProvider::new());
        let time_provider = Arc::new(MockProvider::new(Time::MIN));
        let metric_registry = Arc::new(metric::Registry::new());
        let size_estimator = Arc::new(TestSizeEstimator::default());

        let mut backend = PolicyBackend::new(
            Box::new(HashMap::<u8, String>::new()),
            Arc::clone(&time_provider) as _,
        );
        backend.add_policy(TtlPolicy::new(
            Arc::clone(&ttl_provider) as _,
            "my_cache",
            &metric_registry,
        ));
        let pool = Arc::new(ResourcePool::new(
            "my_pool",
            TestSize(10),
            Arc::clone(&metric_registry),
        ));
        backend.add_policy(LruPolicy::new(
            Arc::clone(&pool),
            "my_cache",
            Arc::clone(&size_estimator) as _,
        ));

        Self {
            backend,
            ttl_provider,
            time_provider,
            size_estimator,
            pool,
        }
    }
}

#[derive(Debug, Default)]
struct TestSizeEstimator {
    sizes: Mutex<HashMap<(u8, String), TestSize>>,
}

impl TestSizeEstimator {
    fn mock_size(&self, k: u8, v: String, s: TestSize) {
        self.sizes.lock().insert((k, v), s);
    }
}

impl ResourceEstimator for TestSizeEstimator {
    type K = u8;
    type V = String;
    type S = TestSize;

    fn consumption(&self, k: &Self::K, v: &Self::V) -> Self::S {
        *self.sizes.lock().get(&(*k, v.clone())).unwrap()
    }
}
