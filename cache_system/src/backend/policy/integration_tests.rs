//! Test integration between different policies.

use std::{collections::HashMap, sync::Arc, time::Duration};

use iox_time::{MockProvider, Time};
use parking_lot::Mutex;
use rand::rngs::mock::StepRng;
use test_helpers::maybe_start_logging;
use tokio::{runtime::Handle, sync::Notify};

use crate::{
    backend::{
        policy::refresh::test_util::{backoff_cfg, NotifyExt},
        CacheBackend,
    },
    loader::test_util::TestLoader,
    resource_consumption::{test_util::TestSize, ResourceEstimator},
};

use super::{
    lru::{LruPolicy, ResourcePool},
    refresh::{test_util::TestRefreshDurationProvider, RefreshPolicy},
    remove_if::{RemoveIfHandle, RemoveIfPolicy},
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

    refresh_duration_provider.set_refresh_in(
        1,
        String::from("a"),
        Some(backoff_cfg(Duration::from_secs(1))),
    );
    ttl_provider.set_expires_in(1, String::from("a"), Some(Duration::from_secs(2)));

    refresh_duration_provider.set_refresh_in(1, String::from("foo"), None);
    ttl_provider.set_expires_in(1, String::from("foo"), Some(Duration::from_secs(2)));

    backend.set(1, String::from("a"));

    // perform refresh
    time_provider.inc(Duration::from_secs(1));
    notify_idle.notified_with_timeout().await;

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

    refresh_duration_provider.set_refresh_in(
        1,
        String::from("a"),
        Some(backoff_cfg(Duration::from_secs(1))),
    );
    ttl_provider.set_expires_in(1, String::from("a"), Some(Duration::from_secs(3)));

    refresh_duration_provider.set_refresh_in(1, String::from("foo"), None);
    ttl_provider.set_expires_in(1, String::from("foo"), Some(Duration::from_secs(3)));

    backend.set(1, String::from("a"));

    // perform refresh
    time_provider.inc(Duration::from_secs(1));
    notify_idle.notified_with_timeout().await;

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
async fn test_refresh_does_not_update_lru_time() {
    let TestStateLruAndRefresh {
        mut backend,
        refresh_duration_provider,
        size_estimator,
        time_provider,
        loader,
        notify_idle,
        pool,
        ..
    } = TestStateLruAndRefresh::new();

    size_estimator.mock_size(1, String::from("a"), TestSize(4));
    size_estimator.mock_size(1, String::from("foo"), TestSize(4));
    size_estimator.mock_size(2, String::from("b"), TestSize(4));
    size_estimator.mock_size(3, String::from("c"), TestSize(4));

    refresh_duration_provider.set_refresh_in(
        1,
        String::from("a"),
        Some(backoff_cfg(Duration::from_secs(1))),
    );
    refresh_duration_provider.set_refresh_in(1, String::from("foo"), None);
    refresh_duration_provider.set_refresh_in(2, String::from("b"), None);
    refresh_duration_provider.set_refresh_in(3, String::from("c"), None);

    let barrier = loader.block_next(1, String::from("foo"));
    backend.set(1, String::from("a"));
    pool.wait_converged().await;

    // trigger refresh
    time_provider.inc(Duration::from_secs(1));

    time_provider.inc(Duration::from_secs(1));
    backend.set(2, String::from("b"));
    pool.wait_converged().await;

    time_provider.inc(Duration::from_secs(1));

    notify_idle.notified_with_timeout().await;
    barrier.wait().await;
    notify_idle.notified_with_timeout().await;

    // add a third item to the cache, forcing LRU to evict one of the items
    backend.set(3, String::from("c"));
    pool.wait_converged().await;

    // Should evict `1` even though it was refreshed after `2` was added
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

    let barrier = loader.block_next(1, String::from("foo"));
    refresh_duration_provider.set_refresh_in(
        1,
        String::from("a"),
        Some(backoff_cfg(Duration::from_secs(1))),
    );
    ttl_provider.set_expires_in(1, String::from("a"), Some(Duration::from_secs(2)));
    backend.set(1, String::from("a"));

    // perform refresh
    time_provider.inc(Duration::from_secs(1));
    notify_idle.notified_with_timeout().await;

    time_provider.inc(Duration::from_secs(1));
    notify_idle.not_notified().await;
    assert_eq!(backend.get(&1), None);

    // late loader finish will NOT bring the entry back
    barrier.wait().await;
    notify_idle.notified_with_timeout().await;
    assert_eq!(backend.get(&1), None);
}

#[tokio::test]
async fn test_refresh_can_trigger_lru_eviction() {
    maybe_start_logging();

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

    refresh_duration_provider.set_refresh_in(
        1,
        String::from("a"),
        Some(backoff_cfg(Duration::from_secs(1))),
    );
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
    pool.wait_converged().await;
    assert_eq!(backend.get(&2), Some(String::from("c")));
    assert_eq!(backend.get(&3), Some(String::from("d")));
    time_provider.inc(Duration::from_millis(1));
    assert_eq!(backend.get(&1), Some(String::from("a")));

    // refresh
    time_provider.inc(Duration::from_secs(10));
    notify_idle.notified_with_timeout().await;
    pool.wait_converged().await;

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

#[tokio::test]
async fn test_remove_if_check_does_not_extend_lifetime() {
    let TestStateLruAndRemoveIf {
        mut backend,
        size_estimator,
        time_provider,
        remove_if_handle,
        pool,
        ..
    } = TestStateLruAndRemoveIf::new().await;

    size_estimator.mock_size(1, String::from("a"), TestSize(4));
    size_estimator.mock_size(2, String::from("b"), TestSize(4));
    size_estimator.mock_size(3, String::from("c"), TestSize(4));

    backend.set(1, String::from("a"));
    pool.wait_converged().await;
    time_provider.inc(Duration::from_secs(1));

    backend.set(2, String::from("b"));
    pool.wait_converged().await;
    time_provider.inc(Duration::from_secs(1));

    // Checking remove_if should not count as a "use" of 1
    // for the "least recently used" calculation
    remove_if_handle.remove_if(&1, |_| false);
    backend.set(3, String::from("c"));
    pool.wait_converged().await;

    // adding "c" totals 12 size, but backend has room for only 10
    // so "least recently used" (in this case 1, not 2) should be removed
    assert_eq!(backend.get(&1), None);
    assert!(backend.get(&2).is_some());
}

/// Test setup that integrates the TTL policy with a refresh.
struct TestStateTtlAndRefresh {
    backend: PolicyBackend<u8, String>,
    ttl_provider: Arc<TestTtlProvider>,
    refresh_duration_provider: Arc<TestRefreshDurationProvider>,
    time_provider: Arc<MockProvider>,
    loader: Arc<TestLoader<u8, (), String>>,
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

        // set up "RNG" that always generates the maximum, so we can test things easier
        let rng_overwrite = StepRng::new(u64::MAX, 0);

        let mut backend = PolicyBackend::hashmap_backed(Arc::clone(&time_provider) as _);
        backend.add_policy(RefreshPolicy::new_inner(
            Arc::clone(&time_provider) as _,
            Arc::clone(&refresh_duration_provider) as _,
            Arc::clone(&loader) as _,
            "my_cache",
            &metric_registry,
            Arc::clone(&notify_idle),
            &Handle::current(),
            Some(rng_overwrite),
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
    loader: Arc<TestLoader<u8, (), String>>,
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

        // set up "RNG" that always generates the maximum, so we can test things easier
        let rng_overwrite = StepRng::new(u64::MAX, 0);

        let mut backend = PolicyBackend::hashmap_backed(Arc::clone(&time_provider) as _);
        backend.add_policy(RefreshPolicy::new_inner(
            Arc::clone(&time_provider) as _,
            Arc::clone(&refresh_duration_provider) as _,
            Arc::clone(&loader) as _,
            "my_cache",
            &metric_registry,
            Arc::clone(&notify_idle),
            &Handle::current(),
            Some(rng_overwrite),
        ));
        let pool = Arc::new(ResourcePool::new(
            "my_pool",
            TestSize(10),
            Arc::clone(&metric_registry),
            &Handle::current(),
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

        let mut backend = PolicyBackend::hashmap_backed(Arc::clone(&time_provider) as _);
        backend.add_policy(TtlPolicy::new(
            Arc::clone(&ttl_provider) as _,
            "my_cache",
            &metric_registry,
        ));
        let pool = Arc::new(ResourcePool::new(
            "my_pool",
            TestSize(10),
            Arc::clone(&metric_registry),
            &Handle::current(),
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

/// Test setup that integrates the LRU policy with RemoveIf and max size of 10
struct TestStateLruAndRemoveIf {
    backend: PolicyBackend<u8, String>,
    time_provider: Arc<MockProvider>,
    size_estimator: Arc<TestSizeEstimator>,
    remove_if_handle: RemoveIfHandle<u8, String>,
    pool: Arc<ResourcePool<TestSize>>,
}

impl TestStateLruAndRemoveIf {
    async fn new() -> Self {
        let time_provider = Arc::new(MockProvider::new(Time::MIN));
        let metric_registry = Arc::new(metric::Registry::new());
        let size_estimator = Arc::new(TestSizeEstimator::default());

        let mut backend = PolicyBackend::hashmap_backed(Arc::clone(&time_provider) as _);

        let pool = Arc::new(ResourcePool::new(
            "my_pool",
            TestSize(10),
            Arc::clone(&metric_registry),
            &Handle::current(),
        ));
        backend.add_policy(LruPolicy::new(
            Arc::clone(&pool),
            "my_cache",
            Arc::clone(&size_estimator) as _,
        ));

        let (constructor, remove_if_handle) =
            RemoveIfPolicy::create_constructor_and_handle("my_cache", &metric_registry);
        backend.add_policy(constructor);

        Self {
            backend,
            time_provider,
            size_estimator,
            remove_if_handle,
            pool,
        }
    }
}

/// Test setup that integrates the LRU policy with a refresh.
struct TestStateLruAndRefresh {
    backend: PolicyBackend<u8, String>,
    size_estimator: Arc<TestSizeEstimator>,
    refresh_duration_provider: Arc<TestRefreshDurationProvider>,
    time_provider: Arc<MockProvider>,
    loader: Arc<TestLoader<u8, (), String>>,
    notify_idle: Arc<Notify>,
    pool: Arc<ResourcePool<TestSize>>,
}

impl TestStateLruAndRefresh {
    fn new() -> Self {
        let refresh_duration_provider = Arc::new(TestRefreshDurationProvider::new());
        let size_estimator = Arc::new(TestSizeEstimator::default());
        let time_provider = Arc::new(MockProvider::new(Time::MIN));
        let metric_registry = Arc::new(metric::Registry::new());
        let loader = Arc::new(TestLoader::default());
        let notify_idle = Arc::new(Notify::new());

        // set up "RNG" that always generates the maximum, so we can test things easier
        let rng_overwrite = StepRng::new(u64::MAX, 0);

        let mut backend = PolicyBackend::hashmap_backed(Arc::clone(&time_provider) as _);
        backend.add_policy(RefreshPolicy::new_inner(
            Arc::clone(&time_provider) as _,
            Arc::clone(&refresh_duration_provider) as _,
            Arc::clone(&loader) as _,
            "my_cache",
            &metric_registry,
            Arc::clone(&notify_idle),
            &Handle::current(),
            Some(rng_overwrite),
        ));

        let pool = Arc::new(ResourcePool::new(
            "my_pool",
            TestSize(10),
            Arc::clone(&metric_registry),
            &Handle::current(),
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
            notify_idle,
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
