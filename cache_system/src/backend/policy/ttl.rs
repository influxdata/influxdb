//! Time-to-live handling.
use std::{fmt::Debug, hash::Hash, marker::PhantomData, sync::Arc, time::Duration};

use iox_time::Time;
use metric::U64Counter;

use crate::addressable_heap::AddressableHeap;

use super::{CallbackHandle, ChangeRequest, Subscriber};

/// Interface to provide TTL (time to live) data for a key-value pair.
pub trait TtlProvider: std::fmt::Debug + Send + Sync + 'static {
    /// Cache key.
    type K;

    /// Cached value.
    type V;

    /// When should the given key-value pair expire?
    ///
    /// Return `None` for "never".
    ///
    /// The function is only called once for a newly cached key-value pair. This means:
    /// - There is no need in remembering the time of a given pair (e.g. you can safely always return a constant).
    /// - You cannot change the TTL after the data was cached.
    ///
    /// Expiration is set to take place AT OR AFTER the provided duration.
    fn expires_in(&self, k: &Self::K, v: &Self::V) -> Option<Duration>;
}

/// [`TtlProvider`] that never expires.
#[derive(Default)]
pub struct NeverTtlProvider<K, V>
where
    K: 'static,
    V: 'static,
{
    // phantom data that is Send and Sync, see https://stackoverflow.com/a/50201389
    _k: PhantomData<fn() -> K>,
    _v: PhantomData<fn() -> V>,
}

impl<K, V> std::fmt::Debug for NeverTtlProvider<K, V> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NeverTtlProvider").finish_non_exhaustive()
    }
}

impl<K, V> TtlProvider for NeverTtlProvider<K, V> {
    type K = K;
    type V = V;

    fn expires_in(&self, _k: &Self::K, _v: &Self::V) -> Option<Duration> {
        None
    }
}

/// [`TtlProvider`] that returns a constant value.
pub struct ConstantValueTtlProvider<K, V>
where
    K: 'static,
    V: 'static,
{
    // phantom data that is Send and Sync, see https://stackoverflow.com/a/50201389
    _k: PhantomData<fn() -> K>,
    _v: PhantomData<fn() -> V>,

    ttl: Option<Duration>,
}

impl<K, V> ConstantValueTtlProvider<K, V>
where
    K: 'static,
    V: 'static,
{
    /// Create new provider with the given TTL value.
    pub fn new(ttl: Option<Duration>) -> Self {
        Self {
            _k: PhantomData,
            _v: PhantomData,
            ttl,
        }
    }
}

impl<K, V> std::fmt::Debug for ConstantValueTtlProvider<K, V> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConstantValueTtlProvider")
            .field("ttl", &self.ttl)
            .finish_non_exhaustive()
    }
}

impl<K, V> TtlProvider for ConstantValueTtlProvider<K, V> {
    type K = K;
    type V = V;

    fn expires_in(&self, _k: &Self::K, _v: &Self::V) -> Option<Duration> {
        self.ttl
    }
}

/// [`TtlProvider`] that returns different values for `None`/`Some(...)` values.
pub struct OptionalValueTtlProvider<K, V>
where
    K: 'static,
    V: 'static,
{
    // phantom data that is Send and Sync, see https://stackoverflow.com/a/50201389
    _k: PhantomData<fn() -> K>,
    _v: PhantomData<fn() -> V>,

    ttl_none: Option<Duration>,
    ttl_some: Option<Duration>,
}

impl<K, V> OptionalValueTtlProvider<K, V>
where
    K: 'static,
    V: 'static,
{
    /// Create new provider with the given TTL values for `None` and `Some(...)`.
    pub fn new(ttl_none: Option<Duration>, ttl_some: Option<Duration>) -> Self {
        Self {
            _k: PhantomData,
            _v: PhantomData,
            ttl_none,
            ttl_some,
        }
    }
}

impl<K, V> std::fmt::Debug for OptionalValueTtlProvider<K, V> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OptionalValueTtlProvider")
            .field("ttl_none", &self.ttl_none)
            .field("ttl_some", &self.ttl_some)
            .finish_non_exhaustive()
    }
}

impl<K, V> TtlProvider for OptionalValueTtlProvider<K, V> {
    type K = K;
    type V = Option<V>;

    fn expires_in(&self, _k: &Self::K, v: &Self::V) -> Option<Duration> {
        match v {
            None => self.ttl_none,
            Some(_) => self.ttl_some,
        }
    }
}

/// Cache policy that implements Time To Life.
///
/// # Cache Eviction
/// Every method ([`get`](Subscriber::get), [`set`](Subscriber::set), [`remove`](Subscriber::remove)) causes the
/// cache to check for expired keys. This may lead to certain delays, esp. when dropping the contained values takes a
/// long time.
#[derive(Debug)]
pub struct TtlPolicy<K, V>
where
    K: Clone + Eq + Debug + Hash + Ord + Send + 'static,
    V: Clone + Debug + Send + 'static,
{
    ttl_provider: Arc<dyn TtlProvider<K = K, V = V>>,
    expiration: AddressableHeap<K, (), Time>,
    metric_expired: U64Counter,
}

impl<K, V> TtlPolicy<K, V>
where
    K: Clone + Eq + Debug + Hash + Ord + Send + 'static,
    V: Clone + Debug + Send + 'static,
{
    /// Create new TTL policy.
    pub fn new(
        ttl_provider: Arc<dyn TtlProvider<K = K, V = V>>,
        name: &'static str,
        metric_registry: &metric::Registry,
    ) -> impl FnOnce(CallbackHandle<K, V>) -> Self {
        let metric_expired = metric_registry
            .register_metric::<U64Counter>(
                "cache_ttl_expired",
                "Number of entries that expired via TTL.",
            )
            .recorder(&[("name", name)]);

        |mut callback_handle| {
            callback_handle.execute_requests(vec![ChangeRequest::ensure_empty()]);

            Self {
                ttl_provider,
                expiration: Default::default(),
                metric_expired,
            }
        }
    }

    fn evict_expired(&mut self, now: Time) -> Vec<ChangeRequest<'static, K, V>> {
        let mut requests = vec![];

        while self
            .expiration
            .peek()
            .map(|(_k, _, t)| *t <= now)
            .unwrap_or_default()
        {
            let (k, _, _t) = self.expiration.pop().unwrap();
            self.metric_expired.inc(1);
            requests.push(ChangeRequest::remove(k));
        }

        requests
    }
}

impl<K, V> Subscriber for TtlPolicy<K, V>
where
    K: Clone + Eq + Debug + Hash + Ord + Send + 'static,
    V: Clone + Debug + Send + 'static,
{
    type K = K;
    type V = V;

    fn get(&mut self, _k: &Self::K, now: Time) -> Vec<ChangeRequest<'static, Self::K, Self::V>> {
        self.evict_expired(now)
    }

    fn set(
        &mut self,
        k: &Self::K,
        v: &Self::V,
        now: Time,
    ) -> Vec<ChangeRequest<'static, Self::K, Self::V>> {
        let mut requests = self.evict_expired(now);

        if let Some(ttl) = self.ttl_provider.expires_in(k, v) {
            if ttl.is_zero() {
                requests.push(ChangeRequest::remove(k.clone()));
            }

            match now.checked_add(ttl) {
                Some(t) => {
                    self.expiration.insert(k.clone(), (), t);
                }
                None => {
                    // Still need to ensure that any current expiration is disabled
                    self.expiration.remove(k);
                }
            }
        } else {
            // Still need to ensure that any current expiration is disabled
            self.expiration.remove(k);
        };

        requests
    }

    fn remove(&mut self, k: &Self::K, now: Time) -> Vec<ChangeRequest<'static, Self::K, Self::V>> {
        self.expiration.remove(k);
        self.evict_expired(now)
    }
}

pub mod test_util {
    //! Test utils for TTL policy.
    use std::collections::HashMap;

    use parking_lot::Mutex;

    use super::*;

    /// [`TtlProvider`] for testing.
    #[derive(Debug, Default)]
    pub struct TestTtlProvider {
        expires_in: Mutex<HashMap<(u8, String), Option<Duration>>>,
    }

    impl TestTtlProvider {
        /// Create new, empty provider.
        pub fn new() -> Self {
            Self::default()
        }

        /// Set TTL time for given key-value pair.
        pub fn set_expires_in(&self, k: u8, v: String, d: Option<Duration>) {
            self.expires_in.lock().insert((k, v), d);
        }
    }

    impl TtlProvider for TestTtlProvider {
        type K = u8;
        type V = String;

        fn expires_in(&self, k: &Self::K, v: &Self::V) -> Option<Duration> {
            *self
                .expires_in
                .lock()
                .get(&(*k, v.clone()))
                .expect("expires_in value not mocked")
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        #[should_panic(expected = "expires_in value not mocked")]
        fn test_panic_value_not_mocked() {
            TestTtlProvider::new().expires_in(&1, &String::from("foo"));
        }

        #[test]
        fn test_mocking() {
            let provider = TestTtlProvider::default();

            provider.set_expires_in(1, String::from("a"), None);
            provider.set_expires_in(1, String::from("b"), Some(Duration::from_secs(1)));
            provider.set_expires_in(2, String::from("a"), Some(Duration::from_secs(2)));

            assert_eq!(provider.expires_in(&1, &String::from("a")), None,);
            assert_eq!(
                provider.expires_in(&1, &String::from("b")),
                Some(Duration::from_secs(1)),
            );
            assert_eq!(
                provider.expires_in(&2, &String::from("a")),
                Some(Duration::from_secs(2)),
            );

            // replace
            provider.set_expires_in(1, String::from("a"), Some(Duration::from_secs(3)));
            assert_eq!(
                provider.expires_in(&1, &String::from("a")),
                Some(Duration::from_secs(3)),
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, time::Duration};

    use iox_time::MockProvider;
    use metric::{Observation, RawReporter};

    use crate::backend::{policy::PolicyBackend, CacheBackend};

    use super::{test_util::TestTtlProvider, *};

    #[test]
    fn test_never_ttl_provider() {
        let provider = NeverTtlProvider::<u8, i8>::default();
        assert_eq!(provider.expires_in(&1, &2), None);
    }

    #[test]
    fn test_constant_value_ttl_provider() {
        let ttl = Some(Duration::from_secs(1));
        let provider = ConstantValueTtlProvider::<u8, i8>::new(ttl);
        assert_eq!(provider.expires_in(&1, &2), ttl);
    }

    #[test]
    fn test_optional_value_ttl_provider() {
        let ttl_none = Some(Duration::from_secs(1));
        let ttl_some = Some(Duration::from_secs(2));
        let provider = OptionalValueTtlProvider::<u8, i8>::new(ttl_none, ttl_some);
        assert_eq!(provider.expires_in(&1, &None), ttl_none);
        assert_eq!(provider.expires_in(&1, &Some(2)), ttl_some);
    }

    #[test]
    #[should_panic(expected = "inner backend is not empty")]
    fn test_panic_inner_not_empty() {
        let ttl_provider = Arc::new(TestTtlProvider::new());
        let metric_registry = metric::Registry::new();

        let time_provider = Arc::new(MockProvider::new(Time::MIN));
        let mut backend: PolicyBackend<u8, String> = PolicyBackend::hashmap_backed(time_provider);
        let policy_constructor =
            TtlPolicy::new(Arc::clone(&ttl_provider) as _, "my_cache", &metric_registry);
        backend.add_policy(|mut handle| {
            handle.execute_requests(vec![ChangeRequest::set(1, String::from("foo"))]);
            policy_constructor(handle)
        });
    }

    #[test]
    fn test_expires_single() {
        let TestState {
            mut backend,
            metric_registry,
            ttl_provider,
            time_provider,
        } = TestState::new();

        assert_eq!(get_expired_metric(&metric_registry), 0);

        ttl_provider.set_expires_in(1, String::from("a"), Some(Duration::from_secs(1)));
        backend.set(1, String::from("a"));
        assert_eq!(backend.get(&1), Some(String::from("a")));

        assert_eq!(get_expired_metric(&metric_registry), 0);

        time_provider.inc(Duration::from_secs(1));
        assert_eq!(backend.get(&1), None);

        assert_eq!(get_expired_metric(&metric_registry), 1);
    }

    #[test]
    fn test_overflow_expire() {
        let ttl_provider = Arc::new(TestTtlProvider::new());
        let metric_registry = metric::Registry::new();

        // init time provider at MAX!
        let time_provider = Arc::new(MockProvider::new(Time::MAX));
        let mut backend: PolicyBackend<u8, String> = PolicyBackend::hashmap_backed(time_provider);
        backend.add_policy(TtlPolicy::new(
            Arc::clone(&ttl_provider) as _,
            "my_cache",
            &metric_registry,
        ));

        ttl_provider.set_expires_in(1, String::from("a"), Some(Duration::MAX));
        backend.set(1, String::from("a"));
        assert_eq!(backend.get(&1), Some(String::from("a")));
    }

    #[test]
    fn test_never_expire() {
        let TestState {
            mut backend,
            ttl_provider,
            time_provider,
            ..
        } = TestState::new();

        ttl_provider.set_expires_in(1, String::from("a"), None);
        backend.set(1, String::from("a"));
        assert_eq!(backend.get(&1), Some(String::from("a")));

        time_provider.inc(Duration::from_secs(1));
        assert_eq!(backend.get(&1), Some(String::from("a")));
    }

    #[test]
    fn test_expiration_uses_key_and_value() {
        let TestState {
            mut backend,
            ttl_provider,
            time_provider,
            ..
        } = TestState::new();

        ttl_provider.set_expires_in(1, String::from("a"), Some(Duration::from_secs(1)));
        ttl_provider.set_expires_in(1, String::from("b"), Some(Duration::from_secs(4)));
        ttl_provider.set_expires_in(2, String::from("a"), Some(Duration::from_secs(2)));
        backend.set(1, String::from("b"));

        time_provider.inc(Duration::from_secs(3));
        assert_eq!(backend.get(&1), Some(String::from("b")));
    }

    #[test]
    fn test_override_with_different_expiration() {
        let TestState {
            mut backend,
            ttl_provider,
            time_provider,
            ..
        } = TestState::new();

        ttl_provider.set_expires_in(1, String::from("a"), Some(Duration::from_secs(1)));
        backend.set(1, String::from("a"));
        assert_eq!(backend.get(&1), Some(String::from("a")));

        ttl_provider.set_expires_in(1, String::from("a"), Some(Duration::from_secs(3)));
        backend.set(1, String::from("a"));

        time_provider.inc(Duration::from_secs(2));
        assert_eq!(backend.get(&1), Some(String::from("a")));
    }

    #[test]
    fn test_override_with_no_expiration() {
        let TestState {
            mut backend,
            ttl_provider,
            time_provider,
            ..
        } = TestState::new();

        ttl_provider.set_expires_in(1, String::from("a"), Some(Duration::from_secs(1)));
        backend.set(1, String::from("a"));
        assert_eq!(backend.get(&1), Some(String::from("a")));

        ttl_provider.set_expires_in(1, String::from("a"), None);
        backend.set(1, String::from("a"));

        time_provider.inc(Duration::from_secs(2));
        assert_eq!(backend.get(&1), Some(String::from("a")));
    }

    #[test]
    fn test_override_with_some_expiration() {
        let TestState {
            mut backend,
            ttl_provider,
            time_provider,
            ..
        } = TestState::new();

        ttl_provider.set_expires_in(1, String::from("a"), None);
        backend.set(1, String::from("a"));
        assert_eq!(backend.get(&1), Some(String::from("a")));

        ttl_provider.set_expires_in(1, String::from("a"), Some(Duration::from_secs(1)));
        backend.set(1, String::from("a"));

        time_provider.inc(Duration::from_secs(2));
        assert_eq!(backend.get(&1), None);
    }

    #[test]
    fn test_override_with_overflow() {
        let ttl_provider = Arc::new(TestTtlProvider::new());
        let metric_registry = metric::Registry::new();

        // init time provider at nearly MAX!
        let time_provider = Arc::new(MockProvider::new(Time::MAX - Duration::from_secs(2)));
        let mut backend: PolicyBackend<u8, String> =
            PolicyBackend::hashmap_backed(Arc::clone(&time_provider) as _);
        backend.add_policy(TtlPolicy::new(
            Arc::clone(&ttl_provider) as _,
            "my_cache",
            &metric_registry,
        ));

        ttl_provider.set_expires_in(1, String::from("a"), Some(Duration::from_secs(1)));
        backend.set(1, String::from("a"));
        assert_eq!(backend.get(&1), Some(String::from("a")));

        ttl_provider.set_expires_in(1, String::from("a"), Some(Duration::from_secs(u64::MAX)));
        backend.set(1, String::from("a"));

        time_provider.inc(Duration::from_secs(2));
        assert_eq!(backend.get(&1), Some(String::from("a")));
    }

    #[test]
    fn test_readd_with_different_expiration() {
        let TestState {
            mut backend,
            ttl_provider,
            time_provider,
            ..
        } = TestState::new();

        ttl_provider.set_expires_in(1, String::from("a"), Some(Duration::from_secs(1)));
        backend.set(1, String::from("a"));
        assert_eq!(backend.get(&1), Some(String::from("a")));

        ttl_provider.set_expires_in(1, String::from("a"), Some(Duration::from_secs(3)));
        backend.remove(&1);
        backend.set(1, String::from("a"));

        time_provider.inc(Duration::from_secs(2));
        assert_eq!(backend.get(&1), Some(String::from("a")));
    }

    #[test]
    fn test_readd_with_no_expiration() {
        let TestState {
            mut backend,
            ttl_provider,
            time_provider,
            ..
        } = TestState::new();

        ttl_provider.set_expires_in(1, String::from("a"), Some(Duration::from_secs(1)));
        backend.set(1, String::from("a"));
        assert_eq!(backend.get(&1), Some(String::from("a")));

        ttl_provider.set_expires_in(1, String::from("a"), None);
        backend.remove(&1);
        backend.set(1, String::from("a"));

        time_provider.inc(Duration::from_secs(2));
        assert_eq!(backend.get(&1), Some(String::from("a")));
    }

    #[test]
    fn test_update_cleans_multiple_keys() {
        let TestState {
            mut backend,
            ttl_provider,
            time_provider,
            ..
        } = TestState::new();

        ttl_provider.set_expires_in(1, String::from("a"), Some(Duration::from_secs(1)));
        ttl_provider.set_expires_in(2, String::from("b"), Some(Duration::from_secs(2)));
        ttl_provider.set_expires_in(3, String::from("c"), Some(Duration::from_secs(2)));
        ttl_provider.set_expires_in(4, String::from("d"), Some(Duration::from_secs(3)));
        backend.set(1, String::from("a"));
        backend.set(2, String::from("b"));
        backend.set(3, String::from("c"));
        backend.set(4, String::from("d"));
        assert_eq!(backend.get(&1), Some(String::from("a")));
        assert_eq!(backend.get(&2), Some(String::from("b")));
        assert_eq!(backend.get(&3), Some(String::from("c")));
        assert_eq!(backend.get(&4), Some(String::from("d")));

        time_provider.inc(Duration::from_secs(2));
        assert_eq!(backend.get(&1), None);

        {
            let inner_ref = backend.inner_ref();
            let inner_backend = inner_ref
                .as_any()
                .downcast_ref::<HashMap<u8, String>>()
                .unwrap();
            assert!(!inner_backend.contains_key(&1));
            assert!(!inner_backend.contains_key(&2));
            assert!(!inner_backend.contains_key(&3));
            assert!(inner_backend.contains_key(&4));
        }

        assert_eq!(backend.get(&2), None);
        assert_eq!(backend.get(&3), None);
        assert_eq!(backend.get(&4), Some(String::from("d")));
    }

    #[test]
    fn test_remove_expired_key() {
        let TestState {
            mut backend,
            ttl_provider,
            time_provider,
            ..
        } = TestState::new();

        ttl_provider.set_expires_in(1, String::from("a"), Some(Duration::from_secs(1)));
        backend.set(1, String::from("a"));
        assert_eq!(backend.get(&1), Some(String::from("a")));

        time_provider.inc(Duration::from_secs(1));
        backend.remove(&1);
        assert_eq!(backend.get(&1), None);
    }

    #[test]
    fn test_expire_removed_key() {
        let TestState {
            mut backend,
            ttl_provider,
            time_provider,
            ..
        } = TestState::new();

        ttl_provider.set_expires_in(1, String::from("a"), Some(Duration::from_secs(1)));
        ttl_provider.set_expires_in(2, String::from("b"), Some(Duration::from_secs(2)));
        backend.set(1, String::from("a"));
        backend.remove(&1);

        time_provider.inc(Duration::from_secs(1));
        backend.set(2, String::from("b"));
        assert_eq!(backend.get(&1), None);
        assert_eq!(backend.get(&2), Some(String::from("b")));
    }

    #[test]
    fn test_expire_immediately() {
        let TestState {
            mut backend,
            ttl_provider,
            ..
        } = TestState::new();

        ttl_provider.set_expires_in(1, String::from("a"), Some(Duration::from_secs(0)));
        backend.set(1, String::from("a"));

        assert!(backend.is_empty());

        assert_eq!(backend.get(&1), None);
    }

    #[test]
    fn test_generic_backend() {
        use crate::backend::test_util::test_generic;

        test_generic(|| {
            let ttl_provider = Arc::new(NeverTtlProvider::default());
            let time_provider = Arc::new(MockProvider::new(Time::MIN));
            let metric_registry = metric::Registry::new();
            let mut backend = PolicyBackend::hashmap_backed(time_provider);
            backend.add_policy(TtlPolicy::new(
                Arc::clone(&ttl_provider) as _,
                "my_cache",
                &metric_registry,
            ));
            backend
        });
    }

    struct TestState {
        backend: PolicyBackend<u8, String>,
        metric_registry: metric::Registry,
        ttl_provider: Arc<TestTtlProvider>,
        time_provider: Arc<MockProvider>,
    }

    impl TestState {
        fn new() -> Self {
            let ttl_provider = Arc::new(TestTtlProvider::new());
            let time_provider = Arc::new(MockProvider::new(Time::MIN));
            let metric_registry = metric::Registry::new();

            let mut backend = PolicyBackend::hashmap_backed(Arc::clone(&time_provider) as _);
            backend.add_policy(TtlPolicy::new(
                Arc::clone(&ttl_provider) as _,
                "my_cache",
                &metric_registry,
            ));

            Self {
                backend,
                metric_registry,
                ttl_provider,
                time_provider,
            }
        }
    }

    fn get_expired_metric(metric_registry: &metric::Registry) -> u64 {
        let mut reporter = RawReporter::default();
        metric_registry.report(&mut reporter);
        let observation = reporter
            .metric("cache_ttl_expired")
            .unwrap()
            .observation(&[("name", "my_cache")])
            .unwrap();

        if let Observation::U64Counter(c) = observation {
            *c
        } else {
            panic!("Wrong observation type")
        }
    }
}
