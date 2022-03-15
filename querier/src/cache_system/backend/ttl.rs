use std::{any::Any, fmt::Debug, hash::Hash, marker::PhantomData, sync::Arc, time::Duration};

use time::{Time, TimeProvider};

use super::{addressable_heap::AddressableHeap, CacheBackend};

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

/// Cache backend that implements Time To Life.
///
/// # Cache Eviction
/// Every method ([`get`](CacheBackend::get), [`set`](CacheBackend::set), [`remove`](CacheBackend::remove)) causes the
/// cache to check for expired keys. This may lead to certain delays, esp. when dropping the contained values takes a
/// long time.
#[derive(Debug)]
pub struct TtlBackend<K, V>
where
    K: Clone + Eq + Debug + Hash + Ord + Send + 'static,
    V: Clone + Debug + Send + 'static,
{
    inner_backend: Box<dyn CacheBackend<K = K, V = V>>,
    ttl_provider: Arc<dyn TtlProvider<K = K, V = V>>,
    time_provider: Arc<dyn TimeProvider>,
    expiration: AddressableHeap<K, (), Time>,
}

impl<K, V> TtlBackend<K, V>
where
    K: Clone + Eq + Debug + Hash + Ord + Send + 'static,
    V: Clone + Debug + Send + 'static,
{
    /// Create new backend w/o any known keys.
    ///
    /// The inner backend SHOULD NOT contain any data at this point, otherwise we will not track any TTLs for these entries.
    pub fn new(
        inner_backend: Box<dyn CacheBackend<K = K, V = V>>,
        ttl_provider: Arc<dyn TtlProvider<K = K, V = V>>,
        time_provider: Arc<dyn TimeProvider>,
    ) -> Self {
        Self {
            inner_backend,
            ttl_provider,
            time_provider,
            expiration: Default::default(),
        }
    }

    fn evict_expired(&mut self, now: Time) {
        while self
            .expiration
            .peek()
            .map(|(_k, _, t)| *t <= now)
            .unwrap_or_default()
        {
            let (k, _, _t) = self.expiration.pop().unwrap();
            self.inner_backend.remove(&k);
        }
    }

    /// Reference to inner backend.
    pub fn inner_backend(&self) -> &dyn CacheBackend<K = K, V = V> {
        self.inner_backend.as_ref()
    }

    /// Reference to TTL provider.
    pub fn ttl_provider(&self) -> &Arc<dyn TtlProvider<K = K, V = V>> {
        &self.ttl_provider
    }
}

impl<K, V> CacheBackend for TtlBackend<K, V>
where
    K: Clone + Eq + Debug + Hash + Ord + Send + 'static,
    V: Clone + Debug + Send + 'static,
{
    type K = K;
    type V = V;

    fn get(&mut self, k: &Self::K) -> Option<Self::V> {
        self.evict_expired(self.time_provider.now());

        self.inner_backend.get(k)
    }

    fn set(&mut self, k: Self::K, v: Self::V) {
        let now = self.time_provider.now();
        self.evict_expired(now);

        if let Some(ttl) = self
            .ttl_provider
            .expires_in(&k, &v)
            .and_then(|d| now.checked_add(d))
        {
            self.expiration.insert(k.clone(), (), ttl);
        } else {
            // Still need to ensure that any current expiration is disabled
            self.expiration.remove(&k);
        }

        self.inner_backend.set(k, v);
    }

    fn remove(&mut self, k: &Self::K) {
        self.evict_expired(self.time_provider.now());

        self.inner_backend.remove(k);
        self.expiration.remove(k);
    }

    fn as_any(&self) -> &dyn Any {
        self as &dyn Any
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, time::Duration};

    use parking_lot::Mutex;
    use time::MockProvider;

    use super::*;

    #[test]
    fn test_expires_single() {
        let ttl_provider = Arc::new(TestTtlProvider::new());
        let time_provider = Arc::new(MockProvider::new(Time::MIN));
        let mut backend = TtlBackend::new(
            Box::new(HashMap::<u8, String>::new()),
            Arc::clone(&ttl_provider) as _,
            Arc::clone(&time_provider) as _,
        );

        ttl_provider.set_expires_in(1, String::from("a"), Some(Duration::from_secs(1)));
        backend.set(1, String::from("a"));
        assert_eq!(backend.get(&1), Some(String::from("a")));

        time_provider.inc(Duration::from_secs(1));
        assert_eq!(backend.get(&1), None);
    }

    #[test]
    fn test_overflow_expire() {
        let ttl_provider = Arc::new(TestTtlProvider::new());

        // init time provider at MAX!
        let time_provider = Arc::new(MockProvider::new(Time::MAX));
        let mut backend = TtlBackend::new(
            Box::new(HashMap::<u8, String>::new()),
            Arc::clone(&ttl_provider) as _,
            Arc::clone(&time_provider) as _,
        );

        ttl_provider.set_expires_in(1, String::from("a"), Some(Duration::MAX));
        backend.set(1, String::from("a"));
        assert_eq!(backend.get(&1), Some(String::from("a")));
    }

    #[test]
    fn test_never_expire() {
        let ttl_provider = Arc::new(TestTtlProvider::new());
        let time_provider = Arc::new(MockProvider::new(Time::MIN));
        let mut backend = TtlBackend::new(
            Box::new(HashMap::<u8, String>::new()),
            Arc::clone(&ttl_provider) as _,
            Arc::clone(&time_provider) as _,
        );

        ttl_provider.set_expires_in(1, String::from("a"), None);
        backend.set(1, String::from("a"));
        assert_eq!(backend.get(&1), Some(String::from("a")));

        time_provider.inc(Duration::from_secs(1));
        assert_eq!(backend.get(&1), Some(String::from("a")));
    }

    #[test]
    fn test_expiration_uses_key_and_value() {
        let ttl_provider = Arc::new(TestTtlProvider::new());
        let time_provider = Arc::new(MockProvider::new(Time::MIN));
        let mut backend = TtlBackend::new(
            Box::new(HashMap::<u8, String>::new()),
            Arc::clone(&ttl_provider) as _,
            Arc::clone(&time_provider) as _,
        );

        ttl_provider.set_expires_in(1, String::from("a"), Some(Duration::from_secs(1)));
        ttl_provider.set_expires_in(1, String::from("b"), Some(Duration::from_secs(4)));
        ttl_provider.set_expires_in(2, String::from("a"), Some(Duration::from_secs(2)));
        backend.set(1, String::from("b"));

        time_provider.inc(Duration::from_secs(3));
        assert_eq!(backend.get(&1), Some(String::from("b")));
    }

    #[test]
    fn test_override_with_different_expiration() {
        let ttl_provider = Arc::new(TestTtlProvider::new());
        let time_provider = Arc::new(MockProvider::new(Time::MIN));
        let mut backend = TtlBackend::new(
            Box::new(HashMap::<u8, String>::new()),
            Arc::clone(&ttl_provider) as _,
            Arc::clone(&time_provider) as _,
        );

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
        let ttl_provider = Arc::new(TestTtlProvider::new());
        let time_provider = Arc::new(MockProvider::new(Time::MIN));
        let mut backend = TtlBackend::new(
            Box::new(HashMap::<u8, String>::new()),
            Arc::clone(&ttl_provider) as _,
            Arc::clone(&time_provider) as _,
        );

        ttl_provider.set_expires_in(1, String::from("a"), Some(Duration::from_secs(1)));
        backend.set(1, String::from("a"));
        assert_eq!(backend.get(&1), Some(String::from("a")));

        ttl_provider.set_expires_in(1, String::from("a"), None);
        backend.set(1, String::from("a"));

        time_provider.inc(Duration::from_secs(2));
        assert_eq!(backend.get(&1), Some(String::from("a")));
    }

    #[test]
    fn test_readd_with_different_expiration() {
        let ttl_provider = Arc::new(TestTtlProvider::new());
        let time_provider = Arc::new(MockProvider::new(Time::MIN));
        let mut backend = TtlBackend::new(
            Box::new(HashMap::<u8, String>::new()),
            Arc::clone(&ttl_provider) as _,
            Arc::clone(&time_provider) as _,
        );

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
        let ttl_provider = Arc::new(TestTtlProvider::new());
        let time_provider = Arc::new(MockProvider::new(Time::MIN));
        let mut backend = TtlBackend::new(
            Box::new(HashMap::<u8, String>::new()),
            Arc::clone(&ttl_provider) as _,
            Arc::clone(&time_provider) as _,
        );

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
        let ttl_provider = Arc::new(TestTtlProvider::new());
        let time_provider = Arc::new(MockProvider::new(Time::MIN));
        let mut backend = TtlBackend::new(
            Box::new(HashMap::<u8, String>::new()),
            Arc::clone(&ttl_provider) as _,
            Arc::clone(&time_provider) as _,
        );

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
        let inner_backend = backend
            .inner_backend()
            .as_any()
            .downcast_ref::<HashMap<u8, String>>()
            .unwrap();
        assert!(!inner_backend.contains_key(&1));
        assert!(!inner_backend.contains_key(&2));
        assert!(!inner_backend.contains_key(&3));
        assert!(inner_backend.contains_key(&4));
        assert_eq!(backend.get(&2), None);
        assert_eq!(backend.get(&3), None);
        assert_eq!(backend.get(&4), Some(String::from("d")));
    }

    #[test]
    fn test_remove_expired_key() {
        let ttl_provider = Arc::new(TestTtlProvider::new());
        let time_provider = Arc::new(MockProvider::new(Time::MIN));
        let mut backend = TtlBackend::new(
            Box::new(HashMap::<u8, String>::new()),
            Arc::clone(&ttl_provider) as _,
            Arc::clone(&time_provider) as _,
        );

        ttl_provider.set_expires_in(1, String::from("a"), Some(Duration::from_secs(1)));
        backend.set(1, String::from("a"));
        assert_eq!(backend.get(&1), Some(String::from("a")));

        time_provider.inc(Duration::from_secs(1));
        backend.remove(&1);
        assert_eq!(backend.get(&1), None);
    }

    #[test]
    fn test_expire_removed_key() {
        let ttl_provider = Arc::new(TestTtlProvider::new());
        let time_provider = Arc::new(MockProvider::new(Time::MIN));
        let mut backend = TtlBackend::new(
            Box::new(HashMap::<u8, String>::new()),
            Arc::clone(&ttl_provider) as _,
            Arc::clone(&time_provider) as _,
        );

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
    fn test_generic() {
        use crate::cache_system::backend::test_util::test_generic;

        test_generic(|| {
            let ttl_provider = Arc::new(NeverTtlProvider::default());
            let time_provider = Arc::new(MockProvider::new(Time::MIN));
            TtlBackend::new(
                Box::new(HashMap::<u8, String>::new()),
                ttl_provider,
                time_provider,
            )
        });
    }

    #[derive(Debug)]
    struct TestTtlProvider {
        expires_in: Mutex<HashMap<(u8, String), Option<Duration>>>,
    }

    impl TestTtlProvider {
        fn new() -> Self {
            Self {
                expires_in: Mutex::new(HashMap::new()),
            }
        }

        fn set_expires_in(&self, k: u8, v: String, d: Option<Duration>) {
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
}
