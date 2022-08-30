//! Backend that supports custom removal / expiry of keys
use metric::U64Counter;
use parking_lot::Mutex;
use std::{fmt::Debug, hash::Hash, marker::PhantomData, sync::Arc};

use super::{CallbackHandle, ChangeRequest, Subscriber};

/// Allows explicitly removing entries from the cache.
#[derive(Debug, Clone)]
pub struct RemoveIfPolicy<K, V>
where
    K: Clone + Eq + Debug + Hash + Ord + Send + 'static,
    V: Clone + Debug + Send + 'static,
{
    // the policy itself doesn't do anything, the handles will do all the work
    _phantom: PhantomData<(K, V)>,
}

impl<K, V> RemoveIfPolicy<K, V>
where
    K: Clone + Eq + Debug + Hash + Ord + Send + 'static,
    V: Clone + Debug + Send + 'static,
{
    /// Create new policy.
    ///
    /// This returns the policy constructor which shall be pass to
    /// [`PolicyBackend::add_policy`] and handle that can be used to remove entries.
    ///
    /// Note that as long as the policy constructor is NOT passed to [`PolicyBackend::add_policy`], the operations on
    /// the handle are essentially no-ops (i.e. they will not remove anything).
    ///
    /// [`PolicyBackend::add_policy`]: super::PolicyBackend::add_policy
    pub fn create_constructor_and_handle(
        name: &'static str,
        metric_registry: &metric::Registry,
    ) -> (
        impl FnOnce(CallbackHandle<K, V>) -> Self,
        RemoveIfHandle<K, V>,
    ) {
        let metric_removed_by_predicate = metric_registry
            .register_metric::<U64Counter>(
                "cache_removed_by_custom_condition",
                "Number of entries removed from a cache via a custom condition",
            )
            .recorder(&[("name", name)]);

        let handle = RemoveIfHandle {
            callback_handle: Arc::new(Mutex::new(None)),
            metric_removed_by_predicate,
        };
        let handle_captured = handle.clone();

        let policy_constructor = move |callback_handle| {
            *handle_captured.callback_handle.lock() = Some(callback_handle);
            Self {
                _phantom: PhantomData::default(),
            }
        };

        (policy_constructor, handle)
    }
}

impl<K, V> Subscriber for RemoveIfPolicy<K, V>
where
    K: Clone + Eq + Debug + Hash + Ord + Send + 'static,
    V: Clone + Debug + Send + 'static,
{
    type K = K;
    type V = V;
}

/// Handle created by [`RemoveIfPolicy`] that can be used to evict data from caches.
///
/// The handle can be cloned freely. All clones will refer to the same underlying backend.
#[derive(Debug, Clone)]
pub struct RemoveIfHandle<K, V>
where
    K: Clone + Eq + Debug + Hash + Ord + Send + 'static,
    V: Clone + Debug + Send + 'static,
{
    callback_handle: Arc<Mutex<Option<CallbackHandle<K, V>>>>,
    metric_removed_by_predicate: U64Counter,
}

impl<K, V> RemoveIfHandle<K, V>
where
    K: Clone + Eq + Debug + Hash + Ord + Send + 'static,
    V: Clone + Debug + Send + 'static,
{
    /// "remove" a key (aka remove it from the shared backend) if the
    /// specified predicate is true. If the key is removed return
    /// true, otherwise return false
    ///
    /// Note that the predicate function is called while the lock is
    /// held (and thus the inner backend can't be concurrently accessed
    pub fn remove_if<P>(&self, k: &K, predicate: P) -> bool
    where
        P: Fn(V) -> bool + Send,
    {
        let mut guard = self.callback_handle.lock();
        let handle = match guard.as_mut() {
            Some(handle) => handle,
            None => return false,
        };

        let metric_removed_by_predicate = self.metric_removed_by_predicate.clone();
        let mut removed = false;
        let removed_captured = &mut removed;
        let k = k.clone();
        handle.execute_requests(vec![ChangeRequest::from_fn(move |backend| {
            if let Some(v) = backend.get(&k) {
                if predicate(v) {
                    metric_removed_by_predicate.inc(1);
                    backend.remove(&k);
                    *removed_captured = true;
                }
            }
        })]);

        removed
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use iox_time::{MockProvider, Time};
    use metric::{Observation, RawReporter};

    use crate::backend::{policy::PolicyBackend, CacheBackend};

    use super::*;

    #[test]
    fn test_generic_backend() {
        use crate::backend::test_util::test_generic;

        test_generic(|| {
            let metric_registry = metric::Registry::new();
            let time_provider = Arc::new(MockProvider::new(Time::MIN));
            let mut backend =
                PolicyBackend::new(Box::new(HashMap::<u8, String>::new()), time_provider);
            let (policy_constructor, _handle) =
                RemoveIfPolicy::create_constructor_and_handle("my_cache", &metric_registry);
            backend.add_policy(policy_constructor);
            backend
        });
    }

    #[test]
    fn test_remove_if() {
        let metric_registry = metric::Registry::new();
        let time_provider = Arc::new(MockProvider::new(Time::MIN));
        let mut backend = PolicyBackend::new(Box::new(HashMap::<u8, String>::new()), time_provider);
        let (policy_constructor, handle) =
            RemoveIfPolicy::create_constructor_and_handle("my_cache", &metric_registry);
        backend.add_policy(policy_constructor);
        backend.set(1, "foo".into());
        backend.set(2, "bar".into());

        assert_eq!(get_removed_metric(&metric_registry), 0);

        assert!(!handle.remove_if(&1, |v| v == "zzz"));
        assert_eq!(backend.get(&1), Some("foo".into()));
        assert_eq!(backend.get(&2), Some("bar".into()));
        assert_eq!(get_removed_metric(&metric_registry), 0);

        assert!(handle.remove_if(&1, |v| v == "foo"));
        assert_eq!(backend.get(&1), None);
        assert_eq!(backend.get(&2), Some("bar".into()));
        assert_eq!(get_removed_metric(&metric_registry), 1);

        assert!(!handle.remove_if(&1, |v| v == "bar"));
        assert_eq!(backend.get(&1), None);
        assert_eq!(backend.get(&2), Some("bar".into()));
        assert_eq!(get_removed_metric(&metric_registry), 1);
    }

    #[test]
    fn test_not_linked() {
        let metric_registry = metric::Registry::new();
        let (_policy_constructor, handle) =
            RemoveIfPolicy::<u8, String>::create_constructor_and_handle(
                "my_cache",
                &metric_registry,
            );

        assert_eq!(get_removed_metric(&metric_registry), 0);

        assert!(!handle.remove_if(&1, |v| v == "zzz"));
        assert_eq!(get_removed_metric(&metric_registry), 0);
    }

    fn get_removed_metric(metric_registry: &metric::Registry) -> u64 {
        let mut reporter = RawReporter::default();
        metric_registry.report(&mut reporter);
        let observation = reporter
            .metric("cache_removed_by_custom_condition")
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
