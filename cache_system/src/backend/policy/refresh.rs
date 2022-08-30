//! Time-to-live handling.
use std::{
    collections::HashMap, fmt::Debug, hash::Hash, marker::PhantomData, sync::Arc, time::Duration,
};

use futures::{future::BoxFuture, stream::FuturesUnordered, FutureExt, StreamExt};
use iox_time::Time;
use metric::U64Counter;
use parking_lot::Mutex;
use tokio::{
    runtime::Handle,
    sync::{mpsc::UnboundedSender, Notify},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;

use crate::loader::Loader;

use super::{CallbackHandle, ChangeRequest, Subscriber};

/// Interface to provide refresh duration for a key-value pair.
pub trait RefreshDurationProvider: std::fmt::Debug + Send + Sync + 'static {
    /// Cache key.
    type K;

    /// Cached value.
    type V;

    /// When should the given key-value pair be refreshed?
    ///
    /// Return `None` for "never".
    ///
    /// The function is only called once for a newly cached key-value pair. This means:
    /// - There is no need in remembering the time of a given pair (e.g. you can safely always return a constant).
    /// - You cannot change the timings after the data was cached.
    ///
    /// Refresh is set to take place AT OR AFTER the provided duration.
    fn refresh_in(&self, k: &Self::K, v: &Self::V) -> Option<Duration>;
}

/// [`RefreshDurationProvider`] that never expires.
#[derive(Default)]
pub struct NeverRefreshProvider<K, V>
where
    K: 'static,
    V: 'static,
{
    // phantom data that is Send and Sync, see https://stackoverflow.com/a/50201389
    _k: PhantomData<fn() -> K>,
    _v: PhantomData<fn() -> V>,
}

impl<K, V> std::fmt::Debug for NeverRefreshProvider<K, V> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NeverRefreshProvider")
            .finish_non_exhaustive()
    }
}

impl<K, V> RefreshDurationProvider for NeverRefreshProvider<K, V> {
    type K = K;
    type V = V;

    fn refresh_in(&self, _k: &Self::K, _v: &Self::V) -> Option<Duration> {
        None
    }
}

/// [`RefreshDurationProvider`] that returns different values for `None`/`Some(...)` values.
pub struct OptionalValueRefreshDurationProvider<K, V>
where
    K: 'static,
    V: 'static,
{
    // phantom data that is Send and Sync, see https://stackoverflow.com/a/50201389
    _k: PhantomData<fn() -> K>,
    _v: PhantomData<fn() -> V>,

    t_none: Option<Duration>,
    t_some: Option<Duration>,
}

impl<K, V> OptionalValueRefreshDurationProvider<K, V>
where
    K: 'static,
    V: 'static,
{
    /// Create new provider with the given refresh duration for `None` and `Some(...)`.
    pub fn new(t_none: Option<Duration>, t_some: Option<Duration>) -> Self {
        Self {
            _k: PhantomData::default(),
            _v: PhantomData::default(),
            t_none,
            t_some,
        }
    }
}

impl<K, V> std::fmt::Debug for OptionalValueRefreshDurationProvider<K, V> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OptionalValueRefreshDurationProvider")
            .field("t_none", &self.t_none)
            .field("t_some", &self.t_some)
            .finish_non_exhaustive()
    }
}

impl<K, V> RefreshDurationProvider for OptionalValueRefreshDurationProvider<K, V> {
    type K = K;
    type V = Option<V>;

    fn refresh_in(&self, _k: &Self::K, v: &Self::V) -> Option<Duration> {
        match v {
            None => self.t_none,
            Some(_) => self.t_some,
        }
    }
}

/// Cache policy that implements refreshing.
#[derive(Debug)]
pub struct RefreshPolicy<K, V>
where
    K: Clone + Eq + Debug + Hash + Ord + Send + 'static,
    V: Clone + Debug + Send + 'static,
{
    refresh_duration_provider: Arc<dyn RefreshDurationProvider<K = K, V = V>>,
    loader: Arc<dyn Loader<K = K, V = V, Extra = ()>>,
    callback_handle: Arc<Mutex<CallbackHandle<K, V>>>,
    metric_refreshed: U64Counter,
    background_worker: JoinHandle<()>,
    timings: Arc<Mutex<HashMap<K, RefreshState>>>,
    tx_refresh_tasks: UnboundedSender<BoxFuture<'static, ()>>,
}

impl<K, V> RefreshPolicy<K, V>
where
    K: Clone + Eq + Debug + Hash + Ord + Send + 'static,
    V: Clone + Debug + Send + 'static,
{
    /// Create new refresh policy.
    #[allow(clippy::new_ret_no_self)]
    pub fn new(
        refresh_duration_provider: Arc<dyn RefreshDurationProvider<K = K, V = V>>,
        loader: Arc<dyn Loader<K = K, V = V, Extra = ()>>,
        name: &'static str,
        metric_registry: &metric::Registry,
        handle: &Handle,
    ) -> impl FnOnce(CallbackHandle<K, V>) -> Self {
        let idle_notify = Arc::new(Notify::new());
        Self::new_inner(
            refresh_duration_provider,
            loader,
            name,
            metric_registry,
            idle_notify,
            handle,
        )
    }

    #[allow(clippy::new_ret_no_self)]
    pub(crate) fn new_inner(
        refresh_duration_provider: Arc<dyn RefreshDurationProvider<K = K, V = V>>,
        loader: Arc<dyn Loader<K = K, V = V, Extra = ()>>,
        name: &'static str,
        metric_registry: &metric::Registry,
        idle_notify: Arc<Notify>,
        handle: &Handle,
    ) -> impl FnOnce(CallbackHandle<K, V>) -> Self {
        let metric_refreshed = metric_registry
            .register_metric::<U64Counter>("cache_refresh", "Number of cache refresh operations.")
            .recorder(&[("name", name)]);

        // clone handle for callback
        let handle = handle.clone();

        move |mut callback_handle| {
            callback_handle.execute_requests(vec![ChangeRequest::ensure_empty()]);

            let (tx_refresh_tasks, mut rx_refresh_tasks) = tokio::sync::mpsc::unbounded_channel();
            let timings: Arc<Mutex<HashMap<K, RefreshState>>> = Default::default();
            let callback_handle = Arc::new(Mutex::new(callback_handle));

            let background_worker = handle.spawn(async move {
                let mut refresh_tasks = FuturesUnordered::<BoxFuture<'static, ()>>::new();

                // We MUST NOT poll the empty task set because this would finish immediately. This will hot-loop
                // the loop. Even worse, since `FuturesUnodered` is not hooked up into tokio's (somewhat bizarre)
                // task preemtion system, tokio will poll this method here forever, essentially blocking this
                // thread.
                refresh_tasks.push(Box::pin(futures::future::pending()));

                // flag that remembers if we can notify idle observers again
                let mut can_notify_idle = true;

                loop {
                    // future that "guards" our idle notification to prevent hot loops (essentially blocking the entire
                    // tokio thread forever)
                    let fut_idle_notify_guard: BoxFuture<'static, ()> = if can_notify_idle {
                        Box::pin(futures::future::ready(()))
                    } else {
                        Box::pin(futures::future::pending())
                    };

                    tokio::select! {
                        biased;
                        _ = refresh_tasks.next() => {
                            // a refresh tasks finished

                            can_notify_idle = true;
                        }
                        maybe_new_task = rx_refresh_tasks.recv() => {
                            match maybe_new_task {
                                Some(new_task) => {
                                    refresh_tasks.push(new_task);
                                }
                                None => {
                                    // sender side (and therefore the whole policy) is gone, we shall exit
                                    return;
                                }
                            }

                            can_notify_idle = true;
                        }
                        _ = fut_idle_notify_guard => {
                            // no other jobs to do (this select is biased!), we inform the external test observer
                            idle_notify.notify_one();
                            can_notify_idle = false;
                        }
                    }
                }
            });

            Self {
                refresh_duration_provider,
                loader,
                callback_handle,
                metric_refreshed,
                background_worker,
                timings,
                tx_refresh_tasks,
            }
        }
    }

    /// Start refresh task for given key and return cancelation token for the task.
    ///
    /// You shall store the given token in [`RefreshState`].
    #[must_use]
    fn refresh(&self, k: K) -> CancellationToken {
        let loader = Arc::clone(&self.loader);
        let callback_handle = Arc::clone(&self.callback_handle);
        let cancelled = CancellationToken::default();

        let cancelled_captured = cancelled.clone();
        let fut = async move {
            // some `let`-dance so that rustc does not complain that `&K` is not `Send`
            let k_for_loader = k.clone();
            let v = loader.load(k_for_loader, ()).await;

            let mut callback_handle = callback_handle.lock();
            callback_handle.execute_requests(vec![ChangeRequest::from_fn(|backend| {
                // Here we have the PolicyBackend implicit lock. There is no way our Subscriber can be
                // active here, but we need to check if we have been canceled one last time.
                if cancelled_captured.is_cancelled() {
                    return;
                }

                backend.set(k, v);
            })]);

            // there is NO need to update our own `timings` after this refresh because this very Subscriber
            // will also get a `set` notification and update its timing table accordingly
        };

        let cancelled_captured = cancelled.clone();
        let fut = async move {
            tokio::select! {
                _ = cancelled_captured.cancelled() => {},
                _ = fut => {}
            }
        }
        .boxed();

        self.tx_refresh_tasks
            .send(fut)
            .map_err(|_| ())
            .expect("background worker alive");

        self.metric_refreshed.inc(1);
        cancelled
    }
}

impl<K, V> Drop for RefreshPolicy<K, V>
where
    K: Clone + Eq + Debug + Hash + Ord + Send + 'static,
    V: Clone + Debug + Send + 'static,
{
    fn drop(&mut self) {
        self.background_worker.abort();
    }
}

impl<K, V> Subscriber for RefreshPolicy<K, V>
where
    K: Clone + Eq + Debug + Hash + Ord + Send + 'static,
    V: Clone + Debug + Send + 'static,
{
    type K = K;
    type V = V;

    fn get(&mut self, k: &Self::K, now: Time) -> Vec<ChangeRequest<'static, Self::K, Self::V>> {
        let mut timings = self.timings.lock();

        // Does this entry exists and is there no a refresh operation running?
        if let Some(RefreshState {
            t,
            running_refresh: running_refresh @ None,
        }) = timings.get_mut(k)
        {
            // Is it time to refresh?
            if *t <= now {
                *running_refresh = Some(self.refresh(k.clone()));
            }
        }

        vec![]
    }

    fn set(
        &mut self,
        k: Self::K,
        v: Self::V,
        now: Time,
    ) -> Vec<ChangeRequest<'static, Self::K, Self::V>> {
        let d = self.refresh_duration_provider.refresh_in(&k, &v);

        let mut timings = self.timings.lock();

        // ignore any entries that don't require any work
        if let Some(t) = d.and_then(|d| now.checked_add(d)) {
            let state = RefreshState {
                t,
                running_refresh: None,
            };

            timings.insert(k, state);
        } else {
            // need to remove potentially existing entry that had some refresh set
            timings.remove(&k);

            // the removal drops the RefreshState which triggers a cancelation for any potentially running
            // refresh operation
        }

        vec![]
    }

    fn remove(&mut self, k: &Self::K, _now: Time) -> Vec<ChangeRequest<'static, Self::K, Self::V>> {
        let mut timings = self.timings.lock();
        timings.remove(k);

        // the removal automatically triggered a cancelation for any potentially running refresh operation

        vec![]
    }
}

/// Current state of an entry managed by the refresh policy.
#[derive(Debug)]
struct RefreshState {
    /// When to refresh or expire.
    t: Time,

    /// Cancellation token for a potentially running refresh operation.
    ///
    /// This token will be triggered on [`drop`](Drop::drop).
    running_refresh: Option<CancellationToken>,
}

impl Drop for RefreshState {
    fn drop(&mut self) {
        if let Some(token) = &self.running_refresh {
            token.cancel();
        }
    }
}

pub mod test_util {
    //! Testing utilities for refresh policy.

    use async_trait::async_trait;
    use tokio::sync::Barrier;

    use super::*;

    /// Easy-to-control [`RefreshDurationProvider`].
    #[derive(Debug, Default)]
    pub struct TestRefreshDurationProvider {
        times: Mutex<HashMap<(u8, String), Option<Duration>>>,
    }

    impl TestRefreshDurationProvider {
        /// Create new, empty provider.
        pub fn new() -> Self {
            Self::default()
        }

        /// Specify a refresh duration for a given key-value pair.
        ///
        /// Existing values will be overridden.
        pub fn set_refresh_in(&self, k: u8, v: String, d: Option<Duration>) {
            self.times.lock().insert((k, v), d);
            // do NOT check if there was already a value set because we allow overrides
        }
    }

    impl RefreshDurationProvider for TestRefreshDurationProvider {
        type K = u8;
        type V = String;

        fn refresh_in(&self, k: &Self::K, v: &Self::V) -> Option<Duration> {
            *self
                .times
                .lock()
                .get(&(*k, v.clone()))
                .expect("refresh time not mocked")
        }
    }

    #[derive(Debug)]
    struct TestLoaderResponse {
        v: String,
        block: Option<Arc<Barrier>>,
    }

    /// An easy-to-mock [`Loader`].
    #[derive(Debug, Default)]
    pub struct TestLoader {
        data: Mutex<HashMap<u8, Vec<TestLoaderResponse>>>,
    }

    impl TestLoader {
        /// Mock next value for given key-value pair.
        pub fn mock_next(&self, k: u8, v: String) {
            self.mock_inner(k, TestLoaderResponse { v, block: None });
        }

        /// Block on next load for given key-value pair.
        ///
        /// Return a barrier that can be used to unblock the load.
        #[must_use]
        pub fn block_next(&self, k: u8, v: String) -> Arc<Barrier> {
            let block = Arc::new(Barrier::new(2));
            self.mock_inner(
                k,
                TestLoaderResponse {
                    v,
                    block: Some(Arc::clone(&block)),
                },
            );
            block
        }

        fn mock_inner(&self, k: u8, response: TestLoaderResponse) {
            let mut data = self.data.lock();
            data.entry(k).or_default().push(response);
        }
    }

    impl Drop for TestLoader {
        fn drop(&mut self) {
            // prevent double-panic (i.e. aborts)
            if !std::thread::panicking() {
                for entries in self.data.lock().values() {
                    assert!(entries.is_empty(), "mocked response left");
                }
            }
        }
    }

    #[async_trait]
    impl Loader for TestLoader {
        type K = u8;
        type V = String;
        type Extra = ();

        async fn load(&self, k: Self::K, _extra: Self::Extra) -> Self::V {
            let TestLoaderResponse { v, block } = {
                let mut guard = self.data.lock();
                let entries = guard.get_mut(&k).expect("entry not mocked");

                assert!(!entries.is_empty(), "no mocked response left");

                entries.remove(0)
            };

            if let Some(block) = block {
                block.wait().await;
            }

            v
        }
    }

    /// Some extensions for [`Notify`].
    pub trait NotifyExt {
        /// Wait for notification but panic after a short timeout.
        fn notified_with_timeout(&self) -> BoxFuture<'_, ()>;

        /// Ensure that we are NOT notified.
        fn not_notified(&self) -> BoxFuture<'_, ()>;
    }

    impl NotifyExt for Notify {
        fn notified_with_timeout(&self) -> BoxFuture<'_, ()> {
            Box::pin(async {
                tokio::time::timeout(Duration::from_secs(1), self.notified())
                    .await
                    .unwrap();
            })
        }

        fn not_notified(&self) -> BoxFuture<'_, ()> {
            Box::pin(async {
                tokio::time::timeout(Duration::from_millis(10), self.notified())
                    .await
                    .unwrap_err();
            })
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        #[should_panic(expected = "refresh time not mocked")]
        fn test_provider_panic_not_mocked() {
            let provider = TestRefreshDurationProvider::default();
            provider.refresh_in(&1, &String::from("foo"));
        }

        #[test]
        fn test_provider_mocking() {
            let provider = TestRefreshDurationProvider::default();

            provider.set_refresh_in(1, String::from("a"), None);
            provider.set_refresh_in(1, String::from("b"), Some(Duration::from_secs(1)));
            provider.set_refresh_in(2, String::from("a"), Some(Duration::from_secs(2)));

            assert_eq!(provider.refresh_in(&1, &String::from("a")), None,);
            assert_eq!(
                provider.refresh_in(&1, &String::from("b")),
                Some(Duration::from_secs(1)),
            );
            assert_eq!(
                provider.refresh_in(&2, &String::from("a")),
                Some(Duration::from_secs(2)),
            );

            // replace
            provider.set_refresh_in(1, String::from("a"), Some(Duration::from_secs(3)));
            assert_eq!(
                provider.refresh_in(&1, &String::from("a")),
                Some(Duration::from_secs(3)),
            );
        }

        #[tokio::test]
        #[should_panic(expected = "entry not mocked")]
        async fn test_loader_panic_entry_unknown() {
            let loader = TestLoader::default();
            loader.load(1, ()).await;
        }

        #[tokio::test]
        #[should_panic(expected = "no mocked response left")]
        async fn test_loader_panic_no_mocked_reponse_left() {
            let loader = TestLoader::default();
            loader.mock_next(1, String::from("foo"));
            loader.load(1, ()).await;
            loader.load(1, ()).await;
        }

        #[test]
        #[should_panic(expected = "mocked response left")]
        fn test_loader_panic_requests_left() {
            let loader = TestLoader::default();
            loader.mock_next(1, String::from("foo"));
        }

        #[test]
        #[should_panic(expected = "panic-by-choice")]
        fn test_loader_no_double_panic() {
            let loader = TestLoader::default();
            loader.mock_next(1, String::from("foo"));
            panic!("panic-by-choice");
        }

        #[tokio::test]
        async fn test_loader_nonblocking_mock() {
            let loader = TestLoader::default();

            loader.mock_next(1, String::from("foo"));
            loader.mock_next(1, String::from("bar"));
            loader.mock_next(2, String::from("baz"));

            assert_eq!(loader.load(1, ()).await, String::from("foo"));
            assert_eq!(loader.load(2, ()).await, String::from("baz"));
            assert_eq!(loader.load(1, ()).await, String::from("bar"));
        }

        #[tokio::test]
        async fn test_loader_blocking_mock() {
            let loader = Arc::new(TestLoader::default());

            let loader_barrier = loader.block_next(1, String::from("foo"));
            loader.mock_next(2, String::from("bar"));

            let is_blocked_barrier = Arc::new(Barrier::new(2));

            let loader_captured = Arc::clone(&loader);
            let is_blocked_barrier_captured = Arc::clone(&is_blocked_barrier);
            let handle = tokio::task::spawn(async move {
                let mut fut_load = loader_captured.load(1, ()).fuse();

                futures::select_biased! {
                    _ = fut_load => {
                        panic!("should not finish");
                    }
                    _ = is_blocked_barrier_captured.wait().fuse() => {}
                }
                fut_load.await
            });

            is_blocked_barrier.wait().await;

            // can still load other entries
            assert_eq!(loader.load(2, ()).await, String::from("bar"));

            // unblock load
            loader_barrier.wait().await;
            assert_eq!(handle.await.unwrap(), String::from("foo"));
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, time::Duration};

    use iox_time::MockProvider;
    use metric::{Observation, RawReporter};

    use crate::backend::{
        policy::{refresh::test_util::NotifyExt, PolicyBackend},
        CacheBackend,
    };

    use super::{
        test_util::{TestLoader, TestRefreshDurationProvider},
        *,
    };

    #[test]
    fn test_never_refresh_provider() {
        let provider = NeverRefreshProvider::<u8, i8>::default();
        assert_eq!(provider.refresh_in(&1, &2), None);
    }

    #[test]
    fn test_optional_value_ttl_provider() {
        let t_none = Some(Duration::from_secs(1));
        let t_some = Some(Duration::from_secs(2));
        let provider = OptionalValueRefreshDurationProvider::<u8, i8>::new(t_none, t_some);
        assert_eq!(provider.refresh_in(&1, &None), t_none);
        assert_eq!(provider.refresh_in(&1, &Some(2)), t_some);
    }

    #[tokio::test]
    #[should_panic(expected = "inner backend is not empty")]
    async fn test_panic_inner_not_empty() {
        let refresh_duration_provider = Arc::new(TestRefreshDurationProvider::new());
        let metric_registry = metric::Registry::new();

        let time_provider = Arc::new(MockProvider::new(Time::MIN));
        let loader = Arc::new(TestLoader::default());
        let mut backend = PolicyBackend::new(Box::new(HashMap::<u8, String>::new()), time_provider);
        let policy_constructor = RefreshPolicy::new(
            refresh_duration_provider,
            loader,
            "my_cache",
            &metric_registry,
            &Handle::current(),
        );
        backend.add_policy(|mut handle| {
            handle.execute_requests(vec![ChangeRequest::set(1, String::from("foo"))]);
            policy_constructor(handle)
        });
    }

    #[tokio::test]
    async fn test_duration_overflow() {
        let refresh_duration_provider = Arc::new(TestRefreshDurationProvider::new());
        refresh_duration_provider.set_refresh_in(1, String::from("a"), Some(Duration::MAX));

        let metric_registry = metric::Registry::new();
        let time_provider = Arc::new(MockProvider::new(Time::MAX - Duration::from_secs(1)));
        let loader = Arc::new(TestLoader::default());
        let mut backend = PolicyBackend::new(
            Box::new(HashMap::<u8, String>::new()),
            Arc::clone(&time_provider) as _,
        );
        backend.add_policy(RefreshPolicy::new(
            refresh_duration_provider,
            loader,
            "my_cache",
            &metric_registry,
            &Handle::current(),
        ));

        backend.set(1, String::from("a"));

        time_provider.inc(Duration::from_secs(1));
        assert_eq!(backend.get(&1), Some(String::from("a")));
        assert_eq!(get_refresh_metric(&metric_registry), 0);
    }

    #[tokio::test]
    async fn test_refresh() {
        let TestState {
            mut backend,
            refresh_duration_provider,
            time_provider,
            loader,
            metric_registry,
            notify_idle,
            ..
        } = TestState::new();

        loader.mock_next(1, String::from("foo"));
        loader.mock_next(1, String::from("bar"));
        refresh_duration_provider.set_refresh_in(
            1,
            String::from("a"),
            Some(Duration::from_secs(1)),
        );
        refresh_duration_provider.set_refresh_in(
            1,
            String::from("foo"),
            Some(Duration::from_secs(2)),
        );
        refresh_duration_provider.set_refresh_in(1, String::from("bar"), None);
        backend.set(1, String::from("a"));

        // still the same key
        assert_eq!(backend.get(&1), Some(String::from("a")));

        time_provider.inc(Duration::from_secs(1));
        notify_idle.notified_with_timeout().await;
        assert_eq!(get_refresh_metric(&metric_registry), 0);

        // no refresh yet because key was not accessed after refresh window started
        assert_eq!(backend.get(&1), Some(String::from("a")));
        assert_eq!(get_refresh_metric(&metric_registry), 1);

        // update arrived
        notify_idle.notified_with_timeout().await;
        assert_eq!(backend.get(&1), Some(String::from("foo")));

        // nothing to refresh yet
        notify_idle.not_notified().await;
        assert_eq!(backend.get(&1), Some(String::from("foo")));

        // just bumping the refresh by the old refresh timer won't do anything (we need 2 seconds this time)
        time_provider.inc(Duration::from_secs(1));
        notify_idle.not_notified().await;
        assert_eq!(backend.get(&1), Some(String::from("foo")));
        notify_idle.not_notified().await;
        assert_eq!(backend.get(&1), Some(String::from("foo")));

        // try a 2nd update
        time_provider.inc(Duration::from_secs(1));
        notify_idle.not_notified().await;
        assert_eq!(backend.get(&1), Some(String::from("foo")));
        assert_eq!(get_refresh_metric(&metric_registry), 2);
        notify_idle.notified_with_timeout().await;
        assert_eq!(backend.get(&1), Some(String::from("bar")));
    }

    #[tokio::test]
    async fn test_do_not_start_refresh_while_one_is_running() {
        let TestState {
            mut backend,
            refresh_duration_provider,
            time_provider,
            loader,
            notify_idle,
            ..
        } = TestState::new();

        let barrier = loader.block_next(1, String::from("foo"));
        refresh_duration_provider.set_refresh_in(
            1,
            String::from("a"),
            Some(Duration::from_secs(1)),
        );
        refresh_duration_provider.set_refresh_in(1, String::from("foo"), None);
        backend.set(1, String::from("a"));

        time_provider.inc(Duration::from_secs(1));
        assert_eq!(backend.get(&1), Some(String::from("a")));
        notify_idle.notified_with_timeout().await;
        // if this would start another refresh then the loader would panic because we've only mocked a single request
        assert_eq!(backend.get(&1), Some(String::from("a")));

        barrier.wait().await;
        notify_idle.notified_with_timeout().await;
        assert_eq!(backend.get(&1), Some(String::from("foo")));
    }

    #[tokio::test]
    async fn test_refresh_does_not_override_new_entries() {
        let TestState {
            mut backend,
            refresh_duration_provider,
            time_provider,
            loader,
            notify_idle,
            ..
        } = TestState::new();

        let barrier = loader.block_next(1, String::from("foo"));
        refresh_duration_provider.set_refresh_in(
            1,
            String::from("a"),
            Some(Duration::from_secs(1)),
        );
        refresh_duration_provider.set_refresh_in(
            1,
            String::from("b"),
            Some(Duration::from_secs(1)),
        );
        backend.set(1, String::from("a"));

        // perform refresh
        time_provider.inc(Duration::from_secs(1));
        assert_eq!(backend.get(&1), Some(String::from("a")));
        notify_idle.notified_with_timeout().await;
        assert_eq!(backend.get(&1), Some(String::from("a")));

        backend.set(1, String::from("b"));
        barrier.wait().await;
        notify_idle.notified_with_timeout().await;
        assert_eq!(backend.get(&1), Some(String::from("b")));
    }

    #[tokio::test]
    async fn test_remove_cancels_loader() {
        let TestState {
            mut backend,
            refresh_duration_provider,
            time_provider,
            loader,
            notify_idle,
            ..
        } = TestState::new();

        let barrier = loader.block_next(1, String::from("foo"));
        refresh_duration_provider.set_refresh_in(
            1,
            String::from("a"),
            Some(Duration::from_secs(1)),
        );
        backend.set(1, String::from("a"));

        // perform refresh
        time_provider.inc(Duration::from_secs(1));
        assert_eq!(backend.get(&1), Some(String::from("a")));
        notify_idle.notified_with_timeout().await;
        assert_eq!(backend.get(&1), Some(String::from("a")));

        backend.remove(&1);
        notify_idle.notified_with_timeout().await;
        assert_eq!(Arc::strong_count(&barrier), 1);
    }

    #[tokio::test]
    async fn test_override_with_no_refresh() {
        let TestState {
            mut backend,
            refresh_duration_provider,
            time_provider,
            loader,
            notify_idle,
            ..
        } = TestState::new();

        let barrier = loader.block_next(1, String::from("foo"));
        refresh_duration_provider.set_refresh_in(
            1,
            String::from("a"),
            Some(Duration::from_secs(1)),
        );
        refresh_duration_provider.set_refresh_in(1, String::from("b"), None);
        backend.set(1, String::from("a"));

        // perform refresh
        time_provider.inc(Duration::from_secs(1));
        assert_eq!(backend.get(&1), Some(String::from("a")));
        notify_idle.notified_with_timeout().await;
        assert_eq!(backend.get(&1), Some(String::from("a")));

        backend.set(1, String::from("b"));
        barrier.wait().await;

        // no refresh
        time_provider.inc(Duration::from_secs(1));
        assert_eq!(backend.get(&1), Some(String::from("b")));
        notify_idle.notified_with_timeout().await;
        assert_eq!(backend.get(&1), Some(String::from("b")));
    }

    #[tokio::test]
    async fn test_generic_backend() {
        use crate::backend::test_util::test_generic;

        test_generic(|| {
            let ttl_provider = Arc::new(NeverRefreshProvider::default());
            let time_provider = Arc::new(MockProvider::new(Time::MIN));
            let metric_registry = metric::Registry::new();
            let loader = Arc::new(TestLoader::default());
            let mut backend =
                PolicyBackend::new(Box::new(HashMap::<u8, String>::new()), time_provider);

            backend.add_policy(RefreshPolicy::new(
                Arc::clone(&ttl_provider) as _,
                loader,
                "my_cache",
                &metric_registry,
                &Handle::current(),
            ));
            backend
        });
    }

    struct TestState {
        backend: PolicyBackend<u8, String>,
        metric_registry: metric::Registry,
        refresh_duration_provider: Arc<TestRefreshDurationProvider>,
        time_provider: Arc<MockProvider>,
        loader: Arc<TestLoader>,
        notify_idle: Arc<Notify>,
    }

    impl TestState {
        fn new() -> Self {
            let refresh_duration_provider = Arc::new(TestRefreshDurationProvider::new());
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

            Self {
                backend,
                metric_registry,
                refresh_duration_provider,
                time_provider,
                loader,
                notify_idle,
            }
        }
    }

    fn get_refresh_metric(metric_registry: &metric::Registry) -> u64 {
        let mut reporter = RawReporter::default();
        metric_registry.report(&mut reporter);
        let observation = reporter
            .metric("cache_refresh")
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
