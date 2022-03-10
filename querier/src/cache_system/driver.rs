use std::{collections::HashMap, hash::Hash, sync::Arc};

use futures::{
    future::{BoxFuture, Shared},
    FutureExt, TryFutureExt,
};
use parking_lot::Mutex;
use tokio::{sync::oneshot::error::RecvError, task::JoinHandle};

use super::loader::Loader;

/// High-level cache implementation.
///
/// # Concurrency
/// Multiple cache requests for different keys can run at the same time. When data is requested for the same key the
/// underlying loader will only be polled once, even when the requests are made while the loader is still running.
///
/// # Cancellation
/// Canceling a [`get`](Self::get) request will NOT cancel the underlying loader. The data will still be cached.
///
/// # Panic
/// If the underlying loader panics, all currently running [`get`](Self::get) requests will panic. The data will NOT be cached.
#[derive(Debug)]
pub struct Cache<K, V>
where
    K: Clone + Eq + Hash + std::fmt::Debug + Send + 'static,
    V: Clone + std::fmt::Debug + Send + 'static,
{
    state: Arc<Mutex<CacheState<K, V>>>,
    loader: Arc<dyn Loader<K = K, V = V>>,
}

impl<K, V> Cache<K, V>
where
    K: Clone + Eq + Hash + std::fmt::Debug + Send + 'static,
    V: Clone + std::fmt::Debug + Send + 'static,
{
    /// Create new, empty cache with given loader function.
    pub fn new(loader: Arc<dyn Loader<K = K, V = V>>) -> Self {
        Self {
            state: Arc::new(Mutex::new(CacheState {
                cached_entries: HashMap::new(),
                running_queries: HashMap::new(),
            })),
            loader,
        }
    }

    /// Get value from cache.
    pub async fn get(&self, k: K) -> V {
        // place state locking into its own scope so it doesn't leak into the generator (async function)
        let receiver = {
            let mut state = self.state.lock();

            // check if the already cached this entry
            if let Some(v) = state.cached_entries.get(&k) {
                return v.clone();
            }

            // check if there is already a query for this key running
            if let Some((receiver, _handle)) = state.running_queries.get(&k) {
                receiver.clone()
            } else {
                // requires new query
                let (tx, rx) = tokio::sync::oneshot::channel();
                let receiver = rx
                    .map_ok(|v| Arc::new(Mutex::new(v)))
                    .map_err(Arc::new)
                    .boxed()
                    .shared();

                // need to wrap the query into a tokio task so that it doesn't get cancelled when this very request is canceled
                let state_captured = Arc::clone(&self.state);
                let loader = Arc::clone(&self.loader);
                let k_captured = k.clone();
                let handle = tokio::spawn(async move {
                    // need to clone K and bind it so rustc doesn't require `K: Sync`
                    let k_for_loader = k_captured.clone();

                    // execute the loader
                    // If we panic here then `tx` will be dropped and the receivers will be notified.
                    let v = loader.load(k_for_loader).await;

                    // broadcast result
                    // It's OK if the receiver side is gone. This might happen during shutdown
                    tx.send(v.clone()).ok();

                    // remove "running" state and store result
                    //
                    // Note: we need to manually drop the result of `.remove(...).expect(...)` here to convince rustc
                    //       that we don't need the shared future within the resulting tuple. The warning we would get
                    //       is:
                    //
                    //       warning: unused `futures::future::Shared` in tuple element 0 that must be used
                    let mut state = state_captured.lock();
                    drop(
                        state
                            .running_queries
                            .remove(&k_captured)
                            .expect("query should be running"),
                    );
                    state.cached_entries.insert(k_captured, v);
                });

                state.running_queries.insert(k, (receiver.clone(), handle));
                receiver
            }
        };

        receiver
            .await
            .expect("cache loader panicked, see logs")
            .lock()
            .clone()
    }
}

impl<K, V> Drop for Cache<K, V>
where
    K: Clone + Eq + Hash + std::fmt::Debug + Send + 'static,
    V: Clone + std::fmt::Debug + Send + 'static,
{
    fn drop(&mut self) {
        for (_k, (_receiver, handle)) in self.state.lock().running_queries.drain() {
            // It's unlikely that anyone is still using the shared receiver at this point, because Cache::get borrow
            // the self. If it is still in use, aborting the task will cancel the contained future which in turn will
            // drop the sender of the oneshot channel. The receivers will be notified.
            handle.abort();
        }
    }
}

/// A [`tokio::sync::oneshot::Receiver`] that can be cloned.
///
/// The types are:
/// - `Arc<Mutex<V>>`: Ensures that we can clone `V` without requiring `V: Sync`. At the same time the reference to `V`
///   (i.e. the `Arc`) must be cloneable for `Shared`
/// - `Arc<RecvError>`: Is required because `RecvError` is not `Clone` but `Shared` requires that.
/// - `BoxFuture`: The transformation from `Result<V, RecvError>` to `Result<Arc<Mutex<V>>, Arc<RecvError>>` results in
///   a kinda messy type and we wanna erase that.
/// - `Shared`: Allow the receiver to be cloned and be awaited from multiple places.
type SharedReceiver<V> = Shared<BoxFuture<'static, Result<Arc<Mutex<V>>, Arc<RecvError>>>>;

/// Inner cache state that is usually guarded by a lock.
///
/// The state parts must be updated in a consistent manner, i.e. while using the same lock guard.
#[derive(Debug)]
struct CacheState<K, V> {
    /// Cached entires (i.e. queries completed).
    cached_entries: HashMap<K, V>,

    /// Currently running queries indexed by cache key.
    ///
    /// For each query we have a receiver that can await the result as well as a handle for the task that is currently
    /// executing the query. The handle can be used to abort the running query, e.g. when dropping the cache.
    running_queries: HashMap<K, (SharedReceiver<V>, JoinHandle<()>)>,
}

#[cfg(test)]
mod tests {
    use std::{collections::HashSet, sync::Arc, time::Duration};

    use async_trait::async_trait;
    use tokio::sync::Notify;

    use super::*;

    #[tokio::test]
    async fn test_answers_are_correct() {
        let (cache, _loader) = setup();

        assert_eq!(cache.get(1).await, String::from("1"));
        assert_eq!(cache.get(2).await, String::from("2"));
    }

    #[tokio::test]
    async fn test_linear_memory() {
        let (cache, loader) = setup();

        assert_eq!(cache.get(1).await, String::from("1"));
        assert_eq!(cache.get(1).await, String::from("1"));
        assert_eq!(cache.get(2).await, String::from("2"));
        assert_eq!(cache.get(2).await, String::from("2"));
        assert_eq!(cache.get(1).await, String::from("1"));

        assert_eq!(loader.loaded(), vec![1, 2]);
    }

    #[tokio::test]
    async fn test_concurrent_query_loads_once() {
        let (cache, loader) = setup();

        loader.block();

        let cache_captured = Arc::clone(&cache);
        let handle_1 = tokio::spawn(async move { cache_captured.get(1).await });
        let handle_2 = tokio::spawn(async move { cache.get(1).await });

        tokio::time::sleep(Duration::from_millis(10)).await;
        // Shouldn't issue concurrent load requests for the same key
        let n_blocked = loader.unblock();
        assert_eq!(n_blocked, 1);

        assert_eq!(handle_1.await.unwrap(), String::from("1"));
        assert_eq!(handle_2.await.unwrap(), String::from("1"));

        assert_eq!(loader.loaded(), vec![1]);
    }

    #[tokio::test]
    async fn test_queries_are_parallelized() {
        let (cache, loader) = setup();

        loader.block();

        let cache_captured = Arc::clone(&cache);
        let handle_1 = tokio::spawn(async move { cache_captured.get(1).await });
        let cache_captured = Arc::clone(&cache);
        let handle_2 = tokio::spawn(async move { cache_captured.get(1).await });
        let handle_3 = tokio::spawn(async move { cache.get(2).await });

        tokio::time::sleep(Duration::from_millis(10)).await;

        let n_blocked = loader.unblock();
        assert_eq!(n_blocked, 2);

        assert_eq!(handle_1.await.unwrap(), String::from("1"));
        assert_eq!(handle_2.await.unwrap(), String::from("1"));
        assert_eq!(handle_3.await.unwrap(), String::from("2"));

        assert_eq!(loader.loaded(), vec![1, 2]);
    }

    #[tokio::test]
    async fn test_cancel_request() {
        let (cache, loader) = setup();

        loader.block();

        let cache_captured = Arc::clone(&cache);
        let handle_1 = tokio::spawn(async move { cache_captured.get(1).await });
        tokio::time::sleep(Duration::from_millis(10)).await;
        let handle_2 = tokio::spawn(async move { cache.get(1).await });

        tokio::time::sleep(Duration::from_millis(10)).await;

        // abort first handle
        handle_1.abort();
        tokio::time::sleep(Duration::from_millis(10)).await;

        let n_blocked = loader.unblock();
        assert_eq!(n_blocked, 1);

        assert_eq!(handle_2.await.unwrap(), String::from("1"));

        assert_eq!(loader.loaded(), vec![1]);
    }

    #[tokio::test]
    async fn test_panic_request() {
        let (cache, loader) = setup();

        loader.panic_once(1);
        loader.block();

        let cache_captured = Arc::clone(&cache);
        let handle_1 = tokio::spawn(async move { cache_captured.get(1).await });
        tokio::time::sleep(Duration::from_millis(10)).await;
        let cache_captured = Arc::clone(&cache);
        let handle_2 = tokio::spawn(async move { cache_captured.get(1).await });
        let handle_3 = tokio::spawn(async move { cache.get(2).await });

        tokio::time::sleep(Duration::from_millis(10)).await;

        let n_blocked = loader.unblock();
        assert_eq!(n_blocked, 2);

        // panic first handle
        handle_1.await.unwrap_err();
        tokio::time::sleep(Duration::from_millis(10)).await;

        // second handle should also panic
        handle_2.await.unwrap_err();

        // third handle should just work
        assert_eq!(handle_3.await.unwrap(), String::from("2"));

        assert_eq!(loader.loaded(), vec![1, 2]);
    }

    #[tokio::test]
    async fn test_drop_cancels_loader() {
        let (cache, loader) = setup();

        loader.block();

        let handle = tokio::spawn(async move { cache.get(1).await });

        tokio::time::sleep(Duration::from_millis(10)).await;

        handle.abort();

        tokio::time::sleep(Duration::from_millis(10)).await;

        assert_eq!(Arc::strong_count(&loader), 1);
    }

    fn setup() -> (Arc<Cache<u8, String>>, Arc<TestLoader>) {
        let loader = Arc::new(TestLoader::default());
        let cache = Arc::new(Cache::new(Arc::clone(&loader) as _));

        (cache, loader)
    }

    /// Flexible loader function for testing.
    #[derive(Debug, Default)]
    struct TestLoader {
        loaded: Mutex<Vec<u8>>,
        blocked: Mutex<Option<Arc<Notify>>>,
        panic: Mutex<HashSet<u8>>,
    }

    impl TestLoader {
        /// Panic when loading value for `k`.
        ///
        /// If this is used together with [`block`](Self::block), the panic will occur AFTER blocking.
        fn panic_once(&self, k: u8) {
            self.panic.lock().insert(k);
        }

        /// Block all [`load`](Self::load) requests until [`unblock`](Self::unblock) is called.
        ///
        /// If this is used together with [`panic_once`](Self::panic_once), the panic will occur AFTER blocking.
        fn block(&self) {
            let mut blocked = self.blocked.lock();
            assert!(blocked.is_none());
            *blocked = Some(Arc::new(Notify::new()));
        }

        /// Unblock all requests.
        ///
        /// Returns number of requests that were blocked.
        fn unblock(&self) -> usize {
            let handle = self.blocked.lock().take().unwrap();
            let blocked_count = Arc::strong_count(&handle) - 1;
            handle.notify_waiters();
            blocked_count
        }

        /// List all keys that were loaded.
        ///
        /// Contains duplicates if keys were loaded multiple times.
        fn loaded(&self) -> Vec<u8> {
            self.loaded.lock().clone()
        }
    }

    #[async_trait]
    impl Loader for TestLoader {
        type K = u8;
        type V = String;

        async fn load(&self, k: u8) -> String {
            self.loaded.lock().push(k);

            // need to capture the cloned notify handle, otherwise the lock guard leaks into the generator
            let maybe_block = self.blocked.lock().clone();
            if let Some(block) = maybe_block {
                block.notified().await;
            }

            // maybe panic
            if self.panic.lock().remove(&k) {
                panic!("test");
            }

            k.to_string()
        }
    }

    fn assert_send<T>()
    where
        T: Send,
    {
    }
    fn assert_sync<T>()
    where
        T: Sync,
    {
    }

    #[test]
    fn test_bounds() {
        assert_send::<Cache<u8, u8>>();
        assert_sync::<Cache<u8, u8>>();
    }
}
