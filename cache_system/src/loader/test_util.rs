use std::{collections::HashMap, fmt::Debug, hash::Hash, sync::Arc};

use async_trait::async_trait;
use parking_lot::Mutex;
use tokio::sync::{Barrier, Notify};

use super::Loader;

#[derive(Debug)]
enum TestLoaderResponse<V> {
    Answer { v: V, block: Option<Arc<Barrier>> },
    Panic,
}

/// An easy-to-mock [`Loader`].
#[derive(Debug, Default)]
pub struct TestLoader<K = u8, Extra = bool, V = String>
where
    K: Clone + Debug + Eq + Hash + Send + 'static,
    Extra: Clone + Debug + Send + 'static,
    V: Clone + Debug + Send + 'static,
{
    responses: Mutex<HashMap<K, Vec<TestLoaderResponse<V>>>>,
    blocked: Mutex<Option<Arc<Notify>>>,
    loaded: Mutex<Vec<(K, Extra)>>,
}

impl<K, V, Extra> TestLoader<K, Extra, V>
where
    K: Clone + Debug + Eq + Hash + Send + 'static,
    Extra: Clone + Debug + Send + 'static,
    V: Clone + Debug + Send + 'static,
{
    /// Mock next value for given key-value pair.
    pub fn mock_next(&self, k: K, v: V) {
        self.mock_inner(k, TestLoaderResponse::Answer { v, block: None });
    }

    /// Block on next load for given key-value pair.
    ///
    /// Return a barrier that can be used to unblock the load.
    #[must_use]
    pub fn block_next(&self, k: K, v: V) -> Arc<Barrier> {
        let block = Arc::new(Barrier::new(2));
        self.mock_inner(
            k,
            TestLoaderResponse::Answer {
                v,
                block: Some(Arc::clone(&block)),
            },
        );
        block
    }

    /// Panic when loading value for `k`.
    ///
    /// If this is used together with [`block_global`](Self::block_global), the panic will occur AFTER
    /// blocking.
    pub fn panic_next(&self, k: K) {
        self.mock_inner(k, TestLoaderResponse::Panic);
    }

    fn mock_inner(&self, k: K, response: TestLoaderResponse<V>) {
        let mut responses = self.responses.lock();
        responses.entry(k).or_default().push(response);
    }

    /// Block all [`load`](Self::load) requests until [`unblock`](Self::unblock) is called.
    ///
    /// If this is used together with [`panic_once`](Self::panic_once), the panic will occur
    /// AFTER blocking.
    pub fn block_global(&self) {
        let mut blocked = self.blocked.lock();
        assert!(blocked.is_none());
        *blocked = Some(Arc::new(Notify::new()));
    }

    /// Unblock all requests.
    ///
    /// Returns number of requests that were blocked.
    pub fn unblock_global(&self) -> usize {
        let handle = self.blocked.lock().take().unwrap();
        let blocked_count = Arc::strong_count(&handle) - 1;
        handle.notify_waiters();
        blocked_count
    }

    /// List all keys that were loaded.
    ///
    /// Contains duplicates if keys were loaded multiple times.
    pub fn loaded(&self) -> Vec<(K, Extra)> {
        self.loaded.lock().clone()
    }
}

impl<K, Extra, V> Drop for TestLoader<K, Extra, V>
where
    K: Clone + Debug + Eq + Hash + Send + 'static,
    Extra: Clone + Debug + Send + 'static,
    V: Clone + Debug + Send + 'static,
{
    fn drop(&mut self) {
        // prevent double-panic (i.e. aborts)
        if !std::thread::panicking() {
            for entries in self.responses.lock().values() {
                assert!(entries.is_empty(), "mocked response left");
            }
        }
    }
}

#[async_trait]
impl<K, V, Extra> Loader for TestLoader<K, Extra, V>
where
    K: Clone + Debug + Eq + Hash + Send + 'static,
    Extra: Clone + Debug + Send + 'static,
    V: Clone + Debug + Send + 'static,
{
    type K = K;
    type Extra = Extra;
    type V = V;

    async fn load(&self, k: Self::K, extra: Self::Extra) -> Self::V {
        self.loaded.lock().push((k.clone(), extra));

        // need to capture the cloned notify handle, otherwise the lock guard leaks into the
        // generator
        let maybe_block = self.blocked.lock().clone();
        if let Some(block) = maybe_block {
            block.notified().await;
        }

        let response = {
            let mut guard = self.responses.lock();
            let entries = guard.get_mut(&k).expect("entry not mocked");

            assert!(!entries.is_empty(), "no mocked response left");

            entries.remove(0)
        };

        match response {
            TestLoaderResponse::Answer { v, block } => {
                if let Some(block) = block {
                    block.wait().await;
                }

                v
            }
            TestLoaderResponse::Panic => {
                panic!("test")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use futures::FutureExt;

    use super::*;

    #[tokio::test]
    #[should_panic(expected = "entry not mocked")]
    async fn test_loader_panic_entry_unknown() {
        let loader = TestLoader::<u8, (), String>::default();
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
        let loader = TestLoader::<u8, (), String>::default();
        loader.mock_next(1, String::from("foo"));
    }

    #[test]
    #[should_panic(expected = "panic-by-choice")]
    fn test_loader_no_double_panic() {
        let loader = TestLoader::<u8, (), String>::default();
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
