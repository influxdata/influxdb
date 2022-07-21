use std::{collections::HashSet, sync::Arc, time::Duration};

use async_trait::async_trait;
use futures::{Future, FutureExt};
use parking_lot::Mutex;
use tokio::{
    sync::{Barrier, Notify},
    task::JoinHandle,
};

use crate::{
    cache::{CacheGetStatus, CachePeekStatus},
    loader::Loader,
};

use super::Cache;

/// Interface between generic tests and a concrete cache type.
pub trait TestAdapter: Send + Sync {
    /// Cache type.
    type Cache: Cache<K = u8, V = String, GetExtra = bool, PeekExtra = ()>;

    /// Create new cache with given loader.
    fn construct(&self, loader: Arc<TestLoader>) -> Arc<Self::Cache>;
}

/// Setup test.
fn setup<T>(adapter: &T) -> (Arc<T::Cache>, Arc<TestLoader>)
where
    T: TestAdapter,
{
    let loader = Arc::new(TestLoader::default());
    let cache = adapter.construct(Arc::clone(&loader));
    (cache, loader)
}

pub async fn run_test_generic<T>(adapter: T)
where
    T: TestAdapter,
{
    test_answers_are_correct(&adapter).await;
    test_linear_memory(&adapter).await;
    test_concurrent_query_loads_once(&adapter).await;
    test_queries_are_parallelized(&adapter).await;
    test_cancel_request(&adapter).await;
    test_panic_request(&adapter).await;
    test_drop_cancels_loader(&adapter).await;
    test_set_before_request(&adapter).await;
    test_set_during_request(&adapter).await;
}

async fn test_answers_are_correct<T>(adapter: &T)
where
    T: TestAdapter,
{
    let (cache, _loader) = setup(adapter);

    assert_eq!(cache.get(1, true).await, String::from("1_true"));
    assert_eq!(cache.peek(1, ()).await, Some(String::from("1_true")));
    assert_eq!(cache.get(2, false).await, String::from("2_false"));
    assert_eq!(cache.peek(2, ()).await, Some(String::from("2_false")));
}

async fn test_linear_memory<T>(adapter: &T)
where
    T: TestAdapter,
{
    let (cache, loader) = setup(adapter);

    assert_eq!(cache.peek_with_status(1, ()).await, None,);
    assert_eq!(
        cache.get_with_status(1, true).await,
        (String::from("1_true"), CacheGetStatus::Miss),
    );
    assert_eq!(
        cache.get_with_status(1, false).await,
        (String::from("1_true"), CacheGetStatus::Hit),
    );
    assert_eq!(
        cache.peek_with_status(1, ()).await,
        Some((String::from("1_true"), CachePeekStatus::Hit)),
    );
    assert_eq!(
        cache.get_with_status(2, false).await,
        (String::from("2_false"), CacheGetStatus::Miss),
    );
    assert_eq!(
        cache.get_with_status(2, false).await,
        (String::from("2_false"), CacheGetStatus::Hit),
    );
    assert_eq!(
        cache.get_with_status(1, true).await,
        (String::from("1_true"), CacheGetStatus::Hit),
    );
    assert_eq!(
        cache.peek_with_status(1, ()).await,
        Some((String::from("1_true"), CachePeekStatus::Hit)),
    );

    assert_eq!(loader.loaded(), vec![1, 2]);
}

async fn test_concurrent_query_loads_once<T>(adapter: &T)
where
    T: TestAdapter,
{
    let (cache, loader) = setup(adapter);

    loader.block();

    let cache_captured = Arc::clone(&cache);
    let barrier_pending_1 = Arc::new(Barrier::new(2));
    let barrier_pending_1_captured = Arc::clone(&barrier_pending_1);
    let handle_1 = tokio::spawn(async move {
        cache_captured
            .get_with_status(1, true)
            .ensure_pending(barrier_pending_1_captured)
            .await
    });

    barrier_pending_1.wait().await;

    let barrier_pending_2 = Arc::new(Barrier::new(3));

    let cache_captured = Arc::clone(&cache);
    let barrier_pending_2_captured = Arc::clone(&barrier_pending_2);
    let handle_2 = tokio::spawn(async move {
        // use a different `extra` here to proof that the first one was used
        cache_captured
            .get_with_status(1, false)
            .ensure_pending(barrier_pending_2_captured)
            .await
    });
    let barrier_pending_2_captured = Arc::clone(&barrier_pending_2);
    let handle_3 = tokio::spawn(async move {
        // use a different `extra` here to proof that the first one was used
        cache
            .peek_with_status(1, ())
            .ensure_pending(barrier_pending_2_captured)
            .await
    });

    barrier_pending_2.wait().await;
    // Shouldn't issue concurrent load requests for the same key
    let n_blocked = loader.unblock();
    assert_eq!(n_blocked, 1);

    assert_eq!(
        handle_1.await.unwrap(),
        (String::from("1_true"), CacheGetStatus::Miss),
    );
    assert_eq!(
        handle_2.await.unwrap(),
        (String::from("1_true"), CacheGetStatus::MissAlreadyLoading),
    );
    assert_eq!(
        handle_3.await.unwrap(),
        Some((String::from("1_true"), CachePeekStatus::MissAlreadyLoading)),
    );

    assert_eq!(loader.loaded(), vec![1]);
}

async fn test_queries_are_parallelized<T>(adapter: &T)
where
    T: TestAdapter,
{
    let (cache, loader) = setup(adapter);

    loader.block();

    let barrier = Arc::new(Barrier::new(4));

    let cache_captured = Arc::clone(&cache);
    let barrier_captured = Arc::clone(&barrier);
    let handle_1 = tokio::spawn(async move {
        cache_captured
            .get(1, true)
            .ensure_pending(barrier_captured)
            .await
    });

    let cache_captured = Arc::clone(&cache);
    let barrier_captured = Arc::clone(&barrier);
    let handle_2 = tokio::spawn(async move {
        cache_captured
            .get(1, true)
            .ensure_pending(barrier_captured)
            .await
    });

    let barrier_captured = Arc::clone(&barrier);
    let handle_3 =
        tokio::spawn(async move { cache.get(2, false).ensure_pending(barrier_captured).await });

    barrier.wait().await;

    let n_blocked = loader.unblock();
    assert_eq!(n_blocked, 2);

    assert_eq!(handle_1.await.unwrap(), String::from("1_true"));
    assert_eq!(handle_2.await.unwrap(), String::from("1_true"));
    assert_eq!(handle_3.await.unwrap(), String::from("2_false"));

    assert_eq!(loader.loaded(), vec![1, 2]);
}

async fn test_cancel_request<T>(adapter: &T)
where
    T: TestAdapter,
{
    let (cache, loader) = setup(adapter);

    loader.block();

    let barrier_pending_1 = Arc::new(Barrier::new(2));
    let barrier_pending_1_captured = Arc::clone(&barrier_pending_1);
    let cache_captured = Arc::clone(&cache);
    let handle_1 = tokio::spawn(async move {
        cache_captured
            .get(1, true)
            .ensure_pending(barrier_pending_1_captured)
            .await
    });

    barrier_pending_1.wait().await;
    let barrier_pending_2 = Arc::new(Barrier::new(2));
    let barrier_pending_2_captured = Arc::clone(&barrier_pending_2);
    let handle_2 = tokio::spawn(async move {
        cache
            .get(1, false)
            .ensure_pending(barrier_pending_2_captured)
            .await
    });

    barrier_pending_2.wait().await;

    // abort first handle
    handle_1.abort_and_wait().await;

    let n_blocked = loader.unblock();
    assert_eq!(n_blocked, 1);

    assert_eq!(handle_2.await.unwrap(), String::from("1_true"));

    assert_eq!(loader.loaded(), vec![1]);
}

async fn test_panic_request<T>(adapter: &T)
where
    T: TestAdapter,
{
    let (cache, loader) = setup(adapter);

    loader.panic_once(1);
    loader.block();

    // set up initial panicking request
    let barrier_pending_get_panic = Arc::new(Barrier::new(2));
    let barrier_pending_get_panic_captured = Arc::clone(&barrier_pending_get_panic);
    let cache_captured = Arc::clone(&cache);
    let handle_get_panic = tokio::spawn(async move {
        cache_captured
            .get(1, true)
            .ensure_pending(barrier_pending_get_panic_captured)
            .await
    });

    barrier_pending_get_panic.wait().await;

    // set up other requests
    let barrier_pending_others = Arc::new(Barrier::new(4));

    let barrier_pending_others_captured = Arc::clone(&barrier_pending_others);
    let cache_captured = Arc::clone(&cache);
    let handle_get_while_loading_panic = tokio::spawn(async move {
        cache_captured
            .get(1, false)
            .ensure_pending(barrier_pending_others_captured)
            .await
    });

    let barrier_pending_others_captured = Arc::clone(&barrier_pending_others);
    let cache_captured = Arc::clone(&cache);
    let handle_peek_while_loading_panic = tokio::spawn(async move {
        cache_captured
            .peek(1, ())
            .ensure_pending(barrier_pending_others_captured)
            .await
    });

    let barrier_pending_others_captured = Arc::clone(&barrier_pending_others);
    let cache_captured = Arc::clone(&cache);
    let handle_get_other_key = tokio::spawn(async move {
        cache_captured
            .get(2, false)
            .ensure_pending(barrier_pending_others_captured)
            .await
    });

    barrier_pending_others.wait().await;

    let n_blocked = loader.unblock();
    assert_eq!(n_blocked, 2);

    // panic of initial request
    handle_get_panic.await.unwrap_err();

    // requests that use the same loading status also panic
    handle_get_while_loading_panic.await.unwrap_err();
    handle_peek_while_loading_panic.await.unwrap_err();

    // unrelated request should succeed
    assert_eq!(handle_get_other_key.await.unwrap(), String::from("2_false"));

    // failing key was tried exactly once (and the other unrelated key as well)
    assert_eq!(loader.loaded(), vec![1, 2]);

    // loading after panic just works (no poisoning)
    assert_eq!(cache.get(1, false).await, String::from("1_false"));
    assert_eq!(loader.loaded(), vec![1, 2, 1]);
}

async fn test_drop_cancels_loader<T>(adapter: &T)
where
    T: TestAdapter,
{
    let (cache, loader) = setup(adapter);

    loader.block();

    let barrier_pending = Arc::new(Barrier::new(2));
    let barrier_pending_captured = Arc::clone(&barrier_pending);
    let handle = tokio::spawn(async move {
        cache
            .get(1, true)
            .ensure_pending(barrier_pending_captured)
            .await
    });

    barrier_pending.wait().await;

    handle.abort_and_wait().await;

    assert_eq!(Arc::strong_count(&loader), 1);
}

async fn test_set_before_request<T>(adapter: &T)
where
    T: TestAdapter,
{
    let (cache, loader) = setup(adapter);

    loader.block();

    cache.set(1, String::from("foo")).await;

    // blocked loader is not used
    let res = tokio::time::timeout(Duration::from_millis(10), cache.get(1, false))
        .await
        .unwrap();
    assert_eq!(res, String::from("foo"));
    assert_eq!(loader.loaded(), Vec::<u8>::new());
}

async fn test_set_during_request<T>(adapter: &T)
where
    T: TestAdapter,
{
    let (cache, loader) = setup(adapter);

    loader.block();

    let cache_captured = Arc::clone(&cache);
    let barrier_pending = Arc::new(Barrier::new(2));
    let barrier_pending_captured = Arc::clone(&barrier_pending);
    let handle = tokio::spawn(async move {
        cache_captured
            .get(1, true)
            .ensure_pending(barrier_pending_captured)
            .await
    });
    barrier_pending.wait().await;

    cache.set(1, String::from("foo")).await;

    // request succeeds even though the loader is blocked
    let res = tokio::time::timeout(Duration::from_millis(10), handle)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(res, String::from("foo"));
    assert_eq!(loader.loaded(), vec![1]);

    // still cached
    let res = tokio::time::timeout(Duration::from_millis(10), cache.get(1, false))
        .await
        .unwrap();
    assert_eq!(res, String::from("foo"));
    assert_eq!(loader.loaded(), vec![1]);
}

/// Flexible loader function for testing.
#[derive(Debug, Default)]
pub struct TestLoader {
    loaded: Mutex<Vec<u8>>,
    blocked: Mutex<Option<Arc<Notify>>>,
    panic: Mutex<HashSet<u8>>,
}

impl TestLoader {
    /// Panic when loading value for `k`.
    ///
    /// If this is used together with [`block`](Self::block), the panic will occur AFTER
    /// blocking.
    pub fn panic_once(&self, k: u8) {
        self.panic.lock().insert(k);
    }

    /// Block all [`load`](Self::load) requests until [`unblock`](Self::unblock) is called.
    ///
    /// If this is used together with [`panic_once`](Self::panic_once), the panic will occur
    /// AFTER blocking.
    pub fn block(&self) {
        let mut blocked = self.blocked.lock();
        assert!(blocked.is_none());
        *blocked = Some(Arc::new(Notify::new()));
    }

    /// Unblock all requests.
    ///
    /// Returns number of requests that were blocked.
    pub fn unblock(&self) -> usize {
        let handle = self.blocked.lock().take().unwrap();
        let blocked_count = Arc::strong_count(&handle) - 1;
        handle.notify_waiters();
        blocked_count
    }

    /// List all keys that were loaded.
    ///
    /// Contains duplicates if keys were loaded multiple times.
    pub fn loaded(&self) -> Vec<u8> {
        self.loaded.lock().clone()
    }
}

#[async_trait]
impl Loader for TestLoader {
    type K = u8;
    type V = String;
    type Extra = bool;

    async fn load(&self, k: u8, extra: bool) -> String {
        self.loaded.lock().push(k);

        // need to capture the cloned notify handle, otherwise the lock guard leaks into the
        // generator
        let maybe_block = self.blocked.lock().clone();
        if let Some(block) = maybe_block {
            block.notified().await;
        }

        // maybe panic
        if self.panic.lock().remove(&k) {
            panic!("test");
        }

        format!("{k}_{extra}")
    }
}

#[async_trait]
pub trait EnsurePendingExt {
    type Out;

    /// Ensure that the future is pending. In the pending case, try to pass the given barrier. Afterwards await the future again.
    ///
    /// This is helpful to ensure a future is in a pending state before continuing with the test setup.
    async fn ensure_pending(self, barrier: Arc<Barrier>) -> Self::Out;
}

#[async_trait]
impl<F> EnsurePendingExt for F
where
    F: Future + Send + Unpin,
{
    type Out = F::Output;

    async fn ensure_pending(self, barrier: Arc<Barrier>) -> Self::Out {
        let mut fut = self.fuse();
        futures::select_biased! {
            _ = fut => panic!("fut should be pending"),
            _ = barrier.wait().fuse() => (),
        }

        fut.await
    }
}

#[async_trait]
pub trait AbortAndWaitExt {
    /// Abort handle and wait for completion.
    ///
    /// Note that this is NOT just a "wait with timeout or panic". This extension is specific to [`JoinHandle`] and will:
    ///
    /// 1. Call [`JoinHandle::abort`].
    /// 2. Await the [`JoinHandle`] with a timeout (or panic if the timeout is reached).
    /// 3. Check that the handle returned a [`JoinError`] that signals that the tracked task was indeed cancelled and
    ///    didn't exit otherwise (either by finishing or by panicking).
    async fn abort_and_wait(self);
}

#[async_trait]
impl<T> AbortAndWaitExt for JoinHandle<T>
where
    T: std::fmt::Debug + Send,
{
    async fn abort_and_wait(mut self) {
        self.abort();

        let join_err = tokio::time::timeout(Duration::from_secs(1), self)
            .await
            .expect("no timeout")
            .expect_err("handle was aborted and therefore MUST fail");
        assert!(join_err.is_cancelled());
    }
}
