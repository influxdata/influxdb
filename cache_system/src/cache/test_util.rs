use std::{sync::Arc, time::Duration};

use tokio::sync::Barrier;

use crate::{
    cache::{CacheGetStatus, CachePeekStatus},
    loader::test_util::TestLoader,
    test_util::{AbortAndWaitExt, EnsurePendingExt},
};

use super::Cache;

/// Interface between generic tests and a concrete cache type.
pub trait TestAdapter: Send + Sync + 'static {
    /// Extra information for GET.
    type GetExtra: Send;

    /// Extra information for PEEK.
    type PeekExtra: Send;

    /// Cache type.
    type Cache: Cache<K = u8, V = String, GetExtra = Self::GetExtra, PeekExtra = Self::PeekExtra>;

    /// Create new cache with given loader.
    fn construct(&self, loader: Arc<TestLoader>) -> Arc<Self::Cache>;

    /// Build [`GetExtra`](Self::GetExtra).
    ///
    /// Must contain a [`bool`] payload that is later included into the value string for testing purposes.
    fn get_extra(&self, inner: bool) -> Self::GetExtra;

    /// Build [`PeekExtra`](Self::PeekExtra).
    fn peek_extra(&self) -> Self::PeekExtra;
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
    let adapter = Arc::new(adapter);

    test_answers_are_correct(Arc::clone(&adapter)).await;
    test_linear_memory(Arc::clone(&adapter)).await;
    test_concurrent_query_loads_once(Arc::clone(&adapter)).await;
    test_queries_are_parallelized(Arc::clone(&adapter)).await;
    test_cancel_request(Arc::clone(&adapter)).await;
    test_panic_request(Arc::clone(&adapter)).await;
    test_drop_cancels_loader(Arc::clone(&adapter)).await;
    test_set_before_request(Arc::clone(&adapter)).await;
    test_set_during_request(Arc::clone(&adapter)).await;
}

async fn test_answers_are_correct<T>(adapter: Arc<T>)
where
    T: TestAdapter,
{
    let (cache, loader) = setup(adapter.as_ref());

    loader.mock_next(1, "res_1".to_owned());
    loader.mock_next(2, "res_2".to_owned());

    assert_eq!(
        cache.get(1, adapter.get_extra(true)).await,
        String::from("res_1")
    );
    assert_eq!(
        cache.peek(1, adapter.peek_extra()).await,
        Some(String::from("res_1"))
    );
    assert_eq!(
        cache.get(2, adapter.get_extra(false)).await,
        String::from("res_2")
    );
    assert_eq!(
        cache.peek(2, adapter.peek_extra()).await,
        Some(String::from("res_2"))
    );
}

async fn test_linear_memory<T>(adapter: Arc<T>)
where
    T: TestAdapter,
{
    let (cache, loader) = setup(adapter.as_ref());

    loader.mock_next(1, "res_1".to_owned());
    loader.mock_next(2, "res_2".to_owned());

    assert_eq!(cache.peek_with_status(1, adapter.peek_extra()).await, None,);
    assert_eq!(
        cache.get_with_status(1, adapter.get_extra(true)).await,
        (String::from("res_1"), CacheGetStatus::Miss),
    );
    assert_eq!(
        cache.get_with_status(1, adapter.get_extra(false)).await,
        (String::from("res_1"), CacheGetStatus::Hit),
    );
    assert_eq!(
        cache.peek_with_status(1, adapter.peek_extra()).await,
        Some((String::from("res_1"), CachePeekStatus::Hit)),
    );
    assert_eq!(
        cache.get_with_status(2, adapter.get_extra(false)).await,
        (String::from("res_2"), CacheGetStatus::Miss),
    );
    assert_eq!(
        cache.get_with_status(2, adapter.get_extra(false)).await,
        (String::from("res_2"), CacheGetStatus::Hit),
    );
    assert_eq!(
        cache.get_with_status(1, adapter.get_extra(true)).await,
        (String::from("res_1"), CacheGetStatus::Hit),
    );
    assert_eq!(
        cache.peek_with_status(1, adapter.peek_extra()).await,
        Some((String::from("res_1"), CachePeekStatus::Hit)),
    );

    assert_eq!(loader.loaded(), vec![(1, true), (2, false)]);
}

async fn test_concurrent_query_loads_once<T>(adapter: Arc<T>)
where
    T: TestAdapter,
{
    let (cache, loader) = setup(adapter.as_ref());

    loader.block_global();

    let adapter_captured = Arc::clone(&adapter);
    let cache_captured = Arc::clone(&cache);
    let barrier_pending_1 = Arc::new(Barrier::new(2));
    let barrier_pending_1_captured = Arc::clone(&barrier_pending_1);
    let handle_1 = tokio::spawn(async move {
        cache_captured
            .get_with_status(1, adapter_captured.get_extra(true))
            .ensure_pending(barrier_pending_1_captured)
            .await
    });

    barrier_pending_1.wait().await;

    let barrier_pending_2 = Arc::new(Barrier::new(3));

    let adapter_captured = Arc::clone(&adapter);
    let cache_captured = Arc::clone(&cache);
    let barrier_pending_2_captured = Arc::clone(&barrier_pending_2);
    let handle_2 = tokio::spawn(async move {
        // use a different `extra` here to proof that the first one was used
        cache_captured
            .get_with_status(1, adapter_captured.get_extra(false))
            .ensure_pending(barrier_pending_2_captured)
            .await
    });
    let barrier_pending_2_captured = Arc::clone(&barrier_pending_2);
    let handle_3 = tokio::spawn(async move {
        // use a different `extra` here to proof that the first one was used
        cache
            .peek_with_status(1, adapter.peek_extra())
            .ensure_pending(barrier_pending_2_captured)
            .await
    });

    barrier_pending_2.wait().await;
    loader.mock_next(1, "res_1".to_owned());
    // Shouldn't issue concurrent load requests for the same key
    let n_blocked = loader.unblock_global();
    assert_eq!(n_blocked, 1);

    assert_eq!(
        handle_1.await.unwrap(),
        (String::from("res_1"), CacheGetStatus::Miss),
    );
    assert_eq!(
        handle_2.await.unwrap(),
        (String::from("res_1"), CacheGetStatus::MissAlreadyLoading),
    );
    assert_eq!(
        handle_3.await.unwrap(),
        Some((String::from("res_1"), CachePeekStatus::MissAlreadyLoading)),
    );

    assert_eq!(loader.loaded(), vec![(1, true)]);
}

async fn test_queries_are_parallelized<T>(adapter: Arc<T>)
where
    T: TestAdapter,
{
    let (cache, loader) = setup(adapter.as_ref());

    loader.block_global();

    let barrier = Arc::new(Barrier::new(4));

    let adapter_captured = Arc::clone(&adapter);
    let cache_captured = Arc::clone(&cache);
    let barrier_captured = Arc::clone(&barrier);
    let handle_1 = tokio::spawn(async move {
        cache_captured
            .get(1, adapter_captured.get_extra(true))
            .ensure_pending(barrier_captured)
            .await
    });

    let adapter_captured = Arc::clone(&adapter);
    let cache_captured = Arc::clone(&cache);
    let barrier_captured = Arc::clone(&barrier);
    let handle_2 = tokio::spawn(async move {
        cache_captured
            .get(1, adapter_captured.get_extra(true))
            .ensure_pending(barrier_captured)
            .await
    });

    let barrier_captured = Arc::clone(&barrier);
    let handle_3 = tokio::spawn(async move {
        cache
            .get(2, adapter.get_extra(false))
            .ensure_pending(barrier_captured)
            .await
    });

    barrier.wait().await;

    loader.mock_next(1, "res_1".to_owned());
    loader.mock_next(2, "res_2".to_owned());

    let n_blocked = loader.unblock_global();
    assert_eq!(n_blocked, 2);

    assert_eq!(handle_1.await.unwrap(), String::from("res_1"));
    assert_eq!(handle_2.await.unwrap(), String::from("res_1"));
    assert_eq!(handle_3.await.unwrap(), String::from("res_2"));

    assert_eq!(loader.loaded(), vec![(1, true), (2, false)]);
}

async fn test_cancel_request<T>(adapter: Arc<T>)
where
    T: TestAdapter,
{
    let (cache, loader) = setup(adapter.as_ref());

    loader.block_global();

    let barrier_pending_1 = Arc::new(Barrier::new(2));
    let barrier_pending_1_captured = Arc::clone(&barrier_pending_1);
    let adapter_captured = Arc::clone(&adapter);
    let cache_captured = Arc::clone(&cache);
    let handle_1 = tokio::spawn(async move {
        cache_captured
            .get(1, adapter_captured.get_extra(true))
            .ensure_pending(barrier_pending_1_captured)
            .await
    });

    barrier_pending_1.wait().await;
    let barrier_pending_2 = Arc::new(Barrier::new(2));
    let barrier_pending_2_captured = Arc::clone(&barrier_pending_2);
    let handle_2 = tokio::spawn(async move {
        cache
            .get(1, adapter.get_extra(false))
            .ensure_pending(barrier_pending_2_captured)
            .await
    });

    barrier_pending_2.wait().await;

    // abort first handle
    handle_1.abort_and_wait().await;

    loader.mock_next(1, "res_1".to_owned());

    let n_blocked = loader.unblock_global();
    assert_eq!(n_blocked, 1);

    assert_eq!(handle_2.await.unwrap(), String::from("res_1"));

    assert_eq!(loader.loaded(), vec![(1, true)]);
}

async fn test_panic_request<T>(adapter: Arc<T>)
where
    T: TestAdapter,
{
    let (cache, loader) = setup(adapter.as_ref());

    loader.block_global();

    // set up initial panicking request
    let barrier_pending_get_panic = Arc::new(Barrier::new(2));
    let barrier_pending_get_panic_captured = Arc::clone(&barrier_pending_get_panic);
    let adapter_captured = Arc::clone(&adapter);
    let cache_captured = Arc::clone(&cache);
    let handle_get_panic = tokio::spawn(async move {
        cache_captured
            .get(1, adapter_captured.get_extra(true))
            .ensure_pending(barrier_pending_get_panic_captured)
            .await
    });

    barrier_pending_get_panic.wait().await;

    // set up other requests
    let barrier_pending_others = Arc::new(Barrier::new(4));

    let barrier_pending_others_captured = Arc::clone(&barrier_pending_others);
    let adapter_captured = Arc::clone(&adapter);
    let cache_captured = Arc::clone(&cache);
    let handle_get_while_loading_panic = tokio::spawn(async move {
        cache_captured
            .get(1, adapter_captured.get_extra(false))
            .ensure_pending(barrier_pending_others_captured)
            .await
    });

    let barrier_pending_others_captured = Arc::clone(&barrier_pending_others);
    let adapter_captured = Arc::clone(&adapter);
    let cache_captured = Arc::clone(&cache);
    let handle_peek_while_loading_panic = tokio::spawn(async move {
        cache_captured
            .peek(1, adapter_captured.peek_extra())
            .ensure_pending(barrier_pending_others_captured)
            .await
    });

    let barrier_pending_others_captured = Arc::clone(&barrier_pending_others);
    let adapter_captured = Arc::clone(&adapter);
    let cache_captured = Arc::clone(&cache);
    let handle_get_other_key = tokio::spawn(async move {
        cache_captured
            .get(2, adapter_captured.get_extra(false))
            .ensure_pending(barrier_pending_others_captured)
            .await
    });

    barrier_pending_others.wait().await;

    loader.panic_next(1);
    loader.mock_next(1, "res_1".to_owned());
    loader.mock_next(2, "res_2".to_owned());

    let n_blocked = loader.unblock_global();
    assert_eq!(n_blocked, 2);

    // panic of initial request
    handle_get_panic.await.unwrap_err();

    // requests that use the same loading status also panic
    handle_get_while_loading_panic.await.unwrap_err();
    handle_peek_while_loading_panic.await.unwrap_err();

    // unrelated request should succeed
    assert_eq!(handle_get_other_key.await.unwrap(), String::from("res_2"));

    // failing key was tried exactly once (and the other unrelated key as well)
    assert_eq!(loader.loaded(), vec![(1, true), (2, false)]);

    // loading after panic just works (no poisoning)
    assert_eq!(
        cache.get(1, adapter.get_extra(false)).await,
        String::from("res_1")
    );
    assert_eq!(loader.loaded(), vec![(1, true), (2, false), (1, false)]);
}

async fn test_drop_cancels_loader<T>(adapter: Arc<T>)
where
    T: TestAdapter,
{
    let (cache, loader) = setup(adapter.as_ref());

    loader.block_global();

    let barrier_pending = Arc::new(Barrier::new(2));
    let barrier_pending_captured = Arc::clone(&barrier_pending);
    let handle = tokio::spawn(async move {
        cache
            .get(1, adapter.get_extra(true))
            .ensure_pending(barrier_pending_captured)
            .await
    });

    barrier_pending.wait().await;

    handle.abort_and_wait().await;

    assert_eq!(Arc::strong_count(&loader), 1);
}

async fn test_set_before_request<T>(adapter: Arc<T>)
where
    T: TestAdapter,
{
    let (cache, loader) = setup(adapter.as_ref());

    loader.block_global();

    cache.set(1, String::from("foo")).await;

    // blocked loader is not used
    let res = tokio::time::timeout(
        Duration::from_millis(10),
        cache.get(1, adapter.get_extra(false)),
    )
    .await
    .unwrap();
    assert_eq!(res, String::from("foo"));
    assert_eq!(loader.loaded(), Vec::<(u8, bool)>::new());
}

async fn test_set_during_request<T>(adapter: Arc<T>)
where
    T: TestAdapter,
{
    let (cache, loader) = setup(adapter.as_ref());

    loader.block_global();

    let adapter_captured = Arc::clone(&adapter);
    let cache_captured = Arc::clone(&cache);
    let barrier_pending = Arc::new(Barrier::new(2));
    let barrier_pending_captured = Arc::clone(&barrier_pending);
    let handle = tokio::spawn(async move {
        cache_captured
            .get(1, adapter_captured.get_extra(true))
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
    assert_eq!(loader.loaded(), vec![(1, true)]);

    // still cached
    let res = tokio::time::timeout(
        Duration::from_millis(10),
        cache.get(1, adapter.get_extra(false)),
    )
    .await
    .unwrap();
    assert_eq!(res, String::from("foo"));
    assert_eq!(loader.loaded(), vec![(1, true)]);
}
