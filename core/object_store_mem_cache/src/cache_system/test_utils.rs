use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use futures::FutureExt;

use futures_concurrency::future::FutureExt as _;
use futures_test_utils::{AssertFutureExt, FutureObserver};
use tokio::sync::Barrier;

use crate::cache_system::{
    AsyncDrop, Cache, CacheRequestResult, CacheState, CacheStateKind, HasSize, InUse,
    hook::{
        EvictResult,
        test_utils::{TestHook, TestHookRecord},
    },
    utils::str_err,
};

/// Type alias for cache result futures
type CacheResultFuture<V> = futures::future::BoxFuture<'static, CacheRequestResult<V>>;

/// Helper function to extract future and state from CacheState for tests
pub(crate) fn extract_future_and_state<V, D>(
    cache_state: CacheState<V, D>,
) -> (CacheResultFuture<V>, CacheStateKind)
where
    V: Clone + Send + Sync + 'static,
    D: Clone + Send + Sync + 'static,
{
    let state = cache_state.kind();
    let fut = cache_state
        .inner_fut()
        .expect("extract_future_and_state should only be used with loading states");
    (fut, state)
}

/// Helper function to extract all components from CacheState for tests with early access data
pub(crate) fn extract_full_state<V, D>(
    cache_state: CacheState<V, D>,
) -> (CacheResultFuture<V>, Option<D>, CacheStateKind)
where
    V: Clone + Send + Sync + 'static,
    D: Clone + Send + Sync + 'static,
{
    let state = cache_state.kind();
    let early_data = cache_state.early_access_data().cloned();
    let fut = cache_state
        .inner_fut()
        .expect("extract_full_state should only be used with loading states");
    (fut, early_data, state)
}

/// The extra size of entries when the S3-FIFO is used.
///
/// This is because the internal bookkeeping of the S3-FIFO implementation is more precise.
const S3_FIFO_EXTRA_SIZE: usize = 72;

/// Assert that the result of `f` converges against the given value;
pub(crate) async fn assert_converge_eq<F, T>(f: F, expected: T)
where
    F: Fn() -> T + Send,
    T: Eq + std::fmt::Debug + Send,
{
    let start = Instant::now();

    loop {
        let actual = f();
        if actual == expected {
            return;
        }
        if start.elapsed() > Duration::from_secs(1) {
            assert_eq!(actual, expected);
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

pub(crate) async fn test_happy_path(setup: TestSetup) {
    let TestSetup { cache, observer } = setup;

    let test_size = 1001;
    let test_size_hook = test_size + S3_FIFO_EXTRA_SIZE;

    let barrier = Arc::new(Barrier::new(2));
    let barrier_captured = Arc::clone(&barrier);
    let k1 = Arc::new("k1");
    let size_hint = Arc::new(TestValue(test_size)).size();
    let cache_state = cache.get_or_fetch(
        &k1,
        Box::new(move || {
            async move {
                barrier_captured.wait().await;
                Ok(Arc::new(TestValue(test_size)))
            }
            .boxed()
        }),
        (),
        size_hint,
    );
    let (mut fut, state) = extract_future_and_state(cache_state);
    fut.assert_pending().await;
    assert_eq!(observer.records(), vec![TestHookRecord::Insert(0, "k1")],);

    let (res, _) = tokio::join!(fut, barrier.wait());
    assert_eq!(state, CacheStateKind::NewEntry);
    assert_eq!(res.unwrap(), Arc::new(TestValue(test_size)));
    assert_eq!(
        observer.records(),
        vec![
            TestHookRecord::Insert(0, "k1"),
            TestHookRecord::Fetched(0, "k1", Ok(test_size_hook))
        ]
    );

    cache.prune();
    assert_eq!(
        observer.records(),
        vec![
            TestHookRecord::Insert(0, "k1"),
            TestHookRecord::Fetched(0, "k1", Ok(test_size_hook))
        ]
    );
}

pub(crate) async fn test_panic_loader(setup: TestSetup) {
    let TestSetup { cache, observer } = setup;

    let barrier = Arc::new(Barrier::new(2));
    let barrier_captured = Arc::clone(&barrier);
    let k1 = Arc::new("k1");
    let cache_state = cache.get_or_fetch(
        &k1,
        Box::new(|| {
            async move {
                barrier_captured.wait().await;
                panic!("foo")
            }
            .boxed()
        }),
        (),
        1,
    );
    let (mut fut, state) = extract_future_and_state(cache_state);
    fut.assert_pending().await;
    assert_eq!(observer.records(), vec![TestHookRecord::Insert(0, "k1")],);

    let (res, _) = tokio::join!(fut, barrier.wait());
    assert_eq!(state, CacheStateKind::NewEntry);
    assert_eq!(res.unwrap_err().to_string(), "panic: foo");

    assert_eq!(
        observer.records(),
        vec![
            TestHookRecord::Insert(0, "k1"),
            TestHookRecord::Fetched(0, "k1", Err("panic: foo".to_owned())),
            TestHookRecord::Evict(0, "k1", EvictResult::Failed)
        ]
    );

    // data gone
    assert!(cache.get(&"k1").is_none());
}

pub(crate) async fn test_error_path_loader(setup: TestSetup) {
    let TestSetup { cache, observer } = setup;

    let barrier = Arc::new(Barrier::new(2));
    let barrier_captured = Arc::clone(&barrier);
    let k1 = Arc::new("k1");
    let cache_state = cache.get_or_fetch(
        &k1,
        Box::new(|| {
            async move {
                barrier_captured.wait().await;
                Err(str_err("my error"))
            }
            .boxed()
        }),
        (),
        1,
    );
    let (mut fut, state) = extract_future_and_state(cache_state);
    fut.assert_pending().await;
    assert_eq!(observer.records(), vec![TestHookRecord::Insert(0, "k1")],);

    let (res, _) = tokio::join!(fut, barrier.wait());
    assert_eq!(state, CacheStateKind::NewEntry);
    assert_eq!(res.unwrap_err().to_string(), "my error");

    assert_eq!(
        observer.records(),
        vec![
            TestHookRecord::Insert(0, "k1"),
            TestHookRecord::Fetched(0, "k1", Err("my error".to_owned())),
            TestHookRecord::Evict(0, "k1", EvictResult::Failed),
        ]
    );

    // data gone
    assert!(cache.get(&"k1").is_none());
}

pub(crate) async fn test_get_keeps_key_alive(setup: TestSetup) {
    let TestSetup { cache, observer } = setup;

    let test_size = 1001;
    let test_size_hook = test_size + S3_FIFO_EXTRA_SIZE;

    let k1 = Arc::new("k1");
    let size_hint = Arc::new(TestValue(test_size)).size();
    let cache_state = cache.get_or_fetch(
        &k1,
        Box::new(move || async move { Ok(Arc::new(TestValue(test_size))) }.boxed()),
        (),
        size_hint,
    );
    let state = cache_state.kind();
    let fut = cache_state.await_inner();
    assert_eq!(state, CacheStateKind::NewEntry);
    assert_eq!(fut.await.unwrap(), Arc::new(TestValue(test_size)));
    assert_eq!(
        observer.records(),
        vec![
            TestHookRecord::Insert(0, "k1"),
            TestHookRecord::Fetched(0, "k1", Ok(test_size_hook))
        ]
    );

    cache.prune();
    assert_eq!(
        observer.records(),
        vec![
            TestHookRecord::Insert(0, "k1"),
            TestHookRecord::Fetched(0, "k1", Ok(test_size_hook))
        ]
    );

    let size_hint = Arc::new(TestValue(test_size)).size();
    let cache_state = cache.get_or_fetch(
        &k1,
        Box::new(move || async move { Ok(Arc::new(TestValue(test_size))) }.boxed()),
        (),
        size_hint,
    );
    assert_eq!(cache_state.kind(), CacheStateKind::WasCached);
    assert_eq!(
        cache_state.await_inner().await.unwrap(),
        Arc::new(TestValue(test_size))
    );

    cache.prune();
    assert_eq!(
        observer.records(),
        vec![
            TestHookRecord::Insert(0, "k1"),
            TestHookRecord::Fetched(0, "k1", Ok(test_size_hook))
        ]
    );
}

pub(crate) async fn test_already_loading(setup: TestSetup) {
    let TestSetup { cache, observer } = setup;

    let test_size_1 = 1001;
    let test_size_1_hook = test_size_1 + S3_FIFO_EXTRA_SIZE;
    let test_size_2 = 1002;

    let barrier = Arc::new(Barrier::new(2));
    let barrier_captured = Arc::clone(&barrier);
    let k1 = Arc::new("k1");
    let size_hint = Arc::new(TestValue(test_size_1)).size();
    let cache_state_1 = cache.get_or_fetch(
        &k1,
        Box::new(move || {
            async move {
                barrier_captured.wait().await;
                Ok(Arc::new(TestValue(test_size_1)))
            }
            .boxed()
        }),
        (),
        size_hint,
    );
    let (mut fut_1, state_1) = extract_future_and_state(cache_state_1);
    fut_1.assert_pending().await;
    assert_eq!(observer.records(), vec![TestHookRecord::Insert(0, "k1")],);

    let size_hint = Arc::new(TestValue(test_size_2)).size();
    let cache_state_2 = cache.get_or_fetch(
        &k1,
        Box::new(move || { async move { Ok(Arc::new(TestValue(test_size_2))) } }.boxed()),
        (),
        size_hint,
    );
    let (mut fut_2, state_2) = extract_future_and_state(cache_state_2);
    fut_2.assert_pending().await;

    let (_, fut_res) = tokio::join!(barrier.wait(), fut_1);
    assert_eq!(state_1, CacheStateKind::NewEntry);
    assert_eq!(fut_res.unwrap(), Arc::new(TestValue(test_size_1)));

    let fut_res = fut_2.await;
    assert_eq!(state_2, CacheStateKind::AlreadyLoading);
    assert_eq!(fut_res.unwrap(), Arc::new(TestValue(test_size_1)));

    assert_eq!(
        observer.records(),
        vec![
            TestHookRecord::Insert(0, "k1"),
            TestHookRecord::Fetched(0, "k1", Ok(test_size_1_hook)),
        ]
    );
}

pub(crate) async fn test_drop_while_load_blocked(setup: TestSetup) {
    let TestSetup { cache, observer: _ } = setup;

    let barrier = Arc::new(Barrier::new(2));
    {
        let barrier_captured = Arc::clone(&barrier);
        let k1 = Arc::new("k1");
        let size_hint = Arc::new(TestValue(1001)).size();
        let cache_state = cache.get_or_fetch(
            &k1,
            Box::new(move || {
                {
                    let barrier = Arc::clone(&barrier_captured);
                    async move {
                        barrier.wait().await;
                        Ok(Arc::new(TestValue(1001)))
                    }
                }
                .boxed()
            }),
            (),
            size_hint,
        );
        let (mut fut, _state) = extract_future_and_state(cache_state);
        fut.assert_pending().await;
    }

    drop(cache);

    // abort takes a while
    assert_converge_eq(|| Arc::strong_count(&barrier), 1).await;
}

/// Ensure that we don't wake every single consumer for every single IO interaction.
pub(crate) async fn test_perfect_waking_one_consumer(setup: TestSetup) {
    let TestSetup { cache, observer } = setup;

    const N_IO_STEPS: usize = 10;
    let barriers = Arc::new((0..N_IO_STEPS).map(|_| Barrier::new(2)).collect::<Vec<_>>());
    let barriers_captured = Arc::clone(&barriers);
    let k1 = Arc::new("k1");
    let size_hint = Arc::new(TestValue(1001)).size();
    let cache_state = cache.get_or_fetch(
        &k1,
        Box::new(|| {
            async move {
                for barrier in barriers_captured.iter() {
                    barrier.wait().await;
                }
                Ok(Arc::new(TestValue(1001)))
            }
            .boxed()
        }),
        (),
        size_hint,
    );
    let (mut fut, state) = extract_future_and_state(cache_state);
    fut.assert_pending().await;
    assert_eq!(observer.records(), vec![TestHookRecord::Insert(0, "k1")],);

    let fut_io = async {
        for barrier in barriers.iter() {
            barrier.wait().await;
        }
    };

    let fut = FutureObserver::new(fut, "fut");
    let stats = fut.stats();
    let fut_io = FutureObserver::new(fut_io, "fut_io");

    // Don't use `tokio::select!` or `tokio::join!` because they poll too often. What the H?!
    // So we use this lovely crate instead: https://crates.io/crates/futures-concurrency
    let (res, ()) = fut.join(fut_io).await;
    assert_eq!(state, CacheStateKind::NewEntry);
    assert_eq!(res.unwrap(), Arc::new(TestValue(1001)),);

    // polled once for to determine that all of them are pending, and then once when we finally got
    // the result
    assert_eq!(stats.polled(), 2);

    // it seems that we wake during the final poll (which is unnecessary, because we are about to return `Ready`).
    // Not perfect, but "good enough".
    assert_eq!(stats.woken(), 2);
}

/// Ensure that we don't wake every single consumer for every single IO interaction.
pub(crate) async fn test_perfect_waking_two_consumers(setup: TestSetup) {
    let TestSetup { cache, observer } = setup;

    const N_IO_STEPS: usize = 10;
    let barriers = Arc::new((0..N_IO_STEPS).map(|_| Barrier::new(2)).collect::<Vec<_>>());
    let barriers_captured = Arc::clone(&barriers);
    let k1 = Arc::new("k1");
    let size_hint = Arc::new(TestValue(1001)).size();
    let cache_state_1 = cache.get_or_fetch(
        &k1,
        Box::new(|| {
            async move {
                for barrier in barriers_captured.iter() {
                    barrier.wait().await;
                }
                Ok(Arc::new(TestValue(1001)))
            }
            .boxed()
        }),
        (),
        size_hint,
    );
    let (mut fut_1, state_1) = extract_future_and_state(cache_state_1);
    fut_1.assert_pending().await;
    assert_eq!(observer.records(), vec![TestHookRecord::Insert(0, "k1")],);

    let cache_state_2 = cache.get_or_fetch(
        &k1,
        Box::new(|| async move { unreachable!() }.boxed()),
        (),
        size_hint,
    );
    let (mut fut_2, state_2) = extract_future_and_state(cache_state_2);
    fut_2.assert_pending().await;
    assert_eq!(observer.records(), vec![TestHookRecord::Insert(0, "k1")],);

    let fut_io = async {
        for barrier in barriers.iter() {
            barrier.wait().await;
        }
    };

    let fut_1 = FutureObserver::new(fut_1, "fut_1");
    let stats_1 = fut_1.stats();
    let fut_2 = FutureObserver::new(fut_2, "fut_2");
    let stats_2 = fut_2.stats();
    let fut_io = FutureObserver::new(fut_io, "fut_io");

    // Don't use `tokio::select!` or `tokio::join!` because they poll too often. What the H?!
    // So we use this lovely crate instead: https://crates.io/crates/futures-concurrency
    let ((res_1, res_2), ()) = fut_1.join(fut_2).join(fut_io).await;
    assert_eq!(state_1, CacheStateKind::NewEntry);
    assert_eq!(state_2, CacheStateKind::AlreadyLoading);
    assert_eq!(res_1.unwrap(), Arc::new(TestValue(1001)),);
    assert_eq!(res_2.unwrap(), Arc::new(TestValue(1001)),);

    // polled once for to determine that all of them are pending, and then once when we finally got
    // the result
    assert_eq!(stats_1.polled(), 2);
    assert_eq!(stats_2.polled(), 2);

    // It seems that we wake during the final poll of the first future (which is unnecessary, because we are about to return `Ready`).
    // Not perfect, but "good enough".
    assert_eq!(stats_1.woken(), 2);
    assert_eq!(stats_2.woken(), 1);
}

pub(crate) fn runtime_shutdown(setup: TestSetup) {
    let TestSetup { cache, observer: _ } = setup;

    let rt_1 = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap();
    let barrier = Arc::new(Barrier::new(2));
    let barrier_captured = Arc::clone(&barrier);
    let cache_captured = &cache;
    let cache_state = rt_1.block_on(async move {
        let k1 = Arc::new("k1");
        cache_captured.get_or_fetch(
            &k1,
            Box::new(|| {
                async move {
                    barrier_captured.wait().await;
                    panic!("foo")
                }
                .boxed()
            }),
            (),
            1,
        )
    });
    let (mut fut, _state) = extract_future_and_state(cache_state);
    rt_1.block_on(async {
        fut.assert_pending().await;
    });

    rt_1.shutdown_timeout(Duration::from_secs(1));

    let rt_2 = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let err = rt_2
        .block_on(async move {
            let (res, _) = tokio::join!(fut, barrier.wait());
            res
        })
        .unwrap_err();

    assert_eq!(err.to_string(), "Runtime was shut down");

    rt_2.shutdown_timeout(Duration::from_secs(1));
}

pub(crate) async fn test_get_ok(setup: TestSetup) {
    let TestSetup { cache, observer } = setup;

    let test_size = 1001;
    let test_size_hook = test_size + S3_FIFO_EXTRA_SIZE;

    // entry does NOT exist yet
    let k1 = Arc::new("k1");
    assert!(cache.get(&k1).is_none());
    assert_eq!(observer.records(), vec![],);

    let barrier = Arc::new(Barrier::new(2));
    let barrier_captured = Arc::clone(&barrier);
    let size_hint = Arc::new(TestValue(test_size)).size();
    let cache_state = cache.get_or_fetch(
        &k1,
        Box::new(move || {
            async move {
                barrier_captured.wait().await;
                Ok(Arc::new(TestValue(test_size)))
            }
            .boxed()
        }),
        (),
        size_hint,
    );
    let (mut fut, state) = extract_future_and_state(cache_state);
    fut.assert_pending().await;
    assert_eq!(observer.records(), vec![TestHookRecord::Insert(0, "k1")],);

    // data is loading but NOT ready yet
    assert!(cache.get(&k1).is_none());
    assert_eq!(observer.records(), vec![TestHookRecord::Insert(0, "k1")],);

    let (_, fut_res) = tokio::join!(barrier.wait(), fut);
    assert_eq!(state, CacheStateKind::NewEntry);
    assert_eq!(fut_res.unwrap(), Arc::new(TestValue(test_size)));
    assert_eq!(
        observer.records(),
        vec![
            TestHookRecord::Insert(0, "k1"),
            TestHookRecord::Fetched(0, "k1", Ok(test_size_hook))
        ]
    );

    // data ready and loaded
    assert_eq!(
        cache.get(&k1).unwrap().unwrap(),
        Arc::new(TestValue(test_size))
    );
    assert_eq!(
        observer.records(),
        vec![
            TestHookRecord::Insert(0, "k1"),
            TestHookRecord::Fetched(0, "k1", Ok(test_size_hook))
        ]
    );

    // test keep alive
    cache.prune();
    assert_eq!(
        observer.records(),
        vec![
            TestHookRecord::Insert(0, "k1"),
            TestHookRecord::Fetched(0, "k1", Ok(test_size_hook))
        ]
    );
    assert_eq!(
        cache.get(&k1).unwrap().unwrap(),
        Arc::new(TestValue(test_size))
    );

    cache.prune();
    assert_eq!(
        observer.records(),
        vec![
            TestHookRecord::Insert(0, "k1"),
            TestHookRecord::Fetched(0, "k1", Ok(test_size_hook))
        ]
    );
}

pub(crate) async fn test_get_err(setup: TestSetup) {
    let TestSetup { cache, observer } = setup;

    // entry does NOT exist yet
    let k1 = Arc::new("k1");
    assert!(cache.get(&k1).is_none());
    assert_eq!(observer.records(), vec![],);

    let barrier = Arc::new(Barrier::new(2));
    let barrier_captured = Arc::clone(&barrier);
    let cache_state = cache.get_or_fetch(
        &k1,
        Box::new(|| {
            async move {
                barrier_captured.wait().await;
                Err(str_err("err"))
            }
            .boxed()
        }),
        (),
        1,
    );
    let (mut fut, state) = extract_future_and_state(cache_state);
    fut.assert_pending().await;
    assert_eq!(observer.records(), vec![TestHookRecord::Insert(0, "k1")],);

    // data is loading but NOT ready yet
    assert!(cache.get(&k1).is_none());
    assert_eq!(observer.records(), vec![TestHookRecord::Insert(0, "k1")],);

    let (_, fut_res) = tokio::join!(barrier.wait(), fut);
    assert_eq!(state, CacheStateKind::NewEntry);
    assert_eq!(fut_res.unwrap_err().to_string(), "err");

    assert_eq!(
        observer.records(),
        vec![
            TestHookRecord::Insert(0, "k1"),
            TestHookRecord::Fetched(0, "k1", Err("err".to_owned())),
            TestHookRecord::Evict(0, "k1", EvictResult::Failed),
        ]
    );

    // data gone
    assert!(cache.get(&k1).is_none());
}

pub(crate) async fn test_hook_gen(setup: TestSetup) {
    let TestSetup { cache, observer } = setup;

    let test_size_1 = 1001;
    let test_size_2 = 1002;
    let test_size_1_hook = test_size_1 + S3_FIFO_EXTRA_SIZE;
    let test_size_2_hook = test_size_2 + S3_FIFO_EXTRA_SIZE;

    let k1 = Arc::new("k1");
    let k2 = Arc::new("k2");

    let size_hint = Arc::new(TestValue(test_size_1)).size();
    let cache_state = cache.get_or_fetch(
        &k1,
        Box::new(move || async move { Ok(Arc::new(TestValue(test_size_1))) }.boxed()),
        (),
        size_hint,
    );
    assert_eq!(cache_state.kind(), CacheStateKind::NewEntry);
    assert_eq!(
        cache_state.await_inner().await.unwrap(),
        Arc::new(TestValue(test_size_1))
    );

    let size_hint = Arc::new(TestValue(test_size_2)).size();
    let cache_state = cache.get_or_fetch(
        &k2,
        Box::new(move || async move { Ok(Arc::new(TestValue(test_size_2))) }.boxed()),
        (),
        size_hint,
    );
    assert_eq!(cache_state.kind(), CacheStateKind::NewEntry);
    assert_eq!(
        cache_state.await_inner().await.unwrap(),
        Arc::new(TestValue(test_size_2))
    );

    assert_eq!(
        observer.records(),
        vec![
            TestHookRecord::Insert(0, "k1"),
            TestHookRecord::Fetched(0, "k1", Ok(test_size_1_hook)),
            TestHookRecord::Insert(1, "k2"),
            TestHookRecord::Fetched(1, "k2", Ok(test_size_2_hook)),
        ]
    );
}

pub(crate) struct TestSetup {
    pub(crate) cache: Arc<dyn Cache<&'static str, Arc<TestValue>, ()>>,
    pub(crate) observer: Arc<TestHook<&'static str>>,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct TestValue(pub(crate) usize);

impl HasSize for TestValue {
    fn size(&self) -> usize {
        self.0
    }
}

impl InUse for TestValue {
    fn in_use(&mut self) -> bool {
        false
    }
}

impl AsyncDrop for TestValue {
    async fn async_drop(self) {}
}

#[macro_export]
macro_rules! gen_cache_tests_impl {
        ($setup_fn:ident, [$($test:ident,)+ $(,)?] $(,)?) => {
            $(
                #[tokio::test]
                async fn $test() {
                    let setup = $setup_fn();
                    $crate::cache_system::test_utils::$test(setup).await;
                }
            )+
        };
    }

pub(crate) use gen_cache_tests_impl;

macro_rules! gen_cache_tests {
    ($setup_fn:ident) => {
        $crate::cache_system::test_utils::gen_cache_tests_impl!(
            $setup_fn,
            [
                test_happy_path,
                test_panic_loader,
                test_error_path_loader,
                test_get_keeps_key_alive,
                test_already_loading,
                test_drop_while_load_blocked,
                test_get_ok,
                test_get_err,
                test_perfect_waking_two_consumers,
                test_perfect_waking_one_consumer,
                test_hook_gen,
            ],
        );
    };
}
pub(crate) use gen_cache_tests;
