use std::{
    future::Future,
    hash::Hash,
    sync::{
        Arc, Weak,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
};

use crate::cache_system::{
    DynError,
    utils::{CatchUnwindDynErrorExt, TokioTask},
};
use dashmap::DashMap;
use futures::{
    FutureExt,
    future::{BoxFuture, Shared},
};
use tokio_util::sync::CancellationToken;

use super::CacheRequestResult;

/// A token that allows a "loader" future to convey that it doesn't want to be canceled anymore because it already made
/// meaningful progress.
#[derive(Debug, Clone, Default)]
pub struct PleaseDontCancelMe(Arc<AtomicBool>);

impl PleaseDontCancelMe {
    /// Meaningful progress was made.
    pub fn signal_meaningful_progress(&self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

/// Future got canceled.
#[derive(Debug, Clone, Copy, Default)]
pub struct Cancelled;

impl std::fmt::Display for Cancelled {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "cancelled")
    }
}

impl std::error::Error for Cancelled {}

/// Execute "loader" futures in background tasks.
///
/// Futures are indexed by a key (`K`) and return a value (`V`).
///
/// A future also has extra data (`D`) attached that can [be accessed](Load::data) even before the future finishes.
/// This can be useful to add tracing information or to stream result data early.
///
/// Requests to the same key are de-duplicated (as long as a future is still running).
#[derive(Debug)]
pub struct Loader<K, V, D>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
    D: Send + Sync + 'static,
{
    gen_counter: AtomicU64,
    tasks: Arc<DashMap<K, Task<V, D>>>,
}

impl<K, V, D> Loader<K, V, D>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
    D: Clone + Send + Sync + 'static,
{
    /// Load data for given key.
    ///
    /// The future generator (`F`) will only be called if there a new future is required, i.e. if there is NO running
    /// future for the given key yet.
    ///
    /// The returned [`Load`] provides:
    ///   * access to the shared future (`F`) by using `Load.await`.
    ///   * access to early data returned as part of the running future, using [`Load::data`].
    ///
    /// What data gets returned early is determined by the caller. When the caller constructs the future (`F`),
    /// if can make use of data (`D`) as a channel to send data (such as object_store metadata for a streaming parquet),
    /// or to perform tracing, etc.
    pub fn load<F, Fut>(&self, k: K, f: F, data: D) -> Load<V, D>
    where
        F: FnOnce(PleaseDontCancelMe) -> Fut + Send,
        Fut: Future<Output = CacheRequestResult<V>> + Send + 'static,
    {
        // fast path
        if let Some(task) = self.tasks.get(&k)
            && let Some(usage) = task.usage.upgrade()
        {
            return Load {
                fut: task.fut.clone(),
                already_loading: true,
                data: task.data.clone(),
                usage,
            };
        }

        // slow path

        // prepare as much as possible before holding the entry lock
        // even if it does extra work, it is better than holding the lock
        // for longer and blocking all concurrency
        let k_captured = k.clone();
        let (usage, cancel, please_dont_cancel_me) = UsageHandleStrong::new();
        let fut = f(please_dont_cancel_me);
        let tasks = Arc::downgrade(&self.tasks);

        // increment generation on creation of new task
        let generation = self.gen_counter.fetch_add(1, Ordering::Relaxed);
        // use TokioTask which will abort on drop (e.g. dropped if occupied entry is found)
        let fut = TokioTask::spawn(async move {
            // Use a biased select: Polling the cancellation token is cheap and we shouldn't bother performing CPU work
            //                      for the loader part if the cancellation happened.
            let res = tokio::select! {
                biased;
                _ = cancel.cancelled() => Err(Arc::new(Cancelled) as DynError),
                res = fut.catch_unwind_dyn_error() => res,
            };

            if let Some(tasks) = tasks.upgrade() {
                tasks.remove_if(&k_captured, |_k, task| task.generation == generation);
            }

            res
        })
        .boxed()
        .shared();

        match self.tasks.entry(k) {
            dashmap::Entry::Occupied(o) => {
                // race condition: an entry got created in the meantime
                let task = o.get();
                if let Some(usage) = task.usage.upgrade() {
                    Load {
                        fut: task.fut.clone(),
                        already_loading: true,
                        data: task.data.clone(),
                        usage,
                    }
                } else {
                    // need to replace the existing entry
                    o.replace_entry(Task {
                        generation,
                        fut: fut.clone(),
                        data: data.clone(),
                        usage: usage.downgrade(),
                    });
                    Load {
                        fut,
                        already_loading: false,
                        data,
                        usage,
                    }
                }
            }
            dashmap::Entry::Vacant(v) => {
                v.insert(Task {
                    generation,
                    fut: fut.clone(),
                    data: data.clone(),
                    usage: usage.downgrade(),
                });
                Load {
                    fut,
                    already_loading: false,
                    data,
                    usage,
                }
            }
        }
    }

    /// Check if given key is currently loading.
    pub fn currently_loading(&self, key: &K) -> bool {
        self.tasks.contains_key(key)
    }
}

impl<K, V, D> Default for Loader<K, V, D>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
    D: Send + Sync + 'static,
{
    fn default() -> Self {
        Self {
            gen_counter: Default::default(),
            tasks: Default::default(),
        }
    }
}

/// Inner type of [`TaskFut`].
type TaskFutInner<V> = Shared<BoxFuture<'static, CacheRequestResult<V>>>;

/// Loader future.
#[derive(Debug)]
pub struct TaskFut<V>
where
    V: Clone + Send + Sync + 'static,
{
    fut: TaskFutInner<V>,

    /// Keep usage handle around to prevent cancellation.
    #[expect(dead_code)]
    usage: UsageHandleStrong,
}

impl<V> Future for TaskFut<V>
where
    V: Clone + Send + Sync + 'static,
{
    type Output = Result<V, DynError>;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        self.get_mut().fut.poll_unpin(cx)
    }
}

#[derive(Debug)]
struct Task<V, D> {
    generation: u64,
    fut: TaskFutInner<V>,
    data: D,
    usage: UsageHandleWeak,
}

/// Data returned by [`Loader::load`].
///
/// Note that this implements [`IntoFuture`]!
#[derive(Debug)]
pub struct Load<V, D>
where
    V: Clone + Send + Sync + 'static,
    D: Send + Sync + 'static,
{
    fut: TaskFutInner<V>,
    already_loading: bool,
    data: D,

    /// Keep usage handle around to prevent cancellation.
    usage: UsageHandleStrong,
}

impl<V, D> Load<V, D>
where
    V: Clone + Send + Sync + 'static,
    D: Clone + Send + Sync + 'static,
{
    /// Returns `true` if the loader already had a running task when [`Loader::load`] was called.
    pub fn already_loading(&self) -> bool {
        self.already_loading
    }

    /// Data attached to the running future.
    pub fn data(&self) -> &D {
        &self.data
    }
}

impl<V, D> IntoFuture for Load<V, D>
where
    V: Clone + Send + Sync + 'static,
    D: Send + Sync + 'static,
{
    type Output = Result<V, DynError>;

    type IntoFuture = TaskFut<V>;

    fn into_future(self) -> Self::IntoFuture {
        TaskFut {
            fut: self.fut,
            usage: self.usage,
        }
    }
}

#[derive(Debug)]
struct UsageHandleInner {
    please_dont_cancel_me: PleaseDontCancelMe,
    cancel: CancellationToken,
}

impl Drop for UsageHandleInner {
    fn drop(&mut self) {
        if !self.please_dont_cancel_me.0.load(Ordering::SeqCst) {
            self.cancel.cancel();
        }
    }
}

#[derive(Debug)]
enum UsageHandleStrong {
    CanCancel(Arc<UsageHandleInner>),
    CannotCancel,
}

impl UsageHandleStrong {
    fn new() -> (Self, CancellationToken, PleaseDontCancelMe) {
        let cancel = CancellationToken::new();
        let please_dont_cancel_me = PleaseDontCancelMe::default();
        let this = Self::CanCancel(Arc::new(UsageHandleInner {
            please_dont_cancel_me: please_dont_cancel_me.clone(),
            cancel: cancel.clone(),
        }));
        (this, cancel, please_dont_cancel_me)
    }

    fn downgrade(&self) -> UsageHandleWeak {
        match self {
            Self::CanCancel(inner) => {
                let please_dont_cancel_me = inner.please_dont_cancel_me.clone();
                UsageHandleWeak::CanCancel(Arc::downgrade(inner), please_dont_cancel_me)
            }
            Self::CannotCancel => UsageHandleWeak::CannotCancel,
        }
    }
}

#[derive(Debug)]
enum UsageHandleWeak {
    CanCancel(Weak<UsageHandleInner>, PleaseDontCancelMe),
    CannotCancel,
}

impl UsageHandleWeak {
    fn upgrade(&self) -> Option<UsageHandleStrong> {
        match self {
            Self::CanCancel(weak, please_dont_cancel_me) => {
                match (
                    weak.upgrade(),
                    please_dont_cancel_me.0.load(Ordering::SeqCst),
                ) {
                    (Some(inner), _) => Some(UsageHandleStrong::CanCancel(inner)),
                    (None, true) => Some(UsageHandleStrong::CannotCancel),
                    (None, false) => None,
                }
            }
            Self::CannotCancel => Some(UsageHandleStrong::CannotCancel),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::cache_system::utils::str_err;
    use tokio::sync::Barrier;

    use super::*;

    #[tokio::test]
    async fn test_loader_uses_task() {
        let loader = Arc::new(Loader::<&'static str, u8, ()>::default());

        let barrier_1 = Arc::new(Barrier::new(2));
        let barrier_1_captured = Arc::clone(&barrier_1);
        let barrier_2 = Arc::new(Barrier::new(2));
        let barrier_2_captured = Arc::clone(&barrier_2);
        let mut fut = loader
            .load(
                "foo",
                async move |please_dont_cancel_me| {
                    please_dont_cancel_me.signal_meaningful_progress();
                    barrier_1_captured.wait().await;
                    barrier_2_captured.wait().await;
                    Ok(1)
                },
                (),
            )
            .into_future();

        tokio::select! {
            biased;
            _ = &mut fut => unreachable!(),
            _ = barrier_1.wait() => {}
        }

        // if loaders would be ordinary futures, this would deadlock
        barrier_2.wait().await;
    }

    #[tokio::test]
    async fn test_dedup() {
        let loader = Arc::new(Loader::<&'static str, u8, Arc<i8>>::default());

        let barrier_1 = Arc::new(Barrier::new(2));
        let barrier_1_captured = Arc::clone(&barrier_1);
        let barrier_2 = Arc::new(Barrier::new(2));
        let barrier_2_captured = Arc::clone(&barrier_2);
        let barrier_3 = Arc::new(Barrier::new(2));
        let barrier_3_captured = Arc::clone(&barrier_3);
        let data_a = Arc::new(11);
        let load_a = loader.load(
            "foo",
            async move |please_dont_cancel_me| {
                please_dont_cancel_me.signal_meaningful_progress();
                barrier_1_captured.wait().await;
                barrier_2_captured.wait().await;
                barrier_3_captured.wait().await;
                Ok(1)
            },
            Arc::clone(&data_a),
        );
        assert!(!load_a.already_loading());
        assert!(Arc::ptr_eq(load_a.data(), &data_a));
        let mut fut_a = load_a.into_future();

        tokio::select! {
            biased;
            _ = &mut fut_a => unreachable!(),
            _ = barrier_1.wait() => {}
        }

        let data_b = Arc::new(22);
        let load_b = loader.load(
            "foo",
            |please_dont_cancel_me| {
                please_dont_cancel_me.signal_meaningful_progress();
                panic!("second future MUST NOT be generated");

                // we need this future to make the type inference happy
                #[expect(unreachable_code)]
                async move {
                    unreachable!()
                }
            },
            Arc::clone(&data_b),
        );
        assert!(load_b.already_loading());
        assert!(Arc::ptr_eq(load_b.data(), &data_a));
        let mut fut_b = load_b.into_future();

        tokio::select! {
            biased;
            _ = &mut fut_b => unreachable!(),
            _ = barrier_2.wait() => {}
        }

        barrier_3.wait().await;

        assert_eq!(fut_a.await.unwrap(), 1);
        assert_eq!(fut_b.await.unwrap(), 1);
    }

    #[tokio::test]
    async fn test_finished_futures_are_forgotten_ok_ok() {
        let loader = Arc::new(Loader::<&'static str, Arc<u8>, ()>::default());

        assert_res_forgotten(&loader, Ok(Arc::new(1))).await;
        assert_res_forgotten(&loader, Ok(Arc::new(2))).await;
    }

    #[tokio::test]
    async fn test_finished_futures_are_forgotten_ok_err() {
        let loader = Arc::new(Loader::<&'static str, Arc<u8>, ()>::default());

        assert_res_forgotten(&loader, Ok(Arc::new(1))).await;
        assert_res_forgotten(&loader, Err(str_err("err"))).await;
    }

    #[tokio::test]
    async fn test_finished_futures_are_forgotten_err_err() {
        let loader = Arc::new(Loader::<&'static str, Arc<u8>, ()>::default());

        assert_res_forgotten(&loader, Err(str_err("err"))).await;
        assert_res_forgotten(&loader, Err(str_err("xxx"))).await;
    }

    #[tokio::test]
    async fn test_finished_futures_are_forgotten_err_ok() {
        let loader = Arc::new(Loader::<&'static str, Arc<u8>, ()>::default());

        assert_res_forgotten(&loader, Err(str_err("err"))).await;
        assert_res_forgotten(&loader, Ok(Arc::new(1))).await;
    }

    async fn assert_res_forgotten(
        loader: &Loader<&'static str, Arc<u8>, ()>,
        res: Result<Arc<u8>, DynError>,
    ) {
        let barrier = Arc::new(Barrier::new(2));
        let barrier_captured = Arc::clone(&barrier);
        let res_captured = res.clone();
        let fut = loader
            .load(
                "foo",
                async move |please_dont_cancel_me| {
                    please_dont_cancel_me.signal_meaningful_progress();
                    barrier_captured.wait().await;
                    res_captured
                },
                (),
            )
            .into_future();

        let (actual, _) = tokio::join!(fut, barrier.wait());
        match res {
            Ok(number) => {
                assert_eq!(Arc::strong_count(&number), 2);

                let actual = actual.unwrap();
                assert!(Arc::ptr_eq(&actual, &number));
            }
            Err(e) => {
                assert_eq!(Arc::strong_count(&e), 2);

                let actual = actual.unwrap_err();
                assert!(Arc::ptr_eq(&actual, &e));
            }
        }
        assert_eq!(Arc::strong_count(&barrier), 1);
    }

    #[tokio::test]
    async fn test_panic_dyn_error() {
        let loader = Arc::new(Loader::<&'static str, u8, ()>::default());
        let err = loader
            .load(
                "foo",
                async move |please_dont_cancel_me| {
                    please_dont_cancel_me.signal_meaningful_progress();
                    panic!("argggggg")
                },
                (),
            )
            .await
            .unwrap_err();
        assert_eq!(err.to_string(), "panic: argggggg");
    }

    #[tokio::test]
    async fn test_panic_does_not_poison() {
        let loader = Arc::new(Loader::<&'static str, u8, ()>::default());

        let err = loader
            .load(
                "foo",
                async move |please_dont_cancel_me| {
                    please_dont_cancel_me.signal_meaningful_progress();
                    panic!("argggggg")
                },
                (),
            )
            .await
            .unwrap_err();
        assert_eq!(err.to_string(), "panic: argggggg");

        assert_eq!(
            loader
                .load(
                    "foo",
                    async move |please_dont_cancel_me| {
                        please_dont_cancel_me.signal_meaningful_progress();
                        Ok(42)
                    },
                    ()
                )
                .await
                .unwrap(),
            42
        );
    }

    #[tokio::test]
    async fn test_cancel_on_drop_yes() {
        let loader = Arc::new(Loader::<&'static str, u8, Arc<i8>>::default());

        let barrier_1 = Arc::new(Barrier::new(2));
        let barrier_1_captured = Arc::clone(&barrier_1);
        let data_a = Arc::new(11);
        let load_a = loader.load(
            "foo",
            async move |_please_dont_cancel_me| {
                barrier_1_captured.wait().await;
                futures::future::pending::<()>().await;
                Ok(1)
            },
            Arc::clone(&data_a),
        );
        assert!(!load_a.already_loading());
        assert!(Arc::ptr_eq(load_a.data(), &data_a));
        let mut fut_a = load_a.into_future();

        tokio::select! {
            biased;
            _ = &mut fut_a => unreachable!(),
            _ = barrier_1.wait() => {}
        }

        // cancel future
        drop(fut_a);
        tokio::time::timeout(Duration::from_millis(500), async {
            while Arc::strong_count(&barrier_1) > 1 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();

        let data_b = Arc::new(22);
        let load_b = loader.load(
            "foo",
            async move |_please_dont_cancel_me| Ok(2),
            Arc::clone(&data_b),
        );
        assert!(!load_b.already_loading());
        assert!(Arc::ptr_eq(load_b.data(), &data_b));
        assert_eq!(load_b.await.unwrap(), 2);
    }

    #[tokio::test]
    async fn test_cancel_on_drop_2nd_future_also_keeps_alive() {
        let loader = Arc::new(Loader::<&'static str, u8, Arc<i8>>::default());

        let barrier_1 = Arc::new(Barrier::new(2));
        let barrier_1_captured = Arc::clone(&barrier_1);
        let barrier_2 = Arc::new(Barrier::new(2));
        let barrier_2_captured = Arc::clone(&barrier_2);
        let data_a = Arc::new(11);
        let load_a = loader.load(
            "foo",
            async move |_please_dont_cancel_me| {
                barrier_1_captured.wait().await;
                barrier_2_captured.wait().await;
                Ok(1)
            },
            Arc::clone(&data_a),
        );
        assert!(!load_a.already_loading());
        assert!(Arc::ptr_eq(load_a.data(), &data_a));
        let mut fut_a = load_a.into_future();

        tokio::select! {
            biased;
            _ = &mut fut_a => unreachable!(),
            _ = barrier_1.wait() => {}
        }

        let data_b = Arc::new(22);
        let load_b = loader.load(
            "foo",
            async move |_please_dont_cancel_me| panic!("should never be called"),
            Arc::clone(&data_b),
        );
        assert!(load_b.already_loading());
        assert!(Arc::ptr_eq(load_b.data(), &data_a));

        // cancel first future
        drop(fut_a);

        let data_c = Arc::new(33);
        let load_c = loader.load(
            "foo",
            async move |_please_dont_cancel_me| panic!("should never be called"),
            Arc::clone(&data_c),
        );
        assert!(load_c.already_loading());
        assert!(Arc::ptr_eq(load_c.data(), &data_a));

        // cancel second future
        drop(load_b);

        let (res, _) = tokio::join!(load_c, barrier_2.wait());
        assert_eq!(res.unwrap(), 1);
    }

    #[tokio::test]
    async fn test_cancel_on_drop_please_dont() {
        let loader = Arc::new(Loader::<&'static str, u8, Arc<i8>>::default());

        let barrier_1 = Arc::new(Barrier::new(2));
        let barrier_1_captured = Arc::clone(&barrier_1);
        let barrier_2 = Arc::new(Barrier::new(2));
        let barrier_2_captured = Arc::clone(&barrier_2);
        let barrier_3 = Arc::new(Barrier::new(2));
        let barrier_3_captured = Arc::clone(&barrier_3);
        let data_a = Arc::new(11);
        let load_a = loader.load(
            "foo",
            async move |please_dont_cancel_me| {
                barrier_1_captured.wait().await;
                please_dont_cancel_me.signal_meaningful_progress();
                barrier_2_captured.wait().await;
                barrier_3_captured.wait().await;
                Ok(1)
            },
            Arc::clone(&data_a),
        );
        assert!(!load_a.already_loading());
        assert!(Arc::ptr_eq(load_a.data(), &data_a));
        let mut fut_a = load_a.into_future();

        tokio::select! {
            biased;
            _ = &mut fut_a => unreachable!(),
            _ = barrier_1.wait() => {}
        }

        // "meaningful progress"
        tokio::select! {
            biased;
            _ = &mut fut_a => unreachable!(),
            _ = barrier_2.wait() => {}
        }

        // cancel future
        drop(fut_a);

        let data_b = Arc::new(22);
        let load_b = loader.load(
            "foo",
            async move |_please_dont_cancel_me| panic!("should never be called"),
            Arc::clone(&data_b),
        );
        assert!(load_b.already_loading());
        assert!(Arc::ptr_eq(load_b.data(), &data_a));
        let (res, _) = tokio::join!(load_b, barrier_3.wait());
        assert_eq!(res.unwrap(), 1);
    }
}
