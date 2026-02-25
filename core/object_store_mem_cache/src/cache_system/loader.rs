use std::{
    future::Future,
    hash::Hash,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
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

use super::CacheRequestResult;

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
        F: FnOnce() -> Fut + Send,
        Fut: Future<Output = CacheRequestResult<V>> + Send + 'static,
    {
        // fast path
        if let Some(task) = self.tasks.get(&k) {
            return Load {
                fut: task.fut.clone(),
                already_loading: true,
                data: task.data.clone(),
            };
        }

        // slow path

        // prepare as much as possible before holding the entry lock
        // even if it does extra work, it is better than holding the lock
        // for longer and blocking all concurrency
        let k_captured = k.clone();
        let fut = f();
        let tasks = Arc::downgrade(&self.tasks);

        // increment generation on creation of new task
        let generation = self.gen_counter.fetch_add(1, Ordering::Relaxed);
        // use TokioTask which will abort on drop (e.g. dropped if occupied entry is found)
        let fut = TokioTask::spawn(async move {
            let res = fut.catch_unwind_dyn_error().await;
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
                Load {
                    fut: task.fut.clone(),
                    already_loading: true,
                    data: task.data.clone(),
                }
            }
            dashmap::Entry::Vacant(v) => {
                v.insert(Task {
                    generation,
                    fut: fut.clone(),
                    data: data.clone(),
                });
                Load {
                    fut,
                    already_loading: false,
                    data,
                }
            }
        }
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

pub type TaskFut<V> = Shared<BoxFuture<'static, CacheRequestResult<V>>>;

#[derive(Debug)]
struct Task<V, D> {
    generation: u64,
    fut: TaskFut<V>,
    data: D,
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
    fut: TaskFut<V>,
    already_loading: bool,
    data: D,
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
        self.fut
    }
}

#[cfg(test)]
mod tests {
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
                async move || {
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
            async move || {
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
            || {
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
                async move || {
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
            .load("foo", async move || panic!("argggggg"), ())
            .await
            .unwrap_err();
        assert_eq!(err.to_string(), "panic: argggggg");
    }

    #[tokio::test]
    async fn test_panic_does_not_poison() {
        let loader = Arc::new(Loader::<&'static str, u8, ()>::default());

        let err = loader
            .load("foo", async move || panic!("argggggg"), ())
            .await
            .unwrap_err();
        assert_eq!(err.to_string(), "panic: argggggg");

        assert_eq!(
            loader.load("foo", async move || Ok(42), ()).await.unwrap(),
            42
        );
    }
}
