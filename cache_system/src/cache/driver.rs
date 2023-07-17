//! Main data structure, see [`CacheDriver`].

use crate::{
    backend::CacheBackend,
    cancellation_safe_future::{CancellationSafeFuture, CancellationSafeFutureReceiver},
    loader::Loader,
};
use async_trait::async_trait;
use futures::{
    future::{BoxFuture, Shared},
    FutureExt, TryFutureExt,
};
use observability_deps::tracing::debug;
use parking_lot::Mutex;
use std::{collections::HashMap, fmt::Debug, future::Future, sync::Arc};
use tokio::sync::oneshot::{error::RecvError, Sender};

use super::{Cache, CacheGetStatus, CachePeekStatus};

/// Combine a [`CacheBackend`] and a [`Loader`] into a single [`Cache`]
#[derive(Debug)]
pub struct CacheDriver<B, L>
where
    B: CacheBackend,
    L: Loader<K = B::K, V = B::V>,
{
    state: Arc<Mutex<CacheState<B>>>,
    loader: Arc<L>,
}

impl<B, L> CacheDriver<B, L>
where
    B: CacheBackend,
    L: Loader<K = B::K, V = B::V>,
{
    /// Create new, empty cache with given loader function.
    pub fn new(loader: Arc<L>, backend: B) -> Self {
        Self {
            state: Arc::new(Mutex::new(CacheState {
                cached_entries: backend,
                running_queries: HashMap::new(),
                tag_counter: 0,
            })),
            loader,
        }
    }

    fn start_new_query(
        state: &mut CacheState<B>,
        state_captured: Arc<Mutex<CacheState<B>>>,
        loader: Arc<L>,
        k: B::K,
        extra: L::Extra,
    ) -> (
        CancellationSafeFuture<impl Future<Output = ()>>,
        SharedReceiver<B::V>,
    ) {
        let (tx_main, rx_main) = tokio::sync::oneshot::channel();
        let receiver = rx_main
            .map_ok(|v| Arc::new(Mutex::new(v)))
            .map_err(Arc::new)
            .boxed()
            .shared();
        let (tx_set, rx_set) = tokio::sync::oneshot::channel();

        // generate unique tag
        let tag = state.tag_counter;
        state.tag_counter += 1;

        // need to wrap the query into a `CancellationSafeFuture` so that it doesn't get cancelled when
        // this very request is cancelled
        let join_handle_receiver = CancellationSafeFutureReceiver::default();
        let k_captured = k.clone();
        let fut = async move {
            let loader_fut = async move {
                let submitter = ResultSubmitter::new(state_captured, k_captured.clone(), tag);

                // execute the loader
                // If we panic here then `tx` will be dropped and the receivers will be
                // notified.
                let v = loader.load(k_captured, extra).await;

                // remove "running" state and store result
                let was_running = submitter.submit(v.clone());

                if !was_running {
                    // value was side-loaded, so we cannot populate `v`. Instead block this
                    // execution branch and wait for `rx_set` to deliver the side-loaded
                    // result.
                    loop {
                        tokio::task::yield_now().await;
                    }
                }

                v
            };

            // prefer the side-loader
            let v = futures::select_biased! {
                maybe_v = rx_set.fuse() => {
                    match maybe_v {
                        Ok(v) => {
                            // data get side-loaded via `Cache::set`. In this case, we do
                            // NOT modify the state because there would be a lock-gap. The
                            // `set` function will do that for us instead.
                            v
                        }
                        Err(_) => {
                            // sender side is gone, very likely the cache is shutting down
                            debug!(
                                "Sender for side-loading data into running query gone.",
                            );
                            return;
                        }
                    }
                }
                v = loader_fut.fuse() => v,
            };

            // broadcast result
            // It's OK if the receiver side is gone. This might happen during shutdown
            tx_main.send(v).ok();
        };
        let fut = CancellationSafeFuture::new(fut, join_handle_receiver.clone());

        state.running_queries.insert(
            k,
            RunningQuery {
                recv: receiver.clone(),
                set: tx_set,
                _join_handle: join_handle_receiver,
                tag,
            },
        );

        (fut, receiver)
    }
}

#[async_trait]
impl<B, L> Cache for CacheDriver<B, L>
where
    B: CacheBackend,
    L: Loader<K = B::K, V = B::V>,
{
    type K = B::K;
    type V = B::V;
    type GetExtra = L::Extra;
    type PeekExtra = ();

    async fn get_with_status(
        &self,
        k: Self::K,
        extra: Self::GetExtra,
    ) -> (Self::V, CacheGetStatus) {
        // place state locking into its own scope so it doesn't leak into the generator (async
        // function)
        let (fut, receiver, status) = {
            let mut state = self.state.lock();

            // check if the entry has already been cached
            if let Some(v) = state.cached_entries.get(&k) {
                return (v, CacheGetStatus::Hit);
            }

            // check if there is already a query for this key running
            if let Some(running_query) = state.running_queries.get(&k) {
                (
                    None,
                    running_query.recv.clone(),
                    CacheGetStatus::MissAlreadyLoading,
                )
            } else {
                // requires new query
                let (fut, receiver) = Self::start_new_query(
                    &mut state,
                    Arc::clone(&self.state),
                    Arc::clone(&self.loader),
                    k,
                    extra,
                );
                (Some(fut), receiver, CacheGetStatus::Miss)
            }
        };

        // try to run the loader future in this very task context to avoid spawning tokio tasks (which adds latency and
        // overhead)
        if let Some(fut) = fut {
            fut.await;
        }

        let v = retrieve_from_shared(receiver).await;

        (v, status)
    }

    async fn peek_with_status(
        &self,
        k: Self::K,
        _extra: Self::PeekExtra,
    ) -> Option<(Self::V, CachePeekStatus)> {
        // place state locking into its own scope so it doesn't leak into the generator (async
        // function)
        let (receiver, status) = {
            let mut state = self.state.lock();

            // check if the entry has already been cached
            if let Some(v) = state.cached_entries.get(&k) {
                return Some((v, CachePeekStatus::Hit));
            }

            // check if there is already a query for this key running
            if let Some(running_query) = state.running_queries.get(&k) {
                (
                    running_query.recv.clone(),
                    CachePeekStatus::MissAlreadyLoading,
                )
            } else {
                return None;
            }
        };

        let v = retrieve_from_shared(receiver).await;

        Some((v, status))
    }

    async fn set(&self, k: Self::K, v: Self::V) {
        let maybe_join_handle = {
            let mut state = self.state.lock();

            let maybe_recv = if let Some(running_query) = state.running_queries.remove(&k) {
                // it's OK when the receiver side is gone (likely panicked)
                running_query.set.send(v.clone()).ok();

                // When we side-load data into the running task, the task does NOT modify the
                // backend, so we have to do that. The reason for not letting the task feed the
                // side-loaded data back into `cached_entries` is that we would need to drop the
                // state lock here before the task could acquire it, leading to a lock gap.
                Some(running_query.recv)
            } else {
                None
            };

            state.cached_entries.set(k, v);

            maybe_recv
        };

        // drive running query (if any) to completion
        if let Some(recv) = maybe_join_handle {
            // we do not care if the query died (e.g. due to a panic)
            recv.await.ok();
        }
    }
}

impl<B, L> Drop for CacheDriver<B, L>
where
    B: CacheBackend,
    L: Loader<K = B::K, V = B::V>,
{
    fn drop(&mut self) {
        for _ in self.state.lock().running_queries.drain() {}
    }
}

/// Helper to submit results of running queries.
///
/// Ensures that running query is removed when dropped (e.g. during panic).
struct ResultSubmitter<B>
where
    B: CacheBackend,
{
    state: Arc<Mutex<CacheState<B>>>,
    tag: u64,
    k: Option<B::K>,
    v: Option<B::V>,
}

impl<B> ResultSubmitter<B>
where
    B: CacheBackend,
{
    fn new(state: Arc<Mutex<CacheState<B>>>, k: B::K, tag: u64) -> Self {
        Self {
            state,
            tag,
            k: Some(k),
            v: None,
        }
    }

    /// Submit value.
    ///
    /// Returns `true` if this very query was running.
    fn submit(mut self, v: B::V) -> bool {
        assert!(self.v.is_none());
        self.v = Some(v);
        self.finalize()
    }

    /// Finalize request.
    ///
    /// Returns `true` if this very query was running.
    fn finalize(&mut self) -> bool {
        let k = self.k.take().expect("finalized twice");
        let mut state = self.state.lock();

        match state.running_queries.get(&k) {
            Some(running_query) if running_query.tag == self.tag => {
                state.running_queries.remove(&k);

                if let Some(v) = self.v.take() {
                    // this very query is in charge of the key, so store in in the
                    // underlying cache
                    state.cached_entries.set(k, v);
                }

                true
            }
            _ => {
                // This query is actually not really running any longer but got
                // shut down, e.g. due to side loading. Do NOT store the
                // generated value in the underlying cache.

                false
            }
        }
    }
}

impl<B> Drop for ResultSubmitter<B>
where
    B: CacheBackend,
{
    fn drop(&mut self) {
        if self.k.is_some() {
            // not finalized yet
            self.finalize();
        }
    }
}

/// A [`tokio::sync::oneshot::Receiver`] that can be cloned.
///
/// The types are:
///
/// - `Arc<Mutex<V>>`: Ensures that we can clone `V` without requiring `V: Sync`. At the same time
///   the reference to `V` (i.e. the `Arc`) must be cloneable for `Shared`
/// - `Arc<RecvError>`: Is required because `RecvError` is not `Clone` but `Shared` requires that.
/// - `BoxFuture`: The transformation from `Result<V, RecvError>` to `Result<Arc<Mutex<V>>,
///   Arc<RecvError>>` results in a kinda messy type and we wanna erase that.
/// - `Shared`: Allow the receiver to be cloned and be awaited from multiple places.
type SharedReceiver<V> = Shared<BoxFuture<'static, Result<Arc<Mutex<V>>, Arc<RecvError>>>>;

/// Retrieve data from shared receiver.
async fn retrieve_from_shared<V>(receiver: SharedReceiver<V>) -> V
where
    V: Clone + Send,
{
    receiver
        .await
        .expect("cache loader panicked, see logs")
        .lock()
        .clone()
}

/// State for coordinating the execution of a single running query.
#[derive(Debug)]
struct RunningQuery<V> {
    /// A receiver that can await the result as well.
    recv: SharedReceiver<V>,

    /// A sender that enables setting entries while the query is running.
    #[allow(dead_code)]
    set: Sender<V>,

    /// A handle for the task that is currently executing the query.
    ///
    /// The handle can be used to abort the running query, e.g. when dropping the cache.
    ///
    /// This is "dead code" because we only store it to keep the future alive. There's no direct interaction.
    _join_handle: CancellationSafeFutureReceiver<()>,

    /// Tag so that queries for the same key (e.g. when starting, side-loading, starting again) can
    /// be told apart.
    tag: u64,
}

/// Inner cache state that is usually guarded by a lock.
///
/// The state parts must be updated in a consistent manner, i.e. while using the same lock guard.
#[derive(Debug)]
struct CacheState<B>
where
    B: CacheBackend,
{
    /// Cached entires (i.e. queries completed).
    cached_entries: B,

    /// Currently running queries indexed by cache key.
    running_queries: HashMap<B::K, RunningQuery<B::V>>,

    /// Tag counter for running queries.
    tag_counter: u64,
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::{
        cache::test_util::{run_test_generic, TestAdapter},
        loader::test_util::TestLoader,
    };

    use super::*;

    #[tokio::test]
    async fn test_generic() {
        run_test_generic(MyTestAdapter).await;
    }

    struct MyTestAdapter;

    impl TestAdapter for MyTestAdapter {
        type GetExtra = bool;
        type PeekExtra = ();
        type Cache = CacheDriver<HashMap<u8, String>, TestLoader>;

        fn construct(&self, loader: Arc<TestLoader>) -> Arc<Self::Cache> {
            Arc::new(CacheDriver::new(Arc::clone(&loader) as _, HashMap::new()))
        }

        fn get_extra(&self, inner: bool) -> Self::GetExtra {
            inner
        }

        fn peek_extra(&self) -> Self::PeekExtra {}
    }
}
