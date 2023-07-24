//! Batching of loader request.
use std::{
    collections::HashMap,
    fmt::Debug,
    future::Future,
    hash::Hash,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    task::Poll,
};

use async_trait::async_trait;
use futures::FutureExt;
use observability_deps::tracing::trace;
use parking_lot::Mutex;
use tokio::sync::oneshot::{channel, Sender};

use crate::cancellation_safe_future::{CancellationSafeFuture, CancellationSafeFutureReceiver};

use super::Loader;

/// Batch [load](Loader::load) requests.
///
/// Requests against this loader will be [pending](std::task::Poll::Pending) until [flush](BatchLoaderFlusher::flush) is
/// called. To simplify the usage -- esp. in combination with [`Cache::get`] -- use [`BatchLoaderFlusherExt`].
///
///
/// [`Cache::get`]: crate::cache::Cache::get
#[derive(Debug)]
pub struct BatchLoader<K, Extra, V, L>
where
    K: Debug + Hash + Send + 'static,
    Extra: Debug + Send + 'static,
    V: Debug + Send + 'static,
    L: Loader<K = Vec<K>, Extra = Vec<Extra>, V = Vec<V>>,
{
    inner: Arc<BatchLoaderInner<K, Extra, V, L>>,
}

impl<K, Extra, V, L> BatchLoader<K, Extra, V, L>
where
    K: Debug + Hash + Send + 'static,
    Extra: Debug + Send + 'static,
    V: Debug + Send + 'static,
    L: Loader<K = Vec<K>, Extra = Vec<Extra>, V = Vec<V>>,
{
    /// Create new batch loader based on a non-batched, vector-based one.
    pub fn new(inner: L) -> Self {
        Self {
            inner: Arc::new(BatchLoaderInner {
                inner,
                pending: Default::default(),
                job_id_counter: Default::default(),
                job_handles: Default::default(),
            }),
        }
    }
}

/// State of [`BatchLoader`].
///
/// This is an extra struct so it can be wrapped into an [`Arc`] and shared with the futures that are spawned into
/// [`CancellationSafeFuture`]
#[derive(Debug)]
struct BatchLoaderInner<K, Extra, V, L>
where
    K: Debug + Hash + Send + 'static,
    Extra: Debug + Send + 'static,
    V: Debug + Send + 'static,
    L: Loader<K = Vec<K>, Extra = Vec<Extra>, V = Vec<V>>,
{
    inner: L,
    pending: Mutex<Vec<(K, Extra, Sender<V>)>>,
    job_id_counter: AtomicU64,
    job_handles: Mutex<HashMap<u64, CancellationSafeFutureReceiver<()>>>,
}

/// Flush interface for [`BatchLoader`].
///
/// This is a trait so you can [type-erase](https://en.wikipedia.org/wiki/Type_erasure) it by putting it into an
/// [`Arc`],
///
/// This trait is object-safe.
#[async_trait]
pub trait BatchLoaderFlusher: Debug + Send + Sync + 'static {
    /// Flush all batched requests.
    async fn flush(&self);
}

#[async_trait]
impl BatchLoaderFlusher for Arc<dyn BatchLoaderFlusher> {
    async fn flush(&self) {
        self.as_ref().flush().await;
    }
}

#[async_trait]
impl<K, Extra, V, L> BatchLoaderFlusher for BatchLoader<K, Extra, V, L>
where
    K: Debug + Hash + Send + 'static,
    Extra: Debug + Send + 'static,
    V: Debug + Send + 'static,
    L: Loader<K = Vec<K>, Extra = Vec<Extra>, V = Vec<V>>,
{
    async fn flush(&self) {
        let pending: Vec<_> = {
            let mut pending = self.inner.pending.lock();
            std::mem::take(pending.as_mut())
        };

        if pending.is_empty() {
            return;
        }
        trace!(n_pending = pending.len(), "flush batch loader",);

        let job_id = self.inner.job_id_counter.fetch_add(1, Ordering::SeqCst);
        let handle_recv = CancellationSafeFutureReceiver::default();

        {
            let mut job_handles = self.inner.job_handles.lock();
            job_handles.insert(job_id, handle_recv.clone());
        }

        let inner = Arc::clone(&self.inner);
        let fut = CancellationSafeFuture::new(
            async move {
                let mut keys = Vec::with_capacity(pending.len());
                let mut extras = Vec::with_capacity(pending.len());
                let mut senders = Vec::with_capacity(pending.len());

                for (k, extra, sender) in pending {
                    keys.push(k);
                    extras.push(extra);
                    senders.push(sender);
                }

                let values = inner.inner.load(keys, extras).await;
                assert_eq!(values.len(), senders.len());

                for (value, sender) in values.into_iter().zip(senders) {
                    sender.send(value).unwrap();
                }

                let mut job_handles = inner.job_handles.lock();
                job_handles.remove(&job_id);
            },
            handle_recv,
        );
        fut.await;
    }
}

#[async_trait]
impl<K, Extra, V, L> Loader for BatchLoader<K, Extra, V, L>
where
    K: Debug + Hash + Send + 'static,
    Extra: Debug + Send + 'static,
    V: Debug + Send + 'static,
    L: Loader<K = Vec<K>, Extra = Vec<Extra>, V = Vec<V>>,
{
    type K = K;
    type Extra = Extra;
    type V = V;

    async fn load(&self, k: Self::K, extra: Self::Extra) -> Self::V {
        let (tx, rx) = channel();

        {
            let mut pending = self.inner.pending.lock();
            pending.push((k, extra, tx));
        }

        rx.await.unwrap()
    }
}

/// Extension trait for [`BatchLoaderFlusher`] because the methods on this extension trait are not object safe.
#[async_trait]
pub trait BatchLoaderFlusherExt {
    /// Try to poll all given futures and automatically [flush](BatchLoaderFlusher) if any of them end up in a pending state.
    ///
    /// This guarantees that the order of the results is identical to the order of the futures.
    async fn auto_flush<F>(&self, futures: Vec<F>) -> Vec<F::Output>
    where
        F: Future + Send,
        F::Output: Send;
}

#[async_trait]
impl<B> BatchLoaderFlusherExt for B
where
    B: BatchLoaderFlusher,
{
    async fn auto_flush<F>(&self, futures: Vec<F>) -> Vec<F::Output>
    where
        F: Future + Send,
        F::Output: Send,
    {
        let mut futures = futures
            .into_iter()
            .map(|f| f.boxed())
            .enumerate()
            .collect::<Vec<_>>();
        let mut output: Vec<Option<F::Output>> = (0..futures.len()).map(|_| None).collect();

        while !futures.is_empty() {
            let mut pending = Vec::with_capacity(futures.len());

            for (idx, mut f) in futures.into_iter() {
                match futures::poll!(&mut f) {
                    Poll::Ready(res) => {
                        output[idx] = Some(res);
                    }
                    Poll::Pending => {
                        pending.push((idx, f));
                    }
                }
            }

            if !pending.is_empty() {
                self.flush().await;

                // prevent hot-looping:
                // It seems that in some cases the underlying loader is ready but the data is not available via the
                // cache driver yet. This is likely due to the signalling system within the cache driver that prevents
                // cancelation, but also allows side-loading and at the same time prevents that the same key is loaded
                // multiple times. Tokio doesn't know that this method here is basically a wait loop. So we yield back
                // to the tokio worker and to allow it to make some progress. Since flush+load take some time anyways,
                // this yield here is not overall performance critical.
                tokio::task::yield_now().await;
            }

            futures = pending;
        }

        output
            .into_iter()
            .map(|o| o.expect("all futures finished"))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use tokio::sync::Barrier;

    use crate::{
        cache::{driver::CacheDriver, Cache},
        loader::test_util::TestLoader,
        test_util::EnsurePendingExt,
    };

    use super::*;

    type TestLoaderT = Arc<TestLoader<Vec<u8>, Vec<bool>, Vec<String>>>;

    #[tokio::test]
    async fn test_flush_empty() {
        let (inner, batch) = setup();
        batch.flush().await;
        assert_eq!(inner.loaded(), vec![],);
    }

    #[tokio::test]
    async fn test_flush_manual() {
        let (inner, batch) = setup();

        let pending_barrier_1 = Arc::new(Barrier::new(2));
        let pending_barrier_1_captured = Arc::clone(&pending_barrier_1);
        let batch_captured = Arc::clone(&batch);
        let handle_1 = tokio::spawn(async move {
            batch_captured
                .load(1, true)
                .ensure_pending(pending_barrier_1_captured)
                .await
        });
        pending_barrier_1.wait().await;

        let pending_barrier_2 = Arc::new(Barrier::new(2));
        let pending_barrier_2_captured = Arc::clone(&pending_barrier_2);
        let batch_captured = Arc::clone(&batch);
        let handle_2 = tokio::spawn(async move {
            batch_captured
                .load(2, false)
                .ensure_pending(pending_barrier_2_captured)
                .await
        });
        pending_barrier_2.wait().await;

        inner.mock_next(vec![1, 2], vec![String::from("foo"), String::from("bar")]);

        batch.flush().await;
        assert_eq!(inner.loaded(), vec![(vec![1, 2], vec![true, false])],);

        assert_eq!(handle_1.await.unwrap(), String::from("foo"));
        assert_eq!(handle_2.await.unwrap(), String::from("bar"));
    }

    /// Simulate the following scenario:
    ///
    /// 1. load `1`, flush it, inner load starts processing `[1]`
    /// 2. load `2`, flush it, inner load starts processing `[2]`
    /// 3. inner loader returns result for `[2]`, batch loader returns that result as well
    /// 4. inner loader returns result for `[1]`, batch loader returns that result as well
    #[tokio::test]
    async fn test_concurrent_load() {
        let (inner, batch) = setup();

        let load_barrier_1 = inner.block_next(vec![1], vec![String::from("foo")]);
        inner.mock_next(vec![2], vec![String::from("bar")]);

        // set up first load
        let pending_barrier_1 = Arc::new(Barrier::new(2));
        let pending_barrier_1_captured = Arc::clone(&pending_barrier_1);
        let batch_captured = Arc::clone(&batch);
        let handle_1 = tokio::spawn(async move {
            batch_captured
                .load(1, true)
                .ensure_pending(pending_barrier_1_captured)
                .await
        });
        pending_barrier_1.wait().await;

        // flush first load, this is blocked by the load barrier
        let pending_barrier_2 = Arc::new(Barrier::new(2));
        let pending_barrier_2_captured = Arc::clone(&pending_barrier_2);
        let batch_captured = Arc::clone(&batch);
        let handle_2 = tokio::spawn(async move {
            batch_captured
                .flush()
                .ensure_pending(pending_barrier_2_captured)
                .await;
        });
        pending_barrier_2.wait().await;

        // set up second load
        let pending_barrier_3 = Arc::new(Barrier::new(2));
        let pending_barrier_3_captured = Arc::clone(&pending_barrier_3);
        let batch_captured = Arc::clone(&batch);
        let handle_3 = tokio::spawn(async move {
            batch_captured
                .load(2, false)
                .ensure_pending(pending_barrier_3_captured)
                .await
        });
        pending_barrier_3.wait().await;

        // flush 2nd load and get result
        batch.flush().await;
        assert_eq!(handle_3.await.unwrap(), String::from("bar"));

        // flush 1st load and get result
        load_barrier_1.wait().await;
        handle_2.await.unwrap();
        assert_eq!(handle_1.await.unwrap(), String::from("foo"));

        assert_eq!(
            inner.loaded(),
            vec![(vec![1], vec![true]), (vec![2], vec![false])],
        );
    }

    #[tokio::test]
    async fn test_cancel_flush() {
        let (inner, batch) = setup();

        let load_barrier_1 = inner.block_next(vec![1], vec![String::from("foo")]);

        // set up load
        let pending_barrier_1 = Arc::new(Barrier::new(2));
        let pending_barrier_1_captured = Arc::clone(&pending_barrier_1);
        let batch_captured = Arc::clone(&batch);
        let handle_1 = tokio::spawn(async move {
            batch_captured
                .load(1, true)
                .ensure_pending(pending_barrier_1_captured)
                .await
        });
        pending_barrier_1.wait().await;

        // flush load, this is blocked by the load barrier
        let pending_barrier_2 = Arc::new(Barrier::new(2));
        let pending_barrier_2_captured = Arc::clone(&pending_barrier_2);
        let batch_captured = Arc::clone(&batch);
        let handle_2 = tokio::spawn(async move {
            batch_captured
                .flush()
                .ensure_pending(pending_barrier_2_captured)
                .await;
        });
        pending_barrier_2.wait().await;

        // abort flush
        handle_2.abort();

        // flush load and get result
        load_barrier_1.wait().await;
        assert_eq!(handle_1.await.unwrap(), String::from("foo"));

        assert_eq!(inner.loaded(), vec![(vec![1], vec![true])],);
    }

    #[tokio::test]
    async fn test_cancel_load_and_flush() {
        let (inner, batch) = setup();

        let load_barrier_1 = inner.block_next(vec![1], vec![String::from("foo")]);

        // set up load
        let pending_barrier_1 = Arc::new(Barrier::new(2));
        let pending_barrier_1_captured = Arc::clone(&pending_barrier_1);
        let batch_captured = Arc::clone(&batch);
        let handle_1 = tokio::spawn(async move {
            batch_captured
                .load(1, true)
                .ensure_pending(pending_barrier_1_captured)
                .await
        });
        pending_barrier_1.wait().await;

        // flush load, this is blocked by the load barrier
        let pending_barrier_2 = Arc::new(Barrier::new(2));
        let pending_barrier_2_captured = Arc::clone(&pending_barrier_2);
        let batch_captured = Arc::clone(&batch);
        let handle_2 = tokio::spawn(async move {
            batch_captured
                .flush()
                .ensure_pending(pending_barrier_2_captured)
                .await;
        });
        pending_barrier_2.wait().await;

        // abort load and flush
        handle_1.abort();
        handle_2.abort();

        // unblock
        load_barrier_1.wait().await;

        // load was still driven to completion
        assert_eq!(inner.loaded(), vec![(vec![1], vec![true])],);
    }

    #[tokio::test]
    async fn test_auto_flush_with_loader() {
        let (inner, batch) = setup();

        inner.mock_next(vec![1, 2], vec![String::from("foo"), String::from("bar")]);

        assert_eq!(
            batch
                .auto_flush(vec![batch.load(1, true), batch.load(2, false)])
                .await,
            vec![String::from("foo"), String::from("bar")],
        );

        assert_eq!(inner.loaded(), vec![(vec![1, 2], vec![true, false])],);
    }

    #[tokio::test]
    async fn test_auto_flush_integration_with_cache_driver() {
        let (inner, batch) = setup();
        let cache = CacheDriver::new(Arc::clone(&batch), HashMap::new());

        inner.mock_next(vec![1, 2], vec![String::from("foo"), String::from("bar")]);
        inner.mock_next(vec![3], vec![String::from("baz")]);

        assert_eq!(
            batch
                .auto_flush(vec![cache.get(1, true), cache.get(2, false)])
                .await,
            vec![String::from("foo"), String::from("bar")],
        );
        assert_eq!(
            batch
                .auto_flush(vec![cache.get(2, true), cache.get(3, true)])
                .await,
            vec![String::from("bar"), String::from("baz")],
        );

        assert_eq!(
            inner.loaded(),
            vec![(vec![1, 2], vec![true, false]), (vec![3], vec![true])],
        );
    }

    fn setup() -> (TestLoaderT, Arc<BatchLoader<u8, bool, String, TestLoaderT>>) {
        let inner = TestLoaderT::default();
        let batch = Arc::new(BatchLoader::new(Arc::clone(&inner)));
        (inner, batch)
    }
}
