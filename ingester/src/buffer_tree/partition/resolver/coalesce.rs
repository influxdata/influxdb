use std::{
    pin::Pin,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use async_trait::async_trait;
use data_types::{NamespaceId, PartitionKey, TableId};
use futures::{future::Shared, FutureExt};
use hashbrown::{hash_map::Entry, HashMap};
use parking_lot::Mutex;

use crate::{
    buffer_tree::{namespace::NamespaceName, partition::PartitionData, table::TableMetadata},
    deferred_load::DeferredLoad,
};

use super::PartitionProvider;

/// A helper alias for a boxed, dynamically dispatched future that resolves to a
/// arc/mutex wrapped [`PartitionData`].
type BoxedResolveFuture =
    Pin<Box<dyn std::future::Future<Output = Arc<Mutex<PartitionData>>> + Send>>;

/// A compound key of `(table, partition_key)` which uniquely
/// identifies a single partition.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct Key {
    table_id: TableId,
    partition_key: PartitionKey,
}

/// The state of the resolver.
///
/// The [`Shared`] requires more space than the simple ref-pointer to the
/// [`PartitionData`], so resolving callers replace the shared handle with the
/// resolved result where possible.
#[derive(Debug)]
enum State {
    /// A resolve task is ongoing, and the caller can await the [`Shared`]
    /// future to obtain the result.
    ///
    /// If the atomic bool is false, no thread is changing this [`State`] to
    /// [`State::Resolved`] for the resolved partition. If true, a thread is in
    /// the process of setting (or already has set) the state to
    /// [`State::Resolved`].
    Resolving(Shared<BoxedResolveFuture>, Arc<AtomicBool>),

    /// A prior call resolved this partition.
    Resolved(Arc<Mutex<PartitionData>>),
}

/// A coalescing [`PartitionProvider`] reducing N partition fetch requests into
/// a single call to `T` on a per-partition basis.
///
/// This type solves a concurrency problem, where a series of concurrent cache
/// misses "above" this type causes a series of concurrent lookups against the
/// inner resolver "below" this type for a single partition. This is wasteful,
/// as only one result is retained by the callers (a single [`PartitionData`] is
/// used to reference a partition of data).
///
/// This type is typically used to coalesce requests against the
/// [`CatalogPartitionResolver`]:
///
/// ```text
///                    ┌─────────────────────────────┐
///                    │            Cache            │
///                    └─────────────────────────────┘
///                               │   │   │
///                               ▼   ▼   ▼
///                    ┌─────────────────────────────┐
///                    │  CoalescePartitionResolver  │
///                    └─────────────────────────────┘
///                                   │
///                                   ▼
///                    ┌─────────────────────────────┐
///                    │  CatalogPartitionResolver   │
///                    └─────────────────────────────┘
/// ```
///
/// Imagine the following concurrent requests without this type:
///
///  * T1: check cache for partition A, miss
///  * T2: check cache for partition A, miss
///  * T1: inner.get_partition(A)
///  * T2: inner.get_partition(A)
///  * T1: cache put partition A
///  * T2: cache put partition A
///
/// With this type, the concurrent requests for a single partition (A) are
/// coalesced into a single request against the inner resolver:
///
///  * T1: check cache for partition A, miss
///  * T2: check cache for partition A, miss
///  * T1: CoalescePartitionResolver::get_partition(A)
///  * T2: CoalescePartitionResolver::get_partition(A)
///  * inner.get_partition() **(a single call to inner is made)**
///  * T1: cache put partition A
///  * T2: cache put partition A
///
/// # Memory Overhead
///
/// This type makes a best effort attempt to minimise the memory overhead of
/// memorising partition fetches. Callers drop the intermediate resolving state
/// upon success, leaving only a ref-counted pointer to the shared
/// [`PartitionData`] (a single [`Arc`] ref overhead).
///
/// # Cancellation Safety
///
/// This type is cancellation safe - calls to
/// [`CoalescePartitionResolver::get_partition()`] are safe to abort at any
/// point - the underlying resolve future is always driven to completion in the
/// background once started.
///
/// [`CatalogPartitionResolver`]: super::CatalogPartitionResolver
#[derive(Debug)]
pub struct CoalescePartitionResolver<T> {
    /// The inner resolver the actual partition fetch is delegated to.
    inner: Arc<T>,

    /// A map of handles to ongoing resolve futures.
    ongoing: Mutex<HashMap<Key, State>>,
}

impl<T> CoalescePartitionResolver<T> {
    pub fn new(inner: Arc<T>) -> Self {
        Self {
            inner,
            ongoing: Mutex::new(HashMap::default()),
        }
    }
}

#[async_trait]
impl<T> PartitionProvider for CoalescePartitionResolver<T>
where
    T: PartitionProvider + 'static,
{
    async fn get_partition(
        &self,
        partition_key: PartitionKey,
        namespace_id: NamespaceId,
        namespace_name: Arc<DeferredLoad<NamespaceName>>,
        table_id: TableId,
        table: Arc<DeferredLoad<TableMetadata>>,
    ) -> Arc<Mutex<PartitionData>> {
        let key = Key {
            table_id,
            partition_key: partition_key.clone(), // Ref-counted anyway!
        };

        // Check if there's an ongoing (or recently completed) resolve.
        let (shared, done) = match self.ongoing.lock().entry(key.clone()) {
            Entry::Occupied(v) => match v.get() {
                State::Resolving(fut, done) => (fut.clone(), Arc::clone(done)),
                State::Resolved(v) => return Arc::clone(v),
            },
            Entry::Vacant(v) => {
                // Spawn a future to resolve the partition, and retain a handle
                // to it.
                let inner = Arc::clone(&self.inner);
                let fut: BoxedResolveFuture = Box::pin(do_fetch(
                    inner,
                    partition_key,
                    namespace_id,
                    namespace_name,
                    table_id,
                    table,
                ));

                // Make the future poll-able by many callers, all of which
                // resolve to the same output PartitionData instance.
                let fut = fut.shared();
                let done = Arc::new(AtomicBool::new(false));

                // Allow future callers to obtain this shared handle, instead of
                // resolving the partition themselves.
                v.insert(State::Resolving(fut.clone(), Arc::clone(&done)));

                (fut, done)
            }
        };

        // Wait for the resolve to complete.
        //
        // If this caller future is dropped before this resolve future
        // completes, then it remains unpolled until the next caller obtains a
        // shared handle and continues the process.
        let res = shared.await;

        // As an optimisation, select exactly one thread to acquire the lock and
        // change the state instead of every caller trying to set the state to
        // "resolved", which involves contending on the lock with all concurrent
        // callers for all concurrent partition fetches.
        //
        // Any caller that has been awaiting the shared future above is a
        // candidate to perform this state change, but only one thread will
        // attempt to. In the presence of aborted callers waiting on the shared
        // future, each completed await caller will attempt to change state
        // (cancellation safe).
        if !done.load(Ordering::Relaxed)
            && done
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Relaxed)
                .is_ok()
        {
            // This task should drop the Shared, swapping it for the resolved
            // state.
            //
            // This thread SHOULD NOT fail to perform this action as no other
            // thread will attempt it now the bool has been toggled.
            let old = self
                .ongoing
                .lock()
                .insert(key, State::Resolved(Arc::clone(&res)));

            // Invariant: the resolve future must exist in the map, and the
            // state may only be changed by the thread that won the CAS.
            assert!(matches!(old, Some(State::Resolving(..))));
        }

        res
    }
}

async fn do_fetch<T>(
    inner: T,
    partition_key: PartitionKey,
    namespace_id: NamespaceId,
    namespace_name: Arc<DeferredLoad<NamespaceName>>,
    table_id: TableId,
    table: Arc<DeferredLoad<TableMetadata>>,
) -> Arc<Mutex<PartitionData>>
where
    T: PartitionProvider + 'static,
{
    // Spawn a task, ensuring the resolve future is always driven to completion
    // independently of callers polling the shared result future.
    //
    // This prevents the resolve future from being abandoned by all callers and
    // left allocated (referenced by the ongoing map) but not polled - this
    // could result in a connection being taken from the connection pool and
    // never returned as the resolve future is neither completed, nor dropped
    // (which would cause the connection to be returned).
    tokio::spawn(async move {
        inner
            .get_partition(partition_key, namespace_id, namespace_name, table_id, table)
            .await
    })
    .await
    .expect("coalesced partition resolve task panic")
}

#[cfg(test)]
mod tests {
    use std::{
        future,
        sync::Arc,
        task::{Context, Poll},
        time::Duration,
    };

    use assert_matches::assert_matches;
    use futures::Future;
    use futures::{stream::FuturesUnordered, StreamExt};
    use test_helpers::timeout::FutureTimeout;
    use tokio::sync::{Notify, Semaphore};

    use crate::{
        buffer_tree::partition::resolver::mock::MockPartitionProvider,
        test_util::{
            defer_namespace_name_1_sec, defer_table_metadata_1_sec, PartitionDataBuilder,
            ARBITRARY_NAMESPACE_ID, ARBITRARY_PARTITION_KEY, ARBITRARY_TABLE_ID,
        },
    };

    use super::*;

    /// This test proves that parallel queries for the same partition are
    /// coalesced, returning the same [`PartitionData`] instance and submitting
    /// a single query to the inner resolver.
    #[tokio::test]
    async fn test_coalesce() {
        const MAX_TASKS: usize = 50;

        let data = PartitionDataBuilder::new().build();

        // Add a single instance of the partition - if more than one call is
        // made, this will cause a panic.
        let inner = Arc::new(MockPartitionProvider::default().with_partition(data));
        let layer = Arc::new(CoalescePartitionResolver::new(Arc::clone(&inner)));

        let results = (0..MAX_TASKS)
            .map(|_| {
                layer.get_partition(
                    ARBITRARY_PARTITION_KEY.clone(),
                    ARBITRARY_NAMESPACE_ID,
                    defer_namespace_name_1_sec(),
                    ARBITRARY_TABLE_ID,
                    defer_table_metadata_1_sec(),
                )
            })
            .collect::<FuturesUnordered<_>>()
            .collect::<Vec<_>>()
            .await;

        // All the resulting instances of PartitionData MUST be the same
        // ref-counted instance.
        results.as_slice().windows(2).for_each(|v| {
            assert!(Arc::ptr_eq(&v[0], &v[1]));
        });

        // The state should have been set to "resolved" to reclaim memory
        assert_matches!(
            layer.ongoing.lock().values().next(),
            Some(State::Resolved(..))
        );
    }

    // A resolver that blocks forever when resolving PARTITION_KEY but instantly
    // finishes all others.
    #[derive(Debug)]
    struct BlockingResolver {
        p: Arc<Mutex<PartitionData>>,
    }

    impl PartitionProvider for BlockingResolver {
        fn get_partition<'life0, 'async_trait>(
            &'life0 self,
            partition_key: PartitionKey,
            _namespace_id: NamespaceId,
            _namespace_name: Arc<DeferredLoad<NamespaceName>>,
            _table_id: TableId,
            _table: Arc<DeferredLoad<TableMetadata>>,
        ) -> core::pin::Pin<
            Box<
                dyn core::future::Future<Output = Arc<Mutex<PartitionData>>>
                    + core::marker::Send
                    + 'async_trait,
            >,
        >
        where
            'life0: 'async_trait,
            Self: 'async_trait,
        {
            if partition_key == *ARBITRARY_PARTITION_KEY {
                return future::pending().boxed();
            }
            future::ready(Arc::clone(&self.p)).boxed()
        }
    }

    /// This test proves queries for different partitions proceed in parallel.
    #[tokio::test]
    async fn test_disjoint_parallelised() {
        use futures::Future;

        let data = PartitionDataBuilder::new().build();
        let namespace_loader = defer_namespace_name_1_sec();
        let table_loader = defer_table_metadata_1_sec();

        // Add a single instance of the partition - if more than one call is
        // made to the mock, it will panic.
        let inner = Arc::new(BlockingResolver {
            p: Arc::new(Mutex::new(data)),
        });
        let layer = Arc::new(CoalescePartitionResolver::new(inner));

        // The following two partitions are for the same (blocked) partition and
        // neither resolve.
        let pa_1 = layer.get_partition(
            ARBITRARY_PARTITION_KEY.clone(),
            ARBITRARY_NAMESPACE_ID,
            Arc::clone(&namespace_loader),
            ARBITRARY_TABLE_ID,
            Arc::clone(&table_loader),
        );
        let pa_2 = layer.get_partition(
            ARBITRARY_PARTITION_KEY.clone(),
            ARBITRARY_NAMESPACE_ID,
            Arc::clone(&namespace_loader),
            ARBITRARY_TABLE_ID,
            Arc::clone(&table_loader),
        );

        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);

        futures::pin_mut!(pa_1);
        futures::pin_mut!(pa_2);

        // Neither make progress
        assert_matches!(Pin::new(&mut pa_1).poll(&mut cx), Poll::Pending);
        assert_matches!(Pin::new(&mut pa_2).poll(&mut cx), Poll::Pending);

        // But a non-blocked partition is resolved without issue.
        let _ = layer
            .get_partition(
                PartitionKey::from("orange you glad i didn't say bananas"),
                ARBITRARY_NAMESPACE_ID,
                namespace_loader,
                ARBITRARY_TABLE_ID,
                table_loader,
            )
            .with_timeout_panic(Duration::from_secs(5))
            .await;

        // While the original requests are still blocked.
        assert_matches!(Pin::new(&mut pa_1).poll(&mut cx), Poll::Pending);
        assert_matches!(Pin::new(&mut pa_2).poll(&mut cx), Poll::Pending);
    }

    /// A resolver that obtains a semaphore (simulating a connection pool
    /// semaphore) during a resolve and then blocks until signalled.
    #[derive(Debug)]
    struct SemaphoreResolver {
        /// The resolver call acquires a permit from here.
        sem: Arc<Semaphore>,
        /// An then waits for this notify to be unblocked before "completing".
        wait: Arc<Notify>,
        /// And returning this data
        p: Arc<Mutex<PartitionData>>,
    }

    #[async_trait]
    impl PartitionProvider for SemaphoreResolver {
        async fn get_partition(
            &self,
            _partition_key: PartitionKey,
            _namespace_id: NamespaceId,
            _namespace_name: Arc<DeferredLoad<NamespaceName>>,
            _table_id: TableId,
            _table: Arc<DeferredLoad<TableMetadata>>,
        ) -> Arc<Mutex<PartitionData>> {
            let waker = self.wait.notified();
            let permit = self.sem.acquire().await.unwrap();
            waker.await;
            drop(permit); // explicit permit drop for clarity
            Arc::clone(&self.p)
        }
    }

    /// This test asserts a resolve future that is started, is always driven to
    /// completion.
    #[tokio::test]
    async fn test_inner_future_always_resolves() {
        // Create a fake "connection pool" to highlight the importance of
        // resolve future completion.
        //
        // In this case, the "pool" only has one permit/connection, and the
        // resolve future will obtain it. If the future does not get driven
        // forwards (because the caller aborted and is no longer polling it) the
        // permit will be stuck in the future that is not making progress, but
        // still allocated, effectively deadlocking the pool.
        let fake_conn_pool = Arc::new(Semaphore::new(1));

        // A waker to unblock the resolver semaphore for completion.
        let notify = Arc::new(Notify::default());

        let inner = Arc::new(SemaphoreResolver {
            sem: Arc::clone(&fake_conn_pool),
            wait: Arc::clone(&notify),
            p: Arc::new(Mutex::new(PartitionDataBuilder::new().build())),
        });

        let layer = Arc::new(CoalescePartitionResolver::new(inner));

        let fut = layer.get_partition(
            ARBITRARY_PARTITION_KEY.clone(),
            ARBITRARY_NAMESPACE_ID,
            defer_namespace_name_1_sec(),
            ARBITRARY_TABLE_ID,
            defer_table_metadata_1_sec(),
        );

        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);

        futures::pin_mut!(fut);

        // Poll the future until it blocks waiting on notify.
        assert_matches!(Pin::new(&mut fut).poll(&mut cx), Poll::Pending);

        // Drop the caller's poll future to prove nothing else polls it.
        #[allow(clippy::drop_non_drop)]
        drop(fut);

        // Allow the resolve future to unblock and complete, if it is being
        // polled.
        notify.notify_waiters();

        // And attempt to acquire the only semaphore permit / "connection"  from
        // the pool. If this succeeds, the dropped resolve future was driven to
        // completion in the background, without an explicit poll by this
        // thread / the only caller.
        let _conn = fake_conn_pool
            .acquire()
            // If a "connection" is not available within 5 seconds, panic.
            .with_timeout_panic(Duration::from_secs(5))
            .await
            .unwrap();
    }
}
