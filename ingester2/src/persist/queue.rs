//! A logical persistence queue abstraction.

use std::{fmt::Debug, sync::Arc};

use async_trait::async_trait;
use parking_lot::Mutex;
use tokio::sync::oneshot;

use crate::buffer_tree::partition::{persisting::PersistingData, PartitionData};

/// An abstract logical queue into which [`PersistingData`] (and their matching
/// [`PartitionData`]) are placed to be persisted.
///
/// Implementations MAY reorder persist jobs placed in this queue, and MAY block
/// indefinitely.
///
/// It is a logical error to enqueue a [`PartitionData`] with a
/// [`PersistingData`] from another instance.
#[async_trait]
pub trait PersistQueue: Send + Sync + Debug {
    /// Place `data` from `partition` into the persistence queue,
    /// (asynchronously) blocking until enqueued.
    async fn enqueue(
        &self,
        partition: Arc<Mutex<PartitionData>>,
        data: PersistingData,
    ) -> oneshot::Receiver<()>;
}

#[async_trait]
impl<T> PersistQueue for Arc<T>
where
    T: PersistQueue,
{
    #[allow(clippy::async_yields_async)]
    async fn enqueue(
        &self,
        partition: Arc<Mutex<PartitionData>>,
        data: PersistingData,
    ) -> oneshot::Receiver<()> {
        (**self).enqueue(partition, data).await
    }
}

maybe_pub! {
    pub use super::mock::*;
}

#[cfg(any(test, feature = "benches"))]
pub(crate) mod mock {
    use std::{sync::Arc, time::Duration};

    use test_helpers::timeout::FutureTimeout;
    use tokio::task::JoinHandle;

    use super::*;

    #[derive(Debug, Default)]
    struct State {
        /// Observed PartitionData instances.
        calls: Vec<Arc<Mutex<PartitionData>>>,
        /// Spawned tasks that call [`PartitionData::mark_persisted()`] - may
        /// have already terminated.
        handles: Vec<JoinHandle<()>>,
    }

    impl Drop for State {
        fn drop(&mut self) {
            std::mem::take(&mut self.handles)
                .into_iter()
                .for_each(|h| h.abort());
        }
    }

    /// A mock [`PersistQueue`] implementation.
    #[derive(Debug, Default)]
    pub struct MockPersistQueue {
        state: Mutex<State>,
    }

    impl MockPersistQueue {
        /// Return all observed [`PartitionData`].
        pub fn calls(&self) -> Vec<Arc<Mutex<PartitionData>>> {
            self.state.lock().calls.clone()
        }

        /// Wait for all outstanding mock persist jobs to complete, propagating
        /// any panics.
        pub async fn join(self) {
            let handles = std::mem::take(&mut self.state.lock().handles);
            for h in handles.into_iter() {
                h.with_timeout_panic(Duration::from_secs(5))
                    .await
                    .expect("mock mark persist panic");
            }
        }
    }

    #[async_trait]
    impl PersistQueue for MockPersistQueue {
        #[allow(clippy::async_yields_async)]
        async fn enqueue(
            &self,
            partition: Arc<Mutex<PartitionData>>,
            data: PersistingData,
        ) -> oneshot::Receiver<()> {
            let (tx, rx) = oneshot::channel();

            let mut guard = self.state.lock();
            guard.calls.push(Arc::clone(&partition));

            // Spawn a persist task that randomly completes (soon) in the
            // future.
            //
            // This helps ensure a realistic mock, and that implementations do
            // not depend on ordered persist operations (which the persist
            // system does not provide).
            guard.handles.push(tokio::spawn(async move {
                let wait_ms: u64 = rand::random::<u64>() % 100;
                tokio::time::sleep(Duration::from_millis(wait_ms)).await;
                partition.lock().mark_persisted(data);
                let _ = tx.send(());
            }));

            rx
        }
    }
}
