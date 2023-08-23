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

/// This needs to be pub for the benchmarks but should not be used outside the crate.
#[cfg(feature = "benches")]
pub use mock::*;

/// This needs to be pub for the benchmarks but should not be used outside the crate.
#[cfg(feature = "benches")]
pub use crate::persist::completion_observer::*;

#[cfg(any(test, feature = "benches"))]
pub(crate) mod mock {
    use std::{sync::Arc, time::Duration};

    use data_types::{
        ColumnId, ColumnSet, NamespaceId, ParquetFile, ParquetFileId, PartitionHashId,
        PartitionKey, TableId, Timestamp, TransitionPartitionId,
    };
    use test_helpers::timeout::FutureTimeout;
    use tokio::task::JoinHandle;

    use super::*;
    use crate::persist::completion_observer::{
        CompletedPersist, NopObserver, PersistCompletionObserver,
    };

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
    #[derive(Debug)]
    pub struct MockPersistQueue<O> {
        state: Mutex<State>,
        completion_observer: O,
    }

    impl Default for MockPersistQueue<NopObserver> {
        fn default() -> Self {
            Self {
                state: Default::default(),
                completion_observer: Default::default(),
            }
        }
    }

    impl<O> MockPersistQueue<O>
    where
        O: PersistCompletionObserver + 'static,
    {
        /// Creates a queue that notifies the [`PersistCompletionObserver`]
        /// on persist enqueue completion.
        pub fn new_with_observer(completion_observer: O) -> Self {
            Self {
                state: Default::default(),
                completion_observer,
            }
        }

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
    impl<O> PersistQueue for MockPersistQueue<O>
    where
        O: PersistCompletionObserver + Clone + 'static,
    {
        #[allow(clippy::async_yields_async)]
        async fn enqueue(
            &self,
            partition: Arc<Mutex<PartitionData>>,
            data: PersistingData,
        ) -> oneshot::Receiver<()> {
            let (tx, rx) = oneshot::channel();

            let mut guard = self.state.lock();
            guard.calls.push(Arc::clone(&partition));

            let completion_observer = self.completion_observer.clone();
            // Spawn a persist task that randomly completes (soon) in the
            // future.
            //
            // This helps ensure a realistic mock, and that implementations do
            // not depend on ordered persist operations (which the persist
            // system does not provide).
            guard.handles.push(tokio::spawn(async move {
                let wait_ms: u64 = rand::random::<u64>() % 100;
                tokio::time::sleep(Duration::from_millis(wait_ms)).await;
                let sequence_numbers = partition.lock().mark_persisted(data);
                let table_id = TableId::new(2);
                let partition_hash_id =
                    PartitionHashId::new(table_id, &PartitionKey::from("arbitrary"));
                let partition_id = TransitionPartitionId::Deterministic(partition_hash_id);
                completion_observer
                    .persist_complete(Arc::new(CompletedPersist::new(
                        ParquetFile {
                            id: ParquetFileId::new(42),
                            to_delete: None,
                            namespace_id: NamespaceId::new(1),
                            table_id,
                            partition_id,
                            object_store_id: Default::default(),
                            min_time: Timestamp::new(42),
                            max_time: Timestamp::new(42),
                            file_size_bytes: 42424242,
                            row_count: 24,
                            compaction_level: data_types::CompactionLevel::Initial,
                            created_at: Timestamp::new(1234),
                            column_set: ColumnSet::new([1, 2, 3, 4].into_iter().map(ColumnId::new)),
                            max_l0_created_at: Timestamp::new(42),
                        },
                        sequence_numbers,
                    )))
                    .await;
                let _ = tx.send(());
            }));

            rx
        }
    }
}
