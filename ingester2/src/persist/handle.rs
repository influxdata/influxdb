use std::sync::Arc;

use iox_catalog::interface::Catalog;
use iox_query::exec::Executor;
use observability_deps::tracing::*;
use parking_lot::Mutex;
use parquet_file::storage::ParquetStorage;
use sharder::JumpHash;
use tokio::{
    sync::{
        mpsc::{self, error::TrySendError},
        oneshot,
    },
    time::Instant,
};

use crate::buffer_tree::partition::{persisting::PersistingData, PartitionData};

use super::{
    backpressure::PersistState,
    context::{Context, PersistRequest},
};

/// A persistence task submission handle.
///
/// This type is cheap to clone to share across threads.
///
/// # Usage
///
/// The caller should construct an [`PersistHandle`] by calling
/// [`PersistHandle::new()`].
///
/// The [`PersistHandle`] can be cheaply cloned and passed over thread/task
/// boundaries to enqueue persist tasks in parallel.
///
/// Dropping all [`PersistHandle`] instances immediately stops all workers.
///
/// # Topology
///
/// The [`PersistHandle`] uses an internal work group to parallelise persistence
/// operations up to `n_workers` number of parallel tasks.
///
/// Submitting a persistence request selects a worker for the persist job and
/// places the job into a bounded queue (of up to `worker_queue_depth` items).
///
/// ```text
///                            ┌─────────────┐
///                            │PersistHandle├┐
///                            └┬────────────┘├┐
///                             └┬────────────┘│
///                              └─────┬───────┘
///                                    │
///                                    │
///                   ┌────────────────┼────────────────┐
///                   │                │                │
///                   │                │                │
///       worker_queue_depth  worker_queue_depth  worker_queue_depth
///                   │                │                │
///                   ▼                ▼                ▼
///            ┌────────────┐   ┌────────────┐   ┌────────────┐
///            │  Worker 1  │   │  Worker 2  │   │  Worker N  │
///            └────────────┘   └────────────┘   └────────────┘
///                   ▲                ▲                ▲
///                                    │
///                   │                ▼                │
///                             ┌ ─ ─ ─ ─ ─ ─
///                   └ ─ ─ ─ ─▶   Executor  │◀ ─ ─ ─ ─ ┘
///                             └ ─ ─ ─ ─ ─ ─
/// ```
///
/// Compaction is performed in the provided [`Executor`] re-org thread-pool and
/// is shared across all workers.
///
/// At any one time, the number of outstanding persist tasks is bounded by:
///
/// ```text
///
///                    workers * worker_queue_depth
///
/// ```
///
/// At any one time, there may be at most `worker_queue_depth` number of
/// outstanding persist jobs for a single partition or worker.
///
/// # Parallelism & Partition Serialisation
///
/// Persistence jobs are parallelised across partitions, with up to at most
/// `n_worker` parallel persist executions at once.
///
/// Because updates of a partition's [`SortKey`] are not commutative, they must
/// be serialised. For this reason, persist operations for given partition are
/// always placed in the same worker queue, ensuring they execute sequentially.
///
/// [`SortKey`]: schema::sort::SortKey
///
/// # Overload & Back-pressure
///
/// The persist queue is bounded, but the caller must prevent new persist jobs
/// from being generated and blocked whilst waiting to add the persist job to
/// the bounded queue, otherwise the system is effectively unbounded. If an
/// unbounded number of threads block on [`PersistHandle::queue_persist()`]
/// waiting to successfully enqueue the job, then there is no bound on
/// outstanding persist jobs at all.
///
/// To prevent this, the persistence system exposes an indicator of saturation
/// (readable via the [`PersistState`]) that the caller MUST use to prevent the
/// generation of new persist tasks (for example, by blocking any further
/// ingest) on a best-effort basis.
///
/// When the persist queue is saturated, the [`PersistState::is_saturated()`]
/// returns true. Once the backlog of persist jobs is reduced, the
/// [`PersistState`] is switched back to a healthy state and new persist jobs
/// may be generated as normal.
///
/// For details of the exact saturation detection & recovery logic, see
/// [`PersistState`].
#[derive(Debug, Clone)]
pub(crate) struct PersistHandle {
    /// THe state/dependencies shared across all worker tasks.
    inner: Arc<Inner>,

    /// A consistent hash implementation used to consistently map buffers from
    /// one partition to the same worker queue.
    ///
    /// This ensures persistence is serialised per-partition, but in parallel
    /// across partitions (up to the number of worker tasks).
    persist_queues: Arc<JumpHash<mpsc::Sender<PersistRequest>>>,

    /// Task handles for the worker tasks, aborted on drop of all
    /// [`PersistHandle`] instances.
    tasks: Arc<Vec<AbortOnDrop<()>>>,

    /// Records the saturation state of the persist system.
    persist_state: Arc<PersistState>,
}

impl PersistHandle {
    /// Initialise a new persist actor & obtain the first handle.
    pub(crate) fn new(
        n_workers: usize,
        worker_queue_depth: usize,
        exec: Arc<Executor>,
        store: ParquetStorage,
        catalog: Arc<dyn Catalog>,
    ) -> (Self, Arc<PersistState>) {
        assert_ne!(n_workers, 0, "must run at least 1 persist worker");
        assert_ne!(worker_queue_depth, 0, "worker queue depth must be non-zero");

        // Log the important configuration parameters of the persist subsystem.
        info!(
            n_workers,
            worker_queue_depth,
            max_queued_tasks = (n_workers * worker_queue_depth),
            "initialised persist task"
        );

        let inner = Arc::new(Inner {
            exec,
            store,
            catalog,
        });

        let (tx_handles, tasks): (Vec<_>, Vec<_>) = (0..n_workers)
            .map(|_| {
                let inner = Arc::clone(&inner);
                let (tx, rx) = mpsc::channel(worker_queue_depth);
                (tx, AbortOnDrop(tokio::spawn(run_task(inner, rx))))
            })
            .unzip();

        assert!(!tasks.is_empty());

        // Initialise the saturation state as "not saturated".
        let persist_state = Default::default();

        (
            Self {
                inner,
                persist_queues: Arc::new(JumpHash::new(tx_handles)),
                tasks: Arc::new(tasks),
                persist_state: Arc::clone(&persist_state),
            },
            persist_state,
        )
    }

    /// Place `data` from `partition` into the persistence queue.
    ///
    /// This call (asynchronously) waits for space to become available in the
    /// assigned worker queue.
    ///
    /// Once persistence is complete, the partition will be locked and the sort
    /// key will be updated, and [`PartitionData::mark_persisted()`] is called
    /// with `data`.
    ///
    /// Once all persistence related tasks for `data` are complete, the returned
    /// channel publishes a notification.
    ///
    /// # Panics
    ///
    /// Panics if the assigned persist worker task has stopped.
    ///
    /// Panics (asynchronously) if the [`PartitionData`]'s sort key is updated
    /// between persistence starting and ending.
    ///
    /// This will panic (asynchronously) if `data` was not from `partition`.
    pub(crate) async fn queue_persist(
        &self,
        partition: Arc<Mutex<PartitionData>>,
        data: PersistingData,
    ) -> oneshot::Receiver<()> {
        debug!(
            partition_id = data.partition_id().get(),
            "enqueuing persistence task"
        );

        // Build the persist task request.
        let enqueued_at = Instant::now();
        let (r, notify) = PersistRequest::new(partition, data, enqueued_at);

        // Select a worker to dispatch this request to.
        let queue = self.persist_queues.hash(r.partition_id());

        // Try and enqueue the persist task immediately.
        match queue.try_send(r) {
            Ok(()) => {} // Success!
            Err(TrySendError::Closed(_)) => panic!("persist worker has stopped"),
            Err(TrySendError::Full(r)) => {
                // The worker's queue is full. Mark the persist system as being
                // saturated, requiring some time to clear outstanding persist
                // operations.
                //
                // The returned guard MUST be held during the send() await
                // below.
                let _guard = PersistState::set_saturated(
                    Arc::clone(&self.persist_state),
                    self.persist_queues.shards().to_owned(),
                );

                // TODO(test): the guard is held over the await point below

                // Park this task waiting to enqueue the persist whilst holding
                // the guard above.
                //
                // If this send() is aborted, the guard is dropped and the
                // number of waiters is decremented. If the send() is
                // successful, the guard is dropped immediately when leaving
                // this scope.
                queue.send(r).await.expect("persist worker stopped");
            }
        };

        notify
    }
}

#[derive(Debug)]
struct AbortOnDrop<T>(tokio::task::JoinHandle<T>);

impl<T> Drop for AbortOnDrop<T> {
    fn drop(&mut self) {
        self.0.abort()
    }
}

#[derive(Debug)]
pub(super) struct Inner {
    pub(super) exec: Arc<Executor>,
    pub(super) store: ParquetStorage,
    pub(super) catalog: Arc<dyn Catalog>,
}

async fn run_task(inner: Arc<Inner>, mut rx: mpsc::Receiver<PersistRequest>) {
    while let Some(req) = rx.recv().await {
        let ctx = Context::new(req, Arc::clone(&inner));

        let compacted = ctx.compact().await;
        let (sort_key_update, parquet_table_data) = ctx.upload(compacted).await;
        ctx.update_database(sort_key_update, parquet_table_data)
            .await;
    }
}
