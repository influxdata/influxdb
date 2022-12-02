use std::sync::Arc;

use iox_catalog::interface::Catalog;
use iox_query::exec::Executor;
use observability_deps::tracing::info;
use parking_lot::Mutex;
use parquet_file::storage::ParquetStorage;
use thiserror::Error;
use tokio::sync::{
    mpsc::{self},
    Notify,
};

use crate::buffer_tree::partition::{persisting::PersistingData, PartitionData};

use super::{actor::PersistActor, context::PersistRequest};

#[derive(Debug, Error)]
pub(crate) enum PersistError {
    #[error("persist queue is full")]
    QueueFull,
}

/// A persistence task submission handle.
///
/// This type is cheap to clone to share across threads.
///
/// # Usage
///
/// The caller should construct an [`PersistHandle`] and [`PersistActor`] by
/// calling [`PersistHandle::new()`], and run the provided [`PersistActor`]
/// instance in another thread / task (by calling [`PersistActor::run()`]).
///
/// From this point on, the caller interacts only with the [`PersistHandle`]
/// which can be cheaply cloned and passed over thread/task boundaries to
/// enqueue persist tasks.
///
/// Dropping all [`PersistHandle`] instances stops the [`PersistActor`], which
/// immediately stops all workers.
///
/// # Topology
///
/// The persist actor is uses an internal work group to parallelise persistence
/// operations up to `n_workers` number of parallel tasks.
///
/// Submitting a persistence request places the job into a bounded queue,
/// providing a buffer for persistence requests to wait for an available worker.
///
/// Two types of queue exists:
///
///   * Submission queue (bounded by `submission_queue_depth`)
///   * `n_worker` number of worker queues (bounded by `worker_queue_depth`)
///
/// ```text
///                            ┌─────────────┐
///                            │PersistHandle├┐
///                            └┬────────────┘├┐
///                             └┬────────────┘│
///                              └─────┬───────┘
///                                    │
///                         submission_queue_depth
///                                    │
///                                    ▼
///                            ╔═══════════════╗
///                            ║ PersistActor  ║
///                            ╚═══════════════╝
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
///         submission_queue_depth + (workers * worker_queue_depth)
///
/// ```
///
/// At any one time, there may be at most `submission_queue_depth +
/// worker_queue_depth` number of outstanding persist jobs for a single
/// partition.
///
/// These two queues are used to decouple submissions from individual workers -
/// this prevents a "hot" / backlogged worker with a full worker queue from
/// blocking tasks from being passed through to workers with spare capacity.
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
#[derive(Debug, Clone)]
pub(crate) struct PersistHandle {
    tx: mpsc::Sender<PersistRequest>,
}

impl PersistHandle {
    /// Initialise a new persist actor & obtain the first handle.
    ///
    /// The caller should call [`PersistActor::run()`] in a separate
    /// thread / task to start the persistence executor.
    pub(crate) fn new(
        submission_queue_depth: usize,
        n_workers: usize,
        worker_queue_depth: usize,
        exec: Arc<Executor>,
        store: ParquetStorage,
        catalog: Arc<dyn Catalog>,
    ) -> (Self, PersistActor) {
        let (tx, rx) = mpsc::channel(submission_queue_depth);

        // Log the important configuration parameters of the persist subsystem.
        info!(
            submission_queue_depth,
            n_workers,
            worker_queue_depth,
            max_queued_tasks = submission_queue_depth + (n_workers * worker_queue_depth),
            "initialised persist task"
        );

        let actor = PersistActor::new(rx, exec, store, catalog, n_workers, worker_queue_depth);

        (Self { tx }, actor)
    }

    /// Place `data` from `partition` into the persistence queue.
    ///
    /// This call (asynchronously) waits for space to become available in the
    /// submission queue.
    ///
    /// Once persistence is complete, the partition will be locked and the sort
    /// key will be updated, and [`PartitionData::mark_persisted()`] is called
    /// with `data`.
    ///
    /// Once all persistence related tasks are complete, the returned [`Notify`]
    /// broadcasts a notification.
    ///
    /// # Panics
    ///
    /// Panics (asynchronously) if the [`PartitionData`]'s sort key is updated
    /// between persistence starting and ending.
    ///
    /// This will panic (asynchronously) if `data` was not from `partition` or
    /// all worker threads have stopped.
    pub(crate) async fn queue_persist(
        &self,
        partition: Arc<Mutex<PartitionData>>,
        data: PersistingData,
    ) -> Arc<Notify> {
        // Build the persist task request
        let r = PersistRequest::new(partition, data);
        let notify = r.complete_notification();

        self.tx
            .send(r)
            .await
            .expect("no persist worker tasks running");

        notify
    }
}
