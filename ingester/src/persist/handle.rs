use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use iox_catalog::interface::Catalog;
use iox_query::{exec::Executor, QueryChunk};
use metric::{DurationHistogram, DurationHistogramOptions, U64Counter, U64Gauge, DURATION_MAX};
use observability_deps::tracing::*;
use parking_lot::Mutex;
use parquet_file::storage::ParquetStorage;
use schema::sort::adjust_sort_key_columns;
use sharder::JumpHash;
use tokio::{
    sync::{mpsc, oneshot, Semaphore, TryAcquireError},
    time::Instant,
};

use super::{
    backpressure::PersistState, completion_observer::PersistCompletionObserver,
    context::PersistRequest, queue::PersistQueue, worker::SharedWorkerState,
};
use crate::{
    buffer_tree::partition::{persisting::PersistingData, PartitionData, SortKeyState},
    ingest_state::IngestState,
    persist::worker,
};

/// A persistence task submission handle.
///
/// # Usage
///
/// The caller should construct an [`PersistHandle`] by calling
/// [`PersistHandle::new()`].
///
/// The [`PersistHandle`] can be wrapped in an [`Arc`] and shared over
/// thread/task boundaries to enqueue persist tasks in parallel.
///
/// Dropping the [`PersistHandle`] instance immediately stops all persist
/// workers and drops all outstanding persist tasks.
///
/// # Topology
///
/// The persist system is exposes a logical queue of persist tasks with up to
/// `persist_queue_depth` slots. Each persist task is executed on a worker in a
/// worker pool in order to parallelise persistence operations up to `n_workers`
/// number of parallel tasks.
///
/// ```text
///                            ┌─────────────┐
///                            │PersistHandle├┐
///                            └┬────────────┘├┐
///                             └┬────────────┘│
///                              └─────┬───────┘
///                                    │
///                           persist_queue_depth
///                                    │
///                                    │
///                   ┌────────────────┼────────────────┐
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
/// Internally, several queues are used, and reordering of persist tasks is
/// allowed for efficiency / performance reasons, allowing maximal
/// parallelisation of work given the constraints of an individual persist job.
///
/// Compaction is performed in the provided [`Executor`] re-org thread-pool and
/// is shared across a set of `n_workers` number of worker tasks.
///
/// While the logical queue bounds the number of outstanding persist tasks to at
/// most `persist_queue_depth`, once this limit is reached threads attempting to
/// push into the queue block for a free slot to become available in the queue -
/// this can increase the number of outstanding jobs in the system beyond
/// `persist_queue_depth` - see "Overload & Back-pressure" below.
///
/// # Parallelism & Partition Serialisation
///
/// Persistence jobs are parallelised across partitions, with up to at most
/// `n_worker` parallel persist executions at once.
///
/// Because updates of a partition's [`SortKey`] are not commutative, they must
/// be serialised. For this reason, persist operations for given partition are
/// placed in the same worker queue, ensuring they execute sequentially unless
/// it can be determined that the persist operation will not cause a [`SortKey`]
/// update, at which point it is enqueued into the global work queue and
/// parallelised with other persist jobs for the same partition.
///
/// Multiple ingester nodes running in a cluster can update the [`SortKey`] in
/// the catalog for a partition independently, and at times, concurrently. When
/// a concurrent catalog sort key update occurs, only one persist job makes
/// progress and the others must be restarted with the newly discovered key.
///
/// [`SortKey`]: schema::sort::SortKey
///
/// # Overload & Back-pressure
///
/// The logical persist queue is bounded, but the caller must prevent new
/// persist jobs from being generated and blocked whilst waiting to add the
/// persist job to the bounded queue, otherwise the system is effectively
/// unbounded. If an unbounded number of threads block on
/// [`PersistHandle::enqueue()`] waiting to successfully enqueue the job, then
/// there is no bound on outstanding persist jobs at all.
///
/// To enforce the bound, the persistence system exposes an indicator of
/// saturation (readable via the [`PersistState`]) that the caller MUST use to
/// prevent the generation of new persist tasks on a best-effort basis (for
/// example; by blocking any further ingest).
///
/// When the persist queue is saturated, a call to [`IngestState::read()`]
/// returns [`IngestStateError::PersistSaturated`]. Once the backlog of persist
/// jobs is reduced, the [`PersistState`] is switched back to a healthy state
/// and new persist jobs may be generated as normal.
///
/// For details of the exact saturation detection & recovery logic, see
/// [`PersistState`].
///
/// [`IngestStateError::PersistSaturated`]:
///     crate::ingest_state::IngestStateError::PersistSaturated
#[derive(Debug)]
pub(crate) struct PersistHandle {
    /// Task handles for the worker tasks, aborted on drop of all
    /// [`PersistHandle`] instances.
    worker_tasks: Vec<AbortOnDrop<()>>,

    /// While the persistence system exposes the concept of a "persistence
    /// queue" externally, it is actually a set of per-worker queues, and the
    /// global task queue.
    ///
    /// Each individual queue is unbounded, with the logical "persistence queue"
    /// bound by a semaphore to limit the number of global outstanding persist
    /// operations.

    /// The persist task semaphore that bounds the number of outstanding persist
    /// operations across all queues.
    ///
    /// Before enqueuing an item into any of the (unbounded) worker queues, or
    /// (unbounded) global queue, the caller MUST obtain a semaphore permit.
    sem: Arc<Semaphore>,

    /// A global queue of persist tasks that may be executed on any worker in
    /// parallel with any other persist task.
    ///
    /// For correctness, only persist operations that do not cause a sort key
    /// update should be enqueued in the global work queue.
    global_queue: async_channel::Sender<PersistRequest>,

    /// A consistent hash implementation used to consistently map persist tasks
    /// from one partition to the same worker queue.
    ///
    /// These queues are used for persist tasks that modify the partition's sort
    /// key, ensuring sort key updates are serialised per-partition.
    worker_queues: JumpHash<mpsc::UnboundedSender<PersistRequest>>,

    /// Marks and recovers the saturation state of the persist system.
    persist_state: Arc<PersistState>,

    /// A counter tracking the number of enqueued into the persist system.
    enqueued_jobs: U64Counter,
}

impl PersistHandle {
    /// Initialise a new persist actor & obtain the first handle.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new<O>(
        n_workers: usize,
        persist_queue_depth: usize,
        ingest_state: Arc<IngestState>,
        exec: Arc<Executor>,
        store: ParquetStorage,
        catalog: Arc<dyn Catalog>,
        completion_observer: O,
        metrics: &metric::Registry,
    ) -> Self
    where
        O: PersistCompletionObserver + 'static,
    {
        assert_ne!(n_workers, 0, "must run at least 1 persist worker");
        assert_ne!(
            persist_queue_depth, 0,
            "persist queue depth must be non-zero"
        );

        // Log the important configuration parameters of the persist subsystem.
        info!(n_workers, persist_queue_depth, "initialised persist task");

        let worker_state = Arc::new(SharedWorkerState {
            exec,
            store,
            catalog,
            completion_observer,
        });

        // Initialise a histogram to capture persist job duration & time spent
        // in the queue.
        let persist_duration = metrics
            .register_metric::<DurationHistogram>(
                "ingester_persist_active_duration",
                "the distribution of persist job processing duration in seconds",
            )
            .recorder(&[]);
        let queue_duration = metrics
            .register_metric_with_options::<DurationHistogram, _>(
                "ingester_persist_enqueue_duration",
                "the distribution of duration a persist job spent enqueued, \
                waiting to be processed in seconds",
                || {
                    DurationHistogramOptions::new([
                        Duration::from_millis(500),
                        Duration::from_secs(1),
                        Duration::from_secs(2),
                        Duration::from_secs(4),
                        Duration::from_secs(8),
                        Duration::from_secs(16),
                        Duration::from_secs(32),
                        Duration::from_secs(64),
                        Duration::from_secs(128),
                        Duration::from_secs(256),
                        DURATION_MAX,
                    ])
                },
            )
            .recorder(&[]);

        // Set the values of static metrics exporting the configured capacity
        // of the persist system.
        //
        // This allows dashboards/alerts to calculate saturation.
        metrics
            .register_metric::<U64Gauge>(
                "ingester_persist_max_parallelism",
                "the maximum parallelism of persist tasks (number of workers)",
            )
            .recorder(&[])
            .set(n_workers as _);
        metrics
            .register_metric::<U64Gauge>(
                "ingester_persist_max_queue_depth",
                "the maximum parallelism of persist tasks (number of workers)",
            )
            .recorder(&[])
            .set(persist_queue_depth as _);

        // Initialise the global queue.
        //
        // Persist tasks that do not require a sort key update are enqueued into
        // this queue, from which all workers consume.
        let (global_tx, global_rx) = async_channel::unbounded();

        let (tx_handles, worker_tasks): (Vec<_>, Vec<_>) = (0..n_workers)
            .map(|_| {
                let worker_state = Arc::clone(&worker_state);

                // Initialise the worker queue that is not shared across workers
                // allowing the persist code to address a single worker.
                let (tx, rx) = mpsc::unbounded_channel();
                (
                    tx,
                    AbortOnDrop(tokio::spawn(worker::run_task(
                        worker_state,
                        global_rx.clone(),
                        rx,
                        queue_duration.clone(),
                        persist_duration.clone(),
                    ))),
                )
            })
            .unzip();

        assert!(!worker_tasks.is_empty());

        // Initialise the semaphore that bounds the total number of persist jobs
        // in the system.
        let sem = Arc::new(Semaphore::new(persist_queue_depth));

        // Initialise the saturation state as "not saturated" and provide it
        // with the task semaphore and total permit count.
        let persist_state = Arc::new(PersistState::new(
            ingest_state,
            persist_queue_depth,
            Arc::clone(&sem),
            metrics,
        ));

        // Initialise a metric tracking the number of jobs enqueued.
        //
        // When combined with the completion count metric, this allows us to
        // derive the rate of enqueues and the number of outstanding jobs.
        let enqueued_jobs = metrics
            .register_metric::<U64Counter>(
                "ingester_persist_enqueued_jobs",
                "the number of partition persist tasks enqueued",
            )
            .recorder(&[]);

        Self {
            sem,
            global_queue: global_tx,
            worker_queues: JumpHash::new(tx_handles),
            worker_tasks,
            persist_state,
            enqueued_jobs,
        }
    }

    fn assign_worker(&self, r: PersistRequest) {
        debug!(
            partition_id = %r.partition_id(),
            "enqueue persist job to assigned worker"
        );

        // Consistently map partition tasks for this partition ID to the
        // same worker.
        self.worker_queues
            .hash(r.partition_id())
            .send(r)
            .expect("persist worker stopped");
    }
}

#[async_trait]
impl PersistQueue for PersistHandle {
    /// Place `data` from `partition` into the persistence queue.
    ///
    /// This call (asynchronously) waits for space to become available in the
    /// persistence queue.
    ///
    /// Once persistence is complete, the partition will be locked and the sort
    /// key will be updated (if necessary), and
    /// [`PartitionData::mark_persisted()`] is called with `data` to mark the
    /// task as complete.
    ///
    /// Once all persistence related tasks for `data` are complete, the returned
    /// channel publishes a notification.
    ///
    /// Persist tasks may be re-ordered w.r.t their submission order for
    /// performance reasons.
    ///
    /// # Panics
    ///
    /// Panics if the assigned persist worker task has stopped.
    ///
    /// Panics (asynchronously) if the [`PartitionData`]'s sort key is updated
    /// between persistence starting and ending.
    ///
    /// This will panic (asynchronously) if `data` was not from `partition`.
    #[allow(clippy::async_yields_async)] // Callers may want to wait async
    async fn enqueue(
        &self,
        partition: Arc<Mutex<PartitionData>>,
        data: PersistingData,
    ) -> oneshot::Receiver<()> {
        let partition_id = data.partition_id().clone();
        debug!(%partition_id, "enqueuing persistence task");

        // Record a starting timestamp, and increment the number of persist jobs
        // before waiting on the semaphore - this ensures the difference between
        // started and completed includes the full count of pending jobs (even
        // those blocked waiting for queue capacity).
        let enqueued_at = Instant::now();
        self.enqueued_jobs.inc(1);

        // Try and acquire the persist task permit immediately.
        let permit = match Arc::clone(&self.sem).try_acquire_owned() {
            Ok(p) => p, // Success!
            Err(TryAcquireError::Closed) => panic!("persist work semaphore is closed"),
            Err(TryAcquireError::NoPermits) => {
                // The persist system is saturated. Mark the persist system as
                // being saturated to observers.
                //
                // The returned guard MUST be held during the acquire_owned()
                // await below.
                let _guard = PersistState::set_saturated(Arc::clone(&self.persist_state));

                // TODO(test): the guard is held over the await point below

                // Park this task waiting to obtain the permit whilst holding
                // the guard above.
                //
                // If this acquire_owned() is aborted, the guard is dropped and
                // the number of waiters is decremented. If the acquire_owned()
                // is successful, the guard is dropped immediately when leaving
                // this scope, after the permit has been granted.

                Arc::clone(&self.sem)
                    .acquire_owned()
                    .await
                    .expect("persist work semaphore is closed")
            }
        };

        // If the persist job has a known sort key, and it can be determined
        // that the persist job does not require updating that sort key, it can
        // be enqueued in the global queue and executed on any worker.
        //
        // Conversely if the sort key is not yet known (unresolved deferred
        // load) do not wait in this handle to resolve it, and instead
        // pessimistically enqueue the task into a specific worker queue by
        // consistently hashing the partition ID, serialising the execution of
        // the persist job w.r.t other persist jobs for the same partition. It
        // is still executed in parallel with other persist jobs for other
        // partitions.
        //
        // Likewise if it can be determined that a sort key is necessary, it
        // must be serialised via the same mechanism.
        //
        // Do NOT attempt to fetch the sort key in this handler, as doing so
        // would cause a large number of requests against the catalog in a short
        // period of time (bounded by the number of tasks being enqueued).
        // Instead, the workers fetch the sort key, bounding the number of
        // queries to at most `n_workers`.

        let sort_key = match partition.lock().sort_key() {
            SortKeyState::Deferred(v) => {
                let sort_key = v.peek();
                match sort_key {
                    None => None,
                    Some((sort_key, _sort_key_ids)) => sort_key,
                }
            }
            SortKeyState::Provided(v, _) => v.as_ref().cloned(),
        };

        // Build the persist task request.
        let schema = data.schema().clone();
        let (r, notify) = PersistRequest::new(Arc::clone(&partition), data, permit, enqueued_at);

        match sort_key {
            Some(v) => {
                // A sort key is known for this partition. If it can be shown
                // that this persist job does not require a sort key update, it
                // can be parallelised with impunity.
                let data_primary_key = schema.primary_key();
                if let Some(new_sort_key) = adjust_sort_key_columns(&v, &data_primary_key).1 {
                    // This persist operation will require a sort key update.
                    trace!(
                        %partition_id,
                        old_sort_key = %v,
                        %new_sort_key,
                        "persist job will require sort key update"
                    );
                    self.assign_worker(r);
                } else {
                    // This persist operation will not require a sort key
                    // update.
                    debug!(%partition_id, "enqueue persist job to global work queue");
                    self.global_queue.send(r).await.expect("no persist workers");
                }
            }
            None => {
                // If no sort key is known (either because it was unresolved, or
                // not yet set), the task must be serialised w.r.t other persist
                // jobs for the same partition.
                trace!(%partition_id, "persist job has no known sort key");
                self.assign_worker(r);
            }
        }

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

#[cfg(test)]
mod tests {
    use std::{sync::Arc, task::Poll, time::Duration};

    use assert_matches::assert_matches;
    use data_types::SortedColumnSet;
    use futures::Future;
    use iox_catalog::mem::MemCatalog;
    use object_store::memory::InMemory;
    use parquet_file::storage::StorageId;
    use schema::sort::SortKey;
    use test_helpers::timeout::FutureTimeout;
    use tokio::sync::mpsc::error::TryRecvError;

    use super::*;
    use crate::{
        buffer_tree::{
            namespace::name_resolver::mock::MockNamespaceNameProvider,
            partition::resolver::mock::MockPartitionProvider,
            post_write::mock::MockPostWriteObserver, BufferTree,
        },
        deferred_load::DeferredLoad,
        dml_payload::IngestOp,
        dml_sink::DmlSink,
        ingest_state::IngestStateError,
        persist::{
            completion_observer::{mock::MockCompletionObserver, NopObserver},
            tests::{assert_metric_counter, assert_metric_gauge},
        },
        test_util::{
            make_write_op, PartitionDataBuilder, ARBITRARY_NAMESPACE_ID, ARBITRARY_NAMESPACE_NAME,
            ARBITRARY_PARTITION_KEY, ARBITRARY_TABLE_ID, ARBITRARY_TABLE_NAME,
            ARBITRARY_TABLE_PROVIDER, ARBITRARY_TRANSITION_PARTITION_ID,
        },
    };

    /// Construct a partition with the above constants, with the given sort key,
    /// and containing a single write.
    async fn new_partition(sort_key: SortKeyState) -> Arc<Mutex<PartitionData>> {
        let buffer_tree = BufferTree::new(
            Arc::new(MockNamespaceNameProvider::new(&**ARBITRARY_NAMESPACE_NAME)),
            Arc::clone(&*ARBITRARY_TABLE_PROVIDER),
            Arc::new(
                MockPartitionProvider::default().with_partition(
                    PartitionDataBuilder::new()
                        .with_sort_key_state(sort_key)
                        .build(),
                ),
            ),
            Arc::new(MockPostWriteObserver::default()),
            Default::default(),
        );

        buffer_tree
            .apply(IngestOp::Write(make_write_op(
                &ARBITRARY_PARTITION_KEY,
                ARBITRARY_NAMESPACE_ID,
                &ARBITRARY_TABLE_NAME,
                ARBITRARY_TABLE_ID,
                0,
                &format!("{},good=yes level=1000 4242424242", &*ARBITRARY_TABLE_NAME),
                None,
            )))
            .await
            .expect("failed to write partition test dataa");

        Arc::clone(&buffer_tree.partitions().next().unwrap())
    }

    /// A test that ensures the correct destination of a partition that has no
    /// assigned sort key yet (a new partition) that was directly assigned. This
    /// will need a sort key update.
    #[tokio::test]
    async fn test_persist_sort_key_provided_none() {
        let storage = ParquetStorage::new(Arc::new(InMemory::default()), StorageId::from("iox"));
        let metrics = Arc::new(metric::Registry::default());
        let catalog = Arc::new(MemCatalog::new(Arc::clone(&metrics)));

        let mut handle = PersistHandle::new(
            1,
            2,
            Arc::new(IngestState::default()),
            Arc::new(Executor::new_testing()),
            storage,
            catalog,
            Arc::new(MockCompletionObserver::default()),
            &metrics,
        );

        // Kill the workers, and replace the queues so we can inspect the
        // enqueue output.
        handle.worker_tasks = vec![];

        let (global_tx, _global_rx) = async_channel::unbounded();
        handle.global_queue = global_tx;

        let (worker1_tx, mut worker1_rx) = mpsc::unbounded_channel();
        let (worker2_tx, mut worker2_rx) = mpsc::unbounded_channel();
        handle.worker_queues = JumpHash::new([worker1_tx, worker2_tx]);

        // Generate a partition with no known sort key.
        let p = new_partition(SortKeyState::Provided(None, None)).await;
        let data = p.lock().mark_persisting().unwrap();

        // Enqueue it
        let notify = handle.enqueue(p, data).await;

        // And assert it wound up in a worker queue.
        assert!(handle.global_queue.is_empty());

        // Remember which queue it wound up in.
        let mut assigned_worker = &mut worker1_rx;
        let msg = match assigned_worker.try_recv() {
            Ok(v) => v,
            Err(TryRecvError::Disconnected) => panic!("worker channel is closed"),
            Err(TryRecvError::Empty) => {
                assigned_worker = &mut worker2_rx;
                assigned_worker
                    .try_recv()
                    .expect("message was not found in either worker")
            }
        };
        assert_eq!(msg.partition_id(), &*ARBITRARY_TRANSITION_PARTITION_ID);

        // Drop the message, and ensure the notification becomes inactive.
        drop(msg);
        assert_matches!(
            notify.with_timeout_panic(Duration::from_secs(5)).await,
            Err(_)
        );

        // Enqueue another partition for the same ID.
        let p = new_partition(SortKeyState::Provided(None, None)).await;
        let data = p.lock().mark_persisting().unwrap();

        // Enqueue it
        let _notify = handle.enqueue(p, data).await;

        // And ensure it was mapped to the same worker.
        let msg = assigned_worker
            .try_recv()
            .expect("message was not found in either worker");
        assert_eq!(msg.partition_id(), &*ARBITRARY_TRANSITION_PARTITION_ID);
    }

    /// A test that ensures the correct destination of a partition that has no
    /// assigned sort key yet (a new partition) that was resolved by the
    /// deferred load. This will need a sort key update.
    #[tokio::test]
    async fn test_persist_sort_key_deferred_resolved_none_update_necessary() {
        let storage = ParquetStorage::new(Arc::new(InMemory::default()), StorageId::from("iox"));
        let metrics = Arc::new(metric::Registry::default());
        let catalog = Arc::new(MemCatalog::new(Arc::clone(&metrics)));

        let mut handle = PersistHandle::new(
            1,
            2,
            Arc::new(IngestState::default()),
            Arc::new(Executor::new_testing()),
            storage,
            catalog,
            Arc::new(MockCompletionObserver::default()),
            &metrics,
        );

        // Kill the workers, and replace the queues so we can inspect the
        // enqueue output.
        handle.worker_tasks = vec![];

        let (global_tx, _global_rx) = async_channel::unbounded();
        handle.global_queue = global_tx;

        let (worker1_tx, mut worker1_rx) = mpsc::unbounded_channel();
        let (worker2_tx, mut worker2_rx) = mpsc::unbounded_channel();
        handle.worker_queues = JumpHash::new([worker1_tx, worker2_tx]);

        // Generate a partition with a resolved, but empty sort key.
        let p = new_partition(SortKeyState::Deferred(Arc::new(DeferredLoad::new(
            Duration::from_secs(1),
            async { (None, None) },
            &metrics,
        ))))
        .await;
        let (loader, data) = {
            let mut p = p.lock();
            (p.sort_key().clone(), p.mark_persisting().unwrap())
        };
        // Ensure the key is resolved.
        assert_matches!(loader.get_sort_key().await, None);
        assert_matches!(loader.get_sort_key_ids().await, None);

        // Enqueue it
        let notify = handle.enqueue(p, data).await;

        // And assert it wound up in a worker queue.
        assert!(handle.global_queue.is_empty());

        // Remember which queue it wound up in.
        let mut assigned_worker = &mut worker1_rx;
        let msg = match assigned_worker.try_recv() {
            Ok(v) => v,
            Err(TryRecvError::Disconnected) => panic!("worker channel is closed"),
            Err(TryRecvError::Empty) => {
                assigned_worker = &mut worker2_rx;
                assigned_worker
                    .try_recv()
                    .expect("message was not found in either worker")
            }
        };
        assert_eq!(msg.partition_id(), &*ARBITRARY_TRANSITION_PARTITION_ID);

        // Drop the message, and ensure the notification becomes inactive.
        drop(msg);
        assert_matches!(
            notify.with_timeout_panic(Duration::from_secs(5)).await,
            Err(_)
        );

        // Enqueue another partition for the same ID and same (resolved)
        // deferred load instance.
        let p = new_partition(loader).await;
        let data = p.lock().mark_persisting().unwrap();

        // Enqueue it
        let _notify = handle.enqueue(p, data).await;

        // And ensure it was mapped to the same worker.
        let msg = assigned_worker
            .try_recv()
            .expect("message was not found in either worker");
        assert_eq!(msg.partition_id(), &*ARBITRARY_TRANSITION_PARTITION_ID);
    }

    /// A test that ensures the correct destination of a partition that has an
    /// assigned sort key, but the data differs and a sort key update is
    /// necessary.
    #[tokio::test]
    async fn test_persist_sort_key_deferred_resolved_some_update_necessary() {
        let storage = ParquetStorage::new(Arc::new(InMemory::default()), StorageId::from("iox"));
        let metrics = Arc::new(metric::Registry::default());
        let catalog = Arc::new(MemCatalog::new(Arc::clone(&metrics)));

        let mut handle = PersistHandle::new(
            1,
            2,
            Arc::new(IngestState::default()),
            Arc::new(Executor::new_testing()),
            storage,
            catalog,
            Arc::new(MockCompletionObserver::default()),
            &metrics,
        );

        // Kill the workers, and replace the queues so we can inspect the
        // enqueue output.
        handle.worker_tasks = vec![];

        let (global_tx, _global_rx) = async_channel::unbounded();
        handle.global_queue = global_tx;

        let (worker1_tx, mut worker1_rx) = mpsc::unbounded_channel();
        let (worker2_tx, mut worker2_rx) = mpsc::unbounded_channel();
        handle.worker_queues = JumpHash::new([worker1_tx, worker2_tx]);

        // Generate a partition with a resolved sort key that does not reflect
        // the data within the partition's buffer.
        let p = new_partition(SortKeyState::Deferred(Arc::new(DeferredLoad::new(
            Duration::from_secs(1),
            async {
                (
                    Some(SortKey::from_columns(["time", "some-other-column"])),
                    Some(SortedColumnSet::from([1, 2])),
                )
            },
            &metrics,
        ))))
        .await;
        let (loader, data) = {
            let mut p = p.lock();
            (p.sort_key().clone(), p.mark_persisting().unwrap())
        };
        // Ensure the key is resolved.
        assert_matches!(loader.get_sort_key().await, Some(_));

        // Enqueue it
        let notify = handle.enqueue(p, data).await;

        // And assert it wound up in a worker queue.
        assert!(handle.global_queue.is_empty());

        // Remember which queue it wound up in.
        let mut assigned_worker = &mut worker1_rx;
        let msg = match assigned_worker.try_recv() {
            Ok(v) => v,
            Err(TryRecvError::Disconnected) => panic!("worker channel is closed"),
            Err(TryRecvError::Empty) => {
                assigned_worker = &mut worker2_rx;
                assigned_worker
                    .try_recv()
                    .expect("message was not found in either worker")
            }
        };
        assert_eq!(msg.partition_id(), &*ARBITRARY_TRANSITION_PARTITION_ID);

        // Drop the message, and ensure the notification becomes inactive.
        drop(msg);
        assert_matches!(
            notify.with_timeout_panic(Duration::from_secs(5)).await,
            Err(_)
        );

        // Enqueue another partition for the same ID and same (resolved)
        // deferred load instance.
        let p = new_partition(loader).await;
        let data = p.lock().mark_persisting().unwrap();

        // Enqueue it
        let _notify = handle.enqueue(p, data).await;

        // And ensure it was mapped to the same worker.
        let msg = assigned_worker
            .try_recv()
            .expect("message was not found in either worker");
        assert_eq!(msg.partition_id(), &*ARBITRARY_TRANSITION_PARTITION_ID);
    }

    /// A test that a partition that does not require a sort key update is
    /// enqueued into the global queue.
    #[tokio::test]
    async fn test_persist_sort_key_no_update_necessary() {
        let storage = ParquetStorage::new(Arc::new(InMemory::default()), StorageId::from("iox"));
        let metrics = Arc::new(metric::Registry::default());
        let catalog = Arc::new(MemCatalog::new(Arc::clone(&metrics)));

        let mut handle = PersistHandle::new(
            1,
            2,
            Arc::new(IngestState::default()),
            Arc::new(Executor::new_testing()),
            storage,
            catalog,
            Arc::new(MockCompletionObserver::default()),
            &metrics,
        );

        // Kill the workers, and replace the queues so we can inspect the
        // enqueue output.
        handle.worker_tasks = vec![];

        let (global_tx, global_rx) = async_channel::unbounded();
        handle.global_queue = global_tx;

        let (worker1_tx, mut worker1_rx) = mpsc::unbounded_channel();
        let (worker2_tx, mut worker2_rx) = mpsc::unbounded_channel();
        handle.worker_queues = JumpHash::new([worker1_tx, worker2_tx]);

        // Generate a partition with a resolved sort key that does not reflect
        // the data within the partition's buffer.
        let p = new_partition(SortKeyState::Deferred(Arc::new(DeferredLoad::new(
            Duration::from_secs(1),
            async {
                (
                    Some(SortKey::from_columns(["time", "good"])),
                    Some(SortedColumnSet::from([1, 2])),
                )
            },
            &metrics,
        ))))
        .await;
        let (loader, data) = {
            let mut p = p.lock();
            (p.sort_key().clone(), p.mark_persisting().unwrap())
        };
        // Ensure the key is resolved.
        assert_matches!(loader.get_sort_key().await, Some(_));

        // Enqueue it
        let notify = handle.enqueue(p, data).await;

        // Assert the task did not get enqueued in a worker
        assert_matches!(worker1_rx.try_recv(), Err(TryRecvError::Empty));
        assert_matches!(worker2_rx.try_recv(), Err(TryRecvError::Empty));

        // And assert it wound up in the global queue.
        let msg = global_rx
            .try_recv()
            .expect("task should be in global queue");
        assert_eq!(msg.partition_id(), &*ARBITRARY_TRANSITION_PARTITION_ID);

        // Drop the message, and ensure the notification becomes inactive.
        drop(msg);
        assert_matches!(
            notify.with_timeout_panic(Duration::from_secs(5)).await,
            Err(_)
        );

        // Enqueue another partition for the same ID and same (resolved)
        // deferred load instance.
        let p = new_partition(loader).await;
        let data = p.lock().mark_persisting().unwrap();

        // Enqueue it
        let _notify = handle.enqueue(p, data).await;

        // And ensure it was mapped to the same worker.
        let msg = global_rx
            .try_recv()
            .expect("task should be in global queue");
        assert_eq!(msg.partition_id(), &*ARBITRARY_TRANSITION_PARTITION_ID);
    }

    /// A test that a ensures tasks waiting to be enqueued (waiting on the
    /// semaphore) appear in the metrics.
    #[tokio::test]
    async fn test_persist_saturated_enqueue_counter() {
        let storage = ParquetStorage::new(Arc::new(InMemory::default()), StorageId::from("iox"));
        let metrics = Arc::new(metric::Registry::default());
        let catalog = Arc::new(MemCatalog::new(Arc::clone(&metrics)));
        let ingest_state = Arc::new(IngestState::default());

        let mut handle = PersistHandle::new(
            1,
            1,
            Arc::clone(&ingest_state),
            Arc::new(Executor::new_testing()),
            storage,
            catalog,
            NopObserver,
            &metrics,
        );
        assert!(ingest_state.read().is_ok());

        // Kill the workers, and replace the queues so we can inspect the
        // enqueue output.
        handle.worker_tasks = vec![];

        let (global_tx, _global_rx) = async_channel::unbounded();
        handle.global_queue = global_tx;

        let (worker1_tx, _worker1_rx) = mpsc::unbounded_channel();
        let (worker2_tx, _worker2_rx) = mpsc::unbounded_channel();
        handle.worker_queues = JumpHash::new([worker1_tx, worker2_tx]);

        // Generate a partition
        let p = new_partition(SortKeyState::Deferred(Arc::new(DeferredLoad::new(
            Duration::from_secs(1),
            async {
                (
                    Some(SortKey::from_columns(["time", "good"])),
                    Some(SortedColumnSet::from([1, 2])),
                )
            },
            &metrics,
        ))))
        .await;
        let data = p.lock().mark_persisting().unwrap();

        // Enqueue it
        let _notify1 = handle.enqueue(p, data).await;

        // Generate a second partition
        let p = new_partition(SortKeyState::Deferred(Arc::new(DeferredLoad::new(
            Duration::from_secs(1),
            async {
                (
                    Some(SortKey::from_columns(["time", "good"])),
                    Some(SortedColumnSet::from([1, 2])),
                )
            },
            &metrics,
        ))))
        .await;
        let data = p.lock().mark_persisting().unwrap();

        // Enqueue it
        let fut = handle.enqueue(p, data);

        // Poll it to the pending state
        let waker = futures::task::noop_waker();
        let mut cx = futures::task::Context::from_waker(&waker);
        futures::pin_mut!(fut);

        let poll = std::pin::Pin::new(&mut fut).poll(&mut cx);
        assert_matches!(poll, Poll::Pending);

        // The queue is now full, and the second enqueue above is blocked
        assert_matches!(ingest_state.read(), Err(IngestStateError::PersistSaturated));

        // And the counter shows two persist ops.
        assert_metric_counter(&metrics, "ingester_persist_enqueued_jobs", 2);
    }

    /// Export metrics showing the static config values.
    #[tokio::test]
    async fn test_static_config_metrics() {
        let storage = ParquetStorage::new(Arc::new(InMemory::default()), StorageId::from("iox"));
        let metrics = Arc::new(metric::Registry::default());
        let catalog = Arc::new(MemCatalog::new(Arc::clone(&metrics)));
        let ingest_state = Arc::new(IngestState::default());

        let _handle = PersistHandle::new(
            5,
            42,
            Arc::clone(&ingest_state),
            Arc::new(Executor::new_testing()),
            storage,
            catalog,
            NopObserver,
            &metrics,
        );

        assert_metric_gauge(&metrics, "ingester_persist_max_parallelism", 5);
        assert_metric_gauge(&metrics, "ingester_persist_max_queue_depth", 42);
    }
}
