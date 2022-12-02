use std::sync::Arc;

use iox_catalog::interface::Catalog;
use iox_query::exec::Executor;
use parquet_file::storage::ParquetStorage;
use sharder::JumpHash;
use tokio::{sync::mpsc, task::JoinHandle};

use super::context::{Context, PersistRequest};

/// An actor implementation that fans out incoming persistence jobs to a set of
/// workers.
///
/// See [`PersistHandle`].
///
/// [`PersistHandle`]: super::handle::PersistHandle
#[must_use = "PersistActor must be ran by calling run()"]
pub(crate) struct PersistActor {
    rx: mpsc::Receiver<PersistRequest>,

    /// THe state/dependencies shared across all worker tasks.
    inner: Arc<Inner>,

    /// A consistent hash implementation used to consistently map buffers from
    /// one partition to the same worker queue.
    ///
    /// This ensures persistence is serialised per-partition, but in parallel
    /// across partitions (up to the number of worker tasks).
    persist_queues: JumpHash<mpsc::Sender<PersistRequest>>,

    /// Task handles for the worker tasks, aborted on drop of this
    /// [`PersistActor`].
    tasks: Vec<JoinHandle<()>>,
}

impl Drop for PersistActor {
    fn drop(&mut self) {
        // Stop all background tasks when the actor goes out of scope.
        self.tasks.iter().for_each(|v| v.abort())
    }
}

impl PersistActor {
    pub(super) fn new(
        rx: mpsc::Receiver<PersistRequest>,
        exec: Arc<Executor>,
        store: ParquetStorage,
        catalog: Arc<dyn Catalog>,
        workers: usize,
        worker_queue_depth: usize,
    ) -> Self {
        let inner = Arc::new(Inner {
            exec,
            store,
            catalog,
        });

        let (tx_handles, tasks): (Vec<_>, Vec<_>) = (0..workers)
            .map(|_| {
                let inner = Arc::clone(&inner);
                let (tx, rx) = mpsc::channel(worker_queue_depth);
                (tx, tokio::spawn(run_task(inner, rx)))
            })
            .unzip();

        // TODO(test): N workers running
        // TODO(test): queue depths

        Self {
            rx,
            inner,
            persist_queues: JumpHash::new(tx_handles),
            tasks,
        }
    }

    /// Execute this actor task and block until all [`PersistHandle`] are
    /// dropped.
    ///
    /// [`PersistHandle`]: super::handle::PersistHandle
    pub(crate) async fn run(mut self) {
        while let Some(req) = self.rx.recv().await {
            let tx = self.persist_queues.hash(req.partition_id());
            tx.send(req).await.expect("persist worker has stopped;")
        }
    }
}

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
