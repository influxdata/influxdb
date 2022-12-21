use std::sync::Arc;

use async_channel::RecvError;
use data_types::ParquetFileParams;
use iox_catalog::interface::Catalog;
use iox_query::exec::Executor;

use parquet_file::storage::ParquetStorage;

use tokio::sync::mpsc;

use super::context::{Context, PersistError, PersistRequest};

/// State shared across workers.
#[derive(Debug)]
pub(super) struct SharedWorkerState {
    pub(super) exec: Arc<Executor>,
    pub(super) store: ParquetStorage,
    pub(super) catalog: Arc<dyn Catalog>,
}

/// The worker routine that drives a [`PersistRequest`] to completion,
/// prioritising jobs from the worker-specific queue, and falling back to jobs
/// from the global work queue.
///
/// Optimistically compacts the [`PersistingData`] using the locally cached sort
/// key read from the [`PartitionData`] instance. If this key proves to be
/// stale, the compaction is retried with the new key.
///
/// See <https://github.com/influxdata/influxdb_iox/issues/6439>.
///
/// ```text
///           ┌───────┐
///           │COMPACT│
///           └───┬───┘
///           ┌───▽──┐
///           │UPLOAD│
///           └───┬──┘
///        _______▽________     ┌────────────────┐
///       ╱                ╲    │TRY UPDATE      │
///      ╱ NEEDS CATALOG    ╲___│CATALOG SORT KEY│
///      ╲ SORT KEY UPDATE? ╱yes└────────┬───────┘
///       ╲________________╱      _______▽________     ┌────────────┐
///               │no            ╱                ╲    │RESTART WITH│
///               │             ╱ SAW CONCURRENT   ╲___│NEW SORT KEY│
///               │             ╲ SORT KEY UPDATE? ╱yes└────────────┘
///               │              ╲________________╱
///               │                      │no
///               └─────┬────────────────┘
///               ┌─────▽─────┐
///               │ADD PARQUET│
///               │TO CATALOG │
///               └─────┬─────┘
///             ┌───────▽──────┐
///             │NOTIFY PERSIST│
///             │JOB COMPLETE  │
///             └──────────────┘
/// ```
///
/// [`PersistingData`]:
///     crate::buffer_tree::partition::persisting::PersistingData
/// [`PartitionData`]: crate::buffer_tree::partition::PartitionData
pub(super) async fn run_task(
    worker_state: Arc<SharedWorkerState>,
    global_queue: async_channel::Receiver<PersistRequest>,
    mut rx: mpsc::UnboundedReceiver<PersistRequest>,
) {
    loop {
        let req = tokio::select! {
            // Bias the channel polling to prioritise work in the
            // worker-specific queue.
            //
            // This causes the worker to do the work assigned to it specifically
            // first, falling back to taking jobs from the global queue if it
            // has no assigned work.
            //
            // This allows persist jobs to be reordered w.r.t the order in which
            // they were enqueued with queue_persist().
            biased;

            v = rx.recv() => {
                match v {
                    Some(v) => v,
                    None => {
                        // The worker channel is closed.
                        return
                    }
                }
            }
            v = global_queue.recv() => {
                match v {
                    Ok(v) => v,
                    Err(RecvError) => {
                        // The global channel is closed.
                        return
                    },
                }
            }
        };

        let mut ctx = Context::new(req, Arc::clone(&worker_state));

        // Compact the data, generate the parquet file from the result, and
        // upload it to object storage.
        //
        // If this process generated a new sort key that must be added to the
        // catalog, attempt to update the catalog with a compare-and-swap
        // operation; if this update fails due to a concurrent sort key update,
        // the compaction must be redone with the new sort key and uploaded
        // before continuing.
        let parquet_table_data = loop {
            match compact_and_upload(&mut ctx).await {
                Ok(v) => break v,
                Err(PersistError::ConcurrentSortKeyUpdate(_)) => continue,
            };
        };

        // Make the newly uploaded parquet file visible to other nodes.
        let object_store_id = ctx.update_catalog_parquet(parquet_table_data).await;
        // And finally mark the persist job as complete and notify any
        // observers.
        ctx.mark_complete(object_store_id);
    }
}

/// Run a compaction on the [`PersistingData`], generate a parquet file and
/// upload it to object storage.
///
/// If in the course of this the sort key is updated, this function attempts to
/// update the sort key in the catalog. This MAY fail because another node has
/// concurrently done the same and the persist must be restarted.
///
/// See <https://github.com/influxdata/influxdb_iox/issues/6439>.
///
/// [`PersistingData`]:
///     crate::buffer_tree::partition::persisting::PersistingData
async fn compact_and_upload(ctx: &mut Context) -> Result<ParquetFileParams, PersistError> {
    let compacted = ctx.compact().await;
    let (sort_key_update, parquet_table_data) = ctx.upload(compacted).await;

    if let Some(update) = sort_key_update {
        ctx.update_catalog_sort_key(update, parquet_table_data.object_store_id)
            .await?
    }

    Ok(parquet_table_data)
}
