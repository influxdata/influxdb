use std::{ops::ControlFlow, sync::Arc};

use async_channel::RecvError;
use backoff::Backoff;
use data_types::{CompactionLevel, ParquetFileParams, SequenceNumber};
use iox_catalog::interface::{get_table_schema_by_id, CasFailure, Catalog};
use iox_query::exec::Executor;
use iox_time::{SystemProvider, TimeProvider};
use metric::DurationHistogram;
use observability_deps::tracing::{debug, info, warn};
use parquet_file::{metadata::IoxMetadata, storage::ParquetStorage};
use schema::sort::SortKey;
use tokio::{sync::mpsc, time::Instant};
use uuid::Uuid;

use crate::persist::compact::compact_persisting_batch;

use super::{
    compact::CompactedStream,
    completion_observer::PersistCompletionObserver,
    context::{Context, PersistError, PersistRequest},
};

/// State shared across workers.
#[derive(Debug)]
pub(super) struct SharedWorkerState<O> {
    pub(super) exec: Arc<Executor>,
    pub(super) store: ParquetStorage,
    pub(super) catalog: Arc<dyn Catalog>,
    pub(super) completion_observer: O,
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
pub(super) async fn run_task<O>(
    worker_state: Arc<SharedWorkerState<O>>,
    global_queue: async_channel::Receiver<PersistRequest>,
    mut rx: mpsc::UnboundedReceiver<PersistRequest>,
    queue_duration: DurationHistogram,
    persist_duration: DurationHistogram,
) where
    O: PersistCompletionObserver,
{
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

        let mut ctx = Context::new(req);

        // Capture the time spent in the queue.
        let started_at = Instant::now();
        queue_duration.record(started_at.duration_since(ctx.enqueued_at()));

        // Compact the data, generate the parquet file from the result, and
        // upload it to object storage.
        //
        // If this process generated a new sort key that must be added to the
        // catalog, attempt to update the catalog with a compare-and-swap
        // operation; if this update fails due to a concurrent sort key update,
        // the compaction must be redone with the new sort key and uploaded
        // before continuing.
        let parquet_table_data = loop {
            match compact_and_upload(&mut ctx, &worker_state).await {
                Ok(v) => break v,
                Err(PersistError::ConcurrentSortKeyUpdate(_)) => continue,
            };
        };

        // Make the newly uploaded parquet file visible to other nodes.
        let object_store_id = update_catalog_parquet(&ctx, &worker_state, parquet_table_data).await;

        // And finally mark the persist job as complete and notify any
        // observers.
        ctx.mark_complete(object_store_id, &worker_state.completion_observer)
            .await;

        // Capture the time spent actively persisting.
        let now = Instant::now();
        persist_duration.record(now.duration_since(started_at));
    }
}

/// Run a compaction on the [`PersistingData`], generate a parquet file and
/// upload it to object storage.
///
/// This function composes functionality from the smaller [`compact()`],
/// [`upload()`], and [`update_catalog_sort_key()`] functions.
///
/// If in the course of this the sort key is updated, this function attempts to
/// update the sort key in the catalog. This MAY fail because another node has
/// concurrently done the same and the persist must be restarted.
///
/// See <https://github.com/influxdata/influxdb_iox/issues/6439>.
///
/// [`PersistingData`]:
///     crate::buffer_tree::partition::persisting::PersistingData
async fn compact_and_upload<O>(
    ctx: &mut Context,
    worker_state: &SharedWorkerState<O>,
) -> Result<ParquetFileParams, PersistError>
where
    O: Send + Sync,
{
    let compacted = compact(ctx, worker_state).await;
    let (sort_key_update, parquet_table_data) = upload(ctx, worker_state, compacted).await;

    if let Some(update) = sort_key_update {
        update_catalog_sort_key(
            ctx,
            worker_state,
            update,
            parquet_table_data.object_store_id,
        )
        .await?
    }

    Ok(parquet_table_data)
}

/// Compact the data in `ctx` using sorted by the sort key returned from
/// [`Context::sort_key()`].
async fn compact<O>(ctx: &Context, worker_state: &SharedWorkerState<O>) -> CompactedStream
where
    O: Send + Sync,
{
    let sort_key = ctx.sort_key().get().await;

    debug!(
        namespace_id = %ctx.namespace_id(),
        namespace_name = %ctx.namespace_name(),
        table_id = %ctx.table_id(),
        table_name = %ctx.table_name(),
        partition_id = %ctx.partition_id(),
        partition_key = %ctx.partition_key(),
        ?sort_key,
        "compacting partition"
    );

    assert!(!ctx.data().record_batches().is_empty());

    // Run a compaction sort the data and resolve any duplicate values.
    //
    // This demands the deferred load values and may have to wait for them
    // to be loaded before compaction starts.
    compact_persisting_batch(
        &worker_state.exec,
        sort_key,
        ctx.table_name().get().await,
        ctx.data().query_adaptor(),
    )
    .await
    .expect("unable to compact persisting batch")
}

/// Upload the compacted data in `compacted`, returning the new sort key value
/// and parquet metadata to be upserted into the catalog.
async fn upload<O>(
    ctx: &Context,
    worker_state: &SharedWorkerState<O>,
    compacted: CompactedStream,
) -> (Option<SortKey>, ParquetFileParams)
where
    O: Send + Sync,
{
    let CompactedStream {
        stream: record_stream,
        catalog_sort_key_update,
        data_sort_key,
    } = compacted;

    // Generate a UUID to uniquely identify this parquet file in
    // object storage.
    let object_store_id = Uuid::new_v4();

    debug!(
        namespace_id = %ctx.namespace_id(),
        namespace_name = %ctx.namespace_name(),
        table_id = %ctx.table_id(),
        table_name = %ctx.table_name(),
        partition_id = %ctx.partition_id(),
        partition_key = %ctx.partition_key(),
        %object_store_id,
        sort_key = %data_sort_key,
        "uploading partition parquet"
    );

    // Construct the metadata for this parquet file.
    let time_now = SystemProvider::new().now();
    let iox_metadata = IoxMetadata {
        object_store_id,
        creation_timestamp: time_now,
        shard_id: ctx.transition_shard_id(),
        namespace_id: ctx.namespace_id(),
        namespace_name: Arc::clone(&*ctx.namespace_name().get().await),
        table_id: ctx.table_id(),
        table_name: Arc::clone(&*ctx.table_name().get().await),
        partition_id: ctx.partition_id(),
        partition_key: ctx.partition_key().clone(),
        max_sequence_number: SequenceNumber::new(0), // TODO: not ordered!
        compaction_level: CompactionLevel::Initial,
        sort_key: Some(data_sort_key),
        max_l0_created_at: time_now,
    };

    // Save the compacted data to a parquet file in object storage.
    //
    // This call retries until it completes.
    let (md, file_size) = worker_state
        .store
        .upload(record_stream, &iox_metadata)
        .await
        .expect("unexpected fatal persist error");

    debug!(
        namespace_id = %ctx.namespace_id(),
        namespace_name = %ctx.namespace_name(),
        table_id = %ctx.table_id(),
        table_name = %ctx.table_name(),
        partition_id = %ctx.partition_id(),
        partition_key = %ctx.partition_key(),
        %object_store_id,
        file_size,
        "partition parquet uploaded"
    );

    // Read the table schema from the catalog to act as a map of column name
    // -> column IDs.
    let table_schema = Backoff::new(&Default::default())
        .retry_all_errors("get table schema", || async {
            let mut repos = worker_state.catalog.repositories().await;
            get_table_schema_by_id(ctx.table_id(), repos.as_mut()).await
        })
        .await
        .expect("retry forever");

    // Build the data that must be inserted into the parquet_files catalog
    // table in order to make the file visible to queriers.
    let parquet_table_data =
        iox_metadata.to_parquet_file(ctx.partition_id(), file_size, &md, |name| {
            table_schema.columns.get(name).expect("unknown column").id
        });

    (catalog_sort_key_update, parquet_table_data)
}

/// Update the sort key value stored in the catalog for this [`Context`].
///
/// # Concurrent Updates
///
/// If a concurrent sort key change is detected (issued by another node) then
/// this method updates the sort key in `ctx` to reflect the newly observed
/// value and returns [`PersistError::ConcurrentSortKeyUpdate`] to the caller.
async fn update_catalog_sort_key<O>(
    ctx: &mut Context,
    worker_state: &SharedWorkerState<O>,
    new_sort_key: SortKey,
    object_store_id: Uuid,
) -> Result<(), PersistError>
where
    O: Send + Sync,
{
    let old_sort_key = ctx
        .sort_key()
        .get()
        .await
        .map(|v| v.to_columns().map(|v| v.to_string()).collect::<Vec<_>>());

    debug!(
        %object_store_id,
        namespace_id = %ctx.namespace_id(),
        namespace_name = %ctx.namespace_name(),
        table_id = %ctx.table_id(),
        table_name = %ctx.table_name(),
        partition_id = %ctx.partition_id(),
        partition_key = %ctx.partition_key(),
        ?new_sort_key,
        ?old_sort_key,
        "updating partition sort key"
    );

    let update_result = Backoff::new(&Default::default())
        .retry_with_backoff("cas_sort_key", || {
            let old_sort_key = old_sort_key.clone();
            let new_sort_key_str = new_sort_key.to_columns().collect::<Vec<_>>();
            let catalog = Arc::clone(&worker_state.catalog);
            let ctx = &ctx;
            async move {
                let mut repos = catalog.repositories().await;
                match repos
                    .partitions()
                    .cas_sort_key(ctx.partition_id(), old_sort_key.clone(), &new_sort_key_str)
                    .await
                {
                    Ok(_) => ControlFlow::Break(Ok(())),
                    Err(CasFailure::QueryError(e)) => ControlFlow::Continue(e),
                    Err(CasFailure::ValueMismatch(observed)) if observed == new_sort_key_str => {
                        // A CAS failure occurred because of a concurrent
                        // sort key update, however the new catalog sort key
                        // exactly matches the sort key this node wants to
                        // commit.
                        //
                        // This is the sad-happy path, and this task can
                        // continue.
                        info!(
                            %object_store_id,
                            namespace_id = %ctx.namespace_id(),
                            namespace_name = %ctx.namespace_name(),
                            table_id = %ctx.table_id(),
                            table_name = %ctx.table_name(),
                            partition_id = %ctx.partition_id(),
                            partition_key = %ctx.partition_key(),
                            expected=?old_sort_key,
                            ?observed,
                            update=?new_sort_key_str,
                            "detected matching concurrent sort key update"
                        );
                        ControlFlow::Break(Ok(()))
                    }
                    Err(CasFailure::ValueMismatch(observed)) => {
                        // Another ingester concurrently updated the sort
                        // key.
                        //
                        // This breaks a sort-key update invariant - sort
                        // key updates MUST be serialised. This persist must
                        // be retried.
                        //
                        // See:
                        //   https://github.com/influxdata/influxdb_iox/issues/6439
                        //
                        warn!(
                            %object_store_id,
                            namespace_id = %ctx.namespace_id(),
                            namespace_name = %ctx.namespace_name(),
                            table_id = %ctx.table_id(),
                            table_name = %ctx.table_name(),
                            partition_id = %ctx.partition_id(),
                            partition_key = %ctx.partition_key(),
                            expected=?old_sort_key,
                            ?observed,
                            update=?new_sort_key_str,
                            "detected concurrent sort key update, regenerating parquet"
                        );
                        // Stop the retry loop with an error containing the
                        // newly observed sort key.
                        ControlFlow::Break(Err(PersistError::ConcurrentSortKeyUpdate(
                            SortKey::from_columns(observed),
                        )))
                    }
                }
            }
        })
        .await
        .expect("retry forever");

    match update_result {
        Ok(_) => {}
        Err(PersistError::ConcurrentSortKeyUpdate(new_key)) => {
            // Update the cached sort key in the Context (which pushes it
            // through into the PartitionData also) to reflect the newly
            // observed value for the next attempt.
            ctx.set_partition_sort_key(new_key.clone()).await;

            return Err(PersistError::ConcurrentSortKeyUpdate(new_key));
        }
    }

    // Update the sort key in the Context & PartitionData.
    ctx.set_partition_sort_key(new_sort_key.clone()).await;

    debug!(
        %object_store_id,
        namespace_id = %ctx.namespace_id(),
        namespace_name = %ctx.namespace_name(),
        table_id = %ctx.table_id(),
        table_name = %ctx.table_name(),
        partition_id = %ctx.partition_id(),
        partition_key = %ctx.partition_key(),
        ?old_sort_key,
        %new_sort_key,
        "adjusted partition sort key"
    );

    Ok(())
}

async fn update_catalog_parquet<O>(
    ctx: &Context,
    worker_state: &SharedWorkerState<O>,
    parquet_table_data: ParquetFileParams,
) -> Uuid
where
    O: Send + Sync,
{
    // Extract the object store ID to the local scope so that it can easily
    // be referenced in debug logging to aid correlation of persist events
    // for a specific file.
    let object_store_id = parquet_table_data.object_store_id;

    debug!(
        namespace_id = %ctx.namespace_id(),
        namespace_name = %ctx.namespace_name(),
        table_id = %ctx.table_id(),
        table_name = %ctx.table_name(),
        partition_id = %ctx.partition_id(),
        partition_key = %ctx.partition_key(),
        %object_store_id,
        ?parquet_table_data,
        "updating catalog parquet table"
    );

    // Add the parquet file to the catalog.
    //
    // This has the effect of allowing the queriers to "discover" the
    // parquet file by polling / querying the catalog.
    Backoff::new(&Default::default())
        .retry_all_errors("add parquet file to catalog", || async {
            let mut repos = worker_state.catalog.repositories().await;
            let parquet_file = repos
                .parquet_files()
                .create(parquet_table_data.clone())
                .await?;

            debug!(
                namespace_id = %ctx.namespace_id(),
                namespace_name = %ctx.namespace_name(),
                table_id = %ctx.table_id(),
                table_name = %ctx.table_name(),
                partition_id = %ctx.partition_id(),
                partition_key = %ctx.partition_key(),
                %object_store_id,
                ?parquet_table_data,
                parquet_file_id=?parquet_file.id,
                "parquet file added to catalog"
            );

            // compiler insisted on getting told the type of the error :shrug:
            Ok(()) as Result<(), iox_catalog::interface::Error>
        })
        .await
        .expect("retry forever");

    object_store_id
}
