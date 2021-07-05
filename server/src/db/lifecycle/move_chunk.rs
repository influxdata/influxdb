pub(crate) use crate::db::chunk::DbChunk;
use crate::db::{catalog::chunk::CatalogChunk, lifecycle::compute_sort_key};
use ::lifecycle::LifecycleWriteGuard;
use data_types::job::Job;
use futures::StreamExt;

use observability_deps::tracing::{debug, info};
use query::{exec::ExecutorType, frontend::reorg::ReorgPlanner, QueryChunkMeta};
use read_buffer::{ChunkMetrics as ReadBufferChunkMetrics, RBChunk};
use std::{future::Future, sync::Arc};
use tracker::{TaskTracker, TrackedFuture, TrackedFutureExt};

use super::{error::Result, LockableCatalogChunk};

/// The implementation for moving a chunk to the read buffer
///
/// Returns a future registered with the tracker registry, and the corresponding tracker
/// The caller can either spawn this future to tokio, or block directly on it
pub fn move_chunk_to_read_buffer(
    mut guard: LifecycleWriteGuard<'_, CatalogChunk, LockableCatalogChunk>,
) -> Result<(
    TaskTracker<Job>,
    TrackedFuture<impl Future<Output = Result<Arc<DbChunk>>> + Send>,
)> {
    let db = Arc::clone(&guard.data().db);
    let addr = guard.addr().clone();
    // TODO: Use ChunkAddr within Job
    let (tracker, registration) = db.jobs.register(Job::CloseChunk {
        db_name: addr.db_name.to_string(),
        partition_key: addr.partition_key.to_string(),
        table_name: addr.table_name.to_string(),
        chunk_id: addr.chunk_id,
    });

    // update the catalog to say we are processing this chunk and
    // then drop the lock while we do the work
    guard.set_moving(&registration)?;
    let table_summary = guard.table_summary();

    // snapshot the data
    let query_chunks = vec![DbChunk::snapshot(&*guard)];

    // Drop locks
    let chunk = guard.unwrap().chunk;

    // create a new read buffer chunk with memory tracking
    let metrics = db
        .metrics_registry
        .register_domain_with_labels("read_buffer", db.metric_labels.clone());

    let mut rb_chunk = RBChunk::new(
        &table_summary.name,
        ReadBufferChunkMetrics::new(&metrics, db.catalog.metrics().memory().read_buffer()),
    );

    let ctx = db.exec.new_context(ExecutorType::Reorg);

    let fut = async move {
        info!(chunk=%addr, "chunk marked MOVING, loading tables into read buffer");

        let key = compute_sort_key(query_chunks.iter().map(|x| x.summary()));

        // Cannot move query_chunks as the sort key borrows the column names
        let (_schema, plan) =
            ReorgPlanner::new().compact_plan(query_chunks.iter().map(Arc::clone), key)?;

        let physical_plan = ctx.prepare_plan(&plan)?;
        let mut stream = ctx.execute(physical_plan).await?;

        // Collect results into RUB chunk
        while let Some(batch) = stream.next().await {
            rb_chunk.upsert_table(batch?)
        }

        // Can drop and re-acquire as lifecycle action prevents concurrent modification
        let mut guard = chunk.write();

        // update the catalog to say we are done processing
        guard
            .set_moved(Arc::new(rb_chunk))
            .expect("failed to move chunk");

        debug!(chunk=%addr, "chunk marked MOVED. loading complete");

        Ok(DbChunk::snapshot(&guard))
    };

    Ok((tracker, fut.track(registration)))
}
