use crate::db::catalog::chunk::CatalogChunk;
pub(crate) use crate::db::chunk::DbChunk;
use ::lifecycle::LifecycleWriteGuard;
use data_types::job::Job;
use internal_types::{arrow::sort::sort_record_batch, selection::Selection};

use observability_deps::tracing::{debug, info};
use read_buffer::{ChunkMetrics as ReadBufferChunkMetrics, RBChunk};
use std::{future::Future, sync::Arc};
use tracker::{TaskTracker, TrackedFuture, TrackedFutureExt};

use super::{error::Result, LockableCatalogChunk};

/// The implementation for moving a chunk to the read buffer
///
/// Returns a future registered with the tracker registry, and the corresponding tracker
/// The caller can either spawn this future to tokio, or block directly on it
pub fn move_chunk_to_read_buffer(
    mut guard: LifecycleWriteGuard<'_, CatalogChunk, LockableCatalogChunk<'_>>,
) -> Result<(
    TaskTracker<Job>,
    TrackedFuture<impl Future<Output = Result<Arc<DbChunk>>> + Send>,
)> {
    let db = guard.data().db;
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
    let (mb_chunk, table_summary) = {
        let mb_chunk = guard.set_moving(&registration)?;
        (mb_chunk, guard.table_summary())
    };

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

    let fut = async move {
        info!(chunk=%addr, "chunk marked MOVING, loading tables into read buffer");

        // load table into the new chunk one by one.
        debug!(chunk=%addr, "loading table to read buffer");
        let batch = mb_chunk
            .read_filter(Selection::All)
            // It is probably reasonable to recover from this error
            // (reset the chunk state to Open) but until that is
            // implemented (and tested) just panic
            .expect("Loading chunk to mutable buffer");

        let sorted = sort_record_batch(batch).expect("failed to sort");
        rb_chunk.upsert_table(&table_summary.name, sorted);

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
