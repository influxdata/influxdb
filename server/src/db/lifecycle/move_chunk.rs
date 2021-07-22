use crate::db::catalog::chunk::CatalogChunk;
pub(crate) use crate::db::chunk::DbChunk;
use ::lifecycle::LifecycleWriteGuard;
use data_types::job::Job;

use observability_deps::tracing::{debug, info};
use query::{compute_sort_key, exec::ExecutorType, frontend::reorg::ReorgPlanner, QueryChunkMeta};
use std::{future::Future, sync::Arc};
use tracker::{TaskTracker, TrackedFuture, TrackedFutureExt};

use super::{error::Result, LockableCatalogChunk};
use crate::db::lifecycle::collect_rub;

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
    let (tracker, registration) = db.jobs.register(Job::CompactChunk {
        chunk: addr.clone(),
    });

    // update the catalog to say we are processing this chunk and
    // then drop the lock while we do the work
    guard.set_moving(&registration)?;
    let table_summary = guard.table_summary();

    // snapshot the data
    // Note: we can just use the chunk-specific schema here since there is only a single chunk and this is somewhat a
    // local operation that should only need to deal with the columns that are really present.
    let db_chunk = DbChunk::snapshot(&*guard);
    let schema = db_chunk.schema();
    let time_of_first_write = db_chunk.time_of_first_write();
    let time_of_last_write = db_chunk.time_of_last_write();
    let query_chunks = vec![db_chunk];

    // Drop locks
    let chunk = guard.into_data().chunk;

    let ctx = db.exec.new_context(ExecutorType::Reorg);

    let fut = async move {
        info!(chunk=%addr, "chunk marked MOVING, loading tables into read buffer");

        let key = compute_sort_key(query_chunks.iter().map(|x| x.summary()));

        // Cannot move query_chunks as the sort key borrows the column names
        let (schema, plan) =
            ReorgPlanner::new().compact_plan(schema, query_chunks.iter().map(Arc::clone), key)?;

        let physical_plan = ctx.prepare_plan(&plan)?;
        let stream = ctx.execute(physical_plan).await?;
        let rb_chunk = collect_rub(
            stream,
            db.as_ref(),
            &table_summary.name,
            time_of_first_write,
            time_of_last_write,
        )
        .await?;

        // Can drop and re-acquire as lifecycle action prevents concurrent modification
        let mut guard = chunk.write();

        let rb_chunk =
            rb_chunk.expect("Chunks moving to the read buffer should have at least one row");

        // update the catalog to say we are done processing
        guard
            .set_moved(Arc::new(rb_chunk), schema)
            .expect("failed to move chunk");

        debug!(chunk=%addr, "chunk marked MOVED. loading complete");

        Ok(DbChunk::snapshot(&guard))
    };

    Ok((tracker, fut.track(registration)))
}
