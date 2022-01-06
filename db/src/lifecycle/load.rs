//! This module contains the code to unload chunks from the read buffer

use super::{
    error::{Error, Result},
    LockableCatalogChunk,
};
use crate::{catalog::chunk::CatalogChunk, lifecycle::collect_rub, DbChunk};
use data_types::job::Job;
use lifecycle::LifecycleWriteGuard;
use observability_deps::tracing::info;
use query::{exec::ExecutorType, frontend::reorg::ReorgPlanner, QueryChunkMeta};
use std::{future::Future, sync::Arc};
use tracker::{TaskTracker, TrackedFuture, TrackedFutureExt};

/// Loads a chunk in object storage back into the read buffer
pub fn load_chunk(
    mut chunk: LifecycleWriteGuard<'_, CatalogChunk, LockableCatalogChunk>,
) -> Result<(
    TaskTracker<Job>,
    TrackedFuture<impl Future<Output = Result<()>> + Send>,
)> {
    let db = Arc::clone(&chunk.data().db);
    let addr = chunk.addr().clone();

    info!(%addr, "loading chunk to read buffer");

    let (tracker, registration) = db.jobs.register(Job::LoadReadBufferChunk {
        chunk: addr.clone(),
    });
    chunk.set_loading_to_read_buffer(&registration)?;

    // Get queryable chunk
    let db_chunk = DbChunk::snapshot(&*chunk);

    // Drop locks
    let chunk = chunk.into_data().chunk;

    let ctx = db.exec.new_context(ExecutorType::Reorg);

    let fut = async move {
        let plan =
            ReorgPlanner::new().scan_single_chunk_plan(db_chunk.schema(), Arc::clone(&db_chunk))?;

        let physical_plan = ctx.prepare_plan(&plan).await?;
        let stream = ctx.execute_stream(physical_plan).await?;
        let maybe_rb_chunk = collect_rub(
            stream,
            &addr.clone().into_partition(),
            db.metric_registry.as_ref(),
        )
        .await?;

        // TODO(raphael): mechanism to indicate this chunk should be dropped
        let rb_chunk = maybe_rb_chunk.ok_or(Error::CannotLoadEmptyChunk { addr })?;

        chunk
            .write()
            .set_loaded_to_read_buffer(Arc::new(rb_chunk))?;

        Ok(())
    };

    Ok((tracker, fut.track(registration)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        test_helpers::{run_query, write_lp},
        utils::make_db,
    };
    use arrow_util::assert_batches_eq;
    use data_types::chunk_metadata::ChunkStorage;
    use lifecycle::LockableChunk;
    use query::{QueryChunk, QueryDatabase};

    #[tokio::test]
    async fn test_load() {
        let db = make_db().await.db;

        write_lp(&db, "cpu bar=3 10");

        let partition_key = "1970-01-01T00";
        let chunk = db
            .persist_partition("cpu", partition_key, true)
            .await
            .unwrap()
            .unwrap();
        let chunk_id = chunk.id();

        let chunks = db.chunk_summaries();
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].storage, ChunkStorage::ReadBufferAndObjectStore);

        db.unload_read_buffer("cpu", partition_key, chunk_id)
            .unwrap();

        let chunks = db.chunk_summaries();
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].storage, ChunkStorage::ObjectStoreOnly);

        let chunk = db.lockable_chunk("cpu", partition_key, chunk_id).unwrap();
        load_chunk(chunk.write()).unwrap().1.await.unwrap().unwrap();

        let chunks = db.chunk_summaries();
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].storage, ChunkStorage::ReadBufferAndObjectStore);

        let batches = run_query(db, "select * from cpu").await;
        assert_batches_eq!(
            &[
                "+-----+--------------------------------+",
                "| bar | time                           |",
                "+-----+--------------------------------+",
                "| 3   | 1970-01-01T00:00:00.000000010Z |",
                "+-----+--------------------------------+",
            ],
            &batches
        );
    }
}
