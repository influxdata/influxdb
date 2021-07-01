//! This module contains the code to compact chunks together

use std::future::Future;
use std::sync::Arc;

use futures::StreamExt;

use data_types::job::Job;
use lifecycle::LifecycleWriteGuard;
use query::exec::ExecutorType;
use query::frontend::reorg::ReorgPlanner;
use query::QueryChunkMeta;
use read_buffer::{ChunkMetrics, RBChunk};
use tracker::{TaskTracker, TrackedFuture, TrackedFutureExt};

use crate::db::catalog::chunk::CatalogChunk;
use crate::db::catalog::partition::Partition;
use crate::db::DbChunk;

use super::compute_sort_key;
use super::{error::Result, LockableCatalogChunk, LockableCatalogPartition};

/// Compact the provided chunks into a single chunk,
/// returning the newly created chunk
///
/// TODO: Replace low-level locks with transaction object
pub(crate) fn compact_chunks(
    partition: LifecycleWriteGuard<'_, Partition, LockableCatalogPartition>,
    chunks: Vec<LifecycleWriteGuard<'_, CatalogChunk, LockableCatalogChunk>>,
) -> Result<(
    TaskTracker<Job>,
    TrackedFuture<impl Future<Output = Result<Arc<DbChunk>>> + Send>,
)> {
    let db = Arc::clone(&partition.data().db);
    let table_name = partition.table_name().to_string();
    let chunk_ids: Vec<_> = chunks.iter().map(|x| x.id()).collect();

    let (tracker, registration) = db.jobs.register(Job::CompactChunks {
        db_name: partition.db_name().to_string(),
        partition_key: partition.key().to_string(),
        table_name: table_name.clone(),
        chunks: chunk_ids.clone(),
    });

    // Mark and snapshot chunks, then drop locks
    let query_chunks = chunks
        .into_iter()
        .map(|mut chunk| {
            // Sanity-check
            assert!(Arc::ptr_eq(&db, &chunk.data().db));
            assert_eq!(chunk.table_name().as_ref(), table_name.as_str());

            chunk.set_compacting(&registration)?;
            Ok(DbChunk::snapshot(&*chunk))
        })
        .collect::<Result<Vec<_>>>()?;

    // drop partition lock
    let partition = partition.unwrap().partition;

    // create a new read buffer chunk with memory tracking
    let metrics = db
        .metrics_registry
        .register_domain_with_labels("read_buffer", db.metric_labels.clone());

    let mut rb_chunk = RBChunk::new(
        &table_name,
        ChunkMetrics::new(&metrics, db.catalog.metrics().memory().read_buffer()),
    );

    let ctx = db.exec.new_context(ExecutorType::Reorg);

    let fut = async move {
        let key = compute_sort_key(query_chunks.iter().map(|x| x.summary()));

        // Cannot move query_chunks as the sort key borrows the column names
        let (schema, plan) =
            ReorgPlanner::new().compact_plan(query_chunks.iter().map(Arc::clone), key)?;

        let physical_plan = ctx.prepare_plan(&plan)?;
        let mut stream = ctx.execute(physical_plan).await?;

        // Collect results into RUB chunk
        while let Some(batch) = stream.next().await {
            rb_chunk.upsert_table(&table_name, batch?)
        }

        let new_chunk = {
            let mut partition = partition.write();
            for id in chunk_ids {
                partition.force_drop_chunk(id)
            }
            partition.create_rub_chunk(rb_chunk, schema)
        };

        let guard = new_chunk.read();
        Ok(DbChunk::snapshot(&guard))
    };

    Ok((tracker, fut.track(registration)))
}
