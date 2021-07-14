//! This module contains the code to compact chunks together

use std::future::Future;
use std::sync::Arc;

use data_types::job::Job;
use lifecycle::LifecycleWriteGuard;
use observability_deps::tracing::info;
use query::exec::ExecutorType;
use query::frontend::reorg::ReorgPlanner;
use query::{compute_sort_key, QueryChunkMeta};
use read_buffer::{ChunkMetrics, RBChunk};
use tracker::{TaskTracker, TrackedFuture, TrackedFutureExt};

use crate::db::catalog::chunk::CatalogChunk;
use crate::db::catalog::partition::Partition;
use crate::db::DbChunk;

use super::merge_schemas;
use super::{error::Result, LockableCatalogChunk, LockableCatalogPartition};
use crate::db::lifecycle::collect_rub;

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
    let now = std::time::Instant::now(); // time compaction duration.
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
    let mut input_rows = 0;
    let query_chunks = chunks
        .into_iter()
        .map(|mut chunk| {
            // Sanity-check
            assert!(Arc::ptr_eq(&db, &chunk.data().db));
            assert_eq!(chunk.table_name().as_ref(), table_name.as_str());

            input_rows += chunk.table_summary().count();
            chunk.set_compacting(&registration)?;
            Ok(DbChunk::snapshot(&*chunk))
        })
        .collect::<Result<Vec<_>>>()?;

    // drop partition lock
    let partition = partition.into_data().partition;

    // create a new read buffer chunk with memory tracking
    let metrics = db
        .metrics_registry
        .register_domain_with_labels("read_buffer", db.metric_labels.clone());

    let mut rb_chunk = RBChunk::new(&table_name, ChunkMetrics::new(&metrics));

    let ctx = db.exec.new_context(ExecutorType::Reorg);

    let fut = async move {
        let key = compute_sort_key(query_chunks.iter().map(|x| x.summary()));
        let key_str = format!("\"{}\"", key); // for logging

        // build schema
        //
        // Note: we only use the merged schema from the to-be-compacted
        // chunks - not the table-wide schema, since we don't need to
        // bother with other columns (e.g. ones that only exist in other
        // partitions).
        let schema = merge_schemas(&query_chunks);

        // Cannot move query_chunks as the sort key borrows the column names
        let (schema, plan) =
            ReorgPlanner::new().compact_plan(schema, query_chunks.iter().map(Arc::clone), key)?;

        let physical_plan = ctx.prepare_plan(&plan)?;
        let stream = ctx.execute(physical_plan).await?;
        collect_rub(stream, &mut rb_chunk).await?;
        let rb_row_groups = rb_chunk.row_groups();

        let new_chunk = {
            let mut partition = partition.write();
            for id in chunk_ids {
                partition.force_drop_chunk(id)
            }
            partition.create_rub_chunk(rb_chunk, schema)
        };

        let guard = new_chunk.read();
        let elapsed = now.elapsed();

        assert!(guard.table_summary().count() > 0, "chunk has zero rows");
        // input rows per second
        let throughput = (input_rows as u128 * 1_000_000_000) / elapsed.as_nanos();

        info!(input_chunks=query_chunks.len(), rub_row_groups=rb_row_groups,
                input_rows=input_rows, output_rows=guard.table_summary().count(),
                sort_key=%key_str, compaction_took = ?elapsed, rows_per_sec=?throughput,  "chunk(s) compacted");

        Ok(DbChunk::snapshot(&guard))
    };

    Ok((tracker, fut.track(registration)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::test_helpers::write_lp;
    use crate::utils::make_db;
    use data_types::chunk_metadata::ChunkStorage;
    use lifecycle::{LockableChunk, LockablePartition};
    use query::QueryDatabase;

    #[tokio::test]
    async fn test_compact_freeze() {
        let test_db = make_db().await;
        let db = test_db.db;

        write_lp(db.as_ref(), "cpu,tag1=cupcakes bar=1 10").await;
        write_lp(db.as_ref(), "cpu,tag1=asfd,tag2=foo bar=2 20").await;
        write_lp(db.as_ref(), "cpu,tag1=bingo,tag2=foo bar=2 10").await;
        write_lp(db.as_ref(), "cpu,tag1=bongo,tag2=a bar=2 20").await;
        write_lp(db.as_ref(), "cpu,tag1=bongo,tag2=a bar=2 10").await;

        let partition_keys = db.partition_keys().unwrap();
        assert_eq!(partition_keys.len(), 1);

        let db_partition = db.partition("cpu", &partition_keys[0]).unwrap();

        let partition = LockableCatalogPartition::new(Arc::clone(&db), Arc::clone(&db_partition));
        let partition = partition.read();

        let chunks = LockablePartition::chunks(&partition);
        assert_eq!(chunks.len(), 1);
        let chunk = chunks[0].1.read();

        let (_, fut) = compact_chunks(partition.upgrade(), vec![chunk.upgrade()]).unwrap();
        // NB: perform the write before spawning the background task that performs the compaction
        write_lp(db.as_ref(), "cpu,tag1=bongo,tag2=a bar=2 40").await;
        tokio::spawn(fut).await.unwrap().unwrap().unwrap();

        let summaries: Vec<_> = db_partition
            .read()
            .chunk_summaries()
            .map(|summary| (summary.storage, summary.row_count))
            .collect();

        assert_eq!(
            summaries,
            vec![
                (ChunkStorage::OpenMutableBuffer, 1),
                (ChunkStorage::ReadBuffer, 5)
            ]
        )
    }
}
