//! This module contains the code that splits and persist chunks

use std::future::Future;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use data_types::job::Job;
use lifecycle::{LifecycleWriteGuard, LockableChunk};
use observability_deps::tracing::info;
use query::exec::ExecutorType;
use query::frontend::reorg::ReorgPlanner;
use query::{QueryChunkMeta, compute_sort_key};
use tracker::{TaskTracker, TrackedFuture, TrackedFutureExt};

use crate::db::catalog::chunk::CatalogChunk;
use crate::db::catalog::partition::Partition;
use crate::db::lifecycle::{
    collect_rub, merge_schemas, new_rub_chunk, write_chunk_to_object_store,
};
use crate::db::DbChunk;

use super::{LockableCatalogChunk, LockableCatalogPartition, Result};
use persistence_windows::persistence_windows::FlushHandle;

/// Split and then persist the provided chunks
///
/// TODO: Replace low-level locks with transaction object
pub(super) fn persist_chunks(
    partition: LifecycleWriteGuard<'_, Partition, LockableCatalogPartition>,
    chunks: Vec<LifecycleWriteGuard<'_, CatalogChunk, LockableCatalogChunk>>,
    max_persistable_timestamp: DateTime<Utc>,
    flush_handle: FlushHandle,
) -> Result<(
    TaskTracker<Job>,
    TrackedFuture<impl Future<Output = Result<()>> + Send>,
)> {
    let now = std::time::Instant::now(); // time persist duration.
    let db = Arc::clone(&partition.data().db);
    let table_name = partition.table_name().to_string();
    let partition_key = partition.key().to_string();
    let chunk_ids: Vec<_> = chunks.iter().map(|x| x.id()).collect();

    info!(%table_name, %partition_key, ?chunk_ids, "splitting and persisting chunks");

    let flush_timestamp = max_persistable_timestamp.timestamp_nanos();

    let (tracker, registration) = db.jobs.register(Job::PersistChunks {
        db_name: partition.db_name().to_string(),
        partition_key: partition.key().to_string(),
        table_name: table_name.clone(),
        chunks: chunk_ids.clone(),
    });

    // Mark and snapshot chunks, then drop locks
    let mut input_rows = 0;
    let mut query_chunks = vec![];
    for mut chunk in chunks {
        // Sanity-check
        assert!(Arc::ptr_eq(&db, &chunk.data().db));
        assert_eq!(chunk.table_name().as_ref(), table_name.as_str());

        input_rows += chunk.table_summary().count();
        chunk.set_writing_to_object_store(&registration)?;
        query_chunks.push(DbChunk::snapshot(&*chunk));
    }

    // drop partition lock guard
    let partition = partition.into_data().partition;
    let mut to_persist = new_rub_chunk(db.as_ref(), &table_name);
    let mut remainder = new_rub_chunk(db.as_ref(), &table_name);

    let ctx = db.exec.new_context(ExecutorType::Reorg);

    let fut = async move {
        let key = compute_sort_key(query_chunks.iter().map(|x| x.summary()));
        let key_str = format!("\"{}\"", key); // for logging

        // build schema
        let schema = merge_schemas(&query_chunks);

        // Cannot move query_chunks as the sort key borrows the column names
        let (schema, plan) = ReorgPlanner::new().split_plan(
            schema,
            query_chunks.iter().map(Arc::clone),
            key,
            flush_timestamp,
        )?;

        let physical_plan = ctx.prepare_plan(&plan)?;
        assert_eq!(
            physical_plan.output_partitioning().partition_count(),
            2,
            "Expected split plan to produce exactly 2 partitions"
        );

        let to_persist_stream = ctx.execute_partition(Arc::clone(&physical_plan), 0).await?;
        let remainder_stream = ctx.execute_partition(physical_plan, 1).await?;

        futures::future::try_join(
            collect_rub(to_persist_stream, &mut to_persist),
            collect_rub(remainder_stream, &mut remainder),
        )
        .await?;

        let persisted_rows = to_persist.rows();
        let remainder_rows = remainder.rows();

        let persist_fut = {
            let mut partition = partition.write();
            for id in chunk_ids {
                partition.force_drop_chunk(id)
            }

            // Upsert remainder to catalog
            if remainder.rows() > 0 {
                partition.create_rub_chunk(remainder, Arc::clone(&schema));
            }

            assert!(to_persist.rows() > 0);

            let to_persist = LockableCatalogChunk {
                db,
                chunk: partition.create_rub_chunk(to_persist, schema),
            };
            let to_persist = to_persist.write();

            // Drop partition lock guard after locking chunk
            std::mem::drop(partition);

            write_chunk_to_object_store(to_persist)?.1
        };

        // Wait for write operation to complete
        persist_fut.await??;

        {
            // Flush persisted data from persistence windows
            let mut partition = partition.write();
            partition
                .persistence_windows_mut()
                .expect("persistence windows removed")
                .flush(flush_handle);
        }

        let elapsed = now.elapsed();
        // input rows per second
        let throughput = (input_rows as u128 * 1_000_000_000) / elapsed.as_nanos();

        info!(input_chunks=query_chunks.len(),
              input_rows, persisted_rows, remainder_rows,
              sort_key=%key_str, compaction_took = ?elapsed,
              ?max_persistable_timestamp,
              rows_per_sec=?throughput,  "chunk(s) persisted");

        Ok(())
    };

    Ok((tracker, fut.track(registration)))
}
