//! This module contains the code that splits and persist chunks

use super::{LockableCatalogChunk, LockableCatalogPartition, Result};
use crate::db::{
    catalog::{chunk::CatalogChunk, partition::Partition},
    lifecycle::{collect_rub, merge_schemas, new_rub_chunk, write::write_chunk_to_object_store},
    DbChunk,
};
use data_types::job::Job;
use lifecycle::{LifecycleWriteGuard, LockableChunk, LockablePartition};
use observability_deps::tracing::info;
use persistence_windows::persistence_windows::FlushHandle;
use query::{compute_sort_key, exec::ExecutorType, frontend::reorg::ReorgPlanner, QueryChunkMeta};
use std::{future::Future, sync::Arc};
use tracker::{TaskTracker, TrackedFuture, TrackedFutureExt};

/// Split and then persist the provided chunks
///
/// TODO: Replace low-level locks with transaction object
pub fn persist_chunks(
    partition: LifecycleWriteGuard<'_, Partition, LockableCatalogPartition>,
    chunks: Vec<LifecycleWriteGuard<'_, CatalogChunk, LockableCatalogChunk>>,
    flush_handle: FlushHandle,
) -> Result<(
    TaskTracker<Job>,
    TrackedFuture<impl Future<Output = Result<Arc<DbChunk>>> + Send>,
)> {
    let now = std::time::Instant::now(); // time persist duration.
    let db = Arc::clone(&partition.data().db);
    let table_name = partition.table_name().to_string();
    let partition_key = partition.key().to_string();
    let chunk_ids: Vec<_> = chunks.iter().map(|x| x.id()).collect();

    info!(%table_name, %partition_key, ?chunk_ids, "splitting and persisting chunks");

    let max_persistable_timestamp = flush_handle.timestamp();
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
            let partition = LockableCatalogPartition::new(Arc::clone(&db), partition);
            let mut partition_write = partition.write();
            for id in chunk_ids {
                partition_write.force_drop_chunk(id)
            }

            // Upsert remainder to catalog
            if remainder.rows() > 0 {
                partition_write.create_rub_chunk(remainder, Arc::clone(&schema));
            }

            assert!(to_persist.rows() > 0);

            let to_persist = LockableCatalogChunk {
                db,
                chunk: partition_write.create_rub_chunk(to_persist, schema),
            };
            let to_persist = to_persist.write();

            write_chunk_to_object_store(partition_write, to_persist, flush_handle)?.1
        };

        // Wait for write operation to complete
        let persisted_chunk = persist_fut.await??;

        let elapsed = now.elapsed();
        // input rows per second
        let throughput = (input_rows as u128 * 1_000_000_000) / elapsed.as_nanos();

        info!(input_chunks=query_chunks.len(),
              input_rows, persisted_rows, remainder_rows,
              sort_key=%key_str, compaction_took = ?elapsed,
              ?max_persistable_timestamp,
              rows_per_sec=?throughput,  "chunk(s) persisted");

        Ok(persisted_chunk)
    };

    Ok((tracker, fut.track(registration)))
}

#[cfg(test)]
mod tests {
    use std::num::{NonZeroU32, NonZeroU64};
    use std::time::Instant;

    use chrono::{TimeZone, Utc};

    use data_types::database_rules::LifecycleRules;
    use lifecycle::{LockableChunk, LockablePartition};
    use query::QueryDatabase;

    use crate::db::test_helpers::write_lp;
    use crate::utils::TestDb;

    use super::*;

    #[tokio::test]
    async fn test_flush_overlapping() {
        let test_db = TestDb::builder()
            .lifecycle_rules(LifecycleRules {
                late_arrive_window_seconds: NonZeroU32::new(1).unwrap(),
                // Disable lifecycle manager - TODO: Better way to do this, as this will still run the loop once
                worker_backoff_millis: NonZeroU64::new(u64::MAX).unwrap(),
                ..Default::default()
            })
            .build()
            .await;

        let db = test_db.db;

        write_lp(db.as_ref(), "cpu,tag1=cupcakes bar=1 10").await;

        let partition_keys = db.partition_keys().unwrap();
        assert_eq!(partition_keys.len(), 1);
        let db_partition = db.partition("cpu", &partition_keys[0]).unwrap();

        // Wait for the persistence window to be closed
        tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

        write_lp(db.as_ref(), "cpu,tag1=lagged bar=1 10").await;

        let partition = LockableCatalogPartition::new(Arc::clone(&db), Arc::clone(&db_partition));
        let partition = partition.read();

        let chunks = LockablePartition::chunks(&partition);
        let chunks: Vec<_> = chunks.iter().map(|x| x.1.read()).collect();

        let mut partition = partition.upgrade();

        let handle = LockablePartition::prepare_persist(&mut partition, Instant::now())
            .unwrap()
            .0;

        assert_eq!(handle.timestamp(), Utc.timestamp_nanos(10));
        let chunks: Vec<_> = chunks.into_iter().map(|x| x.upgrade()).collect();

        persist_chunks(partition, chunks, handle)
            .unwrap()
            .1
            .await
            .unwrap()
            .unwrap();

        assert!(db_partition
            .read()
            .persistence_windows()
            .unwrap()
            .minimum_unpersisted_age()
            .is_none());
    }
}
