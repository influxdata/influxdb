//! This module contains the code that splits and persist chunks

use super::{LockableCatalogChunk, LockableCatalogPartition, Result};
use crate::db::{
    catalog::{chunk::CatalogChunk, partition::Partition},
    lifecycle::{collect_rub, merge_schemas, write::write_chunk_to_object_store},
    DbChunk,
};
use chrono::{DateTime, Utc};
use data_types::{chunk_metadata::ChunkOrder, job::Job};
use lifecycle::{LifecycleWriteGuard, LockableChunk, LockablePartition};
use observability_deps::tracing::info;
use persistence_windows::persistence_windows::FlushHandle;
use predicate::delete_predicate::DeletePredicate;
use query::{compute_sort_key, exec::ExecutorType, frontend::reorg::ReorgPlanner, QueryChunkMeta};
use std::{future::Future, sync::Arc};
use tracker::{TaskTracker, TrackedFuture, TrackedFutureExt};

/// Split and then persist the provided chunks
///
/// `flush_handle` describes both what to persist and also acts as a transaction
/// on the persistence windows
///
/// TODO: Replace low-level locks with transaction object
pub fn persist_chunks(
    partition: LifecycleWriteGuard<'_, Partition, LockableCatalogPartition>,
    chunks: Vec<LifecycleWriteGuard<'_, CatalogChunk, LockableCatalogChunk>>,
    flush_handle: FlushHandle,
) -> Result<(
    TaskTracker<Job>,
    TrackedFuture<impl Future<Output = Result<Option<Arc<DbChunk>>>> + Send>,
)> {
    let now = std::time::Instant::now(); // time persist duration.
    let db = Arc::clone(&partition.data().db);
    let addr = partition.addr().clone();
    let chunk_ids: Vec<_> = chunks.iter().map(|x| x.id()).collect();

    info!(%addr, ?chunk_ids, "splitting and persisting chunks");

    let max_persistable_timestamp = flush_handle.timestamp();
    let flush_timestamp = max_persistable_timestamp.timestamp_nanos();

    let (tracker, registration) = db.jobs.register(Job::PersistChunks {
        partition: partition.addr().clone(),
        chunks: chunk_ids.clone(),
    });

    // Mark and snapshot chunks, then drop locks
    let mut input_rows = 0;
    let mut time_of_first_write: Option<DateTime<Utc>> = None;
    let mut time_of_last_write: Option<DateTime<Utc>> = None;
    let mut query_chunks = vec![];
    let mut delete_predicates: Vec<Arc<DeletePredicate>> = vec![];
    let mut min_order = ChunkOrder::MAX;
    for mut chunk in chunks {
        // Sanity-check
        assert!(Arc::ptr_eq(&db, &chunk.data().db));
        assert_eq!(chunk.table_name().as_ref(), addr.table_name.as_ref());
        assert_eq!(chunk.key(), addr.partition_key.as_ref());

        input_rows += chunk.table_summary().total_count();

        let candidate_first = chunk.time_of_first_write();
        time_of_first_write = time_of_first_write
            .map(|prev_first| prev_first.min(candidate_first))
            .or(Some(candidate_first));

        let candidate_last = chunk.time_of_last_write();
        time_of_last_write = time_of_last_write
            .map(|prev_last| prev_last.max(candidate_last))
            .or(Some(candidate_last));

        delete_predicates.extend(chunk.delete_predicates().iter().cloned());

        min_order = min_order.min(chunk.order());

        chunk.set_writing_to_object_store(&registration)?;
        query_chunks.push(DbChunk::snapshot(&*chunk));
    }

    // drop partition lock guard
    let partition = partition.into_data().partition;

    let metric_registry = Arc::clone(&db.metric_registry);
    let ctx = db.exec.new_context(ExecutorType::Reorg);

    let fut = async move {
        if query_chunks.is_empty() {
            partition
                .write()
                .persistence_windows_mut()
                .unwrap()
                .flush(flush_handle);

            return Ok(None);
        }

        let time_of_first_write =
            time_of_first_write.expect("Should have had a first write somewhere");

        let time_of_last_write =
            time_of_last_write.expect("Should have had a last write somewhere");

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

        let physical_plan = ctx.prepare_plan(&plan).await?;
        assert_eq!(
            physical_plan.output_partitioning().partition_count(),
            2,
            "Expected split plan to produce exactly 2 partitions"
        );

        let to_persist_stream = ctx
            .execute_stream_partitioned(Arc::clone(&physical_plan), 0)
            .await?;
        let remainder_stream = ctx.execute_stream_partitioned(physical_plan, 1).await?;

        let (to_persist, remainder) = futures::future::try_join(
            collect_rub(to_persist_stream, &addr, metric_registry.as_ref()),
            collect_rub(remainder_stream, &addr, metric_registry.as_ref()),
        )
        .await?;

        let persisted_rows = to_persist.as_ref().map(|p| p.rows()).unwrap_or(0);
        let remainder_rows = remainder.as_ref().map(|r| r.rows()).unwrap_or(0);

        let persist_fut = {
            let partition = LockableCatalogPartition::new(Arc::clone(&db), partition);
            let mut partition_write = partition.write();
            for id in &chunk_ids {
                // TODO: Reacquire chunk lock and check for "new" delete predicates (#2666)
                partition_write.force_drop_chunk(*id).expect(
                    "There was a lifecycle action attached to this chunk, who deleted it?!",
                );
            }

            // Upsert remainder to catalog if any
            if let Some(remainder) = remainder {
                partition_write.create_rub_chunk(
                    remainder,
                    time_of_first_write,
                    time_of_last_write,
                    Arc::clone(&schema),
                    // TODO: Filter out predicates applied by the query (#2666)
                    delete_predicates.clone(),
                    min_order,
                    None,
                );
            }

            let to_persist = match to_persist {
                Some(to_persist) => to_persist,
                None => {
                    info!(%addr, ?chunk_ids, "no rows to persist, no chunk created");
                    partition_write
                        .persistence_windows_mut()
                        .unwrap()
                        .flush(flush_handle);
                    return Ok(None);
                }
            };

            let (new_chunk_id, new_chunk) = partition_write.create_rub_chunk(
                to_persist,
                time_of_first_write,
                time_of_last_write,
                schema,
                delete_predicates,
                min_order,
                db.persisted_chunk_id_override.lock().as_ref().cloned(),
            );
            let to_persist = LockableCatalogChunk {
                db,
                chunk: Arc::clone(new_chunk),
                id: new_chunk_id,
                order: min_order,
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

        Ok(Some(persisted_chunk))
    };

    Ok((tracker, fut.track(registration)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{db::test_helpers::write_lp, utils::TestDb, Db};
    use chrono::{TimeZone, Utc};
    use data_types::database_rules::LifecycleRules;
    use data_types::{chunk_metadata::ChunkStorage, timestamp::TimestampRange};
    use lifecycle::{LockableChunk, LockablePartition};
    use query::QueryDatabase;
    use std::{
        num::{NonZeroU32, NonZeroU64},
        time::Duration,
    };

    async fn test_db() -> Arc<Db> {
        let test_db = TestDb::builder()
            .lifecycle_rules(LifecycleRules {
                late_arrive_window_seconds: NonZeroU32::new(1).unwrap(),
                // Disable lifecycle manager - TODO: Better way to do this, as this will still run the loop once
                worker_backoff_millis: NonZeroU64::new(u64::MAX).unwrap(),
                ..Default::default()
            })
            .build()
            .await;

        test_db.db
    }

    #[tokio::test]
    async fn test_flush_overlapping() {
        let db = test_db().await;
        write_lp(db.as_ref(), "cpu,tag1=cupcakes bar=1 10").await;

        let partition_keys = db.partition_keys().unwrap();
        assert_eq!(partition_keys.len(), 1);
        let db_partition = db.partition("cpu", &partition_keys[0]).unwrap();

        // Wait for the persistence window to be closed
        tokio::time::sleep(Duration::from_secs(2)).await;

        write_lp(db.as_ref(), "cpu,tag1=lagged bar=1 10").await;

        let partition = LockableCatalogPartition::new(Arc::clone(&db), Arc::clone(&db_partition));
        let partition = partition.read();

        let chunks = LockablePartition::chunks(&partition);
        let chunks = chunks.iter().map(|x| x.read());

        let mut partition = partition.upgrade();

        let handle = LockablePartition::prepare_persist(&mut partition, Utc::now())
            .unwrap()
            .0;

        assert_eq!(handle.timestamp(), Utc.timestamp_nanos(10));
        let chunks: Vec<_> = chunks.map(|x| x.upgrade()).collect();

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

    #[tokio::test]
    async fn test_persist_delete() {
        let db = test_db().await;

        let late_arrival = chrono::Duration::seconds(1);

        let t0 = Utc::now();
        let t1 = t0 + late_arrival * 10;
        let t2 = t1 + late_arrival * 10;

        *db.now_override.lock() = Some(t0);
        write_lp(db.as_ref(), "cpu,tag1=cupcakes bar=1 10").await;

        *db.now_override.lock() = Some(t1);
        write_lp(db.as_ref(), "cpu,tag1=cupcakes bar=3 23").await;

        let partition_keys = db.partition_keys().unwrap();
        assert_eq!(partition_keys.len(), 1);
        let partition_key = partition_keys.into_iter().next().unwrap();
        let partition = db.partition("cpu", partition_key.as_str()).unwrap();

        // Delete first row
        let predicate = Arc::new(DeletePredicate {
            range: TimestampRange { start: 0, end: 20 },
            exprs: vec![],
        });

        db.delete("cpu", predicate).await.unwrap();

        // Try to persist first write but it has been deleted
        *db.now_override.lock() = Some(t0 + late_arrival);
        let maybe_chunk = db
            .persist_partition("cpu", partition_key.as_str(), false)
            .await
            .unwrap();

        assert!(maybe_chunk.is_none());

        let chunks: Vec<_> = partition.read().chunk_summaries().collect();
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].storage, ChunkStorage::ReadBuffer);
        assert_eq!(chunks[0].row_count, 1);

        assert_eq!(
            partition
                .read()
                .persistence_windows()
                .unwrap()
                .minimum_unpersisted_timestamp()
                .unwrap(),
            Utc.timestamp_nanos(23)
        );

        // Add a second set of writes one of which overlaps the above chunk
        *db.now_override.lock() = Some(t2);
        write_lp(db.as_ref(), "cpu,tag1=foo bar=2 23").await;
        write_lp(db.as_ref(), "cpu,tag1=cupcakes bar=2 26").await;

        // Persist second write but not third
        let maybe_chunk = db
            .persist_partition("cpu", partition_key.as_str(), false)
            .await
            .unwrap();
        assert!(maybe_chunk.is_some());

        assert_eq!(
            partition
                .read()
                .persistence_windows()
                .unwrap()
                .minimum_unpersisted_timestamp()
                .unwrap(),
            // The persistence windows only know that all rows <= 23 have been persisted
            // They do not know that the remaining row has timestamp 26, only that
            // it is in the range 24..=26
            Utc.timestamp_nanos(24)
        );

        let mut chunks: Vec<_> = partition.read().chunk_summaries().collect();
        chunks.sort_by_key(|c| c.storage);
        assert_eq!(chunks.len(), 2);
        assert_eq!(chunks[0].storage, ChunkStorage::ReadBuffer);
        assert_eq!(chunks[0].row_count, 1);
        assert_eq!(chunks[1].storage, ChunkStorage::ReadBufferAndObjectStore);
        assert_eq!(chunks[1].row_count, 2);

        // Delete everything
        let predicate = Arc::new(DeletePredicate {
            range: TimestampRange {
                start: 0,
                end: 1_000,
            },
            exprs: vec![],
        });

        db.delete("cpu", predicate).await.unwrap();

        // Try to persist third set of writes
        *db.now_override.lock() = Some(t2 + late_arrival);
        let maybe_chunk = db
            .persist_partition("cpu", partition_key.as_str(), false)
            .await
            .unwrap();

        assert!(maybe_chunk.is_none());

        // The already persisted chunk should remain
        let chunks: Vec<_> = partition.read().chunk_summaries().collect();
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].storage, ChunkStorage::ReadBufferAndObjectStore);
        assert_eq!(chunks[0].row_count, 2);

        assert!(partition.read().persistence_windows().unwrap().is_empty());
    }

    #[tokio::test]
    async fn persist_compacted_deletes() {
        let db = test_db().await;

        let late_arrival = chrono::Duration::seconds(1);
        let t0 = Utc::now();

        *db.now_override.lock() = Some(t0);
        write_lp(db.as_ref(), "cpu,tag1=cupcakes bar=1 10").await;

        let partition_keys = db.partition_keys().unwrap();
        assert_eq!(partition_keys.len(), 1);
        let partition_key = partition_keys.into_iter().next().unwrap();

        let partition = db.partition("cpu", partition_key.as_str()).unwrap();

        // Cannot simply use empty predicate (#2687)
        let predicate = Arc::new(DeletePredicate {
            range: TimestampRange {
                start: 0,
                end: 1_000,
            },
            exprs: vec![],
        });

        // Delete everything
        db.delete("cpu", predicate).await.unwrap();

        // Compact deletes away
        let chunk = db
            .compact_partition("cpu", partition_key.as_str())
            .await
            .unwrap();

        assert!(chunk.is_none());

        // Persistence windows unaware rows have been deleted
        assert!(!partition.read().persistence_windows().unwrap().is_empty());

        *db.now_override.lock() = Some(t0 + late_arrival);
        let maybe_chunk = db
            .persist_partition("cpu", partition_key.as_str(), false)
            .await
            .unwrap();

        assert!(maybe_chunk.is_none());
        assert!(partition.read().persistence_windows().unwrap().is_empty());
    }
}
