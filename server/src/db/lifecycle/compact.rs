//! This module contains the code to compact chunks together

use super::{error::Result, merge_schemas, LockableCatalogChunk, LockableCatalogPartition};
use crate::db::{
    catalog::{chunk::CatalogChunk, partition::Partition},
    lifecycle::collect_rub,
    DbChunk,
};
use chrono::{DateTime, Utc};
use data_types::{chunk_metadata::ChunkOrder, job::Job};
use lifecycle::LifecycleWriteGuard;
use observability_deps::tracing::info;
use predicate::predicate::Predicate;
use query::{compute_sort_key, exec::ExecutorType, frontend::reorg::ReorgPlanner, QueryChunkMeta};
use std::{future::Future, sync::Arc};
use tracker::{TaskTracker, TrackedFuture, TrackedFutureExt};

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
    assert!(
        !chunks.is_empty(),
        "must provide at least 1 chunk for compaction"
    );

    let now = std::time::Instant::now(); // time compaction duration.
    let db = Arc::clone(&partition.data().db);
    let addr = partition.addr().clone();
    let chunk_ids: Vec<_> = chunks.iter().map(|x| x.id()).collect();

    let (tracker, registration) = db.jobs.register(Job::CompactChunks {
        partition: partition.addr().clone(),
        chunks: chunk_ids.clone(),
    });

    // Mark and snapshot chunks, then drop locks
    let mut input_rows = 0;
    let mut time_of_first_write: Option<DateTime<Utc>> = None;
    let mut time_of_last_write: Option<DateTime<Utc>> = None;
    let mut delete_predicates: Vec<Predicate> = vec![];
    let mut min_order = ChunkOrder::MAX;
    let query_chunks = chunks
        .into_iter()
        .map(|mut chunk| {
            // Sanity-check
            assert!(Arc::ptr_eq(&db, &chunk.data().db));
            assert_eq!(chunk.table_name().as_ref(), addr.table_name.as_ref());

            input_rows += chunk.table_summary().total_count();

            let candidate_first = chunk.time_of_first_write();
            time_of_first_write = time_of_first_write
                .map(|prev_first| prev_first.min(candidate_first))
                .or(Some(candidate_first));

            let candidate_last = chunk.time_of_last_write();
            time_of_last_write = time_of_last_write
                .map(|prev_last| prev_last.max(candidate_last))
                .or(Some(candidate_last));

            let mut preds = (*chunk.delete_predicates()).clone();
            delete_predicates.append(&mut preds);

            min_order = min_order.min(chunk.order());

            chunk.set_compacting(&registration)?;
            Ok(DbChunk::snapshot(&*chunk))
        })
        .collect::<Result<Vec<_>>>()?;

    // drop partition lock
    let partition = partition.into_data().partition;

    let time_of_first_write = time_of_first_write.expect("Should have had a first write somewhere");
    let time_of_last_write = time_of_last_write.expect("Should have had a last write somewhere");

    let metric_registry = Arc::clone(&db.metric_registry);
    let ctx = db.exec.new_context(ExecutorType::Reorg);

    let fut = async move {
        let fut_now = std::time::Instant::now();
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
        let stream = ctx.execute_stream(physical_plan).await?;
        let rb_chunk = collect_rub(stream, &addr, metric_registry.as_ref())
            .await?
            .expect("chunk has zero rows");
        let rb_row_groups = rb_chunk.row_groups();

        let (_id, new_chunk) = {
            let mut partition = partition.write();
            for id in chunk_ids {
                partition.force_drop_chunk(id)
            }
            partition.create_rub_chunk(
                rb_chunk,
                time_of_first_write,
                time_of_last_write,
                schema,
                Arc::new(delete_predicates),
                min_order,
            )
        };

        let guard = new_chunk.read();
        let elapsed = now.elapsed();

        assert!(
            guard.table_summary().total_count() > 0,
            "chunk has zero rows"
        );
        // input rows per second
        let throughput = (input_rows as u128 * 1_000_000_000) / elapsed.as_nanos();

        info!(input_chunks=query_chunks.len(), rub_row_groups=rb_row_groups,
                input_rows=input_rows, output_rows=guard.table_summary().total_count(),
                sort_key=%key_str, compaction_took = ?elapsed, fut_execution_duration= ?fut_now.elapsed(),
                rows_per_sec=?throughput,  "chunk(s) compacted");

        Ok(DbChunk::snapshot(&guard))
    };

    Ok((tracker, fut.track(registration)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{db::test_helpers::write_lp_with_time, utils::make_db};
    use data_types::chunk_metadata::ChunkStorage;
    use lifecycle::{LockableChunk, LockablePartition};
    use query::QueryDatabase;

    #[tokio::test]
    async fn test_compact_freeze() {
        let test_db = make_db().await;
        let db = test_db.db;

        let t_first_write = Utc::now();
        write_lp_with_time(db.as_ref(), "cpu,tag1=cupcakes bar=1 10", t_first_write).await;
        write_lp_with_time(db.as_ref(), "cpu,tag1=asfd,tag2=foo bar=2 20", Utc::now()).await;
        write_lp_with_time(db.as_ref(), "cpu,tag1=bingo,tag2=foo bar=2 10", Utc::now()).await;
        write_lp_with_time(db.as_ref(), "cpu,tag1=bongo,tag2=a bar=2 20", Utc::now()).await;
        let t_last_write = Utc::now();
        write_lp_with_time(db.as_ref(), "cpu,tag1=bongo,tag2=a bar=2 10", t_last_write).await;

        let partition_keys = db.partition_keys().unwrap();
        assert_eq!(partition_keys.len(), 1);

        let db_partition = db.partition("cpu", &partition_keys[0]).unwrap();

        let partition = LockableCatalogPartition::new(Arc::clone(&db), Arc::clone(&db_partition));
        let partition = partition.read();

        let chunks = LockablePartition::chunks(&partition);
        assert_eq!(chunks.len(), 1);
        let chunk = chunks[0].read();

        let (_, fut) = compact_chunks(partition.upgrade(), vec![chunk.upgrade()]).unwrap();
        // NB: perform the write before spawning the background task that performs the compaction
        let t_later_write = Utc::now();
        write_lp_with_time(db.as_ref(), "cpu,tag1=bongo,tag2=a bar=2 40", t_later_write).await;
        tokio::spawn(fut).await.unwrap().unwrap().unwrap();

        let mut chunk_summaries: Vec<_> = db_partition.read().chunk_summaries().collect();

        chunk_summaries.sort_unstable();

        let mub_summary = &chunk_summaries[0];
        let first_mub_write = mub_summary.time_of_first_write;
        let last_mub_write = mub_summary.time_of_last_write;
        assert_eq!(first_mub_write, last_mub_write);
        assert_eq!(first_mub_write, t_later_write);

        let rub_summary = &chunk_summaries[1];
        let first_rub_write = rub_summary.time_of_first_write;
        let last_rub_write = rub_summary.time_of_last_write;
        assert_eq!(first_rub_write, t_first_write);
        assert_eq!(last_rub_write, t_last_write);

        let summaries: Vec<_> = chunk_summaries
            .iter()
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
