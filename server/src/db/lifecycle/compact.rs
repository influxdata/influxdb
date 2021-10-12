//! This module contains the code to compact chunks together

use super::{error::Result, merge_schemas, LockableCatalogChunk, LockableCatalogPartition};
use crate::db::{
    catalog::{chunk::CatalogChunk, partition::Partition},
    lifecycle::collect_rub,
    DbChunk,
};
use data_types::{chunk_metadata::ChunkOrder, job::Job};
use lifecycle::LifecycleWriteGuard;
use observability_deps::tracing::info;
use predicate::delete_predicate::DeletePredicate;
use query::{compute_sort_key, exec::ExecutorType, frontend::reorg::ReorgPlanner, QueryChunkMeta};
use std::{collections::HashSet, future::Future, sync::Arc};
use time::Time;
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
    TrackedFuture<impl Future<Output = Result<Option<Arc<DbChunk>>>> + Send>,
)> {
    assert!(
        !chunks.is_empty(),
        "must provide at least 1 chunk for compaction"
    );

    let now = std::time::Instant::now(); // time compaction duration.
    let db = Arc::clone(&partition.data().db);
    let addr = partition.addr().clone();
    let chunk_ids: Vec<_> = chunks.iter().map(|x| x.id()).collect();

    info!(%addr, ?chunk_ids, "compacting chunks");

    let (tracker, registration) = db.jobs.register(Job::CompactChunks {
        partition: partition.addr().clone(),
        chunks: chunk_ids.clone(),
    });

    // Mark and snapshot chunks, then drop locks
    let mut input_rows = 0;
    let mut time_of_first_write: Option<Time> = None;
    let mut time_of_last_write: Option<Time> = None;
    let mut delete_predicates_before: HashSet<Arc<DeletePredicate>> = HashSet::new();
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

            delete_predicates_before.extend(chunk.delete_predicates().iter().cloned());

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

        let physical_plan = ctx.prepare_plan(&plan).await?;
        let stream = ctx.execute_stream(physical_plan).await?;
        let maybe_rb_chunk = collect_rub(stream, &addr, metric_registry.as_ref()).await?;

        let mut partition = partition.write();
        let mut delete_predicates_after = HashSet::new();
        for id in &chunk_ids {
            let chunk = partition
                .force_drop_chunk(*id)
                .expect("There was a lifecycle action attached to this chunk, who deleted it?!");

            let chunk = chunk.read();
            for pred in chunk.delete_predicates() {
                if !delete_predicates_before.contains(pred) {
                    delete_predicates_after.insert(Arc::clone(pred));
                }
            }
        }

        let delete_predicates = {
            let mut tmp: Vec<_> = delete_predicates_after.into_iter().collect();
            tmp.sort();
            tmp
        };

        let rb_chunk = match maybe_rb_chunk {
            Some(rb_chunk) => rb_chunk,
            None => {
                info!(%addr, ?chunk_ids, "no rows to persist, no chunk created");
                return Ok(None);
            }
        };

        let rub_row_groups = rb_chunk.row_groups();
        let output_rows = rb_chunk.rows();
        let (_, chunk) = partition.create_rub_chunk(
            rb_chunk,
            time_of_first_write,
            time_of_last_write,
            schema,
            delete_predicates,
            min_order,
            None,
        );

        // input rows per second
        let elapsed = now.elapsed();
        let throughput = (input_rows as u128 * 1_000_000_000) / elapsed.as_nanos();

        info!(input_chunks=chunk_ids.len(), %rub_row_groups,
                        %input_rows, %output_rows,
                        sort_key=%key_str, compaction_took = ?elapsed, fut_execution_duration= ?fut_now.elapsed(),
                        rows_per_sec=?throughput,  "chunk(s) compacted");

        let snapshot = DbChunk::snapshot(&chunk.read());
        Ok(Some(snapshot))
    };

    Ok((tracker, fut.track(registration)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::test_helpers::write_lp;
    use crate::utils::{make_db, make_db_time};
    use data_types::chunk_metadata::ChunkStorage;
    use data_types::timestamp::TimestampRange;
    use lifecycle::{LockableChunk, LockablePartition};
    use predicate::delete_expr::{DeleteExpr, Op, Scalar};
    use query::QueryDatabase;
    use std::time::Duration;

    #[tokio::test]
    async fn test_compact_freeze() {
        let (db, time) = make_db_time().await;

        let t_first_write = time.inc(Duration::from_secs(1));
        write_lp(db.as_ref(), "cpu,tag1=cupcakes bar=1 10").await;
        write_lp(db.as_ref(), "cpu,tag1=asfd,tag2=foo bar=2 20").await;
        write_lp(db.as_ref(), "cpu,tag1=bingo,tag2=foo bar=2 10").await;
        write_lp(db.as_ref(), "cpu,tag1=bongo,tag2=a bar=2 20").await;

        let t_last_write = time.inc(Duration::from_secs(1));
        write_lp(db.as_ref(), "cpu,tag1=bongo,tag2=a bar=2 10").await;

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
        let t_later_write = time.inc(Duration::from_secs(1));
        write_lp(db.as_ref(), "cpu,tag1=bongo,tag2=a bar=2 40").await;
        tokio::spawn(fut).await.unwrap().unwrap().unwrap();

        let mut chunk_summaries: Vec<_> = db_partition.read().chunk_summaries().collect();

        chunk_summaries.sort_unstable();

        let mub_summary = &chunk_summaries[1];
        let first_mub_write = mub_summary.time_of_first_write;
        let last_mub_write = mub_summary.time_of_last_write;
        assert_eq!(mub_summary.storage, ChunkStorage::OpenMutableBuffer);
        assert_eq!(first_mub_write, last_mub_write);
        assert_eq!(first_mub_write, t_later_write);

        let rub_summary = &chunk_summaries[0];
        let first_rub_write = rub_summary.time_of_first_write;
        let last_rub_write = rub_summary.time_of_last_write;
        assert_eq!(rub_summary.storage, ChunkStorage::ReadBuffer);
        assert_eq!(first_rub_write, t_first_write);
        assert_eq!(last_rub_write, t_last_write);

        let summaries: Vec<_> = chunk_summaries
            .iter()
            .map(|summary| (summary.storage, summary.row_count))
            .collect();

        assert_eq!(
            summaries,
            vec![
                (ChunkStorage::ReadBuffer, 5),
                (ChunkStorage::OpenMutableBuffer, 1),
            ]
        )
    }

    #[tokio::test]
    async fn test_compact_delete_all() {
        let db = make_db().await.db;

        write_lp(db.as_ref(), "cpu,tag1=cupcakes bar=1 10").await;
        write_lp(db.as_ref(), "cpu,tag1=cupcakes bar=3 23").await;
        write_lp(db.as_ref(), "cpu,tag1=cupcakes bar=2 26").await;

        let partition_keys = db.partition_keys().unwrap();
        assert_eq!(partition_keys.len(), 1);

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
        let chunk = db
            .compact_partition("cpu", partition_keys[0].as_str())
            .await
            .unwrap();

        assert!(chunk.is_none());
    }

    #[tokio::test]
    async fn test_delete_predicate_propagation() {
        // setup DB
        let db = make_db().await.db;

        // | foo | delete before compaction | delete during compaction |
        // | --- | ------------------------ | ------------------------ |
        // |   1 |                      yes |                       no |
        // |   2 |                      yes |                      yes |
        // |   3 |                       no |                      yes |
        // |   4 |                       no |                       no |
        write_lp(db.as_ref(), "cpu foo=1 10").await;
        write_lp(db.as_ref(), "cpu foo=2 20").await;
        write_lp(db.as_ref(), "cpu foo=3 20").await;
        write_lp(db.as_ref(), "cpu foo=4 20").await;

        let range = TimestampRange {
            start: 0,
            end: 1_000,
        };
        let pred1 = Arc::new(DeletePredicate {
            range,
            exprs: vec![DeleteExpr::new("foo".to_string(), Op::Eq, Scalar::I64(1))],
        });
        let pred2 = Arc::new(DeletePredicate {
            range,
            exprs: vec![DeleteExpr::new("foo".to_string(), Op::Eq, Scalar::I64(2))],
        });
        let pred3 = Arc::new(DeletePredicate {
            range,
            exprs: vec![DeleteExpr::new("foo".to_string(), Op::Eq, Scalar::I64(3))],
        });
        db.delete("cpu", Arc::clone(&pred1)).await.unwrap();
        db.delete("cpu", Arc::clone(&pred2)).await.unwrap();

        // start compaction job (but don't poll the future yet)
        let partition_keys = db.partition_keys().unwrap();
        assert_eq!(partition_keys.len(), 1);
        let partition_key: &str = partition_keys[0].as_ref();

        let db_partition = db.partition("cpu", partition_key).unwrap();

        let partition = LockableCatalogPartition::new(Arc::clone(&db), Arc::clone(&db_partition));
        let partition = partition.read();

        let chunks = LockablePartition::chunks(&partition);
        assert_eq!(chunks.len(), 1);
        let chunk = chunks[0].read();

        let (_, fut) = compact_chunks(partition.upgrade(), vec![chunk.upgrade()]).unwrap();

        // add more delete predicates
        db.delete("cpu", Arc::clone(&pred2)).await.unwrap();
        db.delete("cpu", Arc::clone(&pred3)).await.unwrap();

        // finish future
        tokio::spawn(fut).await.unwrap().unwrap().unwrap();

        // check delete predicates
        let chunks = db.catalog.chunks();
        assert_eq!(chunks.len(), 1);
        let chunk = &chunks[0];
        let chunk = chunk.read();
        let actual = chunk.delete_predicates();
        let expected = vec![pred3];
        assert_eq!(actual, &expected);
    }
}
