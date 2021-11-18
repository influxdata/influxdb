//! This module compact object store chunks (aka persisted chunks)

use super::{
    error::{
        ChunksNotContiguous, ChunksNotInPartition, EmptyChunks, ParquetChunkError,
        WritingToObjectStore,
    },
    LockableCatalogChunk, LockableCatalogPartition, Result,
};

use crate::{
    db::{
        catalog::{chunk::CatalogChunk, partition::Partition},
        lifecycle::merge_schemas,
        DbChunk,
    },
    Db,
};
use data_types::{
    chunk_metadata::{ChunkAddr, ChunkId, ChunkOrder},
    delete_predicate::DeletePredicate,
    job::Job,
    partition_metadata::PartitionAddr,
};
use datafusion::physical_plan::SendableRecordBatchStream;
use futures::Future;
use lifecycle::LifecycleWriteGuard;
use observability_deps::tracing::info;
use parquet_file::{
    chunk::{ChunkMetrics as ParquetChunkMetrics, ParquetChunk},
    metadata::IoxMetadata,
    storage::Storage,
};
use persistence_windows::checkpoint::{DatabaseCheckpoint, PartitionCheckpoint};
use query::{compute_sort_key, exec::ExecutorType, frontend::reorg::ReorgPlanner, QueryChunkMeta};
use schema::Schema;
use snafu::ResultExt;
use std::{
    collections::{BTreeSet, HashSet},
    sync::Arc,
};
use time::Time;
use tracker::{TaskRegistration, TaskTracker, TrackedFuture, TrackedFutureExt};

// Compact the provided object store chunks into a single object store chunk,
/// returning the newly created chunk
///
/// The function will error if
///    . No chunks are provided
///    . provided chunk(s) not belong to the provided partition
///    . not all provided chunks are persisted
///    . the provided chunks are not contiguous
/// Implementation steps
///   . Verify the eligible of the input OS chunks and mark them for ready to compact
///   . Compact the chunks
///   . Persist the compacted output into an OS chunk
///   . Drop old chunks and make the new chunk available in one transaction
pub(crate) fn compact_object_store_chunks(
    partition: LifecycleWriteGuard<'_, Partition, LockableCatalogPartition>,
    chunks: Vec<LifecycleWriteGuard<'_, CatalogChunk, LockableCatalogChunk>>,
) -> Result<(
    TaskTracker<Job>,
    TrackedFuture<impl Future<Output = Result<Option<Arc<DbChunk>>>> + Send>,
)> {
    // Track compaction duration
    let now = std::time::Instant::now();
    // Register the  compacting job
    let db = Arc::clone(&partition.data().db);
    let partition_addr = partition.addr().clone();
    let chunk_ids: Vec<_> = chunks.iter().map(|x| x.id()).collect();
    info!(%partition_addr, ?chunk_ids, "compacting object store chunks");
    let (tracker, registration) = db.jobs.register(Job::CompactObjectStoreChunks {
        partition: partition.addr().clone(),
        chunks: chunk_ids.clone(),
    });

    // Step 1: Verify input while marking and snapshoting the chunks for compacting
    let (
        time_of_first_write,
        time_of_last_write,
        input_rows,
        _delete_predicates_before,
        os_chunks,
        min_order,
        database_checkpoint,
        partition_checkpoint,
    ) = mark_chunks_to_compact(partition, chunks, &registration)?;

    let fut = async move {
        // track future runtime
        let fut_now = std::time::Instant::now();

        // Step 2: Compact & Persistent the os_chunks in one os_chunk
        let parquet_chunk = compact_persist_os_chunks(
            &db,
            &partition_addr,
            &os_chunks,
            partition_checkpoint,
            database_checkpoint,
            time_of_first_write,
            time_of_last_write,
            min_order,
        )
        .await?;
        let compacted_rows = parquet_chunk.rows();

        // Step 3: Update the preserved & in-memory catalogs to use the newly created os_chunk
        // Todo: This will be done in a sub-function that creates a single transaction that:
        //   . Drop all os_chunks fro the preserved catalog
        //   . Add the newly created os_chunk into the preserved catalog

        // Step 4: Update the in-memory catalogs to use the newly created os_chunk
        //   . Drop all os_chunks from the in-memory catalog
        //   . Add the new created os_chunk in the in-memory catalog
        //  This step can be done outside a transaction because the in-memory catalog
        //    was design to false tolerant

        // - Extra note: If there is a risk that the parquet files of os_chunks are
        // permanently deleted from the Object Store between step 3 and step 4,
        // we might need to put steps 3 and 4 in the same transaction

        // Log the summary
        let elapsed = now.elapsed();
        // input rows per second
        let throughput = (input_rows as u128 * 1_000_000_000) / elapsed.as_nanos();
        info!(input_chunks=chunk_ids.len(),
            %input_rows, %compacted_rows,
            //%sort_key, 
            compaction_took = ?elapsed,
            fut_execution_duration= ?fut_now.elapsed(),
            rows_per_sec=?throughput,
            "object store chunk(s) compacted");

        Ok(None) // todo: will be a real chunk when all todos done
    };

    Ok((tracker, fut.track(registration)))
}

/// Verify eligible compacting chunks, mark and snapshot them to get ready for compacting
/// Throws error if
///    . provided chunks do not belong to the provided partition
///    . not all provided chunks are persisted
///    . the provided chunks are not contiguous
/// Returns:
///    . min (time_of_first_write) of provided chunks
///    . max (time_of_last_write) of provided chunks
///    . total rows of the provided chunks to be compacted
///    . all delete predicates of the provided chunks
///    . snapshot of the provided chunks
///    . min(order) of the provided chunks
///    . max(database_checkpoint) of the provided chunks
///    . max(partition_checkpoint) of the provided chunks
#[allow(clippy::type_complexity)]
fn mark_chunks_to_compact(
    partition: LifecycleWriteGuard<'_, Partition, LockableCatalogPartition>,
    chunks: Vec<LifecycleWriteGuard<'_, CatalogChunk, LockableCatalogChunk>>,
    registration: &TaskRegistration,
) -> Result<(
    Time,
    Time,
    u64,
    HashSet<Arc<DeletePredicate>>,
    Vec<Arc<DbChunk>>,
    ChunkOrder,
    DatabaseCheckpoint,
    PartitionCheckpoint,
)> {
    // no chunks provided
    if chunks.is_empty() {
        return EmptyChunks {}.fail();
    }

    let db = Arc::clone(&partition.data().db);
    let partition_addr = partition.addr().clone();

    // Mark and snapshot chunks, then drop locks
    let mut time_of_first_write: Option<Time> = None;
    let mut time_of_last_write: Option<Time> = None;
    let mut chunk_orders = BTreeSet::new();
    let mut input_rows = 0;
    let mut delete_predicates: HashSet<Arc<DeletePredicate>> = HashSet::new();
    let mut min_order = ChunkOrder::MAX;

    // Todo: find a better way to initialize these
    let database_checkpoint = DatabaseCheckpoint::new(Default::default());
    let partition_checkpoint = PartitionCheckpoint::new(
        Arc::from("table"),
        Arc::from("part"),
        Default::default(),
        Time::from_timestamp_nanos(0),
    );

    let query_chunks = chunks
        .into_iter()
        .map(|mut chunk| {
            // Sanity-check
            assert!(Arc::ptr_eq(&db, &chunk.data().db));
            assert_eq!(
                chunk.table_name().as_ref(),
                partition_addr.table_name.as_ref()
            );

            // provided chunks not in the provided partition
            if chunk.key() != partition_addr.partition_key.as_ref() {
                return ChunksNotInPartition {}.fail();
            }

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
            chunk_orders.insert(chunk.order());

            // Todo:get chunk's datatbase_checkpoint and partition_checkpoint of the chunk and keep max

            // Set chunk in the right action which is compacting object store
            // This function will also error out if the chunk is not yet persisted
            chunk.set_compacting_object_store(registration)?;
            Ok(DbChunk::snapshot(&*chunk))
        })
        .collect::<Result<Vec<_>>>()?;

    // Verify if all the provided chunks are contiguous
    if !partition.contiguous_object_store_chunks(&chunk_orders) {
        return ChunksNotContiguous {}.fail();
    }

    let time_of_first_write = time_of_first_write.expect("Should have had a first write somewhere");
    let time_of_last_write = time_of_last_write.expect("Should have had a last write somewhere");

    // drop partition lock
    let _partition = partition.into_data().partition;

    Ok((
        time_of_first_write,
        time_of_last_write,
        input_rows,
        delete_predicates,
        query_chunks,
        min_order,
        database_checkpoint,
        partition_checkpoint,
    ))
}

// Compact & Persistent the os_chunks in one os_chunk
//   . Build a compact plan that scan all os_chunks
//   . Execute it the get the compacted output
//   . The compacted output will be written to OS directly without going thru RUB
//     and return a chunk named os_chunk
//     - Extra note: since each os chunk includes 2 checkpoints: chunk and DB,
//       these 2 checkpoints of the newly created os_chunk will be MAX of
//       the corresponding checkpoints in each chunk of the os_chunks
#[allow(clippy::too_many_arguments)]
async fn compact_persist_os_chunks<'a>(
    db: &'a Db,
    partition_addr: &'a PartitionAddr,
    os_chunks: &'a [Arc<DbChunk>],
    partition_checkpoint: PartitionCheckpoint,
    database_checkpoint: DatabaseCheckpoint,
    time_of_first_write: Time,
    time_of_last_write: Time,
    chunk_order: ChunkOrder,
) -> Result<Arc<ParquetChunk>> {
    let (stream, _schema, _sort_key) = compact_chunks(db, os_chunks).await.unwrap(); // Todo: use context(ReadingObjectStore)?;

    persist_stream_to_chunk(
        db,
        partition_addr,
        stream,
        partition_checkpoint,
        database_checkpoint,
        time_of_first_write,
        time_of_last_write,
        chunk_order,
    )
    .await
}

/// Create query plan to compact the given DbChunks and return its output stream
/// Return:
///    . stream of output record batch of the scanned chunks Result<SendableRecordBatchStream>
///        Deleted and duplicated data will be eliminated during the scan
///    . Output schema of the compact plan
///    . Sort Key of the output data
async fn compact_chunks(
    db: &Db,
    query_chunks: &[Arc<DbChunk>],
) -> Result<(SendableRecordBatchStream, Arc<Schema>, String)> {
    // Tracking metric
    let ctx = db.exec.new_context(ExecutorType::Reorg);

    // Compute the sorted output of the compacting result
    let sort_key = compute_sort_key(query_chunks.iter().map(|x| x.summary()));
    let sort_key_str = format!("\"{}\"", sort_key); // for logging

    // Merge schema of the compacting chunks
    let merged_schema = merge_schemas(query_chunks);

    // Build compact query plan
    let (plan_schema, plan) = ReorgPlanner::new().compact_plan(
        Arc::clone(&merged_schema),
        query_chunks.iter().map(Arc::clone),
        sort_key,
    )?;
    let physical_plan = ctx.prepare_plan(&plan).await?;

    // run the plan
    let stream = ctx.execute_stream(physical_plan).await?;

    Ok((stream, plan_schema, sort_key_str))
}

/// Persist a provided stream to a new OS chunk
#[allow(clippy::too_many_arguments)]
async fn persist_stream_to_chunk<'a>(
    db: &'a Db,
    partition_addr: &'a PartitionAddr,
    stream: SendableRecordBatchStream,
    partition_checkpoint: PartitionCheckpoint,
    database_checkpoint: DatabaseCheckpoint,
    time_of_first_write: Time,
    time_of_last_write: Time,
    chunk_order: ChunkOrder,
) -> Result<Arc<ParquetChunk>> {
    // Todo: ask Marco if this cleanup_lock is needed
    // fetch shared (= read) guard preventing the cleanup job from deleting our files
    let _guard = db.cleanup_lock.read().await;

    // Create a new chunk for this stream data
    //let table_name = Arc::from("cpu");
    let table_name = Arc::from(partition_addr.table_name.to_string());
    let partition_key = Arc::from(partition_addr.partition_key.to_string());

    let chunk_id = ChunkId::new();
    let metadata = IoxMetadata {
        creation_timestamp: db.time_provider.now(),
        table_name,
        partition_key,
        chunk_id,
        partition_checkpoint: partition_checkpoint.clone(),
        database_checkpoint: database_checkpoint.clone(),
        time_of_first_write,
        time_of_last_write,
        chunk_order,
    };

    // Create a storage to save data of this chunk
    let storage = Storage::new(Arc::clone(&db.iox_object_store));

    // Write the chunk stream data into a parquet file in the storage
    let chunk_addr = ChunkAddr::new(partition_addr, chunk_id);
    let (path, file_size_bytes, parquet_metadata) = storage
        .write_to_object_store(chunk_addr, stream, metadata)
        .await
        .context(WritingToObjectStore)?;

    // Create parquet chunk for the parquet file
    let parquet_metadata = Arc::new(parquet_metadata);
    let metrics = ParquetChunkMetrics::new(db.metric_registry.as_ref());
    let parquet_chunk = Arc::new(
        ParquetChunk::new(
            &path,
            Arc::clone(&db.iox_object_store),
            file_size_bytes,
            Arc::clone(&parquet_metadata),
            Arc::clone(&partition_addr.table_name),
            Arc::clone(&partition_addr.partition_key),
            metrics,
        )
        .context(ParquetChunkError)?,
    );

    Ok(parquet_chunk)
}

////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{db::test_helpers::write_lp, utils::TestDb, Db};

    use data_types::database_rules::LifecycleRules;
    use lifecycle::{LockableChunk, LockablePartition};
    use query::{QueryChunk, QueryDatabase};
    use std::{
        num::{NonZeroU32, NonZeroU64},
        time::Duration,
    };

    // Todo: this as copied from persist.rs and should be revived to match the needs here
    async fn test_db() -> (Arc<Db>, Arc<time::MockProvider>) {
        let time_provider = Arc::new(time::MockProvider::new(Time::from_timestamp(3409, 45)));
        let test_db = TestDb::builder()
            .lifecycle_rules(LifecycleRules {
                late_arrive_window_seconds: NonZeroU32::new(1).unwrap(),
                worker_backoff_millis: NonZeroU64::new(u64::MAX).unwrap(),
                ..Default::default()
            })
            .time_provider(Arc::<time::MockProvider>::clone(&time_provider))
            .build()
            .await;

        (test_db.db, time_provider)
    }

    #[tokio::test]
    async fn test_compact_os_negative() {
        // Tests that nothing will get compacted

        test_helpers::maybe_start_logging();

        let (db, time) = test_db().await;
        let late_arrival = Duration::from_secs(1);
        write_lp(db.as_ref(), "cpu,tag1=cupcakes bar=1 10").await;
        time.inc(late_arrival);

        let partition_keys = db.partition_keys().unwrap();
        assert_eq!(partition_keys.len(), 1);
        let db_partition = db.partition("cpu", &partition_keys[0]).unwrap();

        // Test 1: no chunks provided
        let partition = LockableCatalogPartition::new(Arc::clone(&db), Arc::clone(&db_partition));
        let partition = partition.read().upgrade();
        let compact_no_chunks = compact_object_store_chunks(partition, vec![]);
        assert!(compact_no_chunks.is_err());

        // test 2: persisted non persisted chunks
        let partition = LockableCatalogPartition::new(Arc::clone(&db), Arc::clone(&db_partition));
        let partition = partition.read();
        let chunks = LockablePartition::chunks(&partition);
        assert_eq!(chunks.len(), 1);
        let partition = partition.upgrade();
        let chunk = chunks[0].read();
        let compact_non_persisted_chunks =
            compact_object_store_chunks(partition, vec![chunk.upgrade()]);
        assert!(compact_non_persisted_chunks.is_err());

        // test 3: persisted non-contiguous chunks
        // persist chunk 1
        db.persist_partition("cpu", partition_keys[0].as_str(), true)
            .await
            .unwrap()
            .unwrap()
            .id();
        //
        // persist chunk 2
        write_lp(db.as_ref(), "cpu,tag1=chunk2,tag2=a bar=2 10").await;
        db.persist_partition("cpu", partition_keys[0].as_str(), true)
            .await
            .unwrap()
            .unwrap()
            .id();
        //
        // persist chunk 3
        write_lp(db.as_ref(), "cpu,tag1=chunk3,tag2=a bar=2 30").await;
        db.persist_partition("cpu", partition_keys[0].as_str(), true)
            .await
            .unwrap()
            .unwrap()
            .id();
        //
        // Add a MUB
        write_lp(db.as_ref(), "cpu,tag1=chunk4,tag2=a bar=2 40").await;
        // todo: Need to ask Marco why there is no handle created here
        time.inc(Duration::from_secs(40));
        //
        // let compact 2 non contiguous chunk 1 and chunk 3
        let partition = LockableCatalogPartition::new(Arc::clone(&db), Arc::clone(&db_partition));
        let partition = partition.read();
        let chunks = LockablePartition::chunks(&partition);
        assert_eq!(chunks.len(), 4);
        let partition = partition.upgrade();
        let chunk1 = chunks[0].read();
        let chunk3 = chunks[2].read();
        let compact_non_persisted_chunks =
            compact_object_store_chunks(partition, vec![chunk1.upgrade(), chunk3.upgrade()]);
        assert!(compact_non_persisted_chunks.is_err());
    }

    // todo: add tests
    //   . compact 2 contiguous OS chunks
    //   . compact 3 chunks with duplicated data
    //  . compact with deletes before compacting
    //  . compact with deletes happening during compaction
    //   . replay
}
