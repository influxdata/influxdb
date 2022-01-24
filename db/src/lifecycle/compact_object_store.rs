//! This module compact object store chunks (aka persisted chunks)

use super::{
    error::{
        ChunksNotContiguousSnafu, ChunksNotInPartitionSnafu, ChunksNotPersistedSnafu, CommitSnafu,
        ComparePartitionCheckpointSnafu, EmptyChunksSnafu, NoCheckpointSnafu, ParquetChunkSnafu,
        ParquetMetaReadSnafu, WritingToObjectStoreSnafu,
    },
    LockableCatalogChunk, LockableCatalogPartition, Result,
};
use crate::{
    catalog::{chunk::CatalogChunk, partition::Partition},
    lifecycle::merge_schemas,
    Db, DbChunk,
};
use data_types::{
    chunk_metadata::{ChunkAddr, ChunkId, ChunkOrder},
    delete_predicate::DeletePredicate,
    job::Job,
    partition_metadata::PartitionAddr,
};
use datafusion::physical_plan::SendableRecordBatchStream;
use futures::Future;
use iox_object_store::ParquetFilePath;
use lifecycle::LifecycleWriteGuard;
use observability_deps::tracing::info;
use parquet_catalog::interface::CatalogParquetInfo;
use parquet_file::{
    chunk::{ChunkMetrics as ParquetChunkMetrics, ParquetChunk},
    metadata::IoxMetadata,
    storage::Storage,
};
use persistence_windows::checkpoint::{DatabaseCheckpoint, PartitionCheckpoint};
use query::{compute_sort_key, exec::ExecutorType, frontend::reorg::ReorgPlanner, QueryChunkMeta};
use schema::Schema;
use snafu::{OptionExt, ResultExt};
use std::{
    cmp::Ordering,
    collections::{BTreeSet, HashSet},
    ops::RangeInclusive,
    sync::Arc,
};
use time::Time;
use tracker::{RwLock, TaskRegistration, TaskTracker, TrackedFuture, TrackedFutureExt};

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

    // Step1: create a new ID for the result chunk after compacting
    // This ID will be kept in the compacting chunk's in-memory catalog for us
    // to save and retrieve its delete predicates added during compaction
    let compacted_chunk_id = ChunkId::new();

    // Step 2: Verify input while marking and snapshoting the chunks for compacting
    // The partition will be unlock after the chunks are marked and snaphot
    let compacting_os_chunks =
        mark_chunks_to_compact(partition, chunks, &registration, compacted_chunk_id)?;
    let delete_predicates_before = compacting_os_chunks.delete_predicates;

    let fut = async move {
        // track future runtime
        let fut_now = std::time::Instant::now();

        // Step 3: Compact the os chunks into a stream
        // No locks are hold during compaction
        let compacted_stream = compact_chunks(&db, &compacting_os_chunks.os_chunks).await?;
        let mut compacted_rows = 0;
        let _schema = compacted_stream.schema;
        let sort_key = compacted_stream.sort_key;

        // Step 4: Start to persist files and update the preserved catalog accordingly
        // This process needs to hold cleanup lock to avoid the persisted file from getting deleted right after
        // it is created and before it is updated in the preserved catalog
        let (iox_metadata, compacted_and_persisted_chunk) = {
            // fetch shared (= read) guard preventing the cleanup job from deleting our files
            let _guard = db.cleanup_lock.read().await;

            // Step 4.1: Write the chunk as a parquet file into the object store
            let iox_metadata = IoxMetadata {
                creation_timestamp: db.time_provider.now(),
                table_name: Arc::clone(&partition_addr.table_name),
                partition_key: Arc::clone(&partition_addr.partition_key),
                chunk_id: compacted_chunk_id,
                partition_checkpoint: compacting_os_chunks.partition_checkpoint.clone(),
                database_checkpoint: compacting_os_chunks.database_checkpoint.clone(),
                time_of_first_write: compacting_os_chunks.time_of_first_write,
                time_of_last_write: compacting_os_chunks.time_of_last_write,
                chunk_order: compacting_os_chunks.min_order,
            };
            let compacted_and_persisted_chunk = persist_stream_to_chunk(
                &db,
                &partition_addr,
                compacted_stream.stream,
                iox_metadata.clone(),
            )
            .await?;

            // Step 4.2: Update the preserved catalogs to use the newly created os_chunk
            update_preserved_catalog(
                &db,
                &compacting_os_chunks.compacted_parquet_file_paths,
                &compacted_and_persisted_chunk,
            )
            .await?;

            (iox_metadata, compacted_and_persisted_chunk)
        }; // End of cleanup locking

        // Step 5: Update the in-memory catalog to use the newly created os_chunk
        //   . Drop all os_chunks from the in-memory catalog
        //   . Add the new created os_chunk in the in-memory catalog
        let dbchunk = update_in_memory_catalog(
            &chunk_ids,
            iox_metadata,
            compacted_and_persisted_chunk.clone(),
            compacting_os_chunks.partition,
            delete_predicates_before,
        )
        .await;

        if let Some(compacted_and_persisted_chunk) = compacted_and_persisted_chunk {
            compacted_rows = compacted_and_persisted_chunk.rows();
        }

        // Log the summary
        let elapsed = now.elapsed();
        // input rows per second
        let throughput =
            (compacting_os_chunks.input_rows as u128 * 1_000_000_000) / elapsed.as_nanos();
        info!(input_chunks=chunk_ids.len(),
            %compacting_os_chunks.input_rows, %compacted_rows,
            %sort_key,
            compaction_took = ?elapsed,
            fut_execution_duration= ?fut_now.elapsed(),
            rows_per_sec=?throughput,
            "object store chunk(s) compacted");

        Ok(dbchunk)
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
/// The partition will be unlocked before the function is returned.
fn mark_chunks_to_compact(
    partition: LifecycleWriteGuard<'_, Partition, LockableCatalogPartition>,
    chunks: Vec<LifecycleWriteGuard<'_, CatalogChunk, LockableCatalogChunk>>,
    registration: &TaskRegistration,
    compacted_chunk_id: ChunkId,
) -> Result<CompactingOsChunks> {
    // no chunks provided
    if chunks.is_empty() {
        return EmptyChunksSnafu {}.fail();
    }

    let db = Arc::clone(&partition.data().db);
    let partition_addr = partition.addr().clone();

    // Mark and snapshot chunks, then drop locks
    let mut time_of_first_write = Time::MAX;
    let mut time_of_last_write = Time::MIN;
    let mut chunk_ids = BTreeSet::new();
    let mut input_rows = 0;
    let mut delete_predicates: HashSet<Arc<DeletePredicate>> = HashSet::new();
    let mut compacted_parquet_file_paths = vec![];
    let mut min_order = ChunkOrder::MAX;
    let mut max_order = ChunkOrder::MIN;

    let mut database_checkpoint = DatabaseCheckpoint::new(Default::default());
    let mut partition_checkpoint: Option<PartitionCheckpoint> = None;

    let os_chunks = chunks
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
                return ChunksNotInPartitionSnafu {}.fail();
            }

            input_rows += chunk.table_summary().total_count();

            let candidate_first = chunk.time_of_first_write();
            time_of_first_write = std::cmp::min(time_of_first_write, candidate_first);

            let candidate_last = chunk.time_of_last_write();
            time_of_last_write = std::cmp::max(time_of_last_write, candidate_last);

            delete_predicates.extend(chunk.delete_predicates().iter().cloned());

            min_order = min_order.min(chunk.order());
            max_order = max_order.max(chunk.order());
            chunk_ids.insert(chunk.id());

            // read IoxMetadata from the parquet chunk's  metadata
            if let Some(parquet_chunk) = chunk.parquet_chunk() {
                let iox_parquet_metadata = parquet_chunk.parquet_metadata();
                let iox_metadata = iox_parquet_metadata
                    .decode()
                    .context(ParquetMetaReadSnafu)?
                    .read_iox_metadata()
                    .context(ParquetMetaReadSnafu)?;

                // fold all database_checkpoints into one for the compacting chunk
                database_checkpoint.fold(&iox_metadata.database_checkpoint);

                // keep max partition_checkpoint for the compacting chunk
                if let Some(part_ckpt) = &partition_checkpoint {
                    let ordering = part_ckpt
                        .partial_cmp(&iox_metadata.partition_checkpoint)
                        .context(ComparePartitionCheckpointSnafu)?;
                    if ordering == Ordering::Less {
                        partition_checkpoint = Some(iox_metadata.partition_checkpoint);
                    }
                } else {
                    partition_checkpoint = Some(iox_metadata.partition_checkpoint);
                }
            } else {
                return ChunksNotPersistedSnafu {}.fail();
            }

            // Set chunk in the right action which is compacting object store
            // This function will also error out if the chunk is not yet persisted
            chunk.set_compacting_object_store(registration, compacted_chunk_id)?;

            // Get the parquet dbchunk snapshot and also keep its file path to remove later
            let dbchunk = DbChunk::parquet_file_snapshot(&*chunk);
            compacted_parquet_file_paths.push(dbchunk.object_store_path().unwrap().clone());
            Ok(dbchunk)
        })
        .collect::<Result<Vec<_>>>()?;

    if partition_checkpoint.is_none() {
        return NoCheckpointSnafu {}.fail();
    }
    let partition_checkpoint = partition_checkpoint.unwrap();

    // Verify if all the provided chunks are contiguous
    let order_range = RangeInclusive::new(min_order, max_order);
    if !partition.contiguous_chunks(&chunk_ids, &order_range)? {
        return ChunksNotContiguousSnafu {}.fail();
    }

    // drop partition lock
    let partition = partition.into_data().partition;

    Ok(CompactingOsChunks {
        time_of_first_write,
        time_of_last_write,
        input_rows,
        delete_predicates,
        compacted_parquet_file_paths,
        os_chunks,
        min_order,
        database_checkpoint,
        partition_checkpoint,
        partition,
    })
}

/// This struct is used as return data of compacting os chunks

#[derive(Debug, Clone)]
struct CompactingOsChunks {
    time_of_first_write: Time,
    time_of_last_write: Time,
    input_rows: u64,
    delete_predicates: HashSet<Arc<DeletePredicate>>,
    compacted_parquet_file_paths: Vec<ParquetFilePath>,
    os_chunks: Vec<Arc<DbChunk>>,
    min_order: ChunkOrder,
    database_checkpoint: DatabaseCheckpoint,
    partition_checkpoint: PartitionCheckpoint,
    partition: Arc<RwLock<Partition>>,
}

/// Create query plan to compact the given DbChunks and return its output stream
/// Return:
///    . stream of output record batch of the scanned chunks Result<SendableRecordBatchStream>
///        Deleted and duplicated data will be eliminated during the scan
///    . Output schema of the compact plan
///    . Sort Key of the output data
async fn compact_chunks(db: &Db, query_chunks: &[Arc<DbChunk>]) -> Result<CompactedStream> {
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

    Ok(CompactedStream {
        stream,
        schema: plan_schema,
        sort_key: sort_key_str,
    })
}

/// Struct holding output of a compacted stream
struct CompactedStream {
    stream: SendableRecordBatchStream,
    schema: Arc<Schema>,
    sort_key: String,
}

/// Persist a provided stream to a new OS chunk
async fn persist_stream_to_chunk<'a>(
    db: &'a Db,
    partition_addr: &'a PartitionAddr,
    stream: SendableRecordBatchStream,
    iox_metadata: IoxMetadata,
) -> Result<Option<Arc<ParquetChunk>>> {
    // Create a storage to save data of this chunk
    let storage = Storage::new(Arc::clone(&db.iox_object_store));

    // Write the chunk stream data into a parquet file in the storage
    let chunk_addr = ChunkAddr::new(partition_addr, iox_metadata.chunk_id);
    let written_result = storage
        .write_to_object_store(chunk_addr, stream, iox_metadata)
        .await
        .context(WritingToObjectStoreSnafu)?;

    // the stream was empty
    if written_result.is_none() {
        return Ok(None);
    }

    // Create parquet chunk for the parquet file
    let (path, file_size_bytes, parquet_metadata) = written_result.unwrap();
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
        .context(ParquetChunkSnafu)?,
    );

    Ok(Some(parquet_chunk))
}

/// Update the preserved catalog : replace compacted chunks with a newly persisted chunk
async fn update_preserved_catalog(
    db: &Db,
    compacted_parquet_file_paths: &[ParquetFilePath],
    parquet_chunk: &Option<Arc<ParquetChunk>>,
) -> Result<()> {
    // Open transaction
    let mut transaction = db.preserved_catalog.open_transaction().await;

    // Remove compacted chunks
    for parquet_file_path in compacted_parquet_file_paths {
        transaction.remove_parquet(parquet_file_path);
    }

    // Add new chunk if compaction returns some data
    if let Some(parquet_chunk) = parquet_chunk {
        let catalog_parquet_info = CatalogParquetInfo::from_chunk(parquet_chunk);
        transaction.add_parquet(&catalog_parquet_info);
    }

    // Close/commit the transaction
    transaction.commit().await.context(CommitSnafu)?;

    Ok(())
}

async fn update_in_memory_catalog(
    chunk_ids: &[ChunkId],
    iox_metadata: IoxMetadata,
    parquet_chunk: Option<Arc<ParquetChunk>>,
    partition: Arc<RwLock<Partition>>,
    delete_predicates_before: HashSet<Arc<DeletePredicate>>,
) -> Option<Arc<DbChunk>> {
    // Acquire write lock to drop the old chunks while also getting delete predicates added during compaction
    let mut partition = partition.write();

    let mut delete_predicates_after = HashSet::new();
    for id in chunk_ids {
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

    // Only create a new chunk if compaction returns rows
    let dbchunk = match parquet_chunk {
        Some(parquet_chunk) => {
            let chunk = partition.insert_object_store_only_chunk(
                iox_metadata.chunk_id,
                parquet_chunk,
                iox_metadata.time_of_first_write,
                iox_metadata.time_of_last_write,
                delete_predicates,
                iox_metadata.chunk_order,
            );
            let dbchunk = DbChunk::parquet_file_snapshot(&*chunk.read());
            Some(dbchunk)
        }
        None => None,
    };

    // drop partition lock
    std::mem::drop(partition);

    dbchunk
}

////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{load::load_or_create_preserved_catalog, test_helpers::write_lp, utils::make_db};
    use data_types::{
        chunk_metadata::ChunkStorage,
        delete_predicate::{DeleteExpr, DeletePredicate},
        timestamp::TimestampRange,
    };
    use lifecycle::{LockableChunk, LockablePartition};
    use query::QueryChunk;

    #[tokio::test]
    async fn test_compact_os_no_chunks() {
        test_helpers::maybe_start_logging();

        let db = make_db().await.db;
        let partition_key = "1970-01-01T00";
        write_lp(&db, "cpu,tag1=cupcakes bar=1 10");

        let partition = db.lockable_partition("cpu", partition_key).unwrap();
        let partition = partition.write();

        let (_, registration) = db.jobs.register(Job::CompactObjectStoreChunks {
            partition: partition.addr().clone(),
            chunks: vec![],
        });
        let chunk_id = ChunkId::new();
        let compact_no_chunks = mark_chunks_to_compact(partition, vec![], &registration, chunk_id);

        let err = compact_no_chunks.unwrap_err();
        assert!(
            err.to_string()
                .contains("No object store chunks provided for compacting"),
            "No object store chunks provided for compacting"
        );
    }

    #[tokio::test]
    async fn test_compact_os_non_os_chunks() {
        test_helpers::maybe_start_logging();

        let db = make_db().await.db;
        let partition_key = "1970-01-01T00";
        write_lp(&db, "cpu,tag1=cupcakes bar=1 10");

        // persisted non persisted chunks
        let partition = db.lockable_partition("cpu", partition_key).unwrap();
        let partition = partition.read();
        let chunks = LockablePartition::chunks(&partition);
        assert_eq!(chunks.len(), 1);
        let partition = partition.upgrade();
        let chunk = chunks[0].write();

        let (_, registration) = db.jobs.register(Job::CompactObjectStoreChunks {
            partition: partition.addr().clone(),
            chunks: vec![chunk.id()],
        });

        let chunk_id = ChunkId::new();
        let compact_non_persisted_chunks =
            mark_chunks_to_compact(partition, vec![chunk], &registration, chunk_id);
        let err = compact_non_persisted_chunks.unwrap_err();
        assert!(
            err.to_string()
                .contains("Cannot compact chunks because at least one is not yet persisted"),
            "Cannot compact chunks because at least one is not yet persisted"
        );
    }

    #[tokio::test]
    async fn test_compact_os_non_contiguous_chunks() {
        test_helpers::maybe_start_logging();

        let db = make_db().await.db;
        let partition_key = "1970-01-01T00";
        write_lp(&db, "cpu,tag1=cupcakes bar=1 10");

        // persist chunk 1
        db.persist_partition("cpu", partition_key, true)
            .await
            .unwrap()
            .unwrap()
            .id();
        //
        // persist chunk 2
        write_lp(db.as_ref(), "cpu,tag1=chunk2,tag2=a bar=2 10");
        db.persist_partition("cpu", partition_key, true)
            .await
            .unwrap()
            .unwrap()
            .id();
        //
        // persist chunk 3
        write_lp(db.as_ref(), "cpu,tag1=chunk3,tag2=a bar=2 30");
        db.persist_partition("cpu", partition_key, true)
            .await
            .unwrap()
            .unwrap()
            .id();
        //
        // Add a MUB
        write_lp(db.as_ref(), "cpu,tag1=chunk4,tag2=a bar=2 40");

        // let compact 2 non contiguous chunk 1 and chunk 3
        let partition = db.lockable_partition("cpu", partition_key).unwrap();
        let partition = partition.read();
        let chunks = LockablePartition::chunks(&partition);
        assert_eq!(chunks.len(), 4);
        let partition = partition.upgrade();
        let chunk1 = chunks[0].write();
        let chunk3 = chunks[2].write();

        let (_, registration) = db.jobs.register(Job::CompactObjectStoreChunks {
            partition: partition.addr().clone(),
            chunks: vec![chunk1.id(), chunk3.id()],
        });

        let chunk_id = ChunkId::new();
        let compact_non_contiguous_persisted_chunks =
            mark_chunks_to_compact(partition, vec![chunk1, chunk3], &registration, chunk_id);
        let err = compact_non_contiguous_persisted_chunks.unwrap_err();
        assert!(
            err.to_string()
                .contains("Cannot compact the provided persisted chunks. They are not contiguous"),
            "Cannot compact the provided persisted chunks. They are not contiguous"
        );
    }

    #[tokio::test]
    async fn test_compact_os_two_contiguous_chunks() {
        test_helpers::maybe_start_logging();

        let db = make_db().await.db;
        let partition_key = "1970-01-01T00";
        write_lp(&db, "cpu,tag1=cupcakes bar=1 10");

        // persist chunk 1
        let chunk_id_1 = db
            .persist_partition("cpu", partition_key, true)
            .await
            .unwrap()
            .unwrap()
            .id();
        //
        // persist chunk 2
        write_lp(&db, "cpu,tag1=cookies bar=2 20");
        let _chunk_id_2 = db
            .persist_partition("cpu", partition_key, true)
            .await
            .unwrap()
            .unwrap()
            .id();
        //
        // persist chunk 3
        write_lp(&db, "cpu,tag1=cookies,tag2=20 bar=3 30");
        let _chunk_id_3 = db
            .persist_partition("cpu", partition_key, true)
            .await
            .unwrap()
            .unwrap()
            .id();
        //
        // drop RUB of chunk 1 but keep its OS
        db.unload_read_buffer("cpu", partition_key, chunk_id_1)
            .unwrap();
        //
        // Add a MUB
        write_lp(db.as_ref(), "cpu,tag1=brownies,tag2=a bar=2 40");

        // Verify results before OS compacting
        let partition = db.lockable_partition("cpu", partition_key).unwrap();
        let partition = partition.read();
        let chunks = LockablePartition::chunks(&partition);
        assert_eq!(chunks.len(), 4);
        // ensure all RUBs are unloaded
        let mut summary_chunks: Vec<_> = partition.chunk_summaries().collect();
        assert_eq!(summary_chunks.len(), 4);
        summary_chunks.sort_by_key(|c| c.storage);
        assert_eq!(summary_chunks[0].storage, ChunkStorage::OpenMutableBuffer);
        assert_eq!(summary_chunks[0].row_count, 1);
        assert_eq!(
            summary_chunks[1].storage,
            ChunkStorage::ReadBufferAndObjectStore
        );
        assert_eq!(summary_chunks[1].row_count, 1);
        assert_eq!(
            summary_chunks[2].storage,
            ChunkStorage::ReadBufferAndObjectStore
        );
        assert_eq!(summary_chunks[2].row_count, 1);
        assert_eq!(summary_chunks[3].storage, ChunkStorage::ObjectStoreOnly);
        assert_eq!(summary_chunks[3].row_count, 1);

        // compact 2 contiguous chunk 1 and chunk 2
        let partition = partition.upgrade();
        let chunk1 = chunks[0].write();
        let chunk2 = chunks[1].write();
        // Provide the chunk ids in reverse contiguous order to see if we handle it well
        let _compacted_chunk = compact_object_store_chunks(partition, vec![chunk2, chunk1])
            .unwrap()
            .1
            .await
            .unwrap()
            .unwrap();

        // verify results
        let partition = db.partition("cpu", partition_key).unwrap();
        let mut summary_chunks: Vec<_> = partition.read().chunk_summaries().collect();
        summary_chunks.sort_by_key(|c| c.storage);
        assert_eq!(summary_chunks.len(), 3);
        // MUB
        assert_eq!(summary_chunks[0].storage, ChunkStorage::OpenMutableBuffer);
        assert_eq!(summary_chunks[0].row_count, 1);
        // RUB & OS (chunk_id_3 tha tis not compacted)
        assert_eq!(
            summary_chunks[1].storage,
            ChunkStorage::ReadBufferAndObjectStore
        );
        assert_eq!(summary_chunks[1].row_count, 1);
        // OS: the result of compacting 2 persisted chunks (chunk_id_1 and chunk_id_2)
        assert_eq!(summary_chunks[2].storage, ChunkStorage::ObjectStoreOnly);
        assert_eq!(summary_chunks[2].row_count, 2);
    }

    #[tokio::test]
    async fn test_compact_os_soft_and_hard_deletes() {
        test_helpers::maybe_start_logging();

        // -----------------------------------------------
        // Create 3 OS & 1 MUB chunks

        let db = make_db().await.db;
        let partition_key = "1970-01-01T00";
        write_lp(&db, "cpu,tag1=cupcakes bar=1 10");
        write_lp(&db, "cpu,tag1=cookies bar=2 10"); // hard delete - pred1 happens before compaction

        // persist chunk 1
        let _chunk_id_1 = db
            .persist_partition("cpu", partition_key, true)
            .await
            .unwrap()
            .unwrap()
            .id();

        // persist chunk 2
        write_lp(&db, "cpu,tag1=cookies bar=2 20"); // hard delete - pred2 happens twice before and during compaction
        write_lp(&db, "cpu,tag1=cookies bar=3 30"); // soft delete - pred3 happens concurrently with compaction
        write_lp(&db, "cpu,tag1=cupcakes bar=2 20");

        let chunk_id_2 = db
            .persist_partition("cpu", partition_key, true)
            .await
            .unwrap()
            .unwrap()
            .id();

        // persist chunk 3
        write_lp(&db, "cpu,tag1=cookies bar=2 20"); // hard delete - pred2 happens twice before and during compaction
        let _chunk_id_3 = db
            .persist_partition("cpu", partition_key, true)
            .await
            .unwrap()
            .unwrap()
            .id();

        // drop RUB of chunk_id_2
        // So chunk_id_1 and chunk_id_3 have both RUB and OS, chunk_id_2 only has OS
        db.unload_read_buffer("cpu", partition_key, chunk_id_2)
            .unwrap();

        // Add a MUB
        write_lp(db.as_ref(), "cpu,tag1=brownies,tag2=a bar=2 40");

        // -----------------------------------------------
        // Verify results before OS compacting

        // Acquire read lock on partition
        let partition = db.lockable_partition("cpu", partition_key).unwrap();
        let partition = partition.read();
        let chunks = LockablePartition::chunks(&partition);
        assert_eq!(chunks.len(), 4);

        // Verify chunk types
        let mut summary_chunks: Vec<_> = partition.chunk_summaries().collect();
        assert_eq!(summary_chunks.len(), 4);
        summary_chunks.sort_by_key(|c| c.storage);
        assert_eq!(summary_chunks[0].storage, ChunkStorage::OpenMutableBuffer);
        assert_eq!(summary_chunks[0].row_count, 1);
        assert_eq!(
            summary_chunks[1].storage,
            ChunkStorage::ReadBufferAndObjectStore
        );
        assert_eq!(summary_chunks[1].row_count, 2); // chunk_id_1
        assert_eq!(
            summary_chunks[2].storage,
            ChunkStorage::ReadBufferAndObjectStore
        );
        assert_eq!(summary_chunks[2].row_count, 1); // chunk_id_3
        assert_eq!(summary_chunks[3].storage, ChunkStorage::ObjectStoreOnly);
        assert_eq!(summary_chunks[3].row_count, 3); // chunk_id_2

        // Get min partition checkpoint which is the checkpoint of the first chunk
        let min_partition_checkpoint = {
            let chunk = chunks[0].clone().chunk;
            let chunk = chunk.read();
            let parquet_chunk = chunk.parquet_chunk().unwrap();
            let iox_parquet_metadata = parquet_chunk.parquet_metadata();
            let iox_metadata = iox_parquet_metadata
                .decode()
                .unwrap()
                .read_iox_metadata()
                .unwrap();
            iox_metadata.partition_checkpoint
        };

        // unlock partition
        partition.into_data();

        // -----------------------------------------------
        // Create 3 delete predicates that will delete all cookies in 3 different deletes

        let pred1 = Arc::new(DeletePredicate {
            range: TimestampRange::new(0, 11),
            exprs: vec![DeleteExpr::new(
                "tag1".to_string(),
                data_types::delete_predicate::Op::Eq,
                data_types::delete_predicate::Scalar::String("cookies".to_string()),
            )],
        });
        let pred2 = Arc::new(DeletePredicate {
            range: TimestampRange::new(12, 21),
            exprs: vec![DeleteExpr::new(
                "tag1".to_string(),
                data_types::delete_predicate::Op::Eq,
                data_types::delete_predicate::Scalar::String("cookies".to_string()),
            )],
        });
        let pred3 = Arc::new(DeletePredicate {
            range: TimestampRange::new(22, 31),
            exprs: vec![DeleteExpr::new(
                "tag1".to_string(),
                data_types::delete_predicate::Op::Eq,
                data_types::delete_predicate::Scalar::String("cookies".to_string()),
            )],
        });

        // -----------------------------------------------
        // Apply deletes pred1 and pred2 before compaction

        db.delete("cpu", pred1).unwrap();
        db.delete(
            "cpu",
            Arc::<data_types::delete_predicate::DeletePredicate>::clone(&pred2),
        )
        .unwrap();

        // -----------------------------------------------
        // Compact 3 contiguous chunks 1, 2, 3

        // acquire write lock to start compacting
        let partition = db.lockable_partition("cpu", partition_key).unwrap();
        let partition = partition.write();
        let chunk1 = chunks[0].write();
        let chunk2 = chunks[1].write();
        let chunk3 = chunks[2].write();

        // Start the OS compaction job but do not poll it yet
        let (_tracker, fut) =
            compact_object_store_chunks(partition, vec![chunk1, chunk2, chunk3]).unwrap();

        // Apply deletes pred2 (again) and pred3
        db.delete("cpu", pred2).unwrap(); // duplicate delete and will not have any effects
        db.delete(
            "cpu",
            Arc::<data_types::delete_predicate::DeletePredicate>::clone(&pred3),
        )
        .unwrap();

        // Continue running compaction job
        let compacted_chunk = tokio::spawn(fut).await.unwrap().unwrap().unwrap();

        // -----------------------------------------------
        // Verify results after compaction

        let partition = db.partition("cpu", partition_key).unwrap();
        let partition = partition.read();
        let mut summary_chunks: Vec<_> = partition.chunk_summaries().collect();

        // ----------
        // Verify chunk types and number of rows in each
        summary_chunks.sort_by_key(|c| c.storage);
        assert_eq!(summary_chunks.len(), 2);
        // MUB still here
        assert_eq!(summary_chunks[0].storage, ChunkStorage::ClosedMutableBuffer);
        assert_eq!(summary_chunks[0].row_count, 1);
        // OS: the result of compacting all 3 persisted chunks
        assert_eq!(summary_chunks[1].storage, ChunkStorage::ObjectStoreOnly);
        // 2 rows + 1 soft deleted row due to pred3 happens during compaction
        assert_eq!(summary_chunks[1].row_count, 3);

        // ----------
        // Verify delete predicates in the in-memory catalog
        let catalog_chunks = &db.catalog.chunks();
        // In-memory catalog should include 2 chunks: MUB and OS
        assert_eq!(catalog_chunks.len(), 2);

        let chunk = &catalog_chunks[0];
        let chunk = chunk.read();
        let del_preds1 = chunk.delete_predicates();
        let id1 = chunk.id();

        let chunk = &catalog_chunks[1];
        let chunk = chunk.read();
        let del_preds2 = chunk.delete_predicates();

        // Get delete pred of the compacted chunk
        let compacted_delete_preds = {
            if id1 == summary_chunks[1].id {
                del_preds1
            } else {
                del_preds2
            }
        };
        // should include pred3 as it happened concurrently with the compaction
        assert_eq!(compacted_delete_preds, vec![Arc::clone(&pred3)]);

        // ----------
        // Verify delete predicates in preserved catalog
        let metric_registry = Arc::new(metric::Registry::new());
        let (_preserved_catalog, catalog, _replay_plan) = load_or_create_preserved_catalog(
            &*db.name(),
            Arc::clone(&db.iox_object_store),
            metric_registry,
            Arc::clone(&db.time_provider),
            false,
            false,
        )
        .await
        .unwrap();

        // preserved catalog only has knowledge of the compacted OS chunk
        let catalog_chunks = catalog.chunks();
        assert_eq!(catalog_chunks.len(), 1);
        let chunk = catalog_chunks[0].read();
        let preserved_compacted_delete_preds = chunk.delete_predicates();

        // The brand new compacted OS chunk should not have any delete predicates.
        // Note that, by designed, predicates of deletes that happen concurrently with compacting OS are only
        // kept in its in-memory catalog chunk (and will be added in the preserved catalog in the background later).
        // The brand new compacted OS chunk does not include any delete predicates. All previous soft-delete
        // predicates were already hard deleted during its OS compaction.
        assert!(preserved_compacted_delete_preds.is_empty());

        // ----------
        // verify partition checkpoint of the compacted chunk which should be the
        // one of the first OS chunk (earliest checkpoint)
        let chunk = compacted_chunk.unwrap();
        let parquet_chunk = chunk.parquet_chunk().unwrap();
        let iox_parquet_metadata = parquet_chunk.parquet_metadata();
        let iox_metadata = iox_parquet_metadata
            .decode()
            .unwrap()
            .read_iox_metadata()
            .unwrap();
        let compacted_partition_checkpoint = iox_metadata.partition_checkpoint;
        assert_eq!(min_partition_checkpoint, compacted_partition_checkpoint);
    }

    #[tokio::test]
    async fn test_compact_os_on_chunk_delete_all() {
        test_helpers::maybe_start_logging();

        let db = make_db().await.db;
        let partition_key = "1970-01-01T00";
        write_lp(&db, "cpu,tag1=cupcakes bar=1 10");
        write_lp(&db, "cpu,tag1=cookies bar=2 10"); // delete

        // persist chunk 1
        let _chunk_id_1 = db
            .persist_partition("cpu", partition_key, true)
            .await
            .unwrap()
            .unwrap()
            .id();

        // Delete all
        let predicate = Arc::new(DeletePredicate {
            range: TimestampRange::new(0, 30),
            exprs: vec![],
        });
        db.delete("cpu", predicate).unwrap();
        //
        // Add a MUB
        write_lp(db.as_ref(), "cpu,tag1=brownies,tag2=a bar=2 40");

        // Verify results before OS compacting
        let partition = db.lockable_partition("cpu", partition_key).unwrap();
        let partition = partition.read();
        let chunks = LockablePartition::chunks(&partition);
        assert_eq!(chunks.len(), 2);
        // chunk summary
        let mut summary_chunks: Vec<_> = partition.chunk_summaries().collect();
        assert_eq!(summary_chunks.len(), 2);
        summary_chunks.sort_by_key(|c| c.storage);
        assert_eq!(summary_chunks[0].storage, ChunkStorage::OpenMutableBuffer);
        assert_eq!(summary_chunks[0].row_count, 1);
        assert_eq!(
            summary_chunks[1].storage,
            ChunkStorage::ReadBufferAndObjectStore
        );
        assert_eq!(summary_chunks[1].row_count, 2);

        // compact the only OS chunk
        let partition = partition.upgrade();
        let chunk1 = chunks[0].write();
        compact_object_store_chunks(partition, vec![chunk1])
            .unwrap()
            .1
            .await
            .unwrap()
            .unwrap();

        // verify results
        let partition = db.partition("cpu", partition_key).unwrap();
        let mut summary_chunks: Vec<_> = partition.read().chunk_summaries().collect();
        summary_chunks.sort_by_key(|c| c.storage);
        //Should only have MUB chunk
        assert_eq!(summary_chunks.len(), 1);
        assert_eq!(summary_chunks[0].storage, ChunkStorage::OpenMutableBuffer);
        assert_eq!(summary_chunks[0].row_count, 1);
    }
}
