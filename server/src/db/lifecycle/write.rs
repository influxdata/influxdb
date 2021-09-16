//! This module contains the code to write chunks to the object store
use crate::db::{
    catalog::{
        chunk::{CatalogChunk, ChunkStage},
        partition::Partition,
        Catalog,
    },
    checkpoint_data_from_catalog,
    lifecycle::LockableCatalogChunk,
    DbChunk,
};

use ::lifecycle::LifecycleWriteGuard;

use chrono::Utc;
use data_types::{chunk_metadata::ChunkLifecycleAction, job::Job};
use internal_types::selection::Selection;
use observability_deps::tracing::{debug, warn};
use parquet_file::{
    catalog::interface::CatalogParquetInfo,
    chunk::{ChunkMetrics as ParquetChunkMetrics, ParquetChunk},
    metadata::IoxMetadata,
    storage::Storage,
};
use persistence_windows::{
    checkpoint::{DatabaseCheckpoint, PartitionCheckpoint, PersistCheckpointBuilder},
    persistence_windows::FlushHandle,
};
use query::{QueryChunk, QueryChunkMeta};
use snafu::ResultExt;
use std::{future::Future, sync::Arc};
use tracker::{TaskTracker, TrackedFuture, TrackedFutureExt};

use super::{
    error::{CommitError, Error, ParquetChunkError, Result, WritingToObjectStore},
    LockableCatalogPartition,
};

/// The implementation for writing a chunk to the object store
///
/// Returns a future registered with the tracker registry, and the corresponding tracker
/// The caller can either spawn this future to tokio, or block directly on it
pub(super) fn write_chunk_to_object_store(
    partition: LifecycleWriteGuard<'_, Partition, LockableCatalogPartition>,
    mut chunk: LifecycleWriteGuard<'_, CatalogChunk, LockableCatalogChunk>,
    flush_handle: FlushHandle,
) -> Result<(
    TaskTracker<Job>,
    TrackedFuture<impl Future<Output = Result<Arc<DbChunk>>> + Send>,
)> {
    let db = Arc::clone(&chunk.data().db);
    let addr = chunk.addr().clone();
    let table_name = Arc::clone(&addr.table_name);
    let partition_key = Arc::clone(&addr.partition_key);
    let chunk_order = chunk.order();

    let (tracker, registration) = db.jobs.register(Job::WriteChunk {
        chunk: addr.clone(),
    });

    // update the catalog to say we are processing this chunk and
    chunk.set_writing_to_object_store(&registration)?;
    let db_chunk = DbChunk::snapshot(&*chunk);

    let time_of_first_write = db_chunk.time_of_first_write();
    let time_of_last_write = db_chunk.time_of_last_write();

    debug!(chunk=%chunk.addr(), "chunk marked WRITING , loading tables into object store");

    // Drop locks
    let chunk = chunk.into_data().chunk;
    let partition = partition.into_data().partition;

    // Create a storage to save data of this chunk
    let storage = Storage::new(Arc::clone(&db.iox_object_store));

    let catalog_transactions_until_checkpoint = db
        .rules
        .read()
        .lifecycle_rules
        .catalog_transactions_until_checkpoint
        .get();

    let fut = async move {
        debug!(chunk=%addr, "loading table to object store");

        let (partition_checkpoint, database_checkpoint) =
            collect_checkpoints(flush_handle.checkpoint(), &db.catalog);

        // Get RecordBatchStream of data from the read buffer chunk
        let stream = db_chunk
            .read_filter(
                &Default::default(),
                Selection::All,
                db_chunk.delete_predicates(),
            )
            .expect("read filter should be infallible");

        // check that the upcoming state change will very likely succeed
        {
            // re-lock
            let guard = chunk.read();
            if matches!(guard.stage(), &ChunkStage::Persisted { .. })
                || !guard.is_in_lifecycle(ChunkLifecycleAction::Persisting)
            {
                return Err(Error::CannotWriteChunk {
                    addr: guard.addr().clone(),
                });
            }
        }

        // catalog-level transaction for preservation layer
        {
            // fetch shared (= read) guard preventing the cleanup job from deleting our files
            let _guard = db.cleanup_lock.read().await;

            // Write this table data into the object store
            //
            // IMPORTANT: Writing must take place while holding the cleanup lock, otherwise the file might be deleted
            //            between creation and the transaction commit.
            let metadata = IoxMetadata {
                creation_timestamp: Utc::now(),
                table_name: Arc::clone(&table_name),
                partition_key: Arc::clone(&partition_key),
                chunk_id: addr.chunk_id,
                partition_checkpoint,
                database_checkpoint,
                time_of_first_write,
                time_of_last_write,
                chunk_order,
            };
            let (path, file_size_bytes, parquet_metadata) = storage
                .write_to_object_store(addr, stream, metadata)
                .await
                .context(WritingToObjectStore)?;
            let parquet_metadata = Arc::new(parquet_metadata);

            let metrics = ParquetChunkMetrics::new(db.metric_registry.as_ref());
            let parquet_chunk = Arc::new(
                ParquetChunk::new(
                    &path,
                    Arc::clone(&db.iox_object_store),
                    file_size_bytes,
                    Arc::clone(&parquet_metadata),
                    Arc::clone(&table_name),
                    Arc::clone(&partition_key),
                    metrics,
                )
                .context(ParquetChunkError)?,
            );

            // IMPORTANT: Start transaction AFTER writing the actual parquet file so we do not hold
            //            the transaction lock (that is part of the PreservedCatalog) for too long.
            //            By using the cleanup lock (see above) it is ensured that the file that we
            //            have written is not deleted in between.
            let mut transaction = db.preserved_catalog.open_transaction().await;
            let info = CatalogParquetInfo {
                path,
                file_size_bytes,
                metadata: parquet_metadata,
            };
            transaction.add_parquet(&info);

            // preserved commit
            let ckpt_handle = transaction.commit().await.context(CommitError)?;

            // in-mem commit
            {
                let mut guard = chunk.write();
                if let Err(e) = guard.set_written_to_object_store(parquet_chunk) {
                    panic!("Chunk written but cannot mark as written {}", e);
                }
            }

            let create_checkpoint =
                ckpt_handle.revision_counter() % catalog_transactions_until_checkpoint == 0;
            if create_checkpoint {
                // Commit is already done, so we can just scan the catalog for the state.
                //
                // NOTE: There can only be a single transaction in this section because the checkpoint handle holds
                //       transaction lock. Therefore we don't need to worry about concurrent modifications of
                //       preserved chunks.
                if let Err(e) = ckpt_handle
                    .create_checkpoint(checkpoint_data_from_catalog(&db.catalog))
                    .await
                {
                    warn!(%e, "cannot create catalog checkpoint");

                    // That's somewhat OK. Don't fail the entire task, because the actual preservation was completed
                    // (both in-mem and within the preserved catalog).
                }
            }
        }

        {
            // Flush persisted data from persistence windows
            let mut partition = partition.write();
            partition
                .persistence_windows_mut()
                .expect("persistence windows removed")
                .flush(flush_handle);
        }

        // We know this chunk is ParquetFile type
        let chunk = chunk.read();
        Ok(DbChunk::parquet_file_snapshot(&chunk))
    };

    Ok((tracker, fut.track(registration)))
}

/// Construct database checkpoint for the given partition checkpoint in the given catalog.
fn collect_checkpoints(
    partition_checkpoint: PartitionCheckpoint,
    catalog: &Catalog,
) -> (PartitionCheckpoint, DatabaseCheckpoint) {
    // remember partition data
    let table_name = Arc::clone(partition_checkpoint.table_name());
    let partition_key = Arc::clone(partition_checkpoint.partition_key());

    // calculate checkpoint
    let mut checkpoint_builder = PersistCheckpointBuilder::new(partition_checkpoint);

    // collect checkpoints of all other partitions of all tables
    for partition in catalog.partitions() {
        let partition = partition.read();
        if (partition.table_name() == table_name.as_ref())
            && (partition.key() == partition_key.as_ref())
        {
            // same partition as the one that we're currently persisting => skip
            continue;
        }

        if let Some(sequencer_numbers) = partition.sequencer_numbers() {
            checkpoint_builder.register_other_partition(&sequencer_numbers);
        }
    }

    checkpoint_builder.build()
}
