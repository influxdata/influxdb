//! This module contains the code to write chunks to the object store
use crate::db::{
    catalog::chunk::{CatalogChunk, ChunkStage},
    checkpoint_data_from_catalog,
    lifecycle::LockableCatalogChunk,
    DbChunk,
};

use ::lifecycle::LifecycleWriteGuard;

use chrono::Utc;
use data_types::job::Job;
use internal_types::selection::Selection;
use object_store::path::parsed::DirsAndFileName;
use observability_deps::tracing::{debug, warn};
use parquet_file::{
    chunk::{ChunkMetrics as ParquetChunkMetrics, ParquetChunk},
    metadata::IoxMetadata,
    storage::Storage,
};
use persistence_windows::checkpoint::{
    DatabaseCheckpoint, PartitionCheckpoint, PersistCheckpointBuilder,
};
use query::QueryChunk;
use snafu::ResultExt;
use std::{collections::BTreeMap, future::Future, sync::Arc};
use tracker::{TaskTracker, TrackedFuture, TrackedFutureExt};

use super::error::{
    CommitError, Error, ParquetChunkError, Result, TransactionError, WritingToObjectStore,
};

/// The implementation for writing a chunk to the object store
///
/// Returns a future registered with the tracker registry, and the corresponding tracker
/// The caller can either spawn this future to tokio, or block directly on it
pub fn write_chunk_to_object_store(
    mut guard: LifecycleWriteGuard<'_, CatalogChunk, LockableCatalogChunk>,
) -> Result<(
    TaskTracker<Job>,
    TrackedFuture<impl Future<Output = Result<Arc<DbChunk>>> + Send>,
)> {
    let db = Arc::clone(&guard.data().db);
    let addr = guard.addr().clone();

    // TODO: Use ChunkAddr within Job
    let (tracker, registration) = db.jobs.register(Job::WriteChunk {
        db_name: addr.db_name.to_string(),
        partition_key: addr.partition_key.to_string(),
        table_name: addr.table_name.to_string(),
        chunk_id: addr.chunk_id,
    });

    // update the catalog to say we are processing this chunk and
    guard.set_writing_to_object_store(&registration)?;
    let db_chunk = DbChunk::snapshot(&*guard);

    debug!(chunk=%guard.addr(), "chunk marked WRITING , loading tables into object store");

    // Drop locks
    let chunk = guard.unwrap().chunk;

    // Create a storage to save data of this chunk
    let storage = Storage::new(Arc::clone(&db.store), db.server_id);

    let catalog_transactions_until_checkpoint = db
        .rules
        .read()
        .lifecycle_rules
        .catalog_transactions_until_checkpoint
        .get();

    let fut = async move {
        debug!(chunk=%addr, "loading table to object store");

        // Get RecordBatchStream of data from the read buffer chunk
        let stream = db_chunk
            .read_filter(&Default::default(), Selection::All)
            .expect("read filter should be infallible");

        // check that the upcoming state change will very likely succeed
        {
            // re-lock
            let guard = chunk.read();
            if matches!(guard.stage(), &ChunkStage::Persisted { .. })
                || !guard.is_in_lifecycle(::lifecycle::ChunkLifecycleAction::Persisting)
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
            let (partition_checkpoint, database_checkpoint) =
                fake_partition_and_database_checkpoint(&addr.table_name, &addr.partition_key);
            let metadata = IoxMetadata {
                creation_timestamp: Utc::now(),
                table_name: addr.table_name.to_string(),
                partition_key: addr.partition_key.to_string(),
                chunk_id: addr.chunk_id,
                partition_checkpoint,
                database_checkpoint,
            };
            let (path, parquet_metadata) = storage
                .write_to_object_store(addr, stream, metadata)
                .await
                .context(WritingToObjectStore)?;
            let parquet_metadata = Arc::new(parquet_metadata);

            let metrics = db
                .catalog
                .metrics_registry
                .register_domain_with_labels("parquet", db.catalog.metric_labels.clone());
            let metrics =
                ParquetChunkMetrics::new(&metrics, db.catalog.metrics().memory().parquet());
            let parquet_chunk = Arc::new(
                ParquetChunk::new(
                    path.clone(),
                    Arc::clone(&db.store),
                    Arc::clone(&parquet_metadata),
                    metrics,
                )
                .context(ParquetChunkError)?,
            );

            let path: DirsAndFileName = path.into();

            // IMPORTANT: Start transaction AFTER writing the actual parquet file so we do not hold the
            //            transaction lock (that is part of the PreservedCatalog) for too long. By using the
            //            cleanup lock (see above) it is ensured that the file that we have written is not deleted
            //            in between.
            let mut transaction = db.preserved_catalog.open_transaction().await;
            transaction
                .add_parquet(&path, &parquet_metadata)
                .context(TransactionError)?;

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

        // We know this chunk is ParquetFile type
        let chunk = chunk.read();
        Ok(DbChunk::parquet_file_snapshot(&chunk))
    };

    Ok((tracker, fut.track(registration)))
}

/// Fake until we have the split implementation in-place.
fn fake_partition_and_database_checkpoint(
    table_name: &str,
    partition_key: &str,
) -> (PartitionCheckpoint, DatabaseCheckpoint) {
    // create partition checkpoint
    let sequencer_numbers = BTreeMap::new();
    let min_unpersisted_timestamp = Utc::now();
    let partition_checkpoint = PartitionCheckpoint::new(
        table_name.to_string(),
        partition_key.to_string(),
        sequencer_numbers,
        min_unpersisted_timestamp,
    );

    // build database checkpoint
    let builder = PersistCheckpointBuilder::new(partition_checkpoint);
    builder.build()
}
