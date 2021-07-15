use std::sync::Arc;

use data_types::job::Job;
use futures::Future;
use lifecycle::LifecycleWriteGuard;
use object_store::path::parsed::DirsAndFileName;
use observability_deps::tracing::debug;
use snafu::ResultExt;
use tracker::{TaskTracker, TrackedFuture, TrackedFutureExt};

use super::{
    error::{CommitError, Result},
    LockableCatalogChunk, LockableCatalogPartition,
};
use crate::db::catalog::{
    chunk::{CatalogChunk, ChunkStage},
    partition::Partition,
};

pub fn drop_chunk(
    partition: LifecycleWriteGuard<'_, Partition, LockableCatalogPartition>,
    mut guard: LifecycleWriteGuard<'_, CatalogChunk, LockableCatalogChunk>,
) -> Result<(
    TaskTracker<Job>,
    TrackedFuture<impl Future<Output = Result<()>> + Send>,
)> {
    let db = Arc::clone(&guard.data().db);
    let preserved_catalog = Arc::clone(&db.preserved_catalog);
    let table_name = partition.table_name().to_string();
    let partition_key = partition.key().to_string();
    let chunk_id = guard.id();

    let (tracker, registration) = db.jobs.register(Job::DropChunk {
        db_name: partition.db_name().to_string(),
        partition_key: partition_key.clone(),
        table_name: table_name.clone(),
        chunk_id,
    });

    guard.set_dropping(&registration)?;

    // Drop locks
    let chunk = guard.into_data().chunk;
    let partition = partition.into_data().partition;

    let fut = async move {
        debug!(%table_name, %partition_key, %chunk_id, "dropping chunk");

        // need a bit of lock-juggling here to convince Rust that we don't actually send the lock
        let parquet_path = {
            let chunk_read = chunk.read();

            if let ChunkStage::Persisted { parquet, .. } = chunk_read.stage() {
                let path: DirsAndFileName = parquet.path().into();
                Some(path)
            } else {
                None
            }
        };
        if let Some(parquet_path) = parquet_path {
            let mut transaction = preserved_catalog.open_transaction().await;
            transaction.remove_parquet(&parquet_path);
            transaction.commit().await.context(CommitError)?;
        }

        let mut partition = partition.write();
        partition
            .drop_chunk(chunk_id)
            .expect("how did we end up dropping a chunk that cannot be dropped?!");

        Ok(())
    };

    Ok((tracker, fut.track(registration)))
}
