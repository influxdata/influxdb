use std::sync::Arc;

use data_types::job::Job;
use futures::Future;
use lifecycle::{LifecycleWriteGuard, LockableChunk};
use object_store::path::parsed::DirsAndFileName;
use observability_deps::tracing::debug;
use snafu::ResultExt;
use tracker::{TaskTracker, TrackedFuture, TrackedFutureExt};

use super::{
    error::{CannotDropUnpersistedChunk, CommitError, Error, Result},
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
    let lifecycle_persist = db.rules().lifecycle_rules.persist;

    let (tracker, registration) = db.jobs.register(Job::DropChunk {
        chunk: guard.addr().clone(),
    });

    // check if we're dropping an unpersisted chunk in a persisted DB
    // See https://github.com/influxdata/influxdb_iox/issues/2291
    if lifecycle_persist && !matches!(guard.stage(), ChunkStage::Persisted { .. }) {
        return CannotDropUnpersistedChunk {
            addr: guard.addr().clone(),
        }
        .fail();
    }

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
            } else if lifecycle_persist {
                unreachable!("Unpersisted chunks in a persisted DB should be ruled out before doing any work.")
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

pub fn drop_partition(
    partition: LifecycleWriteGuard<'_, Partition, LockableCatalogPartition>,
) -> Result<(
    TaskTracker<Job>,
    TrackedFuture<impl Future<Output = Result<()>> + Send>,
)> {
    let db = Arc::clone(&partition.data().db);
    let preserved_catalog = Arc::clone(&db.preserved_catalog);
    let table_name = partition.table_name().to_string();
    let partition_key = partition.key().to_string();
    let lifecycle_persist = db.rules().lifecycle_rules.persist;

    let (tracker, registration) = db.jobs.register(Job::DropPartition {
        partition: partition.addr().clone(),
    });

    // Get locks for all chunks.
    //
    // Note that deadlocks cannot occur here for the following reasons:
    //
    // 1. We have a partition-level write lock (`partition`) at this point, so there can only be a single
    //    `drop_partition` within this critical section (it starts at the start of the method and ends where we drop the
    //    locks) for the same partition at the same time.
    // 2. `partition.chunks()` returns chunks ordered by their IDs, so the lock acquisition order is fixed.
    let lockable_chunks: Vec<_> = partition
        .chunks()
        .map(|chunk| LockableCatalogChunk {
            db: Arc::clone(&db),
            chunk: Arc::clone(chunk),
        })
        .collect();
    let mut guards: Vec<_> = lockable_chunks
        .iter()
        .map(|lockable_chunk| lockable_chunk.write())
        .collect();

    // NOTE: here we could just use `drop_chunk` for every chunk, but that would lead to a large number of catalog
    // transactions and is rather inefficient.

    // pre-check all chunks before touching any
    for guard in &guards {
        // check if we're dropping an unpersisted chunk in a persisted DB
        // See https://github.com/influxdata/influxdb_iox/issues/2291
        if lifecycle_persist && !matches!(guard.stage(), ChunkStage::Persisted { .. }) {
            return CannotDropUnpersistedChunk {
                addr: guard.addr().clone(),
            }
            .fail();
        }

        if let Some(action) = guard.lifecycle_action() {
            return Err(Error::ChunkError {
                source: crate::db::catalog::chunk::Error::LifecycleActionAlreadyInProgress {
                    chunk: guard.addr().clone(),
                    lifecycle_action: action.metadata().name().to_string(),
                },
            });
        }
    }

    // start lifecycle action
    for guard in &mut guards {
        guard.set_dropping(&registration)?;
    }

    // Drop locks
    let chunks: Vec<_> = guards
        .into_iter()
        .map(|guard| guard.into_data().chunk)
        .collect();
    let partition = partition.into_data().partition;

    // Since after this point we don't have any locks, there might be a second `drop_partition` instance starting.
    // However since we check for active lifecycle actions above, the second run might fail until the future (that is
    // linked to the task registration) finishes (either succeeds or fails).

    let fut = async move {
        debug!(%table_name, %partition_key, "dropping partition");

        // collect parquet files that we need to remove
        let mut paths = vec![];
        for chunk in &chunks {
            let chunk_read = chunk.read();

            if let ChunkStage::Persisted { parquet, .. } = chunk_read.stage() {
                let path: DirsAndFileName = parquet.path().into();
                paths.push(path);
            } else if lifecycle_persist {
                unreachable!("Unpersisted chunks in a persisted DB should be ruled out before doing any work.")
            }
        }

        // only create catalog transaction when there's anything to do
        if !paths.is_empty() {
            let mut transaction = preserved_catalog.open_transaction().await;
            for path in paths {
                transaction.remove_parquet(&path);
            }
            transaction.commit().await.context(CommitError)?;
        }

        // AFTER the transaction completes successfully (the involved IO might just fail), finally drop the chunks that
        // we have marked using `chunk.set_dropping`.
        let mut partition = partition.write();
        for chunk in &chunks {
            let chunk_id = {
                let chunk_read = chunk.read();
                chunk_read.id()
            };

            partition
                .drop_chunk(chunk_id)
                .expect("how did we end up dropping a chunk that cannot be dropped?!");
        }

        Ok(())
    };

    Ok((tracker, fut.track(registration)))
}
