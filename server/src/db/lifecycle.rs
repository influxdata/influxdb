use std::sync::Arc;

use chrono::{DateTime, Utc};

use ::lifecycle::LifecycleDb;
use data_types::chunk_metadata::{ChunkAddr, ChunkStorage};
use data_types::database_rules::{LifecycleRules, SortOrder};
use data_types::error::ErrorLogger;
use data_types::job::Job;
use lifecycle::{
    ChunkLifecycleAction, LifecycleChunk, LifecyclePartition, LifecycleReadGuard,
    LifecycleWriteGuard, LockableChunk, LockablePartition,
};
use observability_deps::tracing::info;
use tracker::{RwLock, TaskTracker};

use crate::db::catalog::chunk::CatalogChunk;
use crate::db::catalog::partition::Partition;
use crate::Db;

mod compact;

///
/// A `LockableCatalogChunk` combines a `CatalogChunk` with its owning `Db`
///
/// This provides the `lifecycle::LockableChunk` trait which can be used to lock
/// the chunk, determine what to do, and then optionally trigger an action, all
/// without allowing concurrent modification
///
#[derive(Debug, Clone)]
pub struct LockableCatalogChunk<'a> {
    pub db: &'a Db,
    pub chunk: Arc<RwLock<CatalogChunk>>,
}

impl<'a> LockableChunk for LockableCatalogChunk<'a> {
    type Chunk = CatalogChunk;

    type Job = Job;

    // TODO: Separate error enumeration for lifecycle actions - db::Error is large
    type Error = super::Error;

    fn read(&self) -> LifecycleReadGuard<'_, Self::Chunk, Self> {
        LifecycleReadGuard::new(self.clone(), self.chunk.as_ref())
    }

    fn write(&self) -> LifecycleWriteGuard<'_, Self::Chunk, Self> {
        LifecycleWriteGuard::new(self.clone(), self.chunk.as_ref())
    }

    fn move_to_read_buffer(
        s: LifecycleWriteGuard<'_, Self::Chunk, Self>,
    ) -> Result<TaskTracker<Self::Job>, Self::Error> {
        info!(chunk=%s.addr(), "move to read buffer");
        let (tracker, fut) = Db::load_chunk_to_read_buffer_impl(s)?;
        let _ = tokio::spawn(async move { fut.await.log_if_error("move to read buffer") });
        Ok(tracker)
    }

    fn write_to_object_store(
        s: LifecycleWriteGuard<'_, Self::Chunk, Self>,
    ) -> Result<TaskTracker<Self::Job>, Self::Error> {
        info!(chunk=%s.addr(), "writing to object store");
        let (tracker, fut) = Db::write_chunk_to_object_store_impl(s)?;
        let _ = tokio::spawn(async move { fut.await.log_if_error("writing to object store") });
        Ok(tracker)
    }

    fn unload_read_buffer(
        s: LifecycleWriteGuard<'_, Self::Chunk, Self>,
    ) -> Result<(), Self::Error> {
        info!(chunk=%s.addr(), "unloading from readbuffer");

        let _ = Db::unload_read_buffer_impl(s)?;
        Ok(())
    }
}

///
/// A `LockableCatalogPartition` combines a `Partition` with its owning `Db`
///
/// This provides the `lifecycle::LockablePartition` trait which can be used to lock
/// the chunk, determine what to do, and then optionally trigger an action, all
/// without allowing concurrent modification
///
#[derive(Debug, Clone)]
pub struct LockableCatalogPartition<'a> {
    pub db: &'a Db,
    pub partition: Arc<RwLock<Partition>>,
}

impl<'a> LockablePartition for LockableCatalogPartition<'a> {
    type Partition = Partition;

    type Chunk = LockableCatalogChunk<'a>;

    type DropError = super::catalog::partition::Error;

    type CompactError = super::lifecycle::compact::Error;

    fn read(&self) -> LifecycleReadGuard<'_, Self::Partition, Self> {
        LifecycleReadGuard::new(self.clone(), self.partition.as_ref())
    }

    fn write(&self) -> LifecycleWriteGuard<'_, Self::Partition, Self> {
        LifecycleWriteGuard::new(self.clone(), self.partition.as_ref())
    }

    fn chunk(
        s: &LifecycleReadGuard<'_, Self::Partition, Self>,
        chunk_id: u32,
    ) -> Option<Self::Chunk> {
        s.chunk(chunk_id).map(|chunk| LockableCatalogChunk {
            db: s.data().db,
            chunk: Arc::clone(chunk),
        })
    }

    fn chunks(s: &LifecycleReadGuard<'_, Self::Partition, Self>) -> Vec<(u32, Self::Chunk)> {
        let db = s.data().db;
        s.keyed_chunks()
            .map(|(id, chunk)| {
                (
                    id,
                    LockableCatalogChunk {
                        db,
                        chunk: Arc::clone(chunk),
                    },
                )
            })
            .collect()
    }

    fn compact_chunks(
        partition: LifecycleWriteGuard<'_, Self::Partition, Self>,
        chunks: Vec<LifecycleWriteGuard<'_, CatalogChunk, Self::Chunk>>,
    ) -> Result<TaskTracker<Job>, Self::CompactError> {
        info!(table=%partition.table_name(), partition=%partition.partition_key(), "compacting chunks");
        let (tracker, fut) = compact::compact_chunks(partition, chunks)?;
        let _ = tokio::spawn(async move { fut.await.log_if_error("compacting chunks") });
        Ok(tracker)
    }

    fn drop_chunk(
        mut s: LifecycleWriteGuard<'_, Self::Partition, Self>,
        chunk_id: u32,
    ) -> Result<(), Self::DropError> {
        s.drop_chunk(chunk_id)?;
        Ok(())
    }
}

impl<'a> LifecycleDb for &'a Db {
    type Chunk = LockableCatalogChunk<'a>;
    type Partition = LockableCatalogPartition<'a>;

    fn buffer_size(self) -> usize {
        self.catalog.metrics().memory().total()
    }

    fn rules(self) -> LifecycleRules {
        self.rules.read().lifecycle_rules.clone()
    }

    fn partitions(self) -> Vec<Self::Partition> {
        self.catalog
            .partitions()
            .into_iter()
            .map(|partition| LockableCatalogPartition {
                db: self,
                partition,
            })
            .collect()
    }

    fn chunks(self, sort_order: &SortOrder) -> Vec<Self::Chunk> {
        self.catalog
            .chunks_sorted_by(sort_order)
            .into_iter()
            .map(|chunk| LockableCatalogChunk { db: self, chunk })
            .collect()
    }
}

impl LifecyclePartition for Partition {
    fn partition_key(&self) -> &str {
        self.key()
    }
}

impl LifecycleChunk for CatalogChunk {
    fn lifecycle_action(&self) -> Option<&TaskTracker<ChunkLifecycleAction>> {
        self.lifecycle_action()
    }

    fn clear_lifecycle_action(&mut self) {
        self.clear_lifecycle_action()
            .expect("failed to clear lifecycle action")
    }

    fn time_of_first_write(&self) -> Option<DateTime<Utc>> {
        self.time_of_first_write()
    }

    fn time_of_last_write(&self) -> Option<DateTime<Utc>> {
        self.time_of_last_write()
    }

    fn addr(&self) -> &ChunkAddr {
        self.addr()
    }

    fn storage(&self) -> ChunkStorage {
        self.storage().1
    }

    fn row_count(&self) -> usize {
        self.storage().0
    }
}
