use std::sync::Arc;

use chrono::{DateTime, Utc};

use ::lifecycle::LifecycleDb;
use data_types::chunk_metadata::{ChunkAddr, ChunkStorage};
use data_types::database_rules::LifecycleRules;
use data_types::error::ErrorLogger;
use data_types::job::Job;
use data_types::partition_metadata::{InfluxDbType, TableSummary};
use data_types::DatabaseName;
use hashbrown::HashMap;
use internal_types::schema::sort::SortKey;
use internal_types::schema::TIME_COLUMN_NAME;
use lifecycle::{
    ChunkLifecycleAction, LifecycleChunk, LifecyclePartition, LifecycleReadGuard,
    LifecycleWriteGuard, LockableChunk, LockablePartition,
};
use observability_deps::tracing::info;
use tracker::{RwLock, TaskTracker};

use crate::db::catalog::chunk::CatalogChunk;
use crate::db::catalog::partition::Partition;
use crate::Db;

pub(crate) use compact::compact_chunks;
pub(crate) use error::{Error, Result};
pub(crate) use move_chunk::move_chunk_to_read_buffer;
pub(crate) use unload::unload_read_buffer_chunk;
pub(crate) use write::write_chunk_to_object_store;

mod compact;
mod error;
mod move_chunk;
mod unload;
mod write;

/// A newtype wrapper around `Arc<Db>` to workaround trait orphan rules
#[derive(Debug, Clone)]
pub struct ArcDb(pub(super) Arc<Db>);

impl std::ops::Deref for ArcDb {
    type Target = Db;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

///
/// A `LockableCatalogChunk` combines a `CatalogChunk` with its owning `Db`
///
/// This provides the `lifecycle::LockableChunk` trait which can be used to lock
/// the chunk, determine what to do, and then optionally trigger an action, all
/// without allowing concurrent modification
///
#[derive(Debug, Clone)]
pub struct LockableCatalogChunk {
    pub db: Arc<Db>,
    pub chunk: Arc<RwLock<CatalogChunk>>,
}

impl LockableChunk for LockableCatalogChunk {
    type Chunk = CatalogChunk;

    type Job = Job;

    type Error = Error;

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
        let (tracker, fut) = move_chunk::move_chunk_to_read_buffer(s)?;
        let _ = tokio::spawn(async move { fut.await.log_if_error("move to read buffer") });
        Ok(tracker)
    }

    fn write_to_object_store(
        s: LifecycleWriteGuard<'_, Self::Chunk, Self>,
    ) -> Result<TaskTracker<Self::Job>, Self::Error> {
        info!(chunk=%s.addr(), "writing to object store");
        let (tracker, fut) = write::write_chunk_to_object_store(s)?;
        let _ = tokio::spawn(async move { fut.await.log_if_error("writing to object store") });
        Ok(tracker)
    }

    fn unload_read_buffer(
        s: LifecycleWriteGuard<'_, Self::Chunk, Self>,
    ) -> Result<(), Self::Error> {
        info!(chunk=%s.addr(), "unloading from readbuffer");

        let _ = self::unload::unload_read_buffer_chunk(s)?;
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
pub struct LockableCatalogPartition {
    pub db: Arc<Db>,
    pub partition: Arc<RwLock<Partition>>,
}

impl LockablePartition for LockableCatalogPartition {
    type Partition = Partition;

    type Chunk = LockableCatalogChunk;

    type Error = super::lifecycle::Error;

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
            db: Arc::clone(&s.data().db),
            chunk: Arc::clone(chunk),
        })
    }

    fn chunks(s: &LifecycleReadGuard<'_, Self::Partition, Self>) -> Vec<(u32, Self::Chunk)> {
        s.keyed_chunks()
            .map(|(id, chunk)| {
                (
                    id,
                    LockableCatalogChunk {
                        db: Arc::clone(&s.data().db),
                        chunk: Arc::clone(chunk),
                    },
                )
            })
            .collect()
    }

    fn compact_chunks(
        partition: LifecycleWriteGuard<'_, Self::Partition, Self>,
        chunks: Vec<LifecycleWriteGuard<'_, CatalogChunk, Self::Chunk>>,
    ) -> Result<TaskTracker<Job>, Self::Error> {
        info!(table=%partition.table_name(), partition=%partition.partition_key(), "compacting chunks");
        let (tracker, fut) = compact::compact_chunks(partition, chunks)?;
        let _ = tokio::spawn(async move { fut.await.log_if_error("compacting chunks") });
        Ok(tracker)
    }

    fn drop_chunk(
        mut s: LifecycleWriteGuard<'_, Self::Partition, Self>,
        chunk_id: u32,
    ) -> Result<(), Self::Error> {
        s.drop_chunk(chunk_id)?;
        Ok(())
    }
}

impl LifecycleDb for ArcDb {
    type Chunk = LockableCatalogChunk;
    type Partition = LockableCatalogPartition;

    fn buffer_size(&self) -> usize {
        self.catalog.metrics().memory().total()
    }

    fn rules(&self) -> LifecycleRules {
        self.rules.read().lifecycle_rules.clone()
    }

    fn partitions(&self) -> Vec<Self::Partition> {
        self.catalog
            .partitions()
            .into_iter()
            .map(|partition| LockableCatalogPartition {
                db: Arc::clone(&self.0),
                partition,
            })
            .collect()
    }

    fn name(&self) -> DatabaseName<'static> {
        self.rules.read().name.clone()
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

/// Compute a sort key that orders lower cardinality columns first
///
/// In the absence of more precise information, this should yield a
/// good ordering for RLE compression
fn compute_sort_key<'a>(summaries: impl Iterator<Item = &'a TableSummary>) -> SortKey<'a> {
    let mut cardinalities: HashMap<&str, u64> = Default::default();
    for summary in summaries {
        for column in &summary.columns {
            if column.influxdb_type != Some(InfluxDbType::Tag) {
                continue;
            }

            if let Some(count) = column.stats.distinct_count() {
                *cardinalities.entry(column.name.as_str()).or_default() += count.get()
            }
        }
    }

    let mut cardinalities: Vec<_> = cardinalities.into_iter().collect();
    cardinalities.sort_by_key(|x| x.1);

    let mut key = SortKey::with_capacity(cardinalities.len() + 1);
    for (col, _) in cardinalities {
        key.push(col, Default::default())
    }
    key.push(TIME_COLUMN_NAME, Default::default());
    key
}
