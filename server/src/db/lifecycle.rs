use std::sync::Arc;

use chrono::{DateTime, Utc};

use ::lifecycle::LifecycleDb;
use data_types::chunk_metadata::ChunkStorage;
use data_types::database_rules::{LifecycleRules, SortOrder};
use data_types::error::ErrorLogger;
use lifecycle::{
    ChunkLifecycleAction, LifecycleChunk, LifecycleReadGuard, LifecycleWriteGuard, LockableChunk,
};
use observability_deps::tracing::info;
use tracker::{RwLock, TaskTracker};

use crate::db::catalog::chunk::CatalogChunk;
use crate::Db;
use data_types::job::Job;

///
/// A LockableCatalogChunk combines a `CatalogChunk` with its owning `Db`
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

impl<'a> LifecycleDb for &'a Db {
    type Chunk = LockableCatalogChunk<'a>;

    fn buffer_size(self) -> usize {
        self.preserved_catalog.state().metrics().memory().total()
    }

    fn rules(self) -> LifecycleRules {
        self.rules.read().lifecycle_rules.clone()
    }

    fn chunks(self, sort_order: &SortOrder) -> Vec<Self::Chunk> {
        self.preserved_catalog
            .state()
            .chunks_sorted_by(sort_order)
            .into_iter()
            .map(|chunk| LockableCatalogChunk { db: self, chunk })
            .collect()
    }

    fn drop_chunk(self, table_name: String, partition_key: String, chunk_id: u32) {
        info!(%partition_key, %chunk_id, "dropping chunk");
        let _ = Db::drop_chunk(self, &table_name, &partition_key, chunk_id)
            .log_if_error("dropping chunk to free up memory");
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

    fn table_name(&self) -> String {
        self.table_name().to_string()
    }

    fn partition_key(&self) -> String {
        self.key().to_string()
    }

    fn chunk_id(&self) -> u32 {
        self.id()
    }

    fn storage(&self) -> ChunkStorage {
        self.storage().1
    }
}
