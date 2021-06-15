use std::sync::Arc;

use chrono::{DateTime, Utc};

use ::lifecycle::LifecycleDb;
use data_types::chunk_metadata::ChunkStorage;
use data_types::database_rules::{LifecycleRules, SortOrder};
use data_types::error::ErrorLogger;
use observability_deps::tracing::info;
use tracker::{RwLock, TaskTracker};

use crate::db::catalog::chunk::CatalogChunk;
use crate::Db;
use lifecycle::{ChunkLifecycleAction, LifecycleChunk};

/// Temporary newtype wrapper for Arc<Db>
///
/// TODO: Remove the need for Db::*_in_background to take Arc<Db>
pub struct ArcDb(pub Arc<Db>);

impl LifecycleDb for ArcDb {
    type Chunk = CatalogChunk;

    fn buffer_size(&self) -> usize {
        self.0.preserved_catalog.state().metrics().memory().total()
    }

    fn rules(&self) -> LifecycleRules {
        self.0.rules.read().lifecycle_rules.clone()
    }

    fn chunks(&self, sort_order: &SortOrder) -> Vec<Arc<RwLock<CatalogChunk>>> {
        self.0
            .preserved_catalog
            .state()
            .chunks_sorted_by(sort_order)
    }

    fn move_to_read_buffer(
        &self,
        table_name: String,
        partition_key: String,
        chunk_id: u32,
    ) -> TaskTracker<()> {
        info!(%partition_key, %chunk_id, "moving chunk to read buffer");
        self.0
            .load_chunk_to_read_buffer_in_background(table_name, partition_key, chunk_id)
            .with_metadata(())
    }

    fn write_to_object_store(
        &self,
        table_name: String,
        partition_key: String,
        chunk_id: u32,
    ) -> TaskTracker<()> {
        info!(%partition_key, %chunk_id, "write chunk to object store");
        self.0
            .write_chunk_to_object_store_in_background(table_name, partition_key, chunk_id)
            .with_metadata(())
    }

    fn drop_chunk(&self, table_name: String, partition_key: String, chunk_id: u32) {
        info!(%partition_key, %chunk_id, "dropping chunk");
        let _ = self
            .0
            .drop_chunk(&table_name, &partition_key, chunk_id)
            .log_if_error("dropping chunk to free up memory");
    }

    fn unload_read_buffer(&self, table_name: String, partition_key: String, chunk_id: u32) {
        info!(%partition_key, %chunk_id, "unloading from readbuffer");
        let _ = self
            .0
            .unload_read_buffer(&table_name, &partition_key, chunk_id)
            .log_if_error("unloading from read buffer to free up memory");
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
