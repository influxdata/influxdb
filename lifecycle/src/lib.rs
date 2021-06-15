use data_types::database_rules::{LifecycleRules, SortOrder};
use std::sync::Arc;
use tracker::{RwLock, TaskTracker};

mod policy;

use chrono::{DateTime, Utc};
use data_types::chunk_metadata::ChunkStorage;
pub use policy::*;

/// Any lifecycle action currently in progress for this chunk
#[derive(Debug, Copy, Clone, PartialEq)]
pub enum ChunkLifecycleAction {
    /// Chunk is in the process of being moved to the read buffer
    Moving,

    /// Chunk is in the process of being written to object storage
    Persisting,

    /// Chunk is in the process of being compacted
    Compacting,
}

impl ChunkLifecycleAction {
    pub fn name(&self) -> &'static str {
        match self {
            Self::Moving => "Moving to the Read Buffer",
            Self::Persisting => "Persisting to Object Storage",
            Self::Compacting => "Compacting",
        }
    }
}

/// A trait that encapsulates the database logic that is automated by `LifecyclePolicy`
pub trait LifecycleDb {
    type Chunk: LifecycleChunk;

    /// Return the in-memory size of the database. We expect this
    /// to change from call to call as chunks are dropped
    fn buffer_size(&self) -> usize;

    /// Returns the lifecycle policy
    fn rules(&self) -> LifecycleRules;

    /// Returns a list of chunks sorted in the order
    /// they should prioritised
    fn chunks(&self, order: &SortOrder) -> Vec<Arc<RwLock<Self::Chunk>>>;

    /// Starts an operation to move a chunk to the read buffer
    fn move_to_read_buffer(
        &self,
        table_name: String,
        partition_key: String,
        chunk_id: u32,
    ) -> TaskTracker<()>;

    /// Starts an operation to write a chunk to the object store
    fn write_to_object_store(
        &self,
        table_name: String,
        partition_key: String,
        chunk_id: u32,
    ) -> TaskTracker<()>;

    /// Drops a chunk from the database
    fn drop_chunk(&self, table_name: String, partition_key: String, chunk_id: u32);

    /// Remove the copy of the Chunk's data from the read buffer.
    ///
    /// Note that this can only be called for persisted chunks
    /// (otherwise the read buffer may contain the *only* copy of this
    /// chunk's data). In order to drop un-persisted chunks,
    /// [`drop_chunk`](Self::drop_chunk) must be used.
    fn unload_read_buffer(&self, table_name: String, partition_key: String, chunk_id: u32);
}

/// The lifecycle operates on chunks implementing this trait
pub trait LifecycleChunk {
    fn lifecycle_action(&self) -> Option<&TaskTracker<ChunkLifecycleAction>>;

    fn clear_lifecycle_action(&mut self);

    fn time_of_first_write(&self) -> Option<DateTime<Utc>>;

    fn time_of_last_write(&self) -> Option<DateTime<Utc>>;

    fn table_name(&self) -> String;

    fn partition_key(&self) -> String;

    fn chunk_id(&self) -> u32;

    fn storage(&self) -> ChunkStorage;
}
