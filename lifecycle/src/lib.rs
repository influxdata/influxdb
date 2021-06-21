use chrono::{DateTime, Utc};

use data_types::chunk_metadata::ChunkStorage;
use data_types::database_rules::{LifecycleRules, SortOrder};
pub use guard::*;
pub use policy::*;
use tracker::TaskTracker;

mod guard;
mod policy;

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
///
/// Note: This trait is meant to be implemented for references to types allowing them
/// to yield `LifecycleDb::Chunk` with non-static lifetimes
pub trait LifecycleDb {
    type Chunk: LockableChunk;

    /// Return the in-memory size of the database. We expect this
    /// to change from call to call as chunks are dropped
    fn buffer_size(self) -> usize;

    /// Returns the lifecycle policy
    fn rules(self) -> LifecycleRules;

    /// Returns a list of lockable chunks sorted in the order
    /// they should prioritised
    fn chunks(self, order: &SortOrder) -> Vec<Self::Chunk>;

    /// Drops a chunk from the database
    ///
    /// TODO: Transaction-ify this
    fn drop_chunk(self, table_name: String, partition_key: String, chunk_id: u32);
}

/// A `LockableChunk` is a wrapper around a `LifecycleChunk` that allows for
/// planning and executing lifecycle actions on the chunk
///
/// Specifically a read lock can be obtained, a decision made based on the chunk's
/// data, and then a lifecycle action optionally triggered, all without allowing
/// concurrent modification
///
/// See the module level documentation for the guard module for more information
/// on why this trait is the way it is
///
pub trait LockableChunk: Sized {
    type Chunk: LifecycleChunk;
    type Job: Sized + Send + Sync + 'static;
    type Error: std::error::Error + Send + Sync;

    /// Acquire a shared read lock on the chunk
    fn read(&self) -> LifecycleReadGuard<'_, Self::Chunk, Self>;

    /// Acquire an exclusive write lock on the chunk
    fn write(&self) -> LifecycleWriteGuard<'_, Self::Chunk, Self>;

    /// Starts an operation to move a chunk to the read buffer
    fn move_to_read_buffer(
        s: LifecycleWriteGuard<'_, Self::Chunk, Self>,
    ) -> Result<TaskTracker<Self::Job>, Self::Error>;

    /// Starts an operation to write a chunk to the object store
    fn write_to_object_store(
        s: LifecycleWriteGuard<'_, Self::Chunk, Self>,
    ) -> Result<TaskTracker<Self::Job>, Self::Error>;

    /// Remove the copy of the Chunk's data from the read buffer.
    ///
    /// Note that this can only be called for persisted chunks
    /// (otherwise the read buffer may contain the *only* copy of this
    /// chunk's data). In order to drop un-persisted chunks,
    /// [`drop_chunk`](Self::drop_chunk) must be used.
    fn unload_read_buffer(s: LifecycleWriteGuard<'_, Self::Chunk, Self>)
        -> Result<(), Self::Error>;
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
