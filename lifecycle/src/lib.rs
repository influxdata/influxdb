#![deny(broken_intra_doc_links, rust_2018_idioms)]
#![warn(
    missing_copy_implementations,
    missing_debug_implementations,
    clippy::explicit_iter_loop,
    clippy::future_not_send,
    clippy::use_self,
    clippy::clone_on_ref_ptr
)]

use chrono::{DateTime, Utc};

use data_types::chunk_metadata::{ChunkAddr, ChunkStorage};
use data_types::database_rules::LifecycleRules;
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

impl std::fmt::Display for ChunkLifecycleAction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name())
    }
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
    type Chunk: LockableChunk;
    type Partition: LockablePartition;

    /// Return the in-memory size of the database. We expect this
    /// to change from call to call as chunks are dropped
    fn buffer_size(&self) -> usize;

    /// Returns the lifecycle policy
    fn rules(&self) -> LifecycleRules;

    /// Returns a list of lockable partitions in the database
    fn partitions(&self) -> Vec<Self::Partition>;
}

/// A `LockablePartition` is a wrapper around a `LifecyclePartition` that allows
/// for planning and executing lifecycle actions on the partition
pub trait LockablePartition: Sized {
    type Partition: LifecyclePartition;
    type Chunk: LockableChunk;
    type Error: std::error::Error + Send + Sync;

    /// Acquire a shared read lock on the chunk
    fn read(&self) -> LifecycleReadGuard<'_, Self::Partition, Self>;

    /// Acquire an exclusive write lock on the chunk
    fn write(&self) -> LifecycleWriteGuard<'_, Self::Partition, Self>;

    /// Returns a specific chunk
    fn chunk(
        s: &LifecycleReadGuard<'_, Self::Partition, Self>,
        chunk_id: u32,
    ) -> Option<Self::Chunk>;

    /// Return a list of lockable chunks - the returned order must be stable
    fn chunks(s: &LifecycleReadGuard<'_, Self::Partition, Self>) -> Vec<(u32, Self::Chunk)>;

    /// Compact chunks into a single read buffer chunk
    ///
    /// TODO: Encapsulate these locks into a CatalogTransaction object
    fn compact_chunks(
        partition: LifecycleWriteGuard<'_, Self::Partition, Self>,
        chunks: Vec<LifecycleWriteGuard<'_, <Self::Chunk as LockableChunk>::Chunk, Self::Chunk>>,
    ) -> Result<TaskTracker<<Self::Chunk as LockableChunk>::Job>, Self::Error>;

    /// Drops a chunk from the partition
    fn drop_chunk(
        s: LifecycleWriteGuard<'_, Self::Partition, Self>,
        chunk_id: u32,
    ) -> Result<(), Self::Error>;
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
    /// [`drop_chunk`](LockablePartition::drop_chunk) must be used.
    fn unload_read_buffer(s: LifecycleWriteGuard<'_, Self::Chunk, Self>)
        -> Result<(), Self::Error>;
}

pub trait LifecyclePartition {
    fn partition_key(&self) -> &str;
}

/// The lifecycle operates on chunks implementing this trait
pub trait LifecycleChunk {
    fn lifecycle_action(&self) -> Option<&TaskTracker<ChunkLifecycleAction>>;

    fn clear_lifecycle_action(&mut self);

    fn time_of_first_write(&self) -> Option<DateTime<Utc>>;

    fn time_of_last_write(&self) -> Option<DateTime<Utc>>;

    fn addr(&self) -> &ChunkAddr;

    fn storage(&self) -> ChunkStorage;

    fn row_count(&self) -> usize;
}
