#![deny(rustdoc::broken_intra_doc_links, rustdoc::bare_urls, rust_2018_idioms)]
#![warn(
    missing_copy_implementations,
    missing_debug_implementations,
    clippy::explicit_iter_loop,
    clippy::future_not_send,
    clippy::use_self,
    clippy::clone_on_ref_ptr
)]

use chrono::{DateTime, Utc};
use data_types::{
    chunk_metadata::{ChunkAddr, ChunkLifecycleAction, ChunkStorage},
    database_rules::LifecycleRules,
    DatabaseName,
};
use internal_types::access::AccessMetrics;
use std::time::Instant;
use tracker::TaskTracker;

mod guard;
pub use guard::*;
mod policy;
pub use policy::*;

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

    /// Return the database name.
    fn name(&self) -> DatabaseName<'static>;
}

/// A `LockablePartition` is a wrapper around a `LifecyclePartition` that allows
/// for planning and executing lifecycle actions on the partition
pub trait LockablePartition: Sized + std::fmt::Display {
    type Partition: LifecyclePartition;
    type Chunk: LockableChunk;
    type PersistHandle: PersistHandle + Send + Sync + 'static;

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

    /// Return a list of lockable chunks in this partition.
    ///
    /// This must be ordered by `(order, id)`.
    fn chunks(s: &LifecycleReadGuard<'_, Self::Partition, Self>) -> Vec<Self::Chunk>;

    /// Compact chunks into a single read buffer chunk
    ///
    /// TODO: Encapsulate these locks into a CatalogTransaction object
    fn compact_chunks(
        partition: LifecycleWriteGuard<'_, Self::Partition, Self>,
        chunks: Vec<LifecycleWriteGuard<'_, <Self::Chunk as LockableChunk>::Chunk, Self::Chunk>>,
    ) -> Result<TaskTracker<<Self::Chunk as LockableChunk>::Job>, Self::Error>;

    /// Returns a PersistHandle for the provided partition, and the
    /// timestamp up to which to to flush
    ///
    /// Returns None if there is a persistence operation in flight, or
    /// if there are no persistable windows.
    ///
    /// `now` is the wall clock time that should be used to compute how long a given
    /// write has been present in memory
    fn prepare_persist(
        partition: &mut LifecycleWriteGuard<'_, Self::Partition, Self>,
        now: Instant,
    ) -> Option<Self::PersistHandle>;

    /// Split and persist chunks.
    ///
    /// Combines and deduplicates the data in `chunks` into two new chunks:
    ///
    /// 1. A read buffer chunk that contains any rows with timestamps
    /// prior to `max_persistable_timestamp`
    ///
    /// 2. A read buffer chunk (also written to the object store) with
    /// all other rows
    ///
    /// TODO: Encapsulate these locks into a CatalogTransaction object
    fn persist_chunks(
        partition: LifecycleWriteGuard<'_, Self::Partition, Self>,
        chunks: Vec<LifecycleWriteGuard<'_, <Self::Chunk as LockableChunk>::Chunk, Self::Chunk>>,
        handle: Self::PersistHandle,
    ) -> Result<TaskTracker<<Self::Chunk as LockableChunk>::Job>, Self::Error>;

    /// Drops a chunk from the partition
    fn drop_chunk(
        partition: LifecycleWriteGuard<'_, Self::Partition, Self>,
        chunk: LifecycleWriteGuard<'_, <Self::Chunk as LockableChunk>::Chunk, Self::Chunk>,
    ) -> Result<TaskTracker<<Self::Chunk as LockableChunk>::Job>, Self::Error>;
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
    ///
    /// TODO: Remove this function from the trait as it is
    /// not called from the lifecycle manager
    fn move_to_read_buffer(
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

    fn id(&self) -> u32;

    fn order(&self) -> u32;
}

pub trait LifecyclePartition {
    fn partition_key(&self) -> &str;

    /// Returns true if all chunks in the partition are persisted.
    fn is_persisted(&self) -> bool;

    /// Returns an approximation of the number of rows that can be persisted
    ///
    /// `now` is the wall clock time that should be used to compute how long a given
    /// write has been present in memory
    fn persistable_row_count(&self, now: Instant) -> usize;

    /// Returns the age of the oldest unpersisted write
    fn minimum_unpersisted_age(&self) -> Option<Instant>;
}

/// The lifecycle operates on chunks implementing this trait
pub trait LifecycleChunk {
    fn lifecycle_action(&self) -> Option<&TaskTracker<ChunkLifecycleAction>>;

    fn clear_lifecycle_action(&mut self);

    /// Returns the min timestamp contained within this chunk
    fn min_timestamp(&self) -> DateTime<Utc>;

    /// Returns the access metrics for this chunk
    fn access_metrics(&self) -> AccessMetrics;

    fn time_of_last_write(&self) -> DateTime<Utc>;

    fn addr(&self) -> &ChunkAddr;

    fn storage(&self) -> ChunkStorage;

    fn row_count(&self) -> usize;

    fn order(&self) -> u32;
}

/// Sort chunks for linear processing.
///
/// Sort chunks by:
///
/// 1. order: ensure compacting chunks with any potential updates together
/// 2. ID: for a stable lock order
pub fn sort_chunks<C>(mut chunks: Vec<C>) -> Vec<C>
where
    C: LockableChunk,
{
    chunks.sort_by_key(|chunk| (chunk.order(), chunk.id()));
    chunks
}

/// The trait for a persist handle
pub trait PersistHandle {
    /// Any unpersisted chunks containing rows with timestamps less than or equal to this
    /// must be included in the corresponding `LockablePartition::persist_chunks` call
    fn timestamp(&self) -> DateTime<Utc>;
}
