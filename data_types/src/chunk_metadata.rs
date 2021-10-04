//! Module contains a representation of chunk metadata
use crate::partition_metadata::PartitionAddr;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::{num::NonZeroU32, sync::Arc};

/// Address of the chunk within the catalog
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct ChunkAddr {
    /// Database name
    pub db_name: Arc<str>,

    /// What table does the chunk belong to?
    pub table_name: Arc<str>,

    /// What partition does the chunk belong to?
    pub partition_key: Arc<str>,

    /// The ID of the chunk
    pub chunk_id: ChunkId,
}

impl ChunkAddr {
    pub fn new(partition: &PartitionAddr, chunk_id: ChunkId) -> Self {
        Self {
            db_name: Arc::clone(&partition.db_name),
            table_name: Arc::clone(&partition.table_name),
            partition_key: Arc::clone(&partition.partition_key),
            chunk_id,
        }
    }

    pub fn into_partition(self) -> PartitionAddr {
        PartitionAddr {
            db_name: self.db_name,
            table_name: self.table_name,
            partition_key: self.partition_key,
        }
    }
}

impl std::fmt::Display for ChunkAddr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Chunk('{}':'{}':'{}':{})",
            self.db_name,
            self.table_name,
            self.partition_key,
            self.chunk_id.get()
        )
    }
}

/// Which storage system is a chunk located in?
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone, Copy, Serialize, Deserialize)]
pub enum ChunkStorage {
    /// The chunk is still open for new writes, in the Mutable Buffer
    OpenMutableBuffer,

    /// The chunk is no longer open for writes, in the Mutable Buffer
    ClosedMutableBuffer,

    /// The chunk is in the Read Buffer (where it can not be mutated)
    ReadBuffer,

    /// The chunk is both in ReadBuffer and Object Store
    ReadBufferAndObjectStore,

    /// The chunk is stored in Object Storage (where it can not be mutated)
    ObjectStoreOnly,
}

impl ChunkStorage {
    /// Return a str representation of this storage state
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::OpenMutableBuffer => "OpenMutableBuffer",
            Self::ClosedMutableBuffer => "ClosedMutableBuffer",
            Self::ReadBuffer => "ReadBuffer",
            Self::ReadBufferAndObjectStore => "ReadBufferAndObjectStore",
            Self::ObjectStoreOnly => "ObjectStoreOnly",
        }
    }
}

/// Any lifecycle action currently in progress for this chunk
#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum ChunkLifecycleAction {
    /// Chunk is in the process of being written to object storage
    Persisting,

    /// Chunk is in the process of being compacted
    Compacting,

    /// Chunk is about to be dropped from memory and (if persisted) from object store
    Dropping,
}

impl std::fmt::Display for ChunkLifecycleAction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name())
    }
}

impl ChunkLifecycleAction {
    pub fn name(&self) -> &'static str {
        match self {
            Self::Persisting => "Persisting to Object Storage",
            Self::Compacting => "Compacting",
            Self::Dropping => "Dropping",
        }
    }
}

/// Represents metadata about the physical storage of a chunk in a
/// database.
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone, Serialize, Deserialize)]
pub struct ChunkSummary {
    /// The partition key of this chunk
    pub partition_key: Arc<str>,

    /// The table of this chunk
    pub table_name: Arc<str>,

    /// Order of this chunk relative to other overlapping chunks.
    pub order: ChunkOrder,

    /// The id of this chunk
    pub id: ChunkId,

    /// How is this chunk stored?
    pub storage: ChunkStorage,

    /// Is there any outstanding lifecycle action for this chunk?
    pub lifecycle_action: Option<ChunkLifecycleAction>,

    /// The number of bytes used to store this chunk in memory
    pub memory_bytes: usize,

    /// The number of bytes used to store this chunk in object storage
    pub object_store_bytes: usize,

    /// The total number of rows in this chunk
    pub row_count: usize,

    /// The time at which the chunk data was accessed, by a query or a write
    pub time_of_last_access: Option<DateTime<Utc>>,

    /// The earliest time at which data contained within this chunk was written
    /// into IOx. Note due to the compaction, etc... this may not be the chunk
    /// that data was originally written into
    pub time_of_first_write: DateTime<Utc>,

    /// The latest time at which data contained within this chunk was written
    /// into IOx. Note due to the compaction, etc... this may not be the chunk
    /// that data was originally written into
    pub time_of_last_write: DateTime<Utc>,

    /// Time at which this chunk was marked as closed. Note this is
    /// not the same as the timestamps on the data itself
    pub time_closed: Option<DateTime<Utc>>,
}

/// Represents metadata about the physical storage of a column in a chunk
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct ChunkColumnSummary {
    /// Column name
    pub name: Arc<str>,

    /// Estimated size, in bytes, consumed by this column.
    pub memory_bytes: usize,
}

/// Contains additional per-column details about physical storage of a chunk
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct DetailedChunkSummary {
    /// Overall chunk statistic
    pub inner: ChunkSummary,

    /// Per column breakdown
    pub columns: Vec<ChunkColumnSummary>,
}

impl ChunkSummary {
    pub fn equal_without_timestamps(&self, other: &Self) -> bool {
        self.partition_key == other.partition_key
            && self.table_name == other.table_name
            && self.id == other.id
            && self.storage == other.storage
            && self.lifecycle_action == other.lifecycle_action
            && self.memory_bytes == other.memory_bytes
            && self.object_store_bytes == other.object_store_bytes
            && self.row_count == other.row_count
    }
}

/// ID of a chunk.
///
/// This ID is unique within a single partition.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ChunkId(u32);

impl ChunkId {
    pub const MAX: Self = Self(u32::MAX);

    pub fn new(id: u32) -> Self {
        Self(id)
    }

    pub fn get(&self) -> u32 {
        self.0
    }

    /// Get next chunk ID.
    ///
    /// # Panic
    /// Panics if `self` is already [max](Self::MAX).
    pub fn next(&self) -> Self {
        Self(self.0.checked_add(1).expect("chunk ID overflow"))
    }
}

impl std::fmt::Display for ChunkId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("ChunkId").field(&self.0).finish()
    }
}

/// Order of a chunk.
///
/// This is used for:
/// 1. **upsert order:** chunks with higher order overwrite data in chunks with lower order
/// 2. **locking order:** chunks must be locked in consistent (ascending) order
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ChunkOrder(NonZeroU32);

impl ChunkOrder {
    // TODO: remove `unsafe` once https://github.com/rust-lang/rust/issues/51999 is fixed
    pub const MIN: Self = Self(unsafe { NonZeroU32::new_unchecked(1) });
    pub const MAX: Self = Self(unsafe { NonZeroU32::new_unchecked(u32::MAX) });

    pub fn new(order: u32) -> Option<Self> {
        NonZeroU32::new(order).map(Self)
    }

    pub fn get(&self) -> u32 {
        self.0.get()
    }

    /// Get next chunk order.
    ///
    /// # Panic
    /// Panics if `self` is already [max](Self::MAX).
    pub fn next(&self) -> Self {
        Self(
            NonZeroU32::new(self.0.get().checked_add(1).expect("chunk order overflow"))
                .expect("did not overflow, so cannot be zero"),
        )
    }
}

impl std::fmt::Display for ChunkOrder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("ChunkOrder").field(&self.0.get()).finish()
    }
}
