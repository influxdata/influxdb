//! Module contains a representation of chunk metadata
use std::sync::Arc;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

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

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone, Serialize, Deserialize)]
/// Represents metadata about a chunk in a database.
/// A chunk can contain one or more tables.
pub struct ChunkSummary {
    /// The partition key of this chunk
    pub partition_key: Arc<str>,

    /// The table of this chunk
    pub table_name: Arc<str>,

    /// The id of this chunk
    pub id: u32,

    /// How is this chunk stored?
    pub storage: ChunkStorage,

    /// The total estimated size of this chunk, in bytes
    pub estimated_bytes: usize,

    /// The total number of rows in this chunk
    pub row_count: usize,

    /// Time at which the first data was written into this chunk. Note
    /// this is not the same as the timestamps on the data itself
    pub time_of_first_write: Option<DateTime<Utc>>,

    /// Most recent time at which data write was initiated into this
    /// chunk. Note this is not the same as the timestamps on the data
    /// itself
    pub time_of_last_write: Option<DateTime<Utc>>,

    /// Time at which this chunk was marked as closed. Note this is
    /// not the same as the timestamps on the data itself
    pub time_closed: Option<DateTime<Utc>>,
}

impl ChunkSummary {
    /// Construct a ChunkSummary that has None for all timestamps
    pub fn new_without_timestamps(
        partition_key: Arc<str>,
        table_name: Arc<str>,
        id: u32,
        storage: ChunkStorage,
        estimated_bytes: usize,
        row_count: usize,
    ) -> Self {
        Self {
            partition_key,
            table_name,
            id,
            storage,
            estimated_bytes,
            row_count,
            time_of_first_write: None,
            time_of_last_write: None,
            time_closed: None,
        }
    }
}
