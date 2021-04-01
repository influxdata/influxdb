//! The catalog representation of a Partition

use std::{collections::BTreeMap, sync::Arc};

use super::{
    chunk::{Chunk, ChunkState},
    Result, UnknownChunk,
};
use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use snafu::OptionExt;

/// IOx Catalog Partition
///
/// A partition contains multiple Chunks.
#[derive(Debug)]
pub struct Partition {
    /// The partition key
    key: String,

    /// What the next chunk id is
    next_chunk_id: u32,

    /// The chunks that make up this partition, indexed by id
    chunks: BTreeMap<u32, Arc<RwLock<Chunk>>>,

    /// When this partition was created
    created_at: DateTime<Utc>,

    /// the last time at which write was made to this
    /// partition. Partition::new initializes this to now.
    last_write_at: DateTime<Utc>,
}

impl Partition {
    /// Return the partition_key of this Partition
    pub fn key(&self) -> &str {
        &self.key
    }
}

impl Partition {
    /// Create a new partition catalog object.
    ///
    /// This function is not pub because `Partition`s should be
    /// created using the interfaces on [`Catalog`] and not
    /// instantiated directly.
    pub(crate) fn new(key: impl Into<String>) -> Self {
        let key = key.into();

        let now = Utc::now();
        Self {
            key,
            next_chunk_id: 0,
            chunks: BTreeMap::new(),
            created_at: now,
            last_write_at: now,
        }
    }

    /// Update the last write time to now
    pub fn update_last_write_at(&mut self) {
        self.last_write_at = Utc::now();
    }

    /// Return the time at which this partition was created
    pub fn created_at(&self) -> DateTime<Utc> {
        self.created_at
    }

    /// Return the time at which the last write was written to this partititon
    pub fn last_write_at(&self) -> DateTime<Utc> {
        self.last_write_at
    }

    /// Create a new Chunk in the open state
    pub fn create_open_chunk(&mut self) -> Arc<RwLock<Chunk>> {
        let chunk_id = self.next_chunk_id;
        self.next_chunk_id += 1;

        let chunk = Arc::new(RwLock::new(Chunk::new_open(&self.key, chunk_id)));

        if self.chunks.insert(chunk_id, Arc::clone(&chunk)).is_some() {
            // A fundamental invariant has been violated - abort
            panic!("chunk already existed with id {}", chunk_id)
        }

        chunk
    }

    /// Drop the specified chunk
    pub fn drop_chunk(&mut self, chunk_id: u32) -> Result<()> {
        match self.chunks.remove(&chunk_id) {
            Some(_) => Ok(()),
            None => UnknownChunk {
                partition_key: self.key(),
                chunk_id,
            }
            .fail(),
        }
    }

    /// return the first currently open chunk, if any
    pub fn open_chunk(&self) -> Option<Arc<RwLock<Chunk>>> {
        self.chunks
            .values()
            .find(|chunk| {
                let chunk = chunk.read();
                matches!(chunk.state(), ChunkState::Open(_))
            })
            .cloned()
    }

    /// Return an immutable chunk reference by chunk id
    pub fn chunk(&self, chunk_id: u32) -> Result<Arc<RwLock<Chunk>>> {
        self.chunks.get(&chunk_id).cloned().context(UnknownChunk {
            partition_key: self.key(),
            chunk_id,
        })
    }

    pub fn chunk_ids(&self) -> impl Iterator<Item = u32> + '_ {
        self.chunks.keys().cloned()
    }

    /// Return a iterator over chunks in this partition
    pub fn chunks(&self) -> impl Iterator<Item = &Arc<RwLock<Chunk>>> {
        self.chunks.values()
    }
}
