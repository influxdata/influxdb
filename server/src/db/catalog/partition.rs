//! The catalog representation of a Partition

use std::{
    collections::{btree_map::Entry, BTreeMap},
    sync::Arc,
};

use super::{chunk::Chunk, ChunkAlreadyExists, Result, UnknownChunk};
use parking_lot::RwLock;
use snafu::OptionExt;

/// IOx Catalog Partition
///
/// A partition contains multiple Chunks.
#[derive(Debug, Default)]
pub struct Partition {
    /// The partition key
    key: String,

    /// The chunks that make up this partition, indexed by id
    chunks: BTreeMap<u32, Arc<RwLock<Chunk>>>,
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

        Self {
            key,
            ..Default::default()
        }
    }

    /// Create a new Chunk
    ///
    /// This function is not pub because `Chunks`s should be created
    /// using the interfaces on [`Catalog`] and not instantiated
    /// directly.
    pub fn create_chunk(&mut self, chunk_id: u32) -> Result<Arc<RwLock<Chunk>>> {
        let entry = self.chunks.entry(chunk_id);
        match entry {
            Entry::Vacant(entry) => {
                let chunk = Chunk::new(&self.key, chunk_id);
                let chunk = Arc::new(RwLock::new(chunk));
                entry.insert(Arc::clone(&chunk));
                Ok(chunk)
            }
            Entry::Occupied(_) => ChunkAlreadyExists {
                partition_key: self.key(),
                chunk_id,
            }
            .fail(),
        }
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

    /// Return an immutable chunk reference by chunk id
    pub fn chunk(&self, chunk_id: u32) -> Result<Arc<RwLock<Chunk>>> {
        self.chunks.get(&chunk_id).cloned().context(UnknownChunk {
            partition_key: self.key(),
            chunk_id,
        })
    }

    /// Return a iterator over chunks in this partition
    pub fn chunks(&self) -> impl Iterator<Item = &Arc<RwLock<Chunk>>> {
        self.chunks.values()
    }
}
