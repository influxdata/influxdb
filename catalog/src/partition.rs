//! The catalog representation of a Partition

use crate::chunk::Chunk;
use std::collections::{btree_map::Entry, BTreeMap};

use super::{ChunkAlreadyExists, Result, UnknownChunk};
use snafu::OptionExt;

/// IOx Catalog Partition
///
/// A partition contains multiple Chunks.
#[derive(Debug, Default)]
pub struct Partition {
    /// The partition key
    key: String,

    /// The chunks that make up this partition, indexed by id
    chunks: BTreeMap<u32, Chunk>,
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
    pub(crate) fn create_chunk(&mut self, chunk_id: u32) -> Result<()> {
        let entry = self.chunks.entry(chunk_id);
        match entry {
            Entry::Vacant(entry) => {
                entry.insert(Chunk::new(&self.key, chunk_id));
                Ok(())
            }
            Entry::Occupied(_) => ChunkAlreadyExists {
                partition_key: self.key(),
                chunk_id,
            }
            .fail(),
        }
    }

    /// Drop the specified
    ///
    /// This function is not pub because `Chunks`s should be dropped
    /// using the interfaces on [`Catalog`] and not instantiated
    /// directly.
    pub(crate) fn drop_chunk(&mut self, chunk_id: u32) -> Result<()> {
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
    ///
    /// This function is not pub because `Chunks`s should be
    /// accessed using the interfaces on [`Catalog`]
    pub(crate) fn chunk(&self, chunk_id: u32) -> Result<&Chunk> {
        self.chunks.get(&chunk_id).context(UnknownChunk {
            partition_key: self.key(),
            chunk_id,
        })
    }

    /// Return a mutable chunk reference by chunk id
    ///
    /// This function is not pub because `Chunks`s should be
    /// accessed using the interfaces on [`Catalog`]
    pub(crate) fn chunk_mut(&mut self, chunk_id: u32) -> Result<&mut Chunk> {
        self.chunks.get_mut(&chunk_id).context(UnknownChunk {
            partition_key: &self.key,
            chunk_id,
        })
    }

    /// Return a iterator over chunks
    ///
    /// This function is not pub because `Chunks`s should be
    /// accessed using the interfaces on [`Catalog`]
    pub(crate) fn chunks(&self) -> impl Iterator<Item = &Chunk> {
        self.chunks.values()
    }
}
