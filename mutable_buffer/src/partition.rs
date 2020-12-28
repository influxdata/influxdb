//! Holds one or more Chunks.

use generated_types::wal as wb;
use std::sync::Arc;

use crate::chunk::{Chunk, Error as ChunkError};

use snafu::{ResultExt, Snafu};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "Error writing to active chunk of partition with key '{}': {}",
        partition_key,
        source
    ))]
    WritingChunkData {
        partition_key: String,
        source: ChunkError,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug)]
pub struct Partition {
    /// The partition key that is shared by all Chunks in this Partition
    key: String,

    /// The active mutable Chunk; All new writes go to this chunk
    mutable_chunk: Chunk,

    /// Immutable chunks which can no longer be written
    immutable_chunks: Vec<Arc<Chunk>>,
}

impl Partition {
    pub fn new(key: impl Into<String>) -> Self {
        let key: String = key.into();
        let mutable_chunk = Chunk::new(&key);
        Self {
            key,
            mutable_chunk,
            immutable_chunks: Vec::new(),
        }
    }

    /// write data to the active mutable chunk
    pub fn write_entry(&mut self, entry: &wb::WriteBufferEntry<'_>) -> Result<()> {
        assert_eq!(
            entry
                .partition_key()
                .expect("partition key should be present"),
            self.key
        );
        self.mutable_chunk
            .write_entry(entry)
            .with_context(|| WritingChunkData {
                partition_key: entry.partition_key().unwrap(),
            })
    }

    /// roll over the active chunk into the immutable_chunks,
    /// returning the most recently active chunk. Any new writes to
    /// this partition will go to a new chunk
    pub fn rollover_chunk(&mut self) -> Arc<Chunk> {
        todo!();
    }

    pub fn key(&self) -> &str {
        &self.key
    }

    /// Return an iterator over each Chunk in this partition
    pub fn iter(&self) -> ChunkIter<'_> {
        ChunkIter::new(self)
    }
}

pub struct ChunkIter<'a> {
    partition: &'a Partition,
    visited_mutable: bool,
    next_immutable_index: usize,
}

impl<'a> ChunkIter<'a> {
    fn new(partition: &'a Partition) -> Self {
        Self {
            partition,
            visited_mutable: false,
            next_immutable_index: 0,
        }
    }
}

/// Iterates over chunks in a partition
impl<'a> Iterator for ChunkIter<'a> {
    type Item = &'a Chunk;

    fn next(&mut self) -> Option<Self::Item> {
        let partition = self.partition;

        if !self.visited_mutable {
            let chunk = &partition.mutable_chunk;
            self.visited_mutable = true;
            Some(chunk)
        } else if self.next_immutable_index < partition.immutable_chunks.len() {
            let chunk = &self.partition.immutable_chunks[self.next_immutable_index];
            self.next_immutable_index += 1;
            Some(chunk)
        } else {
            None
        }
    }
}
