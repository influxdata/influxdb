use std::sync::Arc;

use crate::{
    chunk_metadata::{ChunkAddr, ChunkId},
    partition_metadata::PartitionAddr,
};

/// Metadata associated with a set of background tasks
/// Used in combination with TrackerRegistry
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum Job {
    Dummy {
        db_name: Option<Arc<str>>,
        nanos: Vec<u64>,
    },

    /// Write a chunk from read buffer to object store
    WriteChunk { chunk: ChunkAddr },

    /// Compact a set of chunks
    CompactChunks {
        partition: PartitionAddr,
        chunks: Vec<ChunkId>,
    },

    /// Split and persist a set of chunks
    PersistChunks {
        partition: PartitionAddr,
        chunks: Vec<ChunkId>,
    },

    /// Drop chunk from memory and (if persisted) from object store.
    DropChunk { chunk: ChunkAddr },

    /// Drop partition from memory and (if persisted) from object store.
    DropPartition { partition: PartitionAddr },

    /// Wipe preserved catalog
    WipePreservedCatalog { db_name: Arc<str> },
}

impl Job {
    /// Returns the database name associated with this job, if any
    pub fn db_name(&self) -> Option<&Arc<str>> {
        match self {
            Self::Dummy { db_name, .. } => db_name.as_ref(),
            Self::WriteChunk { chunk, .. } => Some(&chunk.db_name),
            Self::CompactChunks { partition, .. } => Some(&partition.db_name),
            Self::PersistChunks { partition, .. } => Some(&partition.db_name),
            Self::DropChunk { chunk, .. } => Some(&chunk.db_name),
            Self::DropPartition { partition, .. } => Some(&partition.db_name),
            Self::WipePreservedCatalog { db_name, .. } => Some(db_name),
        }
    }

    /// Returns the partition name associated with this job, if any
    pub fn partition_key(&self) -> Option<&Arc<str>> {
        match self {
            Self::Dummy { .. } => None,
            Self::WriteChunk { chunk, .. } => Some(&chunk.partition_key),
            Self::CompactChunks { partition, .. } => Some(&partition.partition_key),
            Self::PersistChunks { partition, .. } => Some(&partition.partition_key),
            Self::DropChunk { chunk, .. } => Some(&chunk.partition_key),
            Self::DropPartition { partition, .. } => Some(&partition.partition_key),
            Self::WipePreservedCatalog { .. } => None,
        }
    }

    /// Returns the table name associated with this job, if any
    pub fn table_name(&self) -> Option<&Arc<str>> {
        match self {
            Self::Dummy { .. } => None,
            Self::WriteChunk { chunk, .. } => Some(&chunk.table_name),
            Self::CompactChunks { partition, .. } => Some(&partition.table_name),
            Self::PersistChunks { partition, .. } => Some(&partition.table_name),
            Self::DropChunk { chunk, .. } => Some(&chunk.table_name),
            Self::DropPartition { partition, .. } => Some(&partition.table_name),
            Self::WipePreservedCatalog { .. } => None,
        }
    }

    /// Returns the chunk_ids associated with this job, if any
    pub fn chunk_ids(&self) -> Option<Vec<ChunkId>> {
        match self {
            Self::Dummy { .. } => None,
            Self::WriteChunk { chunk, .. } => Some(vec![chunk.chunk_id]),
            Self::CompactChunks { chunks, .. } => Some(chunks.clone()),
            Self::PersistChunks { chunks, .. } => Some(chunks.clone()),
            Self::DropChunk { chunk, .. } => Some(vec![chunk.chunk_id]),
            Self::DropPartition { .. } => None,
            Self::WipePreservedCatalog { .. } => None,
        }
    }

    /// Returns a human readable description associated with this job, if any
    pub fn description(&self) -> &'static str {
        match self {
            Self::Dummy { .. } => "Dummy Job, for testing",
            Self::WriteChunk { .. } => "Writing chunk to Object Storage",
            Self::CompactChunks { .. } => "Compacting chunks to ReadBuffer",
            Self::PersistChunks { .. } => "Persisting chunks to object storage",
            Self::DropChunk { .. } => "Drop chunk from memory and (if persisted) from object store",
            Self::DropPartition { .. } => {
                "Drop partition from memory and (if persisted) from object store"
            }
            Self::WipePreservedCatalog { .. } => "Wipe preserved catalog",
        }
    }
}
