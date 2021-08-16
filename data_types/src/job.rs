use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::chunk_metadata::ChunkAddr;
use crate::partition_metadata::PartitionAddr;

/// Metadata associated with a set of background tasks
/// Used in combination with TrackerRegistry
///
/// TODO: Serde is temporary until prost adds JSON support
#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
pub enum Job {
    Dummy {
        db_name: Option<Arc<str>>,
        nanos: Vec<u64>,
    },

    /// Move a chunk from mutable buffer to read buffer
    CompactChunk { chunk: ChunkAddr },

    /// Write a chunk from read buffer to object store
    WriteChunk { chunk: ChunkAddr },

    /// Compact a set of chunks
    CompactChunks {
        partition: PartitionAddr,
        chunks: Vec<u32>,
    },

    /// Split and persist a set of chunks
    PersistChunks {
        partition: PartitionAddr,
        chunks: Vec<u32>,
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
            Self::CompactChunk { chunk, .. } => Some(&chunk.db_name),
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
            Self::CompactChunk { chunk, .. } => Some(&chunk.partition_key),
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
            Self::CompactChunk { chunk, .. } => Some(&chunk.table_name),
            Self::WriteChunk { chunk, .. } => Some(&chunk.table_name),
            Self::CompactChunks { partition, .. } => Some(&partition.table_name),
            Self::PersistChunks { partition, .. } => Some(&partition.table_name),
            Self::DropChunk { chunk, .. } => Some(&chunk.table_name),
            Self::DropPartition { partition, .. } => Some(&partition.table_name),
            Self::WipePreservedCatalog { .. } => None,
        }
    }

    /// Returns the chunk_ids associated with this job, if any
    pub fn chunk_ids(&self) -> Option<Vec<u32>> {
        match self {
            Self::Dummy { .. } => None,
            Self::CompactChunk { chunk, .. } => Some(vec![chunk.chunk_id]),
            Self::WriteChunk { chunk, .. } => Some(vec![chunk.chunk_id]),
            Self::CompactChunks { chunks, .. } => Some(chunks.clone()),
            Self::PersistChunks { chunks, .. } => Some(chunks.clone()),
            Self::DropChunk { chunk, .. } => Some(vec![chunk.chunk_id]),
            Self::DropPartition { .. } => None,
            Self::WipePreservedCatalog { .. } => None,
        }
    }

    /// Returns a human readable description associated with this job, if any
    pub fn description(&self) -> &str {
        match self {
            Self::Dummy { .. } => "Dummy Job, for testing",
            Self::CompactChunk { .. } => "Compacting chunk to ReadBuffer",
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

/// The status of a running operation
#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
pub enum OperationStatus {
    /// A task associated with the operation is running
    Running,
    /// All tasks associated with the operation have finished successfully
    Success,
    /// The operation was cancelled and no associated tasks are running
    Cancelled,
    /// An operation error was returned
    Errored,
}

/// A group of asynchronous tasks being performed by an IOx server
///
/// TODO: Temporary until prost adds JSON support
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Operation {
    /// ID of the running operation
    pub id: usize,
    // The total number of created tasks
    pub total_count: u64,
    // The number of pending tasks
    pub pending_count: u64,
    // The number of tasks that completed successfully
    pub success_count: u64,
    // The number of tasks that returned an error
    pub error_count: u64,
    // The number of tasks that were cancelled
    pub cancelled_count: u64,
    // The number of tasks that did not run to completion (e.g. panic)
    pub dropped_count: u64,
    /// Wall time spent executing this operation
    pub wall_time: std::time::Duration,
    /// CPU time spent executing this operation
    pub cpu_time: std::time::Duration,
    /// Additional job metadata
    pub job: Option<Job>,
    /// The status of the running operation
    pub status: OperationStatus,
}
