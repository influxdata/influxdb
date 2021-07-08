use serde::{Deserialize, Serialize};

/// Metadata associated with a set of background tasks
/// Used in combination with TrackerRegistry
///
/// TODO: Serde is temporary until prost adds JSON support
#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
pub enum Job {
    Dummy {
        nanos: Vec<u64>,
    },

    /// Move a chunk from mutable buffer to read buffer
    CloseChunk {
        db_name: String,
        partition_key: String,
        table_name: String,
        chunk_id: u32,
    },

    /// Write a chunk from read buffer to object store
    WriteChunk {
        db_name: String,
        partition_key: String,
        table_name: String,
        chunk_id: u32,
    },

    /// Compact a set of chunks
    CompactChunks {
        db_name: String,
        partition_key: String,
        table_name: String,
        chunks: Vec<u32>,
    },

    /// Split and persist a set of chunks
    PersistChunks {
        db_name: String,
        partition_key: String,
        table_name: String,
        chunks: Vec<u32>,
    },

    /// Wipe preserved catalog
    WipePreservedCatalog {
        db_name: String,
    },
}

impl Job {
    /// Returns the database name assocated with this job, if any
    pub fn db_name(&self) -> Option<&str> {
        match self {
            Self::Dummy { .. } => None,
            Self::CloseChunk { db_name, .. } => Some(db_name),
            Self::WriteChunk { db_name, .. } => Some(db_name),
            Self::CompactChunks { db_name, .. } => Some(db_name),
            Self::PersistChunks { db_name, .. } => Some(db_name),
            Self::WipePreservedCatalog { db_name, .. } => Some(db_name),
        }
    }

    /// Returns the partition name assocated with this job, if any
    pub fn partition_key(&self) -> Option<&str> {
        match self {
            Self::Dummy { .. } => None,
            Self::CloseChunk { partition_key, .. } => Some(partition_key),
            Self::WriteChunk { partition_key, .. } => Some(partition_key),
            Self::CompactChunks { partition_key, .. } => Some(partition_key),
            Self::PersistChunks { partition_key, .. } => Some(partition_key),
            Self::WipePreservedCatalog { .. } => None,
        }
    }

    /// Returns the chunk_id associated with this job, if any
    pub fn chunk_id(&self) -> Option<u32> {
        match self {
            Self::Dummy { .. } => None,
            Self::CloseChunk { chunk_id, .. } => Some(*chunk_id),
            Self::WriteChunk { chunk_id, .. } => Some(*chunk_id),
            Self::CompactChunks { .. } => None,
            Self::PersistChunks { .. } => None,
            Self::WipePreservedCatalog { .. } => None,
        }
    }

    /// Returns a human readable description assocated with this job, if any
    pub fn description(&self) -> &str {
        match self {
            Self::Dummy { .. } => "Dummy Job, for testing",
            Self::CloseChunk { .. } => "Loading chunk to ReadBuffer",
            Self::WriteChunk { .. } => "Writing chunk to Object Storage",
            Self::CompactChunks { .. } => "Compacting chunks to ReadBuffer",
            Self::PersistChunks { .. } => "Persisting chunks to object storage",
            Self::WipePreservedCatalog { .. } => "Wipe preserved catalog",
        }
    }
}

/// The status of a running operation
#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
pub enum OperationStatus {
    /// A task associated with the operation is running
    Running,
    /// All tasks associated with the operation have finished
    ///
    /// Note: This does not indicate success or failure only that
    /// no tasks associated with the operation are running
    Complete,
    /// The operation was cancelled and no associated tasks are running
    Cancelled,
    /// An operation error was returned
    ///
    /// Note: The tracker system currently will never return this
    Errored,
}

/// A group of asynchronous tasks being performed by an IOx server
///
/// TODO: Temporary until prost adds JSON support
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Operation {
    /// ID of the running operation
    pub id: usize,
    /// Number of subtasks for this operation
    pub task_count: u64,
    /// Number of pending tasks for this operation
    pub pending_count: u64,
    /// Wall time spent executing this operation
    pub wall_time: std::time::Duration,
    /// CPU time spent executing this operation
    pub cpu_time: std::time::Duration,
    /// Additional job metadata
    pub job: Option<Job>,
    /// The status of the running operation
    pub status: OperationStatus,
}
