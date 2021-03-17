use generated_types::google::{protobuf::Any, FieldViolation, FieldViolationExt};
use generated_types::{
    google::longrunning, influxdata::iox::management::v1 as management, protobuf_type_url_eq,
};
use serde::{Deserialize, Serialize};
use std::convert::TryFrom;

/// Metadata associated with a set of background tasks
/// Used in combination with TrackerRegistry
///
/// TODO: Serde is temporary until prost adds JSON support
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Job {
    Dummy {
        nanos: Vec<u64>,
    },

    /// Persist a WAL segment to object store
    PersistSegment {
        writer_id: u32,
        segment_id: u64,
    },

    /// Move a chunk from mutable buffer to read buffer
    CloseChunk {
        db_name: String,
        partition_key: String,
        chunk_id: u32,
    },
}

impl From<Job> for management::operation_metadata::Job {
    fn from(job: Job) -> Self {
        match job {
            Job::Dummy { nanos } => Self::Dummy(management::Dummy { nanos }),
            Job::PersistSegment {
                writer_id,
                segment_id,
            } => Self::PersistSegment(management::PersistSegment {
                writer_id,
                segment_id,
            }),
            Job::CloseChunk {
                db_name,
                partition_key,
                chunk_id,
            } => Self::CloseChunk(management::CloseChunk {
                db_name,
                partition_key,
                chunk_id,
            }),
        }
    }
}

impl From<management::operation_metadata::Job> for Job {
    fn from(value: management::operation_metadata::Job) -> Self {
        use management::operation_metadata::Job;
        match value {
            Job::Dummy(management::Dummy { nanos }) => Self::Dummy { nanos },
            Job::PersistSegment(management::PersistSegment {
                writer_id,
                segment_id,
            }) => Self::PersistSegment {
                writer_id,
                segment_id,
            },
            Job::CloseChunk(management::CloseChunk {
                db_name,
                partition_key,
                chunk_id,
            }) => Self::CloseChunk {
                db_name,
                partition_key,
                chunk_id,
            },
        }
    }
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
}

impl TryFrom<longrunning::Operation> for Operation {
    type Error = FieldViolation;

    fn try_from(operation: longrunning::Operation) -> Result<Self, Self::Error> {
        let metadata: Any = operation
            .metadata
            .ok_or_else(|| FieldViolation::required("metadata"))?;

        if !protobuf_type_url_eq(&metadata.type_url, management::OPERATION_METADATA) {
            return Err(FieldViolation {
                field: "metadata.type_url".to_string(),
                description: "Unexpected field type".to_string(),
            });
        }

        let meta: management::OperationMetadata =
            prost::Message::decode(metadata.value).field("metadata.value")?;

        Ok(Self {
            id: operation.name.parse().field("name")?,
            task_count: meta.task_count,
            pending_count: meta.pending_count,
            wall_time: std::time::Duration::from_nanos(meta.wall_nanos),
            cpu_time: std::time::Duration::from_nanos(meta.cpu_nanos),
            job: meta.job.map(Into::into),
        })
    }
}
