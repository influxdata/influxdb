use crate::google::{longrunning, protobuf::Any, FieldViolation, FieldViolationExt};
use crate::influxdata::iox::management::v1 as management;
use crate::protobuf_type_url_eq;
use data_types::chunk_metadata::ChunkAddr;
use data_types::job::{Job, OperationStatus};
use data_types::partition_metadata::{DeleteInfo, PartitionAddr};
use std::convert::TryFrom;
use std::sync::Arc;

impl From<Job> for management::operation_metadata::Job {
    fn from(job: Job) -> Self {
        match job {
            Job::Dummy { nanos, db_name } => Self::Dummy(management::Dummy {
                nanos,
                db_name: db_name.map(|x| x.to_string()).unwrap_or_default(),
            }),
            Job::CompactChunk { chunk } => Self::CloseChunk(management::CloseChunk {
                db_name: chunk.db_name.to_string(),
                partition_key: chunk.partition_key.to_string(),
                table_name: chunk.table_name.to_string(),
                chunk_id: chunk.chunk_id,
            }),
            Job::WriteChunk { chunk } => Self::WriteChunk(management::WriteChunk {
                db_name: chunk.db_name.to_string(),
                partition_key: chunk.partition_key.to_string(),
                table_name: chunk.table_name.to_string(),
                chunk_id: chunk.chunk_id,
            }),
            Job::WipePreservedCatalog { db_name } => {
                Self::WipePreservedCatalog(management::WipePreservedCatalog {
                    db_name: db_name.to_string(),
                })
            }
            Job::CompactChunks { partition, chunks } => {
                Self::CompactChunks(management::CompactChunks {
                    db_name: partition.db_name.to_string(),
                    partition_key: partition.partition_key.to_string(),
                    table_name: partition.table_name.to_string(),
                    chunks,
                })
            }
            Job::PersistChunks { partition, chunks } => {
                Self::PersistChunks(management::PersistChunks {
                    db_name: partition.db_name.to_string(),
                    partition_key: partition.partition_key.to_string(),
                    table_name: partition.table_name.to_string(),
                    chunks,
                })
            }
            Job::DropChunk { chunk } => Self::DropChunk(management::DropChunk {
                db_name: chunk.db_name.to_string(),
                partition_key: chunk.partition_key.to_string(),
                table_name: chunk.table_name.to_string(),
                chunk_id: chunk.chunk_id,
            }),
            Job::DropPartition { partition } => Self::DropPartition(management::DropPartition {
                db_name: partition.db_name.to_string(),
                partition_key: partition.partition_key.to_string(),
                table_name: partition.table_name.to_string(),
            }),
            Job::Delete { delete_info } => Self::Delete(management::Delete {
                db_name: delete_info.db_name.to_string(),
                table_name: delete_info.table_name.to_string(),
                delete_predicate: delete_info.delete_predicate.to_string(),
            }),
        }
    }
}

impl From<management::operation_metadata::Job> for Job {
    fn from(value: management::operation_metadata::Job) -> Self {
        use management::operation_metadata::Job;
        match value {
            Job::Dummy(management::Dummy { nanos, db_name }) => Self::Dummy {
                nanos,
                db_name: (!db_name.is_empty()).then(|| Arc::from(db_name.as_str())),
            },
            Job::CloseChunk(management::CloseChunk {
                db_name,
                partition_key,
                table_name,
                chunk_id,
            }) => Self::CompactChunk {
                chunk: ChunkAddr {
                    db_name: Arc::from(db_name.as_str()),
                    table_name: Arc::from(table_name.as_str()),
                    partition_key: Arc::from(partition_key.as_str()),
                    chunk_id,
                },
            },
            Job::WriteChunk(management::WriteChunk {
                db_name,
                partition_key,
                table_name,
                chunk_id,
            }) => Self::WriteChunk {
                chunk: ChunkAddr {
                    db_name: Arc::from(db_name.as_str()),
                    table_name: Arc::from(table_name.as_str()),
                    partition_key: Arc::from(partition_key.as_str()),
                    chunk_id,
                },
            },
            Job::WipePreservedCatalog(management::WipePreservedCatalog { db_name }) => {
                Self::WipePreservedCatalog {
                    db_name: Arc::from(db_name.as_str()),
                }
            }
            Job::CompactChunks(management::CompactChunks {
                db_name,
                partition_key,
                table_name,
                chunks,
            }) => Self::CompactChunks {
                partition: PartitionAddr {
                    db_name: Arc::from(db_name.as_str()),
                    table_name: Arc::from(table_name.as_str()),
                    partition_key: Arc::from(partition_key.as_str()),
                },
                chunks,
            },
            Job::PersistChunks(management::PersistChunks {
                db_name,
                partition_key,
                table_name,
                chunks,
            }) => Self::PersistChunks {
                partition: PartitionAddr {
                    db_name: Arc::from(db_name.as_str()),
                    table_name: Arc::from(table_name.as_str()),
                    partition_key: Arc::from(partition_key.as_str()),
                },
                chunks,
            },
            Job::DropChunk(management::DropChunk {
                db_name,
                partition_key,
                table_name,
                chunk_id,
            }) => Self::DropChunk {
                chunk: ChunkAddr {
                    db_name: Arc::from(db_name.as_str()),
                    table_name: Arc::from(table_name.as_str()),
                    partition_key: Arc::from(partition_key.as_str()),
                    chunk_id,
                },
            },
            Job::DropPartition(management::DropPartition {
                db_name,
                partition_key,
                table_name,
            }) => Self::DropPartition {
                partition: PartitionAddr {
                    db_name: Arc::from(db_name.as_str()),
                    table_name: Arc::from(table_name.as_str()),
                    partition_key: Arc::from(partition_key.as_str()),
                },
            },
            Job::Delete(management::Delete {
                db_name,
                table_name,
                delete_predicate,
            }) => Self::Delete {
                delete_info: DeleteInfo {
                    db_name: Arc::from(db_name.as_str()),
                    table_name: Arc::from(table_name.as_str()),
                    delete_predicate: Arc::from(delete_predicate.as_str()),
                },
            },
        }
    }
}

impl TryFrom<longrunning::Operation> for data_types::job::Operation {
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

        let status = match &operation.result {
            None => OperationStatus::Running,
            Some(longrunning::operation::Result::Response(_)) => OperationStatus::Success,
            Some(longrunning::operation::Result::Error(status)) => {
                if status.code == tonic::Code::Cancelled as i32 {
                    OperationStatus::Cancelled
                } else {
                    OperationStatus::Errored
                }
            }
        };

        Ok(Self {
            id: operation.name.parse().field("name")?,
            total_count: meta.total_count,
            pending_count: meta.pending_count,
            success_count: meta.success_count,
            error_count: meta.error_count,
            cancelled_count: meta.cancelled_count,
            dropped_count: meta.dropped_count,
            wall_time: std::time::Duration::from_nanos(meta.wall_nanos),
            cpu_time: std::time::Duration::from_nanos(meta.cpu_nanos),
            job: meta.job.map(Into::into),
            status,
        })
    }
}
