use std::sync::Arc;

use data_types::sequence_number_set::SequenceNumberSet;
use data_types::{NamespaceId, PartitionId, PartitionKey, SequenceNumber, TableId};
use generated_types::influxdata::iox::ingester::v1::{
    self as proto, replication_service_server::ReplicationService,
};
use mutable_batch::writer;
use mutable_batch_pb::decode::decode_database_batch;
use observability_deps::tracing::*;
use thiserror::Error;
use tonic::{Code, Request, Response};
use uuid::Uuid;

use crate::{BufferError, ReplicationBuffer};

/// A list of error states when handling a ReplicationService request.
#[derive(Debug, Error)]
enum ReplicationError {
    /// The replication request did not contain a write payload.
    #[error("replication request does not contain a payload")]
    NoPayload,

    /// The replication payload contains no tables.
    #[error("replication request does not contain any table data")]
    NoTables,

    /// The replication request didn't contain an ingester id
    #[error("replication request does not contain an ingester id")]
    NoIngesterId,

    /// The replication request had an invalid sequence number set
    #[error("replication request to persist contained invalid sequence number set {0}")]
    InvalidSequenceNumberSet(String),

    /// Ingester ID not a valid UUID
    #[error("replication request does not contain valid ingester uuid")]
    InvalidIngesterId(#[from] uuid::Error),

    /// The serialised write payload could not be read.
    #[error(transparent)]
    Decode(mutable_batch_pb::decode::Error),

    /// An error buffering the write or persist
    #[error("error buffering replciation request: {0}")]
    Buffer(#[from] BufferError),
}

impl From<ReplicationError> for tonic::Status {
    fn from(e: ReplicationError) -> Self {
        let code = match e {
            ReplicationError::Decode(_)
            | ReplicationError::NoPayload
            | ReplicationError::NoTables
            | ReplicationError::NoIngesterId
            | ReplicationError::InvalidIngesterId(_)
            | ReplicationError::InvalidSequenceNumberSet(_) => Code::InvalidArgument,
            ReplicationError::Buffer(_) => Code::Internal,
        };

        Self::new(code, e.to_string())
    }
}

/// Convert a [`BufferError`] returned by the configured [`ReplicationBuffer`] to a
/// [`tonic::Status`].
impl From<BufferError> for tonic::Status {
    fn from(e: BufferError) -> Self {
        match e {
            BufferError::MutableBatch(e) => map_write_error(e),
        }
    }
}

/// Map a [`mutable_batch::Error`] to a [`tonic::Status`].
///
/// This method takes care to enumerate all possible error states, so that new
/// error additions cause a compilation failure, and therefore require the new
/// error to be explicitly mapped to a gRPC status code.
fn map_write_error(e: mutable_batch::Error) -> tonic::Status {
    use tonic::Status;
    match e {
        mutable_batch::Error::ColumnError { .. }
        | mutable_batch::Error::ArrowError { .. }
        | mutable_batch::Error::InternalSchema { .. }
        | mutable_batch::Error::ColumnNotFound { .. }
        | mutable_batch::Error::WriterError {
            source: writer::Error::KeyNotFound { .. } | writer::Error::InsufficientValues { .. },
        } => Status::internal(e.to_string()),
        mutable_batch::Error::WriterError {
            source: writer::Error::TypeMismatch { .. },
        } => {
            // While a schema type conflict is ultimately a user error, if it
            // reaches the ingester it should have already passed through schema
            // validation in the router, and as such it is an internal system
            // failure.
            Status::internal(e.to_string())
        }
    }
}

/// A gRPC [`ReplicationService`] handler.
///
/// This handler accepts writes from an upstream, and applies them to the
/// provided [`ReplicationBuffer`].
pub(crate) struct ReplicationServer<B: ReplicationBuffer + 'static> {
    buffer: Arc<B>,
}

impl<B: ReplicationBuffer + 'static> ReplicationServer<B> {
    /// Instantiate a new [`ReplicationServer`]
    pub(crate) fn new(buffer: Arc<B>) -> Self {
        Self { buffer }
    }
}

#[tonic::async_trait]
impl<B: ReplicationBuffer + 'static> ReplicationService for ReplicationServer<B> {
    /// Handle an RPC write request.
    async fn replicate(
        &self,
        request: Request<proto::ReplicateRequest>,
    ) -> Result<Response<proto::ReplicateResponse>, tonic::Status> {
        // Extract the remote address for debugging.
        let remote_addr = request
            .remote_addr()
            .map(|v| v.to_string())
            .unwrap_or_else(|| "<unknown>".to_string());

        let request = request.into_inner();
        let ingester_id =
            Uuid::parse_str(&request.ingester_uuid).map_err(ReplicationError::InvalidIngesterId)?;

        // Extract the database batch payload
        let payload = request.payload.ok_or(ReplicationError::NoPayload)?;

        let batches = decode_database_batch(&payload).map_err(ReplicationError::Decode)?;
        let num_tables = batches.len();
        let sequence_number = SequenceNumber::new(request.sequence_number);
        let namespace_id = NamespaceId::new(payload.database_id);
        let partition_key = PartitionKey::from(payload.partition_key);

        if num_tables == 0 {
            return Err(ReplicationError::NoTables)?;
        }

        trace!(
            remote_addr,
            %ingester_id,
            ?sequence_number,
            num_tables,
            %namespace_id,
            %partition_key,
            "received replicate write"
        );

        match self
            .buffer
            .apply_write(namespace_id, batches, ingester_id, sequence_number)
            .await
        {
            Ok(()) => {}
            Err(e) => {
                error!(error=%e, "failed to write into buffer");
                return Err(ReplicationError::Buffer(e))?;
            }
        }

        Ok(Response::new(proto::ReplicateResponse {}))
    }

    async fn persist_complete(
        &self,
        request: Request<proto::PersistCompleteRequest>,
    ) -> Result<Response<proto::PersistCompleteResponse>, tonic::Status> {
        // Extract the remote address for debugging.
        let remote_addr = request
            .remote_addr()
            .map(|v| v.to_string())
            .unwrap_or_else(|| "<unknown>".to_string());

        let request = request.into_inner();
        let ingester_id =
            Uuid::parse_str(&request.ingester_uuid).map_err(ReplicationError::InvalidIngesterId)?;
        let namespace_id = NamespaceId::new(request.namespace_id);
        let table_id = TableId::new(request.table_id);
        let partition_id = PartitionId::new(request.partition_id);
        let sequence_set =
            SequenceNumberSet::try_from(request.croaring_sequence_number_bitmap.as_ref())
                .map_err(ReplicationError::InvalidSequenceNumberSet)?;

        trace!(
            remote_addr,
            ?ingester_id,
            ?namespace_id,
            ?table_id,
            ?partition_id,
        );

        match self
            .buffer
            .apply_persist(
                ingester_id,
                namespace_id,
                table_id,
                partition_id,
                sequence_set,
            )
            .await
        {
            Ok(()) => {}
            Err(e) => {
                error!(error=%e, "failed to apply persist to buffer");
                return Err(ReplicationError::Buffer(e))?;
            }
        }

        Ok(Response::new(proto::PersistCompleteResponse {}))
    }
}

#[cfg(test)]
mod tests {}
