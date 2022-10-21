//! gRPC service implementations for `compactor`.

use crate::handler::{
    CompactorHandler, DeleteSkippedCompactionsError, ListSkippedCompactionsError,
};
use data_types::PartitionId;
use generated_types::influxdata::iox::compactor::v1::{
    self as proto,
    compaction_service_server::{CompactionService, CompactionServiceServer},
};
use std::sync::Arc;
use tonic::{Request, Response};

/// This type is responsible for managing all gRPC services exposed by `compactor`.
#[derive(Debug, Default)]
pub struct GrpcDelegate<I: CompactorHandler> {
    compactor_handler: Arc<I>,
}

impl<I: CompactorHandler + Send + Sync + 'static> GrpcDelegate<I> {
    /// Initialise a new [`GrpcDelegate`] passing valid requests to the specified
    /// `compactor_handler`.
    pub fn new(compactor_handler: Arc<I>) -> Self {
        Self { compactor_handler }
    }

    /// Acquire a Compaction gRPC service implementation.
    pub fn compaction_service(&self) -> CompactionServiceServer<impl CompactionService> {
        CompactionServiceServer::new(CompactionServiceImpl::new(
            Arc::clone(&self.compactor_handler) as _,
        ))
    }
}

/// Implementation of skipped compaction
struct CompactionServiceImpl {
    handler: Arc<dyn CompactorHandler + Send + Sync + 'static>,
}

impl CompactionServiceImpl {
    pub fn new(handler: Arc<dyn CompactorHandler + Send + Sync + 'static>) -> Self {
        Self { handler }
    }
}

impl From<ListSkippedCompactionsError> for tonic::Status {
    /// Logs and converts a result from the business logic into the appropriate tonic status
    fn from(err: ListSkippedCompactionsError) -> Self {
        use ListSkippedCompactionsError::*;

        match err {
            SkippedCompactionLookup(_) => Self::internal(err.to_string()),
        }
    }
}

impl From<DeleteSkippedCompactionsError> for tonic::Status {
    /// Logs and converts a result from the business logic into the appropriate tonic status
    fn from(err: DeleteSkippedCompactionsError) -> Self {
        use DeleteSkippedCompactionsError::*;

        match err {
            SkippedCompactionDelete(_) => Self::internal(err.to_string()),
        }
    }
}

#[tonic::async_trait]
impl CompactionService for CompactionServiceImpl {
    async fn list_skipped_compactions(
        &self,
        _request: Request<proto::ListSkippedCompactionsRequest>,
    ) -> Result<Response<proto::ListSkippedCompactionsResponse>, tonic::Status> {
        let skipped_compactions = self
            .handler
            .skipped_compactions()
            .await?
            .into_iter()
            .map(From::from)
            .collect();

        Ok(tonic::Response::new(
            proto::ListSkippedCompactionsResponse {
                skipped_compactions,
            },
        ))
    }

    async fn delete_skipped_compactions(
        &self,
        request: Request<proto::DeleteSkippedCompactionsRequest>,
    ) -> Result<Response<proto::DeleteSkippedCompactionsResponse>, tonic::Status> {
        let partition_id = request.into_inner().partition_id;
        let partition_id = PartitionId::new(partition_id);

        let skipped_compaction = self
            .handler
            .delete_skipped_compactions(partition_id)
            .await?
            .map(From::from);

        Ok(tonic::Response::new(
            proto::DeleteSkippedCompactionsResponse { skipped_compaction },
        ))
    }
}
