//! gRPC service implementations for `compactor`.

use crate::handler::{CompactorHandler, ListSkippedCompactionsError};
use generated_types::influxdata::iox::compactor::v1::{
    self as proto,
    skipped_compaction_service_server::{SkippedCompactionService, SkippedCompactionServiceServer},
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

    /// Acquire a SkippedCompaction gRPC service implementation.
    pub fn skipped_compaction_service(
        &self,
    ) -> SkippedCompactionServiceServer<impl SkippedCompactionService> {
        SkippedCompactionServiceServer::new(SkippedCompactionServiceImpl::new(Arc::clone(
            &self.compactor_handler,
        ) as _))
    }
}

/// Implementation of skipped compaction
struct SkippedCompactionServiceImpl {
    handler: Arc<dyn CompactorHandler + Send + Sync + 'static>,
}

impl SkippedCompactionServiceImpl {
    pub fn new(handler: Arc<dyn CompactorHandler + Send + Sync + 'static>) -> Self {
        Self { handler }
    }
}

impl From<ListSkippedCompactionsError> for tonic::Status {
    /// Logs and converts a result from the business logic into the appropriate tonic status
    fn from(_err: ListSkippedCompactionsError) -> Self {
        Self::unimplemented("Not yet implemented")
    }
}

#[tonic::async_trait]
impl SkippedCompactionService for SkippedCompactionServiceImpl {
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
}
