//! WriteInfoService gRPC implementation

use generated_types::influxdata::iox::ingester::v1::{
    self as proto,
    write_info_service_server::{WriteInfoService, WriteInfoServiceServer},
};
use querier::Database;
use std::sync::Arc;

/// Acquire a [`WriteInfoService`] gRPC service implementation.
pub fn write_info_service<S: Database + Send + Sync + 'static>(
    server: Arc<S>,
) -> WriteInfoServiceServer<impl WriteInfoService> {
    WriteInfoServiceServer::new(QuerierWriteInfoServiceImpl::new(server))
}

#[derive(Debug)]
struct QuerierWriteInfoServiceImpl<S> {
    server: Arc<S>,
}

impl<S> QuerierWriteInfoServiceImpl<S> {
    pub fn new(server: Arc<S>) -> Self {
        Self { server }
    }
}

#[tonic::async_trait]
impl<S: Database + Send + Sync + 'static> WriteInfoService for QuerierWriteInfoServiceImpl<S> {
    async fn get_write_info(
        &self,
        request: tonic::Request<proto::GetWriteInfoRequest>,
    ) -> Result<tonic::Response<proto::GetWriteInfoResponse>, tonic::Status> {
        let proto::GetWriteInfoRequest { write_token } = request.into_inner();

        let ingester_connection = self
            .server
            .ingester_connection()
            .expect("Ingester connections must be configured to get write info");

        let progresses = ingester_connection
            .get_write_info(&write_token)
            .await
            .map_err(|e| tonic::Status::internal(e.to_string()))?;

        Ok(tonic::Response::new(progresses))
    }
}
