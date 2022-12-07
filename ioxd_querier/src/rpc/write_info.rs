//! WriteInfoService gRPC implementation

use generated_types::influxdata::iox::ingester::v1::{
    self as proto,
    write_info_service_server::{WriteInfoService, WriteInfoServiceServer},
};
use querier::QuerierDatabase;
use std::sync::Arc;

/// Acquire a [`WriteInfoService`] gRPC service implementation.
pub fn write_info_service(
    server: Arc<QuerierDatabase>,
) -> WriteInfoServiceServer<impl WriteInfoService> {
    WriteInfoServiceServer::new(QuerierWriteInfoServiceImpl::new(server))
}

#[derive(Debug)]
struct QuerierWriteInfoServiceImpl {
    server: Arc<QuerierDatabase>,
}

impl QuerierWriteInfoServiceImpl {
    pub fn new(server: Arc<QuerierDatabase>) -> Self {
        Self { server }
    }
}

#[tonic::async_trait]
impl WriteInfoService for QuerierWriteInfoServiceImpl {
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
