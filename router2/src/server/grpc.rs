//! gRPC service implementations for `router2`.

use generated_types::influxdata::pbdata::v1::*;
use tonic::{Request, Response, Status};

/// This type is responsible for managing all gRPC services exposed by
/// `router2`.
#[derive(Debug, Default)]
pub struct GrpcDelegate;

impl GrpcDelegate {
    /// Acquire a [`WriteService`] gRPC service implementation.
    ///
    /// [`WriteService`]: generated_types::influxdata::pbdata::v1::write_service_server::WriteService.
    pub fn write_service(
        &self,
    ) -> write_service_server::WriteServiceServer<impl write_service_server::WriteService> {
        write_service_server::WriteServiceServer::new(WriteService::default())
    }
}

#[derive(Debug, Default)]
struct WriteService;

#[tonic::async_trait]
impl write_service_server::WriteService for WriteService {
    /// Receive a gRPC [`WriteRequest`] and dispatch it to the DML handler.
    async fn write(
        &self,
        _request: Request<WriteRequest>,
    ) -> Result<Response<WriteResponse>, Status> {
        Err(Status::unimplemented("not implemented"))
    }
}
