use super::error::default_server_error_handler;
use generated_types::google::FieldViolation;
use generated_types::influxdata::transfer::column::v1::*;
use server::{ConnectionManager, Server};
use std::fmt::Debug;
use std::sync::Arc;

struct PBWriteService<M: ConnectionManager> {
    server: Arc<Server<M>>,
}

#[tonic::async_trait]
impl<M> write_service_server::WriteService for PBWriteService<M>
where
    M: ConnectionManager + Send + Sync + Debug + 'static,
{
    async fn write(
        &self,
        request: tonic::Request<WriteRequest>,
    ) -> Result<tonic::Response<WriteResponse>, tonic::Status> {
        let database_batch = request
            .into_inner()
            .database_batch
            .ok_or_else(|| FieldViolation::required("database_batch"))?;

        self.server
            .write_pb(database_batch)
            .await
            .map_err(default_server_error_handler)?;

        Ok(tonic::Response::new(WriteResponse {}))
    }
}

pub fn make_server<M>(
    server: Arc<Server<M>>,
    interceptor: impl Into<tonic::Interceptor>,
) -> write_service_server::WriteServiceServer<impl write_service_server::WriteService>
where
    M: ConnectionManager + Send + Sync + Debug + 'static,
{
    write_service_server::WriteServiceServer::with_interceptor(
        PBWriteService { server },
        interceptor,
    )
}
