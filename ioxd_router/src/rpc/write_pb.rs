use dml::{DmlMeta, DmlOperation, DmlWrite};
use generated_types::google::{FieldViolation, NotFound, ResourceType};
use generated_types::influxdata::pbdata::v1::*;
use router::server::RouterServer;
use std::sync::Arc;

struct PBWriteService {
    server: Arc<RouterServer>,
}

#[tonic::async_trait]
impl write_service_server::WriteService for PBWriteService {
    async fn write(
        &self,
        request: tonic::Request<WriteRequest>,
    ) -> Result<tonic::Response<WriteResponse>, tonic::Status> {
        let span_ctx = request.extensions().get().cloned();
        let database_batch = request
            .into_inner()
            .database_batch
            .ok_or_else(|| FieldViolation::required("database_batch"))?;

        let tables =
            mutable_batch_pb::decode::decode_database_batch(&database_batch).map_err(|e| {
                FieldViolation {
                    field: "database_batch".into(),
                    description: format!("Invalid DatabaseBatch: {}", e),
                }
            })?;

        let write = DmlOperation::Write(DmlWrite::new(
            &database_batch.database_name,
            tables,
            DmlMeta::unsequenced(span_ctx),
        ));

        let router = self
            .server
            .router(&database_batch.database_name)
            .ok_or_else(|| NotFound::new(ResourceType::Router, database_batch.database_name))?;

        router
            .write(write)
            .await
            .map_err::<tonic::Status, _>(|e| tonic::Status::aborted(e.to_string()))?;

        Ok(tonic::Response::new(WriteResponse {}))
    }
}

pub fn make_server(
    server: Arc<RouterServer>,
) -> write_service_server::WriteServiceServer<impl write_service_server::WriteService> {
    write_service_server::WriteServiceServer::new(PBWriteService { server })
}
