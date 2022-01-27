use data_types::DatabaseName;
use dml::{DmlMeta, DmlOperation, DmlWrite};
use generated_types::google::{FieldViolation, FieldViolationExt};
use generated_types::influxdata::pbdata::v1::*;
use server::Server;
use std::sync::Arc;

use super::error::{default_dml_error_handler, default_server_error_handler};

struct PBWriteService {
    server: Arc<Server>,
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

        let write = DmlWrite::new(
            &database_batch.database_name,
            tables,
            DmlMeta::unsequenced(span_ctx),
        );

        let db_name = DatabaseName::new(&database_batch.database_name)
            .scope("database_batch.database_name")?;

        let db = self
            .server
            .db(&db_name)
            .map_err(default_server_error_handler)?;

        db.store_operation(&DmlOperation::Write(write))
            .map_err(default_dml_error_handler)?;

        Ok(tonic::Response::new(WriteResponse {}))
    }
}

pub fn make_server(
    server: Arc<Server>,
) -> write_service_server::WriteServiceServer<impl write_service_server::WriteService> {
    write_service_server::WriteServiceServer::new(PBWriteService { server })
}
