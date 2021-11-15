use data_types::DatabaseName;
use dml::{DmlMeta, DmlWrite};
use generated_types::google::{FieldViolation, FieldViolationExt};
use generated_types::influxdata::pbdata::v1::*;
use server::{connection::ConnectionManager, Server};
use std::fmt::Debug;
use std::sync::Arc;

use super::error::default_server_error_handler;

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
        let span_ctx = request.extensions().get().cloned();
        let database_batch = request
            .into_inner()
            .database_batch
            .ok_or_else(|| FieldViolation::required("database_batch"))?;

        let db_name = DatabaseName::new(&database_batch.database_name)
            .scope("database_batch.database_name")?;

        let tables =
            mutable_batch_pb::decode::decode_database_batch(&database_batch).map_err(|e| {
                FieldViolation {
                    field: "database_batch".into(),
                    description: format!("Invalid DatabaseBatch: {}", e),
                }
            })?;

        let write = DmlWrite::new(tables, DmlMeta::unsequenced(span_ctx));

        self.server
            .write(&db_name, write)
            .await
            .map_err(default_server_error_handler)?;

        Ok(tonic::Response::new(WriteResponse {}))
    }
}

pub fn make_server<M>(
    server: Arc<Server<M>>,
) -> write_service_server::WriteServiceServer<impl write_service_server::WriteService>
where
    M: ConnectionManager + Send + Sync + Debug + 'static,
{
    write_service_server::WriteServiceServer::new(PBWriteService { server })
}
