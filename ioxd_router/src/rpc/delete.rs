use std::sync::Arc;

use data_types::non_empty::NonEmptyString;
use dml::{DmlDelete, DmlMeta, DmlOperation};
use generated_types::google::{FromOptionalField, NotFound, OptionalField, ResourceType};
use generated_types::influxdata::iox::delete::v1::*;
use router::server::RouterServer;
use tonic::Response;

struct DeleteService {
    server: Arc<RouterServer>,
}

#[tonic::async_trait]
impl delete_service_server::DeleteService for DeleteService {
    async fn delete(
        &self,
        request: tonic::Request<DeleteRequest>,
    ) -> Result<tonic::Response<DeleteResponse>, tonic::Status> {
        let span_ctx = request.extensions().get().cloned();
        let DeleteRequest { payload } = request.into_inner();
        let DeletePayload {
            db_name,
            table_name,
            predicate,
        } = payload.unwrap_field("payload")?;
        let predicate = predicate.required("predicate")?;

        let table_name = NonEmptyString::new(table_name);
        let meta = DmlMeta::unsequenced(span_ctx);
        let op = DmlOperation::Delete(DmlDelete::new(&db_name, predicate, table_name, meta));

        let router = self
            .server
            .router(&db_name)
            .ok_or_else(|| NotFound::new(ResourceType::Router, db_name))?;

        router
            .write(op)
            .await
            .map_err(|e| tonic::Status::internal(e.to_string()))?;

        Ok(Response::new(DeleteResponse {}))
    }
}

pub fn make_server(
    server: Arc<RouterServer>,
) -> delete_service_server::DeleteServiceServer<impl delete_service_server::DeleteService> {
    delete_service_server::DeleteServiceServer::new(DeleteService { server })
}
