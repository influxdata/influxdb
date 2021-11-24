use std::fmt::Debug;
use std::sync::Arc;

use data_types::non_empty::NonEmptyString;
use data_types::DatabaseName;
use dml::{DmlDelete, DmlMeta};
use generated_types::google::{FieldViolationExt, FromOptionalField, OptionalField};
use generated_types::influxdata::iox::delete::v1::*;
use server::{connection::ConnectionManager, Server};
use tonic::Response;

struct DeleteService<M: ConnectionManager> {
    server: Arc<Server<M>>,
}

use super::error::{default_db_error_handler, default_server_error_handler};

#[tonic::async_trait]
impl<M> delete_service_server::DeleteService for DeleteService<M>
where
    M: ConnectionManager + Send + Sync + Debug + 'static,
{
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
        let delete = DmlDelete::new(predicate, table_name, meta);

        // Validate that the database name is legit
        let db_name = DatabaseName::new(db_name).scope("db_name")?;
        let db = self
            .server
            .db(&db_name)
            .map_err(default_server_error_handler)?;

        db.store_delete(&delete).map_err(default_db_error_handler)?;

        Ok(Response::new(DeleteResponse {}))
    }
}

pub fn make_server<M>(
    server: Arc<Server<M>>,
) -> delete_service_server::DeleteServiceServer<impl delete_service_server::DeleteService>
where
    M: ConnectionManager + Send + Sync + Debug + 'static,
{
    delete_service_server::DeleteServiceServer::new(DeleteService { server })
}
