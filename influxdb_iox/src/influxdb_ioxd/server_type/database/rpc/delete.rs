use std::fmt::Debug;
use std::sync::Arc;

use data_types::DatabaseName;
use generated_types::google::FieldViolationExt;
use generated_types::influxdata::iox::delete::v1::*;
use predicate::delete_predicate::parse_delete_predicate;
use server::{connection::ConnectionManager, Error, Server};
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
        let DeleteRequest {
            db_name,
            table_name,
            start_time,
            stop_time,
            predicate,
        } = request.into_inner();

        // Validate that the database name is legit
        let db_name = DatabaseName::new(db_name).scope("db_name")?;
        let db = self
            .server
            .db(&db_name)
            .map_err(default_server_error_handler)?;

        let del_predicate_result = parse_delete_predicate(&start_time, &stop_time, &predicate);
        match del_predicate_result {
            Err(_) => {
                return Err(default_server_error_handler(Error::DeleteExpression {
                    start_time,
                    stop_time,
                    predicate,
                }))
            }
            Ok(del_predicate) => {
                // execute delete
                db.delete(&table_name, Arc::new(del_predicate))
                    .await
                    .map_err(default_db_error_handler)?;
            }
        }

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
