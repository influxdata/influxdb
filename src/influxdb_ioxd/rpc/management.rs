use std::convert::TryInto;
use std::fmt::Debug;
use std::sync::Arc;

use tonic::{Request, Response, Status};
use tracing::error;

use data_types::database_rules::DatabaseRules;
use data_types::DatabaseName;
use generated_types::google::{
    AlreadyExists, FieldViolation, FieldViolationExt, InternalError, NotFound,
    PreconditionViolation,
};
use generated_types::influxdata::iox::management::v1::*;
use query::DatabaseStore;
use server::{ConnectionManager, Error, Server};

struct ManagementService<M: ConnectionManager> {
    server: Arc<Server<M>>,
}

fn default_error_handler(error: Error) -> tonic::Status {
    match error {
        Error::IdNotSet => PreconditionViolation {
            category: "Writer ID".to_string(),
            subject: "influxdata.com/iox".to_string(),
            description: "Writer ID must be set".to_string(),
        }
        .into(),
        error => {
            error!(?error, "Unexpected error");
            InternalError {}.into()
        }
    }
}

#[tonic::async_trait]
impl<M> management_service_server::ManagementService for ManagementService<M>
where
    M: ConnectionManager + Send + Sync + Debug + 'static,
{
    async fn get_writer_id(
        &self,
        _: Request<GetWriterIdRequest>,
    ) -> Result<Response<GetWriterIdResponse>, Status> {
        match self.server.require_id().ok() {
            Some(id) => Ok(Response::new(GetWriterIdResponse { id })),
            None => return Err(NotFound::default().into()),
        }
    }

    async fn update_writer_id(
        &self,
        request: Request<UpdateWriterIdRequest>,
    ) -> Result<Response<UpdateWriterIdResponse>, Status> {
        self.server.set_id(request.get_ref().id);
        Ok(Response::new(UpdateWriterIdResponse {}))
    }

    async fn list_databases(
        &self,
        _: Request<ListDatabasesRequest>,
    ) -> Result<Response<ListDatabasesResponse>, Status> {
        let names = self.server.db_names_sorted().await;
        Ok(Response::new(ListDatabasesResponse { names }))
    }

    async fn get_database(
        &self,
        request: Request<GetDatabaseRequest>,
    ) -> Result<Response<GetDatabaseResponse>, Status> {
        let name = DatabaseName::new(request.into_inner().name).field("name")?;

        match self.server.db_rules(&name).await {
            Some(rules) => Ok(Response::new(GetDatabaseResponse {
                rules: Some(rules.into()),
            })),
            None => {
                return Err(NotFound {
                    resource_type: "database".to_string(),
                    resource_name: name.to_string(),
                    ..Default::default()
                }
                .into())
            }
        }
    }

    async fn create_database(
        &self,
        request: Request<CreateDatabaseRequest>,
    ) -> Result<Response<CreateDatabaseResponse>, Status> {
        let rules: DatabaseRules = request
            .into_inner()
            .rules
            .ok_or_else(|| FieldViolation::required(""))
            .and_then(TryInto::try_into)
            .map_err(|e| e.scope("rules"))?;

        let name =
            DatabaseName::new(rules.name.clone()).expect("protobuf mapping didn't validate name");

        match self.server.create_database(name, rules).await {
            Ok(_) => Ok(Response::new(CreateDatabaseResponse {})),
            Err(Error::DatabaseAlreadyExists { db_name }) => {
                return Err(AlreadyExists {
                    resource_type: "database".to_string(),
                    resource_name: db_name,
                    ..Default::default()
                }
                .into())
            }
            Err(e) => Err(default_error_handler(e)),
        }
    }
}

pub fn make_server<M>(
    server: Arc<Server<M>>,
) -> management_service_server::ManagementServiceServer<
    impl management_service_server::ManagementService,
>
where
    M: ConnectionManager + Send + Sync + Debug + 'static,
{
    management_service_server::ManagementServiceServer::new(ManagementService { server })
}
