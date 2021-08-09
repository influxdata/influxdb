use std::convert::{TryFrom, TryInto};
use std::fmt::Debug;
use std::sync::Arc;

use data_types::{database_rules::DatabaseRules, server_id::ServerId, DatabaseName};
use generated_types::google::{
    AlreadyExists, FieldViolation, FieldViolationExt, FromFieldOpt, InternalError, NotFound,
};
use generated_types::influxdata::iox::management::v1::{Error as ProtobufError, *};
use observability_deps::tracing::info;
use query::QueryDatabase;
use server::{ApplicationState, ConnectionManager, DatabaseStore, Error, Server};
use tonic::{Request, Response, Status};

struct ManagementService<M: ConnectionManager> {
    application: Arc<ApplicationState>,
    server: Arc<Server<M>>,
    serving_readiness: ServingReadiness,
}

use super::error::{default_db_error_handler, default_server_error_handler};
use crate::influxdb_ioxd::serving_readiness::ServingReadiness;

#[derive(Debug)]
enum UpdateError {
    Update(server::Error),
    Closure(tonic::Status),
}

impl From<UpdateError> for Status {
    fn from(error: UpdateError) -> Self {
        match error {
            UpdateError::Update(error) => {
                info!(?error, "Update error");
                InternalError {}.into()
            }
            UpdateError::Closure(error) => error,
        }
    }
}

impl From<server::UpdateError<Status>> for UpdateError {
    fn from(error: server::UpdateError<Status>) -> Self {
        match error {
            server::UpdateError::Update(error) => Self::Update(error),
            server::UpdateError::Closure(error) => Self::Closure(error),
        }
    }
}

#[tonic::async_trait]
impl<M> management_service_server::ManagementService for ManagementService<M>
where
    M: ConnectionManager + Send + Sync + Debug + 'static,
{
    async fn get_server_id(
        &self,
        _: Request<GetServerIdRequest>,
    ) -> Result<Response<GetServerIdResponse>, Status> {
        match self.server.server_id() {
            Some(id) => Ok(Response::new(GetServerIdResponse { id: id.get_u32() })),
            None => return Err(NotFound::default().into()),
        }
    }

    async fn update_server_id(
        &self,
        request: Request<UpdateServerIdRequest>,
    ) -> Result<Response<UpdateServerIdResponse>, Status> {
        let id =
            ServerId::try_from(request.get_ref().id).map_err(|_| FieldViolation::required("id"))?;

        match self.server.set_id(id) {
            Ok(_) => Ok(Response::new(UpdateServerIdResponse {})),
            Err(e @ Error::IdAlreadySet) => {
                return Err(FieldViolation {
                    field: "id".to_string(),
                    description: e.to_string(),
                }
                .into())
            }
            Err(e) => Err(default_server_error_handler(e)),
        }
    }

    async fn list_databases(
        &self,
        _: Request<ListDatabasesRequest>,
    ) -> Result<Response<ListDatabasesResponse>, Status> {
        let names = self.server.db_names_sorted();
        Ok(Response::new(ListDatabasesResponse { names }))
    }

    async fn get_database(
        &self,
        request: Request<GetDatabaseRequest>,
    ) -> Result<Response<GetDatabaseResponse>, Status> {
        let name = DatabaseName::new(request.into_inner().name).field("name")?;
        let database = self
            .server
            .database(&name)
            .map_err(default_server_error_handler)?;

        match database.rules() {
            Some(rules) => Ok(Response::new(GetDatabaseResponse {
                rules: Some(rules.as_ref().clone().into()),
            })),
            None => {
                return Err(tonic::Status::unavailable(format!(
                    "Rules have not yet been loaded for database ({})",
                    name
                )))
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

        match self.server.create_database(rules).await {
            Ok(_) => Ok(Response::new(CreateDatabaseResponse {})),
            Err(Error::DatabaseAlreadyExists { db_name }) => {
                return Err(AlreadyExists {
                    resource_type: "database".to_string(),
                    resource_name: db_name,
                    ..Default::default()
                }
                .into())
            }
            Err(e) => Err(default_server_error_handler(e)),
        }
    }

    async fn update_database(
        &self,
        request: Request<UpdateDatabaseRequest>,
    ) -> Result<Response<UpdateDatabaseResponse>, Status> {
        let request = request.into_inner();
        let rules: DatabaseRules = request.rules.required("rules")?;
        let db_name = rules.name.clone();
        let updated_rules = self
            .server
            .update_db_rules(&db_name, |_orig| Ok(rules))
            .await
            .map_err(UpdateError::from)?;
        Ok(Response::new(UpdateDatabaseResponse {
            rules: Some(updated_rules.as_ref().clone().into()),
        }))
    }

    async fn list_chunks(
        &self,
        request: Request<ListChunksRequest>,
    ) -> Result<Response<ListChunksResponse>, Status> {
        let db_name = DatabaseName::new(request.into_inner().db_name).field("db_name")?;
        let db = self
            .server
            .db(&db_name)
            .map_err(default_server_error_handler)?;

        let chunk_summaries = match db.chunk_summaries() {
            Ok(chunk_summaries) => chunk_summaries,
            Err(e) => return Err(default_db_error_handler(e)),
        };

        let chunks: Vec<Chunk> = chunk_summaries
            .into_iter()
            .map(|summary| summary.into())
            .collect();

        Ok(Response::new(ListChunksResponse { chunks }))
    }

    async fn create_dummy_job(
        &self,
        request: Request<CreateDummyJobRequest>,
    ) -> Result<Response<CreateDummyJobResponse>, Status> {
        let request = request.into_inner();
        let tracker = self
            .application
            .job_registry()
            .spawn_dummy_job(request.nanos);
        let operation = Some(super::operations::encode_tracker(tracker)?);
        Ok(Response::new(CreateDummyJobResponse { operation }))
    }

    async fn list_remotes(
        &self,
        _: Request<ListRemotesRequest>,
    ) -> Result<Response<ListRemotesResponse>, Status> {
        let remotes = self
            .server
            .remotes_sorted()
            .into_iter()
            .map(|(id, connection_string)| Remote {
                id: id.get_u32(),
                connection_string,
            })
            .collect();

        Ok(Response::new(ListRemotesResponse { remotes }))
    }

    async fn update_remote(
        &self,
        request: Request<UpdateRemoteRequest>,
    ) -> Result<Response<UpdateRemoteResponse>, Status> {
        let remote = request
            .into_inner()
            .remote
            .ok_or_else(|| FieldViolation::required("remote"))?;
        let remote_id = ServerId::try_from(remote.id)
            .map_err(|_| FieldViolation::required("id").scope("remote"))?;

        self.server
            .update_remote(remote_id, remote.connection_string);

        Ok(Response::new(UpdateRemoteResponse {}))
    }

    async fn delete_remote(
        &self,
        request: Request<DeleteRemoteRequest>,
    ) -> Result<Response<DeleteRemoteResponse>, Status> {
        let request = request.into_inner();
        let remote_id =
            ServerId::try_from(request.id).map_err(|_| FieldViolation::required("id"))?;

        match self.server.delete_remote(remote_id) {
            Some(_) => {}
            None => return Err(NotFound::default().into()),
        }

        Ok(Response::new(DeleteRemoteResponse {}))
    }

    async fn list_partitions(
        &self,
        request: Request<ListPartitionsRequest>,
    ) -> Result<Response<ListPartitionsResponse>, Status> {
        let ListPartitionsRequest { db_name } = request.into_inner();
        let db_name = DatabaseName::new(db_name).field("db_name")?;

        let db = self
            .server
            .db(&db_name)
            .map_err(default_server_error_handler)?;

        let partition_keys = db.partition_keys().map_err(default_db_error_handler)?;
        let partitions = partition_keys
            .into_iter()
            .map(|key| Partition { key })
            .collect::<Vec<_>>();

        Ok(Response::new(ListPartitionsResponse { partitions }))
    }

    async fn get_partition(
        &self,
        request: Request<GetPartitionRequest>,
    ) -> Result<Response<GetPartitionResponse>, Status> {
        let GetPartitionRequest {
            db_name,
            partition_key,
        } = request.into_inner();
        let db_name = DatabaseName::new(db_name).field("db_name")?;
        let db = self
            .server
            .db(&db_name)
            .map_err(default_server_error_handler)?;

        // TODO: get more actual partition details
        let partition_keys = db.partition_keys().map_err(default_db_error_handler)?;

        let partition = if partition_keys.contains(&partition_key) {
            Some(Partition { key: partition_key })
        } else {
            None
        };

        Ok(Response::new(GetPartitionResponse { partition }))
    }

    async fn list_partition_chunks(
        &self,
        request: Request<ListPartitionChunksRequest>,
    ) -> Result<Response<ListPartitionChunksResponse>, Status> {
        let ListPartitionChunksRequest {
            db_name,
            partition_key,
        } = request.into_inner();
        let db_name = DatabaseName::new(db_name).field("db_name")?;
        let db = self
            .server
            .db(&db_name)
            .map_err(default_server_error_handler)?;

        let chunks: Vec<Chunk> = db
            .partition_chunk_summaries(&partition_key)
            .into_iter()
            .map(|summary| summary.into())
            .collect();

        Ok(Response::new(ListPartitionChunksResponse { chunks }))
    }

    async fn new_partition_chunk(
        &self,
        request: Request<NewPartitionChunkRequest>,
    ) -> Result<Response<NewPartitionChunkResponse>, Status> {
        let NewPartitionChunkRequest {
            db_name,
            partition_key,
            table_name,
        } = request.into_inner();
        let db_name = DatabaseName::new(db_name).field("db_name")?;
        let db = self
            .server
            .db(&db_name)
            .map_err(default_server_error_handler)?;

        db.rollover_partition(&table_name, &partition_key)
            .await
            .map_err(default_db_error_handler)?;

        Ok(Response::new(NewPartitionChunkResponse {}))
    }

    async fn close_partition_chunk(
        &self,
        request: Request<ClosePartitionChunkRequest>,
    ) -> Result<Response<ClosePartitionChunkResponse>, Status> {
        let ClosePartitionChunkRequest {
            db_name,
            partition_key,
            table_name,
            chunk_id,
        } = request.into_inner();

        // Validate that the database name is legit
        let db_name = DatabaseName::new(db_name).field("db_name")?;

        let tracker = self
            .server
            .close_chunk(&db_name, table_name, partition_key, chunk_id)
            .map_err(default_server_error_handler)?;

        let operation = Some(super::operations::encode_tracker(tracker)?);

        Ok(Response::new(ClosePartitionChunkResponse { operation }))
    }

    async fn unload_partition_chunk(
        &self,
        request: tonic::Request<UnloadPartitionChunkRequest>,
    ) -> Result<tonic::Response<UnloadPartitionChunkResponse>, tonic::Status> {
        let UnloadPartitionChunkRequest {
            db_name,
            partition_key,
            table_name,
            chunk_id,
        } = request.into_inner();

        // Validate that the database name is legit
        let db_name = DatabaseName::new(db_name).field("db_name")?;
        let db = self
            .server
            .db(&db_name)
            .map_err(default_server_error_handler)?;

        db.unload_read_buffer(&table_name, &partition_key, chunk_id)
            .map_err(default_db_error_handler)?;

        Ok(Response::new(UnloadPartitionChunkResponse {}))
    }

    async fn set_serving_readiness(
        &self,
        request: Request<SetServingReadinessRequest>,
    ) -> Result<Response<SetServingReadinessResponse>, Status> {
        let SetServingReadinessRequest { ready } = request.into_inner();
        self.serving_readiness.set(ready.into());
        Ok(Response::new(SetServingReadinessResponse {}))
    }

    async fn get_server_status(
        &self,
        _request: Request<GetServerStatusRequest>,
    ) -> Result<Response<GetServerStatusResponse>, Status> {
        let initialized = self.server.initialized();

        // Purposefully suppress error from server::Databases as don't want
        // to return an error if the server is not initialized
        let mut database_statuses: Vec<_> = self
            .server
            .databases()
            .map(|databases| {
                databases
                    .into_iter()
                    .map(|database| {
                        let state: database_status::DatabaseState = database.state_code().into();

                        DatabaseStatus {
                            db_name: database.config().name.to_string(),
                            error: database.init_error().map(|e| ProtobufError {
                                message: e.to_string(),
                            }),
                            state: state.into(),
                        }
                    })
                    .collect()
            })
            .unwrap_or_default();

        // Sort output by database name
        database_statuses.sort_unstable_by(|a, b| a.db_name.cmp(&b.db_name));

        Ok(Response::new(GetServerStatusResponse {
            server_status: Some(ServerStatus {
                initialized,
                error: self.server.server_init_error().map(|e| ProtobufError {
                    message: e.to_string(),
                }),
                database_statuses,
            }),
        }))
    }

    async fn wipe_preserved_catalog(
        &self,
        request: Request<WipePreservedCatalogRequest>,
    ) -> Result<Response<WipePreservedCatalogResponse>, Status> {
        let WipePreservedCatalogRequest { db_name } = request.into_inner();

        // Validate that the database name is legit
        let db_name = DatabaseName::new(db_name).field("db_name")?;

        let tracker = self
            .server
            .wipe_preserved_catalog(&db_name)
            .map_err(|e| match e {
                Error::DatabaseAlreadyExists { db_name } => AlreadyExists {
                    resource_type: "database".to_string(),
                    resource_name: db_name,
                    ..Default::default()
                }
                .into(),
                e => default_server_error_handler(e),
            })?;
        let operation = Some(super::operations::encode_tracker(tracker)?);

        Ok(Response::new(WipePreservedCatalogResponse { operation }))
    }
}

pub fn make_server<M>(
    application: Arc<ApplicationState>,
    server: Arc<Server<M>>,
    serving_readiness: ServingReadiness,
) -> management_service_server::ManagementServiceServer<
    impl management_service_server::ManagementService,
>
where
    M: ConnectionManager + Send + Sync + Debug + 'static,
{
    management_service_server::ManagementServiceServer::new(ManagementService {
        application,
        server,
        serving_readiness,
    })
}
