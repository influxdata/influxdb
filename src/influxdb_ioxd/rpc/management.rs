use std::convert::{TryFrom, TryInto};
use std::fmt::Debug;
use std::sync::Arc;
use std::time::Instant;

use data_types::chunk_metadata::ChunkId;
use data_types::{server_id::ServerId, DatabaseName};
use generated_types::google::{AlreadyExists, FieldViolation, FieldViolationExt, NotFound};
use generated_types::influxdata::iox::management::v1::{Error as ProtobufError, *};
use predicate::predicate::ParseDeletePredicate;
use query::QueryDatabase;
use server::rules::ProvidedDatabaseRules;
use server::{ApplicationState, ConnectionManager, Error, Server};
use tonic::{Request, Response, Status};

struct ManagementService<M: ConnectionManager> {
    application: Arc<ApplicationState>,
    server: Arc<Server<M>>,
    serving_readiness: ServingReadiness,
}

use super::error::{
    default_database_error_handler, default_db_error_handler, default_server_error_handler,
};
use crate::influxdb_ioxd::serving_readiness::ServingReadiness;

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
        request: Request<ListDatabasesRequest>,
    ) -> Result<Response<ListDatabasesResponse>, Status> {
        let ListDatabasesRequest { omit_defaults } = request.into_inner();

        let rules = self
            .server
            .databases()
            .map_err(default_server_error_handler)?
            .into_iter()
            .filter_map(|db| db.provided_rules())
            .map(|rules| format_rules(rules, omit_defaults))
            .collect::<Vec<_>>();

        Ok(Response::new(ListDatabasesResponse { rules }))
    }

    async fn get_database(
        &self,
        request: Request<GetDatabaseRequest>,
    ) -> Result<Response<GetDatabaseResponse>, Status> {
        let GetDatabaseRequest {
            name,
            omit_defaults,
        } = request.into_inner();

        let name = DatabaseName::new(name).field("name")?;
        let database = self
            .server
            .database(&name)
            .map_err(default_server_error_handler)?;

        if !database.is_active() {
            return Err(NotFound {
                resource_type: "database".to_string(),
                resource_name: name.to_string(),
                ..Default::default()
            }
            .into());
        }

        let rules = database
            .provided_rules()
            .map(|rules| format_rules(rules, omit_defaults))
            .ok_or_else(|| {
                tonic::Status::unavailable(format!(
                    "Rules have not yet been loaded for database ({})",
                    name
                ))
            })?;

        Ok(Response::new(GetDatabaseResponse { rules: Some(rules) }))
    }

    async fn create_database(
        &self,
        request: Request<CreateDatabaseRequest>,
    ) -> Result<Response<CreateDatabaseResponse>, Status> {
        let rules: DatabaseRules = request
            .into_inner()
            .rules
            .ok_or_else(|| FieldViolation::required("rules"))?;

        let provided_rules: ProvidedDatabaseRules = rules
            .try_into()
            .map_err(|e: FieldViolation| e.scope("rules"))?;

        match self.server.create_database(provided_rules).await {
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
        let rules: DatabaseRules = request
            .into_inner()
            .rules
            .ok_or_else(|| FieldViolation::required("rules"))?;

        let provided_rules: ProvidedDatabaseRules = rules
            .try_into()
            .map_err(|e: FieldViolation| e.scope("rules"))?;

        let db_name = provided_rules.db_name().clone();
        let updated_rules = self
            .server
            .update_db_rules(&db_name, provided_rules)
            .await
            .map_err(default_server_error_handler)?;

        Ok(Response::new(UpdateDatabaseResponse {
            rules: Some(updated_rules.rules().as_ref().clone().into()),
        }))
    }

    async fn delete_database(
        &self,
        request: Request<DeleteDatabaseRequest>,
    ) -> Result<Response<DeleteDatabaseResponse>, Status> {
        let db_name = DatabaseName::new(request.into_inner().db_name).field("db_name")?;

        self.server
            .delete_database(&db_name)
            .await
            .map_err(default_server_error_handler)?;

        Ok(Response::new(DeleteDatabaseResponse {}))
    }

    async fn restore_database(
        &self,
        request: Request<RestoreDatabaseRequest>,
    ) -> Result<Response<RestoreDatabaseResponse>, Status> {
        let request = request.into_inner();
        let db_name = DatabaseName::new(request.db_name).field("db_name")?;
        let generation_id = request.generation_id;

        self.server
            .restore_database(&db_name, generation_id)
            .await
            .map_err(default_server_error_handler)?;

        Ok(Response::new(RestoreDatabaseResponse {}))
    }

    async fn list_deleted_databases(
        &self,
        _: Request<ListDeletedDatabasesRequest>,
    ) -> Result<Response<ListDeletedDatabasesResponse>, Status> {
        let deleted_databases = self
            .server
            .list_deleted_databases()
            .await
            .map_err(default_server_error_handler)?
            .into_iter()
            .map(Into::into)
            .collect();

        Ok(Response::new(ListDeletedDatabasesResponse {
            deleted_databases,
        }))
    }

    async fn list_detailed_databases(
        &self,
        _: Request<ListDetailedDatabasesRequest>,
    ) -> Result<Response<ListDetailedDatabasesResponse>, Status> {
        let databases = self
            .server
            .list_detailed_databases()
            .await
            .map_err(default_server_error_handler)?
            .into_iter()
            .map(Into::into)
            .collect();

        Ok(Response::new(ListDetailedDatabasesResponse { databases }))
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
            .spawn_dummy_job(request.nanos, None);
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

        let chunk_id = ChunkId::new(chunk_id);

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

        let chunk_id = ChunkId::new(chunk_id);

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
                    .map(|database| DatabaseStatus {
                        db_name: database.config().name.to_string(),
                        error: database.init_error().map(|e| ProtobufError {
                            message: e.to_string(),
                        }),
                        state: database.state_code().into(),
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

    async fn skip_replay(
        &self,
        request: Request<SkipReplayRequest>,
    ) -> Result<Response<SkipReplayResponse>, Status> {
        let SkipReplayRequest { db_name } = request.into_inner();

        // Validate that the database name is legit
        let db_name = DatabaseName::new(db_name).field("db_name")?;

        let database = self
            .server
            .database(&db_name)
            .map_err(default_server_error_handler)?;

        database
            .skip_replay()
            .await
            .map_err(default_database_error_handler)?;

        Ok(Response::new(SkipReplayResponse {}))
    }

    async fn persist_partition(
        &self,
        request: tonic::Request<PersistPartitionRequest>,
    ) -> Result<tonic::Response<PersistPartitionResponse>, tonic::Status> {
        let PersistPartitionRequest {
            db_name,
            partition_key,
            table_name,
        } = request.into_inner();

        // Validate that the database name is legit
        let db_name = DatabaseName::new(db_name).field("db_name")?;
        let db = self
            .server
            .db(&db_name)
            .map_err(default_server_error_handler)?;

        db.persist_partition(&table_name, &partition_key, Instant::now())
            .await
            .map_err(default_db_error_handler)?;

        Ok(Response::new(PersistPartitionResponse {}))
    }

    async fn drop_partition(
        &self,
        request: tonic::Request<DropPartitionRequest>,
    ) -> Result<tonic::Response<DropPartitionResponse>, tonic::Status> {
        let DropPartitionRequest {
            db_name,
            partition_key,
            table_name,
        } = request.into_inner();

        // Validate that the database name is legit
        let db_name = DatabaseName::new(db_name).field("db_name")?;
        let db = self
            .server
            .db(&db_name)
            .map_err(default_server_error_handler)?;

        db.drop_partition(&table_name, &partition_key)
            .await
            .map_err(default_db_error_handler)?;

        Ok(Response::new(DropPartitionResponse {}))
    }

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
        let db_name = DatabaseName::new(db_name).field("db_name")?;
        let db = self
            .server
            .db(&db_name)
            .map_err(default_server_error_handler)?;

        let del_predicate_result = ParseDeletePredicate::build_delete_predicate(
            table_name.clone(),
            start_time.clone(),
            stop_time.clone(),
            predicate.clone(),
        );
        match del_predicate_result {
            Err(_) => {
                return Err(default_server_error_handler(Error::DeleteExpression {
                    start_time,
                    stop_time,
                    predicate,
                }))
            }
            Ok(del_predicate) => {
                //execute delete
                db.delete(&table_name, Arc::new(del_predicate))
                    .await
                    .map_err(default_db_error_handler)?;
            }
        }

        // NGA todo: return a delete handle with the response?
        Ok(Response::new(DeleteResponse {}))
    }
}

/// returns [`DatabaseRules`] formated accordingo the omit_defaults
/// flag. If omit_defaults is true, returns the stored config,
/// otherwise returns the actual configu
fn format_rules(provided_rules: Arc<ProvidedDatabaseRules>, omit_defaults: bool) -> DatabaseRules {
    if omit_defaults {
        // return rules as originally provided by the user
        provided_rules.original().clone()
    } else {
        // return the active rules (which have all default values filled in)
        provided_rules.rules().as_ref().clone().into()
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
