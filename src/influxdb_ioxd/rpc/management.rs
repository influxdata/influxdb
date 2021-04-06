use std::convert::TryInto;
use std::fmt::Debug;
use std::sync::Arc;

use data_types::database_rules::DatabaseRules;
use data_types::DatabaseName;
use generated_types::google::{AlreadyExists, FieldViolation, FieldViolationExt, NotFound};
use generated_types::influxdata::iox::management::v1::*;
use query::{Database, DatabaseStore};
use server::{ConnectionManager, Error, Server};
use tonic::{Request, Response, Status};

struct ManagementService<M: ConnectionManager> {
    server: Arc<Server<M>>,
}

use super::error::{default_db_error_handler, default_server_error_handler};

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
        let names = self.server.db_names_sorted();
        Ok(Response::new(ListDatabasesResponse { names }))
    }

    async fn get_database(
        &self,
        request: Request<GetDatabaseRequest>,
    ) -> Result<Response<GetDatabaseResponse>, Status> {
        let name = DatabaseName::new(request.into_inner().name).field("name")?;

        match self.server.db_rules(&name) {
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

    async fn list_chunks(
        &self,
        request: Request<ListChunksRequest>,
    ) -> Result<Response<ListChunksResponse>, Status> {
        let db_name = DatabaseName::new(request.into_inner().db_name).field("db_name")?;

        let db = match self.server.db(&db_name) {
            Some(db) => db,
            None => {
                return Err(NotFound {
                    resource_type: "database".to_string(),
                    resource_name: db_name.to_string(),
                    ..Default::default()
                }
                .into())
            }
        };

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
        let tracker = self.server.spawn_dummy_job(request.nanos);
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
                id,
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
        if remote.id == 0 {
            return Err(FieldViolation::required("id").scope("remote").into());
        }
        self.server
            .update_remote(remote.id, remote.connection_string);
        Ok(Response::new(UpdateRemoteResponse {}))
    }

    async fn delete_remote(
        &self,
        request: Request<DeleteRemoteRequest>,
    ) -> Result<Response<DeleteRemoteResponse>, Status> {
        let request = request.into_inner();
        if request.id == 0 {
            return Err(FieldViolation::required("id").into());
        }
        self.server
            .delete_remote(request.id)
            .ok_or_else(NotFound::default)?;

        Ok(Response::new(DeleteRemoteResponse {}))
    }

    async fn list_partitions(
        &self,
        request: Request<ListPartitionsRequest>,
    ) -> Result<Response<ListPartitionsResponse>, Status> {
        let ListPartitionsRequest { db_name } = request.into_inner();
        let db_name = DatabaseName::new(db_name).field("db_name")?;

        let db = self.server.db(&db_name).ok_or_else(|| NotFound {
            resource_type: "database".to_string(),
            resource_name: db_name.to_string(),
            ..Default::default()
        })?;

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

        let db = self.server.db(&db_name).ok_or_else(|| NotFound {
            resource_type: "database".to_string(),
            resource_name: db_name.to_string(),
            ..Default::default()
        })?;

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

        let db = self.server.db(&db_name).ok_or_else(|| NotFound {
            resource_type: "database".to_string(),
            resource_name: db_name.to_string(),
            ..Default::default()
        })?;

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
        } = request.into_inner();
        let db_name = DatabaseName::new(db_name).field("db_name")?;

        let db = self.server.db(&db_name).ok_or_else(|| NotFound {
            resource_type: "database".to_string(),
            resource_name: db_name.to_string(),
            ..Default::default()
        })?;

        db.rollover_partition(&partition_key)
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
            chunk_id,
        } = request.into_inner();

        // Validate that the database name is legit
        let db_name = DatabaseName::new(db_name).field("db_name")?;

        let tracker = self
            .server
            .close_chunk(db_name, partition_key, chunk_id)
            .map_err(default_server_error_handler)?;

        let operation = Some(super::operations::encode_tracker(tracker)?);

        Ok(Response::new(ClosePartitionChunkResponse { operation }))
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
