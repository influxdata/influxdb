use thiserror::Error;

use self::generated_types::{management_service_client::ManagementServiceClient, *};

use crate::connection::Connection;
use ::generated_types::google::longrunning::Operation;

use std::convert::TryInto;
use std::num::NonZeroU32;

/// Re-export generated_types
pub mod generated_types {
    pub use generated_types::influxdata::iox::management::v1::*;
}

/// Errors returned by Client::update_server_id
#[derive(Debug, Error)]
pub enum UpdateServerIdError {
    /// Client received an unexpected error from the server
    #[error("Unexpected server error: {}: {}", .0.code(), .0.message())]
    ServerError(tonic::Status),
}

/// Errors returned by Client::get_server_id
#[derive(Debug, Error)]
pub enum GetServerIdError {
    /// Server ID is not set
    #[error("Server ID not set")]
    NoServerId,

    /// Client received an unexpected error from the server
    #[error("Unexpected server error: {}: {}", .0.code(), .0.message())]
    ServerError(tonic::Status),
}

/// Errors returned by Client::set_serving_readiness
#[derive(Debug, Error)]
pub enum SetServingReadinessError {
    /// Client received an unexpected error from the server
    #[error("Unexpected server error: {}: {}", .0.code(), .0.message())]
    ServerError(tonic::Status),
}

/// Errors returned by Client::create_database
#[derive(Debug, Error)]
pub enum CreateDatabaseError {
    /// Server ID is not set
    #[error("Server ID not set")]
    NoServerId,

    /// Database already exists
    #[error("Database already exists")]
    DatabaseAlreadyExists,

    /// Server returned an invalid argument error
    #[error("Unexpected server error: {}: {}", .0.code(), .0.message())]
    InvalidArgument(tonic::Status),

    /// Server indicated that it is not (yet) available
    #[error("Server unavailable: {}", .0.message())]
    Unavailable(tonic::Status),

    /// Client received an unexpected error from the server
    #[error("Unexpected server error: {}: {}", .0.code(), .0.message())]
    ServerError(tonic::Status),
}

/// Errors returned by Client::update_database
#[derive(Debug, Error)]
pub enum UpdateDatabaseError {
    /// Server ID is not set
    #[error("Server ID not set")]
    NoServerId,

    /// Database not found
    #[error("Database not found")]
    DatabaseNotFound,

    /// Server returned an invalid argument error
    #[error("Unexpected server error: {}: {}", .0.code(), .0.message())]
    InvalidArgument(tonic::Status),

    /// Server indicated that it is not (yet) available
    #[error("Server unavailable: {}", .0.message())]
    Unavailable(tonic::Status),

    /// Client received an unexpected error from the server
    #[error("Unexpected server error: {}: {}", .0.code(), .0.message())]
    ServerError(tonic::Status),
}

/// Errors returned by Client::list_databases
#[derive(Debug, Error)]
pub enum ListDatabaseError {
    /// Server indicated that it is not (yet) available
    #[error("Server unavailable: {}", .0.message())]
    Unavailable(tonic::Status),

    /// Client received an unexpected error from the server
    #[error("Unexpected server error: {}: {}", .0.code(), .0.message())]
    ServerError(tonic::Status),
}

/// Errors returned by Client::get_database
#[derive(Debug, Error)]
pub enum GetDatabaseError {
    /// Server ID is not set
    #[error("Server ID not set")]
    NoServerId,

    /// Database not found
    #[error("Database not found")]
    DatabaseNotFound,

    /// Response contained no payload
    #[error("Server returned an empty response")]
    EmptyResponse,

    /// Server indicated that it is not (yet) available
    #[error("Server unavailable: {}", .0.message())]
    Unavailable(tonic::Status),

    /// Client received an unexpected error from the server
    #[error("Unexpected server error: {}: {}", .0.code(), .0.message())]
    ServerError(tonic::Status),
}

/// Errors returned by Client::list_chunks
#[derive(Debug, Error)]
pub enum ListChunksError {
    /// Server indicated that it is not (yet) available
    #[error("Server unavailable: {}", .0.message())]
    Unavailable(tonic::Status),

    /// Client received an unexpected error from the server
    #[error("Unexpected server error: {}: {}", .0.code(), .0.message())]
    ServerError(tonic::Status),
}

/// Errors returned by Client::list_remotes
#[derive(Debug, Error)]
pub enum ListRemotesError {
    /// Client received an unexpected error from the server
    #[error("Unexpected server error: {}: {}", .0.code(), .0.message())]
    ServerError(tonic::Status),
}

/// Errors returned by Client::update_remote
#[derive(Debug, Error)]
pub enum UpdateRemoteError {
    /// Client received an unexpected error from the server
    #[error("Unexpected server error: {}: {}", .0.code(), .0.message())]
    ServerError(tonic::Status),
}

/// Errors returned by Client::create_dummy_job
#[derive(Debug, Error)]
pub enum CreateDummyJobError {
    /// Response contained no payload
    #[error("Server returned an empty response")]
    EmptyResponse,

    /// Client received an unexpected error from the server
    #[error("Unexpected server error: {}: {}", .0.code(), .0.message())]
    ServerError(tonic::Status),
}

/// Errors returned by Client::list_partitions
#[derive(Debug, Error)]
pub enum ListPartitionsError {
    /// Database not found
    #[error("Database not found")]
    DatabaseNotFound,

    /// Server indicated that it is not (yet) available
    #[error("Server unavailable: {}", .0.message())]
    Unavailable(tonic::Status),

    /// Client received an unexpected error from the server
    #[error("Unexpected server error: {}: {}", .0.code(), .0.message())]
    ServerError(tonic::Status),
}

/// Errors returned by Client::get_partition
#[derive(Debug, Error)]
pub enum GetPartitionError {
    /// Database not found
    #[error("Database not found")]
    DatabaseNotFound,

    /// Partition not found
    #[error("Partition not found")]
    PartitionNotFound,

    /// Server indicated that it is not (yet) available
    #[error("Server unavailable: {}", .0.message())]
    Unavailable(tonic::Status),

    /// Client received an unexpected error from the server
    #[error("Unexpected server error: {}: {}", .0.code(), .0.message())]
    ServerError(tonic::Status),
}

/// Errors returned by Client::list_partition_chunks
#[derive(Debug, Error)]
pub enum ListPartitionChunksError {
    /// Server indicated that it is not (yet) available
    #[error("Server unavailable: {}", .0.message())]
    Unavailable(tonic::Status),

    /// Client received an unexpected error from the server
    #[error("Unexpected server error: {}: {}", .0.code(), .0.message())]
    ServerError(tonic::Status),
}

/// Errors returned by Client::new_partition_chunk
#[derive(Debug, Error)]
pub enum NewPartitionChunkError {
    /// Database or partition not found
    #[error("{}", .0)]
    NotFound(String),

    /// Server indicated that it is not (yet) available
    #[error("Server unavailable: {}", .0.message())]
    Unavailable(tonic::Status),

    /// Client received an unexpected error from the server
    #[error("Unexpected server error: {}: {}", .0.code(), .0.message())]
    ServerError(tonic::Status),
}

/// Errors returned by Client::close_partition_chunk
#[derive(Debug, Error)]
pub enum ClosePartitionChunkError {
    /// Database not found
    #[error("Database not found")]
    DatabaseNotFound,

    /// Response contained no payload
    #[error("Server returned an empty response")]
    EmptyResponse,

    /// Server indicated that it is not (yet) available
    #[error("Server unavailable: {}", .0.message())]
    Unavailable(tonic::Status),

    /// Client received an unexpected error from the server
    #[error("Unexpected server error: {}: {}", .0.code(), .0.message())]
    ServerError(tonic::Status),
}

/// Errors returned by [`Client::unload_partition_chunk`]
#[derive(Debug, Error)]
pub enum UnloadPartitionChunkError {
    /// Database not found
    #[error("Not found: {}", .0)]
    NotFound(String),

    /// Server indicated that it is not (yet) available
    #[error("Server unavailable: {}", .0.message())]
    Unavailable(tonic::Status),

    /// Server indicated that it is not (yet) available
    #[error("Cannot perform operation due to wrong chunk lifecycle state: {}", .0.message())]
    LifecycleError(tonic::Status),

    /// Client received an unexpected error from the server
    #[error("Unexpected server error: {}: {}", .0.code(), .0.message())]
    ServerError(tonic::Status),
}

/// Errors returned by [`Client::get_server_status`]
#[derive(Debug, Error)]
pub enum GetServerStatusError {
    /// Response contained no payload
    #[error("Server returned an empty response")]
    EmptyResponse,

    /// Client received an unexpected error from the server
    #[error("Unexpected server error: {}: {}", .0.code(), .0.message())]
    ServerError(tonic::Status),
}

/// Errors returned by [`Client::wipe_persisted_catalog`]
#[derive(Debug, Error)]
pub enum WipePersistedCatalogError {
    /// Server ID is not set
    #[error("Failed precondition: {}", .0.message())]
    FailedPrecondition(tonic::Status),

    /// Server returned an invalid argument error
    #[error("Invalid argument: {}", .0.message())]
    InvalidArgument(tonic::Status),

    /// Response contained no payload
    #[error("Server returned an empty response")]
    EmptyResponse,

    /// Client received an unexpected error from the server
    #[error("Unexpected server error: {}: {}", .0.code(), .0.message())]
    ServerError(tonic::Status),
}

/// Errors returned by [`Client::skip_replay`]
#[derive(Debug, Error)]
pub enum SkipReplayError {
    /// Server ID is not set
    #[error("Failed precondition: {}", .0.message())]
    FailedPrecondition(tonic::Status),

    /// Server returned an invalid argument error
    #[error("Invalid argument: {}", .0.message())]
    InvalidArgument(tonic::Status),

    /// Client received an unexpected error from the server
    #[error("Unexpected server error: {}: {}", .0.code(), .0.message())]
    ServerError(tonic::Status),
}

/// An IOx Management API client.
///
/// This client wraps the underlying `tonic` generated client with a
/// more ergonomic interface.
///
/// ```no_run
/// #[tokio::main]
/// # async fn main() {
/// use influxdb_iox_client::{
///     management::{Client, generated_types::DatabaseRules},
///     connection::Builder,
/// };
///
/// let mut connection = Builder::default()
///     .build("http://127.0.0.1:8082")
///     .await
///     .unwrap();
///
/// let mut client = Client::new(connection);
///
/// // Create a new database!
/// client
///     .create_database(DatabaseRules{
///     name: "bananas".to_string(),
///     ..Default::default()
/// })
///     .await
///     .expect("failed to create database");
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct Client {
    inner: ManagementServiceClient<Connection>,
}

impl Client {
    /// Creates a new client with the provided connection
    pub fn new(channel: Connection) -> Self {
        Self {
            inner: ManagementServiceClient::new(channel),
        }
    }

    /// Set the server's ID.
    pub async fn update_server_id(&mut self, id: u32) -> Result<(), UpdateServerIdError> {
        self.inner
            .update_server_id(UpdateServerIdRequest { id })
            .await
            .map_err(UpdateServerIdError::ServerError)?;
        Ok(())
    }

    /// Get the server's ID.
    pub async fn get_server_id(&mut self) -> Result<NonZeroU32, GetServerIdError> {
        let response = self
            .inner
            .get_server_id(GetServerIdRequest {})
            .await
            .map_err(|status| match status.code() {
                tonic::Code::NotFound => GetServerIdError::NoServerId,
                _ => GetServerIdError::ServerError(status),
            })?;

        let id = response
            .get_ref()
            .id
            .try_into()
            .map_err(|_| GetServerIdError::NoServerId)?;

        Ok(id)
    }

    /// Check if databases are loaded and ready for read and write.
    pub async fn get_server_status(&mut self) -> Result<ServerStatus, GetServerStatusError> {
        let response = self
            .inner
            .get_server_status(GetServerStatusRequest {})
            .await
            .map_err(GetServerStatusError::ServerError)?;

        let server_status = response
            .into_inner()
            .server_status
            .ok_or(GetServerStatusError::EmptyResponse)?;
        Ok(server_status)
    }

    /// Set serving readiness.
    pub async fn set_serving_readiness(
        &mut self,
        ready: bool,
    ) -> Result<(), SetServingReadinessError> {
        self.inner
            .set_serving_readiness(SetServingReadinessRequest { ready })
            .await
            .map_err(SetServingReadinessError::ServerError)?;
        Ok(())
    }

    /// Creates a new IOx database.
    pub async fn create_database(
        &mut self,
        rules: DatabaseRules,
    ) -> Result<(), CreateDatabaseError> {
        self.inner
            .create_database(CreateDatabaseRequest { rules: Some(rules) })
            .await
            .map_err(|status| match status.code() {
                tonic::Code::AlreadyExists => CreateDatabaseError::DatabaseAlreadyExists,
                tonic::Code::FailedPrecondition => CreateDatabaseError::NoServerId,
                tonic::Code::InvalidArgument => CreateDatabaseError::InvalidArgument(status),
                tonic::Code::Unavailable => CreateDatabaseError::Unavailable(status),
                _ => CreateDatabaseError::ServerError(status),
            })?;

        Ok(())
    }

    /// Updates the configuration for a database.
    pub async fn update_database(
        &mut self,
        rules: DatabaseRules,
    ) -> Result<DatabaseRules, UpdateDatabaseError> {
        let response = self
            .inner
            .update_database(UpdateDatabaseRequest { rules: Some(rules) })
            .await
            .map_err(|status| match status.code() {
                tonic::Code::NotFound => UpdateDatabaseError::DatabaseNotFound,
                tonic::Code::FailedPrecondition => UpdateDatabaseError::NoServerId,
                tonic::Code::InvalidArgument => UpdateDatabaseError::InvalidArgument(status),
                tonic::Code::Unavailable => UpdateDatabaseError::Unavailable(status),
                _ => UpdateDatabaseError::ServerError(status),
            })?;

        Ok(response.into_inner().rules.unwrap())
    }

    /// List databases.
    pub async fn list_databases(&mut self) -> Result<Vec<String>, ListDatabaseError> {
        let response = self
            .inner
            .list_databases(ListDatabasesRequest {})
            .await
            .map_err(|status| match status.code() {
                tonic::Code::Unavailable => ListDatabaseError::Unavailable(status),
                _ => ListDatabaseError::ServerError(status),
            })?;
        Ok(response.into_inner().names)
    }

    /// Get database configuration
    pub async fn get_database(
        &mut self,
        name: impl Into<String> + Send,
    ) -> Result<DatabaseRules, GetDatabaseError> {
        let response = self
            .inner
            .get_database(GetDatabaseRequest { name: name.into() })
            .await
            .map_err(|status| match status.code() {
                tonic::Code::NotFound => GetDatabaseError::DatabaseNotFound,
                tonic::Code::FailedPrecondition => GetDatabaseError::NoServerId,
                tonic::Code::Unavailable => GetDatabaseError::Unavailable(status),
                _ => GetDatabaseError::ServerError(status),
            })?;

        let rules = response
            .into_inner()
            .rules
            .ok_or(GetDatabaseError::EmptyResponse)?;
        Ok(rules)
    }

    /// List chunks in a database.
    pub async fn list_chunks(
        &mut self,
        db_name: impl Into<String> + Send,
    ) -> Result<Vec<Chunk>, ListChunksError> {
        let db_name = db_name.into();

        let response = self
            .inner
            .list_chunks(ListChunksRequest { db_name })
            .await
            .map_err(|status| match status.code() {
                tonic::Code::Unavailable => ListChunksError::Unavailable(status),
                _ => ListChunksError::ServerError(status),
            })?;
        Ok(response.into_inner().chunks)
    }

    /// List remotes.
    pub async fn list_remotes(&mut self) -> Result<Vec<generated_types::Remote>, ListRemotesError> {
        let response = self
            .inner
            .list_remotes(ListRemotesRequest {})
            .await
            .map_err(ListRemotesError::ServerError)?;
        Ok(response.into_inner().remotes)
    }

    /// Update remote
    pub async fn update_remote(
        &mut self,
        id: u32,
        connection_string: impl Into<String> + Send,
    ) -> Result<(), UpdateRemoteError> {
        self.inner
            .update_remote(UpdateRemoteRequest {
                remote: Some(generated_types::Remote {
                    id,
                    connection_string: connection_string.into(),
                }),
            })
            .await
            .map_err(UpdateRemoteError::ServerError)?;
        Ok(())
    }

    /// Delete remote
    pub async fn delete_remote(&mut self, id: u32) -> Result<(), UpdateRemoteError> {
        self.inner
            .delete_remote(DeleteRemoteRequest { id })
            .await
            .map_err(UpdateRemoteError::ServerError)?;
        Ok(())
    }

    /// List all partitions of the database
    pub async fn list_partitions(
        &mut self,
        db_name: impl Into<String> + Send,
    ) -> Result<Vec<Partition>, ListPartitionsError> {
        let db_name = db_name.into();
        let response = self
            .inner
            .list_partitions(ListPartitionsRequest { db_name })
            .await
            .map_err(|status| match status.code() {
                tonic::Code::NotFound => ListPartitionsError::DatabaseNotFound,
                tonic::Code::Unavailable => ListPartitionsError::Unavailable(status),
                _ => ListPartitionsError::ServerError(status),
            })?;

        let ListPartitionsResponse { partitions } = response.into_inner();

        Ok(partitions)
    }

    /// Get details about a specific partition
    pub async fn get_partition(
        &mut self,
        db_name: impl Into<String> + Send,
        partition_key: impl Into<String> + Send,
    ) -> Result<Partition, GetPartitionError> {
        let db_name = db_name.into();
        let partition_key = partition_key.into();

        let response = self
            .inner
            .get_partition(GetPartitionRequest {
                db_name,
                partition_key,
            })
            .await
            .map_err(|status| match status.code() {
                tonic::Code::NotFound => GetPartitionError::DatabaseNotFound,
                tonic::Code::Unavailable => GetPartitionError::Unavailable(status),
                _ => GetPartitionError::ServerError(status),
            })?;

        let GetPartitionResponse { partition } = response.into_inner();

        partition.ok_or(GetPartitionError::PartitionNotFound)
    }

    /// List chunks in a partition
    pub async fn list_partition_chunks(
        &mut self,
        db_name: impl Into<String> + Send,
        partition_key: impl Into<String> + Send,
    ) -> Result<Vec<Chunk>, ListPartitionChunksError> {
        let db_name = db_name.into();
        let partition_key = partition_key.into();

        let response = self
            .inner
            .list_partition_chunks(ListPartitionChunksRequest {
                db_name,
                partition_key,
            })
            .await
            .map_err(|status| match status.code() {
                tonic::Code::Unavailable => ListPartitionChunksError::Unavailable(status),
                _ => ListPartitionChunksError::ServerError(status),
            })?;
        Ok(response.into_inner().chunks)
    }

    /// Create a new chunk in a partition
    pub async fn new_partition_chunk(
        &mut self,
        db_name: impl Into<String> + Send,
        table_name: impl Into<String> + Send,
        partition_key: impl Into<String> + Send,
    ) -> Result<(), NewPartitionChunkError> {
        let db_name = db_name.into();
        let partition_key = partition_key.into();
        let table_name = table_name.into();

        self.inner
            .new_partition_chunk(NewPartitionChunkRequest {
                db_name,
                partition_key,
                table_name,
            })
            .await
            .map_err(|status| match status.code() {
                tonic::Code::NotFound => {
                    NewPartitionChunkError::NotFound(status.message().to_string())
                }
                tonic::Code::Unavailable => NewPartitionChunkError::Unavailable(status),
                _ => NewPartitionChunkError::ServerError(status),
            })?;

        Ok(())
    }

    /// Creates a dummy job that for each value of the nanos field
    /// spawns a task that sleeps for that number of nanoseconds before
    /// returning
    pub async fn create_dummy_job(
        &mut self,
        nanos: Vec<u64>,
    ) -> Result<Operation, CreateDummyJobError> {
        let response = self
            .inner
            .create_dummy_job(CreateDummyJobRequest { nanos })
            .await
            .map_err(CreateDummyJobError::ServerError)?;

        Ok(response
            .into_inner()
            .operation
            .ok_or(CreateDummyJobError::EmptyResponse)?)
    }

    /// Closes the specified chunk in the specified partition and
    /// begins it moving to the read buffer.
    ///
    /// Returns the job tracking the data's movement
    pub async fn close_partition_chunk(
        &mut self,
        db_name: impl Into<String> + Send,
        table_name: impl Into<String> + Send,
        partition_key: impl Into<String> + Send,
        chunk_id: u32,
    ) -> Result<Operation, ClosePartitionChunkError> {
        let db_name = db_name.into();
        let partition_key = partition_key.into();
        let table_name = table_name.into();

        let response = self
            .inner
            .close_partition_chunk(ClosePartitionChunkRequest {
                db_name,
                partition_key,
                table_name,
                chunk_id,
            })
            .await
            .map_err(|status| match status.code() {
                tonic::Code::NotFound => ClosePartitionChunkError::DatabaseNotFound,
                tonic::Code::Unavailable => ClosePartitionChunkError::Unavailable(status),
                _ => ClosePartitionChunkError::ServerError(status),
            })?;

        Ok(response
            .into_inner()
            .operation
            .ok_or(ClosePartitionChunkError::EmptyResponse)?)
    }

    /// Unload chunk from read buffer but keep it in object store.
    pub async fn unload_partition_chunk(
        &mut self,
        db_name: impl Into<String> + Send,
        table_name: impl Into<String> + Send,
        partition_key: impl Into<String> + Send,
        chunk_id: u32,
    ) -> Result<(), UnloadPartitionChunkError> {
        let db_name = db_name.into();
        let partition_key = partition_key.into();
        let table_name = table_name.into();

        self.inner
            .unload_partition_chunk(UnloadPartitionChunkRequest {
                db_name,
                partition_key,
                table_name,
                chunk_id,
            })
            .await
            .map_err(|status| match status.code() {
                tonic::Code::NotFound => {
                    UnloadPartitionChunkError::NotFound(status.message().to_string())
                }
                tonic::Code::Unavailable => UnloadPartitionChunkError::Unavailable(status),
                tonic::Code::FailedPrecondition => {
                    UnloadPartitionChunkError::LifecycleError(status)
                }
                _ => UnloadPartitionChunkError::ServerError(status),
            })?;

        Ok(())
    }

    /// Wipe potential preserved catalog of an uninitialized database.
    pub async fn wipe_persisted_catalog(
        &mut self,
        db_name: impl Into<String> + Send,
    ) -> Result<Operation, WipePersistedCatalogError> {
        let db_name = db_name.into();

        let response = self
            .inner
            .wipe_preserved_catalog(WipePreservedCatalogRequest { db_name })
            .await
            .map_err(|status| match status.code() {
                tonic::Code::FailedPrecondition => {
                    WipePersistedCatalogError::FailedPrecondition(status)
                }
                tonic::Code::InvalidArgument => WipePersistedCatalogError::InvalidArgument(status),
                _ => WipePersistedCatalogError::ServerError(status),
            })?;

        Ok(response
            .into_inner()
            .operation
            .ok_or(WipePersistedCatalogError::EmptyResponse)?)
    }

    /// Skip replay of an uninitialized database.
    pub async fn skip_replay(
        &mut self,
        db_name: impl Into<String> + Send,
    ) -> Result<(), SkipReplayError> {
        let db_name = db_name.into();

        self.inner
            .skip_replay(SkipReplayRequest { db_name })
            .await
            .map_err(|status| match status.code() {
                tonic::Code::FailedPrecondition => SkipReplayError::FailedPrecondition(status),
                tonic::Code::InvalidArgument => SkipReplayError::InvalidArgument(status),
                _ => SkipReplayError::ServerError(status),
            })?;

        Ok(())
    }
}
