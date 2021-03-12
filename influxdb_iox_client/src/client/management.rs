use std::num::NonZeroU32;

use thiserror::Error;

use self::generated_types::{management_service_client::ManagementServiceClient, *};

use crate::connection::Connection;
use std::convert::TryInto;

/// Re-export generated_types
pub mod generated_types {
    pub use generated_types::influxdata::iox::management::v1::*;
}

/// Errors returned by Client::update_writer_id
#[derive(Debug, Error)]
pub enum UpdateWriterIdError {
    /// Client received an unexpected error from the server
    #[error("Unexpected server error: {}: {}", .0.code(), .0.message())]
    ServerError(tonic::Status),
}

/// Errors returned by Client::get_writer_id
#[derive(Debug, Error)]
pub enum GetWriterIdError {
    /// Writer ID is not set
    #[error("Writer ID not set")]
    NoWriterId,

    /// Client received an unexpected error from the server
    #[error("Unexpected server error: {}: {}", .0.code(), .0.message())]
    ServerError(tonic::Status),
}

/// Errors returned by Client::create_database
#[derive(Debug, Error)]
pub enum CreateDatabaseError {
    /// Writer ID is not set
    #[error("Writer ID not set")]
    NoWriterId,

    /// Database already exists
    #[error("Database already exists")]
    DatabaseAlreadyExists,

    /// Server returned an invalid argument error
    #[error("Unexpected server error: {}: {}", .0.code(), .0.message())]
    InvalidArgument(tonic::Status),

    /// Client received an unexpected error from the server
    #[error("Unexpected server error: {}: {}", .0.code(), .0.message())]
    ServerError(tonic::Status),
}

/// Errors returned by Client::list_databases
#[derive(Debug, Error)]
pub enum ListDatabaseError {
    /// Client received an unexpected error from the server
    #[error("Unexpected server error: {}: {}", .0.code(), .0.message())]
    ServerError(tonic::Status),
}

/// Errors returned by Client::get_database
#[derive(Debug, Error)]
pub enum GetDatabaseError {
    /// Writer ID is not set
    #[error("Writer ID not set")]
    NoWriterId,

    /// Database not found
    #[error("Database not found")]
    DatabaseNotFound,

    /// Response contained no payload
    #[error("Server returned an empty response")]
    EmptyResponse,

    /// Client received an unexpected error from the server
    #[error("Unexpected server error: {}: {}", .0.code(), .0.message())]
    ServerError(tonic::Status),
}

/// Errors returned by Client::list_chunks
#[derive(Debug, Error)]
pub enum ListChunksError {
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

/// An IOx Management API client.
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
    pub fn new(channel: tonic::transport::Channel) -> Self {
        Self {
            inner: ManagementServiceClient::new(channel),
        }
    }

    /// Set the server's writer ID.
    pub async fn update_writer_id(&mut self, id: NonZeroU32) -> Result<(), UpdateWriterIdError> {
        self.inner
            .update_writer_id(UpdateWriterIdRequest { id: id.into() })
            .await
            .map_err(UpdateWriterIdError::ServerError)?;
        Ok(())
    }

    /// Get the server's writer ID.
    pub async fn get_writer_id(&mut self) -> Result<NonZeroU32, GetWriterIdError> {
        let response = self
            .inner
            .get_writer_id(GetWriterIdRequest {})
            .await
            .map_err(|status| match status.code() {
                tonic::Code::NotFound => GetWriterIdError::NoWriterId,
                _ => GetWriterIdError::ServerError(status),
            })?;

        let id = response
            .get_ref()
            .id
            .try_into()
            .map_err(|_| GetWriterIdError::NoWriterId)?;

        Ok(id)
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
                tonic::Code::FailedPrecondition => CreateDatabaseError::NoWriterId,
                tonic::Code::InvalidArgument => CreateDatabaseError::InvalidArgument(status),
                _ => CreateDatabaseError::ServerError(status),
            })?;

        Ok(())
    }

    /// List databases.
    pub async fn list_databases(&mut self) -> Result<Vec<String>, ListDatabaseError> {
        let response = self
            .inner
            .list_databases(ListDatabasesRequest {})
            .await
            .map_err(ListDatabaseError::ServerError)?;
        Ok(response.into_inner().names)
    }

    /// Get database configuration
    pub async fn get_database(
        &mut self,
        name: impl Into<String>,
    ) -> Result<DatabaseRules, GetDatabaseError> {
        let response = self
            .inner
            .get_database(GetDatabaseRequest { name: name.into() })
            .await
            .map_err(|status| match status.code() {
                tonic::Code::NotFound => GetDatabaseError::DatabaseNotFound,
                tonic::Code::FailedPrecondition => GetDatabaseError::NoWriterId,
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
        db_name: impl Into<String>,
    ) -> Result<Vec<Chunk>, ListChunksError> {
        let db_name = db_name.into();

        let response = self
            .inner
            .list_chunks(ListChunksRequest { db_name })
            .await
            .map_err(ListChunksError::ServerError)?;
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
        connection_string: impl Into<String>,
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
}
