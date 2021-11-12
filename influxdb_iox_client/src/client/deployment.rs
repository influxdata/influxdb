use self::generated_types::{deployment_service_client::DeploymentServiceClient, *};
use crate::connection::Connection;
use std::{convert::TryInto, num::NonZeroU32};
use thiserror::Error;

/// Re-export generated_types
pub mod generated_types {
    pub use generated_types::influxdata::iox::deployment::v1::*;
}

/// Errors returned by Client::update_server_id
#[derive(Debug, Error)]
pub enum UpdateServerIdError {
    /// Client received an unexpected error from the server
    #[error("Server ID already set")]
    AlreadySet,

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

/// Errors returned by Client::get_serving_readiness
#[derive(Debug, Error)]
pub enum GetServingReadinessError {
    /// Client received an unexpected error from the server
    #[error("Unexpected server error: {}: {}", .0.code(), .0.message())]
    ServerError(tonic::Status),
}

/// An IOx Deployment API client.
///
/// This client wraps the underlying `tonic` generated client with a
/// more ergonomic interface.
///
/// ```no_run
/// #[tokio::main]
/// # async fn main() {
/// use std::num::NonZeroU32;
/// use influxdb_iox_client::{
///     deployment::Client,
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
/// // Update server ID.
/// let server_id = NonZeroU32::new(42).unwrap();
/// client
///     .update_server_id(server_id)
///     .await
///     .expect("could not update server ID");
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct Client {
    inner: DeploymentServiceClient<Connection>,
}

impl Client {
    /// Creates a new client with the provided connection
    pub fn new(channel: Connection) -> Self {
        Self {
            inner: DeploymentServiceClient::new(channel),
        }
    }

    /// Set the server's ID.
    pub async fn update_server_id(&mut self, id: NonZeroU32) -> Result<(), UpdateServerIdError> {
        self.inner
            .update_server_id(UpdateServerIdRequest { id: id.get() })
            .await
            .map_err(|status| match status.code() {
                tonic::Code::InvalidArgument => UpdateServerIdError::AlreadySet,
                _ => UpdateServerIdError::ServerError(status),
            })?;
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

    /// Get serving readiness.
    pub async fn get_serving_readiness(&mut self) -> Result<bool, GetServingReadinessError> {
        let response = self
            .inner
            .get_serving_readiness(GetServingReadinessRequest {})
            .await
            .map_err(GetServingReadinessError::ServerError)?;

        Ok(response.get_ref().ready)
    }
}
