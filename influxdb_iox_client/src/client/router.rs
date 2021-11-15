use thiserror::Error;

use self::generated_types::{router_service_client::RouterServiceClient, *};

use crate::connection::Connection;

/// Re-export generated_types
pub mod generated_types {
    pub use generated_types::influxdata::iox::router::v1::*;
    pub use generated_types::influxdata::iox::write_buffer::v1::*;
}

/// Errors returned by Client::list_routers
#[derive(Debug, Error)]
pub enum ListRoutersError {
    /// Server indicated that it is not (yet) available
    #[error("Server unavailable: {}", .0.message())]
    Unavailable(tonic::Status),

    /// Client received an unexpected error from the server
    #[error("Unexpected server error: {}: {}", .0.code(), .0.message())]
    ServerError(tonic::Status),
}

/// Errors returned by Client::update_router
#[derive(Debug, Error)]
pub enum UpdateRouterError {
    /// Server indicated that it is not (yet) available
    #[error("Server unavailable: {}", .0.message())]
    Unavailable(tonic::Status),

    /// Server returned an invalid argument error
    #[error("Invalid argument: {}", .0.message())]
    InvalidArgument(tonic::Status),

    /// Client received an unexpected error from the server
    #[error("Unexpected server error: {}: {}", .0.code(), .0.message())]
    ServerError(tonic::Status),
}

/// Errors returned by Client::delete_router
#[derive(Debug, Error)]
pub enum DeleteRouterError {
    /// Server indicated that it is not (yet) available
    #[error("Server unavailable: {}", .0.message())]
    Unavailable(tonic::Status),

    /// Client received an unexpected error from the server
    #[error("Unexpected server error: {}: {}", .0.code(), .0.message())]
    ServerError(tonic::Status),
}

/// An IOx Router API client.
///
/// This client wraps the underlying `tonic` generated client with a
/// more ergonomic interface.
///
/// ```no_run
/// #[tokio::main]
/// # async fn main() {
/// use influxdb_iox_client::{
///     router::Client,
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
/// // List routers
/// client
///     .list_routers()
///     .await
///     .expect("listing routers failed");
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct Client {
    inner: RouterServiceClient<Connection>,
}

impl Client {
    /// Creates a new client with the provided connection
    pub fn new(channel: Connection) -> Self {
        Self {
            inner: RouterServiceClient::new(channel),
        }
    }

    /// List routers.
    pub async fn list_routers(&mut self) -> Result<Vec<generated_types::Router>, ListRoutersError> {
        let response = self
            .inner
            .list_routers(ListRoutersRequest {})
            .await
            .map_err(|status| match status.code() {
                tonic::Code::Unavailable => ListRoutersError::Unavailable(status),
                _ => ListRoutersError::ServerError(status),
            })?;

        Ok(response.into_inner().routers)
    }

    /// Update router
    pub async fn update_router(
        &mut self,
        config: generated_types::Router,
    ) -> Result<(), UpdateRouterError> {
        self.inner
            .update_router(UpdateRouterRequest {
                router: Some(config),
            })
            .await
            .map_err(|status| match status.code() {
                tonic::Code::Unavailable => UpdateRouterError::Unavailable(status),
                tonic::Code::InvalidArgument => UpdateRouterError::InvalidArgument(status),
                _ => UpdateRouterError::ServerError(status),
            })?;

        Ok(())
    }

    /// Delete router
    pub async fn delete_router(&mut self, router_name: &str) -> Result<(), DeleteRouterError> {
        self.inner
            .delete_router(DeleteRouterRequest {
                router_name: router_name.to_string(),
            })
            .await
            .map_err(|status| match status.code() {
                tonic::Code::Unavailable => DeleteRouterError::Unavailable(status),
                _ => DeleteRouterError::ServerError(status),
            })?;

        Ok(())
    }
}
