use thiserror::Error;

use generated_types::grpc::health::v1::*;

use crate::connection::Connection;

/// Error type for the health check client
#[derive(Debug, Error)]
pub enum Error {
    /// Service is not serving
    #[error("Service is not serving")]
    NotServing,

    /// Service returned an unexpected variant for the status enumeration
    #[error("Received invalid response: {}", .0)]
    InvalidResponse(i32),

    /// Client received an unexpected error from the server
    #[error("Unexpected server error: {}: {}", .0.code(), .0.message())]
    UnexpectedError(#[from] tonic::Status),
}

/// Result type for the health check client
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// A client for the gRPC health checking API
///
/// Allows checking the status of a given service
#[derive(Debug)]
pub struct Client {
    inner: health_client::HealthClient<Connection>,
}

impl Client {
    /// Creates a new client with the provided connection
    pub fn new(channel: Connection) -> Self {
        Self {
            inner: health_client::HealthClient::new(channel),
        }
    }

    /// Returns `Ok()` if the corresponding service is serving
    pub async fn check(&mut self, service: impl Into<String> + Send) -> Result<()> {
        use health_check_response::ServingStatus;

        let status = self
            .inner
            .check(HealthCheckRequest {
                service: service.into(),
            })
            .await?
            .into_inner();

        match status.status() {
            ServingStatus::Serving => Ok(()),
            ServingStatus::NotServing => Err(Error::NotServing),
            _ => Err(Error::InvalidResponse(status.status)),
        }
    }

    /// Returns `Ok()` if the storage service is serving
    pub async fn check_storage(&mut self) -> Result<()> {
        self.check(generated_types::STORAGE_SERVICE).await
    }
}
