use generated_types::google::FieldViolation;

use generated_types::grpc::health::v1::*;

use crate::connection::{Connection, GrpcConnection};
use crate::error::Error;

/// A client for the gRPC health checking API
///
/// Allows checking the status of a given service
#[derive(Debug)]
pub struct Client {
    inner: health_client::HealthClient<GrpcConnection>,
}

impl Client {
    /// Creates a new client with the provided connection
    pub fn new(channel: Connection) -> Self {
        Self {
            inner: health_client::HealthClient::new(channel.into_grpc_connection()),
        }
    }

    /// Returns `Ok(true)` if the corresponding service is serving
    pub async fn check(&mut self, service: impl Into<String> + Send) -> Result<bool, Error> {
        use health_check_response::ServingStatus;

        let response = self
            .inner
            .check(HealthCheckRequest {
                service: service.into(),
            })
            .await?
            .into_inner();

        match response.status() {
            ServingStatus::Serving => Ok(true),
            ServingStatus::NotServing => Ok(false),
            _ => Err(Error::InvalidResponse(FieldViolation {
                field: "status".to_string(),
                description: format!("invalid response: {}", response.status),
            })),
        }
    }

    /// Returns `Ok(true)` if the storage service is serving
    pub async fn check_storage(&mut self) -> Result<bool, Error> {
        self.check(generated_types::STORAGE_SERVICE).await
    }

    /// Returns `Ok(true)` if the Arrow Flight service is serving
    pub async fn check_arrow(&mut self) -> Result<bool, Error> {
        self.check(generated_types::ARROW_SERVICE).await
    }
}
