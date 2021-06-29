use thiserror::Error;

use ::generated_types::{
    google::FieldViolation, influxdata::iox::management::v1 as management, protobuf_type_url_eq,
};

use self::generated_types::{operations_client::OperationsClient, *};
use crate::connection::Connection;
/// Re-export generated_types
pub mod generated_types {
    pub use generated_types::google::longrunning::*;
}

/// Error type for the operations Client
#[derive(Debug, Error)]
pub enum Error {
    /// Client received an invalid response
    #[error("Invalid server response: {}", .0)]
    InvalidResponse(#[from] FieldViolation),

    /// Operation was not found
    #[error("Operation not found: {}", .0)]
    NotFound(usize),

    /// Client received an unexpected error from the server
    #[error("Unexpected server error: {}: {}", .0.code(), .0.message())]
    ServerError(tonic::Status),

    /// Operation is not type url
    #[error("Operation metadata is not type_url")]
    WrongOperationMetaData,
}

/// Result type for the operations Client
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// An IOx Long Running Operations API client.
///
/// ```no_run
/// #[tokio::main]
/// # async fn main() {
/// use influxdb_iox_client::{
///     operations::Client,
///     connection::Builder,
/// };
///
/// let mut connection = Builder::default()
///     .build("http://127.0.0.1:8082")
///     .await
///     .unwrap();
///
/// let mut client = Client::new(connection);
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct Client {
    inner: OperationsClient<Connection>,
}

impl Client {
    /// Creates a new client with the provided connection
    pub fn new(channel: tonic::transport::Channel) -> Self {
        Self {
            inner: OperationsClient::new(channel),
        }
    }

    /// Get information of all client operation
    pub async fn list_operations(&mut self) -> Result<Vec<ClientOperation>> {
        Ok(self
            .inner
            .list_operations(ListOperationsRequest::default())
            .await
            .map_err(Error::ServerError)?
            .into_inner()
            .operations
            .into_iter()
            .map(|o| ClientOperation::try_new(o).unwrap())
            .collect())
    }

    /// Get information about a specific operation
    pub async fn get_operation(&mut self, id: usize) -> Result<Operation> {
        Ok(self
            .inner
            .get_operation(GetOperationRequest {
                name: id.to_string(),
            })
            .await
            .map_err(|e| match e.code() {
                tonic::Code::NotFound => Error::NotFound(id),
                _ => Error::ServerError(e),
            })?
            .into_inner())
    }

    /// Cancel a given operation
    pub async fn cancel_operation(&mut self, id: usize) -> Result<()> {
        self.inner
            .cancel_operation(CancelOperationRequest {
                name: id.to_string(),
            })
            .await
            .map_err(|e| match e.code() {
                tonic::Code::NotFound => Error::NotFound(id),
                _ => Error::ServerError(e),
            })?;

        Ok(())
    }

    /// Waits until an operation completes, or the timeout expires, and
    /// returns the latest operation metadata
    pub async fn wait_operation(
        &mut self,
        id: usize,
        timeout: Option<std::time::Duration>,
    ) -> Result<Operation> {
        Ok(self
            .inner
            .wait_operation(WaitOperationRequest {
                name: id.to_string(),
                timeout: timeout.map(Into::into),
            })
            .await
            .map_err(|e| match e.code() {
                tonic::Code::NotFound => Error::NotFound(id),
                _ => Error::ServerError(e),
            })?
            .into_inner())
    }

    /// Return the Client Operation
    pub async fn client_operation(&mut self, id: usize) -> Result<ClientOperation> {
        let operation = self.get_operation(id).await?;
        ClientOperation::try_new(operation)
    }
}

/// IOx's Client Operation
#[derive(Debug, Clone)]
pub struct ClientOperation {
    inner: generated_types::Operation,
}

impl ClientOperation {
    /// Create a new Cient Operation
    pub fn try_new(operation: generated_types::Operation) -> Result<Self> {
        if operation.metadata.is_some() {
            let metadata = operation.metadata.clone().unwrap();
            if !protobuf_type_url_eq(&metadata.type_url, management::OPERATION_METADATA) {
                return Err(Error::WrongOperationMetaData);
            }
        } else {
            return Err(Error::NotFound(0));
        }

        Ok(Self { inner: operation })
    }

    /// Return Metadata for this client operation
    pub fn metadata(&self) -> management::OperationMetadata {
        prost::Message::decode(self.inner.metadata.clone().unwrap().value)
            .expect("failed to decode metadata")
    }

    /// Return name of this operation
    pub fn name(&self) -> &str {
        &self.inner.name
    }

    /// Return the inner's Operation
    pub fn operation(self) -> Operation {
        self.inner
    }
}
