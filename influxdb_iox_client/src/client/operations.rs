use thiserror::Error;

use self::generated_types::{operations_client::OperationsClient, *};
use crate::connection::Connection;
use std::convert::TryInto;

/// Re-export generated_types
pub mod generated_types {
    pub use generated_types::google::longrunning::*;
}

/// Error type for the operations Client
#[derive(Debug, Error)]
pub enum Error {
    /// Client received an invalid response
    #[error("Invalid server response: {}", .0)]
    InvalidResponse(#[from] ::generated_types::google::FieldViolation),

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
    pub fn new(channel: Connection) -> Self {
        Self {
            inner: OperationsClient::new(channel),
        }
    }

    /// Get information of all client operation
    pub async fn list_operations(&mut self) -> Result<Vec<IoxOperation>> {
        Ok(self
            .inner
            .list_operations(ListOperationsRequest::default())
            .await
            .map_err(Error::ServerError)?
            .into_inner()
            .operations
            .into_iter()
            .map(TryInto::try_into)
            .collect::<Result<_, _>>()?)
    }

    /// Get information about a specific operation
    pub async fn get_operation(&mut self, id: usize) -> Result<IoxOperation> {
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
            .into_inner()
            .try_into()?)
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
    ) -> Result<IoxOperation> {
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
            .into_inner()
            .try_into()?)
    }
}
