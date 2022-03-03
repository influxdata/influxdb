use self::generated_types::{operations_client::OperationsClient, *};
use crate::connection::Connection;
use crate::error::Error;
use std::convert::TryInto;

/// Re-export generated_types
pub mod generated_types {
    pub use generated_types::google::longrunning::*;
}

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
    pub async fn list_operations(&mut self) -> Result<Vec<IoxOperation>, Error> {
        self.inner
            .list_operations(ListOperationsRequest::default())
            .await?
            .into_inner()
            .operations
            .into_iter()
            .map(TryInto::try_into)
            .collect::<Result<_, _>>()
            .map_err(Error::InvalidResponse)
    }

    /// Get information about a specific operation
    pub async fn get_operation(&mut self, id: usize) -> Result<IoxOperation, Error> {
        self.inner
            .get_operation(GetOperationRequest {
                name: id.to_string(),
            })
            .await?
            .into_inner()
            .try_into()
            .map_err(Error::InvalidResponse)
    }

    /// Cancel a given operation
    pub async fn cancel_operation(&mut self, id: usize) -> Result<(), Error> {
        self.inner
            .cancel_operation(CancelOperationRequest {
                name: id.to_string(),
            })
            .await?;

        Ok(())
    }

    /// Waits until an operation completes, or the timeout expires, and
    /// returns the latest operation metadata
    pub async fn wait_operation(
        &mut self,
        id: usize,
        timeout: Option<std::time::Duration>,
    ) -> Result<IoxOperation, Error> {
        self.inner
            .wait_operation(WaitOperationRequest {
                name: id.to_string(),
                timeout: timeout.map(Into::into),
            })
            .await?
            .into_inner()
            .try_into()
            .map_err(Error::InvalidResponse)
    }
}
