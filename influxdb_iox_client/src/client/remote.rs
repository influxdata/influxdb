use self::generated_types::{remote_service_client::RemoteServiceClient, *};

use crate::connection::Connection;
use crate::error::Error;

/// Re-export generated_types
pub mod generated_types {
    pub use generated_types::influxdata::iox::remote::v1::*;
}

/// An IOx Remote API client.
///
/// This client wraps the underlying `tonic` generated client with a
/// more ergonomic interface.
///
/// ```no_run
/// #[tokio::main]
/// # async fn main() {
/// use influxdb_iox_client::{
///     remote::Client,
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
///     .list_remotes()
///     .await
///     .expect("listing remotes failed");
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct Client {
    inner: RemoteServiceClient<Connection>,
}

impl Client {
    /// Creates a new client with the provided connection
    pub fn new(channel: Connection) -> Self {
        Self {
            inner: RemoteServiceClient::new(channel),
        }
    }

    /// List remotes.
    pub async fn list_remotes(&mut self) -> Result<Vec<generated_types::Remote>, Error> {
        let response = self.inner.list_remotes(ListRemotesRequest {}).await?;
        Ok(response.into_inner().remotes)
    }

    /// Update remote
    pub async fn update_remote(
        &mut self,
        id: u32,
        connection_string: impl Into<String> + Send,
    ) -> Result<(), Error> {
        self.inner
            .update_remote(UpdateRemoteRequest {
                remote: Some(generated_types::Remote {
                    id,
                    connection_string: connection_string.into(),
                }),
            })
            .await?;
        Ok(())
    }

    /// Delete remote
    pub async fn delete_remote(&mut self, id: u32) -> Result<(), Error> {
        self.inner.delete_remote(DeleteRemoteRequest { id }).await?;
        Ok(())
    }
}
