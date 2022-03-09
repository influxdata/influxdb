use self::generated_types::{deployment_service_client::DeploymentServiceClient, *};
use crate::connection::Connection;
use crate::error::Error;
use std::{convert::TryInto, num::NonZeroU32};

/// Re-export generated_types
pub mod generated_types {
    pub use generated_types::influxdata::iox::deployment::v1::*;
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
    pub async fn update_server_id(&mut self, id: NonZeroU32) -> Result<(), Error> {
        self.inner
            .update_server_id(UpdateServerIdRequest { id: id.get() })
            .await?;
        Ok(())
    }

    /// Get the server's ID.
    pub async fn get_server_id(&mut self) -> Result<Option<NonZeroU32>, Error> {
        let response = self.inner.get_server_id(GetServerIdRequest {}).await?;
        let maybe_id = response.get_ref().id.try_into().ok();
        Ok(maybe_id)
    }
}
