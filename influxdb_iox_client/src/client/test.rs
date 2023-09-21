use client_util::connection::GrpcConnection;
/// Re-export generated_types
use generated_types::{i_ox_testing_client::IOxTestingClient, TestErrorRequest};

use crate::connection::Connection;
use crate::error::Error;

/// A client for testing purposes
///
/// ```no_run
/// #[tokio::main]
/// # async fn main() {
/// use influxdb_iox_client::{
///     test::Client,
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
/// // trigger an error
/// client
///     .error()
///     .await
///     .expect("failed to trigger an error");
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct Client {
    inner: IOxTestingClient<GrpcConnection>,
}

impl Client {
    /// Creates a new client with the provided connection
    pub fn new(connection: Connection) -> Self {
        Self {
            inner: IOxTestingClient::new(connection.into_grpc_connection()),
        }
    }

    /// Trigger an error.
    pub async fn error(&mut self) -> Result<(), Error> {
        let request = TestErrorRequest {};
        self.inner.test_error(request).await?;
        Ok(())
    }
}
