use thiserror::Error;

use self::generated_types::{delete_service_client::DeleteServiceClient, *};

use crate::connection::Connection;

/// Re-export generated_types
pub mod generated_types {
    pub use generated_types::influxdata::iox::delete::v1::*;
}

/// Errors returned by [`Client::delete`]
#[derive(Debug, Error)]
pub enum DeleteError {
    /// Database not found
    #[error("Not found: {}", .0)]
    NotFound(String),

    /// Response contained no payload
    #[error("Server returned an empty response")]
    EmptyResponse,

    /// Server indicated that it is not (yet) available
    #[error("Server unavailable: {}", .0.message())]
    Unavailable(tonic::Status),

    /// Client received an unexpected error from the server
    #[error("Unexpected server error: {}: {}", .0.code(), .0.message())]
    ServerError(tonic::Status),
}

/// An IOx Delete API client.
///
/// This client wraps the underlying `tonic` generated client with a
/// more ergonomic interface.
///
/// ```no_run
/// #[tokio::main]
/// # async fn main() {
/// use influxdb_iox_client::{
///     delete::Client,
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
///     .delete(
///         "my_db",
///         "my_table",
///         "100",
///         "200",
///         "A = 1",
///     )
///     .await
///     .expect("failed to delete data");
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct Client {
    inner: DeleteServiceClient<Connection>,
}

impl Client {
    /// Creates a new client with the provided connection
    pub fn new(channel: Connection) -> Self {
        Self {
            inner: DeleteServiceClient::new(channel),
        }
    }

    /// Delete data from a table on a specified predicate
    pub async fn delete(
        &mut self,
        db_name: impl Into<String> + Send,
        table_name: impl Into<String> + Send,
        start_time: impl Into<String> + Send,
        stop_time: impl Into<String> + Send,
        predicate: impl Into<String> + Send,
    ) -> Result<(), DeleteError> {
        let db_name = db_name.into();
        let table_name = table_name.into();
        let start_time = start_time.into();
        let stop_time = stop_time.into();
        let predicate = predicate.into();

        self.inner
            .delete(DeleteRequest {
                db_name,
                table_name,
                start_time,
                stop_time,
                predicate,
            })
            .await
            .map_err(|status| match status.code() {
                tonic::Code::NotFound => DeleteError::NotFound(status.message().to_string()),
                tonic::Code::Unavailable => DeleteError::Unavailable(status),
                _ => DeleteError::ServerError(status),
            })?;

        Ok(())
    }
}
