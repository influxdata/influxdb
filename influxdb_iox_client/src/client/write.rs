use thiserror::Error;

use self::generated_types::{write_service_client::WriteServiceClient, *};

use crate::connection::Connection;

/// Re-export generated_types
pub mod generated_types {
    pub use generated_types::influxdata::iox::write::v1::*;
}

/// Errors returned by Client::write_data
#[derive(Debug, Error)]
pub enum WriteError {
    /// Client received an unexpected error from the server
    #[error("Unexpected server error: {}: {}", .0.code(), .0.message())]
    ServerError(tonic::Status),
}

/// An IOx Write API client.
///
/// ```no_run
/// #[tokio::main]
/// # async fn main() {
/// use influxdb_iox_client::{
///     write::Client,
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
/// // write a line of line procol data
/// client
///     .write("bananas", "cpu,region=west user=23.2 100")
///     .await
///     .expect("failed to create database");
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct Client {
    inner: WriteServiceClient<Connection>,
}

impl Client {
    /// Creates a new client with the provided connection
    pub fn new(channel: tonic::transport::Channel) -> Self {
        Self {
            inner: WriteServiceClient::new(channel),
        }
    }

    /// Write the [LineProtocol] formatted data in `lp_data` to
    /// database `name`. Returns the number of lines which were parsed
    /// and written to the database
    ///
    /// [LineProtocol]: https://docs.influxdata.com/influxdb/v2.0/reference/syntax/line-protocol/#data-types-and-format
    pub async fn write(
        &mut self,
        db_name: impl Into<String>,
        lp_data: impl Into<String>,
    ) -> Result<usize, WriteError> {
        let db_name = db_name.into();
        let lp_data = lp_data.into();
        let response = self
            .inner
            .write(WriteRequest { db_name, lp_data })
            .await
            .map_err(WriteError::ServerError)?;

        Ok(response.into_inner().lines_written as usize)
    }

    /// Write an [Entry] to database `name`.
    ///
    /// An Entry unit of write payload encoded as Flatbuffer structure
    /// and passed as a bytes field in the gRPC protobuf API.
    ///
    /// [Entry]: https://github.com/influxdata/influxdb_iox/blob/main/entry/src/entry.fbs
    pub async fn write_entry(
        &mut self,
        db_name: impl Into<String>,
        entry: impl Into<Vec<u8>>,
    ) -> Result<(), WriteError> {
        let db_name = db_name.into();
        let entry = entry.into();
        self.inner
            .write_entry(WriteEntryRequest { db_name, entry })
            .await
            .map_err(WriteError::ServerError)?;

        Ok(())
    }
}
