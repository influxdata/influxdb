/// Re-export generated_types
pub mod generated_types {
    pub use generated_types::influxdata::pbdata::v1::*;
}

use self::generated_types::write_service_client::WriteServiceClient;

use crate::connection::Connection;
use crate::error::Error;

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
///     .write_lp("bananas", "cpu,region=west user=23.2 100",0)
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
    pub fn new(channel: Connection) -> Self {
        Self {
            inner: WriteServiceClient::new(channel),
        }
    }

    /// Write the [LineProtocol] formatted data in `lp_data` to
    /// database `name`. Lines without a timestamp will be assigned `default_time`
    ///
    /// Returns the number of lines which were parsed and written to the database
    ///
    /// [LineProtocol]: https://docs.influxdata.com/influxdb/v2.0/reference/syntax/line-protocol/#data-types-and-format
    #[cfg(feature = "write_lp")]
    pub async fn write_lp(
        &mut self,
        db_name: impl AsRef<str> + Send,
        lp_data: impl AsRef<str> + Send,
        default_time: i64,
    ) -> Result<usize, Error> {
        let tables = mutable_batch_lp::lines_to_batches(lp_data.as_ref(), default_time)
            .map_err(|e| Error::Client(Box::new(e)))?;

        let meta = dml::DmlMeta::unsequenced(None);
        let write = dml::DmlWrite::new(db_name.as_ref().to_string(), tables, meta);
        let lines = write.tables().map(|(_, table)| table.rows()).sum();

        let database_batch = mutable_batch_pb::encode::encode_write(db_name.as_ref(), &write);

        self.inner
            .write(generated_types::WriteRequest {
                database_batch: Some(database_batch),
            })
            .await?;

        Ok(lines)
    }

    /// Write a protobuf batch.
    pub async fn write_pb(
        &mut self,
        write_request: generated_types::WriteRequest,
    ) -> Result<tonic::Response<generated_types::WriteResponse>, Error> {
        Ok(self.inner.write(write_request).await?)
    }
}
