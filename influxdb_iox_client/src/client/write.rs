/// Re-export generated_types
pub mod generated_types {
    pub use generated_types::influxdata::pbdata::v1::*;
}

use client_util::{connection::HttpConnection, namespace_translation::split_namespace};

use crate::{
    connection::Connection,
    error::{translate_response, Error},
};
use reqwest::Method;

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
///     .build("http://127.0.0.1:8080")
///     .await
///     .unwrap();
///
/// let mut client = Client::new(connection);
///
/// // write a line of line procol data
/// client
///     .write_lp("bananas", "cpu,region=west user=23.2 100")
///     .await
///     .expect("failed to write to IOx");
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct Client {
    inner: HttpConnection,
}

impl Client {
    /// Creates a new client with the provided connection
    pub fn new(connection: Connection) -> Self {
        Self {
            inner: connection.into_http_connection(),
        }
    }

    /// Write the [LineProtocol] formatted data in `lp_data` to
    /// namespace `namespace`.
    ///
    /// Returns the number of bytes which were written to the database
    ///
    /// [LineProtocol]: https://docs.influxdata.com/influxdb/v2.0/reference/syntax/line-protocol/#data-types-and-format
    pub async fn write_lp(
        &mut self,
        namespace: impl AsRef<str> + Send,
        lp_data: impl Into<String> + Send,
    ) -> Result<usize, Error> {
        let lp_data = lp_data.into();
        let data_len = lp_data.len();

        let write_url = format!("{}api/v2/write", self.inner.uri());

        let (org_id, bucket_id) = split_namespace(namespace.as_ref()).map_err(|e| {
            Error::invalid_argument(
                "namespace",
                format!("Could not find valid org_id and bucket_id: {}", e),
            )
        })?;

        let response = self
            .inner
            .client()
            .request(Method::POST, &write_url)
            .query(&[("bucket", bucket_id), ("org", org_id)])
            .body(lp_data)
            .send()
            .await
            .map_err(Error::client)?;

        translate_response(response).await?;

        Ok(data_len)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connection::Builder;

    #[tokio::test]
    /// Ensure the basic plumbing is hooked up correctly
    async fn basic() {
        let url = mockito::server_url();

        let connection = Builder::new().build(&url).await.unwrap();

        let namespace = "orgname_bucketname";
        let data = "m,t=foo f=4";

        let m = mockito::mock("POST", "/api/v2/write?bucket=bucketname&org=orgname")
            .with_status(201)
            .match_body(data)
            .create();

        let res = Client::new(connection).write_lp(namespace, data).await;

        m.assert();

        let num_bytes = res.expect("Error making write request");
        assert_eq!(num_bytes, 11);
    }
}
