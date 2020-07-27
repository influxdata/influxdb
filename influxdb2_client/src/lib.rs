#![deny(rust_2018_idioms)]
#![warn(
    missing_copy_implementations,
    missing_debug_implementations,
    missing_docs,
    clippy::explicit_iter_loop,
    clippy::use_self
)]

//! # influxdb2_client
//!
//! This is a Rust client to InfluxDB using the [2.0 API][2api].
//!
//! [2api]: https://v2.docs.influxdata.com/v2.0/reference/api/
//!
//! ## Work Remaining
//!
//! - Query
//! - Authentication
//! - optional sync client
//! - Influx 1.x API?
//! - Other parts of the API
//! - Pick the best name to use on crates.io and publish
//!
//! ## Quick start
//!
//! This example creates a client to an InfluxDB server running at `http://localhost:8888`, builds
//! two points, and writes them to InfluxDB in the organization with ID `0000111100001111` and the
//! bucket with the ID `1111000011110000`.
//!
//! ```
//! async fn example() -> Result<(), Box<dyn std::error::Error>> {
//!     use influxdb2_client::{Client, DataPoint};
//!     use futures::stream;
//!
//!     let client = Client::new("http://localhost:8888");
//!     let points = vec![
//!         DataPoint::builder("cpu")
//!             .tag("host", "server01")
//!             .field("usage", 0.5)
//!             .build()?,
//!         DataPoint::builder("cpu")
//!             .tag("host", "server01")
//!             .tag("region", "us-west")
//!             .field("usage", 0.87)
//!             .build()?,
//!     ];
//!
//!     let org_id = "0000111100001111";
//!     let bucket_id = "1111000011110000";
//!
//!     client.write(org_id, bucket_id, stream::iter(points)).await?;
//!     Ok(())
//! }
//! ```

use bytes::buf::ext::BufMutExt;
use futures::{Stream, StreamExt};
use reqwest::Body;
use snafu::{ResultExt, Snafu};
use std::io::{self, Write};

mod data_point;
pub use data_point::{DataPoint, FieldValue, WriteDataPoint};

/// Errors that occur while making requests to the Influx server.
#[derive(Debug, Snafu)]
pub enum RequestError {
    /// While making a request to the Influx server, the underlying `reqwest` library returned an
    /// error that was not an HTTP 400 or 500.
    #[snafu(display("Error while processing the HTTP request: {}", source))]
    ReqwestProcessing {
        /// The underlying error object from `reqwest`.
        source: reqwest::Error,
    },
    /// The underlying `reqwest` library returned an HTTP error with code 400 (meaning a client
    /// error) or 500 (meaning a server error).
    #[snafu(display("HTTP request returned an error: {}, `{}`", status, text))]
    Http {
        /// The `StatusCode` returned from the request
        status: reqwest::StatusCode,
        /// Any text data returned from the request
        text: String,
    },
}

/// Client to a server supporting the InfluxData 2.0 API.
#[derive(Debug, Clone)]
pub struct Client {
    url: String,
    reqwest: reqwest::Client,
}

impl Client {
    /// Create a new client pointing to the URL specified in `protocol://server:port` format.
    ///
    /// # Example
    ///
    /// ```
    /// let client = influxdb2_client::Client::new("http://localhost:8888");
    /// ```
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            reqwest: reqwest::Client::new(),
        }
    }

    /// Write line protocol data to the specified organization and bucket.
    pub async fn write_line_protocol(
        &self,
        org_id: &str,
        bucket_id: &str,
        body: impl Into<Body>,
    ) -> Result<(), RequestError> {
        let body = body.into();
        let write_url = format!("{}/api/v2/write", self.url);

        let response = self
            .reqwest
            .post(&write_url)
            .query(&[("bucket", bucket_id), ("org", org_id)])
            .body(body)
            .send()
            .await
            .context(ReqwestProcessing)?;

        if !response.status().is_success() {
            let status = response.status();
            let text = response.text().await.context(ReqwestProcessing)?;
            Http { status, text }.fail()?;
        }

        Ok(())
    }

    /// Write a `Stream` of `DataPoint`s to the specified organization and bucket.
    pub async fn write(
        &self,
        org_id: &str,
        bucket_id: &str,
        body: impl Stream<Item = impl WriteDataPoint> + Send + Sync + 'static,
    ) -> Result<(), RequestError> {
        let mut buffer = bytes::BytesMut::new();

        let body = body.map(move |point| {
            let mut w = (&mut buffer).writer();
            point.write_data_point_to(&mut w)?;
            w.flush()?;
            Ok::<_, io::Error>(buffer.split().freeze())
        });

        let body = Body::wrap_stream(body);

        Ok(self.write_line_protocol(org_id, bucket_id, body).await?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::stream;
    use mockito::mock;

    type Error = Box<dyn std::error::Error>;
    type Result<T = (), E = Error> = std::result::Result<T, E>;

    #[tokio::test]
    async fn writing_points() -> Result {
        let org_id = "0000111100001111";
        let bucket_id = "1111000011110000";

        let mock_server = mock(
            "POST",
            format!("/api/v2/write?bucket={}&org={}", bucket_id, org_id).as_str(),
        )
        .match_body(
            "\
cpu,host=server01 usage=0.5
cpu,host=server01,region=us-west usage=0.87
",
        )
        .create();

        let client = Client::new(&mockito::server_url());

        let points = vec![
            DataPoint::builder("cpu")
                .tag("host", "server01")
                .field("usage", 0.5)
                .build()?,
            DataPoint::builder("cpu")
                .tag("host", "server01")
                .tag("region", "us-west")
                .field("usage", 0.87)
                .build()?,
        ];

        // If the requests made are incorrect, Mockito returns status 501 and `write` will return
        // an error, which causes the test to fail here instead of when we assert on mock_server.
        // The error messages that Mockito provides are much clearer for explaining why a test
        // failed than just that the server returned 501, so don't use `?` here.
        let _result = client.write(org_id, bucket_id, stream::iter(points)).await;

        mock_server.assert();
        Ok(())
    }
}
