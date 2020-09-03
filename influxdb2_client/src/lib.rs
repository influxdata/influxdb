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
//! - optional sync client
//! - Influx 1.x API?
//! - Other parts of the API
//! - Pick the best name to use on crates.io and publish
//!
//! ## Quick start
//!
//! This example creates a client to an InfluxDB server running at `http://localhost:8888`, creates
//! a bucket with the name "mybucket" in the organization with name "myorg" and ID
//! "0000111100001111", builds two points, and writes the points to the bucket.
//!
//! ```
//! async fn example() -> Result<(), Box<dyn std::error::Error>> {
//!     use influxdb2_client::{Client, DataPoint};
//!     use futures::stream;
//!
//!     let org = "myorg";
//!     let org_id = "0000111100001111";
//!     let bucket = "mybucket";
//!
//!     let client = Client::new("http://localhost:8888", "some-token");
//!
//!     client.create_bucket(org_id, bucket).await?;
//!
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
//!     client.write(org, bucket, stream::iter(points)).await?;
//!     Ok(())
//! }
//! ```

use bytes::buf::ext::BufMutExt;
use futures::{Stream, StreamExt};
use reqwest::{Body, Method};
use serde::Serialize;
use snafu::{ResultExt, Snafu};
use std::{
    fmt,
    io::{self, Write},
};

pub mod data_point;
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

    /// While serializing data as JSON to send in a request, the underlying `serde_json` library
    /// returned an error.
    #[snafu(display("Error while serializing to JSON: {}", source))]
    Serializing {
        /// The underlying error object from `serde_json`.
        source: serde_json::error::Error,
    },
}

/// Client to a server supporting the InfluxData 2.0 API.
#[derive(Debug, Clone)]
pub struct Client {
    url: String,
    auth_header: String,
    reqwest: reqwest::Client,
}

impl Client {
    /// Create a new client pointing to the URL specified in `protocol://server:port` format and
    /// using the specified token for authorization.
    ///
    /// # Example
    ///
    /// ```
    /// let client = influxdb2_client::Client::new("http://localhost:8888", "my-token");
    /// ```
    pub fn new(url: impl Into<String>, auth_token: impl fmt::Display) -> Self {
        Self {
            url: url.into(),
            auth_header: format!("Token {}", auth_token),
            reqwest: reqwest::Client::new(),
        }
    }

    /// Consolidate common request building code
    fn request(&self, method: Method, url: &str) -> reqwest::RequestBuilder {
        self.reqwest
            .request(method, url)
            .header("Authorization", &self.auth_header)
    }

    /// Write line protocol data to the specified organization and bucket.
    pub async fn write_line_protocol(
        &self,
        org: &str,
        bucket: &str,
        body: impl Into<Body>,
    ) -> Result<(), RequestError> {
        let body = body.into();
        let write_url = format!("{}/api/v2/write", self.url);

        let response = self
            .request(Method::POST, &write_url)
            .query(&[("bucket", bucket), ("org", org)])
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
        org: &str,
        bucket: &str,
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

        Ok(self.write_line_protocol(org, bucket, body).await?)
    }

    /// Create a new bucket in the organization specified by the 16-digit hexadecimal `org_id` and
    /// with the bucket name `bucket`.
    pub async fn create_bucket(&self, org_id: &str, bucket: &str) -> Result<(), RequestError> {
        let create_bucket_url = format!("{}/api/v2/buckets", self.url);

        #[derive(Serialize, Debug, Default)]
        struct CreateBucketInfo {
            #[serde(rename = "orgID")]
            org_id: String,
            name: String,
            #[serde(rename = "retentionRules")]
            // The type of `retentionRules` isn't `String`; this is included and always set to
            // an empty vector to be compatible with the Influx 2.0 API where `retentionRules` is
            // a required parameter. Delorean ignores this parameter.
            retention_rules: Vec<String>,
        }

        let body = CreateBucketInfo {
            org_id: org_id.into(),
            name: bucket.into(),
            ..Default::default()
        };

        let response = self
            .request(Method::POST, &create_bucket_url)
            .body(serde_json::to_string(&body).context(Serializing)?)
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
        let org = "some-org";
        let bucket = "some-bucket";
        let token = "some-token";

        let mock_server = mock(
            "POST",
            format!("/api/v2/write?bucket={}&org={}", bucket, org).as_str(),
        )
        .match_header("Authorization", format!("Token {}", token).as_str())
        .match_body(
            "\
cpu,host=server01 usage=0.5
cpu,host=server01,region=us-west usage=0.87
",
        )
        .create();

        let client = Client::new(&mockito::server_url(), token);

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
        let _result = client.write(org, bucket, stream::iter(points)).await;

        mock_server.assert();
        Ok(())
    }

    #[tokio::test]
    async fn create_bucket() -> Result {
        let org_id = "0000111100001111";
        let bucket = "some-bucket";
        let token = "some-token";

        let mock_server = mock("POST", "/api/v2/buckets")
            .match_header("Authorization", format!("Token {}", token).as_str())
            .match_body(
                format!(
                    r#"{{"orgID":"{}","name":"{}","retentionRules":[]}}"#,
                    org_id, bucket
                )
                .as_str(),
            )
            .create();

        let client = Client::new(&mockito::server_url(), token);

        let _result = client.create_bucket(org_id, bucket).await;

        mock_server.assert();
        Ok(())
    }
}
