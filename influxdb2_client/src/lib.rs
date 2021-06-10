#![deny(broken_intra_doc_links, rust_2018_idioms)]
#![warn(
    missing_copy_implementations,
    missing_debug_implementations,
    missing_docs,
    clippy::explicit_iter_loop,
    clippy::use_self,
    clippy::clone_on_ref_ptr,
    clippy::future_not_send
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
//! a bucket with the name "mybucket" in the organization with name "myorg" and
//! ID "0000111100001111", builds two points, and writes the points to the
//! bucket.
//!
//! ```
//! async fn example() -> Result<(), Box<dyn std::error::Error>> {
//!     use influxdb2_client::Client;
//!     use influxdb2_client::models::{DataPoint, PostBucketRequest};
//!     use futures::stream;
//!
//!     let org = "myorg";
//!     let org_id = "0000111100001111";
//!     let bucket = "mybucket";
//!
//!     let client = Client::new("http://localhost:8888", "some-token");
//!
//!     client.create_bucket(Some(PostBucketRequest::new(org_id.to_string(), bucket.to_string()))).await?;
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

use reqwest::Method;
use snafu::Snafu;

/// Errors that occur while making requests to the Influx server.
#[derive(Debug, Snafu)]
pub enum RequestError {
    /// While making a request to the Influx server, the underlying `reqwest`
    /// library returned an error that was not an HTTP 400 or 500.
    #[snafu(display("Error while processing the HTTP request: {}", source))]
    ReqwestProcessing {
        /// The underlying error object from `reqwest`.
        source: reqwest::Error,
    },
    /// The underlying `reqwest` library returned an HTTP error with code 400
    /// (meaning a client error) or 500 (meaning a server error).
    #[snafu(display("HTTP request returned an error: {}, `{}`", status, text))]
    Http {
        /// The `StatusCode` returned from the request
        status: reqwest::StatusCode,
        /// Any text data returned from the request
        text: String,
    },

    /// While serializing data as JSON to send in a request, the underlying
    /// `serde_json` library returned an error.
    #[snafu(display("Error while serializing to JSON: {}", source))]
    Serializing {
        /// The underlying error object from `serde_json`.
        source: serde_json::error::Error,
    },
}

/// Client to a server supporting the InfluxData 2.0 API.
#[derive(Debug, Clone)]
pub struct Client {
    /// The base URL this client sends requests to
    pub url: String,
    auth_header: Option<String>,
    reqwest: reqwest::Client,
}

impl Client {
    /// Create a new client pointing to the URL specified in
    /// `protocol://server:port` format and using the specified token for
    /// authorization.
    ///
    /// # Example
    ///
    /// ```
    /// let client = influxdb2_client::Client::new("http://localhost:8888", "my-token");
    /// ```
    pub fn new(url: impl Into<String>, auth_token: impl Into<String>) -> Self {
        let token = auth_token.into();
        let auth_header = if token.is_empty() {
            None
        } else {
            Some(format!("Token {}", token))
        };

        Self {
            url: url.into(),
            auth_header,
            reqwest: reqwest::Client::new(),
        }
    }

    /// Consolidate common request building code
    fn request(&self, method: Method, url: &str) -> reqwest::RequestBuilder {
        let mut req = self.reqwest.request(method, url);

        if let Some(auth) = &self.auth_header {
            req = req.header("Authorization", auth);
        }

        req
    }
}

pub mod common;

pub mod api;
pub mod models;
