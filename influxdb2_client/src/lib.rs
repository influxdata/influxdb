#![deny(rustdoc::broken_intra_doc_links, rustdoc::bare_urls, rust_2018_idioms)]
// `clippy::use_self` is deliberately excluded from the lints this crate uses.
// See <https://github.com/rust-lang/rust-clippy/issues/6902>.
#![warn(
    missing_copy_implementations,
    missing_debug_implementations,
    missing_docs,
    clippy::explicit_iter_loop,
    clippy::clone_on_ref_ptr,
    // See https://github.com/influxdata/influxdb_iox/pull/1671
    clippy::future_not_send,
    clippy::todo,
    clippy::dbg_macro,
    unused_crate_dependencies
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
//!     client.create_bucket(
//!         Some(PostBucketRequest::new(org_id.to_string(), bucket.to_string()))
//!     ).await?;
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

// Workaround for "unused crate" lint false positives.
#[cfg(test)]
use once_cell as _;
#[cfg(test)]
use parking_lot as _;
#[cfg(test)]
use test_helpers as _;

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

    /// While deserializing the response as JSON, something went wrong.
    #[snafu(display("Could not deserialize as JSON. Error: {source}\nText: `{text}`"))]
    DeserializingJsonResponse {
        /// The text of the response
        text: String,
        /// The underlying error object from serde
        source: serde_json::Error,
    },

    /// Something went wrong getting the raw bytes of the response
    #[snafu(display("Could not get response bytes: {source}"))]
    ResponseBytes {
        /// The underlying error object from reqwest
        source: reqwest::Error,
    },

    /// Something went wrong converting the raw bytes of the response to a UTF-8 string
    #[snafu(display("Invalid UTF-8: {source}"))]
    ResponseString {
        /// The underlying error object from std
        source: std::string::FromUtf8Error,
    },
}

/// Client to a server supporting the InfluxData 2.0 API.
#[derive(Debug, Clone)]
pub struct Client {
    /// The base URL this client sends requests to
    pub url: String,
    auth_header: Option<String>,
    reqwest: reqwest::Client,
    jaeger_debug_header: Option<String>,
}

impl Client {
    /// Default [jaeger debug header](Self::with_jaeger_debug) that should work in many
    /// environments.
    pub const DEFAULT_JAEGER_DEBUG_HEADER: &'static str = "jaeger-debug-id";

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
            Some(format!("Token {token}"))
        };

        Self {
            url: url.into(),
            auth_header,
            reqwest: reqwest::Client::builder()
                .connection_verbose(true)
                .build()
                .expect("reqwest::Client should have built"),
            jaeger_debug_header: None,
        }
    }

    /// Enable generation of jaeger debug headers with the given header name.
    pub fn with_jaeger_debug(self, header: String) -> Self {
        Self {
            jaeger_debug_header: Some(header),
            ..self
        }
    }

    /// Consolidate common request building code
    fn request(&self, method: Method, url: &str) -> reqwest::RequestBuilder {
        let mut req = self.reqwest.request(method, url);

        if let Some(auth) = &self.auth_header {
            req = req.header("Authorization", auth);
        }
        if let Some(header) = &self.jaeger_debug_header {
            req = req.header(header, format!("influxdb_client-{}", uuid::Uuid::new_v4()));
        }

        req
    }
}

pub mod common;

pub mod api;
pub mod models;
