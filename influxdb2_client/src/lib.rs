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
//! This is a Rust client to InfluxDB using the 2.0 API.
//! that implements the API in https://v2.docs.influxdata.com/v2.0/reference/api/
//!
//! ## Work Remaining
//!
//! - Write
//! - Query
//! - Authentication
//! - optional sync client
//! - Influx 1.x API?
//! - Other parts of the API
//! - Pick the best name to use on crates.io and publish

use snafu::{ResultExt, Snafu};

/// Public error type.
#[derive(Debug, Snafu)]
pub struct Error(InternalError);

#[derive(Debug, Snafu)]
enum InternalError {
    #[snafu(display("Error while processing the HTTP request: {}", source))]
    ReqwestProcessing { source: reqwest::Error },
    #[snafu(display("HTTP request returned an error: {}, `{}`", status, text))]
    Http {
        status: reqwest::StatusCode,
        text: String,
    },
}

/// A specialized `Result` for `influxdb2_client` errors.
pub type Result<T, E = Error> = std::result::Result<T, E>;

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
    pub async fn write(
        &self,
        org_id: &str,
        bucket_id: &str,
        body: impl Into<String>,
    ) -> Result<()> {
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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let _client = Client::new("http://localhost:8888");
    }
}
