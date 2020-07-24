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

use bytes::Bytes;
use futures::{Stream, StreamExt};
use reqwest::Body;
use snafu::{ensure, ResultExt, Snafu};
use std::{collections::BTreeMap, convert::Infallible, fmt};

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

/// Errors that occur while building `DataPoint`s
#[derive(Debug, Snafu)]
pub enum DataPointError {
    /// Returned when calling `build` on a `DataPointBuilder` that has no fields.
    #[snafu(display(
        "All `DataPoints` must have at least one field. Builder contains: {:?}",
        data_point_builder
    ))]
    AtLeastOneFieldRequired {
        /// The current state of the `DataPointBuilder`
        data_point_builder: DataPointBuilder,
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
        body: impl Stream<Item = DataPoint> + Send + Sync + 'static,
    ) -> Result<(), RequestError> {
        let body = body
            .map(|dp| dp.line_protocol().to_string())
            .map(Bytes::from)
            .map(Ok::<_, Infallible>);
        let body = Body::wrap_stream(body);

        Ok(self.write_line_protocol(org_id, bucket_id, body).await?)
    }
}

/// Incrementally constructs a `DataPoint`.
///
/// Create this via `DataPoint::builder`.
#[derive(Debug)]
pub struct DataPointBuilder {
    measurement: String,
    // Keeping the tags sorted improves performance on the server side
    tags: BTreeMap<String, String>,
    fields: BTreeMap<String, FieldValue>,
    timestamp: Option<i64>,
}

impl DataPointBuilder {
    fn new(measurement: impl Into<String>) -> Self {
        Self {
            measurement: measurement.into(),
            tags: Default::default(),
            fields: Default::default(),
            timestamp: Default::default(),
        }
    }

    /// Sets a tag, replacing any existing tag of the same name.
    pub fn tag(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.tags.insert(name.into(), value.into());
        self
    }

    /// Sets a field, replacing any existing field of the same name.
    pub fn field(mut self, name: impl Into<String>, value: impl Into<FieldValue>) -> Self {
        self.fields.insert(name.into(), value.into());
        self
    }

    /// Sets the timestamp, replacing any existing timestamp.
    ///
    /// The value is treated as the number of nanoseconds since the
    /// UNIX epoch.
    pub fn timestamp(mut self, value: i64) -> Self {
        self.timestamp = Some(value);
        self
    }

    /// Constructs the data point
    pub fn build(self) -> Result<DataPoint, DataPointError> {
        ensure!(
            !self.fields.is_empty(),
            AtLeastOneFieldRequired {
                data_point_builder: self
            }
        );

        let Self {
            measurement,
            tags,
            fields,
            timestamp,
        } = self;

        Ok(DataPoint {
            measurement,
            tags,
            fields,
            timestamp,
        })
    }
}

/// A single point of information to send to InfluxDB.
// TODO: If we want to support non-UTF-8 data, all `String`s stored in `DataPoint` would need
// to be `Vec<u8>` instead, the API for creating a `DataPoint` would need some more consideration,
// and there would need to be more `Write*` trait implementations. Because the `Write*` traits work
// on a writer of bytes, that part of the design supports non-UTF-8 data now.
#[derive(Debug)]
pub struct DataPoint {
    measurement: String,
    tags: BTreeMap<String, String>,
    fields: BTreeMap<String, FieldValue>,
    timestamp: Option<i64>,
}

impl DataPoint {
    /// Create a builder to incrementally construct a `DataPoint`.
    pub fn builder(measurement: impl Into<String>) -> DataPointBuilder {
        DataPointBuilder::new(measurement)
    }

    fn line_protocol(&self) -> LineProtocol<'_> {
        LineProtocol(self)
    }
}

/// The `LineProtocol` struct exists (and is deliberately) private because line protocol
/// isn't guaranteed to be UTF-8, unlike Rust `String`s.
/// Some future version of this library may support creating LineProtocol
/// with data that's not UTF-8
struct LineProtocol<'a>(&'a DataPoint);

impl fmt::Display for LineProtocol<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0.measurement)?;

        for (k, v) in &self.0.tags {
            write!(f, ",{}={}", k, v)?;
        }

        for (i, (k, v)) in self.0.fields.iter().enumerate() {
            let d = if i == 0 { " " } else { "," };
            write!(f, "{}{}={}", d, k, v)?;
        }

        if let Some(ts) = self.0.timestamp {
            write!(f, " {}", ts)?;
        }

        Ok(())
    }
}

/// Possible value types
#[derive(Debug, Clone)]
pub enum FieldValue {
    /// A true or false value
    Bool(bool),
    /// A 64-bit floating point number
    F64(f64),
    /// A 64-bit signed integer number
    I64(i64),
    /// A string value
    String(String),
}

impl From<bool> for FieldValue {
    fn from(other: bool) -> Self {
        Self::Bool(other)
    }
}

impl From<f64> for FieldValue {
    fn from(other: f64) -> Self {
        Self::F64(other)
    }
}

impl From<i64> for FieldValue {
    fn from(other: i64) -> Self {
        Self::I64(other)
    }
}

impl From<&str> for FieldValue {
    fn from(other: &str) -> Self {
        Self::String(other.into())
    }
}

impl From<String> for FieldValue {
    fn from(other: String) -> Self {
        Self::String(other.into())
    }
}

impl fmt::Display for FieldValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use FieldValue::*;

        match self {
            Bool(v) => write!(f, "{}", if *v { "t" } else { "f" }),
            F64(v) => write!(f, "{}", v),
            I64(v) => write!(f, "{}i", v),
            String(v) => write!(f, r#""{}""#, v),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    type Error = Box<dyn std::error::Error>;
    type Result<T = (), E = Error> = std::result::Result<T, E>;

    #[test]
    fn it_works() {
        let _client = Client::new("http://localhost:8888");
    }

    #[test]
    fn point_builder_allows_setting_tags_and_fields() -> Result {
        let point = DataPoint::builder("swap")
            .tag("host", "server01")
            .tag("name", "disk0")
            .field("in", 3_i64)
            .field("out", 4_i64)
            .timestamp(1)
            .build()?;

        assert_eq!(
            point.line_protocol().to_string(),
            "swap,host=server01,name=disk0 in=3i,out=4i 1",
        );

        Ok(())
    }

    #[test]
    fn no_tags_or_timestamp() -> Result {
        let point = DataPoint::builder("m0")
            .field("f0", 1.0)
            .field("f1", 2_i64)
            .build()?;

        assert_eq!(point.line_protocol().to_string(), "m0 f0=1,f1=2i");

        Ok(())
    }

    #[test]
    fn no_timestamp() -> Result {
        let point = DataPoint::builder("m0")
            .tag("t0", "v0")
            .tag("t1", "v1")
            .field("f1", 2_i64)
            .build()?;

        assert_eq!(point.line_protocol().to_string(), "m0,t0=v0,t1=v1 f1=2i");

        Ok(())
    }

    #[test]
    fn no_field() {
        let point_result = DataPoint::builder("m0").build();

        assert!(point_result.is_err());
    }

    const ALL_THE_DELIMITERS: &str = r#"alpha,beta=delta gamma"epsilon"#;

    #[test]
    fn special_characters_are_escaped_in_measurements() {
        assert_eq!(ALL_THE_DELIMITERS, r#"alpha\,beta=delta\ gamma"epsilon"#);
    }

    #[test]
    fn special_characters_are_escaped_in_tag_keys() {
        assert_eq!(ALL_THE_DELIMITERS, r#"alpha\,beta\=delta\ gamma"epsilon"#);
    }

    #[test]
    fn special_characters_are_escaped_in_tag_values() {
        assert_eq!(ALL_THE_DELIMITERS, r#"alpha\,beta\=delta\ gamma"epsilon"#);
    }

    #[test]
    fn special_characters_are_escaped_in_field_keys() {
        assert_eq!(ALL_THE_DELIMITERS, r#"alpha\,beta\=delta\ gamma"epsilon"#);
    }

    #[test]
    fn special_characters_are_escaped_in_field_values_of_strings() {
        assert_eq!(ALL_THE_DELIMITERS, r#"alpha,beta=delta gamma\"epsilon"#);
    }

    #[test]
    fn field_value_of_bool() {
        let e = FieldValue::from(true);
        assert_eq!(e.to_string(), "t");

        let e = FieldValue::from(false);
        assert_eq!(e.to_string(), "f");
    }

    #[test]
    fn field_value_of_float() {
        let e = FieldValue::from(42_f64);
        assert_eq!(e.to_string(), "42");
    }

    #[test]
    fn field_value_of_integer() {
        let e = FieldValue::from(42_i64);
        assert_eq!(e.to_string(), "42i");
    }

    #[test]
    fn field_value_of_string() {
        let e = FieldValue::from("hello");
        assert_eq!(e.to_string(), r#""hello""#);
    }
}
