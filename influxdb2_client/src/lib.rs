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
//! - Write
//! - Query
//! - Authentication
//! - optional sync client
//! - Influx 1.x API?
//! - Other parts of the API
//! - Pick the best name to use on crates.io and publish

use bytes::Bytes;
use futures::{Stream, StreamExt};
use reqwest::Body;
use snafu::{ResultExt, Snafu};
use std::{cmp, collections::BTreeMap, convert::Infallible, fmt, marker::PhantomData};

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

    /// Write line protocol data to the specified organization and bucket.
    pub async fn write2(
        &self,
        org_id: &str,
        bucket_id: &str,
        body: impl Stream<Item = DataPoint> + Send + Sync + 'static,
    ) -> Result<()> {
        let write_url = format!("{}/api/v2/write", self.url);

        let body = body
            .map(|dp| dp.line_protocol().to_string())
            .map(Bytes::from)
            .map(Ok::<_, Infallible>);
        let body = Body::wrap_stream(body);

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

/// Incrementally constructs a `DataPoint`.
///
/// Create this via `DataPoint::builder`.
#[derive(Debug)]
pub struct DataPointBuilder {
    measurement: EscapedMeasurement,
    // Keeping the tags sorted improves performance on the server side
    tags: BTreeMap<EscapedTagKey, EscapedTagKey>,
    fields: BTreeMap<EscapedFieldKey, FieldValue>,
    timestamp: Option<u64>,
}

impl DataPointBuilder {
    fn new(measurement: impl Into<EscapedMeasurement>) -> Self {
        Self {
            measurement: measurement.into(),
            tags: Default::default(),
            fields: Default::default(),
            timestamp: Default::default(),
        }
    }

    /// Sets a tag, replacing any existing tag of the same name.
    pub fn tag(
        mut self,
        name: impl Into<EscapedTagKey>,
        value: impl Into<EscapedTagValue>,
    ) -> Self {
        self.tags.insert(name.into(), value.into());
        self
    }

    /// Sets a field, replacing any existing field of the same name.
    pub fn field(mut self, name: impl Into<EscapedFieldKey>, value: impl Into<FieldValue>) -> Self {
        self.fields.insert(name.into(), value.into());
        self
    }

    /// Sets the timestamp, replacing any existing timestamp.
    ///
    /// The value is treated as the number of nanoseconds since the
    /// UNIX epoch.
    pub fn timestamp(mut self, value: u64) -> Self {
        self.timestamp = Some(value);
        self
    }

    /// Constructs the data point
    pub fn build(self) -> DataPoint {
        let Self {
            measurement,
            tags,
            fields,
            timestamp,
        } = self;
        DataPoint {
            measurement,
            tags,
            fields,
            timestamp,
        }
    }
}

/// A single point of information to send to InfluxDB.
#[derive(Debug)]
pub struct DataPoint {
    measurement: EscapedMeasurement,
    tags: BTreeMap<EscapedTagKey, EscapedTagValue>,
    fields: BTreeMap<EscapedFieldKey, FieldValue>,
    timestamp: Option<u64>,
}

impl DataPoint {
    /// Create a builder to incrementally construct a `DataPoint`.
    pub fn builder(measurement: impl Into<EscapedMeasurement>) -> DataPointBuilder {
        DataPointBuilder::new(measurement)
    }

    fn line_protocol(&self) -> LineProtocol<'_> {
        LineProtocol(self)
    }
}

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

/// A string that will be escaped according to the rules of measurements
pub type EscapedMeasurement = Escaped<Measurement>;
/// A string that will be escaped according to the rules of tag keys
pub type EscapedTagKey = Escaped<TagKey>;
/// A string that will be escaped according to the rules of tag values
pub type EscapedTagValue = Escaped<TagKey>;
/// A string that will be escaped according to the rules of field keys
pub type EscapedFieldKey = Escaped<TagKey>;
/// A string that will be escaped according to the rules of field value strings
pub type EscapedFieldValueString = Escaped<FieldValueString>;

/// Ensures that a string value is appropriately escaped when it is sent to InfluxDB.
#[derive(Debug, Clone)]
pub struct Escaped<K>(String, PhantomData<K>);

impl<K> PartialEq for Escaped<K> {
    fn eq(&self, other: &Self) -> bool {
        self.0.eq(&other.0)
    }
}

impl<K> Eq for Escaped<K> {}

impl<K> PartialOrd for Escaped<K> {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        self.0.partial_cmp(&other.0)
    }
}

impl<K> Ord for Escaped<K> {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.0.cmp(&other.0)
    }
}

impl<K> From<&str> for Escaped<K>
where
    K: EscapingSpecification,
{
    fn from(other: &str) -> Self {
        Self(other.into(), PhantomData)
    }
}

impl<K> From<String> for Escaped<K>
where
    K: EscapingSpecification,
{
    fn from(other: String) -> Self {
        Self(other, PhantomData)
    }
}

impl<K> fmt::Display for Escaped<K>
where
    K: EscapingSpecification,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut last = 0;

        for (idx, delim) in self.0.match_indices(K::DELIMITERS) {
            let s = &self.0[last..idx];
            write!(f, r#"{}\{}"#, s, delim)?;
            last = idx + delim.len();
        }

        self.0[last..].fmt(f)
    }
}

/// Specifies how to escape a particular piece of InfluxDB information.
pub trait EscapingSpecification {
    /// The delimiters that need to be escaped
    const DELIMITERS: &'static [char];
}

/// Rules to escape a field value string
#[derive(Debug, Copy, Clone)]
pub struct Measurement(());

/// Rules to escape a tag key, tag field, or field key string
#[derive(Debug, Copy, Clone)]
pub struct TagKey(());

/// Rules to escape a field value string
#[derive(Debug, Copy, Clone)]
pub struct FieldValueString(());

impl EscapingSpecification for Measurement {
    const DELIMITERS: &'static [char] = &[',', ' '];
}

impl EscapingSpecification for TagKey {
    const DELIMITERS: &'static [char] = &[',', '=', ' '];
}

impl EscapingSpecification for FieldValueString {
    const DELIMITERS: &'static [char] = &['"'];
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
    String(EscapedFieldValueString),
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

    #[test]
    fn it_works() {
        let _client = Client::new("http://localhost:8888");
    }

    #[test]
    fn point_builder_allows_setting_tags_and_fields() {
        let point = DataPoint::builder("swap")
            .tag("host", "server01")
            .tag("name", "disk0")
            .field("in", 3_i64)
            .field("out", 4_i64)
            .timestamp(1)
            .build();

        assert_eq!(
            point.line_protocol().to_string(),
            "swap,host=server01,name=disk0 in=3i,out=4i 1",
        );
    }

    const ALL_THE_DELIMITERS: &str = r#"alpha,beta=delta gamma"epsilon"#;

    #[test]
    fn special_characters_are_escaped_in_measurements() {
        let e = EscapedMeasurement::from(ALL_THE_DELIMITERS);
        assert_eq!(e.to_string(), r#"alpha\,beta=delta\ gamma"epsilon"#);
    }

    #[test]
    fn special_characters_are_escaped_in_tag_keys() {
        let e = EscapedTagKey::from(ALL_THE_DELIMITERS);
        assert_eq!(e.to_string(), r#"alpha\,beta\=delta\ gamma"epsilon"#);
    }

    #[test]
    fn special_characters_are_escaped_in_tag_values() {
        let e = EscapedTagValue::from(ALL_THE_DELIMITERS);
        assert_eq!(e.to_string(), r#"alpha\,beta\=delta\ gamma"epsilon"#);
    }

    #[test]
    fn special_characters_are_escaped_in_field_keys() {
        let e = EscapedFieldKey::from(ALL_THE_DELIMITERS);
        assert_eq!(e.to_string(), r#"alpha\,beta\=delta\ gamma"epsilon"#);
    }

    #[test]
    fn special_characters_are_escaped_in_field_values_of_strings() {
        let e = EscapedFieldValueString::from(ALL_THE_DELIMITERS);
        assert_eq!(e.to_string(), r#"alpha,beta=delta gamma\"epsilon"#);
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
