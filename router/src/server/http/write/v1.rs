//! Parsing of HTTP requests that conform to the [V1 Write API], modified for
//! single-tenancy clusters only.
//!
//! See <https://github.com/influxdata/idpe/issues/17265>.
//!
//! [V1 Write API]:
//!     https://docs.influxdata.com/influxdb/v1.8/tools/api/#write-http-endpoint

use hyper::Request;
use serde::{Deserialize, Deserializer};

use crate::server::http::{write::Precision, Error};

// When a retention policy is provided, it is appended to the db field,
// separated by a single `/`.
//
//      See https://github.com/influxdata/idpe/issues/17265
//
pub(crate) const V1_NAMESPACE_RP_SEPARATOR: char = '/';

/// v1 DmlErrors returned when decoding the database / rp information from a
/// HTTP request and deriving the namespace name from it.
#[derive(Debug, Error)]
pub enum V1WriteParseError {
    /// The request contains no db destination information.
    #[error("no db destination provided")]
    NoQueryParams,

    /// The request contains invalid parameters.
    #[error("failed to deserialize db/rp/precision in request: {0}")]
    DecodeFail(#[from] serde::de::value::Error),
}

/// May be empty string, explicit rp name, or `autogen`. As provided at the
/// write API. Handling is described in context of the construction of the
/// `NamespaceName`, and not an explicit honouring for retention duration.
#[derive(Debug, Default)]
pub(crate) enum RetentionPolicy {
    /// The user did not specify a retention policy (at the write API).
    #[default]
    Unspecified,
    /// Default on v1 database creation, if no rp was provided.
    Autogen,
    /// The user specified the name of the retention policy to be used.
    Named(String),
}

impl<'de> Deserialize<'de> for RetentionPolicy {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?.to_lowercase();

        Ok(match s.as_str() {
            "" => RetentionPolicy::Unspecified,
            "''" => RetentionPolicy::Unspecified,
            "autogen" | "default" => RetentionPolicy::Autogen,
            _ => RetentionPolicy::Named(s),
        })
    }
}

/// Query parameters for v1 write requests.
#[derive(Debug, Deserialize)]
pub(crate) struct WriteParamsV1 {
    pub(crate) db: String,

    #[serde(default)]
    pub(crate) precision: Precision,
    #[serde(default)]
    pub(crate) rp: RetentionPolicy,

    // `username` is an optional v1 query parameter, but is ignored
    // in the CST spec, we treat the `p` parameter as a token
    #[serde(rename(deserialize = "p"))]
    pub(crate) password: Option<String>,
}

impl<T> TryFrom<&Request<T>> for WriteParamsV1 {
    type Error = V1WriteParseError;

    fn try_from(req: &Request<T>) -> Result<Self, Self::Error> {
        let query = req.uri().query().ok_or(V1WriteParseError::NoQueryParams)?;
        let params: WriteParamsV1 = serde_urlencoded::from_str(query)?;

        Ok(params)
    }
}
