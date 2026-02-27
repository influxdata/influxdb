//! Parsing of HTTP requests that conform to the [V2 Write API], with modified
//! behaviour for single-tenancy clusters and edge/community/pro only.
//!
//! [V2 Write API]:
//!     https://docs.influxdata.com/influxdb/v2.6/api/#operation/PostWrite

use iox_http_util::Request;
use serde::Deserialize;

use super::Precision;

/// v2 DmlErrors returned when decoding the organisation / bucket information
/// from a HTTP request and deriving the namespace name from it.
#[derive(Debug, thiserror::Error)]
pub enum V2WriteParseError {
    /// The request contains no org/bucket destination information.
    #[error("no org/bucket destination provided")]
    NoQueryParams,

    /// The request contains invalid parameters.
    #[error("failed to deserialize org/bucket/precision in request: {0}")]
    DecodeFail(#[from] serde::de::value::Error),
}

/// Query parameters for v2 write requests.
#[derive(Debug, Deserialize)]
pub struct WriteParamsV2 {
    #[serde(default)]
    pub org: String,
    #[serde(default)]
    pub bucket: String,

    #[serde(default)]
    pub precision: Precision,
}

impl TryFrom<&Request> for WriteParamsV2 {
    type Error = V2WriteParseError;

    fn try_from(req: &Request) -> Result<Self, Self::Error> {
        let query = req.uri().query().ok_or(V2WriteParseError::NoQueryParams)?;
        serde_urlencoded::from_str(query).map_err(Into::into)
    }
}
