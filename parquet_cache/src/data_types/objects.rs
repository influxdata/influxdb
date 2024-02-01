use hyper::{header::HeaderValue, HeaderMap};
use serde::{Deserialize, Serialize};

use crate::client::cache_connector::Error as CacheServerError;

pub static X_RANGE_START_HEADER: &str = "x-object-range-start";
pub static X_RANGE_END_HEADER: &str = "x-object-range-end";

pub fn extract_usize_header(
    header: &'static str,
    values: &HeaderMap<HeaderValue>,
) -> Result<usize, CacheServerError> {
    let val = values
        .get(header)
        .ok_or(CacheServerError::ReadData(format!(
            "missing header {}",
            header
        )))?
        .to_str()
        .map_err(|_| CacheServerError::ReadData(format!("missing {} header", header)))?;

    val.parse::<usize>()
        .map_err(|_| CacheServerError::ReadData(format!("invalid {} header", header)))
}

/// Metadata for object.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct GetObjectMetaResponse {
    /// The full path to the object
    pub location: String,
    /// The last modified time
    pub last_modified: chrono::DateTime<chrono::Utc>,
    /// The size in bytes of the object
    pub size: usize,
    /// The unique identifier for the object
    pub e_tag: Option<String>,
    /// A version indicator for this object
    pub version: Option<String>,
}

impl From<GetObjectMetaResponse> for object_store::ObjectMeta {
    fn from(value: GetObjectMetaResponse) -> Self {
        let GetObjectMetaResponse {
            location,
            last_modified,
            size,
            e_tag,
            version,
        } = value;

        Self {
            location: object_store::path::Path::parse(location).expect("should be valid path"),
            last_modified,
            size,
            e_tag,
            version,
        }
    }
}

impl From<object_store::ObjectMeta> for GetObjectMetaResponse {
    fn from(value: object_store::ObjectMeta) -> Self {
        let object_store::ObjectMeta {
            location,
            last_modified,
            size,
            e_tag,
            version,
        } = value;

        Self {
            location: location.to_string(),
            last_modified,
            size,
            e_tag,
            version,
        }
    }
}
