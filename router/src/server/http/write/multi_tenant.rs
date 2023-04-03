//! MultiTenantRequestParser

use data_types::{NamespaceName, OrgBucketMappingError};
use hyper::{Body, Request};

use super::{
    v2::{V2WriteParseError, WriteParamsV2},
    WriteParamExtractor, WriteParams,
};
use crate::server::http::Error;

/// Request parsing errors when operating in "single tenant" mode.
#[derive(Debug, Error)]
pub enum MultiTenantExtractError {
    /// A failure to map a org & bucket to a reasonable [`NamespaceName`].
    #[error(transparent)]
    InvalidOrgAndBucket(#[from] OrgBucketMappingError),

    /// A [`WriteParamsV2`] failed to be parsed from the HTTP request.
    #[error(transparent)]
    ParseV2Request(#[from] V2WriteParseError),
}

/// Implement a by-ref conversion to avoid "moving" the inner errors when only
/// matching against the variants is necessary (the actual error content is
/// discarded, replaced with only a HTTP code)
impl From<&MultiTenantExtractError> for hyper::StatusCode {
    fn from(value: &MultiTenantExtractError) -> Self {
        // Exhaustively match the inner parser errors to ensure new additions
        // have to be explicitly mapped and are not accidentally mapped to a
        // "catch all" code.
        match value {
            MultiTenantExtractError::InvalidOrgAndBucket(_) => Self::BAD_REQUEST,
            MultiTenantExtractError::ParseV2Request(
                V2WriteParseError::NoQueryParams | V2WriteParseError::DecodeFail(_),
            ) => Self::BAD_REQUEST,
        }
    }
}

/// Request parsing for cloud2 / multi-tenant deployments.
///
/// This handler respects the [V2 Write API] without modification, and rejects
/// any V1 write requests.
///
/// [V2 Write API]:
///     https://docs.influxdata.com/influxdb/v2.6/api/#operation/PostWrite
#[derive(Debug, Default)]
pub struct MultiTenantRequestParser;

impl WriteParamExtractor for MultiTenantRequestParser {
    fn parse_v1(&self, _req: &Request<Body>) -> Result<WriteParams, Error> {
        Err(Error::NoHandler)
    }

    fn parse_v2(&self, req: &Request<Body>) -> Result<WriteParams, Error> {
        Ok(parse_v2(req)?)
    }
}

// Parse a V2 write request for multi tenant mode.
fn parse_v2(req: &Request<Body>) -> Result<WriteParams, MultiTenantExtractError> {
    let write_params = WriteParamsV2::try_from(req)?;

    let namespace = NamespaceName::from_org_and_bucket(write_params.org, write_params.bucket)?;

    Ok(WriteParams {
        namespace,
        precision: write_params.precision,
    })
}
