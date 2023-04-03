//! Parsing of HTTP requests that conform to either the [V2 Write API] or [V1
//! Write API], with modified behaviour (see
//! <https://github.com/influxdata/idpe/issues/17265>).
//!
//! [V2 Write API]:
//!     https://docs.influxdata.com/influxdb/v2.6/api/#operation/PostWrite
//! [V1 Write API]:
//!     https://docs.influxdata.com/influxdb/v1.8/tools/api/#write-http-endpoint

use hyper::{Body, Request};
use thiserror::Error;

use super::{
    v1::{RetentionPolicy, V1WriteParseError, WriteParamsV1},
    v2::{V2WriteParseError, WriteParamsV2},
    WriteParamExtractor, WriteParams,
};
use crate::server::http::{
    write::v1::V1_NAMESPACE_RP_SEPARATOR,
    Error::{self},
};
use data_types::{NamespaceName, NamespaceNameError};

/// Request parsing errors when operating in "single tenant" mode.
#[derive(Debug, Error)]
pub enum SingleTenantExtractError {
    /// The namespace (or "db" for V1) is not valid.
    #[error(transparent)]
    InvalidNamespace(#[from] NamespaceNameError),

    /// A [`WriteParamsV1`] failed to be parsed from the HTTP request.
    #[error(transparent)]
    ParseV1Request(#[from] V1WriteParseError),

    /// A [`WriteParamsV2`] failed to be parsed from the HTTP request.
    #[error(transparent)]
    ParseV2Request(#[from] V2WriteParseError),
}

/// Implement a by-ref conversion to avoid "moving" the inner errors when only
/// matching against the variants is necessary (the actual error content is
/// discarded, replaced with only a HTTP code)
impl From<&SingleTenantExtractError> for hyper::StatusCode {
    fn from(value: &SingleTenantExtractError) -> Self {
        // Exhaustively match the inner parser errors to ensure new additions
        // have to be explicitly mapped and are not accidentally mapped to a
        // "catch all" code.
        match value {
            SingleTenantExtractError::InvalidNamespace(_) => Self::BAD_REQUEST,
            SingleTenantExtractError::ParseV1Request(
                V1WriteParseError::NoQueryParams
                | V1WriteParseError::DecodeFail(_)
                | V1WriteParseError::NamespaceContainsRpSeparator,
            ) => Self::BAD_REQUEST,
            SingleTenantExtractError::ParseV2Request(
                V2WriteParseError::NoQueryParams | V2WriteParseError::DecodeFail(_),
            ) => Self::BAD_REQUEST,
        }
    }
}

/// Request parsing for cloud2 / multi-tenant deployments.
///
/// This handler respects the [V2 Write API] with the following modifications:
///
///   * The namespace is derived from ONLY the bucket name (org is discarded)
///
/// This handler respects a limited subset of the [V1 Write API] defined in
/// <https://github.com/influxdata/idpe/issues/17265>.
///
/// [V2 Write API]:
///     https://docs.influxdata.com/influxdb/v2.6/api/#operation/PostWrite
/// [V1 Write API]:
///     https://docs.influxdata.com/influxdb/v1.8/tools/api/#write-http-endpoint
#[derive(Debug, Default)]
pub struct SingleTenantRequestParser;

impl WriteParamExtractor for SingleTenantRequestParser {
    fn parse_v1(&self, req: &Request<Body>) -> Result<WriteParams, Error> {
        Ok(parse_v1(req)?)
    }

    fn parse_v2(&self, req: &Request<Body>) -> Result<WriteParams, Error> {
        Ok(parse_v2(req)?)
    }
}

// Parse a V1 write request for single tenant mode.
fn parse_v1(req: &Request<Body>) -> Result<WriteParams, SingleTenantExtractError> {
    // Extract the write parameters.
    let write_params = WriteParamsV1::try_from(req)?;

    // Extracting the write parameters validates the db field never contains the
    // '/' separator to avoid ambiguity with the "namespace/rp" construction.
    debug_assert!(!write_params.db.contains(V1_NAMESPACE_RP_SEPARATOR));

    // Extract or construct the namespace name string from the write parameters
    let namespace = match write_params.rp {
        RetentionPolicy::Unspecified | RetentionPolicy::Autogen => write_params.db,
        RetentionPolicy::Named(rp) => {
            format!(
                "{db}{sep}{rp}",
                db = write_params.db,
                sep = V1_NAMESPACE_RP_SEPARATOR
            )
        }
    };

    Ok(WriteParams {
        namespace: NamespaceName::new(namespace)?,
        precision: write_params.precision,
    })
}

// Parse a V2 write request for single tenant mode.
fn parse_v2(req: &Request<Body>) -> Result<WriteParams, SingleTenantExtractError> {
    let write_params = WriteParamsV2::try_from(req)?;

    // For V2 requests in "single tenant" mode, only the bucket parameter is
    // respected and it is never escaped or otherwise changed. The "org" field
    // is discarded.
    //
    //      See https://github.com/influxdata/idpe/issues/17265
    //

    Ok(WriteParams {
        namespace: NamespaceName::new(write_params.bucket)?,
        precision: write_params.precision,
    })
}
