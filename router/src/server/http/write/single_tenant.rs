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
                | V1WriteParseError::ContainsRpSeparator,
            ) => Self::BAD_REQUEST,
            SingleTenantExtractError::ParseV2Request(
                V2WriteParseError::NoQueryParams | V2WriteParseError::DecodeFail(_),
            ) => Self::BAD_REQUEST,
        }
    }
}

/// Request parsing for single-tenant deployments.
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

#[cfg(test)]
mod tests {
    use crate::server::http::write::Precision;

    use super::*;

    use assert_matches::assert_matches;

    macro_rules! test_parse_v1 {
        (
            $name:ident,
            query_string = $query_string:expr,  // A query string including the ?
            want = $($want:tt)+                 // A pattern match for assert_matches!
        ) => {
            paste::paste! {
                #[test]
                fn [<test_parse_v1_ $name>]() {
                    let parser = SingleTenantRequestParser::default();

                    let query = $query_string;
                    let request = Request::builder()
                        .uri(format!("https://itsallbroken.com/ignored{query}"))
                        .method("POST")
                        .body(Body::from(""))
                        .unwrap();

                    let got = parser.parse_v1(&request);
                    assert_matches!(got, $($want)+);
                }
            }
        };
    }

    test_parse_v1!(
        no_query_string,
        query_string = "",
        want = Err(Error::SingleTenantError(
            SingleTenantExtractError::ParseV1Request(V1WriteParseError::NoQueryParams)
        ))
    );

    test_parse_v1!(
        empty_query_string,
        query_string = "?",
        want = Err(Error::SingleTenantError(
            SingleTenantExtractError::ParseV1Request(V1WriteParseError::DecodeFail(e))
        )) => {
            assert_eq!(e.to_string(), "missing field `db`")
        }
    );

    test_parse_v1!(
        no_db,
        query_string = "?rp=autogen",
        want = Err(Error::SingleTenantError(
            SingleTenantExtractError::ParseV1Request(V1WriteParseError::DecodeFail(e))
        )) => {
            assert_eq!(e.to_string(), "missing field `db`")
        }
    );

    test_parse_v1!(
        no_rp,
        query_string = "?db=bananas",
        want = Ok(WriteParams{ namespace, precision }) => {
            assert_eq!(namespace.as_str(), "bananas");
            assert_matches!(precision, Precision::Nanoseconds);
        }
    );

    // Prevent ambiguity by denying the `/` character in the DB
    test_parse_v1!(
        no_rp_db_with_rp_separator,
        query_string = "?db=bananas/are/great",
        want = Err(Error::SingleTenantError(
            SingleTenantExtractError::ParseV1Request(V1WriteParseError::ContainsRpSeparator)
        ))
    );

    // Prevent ambiguity by denying the `/` character in the RP
    test_parse_v1!(
        rp_with_rp_separator,
        query_string = "?db=bananas&rp=are/great",
        want = Err(Error::SingleTenantError(
            SingleTenantExtractError::ParseV1Request(V1WriteParseError::ContainsRpSeparator)
        ))
    );

    test_parse_v1!(
        rp_empty,
        query_string = "?db=bananas&rp=",
        want = Ok(WriteParams{ namespace, precision }) => {
            assert_eq!(namespace.as_str(), "bananas");
            assert_matches!(precision, Precision::Nanoseconds);
        }
    );

    test_parse_v1!(
        rp_empty_quotes,
        query_string = "?db=bananas&rp=''",
        want = Ok(WriteParams{ namespace, precision }) => {
            assert_eq!(namespace.as_str(), "bananas");
            assert_matches!(precision, Precision::Nanoseconds);
        }
    );

    test_parse_v1!(
        rp_autogen,
        query_string = "?db=bananas&rp=autogen",
        want = Ok(WriteParams{ namespace, precision }) => {
            assert_eq!(namespace.as_str(), "bananas");
            assert_matches!(precision, Precision::Nanoseconds);
        }
    );

    test_parse_v1!(
        rp_specified,
        query_string = "?db=bananas&rp=ageless",
        want = Ok(WriteParams{ namespace, precision }) => {
            assert_eq!(namespace.as_str(), "bananas/ageless");
            assert_matches!(precision, Precision::Nanoseconds);
        }
    );

    test_parse_v1!(
        with_precision,
        query_string = "?db=bananas&rp=ageless&precision=ms",
        want = Ok(WriteParams{ namespace, precision }) => {
            assert_eq!(namespace.as_str(), "bananas/ageless");
            assert_matches!(precision, Precision::Milliseconds);
        }
    );

    macro_rules! test_parse_v2 {
        (
            $name:ident,
            query_string = $query_string:expr,  // A query string including the ?
            want = $($want:tt)+                 // A pattern match for assert_matches!
        ) => {
            paste::paste! {
                #[test]
                fn [<test_parse_v2_ $name>]() {
                    let parser = SingleTenantRequestParser::default();

                    let query = $query_string;
                    let request = Request::builder()
                        .uri(format!("https://itsallbroken.com/ignored{query}"))
                        .method("POST")
                        .body(Body::from(""))
                        .unwrap();

                    let got = parser.parse_v2(&request);
                    assert_matches!(got, $($want)+);
                }
            }
        };
    }

    test_parse_v2!(
        empty_query_string,
        query_string = "?",
        want = Err(Error::SingleTenantError(
            SingleTenantExtractError::InvalidNamespace(NamespaceNameError::LengthConstraint { .. })
        ))
    );

    // This is allowed in single-tenant, it is NOT allowed in multi-tenant
    test_parse_v2!(
        bucket_only,
        query_string = "?bucket=bananas",
        want = Ok(WriteParams{ namespace, precision }) => {
            assert_eq!(namespace.as_str(), "bananas");
            assert_matches!(precision, Precision::Nanoseconds);
        }
    );

    // An empty/missing "bucket" is an error.
    test_parse_v2!(
        org_only,
        query_string = "?org=bananas",
        want = Err(Error::SingleTenantError(
            SingleTenantExtractError::InvalidNamespace(NamespaceNameError::LengthConstraint { .. })
        ))
    );

    test_parse_v2!(
        no_org_no_bucket,
        query_string = "?wat=isthis",
        want = Err(Error::SingleTenantError(
            SingleTenantExtractError::InvalidNamespace(NamespaceNameError::LengthConstraint { .. })
        ))
    );

    // Do not encode potentially problematic input.
    test_parse_v2!(
        no_encoding,
        // URL-encoded input that is decoded in the HTTP layer
        query_string = "?bucket=cool%2Fconfusing%F0%9F%8D%8C&prg=org",
        want = Ok(WriteParams {namespace, ..}) => {
            // Yielding a not-encoded string as the namespace.
            assert_eq!(namespace.as_str(), "cool/confusing🍌");
        }
    );

    test_parse_v2!(
        org_ignored,
        query_string = "?org=wat&bucket=bananas",
        want = Ok(WriteParams {
            namespace,
            precision
        }) => {
            assert_eq!(namespace.as_str(), "bananas");
            assert_matches!(precision, Precision::Nanoseconds);
        }
    );

    test_parse_v2!(
        with_precision,
        query_string = "?bucket=bananas&precision=ms",
        want = Ok(WriteParams {
            namespace,
            precision
        }) => {
            assert_eq!(namespace.as_str(), "bananas");
            assert_matches!(precision, Precision::Milliseconds);
        }
    );
}
