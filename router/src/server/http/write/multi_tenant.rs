//! Parsing of HTTP requests that conform to the [V2 Write API] only.
//!
//! [V2 Write API]:
//!     https://docs.influxdata.com/influxdb/v2.6/api/#operation/PostWrite

use async_trait::async_trait;
use data_types::{NamespaceName, OrgBucketMappingError};
use hyper::{Body, Request};

use super::{
    v2::{V2WriteParseError, WriteParamsV2},
    WriteParams, WriteRequestUnifier,
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
pub struct MultiTenantRequestUnifier;

#[async_trait]
impl WriteRequestUnifier for MultiTenantRequestUnifier {
    async fn parse_v1(&self, _req: &Request<Body>) -> Result<WriteParams, Error> {
        Err(Error::NoHandler)
    }

    async fn parse_v2(&self, req: &Request<Body>) -> Result<WriteParams, Error> {
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

#[cfg(test)]
mod tests {
    use assert_matches::assert_matches;
    use data_types::NamespaceNameError;

    use super::*;
    use crate::server::http::write::Precision;

    #[tokio::test]
    async fn test_parse_v1_always_errors() {
        let unifier = MultiTenantRequestUnifier;

        let got = unifier.parse_v1(&Request::default()).await;
        assert_matches!(got, Err(Error::NoHandler));
    }

    macro_rules! test_parse_v2 {
        (
            $name:ident,
            query_string = $query_string:expr,  // A query string including the ?
            want = $($want:tt)+                 // A pattern match for assert_matches!
        ) => {
            paste::paste! {
                #[tokio::test]
                async fn [<test_parse_v2_ $name>]() {
                    let unifier = MultiTenantRequestUnifier;

                    let query = $query_string;
                    let request = Request::builder()
                        .uri(format!("https://itsallbroken.com/ignored{query}"))
                        .method("POST")
                        .body(Body::from(""))
                        .unwrap();

                    let got = unifier.parse_v2(&request).await;
                    assert_matches!(got, $($want)+);
                }
            }
        };
    }

    test_parse_v2!(
        no_query_string,
        query_string = "",
        want = Err(Error::MultiTenantError(
            MultiTenantExtractError::ParseV2Request(V2WriteParseError::NoQueryParams)
        ))
    );

    test_parse_v2!(
        empty_query_string,
        query_string = "?",
        want = Err(Error::MultiTenantError(
            MultiTenantExtractError::InvalidOrgAndBucket(
                OrgBucketMappingError::NoOrgBucketSpecified
            )
        ))
    );

    // While this is allowed in single-tenant, it is NOT allowed in multi-tenant
    test_parse_v2!(
        bucket_only,
        query_string = "?bucket=bananas",
        want = Err(Error::MultiTenantError(
            MultiTenantExtractError::InvalidOrgAndBucket(
                OrgBucketMappingError::NoOrgBucketSpecified
            )
        ))
    );

    test_parse_v2!(
        org_only,
        query_string = "?org=bananas",
        want = Err(Error::MultiTenantError(
            MultiTenantExtractError::InvalidOrgAndBucket(
                OrgBucketMappingError::NoOrgBucketSpecified
            )
        ))
    );

    test_parse_v2!(
        no_org_no_bucket,
        query_string = "?wat=isthis",
        want = Err(Error::MultiTenantError(
            MultiTenantExtractError::InvalidOrgAndBucket(
                OrgBucketMappingError::NoOrgBucketSpecified
            )
        ))
    );

    test_parse_v2!(
        encoded_separator,
        query_string = "?org=cool_confusing&bucket=bucket",
        want = Ok(WriteParams {
            namespace,
            ..
        }) => {
            assert_eq!(namespace.as_str(), "cool_confusing_bucket");
        }
    );

    test_parse_v2!(
        encoded_case_sensitive,
        query_string = "?org=Captialize&bucket=bucket",
        want = Ok(WriteParams {
            namespace,
            ..
        }) => {
            assert_eq!(namespace.as_str(), "Captialize_bucket");
        }
    );

    test_parse_v2!(
        encoded_quotation,
        query_string = "?org=cool'confusing&bucket=bucket",
        want = Err(Error::MultiTenantError(
            MultiTenantExtractError::InvalidOrgAndBucket(
                OrgBucketMappingError::InvalidNamespaceName(NamespaceNameError::BadChars { .. })
            )
        ))
    );

    test_parse_v2!(
        start_nonalphanumeric,
        query_string = "?org=_coolconfusing&bucket=bucket",
        want = Ok(WriteParams {
            namespace,
            ..
        }) => {
            assert_eq!(namespace.as_str(), "_coolconfusing_bucket");
        }
    );

    test_parse_v2!(
        minimum_length_possible,
        query_string = "?org=o&bucket=b",
        want = Ok(WriteParams {
            namespace,
            ..
        }) => {
            assert_eq!(namespace.as_str().len(), 3);
        }
    );

    // Expected usage (no org) with precision
    test_parse_v2!(
        with_precision,
        query_string = "?org=banana&bucket=cool&precision=ms",
        want = Ok(WriteParams {
            namespace,
            precision
        }) => {
            assert_eq!(namespace.as_str(), "banana_cool");
            assert_matches!(precision, Precision::Milliseconds);
        }
    );
}
