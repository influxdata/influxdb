//! Parsing of HTTP requests that conform to either the [V2 Write API] or [V1
//! Write API], with modified behaviour (see
//! <https://github.com/influxdata/idpe/issues/17265>).
//!
//! [V2 Write API]:
//!     https://docs.influxdata.com/influxdb/v2.6/api/#operation/PostWrite
//! [V1 Write API]:
//!     https://docs.influxdata.com/influxdb/v1.8/tools/api/#write-http-endpoint

pub mod auth;

use std::sync::Arc;

use async_trait::async_trait;
use auth::authorize;
use authz::{self, Authorizer};
use data_types::{NamespaceName, NamespaceNameError};
use hyper::{Body, Request};
use thiserror::Error;

use super::{
    v1::{RetentionPolicy, V1WriteParseError, WriteParamsV1},
    v2::{V2WriteParseError, WriteParamsV2},
    WriteParams, WriteRequestUnifier,
};
use crate::server::http::{
    write::v1::V1_NAMESPACE_RP_SEPARATOR,
    Error::{self},
};

/// Request parsing errors when operating in "single tenant" mode.
#[derive(Debug, Error)]
pub enum SingleTenantExtractError {
    /// v2 bucket is an empty string.
    #[error("missing bucket value")]
    NoBucketSpecified,

    /// The namespace (or "db" for V1) is not valid.
    #[error(transparent)]
    InvalidNamespace(#[from] NamespaceNameError),

    /// A [`WriteParamsV1`] failed to be parsed from the HTTP request.
    #[error(transparent)]
    ParseV1Request(#[from] V1WriteParseError),

    /// A [`WriteParamsV2`] failed to be parsed from the HTTP request.
    #[error(transparent)]
    ParseV2Request(#[from] V2WriteParseError),

    /// An error occurred verifying the authorization token.
    #[error(transparent)]
    Authorizer(authz::Error),
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
            SingleTenantExtractError::NoBucketSpecified => Self::BAD_REQUEST,
            SingleTenantExtractError::InvalidNamespace(_) => Self::BAD_REQUEST,
            SingleTenantExtractError::ParseV1Request(
                V1WriteParseError::NoQueryParams | V1WriteParseError::DecodeFail(_),
            ) => Self::BAD_REQUEST,
            SingleTenantExtractError::ParseV2Request(
                V2WriteParseError::NoQueryParams | V2WriteParseError::DecodeFail(_),
            ) => Self::BAD_REQUEST,
            SingleTenantExtractError::Authorizer(e) => match e {
                authz::Error::Forbidden => Self::FORBIDDEN,
                authz::Error::NoToken => Self::UNAUTHORIZED,
                _ => Self::FORBIDDEN,
            },
        }
    }
}

/// Request parsing for single-tenant deployments.
///
/// This handler respects the [V2 Write API] with the following modifications:
///
///   * The namespace is derived from ONLY the bucket name (org is discarded).
///   * Authorization token is required.
///
/// This handler respects a limited subset of the [V1 Write API] defined in
/// <https://github.com/influxdata/idpe/issues/17265>.
///
/// [V2 Write API]:
///     https://docs.influxdata.com/influxdb/v2.6/api/#operation/PostWrite
/// [V1 Write API]:
///     https://docs.influxdata.com/influxdb/v1.8/tools/api/#write-http-endpoint
#[derive(Debug)]
pub struct SingleTenantRequestUnifier {
    authz: Arc<dyn Authorizer>,
}

impl SingleTenantRequestUnifier {
    /// Creates a new SingleTenantRequestParser
    pub fn new(authz: Arc<dyn Authorizer>) -> Self {
        Self { authz }
    }
}

#[async_trait]
impl WriteRequestUnifier for SingleTenantRequestUnifier {
    async fn parse_v1(&self, req: &Request<Body>) -> Result<WriteParams, Error> {
        Ok(parse_v1(req, &self.authz).await?)
    }

    async fn parse_v2(&self, req: &Request<Body>) -> Result<WriteParams, Error> {
        Ok(parse_v2(req, &self.authz).await?)
    }
}

// Parse a V1 write request for single tenant mode.
async fn parse_v1(
    req: &Request<Body>,
    authz: &Arc<dyn Authorizer>,
) -> Result<WriteParams, SingleTenantExtractError> {
    // Extract the write parameters.
    let write_params = WriteParamsV1::try_from(req)?;

    // Extract or construct the namespace name string from the write parameters
    let namespace = NamespaceName::new(match write_params.rp {
        RetentionPolicy::Unspecified | RetentionPolicy::Autogen => write_params.db,
        RetentionPolicy::Named(rp) => {
            format!(
                "{db}{sep}{rp}",
                db = write_params.db,
                sep = V1_NAMESPACE_RP_SEPARATOR
            )
        }
    })?;
    authorize(authz, req, &namespace, write_params.password)
        .await
        .map_err(SingleTenantExtractError::Authorizer)?;

    Ok(WriteParams {
        namespace,
        precision: write_params.precision,
    })
}

// Parse a V2 write request for single tenant mode.
async fn parse_v2(
    req: &Request<Body>,
    authz: &Arc<dyn Authorizer>,
) -> Result<WriteParams, SingleTenantExtractError> {
    let write_params = WriteParamsV2::try_from(req)?;

    // For V2 requests in "single tenant" mode, only the bucket parameter is
    // respected and it is never escaped or otherwise changed. The "org" field
    // is discarded.
    //
    //      See https://github.com/influxdata/idpe/issues/17265
    //
    if write_params.bucket.is_empty() {
        return Err(SingleTenantExtractError::NoBucketSpecified);
    }
    let namespace = NamespaceName::new(write_params.bucket)?;
    authorize(authz, req, &namespace, None)
        .await
        .map_err(SingleTenantExtractError::Authorizer)?;

    Ok(WriteParams {
        namespace,
        precision: write_params.precision,
    })
}

#[cfg(test)]
mod tests {
    use assert_matches::assert_matches;
    use authz::{http::AuthorizationHeaderExtension, Permission};
    use hyper::header::HeaderValue;
    use parking_lot::Mutex;

    use super::{
        auth::mock::{MockAuthorizer, *},
        *,
    };
    use crate::server::http::write::Precision;

    #[tokio::test]
    async fn test_delegate_to_authz() {
        #[derive(Debug)]
        struct MockCountingAuthorizer {
            calls_counter: Arc<Mutex<usize>>,
        }

        #[async_trait]
        impl Authorizer for MockCountingAuthorizer {
            async fn permissions(
                &self,
                _token: Option<Vec<u8>>,
                perms: &[Permission],
            ) -> Result<Vec<Permission>, authz::Error> {
                *self.calls_counter.lock() += 1;
                Ok(perms.to_vec())
            }
        }
        let counter = Arc::new(Mutex::new(0));
        let authz: Arc<MockCountingAuthorizer> = Arc::new(MockCountingAuthorizer {
            calls_counter: Arc::clone(&counter),
        });
        let unifier = SingleTenantRequestUnifier::new(authz);

        let request = Request::builder()
            .uri("https://foo?db=bananas")
            .method("POST")
            .extension(AuthorizationHeaderExtension::new(Some(
                HeaderValue::from_str(format!("Token {MOCK_AUTH_VALID_TOKEN}").as_str()).unwrap(),
            )))
            .body(Body::from(""))
            .unwrap();

        assert!(unifier.parse_v1(&request).await.is_ok());
        assert_eq!(*counter.lock(), 1);
    }

    #[tokio::test]
    async fn test_query_param_token() {
        let authz = Arc::new(MockAuthorizer::default());
        let unifier = SingleTenantRequestUnifier::new(authz);
        let request = Request::builder()
            .uri(format!(
                "https://itsallbroken.com/write?db=bananas&p={MOCK_AUTH_VALID_TOKEN}"
            ))
            .method("POST")
            .body(Body::from(""))
            .unwrap();

        assert!(unifier.parse_v1(&request).await.is_ok());
    }

    macro_rules! test_parse_v1 {
        (
            $name:ident,
            query_string = $query_string:expr,  // A query string including the ?
            want = $($want:tt)+                 // A pattern match for assert_matches!
        ) => {
            paste::paste! {
                #[tokio::test]
                async fn [<test_parse_v1_ $name>]() {
                    let authz = Arc::new(MockAuthorizer::default());
                    let unifier = SingleTenantRequestUnifier::new(authz);

                    let query = $query_string;
                    let request = Request::builder()
                        .uri(format!("https://itsallbroken.com/ignored{query}"))
                        .method("POST")
                        .extension(AuthorizationHeaderExtension::new(Some(
                            HeaderValue::from_str(format!("Token {MOCK_AUTH_VALID_TOKEN}").as_str()).unwrap(),
                        )))
                        .body(Body::from(""))
                        .unwrap();

                    let got = unifier.parse_v1(&request).await;
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
        invalid_db,
        query_string = format!("?db={}", "A".repeat(1000)),
        want = Err(Error::SingleTenantError(
            SingleTenantExtractError::InvalidNamespace(NamespaceNameError::LengthConstraint { .. })
        ))
    );

    test_parse_v1!(
        no_rp,
        query_string = "?db=bananas",
        want = Ok(WriteParams{ namespace, precision }) => {
            assert_eq!(namespace.as_str(), "bananas");
            assert_matches!(precision, Precision::Nanoseconds);
        }
    );

    // Permit `/` character in the DB
    test_parse_v1!(
        no_rp_db_with_rp_separator,
        query_string = "?db=bananas/are/great",
        want = Ok(WriteParams{ namespace, precision }) => {
            assert_eq!(namespace.as_str(), "bananas/are/great");
            assert_matches!(precision, Precision::Nanoseconds);
        }
    );

    // Permit the `/` character in the RP
    test_parse_v1!(
        rp_with_rp_separator,
        query_string = "?db=bananas&rp=are/great",
        want = Ok(WriteParams{ namespace, precision }) => {
            assert_eq!(namespace.as_str(), "bananas/are/great");
            assert_matches!(precision, Precision::Nanoseconds);
        }
    );

    // `/` character is allowed in the DB, if a named RP is specified
    test_parse_v1!(
        db_with_rp_separator_and_rp,
        query_string = "?db=foo/bar&rp=my_rp",
        want = Ok(WriteParams{ namespace, precision }) => {
            assert_eq!(namespace.as_str(), "foo/bar/my_rp");
            assert_matches!(precision, Precision::Nanoseconds);
        }
    );

    // Always concat, even if this results in duplication rp within the namespace.
    // ** this matches the query API behavior **
    test_parse_v1!(
        db_with_rp_separator_and_duplicate_rp,
        query_string = "?db=foo/my_rp&rp=my_rp",
        want = Ok(WriteParams{ namespace, precision }) => {
            assert_eq!(namespace.as_str(), "foo/my_rp/my_rp");
            assert_matches!(precision, Precision::Nanoseconds);
        }
    );

    // `/` character is allowed in the DB, if an autogen RP is specified
    test_parse_v1!(
        db_with_rp_separator_and_rp_autogen,
        query_string = "?db=foo/bar&rp=autogen",
        want = Ok(WriteParams{ namespace, precision }) => {
            assert_eq!(namespace.as_str(), "foo/bar");
            assert_matches!(precision, Precision::Nanoseconds);
        }
    );

    // `/` character is allowed in the DB, if a default RP is specified
    test_parse_v1!(
        db_with_rp_separator_and_rp_default,
        query_string = "?db=foo/bar&rp=default",
        want = Ok(WriteParams{ namespace, precision }) => {
            assert_eq!(namespace.as_str(), "foo/bar");
            assert_matches!(precision, Precision::Nanoseconds);
        }
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
        encoded_case_sensitive,
        query_string = "?db=BaNanas",
        want = Ok(WriteParams{ namespace, precision: _ }) => {
            assert_eq!(namespace.as_str(), "BaNanas");
        }
    );

    test_parse_v1!(
        encoded_quotation,
        query_string = "?db=ban'anas",
        want = Err(Error::SingleTenantError(
            SingleTenantExtractError::InvalidNamespace(NamespaceNameError::BadChars { .. })
        ))
    );

    test_parse_v1!(
        start_nonalphanumeric,
        query_string = "?db=_bananas",
        want = Ok(WriteParams{ namespace, precision: _ }) => {
            assert_eq!(namespace.as_str(), "_bananas");
        }
    );

    test_parse_v1!(
        minimum_length_possible,
        query_string = "?db=d",
        want = Ok(WriteParams{ namespace, precision: _ }) => {
            assert_eq!(namespace.as_str().len(), 1);
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
                #[tokio::test]
                async fn [<test_parse_v2_ $name>]() {
                    let authz = Arc::new(MockAuthorizer::default());
                    let unifier = SingleTenantRequestUnifier::new(authz);

                    let query = $query_string;
                    let request = Request::builder()
                        .uri(format!("https://itsallbroken.com/ignored{query}"))
                        .method("POST")
                        .extension(AuthorizationHeaderExtension::new(Some(
                            HeaderValue::from_str(format!("Token {MOCK_AUTH_VALID_TOKEN}").as_str()).unwrap(),
                        )))
                        .body(Body::from(""))
                        .unwrap();

                    let got = unifier.parse_v2(&request).await;
                    assert_matches!(got, $($want)+);
                }
            }
        };
    }

    test_parse_v2!(
        empty_query_string,
        query_string = "?",
        want = Err(Error::SingleTenantError(
            SingleTenantExtractError::NoBucketSpecified
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
            SingleTenantExtractError::NoBucketSpecified
        ))
    );

    test_parse_v2!(
        no_org_no_bucket,
        query_string = "?wat=isthis",
        want = Err(Error::SingleTenantError(
            SingleTenantExtractError::NoBucketSpecified
        ))
    );

    test_parse_v2!(
        invalid_bucket,
        query_string = format!("?bucket={}", "A".repeat(1000)),
        want = Err(Error::SingleTenantError(
            SingleTenantExtractError::InvalidNamespace(NamespaceNameError::LengthConstraint { .. })
        ))
    );

    test_parse_v2!(
        url_encoding,
        // URL-encoded input that is decoded in the HTTP layer
        query_string = "?bucket=cool%2Fconfusing&prg=org",
        want = Ok(WriteParams {namespace, ..}) => {
            // Yielding a not-encoded string as the namespace.
            assert_eq!(namespace.as_str(), "cool/confusing");
        }
    );

    test_parse_v2!(
        encoded_emoji,
        query_string = "?bucket=confusing%F0%9F%8D%8C&prg=org",
        want = Err(Error::SingleTenantError(
            SingleTenantExtractError::InvalidNamespace(NamespaceNameError::BadChars { .. })
        ))
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
        encoded_case_sensitive,
        query_string = "?bucket=buCket",
        want = Ok(WriteParams {
            namespace,
            ..
        }) => {
            assert_eq!(namespace.as_str(), "buCket");
        }
    );

    test_parse_v2!(
        single_quotation,
        query_string = "?bucket=buc'ket",
        want = Err(Error::SingleTenantError(
            SingleTenantExtractError::InvalidNamespace(NamespaceNameError::BadChars { .. })
        ))
    );

    test_parse_v2!(
        start_nonalphanumeric,
        query_string = "?bucket=_bucket",
        want = Ok(WriteParams {
            namespace,
            ..
        }) => {
            assert_eq!(namespace.as_str(), "_bucket");
        }
    );

    test_parse_v2!(
        minimum_length_possible,
        query_string = "?bucket=b",
        want = Ok(WriteParams {
            namespace,
            ..
        }) => {
            assert_eq!(namespace.as_str().len(), 1);
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
