//! IOx authorization client.
//!
//! Authorization client interface to be used by IOx components to
//! restrict access to authorized requests where required.

#![deny(rustdoc::broken_intra_doc_links, rust_2018_idioms)]
#![warn(
    missing_copy_implementations,
    missing_docs,
    clippy::explicit_iter_loop,
    clippy::future_not_send,
    clippy::use_self,
    clippy::clone_on_ref_ptr,
    clippy::todo,
    clippy::dbg_macro
)]
#![allow(rustdoc::private_intra_doc_links)]

use async_trait::async_trait;
use base64::{prelude::BASE64_STANDARD, Engine};
use generated_types::influxdata::iox::authz::v1 as proto;
use observability_deps::tracing::warn;
use snafu::Snafu;

mod permission;
pub use permission::{Action, Permission, Resource};

#[cfg(feature = "http")]
pub mod http;

/// Extract a token from an HTTP header or gRPC metadata value.
pub fn extract_token<T: AsRef<[u8]> + ?Sized>(value: Option<&T>) -> Option<Vec<u8>> {
    let mut parts = value?.as_ref().splitn(2, |&v| v == b' ');
    let token = match parts.next()? {
        b"Token" | b"Bearer" => parts.next()?.to_vec(),
        b"Basic" => parts
            .next()
            .and_then(|v| BASE64_STANDARD.decode(v).ok())?
            .splitn(2, |&v| v == b':')
            .nth(1)?
            .to_vec(),
        _ => return None,
    };
    if token.is_empty() {
        None
    } else {
        Some(token)
    }
}

/// An authorizer is used to validate the associated with
/// an authorization token that has been extracted from a request.
#[async_trait]
pub trait Authorizer: std::fmt::Debug + Send + Sync {
    /// Determine the permissions associated with a request token.
    ///
    /// The returned list of permissions is the intersection of the permissions
    /// requested and the permissions associated with the token.
    ///
    /// Implementations of this trait should only error if:
    ///     * there is a failure processing the token.
    ///     * there is not any intersection of permissions.
    async fn permissions(
        &self,
        token: Option<Vec<u8>>,
        perms: &[Permission],
    ) -> Result<Vec<Permission>, Error>;

    /// Make a test request that determines if end-to-end communication
    /// with the service is working.
    async fn probe(&self) -> Result<(), Error> {
        self.permissions(Some(b"".to_vec()), &[]).await?;
        Ok(())
    }
}

/// Wrapped `Option<dyn Authorizer>`
/// Provides response to inner `IoxAuthorizer::permissions()`
#[async_trait]
impl<T: Authorizer> Authorizer for Option<T> {
    async fn permissions(
        &self,
        token: Option<Vec<u8>>,
        perms: &[Permission],
    ) -> Result<Vec<Permission>, Error> {
        match self {
            Some(authz) => authz.permissions(token, perms).await,
            // no authz rpc service => return same perms requested. Used for testing.
            None => Ok(perms.to_vec()),
        }
    }
}

#[async_trait]
impl<T: AsRef<dyn Authorizer> + std::fmt::Debug + Send + Sync> Authorizer for T {
    async fn permissions(
        &self,
        token: Option<Vec<u8>>,
        perms: &[Permission],
    ) -> Result<Vec<Permission>, Error> {
        self.as_ref().permissions(token, perms).await
    }
}

/// Authorizer implementation using influxdata.iox.authz.v1 protocol.
#[derive(Clone, Debug)]
pub struct IoxAuthorizer {
    client:
        proto::iox_authorizer_service_client::IoxAuthorizerServiceClient<tonic::transport::Channel>,
}

impl IoxAuthorizer {
    /// Attempt to create a new client by connecting to a given endpoint.
    pub fn connect_lazy<D>(dst: D) -> Result<Self, Box<dyn std::error::Error>>
    where
        D: TryInto<tonic::transport::Endpoint> + Send,
        D::Error: Into<tonic::codegen::StdError>,
    {
        let ep = tonic::transport::Endpoint::new(dst)?;
        let client = proto::iox_authorizer_service_client::IoxAuthorizerServiceClient::new(
            ep.connect_lazy(),
        );
        Ok(Self { client })
    }
}

#[async_trait]
impl Authorizer for IoxAuthorizer {
    async fn permissions(
        &self,
        token: Option<Vec<u8>>,
        requested_perms: &[Permission],
    ) -> Result<Vec<Permission>, Error> {
        let req = proto::AuthorizeRequest {
            token: token.ok_or(Error::NoToken)?,
            permissions: requested_perms
                .iter()
                .filter_map(|p| p.clone().try_into().ok())
                .collect(),
        };
        let mut client = self.client.clone();
        let authz_rpc_result = client.authorize(req).await?;

        let intersected_perms: Vec<Permission> = authz_rpc_result
            .into_inner()
            .permissions
            .into_iter()
            .filter_map(|p| match p.try_into() {
                Ok(p) => Some(p),
                Err(e) => {
                    warn!(error=%e, "authz service returned incompatible permission");
                    None
                }
            })
            .collect();

        match (requested_perms, &intersected_perms[..]) {
            // used in connection `Authorizer::probe()`
            ([], _) => Ok(vec![]),
            // token does not have any of the requested_perms
            (_, []) => Err(Error::Forbidden),
            // if token has `any_of` the requested_perms => return ok
            _ => Ok(intersected_perms),
        }
    }
}

/// Authorization related error.
#[derive(Debug, Snafu)]
pub enum Error {
    /// Communication error when verifying a token.
    #[snafu(display("token verification not possible: {msg}"))]
    Verification {
        /// Message describing the error.
        msg: String,
        /// Source of the error.
        source: Box<dyn std::error::Error + Send + Sync + 'static>,
    },

    /// The token's permissions do not allow the operation.
    #[snafu(display("forbidden"))]
    Forbidden,

    /// No token has been supplied, but is required.
    #[snafu(display("no token"))]
    NoToken,
}

impl Error {
    /// Create new Error::Verification.
    pub fn verification(
        msg: impl Into<String>,
        source: impl Into<Box<dyn std::error::Error + Send + Sync + 'static>>,
    ) -> Self {
        Self::Verification {
            msg: msg.into(),
            source: source.into(),
        }
    }
}

impl From<tonic::Status> for Error {
    fn from(value: tonic::Status) -> Self {
        Self::verification(value.message(), value.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn verify_error_from_tonic_status() {
        let s = tonic::Status::resource_exhausted("test error");
        let e = Error::from(s);
        assert_eq!(
            "token verification not possible: test error",
            format!("{e}")
        )
    }

    #[test]
    fn test_extract_token() {
        assert_eq!(None, extract_token::<&str>(None));
        assert_eq!(None, extract_token(Some("")));
        assert_eq!(None, extract_token(Some("Basic")));
        assert_eq!(None, extract_token(Some("Basic Og=="))); // ":"
        assert_eq!(None, extract_token(Some("Basic dXNlcm5hbWU6"))); // "username:"
        assert_eq!(None, extract_token(Some("Basic Og=="))); // ":"
        assert_eq!(
            Some(b"password".to_vec()),
            extract_token(Some("Basic OnBhc3N3b3Jk"))
        ); // ":password"
        assert_eq!(
            Some(b"password2".to_vec()),
            extract_token(Some("Basic dXNlcm5hbWU6cGFzc3dvcmQy"))
        ); // "username:password2"
        assert_eq!(None, extract_token(Some("Bearer")));
        assert_eq!(None, extract_token(Some("Bearer ")));
        assert_eq!(Some(b"token".to_vec()), extract_token(Some("Bearer token")));
        assert_eq!(None, extract_token(Some("Token")));
        assert_eq!(None, extract_token(Some("Token ")));
        assert_eq!(
            Some(b"token2".to_vec()),
            extract_token(Some("Token token2"))
        );
    }
}
