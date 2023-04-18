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
use generated_types::influxdata::iox::authz::v1 as proto;
use observability_deps::tracing::warn;
use snafu::Snafu;

mod permission;
pub use permission::{Action, Permission, Resource};

#[cfg(feature = "http")]
pub mod http;

/// An authorizer is used to validate the associated with
/// an authorization token that has been extracted from a request.
#[async_trait]
pub trait Authorizer: std::fmt::Debug + Send + Sync {
    /// Determine the permissions associated with a request token.
    ///
    /// The returned list of permissions is the intersection of the permissions
    /// requested and the permissions associated with the token. An error
    /// will only be returned if there is a failure processing the token.
    /// An invalid token is taken to have no permissions, so these along
    /// with tokens that match none of the requested permissions will return
    /// empty permission sets.
    async fn permissions(
        &self,
        token: Option<&[u8]>,
        perms: &[Permission],
    ) -> Result<Vec<Permission>, Error>;

    /// Make a test request that determines if end-to-end communication
    /// with the service is working.
    async fn probe(&self) -> Result<(), Error> {
        self.permissions(Some(b""), &[]).await?;
        Ok(())
    }

    /// Determine if a token has any of the requested permissions.
    ///
    /// If the token has none of the permissions requested then a Forbidden
    /// error is returned.
    async fn require_any_permission(
        &self,
        token: Option<&[u8]>,
        perms: &[Permission],
    ) -> Result<(), Error> {
        if self.permissions(token, perms).await?.is_empty() {
            Err(Error::Forbidden)
        } else {
            Ok(())
        }
    }
}

#[async_trait]
impl<T: Authorizer> Authorizer for Option<T> {
    async fn permissions(
        &self,
        token: Option<&[u8]>,
        perms: &[Permission],
    ) -> Result<Vec<Permission>, Error> {
        match self {
            Some(authz) => authz.permissions(token, perms).await,
            None => Ok(perms.to_vec()),
        }
    }
}

#[async_trait]
impl<T: AsRef<dyn Authorizer> + std::fmt::Debug + Send + Sync> Authorizer for T {
    async fn permissions(
        &self,
        token: Option<&[u8]>,
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
        token: Option<&[u8]>,
        perms: &[Permission],
    ) -> Result<Vec<Permission>, Error> {
        let req = proto::AuthorizeRequest {
            token: token.ok_or(Error::NoToken)?.to_vec(),
            permissions: perms
                .iter()
                .filter_map(|p| p.clone().try_into().ok())
                .collect(),
        };
        let mut client = self.client.clone();
        let resp = client.authorize(req).await?;
        Ok(resp
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
            .collect())
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
}
