use std::ops::ControlFlow;

use async_trait::async_trait;
use backoff::{Backoff, BackoffConfig};

use super::{Authorization, Error, Permission};

/// An authorizer is used to validate a request
/// (+ associated permissions needed to fulfill the request)
/// with an authorization token that has been extracted from the request.
#[async_trait]
pub trait Authorizer: std::fmt::Debug + Send + Sync {
    /// Determine the authorization provided by a request token.
    ///
    /// The returned [Authorization] contains the subject associated
    /// with the token, if available, along with the subset of the
    /// requested permissions provided by the token.
    ///
    /// Implementations of this trait should return the specified errors under
    /// the following conditions:
    ///
    /// * [`Error::InvalidToken`]: the token is invalid / in an incorrect
    ///       format / otherwise corrupt and a permission check cannot be
    ///       performed
    ///
    /// * [`Error::NoToken`]: the token was not provided
    ///
    /// * [`Error::Forbidden`]: the token was well formed, but lacks
    ///       authorisation to perform the requested action
    ///
    /// * [`Error::Verification`]: the token permissions were not possible
    ///       to validate - an internal error has occurred
    async fn authorize(
        &self,
        token: Option<Vec<u8>>,
        perms: &[Permission],
    ) -> Result<Authorization, Error>;

    /// Make a test request that determines if end-to-end communication
    /// with the service is working.
    ///
    /// Test is performed during deployment, with ordering of availability not being guaranteed.
    async fn probe(&self) -> Result<(), Error> {
        Backoff::new(&BackoffConfig::default())
            .retry_with_backoff("probe iox-authz service", async move || {
                match self.authorize(Some(b"".to_vec()), &[]).await {
                    // got response from authorizer server
                    Ok(_)
                    | Err(Error::Forbidden { .. })
                    | Err(Error::InvalidToken)
                    | Err(Error::NoToken) => ControlFlow::Break(Ok(())),
                    // communication error == Error::Verification
                    Err(e) => ControlFlow::<_, Error>::Continue(e),
                }
            })
            .await
            .expect("retry forever")
    }
}

/// Wrapped `Option<dyn Authorizer>`
/// Provides response to inner `IoxAuthorizer::permissions()`
#[async_trait]
impl<T: Authorizer> Authorizer for Option<T> {
    async fn authorize(
        &self,
        token: Option<Vec<u8>>,
        perms: &[Permission],
    ) -> Result<Authorization, Error> {
        match self {
            Some(authz) => authz.authorize(token, perms).await,
            // no authz rpc service => return same perms requested. Used for testing.
            None => Ok(Authorization::new(None, perms.to_vec())),
        }
    }
}

#[async_trait]
impl<T: AsRef<dyn Authorizer> + std::fmt::Debug + Send + Sync> Authorizer for T {
    async fn authorize(
        &self,
        token: Option<Vec<u8>>,
        perms: &[Permission],
    ) -> Result<Authorization, Error> {
        self.as_ref().authorize(token, perms).await
    }
}
