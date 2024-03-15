use async_trait::async_trait;
use authz::{Authorizer, Error, Permission};
use observability_deps::tracing::{debug, warn};
use sha2::{Digest, Sha512};

/// An [`Authorizer`] implementation that will grant access to all
/// requests that provide `token`
#[derive(Debug)]
pub struct AllOrNothingAuthorizer {
    token: Vec<u8>,
}

impl AllOrNothingAuthorizer {
    pub fn new(token: Vec<u8>) -> Self {
        Self { token }
    }
}

#[async_trait]
impl Authorizer for AllOrNothingAuthorizer {
    async fn permissions(
        &self,
        token: Option<Vec<u8>>,
        perms: &[Permission],
    ) -> Result<Vec<Permission>, Error> {
        debug!(?perms, "requesting permissions");
        let provided = token.as_deref().ok_or(Error::NoToken)?;
        if Sha512::digest(provided)[..] == self.token {
            Ok(perms.to_vec())
        } else {
            warn!("invalid token provided");
            Err(Error::InvalidToken)
        }
    }

    async fn probe(&self) -> Result<(), Error> {
        Ok(())
    }
}

/// The defult [`Authorizer`] implementation that will authorize all requests
#[derive(Debug)]
pub struct DefaultAuthorizer;

#[async_trait]
impl Authorizer for DefaultAuthorizer {
    async fn permissions(
        &self,
        _token: Option<Vec<u8>>,
        perms: &[Permission],
    ) -> Result<Vec<Permission>, Error> {
        Ok(perms.to_vec())
    }

    async fn probe(&self) -> Result<(), Error> {
        Ok(())
    }
}
