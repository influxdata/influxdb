use async_trait::async_trait;
use authz::{Authorizer, Error, Permission};

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
        let Some(provided) = token.as_deref() else {
            return Err(Error::NoToken);
        };
        if provided == self.token.as_slice() {
            Ok(perms.to_vec())
        } else {
            Err(Error::InvalidToken)
        }
    }

    async fn probe(&self) -> Result<(), Error> {
        Ok(())
    }
}

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
