use std::sync::Arc;

use async_trait::async_trait;
use authz::{Authorizer as IoxAuthorizer, Error as IoxError, Permission as IoxPermission};
use influxdb3_id::{DbId, TokenId};
use iox_time::{Time, TimeProvider};
use observability_deps::tracing::{debug, trace};
use serde::Serialize;
use sha2::{Digest, Sha512};
use std::fmt::Debug;
use thiserror::Error;

#[derive(Debug, Copy, Clone, Serialize)]
pub struct DatabaseActions(u16);

impl From<u16> for DatabaseActions {
    fn from(value: u16) -> Self {
        DatabaseActions(value)
    }
}

#[derive(Debug, Copy, Clone, Serialize)]
pub struct CrudActions(u16);

impl From<u16> for CrudActions {
    fn from(value: u16) -> Self {
        CrudActions(value)
    }
}

pub enum AccessRequest {
    Database(DbId, DatabaseActions),
    Token(TokenId, CrudActions),
    Admin,
}

#[derive(Debug, Error)]
pub enum ResourceAuthorizationError {
    #[error("unauthorized to perform requested action with the token")]
    Unauthorized,
}

#[derive(Debug, Error)]
pub enum AuthenticatorError {
    /// Error for token that is present in the request but missing in the catalog
    #[error("token provided is not present in catalog")]
    InvalidToken,
    /// Error for token that has expired
    #[error("token has expired {0}")]
    ExpiredToken(String),
    /// Error for missing token (this should really be handled at the HTTP/Grpc API layer itself)
    #[error("missing token to authenticate")]
    MissingToken,
}

impl From<AuthenticatorError> for IoxError {
    fn from(err: AuthenticatorError) -> Self {
        match err {
            AuthenticatorError::InvalidToken => IoxError::NoToken,
            AuthenticatorError::ExpiredToken(token_expiry_time) => {
                // there is no mapping to let the caller know about expired token in iox so
                // we just log it for now (only useful in debugging)
                debug!(?token_expiry_time, "supplied token has expired");
                IoxError::InvalidToken
            }
            AuthenticatorError::MissingToken => IoxError::NoToken,
        }
    }
}

#[async_trait]
pub trait AuthProvider: Debug + Send + Sync + 'static {
    /// Authenticate request, at this point token maybe missing
    async fn authenticate(
        &self,
        unhashed_token: Option<Vec<u8>>,
    ) -> Result<TokenId, AuthenticatorError>;

    /// Authorize an action for an already authenticated request
    async fn authorize_action(
        &self,
        // you must have a token if you're trying to authorize request
        token_id: &TokenId,
        access_request: AccessRequest,
    ) -> Result<(), ResourceAuthorizationError>;

    // When authenticating we fetch the token_id and add it to http req extensions and
    // later lookup extension. This extra indicator allows to check if the missing token
    // in extension is an authentication issue or not, otherwise need to pass a flag to indicate
    // whether server has been started with auth flag or not
    fn should_check_token(&self) -> bool;

    /// this is only needed so that grpc service can get hold of a `Arc<dyn IoxAuthorizer>`
    fn upcast(&self) -> Arc<dyn IoxAuthorizer>;
}

pub trait TokenProvider: Send + Debug + Sync + 'static {
    fn get_token(&self, token_hash: Vec<u8>) -> Option<Arc<TokenInfo>>;
}

#[derive(Clone, Debug)]
pub struct TokenAuthenticator {
    token_provider: Arc<dyn TokenProvider>,
    time_provider: Arc<dyn TimeProvider>,
}

impl TokenAuthenticator {
    pub fn new(
        token_provider: Arc<dyn TokenProvider>,
        time_provider: Arc<dyn TimeProvider>,
    ) -> Self {
        Self {
            token_provider,
            time_provider,
        }
    }
}

#[async_trait]
impl AuthProvider for TokenAuthenticator {
    async fn authenticate(
        &self,
        unhashed_token: Option<Vec<u8>>,
    ) -> Result<TokenId, AuthenticatorError> {
        let provided = unhashed_token
            .as_deref()
            .ok_or(AuthenticatorError::MissingToken)?;
        let hashed_token = Sha512::digest(provided);
        if let Some(token) = self.token_provider.get_token(hashed_token.to_vec()) {
            let expiry_ms = token.expiry_millis();
            let current_timestamp_ms = self.time_provider.now().timestamp_millis();
            debug!(?expiry_ms, ?current_timestamp_ms, "time comparison");
            let is_active = expiry_ms > current_timestamp_ms;
            if is_active {
                return Ok(token.id);
            } else {
                trace!(token_expiry = ?expiry_ms, "token has expired");
                let formatted = Time::from_timestamp_millis(expiry_ms)
                    .map(|time| time.to_rfc3339())
                    // this should be an error but we're within an error already,
                    // also it's only useful for debugging this error so a string
                    // default is ok here.
                    .unwrap_or("CannotBuildTime".to_owned());
                return Err(AuthenticatorError::ExpiredToken(formatted));
            }
        };
        Err(AuthenticatorError::InvalidToken)
    }

    async fn authorize_action(
        &self,
        _token_id: &TokenId,
        _access_request: AccessRequest,
    ) -> Result<(), ResourceAuthorizationError> {
        // this is a no-op in authenticator
        Ok(())
    }

    fn should_check_token(&self) -> bool {
        true
    }

    fn upcast(&self) -> Arc<dyn IoxAuthorizer> {
        let cloned_self = (*self).clone();
        Arc::new(cloned_self) as _
    }
}

#[async_trait]
impl IoxAuthorizer for TokenAuthenticator {
    async fn permissions(
        &self,
        token: Option<Vec<u8>>,
        perms: &[IoxPermission],
    ) -> Result<Vec<IoxPermission>, IoxError> {
        match self.authenticate(token).await {
            Ok(_) => {
                return Ok(perms.to_vec());
            }
            Err(err) => {
                let iox_err = err.into();
                return Err(iox_err);
            }
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct NoAuthAuthenticator;

#[async_trait]
impl IoxAuthorizer for NoAuthAuthenticator {
    async fn permissions(
        &self,
        _token: Option<Vec<u8>>,
        perms: &[IoxPermission],
    ) -> Result<Vec<IoxPermission>, IoxError> {
        Ok(perms.to_vec())
    }

    async fn probe(&self) -> Result<(), IoxError> {
        Ok(())
    }
}

#[async_trait]
impl AuthProvider for NoAuthAuthenticator {
    async fn authenticate(
        &self,
        _unhashed_token: Option<Vec<u8>>,
    ) -> Result<TokenId, AuthenticatorError> {
        Ok(TokenId::from(0))
    }

    async fn authorize_action(
        &self,
        _token_id: &TokenId,
        _access_request: AccessRequest,
    ) -> Result<(), ResourceAuthorizationError> {
        Ok(())
    }

    fn should_check_token(&self) -> bool {
        false
    }

    fn upcast(&self) -> Arc<dyn IoxAuthorizer> {
        Arc::new(*self) as _
    }
}

#[derive(Debug, Clone)]
pub struct TokenInfo {
    pub id: TokenId,
    pub name: Arc<str>,
    pub hash: Vec<u8>,
    pub description: Option<String>,
    pub created_by: Option<TokenId>,
    pub created_at: i64,
    pub updated_at: Option<i64>,
    pub updated_by: Option<TokenId>,
    pub expiry_millis: i64,
    // should be used only in enterprise
    pub permissions: Vec<Permission>,
}

impl TokenInfo {
    pub fn new(
        id: TokenId,
        name: Arc<str>,
        hash: Vec<u8>,
        created_at: i64,
        expiry_millis: Option<i64>,
    ) -> Self {
        Self {
            id,
            name,
            hash,
            created_at,
            expiry_millis: expiry_millis.unwrap_or(i64::MAX),
            description: None,
            created_by: None,
            updated_at: None,
            updated_by: None,
            permissions: Default::default(),
        }
    }

    pub fn expiry_millis(&self) -> i64 {
        self.expiry_millis
    }

    pub fn maybe_expiry_millis(&self) -> Option<i64> {
        if self.expiry_millis == i64::MAX {
            return None;
        }
        Some(self.expiry_millis)
    }

    // enterprise only
    pub fn set_permissions(&mut self, all_permissions: Vec<Permission>) {
        self.permissions = all_permissions;
    }
}

// common types

/// This permission should map exactly to any of these variations
///   --permission "db:db1,db2:read"
///   --permission "db:*:read"
///   --permission "db:db1:read,write"
///   --permission "db:*:read,write"
///   --permission "*:*:read,write" (not sure if this is useful?)
///   --permission "*:*:*" (this will be admin)
///
/// These permissions are loosely translated to ResourceType, ResourceIdentifier and Actions. There
/// are more restrictive ways to handle it to disallow certain combinations using type system
/// itself. The wildcards at each level makes the setup harder to model everything within a single
/// enum for example. So, once we add few more resources this should be fairly straightforward to
/// refine to make certain combinations impossible to set
#[derive(Debug, Clone, Serialize)]
pub struct Permission {
    pub resource_type: ResourceType,
    pub resource_identifier: ResourceIdentifier,
    pub actions: Actions,
}

#[derive(Debug, Copy, Clone, Eq, Hash, PartialEq, Serialize)]
pub enum ResourceType {
    Database,
    Token,
    Wildcard,
}

#[derive(Debug, Clone, Eq, Hash, PartialEq, Serialize)]
pub enum ResourceIdentifier {
    Database(Vec<DbId>),
    Token(Vec<TokenId>),
    Wildcard,
}

#[derive(Debug, Copy, Clone, Serialize)]
pub enum Actions {
    Database(DatabaseActions),
    Token(CrudActions),
    Wildcard,
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use influxdb3_id::TokenId;
    use iox_time::{MockProvider, Time};
    use sha2::Digest;

    use crate::{AuthProvider, AuthenticatorError, TokenAuthenticator, TokenInfo, TokenProvider};

    #[derive(Debug)]
    struct MockTokenProvider {
        hashed_token: Vec<u8>,
        expired: bool,
    }

    impl MockTokenProvider {
        pub fn new(token: &str, expired: bool) -> Self {
            let hash = sha2::Sha512::digest(token);
            Self {
                hashed_token: hash.to_vec(),
                expired,
            }
        }
    }

    impl TokenProvider for MockTokenProvider {
        fn get_token(&self, token_hash: Vec<u8>) -> Option<std::sync::Arc<crate::TokenInfo>> {
            if token_hash == self.hashed_token {
                if self.expired {
                    Some(Arc::new(TokenInfo::new(
                        TokenId::from(0),
                        "admin-token".into(),
                        self.hashed_token.clone(),
                        1000,
                        Some(1743320379000),
                    )))
                } else {
                    Some(Arc::new(TokenInfo::new(
                        TokenId::from(0),
                        "admin-token".into(),
                        self.hashed_token.clone(),
                        1000,
                        Some(i64::MAX),
                    )))
                }
            } else {
                None
            }
        }
    }

    #[test_log::test(tokio::test)]
    async fn test_authenticator_success() {
        let time_provider = MockProvider::new(Time::from_timestamp_nanos(0));
        let token = "sample-token";
        let token_provider = MockTokenProvider::new(token, false);
        let authenticator =
            TokenAuthenticator::new(Arc::new(token_provider), Arc::new(time_provider));
        let token_id = authenticator
            .authenticate(Some(token.as_bytes().to_vec()))
            .await
            .expect("to get token id after successful auth");
        assert_eq!(TokenId::from(0), token_id);
    }

    #[test_log::test(tokio::test)]
    async fn test_authenticator_missing_token() {
        let time_provider = MockProvider::new(Time::from_timestamp_nanos(0));
        let token = "sample-token";
        let token_provider = MockTokenProvider::new(token, false);
        let authenticator =
            TokenAuthenticator::new(Arc::new(token_provider), Arc::new(time_provider));
        let result = authenticator
            .authenticate(Some("not-matching-token".as_bytes().to_vec()))
            .await;
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            AuthenticatorError::InvalidToken
        ));
    }

    #[test_log::test(tokio::test)]
    async fn test_authenticator_expired_token() {
        let time_provider = MockProvider::new(Time::from_timestamp_millis(1743420379000).unwrap());
        let token = "sample-token";
        let token_provider = MockTokenProvider::new(token, true);
        let authenticator =
            TokenAuthenticator::new(Arc::new(token_provider), Arc::new(time_provider));
        let result = authenticator
            .authenticate(Some("sample-token".as_bytes().to_vec()))
            .await;
        assert!(result.is_err());
        if let AuthenticatorError::ExpiredToken(expiry_time_str) = result.unwrap_err() {
            assert_eq!("2025-03-30T07:39:39+00:00", expiry_time_str);
        } else {
            panic!("not the right type of authentication error");
        }
    }
}
