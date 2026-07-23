use std::sync::Arc;

use async_trait::async_trait;
use authz::{
    Authorization, Authorizer as IoxAuthorizer, Error as IoxError, Permission as IoxPermission,
};
use indexmap::IndexMap;
use influxdb3_id::{DbId, TokenId, UserId};
use iox_time::{Time, TimeProvider};
use observability_deps::tracing::{debug, trace};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha512};
use std::fmt::Debug;

pub mod authorizer;
pub mod permissions;
pub mod role;

pub use authorizer::{
    CatalogProvider, IdProvider, TokenAuthenticatorAndAuthorizer, TokenPermissionProvider,
};

#[derive(Debug, Copy, Clone, PartialEq, Serialize)]
pub struct DatabaseActions(u16);

#[derive(Debug, Copy, Clone, PartialEq, Serialize)]
pub struct CrudActions(u16);

#[derive(Debug, Copy, Clone, PartialEq, Serialize)]
pub struct SystemActions(u16);

#[derive(Debug, Copy, Clone, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct SystemResourceIdentifier(u16);

#[derive(Debug, Clone)]
pub enum Subject {
    /// A request authenticated by a trusted cluster-internal transport.
    ///
    /// This is only installed by server-side internode routing after mTLS or
    /// internode JWT authentication, and therefore has unrestricted access.
    Internode,
    Token(TokenId),
    User {
        user_id: UserId,
        permissions: role::Permissions,
    },
}

impl std::fmt::Display for Subject {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Subject::Internode => write!(f, "internode"),
            Subject::Token(_) => write!(f, "token"),
            Subject::User { .. } => write!(f, "user"),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum AccessRequest {
    MaybeDatabase(Option<DbId>, DatabaseActions),
    Database(DbId, DatabaseActions),
    AnyDatabase(DatabaseActions),
    Token(TokenId, CrudActions),
    System(SystemResourceIdentifier, SystemActions),
    User(role::UserAction),
    Role(role::RoleAction),
    AdminToken(role::AdminTokenAction),
    ResourceToken(role::TokenAction),
    Admin,
}

#[derive(Debug, Clone, thiserror::Error)]
pub enum ResourceAuthorizationError {
    #[error("unauthorized to perform requested action with the token")]
    Unauthorized,

    #[error("resource type not supported, {0}")]
    ResourceNotSupported(String),
}

#[derive(Debug, thiserror::Error)]
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
    /// Error for invalid JWT (bad signature, malformed, etc.)
    #[error("invalid JWT")]
    InvalidJwt,
    /// Error for expired JWT
    #[error("JWT has expired")]
    ExpiredJwt,
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
            AuthenticatorError::InvalidJwt => IoxError::InvalidToken,
            AuthenticatorError::ExpiredJwt => {
                debug!("JWT has expired");
                IoxError::InvalidToken
            }
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

    /// Authorize an action for an already authenticated request.
    ///
    /// When auth is disabled, implementations should return `Ok(())`.
    /// When auth is enabled and `subject` is `None`, implementations should
    /// return `Unauthorized`.
    async fn authorize_action(
        &self,
        subject: Option<&Subject>,
        access_request: AccessRequest,
    ) -> Result<(), ResourceAuthorizationError>;

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
        _subject: Option<&Subject>,
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
    async fn authorize(
        &self,
        token: Option<Vec<u8>>,
        perms: &[IoxPermission],
    ) -> Result<Authorization, IoxError> {
        let _token_id = self.authenticate(token).await?;
        // (trevor) The "subject" may just use the `token_id` as a string, but we withheld doing so
        // in order to decided and ensure that we make use of the subject consistently throughout
        // the system. See: https://github.com/influxdata/influxdb/issues/26440
        Ok(Authorization::new(None, perms.to_vec()))
    }
}

#[derive(Clone, Copy, Debug)]
pub struct NoAuthAuthenticator;

#[async_trait]
impl IoxAuthorizer for NoAuthAuthenticator {
    async fn authorize(
        &self,
        _token: Option<Vec<u8>>,
        perms: &[IoxPermission],
    ) -> Result<Authorization, IoxError> {
        Ok(Authorization::new(None, perms.to_vec()))
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
        _subject: Option<&Subject>,
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

#[derive(Debug, Clone, PartialEq)]
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

    /// Returns true if this token carries unrestricted admin privileges (the
    /// `*:*:*` permission) rather than being scoped to specific resources.
    /// Admin tokens are managed via admin-token permissions, not the
    /// resource-token permissions held by non-admin roles such as `Member`.
    ///
    /// This mirrors the canonical definition the catalog uses elsewhere: an
    /// admin token is exactly the single `*:*:*` permission (the v2->v3
    /// migration's `is_admin_permissions`), and `create_token_with_permission`
    /// enforces the inverse by rejecting a `*` resource type for any non-admin
    /// token. There is no separate token-type discriminator in the catalog.
    pub fn is_admin(&self) -> bool {
        matches!(
            self.permissions.as_slice(),
            [Permission {
                resource_type: ResourceType::Wildcard,
                resource_identifier: ResourceIdentifier::Wildcard,
                actions: Actions::Wildcard,
                ..
            }]
        )
    }
}

// common types

/// Metadata about a resource, including its name and deletion status
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ResourceMetadata {
    pub name: String,
    pub deleted: bool,
}

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
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Permission {
    pub resource_type: ResourceType,
    pub resource_identifier: ResourceIdentifier,
    pub actions: Actions,
    /// Optional mapping of resource IDs to their metadata, used when resources are deleted
    /// but we want to preserve the original names for display purposes
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub resource_names: Option<IndexMap<String, ResourceMetadata>>,
}

#[derive(Debug, Copy, Clone, Eq, Hash, PartialEq, Serialize)]
pub enum ResourceType {
    Database,
    Token,
    System,
    Wildcard,
}

#[derive(Debug, Clone, Eq, Hash, PartialEq, Serialize)]
pub enum ResourceIdentifier {
    Database(Vec<DbId>),
    Token(Vec<TokenId>),
    System(Vec<SystemResourceIdentifier>),
    Wildcard,
}

#[derive(Debug, Copy, Clone, PartialEq, Serialize)]
pub enum Actions {
    Database(DatabaseActions),
    Token(CrudActions),
    System(SystemActions),
    Wildcard,
}

#[cfg(test)]
mod tests;
