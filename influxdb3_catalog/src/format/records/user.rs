//! User records (record_ids 27-38).
//!
//! OAuth login identity records are enterprise-only and live in
//! [`crate::enterprise::format::records`].

use std::sync::Arc;

use crate::catalog::versions::v3::events::CatalogEvent;
use crate::catalog::versions::v3::inner::InnerCatalog;
use crate::catalog::versions::v3::schema::user::{
    LoginIdentityUsernamePassword, RefreshTokenInfo, UserInfo,
};
use crate::format::apply::ApplyError;
use crate::format::records::impl_bitcode_encoding;
use crate::format::{CatalogRecord, RecordFlags, RecordId, RegisteredRecord, record_ids};
use influxdb3_id::{RoleId, UserId};

/// Create a new user.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, bitcode::Encode, bitcode::Decode)]
pub struct CreateUser {
    /// User ID.
    pub user_id: u64,
    /// Optional display name.
    pub display_name: Option<String>,
    /// Creation timestamp in milliseconds.
    pub created_at: i64,
}

impl CatalogRecord for CreateUser {
    const ID: RecordId = record_ids::CREATE_USER;
    const FLAGS: RecordFlags = RecordFlags::none();
    const NAME: &'static str = "CreateUser";

    fn apply(&self, catalog: &mut InnerCatalog) -> Result<(), ApplyError> {
        let user_id = UserId::new(self.user_id);
        let user = UserInfo::new(
            user_id,
            self.display_name.as_ref().map(|s| Arc::from(s.as_str())),
            self.created_at,
        );
        catalog.users.add_user(user).map_err(|e| {
            ApplyError(format!(
                "{}: add user (user_id={}): {e}",
                Self::NAME,
                self.user_id
            ))
        })
    }

    fn event(&self) -> CatalogEvent {
        CatalogEvent::UserCreated {
            user_id: UserId::new(self.user_id),
        }
    }
}

inventory::submit! {
    RegisteredRecord::new::<CreateUser>()
}

/// Update a user's display name.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, bitcode::Encode, bitcode::Decode)]
pub struct UpdateUserDisplayName {
    /// User ID.
    pub user_id: u64,
    /// New display name.
    pub display_name: Option<String>,
    /// Update timestamp in milliseconds.
    pub updated_at: i64,
}

impl CatalogRecord for UpdateUserDisplayName {
    const ID: RecordId = record_ids::UPDATE_USER_DISPLAY_NAME;
    const FLAGS: RecordFlags = RecordFlags::none();
    const NAME: &'static str = "UpdateUserDisplayName";

    fn apply(&self, catalog: &mut InnerCatalog) -> Result<(), ApplyError> {
        let user_id = UserId::new(self.user_id);
        let mut user = catalog.users.get_by_id(&user_id).ok_or_else(|| {
            ApplyError(format!("{}: user {} not found", Self::NAME, self.user_id))
        })?;
        let user_mut = Arc::make_mut(&mut user);
        user_mut.display_name = self.display_name.as_ref().map(|s| Arc::from(s.as_str()));
        user_mut.updated_at = self.updated_at;
        catalog.users.update_user((*user).clone()).map_err(|e| {
            ApplyError(format!(
                "{}: update user (user_id={}): {e}",
                Self::NAME,
                self.user_id
            ))
        })
    }

    fn event(&self) -> CatalogEvent {
        CatalogEvent::UserUpdated {
            user_id: UserId::new(self.user_id),
        }
    }
}

inventory::submit! {
    RegisteredRecord::new::<UpdateUserDisplayName>()
}

/// Soft delete a user.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, bitcode::Encode, bitcode::Decode)]
pub struct DeleteUser {
    /// User ID.
    pub user_id: u64,
    /// Deletion timestamp in milliseconds.
    pub deleted_at: i64,
}

impl CatalogRecord for DeleteUser {
    const ID: RecordId = record_ids::DELETE_USER;
    const FLAGS: RecordFlags = RecordFlags::none();
    const NAME: &'static str = "DeleteUser";

    fn apply(&self, catalog: &mut InnerCatalog) -> Result<(), ApplyError> {
        let user_id = UserId::new(self.user_id);
        let mut user = catalog.users.get_by_id(&user_id).ok_or_else(|| {
            ApplyError(format!("{}: user {} not found", Self::NAME, self.user_id))
        })?;
        let user_mut = Arc::make_mut(&mut user);
        user_mut.deleted_at = Some(self.deleted_at);
        // Remove from lookups so the username/OAuth ID can be reused
        catalog.users.remove_username_lookup(&user_id);
        catalog.users.remove_oauth_lookup(&user_id);
        catalog.users.update_user((*user).clone()).map_err(|e| {
            ApplyError(format!(
                "{}: delete user (user_id={}): {e}",
                Self::NAME,
                self.user_id
            ))
        })
    }

    fn event(&self) -> CatalogEvent {
        CatalogEvent::UserDeleted {
            user_id: UserId::new(self.user_id),
        }
    }
}

inventory::submit! {
    RegisteredRecord::new::<DeleteUser>()
}

/// Restore a deleted user.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, bitcode::Encode, bitcode::Decode)]
pub struct RestoreUser {
    /// User ID.
    pub user_id: u64,
    /// Optional new display name.
    pub display_name: Option<Arc<str>>,
    /// Restore timestamp in milliseconds.
    pub restored_at: i64,
}

impl CatalogRecord for RestoreUser {
    const ID: RecordId = record_ids::RESTORE_USER;
    const FLAGS: RecordFlags = RecordFlags::none();
    const NAME: &'static str = "RestoreUser";

    fn apply(&self, catalog: &mut InnerCatalog) -> Result<(), ApplyError> {
        let user_id = UserId::new(self.user_id);
        catalog
            .users
            .restore_user(
                user_id,
                self.display_name.as_ref().map(Arc::clone),
                self.restored_at,
            )
            .map_err(|e| {
                ApplyError(format!(
                    "{}: restore user (user_id={}): {e}",
                    Self::NAME,
                    self.user_id
                ))
            })
    }

    fn event(&self) -> CatalogEvent {
        CatalogEvent::UserRestored {
            user_id: UserId::new(self.user_id),
        }
    }
}

inventory::submit! {
    RegisteredRecord::new::<RestoreUser>()
}

/// Add a username/password login identity to a user.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, bitcode::Encode, bitcode::Decode)]
pub struct CreateLoginIdentityUsernamePassword {
    /// User ID.
    pub user_id: u64,
    /// Username.
    pub username: String,
    /// Password hash.
    pub password_hash: String,
    /// Whether the user must reset their password on next login.
    pub requires_password_reset: bool,
    /// Creation timestamp in milliseconds.
    pub created_at: i64,
}

impl CatalogRecord for CreateLoginIdentityUsernamePassword {
    const ID: RecordId = record_ids::CREATE_LOGIN_IDENTITY_USERNAME_PASSWORD;
    const FLAGS: RecordFlags = RecordFlags::none();
    const NAME: &'static str = "CreateLoginIdentityUsernamePassword";

    fn apply(&self, catalog: &mut InnerCatalog) -> Result<(), ApplyError> {
        let user_id = UserId::new(self.user_id);
        let identity = LoginIdentityUsernamePassword::new(
            Arc::from(self.username.as_str()),
            Arc::from(self.password_hash.as_str()),
            self.requires_password_reset,
            self.created_at,
        );
        catalog
            .users
            .add_login_identity_username_password(user_id, identity)
            .map_err(|e| {
                ApplyError(format!(
                    "{}: add login identity (user_id={}, username='{}'): {e}",
                    Self::NAME,
                    self.user_id,
                    self.username
                ))
            })
    }

    fn event(&self) -> CatalogEvent {
        CatalogEvent::LoginIdentityCreated {
            user_id: UserId::new(self.user_id),
        }
    }
}

inventory::submit! {
    RegisteredRecord::new::<CreateLoginIdentityUsernamePassword>()
}

/// Update the password hash for a user's login identity.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, bitcode::Encode, bitcode::Decode)]
pub struct UpdateLoginIdentityPasswordHash {
    /// User ID.
    pub user_id: u64,
    /// New password hash.
    pub password_hash: String,
    /// Update timestamp in milliseconds.
    pub updated_at: i64,
}

impl CatalogRecord for UpdateLoginIdentityPasswordHash {
    const ID: RecordId = record_ids::UPDATE_LOGIN_IDENTITY_PASSWORD_HASH;
    const FLAGS: RecordFlags = RecordFlags::none();
    const NAME: &'static str = "UpdateLoginIdentityPasswordHash";

    fn apply(&self, catalog: &mut InnerCatalog) -> Result<(), ApplyError> {
        let user_id = UserId::new(self.user_id);
        catalog
            .users
            .update_login_identity_password(
                user_id,
                Arc::from(self.password_hash.as_str()),
                self.updated_at,
            )
            .map_err(|e| {
                ApplyError(format!(
                    "{}: update password (user_id={}): {e}",
                    Self::NAME,
                    self.user_id
                ))
            })
    }

    fn event(&self) -> CatalogEvent {
        CatalogEvent::LoginIdentityUpdated {
            user_id: UserId::new(self.user_id),
        }
    }
}

inventory::submit! {
    RegisteredRecord::new::<UpdateLoginIdentityPasswordHash>()
}

/// Update the requires_password_reset flag for a user's login identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, bitcode::Encode, bitcode::Decode)]
pub struct UpdateLoginIdentityRequiresPasswordReset {
    /// User ID.
    pub user_id: u64,
    /// New value for requires_password_reset.
    pub requires_password_reset: bool,
    /// Update timestamp in milliseconds.
    pub updated_at: i64,
}

impl CatalogRecord for UpdateLoginIdentityRequiresPasswordReset {
    const ID: RecordId = record_ids::UPDATE_LOGIN_IDENTITY_REQUIRES_PASSWORD_RESET;
    const FLAGS: RecordFlags = RecordFlags::none();
    const NAME: &'static str = "UpdateLoginIdentityRequiresPasswordReset";

    fn apply(&self, catalog: &mut InnerCatalog) -> Result<(), ApplyError> {
        let user_id = UserId::new(self.user_id);
        catalog
            .users
            .update_login_identity_requires_password_reset(
                user_id,
                self.requires_password_reset,
                self.updated_at,
            )
            .map_err(|e| {
                ApplyError(format!(
                    "{}: update requires_password_reset (user_id={}): {e}",
                    Self::NAME,
                    self.user_id
                ))
            })
    }

    fn event(&self) -> CatalogEvent {
        CatalogEvent::LoginIdentityUpdated {
            user_id: UserId::new(self.user_id),
        }
    }
}

inventory::submit! {
    RegisteredRecord::new::<UpdateLoginIdentityRequiresPasswordReset>()
}

/// Delete the username/password login identity for a user.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, bitcode::Encode, bitcode::Decode)]
pub struct DeleteLoginIdentityUsernamePassword {
    /// User ID.
    pub user_id: u64,
}

impl CatalogRecord for DeleteLoginIdentityUsernamePassword {
    const ID: RecordId = record_ids::DELETE_LOGIN_IDENTITY_USERNAME_PASSWORD;
    const FLAGS: RecordFlags = RecordFlags::none();
    const NAME: &'static str = "DeleteLoginIdentityUsernamePassword";

    fn apply(&self, catalog: &mut InnerCatalog) -> Result<(), ApplyError> {
        let user_id = UserId::new(self.user_id);
        catalog
            .users
            .delete_login_identity_username_password(&user_id)
            .map_err(|e| {
                ApplyError(format!(
                    "{}: delete login identity (user_id={}): {e}",
                    Self::NAME,
                    self.user_id
                ))
            })
    }

    fn event(&self) -> CatalogEvent {
        CatalogEvent::LoginIdentityDeleted {
            user_id: UserId::new(self.user_id),
        }
    }
}

inventory::submit! {
    RegisteredRecord::new::<DeleteLoginIdentityUsernamePassword>()
}

/// Create a refresh token for a user.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, bitcode::Encode, bitcode::Decode)]
pub struct CreateRefreshToken {
    /// User ID.
    pub user_id: u64,
    /// Token hash.
    pub token_hash: String,
    /// Creation timestamp in milliseconds.
    pub created_at: i64,
    /// Expiry timestamp in milliseconds.
    pub expires_at: i64,
}

impl CatalogRecord for CreateRefreshToken {
    const ID: RecordId = record_ids::CREATE_REFRESH_TOKEN;
    const FLAGS: RecordFlags = RecordFlags::none();
    const NAME: &'static str = "CreateRefreshToken";

    fn apply(&self, catalog: &mut InnerCatalog) -> Result<(), ApplyError> {
        let user_id = UserId::new(self.user_id);
        let token = RefreshTokenInfo {
            token_hash: Arc::from(self.token_hash.as_str()),
            user_id,
            created_at: self.created_at,
            expires_at: self.expires_at,
            revoked_at: None,
        };
        catalog.users.refresh_tokens_mut().add_token(token);

        // Clean up expired tokens to prevent orphaned tokens from piling up
        catalog
            .users
            .refresh_tokens_mut()
            .cleanup_expired(self.created_at);

        Ok(())
    }

    fn event(&self) -> CatalogEvent {
        CatalogEvent::RefreshTokenCreated {
            user_id: UserId::new(self.user_id),
        }
    }
}

inventory::submit! {
    RegisteredRecord::new::<CreateRefreshToken>()
}

/// Revoke a specific refresh token.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, bitcode::Encode, bitcode::Decode)]
pub struct RevokeRefreshToken {
    /// User ID (stored during prepare for correct event emission).
    pub user_id: u64,
    /// Token hash.
    pub token_hash: String,
    /// Revocation timestamp in milliseconds.
    pub revoked_at: i64,
}

impl CatalogRecord for RevokeRefreshToken {
    const ID: RecordId = record_ids::REVOKE_REFRESH_TOKEN;
    const FLAGS: RecordFlags = RecordFlags::none();
    const NAME: &'static str = "RevokeRefreshToken";

    fn apply(&self, catalog: &mut InnerCatalog) -> Result<(), ApplyError> {
        if catalog
            .users
            .refresh_tokens()
            .get_by_hash(&self.token_hash)
            .is_none()
        {
            return Err(ApplyError(format!(
                "{}: token not found (hash='{}')",
                Self::NAME,
                self.token_hash
            )));
        }
        catalog
            .users
            .refresh_tokens_mut()
            .revoke(&self.token_hash, self.revoked_at);
        Ok(())
    }

    fn event(&self) -> CatalogEvent {
        CatalogEvent::RefreshTokenRevoked {
            user_id: UserId::new(self.user_id),
        }
    }
}

inventory::submit! {
    RegisteredRecord::new::<RevokeRefreshToken>()
}

/// Revoke all refresh tokens for a user.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, bitcode::Encode, bitcode::Decode)]
pub struct RevokeAllRefreshTokensForUser {
    /// User ID.
    pub user_id: u64,
    /// Revocation timestamp in milliseconds.
    pub revoked_at: i64,
}

impl CatalogRecord for RevokeAllRefreshTokensForUser {
    const ID: RecordId = record_ids::REVOKE_ALL_REFRESH_TOKENS_FOR_USER;
    const FLAGS: RecordFlags = RecordFlags::none();
    const NAME: &'static str = "RevokeAllRefreshTokensForUser";

    fn apply(&self, catalog: &mut InnerCatalog) -> Result<(), ApplyError> {
        let user_id = UserId::new(self.user_id);
        catalog
            .users
            .refresh_tokens_mut()
            .revoke_all_for_user(&user_id, self.revoked_at);
        Ok(())
    }

    fn event(&self) -> CatalogEvent {
        CatalogEvent::AllRefreshTokensRevoked {
            user_id: UserId::new(self.user_id),
        }
    }
}

inventory::submit! {
    RegisteredRecord::new::<RevokeAllRefreshTokensForUser>()
}

/// Update the roles assigned to a user.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, bitcode::Encode, bitcode::Decode)]
pub struct UpdateUserRoles {
    /// User ID.
    pub user_id: u64,
    /// New list of role IDs.
    pub role_ids: Vec<u64>,
    /// Update timestamp in milliseconds.
    pub updated_at: i64,
}

impl CatalogRecord for UpdateUserRoles {
    const ID: RecordId = record_ids::UPDATE_USER_ROLES;
    const FLAGS: RecordFlags = RecordFlags::none();
    const NAME: &'static str = "UpdateUserRoles";

    fn apply(&self, catalog: &mut InnerCatalog) -> Result<(), ApplyError> {
        let user_id = UserId::new(self.user_id);
        let mut user = catalog.users.get_by_id(&user_id).ok_or_else(|| {
            ApplyError(format!("{}: user {} not found", Self::NAME, self.user_id))
        })?;
        let user_mut = Arc::make_mut(&mut user);
        user_mut.role_ids = self.role_ids.iter().map(|&id| RoleId::new(id)).collect();
        user_mut.updated_at = self.updated_at;
        catalog.users.update_user((*user).clone()).map_err(|e| {
            ApplyError(format!(
                "{}: update user roles (user_id={}): {e}",
                Self::NAME,
                self.user_id
            ))
        })
    }

    fn event(&self) -> CatalogEvent {
        CatalogEvent::UserRolesUpdated {
            user_id: UserId::new(self.user_id),
            role_ids: self.role_ids.iter().copied().map(RoleId::new).collect(),
        }
    }
}

inventory::submit! {
    RegisteredRecord::new::<UpdateUserRoles>()
}

impl_bitcode_encoding!(
    CreateUser,
    UpdateUserDisplayName,
    DeleteUser,
    RestoreUser,
    CreateLoginIdentityUsernamePassword,
    UpdateLoginIdentityPasswordHash,
    UpdateLoginIdentityRequiresPasswordReset,
    DeleteLoginIdentityUsernamePassword,
    CreateRefreshToken,
    RevokeRefreshToken,
    RevokeAllRefreshTokensForUser,
    UpdateUserRoles,
);

#[cfg(test)]
mod tests;
