//! User records (record_ids 27-38).
//!
//! OAuth login identity records are enterprise-only and live in
//! [`crate::enterprise::format::records`].

use std::sync::Arc;

use influxdb3_catalog_macros::catalog_record;

use crate::catalog::versions::v3::events::CatalogEvent;
use crate::catalog::versions::v3::inner::InnerCatalog;
use crate::catalog::versions::v3::schema::user::{
    LoginIdentityUsernamePassword, RefreshTokenInfo, UserInfo,
};
use crate::format::apply::ApplyError;
use crate::format::{CatalogRecord, RecordApply, record_ids};
use influxdb3_id::{RoleId, UserId};

/// Create a new user.
#[catalog_record(id = record_ids::CREATE_USER, shape = 0x52df4d7a)]
pub struct CreateUser {
    /// User ID.
    pub user_id: u64,
    /// Optional display name.
    pub display_name: Option<String>,
    /// Creation timestamp in milliseconds.
    pub created_at: i64,
}

impl RecordApply for CreateUser {
    fn apply(&self, catalog: &mut InnerCatalog) -> Result<(), ApplyError> {
        let user_id = UserId::new(self.user_id);
        let user = UserInfo::new(user_id, self.display_name.clone(), self.created_at);
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

/// Update a user's display name.
#[catalog_record(id = record_ids::UPDATE_USER_DISPLAY_NAME, shape = 0x52df4d7a)]
pub struct UpdateUserDisplayName {
    /// User ID.
    pub user_id: u64,
    /// New display name.
    pub display_name: Option<String>,
    /// Update timestamp in milliseconds.
    pub updated_at: i64,
}

impl RecordApply for UpdateUserDisplayName {
    fn apply(&self, catalog: &mut InnerCatalog) -> Result<(), ApplyError> {
        let user_id = UserId::new(self.user_id);
        let mut user = catalog.users.get_by_id(&user_id).ok_or_else(|| {
            ApplyError(format!("{}: user {} not found", Self::NAME, self.user_id))
        })?;
        let user_mut = Arc::make_mut(&mut user);
        user_mut.display_name = self.display_name.clone();
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

/// Soft delete a user.
#[catalog_record(id = record_ids::DELETE_USER, shape = 0xa1a5e3bd)]
#[derive(Copy)]
pub struct DeleteUser {
    /// User ID.
    pub user_id: u64,
    /// Deletion timestamp in milliseconds.
    pub deleted_at: i64,
}

impl RecordApply for DeleteUser {
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

/// Restore a deleted user.
#[catalog_record(id = record_ids::RESTORE_USER, shape = 0x52df4d7a)]
pub struct RestoreUser {
    /// User ID.
    pub user_id: u64,
    /// Optional new display name.
    pub display_name: Option<String>,
    /// Restore timestamp in milliseconds.
    pub restored_at: i64,
}

impl RecordApply for RestoreUser {
    fn apply(&self, catalog: &mut InnerCatalog) -> Result<(), ApplyError> {
        let user_id = UserId::new(self.user_id);
        catalog
            .users
            .restore_user(user_id, self.display_name.clone(), self.restored_at)
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

/// Add a username/password login identity to a user.
#[catalog_record(id = record_ids::CREATE_LOGIN_IDENTITY_USERNAME_PASSWORD, shape = 0xaf84d6fe)]
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

impl RecordApply for CreateLoginIdentityUsernamePassword {
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

/// Update the password hash for a user's login identity.
#[catalog_record(id = record_ids::UPDATE_LOGIN_IDENTITY_PASSWORD_HASH, shape = 0x7bd5ec8d)]
pub struct UpdateLoginIdentityPasswordHash {
    /// User ID.
    pub user_id: u64,
    /// New password hash.
    pub password_hash: String,
    /// Update timestamp in milliseconds.
    pub updated_at: i64,
}

impl RecordApply for UpdateLoginIdentityPasswordHash {
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

/// Update the requires_password_reset flag for a user's login identity.
#[catalog_record(id = record_ids::UPDATE_LOGIN_IDENTITY_REQUIRES_PASSWORD_RESET, shape = 0xc670f27e)]
#[derive(Copy)]
pub struct UpdateLoginIdentityRequiresPasswordReset {
    /// User ID.
    pub user_id: u64,
    /// New value for requires_password_reset.
    pub requires_password_reset: bool,
    /// Update timestamp in milliseconds.
    pub updated_at: i64,
}

impl RecordApply for UpdateLoginIdentityRequiresPasswordReset {
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

/// Delete the username/password login identity for a user.
#[catalog_record(id = record_ids::DELETE_LOGIN_IDENTITY_USERNAME_PASSWORD, shape = 0x12f9b977)]
#[derive(Copy)]
pub struct DeleteLoginIdentityUsernamePassword {
    /// User ID.
    pub user_id: u64,
}

impl RecordApply for DeleteLoginIdentityUsernamePassword {
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

/// Create a refresh token for a user.
#[catalog_record(id = record_ids::CREATE_REFRESH_TOKEN, shape = 0x7446b063)]
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

impl RecordApply for CreateRefreshToken {
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

/// Revoke a specific refresh token.
#[catalog_record(id = record_ids::REVOKE_REFRESH_TOKEN, shape = 0x7bd5ec8d)]
pub struct RevokeRefreshToken {
    /// User ID (stored during prepare for correct event emission).
    pub user_id: u64,
    /// Token hash.
    pub token_hash: String,
    /// Revocation timestamp in milliseconds.
    pub revoked_at: i64,
}

impl RecordApply for RevokeRefreshToken {
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

/// Revoke all refresh tokens for a user.
#[catalog_record(id = record_ids::REVOKE_ALL_REFRESH_TOKENS_FOR_USER, shape = 0xa1a5e3bd)]
#[derive(Copy)]
pub struct RevokeAllRefreshTokensForUser {
    /// User ID.
    pub user_id: u64,
    /// Revocation timestamp in milliseconds.
    pub revoked_at: i64,
}

impl RecordApply for RevokeAllRefreshTokensForUser {
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

/// Update the roles assigned to a user.
#[catalog_record(id = record_ids::UPDATE_USER_ROLES, shape = 0x11962ce7)]
pub struct UpdateUserRoles {
    /// User ID.
    pub user_id: u64,
    /// New list of role IDs.
    pub role_ids: Vec<u64>,
    /// Update timestamp in milliseconds.
    pub updated_at: i64,
}

impl RecordApply for UpdateUserRoles {
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

#[cfg(test)]
mod tests;
