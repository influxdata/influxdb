//! User operations: user CRUD, login identities (username/password, OAuth), refresh tokens, role assignment.

use std::sync::Arc;

use super::CatalogOp;
use crate::CatalogError;
use crate::catalog::versions::v3::inner::InnerCatalog;
use crate::catalog::versions::v3::schema::user::UserInfo;
use crate::enterprise::format::records::{CreateLoginIdentityOAuth, DeleteLoginIdentityOAuth};
use crate::format::RecordBatch;
use crate::format::records::user::{
    CreateLoginIdentityUsernamePassword, CreateRefreshToken, CreateUser,
    DeleteLoginIdentityUsernamePassword, DeleteUser, RestoreUser, RevokeAllRefreshTokensForUser,
    RevokeRefreshToken, UpdateLoginIdentityPasswordHash, UpdateLoginIdentityRequiresPasswordReset,
    UpdateUserDisplayName, UpdateUserRoles,
};
use influxdb3_id::{RoleId, UserId};

// ---------------------------------------------------------------------------
// CreateUser
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub(crate) struct CreateUserArgs {
    pub display_name: Option<String>,
    pub created_at: i64,
}

pub(crate) struct CreateUserOp {
    user_id: UserId,
}

impl CatalogOp for CreateUserOp {
    type Input = CreateUserArgs;
    type Output = Arc<UserInfo>;

    fn prepare(
        args: &Self::Input,
        catalog: &InnerCatalog,
        records: &mut RecordBatch,
    ) -> Result<Self, CatalogError> {
        let user_id = catalog.users.repo().next_id();
        records.push(&CreateUser {
            user_id: user_id.get(),
            display_name: args.display_name.as_ref().map(|d| d.as_str().to_string()),
            created_at: args.created_at,
        });

        Ok(Self { user_id })
    }

    fn output(&self, catalog: &InnerCatalog) -> Self::Output {
        catalog
            .users
            .get_by_id(&self.user_id)
            .expect("user should exist after apply")
    }
}

// ---------------------------------------------------------------------------
// UpdateUserDisplayName
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub(crate) struct UpdateUserDisplayNameArgs {
    pub user_id: UserId,
    pub display_name: Option<String>,
    pub updated_at: i64,
}

pub(crate) struct UpdateUserDisplayNameOp {
    user_id: UserId,
}

impl CatalogOp for UpdateUserDisplayNameOp {
    type Input = UpdateUserDisplayNameArgs;
    type Output = Arc<UserInfo>;

    fn prepare(
        args: &Self::Input,
        catalog: &InnerCatalog,
        records: &mut RecordBatch,
    ) -> Result<Self, CatalogError> {
        let user = catalog
            .users
            .get_by_id(&args.user_id)
            .ok_or_else(|| CatalogError::NotFound(format!("user {}", args.user_id)))?;

        if user.is_deleted() {
            return Err(CatalogError::AlreadyDeleted(format!(
                "user {}",
                args.user_id
            )));
        }

        records.push(&UpdateUserDisplayName {
            user_id: args.user_id.get(),
            display_name: args.display_name.as_ref().map(|d| d.as_str().to_string()),
            updated_at: args.updated_at,
        });
        Ok(Self {
            user_id: args.user_id,
        })
    }

    fn output(&self, catalog: &InnerCatalog) -> Self::Output {
        catalog
            .users
            .get_by_id(&self.user_id)
            .expect("user should exist after apply")
    }
}

// ---------------------------------------------------------------------------
// DeleteUser
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy)]
pub(crate) struct DeleteUserArgs {
    pub user_id: UserId,
    pub deleted_at: i64,
}

pub(crate) struct DeleteUserOp {
    user_id: UserId,
}

impl CatalogOp for DeleteUserOp {
    type Input = DeleteUserArgs;
    type Output = Arc<UserInfo>;

    fn prepare(
        args: &Self::Input,
        catalog: &InnerCatalog,
        records: &mut RecordBatch,
    ) -> Result<Self, CatalogError> {
        let user = catalog
            .users
            .get_by_id(&args.user_id)
            .ok_or_else(|| CatalogError::NotFound(format!("user {}", args.user_id)))?;

        if user.is_deleted() {
            return Err(CatalogError::AlreadyDeleted(format!(
                "user {}",
                args.user_id
            )));
        }

        records.push(&DeleteUser {
            user_id: args.user_id.get(),
            deleted_at: args.deleted_at,
        });

        Ok(Self {
            user_id: args.user_id,
        })
    }

    fn output(&self, catalog: &InnerCatalog) -> Self::Output {
        catalog
            .users
            .get_by_id(&self.user_id)
            .expect("user should exist after soft delete")
    }
}

// ---------------------------------------------------------------------------
// RestoreUser
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub(crate) struct RestoreUserArgs {
    pub user_id: UserId,
    pub display_name: Option<String>,
    pub restored_at: i64,
}

pub(crate) struct RestoreUserOp {
    user_id: UserId,
}

impl CatalogOp for RestoreUserOp {
    type Input = RestoreUserArgs;
    type Output = Arc<UserInfo>;

    fn prepare(
        args: &Self::Input,
        catalog: &InnerCatalog,
        records: &mut RecordBatch,
    ) -> Result<Self, CatalogError> {
        let user = catalog
            .users
            .get_by_id(&args.user_id)
            .ok_or_else(|| CatalogError::NotFound(format!("user {}", args.user_id)))?;

        if !user.is_deleted() {
            return Err(CatalogError::AlreadyExists);
        }

        if let Some(ref identity) = user.login_identities.username_password
            && let Some(existing_user) = catalog.users.get_by_username(&identity.username)
            && existing_user.id != args.user_id
        {
            return Err(CatalogError::invalid_configuration(format!(
                "username '{}' is already in use by another user",
                identity.username
            )));
        }

        if let Some(ref oauth) = user.login_identities.oauth
            && let Some(existing_user) = catalog.users.get_by_oauth_id(&oauth.oauth_id)
            && existing_user.id != args.user_id
        {
            return Err(CatalogError::invalid_configuration(format!(
                "OAuth ID '{}' is already in use by another user",
                oauth.oauth_id
            )));
        }

        records.push(&RestoreUser {
            user_id: args.user_id.get(),
            display_name: args.display_name.as_ref().map(|d| d.as_str().to_string()),
            restored_at: args.restored_at,
        });

        Ok(Self {
            user_id: args.user_id,
        })
    }

    fn output(&self, catalog: &InnerCatalog) -> Self::Output {
        catalog
            .users
            .get_by_id(&self.user_id)
            .expect("user should exist after restore")
    }
}

// ---------------------------------------------------------------------------
// CreateLoginIdentityUsernamePassword
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub(crate) struct CreateLoginIdentityUsernamePasswordArgs {
    pub user_id: UserId,
    pub username: String,
    pub password_hash: String,
    pub requires_password_reset: bool,
    pub created_at: i64,
}

pub(crate) struct CreateLoginIdentityUsernamePasswordOp {
    user_id: UserId,
}

impl CatalogOp for CreateLoginIdentityUsernamePasswordOp {
    type Input = CreateLoginIdentityUsernamePasswordArgs;
    type Output = Arc<UserInfo>;

    fn prepare(
        args: &Self::Input,
        catalog: &InnerCatalog,
        records: &mut RecordBatch,
    ) -> Result<Self, CatalogError> {
        let user = catalog
            .users
            .get_by_id(&args.user_id)
            .ok_or_else(|| CatalogError::NotFound(format!("user {}", args.user_id)))?;

        if user.is_deleted() {
            return Err(CatalogError::AlreadyDeleted(format!(
                "user {}",
                args.user_id
            )));
        }

        if user.login_identities.username_password.is_some() {
            return Err(CatalogError::AlreadyExists);
        }

        if catalog.users.username_exists(&args.username) {
            return Err(CatalogError::AlreadyExists);
        }

        records.push(&CreateLoginIdentityUsernamePassword {
            user_id: args.user_id.get(),
            username: args.username.clone(),
            password_hash: args.password_hash.clone(),
            requires_password_reset: args.requires_password_reset,
            created_at: args.created_at,
        });

        Ok(Self {
            user_id: args.user_id,
        })
    }

    fn output(&self, catalog: &InnerCatalog) -> Self::Output {
        catalog
            .users
            .get_by_id(&self.user_id)
            .expect("user should exist after adding login identity")
    }
}

// ---------------------------------------------------------------------------
// UpdateLoginIdentityPasswordHash
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub(crate) struct UpdateLoginIdentityPasswordHashArgs {
    pub user_id: UserId,
    pub password_hash: String,
    pub updated_at: i64,
}

pub(crate) struct UpdateLoginIdentityPasswordHashOp {
    user_id: UserId,
}

impl CatalogOp for UpdateLoginIdentityPasswordHashOp {
    type Input = UpdateLoginIdentityPasswordHashArgs;
    type Output = Arc<UserInfo>;

    fn prepare(
        args: &Self::Input,
        catalog: &InnerCatalog,
        records: &mut RecordBatch,
    ) -> Result<Self, CatalogError> {
        let user = catalog
            .users
            .get_by_id(&args.user_id)
            .ok_or_else(|| CatalogError::NotFound(format!("user {}", args.user_id)))?;

        if user.login_identities.username_password.is_none() {
            return Err(CatalogError::NotFound(format!(
                "login identity for user {}",
                args.user_id
            )));
        }

        records.push(&UpdateLoginIdentityPasswordHash {
            user_id: args.user_id.get(),
            password_hash: args.password_hash.clone(),
            updated_at: args.updated_at,
        });

        Ok(Self {
            user_id: args.user_id,
        })
    }

    fn output(&self, catalog: &InnerCatalog) -> Self::Output {
        catalog
            .users
            .get_by_id(&self.user_id)
            .expect("user should exist after password update")
    }
}

// ---------------------------------------------------------------------------
// UpdateLoginIdentityRequiresPasswordReset
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy)]
pub(crate) struct UpdateLoginIdentityRequiresPasswordResetArgs {
    pub user_id: UserId,
    pub requires_password_reset: bool,
    pub updated_at: i64,
}

pub(crate) struct UpdateLoginIdentityRequiresPasswordResetOp {
    user_id: UserId,
}

impl CatalogOp for UpdateLoginIdentityRequiresPasswordResetOp {
    type Input = UpdateLoginIdentityRequiresPasswordResetArgs;
    type Output = Arc<UserInfo>;

    fn prepare(
        args: &Self::Input,
        catalog: &InnerCatalog,
        records: &mut RecordBatch,
    ) -> Result<Self, CatalogError> {
        let user = catalog
            .users
            .get_by_id(&args.user_id)
            .ok_or_else(|| CatalogError::NotFound(format!("user {}", args.user_id)))?;

        if user.login_identities.username_password.is_none() {
            return Err(CatalogError::NotFound(format!(
                "login identity for user {}",
                args.user_id
            )));
        }

        records.push(&UpdateLoginIdentityRequiresPasswordReset {
            user_id: args.user_id.get(),
            requires_password_reset: args.requires_password_reset,
            updated_at: args.updated_at,
        });

        Ok(Self {
            user_id: args.user_id,
        })
    }

    fn output(&self, catalog: &InnerCatalog) -> Self::Output {
        catalog
            .users
            .get_by_id(&self.user_id)
            .expect("user should exist after password reset flag update")
    }
}

// ---------------------------------------------------------------------------
// DeleteLoginIdentityUsernamePassword
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy)]
pub(crate) struct DeleteLoginIdentityUsernamePasswordArgs {
    pub user_id: UserId,
}

pub(crate) struct DeleteLoginIdentityUsernamePasswordOp {
    user_id: UserId,
}

impl CatalogOp for DeleteLoginIdentityUsernamePasswordOp {
    type Input = DeleteLoginIdentityUsernamePasswordArgs;
    type Output = Arc<UserInfo>;

    fn prepare(
        args: &Self::Input,
        catalog: &InnerCatalog,
        records: &mut RecordBatch,
    ) -> Result<Self, CatalogError> {
        let user = catalog
            .users
            .get_by_id(&args.user_id)
            .ok_or_else(|| CatalogError::NotFound(format!("user {}", args.user_id)))?;

        if user.login_identities.username_password.is_none() {
            return Err(CatalogError::NotFound(format!(
                "login identity for user {}",
                args.user_id
            )));
        }

        records.push(&DeleteLoginIdentityUsernamePassword {
            user_id: args.user_id.get(),
        });

        Ok(Self {
            user_id: args.user_id,
        })
    }

    fn output(&self, catalog: &InnerCatalog) -> Self::Output {
        catalog
            .users
            .get_by_id(&self.user_id)
            .expect("user should exist after deleting login identity")
    }
}

// ---------------------------------------------------------------------------
// CreateLoginIdentityOAuth
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub(crate) struct CreateLoginIdentityOAuthArgs {
    pub user_id: UserId,
    pub oauth_id: String,
    pub created_at: i64,
}

pub(crate) struct CreateLoginIdentityOAuthOp {
    user_id: UserId,
}

impl CatalogOp for CreateLoginIdentityOAuthOp {
    type Input = CreateLoginIdentityOAuthArgs;
    type Output = Arc<UserInfo>;

    fn prepare(
        args: &Self::Input,
        catalog: &InnerCatalog,
        records: &mut RecordBatch,
    ) -> Result<Self, CatalogError> {
        let user = catalog
            .users
            .get_by_id(&args.user_id)
            .ok_or_else(|| CatalogError::NotFound(format!("user {}", args.user_id)))?;

        if user.is_deleted() {
            return Err(CatalogError::AlreadyDeleted(format!(
                "user {}",
                args.user_id
            )));
        }

        if user.login_identities.oauth.is_some() {
            return Err(CatalogError::AlreadyExists);
        }

        if catalog.users.oauth_id_exists(&args.oauth_id) {
            return Err(CatalogError::AlreadyExists);
        }

        records.push(&CreateLoginIdentityOAuth {
            user_id: args.user_id.get(),
            oauth_id: args.oauth_id.clone(),
            created_at: args.created_at,
        });
        Ok(Self {
            user_id: args.user_id,
        })
    }

    fn output(&self, catalog: &InnerCatalog) -> Self::Output {
        catalog
            .users
            .get_by_id(&self.user_id)
            .expect("user should exist after adding OAuth identity")
    }
}

// ---------------------------------------------------------------------------
// DeleteLoginIdentityOAuth
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy)]
pub(crate) struct DeleteLoginIdentityOAuthArgs {
    pub user_id: UserId,
}

pub(crate) struct DeleteLoginIdentityOAuthOp {
    user_id: UserId,
}

impl CatalogOp for DeleteLoginIdentityOAuthOp {
    type Input = DeleteLoginIdentityOAuthArgs;
    type Output = Arc<UserInfo>;

    fn prepare(
        args: &Self::Input,
        catalog: &InnerCatalog,
        records: &mut RecordBatch,
    ) -> Result<Self, CatalogError> {
        let user = catalog
            .users
            .get_by_id(&args.user_id)
            .ok_or_else(|| CatalogError::NotFound(format!("user {}", args.user_id)))?;

        if user.login_identities.oauth.is_none() {
            return Err(CatalogError::NotFound(format!(
                "OAuth identity for user {}",
                args.user_id
            )));
        }

        records.push(&DeleteLoginIdentityOAuth {
            user_id: args.user_id.get(),
        });

        Ok(Self {
            user_id: args.user_id,
        })
    }

    fn output(&self, catalog: &InnerCatalog) -> Self::Output {
        catalog
            .users
            .get_by_id(&self.user_id)
            .expect("user should exist after deleting OAuth identity")
    }
}

// ---------------------------------------------------------------------------
// CreateRefreshToken
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub(crate) struct CreateRefreshTokenArgs {
    pub user_id: UserId,
    pub token_hash: String,
    pub created_at: i64,
    pub expires_at: i64,
}

pub(crate) struct CreateRefreshTokenOp {
    user_id: UserId,
}

impl CatalogOp for CreateRefreshTokenOp {
    type Input = CreateRefreshTokenArgs;
    type Output = UserId;

    fn prepare(
        args: &Self::Input,
        catalog: &InnerCatalog,
        records: &mut RecordBatch,
    ) -> Result<Self, CatalogError> {
        let user = catalog
            .users
            .get_by_id(&args.user_id)
            .ok_or_else(|| CatalogError::NotFound(format!("user {}", args.user_id)))?;

        if user.is_deleted() {
            return Err(CatalogError::AlreadyDeleted(format!(
                "user {}",
                args.user_id
            )));
        }

        records.push(&CreateRefreshToken {
            user_id: args.user_id.get(),
            token_hash: args.token_hash.clone(),
            created_at: args.created_at,
            expires_at: args.expires_at,
        });

        Ok(Self {
            user_id: args.user_id,
        })
    }

    fn output(&self, _catalog: &InnerCatalog) -> Self::Output {
        self.user_id
    }
}

// ---------------------------------------------------------------------------
// RevokeRefreshToken
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub(crate) struct RevokeRefreshTokenArgs {
    pub token_hash: String,
    pub revoked_at: i64,
}

pub(crate) struct RevokeRefreshTokenOp {
    token_hash: String,
}

impl CatalogOp for RevokeRefreshTokenOp {
    type Input = RevokeRefreshTokenArgs;
    type Output = ();

    fn prepare(
        args: &Self::Input,
        catalog: &InnerCatalog,
        records: &mut RecordBatch,
    ) -> Result<Self, CatalogError> {
        let token = catalog
            .users
            .refresh_tokens()
            .get_by_hash(&args.token_hash)
            .ok_or_else(|| CatalogError::NotFound(format!("refresh token {}", args.token_hash)))?;

        records.push(&RevokeRefreshToken {
            user_id: token.user_id.get(),
            token_hash: args.token_hash.clone(),
            revoked_at: args.revoked_at,
        });

        Ok(Self {
            token_hash: args.token_hash.clone(),
        })
    }

    fn output(&self, _catalog: &InnerCatalog) -> Self::Output {}
}

// ---------------------------------------------------------------------------
// RevokeAllRefreshTokensForUser
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy)]
pub(crate) struct RevokeAllRefreshTokensForUserArgs {
    pub user_id: UserId,
    pub revoked_at: i64,
}

pub(crate) struct RevokeAllRefreshTokensForUserOp {
    user_id: UserId,
}

impl CatalogOp for RevokeAllRefreshTokensForUserOp {
    type Input = RevokeAllRefreshTokensForUserArgs;
    type Output = ();

    fn prepare(
        args: &Self::Input,
        catalog: &InnerCatalog,
        records: &mut RecordBatch,
    ) -> Result<Self, CatalogError> {
        let user = catalog
            .users
            .get_by_id(&args.user_id)
            .ok_or_else(|| CatalogError::NotFound(format!("user {}", args.user_id)))?;

        if user.is_deleted() {
            return Err(CatalogError::AlreadyDeleted(format!(
                "user {}",
                args.user_id
            )));
        }

        records.push(&RevokeAllRefreshTokensForUser {
            user_id: args.user_id.get(),
            revoked_at: args.revoked_at,
        });

        Ok(Self {
            user_id: args.user_id,
        })
    }

    fn output(&self, _catalog: &InnerCatalog) -> Self::Output {}
}

// ---------------------------------------------------------------------------
// UpdateUserRoles
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub(crate) struct UpdateUserRolesArgs {
    pub user_id: UserId,
    pub role_ids: Vec<RoleId>,
    pub updated_at: i64,
}

pub(crate) struct UpdateUserRolesOp {
    user_id: UserId,
}

impl CatalogOp for UpdateUserRolesOp {
    type Input = UpdateUserRolesArgs;
    type Output = Arc<UserInfo>;

    fn prepare(
        args: &Self::Input,
        catalog: &InnerCatalog,
        records: &mut RecordBatch,
    ) -> Result<Self, CatalogError> {
        let user = catalog
            .users
            .get_by_id(&args.user_id)
            .ok_or_else(|| CatalogError::NotFound(format!("user {}", args.user_id)))?;

        if user.is_deleted() {
            return Err(CatalogError::AlreadyDeleted(format!(
                "user {}",
                args.user_id
            )));
        }

        for role_id in &args.role_ids {
            if catalog.roles.get_by_id(role_id).is_none() {
                return Err(CatalogError::NotFound(format!("role {}", role_id)));
            }
        }

        records.push(&UpdateUserRoles {
            user_id: args.user_id.get(),
            role_ids: args.role_ids.iter().map(|id| id.get()).collect(),
            updated_at: args.updated_at,
        });

        Ok(Self {
            user_id: args.user_id,
        })
    }

    fn output(&self, catalog: &InnerCatalog) -> Self::Output {
        catalog
            .users
            .get_by_id(&self.user_id)
            .expect("user should exist after role update")
    }
}

#[cfg(test)]
mod tests;
