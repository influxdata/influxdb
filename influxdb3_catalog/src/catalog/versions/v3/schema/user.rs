use std::collections::HashMap;
use std::sync::Arc;

use ahash::RandomState as AHashBuilder;
use influxdb3_id::{RoleId, UserId};
use serde::{Deserialize, Serialize};

use crate::CatalogError;
use crate::Result;
use crate::catalog::Repository;
use crate::resource::CatalogResource;

type BiHashMap<L, R> = bimap::BiHashMap<L, R, AHashBuilder, AHashBuilder>;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct UserInfo {
    pub id: UserId,
    pub display_name: Option<String>,
    pub created_at: i64,
    pub updated_at: i64,
    pub deleted_at: Option<i64>,
    #[serde(default)]
    pub login_identities: LoginIdentities,
    #[serde(default)]
    pub role_ids: Vec<RoleId>,
}

impl UserInfo {
    pub fn new(id: UserId, display_name: Option<String>, created_at: i64) -> Self {
        Self {
            id,
            display_name,
            created_at,
            updated_at: created_at,
            deleted_at: None,
            login_identities: LoginIdentities::default(),
            role_ids: Vec::new(),
        }
    }

    pub fn is_deleted(&self) -> bool {
        self.deleted_at.is_some()
    }
}

impl CatalogResource for UserInfo {
    type Identifier = UserId;

    const CATEGORY: &'static str = "users";

    fn id(&self) -> Self::Identifier {
        self.id
    }

    fn name(&self) -> Arc<str> {
        Arc::from(self.id.to_string())
    }
}

/// Container for all login identity types a user may have.
/// Currently only supports username/password, but designed for future OAuth support.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct LoginIdentities {
    pub username_password: Option<LoginIdentityUsernamePassword>,
    #[serde(default)]
    pub oauth: Option<LoginIdentityOAuth>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LoginIdentityUsernamePassword {
    pub username: Arc<str>,
    pub password_hash: Arc<str>,
    pub requires_password_reset: bool,
    pub created_at: i64,
    pub updated_at: i64,
}

impl LoginIdentityUsernamePassword {
    pub fn new(
        username: Arc<str>,
        password_hash: Arc<str>,
        requires_password_reset: bool,
        created_at: i64,
    ) -> Self {
        Self {
            username,
            password_hash,
            requires_password_reset,
            created_at,
            updated_at: created_at,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LoginIdentityOAuth {
    pub oauth_id: Arc<str>,
    pub created_at: i64,
}

#[derive(Debug, Clone, Default)]
pub struct UserRepository {
    repo: Repository<UserId, UserInfo>,
    /// Bi-directional map from UserId to username for O(1) username lookups
    username_lookup: BiHashMap<UserId, Arc<str>>,
    oauth_lookup: HashMap<Arc<str>, UserId>,
    refresh_tokens: RefreshTokenRepository,
}

impl UserRepository {
    pub fn new(
        repo: Repository<UserId, UserInfo>,
        username_lookup: BiHashMap<UserId, Arc<str>>,
        oauth_map: HashMap<Arc<str>, UserId>,
    ) -> Self {
        Self {
            repo,
            username_lookup,
            oauth_lookup: oauth_map,
            refresh_tokens: RefreshTokenRepository::default(),
        }
    }

    pub fn repo(&self) -> &Repository<UserId, UserInfo> {
        &self.repo
    }

    pub fn repo_mut(&mut self) -> &mut Repository<UserId, UserInfo> {
        &mut self.repo
    }

    pub fn refresh_tokens(&self) -> &RefreshTokenRepository {
        &self.refresh_tokens
    }

    pub fn refresh_tokens_mut(&mut self) -> &mut RefreshTokenRepository {
        &mut self.refresh_tokens
    }

    pub fn get_and_increment_next_id(&mut self) -> UserId {
        self.repo.get_and_increment_next_id()
    }

    pub fn get_by_id(&self, id: &UserId) -> Option<Arc<UserInfo>> {
        self.repo.get_by_id(id)
    }

    pub fn add_user(&mut self, user: UserInfo) -> Result<()> {
        let id = user.id;
        // If the user has a username/password identity, add to lookup
        if let Some(ref identity) = user.login_identities.username_password {
            if self.username_exists(&identity.username) {
                return Err(CatalogError::AlreadyExists);
            }
            self.username_lookup
                .insert(id, Arc::clone(&identity.username));
        }
        if let Some(ref identity) = user.login_identities.oauth {
            if self.oauth_id_exists(&identity.oauth_id) {
                return Err(CatalogError::AlreadyExists);
            }
            self.oauth_lookup.insert(Arc::clone(&identity.oauth_id), id);
        }
        Ok(self.repo.insert(id, user)?)
    }

    pub fn update_user(&mut self, user: UserInfo) -> Result<()> {
        let id = user.id;
        Ok(self.repo.update(id, user)?)
    }

    pub fn restore_user(
        &mut self,
        user_id: UserId,
        display_name: Option<String>,
        restored_at: i64,
    ) -> Result<()> {
        let mut user = self
            .repo
            .get_by_id(&user_id)
            .ok_or_else(|| CatalogError::NotFound(format!("user {}", user_id)))?;

        let user_mut = Arc::make_mut(&mut user);
        user_mut.deleted_at = None;
        user_mut.display_name = display_name;
        user_mut.updated_at = restored_at;
        user_mut.role_ids = Vec::new();

        // Re-register username/password identity in lookup
        if let Some(ref identity) = user_mut.login_identities.username_password {
            // Check for conflicts - another user may have taken this username
            if let Some(&existing_user_id) = self.username_lookup.get_by_right(&identity.username)
                && existing_user_id != user_id
            {
                return Err(CatalogError::AlreadyExists);
            }
            self.username_lookup
                .insert(user_id, Arc::clone(&identity.username));
        }

        // Re-register OAuth identity in lookup
        if let Some(ref oauth) = user_mut.login_identities.oauth {
            // Check for conflicts - another user may have taken this OAuth ID
            if let Some(&existing_user_id) = self.oauth_lookup.get(&oauth.oauth_id)
                && existing_user_id != user_id
            {
                return Err(CatalogError::AlreadyExists);
            }
            self.oauth_lookup
                .insert(Arc::clone(&oauth.oauth_id), user_id);
        }

        self.update_user((*user).clone())
    }

    pub fn iter(&self) -> impl Iterator<Item = (&UserId, &Arc<UserInfo>)> {
        self.repo.iter()
    }

    pub fn active_users(&self) -> impl Iterator<Item = &Arc<UserInfo>> {
        self.repo.resource_iter().filter(|u| !u.is_deleted())
    }

    pub fn has_active_users(&self) -> bool {
        self.active_users().next().is_some()
    }

    pub fn active_user_count(&self) -> usize {
        self.active_users().count()
    }

    // Username lookup methods

    /// Get a user by their username (looks up through login identity)
    pub fn get_by_username(&self, username: &str) -> Option<Arc<UserInfo>> {
        let user_id = self.username_lookup.get_by_right(username)?;
        self.repo.get_by_id(user_id)
    }

    /// Check if a username already exists
    pub fn username_exists(&self, username: &str) -> bool {
        self.username_lookup.contains_right(username)
    }

    // Login identity management methods

    /// Add a username/password login identity to an existing user
    pub fn add_login_identity_username_password(
        &mut self,
        user_id: UserId,
        identity: LoginIdentityUsernamePassword,
    ) -> Result<()> {
        if self.username_exists(&identity.username) {
            return Err(CatalogError::AlreadyExists);
        }

        let mut user = self
            .repo
            .get_by_id(&user_id)
            .ok_or_else(|| CatalogError::NotFound(format!("user {}", user_id)))?;

        let username = Arc::clone(&identity.username);
        let user_mut = Arc::make_mut(&mut user);
        user_mut.login_identities.username_password = Some(identity);
        self.repo.update(user_id, (*user).clone())?;
        self.username_lookup.insert(user_id, username);
        Ok(())
    }

    /// Update the password hash for a user's login identity
    pub fn update_login_identity_password(
        &mut self,
        user_id: UserId,
        password_hash: Arc<str>,
        updated_at: i64,
    ) -> Result<()> {
        let mut user = self.repo.get_by_id(&user_id).ok_or_else(|| {
            CatalogError::NotFound(format!("login identity for user {}", user_id))
        })?;

        let user_mut = Arc::make_mut(&mut user);
        let identity = user_mut
            .login_identities
            .username_password
            .as_mut()
            .ok_or_else(|| {
                CatalogError::NotFound(format!("login identity for user {}", user_id))
            })?;
        identity.password_hash = password_hash;
        identity.updated_at = updated_at;
        Ok(self.repo.update(user_id, (*user).clone())?)
    }

    /// Update the requires_password_reset flag for a user's login identity
    pub fn update_login_identity_requires_password_reset(
        &mut self,
        user_id: UserId,
        requires_password_reset: bool,
        updated_at: i64,
    ) -> Result<()> {
        let mut user = self.repo.get_by_id(&user_id).ok_or_else(|| {
            CatalogError::NotFound(format!("login identity for user {}", user_id))
        })?;

        let user_mut = Arc::make_mut(&mut user);
        let identity = user_mut
            .login_identities
            .username_password
            .as_mut()
            .ok_or_else(|| {
                CatalogError::NotFound(format!("login identity for user {}", user_id))
            })?;
        identity.requires_password_reset = requires_password_reset;
        identity.updated_at = updated_at;
        Ok(self.repo.update(user_id, (*user).clone())?)
    }

    /// Delete the username/password login identity for a user
    pub fn delete_login_identity_username_password(&mut self, user_id: &UserId) -> Result<()> {
        let mut user = self
            .repo
            .get_by_id(user_id)
            .ok_or_else(|| CatalogError::NotFound(format!("user {}", user_id)))?;

        let user_mut = Arc::make_mut(&mut user);
        user_mut.login_identities.username_password = None;
        self.repo.update(*user_id, (*user).clone())?;
        self.username_lookup.remove_by_left(user_id);
        Ok(())
    }

    pub fn get_by_oauth_id(&self, oauth_id: &str) -> Option<Arc<UserInfo>> {
        let user_id = self.oauth_lookup.get(oauth_id)?;
        self.repo.get_by_id(user_id)
    }

    pub fn get_deleted_user_by_oauth_id(&self, oauth_id: &str) -> Option<Arc<UserInfo>> {
        self.repo
            .resource_iter()
            .find(|u| {
                u.is_deleted()
                    && u.login_identities
                        .oauth
                        .as_ref()
                        .is_some_and(|o| o.oauth_id.as_ref() == oauth_id)
            })
            .cloned()
    }

    pub fn oauth_id_exists(&self, oauth_id: &str) -> bool {
        self.oauth_lookup.contains_key(oauth_id)
    }

    pub fn add_login_identity_oauth(
        &mut self,
        user_id: UserId,
        identity: LoginIdentityOAuth,
    ) -> Result<()> {
        if self.oauth_id_exists(&identity.oauth_id) {
            return Err(CatalogError::AlreadyExists);
        }

        let mut user = self
            .repo
            .get_by_id(&user_id)
            .ok_or_else(|| CatalogError::NotFound(format!("user {}", user_id)))?;

        let oauth_id = Arc::clone(&identity.oauth_id);
        let user_mut = Arc::make_mut(&mut user);
        user_mut.login_identities.oauth = Some(identity);

        self.repo.update(user_id, (*user).clone())?;
        self.oauth_lookup.insert(oauth_id, user_id);
        Ok(())
    }

    pub fn delete_login_identity_oauth(&mut self, user_id: &UserId) -> Result<()> {
        let mut user = self
            .repo
            .get_by_id(user_id)
            .ok_or_else(|| CatalogError::NotFound(format!("user {}", user_id)))?;

        let user_mut = Arc::make_mut(&mut user);
        user_mut.login_identities.oauth = None;

        self.repo.update(*user_id, (*user).clone())?;
        self.oauth_lookup.retain(|_, uid| uid != user_id);
        Ok(())
    }

    /// This removes the user from the oauth_lookup map without clearing out
    /// the oauth field on the user record. That's so we can restore later to this
    pub fn remove_oauth_lookup(&mut self, user_id: &UserId) {
        self.oauth_lookup.retain(|_, uid| uid != user_id);
    }

    /// Remove the user from the username_lookup map without clearing the
    /// username/password identity on the user record. This allows the username
    /// to be reused after deletion while preserving the identity for potential restore.
    pub fn remove_username_lookup(&mut self, user_id: &UserId) {
        self.username_lookup.remove_by_left(user_id);
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RefreshTokenInfo {
    pub token_hash: Arc<str>,
    pub user_id: UserId,
    pub created_at: i64,
    pub expires_at: i64,
    pub revoked_at: Option<i64>,
}

#[derive(Debug, Clone, Default)]
pub struct RefreshTokenRepository {
    tokens: HashMap<Arc<str>, RefreshTokenInfo>,
    user_tokens: HashMap<UserId, Vec<Arc<str>>>,
}

impl RefreshTokenRepository {
    pub fn add_token(&mut self, token: RefreshTokenInfo) {
        let hash = Arc::clone(&token.token_hash);
        let user_id = token.user_id;
        self.tokens.insert(Arc::clone(&hash), token);
        self.user_tokens.entry(user_id).or_default().push(hash);
    }

    pub fn get_by_hash(&self, token_hash: &str) -> Option<&RefreshTokenInfo> {
        self.tokens.get(token_hash)
    }

    pub fn revoke(&mut self, token_hash: &str, revoked_at: i64) -> bool {
        if let Some(token) = self.tokens.get_mut(token_hash) {
            token.revoked_at = Some(revoked_at);
            true
        } else {
            false
        }
    }

    pub fn revoke_all_for_user(&mut self, user_id: &UserId, revoked_at: i64) {
        if let Some(hashes) = self.user_tokens.get(user_id) {
            for hash in hashes {
                if let Some(token) = self.tokens.get_mut(hash.as_ref()) {
                    token.revoked_at = Some(revoked_at);
                }
            }
        }
    }

    pub fn cleanup_expired(&mut self, now_ms: i64) {
        let expired_hashes: Vec<Arc<str>> = self
            .tokens
            .iter()
            .filter(|(_, t)| t.expires_at < now_ms || t.revoked_at.is_some())
            .map(|(h, _)| Arc::clone(h))
            .collect();

        for hash in &expired_hashes {
            if let Some(token) = self.tokens.remove(hash.as_ref())
                && let Some(user_hashes) = self.user_tokens.get_mut(&token.user_id)
            {
                user_hashes.retain(|h| h.as_ref() != hash.as_ref());
                if user_hashes.is_empty() {
                    self.user_tokens.remove(&token.user_id);
                }
            }
        }
    }
}
