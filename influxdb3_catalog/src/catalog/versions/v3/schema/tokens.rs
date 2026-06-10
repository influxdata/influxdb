use std::sync::Arc;

use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD as B64};
use influxdb3_authz::TokenInfo;
use influxdb3_id::TokenId;
use rand::{RngCore, rngs::OsRng};
use sha2::{Digest, Sha512};

use crate::{CatalogError, Repository, Result, catalog::BiHashMap};

/// Stores tokens in the catalog. Wraps a [`Repository`] while providing additional functionality
/// needed for looking up tokens at runtime.
#[derive(Debug, Clone, Default)]
pub(crate) struct TokenRepository {
    /// The collection of tokens
    repo: Repository<TokenId, TokenInfo>,
    /// Bi-directional map for quick lookup of tokens by their hash
    hash_lookup_map: BiHashMap<TokenId, Vec<u8>>,
}

impl TokenRepository {
    pub(crate) fn repo(&self) -> &Repository<TokenId, TokenInfo> {
        &self.repo
    }

    pub(crate) fn set_next_id(&mut self, id: TokenId) {
        self.repo.set_next_id(id);
    }

    pub(crate) fn hash_to_info(&self, hash: Vec<u8>) -> Option<Arc<TokenInfo>> {
        let id = self
            .hash_lookup_map
            .get_by_right(&hash)
            .map(|id| id.to_owned())?;
        self.repo.get_by_id(&id)
    }

    pub(crate) fn hash_to_id(&self, hash: Vec<u8>) -> Option<TokenId> {
        self.hash_lookup_map.get_by_right(&hash).copied()
    }

    pub(crate) fn add_token(&mut self, token_id: TokenId, token_info: TokenInfo) -> Result<()> {
        let hash = token_info.hash.clone();
        self.repo.insert(token_id, token_info)?;
        self.hash_lookup_map.insert(token_id, hash);
        Ok(())
    }

    pub(crate) fn update_admin_token_hash(
        &mut self,
        token_id: TokenId,
        hash: Vec<u8>,
        updated_at: i64,
    ) -> Result<()> {
        let mut token_info = self
            .repo
            .get_by_id(&token_id)
            .ok_or_else(|| CatalogError::MissingAdminTokenToUpdate)?;
        let updatable = Arc::make_mut(&mut token_info);

        updatable.hash = hash.clone();
        updatable.updated_at = Some(updated_at);
        updatable.updated_by = Some(token_id);
        self.repo.update(token_id, token_info)?;
        self.hash_lookup_map.insert(token_id, hash);
        Ok(())
    }

    pub(crate) fn delete_token(&mut self, token_name: &str) -> Result<TokenId> {
        let token_id = self
            .repo
            .name_to_id(token_name)
            .ok_or_else(|| CatalogError::NotFound(token_name.to_string()))?;
        self.repo.remove(&token_id);
        self.hash_lookup_map.remove_by_left(&token_id);
        Ok(token_id)
    }

    pub(crate) fn delete_token_by_id(&mut self, token_id: &TokenId) -> Option<Arc<TokenInfo>> {
        let token = self.repo.get_by_id(token_id)?;
        self.repo.remove(token_id);
        self.hash_lookup_map.remove_by_left(token_id);
        Some(token)
    }

    pub(crate) fn update_token(&mut self, token_id: TokenId, token_info: TokenInfo) -> Result<()> {
        self.repo.update(token_id, Arc::new(token_info))?;
        Ok(())
    }
}

/// Compute the SHA512 hash of a token string
/// This is the canonical way to hash tokens across the codebase
pub fn compute_token_hash(token: &str) -> Vec<u8> {
    Sha512::digest(token).to_vec()
}

pub fn create_token_and_hash() -> (String, Vec<u8>) {
    let token = {
        let mut token = String::from("apiv3_");
        let mut key = [0u8; 64];
        OsRng.fill_bytes(&mut key);
        token.push_str(&B64.encode(key));
        token
    };
    (token.clone(), compute_token_hash(&token))
}
