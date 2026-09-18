//! Token records (record_ids 18-20).

use std::sync::Arc;

use influxdb3_catalog_macros::catalog_record;

use crate::catalog::versions::v3::events::CatalogEvent;
use crate::catalog::versions::v3::inner::InnerCatalog;
use crate::format::apply::ApplyError;
use crate::format::{CatalogRecord, RecordApply, record_ids};
use influxdb3_authz::permissions::{ActionsBitmap, TokenPermissionResourceIdentifier};
use influxdb3_authz::{Actions, Permission, ResourceIdentifier, ResourceType, TokenInfo};
use influxdb3_id::TokenId;

/// Create an admin token.
#[catalog_record(id = record_ids::CREATE_ADMIN_TOKEN, shape = 0x2aa2ae96)]
pub struct CreateAdminToken {
    /// Token catalog ID.
    pub token_id: u64,
    /// Token name.
    pub name: String,
    /// Token hash.
    pub hash: Vec<u8>,
    /// Creation timestamp in milliseconds.
    pub created_at: i64,
    /// Optional update timestamp in milliseconds.
    pub updated_at: Option<i64>,
    /// Optional expiry timestamp in milliseconds.
    pub expiry: Option<i64>,
    /// Optional description.
    pub description: Option<String>,
    /// Optional ID of the token that created this one.
    pub created_by: Option<u64>,
    /// Optional ID of the token that last updated this one.
    pub updated_by: Option<u64>,
}

impl RecordApply for CreateAdminToken {
    fn apply(&self, catalog: &mut InnerCatalog) -> Result<(), ApplyError> {
        let token_id = TokenId::from(self.token_id);
        let mut token_info = TokenInfo::new(
            token_id,
            Arc::from(self.name.as_str()),
            self.hash.clone(),
            self.created_at,
            self.expiry,
        );
        token_info.updated_at = self.updated_at;
        token_info.description = self.description.clone();
        token_info.created_by = self.created_by.map(TokenId::from);
        token_info.updated_by = self.updated_by.map(TokenId::from);
        token_info.set_permissions(vec![Permission {
            resource_type: ResourceType::Wildcard,
            resource_identifier: ResourceIdentifier::Wildcard,
            actions: Actions::Wildcard,
            resource_names: None,
        }]);

        catalog
            .tokens
            .add_token(token_id, token_info)
            .map_err(|e| {
                ApplyError(format!(
                    "{}: add admin token (token_id={}, name='{}'): {e}",
                    Self::NAME,
                    self.token_id,
                    self.name,
                ))
            })?;

        catalog.token_permissions.add_permission(
            ResourceType::Wildcard,
            TokenPermissionResourceIdentifier::Wildcard,
            token_id,
            ActionsBitmap::MAX,
        );
        Ok(())
    }

    fn event(&self) -> CatalogEvent {
        CatalogEvent::TokenCreated {
            token_id: TokenId::from(self.token_id),
        }
    }
}

/// Regenerate an admin token (new hash).
#[catalog_record(id = record_ids::REGENERATE_ADMIN_TOKEN, shape = 0x6f9799a8)]
pub struct RegenerateAdminToken {
    /// Token catalog ID.
    pub token_id: u64,
    /// New token hash.
    pub hash: Vec<u8>,
    /// Optional update timestamp in milliseconds.
    pub updated_at: Option<i64>,
}

impl RecordApply for RegenerateAdminToken {
    fn apply(&self, catalog: &mut InnerCatalog) -> Result<(), ApplyError> {
        let token_id = TokenId::from(self.token_id);
        let updated_at = self.updated_at.unwrap_or(0);
        catalog
            .tokens
            .update_admin_token_hash(token_id, self.hash.clone(), updated_at)
            .map_err(|e| {
                ApplyError(format!(
                    "{}: regenerate admin token (token_id={}): {e}",
                    Self::NAME,
                    self.token_id,
                ))
            })
    }

    fn event(&self) -> CatalogEvent {
        CatalogEvent::TokenRegenerated {
            token_id: TokenId::from(self.token_id),
        }
    }
}

/// Delete a token.
#[catalog_record(id = record_ids::DELETE_TOKEN, shape = 0x12f9b977)]
#[derive(Copy)]
pub struct DeleteToken {
    /// Token catalog ID.
    pub token_id: u64,
}

impl RecordApply for DeleteToken {
    fn apply(&self, catalog: &mut InnerCatalog) -> Result<(), ApplyError> {
        let token_id = TokenId::from(self.token_id);
        if catalog.tokens.delete_token_by_id(&token_id).is_none() {
            return Err(ApplyError(format!(
                "{}: token not found (token_id={})",
                Self::NAME,
                self.token_id,
            )));
        }
        catalog.token_permissions.remove(&token_id);
        Ok(())
    }

    fn event(&self) -> CatalogEvent {
        CatalogEvent::TokenDeleted {
            token_id: TokenId::from(self.token_id),
        }
    }
}

#[cfg(test)]
mod tests;
