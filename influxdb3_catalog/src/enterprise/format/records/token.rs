//! Enterprise token records (record_ids e1).

use std::sync::Arc;

use indexmap::IndexMap;

use crate::catalog::versions::v3::events::CatalogEvent;
use crate::catalog::versions::v3::inner::InnerCatalog;
use crate::format::apply::ApplyError;
use crate::format::records::impl_bitcode_encoding;
use crate::format::records::types::{
    Actions as WireActions, Permission as WirePermission, ResourceIdentifier as WireResourceIdent,
    ResourceType as WireResourceType,
};
use crate::format::{CatalogRecord, RecordFlags, RecordId, RegisteredRecord, record_ids};
use influxdb3_authz::permissions::TokenPermissionResourceIdentifier;
use influxdb3_authz::{
    Actions, CrudActions, DatabaseActions, Permission, ResourceIdentifier, ResourceMetadata,
    ResourceType, SystemActions, SystemResourceIdentifier, TokenInfo,
};
use influxdb3_id::{DbId, TokenId};

/// Create a resource-scoped token (enterprise).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, bitcode::Encode, bitcode::Decode)]
pub struct CreateResourceScopedToken {
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
    /// Token permissions.
    pub permissions: Vec<WirePermission>,
}

impl CatalogRecord for CreateResourceScopedToken {
    const ID: RecordId = record_ids::CREATE_RESOURCE_SCOPED_TOKEN;
    const FLAGS: RecordFlags = RecordFlags::none();
    const NAME: &'static str = "CreateResourceScopedToken";

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

        let all_permissions: Vec<Permission> = self.permissions.iter().map(Into::into).collect();
        token_info.set_permissions(all_permissions.clone());
        catalog
            .tokens
            .add_token(token_id, token_info)
            .map_err(|e| {
                ApplyError(format!(
                    "{}: add resource-scoped token (token_id={}, name='{}'): {e}",
                    Self::NAME,
                    self.token_id,
                    self.name,
                ))
            })?;

        for perm in &all_permissions {
            let actions = perm.actions.to_bitmap();
            match &perm.resource_identifier {
                ResourceIdentifier::Database(db_ids) => {
                    for db_id in db_ids {
                        catalog.token_permissions.add_permission(
                            perm.resource_type,
                            TokenPermissionResourceIdentifier::Database(*db_id),
                            token_id,
                            actions,
                        );
                    }
                }
                ResourceIdentifier::System(system_ids) => {
                    for system_id in system_ids {
                        catalog.token_permissions.add_permission(
                            perm.resource_type,
                            TokenPermissionResourceIdentifier::System(*system_id),
                            token_id,
                            actions,
                        );
                    }
                }
                ResourceIdentifier::Wildcard => {
                    catalog.token_permissions.add_permission(
                        perm.resource_type,
                        TokenPermissionResourceIdentifier::Wildcard,
                        token_id,
                        actions,
                    );
                }
                ResourceIdentifier::Token(token_ids) => {
                    for tid in token_ids {
                        catalog.token_permissions.add_permission(
                            perm.resource_type,
                            TokenPermissionResourceIdentifier::Token(*tid),
                            token_id,
                            actions,
                        );
                    }
                }
            }
        }
        Ok(())
    }

    fn event(&self) -> CatalogEvent {
        CatalogEvent::TokenCreated {
            token_id: TokenId::from(self.token_id),
        }
    }
}

inventory::submit! {
    RegisteredRecord::new::<CreateResourceScopedToken>()
}

impl_bitcode_encoding!(CreateResourceScopedToken);

impl From<&WirePermission> for Permission {
    fn from(wp: &WirePermission) -> Self {
        let resource_type = match wp.resource_type {
            WireResourceType::Database => ResourceType::Database,
            WireResourceType::Token => ResourceType::Token,
            WireResourceType::System => ResourceType::System,
            WireResourceType::Wildcard => ResourceType::Wildcard,
        };
        let resource_identifier = match &wp.resource_identifier {
            WireResourceIdent::Database(ids) => {
                ResourceIdentifier::Database(ids.iter().map(|id| DbId::from(*id)).collect())
            }
            WireResourceIdent::Token(ids) => {
                ResourceIdentifier::Token(ids.iter().map(|id| TokenId::from(*id)).collect())
            }
            WireResourceIdent::System(ids) => ResourceIdentifier::System(
                ids.iter()
                    .map(|id| SystemResourceIdentifier::from(*id))
                    .collect(),
            ),
            WireResourceIdent::Wildcard => ResourceIdentifier::Wildcard,
        };
        let actions = match wp.actions {
            WireActions::Database(bits) => Actions::Database(DatabaseActions::from(bits)),
            WireActions::Token(bits) => Actions::Token(CrudActions::from(bits)),
            WireActions::System(bits) => Actions::System(SystemActions::from(bits)),
            WireActions::Wildcard => Actions::Wildcard,
        };
        let names_map: IndexMap<String, ResourceMetadata> = wp
            .resource_names
            .iter()
            .map(|e| {
                (
                    e.resource_id.clone(),
                    ResourceMetadata {
                        name: e.name.clone(),
                        deleted: e.deleted,
                    },
                )
            })
            .collect();
        let resource_names = (!names_map.is_empty()).then_some(names_map);
        Self {
            resource_type,
            resource_identifier,
            actions,
            resource_names,
        }
    }
}

#[cfg(test)]
mod tests;
