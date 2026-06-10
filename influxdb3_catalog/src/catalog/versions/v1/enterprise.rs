use super::*;
use indexmap::IndexMap;
use influxdb3_authz::{
    Actions, Permission, ResourceIdentifier, ResourceType, SystemResourceIdentifier, TokenInfo,
    permissions::{
        ActionsBitmap, PermissionDetailsSpec, ResourceIdToNameProvider, ResourceMappingError,
        ResourceNameToIdProvider, TokenPermissionResourceIdentifier,
    },
};
use influxdb3_id::TokenId;
use iox_time::TimeProvider;
use log::NodeModes;
use observability_deps::tracing::error;
use std::collections::btree_set;
use std::time::Duration;
use std::{str::FromStr, sync::Arc};

use crate::{
    CatalogError, Result,
    log::versions::v3::{
        CatalogBatch, TokenBatch, TokenCatalogOp,
        enterprise::{CreateTokenDetails, PermissionDetails},
    },
};

impl Catalog {
    pub async fn create_token_with_permission(
        &self,
        all_permissions: Vec<PermissionDetailsSpec>,
        token_name: String,
        expiry_secs: Option<u64>,
    ) -> Result<(Arc<TokenInfo>, String)> {
        let (token, hash) = create_token_and_hash();
        self.catalog_update_with_retry(|| {
            if self.inner.read().tokens.repo().contains_name(&token_name) {
                return Err(CatalogError::TokenNameAlreadyExists(token_name.clone()));
            }

            let (token_id, created_at, expiry) = {
                let mut inner = self.inner.write();
                let token_id = inner.tokens.get_and_increment_next_id();
                let created_at = self.time_provider.now();
                let expiry = expiry_secs.map(|secs| {
                    created_at
                        .checked_add(Duration::from_secs(secs))
                        .expect("duration not to overflow")
                        .timestamp_millis()
                });
                (token_id, created_at.timestamp_millis(), expiry)
            };

            // NB: the validation happens here for parsing but the parsed types aren't used in
            //     `CreateTokenDetails` currently. This will be addressed in
            //     https://github.com/influxdata/influxdb_pro/issues/745
            let all_perms = all_permissions.iter().try_fold(
                Vec::with_capacity(all_permissions.len()),
                |mut permission_details, api_permission| {
                    let resource_type = ResourceType::from_str(&api_permission.resource_type)
                        .map(|res_type| {
                            if matches!(res_type, ResourceType::Wildcard) {
                                Err(CatalogError::CannotParsePermissionForToken(
                                    "* resource type can only be set for admin token".to_string(),
                                ))
                            } else {
                                Ok(res_type)
                            }
                        })
                        .map_err(|err| {
                            CatalogError::CannotParsePermissionForToken(err.to_string())
                        })??;

                    let _ = ResourceIdentifier::build_resource_ids_for_type(
                        resource_type,
                        // todo: avoid this cloning
                        Arc::from(self.clone_inner()),
                        &api_permission.resource_identifier,
                    )
                    .map_err(|err| CatalogError::CannotParsePermissionForToken(err.to_string()))?;

                    let _ = Actions::build_actions_for_type(resource_type, &api_permission.actions)
                        .map_err(|err| {
                            CatalogError::CannotParsePermissionForToken(err.to_string())
                        })?;

                    // at this point everything for permission has been parsed
                    permission_details.push(PermissionDetails {
                        resource_type: api_permission.resource_type.clone(),
                        resource_identifier: api_permission.resource_identifier.clone(),
                        actions: api_permission.actions.clone(),
                    });

                    Ok::<Vec<PermissionDetails>, CatalogError>(permission_details)
                },
            )?;

            Ok(CatalogBatch::Token(TokenBatch {
                time_ns: created_at,
                ops: vec![TokenCatalogOp::CreateResourceScopedToken(
                    CreateTokenDetails {
                        token_id,
                        name: Arc::from(token_name.as_str()),
                        hash: hash.clone(),
                        created_at,
                        expiry,
                        permissions: all_perms,
                    },
                )],
            }))
        })
        .await?;

        let token_info = {
            self.inner
                .read()
                .tokens
                .repo()
                .get_by_name(&token_name)
                .expect("token info must be present after token creation by name")
        };

        // we need to pass these details back, especially this token as this is what user should
        // send in subsequent requests
        Ok((token_info, token))
    }
}

impl InnerCatalog {
    pub fn apply_token_batch_enterprise(&mut self, token_batch: &TokenBatch) -> Result<bool> {
        let mut is_updated = false;
        for op in &token_batch.ops {
            is_updated |= match op {
                TokenCatalogOp::CreateAdminToken(create_admin_token_details) => {
                    // add an entry into permissions
                    self.token_permissions.add_permission(
                        ResourceType::Wildcard,
                        TokenPermissionResourceIdentifier::Wildcard,
                        create_admin_token_details.token_id,
                        ActionsBitmap::MAX,
                    );
                    true
                }
                TokenCatalogOp::RegenerateAdminToken(_) => false,
                TokenCatalogOp::CreateDatabaseToken(create_database_token_details) => {
                    let db_token_details =
                        CreateTokenDetails::from(create_database_token_details.clone());
                    self.handle_token_creation(&db_token_details)?;
                    true
                }
                TokenCatalogOp::CreateResourceScopedToken(create_token_details) => {
                    self.handle_token_creation(create_token_details)?;
                    true
                }
                TokenCatalogOp::DeleteToken(delete_token_details) => {
                    let token_id = self
                        .tokens
                        .delete_token(delete_token_details.token_name.to_owned())?;
                    self.token_permissions.remove(&token_id);
                    true
                }
            };
        }
        Ok(is_updated)
    }

    fn handle_token_creation(
        &mut self,
        create_token_details: &CreateTokenDetails,
    ) -> Result<(), crate::CatalogError> {
        let mut token_info = TokenInfo::new(
            create_token_details.token_id,
            Arc::clone(&create_token_details.name),
            create_token_details.hash.clone(),
            create_token_details.created_at,
            create_token_details.expiry,
        );
        let mut all_permissions = Vec::new();
        // NB: the validation has already happened when coming to this point so it's safe
        //     ignore the errors here and use `expect`. This will be tidied up when addressing
        //     issue, https://github.com/influxdata/influxdb_pro/issues/745
        for permission in &create_token_details.permissions {
            let resource_type = ResourceType::from_str(&permission.resource_type)
                .expect("resource type should be parseable");
            let allowed_actions =
                Actions::build_actions_for_type(resource_type, &permission.actions)
                    .expect("resource actions to be parseable");

            let resource_identifier = {
                let name_to_id_provider =
                    Arc::from(self.clone()) as Arc<dyn ResourceNameToIdProvider>;
                ResourceIdentifier::build_resource_ids_for_type(
                    resource_type,
                    name_to_id_provider,
                    &permission.resource_identifier,
                )
                .expect("resource identifier to be parseable")
            };

            match resource_identifier {
                ResourceIdentifier::Database(ref db_ids) => {
                    for db_id in db_ids {
                        self.token_permissions.add_permission(
                            resource_type,
                            TokenPermissionResourceIdentifier::Database(*db_id),
                            create_token_details.token_id,
                            allowed_actions.to_bitmap(),
                        );
                    }
                }
                ResourceIdentifier::System(ref system_resource_identifiers) => {
                    for system_id in system_resource_identifiers {
                        self.token_permissions.add_permission(
                            resource_type,
                            TokenPermissionResourceIdentifier::System(*system_id),
                            create_token_details.token_id,
                            allowed_actions.to_bitmap(),
                        );
                    }
                }
                ResourceIdentifier::Wildcard => {
                    self.token_permissions.add_permission(
                        resource_type,
                        TokenPermissionResourceIdentifier::Wildcard,
                        create_token_details.token_id,
                        allowed_actions.to_bitmap(),
                    );
                }
                _ => {
                    error!(?resource_identifier, "cannot map resource identifier");
                    panic!("unexpected resource identifier mapping");
                }
            };

            // Capture resource names for Database resources
            // Note: v1 users will be migrated to v2, so this is only for backward compatibility
            let resource_names = match (&resource_type, &resource_identifier) {
                (ResourceType::Database, ResourceIdentifier::Database(db_ids)) => {
                    let names_map: IndexMap<_, _> = db_ids
                        .iter()
                        .filter_map(|db_id| {
                            self.databases.id_to_name(db_id).map(|name| {
                                (
                                    db_id.to_string(),
                                    influxdb3_authz::ResourceMetadata {
                                        name: name.to_string(),
                                        deleted: false,
                                    },
                                )
                            })
                        })
                        .collect();

                    (!names_map.is_empty()).then_some(names_map)
                }
                _ => None,
            };

            all_permissions.push(Permission {
                resource_type,
                resource_identifier,
                actions: allowed_actions,
                resource_names,
            });
        }
        token_info.set_permissions(all_permissions);
        self.tokens
            .add_token(create_token_details.token_id, token_info)?;
        Ok(())
    }
}

impl ResourceNameToIdProvider for InnerCatalog {
    fn resource_name_to_id(
        &self,
        resource_type: ResourceType,
        names: &[String],
    ) -> Result<influxdb3_authz::ResourceIdentifier, ResourceMappingError> {
        let mut contains_all_resource_indicator = false;
        for name in names {
            if name == "*" {
                contains_all_resource_indicator = true;
                break;
            }
        }

        if contains_all_resource_indicator && names.len() > 1 {
            return Err(ResourceMappingError::MixedWildcardAndRegularResourceName);
        }

        if contains_all_resource_indicator {
            Ok(ResourceIdentifier::Wildcard)
        } else {
            match resource_type {
                ResourceType::Database => {
                    let ids = names.iter().try_fold(
                        Vec::with_capacity(names.len()),
                        |mut ids, name| {
                            let id = self.databases.name_to_id(name).ok_or_else(|| {
                                ResourceMappingError::InvalidResourceName(name.to_string())
                            })?;
                            ids.push(id);
                            Ok(ids)
                        },
                    )?;
                    Ok(ResourceIdentifier::Database(ids))
                }
                ResourceType::System => {
                    let ids = names.iter().try_fold(
                        Vec::with_capacity(names.len()),
                        |mut ids, name| {
                            let id = match name.to_lowercase().trim() {
                                "health" => {
                                    SystemResourceIdentifier::from(SystemResourceIdentifier::HEALTH)
                                }
                                "ping" => {
                                    SystemResourceIdentifier::from(SystemResourceIdentifier::PING)
                                }
                                "metrics" => SystemResourceIdentifier::from(
                                    SystemResourceIdentifier::METRICS,
                                ),
                                "ready" => {
                                    SystemResourceIdentifier::from(SystemResourceIdentifier::READY)
                                }
                                _ => {
                                    return Err(ResourceMappingError::InvalidResourceName(
                                        name.to_string(),
                                    ));
                                }
                            };
                            ids.push(id);
                            Ok(ids)
                        },
                    )?;
                    Ok(ResourceIdentifier::System(ids))
                }
                ResourceType::Wildcard => Ok(ResourceIdentifier::Wildcard),
                ResourceType::Token => unimplemented!(),
            }
        }
    }
}

impl ResourceIdToNameProvider for InnerCatalog {
    fn resource_id_to_name(
        &self,
        identifier: &ResourceIdentifier,
    ) -> Vec<std::result::Result<String, ResourceMappingError>> {
        match identifier {
            ResourceIdentifier::Database(db_ids) => db_ids
                .iter()
                .map(|id| {
                    self.databases
                        .id_to_name(id)
                        .ok_or_else(|| ResourceMappingError::MissingResourceId(id.to_string()))
                        .map(|name| name.to_string())
                })
                .collect(),
            ResourceIdentifier::System(system_resource_identifiers) => system_resource_identifiers
                .iter()
                .map(|id| Ok(id.to_string()))
                .collect(),
            ResourceIdentifier::Wildcard => vec![Ok("*".to_string())],
            _ => unimplemented!(),
        }
    }
}

pub(crate) fn hydrate_token_permissions(
    snap: &RepositorySnapshot<TokenId, TokenInfoSnapshot>,
) -> TokenPermissions {
    // token_permissions is an in memory repr, this is used for quick lookups when checking
    // perms for a token, so it is not saved to catalog and instead we hydrate this field when
    // loading a snapshot.
    let mut token_perms = TokenPermissions::new();
    snap.repo.iter().for_each(|(id, token)| {
        token.permissions.iter().for_each(|permission| {
            let actions = Actions::from_snapshot(permission.actions.clone());
            match &permission.resource_identifier {
                ResourceIdentifierSnapshot::Database(db_ids) => {
                    for resource_id in db_ids {
                        token_perms.add_permission(
                            ResourceType::from_snapshot(permission.resource_type),
                            TokenPermissionResourceIdentifier::Database(*resource_id),
                            *id,
                            actions.to_bitmap(),
                        );
                    }
                }
                ResourceIdentifierSnapshot::Token(token_ids) => {
                    for resource_id in token_ids {
                        token_perms.add_permission(
                            ResourceType::from_snapshot(permission.resource_type),
                            TokenPermissionResourceIdentifier::Token(*resource_id),
                            *id,
                            actions.to_bitmap(),
                        );
                    }
                }
                ResourceIdentifierSnapshot::System(system_resource_identifiers) => {
                    for resource_id in system_resource_identifiers {
                        token_perms.add_permission(
                            ResourceType::from_snapshot(permission.resource_type),
                            TokenPermissionResourceIdentifier::System(*resource_id),
                            *id,
                            actions.to_bitmap(),
                        );
                    }
                }
                ResourceIdentifierSnapshot::Wildcard => {
                    token_perms.add_permission(
                        ResourceType::from_snapshot(permission.resource_type),
                        TokenPermissionResourceIdentifier::Wildcard,
                        *id,
                        actions.to_bitmap(),
                    );
                }
            };
        });
    });
    token_perms
}

impl NodeModes {
    pub fn is_compactor(&self) -> bool {
        self.0.contains(&NodeMode::Compact) || self.0.contains(&NodeMode::All)
    }

    pub fn is_ingester(&self) -> bool {
        self.0.contains(&NodeMode::Ingest) || self.0.contains(&NodeMode::All)
    }

    pub fn is_querier(&self) -> bool {
        self.0.contains(&NodeMode::Query)
            || self.0.contains(&NodeMode::All)
            || self.0.contains(&NodeMode::Process)
    }
    pub fn is_processor(&self) -> bool {
        self.0.contains(&NodeMode::Process) || self.0.contains(&NodeMode::All)
    }

    pub fn contains(&self, mode: &NodeMode) -> bool {
        self.0.contains(mode)
    }

    pub fn contains_only(&self, mode: &NodeMode) -> bool {
        self.0.len() == 1 && self.0.contains(mode)
    }

    pub fn into_iter(&self) -> btree_set::IntoIter<NodeMode> {
        self.0.clone().into_iter()
    }
}
