//! Enterprise trait implementations for v3 InnerCatalog.

use influxdb3_authz::SystemResourceIdentifier;
use influxdb3_authz::permissions::{
    ResourceIdToNameProvider, ResourceMappingError, ResourceNameToIdProvider,
};
use influxdb3_authz::{ResourceIdentifier, ResourceType};

use super::InnerCatalog;

impl ResourceNameToIdProvider for InnerCatalog {
    fn resource_name_to_id(
        &self,
        resource_type: ResourceType,
        names: &[String],
    ) -> Result<ResourceIdentifier, ResourceMappingError> {
        let contains_wildcard = names.iter().any(|n| n == "*");
        let contains_regular = names.iter().any(|n| n != "*");

        if contains_wildcard && contains_regular {
            return Err(ResourceMappingError::MixedWildcardAndRegularResourceName);
        }

        if contains_wildcard {
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
                                SystemResourceIdentifier::HEALTH_NAME => {
                                    SystemResourceIdentifier::from(SystemResourceIdentifier::HEALTH)
                                }
                                SystemResourceIdentifier::PING_NAME => {
                                    SystemResourceIdentifier::from(SystemResourceIdentifier::PING)
                                }
                                SystemResourceIdentifier::METRICS_NAME => {
                                    SystemResourceIdentifier::from(
                                        SystemResourceIdentifier::METRICS,
                                    )
                                }
                                SystemResourceIdentifier::READY_NAME => {
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
                // Token resource permissions are not yet supported as a
                // permission scope — they can only be referenced as a
                // resource type, not as individually-identified resources.
                ResourceType::Token => Err(ResourceMappingError::ResourceTypeNotSupported(
                    "token".to_string(),
                )),
            }
        }
    }
}

impl ResourceIdToNameProvider for InnerCatalog {
    fn resource_id_to_name(
        &self,
        identifier: &ResourceIdentifier,
    ) -> Vec<Result<String, ResourceMappingError>> {
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
            ResourceIdentifier::Token(_) => vec![Err(
                ResourceMappingError::ResourceTypeNotSupported("token".to_string()),
            )],
        }
    }
}
