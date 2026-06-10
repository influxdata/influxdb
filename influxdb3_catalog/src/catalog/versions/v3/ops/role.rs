//! Role operations: Create, UpdatePermissions, Update, Delete.

use std::sync::Arc;

use super::CatalogOp;
use crate::CatalogError;
use crate::catalog::versions::v3::inner::InnerCatalog;
use crate::format::RecordBatch;
use crate::format::records::role::{CreateRole, DeleteRole, UpdateRole, UpdateRolePermissions};
use influxdb3_authz::role::{Permission, Role, RoleDescription, RoleName};
use influxdb3_id::{RoleId, UserId};

// ---------------------------------------------------------------------------
// CreateRole
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub(crate) struct CreateRoleArgs {
    pub name: String,
    pub description: Option<String>,
    pub permissions: Vec<Permission>,
    pub is_required_role: bool,
    pub created_at: i64,
}

pub(crate) struct CreateRoleOp {
    role_id: RoleId,
}

impl CatalogOp for CreateRoleOp {
    type Input = CreateRoleArgs;
    type Output = Arc<Role>;

    fn prepare(
        args: &Self::Input,
        catalog: &InnerCatalog,
        records: &mut RecordBatch,
    ) -> Result<Self, CatalogError> {
        RoleName::new(&args.name).map_err(|e| {
            CatalogError::invalid_configuration(format!("invalid role name '{}': {}", args.name, e))
        })?;

        if let Some(ref desc) = args.description {
            RoleDescription::new(desc).map_err(|e| {
                CatalogError::invalid_configuration(format!("invalid role description: {}", e))
            })?;
        }

        if catalog.roles.get_by_name(&args.name).is_some() {
            return Err(CatalogError::AlreadyExists);
        }

        let role_id = catalog.roles.repo().next_id();
        let permissions = args.permissions.iter().map(Into::into).collect();

        records.push(&CreateRole {
            role_id: role_id.get(),
            name: args.name.clone(),
            description: args.description.clone(),
            permissions,
            is_required_role: args.is_required_role,
            created_at: args.created_at,
        });
        Ok(Self { role_id })
    }

    fn output(&self, catalog: &InnerCatalog) -> Self::Output {
        catalog
            .roles
            .get_by_id(&self.role_id)
            .expect("role should exist after apply")
    }
}

// ---------------------------------------------------------------------------
// UpdateRolePermissions
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub(crate) struct UpdateRolePermissionsArgs {
    pub role_id: RoleId,
    pub permissions: Vec<Permission>,
    pub updated_at: i64,
}

pub(crate) struct UpdateRolePermissionsOp {
    role_id: RoleId,
}

impl CatalogOp for UpdateRolePermissionsOp {
    type Input = UpdateRolePermissionsArgs;
    type Output = Arc<Role>;

    fn prepare(
        args: &Self::Input,
        catalog: &InnerCatalog,
        records: &mut RecordBatch,
    ) -> Result<Self, CatalogError> {
        if catalog.roles.get_by_id(&args.role_id).is_none() {
            return Err(CatalogError::NotFound(format!("role {}", args.role_id)));
        }

        let permissions = args.permissions.iter().map(Into::into).collect();

        records.push(&UpdateRolePermissions {
            role_id: args.role_id.get(),
            permissions,
            updated_at: args.updated_at,
        });
        Ok(Self {
            role_id: args.role_id,
        })
    }

    fn output(&self, catalog: &InnerCatalog) -> Self::Output {
        catalog
            .roles
            .get_by_id(&self.role_id)
            .expect("role should exist after permissions update")
    }
}

// ---------------------------------------------------------------------------
// UpdateRole
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub(crate) struct UpdateRoleArgs {
    pub role_id: RoleId,
    pub name: Option<String>,
    pub description: Option<String>,
    pub updated_at: i64,
}

pub(crate) struct UpdateRoleOp {
    role_id: RoleId,
}

impl CatalogOp for UpdateRoleOp {
    type Input = UpdateRoleArgs;
    type Output = Arc<Role>;

    fn prepare(
        args: &Self::Input,
        catalog: &InnerCatalog,
        records: &mut RecordBatch,
    ) -> Result<Self, CatalogError> {
        if catalog.roles.get_by_id(&args.role_id).is_none() {
            return Err(CatalogError::NotFound(format!("role {}", args.role_id)));
        }

        if let Some(ref name) = args.name {
            RoleName::new(name).map_err(|e| {
                CatalogError::invalid_configuration(format!("invalid role name '{}': {}", name, e))
            })?;
            if let Some(existing) = catalog.roles.get_by_name(name)
                && existing.id != args.role_id
            {
                return Err(CatalogError::AlreadyExists);
            }
        }

        if let Some(ref desc) = args.description {
            RoleDescription::new(desc).map_err(|e| {
                CatalogError::invalid_configuration(format!("invalid role description: {}", e))
            })?;
        }

        records.push(&UpdateRole {
            role_id: args.role_id.get(),
            name: args.name.clone(),
            description: args.description.clone(),
            updated_at: args.updated_at,
        });

        Ok(Self {
            role_id: args.role_id,
        })
    }

    fn output(&self, catalog: &InnerCatalog) -> Self::Output {
        catalog
            .roles
            .get_by_id(&self.role_id)
            .expect("role should exist after update")
    }
}

// ---------------------------------------------------------------------------
// DeleteRole
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy)]
pub(crate) struct DeleteRoleArgs {
    pub role_id: RoleId,
    pub deleted_at: i64,
}

pub(crate) struct DeleteRoleOp {
    /// Captured before deletion — the role is removed from the catalog after apply.
    role: Arc<Role>,
}

impl CatalogOp for DeleteRoleOp {
    type Input = DeleteRoleArgs;
    type Output = Arc<Role>;

    fn prepare(
        args: &Self::Input,
        catalog: &InnerCatalog,
        records: &mut RecordBatch,
    ) -> Result<Self, CatalogError> {
        let role = catalog
            .roles
            .get_by_id(&args.role_id)
            .ok_or_else(|| CatalogError::NotFound(format!("role {}", args.role_id)))?;

        if role.is_required_role {
            return Err(CatalogError::invalid_configuration(format!(
                "cannot delete required role '{}'",
                role.name.as_str()
            )));
        }

        let affected_user_ids: Vec<UserId> = catalog
            .users
            .iter()
            .filter_map(|(_, user)| {
                if user.role_ids.contains(&args.role_id) {
                    Some(user.id)
                } else {
                    None
                }
            })
            .collect();

        records.push(&DeleteRole {
            role_id: args.role_id.get(),
            affected_user_ids: affected_user_ids.iter().map(|id| id.get()).collect(),
            deleted_at: args.deleted_at,
        });

        Ok(Self { role })
    }

    fn output(&self, _catalog: &InnerCatalog) -> Self::Output {
        Arc::clone(&self.role)
    }
}

#[cfg(test)]
mod tests;
