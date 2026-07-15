//! Role records (record_ids 39-42).

use std::sync::Arc;

use crate::catalog::versions::v3::events::CatalogEvent;
use crate::catalog::versions::v3::inner::InnerCatalog;
use crate::format::apply::ApplyError;
use crate::format::records::impl_bitcode_encoding;
use crate::format::records::types::{
    RoleAdminTokenAction, RoleAdminTokenPermission, RoleDatabaseAction, RoleDatabasePermission,
    RoleDatabaseResource, RolePermissionGrant, RoleRoleAction, RoleRolePermission,
    RoleSystemAction, RoleSystemPermission, RoleSystemResource, RoleTokenAction,
    RoleTokenPermission, RoleUserAction, RoleUserPermission,
};
use crate::format::{CatalogRecord, RecordFlags, RecordId, RegisteredRecord, record_ids};
use influxdb3_authz::role::role_permissions::{
    AdminTokenPermission, DatabasePermission, RolePermission as RoleResourcePermission,
    SystemPermission, TokenPermission, UserPermission,
};
use influxdb3_authz::role::{
    AdminTokenAction, DatabaseAction, Permission, ResourceIdentifier, Role, RoleAction,
    RoleDescription, RoleName, SystemAction, SystemResource, TokenAction, UserAction,
};
use influxdb3_id::{DbId, RoleId};

/// Create a new role.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, bitcode::Encode, bitcode::Decode)]
pub struct CreateRole {
    /// Role ID.
    pub role_id: u64,
    /// Role name.
    pub name: String,
    /// Optional description.
    pub description: Option<String>,
    /// Permissions in the locally-owned wire encoding.
    pub permissions: Vec<RolePermissionGrant>,
    /// Whether this is a required role that cannot be unassigned.
    pub is_required_role: bool,
    /// Creation timestamp in milliseconds.
    pub created_at: i64,
}

impl CatalogRecord for CreateRole {
    const ID: RecordId = record_ids::CREATE_ROLE;
    const FLAGS: RecordFlags = RecordFlags::none();
    const NAME: &'static str = "CreateRole";

    fn apply(&self, catalog: &mut InnerCatalog) -> Result<(), ApplyError> {
        let role_id = RoleId::new(self.role_id);
        let name = RoleName::new(&self.name).map_err(|e| {
            ApplyError(format!(
                "{}: invalid role name '{}': {e}",
                Self::NAME,
                self.name
            ))
        })?;
        let description = self
            .description
            .as_ref()
            .map(|d| RoleDescription::new(d))
            .transpose()
            .map_err(|e| ApplyError(format!("{}: invalid role description: {e}", Self::NAME)))?;
        let permissions: Vec<Permission> = self.permissions.iter().map(Into::into).collect();

        let role = Role::new(
            role_id,
            name,
            description,
            permissions,
            self.is_required_role,
            self.created_at,
        );
        catalog.roles.add_role(role).map_err(|e| {
            ApplyError(format!(
                "{}: add role (role_id={}, name='{}'): {e}",
                Self::NAME,
                self.role_id,
                self.name
            ))
        })
    }

    fn event(&self) -> CatalogEvent {
        CatalogEvent::RoleCreated {
            role_id: RoleId::new(self.role_id),
        }
    }
}

inventory::submit! {
    RegisteredRecord::new::<CreateRole>()
}

/// Update the permissions for a role.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, bitcode::Encode, bitcode::Decode)]
pub struct UpdateRolePermissions {
    /// Role ID.
    pub role_id: u64,
    /// New permissions in the locally-owned wire encoding.
    pub permissions: Vec<RolePermissionGrant>,
    /// Update timestamp in milliseconds.
    pub updated_at: i64,
}

impl CatalogRecord for UpdateRolePermissions {
    const ID: RecordId = record_ids::UPDATE_ROLE_PERMISSIONS;
    const FLAGS: RecordFlags = RecordFlags::none();
    const NAME: &'static str = "UpdateRolePermissions";

    fn apply(&self, catalog: &mut InnerCatalog) -> Result<(), ApplyError> {
        let role_id = RoleId::new(self.role_id);
        let permissions: Vec<Permission> = self.permissions.iter().map(Into::into).collect();

        let mut role = catalog.roles.get_by_id(&role_id).ok_or_else(|| {
            ApplyError(format!("{}: role {} not found", Self::NAME, self.role_id))
        })?;
        let role_mut = Arc::make_mut(&mut role);
        role_mut.permissions = permissions;
        role_mut.updated_at = self.updated_at;
        catalog.roles.update_role((*role).clone()).map_err(|e| {
            ApplyError(format!(
                "{}: update role permissions (role_id={}): {e}",
                Self::NAME,
                self.role_id
            ))
        })
    }

    fn event(&self) -> CatalogEvent {
        CatalogEvent::RoleUpdated {
            role_id: RoleId::new(self.role_id),
        }
    }
}

inventory::submit! {
    RegisteredRecord::new::<UpdateRolePermissions>()
}

/// Update role metadata (name and/or description).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, bitcode::Encode, bitcode::Decode)]
pub struct UpdateRole {
    /// Role ID.
    pub role_id: u64,
    /// New name (if changing).
    pub name: Option<String>,
    /// New description (if changing).
    pub description: Option<String>,
    /// Update timestamp in milliseconds.
    pub updated_at: i64,
}

impl CatalogRecord for UpdateRole {
    const ID: RecordId = record_ids::UPDATE_ROLE;
    const FLAGS: RecordFlags = RecordFlags::none();
    const NAME: &'static str = "UpdateRole";

    fn apply(&self, catalog: &mut InnerCatalog) -> Result<(), ApplyError> {
        let role_id = RoleId::new(self.role_id);
        let mut role = catalog.roles.get_by_id(&role_id).ok_or_else(|| {
            ApplyError(format!("{}: role {} not found", Self::NAME, self.role_id))
        })?;
        let role_mut = Arc::make_mut(&mut role);

        if let Some(ref new_name) = self.name {
            role_mut.name = RoleName::new(new_name).map_err(|e| {
                ApplyError(format!(
                    "{}: invalid role name '{}': {e}",
                    Self::NAME,
                    new_name
                ))
            })?;
        }
        if let Some(ref new_desc) = self.description {
            role_mut.description = Some(RoleDescription::new(new_desc).map_err(|e| {
                ApplyError(format!("{}: invalid role description: {e}", Self::NAME))
            })?);
        }
        role_mut.updated_at = self.updated_at;

        catalog.roles.update_role((*role).clone()).map_err(|e| {
            ApplyError(format!(
                "{}: update role (role_id={}): {e}",
                Self::NAME,
                self.role_id
            ))
        })
    }

    fn event(&self) -> CatalogEvent {
        CatalogEvent::RoleUpdated {
            role_id: RoleId::new(self.role_id),
        }
    }
}

inventory::submit! {
    RegisteredRecord::new::<UpdateRole>()
}

/// Delete a role and remove it from all users.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, bitcode::Encode, bitcode::Decode)]
pub struct DeleteRole {
    /// Role ID.
    pub role_id: u64,
    /// User IDs that have this role (will be updated to remove it).
    pub affected_user_ids: Vec<u64>,
    /// Deletion timestamp in milliseconds.
    pub deleted_at: i64,
}

impl CatalogRecord for DeleteRole {
    const ID: RecordId = record_ids::DELETE_ROLE;
    const FLAGS: RecordFlags = RecordFlags::none();
    const NAME: &'static str = "DeleteRole";

    fn apply(&self, catalog: &mut InnerCatalog) -> Result<(), ApplyError> {
        let role_id = RoleId::new(self.role_id);

        // Remove role from all affected users, collecting the first error but continuing
        // to process remaining users for best-effort consistency.
        let mut first_error: Option<ApplyError> = None;
        for &uid in &self.affected_user_ids {
            let user_id = influxdb3_id::UserId::new(uid);
            if let Some(mut user) = catalog.users.get_by_id(&user_id) {
                let user_mut = Arc::make_mut(&mut user);
                user_mut.role_ids.retain(|&id| id != role_id);
                user_mut.updated_at = self.deleted_at;
                if let Err(e) = catalog.users.update_user((*user).clone()) {
                    first_error.get_or_insert(ApplyError(format!(
                        "{}: update user {}: {e}",
                        Self::NAME,
                        uid
                    )));
                }
            }
        }

        catalog.roles.remove_role(&role_id);

        if let Some(e) = first_error {
            return Err(e);
        }
        Ok(())
    }

    fn event(&self) -> CatalogEvent {
        CatalogEvent::RoleDeleted {
            role_id: RoleId::new(self.role_id),
        }
    }
}

inventory::submit! {
    RegisteredRecord::new::<DeleteRole>()
}

impl_bitcode_encoding!(CreateRole, UpdateRolePermissions, UpdateRole, DeleteRole,);

// ---------------------------------------------------------------------------
// Wire <-> authz conversions
// ---------------------------------------------------------------------------

impl From<&Permission> for RolePermissionGrant {
    fn from(p: &Permission) -> Self {
        match p {
            Permission::AccountAdminAll => RolePermissionGrant::AccountAdminAll,
            Permission::Database(db) => RolePermissionGrant::Database(RoleDatabasePermission {
                action: db.action().into(),
                resource: db.resource().into(),
            }),
            Permission::Token(t) => RolePermissionGrant::Token(RoleTokenPermission {
                action: t.action().into(),
            }),
            Permission::User(u) => RolePermissionGrant::User(RoleUserPermission {
                action: u.action().into(),
            }),
            Permission::Role(r) => RolePermissionGrant::Role(RoleRolePermission {
                action: r.action().into(),
            }),
            Permission::AdminToken(a) => {
                RolePermissionGrant::AdminToken(RoleAdminTokenPermission {
                    action: a.action().into(),
                })
            }
            Permission::System(s) => RolePermissionGrant::System(RoleSystemPermission {
                action: s.action().into(),
                resource: s.resource().into(),
            }),
        }
    }
}

impl From<&RolePermissionGrant> for Permission {
    fn from(g: &RolePermissionGrant) -> Self {
        match g {
            RolePermissionGrant::AccountAdminAll => Permission::AccountAdminAll,
            RolePermissionGrant::Database(db) => Permission::Database(DatabasePermission::new(
                db.action.into(),
                db.resource.into(),
            )),
            RolePermissionGrant::Token(t) => {
                Permission::Token(TokenPermission::new(t.action.into()))
            }
            RolePermissionGrant::User(u) => Permission::User(UserPermission::new(u.action.into())),
            RolePermissionGrant::Role(r) => {
                Permission::Role(RoleResourcePermission::new(r.action.into()))
            }
            RolePermissionGrant::AdminToken(a) => {
                Permission::AdminToken(AdminTokenPermission::new(a.action.into()))
            }
            RolePermissionGrant::System(s) => {
                Permission::System(SystemPermission::new(s.action.into(), s.resource.into()))
            }
        }
    }
}

impl From<DatabaseAction> for RoleDatabaseAction {
    fn from(a: DatabaseAction) -> Self {
        match a {
            DatabaseAction::Describe => RoleDatabaseAction::Describe,
            DatabaseAction::Read => RoleDatabaseAction::Read,
            DatabaseAction::Write => RoleDatabaseAction::Write,
            DatabaseAction::Create => RoleDatabaseAction::Create,
            DatabaseAction::Delete => RoleDatabaseAction::Delete,
            DatabaseAction::GrantUsage => RoleDatabaseAction::GrantUsage,
        }
    }
}

impl From<RoleDatabaseAction> for DatabaseAction {
    fn from(a: RoleDatabaseAction) -> Self {
        match a {
            RoleDatabaseAction::Describe => DatabaseAction::Describe,
            RoleDatabaseAction::Read => DatabaseAction::Read,
            RoleDatabaseAction::Write => DatabaseAction::Write,
            RoleDatabaseAction::Create => DatabaseAction::Create,
            RoleDatabaseAction::Delete => DatabaseAction::Delete,
            RoleDatabaseAction::GrantUsage => DatabaseAction::GrantUsage,
        }
    }
}

impl From<ResourceIdentifier<DbId>> for RoleDatabaseResource {
    fn from(r: ResourceIdentifier<DbId>) -> Self {
        match r {
            ResourceIdentifier::All => RoleDatabaseResource::All,
            ResourceIdentifier::Identifier(id) => RoleDatabaseResource::Identifier(id.get()),
        }
    }
}

impl From<RoleDatabaseResource> for ResourceIdentifier<DbId> {
    fn from(r: RoleDatabaseResource) -> Self {
        match r {
            RoleDatabaseResource::All => ResourceIdentifier::All,
            RoleDatabaseResource::Identifier(id) => ResourceIdentifier::Identifier(DbId::from(id)),
        }
    }
}

impl From<TokenAction> for RoleTokenAction {
    fn from(a: TokenAction) -> Self {
        match a {
            TokenAction::Read => RoleTokenAction::Read,
            TokenAction::Create => RoleTokenAction::Create,
            TokenAction::Delete => RoleTokenAction::Delete,
            TokenAction::GrantUsage => RoleTokenAction::GrantUsage,
        }
    }
}

impl From<RoleTokenAction> for TokenAction {
    fn from(a: RoleTokenAction) -> Self {
        match a {
            RoleTokenAction::Read => TokenAction::Read,
            RoleTokenAction::Create => TokenAction::Create,
            RoleTokenAction::Delete => TokenAction::Delete,
            RoleTokenAction::GrantUsage => TokenAction::GrantUsage,
        }
    }
}

impl From<UserAction> for RoleUserAction {
    fn from(a: UserAction) -> Self {
        match a {
            UserAction::Read => RoleUserAction::Read,
            UserAction::Create => RoleUserAction::Create,
            UserAction::Update => RoleUserAction::Update,
            UserAction::Delete => RoleUserAction::Delete,
            UserAction::GrantUsage => RoleUserAction::GrantUsage,
        }
    }
}

impl From<RoleUserAction> for UserAction {
    fn from(a: RoleUserAction) -> Self {
        match a {
            RoleUserAction::Read => UserAction::Read,
            RoleUserAction::Create => UserAction::Create,
            RoleUserAction::Update => UserAction::Update,
            RoleUserAction::Delete => UserAction::Delete,
            RoleUserAction::GrantUsage => UserAction::GrantUsage,
        }
    }
}

impl From<RoleAction> for RoleRoleAction {
    fn from(a: RoleAction) -> Self {
        match a {
            RoleAction::Read => RoleRoleAction::Read,
            RoleAction::Create => RoleRoleAction::Create,
            RoleAction::Update => RoleRoleAction::Update,
            RoleAction::Delete => RoleRoleAction::Delete,
        }
    }
}

impl From<RoleRoleAction> for RoleAction {
    fn from(a: RoleRoleAction) -> Self {
        match a {
            RoleRoleAction::Read => RoleAction::Read,
            RoleRoleAction::Create => RoleAction::Create,
            RoleRoleAction::Update => RoleAction::Update,
            RoleRoleAction::Delete => RoleAction::Delete,
        }
    }
}

impl From<AdminTokenAction> for RoleAdminTokenAction {
    fn from(a: AdminTokenAction) -> Self {
        match a {
            AdminTokenAction::Create => RoleAdminTokenAction::Create,
            AdminTokenAction::Delete => RoleAdminTokenAction::Delete,
        }
    }
}

impl From<RoleAdminTokenAction> for AdminTokenAction {
    fn from(a: RoleAdminTokenAction) -> Self {
        match a {
            RoleAdminTokenAction::Create => AdminTokenAction::Create,
            RoleAdminTokenAction::Delete => AdminTokenAction::Delete,
        }
    }
}

impl From<SystemAction> for RoleSystemAction {
    fn from(a: SystemAction) -> Self {
        match a {
            SystemAction::Read => RoleSystemAction::Read,
        }
    }
}

impl From<RoleSystemAction> for SystemAction {
    fn from(a: RoleSystemAction) -> Self {
        match a {
            RoleSystemAction::Read => SystemAction::Read,
        }
    }
}

impl From<ResourceIdentifier<SystemResource>> for RoleSystemResource {
    fn from(r: ResourceIdentifier<SystemResource>) -> Self {
        match r {
            ResourceIdentifier::All => RoleSystemResource::All,
            ResourceIdentifier::Identifier(SystemResource::Health) => RoleSystemResource::Health,
            ResourceIdentifier::Identifier(SystemResource::Metrics) => RoleSystemResource::Metrics,
            ResourceIdentifier::Identifier(SystemResource::Ping) => RoleSystemResource::Ping,
            ResourceIdentifier::Identifier(SystemResource::Ready) => RoleSystemResource::Ready,
        }
    }
}

impl From<RoleSystemResource> for ResourceIdentifier<SystemResource> {
    fn from(r: RoleSystemResource) -> Self {
        match r {
            RoleSystemResource::All => ResourceIdentifier::All,
            RoleSystemResource::Health => ResourceIdentifier::Identifier(SystemResource::Health),
            RoleSystemResource::Metrics => ResourceIdentifier::Identifier(SystemResource::Metrics),
            RoleSystemResource::Ping => ResourceIdentifier::Identifier(SystemResource::Ping),
            RoleSystemResource::Ready => ResourceIdentifier::Identifier(SystemResource::Ready),
        }
    }
}

#[cfg(test)]
mod tests;
