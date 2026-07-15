use influxdb3_id::DbId;
use serde::Deserialize;
use serde::Serialize;

use crate::role::actions::{
    AdminTokenAction, DatabaseAction, ResourceIdentifier, RoleAction, SystemAction, SystemResource,
    TokenAction, UserAction,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Permission {
    AccountAdminAll,
    Database(DatabasePermission),
    Token(TokenPermission),
    User(UserPermission),
    Role(RolePermission),
    AdminToken(AdminTokenPermission),
    System(SystemPermission),
}

impl Permission {
    pub fn covers<P: PermissionType>(&self, required: &P) -> bool {
        P::covers(self, required)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct DatabasePermission {
    action: DatabaseAction,
    resource: ResourceIdentifier<DbId>,
}

impl DatabasePermission {
    pub fn new(action: DatabaseAction, resource: ResourceIdentifier<DbId>) -> Self {
        Self { action, resource }
    }

    pub fn action(&self) -> DatabaseAction {
        self.action
    }

    pub fn resource(&self) -> ResourceIdentifier<DbId> {
        self.resource
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct TokenPermission {
    action: TokenAction,
}

impl TokenPermission {
    pub fn new(action: TokenAction) -> Self {
        Self { action }
    }

    pub fn action(&self) -> TokenAction {
        self.action
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct UserPermission {
    action: UserAction,
}

impl UserPermission {
    pub fn new(action: UserAction) -> Self {
        Self { action }
    }

    pub fn action(&self) -> UserAction {
        self.action
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct RolePermission {
    action: RoleAction,
}

impl RolePermission {
    pub fn new(action: RoleAction) -> Self {
        Self { action }
    }

    pub fn action(&self) -> RoleAction {
        self.action
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdminTokenPermission {
    action: AdminTokenAction,
}

impl AdminTokenPermission {
    pub fn new(action: AdminTokenAction) -> Self {
        Self { action }
    }

    pub fn action(&self) -> AdminTokenAction {
        self.action
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct SystemPermission {
    action: SystemAction,
    resource: ResourceIdentifier<SystemResource>,
}

impl SystemPermission {
    pub fn new(action: SystemAction, resource: ResourceIdentifier<SystemResource>) -> Self {
        Self { action, resource }
    }

    pub fn action(&self) -> SystemAction {
        self.action
    }

    pub fn resource(&self) -> ResourceIdentifier<SystemResource> {
        self.resource
    }
}

pub trait PermissionType: Sized {
    fn covers(perm: &Permission, required: &Self) -> bool;
}

macro_rules! permission_covers {
    ($variant:ident, action) => {
        fn covers(permission_variant: &Permission, required: &Self) -> bool {
            match permission_variant {
                Permission::AccountAdminAll => true,
                Permission::$variant(permission) => permission.action == required.action,
                _ => false,
            }
        }
    };
    ($variant:ident, action, resource) => {
        fn covers(permission_variant: &Permission, required: &Self) -> bool {
            match permission_variant {
                Permission::AccountAdminAll => true,
                Permission::$variant(permission) => {
                    permission.action == required.action
                        && permission.resource.covers(&required.resource)
                }
                _ => false,
            }
        }
    };
}

impl PermissionType for DatabasePermission {
    permission_covers!(Database, action, resource);
}

impl PermissionType for TokenPermission {
    permission_covers!(Token, action);
}

impl PermissionType for UserPermission {
    permission_covers!(User, action);
}

impl PermissionType for RolePermission {
    permission_covers!(Role, action);
}

impl PermissionType for AdminTokenPermission {
    permission_covers!(AdminToken, action);
}

impl PermissionType for SystemPermission {
    permission_covers!(System, action, resource);
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct Permissions(Vec<Permission>);

impl Permissions {
    pub fn new(permissions: Vec<Permission>) -> Self {
        Self(permissions)
    }

    pub fn has_permission<Permission: PermissionType>(&self, required: &Permission) -> bool {
        self.0.iter().any(|permission| permission.covers(required))
    }

    pub fn as_slice(&self) -> &[Permission] {
        &self.0
    }
}

impl From<Vec<Permission>> for Permissions {
    fn from(permissions: Vec<Permission>) -> Self {
        Self::new(permissions)
    }
}
