use super::{Permission, Permissions, RoleDescription, RoleName};

pub const ACCOUNT_ADMIN_ROLE_NAME: &str = "Admin";

#[derive(Debug)]
pub struct DefaultRoleDefinition {
    pub name: RoleName,
    pub description: RoleDescription,
    pub permissions: Permissions,
    pub is_required_role: bool,
}

pub fn account_admin_role_definition() -> DefaultRoleDefinition {
    DefaultRoleDefinition {
        name: RoleName::new(ACCOUNT_ADMIN_ROLE_NAME).unwrap(),
        description: RoleDescription::new("Full access to all resources. Cannot be deleted.")
            .unwrap(),
        permissions: Permissions::new(vec![Permission::AccountAdminAll]),
        is_required_role: true,
    }
}
