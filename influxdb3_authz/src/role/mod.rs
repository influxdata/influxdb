pub mod actions;
#[allow(clippy::module_inception)]
pub mod role;
pub mod role_defaults;
pub mod role_permissions;

pub use actions::{
    AdminTokenAction, DatabaseAction, ResourceIdentifier, RoleAction, SystemAction, SystemResource,
    TokenAction, UserAction,
};
pub use role::{NewRoleDescriptionError, NewRoleNameError, Role, RoleDescription, RoleName};
pub use role_permissions::{Permission, Permissions};

#[cfg(test)]
mod tests;
