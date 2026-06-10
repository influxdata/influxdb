use super::actions::*;
use super::role_defaults;
use super::role_permissions::*;
use influxdb3_id::DbId;

// ResourceIdentifier tests

#[test]
fn resource_identifier_all_covers_everything() {
    let id: ResourceIdentifier<DbId> = ResourceIdentifier::All;
    assert!(id.covers(&ResourceIdentifier::All));
    assert!(id.covers(&ResourceIdentifier::Identifier(DbId::new(1))));
}

#[test]
fn resource_identifier_covers_exact_match() {
    let id: ResourceIdentifier<DbId> = ResourceIdentifier::Identifier(DbId::new(1));
    assert!(id.covers(&ResourceIdentifier::Identifier(DbId::new(1))));
}

#[test]
fn resource_identifier_does_not_cover_different_id() {
    let id: ResourceIdentifier<DbId> = ResourceIdentifier::Identifier(DbId::new(1));
    assert!(!id.covers(&ResourceIdentifier::Identifier(DbId::new(2))));
    assert!(!id.covers(&ResourceIdentifier::All));
}

// Permission covers tests (using PermissionType trait via Permission::covers)

#[test]
fn database_permission_covers_matching_action_and_resource() {
    let perm = Permission::Database(DatabasePermission::new(
        DatabaseAction::Read,
        ResourceIdentifier::Identifier(DbId::new(1)),
    ));
    let required = DatabasePermission::new(
        DatabaseAction::Read,
        ResourceIdentifier::Identifier(DbId::new(1)),
    );
    assert!(perm.covers(&required));
}

#[test]
fn database_permission_all_resource_covers_specific_resource() {
    let perm = Permission::Database(DatabasePermission::new(
        DatabaseAction::Read,
        ResourceIdentifier::All,
    ));
    let required = DatabasePermission::new(
        DatabaseAction::Read,
        ResourceIdentifier::Identifier(DbId::new(42)),
    );
    assert!(perm.covers(&required));
}

#[test]
fn database_permission_specific_does_not_cover_different_id() {
    let perm = Permission::Database(DatabasePermission::new(
        DatabaseAction::Read,
        ResourceIdentifier::Identifier(DbId::new(1)),
    ));
    let required = DatabasePermission::new(
        DatabaseAction::Read,
        ResourceIdentifier::Identifier(DbId::new(2)),
    );
    assert!(!perm.covers(&required));
}

#[test]
fn database_permission_does_not_cover_different_action() {
    let perm = Permission::Database(DatabasePermission::new(
        DatabaseAction::Read,
        ResourceIdentifier::All,
    ));
    let required = DatabasePermission::new(DatabaseAction::Write, ResourceIdentifier::All);
    assert!(!perm.covers(&required));
}

#[test]
fn user_permission_covers_matching_action() {
    let perm = Permission::User(UserPermission::new(UserAction::Read));
    let required = UserPermission::new(UserAction::Read);
    assert!(perm.covers(&required));
}

#[test]
fn user_permission_does_not_cover_different_action() {
    let perm = Permission::User(UserPermission::new(UserAction::Read));
    let required = UserPermission::new(UserAction::Delete);
    assert!(!perm.covers(&required));
}

#[test]
fn different_resource_types_never_cover() {
    let perm = Permission::User(UserPermission::new(UserAction::Read));
    let required = RolePermission::new(RoleAction::Read);
    assert!(!perm.covers(&required));
}

#[test]
fn has_permission_checks_any_matching() {
    let permissions = Permissions::new(vec![
        Permission::User(UserPermission::new(UserAction::Read)),
        Permission::Role(RolePermission::new(RoleAction::Read)),
    ]);

    assert!(permissions.has_permission(&RolePermission::new(RoleAction::Read)));

    assert!(!permissions.has_permission(&RolePermission::new(RoleAction::Create)));
}

#[test]
fn account_admin_all_covers_database_permission() {
    let perm = Permission::AccountAdminAll;
    let required = DatabasePermission::new(DatabaseAction::Delete, ResourceIdentifier::All);
    assert!(perm.covers(&required));
}

#[test]
fn account_admin_all_covers_user_permission() {
    let perm = Permission::AccountAdminAll;
    let required = UserPermission::new(UserAction::Create);
    assert!(perm.covers(&required));
}

#[test]
fn account_admin_all_covers_role_permission() {
    let perm = Permission::AccountAdminAll;
    let required = RolePermission::new(RoleAction::Delete);
    assert!(perm.covers(&required));
}

// RoleName tests
use super::role::RoleName;

#[test]
fn role_name_valid() {
    assert!(RoleName::new("Account Admin").is_ok());
    assert!(RoleName::new("my-role_123").is_ok());
    assert!(RoleName::new("A").is_ok());
}

#[test]
fn role_name_empty_rejected() {
    assert!(RoleName::new("").is_err());
}

#[test]
fn role_name_too_long_rejected() {
    let long_name = "a".repeat(65);
    assert!(RoleName::new(&long_name).is_err());
}

#[test]
fn role_name_invalid_chars_rejected() {
    assert!(RoleName::new("role@admin").is_err());
    assert!(RoleName::new("role!name").is_err());
}

#[test]
fn role_name_max_length_accepted() {
    let name = "a".repeat(64);
    assert!(RoleName::new(&name).is_ok());
}

// Default roles tests

#[test]
fn account_admin_has_wildcard_permission() {
    let perms = role_defaults::account_admin_role_definition().permissions;
    assert_eq!(perms.as_slice()[0], Permission::AccountAdminAll);
}
