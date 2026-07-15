//! Tests for role records.

use std::sync::Arc;
use uuid::Uuid;

use crate::catalog::versions::v3::inner::InnerCatalog;
use crate::format::CatalogRecord;
use crate::format::records::assert_roundtrip;
use crate::format::records::types::{
    RoleDatabaseAction, RoleDatabasePermission, RoleDatabaseResource, RolePermissionGrant,
    RoleSystemAction, RoleSystemPermission, RoleSystemResource,
};

use super::*;

fn test_catalog() -> InnerCatalog {
    InnerCatalog::new(Arc::from("test"), Uuid::nil())
}

// ---------------------------------------------------------------------------
// Roundtrip tests
// ---------------------------------------------------------------------------

#[test]
fn create_role_roundtrip() {
    assert_roundtrip!(
        CreateRole {
            role_id: 1,
            name: "admin".to_string(),
            description: Some("Administrator role".to_string()),
            permissions: vec![RolePermissionGrant::AccountAdminAll],
            is_required_role: true,
            created_at: 1234567890,
        },
        "06010561646d696e011241646d696e6973747261746f7220726f6c6501000102d2029649"
    );
}

#[test]
fn create_role_no_description_roundtrip() {
    assert_roundtrip!(
        CreateRole {
            role_id: 2,
            name: "viewer".to_string(),
            description: None,
            permissions: vec![],
            is_required_role: false,
            created_at: 1234567890,
        },
        "06020676696577657200000002d2029649"
    );
}

#[test]
fn create_role_database_permission_roundtrip() {
    assert_roundtrip!(
        CreateRole {
            role_id: 3,
            name: "db-writer".to_string(),
            description: None,
            permissions: vec![RolePermissionGrant::Database(RoleDatabasePermission {
                action: RoleDatabaseAction::Write,
                resource: RoleDatabaseResource::Identifier(42),
            })],
            is_required_role: false,
            created_at: 1234567890,
        },
        "06030964622d7772697465720001010201042a0002d2029649"
    );
}

#[test]
fn create_role_system_permission_all_roundtrip() {
    assert_roundtrip!(
        CreateRole {
            role_id: 4,
            name: "monitor".to_string(),
            description: None,
            permissions: vec![RolePermissionGrant::System(RoleSystemPermission {
                action: RoleSystemAction::Read,
                resource: RoleSystemResource::All,
            })],
            is_required_role: false,
            created_at: 1234567890,
        },
        "0604076d6f6e69746f72000106000002d2029649"
    );
}

#[test]
fn create_role_system_permission_specific_roundtrip() {
    assert_roundtrip!(
        CreateRole {
            role_id: 5,
            name: "health-only".to_string(),
            description: None,
            permissions: vec![RolePermissionGrant::System(RoleSystemPermission {
                action: RoleSystemAction::Read,
                resource: RoleSystemResource::Health,
            })],
            is_required_role: false,
            created_at: 1234567890,
        },
        "06050b6865616c74682d6f6e6c79000106010002d2029649"
    );
}

#[test]
fn permission_system_authz_wire_roundtrip() {
    use influxdb3_authz::role::{
        Permission, ResourceIdentifier, SystemAction, SystemResource,
        role_permissions::SystemPermission,
    };

    let cases = [
        Permission::System(SystemPermission::new(
            SystemAction::Read,
            ResourceIdentifier::All,
        )),
        Permission::System(SystemPermission::new(
            SystemAction::Read,
            ResourceIdentifier::Identifier(SystemResource::Health),
        )),
        Permission::System(SystemPermission::new(
            SystemAction::Read,
            ResourceIdentifier::Identifier(SystemResource::Metrics),
        )),
        Permission::System(SystemPermission::new(
            SystemAction::Read,
            ResourceIdentifier::Identifier(SystemResource::Ping),
        )),
        Permission::System(SystemPermission::new(
            SystemAction::Read,
            ResourceIdentifier::Identifier(SystemResource::Ready),
        )),
    ];

    for original in cases {
        let wire: RolePermissionGrant = RolePermissionGrant::from(&original);
        let back = Permission::from(&wire);
        assert_eq!(original, back, "system permission wire roundtrip");
    }
}

#[test]
fn update_role_permissions_roundtrip() {
    assert_roundtrip!(
        UpdateRolePermissions {
            role_id: 1,
            permissions: vec![RolePermissionGrant::AccountAdminAll],
            updated_at: 1234567890,
        },
        "0601010002d2029649"
    );
}

#[test]
fn update_role_roundtrip() {
    assert_roundtrip!(
        UpdateRole {
            role_id: 1,
            name: Some("new_name".to_string()),
            description: Some("New description".to_string()),
            updated_at: 1234567890,
        },
        "060101086e65775f6e616d65010f4e6577206465736372697074696f6e02d2029649"
    );
}

#[test]
fn delete_role_roundtrip() {
    assert_roundtrip!(
        DeleteRole {
            role_id: 1,
            affected_user_ids: vec![1, 2, 3],
            deleted_at: 1234567890,
        },
        "06010306063902d2029649"
    );
}

// ---------------------------------------------------------------------------
// Apply tests
// ---------------------------------------------------------------------------

#[test]
fn apply_create_role() {
    let mut catalog = test_catalog();
    CreateRole {
        role_id: 1,
        name: "admin".to_string(),
        description: Some("Administrator role".to_string()),
        permissions: vec![],
        is_required_role: true,
        created_at: 1234567890,
    }
    .apply(&mut catalog)
    .unwrap();

    let role = catalog
        .roles
        .get_by_id(&influxdb3_id::RoleId::new(1))
        .expect("role should exist");
    assert_eq!(role.id, influxdb3_id::RoleId::new(1));
    assert_eq!(role.name.as_str(), "admin");
    assert_eq!(role.created_at, 1234567890);
}

#[test]
fn apply_role_lifecycle() {
    let mut catalog = test_catalog();

    // Create role
    CreateRole {
        role_id: 1,
        name: "editor".to_string(),
        description: Some("Editor role".to_string()),
        permissions: vec![],
        is_required_role: false,
        created_at: 1000,
    }
    .apply(&mut catalog)
    .unwrap();

    // Update role name
    UpdateRole {
        role_id: 1,
        name: Some("content-editor".to_string()),
        description: None,
        updated_at: 2000,
    }
    .apply(&mut catalog)
    .unwrap();

    let role = catalog
        .roles
        .get_by_id(&influxdb3_id::RoleId::new(1))
        .unwrap();
    assert_eq!(role.name.as_str(), "content-editor");

    // Delete role
    DeleteRole {
        role_id: 1,
        affected_user_ids: vec![],
        deleted_at: 3000,
    }
    .apply(&mut catalog)
    .unwrap();

    assert!(
        catalog
            .roles
            .get_by_id(&influxdb3_id::RoleId::new(1))
            .is_none()
    );
}
