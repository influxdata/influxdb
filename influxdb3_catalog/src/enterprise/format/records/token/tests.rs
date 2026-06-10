use std::sync::Arc;

use influxdb3_authz::DatabaseActions;
use influxdb3_id::DbId;
use uuid::Uuid;

use crate::catalog::versions::v3::inner::InnerCatalog;
use crate::enterprise::format::records::token::CreateResourceScopedToken;
use crate::format::CatalogRecord;
use crate::format::records::CreateDatabase;
use crate::format::records::assert_roundtrip;
use crate::format::records::types::{
    Actions as WireActions, Permission as WirePermission, ResourceIdentifier as WireResourceIdent,
    ResourceNameEntry as WireResourceNameEntry, ResourceType as WireResourceType, RetentionPeriod,
};

/// Helper to create a test catalog.
fn test_catalog() -> InnerCatalog {
    InnerCatalog::new(Arc::from("test"), Uuid::nil())
}

/// Helper: create a database.
fn setup_database(catalog: &mut InnerCatalog, db_id: u32, name: &str) {
    CreateDatabase {
        database_id: db_id,
        database_name: name.to_string(),
        retention_period: RetentionPeriod::Indefinite,
    }
    .apply(catalog)
    .unwrap();
}

#[test]
fn create_resource_scoped_token_round_trip() {
    assert_roundtrip!(
        CreateResourceScopedToken {
            token_id: 42,
            name: "scoped_token".to_string(),
            hash: vec![10, 20, 30],
            created_at: 1000000000,
            updated_at: None,
            expiry: Some(2000000000),
            description: None,
            created_by: None,
            updated_by: None,
            permissions: vec![],
        },
        "062a0c73636f7065645f746f6b656e03000a141e0200ca9a3b0001020094357700000000"
    );
}

#[test]
fn record_ids() {
    assert_eq!(
        CreateResourceScopedToken::ID.raw(),
        crate::format::record_ids::CREATE_RESOURCE_SCOPED_TOKEN.raw()
    );
    assert!(!CreateResourceScopedToken::FLAGS.is_upgrade_safe());
}

// ---------------------------------------------------------------------------
// Apply tests
// ---------------------------------------------------------------------------

#[test]
fn apply_create_resource_scoped_token() {
    let mut catalog = test_catalog();
    setup_database(&mut catalog, 1, "mydb");

    assert_eq!(catalog.tokens.repo().len(), 0);

    CreateResourceScopedToken {
        token_id: 2,
        name: "db_reader".to_string(),
        hash: vec![10, 11, 12],
        created_at: 1000,
        updated_at: None,
        expiry: None,
        description: None,
        created_by: None,
        updated_by: None,
        permissions: vec![WirePermission {
            resource_type: WireResourceType::Database,
            resource_identifier: WireResourceIdent::Database(vec![1]),
            actions: WireActions::Database(DatabaseActions::READ),
            resource_names: vec![],
        }],
    }
    .apply(&mut catalog)
    .unwrap();

    assert_eq!(catalog.tokens.repo().len(), 1);
    let token = catalog
        .tokens
        .repo()
        .get_by_name("db_reader")
        .expect("token should exist");
    assert!(!token.permissions.is_empty());
}

#[test]
fn apply_carries_resource_names_as_is() {
    use influxdb3_authz::{ResourceIdentifier, ResourceType};

    let mut catalog = test_catalog();
    setup_database(&mut catalog, 1, "live_db");

    CreateResourceScopedToken {
        token_id: 2,
        name: "tok".to_string(),
        hash: vec![1],
        created_at: 1000,
        updated_at: None,
        expiry: None,
        description: None,
        created_by: None,
        updated_by: None,
        permissions: vec![WirePermission {
            resource_type: WireResourceType::Database,
            resource_identifier: WireResourceIdent::Database(vec![1, 9]),
            actions: WireActions::Database(DatabaseActions::READ),
            resource_names: vec![WireResourceNameEntry {
                resource_id: "9".to_string(),
                name: "gone_db".to_string(),
                deleted: true,
            }],
        }],
    }
    .apply(&mut catalog)
    .unwrap();

    let token = catalog
        .tokens
        .repo()
        .get_by_name("tok")
        .expect("token should exist");
    let names = token.permissions[0]
        .resource_names
        .as_ref()
        .expect("resource names present");
    let gone = names.get("9").expect("entry for the deleted db is present");
    assert_eq!(
        gone.name, "gone_db",
        "display name for deleted db is preserved"
    );
    assert!(gone.deleted, "db is shown as deleted in the resource map");
    assert!(
        names.get("1").is_none(),
        "active db is not in resource map as it was not added"
    );

    assert!(
        matches!(token.permissions[0].resource_type, ResourceType::Database),
        "permission is for database"
    );
    assert!(matches!(
        &token.permissions[0].resource_identifier,
        ResourceIdentifier::Database(ids) if ids == &[DbId::new(1), DbId::new(9)]
    ));
}
