use std::sync::Arc;

use uuid::Uuid;

use crate::catalog::versions::v3::inner::InnerCatalog;
use crate::format::CatalogRecord;
use crate::format::records::assert_roundtrip;
use crate::format::records::database::{CreateDatabase, HardDeleteDatabase, SoftDeleteDatabase};
use crate::format::records::types::{DeletionScope, RetentionPeriod};

/// Helper to create a test catalog.
fn test_catalog() -> InnerCatalog {
    InnerCatalog::new(Arc::from("test"), Uuid::nil())
}

#[test]
fn create_database_round_trip() {
    assert_roundtrip!(
        CreateDatabase {
            database_id: 1,
            database_name: "mydb".to_string(),
            retention_period: RetentionPeriod::Duration {
                duration_secs: 3600,
            },
        },
        "0401046d7964620104100e"
    );
}

#[test]
fn create_database_no_retention() {
    assert_roundtrip!(
        CreateDatabase {
            database_id: 42,
            database_name: "test_db".to_string(),
            retention_period: RetentionPeriod::Indefinite,
        },
        "042a07746573745f646200"
    );
}

#[test]
fn soft_delete_database_round_trip() {
    assert_roundtrip!(
        SoftDeleteDatabase {
            database_id: 1,
            deletion_time_ns: 1234567890,
            hard_deletion_time_ns: Some(9876543210),
            hard_delete_scope: Some(DeletionScope::DataAndCatalog),
        },
        "040102d20296490100ea16b04c020000000100"
    );
}

#[test]
fn hard_delete_database_round_trip() {
    assert_roundtrip!(HardDeleteDatabase { db_id: 42 }, "042a");
}

#[test]
fn record_ids() {
    assert_eq!(CreateDatabase::ID.raw(), 4);
    assert!(!CreateDatabase::FLAGS.is_upgrade_safe());

    assert_eq!(SoftDeleteDatabase::ID.raw(), 5);
    assert!(!SoftDeleteDatabase::FLAGS.is_upgrade_safe());

    assert_eq!(HardDeleteDatabase::ID.raw(), 22);
    assert!(!HardDeleteDatabase::FLAGS.is_upgrade_safe());
}

// ---------------------------------------------------------------------------
// Apply tests
// ---------------------------------------------------------------------------

#[test]
fn apply_create_database() {
    let mut catalog = test_catalog();
    CreateDatabase {
        database_id: 1,
        database_name: "mydb".to_string(),
        retention_period: RetentionPeriod::Indefinite,
    }
    .apply(&mut catalog)
    .unwrap();

    let db = catalog
        .databases
        .get_by_name("mydb")
        .expect("db should exist");
    assert_eq!(db.id, influxdb3_id::DbId::new(1));
    assert_eq!(db.name().as_ref(), "mydb");
}

#[test]
fn apply_create_database_with_retention() {
    let mut catalog = test_catalog();
    CreateDatabase {
        database_id: 1,
        database_name: "mydb".to_string(),
        retention_period: RetentionPeriod::Duration {
            duration_secs: 86400,
        },
    }
    .apply(&mut catalog)
    .unwrap();

    let db = catalog
        .databases
        .get_by_id(&influxdb3_id::DbId::new(1))
        .unwrap();
    assert!(matches!(
        db.retention_period,
        crate::catalog::versions::v3::schema::retention::RetentionPeriod::Duration(d)
        if d == std::time::Duration::from_secs(86400)
    ));
}

#[test]
fn apply_soft_delete_database() {
    let mut catalog = test_catalog();
    CreateDatabase {
        database_id: 1,
        database_name: "mydb".to_string(),
        retention_period: RetentionPeriod::Indefinite,
    }
    .apply(&mut catalog)
    .unwrap();

    SoftDeleteDatabase {
        database_id: 1,
        deletion_time_ns: 1_000_000_000_000_000_000, // 2001-09-09T01:46:40Z
        hard_deletion_time_ns: Some(2_000_000_000_000_000_000),
        hard_delete_scope: Some(DeletionScope::DataAndCatalog),
    }
    .apply(&mut catalog)
    .unwrap();

    let db = catalog
        .databases
        .get_by_id(&influxdb3_id::DbId::new(1))
        .unwrap();
    assert!(db.deleted);
    assert!(db.hard_delete_time.is_some());
    // Name should be renamed to include deletion timestamp
    assert!(db.name().contains("mydb-"));
    // Original name should no longer resolve
    assert!(catalog.databases.get_by_name("mydb").is_none());
}

#[test]
fn apply_hard_delete_database() {
    let mut catalog = test_catalog();
    CreateDatabase {
        database_id: 1,
        database_name: "mydb".to_string(),
        retention_period: RetentionPeriod::Indefinite,
    }
    .apply(&mut catalog)
    .unwrap();

    SoftDeleteDatabase {
        database_id: 1,
        deletion_time_ns: 1_000_000_000,
        hard_deletion_time_ns: Some(2_000_000_000),
        hard_delete_scope: Some(DeletionScope::DataAndCatalog),
    }
    .apply(&mut catalog)
    .unwrap();

    HardDeleteDatabase { db_id: 1 }.apply(&mut catalog).unwrap();

    assert!(
        catalog
            .databases
            .get_by_id(&influxdb3_id::DbId::new(1))
            .is_none()
    );
}

#[test]
fn apply_hard_delete_database_data_only_remove_tables() {
    let mut catalog = test_catalog();
    CreateDatabase {
        database_id: 1,
        database_name: "mydb".to_string(),
        retention_period: RetentionPeriod::Indefinite,
    }
    .apply(&mut catalog)
    .unwrap();

    SoftDeleteDatabase {
        database_id: 1,
        deletion_time_ns: 1_000_000_000,
        hard_deletion_time_ns: Some(2_000_000_000),
        hard_delete_scope: Some(DeletionScope::DataOnlyRemoveTables),
    }
    .apply(&mut catalog)
    .unwrap();

    HardDeleteDatabase { db_id: 1 }.apply(&mut catalog).unwrap();

    // Database still exists but tables are removed
    let db = catalog
        .databases
        .get_by_id(&influxdb3_id::DbId::new(1))
        .unwrap();
    assert_eq!(db.table_count(), 0);
}

/// A direct hard delete (no prior soft delete) of a `DataAndCatalog`-scoped
/// database must still capture its name into referencing tokens before the
/// database is removed, so `show tokens` keeps the original name. Mirrors v2's
/// `apply_delete_batch`.
#[test]
fn apply_hard_delete_database_captures_resource_names_in_tokens() {
    use indexmap::IndexMap;
    use influxdb3_authz::{
        Actions, DatabaseActions, Permission, ResourceIdentifier, ResourceMetadata, ResourceType,
        TokenInfo,
    };
    use influxdb3_id::{DbId, TokenId};

    let mut catalog = test_catalog();

    // The database being hard-deleted, and one that should be untouched.
    let db_id = DbId::new(1);
    let other_db_id = DbId::new(2);
    CreateDatabase {
        database_id: db_id.get(),
        database_name: "test_db".to_string(),
        retention_period: RetentionPeriod::Indefinite,
    }
    .apply(&mut catalog)
    .unwrap();
    CreateDatabase {
        database_id: other_db_id.get(),
        database_name: "other_db".to_string(),
        retention_period: RetentionPeriod::Indefinite,
    }
    .apply(&mut catalog)
    .unwrap();

    let db_permission = |db_ids: Vec<DbId>, resource_names| Permission {
        resource_type: ResourceType::Database,
        resource_identifier: ResourceIdentifier::Database(db_ids),
        actions: Actions::Database(DatabaseActions::from(DatabaseActions::READ)),
        resource_names,
    };
    let mut token = |id: u64, perm| {
        let token_id = TokenId::new(id);
        let mut info = TokenInfo::new(
            token_id,
            Arc::from(format!("token-{id}")),
            format!("hash{id}").into_bytes(),
            1234567890,
            None,
        );
        info.set_permissions(vec![perm]);
        catalog.tokens.add_token(token_id, info).unwrap();
        token_id
    };

    // Token 1: references the deleted db, no resource_names yet.
    let token_id_1 = token(1, db_permission(vec![db_id], None));
    // Token 2: references the deleted db plus an unrelated id, with an existing
    // captured name that must be preserved.
    let mut existing_names = IndexMap::new();
    existing_names.insert(
        "999".to_string(),
        ResourceMetadata {
            name: "some_other_db".to_string(),
            deleted: false,
        },
    );
    let token_id_2 = token(
        2,
        db_permission(vec![db_id, DbId::new(999)], Some(existing_names)),
    );
    // Token 3: references only the surviving db; must be left alone.
    let token_id_3 = token(3, db_permission(vec![other_db_id], None));

    HardDeleteDatabase { db_id: db_id.get() }
        .apply(&mut catalog)
        .unwrap();

    // Token 1 gained the captured name, flagged deleted.
    let names_1 = catalog
        .tokens
        .repo()
        .get_by_id(&token_id_1)
        .unwrap()
        .permissions[0]
        .resource_names
        .clone()
        .expect("token 1 should have captured resource_names");
    let captured = names_1.get(&db_id.to_string()).expect("name captured");
    assert_eq!(captured.name, "test_db");
    assert!(captured.deleted);

    // Token 2 keeps its prior name and gains the captured one.
    let names_2 = catalog
        .tokens
        .repo()
        .get_by_id(&token_id_2)
        .unwrap()
        .permissions[0]
        .resource_names
        .clone()
        .expect("token 2 should retain resource_names");
    assert_eq!(
        names_2.get("999").map(|m| m.name.as_str()),
        Some("some_other_db")
    );
    assert_eq!(
        names_2.get(&db_id.to_string()).map(|m| m.name.as_str()),
        Some("test_db")
    );

    // Token 3 references a different db and is unchanged.
    assert!(
        catalog
            .tokens
            .repo()
            .get_by_id(&token_id_3)
            .unwrap()
            .permissions[0]
            .resource_names
            .is_none()
    );

    // The database was removed; the other one survives.
    assert!(catalog.databases.get_by_id(&db_id).is_none());
    assert!(catalog.databases.get_by_id(&other_db_id).is_some());
}
