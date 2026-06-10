use std::sync::Arc;

use uuid::Uuid;

use crate::catalog::versions::v3::inner::InnerCatalog;
use crate::format::CatalogRecord;
use crate::format::records::assert_roundtrip;
use crate::format::records::database::CreateDatabase;
use crate::format::records::retention::{ClearDbRetentionPeriod, SetDbRetentionPeriod};
use crate::format::records::types::RetentionPeriod;

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
fn set_db_retention_round_trip() {
    assert_roundtrip!(
        SetDbRetentionPeriod {
            database_id: 1,
            retention_period: RetentionPeriod::Duration {
                duration_secs: 86_400,
            },
        },
        "0401010280510100"
    );
}

#[test]
fn clear_db_retention_round_trip() {
    assert_roundtrip!(ClearDbRetentionPeriod { database_id: 1 }, "0401");
}

#[test]
fn record_ids() {
    assert_eq!(SetDbRetentionPeriod::ID.raw(), 17);
    assert_eq!(ClearDbRetentionPeriod::ID.raw(), 18);

    assert!(!SetDbRetentionPeriod::FLAGS.is_upgrade_safe());
    assert!(!ClearDbRetentionPeriod::FLAGS.is_upgrade_safe());
}

// ---------------------------------------------------------------------------
// Apply tests
// ---------------------------------------------------------------------------

#[test]
fn apply_set_and_clear_db_retention() {
    let mut catalog = test_catalog();
    setup_database(&mut catalog, 1, "mydb");

    SetDbRetentionPeriod {
        database_id: 1,
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
        crate::catalog::versions::v3::schema::retention::RetentionPeriod::Duration(_)
    ));

    ClearDbRetentionPeriod { database_id: 1 }
        .apply(&mut catalog)
        .unwrap();

    let db = catalog
        .databases
        .get_by_id(&influxdb3_id::DbId::new(1))
        .unwrap();
    assert!(matches!(
        db.retention_period,
        crate::catalog::versions::v3::schema::retention::RetentionPeriod::Indefinite
    ));
}
