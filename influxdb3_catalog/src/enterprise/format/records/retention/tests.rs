use std::sync::Arc;

use uuid::Uuid;

use crate::catalog::versions::v3::inner::InnerCatalog;
use crate::enterprise::format::records::retention::{
    ClearTableRetentionPeriod, SetTableRetentionPeriod,
};
use crate::format::CatalogRecord;
use crate::format::records::CreateDatabase;
use crate::format::records::CreateTable;
use crate::format::records::assert_roundtrip;
use crate::format::records::types::{FieldFamilyMode, RetentionPeriod};

/// Helper to create a test catalog.
fn test_catalog() -> InnerCatalog {
    InnerCatalog::new(Arc::from("test"), Uuid::nil())
}

/// Helper: create a database + empty table.
fn setup_db_and_table(catalog: &mut InnerCatalog) {
    CreateDatabase {
        database_id: 1,
        database_name: "mydb".to_string(),
        retention_period: RetentionPeriod::Indefinite,
    }
    .apply(catalog)
    .unwrap();
    CreateTable {
        database_id: 1,
        database_name: "mydb".to_string(),
        table_name: "cpu".to_string(),
        table_id: 10,
        retention_period: RetentionPeriod::Indefinite,
        field_family_mode: FieldFamilyMode::Auto,
    }
    .apply(catalog)
    .unwrap();
}

#[test]
fn set_table_retention_round_trip() {
    assert_roundtrip!(
        SetTableRetentionPeriod {
            database_id: 1,
            table_id: 10,
            retention_period: RetentionPeriod::Duration {
                duration_secs: 3_600,
            },
        },
        "0401040a0104100e"
    );
}

#[test]
fn clear_table_retention_round_trip() {
    assert_roundtrip!(
        ClearTableRetentionPeriod {
            database_id: 1,
            table_id: 10,
        },
        "0401040a"
    );
}

#[test]
fn record_ids() {
    assert_eq!(
        SetTableRetentionPeriod::ID.raw(),
        crate::format::record_ids::SET_TABLE_RETENTION_PERIOD.raw()
    );
    assert_eq!(
        ClearTableRetentionPeriod::ID.raw(),
        crate::format::record_ids::CLEAR_TABLE_RETENTION_PERIOD.raw()
    );
    assert!(!SetTableRetentionPeriod::FLAGS.is_upgrade_safe());
    assert!(!ClearTableRetentionPeriod::FLAGS.is_upgrade_safe());
}

// ---------------------------------------------------------------------------
// Apply tests
// ---------------------------------------------------------------------------

#[test]
fn apply_set_and_clear_table_retention() {
    let mut catalog = test_catalog();
    setup_db_and_table(&mut catalog);

    let table = catalog
        .databases
        .get_by_id(&influxdb3_id::DbId::new(1))
        .unwrap()
        .table_definition_by_id(&influxdb3_id::TableId::new(10))
        .unwrap();
    assert!(matches!(
        table.retention_period,
        crate::catalog::versions::v3::schema::retention::RetentionPeriod::Indefinite
    ));

    SetTableRetentionPeriod {
        database_id: 1,
        table_id: 10,
        retention_period: RetentionPeriod::Duration {
            duration_secs: 7200,
        },
    }
    .apply(&mut catalog)
    .unwrap();

    let table = catalog
        .databases
        .get_by_id(&influxdb3_id::DbId::new(1))
        .unwrap()
        .table_definition_by_id(&influxdb3_id::TableId::new(10))
        .unwrap();
    assert!(matches!(
        table.retention_period,
        crate::catalog::versions::v3::schema::retention::RetentionPeriod::Duration(_)
    ));

    ClearTableRetentionPeriod {
        database_id: 1,
        table_id: 10,
    }
    .apply(&mut catalog)
    .unwrap();

    let table = catalog
        .databases
        .get_by_id(&influxdb3_id::DbId::new(1))
        .unwrap()
        .table_definition_by_id(&influxdb3_id::TableId::new(10))
        .unwrap();
    assert!(matches!(
        table.retention_period,
        crate::catalog::versions::v3::schema::retention::RetentionPeriod::Indefinite
    ));
}
