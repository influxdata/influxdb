use std::sync::Arc;

use uuid::Uuid;

use crate::catalog::versions::v3::inner::InnerCatalog;
use crate::format::CatalogRecord;
use crate::format::records::assert_roundtrip;
use crate::format::records::database::CreateDatabase;
use crate::format::records::table::{AddColumns, CreateTable, HardDeleteTable, SoftDeleteTable};
use crate::format::records::types::{
    ColumnDefinition, DeletionScope, FieldColumn, FieldDataType, FieldFamilyDefinition,
    FieldFamilyMode, FieldFamilyName, FieldIdentifier, RetentionPeriod, TagColumn, TimestampColumn,
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
fn create_table_round_trip() {
    assert_roundtrip!(
        CreateTable {
            database_id: 1,
            database_name: "mydb".to_string(),
            table_name: "cpu".to_string(),
            table_id: 10,
            retention_period: RetentionPeriod::Duration {
                duration_secs: 86_400,
            },
            field_family_mode: FieldFamilyMode::Auto,
        },
        "0401046d79646203637075040a01028051010001"
    );
}

#[test]
fn soft_delete_table_round_trip() {
    assert_roundtrip!(
        SoftDeleteTable {
            database_id: 1,
            table_id: 10,
            deletion_time_ns: 1234567890,
            hard_deletion_time_ns: Some(9876543210),
            hard_delete_scope: Some(DeletionScope::DataAndCatalog),
        },
        "0401040a02d20296490100ea16b04c020000000100"
    );
}

#[test]
fn hard_delete_table_round_trip() {
    assert_roundtrip!(
        HardDeleteTable {
            db_id: 1,
            table_id: 10,
        },
        "0401040a"
    );
}

#[test]
fn add_columns_round_trip() {
    assert_roundtrip!(
        AddColumns {
            database_id: 1,
            table_id: 10,
            columns: vec![
                ColumnDefinition::Field(FieldColumn {
                    id: FieldIdentifier {
                        family_id: 0,
                        field_id: 1,
                    },
                    column_id: Some(1),
                    name: "value".to_string(),
                    data_type: FieldDataType::Float,
                }),
                // A column added after the legacy ordinal space was exhausted
                // (PachaTree mode) carries no ordinal.
                ColumnDefinition::Field(FieldColumn {
                    id: FieldIdentifier {
                        family_id: 0,
                        field_id: 2,
                    },
                    column_id: None,
                    name: "overflow".to_string(),
                    data_type: FieldDataType::Integer,
                }),
            ],
            field_families: vec![],
        },
        "0401040a0208020000020102010100050876616c75656f766572666c6f770900"
    );
}

#[test]
fn record_ids() {
    assert_eq!(CreateTable::ID.raw(), 6);
    assert!(!CreateTable::FLAGS.is_upgrade_safe());

    assert_eq!(SoftDeleteTable::ID.raw(), 7);
    assert!(!SoftDeleteTable::FLAGS.is_upgrade_safe());

    assert_eq!(AddColumns::ID.raw(), 8);
    assert!(!AddColumns::FLAGS.is_upgrade_safe());

    assert_eq!(HardDeleteTable::ID.raw(), 23);
    assert!(!HardDeleteTable::FLAGS.is_upgrade_safe());
}

// ---------------------------------------------------------------------------
// Apply tests
// ---------------------------------------------------------------------------

#[test]
fn apply_create_table() {
    let mut catalog = test_catalog();
    setup_database(&mut catalog, 1, "mydb");

    CreateTable {
        database_id: 1,
        database_name: "mydb".to_string(),
        table_name: "cpu".to_string(),
        table_id: 10,
        retention_period: RetentionPeriod::Indefinite,
        field_family_mode: FieldFamilyMode::Auto,
    }
    .apply(&mut catalog)
    .unwrap();

    let db = catalog
        .databases
        .get_by_id(&influxdb3_id::DbId::new(1))
        .unwrap();
    let table = db.table_definition("cpu").expect("table should exist");
    assert_eq!(table.table_id, influxdb3_id::TableId::new(10));
}

#[test]
fn apply_add_columns() {
    let mut catalog = test_catalog();
    setup_database(&mut catalog, 1, "mydb");

    CreateTable {
        database_id: 1,
        database_name: "mydb".to_string(),
        table_name: "cpu".to_string(),
        table_id: 10,
        retention_period: RetentionPeriod::Indefinite,
        field_family_mode: FieldFamilyMode::Auto,
    }
    .apply(&mut catalog)
    .unwrap();

    AddColumns {
        database_id: 1,
        table_id: 10,
        columns: vec![
            ColumnDefinition::Timestamp(TimestampColumn {
                column_id: Some(0),
                name: "time".to_string(),
            }),
            ColumnDefinition::Tag(TagColumn {
                id: 0,
                column_id: Some(1),
                name: "host".to_string(),
            }),
            ColumnDefinition::Field(FieldColumn {
                id: FieldIdentifier {
                    family_id: 0,
                    field_id: 0,
                },
                column_id: Some(2),
                name: "usage".to_string(),
                data_type: FieldDataType::Float,
            }),
        ],
        field_families: vec![FieldFamilyDefinition {
            id: 0,
            name: FieldFamilyName::Auto(0),
        }],
    }
    .apply(&mut catalog)
    .unwrap();

    let db = catalog
        .databases
        .get_by_id(&influxdb3_id::DbId::new(1))
        .unwrap();
    let table = db.table_definition("cpu").unwrap();
    assert_eq!(table.num_columns(), 3);
    assert_eq!(table.num_tag_columns(), 1);
    assert_eq!(table.num_field_columns(), 1);
}

#[test]
fn apply_soft_and_hard_delete_table() {
    let mut catalog = test_catalog();
    setup_database(&mut catalog, 1, "mydb");

    CreateTable {
        database_id: 1,
        database_name: "mydb".to_string(),
        table_name: "cpu".to_string(),
        table_id: 10,
        retention_period: RetentionPeriod::Indefinite,
        field_family_mode: FieldFamilyMode::Auto,
    }
    .apply(&mut catalog)
    .unwrap();

    SoftDeleteTable {
        database_id: 1,
        table_id: 10,
        deletion_time_ns: 1_000_000_000,
        hard_deletion_time_ns: Some(2_000_000_000),
        hard_delete_scope: Some(DeletionScope::DataAndCatalog),
    }
    .apply(&mut catalog)
    .unwrap();

    let db = catalog
        .databases
        .get_by_id(&influxdb3_id::DbId::new(1))
        .unwrap();
    let table = db
        .table_definition_by_id(&influxdb3_id::TableId::new(10))
        .unwrap();
    assert!(table.deleted);
    // Table name should be renamed
    assert!(table.table_name.contains("cpu-"));
    // Original name should no longer resolve
    assert!(db.table_definition("cpu").is_none());

    HardDeleteTable {
        db_id: 1,
        table_id: 10,
    }
    .apply(&mut catalog)
    .unwrap();

    let db = catalog
        .databases
        .get_by_id(&influxdb3_id::DbId::new(1))
        .unwrap();
    assert!(
        db.table_definition_by_id(&influxdb3_id::TableId::new(10))
            .is_none()
    );
}
