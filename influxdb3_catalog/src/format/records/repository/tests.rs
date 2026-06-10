use std::sync::Arc;

use influxdb3_id::{ColumnId, DbId, TableId};
use uuid::Uuid;

use crate::catalog::versions::v3::inner::InnerCatalog;
use crate::format::CatalogRecord;
use crate::format::records::assert_roundtrip;
use crate::format::records::database::CreateDatabase;
use crate::format::records::repository::{NextIdScope, SetNextId};
use crate::format::records::table::CreateTable;
use crate::format::records::types::{FieldFamilyMode, RetentionPeriod};

fn test_catalog() -> InnerCatalog {
    InnerCatalog::new(Arc::from("test"), Uuid::nil())
}

fn setup_database(catalog: &mut InnerCatalog, db_id: u32, name: &str) {
    CreateDatabase {
        database_id: db_id,
        database_name: name.to_string(),
        retention_period: RetentionPeriod::Indefinite,
    }
    .apply(catalog)
    .unwrap();
}

fn setup_table(catalog: &mut InnerCatalog, db_id: u32, db_name: &str, table_id: u32, name: &str) {
    CreateTable {
        database_id: db_id,
        database_name: db_name.to_string(),
        table_name: name.to_string(),
        table_id,
        retention_period: RetentionPeriod::Indefinite,
        field_family_mode: FieldFamilyMode::Auto,
    }
    .apply(catalog)
    .unwrap();
}

#[test]
fn set_next_id_round_trip() {
    assert_roundtrip!(
        SetNextId {
            scope: NextIdScope::Databases,
            id: 99,
        },
        "010663"
    );
    assert_roundtrip!(
        SetNextId {
            scope: NextIdScope::Tables { database_id: 7 },
            id: 42,
        },
        "030407062a"
    );
    assert_roundtrip!(
        SetNextId {
            scope: NextIdScope::Columns {
                database_id: 7,
                table_id: 3,
            },
            id: 65_535,
        },
        "050407040304ffff"
    );
    assert_roundtrip!(
        SetNextId {
            scope: NextIdScope::Tokens,
            id: 9_000_000_000,
        },
        "0200001a711802000000"
    );
}

#[test]
fn record_ids() {
    assert_eq!(SetNextId::ID.raw(), 26);
    assert!(!SetNextId::FLAGS.is_upgrade_safe());
}

#[test]
fn apply_sets_database_counter() {
    let mut catalog = test_catalog();
    setup_database(&mut catalog, 1, "db1");

    SetNextId {
        scope: NextIdScope::Databases,
        id: 99,
    }
    .apply(&mut catalog)
    .unwrap();

    assert_eq!(catalog.databases.next_id(), DbId::new(99));
}

#[test]
fn apply_sets_nested_table_and_column_counters() {
    let mut catalog = test_catalog();
    setup_database(&mut catalog, 1, "db1");
    setup_table(&mut catalog, 1, "db1", 10, "t1");

    SetNextId {
        scope: NextIdScope::Tables { database_id: 1 },
        id: 50,
    }
    .apply(&mut catalog)
    .unwrap();
    SetNextId {
        scope: NextIdScope::Columns {
            database_id: 1,
            table_id: 10,
        },
        id: 65_535,
    }
    .apply(&mut catalog)
    .unwrap();

    let db = catalog.databases.require_by_id(&DbId::new(1)).unwrap();
    assert_eq!(db.tables.next_id(), TableId::new(50));
    let table = db.tables.require_by_id(&TableId::new(10)).unwrap();
    assert_eq!(table.columns.next_id, Some(ColumnId::new(65_535)));
}

#[test]
fn apply_rejects_id_overflowing_repo_type() {
    let mut catalog = test_catalog();
    setup_database(&mut catalog, 1, "db1");
    setup_table(&mut catalog, 1, "db1", 10, "t1");

    // ColumnId is u16; 70_000 does not fit.
    let err = SetNextId {
        scope: NextIdScope::Columns {
            database_id: 1,
            table_id: 10,
        },
        id: 70_000,
    }
    .apply(&mut catalog)
    .unwrap_err();
    assert!(err.0.contains("overflows u16"), "{err:?}");
}
