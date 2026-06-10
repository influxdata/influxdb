use std::sync::Arc;

use uuid::Uuid;

use crate::catalog::versions::v3::inner::InnerCatalog;
use crate::format::CatalogRecord;
use crate::format::records::assert_roundtrip;
use crate::format::records::database::CreateDatabase;
use crate::format::records::trigger::{
    CreateTrigger, DeleteTrigger, DisableTrigger, EnableTrigger,
};
use crate::format::records::types::{
    ErrorBehavior, NodeSpec, RetentionPeriod, TriggerSettings, TriggerSpec,
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
fn create_trigger_round_trip() {
    assert_roundtrip!(
        CreateTrigger {
            trigger_id: 1,
            trigger_name: "my_trigger".to_string(),
            plugin_filename: "plugin.wasm".to_string(),
            database_id: 1,
            node_spec: NodeSpec::All,
            trigger: TriggerSpec::AllTablesWalWrite,
            trigger_settings: TriggerSettings {
                run_async: false,
                error_behavior: ErrorBehavior::Log,
            },
            trigger_arguments: Some(vec![("key".to_string(), "value".to_string())]),
            disabled: false,
        },
        "04010a6d795f747269676765720b706c7567696e2e7761736d0401000100000101036b65790576616c756500"
    );
}

#[test]
fn delete_trigger_round_trip() {
    assert_roundtrip!(
        DeleteTrigger {
            trigger_id: 1,
            trigger_name: "my_trigger".to_string(),
            database_id: 1,
            force: true,
        },
        "04010a6d795f74726967676572040101"
    );
}

#[test]
fn enable_trigger_round_trip() {
    assert_roundtrip!(
        EnableTrigger {
            db_id: 1,
            trigger_id: 5,
            trigger_name: "t1".to_string(),
        },
        "04010405027431"
    );
}

#[test]
fn disable_trigger_round_trip() {
    assert_roundtrip!(
        DisableTrigger {
            db_id: 1,
            trigger_id: 5,
            trigger_name: "t1".to_string(),
        },
        "04010405027431"
    );
}

#[test]
fn record_ids() {
    assert_eq!(CreateTrigger::ID.raw(), 13);
    assert_eq!(DeleteTrigger::ID.raw(), 14);
    assert_eq!(EnableTrigger::ID.raw(), 15);
    assert_eq!(DisableTrigger::ID.raw(), 16);

    assert!(!CreateTrigger::FLAGS.is_upgrade_safe());
    assert!(!DeleteTrigger::FLAGS.is_upgrade_safe());
    assert!(!EnableTrigger::FLAGS.is_upgrade_safe());
    assert!(!DisableTrigger::FLAGS.is_upgrade_safe());
}

// ---------------------------------------------------------------------------
// Apply tests
// ---------------------------------------------------------------------------

#[test]
fn apply_create_and_delete_trigger() {
    let mut catalog = test_catalog();
    setup_database(&mut catalog, 1, "mydb");

    let db = catalog
        .databases
        .get_by_id(&influxdb3_id::DbId::new(1))
        .unwrap();
    assert_eq!(db.processing_engine_triggers.len(), 0);

    CreateTrigger {
        trigger_id: 1,
        trigger_name: "my_trigger".to_string(),
        plugin_filename: "plugin.wasm".to_string(),
        database_id: 1,
        node_spec: NodeSpec::All,
        trigger: TriggerSpec::AllTablesWalWrite,
        trigger_settings: TriggerSettings {
            run_async: false,
            error_behavior: ErrorBehavior::Log,
        },
        trigger_arguments: None,
        disabled: false,
    }
    .apply(&mut catalog)
    .unwrap();

    let db = catalog
        .databases
        .get_by_id(&influxdb3_id::DbId::new(1))
        .unwrap();
    assert_eq!(db.processing_engine_triggers.len(), 1);

    DeleteTrigger {
        trigger_id: 1,
        trigger_name: "my_trigger".to_string(),
        database_id: 1,
        force: false,
    }
    .apply(&mut catalog)
    .unwrap();

    let db = catalog
        .databases
        .get_by_id(&influxdb3_id::DbId::new(1))
        .unwrap();
    assert_eq!(db.processing_engine_triggers.len(), 0);
}

#[test]
fn apply_enable_disable_trigger() {
    let mut catalog = test_catalog();
    setup_database(&mut catalog, 1, "mydb");

    CreateTrigger {
        trigger_id: 1,
        trigger_name: "my_trigger".to_string(),
        plugin_filename: "plugin.wasm".to_string(),
        database_id: 1,
        node_spec: NodeSpec::All,
        trigger: TriggerSpec::AllTablesWalWrite,
        trigger_settings: TriggerSettings {
            run_async: false,
            error_behavior: ErrorBehavior::Log,
        },
        trigger_arguments: None,
        disabled: false,
    }
    .apply(&mut catalog)
    .unwrap();

    DisableTrigger {
        db_id: 1,
        trigger_id: 1,
        trigger_name: "my_trigger".to_string(),
    }
    .apply(&mut catalog)
    .unwrap();

    let db = catalog
        .databases
        .get_by_id(&influxdb3_id::DbId::new(1))
        .unwrap();
    let trigger = db
        .processing_engine_triggers
        .get_by_id(&influxdb3_id::TriggerId::new(1))
        .unwrap();
    assert!(trigger.disabled);

    EnableTrigger {
        db_id: 1,
        trigger_id: 1,
        trigger_name: "my_trigger".to_string(),
    }
    .apply(&mut catalog)
    .unwrap();

    let db = catalog
        .databases
        .get_by_id(&influxdb3_id::DbId::new(1))
        .unwrap();
    let trigger = db
        .processing_engine_triggers
        .get_by_id(&influxdb3_id::TriggerId::new(1))
        .unwrap();
    assert!(!trigger.disabled);
}
