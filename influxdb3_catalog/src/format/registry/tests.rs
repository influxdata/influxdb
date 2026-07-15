use std::sync::Arc;

use uuid::Uuid;

use super::*;
use crate::catalog::versions::v3::events::CatalogEvent;
use crate::format::Encode;
use crate::format::FeatureLevel;
use crate::format::record_ids;
use crate::format::records::CreateDatabase;
use crate::format::records::RegisterNode;
use crate::format::records::types::{NodeMode, RetentionPeriod};

#[test]
fn registry_is_accessible() {
    // Verify we can access the registry without panicking.
    assert!(!REGISTRY.is_empty());
}

#[test]
fn decode_apply_and_event_round_trip() {
    let record = RegisterNode {
        node_catalog_id: 1,
        node_id: "node-a".to_string(),
        instance_id: "inst-1".to_string(),
        registered_time_ns: 1000,
        core_count: 4,
        mode: vec![NodeMode::Core],
        process_uuid: [0u8; 16],
        conn_info: None,
        cli_params: None,
        row_delete_predicate_version: 0,
        feature_level: FeatureLevel::ZERO,
    };

    // Encode
    let mut buf = Vec::new();
    record.encode(&mut buf);

    // Look up in registry
    let entry = REGISTRY.get(record_ids::REGISTER_NODE).unwrap();

    // Decode, apply, and get event via function pointer
    let mut catalog = InnerCatalog::new(Arc::from("test"), Uuid::nil());
    let event = (entry.decode_apply_and_event)(&buf, &mut catalog).unwrap();

    assert!(catalog.nodes.get_by_name("node-a").is_some());
    assert!(matches!(event, CatalogEvent::NodeRegistered { .. }));
}

#[test]
fn unknown_upgrade_safe_returns_false() {
    let result = validate_record_flags(RecordId::from_raw(9999), RecordFlags::upgrade_safe());
    assert!(!result.unwrap());
}

#[test]
fn unknown_non_upgrade_safe_returns_error() {
    let result = validate_record_flags(RecordId::from_raw(9999), RecordFlags::none());
    assert!(matches!(
        result,
        Err(FormatError::UnknownNonUpgradeSafeRecord { record_id: 9999 })
    ));
}

#[test]
fn decode_to_value_round_trips_create_database() {
    let record = CreateDatabase {
        database_id: 7,
        database_name: "mydb".to_string(),
        retention_period: RetentionPeriod::Indefinite,
    };
    let mut buf = Vec::new();
    record.encode(&mut buf);

    let entry = REGISTRY
        .get(<CreateDatabase as CatalogRecord>::ID)
        .expect("CreateDatabase registered");
    let value = (entry.decode_to_value)(&buf).expect("decode to value");

    assert_eq!(value["database_id"], serde_json::json!(7));
    assert_eq!(value["database_name"], serde_json::json!("mydb"));
}
