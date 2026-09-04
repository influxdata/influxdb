//! Compatibility tests for v4-format catalog files written by InfluxDB 3 Core before the
//! catalog codebases were unified.
//!
//! Core's catalog structs lacked several fields that exist in the unified definitions, and
//! its trigger definitions carried a `node_id` string instead of a `node_spec`. Files under
//! `fixtures/` were produced by a real `influxdb:3.9.3-core` instance; the unit tests cover
//! Core-written shapes not present in those files. See
//! <https://github.com/influxdata/influxdb/issues/27554>.

use bytes::Bytes;
use serde_json::json;

use crate::log::versions::v4::{
    ClearRetentionPeriodLog, CreateTableLog, DistinctCacheDefinition, MaxAge, MaxCardinality,
    NodeSpec, RetentionPeriod, SetRetentionPeriodLog, TriggerSettings,
    TriggerSpecificationDefinition,
};
use crate::serialize::versions::test_util::from_core_shape;
use crate::snapshot::versions::v4::ProcessingEngineTriggerSnapshot;
use influxdb3_id::{DbId, DistinctCacheId, TableId, TriggerId};

use super::{verify_and_deserialize_catalog_checkpoint_file, verify_and_deserialize_catalog_file};

/// Checkpoint written by Core 3.9.3 at sequence 100, containing a last value cache and a
/// distinct value cache on table `cpu` in database `mydb`.
const CORE_393_CHECKPOINT: &[u8] = include_bytes!("fixtures/core-3.9.3-checkpoint");

#[test]
fn deserialize_core_393_checkpoint() {
    let snapshot =
        verify_and_deserialize_catalog_checkpoint_file(Bytes::from_static(CORE_393_CHECKPOINT))
            .expect("checkpoint written by core 3.9.3 must deserialize");

    let db = snapshot
        .databases
        .repo
        .iter()
        .map(|(_, db)| db)
        .find(|db| db.name.as_ref() == "mydb")
        .expect("mydb is in the checkpoint");
    let table = db
        .tables
        .repo
        .iter()
        .map(|(_, table)| table)
        .find(|table| table.table_name.as_ref() == "cpu")
        .expect("cpu table is in the checkpoint");

    assert!(table.retention_period.is_none());
    let last_cache = table
        .last_caches
        .repo
        .iter()
        .map(|(_, cache)| cache)
        .find(|cache| cache.name.as_ref() == "cpu_lvc")
        .expect("last value cache is in the checkpoint");
    assert_eq!(NodeSpec::All, last_cache.node_spec);
    let distinct_cache = table
        .distinct_caches
        .repo
        .iter()
        .map(|(_, cache)| cache)
        .find(|cache| cache.name.as_ref() == "cpu_dvc")
        .expect("distinct value cache is in the checkpoint");
    assert_eq!(NodeSpec::All, distinct_cache.node_spec);
    assert!(distinct_cache.lookback_seconds.is_none());
    assert!(distinct_cache.refresh_interval.is_none());
}

#[test]
fn deserialize_core_393_log_files() {
    let fixtures: [(&str, &[u8]); 4] = [
        (
            "register-node",
            include_bytes!("fixtures/core-3.9.3-register-node.catalog"),
        ),
        (
            "create-last-cache",
            include_bytes!("fixtures/core-3.9.3-create-last-cache.catalog"),
        ),
        (
            "create-distinct-cache",
            include_bytes!("fixtures/core-3.9.3-create-distinct-cache.catalog"),
        ),
        (
            "create-table",
            include_bytes!("fixtures/core-3.9.3-create-table.catalog"),
        ),
    ];
    for (name, bytes) in fixtures {
        verify_and_deserialize_catalog_file(Bytes::from_static(bytes)).unwrap_or_else(|error| {
            panic!("log file `{name}` written by core 3.9.3 must deserialize: {error:?}")
        });
    }
}

#[test]
fn core_trigger_snapshot_carries_node_id_not_node_spec() {
    let trigger = ProcessingEngineTriggerSnapshot {
        trigger_id: TriggerId::new(0),
        trigger_name: "tr1".into(),
        node_spec: NodeSpec::All,
        plugin_filename: "plugin.py".to_string(),
        database_name: "mydb".into(),
        trigger_specification: TriggerSpecificationDefinition::AllTablesWalWrite,
        trigger_settings: TriggerSettings::default(),
        trigger_arguments: None,
        disabled: false,
    };
    let deserialized = from_core_shape(&trigger, &["node_spec"], &[("node_id", json!("node0"))])
        .expect("trigger snapshot written by core must deserialize");
    assert_eq!(NodeSpec::All, deserialized.node_spec);
}

#[test]
fn core_create_table_log_has_no_retention_period() {
    let log = CreateTableLog {
        database_id: DbId::new(0),
        database_name: "mydb".into(),
        table_name: "cpu".into(),
        table_id: TableId::new(0),
        retention_period: None,
        field_family_mode: Default::default(),
    };
    let deserialized = from_core_shape(&log, &["retention_period"], &[])
        .expect("create table log written by core must deserialize");
    assert!(deserialized.retention_period.is_none());
}

#[test]
fn core_set_retention_period_log_has_no_table() {
    let log = SetRetentionPeriodLog {
        database_name: "mydb".into(),
        database_id: DbId::new(0),
        table: None,
        retention_period: RetentionPeriod::Indefinite,
    };
    let deserialized = from_core_shape(&log, &["table"], &[])
        .expect("set retention period log written by core must deserialize");
    assert!(deserialized.table.is_none());
}

#[test]
fn core_clear_retention_period_log_has_no_table() {
    let log = ClearRetentionPeriodLog {
        database_name: "mydb".into(),
        database_id: DbId::new(0),
        table: None,
    };
    let deserialized = from_core_shape(&log, &["table"], &[])
        .expect("clear retention period log written by core must deserialize");
    assert!(deserialized.table.is_none());
}

#[test]
fn core_distinct_cache_definition_has_no_lookback_or_refresh() {
    let definition = DistinctCacheDefinition {
        table_id: TableId::new(0),
        table_name: "cpu".into(),
        node_spec: NodeSpec::All,
        cache_id: DistinctCacheId::new(0),
        cache_name: "cpu_dvc".into(),
        column_ids: vec![],
        max_cardinality: MaxCardinality::default(),
        max_age_seconds: MaxAge::default(),
        source: Default::default(),
        lookback_seconds: None,
        refresh_interval: None,
    };
    // `lookback_seconds` and `refresh_interval` are skipped when `None`, so the core shape
    // falls out of serializing with them unset; drop the remaining unified-only fields.
    let deserialized = from_core_shape(&definition, &["node_spec", "source"], &[])
        .expect("distinct cache definition written by core must deserialize");
    assert!(deserialized.lookback_seconds.is_none());
    assert!(deserialized.refresh_interval.is_none());
}
