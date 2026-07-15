use std::sync::Arc;
use std::time::Duration;

use hashbrown::HashMap;
use indexmap::IndexMap;
use influxdb3_authz::{ResourceMetadata, SystemActions, SystemResourceIdentifier};
use influxdb3_id::{
    CatalogId, ColumnId, ColumnIdentifier, DbId, DistinctCacheId, FieldFamilyId, FieldId,
    FieldIdentifier, LastCacheId, NodeId, SerdeVecMap, TableId, TagId, TokenId, TriggerId,
};
use uuid::Uuid;

use super::synthesize_records;
use crate::catalog::versions::v2::{
    DeletionScope as V2DeletionScope, FieldFamilyMode as V2FieldFamilyMode,
    FieldFamilyName as V2FieldFamilyName,
};
use crate::catalog::versions::v3::inner::InnerCatalog;
use crate::enterprise::format::records::CreateResourceScopedToken;
use crate::format::Decode;
use crate::format::apply::{RestorePreload, apply_records};
use crate::format::record_ids;
use crate::format::records::types::{
    Actions as WireActions, CacheSource as WireCacheSource, ColumnDefinition as WireColumnDef,
    ColumnIdentifier as WireColumnId, DeletionScope as WireDeletionScope,
    FieldFamilyMode as WireFieldFamilyMode, FieldFamilyName as WireFieldFamilyName,
    LastCacheValueColumnsDef as WireLvcDef, ResourceIdentifier as WireResourceIdent,
    ResourceType as WireResourceType, RetentionPeriod as WireRetentionPeriod,
};
use crate::format::records::{
    AddColumns, CreateAdminToken, CreateDatabase, CreateDistinctCache, CreateLastCache,
    CreateTable, CreateTrigger, NextIdScope, RegisterNode, SetNextId, SoftDeleteDatabase,
    SoftDeleteTable,
};
use crate::log::versions::v4::{
    CacheSource, MaxAge, MaxCardinality, NodeMode, NodeSpec, RefreshInterval, StorageMode,
    TriggerSettings, TriggerSpecificationDefinition,
};
use crate::snapshot::versions::v4::{
    ActionsSnapshot, CatalogSnapshot, ColumnDefinitionSnapshot, ColumnSetSnapshot,
    DatabaseActionsSnapshot, DatabaseSnapshot, DistinctCacheSnapshot, FieldColumnSnapshot,
    FieldDataType, FieldFamilySnapshot, GenerationConfigSnapshot, LastCacheSnapshot, NodeSnapshot,
    NodeStateSnapshot, PermissionSnapshot, ProcessingEngineTriggerSnapshot, RepositorySnapshot,
    ResourceIdentifierSnapshot, ResourceTypeSnapshot, RetentionPeriodSnapshot,
    SystemActionsSnapshot, TableSnapshot, TagColumnSnapshot, TimestampColumnSnapshot,
    TokenInfoSnapshot,
};
use crate::{catalog::CatalogSequenceNumber, format::RecordId};

// ---------------------------------------------------------------------------
// Snapshot builders
// ---------------------------------------------------------------------------

fn empty_snapshot(sequence: u64) -> CatalogSnapshot {
    CatalogSnapshot {
        generation_config: GenerationConfigSnapshot {
            generation_durations: SerdeVecMap::default(),
        },
        storage_mode: StorageMode::default(),
        nodes: empty_repo(),
        databases: empty_repo(),
        sequence: CatalogSequenceNumber::new(sequence),
        tokens: empty_repo(),
        catalog_id: Arc::from("test"),
        catalog_uuid: Uuid::nil(),
    }
}

fn empty_repo<I, V>() -> RepositorySnapshot<I, V>
where
    I: influxdb3_id::CatalogId + std::hash::Hash + Eq + Default,
{
    RepositorySnapshot {
        repo: SerdeVecMap::default(),
        next_id: I::default(),
    }
}

fn running_node(id: u32, name: &str, registered_time_ns: i64) -> NodeSnapshot {
    NodeSnapshot {
        node_id: Arc::from(name),
        node_catalog_id: NodeId::new(id),
        instance_id: Arc::from(format!("inst-{id}")),
        mode: vec![NodeMode::Core],
        state: NodeStateSnapshot::Running { registered_time_ns },
        core_count: 4,
        conn_info: None,
        cli_params: None,
        row_delete_predicate_version: 0,
    }
}

fn stopped_node(id: u32, name: &str, stopped_time_ns: i64) -> NodeSnapshot {
    NodeSnapshot {
        state: NodeStateSnapshot::Stopped { stopped_time_ns },
        ..running_node(id, name, 0)
    }
}

fn database(id: u32, name: &str, retention: Option<RetentionPeriodSnapshot>) -> DatabaseSnapshot {
    DatabaseSnapshot {
        id: DbId::new(id),
        name: Arc::from(name),
        tables: empty_repo(),
        retention_period: retention,
        processing_engine_triggers: empty_repo(),
        deleted: false,
        hard_delete_time: None,
        hard_delete_scope: None,
    }
}

fn table(id: u32, name: &str) -> TableSnapshot {
    TableSnapshot {
        table_id: TableId::new(id),
        table_name: Arc::from(name),
        key: vec![],
        columns: ColumnSetSnapshot::default(),
        field_families: empty_repo(),
        retention_period: None,
        last_caches: empty_repo(),
        distinct_caches: empty_repo(),
        deleted: false,
        hard_delete_time: None,
        field_family_mode: V2FieldFamilyMode::Aware,
        hard_delete_scope: None,
    }
}

fn tag_col(id: u16, column_id: u16, name: &str) -> ColumnDefinitionSnapshot {
    ColumnDefinitionSnapshot::Tag(TagColumnSnapshot {
        id: TagId::new(id),
        column_id: Some(ColumnId::new(column_id)),
        name: Arc::from(name),
    })
}

fn time_col(column_id: u16) -> ColumnDefinitionSnapshot {
    ColumnDefinitionSnapshot::Timestamp(TimestampColumnSnapshot {
        column_id: Some(ColumnId::new(column_id)),
        name: Arc::from("time"),
    })
}

fn field_col(
    family_id: u16,
    field_id: u16,
    column_id: u16,
    name: &str,
    data_type: FieldDataType,
) -> ColumnDefinitionSnapshot {
    ColumnDefinitionSnapshot::Field(FieldColumnSnapshot {
        id: FieldIdentifier::new(FieldFamilyId::new(family_id), FieldId::new(field_id)),
        column_id: Some(ColumnId::new(column_id)),
        name: Arc::from(name),
        data_type,
    })
}

fn field_family(id: u16, name: V2FieldFamilyName) -> FieldFamilySnapshot {
    FieldFamilySnapshot {
        name,
        id: FieldFamilyId::new(id),
    }
}

fn admin_token(id: u64, name: &str) -> TokenInfoSnapshot {
    TokenInfoSnapshot {
        id: TokenId::from(id),
        name: Arc::from(name),
        hash: vec![1, 2, 3],
        created_at: 1_700_000_000_000,
        description: None,
        created_by: None,
        // v2 uses `i64::MAX` for "never expires".
        expiry: i64::MAX,
        updated_by: None,
        updated_at: None,
        permissions: vec![PermissionSnapshot {
            resource_type: ResourceTypeSnapshot::Wildcard,
            resource_identifier: ResourceIdentifierSnapshot::Wildcard,
            actions: ActionsSnapshot::Wildcard,
            resource_names: None,
        }],
    }
}

fn resource_scoped_token(
    id: u64,
    name: &str,
    permissions: Vec<PermissionSnapshot>,
) -> TokenInfoSnapshot {
    TokenInfoSnapshot {
        permissions,
        ..admin_token(id, name)
    }
}

fn last_cache(table_id: u32, id: u16, name: &str) -> LastCacheSnapshot {
    LastCacheSnapshot {
        table_id: TableId::new(table_id),
        table: Arc::from("t"),
        node_spec: NodeSpec::All,
        id: LastCacheId::new(id),
        name: Arc::from(name),
        keys: vec![ColumnIdentifier::Tag(TagId::new(0))],
        vals: None,
        n: 1,
        ttl: 3_600,
    }
}

fn distinct_cache(table_id: u32, id: u16, name: &str) -> DistinctCacheSnapshot {
    DistinctCacheSnapshot {
        table_id: TableId::new(table_id),
        table: Arc::from("t"),
        node_spec: NodeSpec::All,
        id: DistinctCacheId::new(id),
        name: Arc::from(name),
        cols: vec![ColumnIdentifier::Tag(TagId::new(0))],
        max_cardinality: MaxCardinality::default(),
        max_age_seconds: MaxAge::from_secs(86_400),
        source: CacheSource::User,
        lookback_seconds: None,
        refresh_interval: None,
    }
}

fn trigger(id: u32, name: &str, disabled: bool) -> ProcessingEngineTriggerSnapshot {
    ProcessingEngineTriggerSnapshot {
        trigger_id: TriggerId::new(id),
        trigger_name: Arc::from(name),
        node_spec: NodeSpec::All,
        plugin_filename: format!("{name}.py"),
        database_name: Arc::from("db"),
        trigger_specification: TriggerSpecificationDefinition::AllTablesWalWrite,
        trigger_settings: TriggerSettings::default(),
        trigger_arguments: None,
        disabled,
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[test]
fn synthesize_empty_snapshot_produces_empty_batch() {
    let snap = empty_snapshot(7);
    let batch = synthesize_records(&snap);

    assert_eq!(batch.sequence(), 7);
    assert!(batch.is_empty());
}

#[test]
fn synthesize_uses_snapshots_sequence() {
    let snap = empty_snapshot(42);
    let batch = synthesize_records(&snap);
    assert_eq!(batch.sequence(), 42);
}

#[test]
fn generation_config_emits_one_record_per_level() {
    let mut snap = empty_snapshot(1);
    snap.generation_config
        .generation_durations
        .insert(0, Duration::from_secs(60));
    snap.generation_config
        .generation_durations
        .insert(1, Duration::from_secs(3_600));

    let batch = synthesize_records(&snap);
    let records = batch.as_slice();

    assert_eq!(records.len(), 2);
    assert_eq!(records[0].id(), record_ids::SET_GENERATION_DURATION);
    assert_eq!(records[1].id(), record_ids::SET_GENERATION_DURATION);
}

#[test]
fn storage_mode_default_emits_no_record() {
    let snap = empty_snapshot(1); // StorageMode::default() == Parquet
    let batch = synthesize_records(&snap);
    assert!(batch.is_empty());
}

#[test]
fn storage_mode_non_default_emits_one_record() {
    let mut snap = empty_snapshot(1);
    snap.storage_mode = StorageMode::PachaTree;

    let batch = synthesize_records(&snap);
    let records = batch.as_slice();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].id(), record_ids::SET_STORAGE_MODE);
}

#[test]
fn running_node_emits_register_only() {
    let mut snap = empty_snapshot(1);
    snap.nodes
        .repo
        .insert(NodeId::new(1), running_node(1, "node-a", 12_345));

    let batch = synthesize_records(&snap);
    let records = batch.as_slice();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].id(), record_ids::REGISTER_NODE);
}

#[test]
fn stopped_node_emits_register_then_stop_with_zero_registration() {
    let mut snap = empty_snapshot(1);
    snap.nodes
        .repo
        .insert(NodeId::new(2), stopped_node(2, "node-b", 99_999));

    let batch = synthesize_records(&snap);
    let records = batch.as_slice();

    // RegisterNode is emitted with the placeholder registration time, then
    // StopNode drives the node into the Stopped state.
    assert_eq!(records.len(), 2);
    assert_eq!(records[0].id(), record_ids::REGISTER_NODE);
    assert_eq!(records[1].id(), record_ids::STOP_NODE);

    let register = RegisterNode::decode(&records[0].data).expect("decode RegisterNode");
    assert_eq!(register.registered_time_ns, 0);
    assert_eq!(register.process_uuid, [0u8; 16]);
}

#[test]
fn register_node_carries_row_delete_predicate_version() {
    let mut node = running_node(3, "node-c", 1_000);
    node.row_delete_predicate_version = 7;
    let mut snap = empty_snapshot(1);
    snap.nodes.repo.insert(NodeId::new(3), node);

    let batch = synthesize_records(&snap);
    let records = batch.as_slice();
    let register = RegisterNode::decode(&records[0].data).expect("decode RegisterNode");
    assert_eq!(register.row_delete_predicate_version, 7);
}

#[test]
fn database_emits_create_with_name_and_id() {
    let mut snap = empty_snapshot(1);
    snap.databases
        .repo
        .insert(DbId::new(10), database(10, "shop", None));

    let batch = synthesize_records(&snap);
    let records = batch.as_slice();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].id(), record_ids::CREATE_DATABASE);

    let create = CreateDatabase::decode(&records[0].data).expect("decode CreateDatabase");
    assert_eq!(create.database_id, 10);
    assert_eq!(create.database_name, "shop");
    assert_eq!(create.retention_period, WireRetentionPeriod::Indefinite);
}

#[test]
fn database_carries_retention_period_duration() {
    let mut snap = empty_snapshot(1);
    snap.databases.repo.insert(
        DbId::new(11),
        database(
            11,
            "with_retention",
            Some(RetentionPeriodSnapshot::Duration(Duration::from_secs(
                3_600,
            ))),
        ),
    );

    let batch = synthesize_records(&snap);
    let create = CreateDatabase::decode(&batch.as_slice()[0].data).expect("decode CreateDatabase");
    assert_eq!(
        create.retention_period,
        WireRetentionPeriod::Duration {
            duration_secs: 3_600
        }
    );
}

#[test]
fn soft_deleted_database_emits_soft_delete_after_create() {
    let mut db = database(20, "gone", None);
    db.deleted = true;
    db.hard_delete_time = Some(1_700_000_000_000);
    db.hard_delete_scope = Some(V2DeletionScope::DataAndCatalog);

    let mut snap = empty_snapshot(1);
    snap.databases.repo.insert(DbId::new(20), db);

    let batch = synthesize_records(&snap);
    let records = batch.as_slice();
    assert_eq!(records.len(), 2);
    assert_eq!(records[0].id(), record_ids::CREATE_DATABASE);
    assert_eq!(records[1].id(), record_ids::SOFT_DELETE_DATABASE);

    let soft = SoftDeleteDatabase::decode(&records[1].data).expect("decode SoftDeleteDatabase");
    assert_eq!(soft.database_id, 20);
    // The name "gone" has no parseable suffix, so the recovery falls back to 0.
    assert_eq!(soft.deletion_time_ns, 0);
    assert_eq!(soft.hard_deletion_time_ns, Some(1_700_000_000_000));
    assert_eq!(
        soft.hard_delete_scope,
        Some(WireDeletionScope::DataAndCatalog)
    );
}

#[test]
fn soft_deleted_database_recovers_name_and_timestamp_from_suffix() {
    // v2 renames a soft-deleted database in place to `{original}-{ts}`; the
    // migration recovers the original name and timestamp from the suffix.
    let mut db = database(21, "shop-20250511T143000", None);
    db.deleted = true;

    let mut snap = empty_snapshot(1);
    snap.databases.repo.insert(DbId::new(21), db);

    let batch = synthesize_records(&snap);
    let records = batch.as_slice();
    let create = CreateDatabase::decode(&records[0].data).expect("decode CreateDatabase");
    let soft = SoftDeleteDatabase::decode(&records[1].data).expect("decode SoftDeleteDatabase");

    assert_eq!(create.database_name, "shop");
    assert!(soft.deletion_time_ns > 0);
}

#[test]
fn migrated_soft_deleted_database_apply_reproduces_renamed_name() {
    // End-to-end: the synthesized records, replayed through `apply_records`,
    // must reproduce v2's renamed name exactly — not append a second suffix.
    let renamed = "analytics-20240301T091500";
    let mut db = database(30, renamed, None);
    db.deleted = true;

    let mut snap = empty_snapshot(1);
    snap.databases.repo.insert(DbId::new(30), db);

    let batch = synthesize_records(&snap);
    let mut catalog = InnerCatalog::new(Arc::from("test"), Uuid::nil());
    apply_records(
        batch.as_slice(),
        &mut catalog,
        CatalogSequenceNumber::new(1),
        &mut RestorePreload::empty(),
    )
    .expect("apply succeeds");

    let db = catalog
        .databases
        .require_by_id(&DbId::new(30))
        .expect("database present");
    assert!(db.deleted);
    assert_eq!(db.name.as_ref(), renamed);
}

#[test]
fn split_soft_deleted_name_recovers_original_and_falls_back() {
    use super::split_soft_deleted_name;

    let (name, ns) = split_soft_deleted_name("orders-20250511T143000");
    assert_eq!(name, "orders");
    assert!(ns > 0);
    // Splits on the last hyphen — the original name may itself contain hyphens.
    assert_eq!(
        split_soft_deleted_name("us-east-orders-20250511T143000").0,
        "us-east-orders"
    );
    // Unparseable suffix → name verbatim, epoch-0 timestamp.
    assert_eq!(split_soft_deleted_name("plain"), ("plain".to_string(), 0));
    assert_eq!(
        split_soft_deleted_name("orders-notatime"),
        ("orders-notatime".to_string(), 0)
    );
}

#[test]
fn multiple_databases_emitted_in_repo_order() {
    let mut snap = empty_snapshot(1);
    snap.databases
        .repo
        .insert(DbId::new(1), database(1, "first", None));
    snap.databases
        .repo
        .insert(DbId::new(2), database(2, "second", None));
    snap.databases
        .repo
        .insert(DbId::new(3), database(3, "third", None));

    let batch = synthesize_records(&snap);
    let names: Vec<String> = batch
        .as_slice()
        .iter()
        .map(|r| {
            CreateDatabase::decode(&r.data)
                .expect("decode")
                .database_name
        })
        .collect();
    assert_eq!(names, vec!["first", "second", "third"]);
}

#[test]
fn database_with_triggers_emits_create_trigger_per_trigger() {
    let mut db = database(40, "db_with_triggers", None);
    db.processing_engine_triggers
        .repo
        .insert(TriggerId::new(1), trigger(1, "first", false));
    db.processing_engine_triggers
        .repo
        .insert(TriggerId::new(2), trigger(2, "second", false));

    let mut snap = empty_snapshot(1);
    snap.databases.repo.insert(DbId::new(40), db);

    let batch = synthesize_records(&snap);
    let records = batch.as_slice();
    assert_eq!(records.len(), 3);
    assert_eq!(records[0].id(), record_ids::CREATE_DATABASE);
    assert_eq!(records[1].id(), record_ids::CREATE_TRIGGER);
    assert_eq!(records[2].id(), record_ids::CREATE_TRIGGER);

    let names: Vec<String> = records[1..]
        .iter()
        .map(|r| CreateTrigger::decode(&r.data).expect("decode").trigger_name)
        .collect();
    assert_eq!(names, vec!["first", "second"]);
}

#[test]
fn trigger_disabled_flag_carries_through() {
    let mut db = database(41, "db", None);
    db.processing_engine_triggers
        .repo
        .insert(TriggerId::new(1), trigger(1, "off", true));

    let mut snap = empty_snapshot(1);
    snap.databases.repo.insert(DbId::new(41), db);

    let batch = synthesize_records(&snap);
    let create = CreateTrigger::decode(&batch.as_slice()[1].data).expect("decode CreateTrigger");
    assert!(create.disabled);
    assert_eq!(create.database_id, 41);
}

#[test]
fn trigger_arguments_sorted_by_key_for_determinism() {
    // v2's `HashMap` iteration order is non-deterministic; synthesis sorts by
    // key so re-running on the same snapshot produces an identical batch.
    let mut t = trigger(1, "t", false);
    let mut args = HashMap::new();
    args.insert("zeta".to_string(), "3".to_string());
    args.insert("alpha".to_string(), "1".to_string());
    args.insert("mu".to_string(), "2".to_string());
    t.trigger_arguments = Some(args);

    let mut db = database(42, "db", None);
    db.processing_engine_triggers
        .repo
        .insert(TriggerId::new(1), t);
    let mut snap = empty_snapshot(1);
    snap.databases.repo.insert(DbId::new(42), db);

    let batch = synthesize_records(&snap);
    let create = CreateTrigger::decode(&batch.as_slice()[1].data).expect("decode CreateTrigger");
    let keys: Vec<&str> = create
        .trigger_arguments
        .as_ref()
        .expect("args present")
        .iter()
        .map(|(k, _)| k.as_str())
        .collect();
    assert_eq!(keys, vec!["alpha", "mu", "zeta"]);
}

#[test]
fn empty_database_emits_no_table_records() {
    let mut snap = empty_snapshot(1);
    snap.databases
        .repo
        .insert(DbId::new(1), database(1, "db", None));

    let batch = synthesize_records(&snap);
    let ids: Vec<RecordId> = batch.as_slice().iter().map(|r| r.id()).collect();
    assert_eq!(ids, [record_ids::CREATE_DATABASE]);
}

#[test]
fn table_emits_create_then_add_columns() {
    let mut t = table(100, "metrics");
    t.columns.repo.push(time_col(0));
    t.columns.repo.push(tag_col(0, 1, "host"));
    t.key.push(TagId::new(0));

    let mut db = database(1, "db", None);
    db.tables.repo.insert(TableId::new(100), t);

    let mut snap = empty_snapshot(1);
    snap.databases.repo.insert(DbId::new(1), db);

    let batch = synthesize_records(&snap);
    let records = batch.as_slice();
    assert_eq!(records.len(), 3);
    assert_eq!(records[0].id(), record_ids::CREATE_DATABASE);
    assert_eq!(records[1].id(), record_ids::CREATE_TABLE);
    assert_eq!(records[2].id(), record_ids::ADD_COLUMNS);

    let create = CreateTable::decode(&records[1].data).expect("decode CreateTable");
    assert_eq!(create.table_id, 100);
    assert_eq!(create.table_name, "metrics");
    assert_eq!(create.database_id, 1);
    assert_eq!(create.database_name, "db");
    assert_eq!(create.field_family_mode, WireFieldFamilyMode::Aware);
}

#[test]
fn add_columns_orders_tags_by_v2_series_key() {
    // v3 rebuilds series_key from tag-insertion order in AddColumns, so the
    // migration must emit tags in v2.key order even if they appear in a
    // different order in the columns repo.
    let mut t = table(1, "t");
    t.columns.repo.push(time_col(0));
    t.columns.repo.push(tag_col(0, 1, "a"));
    t.columns.repo.push(tag_col(1, 2, "b"));
    t.columns.repo.push(tag_col(2, 3, "c"));
    // Series key in non-repo order.
    t.key = vec![TagId::new(2), TagId::new(0), TagId::new(1)];

    let mut db = database(1, "db", None);
    db.tables.repo.insert(TableId::new(1), t);
    let mut snap = empty_snapshot(1);
    snap.databases.repo.insert(DbId::new(1), db);

    let batch = synthesize_records(&snap);
    let add = AddColumns::decode(&batch.as_slice()[2].data).expect("decode AddColumns");

    let tag_names: Vec<&str> = add
        .columns
        .iter()
        .filter_map(|c| match c {
            WireColumnDef::Tag(t) => Some(t.name.as_str()),
            _ => None,
        })
        .collect();
    assert_eq!(tag_names, vec!["c", "a", "b"]);
}

#[test]
fn add_columns_carries_field_families_and_fields() {
    let mut t = table(1, "t");
    t.columns.repo.push(time_col(0));
    t.columns
        .repo
        .push(field_col(0, 0, 1, "cpu", FieldDataType::Float));
    t.field_families.repo.insert(
        FieldFamilyId::new(0),
        field_family(0, V2FieldFamilyName::User(Arc::from("user_ff"))),
    );

    let mut db = database(1, "db", None);
    db.tables.repo.insert(TableId::new(1), t);
    let mut snap = empty_snapshot(1);
    snap.databases.repo.insert(DbId::new(1), db);

    let batch = synthesize_records(&snap);
    let add = AddColumns::decode(&batch.as_slice()[2].data).expect("decode AddColumns");

    assert_eq!(add.field_families.len(), 1);
    assert_eq!(add.field_families[0].id, 0);
    match &add.field_families[0].name {
        WireFieldFamilyName::User(s) => assert_eq!(s, "user_ff"),
        _ => panic!("expected User field family name"),
    }
    let has_field = add
        .columns
        .iter()
        .any(|c| matches!(c, WireColumnDef::Field(f) if f.name == "cpu"));
    assert!(has_field);
}

#[test]
fn add_columns_preserves_auto_field_family_name() {
    let mut t = table(1, "t");
    t.field_family_mode = V2FieldFamilyMode::Auto;
    t.columns.repo.push(time_col(0));
    t.field_families.repo.insert(
        FieldFamilyId::new(0),
        field_family(0, V2FieldFamilyName::Auto(0)),
    );

    let mut db = database(1, "db", None);
    db.tables.repo.insert(TableId::new(1), t);
    let mut snap = empty_snapshot(1);
    snap.databases.repo.insert(DbId::new(1), db);

    let batch = synthesize_records(&snap);
    let create = CreateTable::decode(&batch.as_slice()[1].data).expect("decode CreateTable");
    assert_eq!(create.field_family_mode, WireFieldFamilyMode::Auto);

    let add = AddColumns::decode(&batch.as_slice()[2].data).expect("decode AddColumns");
    match &add.field_families[0].name {
        WireFieldFamilyName::Auto(n) => assert_eq!(*n, 0),
        _ => panic!("expected Auto field family name"),
    }
}

#[test]
fn table_without_timestamp_column_gets_one_synthesized() {
    // v2 should never produce a table without a timestamp column, but if it
    // did the migrated v3 table would have no way to acquire one (no API to
    // add `time` after the fact). The migration adds the time timestamp column
    // so the table comes out usable.
    let mut db = database(1, "db", None);
    db.tables.repo.insert(TableId::new(1), table(1, "t"));
    let mut snap = empty_snapshot(1);
    snap.databases.repo.insert(DbId::new(1), db);

    let batch = synthesize_records(&snap);
    let ids: Vec<RecordId> = batch.as_slice().iter().map(|r| r.id()).collect();
    assert_eq!(
        ids,
        [
            record_ids::CREATE_DATABASE,
            record_ids::CREATE_TABLE,
            record_ids::ADD_COLUMNS,
        ]
    );

    let add = AddColumns::decode(&batch.as_slice()[2].data).expect("decode AddColumns");
    assert_eq!(add.columns.len(), 1);
    let WireColumnDef::Timestamp(ts) = &add.columns[0] else {
        panic!(
            "expected synthesized timestamp column, got {:?}",
            add.columns[0]
        )
    };
    assert_eq!(ts.name, "time");
    assert_eq!(ts.column_id, Some(1));
}

#[test_log::test]
fn table_with_max_legacy_columns_but_no_time_gets_none_column_id_for_time() {
    let mut db = database(1, "foo", None);
    let mut tbl = table(1, "bar");
    tbl.columns
        .repo
        .push(ColumnDefinitionSnapshot::Tag(TagColumnSnapshot {
            id: TagId::new(1),
            column_id: Some(ColumnId::MAX),
            name: "baz".into(),
        }));
    tbl.key.push(TagId::new(1));
    tbl.columns.next_id = ColumnId::MAX;
    db.tables.repo.insert(TableId::new(1), tbl);
    let mut snap = empty_snapshot(1);
    snap.databases.repo.insert(DbId::new(1), db);

    let batch = synthesize_records(&snap);
    let ids: Vec<RecordId> = batch.as_slice().iter().map(|r| r.id()).collect();
    assert_eq!(
        ids,
        [
            record_ids::CREATE_DATABASE,
            record_ids::CREATE_TABLE,
            record_ids::ADD_COLUMNS,
        ]
    );
    let add = AddColumns::decode(&batch.as_slice()[2].data).expect("decode AddColumns");
    assert_eq!(add.columns.len(), 2);
    let WireColumnDef::Timestamp(ts) = &add.columns[1] else {
        panic!(
            "expected synthesized timestamp column, got {:?}",
            add.columns[1]
        );
    };
    assert_eq!(ts.name, "time");
    assert!(ts.column_id.is_none());
}

#[test]
fn migrated_table_without_timestamp_gets_usable_v3_table() {
    let mut db = database(1, "db", None);
    db.tables.repo.insert(TableId::new(1), table(1, "t"));
    let mut snap = empty_snapshot(1);
    snap.databases.repo.insert(DbId::new(1), db);

    let batch = synthesize_records(&snap);
    let mut catalog = InnerCatalog::new(Arc::from("test"), Uuid::nil());
    apply_records(
        batch.as_slice(),
        &mut catalog,
        CatalogSequenceNumber::new(1),
        &mut RestorePreload::empty(),
    )
    .expect("apply succeeds");

    let db = catalog.databases.require_by_id(&DbId::new(1)).unwrap();
    let table = db.tables.require_by_id(&TableId::new(1)).unwrap();
    assert!(
        table.column_exists("time"),
        "migrated table should have a timestamp column"
    );
}

#[test]
fn table_with_existing_timestamp_is_not_perturbed() {
    let mut t = table(1, "t");
    t.columns.repo.push(time_col(0));
    let mut db = database(1, "db", None);
    db.tables.repo.insert(TableId::new(1), t);
    let mut snap = empty_snapshot(1);
    snap.databases.repo.insert(DbId::new(1), db);

    let batch = synthesize_records(&snap);
    let add = AddColumns::decode(&batch.as_slice()[2].data).expect("decode AddColumns");
    let timestamps: Vec<_> = add
        .columns
        .iter()
        .filter(|c| matches!(c, WireColumnDef::Timestamp(_)))
        .collect();
    assert_eq!(timestamps.len(), 1, "no duplicate timestamp synthesized");
    let WireColumnDef::Timestamp(ts) = timestamps[0] else {
        unreachable!()
    };
    assert_eq!(ts.column_id, Some(0));
}

#[test]
fn soft_deleted_table_emits_soft_delete_with_recovered_deletion_timestamp() {
    let mut t = table(1, "metrics-20250511T143000");
    t.deleted = true;
    t.hard_delete_time = Some(1_700_000_000_000);
    t.hard_delete_scope = Some(V2DeletionScope::DataAndCatalog);
    t.columns.repo.push(time_col(0));

    let mut db = database(1, "db", None);
    db.tables.repo.insert(TableId::new(1), t);
    let mut snap = empty_snapshot(1);
    snap.databases.repo.insert(DbId::new(1), db);

    let batch = synthesize_records(&snap);
    let records = batch.as_slice();
    assert_eq!(records.len(), 4);
    assert_eq!(records[0].id(), record_ids::CREATE_DATABASE);
    assert_eq!(records[1].id(), record_ids::CREATE_TABLE);
    assert_eq!(records[2].id(), record_ids::ADD_COLUMNS);
    assert_eq!(records[3].id(), record_ids::SOFT_DELETE_TABLE);

    let create = CreateTable::decode(&records[1].data).expect("decode CreateTable");
    assert_eq!(create.table_name, "metrics");

    let soft = SoftDeleteTable::decode(&records[3].data).expect("decode SoftDeleteTable");
    assert!(soft.deletion_time_ns > 0);
    assert_eq!(soft.hard_deletion_time_ns, Some(1_700_000_000_000));
    assert_eq!(
        soft.hard_delete_scope,
        Some(WireDeletionScope::DataAndCatalog)
    );
}

#[test]
fn migrated_soft_deleted_table_apply_reproduces_renamed_name() {
    let renamed = "events-20240301T091500";
    let mut t = table(1, renamed);
    t.deleted = true;
    t.columns.repo.push(time_col(0));

    let mut db = database(1, "db", None);
    db.tables.repo.insert(TableId::new(1), t);
    let mut snap = empty_snapshot(1);
    snap.databases.repo.insert(DbId::new(1), db);

    let batch = synthesize_records(&snap);
    let mut catalog = InnerCatalog::new(Arc::from("test"), Uuid::nil());
    apply_records(
        batch.as_slice(),
        &mut catalog,
        CatalogSequenceNumber::new(1),
        &mut RestorePreload::empty(),
    )
    .expect("apply succeeds");

    let db = catalog
        .databases
        .require_by_id(&DbId::new(1))
        .expect("database present");
    let table = db
        .tables
        .require_by_id(&TableId::new(1))
        .expect("table present");
    assert!(table.deleted);
    assert_eq!(table.table_name.as_ref(), renamed);
}

#[test]
fn migrated_table_apply_reproduces_series_key_order() {
    let mut t = table(1, "metrics");
    t.columns.repo.push(time_col(0));
    t.columns.repo.push(tag_col(0, 1, "host"));
    t.columns.repo.push(tag_col(1, 2, "region"));
    t.columns.repo.push(tag_col(2, 3, "service"));
    // Set series-key order distinct from repo order.
    t.key = vec![TagId::new(2), TagId::new(0), TagId::new(1)];

    let mut db = database(1, "db", None);
    db.tables.repo.insert(TableId::new(1), t);
    let mut snap = empty_snapshot(1);
    snap.databases.repo.insert(DbId::new(1), db);

    let batch = synthesize_records(&snap);
    let mut catalog = InnerCatalog::new(Arc::from("test"), Uuid::nil());
    apply_records(
        batch.as_slice(),
        &mut catalog,
        CatalogSequenceNumber::new(1),
        &mut RestorePreload::empty(),
    )
    .expect("apply succeeds");

    let db = catalog.databases.require_by_id(&DbId::new(1)).unwrap();
    let table = db.tables.require_by_id(&TableId::new(1)).unwrap();
    let key_names: Vec<&str> = table
        .series_key_names()
        .iter()
        .map(|s| s.as_ref())
        .collect();
    assert_eq!(key_names, vec!["service", "host", "region"]);
}

#[test]
fn last_cache_synthesized_per_table_cache() {
    let mut t = table(1, "metrics");
    t.columns.repo.push(time_col(0));
    t.columns.repo.push(tag_col(0, 1, "host"));
    t.key.push(TagId::new(0));
    t.last_caches
        .repo
        .insert(LastCacheId::new(0), last_cache(1, 0, "lc"));

    let mut db = database(1, "db", None);
    db.tables.repo.insert(TableId::new(1), t);
    let mut snap = empty_snapshot(1);
    snap.databases.repo.insert(DbId::new(1), db);

    let batch = synthesize_records(&snap);
    let ids: Vec<_> = batch.as_slice().iter().map(|r| r.id()).collect();
    assert_eq!(
        ids,
        [
            record_ids::CREATE_DATABASE,
            record_ids::CREATE_TABLE,
            record_ids::ADD_COLUMNS,
            record_ids::CREATE_LAST_CACHE,
        ]
    );

    let cache = CreateLastCache::decode(&batch.as_slice()[3].data).expect("decode CreateLastCache");
    assert_eq!(cache.db_id, 1);
    assert_eq!(cache.table_id, 1);
    assert_eq!(cache.id, 0);
    assert_eq!(cache.name, "lc");
    assert_eq!(cache.count, 1);
    assert_eq!(cache.ttl_seconds, 3_600);
    assert_eq!(cache.value_columns, WireLvcDef::AllNonKeyColumns);
}

#[test]
fn last_cache_explicit_value_columns_carry_through() {
    let mut t = table(1, "t");
    t.columns.repo.push(time_col(0));
    let mut lc = last_cache(1, 0, "lc");
    lc.vals = Some(vec![ColumnIdentifier::Timestamp]);
    t.last_caches.repo.insert(LastCacheId::new(0), lc);

    let mut db = database(1, "db", None);
    db.tables.repo.insert(TableId::new(1), t);
    let mut snap = empty_snapshot(1);
    snap.databases.repo.insert(DbId::new(1), db);

    let batch = synthesize_records(&snap);
    let cache = CreateLastCache::decode(&batch.as_slice()[3].data).expect("decode CreateLastCache");
    assert_eq!(
        cache.value_columns,
        WireLvcDef::Explicit(vec![WireColumnId::Timestamp])
    );
}

#[test]
fn distinct_cache_synthesized_per_table_cache() {
    let mut t = table(1, "metrics");
    t.columns.repo.push(time_col(0));
    t.columns.repo.push(tag_col(0, 1, "host"));
    t.key.push(TagId::new(0));
    t.distinct_caches
        .repo
        .insert(DistinctCacheId::new(0), distinct_cache(1, 0, "dc"));

    let mut db = database(1, "db", None);
    db.tables.repo.insert(TableId::new(1), t);
    let mut snap = empty_snapshot(1);
    snap.databases.repo.insert(DbId::new(1), db);

    let batch = synthesize_records(&snap);
    let ids: Vec<_> = batch.as_slice().iter().map(|r| r.id()).collect();
    assert_eq!(
        ids,
        [
            record_ids::CREATE_DATABASE,
            record_ids::CREATE_TABLE,
            record_ids::ADD_COLUMNS,
            record_ids::CREATE_DISTINCT_CACHE,
        ]
    );

    let cache =
        CreateDistinctCache::decode(&batch.as_slice()[3].data).expect("decode CreateDistinctCache");
    assert_eq!(cache.db_id, 1);
    assert_eq!(cache.table_id, 1);
    assert_eq!(cache.cache_id, 0);
    assert_eq!(cache.cache_name, "dc");
    assert_eq!(cache.max_age_seconds, 86_400);
    assert_eq!(cache.source, WireCacheSource::User);
    assert_eq!(cache.lookback_seconds, None);
    assert_eq!(cache.refresh_interval_seconds, None);
}

#[test]
fn distinct_cache_auto_source_and_intervals_carry_through() {
    let mut t = table(1, "t");
    t.columns.repo.push(time_col(0));
    let mut dc = distinct_cache(1, 0, "dc");
    dc.source = CacheSource::Auto;
    dc.lookback_seconds = Some(3_600);
    dc.refresh_interval = Some(RefreshInterval::from_secs(60));
    t.distinct_caches.repo.insert(DistinctCacheId::new(0), dc);

    let mut db = database(1, "db", None);
    db.tables.repo.insert(TableId::new(1), t);
    let mut snap = empty_snapshot(1);
    snap.databases.repo.insert(DbId::new(1), db);

    let batch = synthesize_records(&snap);
    let cache =
        CreateDistinctCache::decode(&batch.as_slice()[3].data).expect("decode CreateDistinctCache");
    assert_eq!(cache.source, WireCacheSource::Auto);
    assert_eq!(cache.lookback_seconds, Some(3_600));
    assert_eq!(cache.refresh_interval_seconds, Some(60));
}

#[test]
fn caches_emitted_between_add_columns_and_soft_delete() {
    let mut t = table(1, "metrics-20250511T143000");
    t.deleted = true;
    t.columns.repo.push(time_col(0));
    t.columns.repo.push(tag_col(0, 1, "host"));
    t.key.push(TagId::new(0));
    t.last_caches
        .repo
        .insert(LastCacheId::new(0), last_cache(1, 0, "lc"));
    t.distinct_caches
        .repo
        .insert(DistinctCacheId::new(0), distinct_cache(1, 0, "dc"));

    let mut db = database(1, "db", None);
    db.tables.repo.insert(TableId::new(1), t);
    let mut snap = empty_snapshot(1);
    snap.databases.repo.insert(DbId::new(1), db);

    let batch = synthesize_records(&snap);
    let ids: Vec<_> = batch.as_slice().iter().map(|r| r.id()).collect();
    assert_eq!(
        ids,
        [
            record_ids::CREATE_DATABASE,
            record_ids::CREATE_TABLE,
            record_ids::ADD_COLUMNS,
            record_ids::CREATE_LAST_CACHE,
            record_ids::CREATE_DISTINCT_CACHE,
            record_ids::SOFT_DELETE_TABLE,
        ]
    );
}

#[test]
fn migrated_caches_apply_to_table() {
    let mut t = table(1, "metrics");
    t.columns.repo.push(time_col(0));
    t.columns.repo.push(tag_col(0, 1, "host"));
    t.key.push(TagId::new(0));
    t.last_caches
        .repo
        .insert(LastCacheId::new(0), last_cache(1, 0, "lc"));
    t.distinct_caches
        .repo
        .insert(DistinctCacheId::new(0), distinct_cache(1, 0, "dc"));

    let mut db = database(1, "db", None);
    db.tables.repo.insert(TableId::new(1), t);
    let mut snap = empty_snapshot(1);
    snap.databases.repo.insert(DbId::new(1), db);

    let batch = synthesize_records(&snap);
    let mut catalog = InnerCatalog::new(Arc::from("test"), Uuid::nil());
    apply_records(
        batch.as_slice(),
        &mut catalog,
        CatalogSequenceNumber::new(1),
        &mut RestorePreload::empty(),
    )
    .expect("apply succeeds");

    let db = catalog.databases.require_by_id(&DbId::new(1)).unwrap();
    let table = db.tables.require_by_id(&TableId::new(1)).unwrap();
    assert_eq!(table.last_caches.len(), 1);
    assert_eq!(table.distinct_caches.len(), 1);
}

#[test]
fn tables_emitted_before_triggers_and_soft_delete() {
    let mut t = table(1, "metrics");
    t.columns.repo.push(time_col(0));
    let mut db = database(1, "going-20250511T143000", None);
    db.deleted = true;
    db.tables.repo.insert(TableId::new(1), t);
    db.processing_engine_triggers
        .repo
        .insert(TriggerId::new(1), trigger(1, "trig", false));

    let mut snap = empty_snapshot(1);
    snap.databases.repo.insert(DbId::new(1), db);

    let batch = synthesize_records(&snap);
    let ids: Vec<_> = batch.as_slice().iter().map(|r| r.id()).collect();
    assert_eq!(
        ids,
        [
            record_ids::CREATE_DATABASE,
            record_ids::CREATE_TABLE,
            record_ids::ADD_COLUMNS,
            record_ids::CREATE_TRIGGER,
            record_ids::SOFT_DELETE_DATABASE,
        ]
    );
}

#[test]
fn admin_token_emits_create_admin_token() {
    let mut snap = empty_snapshot(1);
    snap.tokens
        .repo
        .insert(TokenId::from(1), admin_token(1, "admin"));

    let batch = synthesize_records(&snap);
    let records = batch.as_slice();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].id(), record_ids::CREATE_ADMIN_TOKEN);

    let token = CreateAdminToken::decode(&records[0].data).expect("decode CreateAdminToken");
    assert_eq!(token.token_id, 1);
    assert_eq!(token.name, "admin");
    assert_eq!(token.hash, vec![1, 2, 3]);
    assert_eq!(token.created_at, 1_700_000_000_000);
}

#[test]
fn token_expiry_i64_max_becomes_none() {
    // v2 encodes "never expires" as `i64::MAX`; the wire record uses `None`.
    let mut snap = empty_snapshot(1);
    snap.tokens
        .repo
        .insert(TokenId::from(1), admin_token(1, "admin"));

    let batch = synthesize_records(&snap);
    let token = CreateAdminToken::decode(&batch.as_slice()[0].data).expect("decode");
    assert_eq!(token.expiry, None);
}

#[test]
fn token_finite_expiry_passes_through() {
    let mut t = admin_token(1, "admin");
    t.expiry = 1_900_000_000_000;
    let mut snap = empty_snapshot(1);
    snap.tokens.repo.insert(TokenId::from(1), t);

    let batch = synthesize_records(&snap);
    let token = CreateAdminToken::decode(&batch.as_slice()[0].data).expect("decode");
    assert_eq!(token.expiry, Some(1_900_000_000_000));
}

#[test]
fn resource_scoped_token_expiry_i64_max_becomes_none() {
    let permission = PermissionSnapshot {
        resource_type: ResourceTypeSnapshot::Database,
        resource_identifier: ResourceIdentifierSnapshot::Database(vec![DbId::new(7)]),
        actions: ActionsSnapshot::Database(DatabaseActionsSnapshot(0b1)),
        resource_names: None,
    };
    let mut snap = empty_snapshot(1);
    snap.tokens.repo.insert(
        TokenId::from(2),
        resource_scoped_token(2, "scoped", vec![permission]),
    );

    let batch = synthesize_records(&snap);
    let token = CreateResourceScopedToken::decode(&batch.as_slice()[0].data).expect("decode");
    assert_eq!(token.expiry, None);
}

#[test]
fn resource_scoped_token_emits_create_resource_scoped_token() {
    let permission = PermissionSnapshot {
        resource_type: ResourceTypeSnapshot::Database,
        resource_identifier: ResourceIdentifierSnapshot::Database(vec![DbId::new(7)]),
        actions: ActionsSnapshot::Database(DatabaseActionsSnapshot(0b11)),
        resource_names: None,
    };
    let mut snap = empty_snapshot(1);
    snap.tokens.repo.insert(
        TokenId::from(2),
        resource_scoped_token(2, "scoped", vec![permission]),
    );

    let batch = synthesize_records(&snap);
    let records = batch.as_slice();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].id(), record_ids::CREATE_RESOURCE_SCOPED_TOKEN);

    let token = CreateResourceScopedToken::decode(&records[0].data)
        .expect("decode CreateResourceScopedToken");
    assert_eq!(token.token_id, 2);
    assert_eq!(token.permissions.len(), 1);
    let perm = &token.permissions[0];
    assert_eq!(perm.resource_type, WireResourceType::Database);
    assert_eq!(
        perm.resource_identifier,
        WireResourceIdent::Database(vec![7])
    );
    assert_eq!(perm.actions, WireActions::Database(0b11));
    assert!(perm.resource_names.is_empty());
}

#[test]
fn token_carries_optional_metadata_fields() {
    let mut t = admin_token(3, "with_meta");
    t.description = Some("desc".to_string());
    t.created_by = Some(TokenId::from(1));
    t.updated_by = Some(TokenId::from(2));
    t.updated_at = Some(1_700_000_001_000);
    let mut snap = empty_snapshot(1);
    snap.tokens.repo.insert(TokenId::from(3), t);

    let batch = synthesize_records(&snap);
    let token = CreateAdminToken::decode(&batch.as_slice()[0].data).expect("decode");
    assert_eq!(token.description.as_deref(), Some("desc"));
    assert_eq!(token.created_by, Some(1));
    assert_eq!(token.updated_by, Some(2));
    assert_eq!(token.updated_at, Some(1_700_000_001_000));
}

#[test]
fn resource_scoped_token_preserves_resource_names() {
    let mut names = IndexMap::new();
    names.insert(
        "7".to_string(),
        ResourceMetadata {
            name: "gone".to_string(),
            deleted: true,
        },
    );
    let permission = PermissionSnapshot {
        resource_type: ResourceTypeSnapshot::Database,
        resource_identifier: ResourceIdentifierSnapshot::Database(vec![DbId::new(7)]),
        actions: ActionsSnapshot::Database(DatabaseActionsSnapshot(0b1)),
        resource_names: Some(names),
    };
    let mut snap = empty_snapshot(1);
    snap.tokens.repo.insert(
        TokenId::from(4),
        resource_scoped_token(4, "with_names", vec![permission]),
    );

    let batch = synthesize_records(&snap);
    let token = CreateResourceScopedToken::decode(&batch.as_slice()[0].data).expect("decode");
    let entries = &token.permissions[0].resource_names;
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].resource_id, "7");
    assert_eq!(entries[0].name, "gone");
    assert!(entries[0].deleted);
}

#[test]
fn system_resource_identifier_and_actions_preserved() {
    let permission = PermissionSnapshot {
        resource_type: ResourceTypeSnapshot::System,
        resource_identifier: ResourceIdentifierSnapshot::System(vec![
            SystemResourceIdentifier::from(SystemResourceIdentifier::HEALTH),
            SystemResourceIdentifier::from(SystemResourceIdentifier::METRICS),
        ]),
        actions: ActionsSnapshot::System(SystemActionsSnapshot(SystemActions::READ)),
        resource_names: None,
    };
    let mut snap = empty_snapshot(1);
    snap.tokens.repo.insert(
        TokenId::from(5),
        resource_scoped_token(5, "sys", vec![permission]),
    );

    let batch = synthesize_records(&snap);
    let token = CreateResourceScopedToken::decode(&batch.as_slice()[0].data).expect("decode");
    assert_eq!(
        token.permissions[0].resource_identifier,
        WireResourceIdent::System(vec![
            SystemResourceIdentifier::HEALTH,
            SystemResourceIdentifier::METRICS,
        ])
    );
    assert_eq!(
        token.permissions[0].actions,
        WireActions::System(SystemActions::READ)
    );
}

#[test]
fn migrated_admin_token_apply_records() {
    use crate::catalog::versions::v3::inner::InnerCatalog;
    use crate::format::apply::{RestorePreload, apply_records};

    let mut snap = empty_snapshot(1);
    snap.tokens
        .repo
        .insert(TokenId::from(1), admin_token(1, "admin"));

    let batch = synthesize_records(&snap);
    let mut catalog = InnerCatalog::new(Arc::from("test"), Uuid::nil());
    apply_records(
        batch.as_slice(),
        &mut catalog,
        CatalogSequenceNumber::new(1),
        &mut RestorePreload::empty(),
    )
    .expect("apply succeeds");

    let token = catalog
        .tokens
        .repo()
        .get_by_id(&TokenId::from(1))
        .expect("token present");
    assert_eq!(token.name.as_ref(), "admin");
}

#[test]
fn tokens_emitted_after_databases() {
    let mut snap = empty_snapshot(1);
    snap.databases
        .repo
        .insert(DbId::new(1), database(1, "db", None));
    snap.tokens
        .repo
        .insert(TokenId::from(1), admin_token(1, "admin"));

    let batch = synthesize_records(&snap);
    let ids: Vec<_> = batch.as_slice().iter().map(|r| r.id()).collect();
    assert_eq!(
        ids,
        [record_ids::CREATE_DATABASE, record_ids::CREATE_ADMIN_TOKEN,]
    );
}

#[test]
fn no_set_next_id_when_repo_next_matches_max_plus_one() {
    let mut snap = empty_snapshot(1);
    snap.databases
        .repo
        .insert(DbId::new(1), database(1, "a", None));
    snap.databases.next_id = DbId::new(2);

    let batch = synthesize_records(&snap);
    let has_set_next = batch
        .as_slice()
        .iter()
        .any(|r| r.id() == record_ids::SET_NEXT_ID);
    assert!(!has_set_next);
}

#[test]
fn set_next_id_for_databases_when_top_db_was_hard_deleted() {
    // snapshot.next_id sits ahead of max(present)+1 — typical after the
    // highest-id db was hard-deleted.
    let mut snap = empty_snapshot(1);
    snap.databases
        .repo
        .insert(DbId::new(1), database(1, "a", None));
    snap.databases.next_id = DbId::new(5);

    let batch = synthesize_records(&snap);
    let records = batch.as_slice();
    let set_next = records
        .iter()
        .find(|r| r.id() == record_ids::SET_NEXT_ID)
        .expect("SetNextId emitted");
    let s = SetNextId::decode(&set_next.data).expect("decode SetNextId");
    assert!(matches!(s.scope, NextIdScope::Databases));
    assert_eq!(s.id, 5);
}

#[test]
fn set_next_id_for_tokens_when_top_token_was_hard_deleted() {
    let mut snap = empty_snapshot(1);
    snap.tokens
        .repo
        .insert(TokenId::from(1), admin_token(1, "t"));
    snap.tokens.next_id = TokenId::from(10);

    let batch = synthesize_records(&snap);
    let s = decode_set_next_id(&batch, NextIdScope::Tokens).expect("Tokens SetNextId");
    assert_eq!(s.id, 10);
}

#[test]
fn set_next_id_for_empty_repo_with_advanced_counter() {
    // Repo is empty (every member hard-deleted) but the counter has moved
    // past zero — the SetNextId record carries the snapshot's counter.
    let mut snap = empty_snapshot(1);
    snap.databases.next_id = DbId::new(7);

    let batch = synthesize_records(&snap);
    let s = decode_set_next_id(&batch, NextIdScope::Databases).expect("SetNextId emitted");
    assert_eq!(s.id, 7);
}

#[test]
fn set_next_id_for_tables_within_database() {
    let mut t = table(1, "t");
    t.columns.repo.push(time_col(0));
    let mut db = database(1, "db", None);
    db.tables.repo.insert(TableId::new(1), t);
    db.tables.next_id = TableId::new(5);

    let mut snap = empty_snapshot(1);
    snap.databases.repo.insert(DbId::new(1), db);

    let batch = synthesize_records(&snap);
    let s = decode_set_next_id(&batch, NextIdScope::Tables { database_id: 1 })
        .expect("Tables SetNextId");
    assert_eq!(s.id, 5);
}

#[test]
fn set_next_id_for_triggers_within_database() {
    let mut db = database(1, "db", None);
    db.processing_engine_triggers
        .repo
        .insert(TriggerId::new(0), trigger(0, "trig", false));
    db.processing_engine_triggers.next_id = TriggerId::new(4);

    let mut snap = empty_snapshot(1);
    snap.databases.repo.insert(DbId::new(1), db);

    let batch = synthesize_records(&snap);
    let s = decode_set_next_id(&batch, NextIdScope::Triggers { database_id: 1 })
        .expect("Triggers SetNextId");
    assert_eq!(s.id, 4);
}

#[test]
fn set_next_id_for_last_caches_within_table() {
    let mut t = table(1, "t");
    t.columns.repo.push(time_col(0));
    t.last_caches
        .repo
        .insert(LastCacheId::new(0), last_cache(1, 0, "lc"));
    t.last_caches.next_id = LastCacheId::new(4);

    let mut db = database(1, "db", None);
    db.tables.repo.insert(TableId::new(1), t);
    let mut snap = empty_snapshot(1);
    snap.databases.repo.insert(DbId::new(1), db);

    let batch = synthesize_records(&snap);
    let s = decode_set_next_id(
        &batch,
        NextIdScope::LastCaches {
            database_id: 1,
            table_id: 1,
        },
    )
    .expect("LastCaches SetNextId");
    assert_eq!(s.id, 4);
}

#[test]
fn set_next_id_for_distinct_caches_within_table() {
    let mut t = table(1, "t");
    t.columns.repo.push(time_col(0));
    t.distinct_caches
        .repo
        .insert(DistinctCacheId::new(0), distinct_cache(1, 0, "dc"));
    t.distinct_caches.next_id = DistinctCacheId::new(2);

    let mut db = database(1, "db", None);
    db.tables.repo.insert(TableId::new(1), t);
    let mut snap = empty_snapshot(1);
    snap.databases.repo.insert(DbId::new(1), db);

    let batch = synthesize_records(&snap);
    let s = decode_set_next_id(
        &batch,
        NextIdScope::DistinctCaches {
            database_id: 1,
            table_id: 1,
        },
    )
    .expect("DistinctCaches SetNextId");
    assert_eq!(s.id, 2);
}

#[test]
fn migrated_set_next_id_apply_restores_counters() {
    use crate::catalog::versions::v3::inner::InnerCatalog;
    use crate::format::apply::{RestorePreload, apply_records};

    // Set up one of each deletable v2 resource with its parent repo's
    // `next_id` advanced past `max(present)+1` — the gap that arises after
    // the top-id member is hard-deleted.
    let mut t = table(1, "t");
    t.columns.repo.push(time_col(0));
    t.last_caches
        .repo
        .insert(LastCacheId::new(0), last_cache(1, 0, "lc"));
    t.last_caches.next_id = LastCacheId::new(9);
    t.distinct_caches
        .repo
        .insert(DistinctCacheId::new(0), distinct_cache(1, 0, "dc"));
    t.distinct_caches.next_id = DistinctCacheId::new(11);

    let mut db = database(1, "a", None);
    db.tables.repo.insert(TableId::new(1), t);
    db.tables.next_id = TableId::new(7);
    db.processing_engine_triggers
        .repo
        .insert(TriggerId::new(0), trigger(0, "trig", false));
    db.processing_engine_triggers.next_id = TriggerId::new(13);

    let mut snap = empty_snapshot(1);
    snap.databases.repo.insert(DbId::new(1), db);
    snap.databases.next_id = DbId::new(5);
    snap.tokens
        .repo
        .insert(TokenId::from(1), admin_token(1, "admin"));
    snap.tokens.next_id = TokenId::from(15);

    let batch = synthesize_records(&snap);
    let mut catalog = InnerCatalog::new(Arc::from("test"), Uuid::nil());
    apply_records(
        batch.as_slice(),
        &mut catalog,
        CatalogSequenceNumber::new(1),
        &mut RestorePreload::empty(),
    )
    .expect("apply succeeds");

    assert_eq!(catalog.databases.next_id().get(), 5);
    assert_eq!(catalog.tokens.repo().next_id().get(), 15);
    let db = catalog
        .databases
        .require_by_id(&DbId::new(1))
        .expect("database present");
    assert_eq!(db.tables.next_id().get(), 7);
    assert_eq!(db.processing_engine_triggers.next_id().get(), 13);
    let table = db
        .tables
        .require_by_id(&TableId::new(1))
        .expect("table present");
    assert_eq!(table.last_caches.next_id().get(), 9);
    assert_eq!(table.distinct_caches.next_id().get(), 11);
}

fn decode_set_next_id(
    batch: &crate::format::RecordBatch,
    expected_scope: NextIdScope,
) -> Option<SetNextId> {
    batch
        .as_slice()
        .iter()
        .filter(|r| r.id() == record_ids::SET_NEXT_ID)
        .filter_map(|r| SetNextId::decode(&r.data).ok())
        .find(|s| std::mem::discriminant(&s.scope) == std::mem::discriminant(&expected_scope))
}

#[test]
fn triggers_emitted_between_create_database_and_soft_delete() {
    let mut db = database(43, "going-20250511T143000", None);
    db.deleted = true;
    db.processing_engine_triggers
        .repo
        .insert(TriggerId::new(1), trigger(1, "t1", false));

    let mut snap = empty_snapshot(1);
    snap.databases.repo.insert(DbId::new(43), db);

    let batch = synthesize_records(&snap);
    let ids: Vec<_> = batch.as_slice().iter().map(|r| r.id()).collect();
    assert_eq!(
        ids,
        [
            record_ids::CREATE_DATABASE,
            record_ids::CREATE_TRIGGER,
            record_ids::SOFT_DELETE_DATABASE,
        ]
    );
}

// ---------------------------------------------------------------------------
// check_and_migrate_v2_to_v3 — orchestrator
// ---------------------------------------------------------------------------

mod runner {
    use crate::catalog::migrations::v3::{MigrationResult, check_and_migrate_v2_to_v3};
    use crate::catalog::versions::v2::InnerCatalog as V2InnerCatalog;
    use crate::catalog::versions::v2::Snapshot as V2Snapshot;
    use crate::catalog::versions::v3::schema::storage::StorageMode as V3StorageMode;
    use crate::log::versions::v4::StorageMode as V2StorageMode;
    use crate::object_store::PersistCatalogResult;
    use crate::object_store::versions as ostore;
    use crate::serialize::versions as ser;
    use crate::snapshot::versions::v4::ColumnSetSnapshot;
    use influxdb3_id::ColumnId;
    use object_store::ObjectStore;
    use object_store::memory::InMemory;
    use std::sync::Arc;
    use uuid::Uuid;

    fn shared_store() -> Arc<dyn ObjectStore> {
        Arc::new(InMemory::new())
    }

    async fn seed_v2_catalog(
        prefix: Arc<str>,
        store: Arc<dyn ObjectStore>,
        uuid: Uuid,
    ) -> V2InnerCatalog {
        let v2_inner = V2InnerCatalog::new(Arc::clone(&prefix), uuid);
        let v2_snap = v2_inner.snapshot();
        let v2_store = ostore::v2::ObjectStoreCatalog::new(
            Arc::clone(&prefix),
            u64::MAX,
            store,
            V2StorageMode::default(),
        );
        let result = v2_store.persist_catalog_checkpoint(&v2_snap).await.unwrap();
        assert!(matches!(result, PersistCatalogResult::Success));
        v2_inner
    }

    #[tokio::test]
    async fn nothing_to_migrate_on_empty_store() {
        let prefix: Arc<str> = Arc::from("empty");
        let store = shared_store();
        let result = check_and_migrate_v2_to_v3(Arc::clone(&prefix), Arc::clone(&store))
            .await
            .unwrap();
        assert_eq!(result, MigrationResult::NothingToMigrate);
    }

    #[tokio::test]
    async fn already_migrated_when_v3_snapshot_present() {
        let prefix: Arc<str> = Arc::from("pre-migrated");
        let store = shared_store();

        // Seed both v2 and v3 — having a v3 snapshot wins regardless of v2.
        seed_v2_catalog(Arc::clone(&prefix), Arc::clone(&store), Uuid::nil()).await;
        let v3_store = ostore::v3::ObjectStoreCatalog::new(
            Arc::clone(&prefix),
            Arc::clone(&store),
            V3StorageMode::default(),
        );
        let mut v3_inner = crate::catalog::versions::v3::inner::InnerCatalog::new(
            Arc::clone(&prefix),
            Uuid::nil(),
        );
        v3_store
            .initialize_snapshot(v3_inner.create_snapshot())
            .await
            .unwrap();

        let result = check_and_migrate_v2_to_v3(prefix, store).await.unwrap();
        assert_eq!(result, MigrationResult::AlreadyMigrated);
    }

    #[tokio::test]
    async fn clean_migration_writes_v3_snapshot_and_fences_v2() {
        let prefix: Arc<str> = Arc::from("clean");
        let store = shared_store();
        let uuid = Uuid::new_v4();
        seed_v2_catalog(Arc::clone(&prefix), Arc::clone(&store), uuid).await;

        let result = check_and_migrate_v2_to_v3(Arc::clone(&prefix), Arc::clone(&store))
            .await
            .unwrap();
        assert_eq!(result, MigrationResult::Migrated);

        // The v3 snapshot is now present and carries the v2 catalog UUID.
        let v3_store = ostore::v3::ObjectStoreCatalog::new(
            Arc::clone(&prefix),
            Arc::clone(&store),
            V3StorageMode::default(),
        );
        let (v3_snap, _size_bytes) = v3_store
            .load_snapshot()
            .await
            .unwrap()
            .expect("v3 snapshot");
        assert_eq!(v3_snap.header.catalog_uuid, uuid.as_u128());

        // Re-loading the v2 catalog observes the upgrade log.
        let v2_store = ostore::v2::ObjectStoreCatalog::new(
            Arc::clone(&prefix),
            u64::MAX,
            Arc::clone(&store),
            V2StorageMode::default(),
        );
        let v2_reloaded = ser::v2::load_catalog(Arc::clone(&prefix), &v2_store)
            .await
            .unwrap()
            .expect("v2 catalog");
        assert!(v2_reloaded.has_upgraded);
    }

    #[tokio::test]
    async fn rerun_after_success_returns_already_migrated() {
        let prefix: Arc<str> = Arc::from("rerun");
        let store = shared_store();
        seed_v2_catalog(Arc::clone(&prefix), Arc::clone(&store), Uuid::new_v4()).await;

        let first = check_and_migrate_v2_to_v3(Arc::clone(&prefix), Arc::clone(&store))
            .await
            .unwrap();
        assert_eq!(first, MigrationResult::Migrated);

        let second = check_and_migrate_v2_to_v3(Arc::clone(&prefix), Arc::clone(&store))
            .await
            .unwrap();
        assert_eq!(second, MigrationResult::AlreadyMigrated);
    }

    #[tokio::test]
    async fn recovery_completes_after_partial_failure() {
        // Simulate a partial failure: the upgrade log was stamped but
        // the v3 snapshot write didn't complete. A subsequent run must
        // observe `has_upgraded` on reload and complete the snapshot
        // write idempotently.
        let prefix: Arc<str> = Arc::from("recovery");
        let store = shared_store();
        let v2_inner =
            seed_v2_catalog(Arc::clone(&prefix), Arc::clone(&store), Uuid::new_v4()).await;

        // Stamp the upgrade log without writing the v3 snapshot.
        let v2_store = ostore::v2::ObjectStoreCatalog::new(
            Arc::clone(&prefix),
            u64::MAX,
            Arc::clone(&store),
            V2StorageMode::default(),
        );
        v2_store
            .persist_upgrade_log(v2_inner.sequence_number().next())
            .await
            .unwrap();

        // Recovery pass: should observe has_upgraded on v2 reload, skip
        // the upgrade-log write, and land the v3 snapshot.
        let result = check_and_migrate_v2_to_v3(Arc::clone(&prefix), Arc::clone(&store))
            .await
            .unwrap();
        assert_eq!(result, MigrationResult::Migrated);

        let v3_store = ostore::v3::ObjectStoreCatalog::new(
            Arc::clone(&prefix),
            Arc::clone(&store),
            V3StorageMode::default(),
        );
        assert!(v3_store.load_snapshot().await.unwrap().is_some());
    }

    #[tokio::test]
    async fn migration_round_trips_database_and_table_into_v3() {
        // End-to-end: a v2 catalog with one database and one table
        // round-trips through the migration into a v3 InnerCatalog with
        // the same logical state.
        use crate::catalog::CatalogSequenceNumber;
        use crate::catalog::versions::v3::inner::InnerCatalog as V3InnerCatalog;
        use crate::format::apply::{RestorePreload, apply_catalog_file};
        use crate::format::reader::CatalogFile;
        use crate::object_store::versions as ostore;
        use influxdb3_id::{DbId, TableId, TagId};
        use std::io::Cursor;

        let prefix: Arc<str> = Arc::from("populated");
        let store = shared_store();
        let uuid = Uuid::new_v4();

        let mut snap = super::empty_snapshot(5);
        snap.catalog_id = Arc::clone(&prefix);
        snap.catalog_uuid = uuid;
        let mut db = super::database(10, "metrics", None);
        let mut table = super::table(100, "cpu");
        table.columns = ColumnSetSnapshot {
            repo: vec![super::time_col(0), super::tag_col(0, 1, "host")],
            next_id: ColumnId::new(2),
        };
        table.key = vec![TagId::new(0)];
        db.tables.repo.insert(TableId::new(100), table);
        db.tables.next_id = TableId::new(101);
        snap.databases.repo.insert(DbId::new(10), db);
        snap.databases.next_id = DbId::new(11);

        // Persist v2 directly via the snapshot path.
        let v2_store = ostore::v2::ObjectStoreCatalog::new(
            Arc::clone(&prefix),
            u64::MAX,
            Arc::clone(&store),
            V2StorageMode::default(),
        );
        v2_store.persist_catalog_checkpoint(&snap).await.unwrap();

        // Migrate.
        let result = check_and_migrate_v2_to_v3(Arc::clone(&prefix), Arc::clone(&store))
            .await
            .unwrap();
        assert_eq!(result, MigrationResult::Migrated);

        // Read back the v3 snapshot, apply it to a fresh InnerCatalog,
        // and verify the database + table replayed.
        let path = ostore::v3::CatalogFilePath::snapshot(&prefix);
        let bytes = store.get(&path).await.unwrap().bytes().await.unwrap();
        let mut cursor = Cursor::new(bytes.as_ref());
        let file = CatalogFile::read_from(&mut cursor).unwrap();

        let mut v3_inner = V3InnerCatalog::new(Arc::clone(&prefix), uuid);
        apply_catalog_file(&file, &mut v3_inner, &mut RestorePreload::empty()).unwrap();

        assert_eq!(v3_inner.sequence_number(), CatalogSequenceNumber::new(5));
        assert_eq!(v3_inner.catalog_uuid, uuid);
        let db_replayed = v3_inner.databases.get_by_name("metrics").expect("db");
        assert_eq!(db_replayed.id, DbId::new(10));
        let table_replayed = db_replayed.tables.get_by_name("cpu").expect("table");
        assert_eq!(table_replayed.table_id, TableId::new(100));
    }

    #[tokio::test]
    async fn migration_is_deterministic_across_distinct_stores() {
        // Two independent runs against equivalent v2 inputs must produce
        // byte-identical v3 snapshots — the synthesis is map-iteration-
        // deterministic, and apply order is insertion order.
        async fn migrate_and_load_snapshot(uuid: Uuid) -> bytes::Bytes {
            let prefix: Arc<str> = Arc::from("det");
            let store = shared_store();
            seed_v2_catalog(Arc::clone(&prefix), Arc::clone(&store), uuid).await;
            check_and_migrate_v2_to_v3(Arc::clone(&prefix), Arc::clone(&store))
                .await
                .unwrap();
            let path = ostore::v3::CatalogFilePath::snapshot(&prefix);
            store.get(&path).await.unwrap().bytes().await.unwrap()
        }

        let uuid = Uuid::from_u128(0x1234_5678_90AB_CDEF_FEDC_BA09_8765_4321);
        let snap_a = migrate_and_load_snapshot(uuid).await;
        let snap_b = migrate_and_load_snapshot(uuid).await;
        assert_eq!(snap_a, snap_b);
    }
}
