use std::{sync::Arc, time::Duration};

use influxdb3_authz::permissions::{
    AUTHZ_CREATE_DB_ACTION, AUTHZ_DB_RESOURCE_TYPE, AUTHZ_WILDCARD, AUTHZ_WRITE_DB_ACTION,
};
use influxdb3_id::{
    ColumnId, DbId, DistinctCacheId, LastCacheId, NodeId, TableId, TokenId, TriggerId,
};
use uuid::Uuid;

use crate::{
    catalog::CatalogSequenceNumber,
    catalog::versions::v1::log::{self},
    catalog::versions::v1::snapshot::{self},
    log::versions::{
        v1::{
            AddFieldsLog, CreateDatabaseLog, CreateTableLog, DatabaseBatch, DatabaseCatalogOp,
            DeleteDistinctCacheLog, DeleteLastCacheLog, DeleteTriggerLog, DistinctCacheDefinition,
            FieldDefinition, LastCacheDefinition, LastCacheSize, LastCacheTtl, MaxAge,
            MaxCardinality, NodeBatch, NodeCatalogOp, NodeMode, OrderedCatalogBatch,
            RegisterNodeLog, SoftDeleteDatabaseLog, SoftDeleteTableLog, TriggerDefinition,
            TriggerIdentifier, TriggerSettings,
        },
        v2, v3,
    },
    serialize::VersionedFileType,
    snapshot::versions::v1::{CatalogSnapshot, test_util::Generate},
};

use super::{
    serialize_catalog_file, verify_and_deserialize_catalog_checkpoint_file,
    verify_and_deserialize_catalog_file,
};

/// Test that uses the main `verify_and_deserialize_catalog_file` method which deseriales a
/// versioned catalog file into the latest version, to test round-trip serialize/deserialize
/// v1 catalog log files.
///
/// This just manually constructs a big catalog batch with all the different log types.
#[test]
fn test_deserialize_catalog_file_from_v1() {
    // test a node log file:
    verify_and_deserialize_catalog_file(
        serialize_catalog_file(&OrderedCatalogBatch {
            catalog_batch: crate::log::versions::v1::CatalogBatch::Node(NodeBatch {
                time_ns: 0,
                node_catalog_id: NodeId::new(0),
                node_id: "test-node".into(),
                ops: vec![NodeCatalogOp::RegisterNode(RegisterNodeLog {
                    node_id: "test-node".into(),
                    instance_id: "uuid".into(),
                    registered_time_ns: 0,
                    core_count: 2,
                    mode: vec![NodeMode::Core],
                })],
            }),
            sequence_number: CatalogSequenceNumber::new(0),
        })
        .expect("must be able to serialize"),
    )
    .expect("deserialize from v1");

    // test a database log file:
    let log_v1 = OrderedCatalogBatch {
        catalog_batch: crate::log::versions::v1::CatalogBatch::Database(DatabaseBatch {
            time_ns: 0,
            database_id: DbId::new(0),
            database_name: "test-db".into(),
            ops: vec![
                DatabaseCatalogOp::CreateDatabase(CreateDatabaseLog {
                    database_id: DbId::new(0),
                    database_name: "test-db".into(),
                }),
                DatabaseCatalogOp::SoftDeleteDatabase(SoftDeleteDatabaseLog {
                    database_id: DbId::new(0),
                    database_name: "test-db".into(),
                    deletion_time: 0,
                }),
                DatabaseCatalogOp::CreateTable(CreateTableLog {
                    database_id: DbId::new(0),
                    database_name: "test-db".into(),
                    table_name: "test-table".into(),
                    table_id: TableId::new(0),
                    field_definitions: vec![
                        FieldDefinition {
                            name: "string".into(),
                            id: ColumnId::new(0),
                            data_type: crate::log::versions::v1::FieldDataType::String,
                        },
                        FieldDefinition {
                            name: "int".into(),
                            id: ColumnId::new(1),
                            data_type: crate::log::versions::v1::FieldDataType::Integer,
                        },
                        FieldDefinition {
                            name: "uint".into(),
                            id: ColumnId::new(2),
                            data_type: crate::log::versions::v1::FieldDataType::UInteger,
                        },
                        FieldDefinition {
                            name: "float".into(),
                            id: ColumnId::new(3),
                            data_type: crate::log::versions::v1::FieldDataType::Float,
                        },
                        FieldDefinition {
                            name: "bool".into(),
                            id: ColumnId::new(4),
                            data_type: crate::log::versions::v1::FieldDataType::Boolean,
                        },
                        FieldDefinition {
                            name: "tag".into(),
                            id: ColumnId::new(5),
                            data_type: crate::log::versions::v1::FieldDataType::Tag,
                        },
                        FieldDefinition {
                            name: "time".into(),
                            id: ColumnId::new(6),
                            data_type: crate::log::versions::v1::FieldDataType::Timestamp,
                        },
                    ],
                    key: vec![ColumnId::new(5)],
                }),
                DatabaseCatalogOp::SoftDeleteTable(SoftDeleteTableLog {
                    database_id: DbId::new(0),
                    database_name: "test-db".into(),
                    table_id: TableId::new(0),
                    table_name: "test-table".into(),
                    deletion_time: 10,
                }),
                DatabaseCatalogOp::AddFields(AddFieldsLog {
                    database_name: "test-db".into(),
                    database_id: DbId::new(0),
                    table_name: "test-table".into(),
                    table_id: TableId::new(0),
                    field_definitions: vec![
                        FieldDefinition {
                            name: "string".into(),
                            id: ColumnId::new(0),
                            data_type: crate::log::versions::v1::FieldDataType::String,
                        },
                        FieldDefinition {
                            name: "int".into(),
                            id: ColumnId::new(1),
                            data_type: crate::log::versions::v1::FieldDataType::Integer,
                        },
                        FieldDefinition {
                            name: "uint".into(),
                            id: ColumnId::new(2),
                            data_type: crate::log::versions::v1::FieldDataType::UInteger,
                        },
                        FieldDefinition {
                            name: "float".into(),
                            id: ColumnId::new(3),
                            data_type: crate::log::versions::v1::FieldDataType::Float,
                        },
                        FieldDefinition {
                            name: "bool".into(),
                            id: ColumnId::new(4),
                            data_type: crate::log::versions::v1::FieldDataType::Boolean,
                        },
                        FieldDefinition {
                            name: "tag".into(),
                            id: ColumnId::new(5),
                            data_type: crate::log::versions::v1::FieldDataType::Tag,
                        },
                        FieldDefinition {
                            name: "time".into(),
                            id: ColumnId::new(6),
                            data_type: crate::log::versions::v1::FieldDataType::Timestamp,
                        },
                    ],
                }),
                DatabaseCatalogOp::CreateDistinctCache(DistinctCacheDefinition {
                    table_id: TableId::new(0),
                    table_name: "test-table".into(),
                    cache_id: DistinctCacheId::new(0),
                    cache_name: "test-distinct-cache".into(),
                    column_ids: vec![ColumnId::new(0), ColumnId::new(1)],
                    max_cardinality: MaxCardinality::default(),
                    max_age_seconds: MaxAge::default(),
                    node_spec: crate::log::versions::v1::NodeSpec::All,
                }),
                DatabaseCatalogOp::DeleteDistinctCache(DeleteDistinctCacheLog {
                    table_id: TableId::new(0),
                    table_name: "test-table".into(),
                    cache_id: DistinctCacheId::new(0),
                    cache_name: "test-distinct-cache".into(),
                }),
                DatabaseCatalogOp::CreateLastCache(LastCacheDefinition {
                    table_id: TableId::new(0),
                    table: "test-table".into(),
                    id: LastCacheId::new(0),
                    name: "test-last-cache".into(),
                    key_columns: vec![ColumnId::new(0), ColumnId::new(1)],
                    value_columns:
                        crate::log::versions::v1::LastCacheValueColumnsDef::AllNonKeyColumns,
                    count: LastCacheSize::default(),
                    ttl: LastCacheTtl::default(),
                    node_spec: crate::log::versions::v1::NodeSpec::Nodes(vec![
                        NodeId::new(0),
                        NodeId::new(1),
                    ]),
                }),
                DatabaseCatalogOp::CreateLastCache(LastCacheDefinition {
                    table_id: TableId::new(0),
                    table: "test-table".into(),
                    id: LastCacheId::new(0),
                    name: "test-last-cache".into(),
                    key_columns: vec![ColumnId::new(0), ColumnId::new(1)],
                    value_columns:
                        crate::log::versions::v1::LastCacheValueColumnsDef::Explicit {
                            columns: vec![ColumnId::new(2), ColumnId::new(3)],
                        },
                    count: LastCacheSize::default(),
                    ttl: LastCacheTtl::default(),
                    node_spec: crate::log::versions::v1::NodeSpec::All,
                }),
                DatabaseCatalogOp::DeleteLastCache(DeleteLastCacheLog {
                    table_id: TableId::new(0),
                    table_name: "test-table".into(),
                    id: LastCacheId::new(0),
                    name: "test-last-cache".into(),
                }),
                DatabaseCatalogOp::CreateTrigger(TriggerDefinition {
                    trigger_id: TriggerId::new(0),
                    trigger_name: "test-trigger".into(),
                    plugin_filename: "plugin.py".into(),
                    database_name: "test-db".into(),
                    node_spec: crate::log::versions::v1::NodeSpec::All,
                    trigger:
                    crate::log::versions::v1::TriggerSpecificationDefinition::AllTablesWalWrite,
                    trigger_settings: TriggerSettings {
                        run_async: true,
                        error_behavior: crate::log::versions::v1::ErrorBehavior::Retry,
                    },
                    trigger_arguments: Some(
                        [(String::from("k"), String::from("v"))]
                            .into_iter()
                            .collect(),
                    ),
                    disabled: false,
                }),
                DatabaseCatalogOp::CreateTrigger(TriggerDefinition {
                    trigger_id: TriggerId::new(0),
                    trigger_name: "test-trigger".into(),
                    plugin_filename: "plugin.py".into(),
                    database_name: "test-db".into(),
                    node_spec: crate::log::versions::v1::NodeSpec::Nodes(vec![
                        NodeId::new(0),
                        NodeId::new(1),
                    ]),
                    trigger:
                    crate::log::versions::v1::TriggerSpecificationDefinition::SingleTableWalWrite {
                            table_name: "test-table".into(),
                        },
                    trigger_settings: TriggerSettings {
                        run_async: true,
                        error_behavior: crate::log::versions::v1::ErrorBehavior::Log,
                    },
                    trigger_arguments: Some(
                        [(String::from("k"), String::from("v"))]
                            .into_iter()
                            .collect(),
                    ),
                    disabled: false,
                }),
                DatabaseCatalogOp::CreateTrigger(TriggerDefinition {
                    trigger_id: TriggerId::new(0),
                    trigger_name: "test-trigger".into(),
                    plugin_filename: "plugin.py".into(),
                    database_name: "test-db".into(),
                    node_spec: crate::log::versions::v1::NodeSpec::All,
                    trigger:
                        crate::log::versions::v1::TriggerSpecificationDefinition::Schedule {
                            schedule: "* 1 * * *".into(),
                        },
                    trigger_settings: TriggerSettings {
                        run_async: false,
                        error_behavior: crate::log::versions::v1::ErrorBehavior::Disable,
                    },
                    trigger_arguments: Some(
                        [(String::from("k"), String::from("v"))]
                            .into_iter()
                            .collect(),
                    ),
                    disabled: false,
                }),
                DatabaseCatalogOp::CreateTrigger(TriggerDefinition {
                    trigger_id: TriggerId::new(0),
                    trigger_name: "test-trigger".into(),
                    plugin_filename: "plugin.py".into(),
                    database_name: "test-db".into(),
                    node_spec: crate::log::versions::v1::NodeSpec::All,
                    trigger:
                        crate::log::versions::v1::TriggerSpecificationDefinition::RequestPath {
                            path: "/my/fancy/api".into(),
                        },
                    trigger_settings: TriggerSettings {
                        run_async: true,
                        error_behavior: crate::log::versions::v1::ErrorBehavior::Retry,
                    },
                    trigger_arguments: Some(
                        [(String::from("k"), String::from("v"))]
                            .into_iter()
                            .collect(),
                    ),
                    disabled: false,
                }),
                DatabaseCatalogOp::CreateTrigger(TriggerDefinition {
                    trigger_id: TriggerId::new(0),
                    trigger_name: "test-trigger".into(),
                    plugin_filename: "plugin.py".into(),
                    database_name: "test-db".into(),
                    node_spec: crate::log::versions::v1::NodeSpec::All,
                    trigger: crate::log::versions::v1::TriggerSpecificationDefinition::Every {
                        duration: Duration::from_secs(1337),
                    },
                    trigger_settings: TriggerSettings {
                        run_async: true,
                        error_behavior: crate::log::versions::v1::ErrorBehavior::Retry,
                    },
                    trigger_arguments: Some(
                        [(String::from("k"), String::from("v"))]
                            .into_iter()
                            .collect(),
                    ),
                    disabled: false,
                }),
                DatabaseCatalogOp::DeleteTrigger(DeleteTriggerLog {
                    trigger_id: TriggerId::new(0),
                    trigger_name: "test-trigger".into(),
                    force: false,
                }),
                DatabaseCatalogOp::EnableTrigger(TriggerIdentifier {
                    db_id: DbId::new(0),
                    db_name: "test-db".into(),
                    trigger_id: TriggerId::new(0),
                    trigger_name: "test-trigger".into(),
                }),
                DatabaseCatalogOp::DisableTrigger(TriggerIdentifier {
                    db_id: DbId::new(0),
                    db_name: "test-db".into(),
                    trigger_id: TriggerId::new(0),
                    trigger_name: "test-trigger".into(),
                }),
            ],
        }),
        sequence_number: CatalogSequenceNumber::new(0),
    };
    verify_and_deserialize_catalog_file(
        serialize_catalog_file(&log_v1).expect("must be able to serialize"),
    )
    .expect("deserialize from v1 to latest");
}

#[test]
fn test_deserialize_catalog_file_from_v2() {
    // test a node log file:
    verify_and_deserialize_catalog_file(
        serialize_catalog_file(&v2::OrderedCatalogBatch {
            catalog_batch: crate::log::versions::v2::CatalogBatch::Node(v2::NodeBatch {
                time_ns: 0,
                node_catalog_id: NodeId::new(0),
                node_id: "test-node".into(),
                ops: vec![v2::NodeCatalogOp::RegisterNode(v2::RegisterNodeLog {
                    node_id: "test-node".into(),
                    instance_id: "uuid".into(),
                    registered_time_ns: 0,
                    core_count: 2,
                    mode: vec![v2::NodeMode::Core],
                    // v2 only
                    process_uuid: Uuid::new_v4(),
                })],
            }),
            sequence_number: CatalogSequenceNumber::new(0),
        })
        .expect("must be able to serialize"),
    )
    .expect("deserialize from v2");

    // test a database log file:
    let log_v2 = v2::OrderedCatalogBatch {
        catalog_batch: crate::log::versions::v2::CatalogBatch::Database(v2::DatabaseBatch {
            time_ns: 0,
            database_id: DbId::new(0),
            database_name: "test-db".into(),
            ops: vec![
                v2::DatabaseCatalogOp::CreateDatabase(v2::CreateDatabaseLog {
                    database_id: DbId::new(0),
                    database_name: "test-db".into(),
                }),
                v2::DatabaseCatalogOp::SoftDeleteDatabase(v2::SoftDeleteDatabaseLog {
                    database_id: DbId::new(0),
                    database_name: "test-db".into(),
                    deletion_time: 0,
                }),
                v2::DatabaseCatalogOp::CreateTable(v2::CreateTableLog {
                    database_id: DbId::new(0),
                    database_name: "test-db".into(),
                    table_name: "test-table".into(),
                    table_id: TableId::new(0),
                    field_definitions: vec![
                        v2::FieldDefinition {
                            name: "string".into(),
                            id: ColumnId::new(0),
                            data_type: crate::log::versions::v2::FieldDataType::String,
                        },
                        v2::FieldDefinition {
                            name: "int".into(),
                            id: ColumnId::new(1),
                            data_type: crate::log::versions::v2::FieldDataType::Integer,
                        },
                        v2::FieldDefinition {
                            name: "uint".into(),
                            id: ColumnId::new(2),
                            data_type: crate::log::versions::v2::FieldDataType::UInteger,
                        },
                        v2::FieldDefinition {
                            name: "float".into(),
                            id: ColumnId::new(3),
                            data_type: crate::log::versions::v2::FieldDataType::Float,
                        },
                        v2::FieldDefinition {
                            name: "bool".into(),
                            id: ColumnId::new(4),
                            data_type: crate::log::versions::v2::FieldDataType::Boolean,
                        },
                        v2::FieldDefinition {
                            name: "tag".into(),
                            id: ColumnId::new(5),
                            data_type: crate::log::versions::v2::FieldDataType::Tag,
                        },
                        v2::FieldDefinition {
                            name: "time".into(),
                            id: ColumnId::new(6),
                            data_type: crate::log::versions::v2::FieldDataType::Timestamp,
                        },
                    ],
                    key: vec![ColumnId::new(5)],
                }),
                v2::DatabaseCatalogOp::SoftDeleteTable(v2::SoftDeleteTableLog {
                    database_id: DbId::new(0),
                    database_name: "test-db".into(),
                    table_id: TableId::new(0),
                    table_name: "test-table".into(),
                    deletion_time: 10,
                }),
                v2::DatabaseCatalogOp::AddFields(v2::AddFieldsLog {
                    database_name: "test-db".into(),
                    database_id: DbId::new(0),
                    table_name: "test-table".into(),
                    table_id: TableId::new(0),
                    field_definitions: vec![
                        v2::FieldDefinition {
                            name: "string".into(),
                            id: ColumnId::new(0),
                            data_type: crate::log::versions::v2::FieldDataType::String,
                        },
                        v2::FieldDefinition {
                            name: "int".into(),
                            id: ColumnId::new(1),
                            data_type: crate::log::versions::v2::FieldDataType::Integer,
                        },
                        v2::FieldDefinition {
                            name: "uint".into(),
                            id: ColumnId::new(2),
                            data_type: crate::log::versions::v2::FieldDataType::UInteger,
                        },
                        v2::FieldDefinition {
                            name: "float".into(),
                            id: ColumnId::new(3),
                            data_type: crate::log::versions::v2::FieldDataType::Float,
                        },
                        v2::FieldDefinition {
                            name: "bool".into(),
                            id: ColumnId::new(4),
                            data_type: crate::log::versions::v2::FieldDataType::Boolean,
                        },
                        v2::FieldDefinition {
                            name: "tag".into(),
                            id: ColumnId::new(5),
                            data_type: crate::log::versions::v2::FieldDataType::Tag,
                        },
                        v2::FieldDefinition {
                            name: "time".into(),
                            id: ColumnId::new(6),
                            data_type: crate::log::versions::v2::FieldDataType::Timestamp,
                        },
                    ],
                }),
                v2::DatabaseCatalogOp::CreateDistinctCache(v2::DistinctCacheDefinition {
                    table_id: TableId::new(0),
                    table_name: "test-table".into(),
                    cache_id: DistinctCacheId::new(0),
                    cache_name: "test-distinct-cache".into(),
                    column_ids: vec![ColumnId::new(0), ColumnId::new(1)],
                    max_cardinality: v2::MaxCardinality::default(),
                    max_age_seconds: v2::MaxAge::default(),
                    node_spec: crate::log::versions::v2::NodeSpec::All,
                }),
                v2::DatabaseCatalogOp::DeleteDistinctCache(v2::DeleteDistinctCacheLog {
                    table_id: TableId::new(0),
                    table_name: "test-table".into(),
                    cache_id: DistinctCacheId::new(0),
                    cache_name: "test-distinct-cache".into(),
                }),
                v2::DatabaseCatalogOp::CreateLastCache(v2::LastCacheDefinition {
                    table_id: TableId::new(0),
                    table: "test-table".into(),
                    id: LastCacheId::new(0),
                    name: "test-last-cache".into(),
                    key_columns: vec![ColumnId::new(0), ColumnId::new(1)],
                    value_columns:
                    crate::log::versions::v2::LastCacheValueColumnsDef::AllNonKeyColumns,
                    count: v2::LastCacheSize::default(),
                    ttl: v2::LastCacheTtl::default(),
                    node_spec: crate::log::versions::v2::NodeSpec::Nodes(vec![
                        NodeId::new(0),
                        NodeId::new(1),
                    ]),
                }),
                v2::DatabaseCatalogOp::CreateLastCache(v2::LastCacheDefinition {
                    table_id: TableId::new(0),
                    table: "test-table".into(),
                    id: LastCacheId::new(0),
                    name: "test-last-cache".into(),
                    key_columns: vec![ColumnId::new(0), ColumnId::new(1)],
                    value_columns:
                        crate::log::versions::v2::LastCacheValueColumnsDef::Explicit {
                            columns: vec![ColumnId::new(2), ColumnId::new(3)],
                        },
                    count: v2::LastCacheSize::default(),
                    ttl: v2::LastCacheTtl::default(),
                    node_spec: crate::log::versions::v2::NodeSpec::All,
                }),
                v2::DatabaseCatalogOp::DeleteLastCache(v2::DeleteLastCacheLog {
                    table_id: TableId::new(0),
                    table_name: "test-table".into(),
                    id: LastCacheId::new(0),
                    name: "test-last-cache".into(),
                }),
                v2::DatabaseCatalogOp::CreateTrigger(v2::TriggerDefinition {
                    trigger_id: TriggerId::new(0),
                    trigger_name: "test-trigger".into(),
                    plugin_filename: "plugin.py".into(),
                    database_name: "test-db".into(),
                    node_spec: crate::log::versions::v2::NodeSpec::All,
                    trigger:
                    crate::log::versions::v2::TriggerSpecificationDefinition::AllTablesWalWrite,
                    trigger_settings: v2::TriggerSettings {
                        run_async: true,
                        error_behavior: crate::log::versions::v2::ErrorBehavior::Retry,
                    },
                    trigger_arguments: Some(
                        [(String::from("k"), String::from("v"))]
                            .into_iter()
                            .collect(),
                    ),
                    disabled: false,
                }),
                v2::DatabaseCatalogOp::CreateTrigger(v2::TriggerDefinition {
                    trigger_id: TriggerId::new(0),
                    trigger_name: "test-trigger".into(),
                    plugin_filename: "plugin.py".into(),
                    database_name: "test-db".into(),
                    node_spec: crate::log::versions::v2::NodeSpec::Nodes(vec![
                        NodeId::new(0),
                        NodeId::new(1),
                    ]),
                    trigger:
                    crate::log::versions::v2::TriggerSpecificationDefinition::SingleTableWalWrite {
                            table_name: "test-table".into(),
                        },
                    trigger_settings: v2::TriggerSettings {
                        run_async: true,
                        error_behavior: crate::log::versions::v2::ErrorBehavior::Log,
                    },
                    trigger_arguments: Some(
                        [(String::from("k"), String::from("v"))]
                            .into_iter()
                            .collect(),
                    ),
                    disabled: false,
                }),
                v2::DatabaseCatalogOp::CreateTrigger(v2::TriggerDefinition {
                    trigger_id: TriggerId::new(0),
                    trigger_name: "test-trigger".into(),
                    plugin_filename: "plugin.py".into(),
                    database_name: "test-db".into(),
                    node_spec: crate::log::versions::v2::NodeSpec::All,
                    trigger:
                        crate::log::versions::v2::TriggerSpecificationDefinition::Schedule {
                            schedule: "* 1 * * *".into(),
                        },
                    trigger_settings: v2::TriggerSettings {
                        run_async: false,
                        error_behavior: crate::log::versions::v2::ErrorBehavior::Disable,
                    },
                    trigger_arguments: Some(
                        [(String::from("k"), String::from("v"))]
                            .into_iter()
                            .collect(),
                    ),
                    disabled: false,
                }),
                v2::DatabaseCatalogOp::CreateTrigger(v2::TriggerDefinition {
                    trigger_id: TriggerId::new(0),
                    trigger_name: "test-trigger".into(),
                    plugin_filename: "plugin.py".into(),
                    database_name: "test-db".into(),
                    node_spec: crate::log::versions::v2::NodeSpec::All,
                    trigger:
                        crate::log::versions::v2::TriggerSpecificationDefinition::RequestPath {
                            path: "/my/fancy/api".into(),
                        },
                    trigger_settings: v2::TriggerSettings {
                        run_async: true,
                        error_behavior: crate::log::versions::v2::ErrorBehavior::Retry,
                    },
                    trigger_arguments: Some(
                        [(String::from("k"), String::from("v"))]
                            .into_iter()
                            .collect(),
                    ),
                    disabled: false,
                }),
                v2::DatabaseCatalogOp::CreateTrigger(v2::TriggerDefinition {
                    trigger_id: TriggerId::new(0),
                    trigger_name: "test-trigger".into(),
                    plugin_filename: "plugin.py".into(),
                    database_name: "test-db".into(),
                    node_spec: crate::log::versions::v2::NodeSpec::All,
                    trigger: crate::log::versions::v2::TriggerSpecificationDefinition::Every {
                        duration: Duration::from_secs(1337),
                    },
                    trigger_settings: v2::TriggerSettings {
                        run_async: true,
                        error_behavior: crate::log::versions::v2::ErrorBehavior::Retry,
                    },
                    trigger_arguments: Some(
                        [(String::from("k"), String::from("v"))]
                            .into_iter()
                            .collect(),
                    ),
                    disabled: false,
                }),
                v2::DatabaseCatalogOp::DeleteTrigger(v2::DeleteTriggerLog {
                    trigger_id: TriggerId::new(0),
                    trigger_name: "test-trigger".into(),
                    force: false,
                }),
                v2::DatabaseCatalogOp::EnableTrigger(v2::TriggerIdentifier {
                    db_id: DbId::new(0),
                    db_name: "test-db".into(),
                    trigger_id: TriggerId::new(0),
                    trigger_name: "test-trigger".into(),
                }),
                v2::DatabaseCatalogOp::DisableTrigger(v2::TriggerIdentifier {
                    db_id: DbId::new(0),
                    db_name: "test-db".into(),
                    trigger_id: TriggerId::new(0),
                    trigger_name: "test-trigger".into(),
                }),
            ],
        }),
        sequence_number: CatalogSequenceNumber::new(0),
    };

    verify_and_deserialize_catalog_file(
        serialize_catalog_file(&log_v2).expect("must be able to serialize"),
    )
    .expect("deserialize from v2 to latest");

    // test a token log file:
    let token_v3_log: v3::OrderedCatalogBatch = verify_and_deserialize_catalog_file(
        serialize_catalog_file(&v2::OrderedCatalogBatch {
            catalog_batch: crate::log::versions::v2::CatalogBatch::Token(v2::TokenBatch {
                time_ns: 0,
                ops: vec![v2::TokenCatalogOp::CreateDatabaseToken(
                    v2::enterprise::CreateDatabaseTokenDetails {
                        token_id: TokenId::new(0),
                        name: Arc::from("foo"),
                        hash: "some-hash".as_bytes().to_vec(),
                        created_at: 1234,
                        expiry: None,
                        permissions: vec![v2::enterprise::PermissionDetails {
                            resource_type: AUTHZ_DB_RESOURCE_TYPE.to_owned(),
                            resource_identifier: vec![AUTHZ_WILDCARD.to_owned()],
                            actions: vec![AUTHZ_WRITE_DB_ACTION.to_owned()],
                        }],
                    },
                )],
            }),
            sequence_number: CatalogSequenceNumber::new(0),
        })
        .expect("must be able to serialize"),
    )
    .expect("deserialize from v2");
    let token_batch = token_v3_log
        .catalog_batch
        .as_token()
        .expect("token batch to be present");
    let db_token_log = token_batch.ops.first().expect("token log op to be present");
    match db_token_log {
        v3::TokenCatalogOp::CreateDatabaseToken(create_database_token_details) => {
            let permissions = &create_database_token_details.permissions;
            assert_eq!(1, permissions.len());
            let permission = permissions.first().expect("permission to be present");
            assert_eq!(2, permission.actions.len());
            assert!(
                &[AUTHZ_WRITE_DB_ACTION, AUTHZ_CREATE_DB_ACTION]
                    .iter()
                    .all(|action| { permission.actions.contains(&action.to_string()) })
            );
        }
        _ => panic!("unexpected token log op type"),
    }
}

#[test]
fn test_deserialize_catalog_file_from_latest() {
    // test a node log file:
    verify_and_deserialize_catalog_file(
        super::serialize_catalog_file(&log::OrderedCatalogBatch {
            catalog_batch: log::CatalogBatch::Node(log::NodeBatch {
                time_ns: 0,
                node_catalog_id: NodeId::new(0),
                node_id: "test-node".into(),
                ops: vec![log::NodeCatalogOp::RegisterNode(log::RegisterNodeLog {
                    node_id: "test-node".into(),
                    instance_id: "uuid".into(),
                    registered_time_ns: 0,
                    core_count: 2,
                    mode: vec![log::NodeMode::Core],
                    // v2 only
                    process_uuid: Uuid::new_v4(),
                })],
            }),
            sequence_number: CatalogSequenceNumber::new(0),
        })
        .expect("must be able to serialize to latest"),
    )
    .expect("deserialize from latest");

    // test a database log file:
    let log_vlatest = log::OrderedCatalogBatch {
        catalog_batch: log::CatalogBatch::Database(log::DatabaseBatch {
            time_ns: 0,
            database_id: DbId::new(0),
            database_name: "test-db".into(),
            ops: vec![
                log::DatabaseCatalogOp::CreateDatabase(log::CreateDatabaseLog {
                    database_id: DbId::new(0),
                    database_name: "test-db".into(),
                    retention_period: None,
                }),
                log::DatabaseCatalogOp::SoftDeleteDatabase(log::SoftDeleteDatabaseLog {
                    database_id: DbId::new(0),
                    database_name: "test-db".into(),
                    deletion_time: 0,
                    hard_deletion_time: None,
                }),
                log::DatabaseCatalogOp::CreateTable(log::CreateTableLog {
                    database_id: DbId::new(0),
                    database_name: "test-db".into(),
                    table_name: "test-table".into(),
                    table_id: TableId::new(0),
                    field_definitions: vec![
                        log::FieldDefinition {
                            name: "string".into(),
                            id: ColumnId::new(0),
                            data_type: log::FieldDataType::String,
                        },
                        log::FieldDefinition {
                            name: "int".into(),
                            id: ColumnId::new(1),
                            data_type: log::FieldDataType::Integer,
                        },
                        log::FieldDefinition {
                            name: "uint".into(),
                            id: ColumnId::new(2),
                            data_type: log::FieldDataType::UInteger,
                        },
                        log::FieldDefinition {
                            name: "float".into(),
                            id: ColumnId::new(3),
                            data_type: log::FieldDataType::Float,
                        },
                        log::FieldDefinition {
                            name: "bool".into(),
                            id: ColumnId::new(4),
                            data_type: log::FieldDataType::Boolean,
                        },
                        log::FieldDefinition {
                            name: "tag".into(),
                            id: ColumnId::new(5),
                            data_type: log::FieldDataType::Tag,
                        },
                        log::FieldDefinition {
                            name: "time".into(),
                            id: ColumnId::new(6),
                            data_type: log::FieldDataType::Timestamp,
                        },
                    ],
                    key: vec![ColumnId::new(5)],
                    retention_period: None,
                }),
                log::DatabaseCatalogOp::SoftDeleteTable(log::SoftDeleteTableLog {
                    database_id: DbId::new(0),
                    database_name: "test-db".into(),
                    table_id: TableId::new(0),
                    table_name: "test-table".into(),
                    deletion_time: 10,
                    hard_deletion_time: None,
                }),
                log::DatabaseCatalogOp::AddFields(log::AddFieldsLog {
                    database_name: "test-db".into(),
                    database_id: DbId::new(0),
                    table_name: "test-table".into(),
                    table_id: TableId::new(0),
                    field_definitions: vec![
                        log::FieldDefinition {
                            name: "string".into(),
                            id: ColumnId::new(0),
                            data_type: log::FieldDataType::String,
                        },
                        log::FieldDefinition {
                            name: "int".into(),
                            id: ColumnId::new(1),
                            data_type: log::FieldDataType::Integer,
                        },
                        log::FieldDefinition {
                            name: "uint".into(),
                            id: ColumnId::new(2),
                            data_type: log::FieldDataType::UInteger,
                        },
                        log::FieldDefinition {
                            name: "float".into(),
                            id: ColumnId::new(3),
                            data_type: log::FieldDataType::Float,
                        },
                        log::FieldDefinition {
                            name: "bool".into(),
                            id: ColumnId::new(4),
                            data_type: log::FieldDataType::Boolean,
                        },
                        log::FieldDefinition {
                            name: "tag".into(),
                            id: ColumnId::new(5),
                            data_type: log::FieldDataType::Tag,
                        },
                        log::FieldDefinition {
                            name: "time".into(),
                            id: ColumnId::new(6),
                            data_type: log::FieldDataType::Timestamp,
                        },
                    ],
                }),
                log::DatabaseCatalogOp::CreateDistinctCache(log::DistinctCacheDefinition {
                    table_id: TableId::new(0),
                    table_name: "test-table".into(),
                    cache_id: DistinctCacheId::new(0),
                    cache_name: "test-distinct-cache".into(),
                    column_ids: vec![ColumnId::new(0), ColumnId::new(1)],
                    max_cardinality: log::MaxCardinality::default(),
                    max_age_seconds: log::MaxAge::default(),
                    node_spec: log::NodeSpec::All,
                }),
                log::DatabaseCatalogOp::DeleteDistinctCache(log::DeleteDistinctCacheLog {
                    table_id: TableId::new(0),
                    table_name: "test-table".into(),
                    cache_id: DistinctCacheId::new(0),
                    cache_name: "test-distinct-cache".into(),
                }),
                log::DatabaseCatalogOp::CreateLastCache(log::LastCacheDefinition {
                    table_id: TableId::new(0),
                    table: "test-table".into(),
                    id: LastCacheId::new(0),
                    name: "test-last-cache".into(),
                    key_columns: vec![ColumnId::new(0), ColumnId::new(1)],
                    value_columns: log::LastCacheValueColumnsDef::AllNonKeyColumns,
                    count: log::LastCacheSize::default(),
                    ttl: log::LastCacheTtl::default(),
                    node_spec: log::NodeSpec::Nodes(vec![NodeId::new(0), NodeId::new(1)]),
                }),
                log::DatabaseCatalogOp::CreateLastCache(log::LastCacheDefinition {
                    table_id: TableId::new(0),
                    table: "test-table".into(),
                    id: LastCacheId::new(0),
                    name: "test-last-cache".into(),
                    key_columns: vec![ColumnId::new(0), ColumnId::new(1)],
                    value_columns: log::LastCacheValueColumnsDef::Explicit {
                        columns: vec![ColumnId::new(2), ColumnId::new(3)],
                    },
                    count: log::LastCacheSize::default(),
                    ttl: log::LastCacheTtl::default(),
                    node_spec: log::NodeSpec::All,
                }),
                log::DatabaseCatalogOp::DeleteLastCache(log::DeleteLastCacheLog {
                    table_id: TableId::new(0),
                    table_name: "test-table".into(),
                    id: LastCacheId::new(0),
                    name: "test-last-cache".into(),
                }),
                log::DatabaseCatalogOp::CreateTrigger(log::TriggerDefinition {
                    trigger_id: TriggerId::new(0),
                    trigger_name: "test-trigger".into(),
                    plugin_filename: "plugin.py".into(),
                    database_name: "test-db".into(),
                    node_spec: log::NodeSpec::All,
                    trigger: log::TriggerSpecificationDefinition::AllTablesWalWrite,
                    trigger_settings: log::TriggerSettings {
                        run_async: true,
                        error_behavior: log::ErrorBehavior::Retry,
                    },
                    trigger_arguments: Some(
                        [(String::from("k"), String::from("v"))]
                            .into_iter()
                            .collect(),
                    ),
                    disabled: false,
                }),
                log::DatabaseCatalogOp::CreateTrigger(log::TriggerDefinition {
                    trigger_id: TriggerId::new(0),
                    trigger_name: "test-trigger".into(),
                    plugin_filename: "plugin.py".into(),
                    database_name: "test-db".into(),
                    node_spec: log::NodeSpec::Nodes(vec![NodeId::new(0), NodeId::new(1)]),
                    trigger: log::TriggerSpecificationDefinition::SingleTableWalWrite {
                        table_name: "test-table".into(),
                    },
                    trigger_settings: log::TriggerSettings {
                        run_async: true,
                        error_behavior: log::ErrorBehavior::Log,
                    },
                    trigger_arguments: Some(
                        [(String::from("k"), String::from("v"))]
                            .into_iter()
                            .collect(),
                    ),
                    disabled: false,
                }),
                log::DatabaseCatalogOp::CreateTrigger(log::TriggerDefinition {
                    trigger_id: TriggerId::new(0),
                    trigger_name: "test-trigger".into(),
                    plugin_filename: "plugin.py".into(),
                    database_name: "test-db".into(),
                    node_spec: log::NodeSpec::All,
                    trigger: log::TriggerSpecificationDefinition::Schedule {
                        schedule: "* 1 * * *".into(),
                    },
                    trigger_settings: log::TriggerSettings {
                        run_async: false,
                        error_behavior: log::ErrorBehavior::Disable,
                    },
                    trigger_arguments: Some(
                        [(String::from("k"), String::from("v"))]
                            .into_iter()
                            .collect(),
                    ),
                    disabled: false,
                }),
                log::DatabaseCatalogOp::CreateTrigger(log::TriggerDefinition {
                    trigger_id: TriggerId::new(0),
                    trigger_name: "test-trigger".into(),
                    plugin_filename: "plugin.py".into(),
                    database_name: "test-db".into(),
                    node_spec: log::NodeSpec::All,
                    trigger: log::TriggerSpecificationDefinition::RequestPath {
                        path: "/my/fancy/api".into(),
                    },
                    trigger_settings: log::TriggerSettings {
                        run_async: true,
                        error_behavior: log::ErrorBehavior::Retry,
                    },
                    trigger_arguments: Some(
                        [(String::from("k"), String::from("v"))]
                            .into_iter()
                            .collect(),
                    ),
                    disabled: false,
                }),
                log::DatabaseCatalogOp::CreateTrigger(log::TriggerDefinition {
                    trigger_id: TriggerId::new(0),
                    trigger_name: "test-trigger".into(),
                    plugin_filename: "plugin.py".into(),
                    database_name: "test-db".into(),
                    node_spec: log::NodeSpec::All,
                    trigger: log::TriggerSpecificationDefinition::Every {
                        duration: Duration::from_secs(1337),
                    },
                    trigger_settings: log::TriggerSettings {
                        run_async: true,
                        error_behavior: log::ErrorBehavior::Retry,
                    },
                    trigger_arguments: Some(
                        [(String::from("k"), String::from("v"))]
                            .into_iter()
                            .collect(),
                    ),
                    disabled: false,
                }),
                log::DatabaseCatalogOp::DeleteTrigger(log::DeleteTriggerLog {
                    trigger_id: TriggerId::new(0),
                    trigger_name: "test-trigger".into(),
                    force: false,
                }),
                log::DatabaseCatalogOp::EnableTrigger(log::TriggerIdentifier {
                    db_id: DbId::new(0),
                    db_name: "test-db".into(),
                    trigger_id: TriggerId::new(0),
                    trigger_name: "test-trigger".into(),
                }),
                log::DatabaseCatalogOp::DisableTrigger(log::TriggerIdentifier {
                    db_id: DbId::new(0),
                    db_name: "test-db".into(),
                    trigger_id: TriggerId::new(0),
                    trigger_name: "test-trigger".into(),
                }),
            ],
        }),
        sequence_number: CatalogSequenceNumber::new(0),
    };

    verify_and_deserialize_catalog_file(
        super::serialize_catalog_file(&log_vlatest).expect("must be able to serialize to latest"),
    )
    .expect("deserialize from latest");
}

#[test]
fn test_serialize_catalog_with_latest_identifier() {
    // test a node log file:
    let serialized = super::serialize_catalog_file(&crate::log::OrderedCatalogBatch {
        catalog_batch: crate::log::CatalogBatch::Node(crate::log::NodeBatch {
            time_ns: 0,
            node_catalog_id: NodeId::new(0),
            node_id: "test-node".into(),
            ops: vec![crate::log::NodeCatalogOp::RegisterNode(
                crate::log::RegisterNodeLog {
                    node_id: "test-node".into(),
                    instance_id: "uuid".into(),
                    registered_time_ns: 0,
                    core_count: 2,
                    mode: vec![crate::log::NodeMode::Core],
                    // v2 only
                    process_uuid: Uuid::new_v4(),
                    conn_info: None,
                    cli_params: None,
                    row_delete_predicate_version: 0,
                },
            )],
        }),
        sequence_number: CatalogSequenceNumber::new(0),
    })
    .expect("must be able to serialize to latest");

    assert!(
        serialized.starts_with(&crate::log::OrderedCatalogBatch::VERSION_ID),
        "serialized catalog log file must always start with latest verison identifier"
    );
}

#[test]
fn test_serialize_catalog_snapshot_with_latest_identifier() {
    let snapshot = crate::snapshot::versions::v1::CatalogSnapshot::generate();
    let snapshot: crate::snapshot::versions::v2::CatalogSnapshot = snapshot.into();
    let snapshot: snapshot::CatalogSnapshot = snapshot.into();
    let serialized = super::serialize_catalog_file(&snapshot)
        .expect("must be able to serialize generated snapshot");

    assert!(
        serialized.starts_with(&snapshot::CatalogSnapshot::VERSION_ID),
        "serialized catalog log file must always start with latest verison identifier"
    );
}

#[test]
fn test_deserialize_catalog_checkpoint_file_from_v1() {
    let result = verify_and_deserialize_catalog_checkpoint_file(
        serialize_catalog_file(&CatalogSnapshot::generate()).expect("must be able to serialize"),
    );
    println!("{result:?}");
    result.expect("deserialize from v1");
}

/// Compatibility tests for v2- and v3-format catalog files written by InfluxDB 3 Core
/// before the catalog codebases were unified.
///
/// Core's structs lacked several fields that exist in the unified definitions, and its
/// trigger definitions carried a `node_id` string instead of a `node_spec`. See
/// <https://github.com/influxdata/influxdb/issues/27554>.
mod core_file_compat {
    use serde_json::json;

    use crate::serialize::versions::test_util::from_core_shape;

    mod snapshot_v2 {
        use super::*;
        use crate::log::versions::v2::{
            MaxAge, MaxCardinality, NodeSpec, TriggerSettings, TriggerSpecificationDefinition,
        };
        use crate::snapshot::versions::v2::{
            DistinctCacheSnapshot, LastCacheSnapshot, ProcessingEngineTriggerSnapshot,
        };
        use influxdb3_id::{DistinctCacheId, LastCacheId, TableId, TriggerId};

        #[test]
        fn core_last_cache_has_no_node_spec() {
            let cache = LastCacheSnapshot {
                table_id: TableId::new(0),
                table: "cpu".into(),
                node_spec: NodeSpec::All,
                id: LastCacheId::new(0),
                name: "cpu_lvc".into(),
                keys: vec![],
                vals: None,
                n: 1,
                ttl: 14400,
            };
            let deserialized = from_core_shape(&cache, &["node_spec"], &[])
                .expect("last cache snapshot written by core must deserialize");
            assert_eq!(NodeSpec::All, deserialized.node_spec);
        }

        #[test]
        fn core_distinct_cache_has_no_node_spec() {
            let cache = DistinctCacheSnapshot {
                table_id: TableId::new(0),
                table: "cpu".into(),
                node_spec: NodeSpec::All,
                id: DistinctCacheId::new(0),
                name: "cpu_dvc".into(),
                cols: vec![],
                max_cardinality: MaxCardinality::default(),
                max_age_seconds: MaxAge::default(),
            };
            let deserialized = from_core_shape(&cache, &["node_spec"], &[])
                .expect("distinct cache snapshot written by core must deserialize");
            assert_eq!(NodeSpec::All, deserialized.node_spec);
        }

        #[test]
        fn core_trigger_carries_node_id_not_node_spec() {
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
            let deserialized =
                from_core_shape(&trigger, &["node_spec"], &[("node_id", json!("node0"))])
                    .expect("trigger snapshot written by core must deserialize");
            assert_eq!(NodeSpec::All, deserialized.node_spec);
        }
    }

    mod snapshot_v3 {
        use super::*;
        use crate::log::versions::v3::{
            MaxAge, MaxCardinality, NodeSpec, TriggerSettings, TriggerSpecificationDefinition,
        };
        use crate::snapshot::versions::v3::{
            DistinctCacheSnapshot, LastCacheSnapshot, ProcessingEngineTriggerSnapshot,
            RepositorySnapshot, TableSnapshot,
        };
        use influxdb3_id::{
            ColumnId, DistinctCacheId, LastCacheId, SerdeVecMap, TableId, TriggerId,
        };

        #[test]
        fn core_table_has_no_retention_period_and_omits_hard_delete_time() {
            let table = TableSnapshot {
                table_id: TableId::new(0),
                table_name: "cpu".into(),
                key: vec![],
                columns: RepositorySnapshot {
                    repo: SerdeVecMap::default(),
                    next_id: ColumnId::new(0),
                },
                retention_period: None,
                last_caches: RepositorySnapshot {
                    repo: SerdeVecMap::default(),
                    next_id: LastCacheId::new(0),
                },
                distinct_caches: RepositorySnapshot {
                    repo: SerdeVecMap::default(),
                    next_id: DistinctCacheId::new(0),
                },
                deleted: false,
                hard_delete_time: None,
            };
            let deserialized =
                from_core_shape(&table, &["retention_period", "hard_delete_time"], &[])
                    .expect("table snapshot written by core must deserialize");
            assert!(deserialized.retention_period.is_none());
            assert!(deserialized.hard_delete_time.is_none());
        }

        #[test]
        fn core_last_cache_has_no_node_spec() {
            let cache = LastCacheSnapshot {
                table_id: TableId::new(0),
                table: "cpu".into(),
                node_spec: NodeSpec::All,
                id: LastCacheId::new(0),
                name: "cpu_lvc".into(),
                keys: vec![],
                vals: None,
                n: 1,
                ttl: 14400,
            };
            let deserialized = from_core_shape(&cache, &["node_spec"], &[])
                .expect("last cache snapshot written by core must deserialize");
            assert_eq!(NodeSpec::All, deserialized.node_spec);
        }

        #[test]
        fn core_distinct_cache_has_no_node_spec() {
            let cache = DistinctCacheSnapshot {
                table_id: TableId::new(0),
                table: "cpu".into(),
                node_spec: NodeSpec::All,
                id: DistinctCacheId::new(0),
                name: "cpu_dvc".into(),
                cols: vec![],
                max_cardinality: MaxCardinality::default(),
                max_age_seconds: MaxAge::default(),
            };
            let deserialized = from_core_shape(&cache, &["node_spec"], &[])
                .expect("distinct cache snapshot written by core must deserialize");
            assert_eq!(NodeSpec::All, deserialized.node_spec);
        }

        #[test]
        fn core_trigger_carries_node_id_not_node_spec() {
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
            let deserialized =
                from_core_shape(&trigger, &["node_spec"], &[("node_id", json!("node0"))])
                    .expect("trigger snapshot written by core must deserialize");
            assert_eq!(NodeSpec::All, deserialized.node_spec);
        }
    }

    mod log_v3 {
        use super::*;
        use crate::log::versions::v3::{
            ClearRetentionPeriodLog, CreateTableLog, RetentionPeriod, SetRetentionPeriodLog,
        };
        use influxdb3_id::{DbId, TableId};

        #[test]
        fn core_create_table_log_has_no_retention_period() {
            let log = CreateTableLog {
                database_id: DbId::new(0),
                database_name: "mydb".into(),
                table_name: "cpu".into(),
                table_id: TableId::new(0),
                field_definitions: vec![],
                key: vec![],
                retention_period: None,
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
    }
}
