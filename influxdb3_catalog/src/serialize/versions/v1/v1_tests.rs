//! Test the conversion from `v1` catalog format, which used bitcode serialization/deserialization
use std::time::Duration;

use influxdb3_id::{ColumnId, DbId, DistinctCacheId, LastCacheId, NodeId, TableId, TriggerId};
use uuid::Uuid;

use crate::{
    catalog::CatalogSequenceNumber,
    log::{
        self,
        versions::{
            v1::{
                AddFieldsLog, CreateDatabaseLog, CreateTableLog, DatabaseBatch, DatabaseCatalogOp,
                DeleteDistinctCacheLog, DeleteLastCacheLog, DeleteTriggerLog,
                DistinctCacheDefinition, FieldDefinition, LastCacheDefinition, LastCacheSize,
                LastCacheTtl, MaxAge, MaxCardinality, NodeBatch, NodeCatalogOp, NodeMode,
                OrderedCatalogBatch, RegisterNodeLog, SoftDeleteDatabaseLog, SoftDeleteTableLog,
                TriggerDefinition, TriggerIdentifier, TriggerSettings,
            },
            v2,
        },
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
            catalog_batch: log::versions::v1::CatalogBatch::Node(NodeBatch {
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
    verify_and_deserialize_catalog_file(
        serialize_catalog_file(&OrderedCatalogBatch {
            catalog_batch: log::versions::v1::CatalogBatch::Database(DatabaseBatch {
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
                                data_type: log::versions::v1::FieldDataType::String,
                            },
                            FieldDefinition {
                                name: "int".into(),
                                id: ColumnId::new(1),
                                data_type: log::versions::v1::FieldDataType::Integer,
                            },
                            FieldDefinition {
                                name: "uint".into(),
                                id: ColumnId::new(2),
                                data_type: log::versions::v1::FieldDataType::UInteger,
                            },
                            FieldDefinition {
                                name: "float".into(),
                                id: ColumnId::new(3),
                                data_type: log::versions::v1::FieldDataType::Float,
                            },
                            FieldDefinition {
                                name: "bool".into(),
                                id: ColumnId::new(4),
                                data_type: log::versions::v1::FieldDataType::Boolean,
                            },
                            FieldDefinition {
                                name: "tag".into(),
                                id: ColumnId::new(5),
                                data_type: log::versions::v1::FieldDataType::Tag,
                            },
                            FieldDefinition {
                                name: "time".into(),
                                id: ColumnId::new(6),
                                data_type: log::versions::v1::FieldDataType::Timestamp,
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
                                data_type: log::versions::v1::FieldDataType::String,
                            },
                            FieldDefinition {
                                name: "int".into(),
                                id: ColumnId::new(1),
                                data_type: log::versions::v1::FieldDataType::Integer,
                            },
                            FieldDefinition {
                                name: "uint".into(),
                                id: ColumnId::new(2),
                                data_type: log::versions::v1::FieldDataType::UInteger,
                            },
                            FieldDefinition {
                                name: "float".into(),
                                id: ColumnId::new(3),
                                data_type: log::versions::v1::FieldDataType::Float,
                            },
                            FieldDefinition {
                                name: "bool".into(),
                                id: ColumnId::new(4),
                                data_type: log::versions::v1::FieldDataType::Boolean,
                            },
                            FieldDefinition {
                                name: "tag".into(),
                                id: ColumnId::new(5),
                                data_type: log::versions::v1::FieldDataType::Tag,
                            },
                            FieldDefinition {
                                name: "time".into(),
                                id: ColumnId::new(6),
                                data_type: log::versions::v1::FieldDataType::Timestamp,
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
                            log::versions::v1::LastCacheValueColumnsDef::AllNonKeyColumns,
                        count: LastCacheSize::default(),
                        ttl: LastCacheTtl::default(),
                    }),
                    DatabaseCatalogOp::CreateLastCache(LastCacheDefinition {
                        table_id: TableId::new(0),
                        table: "test-table".into(),
                        id: LastCacheId::new(0),
                        name: "test-last-cache".into(),
                        key_columns: vec![ColumnId::new(0), ColumnId::new(1)],
                        value_columns: log::versions::v1::LastCacheValueColumnsDef::Explicit {
                            columns: vec![ColumnId::new(2), ColumnId::new(3)],
                        },
                        count: LastCacheSize::default(),
                        ttl: LastCacheTtl::default(),
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
                        node_id: "test-node".into(),
                        trigger:
                            log::versions::v1::TriggerSpecificationDefinition::AllTablesWalWrite,
                        trigger_settings: TriggerSettings {
                            run_async: true,
                            error_behavior: log::versions::v1::ErrorBehavior::Retry,
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
                        node_id: "test-node".into(),
                        trigger:
                            log::versions::v1::TriggerSpecificationDefinition::SingleTableWalWrite {
                                table_name: "test-table".into(),
                            },
                        trigger_settings: TriggerSettings {
                            run_async: true,
                            error_behavior: log::versions::v1::ErrorBehavior::Log,
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
                        node_id: "test-node".into(),
                        trigger: log::versions::v1::TriggerSpecificationDefinition::Schedule {
                            schedule: "* 1 * * *".into(),
                        },
                        trigger_settings: TriggerSettings {
                            run_async: false,
                            error_behavior: log::versions::v1::ErrorBehavior::Disable,
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
                        node_id: "test-node".into(),
                        trigger: log::versions::v1::TriggerSpecificationDefinition::RequestPath {
                            path: "/my/fancy/api".into(),
                        },
                        trigger_settings: TriggerSettings {
                            run_async: true,
                            error_behavior: log::versions::v1::ErrorBehavior::Retry,
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
                        node_id: "test-node".into(),
                        trigger: log::versions::v1::TriggerSpecificationDefinition::Every {
                            duration: Duration::from_secs(1337),
                        },
                        trigger_settings: TriggerSettings {
                            run_async: true,
                            error_behavior: log::versions::v1::ErrorBehavior::Retry,
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
        })
        .expect("must serialize"),
    )
    .expect("must deserialize");
}

#[test]
fn test_deserialize_catalog_file_from_v2() {
    // test a node log file:
    verify_and_deserialize_catalog_file(
        serialize_catalog_file(&v2::OrderedCatalogBatch {
            catalog_batch: log::versions::v2::CatalogBatch::Node(v2::NodeBatch {
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
        catalog_batch: log::versions::v2::CatalogBatch::Database(v2::DatabaseBatch {
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
                            data_type: log::versions::v2::FieldDataType::String,
                        },
                        v2::FieldDefinition {
                            name: "int".into(),
                            id: ColumnId::new(1),
                            data_type: log::versions::v2::FieldDataType::Integer,
                        },
                        v2::FieldDefinition {
                            name: "uint".into(),
                            id: ColumnId::new(2),
                            data_type: log::versions::v2::FieldDataType::UInteger,
                        },
                        v2::FieldDefinition {
                            name: "float".into(),
                            id: ColumnId::new(3),
                            data_type: log::versions::v2::FieldDataType::Float,
                        },
                        v2::FieldDefinition {
                            name: "bool".into(),
                            id: ColumnId::new(4),
                            data_type: log::versions::v2::FieldDataType::Boolean,
                        },
                        v2::FieldDefinition {
                            name: "tag".into(),
                            id: ColumnId::new(5),
                            data_type: log::versions::v2::FieldDataType::Tag,
                        },
                        v2::FieldDefinition {
                            name: "time".into(),
                            id: ColumnId::new(6),
                            data_type: log::versions::v2::FieldDataType::Timestamp,
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
                            data_type: log::versions::v2::FieldDataType::String,
                        },
                        v2::FieldDefinition {
                            name: "int".into(),
                            id: ColumnId::new(1),
                            data_type: log::versions::v2::FieldDataType::Integer,
                        },
                        v2::FieldDefinition {
                            name: "uint".into(),
                            id: ColumnId::new(2),
                            data_type: log::versions::v2::FieldDataType::UInteger,
                        },
                        v2::FieldDefinition {
                            name: "float".into(),
                            id: ColumnId::new(3),
                            data_type: log::versions::v2::FieldDataType::Float,
                        },
                        v2::FieldDefinition {
                            name: "bool".into(),
                            id: ColumnId::new(4),
                            data_type: log::versions::v2::FieldDataType::Boolean,
                        },
                        v2::FieldDefinition {
                            name: "tag".into(),
                            id: ColumnId::new(5),
                            data_type: log::versions::v2::FieldDataType::Tag,
                        },
                        v2::FieldDefinition {
                            name: "time".into(),
                            id: ColumnId::new(6),
                            data_type: log::versions::v2::FieldDataType::Timestamp,
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
                    value_columns: log::versions::v2::LastCacheValueColumnsDef::AllNonKeyColumns,
                    count: v2::LastCacheSize::default(),
                    ttl: v2::LastCacheTtl::default(),
                }),
                v2::DatabaseCatalogOp::CreateLastCache(v2::LastCacheDefinition {
                    table_id: TableId::new(0),
                    table: "test-table".into(),
                    id: LastCacheId::new(0),
                    name: "test-last-cache".into(),
                    key_columns: vec![ColumnId::new(0), ColumnId::new(1)],
                    value_columns: log::versions::v2::LastCacheValueColumnsDef::Explicit {
                        columns: vec![ColumnId::new(2), ColumnId::new(3)],
                    },
                    count: v2::LastCacheSize::default(),
                    ttl: v2::LastCacheTtl::default(),
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
                    node_id: "test-node".into(),
                    plugin_filename: "plugin.py".into(),
                    database_name: "test-db".into(),
                    trigger: log::versions::v2::TriggerSpecificationDefinition::AllTablesWalWrite,
                    trigger_settings: v2::TriggerSettings {
                        run_async: true,
                        error_behavior: log::versions::v2::ErrorBehavior::Retry,
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
                    node_id: "test-node".into(),
                    plugin_filename: "plugin.py".into(),
                    database_name: "test-db".into(),
                    trigger:
                        log::versions::v2::TriggerSpecificationDefinition::SingleTableWalWrite {
                            table_name: "test-table".into(),
                        },
                    trigger_settings: v2::TriggerSettings {
                        run_async: true,
                        error_behavior: log::versions::v2::ErrorBehavior::Log,
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
                    node_id: "test-node".into(),
                    plugin_filename: "plugin.py".into(),
                    database_name: "test-db".into(),
                    trigger: log::versions::v2::TriggerSpecificationDefinition::Schedule {
                        schedule: "* 1 * * *".into(),
                    },
                    trigger_settings: v2::TriggerSettings {
                        run_async: false,
                        error_behavior: log::versions::v2::ErrorBehavior::Disable,
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
                    node_id: "test-node".into(),
                    plugin_filename: "plugin.py".into(),
                    database_name: "test-db".into(),
                    trigger: log::versions::v2::TriggerSpecificationDefinition::RequestPath {
                        path: "/my/fancy/api".into(),
                    },
                    trigger_settings: v2::TriggerSettings {
                        run_async: true,
                        error_behavior: log::versions::v2::ErrorBehavior::Retry,
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
                    node_id: "test-node".into(),
                    plugin_filename: "plugin.py".into(),
                    database_name: "test-db".into(),
                    trigger: log::versions::v2::TriggerSpecificationDefinition::Every {
                        duration: Duration::from_secs(1337),
                    },
                    trigger_settings: v2::TriggerSettings {
                        run_async: true,
                        error_behavior: log::versions::v2::ErrorBehavior::Retry,
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
}

#[test]
fn test_deserialize_catalog_file_from_latest() {
    // test a node log file:
    verify_and_deserialize_catalog_file(
        super::serialize_catalog_file(&log::versions::v3::OrderedCatalogBatch {
            catalog_batch: log::versions::v3::CatalogBatch::Node(log::versions::v3::NodeBatch {
                time_ns: 0,
                node_catalog_id: NodeId::new(0),
                node_id: "test-node".into(),
                ops: vec![log::versions::v3::NodeCatalogOp::RegisterNode(
                    log::versions::v3::RegisterNodeLog {
                        node_id: "test-node".into(),
                        instance_id: "uuid".into(),
                        registered_time_ns: 0,
                        core_count: 2,
                        mode: vec![log::versions::v3::NodeMode::Core],
                        // v2 only
                        process_uuid: Uuid::new_v4(),
                    },
                )],
            }),
            sequence_number: CatalogSequenceNumber::new(0),
        })
        .expect("must be able to serialize to latest"),
    )
    .expect("deserialize from latest");

    // test a database log file:
    let log_vlatest = log::versions::v3::OrderedCatalogBatch {
        catalog_batch: log::versions::v3::CatalogBatch::Database(log::versions::v3::DatabaseBatch {
            time_ns: 0,
            database_id: DbId::new(0),
            database_name: "test-db".into(),
            ops: vec![
                log::versions::v3::DatabaseCatalogOp::CreateDatabase(log::versions::v3::CreateDatabaseLog {
                    database_id: DbId::new(0),
                    database_name: "test-db".into(),
                    retention_period: None,
                }),
                log::versions::v3::DatabaseCatalogOp::SoftDeleteDatabase(
                    log::versions::v3::SoftDeleteDatabaseLog {
                        database_id: DbId::new(0),
                        database_name: "test-db".into(),
                        deletion_time: 0,
                        hard_deletion_time: None,
                    },
                ),
                log::versions::v3::DatabaseCatalogOp::CreateTable(log::versions::v3::CreateTableLog {
                    database_id: DbId::new(0),
                    database_name: "test-db".into(),
                    table_name: "test-table".into(),
                    table_id: TableId::new(0),
                    field_definitions: vec![
                        log::versions::v3::FieldDefinition {
                            name: "string".into(),
                            id: ColumnId::new(0),
                            data_type: log::versions::v3::FieldDataType::String,
                        },
                        log::versions::v3::FieldDefinition {
                            name: "int".into(),
                            id: ColumnId::new(1),
                            data_type: log::versions::v3::FieldDataType::Integer,
                        },
                        log::versions::v3::FieldDefinition {
                            name: "uint".into(),
                            id: ColumnId::new(2),
                            data_type: log::versions::v3::FieldDataType::UInteger,
                        },
                        log::versions::v3::FieldDefinition {
                            name: "float".into(),
                            id: ColumnId::new(3),
                            data_type: log::versions::v3::FieldDataType::Float,
                        },
                        log::versions::v3::FieldDefinition {
                            name: "bool".into(),
                            id: ColumnId::new(4),
                            data_type: log::versions::v3::FieldDataType::Boolean,
                        },
                        log::versions::v3::FieldDefinition {
                            name: "tag".into(),
                            id: ColumnId::new(5),
                            data_type: log::versions::v3::FieldDataType::Tag,
                        },
                        log::versions::v3::FieldDefinition {
                            name: "time".into(),
                            id: ColumnId::new(6),
                            data_type: log::versions::v3::FieldDataType::Timestamp,
                        },
                    ],
                    key: vec![ColumnId::new(5)],
                }),
                log::versions::v3::DatabaseCatalogOp::SoftDeleteTable(
                    log::versions::v3::SoftDeleteTableLog {
                        database_id: DbId::new(0),
                        database_name: "test-db".into(),
                        table_id: TableId::new(0),
                        table_name: "test-table".into(),
                        deletion_time: 10,
                        hard_deletion_time: None,
                    },
                ),
                log::versions::v3::DatabaseCatalogOp::AddFields(log::versions::v3::AddFieldsLog {
                    database_name: "test-db".into(),
                    database_id: DbId::new(0),
                    table_name: "test-table".into(),
                    table_id: TableId::new(0),
                    field_definitions: vec![
                        log::versions::v3::FieldDefinition {
                            name: "string".into(),
                            id: ColumnId::new(0),
                            data_type: log::versions::v3::FieldDataType::String,
                        },
                        log::versions::v3::FieldDefinition {
                            name: "int".into(),
                            id: ColumnId::new(1),
                            data_type: log::versions::v3::FieldDataType::Integer,
                        },
                        log::versions::v3::FieldDefinition {
                            name: "uint".into(),
                            id: ColumnId::new(2),
                            data_type: log::versions::v3::FieldDataType::UInteger,
                        },
                        log::versions::v3::FieldDefinition {
                            name: "float".into(),
                            id: ColumnId::new(3),
                            data_type: log::versions::v3::FieldDataType::Float,
                        },
                        log::versions::v3::FieldDefinition {
                            name: "bool".into(),
                            id: ColumnId::new(4),
                            data_type: log::versions::v3::FieldDataType::Boolean,
                        },
                        log::versions::v3::FieldDefinition {
                            name: "tag".into(),
                            id: ColumnId::new(5),
                            data_type: log::versions::v3::FieldDataType::Tag,
                        },
                        log::versions::v3::FieldDefinition {
                            name: "time".into(),
                            id: ColumnId::new(6),
                            data_type: log::versions::v3::FieldDataType::Timestamp,
                        },
                    ],
                }),
                log::versions::v3::DatabaseCatalogOp::CreateDistinctCache(
                    log::versions::v3::DistinctCacheDefinition {
                        table_id: TableId::new(0),
                        table_name: "test-table".into(),
                        cache_id: DistinctCacheId::new(0),
                        cache_name: "test-distinct-cache".into(),
                        column_ids: vec![ColumnId::new(0), ColumnId::new(1)],
                        max_cardinality: log::versions::v3::MaxCardinality::default(),
                        max_age_seconds: log::versions::v3::MaxAge::default(),
                    },
                ),
                log::versions::v3::DatabaseCatalogOp::DeleteDistinctCache(
                    log::versions::v3::DeleteDistinctCacheLog {
                        table_id: TableId::new(0),
                        table_name: "test-table".into(),
                        cache_id: DistinctCacheId::new(0),
                        cache_name: "test-distinct-cache".into(),
                    },
                ),
                log::versions::v3::DatabaseCatalogOp::CreateLastCache(
                    log::versions::v3::LastCacheDefinition {
                        table_id: TableId::new(0),
                        table: "test-table".into(),
                        id: LastCacheId::new(0),
                        name: "test-last-cache".into(),
                        key_columns: vec![ColumnId::new(0), ColumnId::new(1)],
                        value_columns: log::versions::v3::LastCacheValueColumnsDef::AllNonKeyColumns,
                        count: log::versions::v3::LastCacheSize::default(),
                        ttl: log::versions::v3::LastCacheTtl::default(),
                    },
                ),
                log::versions::v3::DatabaseCatalogOp::CreateLastCache(
                    log::versions::v3::LastCacheDefinition {
                        table_id: TableId::new(0),
                        table: "test-table".into(),
                        id: LastCacheId::new(0),
                        name: "test-last-cache".into(),
                        key_columns: vec![ColumnId::new(0), ColumnId::new(1)],
                        value_columns: log::versions::v3::LastCacheValueColumnsDef::Explicit {
                            columns: vec![ColumnId::new(2), ColumnId::new(3)],
                        },
                        count: log::versions::v3::LastCacheSize::default(),
                        ttl: log::versions::v3::LastCacheTtl::default(),
                    },
                ),
                log::versions::v3::DatabaseCatalogOp::DeleteLastCache(
                    log::versions::v3::DeleteLastCacheLog {
                        table_id: TableId::new(0),
                        table_name: "test-table".into(),
                        id: LastCacheId::new(0),
                        name: "test-last-cache".into(),
                    },
                ),
                log::versions::v3::DatabaseCatalogOp::CreateTrigger(log::versions::v3::TriggerDefinition {
                    trigger_id: TriggerId::new(0),
                    trigger_name: "test-trigger".into(),
                    node_id: "test-node".into(),
                    plugin_filename: "plugin.py".into(),
                    database_name: "test-db".into(),
                    trigger: log::versions::v3::TriggerSpecificationDefinition::AllTablesWalWrite,
                    trigger_settings: log::versions::v3::TriggerSettings {
                        run_async: true,
                        error_behavior: log::versions::v3::ErrorBehavior::Retry,
                    },
                    trigger_arguments: Some(
                        [(String::from("k"), String::from("v"))]
                            .into_iter()
                            .collect(),
                    ),
                    disabled: false,
                }),
                log::versions::v3::DatabaseCatalogOp::CreateTrigger(log::versions::v3::TriggerDefinition {
                    trigger_id: TriggerId::new(0),
                    trigger_name: "test-trigger".into(),
                    node_id: "test-node".into(),
                    plugin_filename: "plugin.py".into(),
                    database_name: "test-db".into(),
                    trigger: log::versions::v3::TriggerSpecificationDefinition::SingleTableWalWrite {
                        table_name: "test-table".into(),
                    },
                    trigger_settings: log::versions::v3::TriggerSettings {
                        run_async: true,
                        error_behavior: log::versions::v3::ErrorBehavior::Log,
                    },
                    trigger_arguments: Some(
                        [(String::from("k"), String::from("v"))]
                            .into_iter()
                            .collect(),
                    ),
                    disabled: false,
                }),
                log::versions::v3::DatabaseCatalogOp::CreateTrigger(log::versions::v3::TriggerDefinition {
                    trigger_id: TriggerId::new(0),
                    trigger_name: "test-trigger".into(),
                    node_id: "test-node".into(),
                    plugin_filename: "plugin.py".into(),
                    database_name: "test-db".into(),
                    trigger:
                        log::versions::v3::TriggerSpecificationDefinition::AllTablesWalWrite,
                    trigger_settings: log::versions::v3::TriggerSettings {
                        run_async: false,
                        error_behavior: log::versions::v3::ErrorBehavior::Disable,
                    },
                    trigger_arguments: Some(
                        [(String::from("k"), String::from("v"))]
                            .into_iter()
                            .collect(),
                    ),
                    disabled: false,
                }),
                log::versions::v3::DatabaseCatalogOp::CreateTrigger(log::versions::v3::TriggerDefinition {
                    trigger_id: TriggerId::new(0),
                    trigger_name: "test-trigger".into(),
                    node_id: "test-node".into(),
                    plugin_filename: "plugin.py".into(),
                    database_name: "test-db".into(),
                    trigger:
                        log::versions::v3::TriggerSpecificationDefinition::AllTablesWalWrite,
                    trigger_settings: log::versions::v3::TriggerSettings {
                        run_async: true,
                        error_behavior: log::versions::v3::ErrorBehavior::Retry,
                    },
                    trigger_arguments: Some(
                        [(String::from("k"), String::from("v"))]
                            .into_iter()
                            .collect(),
                    ),
                    disabled: false,
                }),
                log::versions::v3::DatabaseCatalogOp::CreateTrigger(log::versions::v3::TriggerDefinition {
                    trigger_id: TriggerId::new(0),
                    trigger_name: "test-trigger".into(),
                    node_id: "test-node".into(),
                    plugin_filename: "plugin.py".into(),
                    database_name: "test-db".into(),
                    trigger:
                        log::versions::v3::TriggerSpecificationDefinition::AllTablesWalWrite,
                    trigger_settings: log::versions::v3::TriggerSettings {
                        run_async: true,
                        error_behavior: log::versions::v3::ErrorBehavior::Retry,
                    },
                    trigger_arguments: Some(
                        [(String::from("k"), String::from("v"))]
                            .into_iter()
                            .collect(),
                    ),
                    disabled: false,
                }),
                log::versions::v3::DatabaseCatalogOp::DeleteTrigger(log::versions::v3::DeleteTriggerLog {
                    trigger_id: TriggerId::new(0),
                    trigger_name: "test-trigger".into(),
                    force: false,
                }),
                log::versions::v3::DatabaseCatalogOp::EnableTrigger(log::versions::v3::TriggerIdentifier {
                    db_id: DbId::new(0),
                    db_name: "test-db".into(),
                    trigger_id: TriggerId::new(0),
                    trigger_name: "test-trigger".into(),
                }),
                log::versions::v3::DatabaseCatalogOp::DisableTrigger(log::versions::v3::TriggerIdentifier {
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
    let serialized = super::serialize_catalog_file(&log::versions::v3::OrderedCatalogBatch {
        catalog_batch: log::versions::v3::CatalogBatch::Node(log::versions::v3::NodeBatch {
            time_ns: 0,
            node_catalog_id: NodeId::new(0),
            node_id: "test-node".into(),
            ops: vec![log::versions::v3::NodeCatalogOp::RegisterNode(
                log::versions::v3::RegisterNodeLog {
                    node_id: "test-node".into(),
                    instance_id: "uuid".into(),
                    registered_time_ns: 0,
                    core_count: 2,
                    mode: vec![log::versions::v3::NodeMode::Core],
                    // v2 only
                    process_uuid: Uuid::new_v4(),
                },
            )],
        }),
        sequence_number: CatalogSequenceNumber::new(0),
    })
    .expect("must be able to serialize to latest");

    assert!(
        serialized.starts_with(&log::versions::v3::OrderedCatalogBatch::VERSION_ID),
        "serialized catalog log file must always start with latest verison identifier"
    );
}

#[test]
fn test_serialize_catalog_snapshot_with_latest_identifier() {
    let snapshot = crate::snapshot::versions::v1::CatalogSnapshot::generate();
    let snapshot: crate::snapshot::versions::v2::CatalogSnapshot = snapshot.into();
    let snapshot: crate::snapshot::versions::v3::CatalogSnapshot = snapshot.into();
    let serialized = super::serialize_catalog_file(&snapshot)
        .expect("must be able to serialize generated snapshot");

    assert!(
        serialized.starts_with(&crate::snapshot::versions::v3::CatalogSnapshot::VERSION_ID),
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
