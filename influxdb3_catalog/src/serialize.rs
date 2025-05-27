use std::io::Cursor;

use anyhow::Context;
use byteorder::{BigEndian, ReadBytesExt};
use bytes::{Bytes, BytesMut};

use crate::{
    CatalogError, Result,
    log::{self, OrderedCatalogBatch},
    snapshot::{self, CatalogSnapshot},
};

const CHECKSUM_LEN: usize = size_of::<u32>();

pub fn verify_and_deserialize_catalog_file(
    bytes: Bytes,
) -> Result<log::versions::v3::OrderedCatalogBatch> {
    if bytes.starts_with(LOG_FILE_TYPE_IDENTIFIER_V1) {
        // V1 Deserialization:
        let id_len = LOG_FILE_TYPE_IDENTIFIER_V1.len();
        let checksum = bytes.slice(id_len..id_len + CHECKSUM_LEN);
        let data = bytes.slice(id_len + CHECKSUM_LEN..);
        verify_checksum(&checksum, &data)?;
        let log = bitcode::deserialize::<log::versions::v1::OrderedCatalogBatch>(&data)
            .context("failed to deserialize v1 catalog log file contents")?;

        // explicit type annotations are needed once you start chaining `.into` (something to check
        // later to see if that could be avoided, then this can just be a loop with starting and
        // end point based on current log file's version)
        let log_v2: log::versions::v2::OrderedCatalogBatch = log.into();
        let log_v3: log::versions::v3::OrderedCatalogBatch = log_v2.into();
        Ok(log_v3)
    } else if bytes.starts_with(LOG_FILE_TYPE_IDENTIFIER_V2) {
        // V2 Deserialization:
        let id_len = LOG_FILE_TYPE_IDENTIFIER_V2.len();
        let checksum = bytes.slice(id_len..id_len + CHECKSUM_LEN);
        let data = bytes.slice(id_len + CHECKSUM_LEN..);
        verify_checksum(&checksum, &data)?;
        let log = serde_json::from_slice::<log::versions::v2::OrderedCatalogBatch>(&data)
            .context("failed to deserialize v2 catalog log file contents")?;
        Ok(log.into())
    } else if bytes.starts_with(LOG_FILE_TYPE_IDENTIFIER_V3) {
        // V3 Deserialization:
        let id_len = LOG_FILE_TYPE_IDENTIFIER_V3.len();
        let checksum = bytes.slice(id_len..id_len + CHECKSUM_LEN);
        let data = bytes.slice(id_len + CHECKSUM_LEN..);
        verify_checksum(&checksum, &data)?;
        let log = serde_json::from_slice::<log::versions::v3::OrderedCatalogBatch>(&data)
            .context("failed to deserialize v3 catalog log file contents")?;
        Ok(log)
    } else {
        Err(CatalogError::unexpected("unrecognized catalog file format"))
    }
}

pub fn verify_and_deserialize_catalog_checkpoint_file(bytes: Bytes) -> Result<CatalogSnapshot> {
    if bytes.starts_with(SNAPSHOT_FILE_TYPE_IDENTIFIER_V1) {
        let id_len = SNAPSHOT_FILE_TYPE_IDENTIFIER_V1.len();
        let checksum = bytes.slice(id_len..id_len + CHECKSUM_LEN);
        let data = bytes.slice(id_len + CHECKSUM_LEN..);
        verify_checksum(&checksum, &data)?;
        let snapshot = bitcode::deserialize::<snapshot::versions::v1::CatalogSnapshot>(&data)
            .context("failed to deserialize catalog snapshot file contents")?;
        let snapshot_v2: snapshot::versions::v2::CatalogSnapshot = snapshot.into();
        let snapshot_v3: snapshot::versions::v3::CatalogSnapshot = snapshot_v2.into();
        Ok(snapshot_v3)
    } else if bytes.starts_with(SNAPSHOT_FILE_TYPE_IDENTIFIER_V2) {
        let id_len = SNAPSHOT_FILE_TYPE_IDENTIFIER_V2.len();
        let checksum = bytes.slice(id_len..id_len + CHECKSUM_LEN);
        let data = bytes.slice(id_len + CHECKSUM_LEN..);
        verify_checksum(&checksum, &data)?;
        let snapshot: snapshot::versions::v2::CatalogSnapshot = serde_json::from_slice(&data)
            .context("failed to deserialize catalog snapshot file contents")?;
        Ok(snapshot.into())
    } else if bytes.starts_with(SNAPSHOT_FILE_TYPE_IDENTIFIER_V3) {
        let id_len = SNAPSHOT_FILE_TYPE_IDENTIFIER_V3.len();
        let checksum = bytes.slice(id_len..id_len + CHECKSUM_LEN);
        let data = bytes.slice(id_len + CHECKSUM_LEN..);
        verify_checksum(&checksum, &data)?;
        let snapshot: snapshot::versions::v3::CatalogSnapshot = serde_json::from_slice(&data)
            .context("failed to deserialize catalog snapshot file contents")?;
        Ok(snapshot)
    } else {
        Err(CatalogError::unexpected(
            "unrecognized catalog checkpoint file format",
        ))
    }
}

fn verify_checksum(checksum: &[u8], data: &[u8]) -> Result<()> {
    let mut cursor = Cursor::new(checksum);
    let crc32_checksum = cursor
        .read_u32::<BigEndian>()
        .expect("read big endian u32 checksum");
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(data);
    let checksum = hasher.finalize();
    if checksum != crc32_checksum {
        return Err(CatalogError::unexpected(
            "crc 32 checksum mismatch when deserializing catalog log file",
        ));
    }
    Ok(())
}

/// Version 1 uses the `bitcode` crate for serialization/deserialization
const LOG_FILE_TYPE_IDENTIFIER_V1: &[u8] = b"idb3.001.l";
/// Version 2 uses the `serde_json` crate for serialization/deserialization
const LOG_FILE_TYPE_IDENTIFIER_V2: &[u8] = b"idb3.002.l";
/// Version 3 introduced to migration db write permission in pro
const LOG_FILE_TYPE_IDENTIFIER_V3: &[u8] = b"idb3.003.l";

pub fn serialize_catalog_log(log: &OrderedCatalogBatch) -> Result<Bytes> {
    let mut buf = BytesMut::new();
    buf.extend_from_slice(LOG_FILE_TYPE_IDENTIFIER_V2);

    let data = serde_json::to_vec(log).context("failed to serialize catalog log file")?;

    Ok(hash_and_freeze(buf, data))
}

/// Version 1 uses the `bitcode` crate for serialization/deserialization
const SNAPSHOT_FILE_TYPE_IDENTIFIER_V1: &[u8] = b"idb3.001.s";
/// Version 2 uses the `serde_json` crate for serialization/deserialization
const SNAPSHOT_FILE_TYPE_IDENTIFIER_V2: &[u8] = b"idb3.002.s";
/// Version 3 introduced to migration db write permission in pro
const SNAPSHOT_FILE_TYPE_IDENTIFIER_V3: &[u8] = b"idb3.003.s";

pub fn serialize_catalog_snapshot(snapshot: &CatalogSnapshot) -> Result<Bytes> {
    let mut buf = BytesMut::new();
    buf.extend_from_slice(SNAPSHOT_FILE_TYPE_IDENTIFIER_V2);

    let data = serde_json::to_vec(snapshot).context("failed to serialize catalog snapshot file")?;

    Ok(hash_and_freeze(buf, data))
}

fn hash_and_freeze(mut buf: BytesMut, data: Vec<u8>) -> Bytes {
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(&data);
    let checksum = hasher.finalize();

    buf.extend_from_slice(&checksum.to_be_bytes());
    buf.extend_from_slice(&data);

    buf.freeze()
}

/// Test the conversion from `v1` catalog format, which used bitcode serialization/deserialization
#[cfg(test)]
mod v1_tests {
    use std::time::Duration;

    use bytes::{Bytes, BytesMut};
    use influxdb3_id::{ColumnId, DbId, DistinctCacheId, LastCacheId, NodeId, TableId, TriggerId};

    use crate::{
        catalog::CatalogSequenceNumber,
        log::{
            self,
            versions::v1::{
                AddFieldsLog, CreateDatabaseLog, CreateTableLog, DatabaseBatch, DatabaseCatalogOp,
                DeleteDistinctCacheLog, DeleteLastCacheLog, DeleteTriggerLog,
                DistinctCacheDefinition, FieldDefinition, LastCacheDefinition, LastCacheSize,
                LastCacheTtl, MaxAge, MaxCardinality, NodeBatch, NodeCatalogOp, NodeMode,
                OrderedCatalogBatch, RegisterNodeLog, SoftDeleteDatabaseLog, SoftDeleteTableLog,
                TriggerDefinition, TriggerIdentifier, TriggerSettings,
            },
        },
        snapshot::{
            self,
            versions::v1::{CatalogSnapshot, test_util::Generate},
        },
    };

    use super::{
        LOG_FILE_TYPE_IDENTIFIER_V1, SNAPSHOT_FILE_TYPE_IDENTIFIER_V1, hash_and_freeze,
        verify_and_deserialize_catalog_checkpoint_file, verify_and_deserialize_catalog_file,
    };

    /// Method that uses v1 serialization logic for catalog files
    fn serialize_catalog_log_v1(log: &log::versions::v1::OrderedCatalogBatch) -> Bytes {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(LOG_FILE_TYPE_IDENTIFIER_V1);

        let data = bitcode::serialize(log).expect("failed to serialize catalog log file");

        hash_and_freeze(buf, data)
    }

    /// Test that uses the main `verify_and_deserialize_catalog_file` method which deseriales a
    /// versioned catalog file into the latest version, to test round-trip serialize/deserialize
    /// v1 catalog log files.
    ///
    /// This just manually constructs a big catalog batch with all the different log types.
    #[test]
    fn test_deserialize_catalog_file_from_v1() {
        // test a node log file:
        verify_and_deserialize_catalog_file(serialize_catalog_log_v1(&OrderedCatalogBatch {
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
        }))
        .expect("deserialize from v1");

        // test a database log file:
        verify_and_deserialize_catalog_file(serialize_catalog_log_v1(&OrderedCatalogBatch {
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
        }))
        .expect("deserialize from v1 to latest");
    }

    /// Method that uses v1 serialization logic for catalog checkpoint/snapshot files
    fn serialize_catalog_snapshot_v1(snapshot: &snapshot::versions::v1::CatalogSnapshot) -> Bytes {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(SNAPSHOT_FILE_TYPE_IDENTIFIER_V1);

        let data = bitcode::serialize(snapshot).expect("failed to serialize catalog snapshot file");

        hash_and_freeze(buf, data)
    }

    #[test]
    fn test_deserialize_catalog_checkpoint_file_from_v1() {
        verify_and_deserialize_catalog_checkpoint_file(serialize_catalog_snapshot_v1(
            &CatalogSnapshot::generate(),
        ))
        .expect("deserialize from v1");
    }
}
