use anyhow::Context;
use bytes::{Bytes, BytesMut};
use futures::StreamExt;
use futures::stream::FuturesOrdered;
use object_store::ObjectStore;
use observability_deps::tracing::{debug, trace};
use serde::Serialize;
use std::sync::Arc;

type Result<T> = std::result::Result<T, ObjectStoreCatalogError>;

use crate::catalog::versions::v1::{
    InnerCatalog, Snapshot,
    log::{self},
    snapshot::CatalogSnapshot,
};
use crate::object_store::ObjectStoreCatalogError;
use crate::object_store::versions::v1::CatalogFilePath;
use crate::serialize::{VersionedFileType, versions};

const CHECKSUM_LEN: usize = size_of::<u32>();

pub async fn load_catalog(
    prefix: Arc<str>,
    store: Arc<dyn ObjectStore>,
) -> Result<Option<InnerCatalog>> {
    // get the checkpoint to initialize the catalog:
    let mut inner_catalog = match store.get(&CatalogFilePath::checkpoint(&prefix)).await {
        Ok(get_result) => {
            let bytes = get_result.bytes().await.context("Failed to read catalog")?;
            let snapshot = verify_and_deserialize_catalog_checkpoint_file(bytes).context(
                "there was a catalog checkpoint file on object store, but it \
                        could not be verified and deserialized",
            )?;
            InnerCatalog::from_snapshot(snapshot)
        }
        // there should always be a checkpoint if the server and catalog was successfully
        // initialized:
        Err(object_store::Error::NotFound { .. }) => return Ok(None),
        Err(error) => return Err(error.into()),
    };

    // fetch catalog files after the checkpoint
    //
    // catalog files are listed with offset and sorted for every group of listed items that
    // are added in the loop iteration; ideally, only one iteration would be needed if the
    // catalog is checkpointed frequently enough.
    let sequence_start = inner_catalog.sequence_number();
    let mut offset = CatalogFilePath::log(&prefix, sequence_start).into();
    let mut catalog_file_metas = Vec::new();
    loop {
        let mut list_items = store.list_with_offset(Some(&CatalogFilePath::dir(&prefix)), &offset);

        let mut objects = Vec::new();
        while let Some(item) = list_items.next().await {
            objects.push(item?);
        }
        if objects.is_empty() {
            break;
        }
        catalog_file_metas.append(&mut objects);
        catalog_file_metas.sort_unstable_by(|a, b| a.location.cmp(&b.location));

        if let Some(last) = catalog_file_metas.last() {
            offset = last.location.clone();
        } else {
            break;
        }
    }

    let mut futures = FuturesOrdered::from_iter(catalog_file_metas.into_iter().map(async |meta| {
        let bytes = store.get(&meta.location).await?.bytes().await?;
        verify_and_deserialize_catalog_file(bytes)
    }));

    let mut catalog_files = Vec::new();
    while let Some(result) = futures.next().await {
        match result {
            Ok(log) => catalog_files.push(log),
            Err(ObjectStoreCatalogError::UpgradedLog) => {
                // NOTE: this log entry is not added to catalog_files,
                // and therefore does not update the sequence number of the
                // InnerCatalog. Any attempt to write another log entry will fail with
                // PersistCatalogResult::AlreadyExists, ensuring the v1 catalog can no longer be
                // changed.
                inner_catalog.has_upgraded = true;
                break;
            }
            Err(err) => return Err(err),
        }
    }

    debug!(
        n_catalog_files = catalog_files.len(),
        "loaded catalog files since last checkpoint"
    );

    for ordered_catalog_batch in catalog_files {
        debug!(
            sequence = ordered_catalog_batch.sequence_number().get(),
            "processing catalog file"
        );
        trace!(?ordered_catalog_batch, "processing catalog file");
        inner_catalog
            .apply_catalog_batch(
                ordered_catalog_batch.batch(),
                ordered_catalog_batch.sequence_number(),
            )
            .context("failed to apply persisted catalog batch")?;
    }
    debug!("loaded the catalog");
    trace!(loaded_catalog = ?inner_catalog, "loaded the catalog");
    Ok(Some(inner_catalog))
}

pub(crate) fn verify_and_deserialize_catalog_file(
    bytes: Bytes,
) -> Result<log::OrderedCatalogBatch> {
    let version_id: &[u8; 10] = bytes
        .first_chunk()
        .ok_or(ObjectStoreCatalogError::unexpected(
            "file must contain at least 10 bytes",
        ))?;

    match *version_id {
        // Version 1 uses the `bitcode` crate for serialization/deserialization
        crate::log::versions::v1::OrderedCatalogBatch::VERSION_ID => {
            // V1 Deserialization:
            let checksum = bytes.slice(10..10 + CHECKSUM_LEN);
            let data = bytes.slice(10 + CHECKSUM_LEN..);
            versions::verify_checksum(&checksum, &data)?;
            let log = bitcode::deserialize::<crate::log::versions::v1::OrderedCatalogBatch>(&data)
                .context("failed to deserialize v1 catalog log file contents")?;

            // explicit type annotations are needed once you start chaining `.into` (something to check
            // later to see if that could be avoided, then this can just be a loop with starting and
            // end point based on current log file's version)
            let log_v2: crate::log::versions::v2::OrderedCatalogBatch = log.into();
            let log_v3: crate::log::versions::v3::OrderedCatalogBatch = log_v2.into();
            Ok(log_v3)
        }
        // Version 2 uses the `serde_json` crate for serialization/deserialization
        crate::log::versions::v2::OrderedCatalogBatch::VERSION_ID => {
            // V2 Deserialization:
            let checksum = bytes.slice(10..10 + CHECKSUM_LEN);
            let data = bytes.slice(10 + CHECKSUM_LEN..);
            versions::verify_checksum(&checksum, &data)?;
            let log =
                serde_json::from_slice::<crate::log::versions::v2::OrderedCatalogBatch>(&data)
                    .context("failed to deserialize v2 catalog log file contents")?;
            Ok(log.into())
        }
        // Version 3 added a conversion function to map db:*:write tokens to be db:*:create,write
        // tokens, and because it's a one time migration it relies on the version in file the
        // version has to be updated.
        crate::log::versions::v3::OrderedCatalogBatch::VERSION_ID => {
            // V3 Deserialization:
            let checksum = bytes.slice(10..10 + CHECKSUM_LEN);
            let data = bytes.slice(10 + CHECKSUM_LEN..);
            versions::verify_checksum(&checksum, &data)?;
            let log =
                serde_json::from_slice::<crate::log::versions::v3::OrderedCatalogBatch>(&data)
                    .context("failed to deserialize v3 catalog log file contents")?;
            Ok(log)
        }
        crate::log::UpgradedLog::VERSION_ID => Err(ObjectStoreCatalogError::UpgradedLog),
        _ => Err(ObjectStoreCatalogError::unexpected(
            "unrecognized catalog file format",
        )),
    }
}

pub(crate) fn verify_and_deserialize_catalog_checkpoint_file(
    bytes: Bytes,
) -> Result<CatalogSnapshot> {
    let version_id: &[u8; 10] = bytes
        .first_chunk()
        .ok_or(ObjectStoreCatalogError::unexpected(
            "file must contain at least 10 bytes",
        ))?;

    match *version_id {
        // Version 1 uses the `bitcode` crate for serialization/deserialization
        crate::snapshot::versions::v1::CatalogSnapshot::VERSION_ID => {
            let checksum = bytes.slice(10..10 + CHECKSUM_LEN);
            let data = bytes.slice(10 + CHECKSUM_LEN..);
            versions::verify_checksum(&checksum, &data)?;
            let snapshot =
                bitcode::deserialize::<crate::snapshot::versions::v1::CatalogSnapshot>(&data);
            let snapshot =
                snapshot.context("failed to deserialize v1 catalog snapshot file contents")?;
            let snapshot_v2: crate::snapshot::versions::v2::CatalogSnapshot = snapshot.into();
            let snapshot_v3: crate::snapshot::versions::v3::CatalogSnapshot = snapshot_v2.into();
            Ok(snapshot_v3)
        }
        // Version 2 uses the `serde_json` crate for serialization/deserialization
        crate::snapshot::versions::v2::CatalogSnapshot::VERSION_ID => {
            let checksum = bytes.slice(10..10 + CHECKSUM_LEN);
            let data = bytes.slice(10 + CHECKSUM_LEN..);
            versions::verify_checksum(&checksum, &data)?;
            let snapshot: crate::snapshot::versions::v2::CatalogSnapshot =
                serde_json::from_slice(&data)
                    .context("failed to deserialize v2 catalog snapshot file contents")?;
            Ok(snapshot.into())
        }
        // Version 3 added a conversion function to map db:*:write tokens to be db:*:create,write
        // tokens, and because it's a one time migration it relies on the version in file the
        // version has to be updated.
        crate::snapshot::versions::v3::CatalogSnapshot::VERSION_ID => {
            let checksum = bytes.slice(10..10 + CHECKSUM_LEN);
            let data = bytes.slice(10 + CHECKSUM_LEN..);
            versions::verify_checksum(&checksum, &data)?;
            let snapshot: crate::snapshot::versions::v3::CatalogSnapshot =
                serde_json::from_slice(&data)
                    .context("failed to deserialize v3 catalog snapshot file contents")?;
            Ok(snapshot)
        }
        _ => Err(ObjectStoreCatalogError::unexpected(
            "unrecognized catalog checkpoint file format",
        )),
    }
}

pub fn serialize_catalog_file<T: Serialize + VersionedFileType>(file: &T) -> Result<Bytes> {
    let mut buf = BytesMut::new();
    buf.extend_from_slice(&T::VERSION_ID);

    let data = match T::VERSION_ID {
        crate::snapshot::versions::v1::CatalogSnapshot::VERSION_ID
        | crate::log::versions::v1::OrderedCatalogBatch::VERSION_ID => {
            bitcode::serialize(file).context("failed to serialize catalog file")?
        }
        _ => serde_json::to_vec(file).context("failed to serialize catalog file")?,
    };

    Ok(versions::hash_and_freeze(buf, data))
}

/// Test the conversion from `v1` catalog format, which used bitcode serialization/deserialization
#[cfg(test)]
mod v1_tests {
    use std::time::Duration;

    use influxdb3_id::{ColumnId, DbId, DistinctCacheId, LastCacheId, NodeId, TableId, TriggerId};
    use uuid::Uuid;

    use crate::{
        catalog::CatalogSequenceNumber,
        log::{
            self,
            versions::{
                v1::{
                    AddFieldsLog, CreateDatabaseLog, CreateTableLog, DatabaseBatch,
                    DatabaseCatalogOp, DeleteDistinctCacheLog, DeleteLastCacheLog,
                    DeleteTriggerLog, DistinctCacheDefinition, FieldDefinition,
                    LastCacheDefinition, LastCacheSize, LastCacheTtl, MaxAge, MaxCardinality,
                    NodeBatch, NodeCatalogOp, NodeMode, OrderedCatalogBatch, RegisterNodeLog,
                    SoftDeleteDatabaseLog, SoftDeleteTableLog, TriggerDefinition,
                    TriggerIdentifier, TriggerSettings,
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
                        value_columns:
                            log::versions::v2::LastCacheValueColumnsDef::AllNonKeyColumns,
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
                        trigger:
                            log::versions::v2::TriggerSpecificationDefinition::AllTablesWalWrite,
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
                catalog_batch: log::versions::v3::CatalogBatch::Node(
                    log::versions::v3::NodeBatch {
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
                    },
                ),
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
            super::serialize_catalog_file(&log_vlatest)
                .expect("must be able to serialize to latest"),
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
            serialize_catalog_file(&CatalogSnapshot::generate())
                .expect("must be able to serialize"),
        );
        println!("{result:?}");
        result.expect("deserialize from v1");
    }
}
