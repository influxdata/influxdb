//! Module for the compacted catalog, which is a unified view of the catalgos across all writers
//! that have been compacted.

use anyhow::Context;
use futures_util::StreamExt;
use hashbrown::HashMap;
use influxdb3_catalog::catalog::{
    enterprise::CatalogIdMap, Catalog, CatalogSequenceNumber, DatabaseSchema, InnerCatalog,
};
use influxdb3_enterprise_data_layout::persist::get_bytes_at_path;
use influxdb3_id::{DbId, SerdeVecMap};
use influxdb3_wal::SnapshotSequenceNumber;
use influxdb3_write::paths::CatalogFilePath;
use influxdb3_write::persister::Persister;
use influxdb3_write::{DatabaseTables, PersistedSnapshot};
use iox_time::TimeProvider;
use object_store::path::Path as ObjPath;
use object_store::ObjectStore;
use observability_deps::tracing::log::error;
use observability_deps::tracing::{debug, info};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::{sync::Arc, time::Instant};
use thiserror::Error;
use uuid::Uuid;

use crate::sys_events::{catalog_fetched, CompactionEventStore};

#[derive(Debug, Error)]
pub enum Error {
    #[error("Error loading compacted catalog: {0}")]
    LoadError(#[from] object_store::Error),
    #[error("Error serializing compacted catalog: {0}")]
    SerializationError(#[from] serde_json::Error),
    #[error("Error loading writer catalog: {0}")]
    WriterCatalogLoadError(#[from] influxdb3_write::persister::Error),
    #[error("Error merging writer catalogs: {0}")]
    MergeError(#[from] influxdb3_catalog::catalog::Error),
    #[error("Unknown error: {0}")]
    Unexpected(#[from] anyhow::Error),
}

/// Result type for functions in this module.
pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug)]
pub struct CompactedCatalog {
    pub compactor_id: Arc<str>,
    inner: RwLock<InnerCompactedCatalog>,
}

#[derive(Debug)]
struct InnerCompactedCatalog {
    /// The catalog that is the union of all the writer catalogs
    catalog: Arc<Catalog>,
    /// Map of node id to id-to-id map into this catalog
    node_maps: HashMap<String, NodeCatalog>,
}

// First, create a helper struct for serialization
#[derive(Serialize, Deserialize)]
struct CompactedCatalogHelper {
    compactor_id: String,
    catalog: InnerCatalog,
    node_maps: HashMap<String, NodeCatalog>,
}

// Implement Serialize for CompactedCatalog
impl Serialize for CompactedCatalog {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let inner = self.inner.read();

        // Create the helper struct
        let helper = CompactedCatalogHelper {
            compactor_id: self.compactor_id.to_string(),
            catalog: inner.catalog.clone_inner(),
            // We need to block here to get the mutex contents
            node_maps: inner.node_maps.clone(),
        };
        helper.serialize(serializer)
    }
}

// Implement Deserialize for CompactedCatalog
impl<'de> Deserialize<'de> for CompactedCatalog {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        // Deserialize into our helper struct first
        let helper = CompactedCatalogHelper::deserialize(deserializer)?;

        Ok(CompactedCatalog {
            compactor_id: helper.compactor_id.into(),
            inner: RwLock::new(InnerCompactedCatalog {
                catalog: Arc::new(Catalog::from_inner(helper.catalog)),
                node_maps: helper.node_maps,
            }),
        })
    }
}

impl CompactedCatalog {
    pub fn catalog(&self) -> Arc<Catalog> {
        Arc::clone(&self.inner.read().catalog)
    }

    pub fn map_persisted_snapshot_contents(
        &self,
        persisted_snapshot: PersistedSnapshot,
    ) -> Result<PersistedSnapshot> {
        let inner = self.inner.read();
        let node_id_map = inner
            .node_maps
            .get(&persisted_snapshot.node_id)
            .context("writer id not found in catalog")?;
        map_snapshot_contents(&node_id_map.catalog_id_map, persisted_snapshot)
    }

    /// Load the `CompactedCatalog` from the object store.
    pub async fn load(
        compactor_id: Arc<str>,
        object_store: Arc<dyn ObjectStore>,
    ) -> Result<Option<CompactedCatalog>> {
        let path = ObjPath::from(format!("{}/catalog", compactor_id));
        let mut stream = object_store.list(Some(&path));
        let mut latest = None;

        // go through until we find the last one
        while let Some(entry) = stream.next().await {
            let entry = entry?;
            latest = Some(entry);
        }

        match latest {
            None => Ok(None),
            Some(entry) => {
                let bytes = object_store.get(&entry.location).await?.bytes().await?;
                let catalog = serde_json::from_slice(&bytes)?;
                info!(catalog = ?catalog, "Loaded compacted catalog");
                Ok(Some(catalog))
            }
        }
    }

    /// Reloads the compacted catalog from object store if the passed in sequence number is greater
    /// than the last sequence number for the compacted catalog.
    pub async fn reload_if_needed(
        &self,
        sequence_number: CatalogSequenceNumber,
        object_store: Arc<dyn ObjectStore>,
    ) -> Result<()> {
        let last_sequence_number = self.sequence_number();
        if sequence_number > last_sequence_number {
            info!(
                last_sequence_number = last_sequence_number.as_u32(),
                compaction_summary_sequence_number = sequence_number.as_u32(),
                "reloading compactor catalog"
            );
            let path = CompactedCatalogPath::new(self.compactor_id.as_ref(), sequence_number);
            let bytes = object_store.get(&path.0).await?.bytes().await?;
            let helper: CompactedCatalogHelper = serde_json::from_slice(&bytes)?;
            let mut inner = self.inner.write();
            inner.catalog = Arc::new(Catalog::from_inner(helper.catalog));
            inner.node_maps = helper.node_maps;
        }
        Ok(())
    }

    /// Load the `CompactedCatalog` from the object store using the provided compactor id and sequence number.
    pub async fn load_from_id(
        compactor_id: &str,
        sequence_number: CatalogSequenceNumber,
        object_store: Arc<dyn ObjectStore>,
    ) -> Result<Option<CompactedCatalog>> {
        let path = CompactedCatalogPath::new(compactor_id, sequence_number);
        let bytes = object_store.get(&path.0).await?.bytes().await?;
        let catalog = serde_json::from_slice(&bytes)?;
        debug!(catalog = ?catalog, "Loaded compacted catalog");
        Ok(Some(catalog))
    }

    pub async fn persist(&self, object_store: Arc<dyn ObjectStore>) -> Result<()> {
        let path = CompactedCatalogPath::new(self.compactor_id.as_ref(), self.sequence_number());
        let bytes = serde_json::to_vec(&self)?;
        object_store.put(&path.0, bytes.into()).await?;
        info!(path = %path.0, "Persisted compacted catalog");
        Ok(())
    }

    pub fn sequence_number(&self) -> CatalogSequenceNumber {
        self.inner.read().catalog.sequence_number()
    }

    pub fn db_schema(&self, db_name: &str) -> Option<Arc<DatabaseSchema>> {
        self.inner.read().catalog.db_schema(db_name)
    }

    pub fn db_schema_by_id(&self, db_id: &DbId) -> Option<Arc<DatabaseSchema>> {
        self.inner.read().catalog.db_schema_by_id(db_id)
    }

    /// Updates the catalog id maps and persists the new catalog if any of the passed in markers
    /// have a sequence number greater than the last sequence number for the writer.
    pub async fn update_from_markers(
        &self,
        catalog_snapshot_markers: Vec<CatalogSnapshotMarker>,
        object_store: Arc<dyn ObjectStore>,
        sys_events_store: Arc<dyn CompactionEventStore>,
    ) -> Result<()> {
        // get the catalog snapshot markers we need to update because they have a higher sequence number
        let catalogs_to_update = {
            let inner = self.inner.read();
            catalog_snapshot_markers
                .iter()
                .filter(|marker| {
                    let last_sequence_number = inner
                        .node_maps
                        .get(&marker.node_id)
                        .map(|h| h.last_catalog_sequence_number)
                        .unwrap_or(CatalogSequenceNumber::new(0));
                    marker.catalog_sequence_number > last_sequence_number
                })
                .collect::<Vec<_>>()
        };

        if catalogs_to_update.is_empty() {
            return Ok(());
        }

        // now fetch all the catalogs while we're not holding any locks
        let mut updated_node_maps = HashMap::new();

        for marker in catalogs_to_update {
            let start = Instant::now();
            let catalog_path =
                CatalogFilePath::new(&marker.node_id, marker.catalog_sequence_number);
            let Some(updated_catalog) =
                get_bytes_at_path(catalog_path.as_ref(), Arc::clone(&object_store)).await
            else {
                error!("Catalog not found at path: {}", catalog_path.to_string());
                continue;
            };
            let catalog = Arc::new(Catalog::from_inner(serde_json::from_slice(
                &updated_catalog,
            )?));

            let catalog_id_map =
                self.merge_catalog_and_record_error(&catalog, marker, &sys_events_store)?;
            updated_node_maps.insert(
                catalog.node_id(),
                NodeCatalog {
                    last_catalog_sequence_number: catalog.sequence_number(),
                    catalog_id_map,
                },
            );
            info!(node_id = %catalog.node_id(), sequence = %catalog.sequence_number().as_u32(), "Updated writer catalog");
            let event = catalog_fetched::SuccessInfo {
                node_id: catalog.node_id(),
                catalog_sequence_number: catalog.sequence_number().as_u32(),
                duration: start.elapsed(),
            };
            sys_events_store.record_catalog_success(event);
        }

        {
            // and update the writer maps
            let mut inner = self.inner.write();
            for (writer, updated_node_catalog) in updated_node_maps {
                inner
                    .node_maps
                    .insert(writer.to_string(), updated_node_catalog);
            }
        }

        //  and persist
        self.persist(Arc::clone(&object_store)).await?;

        Ok(())
    }

    fn merge_catalog_and_record_error(
        &self,
        catalog: &Arc<Catalog>,
        marker: &CatalogSnapshotMarker,
        sys_events_store: &Arc<dyn CompactionEventStore>,
    ) -> Result<CatalogIdMap, Error> {
        let start = Instant::now();
        let catalog_id_map = self
            .inner
            .read()
            .catalog
            .merge(Arc::clone(catalog))
            .map_err(crate::catalog::Error::MergeError)
            .inspect_err(|err| {
                if let crate::catalog::Error::MergeError(error) = err {
                    let error_msg = error.to_string();
                    let failed_event = catalog_fetched::FailedInfo {
                        node_id: Arc::from(marker.node_id.as_str()),
                        sequence_number: marker.snapshot_sequence_number.as_u64(),
                        error: error_msg,
                        duration: start.elapsed(),
                    };
                    sys_events_store.record_catalog_failed(failed_event);
                }
            })?;
        Ok(catalog_id_map)
    }

    /// Loads the most recently persisted catalog from each of the writers. Returns a new catalog
    /// that is the union of all the writer catalogs. If none of the writers has persisted a catalog,
    /// returns a newly initialized one.
    pub async fn load_merged_from_node_ids(
        compactor_id: Arc<str>,
        node_ids: Vec<String>,
        object_store: Arc<dyn ObjectStore>,
        time_provider: Arc<dyn TimeProvider>,
    ) -> Result<CompactedCatalog> {
        let uuid = Uuid::new_v4().to_string();
        let instance_id = Arc::from(uuid.as_str());

        let primary_catalog = Catalog::new(Arc::clone(&compactor_id), instance_id);
        let mut node_maps = HashMap::new();

        for node_id in node_ids {
            let persister = Persister::new(
                Arc::clone(&object_store),
                &node_id,
                Arc::clone(&time_provider),
            );
            if let Some(catalog) = persister.load_catalog().await? {
                let last_catalog_sequence_number = catalog.sequence_number();
                let catalog = Arc::new(Catalog::from_inner(catalog));
                let catalog_id_map = primary_catalog.merge(catalog)?;

                info!(%node_id, "Loaded writer catalog");
                node_maps.insert(
                    node_id,
                    NodeCatalog {
                        last_catalog_sequence_number,
                        catalog_id_map,
                    },
                );
            } else {
                info!(%node_id, "No writer catalog found");
                node_maps.insert(
                    node_id,
                    NodeCatalog {
                        last_catalog_sequence_number: CatalogSequenceNumber::new(0),
                        catalog_id_map: CatalogIdMap::default(),
                    },
                );
            }
        }

        Ok(Self {
            compactor_id,
            inner: RwLock::new(InnerCompactedCatalog {
                catalog: Arc::new(primary_catalog),
                node_maps,
            }),
        })
    }

    /// Create a compacted catalog that is empty and can be used in mocks/tests
    pub fn new_for_testing(id: impl Into<Arc<str>>, catalog: Arc<Catalog>) -> Self {
        Self {
            compactor_id: id.into(),
            inner: RwLock::new(InnerCompactedCatalog {
                catalog,
                node_maps: Default::default(),
            }),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
struct NodeCatalog {
    last_catalog_sequence_number: CatalogSequenceNumber,
    catalog_id_map: CatalogIdMap,
}

#[derive(Debug, Eq, PartialEq)]
pub struct CatalogSnapshotMarker {
    pub snapshot_sequence_number: SnapshotSequenceNumber,
    pub catalog_sequence_number: CatalogSequenceNumber,
    pub node_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompactedCatalogPath(ObjPath);

impl CompactedCatalogPath {
    pub fn new(compactor_id: &str, sequence_number: CatalogSequenceNumber) -> Self {
        Self(ObjPath::from(format!(
            "{}/catalog/{:010}.json",
            compactor_id,
            sequence_number.as_u32()
        )))
    }

    pub fn as_path(&self) -> &ObjPath {
        &self.0
    }
}

/// Converts the snapshot to have the IDs in the map.
/// TODO: This is copied from replica.rs. Ideally, we'd have this as a function on CatalogIdMap,
///       but that would require moving PersistedSnapshot to a place wheree it could pull it in
///       (like the wal crate).
fn map_snapshot_contents(
    id_map: &CatalogIdMap,
    snapshot: PersistedSnapshot,
) -> Result<PersistedSnapshot> {
    Ok(PersistedSnapshot {
        databases: snapshot
            .databases
            .into_iter()
            .map(|(replica_db_id, db_tables)| {
                let local_db_id = id_map.map_db_id(&replica_db_id).context("invalid database id in persisted snapshot: {replica_db_id}")?;
                Ok((
                    local_db_id,
                    DatabaseTables {
                        tables: db_tables
                            .tables
                            .into_iter()
                            .map(|(replica_table_id, files)| {
                                Ok((
                                    id_map
                                        .map_table_id(&replica_table_id)
                                        .context("invalid table id in persisted snapshot: {replica_table_id}")?,
                                    files,
                                ))
                            })
                            .collect::<Result<SerdeVecMap<_, _>>>()?,
                    },
                ))
            })
            .collect::<Result<SerdeVecMap<DbId, DatabaseTables>>>()?,
        ..snapshot
    })
}

#[cfg(test)]
pub(crate) mod test_helpers {
    use influxdb3_catalog::catalog::Catalog;
    use influxdb3_id::{ColumnId, DbId, TableId};
    use influxdb3_wal::{
        CatalogBatch, CatalogOp, DatabaseDefinition, FieldDataType, FieldDefinition,
        WalTableDefinition,
    };
    use influxdb3_write::persister::Persister;
    use iox_time::TimeProvider;
    use object_store::ObjectStore;
    use std::sync::Arc;

    pub(crate) async fn create_node_catalog_with_table(
        node_id: &str,
        db_name: &str,
        table_name: &str,
        tag_column_type: FieldDataType,
        object_store: Arc<dyn ObjectStore>,
        time_provider: Arc<dyn TimeProvider>,
    ) -> Arc<Catalog> {
        let catalog = Catalog::new(node_id.into(), node_id.into());
        let database_id = DbId::new();
        let table_id = TableId::new();
        let tag_id = ColumnId::new();

        let batch = CatalogBatch {
            database_id,
            database_name: db_name.into(),
            time_ns: 0,
            ops: vec![
                CatalogOp::CreateDatabase(DatabaseDefinition {
                    database_id,
                    database_name: db_name.into(),
                }),
                CatalogOp::CreateTable(WalTableDefinition {
                    database_id,
                    database_name: db_name.into(),
                    table_name: table_name.into(),
                    table_id,
                    field_definitions: vec![
                        FieldDefinition {
                            name: "tag1".into(),
                            id: tag_id,
                            // data_type here is parameterised to simulate column
                            // mismatch errors
                            data_type: tag_column_type,
                        },
                        FieldDefinition {
                            name: "field1".into(),
                            id: ColumnId::new(),
                            data_type: FieldDataType::Integer,
                        },
                        FieldDefinition {
                            name: "time".into(),
                            id: ColumnId::new(),
                            data_type: FieldDataType::Timestamp,
                        },
                    ],
                    key: vec![tag_id],
                }),
            ],
        };
        catalog.apply_catalog_batch(&batch).unwrap();

        let persister = Persister::new(object_store, node_id, time_provider);
        persister.persist_catalog(&catalog).await.unwrap();

        Arc::new(catalog)
    }
}

#[cfg(test)]
mod tests {
    use crate::sys_events::{catalog_fetched::CatalogFetched, CompactionEvent};

    use super::*;
    use influxdb3_sys_events::SysEventStore;
    use influxdb3_wal::FieldDataType;
    use iox_time::{MockProvider, Time};
    use object_store::memory::InMemory;
    use observability_deps::tracing::debug;
    use pretty_assertions::assert_eq;

    #[tokio::test]
    async fn loads_merged_from_nodes() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let compactor_id = "compactor_id".into();
        let writer1 = "host1";
        let writer2 = "host2";
        let time_provider: Arc<dyn TimeProvider> =
            Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));

        let catalog1 = test_helpers::create_node_catalog_with_table(
            writer1,
            "db1",
            "table1",
            FieldDataType::Tag,
            Arc::clone(&object_store),
            Arc::clone(&time_provider) as _,
        )
        .await;
        let catalog2 = test_helpers::create_node_catalog_with_table(
            writer2,
            "db1",
            "table2",
            FieldDataType::Tag,
            Arc::clone(&object_store),
            Arc::clone(&time_provider),
        )
        .await;

        let catalog = CompactedCatalog::load_merged_from_node_ids(
            Arc::clone(&compactor_id),
            vec![writer1.into(), writer2.into()],
            Arc::clone(&object_store),
            Arc::clone(&time_provider),
        )
        .await
        .expect("failed to load merged catalog");

        assert_eq!(catalog.compactor_id, compactor_id);
        assert_eq!(catalog.inner.read().node_maps.len(), 2);

        let db = catalog.db_schema("db1").expect("db not found");
        assert!(db.table_definition("table1").is_some());
        assert!(db.table_definition("table2").is_some());

        let inner = catalog.inner.read();
        let writer1dbid = catalog1.db_name_to_id("db1").expect("db not found");
        let writer2dbid = catalog2.db_name_to_id("db1").expect("db not found");
        assert_eq!(
            inner
                .node_maps
                .get(writer1)
                .unwrap()
                .catalog_id_map
                .map_db_id(&writer1dbid)
                .unwrap(),
            db.id
        );
        assert_eq!(
            inner
                .node_maps
                .get(writer2)
                .unwrap()
                .catalog_id_map
                .map_db_id(&writer2dbid)
                .unwrap(),
            db.id
        );

        let writer1tableid = catalog1
            .db_schema("db1")
            .expect("db not found")
            .table_name_to_id("table1")
            .expect("table not found");
        let writer2tableid = catalog2
            .db_schema("db1")
            .expect("db not found")
            .table_name_to_id("table2")
            .expect("table not found");
        assert_eq!(
            inner
                .node_maps
                .get(writer1)
                .unwrap()
                .catalog_id_map
                .map_table_id(&writer1tableid)
                .unwrap(),
            db.table_name_to_id("table1").unwrap()
        );
        assert_eq!(
            inner
                .node_maps
                .get(writer2)
                .unwrap()
                .catalog_id_map
                .map_table_id(&writer2tableid)
                .unwrap(),
            db.table_name_to_id("table2").unwrap()
        );
    }

    #[test_log::test(tokio::test)]
    async fn test_merge_catalog_and_record_error_success() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let compactor_id = "compactor_id".into();
        let host1 = "host1";
        let host2 = "host2";
        let time_provider: Arc<dyn TimeProvider> =
            Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));

        let _ = test_helpers::create_node_catalog_with_table(
            host1,
            "db1",
            "table1",
            FieldDataType::Tag,
            Arc::clone(&object_store),
            Arc::clone(&time_provider),
        )
        .await;
        let catalog2 = test_helpers::create_node_catalog_with_table(
            host2,
            "db1",
            "table2",
            FieldDataType::Tag,
            Arc::clone(&object_store),
            Arc::clone(&time_provider),
        )
        .await;

        let catalog = CompactedCatalog::load_merged_from_node_ids(
            compactor_id,
            vec![host1.into()],
            Arc::clone(&object_store),
            Arc::clone(&time_provider),
        )
        .await
        .expect("failed to load merged catalog");

        let marker = CatalogSnapshotMarker {
            snapshot_sequence_number: SnapshotSequenceNumber::new(123),
            catalog_sequence_number: CatalogSequenceNumber::new(345),
            node_id: "host-id".to_string(),
        };

        let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
        let sys_events_store: Arc<dyn CompactionEventStore> =
            Arc::new(SysEventStore::new(Arc::<MockProvider>::clone(
                &time_provider,
            )));
        let res = catalog.merge_catalog_and_record_error(&catalog2, &marker, &sys_events_store);
        let failed_events = sys_events_store.compaction_events_as_vec();
        assert!(res.is_ok());
        debug!(catalogidmap = ?res.unwrap(), "Result from merging other catalog");
        debug!(events_captured = ?failed_events, "Sys events captured after merging compacted catalogs");
        assert!(failed_events.is_empty());
    }

    #[test_log::test(tokio::test)]
    async fn test_merge_catalog_and_record_error_returns_error() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let compactor_id = "compactor_id".into();
        let host1 = "host1";
        let host2 = "host2";
        let time_provider: Arc<dyn TimeProvider> =
            Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));

        let _ = test_helpers::create_node_catalog_with_table(
            host1,
            "db1",
            "table1",
            FieldDataType::Tag,
            Arc::clone(&object_store),
            Arc::clone(&time_provider),
        )
        .await;
        let catalog2 = test_helpers::create_node_catalog_with_table(
            host2,
            "db1",
            "table1",
            FieldDataType::Integer,
            Arc::clone(&object_store),
            Arc::clone(&time_provider),
        )
        .await;

        let catalog = CompactedCatalog::load_merged_from_node_ids(
            compactor_id,
            vec![host1.into()],
            Arc::clone(&object_store),
            Arc::clone(&time_provider),
        )
        .await
        .expect("failed to load merged catalog");

        let marker = CatalogSnapshotMarker {
            snapshot_sequence_number: SnapshotSequenceNumber::new(123),
            catalog_sequence_number: CatalogSequenceNumber::new(345),
            node_id: "host-id".to_string(),
        };

        let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
        let sys_events_store: Arc<dyn CompactionEventStore> =
            Arc::new(SysEventStore::new(Arc::<MockProvider>::clone(
                &time_provider,
            )));
        let res = catalog.merge_catalog_and_record_error(&catalog2, &marker, &sys_events_store);
        let failed_events = sys_events_store.compaction_events_as_vec();
        assert!(res.is_err());
        debug!(result = ?res, "Result from merging other catalog with column type mismatch");
        debug!(events_captured = ?failed_events, "Sys events captured after merging compacted catalogs");
        assert_eq!(1, failed_events.len());
        let failed_event = failed_events.first().unwrap();
        match &failed_event.data {
            CompactionEvent::CatalogFetched(catalog_info) => match catalog_info {
                CatalogFetched::Success(_) => panic!("cannot be success"),
                CatalogFetched::Failed(failed_event_info) => {
                    assert_eq!(Arc::from("host-id"), failed_event_info.node_id);
                    assert_eq!(123, failed_event_info.sequence_number);
                    assert_eq!("Field type mismatch on table table1 column tag1. Existing column is iox::column_type::tag but attempted to add iox::column_type::field::integer".to_string(),
                        failed_event_info.error);
                }
            },
            _ => panic!("no other event type expected"),
        }
    }
}
