//! Module for the compacted catalog, which is a unified view of the catalgos across all hosts
//! that have been compacted.

use anyhow::Context;
use futures_util::StreamExt;
use hashbrown::HashMap;
use influxdb3_catalog::catalog::{pro::CatalogIdMap, Catalog, CatalogSequenceNumber, InnerCatalog};
use influxdb3_id::DbId;
use influxdb3_pro_data_layout::persist::get_bytes_at_path;
use influxdb3_write::paths::CatalogFilePath;
use influxdb3_write::persister::Persister;
use influxdb3_write::{DatabaseTables, PersistedSnapshot};
use object_store::path::Path as ObjPath;
use object_store::ObjectStore;
use observability_deps::tracing::info;
use observability_deps::tracing::log::error;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use thiserror::Error;
use uuid::Uuid;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Error loading compacted catalog: {0}")]
    LoadError(#[from] object_store::Error),
    #[error("Error serializing compacted catalog: {0}")]
    SerializationError(#[from] serde_json::Error),
    #[error("Error loading host catalog: {0}")]
    HostCatalogLoadError(#[from] influxdb3_write::persister::Error),
    #[error("Error merging host catalogs: {0}")]
    MergeError(#[from] influxdb3_catalog::catalog::Error),
    #[error("Unknown error: {0}")]
    Unexpected(#[from] anyhow::Error),
}

/// Result type for functions in this module.
pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug)]
pub struct CompactedCatalog {
    pub comapctor_id: Arc<str>,
    /// A unified catalog from all hosts getting compacted
    pub catalog: Arc<Catalog>,
    /// map of host id to ids id map into this catalog
    host_maps: RwLock<HashMap<String, HostCatalog>>,
}

// First, create a helper struct for serialization
#[derive(Serialize, Deserialize)]
struct CompactedCatalogHelper {
    compactor_id: String,
    catalog: InnerCatalog,
    host_maps: HashMap<String, HostCatalog>,
}

// Implement Serialize for CompactedCatalog
impl Serialize for CompactedCatalog {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        // Create the helper struct
        let helper = CompactedCatalogHelper {
            compactor_id: self.comapctor_id.to_string(),
            catalog: self.catalog.clone_inner(),
            // We need to block here to get the mutex contents
            host_maps: self.host_maps.read().clone(),
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
            comapctor_id: helper.compactor_id.into(),
            catalog: Arc::new(Catalog::from_inner(helper.catalog)),
            host_maps: RwLock::new(helper.host_maps),
        })
    }
}

impl CompactedCatalog {
    pub fn map_persisted_snapshot_contents(
        &self,
        persisted_snapshot: PersistedSnapshot,
    ) -> Result<PersistedSnapshot> {
        let idmap = self.host_maps.read();
        let hostmap = idmap
            .get(&persisted_snapshot.host_id)
            .context("Host not found in catalog")?;
        map_snapshot_contents(&hostmap.catalog_id_map, persisted_snapshot)
    }

    /// Load the `CompactedCatalog` from the object store.
    pub async fn load(
        compactor_id: &str,
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

    pub async fn persist(&self, object_store: Arc<dyn ObjectStore>) -> Result<()> {
        let path =
            CompactedCatalogPath::new(self.comapctor_id.as_ref(), self.catalog.sequence_number());
        let bytes = serde_json::to_vec(&self)?;
        object_store.put(&path.0, bytes.into()).await?;
        info!(path = %path.0, "Persisted compacted catalog");
        Ok(())
    }

    /// Updates the catalog id maps and persists the new catalog if any of the passed in markers
    /// have a sequence number greater than the last sequence number for the host.
    pub async fn update_from_markers(
        &self,
        catalog_snapshot_markers: Vec<CatalogSnapshotMarker>,
        object_store: Arc<dyn ObjectStore>,
    ) -> Result<()> {
        // get the paths of any catalogs we need to update because they have a higher sequence number
        let catalogs_to_update = {
            let mut paths = vec![];

            let host_maps = self.host_maps.read();
            for marker in catalog_snapshot_markers {
                let last_sequence_number = host_maps
                    .get(&marker.host)
                    .map(|h| h.last_catalog_sequence_number)
                    .unwrap_or(CatalogSequenceNumber::new(0));
                if marker.sequence_number > last_sequence_number {
                    let catalog_path = CatalogFilePath::new(&marker.host, marker.sequence_number);
                    paths.push(catalog_path);
                }
            }

            paths
        };

        if catalogs_to_update.is_empty() {
            return Ok(());
        }

        // now fetch all the catalogs while we're not holding any locks
        let mut updated_host_maps = HashMap::new();

        for catalog_path in catalogs_to_update {
            let Some(updated_catalog) =
                get_bytes_at_path(catalog_path.as_ref(), Arc::clone(&object_store)).await
            else {
                error!("Catalog not found at path: {}", catalog_path.to_string());
                continue;
            };
            let catalog = Arc::new(Catalog::from_inner(serde_json::from_slice(
                &updated_catalog,
            )?));

            let catalog_id_map = self.catalog.merge(Arc::clone(&catalog))?;
            updated_host_maps.insert(
                catalog.host_id(),
                HostCatalog {
                    last_catalog_sequence_number: catalog.sequence_number(),
                    catalog_id_map,
                },
            );
            info!(host = %catalog.host_id(), sequence = %catalog.sequence_number().as_u32(), "Updated host catalog");
        }

        // and update the host maps
        let mut host_maps = self.host_maps.write();
        for (host, updated_host_catalog) in updated_host_maps {
            host_maps.insert(host.to_string(), updated_host_catalog);
        }

        Ok(())
    }

    /// Loads the most recently persisted catalog from each of the hosts. Returns a new catalog
    /// that is the union of all the host catalogs. If none of the hosts has persisted a catalog,
    /// returns a newly initialized one.
    pub async fn load_merged_from_hosts(
        compactor_id: &str,
        hosts: Vec<String>,
        object_store: Arc<dyn ObjectStore>,
    ) -> Result<CompactedCatalog> {
        let uuid = Uuid::new_v4().to_string();
        let instance_id = Arc::from(uuid.as_str());

        let primary_catalog = Catalog::new(compactor_id.into(), instance_id);
        let mut host_maps = HashMap::new();

        for host in hosts {
            let persister = Persister::new(Arc::clone(&object_store), &host);
            if let Some(catalog) = persister.load_catalog().await? {
                let last_catalog_sequence_number = catalog.sequence_number();
                let catalog = Arc::new(Catalog::from_inner(catalog));
                let catalog_id_map = primary_catalog.merge(catalog)?;

                info!(host = %host, "Loaded host catalog");
                host_maps.insert(
                    host,
                    HostCatalog {
                        last_catalog_sequence_number,
                        catalog_id_map,
                    },
                );
            } else {
                info!(host = %host, "No host catalog found");
                host_maps.insert(
                    host,
                    HostCatalog {
                        last_catalog_sequence_number: CatalogSequenceNumber::new(0),
                        catalog_id_map: CatalogIdMap::default(),
                    },
                );
            }
        }

        Ok(Self {
            comapctor_id: compactor_id.into(),
            catalog: Arc::new(primary_catalog),
            host_maps: RwLock::new(host_maps),
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
struct HostCatalog {
    last_catalog_sequence_number: CatalogSequenceNumber,
    catalog_id_map: CatalogIdMap,
}

#[derive(Debug, Eq, PartialEq)]
pub struct CatalogSnapshotMarker {
    pub sequence_number: CatalogSequenceNumber,
    pub host: String,
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
                            .collect::<Result<hashbrown::HashMap<_, _>>>()?,
                    },
                ))
            })
            // TODO: it is a bit annoying that two HashMap types are used in influxdb3_write,
            // so we should probably pick one there.
            .collect::<Result<std::collections::HashMap<DbId, DatabaseTables>>>()?,
        ..snapshot
    })
}

#[cfg(test)]
pub(crate) mod test_helpers {
    use influxdb3_catalog::catalog::Catalog;
    use influxdb3_id::{ColumnId, DbId, TableId};
    use influxdb3_wal::{
        CatalogBatch, CatalogOp, DatabaseDefinition, FieldDataType, FieldDefinition,
        TableDefinition,
    };
    use influxdb3_write::persister::Persister;
    use object_store::ObjectStore;
    use std::sync::Arc;

    pub(crate) async fn create_host_catalog_with_table(
        host_id: &str,
        db_name: &str,
        table_name: &str,
        object_store: Arc<dyn ObjectStore>,
    ) -> Arc<Catalog> {
        let catalog = Catalog::new(host_id.into(), host_id.into());
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
                CatalogOp::CreateTable(TableDefinition {
                    database_id,
                    database_name: db_name.into(),
                    table_name: table_name.into(),
                    table_id,
                    field_definitions: vec![
                        FieldDefinition {
                            name: "tag1".into(),
                            id: tag_id,
                            data_type: FieldDataType::Tag,
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
                    key: Some(vec![tag_id]),
                }),
            ],
        };
        catalog.apply_catalog_batch(&batch).unwrap();

        let persister = Persister::new(object_store, host_id);
        persister.persist_catalog(&catalog).await.unwrap();

        Arc::new(catalog)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;

    #[tokio::test]
    async fn loads_merged_from_hosts() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let compactor_id = "compactor_id";
        let host1 = "host1";
        let host2 = "host2";

        let catalog1 = test_helpers::create_host_catalog_with_table(
            host1,
            "db1",
            "table1",
            Arc::clone(&object_store),
        )
        .await;
        let catalog2 = test_helpers::create_host_catalog_with_table(
            host2,
            "db1",
            "table2",
            Arc::clone(&object_store),
        )
        .await;

        let catalog = CompactedCatalog::load_merged_from_hosts(
            compactor_id,
            vec![host1.into(), host2.into()],
            Arc::clone(&object_store),
        )
        .await
        .expect("failed to load merged catalog");

        assert_eq!(catalog.comapctor_id.as_ref(), compactor_id);
        assert_eq!(catalog.host_maps.read().len(), 2);

        let db = catalog.catalog.db_schema("db1").expect("db not found");
        assert!(db.table_definition("table1").is_some());
        assert!(db.table_definition("table2").is_some());

        let host_maps = catalog.host_maps.read();
        let host1dbid = catalog1.db_name_to_id("db1").expect("db not found");
        let host2dbid = catalog2.db_name_to_id("db1").expect("db not found");
        assert_eq!(
            host_maps
                .get(host1)
                .unwrap()
                .catalog_id_map
                .map_db_id(&host1dbid)
                .unwrap(),
            db.id
        );
        assert_eq!(
            host_maps
                .get(host2)
                .unwrap()
                .catalog_id_map
                .map_db_id(&host2dbid)
                .unwrap(),
            db.id
        );

        let host1tableid = catalog1
            .db_schema("db1")
            .expect("db not found")
            .table_name_to_id("table1")
            .expect("table not found");
        let host2tableid = catalog2
            .db_schema("db1")
            .expect("db not found")
            .table_name_to_id("table2")
            .expect("table not found");
        assert_eq!(
            host_maps
                .get(host1)
                .unwrap()
                .catalog_id_map
                .map_table_id(&host1tableid)
                .unwrap(),
            db.table_name_to_id("table1").unwrap()
        );
        assert_eq!(
            host_maps
                .get(host2)
                .unwrap()
                .catalog_id_map
                .map_table_id(&host2tableid)
                .unwrap(),
            db.table_name_to_id("table2").unwrap()
        );
    }
}
