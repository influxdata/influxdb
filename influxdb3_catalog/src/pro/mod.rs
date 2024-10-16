use anyhow::Result;
use bimap::BiHashMap;
use influxdb3_wal::CatalogOp;
use parking_lot::RwLock;
use std::{collections::HashMap, sync::Arc};

use influxdb3_id::DbId;

use crate::catalog::{Catalog, DatabaseSchema};
use crate::DatabaseSchemaProvider;

#[derive(Debug, Default)]
struct IdManifest {
    dbs: HashMap<(Arc<str>, DbId), DbId>,
}

#[derive(Debug, Default)]
pub struct SynthesizedCatalog {
    inner: Arc<RwLock<Inner>>,
}

#[derive(Debug, Default)]
pub struct Inner {
    databases: HashMap<DbId, Arc<DatabaseSchema>>,
    db_name_to_id_map: BiHashMap<DbId, Arc<str>>,
    ids: IdManifest,
}

impl SynthesizedCatalog {
    pub fn new() -> Self {
        Default::default()
    }

    /// Merge another catalog into this, using the names of entities in the either catalog to
    /// determine similarity.
    pub fn merge_catalog(&self, other: Arc<Catalog>, host_id: impl Into<Arc<str>>) -> Result<()> {
        let host_id = host_id.into();
        let mut inner = self.inner.write();
        for db_schema in other.list_db_schema() {
            let other_db_id = db_schema.id;
            let other_db_name = Arc::clone(&db_schema.name);
            let local_db_id = inner
                .databases
                .iter()
                .find_map(|(local_db_id, local_db_schema)| {
                    local_db_schema
                        .name
                        .eq(&other_db_name)
                        .then_some(local_db_id)
                })
                .cloned()
                .unwrap_or_else(DbId::new);
            assert!(
                inner
                    .ids
                    .dbs
                    .insert((Arc::clone(&host_id), other_db_id), local_db_id)
                    .is_none(),
                "there should not be an entry in the manifest"
            );
            // TODO: update inner tables, etc. in manifest

            assert!(
                inner
                    .databases
                    .insert(
                        local_db_id,
                        Arc::new(DatabaseSchema {
                            id: local_db_id,
                            name: other_db_name,
                            tables: db_schema.tables.clone(),
                            table_map: db_schema.table_map.clone()
                        }),
                    )
                    .is_none(),
                "there should not be an entry in the databases table"
            );
        }
        Ok(())
    }

    pub fn apply_catalog_op(&self, op: CatalogOp, host_id: impl Into<Arc<str>>) -> Result<()> {
        Ok(())
    }
}

impl DatabaseSchemaProvider for SynthesizedCatalog {
    fn db_name_to_id(&self, db_name: &str) -> Option<DbId> {
        self.inner
            .read()
            .db_name_to_id_map
            .get_by_right(db_name)
            .copied()
    }

    fn db_id_to_name(&self, db_id: DbId) -> Option<Arc<str>> {
        self.inner
            .read()
            .db_name_to_id_map
            .get_by_left(&db_id)
            .cloned()
    }

    fn db_schema(&self, db_name: &str) -> Option<Arc<DatabaseSchema>> {
        let inner = self.inner.read();
        let db_id = inner.db_name_to_id_map.get_by_right(db_name)?;
        inner.databases.get(db_id).cloned()
    }

    fn db_schema_by_id(&self, db_id: DbId) -> Option<Arc<DatabaseSchema>> {
        self.inner.read().databases.get(&db_id).cloned()
    }

    fn db_schema_and_id(&self, db_name: &str) -> Option<(DbId, Arc<DatabaseSchema>)> {
        let inner = self.inner.read();
        let db_id = inner.db_name_to_id_map.get_by_right(db_name).copied()?;
        let db_schema = inner.databases.get(&db_id).cloned()?;
        Some((db_id, db_schema))
    }

    fn db_names(&self) -> Vec<String> {
        self.inner
            .read()
            .databases
            .values()
            .map(|db_schema| db_schema.name.to_string())
            .collect()
    }

    fn list_db_schema(&self) -> Vec<Arc<DatabaseSchema>> {
        self.inner.read().databases.values().cloned().collect()
    }
}
