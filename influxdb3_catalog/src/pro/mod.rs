use anyhow::Result;
use influxdb3_wal::CatalogOp;
use parking_lot::RwLock;
use std::{
    collections::{hash_map::Entry, HashMap},
    sync::Arc,
};

use influxdb3_id::DbId;

use crate::catalog::{Catalog, DatabaseSchema};

#[derive(Debug)]
struct IdManifest {
    dbs: HashMap<(Arc<str>, DbId), DbId>,
}

impl IdManifest {
    fn new() -> Self {
        Self {
            dbs: HashMap::new(),
        }
    }
}

#[derive(Debug)]
pub struct SynthesizedCatalog {
    inner: Arc<RwLock<Inner>>,
}

#[derive(Debug)]
pub struct Inner {
    databases: HashMap<DbId, DatabaseSchema>,
    ids: IdManifest,
}

impl SynthesizedCatalog {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(Inner {
                databases: HashMap::new(),
                ids: IdManifest::new(),
            })),
        }
    }

    /// Merge another catalog into this, using the names of entities in the either catalog to
    /// determine similarity.
    pub fn merge_catalog(&self, other: Arc<Catalog>, host_id: impl Into<Arc<str>>) -> Result<()> {
        let host_id = host_id.into();
        let mut inner = self.inner.write();
        // TODO: just expose this method directly on the outer Catalog instead of needing to
        // clone the entire inner:
        let other = other.clone_inner();
        for db_schema in other.databases() {
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
                        DatabaseSchema {
                            id: local_db_id,
                            name: other_db_name,
                            // TODO: tables need to be added using separate process that
                            // upserts using new table ids, similar to how we are doing for databases
                            // here...
                            tables: db_schema.tables.clone(),
                        },
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
