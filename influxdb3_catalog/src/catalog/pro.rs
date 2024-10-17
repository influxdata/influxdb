use influxdb3_wal::{
    CatalogBatch, CatalogOp, DatabaseDefinition, FieldAdditions, LastCacheDefinition,
    LastCacheDelete, TableDefinition as WalTableDefinition, WalContents, WalOp, WriteBatch,
};
use schema::InfluxColumnType;
use std::collections::BTreeMap;
use std::{collections::HashMap, sync::Arc};

use influxdb3_id::{DbId, TableId};

use crate::catalog::{Catalog, DatabaseSchema, Error, InnerCatalog, Result, TableDefinition};

impl Catalog {
    /// Merge another catlog into this one, producing a mapping that maps the IDs from the other
    /// catalog to this one
    pub fn merge(&self, other: Arc<Catalog>) -> Result<CatalogIdMap> {
        self.inner().write().merge(other)
    }
}

impl InnerCatalog {
    fn merge(&mut self, other: Arc<Catalog>) -> Result<CatalogIdMap> {
        let mut id_map = CatalogIdMap::default();
        for other_db_schema in other.list_db_schema() {
            let other_db_id = other_db_schema.id;
            let other_db_name = Arc::clone(&other_db_schema.name);
            let local_db_id = if let Some(local_db_schema) =
                self.databases.iter().find_map(|(_, db_schema)| {
                    db_schema
                        .name
                        .eq(&other_db_name)
                        .then_some(Arc::clone(db_schema))
                }) {
                let (ids, new_db) = local_db_schema.merge(other_db_schema)?;
                if let Some(s) = new_db {
                    self.databases.insert(s.id, Arc::clone(&s));
                    self.sequence = self.sequence.next();
                    self.updated = true;
                    self.db_map.insert(s.id, Arc::clone(&s.name));
                }
                id_map.extend(ids);
                local_db_schema.id
            } else {
                let id = DbId::new();
                let db = DatabaseSchema::new(id, Arc::clone(&other_db_name));
                let (ids, new_db) = db.merge(other_db_schema)?;
                let new_db = new_db.unwrap_or_else(|| Arc::new(db));
                self.databases.insert(new_db.id, Arc::clone(&new_db));
                self.sequence = self.sequence.next();
                self.updated = true;
                self.db_map.insert(new_db.id, Arc::clone(&new_db.name));
                id_map.extend(ids);
                id
            };
            id_map.dbs.insert(other_db_id, local_db_id);
        }
        Ok(id_map)
    }
}

impl DatabaseSchema {
    fn merge(&self, other: Arc<Self>) -> Result<(CatalogIdMap, Option<Arc<Self>>)> {
        let mut id_map = CatalogIdMap::default();
        let mut new_tables = BTreeMap::new();
        for other_tbl_def in other.tables.values() {
            let other_tbl_id = other_tbl_def.table_id;
            let other_tbl_name = Arc::clone(&other_tbl_def.table_name);
            let local_tbl_id =
                if let Some(local_tbl_def) = self.table_definition(other_tbl_name.as_ref()) {
                    let (ids, new_tbl) = local_tbl_def.merge(other_tbl_def)?;
                    if let Some(new_tbl) = new_tbl {
                        new_tables.insert(new_tbl.table_id, new_tbl);
                    }
                    id_map.extend(ids);
                    local_tbl_def.table_id
                } else {
                    let new_tbl_id = TableId::new();
                    let tbl_def = TableDefinition {
                        table_id: new_tbl_id,
                        table_name: Arc::clone(&other_tbl_name),
                        schema: other_tbl_def.schema().clone(),
                        last_caches: other_tbl_def.last_caches.clone(),
                    };
                    new_tables.insert(new_tbl_id, tbl_def);
                    new_tbl_id
                };
            id_map.tables.insert(other_tbl_id, local_tbl_id);
        }
        let updated_schema = (!new_tables.is_empty()).then(|| {
            let table_map = new_tables
                .iter()
                .map(|(id, def)| (*id, Arc::clone(&def.table_name)))
                .collect();
            Arc::new(DatabaseSchema {
                id: self.id,
                name: Arc::clone(&self.name),
                tables: new_tables,
                table_map,
            })
        });
        Ok((id_map, updated_schema))
    }
}

impl TableDefinition {
    fn merge(&self, other: &Self) -> Result<(CatalogIdMap, Option<Self>)> {
        let mut id_map = CatalogIdMap::default();
        id_map.tables.insert(other.table_id, self.table_id);
        let existing_key = self.schema.series_key();
        if other.schema.series_key() != existing_key {
            return Err(Error::SeriesKeyMismatch {
                table_name: self.table_name.to_string(),
                existing: existing_key.unwrap_or_default().join("/"),
                attempted: other.schema.series_key().unwrap_or_default().join("/"),
            });
        }
        let mut new_fields: Vec<(String, InfluxColumnType)> = Vec::new();
        for (merge_type, merge_field) in other.influx_schema().iter() {
            // TODO: need to map the column ID here...
            if let Some(existing_type) = self.schema.schema().field_type_by_name(merge_field.name())
            {
                if existing_type != merge_type {
                    return Err(Error::FieldTypeMismatch {
                        table_name: self.table_name.to_string(),
                        column_name: merge_field.name().to_string(),
                        existing: existing_type,
                        attempted: merge_type,
                    });
                }
            } else {
                new_fields.push((merge_field.name().to_owned(), merge_type));
            }
        }

        let mut new_last_caches: Vec<LastCacheDefinition> = vec![];
        for (merge_name, merge_last_cache) in other.last_caches() {
            // TODO: need to validate the configuration of the cache
            // for compatability here...
            if !self.last_caches.contains_key(merge_name) {
                new_last_caches.push(merge_last_cache.to_owned());
            }
        }

        if !new_fields.is_empty() || !new_last_caches.is_empty() {
            let mut new_table = self.clone();
            new_table.add_columns(new_fields)?;
            for lc in new_last_caches {
                new_table.add_last_cache(lc);
            }
            Ok((id_map, Some(new_table)))
        } else {
            Ok((id_map, None))
        }
    }
}

#[derive(Debug, Default)]
pub struct CatalogIdMap {
    dbs: HashMap<DbId, DbId>,
    tables: HashMap<TableId, TableId>,
}

impl CatalogIdMap {
    fn extend(&mut self, mut other: Self) {
        self.dbs.extend(other.dbs.drain());
        self.tables.extend(other.tables.drain());
    }

    pub fn map_db_or_new(
        &mut self,
        target_catalog: &Catalog,
        from_name: &str,
        from_id: DbId,
    ) -> DbId {
        self.dbs
            .get(&from_id)
            .copied()
            .or_else(|| {
                let id = target_catalog.db_name_to_id(from_name)?;
                self.dbs.insert(from_id, id);
                Some(id)
            })
            .unwrap_or_else(|| {
                let new_id = DbId::new();
                self.dbs.insert(from_id, new_id);
                new_id
            })
    }

    pub fn map_table_or_new(
        &mut self,
        target_catalog: &Catalog,
        database_id: DbId,
        table_name: &str,
        table_id: TableId,
    ) -> TableId {
        self.tables
            .get(&table_id)
            .copied()
            .or_else(|| {
                let id = target_catalog
                    .db_schema_by_id(database_id)?
                    .table_name_to_id(table_name)?;
                self.tables.insert(table_id, id);
                Some(id)
            })
            .unwrap_or_else(|| {
                let new_id = TableId::new();
                self.tables.insert(table_id, new_id);
                new_id
            })
    }

    pub fn map_wal_contents(
        &mut self,
        target_catalog: &Catalog,
        wal_contents: WalContents,
    ) -> WalContents {
        WalContents {
            min_timestamp_ns: wal_contents.min_timestamp_ns,
            max_timestamp_ns: wal_contents.max_timestamp_ns,
            wal_file_number: wal_contents.wal_file_number,
            ops: wal_contents
                .ops
                .into_iter()
                .map(|op| match op {
                    WalOp::Write(write_batch) => {
                        WalOp::Write(self.map_write_batch(target_catalog, write_batch))
                    }
                    WalOp::Catalog(catalog_batch) => {
                        WalOp::Catalog(self.map_catalog_batch(target_catalog, catalog_batch))
                    }
                })
                .collect(),
            snapshot: wal_contents.snapshot,
        }
    }

    fn map_write_batch(&mut self, target_catalog: &Catalog, from: WriteBatch) -> WriteBatch {
        let database_id = self.map_db_or_new(target_catalog, &from.database_name, from.database_id);
        WriteBatch {
            database_id,
            database_name: Arc::clone(&from.database_name),
            table_chunks: from
                .table_chunks
                .into_iter()
                .map(|(table_id, chunks)| {
                    (
                        self.tables
                            .get(&table_id)
                            .copied()
                            .expect("write batch encountered for unseen table"),
                        chunks,
                    )
                })
                .collect(),
            min_time_ns: from.min_time_ns,
            max_time_ns: from.max_time_ns,
        }
    }

    fn map_catalog_batch(&mut self, target_catalog: &Catalog, from: CatalogBatch) -> CatalogBatch {
        let database_id = self.map_db_or_new(target_catalog, &from.database_name, from.database_id);
        CatalogBatch {
            database_id,
            database_name: Arc::clone(&from.database_name),
            time_ns: from.time_ns,
            ops: from
                .ops
                .into_iter()
                .map(|op| self.map_catalog_op(target_catalog, database_id, op))
                .collect(),
        }
    }

    fn map_catalog_op(
        &mut self,
        target_catalog: &Catalog,
        database_id: DbId,
        op: CatalogOp,
    ) -> CatalogOp {
        match op {
            CatalogOp::CreateDatabase(def) => CatalogOp::CreateDatabase(DatabaseDefinition {
                database_id,
                database_name: Arc::clone(&def.database_name),
            }),
            CatalogOp::CreateTable(def) => CatalogOp::CreateTable(WalTableDefinition {
                database_id,
                database_name: Arc::clone(&def.database_name),
                table_name: Arc::clone(&def.table_name),
                table_id: self.map_table_or_new(
                    target_catalog,
                    database_id,
                    &def.table_name,
                    def.table_id,
                ),
                field_definitions: def.field_definitions.clone(),
                key: def.key.clone(),
            }),
            CatalogOp::AddFields(def) => CatalogOp::AddFields(FieldAdditions {
                database_name: Arc::clone(&def.database_name),
                database_id,
                table_name: Arc::clone(&def.table_name),
                table_id: self.map_table_or_new(
                    target_catalog,
                    database_id,
                    &def.table_name,
                    def.table_id,
                ),
                field_definitions: def.field_definitions.clone(),
            }),
            CatalogOp::CreateLastCache(def) => CatalogOp::CreateLastCache(LastCacheDefinition {
                table_id: self.map_table_or_new(
                    target_catalog,
                    database_id,
                    &def.table,
                    def.table_id,
                ),
                table: def.table.clone(),
                name: def.name.clone(),
                key_columns: def.key_columns.clone(),
                value_columns: def.value_columns.clone(),
                count: def.count,
                ttl: def.ttl,
            }),
            // TODO: if the table doesn't exist locally, do we need to bother with
            // deleting it?
            CatalogOp::DeleteLastCache(def) => CatalogOp::DeleteLastCache(LastCacheDelete {
                table_name: def.table_name.clone(),
                table_id: self.map_table_or_new(
                    target_catalog,
                    database_id,
                    &def.table_name,
                    def.table_id,
                ),
                name: def.name.clone(),
            }),
        }
    }
}
