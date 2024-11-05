use anyhow::anyhow;
use bimap::BiHashMap;
use hashbrown::HashMap;
use indexmap::IndexMap;
use influxdb3_wal::{
    CatalogBatch, CatalogOp, DatabaseDefinition, Field, FieldAdditions, FieldDefinition,
    LastCacheDefinition, LastCacheDelete, LastCacheValueColumnsDef, Row, TableChunk, TableChunks,
    TableDefinition as WalTableDefinition, WalContents, WalOp, WriteBatch,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use influxdb3_id::{ColumnId, DbId, TableId};

use crate::catalog::{Catalog, DatabaseSchema, Error, InnerCatalog, Result, TableDefinition};

use super::ColumnDefinition;

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
        let mut new_tables = IndexMap::new();
        for other_tbl_def in other.tables.values() {
            let other_tbl_name = Arc::clone(&other_tbl_def.table_name);
            if let Some(local_tbl_def) = self.table_definition(other_tbl_name.as_ref()) {
                let (ids, new_tbl) = local_tbl_def.merge(Arc::clone(other_tbl_def))?;
                if let Some(new_tbl) = new_tbl {
                    new_tables.insert(new_tbl.table_id, Arc::new(new_tbl));
                }
                id_map.extend(ids);
            } else {
                let (ids, tbl_def) =
                    TableDefinition::new_from_other_host(Arc::clone(other_tbl_def))?;
                new_tables.insert(tbl_def.table_id, tbl_def);
                id_map.extend(ids);
            };
        }
        let updated_schema = (!new_tables.is_empty()).then(|| {
            let table_map = new_tables
                .iter()
                .map(|(id, def)| (*id, Arc::clone(&def.table_name)))
                .collect();
            Arc::new(DatabaseSchema {
                id: self.id,
                name: Arc::clone(&self.name),
                tables: new_tables.into(),
                table_map,
            })
        });
        Ok((id_map, updated_schema))
    }
}

impl TableDefinition {
    fn merge(&self, other: Arc<Self>) -> Result<(CatalogIdMap, Option<Self>)> {
        let mut id_map = CatalogIdMap::default();
        id_map.tables.insert(other.table_id, self.table_id);
        let mut new_columns = Vec::new();
        for other_col_def in other.columns.values() {
            let other_col_id = other_col_def.id;
            let other_col_name = Arc::clone(&other_col_def.name);
            if let Some(local_col_def) = self
                .column_map
                .get_by_right(&other_col_name)
                .and_then(|id| self.columns.get(id))
            {
                let ids = local_col_def.merge(self.table_name.as_ref(), other_col_def)?;
                id_map.extend(ids);
            } else {
                let new_col_id = ColumnId::new();
                let other_cloned = other_col_def.clone();
                let col_def = ColumnDefinition {
                    id: new_col_id,
                    ..other_cloned
                };
                new_columns.push((new_col_id, Arc::clone(&col_def.name), col_def.data_type));
                id_map.columns.insert(other_col_id, new_col_id);
            };
        }

        // validate the series keys match for existing tables:
        let mapped_series_key = other
            .series_key
            .as_ref()
            .map(|sk| {
                sk.iter()
                    .map(|id| {
                        id_map.columns.get(id).copied().ok_or(Error::Other(anyhow!(
                            "other table series key contained invalid id"
                        )))
                    })
                    .collect::<Result<Vec<ColumnId>>>()
            })
            .transpose()?;
        if mapped_series_key != self.series_key {
            return Err(Error::Other(anyhow!("the series key from the other catalog's table does not match that of the local catalog")));
        }

        // merge in any new last cache definitions
        // TODO: need to validate the configuration of last caches for compatability here
        let mut new_last_caches: Vec<LastCacheDefinition> = vec![];
        for (merge_name, merge_last_cache) in other.last_caches() {
            if !self.last_caches.contains_key(&merge_name) {
                new_last_caches.push(map_last_cache_definition(merge_last_cache, &id_map)?);
            }
        }

        if !new_columns.is_empty() || !new_last_caches.is_empty() {
            let mut new_table = self.clone();
            new_table.add_columns(new_columns)?;
            for lc in new_last_caches {
                new_table.add_last_cache(lc);
            }
            Ok((id_map, Some(new_table)))
        } else {
            Ok((id_map, None))
        }
    }

    fn new_from_other_host(other: Arc<Self>) -> Result<(CatalogIdMap, Arc<Self>)> {
        let mut id_map = CatalogIdMap::default();
        let table_id = TableId::new();
        id_map.tables.insert(other.table_id, table_id);

        // map ids for columns:
        let mut columns = IndexMap::with_capacity(other.columns.len());
        let mut column_map = BiHashMap::with_capacity(other.columns.len());
        for (other_id, other_def) in other.columns.iter() {
            let col_id = ColumnId::new();
            id_map.columns.insert(*other_id, col_id);
            column_map.insert(col_id, Arc::clone(&other_def.name));
            columns.insert(
                col_id,
                ColumnDefinition {
                    id: col_id,
                    ..other_def.clone()
                },
            );
        }

        // map the column ids in the series key:
        let series_key = other
            .series_key
            .as_ref()
            .map(|sk| sk
                .iter()
                .map(|other_id| id_map
                    .columns
                    .get(other_id)
                    .copied()
                    .ok_or_else(|| Error::Other(
                        anyhow!("the table from the other catalog contained an invalid column in its series key (id: {other_id})")
                    ))
                )
                .collect::<Result<Vec<ColumnId>>>()
            ).transpose()?;

        // map the ids from last cache definitions
        let mut last_caches = HashMap::new();
        for (name, lc) in &other.last_caches {
            last_caches.insert(Arc::clone(name), map_last_cache_definition(lc, &id_map)?);
        }

        Ok((
            id_map,
            Arc::new(TableDefinition {
                table_id,
                table_name: Arc::clone(&other.table_name),
                schema: other.schema.clone(),
                columns,
                column_map,
                series_key,
                last_caches,
            }),
        ))
    }
}

fn map_last_cache_definition(
    def: &LastCacheDefinition,
    id_map: &CatalogIdMap,
) -> Result<LastCacheDefinition> {
    let table_id =
        id_map.tables.get(&def.table_id).copied().ok_or_else(|| {
            Error::Other(anyhow!("last cache definition contained invalid table id"))
        })?;
    let key_columns = def
        .key_columns
        .iter()
        .map(|id| {
            id_map.columns.get(id).copied().ok_or_else(|| {
                Error::Other(anyhow!(
                    "last cache definition contained invalid key column id"
                ))
            })
        })
        .collect::<Result<Vec<ColumnId>>>()?;
    let value_columns = match def.value_columns {
        LastCacheValueColumnsDef::Explicit { ref columns } => {
            let columns = columns
                .iter()
                .map(|id| {
                    id_map.columns.get(id).copied().ok_or_else(|| {
                        Error::Other(anyhow!(
                            "last cache definition contained invalid value column id"
                        ))
                    })
                })
                .collect::<Result<Vec<ColumnId>>>()?;
            LastCacheValueColumnsDef::Explicit { columns }
        }
        LastCacheValueColumnsDef::AllNonKeyColumns => LastCacheValueColumnsDef::AllNonKeyColumns,
    };
    Ok(LastCacheDefinition {
        table_id,
        table: Arc::clone(&def.table),
        name: Arc::clone(&def.name),
        key_columns,
        value_columns,
        count: def.count,
        ttl: def.ttl,
    })
}

impl ColumnDefinition {
    fn merge(&self, table_name: impl Into<String>, other: &Self) -> Result<CatalogIdMap> {
        let mut id_map = CatalogIdMap::default();
        id_map.columns.insert(other.id, self.id);
        if other.nullable != self.nullable {
            return Err(Error::Other(anyhow!(
                "column nullability does not match, this: {}, other: {}",
                self.nullable,
                other.nullable
            )));
        }
        if other.data_type != self.data_type {
            return Err(Error::FieldTypeMismatch {
                table_name: table_name.into(),
                column_name: self.name.as_ref().to_string(),
                existing: self.data_type,
                attempted: other.data_type,
            });
        }
        Ok(id_map)
    }
}

#[derive(Debug, Default, Serialize, Deserialize, Clone)]
pub struct CatalogIdMap {
    dbs: HashMap<DbId, DbId>,
    tables: HashMap<TableId, TableId>,
    columns: HashMap<ColumnId, ColumnId>,
}

impl CatalogIdMap {
    fn extend(&mut self, mut other: Self) {
        self.dbs.extend(other.dbs.drain());
        self.tables.extend(other.tables.drain());
        self.columns.extend(other.columns.drain());
    }

    pub fn map_db_id(&self, database_id: DbId) -> Option<DbId> {
        self.dbs.get(&database_id).copied()
    }

    pub fn map_table_id(&self, table_id: TableId) -> Option<TableId> {
        self.tables.get(&table_id).copied()
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

    pub fn map_column_or_new(
        &mut self,
        target_catalog: &Catalog,
        database_id: DbId,
        table_id: TableId,
        column_name: &str,
        column_id: ColumnId,
    ) -> ColumnId {
        self.columns
            .get(&column_id)
            .copied()
            .or_else(|| {
                let id = target_catalog
                    .db_schema_by_id(database_id)?
                    .table_definition_by_id(table_id)?
                    .column_name_to_id(column_name)?;
                self.columns.insert(column_id, id);
                Some(id)
            })
            .unwrap_or_else(|| {
                let new_id = ColumnId::new();
                self.columns.insert(column_id, new_id);
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
                        self.map_table_chunks(chunks),
                    )
                })
                .collect(),
            min_time_ns: from.min_time_ns,
            max_time_ns: from.max_time_ns,
        }
    }

    fn map_table_chunks(&mut self, from: TableChunks) -> TableChunks {
        TableChunks {
            min_time: from.min_time,
            max_time: from.max_time,
            chunk_time_to_chunk: from
                .chunk_time_to_chunk
                .into_iter()
                .map(|(time, chunk)| (time, self.map_table_chunk(chunk)))
                .collect(),
        }
    }

    fn map_table_chunk(&mut self, from: TableChunk) -> TableChunk {
        TableChunk {
            rows: from.rows.into_iter().map(|row| self.map_row(row)).collect(),
        }
    }

    fn map_row(&mut self, row: Row) -> Row {
        Row {
            time: row.time,
            fields: row
                .fields
                .into_iter()
                .map(|field| self.map_field(field))
                .collect(),
        }
    }

    fn map_field(&mut self, from: Field) -> Field {
        Field {
            id: self
                .columns
                .get(&from.id)
                .copied()
                .expect("write batch contained unseen column"),
            value: from.value,
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
                database_name: def.database_name,
            }),
            CatalogOp::CreateTable(def) => {
                let table_id = self.map_table_or_new(
                    target_catalog,
                    database_id,
                    &def.table_name,
                    def.table_id,
                );
                CatalogOp::CreateTable(WalTableDefinition {
                    database_id,
                    database_name: def.database_name,
                    table_name: Arc::clone(&def.table_name),
                    table_id,
                    field_definitions: self.map_field_definitions(
                        target_catalog,
                        database_id,
                        table_id,
                        def.field_definitions,
                    ),
                    key: def.key,
                })
            }
            CatalogOp::AddFields(def) => {
                let table_id = self.map_table_or_new(
                    target_catalog,
                    database_id,
                    &def.table_name,
                    def.table_id,
                );
                CatalogOp::AddFields(FieldAdditions {
                    database_name: def.database_name,
                    database_id,
                    table_name: Arc::clone(&def.table_name),
                    table_id,
                    field_definitions: self.map_field_definitions(
                        target_catalog,
                        database_id,
                        table_id,
                        def.field_definitions,
                    ),
                })
            }

            CatalogOp::CreateLastCache(def) => CatalogOp::CreateLastCache(LastCacheDefinition {
                table_id: self.map_table_or_new(
                    target_catalog,
                    database_id,
                    &def.table,
                    def.table_id,
                ),
                table: def.table,
                name: def.name,
                key_columns: def
                    .key_columns
                    .into_iter()
                    .map(|id| {
                        self.columns
                            .get(&id)
                            .copied()
                            .expect("create last cache operation contained invalid key column id")
                    })
                    .collect(),
                value_columns: match def.value_columns {
                    LastCacheValueColumnsDef::Explicit { columns } => {
                        LastCacheValueColumnsDef::Explicit {
                            columns: columns
                                .into_iter()
                                .map(|id| {
                                    self.columns.get(&id).copied().expect(
                                    "create last cache operation contained invalid value column id",
                                )
                                })
                                .collect(),
                        }
                    }
                    LastCacheValueColumnsDef::AllNonKeyColumns => {
                        LastCacheValueColumnsDef::AllNonKeyColumns
                    }
                },
                count: def.count,
                ttl: def.ttl,
            }),
            // TODO: if the table doesn't exist locally, do we need to bother with
            // deleting it?
            CatalogOp::DeleteLastCache(def) => CatalogOp::DeleteLastCache(LastCacheDelete {
                table_name: Arc::clone(&def.table_name),
                table_id: self.map_table_or_new(
                    target_catalog,
                    database_id,
                    &def.table_name,
                    def.table_id,
                ),
                name: def.name,
            }),
        }
    }

    fn map_field_definitions(
        &mut self,
        target_catalog: &Catalog,
        database_id: DbId,
        table_id: TableId,
        field_definitions: Vec<FieldDefinition>,
    ) -> Vec<FieldDefinition> {
        field_definitions
            .into_iter()
            .map(|def| FieldDefinition {
                name: Arc::clone(&def.name),
                id: self.map_column_or_new(
                    target_catalog,
                    database_id,
                    table_id,
                    def.name.as_ref(),
                    def.id,
                ),
                data_type: def.data_type,
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use std::{ops::Deref, sync::Arc};

    use influxdb3_id::{ColumnId, DbId, TableId};
    use pretty_assertions::assert_eq;
    use schema::InfluxColumnType;

    use crate::catalog::{Catalog, DatabaseSchema, Error, TableDefinition};

    fn create_table<C, N>(name: &str, cols: C) -> TableDefinition
    where
        C: IntoIterator<Item = (ColumnId, N, InfluxColumnType)>,
        N: Into<Arc<str>>,
    {
        TableDefinition::new(
            TableId::new(),
            name.into(),
            cols.into_iter()
                .map(|(id, name, ty)| (id, name.into(), ty))
                .collect(),
            None,
        )
        .expect("create a TableDefinition")
    }

    fn create_table_with_series_key<C, N, SK>(
        name: &str,
        cols: C,
        series_key: SK,
    ) -> TableDefinition
    where
        C: IntoIterator<Item = (ColumnId, N, InfluxColumnType)>,
        N: Into<Arc<str>>,
        SK: IntoIterator<Item = ColumnId>,
    {
        TableDefinition::new(
            TableId::new(),
            name.into(),
            cols.into_iter()
                .map(|(id, name, ty)| (id, name.into(), ty))
                .collect(),
            Some(series_key.into_iter().collect()),
        )
        .expect("create a TableDefinition with a series key")
    }

    fn create_catalog(name: &str) -> Arc<Catalog> {
        let host_name = format!("host-{name}").as_str().into();
        let instance_name = format!("instance-{name}").as_str().into();
        let cat = Catalog::new(host_name, instance_name);
        let tbl = create_table(
            "bar",
            [
                (ColumnId::new(), "t1", InfluxColumnType::Tag),
                (ColumnId::new(), "t2", InfluxColumnType::Tag),
                (
                    ColumnId::new(),
                    "f1",
                    InfluxColumnType::Field(schema::InfluxFieldType::Boolean),
                ),
            ],
        );
        let mut db = DatabaseSchema::new(DbId::new(), "foo".into());
        db.table_map
            .insert(tbl.table_id, Arc::clone(&tbl.table_name));
        db.tables.insert(tbl.table_id, Arc::new(tbl));
        cat.insert_database(db);
        cat.into()
    }

    #[test]
    fn merge_two_identical_catalogs() {
        let a = create_catalog("a");
        // clone inner so that b is a carbon copy of a:
        let b = Arc::new(Catalog::from_inner(a.clone_inner()));
        let b_to_a_map = a.merge(Arc::clone(&b)).unwrap();
        insta::assert_yaml_snapshot!(a);
        insta::with_settings!({
            sort_maps => true
        }, {
            insta::assert_yaml_snapshot!(b_to_a_map);
        });
        let a_to_b_map = b.merge(a).unwrap();
        insta::assert_yaml_snapshot!(b);
        insta::with_settings!({
            sort_maps => true
        }, {
            insta::assert_yaml_snapshot!(a_to_b_map);
        });
    }

    #[test]
    fn merge_two_catalogs_with_same_content_different_ids() {
        let a = create_catalog("a");
        // b will have the same content as a, but will have assigned different IDs:
        let b = create_catalog("b");
        let b_to_a_map = a.merge(Arc::clone(&b)).unwrap();
        insta::assert_yaml_snapshot!(a);
        insta::with_settings!({
            sort_maps => true
        }, {
            insta::assert_yaml_snapshot!(b_to_a_map);
        });
        let a_to_b_map = b.merge(a).unwrap();
        insta::assert_yaml_snapshot!(b);
        insta::with_settings!({
            sort_maps => true
        }, {
            insta::assert_yaml_snapshot!(a_to_b_map);
        });
    }

    #[test]
    fn merge_catalog_with_new_database() {
        let a = create_catalog("a");
        let b = create_catalog("b");
        b.db_or_create("biz").unwrap();
        // check the db by name in b:
        assert_eq!(DbId::from(2), b.db_name_to_id("biz").unwrap());
        let b_to_a_map = a.merge(Arc::clone(&b)).unwrap();
        // only use insta for the map here as the nesting of the catalog structure makes it
        // difficult to use snapshots, due to there being several nested bi-directional maps,
        // which don't seem to play nice with insta's sorting mechanisms.
        insta::with_settings!({ sort_maps => true }, {
            insta::assert_yaml_snapshot!(b_to_a_map);
        });
        // check the db by name in a after merge:
        assert_eq!(DbId::from(3), a.db_name_to_id("biz").unwrap());
        let a_to_b_map = b.merge(a).unwrap();
        insta::with_settings!({ sort_maps => true }, {
            insta::assert_yaml_snapshot!(a_to_b_map);
        });
    }

    #[test]
    fn merge_catalog_with_new_table() {
        let a = create_catalog("a");
        let b = create_catalog("b");
        let new_tbl = create_table(
            "doh",
            [
                (ColumnId::new(), "t3", InfluxColumnType::Tag),
                (
                    ColumnId::new(),
                    "f2",
                    InfluxColumnType::Field(schema::InfluxFieldType::Integer),
                ),
            ],
        );
        let mut db = b.db_schema("foo").unwrap().deref().clone();
        db.insert_table(new_tbl.table_id, Arc::new(new_tbl));
        b.insert_database(db);
        // check the db/table by name in b:
        {
            let (db_id, db_schema) = b.db_schema_and_id("foo").unwrap();
            assert_eq!(DbId::from(1), db_id);
            assert_eq!(TableId::from(2), db_schema.table_name_to_id("doh").unwrap());
        }
        let b_to_a_map = a.merge(Arc::clone(&b)).unwrap();
        // check the db/table by name in a after merge:
        {
            let (db_id, db_schema) = a.db_schema_and_id("foo").unwrap();
            assert_eq!(DbId::from(0), db_id);
            assert_eq!(TableId::from(3), db_schema.table_name_to_id("doh").unwrap());
        }
        insta::with_settings!({ sort_maps => true }, {
            insta::assert_yaml_snapshot!(b_to_a_map);
        });
        let a_to_b_map = b.merge(a).unwrap();
        insta::with_settings!({ sort_maps => true }, {
            insta::assert_yaml_snapshot!(a_to_b_map);
        });
    }

    #[test]
    fn merge_incompatible_catalog_field_type_mismatch() {
        let a = create_catalog("a");
        let b = create_catalog("b");
        // Add a new table to a:
        {
            let new_tbl = create_table(
                "doh",
                [
                    (ColumnId::new(), "t3", InfluxColumnType::Tag),
                    (
                        ColumnId::new(),
                        "f2",
                        InfluxColumnType::Field(schema::InfluxFieldType::UInteger),
                    ),
                ],
            );
            let mut db = a.db_schema("foo").unwrap().deref().clone();
            db.table_map
                .insert(new_tbl.table_id, Arc::clone(&new_tbl.table_name));
            db.tables.insert(new_tbl.table_id, Arc::new(new_tbl));
            a.insert_database(db);
        }
        // Add a similar table to b, but in this case, the f2 field is an Integer, not UInteger
        {
            let new_tbl = create_table(
                "doh",
                [
                    (ColumnId::new(), "t3", InfluxColumnType::Tag),
                    (
                        ColumnId::new(),
                        "f2",
                        InfluxColumnType::Field(schema::InfluxFieldType::Integer),
                    ),
                ],
            );
            let mut db = b.db_schema("foo").unwrap().deref().clone();
            db.table_map
                .insert(new_tbl.table_id, Arc::clone(&new_tbl.table_name));
            db.tables.insert(new_tbl.table_id, Arc::new(new_tbl));
            b.insert_database(db);
        }
        let err = a
            .merge(b)
            .expect_err("merge should fail for incompatible field type");
        assert!(matches!(err, Error::FieldTypeMismatch { .. }));
    }

    #[test]
    fn merge_incompatible_catalog_series_key_mismatch() {
        let a = create_catalog("a");
        let b = create_catalog("b");
        // Add a new table to a:
        {
            let new_tbl = create_table_with_series_key(
                "doh",
                [
                    (ColumnId::from(10), "t1", InfluxColumnType::Tag),
                    (ColumnId::from(20), "t2", InfluxColumnType::Tag),
                    (
                        ColumnId::from(30),
                        "f2",
                        InfluxColumnType::Field(schema::InfluxFieldType::Boolean),
                    ),
                ],
                [ColumnId::from(10), ColumnId::from(20)],
            );
            let mut db = a.db_schema("foo").unwrap().deref().clone();
            db.table_map
                .insert(new_tbl.table_id, Arc::clone(&new_tbl.table_name));
            db.tables.insert(new_tbl.table_id, Arc::new(new_tbl));
            a.insert_database(db);
        }
        // Add a similar table to b, but in this case, the f2 field is an Integer, not UInteger
        {
            let new_tbl = create_table_with_series_key(
                "doh",
                [
                    (ColumnId::from(100), "t1", InfluxColumnType::Tag),
                    (ColumnId::from(200), "t2", InfluxColumnType::Tag),
                    (ColumnId::from(300), "t3", InfluxColumnType::Tag),
                    (
                        ColumnId::from(400),
                        "f2",
                        InfluxColumnType::Field(schema::InfluxFieldType::Boolean),
                    ),
                ],
                // series key has an extra tag column
                [
                    ColumnId::from(100),
                    ColumnId::from(200),
                    ColumnId::from(300),
                ],
            );
            let mut db = b.db_schema("foo").unwrap().deref().clone();
            db.table_map
                .insert(new_tbl.table_id, Arc::clone(&new_tbl.table_name));
            db.tables.insert(new_tbl.table_id, Arc::new(new_tbl));
            b.insert_database(db);
        }
        let err = a
            .merge(b)
            .expect_err("merge should fail for incompatible series key");
        let Error::Other(e) = err else {
            panic!("incorrect error type");
        };
        let err = e.to_string();
        assert_eq!("the series key from the other catalog's table does not match that of the local catalog", err);
    }
}
