use anyhow::anyhow;
use bimap::BiHashMap;
use hashbrown::{HashMap, HashSet};
use indexmap::IndexMap;
use influxdb3_wal::{
    CatalogBatch, CatalogOp, DatabaseDefinition, DeleteDatabaseDefinition, DeleteTableDefinition,
    DistinctCacheDefinition, DistinctCacheDelete, Field, FieldAdditions, FieldDefinition,
    LastCacheDefinition, LastCacheDelete, LastCacheValueColumnsDef, OrderedCatalogBatch, Row,
    TableChunk, TableChunks, WalContents, WalOp, WalTableDefinition, WriteBatch,
};
use schema::InfluxColumnType;
use serde::{Deserialize, Serialize};
use std::{
    ops::{Deref, DerefMut},
    sync::Arc,
};

use influxdb3_id::{ColumnId, DbId, TableId};

use crate::catalog::{Catalog, DatabaseSchema, Error, InnerCatalog, Result, TableDefinition};

use super::ColumnDefinition;

impl Catalog {
    /// Merge the `other` [`Catalog`] with this one, producing a [`CatalogIdMap`] that maps the
    /// identifiers from the other catalog onto identifiers in this one.
    ///
    /// This uses the names of entities to determine a match, so a database named "foo" in the other
    /// catalog will be merged into a database called "foo" in this catalog, or if "foo" does not
    /// yet exist in this catalog, then it will be created, and a new identifier will be generated
    /// for "foo". This logic applies down the hierarchy of the catalog, so tables and columns will
    /// be treated similarly.
    pub fn merge(&self, other: Arc<Catalog>) -> Result<CatalogIdMap> {
        self.inner().write().merge(other)
    }
}

impl InnerCatalog {
    /// Iterate over the [`DatabaseSchema`] in the `other` [`Catalog`] and merge them into this one.
    ///
    /// [`DatabaseSchema`] that do not exist locally will be created and added to the catalog with
    /// new identifiers generated for the [`DatabaseSchema`] as well as its nested tables, columns,
    /// etc.
    ///
    /// Databases that exist locally but not in the other catalog can be ignored, since the purpose
    /// of the generated [`CatalogIdMap`] is to map identifiers from the `other` catalog to this one.
    fn merge(&mut self, other: Arc<Catalog>) -> Result<CatalogIdMap> {
        let mut id_map = CatalogIdMap::default();
        for other_db in other.list_db_schema() {
            if let Some(this_db) = self
                .db_map
                .get_by_right(&other_db.name)
                .and_then(|id| self.databases.get(id))
                .cloned()
            {
                let (mapped_ids, new_db_if_updated) = this_db.merge(other_db)?;
                if let Some(s) = new_db_if_updated {
                    self.databases.insert(s.id, Arc::clone(&s));
                    self.sequence = self.sequence.next();
                    self.updated = true;
                    self.db_map.insert(s.id, Arc::clone(&s.name));
                }
                id_map.extend(mapped_ids);
            } else {
                let id = DbId::new();
                let db = DatabaseSchema::new(id, Arc::clone(&other_db.name));
                let (mapped_ids, new_db_if_updated) = db.merge(other_db)?;
                let new_db = new_db_if_updated.unwrap_or_else(|| Arc::new(db));
                self.databases.insert(new_db.id, Arc::clone(&new_db));
                self.sequence = self.sequence.next();
                self.updated = true;
                self.db_map.insert(new_db.id, Arc::clone(&new_db.name));
                id_map.extend(mapped_ids);
            }
        }
        Ok(id_map)
    }
}

impl DatabaseSchema {
    /// Merge the `other` [`DatabaseSchema`] with this one, producing a [`CatalogIdMap`] that maps
    /// the identifiers from the `other` schema onto this one, as well as a new `Arc<DatabaseSchema>`
    /// if the merge resulted in changes to the schema. If the merge does not result in changes, then
    /// `None` will be returned.
    fn merge(&self, other: Arc<Self>) -> Result<(CatalogIdMap, Option<Arc<Self>>)> {
        let mut id_map = CatalogIdMap::default();
        id_map.dbs.insert(other.id, self.id);
        let mut new_or_updated_tables = IndexMap::new();
        // track new table ids for producing the updated schema:
        let mut new_or_updated_table_ids = HashSet::new();
        for other_tbl in other.tables.values() {
            if let Some(this_tbl) = self.table_definition(other_tbl.table_name.as_ref()) {
                let (mapped_ids, new_tbl_if_updated) = this_tbl.merge(Arc::clone(other_tbl))?;
                if let Some(new_tbl) = new_tbl_if_updated {
                    new_or_updated_table_ids.insert(new_tbl.table_id);
                    new_or_updated_tables.insert(new_tbl.table_id, Arc::new(new_tbl));
                }
                id_map.extend(mapped_ids);
            } else {
                let (mapped_ids, new_tbl_def) =
                    TableDefinition::new_from_other_writer(Arc::clone(other_tbl))?;
                new_or_updated_tables.insert(new_tbl_def.table_id, new_tbl_def);
                id_map.extend(mapped_ids);
            };
        }
        let updated_schema = (!new_or_updated_tables.is_empty()).then(|| {
            let tables = self
                .tables
                .iter()
                // bring along tables from current schema that were not updated:
                .filter(|(id, _)| !new_or_updated_table_ids.contains(*id))
                .map(|(id, def)| (*id, Arc::clone(def)))
                // bring in the new tables that were updated by the merge:
                .chain(new_or_updated_tables)
                .collect::<IndexMap<TableId, Arc<TableDefinition>>>();
            let table_map = tables
                .iter()
                .map(|(id, def)| (*id, Arc::clone(&def.table_name)))
                .collect();
            Arc::new(DatabaseSchema {
                id: self.id,
                name: Arc::clone(&self.name),
                tables: tables.into(),
                table_map,
                deleted: self.deleted,
                processing_engine_triggers: self.processing_engine_triggers.clone(),
            })
        });
        Ok((id_map, updated_schema))
    }
}

impl TableDefinition {
    /// Merge the `other` [`TableDefinition`] into this one, producing a [`CatalogIdMap`] that maps
    /// identifiers from the `other` table onto this one, as well as a new `TableDefinition` if the
    /// merge results in updates, i.e., new columns being added.
    fn merge(&self, other: Arc<Self>) -> Result<(CatalogIdMap, Option<Self>)> {
        let mut id_map = CatalogIdMap::default();
        id_map.tables.insert(other.table_id, self.table_id);
        let mut new_columns = Vec::new();
        for other_col in other.columns.values() {
            if let Some(this_col_def) = self
                .column_map
                .get_by_right(&other_col.name)
                .and_then(|id| self.columns.get(id))
            {
                let mapped_ids = this_col_def
                    .check_compatibility_and_map_id(self.table_name.as_ref(), other_col)?;
                id_map.extend(mapped_ids);
            } else {
                let new_col_id = ColumnId::new();
                let other_cloned = other_col.clone();
                let new_col = ColumnDefinition {
                    id: new_col_id,
                    ..other_cloned
                };
                new_columns.push((new_col_id, Arc::clone(&new_col.name), new_col.data_type));
                id_map.columns.insert(other_col.id, new_col_id);
            };
        }

        // validate the series keys match for existing tables:
        let mapped_series_key = other
            .series_key
            .iter()
            .map(|id| {
                id_map.map_column_id(id).ok_or(Error::Other(anyhow!(
                    "other table series key contained invalid id"
                )))
            })
            .collect::<Result<Vec<ColumnId>>>()?;
        if mapped_series_key != self.series_key {
            return Err(Error::Other(anyhow!("the series key from the other catalog's table does not match that of the local catalog")));
        }

        // merge in any new last cache definitions
        let mut new_last_caches: Vec<LastCacheDefinition> = vec![];
        for (merge_name, merge_last_cache) in other.last_caches() {
            if let Some(local_last_cache) = self.last_caches.get(&merge_name) {
                let mapped_other_last_cache_def =
                    id_map.map_last_cache_definition_column_ids(merge_last_cache)?;
                if local_last_cache != &mapped_other_last_cache_def {
                    return Err(
                        Error::Other(
                            anyhow!("the last cache definition from the other writer does not match the local one.\n\
                            local: {local_last_cache:#?}\n\
                            other: {mapped_other_last_cache_def:#?}")
                    ));
                }
            } else {
                new_last_caches
                    .push(id_map.map_last_cache_definition_column_ids(merge_last_cache)?);
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

    /// Create a new [`TableDefinition`] from another catalog, i.e., the `other` `TableDefinition`.
    ///
    /// This is intended for the case where a table exists in the other catalog that does not exist
    /// locally. The main difference being that it will recreate the series key (with mapped column
    /// ids).
    fn new_from_other_writer(other: Arc<Self>) -> Result<(CatalogIdMap, Arc<Self>)> {
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
                .iter()
                .map(|other_id| id_map
                    .map_column_id(other_id)
                    .ok_or_else(|| Error::Other(
                        anyhow!("the table from the other catalog contained an invalid column in its series key (id: {other_id})")
                    ))
                )
                .collect::<Result<Vec<ColumnId>>>()?;

        // get the names of all the series keys
        let series_key_names = series_key
            .clone()
            .into_iter()
            .map(|id| {
                column_map
                    .get_by_left(&id)
                    .cloned()
                    // NOTE: should this be an error instead of panic?
                    .expect("invalid column id in series key definition")
            })
            .collect::<Vec<Arc<str>>>();

        // map the ids from last cache definitions
        let mut last_caches = HashMap::new();
        for (name, lc) in &other.last_caches {
            last_caches.insert(
                Arc::clone(name),
                id_map.map_last_cache_definition_column_ids(lc)?,
            );
        }

        // map the ids from the meta cache definitions
        let mut distinct_caches = HashMap::new();
        for (name, mc) in &other.distinct_caches {
            distinct_caches.insert(
                Arc::clone(name),
                id_map.map_meta_cache_definition_column_ids(mc)?,
            );
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
                series_key_names,
                last_caches,
                distinct_caches,
                deleted: other.deleted,
            }),
        ))
    }
}

impl ColumnDefinition {
    /// Check for compatibility between two column definitions and produce a [`CatalogIdMap`] that
    /// contains a mapping of the [`ColumnId`] from the `other` catalog to this one.
    fn check_compatibility_and_map_id(
        &self,
        table_name: impl Into<String>,
        other: &Self,
    ) -> Result<CatalogIdMap> {
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

/// Holds a set of maps for mapping identifiers from another writer's [`Catalog`] onto the one being
/// used locally. This is generated via methods on the various catalog types, e.g., see
/// [`DatabaseSchema::merge`], [`TableDefinition::merge`], etc.
#[derive(Debug, Default, Serialize, Deserialize, Clone, Eq, PartialEq)]
pub struct CatalogIdMap {
    dbs: HashMap<DbId, DbId>,
    tables: HashMap<TableId, TableId>,
    columns: HashMap<ColumnId, ColumnId>,
}

impl CatalogIdMap {
    /// Extend this [`CatalogIdMap`] with the contents of another.
    pub fn extend(&mut self, mut other: Self) {
        self.dbs.extend(other.dbs.drain());
        self.tables.extend(other.tables.drain());
        self.columns.extend(other.columns.drain());
    }

    /// Map the given `database_id` from the other writer's [`Catalog`] to the local one
    pub fn map_db_id(&self, database_id: &DbId) -> Option<DbId> {
        self.dbs.get(database_id).copied()
    }

    /// Map the given `table_id` from the other writer's [`Catalog`] to the local one
    pub fn map_table_id(&self, table_id: &TableId) -> Option<TableId> {
        self.tables.get(table_id).copied()
    }

    /// Map the given `column_id` from the other writer's [`Catalog`] to the local one
    pub fn map_column_id(&self, column_id: &ColumnId) -> Option<ColumnId> {
        self.columns.get(column_id).copied()
    }

    /// Map the given `other_db_id` from another writer's [`Catalog`] to the local one, or if it
    /// does not exist locally, create a new [`DbId`]. This first checks the internal ID map, then
    /// checks to see if `other_db_name` exists in the local catalog to find a match before generating
    /// a new [`DbId`].
    fn map_db_or_new(
        &mut self,
        local_catalog: &Catalog,
        other_db_name: &str,
        other_db_id: DbId,
    ) -> Mapped<DbId> {
        self.map_db_id(&other_db_id)
            .or_else(|| {
                let id = local_catalog.db_name_to_id(other_db_name)?;
                self.dbs.insert(other_db_id, id);
                Some(id)
            })
            .map(Mapped::Ignore)
            .unwrap_or_else(|| {
                let new_id = DbId::new();
                self.dbs.insert(other_db_id, new_id);
                Mapped::Replay(new_id)
            })
    }

    /// Map the given `other_table_id` from another writer's [`Catalog`] to the local one, or if it
    /// does not exist locally, create a new [`TableId`]. This first checks the internal ID map, then
    /// checks to see if `other_table_name` exists in the local catalog to find a match before generating
    /// a new [`TableId`].
    fn map_table_or_new(
        &mut self,
        local_catalog: &Catalog,
        local_db_id: DbId,
        other_table_name: &str,
        other_table_id: TableId,
    ) -> Mapped<TableId> {
        self.map_table_id(&other_table_id)
            .or_else(|| {
                let id = local_catalog
                    .db_schema_by_id(&local_db_id)?
                    .table_name_to_id(other_table_name)?;
                self.tables.insert(other_table_id, id);
                Some(id)
            })
            .map(Mapped::Ignore)
            .unwrap_or_else(|| {
                let new_id = TableId::new();
                self.tables.insert(other_table_id, new_id);
                Mapped::Replay(new_id)
            })
    }

    /// Map the given `other_column_id` from another writer's [`Catalog`] to the local one, or if it
    /// does not exist locally, create a new [`ColumnId`]. This first checks the internal ID map, then
    /// checks to see if `other_column_name` exists in the local catalog to find a match before generating
    /// a new [`ColumnId`].
    #[allow(clippy::too_many_arguments)]
    fn map_column_or_new(
        &mut self,
        local_catalog: &Catalog,
        local_db_id: DbId,
        local_table_id: TableId,
        local_table_name: Arc<str>,
        other_column_name: &str,
        other_column_id: ColumnId,
        other_column_type: InfluxColumnType,
    ) -> Result<Mapped<ColumnId>> {
        self.map_column_id(&other_column_id)
            .map(Mapped::Ignore)
            .map(Ok)
            .or_else(|| {
                let local_tbl_def = local_catalog
                    .db_schema_by_id(&local_db_id)?
                    .table_definition_by_id(&local_table_id)?;
                let (id, def) = local_tbl_def.column_id_and_definition(other_column_name)?;
                if def.data_type != other_column_type {
                    return Some(Err(Error::FieldTypeMismatch {
                        table_name: local_table_name.to_string(),
                        column_name: def.name.to_string(),
                        existing: def.data_type,
                        attempted: other_column_type,
                    }));
                }
                self.columns.insert(other_column_id, id);
                Some(Ok(Mapped::Ignore(id)))
            })
            .unwrap_or_else(|| {
                let new_id = ColumnId::new();
                self.columns.insert(other_column_id, new_id);
                Ok(Mapped::Replay(new_id))
            })
    }

    /// Take the [`WalContents`] from another writer and use the internal maps to map all of its
    /// identifiers over to their local equivalent.
    pub fn map_wal_contents(
        &mut self,
        target_catalog: &Catalog,
        wal_contents: WalContents,
    ) -> Result<WalContents> {
        let ops = wal_contents
            .ops
            .into_iter()
            .filter_map(|op| {
                self.map_wal_op(target_catalog, op)
                    .map(Mapped::into_replay)
                    .transpose()
            })
            .collect::<Result<Vec<WalOp>>>()?;
        Ok(WalContents {
            persist_timestamp_ms: wal_contents.persist_timestamp_ms,
            min_timestamp_ns: wal_contents.min_timestamp_ns,
            max_timestamp_ns: wal_contents.max_timestamp_ns,
            wal_file_number: wal_contents.wal_file_number,
            ops,
            snapshot: wal_contents.snapshot,
        })
    }

    /// Map a [`WalOp`] from a replicated writer to the local writer
    ///
    /// This leverages the [`Mapped`] type to determine [`CatalogOp`]s that need to be replayed or
    /// ignored. In general, if a `CatalogOp` results in something new being created on the local
    /// writer, then we want to apply the `CatalogOp`, while those that would result in a no-op should
    /// be ignored.
    ///
    /// Although `CatalogOp`s are handled idempotently, we need to make this distinction for high
    /// availability configurations where more than one writer is accepting writes and replicating
    /// the WAL from another write-enabled writer. Consider the following scenario:
    ///
    /// * Writer `A` is accepting writes and replicating the WAL of writer `B`
    /// * Writer `B` is accepting writes and replicating the WAL of writer `A`
    ///
    /// When `A` receives a write that alters the catalog, e.g., one that creates a table, its WAL
    /// will look like so:
    ///
    /// ```text
    ///            Writer `A` WAL
    /// ╔═════════════════════════════════╗
    /// ║ CreateTable{id: 0, name: 'foo'} ║
    /// ║ Write{table_id: 0, data: ...}   ║
    /// ╚═════════════════════════════════╝
    /// ```
    ///
    /// When `B` then replays `A`'s WAL, it will see the `CreateTable{id: 0, name: 'foo'}`, and map
    /// it to the local equivalent, i.e., by creating a new [`TableId`] for the 'foo' table:
    ///
    /// ```text
    ///            Writer `A` WAL                                     Writer `B` WAL
    /// ╔═════════════════════════════════╗   Map/Replay   ╔═════════════════════════════════╗
    /// ║ CreateTable{id: 0, name: 'foo'} ║ ━━━━━━━━━━━━━▶ ║ CreateTable{id: 1, name: 'foo'} ║
    /// ║ Write{table_id: 0, data: ...}   ║                ╚═════════════════════════════════╝
    /// ╚═════════════════════════════════╝                ╔═════════════════════════════════╗
    ///                                                    ║ Writer `B` ⏐ {                  ║
    ///                                                    ║ Catalog ID ⏐   tables: {0 ━▶ 1} ║
    ///                                                    ║     Map    ⏐ }                  ║
    ///                                                    ╚═════════════════════════════════╝
    /// ```
    ///
    /// The mapped `CreateTable` operation will be wrapped in `Mapped::Replay`, so that it gets sent
    /// and persisted to the local WAL on `B`. Hence why `B`'s WAL now contains a `CreateTable` op.
    ///
    /// `B` does not insert the `Write` operation to its WAL; instead, it only maps the write op
    /// and buffers its contents to its in-memory buffer.
    ///
    /// Now, when `A` is replaying `B`'s WAL, it will see the `CreateTable` operation, and will use
    /// that to extend its internal [`CatalogIdMap`]; however, `A` does not need to send the op to
    /// its WAL. Doing so would lead to an infinite loop between `A` and `B` inserting the same op
    /// repeatedly to their respective WAL. Instead, `A` will map the `CreateTable` operation, thus
    /// updating its `CatalogIdMap`, but then wrap it in `Mapped::Ignore`.
    ///
    /// ```text
    ///            Writer `A` WAL
    /// ╔═════════════════════════════════╗
    /// ║ CreateTable{id: 0, name: 'foo'} ║                           Writer `B` WAL
    /// ║ Write{table_id: 0, data: ... }  ║   Map/Replay  ╔═════════════════════════════════╗
    /// ║                                 ║ ◀━━━━━━━━━━━━ ║ CreateTable{id: 1, name: 'foo'} ║
    /// ╚═════════════════════════════════╝               ╚═════════════════════════════════╝
    /// ╔═════════════════════════════════╗
    /// ║ Writer `A` ⏐ {                  ║
    /// ║ Catalog ID ⏐   tables: {1 ━▶ 0} ║
    /// ║     Map    ⏐ }                  ║
    /// ╚═════════════════════════════════╝
    /// ```
    ///
    /// `A` can now replay writes to the 'foo' table that were made to `B` and buffer them to its
    /// in-memory replicated buffer:
    ///
    /// ```text
    ///            Writer `A` WAL                                    Writer `B` WAL
    /// ╔═════════════════════════════════╗               ╔═════════════════════════════════╗
    /// ║ CreateTable{id: 0, name: 'foo'} ║               ║ CreateTable{id: 1, name: 'foo'} ║
    /// ║ Write{table_id: 0, data: ... }  ║   Map/Replay  ║             ...                 ║
    /// ║                                 ║ ◀━━━━━━━━━━━━ ║ Write{table_id: 1, data: ... }  ║
    /// ╚═════════════════════════════════╝               ╚═════════════════════════════════╝
    /// ```
    fn map_wal_op(&mut self, target_catalog: &Catalog, op: WalOp) -> Result<Mapped<WalOp>> {
        Ok(match op {
            WalOp::Write(write_batch) => self
                .map_write_batch(target_catalog, write_batch)
                .map(WalOp::Write),
            WalOp::Catalog(catalog_batch) => self
                .map_catalog_batch(target_catalog, catalog_batch)?
                .map(WalOp::Catalog),
            WalOp::Noop(details) => Mapped::Ignore(WalOp::Noop(details)),
        })
    }

    fn map_write_batch(
        &mut self,
        target_catalog: &Catalog,
        from: WriteBatch,
    ) -> Mapped<WriteBatch> {
        let database_id =
            *self.map_db_or_new(target_catalog, &from.database_name, from.database_id);
        // we always want to replay write operations:
        Mapped::Replay(WriteBatch {
            database_id,
            database_name: Arc::clone(&from.database_name),
            table_chunks: from
                .table_chunks
                .into_iter()
                .map(|(table_id, chunks)| {
                    (
                        self.tables.get(&table_id).copied().unwrap_or_else(|| {
                            panic!(
                                "write batch encountered for unseen table, \
                                table id: {table_id}, existing table map: {table_map:?}",
                                table_map = self.tables
                            )
                        }),
                        self.map_table_chunks(chunks),
                    )
                })
                .collect(),
            min_time_ns: from.min_time_ns,
            max_time_ns: from.max_time_ns,
        })
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

    fn map_catalog_batch(
        &mut self,
        target_catalog: &Catalog,
        from: OrderedCatalogBatch,
    ) -> Result<Mapped<OrderedCatalogBatch>> {
        let sequence_number = from.sequence_number();
        let from_batch = from.into_batch();
        let database_id = self.map_db_or_new(
            target_catalog,
            &from_batch.database_name,
            from_batch.database_id,
        );
        Ok(from_batch
            .ops
            .into_iter()
            .map(|op| self.map_catalog_op(target_catalog, database_id.copied(), op))
            .collect::<Result<Mapped<Vec<CatalogOp>>>>()?
            .map(|ops| {
                OrderedCatalogBatch::new(
                    CatalogBatch {
                        database_id: *database_id,
                        database_name: Arc::clone(&from_batch.database_name),
                        time_ns: from_batch.time_ns,
                        ops,
                    },
                    sequence_number,
                )
            }))
    }

    fn map_catalog_op(
        &mut self,
        target_catalog: &Catalog,
        database_id: Mapped<DbId>,
        op: CatalogOp,
    ) -> Result<Mapped<CatalogOp>> {
        let mapped_op = match op {
            CatalogOp::CreateDatabase(def) => database_id.map(|database_id| {
                CatalogOp::CreateDatabase(DatabaseDefinition {
                    database_id,
                    database_name: def.database_name,
                })
            }),
            CatalogOp::DeleteDatabase(def) => {
                let database_id = *database_id;
                let mapped_op = CatalogOp::DeleteDatabase(DeleteDatabaseDefinition {
                    database_id,
                    database_name: Arc::clone(&def.database_name),
                    deletion_time: def.deletion_time,
                });
                if target_catalog
                    .db_schema_by_id(&database_id)
                    .is_some_and(|db| db.deleted)
                {
                    Mapped::Ignore(mapped_op)
                } else {
                    Mapped::Replay(mapped_op)
                }
            }
            CatalogOp::CreateTable(def) => {
                let database_id = *database_id;
                let table_id = self.map_table_or_new(
                    target_catalog,
                    database_id,
                    &def.table_name,
                    def.table_id,
                );
                let field_definitions = self.map_field_definitions(
                    target_catalog,
                    database_id,
                    *table_id,
                    Arc::clone(&def.table_name),
                    def.field_definitions,
                )?;
                let key = def
                    .key
                    .into_iter()
                    .map(|column_id| {
                        self.map_column_id(&column_id).ok_or_else(|| {
                            Error::Other(anyhow!("invalid column id in series key: {column_id}"))
                        })
                    })
                    .collect::<Result<Vec<_>>>()?;
                let is_new = table_id.is_replay() || field_definitions.is_replay();
                let op = CatalogOp::CreateTable(WalTableDefinition {
                    database_id,
                    database_name: def.database_name,
                    table_name: Arc::clone(&def.table_name),
                    table_id: table_id.into_inner(),
                    field_definitions: field_definitions.into_inner(),
                    key,
                });
                if is_new {
                    Mapped::Replay(op)
                } else {
                    Mapped::Ignore(op)
                }
            }
            CatalogOp::DeleteTable(def) => {
                let database_id = *database_id;
                let table_id = self.map_table_id(&def.table_id).ok_or_else(|| {
                    Error::Other(anyhow!(
                        "attempted to delete a table that does not exist locally"
                    ))
                })?;
                let tbl_def = target_catalog
                    .db_schema_by_id(&database_id)
                    .and_then(|db| db.table_definition_by_id(&table_id))
                    // this unwrap is okay because the table_id has been determined valid when
                    // mapped above:
                    .unwrap();
                let mapped_op = CatalogOp::DeleteTable(DeleteTableDefinition {
                    database_id,
                    database_name: Arc::clone(&def.database_name),
                    table_id,
                    table_name: Arc::clone(&def.table_name),
                    deletion_time: def.deletion_time,
                });
                // if the table is already deleted then we can ignore this operation
                if tbl_def.deleted {
                    Mapped::Ignore(mapped_op)
                } else {
                    Mapped::Replay(mapped_op)
                }
            }
            CatalogOp::AddFields(def) => {
                let table_id =
                    self.map_table_id(&def.table_id)
                        .ok_or_else(|| Error::TableNotFound {
                            db_name: Arc::clone(&def.database_name),
                            table_name: Arc::clone(&def.table_name),
                        })?;
                let database_id = *database_id;
                self.map_field_definitions(
                    target_catalog,
                    database_id,
                    table_id,
                    Arc::clone(&def.table_name),
                    def.field_definitions,
                )?
                .map(|field_definitions| {
                    CatalogOp::AddFields(FieldAdditions {
                        database_name: def.database_name,
                        database_id,
                        table_name: Arc::clone(&def.table_name),
                        table_id,
                        field_definitions,
                    })
                })
            }
            // The following last cache ops will throw an error if they are for a table that does
            // not exist. If such an op is encountered, that would indicate that the WAL is
            // corrupted on the other writer, since there should always be a CreateTable op preceding
            // one of these last cache ops.
            CatalogOp::CreateLastCache(def) => {
                let mapped_def = self.map_last_cache_definition_column_ids(&def)?;
                let tbl_def = target_catalog
                    .db_schema_by_id(&database_id)
                    .and_then(|db| db.table_definition_by_id(&mapped_def.table_id))
                    // this unwrap is okay as the call to map the last cache definition would
                    // catch a missing table:
                    .unwrap();
                if let Some(local_def) = tbl_def.last_caches.get(&def.name) {
                    if local_def != &mapped_def {
                        return Err(Error::Other(
                                anyhow!("WAL contained a CreateLastCache operation with a last cache \
                                name that already exists in the local catalog, but is not compatible. \
                                This means that the catalogs for these two writers have diverged and the \
                                last cache named '{name}' needs to be removed on one of the writers.",
                                name = def.name
                            )));
                    }
                    Mapped::Ignore(CatalogOp::CreateLastCache(mapped_def))
                } else {
                    Mapped::Replay(CatalogOp::CreateLastCache(mapped_def))
                }
            }
            CatalogOp::DeleteLastCache(def) => {
                let table_id = self.map_table_id(&def.table_id).ok_or_else(|| {
                    Error::Other(anyhow!(
                        "attempted to delete a last cache for a table that does not exist locally"
                    ))
                })?;
                let tbl_def = target_catalog
                    .db_schema_by_id(&database_id)
                    .and_then(|db| db.table_definition_by_id(&table_id))
                    .unwrap();
                let mapped_op = CatalogOp::DeleteLastCache(LastCacheDelete {
                    table_name: Arc::clone(&def.table_name),
                    table_id,
                    name: Arc::clone(&def.name),
                });
                if tbl_def.last_caches.get(&def.name).is_some() {
                    Mapped::Replay(mapped_op)
                } else {
                    Mapped::Ignore(mapped_op)
                }
            }
            CatalogOp::CreateDistinctCache(def) => {
                let mapped_def = self.map_meta_cache_definition_column_ids(&def)?;
                let tbl_def = target_catalog
                    .db_schema_by_id(&database_id)
                    .and_then(|db| db.table_definition_by_id(&mapped_def.table_id))
                    // unwrap is okay here because the call to map the meta cache definition would
                    // have caught an invalid table id etc.
                    .unwrap();
                if let Some(local_def) = tbl_def.distinct_caches.get(&def.cache_name) {
                    if local_def != &mapped_def {
                        return Err(Error::Other(anyhow!(
                            "WAL contained a CreateMetaCache operation with a metadata cache \
                            name that already exists in the local catalog, but is not compatible. \
                            This means that the catalogs for these two writers have diverged and the \
                            meta cache named '{name}' needs to be removed on one of the writers.",
                            name = def.cache_name
                        )));
                    }
                    Mapped::Ignore(CatalogOp::CreateDistinctCache(mapped_def))
                } else {
                    Mapped::Replay(CatalogOp::CreateDistinctCache(mapped_def))
                }
            }
            CatalogOp::DeleteDistinctCache(def) => {
                let table_id = self.map_table_id(&def.table_id).ok_or_else(|| {
                        Error::Other(anyhow!(
                            "attempted to delete a metadata cache for a table that does not exist locally"
                        ))
                    })?;
                let tbl_def = target_catalog
                    .db_schema_by_id(&database_id)
                    .and_then(|db| db.table_definition_by_id(&table_id))
                    .unwrap();
                let mapped_op = CatalogOp::DeleteDistinctCache(DistinctCacheDelete {
                    table_name: Arc::clone(&def.table_name),
                    table_id,
                    cache_name: Arc::clone(&def.cache_name),
                });
                if tbl_def.distinct_caches.get(&def.cache_name).is_some() {
                    Mapped::Replay(mapped_op)
                } else {
                    Mapped::Ignore(mapped_op)
                }
            }
            CatalogOp::CreateTrigger(create_trigger) => {
                Mapped::Ignore(CatalogOp::CreateTrigger(create_trigger))
            }
            CatalogOp::DeleteTrigger(delete_trigger) => {
                Mapped::Ignore(CatalogOp::DeleteTrigger(delete_trigger))
            }
            CatalogOp::EnableTrigger(enable_trigger) => {
                Mapped::Ignore(CatalogOp::EnableTrigger(enable_trigger))
            }
            CatalogOp::DisableTrigger(disable_trigger) => {
                Mapped::Ignore(CatalogOp::DisableTrigger(disable_trigger))
            }
        };
        Ok(mapped_op)
    }

    fn map_field_definitions(
        &mut self,
        target_catalog: &Catalog,
        database_id: DbId,
        table_id: TableId,
        table_name: Arc<str>,
        field_definitions: Vec<FieldDefinition>,
    ) -> Result<Mapped<Vec<FieldDefinition>>> {
        field_definitions
            .into_iter()
            .map(|def| {
                Ok(self
                    .map_column_or_new(
                        target_catalog,
                        database_id,
                        table_id,
                        Arc::clone(&table_name),
                        def.name.as_ref(),
                        def.id,
                        def.data_type.into(),
                    )?
                    .map(|id| FieldDefinition {
                        name: Arc::clone(&def.name),
                        id,
                        data_type: def.data_type,
                    }))
            })
            .collect()
    }

    /// Map all of the [`ColumnId`]s within a [`LastCacheDefinition`] from another writer's catalog to
    /// their respective IDs on the local catalog. This assumes that all columns have been mapped
    /// already via the caller, and will throw errors if there are columns that cannot be found.
    fn map_last_cache_definition_column_ids(
        &self,
        def: &LastCacheDefinition,
    ) -> Result<LastCacheDefinition> {
        let table_id = self.tables.get(&def.table_id).copied().ok_or_else(|| {
            Error::Other(anyhow!("last cache definition contained invalid table id"))
        })?;
        let key_columns = def
            .key_columns
            .iter()
            .map(|id| {
                self.map_column_id(id).ok_or_else(|| {
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
                        self.map_column_id(id).ok_or_else(|| {
                            Error::Other(anyhow!(
                                "last cache definition contained invalid value column id"
                            ))
                        })
                    })
                    .collect::<Result<Vec<ColumnId>>>()?;
                LastCacheValueColumnsDef::Explicit { columns }
            }
            LastCacheValueColumnsDef::AllNonKeyColumns => {
                LastCacheValueColumnsDef::AllNonKeyColumns
            }
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

    fn map_meta_cache_definition_column_ids(
        &self,
        def: &DistinctCacheDefinition,
    ) -> Result<DistinctCacheDefinition> {
        let table_id = self.tables.get(&def.table_id).copied().ok_or_else(|| {
            Error::Other(anyhow!("meta cache definition contained invalid table id"))
        })?;
        let column_ids = def
            .column_ids
            .iter()
            .map(|id| {
                self.map_column_id(id).ok_or_else(|| {
                    Error::Other(anyhow!("meta cache definition contained invalid column id"))
                })
            })
            .collect::<Result<Vec<ColumnId>>>()?;
        Ok(DistinctCacheDefinition {
            table_id,
            table_name: Arc::clone(&def.table_name),
            cache_name: Arc::clone(&def.cache_name),
            column_ids,
            max_cardinality: def.max_cardinality,
            max_age_seconds: def.max_age_seconds,
        })
    }
}

#[derive(Debug)]
enum Mapped<T> {
    Replay(T),
    Ignore(T),
}

impl<T> Deref for Mapped<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        match self {
            Mapped::Replay(t) => t,
            Mapped::Ignore(t) => t,
        }
    }
}

impl<T> DerefMut for Mapped<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        match self {
            Mapped::Replay(t) => t,
            Mapped::Ignore(t) => t,
        }
    }
}

impl<A, T: FromIterator<A>> FromIterator<Mapped<A>> for Mapped<T> {
    fn from_iter<I: IntoIterator<Item = Mapped<A>>>(iter: I) -> Self {
        let mut is_new = false;
        let vals = iter
            .into_iter()
            .map(|val| match val {
                Mapped::Replay(v) => {
                    is_new = true;
                    v
                }
                Mapped::Ignore(v) => v,
            })
            .collect();
        if is_new {
            Mapped::Replay(vals)
        } else {
            Mapped::Ignore(vals)
        }
    }
}

impl<T> Mapped<T> {
    fn map<U, F>(self, f: F) -> Mapped<U>
    where
        F: FnOnce(T) -> U,
    {
        match self {
            Mapped::Replay(t) => Mapped::Replay(f(t)),
            Mapped::Ignore(t) => Mapped::Ignore(f(t)),
        }
    }

    fn copied(&self) -> Mapped<T>
    where
        T: Copy,
    {
        match self {
            Mapped::Replay(t) => Mapped::Replay(*t),
            Mapped::Ignore(t) => Mapped::Ignore(*t),
        }
    }

    fn into_inner(self) -> T {
        match self {
            Mapped::Replay(v) => v,
            Mapped::Ignore(v) => v,
        }
    }

    fn into_replay(self) -> Option<T> {
        match self {
            Mapped::Replay(t) => Some(t),
            Mapped::Ignore(_) => None,
        }
    }

    fn is_replay(&self) -> bool {
        match self {
            Mapped::Replay(_) => true,
            Mapped::Ignore(_) => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{ops::Deref, sync::Arc};

    use influxdb3_id::{ColumnId, DbId, TableId};
    use influxdb3_wal::{LastCacheDefinition, LastCacheSize, LastCacheValueColumnsDef};
    use pretty_assertions::assert_eq;
    use schema::{InfluxColumnType, InfluxFieldType};
    use test_helpers::assert_contains;

    use crate::catalog::{Catalog, DatabaseSchema, Error, TableDefinition};

    fn create_table<C, N, SK>(name: &str, cols: C, series_key: SK) -> TableDefinition
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
            series_key.into_iter().collect(),
        )
        .expect("create a TableDefinition with a series key")
    }

    fn create_catalog(name: &str) -> Arc<Catalog> {
        let host_name = format!("host-{name}").as_str().into();
        let instance_name = format!("instance-{name}").as_str().into();
        let cat = Catalog::new(host_name, instance_name);
        let t1_col_id = ColumnId::new();
        let t2_col_id = ColumnId::new();
        let tbl = create_table(
            "bar",
            [
                (t1_col_id, "t1", InfluxColumnType::Tag),
                (t2_col_id, "t2", InfluxColumnType::Tag),
                (
                    ColumnId::new(),
                    "f1",
                    InfluxColumnType::Field(schema::InfluxFieldType::Boolean),
                ),
            ],
            [t1_col_id, t2_col_id],
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
        let t3_col_id = ColumnId::new();
        let new_tbl = create_table(
            "doh",
            [
                (t3_col_id, "t3", InfluxColumnType::Tag),
                (
                    ColumnId::new(),
                    "f2",
                    InfluxColumnType::Field(schema::InfluxFieldType::Integer),
                ),
            ],
            [t3_col_id],
        );
        let mut db = b.db_schema("foo").unwrap().deref().clone();
        db.insert_table(new_tbl.table_id, Arc::new(new_tbl))
            .unwrap();
        b.insert_database(db);
        // check the db/table by name in b:
        {
            let (db_id, db_schema) = b.db_id_and_schema("foo").unwrap();
            assert_eq!(DbId::from(1), db_id);
            assert_eq!(TableId::from(2), db_schema.table_name_to_id("doh").unwrap());
        }
        let b_to_a_map = a.merge(Arc::clone(&b)).unwrap();
        // check the db/table by name in a after merge:
        {
            let (db_id, db_schema) = a.db_id_and_schema("foo").unwrap();
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
                    (ColumnId::from(0), "t3", InfluxColumnType::Tag),
                    (
                        ColumnId::from(1),
                        "f2",
                        InfluxColumnType::Field(schema::InfluxFieldType::UInteger),
                    ),
                ],
                [ColumnId::from(0)],
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
                    (ColumnId::from(2), "t3", InfluxColumnType::Tag),
                    (
                        ColumnId::from(3),
                        "f2",
                        InfluxColumnType::Field(schema::InfluxFieldType::Integer),
                    ),
                ],
                [ColumnId::from(2)],
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
            let new_tbl = create_table(
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
            let new_tbl = create_table(
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

    #[test]
    fn merge_db_schema_not_updating_all_tables() {
        let a = create_catalog("a");
        let b = create_catalog("b");
        // add a table to a:
        {
            let new_tbl = create_table(
                "doh",
                [
                    (ColumnId::from(10), "t1", InfluxColumnType::Tag),
                    (ColumnId::from(20), "t2", InfluxColumnType::Tag),
                    (
                        ColumnId::from(30),
                        "f2",
                        InfluxColumnType::Field(InfluxFieldType::Boolean),
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
        // add the same table to b, but with an additional field "f3" - this will
        // update the existing table definition in a when we do the merge:
        {
            let new_tbl = create_table(
                "doh",
                [
                    (ColumnId::from(100), "t1", InfluxColumnType::Tag),
                    (ColumnId::from(200), "t2", InfluxColumnType::Tag),
                    (
                        ColumnId::from(300),
                        "f2",
                        InfluxColumnType::Field(InfluxFieldType::Boolean),
                    ),
                    (
                        ColumnId::from(400),
                        "f3",
                        InfluxColumnType::Field(InfluxFieldType::String),
                    ),
                ],
                [ColumnId::from(100), ColumnId::from(200)],
            );
            let mut db = b.db_schema("foo").unwrap().deref().clone();
            db.table_map
                .insert(new_tbl.table_id, Arc::clone(&new_tbl.table_name));
            db.tables.insert(new_tbl.table_id, Arc::new(new_tbl));
            b.insert_database(db);
        }

        let b_to_a_map = a.merge(Arc::clone(&b)).unwrap();
        insta::with_settings!({ sort_maps => true }, {
            insta::assert_yaml_snapshot!(b_to_a_map);
        });
        let db = a.db_schema("foo").unwrap();
        insta::with_settings!({ sort_maps => true }, {
            insta::assert_json_snapshot!(db);
        })
    }

    #[test]
    fn merge_with_incompatible_last_cache() {
        let a = create_catalog("a");
        let b = create_catalog("b");
        let cache_name = "test_cache";
        // add a last cache to 'a':
        {
            let mut db = a.db_schema("foo").unwrap().deref().clone();
            let mut tbl = db.table_definition("bar").unwrap().deref().clone();
            tbl.add_last_cache(LastCacheDefinition {
                table_id: tbl.table_id,
                table: Arc::clone(&tbl.table_name),
                name: cache_name.into(),
                key_columns: vec![tbl.column_name_to_id_unchecked("t1")],
                value_columns: LastCacheValueColumnsDef::AllNonKeyColumns,
                count: LastCacheSize::new(1).unwrap(),
                ttl: 3600,
            });
            db.insert_table(tbl.table_id, Arc::new(tbl)).unwrap();
            a.insert_database(db);
        }
        // add a last cache to 'b' but with a different configuration:
        {
            let mut db = b.db_schema("foo").unwrap().deref().clone();
            let mut tbl = db.table_definition("bar").unwrap().deref().clone();
            tbl.add_last_cache(LastCacheDefinition {
                table_id: tbl.table_id,
                table: Arc::clone(&tbl.table_name),
                name: cache_name.into(),
                // this def has no key columns:
                key_columns: vec![],
                value_columns: LastCacheValueColumnsDef::AllNonKeyColumns,
                count: LastCacheSize::new(1).unwrap(),
                ttl: 3600,
            });
            db.insert_table(tbl.table_id, Arc::new(tbl)).unwrap();
            b.insert_database(db);
        }
        let err = a
            .merge(b)
            .expect_err("merging incompatible last caches should fail");
        assert_contains!(
            err.to_string(),
            "the last cache definition from the other writer does not match the local one."
        );
    }
}
