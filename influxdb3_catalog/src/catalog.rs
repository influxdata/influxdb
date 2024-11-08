//! Implementation of the Catalog that sits entirely in memory.

use crate::catalog::Error::TableNotFound;
use bimap::BiHashMap;
use hashbrown::HashMap;
use indexmap::IndexMap;
use influxdb3_id::{ColumnId, DbId, SerdeVecMap, TableId};
use influxdb3_wal::{
    CatalogBatch, CatalogOp, FieldAdditions, LastCacheDefinition, LastCacheDelete,
};
use influxdb_line_protocol::FieldValue;
use observability_deps::tracing::info;
use parking_lot::RwLock;
use schema::{InfluxColumnType, InfluxFieldType, Schema, SchemaBuilder};
use serde::{Deserialize, Serialize, Serializer};
use std::collections::BTreeMap;
use std::sync::Arc;
use thiserror::Error;

pub mod pro;

#[derive(Debug, Error)]
pub enum Error {
    #[error("catalog updated elsewhere")]
    CatalogUpdatedElsewhere,

    #[error(
        "Update to schema would exceed number of columns per table limit of {} columns",
        Catalog::NUM_COLUMNS_PER_TABLE_LIMIT - 1
    )]
    TooManyColumns,

    #[error(
        "Update to schema would exceed number of tables limit of {} tables",
        Catalog::NUM_TABLES_LIMIT
    )]
    TooManyTables,

    #[error(
        "Adding a new database would exceed limit of {} databases",
        Catalog::NUM_DBS_LIMIT
    )]
    TooManyDbs,

    #[error("Table {} not in DB schema for {}", table_name, db_name)]
    TableNotFound {
        db_name: Arc<str>,
        table_name: Arc<str>,
    },

    #[error(
        "Field type mismatch on table {} column {}. Existing column is {} but attempted to add {}",
        table_name,
        column_name,
        existing,
        attempted
    )]
    FieldTypeMismatch {
        table_name: String,
        column_name: String,
        existing: InfluxColumnType,
        attempted: InfluxColumnType,
    },

    #[error(
        "Series key mismatch on table {}. Existing table has {}",
        table_name,
        existing
    )]
    SeriesKeyMismatch {
        table_name: String,
        existing: String,
    },

    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

pub const TIME_COLUMN_NAME: &str = "time";

/// The sequence number of a batch of WAL operations.
#[derive(
    Debug, Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
pub struct CatalogSequenceNumber(u32);

impl CatalogSequenceNumber {
    pub fn new(id: u32) -> Self {
        Self(id)
    }

    pub fn next(&self) -> Self {
        Self(self.0 + 1)
    }

    pub fn as_u32(&self) -> u32 {
        self.0
    }
}

#[derive(Debug)]
pub struct Catalog {
    inner: RwLock<InnerCatalog>,
}

impl PartialEq for Catalog {
    fn eq(&self, other: &Self) -> bool {
        self.inner.read().eq(&other.inner.read())
    }
}

impl Serialize for Catalog {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.inner.read().serialize(serializer)
    }
}

impl Catalog {
    /// Limit for the number of Databases that InfluxDB Edge can have
    pub(crate) const NUM_DBS_LIMIT: usize = 5;
    /// Limit for the number of columns per table that InfluxDB Edge can have
    pub(crate) const NUM_COLUMNS_PER_TABLE_LIMIT: usize = 500;
    /// Limit for the number of tables across all DBs that InfluxDB Edge can have
    pub(crate) const NUM_TABLES_LIMIT: usize = 2000;

    pub fn new(host_id: Arc<str>, instance_id: Arc<str>) -> Self {
        Self {
            inner: RwLock::new(InnerCatalog::new(host_id, instance_id)),
        }
    }

    pub fn from_inner(inner: InnerCatalog) -> Self {
        Self {
            inner: RwLock::new(inner),
        }
    }

    pub fn apply_catalog_batch(&self, catalog_batch: &CatalogBatch) -> Result<()> {
        self.inner.write().apply_catalog_batch(catalog_batch)
    }

    pub fn db_or_create(&self, db_name: &str) -> Result<Arc<DatabaseSchema>> {
        let db = match self.db_schema(db_name) {
            Some(db) => db,
            None => {
                let mut inner = self.inner.write();

                if inner.databases.len() >= Self::NUM_DBS_LIMIT {
                    return Err(Error::TooManyDbs);
                }

                info!("return new db {}", db_name);
                let db_id = DbId::new();
                let db_name = db_name.into();
                let db = Arc::new(DatabaseSchema::new(db_id, Arc::clone(&db_name)));
                inner.databases.insert(db.id, Arc::clone(&db));
                inner.sequence = inner.sequence.next();
                inner.updated = true;
                inner.db_map.insert(db_id, db_name);
                db
            }
        };

        Ok(db)
    }

    pub fn db_name_to_id(&self, db_name: &str) -> Option<DbId> {
        self.inner.read().db_map.get_by_right(db_name).copied()
    }

    pub fn db_id_to_name(&self, db_id: &DbId) -> Option<Arc<str>> {
        self.inner.read().db_map.get_by_left(db_id).map(Arc::clone)
    }

    pub fn db_schema(&self, db_name: &str) -> Option<Arc<DatabaseSchema>> {
        self.db_schema_and_id(db_name).map(|(_, schema)| schema)
    }

    pub fn db_schema_by_id(&self, db_id: &DbId) -> Option<Arc<DatabaseSchema>> {
        self.inner.read().databases.get(db_id).cloned()
    }

    pub fn db_schema_and_id(&self, db_name: &str) -> Option<(DbId, Arc<DatabaseSchema>)> {
        let inner = self.inner.read();
        let db_id = inner.db_map.get_by_right(db_name)?;
        inner
            .databases
            .get(db_id)
            .map(|db| (*db_id, Arc::clone(db)))
    }

    pub fn db_names(&self) -> Vec<String> {
        self.inner
            .read()
            .databases
            .values()
            .map(|db| db.name.to_string())
            .collect()
    }

    pub fn list_db_schema(&self) -> Vec<Arc<DatabaseSchema>> {
        self.inner.read().databases.values().cloned().collect()
    }

    pub fn sequence_number(&self) -> CatalogSequenceNumber {
        self.inner.read().sequence
    }

    pub fn clone_inner(&self) -> InnerCatalog {
        self.inner.read().clone()
    }

    pub fn add_last_cache(&self, db_id: DbId, table_id: TableId, last_cache: LastCacheDefinition) {
        let mut inner = self.inner.write();
        let mut db = inner
            .databases
            .get(&db_id)
            .expect("db should exist")
            .as_ref()
            .clone();
        let mut table = db
            .tables
            .get(&table_id)
            .expect("table should exist")
            .as_ref()
            .clone();
        table.add_last_cache(last_cache);
        db.tables.insert(table_id, Arc::new(table));
        inner.databases.insert(db_id, Arc::new(db));
        inner.sequence = inner.sequence.next();
        inner.updated = true;
    }

    pub fn delete_last_cache(&self, db_id: DbId, table_id: TableId, name: &str) {
        let mut inner = self.inner.write();
        let mut db = inner
            .databases
            .get(&db_id)
            .expect("db should exist")
            .as_ref()
            .clone();
        let mut table = db
            .tables
            .get(&table_id)
            .expect("table should exist")
            .as_ref()
            .clone();
        table.remove_last_cache(name);
        db.tables.insert(table_id, Arc::new(table));
        inner.databases.insert(db_id, Arc::new(db));
        inner.sequence = inner.sequence.next();
        inner.updated = true;
    }

    pub fn instance_id(&self) -> Arc<str> {
        Arc::clone(&self.inner.read().instance_id)
    }

    pub fn host_id(&self) -> Arc<str> {
        Arc::clone(&self.inner.read().host_id)
    }

    #[cfg(test)]
    pub fn db_exists(&self, db_id: DbId) -> bool {
        self.inner.read().db_exists(db_id)
    }

    pub fn insert_database(&self, db: DatabaseSchema) {
        let mut inner = self.inner.write();
        inner.db_map.insert(db.id, Arc::clone(&db.name));
        inner.databases.insert(db.id, Arc::new(db));
        inner.sequence = inner.sequence.next();
        inner.updated = true;
    }

    pub fn is_updated(&self) -> bool {
        self.inner.read().updated
    }

    /// After the catalog has been persisted, mark it as not updated, if the sequence number
    /// matches. If it doesn't then the catalog was updated while persistence was running and
    /// will need to be persisted on the next snapshot.
    pub fn set_updated_false_if_sequence_matches(&self, sequence_number: CatalogSequenceNumber) {
        let mut inner = self.inner.write();
        if inner.sequence == sequence_number {
            inner.updated = false;
        }
    }

    pub fn inner(&self) -> &RwLock<InnerCatalog> {
        &self.inner
    }
}

#[serde_with::serde_as]
#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone, Default)]
pub struct InnerCatalog {
    /// The catalog is a map of databases with their table schemas
    databases: SerdeVecMap<DbId, Arc<DatabaseSchema>>,
    sequence: CatalogSequenceNumber,
    /// The host_id is the prefix that is passed in when starting up (`host_identifier_prefix`)
    host_id: Arc<str>,
    /// The instance_id uniquely identifies the instance that generated the catalog
    instance_id: Arc<str>,
    /// If true, the catalog has been updated since the last time it was serialized
    #[serde(skip)]
    updated: bool,
    #[serde_as(as = "DbMapAsArray")]
    db_map: BiHashMap<DbId, Arc<str>>,
}

serde_with::serde_conv!(
    DbMapAsArray,
    BiHashMap<DbId, Arc<str>>,
    |map: &BiHashMap<DbId, Arc<str>>| {
        map.iter().fold(Vec::new(), |mut acc, (id, name)| {
            acc.push(DbMap {
                db_id: *id,
                name: Arc::clone(&name)
            });
            acc
        })
    },
    |vec: Vec<DbMap>| -> Result<_, std::convert::Infallible> {
        Ok(vec.into_iter().fold(BiHashMap::new(), |mut acc, db| {
            acc.insert(db.db_id, db.name);
            acc
        }))
    }
);

#[derive(Debug, Serialize, Deserialize)]
struct DbMap {
    db_id: DbId,
    name: Arc<str>,
}

#[derive(Debug, Serialize, Deserialize)]
struct TableMap {
    table_id: TableId,
    name: Arc<str>,
}

serde_with::serde_conv!(
    TableMapAsArray,
    BiHashMap<TableId, Arc<str>>,
    |map: &BiHashMap<TableId, Arc<str>>| {
        map.iter().fold(Vec::new(), |mut acc, (table_id, name)| {
            acc.push(TableMap {
                table_id: *table_id,
                name: Arc::clone(&name)
            });
            acc
        })
    },
    |vec: Vec<TableMap>| -> Result<_, std::convert::Infallible> {
        let mut map = BiHashMap::new();
        for item in vec {
            map.insert(item.table_id, item.name);
        }
        Ok(map)
    }
);

impl InnerCatalog {
    pub(crate) fn new(host_id: Arc<str>, instance_id: Arc<str>) -> Self {
        Self {
            databases: SerdeVecMap::new(),
            sequence: CatalogSequenceNumber::new(0),
            host_id,
            instance_id,
            updated: false,
            db_map: BiHashMap::new(),
        }
    }

    pub fn sequence_number(&self) -> CatalogSequenceNumber {
        self.sequence
    }

    pub fn table_count(&self) -> usize {
        self.databases.values().map(|db| db.tables.len()).sum()
    }

    /// Applies the `CatalogBatch` while validating that all updates are compatible. If updates
    /// have already been applied, the sequence number and updated tracker are not updated.
    pub fn apply_catalog_batch(&mut self, catalog_batch: &CatalogBatch) -> Result<()> {
        let table_count = self.table_count();

        if let Some(db) = self.databases.get(&catalog_batch.database_id) {
            let existing_table_count = db.tables.len();

            if let Some(new_db) = db.new_if_updated_from_batch(catalog_batch)? {
                let new_table_count = new_db.tables.len() - existing_table_count;
                if table_count + new_table_count > Catalog::NUM_TABLES_LIMIT {
                    return Err(Error::TooManyTables);
                }
                let new_db = Arc::new(new_db);
                self.databases.insert(new_db.id, Arc::clone(&new_db));
                self.sequence = self.sequence.next();
                self.updated = true;
                self.db_map.insert(new_db.id, Arc::clone(&new_db.name));
            }
        } else {
            if self.databases.len() >= Catalog::NUM_DBS_LIMIT {
                return Err(Error::TooManyDbs);
            }

            let new_db = DatabaseSchema::new_from_batch(catalog_batch)?;
            if table_count + new_db.tables.len() > Catalog::NUM_TABLES_LIMIT {
                return Err(Error::TooManyTables);
            }

            let new_db = Arc::new(new_db);
            self.databases.insert(new_db.id, Arc::clone(&new_db));
            self.sequence = self.sequence.next();
            self.updated = true;
            self.db_map.insert(new_db.id, Arc::clone(&new_db.name));
        }

        Ok(())
    }

    pub fn db_exists(&self, db_id: DbId) -> bool {
        self.databases.contains_key(&db_id)
    }
}

#[derive(Debug, Eq, PartialEq, Clone)]
pub struct DatabaseSchema {
    pub id: DbId,
    pub name: Arc<str>,
    /// The database is a map of tables
    pub tables: SerdeVecMap<TableId, Arc<TableDefinition>>,
    pub table_map: BiHashMap<TableId, Arc<str>>,
}

impl DatabaseSchema {
    pub fn new(id: DbId, name: Arc<str>) -> Self {
        Self {
            id,
            name,
            tables: Default::default(),
            table_map: BiHashMap::new(),
        }
    }

    /// Validates the updates in the `CatalogBatch` are compatible with this schema. If
    /// everything is compatible and there are no updates to the existing schema, None will be
    /// returned, otherwise a new `DatabaseSchema` will be returned with the updates applied.
    pub fn new_if_updated_from_batch(&self, catalog_batch: &CatalogBatch) -> Result<Option<Self>> {
        let mut updated_or_new_tables = SerdeVecMap::new();

        for catalog_op in &catalog_batch.ops {
            match catalog_op {
                CatalogOp::CreateDatabase(_) => (),
                CatalogOp::CreateTable(table_definition) => {
                    let new_or_existing_table = updated_or_new_tables
                        .get(&table_definition.table_id)
                        .or_else(|| self.tables.get(&table_definition.table_id));
                    if let Some(existing_table) = new_or_existing_table {
                        if let Some(new_table) =
                            existing_table.new_if_definition_adds_new_fields(table_definition)?
                        {
                            updated_or_new_tables.insert(new_table.table_id, Arc::new(new_table));
                        }
                    } else {
                        let new_table = TableDefinition::new_from_op(table_definition);
                        updated_or_new_tables.insert(new_table.table_id, Arc::new(new_table));
                    }
                }
                CatalogOp::AddFields(field_additions) => {
                    let Some(new_or_existing_table) = updated_or_new_tables
                        .get(&field_additions.table_id)
                        .or_else(|| self.tables.get(&field_additions.table_id))
                    else {
                        return Err(Error::TableNotFound {
                            db_name: Arc::clone(&field_additions.database_name),
                            table_name: Arc::clone(&field_additions.table_name),
                        });
                    };
                    if let Some(new_table) =
                        new_or_existing_table.new_if_field_additions_add_fields(field_additions)?
                    {
                        updated_or_new_tables.insert(new_table.table_id, Arc::new(new_table));
                    }
                }
                CatalogOp::CreateLastCache(last_cache_definition) => {
                    let new_or_existing_table = updated_or_new_tables
                        .get(&last_cache_definition.table_id)
                        .or_else(|| self.tables.get(&last_cache_definition.table_id));

                    let table = new_or_existing_table.ok_or(TableNotFound {
                        db_name: Arc::clone(&self.name),
                        table_name: Arc::clone(&last_cache_definition.table),
                    })?;

                    if let Some(new_table) =
                        table.new_if_last_cache_definition_is_new(last_cache_definition)
                    {
                        updated_or_new_tables.insert(new_table.table_id, Arc::new(new_table));
                    }
                }
                CatalogOp::DeleteLastCache(last_cache_deletion) => {
                    let new_or_existing_table = updated_or_new_tables
                        .get(&last_cache_deletion.table_id)
                        .or_else(|| self.tables.get(&last_cache_deletion.table_id));

                    let table = new_or_existing_table.ok_or(TableNotFound {
                        db_name: Arc::clone(&self.name),
                        table_name: Arc::clone(&last_cache_deletion.table_name),
                    })?;

                    if let Some(new_table) =
                        table.new_if_last_cache_deletes_existing(last_cache_deletion)
                    {
                        updated_or_new_tables.insert(new_table.table_id, Arc::new(new_table));
                    }
                }
            }
        }

        if updated_or_new_tables.is_empty() {
            Ok(None)
        } else {
            for (table_id, table_def) in &self.tables {
                if !updated_or_new_tables.contains_key(table_id) {
                    updated_or_new_tables.insert(*table_id, Arc::clone(table_def));
                }
            }

            // With the final list of updated/new tables update the current mapping
            let new_table_maps = updated_or_new_tables
                .iter()
                .map(|(table_id, table_def)| (*table_id, Arc::clone(&table_def.table_name)))
                .collect();

            Ok(Some(Self {
                id: self.id,
                name: Arc::clone(&self.name),
                tables: updated_or_new_tables,
                table_map: new_table_maps,
            }))
        }
    }

    pub fn new_from_batch(catalog_batch: &CatalogBatch) -> Result<Self> {
        let db_schema = Self::new(
            catalog_batch.database_id,
            Arc::clone(&catalog_batch.database_name),
        );
        let new_db = db_schema
            .new_if_updated_from_batch(catalog_batch)?
            .expect("database must be new");
        Ok(new_db)
    }

    /// Insert a [`TableDefinition`] to the `tables` map and also update the `table_map`
    pub fn insert_table(
        &mut self,
        table_id: TableId,
        table_def: Arc<TableDefinition>,
    ) -> Option<Arc<TableDefinition>> {
        self.table_map
            .insert(table_id, Arc::clone(&table_def.table_name));
        self.tables.insert(table_id, table_def)
    }

    pub fn table_schema(&self, table_name: impl Into<Arc<str>>) -> Option<Schema> {
        self.table_schema_and_id(table_name)
            .map(|(_, schema)| schema.clone())
    }

    pub fn table_schema_by_id(&self, table_id: &TableId) -> Option<Schema> {
        self.tables
            .get(table_id)
            .map(|table| table.influx_schema())
            .cloned()
    }

    pub fn table_schema_and_id(
        &self,
        table_name: impl Into<Arc<str>>,
    ) -> Option<(TableId, Schema)> {
        self.table_map
            .get_by_right(&table_name.into())
            .and_then(|table_id| {
                self.tables
                    .get(table_id)
                    .map(|table_def| (*table_id, table_def.influx_schema().clone()))
            })
    }

    pub fn table_definition(
        &self,
        table_name: impl Into<Arc<str>>,
    ) -> Option<Arc<TableDefinition>> {
        self.table_map
            .get_by_right(&table_name.into())
            .and_then(|table_id| self.tables.get(table_id).cloned())
    }

    pub fn table_definition_by_id(&self, table_id: &TableId) -> Option<Arc<TableDefinition>> {
        self.tables.get(table_id).cloned()
    }

    pub fn table_definition_and_id(
        &self,
        table_name: impl Into<Arc<str>>,
    ) -> Option<(TableId, Arc<TableDefinition>)> {
        let table_id = self.table_map.get_by_right(&table_name.into())?;
        self.tables
            .get(table_id)
            .map(|table_def| (*table_id, Arc::clone(table_def)))
    }

    pub fn table_ids(&self) -> Vec<TableId> {
        self.tables.keys().cloned().collect()
    }

    pub fn table_names(&self) -> Vec<Arc<str>> {
        self.tables
            .values()
            .map(|td| Arc::clone(&td.table_name))
            .collect()
    }

    pub fn table_exists(&self, table_id: &TableId) -> bool {
        self.tables.contains_key(table_id)
    }

    pub fn tables(&self) -> impl Iterator<Item = Arc<TableDefinition>> + use<'_> {
        self.tables.values().map(Arc::clone)
    }

    pub fn table_name_to_id(&self, table_name: impl Into<Arc<str>>) -> Option<TableId> {
        self.table_map.get_by_right(&table_name.into()).copied()
    }

    pub fn table_id_to_name(&self, table_id: &TableId) -> Option<Arc<str>> {
        self.table_map.get_by_left(table_id).map(Arc::clone)
    }
}

#[derive(Debug, Eq, PartialEq, Clone)]
pub struct TableDefinition {
    pub table_id: TableId,
    pub table_name: Arc<str>,
    pub schema: Schema,
    pub columns: IndexMap<ColumnId, ColumnDefinition>,
    pub column_map: BiHashMap<ColumnId, Arc<str>>,
    pub series_key: Option<Vec<ColumnId>>,
    pub last_caches: HashMap<Arc<str>, LastCacheDefinition>,
}

impl TableDefinition {
    /// Create a new [`TableDefinition`]
    ///
    /// Ensures the provided columns will be ordered before constructing the schema.
    pub fn new(
        table_id: TableId,
        table_name: Arc<str>,
        columns: Vec<(ColumnId, Arc<str>, InfluxColumnType)>,
        series_key: Option<Vec<ColumnId>>,
    ) -> Result<Self> {
        // ensure we're under the column limit
        if columns.len() > Catalog::NUM_COLUMNS_PER_TABLE_LIMIT {
            return Err(Error::TooManyColumns);
        }

        // Use a BTree to ensure that the columns are ordered:
        let mut ordered_columns = BTreeMap::new();
        for (col_id, name, column_type) in &columns {
            ordered_columns.insert(name.as_ref(), (col_id, column_type));
        }
        let mut schema_builder = SchemaBuilder::with_capacity(columns.len());
        schema_builder.measurement(table_name.as_ref());
        let mut columns = IndexMap::with_capacity(ordered_columns.len());
        let mut column_map = BiHashMap::with_capacity(ordered_columns.len());
        for (name, (col_id, column_type)) in ordered_columns {
            schema_builder.influx_column(name, *column_type);
            let not_nullable = matches!(column_type, InfluxColumnType::Timestamp)
                || series_key.as_ref().is_some_and(|sk| sk.contains(col_id));
            columns.insert(
                *col_id,
                ColumnDefinition::new(*col_id, name, *column_type, !not_nullable),
            );
            column_map.insert(*col_id, name.into());
        }
        if let Some(sk) = series_key.clone() {
            schema_builder.with_series_key(sk.into_iter().map(|id| {
                column_map
                    .get_by_left(&id)
                    // NOTE: should this be an error instead of panic?
                    .expect("invalid column id in series key definition")
            }));
        }
        let schema = schema_builder.build().expect("schema should be valid");

        Ok(Self {
            table_id,
            table_name,
            schema,
            columns,
            column_map,
            series_key,
            last_caches: HashMap::new(),
        })
    }

    /// Create a new table definition from a catalog op
    pub fn new_from_op(table_definition: &influxdb3_wal::TableDefinition) -> Self {
        let mut columns = Vec::with_capacity(table_definition.field_definitions.len());
        for field_def in &table_definition.field_definitions {
            columns.push((
                field_def.id,
                Arc::clone(&field_def.name),
                field_def.data_type.into(),
            ));
        }
        Self::new(
            table_definition.table_id,
            Arc::clone(&table_definition.table_name),
            columns,
            table_definition.key.clone(),
        )
        .expect("tables defined from ops should not exceed column limits")
    }

    /// Validates that the `influxdb3_wal::TableDefinition` is compatible with existing and returns a new
    /// `TableDefinition` if new definition adds new fields.
    pub(crate) fn new_if_definition_adds_new_fields(
        &self,
        table_definition: &influxdb3_wal::TableDefinition,
    ) -> Result<Option<Self>> {
        // validate the series key is the same
        if table_definition.key != self.series_key {
            return Err(Error::SeriesKeyMismatch {
                table_name: self.table_name.to_string(),
                existing: self.schema.series_key().unwrap_or_default().join("/"),
            });
        }
        let mut new_fields: Vec<(ColumnId, Arc<str>, InfluxColumnType)> =
            Vec::with_capacity(table_definition.field_definitions.len());

        for field_def in &table_definition.field_definitions {
            if let Some(existing_type) = self.columns.get(&field_def.id).map(|def| def.data_type) {
                if existing_type != field_def.data_type.into() {
                    return Err(Error::FieldTypeMismatch {
                        table_name: self.table_name.to_string(),
                        column_name: field_def.name.to_string(),
                        existing: existing_type,
                        attempted: field_def.data_type.into(),
                    });
                }
            } else {
                new_fields.push((
                    field_def.id,
                    Arc::clone(&field_def.name),
                    field_def.data_type.into(),
                ));
            }
        }

        if new_fields.is_empty() {
            Ok(None)
        } else {
            let mut new_table = self.clone();
            new_table.add_columns(new_fields)?;
            Ok(Some(new_table))
        }
    }

    /// Validates that the `TableDefinition` is compatible with existing and returns a new
    /// `TableDefinition` if new definition adds new fields.
    pub(crate) fn new_if_field_additions_add_fields(
        &self,
        field_additions: &FieldAdditions,
    ) -> Result<Option<Self>> {
        let mut new_fields = Vec::with_capacity(field_additions.field_definitions.len());
        for field_def in &field_additions.field_definitions {
            if let Some(existing_type) = self.columns.get(&field_def.id).map(|def| def.data_type) {
                if existing_type != field_def.data_type.into() {
                    return Err(Error::FieldTypeMismatch {
                        table_name: self.table_name.to_string(),
                        column_name: field_def.name.to_string(),
                        existing: existing_type,
                        attempted: field_def.data_type.into(),
                    });
                }
            } else {
                new_fields.push((
                    field_def.id,
                    Arc::clone(&field_def.name),
                    field_def.data_type.into(),
                ));
            }
        }

        if new_fields.is_empty() {
            Ok(None)
        } else {
            let mut new_table = self.clone();
            new_table.add_columns(new_fields)?;
            Ok(Some(new_table))
        }
    }

    pub(crate) fn new_if_last_cache_definition_is_new(
        &self,
        last_cache_definition: &LastCacheDefinition,
    ) -> Option<Self> {
        if self.last_caches.contains_key(&last_cache_definition.name) {
            None
        } else {
            let mut new_table = self.clone();
            new_table.add_last_cache(last_cache_definition.clone());
            Some(new_table)
        }
    }

    pub(crate) fn new_if_last_cache_deletes_existing(
        &self,
        last_cache_delete: &LastCacheDelete,
    ) -> Option<Self> {
        if self.last_caches.contains_key(&last_cache_delete.name) {
            let mut new_table = self.clone();
            new_table.remove_last_cache(&last_cache_delete.name);
            Some(new_table)
        } else {
            None
        }
    }

    /// Check if the column exists in the [`TableDefinition`]
    pub fn column_exists(&self, column: impl Into<Arc<str>>) -> bool {
        self.column_map.get_by_right(&column.into()).is_some()
    }

    /// Add the columns to this [`TableDefinition`]
    ///
    /// This ensures that the resulting schema has its columns ordered
    pub fn add_columns(
        &mut self,
        columns: Vec<(ColumnId, Arc<str>, InfluxColumnType)>,
    ) -> Result<()> {
        // Use BTree to insert existing and new columns, and use that to generate the
        // resulting schema, to ensure column order is consistent:
        let mut cols = BTreeMap::new();
        for (_, col_def) in self.columns.drain(..) {
            cols.insert(Arc::clone(&col_def.name), col_def);
        }
        for (id, name, column_type) in columns {
            assert!(
                cols.insert(
                    Arc::clone(&name),
                    // any new column added by this function must be nullable:
                    ColumnDefinition::new(id, name, column_type, true)
                )
                .is_none(),
                "attempted to add existing column"
            );
        }

        // ensure we don't go over the column limit
        if cols.len() > Catalog::NUM_COLUMNS_PER_TABLE_LIMIT {
            return Err(Error::TooManyColumns);
        }

        let mut schema_builder = SchemaBuilder::with_capacity(cols.len());
        // TODO: may need to capture some schema-level metadata, currently, this causes trouble in
        // tests, so I am omitting this for now:
        // schema_builder.measurement(&self.name);
        for (name, col_def) in &cols {
            schema_builder.influx_column(name.as_ref(), col_def.data_type);
        }
        let schema = schema_builder.build().expect("schema should be valid");
        self.schema = schema;

        self.columns = cols
            .into_iter()
            .inspect(|(_, def)| {
                self.column_map.insert(def.id, Arc::clone(&def.name));
            })
            .map(|(_, def)| (def.id, def))
            .collect();

        Ok(())
    }

    pub fn index_column_ids(&self) -> Vec<ColumnId> {
        self.columns
            .iter()
            .filter_map(|(id, def)| match def.data_type {
                InfluxColumnType::Tag => Some(*id),
                InfluxColumnType::Field(_) | InfluxColumnType::Timestamp => None,
            })
            .collect()
    }

    pub fn influx_schema(&self) -> &Schema {
        &self.schema
    }

    pub fn num_columns(&self) -> usize {
        self.influx_schema().len()
    }

    pub fn field_type_by_name(&self, name: impl Into<Arc<str>>) -> Option<InfluxColumnType> {
        self.column_name_to_id(name)
            .and_then(|id| self.columns.get(&id))
            .map(|def| def.data_type)
    }

    pub fn is_v3(&self) -> bool {
        self.influx_schema().series_key().is_some()
    }

    /// Add a new last cache to this table definition
    pub fn add_last_cache(&mut self, last_cache: LastCacheDefinition) {
        self.last_caches
            .insert(Arc::clone(&last_cache.name), last_cache);
    }

    /// Remove a last cache from the table definition
    pub fn remove_last_cache(&mut self, name: &str) {
        self.last_caches.remove(name);
    }

    pub fn last_caches(&self) -> impl Iterator<Item = (Arc<str>, &LastCacheDefinition)> {
        self.last_caches
            .iter()
            .map(|(name, def)| (Arc::clone(name), def))
    }

    pub fn column_name_to_id(&self, name: impl Into<Arc<str>>) -> Option<ColumnId> {
        self.column_map.get_by_right(&name.into()).copied()
    }

    pub fn column_id_to_name(&self, id: &ColumnId) -> Option<Arc<str>> {
        self.column_map.get_by_left(id).cloned()
    }

    pub fn column_name_to_id_unchecked(&self, name: Arc<str>) -> ColumnId {
        *self
            .column_map
            .get_by_right(&name)
            .expect("Column exists in mapping")
    }

    pub fn column_id_to_name_unchecked(&self, id: &ColumnId) -> Arc<str> {
        Arc::clone(
            self.column_map
                .get_by_left(id)
                .expect("Column exists in mapping"),
        )
    }

    pub fn column_def_and_id(
        &self,
        name: impl Into<Arc<str>>,
    ) -> Option<(ColumnId, &ColumnDefinition)> {
        self.column_map
            .get_by_right(&name.into())
            .and_then(|id| self.columns.get(id).map(|def| (*id, def)))
    }
}

#[derive(Debug, Eq, PartialEq, Clone)]
pub struct ColumnDefinition {
    pub id: ColumnId,
    pub name: Arc<str>,
    pub data_type: InfluxColumnType,
    pub nullable: bool,
}

impl ColumnDefinition {
    pub fn new(
        id: ColumnId,
        name: impl Into<Arc<str>>,
        data_type: InfluxColumnType,
        nullable: bool,
    ) -> Self {
        Self {
            id,
            name: name.into(),
            data_type,
            nullable,
        }
    }
}

pub fn influx_column_type_from_field_value(fv: &FieldValue<'_>) -> InfluxColumnType {
    match fv {
        FieldValue::I64(_) => InfluxColumnType::Field(InfluxFieldType::Integer),
        FieldValue::U64(_) => InfluxColumnType::Field(InfluxFieldType::UInteger),
        FieldValue::F64(_) => InfluxColumnType::Field(InfluxFieldType::Float),
        FieldValue::String(_) => InfluxColumnType::Field(InfluxFieldType::String),
        FieldValue::Boolean(_) => InfluxColumnType::Field(InfluxFieldType::Boolean),
    }
}

#[cfg(test)]
mod tests {
    use influxdb3_wal::{create, FieldDataType};
    use pretty_assertions::assert_eq;
    use test_helpers::assert_contains;

    use super::*;

    type SeriesKey = Option<Vec<ColumnId>>;

    #[test]
    fn catalog_serialization() {
        let host_id = Arc::from("sample-host-id");
        let instance_id = Arc::from("instance-id");
        let cloned_instance_id = Arc::clone(&instance_id);
        let catalog = Catalog::new(host_id, cloned_instance_id);
        let mut database = DatabaseSchema {
            id: DbId::from(0),
            name: "test_db".into(),
            tables: SerdeVecMap::new(),
            table_map: {
                let mut map = BiHashMap::new();
                map.insert(TableId::from(1), "test_table_1".into());
                map.insert(TableId::from(2), "test_table_2".into());
                map
            },
        };
        use InfluxColumnType::*;
        use InfluxFieldType::*;
        database.tables.insert(
            TableId::from(1),
            Arc::new(
                TableDefinition::new(
                    TableId::from(1),
                    "test_table_1".into(),
                    vec![
                        (ColumnId::new(), "tag_1".into(), Tag),
                        (ColumnId::new(), "tag_2".into(), Tag),
                        (ColumnId::new(), "tag_3".into(), Tag),
                        (ColumnId::new(), "time".into(), Timestamp),
                        (ColumnId::new(), "string_field".into(), Field(String)),
                        (ColumnId::new(), "bool_field".into(), Field(Boolean)),
                        (ColumnId::new(), "i64_field".into(), Field(Integer)),
                        (ColumnId::new(), "u64_field".into(), Field(UInteger)),
                        (ColumnId::new(), "f64_field".into(), Field(Float)),
                    ],
                    SeriesKey::None,
                )
                .unwrap(),
            ),
        );
        database.tables.insert(
            TableId::from(2),
            Arc::new(
                TableDefinition::new(
                    TableId::from(2),
                    "test_table_2".into(),
                    vec![
                        (ColumnId::new(), "tag_1".into(), Tag),
                        (ColumnId::new(), "tag_2".into(), Tag),
                        (ColumnId::new(), "tag_3".into(), Tag),
                        (ColumnId::new(), "time".into(), Timestamp),
                        (ColumnId::new(), "string_field".into(), Field(String)),
                        (ColumnId::new(), "bool_field".into(), Field(Boolean)),
                        (ColumnId::new(), "i64_field".into(), Field(Integer)),
                        (ColumnId::new(), "u64_field".into(), Field(UInteger)),
                        (ColumnId::new(), "f64_field".into(), Field(Float)),
                    ],
                    SeriesKey::None,
                )
                .unwrap(),
            ),
        );
        catalog
            .inner
            .write()
            .databases
            .insert(database.id, Arc::new(database));

        insta::with_settings!({
            sort_maps => true,
            description => "catalog serialization to help catch breaking changes"
        }, {
            insta::assert_json_snapshot!(catalog);
        });

        // Serialize/deserialize to ensure roundtrip to/from JSON
        let serialized = serde_json::to_string(&catalog).unwrap();
        let deserialized_inner: InnerCatalog = serde_json::from_str(&serialized).unwrap();
        let deserialized = Catalog::from_inner(deserialized_inner);
        assert_eq!(catalog, deserialized);
        assert_eq!(instance_id, deserialized.instance_id());
    }

    #[test]
    fn invalid_catalog_deserialization() {
        // Duplicate databases
        {
            let json = r#"{
                "databases": [
                    [
                        0,
                        {
                            "id": 0,
                            "name": "db1",
                            "tables": [],
                            "table_map": []
                        }
                    ],
                    [
                        0,
                        {
                            "id": 0,
                            "name": "db1",
                            "tables": [],
                            "table_map": []
                        }
                    ]
                ],
                "sequence": 0,
                "host_id": "test",
                "instance_id": "test",
                "db_map": []
            }"#;
            let err = serde_json::from_str::<InnerCatalog>(json).unwrap_err();
            assert_contains!(err.to_string(), "duplicate key found");
        }
        // Duplicate tables
        {
            let json = r#"{
                "databases": [
                    [
                        0,
                        {
                            "id": 0,
                            "name": "db1",
                            "tables": [
                                [
                                    0,
                                    {
                                        "table_id": 0,
                                        "table_name": "tbl1",
                                        "cols": [],
                                        "next_column_id": 0
                                    }
                                ],
                                [
                                    0,
                                    {
                                        "table_id": 0,
                                        "table_name": "tbl1",
                                        "cols": [],
                                        "next_column_id": 0
                                    }
                                ]
                            ]
                        }
                    ]
                ],
                "sequence": 0,
                "host_id": "test",
                "instance_id": "test",
                "db_map": []
            }"#;
            let err = serde_json::from_str::<InnerCatalog>(json).unwrap_err();
            assert_contains!(err.to_string(), "duplicate key found");
        }
        // Duplicate columns
        {
            let json = r#"{
                "databases": [
                    [
                        0,
                        {
                            "id": 0,
                            "name": "db1",
                            "tables": [
                                [
                                    0,
                                    {
                                        "table_id": 0,
                                        "table_name": "tbl1",
                                        "cols": [
                                            [
                                                0,
                                                {
                                                    "id": 0,
                                                    "name": "col",
                                                    "type": "i64",
                                                    "influx_type": "field",
                                                    "nullable": true
                                                }
                                            ],
                                            [
                                                0,
                                                {
                                                    "id": 0,
                                                    "name": "col",
                                                    "type": "u64",
                                                    "influx_type": "field",
                                                    "nullable": true
                                                }
                                            ]
                                        ]
                                    }
                                ]
                            ]
                        }
                    ]
                ]
            }"#;
            let err = serde_json::from_str::<InnerCatalog>(json).unwrap_err();
            assert_contains!(err.to_string(), "duplicate key found");
        }
    }

    #[test]
    fn add_columns_updates_schema_and_column_map() {
        let mut database = DatabaseSchema {
            id: DbId::from(0),
            name: "test".into(),
            tables: SerdeVecMap::new(),
            table_map: BiHashMap::new(),
        };
        database.tables.insert(
            TableId::from(0),
            Arc::new(
                TableDefinition::new(
                    TableId::from(0),
                    "test".into(),
                    vec![(
                        ColumnId::from(0),
                        "test".into(),
                        InfluxColumnType::Field(InfluxFieldType::String),
                    )],
                    SeriesKey::None,
                )
                .unwrap(),
            ),
        );

        let table = database.tables.get_mut(&TableId::from(0)).unwrap();
        println!("table: {table:#?}");
        assert_eq!(table.column_map.len(), 1);
        assert_eq!(table.column_id_to_name_unchecked(&0.into()), "test".into());

        Arc::make_mut(table)
            .add_columns(vec![(
                ColumnId::from(1),
                "test2".into(),
                InfluxColumnType::Tag,
            )])
            .unwrap();
        let schema = table.influx_schema();
        assert_eq!(
            schema.field(0).0,
            InfluxColumnType::Field(InfluxFieldType::String)
        );
        assert_eq!(schema.field(1).0, InfluxColumnType::Tag);

        println!("table: {table:#?}");
        assert_eq!(table.column_map.len(), 2);
        assert_eq!(table.column_name_to_id_unchecked("test2".into()), 1.into());
    }

    #[test]
    fn serialize_series_keys() {
        let host_id = Arc::from("sample-host-id");
        let instance_id = Arc::from("instance-id");
        let catalog = Catalog::new(host_id, instance_id);
        let mut database = DatabaseSchema {
            id: DbId::from(0),
            name: "test_db".into(),
            tables: SerdeVecMap::new(),
            table_map: {
                let mut map = BiHashMap::new();
                map.insert(TableId::from(1), "test_table_1".into());
                map
            },
        };
        use InfluxColumnType::*;
        use InfluxFieldType::*;
        database.tables.insert(
            TableId::from(1),
            Arc::new(
                TableDefinition::new(
                    TableId::from(1),
                    "test_table_1".into(),
                    vec![
                        (ColumnId::from(0), "tag_1".into(), Tag),
                        (ColumnId::from(1), "tag_2".into(), Tag),
                        (ColumnId::from(2), "tag_3".into(), Tag),
                        (ColumnId::from(3), "time".into(), Timestamp),
                        (ColumnId::from(4), "field".into(), Field(String)),
                    ],
                    SeriesKey::Some(vec![
                        ColumnId::from(0),
                        ColumnId::from(1),
                        ColumnId::from(2),
                    ]),
                )
                .unwrap(),
            ),
        );
        catalog
            .inner
            .write()
            .databases
            .insert(database.id, Arc::new(database));

        insta::with_settings!({
            sort_maps => true,
            description => "catalog serialization to help catch breaking changes"
        }, {
            insta::assert_json_snapshot!(catalog);
        });

        let serialized = serde_json::to_string(&catalog).unwrap();
        let deserialized_inner: InnerCatalog = serde_json::from_str(&serialized).unwrap();
        let deserialized = Catalog::from_inner(deserialized_inner);
        assert_eq!(catalog, deserialized);
    }

    #[test]
    fn serialize_last_cache() {
        let host_id = Arc::from("sample-host-id");
        let instance_id = Arc::from("instance-id");
        let catalog = Catalog::new(host_id, instance_id);
        let mut database = DatabaseSchema {
            id: DbId::from(0),
            name: "test_db".into(),
            tables: SerdeVecMap::new(),
            table_map: {
                let mut map = BiHashMap::new();
                map.insert(TableId::from(0), "test".into());
                map
            },
        };
        use InfluxColumnType::*;
        use InfluxFieldType::*;
        let mut table_def = TableDefinition::new(
            TableId::from(0),
            "test".into(),
            vec![
                (ColumnId::from(0), "tag_1".into(), Tag),
                (ColumnId::from(1), "tag_2".into(), Tag),
                (ColumnId::from(2), "tag_3".into(), Tag),
                (ColumnId::from(3), "time".into(), Timestamp),
                (ColumnId::from(4), "field".into(), Field(String)),
            ],
            SeriesKey::None,
        )
        .unwrap();
        table_def.add_last_cache(
            LastCacheDefinition::new_with_explicit_value_columns(
                TableId::from(0),
                "test",
                "test_table_last_cache",
                vec![ColumnId::from(1), ColumnId::from(2)],
                vec![ColumnId::from(4)],
                1,
                600,
            )
            .unwrap(),
        );
        database
            .tables
            .insert(TableId::from(0), Arc::new(table_def));
        catalog
            .inner
            .write()
            .databases
            .insert(database.id, Arc::new(database));

        insta::with_settings!({
            sort_maps => true,
            description => "catalog serialization to help catch breaking changes"
        }, {
            insta::assert_json_snapshot!(catalog);
        });

        let serialized = serde_json::to_string(&catalog).unwrap();
        let deserialized_inner: InnerCatalog = serde_json::from_str(&serialized).unwrap();
        let deserialized = Catalog::from_inner(deserialized_inner);
        assert_eq!(catalog, deserialized);
    }

    #[test]
    fn catalog_instance_and_host_ids() {
        let host_id = Arc::from("sample-host-id");
        let instance_id = Arc::from("sample-instance-id");
        let cloned_host_id = Arc::clone(&host_id);
        let cloned_instance_id = Arc::clone(&instance_id);
        let catalog = Catalog::new(cloned_host_id, cloned_instance_id);
        assert_eq!(instance_id, catalog.instance_id());
        assert_eq!(host_id, catalog.host_id());
    }

    /// See: https://github.com/influxdata/influxdb/issues/25524
    #[test]
    fn apply_catalog_batch_fails_for_add_fields_on_nonexist_table() {
        let catalog = Catalog::new(Arc::from("host"), Arc::from("instance"));
        catalog.insert_database(DatabaseSchema::new(DbId::new(), Arc::from("foo")));
        let db_id = catalog.db_name_to_id("foo").unwrap();
        let catalog_batch = create::catalog_batch_op(
            db_id,
            "foo",
            0,
            [create::add_fields_op(
                db_id,
                "foo",
                TableId::new(),
                "banana",
                [create::field_def(
                    ColumnId::new(),
                    "papaya",
                    FieldDataType::String,
                )],
            )],
        );
        let err = catalog
            .apply_catalog_batch(catalog_batch.as_catalog().unwrap())
            .expect_err("should fail to apply AddFields operation for non-existent table");
        assert_contains!(err.to_string(), "Table banana not in DB schema for foo");
    }
}
