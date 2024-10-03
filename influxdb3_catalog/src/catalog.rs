//! Implementation of the Catalog that sits entirely in memory.

use crate::catalog::Error::TableNotFound;
use bimap::BiHashMap;
use influxdb3_id::{DbId, TableId};
use influxdb3_wal::{
    CatalogBatch, CatalogOp, FieldAdditions, LastCacheDefinition, LastCacheDelete,
};
use influxdb_line_protocol::FieldValue;
use observability_deps::tracing::info;
use parking_lot::RwLock;
use schema::{InfluxColumnType, InfluxFieldType, Schema, SchemaBuilder};
use serde::{Deserialize, Serialize, Serializer};
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use thiserror::Error;

#[derive(Debug, Error, Clone)]
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
    TableNotFound { db_name: String, table_name: String },

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
        "Series key mismatch on table {}. Existing table has {} but attempted to add {}",
        table_name,
        existing,
        attempted
    )]
    SeriesKeyMismatch {
        table_name: String,
        existing: String,
        attempted: String,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

pub const TIME_COLUMN_NAME: &str = "time";

/// The sequence number of a batch of WAL operations.
#[derive(
    Debug, Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
pub struct SequenceNumber(u32);

type SeriesKey = Option<Vec<String>>;

impl SequenceNumber {
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
        let db = self
            .db_name_to_id(db_name.into())
            .and_then(|db_id| self.inner.read().databases.get(&db_id).map(Arc::clone));

        let db = match db {
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

    pub fn add_table_to_lookup(&self, db_id: DbId, table_id: TableId, name: Arc<str>) {
        self.inner
            .write()
            .table_map
            .entry(db_id)
            .or_default()
            .insert(table_id, name);
    }

    pub fn db_name_to_id(&self, db_name: Arc<str>) -> Option<DbId> {
        self.inner.read().db_map.get_by_right(&db_name).copied()
    }

    pub fn db_id_to_name(&self, db_id: DbId) -> Option<Arc<str>> {
        self.inner.read().db_map.get_by_left(&db_id).map(Arc::clone)
    }

    pub fn table_name_to_id(&self, db_id: DbId, table_name: Arc<str>) -> Option<TableId> {
        self.inner
            .read()
            .table_map
            .get(&db_id)
            .and_then(|map| map.get_by_right(&table_name).copied())
    }

    pub fn table_id_to_name(&self, db_id: DbId, table_id: TableId) -> Option<Arc<str>> {
        self.inner
            .read()
            .table_map
            .get(&db_id)
            .and_then(|map| map.get_by_left(&table_id))
            .map(Arc::clone)
    }

    pub fn db_schema(&self, id: &DbId) -> Option<Arc<DatabaseSchema>> {
        self.inner.read().databases.get(id).cloned()
    }

    pub fn sequence_number(&self) -> SequenceNumber {
        self.inner.read().sequence
    }

    pub fn clone_inner(&self) -> InnerCatalog {
        self.inner.read().clone()
    }

    pub fn list_databases(&self) -> Vec<String> {
        self.inner
            .read()
            .databases
            .values()
            .map(|db| db.name.to_string())
            .collect()
    }

    pub fn add_last_cache(&self, db_id: DbId, table_id: TableId, last_cache: LastCacheDefinition) {
        let mut inner = self.inner.write();
        let mut db = inner
            .databases
            .get(&db_id)
            .expect("db should exist")
            .as_ref()
            .clone();
        let table = db.tables.get_mut(&table_id).expect("table should exist");
        table.add_last_cache(last_cache);
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
        let table = db.tables.get_mut(&table_id).expect("table should exist");
        table.remove_last_cache(name);
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

    pub fn insert_database(&mut self, db: DatabaseSchema) {
        let mut inner = self.inner.write();
        for (table_id, table_def) in db.tables.iter() {
            inner
                .table_map
                .entry(db.id)
                .and_modify(|map: &mut BiHashMap<TableId, Arc<str>>| {
                    map.insert(*table_id, Arc::clone(&table_def.table_name));
                })
                .or_insert_with(|| {
                    let mut map = BiHashMap::new();
                    map.insert(*table_id, Arc::clone(&table_def.table_name));
                    map
                });
        }
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
    pub fn set_updated_false_if_sequence_matches(&self, sequence_number: SequenceNumber) {
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
    #[serde_as(as = "DatabasesAsArray")]
    databases: HashMap<DbId, Arc<DatabaseSchema>>,
    sequence: SequenceNumber,
    /// The host_id is the prefix that is passed in when starting up (`host_identifier_prefix`)
    host_id: Arc<str>,
    /// The instance_id uniquely identifies the instance that generated the catalog
    instance_id: Arc<str>,
    /// If true, the catalog has been updated since the last time it was serialized
    #[serde(skip)]
    updated: bool,
    #[serde_as(as = "DbMapAsArray")]
    db_map: BiHashMap<DbId, Arc<str>>,
    #[serde_as(as = "TableMapAsArray")]
    pub table_map: HashMap<DbId, BiHashMap<TableId, Arc<str>>>,
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

serde_with::serde_conv!(
    TableMapAsArray,
    HashMap<DbId, BiHashMap<TableId, Arc<str>>>,
    |map: &HashMap<DbId, BiHashMap<TableId, Arc<str>>>| {
        map.iter().fold(Vec::new(), |mut acc, (db_id, table_map)| {
            for (table_id, name) in table_map.iter() {
                acc.push(TableMap {
                    db_id: *db_id,
                    table_id: *table_id,
                    name: Arc::clone(&name)
                });
            }
            acc
        })
    },
    |vec: Vec<TableMap>| -> Result<_, std::convert::Infallible> {
        let mut map = HashMap::new();
        for item in vec {
            map.entry(item.db_id)
               .and_modify(|entry: &mut BiHashMap<TableId, Arc<str>>| {
                   entry.insert(item.table_id, Arc::clone(&item.name));
               })
               .or_insert_with(||{
                    let mut inner_map = BiHashMap::new();
                    inner_map.insert(item.table_id, Arc::clone(&item.name));
                    inner_map
               });
        }
        Ok(map)
    }
);

#[derive(Debug, Serialize, Deserialize)]
struct TableMap {
    db_id: DbId,
    table_id: TableId,
    name: Arc<str>,
}

serde_with::serde_conv!(
    DatabasesAsArray,
    HashMap<DbId, Arc<DatabaseSchema>>,
    |map: &HashMap<DbId, Arc<DatabaseSchema>>| {
        map.values().fold(Vec::new(), |mut acc, db| {
            acc.push(DatabasesSerialized {
                id: db.id,
                name: Arc::clone(&db.name),
                tables: db.tables.values().cloned().collect(),
            });
            acc
        })
    },
    |vec: Vec<DatabasesSerialized>| -> Result<_, String> {
        vec.into_iter().fold(Ok(HashMap::new()), |acc, db| {
            let mut acc = acc?;
            if let Some(_) = acc.insert(db.id, Arc::new(DatabaseSchema {
                id: db.id,
                name: Arc::clone(&db.name),
                tables: db.tables.into_iter().fold(Ok(BTreeMap::new()), |acc, table| {
                    let mut acc = acc?;
                    let table_name = Arc::clone(&table.table_name);
                    if let Some(_) = acc.insert(table.table_id, table) {
                        return Err(format!("found duplicate table: {}", table_name));
                    }
                    Ok(acc)
                })?
            })) {
                return Err(format!("found duplicate db: {}", db.name));
            }
            Ok(acc)
        })
    }
);

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone, Default)]
struct DatabasesSerialized {
    pub id: DbId,
    pub name: Arc<str>,
    pub tables: Vec<TableDefinition>,
}

impl InnerCatalog {
    pub(crate) fn new(host_id: Arc<str>, instance_id: Arc<str>) -> Self {
        Self {
            databases: HashMap::new(),
            sequence: SequenceNumber::new(0),
            host_id,
            instance_id,
            updated: false,
            db_map: BiHashMap::new(),
            table_map: HashMap::new(),
        }
    }

    pub fn sequence_number(&self) -> SequenceNumber {
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
                for (table_id, table_def) in new_db.tables.iter() {
                    self.table_map
                        .entry(new_db.id)
                        .and_modify(|map| {
                            map.insert(*table_id, Arc::clone(&table_def.table_name));
                        })
                        .or_insert_with(|| {
                            let mut map = BiHashMap::new();
                            map.insert(*table_id, Arc::clone(&table_def.table_name));
                            map
                        });
                }
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
            for (table_id, table_def) in new_db.tables.iter() {
                self.table_map
                    .entry(new_db.id)
                    .and_modify(|map| {
                        map.insert(*table_id, Arc::clone(&table_def.table_name));
                    })
                    .or_insert_with(|| {
                        let mut map = BiHashMap::new();
                        map.insert(*table_id, Arc::clone(&table_def.table_name));
                        map
                    });
            }
        }

        Ok(())
    }

    pub fn databases(&self) -> impl Iterator<Item = &Arc<DatabaseSchema>> {
        self.databases.values()
    }

    pub fn db_exists(&self, db_id: DbId) -> bool {
        self.databases.contains_key(&db_id)
    }
}

#[serde_with::serde_as]
#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone)]
pub struct DatabaseSchema {
    pub id: DbId,
    pub name: Arc<str>,
    /// The database is a map of tables
    pub tables: BTreeMap<TableId, TableDefinition>,
}

impl DatabaseSchema {
    pub fn new(id: DbId, name: Arc<str>) -> Self {
        Self {
            id,
            name,
            tables: BTreeMap::new(),
        }
    }

    /// Validates the updates in the `CatalogBatch` are compatible with this schema. If
    /// everything is compatible and there are no updates to the existing schema, None will be
    /// returned, otherwise a new `DatabaseSchema` will be returned with the updates applied.
    pub fn new_if_updated_from_batch(&self, catalog_batch: &CatalogBatch) -> Result<Option<Self>> {
        let mut updated_or_new_tables = BTreeMap::new();

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
                            updated_or_new_tables.insert(new_table.table_id, new_table);
                        }
                    } else {
                        let new_table = TableDefinition::new_from_op(table_definition);
                        updated_or_new_tables.insert(new_table.table_id, new_table);
                    }
                }
                CatalogOp::AddFields(field_additions) => {
                    let new_or_existing_table = updated_or_new_tables
                        .get(&field_additions.table_id)
                        .or_else(|| self.tables.get(&field_additions.table_id));
                    if let Some(existing_table) = new_or_existing_table {
                        if let Some(new_table) =
                            existing_table.new_if_field_additions_add_fields(field_additions)?
                        {
                            updated_or_new_tables.insert(new_table.table_id, new_table);
                        }
                    } else {
                        let fields = field_additions
                            .field_definitions
                            .iter()
                            .map(|f| (f.name.to_string(), f.data_type.into()))
                            .collect::<Vec<_>>();
                        let new_table = TableDefinition::new(
                            field_additions.table_id,
                            Arc::clone(&field_additions.table_name),
                            fields,
                            SeriesKey::None,
                        )?;
                        updated_or_new_tables.insert(new_table.table_id, new_table);
                    }
                }
                CatalogOp::CreateLastCache(last_cache_definition) => {
                    let new_or_existing_table = updated_or_new_tables
                        .get(&last_cache_definition.table_id)
                        .or_else(|| self.tables.get(&last_cache_definition.table_id));

                    let table = new_or_existing_table.ok_or(TableNotFound {
                        db_name: self.name.to_string(),
                        table_name: last_cache_definition.table.clone(),
                    })?;

                    if let Some(new_table) =
                        table.new_if_last_cache_definition_is_new(last_cache_definition)
                    {
                        updated_or_new_tables.insert(new_table.table_id, new_table);
                    }
                }
                CatalogOp::DeleteLastCache(last_cache_deletion) => {
                    let new_or_existing_table = updated_or_new_tables
                        .get(&last_cache_deletion.table_id)
                        .or_else(|| self.tables.get(&last_cache_deletion.table_id));

                    let table = new_or_existing_table.ok_or(TableNotFound {
                        db_name: self.name.to_string(),
                        table_name: last_cache_deletion.table_name.clone(),
                    })?;

                    if let Some(new_table) =
                        table.new_if_last_cache_deletes_existing(last_cache_deletion)
                    {
                        updated_or_new_tables.insert(new_table.table_id, new_table);
                    }
                }
            }
        }

        if updated_or_new_tables.is_empty() {
            Ok(None)
        } else {
            for (table_id, table_def) in &self.tables {
                if !updated_or_new_tables.contains_key(table_id) {
                    updated_or_new_tables.insert(*table_id, table_def.clone());
                }
            }

            Ok(Some(Self {
                id: self.id,
                name: Arc::clone(&self.name),
                tables: updated_or_new_tables,
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

    pub fn get_table_schema(&self, table_id: TableId) -> Option<&Schema> {
        self.tables.get(&table_id).map(|table| &table.schema)
    }

    pub fn get_table(&self, table_id: TableId) -> Option<&TableDefinition> {
        self.tables.get(&table_id)
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

    pub fn table_exists(&self, table_id: TableId) -> bool {
        self.tables.contains_key(&table_id)
    }

    pub fn tables(&self) -> impl Iterator<Item = &TableDefinition> {
        self.tables.values()
    }
}

#[derive(Debug, Eq, PartialEq, Clone)]
pub struct TableDefinition {
    pub table_id: TableId,
    pub table_name: Arc<str>,
    pub schema: Schema,
    pub last_caches: BTreeMap<String, LastCacheDefinition>,
}

impl TableDefinition {
    /// Create a new [`TableDefinition`]
    ///
    /// Ensures the provided columns will be ordered before constructing the schema.
    pub fn new<CN: AsRef<str>>(
        table_id: TableId,
        table_name: Arc<str>,
        columns: impl AsRef<[(CN, InfluxColumnType)]>,
        series_key: Option<impl IntoIterator<Item: AsRef<str>>>,
    ) -> Result<Self> {
        // ensure we're under the column limit
        if columns.as_ref().len() > Catalog::NUM_COLUMNS_PER_TABLE_LIMIT {
            return Err(Error::TooManyColumns);
        }

        // Use a BTree to ensure that the columns are ordered:
        let mut ordered_columns = BTreeMap::new();
        for (name, column_type) in columns.as_ref() {
            ordered_columns.insert(name.as_ref(), column_type);
        }
        let mut schema_builder = SchemaBuilder::with_capacity(columns.as_ref().len());
        schema_builder.measurement(table_name.as_ref());
        if let Some(sk) = series_key {
            schema_builder.with_series_key(sk);
        }
        for (name, column_type) in ordered_columns {
            schema_builder.influx_column(name, *column_type);
        }
        let schema = schema_builder.build().unwrap();

        Ok(Self {
            table_id,
            table_name,
            schema,
            last_caches: BTreeMap::new(),
        })
    }

    /// Create a new table definition from a catalog op
    pub fn new_from_op(table_definition: &influxdb3_wal::TableDefinition) -> Self {
        let mut columns = Vec::new();
        for field_def in &table_definition.field_definitions {
            columns.push((field_def.name.as_ref(), field_def.data_type.into()));
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
        let existing_key = self
            .schema
            .series_key()
            .map(|k| k.iter().map(|v| v.to_string()).collect());

        if table_definition.key != existing_key {
            return Err(Error::SeriesKeyMismatch {
                table_name: self.table_name.to_string(),
                existing: existing_key.unwrap_or_default().join("/"),
                attempted: table_definition.key.clone().unwrap_or_default().join("/"),
            });
        }
        let mut new_fields: Vec<(String, InfluxColumnType)> =
            Vec::with_capacity(table_definition.field_definitions.len());

        for field_def in &table_definition.field_definitions {
            if let Some(existing_type) = self.schema.field_type_by_name(field_def.name.as_ref()) {
                if existing_type != field_def.data_type.into() {
                    return Err(Error::FieldTypeMismatch {
                        table_name: self.table_name.to_string(),
                        column_name: field_def.name.to_string(),
                        existing: existing_type,
                        attempted: field_def.data_type.into(),
                    });
                }
            } else {
                new_fields.push((field_def.name.to_string(), field_def.data_type.into()));
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
        for c in &field_additions.field_definitions {
            let field_type = c.data_type.into();
            match self.field_type_by_name(&c.name) {
                None => {
                    new_fields.push((c.name.to_string(), field_type));
                }
                Some(existing_field_type) => {
                    if existing_field_type != field_type {
                        return Err(Error::FieldTypeMismatch {
                            table_name: self.table_name.to_string(),
                            column_name: c.name.to_string(),
                            existing: existing_field_type,
                            attempted: field_type,
                        });
                    }
                }
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

    /// Check if the column exists in the [`TableDefinition`]s schema
    pub fn column_exists(&self, column: &str) -> bool {
        self.schema.find_index_of(column).is_some()
    }

    /// Add the columns to this [`TableDefinition`]
    ///
    /// This ensures that the resulting schema has its columns ordered
    pub fn add_columns(&mut self, columns: Vec<(String, InfluxColumnType)>) -> Result<()> {
        // Use BTree to insert existing and new columns, and use that to generate the
        // resulting schema, to ensure column order is consistent:
        let mut cols = BTreeMap::new();
        for (col_type, field) in self.schema.iter() {
            cols.insert(field.name(), col_type);
        }
        for (name, column_type) in columns.iter() {
            cols.insert(name, *column_type);
        }

        // ensure we don't go over the column limit
        if cols.len() > Catalog::NUM_COLUMNS_PER_TABLE_LIMIT {
            return Err(Error::TooManyColumns);
        }
        let mut schema_builder = SchemaBuilder::with_capacity(columns.len());
        // TODO: may need to capture some schema-level metadata, currently, this causes trouble in
        // tests, so I am omitting this for now:
        // schema_builder.measurement(&self.name);
        for (name, col_type) in cols {
            schema_builder.influx_column(name, col_type);
        }
        let schema = schema_builder.build().unwrap();

        self.schema = schema;

        Ok(())
    }

    pub fn index_columns(&self) -> Vec<&str> {
        self.schema
            .iter()
            .filter_map(|(col_type, field)| match col_type {
                InfluxColumnType::Tag => Some(field.name().as_str()),
                InfluxColumnType::Field(_) | InfluxColumnType::Timestamp => None,
            })
            .collect()
    }

    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    pub fn num_columns(&self) -> usize {
        self.schema.len()
    }

    pub fn field_type_by_name(&self, name: &str) -> Option<InfluxColumnType> {
        self.schema.field_type_by_name(name)
    }

    pub fn is_v3(&self) -> bool {
        self.schema.series_key().is_some()
    }

    /// Add a new last cache to this table definition
    pub fn add_last_cache(&mut self, last_cache: LastCacheDefinition) {
        self.last_caches
            .insert(last_cache.name.to_string(), last_cache);
    }

    /// Remove a last cache from the table definition
    pub fn remove_last_cache(&mut self, name: &str) {
        self.last_caches.remove(name);
    }

    pub fn last_caches(&self) -> impl Iterator<Item = (&String, &LastCacheDefinition)> {
        self.last_caches.iter()
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
    use insta::assert_json_snapshot;
    use pretty_assertions::assert_eq;
    use test_helpers::assert_contains;

    use super::*;

    type SeriesKey = Option<Vec<String>>;

    #[test]
    fn catalog_serialization() {
        let host_id = Arc::from("dummy-host-id");
        let instance_id = Arc::from("instance-id");
        let cloned_instance_id = Arc::clone(&instance_id);
        let catalog = Catalog::new(host_id, cloned_instance_id);
        let mut database = DatabaseSchema {
            id: DbId::from(0),
            name: "test_db".into(),
            tables: BTreeMap::new(),
        };
        use InfluxColumnType::*;
        use InfluxFieldType::*;
        database.tables.insert(
            TableId::from(1),
            TableDefinition::new(
                TableId::from(1),
                "test_table_1".into(),
                [
                    ("tag_1", Tag),
                    ("tag_2", Tag),
                    ("tag_3", Tag),
                    ("time", Timestamp),
                    ("string_field", Field(String)),
                    ("bool_field", Field(Boolean)),
                    ("i64_field", Field(Integer)),
                    ("u64_field", Field(UInteger)),
                    ("f64_field", Field(Float)),
                ],
                SeriesKey::None,
            )
            .unwrap(),
        );
        database.tables.insert(
            TableId::from(2),
            TableDefinition::new(
                TableId::from(2),
                "test_table_2".into(),
                [
                    ("tag_1", Tag),
                    ("tag_2", Tag),
                    ("tag_3", Tag),
                    ("time", Timestamp),
                    ("string_field", Field(String)),
                    ("bool_field", Field(Boolean)),
                    ("i64_field", Field(Integer)),
                    ("u64_field", Field(UInteger)),
                    ("f64_field", Field(Float)),
                ],
                SeriesKey::None,
            )
            .unwrap(),
        );
        catalog
            .inner
            .write()
            .databases
            .insert(database.id, Arc::new(database));

        // Perform a snapshot test to check that the JSON serialized catalog does not change in an
        // undesired way when introducing features etc.
        assert_json_snapshot!(catalog);

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
                    {
                        "id": 0,
                        "name": "db1",
                        "tables": []
                    },
                    {
                        "id": 0,
                        "name": "db1",
                        "tables": []
                    }
                ]
            }"#;
            let err = serde_json::from_str::<InnerCatalog>(json).unwrap_err();
            assert_contains!(err.to_string(), "found duplicate db: db1");
        }
        // Duplicate tables
        {
            let json = r#"{
                "databases": [
                    {
                        "id": 0,
                        "name": "db1",
                        "tables": [
                            {
                                "table_id": 0,
                                "table_name": "tbl1",
                                "cols": {}
                            },
                            {
                                "table_id": 0,
                                "table_name": "tbl1",
                                "cols": {}
                            }
                        ]
                    }
                ]
            }"#;
            let err = serde_json::from_str::<InnerCatalog>(json).unwrap_err();
            assert_contains!(err.to_string(), "found duplicate table: tbl1");
        }
        // Duplicate columns
        {
            let json = r#"{
                "databases": [
                    {
                        "id": 0,
                        "name": "db1",
                        "tables": [
                            {
                                "table_id": 0,
                                "table_name": "tbl1",
                                "cols": {
                                    "col1": {
                                        "type": "i64",
                                        "influx_type": "field",
                                        "nullable": true
                                    },
                                    "col1": {
                                        "type": "u64",
                                        "influx_type": "field",
                                        "nullable": true
                                    }
                                }
                            }
                        ]
                    }
                ]
            }"#;
            let err = serde_json::from_str::<InnerCatalog>(json).unwrap_err();
            assert_contains!(err.to_string(), "found duplicate key");
        }
    }

    #[test]
    fn add_columns_updates_schema() {
        let mut database = DatabaseSchema {
            id: DbId::from(0),
            name: "test".into(),
            tables: BTreeMap::new(),
        };
        database.tables.insert(
            TableId::from(0),
            TableDefinition::new(
                TableId::from(0),
                "test".into(),
                [(
                    "test".to_string(),
                    InfluxColumnType::Field(InfluxFieldType::String),
                )],
                SeriesKey::None,
            )
            .unwrap(),
        );

        let table = database.tables.get_mut(&TableId::from(0)).unwrap();
        table
            .add_columns(vec![("test2".to_string(), InfluxColumnType::Tag)])
            .unwrap();
        let schema = table.schema();
        assert_eq!(
            schema.field(0).0,
            InfluxColumnType::Field(InfluxFieldType::String)
        );
        assert_eq!(schema.field(1).0, InfluxColumnType::Tag);
    }

    #[test]
    fn serialize_series_keys() {
        let host_id = Arc::from("dummy-host-id");
        let instance_id = Arc::from("instance-id");
        let catalog = Catalog::new(host_id, instance_id);
        let mut database = DatabaseSchema {
            id: DbId::from(0),
            name: "test_db".into(),
            tables: BTreeMap::new(),
        };
        use InfluxColumnType::*;
        use InfluxFieldType::*;
        database.tables.insert(
            TableId::from(1),
            TableDefinition::new(
                TableId::from(1),
                "test_table_1".into(),
                [
                    ("tag_1", Tag),
                    ("tag_2", Tag),
                    ("tag_3", Tag),
                    ("time", Timestamp),
                    ("field", Field(String)),
                ],
                SeriesKey::Some(vec![
                    "tag_1".to_string(),
                    "tag_2".to_string(),
                    "tag_3".to_string(),
                ]),
            )
            .unwrap(),
        );
        catalog
            .inner
            .write()
            .databases
            .insert(database.id, Arc::new(database));

        assert_json_snapshot!(catalog);

        let serialized = serde_json::to_string(&catalog).unwrap();
        let deserialized_inner: InnerCatalog = serde_json::from_str(&serialized).unwrap();
        let deserialized = Catalog::from_inner(deserialized_inner);
        assert_eq!(catalog, deserialized);
    }

    #[test]
    fn serialize_last_cache() {
        let host_id = Arc::from("dummy-host-id");
        let instance_id = Arc::from("instance-id");
        let catalog = Catalog::new(host_id, instance_id);
        let mut database = DatabaseSchema {
            id: DbId::from(0),
            name: "test_db".into(),
            tables: BTreeMap::new(),
        };
        use InfluxColumnType::*;
        use InfluxFieldType::*;
        let mut table_def = TableDefinition::new(
            TableId::from(0),
            "test".into(),
            [
                ("tag_1", Tag),
                ("tag_2", Tag),
                ("tag_3", Tag),
                ("time", Timestamp),
                ("field", Field(String)),
            ],
            SeriesKey::None,
        )
        .unwrap();
        table_def.add_last_cache(
            LastCacheDefinition::new_with_explicit_value_columns(
                TableId::from(0),
                "test",
                "test_table_last_cache",
                ["tag_2", "tag_3"],
                ["field"],
                1,
                600,
            )
            .unwrap(),
        );
        database.tables.insert(TableId::from(0), table_def);
        catalog
            .inner
            .write()
            .databases
            .insert(database.id, Arc::new(database));

        assert_json_snapshot!(catalog);

        let serialized = serde_json::to_string(&catalog).unwrap();
        let deserialized_inner: InnerCatalog = serde_json::from_str(&serialized).unwrap();
        let deserialized = Catalog::from_inner(deserialized_inner);
        assert_eq!(catalog, deserialized);
    }

    #[test]
    fn catalog_instance_and_host_ids() {
        let host_id = Arc::from("dummy-host-id");
        let instance_id = Arc::from("dummy-instance-id");
        let cloned_host_id = Arc::clone(&host_id);
        let cloned_instance_id = Arc::clone(&instance_id);
        let catalog = Catalog::new(cloned_host_id, cloned_instance_id);
        assert_eq!(instance_id, catalog.instance_id());
        assert_eq!(host_id, catalog.host_id());
    }
}
