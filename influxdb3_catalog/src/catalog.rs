//! Implementation of the Catalog that sits entirely in memory.

use crate::catalog::Error::{
    ProcessingEngineCallNotFound, ProcessingEngineUnimplemented, TableNotFound,
};
use arrow::array::RecordBatch;
use bimap::BiHashMap;
use hashbrown::{HashMap, HashSet};
use indexmap::IndexMap;
use influxdb3_id::{ColumnId, DbId, SerdeVecMap, TableId};
use influxdb3_process_engine::python_call::{ProcessEngineTrigger, PythonCall, TriggerType};
use influxdb3_wal::{
    CatalogBatch, CatalogOp, DeleteTableDefinition, FieldAdditions, LastCacheDefinition,
    LastCacheDelete, MetaCacheDefinition, MetaCacheDelete,
};
use influxdb_line_protocol::FieldValue;
use iox_time::Time;
use observability_deps::tracing::{debug, info};
use parking_lot::RwLock;
use schema::{InfluxColumnType, InfluxFieldType, Schema, SchemaBuilder};
use serde::{Deserialize, Serialize, Serializer};
use std::collections::BTreeMap;
use std::sync::Arc;
use thiserror::Error;

const SOFT_DELETION_TIME_FORMAT: &str = "%Y%m%dT%H%M%S";

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

    #[error(
        "Cannot overwrite Process Engine Call {} in Database {}",
        call_name,
        database_name
    )]
    ProcessEngineCallExists {
        database_name: String,
        call_name: String,
    },

    #[error(
        "Processing Engine Call {} not in DB schema for {}",
        call_name,
        database_name
    )]
    ProcessingEngineCallNotFound {
        call_name: String,
        database_name: String,
    },
    #[error("Processing Engine Unimplemented: {}", feature_description)]
    ProcessingEngineUnimplemented { feature_description: String },
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
        self.db_id_and_schema(db_name).map(|(_, schema)| schema)
    }

    pub fn db_schema_by_id(&self, db_id: &DbId) -> Option<Arc<DatabaseSchema>> {
        self.inner.read().databases.get(db_id).cloned()
    }

    pub fn db_id_and_schema(&self, db_name: &str) -> Option<(DbId, Arc<DatabaseSchema>)> {
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

    pub fn add_meta_cache(&self, db_id: DbId, table_id: TableId, meta_cache: MetaCacheDefinition) {
        let mut inner = self.inner.write();
        let mut db = inner
            .databases
            .get(&db_id)
            .expect("db should exist")
            .as_ref()
            .clone();
        let mut table = db
            .table_definition_by_id(&table_id)
            .expect("table should exist")
            .as_ref()
            .clone();
        table.add_meta_cache(meta_cache);
        db.insert_table(table_id, Arc::new(table));
        inner.upsert_db(db);
    }

    pub fn remove_meta_cache(&self, db_id: &DbId, table_id: &TableId, name: &str) {
        let mut inner = self.inner.write();
        let mut db = inner
            .databases
            .get(db_id)
            .expect("db should exist")
            .as_ref()
            .clone();
        let mut table = db
            .tables
            .get(table_id)
            .expect("table should exist")
            .as_ref()
            .clone();
        table.remove_meta_cache(name);
        db.insert_table(*table_id, Arc::new(table));
        inner.upsert_db(db);
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
        db.insert_table(table_id, Arc::new(table));
        inner.upsert_db(db);
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
        db.insert_table(table_id, Arc::new(table));
        inner.upsert_db(db);
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
        inner.upsert_db(db);
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

    pub fn insert_process_engine_call(
        &self,
        db: &str,
        call_name: String,
        code: String,
        function_name: String,
    ) -> Result<()> {
        let mut inner = self.inner.write();
        let Some(db_id) = inner.db_map.get_by_right(db) else {
            return Err(TableNotFound {
                db_name: format!("{:?}", inner.db_map).into(),
                table_name: "".into(),
            });
        };
        let db = inner.databases.get(db_id).expect("db should exist");
        if db.processing_engine_calls.contains_key(&call_name) {
            return Err(Error::ProcessEngineCallExists {
                database_name: db.name.to_string(),
                call_name,
            });
        }
        let mut new_db = db.as_ref().clone();
        new_db.processing_engine_calls.insert(
            call_name.clone(),
            PythonCall::new(call_name, code, function_name),
        );
        inner.upsert_db(new_db);
        Ok(())
    }

    pub fn insert_process_engine_trigger(
        &self,
        db_name: &str,
        call_name: String,
        source_table: String,
        derived_table: String,
        keys: Option<Vec<String>>,
    ) -> Result<()> {
        let mut inner = self.inner.write();
        // TODO: proper error
        let db_id = inner.db_map.get_by_right(db_name).unwrap();
        let db = inner.databases.get(db_id).expect("db should exist");
        if db.table_id_and_definition(derived_table.as_str()).is_some() {
            return Err(Error::ProcessEngineCallExists {
                database_name: derived_table,
                call_name,
            });
        }

        let Some((source_table_id, table_definition)) =
            db.table_id_and_definition(source_table.as_str())
        else {
            return Err(TableNotFound {
                db_name: db_name.into(),
                table_name: source_table.into(),
            });
        };

        let Some(call) = db.processing_engine_calls.get(&call_name) else {
            panic!("create error.")
        };

        let output_keys = match &keys {
            Some(inner_keys) => Some(inner_keys.iter().map(|key| key.as_str()).collect()),
            None => table_definition.schema.series_key(),
        };

        let key_set = output_keys
            .clone()
            .unwrap_or_default()
            .iter()
            .map(|x| *x)
            .collect::<HashSet<_>>();

        let output_schema = call
            .call(&RecordBatch::new_empty(table_definition.schema.as_arrow()))
            .unwrap()
            .schema();
        let table_id = TableId::new();
        let mut key_column_ids = vec![];
        // This is a new table, so build up its columns:
        let mut columns = Vec::new();
        for field in output_schema.fields() {
            let col_id = ColumnId::new();
            let column_type =
                if let Some(influx_column_type) = field.metadata().get("iox::column::type") {
                    InfluxColumnType::try_from(influx_column_type.as_str()).unwrap()
                } else if key_set.contains(field.name().as_str()) {
                    key_column_ids.push(col_id);
                    InfluxColumnType::Tag
                } else if field.name() == "time" {
                    InfluxColumnType::Timestamp
                } else {
                    InfluxColumnType::Field(field.data_type().clone().try_into().unwrap())
                };
            columns.push((col_id, field.name().as_str().into(), column_type));
        }
        let key_column_ids = if output_keys.is_none() {
            None
        } else {
            Some(key_column_ids)
        };

        let new_definition = TableDefinition::new(
            table_id,
            derived_table.as_str().into(),
            columns,
            key_column_ids,
        )?;
        let mut new_db = db.as_ref().clone();
        new_db.insert_table(table_id, Arc::new(new_definition));
        let trigger = ProcessEngineTrigger {
            source_table: source_table_id,
            trigger_table: table_id,
            trigger_name: call_name,
            trigger_type: TriggerType::OnRead,
        };
        new_db
            .processing_engine_write_triggers
            .entry(source_table_id)
            .or_default()
            .insert(table_id, trigger.clone());
        new_db
            .processing_engine_source_table
            .insert(table_id, trigger);

        inner.upsert_db(new_db);
        Ok(())
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
            if let Some(new_db) = DatabaseSchema::new_if_updated_from_batch(db, catalog_batch)? {
                check_overall_table_count(Some(db), &new_db, table_count)?;
                self.upsert_db(new_db);
            }
        } else {
            if self.databases.len() >= Catalog::NUM_DBS_LIMIT {
                return Err(Error::TooManyDbs);
            }
            let new_db = DatabaseSchema::new_from_batch(catalog_batch)?;
            check_overall_table_count(None, &new_db, table_count)?;
            self.upsert_db(new_db);
        }

        Ok(())
    }

    pub fn db_exists(&self, db_id: DbId) -> bool {
        self.databases.contains_key(&db_id)
    }

    pub fn upsert_db(&mut self, db: DatabaseSchema) {
        let name = Arc::clone(&db.name);
        let id = db.id;
        self.databases.insert(id, Arc::new(db));
        self.set_updated_and_increment_sequence();
        self.db_map.insert(id, name);
    }

    fn set_updated_and_increment_sequence(&mut self) {
        self.sequence = self.sequence.next();
        self.updated = true;
    }
}

fn check_overall_table_count(
    existing_db: Option<&Arc<DatabaseSchema>>,
    new_db: &DatabaseSchema,
    current_table_count: usize,
) -> Result<()> {
    let existing_table_count = if let Some(existing_db) = existing_db {
        existing_db.tables.len()
    } else {
        0
    };
    let newly_added_table_count = new_db.tables.len() - existing_table_count;
    if current_table_count + newly_added_table_count > Catalog::NUM_TABLES_LIMIT {
        return Err(Error::TooManyTables);
    }
    Ok(())
}

#[derive(Debug, Eq, PartialEq, Clone)]
pub struct DatabaseSchema {
    pub id: DbId,
    pub name: Arc<str>,
    /// The database is a map of tables
    pub tables: SerdeVecMap<TableId, Arc<TableDefinition>>,
    pub table_map: BiHashMap<TableId, Arc<str>>,
    pub processing_engine_calls: HashMap<String, PythonCall>,
    // Map from source table to output table for processing engine triggers.
    pub processing_engine_write_triggers: HashMap<TableId, HashMap<TableId, ProcessEngineTrigger>>,
    pub processing_engine_source_table: HashMap<TableId, ProcessEngineTrigger>,
    pub deleted: bool,
}

impl DatabaseSchema {
    pub fn new(id: DbId, name: Arc<str>) -> Self {
        Self {
            id,
            name,
            tables: Default::default(),
            table_map: BiHashMap::new(),
            processing_engine_calls: HashMap::new(),
            processing_engine_write_triggers: HashMap::new(),
            processing_engine_source_table: HashMap::new(),
            deleted: false,
        }
    }

    /// Validates the updates in the `CatalogBatch` are compatible with this schema. If
    /// everything is compatible and there are no updates to the existing schema, None will be
    /// returned, otherwise a new `DatabaseSchema` will be returned with the updates applied.
    pub fn new_if_updated_from_batch(
        db_schema: &DatabaseSchema,
        catalog_batch: &CatalogBatch,
    ) -> Result<Option<Self>> {
        debug!(name = ?db_schema.name, deleted = ?db_schema.deleted, full_batch = ?catalog_batch, "Updating / adding to catalog");
        let mut updated_or_new_tables = SerdeVecMap::new();
        let mut schema_deleted = false;
        let mut table_deleted = false;
        let mut deleted_table_defn = None;
        let mut schema_name = Arc::clone(&db_schema.name);

        for catalog_op in &catalog_batch.ops {
            match catalog_op {
                CatalogOp::CreateDatabase(_) => (),
                CatalogOp::CreateTable(table_definition) => {
                    let new_or_existing_table = updated_or_new_tables
                        .get(&table_definition.table_id)
                        .or_else(|| db_schema.tables.get(&table_definition.table_id));
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
                        .or_else(|| db_schema.tables.get(&field_additions.table_id))
                    else {
                        return Err(TableNotFound {
                            db_name: Arc::clone(&field_additions.database_name),
                            table_name: Arc::clone(&field_additions.table_name),
                        });
                    };
                    if let Some(new_table) =
                        new_or_existing_table.new_if_field_additions_add_fields(field_additions)?
                    {
                        // this table's schema was updated. Downstream processing engines may need to be modified.
                        if let Some(downstream_triggers) = db_schema
                            .processing_engine_write_triggers
                            .get(&new_table.table_id)
                        {
                            for (downstream_table_id, trigger) in downstream_triggers {
                                let downstream_table =
                                    db_schema.tables.get(downstream_table_id).unwrap();
                                let Some(trigger) =
                                    db_schema.processing_engine_calls.get(&trigger.trigger_name)
                                else {
                                    return Err(ProcessingEngineCallNotFound {
                                        call_name: trigger.trigger_name.to_string(),
                                        database_name: db_schema.name.to_string(),
                                    });
                                };
                                // TODO: Handle python errors
                                let new_schema = trigger
                                    .call(&RecordBatch::new_empty(new_table.schema.as_arrow()))
                                    .unwrap()
                                    .schema();
                                // TODO: Determine what semantics we want when new fields are added to source table.
                                if new_schema != downstream_table.schema.as_arrow() {
                                    return Err(ProcessingEngineUnimplemented {
                                        feature_description: "output schema update not supported"
                                            .to_string(),
                                    });
                                }
                            }
                        }

                        updated_or_new_tables.insert(new_table.table_id, Arc::new(new_table));
                    }
                }
                CatalogOp::CreateMetaCache(meta_cache_definition) => {
                    let table = updated_or_new_tables
                        .get(&meta_cache_definition.table_id)
                        .cloned()
                        .or_else(|| {
                            db_schema.table_definition_by_id(&meta_cache_definition.table_id)
                        })
                        .ok_or(TableNotFound {
                            db_name: Arc::clone(&db_schema.name),
                            table_name: Arc::clone(&meta_cache_definition.table_name),
                        })?;
                    if let Some(new_table) =
                        table.new_if_meta_cache_definition_is_new(meta_cache_definition)
                    {
                        updated_or_new_tables.insert(new_table.table_id, Arc::new(new_table));
                    }
                }
                CatalogOp::DeleteMetaCache(delete_meta_cache) => {
                    let table = updated_or_new_tables
                        .get(&delete_meta_cache.table_id)
                        .cloned()
                        .or_else(|| db_schema.table_definition_by_id(&delete_meta_cache.table_id))
                        .ok_or(TableNotFound {
                            db_name: Arc::clone(&db_schema.name),
                            table_name: Arc::clone(&delete_meta_cache.table_name),
                        })?;
                    if let Some(new_table) =
                        table.new_if_meta_cache_deletes_existing(delete_meta_cache)
                    {
                        updated_or_new_tables.insert(new_table.table_id, Arc::new(new_table));
                    }
                }
                CatalogOp::CreateLastCache(last_cache_definition) => {
                    let new_or_existing_table = updated_or_new_tables
                        .get(&last_cache_definition.table_id)
                        .or_else(|| db_schema.tables.get(&last_cache_definition.table_id));

                    let table = new_or_existing_table.ok_or(TableNotFound {
                        db_name: Arc::clone(&db_schema.name),
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
                        .or_else(|| db_schema.tables.get(&last_cache_deletion.table_id));

                    let table = new_or_existing_table.ok_or(TableNotFound {
                        db_name: Arc::clone(&db_schema.name),
                        table_name: Arc::clone(&last_cache_deletion.table_name),
                    })?;

                    if let Some(new_table) =
                        table.new_if_last_cache_deletes_existing(last_cache_deletion)
                    {
                        updated_or_new_tables.insert(new_table.table_id, Arc::new(new_table));
                    }
                }
                CatalogOp::DeleteDatabase(params) => {
                    schema_deleted = true;
                    let deletion_time = Time::from_timestamp_nanos(params.deletion_time);
                    schema_name =
                        make_new_name_using_deleted_time(&params.database_name, deletion_time);
                }
                CatalogOp::DeleteTable(table_definition) => {
                    table_deleted = true;
                    deleted_table_defn = Some(table_definition);
                }
            }
        }

        if updated_or_new_tables.is_empty() && !schema_deleted && !table_deleted {
            Ok(None)
        } else {
            for (table_id, table_def) in &db_schema.tables {
                if !updated_or_new_tables.contains_key(table_id) {
                    updated_or_new_tables.insert(*table_id, Arc::clone(table_def));
                }
            }

            check_and_mark_table_as_deleted(deleted_table_defn, &mut updated_or_new_tables);

            // With the final list of updated/new tables update the current mapping
            let new_table_maps = updated_or_new_tables
                .iter()
                .map(|(table_id, table_def)| (*table_id, Arc::clone(&table_def.table_name)))
                .collect();

            Ok(Some(Self {
                id: db_schema.id,
                name: schema_name,
                tables: updated_or_new_tables,
                table_map: new_table_maps,
                processing_engine_calls: HashMap::new(),
                processing_engine_write_triggers: HashMap::new(),
                processing_engine_source_table: HashMap::new(),
                deleted: schema_deleted,
            }))
        }
    }

    pub fn new_from_batch(catalog_batch: &CatalogBatch) -> Result<Self> {
        let db_schema = Self::new(
            catalog_batch.database_id,
            Arc::clone(&catalog_batch.database_name),
        );
        let new_db = DatabaseSchema::new_if_updated_from_batch(&db_schema, catalog_batch)?
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
        self.table_id_and_schema(table_name)
            .map(|(_, schema)| schema.clone())
    }

    pub fn table_schema_by_id(&self, table_id: &TableId) -> Option<Schema> {
        self.tables
            .get(table_id)
            .map(|table| table.influx_schema())
            .cloned()
    }

    pub fn table_id_and_schema(
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

    pub fn table_id_and_definition(
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

fn check_and_mark_table_as_deleted(
    deleted_table_defn: Option<&DeleteTableDefinition>,
    updated_or_new_tables: &mut SerdeVecMap<TableId, Arc<TableDefinition>>,
) {
    if let Some(deleted_table_defn) = deleted_table_defn {
        if let Some(deleted_table) = updated_or_new_tables.get_mut(&deleted_table_defn.table_id) {
            let deletion_time = Time::from_timestamp_nanos(deleted_table_defn.deletion_time);
            let table_name =
                make_new_name_using_deleted_time(&deleted_table_defn.table_name, deletion_time);
            let new_table_def = Arc::make_mut(deleted_table);
            new_table_def.deleted = true;
            new_table_def.table_name = table_name;
        }
    }
}

fn make_new_name_using_deleted_time(name: &str, deletion_time: Time) -> Arc<str> {
    Arc::from(format!(
        "{}-{}",
        name,
        deletion_time.date_time().format(SOFT_DELETION_TIME_FORMAT)
    ))
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
    pub meta_caches: HashMap<Arc<str>, MetaCacheDefinition>,
    pub deleted: bool,
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
            meta_caches: HashMap::new(),
            deleted: false,
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

    pub(crate) fn new_if_meta_cache_definition_is_new(
        &self,
        meta_cache_definition: &MetaCacheDefinition,
    ) -> Option<Self> {
        if self
            .meta_caches
            .contains_key(&meta_cache_definition.cache_name)
        {
            None
        } else {
            let mut new_table = self.clone();
            new_table.add_meta_cache(meta_cache_definition.clone());
            Some(new_table)
        }
    }

    pub(crate) fn new_if_meta_cache_deletes_existing(
        &self,
        meta_cache_delete: &MetaCacheDelete,
    ) -> Option<Self> {
        if self.meta_caches.contains_key(&meta_cache_delete.cache_name) {
            let mut new_table = self.clone();
            new_table.remove_meta_cache(&meta_cache_delete.cache_name);
            Some(new_table)
        } else {
            None
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

    /// Add the given [`MetaCacheDefinition`] to this table
    pub fn add_meta_cache(&mut self, meta_cache: MetaCacheDefinition) {
        self.meta_caches
            .insert(Arc::clone(&meta_cache.cache_name), meta_cache);
    }

    /// Remove the meta cache with the given name
    pub fn remove_meta_cache(&mut self, cache_name: &str) {
        self.meta_caches.remove(cache_name);
    }

    /// Add a new last cache to this table definition
    pub fn add_last_cache(&mut self, last_cache: LastCacheDefinition) {
        self.last_caches
            .insert(Arc::clone(&last_cache.name), last_cache);
    }

    /// Remove a last cache from the table definition by its name
    pub fn remove_last_cache(&mut self, cache_name: &str) {
        self.last_caches.remove(cache_name);
    }

    pub fn last_caches(&self) -> impl Iterator<Item = (Arc<str>, &LastCacheDefinition)> {
        self.last_caches
            .iter()
            .map(|(name, def)| (Arc::clone(name), def))
    }

    pub fn meta_caches(&self) -> impl Iterator<Item = (Arc<str>, &MetaCacheDefinition)> {
        self.meta_caches
            .iter()
            .map(|(name, def)| (Arc::clone(name), def))
    }

    pub fn column_name_to_id(&self, name: impl Into<Arc<str>>) -> Option<ColumnId> {
        self.column_map.get_by_right(&name.into()).copied()
    }

    pub fn column_id_to_name(&self, id: &ColumnId) -> Option<Arc<str>> {
        self.column_map.get_by_left(id).cloned()
    }

    pub fn column_name_to_id_unchecked(&self, name: impl Into<Arc<str>>) -> ColumnId {
        *self
            .column_map
            .get_by_right(&name.into())
            .expect("Column exists in mapping")
    }

    pub fn column_id_to_name_unchecked(&self, id: &ColumnId) -> Arc<str> {
        Arc::clone(
            self.column_map
                .get_by_left(id)
                .expect("Column exists in mapping"),
        )
    }

    pub fn column_id_and_definition(
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
            processing_engine_calls: Default::default(),
            processing_engine_write_triggers: HashMap::new(),
            processing_engine_source_table: HashMap::new(),
            deleted: false,
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
                            "table_map": [],
                            "deleted": false
                        }
                    ],
                    [
                        0,
                        {
                            "id": 0,
                            "name": "db1",
                            "tables": [],
                            "table_map": [],
                            "deleted": false
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
                                        "next_column_id": 0,
                                        "deleted": false

                                    }
                                ],
                                [
                                    0,
                                    {
                                        "table_id": 0,
                                        "table_name": "tbl1",
                                        "cols": [],
                                        "next_column_id": 0,
                                        "deleted": false

                                    }
                                ]
                            ],
                            "deleted": false
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
                                        ],
                                        "deleted": false

                                    }
                                ]
                            ],
                            "deleted": false
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
            processing_engine_calls: Default::default(),
            processing_engine_write_triggers: HashMap::new(),
            processing_engine_source_table: HashMap::new(),
            deleted: false,
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
        assert_eq!(table.column_name_to_id_unchecked("test2"), 1.into());
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
            processing_engine_calls: Default::default(),
            processing_engine_write_triggers: HashMap::new(),
            processing_engine_source_table: HashMap::new(),
            deleted: false,
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
            processing_engine_calls: Default::default(),
            processing_engine_write_triggers: HashMap::new(),
            processing_engine_source_table: HashMap::new(),
            deleted: false,
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

    #[test]
    fn test_check_and_mark_table_as_deleted() {
        let db_id = DbId::new();
        let deleted_table_id = TableId::new();
        let table_name = Arc::from("boo");
        let deleted_table_defn = DeleteTableDefinition {
            database_id: db_id,
            database_name: Arc::from("foo"),
            table_id: deleted_table_id,
            table_name: Arc::clone(&table_name),
            deletion_time: 0,
        };
        let mut map = IndexMap::new();
        let table_defn = Arc::new(
            TableDefinition::new(
                deleted_table_id,
                Arc::clone(&table_name),
                vec![
                    (ColumnId::from(0), "tag_1".into(), InfluxColumnType::Tag),
                    (ColumnId::from(1), "tag_2".into(), InfluxColumnType::Tag),
                    (ColumnId::from(2), "tag_3".into(), InfluxColumnType::Tag),
                    (
                        ColumnId::from(3),
                        "time".into(),
                        InfluxColumnType::Timestamp,
                    ),
                    (
                        ColumnId::from(4),
                        "field".into(),
                        InfluxColumnType::Field(InfluxFieldType::String),
                    ),
                ],
                SeriesKey::Some(vec![
                    ColumnId::from(0),
                    ColumnId::from(1),
                    ColumnId::from(2),
                ]),
            )
            .unwrap(),
        );
        map.insert(deleted_table_id, table_defn);
        let mut updated_or_new_tables = SerdeVecMap::from(map);

        check_and_mark_table_as_deleted(Some(&deleted_table_defn), &mut updated_or_new_tables);

        let deleted_table = updated_or_new_tables.get(&deleted_table_id).unwrap();
        assert_eq!(&*deleted_table.table_name, "boo-19700101T000000");
        assert!(deleted_table.deleted);
        assert!(deleted_table.series_key.is_some());
    }
}
