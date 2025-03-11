//! Implementation of the Catalog that sits entirely in memory.

use bimap::BiHashMap;
use hashbrown::HashMap;
use indexmap::IndexMap;
use influxdb3_id::{ColumnId, DbId, SerdeVecMap, TableId};
use iox_time::{Time, TimeProvider};
use object_store::ObjectStore;
use observability_deps::tracing::{debug, info, warn};
use parking_lot::RwLock;
use schema::{Schema, SchemaBuilder};
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::{borrow::Cow, ops::Deref};
use tokio::sync::{Mutex, MutexGuard, broadcast};
use update::CatalogUpdate;
use uuid::Uuid;

mod update;
pub use schema::{InfluxColumnType, InfluxFieldType};
pub use update::{DatabaseCatalogTransaction, Prompt};

use crate::id::IdProvider;
use crate::log::{
    CreateDatabaseLog, DatabaseBatch, DatabaseCatalogOp, NodeBatch, NodeCatalogOp, NodeMode,
    RegisterNodeLog,
};
use crate::object_store::ObjectStoreCatalog;
use crate::{
    CatalogError, Result,
    log::{
        AddFieldsLog, CatalogBatch, CreateDistinctCacheLog, CreateLastCacheLog, CreateTableLog,
        CreateTriggerLog, DeleteDistinctCacheLog, DeleteLastCacheLog, DeleteTriggerLog,
        FieldDefinition, OrderedCatalogBatch, SoftDeleteDatabaseLog, SoftDeleteTableLog,
        TriggerIdentifier,
    },
    snapshot::CatalogSnapshot,
};

const SOFT_DELETION_TIME_FORMAT: &str = "%Y%m%dT%H%M%S";

pub const TIME_COLUMN_NAME: &str = "time";

/// The sequence number of a batch of WAL operations.
#[derive(
    Debug, Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
pub struct CatalogSequenceNumber(u64);

impl CatalogSequenceNumber {
    pub const fn new(id: u64) -> Self {
        Self(id)
    }

    pub fn next(&self) -> Self {
        Self(self.0 + 1)
    }

    pub fn get(&self) -> u64 {
        self.0
    }
}

impl From<u64> for CatalogSequenceNumber {
    fn from(value: u64) -> Self {
        Self(value)
    }
}

static CATALOG_WRITE_PERMIT: Mutex<CatalogSequenceNumber> =
    Mutex::const_new(CatalogSequenceNumber::new(0));

/// Convenience type alias for the write permit on the catalog
///
/// This is a mutex that, when a lock is acquired, holds the next catalog sequence number at the
/// time that the permit was acquired.
pub type CatalogWritePermit = MutexGuard<'static, CatalogSequenceNumber>;

const CATALOG_BROADCAST_CHANNEL_CAPACITY: usize = 1_000;

pub type CatalogBroadcastSender = broadcast::Sender<Arc<CatalogUpdate>>;
pub type CatalogBroadcastReceiver = broadcast::Receiver<Arc<CatalogUpdate>>;

pub struct Catalog {
    /// Channel for broadcasting updates to other components that must handle `CatalogOp`s
    ///
    /// # Implementation Note
    ///
    /// This currently uses a `tokio::broadcast` channel, which can lead to dropped messages if
    /// the channel fills up. If that is a concern a more durable form of broadcasting single
    /// producer to multiple consumer messages would need to be implemented.
    channel: CatalogBroadcastSender,
    time_provider: Arc<dyn TimeProvider>,
    /// Connection to the object store for managing persistence and updates to the catalog
    store: ObjectStoreCatalog,
    /// In-memory representation of the catalog
    inner: RwLock<InnerCatalog>,
}

/// Custom implementation of `Debug` for the `Catalog` type to avoid serializing the object store
impl std::fmt::Debug for Catalog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Catalog")
            .field("inner", &self.inner)
            .finish()
    }
}

impl Catalog {
    /// Limit for the number of Databases that InfluxDB 3 Enterprise can have
    pub(crate) const NUM_DBS_LIMIT: usize = 100;
    /// Limit for the number of columns per table that InfluxDB 3 Enterprise can have
    pub(crate) const NUM_COLUMNS_PER_TABLE_LIMIT: usize = 500;
    /// Limit for the number of tables across all DBs that InfluxDB 3 Enterprise can have
    pub(crate) const NUM_TABLES_LIMIT: usize = 4000;

    pub async fn new(
        catalog_id: impl Into<Arc<str>>,
        store: Arc<dyn ObjectStore>,
        time_provider: Arc<dyn TimeProvider>,
    ) -> Result<Self> {
        let node_id = catalog_id.into();
        let store = ObjectStoreCatalog::new(Arc::clone(&node_id), store);
        let (channel, _) = broadcast::channel(CATALOG_BROADCAST_CHANNEL_CAPACITY);
        store
            .load_or_create_catalog()
            .await
            .map_err(Into::into)
            .map(RwLock::new)
            .map(|inner| Self {
                channel,
                time_provider,
                store,
                inner,
            })
    }

    pub fn object_store_prefix(&self) -> Arc<str> {
        Arc::clone(&self.store.prefix)
    }

    pub fn catalog_uuid(&self) -> Uuid {
        self.inner.read().catalog_uuid
    }

    pub fn subscribe_to_updates(&self) -> broadcast::Receiver<Arc<CatalogUpdate>> {
        self.channel.subscribe()
    }

    /// Create new `Catalog` that uses an in-memory object store.
    ///
    /// # Note
    ///
    /// This is intended as a convenience constructor for testing
    pub async fn new_in_memory(catalog_id: impl Into<Arc<str>>) -> Result<Self> {
        use iox_time::MockProvider;
        use object_store::memory::InMemory;

        let store = Arc::new(InMemory::new());
        let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
        Self::new(catalog_id.into(), store, time_provider).await
    }

    pub fn object_store(&self) -> Arc<dyn ObjectStore> {
        self.store.object_store()
    }

    pub fn snapshot(&self) -> CatalogSnapshot {
        self.inner.read().deref().into()
    }

    pub fn update_from_snapshot(&self, snapshot: CatalogSnapshot) {
        let mut inner = self.inner.write();
        *inner = snapshot.into();
    }

    pub async fn get_permit_and_verify_catalog_batch(
        &self,
        catalog_batch: &CatalogBatch,
    ) -> Result<Option<(OrderedCatalogBatch, CatalogWritePermit)>> {
        // Get the write permit, and update its contents with the next catalog sequence number. If
        // the `catalog_batch` provided results in an update, i.e., changes the catalog, then this
        // will be the sequence number that the catalog is updated to.
        let mut permit = CATALOG_WRITE_PERMIT.lock().await;
        *permit = self.sequence_number().next();
        debug!(
            next_sequence = permit.get(),
            "got permit to write to catalog"
        );
        Ok(self
            .inner
            .write()
            .verify_catalog_batch(catalog_batch, *permit)?
            .map(|batch| (batch, permit)))
    }

    /// Apply an `OrderedCatalogBatch` to this catalog
    ///
    /// # Implementation note
    ///
    /// This accepts a `_permit`, which is not used, and is just a way to ensure that the caller
    /// has a handle on the write permit at the time of invocation.
    pub fn apply_ordered_catalog_batch(
        &self,
        batch: &OrderedCatalogBatch,
        _permit: &CatalogWritePermit,
    ) -> CatalogBatch {
        let batch_sequence = batch.sequence_number().get();
        let current_sequence = self.sequence_number().get();
        debug!(
            batch_sequence,
            current_sequence, "apply ordered catalog batch"
        );
        assert_eq!(
            batch_sequence,
            current_sequence + 1,
            "catalog batch received out of order"
        );
        let catalog_batch = self
            .inner
            .write()
            .apply_catalog_batch(batch.batch(), batch.sequence_number())
            .expect("ordered catalog batch should succeed when applied")
            .expect("ordered catalog batch should contain changes");
        catalog_batch.into_batch()
    }

    pub fn node(&self, node_id: &str) -> Option<Arc<NodeDefinition>> {
        self.inner.read().nodes.get(node_id).cloned()
    }

    pub fn next_db_id(&self) -> DbId {
        self.inner.read().get_next_id()
    }

    pub fn db_or_create(
        &self,
        db_name: &str,
        now_time_ns: i64,
    ) -> Result<(Arc<DatabaseSchema>, Option<CatalogBatch>)> {
        match self.db_schema(db_name) {
            Some(db) => Ok((db, None)),
            None => {
                let mut inner = self.inner.write();

                if inner.database_count() >= Self::NUM_DBS_LIMIT {
                    return Err(CatalogError::TooManyDbs);
                }

                info!(database_name = db_name, "creating new database");
                let db_id = inner.get_and_increment_next_id();
                let db_name = db_name.into();
                let db = Arc::new(DatabaseSchema::new(db_id, Arc::clone(&db_name)));
                let batch = CatalogBatch::database(
                    now_time_ns,
                    db.id,
                    db.name(),
                    vec![DatabaseCatalogOp::CreateDatabase(CreateDatabaseLog {
                        database_id: db.id,
                        database_name: Arc::clone(&db.name),
                    })],
                );
                Ok((db, Some(batch)))
            }
        }
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

    /// List names of databases that have not been deleted
    pub fn db_names(&self) -> Vec<String> {
        self.inner
            .read()
            .databases
            .values()
            .filter(|db| !db.deleted)
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

    pub fn catalog_id(&self) -> Arc<str> {
        Arc::clone(&self.inner.read().catalog_id)
    }

    pub fn db_exists(&self, db_id: DbId) -> bool {
        self.inner.read().db_exists(db_id)
    }

    pub fn active_triggers(&self) -> Vec<(String, String)> {
        let inner = self.inner.read();
        let result = inner
            .databases
            .values()
            .flat_map(|schema| {
                schema
                    .processing_engine_triggers
                    .iter()
                    .filter_map(move |(key, trigger)| {
                        if trigger.disabled {
                            None
                        } else {
                            Some((schema.name.to_string(), key.to_string()))
                        }
                    })
            })
            .collect();
        result
    }
}

#[derive(Debug, Clone)]
pub struct InnerCatalog {
    pub(crate) sequence: CatalogSequenceNumber,
    /// The `catalog_id` is the user-provided value used to prefix catalog paths on the object store
    pub(crate) catalog_id: Arc<str>,
    /// The `catalog_uuid` is a unique identifier to distinguish catalog instantiations
    pub(crate) catalog_uuid: Uuid,
    pub(crate) nodes: SerdeVecMap<Arc<str>, Arc<NodeDefinition>>,
    /// The catalog is a map of databases with their table schemas
    pub(crate) databases: SerdeVecMap<DbId, Arc<DatabaseSchema>>,
    /// A bidirectional map for fast lookups of database name via ID, or vice versa
    pub(crate) db_map: BiHashMap<DbId, Arc<str>>,
    pub(crate) next_db_id: DbId,
}

impl InnerCatalog {
    pub(crate) fn new(catalog_id: Arc<str>, catalog_uuid: Uuid) -> Self {
        Self {
            nodes: SerdeVecMap::new(),
            databases: SerdeVecMap::new(),
            sequence: CatalogSequenceNumber::new(0),
            catalog_id,
            catalog_uuid,
            db_map: BiHashMap::new(),
            next_db_id: DbId::default(),
        }
    }

    pub fn sequence_number(&self) -> CatalogSequenceNumber {
        self.sequence
    }

    pub fn database_count(&self) -> usize {
        self.databases.iter().filter(|db| !db.1.deleted).count()
    }

    pub fn table_count(&self) -> usize {
        self.databases.values().map(|db| db.table_count()).sum()
    }

    /// Verifies the `CatalogBatch` by validating that all updates are compatible, and returns an
    /// `OrderedCatalogBatch` if the updates result in a change to the catalog, or `None` otherwise.
    pub(crate) fn verify_catalog_batch(
        &mut self,
        catalog_batch: &CatalogBatch,
        sequence: CatalogSequenceNumber,
    ) -> Result<Option<OrderedCatalogBatch>> {
        debug!(
            ?catalog_batch,
            current_sequence = self.sequence_number().get(),
            verified_sequence = sequence.get(),
            "verify catalog batch"
        );

        self.verify_or_apply_catalog_batch(catalog_batch, sequence, false)
    }

    /// Verifies _and_ applies the `CatalogBatch` to the catalog.
    pub(crate) fn apply_catalog_batch(
        &mut self,
        catalog_batch: &CatalogBatch,
        sequence: CatalogSequenceNumber,
    ) -> Result<Option<OrderedCatalogBatch>> {
        debug!(
            ?catalog_batch,
            current_sequence = self.sequence_number().get(),
            applied_sequence = sequence.get(),
            "apply catalog batch"
        );

        self.verify_or_apply_catalog_batch(catalog_batch, sequence, true)
    }

    /// Inner method for shared logic between `verify_catalog_batch` and `apply_catalog_batch`
    fn verify_or_apply_catalog_batch(
        &mut self,
        catalog_batch: &CatalogBatch,
        sequence: CatalogSequenceNumber,
        apply_changes: bool,
    ) -> Result<Option<OrderedCatalogBatch>> {
        let updated = match catalog_batch {
            CatalogBatch::Node(root_batch) => {
                self.verify_or_apply_node_batch(root_batch, apply_changes)?
            }
            CatalogBatch::Database(database_batch) => {
                self.verify_or_apply_database_batch(database_batch, apply_changes)?
            }
        };

        Ok(updated.then(|| {
            if apply_changes {
                debug!(
                    sequence = sequence.get(),
                    "catalog batch applied, updating sequence"
                );
                self.sequence = sequence;
            } else {
                debug!(
                    sequence = sequence.get(),
                    "catalog batch verified, will update sequence"
                );
            }
            OrderedCatalogBatch::new(catalog_batch.clone(), sequence)
        }))
    }

    fn verify_or_apply_node_batch(
        &mut self,
        node_batch: &NodeBatch,
        apply_changes: bool,
    ) -> Result<bool> {
        let mut updated = false;
        for op in &node_batch.ops {
            updated |= match op {
                NodeCatalogOp::RegisterNode(RegisterNodeLog {
                    node_id,
                    instance_id,
                    registered_time_ns,
                    core_count,
                    mode,
                }) => {
                    let new_node = Arc::new(NodeDefinition {
                        node_id: Arc::clone(node_id),
                        instance_id: Arc::clone(instance_id),
                        mode: mode.clone(),
                        core_count: *core_count,
                        state: NodeState::Running {
                            registered_time_ns: *registered_time_ns,
                        },
                    });
                    if let Some(node) = self.nodes.get(node_id) {
                        if &node.instance_id != instance_id {
                            return Err(CatalogError::InvalidNodeRegistration);
                        }
                        if *node == new_node {
                            continue;
                        }
                    }
                    if apply_changes {
                        self.nodes.insert(Arc::clone(node_id), new_node);
                    }
                    true
                }
            };
        }
        Ok(updated)
    }

    fn verify_or_apply_database_batch(
        &mut self,
        database_batch: &DatabaseBatch,
        apply_changes: bool,
    ) -> Result<bool> {
        let table_count = self.table_count();
        let db = if let Some(db) = self.databases.get(&database_batch.database_id) {
            let Some(new_db) = DatabaseSchema::new_if_updated_from_batch(db, database_batch)?
            else {
                return Ok(false);
            };
            check_overall_table_count(Some(db), &new_db, table_count)?;
            new_db
        } else {
            if self.database_count() >= Catalog::NUM_DBS_LIMIT {
                return Err(CatalogError::TooManyDbs);
            }
            let new_db = DatabaseSchema::new_from_batch(database_batch)?;
            check_overall_table_count(None, &new_db, table_count)?;
            new_db
        };
        if apply_changes {
            let name = Arc::clone(&db.name);
            let id = db.id;
            self.next_db_id = self.next_db_id.max(id.next());
            self.databases.insert(id, Arc::new(db));
            self.db_map.insert(id, name);
        }
        Ok(true)
    }

    pub fn db_exists(&self, db_id: DbId) -> bool {
        self.databases.contains_key(&db_id)
    }
}

fn check_overall_table_count(
    existing_db: Option<&Arc<DatabaseSchema>>,
    new_db: &DatabaseSchema,
    current_table_count: usize,
) -> Result<()> {
    let existing_table_count = if let Some(existing_db) = existing_db {
        existing_db.table_count()
    } else {
        0
    };
    let new_table_count = new_db.table_count();
    match new_table_count.cmp(&existing_table_count) {
        Ordering::Less | Ordering::Equal => Ok(()),
        Ordering::Greater => {
            let newly_added_table_count = new_db.table_count() - existing_table_count;
            if current_table_count + newly_added_table_count > Catalog::NUM_TABLES_LIMIT {
                Err(CatalogError::TooManyTables)
            } else {
                Ok(())
            }
        }
    }
}

#[derive(Debug, Eq, PartialEq, Clone)]
pub struct NodeDefinition {
    pub(crate) node_id: Arc<str>,
    pub(crate) instance_id: Arc<str>,
    pub(crate) mode: Vec<NodeMode>,
    pub(crate) core_count: u64,
    pub(crate) state: NodeState,
}

impl NodeDefinition {
    pub fn instance_id(&self) -> Arc<str> {
        Arc::clone(&self.instance_id)
    }
}

#[derive(Debug, Eq, PartialEq, Clone, Copy, Serialize, Deserialize)]
pub enum NodeState {
    Running { registered_time_ns: i64 },
    Stopped { stopped_time_ns: i64 },
}

#[derive(Debug, Eq, PartialEq, Clone)]
pub struct DatabaseSchema {
    pub id: DbId,
    pub name: Arc<str>,
    pub tables: SerdeVecMap<TableId, Arc<TableDefinition>>,
    pub table_map: BiHashMap<TableId, Arc<str>>,
    pub processing_engine_triggers: HashMap<String, CreateTriggerLog>,
    pub deleted: bool,
    pub(crate) next_table_id: TableId,
}

impl DatabaseSchema {
    pub fn new(id: DbId, name: Arc<str>) -> Self {
        Self {
            id,
            name,
            tables: Default::default(),
            table_map: BiHashMap::new(),
            processing_engine_triggers: HashMap::new(),
            deleted: false,
            next_table_id: TableId::default(),
        }
    }

    pub fn name(&self) -> Arc<str> {
        Arc::clone(&self.name)
    }

    pub fn table_count(&self) -> usize {
        self.tables.iter().filter(|table| !table.1.deleted).count()
    }

    /// Validates the updates in the `CatalogBatch` are compatible with this schema. If
    /// everything is compatible and there are no updates to the existing schema, None will be
    /// returned, otherwise a new `DatabaseSchema` will be returned with the updates applied.
    pub fn new_if_updated_from_batch(
        db_schema: &DatabaseSchema,
        database_batch: &DatabaseBatch,
    ) -> Result<Option<Self>> {
        debug!(
            name = ?db_schema.name,
            deleted = ?db_schema.deleted,
            full_batch = ?database_batch,
            "updating / adding to catalog"
        );

        let mut schema = Cow::Borrowed(db_schema);

        for catalog_op in &database_batch.ops {
            schema = catalog_op.update_schema(schema)?;
        }
        // If there were updates then it will have become owned, so we should return the new schema.
        if let Cow::Owned(schema) = schema {
            Ok(Some(schema))
        } else {
            Ok(None)
        }
    }

    pub fn new_from_batch(database_batch: &DatabaseBatch) -> Result<Self> {
        let db_schema = Self::new(
            database_batch.database_id,
            Arc::clone(&database_batch.database_name),
        );
        let new_db = DatabaseSchema::new_if_updated_from_batch(&db_schema, database_batch)?
            .expect("database must be new");
        Ok(new_db)
    }

    /// Create a new empty table definition with the given `table_name` in the database.
    ///
    /// # Panics
    ///
    /// This panics if either the `tables` map or `table_map` contain the table already.
    pub(crate) fn create_new_empty_table(
        &mut self,
        table_name: impl Into<Arc<str>>,
    ) -> Result<Arc<TableDefinition>> {
        let table_id = self.get_next_id();
        let table_def = Arc::new(TableDefinition::new_empty(table_id, table_name.into()));
        if self.table_map.contains_right(&table_def.table_name)
            || self.table_map.contains_left(&table_id)
        {
            return Err(CatalogError::AlreadyExists);
        }
        assert!(
            !self
                .table_map
                .insert(table_id, Arc::clone(&table_def.table_name))
                .did_overwrite(),
            "table is in an unexpected state"
        );
        assert!(
            self.tables
                .insert(table_id, Arc::clone(&table_def))
                .is_none(),
            "table map and table name/id map are in inconsistent state"
        );
        self.next_table_id = self.next_table_id.next();
        Ok(table_def)
    }

    /// Update a table in the database. This fails if the table doesn't exist.
    ///
    /// # Panics
    ///
    /// This panics if the `tables` map and `table_map` are in an inconsistent state.
    pub(crate) fn update_table(
        &mut self,
        table_id: TableId,
        table_def: Arc<TableDefinition>,
    ) -> Result<Arc<TableDefinition>> {
        let in_table_map = self.table_map.contains_left(&table_id);
        let in_tables = self.tables.contains_key(&table_id);
        assert_eq!(
            in_table_map, in_tables,
            "tables map and table id/name maps are in inconsistent state"
        );
        if !in_tables {
            return Err(CatalogError::NotFound);
        }
        self.table_map
            .insert(table_id, Arc::clone(&table_def.table_name));
        self.tables
            .insert(table_id, table_def)
            .ok_or_else(|| CatalogError::NotFound)
    }

    /// Insert a [`TableDefinition`] to the `tables` map and also update the `table_map` and
    /// increment the database next id.
    ///
    /// # Implementation Note
    ///
    /// This method is intended for table definitions being inserted from a log, where the `TableId`
    /// is known, but the table does not yet exist in this instance of the `DatabaseSchema`, i.e.,
    /// on catalog initialization/replay.
    pub fn insert_table_from_log(&mut self, table_id: TableId, table_def: Arc<TableDefinition>) {
        debug!(table_id = table_id.get(), "insert table from log");
        let in_table_map = self.table_map.contains_left(&table_id)
            || self.table_map.contains_right(&table_def.table_name);
        let in_tables = self.tables.contains_key(&table_id);
        assert!(
            !in_tables && !in_table_map,
            "table created from log should not exist already"
        );
        self.table_map
            .insert(table_id, Arc::clone(&table_def.table_name));
        self.next_table_id = self.next_table_id.max(table_id.next());
        self.tables.insert(table_id, table_def);
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

    pub fn list_distinct_caches(&self) -> Vec<&CreateDistinctCacheLog> {
        self.tables
            .values()
            .filter(|t| !t.deleted)
            .flat_map(|t| t.distinct_caches())
            .map(|(_, def)| def)
            .collect()
    }

    pub fn list_last_caches(&self) -> Vec<&CreateLastCacheLog> {
        self.tables
            .values()
            .filter(|t| !t.deleted)
            .flat_map(|t| t.last_caches())
            .map(|(_, def)| def)
            .collect()
    }
}

trait UpdateDatabaseSchema {
    fn update_schema<'a>(&self, schema: Cow<'a, DatabaseSchema>)
    -> Result<Cow<'a, DatabaseSchema>>;
}

impl UpdateDatabaseSchema for DatabaseCatalogOp {
    fn update_schema<'a>(
        &self,
        mut schema: Cow<'a, DatabaseSchema>,
    ) -> Result<Cow<'a, DatabaseSchema>> {
        match &self {
            DatabaseCatalogOp::CreateDatabase(create_database) => {
                if create_database.database_id != schema.id
                    || create_database.database_name != schema.name
                {
                    warn!(
                        "Create database call received by a mismatched DatabaseSchema. This should not be possible."
                    )
                }
                // NOTE(trevor/catalog-refactor): if the database is already there, shouldn't this
                // be a no-op, and not to_mut the cow?
                schema.to_mut();
                Ok(schema)
            }
            DatabaseCatalogOp::CreateTable(create_table) => create_table.update_schema(schema),
            DatabaseCatalogOp::AddFields(field_additions) => field_additions.update_schema(schema),
            DatabaseCatalogOp::CreateDistinctCache(distinct_cache_definition) => {
                distinct_cache_definition.update_schema(schema)
            }
            DatabaseCatalogOp::DeleteDistinctCache(delete_distinct_cache) => {
                delete_distinct_cache.update_schema(schema)
            }
            DatabaseCatalogOp::CreateLastCache(create_last_cache) => {
                create_last_cache.update_schema(schema)
            }
            DatabaseCatalogOp::DeleteLastCache(delete_last_cache) => {
                delete_last_cache.update_schema(schema)
            }
            DatabaseCatalogOp::SoftDeleteDatabase(delete_database) => {
                delete_database.update_schema(schema)
            }
            DatabaseCatalogOp::SoftDeleteTable(delete_table) => delete_table.update_schema(schema),
            DatabaseCatalogOp::CreateTrigger(create_trigger) => {
                create_trigger.update_schema(schema)
            }
            DatabaseCatalogOp::DeleteTrigger(delete_trigger) => {
                delete_trigger.update_schema(schema)
            }
            DatabaseCatalogOp::EnableTrigger(trigger_identifier) => {
                EnableTrigger(trigger_identifier.clone()).update_schema(schema)
            }
            DatabaseCatalogOp::DisableTrigger(trigger_identifier) => {
                DisableTrigger(trigger_identifier.clone()).update_schema(schema)
            }
        }
    }
}

impl UpdateDatabaseSchema for CreateTableLog {
    fn update_schema<'a>(
        &self,
        mut database_schema: Cow<'a, DatabaseSchema>,
    ) -> Result<Cow<'a, DatabaseSchema>> {
        match database_schema.tables.get(&self.table_id) {
            Some(existing_table) => {
                debug!("creating existing table");
                if let Cow::Owned(updated_table) = existing_table.check_and_add_new_fields(self)? {
                    database_schema
                        .to_mut()
                        .update_table(self.table_id, Arc::new(updated_table))?;
                }
            }
            None => {
                debug!(log = ?self, "creating new table from log");
                let new_table = TableDefinition::new_from_op(self);
                database_schema
                    .to_mut()
                    .insert_table_from_log(new_table.table_id, Arc::new(new_table));
            }
        }

        debug!("updated schema for create table");
        Ok(database_schema)
    }
}

impl UpdateDatabaseSchema for SoftDeleteDatabaseLog {
    fn update_schema<'a>(
        &self,
        mut schema: Cow<'a, DatabaseSchema>,
    ) -> Result<Cow<'a, DatabaseSchema>> {
        let deletion_time = Time::from_timestamp_nanos(self.deletion_time);
        let owned = schema.to_mut();
        owned.name = make_new_name_using_deleted_time(&self.database_name, deletion_time);
        owned.deleted = true;
        Ok(schema)
    }
}

impl UpdateDatabaseSchema for SoftDeleteTableLog {
    fn update_schema<'a>(
        &self,
        mut schema: Cow<'a, DatabaseSchema>,
    ) -> Result<Cow<'a, DatabaseSchema>> {
        // unlike other table ops, this is not an error.
        if !schema.tables.contains_key(&self.table_id) {
            return Ok(schema);
        }
        let mut_schema = schema.to_mut();
        if let Some(deleted_table) = mut_schema.tables.get_mut(&self.table_id) {
            let deletion_time = Time::from_timestamp_nanos(self.deletion_time);
            let table_name = make_new_name_using_deleted_time(&self.table_name, deletion_time);
            let new_table_def = Arc::make_mut(deleted_table);
            new_table_def.deleted = true;
            new_table_def.table_name = table_name;
            mut_schema.table_map.insert(
                new_table_def.table_id,
                Arc::clone(&new_table_def.table_name),
            );
        }
        Ok(schema)
    }
}

struct EnableTrigger(TriggerIdentifier);
struct DisableTrigger(TriggerIdentifier);

impl UpdateDatabaseSchema for EnableTrigger {
    fn update_schema<'a>(
        &self,
        mut schema: Cow<'a, DatabaseSchema>,
    ) -> Result<Cow<'a, DatabaseSchema>> {
        let Some(trigger) = schema.processing_engine_triggers.get(&self.0.trigger_name) else {
            return Err(CatalogError::ProcessingEngineTriggerNotFound {
                database_name: self.0.db_name.to_string(),
                trigger_name: self.0.trigger_name.to_string(),
            });
        };
        if !trigger.disabled {
            return Ok(schema);
        }
        let mut_trigger = schema
            .to_mut()
            .processing_engine_triggers
            .get_mut(&self.0.trigger_name)
            .expect("already checked containment");
        mut_trigger.disabled = false;
        Ok(schema)
    }
}

impl UpdateDatabaseSchema for DisableTrigger {
    fn update_schema<'a>(
        &self,
        mut schema: Cow<'a, DatabaseSchema>,
    ) -> Result<Cow<'a, DatabaseSchema>> {
        let Some(trigger) = schema.processing_engine_triggers.get(&self.0.trigger_name) else {
            return Err(CatalogError::ProcessingEngineTriggerNotFound {
                database_name: self.0.db_name.to_string(),
                trigger_name: self.0.trigger_name.to_string(),
            });
        };
        if trigger.disabled {
            return Ok(schema);
        }
        let mut_trigger = schema
            .to_mut()
            .processing_engine_triggers
            .get_mut(&self.0.trigger_name)
            .expect("already checked containment");
        mut_trigger.disabled = true;
        Ok(schema)
    }
}

impl UpdateDatabaseSchema for CreateTriggerLog {
    fn update_schema<'a>(
        &self,
        mut schema: Cow<'a, DatabaseSchema>,
    ) -> Result<Cow<'a, DatabaseSchema>> {
        if let Some(current) = schema.processing_engine_triggers.get(&self.trigger_name) {
            if current == self {
                return Ok(schema);
            }
            return Err(CatalogError::ProcessingEngineTriggerExists {
                database_name: schema.name.to_string(),
                trigger_name: self.trigger_name.to_string(),
            });
        }
        schema
            .to_mut()
            .processing_engine_triggers
            .insert(self.trigger_name.to_string(), self.clone());
        Ok(schema)
    }
}

impl UpdateDatabaseSchema for DeleteTriggerLog {
    fn update_schema<'a>(
        &self,
        mut schema: Cow<'a, DatabaseSchema>,
    ) -> Result<Cow<'a, DatabaseSchema>> {
        let Some(trigger) = schema.processing_engine_triggers.get(&self.trigger_name) else {
            // deleting a non-existent trigger is a no-op to make it idempotent.
            return Ok(schema);
        };
        if !trigger.disabled && !self.force {
            if self.force {
                warn!("deleting running trigger {}", self.trigger_name);
            } else {
                return Err(CatalogError::ProcessingEngineTriggerRunning {
                    trigger_name: self.trigger_name.to_string(),
                });
            }
        }
        schema
            .to_mut()
            .processing_engine_triggers
            .remove(&self.trigger_name);

        Ok(schema)
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
    pub series_key: Vec<ColumnId>,
    pub series_key_names: Vec<Arc<str>>,
    pub last_caches: HashMap<Arc<str>, CreateLastCacheLog>,
    pub distinct_caches: HashMap<Arc<str>, CreateDistinctCacheLog>,
    pub deleted: bool,
    pub(crate) next_column_id: ColumnId,
}

impl TableDefinition {
    /// Create new empty `TableDefinition`
    pub fn new_empty(table_id: TableId, table_name: Arc<str>) -> Self {
        Self::new(table_id, table_name, vec![], vec![])
            .expect("empty table should create without error")
    }

    /// Create a new [`TableDefinition`]
    ///
    /// Ensures the provided columns will be ordered before constructing the schema.
    pub fn new(
        table_id: TableId,
        table_name: Arc<str>,
        columns: Vec<(ColumnId, Arc<str>, InfluxColumnType)>,
        series_key: Vec<ColumnId>,
    ) -> Result<Self> {
        // ensure we're under the column limit
        if columns.len() > Catalog::NUM_COLUMNS_PER_TABLE_LIMIT {
            return Err(CatalogError::TooManyColumns);
        }

        // Use a BTree to ensure that the columns are ordered:
        let mut ordered_columns = BTreeMap::new();
        for (col_id, name, column_type) in &columns {
            ordered_columns.insert(name.as_ref(), (col_id, column_type));
        }
        let mut schema_builder = SchemaBuilder::with_capacity(columns.len());
        schema_builder.measurement(table_name.as_ref());
        let next_column_id = if columns.is_empty() {
            ColumnId::default()
        } else {
            columns
                .iter()
                .map(|c| c.0)
                .max()
                .expect("there should be max column id when columns provided")
                .next()
        };
        let mut columns = IndexMap::with_capacity(ordered_columns.len());
        let mut column_map = BiHashMap::with_capacity(ordered_columns.len());
        for (name, (col_id, column_type)) in ordered_columns {
            schema_builder.influx_column(name, *column_type);
            let not_nullable = matches!(column_type, InfluxColumnType::Timestamp);
            assert!(
                columns
                    .insert(
                        *col_id,
                        ColumnDefinition::new(*col_id, name, *column_type, !not_nullable),
                    )
                    .is_none(),
                "table definition initialized with duplicate column ids"
            );
            column_map.insert(*col_id, name.into());
        }
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
        schema_builder.with_series_key(&series_key_names);
        let schema = schema_builder.build().expect("schema should be valid");

        Ok(Self {
            table_id,
            table_name,
            schema,
            columns,
            column_map,
            series_key,
            series_key_names,
            last_caches: HashMap::new(),
            distinct_caches: HashMap::new(),
            deleted: false,
            next_column_id,
        })
    }

    /// Create a new table definition from a catalog op
    pub fn new_from_op(table_definition: &CreateTableLog) -> Self {
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

    pub(crate) fn check_and_add_new_fields(
        &self,
        table_definition: &CreateTableLog,
    ) -> Result<Cow<'_, Self>> {
        Self::add_fields(Cow::Borrowed(self), &table_definition.field_definitions)
    }

    pub(crate) fn add_fields<'a>(
        mut table: Cow<'a, Self>,
        fields: &Vec<FieldDefinition>,
    ) -> Result<Cow<'a, Self>> {
        let mut new_fields: Vec<(ColumnId, Arc<str>, InfluxColumnType)> =
            Vec::with_capacity(fields.len());
        for field_def in fields {
            if let Some(existing_type) = table.columns.get(&field_def.id).map(|def| def.data_type) {
                if existing_type != field_def.data_type.into() {
                    return Err(CatalogError::FieldTypeMismatch {
                        table_name: table.table_name.to_string(),
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

        if !new_fields.is_empty() {
            let table = table.to_mut();
            table.next_column_id = new_fields
                .iter()
                .map(|f| f.0.next())
                .max()
                .unwrap_or(table.next_column_id);
            table.add_columns(new_fields)?;
        }
        Ok(table)
    }

    /// Check if the column exists in the [`TableDefinition`]
    pub fn column_exists(&self, column: impl Into<Arc<str>>) -> bool {
        self.column_map.get_by_right(&column.into()).is_some()
    }

    pub(crate) fn add_column(
        &mut self,
        column_name: Arc<str>,
        column_type: InfluxColumnType,
    ) -> Result<ColumnId> {
        let col_id = self.get_and_increment_next_id();
        self.add_columns(vec![(col_id, column_name, column_type)])?;
        Ok(col_id)
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
            let nullable = name.as_ref() != TIME_COLUMN_NAME;
            assert!(
                cols.insert(
                    Arc::clone(&name),
                    ColumnDefinition::new(id, name, column_type, nullable)
                )
                .is_none(),
                "attempted to add existing column"
            );
            // add new tags to the series key in the order provided
            if matches!(column_type, InfluxColumnType::Tag) && !self.series_key.contains(&id) {
                self.series_key.push(id);
            }
        }

        // ensure we don't go over the column limit
        if cols.len() > Catalog::NUM_COLUMNS_PER_TABLE_LIMIT {
            return Err(CatalogError::TooManyColumns);
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

    /// Add the `distinct_cache` to this table
    pub fn add_distinct_cache(&mut self, distinct_cache: CreateDistinctCacheLog) {
        self.distinct_caches
            .insert(Arc::clone(&distinct_cache.cache_name), distinct_cache);
    }

    /// Remove the distinct cache with the given name
    pub fn remove_distinct_cache(&mut self, cache_name: &str) {
        self.distinct_caches.remove(cache_name);
    }

    /// Add a new last cache to this table definition
    pub fn add_last_cache(&mut self, last_cache: CreateLastCacheLog) {
        self.last_caches
            .insert(Arc::clone(&last_cache.name), last_cache);
    }

    /// Remove a last cache from the table definition by its name
    pub fn remove_last_cache(&mut self, cache_name: &str) {
        self.last_caches.remove(cache_name);
    }

    pub fn last_caches(&self) -> impl Iterator<Item = (Arc<str>, &CreateLastCacheLog)> {
        self.last_caches
            .iter()
            .map(|(name, def)| (Arc::clone(name), def))
    }

    pub fn distinct_caches(&self) -> impl Iterator<Item = (Arc<str>, &CreateDistinctCacheLog)> {
        self.distinct_caches
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

    pub fn column_definition(&self, name: impl Into<Arc<str>>) -> Option<&ColumnDefinition> {
        self.column_map
            .get_by_right(&name.into())
            .and_then(|id| self.columns.get(id))
    }

    pub fn column_definition_by_id(&self, id: &ColumnId) -> Option<&ColumnDefinition> {
        self.columns.get(id)
    }

    pub fn column_id_and_definition(
        &self,
        name: impl Into<Arc<str>>,
    ) -> Option<(ColumnId, &ColumnDefinition)> {
        self.column_map
            .get_by_right(&name.into())
            .and_then(|id| self.columns.get(id).map(|def| (*id, def)))
    }

    pub fn series_key_ids(&self) -> &[ColumnId] {
        &self.series_key
    }

    pub fn series_key_names(&self) -> &[Arc<str>] {
        &self.series_key_names
    }
}

trait TableUpdate {
    fn table_id(&self) -> TableId;
    fn table_name(&self) -> Arc<str>;
    fn update_table<'a>(&self, table: Cow<'a, TableDefinition>)
    -> Result<Cow<'a, TableDefinition>>;
}

impl<T: TableUpdate> UpdateDatabaseSchema for T {
    fn update_schema<'a>(
        &self,
        mut schema: Cow<'a, DatabaseSchema>,
    ) -> Result<Cow<'a, DatabaseSchema>> {
        let Some(table) = schema.tables.get(&self.table_id()) else {
            return Err(CatalogError::TableNotFound {
                db_name: Arc::clone(&schema.name),
                table_name: Arc::clone(&self.table_name()),
            });
        };
        if let Cow::Owned(new_table) = self.update_table(Cow::Borrowed(table.as_ref()))? {
            schema
                .to_mut()
                .update_table(new_table.table_id, Arc::new(new_table))?;
        }
        Ok(schema)
    }
}

impl TableUpdate for AddFieldsLog {
    fn table_id(&self) -> TableId {
        self.table_id
    }
    fn table_name(&self) -> Arc<str> {
        Arc::clone(&self.table_name)
    }
    fn update_table<'a>(
        &self,
        table: Cow<'a, TableDefinition>,
    ) -> Result<Cow<'a, TableDefinition>> {
        TableDefinition::add_fields(table, &self.field_definitions)
    }
}

impl TableUpdate for CreateDistinctCacheLog {
    fn table_id(&self) -> TableId {
        self.table_id
    }
    fn table_name(&self) -> Arc<str> {
        Arc::clone(&self.table_name)
    }
    fn update_table<'a>(
        &self,
        mut table: Cow<'a, TableDefinition>,
    ) -> Result<Cow<'a, TableDefinition>> {
        if !table.distinct_caches.contains_key(&self.cache_name) {
            table.to_mut().add_distinct_cache(self.clone());
        }
        Ok(table)
    }
}

impl TableUpdate for DeleteDistinctCacheLog {
    fn table_id(&self) -> TableId {
        self.table_id
    }
    fn table_name(&self) -> Arc<str> {
        Arc::clone(&self.table_name)
    }
    fn update_table<'a>(
        &self,
        mut table: Cow<'a, TableDefinition>,
    ) -> Result<Cow<'a, TableDefinition>> {
        if table.distinct_caches.contains_key(&self.cache_name) {
            table.to_mut().distinct_caches.remove(&self.cache_name);
        }
        Ok(table)
    }
}

impl TableUpdate for CreateLastCacheLog {
    fn table_id(&self) -> TableId {
        self.table_id
    }

    fn table_name(&self) -> Arc<str> {
        Arc::clone(&self.table)
    }

    fn update_table<'a>(
        &self,
        mut table: Cow<'a, TableDefinition>,
    ) -> Result<Cow<'a, TableDefinition>> {
        if !table.last_caches.contains_key(&self.name) {
            table.to_mut().add_last_cache(self.clone());
        }
        Ok(table)
    }
}

impl TableUpdate for DeleteLastCacheLog {
    fn table_id(&self) -> TableId {
        self.table_id
    }
    fn table_name(&self) -> Arc<str> {
        Arc::clone(&self.table_name)
    }

    fn update_table<'a>(
        &self,
        mut table: Cow<'a, TableDefinition>,
    ) -> Result<Cow<'a, TableDefinition>> {
        if table.last_caches.contains_key(&self.name) {
            table.to_mut().last_caches.remove(&self.name);
        }
        Ok(table)
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

#[cfg(test)]
mod tests {
    use core::panic;

    use crate::{
        log::{FieldDataType, LastCacheSize, LastCacheTtl, MaxAge, MaxCardinality, create},
        serialize::{CatalogFile, serialize_catalog_snapshot, verify_and_deserialize_catalog},
    };

    use super::*;
    use pretty_assertions::assert_eq;
    use test_helpers::assert_contains;

    #[test_log::test(tokio::test)]
    async fn catalog_serialization() {
        let catalog = Catalog::new_in_memory("sample-host-id").await.unwrap();
        catalog.create_database("test_db").await.unwrap();
        catalog
            .create_table(
                "test_db",
                "test_table_1",
                &["tag_1", "tag_2", "tag_3"],
                &[
                    ("string_field", FieldDataType::String),
                    ("bool_field", FieldDataType::Boolean),
                    ("i64_field", FieldDataType::Integer),
                    ("u64_field", FieldDataType::UInteger),
                    ("float_field", FieldDataType::Float),
                ],
            )
            .await
            .unwrap();
        catalog
            .create_table(
                "test_db",
                "test_table_2",
                &["tag_1", "tag_2", "tag_3"],
                &[
                    ("string_field", FieldDataType::String),
                    ("bool_field", FieldDataType::Boolean),
                    ("i64_field", FieldDataType::Integer),
                    ("u64_field", FieldDataType::UInteger),
                    ("float_field", FieldDataType::Float),
                ],
            )
            .await
            .unwrap();

        insta::allow_duplicates! {
            insta::with_settings!({
                sort_maps => true,
                description => "catalog serialization to help catch breaking changes"
            }, {
                let snapshot = catalog.snapshot();
                insta::assert_json_snapshot!(snapshot, {
                    ".catalog_uuid" => "[uuid]"
                });
                // Serialize/deserialize to ensure roundtrip
                let serialized = serialize_catalog_snapshot(&snapshot).unwrap();
                let CatalogFile::Snapshot(snapshot) = verify_and_deserialize_catalog(serialized).unwrap() else {
                    panic!("wrong catalog file type");
                };
                insta::assert_json_snapshot!(snapshot, {
                    ".catalog_uuid" => "[uuid]"
                });
                catalog.update_from_snapshot(snapshot);
                assert_eq!(catalog.db_name_to_id("test_db"), Some(DbId::from(0)));
            });
        }
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
                            "deleted": false,
                            "next_table_id": 0
                        }
                    ],
                    [
                        0,
                        {
                            "id": 0,
                            "name": "db1",
                            "tables": [],
                            "table_map": [],
                            "deleted": false,
                            "next_table_id": 0
                        }
                    ]
                ],
                "sequence": 0,
                "catalog_id": "test",
                "catalog_uuid": "test",
                "next_db_id": 1
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
                                        "deleted": false,
                                        "key": []

                                    }
                                ],
                                [
                                    0,
                                    {
                                        "table_id": 0,
                                        "table_name": "tbl1",
                                        "cols": [],
                                        "next_column_id": 0,
                                        "deleted": false,
                                        "key": []
                                    }
                                ]
                            ],
                            "deleted": false
                        }
                    ]
                ],
                "sequence": 0,
                "catalog_id": "test",
                "catalog_uuid": "test",
                "next_db_id": 1
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
                ],
                "catalog_id": "test",
                "catalog_uuid": "test",
                "next_db_id": 1
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
            processing_engine_triggers: Default::default(),
            deleted: false,
            next_table_id: Default::default(),
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
                    vec![],
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

    #[tokio::test]
    async fn serialize_series_keys() {
        let catalog = Catalog::new_in_memory("sample-host-id").await.unwrap();
        catalog.create_database("test_db").await.unwrap();
        catalog
            .create_table(
                "test_db",
                "test_table_1",
                &["tag_1", "tag_2", "tag_3"],
                &[("field", FieldDataType::String)],
            )
            .await
            .unwrap();

        insta::allow_duplicates! {
            insta::with_settings!({
                sort_maps => true,
                description => "catalog serialization to help catch breaking changes"
            }, {
                let snapshot = catalog.snapshot();
                insta::assert_json_snapshot!(snapshot, {
                    ".catalog_uuid" => "[uuid]"
                });
                // Serialize/deserialize to ensure roundtrip
                let serialized = serialize_catalog_snapshot(&snapshot).unwrap();
                let CatalogFile::Snapshot(snapshot) = verify_and_deserialize_catalog(serialized).unwrap() else {
                    panic!("wrong catalog file type");
                };
                insta::assert_json_snapshot!(snapshot, {
                    ".catalog_uuid" => "[uuid]"
                });
                catalog.update_from_snapshot(snapshot);
                assert_eq!(catalog.db_name_to_id("test_db"), Some(DbId::from(0)));
            });
        }
    }

    #[tokio::test]
    async fn serialize_last_cache() {
        let catalog = Catalog::new_in_memory("sample-host-id").await.unwrap();
        catalog.create_database("test_db").await.unwrap();
        catalog
            .create_table(
                "test_db",
                "test",
                &["tag_1", "tag_2", "tag_3"],
                &[("field", FieldDataType::String)],
            )
            .await
            .unwrap();
        catalog
            .create_last_cache(
                "test_db",
                "test",
                Some("test_table_last_cache"),
                Some(&["tag_1", "tag_3"]),
                Some(&["field"]),
                LastCacheSize::new(1).unwrap(),
                LastCacheTtl::from_secs(600),
            )
            .await
            .unwrap();

        insta::allow_duplicates! {
            insta::with_settings!({
                sort_maps => true,
                description => "catalog serialization to help catch breaking changes"
            }, {
                let snapshot = catalog.snapshot();
                insta::assert_json_snapshot!(snapshot, {
                    ".catalog_uuid" => "[uuid]"
                });
                // Serialize/deserialize to ensure roundtrip
                let serialized = serialize_catalog_snapshot(&snapshot).unwrap();
                let CatalogFile::Snapshot(snapshot) = verify_and_deserialize_catalog(serialized).unwrap() else {
                    panic!("wrong catalog file type");
                };
                insta::assert_json_snapshot!(snapshot, {
                    ".catalog_uuid" => "[uuid]"
                });
                catalog.update_from_snapshot(snapshot);
                assert_eq!(catalog.db_name_to_id("test_db"), Some(DbId::from(0)));
            });
        }
    }

    #[test_log::test(tokio::test)]
    async fn test_serialize_distinct_cache() {
        let catalog = Catalog::new_in_memory("sample-host-id").await.unwrap();
        catalog.create_database("test_db").await.unwrap();
        catalog
            .create_table(
                "test_db",
                "test_table",
                &["tag_1", "tag_2", "tag_3"],
                &[("field", FieldDataType::String)],
            )
            .await
            .unwrap();
        catalog
            .create_distinct_cache(
                "test_db",
                "test_table",
                Some("test_cache"),
                &["tag_1", "tag_2"],
                MaxCardinality::from_usize_unchecked(100),
                MaxAge::from_secs(10),
            )
            .await
            .unwrap();

        insta::allow_duplicates! {
            insta::with_settings!({
                sort_maps => true,
                description => "catalog serialization to help catch breaking changes"
            }, {
                let snapshot = catalog.snapshot();
                insta::assert_json_snapshot!(snapshot, {
                    ".catalog_uuid" => "[uuid]"
                });
                // Serialize/deserialize to ensure roundtrip
                let serialized = serialize_catalog_snapshot(&snapshot).unwrap();
                let CatalogFile::Snapshot(snapshot) = verify_and_deserialize_catalog(serialized).unwrap() else {
                    panic!("wrong catalog file type");
                };
                insta::assert_json_snapshot!(snapshot, {
                    ".catalog_uuid" => "[uuid]"
                });
                catalog.update_from_snapshot(snapshot);
                assert_eq!(catalog.db_name_to_id("test_db"), Some(DbId::from(0)));
            });
        }
    }

    #[tokio::test]
    async fn test_catalog_id() {
        let catalog = Catalog::new_in_memory("sample-host-id").await.unwrap();
        assert_eq!("sample-host-id", catalog.catalog_id().as_ref());
    }

    /// See: https://github.com/influxdata/influxdb/issues/25524
    #[test_log::test(tokio::test)]
    async fn apply_catalog_batch_fails_for_add_fields_on_nonexist_table() {
        let catalog = Catalog::new_in_memory("host").await.unwrap();
        catalog.create_database("foo").await.unwrap();
        let db_id = catalog.db_name_to_id("foo").unwrap();
        let catalog_batch = create::catalog_batch(
            db_id,
            "foo",
            0,
            [create::add_fields_op(
                db_id,
                "foo",
                TableId::new(0),
                "banana",
                [create::field_def(
                    ColumnId::new(0),
                    "papaya",
                    FieldDataType::String,
                )],
            )],
        );
        let err = catalog
            .get_permit_and_verify_catalog_batch(&catalog_batch)
            .await
            .expect_err("should fail to apply AddFields operation for non-existent table");
        assert_contains!(err.to_string(), "Table banana not in DB schema for foo");
    }

    #[tokio::test]
    async fn test_check_and_mark_table_as_deleted() {
        let catalog = Catalog::new_in_memory("test-catalog").await.unwrap();
        catalog.create_database("test").await.unwrap();
        catalog
            .create_table(
                "test",
                "boo",
                &["tag_1", "tag_2", "tag_3"],
                &[("field", FieldDataType::String)],
            )
            .await
            .unwrap();

        assert!(
            !catalog
                .db_schema("test")
                .unwrap()
                .table_definition("boo")
                .unwrap()
                .deleted
        );

        catalog.soft_delete_table("test", "boo").await.unwrap();

        assert!(
            catalog
                .db_schema("test")
                .unwrap()
                .table_definition("boo-19700101T000000")
                .unwrap()
                .deleted
        );
    }

    // NOTE(trevor/catalog-refactor): this test predates the object-store based catalog, where
    // ordering is still enforced, but it is different. This test mainly verifies that when
    // `OrderedCatalogBatch`s are sorted, they are sorted into the correct order of application.
    // This may still be relevant, but for now am ignoring this test I'm not sure where exactly this
    // is needed; this test is the only piece of code currently that relies on the `PartialOrd`/`Ord`
    // implementations on the `OrderedCatalogBatch` type.
    //
    // Original comment: tests that sorting catalog ops by the sequence number returned from
    // apply_catalog_batch fixes potential ordering issues.
    #[test_log::test(tokio::test)]
    #[ignore]
    async fn test_out_of_order_ops() {
        let catalog = Catalog::new_in_memory("host").await.unwrap();
        let db_id = DbId::new(0);
        let db_name = Arc::from("foo");
        let table_id = TableId::new(0);
        let table_name = Arc::from("bar");
        let table_definition = CreateTableLog {
            database_id: db_id,
            database_name: Arc::clone(&db_name),
            table_name: Arc::clone(&table_name),
            table_id,
            field_definitions: vec![
                FieldDefinition::new(ColumnId::from(0), "tag_1", FieldDataType::Tag),
                FieldDefinition::new(ColumnId::from(1), "time", FieldDataType::Timestamp),
                FieldDefinition::new(ColumnId::from(2), "field", FieldDataType::String),
            ],
            key: vec![ColumnId::from(0)],
        };
        let create_op = CatalogBatch::database(
            0,
            db_id,
            Arc::clone(&db_name),
            vec![DatabaseCatalogOp::CreateTable(table_definition.clone())],
        );
        let add_column_op = CatalogBatch::database(
            0,
            db_id,
            Arc::clone(&db_name),
            vec![DatabaseCatalogOp::AddFields(AddFieldsLog {
                database_name: Arc::clone(&db_name),
                database_id: db_id,
                table_name,
                table_id,
                field_definitions: vec![FieldDefinition::new(
                    ColumnId::from(3),
                    "tag_2",
                    FieldDataType::Tag,
                )],
            })],
        );
        debug!("apply create op");
        let (create_ordered_op, _) = catalog
            .get_permit_and_verify_catalog_batch(&create_op)
            .await
            .expect("get permit and verify create op")
            .expect("should be able to create");
        debug!("apply add column op");
        let (add_column_op, permit) = catalog
            .get_permit_and_verify_catalog_batch(&add_column_op)
            .await
            .expect("verify and permit add column op")
            .expect("should produce operation");
        let mut ordered_batches = vec![add_column_op, create_ordered_op];
        ordered_batches.sort();

        let replayed_catalog = Catalog::new_in_memory("host").await.unwrap();
        debug!(?ordered_batches, "apply sorted ops");
        for ordered_batch in ordered_batches {
            replayed_catalog.apply_ordered_catalog_batch(&ordered_batch, &permit);
        }
        let original_table = catalog
            .db_schema_by_id(&db_id)
            .unwrap()
            .table_definition_by_id(&table_id)
            .unwrap();
        let replayed_table = catalog
            .db_schema_by_id(&db_id)
            .unwrap()
            .table_definition_by_id(&table_id)
            .unwrap();

        assert_eq!(original_table, replayed_table);
    }

    #[test_log::test(tokio::test)]
    async fn deleted_dbs_dont_count() {
        let catalog = Catalog::new_in_memory("test").await.unwrap();

        for i in 0..Catalog::NUM_DBS_LIMIT {
            let db_name = format!("test-db-{i}");
            catalog.create_database(&db_name).await.unwrap();
        }

        // check the count of databases:
        assert_eq!(
            Catalog::NUM_DBS_LIMIT,
            catalog.inner.read().database_count()
        );

        // now create another database, this should NOT be allowed:
        let db_name = "a-database-too-far";
        catalog
            .create_database(db_name)
            .await
            .expect_err("should not be able to create more than permitted number of databases");

        // now delete a database:
        let db_name = format!("test-db-{}", Catalog::NUM_DBS_LIMIT - 1);
        catalog.soft_delete_database(&db_name).await.unwrap();

        // check again, count should have gone down:
        assert_eq!(
            Catalog::NUM_DBS_LIMIT - 1,
            catalog.inner.read().database_count()
        );

        // now create another database (using same name as the deleted one), this should be allowed:
        catalog
            .create_database(&db_name)
            .await
            .expect("can create a database again");

        // check new count:
        assert_eq!(
            Catalog::NUM_DBS_LIMIT,
            catalog.inner.read().database_count()
        );
    }

    #[test_log::test(tokio::test)]
    async fn deleted_tables_dont_count() {
        let catalog = Catalog::new_in_memory("test").await.unwrap();

        let mut txn = catalog.begin("test-db").unwrap();

        // create as many tables as are allowed:
        for i in 0..Catalog::NUM_TABLES_LIMIT {
            let table_name = format!("test-table-{i}");
            txn.table_or_create(&table_name).unwrap();
            txn.column_or_create(&table_name, "field", FieldDataType::String)
                .unwrap();
            txn.column_or_create(&table_name, "time", FieldDataType::Timestamp)
                .unwrap();
        }
        catalog.commit(txn).await.unwrap();

        assert_eq!(
            Catalog::NUM_TABLES_LIMIT,
            catalog.inner.read().table_count()
        );

        // should not be able to create another table:
        let table_name = "a-table-too-far";
        catalog
            .create_table(
                "test-db",
                table_name,
                &["tag"],
                &[("field", FieldDataType::String)],
            )
            .await
            .expect_err("should not be able to exceed table limit");

        catalog
            .soft_delete_table(
                "test-db",
                format!("test-table-{}", Catalog::NUM_TABLES_LIMIT - 1).as_str(),
            )
            .await
            .unwrap();

        assert_eq!(
            Catalog::NUM_TABLES_LIMIT - 1,
            catalog.inner.read().table_count()
        );

        // now create it again, this should be allowed:
        catalog
            .create_table(
                "test-db",
                table_name,
                &["tag"],
                &[("field", FieldDataType::String)],
            )
            .await
            .expect("should be created");

        assert_eq!(
            Catalog::NUM_TABLES_LIMIT,
            catalog.inner.read().table_count()
        );
    }
}
