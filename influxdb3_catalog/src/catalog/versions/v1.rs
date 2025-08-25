//! Implementation of the Catalog that sits entirely in memory.

use bimap::BiHashMap;
use hashbrown::HashMap;
use influxdb3_authz::{CrudActions, DatabaseActions, Permission, ResourceIdentifier, TokenInfo};
use influxdb3_id::{
    CatalogId, ColumnId, DbId, DistinctCacheId, LastCacheId, NodeId, TableId, TokenId, TriggerId,
};
use iox_time::{Time, TimeProvider};
use object_store::ObjectStore;
use observability_deps::tracing::{debug, error, info, trace, warn};
use parking_lot::RwLock;
use schema::{Schema, SchemaBuilder};
use std::borrow::Cow;
use std::collections::BTreeMap;
use std::collections::btree_map::Entry;
use std::iter;
use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;

use crate::catalog::{
    CatalogSequenceNumber, DEFAULT_OPERATOR_TOKEN_NAME, DeletedSchema, INTERNAL_DB_NAME,
    IfNotDeleted, Repository, TIME_COLUMN_NAME, TokenRepository, create_token_and_hash,
    make_new_name_using_deleted_time,
};

mod resource;

pub(crate) mod update;

use schema::sort::SortKey;
pub use schema::{InfluxColumnType, InfluxFieldType};
pub use update::HardDeletionTime;
pub use update::{CreateDatabaseOptions, CreateTableOptions, DatabaseCatalogTransaction, Prompt};

// Specifies the current version of the log used by the catalog
pub use crate::log::versions::v3 as log;
use crate::object_store::versions::v1::ObjectStoreCatalog;
use crate::resource::CatalogResource;
pub(crate) use crate::snapshot::versions::v3 as snapshot;
use crate::{CatalogError, Result};
use log::{
    AddFieldsLog, CatalogBatch, ClearRetentionPeriodLog, CreateAdminTokenDetails,
    CreateDatabaseLog, CreateTableLog, DatabaseBatch, DatabaseCatalogOp, DeleteBatch,
    DeleteDistinctCacheLog, DeleteLastCacheLog, DeleteOp, DeleteTriggerLog,
    DistinctCacheDefinition, FieldDefinition, GenerationBatch, GenerationOp, LastCacheDefinition,
    NodeBatch, NodeCatalogOp, NodeMode, OrderedCatalogBatch, RegenerateAdminTokenDetails,
    RegisterNodeLog, RetentionPeriod, SetRetentionPeriodLog, SoftDeleteDatabaseLog,
    SoftDeleteTableLog, StopNodeLog, TokenBatch, TokenCatalogOp, TriggerDefinition,
    TriggerIdentifier,
};
use snapshot::CatalogSnapshot;

/// Limit for the number of tag columns on a table
pub(crate) const NUM_TAG_COLUMNS_LIMIT: usize = 250;

pub struct Catalog {
    time_provider: Arc<dyn TimeProvider>,
    /// In-memory representation of the catalog
    pub(crate) inner: RwLock<InnerCatalog>,
    limits: CatalogLimits,
    args: CatalogArgs,
}

/// Custom implementation of `Debug` for the `Catalog` type to avoid serializing the object store
impl std::fmt::Debug for Catalog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Catalog")
            .field("inner", &self.inner)
            .finish()
    }
}

#[derive(Clone, Copy, Debug)]
pub struct CatalogArgs {
    pub default_hard_delete_duration: Duration,
}

impl CatalogArgs {
    pub fn new(default_hard_delete_duration: Duration) -> Self {
        Self {
            default_hard_delete_duration,
        }
    }
}

impl Default for CatalogArgs {
    fn default() -> Self {
        Self {
            default_hard_delete_duration: Catalog::DEFAULT_HARD_DELETE_DURATION,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct CatalogLimits {
    num_dbs: usize,
    num_tables: usize,
    num_columns_per_table: usize,
}

impl Default for CatalogLimits {
    fn default() -> Self {
        Self {
            num_dbs: Catalog::NUM_DBS_LIMIT,
            num_tables: Catalog::NUM_TABLES_LIMIT,
            num_columns_per_table: Catalog::NUM_COLUMNS_PER_TABLE_LIMIT,
        }
    }
}

impl Catalog {
    /// Limit for the number of Databases that InfluxDB 3 Core can have
    pub const NUM_DBS_LIMIT: usize = 5;
    /// Limit for the number of columns per table that InfluxDB 3 Core can have
    pub const NUM_COLUMNS_PER_TABLE_LIMIT: usize = 500;
    /// Limit for the number of tables across all DBs that InfluxDB 3 Core can have
    pub const NUM_TABLES_LIMIT: usize = 2000;
    /// Default duration for hard deletion of soft-deleted databases and tables
    pub const DEFAULT_HARD_DELETE_DURATION: Duration = Duration::from_secs(60 * 60 * 72); // 72 hours

    pub async fn new_with_args(
        node_id: impl Into<Arc<str>>,
        store: Arc<dyn ObjectStore>,
        time_provider: Arc<dyn TimeProvider>,
        args: CatalogArgs,
    ) -> Result<Self> {
        let node_id = node_id.into();
        let store = ObjectStoreCatalog::new(Arc::clone(&node_id), store);
        let catalog = store
            .load_or_create_catalog()
            .await
            .map(RwLock::new)
            .map(|inner| Self {
                time_provider,
                inner,
                limits: Default::default(),
                args,
            })?;

        create_internal_db(&catalog).await;

        Ok(catalog)
    }

    fn num_dbs_limit(&self) -> usize {
        self.limits.num_dbs
    }

    fn num_tables_limit(&self) -> usize {
        self.limits.num_tables
    }

    fn num_columns_per_table_limit(&self) -> usize {
        self.limits.num_columns_per_table
    }

    fn default_hard_delete_duration(&self) -> Duration {
        self.args.default_hard_delete_duration
    }

    pub fn catalog_uuid(&self) -> Uuid {
        self.inner.read().catalog_uuid
    }

    pub fn snapshot(&self) -> CatalogSnapshot {
        self.inner.read().snapshot()
    }

    pub fn update_from_snapshot(&self, snapshot: CatalogSnapshot) {
        let mut inner = self.inner.write();
        *inner = InnerCatalog::from_snapshot(snapshot);
    }

    /// Apply an `OrderedCatalogBatch` to this catalog
    ///
    /// # Implementation note
    ///
    /// This accepts a `_permit`, which is not used, and is just a way to ensure that the caller
    /// has a handle on the write permit at the time of invocation.
    pub(crate) fn apply_ordered_catalog_batch(&self, batch: &OrderedCatalogBatch) -> CatalogBatch {
        let batch_sequence = batch.sequence_number().get();
        let current_sequence = self.sequence_number().get();
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
        self.inner.read().nodes.get_by_name(node_id)
    }

    pub(crate) fn db_or_create(
        &self,
        db_name: &str,
        retention_period: Option<Duration>,
        now_time_ns: i64,
    ) -> Result<(Arc<DatabaseSchema>, Option<CatalogBatch>)> {
        match self.db_schema(db_name) {
            Some(db) => Ok((db, None)),
            None => {
                let mut inner = self.inner.write();

                if inner.database_count() >= self.num_dbs_limit() {
                    return Err(CatalogError::TooManyDbs(self.num_dbs_limit()));
                }

                info!(database_name = db_name, "creating new database");
                let db_id = inner.databases.get_and_increment_next_id();
                let db_name = db_name.into();
                let db = Arc::new(DatabaseSchema::new(db_id, Arc::clone(&db_name)));
                let batch = CatalogBatch::database(
                    now_time_ns,
                    db.id,
                    db.name(),
                    vec![DatabaseCatalogOp::CreateDatabase(CreateDatabaseLog {
                        database_id: db.id,
                        database_name: Arc::clone(&db.name),
                        retention_period,
                    })],
                );
                Ok((db, Some(batch)))
            }
        }
    }

    pub fn db_name_to_id(&self, db_name: &str) -> Option<DbId> {
        self.inner.read().databases.name_to_id(db_name)
    }

    pub fn db_schema(&self, db_name: &str) -> Option<Arc<DatabaseSchema>> {
        self.inner.read().databases.get_by_name(db_name)
    }

    pub fn db_schema_by_id(&self, db_id: &DbId) -> Option<Arc<DatabaseSchema>> {
        self.inner.read().databases.get_by_id(db_id)
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

    pub async fn create_admin_token(&self, regenerate: bool) -> Result<(Arc<TokenInfo>, String)> {
        // if regen, if token is present already create a new token and hash and update the
        // existing token otherwise we should insert to catalog (essentially an upsert)
        let (token, hash) = create_token_and_hash();
        self.catalog_update_with_retry(|| {
            if regenerate {
                let default_admin_token = self
                    .inner
                    .read()
                    .tokens
                    .repo()
                    .get_by_name(DEFAULT_OPERATOR_TOKEN_NAME);

                if default_admin_token.is_none() {
                    return Err(CatalogError::MissingAdminTokenToUpdate);
                }

                // now just update the hash and updated at
                Ok(CatalogBatch::Token(TokenBatch {
                    time_ns: self.time_provider.now().timestamp_nanos(),
                    ops: vec![TokenCatalogOp::RegenerateAdminToken(
                        RegenerateAdminTokenDetails {
                            token_id: default_admin_token.unwrap().as_ref().id,
                            hash: hash.clone(),
                            updated_at: self.time_provider.now().timestamp_millis(),
                        },
                    )],
                }))
            } else {
                // validate name
                if self
                    .inner
                    .read()
                    .tokens
                    .repo()
                    .contains_name(DEFAULT_OPERATOR_TOKEN_NAME)
                {
                    return Err(CatalogError::TokenNameAlreadyExists(
                        DEFAULT_OPERATOR_TOKEN_NAME.to_owned(),
                    ));
                }

                let (token_id, created_at, expiry) = {
                    let mut inner = self.inner.write();
                    let token_id = inner.tokens.get_and_increment_next_id();
                    let created_at = self.time_provider.now();
                    let expiry = None;
                    (token_id, created_at.timestamp_millis(), expiry)
                };

                Ok(CatalogBatch::Token(TokenBatch {
                    time_ns: created_at,
                    ops: vec![TokenCatalogOp::CreateAdminToken(CreateAdminTokenDetails {
                        token_id,
                        name: Arc::from(DEFAULT_OPERATOR_TOKEN_NAME),
                        hash: hash.clone(),
                        created_at,
                        updated_at: None,
                        expiry,
                    })],
                }))
            }
        })
        .await?;

        let token_info = {
            self.inner
                .read()
                .tokens
                .repo()
                .get_by_name(DEFAULT_OPERATOR_TOKEN_NAME)
                .expect("token info must be present after token creation by name")
        };

        // we need to pass these details back, especially this token as this is what user should
        // send in subsequent requests
        Ok((token_info, token))
    }

    pub async fn create_named_admin_token_with_permission(
        &self,
        token_name: String,
        expiry_secs: Option<u64>,
    ) -> Result<(Arc<TokenInfo>, String)> {
        let (token, hash) = create_token_and_hash();
        self.catalog_update_with_retry(|| {
            if self.inner.read().tokens.repo().contains_name(&token_name) {
                return Err(CatalogError::TokenNameAlreadyExists(token_name.clone()));
            }

            let (token_id, created_at, expiry) = {
                let mut inner = self.inner.write();
                let token_id = inner.tokens.get_and_increment_next_id();
                let created_at = self.time_provider.now();
                let expiry = expiry_secs.map(|secs| {
                    created_at
                        .checked_add(Duration::from_secs(secs))
                        .expect("duration not to overflow")
                        .timestamp_millis()
                });
                (token_id, created_at.timestamp_millis(), expiry)
            };

            Ok(CatalogBatch::Token(TokenBatch {
                time_ns: created_at,
                ops: vec![TokenCatalogOp::CreateAdminToken(CreateAdminTokenDetails {
                    token_id,
                    name: Arc::from(token_name.as_str()),
                    hash: hash.clone(),
                    created_at,
                    updated_at: None,
                    expiry,
                })],
            }))
        })
        .await?;

        let token_info = {
            self.inner
                .read()
                .tokens
                .repo()
                .get_by_name(&token_name)
                .expect("token info must be present after token creation by name")
        };

        // we need to pass these details back, especially this token as this is what user should
        // send in subsequent requests
        Ok((token_info, token))
    }
}

async fn create_internal_db(catalog: &Catalog) {
    let result = catalog.create_database(INTERNAL_DB_NAME).await;
    // what is the best outcome if "_internal" cannot be created?
    match result {
        Ok(_) => info!("created internal database"),
        Err(err) => {
            match err {
                CatalogError::AlreadyExists => {
                    // this is probably ok
                    debug!("not creating internal db as it exists already");
                }
                _ => {
                    // all other errors are unexpected state
                    error!(?err, "unexpected error when creating internal db");
                    panic!("cannot create internal db");
                }
            }
        }
    };
}

impl Catalog {
    /// Create new `Catalog` that uses an in-memory object store with additional configuration
    /// arguments.
    ///
    /// # Note
    ///
    /// This is intended as a convenience constructor for testing
    pub async fn new_in_memory_with_args(
        catalog_id: impl Into<Arc<str>>,
        time_provider: Arc<dyn TimeProvider>,
        args: CatalogArgs,
    ) -> Result<Self> {
        use object_store::memory::InMemory;

        let store = Arc::new(InMemory::new());
        Self::new_with_args(catalog_id.into(), store, time_provider, args).await
    }
}

#[derive(Debug, Clone)]
pub struct InnerCatalog {
    /// A unique monotonically increasing sequence to differentiate the catalog state as it changes
    /// over time.
    pub(crate) sequence: CatalogSequenceNumber,
    /// The `catalog_id` is the user-provided value used to prefix catalog paths on the object store
    pub(crate) catalog_id: Arc<str>,
    /// The `catalog_uuid` is a unique identifier to distinguish catalog instantiations
    pub(crate) catalog_uuid: Uuid,
    /// Global generation settings to configure the layout of persisted parquet files
    pub(crate) generation_config: GenerationConfig,
    /// Collection of nodes in the catalog
    pub(crate) nodes: Repository<NodeId, NodeDefinition>,
    /// Collection of databases in the catalog
    pub(crate) databases: Repository<DbId, DatabaseSchema>,
    /// Collection of tokens in the catalog
    pub(crate) tokens: TokenRepository,
    /// `true` if the catalog was loaded and an [UpgradedLog][crate::log::UpgradedLog] entry
    /// was found in the catalog log.
    pub(crate) has_upgraded: bool,
}

impl InnerCatalog {
    pub(crate) fn new(catalog_id: Arc<str>, catalog_uuid: Uuid) -> Self {
        Self {
            sequence: CatalogSequenceNumber::new(0),
            catalog_id,
            catalog_uuid,
            nodes: Repository::default(),
            databases: Repository::default(),
            tokens: TokenRepository::default(),
            // TODO(tjh): using default here will result in an empty config; some type state could
            // help us prevent starting a catalog that avoids this case, but we also need to keep
            // backward compatibility so, just defaulting this for now...
            generation_config: Default::default(),
            has_upgraded: false,
        }
    }

    pub fn sequence_number(&self) -> CatalogSequenceNumber {
        self.sequence
    }

    pub fn database_count(&self) -> usize {
        self.databases
            .iter()
            // count if not db deleted _and_ not internal
            .filter(|db| !db.1.deleted && db.1.name().as_ref() != INTERNAL_DB_NAME)
            .count()
    }

    pub fn table_count(&self) -> usize {
        self.databases
            .resource_iter()
            .map(|db| db.table_count())
            .sum()
    }

    /// Verifies _and_ applies the `CatalogBatch` to the catalog.
    pub(crate) fn apply_catalog_batch(
        &mut self,
        catalog_batch: &CatalogBatch,
        sequence: CatalogSequenceNumber,
    ) -> Result<Option<OrderedCatalogBatch>> {
        debug!(
            n_ops = catalog_batch.n_ops(),
            current_sequence = self.sequence_number().get(),
            applied_sequence = sequence.get(),
            "apply catalog batch"
        );
        let updated = match catalog_batch {
            CatalogBatch::Node(root_batch) => self.apply_node_batch(root_batch)?,
            CatalogBatch::Database(database_batch) => self.apply_database_batch(database_batch)?,
            CatalogBatch::Token(token_batch) => self.apply_token_batch(token_batch)?,
            CatalogBatch::Delete(delete_batch) => self.apply_delete_batch(delete_batch)?,
            CatalogBatch::Generation(generation_batch) => {
                self.apply_generation_batch(generation_batch)?
            }
        };

        Ok(updated.then(|| {
            self.sequence = sequence;
            OrderedCatalogBatch::new(catalog_batch.clone(), sequence)
        }))
    }

    fn apply_node_batch(&mut self, node_batch: &NodeBatch) -> Result<bool> {
        let mut updated = false;
        for op in &node_batch.ops {
            updated |= match op {
                NodeCatalogOp::RegisterNode(RegisterNodeLog {
                    node_id,
                    instance_id,
                    registered_time_ns,
                    core_count,
                    mode,
                    ..
                }) => {
                    if let Some(mut node) = self.nodes.get_by_name(node_id) {
                        if &node.instance_id != instance_id {
                            return Err(CatalogError::InvalidNodeRegistration);
                        }
                        let n = Arc::make_mut(&mut node);
                        n.mode = mode.clone();
                        n.core_count = *core_count;
                        n.state = NodeState::Running {
                            registered_time_ns: *registered_time_ns,
                        };
                        self.nodes
                            .update(node_batch.node_catalog_id, node)
                            .expect("existing node should update");
                    } else {
                        let new_node = Arc::new(NodeDefinition {
                            node_id: Arc::clone(node_id),
                            node_catalog_id: node_batch.node_catalog_id,
                            instance_id: Arc::clone(instance_id),
                            mode: mode.clone(),
                            core_count: *core_count,
                            state: NodeState::Running {
                                registered_time_ns: *registered_time_ns,
                            },
                        });
                        self.nodes
                            .insert(node_batch.node_catalog_id, new_node)
                            .expect("there should not already be a node");
                    }
                    true
                }
                NodeCatalogOp::StopNode(StopNodeLog {
                    stopped_time_ns, ..
                }) => {
                    let mut new_node = self
                        .nodes
                        .get_by_id(&node_batch.node_catalog_id)
                        .expect("node should exist");
                    let n = Arc::make_mut(&mut new_node);
                    n.state = NodeState::Stopped {
                        stopped_time_ns: *stopped_time_ns,
                    };
                    self.nodes
                        .update(node_batch.node_catalog_id, new_node)
                        .expect("there should be a node to update");
                    true
                }
            };
        }
        Ok(updated)
    }

    fn apply_token_batch(&mut self, token_batch: &TokenBatch) -> Result<bool> {
        let mut is_updated = false;
        for op in &token_batch.ops {
            is_updated |= match op {
                TokenCatalogOp::CreateAdminToken(create_admin_token_details) => {
                    let mut token_info = TokenInfo::new(
                        create_admin_token_details.token_id,
                        Arc::clone(&create_admin_token_details.name),
                        create_admin_token_details.hash.clone(),
                        create_admin_token_details.created_at,
                        create_admin_token_details.expiry,
                    );

                    token_info.set_permissions(vec![Permission {
                        resource_type: ResourceType::Wildcard,
                        resource_identifier: ResourceIdentifier::Wildcard,
                        actions: Actions::Wildcard,
                    }]);
                    // add the admin token itself
                    self.tokens
                        .add_token(create_admin_token_details.token_id, token_info)?;
                    true
                }
                TokenCatalogOp::RegenerateAdminToken(regenerate_admin_token_details) => {
                    self.tokens.update_admin_token_hash(
                        regenerate_admin_token_details.token_id,
                        regenerate_admin_token_details.hash.clone(),
                        regenerate_admin_token_details.updated_at,
                    )?;
                    true
                }
                TokenCatalogOp::DeleteToken(delete_token_details) => {
                    self.tokens
                        .delete_token(delete_token_details.token_name.to_owned())?;
                    true
                }
            };
        }

        Ok(is_updated)
    }

    fn apply_database_batch(&mut self, database_batch: &DatabaseBatch) -> Result<bool> {
        if let Some(db) = self.databases.get_by_id(&database_batch.database_id) {
            let Some(new_db) = DatabaseSchema::new_if_updated_from_batch(&db, database_batch)?
            else {
                return Ok(false);
            };
            self.databases
                .update(db.id, new_db)
                .expect("existing database should be updated");
        } else {
            let new_db = DatabaseSchema::new_from_batch(database_batch)?;
            self.databases
                .insert(new_db.id, new_db)
                .expect("new database should be inserted");
        };
        Ok(true)
    }

    fn apply_delete_batch(&mut self, delete_batch: &DeleteBatch) -> Result<bool> {
        let mut updated = false;
        for op in &delete_batch.ops {
            match op {
                DeleteOp::DeleteDatabase(db_id) => {
                    // Remove the database from schema
                    if self.databases.get_by_id(db_id).is_some() {
                        self.databases.remove(db_id);
                        updated = true;
                    }
                }
                DeleteOp::DeleteTable(db_id, table_id) => {
                    // Remove the table from the database schema
                    if let Some(mut db_schema) = self.databases.get_by_id(db_id)
                        && db_schema.tables.get_by_id(table_id).is_some()
                    {
                        Arc::make_mut(&mut db_schema).tables.remove(table_id);
                        self.databases.update(*db_id, db_schema)?;
                        updated = true;
                    }
                }
            }
        }
        Ok(updated)
    }

    fn apply_generation_batch(&mut self, generation_batch: &GenerationBatch) -> Result<bool> {
        let mut updated = false;
        for op in &generation_batch.ops {
            match op {
                GenerationOp::SetGenerationDuration(log) => {
                    updated |= self
                        .generation_config
                        .set_duration(log.level, log.duration)?;
                }
            }
        }
        Ok(updated)
    }

    pub fn db_exists(&self, db_id: DbId) -> bool {
        self.databases.get_by_id(&db_id).is_some()
    }
}

#[derive(Debug, Clone, Default)]
pub struct GenerationConfig {
    /// Map of generation levels to their duration
    pub(crate) generation_durations: BTreeMap<u8, Duration>,
}

impl GenerationConfig {
    fn set_duration(&mut self, level: impl Into<u8>, duration: Duration) -> Result<bool> {
        let level = level.into();
        match self.generation_durations.entry(level) {
            Entry::Occupied(occupied_entry) => {
                let existing = *occupied_entry.get();
                if existing != duration {
                    Err(CatalogError::CannotChangeGenerationDuration {
                        level,
                        existing: existing.into(),
                        attempted: duration.into(),
                    })
                } else {
                    Ok(false)
                }
            }
            Entry::Vacant(vacant_entry) => {
                vacant_entry.insert(duration);
                Ok(true)
            }
        }
    }
}

/// The definition of a node in the catalog
#[derive(Debug, Eq, PartialEq, Clone)]
pub struct NodeDefinition {
    /// User-provided, unique name for the node
    ///
    /// # Note
    ///
    /// The naming may be a bit confusing for this. This may be more aptly named `node_name`;
    /// however, it is `node_id`, because this corresponds to the user-provided `--node-id` that is
    /// used to identify the node on server start. The unique and automatically generated catalog
    /// identifier for the node is stored in `node_catalog_id`.
    pub(crate) node_id: Arc<str>,
    /// Unique identifier for the node in the catalog
    pub(crate) node_catalog_id: NodeId,
    /// A UUID generated when the node is first registered into the catalog
    pub(crate) instance_id: Arc<str>,
    /// The mode the node is operating in
    pub(crate) mode: Vec<NodeMode>,
    /// The number of cores this node is using
    pub(crate) core_count: u64,
    /// The state of the node
    pub(crate) state: NodeState,
}

impl NodeDefinition {
    pub fn instance_id(&self) -> Arc<str> {
        Arc::clone(&self.instance_id)
    }

    pub fn node_id(&self) -> Arc<str> {
        Arc::clone(&self.node_id)
    }

    pub fn node_catalog_id(&self) -> NodeId {
        self.node_catalog_id
    }

    pub fn modes(&self) -> &Vec<NodeMode> {
        &self.mode
    }

    pub fn is_running(&self) -> bool {
        match self.state {
            NodeState::Running { .. } => true,
            NodeState::Stopped { .. } => false,
        }
    }

    pub fn core_count(&self) -> u64 {
        self.core_count
    }

    pub fn state(&self) -> NodeState {
        self.state
    }
}

/// The state of a node in an InfluxDB 3 cluster
#[derive(Debug, Eq, PartialEq, Clone, Copy)]
pub enum NodeState {
    /// A node is set to `Running` when first started and registered into the catalog
    Running { registered_time_ns: i64 },
    /// A node is set to `Stopped` during graceful shutdown
    Stopped { stopped_time_ns: i64 },
}

impl NodeState {
    pub fn as_str(&self) -> &'static str {
        match self {
            NodeState::Running { .. } => "running",
            NodeState::Stopped { .. } => "stopped",
        }
    }

    pub fn updated_at_ns(&self) -> i64 {
        match self {
            NodeState::Running { registered_time_ns } => *registered_time_ns,
            NodeState::Stopped { stopped_time_ns } => *stopped_time_ns,
        }
    }
}

/// Definition of a database in the catalog
#[derive(Debug, Eq, PartialEq, Clone)]
pub struct DatabaseSchema {
    /// Unique identifier for the database
    pub id: DbId,
    /// Unique user-provided name for the database
    pub name: Arc<str>,
    /// Tables contained in the database
    pub tables: Repository<TableId, TableDefinition>,
    /// Retention period for the database
    pub retention_period: RetentionPeriod,
    /// Processing engine triggers configured on the database
    pub processing_engine_triggers: Repository<TriggerId, TriggerDefinition>,
    /// Whether this database has been flagged as deleted
    pub deleted: bool,
    /// The time when the database is scheduled to be hard deleted.
    pub hard_delete_time: Option<Time>,
}

impl DatabaseSchema {
    pub fn new(id: DbId, name: Arc<str>) -> Self {
        Self {
            id,
            name,
            tables: Repository::new(),
            retention_period: RetentionPeriod::Indefinite,
            processing_engine_triggers: Repository::new(),
            deleted: false,
            hard_delete_time: None,
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
        trace!(
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
    pub(crate) fn create_new_empty_table(
        &mut self,
        table_name: impl Into<Arc<str>>,
    ) -> Result<Arc<TableDefinition>> {
        let table_id = self.tables.get_and_increment_next_id();
        let table_def = Arc::new(TableDefinition::new_empty(table_id, table_name.into()));
        self.tables.insert(table_id, Arc::clone(&table_def))?;
        Ok(table_def)
    }

    /// Update a table in the database. This fails if the table doesn't exist.
    pub(crate) fn update_table(
        &mut self,
        table_id: TableId,
        table_def: Arc<TableDefinition>,
    ) -> Result<()> {
        self.tables.update(table_id, table_def)
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
        self.tables
            .insert(table_id, table_def)
            .expect("table inserted from the log should not already exist");
    }

    pub fn table_definition(&self, table_name: impl AsRef<str>) -> Option<Arc<TableDefinition>> {
        self.tables.get_by_name(table_name.as_ref())
    }

    pub fn table_definition_by_id(&self, table_id: &TableId) -> Option<Arc<TableDefinition>> {
        self.tables.get_by_id(table_id)
    }

    pub fn table_name_to_id(&self, table_name: impl AsRef<str>) -> Option<TableId> {
        self.tables.name_to_id(table_name.as_ref())
    }
}

impl DeletedSchema for DatabaseSchema {
    fn is_deleted(&self) -> bool {
        self.deleted
    }
}

impl IfNotDeleted for DatabaseSchema {
    type T = Self;

    fn if_not_deleted(self) -> Option<Self::T> {
        (!self.deleted).then_some(self)
    }
}

impl DeletedSchema for TableDefinition {
    fn is_deleted(&self) -> bool {
        self.deleted
    }
}

impl IfNotDeleted for TableDefinition {
    type T = Self;

    fn if_not_deleted(self) -> Option<Self::T> {
        (!self.deleted).then_some(self)
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
                    panic!(
                        "Create database call received by a mismatched DatabaseSchema. This should not be possible."
                    )
                }
                schema.to_mut().retention_period = match create_database.retention_period {
                    Some(duration) => RetentionPeriod::Duration(duration),
                    None => RetentionPeriod::Indefinite,
                };

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
            DatabaseCatalogOp::SetRetentionPeriod(update) => update.update_schema(schema),
            DatabaseCatalogOp::ClearRetentionPeriod(update) => update.update_schema(schema),
        }
    }
}

impl UpdateDatabaseSchema for CreateTableLog {
    fn update_schema<'a>(
        &self,
        mut database_schema: Cow<'a, DatabaseSchema>,
    ) -> Result<Cow<'a, DatabaseSchema>> {
        match database_schema.tables.get_by_id(&self.table_id) {
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
        let owned = schema.to_mut();
        // If it isn't already deleted, then we must generate a "deleted" name for the schema,
        // based on the deletion_time
        if !owned.deleted {
            let deletion_time = Time::from_timestamp_nanos(self.deletion_time);
            owned.name = make_new_name_using_deleted_time(&self.database_name, deletion_time);
            owned.deleted = true;
        }
        owned.hard_delete_time = self.hard_deletion_time.map(Time::from_timestamp_nanos);
        Ok(schema)
    }
}

impl UpdateDatabaseSchema for SoftDeleteTableLog {
    fn update_schema<'a>(
        &self,
        mut schema: Cow<'a, DatabaseSchema>,
    ) -> Result<Cow<'a, DatabaseSchema>> {
        // unlike other table ops, this is not an error.
        if !schema.tables.contains_id(&self.table_id) {
            return Ok(schema);
        }
        let mut_schema = schema.to_mut();
        if let Some(mut deleted_table) = mut_schema.tables.get_by_id(&self.table_id) {
            let new_table_def = Arc::make_mut(&mut deleted_table);
            // If it isn't already deleted, then we must generate a "deleted" name for the schema,
            // based on the deletion_time
            if !new_table_def.deleted {
                let deletion_time = Time::from_timestamp_nanos(self.deletion_time);
                let table_name = make_new_name_using_deleted_time(&self.table_name, deletion_time);
                new_table_def.deleted = true;
                new_table_def.table_name = table_name;
            }
            new_table_def.hard_delete_time =
                self.hard_deletion_time.map(Time::from_timestamp_nanos);
            mut_schema
                .tables
                .update(new_table_def.table_id, deleted_table)
                .expect("the table should exist");
        }
        Ok(schema)
    }
}

impl UpdateDatabaseSchema for SetRetentionPeriodLog {
    fn update_schema<'a>(
        &self,
        mut schema: Cow<'a, DatabaseSchema>,
    ) -> Result<Cow<'a, DatabaseSchema>> {
        let mut_schema = schema.to_mut();
        mut_schema.retention_period = self.retention_period;
        Ok(schema)
    }
}

impl UpdateDatabaseSchema for ClearRetentionPeriodLog {
    fn update_schema<'a>(
        &self,
        mut schema: Cow<'a, DatabaseSchema>,
    ) -> Result<Cow<'a, DatabaseSchema>> {
        let mut_schema = schema.to_mut();
        mut_schema.retention_period = RetentionPeriod::Indefinite;
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
        let Some(trigger) = schema
            .processing_engine_triggers
            .get_by_name(&self.0.trigger_name)
        else {
            return Err(CatalogError::ProcessingEngineTriggerNotFound {
                database_name: self.0.db_name.to_string(),
                trigger_name: self.0.trigger_name.to_string(),
            });
        };
        if !trigger.disabled {
            return Ok(schema);
        }
        let mut mut_trigger = schema
            .processing_engine_triggers
            .get_by_id(&trigger.trigger_id)
            .expect("already checked containment");
        Arc::make_mut(&mut mut_trigger).disabled = false;
        schema
            .to_mut()
            .processing_engine_triggers
            .update(trigger.trigger_id, mut_trigger)
            .expect("existing trigger should update");
        Ok(schema)
    }
}

impl UpdateDatabaseSchema for DisableTrigger {
    fn update_schema<'a>(
        &self,
        mut schema: Cow<'a, DatabaseSchema>,
    ) -> Result<Cow<'a, DatabaseSchema>> {
        let Some(trigger) = schema
            .processing_engine_triggers
            .get_by_name(&self.0.trigger_name)
        else {
            return Err(CatalogError::ProcessingEngineTriggerNotFound {
                database_name: self.0.db_name.to_string(),
                trigger_name: self.0.trigger_name.to_string(),
            });
        };
        if trigger.disabled {
            return Ok(schema);
        }
        let mut mut_trigger = schema
            .processing_engine_triggers
            .get_by_id(&trigger.trigger_id)
            .expect("already checked containment");
        Arc::make_mut(&mut mut_trigger).disabled = true;
        schema
            .to_mut()
            .processing_engine_triggers
            .update(trigger.trigger_id, mut_trigger)
            .expect("existing trigger should update");
        Ok(schema)
    }
}

impl UpdateDatabaseSchema for TriggerDefinition {
    fn update_schema<'a>(
        &self,
        mut schema: Cow<'a, DatabaseSchema>,
    ) -> Result<Cow<'a, DatabaseSchema>> {
        if let Some(current) = schema
            .processing_engine_triggers
            .get_by_name(&self.trigger_name)
        {
            if current.as_ref() == self {
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
            .insert(self.trigger_id, Arc::new(self.clone()))
            .expect("new trigger should insert");
        Ok(schema)
    }
}

impl UpdateDatabaseSchema for DeleteTriggerLog {
    fn update_schema<'a>(
        &self,
        mut schema: Cow<'a, DatabaseSchema>,
    ) -> Result<Cow<'a, DatabaseSchema>> {
        let Some(trigger) = schema
            .processing_engine_triggers
            .get_by_name(&self.trigger_name)
        else {
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
            .remove(&trigger.trigger_id);

        Ok(schema)
    }
}

/// Definition of a table in the catalog
#[derive(Debug, Eq, PartialEq, Clone)]
pub struct TableDefinition {
    /// Unique identifier of the table in the catalog
    pub table_id: TableId,
    /// User-provided unique name for the table
    pub table_name: Arc<str>,
    /// The IOx/Arrow schema for the table
    pub schema: Schema,
    /// Column definitions for the table
    pub columns: Repository<ColumnId, ColumnDefinition>,
    /// List of column identifiers that form the series key for the table
    ///
    /// The series key along with the `time` column form the primary key for the table. The series
    /// key is determined as the order of tags provided when the table is first created, either by
    /// a write of line protocol, or by an explicit table creation.
    ///
    /// The series key is used as the sort order, i.e., sort key, for the table during persistence.
    pub series_key: Vec<ColumnId>,
    /// The names of the columns in the table's series key
    pub series_key_names: Vec<Arc<str>>,
    /// The series key is the ordered list of tags that uniquely identify a series. This map
    /// provides the position of each tag in the series key, which can be used as an identifier
    /// in PachaTree
    pub(crate) tag_column_name_to_position_id: HashMap<Arc<str>, u8>,
    /// The sort key for the table when persisted to storage.
    pub sort_key: SortKey,
    /// Last cache definitions for the table
    pub last_caches: Repository<LastCacheId, LastCacheDefinition>,
    /// Distinct cache definitions for the table
    pub distinct_caches: Repository<DistinctCacheId, DistinctCacheDefinition>,
    /// Whether this table has been set as deleted
    pub deleted: bool,
    /// The time when the table is scheduled to be hard deleted.
    pub hard_delete_time: Option<Time>,
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
        // Use a BTree to ensure that the columns are ordered:
        let mut ordered_columns = BTreeMap::new();
        for (col_id, name, column_type) in &columns {
            ordered_columns.insert(name.as_ref(), (col_id, column_type));
        }
        let mut schema_builder = SchemaBuilder::with_capacity(columns.len());
        schema_builder.measurement(table_name.as_ref());
        let mut columns = Repository::new();
        for (name, (col_id, column_type)) in ordered_columns {
            schema_builder.influx_column(name, *column_type);
            let not_nullable = matches!(column_type, InfluxColumnType::Timestamp);
            assert!(
                columns
                    .insert(
                        *col_id,
                        Arc::new(ColumnDefinition::new(
                            *col_id,
                            name,
                            *column_type,
                            !not_nullable
                        )),
                    )
                    .is_ok(),
                "table definition initialized with duplicate column ids"
            );
        }
        let series_key_names = series_key
            .clone()
            .into_iter()
            .map(|id| {
                columns
                    .id_to_name(&id)
                    // NOTE: should this be an error instead of panic?
                    .expect("invalid column id in series key definition")
            })
            .collect::<Vec<Arc<str>>>();
        schema_builder.with_series_key(&series_key_names);
        let schema = schema_builder.build().expect("schema should be valid");

        let sort_key =
            Self::make_sort_key(&series_key_names, columns.contains_name(TIME_COLUMN_NAME));

        // build the tag column name to position id map
        let tag_column_name_to_position_id = series_key_names
            .iter()
            .enumerate()
            .map(|(pos, name)| (Arc::clone(name), pos as u8))
            .collect();

        Ok(Self {
            table_id,
            table_name,
            schema,
            columns,
            series_key,
            series_key_names,
            tag_column_name_to_position_id,
            sort_key,
            last_caches: Repository::new(),
            distinct_caches: Repository::new(),
            deleted: false,
            hard_delete_time: None,
        })
    }

    fn make_sort_key(series_key_names: &[Arc<str>], add_time: bool) -> SortKey {
        let iter = series_key_names.iter().cloned();
        if add_time {
            SortKey::from_columns(iter.chain(iter::once(TIME_COLUMN_NAME.into())))
        } else {
            SortKey::from_columns(iter)
        }
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
            if let Some(existing_type) = table
                .columns
                .get_by_id(&field_def.id)
                .map(|def| def.data_type)
            {
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
            table.add_columns(new_fields)?;
        }
        Ok(table)
    }

    pub(crate) fn add_column(
        &mut self,
        column_name: Arc<str>,
        column_type: InfluxColumnType,
    ) -> Result<ColumnId> {
        let col_id = self.columns.get_and_increment_next_id();
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
        for col_def in self.columns.resource_iter().cloned() {
            cols.insert(Arc::clone(&col_def.name), col_def);
        }

        let mut sort_key_changed = false;

        for (id, name, column_type) in columns {
            let nullable = name.as_ref() != TIME_COLUMN_NAME;
            assert!(
                cols.insert(
                    Arc::clone(&name),
                    Arc::new(ColumnDefinition::new(
                        id,
                        Arc::clone(&name),
                        column_type,
                        nullable
                    ))
                )
                .is_none(),
                "attempted to add existing column"
            );
            // add new tags to the series key in the order provided
            if matches!(column_type, InfluxColumnType::Tag) && !self.series_key.contains(&id) {
                self.tag_column_name_to_position_id
                    .insert(Arc::clone(&name), self.series_key.len() as u8);
                self.series_key.push(id);
                self.series_key_names.push(name);
                sort_key_changed = true;
            } else if matches!(column_type, InfluxColumnType::Timestamp) {
                sort_key_changed = true;
            }
        }

        let mut schema_builder = SchemaBuilder::with_capacity(cols.len());
        schema_builder.measurement(self.table_name.as_ref());
        for (name, col_def) in &cols {
            schema_builder.influx_column(name.as_ref(), col_def.data_type);
        }
        schema_builder.with_series_key(&self.series_key_names);
        let schema = schema_builder.build().expect("schema should be valid");
        self.schema = schema;

        let mut new_columns = Repository::new();
        for col in cols.values().cloned() {
            new_columns
                .insert(col.id, col)
                .expect("should be a new column");
        }
        self.columns = new_columns;

        if sort_key_changed {
            self.sort_key = Self::make_sort_key(
                &self.series_key_names,
                self.columns.contains_name(TIME_COLUMN_NAME),
            );
        }

        Ok(())
    }

    pub fn influx_schema(&self) -> &Schema {
        &self.schema
    }

    pub fn num_columns(&self) -> usize {
        self.influx_schema().len()
    }

    pub fn num_tag_columns(&self) -> usize {
        self.columns
            .resource_iter()
            .filter(|c| matches!(c.data_type, InfluxColumnType::Tag))
            .count()
    }

    pub fn field_type_by_name(&self, name: impl AsRef<str>) -> Option<InfluxColumnType> {
        self.columns
            .get_by_name(name.as_ref())
            .map(|def| def.data_type)
    }

    // TODO(trevor): remove thid API in favour of the Repository APIs
    pub fn column_name_to_id(&self, name: impl AsRef<str>) -> Option<ColumnId> {
        self.columns.name_to_id(name.as_ref())
    }

    // TODO(trevor): remove thid API in favour of the Repository APIs
    pub fn column_id_to_name(&self, id: &ColumnId) -> Option<Arc<str>> {
        self.columns.id_to_name(id)
    }

    // TODO(trevor): remove thid API in favour of the Repository APIs
    pub fn column_definition(&self, name: impl AsRef<str>) -> Option<Arc<ColumnDefinition>> {
        self.columns.get_by_name(name.as_ref())
    }

    // TODO(trevor): remove thid API in favour of the Repository APIs
    pub fn column_definition_by_id(&self, id: &ColumnId) -> Option<Arc<ColumnDefinition>> {
        self.columns.get_by_id(id)
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
        let Some(table) = schema.tables.get_by_id(&self.table_id()) else {
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

impl TableUpdate for DistinctCacheDefinition {
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
        table
            .to_mut()
            .distinct_caches
            .insert(self.cache_id, self.clone())?;
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
        table.to_mut().distinct_caches.remove(&self.cache_id);
        Ok(table)
    }
}

impl TableUpdate for LastCacheDefinition {
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
        table.to_mut().last_caches.insert(self.id, self.clone())?;
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
        table.to_mut().last_caches.remove(&self.id);
        Ok(table)
    }
}

/// Definition of a column in the catalog
#[derive(Debug, Eq, PartialEq, Clone)]
pub struct ColumnDefinition {
    /// Unique identifier of the column in the catalog
    pub id: ColumnId,
    /// User-provided unique name for the column
    pub name: Arc<str>,
    /// Influx type of the column
    pub data_type: InfluxColumnType,
    /// Whether this column can hold `NULL` values
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

use crate::log::versions::v3::{LastCacheTtl, LastCacheValueColumnsDef};
use crate::snapshot::versions::v3::{
    ColumnDefinitionSnapshot, DatabaseSnapshot, DistinctCacheSnapshot, GenerationConfigSnapshot,
    InfluxType, LastCacheSnapshot, NodeSnapshot, ProcessingEngineTriggerSnapshot,
    RetentionPeriodSnapshot, TableSnapshot,
};

/// This trait is implemented only for the most recent snapshot version. The reasoning is explained
/// below
///
/// When snapshots are loaded following is the sequence
///   - load snapshot file from object store
///   - run through conversion function to convert it to $latest:CatalogSnapshot
///   - call InnerCatalog::from_snapshot (from this trait) to get to latest in memory types
///     (InnerCatalog et. al)
///
/// When snapshots are written back following is the sequence
///   - call InnerCatalog::snapshot to get $latest:CatalogSnapshot
///   - write CatalogSnapshot to object store
///
/// As you can see the method from_snapshot is called after the conversion function is called so
/// any version migration would have to happen in conversion functions to get
/// $latest:CatalogSnapshot. Usually anything that is populated on the fly is implemented here, i.e
/// types that need to be populated only for in-memory representation (eg. bihashmaps, token
/// permissions map etc)
///
/// When writing another migration the snapshot types (CatalogSnapshot and others referenced by it)
/// should come from snapshot::versions::$latest.rs and then impls go here as this is expected to
/// only be implemented for going back and forth between InnerCatalog and $latest:CatalogSnapshot
pub(crate) trait Snapshot {
    type Serialized;

    fn snapshot(&self) -> Self::Serialized;
    fn from_snapshot(snap: Self::Serialized) -> Self;
}

impl<I, R> Snapshot for Repository<I, R>
where
    I: CatalogId,
    R: Snapshot + CatalogResource,
{
    type Serialized = RepositorySnapshot<I, R::Serialized>;

    fn snapshot(&self) -> Self::Serialized {
        Self::Serialized {
            repo: self
                .repo
                .iter()
                .map(|(id, res)| (*id, res.snapshot()))
                .collect(),
            next_id: self.next_id,
        }
    }

    fn from_snapshot(snap: Self::Serialized) -> Self {
        let mut repo = Self::new();
        for (id, res) in snap.repo {
            repo.insert(id, Arc::new(R::from_snapshot(res)))
                .expect("catalog should contain no duplicates");
        }
        Self {
            id_name_map: repo.id_name_map,
            repo: repo.repo,
            next_id: snap.next_id,
        }
    }
}

impl Snapshot for InnerCatalog {
    type Serialized = CatalogSnapshot;

    fn snapshot(&self) -> Self::Serialized {
        Self::Serialized {
            nodes: self.nodes.snapshot(),
            databases: self.databases.snapshot(),
            tokens: self.tokens.repo().snapshot(),
            sequence: self.sequence,
            catalog_id: Arc::clone(&self.catalog_id),
            catalog_uuid: self.catalog_uuid,
            generation_config: self.generation_config.snapshot(),
        }
    }

    fn from_snapshot(snap: Self::Serialized) -> Self {
        let repository: Repository<TokenId, TokenInfo> = Repository::from_snapshot(snap.tokens);
        let mut hash_lookup_map = BiHashMap::new();
        repository.repo.iter().for_each(|(id, info)| {
            // this clone should maybe be switched to arc?
            hash_lookup_map.insert(*id, info.hash.clone());
        });

        let token_info_repo = TokenRepository::new(repository, hash_lookup_map);
        Self {
            sequence: snap.sequence,
            catalog_id: snap.catalog_id,
            catalog_uuid: snap.catalog_uuid,
            nodes: Repository::from_snapshot(snap.nodes),
            databases: Repository::from_snapshot(snap.databases),
            tokens: token_info_repo,
            generation_config: GenerationConfig::from_snapshot(snap.generation_config),
            has_upgraded: false,
        }
    }
}

impl Snapshot for GenerationConfig {
    type Serialized = GenerationConfigSnapshot;

    fn snapshot(&self) -> Self::Serialized {
        Self::Serialized {
            generation_durations: self
                .generation_durations
                .iter()
                .map(|(level, duration)| (*level, *duration))
                .collect(),
        }
    }

    fn from_snapshot(snap: Self::Serialized) -> Self {
        Self {
            generation_durations: snap.generation_durations.into_iter().collect(),
        }
    }
}

impl Snapshot for NodeDefinition {
    type Serialized = NodeSnapshot;

    fn snapshot(&self) -> Self::Serialized {
        Self::Serialized {
            node_id: Arc::clone(&self.node_id),
            node_catalog_id: self.node_catalog_id,
            instance_id: Arc::clone(&self.instance_id),
            mode: self.mode.clone(),
            state: self.state.snapshot(),
            core_count: self.core_count,
        }
    }

    fn from_snapshot(snap: Self::Serialized) -> Self {
        Self {
            node_id: snap.node_id,
            node_catalog_id: snap.node_catalog_id,
            instance_id: snap.instance_id,
            mode: snap.mode,
            core_count: snap.core_count,
            state: NodeState::from_snapshot(snap.state),
        }
    }
}

impl Snapshot for DatabaseSchema {
    type Serialized = DatabaseSnapshot;

    fn snapshot(&self) -> Self::Serialized {
        Self::Serialized {
            id: self.id,
            name: Arc::clone(&self.name),
            tables: self.tables.snapshot(),
            retention_period: Some(self.retention_period.snapshot()),
            processing_engine_triggers: self.processing_engine_triggers.snapshot(),
            deleted: self.deleted,
            hard_delete_time: self.hard_delete_time.as_ref().map(Time::timestamp_nanos),
        }
    }

    fn from_snapshot(snap: Self::Serialized) -> Self {
        Self {
            id: snap.id,
            name: snap.name,
            tables: Repository::from_snapshot(snap.tables),
            retention_period: snap
                .retention_period
                .map(Snapshot::from_snapshot)
                .unwrap_or(RetentionPeriod::Indefinite),
            processing_engine_triggers: Repository::from_snapshot(snap.processing_engine_triggers),
            deleted: snap.deleted,
            hard_delete_time: snap.hard_delete_time.map(Time::from_timestamp_nanos),
        }
    }
}

/// A snapshot of a [`TableDefinition`] used for serialization of table information from the
/// catalog.
///
/// This is used over serde's `Serialize`/`Deserialize` implementations on the inner `Schema` type
/// due to them being considered unstable. This type intends to mimic the structure of the Arrow
/// `Schema`, and will help guard against potential breaking changes to the Arrow Schema types.
impl Snapshot for TableDefinition {
    type Serialized = TableSnapshot;

    fn snapshot(&self) -> Self::Serialized {
        Self::Serialized {
            table_id: self.table_id,
            table_name: Arc::clone(&self.table_name),
            key: self.series_key.clone(),
            columns: self.columns.snapshot(),
            last_caches: self.last_caches.snapshot(),
            distinct_caches: self.distinct_caches.snapshot(),
            deleted: self.deleted,
            hard_delete_time: self.hard_delete_time.as_ref().map(Time::timestamp_nanos),
        }
    }

    fn from_snapshot(snap: Self::Serialized) -> Self {
        let table_id = snap.table_id;
        // use the TableDefinition constructor here since it handles
        // Schema construction:
        let mut table_def = Self::new(
            table_id,
            snap.table_name,
            snap.columns
                .repo
                .into_iter()
                .map(|(id, def)| {
                    (
                        id,
                        def.name,
                        match def.influx_type {
                            InfluxType::Tag => InfluxColumnType::Tag,
                            InfluxType::Field => {
                                InfluxColumnType::Field(InfluxFieldType::from(def.r#type))
                            }
                            InfluxType::Time => InfluxColumnType::Timestamp,
                        },
                    )
                })
                .collect(),
            snap.key,
        )
        .expect("serialized table definition from catalog should be valid");
        // ensure next col id is set from the snapshot incase we ever allow
        // hard-deletes:
        table_def.columns.set_next_id(snap.columns.next_id);

        // build the tag column name to position id map
        let tag_column_name_to_position_id = table_def
            .series_key_names
            .iter()
            .enumerate()
            .map(|(pos, name)| (Arc::clone(name), pos as u8))
            .collect();

        Self {
            table_id,
            table_name: table_def.table_name,
            schema: table_def.schema,
            columns: table_def.columns,
            series_key: table_def.series_key,
            series_key_names: table_def.series_key_names,
            tag_column_name_to_position_id,
            sort_key: table_def.sort_key,
            last_caches: Repository::from_snapshot(snap.last_caches),
            distinct_caches: Repository::from_snapshot(snap.distinct_caches),
            deleted: snap.deleted,
            hard_delete_time: snap.hard_delete_time.map(Time::from_timestamp_nanos),
        }
    }
}

impl Snapshot for ColumnDefinition {
    type Serialized = ColumnDefinitionSnapshot;

    fn snapshot(&self) -> Self::Serialized {
        Self::Serialized {
            name: Arc::clone(&self.name),
            id: self.id,
            r#type: self.data_type.into(),
            influx_type: self.data_type.into(),
            nullable: self.nullable,
        }
    }

    fn from_snapshot(snap: Self::Serialized) -> Self {
        Self {
            id: snap.id,
            name: snap.name,
            data_type: match snap.influx_type {
                InfluxType::Tag => InfluxColumnType::Tag,
                InfluxType::Field => InfluxColumnType::Field(InfluxFieldType::from(&snap.r#type)),
                InfluxType::Time => InfluxColumnType::Timestamp,
            },
            nullable: snap.nullable,
        }
    }
}

impl Snapshot for RetentionPeriod {
    type Serialized = RetentionPeriodSnapshot;

    fn snapshot(&self) -> Self::Serialized {
        match self {
            Self::Indefinite => RetentionPeriodSnapshot::Indefinite,
            Self::Duration(d) => RetentionPeriodSnapshot::Duration(*d),
        }
    }

    fn from_snapshot(snap: Self::Serialized) -> Self {
        match snap {
            RetentionPeriodSnapshot::Indefinite => Self::Indefinite,
            RetentionPeriodSnapshot::Duration(d) => Self::Duration(d),
        }
    }
}

impl Snapshot for TriggerDefinition {
    type Serialized = ProcessingEngineTriggerSnapshot;

    fn snapshot(&self) -> Self::Serialized {
        ProcessingEngineTriggerSnapshot {
            trigger_id: self.trigger_id,
            trigger_name: Arc::clone(&self.trigger_name),
            node_id: Arc::clone(&self.node_id),
            plugin_filename: self.plugin_filename.clone(),
            database_name: Arc::clone(&self.database_name),
            trigger_specification: self.trigger.clone(),
            trigger_settings: self.trigger_settings,
            trigger_arguments: self.trigger_arguments.clone(),
            disabled: self.disabled,
        }
    }

    fn from_snapshot(snap: Self::Serialized) -> Self {
        Self {
            trigger_id: snap.trigger_id,
            trigger_name: snap.trigger_name,
            node_id: snap.node_id,
            plugin_filename: snap.plugin_filename,
            database_name: snap.database_name,
            trigger: snap.trigger_specification,
            trigger_settings: snap.trigger_settings,
            trigger_arguments: snap.trigger_arguments,
            disabled: snap.disabled,
        }
    }
}

impl Snapshot for LastCacheDefinition {
    type Serialized = LastCacheSnapshot;

    fn snapshot(&self) -> Self::Serialized {
        LastCacheSnapshot {
            table_id: self.table_id,
            table: Arc::clone(&self.table),
            id: self.id,
            name: Arc::clone(&self.name),
            keys: self.key_columns.to_vec(),
            vals: match &self.value_columns {
                LastCacheValueColumnsDef::Explicit { columns } => Some(columns.to_vec()),
                LastCacheValueColumnsDef::AllNonKeyColumns => None,
            },
            n: self.count.into(),
            ttl: self.ttl.as_secs(),
        }
    }

    fn from_snapshot(snap: Self::Serialized) -> Self {
        Self {
            table_id: snap.table_id,
            table: snap.table,
            id: snap.id,
            name: snap.name,
            key_columns: snap.keys,
            value_columns: snap.vals.map_or_else(Default::default, |columns| {
                LastCacheValueColumnsDef::Explicit { columns }
            }),
            count: snap
                .n
                .try_into()
                .expect("catalog contains invalid last cache size"),
            ttl: LastCacheTtl::from_secs(snap.ttl),
        }
    }
}

impl Snapshot for DistinctCacheDefinition {
    type Serialized = DistinctCacheSnapshot;

    fn snapshot(&self) -> Self::Serialized {
        DistinctCacheSnapshot {
            table_id: self.table_id,
            table: Arc::clone(&self.table_name),
            id: self.cache_id,
            name: Arc::clone(&self.cache_name),
            cols: self.column_ids.clone(),
            max_cardinality: self.max_cardinality,
            max_age_seconds: self.max_age_seconds,
        }
    }

    fn from_snapshot(snap: Self::Serialized) -> Self {
        Self {
            table_id: snap.table_id,
            table_name: snap.table,
            cache_id: snap.id,
            cache_name: snap.name,
            column_ids: snap.cols,
            max_cardinality: snap.max_cardinality,
            max_age_seconds: snap.max_age_seconds,
        }
    }
}

use influxdb3_authz::{Actions, ResourceType};
use snapshot::{
    ActionsSnapshot, CrudActionsSnapshot, DatabaseActionsSnapshot, NodeStateSnapshot,
    PermissionSnapshot, RepositorySnapshot, ResourceIdentifierSnapshot, ResourceTypeSnapshot,
    TokenInfoSnapshot,
};

impl Snapshot for TokenInfo {
    type Serialized = TokenInfoSnapshot;

    fn snapshot(&self) -> Self::Serialized {
        Self::Serialized {
            id: self.id,
            name: Arc::clone(&self.name),
            hash: self.hash.clone(),
            created_at: self.created_at,
            expiry: self.expiry_millis,
            created_by: self.created_by,
            updated_at: self.updated_at,
            updated_by: self.updated_by,
            description: self.description.clone(),
            permissions: self
                .permissions
                .iter()
                .map(|perm| perm.snapshot())
                .collect(),
        }
    }

    fn from_snapshot(snap: Self::Serialized) -> Self {
        Self {
            id: snap.id,
            name: snap.name,
            hash: snap.hash,
            created_at: snap.created_at,
            expiry_millis: snap.expiry,
            created_by: snap.created_by,
            updated_by: snap.updated_by,
            updated_at: snap.updated_at,
            permissions: snap
                .permissions
                .into_iter()
                .map(Permission::from_snapshot)
                .collect(),
            description: snap.description,
        }
    }
}

impl Snapshot for Permission {
    type Serialized = PermissionSnapshot;

    fn snapshot(&self) -> Self::Serialized {
        PermissionSnapshot {
            resource_type: self.resource_type.snapshot(),
            resource_identifier: self.resource_identifier.snapshot(),
            actions: self.actions.snapshot(),
        }
    }

    fn from_snapshot(snap: Self::Serialized) -> Self {
        Self {
            resource_type: ResourceType::from_snapshot(snap.resource_type),
            resource_identifier: ResourceIdentifier::from_snapshot(snap.resource_identifier),
            actions: Actions::from_snapshot(snap.actions),
        }
    }
}

impl Snapshot for Actions {
    type Serialized = ActionsSnapshot;

    fn snapshot(&self) -> Self::Serialized {
        match self {
            Actions::Database(database_actions) => {
                ActionsSnapshot::Database(database_actions.snapshot())
            }
            Actions::Token(crud_actions) => ActionsSnapshot::Token(crud_actions.snapshot()),
            Actions::Wildcard => ActionsSnapshot::Wildcard,
        }
    }

    fn from_snapshot(snap: Self::Serialized) -> Self {
        match snap {
            ActionsSnapshot::Database(db_actions) => {
                Actions::Database(DatabaseActions::from_snapshot(db_actions))
            }
            ActionsSnapshot::Token(crud_actions) => {
                Actions::Token(CrudActions::from_snapshot(crud_actions))
            }
            ActionsSnapshot::Wildcard => Actions::Wildcard,
        }
    }
}

impl Snapshot for ResourceType {
    type Serialized = ResourceTypeSnapshot;

    fn snapshot(&self) -> Self::Serialized {
        match self {
            ResourceType::Database => ResourceTypeSnapshot::Database,
            ResourceType::Token => ResourceTypeSnapshot::Token,
            ResourceType::Wildcard => ResourceTypeSnapshot::Wildcard,
        }
    }

    fn from_snapshot(snap: Self::Serialized) -> Self {
        match snap {
            ResourceTypeSnapshot::Database => ResourceType::Database,
            ResourceTypeSnapshot::Token => ResourceType::Token,
            ResourceTypeSnapshot::Wildcard => ResourceType::Wildcard,
        }
    }
}

impl Snapshot for ResourceIdentifier {
    type Serialized = ResourceIdentifierSnapshot;

    fn snapshot(&self) -> Self::Serialized {
        match self {
            ResourceIdentifier::Database(db_id) => {
                ResourceIdentifierSnapshot::Database(db_id.clone())
            }
            ResourceIdentifier::Token(token_id) => {
                ResourceIdentifierSnapshot::Token(token_id.clone())
            }
            ResourceIdentifier::Wildcard => ResourceIdentifierSnapshot::Wildcard,
        }
    }

    fn from_snapshot(snap: Self::Serialized) -> Self {
        match snap {
            ResourceIdentifierSnapshot::Database(db_id) => ResourceIdentifier::Database(db_id),
            ResourceIdentifierSnapshot::Token(token_id) => ResourceIdentifier::Token(token_id),
            ResourceIdentifierSnapshot::Wildcard => ResourceIdentifier::Wildcard,
        }
    }
}

impl Snapshot for DatabaseActions {
    type Serialized = DatabaseActionsSnapshot;

    fn snapshot(&self) -> Self::Serialized {
        DatabaseActionsSnapshot(u16::MAX)
    }

    fn from_snapshot(snap: Self::Serialized) -> Self {
        snap.0.into()
    }
}

impl Snapshot for CrudActions {
    type Serialized = CrudActionsSnapshot;

    fn snapshot(&self) -> Self::Serialized {
        CrudActionsSnapshot(u16::MAX)
    }

    fn from_snapshot(snap: Self::Serialized) -> Self {
        snap.0.into()
    }
}

impl Snapshot for NodeState {
    type Serialized = NodeStateSnapshot;

    fn snapshot(&self) -> Self::Serialized {
        match self {
            NodeState::Running { registered_time_ns } => NodeStateSnapshot::Running {
                registered_time_ns: *registered_time_ns,
            },
            NodeState::Stopped { stopped_time_ns } => NodeStateSnapshot::Stopped {
                stopped_time_ns: *stopped_time_ns,
            },
        }
    }

    fn from_snapshot(snap: Self::Serialized) -> Self {
        match snap {
            NodeStateSnapshot::Running { registered_time_ns } => {
                Self::Running { registered_time_ns }
            }
            NodeStateSnapshot::Stopped { stopped_time_ns } => Self::Stopped { stopped_time_ns },
        }
    }
}
