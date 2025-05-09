//! Implementation of the Catalog that sits entirely in memory.

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD as B64;
use bimap::BiHashMap;
use influxdb3_authz::Actions;
use influxdb3_authz::Permission;
use influxdb3_authz::ResourceIdentifier;
use influxdb3_authz::ResourceType;
use influxdb3_authz::TokenInfo;
use influxdb3_authz::TokenProvider;
use influxdb3_id::{
    CatalogId, ColumnId, DbId, DistinctCacheId, LastCacheId, NodeId, SerdeVecMap, TableId, TokenId,
    TriggerId,
};
use influxdb3_shutdown::ShutdownToken;
use iox_time::{Time, TimeProvider};
use metric::Registry;
use metrics::CatalogMetrics;
use object_store::ObjectStore;
use observability_deps::tracing::{debug, error, info, trace, warn};
use parking_lot::RwLock;
use rand::RngCore;
use rand::rngs::OsRng;
use schema::{Schema, SchemaBuilder};
use serde::{Deserialize, Serialize};
use sha2::Digest;
use sha2::Sha512;
use std::borrow::Cow;
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::hash::Hash;
use std::sync::Arc;
use tokio::sync::{Mutex, MutexGuard};
use uuid::Uuid;

mod metrics;
mod update;
pub use schema::{InfluxColumnType, InfluxFieldType};
pub use update::{CatalogUpdate, DatabaseCatalogTransaction, Prompt};

use crate::channel::{CatalogSubscriptions, CatalogUpdateReceiver};
use crate::log::CreateAdminTokenDetails;
use crate::log::{
    CreateDatabaseLog, DatabaseBatch, DatabaseCatalogOp, NodeBatch, NodeCatalogOp, NodeMode,
    RegenerateAdminTokenDetails, RegisterNodeLog, StopNodeLog, TokenBatch, TokenCatalogOp,
};
use crate::object_store::ObjectStoreCatalog;
use crate::resource::CatalogResource;
use crate::snapshot::{CatalogSnapshot, Snapshot};
use crate::{
    CatalogError, Result,
    log::{
        AddFieldsLog, CatalogBatch, CreateTableLog, DeleteDistinctCacheLog, DeleteLastCacheLog,
        DeleteTriggerLog, DistinctCacheDefinition, FieldDefinition, LastCacheDefinition,
        OrderedCatalogBatch, SoftDeleteDatabaseLog, SoftDeleteTableLog, TriggerDefinition,
        TriggerIdentifier,
    },
};

const SOFT_DELETION_TIME_FORMAT: &str = "%Y%m%dT%H%M%S";

pub const TIME_COLUMN_NAME: &str = "time";

pub const INTERNAL_DB_NAME: &str = "_internal";

const DEFAULT_ADMIN_TOKEN_NAME: &str = "_admin";

/// Limit for the number of tag columns on a table
pub(crate) const NUM_TAG_COLUMNS_LIMIT: usize = 250;

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

pub struct Catalog {
    // The Catalog stores a reference to the metric registry so that other components in the
    // system that are initialized from/with the catalog can easily access it as needed
    metric_registry: Arc<Registry>,
    state: parking_lot::Mutex<CatalogState>,
    subscriptions: Arc<tokio::sync::RwLock<CatalogSubscriptions>>,
    time_provider: Arc<dyn TimeProvider>,
    /// Connection to the object store for managing persistence and updates to the catalog
    store: ObjectStoreCatalog,
    metrics: Arc<CatalogMetrics>,
    /// In-memory representation of the catalog
    pub(crate) inner: RwLock<InnerCatalog>,
    limits: CatalogLimits,
}

/// Custom implementation of `Debug` for the `Catalog` type to avoid serializing the object store
impl std::fmt::Debug for Catalog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Catalog")
            .field("inner", &self.inner)
            .finish()
    }
}

#[derive(Debug, Clone, Copy)]
enum CatalogState {
    Active,
    Shutdown,
}

impl CatalogState {
    fn is_shutdown(&self) -> bool {
        matches!(self, Self::Shutdown)
    }
}

const CATALOG_CHECKPOINT_INTERVAL: u64 = 100;

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

    pub async fn new(
        node_id: impl Into<Arc<str>>,
        store: Arc<dyn ObjectStore>,
        time_provider: Arc<dyn TimeProvider>,
        metric_registry: Arc<Registry>,
    ) -> Result<Self> {
        let node_id = node_id.into();
        let store =
            ObjectStoreCatalog::new(Arc::clone(&node_id), CATALOG_CHECKPOINT_INTERVAL, store);
        let subscriptions = Default::default();
        let metrics = Arc::new(CatalogMetrics::new(&metric_registry));
        let catalog = store
            .load_or_create_catalog()
            .await
            .map(RwLock::new)
            .map(|inner| Self {
                metric_registry,
                state: parking_lot::Mutex::new(CatalogState::Active),
                subscriptions,
                time_provider,
                store,
                metrics,
                inner,
                limits: Default::default(),
            })?;

        create_internal_db(&catalog).await;
        catalog.metrics.operation_observer(
            catalog
                .subscribe_to_updates("catalog_operation_metrics")
                .await,
        );
        Ok(catalog)
    }

    pub async fn new_with_shutdown(
        node_id: impl Into<Arc<str>>,
        store: Arc<dyn ObjectStore>,
        time_provider: Arc<dyn TimeProvider>,
        metric_registry: Arc<Registry>,
        shutdown_token: ShutdownToken,
    ) -> Result<Arc<Self>> {
        let node_id = node_id.into();
        let catalog =
            Arc::new(Self::new(Arc::clone(&node_id), store, time_provider, metric_registry).await?);
        let catalog_cloned = Arc::clone(&catalog);
        tokio::spawn(async move {
            shutdown_token.wait_for_shutdown().await;
            info!(
                node_id = node_id.as_ref(),
                "updating node state to stopped in catalog"
            );
            if let Err(error) = catalog_cloned
                .update_node_state_stopped(node_id.as_ref())
                .await
            {
                error!(
                    ?error,
                    node_id = node_id.as_ref(),
                    "encountered error while updating node to stopped state in catalog"
                );
            }
        });
        Ok(catalog)
    }

    pub fn metric_registry(&self) -> Arc<Registry> {
        Arc::clone(&self.metric_registry)
    }

    pub fn time_provider(&self) -> Arc<dyn TimeProvider> {
        Arc::clone(&self.time_provider)
    }

    pub fn set_state_shutdown(&self) {
        *self.state.lock() = CatalogState::Shutdown;
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

    pub fn object_store_prefix(&self) -> Arc<str> {
        Arc::clone(&self.store.prefix)
    }

    pub fn catalog_uuid(&self) -> Uuid {
        self.inner.read().catalog_uuid
    }

    pub async fn subscribe_to_updates(&self, name: &'static str) -> CatalogUpdateReceiver {
        self.subscriptions.write().await.subscribe(name)
    }

    pub fn object_store(&self) -> Arc<dyn ObjectStore> {
        self.store.object_store()
    }

    pub fn snapshot(&self) -> CatalogSnapshot {
        self.inner.read().snapshot()
    }

    pub fn update_from_snapshot(&self, snapshot: CatalogSnapshot) {
        let mut inner = self.inner.write();
        *inner = InnerCatalog::from_snapshot(snapshot);
    }

    /// Acquire a permit to write the provided `CatalogBatch` to object store
    ///
    /// This issues a `Prompt` to signal retry or success. The provided `sequence` is checked
    /// against the current catlog's sequence. If it is behind, due to some other concurrent
    /// update to the catalog, a retry is issued, so that the caller can re-compose the catalog
    /// batch using the latest state of the catalog and try again.
    pub async fn get_permit_and_verify_catalog_batch(
        &self,
        catalog_batch: CatalogBatch,
        sequence: CatalogSequenceNumber,
    ) -> Prompt<(OrderedCatalogBatch, CatalogWritePermit)> {
        // Get the write permit, and update its contents with the next catalog sequence number. If
        // the `catalog_batch` provided results in an update, i.e., changes the catalog, then this
        // will be the sequence number that the catalog is updated to.
        let mut permit = CATALOG_WRITE_PERMIT.lock().await;
        if sequence != self.sequence_number() {
            self.metrics.catalog_operation_retries.inc(1);
            return Prompt::Retry(());
        }
        *permit = self.sequence_number().next();
        trace!(
            next_sequence = permit.get(),
            "got permit to write to catalog"
        );
        Prompt::Success((OrderedCatalogBatch::new(catalog_batch, *permit), permit))
    }

    /// Apply an `OrderedCatalogBatch` to this catalog
    ///
    /// # Implementation note
    ///
    /// This accepts a `_permit`, which is not used, and is just a way to ensure that the caller
    /// has a handle on the write permit at the time of invocation.
    pub(crate) fn apply_ordered_catalog_batch(
        &self,
        batch: &OrderedCatalogBatch,
        _permit: &CatalogWritePermit,
    ) -> CatalogBatch {
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

    pub fn next_db_id(&self) -> DbId {
        self.inner.read().databases.next_id()
    }

    pub(crate) fn db_or_create(
        &self,
        db_name: &str,
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
                    })],
                );
                Ok((db, Some(batch)))
            }
        }
    }

    pub fn db_name_to_id(&self, db_name: &str) -> Option<DbId> {
        self.inner.read().databases.name_to_id(db_name)
    }

    pub fn db_id_to_name(&self, db_id: &DbId) -> Option<Arc<str>> {
        self.inner.read().databases.id_to_name(db_id)
    }

    pub fn db_schema(&self, db_name: &str) -> Option<Arc<DatabaseSchema>> {
        self.inner.read().databases.get_by_name(db_name)
    }

    pub fn db_schema_by_id(&self, db_id: &DbId) -> Option<Arc<DatabaseSchema>> {
        self.inner.read().databases.get_by_id(db_id)
    }

    /// List names of databases that have not been deleted
    pub fn db_names(&self) -> Vec<String> {
        self.inner
            .read()
            .databases
            .resource_iter()
            .filter(|db| !db.deleted)
            .map(|db| db.name.to_string())
            .collect()
    }

    pub fn list_db_schema(&self) -> Vec<Arc<DatabaseSchema>> {
        self.inner
            .read()
            .databases
            .resource_iter()
            .cloned()
            .collect()
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

    /// Get active triggers by database and trigger name
    // NOTE: this could be id-based in future
    pub fn active_triggers(&self) -> Vec<(Arc<str>, Arc<str>)> {
        let inner = self.inner.read();
        let result = inner
            .databases
            .resource_iter()
            .flat_map(|db| {
                db.processing_engine_triggers
                    .resource_iter()
                    .filter_map(move |trigger| {
                        if trigger.disabled {
                            None
                        } else {
                            Some((Arc::clone(&db.name), Arc::clone(&trigger.trigger_name)))
                        }
                    })
            })
            .collect();
        result
    }

    pub fn get_tokens(&self) -> Vec<Arc<TokenInfo>> {
        self.inner
            .read()
            .tokens
            .repo()
            .iter()
            .map(|(_, token_info)| Arc::clone(token_info))
            .collect()
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
                    .get_by_name(DEFAULT_ADMIN_TOKEN_NAME);

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
                    .contains_name(DEFAULT_ADMIN_TOKEN_NAME)
                {
                    return Err(CatalogError::TokenNameAlreadyExists(
                        DEFAULT_ADMIN_TOKEN_NAME.to_owned(),
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
                        name: Arc::from(DEFAULT_ADMIN_TOKEN_NAME),
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
                .get_by_name(DEFAULT_ADMIN_TOKEN_NAME)
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
        let metric_registry = Default::default();
        Self::new(catalog_id.into(), store, time_provider, metric_registry).await
    }

    /// Create a new `Catalog` with the specified checkpoint interval
    ///
    /// # Note
    ///
    /// This is intended for testing purposes.
    pub async fn new_with_checkpoint_interval(
        catalog_id: impl Into<Arc<str>>,
        store: Arc<dyn ObjectStore>,
        time_provider: Arc<dyn TimeProvider>,
        metric_registry: Arc<Registry>,
        checkpoint_interval: u64,
    ) -> Result<Self> {
        let store = ObjectStoreCatalog::new(catalog_id, checkpoint_interval, store);
        let inner = store.load_or_create_catalog().await?;
        let subscriptions = Default::default();

        let catalog = Self {
            state: parking_lot::Mutex::new(CatalogState::Active),
            subscriptions,
            time_provider,
            store,
            metrics: Arc::new(CatalogMetrics::new(&metric_registry)),
            metric_registry,
            inner: RwLock::new(inner),
            limits: Default::default(),
        };

        create_internal_db(&catalog).await;
        Ok(catalog)
    }
}

impl TokenProvider for Catalog {
    fn get_token(&self, token_hash: Vec<u8>) -> Option<Arc<TokenInfo>> {
        self.inner.read().tokens.hash_to_info(token_hash)
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Repository<I: CatalogId, R: CatalogResource> {
    pub(crate) repo: SerdeVecMap<I, Arc<R>>,
    pub(crate) id_name_map: BiHashMap<I, Arc<str>>,
    pub(crate) next_id: I,
}

impl<I: CatalogId, R: CatalogResource> Repository<I, R> {
    pub fn new() -> Self {
        Self {
            repo: SerdeVecMap::new(),
            id_name_map: BiHashMap::new(),
            next_id: I::default(),
        }
    }

    pub(crate) fn get_and_increment_next_id(&mut self) -> I {
        let next_id = self.next_id;
        self.next_id = self.next_id.next();
        next_id
    }

    pub(crate) fn next_id(&self) -> I {
        self.next_id
    }

    pub(crate) fn set_next_id(&mut self, id: I) {
        self.next_id = id;
    }

    pub fn name_to_id(&self, name: &str) -> Option<I> {
        self.id_name_map.get_by_right(name).copied()
    }

    pub fn id_to_name(&self, id: &I) -> Option<Arc<str>> {
        self.id_name_map.get_by_left(id).cloned()
    }

    pub fn get_by_name(&self, name: &str) -> Option<Arc<R>> {
        self.id_name_map
            .get_by_right(name)
            .and_then(|id| self.repo.get(id))
            .cloned()
    }

    pub fn get_by_id(&self, id: &I) -> Option<Arc<R>> {
        self.repo.get(id).cloned()
    }

    pub fn contains_id(&self, id: &I) -> bool {
        self.repo.contains_key(id)
    }

    pub fn contains_name(&self, name: &str) -> bool {
        self.id_name_map.contains_right(name)
    }

    pub fn len(&self) -> usize {
        self.repo.len()
    }

    pub fn is_empty(&self) -> bool {
        self.repo.is_empty()
    }

    /// Check if a resource exists in the repository by `id`
    ///
    /// # Panics
    ///
    /// This panics if the `id` is in the id-to-name map, but not in the actual repository map, as
    /// that would be a bad state for the repository to be in.
    fn id_exists(&self, id: &I) -> bool {
        let id_in_map = self.id_name_map.contains_left(id);
        let id_in_repo = self.repo.contains_key(id);
        assert_eq!(
            id_in_map, id_in_repo,
            "id map and repository are in an inconsistent state, \
            in map: {id_in_map}, in repo: {id_in_repo}"
        );
        id_in_repo
    }

    /// Check if a resource exists in the repository by `id` and `name`
    ///
    /// # Panics
    ///
    /// This panics if the `id` is in the id-to-name map, but not in the actual repository map, as
    /// that would be a bad state for the repository to be in.
    fn id_and_name_exists(&self, id: &I, name: &str) -> bool {
        let name_in_map = self.id_name_map.contains_right(name);
        self.id_exists(id) && name_in_map
    }

    /// Insert a new resource to the repository
    pub(crate) fn insert(&mut self, id: I, resource: impl Into<Arc<R>>) -> Result<()> {
        let resource = resource.into();
        if self.id_and_name_exists(&id, resource.name().as_ref()) {
            return Err(CatalogError::AlreadyExists);
        }
        self.id_name_map.insert(id, resource.name());
        self.repo.insert(id, resource);
        self.next_id = match self.next_id.cmp(&id) {
            Ordering::Less | Ordering::Equal => id.next(),
            Ordering::Greater => self.next_id,
        };
        Ok(())
    }

    /// Update an existing resource in the repository
    pub(crate) fn update(&mut self, id: I, resource: impl Into<Arc<R>>) -> Result<()> {
        let resource = resource.into();
        if !self.id_exists(&id) {
            return Err(CatalogError::NotFound);
        }
        self.id_name_map.insert(id, resource.name());
        self.repo.insert(id, resource);
        Ok(())
    }

    pub(crate) fn remove(&mut self, id: &I) {
        self.id_name_map.remove_by_left(id);
        self.repo.shift_remove(id);
    }

    pub fn iter(&self) -> impl Iterator<Item = (&I, &Arc<R>)> {
        self.repo.iter()
    }

    pub fn id_iter(&self) -> impl Iterator<Item = &I> {
        self.repo.keys()
    }

    pub fn resource_iter(&self) -> impl Iterator<Item = &Arc<R>> {
        self.repo.values()
    }
}

impl<I: CatalogId, R: CatalogResource> Default for Repository<I, R> {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone)]
pub struct InnerCatalog {
    pub(crate) sequence: CatalogSequenceNumber,
    /// The `catalog_id` is the user-provided value used to prefix catalog paths on the object store
    pub(crate) catalog_id: Arc<str>,
    /// The `catalog_uuid` is a unique identifier to distinguish catalog instantiations
    pub(crate) catalog_uuid: Uuid,
    pub(crate) nodes: Repository<NodeId, NodeDefinition>,
    /// The catalog is a map of databases with their table schemas
    pub(crate) databases: Repository<DbId, DatabaseSchema>,
    /// This holds all the tokens created and saved in catalog
    /// saved in catalog snapshot
    pub(crate) tokens: TokenRepository,
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

    pub fn db_exists(&self, db_id: DbId) -> bool {
        self.databases.get_by_id(&db_id).is_some()
    }
}

#[derive(Debug, Eq, PartialEq, Clone)]
pub struct NodeDefinition {
    pub(crate) node_id: Arc<str>,
    pub(crate) node_catalog_id: NodeId,
    pub(crate) instance_id: Arc<str>,
    pub(crate) mode: Vec<NodeMode>,
    pub(crate) core_count: u64,
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

#[derive(Debug, Eq, PartialEq, Clone, Copy, Serialize, Deserialize)]
pub enum NodeState {
    Running { registered_time_ns: i64 },
    Stopped { stopped_time_ns: i64 },
}

#[derive(Debug, Eq, PartialEq, Clone)]
pub struct DatabaseSchema {
    pub id: DbId,
    pub name: Arc<str>,
    pub tables: Repository<TableId, TableDefinition>,
    pub processing_engine_triggers: Repository<TriggerId, TriggerDefinition>,
    pub deleted: bool,
}

impl DatabaseSchema {
    pub fn new(id: DbId, name: Arc<str>) -> Self {
        Self {
            id,
            name,
            tables: Repository::new(),
            processing_engine_triggers: Repository::new(),
            deleted: false,
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

    pub fn table_schema_by_id(&self, table_id: &TableId) -> Option<Schema> {
        self.tables
            .get_by_id(table_id)
            .map(|table| table.influx_schema().clone())
    }

    pub fn table_definition(&self, table_name: impl AsRef<str>) -> Option<Arc<TableDefinition>> {
        self.tables.get_by_name(table_name.as_ref())
    }

    pub fn table_definition_by_id(&self, table_id: &TableId) -> Option<Arc<TableDefinition>> {
        self.tables.get_by_id(table_id)
    }

    pub fn table_ids(&self) -> Vec<TableId> {
        self.tables.id_iter().copied().collect()
    }

    pub fn table_names(&self) -> Vec<Arc<str>> {
        self.tables
            .resource_iter()
            .map(|td| Arc::clone(&td.table_name))
            .collect()
    }

    pub fn table_exists(&self, table_id: &TableId) -> bool {
        self.tables.get_by_id(table_id).is_some()
    }

    pub fn tables(&self) -> impl Iterator<Item = Arc<TableDefinition>> + use<'_> {
        self.tables.resource_iter().map(Arc::clone)
    }

    pub fn table_name_to_id(&self, table_name: impl AsRef<str>) -> Option<TableId> {
        self.tables.name_to_id(table_name.as_ref())
    }

    pub fn table_id_to_name(&self, table_id: &TableId) -> Option<Arc<str>> {
        self.tables.id_to_name(table_id)
    }

    pub fn list_distinct_caches(&self) -> Vec<Arc<DistinctCacheDefinition>> {
        self.tables
            .resource_iter()
            .filter(|t| !t.deleted)
            .flat_map(|t| t.distinct_caches.resource_iter())
            .cloned()
            .collect()
    }

    pub fn list_last_caches(&self) -> Vec<Arc<LastCacheDefinition>> {
        self.tables
            .resource_iter()
            .filter(|t| !t.deleted)
            .flat_map(|t| t.last_caches.resource_iter())
            .cloned()
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
        if !schema.tables.contains_id(&self.table_id) {
            return Ok(schema);
        }
        let mut_schema = schema.to_mut();
        if let Some(mut deleted_table) = mut_schema.tables.get_by_id(&self.table_id) {
            let deletion_time = Time::from_timestamp_nanos(self.deletion_time);
            let table_name = make_new_name_using_deleted_time(&self.table_name, deletion_time);
            let new_table_def = Arc::make_mut(&mut deleted_table);
            new_table_def.deleted = true;
            new_table_def.table_name = table_name;
            mut_schema
                .tables
                .update(new_table_def.table_id, deleted_table)
                .expect("the table should exist");
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
    pub columns: Repository<ColumnId, ColumnDefinition>,
    pub series_key: Vec<ColumnId>,
    pub series_key_names: Vec<Arc<str>>,
    pub last_caches: Repository<LastCacheId, LastCacheDefinition>,
    pub distinct_caches: Repository<DistinctCacheId, DistinctCacheDefinition>,
    pub deleted: bool,
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

        Ok(Self {
            table_id,
            table_name,
            schema,
            columns,
            series_key,
            series_key_names,
            last_caches: Repository::new(),
            distinct_caches: Repository::new(),
            deleted: false,
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

    /// Check if the column exists in the [`TableDefinition`]
    pub fn column_exists(&self, column: impl AsRef<str>) -> bool {
        self.columns.contains_name(column.as_ref())
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
        for (id, name, column_type) in columns {
            let nullable = name.as_ref() != TIME_COLUMN_NAME;
            assert!(
                cols.insert(
                    Arc::clone(&name),
                    Arc::new(ColumnDefinition::new(id, name, column_type, nullable))
                )
                .is_none(),
                "attempted to add existing column"
            );
            // add new tags to the series key in the order provided
            if matches!(column_type, InfluxColumnType::Tag) && !self.series_key.contains(&id) {
                self.series_key.push(id);
            }
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

        let mut new_columns = Repository::new();
        for col in cols.values().cloned() {
            new_columns
                .insert(col.id, col)
                .expect("should be a new column");
        }
        self.columns = new_columns;

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
    pub fn column_name_to_id_unchecked(&self, name: impl AsRef<str>) -> ColumnId {
        self.columns
            .name_to_id(name.as_ref())
            .expect("Column exists in mapping")
    }

    // TODO(trevor): remove thid API in favour of the Repository APIs
    pub fn column_id_to_name_unchecked(&self, id: &ColumnId) -> Arc<str> {
        self.columns
            .id_to_name(id)
            .expect("Column exists in mapping")
    }

    // TODO(trevor): remove thid API in favour of the Repository APIs
    pub fn column_definition(&self, name: impl AsRef<str>) -> Option<Arc<ColumnDefinition>> {
        self.columns.get_by_name(name.as_ref())
    }

    // TODO(trevor): remove thid API in favour of the Repository APIs
    pub fn column_definition_by_id(&self, id: &ColumnId) -> Option<Arc<ColumnDefinition>> {
        self.columns.get_by_id(id)
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

#[derive(Debug, Clone, Default)]
pub(crate) struct TokenRepository {
    repo: Repository<TokenId, TokenInfo>,
    hash_lookup_map: BiHashMap<TokenId, Vec<u8>>,
}

impl TokenRepository {
    pub(crate) fn new(
        repo: Repository<TokenId, TokenInfo>,
        hash_lookup_map: BiHashMap<TokenId, Vec<u8>>,
    ) -> Self {
        Self {
            repo,
            hash_lookup_map,
        }
    }

    pub(crate) fn repo(&self) -> &Repository<TokenId, TokenInfo> {
        &self.repo
    }

    pub(crate) fn get_and_increment_next_id(&mut self) -> TokenId {
        self.repo.get_and_increment_next_id()
    }

    pub(crate) fn hash_to_info(&self, hash: Vec<u8>) -> Option<Arc<TokenInfo>> {
        let id = self
            .hash_lookup_map
            .get_by_right(&hash)
            .map(|id| id.to_owned())?;
        self.repo.get_by_id(&id)
    }

    pub(crate) fn add_token(&mut self, token_id: TokenId, token_info: TokenInfo) -> Result<()> {
        self.hash_lookup_map
            .insert(token_id, token_info.hash.clone());
        self.repo.insert(token_id, token_info)?;
        Ok(())
    }

    pub(crate) fn update_admin_token_hash(
        &mut self,
        token_id: TokenId,
        hash: Vec<u8>,
        updated_at: i64,
    ) -> Result<()> {
        let mut token_info = self
            .repo
            .get_by_id(&token_id)
            .ok_or_else(|| CatalogError::MissingAdminTokenToUpdate)?;
        let updatable = Arc::make_mut(&mut token_info);
        updatable.hash = hash.clone();
        updatable.updated_at = Some(updated_at);
        updatable.updated_by = Some(token_id);
        self.repo.update(token_id, token_info)?;
        self.hash_lookup_map.insert(token_id, hash);
        Ok(())
    }

    pub(crate) fn delete_token(&mut self, token_name: String) -> Result<()> {
        let token_id = self
            .repo
            .name_to_id(&token_name)
            .ok_or_else(|| CatalogError::NotFound)?;
        self.repo.remove(&token_id);
        self.hash_lookup_map.remove_by_left(&token_id);
        Ok(())
    }
}

impl CatalogResource for TokenInfo {
    type Identifier = TokenId;

    fn id(&self) -> Self::Identifier {
        self.id
    }

    fn name(&self) -> Arc<str> {
        Arc::clone(&self.name)
    }
}

fn create_token_and_hash() -> (String, Vec<u8>) {
    let token = {
        let mut token = String::from("apiv3_");
        let mut key = [0u8; 64];
        OsRng.fill_bytes(&mut key);
        token.push_str(&B64.encode(key));
        token
    };
    (token.clone(), Sha512::digest(&token).to_vec())
}

#[cfg(test)]
mod tests {

    use crate::{
        log::{FieldDataType, LastCacheSize, LastCacheTtl, MaxAge, MaxCardinality, create},
        object_store::CatalogFilePath,
        serialize::{serialize_catalog_snapshot, verify_and_deserialize_catalog_checkpoint_file},
    };

    use super::*;
    use influxdb3_test_helpers::object_store::RequestCountedObjectStore;
    use iox_time::MockProvider;
    use object_store::local::LocalFileSystem;
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
                let snapshot = verify_and_deserialize_catalog_checkpoint_file(serialized).unwrap() ;
                insta::assert_json_snapshot!(snapshot, {
                    ".catalog_uuid" => "[uuid]"
                });
                catalog.update_from_snapshot(snapshot);
                assert_eq!(catalog.db_name_to_id("test_db"), Some(DbId::from(1)));
            });
        }
    }

    #[test]
    fn add_columns_updates_schema_and_column_map() {
        let mut database = DatabaseSchema {
            id: DbId::from(0),
            name: "test".into(),
            tables: Repository::new(),
            processing_engine_triggers: Default::default(),
            deleted: false,
        };
        database
            .tables
            .insert(
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
            )
            .unwrap();

        let mut table = database.tables.get_by_id(&TableId::from(0)).unwrap();
        println!("table: {table:#?}");
        assert_eq!(table.columns.len(), 1);
        assert_eq!(table.column_id_to_name_unchecked(&0.into()), "test".into());

        Arc::make_mut(&mut table)
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
        assert_eq!(table.columns.len(), 2);
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
                let snapshot = verify_and_deserialize_catalog_checkpoint_file(serialized).unwrap() ;
                insta::assert_json_snapshot!(snapshot, {
                    ".catalog_uuid" => "[uuid]"
                });
                catalog.update_from_snapshot(snapshot);
                assert_eq!(catalog.db_name_to_id("test_db"), Some(DbId::from(1)));
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
                let snapshot = verify_and_deserialize_catalog_checkpoint_file(serialized).unwrap() ;
                insta::assert_json_snapshot!(snapshot, {
                    ".catalog_uuid" => "[uuid]"
                });
                catalog.update_from_snapshot(snapshot);
                assert_eq!(catalog.db_name_to_id("test_db"), Some(DbId::from(1)));
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
                let snapshot = verify_and_deserialize_catalog_checkpoint_file(serialized).unwrap() ;
                insta::assert_json_snapshot!(snapshot, {
                    ".catalog_uuid" => "[uuid]"
                });
                catalog.update_from_snapshot(snapshot);
                assert_eq!(catalog.db_name_to_id("test_db"), Some(DbId::from(1)));
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
        debug!("getting write lock");
        let mut inner = catalog.inner.write();
        let sequence = inner.sequence_number();
        let err = inner
            .apply_catalog_batch(&catalog_batch, sequence.next())
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
        let create_ordered_op = catalog
            .inner
            .write()
            .apply_catalog_batch(&create_op, catalog.sequence_number().next())
            .expect("apply create op")
            .expect("should be able to create");
        debug!("apply add column op");
        let add_column_op = catalog
            .inner
            .write()
            .apply_catalog_batch(&add_column_op, catalog.sequence_number().next())
            .expect("apply add column op")
            .expect("should produce operation");
        let mut ordered_batches = vec![add_column_op, create_ordered_op];
        ordered_batches.sort();

        let replayed_catalog = Catalog::new_in_memory("host").await.unwrap();
        debug!(?ordered_batches, "apply sorted ops");
        let permit = CATALOG_WRITE_PERMIT.lock().await;
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

    #[test_log::test(tokio::test)]
    async fn test_catalog_file_ordering() {
        let local_disk =
            Arc::new(LocalFileSystem::new_with_prefix(test_helpers::tmp_dir().unwrap()).unwrap());
        let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));

        let init = async || {
            Catalog::new(
                "test",
                Arc::clone(&local_disk) as _,
                Arc::clone(&time_provider) as _,
                Default::default(),
            )
            .await
            .unwrap()
        };

        let catalog = init().await;

        // create a database, then a table, then add fields to that table
        // on reload, the add fields would fail if it was applied before the creation of the
        // table...
        catalog.create_database("test_db").await.unwrap();
        catalog
            .create_table(
                "test_db",
                "test_tbl",
                &["t1"],
                &[("f1", FieldDataType::String)],
            )
            .await
            .unwrap();
        let mut txn = catalog.begin("test_db").unwrap();
        txn.column_or_create("test_tbl", "f2", FieldDataType::Integer)
            .unwrap();
        catalog.commit(txn).await.unwrap();

        drop(catalog);

        let catalog = init().await;

        insta::assert_json_snapshot!(catalog.snapshot(), {
            ".catalog_uuid" => "[uuid]"
        });
    }

    #[test_log::test(tokio::test)]
    async fn test_load_from_catalog_checkpoint() {
        let obj_store =
            Arc::new(LocalFileSystem::new_with_prefix(test_helpers::tmp_dir().unwrap()).unwrap());
        let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));

        let init = async || {
            // create a catalog that checkpoints every 10 sequences
            Catalog::new_with_checkpoint_interval(
                "test",
                Arc::clone(&obj_store) as _,
                Arc::clone(&time_provider) as _,
                Default::default(),
                10,
            )
            .await
            .unwrap()
        };

        let catalog = init().await;

        // make changes to create catalog operations that get persisted to the log:
        catalog.create_database("test_db").await.unwrap();
        for i in 0..10 {
            catalog
                .create_table(
                    "test_db",
                    format!("table_{i}").as_str(),
                    &["t1"],
                    &[("f1", FieldDataType::String)],
                )
                .await
                .unwrap();
        }
        tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

        let prefix = catalog.object_store_prefix();
        drop(catalog);

        // delete up to the 10th catalog file so that when we re-init, we know it is loading
        // from the checkpoint:
        for i in 1..=10 {
            obj_store
                .delete(
                    CatalogFilePath::log(prefix.as_ref(), CatalogSequenceNumber::new(i)).as_ref(),
                )
                .await
                .unwrap();
        }

        // catalog should load successfully:
        let catalog = init().await;

        // we created 10 tables so the catalog should have 10:
        assert_eq!(10, catalog.db_schema("test_db").unwrap().tables.len());
    }

    #[test_log::test(tokio::test)]
    async fn test_load_many_files_with_default_checkpoint_interval() {
        let obj_store =
            Arc::new(LocalFileSystem::new_with_prefix(test_helpers::tmp_dir().unwrap()).unwrap());
        let obj_store = Arc::new(RequestCountedObjectStore::new(obj_store as _));
        let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));

        let init = async || {
            // create a catalog that checkpoints every 10 sequences
            Catalog::new(
                "test",
                Arc::clone(&obj_store) as _,
                Arc::clone(&time_provider) as _,
                Default::default(),
            )
            .await
            .unwrap()
        };

        let catalog = init().await;
        catalog.create_database("foo").await.unwrap();
        for i in 0..100 {
            let table_name = format!("table_{i}");
            catalog
                .create_table(
                    "foo",
                    &table_name,
                    &["t1"],
                    &[("f1", FieldDataType::String)],
                )
                .await
                .unwrap();
            let mut txn = catalog.begin("foo").unwrap();
            txn.column_or_create(&table_name, "f2", FieldDataType::String)
                .unwrap();
            catalog.commit(txn).await.unwrap();
        }

        let checkpoint_read_count = obj_store.total_read_request_count(
            CatalogFilePath::checkpoint(catalog.object_store_prefix().as_ref()).as_ref(),
        );
        // checkpoint would have been attempted to be read on initialization, hence it is 1:
        assert_eq!(1, checkpoint_read_count);

        let first_log_read_count = obj_store.total_read_request_count(
            CatalogFilePath::log(
                catalog.object_store_prefix().as_ref(),
                CatalogSequenceNumber::new(1),
            )
            .as_ref(),
        );
        // this file should never have been read:
        assert_eq!(0, first_log_read_count);

        let last_log_read_count = obj_store.total_read_request_count(
            CatalogFilePath::log(
                catalog.object_store_prefix().as_ref(),
                catalog.sequence_number(),
            )
            .as_ref(),
        );
        // this file should never have been read:
        assert_eq!(0, last_log_read_count);

        // drop the catalog and re-initialize:
        drop(catalog);
        let catalog = init().await;

        let checkpoint_read_count = obj_store.total_read_request_count(
            CatalogFilePath::checkpoint(catalog.object_store_prefix().as_ref()).as_ref(),
        );
        // re-init will read the checkpoint again:
        assert_eq!(2, checkpoint_read_count);

        let first_log_read_count = obj_store.total_read_request_count(
            CatalogFilePath::log(
                catalog.object_store_prefix().as_ref(),
                CatalogSequenceNumber::new(1),
            )
            .as_ref(),
        );
        // this file should still not have been read, since it would have been covered by a
        // recent checkpoint:
        assert_eq!(0, first_log_read_count);

        let last_log_read_count = obj_store.total_read_request_count(
            CatalogFilePath::log(
                catalog.object_store_prefix().as_ref(),
                catalog.sequence_number(),
            )
            .as_ref(),
        );
        // this file should have been read on re-init, as it would not be covered by a
        // checkpoint:
        assert_eq!(1, last_log_read_count);
    }

    #[test_log::test(tokio::test)]
    async fn apply_catalog_batch_fails_for_add_fields_past_tag_limit() {
        let catalog = Catalog::new_in_memory("host").await.unwrap();
        catalog.create_database("foo").await.unwrap();
        let tags = (0..NUM_TAG_COLUMNS_LIMIT)
            .map(|i| format!("tag_{i}"))
            .collect::<Vec<_>>();
        catalog
            .create_table("foo", "bar", &tags, &[("f1", FieldDataType::String)])
            .await
            .unwrap();

        let mut txn = catalog.begin("foo").unwrap();
        let err = txn
            .column_or_create("bar", "tag_too_much", FieldDataType::Tag)
            .unwrap_err();
        assert_contains!(
            err.to_string(),
            "Update to schema would exceed number of tag columns per table limit of 250 columns"
        );
    }

    #[test_log::test(tokio::test)]
    async fn apply_catalog_batch_fails_to_create_table_with_too_many_tags() {
        let catalog = Catalog::new_in_memory("host").await.unwrap();
        catalog.create_database("foo").await.unwrap();
        let tags = (0..NUM_TAG_COLUMNS_LIMIT + 1)
            .map(|i| format!("tag_{i}"))
            .collect::<Vec<_>>();
        let err = catalog
            .create_table("foo", "bar", &tags, &[("f1", FieldDataType::String)])
            .await;
        assert_contains!(
            err.unwrap_err().to_string(),
            "Update to schema would exceed number of tag columns per table limit of 250 columns"
        );
    }
}
