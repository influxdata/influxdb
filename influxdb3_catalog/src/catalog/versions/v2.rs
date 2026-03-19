//! Version 2 implementation of the Catalog that sits entirely in memory.

use bimap::BiHashMap;
use data_types::{Namespace, NamespaceId};
use hashbrown::HashMap;
use indexmap::IndexMap;
use influxdb3_authz::{
    CrudActions, DatabaseActions, Permission, ResourceIdentifier, TokenInfo, TokenProvider,
};
use influxdb3_id::{
    CatalogId, ColumnId, ColumnIdentifier, DbId, DistinctCacheId, FieldFamilyId, FieldId,
    FieldIdentifier, LastCacheId, NodeId, TableId, TagId, TokenId, TriggerId,
};
use influxdb3_process::ProcessUuidGetter;
use influxdb3_shutdown::ShutdownToken;
use influxdb3_telemetry::ProcessingEngineMetrics;
use iox_time::{Time, TimeProvider};
use metric::Registry;
use metrics::CatalogMetrics;
use object_store::ObjectStore;
use observability_deps::tracing::{debug, error, info, trace, warn};
use parking_lot::RwLock;
use schema::{Schema, SchemaBuilder};
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::collections::btree_map::Entry;
use std::iter;
use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;

use crate::catalog::{
    CATALOG_WRITE_PERMIT, CatalogSequenceNumber, CatalogWritePermit, DEFAULT_OPERATOR_TOKEN_NAME,
    DeletedSchema, DeletionStatus, INTERNAL_DB_NAME, INTERNAL_DB_RETENTION_PERIOD, IfNotDeleted,
    RESERVED_COLUMN_NAMES, Repository, TIME_COLUMN_NAME, TokenRepository, create_token_and_hash,
    make_new_name_using_deleted_time,
};

mod resource;

#[cfg(test)]
mod create;
pub(crate) mod field;
pub mod legacy;
mod metrics;
pub(crate) mod update;

use schema::sort::SortKey;
pub use schema::{InfluxColumnType, InfluxFieldType};
pub use update::HardDeletionTime;
pub use update::{
    CatalogUpdate, CreateDatabaseOptions, CreateTableOptions, DatabaseCatalogTransaction, Prompt,
};

use crate::channel::versions::v2::{CatalogSubscriptions, CatalogUpdateReceiver};
// Specifies the current version of the log used by the catalog
pub use crate::log::versions::v4 as log;
use crate::object_store::versions::v2::ObjectStoreCatalog;
use crate::resource::CatalogResource;
pub(crate) use crate::snapshot::versions::v4 as snapshot;
use crate::{CatalogError, Result};
use log::{
    AddColumnsLog, CatalogBatch, ClearRetentionPeriodLog, CreateAdminTokenDetails,
    CreateDatabaseLog, CreateTableLog, DatabaseBatch, DatabaseCatalogOp, DeleteBatch,
    DeleteDistinctCacheLog, DeleteLastCacheLog, DeleteOp, DeleteTriggerLog,
    DistinctCacheDefinition, FieldFamilyDefinitionLog, GenerationBatch, GenerationOp,
    LastCacheDefinition, NodeBatch, NodeCatalogOp, NodeMode, OrderedCatalogBatch,
    RegenerateAdminTokenDetails, RegisterNodeLog, RetentionPeriod, SetRetentionPeriodLog,
    SoftDeleteDatabaseLog, SoftDeleteTableLog, StopNodeLog, TokenBatch, TokenCatalogOp,
    TriggerDefinition, TriggerIdentifier, TriggerSpecificationDefinition,
};
use snapshot::CatalogSnapshot;

/// Limit for the number of tag columns on a table
pub(crate) const NUM_TAG_COLUMNS_LIMIT: usize = TagId::MAX.get() as usize + 1;
/// Limit for the number of fields in a field family.
pub(crate) const NUM_FIELDS_PER_FAMILY_LIMIT: usize = 100;

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

    pub async fn new(
        node_id: impl Into<Arc<str>>,
        store: Arc<dyn ObjectStore>,
        time_provider: Arc<dyn TimeProvider>,
        metric_registry: Arc<Registry>,
    ) -> Result<Self> {
        Self::new_with_args(
            node_id,
            store,
            time_provider,
            metric_registry,
            CatalogArgs::default(),
            CatalogLimits::default(),
        )
        .await
    }

    pub async fn new_with_args(
        node_id: impl Into<Arc<str>>,
        store: Arc<dyn ObjectStore>,
        time_provider: Arc<dyn TimeProvider>,
        metric_registry: Arc<Registry>,
        args: CatalogArgs,
        limits: CatalogLimits,
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
                limits,
                args,
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
        process_uuid_getter: Arc<dyn ProcessUuidGetter>,
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
                .update_node_state_stopped(node_id.as_ref(), process_uuid_getter)
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

    fn default_hard_delete_duration(&self) -> Duration {
        self.args.default_hard_delete_duration
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
    /// against the current catalog's sequence. If it is behind, due to some other concurrent
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

    pub fn list_nodes(&self) -> Vec<Arc<NodeDefinition>> {
        self.inner.read().nodes.resource_iter().cloned().collect()
    }

    pub fn next_db_id(&self) -> DbId {
        self.inner.read().databases.next_id()
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

    /// Returns the deletion status of a database by its ID.
    ///
    /// If the database exists as is not marked for deletion, `None` is returned.
    pub fn database_deletion_status(&self, db_id: DbId) -> Option<DeletionStatus> {
        let inner = self.inner.read();

        database_or_deletion_status(inner.databases.get_by_id(&db_id), &self.time_provider).err()
    }

    /// Returns the deletion status of a table by its ID within a specific database.
    ///
    /// If the table exists and is not marked for deletion, `None` is returned.
    pub fn table_deletion_status(&self, db_id: DbId, table_id: TableId) -> Option<DeletionStatus> {
        let inner = self.inner.read();

        match database_or_deletion_status(inner.databases.get_by_id(&db_id), &self.time_provider) {
            Ok(db_schema) => table_deletion_status(&db_schema, table_id, &self.time_provider),
            Err(status) => Some(status),
        }
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
        inner
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
            .collect()
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

    /// Create an admin token with a specific name and hash
    pub async fn create_named_admin_token_with_hash(
        &self,
        name: String,
        hash: Vec<u8>,
        expiry_millis: Option<i64>,
    ) -> Result<()> {
        use crate::log::CreateAdminTokenDetails;

        self.catalog_update_with_retry(|| {
            if self.inner.read().tokens.repo().contains_name(&name) {
                return Err(CatalogError::TokenNameAlreadyExists(name.clone()));
            }
            let (token_id, created_at) = {
                let mut inner = self.inner.write();
                let token_id = inner.tokens.get_and_increment_next_id();
                let created_at = self.time_provider.now();
                (token_id, created_at.timestamp_millis())
            };
            Ok(CatalogBatch::Token(TokenBatch {
                time_ns: created_at,
                ops: vec![TokenCatalogOp::CreateAdminToken(CreateAdminTokenDetails {
                    token_id,
                    name: Arc::from(name.as_str()),
                    hash: hash.clone(),
                    created_at,
                    updated_at: None,
                    expiry: expiry_millis,
                })],
            }))
        })
        .await?;
        Ok(())
    }

    // Return a map of all retention periods indexed by their combined database & table IDs.
    pub fn get_retention_period_cutoff_map(&self) -> BTreeMap<(DbId, TableId), i64> {
        self.list_db_schema()
            .into_iter()
            .flat_map(|db_schema| {
                db_schema
                    .tables()
                    .filter_map(|table_def| {
                        let db_id = db_schema.id();
                        let table_id = table_def.id();
                        db_schema
                            .get_retention_period_cutoff_ts_nanos(self.time_provider())
                            .map(|cutoff| ((db_id, table_id), cutoff))
                    })
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    pub fn get_generation_duration(&self, level: u8) -> Option<Duration> {
        self.inner
            .read()
            .generation_config
            .duration_for_level(level)
    }

    pub fn list_generation_durations(&self) -> Vec<(u8, Duration)> {
        self.inner
            .read()
            .generation_config
            .generation_durations
            .iter()
            .map(|(level, duration)| (*level, *duration))
            .collect()
    }

    pub fn list_namespaces(&self) -> Vec<Namespace> {
        self.inner
            .read()
            .databases
            .resource_iter()
            .map(|db| db.as_namespace())
            .collect()
    }
}

async fn create_internal_db(catalog: &Catalog) {
    let result = catalog
        .create_database_opts(
            INTERNAL_DB_NAME,
            CreateDatabaseOptions::default().retention_period(INTERNAL_DB_RETENTION_PERIOD),
        )
        .await;
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

    pub async fn new_in_memory_with_limits(
        catalog_id: impl Into<Arc<str>>,
        limits: CatalogLimits,
    ) -> Result<Self> {
        use iox_time::MockProvider;

        let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
        Self::new_in_memory_with_args_limits(
            catalog_id,
            time_provider,
            CatalogArgs::default(),
            limits,
        )
        .await
    }

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
        Self::new_in_memory_with_args_limits(catalog_id, time_provider, args, Default::default())
            .await
    }

    /// Create new `Catalog` that uses an in-memory object store with additional configuration
    /// arguments and limits for testing.
    ///
    /// # Note
    ///
    /// This is intended as a convenience constructor for testing
    pub async fn new_in_memory_with_args_limits(
        catalog_id: impl Into<Arc<str>>,
        time_provider: Arc<dyn TimeProvider>,
        args: CatalogArgs,
        limits: CatalogLimits,
    ) -> Result<Self> {
        use object_store::memory::InMemory;

        let store = Arc::new(InMemory::new());
        let metric_registry = Default::default();
        Self::new_with_args(
            catalog_id.into(),
            store,
            time_provider,
            metric_registry,
            args,
            limits,
        )
        .await
    }

    /// Create new `Catalog` that uses the specified `ObjectStore`.
    ///
    /// # Note
    ///
    /// This is intended as a convenience constructor for testing
    pub async fn new_with_store(
        catalog_id: impl Into<Arc<str>>,
        store: Arc<dyn ObjectStore>,
        time_provider: Arc<dyn TimeProvider>,
    ) -> Result<Self> {
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
            args: Default::default(),
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

impl ProcessingEngineMetrics for Catalog {
    fn num_triggers(&self) -> (u64, u64, u64, u64) {
        self.inner.read().num_triggers()
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
            // count if not db deleted _and_ not internal
            .filter(|db| !db.deleted && db.name().as_ref() != INTERNAL_DB_NAME)
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
                    cli_params,
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
                        n.cli_params = cli_params.as_ref().map(|s| Arc::from(s.as_str()));
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
                            cli_params: cli_params.as_ref().map(|s| Arc::from(s.as_str())),
                        });
                        self.nodes
                            .insert(node_batch.node_catalog_id, new_node)
                            .expect("there should not already be a node");
                    }
                    true
                }
                NodeCatalogOp::StopNode(StopNodeLog {
                    stopped_time_ns, ..
                }) => match self.nodes.get_by_id(&node_batch.node_catalog_id) {
                    Some(mut new_node) => {
                        let n = Arc::make_mut(&mut new_node);
                        n.state = NodeState::Stopped {
                            stopped_time_ns: *stopped_time_ns,
                        };
                        self.nodes
                            .update(node_batch.node_catalog_id, new_node)
                            .expect("there should be a node to update");
                        true
                    }
                    None => {
                        warn!(
                            node_id = %&node_batch.node_catalog_id,
                            "cannot find node id, skipping stop operation"
                        );
                        false
                    }
                },
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

    pub fn num_triggers(&self) -> (u64, u64, u64, u64) {
        self.databases
            .iter()
            .map(|(_, db)| db.trigger_count_by_type())
            .fold(
                (0, 0, 0, 0),
                |(
                    mut overall_wal_count,
                    mut overall_all_wal_count,
                    mut overall_schedule_count,
                    mut overall_request_count,
                ),
                 (wal_count, all_wal_count, schedule_count, request_count)| {
                    overall_wal_count += wal_count;
                    overall_all_wal_count += all_wal_count;
                    overall_schedule_count += schedule_count;
                    overall_request_count += request_count;
                    (
                        overall_wal_count,
                        overall_all_wal_count,
                        overall_schedule_count,
                        overall_request_count,
                    )
                },
            )
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

    pub fn duration_for_level(&self, level: u8) -> Option<Duration> {
        self.generation_durations.get(&level).copied()
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
    /// CLI parameters provided when the node was registered
    pub(crate) cli_params: Option<Arc<str>>,
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

    pub fn cli_params(&self) -> Option<Arc<str>> {
        self.cli_params.clone()
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

    pub fn trigger_count_by_type(&self) -> (u64, u64, u64, u64) {
        self.processing_engine_triggers.iter().fold(
            (0, 0, 0, 0),
            |(mut wal_count, mut all_wal_count, mut schedule_count, mut request_count),
             (_, trigger)| {
                match trigger.trigger {
                    // wal
                    TriggerSpecificationDefinition::SingleTableWalWrite { .. } => wal_count += 1,
                    TriggerSpecificationDefinition::AllTablesWalWrite => all_wal_count += 1,
                    // schedule
                    TriggerSpecificationDefinition::Schedule { .. }
                    | TriggerSpecificationDefinition::Every { .. } => schedule_count += 1,
                    // request
                    TriggerSpecificationDefinition::RequestPath { .. } => request_count += 1,
                };
                (wal_count, all_wal_count, schedule_count, request_count)
            },
        )
    }

    // Return the oldest allowable timestamp for the given table according to the
    // currently-available set of retention policies. This is returned as a number of nanoseconds
    // since the Unix Epoch.
    pub fn get_retention_period_cutoff_ts_nanos(
        &self,
        time_provider: Arc<dyn TimeProvider>,
    ) -> Option<i64> {
        let retention_period = match self.retention_period {
            RetentionPeriod::Duration(d) => Some(d.as_nanos() as u64),
            RetentionPeriod::Indefinite => None,
        }?;

        let now = time_provider.now().timestamp_nanos();
        Some(now - retention_period as i64)
    }

    /// Returns the deletion status of a table by its table ID
    ///
    /// If the table exists and is not deleted, returns `None`.
    pub fn table_deletion_status(
        &self,
        table_id: TableId,
        time_provider: Arc<dyn TimeProvider>,
    ) -> Option<DeletionStatus> {
        table_deletion_status(self, table_id, &time_provider)
    }

    pub fn as_namespace(&self) -> Namespace {
        let retention_period_ns = match self.retention_period {
            RetentionPeriod::Indefinite => None,
            RetentionPeriod::Duration(duration) => {
                TryInto::<i64>::try_into(duration.as_nanos()).ok()
            }
        };
        Namespace {
            id: NamespaceId::new(self.id.get().into()),
            name: self.name.to_string(),
            retention_period_ns,
            deleted_at: self.hard_delete_time.map(Into::into),
            // NOTE(tjh): all of the below are IOx attributes that are not used in Core/Enterprise
            // and therefore are populated with defaults.
            max_tables: Default::default(),
            max_columns_per_table: Default::default(),
            partition_template: Default::default(),
            router_version: Default::default(),
            created_at: None,
        }
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
            DatabaseCatalogOp::AddColumns(column_additions) => {
                column_additions.update_schema(schema)
            }
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
        if !database_schema.tables.contains_id(&self.table_id) {
            debug!(log = ?self, "creating new table from log");
            let new_table = TableDefinition::new_from_op(self);
            database_schema
                .to_mut()
                .insert_table_from_log(new_table.table_id, Arc::new(new_table));
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
        // Disable all triggers so they are marked as disabled in the catalog
        let trigger_ids: Vec<_> = owned
            .processing_engine_triggers
            .iter()
            .filter(|(_, t)| !t.disabled)
            .map(|(id, _)| *id)
            .collect();
        for id in trigger_ids {
            if let Some(mut trigger) = owned.processing_engine_triggers.get_by_id(&id) {
                Arc::make_mut(&mut trigger).disabled = true;
                owned
                    .processing_engine_triggers
                    .update(id, trigger)
                    .expect("trigger should exist");
            }
        }
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

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ColumnSet {
    /// Store for items in the repository
    pub(crate) repo: IndexMap<ColumnIdentifier, ColumnDefinition>,
    /// Bi-directional map of identifiers to names in the repository
    pub(crate) id_name_map: BiHashMap<ColumnIdentifier, Arc<str>>,
    /// Bi-directional map of identifiers to ordinal column IDs in the repository
    pub(crate) id_ord_id_map: BiHashMap<ColumnIdentifier, ColumnId>,
    /// The next available column ID to use for new columns
    pub(crate) next_id: ColumnId,
}

impl ColumnSet {
    pub fn new() -> Self {
        Self {
            repo: IndexMap::new(),
            id_name_map: BiHashMap::new(),
            id_ord_id_map: BiHashMap::new(),
            next_id: 0.into(),
        }
    }

    pub fn get_and_increment_next_id(&mut self) -> ColumnId {
        let next_id = self.next_id;
        self.next_id = self.next_id.next();
        next_id
    }

    pub fn get_by_name(&self, name: &str) -> Option<&ColumnDefinition> {
        self.id_name_map
            .get_by_right(name)
            .and_then(|id| self.repo.get(id))
    }

    pub fn get_by_id(&self, id: &ColumnIdentifier) -> Option<&ColumnDefinition> {
        self.repo.get(id)
    }

    pub fn get_by_ord_id(&self, column_id: &ColumnId) -> Option<&ColumnDefinition> {
        self.id_ord_id_map
            .get_by_right(column_id)
            .and_then(|id| self.repo.get(id))
    }

    pub fn id_to_name(&self, id: &ColumnIdentifier) -> Option<Arc<str>> {
        self.id_name_map.get_by_left(id).cloned()
    }

    pub fn name_to_id(&self, column_name: &str) -> Option<ColumnIdentifier> {
        self.id_name_map.get_by_right(column_name).cloned()
    }

    pub fn name_to_ord_id(&self, column_name: &str) -> Option<ColumnId> {
        self.id_name_map
            .get_by_right(column_name)
            .and_then(|id| self.id_ord_id_map.get_by_left(id).copied())
    }

    pub fn ord_id_to_name(&self, column_id: &ColumnId) -> Option<Arc<str>> {
        self.id_ord_id_map
            .get_by_right(column_id)
            .and_then(|id| self.id_name_map.get_by_left(id).cloned())
    }

    pub fn id_to_ord_id(&self, id: &ColumnIdentifier) -> Option<ColumnId> {
        self.id_ord_id_map.get_by_left(id).copied()
    }

    pub fn contains_id(&self, id: &ColumnIdentifier) -> bool {
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
    fn id_exists(&self, id: &ColumnIdentifier) -> bool {
        let id_in_name_map = self.id_name_map.contains_left(id);
        let id_in_col_id_map = self.id_ord_id_map.contains_left(id);
        let id_in_repo = self.repo.contains_key(id);
        assert!(
            (id_in_name_map == id_in_col_id_map) && (id_in_col_id_map == id_in_repo),
            "name, col_id and repository are in an inconsistent state, \
            name map: {id_in_name_map}, col_id map: {id_in_col_id_map}, in repo: {id_in_repo}"
        );
        id_in_repo
    }

    /// Check if a [ColumnDefinition] exists in the repository by `id`, `column_id` and `name`
    ///
    /// # Panics
    ///
    /// This panics if the `id` is in the id-to-name map, but not in the actual repository map, as
    /// that would be a bad state for the repository to be in.
    fn definition_exists(&self, v: &ColumnDefinition) -> bool {
        let name_in_map = self.id_name_map.contains_right(v.name().as_ref());
        let ord_id_in_map = self.id_ord_id_map.contains_right(&v.ord_id());
        self.id_exists(&v.id()) && name_in_map && ord_id_in_map
    }

    /// Insert a new resource to the repository
    pub fn insert(&mut self, resource: impl Into<ColumnDefinition>) -> Result<()> {
        let resource = resource.into();
        if self.definition_exists(&resource) {
            return Err(CatalogError::AlreadyExists);
        }
        let id = resource.id();
        let ord_id = resource.ord_id();
        self.id_name_map.insert(id, resource.name());
        self.id_ord_id_map.insert(id, ord_id);
        self.repo.insert(id, resource);
        self.next_id = match self.next_id.cmp(&ord_id) {
            Ordering::Less | Ordering::Equal => ord_id.next(),
            Ordering::Greater => self.next_id,
        };
        Ok(())
    }

    pub fn iter(&self) -> impl Iterator<Item = (&ColumnIdentifier, &ColumnDefinition)> {
        self.repo.iter()
    }

    pub fn resource_iter(&self) -> impl Iterator<Item = &ColumnDefinition> {
        self.repo.values()
    }
}

impl Default for ColumnSet {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum FieldFamilyMode {
    /// Fields must always be specified with a field family prefix for
    /// SQL and line protocol.
    #[default]
    Aware,
    /// Fields are never interpreted with a field family prefix, and
    /// fields are automatically assigned to the next available slot in
    /// the current auto field family.
    Auto,
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
    /// Ordered column definitions for the table
    pub columns: ColumnSet,
    /// The timestamp column for the table, if it exists
    timestamp_column: Option<Arc<TimestampColumn>>,
    /// Tag columns for the table
    tag_columns: Repository<TagId, TagColumn>,
    /// Field family definitions for the table
    pub field_families: Repository<FieldFamilyId, FieldFamilyDefinition>,
    field_count: usize,
    /// Fields which must be auto-assigned to a field family will be added to this one.
    auto_field_family: Option<FieldFamilyId>,
    /// The next auto field family name to be used when there is no existing `auto_field_family`.
    next_auto_field_family_name: u16,
    /// List of column identifiers that form the series key for the table
    ///
    /// The series key is used as the sort order, i.e., sort key, for the table during persistence.
    pub series_key: Vec<TagId>,
    /// The names of the columns in the table's series key
    pub series_key_names: Vec<Arc<str>>,
    /// The sort key for the table when persisted to storage.
    ///
    /// The sort key is the series key along with the `time` column form the primary key for the
    /// table. The series key is determined as the order of tags provided when the table is
    /// first created, either by a write of line protocol, or by an explicit table creation.
    pub sort_key: SortKey,
    /// Last cache definitions for the table
    pub last_caches: Repository<LastCacheId, LastCacheDefinition>,
    /// Distinct cache definitions for the table
    pub distinct_caches: Repository<DistinctCacheId, DistinctCacheDefinition>,
    /// Whether this table has been set as deleted
    pub deleted: bool,
    /// The time when the table is scheduled to be hard deleted.
    pub hard_delete_time: Option<Time>,
    pub field_family_mode: FieldFamilyMode,
}

impl TableDefinition {
    /// Create new empty `TableDefinition`
    pub fn new_empty(
        table_id: TableId,
        table_name: Arc<str>,
        field_family_mode: FieldFamilyMode,
    ) -> Self {
        Self::new(
            table_id,
            table_name,
            vec![],
            vec![],
            vec![],
            field_family_mode,
        )
        .expect("empty table should create without error")
    }

    /// Create a new [`TableDefinition`]
    ///
    /// Ensures the provided columns will be ordered before constructing the schema.
    pub fn new(
        table_id: TableId,
        table_name: Arc<str>,
        column_defs: Vec<ColumnDefinition>,
        field_family_defs: Vec<(FieldFamilyId, FieldFamilyName)>,
        series_key: Vec<TagId>,
        field_family_mode: FieldFamilyMode,
    ) -> Result<Self> {
        let mut field_families = Repository::new();
        let mut auto_field_family = None;
        let mut next_auto_field_family_name = 0;
        for (id, name) in field_family_defs {
            if let FieldFamilyName::Auto(v) = name
                && v > next_auto_field_family_name
            {
                // It is not possible for the next auto ID overflow, because there is a
                // limit of u16::MAX field families per table, and the auto ID starts at 0.
                next_auto_field_family_name = v.checked_add(1).expect("field family ID overflowed");
                auto_field_family = Some(id);
            }

            field_families
                .insert(id, FieldFamilyDefinition::new(id, name))
                .expect("field family definition should not exist");
        }

        // Create an empty schema.
        let mut schema_builder = SchemaBuilder::with_capacity(column_defs.len());
        schema_builder.measurement(table_name.as_ref());
        let schema = schema_builder.build().expect("schema should be valid");

        let mut table_def = Self {
            table_id,
            table_name,
            schema,
            columns: ColumnSet::new(),
            timestamp_column: None,
            tag_columns: Repository::new(),
            field_families,
            field_count: 0,
            auto_field_family,
            next_auto_field_family_name,
            series_key: vec![],
            series_key_names: vec![],
            sort_key: SortKey::empty(),
            last_caches: Repository::new(),
            distinct_caches: Repository::new(),
            deleted: false,
            hard_delete_time: None,
            field_family_mode,
        };

        let mut schema_builder = SchemaBuilder::with_capacity(column_defs.len());
        schema_builder.measurement(table_def.table_name.as_ref());

        table_def.add_columns_impl(&mut schema_builder, column_defs)?;

        table_def.series_key_names = series_key
            .clone()
            .into_iter()
            .map(|id| {
                table_def
                    .tag_columns
                    .get_by_id(&id)
                    .expect("tag column should exist by ID")
                    .name()
            })
            .collect::<Vec<Arc<str>>>();
        table_def.series_key = series_key;
        table_def.sort_key = Self::make_sort_key(
            &table_def.series_key_names,
            table_def.timestamp_column.is_some(),
        );

        schema_builder.with_series_key(&table_def.series_key_names);
        table_def.schema = schema_builder.build().expect("schema should be valid");

        Ok(table_def)
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
        Self::new(
            table_definition.table_id,
            Arc::clone(&table_definition.table_name),
            vec![],
            vec![],
            vec![],
            table_definition.field_family_mode,
        )
        .expect("tables defined from ops should not exceed column limits")
    }

    /// Check if the column name is available and not a reserved name.
    pub(crate) fn check_name(&self, name: impl AsRef<str>) -> Result<()> {
        let name_ref = name.as_ref();

        // Check if it's a reserved column name
        for reserved in RESERVED_COLUMN_NAMES {
            if name_ref == *reserved {
                return Err(CatalogError::ReservedColumn(Arc::from(name_ref)));
            }
        }

        // Check if column already exists
        if let Some(existing) = self.column_definition(name_ref) {
            Err(CatalogError::DuplicateColumn {
                name: existing.name(),
                existing: existing.column_type(),
            })
        } else {
            Ok(())
        }
    }

    fn update_field_families<'a>(
        mut table: Cow<'a, Self>,
        families: &Vec<FieldFamilyDefinitionLog>,
    ) -> Result<Cow<'a, Self>> {
        let mut new_families = Vec::with_capacity(families.len());
        for def in families {
            if table.field_families.get_by_id(&def.id).is_none() {
                new_families.push(FieldFamilyDefinition::new(def.id, def.name.clone()));
            }
        }

        if !new_families.is_empty() {
            let table = table.to_mut();
            for def in new_families {
                table.field_families.insert(def.id, def)?;
            }
        }

        Ok(table)
    }

    fn update_columns<'a>(
        mut table: Cow<'a, Self>,
        columns: &Vec<ColumnDefinitionLog>,
    ) -> Result<Cow<'a, Self>> {
        let mut new_columns: Vec<ColumnDefinition> = Vec::with_capacity(columns.len());
        for col_def in columns {
            if let Some(existing_type) = table
                .columns
                .get_by_id(&col_def.id())
                .map(|def| def.column_type())
            {
                if existing_type != col_def.column_type() {
                    return Err(CatalogError::FieldTypeMismatch {
                        table_name: table.table_name.to_string(),
                        column_name: col_def.name().to_string(),
                        existing: existing_type,
                        attempted: col_def.column_type(),
                    });
                }
            } else {
                new_columns.push(col_def.clone().into());
            }
        }

        if !new_columns.is_empty() {
            let table = table.to_mut();
            table.add_columns(new_columns)?;
        }
        Ok(table)
    }

    /// Check if the column exists in the [`TableDefinition`]
    pub fn column_exists(&self, column: impl AsRef<str>) -> bool {
        self.columns.contains_name(column.as_ref())
    }

    /// Add the columns to this [`TableDefinition`]
    ///
    /// This ensures that the resulting schema has its columns ordered
    pub(crate) fn add_columns(&mut self, new_columns: Vec<ColumnDefinition>) -> Result<()> {
        let mut schema_builder =
            SchemaBuilder::with_capacity(self.columns.len() + new_columns.len());
        schema_builder.measurement(self.table_name.as_ref());
        self.add_columns_impl(&mut schema_builder, new_columns)?;
        schema_builder.with_series_key(&self.series_key_names);
        let schema = schema_builder.build().expect("schema should be valid");
        self.schema = schema;

        Ok(())
    }

    fn add_columns_impl(
        &mut self,
        schema_builder: &mut SchemaBuilder,
        new_columns: Vec<ColumnDefinition>,
    ) -> Result<()> {
        let columns = &mut self.columns;
        let tag_columns = &mut self.tag_columns;
        let mut ffs = HashMap::<FieldFamilyId, FieldFamilyDefinition>::new();

        let mut sort_key_changed = false;

        for col in new_columns {
            let name = col.name();
            assert!(
                !columns.contains_name(&name),
                "attempted to add existing column"
            );

            columns.insert(col.clone())?;

            match col {
                ColumnDefinition::Timestamp(v) => {
                    assert!(
                        self.timestamp_column.is_none(),
                        "table definition initialized with multiple timestamp columns"
                    );
                    self.timestamp_column = Some(v);
                    sort_key_changed = true;
                }
                ColumnDefinition::Tag(v) => {
                    let id = v.id;
                    assert!(
                        tag_columns.insert(v.id, v).is_ok(),
                        "table definition initialized with duplicate tags"
                    );
                    self.series_key.push(id);
                    self.series_key_names.push(name);
                    sort_key_changed = true;
                }
                ColumnDefinition::Field(v) => {
                    // If the field family is not already in the map, insert it
                    if !ffs.contains_key(&v.id.0) {
                        let ff = self
                            .field_families
                            .repo
                            .get(&v.id.0)
                            // This function expects that new field families have already been added
                            .expect("field family ID should exist in the table definition")
                            .as_ref()
                            .clone();
                        ffs.insert(v.id.0, ff);
                    }
                    let ffd = ffs
                        .get_mut(&v.id.0)
                        .expect("field family should exist in the table definition");
                    ffd.fields.insert(v.id.1, v).expect("");
                    self.field_count += 1;
                }
            }
        }

        // Columns are always sorted by name.
        //
        // We use an unstable sort, as the ColumnSet guarantees no duplicate names,
        // so the order will always be consistent.
        columns
            .repo
            .sort_unstable_by(|_, a, _, b| a.name().cmp(&b.name()));

        for id in columns.repo.keys() {
            let col_def = columns
                .get_by_id(id)
                .expect("column should exist in the table definition");
            schema_builder.influx_column(col_def.name().as_ref(), col_def.column_type());
        }

        for (id, ff) in ffs {
            // Replace any updated field family definitions
            self.field_families.repo.insert(id, Arc::new(ff));
        }

        if sort_key_changed {
            self.sort_key = Self::make_sort_key(
                &self.series_key_names,
                self.columns.contains_id(&ColumnIdentifier::Timestamp),
            );
        }

        Ok(())
    }

    pub fn index_column_ids(&self) -> Vec<TagId> {
        self.tag_columns.repo.keys().cloned().collect()
    }

    pub fn influx_schema(&self) -> &Schema {
        &self.schema
    }

    pub fn num_columns(&self) -> usize {
        self.columns.len()
    }

    pub fn num_tag_columns(&self) -> usize {
        self.tag_columns.repo.len()
    }

    pub fn num_field_columns(&self) -> usize {
        self.field_count
    }

    pub fn num_field_families(&self) -> usize {
        self.field_families.len()
    }

    pub fn field_type_by_name(&self, name: impl AsRef<str>) -> Option<InfluxColumnType> {
        self.columns
            .get_by_name(name.as_ref())
            .map(|v| v.column_type())
    }

    pub fn column_id_to_name(&self, column_id: &ColumnIdentifier) -> Option<Arc<str>> {
        self.columns.get_by_id(column_id).map(|def| def.name())
    }

    pub fn column_name_to_id(&self, column_name: impl AsRef<str>) -> Option<ColumnIdentifier> {
        self.columns.name_to_id(column_name.as_ref())
    }

    pub fn column_name_to_id_unchecked(&self, column_name: impl AsRef<str>) -> ColumnIdentifier {
        self.columns
            .name_to_id(column_name.as_ref())
            .expect("column ID should exist in the table definition")
    }

    // TODO(trevor): remove thid API in favour of the Repository APIs
    pub fn column_definition(&self, name: impl AsRef<str>) -> Option<ColumnDefinition> {
        self.columns.get_by_name(name.as_ref()).cloned()
    }

    // TODO(trevor): remove thid API in favour of the Repository APIs
    pub fn column_definition_by_id(&self, id: &ColumnIdentifier) -> Option<ColumnDefinition> {
        self.columns.get_by_id(id).cloned()
    }

    pub fn tag_id_by_name(&self, name: impl AsRef<str>) -> Option<TagId> {
        self.tag_columns.name_to_id(name.as_ref())
    }

    pub fn series_key_ids(&self) -> &[TagId] {
        &self.series_key
    }

    pub fn series_key_names(&self) -> &[Arc<str>] {
        &self.series_key_names
    }
}

// APIs related to managing field families
impl TableDefinition {}

pub(crate) trait TableUpdate {
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

impl TableUpdate for AddColumnsLog {
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
        let table = TableDefinition::update_field_families(table, &self.field_family_definitions)?;
        TableDefinition::update_columns(table, &self.column_definitions)
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
pub enum AddColumnSpec {
    Timestamp,
    Tag(Arc<str>),
    Field(FieldFamilyId, Arc<str>, InfluxFieldType),
}

impl AddColumnSpec {
    pub fn timestamp() -> Self {
        Self::Timestamp
    }

    pub fn tag(name: impl Into<Arc<str>>) -> Self {
        Self::Tag(name.into())
    }

    pub fn field(
        ff_id: FieldFamilyId,
        name: impl Into<Arc<str>>,
        data_type: InfluxFieldType,
    ) -> Self {
        Self::Field(ff_id, name.into(), data_type)
    }
}

#[derive(Debug, Eq, PartialEq, Clone)]
pub enum ColumnDefinition {
    Timestamp(Arc<TimestampColumn>),
    Tag(Arc<TagColumn>),
    Field(Arc<FieldColumn>),
}

impl ColumnDefinition {
    pub fn timestamp(column_id: impl Into<ColumnId>) -> Self {
        Self::Timestamp(Arc::new(TimestampColumn {
            column_id: column_id.into(),
            name: TIME_COLUMN_NAME.into(),
        }))
    }

    pub fn tag(
        id: impl Into<TagId>,
        column_id: impl Into<ColumnId>,
        name: impl Into<Arc<str>>,
    ) -> Self {
        Self::Tag(Arc::new(TagColumn::new(id, column_id, name)))
    }

    pub fn field(
        id: impl Into<FieldIdentifier>,
        column_id: impl Into<ColumnId>,
        name: impl Into<Arc<str>>,
        data_type: InfluxFieldType,
    ) -> Self {
        Self::Field(Arc::new(FieldColumn::new(id, column_id, name, data_type)))
    }
}

impl ColumnDefinition {
    /// Return the identifier of the column.
    pub fn id(&self) -> ColumnIdentifier {
        match self {
            Self::Timestamp(_) => ColumnIdentifier::Timestamp,
            Self::Tag(v) => ColumnIdentifier::Tag(v.id),
            Self::Field(v) => ColumnIdentifier::Field(v.id),
        }
    }

    /// Return the column ordinal ID.
    pub fn ord_id(&self) -> ColumnId {
        match self {
            Self::Timestamp(v) => v.column_id,
            Self::Tag(v) => v.column_id,
            Self::Field(v) => v.column_id,
        }
    }

    /// Return the name of the column.
    pub fn name(&self) -> Arc<str> {
        match self {
            Self::Timestamp(v) => Arc::clone(&v.name),
            Self::Tag(v) => Arc::clone(&v.name),
            Self::Field(v) => Arc::clone(&v.name),
        }
    }

    /// Return the InfluxDB type of the column.
    pub fn column_type(&self) -> InfluxColumnType {
        match self {
            Self::Timestamp(_) => InfluxColumnType::Timestamp,
            Self::Tag(_) => InfluxColumnType::Tag,
            Self::Field(v) => InfluxColumnType::Field(v.data_type),
        }
    }

    /// Returns whether the column can hold `NULL` values.
    pub fn is_nullable(&self) -> bool {
        matches!(self, Self::Field(_) | Self::Tag(_))
    }
}

#[derive(Debug, Eq, PartialEq, Clone)]
pub struct TimestampColumn {
    pub column_id: ColumnId,
    pub name: Arc<str>,
}

impl TimestampColumn {
    pub fn new(column_id: impl Into<ColumnId>, name: impl Into<Arc<str>>) -> Self {
        Self {
            column_id: column_id.into(),
            name: name.into(),
        }
    }
}

impl From<Arc<TimestampColumn>> for ColumnDefinition {
    fn from(column: Arc<TimestampColumn>) -> Self {
        Self::Timestamp(column)
    }
}

#[derive(Debug, Eq, PartialEq, Clone)]
pub struct TagColumn {
    pub id: TagId,
    pub column_id: ColumnId,
    pub name: Arc<str>,
}

impl TagColumn {
    pub fn new(
        id: impl Into<TagId>,
        column_id: impl Into<ColumnId>,
        name: impl Into<Arc<str>>,
    ) -> Self {
        Self {
            id: id.into(),
            column_id: column_id.into(),
            name: name.into(),
        }
    }
}

impl From<Arc<TagColumn>> for ColumnDefinition {
    fn from(column: Arc<TagColumn>) -> Self {
        ColumnDefinition::Tag(column)
    }
}

#[derive(Debug, Eq, PartialEq, Clone)]
pub struct FieldColumn {
    pub id: FieldIdentifier,
    pub column_id: ColumnId,
    pub name: Arc<str>,
    pub data_type: InfluxFieldType,
}

impl FieldColumn {
    pub fn new(
        id: impl Into<FieldIdentifier>,
        column_id: impl Into<ColumnId>,
        name: impl Into<Arc<str>>,
        data_type: InfluxFieldType,
    ) -> Self {
        Self {
            id: id.into(),
            column_id: column_id.into(),
            name: name.into(),
            data_type,
        }
    }
}

impl From<Arc<FieldColumn>> for ColumnDefinition {
    fn from(column: Arc<FieldColumn>) -> Self {
        ColumnDefinition::Field(column)
    }
}

#[derive(Debug, Eq, PartialEq, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FieldFamilyName {
    /// A user-defined field family with a specific name
    User(Arc<str>),
    /// The Nth auto-generated field family
    Auto(u16),
}

/// Definition of a field family in the catalog for a specific table.
#[derive(Debug, Eq, PartialEq, Clone)]
pub struct FieldFamilyDefinition {
    pub id: FieldFamilyId,
    pub name: FieldFamilyName,
    pub fields: Repository<FieldId, FieldColumn>,
}

impl FieldFamilyDefinition {
    pub fn new(id: FieldFamilyId, name: FieldFamilyName) -> Self {
        Self {
            id,
            name,
            fields: Repository::new(),
        }
    }
}

use crate::log::versions::v4::{ColumnDefinitionLog, LastCacheTtl, LastCacheValueColumnsDef};
use crate::snapshot::versions::v4::{
    ColumnDefinitionSnapshot, ColumnSetSnapshot, DatabaseSnapshot, DistinctCacheSnapshot,
    FieldColumnSnapshot, FieldFamilySnapshot, GenerationConfigSnapshot, LastCacheSnapshot,
    NodeSnapshot, ProcessingEngineTriggerSnapshot, RetentionPeriodSnapshot, TableSnapshot,
    TagColumnSnapshot, TimestampColumnSnapshot,
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
            cli_params: self.cli_params.clone(),
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
            cli_params: snap.cli_params,
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
            field_families: self.field_families.snapshot(),
            last_caches: self.last_caches.snapshot(),
            distinct_caches: self.distinct_caches.snapshot(),
            deleted: self.deleted,
            hard_delete_time: self.hard_delete_time.as_ref().map(Time::timestamp_nanos),
            field_family_mode: self.field_family_mode,
        }
    }

    fn from_snapshot(snap: Self::Serialized) -> Self {
        let table_id = snap.table_id;
        // use the TableDefinition constructor here since it handles
        // Schema construction:
        let table_def = Self::new(
            table_id,
            snap.table_name,
            snap.columns.repo.iter().cloned().map(Into::into).collect(),
            snap.field_families
                .repo
                .values()
                .map(|s| (s.id, s.name.clone()))
                .collect(),
            snap.key,
            snap.field_family_mode,
        )
        .expect("serialized table definition from catalog should be valid");
        // ensure next col id is set from the snapshot incase we ever allow
        // hard-deletes:
        Self {
            table_id,
            table_name: table_def.table_name,
            schema: table_def.schema,
            columns: table_def.columns,
            timestamp_column: table_def.timestamp_column,
            tag_columns: table_def.tag_columns,
            field_families: table_def.field_families,
            field_count: table_def.field_count,
            auto_field_family: table_def.auto_field_family,
            next_auto_field_family_name: table_def.next_auto_field_family_name,
            series_key: table_def.series_key,
            series_key_names: table_def.series_key_names,
            sort_key: table_def.sort_key,
            last_caches: Repository::from_snapshot(snap.last_caches),
            distinct_caches: Repository::from_snapshot(snap.distinct_caches),
            deleted: snap.deleted,
            hard_delete_time: snap.hard_delete_time.map(Time::from_timestamp_nanos),
            field_family_mode: table_def.field_family_mode,
        }
    }
}

impl Snapshot for FieldFamilyDefinition {
    type Serialized = FieldFamilySnapshot;

    fn snapshot(&self) -> Self::Serialized {
        Self::Serialized {
            id: self.id,
            name: self.name.clone(),
        }
    }

    fn from_snapshot(snap: Self::Serialized) -> Self {
        Self {
            id: snap.id,
            name: snap.name,
            fields: Repository::new(),
        }
    }
}

impl Snapshot for ColumnSet {
    type Serialized = ColumnSetSnapshot;

    fn snapshot(&self) -> Self::Serialized {
        let mut repo = self.repo.values().map(|v| v.snapshot()).collect::<Vec<_>>();
        // Ensure consistent ordering of serialised state.
        repo.sort_unstable_by_key(|a| a.column_id());
        ColumnSetSnapshot {
            repo,
            next_id: self.next_id,
        }
    }

    fn from_snapshot(snap: Self::Serialized) -> Self {
        let mut repo = Self::new();

        for ser in snap.repo {
            repo.insert(ser)
                .expect("columns should contain no duplicates");
        }

        repo.next_id = snap.next_id;
        repo
    }
}

impl Snapshot for ColumnDefinition {
    type Serialized = ColumnDefinitionSnapshot;

    fn snapshot(&self) -> Self::Serialized {
        match self {
            ColumnDefinition::Timestamp(v) => ColumnDefinitionSnapshot::Timestamp(v.snapshot()),
            ColumnDefinition::Tag(v) => ColumnDefinitionSnapshot::Tag(v.snapshot()),
            ColumnDefinition::Field(v) => ColumnDefinitionSnapshot::Field(v.snapshot()),
        }
    }

    fn from_snapshot(snap: Self::Serialized) -> Self {
        match snap {
            ColumnDefinitionSnapshot::Timestamp(v) => {
                ColumnDefinition::Timestamp(Arc::new(TimestampColumn::from_snapshot(v)))
            }
            ColumnDefinitionSnapshot::Tag(v) => {
                ColumnDefinition::Tag(Arc::new(TagColumn::from_snapshot(v)))
            }
            ColumnDefinitionSnapshot::Field(v) => {
                ColumnDefinition::Field(Arc::new(FieldColumn::from_snapshot(v)))
            }
        }
    }
}

impl Snapshot for TimestampColumn {
    type Serialized = TimestampColumnSnapshot;

    fn snapshot(&self) -> Self::Serialized {
        TimestampColumnSnapshot {
            column_id: self.column_id,
            name: Arc::clone(&self.name),
        }
    }

    fn from_snapshot(snap: Self::Serialized) -> Self {
        Self {
            column_id: snap.column_id,
            name: Arc::clone(&snap.name),
        }
    }
}

impl Snapshot for TagColumn {
    type Serialized = TagColumnSnapshot;

    fn snapshot(&self) -> Self::Serialized {
        Self::Serialized {
            id: self.id,
            column_id: self.column_id,
            name: Arc::clone(&self.name),
        }
    }

    fn from_snapshot(snap: Self::Serialized) -> Self {
        Self {
            id: snap.id,
            column_id: snap.column_id,
            name: snap.name,
        }
    }
}

impl Snapshot for FieldColumn {
    type Serialized = FieldColumnSnapshot;

    fn snapshot(&self) -> Self::Serialized {
        FieldColumnSnapshot {
            id: self.id,
            column_id: self.column_id,
            name: Arc::clone(&self.name),
            data_type: self.data_type.into(),
        }
    }

    fn from_snapshot(snap: Self::Serialized) -> Self {
        Self {
            id: snap.id,
            column_id: snap.column_id,
            name: snap.name,
            data_type: InfluxFieldType::from(snap.data_type),
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

use crate::snapshot::versions::v4::{
    ActionsSnapshot, CrudActionsSnapshot, DatabaseActionsSnapshot, NodeStateSnapshot,
    PermissionSnapshot, RepositorySnapshot, ResourceIdentifierSnapshot, ResourceTypeSnapshot,
    TokenInfoSnapshot,
};
// Enterprise import removed - hydrate_token_permissions
use influxdb3_authz::{Actions, ResourceType};

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

fn database_or_deletion_status(
    db_schema: Option<Arc<DatabaseSchema>>,
    time_provider: &Arc<dyn TimeProvider>,
) -> Result<Arc<DatabaseSchema>, DeletionStatus> {
    match db_schema {
        Some(db_schema) if db_schema.deleted => Err(db_schema
            .hard_delete_time
            .and_then(|time| {
                time_provider
                    .now()
                    .checked_duration_since(time)
                    .map(DeletionStatus::Hard)
            })
            .unwrap_or(DeletionStatus::Soft)),
        Some(db_schema) => Ok(db_schema),
        None => Err(DeletionStatus::NotFound),
    }
}

fn table_deletion_status(
    db_schema: &DatabaseSchema,
    table_id: TableId,
    time_provider: &dyn TimeProvider,
) -> Option<DeletionStatus> {
    match db_schema.tables.get_by_id(&table_id) {
        Some(table_def) if table_def.deleted => Some(
            table_def
                .hard_delete_time
                .and_then(|time| {
                    time_provider
                        .now()
                        .checked_duration_since(time)
                        .map(DeletionStatus::Hard)
                })
                .unwrap_or(DeletionStatus::Soft),
        ),
        Some(_) => None,
        None => Some(DeletionStatus::NotFound),
    }
}

#[cfg(test)]
mod tests;
