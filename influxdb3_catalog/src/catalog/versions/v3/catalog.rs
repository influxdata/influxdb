//! Outer Catalog wrapper that drives the prepare → persist → apply → broadcast
//! flow for v3 catalog mutations.

use std::collections::BTreeMap;
use std::num::NonZeroU32;
use std::ops::{Add, Deref};
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use backon::{ExponentialBuilder, Retryable};
use data_types::Namespace;
use hashbrown::HashMap;
use influxdb3_authz::permissions::{
    PermissionAttributes, PermissionDetailsSpec, ResourceNameToIdProvider,
    TokenPermissionResourceIdentifier,
};
use influxdb3_authz::role::Role;
use influxdb3_authz::{Actions, ResourceIdentifier, ResourceType, TokenInfo};
use influxdb3_id::{DbId, NodeId, QueryGroupId, RoleId, TableId, TagId, TriggerId, UserId};
use influxdb3_process::ProcessUuidGetter;
use influxdb3_shutdown::ShutdownToken;
use influxdb3_wal::SnapshotSequenceNumber;
use iox_time::{Time, TimeProvider};
use metric::Registry;
use object_store::ObjectStore;
use object_store::path::Path as ObjectStorePath;
use observability_deps::tracing::{debug, error, info, warn};
use parking_lot::{Mutex as ParkingMutex, RwLock};
use tokio::sync::{Mutex, RwLock as AsyncRwLock, Semaphore};
use tokio::task::JoinHandle;
use uuid::Uuid;

use super::schema::user::{LoginIdentityUsernamePassword, RefreshTokenInfo, UserInfo};
use crate::catalog::migrations::v3::check_and_migrate_v2_to_v3;
use crate::catalog::versions::v3::backup::{CatalogBackupView, CatalogRestoreSource};
use crate::catalog::versions::v3::deletes::DeletionScope;
use crate::catalog::versions::v3::events::{
    CatalogEvent, CatalogSubscriptions, CatalogUpdate, CatalogUpdateReceiver,
};
use crate::catalog::versions::v3::inner::InnerCatalog;
use crate::catalog::versions::v3::ops::CatalogOp;
use crate::catalog::versions::v3::ops::cache::{
    CreateDistinctCacheArgs, CreateDistinctCacheOp, CreateLastCacheArgs, CreateLastCacheOp,
    DeleteDistinctCacheArgs, DeleteDistinctCacheOp, DeleteLastCacheArgs, DeleteLastCacheOp,
};
use crate::catalog::versions::v3::ops::config::{
    DowngradeStorageModeOp, SetAllGenerationDurationsArgs, SetAllGenerationDurationsOp,
    SetGenerationDurationArgs, SetGenerationDurationOp, SetStorageModeArgs, SetStorageModeOp,
};
use crate::catalog::versions::v3::ops::database::{
    CreateDatabaseArgs, CreateDatabaseOp, HardDeleteDatabaseArgs, HardDeleteDatabaseOp,
    SoftDeleteDatabaseArgs, SoftDeleteDatabaseOp,
};
use crate::catalog::versions::v3::ops::node::{
    AckStopNodeArgs, AckStopNodeOp, RegisterNodeArgs, RegisterNodeOp, RemoveNodeArgs, RemoveNodeOp,
    RequestStopNodeArgs, RequestStopNodeOp, StopNodeArgs, StopNodeOp, UnregisterNodeArgs,
    UnregisterNodeOp,
};
use crate::catalog::versions::v3::ops::query_group::{
    CreateQueryGroupArgs, CreateQueryGroupOp, DeleteQueryGroupArgs, DeleteQueryGroupOp,
    QueryGroupMemberMutation, UpdateQueryGroupArgs, UpdateQueryGroupMembersArgs,
    UpdateQueryGroupMembersOp, UpdateQueryGroupOp,
};
pub use crate::catalog::versions::v3::ops::restore::RestoreReport;
use crate::catalog::versions::v3::ops::restore::{RestoreOp, RestoreOpArgs};
use crate::catalog::versions::v3::ops::retention::{
    ClearDbRetentionPeriodArgs, ClearDbRetentionPeriodOp, ClearTableRetentionPeriodArgs,
    ClearTableRetentionPeriodOp, SetDbRetentionPeriodArgs, SetDbRetentionPeriodOp,
    SetTableRetentionPeriodArgs, SetTableRetentionPeriodOp,
};
use crate::catalog::versions::v3::ops::role::{
    CreateRoleArgs, CreateRoleOp, DeleteRoleArgs, DeleteRoleOp, UpdateRoleArgs, UpdateRoleOp,
    UpdateRolePermissionsArgs, UpdateRolePermissionsOp,
};
use crate::catalog::versions::v3::ops::table::{
    HardDeleteTableArgs, HardDeleteTableOp, SoftDeleteTableArgs, SoftDeleteTableOp,
};
use crate::catalog::versions::v3::ops::token::{
    CreateAdminTokenArgs, CreateAdminTokenOp, CreateResourceScopedTokenArgs,
    CreateResourceScopedTokenOp, DeleteTokenArgs, DeleteTokenOp, RegenerateAdminTokenArgs,
    RegenerateAdminTokenOp,
};
use crate::catalog::versions::v3::ops::trigger::{
    CreateTriggerArgs, CreateTriggerOp, DeleteTriggerArgs, DeleteTriggerOp, DisableTriggerArgs,
    DisableTriggerOp, EnableTriggerArgs, EnableTriggerOp,
};
use crate::catalog::versions::v3::ops::user::{
    CreateLoginIdentityOAuthArgs, CreateLoginIdentityOAuthOp,
    CreateLoginIdentityUsernamePasswordArgs, CreateLoginIdentityUsernamePasswordOp,
    CreateRefreshTokenArgs, CreateRefreshTokenOp, CreateUserArgs, CreateUserOp,
    DeleteLoginIdentityOAuthArgs, DeleteLoginIdentityOAuthOp, DeleteUserArgs, DeleteUserOp,
    RestoreUserArgs, RestoreUserOp, RevokeAllRefreshTokensForUserArgs,
    RevokeAllRefreshTokensForUserOp, RevokeRefreshTokenArgs, RevokeRefreshTokenOp,
    UpdateLoginIdentityPasswordHashArgs, UpdateLoginIdentityPasswordHashOp,
    UpdateLoginIdentityRequiresPasswordResetArgs, UpdateLoginIdentityRequiresPasswordResetOp,
    UpdateUserDisplayNameArgs, UpdateUserDisplayNameOp, UpdateUserRolesArgs, UpdateUserRolesOp,
};
use crate::catalog::versions::v3::schema::cache::{
    DistinctCacheDefinition, LastCacheDefinition, LastCacheSize, LastCacheTtl, MaxAge,
    MaxCardinality, RefreshInterval,
};
use crate::catalog::versions::v3::schema::column::{FieldDataType, FieldFamilyMode};
use crate::catalog::versions::v3::schema::database::DatabaseSchema;
use crate::catalog::versions::v3::schema::node::{NodeDefinition, NodeMode, NodeModes, NodeSpec};
use crate::catalog::versions::v3::schema::query_group::{
    QueryGroupDefinition, QueryGroupInsertPosition, QueryGroupUpdate,
};
use crate::catalog::versions::v3::schema::storage::{GenerationConfig, StorageMode};
use crate::catalog::versions::v3::schema::table::TableDefinition;
use crate::catalog::versions::v3::schema::tokens::create_token_and_hash;
use crate::catalog::versions::v3::schema::trigger::{
    TriggerDefinition, TriggerSettings, TriggerSpecificationDefinition, ValidPluginFilename,
};
use crate::catalog::versions::v3::transaction::{
    CatalogTransaction, DatabaseCatalogTransaction, Prompt,
};
use crate::catalog::versions::v3::usage::{CatalogLimiter, CatalogLimits};
use crate::catalog::{
    CatalogSequenceNumber, DEFAULT_OPERATOR_TOKEN_NAME, DeletionStatus, INTERNAL_DB_NAME,
};
use crate::format::FeatureLevel;
use crate::format::Record;
use crate::format::RecordBatch;
use crate::format::apply::{
    RestorePreload, apply_catalog_file, apply_records, preload_restore_for_file,
    preload_restore_for_records, serialize_log_file, serialize_snapshot_file,
};
use crate::format::derive_feature_level;
use crate::format::records::CreateDatabase;
use crate::format::records::types::Actions as WireActions;
use crate::format::records::types::Permission as WirePermission;
use crate::format::records::types::ResourceIdentifier as WireResourceIdentifier;
use crate::format::records::types::ResourceType as WireResourceType;
use crate::format::records::types::RetentionPeriod as WireRetentionPeriod;
use crate::object_store::PersistCatalogResult;
use crate::object_store::versions::v3::{CatalogLoad, ObjectStoreCatalog};
use crate::resource::CatalogResource;
use crate::{CatalogError, Result};
use influxdb3_wal::CatalogSnapshotObserver;

/// Resolves to the wall-clock time at which a soft-deleted resource may be
/// hard-deleted.
#[derive(Clone, Copy, Debug)]
pub enum HardDeletionTime {
    /// The resource will never be hard deleted.
    Never,
    /// The resource will be hard deleted after the catalog's default
    /// duration has elapsed.
    Default,
    /// The resource will be hard deleted at a specific timestamp.
    Timestamp(Time),
    /// The resource will be hard deleted as soon as possible.
    Now,
}

impl HardDeletionTime {
    fn as_time(self, time_provider: &dyn TimeProvider, default: Duration) -> Option<Time> {
        match self {
            HardDeletionTime::Never => None,
            HardDeletionTime::Default => Some(time_provider.now().add(default)),
            HardDeletionTime::Timestamp(time) => Some(time),
            HardDeletionTime::Now => Some(time_provider.now()),
        }
    }

    fn is_default(self) -> bool {
        matches!(self, HardDeletionTime::Default)
    }
}

impl std::fmt::Display for HardDeletionTime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            HardDeletionTime::Never => write!(f, "never"),
            HardDeletionTime::Default => write!(f, "default"),
            HardDeletionTime::Timestamp(time) => write!(f, "{time}"),
            HardDeletionTime::Now => write!(f, "now"),
        }
    }
}

/// Construction-time configuration for [`Catalog`].
#[derive(Clone, Copy, Debug)]
pub struct CatalogArgs {
    pub default_hard_delete_duration: Duration,
    pub storage_mode: StorageMode,
    pub shard_count: NonZeroU32,
}

impl CatalogArgs {
    pub fn new(default_hard_delete_duration: Duration) -> Self {
        Self {
            default_hard_delete_duration,
            ..Default::default()
        }
    }
}

impl Default for CatalogArgs {
    fn default() -> Self {
        Self {
            default_hard_delete_duration: Catalog::DEFAULT_HARD_DELETE_DURATION,
            storage_mode: StorageMode::Parquet,
            // `NonZeroU32::MIN` is `1`.
            shard_count: NonZeroU32::MIN,
        }
    }
}

/// Builder API for the [`Catalog`].
///
/// # Note
///
/// This is meant for testing; see also: [`Catalog::builder_testing`]
#[cfg(any(test, feature = "test_helpers"))]
#[derive(Default)]
pub struct CatalogBuilder {
    catalog_id: Arc<str>,
    object_store: Option<Arc<dyn ObjectStore>>,
    time_provider: Option<Arc<dyn TimeProvider>>,
    metric_registry: Option<Arc<Registry>>,
}

#[cfg(any(test, feature = "test_helpers"))]
impl std::fmt::Debug for CatalogBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CatalogBuilder")
            .field("catalog_id", &self.catalog_id)
            .finish_non_exhaustive()
    }
}

#[cfg(any(test, feature = "test_helpers"))]
impl CatalogBuilder {
    pub fn catalog_id(mut self, catalog_id: impl Into<Arc<str>>) -> Self {
        self.catalog_id = catalog_id.into();
        self
    }

    pub fn object_store(mut self, object_store: Arc<dyn ObjectStore>) -> Self {
        self.object_store.replace(object_store);
        self
    }

    pub fn time_provider(mut self, time_provider: Arc<dyn TimeProvider>) -> Self {
        self.time_provider.replace(time_provider);
        self
    }

    pub fn metric_registry(mut self, registry: Arc<Registry>) -> Self {
        self.metric_registry.replace(registry);
        self
    }

    pub async fn build(self) -> Result<Arc<Catalog>> {
        use iox_time::MockProvider;
        use object_store::memory::InMemory;

        let store = self
            .object_store
            .unwrap_or_else(|| Arc::new(InMemory::new()));
        let time_provider = self
            .time_provider
            .unwrap_or_else(|| Arc::new(MockProvider::new(Time::from_timestamp_nanos(0))));
        let metric_registry = self.metric_registry.unwrap_or_default();
        Catalog::new(self.catalog_id, store, time_provider, metric_registry).await
    }
}

#[derive(Default, Debug, Clone, Copy)]
pub struct CreateDatabaseOptions {
    pub retention_period: Option<Duration>,
}

impl CreateDatabaseOptions {
    pub fn retention_period(mut self, retention_period: Duration) -> Self {
        self.retention_period = Some(retention_period);
        self
    }
}

#[derive(Default, Debug, Clone, Copy)]
pub struct CreateTableOptions {
    pub retention_period: Option<Duration>,
    pub field_family_mode: FieldFamilyMode,
}

impl CreateTableOptions {
    pub fn retention_period(mut self, retention_period: Duration) -> Self {
        self.retention_period = Some(retention_period);
        self
    }

    pub fn field_family_mode(mut self, mode: FieldFamilyMode) -> Self {
        self.field_family_mode = mode;
        self
    }
}

/// Operator-facing node selector for cache and trigger creation.
#[derive(Debug, Default, Clone, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ApiNodeSpec {
    #[default]
    All,
    /// List of node _names_, i.e., user-provided `--node-id` for each node.
    Nodes(Vec<String>),
}

impl std::str::FromStr for ApiNodeSpec {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s {
            "all" => Ok(Self::All),
            s if s.starts_with("nodes") => {
                let (_, node_str) = s
                    .split_once(":")
                    .ok_or_else(|| anyhow::Error::msg("unsupported node spec format"))?;
                Ok(Self::Nodes(
                    node_str.split(",").map(|s| s.to_string()).collect(),
                ))
            }
            _ => Err(anyhow::Error::msg("unsupported node spec format")),
        }
    }
}

impl std::fmt::Display for ApiNodeSpec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ApiNodeSpec::All => write!(f, "all"),
            ApiNodeSpec::Nodes(vec) => write!(f, "nodes:{}", vec.join(",")),
        }
    }
}

/// Tuning for background snapshot checkpointing.
///
/// A checkpoint is triggered after each successful log persist whenever
/// EITHER threshold has elapsed since the last successful checkpoint:
/// `log_interval` catalog sequences, or `time_interval` of wall-clock time.
#[derive(Debug, Clone, Copy)]
pub(crate) struct CheckpointPolicy {
    pub(crate) log_interval: u64,
    pub(crate) time_interval: Duration,
}

impl Default for CheckpointPolicy {
    fn default() -> Self {
        Self {
            log_interval: 100,
            time_interval: Duration::from_secs(3600),
        }
    }
}

/// Catalog lifecycle state. Set to [`CatalogState::Shutdown`] by
/// [`Catalog::set_state_shutdown`] when the owning node is being shutdown.
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

/// Marks the last successful background checkpoint for threshold comparisons.
#[derive(Debug, Clone, Copy)]
struct Checkpoint {
    sequence: CatalogSequenceNumber,
    when: Instant,
}

impl Checkpoint {
    fn elapsed(&self) -> Duration {
        self.when.elapsed()
    }
}

/// A committed catalog write: the operation's output paired with the
/// sequence number it was applied at and the timestamp of that apply.
#[derive(Debug, Clone)]
pub struct Committed<T> {
    pub output: T,
    pub sequence: CatalogSequenceNumber,
    pub time_ns: i64,
}

pub struct Catalog {
    inner: RwLock<InnerCatalog>,
    subscriptions: Arc<AsyncRwLock<CatalogSubscriptions>>,
    write_permit: Mutex<CatalogSequenceNumber>,
    store: ObjectStoreCatalog,
    catalog_uuid: Uuid,
    time_provider: Arc<dyn TimeProvider>,
    metric_registry: Arc<Registry>,
    shard_count: NonZeroU32,
    default_hard_delete_duration: Duration,
    /// Node id of the process this catalog is running in. `None` for
    /// configurations without a distinct node identity (Core).
    current_node_id: Option<Arc<str>>,
    limits: Arc<dyn CatalogLimiter>,
    state: ParkingMutex<CatalogState>,
    checkpoint_policy: CheckpointPolicy,
    last_checkpoint: Arc<ParkingMutex<Checkpoint>>,
    /// Single-permit semaphore that keeps at most one background checkpoint
    /// task in flight; the owned permit is dropped when the task returns.
    checkpoint_slot: Arc<Semaphore>,
}

impl std::fmt::Debug for Catalog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Catalog")
            .field("catalog_uuid", &self.catalog_uuid)
            .finish_non_exhaustive()
    }
}

impl Catalog {
    /// Maximum number of columns total across all tables (enterprise cap).
    pub const MAX_TOTAL_COLUMNS: u32 = 10_000_000;
    /// Default duration used to resolve hard-delete time when a constructor
    /// caller doesn't supply an explicit value.
    pub const DEFAULT_HARD_DELETE_DURATION: Duration = Duration::from_secs(60 * 60 * 72);

    /// Construct a `Catalog` for Core. `node_id` is used as both the object-store
    /// prefix and the current node identifier.
    pub async fn new(
        node_id: impl Into<Arc<str>>,
        store: Arc<dyn ObjectStore>,
        time_provider: Arc<dyn TimeProvider>,
        metric_registry: Arc<Registry>,
    ) -> Result<Arc<Self>> {
        Self::new_with_args(
            node_id,
            store,
            time_provider,
            metric_registry,
            CatalogArgs::default(),
            CatalogLimits::none(),
        )
        .await
    }

    /// Construct `Catalog` with [`CatalogArgs`] and [`CatalogLimits`].
    pub async fn new_with_args(
        node_id: impl Into<Arc<str>>,
        store: Arc<dyn ObjectStore>,
        time_provider: Arc<dyn TimeProvider>,
        metric_registry: Arc<Registry>,
        args: CatalogArgs,
        limits: CatalogLimits,
    ) -> Result<Arc<Self>> {
        Self::load_or_create(
            node_id,
            None,
            store,
            time_provider,
            metric_registry,
            Arc::new(limits),
            args,
        )
        .await
        .map(Arc::new)
    }

    /// Construct `Catalog` and initiate a background task that flips the registered
    /// node to `Stopped` when `shutdown_token` is cancelled.
    pub async fn new_with_shutdown(
        node_id: impl Into<Arc<str>>,
        store: Arc<dyn ObjectStore>,
        time_provider: Arc<dyn TimeProvider>,
        metric_registry: Arc<Registry>,
        shutdown_token: ShutdownToken,
        process_uuid_getter: Arc<dyn ProcessUuidGetter>,
        limits: CatalogLimits,
    ) -> Result<Arc<Self>> {
        let node_id = node_id.into();

        let catalog = Self::new_with_args(
            Arc::clone(&node_id),
            store,
            time_provider,
            metric_registry,
            CatalogArgs::default(),
            limits,
        )
        .await?;
        let catalog_for_shutdown = Arc::clone(&catalog);
        tokio::spawn(async move {
            shutdown_token.wait_for_shutdown().await;
            if catalog_for_shutdown.state.lock().is_shutdown() {
                return;
            }
            match catalog_for_shutdown
                .update_node_state_stopped(node_id.as_ref(), process_uuid_getter)
                .await
            {
                Ok(_) => {
                    info!(
                        node_id = node_id.as_ref(),
                        "successfully updated node to stopped in catalog"
                    );
                }
                error @ Err(CatalogError::NodeAlreadyStopped { .. }) => {
                    info!(
                        ?error,
                        node_id = node_id.as_ref(),
                        "node is already stopped for this process, do not need to update catalog"
                    );
                }
                Err(error) => {
                    error!(
                        ?error,
                        node_id = node_id.as_ref(),
                        "encountered error while updating node to stopped state in catalog"
                    );
                }
            }
        });
        Ok(catalog)
    }

    /// Create a [`CatalogBuilder`] for assembling a `Catalog`.
    ///
    /// # Implementation Note
    ///
    /// This is intended for tests.
    #[cfg(any(test, feature = "test_helpers"))]
    pub fn builder_testing(catalog_id: impl Into<Arc<str>>) -> CatalogBuilder {
        CatalogBuilder::default().catalog_id(catalog_id)
    }

    /// Construct a `Catalog` backed by an in-memory object store and a
    /// `MockProvider` clock fixed at epoch.
    ///
    /// # Implementation Note
    ///
    /// This is intended for tests.
    #[cfg(any(test, feature = "test_helpers"))]
    pub async fn new_in_memory(catalog_id: impl Into<Arc<str>>) -> Result<Arc<Self>> {
        use iox_time::MockProvider;
        use object_store::memory::InMemory;

        let store = Arc::new(InMemory::new());
        let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
        let metric_registry = Default::default();
        Self::new(catalog_id, store, time_provider, metric_registry).await
    }

    #[cfg(any(test, feature = "test_helpers"))]
    pub async fn new_in_memory_with_limits(
        catalog_id: impl Into<Arc<str>>,
        limits: CatalogLimits,
    ) -> Result<Arc<Self>> {
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

    #[cfg(any(test, feature = "test_helpers"))]
    pub async fn new_in_memory_with_args(
        catalog_id: impl Into<Arc<str>>,
        time_provider: Arc<dyn TimeProvider>,
        args: CatalogArgs,
    ) -> Result<Arc<Self>> {
        Self::new_in_memory_with_args_limits(catalog_id, time_provider, args, CatalogLimits::none())
            .await
    }

    #[cfg(any(test, feature = "test_helpers"))]
    pub async fn new_in_memory_with_args_limits(
        catalog_id: impl Into<Arc<str>>,
        time_provider: Arc<dyn TimeProvider>,
        args: CatalogArgs,
        limits: CatalogLimits,
    ) -> Result<Arc<Self>> {
        use object_store::memory::InMemory;

        let store = Arc::new(InMemory::new());
        let metric_registry = Default::default();
        Self::new_with_args(
            catalog_id,
            store,
            time_provider,
            metric_registry,
            args,
            limits,
        )
        .await
    }

    /// Register `count` number of query nodes and
    /// return their catalog-assigned node IDs.
    ///
    /// Nodes are named `query-node-0`, `query-node-1`, … and assigned instance
    /// IDs `inst-query-0`, `inst-query-1`, … with 4 cores each in `Query` mode.
    ///
    /// This is intended for tests.
    #[cfg(any(test, feature = "test_helpers"))]
    pub async fn register_query_nodes(&self, count: usize) -> Vec<influxdb3_id::NodeId> {
        use influxdb3_process::ProcessUuidWrapper;

        let mut ids = Vec::with_capacity(count);
        for i in 0..count {
            let node = self
                .register_node(
                    &format!("query-node-{i}"),
                    4,
                    vec![NodeMode::Query],
                    Arc::new(ProcessUuidWrapper::new()),
                    Arc::from(format!("inst-query-{i}")),
                    None,
                    None,
                    0,
                )
                .await
                .expect("register node");
            ids.push(node.node_catalog_id());
        }
        ids
    }

    /// Register a single ingester node.
    ///
    /// This is intended for tests.
    #[cfg(any(test, feature = "test_helpers"))]
    pub async fn register_ingester_node(
        &self,
    ) -> Arc<crate::catalog::versions::v3::schema::node::NodeDefinition> {
        use influxdb3_process::ProcessUuidWrapper;

        self.register_node(
            "ingest-node",
            4,
            vec![NodeMode::Ingest],
            Arc::new(ProcessUuidWrapper::new()),
            Arc::from("inst-ingest"),
            None,
            None,
            0,
        )
        .await
        .expect("register ingest node")
    }

    pub async fn new_with_store(
        catalog_id: impl Into<Arc<str>>,
        store: Arc<dyn ObjectStore>,
        time_provider: Arc<dyn TimeProvider>,
    ) -> Result<Arc<Self>> {
        let metric_registry = Default::default();
        Self::new(catalog_id, store, time_provider, metric_registry).await
    }

    pub async fn new_with_checkpoint_interval(
        catalog_id: impl Into<Arc<str>>,
        store: Arc<dyn ObjectStore>,
        time_provider: Arc<dyn TimeProvider>,
        metric_registry: Arc<Registry>,
        checkpoint_interval: u64,
    ) -> Result<Self> {
        let policy = CheckpointPolicy {
            log_interval: checkpoint_interval,
            ..CheckpointPolicy::default()
        };
        let catalog_id: Arc<str> = catalog_id.into();
        let store = ObjectStoreCatalog::new(catalog_id, store, StorageMode::default());
        let inner = store.load_or_create_catalog().await?.inner;
        Ok(Self::from_parts(
            inner,
            store,
            None,
            time_provider,
            metric_registry,
            NonZeroU32::MIN,
            Self::DEFAULT_HARD_DELETE_DURATION,
            policy,
            Arc::new(CatalogLimits::none()),
        ))
    }

    /// Bootstrap a `Catalog` from an object store: load the existing v3
    /// catalog at `catalog_id`, or initialize a fresh one with `storage_mode`
    /// if none exists.
    ///
    /// Fails fast if the loaded catalog's committed feature level exceeds
    /// this binary's local level — the cluster has advanced past what this
    /// node understands and an upgrade is required before the node can
    /// participate. The check runs before any `RegisterNode` is written so
    /// stale binaries cannot rejoin the cluster.
    pub async fn load_or_create(
        catalog_id: impl Into<Arc<str>>,
        current_node_id: Option<Arc<str>>,
        store: Arc<dyn ObjectStore>,
        time_provider: Arc<dyn TimeProvider>,
        metric_registry: Arc<Registry>,
        limits: Arc<dyn CatalogLimiter>,
        args: CatalogArgs,
    ) -> Result<Self> {
        use influxdb3_wal::NoopCatalogSnapshotObserver;
        Self::load_or_create_with_observer(
            catalog_id,
            current_node_id,
            store,
            time_provider,
            metric_registry,
            limits,
            args,
            Arc::new(NoopCatalogSnapshotObserver),
        )
        .await
    }

    /// Same as [`Self::load_or_create`] but threads a
    /// [`CatalogSnapshotObserver`] through to the underlying store so SLL
    /// emissions fire on initial-snapshot and periodic-checkpoint persists.
    /// Used by the enterprise binary; OSS-style callers use
    /// [`Self::load_or_create`] which supplies a noop observer.
    #[allow(clippy::too_many_arguments)]
    async fn load_or_create_with_observer(
        catalog_id: impl Into<Arc<str>>,
        current_node_id: Option<Arc<str>>,
        store: Arc<dyn ObjectStore>,
        time_provider: Arc<dyn TimeProvider>,
        metric_registry: Arc<Registry>,
        limits: Arc<dyn CatalogLimiter>,
        args: CatalogArgs,
        catalog_snapshot_observer: Arc<dyn CatalogSnapshotObserver>,
    ) -> Result<Self> {
        let catalog_id: Arc<str> = catalog_id.into();

        // If a v2 catalog is present at this prefix and no v3 snapshot
        // has been written yet, migrate v2 → v3.
        (async || check_and_migrate_v2_to_v3(Arc::clone(&catalog_id), Arc::clone(&store)).await)
            .retry(
                ExponentialBuilder::new()
                    .with_max_delay(Duration::from_secs(1))
                    .with_max_times(5),
            )
            .when(|e| e.is_retryable())
            .notify(|error, dur| warn!(?error, "v2→v3 migration retry in {dur:?}"))
            .await
            .map_err(|e| CatalogError::Internal {
                details: format!("v2→v3 migration failed: {e}"),
            })?;

        let store = ObjectStoreCatalog::new(catalog_id, store, args.storage_mode)
            .with_catalog_snapshot_observer(catalog_snapshot_observer);
        let CatalogLoad {
            inner,
            snapshot_needs_rewrite,
        } = store.load_or_create_catalog().await?;

        let local = derive_feature_level();
        let committed = inner.committed_feature_level;
        // Fail on `Less` or incomparable: both mean the cluster
        // understands at least one record id this binary doesn't.
        match local.partial_cmp(&committed) {
            Some(std::cmp::Ordering::Greater | std::cmp::Ordering::Equal) => {}
            Some(std::cmp::Ordering::Less) | None => {
                return Err(CatalogError::NodeBelowCommittedFeatureLevel { committed, local });
            }
        }

        let catalog = Self::from_parts(
            inner,
            store,
            current_node_id,
            time_provider,
            metric_registry,
            args.shard_count,
            args.default_hard_delete_duration,
            CheckpointPolicy::default(),
            limits,
        );

        // Snapshots in a non-current layout — legacy multi-group, or the
        // indexless flat form that pre-flat readers reject — still load, but
        // the file itself should not linger on object store: rewrite it in
        // the current single-group format now rather than waiting for the
        // next organic checkpoint. `force_checkpoint` snapshots the live
        // state, so any log files replayed on top of the snapshot are folded
        // in as well. Failure is non-fatal — startup proceeds and the next
        // checkpoint retires the file instead.
        if snapshot_needs_rewrite {
            match catalog.force_checkpoint().await {
                Ok(()) => info!("rewrote catalog snapshot in current format"),
                Err(error) => warn!(
                    ?error,
                    "failed to rewrite non-current catalog snapshot at startup; \
                     it will be retired by the next checkpoint"
                ),
            }
        }

        // Mirror v2: ensure the `_internal` system database exists. v2
        // calls `create_internal_db` from every public constructor; v3
        // does it once here in the canonical entry point.
        catalog.create_internal_db_if_missing().await;

        Ok(catalog)
    }

    /// Idempotently create the `_internal` system database with the
    /// long-lived retention period that hosts catalog system tables.
    async fn create_internal_db_if_missing(&self) {
        use crate::catalog::INTERNAL_DB_RETENTION_PERIOD;
        let result = self
            .create_database_opts(
                INTERNAL_DB_NAME,
                CreateDatabaseOptions::default().retention_period(INTERNAL_DB_RETENTION_PERIOD),
            )
            .await;
        match result {
            Ok(_) => info!("created internal database"),
            Err(CatalogError::AlreadyExists) => {
                debug!("internal database already exists");
            }
            Err(err) => {
                warn!(?err, "failed to create internal database");
            }
        }
    }

    /// Construct a `Catalog` for a node in an Enterprise cluster.
    pub async fn new_enterprise(
        current_node_id: impl Into<Arc<str>>,
        catalog_id: impl Into<Arc<str>>,
        store: Arc<dyn ObjectStore>,
        time_provider: Arc<dyn TimeProvider>,
        metric_registry: Arc<Registry>,
        limits: Arc<dyn CatalogLimiter>,
        args: CatalogArgs,
    ) -> Result<Arc<Self>> {
        let current_node_id: Arc<str> = current_node_id.into();
        let catalog_id: Arc<str> = catalog_id.into();

        promote_core_catalog_to_cluster_prefix(
            Arc::clone(&current_node_id),
            Arc::clone(&catalog_id),
            Arc::clone(&store),
            args.storage_mode,
        )
        .await?;
        Self::load_or_create(
            catalog_id,
            Some(current_node_id),
            store,
            time_provider,
            metric_registry,
            limits,
            args,
        )
        .await
        .map(Arc::new)
    }

    /// Construct a `Catalog` for a node in an Enterprise cluster and initiate
    /// a background task that flips the current node to `Stopped` when
    /// `shutdown_token` is cancelled.
    ///
    /// `catalog_snapshot_observer` receives SLL events on catalog snapshot
    /// persistence (init + periodic checkpoints). Pass
    /// [`NoopCatalogSnapshotObserver`](influxdb3_wal::NoopCatalogSnapshotObserver)
    /// when SLL is disabled or in tests.
    #[allow(clippy::too_many_arguments)]
    pub async fn new_enterprise_with_shutdown(
        current_node_id: impl Into<Arc<str>>,
        catalog_id: impl Into<Arc<str>>,
        store: Arc<dyn ObjectStore>,
        time_provider: Arc<dyn TimeProvider>,
        metric_registry: Arc<Registry>,
        shutdown_token: ShutdownToken,
        limits: Arc<dyn CatalogLimiter>,
        args: CatalogArgs,
        process_uuid_getter: Arc<dyn ProcessUuidGetter>,
        catalog_snapshot_observer: Arc<dyn CatalogSnapshotObserver>,
    ) -> Result<Arc<Self>> {
        let current_node_id: Arc<str> = current_node_id.into();
        let catalog_id: Arc<str> = catalog_id.into();
        // Inline the v2 → cluster-prefix migration step from `new_enterprise`
        // so the dedicated `load_or_create_with_observer` path can attach the
        // SLL observer before any persist call. Test/other callers continue
        // to use `new_enterprise` → `load_or_create` (noop observer).
        promote_core_catalog_to_cluster_prefix(
            Arc::clone(&current_node_id),
            Arc::clone(&catalog_id),
            Arc::clone(&store),
            args.storage_mode,
        )
        .await?;
        let catalog = Arc::new(
            Self::load_or_create_with_observer(
                catalog_id,
                Some(current_node_id),
                store,
                time_provider,
                metric_registry,
                limits,
                args,
                catalog_snapshot_observer,
            )
            .await?,
        );
        let catalog_for_shutdown = Arc::clone(&catalog);
        tokio::spawn(async move {
            shutdown_token.wait_for_shutdown().await;
            let node_id = catalog_for_shutdown.current_node_id();
            if catalog_for_shutdown.state.lock().is_shutdown() {
                return;
            }
            match catalog_for_shutdown
                .update_node_state_stopped(node_id.as_ref(), process_uuid_getter)
                .await
            {
                Ok(_) => {
                    info!(
                        node_id = node_id.as_ref(),
                        "successfully updated node to stopped in catalog"
                    );
                }
                error @ Err(CatalogError::NodeAlreadyStopped { .. }) => {
                    info!(
                        ?error,
                        node_id = node_id.as_ref(),
                        "node is already stopped for this process, do not need to update catalog"
                    );
                }
                Err(error) => {
                    error!(
                        ?error,
                        node_id = node_id.as_ref(),
                        "encountered error while updating node to stopped state in catalog"
                    );
                }
            }
        });
        Ok(catalog)
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn from_parts(
        inner: InnerCatalog,
        store: ObjectStoreCatalog,
        current_node_id: Option<Arc<str>>,
        time_provider: Arc<dyn TimeProvider>,
        metric_registry: Arc<Registry>,
        shard_count: NonZeroU32,
        default_hard_delete_duration: Duration,
        checkpoint_policy: CheckpointPolicy,
        limits: Arc<dyn CatalogLimiter>,
    ) -> Self {
        let catalog_uuid = inner.catalog_uuid;
        let sequence = inner.sequence;
        Self {
            inner: RwLock::new(inner),
            subscriptions: Arc::new(AsyncRwLock::new(CatalogSubscriptions::default())),
            write_permit: Mutex::new(sequence),
            store,
            catalog_uuid,
            time_provider,
            metric_registry,
            shard_count,
            default_hard_delete_duration,
            current_node_id,
            limits,
            state: ParkingMutex::new(CatalogState::Active),
            checkpoint_policy,
            last_checkpoint: Arc::new(ParkingMutex::new(Checkpoint {
                sequence,
                when: Instant::now(),
            })),
            checkpoint_slot: Arc::new(Semaphore::new(1)),
        }
    }

    pub fn catalog_uuid(&self) -> Uuid {
        self.catalog_uuid
    }

    pub fn sequence_number(&self) -> CatalogSequenceNumber {
        self.inner.read().sequence
    }

    pub fn catalog_id(&self) -> Arc<str> {
        Arc::clone(&self.inner.read().catalog_id)
    }

    pub fn clone_inner(&self) -> InnerCatalog {
        self.inner.read().clone()
    }

    pub fn time_provider(&self) -> Arc<dyn TimeProvider> {
        Arc::clone(&self.time_provider)
    }

    /// Flip the catalog's lifecycle state to `Shutdown`.
    pub fn set_state_shutdown(&self) {
        *self.state.lock() = CatalogState::Shutdown;
    }

    pub fn metric_registry(&self) -> Arc<Registry> {
        Arc::clone(&self.metric_registry)
    }

    /// The target number of compaction shards per window.
    pub fn shard_count(&self) -> NonZeroU32 {
        self.shard_count
    }

    /// Returns the minimum row-delete-predicate version supported across all
    /// currently-registered nodes, or `None` if there are no nodes.
    pub fn minimum_supported_row_delete_predicate_version(&self) -> Option<usize> {
        self.inner
            .read()
            .nodes
            .resource_iter()
            .map(|node| node.row_delete_predicate_version() as usize)
            .min()
    }

    pub fn object_store(&self) -> Arc<dyn ObjectStore> {
        self.store.object_store()
    }

    pub fn object_store_prefix(&self) -> Arc<str> {
        Arc::clone(&self.store.prefix)
    }

    /// Capture a stable backup view: the live snapshot (checkpoint) plus the
    /// log files that follow it up to the current live sequence.
    ///
    /// The upper bound is captured immediately after reading the checkpoint so
    /// the returned checkpoint plus log window describe one atomic view. A
    /// later snapshot overwrite cannot race the backup: the checkpoint path and
    /// its decoded sequence are read together.
    pub async fn backup_view(&self) -> Result<CatalogBackupView> {
        let checkpoint = self.store.checkpoint_for_backup().await?;
        // Capture the upper bound immediately after reading the checkpoint. The
        // checkpoint sequence plus this window are intended to describe one
        // atomic backup view.
        let through_sequence = self.sequence_number();

        if through_sequence < checkpoint.sequence {
            return Err(CatalogError::BackupCheckpointAhead {
                checkpoint_sequence: checkpoint.sequence.get(),
                live_sequence: through_sequence.get(),
            });
        }

        let log_files = self
            .store
            .log_files_for_backup(checkpoint.sequence, through_sequence)
            .await?;

        Ok(CatalogBackupView {
            checkpoint,
            through_sequence,
            log_files,
        })
    }

    /// Replace the catalog with state loaded from a backup, coordinated
    /// across the cluster.
    ///
    /// Persists a [`RestoreCatalog`](crate::format::records::RestoreCatalog)
    /// record at the next sequence, applies it locally (loading the backup
    /// snapshot + log files and swapping `*inner`), broadcasts a
    /// [`CatalogRestored`](crate::catalog::versions::v3::events::CatalogEvent::CatalogFullyRestored)
    /// event to in-process subscribers, then forces a checkpoint so the
    /// restored state becomes the new snapshot baseline. Other nodes pick
    /// up the restore via the normal catch-up path — the persisted restore
    /// record drives the same load + swap on each of them.
    ///
    /// Subscribers (caches, processing engine, replicated buffer, etc.)
    /// observe the broadcast via `CatalogUpdateMessage::is_restore()` and
    /// rebuild their per-resource state from the post-restore catalog
    /// view.
    pub async fn restore(
        &self,
        restore_id: Arc<str>,
        source: CatalogRestoreSource,
    ) -> Result<RestoreReport> {
        info!(
            %restore_id,
            checkpoint_path = %source.checkpoint_path,
            n_log_files = source.log_paths.len(),
            "restore catalog"
        );
        let args = RestoreOpArgs {
            restore_id,
            source,
            time_ns: self.time_provider.now().timestamp_nanos(),
        };
        let committed = self.update_committed::<RestoreOp>(args).await?;
        // Force a checkpoint so the restored state becomes the new snapshot
        // baseline. Without this, the persisted `RestoreCatalog` log file
        // remains load-bearing for cold-starting peers — they'd re-execute
        // the backup load against paths that may no longer exist.
        //
        // `force_checkpoint` snapshots whatever sequence is live when it runs
        // (>= `committed.sequence`); the write permit is already released here,
        // so another writer may have advanced the catalog in between.
        //
        // This is a *separate, non-atomic* step: `update_committed` above has
        // already made the `RestoreCatalog` record durable at `restore_seq`. A
        // crash between the two leaves a durable restore record with no
        // snapshot covering it, and a node cold-starting in that window replays
        // the restore log and re-loads the backup from the paths embedded in
        // the record. Correctness there depends entirely on those backup
        // artifacts still existing — which is exactly the lifetime guarantee
        // the restore staging from #3891 provides: the staged checkpoint + logs
        // referenced by the restore record stay in place until a checkpoint
        // supersedes that sequence, after which catalog GC reclaims them. This
        // ordering must not be reworked to write the checkpoint first (it can
        // only run after the restored state is applied) nor to drop the staging
        // before a covering checkpoint exists.
        self.force_checkpoint().await?;
        Ok(committed.output)
    }

    /// Synchronously checkpoint the catalog at its current sequence.
    ///
    /// Skips the throttling that [`Self::maybe_background_checkpoint`]
    /// applies (slot acquisition, threshold checks) — meant for the rare
    /// cases (currently: just after a restore) where the caller has a
    /// correctness reason to force a snapshot regardless of recent
    /// checkpoint activity.
    ///
    /// The header sequence and the record body are read together under a
    /// single lock acquisition, so the persisted snapshot is always
    /// internally consistent. Callers must NOT pass in a sequence captured
    /// earlier: between a triggering apply (e.g. a restore) and this call
    /// another writer may advance the catalog, and stamping the snapshot
    /// with the older sequence while serializing the newer `ordered_records`
    /// would produce a checkpoint whose header undercounts its body. A
    /// cold-starting node would then replay the log at `header + 1` whose
    /// record is already present in the snapshot, failing the duplicate
    /// apply. Reading the live sequence here keeps header and body aligned
    /// regardless of any interleaving.
    pub(crate) async fn force_checkpoint(&self) -> Result<()> {
        // Hold the checkpoint slot across the read+write. `maybe_background_checkpoint`
        // acquires the same slot, so a background snapshot task spawned just before a
        // restore — capturing an older sequence and pre-restore records — cannot land
        // its `put` after this forced write and clobber the post-restore snapshot with
        // a stale image. We block on any in-flight background checkpoint, then read the
        // live sequence and write last, so the forced checkpoint always wins.
        let _checkpoint_permit = Arc::clone(&self.checkpoint_slot)
            .acquire_owned()
            .await
            .map_err(|e| CatalogError::Internal {
                details: format!("failed to acquire checkpoint slot: {e}"),
            })?;
        let (catalog_uuid, snapshot_seq, records) = {
            let inner = self.inner.read();
            (
                inner.catalog_uuid,
                inner.sequence_number(),
                inner.ordered_records.clone(),
            )
        };
        let snapshot_bytes = serialize_snapshot_file(catalog_uuid, snapshot_seq.get(), &records);
        self.store
            .write_checkpoint(snapshot_seq, snapshot_bytes)
            .await?;
        *self.last_checkpoint.lock() = Checkpoint {
            sequence: snapshot_seq,
            when: Instant::now(),
        };
        Ok(())
    }

    /// Object store path holding the cluster's commercial license file,
    /// derived from the catalog id.
    pub fn commercial_license_file_path(&self) -> ObjectStorePath {
        format!("{}/commercial_license", self.catalog_id()).into()
    }

    /// Object store path holding the cluster's trial or home license
    /// file, derived from the catalog id.
    pub fn trial_or_home_license_file_path(&self) -> ObjectStorePath {
        format!("{}/trial_or_home_license", self.catalog_id()).into()
    }

    pub fn list_namespaces(&self) -> Vec<Namespace> {
        self.inner
            .read()
            .databases
            .resource_iter()
            .map(|db| db.as_namespace())
            .collect()
    }

    pub fn node(&self, node_id: &str) -> Option<Arc<NodeDefinition>> {
        self.inner.read().nodes.get_by_name(node_id)
    }

    pub fn node_by_id(&self, node_id: &NodeId) -> Option<Arc<NodeDefinition>> {
        self.inner.read().nodes.get_by_id(node_id)
    }

    pub fn list_nodes(&self) -> Vec<Arc<NodeDefinition>> {
        self.inner.read().nodes.resource_iter().cloned().collect()
    }

    /// Node id of the running process, falling back to the catalog id
    /// when no current node is configured (e.g. single-process core).
    pub fn current_node_id(&self) -> Arc<str> {
        self.current_node_id
            .clone()
            .unwrap_or_else(|| self.catalog_id())
    }

    /// Resolve the current node's [`NodeDefinition`] from the registry.
    /// Errors if no current node is configured or if the configured id is
    /// not registered.
    pub fn current_node(&self) -> Result<Arc<NodeDefinition>> {
        let node_id = self.current_node_id.as_ref().ok_or_else(|| {
            CatalogError::NotFound("current node is not configured on the catalog".to_string())
        })?;
        self.inner.read().nodes.get_by_name(node_id).ok_or_else(|| {
            CatalogError::NotFound(format!(
                "current node {node_id} is not registered in the catalog"
            ))
        })
    }

    /// Test whether the current node satisfies `node_spec`. `All` always
    /// matches; `Nodes(_)` requires the current node to be configured and
    /// its catalog id to be in the list.
    pub fn matches_node_spec(&self, node_spec: &NodeSpec) -> Result<bool> {
        match node_spec {
            NodeSpec::All => Ok(true),
            NodeSpec::Nodes(ids) => Ok(ids.contains(&self.current_node()?.node_catalog_id())),
        }
    }

    /// Sum of `core_count` across every running node in the cluster.
    pub fn total_active_cores_in_use_by_all_nodes(&self) -> u64 {
        self.list_nodes()
            .iter()
            .filter(|node| node.is_running())
            .map(|node| node.core_count())
            .sum()
    }

    /// Sum of `core_count` across every running node in the cluster
    /// except the current one.
    pub fn total_active_cores_in_use_by_other_nodes(&self) -> u64 {
        let current_id = self.current_node().ok().map(|n| n.node_catalog_id());
        self.list_nodes()
            .iter()
            .filter(|node| node.is_running() && current_id != Some(node.node_catalog_id()))
            .map(|node| node.core_count())
            .sum()
    }

    // =========================================
    // Query group interfaces
    // =========================================

    /// Create a new query group.
    ///
    /// The members are kept in the order given. The replication factor sets
    /// how many copies of the data the group keeps.
    ///
    /// This API does not validate member-list policy, such as non-empty
    /// membership, uniqueness, node existence, or whether members are
    /// query-capable. Callers that accept user input should validate those
    /// invariants before calling into the catalog.
    ///
    /// Returns an error when:
    /// - The name is already in use.
    pub async fn create_query_group(
        &self,
        name: impl Into<Arc<str>>,
        members: Vec<NodeId>,
        replication_factor: std::num::NonZeroUsize,
    ) -> Result<Arc<QueryGroupDefinition>> {
        let name: Arc<str> = name.into();
        info!(
            name = %name,
            n_members = members.len(),
            replication_factor = replication_factor.get(),
            "create query group"
        );
        self.update::<CreateQueryGroupOp>(CreateQueryGroupArgs {
            name,
            members,
            replication_factor,
        })
        .await
    }

    /// Look up a query group by its operator-facing name.
    pub fn query_group_by_name(&self, name: &str) -> Option<Arc<QueryGroupDefinition>> {
        self.inner.read().query_groups.get_by_name(name)
    }

    /// Look up a query group by its catalog ID.
    pub fn query_group_by_id(&self, id: &QueryGroupId) -> Option<Arc<QueryGroupDefinition>> {
        self.inner.read().query_groups.get_by_id(id)
    }

    /// Return all query groups in insertion order.
    pub fn list_query_groups(&self) -> Vec<Arc<QueryGroupDefinition>> {
        self.inner
            .read()
            .query_groups
            .resource_iter()
            .cloned()
            .collect()
    }

    /// Update a query group's durable catalog definition.
    ///
    /// Only the fields set in `update` are changed. Any field left as `None`
    /// keeps its current value. Internally this writes a full-state record, so
    /// the stored group is always complete after each update.
    ///
    /// This API does not validate member-list policy, such as non-empty
    /// membership, uniqueness, node existence, or whether members are
    /// query-capable. Callers that accept user input should validate those
    /// invariants before calling into the catalog.
    pub async fn update_query_group(
        &self,
        id: &QueryGroupId,
        update: QueryGroupUpdate,
    ) -> Result<Arc<QueryGroupDefinition>> {
        info!(
            %id,
            rename = update.name.is_some(),
            update_members = update.members.is_some(),
            update_replication_factor = update.replication_factor.is_some(),
            "update query group"
        );
        self.update::<UpdateQueryGroupOp>(UpdateQueryGroupArgs { id: *id, update })
            .await
    }

    /// Add a member to a query group at the requested position.
    ///
    /// This API does not validate member-list policy, such as uniqueness, node
    /// existence, or whether the member is query-capable. Callers that accept
    /// user input should validate those invariants before calling into the
    /// catalog.
    pub async fn add_query_group_member(
        &self,
        id: &QueryGroupId,
        member: NodeId,
        position: QueryGroupInsertPosition,
    ) -> Result<Arc<QueryGroupDefinition>> {
        info!(%id, %member, ?position, "add query group member");
        self.update::<UpdateQueryGroupMembersOp>(UpdateQueryGroupMembersArgs {
            id: *id,
            mutation: QueryGroupMemberMutation::Add { member, position },
        })
        .await
    }

    /// Remove a member from a query group.
    ///
    /// This API does not validate member-list policy, such as non-empty
    /// membership after removal. Callers that accept user input should validate
    /// those invariants before calling into the catalog.
    pub async fn remove_query_group_member(
        &self,
        id: &QueryGroupId,
        member: NodeId,
    ) -> Result<Arc<QueryGroupDefinition>> {
        info!(%id, %member, "remove query group member");
        self.update::<UpdateQueryGroupMembersOp>(UpdateQueryGroupMembersArgs {
            id: *id,
            mutation: QueryGroupMemberMutation::Remove { member },
        })
        .await
    }

    /// Replace a query group's complete member list.
    ///
    /// This API does not validate member-list policy, such as non-empty
    /// membership, uniqueness, node existence, or whether members are
    /// query-capable. Callers that accept user input should validate those
    /// invariants before calling into the catalog.
    pub async fn replace_query_group_members(
        &self,
        id: &QueryGroupId,
        members: Vec<NodeId>,
    ) -> Result<Arc<QueryGroupDefinition>> {
        info!(%id, n_members = members.len(), "replace query group members");
        self.update::<UpdateQueryGroupOp>(UpdateQueryGroupArgs {
            id: *id,
            update: QueryGroupUpdate {
                members: Some(members),
                ..Default::default()
            },
        })
        .await
    }

    /// Update a query group's replication factor.
    pub async fn update_query_group_replication_factor(
        &self,
        id: &QueryGroupId,
        replication_factor: std::num::NonZeroUsize,
    ) -> Result<Arc<QueryGroupDefinition>> {
        info!(
            %id,
            replication_factor = replication_factor.get(),
            "update query group replication factor"
        );
        self.update::<UpdateQueryGroupOp>(UpdateQueryGroupArgs {
            id: *id,
            update: QueryGroupUpdate {
                replication_factor: Some(replication_factor),
                ..Default::default()
            },
        })
        .await
    }

    /// Remove a query group by ID.
    ///
    /// Returns the removed group definition so callers can inspect which
    /// members were affected (for example, node stop/remove safety checks).
    pub async fn delete_query_group(&self, id: &QueryGroupId) -> Result<Arc<QueryGroupDefinition>> {
        info!(%id, "delete query group");
        self.update::<DeleteQueryGroupOp>(DeleteQueryGroupArgs { id: *id })
            .await
    }

    pub fn next_db_id(&self) -> DbId {
        self.inner.read().databases.next_id()
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

    /// Names of databases that have not been soft-deleted.
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

    pub fn db_exists(&self, db_id: DbId) -> bool {
        self.inner.read().databases.contains_id(&db_id)
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

    pub fn has_admin_token(&self) -> bool {
        self.inner
            .read()
            .tokens
            .repo()
            .get_by_name(DEFAULT_OPERATOR_TOKEN_NAME)
            .is_some()
    }

    pub fn get_token(&self, token_name: &str) -> Option<Arc<TokenInfo>> {
        self.inner.read().tokens.repo().get_by_name(token_name)
    }

    pub fn get_retention_period_cutoff_map(&self) -> BTreeMap<(DbId, TableId), Time> {
        let now = self.time_provider.now();
        self.list_db_schema()
            .into_iter()
            .flat_map(|db_schema| {
                db_schema
                    .tables()
                    .filter_map(|table_def| {
                        let db_id = db_schema.id();
                        let table_id = table_def.id();
                        db_schema
                            .get_retention_period_cutoff_ts_nanos(now, &table_id)
                            .map(|cutoff| ((db_id, table_id), cutoff))
                    })
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    /// Like `get_retention_period_cutoff_map`, but also returns the retention
    /// period (in nanoseconds) alongside each cutoff.
    pub fn get_retention_cutoff_and_period_map(&self) -> BTreeMap<(DbId, TableId), (Time, i64)> {
        let now = self.time_provider.now();
        self.list_db_schema()
            .into_iter()
            .flat_map(|db_schema| {
                db_schema
                    .tables()
                    .filter_map(|table_def| {
                        let db_id = db_schema.id();
                        let table_id = table_def.id();
                        let (cutoff, period) =
                            db_schema.get_retention_cutoff_and_period(now, &table_id)?;
                        Some(((db_id, table_id), (cutoff, period.as_nanos() as i64)))
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

    /// Gen2+ compaction durations sorted by level, or `None` when no
    /// generation beyond gen1 is configured.
    pub fn compacted_generation_durations(&self) -> Option<Vec<Duration>> {
        let mut durations: Vec<(u8, Duration)> = self
            .list_generation_durations()
            .into_iter()
            .filter(|(level, _)| *level >= 2)
            .collect();
        if durations.is_empty() {
            return None;
        }
        durations.sort_by_key(|(level, _)| *level);
        Some(durations.into_iter().map(|(_, d)| d).collect())
    }

    pub fn generation_config(&self) -> GenerationConfig {
        self.inner.read().generation_config.clone()
    }

    pub fn storage_mode(&self) -> StorageMode {
        self.inner.read().storage_mode
    }

    /// Look up the database-scoped permission a `token_hash` carries for
    /// `db_name`. Returns `None` if the hash is unknown, the database is
    /// unknown, or the token has no permission for that database.
    pub fn get_permission(
        &self,
        db_name: &str,
        token_hash: Vec<u8>,
    ) -> Option<PermissionAttributes> {
        let inner = self.inner.read();
        let token_id = inner.tokens.hash_to_id(token_hash)?;
        let resource_id = inner.databases.name_to_id(db_name)?;
        inner.token_permissions.get_permission(
            ResourceType::Database,
            TokenPermissionResourceIdentifier::Database(resource_id),
            &token_id,
        )
    }

    pub fn database_count(&self) -> usize {
        self.inner.read().databases.len()
    }

    pub fn table_count(&self) -> usize {
        self.inner
            .read()
            .databases
            .resource_iter()
            .map(|db| db.table_count())
            .sum()
    }

    /// IDs of databases and tables marked for hard deletion.
    ///
    /// `(db_id, None)` flags the database itself; `(db_id, Some(table_id))`
    /// flags a single table within the database. Skips `_internal`.
    pub fn list_hard_deleted_dbs_tables(&self, limit: usize) -> Vec<(DbId, Option<TableId>)> {
        let mut result = Vec::new();
        let mut db_count = 0;
        let mut table_count = 0;
        let now = self.time_provider.now();

        let inner = self.inner.read();
        for db in inner.databases.resource_iter() {
            if result.len() >= limit {
                break;
            }
            if db.name.as_ref() == INTERNAL_DB_NAME {
                continue;
            }

            if db.deleted {
                if db
                    .hard_delete_time
                    .is_some_and(|hard_delete_time| hard_delete_time <= now)
                {
                    result.push((db.id, None));
                    db_count += 1;
                }
                continue;
            }

            for table in db.tables.resource_iter() {
                if result.len() >= limit {
                    break;
                }
                if table.deleted
                    && table
                        .hard_delete_time
                        .is_some_and(|hard_delete_time| hard_delete_time <= now)
                {
                    result.push((db.id, Some(table.table_id)));
                    table_count += 1;
                }
            }
        }

        if db_count > 0 || table_count > 0 {
            info!(
                db_count,
                table_count,
                limit,
                "listed hard deleted databases, tables, and max limit of results",
            );
        }

        result
    }

    /// Deletion status of a database by ID, or `None` if it exists and is not deleted.
    pub fn database_deletion_status(&self, db_id: DbId) -> Option<DeletionStatus> {
        let inner = self.inner.read();
        match inner.databases.get_by_id(&db_id) {
            Some(db_schema) if db_schema.deleted => db_schema
                .deleted
                .then(|| {
                    db_schema
                        .hard_delete_time
                        .and_then(|time| {
                            self.time_provider
                                .now()
                                .checked_duration_since(time)
                                .map(DeletionStatus::Hard)
                        })
                        .or(Some(DeletionStatus::Soft))
                })
                .flatten(),
            Some(_) => None,
            None => Some(DeletionStatus::NotFound),
        }
    }

    /// Deletion status of a table by ID within a database, or `None` if it
    /// exists and is not deleted.
    pub fn table_deletion_status(&self, db_id: DbId, table_id: TableId) -> Option<DeletionStatus> {
        let inner = self.inner.read();
        match inner.databases.get_by_id(&db_id) {
            Some(db_schema) => db_schema
                .table_deletion_status(table_id, Arc::clone(&self.time_provider))
                .or_else(|| self.database_deletion_status(db_id)),
            None => Some(DeletionStatus::NotFound),
        }
    }

    pub fn active_triggers(&self) -> Vec<(DbId, TriggerId)> {
        let inner = self.inner.read();
        inner
            .databases
            .resource_iter()
            .flat_map(|db| {
                let db_id = db.id();
                db.processing_engine_triggers
                    .resource_iter()
                    .filter_map(move |trigger| {
                        if trigger.disabled {
                            None
                        } else {
                            Some((db_id, trigger.trigger_id))
                        }
                    })
            })
            .collect()
    }

    pub(crate) async fn subscribe(&self, name: &'static str) -> CatalogUpdateReceiver {
        self.subscriptions.write().await.subscribe(name)
    }

    // -----------------------------------------------------------------
    // Subscriptions
    // -----------------------------------------------------------------

    /// Register a named subscriber for catalog-update event broadcasts.
    pub async fn subscribe_to_updates(&self, name: &'static str) -> CatalogUpdateReceiver {
        self.subscriptions.write().await.subscribe(name)
    }

    /// Drop the subscription registered under `name`.
    pub async fn unsubscribe_from_updates(&self, name: &str) {
        self.subscriptions.write().await.unsubscribe(name);
    }

    /// Drop subscriptions whose receivers have been closed.
    pub async fn prune_subscriptions(&self) {
        self.subscriptions.write().await.prune_closed();
    }
    // -----------------------------------------------------------------
    // Node mutations
    // -----------------------------------------------------------------

    /// Register a node in the cluster.
    #[allow(clippy::too_many_arguments)]
    pub async fn register_node(
        &self,
        node_id: &str,
        core_count: u64,
        mode: Vec<NodeMode>,
        process_uuid_getter: Arc<dyn ProcessUuidGetter>,
        instance_id: Arc<str>,
        conn_info: Option<String>,
        cli_params: Option<String>,
        row_delete_predicate_version: usize,
    ) -> Result<Arc<NodeDefinition>> {
        info!(
            node_id,
            core_count,
            mode = ?mode,
            conn_info = conn_info.as_deref().unwrap_or("<none>"),
            "register node"
        );
        let process_uuid = *process_uuid_getter.get_process_uuid();
        self.update::<RegisterNodeOp>(RegisterNodeArgs {
            node_id: Arc::from(node_id),
            core_count,
            mode,
            process_uuid,
            instance_id,
            conn_info,
            cli_params,
            registered_time: self.time_provider.now(),
            row_delete_predicate_version,
        })
        .await
    }

    /// Mark a node as stopped, recording the stopping process UUID. Used
    /// by a node's own graceful-shutdown path.
    pub async fn update_node_state_stopped(
        &self,
        node_id: &str,
        process_uuid_getter: Arc<dyn ProcessUuidGetter>,
    ) -> Result<Arc<NodeDefinition>> {
        let process_uuid = *process_uuid_getter.get_process_uuid();
        info!(
            node_id,
            %process_uuid,
            "updating node state to Stopped in catalog"
        );
        self.update::<StopNodeOp>(StopNodeArgs {
            node_id: Arc::from(node_id),
            stopped_time: self.time_provider.now(),
            process_uuid,
        })
        .await
    }

    /// Administratively stop a node. Records `Uuid::nil()` as the
    /// stopping process UUID to distinguish operator-driven stops from
    /// graceful-shutdown stops.
    pub async fn stop_node(&self, node_id: &str) -> Result<Arc<NodeDefinition>> {
        info!(node_id, "administratively stopping node in catalog");
        self.update::<StopNodeOp>(StopNodeArgs {
            node_id: Arc::from(node_id),
            stopped_time: self.time_provider.now(),
            process_uuid: Uuid::nil(),
        })
        .await
    }

    /// Request a graceful stop for a node, driving its state from `Running`
    /// to `Stopping`. The target node finishes its in-flight work and then
    /// writes an `AckStopNode` record to settle into the terminal `Stopped` state.
    pub async fn request_stop_node(
        &self,
        node_id: &str,
        process_uuid: Uuid,
    ) -> Result<Arc<NodeDefinition>> {
        info!(node_id, %process_uuid, "request graceful stop for node");
        self.update::<RequestStopNodeOp>(RequestStopNodeArgs {
            node_id: Arc::from(node_id),
            stopped_time: self.time_provider.now(),
            process_uuid,
        })
        .await
    }

    /// Acknowledge that a stopping node has persisted its final snapshot,
    /// transitioning it from `Stopping` to `Stopped`.
    pub async fn ack_stop_node(
        &self,
        node_id: &str,
        ack_time: Time,
        process_uuid: Uuid,
        final_snapshot_sequence: Option<SnapshotSequenceNumber>,
    ) -> Result<Arc<NodeDefinition>> {
        self.update::<AckStopNodeOp>(AckStopNodeArgs {
            node_id: Arc::from(node_id),
            ack_time,
            process_uuid,
            final_snapshot_sequence,
        })
        .await
    }

    /// Mark a fully-stopped node for permanent removal from the cluster.
    ///
    /// Returns [`CatalogError::IdempotentNoOp`] when the node is already in
    /// `Removing`; HTTP handlers translate that to 200 OK.
    pub async fn remove_node(
        &self,
        node_id: &str,
        requested_time: Time,
    ) -> Result<Arc<NodeDefinition>> {
        self.update::<RemoveNodeOp>(RemoveNodeArgs {
            node_id: Arc::from(node_id),
            requested_time,
        })
        .await
    }

    /// Permanently unregister a node from the catalog. Called by the
    /// compactor's node_removal driver after object-store cleanup completes.
    pub async fn unregister_node(&self, node_id: &str, unregistered_time: Time) -> Result<()> {
        self.update::<UnregisterNodeOp>(UnregisterNodeArgs {
            node_id: Arc::from(node_id),
            unregistered_time,
        })
        .await
    }

    /// Create a new database with default options.
    pub async fn create_database(&self, name: &str) -> Result<Arc<DatabaseSchema>> {
        self.create_database_opts(name, CreateDatabaseOptions::default())
            .await
    }

    /// Create a new database with the supplied options.
    pub async fn create_database_opts(
        &self,
        name: &str,
        options: CreateDatabaseOptions,
    ) -> Result<Arc<DatabaseSchema>> {
        info!(name, ?options, "create database");
        self.update::<CreateDatabaseOp>(CreateDatabaseArgs {
            name: name.to_string(),
            retention_period: options.retention_period,
        })
        .await
    }

    /// Soft-delete a database. The database is renamed in place (so the original name
    /// can be re-used) and scheduled for hard deletion at the resolved time.
    pub async fn soft_delete_database(
        &self,
        db_name: &str,
        hard_delete_time: HardDeletionTime,
        hard_delete_scope: DeletionScope,
    ) -> Result<Arc<DatabaseSchema>> {
        info!(
            db_name,
            ?hard_delete_time,
            ?hard_delete_scope,
            "soft delete database"
        );
        if db_name == INTERNAL_DB_NAME {
            return Err(CatalogError::CannotDeleteInternalDatabase);
        }

        // If the request uses `Default` and the database already has a hard-delete time, adopt the
        // existing time so re-issuing the same request is a no-op.
        let resolved_hard_delete_time = if hard_delete_time.is_default()
            && let Some(existing) = self.db_schema(db_name).and_then(|db| db.hard_delete_time)
        {
            Some(existing)
        } else {
            hard_delete_time.as_time(
                self.time_provider.as_ref(),
                self.default_hard_delete_duration,
            )
        };

        // Prevent scope changes against an already-deleted database.
        if let Some(db) = self.db_schema(db_name)
            && db.deleted
            && hard_delete_scope.as_option() != db.hard_delete_scope
        {
            return Err(CatalogError::AlreadyDeleted(db_name.to_string()));
        }

        self.update::<SoftDeleteDatabaseOp>(SoftDeleteDatabaseArgs {
            database_name: db_name.to_string(),
            deletion_time: self.time_provider.now(),
            hard_delete_time: resolved_hard_delete_time,
            hard_delete_scope: hard_delete_scope.as_option(),
        })
        .await
    }

    /// Hard-delete a database.
    pub async fn hard_delete_database(&self, db_id: &DbId) -> Result<Arc<DatabaseSchema>> {
        info!(db_id = db_id.get(), "hard delete database");
        self.update::<HardDeleteDatabaseOp>(HardDeleteDatabaseArgs { db_id: *db_id })
            .await
    }

    /// Apply a retention policy to an active database.
    pub async fn set_retention_period_for_database(
        &self,
        db_name: &str,
        duration: Duration,
    ) -> Result<Arc<DatabaseSchema>> {
        info!(db_name, ?duration, "set retention period for database");
        self.require_active_db(db_name)?;
        self.update::<SetDbRetentionPeriodOp>(SetDbRetentionPeriodArgs {
            db_name: db_name.to_string(),
            retention_period: duration,
        })
        .await
    }

    /// Remove the retention policy from an active database.
    pub async fn clear_retention_period_for_database(
        &self,
        db_name: &str,
    ) -> Result<Arc<DatabaseSchema>> {
        info!(db_name, "clear retention period for database");
        self.require_active_db(db_name)?;
        self.update::<ClearDbRetentionPeriodOp>(ClearDbRetentionPeriodArgs {
            db_name: db_name.to_string(),
        })
        .await
    }

    /// Validate that `db_name` resolves to a non-soft-deleted database.
    fn require_active_db(&self, db_name: &str) -> Result<Arc<DatabaseSchema>> {
        let Some(db) = self.db_schema(db_name) else {
            return Err(CatalogError::NotFound(db_name.to_string()));
        };
        if db.deleted || db.hard_delete_time.is_some() {
            return Err(CatalogError::AlreadyDeleted(db_name.to_string()));
        }
        Ok(db)
    }

    // -----------------------------------------------------------------
    // Table mutations
    // -----------------------------------------------------------------

    /// Create a table in `db_name` with the given tag and field columns, using default options.
    /// The database is auto-created if it does not exist.
    pub async fn create_table<S, F>(
        &self,
        db_name: &str,
        table_name: &str,
        tags: &[S],
        fields: &[(F, FieldDataType)],
    ) -> Result<Arc<TableDefinition>>
    where
        S: AsRef<str> + Send + Sync,
        F: AsRef<str> + Send + Sync,
    {
        self.create_table_opts(
            db_name,
            table_name,
            tags,
            fields,
            CreateTableOptions::default(),
        )
        .await
    }

    /// Create a table with the provided options.
    pub async fn create_table_opts<S, F>(
        &self,
        db_name: &str,
        table_name: &str,
        tags: &[S],
        fields: &[(F, FieldDataType)],
        options: CreateTableOptions,
    ) -> Result<Arc<TableDefinition>>
    where
        S: AsRef<str> + Send + Sync,
        F: AsRef<str> + Send + Sync,
    {
        info!(
            db_name,
            table_name,
            n_tags = tags.len(),
            n_fields = fields.len(),
            ?options,
            "create table"
        );
        loop {
            let mut txn = self.begin_database_transaction(db_name)?;
            let db_id = txn.db_schema().id;
            let table_id = txn.create_table_with_opts(table_name, options)?;
            let tbl_txn = txn.table_tx_or_create(table_name)?;
            for tag in tags.iter().map(AsRef::as_ref) {
                tbl_txn.tag_or_create(tag)?;
            }
            for (f_name, f_type) in fields.iter().map(|(f, d)| (f.as_ref(), *d)) {
                tbl_txn.field_or_create(f_name, f_type.into())?;
            }
            tbl_txn.time_or_create()?;
            match self.commit(txn).await? {
                Prompt::Success(_) => {
                    return self
                        .db_schema_by_id(&db_id)
                        .ok_or_else(|| {
                            CatalogError::NotFound(format!(
                                "database (id: {db_id}, name: {db_name})"
                            ))
                        })?
                        .table_definition_by_id(&table_id)
                        .ok_or_else(|| {
                            CatalogError::NotFound(format!(
                                "table (id: {table_id}, name: {table_name})"
                            ))
                        });
                }
                Prompt::Retry(_) => continue,
            }
        }
    }

    /// Soft-delete a table. The table is renamed in place and scheduled
    /// for hard deletion at the resolved time.
    pub async fn soft_delete_table(
        &self,
        db_name: &str,
        table_name: &str,
        hard_delete_time: HardDeletionTime,
        hard_delete_scope: DeletionScope,
    ) -> Result<Arc<TableDefinition>> {
        info!(
            db_name,
            table_name,
            ?hard_delete_time,
            ?hard_delete_scope,
            "soft delete table"
        );
        // Resolve `Default` against the table's existing hard-delete
        // time so re-issuing a Default request is a no-op.
        let resolved_hard_delete_time = if hard_delete_time.is_default()
            && let Some(existing) = self
                .db_schema(db_name)
                .and_then(|db| db.table_definition(table_name))
                .and_then(|tbl| tbl.hard_delete_time)
        {
            Some(existing)
        } else {
            hard_delete_time.as_time(
                self.time_provider.as_ref(),
                self.default_hard_delete_duration,
            )
        };

        self.update::<SoftDeleteTableOp>(SoftDeleteTableArgs {
            db_name: db_name.to_string(),
            table_name: table_name.to_string(),
            deletion_time: self.time_provider.now(),
            hard_delete_time: resolved_hard_delete_time,
            hard_delete_scope: hard_delete_scope.as_option(),
        })
        .await
    }

    /// Hard-delete a table, removing it from the catalog.
    pub async fn hard_delete_table(
        &self,
        db_id: &DbId,
        table_id: &TableId,
    ) -> Result<Arc<TableDefinition>> {
        info!(
            db_id = db_id.get(),
            table_id = table_id.get(),
            "hard delete table"
        );
        self.update::<HardDeleteTableOp>(HardDeleteTableArgs {
            db_id: *db_id,
            table_id: *table_id,
        })
        .await
    }

    /// Apply a per-table retention policy. Errors if the database or
    /// table is missing or soft-deleted.
    pub async fn set_retention_period_for_table(
        &self,
        db_name: &str,
        table_name: &str,
        duration: Duration,
    ) -> Result<Arc<TableDefinition>> {
        info!(
            db_name,
            table_name,
            ?duration,
            "set retention period for table"
        );
        self.update::<SetTableRetentionPeriodOp>(SetTableRetentionPeriodArgs {
            db_name: db_name.to_string(),
            table_name: table_name.to_string(),
            retention_period: duration,
        })
        .await
    }

    /// Remove the per-table retention policy.
    pub async fn clear_retention_period_for_table(
        &self,
        db_name: &str,
        table_name: &str,
    ) -> Result<Arc<TableDefinition>> {
        info!(db_name, table_name, "clear retention period for table");
        self.update::<ClearTableRetentionPeriodOp>(ClearTableRetentionPeriodArgs {
            db_name: db_name.to_string(),
            table_name: table_name.to_string(),
        })
        .await
    }

    // -----------------------------------------------------------------
    // Tokens
    // -----------------------------------------------------------------

    /// Create or regenerate the cluster's default operator (`_admin`) token. Returns the
    /// `TokenInfo` plus the raw token string.
    ///
    /// `regenerate=true` requires an existing operator token; the hash on the existing entry is
    /// rotated. `regenerate=false` requires no existing operator token; a fresh entry is created.
    pub async fn create_admin_token(&self, regenerate: bool) -> Result<(Arc<TokenInfo>, String)> {
        info!(regenerate, "create admin token");
        let (token, hash) = create_token_and_hash();
        let now = self.time_provider.now();

        let info = if regenerate {
            let token_id = self
                .inner
                .read()
                .tokens
                .repo()
                .get_by_name(DEFAULT_OPERATOR_TOKEN_NAME)
                .ok_or(CatalogError::MissingAdminTokenToUpdate)?
                .id;
            self.update::<RegenerateAdminTokenOp>(RegenerateAdminTokenArgs {
                token_id,
                new_hash: hash,
                updated_at: now.timestamp_millis(),
            })
            .await?
        } else {
            self.update::<CreateAdminTokenOp>(CreateAdminTokenArgs {
                name: DEFAULT_OPERATOR_TOKEN_NAME.to_string(),
                hash,
                created_at: now.timestamp_millis(),
                updated_at: None,
                expiry: None,
                description: None,
                created_by: None,
                updated_by: None,
            })
            .await?
        };
        Ok((info, token))
    }

    /// Create a named admin token.
    // NOTE(tjh): this was previously called create_named_admin_token_with_permissions but was
    // renamed here because the method does not give any permissions, it is an admin token that
    // gets full permissions.
    pub async fn create_named_admin_token(
        &self,
        token_name: String,
        expiry_secs: Option<u64>,
    ) -> Result<(Arc<TokenInfo>, String)> {
        info!(token_name, ?expiry_secs, "create named admin token");
        let (token, hash) = create_token_and_hash();
        let created_at = self.time_provider.now();
        let expiry = expiry_secs.and_then(|secs| {
            created_at
                .checked_add(Duration::from_secs(secs))
                .map(|t| t.timestamp_millis())
        });
        let info = self
            .update::<CreateAdminTokenOp>(CreateAdminTokenArgs {
                name: token_name,
                hash,
                created_at: created_at.timestamp_millis(),
                updated_at: None,
                expiry,
                description: None,
                created_by: None,
                updated_by: None,
            })
            .await?;
        Ok((info, token))
    }

    /// Create a named admin token using a caller-supplied hash.
    pub async fn create_named_admin_token_with_hash(
        &self,
        name: String,
        hash: Vec<u8>,
        expiry_millis: Option<i64>,
    ) -> Result<()> {
        info!(name, ?expiry_millis, "create named admin token with hash");
        self.update::<CreateAdminTokenOp>(CreateAdminTokenArgs {
            name,
            hash,
            created_at: self.time_provider.now().timestamp_millis(),
            updated_at: None,
            expiry: expiry_millis,
            description: None,
            created_by: None,
            updated_by: None,
        })
        .await?;
        Ok(())
    }

    /// Delete a named token. Operator token cannot be deleted.
    pub async fn delete_token(&self, token_name: &str) -> Result<Arc<TokenInfo>> {
        info!(token_name, "delete token");
        if token_name == DEFAULT_OPERATOR_TOKEN_NAME {
            return Err(CatalogError::CannotDeleteOperatorToken);
        }
        self.update::<DeleteTokenOp>(DeleteTokenArgs {
            token_name: token_name.to_string(),
        })
        .await
    }

    /// Create a resource-scoped token. Returns the stored `TokenInfo` plus the
    /// raw token string.
    pub async fn create_token_with_permission(
        &self,
        all_permissions: Vec<PermissionDetailsSpec>,
        token_name: String,
        expiry_secs: Option<u64>,
    ) -> Result<(Arc<TokenInfo>, String)> {
        info!(token_name, ?expiry_secs, permissions = ?all_permissions, "create token with permissions");
        let (token, hash) = create_token_and_hash();
        let permissions = self.validate_permission_specs(&all_permissions)?;
        let created_at = self.time_provider.now();
        let expiry = expiry_secs.and_then(|secs| {
            created_at
                .checked_add(Duration::from_secs(secs))
                .map(|t| t.timestamp_millis())
        });
        let info = self
            .update::<CreateResourceScopedTokenOp>(CreateResourceScopedTokenArgs {
                name: token_name,
                hash,
                created_at: created_at.timestamp_millis(),
                updated_at: None,
                expiry,
                description: None,
                created_by: None,
                updated_by: None,
                permissions,
            })
            .await?;
        Ok((info, token))
    }

    /// Create a resource-scoped token with caller-supplied hash and a
    /// list of permission specs.
    pub async fn create_token_with_permission_and_hash(
        &self,
        all_permissions: Vec<PermissionDetailsSpec>,
        token_name: String,
        hash: Vec<u8>,
        expiry_millis: Option<i64>,
    ) -> Result<()> {
        info!(token_name, ?expiry_millis, permissions = ?all_permissions, "create token with permissions and hash");
        let permissions = self.validate_permission_specs(&all_permissions)?;
        self.update::<CreateResourceScopedTokenOp>(CreateResourceScopedTokenArgs {
            name: token_name,
            hash,
            created_at: self.time_provider.now().timestamp_millis(),
            updated_at: None,
            expiry: expiry_millis,
            description: None,
            created_by: None,
            updated_by: None,
            permissions,
        })
        .await?;
        Ok(())
    }

    // -----------------------------------------------------------------
    // Distinct + last caches
    // -----------------------------------------------------------------

    /// Create a user-defined distinct-value cache.
    #[allow(clippy::too_many_arguments)]
    pub async fn create_distinct_cache<C: AsRef<str> + Send + Sync>(
        &self,
        db_name: &str,
        table_name: &str,
        node_spec: ApiNodeSpec,
        cache_name: Option<&str>,
        columns: &[C],
        max_cardinality: MaxCardinality,
        max_age_seconds: MaxAge,
    ) -> Result<Arc<DistinctCacheDefinition>> {
        Ok(self
            .create_distinct_cache_committed(
                db_name,
                table_name,
                node_spec,
                cache_name,
                columns,
                max_cardinality,
                max_age_seconds,
            )
            .await?
            .output)
    }

    /// Like [`Self::create_distinct_cache`], returning the applied sequence
    /// number and timestamp alongside the new definition.
    #[allow(clippy::too_many_arguments)]
    pub async fn create_distinct_cache_committed<C: AsRef<str> + Send + Sync>(
        &self,
        db_name: &str,
        table_name: &str,
        node_spec: ApiNodeSpec,
        cache_name: Option<&str>,
        columns: &[C],
        max_cardinality: MaxCardinality,
        max_age_seconds: MaxAge,
    ) -> Result<Committed<Arc<DistinctCacheDefinition>>> {
        let columns = columns.iter().map(|c| c.as_ref().to_string()).collect();
        info!(
            db_name,
            table_name,
            cache_name,
            ?node_spec,
            ?columns,
            max_cardinality = max_cardinality.to_u64(),
            max_age_seconds = max_age_seconds.as_secs(),
            "create distinct cache"
        );
        let resolved = self.resolve_node_spec(node_spec)?;
        self.update_committed::<CreateDistinctCacheOp>(CreateDistinctCacheArgs::User {
            db_name: db_name.to_string(),
            table_name: table_name.to_string(),
            node_spec: resolved,
            cache_name: cache_name.map(String::from),
            columns,
            max_cardinality,
            max_age: max_age_seconds,
        })
        .await
    }

    pub async fn delete_distinct_cache(
        &self,
        db_name: &str,
        table_name: &str,
        cache_name: &str,
    ) -> Result<Arc<DistinctCacheDefinition>> {
        info!(db_name, table_name, cache_name, "delete distinct cache");
        self.update::<DeleteDistinctCacheOp>(DeleteDistinctCacheArgs {
            db_name: db_name.to_string(),
            table_name: table_name.to_string(),
            cache_name: cache_name.to_string(),
        })
        .await
    }

    /// Create an auto-generated distinct-value cache.
    #[allow(clippy::too_many_arguments)]
    pub async fn create_auto_distinct_cache(
        &self,
        db_id: DbId,
        table_id: TableId,
        tag_ids: &[TagId],
        max_cardinality: MaxCardinality,
        max_age_seconds: MaxAge,
        lookback_seconds: u64,
        refresh_interval: RefreshInterval,
    ) -> Result<Arc<DistinctCacheDefinition>> {
        info!(
            db_id = db_id.get(),
            table_id = table_id.get(),
            ?tag_ids,
            max_cardinality = max_cardinality.to_u64(),
            max_age_seconds = max_age_seconds.as_secs(),
            lookback_seconds,
            refresh_interval_seconds = refresh_interval.as_secs(),
            "create auto distinct cache"
        );
        self.update::<CreateDistinctCacheOp>(CreateDistinctCacheArgs::Auto {
            db_id,
            table_id,
            tag_ids: tag_ids.to_vec(),
            max_cardinality,
            max_age: max_age_seconds,
            lookback_seconds: Some(lookback_seconds),
            refresh_interval: Some(refresh_interval),
        })
        .await
    }

    /// Delete the auto-generated distinct cache for `db_id`, `table_id`.
    pub async fn delete_auto_distinct_cache(
        &self,
        db_id: DbId,
        table_id: TableId,
    ) -> Result<Arc<DistinctCacheDefinition>> {
        info!(
            db_id = db_id.get(),
            table_id = table_id.get(),
            "delete auto distinct cache"
        );
        // Resolve names from ids and pass through to the standard
        // delete path. The auto cache is identified by its reserved
        // name on the schema. Scope the read guard so it is dropped
        // before the await — parking_lot guards are !Send.
        let (db_name, table_name) = {
            let inner = self.inner.read();
            let db = inner
                .databases
                .get_by_id(&db_id)
                .ok_or_else(|| CatalogError::NotFound(format!("database id {db_id}")))?;
            let table = db
                .tables
                .get_by_id(&table_id)
                .ok_or_else(|| CatalogError::NotFound(format!("table id {table_id}")))?;
            (db.name().to_string(), table.table_name.to_string())
        };
        let cache_name = DistinctCacheDefinition::auto_cache_name();

        self.update::<DeleteDistinctCacheOp>(DeleteDistinctCacheArgs {
            db_name,
            table_name,
            cache_name: cache_name.to_string(),
        })
        .await
    }

    /// Create a last-value cache.
    #[allow(clippy::too_many_arguments)]
    pub async fn create_last_cache<K, V>(
        &self,
        db_name: &str,
        table_name: &str,
        node_spec: ApiNodeSpec,
        cache_name: Option<&str>,
        key_columns: Option<&[K]>,
        value_columns: Option<&[V]>,
        count: LastCacheSize,
        ttl: LastCacheTtl,
    ) -> Result<Arc<LastCacheDefinition>>
    where
        K: AsRef<str> + Send + Sync,
        V: AsRef<str> + Send + Sync,
    {
        Ok(self
            .create_last_cache_committed(
                db_name,
                table_name,
                node_spec,
                cache_name,
                key_columns,
                value_columns,
                count,
                ttl,
            )
            .await?
            .output)
    }

    /// Like [`Self::create_last_cache`], returning the applied sequence
    /// number and timestamp alongside the new definition.
    #[allow(clippy::too_many_arguments)]
    pub async fn create_last_cache_committed<K, V>(
        &self,
        db_name: &str,
        table_name: &str,
        node_spec: ApiNodeSpec,
        cache_name: Option<&str>,
        key_columns: Option<&[K]>,
        value_columns: Option<&[V]>,
        count: LastCacheSize,
        ttl: LastCacheTtl,
    ) -> Result<Committed<Arc<LastCacheDefinition>>>
    where
        K: AsRef<str> + Send + Sync,
        V: AsRef<str> + Send + Sync,
    {
        let key_columns =
            key_columns.map(|c| c.iter().map(|s| s.as_ref().to_string()).collect::<Vec<_>>());
        let value_columns =
            value_columns.map(|c| c.iter().map(|s| s.as_ref().to_string()).collect::<Vec<_>>());
        info!(
            db_name,
            table_name,
            cache_name,
            ?node_spec,
            ?key_columns,
            ?value_columns,
            count = count.0,
            ttl_secs = ttl.as_secs(),
            "create last cache"
        );
        let resolved = self.resolve_node_spec(node_spec)?;

        self.update_committed::<CreateLastCacheOp>(CreateLastCacheArgs {
            db_name: db_name.to_string(),
            table_name: table_name.to_string(),
            node_spec: resolved,
            cache_name: cache_name.map(String::from),
            key_columns,
            value_columns,
            count,
            ttl,
        })
        .await
    }

    pub async fn delete_last_cache(
        &self,
        db_name: &str,
        table_name: &str,
        cache_name: &str,
    ) -> Result<Arc<LastCacheDefinition>> {
        info!(db_name, table_name, cache_name, "delete last cache");
        self.update::<DeleteLastCacheOp>(DeleteLastCacheArgs {
            db_name: db_name.to_string(),
            table_name: table_name.to_string(),
            cache_name: cache_name.to_string(),
        })
        .await
    }

    // -----------------------------------------------------------------
    // Processing-engine triggers
    // -----------------------------------------------------------------

    #[allow(clippy::too_many_arguments)]
    pub async fn create_processing_engine_trigger(
        &self,
        db_name: &str,
        trigger_name: &str,
        plugin_filename: ValidPluginFilename<'_>,
        node_spec: ApiNodeSpec,
        trigger_specification: &str,
        trigger_settings: TriggerSettings,
        trigger_arguments: &Option<HashMap<String, String>>,
        disabled: bool,
    ) -> Result<Arc<TriggerDefinition>> {
        info!(
            db_name,
            trigger_name,
            plugin_filename = plugin_filename.deref(),
            ?node_spec,
            trigger_specification,
            disabled,
            "create processing engine trigger"
        );
        // Explicit-node specs must reference processor-mode nodes only.
        if let ApiNodeSpec::Nodes(ref names) = node_spec {
            for name in names {
                let node = self
                    .node(name)
                    .ok_or_else(|| CatalogError::InvalidNodeName(name.clone()))?;
                if !NodeModes::from(node.modes().clone()).is_processor() {
                    return Err(
                        crate::error::enterprise::EnterpriseCatalogError::InvalidNodeMode {
                            expected: crate::error::enterprise::NodeMode::Process,
                        }
                        .into(),
                    );
                }
            }
        }
        let resolved = self.resolve_node_spec(node_spec)?;
        let trigger_spec = TriggerSpecificationDefinition::from_string_rep(trigger_specification)?;
        self.update::<CreateTriggerOp>(CreateTriggerArgs {
            db_name: db_name.to_string(),
            trigger_name: trigger_name.to_string(),
            plugin_filename: plugin_filename.to_string(),
            node_spec: resolved,
            trigger_specification: trigger_spec,
            trigger_settings,
            trigger_arguments: trigger_arguments.clone(),
            disabled,
        })
        .await
    }

    pub async fn delete_processing_engine_trigger(
        &self,
        db_name: &str,
        trigger_name: &str,
        force: bool,
    ) -> Result<Arc<TriggerDefinition>> {
        info!(
            db_name,
            trigger_name, force, "delete processing engine trigger"
        );
        self.update::<DeleteTriggerOp>(DeleteTriggerArgs {
            db_name: db_name.to_string(),
            trigger_name: trigger_name.to_string(),
            force,
        })
        .await
    }

    pub async fn enable_processing_engine_trigger(
        &self,
        db_name: &str,
        trigger_name: &str,
    ) -> Result<Arc<TriggerDefinition>> {
        info!(db_name, trigger_name, "enable processing engine trigger");
        self.update::<EnableTriggerOp>(EnableTriggerArgs {
            db_name: db_name.to_string(),
            trigger_name: trigger_name.to_string(),
        })
        .await
    }

    pub async fn disable_processing_engine_trigger(
        &self,
        db_name: &str,
        trigger_name: &str,
    ) -> Result<Arc<TriggerDefinition>> {
        info!(db_name, trigger_name, "disable processing engine trigger");
        self.update::<DisableTriggerOp>(DisableTriggerArgs {
            db_name: db_name.to_string(),
            trigger_name: trigger_name.to_string(),
        })
        .await
    }

    // -----------------------------------------------------------------
    // Cluster configuration
    // -----------------------------------------------------------------

    /// Set the generation-1 compaction duration.
    ///
    /// # Legacy Note
    ///
    /// This is method is for the legacy Parquet storage engined setting.
    pub async fn set_gen1_duration(&self, duration: Duration) -> Result<()> {
        info!(duration_ns = duration.as_nanos(), "set gen1 duration");
        self.update::<SetGenerationDurationOp>(SetGenerationDurationArgs { level: 1, duration })
            .await
    }

    /// Set every generation duration in a single atomic batch.
    /// `durations[i]` configures level `i + 1`. Re-issuing identical
    /// durations is a no-op; mismatched levels surface
    /// `CannotChangeGenerationDuration`. Adjacent durations must be in
    /// increasing-multiple alignment (`durations[i+1] % durations[i] == 0`),
    /// otherwise `MisalignedGenerations` is returned. Empty input is a
    /// silent no-op.
    ///
    /// # Legacy Note
    ///
    /// This is method is for the legacy Parquet storage engined setting.
    pub async fn set_all_generation_durations(&self, durations: &[Duration]) -> Result<()> {
        if durations.is_empty() {
            return Ok(());
        }
        info!(?durations, "set all generation durations");
        self.update::<SetAllGenerationDurationsOp>(SetAllGenerationDurationsArgs {
            durations: durations.to_vec(),
        })
        .await
    }

    /// Forward-only storage-mode transition:
    /// `Parquet → ParquetAndPachaTree → PachaTree`. Other transitions
    /// are rejected as Internal errors. Re-issuing the current mode is
    /// a `NoCatalogChange` no-op.
    pub async fn set_storage_mode(&self, storage_mode: StorageMode) -> Result<()> {
        info!(?storage_mode, "set storage mode");
        self.update::<SetStorageModeOp>(SetStorageModeArgs { storage_mode })
            .await
    }

    /// Operator-driven revert to `Parquet` from any other storage mode.
    ///
    /// # Warning
    ///
    /// Should not be called while nodes in the cluster are running.
    pub async fn downgrade_storage_mode_to_parquet(&self) -> Result<()> {
        info!("downgrade storage mode to parquet");
        self.update::<DowngradeStorageModeOp>(()).await
    }

    /// Resolve name-based [`ApiNodeSpec`] to the id-based [`NodeSpec`].
    fn resolve_node_spec(&self, api: ApiNodeSpec) -> Result<NodeSpec> {
        match api {
            ApiNodeSpec::All => Ok(NodeSpec::All),
            ApiNodeSpec::Nodes(names) => names
                .into_iter()
                .map(|name| {
                    self.node(&name)
                        .map(|n| n.node_catalog_id())
                        .ok_or(CatalogError::InvalidNodeName(name))
                })
                .collect::<Result<Vec<_>>>()
                .map(NodeSpec::Nodes),
        }
    }

    /// Execute an op and return just its output. See [`Self::update_committed`].
    pub(crate) async fn update<Op: CatalogOp>(&self, args: Op::Input) -> Result<Op::Output> {
        self.update_committed::<Op>(args).await.map(|c| c.output)
    }

    /// Execute an op: prepare → persist → apply → broadcast, returning its
    /// output along with the sequence number and timestamp it was applied at.
    ///
    /// On `PersistCatalogResult::AlreadyExists` (another writer won the race),
    /// load and apply the winning log files, then retry from prepare with the
    /// refreshed catalog state.
    pub(crate) async fn update_committed<Op: CatalogOp>(
        &self,
        args: Op::Input,
    ) -> Result<Committed<Op::Output>> {
        let mut permit = self.write_permit.lock().await;
        loop {
            let next_seq = permit.next();

            let mut batch = RecordBatch::new(next_seq.get());
            let (op, committed) = {
                let cat = self.inner.read();
                // Prepare first so existence/validation errors surface
                // ahead of any `TooMany*` from limits_check.
                let op = Op::prepare(&args, &cat, &mut batch)?;
                let usage = cat.current_usage();
                Op::limits_check(&args, &cat, &usage, self.limits.as_ref())?;
                (op, cat.committed_feature_level)
            };

            // The local `committed` may be stale; catch up and retry
            // before surfacing the rejection.
            if let Err(err) = check_batch_against_committed(batch.as_slice(), committed) {
                let before = self.sequence_number();
                self.catch_up_from(next_seq).await?;
                if self.sequence_number() != before {
                    *permit = self.sequence_number();
                    continue;
                }
                return Err(err);
            }

            let bytes = serialize_log_file(self.catalog_uuid, next_seq.get(), batch.as_slice());

            // Pre-load any RestoreCatalog backup state before taking the
            // sync write lock — the load is async I/O against object store
            // and the lock cannot be held across `.await`.
            let mut preload =
                preload_restore_for_records(batch.as_slice(), &self.store, committed).await?;

            match self.store.persist_log(next_seq, bytes).await? {
                PersistCatalogResult::Success => {
                    // TODO(tjh): a failure here leaves the catalog wedged — the log
                    // file is durable at `next_seq`, in-memory state is not, and
                    // every subsequent write hits AlreadyExists then re-fails the
                    // same apply during catch-up. Should poison/halt the catalog
                    // rather than surface as a per-call Internal error.
                    //
                    // See: https://github.com/influxdata/influxdb_pro/issues/3405
                    let (output, events, time_ns) = {
                        let mut cat = self.inner.write();
                        let events =
                            apply_records(batch.as_slice(), &mut cat, next_seq, &mut preload)
                                .map_err(|e| CatalogError::Internal {
                                    details: format!("apply_records after persist: {e}"),
                                })?;
                        let time_ns = self.time_provider.now().timestamp_nanos();
                        (op.output(&cat), events, time_ns)
                    };
                    *permit = next_seq;
                    self.broadcast(events, "broadcast").await?;
                    self.maybe_background_checkpoint(next_seq);
                    return Ok(Committed {
                        output,
                        sequence: next_seq,
                        time_ns,
                    });
                }
                PersistCatalogResult::AlreadyExists => {
                    self.catch_up_from(next_seq).await?;
                    *permit = self.sequence_number();
                    continue;
                }
            }
        }
    }

    pub fn begin_transaction(&self) -> CatalogTransaction {
        let sequence_at_begin = self.sequence_number();
        CatalogTransaction {
            sequence_at_begin,
            records: RecordBatch::new(sequence_at_begin.next().get()),
        }
    }

    /// Alias for [`Self::begin_database_transaction`], matching v2's
    /// `begin(db_name)` shape so consumers (write-buffer validators, write
    /// path) compile against either v2 or v3.
    pub fn begin(&self, db_name: &str) -> Result<DatabaseCatalogTransaction> {
        self.begin_database_transaction(db_name)
    }

    /// Open a [`DatabaseCatalogTransaction`] for the given database.
    ///
    /// If the database does not exist, it is auto-created and a
    /// [`crate::format::records::CreateDatabase`] record is staged in the
    /// underlying [`CatalogTransaction`]. Retention period is
    /// [`crate::catalog::versions::v3::schema::retention::RetentionPeriod::Indefinite`].
    pub fn begin_database_transaction(&self, db_name: &str) -> Result<DatabaseCatalogTransaction> {
        let mut inner_txn = self.begin_transaction();

        let (database_schema, next_table_id, usage, storage_mode) = {
            let cat = self.inner.read();
            let usage = cat.current_usage();
            let storage_mode = cat.storage_mode;

            match cat.databases.get_by_name(db_name) {
                Some(db) => {
                    let next_table_id = db.tables.next_id();
                    (db, next_table_id, usage, storage_mode)
                }
                None => {
                    let db_limit = self.limits.database_count_limit(&usage);
                    if usage.total_db_count() >= db_limit {
                        return Err(CatalogError::TooManyDbs(db_limit));
                    }
                    let db_id = cat.databases.next_id();
                    inner_txn.push(&CreateDatabase {
                        database_id: db_id.get(),
                        database_name: db_name.to_string(),
                        retention_period: WireRetentionPeriod::Indefinite,
                    });
                    let db = Arc::new(DatabaseSchema::new(db_id, Arc::from(db_name)));
                    (db, influxdb3_id::TableId::default(), usage, storage_mode)
                }
            }
        };

        Ok(DatabaseCatalogTransaction::new(
            inner_txn,
            database_schema,
            next_table_id,
            usage.total_table_count(),
            self.limits.table_count_limit(&usage),
            self.limits.column_per_table_limit(&usage),
            self.limits.tag_column_per_table_limit(&usage),
            storage_mode,
        ))
    }

    /// Commit an accumulated database transaction atomically.
    ///
    /// Returns `Prompt::Retry` when the catalog advanced between
    /// `begin_database_transaction()` and `commit()`, or when another
    /// writer raced us to the next sequence. The caller must re-run
    /// their domain logic against the refreshed catalog.
    pub async fn commit(
        &self,
        txn: DatabaseCatalogTransaction,
    ) -> Result<Prompt<CatalogSequenceNumber>> {
        self.commit_transaction(txn.finalize()).await
    }

    pub(crate) async fn commit_transaction(
        &self,
        txn: CatalogTransaction,
    ) -> Result<Prompt<CatalogSequenceNumber>> {
        let CatalogTransaction {
            sequence_at_begin,
            records,
        } = txn;

        if records.as_slice().is_empty() {
            return Ok(Prompt::Success(sequence_at_begin));
        }

        let mut permit = self.write_permit.lock().await;

        if *permit != sequence_at_begin {
            return Ok(Prompt::Retry(()));
        }

        let next_seq = permit.next();

        let committed = self.inner.read().committed_feature_level;
        // Same catch-up-then-recheck as `update`, but signal
        // `Prompt::Retry` instead of looping so the caller re-runs
        // its domain logic against the refreshed catalog.
        if let Err(err) = check_batch_against_committed(records.as_slice(), committed) {
            let before = self.sequence_number();
            self.catch_up_from(next_seq).await?;
            if self.sequence_number() != before {
                *permit = self.sequence_number();
                return Ok(Prompt::Retry(()));
            }
            return Err(err);
        }

        let bytes = serialize_log_file(self.catalog_uuid, next_seq.get(), records.as_slice());

        match self.store.persist_log(next_seq, bytes).await? {
            PersistCatalogResult::Success => {
                // TODO(tjh): a failure here leaves the catalog wedged — the log
                // file is durable at `next_seq`, in-memory state is not, and
                // every subsequent write hits AlreadyExists then re-fails the
                // same apply during catch-up. Should poison/halt the catalog
                // rather than surface as a per-call Internal error.
                //
                // See: https://github.com/influxdata/influxdb_pro/issues/3405
                let events = {
                    let mut cat = self.inner.write();
                    // Database transactions only carry DDL records; never
                    // a RestoreCatalog. Empty preload is sufficient.
                    apply_records(
                        records.as_slice(),
                        &mut cat,
                        next_seq,
                        &mut RestorePreload::empty(),
                    )
                    .map_err(|e| CatalogError::Internal {
                        details: format!("apply_records after commit persist: {e}"),
                    })?
                };
                *permit = next_seq;
                self.broadcast(events, "broadcast during transaction commit")
                    .await?;
                self.maybe_background_checkpoint(next_seq);
                Ok(Prompt::Success(next_seq))
            }
            PersistCatalogResult::AlreadyExists => {
                self.catch_up_from(next_seq).await?;
                *permit = self.sequence_number();
                Ok(Prompt::Retry(()))
            }
        }
    }

    /// Spawn a background checkpoint task if both the concurrency slot is
    /// available and a threshold (log count or wall-clock time) has been
    /// crossed since the last successful checkpoint.
    ///
    /// Returns `None` when the attempt is skipped (another checkpoint is in
    /// flight, no threshold met, or the catalog is empty). Returns
    /// `Some(handle)` when a task is spawned; callers in production ignore
    /// the handle — the task warns internally on failure — while tests can
    /// await it to observe success or error.
    fn maybe_background_checkpoint(
        &self,
        persisted_seq: CatalogSequenceNumber,
    ) -> Option<JoinHandle<Result<()>>> {
        let Ok(permit) = Arc::clone(&self.checkpoint_slot).try_acquire_owned() else {
            return None;
        };

        let last = *self.last_checkpoint.lock();
        let seq_delta = persisted_seq.get().saturating_sub(last.sequence.get());
        if seq_delta < self.checkpoint_policy.log_interval
            && last.elapsed() < self.checkpoint_policy.time_interval
        {
            return None;
        }

        // Clone the records under the read lock so the (potentially expensive)
        // serialization happens off-lock in the spawned task. `Record` clone
        // is a `Bytes` refcount bump plus a 16-byte header copy.
        let (catalog_uuid, records) = {
            let inner = self.inner.read();
            if inner.ordered_records.is_empty() {
                return None;
            }
            (inner.catalog_uuid, inner.ordered_records.clone())
        };

        let store = self.store.clone();
        let last_checkpoint = Arc::clone(&self.last_checkpoint);
        Some(tokio::spawn(async move {
            let _permit = permit;
            let snapshot_bytes =
                serialize_snapshot_file(catalog_uuid, persisted_seq.get(), &records);
            let result = store.write_checkpoint(persisted_seq, snapshot_bytes).await;
            match &result {
                Ok(()) => {
                    *last_checkpoint.lock() = Checkpoint {
                        sequence: persisted_seq,
                        when: Instant::now(),
                    };
                }
                Err(e) => warn!(
                    error = ?e,
                    prefix = %store.prefix,
                    sequence = persisted_seq.get(),
                    "background checkpoint failed",
                ),
            }
            result.map_err(Into::into)
        }))
    }

    pub(super) async fn catch_up_from(&self, from_seq: CatalogSequenceNumber) -> Result<()> {
        let mut seq = from_seq;
        while let Some(file) = self.store.load_log(seq).await? {
            // Pre-load restore state (if any) outside the sync write lock.
            let committed = self.inner.read().committed_feature_level;
            let mut preload = preload_restore_for_file(&file, &self.store, committed).await?;
            let events = {
                let mut cat = self.inner.write();
                apply_catalog_file(&file, &mut cat, &mut preload).map_err(|e| {
                    CatalogError::Internal {
                        details: format!(
                            "apply_catalog_file during catch-up at {}: {e}",
                            seq.get(),
                        ),
                    }
                })?
            };
            let saw_restore = events
                .iter()
                .any(|e| matches!(e, CatalogEvent::CatalogFullyRestored { .. }));
            self.broadcast(events, "broadcast during catch-up").await?;
            if saw_restore {
                // Force a fresh checkpoint so this node's snapshot reflects
                // the restored state — peers cold-starting later can load
                // the snapshot directly instead of replaying the restore
                // log against backup paths that may have moved.
                self.force_checkpoint().await?;
            }
            seq = seq.next();
        }
        Ok(())
    }

    /// Catch up by replaying log files up to and including
    /// `update_to`. Returns immediately if the local catalog is
    /// already at or past `update_to`. Stops when the next sequence
    /// to load is missing, even if `update_to` has not been reached
    /// (the missing log is the natural upper bound on what's
    /// persisted). Also bails out if the catalog has been marked for
    /// shutdown.
    ///
    /// Each applied file's events are broadcast to subscribers — the
    /// caches, processing engine, and deleter rely on this loop for
    /// updates that arrive via background poll, not via the local
    /// write path.
    pub async fn update_to_sequence_number(&self, update_to: CatalogSequenceNumber) -> Result<()> {
        let mut permit = self.write_permit.lock().await;
        let mut next = permit.next();
        if next > update_to {
            return Ok(());
        }
        while next <= update_to {
            if self.state.lock().is_shutdown() {
                break;
            }
            let Some(file) = self.store.load_log(next).await? else {
                break;
            };
            // Pre-load restore state (if any) outside the sync write lock.
            let committed = self.inner.read().committed_feature_level;
            let mut preload = preload_restore_for_file(&file, &self.store, committed).await?;
            let events = {
                let mut cat = self.inner.write();
                apply_catalog_file(&file, &mut cat, &mut preload).map_err(|e| {
                    CatalogError::Internal {
                        details: format!(
                            "apply_catalog_file during bounded catch-up at {}: {e}",
                            next.get(),
                        ),
                    }
                })?
            };
            *permit = next;
            let saw_restore = events
                .iter()
                .any(|e| matches!(e, CatalogEvent::CatalogFullyRestored { .. }));
            self.broadcast(events, "broadcast during bounded catch-up")
                .await?;
            if saw_restore {
                self.force_checkpoint().await?;
            }
            next = next.next();
        }
        Ok(())
    }

    /// Periodic catch-up loop. Sleeps for `duration` between probes and,
    /// when a log file is present at the next-expected sequence, drains
    /// every contiguously available log via
    /// [`Self::update_to_sequence_number`].
    ///
    /// Cadence is anchored to the loop start, not to the previous IO
    /// completion — successive wakes target `start + n * duration` so
    /// long IO doesn't accumulate drift. Exits on either the catalog's
    /// internal shutdown state or external `shutdown_token` cancellation.
    pub async fn background_update(&self, duration: Duration, shutdown_token: ShutdownToken) {
        let mut next_check = self.time_provider.now() + duration;
        loop {
            tokio::select! {
                _ = self.time_provider.sleep_until(next_check) => {
                    if self.state.lock().is_shutdown() {
                        break;
                    }
                    let next_seq = self.sequence_number().next();
                    match self.store.load_log(next_seq).await {
                        Ok(Some(_)) => {
                            if shutdown_token.is_cancelled() {
                                break;
                            }
                            if let Err(err) = self
                                .update_to_sequence_number(CatalogSequenceNumber::new(u64::MAX))
                                .await
                            {
                                error!(?err, "background catalog update failed");
                            }
                        }
                        Ok(None) => {}
                        Err(err) => {
                            error!(?err, "background catalog update probe failed");
                        }
                    }
                    next_check = self.time_provider.now() + duration;
                }
                _ = shutdown_token.wait_for_shutdown() => break,
            }
        }
    }

    async fn broadcast(&self, events: Vec<CatalogEvent>, context: &str) -> Result<()> {
        if events.is_empty() {
            return Ok(());
        }
        let update = Arc::new(CatalogUpdate::new(events));
        self.subscriptions
            .read()
            .await
            .send_update(update)
            .await
            .map_err(|e| CatalogError::Internal {
                details: format!("{context} failed: {e}"),
            })
    }

    /// Validate each permission spec against the catalog and convert to the
    /// wire format used by [`CreateResourceScopedTokenOp`].
    ///
    /// Each spec is checked for:
    /// - parseable resource type (`db`, `system`, or wildcard);
    /// - resolvable resource names (an unknown name fails during name→id
    ///   resolution; there is no separate existence pre-check);
    /// - actions valid for the resource type.
    fn validate_permission_specs(
        &self,
        specs: &[PermissionDetailsSpec],
    ) -> Result<Vec<WirePermission>> {
        let inner = self.inner.read();
        let inner_snapshot: Arc<dyn ResourceNameToIdProvider> = Arc::new(inner.clone());

        let mut out = Vec::with_capacity(specs.len());
        for perm in specs {
            let resource_type = ResourceType::from_str(&perm.resource_type)
                .map_err(|err| CatalogError::CannotParsePermissionForToken(err.to_string()))?;

            if matches!(resource_type, ResourceType::Wildcard) {
                return Err(CatalogError::CannotParsePermissionForToken(
                    "* resource type can only be set for admin token".to_string(),
                ));
            }

            // Resolve resource names to ids (handling the `*` wildcard and
            // surfacing a clear error for unknown names). Matches v2: there is
            // no separate database-existence pre-check — a missing database
            // surfaces as `InvalidResourceName` below.
            let resource_identifier = ResourceIdentifier::build_resource_ids_for_type(
                resource_type,
                Arc::clone(&inner_snapshot),
                &perm.resource_identifier,
            )
            .map_err(|err| CatalogError::CannotParsePermissionForToken(err.to_string()))?;

            let actions = Actions::build_actions_for_type(resource_type, &perm.actions)
                .map_err(|err| CatalogError::CannotParsePermissionForToken(err.to_string()))?;

            let wire_resource_type = match resource_type {
                ResourceType::Database => WireResourceType::Database,
                ResourceType::Token => WireResourceType::Token,
                ResourceType::System => WireResourceType::System,
                ResourceType::Wildcard => WireResourceType::Wildcard,
            };
            let wire_resource_identifier = match resource_identifier {
                ResourceIdentifier::Database(ids) => {
                    WireResourceIdentifier::Database(ids.iter().map(|id| id.get()).collect())
                }
                ResourceIdentifier::Token(ids) => {
                    WireResourceIdentifier::Token(ids.iter().map(|id| id.get()).collect())
                }
                ResourceIdentifier::System(ids) => {
                    WireResourceIdentifier::System(ids.iter().map(|id| id.as_u16()).collect())
                }
                ResourceIdentifier::Wildcard => WireResourceIdentifier::Wildcard,
            };
            let wire_actions = match actions {
                Actions::Database(d) => WireActions::Database(d.as_u16()),
                Actions::Token(t) => WireActions::Token(t.as_u16()),
                Actions::System(s) => WireActions::System(s.as_u16()),
                Actions::Wildcard => WireActions::Wildcard,
            };

            out.push(WirePermission {
                resource_type: wire_resource_type,
                resource_identifier: wire_resource_identifier,
                actions: wire_actions,
                resource_names: Vec::new(),
            });
        }
        Ok(out)
    }
}

/// Reject any non-`UPGRADE_SAFE` record whose ID exceeds `committed`.
fn check_batch_against_committed(records: &[Record], committed: FeatureLevel) -> Result<()> {
    for record in records {
        if record.flags().is_upgrade_safe() {
            continue;
        }
        let id = record.id();
        if !committed.allows(id) {
            return Err(CatalogError::RecordExceedsCommittedFeatureLevel {
                record_id: id.raw(),
                committed,
            });
        }
    }
    Ok(())
}

// -----------------------------------------------------------------
// Users
// -----------------------------------------------------------------

impl Catalog {
    pub fn get_user(&self, user_id: UserId) -> Option<Arc<UserInfo>> {
        self.inner.read().users.get_by_id(&user_id)
    }

    pub fn get_active_user(&self, user_id: UserId) -> Option<Arc<UserInfo>> {
        self.get_user(user_id).filter(|u| !u.is_deleted())
    }

    pub fn get_users(&self) -> Vec<Arc<UserInfo>> {
        self.inner.read().users.active_users().cloned().collect()
    }

    pub fn has_users(&self) -> bool {
        self.inner.read().users.has_active_users()
    }

    pub fn get_user_by_oauth_id(&self, oauth_id: &str) -> Option<Arc<UserInfo>> {
        self.inner.read().users.get_by_oauth_id(oauth_id)
    }

    pub fn get_deleted_user_by_oauth_id(&self, oauth_id: &str) -> Option<Arc<UserInfo>> {
        self.inner
            .read()
            .users
            .get_deleted_user_by_oauth_id(oauth_id)
    }

    pub async fn delete_user(&self, user_id: UserId) -> Result<()> {
        info!(%user_id, "delete user");
        self.update::<DeleteUserOp>(DeleteUserArgs {
            user_id,
            deleted_at: self.time_provider.now().timestamp_millis(),
        })
        .await?;
        Ok(())
    }

    pub async fn revoke_refresh_token(&self, token_hash: Arc<str>) -> Result<()> {
        info!("revoke refresh token");
        self.update::<RevokeRefreshTokenOp>(RevokeRefreshTokenArgs {
            token_hash: token_hash.to_string(),
            revoked_at: self.time_provider.now().timestamp_millis(),
        })
        .await?;
        Ok(())
    }

    pub fn get_login_identity(&self, user_id: UserId) -> Option<LoginIdentityUsernamePassword> {
        self.inner
            .read()
            .users
            .get_by_id(&user_id)
            .and_then(|u| u.login_identities.username_password.clone())
    }

    pub async fn update_user_roles(&self, user_id: UserId, role_ids: Vec<RoleId>) -> Result<()> {
        info!(%user_id, "update user roles");
        self.update::<UpdateUserRolesOp>(UpdateUserRolesArgs {
            user_id,
            role_ids,
            updated_at: self.time_provider.now().timestamp_millis(),
        })
        .await?;
        Ok(())
    }

    pub fn get_active_user_by_username(&self, username: &str) -> Option<Arc<UserInfo>> {
        self.inner
            .read()
            .users
            .get_by_username(username)
            .filter(|u| !u.is_deleted())
    }

    pub async fn create_user(&self, display_name: Option<Arc<str>>) -> Result<Arc<UserInfo>> {
        info!("create user");
        self.update::<CreateUserOp>(CreateUserArgs {
            display_name: display_name.map(|d| d.to_string()),
            created_at: self.time_provider.now().timestamp_millis(),
        })
        .await
    }

    pub async fn restore_user(
        &self,
        user_id: UserId,
        display_name: Option<Arc<str>>,
    ) -> Result<Arc<UserInfo>> {
        info!(%user_id, "restore user");
        self.update::<RestoreUserOp>(RestoreUserArgs {
            user_id,
            display_name,
            restored_at: self.time_provider.now().timestamp_millis(),
        })
        .await
    }

    pub async fn create_username_login_identity(
        &self,
        user_id: UserId,
        username: Arc<str>,
        password_hash: Arc<str>,
        requires_password_reset: bool,
    ) -> Result<LoginIdentityUsernamePassword> {
        info!(%user_id, "create username login identity");
        let user = self
            .update::<CreateLoginIdentityUsernamePasswordOp>(
                CreateLoginIdentityUsernamePasswordArgs {
                    user_id,
                    username: username.to_string(),
                    password_hash: password_hash.to_string(),
                    requires_password_reset,
                    created_at: self.time_provider.now().timestamp_millis(),
                },
            )
            .await?;
        Ok(user
            .login_identities
            .username_password
            .clone()
            .expect("identity should exist after creation"))
    }

    pub async fn create_oauth_login_identity(
        &self,
        user_id: UserId,
        oauth_id: Arc<str>,
    ) -> Result<Arc<UserInfo>> {
        info!(%user_id, "create OAuth login identity");
        self.update::<CreateLoginIdentityOAuthOp>(CreateLoginIdentityOAuthArgs {
            user_id,
            oauth_id: oauth_id.to_string(),
            created_at: self.time_provider.now().timestamp_millis(),
        })
        .await
    }

    pub async fn delete_oauth_login_identity(&self, user_id: UserId) -> Result<Arc<UserInfo>> {
        info!(%user_id, "delete OAuth login identity");
        self.update::<DeleteLoginIdentityOAuthOp>(DeleteLoginIdentityOAuthArgs { user_id })
            .await
    }

    pub async fn update_user_display_name(
        &self,
        user_id: UserId,
        display_name: Option<Arc<str>>,
    ) -> Result<Arc<UserInfo>> {
        info!(%user_id, "update user display name");
        self.update::<UpdateUserDisplayNameOp>(UpdateUserDisplayNameArgs {
            user_id,
            display_name: display_name.map(|d| d.to_string()),
            updated_at: self.time_provider.now().timestamp_millis(),
        })
        .await
    }

    pub async fn update_password_hash(
        &self,
        user_id: UserId,
        password_hash: Arc<str>,
    ) -> Result<Arc<UserInfo>> {
        info!(%user_id, "update password hash");
        self.update::<UpdateLoginIdentityPasswordHashOp>(UpdateLoginIdentityPasswordHashArgs {
            user_id,
            password_hash: password_hash.to_string(),
            updated_at: self.time_provider.now().timestamp_millis(),
        })
        .await
    }

    pub async fn update_requires_password_reset(
        &self,
        user_id: UserId,
        requires_password_reset: bool,
    ) -> Result<Arc<UserInfo>> {
        info!(%user_id, "update requires password reset");
        self.update::<UpdateLoginIdentityRequiresPasswordResetOp>(
            UpdateLoginIdentityRequiresPasswordResetArgs {
                user_id,
                requires_password_reset,
                updated_at: self.time_provider.now().timestamp_millis(),
            },
        )
        .await
    }

    pub fn get_refresh_token_by_hash(&self, token_hash: &str) -> Option<RefreshTokenInfo> {
        self.inner
            .read()
            .users
            .refresh_tokens()
            .get_by_hash(token_hash)
            .cloned()
    }

    pub async fn create_refresh_token(
        &self,
        user_id: UserId,
        token_hash: Arc<str>,
        expires_at: i64,
    ) -> Result<()> {
        info!(%user_id, "create refresh token");
        self.update::<CreateRefreshTokenOp>(CreateRefreshTokenArgs {
            user_id,
            token_hash: token_hash.to_string(),
            created_at: self.time_provider.now().timestamp_millis(),
            expires_at,
        })
        .await?;
        Ok(())
    }

    pub async fn revoke_all_refresh_tokens_for_user(&self, user_id: UserId) -> Result<()> {
        info!(%user_id, "revoke all refresh tokens");
        self.update::<RevokeAllRefreshTokensForUserOp>(RevokeAllRefreshTokensForUserArgs {
            user_id,
            revoked_at: self.time_provider.now().timestamp_millis(),
        })
        .await?;
        Ok(())
    }

    // -----------------------------------------------------------------
    // Roles
    // -----------------------------------------------------------------

    pub fn get_role(&self, role_id: RoleId) -> Option<Arc<Role>> {
        self.inner.read().roles.get_by_id(&role_id)
    }

    pub fn get_roles(&self) -> Vec<Arc<Role>> {
        self.inner.read().roles.all_roles().cloned().collect()
    }

    pub fn get_role_by_name(&self, name: &str) -> Option<Arc<Role>> {
        self.inner.read().roles.get_by_name(name)
    }

    pub async fn create_role(
        &self,
        name: influxdb3_authz::role::RoleName,
        description: Option<influxdb3_authz::role::RoleDescription>,
        permissions: Vec<influxdb3_authz::role::Permission>,
        is_required_role: bool,
    ) -> Result<Arc<Role>> {
        info!(name = %name, "create role");
        self.update::<CreateRoleOp>(CreateRoleArgs {
            name: name.to_string(),
            description: description.map(|d| d.as_str().to_string()),
            permissions,
            is_required_role,
            created_at: self.time_provider.now().timestamp_millis(),
        })
        .await
    }

    pub async fn update_role_permissions(
        &self,
        role_id: RoleId,
        permissions: Vec<influxdb3_authz::role::Permission>,
    ) -> Result<Arc<Role>> {
        info!(%role_id, "update role permissions");
        self.update::<UpdateRolePermissionsOp>(UpdateRolePermissionsArgs {
            role_id,
            permissions,
            updated_at: self.time_provider.now().timestamp_millis(),
        })
        .await
    }

    pub async fn update_role(
        &self,
        role_id: RoleId,
        name: Option<Arc<str>>,
        description: Option<influxdb3_authz::role::RoleDescription>,
    ) -> Result<Arc<Role>> {
        info!(%role_id, "update role");
        self.update::<UpdateRoleOp>(UpdateRoleArgs {
            role_id,
            name: name.map(|n| n.to_string()),
            description: description.map(|d| d.as_str().to_string()),
            updated_at: self.time_provider.now().timestamp_millis(),
        })
        .await
    }

    /// Delete a role. Returns an error if the role is a required role.
    pub async fn delete_role(&self, role_id: RoleId) -> Result<()> {
        info!(%role_id, "delete role");
        self.update::<DeleteRoleOp>(DeleteRoleArgs {
            role_id,
            deleted_at: self.time_provider.now().timestamp_millis(),
        })
        .await?;
        Ok(())
    }
}

#[cfg(feature = "test_helpers")]
impl Catalog {
    /// Insert a fully-formed `NodeDefinition` directly into the catalog,
    /// bypassing the prepare/persist/apply path. Test-only.
    pub fn test_only_insert_fake_running_node(
        &self,
        node_id: Arc<str>,
        node_catalog_id: influxdb3_id::NodeId,
        instance_id: Arc<str>,
        mode: Vec<crate::catalog::versions::v3::schema::node::NodeMode>,
        core_count: u64,
        row_delete_predicate_version: usize,
    ) {
        use crate::catalog::versions::v3::schema::node::{NodeDefinition, NodeState};
        self.inner
            .write()
            .nodes
            .insert(
                node_catalog_id,
                NodeDefinition {
                    node_id,
                    node_catalog_id,
                    instance_id,
                    mode,
                    core_count,
                    state: NodeState::Running {
                        registered_time_ns: 0,
                    },
                    conn_info: None,
                    cli_params: None,
                    feature_level: crate::format::derive_feature_level(),
                    row_delete_predicate_version: row_delete_predicate_version as u64,
                },
            )
            .unwrap();
    }

    /// Insert a fully-formed `DatabaseSchema` (with one table populated by
    /// the supplied `columns`) directly into the catalog, bypassing the
    /// prepare/persist/apply path. Test-only.
    pub fn test_only_insert_new_table_with_columns<const N: usize>(
        &self,
        table_name: &'static str,
        table_id: influxdb3_id::TableId,
        db_name: &'static str,
        db_id: influxdb3_id::DbId,
        columns: [crate::catalog::versions::v3::schema::column::ColumnDefinition; N],
    ) {
        use crate::Repository;
        use crate::catalog::versions::v3::schema::column::FieldFamilyMode;
        use crate::catalog::versions::v3::schema::database::DatabaseSchema;
        use crate::catalog::versions::v3::schema::retention::RetentionPeriod;
        use crate::catalog::versions::v3::schema::table::TableDefinition;

        let mut tables = Repository::new();
        let mut table_def =
            TableDefinition::new_empty(table_id, table_name.into(), FieldFamilyMode::Auto);
        table_def.add_columns(columns.to_vec()).unwrap();
        tables.insert(table_id, table_def).unwrap();

        self.inner
            .write()
            .databases
            .insert(
                db_id,
                DatabaseSchema {
                    id: db_id,
                    name: db_name.into(),
                    tables,
                    retention_period: RetentionPeriod::Indefinite,
                    processing_engine_triggers: Repository::default(),
                    deleted: false,
                    hard_delete_time: None,
                    hard_delete_scope: None,
                },
            )
            .unwrap();
    }
}

impl influxdb3_authz::TokenProvider for Catalog {
    fn get_token(&self, token_hash: Vec<u8>) -> Option<Arc<TokenInfo>> {
        self.inner.read().tokens.hash_to_info(token_hash)
    }
}

impl influxdb3_telemetry::ProcessingEngineMetrics for Catalog {
    fn num_triggers(&self) -> (u64, u64, u64, u64) {
        self.inner.read().num_triggers()
    }
}

impl influxdb3_authz::TokenPermissionProvider for Catalog {
    fn is_token_allowed_access(
        &self,
        token_id: &influxdb3_id::TokenId,
        access_request: influxdb3_authz::AccessRequest,
    ) -> Option<bool> {
        self.inner
            .read()
            .token_permissions
            .is_allowed_access(token_id, access_request)
    }
}

impl influxdb3_authz::IdProvider for Catalog {
    fn db_name_to_id(&self, db_name: &str) -> Option<influxdb3_id::DbId> {
        self.db_name_to_id(db_name)
    }

    fn validate_db_id(&self, db_id: &str) -> Option<influxdb3_id::DbId> {
        let db_id: influxdb3_id::DbId = db_id.parse().ok()?;
        self.db_exists(db_id).then_some(db_id)
    }
}

impl influxdb3_authz::CatalogProvider for Catalog {}

/// If neither a v3 nor a v2 catalog exists at `cluster_id` but a v2 catalog
/// exists at `current_node_id` (i.e. this binary previously ran as a core
/// node), copy the v2 core catalog to the cluster prefix with its
/// `catalog_id` rewritten so the v2 → v3 migration runner can pick it up.
///
/// Mirrors the equivalent step in v2's `Catalog::new_enterprise`.
async fn promote_core_catalog_to_cluster_prefix(
    current_node_id: Arc<str>,
    cluster_id: Arc<str>,
    store: Arc<dyn ObjectStore>,
    storage_mode: StorageMode,
) -> Result<()> {
    use crate::catalog::versions::v2::Snapshot;
    use crate::object_store::versions::v2::ObjectStoreCatalog as V2Store;
    use crate::object_store::versions::v3::ObjectStoreCatalog as V3Store;

    if current_node_id == cluster_id {
        return Ok(());
    }

    let v3_cluster = V3Store::new(Arc::clone(&cluster_id), Arc::clone(&store), storage_mode);
    if v3_cluster.load_snapshot().await?.is_some() {
        return Ok(());
    }

    let v2_storage_mode: crate::log::StorageMode = storage_mode.into();
    let v2_cluster = V2Store::new(
        Arc::clone(&cluster_id),
        u64::MAX,
        Arc::clone(&store),
        v2_storage_mode,
    );
    if v2_cluster.checkpoint_exists().await? {
        return Ok(());
    }

    let v2_core = V2Store::new(
        Arc::clone(&current_node_id),
        u64::MAX,
        Arc::clone(&store),
        v2_storage_mode,
    );
    if v2_core.checkpoint_exists().await? {
        let mut inner = v2_core
            .load_catalog()
            .await
            .map_err(|e| CatalogError::Internal {
                details: format!("loading v2 core catalog from {current_node_id}: {e}"),
            })?
            .ok_or_else(|| CatalogError::Internal {
                details: format!(
                    "v2 core catalog at {current_node_id} reported a checkpoint but did not load"
                ),
            })?;
        inner.catalog_id = Arc::clone(&cluster_id);

        let _ = v2_cluster
            .persist_catalog_checkpoint(&inner.snapshot())
            .await
            .map_err(|e| CatalogError::Internal {
                details: format!("persisting v2 catalog at cluster prefix {cluster_id}: {e}"),
            })?;
        return Ok(());
    }

    // No v2 core checkpoint: the node may have run the v3 Core build, which
    // writes a v3 catalog. Copy it directly to the v3 cluster prefix.
    let v3_core = V3Store::new(
        Arc::clone(&current_node_id),
        Arc::clone(&store),
        storage_mode,
    );
    if let Some(load) = v3_core
        .load_catalog()
        .await
        .map_err(|e| CatalogError::Internal {
            details: format!("loading v3 core catalog from {current_node_id}: {e}"),
        })?
    {
        let mut inner = load.inner;
        inner.catalog_id = Arc::clone(&cluster_id);
        let _ = v3_cluster
            .initialize_snapshot(inner.create_snapshot())
            .await
            .map_err(|e| CatalogError::Internal {
                details: format!("persisting v3 catalog at cluster prefix {cluster_id}: {e}"),
            })?;
    }
    Ok(())
}

#[cfg(test)]
mod tests;
