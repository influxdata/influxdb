use std::{
    cmp::{Ord, PartialOrd},
    num::NonZeroUsize,
    ops::Deref,
    str::FromStr,
    sync::Arc,
    time::Duration,
};

use anyhow::Context;
use hashbrown::HashMap;
use influxdb3_id::{ColumnId, DbId, DistinctCacheId, LastCacheId, NodeId, TableId, TriggerId};
use serde::{Deserialize, Serialize};

use crate::{CatalogError, Result, catalog::CatalogSequenceNumber};

mod conversion;

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) enum CatalogBatch {
    Node(NodeBatch),
    Database(DatabaseBatch),
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize, Default)]
pub(crate) struct NodeBatch {
    pub time_ns: i64,
    pub node_catalog_id: NodeId,
    pub node_id: Arc<str>,
    pub ops: Vec<NodeCatalogOp>,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize, Default)]
pub(crate) struct DatabaseBatch {
    pub time_ns: i64,
    pub database_id: DbId,
    pub database_name: Arc<str>,
    pub ops: Vec<DatabaseCatalogOp>,
}

/// A catalog batch that has been processed by the catalog and given a sequence number.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct OrderedCatalogBatch {
    catalog_batch: CatalogBatch,
    sequence_number: CatalogSequenceNumber,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) enum NodeCatalogOp {
    RegisterNode(RegisterNodeLog),
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) enum DatabaseCatalogOp {
    // Database ops:
    CreateDatabase(CreateDatabaseLog),
    SoftDeleteDatabase(SoftDeleteDatabaseLog),
    // Table ops:
    CreateTable(CreateTableLog),
    SoftDeleteTable(SoftDeleteTableLog),
    AddFields(AddFieldsLog),
    // Distinct cache ops:
    CreateDistinctCache(DistinctCacheDefinition),
    DeleteDistinctCache(DeleteDistinctCacheLog),
    // Last cache ops:
    CreateLastCache(LastCacheDefinition),
    DeleteLastCache(DeleteLastCacheLog),
    // Plugin trigger ops:
    CreateTrigger(TriggerDefinition),
    DeleteTrigger(DeleteTriggerLog),
    EnableTrigger(TriggerIdentifier),
    DisableTrigger(TriggerIdentifier),
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct RegisterNodeLog {
    pub node_id: Arc<str>,
    pub instance_id: Arc<str>,
    pub registered_time_ns: i64,
    pub core_count: u64,
    pub mode: Vec<NodeMode>,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) enum NodeMode {
    Core,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct CreateDatabaseLog {
    pub database_id: DbId,
    pub database_name: Arc<str>,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct SoftDeleteDatabaseLog {
    pub database_id: DbId,
    pub database_name: Arc<str>,
    pub deletion_time: i64,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct SoftDeleteTableLog {
    pub database_id: DbId,
    pub database_name: Arc<str>,
    pub table_id: TableId,
    pub table_name: Arc<str>,
    pub deletion_time: i64,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct CreateTableLog {
    pub database_id: DbId,
    pub database_name: Arc<str>,
    pub table_name: Arc<str>,
    pub table_id: TableId,
    pub field_definitions: Vec<FieldDefinition>,
    pub key: Vec<ColumnId>,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct AddFieldsLog {
    pub database_name: Arc<str>,
    pub database_id: DbId,
    pub table_name: Arc<str>,
    pub table_id: TableId,
    pub field_definitions: Vec<FieldDefinition>,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct FieldDefinition {
    pub name: Arc<str>,
    pub id: ColumnId,
    pub data_type: FieldDataType,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) enum FieldDataType {
    String,
    Integer,
    UInteger,
    Float,
    Boolean,
    Timestamp,
    Tag,
}

/// Defines a last cache in a given table and database
#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone)]
pub(crate) struct LastCacheDefinition {
    /// The table id the cache is associated with
    pub table_id: TableId,
    /// The table name the cache is associated with
    pub table: Arc<str>,
    /// The last cache id scoped to parent table
    pub id: LastCacheId,
    /// Given name of the cache
    pub name: Arc<str>,
    /// Columns intended to be used as predicates in the cache
    pub key_columns: Vec<ColumnId>,
    /// Columns that store values in the cache
    pub value_columns: LastCacheValueColumnsDef,
    /// The number of last values to hold in the cache
    pub count: LastCacheSize,
    /// The time-to-live (TTL) in seconds for entries in the cache
    pub ttl: LastCacheTtl,
}

/// A last cache will either store values for an explicit set of columns, or will accept all
/// non-key columns
#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone, Default)]
#[serde(rename_all = "snake_case")]
pub(crate) enum LastCacheValueColumnsDef {
    /// Explicit list of column names
    Explicit { columns: Vec<ColumnId> },
    /// Stores all non-key columns
    #[default]
    AllNonKeyColumns,
}

/// The maximum allowed size for a last cache
pub(crate) const LAST_CACHE_MAX_SIZE: usize = 10;

/// The size of the last cache
///
/// Must be between 1 and [`LAST_CACHE_MAX_SIZE`]
#[derive(Debug, Serialize, Eq, PartialEq, Clone, Copy)]
pub(crate) struct LastCacheSize(usize);

impl LastCacheSize {
    fn new(size: usize) -> Result<Self> {
        if size == 0 || size > LAST_CACHE_MAX_SIZE {
            Err(CatalogError::InvalidLastCacheSize)
        } else {
            Ok(Self(size))
        }
    }
}

impl<'de> Deserialize<'de> for LastCacheSize {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let count = usize::deserialize(deserializer)?;
        Self::new(count).map_err(|e| serde::de::Error::custom(e.to_string()))
    }
}

impl Default for LastCacheSize {
    fn default() -> Self {
        Self(1)
    }
}

impl FromStr for LastCacheSize {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        let size =
            usize::from_str(s).context("expected an unsigned integer for last cache count")?;
        Self::new(size).context("invalid count for last cache provided")
    }
}

impl TryFrom<usize> for LastCacheSize {
    type Error = CatalogError;

    fn try_from(value: usize) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl From<LastCacheSize> for usize {
    fn from(value: LastCacheSize) -> Self {
        value.0
    }
}

impl From<LastCacheSize> for u64 {
    fn from(value: LastCacheSize) -> Self {
        value
            .0
            .try_into()
            .expect("usize fits into a 64 bit unsigned integer")
    }
}

impl PartialEq<usize> for LastCacheSize {
    fn eq(&self, other: &usize) -> bool {
        self.0.eq(other)
    }
}

impl PartialEq<LastCacheSize> for usize {
    fn eq(&self, other: &LastCacheSize) -> bool {
        self.eq(&other.0)
    }
}

/// The default cache time-to-live (TTL) is 4 hours
const DEFAULT_CACHE_TTL: Duration = Duration::from_secs(60 * 60 * 4);

/// The time to live (TTL) for entries in the cache
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct LastCacheTtl(Duration);

impl<'de> Deserialize<'de> for LastCacheTtl {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let seconds = u64::deserialize(deserializer)?;
        Ok(Self::from_secs(seconds))
    }
}

impl Serialize for LastCacheTtl {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.0.as_secs().serialize(serializer)
    }
}

impl Deref for LastCacheTtl {
    type Target = Duration;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<Duration> for LastCacheTtl {
    fn from(duration: Duration) -> Self {
        Self(duration)
    }
}

impl LastCacheTtl {
    pub fn from_secs(seconds: u64) -> Self {
        Self(Duration::from_secs(seconds))
    }

    pub fn as_secs(&self) -> u64 {
        self.0.as_secs()
    }
}

impl From<LastCacheTtl> for Duration {
    fn from(ttl: LastCacheTtl) -> Self {
        ttl.0
    }
}

impl Default for LastCacheTtl {
    fn default() -> Self {
        Self(DEFAULT_CACHE_TTL)
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct DeleteLastCacheLog {
    pub table_id: TableId,
    pub table_name: Arc<str>,
    pub id: LastCacheId,
    pub name: Arc<str>,
}

/// Defines a distinct value cache in a given table and database
#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone)]
pub(crate) struct DistinctCacheDefinition {
    /// The id of the associated table
    pub table_id: TableId,
    /// The name of the associated table
    pub table_name: Arc<str>,
    /// The cache id in the catalog scoped to its parent table
    pub cache_id: DistinctCacheId,
    /// The name of the cache, is unique within the associated table
    pub cache_name: Arc<str>,
    /// The ids of columns tracked by this distinct value cache, in the defined order
    pub column_ids: Vec<ColumnId>,
    /// The maximum number of distinct value combintions the cache will hold
    pub max_cardinality: MaxCardinality,
    /// The maximum age in seconds, similar to a time-to-live (TTL), for entries in the cache
    pub max_age_seconds: MaxAge,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct MaxCardinality(NonZeroUsize);

impl MaxCardinality {
    pub fn from_usize_unchecked(value: usize) -> Self {
        Self::try_from(value).unwrap()
    }

    pub fn to_u64(&self) -> u64 {
        usize::from(self.0)
            .try_into()
            .expect("usize not to overflow u64")
    }
}

impl TryFrom<usize> for MaxCardinality {
    type Error = anyhow::Error;

    fn try_from(value: usize) -> Result<Self, Self::Error> {
        Ok(Self(
            NonZeroUsize::try_from(value).context("invalid size provided")?,
        ))
    }
}

const DEFAULT_MAX_CARDINALITY: usize = 100_000;

impl Default for MaxCardinality {
    fn default() -> Self {
        Self(NonZeroUsize::new(DEFAULT_MAX_CARDINALITY).unwrap())
    }
}

impl From<NonZeroUsize> for MaxCardinality {
    fn from(v: NonZeroUsize) -> Self {
        Self(v)
    }
}

impl From<MaxCardinality> for usize {
    fn from(value: MaxCardinality) -> Self {
        value.0.into()
    }
}

const DEFAULT_MAX_AGE: Duration = Duration::from_secs(24 * 60 * 60);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct MaxAge(Duration);

impl<'de> Deserialize<'de> for MaxAge {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let seconds = u64::deserialize(deserializer)?;
        Ok(Self::from_secs(seconds))
    }
}

impl Serialize for MaxAge {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.0.as_secs().serialize(serializer)
    }
}

impl Default for MaxAge {
    fn default() -> Self {
        Self(DEFAULT_MAX_AGE)
    }
}

impl From<MaxAge> for Duration {
    fn from(value: MaxAge) -> Self {
        value.0
    }
}

impl From<Duration> for MaxAge {
    fn from(duration: Duration) -> Self {
        Self(duration)
    }
}

impl MaxAge {
    pub fn from_secs(seconds: u64) -> Self {
        Self(Duration::from_secs(seconds))
    }

    pub fn as_secs(&self) -> u64 {
        self.0.as_secs()
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct DeleteDistinctCacheLog {
    pub table_id: TableId,
    pub table_name: Arc<str>,
    pub cache_id: DistinctCacheId,
    pub cache_name: Arc<str>,
}

#[derive(Debug, Eq, PartialEq, Clone, Serialize, Deserialize, Copy)]
#[serde(rename_all = "snake_case")]
pub(crate) enum PluginType {
    WalRows,
    Schedule,
    Request,
}

impl std::fmt::Display for PluginType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(self, f)
    }
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone)]
pub(crate) struct ValidPluginFilename<'a>(&'a str);

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone)]
pub(crate) struct TriggerDefinition {
    pub trigger_id: TriggerId,
    pub trigger_name: Arc<str>,
    pub plugin_filename: String,
    pub database_name: Arc<str>,
    pub node_id: Arc<str>,
    pub trigger: TriggerSpecificationDefinition,
    pub trigger_settings: TriggerSettings,
    pub trigger_arguments: Option<HashMap<String, String>>,
    pub disabled: bool,
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone, Copy, Default)]
#[serde(rename_all = "snake_case")]
pub(crate) struct TriggerSettings {
    pub run_async: bool,
    pub error_behavior: ErrorBehavior,
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone, Copy, Default, clap::ValueEnum)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ErrorBehavior {
    #[default]
    /// Log the error to the service output and system.processing_engine_logs table.
    Log,
    /// Rerun the trigger on error.
    Retry,
    /// Turn off the plugin until it is manually re-enabled.
    Disable,
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone)]
pub(crate) struct DeleteTriggerLog {
    pub trigger_id: TriggerId,
    pub trigger_name: Arc<str>,
    pub force: bool,
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone)]
pub(crate) struct TriggerIdentifier {
    pub db_id: DbId,
    pub db_name: Arc<str>,
    pub trigger_id: TriggerId,
    pub trigger_name: Arc<str>,
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone)]
#[serde(rename_all = "snake_case")]
pub(crate) enum TriggerSpecificationDefinition {
    SingleTableWalWrite { table_name: String },
    AllTablesWalWrite,
    Schedule { schedule: String },
    RequestPath { path: String },
    Every { duration: Duration },
}
