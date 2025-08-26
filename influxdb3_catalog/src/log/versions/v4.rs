#![allow(unreachable_pub, dead_code, clippy::wrong_self_convention)]

// Enterprise module removed during port

use std::{
    cmp::{Ord, PartialOrd},
    num::NonZeroUsize,
    ops::Deref,
    str::FromStr,
    sync::Arc,
    time::Duration,
};

use anyhow::Context;
use cron::Schedule;
// Enterprise import removed - CreateTokenDetails
use hashbrown::HashMap;
use humantime::{format_duration, parse_duration};
use influxdb3_id::{
    ColumnId, ColumnIdentifier, DbId, DistinctCacheId, FieldFamilyId, FieldIdentifier, LastCacheId,
    NodeId, TableId, TagId, TokenId, TriggerId,
};
use schema::{InfluxColumnType, InfluxFieldType};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::catalog::versions::v2::{
    ColumnDefinition, FieldColumn, FieldFamilyDefinition, FieldFamilyMode, FieldFamilyName,
    TagColumn, TimestampColumn,
};
use crate::{CatalogError, Result, catalog::CatalogSequenceNumber, serialize::VersionedFileType};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum RetentionPeriod {
    Indefinite,
    Duration(Duration),
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub enum CatalogBatch {
    Node(NodeBatch),
    Database(DatabaseBatch),
    Token(TokenBatch),
    /// A batch of delete operations to track database objects that have been permanently deleted
    /// from the catalog.
    Delete(DeleteBatch),
    /// A batch for modifying catalog generation configuration
    Generation(GenerationBatch),
}

impl CatalogBatch {
    pub fn node(
        time_ns: i64,
        node_catalog_id: NodeId,
        node_id: impl Into<Arc<str>>,
        ops: Vec<NodeCatalogOp>,
    ) -> Self {
        Self::Node(NodeBatch {
            time_ns,
            node_catalog_id,
            node_id: node_id.into(),
            ops,
        })
    }

    pub fn database(
        time_ns: i64,
        database_id: DbId,
        database_name: impl Into<Arc<str>>,
        ops: Vec<DatabaseCatalogOp>,
    ) -> Self {
        Self::Database(DatabaseBatch {
            time_ns,
            database_id,
            database_name: database_name.into(),
            ops,
        })
    }

    pub fn delete(time_ns: i64, ops: Vec<DeleteOp>) -> Self {
        Self::Delete(DeleteBatch { time_ns, ops })
    }

    pub fn generation(time_ns: i64, ops: Vec<GenerationOp>) -> Self {
        Self::Generation(GenerationBatch { time_ns, ops })
    }

    pub fn n_ops(&self) -> usize {
        match self {
            CatalogBatch::Node(node_batch) => node_batch.ops.len(),
            CatalogBatch::Database(database_batch) => database_batch.ops.len(),
            CatalogBatch::Token(token_batch) => token_batch.ops.len(),
            CatalogBatch::Delete(delete_batch) => delete_batch.ops.len(),
            CatalogBatch::Generation(generation_batch) => generation_batch.ops.len(),
        }
    }

    pub fn as_database(&self) -> Option<&DatabaseBatch> {
        match self {
            CatalogBatch::Database(database_batch) => Some(database_batch),
            _ => None,
        }
    }

    pub fn as_delete(&self) -> Option<&DeleteBatch> {
        match self {
            CatalogBatch::Delete(delete_batch) => Some(delete_batch),
            _ => None,
        }
    }

    pub fn to_database(self) -> Option<DatabaseBatch> {
        match self {
            CatalogBatch::Database(database_batch) => Some(database_batch),
            CatalogBatch::Node(_) => None,
            CatalogBatch::Token(_) => None,
            CatalogBatch::Delete(_) => None,
            CatalogBatch::Generation(_) => None,
        }
    }

    pub fn as_node(&self) -> Option<&NodeBatch> {
        match self {
            CatalogBatch::Node(node_batch) => Some(node_batch),
            CatalogBatch::Database(_) => None,
            CatalogBatch::Token(_) => None,
            CatalogBatch::Delete(_) => None,
            CatalogBatch::Generation(_) => None,
        }
    }

    pub fn as_generation(&self) -> Option<&GenerationBatch> {
        match self {
            CatalogBatch::Node(_) => None,
            CatalogBatch::Database(_) => None,
            CatalogBatch::Token(_) => None,
            CatalogBatch::Delete(_) => None,
            CatalogBatch::Generation(generation_batch) => Some(generation_batch),
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize, Default)]
pub struct NodeBatch {
    pub time_ns: i64,
    pub node_catalog_id: NodeId,
    pub node_id: Arc<str>,
    pub ops: Vec<NodeCatalogOp>,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize, Default)]
pub struct DatabaseBatch {
    pub time_ns: i64,
    pub database_id: DbId,
    pub database_name: Arc<str>,
    pub ops: Vec<DatabaseCatalogOp>,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize, Default)]
pub struct DeleteBatch {
    pub time_ns: i64,
    pub ops: Vec<DeleteOp>,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
pub enum DeleteOp {
    DeleteDatabase(DbId),
    DeleteTable(DbId, TableId),
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct GenerationBatch {
    pub time_ns: i64,
    pub ops: Vec<GenerationOp>,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
pub enum GenerationOp {
    SetGenerationDuration(SetGenerationDurationLog),
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
pub struct SetGenerationDurationLog {
    pub level: u8,
    pub duration: Duration,
}

/// A catalog batch that has been processed by the catalog and given a sequence number.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct OrderedCatalogBatch {
    pub(crate) catalog_batch: CatalogBatch,
    pub(crate) sequence_number: CatalogSequenceNumber,
}

impl OrderedCatalogBatch {
    pub fn new(catalog: CatalogBatch, sequence_number: CatalogSequenceNumber) -> Self {
        Self {
            catalog_batch: catalog,
            sequence_number,
        }
    }

    pub fn sequence_number(&self) -> CatalogSequenceNumber {
        self.sequence_number
    }

    pub fn batch(&self) -> &CatalogBatch {
        &self.catalog_batch
    }

    pub fn into_batch(self) -> CatalogBatch {
        self.catalog_batch
    }
}

impl VersionedFileType for OrderedCatalogBatch {
    const VERSION_ID: [u8; 10] = *b"idb3.004.l";
}

impl PartialOrd for OrderedCatalogBatch {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for OrderedCatalogBatch {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.sequence_number.cmp(&other.sequence_number)
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub enum NodeCatalogOp {
    RegisterNode(RegisterNodeLog),
    StopNode(StopNodeLog),
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub enum DatabaseCatalogOp {
    // Database ops:
    CreateDatabase(CreateDatabaseLog),
    SoftDeleteDatabase(SoftDeleteDatabaseLog),
    // Table ops:
    CreateTable(CreateTableLog),
    SoftDeleteTable(SoftDeleteTableLog),
    AddColumns(AddColumnsLog),
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
    // Retention period ops:
    SetRetentionPeriod(SetRetentionPeriodLog),
    ClearRetentionPeriod(ClearRetentionPeriodLog),
}

impl DatabaseCatalogOp {
    pub fn to_create_last_cache(self) -> Option<LastCacheDefinition> {
        match self {
            Self::CreateLastCache(create_last_cache_log) => Some(create_last_cache_log),
            _ => None,
        }
    }

    pub fn as_soft_delete_database(&self) -> Option<&SoftDeleteDatabaseLog> {
        match self {
            Self::SoftDeleteDatabase(log) => Some(log),
            _ => None,
        }
    }

    pub fn as_soft_delete_table(&self) -> Option<&SoftDeleteTableLog> {
        match self {
            Self::SoftDeleteTable(log) => Some(log),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct RegisterNodeLog {
    pub node_id: Arc<str>,
    pub instance_id: Arc<str>,
    pub registered_time_ns: i64,
    pub core_count: u64,
    pub mode: Vec<NodeMode>,
    pub process_uuid: Uuid,
    #[serde(default)]
    pub cli_params: Option<String>,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct StopNodeLog {
    pub node_id: Arc<str>,
    pub stopped_time_ns: i64,
    pub process_uuid: Uuid,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize, PartialOrd, Ord)]
pub enum NodeMode {
    Core,
}

impl NodeMode {
    pub fn as_str(&self) -> &'static str {
        match self {
            NodeMode::Core => "core",
        }
    }
}
impl std::fmt::Display for NodeMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct CreateDatabaseLog {
    pub database_id: DbId,
    pub database_name: Arc<str>,
    pub retention_period: Option<Duration>,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct SoftDeleteDatabaseLog {
    pub database_id: DbId,
    pub database_name: Arc<str>,
    pub deletion_time: i64,
    pub hard_deletion_time: Option<i64>,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct SoftDeleteTableLog {
    pub database_id: DbId,
    pub database_name: Arc<str>,
    pub table_id: TableId,
    pub table_name: Arc<str>,
    pub deletion_time: i64,
    pub hard_deletion_time: Option<i64>,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct CreateTableLog {
    pub database_id: DbId,
    pub database_name: Arc<str>,
    pub table_name: Arc<str>,
    pub table_id: TableId,
    pub field_family_mode: FieldFamilyMode,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct SetRetentionPeriodLog {
    pub database_name: Arc<str>,
    pub database_id: DbId,
    pub retention_period: RetentionPeriod,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClearRetentionPeriodLog {
    pub database_name: Arc<str>,
    pub database_id: DbId,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct AddColumnsLog {
    pub database_name: Arc<str>,
    pub database_id: DbId,
    pub table_name: Arc<str>,
    pub table_id: TableId,
    pub column_definitions: Vec<ColumnDefinitionLog>,
    pub field_family_definitions: Vec<FieldFamilyDefinitionLog>,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ColumnDefinitionLog {
    Timestamp(TimestampColumnLog),
    Tag(TagColumnLog),
    Field(FieldColumnLog),
}

impl ColumnDefinitionLog {
    pub fn id(&self) -> ColumnIdentifier {
        match self {
            ColumnDefinitionLog::Timestamp(_) => ColumnIdentifier::Timestamp,
            ColumnDefinitionLog::Tag(tag_log) => ColumnIdentifier::Tag(tag_log.id),
            ColumnDefinitionLog::Field(field_log) => ColumnIdentifier::Field(field_log.id),
        }
    }

    pub fn column_id(&self) -> ColumnId {
        match self {
            ColumnDefinitionLog::Timestamp(log) => log.column_id,
            ColumnDefinitionLog::Tag(log) => log.column_id,
            ColumnDefinitionLog::Field(log) => log.column_id,
        }
    }

    pub fn name(&self) -> Arc<str> {
        match self {
            ColumnDefinitionLog::Timestamp(log) => Arc::clone(&log.name),
            ColumnDefinitionLog::Tag(log) => Arc::clone(&log.name),
            ColumnDefinitionLog::Field(log) => Arc::clone(&log.name),
        }
    }

    pub fn column_type(&self) -> InfluxColumnType {
        match self {
            ColumnDefinitionLog::Timestamp(_) => InfluxColumnType::Timestamp,
            ColumnDefinitionLog::Tag(_) => InfluxColumnType::Tag,
            ColumnDefinitionLog::Field(v) => InfluxColumnType::Field(v.data_type.into()),
        }
    }
}

impl From<ColumnDefinitionLog> for ColumnDefinition {
    fn from(value: ColumnDefinitionLog) -> Self {
        match value {
            ColumnDefinitionLog::Timestamp(v) => ColumnDefinition::Timestamp(Arc::new(v.into())),
            ColumnDefinitionLog::Tag(v) => ColumnDefinition::Tag(Arc::new(v.into())),
            ColumnDefinitionLog::Field(v) => ColumnDefinition::Field(Arc::new(v.into())),
        }
    }
}

impl From<ColumnDefinition> for ColumnDefinitionLog {
    fn from(value: ColumnDefinition) -> Self {
        match value {
            ColumnDefinition::Timestamp(v) => {
                ColumnDefinitionLog::Timestamp(v.as_ref().clone().into())
            }
            ColumnDefinition::Tag(v) => ColumnDefinitionLog::Tag(v.as_ref().clone().into()),
            ColumnDefinition::Field(v) => ColumnDefinitionLog::Field(v.as_ref().clone().into()),
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct TimestampColumnLog {
    pub column_id: ColumnId,
    pub name: Arc<str>,
}

impl From<TimestampColumnLog> for TimestampColumn {
    fn from(value: TimestampColumnLog) -> Self {
        TimestampColumn {
            column_id: value.column_id,
            name: Arc::clone(&value.name),
        }
    }
}

impl From<TimestampColumn> for TimestampColumnLog {
    fn from(value: TimestampColumn) -> Self {
        TimestampColumnLog {
            column_id: value.column_id,
            name: Arc::clone(&value.name),
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct TagColumnLog {
    pub id: TagId,
    pub column_id: ColumnId,
    pub name: Arc<str>,
}

impl From<TagColumnLog> for TagColumn {
    fn from(value: TagColumnLog) -> Self {
        TagColumn {
            id: value.id,
            column_id: value.column_id,
            name: value.name,
        }
    }
}

impl From<TagColumn> for TagColumnLog {
    fn from(value: TagColumn) -> Self {
        TagColumnLog {
            id: value.id,
            column_id: value.column_id,
            name: Arc::clone(&value.name),
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct FieldColumnLog {
    pub id: FieldIdentifier,
    pub column_id: ColumnId,
    pub name: Arc<str>,
    pub data_type: FieldDataType,
}

impl From<FieldColumnLog> for FieldColumn {
    fn from(value: FieldColumnLog) -> Self {
        FieldColumn {
            id: value.id,
            column_id: value.column_id,
            name: value.name,
            data_type: value.data_type.into(),
        }
    }
}

impl From<FieldColumn> for FieldColumnLog {
    fn from(value: FieldColumn) -> Self {
        FieldColumnLog {
            id: value.id,
            column_id: value.column_id,
            name: Arc::clone(&value.name),
            data_type: value.data_type.into(),
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub enum FieldDataType {
    String,
    Integer,
    UInteger,
    Float,
    Boolean,
}

impl From<FieldDataType> for InfluxFieldType {
    fn from(value: FieldDataType) -> Self {
        match value {
            FieldDataType::String => Self::String,
            FieldDataType::Integer => Self::Integer,
            FieldDataType::UInteger => Self::UInteger,
            FieldDataType::Float => Self::Float,
            FieldDataType::Boolean => Self::Boolean,
        }
    }
}

impl From<InfluxFieldType> for FieldDataType {
    fn from(value: InfluxFieldType) -> Self {
        match value {
            InfluxFieldType::String => Self::String,
            InfluxFieldType::Integer => Self::Integer,
            InfluxFieldType::UInteger => Self::UInteger,
            InfluxFieldType::Float => Self::Float,
            InfluxFieldType::Boolean => Self::Boolean,
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct FieldFamilyDefinitionLog {
    pub id: FieldFamilyId,
    pub name: FieldFamilyName,
}

impl From<&FieldFamilyDefinition> for FieldFamilyDefinitionLog {
    fn from(value: &FieldFamilyDefinition) -> Self {
        Self {
            id: value.id,
            name: value.name.clone(),
        }
    }
}

/// Defines a last cache in a given table and database
#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone)]
pub struct LastCacheDefinition {
    /// The table id the cache is associated with
    pub table_id: TableId,
    /// The table name the cache is associated with
    pub table: Arc<str>,
    /// The last cache id scoped to parent table
    pub id: LastCacheId,
    /// Given name of the cache
    pub name: Arc<str>,
    /// Columns intended to be used as predicates in the cache
    pub key_columns: Vec<ColumnIdentifier>,
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
pub enum LastCacheValueColumnsDef {
    /// Explicit list of column names
    Explicit { columns: Vec<ColumnIdentifier> },
    /// Stores all non-key columns
    #[default]
    AllNonKeyColumns,
}

/// The size of the last cache
///
/// Must be greater than 0
#[derive(Debug, Serialize, Eq, PartialEq, Clone, Copy)]
pub struct LastCacheSize(pub(crate) usize);

impl LastCacheSize {
    pub fn new(size: usize) -> Result<Self> {
        if size == 0 {
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
pub const DEFAULT_CACHE_TTL: Duration = Duration::from_secs(60 * 60 * 4);

/// The time to live (TTL) for entries in the cache
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct LastCacheTtl(pub(crate) Duration);

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
pub struct DeleteLastCacheLog {
    pub table_id: TableId,
    pub table_name: Arc<str>,
    pub id: LastCacheId,
    pub name: Arc<str>,
}

/// Defines a distinct value cache in a given table and database
#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone)]
pub struct DistinctCacheDefinition {
    /// The id of the associated table
    pub table_id: TableId,
    /// The name of the associated table
    pub table_name: Arc<str>,
    /// The cache id in the catalog scoped to its parent table
    pub cache_id: DistinctCacheId,
    /// The name of the cache, is unique within the associated table
    pub cache_name: Arc<str>,
    /// The ids of columns tracked by this distinct value cache, in the defined order
    pub column_ids: Vec<ColumnIdentifier>,
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
pub struct MaxAge(pub(crate) Duration);

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
pub struct DeleteDistinctCacheLog {
    pub table_id: TableId,
    pub table_name: Arc<str>,
    pub cache_id: DistinctCacheId,
    pub cache_name: Arc<str>,
}

#[derive(Debug, Eq, PartialEq, Clone, Serialize, Deserialize, Copy)]
#[serde(rename_all = "snake_case")]
pub enum PluginType {
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
pub struct ValidPluginFilename<'a>(&'a str);

impl<'a> ValidPluginFilename<'a> {
    pub fn from_validated_name(name: &'a str) -> Self {
        Self(name)
    }
}

impl Deref for ValidPluginFilename<'_> {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        self.0
    }
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone)]
pub struct TriggerDefinition {
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
pub struct TriggerSettings {
    pub run_async: bool,
    pub error_behavior: ErrorBehavior,
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone, Copy, Default, clap::ValueEnum)]
#[serde(rename_all = "snake_case")]
pub enum ErrorBehavior {
    #[default]
    /// Log the error to the service output and system.processing_engine_logs table.
    Log,
    /// Rerun the trigger on error.
    Retry,
    /// Turn off the plugin until it is manually re-enabled.
    Disable,
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone)]
pub struct DeleteTriggerLog {
    pub trigger_id: TriggerId,
    pub trigger_name: Arc<str>,
    #[serde(default)]
    pub force: bool,
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone)]
pub struct TriggerIdentifier {
    pub db_id: DbId,
    pub db_name: Arc<str>,
    pub trigger_id: TriggerId,
    pub trigger_name: Arc<str>,
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone)]
#[serde(rename_all = "snake_case")]
pub enum TriggerSpecificationDefinition {
    SingleTableWalWrite { table_name: String },
    AllTablesWalWrite,
    Schedule { schedule: String },
    RequestPath { path: String },
    Every { duration: Duration },
}

impl TriggerSpecificationDefinition {
    pub fn from_string_rep(spec_str: &str) -> Result<TriggerSpecificationDefinition> {
        let spec_str = spec_str.trim();
        match spec_str {
            s if s.starts_with("table:") => {
                let table_name = s.trim_start_matches("table:").trim();
                if table_name.is_empty() {
                    return Err(CatalogError::TriggerSpecificationParseError {
                        trigger_spec: spec_str.to_string(),
                        context: Some("table name is empty".to_string()),
                    });
                }
                Ok(TriggerSpecificationDefinition::SingleTableWalWrite {
                    table_name: table_name.to_string(),
                })
            }
            "all_tables" => Ok(TriggerSpecificationDefinition::AllTablesWalWrite),
            s if s.starts_with("cron:") => {
                let cron_schedule = s.trim_start_matches("cron:").trim();
                if cron_schedule.is_empty() || Schedule::from_str(cron_schedule).is_err() {
                    return Err(CatalogError::TriggerSpecificationParseError {
                        trigger_spec: spec_str.to_string(),
                        context: None,
                    });
                }
                Ok(TriggerSpecificationDefinition::Schedule {
                    schedule: cron_schedule.to_string(),
                })
            }
            s if s.starts_with("every:") => {
                let duration_str = s.trim_start_matches("every:").trim();
                let Ok(duration) = parse_duration(duration_str) else {
                    return Err(CatalogError::TriggerSpecificationParseError {
                        trigger_spec: spec_str.to_string(),
                        context: Some("couldn't parse to duration".to_string()),
                    });
                };
                if duration > parse_duration("1 year").unwrap() {
                    return Err(CatalogError::TriggerSpecificationParseError {
                        trigger_spec: spec_str.to_string(),
                        context: Some("don't support every schedules of over 1 year".to_string()),
                    });
                }
                Ok(TriggerSpecificationDefinition::Every { duration })
            }
            s if s.starts_with("request:") => {
                let path = s.trim_start_matches("request:").trim();
                if path.is_empty() {
                    return Err(CatalogError::TriggerSpecificationParseError {
                        trigger_spec: spec_str.to_string(),
                        context: None,
                    });
                }
                Ok(TriggerSpecificationDefinition::RequestPath {
                    path: path.to_string(),
                })
            }
            _ => Err(CatalogError::TriggerSpecificationParseError {
                trigger_spec: spec_str.to_string(),
                context: Some("expect one of the following prefixes: 'table:', 'all_tables:', 'cron:', 'every:', or 'request:'".to_string()),
            }),
        }
    }

    pub fn string_rep(&self) -> String {
        match self {
            TriggerSpecificationDefinition::SingleTableWalWrite { table_name } => {
                format!("table:{table_name}")
            }
            TriggerSpecificationDefinition::AllTablesWalWrite => "all_tables".to_string(),
            TriggerSpecificationDefinition::Schedule { schedule } => {
                format!("cron:{schedule}")
            }
            TriggerSpecificationDefinition::Every { duration } => {
                format!("every:{}", format_duration(*duration))
            }
            TriggerSpecificationDefinition::RequestPath { path } => {
                format!("request:{path}")
            }
        }
    }

    pub fn plugin_type(&self) -> PluginType {
        match self {
            TriggerSpecificationDefinition::SingleTableWalWrite { .. }
            | TriggerSpecificationDefinition::AllTablesWalWrite => PluginType::WalRows,
            TriggerSpecificationDefinition::Schedule { .. }
            | TriggerSpecificationDefinition::Every { .. } => PluginType::Schedule,
            TriggerSpecificationDefinition::RequestPath { .. } => PluginType::Request,
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize, Default)]
pub struct TokenBatch {
    pub time_ns: i64,
    pub ops: Vec<TokenCatalogOp>,
}

// PK: I cannot come up with better names for variants, I _think_
//     it is ok to ignore. Maybe I can break them into separate
//     enum for each type
#[allow(clippy::enum_variant_names)]
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub enum TokenCatalogOp {
    CreateAdminToken(CreateAdminTokenDetails),
    RegenerateAdminToken(RegenerateAdminTokenDetails),
    DeleteToken(DeleteTokenDetails),
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct CreateAdminTokenDetails {
    pub token_id: TokenId,
    pub name: Arc<str>,
    pub hash: Vec<u8>,
    pub created_at: i64,
    pub updated_at: Option<i64>,
    pub expiry: Option<i64>,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct RegenerateAdminTokenDetails {
    pub token_id: TokenId,
    pub hash: Vec<u8>,
    pub updated_at: i64,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct DeleteTokenDetails {
    pub token_name: String,
}
