use std::{num::NonZeroUsize, ops::Deref, str::FromStr, sync::Arc, time::Duration};

use anyhow::Context;
use influxdb3_id::{ColumnIdentifier, DistinctCacheId, LastCacheId, TableId};
use serde::{Deserialize, Serialize};

use crate::{CatalogError, Result, resource::CatalogResource};

use super::node::NodeSpec;

/// Defines a last cache in a given table and database
#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone)]
pub struct LastCacheDefinition {
    /// The table id the cache is associated with
    pub table_id: TableId,
    /// The table name the cache is associated with
    pub table: Arc<str>,
    /// The last cache id scoped to parent table
    pub id: LastCacheId,
    /// Specify the node(s) which should have the cache enabled
    ///
    /// # Implementation Note
    ///
    /// This uses a default for cache definitions from core which do not specify a `node_spec`.
    #[serde(default)]
    pub node_spec: NodeSpec,
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

impl CatalogResource for LastCacheDefinition {
    type Identifier = LastCacheId;

    const CATEGORY: &'static str = "last_caches";

    fn id(&self) -> Self::Identifier {
        self.id
    }

    fn name(&self) -> Arc<str> {
        Arc::clone(&self.name)
    }
}

impl From<&LastCacheDefinition> for crate::log::LastCacheDefinition {
    fn from(value: &LastCacheDefinition) -> Self {
        Self {
            table_id: value.table_id,
            table: Arc::clone(&value.table),
            id: value.id,
            node_spec: (&value.node_spec).into(),
            name: Arc::clone(&value.name),
            key_columns: value.key_columns.clone(),
            value_columns: (&value.value_columns).into(),
            count: value.count.into(),
            ttl: value.ttl.into(),
        }
    }
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

impl From<LastCacheValueColumnsDef> for crate::log::LastCacheValueColumnsDef {
    fn from(value: LastCacheValueColumnsDef) -> Self {
        match value {
            LastCacheValueColumnsDef::Explicit { columns } => Self::Explicit { columns },
            LastCacheValueColumnsDef::AllNonKeyColumns => Self::AllNonKeyColumns,
        }
    }
}

impl From<&LastCacheValueColumnsDef> for crate::log::LastCacheValueColumnsDef {
    fn from(value: &LastCacheValueColumnsDef) -> Self {
        match value {
            LastCacheValueColumnsDef::Explicit { columns } => Self::Explicit {
                columns: columns.clone(),
            },
            LastCacheValueColumnsDef::AllNonKeyColumns => Self::AllNonKeyColumns,
        }
    }
}

impl From<crate::log::LastCacheValueColumnsDef> for LastCacheValueColumnsDef {
    fn from(value: crate::log::LastCacheValueColumnsDef) -> Self {
        match value {
            crate::log::LastCacheValueColumnsDef::Explicit { columns } => {
                Self::Explicit { columns }
            }
            crate::log::LastCacheValueColumnsDef::AllNonKeyColumns => Self::AllNonKeyColumns,
        }
    }
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

impl From<LastCacheSize> for crate::log::LastCacheSize {
    fn from(value: LastCacheSize) -> Self {
        let size: usize = value.into();
        Self::new(size).expect("v3 LastCacheSize should be valid")
    }
}

impl From<crate::log::LastCacheSize> for LastCacheSize {
    fn from(value: crate::log::LastCacheSize) -> Self {
        let size: usize = value.into();
        Self::new(size).expect("log LastCacheSize should be valid")
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

impl From<LastCacheTtl> for crate::log::LastCacheTtl {
    fn from(value: LastCacheTtl) -> Self {
        let duration: Duration = value.into();
        Self(duration)
    }
}

impl From<crate::log::LastCacheTtl> for LastCacheTtl {
    fn from(value: crate::log::LastCacheTtl) -> Self {
        let duration: Duration = value.into();
        Self(duration)
    }
}

impl From<CacheSource> for crate::log::CacheSource {
    fn from(value: CacheSource) -> Self {
        match value {
            CacheSource::User => Self::User,
            CacheSource::Auto => Self::Auto,
        }
    }
}

impl From<RefreshInterval> for crate::log::RefreshInterval {
    fn from(value: RefreshInterval) -> Self {
        crate::log::RefreshInterval::from(Duration::from(value))
    }
}

impl Default for LastCacheTtl {
    fn default() -> Self {
        Self(DEFAULT_CACHE_TTL)
    }
}

/// Source of a distinct value cache - whether created by user or auto-generated
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(tag = "type", content = "content")]
pub enum CacheSource {
    /// Manually created by a user via API
    #[default]
    User,
    /// Auto-generated from schema browsing queries
    Auto,
}

const DEFAULT_REFRESH_INTERVAL: Duration = Duration::from_secs(60 * 60);

/// Configuration for background refresh of distinct value caches
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct RefreshInterval(pub(crate) Duration);

impl<'de> Deserialize<'de> for RefreshInterval {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let seconds = u64::deserialize(deserializer)?;
        Ok(Self::from_secs(seconds))
    }
}

impl Serialize for RefreshInterval {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.0.as_secs().serialize(serializer)
    }
}

impl Default for RefreshInterval {
    fn default() -> Self {
        Self(DEFAULT_REFRESH_INTERVAL)
    }
}

impl From<RefreshInterval> for Duration {
    fn from(value: RefreshInterval) -> Self {
        value.0
    }
}

impl From<Duration> for RefreshInterval {
    fn from(duration: Duration) -> Self {
        Self(duration)
    }
}

impl RefreshInterval {
    pub fn from_secs(seconds: u64) -> Self {
        Self(Duration::from_secs(seconds))
    }

    pub fn as_secs(&self) -> u64 {
        self.0.as_secs()
    }
}

/// Defines a distinct value cache in a given table and database
#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone)]
pub struct DistinctCacheDefinition {
    /// The id of the associated table
    pub table_id: TableId,
    /// The name of the associated table
    pub table_name: Arc<str>,
    /// Specify the node(s) which should have the cache enabled
    ///
    /// # Implementation Note
    ///
    /// This uses a default for cache definitions from core which do not specify a `node_spec`.
    #[serde(default)]
    pub node_spec: NodeSpec,
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
    /// Source of the cache - User created or Auto-generated
    #[serde(default)]
    pub source: CacheSource,
    /// Lookback window in seconds for the query we run to populate auto-generated caches (e.g., 3600 = 1 hour)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lookback_seconds: Option<u64>,
    /// Background refresh configuration for auto-generated caches
    #[serde(skip_serializing_if = "Option::is_none")]
    pub refresh_interval: Option<RefreshInterval>,
}

impl From<&DistinctCacheDefinition> for crate::log::DistinctCacheDefinition {
    fn from(value: &DistinctCacheDefinition) -> Self {
        Self {
            table_id: value.table_id,
            table_name: Arc::clone(&value.table_name),
            node_spec: (&value.node_spec).into(),
            cache_id: value.cache_id,
            cache_name: Arc::clone(&value.cache_name),
            column_ids: value.column_ids.clone(),
            max_cardinality: value.max_cardinality.into(),
            max_age_seconds: value.max_age_seconds.into(),
            source: value.source.into(),
            lookback_seconds: value.lookback_seconds,
            refresh_interval: value.refresh_interval.map(Into::into),
        }
    }
}

impl CatalogResource for DistinctCacheDefinition {
    type Identifier = DistinctCacheId;

    const CATEGORY: &'static str = "distinct_caches";

    fn id(&self) -> Self::Identifier {
        self.cache_id
    }

    fn name(&self) -> Arc<str> {
        Arc::clone(&self.cache_name)
    }
}

impl DistinctCacheDefinition {
    /// Check if this is an auto-generated cache
    pub fn is_auto_generated(&self) -> bool {
        self.source == CacheSource::Auto
    }

    /// Get the reserved name for auto-generated caches
    pub fn auto_cache_name() -> Arc<str> {
        Arc::from("__auto__")
    }

    /// Check if cache name is reserved for auto-generation
    pub fn is_reserved_name(name: &str) -> bool {
        name == "__auto__"
    }
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

    fn try_from(value: usize) -> std::result::Result<Self, Self::Error> {
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

impl From<MaxCardinality> for crate::log::MaxCardinality {
    fn from(value: MaxCardinality) -> Self {
        let size: usize = value.into();
        Self::try_from(size).expect("v3 MaxCardinality should be valid")
    }
}

impl From<crate::log::MaxCardinality> for MaxCardinality {
    fn from(value: crate::log::MaxCardinality) -> Self {
        let size: usize = value.into();
        Self::try_from(size).expect("log MaxCardinality should be valid")
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

impl From<MaxAge> for crate::log::MaxAge {
    fn from(value: MaxAge) -> Self {
        let duration: Duration = value.into();
        Self::from(duration)
    }
}

impl From<crate::log::MaxAge> for MaxAge {
    fn from(value: crate::log::MaxAge) -> Self {
        let duration: Duration = value.into();
        Self(duration)
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
