//! Cache operations (record_ids 8-11).

use std::sync::Arc;

use super::impl_bitcode_encoding;
use super::types::{
    CacheSource as WireCacheSource, ColumnIdentifier as WireColumnId,
    FieldIdentifier as WireFieldId, LastCacheValueColumnsDef as WireLvcDef,
    NodeSpec as WireNodeSpec,
};
use crate::catalog::versions::v3::events::CatalogEvent;
use crate::catalog::versions::v3::inner::InnerCatalog;
use crate::catalog::versions::v3::schema::cache::{
    CacheSource, DistinctCacheDefinition, LastCacheDefinition, LastCacheSize, LastCacheTtl,
    LastCacheValueColumnsDef, MaxAge, MaxCardinality, RefreshInterval,
};
use crate::catalog::versions::v3::schema::node::NodeSpec as SchemaNodeSpec;
use crate::format::apply::ApplyError;
use crate::format::{CatalogRecord, RecordFlags, RecordId, RegisteredRecord, record_ids};
use crate::resource::CatalogResource;
use influxdb3_id::{
    ColumnIdentifier, DbId, DistinctCacheId, FieldFamilyId, FieldId, FieldIdentifier, LastCacheId,
    TableId, TagId,
};

/// Create a distinct value cache.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, bitcode::Encode, bitcode::Decode)]
pub struct CreateDistinctCache {
    /// Database catalog ID.
    pub db_id: u32,
    /// Table catalog ID.
    pub table_id: u32,
    /// Node specification for cache placement.
    pub node_spec: WireNodeSpec,
    /// Cache ID within the table.
    pub cache_id: u16,
    /// Cache name.
    pub cache_name: String,
    /// Column identifiers to track.
    pub column_ids: Vec<WireColumnId>,
    /// Maximum number of distinct values.
    pub max_cardinality: u64,
    /// Maximum age in seconds.
    pub max_age_seconds: u64,
    /// Source of the cache (user or auto).
    pub source: WireCacheSource,
    /// Optional lookback window in seconds.
    pub lookback_seconds: Option<u64>,
    /// Optional refresh interval in seconds.
    pub refresh_interval_seconds: Option<u64>,
}

impl CatalogRecord for CreateDistinctCache {
    const ID: RecordId = record_ids::CREATE_DISTINCT_CACHE;
    const FLAGS: RecordFlags = RecordFlags::none();
    const NAME: &'static str = "CreateDistinctCache";

    fn apply(&self, catalog: &mut InnerCatalog) -> Result<(), ApplyError> {
        let db_id = DbId::new(self.db_id);
        let table_id = TableId::new(self.table_id);
        let cache_id = DistinctCacheId::new(self.cache_id);

        catalog.databases.modify_by_id(&db_id, |db| {
            db.tables.modify_by_id(&table_id, |tbl| {
                let cache_def = DistinctCacheDefinition {
                    table_id: tbl.id(),
                    table_name: tbl.name(),
                    node_spec: SchemaNodeSpec::from(&self.node_spec),
                    cache_id,
                    cache_name: Arc::from(self.cache_name.as_str()),
                    column_ids: self.column_ids.iter().map(ColumnIdentifier::from).collect(),
                    max_cardinality: MaxCardinality::from_usize_unchecked(
                        self.max_cardinality as usize,
                    ),
                    max_age_seconds: MaxAge::from_secs(self.max_age_seconds),
                    source: CacheSource::from(self.source),
                    lookback_seconds: self.lookback_seconds,
                    refresh_interval: self
                        .refresh_interval_seconds
                        .map(RefreshInterval::from_secs),
                };

                tbl.distinct_caches.insert(cache_id, cache_def)?;
                Ok(())
            })
        })
    }

    fn event(&self) -> CatalogEvent {
        CatalogEvent::DistinctCacheCreated {
            db_id: DbId::new(self.db_id),
            table_id: TableId::new(self.table_id),
            cache_id: DistinctCacheId::new(self.cache_id),
        }
    }
}

inventory::submit! {
    RegisteredRecord::new::<CreateDistinctCache>()
}

/// Delete a distinct value cache.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, bitcode::Encode, bitcode::Decode)]
pub struct DeleteDistinctCache {
    /// Database catalog ID.
    pub db_id: u32,
    /// Table catalog ID.
    pub table_id: u32,
    /// Cache ID.
    pub cache_id: u16,
}

impl CatalogRecord for DeleteDistinctCache {
    const ID: RecordId = record_ids::DELETE_DISTINCT_CACHE;
    const FLAGS: RecordFlags = RecordFlags::none();
    const NAME: &'static str = "DeleteDistinctCache";

    fn apply(&self, catalog: &mut InnerCatalog) -> Result<(), ApplyError> {
        let db_id = DbId::new(self.db_id);
        let table_id = TableId::new(self.table_id);
        let cache_id = DistinctCacheId::new(self.cache_id);

        catalog.databases.modify_by_id(&db_id, |db| {
            db.tables.modify_by_id(&table_id, |tbl| {
                tbl.distinct_caches.remove(&cache_id);
                Ok(())
            })
        })
    }

    fn event(&self) -> CatalogEvent {
        CatalogEvent::DistinctCacheDeleted {
            db_id: DbId::new(self.db_id),
            table_id: TableId::new(self.table_id),
            cache_id: DistinctCacheId::new(self.cache_id),
        }
    }
}

inventory::submit! {
    RegisteredRecord::new::<DeleteDistinctCache>()
}

/// Create a last value cache.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, bitcode::Encode, bitcode::Decode)]
pub struct CreateLastCache {
    /// Database catalog ID.
    pub db_id: u32,
    /// Table catalog ID.
    pub table_id: u32,
    /// Cache ID within the table.
    pub id: u16,
    /// Node specification for cache placement.
    pub node_spec: WireNodeSpec,
    /// Cache name.
    pub name: String,
    /// Key column identifiers.
    pub key_columns: Vec<WireColumnId>,
    /// Value column definition.
    pub value_columns: WireLvcDef,
    /// Number of last values to store.
    pub count: u64,
    /// TTL in seconds.
    pub ttl_seconds: u64,
}

impl CatalogRecord for CreateLastCache {
    const ID: RecordId = record_ids::CREATE_LAST_CACHE;
    const FLAGS: RecordFlags = RecordFlags::none();
    const NAME: &'static str = "CreateLastCache";

    fn apply(&self, catalog: &mut InnerCatalog) -> Result<(), ApplyError> {
        let db_id = DbId::new(self.db_id);
        let table_id = TableId::new(self.table_id);
        let cache_id = LastCacheId::new(self.id);

        catalog.databases.modify_by_id(&db_id, |db| {
            db.tables.modify_by_id(&table_id, |tbl| {
                let cache_def = LastCacheDefinition {
                    table_id: tbl.id(),
                    table: tbl.name(),
                    id: cache_id,
                    node_spec: SchemaNodeSpec::from(&self.node_spec),
                    name: Arc::from(self.name.as_str()),
                    key_columns: self
                        .key_columns
                        .iter()
                        .map(ColumnIdentifier::from)
                        .collect(),
                    value_columns: LastCacheValueColumnsDef::from(&self.value_columns),
                    count: LastCacheSize::new(self.count as usize).expect("cache size must be > 0"),
                    ttl: LastCacheTtl::from_secs(self.ttl_seconds),
                };

                tbl.last_caches.insert(cache_id, cache_def)?;
                Ok(())
            })
        })
    }

    fn event(&self) -> CatalogEvent {
        CatalogEvent::LastCacheCreated {
            db_id: DbId::new(self.db_id),
            table_id: TableId::new(self.table_id),
            cache_id: LastCacheId::new(self.id),
        }
    }
}

inventory::submit! {
    RegisteredRecord::new::<CreateLastCache>()
}

/// Delete a last value cache.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, bitcode::Encode, bitcode::Decode)]
pub struct DeleteLastCache {
    /// Database catalog ID.
    pub db_id: u32,
    /// Table catalog ID.
    pub table_id: u32,
    /// Cache ID.
    pub cache_id: u16,
}

impl CatalogRecord for DeleteLastCache {
    const ID: RecordId = record_ids::DELETE_LAST_CACHE;
    const FLAGS: RecordFlags = RecordFlags::none();
    const NAME: &'static str = "DeleteLastCache";

    fn apply(&self, catalog: &mut InnerCatalog) -> Result<(), ApplyError> {
        let db_id = DbId::new(self.db_id);
        let table_id = TableId::new(self.table_id);
        let cache_id = LastCacheId::new(self.cache_id);

        catalog.databases.modify_by_id(&db_id, |db| {
            db.tables.modify_by_id(&table_id, |tbl| {
                tbl.last_caches.remove(&cache_id);
                Ok(())
            })
        })
    }

    fn event(&self) -> CatalogEvent {
        CatalogEvent::LastCacheDeleted {
            db_id: DbId::new(self.db_id),
            table_id: TableId::new(self.table_id),
            cache_id: LastCacheId::new(self.cache_id),
        }
    }
}

inventory::submit! {
    RegisteredRecord::new::<DeleteLastCache>()
}

// ---------------------------------------------------------------------------
// Wire → schema conversions
// ---------------------------------------------------------------------------

impl From<&WireColumnId> for ColumnIdentifier {
    fn from(value: &WireColumnId) -> Self {
        match value {
            WireColumnId::Timestamp => Self::Timestamp,
            WireColumnId::Tag(id) => Self::Tag(TagId::new(*id)),
            WireColumnId::Field(f) => Self::Field(FieldIdentifier::new(
                FieldFamilyId::new(f.family_id),
                FieldId::new(f.field_id),
            )),
        }
    }
}

impl From<ColumnIdentifier> for WireColumnId {
    fn from(value: ColumnIdentifier) -> Self {
        match value {
            ColumnIdentifier::Timestamp => Self::Timestamp,
            ColumnIdentifier::Tag(t) => Self::Tag(t.get()),
            ColumnIdentifier::Field(f) => Self::Field(WireFieldId {
                field_id: f.id().get(),
                family_id: f.field_family_id().get(),
            }),
        }
    }
}

impl From<WireCacheSource> for CacheSource {
    fn from(value: WireCacheSource) -> Self {
        match value {
            WireCacheSource::User => Self::User,
            WireCacheSource::Auto => Self::Auto,
        }
    }
}

impl From<&WireLvcDef> for LastCacheValueColumnsDef {
    fn from(value: &WireLvcDef) -> Self {
        match value {
            WireLvcDef::AllNonKeyColumns => Self::AllNonKeyColumns,
            WireLvcDef::Explicit(cols) => Self::Explicit {
                columns: cols.iter().map(ColumnIdentifier::from).collect(),
            },
        }
    }
}

impl_bitcode_encoding!(
    CreateDistinctCache,
    DeleteDistinctCache,
    CreateLastCache,
    DeleteLastCache,
);

#[cfg(test)]
mod tests;
