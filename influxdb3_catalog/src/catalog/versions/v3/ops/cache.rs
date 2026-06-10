//! Cache operations: Create/Delete DistinctCache and LastCache.

use std::sync::Arc;

use super::CatalogOp;
use crate::CatalogError;
use crate::catalog::versions::v3::inner::InnerCatalog;
use crate::catalog::versions::v3::schema::cache::{
    DistinctCacheDefinition, LastCacheDefinition, LastCacheSize, LastCacheTtl, MaxAge,
    MaxCardinality, RefreshInterval,
};
use crate::catalog::versions::v3::schema::column::ColumnDefinition;
use crate::catalog::versions::v3::schema::node::NodeSpec;
use crate::format::RecordBatch;
use crate::format::records::types::{
    CacheSource as WireCacheSource, ColumnIdentifier as WireColumnId,
    LastCacheValueColumnsDef as WireLvcDef, NodeSpec as WireNodeSpec,
};
use crate::format::records::{
    CreateDistinctCache, CreateLastCache, DeleteDistinctCache, DeleteLastCache,
};
use crate::resource::CatalogResource;
use influxdb3_id::{ColumnIdentifier, DbId, DistinctCacheId, LastCacheId, TableId, TagId};
use schema::{InfluxColumnType, InfluxFieldType};

fn is_valid_distinct_cache_type(def: &ColumnDefinition) -> bool {
    matches!(
        def.column_type(),
        InfluxColumnType::Tag | InfluxColumnType::Field(InfluxFieldType::String),
    )
}

fn is_valid_last_cache_key_col(def: &ColumnDefinition) -> bool {
    matches!(
        def.column_type(),
        InfluxColumnType::Tag
            | InfluxColumnType::Field(
                InfluxFieldType::String
                    | InfluxFieldType::Integer
                    | InfluxFieldType::UInteger
                    | InfluxFieldType::Boolean
            ),
    )
}

// ---------------------------------------------------------------------------
// CreateDistinctCache
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub(crate) enum CreateDistinctCacheArgs {
    Auto {
        db_id: DbId,
        table_id: TableId,
        tag_ids: Vec<TagId>,
        max_cardinality: MaxCardinality,
        max_age: MaxAge,
        lookback_seconds: Option<u64>,
        refresh_interval: Option<RefreshInterval>,
    },
    User {
        db_name: String,
        table_name: String,
        node_spec: NodeSpec,
        cache_name: Option<String>,
        columns: Vec<String>,
        max_cardinality: MaxCardinality,
        max_age: MaxAge,
    },
}

pub(crate) struct CreateDistinctCacheOp {
    db_id: DbId,
    table_id: TableId,
    cache_id: DistinctCacheId,
}

impl CatalogOp for CreateDistinctCacheOp {
    type Input = CreateDistinctCacheArgs;
    type Output = Arc<DistinctCacheDefinition>;

    fn prepare(
        args: &Self::Input,
        catalog: &InnerCatalog,
        records: &mut RecordBatch,
    ) -> Result<Self, CatalogError> {
        match args {
            CreateDistinctCacheArgs::Auto {
                db_id,
                table_id,
                tag_ids,
                max_cardinality,
                max_age,
                lookback_seconds,
                refresh_interval,
            } => {
                if tag_ids.is_empty() {
                    return Err(CatalogError::invalid_configuration(
                        "no tag columns provided when creating auto distinct cache",
                    ));
                }
                let db = catalog
                    .databases
                    .get_by_id(db_id)
                    .ok_or_else(|| CatalogError::NotFound(format!("database id {db_id}")))?;
                let tbl = db
                    .tables
                    .get_by_id(table_id)
                    .ok_or_else(|| CatalogError::NotFound(format!("table id {table_id}")))?;
                let cache_name = DistinctCacheDefinition::auto_cache_name();
                if tbl.distinct_caches.contains_name(&cache_name) {
                    return Err(CatalogError::AlreadyExists);
                }
                let column_ids = tag_ids
                    .iter()
                    .map(|tag_id| {
                        if tbl.tag_columns.contains_id(tag_id) {
                            Ok(WireColumnId::Tag(tag_id.get()))
                        } else {
                            Err(CatalogError::invalid_configuration(format!(
                                "invalid tag id provided: {tag_id}"
                            )))
                        }
                    })
                    .collect::<Result<Vec<_>, _>>()?;

                let cache_id = tbl.distinct_caches.next_id();
                records.push(&CreateDistinctCache {
                    db_id: db.id().get(),
                    table_id: tbl.table_id.get(),
                    node_spec: WireNodeSpec::All,
                    cache_id: cache_id.get(),
                    cache_name: cache_name.to_string(),
                    column_ids,
                    max_cardinality: max_cardinality.to_u64(),
                    max_age_seconds: max_age.as_secs(),
                    source: WireCacheSource::Auto,
                    lookback_seconds: *lookback_seconds,
                    refresh_interval_seconds: refresh_interval.map(|i| i.as_secs()),
                });
                Ok(Self {
                    db_id: *db_id,
                    table_id: *table_id,
                    cache_id,
                })
            }
            CreateDistinctCacheArgs::User {
                db_name,
                table_name,
                node_spec,
                cache_name,
                columns,
                max_cardinality,
                max_age,
            } => {
                let (db, tbl) = super::active_table(catalog, db_name, table_name)?;
                if columns.is_empty() {
                    return Err(CatalogError::invalid_configuration(
                        "no columns provided when creating distinct cache",
                    ));
                }

                let column_ids = columns
                    .iter()
                    .map(|name| {
                        tbl.column_definition(name)
                            .ok_or_else(|| {
                                CatalogError::invalid_configuration(format!(
                                    "invalid column provided: {name}"
                                ))
                            })
                            .and_then(|def| {
                                if is_valid_distinct_cache_type(&def) {
                                    Ok(WireColumnId::from(def.id()))
                                } else {
                                    Err(CatalogError::InvalidDistinctCacheColumnType)
                                }
                            })
                    })
                    .collect::<Result<Vec<_>, _>>()?;

                let resolved_cache_name = cache_name.clone().unwrap_or_else(|| {
                    let col_names: Vec<_> = columns.iter().map(|s| s.as_str()).collect();
                    format!(
                        "{table_name}_{cols}_distinct_cache",
                        cols = col_names.join("_")
                    )
                });
                if tbl.distinct_caches.contains_name(&resolved_cache_name) {
                    return Err(CatalogError::AlreadyExists);
                }

                let cache_id = tbl.distinct_caches.next_id();
                records.push(&CreateDistinctCache {
                    db_id: db.id().get(),
                    table_id: tbl.id().get(),
                    node_spec: node_spec.into(),
                    cache_id: cache_id.get(),
                    cache_name: resolved_cache_name,
                    column_ids,
                    max_cardinality: max_cardinality.to_u64(),
                    max_age_seconds: max_age.as_secs(),
                    source: WireCacheSource::User,
                    lookback_seconds: None,
                    refresh_interval_seconds: None,
                });
                Ok(Self {
                    db_id: db.id(),
                    table_id: tbl.id(),
                    cache_id,
                })
            }
        }
    }

    fn output(&self, catalog: &InnerCatalog) -> Self::Output {
        let db = catalog
            .databases
            .get_by_id(&self.db_id)
            .expect("database should exist");
        let tbl = db
            .tables
            .get_by_id(&self.table_id)
            .expect("table should exist");
        tbl.distinct_caches
            .get_by_id(&self.cache_id)
            .expect("distinct cache should exist after apply")
    }
}

// ---------------------------------------------------------------------------
// DeleteDistinctCache
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub(crate) struct DeleteDistinctCacheArgs {
    pub db_name: String,
    pub table_name: String,
    pub cache_name: String,
}

pub(crate) struct DeleteDistinctCacheOp {
    cache: Arc<DistinctCacheDefinition>,
}

impl CatalogOp for DeleteDistinctCacheOp {
    type Input = DeleteDistinctCacheArgs;
    type Output = Arc<DistinctCacheDefinition>;

    fn prepare(
        args: &Self::Input,
        catalog: &InnerCatalog,
        records: &mut RecordBatch,
    ) -> Result<Self, CatalogError> {
        let (db, tbl) = super::active_table(catalog, &args.db_name, &args.table_name)?;
        let cache = tbl
            .distinct_caches
            .get_by_name(&args.cache_name)
            .ok_or_else(|| CatalogError::NotFound(args.cache_name.clone()))?;
        records.push(&DeleteDistinctCache {
            db_id: db.id().get(),
            table_id: tbl.id().get(),
            cache_id: cache.id().get(),
        });
        Ok(Self { cache })
    }

    fn output(&self, _catalog: &InnerCatalog) -> Self::Output {
        Arc::clone(&self.cache)
    }
}

// ---------------------------------------------------------------------------
// CreateLastCache
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub(crate) struct CreateLastCacheArgs {
    pub db_name: String,
    pub table_name: String,
    pub node_spec: NodeSpec,
    pub cache_name: Option<String>,
    pub key_columns: Option<Vec<String>>,
    pub value_columns: Option<Vec<String>>,
    pub count: LastCacheSize,
    pub ttl: LastCacheTtl,
}

pub(crate) struct CreateLastCacheOp {
    db_id: DbId,
    table_id: TableId,
    cache_id: LastCacheId,
}

impl CatalogOp for CreateLastCacheOp {
    type Input = CreateLastCacheArgs;
    type Output = Arc<LastCacheDefinition>;

    fn prepare(
        args: &Self::Input,
        catalog: &InnerCatalog,
        records: &mut RecordBatch,
    ) -> Result<Self, CatalogError> {
        let (db, tbl) = super::active_table(catalog, &args.db_name, &args.table_name)?;

        let (key_columns, key_names): (Vec<_>, Vec<_>) =
            if let Some(key_columns) = &args.key_columns {
                key_columns
                    .iter()
                    .map(|name| {
                        tbl.column_definition(name)
                            .ok_or_else(|| {
                                CatalogError::invalid_configuration(format!(
                                    "invalid key column provided: {name}"
                                ))
                            })
                            .and_then(|def| {
                                if is_valid_last_cache_key_col(&def) {
                                    Ok((WireColumnId::from(def.id()), name.to_string()))
                                } else {
                                    Err(CatalogError::InvalidLastCacheKeyColumnType)
                                }
                            })
                    })
                    .collect::<Result<Vec<_>, _>>()?
                    .into_iter()
                    .unzip()
            } else {
                tbl.series_key
                    .iter()
                    .map(|id| {
                        let def = tbl
                            .column_definition_by_id(&ColumnIdentifier::tag(*id))
                            .expect("column id in series key should be valid");
                        (WireColumnId::from(def.id()), def.name().to_string())
                    })
                    .unzip()
            };

        let value_columns = if let Some(value_columns) = &args.value_columns {
            let columns = value_columns
                .iter()
                .map(|name| {
                    tbl.column_definition(name)
                        .map(|def| WireColumnId::from(def.id()))
                        .ok_or_else(|| {
                            CatalogError::invalid_configuration(format!(
                                "invalid value column provided: {name}"
                            ))
                        })
                })
                .collect::<Result<Vec<_>, _>>()?;
            WireLvcDef::Explicit(columns)
        } else {
            WireLvcDef::AllNonKeyColumns
        };

        let cache_name = args.cache_name.clone().unwrap_or_else(|| {
            format!(
                "{table_name}_{cols}_last_cache",
                table_name = tbl.name(),
                cols = key_names.join("_")
            )
        });
        if tbl.last_caches.contains_name(&cache_name) {
            return Err(CatalogError::AlreadyExists);
        }

        let cache_id = tbl.last_caches.next_id();
        records.push(&CreateLastCache {
            db_id: db.id().get(),
            table_id: tbl.id().get(),
            id: cache_id.get(),
            node_spec: (&args.node_spec).into(),
            name: cache_name,
            key_columns,
            value_columns,
            count: u64::from(args.count),
            ttl_seconds: args.ttl.as_secs(),
        });
        Ok(Self {
            db_id: db.id(),
            table_id: tbl.id(),
            cache_id,
        })
    }

    fn output(&self, catalog: &InnerCatalog) -> Self::Output {
        let db = catalog
            .databases
            .get_by_id(&self.db_id)
            .expect("database should exist");
        let tbl = db
            .tables
            .get_by_id(&self.table_id)
            .expect("table should exist");
        tbl.last_caches
            .get_by_id(&self.cache_id)
            .expect("last cache should exist after apply")
    }
}

// ---------------------------------------------------------------------------
// DeleteLastCache
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub(crate) struct DeleteLastCacheArgs {
    pub db_name: String,
    pub table_name: String,
    pub cache_name: String,
}

pub(crate) struct DeleteLastCacheOp {
    cache: Arc<LastCacheDefinition>,
}

impl CatalogOp for DeleteLastCacheOp {
    type Input = DeleteLastCacheArgs;
    type Output = Arc<LastCacheDefinition>;

    fn prepare(
        args: &Self::Input,
        catalog: &InnerCatalog,
        records: &mut RecordBatch,
    ) -> Result<Self, CatalogError> {
        let (db, tbl) = super::active_table(catalog, &args.db_name, &args.table_name)?;
        let cache = tbl
            .last_caches
            .get_by_name(&args.cache_name)
            .ok_or_else(|| CatalogError::NotFound(args.cache_name.clone()))?;
        records.push(&DeleteLastCache {
            db_id: db.id().get(),
            table_id: tbl.id().get(),
            cache_id: cache.id().get(),
        });
        Ok(Self { cache })
    }

    fn output(&self, _catalog: &InnerCatalog) -> Self::Output {
        Arc::clone(&self.cache)
    }
}

#[cfg(test)]
mod tests;
