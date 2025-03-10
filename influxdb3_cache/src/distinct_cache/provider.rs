use std::{collections::HashMap, sync::Arc, time::Duration};

use anyhow::Context;
use arrow::datatypes::SchemaRef;
use influxdb3_catalog::{
    catalog::{Catalog, CatalogBroadcastReceiver},
    log::{
        CatalogBatch, CreateDistinctCacheLog, DatabaseCatalogOp, DeleteDistinctCacheLog,
        SoftDeleteTableLog,
    },
};
use influxdb3_id::{DbId, TableId};
use influxdb3_wal::{WalContents, WalOp};
use iox_time::TimeProvider;
use observability_deps::tracing::warn;
use parking_lot::RwLock;
use tokio::sync::broadcast::error::RecvError;

use super::{
    CacheError,
    cache::{CreateDistinctCacheArgs, DistinctCache},
};

#[derive(Debug, thiserror::Error)]
pub enum ProviderError {
    #[error("cache error: {0}")]
    Cache(#[from] CacheError),
    #[error("cache not found")]
    CacheNotFound,
    #[error("unexpected error: {0:#}")]
    Unexpected(#[from] anyhow::Error),
}

/// Triple nested map for storing a multiple distinct value caches per table.
///
/// That is, the map nesting is `database -> table -> cache name`
type CacheMap = RwLock<HashMap<DbId, HashMap<TableId, HashMap<Arc<str>, DistinctCache>>>>;

/// Provides the distinct value caches for the running instance of InfluxDB
#[derive(Debug)]
pub struct DistinctCacheProvider {
    pub(crate) time_provider: Arc<dyn TimeProvider>,
    pub(crate) catalog: Arc<Catalog>,
    pub(crate) cache_map: CacheMap,
}

impl DistinctCacheProvider {
    /// Initialize a [`DistinctCacheProvider`] from a [`Catalog`], populating the provider's
    /// `cache_map` from the definitions in the catalog.
    pub fn new_from_catalog(
        time_provider: Arc<dyn TimeProvider>,
        catalog: Arc<Catalog>,
    ) -> Result<Arc<Self>, ProviderError> {
        let provider = Arc::new(DistinctCacheProvider {
            time_provider,
            catalog: Arc::clone(&catalog),
            cache_map: Default::default(),
        });
        for db_schema in catalog.list_db_schema() {
            for table_def in db_schema.tables() {
                for (cache_name, cache_def) in table_def.distinct_caches() {
                    assert!(
                        provider
                            .create_cache(
                                db_schema.id,
                                Some(cache_name),
                                CreateDistinctCacheArgs {
                                    table_def: Arc::clone(&table_def),
                                    max_cardinality: cache_def.max_cardinality,
                                    max_age: cache_def.max_age_seconds,
                                    column_ids: cache_def.column_ids.to_vec()
                                }
                            )?
                            .is_some(),
                        "there should not be duplicated cache definitions in the catalog"
                    )
                }
            }
        }

        background_catalog_update(Arc::clone(&provider), catalog.subscribe_to_updates());

        Ok(provider)
    }

    /// Initialize a [`DistinctCacheProvider`] from a [`Catalog`], populating the provider's
    /// `cache_map` from the definitions in the catalog. This starts a background process that
    /// runs on the provided `eviction_interval` to perform eviction on all of the caches
    /// in the created [`DistinctCacheProvider`]'s `cache_map`.
    pub fn new_from_catalog_with_background_eviction(
        time_provider: Arc<dyn TimeProvider>,
        catalog: Arc<Catalog>,
        eviction_interval: Duration,
    ) -> Result<Arc<Self>, ProviderError> {
        let provider = Self::new_from_catalog(time_provider, catalog)?;

        background_eviction_process(Arc::clone(&provider), eviction_interval);

        Ok(provider)
    }

    /// Get a particular cache's name and arrow schema
    ///
    /// This is used for the implementation of DataFusion's `TableFunctionImpl` and
    /// `TableProvider` traits as a convenience method for the scenario where there is only a
    /// single cache on a table, and therefore one does not need to specify the cache name
    /// in addition to the db/table identifiers.
    pub(crate) fn get_cache_name_and_schema(
        &self,
        db_id: DbId,
        table_id: TableId,
        cache_name: Option<&str>,
    ) -> Option<(Arc<str>, SchemaRef)> {
        self.cache_map
            .read()
            .get(&db_id)
            .and_then(|db| db.get(&table_id))
            .and_then(|table| {
                if let Some(cache_name) = cache_name {
                    table
                        .get_key_value(cache_name)
                        .map(|(name, mc)| (Arc::clone(name), mc.arrow_schema()))
                } else if table.len() == 1 {
                    table
                        .iter()
                        .map(|(name, mc)| (Arc::clone(name), mc.arrow_schema()))
                        .next()
                } else {
                    None
                }
            })
    }

    /// Create a new entry in the distinct cache for a given database and parameters.
    ///
    /// If a new cache is created, this will return the [`CreateDistinctCacheLog`] for the created
    /// cache; otherwise, if the provided arguments are identical to an existing cache, along with
    /// any defaults, then `None` will be returned. It is an error to attempt to create a cache that
    /// overwite an existing one with different parameters.
    ///
    /// The cache name is optional; if not provided, it will be of the form:
    /// ```text
    /// <table_name>_<column_names>_distinct_cache
    /// ```
    /// Where `<column_names>` is an `_`-separated list of the column names used in the cache.
    pub fn create_cache(
        &self,
        db_id: DbId,
        cache_name: Option<Arc<str>>,
        CreateDistinctCacheArgs {
            table_def,
            max_cardinality,
            max_age: max_age_seconds,
            column_ids,
        }: CreateDistinctCacheArgs,
    ) -> Result<Option<CreateDistinctCacheLog>, ProviderError> {
        let cache_name = if let Some(cache_name) = cache_name {
            cache_name
        } else {
            format!(
                "{table_name}_{cols}_distinct_cache",
                table_name = table_def.table_name,
                cols = column_ids
                    .iter()
                    .map(
                        |id| table_def.column_id_to_name(id).with_context(|| format!(
                            "invalid column id ({id}) encountered in cache creation arguments"
                        ))
                    )
                    .collect::<Result<Vec<_>, anyhow::Error>>()?
                    .join("_")
            )
            .into()
        };

        let new_cache = DistinctCache::new(
            Arc::clone(&self.time_provider),
            CreateDistinctCacheArgs {
                table_def: Arc::clone(&table_def),
                max_cardinality,
                max_age: max_age_seconds,
                column_ids: column_ids.clone(),
            },
        )?;

        let mut lock = self.cache_map.write();
        if let Some(cache) = lock
            .get(&db_id)
            .and_then(|db| db.get(&table_def.table_id))
            .and_then(|table| table.get(&cache_name))
        {
            return cache
                .compare_config(&new_cache)
                .map(|_| None)
                .map_err(Into::into);
        }

        lock.entry(db_id)
            .or_default()
            .entry(table_def.table_id)
            .or_default()
            .insert(Arc::clone(&cache_name), new_cache);

        Ok(Some(CreateDistinctCacheLog {
            table_id: table_def.table_id,
            table_name: Arc::clone(&table_def.table_name),
            cache_name,
            column_ids,
            max_cardinality,
            max_age_seconds,
        }))
    }

    /// Create a new cache given the database schema and WAL definition. This is useful during WAL
    /// replay.
    pub fn create_from_catalog(&self, db_id: DbId, definition: &CreateDistinctCacheLog) {
        let table_def = self
            .catalog
            .db_schema_by_id(&db_id)
            .and_then(|db| db.table_definition_by_id(&definition.table_id))
            .expect("db and table id should be valid in distinct cache log");
        let distinct_cache = DistinctCache::new(
            Arc::clone(&self.time_provider),
            CreateDistinctCacheArgs {
                table_def,
                max_cardinality: definition.max_cardinality,
                max_age: definition.max_age_seconds,
                column_ids: definition.column_ids.to_vec(),
            },
        )
        .expect("definition should be valid coming from the WAL");
        self.cache_map
            .write()
            .entry(db_id)
            .or_default()
            .entry(definition.table_id)
            .or_default()
            .insert(Arc::clone(&definition.cache_name), distinct_cache);
    }

    /// Delete a cache from the provider
    ///
    /// This also cleans up the provider hierarchy, so if the delete leaves a branch for a given
    /// table or its parent database empty, this will remove that branch.
    pub(crate) fn delete_cache(
        &self,
        db_id: &DbId,
        table_id: &TableId,
        cache_name: &str,
    ) -> Result<(), ProviderError> {
        let mut lock = self.cache_map.write();
        let db = lock.get_mut(db_id).ok_or(ProviderError::CacheNotFound)?;
        let table = db.get_mut(table_id).ok_or(ProviderError::CacheNotFound)?;
        table
            .remove(cache_name)
            .ok_or(ProviderError::CacheNotFound)?;
        if table.is_empty() {
            db.remove(table_id);
        }
        if db.is_empty() {
            lock.remove(db_id);
        }
        Ok(())
    }

    /// Delete all caches for a given database
    pub(crate) fn delete_caches_for_db(&self, db_id: &DbId) {
        self.cache_map.write().remove(db_id);
    }

    /// Delete all caches for a given database and table
    pub(crate) fn delete_caches_for_db_and_table(&self, db_id: &DbId, table_id: &TableId) {
        let mut lock = self.cache_map.write();
        let Some(db) = lock.get_mut(db_id) else {
            return;
        };
        db.remove(table_id);
        if db.is_empty() {
            lock.remove(db_id);
        }
    }

    /// Write the contents of a WAL file to the cache by iterating over its database and table
    /// batches to find entries that belong in the cache.
    pub fn write_wal_contents_to_cache(&self, wal_contents: &WalContents) {
        let mut lock = self.cache_map.write();
        for op in &wal_contents.ops {
            let WalOp::Write(write_batch) = op else {
                continue;
            };
            let Some(db_caches) = lock.get_mut(&write_batch.database_id) else {
                continue;
            };
            if db_caches.is_empty() {
                continue;
            }
            for (table_id, table_chunks) in &write_batch.table_chunks {
                let Some(table_caches) = db_caches.get_mut(table_id) else {
                    continue;
                };
                if table_caches.is_empty() {
                    continue;
                }
                for (_, cache) in table_caches.iter_mut() {
                    for chunk in table_chunks.chunk_time_to_chunk.values() {
                        for row in &chunk.rows {
                            cache.push(row);
                        }
                    }
                }
            }
        }
    }

    /// Run eviction across all caches in the provider.
    pub fn evict_cache_entries(&self) {
        let mut lock = self.cache_map.write();
        lock.iter_mut().for_each(|(_, db_caches)| {
            db_caches.iter_mut().for_each(|(_, table_caches)| {
                table_caches.iter_mut().for_each(|(_, cache)| cache.prune())
            })
        });
    }
}

fn background_catalog_update(
    provider: Arc<DistinctCacheProvider>,
    mut subscription: CatalogBroadcastReceiver,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            match subscription.recv().await {
                Ok(catalog_update) => {
                    for batch in catalog_update
                        .batches()
                        .filter_map(CatalogBatch::as_database)
                    {
                        for op in batch.ops.iter() {
                            match op {
                                DatabaseCatalogOp::SoftDeleteDatabase(_) => {
                                    provider.delete_caches_for_db(&batch.database_id);
                                }
                                DatabaseCatalogOp::SoftDeleteTable(SoftDeleteTableLog {
                                    database_id,
                                    table_id,
                                    ..
                                }) => {
                                    provider.delete_caches_for_db_and_table(database_id, table_id);
                                }
                                DatabaseCatalogOp::CreateDistinctCache(log) => {
                                    provider.create_from_catalog(batch.database_id, log);
                                }
                                DatabaseCatalogOp::DeleteDistinctCache(
                                    DeleteDistinctCacheLog {
                                        table_id,
                                        cache_name,
                                        ..
                                    },
                                ) => {
                                    // This only errors when the cache isn't there, so we ignore the
                                    // error...
                                    let _ = provider.delete_cache(
                                        &batch.database_id,
                                        table_id,
                                        cache_name,
                                    );
                                }
                                _ => (),
                            }
                        }
                    }
                }
                Err(RecvError::Closed) => break,
                Err(RecvError::Lagged(num_messages_skipped)) => {
                    // TODO: in this case, we would need to re-initialize the distinct cache provider
                    // from the catalog, if possible.
                    warn!(
                        num_messages_skipped,
                        "distinct cache provider catalog subscription is lagging"
                    );
                }
            }
        }
    })
}

fn background_eviction_process(
    provider: Arc<DistinctCacheProvider>,
    eviction_interval: Duration,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(eviction_interval);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            interval.tick().await;

            provider.evict_cache_entries();
        }
    })
}
