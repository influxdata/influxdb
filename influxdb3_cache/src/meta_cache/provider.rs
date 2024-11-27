use std::{collections::HashMap, sync::Arc, time::Duration};

use anyhow::Context;
use arrow::datatypes::SchemaRef;
use influxdb3_catalog::catalog::{Catalog, TableDefinition};
use influxdb3_id::{DbId, TableId};
use influxdb3_wal::{MetaCacheDefinition, WalContents, WalOp};
use iox_time::TimeProvider;
use parking_lot::RwLock;

use crate::meta_cache::cache::{MaxAge, MaxCardinality};

use super::{
    cache::{CreateMetaCacheArgs, MetaCache},
    CacheError,
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

/// Triple nested map for storing a multiple metadata caches per table.
///
/// That is, the map nesting is `database -> table -> cache name`
type CacheMap = RwLock<HashMap<DbId, HashMap<TableId, HashMap<Arc<str>, MetaCache>>>>;

/// Provides the metadata caches for the running instance of InfluxDB
#[derive(Debug)]
pub struct MetaCacheProvider {
    pub(crate) time_provider: Arc<dyn TimeProvider>,
    pub(crate) catalog: Arc<Catalog>,
    pub(crate) cache_map: CacheMap,
}

impl MetaCacheProvider {
    /// Initialize a [`MetaCacheProvider`] from a [`Catalog`], populating the provider's
    /// `cache_map` from the definitions in the catalog.
    pub fn new_from_catalog(
        time_provider: Arc<dyn TimeProvider>,
        catalog: Arc<Catalog>,
    ) -> Result<Arc<Self>, ProviderError> {
        let provider = Arc::new(MetaCacheProvider {
            time_provider,
            catalog: Arc::clone(&catalog),
            cache_map: Default::default(),
        });
        for db_schema in catalog.list_db_schema() {
            for table_def in db_schema.tables() {
                for (cache_name, cache_def) in table_def.meta_caches() {
                    assert!(
                        provider
                            .create_cache(
                                db_schema.id,
                                Some(cache_name),
                                CreateMetaCacheArgs {
                                    table_def: Arc::clone(&table_def),
                                    max_cardinality: MaxCardinality::try_from(
                                        cache_def.max_cardinality
                                    )?,
                                    max_age: MaxAge::from(Duration::from_secs(
                                        cache_def.max_age_seconds
                                    )),
                                    column_ids: cache_def.column_ids.to_vec()
                                }
                            )?
                            .is_some(),
                        "there should not be duplicated cache definitions in the catalog"
                    )
                }
            }
        }
        Ok(provider)
    }

    /// Initialize a [`MetaCacheProvider`] from a [`Catalog`], populating the provider's
    /// `cache_map` from the definitions in the catalog. This starts a background process that
    /// runs on the provided `eviction_interval` to perform eviction on all of the caches
    /// in the created [`MetaCacheProvider`]'s `cache_map`.
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

    /// Create a new entry in the metadata cache for a given database and parameters.
    ///
    /// If a new cache is created, this will return the [`MetaCacheDefinition`] for the created
    /// cache; otherwise, if the provided arguments are identical to an existing cache, along with
    /// any defaults, then `None` will be returned. It is an error to attempt to create a cache that
    /// overwite an existing one with different parameters.
    ///
    /// The cache name is optional; if not provided, it will be of the form:
    /// ```text
    /// <table_name>_<column_names>_meta_cache
    /// ```
    /// Where `<column_names>` is an `_`-separated list of the column names used in the cache.
    pub fn create_cache(
        &self,
        db_id: DbId,
        cache_name: Option<Arc<str>>,
        CreateMetaCacheArgs {
            table_def,
            max_cardinality,
            max_age,
            column_ids,
        }: CreateMetaCacheArgs,
    ) -> Result<Option<MetaCacheDefinition>, ProviderError> {
        let cache_name = if let Some(cache_name) = cache_name {
            cache_name
        } else {
            format!(
                "{table_name}_{cols}_meta_cache",
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

        let new_cache = MetaCache::new(
            Arc::clone(&self.time_provider),
            CreateMetaCacheArgs {
                table_def: Arc::clone(&table_def),
                max_cardinality,
                max_age,
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

        Ok(Some(MetaCacheDefinition {
            table_id: table_def.table_id,
            table_name: Arc::clone(&table_def.table_name),
            cache_name,
            column_ids,
            max_cardinality: max_cardinality.into(),
            max_age_seconds: max_age.as_seconds(),
        }))
    }

    /// Create a new cache given the database schema and WAL definition. This is useful during WAL
    /// replay.
    pub fn create_from_definition(
        &self,
        db_id: DbId,
        table_def: Arc<TableDefinition>,
        definition: &MetaCacheDefinition,
    ) {
        let meta_cache = MetaCache::new(
            Arc::clone(&self.time_provider),
            CreateMetaCacheArgs {
                table_def,
                max_cardinality: definition
                    .max_cardinality
                    .try_into()
                    .expect("definition should have a valid max_cardinality"),
                max_age: MaxAge::from(Duration::from_secs(definition.max_age_seconds)),
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
            .insert(Arc::clone(&definition.cache_name), meta_cache);
    }

    /// Delete a cache from the provider
    ///
    /// This also cleans up the provider hierarchy, so if the delete leaves a branch for a given
    /// table or its parent database empty, this will remove that branch.
    pub fn delete_cache(
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

fn background_eviction_process(
    provider: Arc<MetaCacheProvider>,
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
