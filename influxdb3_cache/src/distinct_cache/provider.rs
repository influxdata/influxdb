use std::{collections::HashMap, sync::Arc, time::Duration};

use super::{
    CacheError,
    cache::{CreateDistinctCacheArgs, DistinctCache},
};
use arrow::datatypes::SchemaRef;
use influxdb3_catalog::catalog::IfNotDeleted;
use influxdb3_catalog::{
    catalog::Catalog,
    channel::CatalogUpdateReceiver,
    log::{
        CatalogBatch, DatabaseCatalogOp, DeleteDistinctCacheLog, DistinctCacheDefinition,
        SoftDeleteTableLog,
    },
};
use influxdb3_id::{DbId, DistinctCacheId, TableId};
use influxdb3_wal::{WalContents, WalOp};
use iox_time::TimeProvider;
use parking_lot::RwLock;

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
/// That is, the map nesting is `database -> table -> cache id`
type CacheMap = RwLock<HashMap<DbId, HashMap<TableId, HashMap<DistinctCacheId, DistinctCache>>>>;

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
    pub async fn new_from_catalog(
        time_provider: Arc<dyn TimeProvider>,
        catalog: Arc<Catalog>,
    ) -> Result<Arc<Self>, ProviderError> {
        let provider = Arc::new(DistinctCacheProvider {
            time_provider,
            catalog: Arc::clone(&catalog),
            cache_map: Default::default(),
        });
        for db_schema in catalog
            .list_db_schema()
            .into_iter()
            .filter_map(IfNotDeleted::if_not_deleted)
        {
            for table_def in db_schema.tables().filter_map(IfNotDeleted::if_not_deleted) {
                for (cache_id, cache_def) in table_def.distinct_caches.iter() {
                    provider.create_cache(
                        db_schema.id,
                        *cache_id,
                        CreateDistinctCacheArgs {
                            table_def: Arc::clone(&table_def),
                            max_cardinality: cache_def.max_cardinality,
                            max_age: cache_def.max_age_seconds,
                            column_ids: cache_def.column_ids.to_vec(),
                        },
                    )?
                }
            }
        }

        background_catalog_update(
            Arc::clone(&provider),
            catalog.subscribe_to_updates("distinct_cache").await,
        );

        Ok(provider)
    }

    /// Initialize a [`DistinctCacheProvider`] from a [`Catalog`], populating the provider's
    /// `cache_map` from the definitions in the catalog. This starts a background process that
    /// runs on the provided `eviction_interval` to perform eviction on all of the caches
    /// in the created [`DistinctCacheProvider`]'s `cache_map`.
    pub async fn new_from_catalog_with_background_eviction(
        time_provider: Arc<dyn TimeProvider>,
        catalog: Arc<Catalog>,
        eviction_interval: Duration,
    ) -> Result<Arc<Self>, ProviderError> {
        let provider = Self::new_from_catalog(time_provider, catalog).await?;

        background_eviction_process(Arc::clone(&provider), eviction_interval);

        Ok(provider)
    }

    /// Get a particular cache's name and arrow schema
    ///
    /// This is used for the implementation of DataFusion's `TableFunctionImpl` and
    /// `TableProvider` traits as a convenience method for the scenario where there is only a
    /// single cache on a table, and therefore one does not need to specify the cache name
    /// in addition to the db/table identifiers.
    pub(crate) fn get_cache_schema(
        &self,
        db_id: DbId,
        table_id: TableId,
        cache_id: DistinctCacheId,
    ) -> Option<SchemaRef> {
        self.cache_map
            .read()
            .get(&db_id)
            .and_then(|db| db.get(&table_id))
            .and_then(|table| table.get(&cache_id))
            .map(|cache| cache.arrow_schema())
    }

    /// Create a new entry in the distinct cache for a given database and parameters.
    pub fn create_cache(
        &self,
        db_id: DbId,
        cache_id: DistinctCacheId,
        CreateDistinctCacheArgs {
            table_def,
            max_cardinality,
            max_age: max_age_seconds,
            column_ids,
        }: CreateDistinctCacheArgs,
    ) -> Result<(), ProviderError> {
        // NOTE(trevor): if cache creation is validated by catalog, this function may not need to be
        // fallible...
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
            .and_then(|table| table.get(&cache_id))
        {
            return cache.compare_config(&new_cache).map_err(Into::into);
        }

        lock.entry(db_id)
            .or_default()
            .entry(table_def.table_id)
            .or_default()
            .insert(cache_id, new_cache);

        Ok(())
    }

    /// Create a new cache given the database schema and WAL definition. This is useful during WAL
    /// replay.
    pub fn create_from_catalog(&self, db_id: DbId, definition: &DistinctCacheDefinition) {
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
            .insert(definition.cache_id, distinct_cache);
    }

    /// Delete a cache from the provider
    ///
    /// This also cleans up the provider hierarchy, so if the delete leaves a branch for a given
    /// table or its parent database empty, this will remove that branch.
    pub(crate) fn delete_cache(
        &self,
        db_id: &DbId,
        table_id: &TableId,
        cache_id: &DistinctCacheId,
    ) -> Result<(), ProviderError> {
        let mut lock = self.cache_map.write();
        let db = lock.get_mut(db_id).ok_or(ProviderError::CacheNotFound)?;
        let table = db.get_mut(table_id).ok_or(ProviderError::CacheNotFound)?;
        table.remove(cache_id).ok_or(ProviderError::CacheNotFound)?;
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
    mut subscription: CatalogUpdateReceiver,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        while let Some(catalog_update) = subscription.recv().await {
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
                        DatabaseCatalogOp::DeleteDistinctCache(DeleteDistinctCacheLog {
                            table_id,
                            cache_id,
                            ..
                        }) => {
                            // This only errors when the cache isn't there, so we ignore the
                            // error...
                            let _ = provider.delete_cache(&batch.database_id, table_id, cache_id);
                        }
                        _ => (),
                    }
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
