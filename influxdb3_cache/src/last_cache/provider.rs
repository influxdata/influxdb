use std::{collections::HashMap, sync::Arc, time::Duration};

use arrow::{array::RecordBatch, datatypes::SchemaRef as ArrowSchemaRef, error::ArrowError};

use influxdb3_catalog::catalog::{Catalog, TableDefinition};
use influxdb3_id::{DbId, TableId};
use influxdb3_wal::{LastCacheDefinition, LastCacheValueColumnsDef, WalContents, WalOp};
use observability_deps::tracing::debug;
use parking_lot::RwLock;

use super::{
    cache::{LastCache, LastCacheValueColumnsArg, Predicate},
    CreateLastCacheArgs, Error,
};

/// A three level hashmap storing DbId -> TableId -> Cache Name -> LastCache
// TODO: last caches could get a similar ID, e.g., `LastCacheId`
type CacheMap = RwLock<HashMap<DbId, HashMap<TableId, HashMap<Arc<str>, LastCache>>>>;

/// Provides all last-N-value caches for the entire database
pub struct LastCacheProvider {
    pub(crate) catalog: Arc<Catalog>,
    pub(crate) cache_map: CacheMap,
}

impl std::fmt::Debug for LastCacheProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "LastCacheProvider")
    }
}

impl LastCacheProvider {
    /// Initialize a [`LastCacheProvider`] from a [`Catalog`]
    pub fn new_from_catalog(catalog: Arc<Catalog>) -> Result<Arc<Self>, Error> {
        let provider = Arc::new(LastCacheProvider {
            catalog: Arc::clone(&catalog),
            cache_map: Default::default(),
        });
        for db_schema in catalog.list_db_schema() {
            for table_def in db_schema.tables() {
                for (cache_name, cache_def) in table_def.last_caches() {
                    debug!(%cache_name, ?cache_def, "adding last cache from catalog");
                    assert!(
                        provider
                            .create_cache(
                                db_schema.id,
                                Some(cache_name.as_ref()),
                                CreateLastCacheArgs {
                                    table_def: Arc::clone(&table_def),
                                    count: cache_def.count,
                                    ttl: Duration::from_secs(cache_def.ttl).into(),
                                    key_columns:
                                        crate::last_cache::cache::LastCacheKeyColumnsArg::Explicit(
                                            cache_def.key_columns.clone()
                                        ),
                                    value_columns: match &cache_def.value_columns {
                                        LastCacheValueColumnsDef::Explicit { columns } =>
                                            LastCacheValueColumnsArg::Explicit(columns.clone()),
                                        LastCacheValueColumnsDef::AllNonKeyColumns =>
                                            LastCacheValueColumnsArg::AcceptNew,
                                    }
                                }
                            )?
                            .is_some(),
                        "catalog should not contain duplicate last cache definitions"
                    );
                }
            }
        }

        Ok(provider)
    }

    /// Initialize a [`LastCacheProvider`] from a [`Catalog`] and run a background process to
    /// evict expired entries from the cache
    pub fn new_from_catalog_with_background_eviction(
        catalog: Arc<Catalog>,
        eviction_interval: Duration,
    ) -> Result<Arc<Self>, Error> {
        let provider = Self::new_from_catalog(catalog)?;

        background_eviction_process(Arc::clone(&provider), eviction_interval);

        Ok(provider)
    }

    /// Get a particular cache's name and arrow schema
    ///
    /// This is used for the implementation of DataFusion's `TableFunctionImpl` and `TableProvider`
    /// traits.
    pub(crate) fn get_cache_name_and_schema(
        &self,
        db_id: DbId,
        table_id: TableId,
        cache_name: Option<&str>,
    ) -> Option<(Arc<str>, ArrowSchemaRef)> {
        self.cache_map
            .read()
            .get(&db_id)
            .and_then(|db| db.get(&table_id))
            .and_then(|table| {
                if let Some(cache_name) = cache_name {
                    table
                        .get_key_value(cache_name)
                        .map(|(name, lc)| (Arc::clone(name), lc.arrow_schema()))
                } else if table.len() == 1 {
                    table
                        .iter()
                        .map(|(name, lc)| (Arc::clone(name), lc.arrow_schema()))
                        .next()
                } else {
                    None
                }
            })
    }

    /// Get the [`LastCacheDefinition`] for all caches contained in a database
    pub fn get_last_caches_for_db(&self, db: DbId) -> Vec<LastCacheDefinition> {
        let read = self.cache_map.read();
        read.get(&db)
            .map(|table| {
                table
                    .iter()
                    .flat_map(|(table_id, table_map)| {
                        let table_name = self
                            .catalog
                            .db_schema_by_id(&db)
                            .expect("db exists")
                            .table_id_to_name(table_id)
                            .expect("table exists");
                        table_map.iter().map(move |(lc_name, lc)| {
                            lc.to_definition(*table_id, table_name.as_ref(), Arc::clone(lc_name))
                        })
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Create a new entry in the last cache for a given database and table, along with the given
    /// parameters.
    ///
    /// If a new cache is created, it will return its name. If the provided arguments are identical
    /// to an existing cache (along with any defaults), then `None` will be returned.
    pub fn create_cache(
        &self,
        db_id: DbId,
        cache_name: Option<&str>,
        args: CreateLastCacheArgs,
    ) -> Result<Option<LastCacheDefinition>, Error> {
        let table_def = Arc::clone(&args.table_def);
        let last_cache = LastCache::new(args)?;
        let key_column_names = last_cache
            .key_column_ids()
            .iter()
            .map(|id| table_def.column_id_to_name_unchecked(id).to_string())
            .collect::<Vec<String>>();

        // Generate the cache name if it was not provided
        let cache_name = cache_name.map(Arc::from).unwrap_or_else(|| {
            format!(
                "{table_name}_{keys}_last_cache",
                table_name = table_def.table_name,
                keys = key_column_names.join("_")
            )
            .into()
        });

        // set aside some variables used in the return:
        let key_columns = last_cache.key_column_ids().iter().copied().collect();
        let value_columns = last_cache.value_columns().into();
        let count = last_cache.count();
        let ttl = last_cache.ttl_seconds();

        // Check to see if there is already a cache for the same database/table/cache name, and with
        // the exact same configuration. If so, we return None, indicating that the operation did
        // not fail, but that a cache was not created because it already exists. If the underlying
        // configuration of the newly created cache is different than the one that already exists,
        // then this is an error.
        let mut lock = self.cache_map.write();
        if let Some(lc) = lock
            .get(&db_id)
            .and_then(|db| db.get(&table_def.table_id))
            .and_then(|table| table.get(&cache_name))
        {
            return lc.compare_config(&last_cache).map(|_| None);
        }

        lock.entry(db_id)
            .or_default()
            .entry(table_def.table_id)
            .or_default()
            .insert(Arc::clone(&cache_name), last_cache);

        Ok(Some(LastCacheDefinition {
            table_id: table_def.table_id,
            table: Arc::clone(&table_def.table_name),
            name: cache_name,
            key_columns,
            value_columns,
            count,
            ttl,
        }))
    }

    pub fn create_cache_from_definition(
        &self,
        db_id: DbId,
        table_def: Arc<TableDefinition>,
        definition: &LastCacheDefinition,
    ) {
        let last_cache = LastCache::new(CreateLastCacheArgs {
            table_def,
            count: definition.count,
            ttl: Duration::from_secs(definition.ttl).into(),
            key_columns: super::cache::LastCacheKeyColumnsArg::Explicit(
                definition.key_columns.clone(),
            ),
            value_columns: match &definition.value_columns {
                LastCacheValueColumnsDef::Explicit { columns } => {
                    LastCacheValueColumnsArg::Explicit(columns.clone())
                }
                LastCacheValueColumnsDef::AllNonKeyColumns => LastCacheValueColumnsArg::AcceptNew,
            },
        })
        .expect("last cache defined in WAL should be valid");

        self.cache_map
            .write()
            .entry(db_id)
            .or_default()
            .entry(definition.table_id)
            .or_default()
            .insert(Arc::clone(&definition.name), last_cache);
    }

    /// Delete a cache from the provider
    ///
    /// This will also clean up empty levels in the provider hierarchy, so if there are no more
    /// caches for a given table, that table's entry will be removed from the parent map for that
    /// table's database; likewise for the database's entry in the provider's cache map.
    pub fn delete_cache(
        &self,
        db_id: DbId,
        table_id: TableId,
        cache_name: &str,
    ) -> Result<(), Error> {
        let mut lock = self.cache_map.write();

        let Some(db) = lock.get_mut(&db_id) else {
            return Err(Error::CacheDoesNotExist);
        };

        let Some(table) = db.get_mut(&table_id) else {
            return Err(Error::CacheDoesNotExist);
        };

        if table.remove(cache_name).is_none() {
            return Err(Error::CacheDoesNotExist);
        }

        if table.is_empty() {
            db.remove(&table_id);
        }

        if db.is_empty() {
            lock.remove(&db_id);
        }

        Ok(())
    }

    /// Delete all caches for database from the provider
    pub fn delete_caches_for_db(&self, db_id: &DbId) {
        let mut lock = self.cache_map.write();
        lock.remove(db_id);
    }

    /// Delete all caches for table from the provider
    pub fn delete_caches_for_table(&self, db_id: &DbId, table_id: &TableId) {
        let mut write_guard = self.cache_map.write();
        if let Some(tables) = write_guard.get_mut(db_id) {
            tables.remove(table_id);
        }
    }

    /// Write the contents from a wal file into the cache by iterating over its database and table batches
    /// to find entries that belong in the cache.
    ///
    /// Only if rows are newer than the latest entry in the cache will they be entered.
    pub fn write_wal_contents_to_cache(&self, wal_contents: &WalContents) {
        let mut cache_map = self.cache_map.write();
        for op in &wal_contents.ops {
            match op {
                WalOp::Write(batch) => {
                    if let Some(db_cache) = cache_map.get_mut(&batch.database_id) {
                        if db_cache.is_empty() {
                            continue;
                        }
                        let Some(db_schema) = self.catalog.db_schema_by_id(&batch.database_id)
                        else {
                            continue;
                        };
                        for (table_id, table_chunks) in &batch.table_chunks {
                            if let Some(table_cache) = db_cache.get_mut(table_id) {
                                let Some(table_def) = db_schema.table_definition_by_id(table_id)
                                else {
                                    continue;
                                };
                                for (_, last_cache) in table_cache.iter_mut() {
                                    for chunk in table_chunks.chunk_time_to_chunk.values() {
                                        for row in &chunk.rows {
                                            last_cache.push(row, Arc::clone(&table_def));
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                WalOp::Catalog(_) => (),
            }
        }
    }

    /// Recurse down the cache structure to evict expired cache entries, based on their respective
    /// time-to-live (TTL).
    pub fn evict_expired_cache_entries(&self) {
        let mut cache_map = self.cache_map.write();
        cache_map.iter_mut().for_each(|(_, db)| {
            db.iter_mut()
                .for_each(|(_, table)| table.iter_mut().for_each(|(_, lc)| lc.remove_expired()))
        });
    }

    /// Output the records for a given cache as arrow [`RecordBatch`]es
    ///
    /// This method is meant for testing.
    pub fn get_cache_record_batches(
        &self,
        db_id: DbId,
        table_id: TableId,
        cache_name: Option<&str>,
        predicates: &[Predicate],
    ) -> Option<Result<Vec<RecordBatch>, ArrowError>> {
        let table_def = self
            .catalog
            .db_schema_by_id(&db_id)
            .and_then(|db| db.table_definition_by_id(&table_id))
            .expect("valid db and table ids to get table definition");

        self.cache_map
            .read()
            .get(&db_id)
            .and_then(|db| db.get(&table_id))
            .and_then(|table| {
                if let Some(name) = cache_name {
                    table.get(name)
                } else if table.len() == 1 {
                    table.iter().next().map(|(_, lc)| lc)
                } else {
                    None
                }
            })
            .map(|lc| lc.to_record_batches(table_def, predicates))
    }

    /// Returns the total number of caches contained in the provider
    #[cfg(test)]
    pub(crate) fn size(&self) -> usize {
        self.cache_map
            .read()
            .iter()
            .flat_map(|(_, db)| db.iter().flat_map(|(_, table)| table.iter()))
            .count()
    }
}

fn background_eviction_process(
    provider: Arc<LastCacheProvider>,
    eviction_interval: Duration,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(eviction_interval);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            interval.tick().await;

            provider.evict_expired_cache_entries();
        }
    })
}
