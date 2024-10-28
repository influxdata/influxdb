use std::{
    collections::VecDeque,
    sync::Arc,
    time::{Duration, Instant},
};

use arrow::datatypes::SchemaRef;
use arrow::{
    array::{
        new_null_array, ArrayRef, BooleanBuilder, Float64Builder, GenericByteDictionaryBuilder,
        Int64Builder, RecordBatch, StringBuilder, StringDictionaryBuilder,
        TimestampNanosecondBuilder, UInt64Builder,
    },
    datatypes::{
        DataType, Field as ArrowField, FieldRef, GenericStringType, Int32Type,
        SchemaBuilder as ArrowSchemaBuilder, SchemaRef as ArrowSchemaRef,
    },
    error::ArrowError,
};
use datafusion::{
    logical_expr::{expr::InList, BinaryExpr, Expr, Operator},
    scalar::ScalarValue,
};
use hashbrown::{HashMap, HashSet};
use indexmap::IndexMap;
use influxdb3_catalog::catalog::{Catalog, TableDefinition};
use influxdb3_id::TableId;
use influxdb3_id::{ColumnId, DbId};
use influxdb3_wal::{
    Field, FieldData, LastCacheDefinition, LastCacheSize, LastCacheValueColumnsDef, Row,
    WalContents, WalOp,
};
use iox_time::Time;
use parking_lot::RwLock;
use schema::{InfluxColumnType, InfluxFieldType, Schema, TIME_COLUMN_NAME};

mod table_function;
pub use table_function::LastCacheFunction;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("invalid cache size")]
    InvalidCacheSize,
    #[error("last cache already exists for database and table, but it was configured differently: {reason}")]
    CacheAlreadyExists { reason: String },
    #[error("specified column (name: {column_name}) does not exist in the table definition")]
    ColumnDoesNotExistByName { column_name: String },
    #[error("specified key column (id: {column_id}) does not exist in the table schema")]
    KeyColumnDoesNotExist { column_id: ColumnId },
    #[error("specified key column (name: {column_name}) does not exist in the table schema")]
    KeyColumnDoesNotExistByName { column_name: String },
    #[error("key column must be string, int, uint, or bool types")]
    InvalidKeyColumn,
    #[error("specified value column ({column_id}) does not exist in the table schema")]
    ValueColumnDoesNotExist { column_id: ColumnId },
    #[error("requested last cache does not exist")]
    CacheDoesNotExist,
}

impl Error {
    fn cache_already_exists(reason: impl Into<String>) -> Self {
        Self::CacheAlreadyExists {
            reason: reason.into(),
        }
    }
}

/// A three level hashmap storing DbId -> TableId -> Cache Name -> LastCache
// TODO: last caches could get a similar ID, e.g., `LastCacheId`
type CacheMap = RwLock<HashMap<DbId, HashMap<TableId, HashMap<Arc<str>, LastCache>>>>;

/// Provides all last-N-value caches for the entire database
pub struct LastCacheProvider {
    catalog: Arc<Catalog>,
    cache_map: CacheMap,
}

impl std::fmt::Debug for LastCacheProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "LastCacheProvider")
    }
}

/// The default cache time-to-live (TTL) is 4 hours
pub const DEFAULT_CACHE_TTL: Duration = Duration::from_secs(60 * 60 * 4);

/// Arguments to the [`LastCacheProvider::create_cache`] method
pub struct CreateCacheArguments {
    /// The id of the database to create the cache for
    pub db_id: DbId,
    /// The definition of the table for which the cache is being created
    pub table_def: Arc<TableDefinition>,
    /// An optional name for the cache
    ///
    /// The cache name will default to `<table_name>_<keys>_last_cache`
    pub cache_name: Option<Arc<str>>,
    /// The number of values to hold in the created cache
    ///
    /// This will default to 1.
    pub count: Option<usize>,
    /// The time-to-live (TTL) for the created cache
    ///
    /// This will default to [`DEFAULT_CACHE_TTL`]
    pub ttl: Option<Duration>,
    /// The key column names to use in the cache hierarchy
    ///
    /// This will default to:
    /// - the series key columns for a v3 table
    /// - the lexicographically ordered tag set for a v1 table
    pub key_columns: Option<Vec<(ColumnId, Arc<str>)>>,
    /// The value columns to use in the cache
    ///
    /// This will default to all non-key columns. The `time` column is always included.
    pub value_columns: Option<Vec<(ColumnId, Arc<str>)>>,
}

impl LastCacheProvider {
    /// Initialize a [`LastCacheProvider`] from a [`Catalog`]
    pub fn new_from_catalog(catalog: Arc<Catalog>) -> Result<Self, Error> {
        let provider = LastCacheProvider {
            catalog: Arc::clone(&catalog),
            cache_map: Default::default(),
        };
        for db_schema in catalog.list_db_schema() {
            for table_def in db_schema.tables() {
                for (cache_name, cache_def) in table_def.last_caches() {
                    let key_columns = cache_def
                        .key_columns
                        .iter()
                        .map(|id| {
                            table_def
                                .id_to_name(*id)
                                .map(|name| (*id, name))
                                .ok_or_else(|| Error::KeyColumnDoesNotExist { column_id: *id })
                        })
                        .collect::<Result<Vec<_>, Error>>()?;
                    let value_columns = match cache_def.value_columns {
                        LastCacheValueColumnsDef::Explicit { ref columns } => Some(
                            columns
                                .iter()
                                .map(|id| {
                                    table_def
                                        .id_to_name(*id)
                                        .map(|name| (*id, name))
                                        .ok_or_else(|| Error::ValueColumnDoesNotExist {
                                            column_id: *id,
                                        })
                                })
                                .collect::<Result<Vec<_>, Error>>()?,
                        ),
                        LastCacheValueColumnsDef::AllNonKeyColumns => None,
                    };
                    assert!(
                        provider
                            .create_cache(CreateCacheArguments {
                                db_id: db_schema.id,
                                table_def: Arc::clone(&table_def),
                                cache_name: Some(Arc::clone(&cache_name)),
                                count: Some(cache_def.count.into()),
                                ttl: Some(Duration::from_secs(cache_def.ttl)),
                                key_columns: Some(key_columns),
                                value_columns,
                            })?
                            .is_some(),
                        "catalog should not contain duplicate last cache definitions"
                    );
                }
            }
        }
        Ok(provider)
    }

    /// Get a particular cache's name and arrow schema
    ///
    /// This is used for the implementation of DataFusion's `TableFunctionImpl` and `TableProvider`
    /// traits.
    fn get_cache_name_and_schema(
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
                        .map(|(name, lc)| (Arc::clone(&name), Arc::clone(&lc.schema)))
                } else if table.len() == 1 {
                    table
                        .iter()
                        .map(|(name, lc)| (Arc::clone(&name), Arc::clone(&lc.schema)))
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
                            .db_schema_by_id(db)
                            .expect("db exists")
                            .table_id_to_name(*table_id)
                            .expect("table exists");
                        table_map.iter().map(move |(lc_name, lc)| {
                            lc.to_definition(*table_id, table_name.as_ref(), Arc::clone(&lc_name))
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
        CreateCacheArguments {
            db_id,
            table_def,
            cache_name,
            count,
            ttl,
            key_columns,
            value_columns,
        }: CreateCacheArguments,
    ) -> Result<Option<LastCacheDefinition>, Error> {
        let key_columns = if let Some(keys) = key_columns {
            // validate the key columns specified to ensure correct type (string, int, unit, or bool)
            // and that they exist in the table's schema.
            for (col_id, col_name) in keys.iter() {
                use InfluxColumnType::*;
                use InfluxFieldType::*;
                match table_def.schema.field_by_name(&col_name) {
                    Some((
                        Tag | Field(Integer) | Field(UInteger) | Field(String) | Field(Boolean),
                        _,
                    )) => (),
                    Some((_, _)) => return Err(Error::InvalidKeyColumn),
                    None => return Err(Error::KeyColumnDoesNotExist { column_id: *col_id }),
                }
            }
            keys
        } else {
            // use primary key, which defaults to series key if present, then lexicographically
            // ordered tags otherwise, there is no user-defined sort order in the schema, so if that
            // is introduced, we will need to make sure that is accommodated here.
            let mut keys = table_def.schema.primary_key();
            if let Some(&TIME_COLUMN_NAME) = keys.last() {
                keys.pop();
            }
            keys.iter()
                .map(|s| {
                    table_def
                        .name_to_id(Arc::<str>::from(*s))
                        .map(|id| (id, Arc::<str>::from(*s)))
                        .ok_or_else(|| Error::KeyColumnDoesNotExistByName {
                            column_name: s.to_string(),
                        })
                })
                .collect::<Result<Vec<_>, Error>>()?
        };

        // Generate the cache name if it was not provided
        let cache_name = cache_name.unwrap_or_else(|| {
            format!(
                "{table_name}_{keys}_last_cache",
                table_name = table_def.table_name,
                keys = key_columns
                    .iter()
                    .map(|(_, name)| Arc::clone(name))
                    .collect::<Vec<_>>()
                    .join("_")
            )
            .into()
        });

        let accept_new_fields = value_columns.is_none();
        let last_cache_value_columns_def = match &value_columns {
            None => LastCacheValueColumnsDef::AllNonKeyColumns,
            Some(cols) => LastCacheValueColumnsDef::Explicit {
                columns: cols.iter().map(|(id, _)| *id).collect(),
            },
        };

        let cache_schema = self.last_cache_schema_from_schema(
            &table_def.schema,
            key_columns
                .iter()
                .map(|(_, name)| Arc::clone(&name))
                .collect(),
            value_columns.map(|cols| cols.iter().map(|(_, name)| Arc::clone(&name)).collect()),
        );

        let series_key = table_def.series_key.as_ref().map(|sk| sk.as_slice());

        // create the actual last cache:
        let count = count
            .unwrap_or(1)
            .try_into()
            .map_err(|_| Error::InvalidCacheSize)?;
        let ttl = ttl.unwrap_or(DEFAULT_CACHE_TTL);
        let last_cache = LastCache::new(
            count,
            ttl,
            key_columns.clone(),
            cache_schema,
            series_key,
            accept_new_fields,
        );

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
            .insert(cache_name.clone(), last_cache);

        Ok(Some(LastCacheDefinition {
            table_id: table_def.table_id,
            table: Arc::clone(&table_def.table_name),
            name: cache_name,
            key_columns: key_columns.into_iter().map(|(id, _)| id).collect(),
            value_columns: last_cache_value_columns_def,
            count,
            ttl: ttl.as_secs(),
        }))
    }

    fn last_cache_schema_from_schema(
        &self,
        schema: &Schema,
        key_columns: Vec<Arc<str>>,
        value_columns: Option<Vec<Arc<str>>>,
    ) -> SchemaRef {
        let mut schema_builder = ArrowSchemaBuilder::new();
        // Add key columns first:
        for (t, field) in schema
            .iter()
            .filter(|&(_, f)| key_columns.contains(&Arc::<str>::from(f.name().as_str())))
        {
            if let InfluxColumnType::Tag = t {
                // override tags with string type in the schema, because the KeyValue type stores
                // them as strings, and produces them as StringArray when creating RecordBatches:
                schema_builder.push(ArrowField::new(field.name(), DataType::Utf8, false))
            } else {
                schema_builder.push(field.clone());
            };
        }
        // Add value columns second:
        match value_columns {
            Some(cols) => {
                for (_, field) in schema.iter().filter(|&(_, f)| {
                    cols.contains(&Arc::<str>::from(f.name().as_str()))
                        || f.name() == TIME_COLUMN_NAME
                }) {
                    schema_builder.push(field.clone());
                }
            }
            None => {
                for (_, field) in schema
                    .iter()
                    .filter(|&(_, f)| !key_columns.contains(&Arc::<str>::from(f.name().as_str())))
                {
                    schema_builder.push(field.clone());
                }
            }
        }

        Arc::new(schema_builder.finish())
    }

    pub fn create_cache_from_definition(
        &self,
        db_id: DbId,
        table_def: Arc<TableDefinition>,
        definition: &LastCacheDefinition,
    ) {
        let key_columns = definition
            .key_columns
            .iter()
            .map(|id| {
                table_def
                    .id_to_name(*id)
                    .map(|name| (*id, name))
                    .expect("a valid column id for key column")
            })
            .collect::<Vec<(ColumnId, Arc<str>)>>();
        let value_columns = match &definition.value_columns {
            LastCacheValueColumnsDef::AllNonKeyColumns => None,
            LastCacheValueColumnsDef::Explicit { columns } => Some(
                columns
                    .iter()
                    .map(|id| {
                        table_def
                            .id_to_name(*id)
                            .expect("a valid column id for value column")
                    })
                    .collect(),
            ),
        };
        let accept_new_fields = value_columns.is_none();
        let series_key = table_def.series_key.as_ref().map(|sk| sk.as_slice());

        let schema = self.last_cache_schema_from_schema(
            &table_def.schema,
            key_columns
                .iter()
                .map(|(_, name)| Arc::clone(&name))
                .collect(),
            value_columns,
        );

        let last_cache = LastCache::new(
            definition.count,
            Duration::from_secs(definition.ttl),
            key_columns,
            schema,
            series_key,
            accept_new_fields,
        );

        let mut lock = self.cache_map.write();

        lock.entry(db_id)
            .or_default()
            .entry(definition.table_id)
            .or_default()
            .insert(definition.name.clone(), last_cache);
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
                        let Some(db_schema) = self.catalog.db_schema_by_id(batch.database_id)
                        else {
                            continue;
                        };
                        for (table_id, table_chunks) in &batch.table_chunks {
                            if let Some(table_cache) = db_cache.get_mut(table_id) {
                                let Some(table_def) = db_schema.table_definition_by_id(*table_id)
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
    #[cfg(test)]
    pub(crate) fn get_cache_record_batches(
        &self,
        db_id: DbId,
        table_id: TableId,
        cache_name: Option<&str>,
        predicates: &[Predicate],
    ) -> Option<Result<Vec<RecordBatch>, ArrowError>> {
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
            .map(|lc| lc.to_record_batches(predicates))
    }

    /// Returns the total number of caches contained in the provider
    #[cfg(test)]
    fn size(&self) -> usize {
        self.cache_map
            .read()
            .iter()
            .flat_map(|(_, db)| db.iter().flat_map(|(_, table)| table.iter()))
            .count()
    }
}

/// A Last-N-Values Cache
///
/// A hierarchical cache whose structure is determined by a set of `key_columns`, each of which
/// represents a level in the hierarchy. The lowest level of the hierarchy holds the last N values
/// for the field columns in the cache.
#[derive(Debug)]
pub(crate) struct LastCache {
    /// The number of values to hold in the cache
    ///
    /// Once the cache reaches this size, old values will be evicted when new values are pushed in.
    pub(crate) count: LastCacheSize,
    /// The time-to-live (TTL) for values in the cache
    ///
    /// Once values have lived in the cache beyond this [`Duration`], they can be evicted using
    /// the [`remove_expired`][LastCache::remove_expired] method.
    pub(crate) ttl: Duration,
    /// The key columns for this cache, by their IDs
    ///
    /// Uses an [`IndexMap`] for both fast iteration and fast lookup and more importantly, this
    /// map preserves the order of the elements, thereby maintaining the order of the keys in
    /// the cache.
    pub(crate) key_column_id_to_names: Arc<IndexMap<ColumnId, Arc<str>>>,
    /// The key columns for this cache, by their names
    pub(crate) key_column_name_to_ids: Arc<HashMap<Arc<str>, ColumnId>>,
    /// The Arrow Schema for the table that this cache is associated with
    pub(crate) schema: ArrowSchemaRef,
    /// Whether or not this cache accepts newly written fields
    pub(crate) accept_new_fields: bool,
    /// Optionally store the series key for tables that use it for ensuring non-nullability in the
    /// column buffer for series key columns
    ///
    /// We only use this to check for columns that are part of the series key, so we don't care
    /// about the order, and a HashSet is sufficient.
    series_key: Option<HashSet<ColumnId>>,
    /// The internal state of the cache
    state: LastCacheState,
}

impl LastCache {
    /// Create a new [`LastCache`]
    fn new(
        count: LastCacheSize,
        ttl: Duration,
        key_columns: Vec<(ColumnId, Arc<str>)>,
        schema: ArrowSchemaRef,
        series_key: Option<&[ColumnId]>,
        accept_new_fields: bool,
    ) -> Self {
        let mut key_column_ids = IndexMap::new();
        let mut key_column_name_to_ids = HashMap::new();
        for (id, name) in key_columns {
            key_column_ids.insert(id, Arc::clone(&name));
            key_column_name_to_ids.insert(name, id);
        }
        Self {
            count,
            ttl,
            key_column_id_to_names: Arc::new(key_column_ids),
            key_column_name_to_ids: Arc::new(key_column_name_to_ids),
            series_key: series_key.map(|sk| sk.iter().copied().collect()),
            accept_new_fields,
            schema,
            state: LastCacheState::Init,
        }
    }

    /// Compare this cache's configuration with that of another
    fn compare_config(&self, other: &Self) -> Result<(), Error> {
        if self.count != other.count {
            return Err(Error::cache_already_exists(
                "different cache size specified",
            ));
        }
        if self.ttl != other.ttl {
            return Err(Error::cache_already_exists("different ttl specified"));
        }
        if self.key_column_id_to_names != other.key_column_id_to_names {
            return Err(Error::cache_already_exists("key columns are not the same"));
        }
        if self.accept_new_fields != other.accept_new_fields {
            return Err(Error::cache_already_exists(if self.accept_new_fields {
                "new configuration does not accept new fields"
            } else {
                "new configuration accepts new fields while the existing does not"
            }));
        }
        if !self.schema.contains(&other.schema) {
            return Err(Error::cache_already_exists(
                "the schema from specified value columns do not align",
            ));
        }
        if self.series_key != other.series_key {
            return Err(Error::cache_already_exists(
                "the series key is not the same",
            ));
        }
        Ok(())
    }

    /// Push a [`Row`] from the write buffer into the cache
    ///
    /// If a key column is not present in the row, the row will be ignored.
    ///
    /// This accepts a catalog along with database and table ids such that if new columns are added
    /// to the cache for this row, i.e., for a cache that accepts new fields, then the name of the
    /// field can be looked up for the arrow schema.
    ///
    /// # Panics
    ///
    /// This will panic if the internal cache state's keys are out-of-order with respect to the
    /// order of the `key_columns` on this [`LastCache`]
    pub(crate) fn push(&mut self, row: &Row, table_def: Arc<TableDefinition>) {
        let mut target = &mut self.state;
        let mut key_iter = self.key_column_id_to_names.iter().peekable();
        while let (Some((col_id, col_name)), peek) = (key_iter.next(), key_iter.peek()) {
            if target.is_init() {
                *target = LastCacheState::Key(LastCacheKey {
                    column_id: *col_id,
                    column_name: Arc::clone(&col_name),
                    value_map: Default::default(),
                });
            }
            let Some(value) = row
                .fields
                .iter()
                .find(|f| f.id == *col_id)
                .map(|f| KeyValue::from(&f.value))
            else {
                // ignore the row if it does not contain all key columns
                return;
            };
            let cache_key = target.as_key_mut().unwrap();
            assert_eq!(
                &cache_key.column_id, col_id,
                "key columns must match cache key order"
            );
            target = cache_key.value_map.entry(value).or_insert_with(|| {
                if let Some((next_col_id, next_col_name)) = peek {
                    LastCacheState::Key(LastCacheKey {
                        column_id: **next_col_id,
                        column_name: Arc::clone(&next_col_name),
                        value_map: Default::default(),
                    })
                } else {
                    LastCacheState::Store(LastCacheStore::new(
                        self.count.into(),
                        self.ttl,
                        Arc::clone(&table_def),
                        Arc::clone(&self.key_column_id_to_names),
                        self.series_key.as_ref(),
                    ))
                }
            });
        }
        // If there are no key columns we still need to initialize the state the first time:
        if target.is_init() {
            *target = LastCacheState::Store(LastCacheStore::new(
                self.count.into(),
                self.ttl,
                Arc::clone(&table_def),
                Arc::clone(&self.key_column_id_to_names),
                self.series_key.as_ref(),
            ));
        }
        let store = target.as_store_mut().expect(
            "cache target should be the actual store after iterating through all key columns",
        );
        let Some(new_columns) = store.push(row, self.accept_new_fields) else {
            // Unless new columns were added, and we need to update the schema, we are done.
            return;
        };

        let mut sb = ArrowSchemaBuilder::new();
        for f in self.schema.fields().iter() {
            sb.push(Arc::clone(f));
        }
        for (id, data_type) in new_columns {
            let col_name = table_def
                .id_to_name(id)
                .expect("valid column id for new field added to last cache");
            let field = Arc::new(ArrowField::new(col_name.as_ref(), data_type, true));
            sb.try_merge(&field).expect("buffer should have validated incoming writes to prevent against data type conflicts");
        }
        let new_schema = Arc::new(sb.finish());
        self.schema = new_schema;
    }

    /// Produce a set of [`RecordBatch`]es from the cache, using the given set of [`Predicate`]s
    fn to_record_batches(&self, predicates: &[Predicate]) -> Result<Vec<RecordBatch>, ArrowError> {
        // map the provided predicates on to the key columns
        // there may not be predicates provided for each key column, hence the Option
        let predicates: Vec<Option<&Predicate>> = self
            .key_column_id_to_names
            .keys()
            .map(|col_id| predicates.iter().find(|p| p.column_id == *col_id))
            .collect();

        let mut caches = vec![ExtendedLastCacheState {
            state: &self.state,
            additional_columns: vec![],
        }];

        for predicate in predicates {
            if caches.is_empty() {
                return Ok(vec![]);
            }
            let mut new_caches = vec![];
            for c in caches {
                let Some(cache_key) = c.state.as_key() else {
                    continue;
                };
                if let Some(pred) = predicate {
                    let next_states = cache_key.evaluate_predicate(pred);
                    new_caches.extend(next_states.into_iter().map(|(state, value)| {
                        let mut additional_columns = c.additional_columns.clone();
                        additional_columns.push((
                            cache_key.column_id,
                            Arc::clone(&cache_key.column_name),
                            value,
                        ));
                        ExtendedLastCacheState {
                            state,
                            additional_columns,
                        }
                    }));
                } else {
                    new_caches.extend(cache_key.value_map.iter().map(|(v, state)| {
                        let mut additional_columns = c.additional_columns.clone();
                        additional_columns.push((
                            cache_key.column_id,
                            Arc::clone(&cache_key.column_name),
                            v,
                        ));
                        ExtendedLastCacheState {
                            state,
                            additional_columns,
                        }
                    }));
                }
            }
            caches = new_caches;
        }

        caches
            .into_iter()
            .map(|c| c.to_record_batch(&self.schema))
            .collect()
    }

    /// Convert a set of DataFusion filter [`Expr`]s into [`Predicate`]s
    ///
    /// This only handles binary expressions, e.g., `foo = 'bar'`, and will use the `key_columns`
    /// to filter out expressions that do not match key columns in the cache.
    fn convert_filter_exprs(&self, exprs: &[Expr]) -> Vec<Predicate> {
        exprs
            .iter()
            .filter_map(|expr| {
                match expr {
                    Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
                        let col_id = if let Expr::Column(c) = left.as_ref() {
                            self.key_column_name_to_ids.get(c.name()).copied()?
                        } else {
                            return None;
                        };
                        let value = match right.as_ref() {
                            Expr::Literal(ScalarValue::Utf8(Some(v))) => {
                                KeyValue::String(v.to_owned())
                            }
                            Expr::Literal(ScalarValue::Boolean(Some(v))) => KeyValue::Bool(*v),
                            // TODO: handle integer types that can be casted up to i64/u64:
                            Expr::Literal(ScalarValue::Int64(Some(v))) => KeyValue::Int(*v),
                            Expr::Literal(ScalarValue::UInt64(Some(v))) => KeyValue::UInt(*v),
                            _ => return None,
                        };
                        match op {
                            Operator::Eq => Some(Predicate::new_eq(col_id, value)),
                            Operator::NotEq => Some(Predicate::new_not_eq(col_id, value)),
                            _ => None,
                        }
                    }
                    Expr::InList(InList {
                        expr,
                        list,
                        negated,
                    }) => {
                        let col_id = if let Expr::Column(c) = expr.as_ref() {
                            self.key_column_name_to_ids.get(c.name()).copied()?
                        } else {
                            return None;
                        };
                        let values: Vec<KeyValue> = list
                            .iter()
                            .filter_map(|e| match e {
                                Expr::Literal(ScalarValue::Utf8(Some(v))) => {
                                    Some(KeyValue::String(v.to_owned()))
                                }
                                Expr::Literal(ScalarValue::Boolean(Some(v))) => {
                                    Some(KeyValue::Bool(*v))
                                }
                                // TODO: handle integer types that can be casted up to i64/u64:
                                Expr::Literal(ScalarValue::Int64(Some(v))) => {
                                    Some(KeyValue::Int(*v))
                                }
                                Expr::Literal(ScalarValue::UInt64(Some(v))) => {
                                    Some(KeyValue::UInt(*v))
                                }
                                _ => None,
                            })
                            .collect();
                        if *negated {
                            Some(Predicate::new_not_in(col_id, values))
                        } else {
                            Some(Predicate::new_in(col_id, values))
                        }
                    }
                    _ => None,
                }
            })
            .collect()
    }

    /// Remove expired values from the internal cache state
    fn remove_expired(&mut self) {
        self.state.remove_expired();
    }

    /// Convert the `LastCache` into a `LastCacheDefinition`
    fn to_definition(
        &self,
        table_id: TableId,
        table: impl Into<Arc<str>>,
        name: impl Into<Arc<str>>,
    ) -> LastCacheDefinition {
        LastCacheDefinition {
            table_id,
            table: table.into(),
            name: name.into(),
            key_columns: self.key_column_id_to_names.keys().copied().collect(),
            value_columns: if self.accept_new_fields {
                LastCacheValueColumnsDef::AllNonKeyColumns
            } else {
                LastCacheValueColumnsDef::Explicit {
                    columns: self
                        .schema
                        .fields()
                        .iter()
                        .filter_map(|f| {
                            self.key_column_name_to_ids
                                .get(&Arc::<str>::from(f.name().as_str()))
                                .copied()
                        })
                        .collect(),
                }
            },
            count: self.count,
            ttl: self.ttl.as_secs(),
        }
    }
}

/// Extend a [`LastCacheState`] with additional columns
///
/// This is used for scenarios where key column values need to be produced in query outputs. Since
/// They are not stored in the terminal [`LastCacheStore`], we pass them down using this structure.
#[derive(Debug)]
struct ExtendedLastCacheState<'a> {
    state: &'a LastCacheState,
    additional_columns: Vec<(ColumnId, Arc<str>, &'a KeyValue)>,
}

impl<'a> ExtendedLastCacheState<'a> {
    /// Produce a set of [`RecordBatch`]es from this extended state
    ///
    /// This converts any additional columns to arrow arrays which will extend the [`RecordBatch`]es
    /// produced by the inner [`LastCacheStore`]
    ///
    /// # Panics
    ///
    /// This assumes that the `state` is a [`LastCacheStore`] and will panic otherwise.
    fn to_record_batch(&self, output_schema: &ArrowSchemaRef) -> Result<RecordBatch, ArrowError> {
        let store = self
            .state
            .as_store()
            .expect("should only be calling to_record_batch when using a store");
        let n = store.len();
        let extended: Option<(Vec<FieldRef>, Vec<ArrayRef>)> = if self.additional_columns.is_empty()
        {
            None
        } else {
            Some(
                self.additional_columns
                    .iter()
                    .map(|(_id, name, value)| {
                        let field = Arc::new(value.as_arrow_field(name.as_ref()));
                        match value {
                            KeyValue::String(v) => {
                                let mut builder = StringBuilder::new();
                                for _ in 0..n {
                                    builder.append_value(v);
                                }
                                (field, Arc::new(builder.finish()) as ArrayRef)
                            }
                            KeyValue::Int(v) => {
                                let mut builder = Int64Builder::new();
                                for _ in 0..n {
                                    builder.append_value(*v);
                                }
                                (field, Arc::new(builder.finish()) as ArrayRef)
                            }
                            KeyValue::UInt(v) => {
                                let mut builder = UInt64Builder::new();
                                for _ in 0..n {
                                    builder.append_value(*v);
                                }
                                (field, Arc::new(builder.finish()) as ArrayRef)
                            }
                            KeyValue::Bool(v) => {
                                let mut builder = BooleanBuilder::new();
                                for _ in 0..n {
                                    builder.append_value(*v);
                                }
                                (field, Arc::new(builder.finish()) as ArrayRef)
                            }
                        }
                    })
                    .collect(),
            )
        };
        store.to_record_batch(output_schema, extended)
    }
}

/// A predicate used for evaluating key column values in the cache on query
#[derive(Debug, Clone)]
pub(crate) struct Predicate {
    /// The left-hand-side of the predicate as a valid `ColumnId`
    column_id: ColumnId,
    /// The right-hand-side of the predicate
    kind: PredicateKind,
}

impl Predicate {
    fn new_eq(column_id: ColumnId, value: KeyValue) -> Self {
        Self {
            column_id,
            kind: PredicateKind::Eq(value),
        }
    }

    fn new_not_eq(column_id: ColumnId, value: KeyValue) -> Self {
        Self {
            column_id,
            kind: PredicateKind::NotEq(value),
        }
    }

    fn new_in(column_id: ColumnId, values: Vec<KeyValue>) -> Self {
        Self {
            column_id,
            kind: PredicateKind::In(values),
        }
    }

    fn new_not_in(column_id: ColumnId, values: Vec<KeyValue>) -> Self {
        Self {
            column_id,
            kind: PredicateKind::NotIn(values),
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) enum PredicateKind {
    Eq(KeyValue),
    NotEq(KeyValue),
    In(Vec<KeyValue>),
    NotIn(Vec<KeyValue>),
}

/// Represents the hierarchical last cache structure
#[derive(Debug)]
enum LastCacheState {
    /// An initialized state that is used for easy construction of the cache
    Init,
    /// Represents a branch node in the hierarchy of key columns for the cache
    Key(LastCacheKey),
    /// Represents a terminal node in the hierarchy, i.e., the cache of field values
    Store(LastCacheStore),
}

impl LastCacheState {
    fn is_init(&self) -> bool {
        matches!(self, Self::Init)
    }

    fn as_key(&self) -> Option<&LastCacheKey> {
        match self {
            LastCacheState::Key(key) => Some(key),
            LastCacheState::Store(_) | LastCacheState::Init => None,
        }
    }

    fn as_store(&self) -> Option<&LastCacheStore> {
        match self {
            LastCacheState::Key(_) | LastCacheState::Init => None,
            LastCacheState::Store(store) => Some(store),
        }
    }

    fn as_key_mut(&mut self) -> Option<&mut LastCacheKey> {
        match self {
            LastCacheState::Key(key) => Some(key),
            LastCacheState::Store(_) | LastCacheState::Init => None,
        }
    }

    fn as_store_mut(&mut self) -> Option<&mut LastCacheStore> {
        match self {
            LastCacheState::Key(_) | LastCacheState::Init => None,
            LastCacheState::Store(store) => Some(store),
        }
    }

    /// Remove expired values from this [`LastCacheState`]
    fn remove_expired(&mut self) -> bool {
        match self {
            LastCacheState::Key(k) => k.remove_expired(),
            LastCacheState::Store(s) => s.remove_expired(),
            LastCacheState::Init => false,
        }
    }
}

/// Holds a node within a [`LastCache`] for a given key column
#[derive(Debug)]
struct LastCacheKey {
    /// The column's ID
    column_id: ColumnId,
    /// The column's name
    column_name: Arc<str>,
    /// A map of key column value to nested [`LastCacheState`]
    ///
    /// All values should point at either another key or a [`LastCacheStore`]
    value_map: HashMap<KeyValue, LastCacheState>,
}

impl LastCacheKey {
    /// Evaluate the provided [`Predicate`] by using its value to lookup in this [`LastCacheKey`]'s
    /// value map.
    ///
    /// # Panics
    ///
    /// This assumes that a predicate for this [`LastCacheKey`]'s column was provided, and will panic
    /// otherwise.
    fn evaluate_predicate<'a: 'b, 'b>(
        &'a self,
        predicate: &'b Predicate,
    ) -> Vec<(&'a LastCacheState, &'b KeyValue)> {
        if predicate.column_id != self.column_id {
            panic!(
                "attempted to evaluate unexpected predicate with key {} for column with id {}",
                predicate.column_id, self.column_id
            );
        }
        match &predicate.kind {
            PredicateKind::Eq(val) => self
                .value_map
                .get(val)
                .map(|s| vec![(s, val)])
                .unwrap_or_default(),
            PredicateKind::NotEq(val) => self
                .value_map
                .iter()
                .filter_map(|(v, s)| (v != val).then_some((s, v)))
                .collect(),
            PredicateKind::In(vals) => vals
                .iter()
                .filter_map(|v| self.value_map.get(v).map(|s| (s, v)))
                .collect(),
            PredicateKind::NotIn(vals) => self
                .value_map
                .iter()
                .filter_map(|(v, s)| (!vals.contains(v)).then_some((s, v)))
                .collect(),
        }
    }

    /// Remove expired values from any cache nested within this [`LastCacheKey`]
    ///
    /// This will recurse down the cache hierarchy, removing all expired cache values from individual
    /// [`LastCacheStore`]s at the lowest level, then dropping any [`LastCacheStore`] that is
    /// completeley empty. As it walks back up the hierarchy, any [`LastCacheKey`] that is empty will
    /// also be dropped from its parent map.
    fn remove_expired(&mut self) -> bool {
        self.value_map.retain(|_, s| !s.remove_expired());
        self.value_map.is_empty()
    }
}

/// A value for a key column in a [`LastCache`]
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub(crate) enum KeyValue {
    String(String),
    Int(i64),
    UInt(u64),
    Bool(bool),
}

#[cfg(test)]
impl KeyValue {
    fn string(s: impl Into<String>) -> Self {
        Self::String(s.into())
    }
}

impl KeyValue {
    /// Get the corresponding arrow field definition for this [`KeyValue`]
    fn as_arrow_field(&self, name: impl Into<String>) -> ArrowField {
        match self {
            KeyValue::String(_) => ArrowField::new(name, DataType::Utf8, false),
            KeyValue::Int(_) => ArrowField::new(name, DataType::Int64, false),
            KeyValue::UInt(_) => ArrowField::new(name, DataType::UInt64, false),
            KeyValue::Bool(_) => ArrowField::new(name, DataType::Boolean, false),
        }
    }
}

impl From<&FieldData> for KeyValue {
    fn from(field: &FieldData) -> Self {
        match field {
            FieldData::Key(s) | FieldData::Tag(s) | FieldData::String(s) => {
                Self::String(s.to_owned())
            }
            FieldData::Integer(i) => Self::Int(*i),
            FieldData::UInteger(u) => Self::UInt(*u),
            FieldData::Boolean(b) => Self::Bool(*b),
            FieldData::Timestamp(_) => panic!("unexpected time stamp as key value"),
            FieldData::Float(_) => panic!("unexpected float as key value"),
        }
    }
}

/// Stores the cached column data for the field columns of a given [`LastCache`]
#[derive(Debug)]
struct LastCacheStore {
    /// A map of column name to a [`CacheColumn`] which holds the buffer of data for the column
    /// in the cache.
    ///
    /// An `IndexMap` is used for its performance characteristics: namely, fast iteration as well
    /// as fast lookup (see [here][perf]).
    ///
    /// [perf]: https://github.com/indexmap-rs/indexmap?tab=readme-ov-file#performance
    cache: IndexMap<ColumnId, CacheColumn>,
    /// A reference to the key column id lookup for the cache
    key_column_id_to_names: Arc<IndexMap<ColumnId, Arc<str>>>,
    /// A reference to the column name map for the parent cache
    table_def: Arc<TableDefinition>,
    /// A ring buffer holding the instants at which entries in the cache were inserted
    ///
    /// This is used to evict cache values that outlive the `ttl`
    instants: VecDeque<Instant>,
    /// The capacity of the internal cache buffers
    count: usize,
    /// Time-to-live (TTL) for values in the cache
    ttl: Duration,
    /// The timestamp of the last [`Row`] that was pushed into this store from the buffer.
    ///
    /// This is used to ignore rows that are received with older timestamps.
    last_time: Time,
}

impl LastCacheStore {
    /// Create a new [`LastCacheStore`]
    fn new(
        count: usize,
        ttl: Duration,
        table_def: Arc<TableDefinition>,
        key_column_id_to_names: Arc<IndexMap<ColumnId, Arc<str>>>,
        series_keys: Option<&HashSet<ColumnId>>,
    ) -> Self {
        let cache = table_def
            .columns
            .iter()
            .filter_map(|(col_id, col_def)| {
                (!key_column_id_to_names.contains_key(col_id)).then(|| {
                    (
                        *col_id,
                        CacheColumn::new(
                            &col_def.data_type,
                            count,
                            series_keys.is_some_and(|sk| sk.contains(col_id)),
                        ),
                    )
                })
            })
            .collect();
        Self {
            cache,
            key_column_id_to_names,
            table_def,
            instants: VecDeque::with_capacity(count),
            count,
            ttl,
            last_time: Time::from_timestamp_nanos(0),
        }
    }

    /// Get the number of values in the cache.
    fn len(&self) -> usize {
        self.instants.len()
    }

    /// Check if the cache is empty
    fn is_empty(&self) -> bool {
        self.instants.is_empty()
    }

    /// Push a [`Row`] from the buffer into this cache
    ///
    /// If new fields were added to the [`LastCacheStore`] by this push, the return will be a
    /// list of those field's name and arrow [`DataType`], and `None` otherwise.
    fn push(&mut self, row: &Row, accept_new_fields: bool) -> Option<Vec<(ColumnId, DataType)>> {
        if row.time <= self.last_time.timestamp_nanos() {
            return None;
        }
        let mut result = None;
        let mut seen = HashSet::<ColumnId>::new();
        if accept_new_fields {
            // Check the length before any rows are added to ensure that the correct amount
            // of nulls are back-filled when new fields/columns are added:
            let starting_cache_size = self.len();
            for field in row.fields.iter() {
                seen.insert(field.id);
                if let Some(col) = self.cache.get_mut(&field.id) {
                    // In this case, the field already has an entry in the cache, so just push:
                    col.push(&field.value);
                } else if !self.key_column_id_to_names.contains_key(&field.id) {
                    // In this case, there is not an entry for the field in the cache, so if the
                    // value is not one of the key columns, then it is a new field being added.
                    let data_type = data_type_from_buffer_field(field);
                    let col = self
                        .cache
                        .entry(field.id)
                        .or_insert_with(|| CacheColumn::new(&data_type, self.count, false));
                    // Back-fill the new cache entry with nulls, then push the new value:
                    for _ in 0..starting_cache_size {
                        col.push_null();
                    }
                    col.push(&field.value);
                    // Add the new field to the list of new columns returned:
                    result
                        .get_or_insert_with(Vec::new)
                        .push((field.id, DataType::from(&data_type)));
                }
                // There is no else block, because the only alternative would be that this is a
                // key column, which we ignore.
            }
        } else {
            for field in row.fields.iter() {
                seen.insert(field.id);
                if let Some(c) = self.cache.get_mut(&field.id) {
                    c.push(&field.value);
                }
            }
        }
        // Need to check for columns not seen in the buffered row data, to push nulls into
        // those respective cache entries.
        for (id, column) in self.cache.iter_mut() {
            if !seen.contains(id) {
                column.push_null();
            }
        }
        if self.instants.len() == self.count {
            self.instants.pop_back();
        }
        self.instants.push_front(Instant::now());
        self.last_time = Time::from_timestamp_nanos(row.time);
        result
    }

    /// Convert the contents of this cache into a arrow [`RecordBatch`]
    ///
    /// Accepts an optional `extended` argument containing additional columns to add to the
    /// produced set of [`RecordBatch`]es. These are for the scenario where key columns are
    /// included in the outputted batches, as the [`LastCacheStore`] only holds the field columns
    /// for the cache.
    fn to_record_batch(
        &self,
        output_schema: &ArrowSchemaRef,
        extended: Option<(Vec<FieldRef>, Vec<ArrayRef>)>,
    ) -> Result<RecordBatch, ArrowError> {
        let (fields, mut arrays): (Vec<FieldRef>, Vec<ArrayRef>) = output_schema
            .fields()
            .iter()
            .cloned()
            .filter_map(|f| {
                let Some(col_id) = self
                    .table_def
                    .name_to_id(Arc::<str>::from(f.name().as_str()))
                else {
                    return Some(Err(ArrowError::from_external_error(Box::new(
                        Error::ColumnDoesNotExistByName {
                            column_name: f.name().to_string(),
                        },
                    ))));
                };
                if let Some(c) = self.cache.get(&col_id) {
                    Some(Ok((f, c.data.as_array())))
                } else if self.key_column_id_to_names.contains_key(&col_id) {
                    // We prepend key columns with the extended set provided
                    None
                } else {
                    Some(Ok((
                        Arc::clone(&f),
                        new_null_array(f.data_type(), self.len()),
                    )))
                }
            })
            .collect::<Result<(Vec<FieldRef>, Vec<ArrayRef>), ArrowError>>()?;

        let mut sb = ArrowSchemaBuilder::new();
        if let Some((ext_fields, mut ext_arrays)) = extended {
            // If there are key column values being outputted, they get prepended to appear before
            // all value columns in the query output:
            sb.extend(ext_fields);
            ext_arrays.append(&mut arrays);
            arrays = ext_arrays;
        }
        sb.extend(fields);
        RecordBatch::try_new(Arc::new(sb.finish()), arrays)
    }

    /// Remove expired values from the [`LastCacheStore`]
    ///
    /// Returns whether or not the store is empty after expired entries are removed.
    fn remove_expired(&mut self) -> bool {
        while let Some(instant) = self.instants.back() {
            if instant.elapsed() > self.ttl {
                self.instants.pop_back();
            } else {
                break;
            }
        }
        self.cache
            .iter_mut()
            .for_each(|(_, c)| c.truncate(self.instants.len()));
        // reset the last_time if TTL evicts everything from the cache
        if self.is_empty() {
            self.last_time = Time::from_timestamp_nanos(0);
        }
        self.is_empty()
    }
}

/// A column in a [`LastCache`]
///
/// Stores its size so it can evict old data on push. Stores the time-to-live (TTL) in order
/// to remove expired data.
#[derive(Debug)]
struct CacheColumn {
    /// The number of entries the [`CacheColumn`] will hold before evicting old ones on push
    size: usize,
    /// The buffer containing data for the column
    data: CacheColumnData,
}

impl CacheColumn {
    /// Create a new [`CacheColumn`] for the given arrow [`DataType`] and size
    fn new(data_type: &InfluxColumnType, size: usize, is_series_key: bool) -> Self {
        Self {
            size,
            data: CacheColumnData::new(data_type, size, is_series_key),
        }
    }

    /// Push [`FieldData`] from the buffer into this column
    fn push(&mut self, field_data: &FieldData) {
        if self.data.len() >= self.size {
            self.data.pop_back();
        }
        self.data.push_front(field_data);
    }

    fn push_null(&mut self) {
        if self.data.len() >= self.size {
            self.data.pop_back();
        }
        self.data.push_front_null();
    }

    /// Truncate the [`CacheColumn`]. This is useful for evicting expired entries.
    fn truncate(&mut self, len: usize) {
        self.data.truncate(len);
    }
}

/// Enumerated type for storing column data for the cache in a buffer
#[derive(Debug)]
enum CacheColumnData {
    I64(VecDeque<Option<i64>>),
    U64(VecDeque<Option<u64>>),
    F64(VecDeque<Option<f64>>),
    String(VecDeque<Option<String>>),
    Bool(VecDeque<Option<bool>>),
    Tag(VecDeque<Option<String>>),
    Key(VecDeque<String>),
    Time(VecDeque<i64>),
}

impl CacheColumnData {
    /// Create a new [`CacheColumnData`]
    fn new(data_type: &InfluxColumnType, size: usize, is_series_key: bool) -> Self {
        match data_type {
            InfluxColumnType::Tag => {
                if is_series_key {
                    Self::Key(VecDeque::with_capacity(size))
                } else {
                    Self::Tag(VecDeque::with_capacity(size))
                }
            }
            InfluxColumnType::Field(field) => match field {
                InfluxFieldType::Float => Self::F64(VecDeque::with_capacity(size)),
                InfluxFieldType::Integer => Self::I64(VecDeque::with_capacity(size)),
                InfluxFieldType::UInteger => Self::U64(VecDeque::with_capacity(size)),
                InfluxFieldType::String => Self::String(VecDeque::with_capacity(size)),
                InfluxFieldType::Boolean => Self::Bool(VecDeque::with_capacity(size)),
            },
            InfluxColumnType::Timestamp => Self::Time(VecDeque::with_capacity(size)),
        }
    }

    /// Get the length of the [`CacheColumn`]
    fn len(&self) -> usize {
        match self {
            CacheColumnData::I64(buf) => buf.len(),
            CacheColumnData::U64(buf) => buf.len(),
            CacheColumnData::F64(buf) => buf.len(),
            CacheColumnData::String(buf) => buf.len(),
            CacheColumnData::Bool(buf) => buf.len(),
            CacheColumnData::Tag(buf) => buf.len(),
            CacheColumnData::Key(buf) => buf.len(),
            CacheColumnData::Time(buf) => buf.len(),
        }
    }

    /// Pop the oldest element from the [`CacheColumn`]
    fn pop_back(&mut self) {
        match self {
            CacheColumnData::I64(v) => {
                v.pop_back();
            }
            CacheColumnData::U64(v) => {
                v.pop_back();
            }
            CacheColumnData::F64(v) => {
                v.pop_back();
            }
            CacheColumnData::String(v) => {
                v.pop_back();
            }
            CacheColumnData::Bool(v) => {
                v.pop_back();
            }
            CacheColumnData::Tag(v) => {
                v.pop_back();
            }
            CacheColumnData::Key(v) => {
                v.pop_back();
            }
            CacheColumnData::Time(v) => {
                v.pop_back();
            }
        }
    }

    /// Push a new element into the [`CacheColumn`]
    fn push_front(&mut self, field_data: &FieldData) {
        match (field_data, self) {
            (FieldData::Timestamp(val), CacheColumnData::Time(buf)) => buf.push_front(*val),
            (FieldData::Key(val), CacheColumnData::Key(buf)) => buf.push_front(val.to_owned()),
            (FieldData::Tag(val), CacheColumnData::Tag(buf)) => {
                buf.push_front(Some(val.to_owned()))
            }
            (FieldData::String(val), CacheColumnData::String(buf)) => {
                buf.push_front(Some(val.to_owned()))
            }
            (FieldData::Integer(val), CacheColumnData::I64(buf)) => buf.push_front(Some(*val)),
            (FieldData::UInteger(val), CacheColumnData::U64(buf)) => buf.push_front(Some(*val)),
            (FieldData::Float(val), CacheColumnData::F64(buf)) => buf.push_front(Some(*val)),
            (FieldData::Boolean(val), CacheColumnData::Bool(buf)) => buf.push_front(Some(*val)),
            _ => panic!("invalid field data for cache column"),
        }
    }

    fn push_front_null(&mut self) {
        match self {
            CacheColumnData::I64(buf) => buf.push_front(None),
            CacheColumnData::U64(buf) => buf.push_front(None),
            CacheColumnData::F64(buf) => buf.push_front(None),
            CacheColumnData::String(buf) => buf.push_front(None),
            CacheColumnData::Bool(buf) => buf.push_front(None),
            CacheColumnData::Tag(buf) => buf.push_front(None),
            CacheColumnData::Key(_) => panic!("pushed null value to series key column in cache"),
            CacheColumnData::Time(_) => panic!("pushed null value to time column in cache"),
        }
    }

    /// Produce an arrow [`ArrayRef`] from this column for the sake of producing [`RecordBatch`]es
    fn as_array(&self) -> ArrayRef {
        match self {
            CacheColumnData::I64(buf) => {
                let mut b = Int64Builder::new();
                buf.iter().for_each(|val| match val {
                    Some(v) => b.append_value(*v),
                    None => b.append_null(),
                });
                Arc::new(b.finish())
            }
            CacheColumnData::U64(buf) => {
                let mut b = UInt64Builder::new();
                buf.iter().for_each(|val| match val {
                    Some(v) => b.append_value(*v),
                    None => b.append_null(),
                });
                Arc::new(b.finish())
            }
            CacheColumnData::F64(buf) => {
                let mut b = Float64Builder::new();
                buf.iter().for_each(|val| match val {
                    Some(v) => b.append_value(*v),
                    None => b.append_null(),
                });
                Arc::new(b.finish())
            }
            CacheColumnData::String(buf) => {
                let mut b = StringBuilder::new();
                buf.iter().for_each(|val| match val {
                    Some(v) => b.append_value(v),
                    None => b.append_null(),
                });
                Arc::new(b.finish())
            }
            CacheColumnData::Bool(buf) => {
                let mut b = BooleanBuilder::new();
                buf.iter().for_each(|val| match val {
                    Some(v) => b.append_value(*v),
                    None => b.append_null(),
                });
                Arc::new(b.finish())
            }
            CacheColumnData::Tag(buf) => {
                let mut b: GenericByteDictionaryBuilder<Int32Type, GenericStringType<i32>> =
                    StringDictionaryBuilder::new();
                buf.iter().for_each(|val| match val {
                    Some(v) => b.append_value(v),
                    None => b.append_null(),
                });
                Arc::new(b.finish())
            }
            CacheColumnData::Key(buf) => {
                let mut b: GenericByteDictionaryBuilder<Int32Type, GenericStringType<i32>> =
                    StringDictionaryBuilder::new();
                buf.iter().for_each(|val| {
                    b.append_value(val);
                });
                Arc::new(b.finish())
            }
            CacheColumnData::Time(buf) => {
                let mut b = TimestampNanosecondBuilder::new();
                buf.iter().for_each(|val| b.append_value(*val));
                Arc::new(b.finish())
            }
        }
    }

    fn truncate(&mut self, len: usize) {
        match self {
            CacheColumnData::I64(buf) => buf.truncate(len),
            CacheColumnData::U64(buf) => buf.truncate(len),
            CacheColumnData::F64(buf) => buf.truncate(len),
            CacheColumnData::String(buf) => buf.truncate(len),
            CacheColumnData::Bool(buf) => buf.truncate(len),
            CacheColumnData::Tag(buf) => buf.truncate(len),
            CacheColumnData::Key(buf) => buf.truncate(len),
            CacheColumnData::Time(buf) => buf.truncate(len),
        }
    }
}

fn data_type_from_buffer_field(field: &Field) -> InfluxColumnType {
    match field.value {
        FieldData::Timestamp(_) => InfluxColumnType::Timestamp,
        FieldData::Key(_) | FieldData::Tag(_) => InfluxColumnType::Tag,
        FieldData::String(_) => InfluxColumnType::Field(InfluxFieldType::String),
        FieldData::Integer(_) => InfluxColumnType::Field(InfluxFieldType::Integer),
        FieldData::UInteger(_) => InfluxColumnType::Field(InfluxFieldType::UInteger),
        FieldData::Float(_) => InfluxColumnType::Field(InfluxFieldType::Float),
        FieldData::Boolean(_) => InfluxColumnType::Field(InfluxFieldType::Boolean),
    }
}

#[cfg(test)]
mod tests {
    use std::{cmp::Ordering, sync::Arc, time::Duration};

    use crate::{
        last_cache::{KeyValue, LastCacheProvider, Predicate, DEFAULT_CACHE_TTL},
        parquet_cache::test_cached_obj_store_and_oracle,
        persister::Persister,
        write_buffer::WriteBufferImpl,
        Bufferer, LastCacheManager, Precision,
    };
    use ::object_store::{memory::InMemory, ObjectStore};
    use arrow_util::{assert_batches_eq, assert_batches_sorted_eq};
    use bimap::BiHashMap;
    use data_types::NamespaceName;
    use influxdb3_catalog::catalog::{Catalog, DatabaseSchema, TableDefinition};
    use influxdb3_id::{ColumnId, DbId, SerdeVecHashMap, TableId};
    use influxdb3_wal::{LastCacheDefinition, WalConfig};
    use insta::assert_json_snapshot;
    use iox_time::{MockProvider, Time, TimeProvider};

    async fn setup_write_buffer() -> WriteBufferImpl {
        let obj_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let time_provider: Arc<dyn TimeProvider> =
            Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
        let (obj_store, parquet_cache) =
            test_cached_obj_store_and_oracle(obj_store, Arc::clone(&time_provider));
        let persister = Arc::new(Persister::new(obj_store, "test_host"));
        let host_id = Arc::from("sample-host-id");
        let instance_id = Arc::from("sample-instance-id");
        let catalog = Arc::new(Catalog::new(host_id, instance_id));
        WriteBufferImpl::new(
            persister,
            Arc::clone(&catalog),
            Arc::new(LastCacheProvider::new_from_catalog(catalog as _).unwrap()),
            time_provider,
            crate::test_help::make_exec(),
            WalConfig::test_config(),
            Some(parquet_cache),
        )
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn pick_up_latest_write() {
        let db_name = "foo";
        let tbl_name = "cpu";

        let wbuf = setup_write_buffer().await;

        // Do a write to update the catalog with a database and table:
        wbuf.write_lp(
            NamespaceName::new(db_name).unwrap(),
            format!("{tbl_name},host=a,region=us usage=120").as_str(),
            Time::from_timestamp_nanos(1_000),
            false,
            Precision::Nanosecond,
        )
        .await
        .unwrap();

        let (db_id, db_schema) = wbuf.catalog().db_schema_and_id("foo").unwrap();
        let (tbl_id, table_def) = db_schema.table_definition_and_id("cpu").unwrap();
        let col_id = table_def.name_to_id("host").unwrap();

        // Create the last cache:
        wbuf.create_last_cache(
            db_id,
            tbl_id,
            Some("cache"),
            None,
            None,
            Some(vec![(col_id, "host".into())]),
            None,
        )
        .await
        .expect("create the last cache");

        // Do a write to update the last cache:
        wbuf.write_lp(
            NamespaceName::new(db_name).unwrap(),
            format!("{tbl_name},host=a,region=us usage=99").as_str(),
            Time::from_timestamp_nanos(2_000),
            false,
            Precision::Nanosecond,
        )
        .await
        .unwrap();

        let predicates = &[Predicate::new_eq(col_id, KeyValue::string("a"))];

        // Check what is in the last cache:
        let batch = wbuf
            .last_cache_provider()
            .get_cache_record_batches(db_id, tbl_id, None, predicates)
            .unwrap()
            .unwrap();

        assert_batches_eq!(
            [
                "+------+--------+-----------------------------+-------+",
                "| host | region | time                        | usage |",
                "+------+--------+-----------------------------+-------+",
                "| a    | us     | 1970-01-01T00:00:00.000002Z | 99.0  |",
                "+------+--------+-----------------------------+-------+",
            ],
            &batch
        );

        // Do another write and see that the cache only holds the latest value:
        wbuf.write_lp(
            NamespaceName::new(db_name).unwrap(),
            format!("{tbl_name},host=a,region=us usage=88").as_str(),
            Time::from_timestamp_nanos(3_000),
            false,
            Precision::Nanosecond,
        )
        .await
        .unwrap();

        let batch = wbuf
            .last_cache_provider()
            .get_cache_record_batches(db_id, tbl_id, None, predicates)
            .unwrap()
            .unwrap();

        assert_batches_eq!(
            [
                "+------+--------+-----------------------------+-------+",
                "| host | region | time                        | usage |",
                "+------+--------+-----------------------------+-------+",
                "| a    | us     | 1970-01-01T00:00:00.000003Z | 88.0  |",
                "+------+--------+-----------------------------+-------+",
            ],
            &batch
        );
    }

    /// Test to ensure that predicates on caches that contain multiple
    /// key columns work as expected.
    ///
    /// When a cache contains multiple key columns, if only a subset, or none of those key columns
    /// are used as predicates, then the remaining key columns, along with their respective values,
    /// will be returned in the query output.
    ///
    /// For example, give the key columns 'region' and 'host', along with the following query:
    ///
    /// ```sql
    /// SELECT * FROM last_cache('cpu') WHERE region = 'us-east';
    /// ```
    ///
    /// We expect that the query result will include a `host` column, to delineate rows associated
    /// with different host values in the cache.
    #[tokio::test]
    async fn cache_key_column_predicates() {
        let db_name = "foo";
        let tbl_name = "cpu";
        let wbuf = setup_write_buffer().await;

        // Do one write to update the catalog with a db and table:
        wbuf.write_lp(
            NamespaceName::new(db_name).unwrap(),
            format!("{tbl_name},region=us,host=a usage=1").as_str(),
            Time::from_timestamp_nanos(500),
            false,
            Precision::Nanosecond,
        )
        .await
        .unwrap();

        let (db_id, db_schema) = wbuf.catalog().db_schema_and_id("foo").unwrap();
        let (tbl_id, table_def) = db_schema.table_definition_and_id("cpu").unwrap();
        let host_col_id = table_def.name_to_id("host").unwrap();
        let region_col_id = table_def.name_to_id("host").unwrap();

        // Create the last cache with keys on all tag columns:
        wbuf.create_last_cache(
            db_id,
            tbl_id,
            Some("cache"),
            None,
            None,
            Some(vec![
                (region_col_id, "region".into()),
                (host_col_id, "host".into()),
            ]),
            None,
        )
        .await
        .expect("create last cache");

        // Write some lines to fill multiple keys in the cache:
        wbuf.write_lp(
            NamespaceName::new(db_name).unwrap(),
            format!(
                "\
                {tbl_name},region=us,host=a usage=100\n\
                {tbl_name},region=us,host=b usage=80\n\
                {tbl_name},region=us,host=c usage=60\n\
                {tbl_name},region=ca,host=d usage=40\n\
                {tbl_name},region=ca,host=e usage=20\n\
                {tbl_name},region=ca,host=f usage=30\n\
                "
            )
            .as_str(),
            Time::from_timestamp_nanos(1_000),
            false,
            Precision::Nanosecond,
        )
        .await
        .unwrap();

        struct TestCase<'a> {
            predicates: &'a [Predicate],
            expected: &'a [&'a str],
        }

        let test_cases = [
            // Predicate including both key columns only produces value columns from the cache
            TestCase {
                predicates: &[
                    Predicate::new_eq(region_col_id, KeyValue::string("us")),
                    Predicate::new_eq(host_col_id, KeyValue::string("c")),
                ],
                expected: &[
                    "+--------+------+-----------------------------+-------+",
                    "| region | host | time                        | usage |",
                    "+--------+------+-----------------------------+-------+",
                    "| us     | c    | 1970-01-01T00:00:00.000001Z | 60.0  |",
                    "+--------+------+-----------------------------+-------+",
                ],
            },
            // Predicate on only region key column will have host column outputted in addition to
            // the value columns:
            TestCase {
                predicates: &[Predicate::new_eq(region_col_id, KeyValue::string("us"))],
                expected: &[
                    "+--------+------+-----------------------------+-------+",
                    "| region | host | time                        | usage |",
                    "+--------+------+-----------------------------+-------+",
                    "| us     | a    | 1970-01-01T00:00:00.000001Z | 100.0 |",
                    "| us     | b    | 1970-01-01T00:00:00.000001Z | 80.0  |",
                    "| us     | c    | 1970-01-01T00:00:00.000001Z | 60.0  |",
                    "+--------+------+-----------------------------+-------+",
                ],
            },
            // Similar to previous, with a different region predicate:
            TestCase {
                predicates: &[Predicate::new_eq(region_col_id, KeyValue::string("ca"))],
                expected: &[
                    "+--------+------+-----------------------------+-------+",
                    "| region | host | time                        | usage |",
                    "+--------+------+-----------------------------+-------+",
                    "| ca     | d    | 1970-01-01T00:00:00.000001Z | 40.0  |",
                    "| ca     | e    | 1970-01-01T00:00:00.000001Z | 20.0  |",
                    "| ca     | f    | 1970-01-01T00:00:00.000001Z | 30.0  |",
                    "+--------+------+-----------------------------+-------+",
                ],
            },
            // Predicate on only host key column will have region column outputted in addition to
            // the value columns:
            TestCase {
                predicates: &[Predicate::new_eq(host_col_id, KeyValue::string("a"))],
                expected: &[
                    "+--------+------+-----------------------------+-------+",
                    "| region | host | time                        | usage |",
                    "+--------+------+-----------------------------+-------+",
                    "| us     | a    | 1970-01-01T00:00:00.000001Z | 100.0 |",
                    "+--------+------+-----------------------------+-------+",
                ],
            },
            // Omitting all key columns from the predicate will have all key columns included in
            // the query result:
            TestCase {
                predicates: &[],
                expected: &[
                    "+--------+------+-----------------------------+-------+",
                    "| region | host | time                        | usage |",
                    "+--------+------+-----------------------------+-------+",
                    "| ca     | d    | 1970-01-01T00:00:00.000001Z | 40.0  |",
                    "| ca     | e    | 1970-01-01T00:00:00.000001Z | 20.0  |",
                    "| ca     | f    | 1970-01-01T00:00:00.000001Z | 30.0  |",
                    "| us     | a    | 1970-01-01T00:00:00.000001Z | 100.0 |",
                    "| us     | b    | 1970-01-01T00:00:00.000001Z | 80.0  |",
                    "| us     | c    | 1970-01-01T00:00:00.000001Z | 60.0  |",
                    "+--------+------+-----------------------------+-------+",
                ],
            },
            // Using a non-existent key column as a predicate has no effect:
            // TODO: should this be an error?
            TestCase {
                predicates: &[Predicate::new_eq(
                    ColumnId::new(),
                    KeyValue::string("12345"),
                )],
                expected: &[
                    "+--------+------+-----------------------------+-------+",
                    "| region | host | time                        | usage |",
                    "+--------+------+-----------------------------+-------+",
                    "| ca     | d    | 1970-01-01T00:00:00.000001Z | 40.0  |",
                    "| ca     | e    | 1970-01-01T00:00:00.000001Z | 20.0  |",
                    "| ca     | f    | 1970-01-01T00:00:00.000001Z | 30.0  |",
                    "| us     | a    | 1970-01-01T00:00:00.000001Z | 100.0 |",
                    "| us     | b    | 1970-01-01T00:00:00.000001Z | 80.0  |",
                    "| us     | c    | 1970-01-01T00:00:00.000001Z | 60.0  |",
                    "+--------+------+-----------------------------+-------+",
                ],
            },
            // Using a non existent key column value yields empty result set:
            TestCase {
                predicates: &[Predicate::new_eq(region_col_id, KeyValue::string("eu"))],
                expected: &["++", "++"],
            },
            // Using an invalid combination of key column values yields an empty result set:
            TestCase {
                predicates: &[
                    Predicate::new_eq(region_col_id, KeyValue::string("ca")),
                    Predicate::new_eq(host_col_id, KeyValue::string("a")),
                ],
                expected: &["++", "++"],
            },
            // Using a non-existent key column value (for host column) also yields empty result set:
            TestCase {
                predicates: &[Predicate::new_eq(host_col_id, KeyValue::string("g"))],
                expected: &["++", "++"],
            },
            // Using an incorrect type for a key column value in predicate also yields empty result
            // set. TODO: should this be an error?
            TestCase {
                predicates: &[Predicate::new_eq(host_col_id, KeyValue::Bool(true))],
                expected: &["++", "++"],
            },
            // Using a != predicate
            TestCase {
                predicates: &[Predicate::new_not_eq(region_col_id, KeyValue::string("us"))],
                expected: &[
                    "+--------+------+-----------------------------+-------+",
                    "| region | host | time                        | usage |",
                    "+--------+------+-----------------------------+-------+",
                    "| ca     | d    | 1970-01-01T00:00:00.000001Z | 40.0  |",
                    "| ca     | e    | 1970-01-01T00:00:00.000001Z | 20.0  |",
                    "| ca     | f    | 1970-01-01T00:00:00.000001Z | 30.0  |",
                    "+--------+------+-----------------------------+-------+",
                ],
            },
            // Using an IN predicate:
            TestCase {
                predicates: &[Predicate::new_in(
                    host_col_id,
                    vec![KeyValue::string("a"), KeyValue::string("b")],
                )],
                expected: &[
                    "+--------+------+-----------------------------+-------+",
                    "| region | host | time                        | usage |",
                    "+--------+------+-----------------------------+-------+",
                    "| us     | a    | 1970-01-01T00:00:00.000001Z | 100.0 |",
                    "| us     | b    | 1970-01-01T00:00:00.000001Z | 80.0  |",
                    "+--------+------+-----------------------------+-------+",
                ],
            },
            // Using a NOT IN predicate:
            TestCase {
                predicates: &[Predicate::new_not_in(
                    host_col_id,
                    vec![KeyValue::string("a"), KeyValue::string("b")],
                )],
                expected: &[
                    "+--------+------+-----------------------------+-------+",
                    "| region | host | time                        | usage |",
                    "+--------+------+-----------------------------+-------+",
                    "| ca     | d    | 1970-01-01T00:00:00.000001Z | 40.0  |",
                    "| ca     | e    | 1970-01-01T00:00:00.000001Z | 20.0  |",
                    "| ca     | f    | 1970-01-01T00:00:00.000001Z | 30.0  |",
                    "| us     | c    | 1970-01-01T00:00:00.000001Z | 60.0  |",
                    "+--------+------+-----------------------------+-------+",
                ],
            },
        ];

        for t in test_cases {
            let batches = wbuf
                .last_cache_provider()
                .get_cache_record_batches(db_id, tbl_id, None, t.predicates)
                .unwrap()
                .unwrap();

            assert_batches_sorted_eq!(t.expected, &batches);
        }
    }

    #[tokio::test]
    async fn non_default_cache_size() {
        let db_name = "foo";
        let tbl_name = "cpu";
        let wbuf = setup_write_buffer().await;

        // Do one write to update the catalog with a db and table:
        wbuf.write_lp(
            NamespaceName::new(db_name).unwrap(),
            format!("{tbl_name},region=us,host=a usage=1").as_str(),
            Time::from_timestamp_nanos(500),
            false,
            Precision::Nanosecond,
        )
        .await
        .unwrap();

        let (db_id, db_schema) = wbuf.catalog().db_schema_and_id("foo").unwrap();
        let (tbl_id, table_def) = db_schema.table_definition_and_id("cpu").unwrap();
        let host_col_id = table_def.name_to_id("host").unwrap();
        let region_col_id = table_def.name_to_id("host").unwrap();

        // Create the last cache with keys on all tag columns:
        wbuf.create_last_cache(
            db_id,
            tbl_id,
            Some("cache"),
            Some(10),
            None,
            Some(vec![
                (region_col_id, "region".into()),
                (host_col_id, "host".into()),
            ]),
            None,
        )
        .await
        .expect("create last cache");

        // Do several writes to populate the cache:
        struct Write {
            lp: String,
            time: i64,
        }

        let writes = [
            Write {
                lp: format!(
                    "{tbl_name},region=us,host=a usage=100\n\
                    {tbl_name},region=us,host=b usage=80"
                ),
                time: 1_000,
            },
            Write {
                lp: format!(
                    "{tbl_name},region=us,host=a usage=99\n\
                    {tbl_name},region=us,host=b usage=88"
                ),
                time: 1_500,
            },
            Write {
                lp: format!(
                    "{tbl_name},region=us,host=a usage=95\n\
                    {tbl_name},region=us,host=b usage=92"
                ),
                time: 2_000,
            },
            Write {
                lp: format!(
                    "{tbl_name},region=us,host=a usage=90\n\
                    {tbl_name},region=us,host=b usage=99"
                ),
                time: 2_500,
            },
        ];

        for write in writes {
            wbuf.write_lp(
                NamespaceName::new(db_name).unwrap(),
                write.lp.as_str(),
                Time::from_timestamp_nanos(write.time),
                false,
                Precision::Nanosecond,
            )
            .await
            .unwrap();
        }

        struct TestCase<'a> {
            predicates: &'a [Predicate],
            expected: &'a [&'a str],
        }

        let test_cases = [
            TestCase {
                predicates: &[
                    Predicate::new_eq(region_col_id, KeyValue::string("us")),
                    Predicate::new_eq(host_col_id, KeyValue::string("a")),
                ],
                expected: &[
                    "+--------+------+--------------------------------+-------+",
                    "| region | host | time                           | usage |",
                    "+--------+------+--------------------------------+-------+",
                    "| us     | a    | 1970-01-01T00:00:00.000001500Z | 99.0  |",
                    "| us     | a    | 1970-01-01T00:00:00.000001Z    | 100.0 |",
                    "| us     | a    | 1970-01-01T00:00:00.000002500Z | 90.0  |",
                    "| us     | a    | 1970-01-01T00:00:00.000002Z    | 95.0  |",
                    "+--------+------+--------------------------------+-------+",
                ],
            },
            TestCase {
                predicates: &[Predicate::new_eq(region_col_id, KeyValue::string("us"))],
                expected: &[
                    "+--------+------+--------------------------------+-------+",
                    "| region | host | time                           | usage |",
                    "+--------+------+--------------------------------+-------+",
                    "| us     | a    | 1970-01-01T00:00:00.000001500Z | 99.0  |",
                    "| us     | a    | 1970-01-01T00:00:00.000001Z    | 100.0 |",
                    "| us     | a    | 1970-01-01T00:00:00.000002500Z | 90.0  |",
                    "| us     | a    | 1970-01-01T00:00:00.000002Z    | 95.0  |",
                    "| us     | b    | 1970-01-01T00:00:00.000001500Z | 88.0  |",
                    "| us     | b    | 1970-01-01T00:00:00.000001Z    | 80.0  |",
                    "| us     | b    | 1970-01-01T00:00:00.000002500Z | 99.0  |",
                    "| us     | b    | 1970-01-01T00:00:00.000002Z    | 92.0  |",
                    "+--------+------+--------------------------------+-------+",
                ],
            },
            TestCase {
                predicates: &[Predicate::new_eq(host_col_id, KeyValue::string("a"))],
                expected: &[
                    "+--------+------+--------------------------------+-------+",
                    "| region | host | time                           | usage |",
                    "+--------+------+--------------------------------+-------+",
                    "| us     | a    | 1970-01-01T00:00:00.000001500Z | 99.0  |",
                    "| us     | a    | 1970-01-01T00:00:00.000001Z    | 100.0 |",
                    "| us     | a    | 1970-01-01T00:00:00.000002500Z | 90.0  |",
                    "| us     | a    | 1970-01-01T00:00:00.000002Z    | 95.0  |",
                    "+--------+------+--------------------------------+-------+",
                ],
            },
            TestCase {
                predicates: &[Predicate::new_eq(host_col_id, KeyValue::string("b"))],
                expected: &[
                    "+--------+------+--------------------------------+-------+",
                    "| region | host | time                           | usage |",
                    "+--------+------+--------------------------------+-------+",
                    "| us     | b    | 1970-01-01T00:00:00.000001500Z | 88.0  |",
                    "| us     | b    | 1970-01-01T00:00:00.000001Z    | 80.0  |",
                    "| us     | b    | 1970-01-01T00:00:00.000002500Z | 99.0  |",
                    "| us     | b    | 1970-01-01T00:00:00.000002Z    | 92.0  |",
                    "+--------+------+--------------------------------+-------+",
                ],
            },
            TestCase {
                predicates: &[],
                expected: &[
                    "+--------+------+--------------------------------+-------+",
                    "| region | host | time                           | usage |",
                    "+--------+------+--------------------------------+-------+",
                    "| us     | a    | 1970-01-01T00:00:00.000001500Z | 99.0  |",
                    "| us     | a    | 1970-01-01T00:00:00.000001Z    | 100.0 |",
                    "| us     | a    | 1970-01-01T00:00:00.000002500Z | 90.0  |",
                    "| us     | a    | 1970-01-01T00:00:00.000002Z    | 95.0  |",
                    "| us     | b    | 1970-01-01T00:00:00.000001500Z | 88.0  |",
                    "| us     | b    | 1970-01-01T00:00:00.000001Z    | 80.0  |",
                    "| us     | b    | 1970-01-01T00:00:00.000002500Z | 99.0  |",
                    "| us     | b    | 1970-01-01T00:00:00.000002Z    | 92.0  |",
                    "+--------+------+--------------------------------+-------+",
                ],
            },
        ];

        for t in test_cases {
            let batches = wbuf
                .last_cache_provider()
                .get_cache_record_batches(db_id, tbl_id, None, t.predicates)
                .unwrap()
                .unwrap();

            assert_batches_sorted_eq!(t.expected, &batches);
        }
    }

    #[tokio::test]
    async fn cache_ttl() {
        let db_name = "foo";
        let tbl_name = "cpu";
        let wbuf = setup_write_buffer().await;

        // Do one write to update the catalog with a db and table:
        wbuf.write_lp(
            NamespaceName::new(db_name).unwrap(),
            format!("{tbl_name},region=us,host=a usage=1").as_str(),
            Time::from_timestamp_nanos(500),
            false,
            Precision::Nanosecond,
        )
        .await
        .unwrap();

        let (db_id, db_schema) = wbuf.catalog().db_schema_and_id("foo").unwrap();
        let (tbl_id, table_def) = db_schema.table_definition_and_id("cpu").unwrap();
        let host_col_id = table_def.name_to_id("host").unwrap();
        let region_col_id = table_def.name_to_id("host").unwrap();

        // Create the last cache with keys on all tag columns:
        wbuf.create_last_cache(
            db_id,
            tbl_id,
            Some("cache"),
            // use a cache size greater than 1 to ensure the TTL is doing the evicting
            Some(10),
            Some(Duration::from_millis(50)),
            Some(vec![
                (region_col_id, "region".into()),
                (host_col_id, "host".into()),
            ]),
            None,
        )
        .await
        .expect("create last cache");

        // Write some lines to fill the cache:
        wbuf.write_lp(
            NamespaceName::new(db_name).unwrap(),
            format!(
                "\
                {tbl_name},region=us,host=a usage=100\n\
                {tbl_name},region=us,host=b usage=80\n\
                {tbl_name},region=us,host=c usage=60\n\
                {tbl_name},region=ca,host=d usage=40\n\
                {tbl_name},region=ca,host=e usage=20\n\
                {tbl_name},region=ca,host=f usage=30\n\
                "
            )
            .as_str(),
            Time::from_timestamp_nanos(1_000),
            false,
            Precision::Nanosecond,
        )
        .await
        .unwrap();

        // Check the cache for values:
        let predicates = &[
            Predicate::new_eq(region_col_id, KeyValue::string("us")),
            Predicate::new_eq(host_col_id, KeyValue::string("a")),
        ];

        // Check what is in the last cache:
        let batches = wbuf
            .last_cache_provider()
            .get_cache_record_batches(db_id, tbl_id, None, predicates)
            .unwrap()
            .unwrap();

        assert_batches_sorted_eq!(
            [
                "+--------+------+-----------------------------+-------+",
                "| region | host | time                        | usage |",
                "+--------+------+-----------------------------+-------+",
                "| us     | a    | 1970-01-01T00:00:00.000001Z | 100.0 |",
                "+--------+------+-----------------------------+-------+",
            ],
            &batches
        );

        // wait for the TTL to clear the cache
        tokio::time::sleep(Duration::from_millis(100)).await;

        // the last cache eviction only happens when writes are flushed out to the buffer. If
        // no writes are coming in, the last cache will still have data in it. So, we need to write
        // some data to the buffer to trigger the last cache eviction.
        wbuf.write_lp(
            NamespaceName::new(db_name).unwrap(),
            format!(
                "\
                {tbl_name},region=us,host=b usage=200\n\
                "
            )
            .as_str(),
            Time::from_timestamp_nanos(2_000),
            false,
            Precision::Nanosecond,
        )
        .await
        .unwrap();

        // Check what is in the last cache:
        let batches = wbuf
            .last_cache_provider()
            .get_cache_record_batches(db_id, tbl_id, None, predicates)
            .unwrap()
            .unwrap();

        // The cache is completely empty after the TTL evicted data, so it will give back nothing:
        assert_batches_sorted_eq!(["++", "++",], &batches);

        // Ensure that records can be written to the cache again:
        wbuf.write_lp(
            NamespaceName::new(db_name).unwrap(),
            format!(
                "\
                {tbl_name},region=us,host=a usage=333\n\
                "
            )
            .as_str(),
            Time::from_timestamp_nanos(500_000_000),
            false,
            Precision::Nanosecond,
        )
        .await
        .unwrap();

        // Check the cache for values:
        let predicates = &[Predicate::new_eq(host_col_id, KeyValue::string("a"))];

        // Check what is in the last cache:
        let batches = wbuf
            .last_cache_provider()
            .get_cache_record_batches(db_id, tbl_id, None, predicates)
            .unwrap()
            .unwrap();

        assert_batches_sorted_eq!(
            [
                "+--------+------+--------------------------+-------+",
                "| region | host | time                     | usage |",
                "+--------+------+--------------------------+-------+",
                "| us     | a    | 1970-01-01T00:00:00.500Z | 333.0 |",
                "+--------+------+--------------------------+-------+",
            ],
            &batches
        );
    }

    #[tokio::test]
    async fn fields_as_key_columns() {
        let db_name = "cassini_mission";
        let tbl_name = "temp";
        let wbuf = setup_write_buffer().await;

        // Do one write to update the catalog with a db and table:
        wbuf.write_lp(
            NamespaceName::new(db_name).unwrap(),
            format!(
                "{tbl_name},component_id=111 active=true,type=\"camera\",loc=\"port\",reading=150"
            )
            .as_str(),
            Time::from_timestamp_nanos(500),
            false,
            Precision::Nanosecond,
        )
        .await
        .unwrap();

        let (db_id, db_schema) = wbuf.catalog().db_schema_and_id("cassini_mission").unwrap();
        let (tbl_id, table_def) = db_schema.table_definition_and_id("temp").unwrap();
        let component_id_col_id = table_def.name_to_id("component_id").unwrap();
        let active_col_id = table_def.name_to_id("active").unwrap();
        let type_col_id = table_def.name_to_id("type").unwrap();
        let loc_col_id = table_def.name_to_id("loc").unwrap();

        // Create the last cache with keys on some field columns:
        wbuf.create_last_cache(
            db_id,
            tbl_id,
            Some("cache"),
            None,
            Some(Duration::from_millis(50)),
            Some(vec![
                (component_id_col_id, "component_id".into()),
                (active_col_id, "active".into()),
                (type_col_id, "type".into()),
                (loc_col_id, "loc".into()),
            ]),
            None,
        )
        .await
        .expect("create last cache");

        // Write some lines to fill the cache:
        wbuf.write_lp(
            NamespaceName::new(db_name).unwrap(),
            format!(
                "\
                {tbl_name},component_id=111 active=true,type=\"camera\",loc=\"port\",reading=150\n\
                {tbl_name},component_id=222 active=true,type=\"camera\",loc=\"starboard\",reading=250\n\
                {tbl_name},component_id=333 active=true,type=\"camera\",loc=\"fore\",reading=145\n\
                {tbl_name},component_id=444 active=true,type=\"solar-panel\",loc=\"port\",reading=233\n\
                {tbl_name},component_id=555 active=false,type=\"solar-panel\",loc=\"huygens\",reading=200\n\
                {tbl_name},component_id=666 active=false,type=\"comms-dish\",loc=\"huygens\",reading=220\n\
                "
            )
            .as_str(),
            Time::from_timestamp_nanos(1_000),
            false,
            Precision::Nanosecond,
        )
        .await
        .unwrap();

        struct TestCase<'a> {
            predicates: &'a [Predicate],
            expected: &'a [&'a str],
        }

        let test_cases = [
            // No predicates gives everything:
            TestCase {
                predicates: &[],
                expected: &[
                    "+--------------+--------+-------------+-----------+---------+-----------------------------+",
                    "| component_id | active | type        | loc       | reading | time                        |",
                    "+--------------+--------+-------------+-----------+---------+-----------------------------+",
                    "| 111          | true   | camera      | port      | 150.0   | 1970-01-01T00:00:00.000001Z |",
                    "| 222          | true   | camera      | starboard | 250.0   | 1970-01-01T00:00:00.000001Z |",
                    "| 333          | true   | camera      | fore      | 145.0   | 1970-01-01T00:00:00.000001Z |",
                    "| 444          | true   | solar-panel | port      | 233.0   | 1970-01-01T00:00:00.000001Z |",
                    "| 555          | false  | solar-panel | huygens   | 200.0   | 1970-01-01T00:00:00.000001Z |",
                    "| 666          | false  | comms-dish  | huygens   | 220.0   | 1970-01-01T00:00:00.000001Z |",
                    "+--------------+--------+-------------+-----------+---------+-----------------------------+",
                ],
            },
            // Predicates on tag key column work as expected:
            TestCase {
                predicates: &[Predicate::new_eq(component_id_col_id, KeyValue::string("333"))],
                expected: &[
                    "+--------------+--------+--------+------+---------+-----------------------------+",
                    "| component_id | active | type   | loc  | reading | time                        |",
                    "+--------------+--------+--------+------+---------+-----------------------------+",
                    "| 333          | true   | camera | fore | 145.0   | 1970-01-01T00:00:00.000001Z |",
                    "+--------------+--------+--------+------+---------+-----------------------------+",
                ],
            },
            // Predicate on a non-string field key:
            TestCase {
                predicates: &[Predicate::new_eq(active_col_id, KeyValue::Bool(false))],
                expected: &[
                    "+--------------+--------+-------------+---------+---------+-----------------------------+",
                    "| component_id | active | type        | loc     | reading | time                        |",
                    "+--------------+--------+-------------+---------+---------+-----------------------------+",
                    "| 555          | false  | solar-panel | huygens | 200.0   | 1970-01-01T00:00:00.000001Z |",
                    "| 666          | false  | comms-dish  | huygens | 220.0   | 1970-01-01T00:00:00.000001Z |",
                    "+--------------+--------+-------------+---------+---------+-----------------------------+",
                ],
            },
            // Predicate on a string field key:
            TestCase {
                predicates: &[Predicate::new_eq(type_col_id, KeyValue::string("camera"))],
                expected: &[
                    "+--------------+--------+--------+-----------+---------+-----------------------------+",
                    "| component_id | active | type   | loc       | reading | time                        |",
                    "+--------------+--------+--------+-----------+---------+-----------------------------+",
                    "| 111          | true   | camera | port      | 150.0   | 1970-01-01T00:00:00.000001Z |",
                    "| 222          | true   | camera | starboard | 250.0   | 1970-01-01T00:00:00.000001Z |",
                    "| 333          | true   | camera | fore      | 145.0   | 1970-01-01T00:00:00.000001Z |",
                    "+--------------+--------+--------+-----------+---------+-----------------------------+",
                ],
            }
        ];

        for t in test_cases {
            let batches = wbuf
                .last_cache_provider()
                .get_cache_record_batches(db_id, tbl_id, None, t.predicates)
                .unwrap()
                .unwrap();

            assert_batches_sorted_eq!(t.expected, &batches);
        }
    }

    #[tokio::test]
    async fn series_key_as_default() {
        let db_name = "windmills";
        let tbl_name = "wind_speed";
        let wbuf = setup_write_buffer().await;

        // Do one write to update the catalog with a db and table:
        wbuf.write_lp_v3(
            NamespaceName::new(db_name).unwrap(),
            format!("{tbl_name},state/ca/county/napa/farm/10-01 speed=60").as_str(),
            Time::from_timestamp_nanos(500),
            false,
            Precision::Nanosecond,
        )
        .await
        .unwrap();

        let (db_id, db_schema) = wbuf.catalog().db_schema_and_id(db_name).unwrap();
        let (tbl_id, table_def) = db_schema.table_definition_and_id(tbl_name).unwrap();
        let state_col_id = table_def.name_to_id("state").unwrap();
        let county_col_id = table_def.name_to_id("county").unwrap();
        let farm_col_id = table_def.name_to_id("farm").unwrap();

        // Create the last cache with keys on some field columns:
        wbuf.create_last_cache(db_id, tbl_id, Some("cache"), None, None, None, None)
            .await
            .expect("create last cache");

        // Write some lines to fill the cache:
        wbuf.write_lp_v3(
            NamespaceName::new(db_name).unwrap(),
            format!(
                "\
                {tbl_name},state/ca/county/napa/farm/10-01 speed=50\n\
                {tbl_name},state/ca/county/napa/farm/10-02 speed=49\n\
                {tbl_name},state/ca/county/orange/farm/20-01 speed=40\n\
                {tbl_name},state/ca/county/orange/farm/20-02 speed=33\n\
                {tbl_name},state/ca/county/yolo/farm/30-01 speed=62\n\
                {tbl_name},state/ca/county/nevada/farm/40-01 speed=66\n\
                "
            )
            .as_str(),
            Time::from_timestamp_nanos(1_000),
            false,
            Precision::Nanosecond,
        )
        .await
        .unwrap();

        struct TestCase<'a> {
            predicates: &'a [Predicate],
            expected: &'a [&'a str],
        }

        let test_cases = [
            // No predicates yields everything in the cache
            TestCase {
                predicates: &[],
                expected: &[
                    "+-------+--------+-------+-------+-----------------------------+",
                    "| state | county | farm  | speed | time                        |",
                    "+-------+--------+-------+-------+-----------------------------+",
                    "| ca    | napa   | 10-01 | 50.0  | 1970-01-01T00:00:00.000001Z |",
                    "| ca    | napa   | 10-02 | 49.0  | 1970-01-01T00:00:00.000001Z |",
                    "| ca    | nevada | 40-01 | 66.0  | 1970-01-01T00:00:00.000001Z |",
                    "| ca    | orange | 20-01 | 40.0  | 1970-01-01T00:00:00.000001Z |",
                    "| ca    | orange | 20-02 | 33.0  | 1970-01-01T00:00:00.000001Z |",
                    "| ca    | yolo   | 30-01 | 62.0  | 1970-01-01T00:00:00.000001Z |",
                    "+-------+--------+-------+-------+-----------------------------+",
                ],
            },
            // Predicate on state column, which is part of the series key:
            TestCase {
                predicates: &[Predicate::new_eq(state_col_id, KeyValue::string("ca"))],
                expected: &[
                    "+-------+--------+-------+-------+-----------------------------+",
                    "| state | county | farm  | speed | time                        |",
                    "+-------+--------+-------+-------+-----------------------------+",
                    "| ca    | napa   | 10-01 | 50.0  | 1970-01-01T00:00:00.000001Z |",
                    "| ca    | napa   | 10-02 | 49.0  | 1970-01-01T00:00:00.000001Z |",
                    "| ca    | nevada | 40-01 | 66.0  | 1970-01-01T00:00:00.000001Z |",
                    "| ca    | orange | 20-01 | 40.0  | 1970-01-01T00:00:00.000001Z |",
                    "| ca    | orange | 20-02 | 33.0  | 1970-01-01T00:00:00.000001Z |",
                    "| ca    | yolo   | 30-01 | 62.0  | 1970-01-01T00:00:00.000001Z |",
                    "+-------+--------+-------+-------+-----------------------------+",
                ],
            },
            // Predicate on county column, which is part of the series key:
            TestCase {
                predicates: &[Predicate::new_eq(county_col_id, KeyValue::string("napa"))],
                expected: &[
                    "+-------+--------+-------+-------+-----------------------------+",
                    "| state | county | farm  | speed | time                        |",
                    "+-------+--------+-------+-------+-----------------------------+",
                    "| ca    | napa   | 10-01 | 50.0  | 1970-01-01T00:00:00.000001Z |",
                    "| ca    | napa   | 10-02 | 49.0  | 1970-01-01T00:00:00.000001Z |",
                    "+-------+--------+-------+-------+-----------------------------+",
                ],
            },
            // Predicate on farm column, which is part of the series key:
            TestCase {
                predicates: &[Predicate::new_eq(farm_col_id, KeyValue::string("30-01"))],
                expected: &[
                    "+-------+--------+-------+-------+-----------------------------+",
                    "| state | county | farm  | speed | time                        |",
                    "+-------+--------+-------+-------+-----------------------------+",
                    "| ca    | yolo   | 30-01 | 62.0  | 1970-01-01T00:00:00.000001Z |",
                    "+-------+--------+-------+-------+-----------------------------+",
                ],
            },
            // Predicate on all series key columns:
            TestCase {
                predicates: &[
                    Predicate::new_eq(state_col_id, KeyValue::string("ca")),
                    Predicate::new_eq(county_col_id, KeyValue::string("nevada")),
                    Predicate::new_eq(farm_col_id, KeyValue::string("40-01")),
                ],
                expected: &[
                    "+-------+--------+-------+-------+-----------------------------+",
                    "| state | county | farm  | speed | time                        |",
                    "+-------+--------+-------+-------+-----------------------------+",
                    "| ca    | nevada | 40-01 | 66.0  | 1970-01-01T00:00:00.000001Z |",
                    "+-------+--------+-------+-------+-----------------------------+",
                ],
            },
        ];

        for t in test_cases {
            let batches = wbuf
                .last_cache_provider()
                .get_cache_record_batches(db_id, tbl_id, None, t.predicates)
                .unwrap()
                .unwrap();

            assert_batches_sorted_eq!(t.expected, &batches);
        }
    }

    #[tokio::test]
    async fn tag_set_as_default() {
        let db_name = "windmills";
        let tbl_name = "wind_speed";
        let wbuf = setup_write_buffer().await;

        // Do one write to update the catalog with a db and table:
        wbuf.write_lp(
            NamespaceName::new(db_name).unwrap(),
            format!("{tbl_name},state=ca,county=napa,farm=10-01 speed=60").as_str(),
            Time::from_timestamp_nanos(500),
            false,
            Precision::Nanosecond,
        )
        .await
        .unwrap();

        let (db_id, db_schema) = wbuf.catalog().db_schema_and_id(db_name).unwrap();
        let (tbl_id, table_def) = db_schema.table_definition_and_id(tbl_name).unwrap();
        let state_col_id = table_def.name_to_id("state").unwrap();
        let county_col_id = table_def.name_to_id("county").unwrap();
        let farm_col_id = table_def.name_to_id("farm").unwrap();

        // Create the last cache with keys on some field columns:
        wbuf.create_last_cache(db_id, tbl_id, Some("cache"), None, None, None, None)
            .await
            .expect("create last cache");

        // Write some lines to fill the cache:
        wbuf.write_lp(
            NamespaceName::new(db_name).unwrap(),
            format!(
                "\
                {tbl_name},state=ca,county=napa,farm=10-01 speed=50\n\
                {tbl_name},state=ca,county=napa,farm=10-02 speed=49\n\
                {tbl_name},state=ca,county=orange,farm=20-01 speed=40\n\
                {tbl_name},state=ca,county=orange,farm=20-02 speed=33\n\
                {tbl_name},state=ca,county=yolo,farm=30-01 speed=62\n\
                {tbl_name},state=ca,county=nevada,farm=40-01 speed=66\n\
                "
            )
            .as_str(),
            Time::from_timestamp_nanos(1_000),
            false,
            Precision::Nanosecond,
        )
        .await
        .unwrap();

        struct TestCase<'a> {
            predicates: &'a [Predicate],
            expected: &'a [&'a str],
        }

        let test_cases = [
            // No predicates yields everything in the cache
            TestCase {
                predicates: &[],
                expected: &[
                    "+--------+-------+-------+-------+-----------------------------+",
                    "| county | farm  | state | speed | time                        |",
                    "+--------+-------+-------+-------+-----------------------------+",
                    "| napa   | 10-01 | ca    | 50.0  | 1970-01-01T00:00:00.000001Z |",
                    "| napa   | 10-02 | ca    | 49.0  | 1970-01-01T00:00:00.000001Z |",
                    "| nevada | 40-01 | ca    | 66.0  | 1970-01-01T00:00:00.000001Z |",
                    "| orange | 20-01 | ca    | 40.0  | 1970-01-01T00:00:00.000001Z |",
                    "| orange | 20-02 | ca    | 33.0  | 1970-01-01T00:00:00.000001Z |",
                    "| yolo   | 30-01 | ca    | 62.0  | 1970-01-01T00:00:00.000001Z |",
                    "+--------+-------+-------+-------+-----------------------------+",
                ],
            },
            // Predicate on state column, which is part of the series key:
            TestCase {
                predicates: &[Predicate::new_eq(state_col_id, KeyValue::string("ca"))],
                expected: &[
                    "+--------+-------+-------+-------+-----------------------------+",
                    "| county | farm  | state | speed | time                        |",
                    "+--------+-------+-------+-------+-----------------------------+",
                    "| napa   | 10-01 | ca    | 50.0  | 1970-01-01T00:00:00.000001Z |",
                    "| napa   | 10-02 | ca    | 49.0  | 1970-01-01T00:00:00.000001Z |",
                    "| nevada | 40-01 | ca    | 66.0  | 1970-01-01T00:00:00.000001Z |",
                    "| orange | 20-01 | ca    | 40.0  | 1970-01-01T00:00:00.000001Z |",
                    "| orange | 20-02 | ca    | 33.0  | 1970-01-01T00:00:00.000001Z |",
                    "| yolo   | 30-01 | ca    | 62.0  | 1970-01-01T00:00:00.000001Z |",
                    "+--------+-------+-------+-------+-----------------------------+",
                ],
            },
            // Predicate on county column, which is part of the series key:
            TestCase {
                predicates: &[Predicate::new_eq(county_col_id, KeyValue::string("napa"))],
                expected: &[
                    "+--------+-------+-------+-------+-----------------------------+",
                    "| county | farm  | state | speed | time                        |",
                    "+--------+-------+-------+-------+-----------------------------+",
                    "| napa   | 10-01 | ca    | 50.0  | 1970-01-01T00:00:00.000001Z |",
                    "| napa   | 10-02 | ca    | 49.0  | 1970-01-01T00:00:00.000001Z |",
                    "+--------+-------+-------+-------+-----------------------------+",
                ],
            },
            // Predicate on farm column, which is part of the series key:
            TestCase {
                predicates: &[Predicate::new_eq(farm_col_id, KeyValue::string("30-01"))],
                expected: &[
                    "+--------+-------+-------+-------+-----------------------------+",
                    "| county | farm  | state | speed | time                        |",
                    "+--------+-------+-------+-------+-----------------------------+",
                    "| yolo   | 30-01 | ca    | 62.0  | 1970-01-01T00:00:00.000001Z |",
                    "+--------+-------+-------+-------+-----------------------------+",
                ],
            },
            // Predicate on all series key columns:
            TestCase {
                predicates: &[
                    Predicate::new_eq(state_col_id, KeyValue::string("ca")),
                    Predicate::new_eq(county_col_id, KeyValue::string("nevada")),
                    Predicate::new_eq(farm_col_id, KeyValue::string("40-01")),
                ],
                expected: &[
                    "+--------+-------+-------+-------+-----------------------------+",
                    "| county | farm  | state | speed | time                        |",
                    "+--------+-------+-------+-------+-----------------------------+",
                    "| nevada | 40-01 | ca    | 66.0  | 1970-01-01T00:00:00.000001Z |",
                    "+--------+-------+-------+-------+-----------------------------+",
                ],
            },
        ];

        for t in test_cases {
            let batches = wbuf
                .last_cache_provider()
                .get_cache_record_batches(db_id, tbl_id, None, t.predicates)
                .unwrap()
                .unwrap();

            assert_batches_sorted_eq!(t.expected, &batches);
        }
    }

    #[tokio::test]
    async fn null_values() {
        let db_name = "weather";
        let tbl_name = "temp";
        let wbuf = setup_write_buffer().await;

        // Do a write to update catalog
        wbuf.write_lp(
            NamespaceName::new(db_name).unwrap(),
            format!("{tbl_name},province=on,county=bruce,township=kincardine lo=15,hi=21,avg=18")
                .as_str(),
            Time::from_timestamp_nanos(500),
            false,
            Precision::Nanosecond,
        )
        .await
        .unwrap();

        let (db_id, db_schema) = wbuf.catalog().db_schema_and_id(db_name).unwrap();
        let tbl_id = db_schema.table_name_to_id(tbl_name).unwrap();

        // Create the last cache using default tags as keys
        wbuf.create_last_cache(db_id, tbl_id, None, Some(10), None, None, None)
            .await
            .expect("create last cache");

        // Write some lines to fill the cache, but omit fields to produce nulls:
        wbuf.write_lp(
            NamespaceName::new(db_name).unwrap(),
            format!(
                "\
                {tbl_name},province=on,county=bruce,township=kincardine hi=21,avg=18\n\
                {tbl_name},province=on,county=huron,township=goderich lo=16,hi=22\n\
                {tbl_name},province=on,county=bruce,township=culrock lo=13,avg=15\n\
                {tbl_name},province=on,county=wentworth,township=ancaster lo=18,hi=23,avg=20\n\
                {tbl_name},province=on,county=york,township=york lo=20\n\
                {tbl_name},province=on,county=welland,township=bertie avg=20\n\
                "
            )
            .as_str(),
            Time::from_timestamp_nanos(1_000),
            false,
            Precision::Nanosecond,
        )
        .await
        .unwrap();

        let batches = wbuf
            .last_cache_provider()
            .get_cache_record_batches(db_id, tbl_id, None, &[])
            .unwrap()
            .unwrap();

        assert_batches_sorted_eq!(
            [
                "+-----------+----------+------------+------+------+------+-----------------------------+",
                "| county    | province | township   | avg  | hi   | lo   | time                        |",
                "+-----------+----------+------------+------+------+------+-----------------------------+",
                "| bruce     | on       | culrock    | 15.0 |      | 13.0 | 1970-01-01T00:00:00.000001Z |",
                "| bruce     | on       | kincardine | 18.0 | 21.0 |      | 1970-01-01T00:00:00.000001Z |",
                "| huron     | on       | goderich   |      | 22.0 | 16.0 | 1970-01-01T00:00:00.000001Z |",
                "| welland   | on       | bertie     | 20.0 |      |      | 1970-01-01T00:00:00.000001Z |",
                "| wentworth | on       | ancaster   | 20.0 | 23.0 | 18.0 | 1970-01-01T00:00:00.000001Z |",
                "| york      | on       | york       |      |      | 20.0 | 1970-01-01T00:00:00.000001Z |",
                "+-----------+----------+------------+------+------+------+-----------------------------+",
            ],
            &batches
        );
    }

    #[tokio::test]
    async fn new_fields_added_to_default_cache() {
        let db_name = "nhl_stats";
        let tbl_name = "plays";
        let wbuf = setup_write_buffer().await;

        // Do a write to setup catalog:
        wbuf.write_lp(
            NamespaceName::new(db_name).unwrap(),
            format!(r#"{tbl_name},game_id=1 type="shot",player="kessel""#).as_str(),
            Time::from_timestamp_nanos(500),
            false,
            Precision::Nanosecond,
        )
        .await
        .unwrap();

        let (db_id, db_schema) = wbuf.catalog().db_schema_and_id(db_name).unwrap();
        let (tbl_id, table_def) = db_schema.table_definition_and_id(tbl_name).unwrap();
        let game_id_col_id = table_def.name_to_id("game_id").unwrap();

        // Create the last cache using default tags as keys
        wbuf.create_last_cache(db_id, tbl_id, None, Some(10), None, None, None)
            .await
            .expect("create last cache");

        // Write some lines to fill the cache. The last two lines include a new field "zone" which
        // should be added and appear in queries:
        wbuf.write_lp(
            NamespaceName::new(db_name).unwrap(),
            format!(
                "\
                {tbl_name},game_id=1 type=\"shot\",player=\"mackinnon\"\n\
                {tbl_name},game_id=2 type=\"shot\",player=\"matthews\"\n\
                {tbl_name},game_id=3 type=\"hit\",player=\"tkachuk\",zone=\"away\"\n\
                {tbl_name},game_id=4 type=\"save\",player=\"bobrovsky\",zone=\"home\"\n\
                "
            )
            .as_str(),
            Time::from_timestamp_nanos(1_000),
            false,
            Precision::Nanosecond,
        )
        .await
        .unwrap();

        struct TestCase<'a> {
            predicates: &'a [Predicate],
            expected: &'a [&'a str],
        }

        let test_cases = [
            // Cache that has values in the zone columns should produce them:
            TestCase {
                predicates: &[Predicate::new_eq(game_id_col_id, KeyValue::string("4"))],
                expected: &[
                    "+---------+-----------+-----------------------------+------+------+",
                    "| game_id | player    | time                        | type | zone |",
                    "+---------+-----------+-----------------------------+------+------+",
                    "| 4       | bobrovsky | 1970-01-01T00:00:00.000001Z | save | home |",
                    "+---------+-----------+-----------------------------+------+------+",
                ],
            },
            // Cache that does not have a zone column will produce it with nulls:
            TestCase {
                predicates: &[Predicate::new_eq(game_id_col_id, KeyValue::string("1"))],
                expected: &[
                    "+---------+-----------+-----------------------------+------+------+",
                    "| game_id | player    | time                        | type | zone |",
                    "+---------+-----------+-----------------------------+------+------+",
                    "| 1       | mackinnon | 1970-01-01T00:00:00.000001Z | shot |      |",
                    "+---------+-----------+-----------------------------+------+------+",
                ],
            },
            // Pulling from multiple caches will fill in with nulls:
            TestCase {
                predicates: &[],
                expected: &[
                    "+---------+-----------+-----------------------------+------+------+",
                    "| game_id | player    | time                        | type | zone |",
                    "+---------+-----------+-----------------------------+------+------+",
                    "| 1       | mackinnon | 1970-01-01T00:00:00.000001Z | shot |      |",
                    "| 2       | matthews  | 1970-01-01T00:00:00.000001Z | shot |      |",
                    "| 3       | tkachuk   | 1970-01-01T00:00:00.000001Z | hit  | away |",
                    "| 4       | bobrovsky | 1970-01-01T00:00:00.000001Z | save | home |",
                    "+---------+-----------+-----------------------------+------+------+",
                ],
            },
        ];

        for t in test_cases {
            let batches = wbuf
                .last_cache_provider()
                .get_cache_record_batches(db_id, tbl_id, None, t.predicates)
                .unwrap()
                .unwrap();

            assert_batches_sorted_eq!(t.expected, &batches);
        }
    }

    #[tokio::test]
    async fn new_field_ordering() {
        let db_name = "db";
        let tbl_name = "tbl";
        let wbuf = setup_write_buffer().await;

        // Do a write to setup catalog:
        wbuf.write_lp(
            NamespaceName::new(db_name).unwrap(),
            format!(r#"{tbl_name},t1=a f1=1"#).as_str(),
            Time::from_timestamp_nanos(500),
            false,
            Precision::Nanosecond,
        )
        .await
        .unwrap();

        let (db_id, db_schema) = wbuf.catalog().db_schema_and_id(db_name).unwrap();
        let (tbl_id, table_def) = db_schema.table_definition_and_id(tbl_name).unwrap();
        let t1_col_id = table_def.name_to_id("t1").unwrap();

        // Create the last cache using the single `t1` tag column as key
        // and using the default for fields, so that new fields will get added
        // to the cache.
        wbuf.create_last_cache(
            db_id,
            tbl_id,
            None,
            None, // use default cache size of 1
            None,
            Some(vec![(t1_col_id, "t1".into())]),
            None,
        )
        .await
        .expect("create last cache");

        // Write some lines to fill the cache. In this case, with just the existing
        // columns in the table, i.e., t1 and f1
        wbuf.write_lp(
            NamespaceName::new(db_name).unwrap(),
            format!(
                "\
                {tbl_name},t1=a f1=1
                {tbl_name},t1=b f1=10
                {tbl_name},t1=c f1=100
                "
            )
            .as_str(),
            Time::from_timestamp_nanos(1_000),
            false,
            Precision::Nanosecond,
        )
        .await
        .unwrap();

        // Write lines containing new fields f2 and f3, but with different orders for
        // each key column value, i.e., t1=a and t1=b:
        wbuf.write_lp(
            NamespaceName::new(db_name).unwrap(),
            format!(
                "\
                {tbl_name},t1=a f1=1,f2=2,f3=3,f4=4
                {tbl_name},t1=b f1=10,f4=40,f3=30
                {tbl_name},t1=c f1=100,f3=300,f2=200
                "
            )
            .as_str(),
            Time::from_timestamp_nanos(1_500),
            false,
            Precision::Nanosecond,
        )
        .await
        .unwrap();

        struct TestCase<'a> {
            predicates: &'a [Predicate],
            expected: &'a [&'a str],
        }

        let test_cases = [
            // Can query on specific key column values:
            TestCase {
                predicates: &[Predicate::new_eq(t1_col_id, KeyValue::string("a"))],
                expected: &[
                    "+----+-----+--------------------------------+-----+-----+-----+",
                    "| t1 | f1  | time                           | f2  | f3  | f4  |",
                    "+----+-----+--------------------------------+-----+-----+-----+",
                    "| a  | 1.0 | 1970-01-01T00:00:00.000001500Z | 2.0 | 3.0 | 4.0 |",
                    "+----+-----+--------------------------------+-----+-----+-----+",
                ],
            },
            TestCase {
                predicates: &[Predicate::new_eq(t1_col_id, KeyValue::string("b"))],
                expected: &[
                    "+----+------+--------------------------------+----+------+------+",
                    "| t1 | f1   | time                           | f2 | f3   | f4   |",
                    "+----+------+--------------------------------+----+------+------+",
                    "| b  | 10.0 | 1970-01-01T00:00:00.000001500Z |    | 30.0 | 40.0 |",
                    "+----+------+--------------------------------+----+------+------+",
                ],
            },
            TestCase {
                predicates: &[Predicate::new_eq(t1_col_id, KeyValue::string("c"))],
                expected: &[
                    "+----+-------+--------------------------------+-------+-------+----+",
                    "| t1 | f1    | time                           | f2    | f3    | f4 |",
                    "+----+-------+--------------------------------+-------+-------+----+",
                    "| c  | 100.0 | 1970-01-01T00:00:00.000001500Z | 200.0 | 300.0 |    |",
                    "+----+-------+--------------------------------+-------+-------+----+",
                ],
            },
            // Can query accross key column values:
            TestCase {
                predicates: &[],
                expected: &[
                    "+----+-------+--------------------------------+-------+-------+------+",
                    "| t1 | f1    | time                           | f2    | f3    | f4   |",
                    "+----+-------+--------------------------------+-------+-------+------+",
                    "| a  | 1.0   | 1970-01-01T00:00:00.000001500Z | 2.0   | 3.0   | 4.0  |",
                    "| b  | 10.0  | 1970-01-01T00:00:00.000001500Z |       | 30.0  | 40.0 |",
                    "| c  | 100.0 | 1970-01-01T00:00:00.000001500Z | 200.0 | 300.0 |      |",
                    "+----+-------+--------------------------------+-------+-------+------+",
                ],
            },
        ];

        for t in test_cases {
            let batches = wbuf
                .last_cache_provider()
                .get_cache_record_batches(db_id, tbl_id, None, t.predicates)
                .unwrap()
                .unwrap();

            assert_batches_sorted_eq!(t.expected, &batches);
        }
    }

    #[tokio::test]
    async fn idempotent_cache_creation() {
        let db_name = "db";
        let tbl_name = "tbl";
        let wbuf = setup_write_buffer().await;

        // Do a write to setup catalog:
        wbuf.write_lp(
            NamespaceName::new(db_name).unwrap(),
            format!(r#"{tbl_name},t1=a,t2=b f1=1,f2=2"#).as_str(),
            Time::from_timestamp_nanos(500),
            false,
            Precision::Nanosecond,
        )
        .await
        .unwrap();

        let (db_id, db_schema) = wbuf.catalog().db_schema_and_id(db_name).unwrap();
        let (tbl_id, table_def) = db_schema.table_definition_and_id(tbl_name).unwrap();
        let t1_col_id = table_def.name_to_id("t1").unwrap();
        let t2_col_id = table_def.name_to_id("t2").unwrap();
        let f1_col_id = table_def.name_to_id("f1").unwrap();
        let f2_col_id = table_def.name_to_id("f2").unwrap();

        // Create a last cache using all default settings
        wbuf.create_last_cache(db_id, tbl_id, None, None, None, None, None)
            .await
            .expect("create last cache");
        assert_eq!(wbuf.last_cache_provider().size(), 1);

        // Doing the same should be fine:
        wbuf.create_last_cache(db_id, tbl_id, None, None, None, None, None)
            .await
            .expect("create last cache");
        assert_eq!(wbuf.last_cache_provider().size(), 1);

        // Specify the same arguments as what the defaults would produce (minus the value columns)
        wbuf.create_last_cache(
            db_id,
            tbl_id,
            Some("tbl_t1_t2_last_cache"),
            Some(1),
            Some(DEFAULT_CACHE_TTL),
            Some(vec![(t1_col_id, "t1".into()), (t2_col_id, "t2".into())]),
            None,
        )
        .await
        .expect("create last cache");
        assert_eq!(wbuf.last_cache_provider().size(), 1);

        // Specify value columns, which would deviate from above, as that implies different cache
        // behaviour, i.e., no new fields are accepted:
        wbuf.create_last_cache(
            db_id,
            tbl_id,
            None,
            None,
            None,
            None,
            Some(vec![(f1_col_id, "f1".into()), (f2_col_id, "f2".into())]),
        )
        .await
        .expect_err("create last cache should have failed");
        assert_eq!(wbuf.last_cache_provider().size(), 1);

        // Specify different key columns, along with the same cache name will produce error:
        wbuf.create_last_cache(
            db_id,
            tbl_id,
            Some("tbl_t1_t2_last_cache"),
            None,
            None,
            Some(vec![(t1_col_id, "t1".into())]),
            None,
        )
        .await
        .expect_err("create last cache should have failed");
        assert_eq!(wbuf.last_cache_provider().size(), 1);

        // However, just specifying different key columns and no cache name will result in a
        // different generated cache name, and therefore cache, so it will work:
        let name = wbuf
            .create_last_cache(
                db_id,
                tbl_id,
                None,
                None,
                None,
                Some(vec![(t1_col_id, "t1".into())]),
                None,
            )
            .await
            .expect("create last cache should have failed");
        assert_eq!(wbuf.last_cache_provider().size(), 2);
        assert_eq!(
            Some("tbl_t1_last_cache"),
            name.map(|info| info.name).as_deref()
        );

        // Specify different TTL:
        wbuf.create_last_cache(
            db_id,
            tbl_id,
            None,
            None,
            Some(Duration::from_secs(10)),
            None,
            None,
        )
        .await
        .expect_err("create last cache should have failed");
        assert_eq!(wbuf.last_cache_provider().size(), 2);

        // Specify different count:
        wbuf.create_last_cache(db_id, tbl_id, None, Some(10), None, None, None)
            .await
            .expect_err("create last cache should have failed");
        assert_eq!(wbuf.last_cache_provider().size(), 2);
    }

    type SeriesKey = Option<Vec<ColumnId>>;

    #[test]
    fn catalog_initialization() {
        // Set up a database in the catalog:
        let db_name = "test_db";
        let mut database = DatabaseSchema {
            id: DbId::from(0),
            name: db_name.into(),
            tables: SerdeVecHashMap::new(),
            table_map: {
                let mut map = BiHashMap::new();
                map.insert(TableId::from(0), "test_table_1".into());
                map.insert(TableId::from(1), "test_table_2".into());
                map
            },
        };
        let table_id = TableId::from(0);
        use schema::InfluxColumnType::*;
        use schema::InfluxFieldType::*;
        // Add a table to it:
        let mut table_def = TableDefinition::new(
            table_id,
            "test_table_1".into(),
            vec![
                (ColumnId::from(0), "t1".into(), Tag),
                (ColumnId::from(1), "t2".into(), Tag),
                (ColumnId::from(2), "t3".into(), Tag),
                (ColumnId::from(3), "time".into(), Timestamp),
                (ColumnId::from(4), "f1".into(), Field(String)),
                (ColumnId::from(5), "f2".into(), Field(Float)),
            ],
            SeriesKey::None,
        )
        .unwrap();
        // Give that table a last cache:
        table_def.add_last_cache(
            LastCacheDefinition::new_all_non_key_value_columns(
                table_id,
                "test_table_1",
                "test_cache_1",
                vec![ColumnId::from(0), ColumnId::from(1)],
                1,
                600,
            )
            .unwrap(),
        );
        database
            .tables
            .insert(table_def.table_id, Arc::new(table_def));
        // Add another table to it:
        let table_id = TableId::from(1);
        let mut table_def = TableDefinition::new(
            table_id,
            "test_table_2".into(),
            vec![
                (ColumnId::from(6), "t1".into(), Tag),
                (ColumnId::from(7), "time".into(), Timestamp),
                (ColumnId::from(8), "f1".into(), Field(String)),
                (ColumnId::from(9), "f2".into(), Field(Float)),
            ],
            SeriesKey::None,
        )
        .unwrap();
        // Give that table a last cache:
        table_def.add_last_cache(
            LastCacheDefinition::new_with_explicit_value_columns(
                table_id,
                "test_table_2",
                "test_cache_2",
                vec![ColumnId::from(6)],
                vec![ColumnId::from(8)],
                5,
                60,
            )
            .unwrap(),
        );
        // Give that table another last cache:
        table_def.add_last_cache(
            LastCacheDefinition::new_with_explicit_value_columns(
                table_id,
                "test_table_2",
                "test_cache_3",
                vec![],
                vec![ColumnId::from(9)],
                10,
                500,
            )
            .unwrap(),
        );
        database
            .tables
            .insert(table_def.table_id, Arc::new(table_def));
        // Create the catalog and clone its InnerCatalog (which is what the LastCacheProvider is
        // initialized from):
        let host_id = Arc::from("sample-host-id");
        let instance_id = Arc::from("sample-instance-id");
        let catalog = Catalog::new(host_id, instance_id);
        let db_id = database.id;
        catalog.insert_database(database);
        let catalog = Arc::new(catalog);
        // This is the function we are testing, which initializes the LastCacheProvider from the catalog:
        let provider = LastCacheProvider::new_from_catalog(Arc::clone(&catalog) as _)
            .expect("create last cache provider from catalog");
        // There should be a total of 3 caches:
        assert_eq!(3, provider.size());
        // Get the cache definitions and snapshot them to check their content. They are sorted to
        // ensure order, since the provider uses hashmaps and their order may not be guaranteed.
        let mut caches = provider.get_last_caches_for_db(db_id);
        caches.sort_by(|a, b| match a.table.partial_cmp(&b.table).unwrap() {
            ord @ Ordering::Less | ord @ Ordering::Greater => ord,
            Ordering::Equal => a.name.partial_cmp(&b.name).unwrap(),
        });
        assert_json_snapshot!(caches);
    }
}
