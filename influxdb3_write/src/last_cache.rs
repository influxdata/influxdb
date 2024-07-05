use std::{
    any::Any,
    collections::VecDeque,
    sync::Arc,
    time::{Duration, Instant},
};

use arrow::{
    array::{
        new_null_array, ArrayRef, BooleanBuilder, Float64Builder, GenericByteDictionaryBuilder,
        Int64Builder, RecordBatch, StringBuilder, StringDictionaryBuilder,
        TimestampNanosecondBuilder, UInt64Builder,
    },
    datatypes::{
        DataType, Field as ArrowField, FieldRef, GenericStringType, Int32Type,
        SchemaBuilder as ArrowSchemaBuilder, SchemaRef as ArrowSchemaRef, TimeUnit,
    },
    error::ArrowError,
};
use async_trait::async_trait;
use datafusion::{
    common::Result as DFResult,
    datasource::{TableProvider, TableType},
    execution::context::SessionState,
    logical_expr::{BinaryExpr, Expr, Operator, TableProviderFilterPushDown},
    physical_plan::{memory::MemoryExec, ExecutionPlan},
    scalar::ScalarValue,
};
use hashbrown::{HashMap, HashSet};
use indexmap::{IndexMap, IndexSet};
use iox_time::Time;
use parking_lot::RwLock;
use schema::{InfluxColumnType, InfluxFieldType, Schema, SchemaBuilder, TIME_COLUMN_NAME};

use crate::{
    catalog::LastCacheSize,
    write_buffer::{buffer_segment::WriteBatch, Field, FieldData, Row},
};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("invalid cache size")]
    InvalidCacheSize,
    #[error("last cache already exists for database and table")]
    CacheAlreadyExists,
    #[error("specified key column ({column_name}) does not exist in the table schema")]
    KeyColumnDoesNotExist { column_name: String },
    #[error("key column must be string, int, uint, or bool types")]
    InvalidKeyColumn,
    #[error("specified value column ({column_name}) does not exist in the table schema")]
    ValueColumnDoesNotExist { column_name: String },
    #[error("schema builder error: {0}")]
    SchemaBuilder(#[from] schema::builder::Error),
}

/// A three level hashmap storing Database Name -> Table Name -> Cache Name -> LastCache
type CacheMap = RwLock<HashMap<String, HashMap<String, HashMap<String, LastCache>>>>;

/// Provides all last-N-value caches for the entire database
pub struct LastCacheProvider {
    cache_map: CacheMap,
}

impl std::fmt::Debug for LastCacheProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "LastCacheProvider")
    }
}

/// The default cache time-to-live (TTL) is 4 hours
const DEFAULT_CACHE_TTL: Duration = Duration::from_secs(60 * 60 * 4);

/// Arguments to the [`LastCacheProvider::create_cache`] method
pub(crate) struct CreateCacheArguments {
    /// The name of the database to create the cache for
    pub(crate) db_name: String,
    /// The name of the table in the database to create the cache for
    pub(crate) tbl_name: String,
    /// The Influx Schema of the table
    pub(crate) schema: Schema,
    /// An optional name for the cache
    ///
    /// The cache name will default to `<table_name>_<keys>_last_cache`
    pub(crate) cache_name: Option<String>,
    /// The number of values to hold in the created cache
    ///
    /// This will default to 1.
    pub(crate) count: Option<usize>,
    /// The time-to-live (TTL) for the created cache
    ///
    /// This will default to [`DEFAULT_CACHE_TTL`]
    pub(crate) ttl: Option<Duration>,
    /// The key column names to use in the cache hierarchy
    ///
    /// This will default to:
    /// - the series key columns for a v3 table
    /// - the lexicographically ordered tag set for a v1 table
    pub(crate) key_columns: Option<Vec<String>>,
    /// The value columns to use in the cache
    ///
    /// This will default to all non-key columns. The `time` column is always included.
    pub(crate) value_columns: Option<Vec<String>>,
}

impl LastCacheProvider {
    /// Create a new [`LastCacheProvider`]
    pub(crate) fn new() -> Self {
        Self {
            cache_map: Default::default(),
        }
    }

    /// Create a new entry in the last cache for a given database and table, along with the given
    /// parameters.
    pub(crate) fn create_cache(
        &self,
        CreateCacheArguments {
            db_name,
            tbl_name,
            schema,
            cache_name,
            count,
            ttl,
            key_columns,
            value_columns,
        }: CreateCacheArguments,
    ) -> Result<(), Error> {
        let key_columns = if let Some(keys) = key_columns {
            // validate the key columns specified to ensure correct type (string, int, unit, or bool)
            // and that they exist in the table's schema.
            for key in keys.iter() {
                use InfluxColumnType::*;
                use InfluxFieldType::*;
                match schema.field_by_name(key) {
                    Some((
                        Tag | Field(Integer) | Field(UInteger) | Field(String) | Field(Boolean),
                        _,
                    )) => (),
                    Some((_, _)) => return Err(Error::InvalidKeyColumn),
                    None => {
                        return Err(Error::KeyColumnDoesNotExist {
                            column_name: key.into(),
                        })
                    }
                }
            }
            keys
        } else {
            // use primary key, which defaults to series key if present, then lexicographically
            // ordered tags otherwise, there is no user-defined sort order in the schema, so if that
            // is introduced, we will need to make sure that is accommodated here.
            let mut keys = schema.primary_key();
            if let Some(&TIME_COLUMN_NAME) = keys.last() {
                keys.pop();
            }
            keys.iter().map(|s| s.to_string()).collect()
        };

        // Generate the cache name if it was not provided
        let cache_name = cache_name.unwrap_or_else(|| {
            format!("{tbl_name}_{keys}_last_cache", keys = key_columns.join("_"))
        });

        // reject creation if there is already a cache with specified database, table, and cache name
        if self
            .cache_map
            .read()
            .get(&db_name)
            .and_then(|db| db.get(&tbl_name))
            .is_some_and(|tbl| tbl.contains_key(&cache_name))
        {
            return Err(Error::CacheAlreadyExists);
        }

        let (value_columns, accept_new) = if let Some(mut vals) = value_columns {
            // if value columns are specified, check that they are present in the table schema
            for name in vals.iter() {
                if schema.field_by_name(name).is_none() {
                    return Err(Error::ValueColumnDoesNotExist {
                        column_name: name.into(),
                    });
                }
            }
            // double-check that time column is included
            let time_col = TIME_COLUMN_NAME.to_string();
            if !vals.contains(&time_col) {
                vals.push(time_col);
            }
            (vals, false)
        } else {
            // default to all non-key columns
            (
                schema
                    .iter()
                    .filter_map(|(_, f)| {
                        if key_columns.contains(f.name()) {
                            None
                        } else {
                            Some(f.name().to_string())
                        }
                    })
                    .collect::<Vec<String>>(),
                true,
            )
        };

        // build a schema that only holds the field columns
        let mut schema_builder = SchemaBuilder::new();
        for (t, name) in schema
            .iter()
            .filter(|&(_, f)| value_columns.contains(f.name()))
            .map(|(t, f)| (t, f.name()))
        {
            schema_builder.influx_column(name, t);
        }

        let series_key = schema
            .series_key()
            .map(|keys| keys.into_iter().map(|s| s.to_string()).collect());

        // create the actual last cache:
        let last_cache = LastCache::new(
            count
                .unwrap_or(1)
                .try_into()
                .map_err(|_| Error::InvalidCacheSize)?,
            ttl.unwrap_or(DEFAULT_CACHE_TTL),
            key_columns,
            schema_builder.build()?.as_arrow(),
            series_key,
            accept_new,
        );

        // get the write lock and insert:
        self.cache_map
            .write()
            .entry(db_name)
            .or_default()
            .entry(tbl_name)
            .or_default()
            .insert(cache_name, last_cache);

        Ok(())
    }

    /// Write a batch from the buffer into the cache by iterating over its database and table batches
    /// to find entries that belong in the cache.
    ///
    /// Only if rows are newer than the latest entry in the cache will they be entered.
    pub(crate) fn write_batch_to_cache(&self, write_batch: &WriteBatch) {
        let mut cache_map = self.cache_map.write();
        for (db_name, db_batch) in &write_batch.database_batches {
            if let Some(db_cache) = cache_map.get_mut(db_name.as_str()) {
                if db_cache.is_empty() {
                    continue;
                }
                for (tbl_name, tbl_batch) in &db_batch.table_batches {
                    if let Some(tbl_cache) = db_cache.get_mut(tbl_name) {
                        for (_, last_cache) in tbl_cache.iter_mut() {
                            for row in &tbl_batch.rows {
                                last_cache.push(row);
                            }
                        }
                    }
                }
            }
        }
    }

    /// Recurse down the cache structure to evict expired cache entries, based on their respective
    /// time-to-live (TTL).
    pub(crate) fn evict_expired_cache_entries(&self) {
        let mut cache_map = self.cache_map.write();
        cache_map.iter_mut().for_each(|(_, db)| {
            db.iter_mut()
                .for_each(|(_, tbl)| tbl.iter_mut().for_each(|(_, lc)| lc.remove_expired()))
        });
    }

    /// Output the records for a given cache as arrow [`RecordBatch`]es
    #[cfg(test)]
    fn get_cache_record_batches(
        &self,
        db_name: &str,
        tbl_name: &str,
        cache_name: Option<&str>,
        predicates: &[Predicate],
    ) -> Option<Result<Vec<RecordBatch>, ArrowError>> {
        self.cache_map
            .read()
            .get(db_name)
            .and_then(|db| db.get(tbl_name))
            .and_then(|tbl| {
                if let Some(name) = cache_name {
                    tbl.get(name)
                } else if tbl.len() == 1 {
                    tbl.iter().next().map(|(_, lc)| lc)
                } else {
                    None
                }
            })
            .map(|lc| lc.to_record_batches(predicates))
    }
}

/// A Last-N-Values Cache
///
/// A hierarchical cache whose structure is determined by a set of `key_columns`, each of which
/// represents a level in the hierarchy. The lowest level of the hierarchy holds the last N values
/// for the field columns in the cache.
pub(crate) struct LastCache {
    /// The number of values to hold in the cache
    ///
    /// Once the cache reaches this size, old values will be evicted when new values are pushed in.
    count: LastCacheSize,
    /// The time-to-live (TTL) for values in the cache
    ///
    /// Once values have lived in the cache beyond this [`Duration`], they can be evicted using
    /// the [`remove_expired`][LastCache::remove_expired] method.
    ttl: Duration,
    /// The key columns for this cache
    key_columns: IndexSet<String>,
    /// The Arrow Schema for the table that this cache is associated with
    schema: ArrowSchemaRef,
    /// Optionally store the series key for tables that use it for ensuring non-nullability in the
    /// column buffer for series key columns
    series_key: Option<HashSet<String>>,
    /// Whether or not this cache accepts newly written fields
    accept_new: bool,
    /// The internal state of the cache
    state: LastCacheState,
}

impl LastCache {
    /// Create a new [`LastCache`]
    fn new(
        count: LastCacheSize,
        ttl: Duration,
        key_columns: Vec<String>,
        schema: ArrowSchemaRef,
        series_key: Option<HashSet<String>>,
        accept_new: bool,
    ) -> Self {
        Self {
            count,
            ttl,
            key_columns: key_columns.into_iter().collect(),
            schema,
            series_key,
            accept_new,
            state: LastCacheState::Init,
        }
    }

    /// Push a [`Row`] from the write buffer into the cache
    ///
    /// If a key column is not present in the row, the row will be ignored.
    ///
    /// # Panics
    ///
    /// This will panic if the internal cache state's keys are out-of-order with respect to the
    /// order of the `key_columns` on this [`LastCache`]
    pub(crate) fn push(&mut self, row: &Row) {
        let mut target = &mut self.state;
        let mut key_iter = self.key_columns.iter().peekable();
        while let (Some(key), peek) = (key_iter.next(), key_iter.peek()) {
            if target.is_init() {
                *target = LastCacheState::Key(LastCacheKey {
                    column_name: key.to_string(),
                    value_map: Default::default(),
                });
            }
            let Some(value) = row
                .fields
                .iter()
                .find(|f| f.name == *key)
                .map(|f| KeyValue::from(&f.value))
            else {
                // ignore the row if it does not contain all key columns
                return;
            };
            let cache_key = target.as_key_mut().unwrap();
            assert_eq!(
                &cache_key.column_name, key,
                "key columns must match cache key order"
            );
            target = cache_key.value_map.entry(value).or_insert_with(|| {
                if let Some(next_key) = peek {
                    LastCacheState::Key(LastCacheKey {
                        column_name: next_key.to_string(),
                        value_map: Default::default(),
                    })
                } else {
                    LastCacheState::Store(LastCacheStore::new(
                        self.count.into(),
                        self.ttl,
                        Arc::clone(&self.schema),
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
                Arc::clone(&self.schema),
                self.series_key.as_ref(),
            ));
        }
        let store = target.as_store_mut().expect(
            "cache target should be the actual store after iterating through all key columns",
        );
        let Some(new_columns) = store.push(row, self.accept_new, &self.key_columns) else {
            // Unless new columns were added, and we need to update the schema, we are done.
            return;
        };

        let mut sb = ArrowSchemaBuilder::new();
        for f in self.schema.fields().iter() {
            sb.push(Arc::clone(&f));
        }
        for (name, data_type) in new_columns {
            sb.push(Arc::new(ArrowField::new(name, data_type, true)));
        }
        let new_schema = Arc::new(sb.finish());
        self.schema = new_schema;
        store.update_schema(Arc::clone(&self.schema));
    }

    /// Produce a set of [`RecordBatch`]es from the cache, using the given set of [`Predicate`]s
    fn to_record_batches(&self, predicates: &[Predicate]) -> Result<Vec<RecordBatch>, ArrowError> {
        // map the provided predicates on to the key columns
        // there may not be predicates provided for each key column, hence the Option
        let predicates: Vec<Option<Predicate>> = self
            .key_columns
            .iter()
            .map(|key| predicates.iter().find(|p| p.key == *key).cloned())
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
            'cache_loop: for c in caches {
                let cache_key = c.state.as_key().unwrap();
                if let Some(ref pred) = predicate {
                    let Some(next_state) = cache_key.evaluate_predicate(pred) else {
                        continue 'cache_loop;
                    };
                    new_caches.push(ExtendedLastCacheState {
                        state: next_state,
                        additional_columns: c.additional_columns.clone(),
                    });
                } else {
                    new_caches.extend(cache_key.value_map.iter().map(|(v, state)| {
                        let mut additional_columns = c.additional_columns.clone();
                        additional_columns.push((&cache_key.column_name, v));
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
                if let Expr::BinaryExpr(BinaryExpr { left, op, right }) = expr {
                    if *op == Operator::Eq {
                        if let Expr::Column(c) = left.as_ref() {
                            let key = c.name.to_string();
                            if !self.key_columns.contains(&key) {
                                return None;
                            }
                            return match right.as_ref() {
                                Expr::Literal(ScalarValue::Utf8(Some(v))) => Some(Predicate {
                                    key,
                                    value: KeyValue::String(v.to_owned()),
                                }),
                                Expr::Literal(ScalarValue::Boolean(Some(v))) => Some(Predicate {
                                    key,
                                    value: KeyValue::Bool(*v),
                                }),
                                // TODO: handle integer types that can be casted up to i64/u64:
                                Expr::Literal(ScalarValue::Int64(Some(v))) => Some(Predicate {
                                    key,
                                    value: KeyValue::Int(*v),
                                }),
                                Expr::Literal(ScalarValue::UInt64(Some(v))) => Some(Predicate {
                                    key,
                                    value: KeyValue::UInt(*v),
                                }),
                                _ => None,
                            };
                        }
                    }
                }
                None
            })
            .collect()
    }

    /// Remove expired values from the internal cache state
    fn remove_expired(&mut self) {
        self.state.remove_expired();
    }
}

/// Extend a [`LastCacheState`] with additional columns
///
/// This is used for scenarios where key column values need to be produced in query outputs. Since
/// They are not stored in the terminal [`LastCacheStore`], we pass them down using this structure.
#[derive(Debug)]
struct ExtendedLastCacheState<'a> {
    state: &'a LastCacheState,
    additional_columns: Vec<(&'a String, &'a KeyValue)>,
}

impl<'a> ExtendedLastCacheState<'a> {
    /// Produce a set of [`RecordBatch`]es from this extended state
    ///
    /// This converts any additional columns to arrow arrays which will extend the [`RecordBatch`]es
    /// produced by the inner [`LastCacheStore`]
    ///
    /// # Panics
    ///
    /// This assumes taht the `state` is a [`LastCacheStore`] and will panic otherwise.
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
                    .map(|(name, value)| {
                        let field = Arc::new(value.as_arrow_field(*name));
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
struct Predicate {
    /// The left-hand-side of the predicate
    key: String,
    /// The right-hand-side of the predicate
    value: KeyValue,
}

#[cfg(test)]
impl Predicate {
    fn new(key: impl Into<String>, value: KeyValue) -> Self {
        Self {
            key: key.into(),
            value,
        }
    }
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
    fn remove_expired(&mut self) {
        match self {
            LastCacheState::Key(k) => k.remove_expired(),
            LastCacheState::Store(s) => s.remove_expired(),
            LastCacheState::Init => (),
        }
    }
}

/// Holds a node within a [`LastCache`] for a given key column
#[derive(Debug)]
struct LastCacheKey {
    /// The name of the key column
    column_name: String,
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
    fn evaluate_predicate(&self, predicate: &Predicate) -> Option<&LastCacheState> {
        if predicate.key != self.column_name {
            panic!(
                "attempted to evaluate unexpected predicate with key {} for column named {}",
                predicate.key, self.column_name
            );
        }
        self.value_map.get(&predicate.value)
    }

    /// Remove expired values from any cache nested within this [`LastCacheKey`]
    fn remove_expired(&mut self) {
        self.value_map
            .iter_mut()
            .for_each(|(_, m)| m.remove_expired());
    }
}

/// A value for a key column in a [`LastCache`]
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
enum KeyValue {
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
    /// Holds a copy of the arrow schema for the sake of producing [`RecordBatch`]es.
    schema: ArrowSchemaRef,
    /// A map of column name to a [`CacheColumn`] which holds the buffer of data for the column
    /// in the cache.
    ///
    /// This uses an IndexMap to preserve insertion order, so that when producing record batches
    /// the order of columns are accessed in the same order that they were inserted. Since they are
    /// inserted in the same order that they appear in the arrow schema associated with the cache,
    /// this allows record batch creation with a straight iteration through the map, vs. having
    /// to look up columns on the fly to get the correct order.
    cache: IndexMap<String, CacheColumn>,
    /// The capacity of the internal cache buffers
    count: usize,
    // TODO: will need to use this if new columns are added for * caches
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
        schema: ArrowSchemaRef,
        series_keys: Option<&HashSet<String>>,
    ) -> Self {
        let cache = schema
            .fields()
            .iter()
            .map(|f| {
                (
                    f.name().to_string(),
                    CacheColumn::new(
                        f.data_type(),
                        count,
                        ttl,
                        series_keys.is_some_and(|sk| sk.contains(f.name().as_str())),
                    ),
                )
            })
            .collect();
        Self {
            schema,
            cache,
            count,
            ttl,
            last_time: Time::from_timestamp_nanos(0),
        }
    }

    /// Get the number of values in the cache.
    ///
    /// Assumes that all columns are the same length, and only checks the first.
    fn len(&self) -> usize {
        self.cache.first().map_or(0, |(_, c)| c.len())
    }

    /// Check if the cache is empty
    ///
    /// Assumes that all columns are the same length, and only checks the first.
    fn is_empty(&self) -> bool {
        self.cache.first().map_or(false, |(_, c)| c.is_empty())
    }

    /// Push a [`Row`] from the buffer into this cache
    fn push<'a>(
        &mut self,
        row: &'a Row,
        accept_new: bool,
        key_columns: &IndexSet<String>,
    ) -> Option<Vec<(&'a str, DataType)>> {
        if row.time <= self.last_time.timestamp_nanos() {
            return None;
        }
        let mut result = None;
        if accept_new {
            // check the length before any rows are added to ensure that the correct amount
            // of nulls are back-filled when new fields/columns are added:
            let n = self.len();
            let mut seen: HashSet<&str> = HashSet::new();
            for field in row.fields.iter() {
                seen.insert(field.name.as_str());
                if let Some(col) = self.cache.get_mut(&field.name) {
                    col.push(&field.value);
                } else if !key_columns.contains(&field.name) {
                    let data_type = data_type_from_buffer_field(&field);
                    let col = self.cache.entry(field.name.to_string()).or_insert_with(|| {
                        CacheColumn::new(&data_type, self.count, self.ttl, false)
                    });
                    for _ in 0..n {
                        col.push_null();
                    }
                    col.push(&field.value);
                    result
                        .get_or_insert_with(|| vec![])
                        .push((field.name.as_str(), data_type));
                }
            }
            for (name, column) in self.cache.iter_mut() {
                if !seen.contains(name.as_str()) {
                    column.push_null();
                }
            }
        } else {
            for (name, column) in self.cache.iter_mut() {
                if let Some(field) = row.fields.iter().find(|f| f.name == *name) {
                    column.push(&field.value);
                } else {
                    column.push_null();
                }
            }
        }
        self.last_time = Time::from_timestamp_nanos(row.time);
        result
    }

    /// Update the schema on this [`LastCacheStore`]
    fn update_schema(&mut self, schema: ArrowSchemaRef) {
        self.schema = schema;
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
        // This is where using IndexMap is important, because we inserted the cache entries
        // using the schema field ordering, we will get the correct order by iterating directly
        // over the map here:
        let (fields, mut arrays): (Vec<FieldRef>, Vec<ArrayRef>) =
            if output_schema.fields().len() > self.cache.len() {
                (
                    self.schema
                        .fields()
                        .iter()
                        .chain(output_schema.fields().iter().skip(self.cache.len()))
                        .cloned()
                        .collect(),
                    self.cache
                        .iter()
                        .map(|(_, c)| c.data.as_array())
                        .chain(
                            output_schema
                                .fields()
                                .iter()
                                .skip(self.cache.len())
                                .map(|f| new_null_array(f.data_type(), self.len())),
                        )
                        .collect(),
                )
            } else {
                (
                    self.schema.fields().iter().cloned().collect(),
                    self.cache.iter().map(|(_, c)| c.data.as_array()).collect(),
                )
            };
        let mut sb = ArrowSchemaBuilder::new();
        if let Some((ext_fields, mut ext_arrays)) = extended {
            sb.extend(ext_fields);
            ext_arrays.extend(arrays.drain(..));
            arrays = ext_arrays;
        }
        sb.extend(fields);
        RecordBatch::try_new(Arc::new(sb.finish()), arrays)
    }

    /// Remove expired values from the [`LastCacheStore`]
    fn remove_expired(&mut self) {
        self.cache.iter_mut().for_each(|(_, c)| c.remove_expired());
        // reset the last_time if TTL evicts everything from the cache
        if self.is_empty() {
            self.last_time = Time::from_timestamp_nanos(0);
        }
    }
}

#[async_trait]
impl TableProvider for LastCache {
    fn as_any(&self) -> &dyn Any {
        self as &dyn Any
    }

    fn schema(&self) -> ArrowSchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Temporary
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DFResult<Vec<TableProviderFilterPushDown>> {
        Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
    }

    async fn scan(
        &self,
        ctx: &SessionState,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        _limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let predicates = self.convert_filter_exprs(filters);
        let partitions = vec![self.to_record_batches(&predicates)?];
        let mut exec = MemoryExec::try_new(&partitions, self.schema(), projection.cloned())?;

        let show_sizes = ctx.config_options().explain.show_sizes;
        exec = exec.with_show_sizes(show_sizes);

        Ok(Arc::new(exec))
    }
}

/// A column in a [`LastCache`]
///
/// Stores its size so it can evict old data on push. Stores the time-to-live (TTL) in order
/// to remove expired data.
#[derive(Debug)]
struct CacheColumn {
    size: usize,
    ttl: Duration,
    data: CacheColumnData,
}

impl CacheColumn {
    /// Create a new [`CacheColumn`] for the given arrow [`DataType`] and size
    fn new(data_type: &DataType, size: usize, ttl: Duration, is_series_key: bool) -> Self {
        Self {
            size,
            ttl,
            data: CacheColumnData::new(data_type, size, is_series_key),
        }
    }

    /// Get the length of the [`CacheColumn`]
    fn len(&self) -> usize {
        self.data.len()
    }

    /// Check if the column is empty
    fn is_empty(&self) -> bool {
        self.data.is_empty()
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

    /// Remove expired values from this [`CacheColumn`]
    fn remove_expired(&mut self) {
        self.data.remove_expired(self.ttl);
    }
}

/// Enumerated type for storing column data for the cache in a buffer
#[derive(Debug)]
enum CacheColumnData {
    I64(ColumnBuffer<Option<i64>>),
    U64(ColumnBuffer<Option<u64>>),
    F64(ColumnBuffer<Option<f64>>),
    String(ColumnBuffer<Option<String>>),
    Bool(ColumnBuffer<Option<bool>>),
    Tag(ColumnBuffer<Option<String>>),
    Key(ColumnBuffer<String>),
    Time(ColumnBuffer<i64>),
}

impl CacheColumnData {
    /// Create a new [`CacheColumnData`]
    fn new(data_type: &DataType, size: usize, is_series_key: bool) -> Self {
        match data_type {
            DataType::Boolean => Self::Bool(ColumnBuffer::with_capacity(size)),
            DataType::Int64 => Self::I64(ColumnBuffer::with_capacity(size)),
            DataType::UInt64 => Self::U64(ColumnBuffer::with_capacity(size)),
            DataType::Float64 => Self::F64(ColumnBuffer::with_capacity(size)),
            DataType::Timestamp(TimeUnit::Nanosecond, _) => {
                Self::Time(ColumnBuffer::with_capacity(size))
            }
            DataType::Utf8 => Self::String(ColumnBuffer::with_capacity(size)),
            DataType::Dictionary(k, v) if **k == DataType::Int32 && **v == DataType::Utf8 => {
                if is_series_key {
                    Self::Key(ColumnBuffer::with_capacity(size))
                } else {
                    Self::Tag(ColumnBuffer::with_capacity(size))
                }
            }
            _ => panic!("unsupported data type for last cache: {data_type}"),
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

    /// Check if the column is empty
    fn is_empty(&self) -> bool {
        match self {
            CacheColumnData::I64(buf) => buf.is_empty(),
            CacheColumnData::U64(buf) => buf.is_empty(),
            CacheColumnData::F64(buf) => buf.is_empty(),
            CacheColumnData::String(buf) => buf.is_empty(),
            CacheColumnData::Bool(buf) => buf.is_empty(),
            CacheColumnData::Tag(buf) => buf.is_empty(),
            CacheColumnData::Key(buf) => buf.is_empty(),
            CacheColumnData::Time(buf) => buf.is_empty(),
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

    /// Remove expired values from the inner [`ColumnBuffer`]
    fn remove_expired(&mut self, ttl: Duration) {
        match self {
            CacheColumnData::I64(buf) => buf.remove_expired(ttl),
            CacheColumnData::U64(buf) => buf.remove_expired(ttl),
            CacheColumnData::F64(buf) => buf.remove_expired(ttl),
            CacheColumnData::String(buf) => buf.remove_expired(ttl),
            CacheColumnData::Bool(buf) => buf.remove_expired(ttl),
            CacheColumnData::Tag(buf) => buf.remove_expired(ttl),
            CacheColumnData::Key(buf) => buf.remove_expired(ttl),
            CacheColumnData::Time(buf) => buf.remove_expired(ttl),
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
}

/// Newtype wrapper around a [`VecDeque`] whose values are paired with an [`Instant`] that
/// represents the time at which each value was pushed into the buffer.
#[derive(Debug)]
struct ColumnBuffer<T>(VecDeque<(Instant, T)>);

impl<T> ColumnBuffer<T> {
    /// Create a new [`ColumnBuffer`] with the given capacity
    fn with_capacity(capacity: usize) -> Self {
        Self(VecDeque::with_capacity(capacity))
    }

    /// Get the length of the [`ColumnBuffer`]
    fn len(&self) -> usize {
        self.0.len()
    }

    /// Check if the buffer is empty
    fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Pop the last element off of the [`ColumnBuffer`]
    fn pop_back(&mut self) {
        self.0.pop_back();
    }

    /// Push a value to the front of the [`ColumnBuffer`], the value will be paired with an
    /// [`Instant`].
    fn push_front(&mut self, value: T) {
        self.0.push_front((Instant::now(), value));
    }

    /// Remove any elements in the [`ColumnBuffer`] that have outlived a given time-to-live (TTL)
    fn remove_expired(&mut self, ttl: Duration) {
        while let Some((created, _)) = self.0.back() {
            if created.elapsed() >= ttl {
                self.0.pop_back();
            } else {
                return;
            }
        }
    }

    /// Produce an iterator over the values in the buffer
    fn iter(&self) -> impl Iterator<Item = &T> {
        self.0.iter().map(|(_, v)| v)
    }
}

fn data_type_from_buffer_field(field: &Field) -> DataType {
    match field.value {
        FieldData::Timestamp(_) => DataType::Timestamp(TimeUnit::Nanosecond, None),
        FieldData::Key(_) => {
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8))
        }
        FieldData::Tag(_) => {
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8))
        }
        FieldData::String(_) => DataType::Utf8,
        FieldData::Integer(_) => DataType::Int64,
        FieldData::UInteger(_) => DataType::UInt64,
        FieldData::Float(_) => DataType::Float64,
        FieldData::Boolean(_) => DataType::Boolean,
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use ::object_store::{memory::InMemory, ObjectStore};
    use arrow_util::{assert_batches_eq, assert_batches_sorted_eq};
    use data_types::NamespaceName;
    use iox_time::{MockProvider, Time};

    use crate::{
        last_cache::{KeyValue, Predicate},
        persister::PersisterImpl,
        wal::WalImpl,
        write_buffer::WriteBufferImpl,
        Bufferer, Precision, SegmentDuration,
    };

    async fn setup_write_buffer() -> WriteBufferImpl<WalImpl, MockProvider> {
        let obj_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let persister = Arc::new(PersisterImpl::new(obj_store));
        let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
        WriteBufferImpl::new(
            persister,
            Option::<Arc<WalImpl>>::None,
            time_provider,
            SegmentDuration::new_5m(),
            crate::test_help::make_exec(),
            1000,
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

        // Create the last cache:
        wbuf.create_last_cache(
            db_name,
            tbl_name,
            Some("cache"),
            None,
            None,
            Some(vec!["host".to_string()]),
            None,
        )
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

        let predicates = &[Predicate::new("host", KeyValue::string("a"))];

        // Check what is in the last cache:
        let batch = wbuf
            .last_cache()
            .get_cache_record_batches(db_name, tbl_name, None, predicates)
            .unwrap()
            .unwrap();

        assert_batches_eq!(
            [
                "+--------+-----------------------------+-------+",
                "| region | time                        | usage |",
                "+--------+-----------------------------+-------+",
                "| us     | 1970-01-01T00:00:00.000002Z | 99.0  |",
                "+--------+-----------------------------+-------+",
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
            .last_cache()
            .get_cache_record_batches(db_name, tbl_name, None, predicates)
            .unwrap()
            .unwrap();

        assert_batches_eq!(
            [
                "+--------+-----------------------------+-------+",
                "| region | time                        | usage |",
                "+--------+-----------------------------+-------+",
                "| us     | 1970-01-01T00:00:00.000003Z | 88.0  |",
                "+--------+-----------------------------+-------+",
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

        // Create the last cache with keys on all tag columns:
        wbuf.create_last_cache(
            db_name,
            tbl_name,
            Some("cache"),
            None,
            None,
            Some(vec!["region".to_string(), "host".to_string()]),
            None,
        )
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
                    Predicate::new("region", KeyValue::string("us")),
                    Predicate::new("host", KeyValue::string("c")),
                ],
                expected: &[
                    "+-----------------------------+-------+",
                    "| time                        | usage |",
                    "+-----------------------------+-------+",
                    "| 1970-01-01T00:00:00.000001Z | 60.0  |",
                    "+-----------------------------+-------+",
                ],
            },
            // Predicate on only region key column will have host column outputted in addition to
            // the value columns:
            TestCase {
                predicates: &[Predicate::new("region", KeyValue::string("us"))],
                expected: &[
                    "+------+-----------------------------+-------+",
                    "| host | time                        | usage |",
                    "+------+-----------------------------+-------+",
                    "| a    | 1970-01-01T00:00:00.000001Z | 100.0 |",
                    "| c    | 1970-01-01T00:00:00.000001Z | 60.0  |",
                    "| b    | 1970-01-01T00:00:00.000001Z | 80.0  |",
                    "+------+-----------------------------+-------+",
                ],
            },
            // Similar to previous, with a different region predicate:
            TestCase {
                predicates: &[Predicate::new("region", KeyValue::string("ca"))],
                expected: &[
                    "+------+-----------------------------+-------+",
                    "| host | time                        | usage |",
                    "+------+-----------------------------+-------+",
                    "| d    | 1970-01-01T00:00:00.000001Z | 40.0  |",
                    "| e    | 1970-01-01T00:00:00.000001Z | 20.0  |",
                    "| f    | 1970-01-01T00:00:00.000001Z | 30.0  |",
                    "+------+-----------------------------+-------+",
                ],
            },
            // Predicate on only host key column will have region column outputted in addition to
            // the value columns:
            TestCase {
                predicates: &[Predicate::new("host", KeyValue::string("a"))],
                expected: &[
                    "+--------+-----------------------------+-------+",
                    "| region | time                        | usage |",
                    "+--------+-----------------------------+-------+",
                    "| us     | 1970-01-01T00:00:00.000001Z | 100.0 |",
                    "+--------+-----------------------------+-------+",
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
                predicates: &[Predicate::new("container_id", KeyValue::string("12345"))],
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
                predicates: &[Predicate::new("region", KeyValue::string("eu"))],
                expected: &["++", "++"],
            },
            // Using an invalid combination of key column values yields an empty result set:
            TestCase {
                predicates: &[
                    Predicate::new("region", KeyValue::string("ca")),
                    Predicate::new("host", KeyValue::string("a")),
                ],
                expected: &["++", "++"],
            },
            // Using a non-existent key column value (for host column) also yields empty result set:
            TestCase {
                predicates: &[Predicate::new("host", KeyValue::string("g"))],
                expected: &["++", "++"],
            },
            // Using an incorrect type for a key column value in predicate also yields empty result
            // set. TODO: should this be an error?
            TestCase {
                predicates: &[Predicate::new("host", KeyValue::Bool(true))],
                expected: &["++", "++"],
            },
        ];

        for t in test_cases {
            let batches = wbuf
                .last_cache()
                .get_cache_record_batches(db_name, tbl_name, None, t.predicates)
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

        // Create the last cache with keys on all tag columns:
        wbuf.create_last_cache(
            db_name,
            tbl_name,
            Some("cache"),
            Some(10),
            None,
            Some(vec!["region".to_string(), "host".to_string()]),
            None,
        )
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
                    Predicate::new("region", KeyValue::string("us")),
                    Predicate::new("host", KeyValue::string("a")),
                ],
                expected: &[
                    "+--------------------------------+-------+",
                    "| time                           | usage |",
                    "+--------------------------------+-------+",
                    "| 1970-01-01T00:00:00.000001500Z | 99.0  |",
                    "| 1970-01-01T00:00:00.000001Z    | 100.0 |",
                    "| 1970-01-01T00:00:00.000002500Z | 90.0  |",
                    "| 1970-01-01T00:00:00.000002Z    | 95.0  |",
                    "+--------------------------------+-------+",
                ],
            },
            TestCase {
                predicates: &[Predicate::new("region", KeyValue::string("us"))],
                expected: &[
                    "+------+--------------------------------+-------+",
                    "| host | time                           | usage |",
                    "+------+--------------------------------+-------+",
                    "| a    | 1970-01-01T00:00:00.000001500Z | 99.0  |",
                    "| a    | 1970-01-01T00:00:00.000001Z    | 100.0 |",
                    "| a    | 1970-01-01T00:00:00.000002500Z | 90.0  |",
                    "| a    | 1970-01-01T00:00:00.000002Z    | 95.0  |",
                    "| b    | 1970-01-01T00:00:00.000001500Z | 88.0  |",
                    "| b    | 1970-01-01T00:00:00.000001Z    | 80.0  |",
                    "| b    | 1970-01-01T00:00:00.000002500Z | 99.0  |",
                    "| b    | 1970-01-01T00:00:00.000002Z    | 92.0  |",
                    "+------+--------------------------------+-------+",
                ],
            },
            TestCase {
                predicates: &[Predicate::new("host", KeyValue::string("a"))],
                expected: &[
                    "+--------+--------------------------------+-------+",
                    "| region | time                           | usage |",
                    "+--------+--------------------------------+-------+",
                    "| us     | 1970-01-01T00:00:00.000001500Z | 99.0  |",
                    "| us     | 1970-01-01T00:00:00.000001Z    | 100.0 |",
                    "| us     | 1970-01-01T00:00:00.000002500Z | 90.0  |",
                    "| us     | 1970-01-01T00:00:00.000002Z    | 95.0  |",
                    "+--------+--------------------------------+-------+",
                ],
            },
            TestCase {
                predicates: &[Predicate::new("host", KeyValue::string("b"))],
                expected: &[
                    "+--------+--------------------------------+-------+",
                    "| region | time                           | usage |",
                    "+--------+--------------------------------+-------+",
                    "| us     | 1970-01-01T00:00:00.000001500Z | 88.0  |",
                    "| us     | 1970-01-01T00:00:00.000001Z    | 80.0  |",
                    "| us     | 1970-01-01T00:00:00.000002500Z | 99.0  |",
                    "| us     | 1970-01-01T00:00:00.000002Z    | 92.0  |",
                    "+--------+--------------------------------+-------+",
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
                .last_cache()
                .get_cache_record_batches(db_name, tbl_name, None, t.predicates)
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

        // Create the last cache with keys on all tag columns:
        wbuf.create_last_cache(
            db_name,
            tbl_name,
            Some("cache"),
            // use a cache size greater than 1 to ensure the TTL is doing the evicting
            Some(10),
            Some(Duration::from_millis(50)),
            Some(vec!["region".to_string(), "host".to_string()]),
            None,
        )
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
            Predicate::new("region", KeyValue::string("us")),
            Predicate::new("host", KeyValue::string("a")),
        ];

        // Check what is in the last cache:
        let batches = wbuf
            .last_cache()
            .get_cache_record_batches(db_name, tbl_name, None, predicates)
            .unwrap()
            .unwrap();

        assert_batches_sorted_eq!(
            [
                "+-----------------------------+-------+",
                "| time                        | usage |",
                "+-----------------------------+-------+",
                "| 1970-01-01T00:00:00.000001Z | 100.0 |",
                "+-----------------------------+-------+",
            ],
            &batches
        );

        // wait for the TTL to clear the cache
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Check what is in the last cache:
        let batches = wbuf
            .last_cache()
            .get_cache_record_batches(db_name, tbl_name, None, predicates)
            .unwrap()
            .unwrap();

        assert_batches_sorted_eq!(
            [
                "+------+-------+",
                "| time | usage |",
                "+------+-------+",
                "+------+-------+",
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

        // Create the last cache with keys on some field columns:
        wbuf.create_last_cache(
            db_name,
            tbl_name,
            Some("cache"),
            None,
            Some(Duration::from_millis(50)),
            Some(vec![
                "component_id".to_string(),
                "active".to_string(),
                "type".to_string(),
                "loc".to_string(),
            ]),
            None,
        )
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
                predicates: &[Predicate::new("component_id", KeyValue::string("333"))],
                expected: &[
                    "+--------+--------+------+---------+-----------------------------+",
                    "| active | type   | loc  | reading | time                        |",
                    "+--------+--------+------+---------+-----------------------------+",
                    "| true   | camera | fore | 145.0   | 1970-01-01T00:00:00.000001Z |",
                    "+--------+--------+------+---------+-----------------------------+",
                ],
            },
            // Predicate on a non-string field key:
            TestCase {
                predicates: &[Predicate::new("active", KeyValue::Bool(false))],
                expected: &[
                    "+--------------+-------------+---------+---------+-----------------------------+",
                    "| component_id | type        | loc     | reading | time                        |",
                    "+--------------+-------------+---------+---------+-----------------------------+",
                    "| 555          | solar-panel | huygens | 200.0   | 1970-01-01T00:00:00.000001Z |",
                    "| 666          | comms-dish  | huygens | 220.0   | 1970-01-01T00:00:00.000001Z |",
                    "+--------------+-------------+---------+---------+-----------------------------+",
                ],
            },
            // Predicate on a string field key:
            TestCase {
                predicates: &[Predicate::new("type", KeyValue::string("camera"))],
                expected: &[
                    "+--------------+--------+-----------+---------+-----------------------------+",
                    "| component_id | active | loc       | reading | time                        |",
                    "+--------------+--------+-----------+---------+-----------------------------+",
                    "| 111          | true   | port      | 150.0   | 1970-01-01T00:00:00.000001Z |",
                    "| 222          | true   | starboard | 250.0   | 1970-01-01T00:00:00.000001Z |",
                    "| 333          | true   | fore      | 145.0   | 1970-01-01T00:00:00.000001Z |",
                    "+--------------+--------+-----------+---------+-----------------------------+",
                ],
            }
        ];

        for t in test_cases {
            let batches = wbuf
                .last_cache()
                .get_cache_record_batches(db_name, tbl_name, None, t.predicates)
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
            format!("{tbl_name} state/ca/county/napa/farm/10-01 speed=60").as_str(),
            Time::from_timestamp_nanos(500),
            false,
            Precision::Nanosecond,
        )
        .await
        .unwrap();

        // Create the last cache with keys on some field columns:
        wbuf.create_last_cache(db_name, tbl_name, Some("cache"), None, None, None, None)
            .expect("create last cache");

        // Write some lines to fill the cache:
        wbuf.write_lp_v3(
            NamespaceName::new(db_name).unwrap(),
            format!(
                "\
                {tbl_name} state/ca/county/napa/farm/10-01 speed=50\n\
                {tbl_name} state/ca/county/napa/farm/10-02 speed=49\n\
                {tbl_name} state/ca/county/orange/farm/20-01 speed=40\n\
                {tbl_name} state/ca/county/orange/farm/20-02 speed=33\n\
                {tbl_name} state/ca/county/yolo/farm/30-01 speed=62\n\
                {tbl_name} state/ca/county/nevada/farm/40-01 speed=66\n\
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
                predicates: &[Predicate::new("state", KeyValue::string("ca"))],
                expected: &[
                    "+--------+-------+-------+-----------------------------+",
                    "| county | farm  | speed | time                        |",
                    "+--------+-------+-------+-----------------------------+",
                    "| napa   | 10-01 | 50.0  | 1970-01-01T00:00:00.000001Z |",
                    "| napa   | 10-02 | 49.0  | 1970-01-01T00:00:00.000001Z |",
                    "| nevada | 40-01 | 66.0  | 1970-01-01T00:00:00.000001Z |",
                    "| orange | 20-01 | 40.0  | 1970-01-01T00:00:00.000001Z |",
                    "| orange | 20-02 | 33.0  | 1970-01-01T00:00:00.000001Z |",
                    "| yolo   | 30-01 | 62.0  | 1970-01-01T00:00:00.000001Z |",
                    "+--------+-------+-------+-----------------------------+",
                ],
            },
            // Predicate on county column, which is part of the series key:
            TestCase {
                predicates: &[Predicate::new("county", KeyValue::string("napa"))],
                expected: &[
                    "+-------+-------+-------+-----------------------------+",
                    "| state | farm  | speed | time                        |",
                    "+-------+-------+-------+-----------------------------+",
                    "| ca    | 10-01 | 50.0  | 1970-01-01T00:00:00.000001Z |",
                    "| ca    | 10-02 | 49.0  | 1970-01-01T00:00:00.000001Z |",
                    "+-------+-------+-------+-----------------------------+",
                ],
            },
            // Predicate on farm column, which is part of the series key:
            TestCase {
                predicates: &[Predicate::new("farm", KeyValue::string("30-01"))],
                expected: &[
                    "+-------+--------+-------+-----------------------------+",
                    "| state | county | speed | time                        |",
                    "+-------+--------+-------+-----------------------------+",
                    "| ca    | yolo   | 62.0  | 1970-01-01T00:00:00.000001Z |",
                    "+-------+--------+-------+-----------------------------+",
                ],
            },
            // Predicate on all series key columns:
            TestCase {
                predicates: &[
                    Predicate::new("state", KeyValue::string("ca")),
                    Predicate::new("county", KeyValue::string("nevada")),
                    Predicate::new("farm", KeyValue::string("40-01")),
                ],
                expected: &[
                    "+-------+-----------------------------+",
                    "| speed | time                        |",
                    "+-------+-----------------------------+",
                    "| 66.0  | 1970-01-01T00:00:00.000001Z |",
                    "+-------+-----------------------------+",
                ],
            },
        ];

        for t in test_cases {
            let batches = wbuf
                .last_cache()
                .get_cache_record_batches(db_name, tbl_name, None, t.predicates)
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

        // Create the last cache with keys on some field columns:
        wbuf.create_last_cache(db_name, tbl_name, Some("cache"), None, None, None, None)
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
                predicates: &[Predicate::new("state", KeyValue::string("ca"))],
                expected: &[
                    "+--------+-------+-------+-----------------------------+",
                    "| county | farm  | speed | time                        |",
                    "+--------+-------+-------+-----------------------------+",
                    "| napa   | 10-01 | 50.0  | 1970-01-01T00:00:00.000001Z |",
                    "| napa   | 10-02 | 49.0  | 1970-01-01T00:00:00.000001Z |",
                    "| nevada | 40-01 | 66.0  | 1970-01-01T00:00:00.000001Z |",
                    "| orange | 20-01 | 40.0  | 1970-01-01T00:00:00.000001Z |",
                    "| orange | 20-02 | 33.0  | 1970-01-01T00:00:00.000001Z |",
                    "| yolo   | 30-01 | 62.0  | 1970-01-01T00:00:00.000001Z |",
                    "+--------+-------+-------+-----------------------------+",
                ],
            },
            // Predicate on county column, which is part of the series key:
            TestCase {
                predicates: &[Predicate::new("county", KeyValue::string("napa"))],
                expected: &[
                    "+-------+-------+-------+-----------------------------+",
                    "| farm  | state | speed | time                        |",
                    "+-------+-------+-------+-----------------------------+",
                    "| 10-01 | ca    | 50.0  | 1970-01-01T00:00:00.000001Z |",
                    "| 10-02 | ca    | 49.0  | 1970-01-01T00:00:00.000001Z |",
                    "+-------+-------+-------+-----------------------------+",
                ],
            },
            // Predicate on farm column, which is part of the series key:
            TestCase {
                predicates: &[Predicate::new("farm", KeyValue::string("30-01"))],
                expected: &[
                    "+--------+-------+-------+-----------------------------+",
                    "| county | state | speed | time                        |",
                    "+--------+-------+-------+-----------------------------+",
                    "| yolo   | ca    | 62.0  | 1970-01-01T00:00:00.000001Z |",
                    "+--------+-------+-------+-----------------------------+",
                ],
            },
            // Predicate on all series key columns:
            TestCase {
                predicates: &[
                    Predicate::new("state", KeyValue::string("ca")),
                    Predicate::new("county", KeyValue::string("nevada")),
                    Predicate::new("farm", KeyValue::string("40-01")),
                ],
                expected: &[
                    "+-------+-----------------------------+",
                    "| speed | time                        |",
                    "+-------+-----------------------------+",
                    "| 66.0  | 1970-01-01T00:00:00.000001Z |",
                    "+-------+-----------------------------+",
                ],
            },
        ];

        for t in test_cases {
            let batches = wbuf
                .last_cache()
                .get_cache_record_batches(db_name, tbl_name, None, t.predicates)
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

        // Create the last cache using default tags as keys
        wbuf.create_last_cache(db_name, tbl_name, None, Some(10), None, None, None)
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
            .last_cache()
            .get_cache_record_batches(db_name, tbl_name, None, &[])
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

        // Create the last cache using default tags as keys
        wbuf.create_last_cache(db_name, tbl_name, None, Some(10), None, None, None)
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
                predicates: &[Predicate::new("game_id", KeyValue::string("4"))],
                expected: &[
                    "+-----------+-----------------------------+------+------+",
                    "| player    | time                        | type | zone |",
                    "+-----------+-----------------------------+------+------+",
                    "| bobrovsky | 1970-01-01T00:00:00.000001Z | save | home |",
                    "+-----------+-----------------------------+------+------+",
                ],
            },
            // Cache that does not have a zone column will produce it with nulls:
            TestCase {
                predicates: &[Predicate::new("game_id", KeyValue::string("1"))],
                expected: &[
                    "+-----------+-----------------------------+------+------+",
                    "| player    | time                        | type | zone |",
                    "+-----------+-----------------------------+------+------+",
                    "| mackinnon | 1970-01-01T00:00:00.000001Z | shot |      |",
                    "+-----------+-----------------------------+------+------+",
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
                .last_cache()
                .get_cache_record_batches(db_name, tbl_name, None, t.predicates)
                .unwrap()
                .unwrap();

            assert_batches_sorted_eq!(t.expected, &batches);
        }
    }
}
