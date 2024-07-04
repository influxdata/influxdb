use std::{
    any::Any,
    collections::VecDeque,
    sync::Arc,
    time::{Duration, Instant},
};

use arrow::{
    array::{
        ArrayRef, BooleanBuilder, Float64Builder, GenericByteDictionaryBuilder, Int64Builder,
        RecordBatch, StringBuilder, StringDictionaryBuilder, TimestampNanosecondBuilder,
        UInt64Builder,
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
use hashbrown::HashMap;
use indexmap::IndexMap;
use iox_time::Time;
use parking_lot::RwLock;
use schema::{InfluxColumnType, InfluxFieldType, Schema, SchemaBuilder, TIME_COLUMN_NAME};

use crate::{
    catalog::LastCacheSize,
    write_buffer::{buffer_segment::WriteBatch, FieldData, Row},
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

impl LastCacheProvider {
    /// Create a new [`LastCacheProvider`]
    pub(crate) fn new() -> Self {
        Self {
            cache_map: Default::default(),
        }
    }

    /// Create a new entry in the last cache for a given database and table, along with the given
    /// parameters.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn create_cache(
        &self,
        db_name: String,
        tbl_name: String,
        schema: Schema,
        cache_name: Option<String>,
        count: Option<usize>,
        ttl: Option<Duration>,
        key_columns: Option<Vec<String>>,
        value_columns: Option<Vec<String>>,
    ) -> Result<(), Error> {
        if self
            .cache_map
            .read()
            .get(&db_name)
            .is_some_and(|db| db.contains_key(&tbl_name))
        {
            return Err(Error::CacheAlreadyExists);
        }
        let key_columns = if let Some(keys) = key_columns {
            // validate the key columns specified to ensure correct type (string, int, unit, or bool)
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

        let cache_name = cache_name.unwrap_or_else(|| {
            format!("{tbl_name}_{keys}_last_cache", keys = key_columns.join("_"))
        });

        let value_columns = if let Some(mut vals) = value_columns {
            for name in vals.iter() {
                if schema.field_by_name(name).is_none() {
                    return Err(Error::ValueColumnDoesNotExist {
                        column_name: name.into(),
                    });
                }
            }
            let time_col = TIME_COLUMN_NAME.to_string();
            if !vals.contains(&time_col) {
                vals.push(time_col);
            }
            vals
        } else {
            schema
                .iter()
                .filter_map(|(_, f)| {
                    if key_columns.contains(f.name()) {
                        None
                    } else {
                        Some(f.name().to_string())
                    }
                })
                .collect::<Vec<String>>()
        };

        let mut schema_builder = SchemaBuilder::new();
        for (t, name) in schema
            .iter()
            .filter(|&(_, f)| value_columns.contains(f.name()))
            .map(|(t, f)| (t, f.name()))
        {
            schema_builder.influx_column(name, t);
        }

        let last_cache = LastCache::new(
            count
                .unwrap_or(1)
                .try_into()
                .map_err(|_| Error::InvalidCacheSize)?,
            ttl.unwrap_or(DEFAULT_CACHE_TTL),
            key_columns,
            schema_builder.build()?,
        );

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
                        if tbl_cache.is_empty() {
                            continue;
                        }
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

pub(crate) struct LastCache {
    count: LastCacheSize,
    ttl: Duration,
    key_columns: Vec<String>,
    schema: Schema,
    state: LastCacheState,
}

impl LastCache {
    fn new(count: LastCacheSize, ttl: Duration, key_columns: Vec<String>, schema: Schema) -> Self {
        Self {
            count,
            ttl,
            key_columns,
            schema,
            state: LastCacheState::Init,
        }
    }

    /// Push a [`Row`] from the write buffer into the cache
    ///
    /// If a key column is not present in the row, the row will be ignored.
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
                        self.schema.as_arrow(),
                    ))
                }
            });
        }
        // If there are no key columns we still need to initialize the state the first time:
        if target.is_init() {
            *target = LastCacheState::Store(LastCacheStore::new(
                self.count.into(),
                self.ttl,
                self.schema.as_arrow(),
            ));
        }
        target
            .as_store_mut()
            .expect(
                "cache target should be the actual store after iterating through all key columns",
            )
            .push(row);
    }

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

        caches.into_iter().map(|c| c.to_record_batch()).collect()
    }

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

    fn remove_expired(&mut self) {
        self.state.remove_expired();
    }
}

#[derive(Debug)]
struct ExtendedLastCacheState<'a> {
    state: &'a LastCacheState,
    additional_columns: Vec<(&'a String, &'a KeyValue)>,
}

impl<'a> ExtendedLastCacheState<'a> {
    fn to_record_batch(&self) -> Result<RecordBatch, ArrowError> {
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
        store.to_record_batch(extended)
    }
}

#[derive(Debug, Clone)]
struct Predicate {
    key: String,
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

#[derive(Debug)]
enum LastCacheState {
    Init,
    Key(LastCacheKey),
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

    fn remove_expired(&mut self) {
        match self {
            LastCacheState::Key(k) => k.remove_expired(),
            LastCacheState::Store(s) => s.remove_expired(),
            LastCacheState::Init => (),
        }
    }
}

#[derive(Debug)]
struct LastCacheKey {
    column_name: String,
    value_map: HashMap<KeyValue, LastCacheState>,
}

impl LastCacheKey {
    fn evaluate_predicate(&self, predicate: &Predicate) -> Option<&LastCacheState> {
        if predicate.key != self.column_name {
            panic!(
                "attempted to evaluate unexpected predicate with key {} for column named {}",
                predicate.key, self.column_name
            );
        }
        self.value_map.get(&predicate.value)
    }

    fn remove_expired(&mut self) {
        self.value_map
            .iter_mut()
            .for_each(|(_, m)| m.remove_expired());
    }
}

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

/// Stores the last N values, as configured, for a given table in a database
#[derive(Debug)]
struct LastCacheStore {
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
    // TODO: will need to use this if new columns are added for * caches
    _ttl: Duration,
    last_time: Time,
}

impl LastCacheStore {
    /// Create a new [`LastCache`]
    fn new(count: usize, ttl: Duration, schema: ArrowSchemaRef) -> Self {
        let cache = schema
            .fields()
            .iter()
            .map(|f| {
                (
                    f.name().to_string(),
                    CacheColumn::new(f.data_type(), count, ttl),
                )
            })
            .collect();
        Self {
            schema,
            cache,
            _ttl: ttl,
            last_time: Time::from_timestamp_nanos(0),
        }
    }

    fn len(&self) -> usize {
        self.cache.first().map_or(0, |(_, c)| c.len())
    }

    /// Push a [`Row`] from the buffer into this cache
    fn push(&mut self, row: &Row) {
        if row.time <= self.last_time.timestamp_nanos() {
            return;
        }
        for field in &row.fields {
            if let Some(c) = self.cache.get_mut(&field.name) {
                c.push(&field.value);
            }
        }
        self.last_time = Time::from_timestamp_nanos(row.time);
    }

    /// Convert the contents of this cache into a arrow [`RecordBatch`]
    fn to_record_batch(
        &self,
        extended: Option<(Vec<FieldRef>, Vec<ArrayRef>)>,
    ) -> Result<RecordBatch, ArrowError> {
        // This is where using IndexMap is important, because we inserted the cache entries
        // using the schema field ordering, we will get the correct order by iterating directly
        // over the map here:
        let cache_arrays = self.cache.iter().map(|(_, c)| c.data.as_array());
        let (schema, arrays) = if let Some((fields, mut arrays)) = extended {
            let mut sb = ArrowSchemaBuilder::new();
            sb.extend(fields);
            sb.extend(self.schema.fields().iter().map(Arc::clone));
            arrays.extend(cache_arrays);
            (Arc::new(sb.finish()), arrays)
        } else {
            (Arc::clone(&self.schema), cache_arrays.collect())
        };
        RecordBatch::try_new(schema, arrays)
    }

    fn remove_expired(&mut self) {
        self.cache.iter_mut().for_each(|(_, c)| c.remove_expired());
    }
}

#[async_trait]
impl TableProvider for LastCache {
    fn as_any(&self) -> &dyn Any {
        self as &dyn Any
    }

    fn schema(&self) -> ArrowSchemaRef {
        self.schema.as_arrow()
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
/// Stores its size so it can evict old data on push.
#[derive(Debug)]
struct CacheColumn {
    size: usize,
    ttl: Duration,
    data: CacheColumnData,
}

impl CacheColumn {
    /// Create a new [`CacheColumn`] for the given arrow [`DataType`] and size
    fn new(data_type: &DataType, size: usize, ttl: Duration) -> Self {
        Self {
            size,
            ttl,
            data: CacheColumnData::new(data_type, size),
        }
    }

    fn len(&self) -> usize {
        self.data.len()
    }

    /// Push [`FieldData`] from the buffer into this column
    fn push(&mut self, field_data: &FieldData) {
        if self.data.len() >= self.size {
            self.data.pop_back();
        }
        self.data.push_front(field_data);
    }

    fn remove_expired(&mut self) {
        self.data.remove_expired(self.ttl);
    }
}

/// Enumerated type for storing column data for the cache in a ring buffer
#[derive(Debug)]
enum CacheColumnData {
    I64(ColumnBuffer<i64>),
    U64(ColumnBuffer<u64>),
    F64(ColumnBuffer<f64>),
    String(ColumnBuffer<String>),
    Bool(ColumnBuffer<bool>),
    Tag(ColumnBuffer<String>),
    Time(ColumnBuffer<i64>),
}

impl CacheColumnData {
    /// Create a new [`CacheColumnData`]
    fn new(data_type: &DataType, size: usize) -> Self {
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
                Self::Tag(ColumnBuffer::with_capacity(size))
            }
            _ => panic!("unsupported data type for last cache: {data_type}"),
        }
    }

    /// Get the length of the [`CacheColumn`]
    fn len(&self) -> usize {
        match self {
            CacheColumnData::I64(v) => v.len(),
            CacheColumnData::U64(v) => v.len(),
            CacheColumnData::F64(v) => v.len(),
            CacheColumnData::String(v) => v.len(),
            CacheColumnData::Bool(v) => v.len(),
            CacheColumnData::Tag(v) => v.len(),
            CacheColumnData::Time(v) => v.len(),
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
            CacheColumnData::Time(v) => {
                v.pop_back();
            }
        }
    }

    /// Push a new element into the [`CacheColumn`]
    fn push_front(&mut self, field_data: &FieldData) {
        match (field_data, self) {
            (FieldData::Timestamp(d), CacheColumnData::Time(v)) => v.push_front(*d),
            (FieldData::Key(d), CacheColumnData::String(v)) => v.push_front(d.to_owned()),
            (FieldData::Tag(d), CacheColumnData::Tag(v)) => v.push_front(d.to_owned()),
            (FieldData::String(d), CacheColumnData::String(v)) => v.push_front(d.to_owned()),
            (FieldData::Integer(d), CacheColumnData::I64(v)) => v.push_front(*d),
            (FieldData::UInteger(d), CacheColumnData::U64(v)) => v.push_front(*d),
            (FieldData::Float(d), CacheColumnData::F64(v)) => v.push_front(*d),
            (FieldData::Boolean(d), CacheColumnData::Bool(v)) => v.push_front(*d),
            _ => panic!("invalid field data for cache column"),
        }
    }

    fn remove_expired(&mut self, ttl: Duration) {
        match self {
            CacheColumnData::I64(b) => b.remove_expired(ttl),
            CacheColumnData::U64(b) => b.remove_expired(ttl),
            CacheColumnData::F64(b) => b.remove_expired(ttl),
            CacheColumnData::String(b) => b.remove_expired(ttl),
            CacheColumnData::Bool(b) => b.remove_expired(ttl),
            CacheColumnData::Tag(b) => b.remove_expired(ttl),
            CacheColumnData::Time(b) => b.remove_expired(ttl),
        }
    }

    /// Produce an arrow [`ArrayRef`] from this column for the sake of producing [`RecordBatch`]es
    fn as_array(&self) -> ArrayRef {
        match self {
            CacheColumnData::I64(v) => {
                let mut b = Int64Builder::new();
                v.iter().for_each(|val| b.append_value(*val));
                Arc::new(b.finish())
            }
            CacheColumnData::U64(v) => {
                let mut b = UInt64Builder::new();
                v.iter().for_each(|val| b.append_value(*val));
                Arc::new(b.finish())
            }
            CacheColumnData::F64(v) => {
                let mut b = Float64Builder::new();
                v.iter().for_each(|val| b.append_value(*val));
                Arc::new(b.finish())
            }
            CacheColumnData::String(v) => {
                let mut b = StringBuilder::new();
                v.iter().for_each(|val| b.append_value(val));
                Arc::new(b.finish())
            }
            CacheColumnData::Bool(v) => {
                let mut b = BooleanBuilder::new();
                v.iter().for_each(|val| b.append_value(*val));
                Arc::new(b.finish())
            }
            CacheColumnData::Tag(v) => {
                let mut b: GenericByteDictionaryBuilder<Int32Type, GenericStringType<i32>> =
                    StringDictionaryBuilder::new();
                v.iter().for_each(|val| {
                    b.append(val)
                        .expect("should not overflow 32 bit dictionary");
                });
                Arc::new(b.finish())
            }
            CacheColumnData::Time(v) => {
                let mut b = TimestampNanosecondBuilder::new();
                v.iter().for_each(|val| b.append_value(*val));
                Arc::new(b.finish())
            }
        }
    }
}

#[derive(Debug)]
struct ColumnBuffer<T>(VecDeque<(Instant, T)>);

impl<T> ColumnBuffer<T> {
    fn with_capacity(capacity: usize) -> Self {
        Self(VecDeque::with_capacity(capacity))
    }

    fn len(&self) -> usize {
        self.0.len()
    }

    fn pop_back(&mut self) {
        self.0.pop_back();
    }

    fn push_front(&mut self, value: T) {
        self.0.push_front((Instant::now(), value));
    }

    fn remove_expired(&mut self, ttl: Duration) {
        while let Some((created, _)) = self.0.back() {
            if created.elapsed() >= ttl {
                self.0.pop_back();
            } else {
                return;
            }
        }
    }

    fn iter(&self) -> impl Iterator<Item = &T> {
        self.0.iter().map(|(_, v)| v)
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
}
