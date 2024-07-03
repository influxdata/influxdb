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
    datatypes::{DataType, GenericStringType, Int32Type, SchemaRef, TimeUnit},
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
    SchemaBuilderError(#[from] schema::builder::Error),
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
        let db_name = db_name.into();
        let tbl_name = tbl_name.into();
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
            .filter_map(|(t, f)| value_columns.contains(f.name()).then(|| (t, f.name())))
        {
            schema_builder.influx_column(name, t);
        }

        let last_cache = LastCache::new(
            count
                .unwrap_or(1)
                .try_into()
                .map_err(|_| Error::InvalidCacheSize)?,
            ttl.unwrap_or(DEFAULT_CACHE_TTL),
            key_columns.into(),
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
        cache_map.iter_mut().for_each(|(_, db)| {
            db.iter_mut()
                .for_each(|(_, tbl)| tbl.iter_mut().for_each(|(_, lc)| lc.remove_expired()))
        });
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
        let mut state =
            LastCacheState::Store(LastCacheStore::new(count.into(), ttl, schema.as_arrow()));
        for column_name in key_columns.iter().cloned().rev() {
            state = LastCacheState::Key(LastCacheKey {
                column_name,
                value_map: [(None, state)].into(),
            });
        }
        Self {
            count,
            ttl,
            key_columns,
            schema,
            state,
        }
    }

    pub(crate) fn push(&mut self, row: &Row) {
        let mut target = &mut self.state;
        let mut key_iter = self.key_columns.iter().peekable();
        while let (Some(key), peek) = (key_iter.next(), key_iter.peek()) {
            let value = row
                .fields
                .iter()
                .find(|f| f.name == *key)
                .map(|f| KeyValue::from(&f.value));
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

        let mut caches = vec![&self.state];
        let mut predicate_iter = predicates.into_iter();

        while let Some(predicate) = predicate_iter.next() {
            if caches.is_empty() {
                return Ok(vec![]);
            }
            let mut new_caches = vec![];
            for c in caches.iter() {
                let cache_key = c.as_key().unwrap();
                if let Some(ref pred) = predicate {
                    if let Some(next_state) = cache_key.evaluate_predicate(pred) {
                        new_caches.push(next_state);
                    }
                } else {
                    new_caches.extend(cache_key.value_map.iter().map(|(_, s)| s));
                }
            }
            caches = new_caches;
        }

        caches.into_iter().map(|c| {
            c.as_store().expect("caches should all be stores after iterating through all key column-mapped predicates").to_record_batch()
        }).collect()
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
                                    value: Some(KeyValue::String(v.to_owned())),
                                }),
                                Expr::Literal(ScalarValue::Boolean(Some(v))) => Some(Predicate {
                                    key,
                                    value: Some(KeyValue::Bool(*v)),
                                }),
                                // TODO: handle integer types that can be casted up to i64/u64:
                                Expr::Literal(ScalarValue::Int64(Some(v))) => Some(Predicate {
                                    key,
                                    value: Some(KeyValue::Int(*v)),
                                }),
                                Expr::Literal(ScalarValue::UInt64(Some(v))) => Some(Predicate {
                                    key,
                                    value: Some(KeyValue::UInt(*v)),
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

#[derive(Debug, Clone)]
struct Predicate {
    key: String,
    value: Option<KeyValue>,
}

#[cfg(test)]
impl Predicate {
    fn new(key: impl Into<String>, value: Option<KeyValue>) -> Self {
        Self {
            key: key.into(),
            value,
        }
    }
}

enum LastCacheState {
    Key(LastCacheKey),
    Store(LastCacheStore),
}

impl LastCacheState {
    fn as_key(&self) -> Option<&LastCacheKey> {
        match self {
            LastCacheState::Key(key) => Some(key),
            LastCacheState::Store(_) => None,
        }
    }

    fn as_store(&self) -> Option<&LastCacheStore> {
        match self {
            LastCacheState::Key(_) => None,
            LastCacheState::Store(store) => Some(store),
        }
    }

    fn as_key_mut(&mut self) -> Option<&mut LastCacheKey> {
        match self {
            LastCacheState::Key(key) => Some(key),
            LastCacheState::Store(_) => None,
        }
    }

    fn as_store_mut(&mut self) -> Option<&mut LastCacheStore> {
        match self {
            LastCacheState::Key(_) => None,
            LastCacheState::Store(store) => Some(store),
        }
    }

    fn remove_expired(&mut self) {
        match self {
            LastCacheState::Key(k) => k.remove_expired(),
            LastCacheState::Store(s) => s.remove_expired(),
        }
    }
}

struct LastCacheKey {
    column_name: String,
    value_map: HashMap<Option<KeyValue>, LastCacheState>,
}

impl LastCacheKey {
    fn evaluate_predicate(&self, predicate: &Predicate) -> Option<&LastCacheState> {
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
struct LastCacheStore {
    schema: SchemaRef,
    // use an IndexMap to preserve insertion order:
    cache: IndexMap<String, CacheColumn>,
    // TODO: will need to use this if new columns are added for * caches
    _ttl: Duration,
    last_time: Time,
}

impl LastCacheStore {
    /// Create a new [`LastCache`]
    fn new(count: usize, ttl: Duration, schema: SchemaRef) -> Self {
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

    /// Push a [`Row`] from the buffer into this cache
    fn push(&mut self, row: &Row) {
        if row.time <= self.last_time.timestamp_nanos() {
            return;
        }
        let time_col = self
            .cache
            .get_mut(TIME_COLUMN_NAME)
            .expect("there should always be a time column");
        time_col.push(&FieldData::Timestamp(row.time));
        for field in &row.fields {
            if let Some(c) = self.cache.get_mut(&field.name) {
                c.push(&field.value);
            }
        }
        self.last_time = Time::from_timestamp_nanos(row.time);
    }

    /// Convert the contents of this cache into a arrow [`RecordBatch`]
    fn to_record_batch(&self) -> Result<RecordBatch, ArrowError> {
        RecordBatch::try_new(
            Arc::clone(&self.schema),
            // This is where using IndexMap is important, because we inserted the cache entries
            // using the schema field ordering, we will get the correct order by iterating directly
            // over the map here:
            self.cache.iter().map(|(_, c)| c.data.as_array()).collect(),
        )
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

    fn schema(&self) -> SchemaRef {
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
        while let Some((ins, _)) = self.0.back() {
            if ins.elapsed() >= ttl {
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
    use std::sync::Arc;

    use ::object_store::{memory::InMemory, ObjectStore};
    use arrow_util::assert_batches_eq;
    use data_types::NamespaceName;
    use iox_time::{MockProvider, Time};

    use crate::{
        last_cache::{KeyValue, Predicate},
        persister::PersisterImpl,
        wal::WalImpl,
        write_buffer::WriteBufferImpl,
        Bufferer, Precision, SegmentDuration,
    };

    #[tokio::test]
    async fn pick_up_latest_write() {
        let obj_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let persister = Arc::new(PersisterImpl::new(obj_store));
        let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
        let wbuf = WriteBufferImpl::new(
            persister,
            Option::<Arc<WalImpl>>::None,
            time_provider,
            SegmentDuration::new_5m(),
            crate::test_help::make_exec(),
            1000,
        )
        .await
        .unwrap();

        let db_name = "foo";
        let tbl_name = "cpu";

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

        let predicates = &[Predicate::new("host", Some(KeyValue::string("a")))];

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
}
