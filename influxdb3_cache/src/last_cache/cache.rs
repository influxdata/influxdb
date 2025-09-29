use std::{
    collections::{BTreeSet, HashMap, HashSet, VecDeque},
    sync::Arc,
    time::{Duration, Instant},
};

use arrow::{
    array::{
        ArrayRef, BooleanBuilder, Float64Builder, GenericByteDictionaryBuilder, Int64Builder,
        RecordBatch, StringBuilder, StringDictionaryBuilder, TimestampNanosecondBuilder,
        UInt64Builder, new_null_array,
    },
    datatypes::{
        DataType, Field as ArrowField, GenericStringType, Int32Type,
        SchemaBuilder as ArrowSchemaBuilder, SchemaRef as ArrowSchemaRef,
    },
    error::ArrowError,
};
use indexmap::{IndexMap, IndexSet};
use influxdb3_catalog::catalog::legacy;
use influxdb3_catalog::{
    catalog::{TIME_COLUMN_NAME, TableDefinition},
    log::{LastCacheSize, LastCacheTtl},
};
use influxdb3_id::ColumnId;
use influxdb3_wal::{Field, FieldData, Row};
use iox_time::Time;
use schema::{InfluxColumnType, InfluxFieldType};

use super::Error;

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
    /// Uses an [`IndexSet`] for both fast iteration and fast lookup and more importantly, this
    /// map preserves the order of the elements, thereby maintaining the order of the keys in
    /// the cache.
    pub(crate) key_column_ids: Arc<IndexSet<ColumnId>>,
    /// The value columns for this cache
    pub(crate) value_columns: ValueColumnType,
    /// The Arrow Schema for the table that this cache is associated with
    pub(crate) schema: ArrowSchemaRef,
    /// The internal state of the cache
    pub(crate) state: LastCacheState,
}

#[derive(Debug, Clone)]
pub struct CreateLastCacheArgs {
    /// The definition of the table for which the cache is being created
    pub table_def: Arc<TableDefinition>,
    /// The number of values to hold in the created cache
    pub count: LastCacheSize,
    /// The time-to-live (TTL) for the created cache
    pub ttl: LastCacheTtl,
    /// The key column names to use in the cache hierarchy
    pub key_columns: LastCacheKeyColumnsArg,
    /// The value columns to use in the cache
    pub value_columns: LastCacheValueColumnsArg,
}

/// Specifies the key column configuration for a new [`LastCache`]
#[derive(Debug, Default, Clone)]
pub enum LastCacheKeyColumnsArg {
    /// Use the series key columns in their order as the last cache key columns
    #[default]
    SeriesKey,
    /// Provide an explicit set of columns to use for the keys in the cache
    Explicit(Vec<ColumnId>),
}

impl From<Option<Vec<ColumnId>>> for LastCacheKeyColumnsArg {
    fn from(value: Option<Vec<ColumnId>>) -> Self {
        match value {
            Some(columns) => Self::Explicit(columns),
            None => Self::SeriesKey,
        }
    }
}

/// Specifies the value column configuration for a new [`LastCache`]
#[derive(Debug, Default, Clone)]
pub enum LastCacheValueColumnsArg {
    /// Use all non-key columns as value columns when initialized, and add new field columns that
    /// are added to the table to the value columns in the cache
    #[default]
    AcceptNew,
    /// Provide an explicit set of columns to use for values stored in the cache
    ///
    /// The `time` column will be included if not specified.
    Explicit(Vec<ColumnId>),
}

impl From<Option<Vec<ColumnId>>> for LastCacheValueColumnsArg {
    fn from(value: Option<Vec<ColumnId>>) -> Self {
        match value {
            Some(columns) => Self::Explicit(columns),
            None => Self::AcceptNew,
        }
    }
}

impl LastCache {
    /// Create a new [`LastCache`]
    ///
    /// The uses the provided `TableDefinition` to build an arrow schema for the cache. This will
    /// validate the given arguments and can error if there are invalid columns specified, or if a
    /// non-compatible column is used as a key to the cache.
    pub(crate) fn new(
        CreateLastCacheArgs {
            table_def,
            count,
            ttl,
            key_columns,
            value_columns,
        }: CreateLastCacheArgs,
    ) -> Result<Self, Error> {
        let table_def = legacy::TableDefinition::new(table_def);

        let mut seen = HashSet::new();
        let mut schema_builder = ArrowSchemaBuilder::new();
        // handle key columns:
        let key_column_definitions: Vec<legacy::ColumnDefinition> = match &key_columns {
            LastCacheKeyColumnsArg::SeriesKey => table_def.series_key.iter(),
            LastCacheKeyColumnsArg::Explicit(col_ids) => col_ids.iter(),
        }
        .map(|id| {
            seen.insert(*id);
            table_def
                .column_definition_by_id(id)
                .ok_or(Error::KeyColumnDoesNotExist { column_id: *id })
        })
        .collect::<Result<Vec<_>, Error>>()?;
        let mut key_column_ids = IndexSet::new();
        for col_def in key_column_definitions {
            use InfluxFieldType::*;
            match col_def.data_type {
                InfluxColumnType::Tag => {
                    // override tags with string type in the schema, because the KeyValue type stores
                    // them as strings, and produces them as StringArray when creating RecordBatches:
                    schema_builder.push(ArrowField::new(
                        col_def.name.as_ref(),
                        DataType::Utf8,
                        false,
                    ))
                }
                InfluxColumnType::Field(Integer)
                | InfluxColumnType::Field(UInteger)
                | InfluxColumnType::Field(String)
                | InfluxColumnType::Field(Boolean) => schema_builder.push(ArrowField::new(
                    col_def.name.as_ref(),
                    DataType::from(&col_def.data_type),
                    true,
                )),
                column_type => return Err(Error::InvalidKeyColumn { column_type }),
            }
            key_column_ids.insert(col_def.id);
        }

        // handle value columns:
        let value_column_definitions: Vec<legacy::ColumnDefinition> = match &value_columns {
            LastCacheValueColumnsArg::AcceptNew => table_def
                .columns
                .iter()
                .filter(|(id, _)| !key_column_ids.contains(*id))
                .map(|(_, def)| def)
                .collect(),
            LastCacheValueColumnsArg::Explicit(col_ids) => col_ids
                .iter()
                // filter out time column if specified, as we will add at end
                .filter(|id| {
                    table_def
                        .column_definition_by_id(id)
                        .is_some_and(|def| def.name.as_ref() != TIME_COLUMN_NAME)
                })
                // validate columns specified:
                .map(|id| {
                    table_def
                        .column_definition_by_id(id)
                        .ok_or(Error::ColumnDoesNotExistById { column_id: *id })
                })
                // now add the time column
                .chain(
                    table_def
                        .columns
                        .iter()
                        .filter(|(_, def)| def.name.as_ref() == TIME_COLUMN_NAME)
                        .map(|(_, def)| Ok(def)),
                )
                .collect::<Result<Vec<_>, Error>>()?,
        };
        for col_def in &value_column_definitions {
            seen.insert(col_def.id);
            schema_builder.push(ArrowField::new(
                col_def.name.as_ref(),
                DataType::from(&col_def.data_type),
                true,
            ));
        }

        Ok(Self {
            count,
            ttl: ttl.into(),
            key_column_ids: Arc::new(key_column_ids),
            value_columns: match value_columns {
                LastCacheValueColumnsArg::AcceptNew => ValueColumnType::AcceptNew { seen },
                LastCacheValueColumnsArg::Explicit(_) => ValueColumnType::Explicit {
                    // use the derived value columns to ensure we pick up time:
                    columns: value_column_definitions.iter().map(|def| def.id).collect(),
                },
            },
            schema: Arc::new(schema_builder.finish()),
            state: LastCacheState::Init,
        })
    }

    /// Compare this cache's configuration with that of another
    pub(crate) fn compare_config(&self, other: &Self) -> Result<(), Error> {
        if self.count != other.count {
            return Err(Error::cache_already_exists(
                "different cache size specified",
            ));
        }
        if self.ttl != other.ttl {
            return Err(Error::cache_already_exists("different ttl specified"));
        }
        if self.key_column_ids != other.key_column_ids {
            return Err(Error::cache_already_exists("key columns are not the same"));
        }
        if self.value_columns != other.value_columns {
            return Err(Error::cache_already_exists(
                "provided value columns are not the same",
            ));
        }
        Ok(())
    }

    pub(crate) fn arrow_schema(&self) -> ArrowSchemaRef {
        Arc::clone(&self.schema)
    }

    fn should_update_schema_from_row(&self, row: &Row) -> bool {
        match &self.value_columns {
            ValueColumnType::AcceptNew { seen } => row.fields.iter().any(|f| !seen.contains(&f.id)),
            ValueColumnType::Explicit { .. } => false,
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
    pub(crate) fn push(&mut self, row: &Row, table_def: Arc<TableDefinition>) {
        let legacy_def = legacy::TableDefinition::new(Arc::clone(&table_def));

        let mut values = Vec::with_capacity(self.key_column_ids.len());
        for id in self.key_column_ids.iter() {
            let Some(value) = row
                .fields
                .iter()
                .find(|f| &f.id == id)
                .map(|f| KeyValue::from(&f.value))
            else {
                // ignore the row if it does not contain all key columns
                return;
            };
            values.push(value);
        }
        let mut target = &mut self.state;
        let mut iter = self.key_column_ids.iter().zip(values).peekable();
        while let (Some((col_id, value)), peek) = (iter.next(), iter.peek()) {
            if target.is_init() {
                *target = LastCacheState::Key(LastCacheKey {
                    column_id: *col_id,
                    value_map: Default::default(),
                });
            }
            let cache_key = target.as_key_mut().unwrap();
            assert_eq!(
                &cache_key.column_id, col_id,
                "key columns must match cache key order"
            );
            target = cache_key.value_map.entry(value).or_insert_with(|| {
                if let Some((next_col_id, _)) = peek {
                    LastCacheState::Key(LastCacheKey {
                        column_id: **next_col_id,
                        value_map: Default::default(),
                    })
                } else {
                    LastCacheState::Store(LastCacheStore::new(
                        self.count.into(),
                        self.ttl,
                        &legacy_def,
                        Arc::clone(&self.key_column_ids),
                        &self.value_columns,
                    ))
                }
            });
        }
        // If there are no key columns we still need to initialize the state the first time:
        if target.is_init() {
            *target = LastCacheState::Store(LastCacheStore::new(
                self.count.into(),
                self.ttl,
                &legacy_def,
                Arc::clone(&self.key_column_ids),
                &self.value_columns,
            ));
        }
        let store = target.as_store_mut().expect(
            "cache target should be the actual store after iterating through all key columns",
        );
        store.push(row);
        if self.should_update_schema_from_row(row) {
            let (schema, seen) = update_last_cache_schema_for_new_fields(
                &legacy_def,
                self.key_column_ids.iter().copied().collect(),
            );
            self.schema = schema;
            self.value_columns = ValueColumnType::AcceptNew { seen };
        }
    }

    /// Produce a set of [`RecordBatch`]es from the cache, using the given set of [`Predicate`]s
    pub(crate) fn to_record_batches(
        &self,
        table_def: Arc<TableDefinition>,
        predicates: &IndexMap<ColumnId, Predicate>,
    ) -> Result<Vec<RecordBatch>, ArrowError> {
        // map the provided predicates on to the key columns
        // there may not be predicates provided for each key column, hence the Option
        let predicates: Vec<Option<&Predicate>> = self
            .key_column_ids
            .iter()
            .map(|id| predicates.get(id))
            .collect();

        let mut caches = vec![ExtendedLastCacheState {
            state: &self.state,
            key_column_values: vec![],
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
                        let mut additional_columns = c.key_column_values.clone();
                        additional_columns.push(value);
                        ExtendedLastCacheState {
                            state,
                            key_column_values: additional_columns,
                        }
                    }));
                } else {
                    new_caches.extend(cache_key.value_map.iter().map(|(v, state)| {
                        let mut additional_columns = c.key_column_values.clone();
                        additional_columns.push(v);
                        ExtendedLastCacheState {
                            state,
                            key_column_values: additional_columns,
                        }
                    }));
                }
            }
            caches = new_caches;
        }

        let table_def = legacy::TableDefinition::new(table_def);

        caches
            .into_iter()
            .map(|c| c.to_record_batch(&table_def, Arc::clone(&self.schema)))
            .collect()
    }

    /// Remove expired values from the internal cache state
    pub(crate) fn remove_expired(&mut self) {
        self.state.remove_expired();
    }
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum ValueColumnType {
    /// Accept new fields and store them in the cache when received in incoming writes.
    ///
    /// Tracks the columns that have been _seen_ so it can check incoming writes for new columns.
    AcceptNew { seen: HashSet<ColumnId> },
    /// The cache uses an explicit set of columns to store as values. This is the more optimal
    /// configuration as it allows the cache to use a static schema that never changes.
    Explicit { columns: Vec<ColumnId> },
}

/// Extend a [`LastCacheState`] with additional columns
///
/// This is used for scenarios where key column values need to be produced in query outputs. Since
/// They are not stored in the terminal [`LastCacheStore`], we pass them down using this structure.
#[derive(Debug)]
struct ExtendedLastCacheState<'a> {
    state: &'a LastCacheState,
    key_column_values: Vec<&'a KeyValue>,
}

impl ExtendedLastCacheState<'_> {
    /// Produce a set of [`RecordBatch`]es from this extended state
    ///
    /// This converts any additional columns to arrow arrays which will extend the [`RecordBatch`]es
    /// produced by the inner [`LastCacheStore`]
    ///
    /// # Panics
    ///
    /// This assumes that the `state` is a [`LastCacheStore`] and will panic otherwise.
    fn to_record_batch(
        &self,
        table_def: &legacy::TableDefinition,
        schema: ArrowSchemaRef,
    ) -> Result<RecordBatch, ArrowError> {
        let store = self
            .state
            .as_store()
            .expect("should only be calling to_record_batch when using a store");
        // Determine the number of elements that have not expired up front, so that the value used
        // is consistent in the chain of methods used to produce record batches below:
        let n_non_expired = store.len();
        let extended: Option<Vec<ArrayRef>> = if self.key_column_values.is_empty() {
            None
        } else {
            Some(
                self.key_column_values
                    .iter()
                    .map(|value| match value {
                        KeyValue::String(v) => {
                            let mut builder = StringBuilder::new();
                            for _ in 0..n_non_expired {
                                builder.append_value(v);
                            }
                            Arc::new(builder.finish()) as ArrayRef
                        }
                        KeyValue::Int(v) => {
                            let mut builder = Int64Builder::new();
                            for _ in 0..n_non_expired {
                                builder.append_value(*v);
                            }
                            Arc::new(builder.finish()) as ArrayRef
                        }
                        KeyValue::UInt(v) => {
                            let mut builder = UInt64Builder::new();
                            for _ in 0..n_non_expired {
                                builder.append_value(*v);
                            }
                            Arc::new(builder.finish()) as ArrayRef
                        }
                        KeyValue::Bool(v) => {
                            let mut builder = BooleanBuilder::new();
                            for _ in 0..n_non_expired {
                                builder.append_value(*v);
                            }
                            Arc::new(builder.finish()) as ArrayRef
                        }
                    })
                    .collect(),
            )
        };
        store.to_record_batch(table_def, schema, extended, n_non_expired)
    }
}

/// A predicate used for evaluating key column values in the cache on query
///
/// Can either be an inclusive set or exclusive set. `BTreeSet` is used to
/// have the predicate values odered and displayed in a sorted order in
/// query `EXPLAIN` plans.
#[derive(Debug, Clone)]
pub(crate) enum Predicate {
    In(BTreeSet<KeyValue>),
    NotIn(BTreeSet<KeyValue>),
}

impl std::fmt::Display for Predicate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Predicate::In(_) => write!(f, "IN (")?,
            Predicate::NotIn(_) => write!(f, "NOT IN (")?,
        }
        let mut values = self.values();
        while let Some(v) = values.next() {
            write!(f, "{v}")?;
            if values.size_hint().0 > 0 {
                write!(f, ",")?;
            }
        }

        write!(f, ")")
    }
}

impl Predicate {
    fn values(&self) -> impl Iterator<Item = &KeyValue> {
        match self {
            Predicate::In(btree_set) => btree_set.iter(),
            Predicate::NotIn(btree_set) => btree_set.iter(),
        }
    }
}

#[cfg(test)]
impl Predicate {
    pub(crate) fn new_in(values: impl IntoIterator<Item = KeyValue>) -> Self {
        Self::In(values.into_iter().collect())
    }

    pub(crate) fn new_not_in(values: impl IntoIterator<Item = KeyValue>) -> Self {
        Self::NotIn(values.into_iter().collect())
    }
}

/// Represents the hierarchical last cache structure
#[derive(Debug)]
pub(crate) enum LastCacheState {
    /// An initialized state that is used for easy construction of the cache
    Init,
    /// Represents a branch node in the hierarchy of key columns for the cache
    Key(LastCacheKey),
    /// Represents a terminal node in the hierarchy, i.e., the cache of field values
    Store(LastCacheStore),
}

impl LastCacheState {
    pub(crate) fn is_init(&self) -> bool {
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

    pub(crate) fn as_key_mut(&mut self) -> Option<&mut LastCacheKey> {
        match self {
            LastCacheState::Key(key) => Some(key),
            LastCacheState::Store(_) | LastCacheState::Init => None,
        }
    }

    pub(crate) fn as_store_mut(&mut self) -> Option<&mut LastCacheStore> {
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
pub(crate) struct LastCacheKey {
    /// The column's ID
    pub(crate) column_id: ColumnId,
    /// A map of key column value to nested [`LastCacheState`]
    ///
    /// All values should point at either another key or a [`LastCacheStore`]
    pub(crate) value_map: HashMap<KeyValue, LastCacheState>,
}

impl LastCacheKey {
    /// Evaluate the provided [`Predicate`] by using its value to lookup in this [`LastCacheKey`]'s
    /// value map.
    fn evaluate_predicate<'a: 'b, 'b>(
        &'a self,
        predicate: &'b Predicate,
    ) -> Vec<(&'a LastCacheState, &'b KeyValue)> {
        match predicate {
            Predicate::In(vals) => vals
                .iter()
                .filter_map(|v| self.value_map.get(v).map(|s| (s, v)))
                .collect(),
            Predicate::NotIn(vals) => self
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
#[derive(Debug, Clone, Eq, PartialEq, Hash, PartialOrd, Ord)]
pub(crate) enum KeyValue {
    String(String),
    Int(i64),
    UInt(u64),
    Bool(bool),
}

impl std::fmt::Display for KeyValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            KeyValue::String(s) => write!(f, "'{s}'"),
            KeyValue::Int(i) => write!(f, "{i}i"),
            KeyValue::UInt(u) => write!(f, "{u}u"),
            KeyValue::Bool(b) => write!(f, "{b}"),
        }
    }
}

#[cfg(test)]
impl KeyValue {
    pub(crate) fn string(s: impl Into<String>) -> Self {
        Self::String(s.into())
    }
}

impl From<&FieldData> for KeyValue {
    fn from(field: &FieldData) -> Self {
        match field {
            FieldData::Tag(s) | FieldData::String(s) => Self::String(s.to_owned()),
            FieldData::Integer(i) => Self::Int(*i),
            FieldData::UInteger(u) => Self::UInt(*u),
            FieldData::Boolean(b) => Self::Bool(*b),
            FieldData::Timestamp(_) => panic!("unexpected time stamp as key value"),
            FieldData::Float(_) => panic!("unexpected float as key value"),
            FieldData::Key(_) => unreachable!("key type should never be constructed"),
        }
    }
}

/// Stores the cached column data for the field columns of a given [`LastCache`]
#[derive(Debug)]
pub(crate) struct LastCacheStore {
    /// A map of column name to a [`CacheColumn`] which holds the buffer of data for the column
    /// in the cache.
    ///
    /// An `IndexMap` is used for its performance characteristics: namely, fast iteration as well
    /// as fast lookup (see [here][perf]).
    ///
    /// [perf]: https://github.com/indexmap-rs/indexmap?tab=readme-ov-file#performance
    pub(crate) cache: IndexMap<ColumnId, CacheColumn>,
    /// A reference to the key column id lookup for the cache. This is within an `Arc` because it is
    /// shared with the parent `LastCache`.
    pub(crate) key_column_ids: Arc<IndexSet<ColumnId>>,
    /// Whether or not this store accepts new fields when they are added to the cached table
    pub(crate) value_column_ids: Option<Vec<ColumnId>>,
    /// A ring buffer holding the instants at which entries in the cache were inserted
    ///
    /// This is used to evict cache values that outlive the `ttl`
    pub(crate) instants: VecDeque<Instant>,
    /// The capacity of the internal cache buffers
    pub(crate) count: usize,
    /// Time-to-live (TTL) for values in the cache
    pub(crate) ttl: Duration,
    /// The timestamp of the last [`Row`] that was pushed into this store from the buffer.
    ///
    /// This is used to ignore rows that are received with older timestamps.
    pub(crate) last_time: Time,
}

impl LastCacheStore {
    /// Create a new [`LastCacheStore`]
    pub(crate) fn new(
        count: usize,
        ttl: Duration,
        table_def: &legacy::TableDefinition,
        key_column_ids: Arc<IndexSet<ColumnId>>,
        value_columns: &ValueColumnType,
    ) -> Self {
        let (cache, value_column_ids) = match value_columns {
            ValueColumnType::AcceptNew { .. } => {
                let cache = table_def
                    .columns
                    .iter()
                    .filter(|&(col_id, _)| !key_column_ids.contains(col_id))
                    .map(|(col_id, col_def)| (*col_id, CacheColumn::new(col_def.data_type, count)))
                    .collect();
                (cache, None)
            }
            ValueColumnType::Explicit { columns } => {
                let cache = columns
                    .iter()
                    .map(|id| {
                        table_def
                            .column_definition_by_id(id)
                            .expect("valid column id")
                    })
                    .map(|col_def| (col_def.id, CacheColumn::new(col_def.data_type, count)))
                    .collect();
                (cache, Some(columns.clone()))
            }
        };
        Self {
            cache,
            key_column_ids,
            instants: VecDeque::with_capacity(count),
            count,
            ttl,
            last_time: Time::from_timestamp_nanos(0),
            value_column_ids,
        }
    }

    /// Get the number of values in the cache that have not expired past the TTL.
    fn len(&self) -> usize {
        self.instants
            .iter()
            .filter(|i| i.elapsed() < self.ttl)
            .count()
    }

    /// Check if the cache is empty
    fn is_empty(&self) -> bool {
        self.instants.is_empty()
    }

    /// Push a [`Row`] from the buffer into this cache
    ///
    /// If new fields were added to the [`LastCacheStore`] by this push, the return will be a
    /// list of those field's name and arrow [`DataType`], and `None` otherwise.
    fn push(&mut self, row: &Row) {
        if row.time <= self.last_time.timestamp_nanos() {
            return;
        }
        let mut seen = HashSet::<ColumnId>::new();
        match self.value_column_ids {
            Some(_) => {
                for field in row.fields.iter() {
                    seen.insert(field.id);
                    if let Some(c) = self.cache.get_mut(&field.id) {
                        c.push(&field.value);
                    }
                }
            }
            None => {
                // Check the length before any rows are added to ensure that the correct amount
                // of nulls are back-filled when new fields/columns are added:
                let starting_cache_size = self.len();
                for field in row.fields.iter() {
                    seen.insert(field.id);
                    if let Some(col) = self.cache.get_mut(&field.id) {
                        // In this case, the field already has an entry in the cache, so just push:
                        col.push(&field.value);
                    } else if !self.key_column_ids.contains(&field.id) {
                        // In this case, there is not an entry for the field in the cache, so if the
                        // value is not one of the key columns, then it is a new field being added.
                        let col = self.cache.entry(field.id).or_insert_with(|| {
                            CacheColumn::new(data_type_from_buffer_field(field), self.count)
                        });
                        // Back-fill the new cache entry with nulls, then push the new value:
                        for _ in 0..starting_cache_size {
                            col.push_null();
                        }
                        col.push(&field.value);
                    }
                    // There is no else block, because the only alternative would be that this is a
                    // key column, which we ignore.
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
        self.instants.push_front(Instant::now());
        if self.instants.len() > self.count {
            self.instants.truncate(self.count);
        }
        self.last_time = Time::from_timestamp_nanos(row.time);
    }

    /// Convert the contents of this cache into a arrow [`RecordBatch`]
    ///
    /// Accepts an optional `extended` argument containing additional columns to add to the
    /// produced set of [`RecordBatch`]es. These are for the scenario where key columns are
    /// included in the outputted batches, as the [`LastCacheStore`] only holds the field columns
    /// for the cache.
    ///
    /// Accepts an `n_non_expired` argument to indicate the number of non-expired elements in the
    /// store. This is passed in vs. calling `self.len()`, since that is already invoked in the
    /// calling function, and calling it here _could_ produce a different result.
    fn to_record_batch(
        &self,
        table_def: &legacy::TableDefinition,
        schema: ArrowSchemaRef,
        extended: Option<Vec<ArrayRef>>,
        n_non_expired: usize,
    ) -> Result<RecordBatch, ArrowError> {
        let mut arrays = extended.unwrap_or_default();
        match self.value_column_ids {
            Some(_) => {
                arrays.extend(
                    self.cache
                        .iter()
                        .map(|(_, col)| col.data.as_array(n_non_expired)),
                );
            }
            None => {
                for field in schema.fields().iter() {
                    let id = table_def
                        .column_name_to_id(field.name().as_str())
                        .ok_or_else(|| {
                            ArrowError::from_external_error(Box::new(
                                Error::ColumnDoesNotExistByName {
                                    column_name: field.name().to_string(),
                                },
                            ))
                        })?;
                    if self.key_column_ids.contains(&id) {
                        continue;
                    }
                    arrays.push(self.cache.get(&id).map_or_else(
                        || new_null_array(field.data_type(), n_non_expired),
                        |c| c.data.as_array(n_non_expired),
                    ));
                }
            }
        }
        RecordBatch::try_new(schema, arrays)
    }

    /// Remove expired values from the [`LastCacheStore`]
    ///
    /// Returns whether or not the store is empty after expired entries are removed.
    fn remove_expired(&mut self) -> bool {
        // scan from right to find first non-expired entry:
        match self.instants.iter().rposition(|i| i.elapsed() < self.ttl) {
            Some(last_unexpired_position) => self.instants.truncate(last_unexpired_position + 1),
            None => self.instants.clear(),
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

/// For caches that accept new fields, this is used when new fields are encountered and the cache
/// schema needs to be updated.
fn update_last_cache_schema_for_new_fields(
    table_def: &legacy::TableDefinition,
    key_columns: Vec<ColumnId>,
) -> (ArrowSchemaRef, HashSet<ColumnId>) {
    let mut seen = HashSet::new();
    let mut schema_builder = ArrowSchemaBuilder::new();
    // Add key columns first, because of how the cache produces records, they should appear first
    // in the schema, it is important that they go in order:
    for id in &key_columns {
        let def = table_def.columns.get_by_id(id).expect("valid key column");
        seen.insert(*id);
        if let InfluxColumnType::Tag = def.data_type {
            // override tags with string type in the schema, because the KeyValue type stores
            // them as strings, and produces them as StringArray when creating RecordBatches:
            schema_builder.push(ArrowField::new(def.name.as_ref(), DataType::Utf8, false))
        } else {
            schema_builder.push(ArrowField::new(
                def.name.as_ref(),
                DataType::from(&def.data_type),
                false,
            ));
        };
    }
    // Add value columns second:
    for (id, def) in table_def
        .columns
        .iter()
        .filter(|(id, _)| !key_columns.contains(id))
    {
        seen.insert(*id);
        schema_builder.push(ArrowField::new(
            def.name.as_ref(),
            DataType::from(&def.data_type),
            true,
        ));
    }

    (Arc::new(schema_builder.finish()), seen)
}

/// A column in a [`LastCache`]
///
/// Stores its size so it can evict old data on push. Stores the time-to-live (TTL) in order
/// to remove expired data.
#[derive(Debug)]
pub(crate) struct CacheColumn {
    /// The number of entries the [`CacheColumn`] will hold before evicting old ones on push
    size: usize,
    /// The buffer containing data for the column
    data: CacheColumnData,
}

impl CacheColumn {
    /// Create a new [`CacheColumn`] for the given arrow [`DataType`] and size
    fn new(data_type: InfluxColumnType, size: usize) -> Self {
        Self {
            size,
            data: CacheColumnData::new(data_type, size),
        }
    }

    /// Push [`FieldData`] from the buffer into this column
    pub(crate) fn push(&mut self, field_data: &FieldData) {
        self.data.push_front(field_data);
        if self.data.len() > self.size {
            self.data.truncate(self.size);
        }
    }

    pub(crate) fn push_null(&mut self) {
        self.data.push_front_null();
        if self.data.len() > self.size {
            self.data.truncate(self.size);
        }
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
    Time(VecDeque<i64>),
}

impl CacheColumnData {
    /// Create a new [`CacheColumnData`]
    fn new(data_type: InfluxColumnType, size: usize) -> Self {
        match data_type {
            InfluxColumnType::Tag => Self::Tag(VecDeque::with_capacity(size)),
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
            CacheColumnData::Time(buf) => buf.len(),
        }
    }

    /// Push a new element into the [`CacheColumn`]
    fn push_front(&mut self, field_data: &FieldData) {
        match (field_data, self) {
            (FieldData::Timestamp(val), CacheColumnData::Time(buf)) => buf.push_front(*val),
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
            data => panic!("invalid field data for cache column: {data:#?}"),
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
            CacheColumnData::Time(_) => panic!("pushed null value to time column in cache"),
        }
    }

    /// Produce an arrow [`ArrayRef`] from this column for the sake of producing [`RecordBatch`]es
    ///
    /// Accepts `n_non_expired` to indicate how many of the first elements in the column buffer to
    /// take, i.e., those that have not yet expired. That value is determined externally by the
    /// [`LastCacheStore`] that tracks TTL.
    fn as_array(&self, n_non_expired: usize) -> ArrayRef {
        match self {
            CacheColumnData::I64(buf) => {
                let mut b = Int64Builder::new();
                buf.iter().take(n_non_expired).for_each(|val| match val {
                    Some(v) => b.append_value(*v),
                    None => b.append_null(),
                });
                Arc::new(b.finish())
            }
            CacheColumnData::U64(buf) => {
                let mut b = UInt64Builder::new();
                buf.iter().take(n_non_expired).for_each(|val| match val {
                    Some(v) => b.append_value(*v),
                    None => b.append_null(),
                });
                Arc::new(b.finish())
            }
            CacheColumnData::F64(buf) => {
                let mut b = Float64Builder::new();
                buf.iter().take(n_non_expired).for_each(|val| match val {
                    Some(v) => b.append_value(*v),
                    None => b.append_null(),
                });
                Arc::new(b.finish())
            }
            CacheColumnData::String(buf) => {
                let mut b = StringBuilder::new();
                buf.iter().take(n_non_expired).for_each(|val| match val {
                    Some(v) => b.append_value(v),
                    None => b.append_null(),
                });
                Arc::new(b.finish())
            }
            CacheColumnData::Bool(buf) => {
                let mut b = BooleanBuilder::new();
                buf.iter().take(n_non_expired).for_each(|val| match val {
                    Some(v) => b.append_value(*v),
                    None => b.append_null(),
                });
                Arc::new(b.finish())
            }
            CacheColumnData::Tag(buf) => {
                let mut b: GenericByteDictionaryBuilder<Int32Type, GenericStringType<i32>> =
                    StringDictionaryBuilder::new();
                buf.iter().take(n_non_expired).for_each(|val| match val {
                    Some(v) => b.append_value(v),
                    None => b.append_null(),
                });
                Arc::new(b.finish())
            }
            CacheColumnData::Time(buf) => {
                let mut b = TimestampNanosecondBuilder::new();
                buf.iter()
                    .take(n_non_expired)
                    .for_each(|val| b.append_value(*val));
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
            CacheColumnData::Time(buf) => buf.truncate(len),
        }
    }
}

fn data_type_from_buffer_field(field: &Field) -> InfluxColumnType {
    match field.value {
        FieldData::Timestamp(_) => InfluxColumnType::Timestamp,
        FieldData::Tag(_) => InfluxColumnType::Tag,
        FieldData::String(_) => InfluxColumnType::Field(InfluxFieldType::String),
        FieldData::Integer(_) => InfluxColumnType::Field(InfluxFieldType::Integer),
        FieldData::UInteger(_) => InfluxColumnType::Field(InfluxFieldType::UInteger),
        FieldData::Float(_) => InfluxColumnType::Field(InfluxFieldType::Float),
        FieldData::Boolean(_) => InfluxColumnType::Field(InfluxFieldType::Boolean),
        FieldData::Key(_) => unreachable!("key type should never be constructed"),
    }
}
