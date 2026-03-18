//! The in memory buffer of a table that can be quickly added to and queried

use arrow::array::{
    Array, ArrayRef, BooleanBuilder, Float64Builder, Int64Builder, StringBuilder,
    StringDictionaryBuilder, TimestampNanosecondBuilder, UInt64Builder,
};
use arrow::datatypes::Int32Type;
use arrow::record_batch::RecordBatch;
use data_types::TimestampMinMax;
use hashbrown::{HashMap, HashSet};
use influxdb3_catalog::catalog::{TableDefinition, legacy};
use influxdb3_id::ColumnId;
use influxdb3_wal::{FieldData, Row};
use schema::{InfluxColumnType, InfluxFieldType, Schema, SchemaBuilder};
use std::collections::BTreeMap;
use std::collections::btree_map::Entry;
use std::mem::size_of;
use std::sync::Arc;
use thiserror::Error;

use crate::ChunkFilter;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Field not found in table buffer: {0}")]
    FieldNotFound(String),

    #[error("Error creating record batch: {0}")]
    RecordBatchError(#[from] arrow::error::ArrowError),
}

pub(crate) type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Default)]
pub struct TableBuffer {
    chunk_time_to_chunks: BTreeMap<i64, Vec<MutableTableChunk>>,
    snapshotting_chunks: Vec<SnapshotChunk>,
}

impl TableBuffer {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn buffer_chunk(&mut self, chunk_time: i64, rows: &[Row]) {
        let chunks = self.chunk_time_to_chunks.entry(chunk_time).or_default();

        let mut incoming_per_column = HashMap::new();
        for r in rows {
            for f in &r.fields {
                match &f.value {
                    FieldData::String(s) | FieldData::Tag(s) => {
                        *incoming_per_column.entry(f.id).or_default() += s.len();
                    }
                    _ => {}
                }
            }
        }

        let needs_new_chunk = chunks.is_empty()
            || chunks
                .last()
                .is_some_and(|c| c.would_exceed_limit_with(&incoming_per_column));

        if needs_new_chunk {
            chunks.push(MutableTableChunk::new());
        }

        let buffer_chunk = chunks.last_mut().unwrap();
        buffer_chunk.add_rows(rows);
    }

    /// Produce a partitioned set of record batches along with their min/max timestamp
    ///
    /// The partitions are stored and returned in a `HashMap`, keyed on the generation time.
    ///
    /// This uses the provided `filter` to prune out chunks from the buffer that do not fall in
    /// the filter's time boundaries. If the filter contains literal guarantees on tag columns
    /// that are in the buffer index, this will also leverage those to prune rows in the resulting
    /// chunks that do not satisfy the guarantees specified in the filter.
    pub fn partitioned_record_batches(
        &self,
        table_def: Arc<TableDefinition>,
        filter: &ChunkFilter<'_>,
    ) -> Result<HashMap<i64, (TimestampMinMax, Vec<RecordBatch>)>> {
        let mut batches = HashMap::new();
        let schema = table_def.schema.as_arrow();
        for sc in self.snapshotting_chunks.iter().filter(|sc| {
            filter.test_time_stamp_min_max(sc.timestamp_min_max.min, sc.timestamp_min_max.max)
        }) {
            let cols: std::result::Result<Vec<_>, _> = schema
                .fields()
                .iter()
                .map(|f| {
                    let col = sc
                        .record_batch
                        .column_by_name(f.name())
                        .ok_or(Error::FieldNotFound(f.name().to_string()));
                    col.cloned()
                })
                .collect();
            let cols = cols?;
            let rb = RecordBatch::try_new(Arc::clone(&schema), cols)?;
            let (ts, v) = batches
                .entry(sc.chunk_time)
                .or_insert_with(|| (sc.timestamp_min_max, Vec::new()));
            *ts = ts.union(&sc.timestamp_min_max);
            v.push(rb);
        }
        for (t, chunks) in self.chunk_time_to_chunks.iter() {
            for c in chunks
                .iter()
                .filter(|c| filter.test_time_stamp_min_max(c.timestamp_min, c.timestamp_max))
            {
                let ts_min_max = TimestampMinMax::new(c.timestamp_min, c.timestamp_max);
                let (ts, v) = batches
                    .entry(*t)
                    .or_insert_with(|| (ts_min_max, Vec::new()));
                *ts = ts.union(&ts_min_max);
                v.push(c.record_batch(Arc::clone(&table_def))?);
            }
        }
        Ok(batches)
    }

    pub fn timestamp_min_max(&self) -> TimestampMinMax {
        let (min, max) = if self.chunk_time_to_chunks.is_empty() {
            (0, 0)
        } else {
            self.chunk_time_to_chunks
                .values()
                .flatten()
                .map(|c| (c.timestamp_min, c.timestamp_max))
                .fold((i64::MAX, i64::MIN), |(a_min, b_min), (a_max, b_max)| {
                    (a_min.min(b_min), a_max.max(b_max))
                })
        };
        let mut timestamp_min_max = TimestampMinMax::new(min, max);

        for sc in &self.snapshotting_chunks {
            timestamp_min_max = timestamp_min_max.union(&sc.timestamp_min_max);
        }

        timestamp_min_max
    }

    /// Returns an estimate of the size of this table buffer based on the data and index sizes.
    #[allow(dead_code)]
    pub fn computed_size(&self) -> usize {
        let mut size = size_of::<Self>();

        for chunks in self.chunk_time_to_chunks.values() {
            for c in chunks {
                for builder in c.data.values() {
                    size += size_of::<ColumnId>() + size_of::<String>() + builder.size();
                }
            }
        }

        size
    }

    pub fn snapshot(
        &mut self,
        table_def: Arc<TableDefinition>,
        older_than_chunk_time: i64,
    ) -> Vec<SnapshotChunk> {
        let keys_to_remove = self
            .chunk_time_to_chunks
            .keys()
            .filter(|k| **k < older_than_chunk_time)
            .copied()
            .collect::<Vec<_>>();

        let mut snapshot_chunks = Vec::new();
        for chunk_time in keys_to_remove {
            let chunks = self.chunk_time_to_chunks.remove(&chunk_time).unwrap();
            for chunk in chunks {
                let timestamp_min_max = chunk.timestamp_min_max();
                let (schema, record_batch) = chunk.into_schema_record_batch(Arc::clone(&table_def));

                snapshot_chunks.push(SnapshotChunk {
                    chunk_time,
                    timestamp_min_max,
                    record_batch,
                    schema,
                });
            }
        }
        self.snapshotting_chunks = snapshot_chunks;

        self.snapshotting_chunks.clone()
    }

    pub fn clear_snapshots(&mut self) {
        self.snapshotting_chunks.clear();
    }
}

#[derive(Debug, Clone)]
pub struct SnapshotChunk {
    pub(crate) chunk_time: i64,
    pub(crate) timestamp_min_max: TimestampMinMax,
    pub(crate) record_batch: RecordBatch,
    pub(crate) schema: Schema,
}

// Debug implementation for TableBuffer
impl std::fmt::Debug for TableBuffer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let (min_time, max_time, row_count) = self
            .chunk_time_to_chunks
            .values()
            .flatten()
            .map(|c| (c.timestamp_min, c.timestamp_max, c.row_count))
            .fold(
                (i64::MAX, i64::MIN, 0),
                |(a_min, a_max, a_count), (b_min, b_max, b_count)| {
                    (a_min.min(b_min), a_max.max(b_max), a_count + b_count)
                },
            );
        let chunk_count: usize = self.chunk_time_to_chunks.values().map(|v| v.len()).sum();
        f.debug_struct("TableBuffer")
            .field("chunk_count", &chunk_count)
            .field("timestamp_min", &min_time)
            .field("timestamp_max", &max_time)
            .field("row_count", &row_count)
            .finish()
    }
}

struct MutableTableChunk {
    timestamp_min: i64,
    timestamp_max: i64,
    data: BTreeMap<ColumnId, Builder>,
    row_count: usize,
    string_bytes_per_column: HashMap<ColumnId, usize>,
}

// Test infrastructure for configurable string size limit - thread-local for test isolation.
#[cfg(test)]
thread_local! {
    static TEST_VAR_COL_MAX_BYTES: std::cell::Cell<usize> = const {
        std::cell::Cell::new(influxdb3_types::arrow_limits::ARROW_VAR_COL_MAX_BYTES)
    };
}

/// Returns the variable-column byte capacity limit.
fn var_col_max_bytes() -> usize {
    #[cfg(test)]
    {
        TEST_VAR_COL_MAX_BYTES.with(|c| c.get())
    }
    #[cfg(not(test))]
    {
        influxdb3_types::arrow_limits::ARROW_VAR_COL_MAX_BYTES
    }
}

#[cfg(test)]
#[derive(Debug)]
struct VarColMaxGuard(usize);

#[cfg(test)]
impl VarColMaxGuard {
    fn new(cap: usize) -> Self {
        let prev = TEST_VAR_COL_MAX_BYTES.with(|c| {
            let prev = c.get();
            c.set(cap);
            prev
        });
        Self(prev)
    }
}

#[cfg(test)]
impl Drop for VarColMaxGuard {
    fn drop(&mut self) {
        TEST_VAR_COL_MAX_BYTES.with(|c| c.set(self.0));
    }
}

impl MutableTableChunk {
    fn new() -> Self {
        Self {
            timestamp_min: i64::MAX,
            timestamp_max: i64::MIN,
            data: Default::default(),
            row_count: 0,
            string_bytes_per_column: HashMap::new(),
        }
    }

    fn would_exceed_limit_with(&self, incoming_per_column: &HashMap<ColumnId, usize>) -> bool {
        let limit = var_col_max_bytes();
        incoming_per_column.iter().any(|(col_id, additional)| {
            let existing = self
                .string_bytes_per_column
                .get(col_id)
                .copied()
                .unwrap_or(0);
            existing.saturating_add(*additional) > limit
        })
    }
}

impl MutableTableChunk {
    fn add_rows(&mut self, rows: &[Row]) {
        let new_row_count = rows.len();
        // Capacity needed for builders created in this call.
        // After this batch of rows, each builder will have exactly (self.row_count + new_row_count)
        // entries for values and nulls.
        // Using exact capacity avoids the default 1024-element allocation which may cause excessive
        // memory usage when there are many chunks with few rows each in sparse time-series data.
        let builder_capacity = self.row_count + new_row_count;

        for (row_index, r) in rows.iter().enumerate() {
            let mut value_added = HashSet::with_capacity(r.fields.len());

            for f in &r.fields {
                value_added.insert(f.id);

                match &f.value {
                    FieldData::Timestamp(v) => {
                        self.timestamp_min = self.timestamp_min.min(*v);
                        self.timestamp_max = self.timestamp_max.max(*v);

                        let b = self.data.entry(f.id).or_insert_with(|| {
                            let mut time_builder =
                                TimestampNanosecondBuilder::with_capacity(builder_capacity);
                            // append nulls for all previous rows
                            time_builder.append_nulls(row_index + self.row_count);
                            Builder::Time(time_builder)
                        });
                        if let Builder::Time(b) = b {
                            b.append_value(*v);
                        } else {
                            panic!("unexpected field type");
                        }
                    }
                    FieldData::Tag(v) => {
                        *self.string_bytes_per_column.entry(f.id).or_default() += v.len();

                        if let Entry::Vacant(e) = self.data.entry(f.id) {
                            let mut tag_builder = StringDictionaryBuilder::with_capacity(
                                builder_capacity,
                                builder_capacity.min(1024),
                                (builder_capacity * 64).min(1024),
                            );
                            // append nulls for all previous rows
                            tag_builder.append_nulls(row_index + self.row_count);
                            e.insert(Builder::Tag(tag_builder));
                        }
                        let b = self.data.get_mut(&f.id).expect("tag builder should exist");
                        if let Builder::Tag(b) = b {
                            b.append(v)
                                .expect("shouldn't be able to overflow 32 bit dictionary");
                        } else {
                            panic!("unexpected field type");
                        }
                    }
                    FieldData::String(v) => {
                        *self.string_bytes_per_column.entry(f.id).or_default() += v.len();

                        let b = self.data.entry(f.id).or_insert_with(|| {
                            let mut string_builder = StringBuilder::with_capacity(
                                builder_capacity,
                                (builder_capacity * 64).min(1024),
                            );
                            // append nulls for all previous rows
                            string_builder.append_nulls(row_index + self.row_count);
                            Builder::String(string_builder)
                        });
                        if let Builder::String(b) = b {
                            b.append_value(v);
                        } else {
                            panic!("unexpected field type");
                        }
                    }
                    FieldData::Integer(v) => {
                        let b = self.data.entry(f.id).or_insert_with(|| {
                            let mut int_builder = Int64Builder::with_capacity(builder_capacity);
                            // append nulls for all previous rows
                            int_builder.append_nulls(row_index + self.row_count);
                            Builder::I64(int_builder)
                        });
                        if let Builder::I64(b) = b {
                            b.append_value(*v);
                        } else {
                            panic!("unexpected field type");
                        }
                    }
                    FieldData::UInteger(v) => {
                        let b = self.data.entry(f.id).or_insert_with(|| {
                            let mut uint_builder = UInt64Builder::with_capacity(builder_capacity);
                            // append nulls for all previous rows
                            uint_builder.append_nulls(row_index + self.row_count);
                            Builder::U64(uint_builder)
                        });
                        if let Builder::U64(b) = b {
                            b.append_value(*v);
                        } else {
                            panic!("unexpected field type");
                        }
                    }
                    FieldData::Float(v) => {
                        let b = self.data.entry(f.id).or_insert_with(|| {
                            let mut float_builder = Float64Builder::with_capacity(builder_capacity);
                            // append nulls for all previous rows
                            float_builder.append_nulls(row_index + self.row_count);
                            Builder::F64(float_builder)
                        });
                        if let Builder::F64(b) = b {
                            b.append_value(*v);
                        } else {
                            panic!("unexpected field type");
                        }
                    }
                    FieldData::Boolean(v) => {
                        let b = self.data.entry(f.id).or_insert_with(|| {
                            let mut bool_builder = BooleanBuilder::with_capacity(builder_capacity);
                            // append nulls for all previous rows
                            bool_builder.append_nulls(row_index + self.row_count);
                            Builder::Bool(bool_builder)
                        });
                        if let Builder::Bool(b) = b {
                            b.append_value(*v);
                        } else {
                            panic!("unexpected field type");
                        }
                    }
                    FieldData::Key(_) => unreachable!("key type should never be constructed"),
                }
            }

            // add nulls for any columns not present
            for (column_id, builder) in &mut self.data {
                if !value_added.contains(column_id) {
                    builder.append_null();
                }
            }
        }

        self.row_count += new_row_count;
    }

    fn timestamp_min_max(&self) -> TimestampMinMax {
        TimestampMinMax::new(self.timestamp_min, self.timestamp_max)
    }

    fn record_batch(&self, table_def: Arc<TableDefinition>) -> Result<RecordBatch> {
        let schema = table_def.schema.as_arrow();
        let table_def = legacy::TableDefinition::new(table_def);

        let mut cols = Vec::with_capacity(schema.fields().len());

        for f in schema.fields() {
            let column_def = table_def
                .column_definition(f.name())
                .expect("a valid column name");
            let b = match self.data.get(&column_def.id) {
                Some(b) => b.as_arrow(),
                None => array_ref_nulls_for_type(column_def.data_type, self.row_count),
            };

            cols.push(b);
        }

        Ok(RecordBatch::try_new(schema, cols)?)
    }

    fn into_schema_record_batch(self, table_def: Arc<TableDefinition>) -> (Schema, RecordBatch) {
        let table_def = legacy::TableDefinition::new(table_def);
        let mut cols = Vec::with_capacity(self.data.len());
        let mut schema_builder = SchemaBuilder::new();
        let mut cols_in_batch = HashSet::new();
        for (col_id, builder) in self.data.into_iter() {
            cols_in_batch.insert(col_id);
            let (col_type, col) = builder.into_influxcol_and_arrow();
            schema_builder.influx_column(
                table_def
                    .column_id_to_name(&col_id)
                    .expect("valid column id")
                    .as_ref(),
                col_type,
            );
            cols.push(col);
            schema_builder.with_series_key(&table_def.inner().series_key_names);
        }

        // ensure that every field column is present in the batch
        for (col_id, col_def) in table_def.columns.iter() {
            if !cols_in_batch.contains(col_id) {
                schema_builder.influx_column(col_def.name.as_ref(), col_def.data_type);
                let col = array_ref_nulls_for_type(col_def.data_type, self.row_count);

                cols.push(col);
            }
        }
        let schema = schema_builder
            .build()
            .expect("should always be able to build schema");
        let arrow_schema = schema.as_arrow();

        (
            schema,
            RecordBatch::try_new(arrow_schema, cols)
                .expect("should always be able to build record batch"),
        )
    }
}

fn array_ref_nulls_for_type(data_type: InfluxColumnType, len: usize) -> ArrayRef {
    match data_type {
        InfluxColumnType::Field(InfluxFieldType::Boolean) => {
            let mut builder = BooleanBuilder::new();
            builder.append_nulls(len);
            Arc::new(builder.finish())
        }
        InfluxColumnType::Timestamp => {
            let mut builder = TimestampNanosecondBuilder::new();
            builder.append_nulls(len);
            Arc::new(builder.finish())
        }
        InfluxColumnType::Tag => {
            let mut builder: StringDictionaryBuilder<Int32Type> = StringDictionaryBuilder::new();
            builder.append_nulls(len);
            Arc::new(builder.finish())
        }
        InfluxColumnType::Field(InfluxFieldType::Integer) => {
            let mut builder = Int64Builder::new();
            builder.append_nulls(len);
            Arc::new(builder.finish())
        }
        InfluxColumnType::Field(InfluxFieldType::Float) => {
            let mut builder = Float64Builder::new();
            builder.append_nulls(len);
            Arc::new(builder.finish())
        }
        InfluxColumnType::Field(InfluxFieldType::String) => {
            let mut builder = StringBuilder::new();
            builder.append_nulls(len);
            Arc::new(builder.finish())
        }
        InfluxColumnType::Field(InfluxFieldType::UInteger) => {
            let mut builder = UInt64Builder::new();
            builder.append_nulls(len);
            Arc::new(builder.finish())
        }
    }
}

// Debug implementation for TableBuffer
impl std::fmt::Debug for MutableTableChunk {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MutableTableChunk")
            .field("timestamp_min", &self.timestamp_min)
            .field("timestamp_max", &self.timestamp_max)
            .field("row_count", &self.row_count)
            .finish()
    }
}

pub(super) enum Builder {
    Bool(BooleanBuilder),
    I64(Int64Builder),
    F64(Float64Builder),
    U64(UInt64Builder),
    String(StringBuilder),
    Tag(StringDictionaryBuilder<Int32Type>),
    Time(TimestampNanosecondBuilder),
}

impl Builder {
    fn as_arrow(&self) -> ArrayRef {
        match self {
            Self::Bool(b) => Arc::new(b.finish_cloned()),
            Self::I64(b) => Arc::new(b.finish_cloned()),
            Self::F64(b) => Arc::new(b.finish_cloned()),
            Self::U64(b) => Arc::new(b.finish_cloned()),
            Self::String(b) => Arc::new(b.finish_cloned()),
            Self::Tag(b) => Arc::new(b.finish_cloned()),
            Self::Time(b) => Arc::new(b.finish_cloned()),
        }
    }

    fn append_null(&mut self) {
        match self {
            Builder::Bool(b) => b.append_null(),
            Builder::I64(b) => b.append_null(),
            Builder::F64(b) => b.append_null(),
            Builder::U64(b) => b.append_null(),
            Builder::String(b) => b.append_null(),
            Builder::Tag(b) => b.append_null(),
            Builder::Time(b) => b.append_null(),
        }
    }

    fn into_influxcol_and_arrow(self) -> (InfluxColumnType, ArrayRef) {
        match self {
            Self::Bool(mut b) => (
                InfluxColumnType::Field(InfluxFieldType::Boolean),
                Arc::new(b.finish()),
            ),
            Self::I64(mut b) => (
                InfluxColumnType::Field(InfluxFieldType::Integer),
                Arc::new(b.finish()),
            ),
            Self::F64(mut b) => (
                InfluxColumnType::Field(InfluxFieldType::Float),
                Arc::new(b.finish()),
            ),
            Self::U64(mut b) => (
                InfluxColumnType::Field(InfluxFieldType::UInteger),
                Arc::new(b.finish()),
            ),
            Self::String(mut b) => (
                InfluxColumnType::Field(InfluxFieldType::String),
                Arc::new(b.finish()),
            ),
            Self::Tag(mut b) => (InfluxColumnType::Tag, Arc::new(b.finish())),
            Self::Time(mut b) => (InfluxColumnType::Timestamp, Arc::new(b.finish())),
        }
    }

    fn size(&self) -> usize {
        let data_size = match self {
            Self::Bool(b) => b.capacity() + b.validity_slice().map(|s| s.len()).unwrap_or(0),
            Self::I64(b) => {
                size_of::<i64>() * b.capacity() + b.validity_slice().map(|s| s.len()).unwrap_or(0)
            }
            Self::F64(b) => {
                size_of::<f64>() * b.capacity() + b.validity_slice().map(|s| s.len()).unwrap_or(0)
            }
            Self::U64(b) => {
                size_of::<u64>() * b.capacity() + b.validity_slice().map(|s| s.len()).unwrap_or(0)
            }
            Self::String(b) => {
                b.values_slice().len()
                    + b.offsets_slice().len()
                    + b.validity_slice().map(|s| s.len()).unwrap_or(0)
            }
            Self::Tag(b) => {
                let b = b.finish_cloned();
                b.keys().len() * size_of::<i32>() + b.values().get_array_memory_size()
            }
            Self::Time(b) => size_of::<i64>() * b.capacity(),
        };
        size_of::<Self>() + data_size
    }
}

#[cfg(test)]
mod tests;
