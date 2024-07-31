//! The in memory buffer of a table that can be quickly added to and queried

use arrow::array::{
    Array, ArrayBuilder, ArrayRef, BooleanBuilder, Float64Builder, GenericByteDictionaryBuilder,
    Int64Builder, StringArray, StringBuilder, StringDictionaryBuilder, TimestampNanosecondBuilder,
    UInt64Builder,
};
use arrow::datatypes::{GenericStringType, Int32Type, SchemaRef};
use arrow::record_batch::RecordBatch;
use data_types::TimestampMinMax;
use datafusion::logical_expr::{BinaryExpr, Expr};
use hashbrown::HashMap;
use influxdb3_wal::{FieldData, Row};
use observability_deps::tracing::{debug, error};
use schema::sort::SortKey;
use schema::{InfluxColumnType, InfluxFieldType, Schema, SchemaBuilder};
use std::collections::{BTreeMap, HashSet};
use std::mem::size_of;
use std::sync::Arc;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Field not found in table buffer: {0}")]
    FieldNotFound(String),

    #[error("Error creating record batch: {0}")]
    RecordBatchError(#[from] arrow::error::ArrowError),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

pub struct TableBuffer {
    chunk_time_to_chunks: BTreeMap<i64, MutableTableChunk>,
    snapshotting_chunks: Vec<SnapshotChunk>,
    index: BufferIndex,
    pub(crate) sort_key: SortKey,
}

impl TableBuffer {
    pub fn new(index_columns: &[&str], sort_key: SortKey) -> Self {
        Self {
            chunk_time_to_chunks: BTreeMap::default(),
            snapshotting_chunks: vec![],
            index: BufferIndex::new(index_columns),
            sort_key,
        }
    }

    pub fn buffer_chunk(&mut self, chunk_time: i64, rows: Vec<Row>) {
        let buffer_chunk = self
            .chunk_time_to_chunks
            .entry(chunk_time)
            .or_insert_with(|| MutableTableChunk {
                timestamp_min: i64::MAX,
                timestamp_max: i64::MIN,
                data: Default::default(),
                row_count: 0,
                index: self.index.clone(),
            });

        buffer_chunk.add_rows(rows);
    }

    pub fn record_batches(&self, schema: SchemaRef, filter: &[Expr]) -> Result<Vec<RecordBatch>> {
        println!(
            "chunk time to chunks: {:?}",
            self.chunk_time_to_chunks.keys().collect::<Vec<_>>()
        );
        let mut batches =
            Vec::with_capacity(self.snapshotting_chunks.len() + self.chunk_time_to_chunks.len());

        for sc in &self.snapshotting_chunks {
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
            let rb = RecordBatch::try_new(schema.clone(), cols)?;

            batches.push(rb);
        }

        for c in self.chunk_time_to_chunks.values() {
            batches.push(c.record_batch(schema.clone(), filter)?)
        }

        Ok(batches)
    }

    pub fn timestamp_min_max(&self) -> TimestampMinMax {
        let (min, max) = self
            .chunk_time_to_chunks
            .values()
            .map(|c| (c.timestamp_min, c.timestamp_max))
            .fold((i64::MAX, i64::MIN), |(a_min, b_min), (a_max, b_max)| {
                (a_min.min(b_min), a_max.max(b_max))
            });
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

        for c in self.chunk_time_to_chunks.values() {
            for (k, v) in &c.data {
                size += k.len() + size_of::<String>() + v.size();
            }

            size += c.index.size();
        }

        size
    }

    pub fn snapshot(&mut self, older_than_chunk_time: i64) -> Vec<SnapshotChunk> {
        let keys_to_remove = self
            .chunk_time_to_chunks
            .keys()
            .filter(|k| **k < older_than_chunk_time)
            .copied()
            .collect::<Vec<_>>();
        self.snapshotting_chunks = keys_to_remove
            .into_iter()
            .map(|chunk_time| {
                let chunk = self.chunk_time_to_chunks.remove(&chunk_time).unwrap();
                let timestamp_min_max = chunk.timestamp_min_max();
                let (schema, record_batch) = chunk.into_schema_record_batch();

                SnapshotChunk {
                    chunk_time,
                    timestamp_min_max,
                    record_batch,
                    schema,
                }
            })
            .collect::<Vec<_>>();

        self.snapshotting_chunks.clone()
    }

    pub fn clear_snapshots(&mut self) {
        self.snapshotting_chunks.clear();
    }
}

#[derive(Debug, Clone)]
pub(crate) struct SnapshotChunk {
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
            .map(|c| (c.timestamp_min, c.timestamp_max, c.row_count))
            .fold(
                (i64::MAX, i64::MIN, 0),
                |(a_min, a_max, a_count), (b_min, b_max, b_count)| {
                    (a_min.min(b_min), a_max.max(b_max), a_count + b_count)
                },
            );
        f.debug_struct("TableBuffer")
            .field("chunk_count", &self.chunk_time_to_chunks.len())
            .field("timestamp_min", &min_time)
            .field("timestamp_max", &max_time)
            .field("row_count", &row_count)
            .finish()
    }
}

struct MutableTableChunk {
    timestamp_min: i64,
    timestamp_max: i64,
    data: BTreeMap<Arc<str>, Builder>,
    row_count: usize,
    index: BufferIndex,
}

impl MutableTableChunk {
    fn add_rows(&mut self, rows: Vec<Row>) {
        let new_row_count = rows.len();

        for (row_index, r) in rows.into_iter().enumerate() {
            let mut value_added = HashSet::with_capacity(r.fields.len());

            for f in r.fields {
                value_added.insert(f.name.clone());

                match f.value {
                    FieldData::Timestamp(v) => {
                        self.timestamp_min = self.timestamp_min.min(v);
                        self.timestamp_max = self.timestamp_max.max(v);

                        let b = self.data.entry(f.name).or_insert_with(|| {
                            debug!("Creating new timestamp builder");
                            let mut time_builder = TimestampNanosecondBuilder::new();
                            // append nulls for all previous rows
                            for _ in 0..(row_index + self.row_count) {
                                debug!("Appending null for timestamp");
                                time_builder.append_null();
                            }
                            Builder::Time(time_builder)
                        });
                        if let Builder::Time(b) = b {
                            b.append_value(v);
                        } else {
                            panic!("unexpected field type");
                        }
                    }
                    FieldData::Tag(v) => {
                        if !self.data.contains_key(&f.name) {
                            let mut tag_builder = StringDictionaryBuilder::new();
                            // append nulls for all previous rows
                            for _ in 0..(row_index + self.row_count) {
                                tag_builder.append_null();
                            }
                            self.data.insert(f.name.clone(), Builder::Tag(tag_builder));
                        }
                        let b = self
                            .data
                            .get_mut(&f.name)
                            .expect("tag builder should exist");
                        if let Builder::Tag(b) = b {
                            self.index.add_row_if_indexed_column(b.len(), &f.name, &v);
                            b.append(v)
                                .expect("shouldn't be able to overflow 32 bit dictionary");
                        } else {
                            panic!("unexpected field type");
                        }
                    }
                    FieldData::Key(v) => {
                        if !self.data.contains_key(&f.name) {
                            let key_builder = StringDictionaryBuilder::new();
                            if self.row_count > 0 {
                                panic!("series key columns must be passed in the very first write for a table");
                            }
                            self.data.insert(f.name.clone(), Builder::Key(key_builder));
                        }
                        let b = self
                            .data
                            .get_mut(&f.name)
                            .expect("key builder should exist");
                        let Builder::Key(b) = b else {
                            panic!("unexpected field type");
                        };
                        self.index.add_row_if_indexed_column(b.len(), &f.name, &v);
                        b.append_value(v);
                    }
                    FieldData::String(v) => {
                        let b = self.data.entry(f.name).or_insert_with(|| {
                            let mut string_builder = StringBuilder::new();
                            // append nulls for all previous rows
                            for _ in 0..(row_index + self.row_count) {
                                string_builder.append_null();
                            }
                            Builder::String(string_builder)
                        });
                        if let Builder::String(b) = b {
                            b.append_value(v);
                        } else {
                            panic!("unexpected field type");
                        }
                    }
                    FieldData::Integer(v) => {
                        let b = self.data.entry(f.name).or_insert_with(|| {
                            let mut int_builder = Int64Builder::new();
                            // append nulls for all previous rows
                            for _ in 0..(row_index + self.row_count) {
                                int_builder.append_null();
                            }
                            Builder::I64(int_builder)
                        });
                        if let Builder::I64(b) = b {
                            b.append_value(v);
                        } else {
                            panic!("unexpected field type");
                        }
                    }
                    FieldData::UInteger(v) => {
                        let b = self.data.entry(f.name).or_insert_with(|| {
                            let mut uint_builder = UInt64Builder::new();
                            // append nulls for all previous rows
                            for _ in 0..(row_index + self.row_count) {
                                uint_builder.append_null();
                            }
                            Builder::U64(uint_builder)
                        });
                        if let Builder::U64(b) = b {
                            b.append_value(v);
                        } else {
                            panic!("unexpected field type");
                        }
                    }
                    FieldData::Float(v) => {
                        let b = self.data.entry(f.name).or_insert_with(|| {
                            let mut float_builder = Float64Builder::new();
                            // append nulls for all previous rows
                            for _ in 0..(row_index + self.row_count) {
                                float_builder.append_null();
                            }
                            Builder::F64(float_builder)
                        });
                        if let Builder::F64(b) = b {
                            b.append_value(v);
                        } else {
                            panic!("unexpected field type");
                        }
                    }
                    FieldData::Boolean(v) => {
                        let b = self.data.entry(f.name).or_insert_with(|| {
                            let mut bool_builder = BooleanBuilder::new();
                            // append nulls for all previous rows
                            for _ in 0..(row_index + self.row_count) {
                                bool_builder.append_null();
                            }
                            Builder::Bool(bool_builder)
                        });
                        if let Builder::Bool(b) = b {
                            b.append_value(v);
                        } else {
                            panic!("unexpected field type");
                        }
                    }
                }
            }

            // add nulls for any columns not present
            for (name, builder) in &mut self.data {
                if !value_added.contains(name) {
                    debug!("Adding null for column {}", name);
                    match builder {
                        Builder::Bool(b) => b.append_null(),
                        Builder::F64(b) => b.append_null(),
                        Builder::I64(b) => b.append_null(),
                        Builder::U64(b) => b.append_null(),
                        Builder::String(b) => b.append_null(),
                        Builder::Tag(b) => b.append_null(),
                        Builder::Key(b) => b.append_null(),
                        Builder::Time(b) => b.append_null(),
                    }
                }
            }
        }

        self.row_count += new_row_count;
    }

    fn timestamp_min_max(&self) -> TimestampMinMax {
        TimestampMinMax::new(self.timestamp_min, self.timestamp_max)
    }

    fn record_batch(&self, schema: SchemaRef, filter: &[Expr]) -> Result<RecordBatch> {
        let row_ids = self.index.get_rows_from_index_for_filter(filter);

        let mut cols = Vec::with_capacity(schema.fields().len());

        for f in schema.fields() {
            match row_ids {
                Some(row_ids) => {
                    let b = self
                        .data
                        .get(f.name().as_str())
                        .ok_or_else(|| Error::FieldNotFound(f.name().to_string()))?
                        .get_rows(row_ids);
                    cols.push(b);
                }
                None => {
                    let b = self
                        .data
                        .get(f.name().as_str())
                        .ok_or_else(|| Error::FieldNotFound(f.name().to_string()))?
                        .as_arrow();
                    cols.push(b);
                }
            }
        }

        Ok(RecordBatch::try_new(schema, cols)?)
    }

    fn into_schema_record_batch(self) -> (Schema, RecordBatch) {
        let mut cols = Vec::with_capacity(self.data.len());
        let mut schema_builder = SchemaBuilder::new();
        for (col_name, builder) in self.data.into_iter() {
            let (col_type, col) = builder.into_influxcol_and_arrow();
            schema_builder.influx_column(col_name.as_ref(), col_type);
            cols.push(col);
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

#[derive(Debug, Default, Clone)]
struct BufferIndex {
    // column name -> string value -> row indexes
    columns: HashMap<Arc<str>, HashMap<String, Vec<usize>>>,
}

impl BufferIndex {
    fn new(column_names: &[&str]) -> Self {
        let mut columns = HashMap::new();

        for c in column_names {
            columns.insert(c.to_string().into(), HashMap::new());
        }

        Self { columns }
    }

    fn add_row_if_indexed_column(&mut self, row_index: usize, column_name: &str, value: &str) {
        if let Some(column) = self.columns.get_mut(column_name) {
            column
                .entry_ref(value)
                .and_modify(|c| c.push(row_index))
                .or_insert(vec![row_index]);
        }
    }

    fn get_rows_from_index_for_filter(&self, filter: &[Expr]) -> Option<&Vec<usize>> {
        for expr in filter {
            if let Expr::BinaryExpr(BinaryExpr { left, op, right }) = expr {
                if *op == datafusion::logical_expr::Operator::Eq {
                    if let Expr::Column(c) = left.as_ref() {
                        if let Expr::Literal(datafusion::scalar::ScalarValue::Utf8(Some(v))) =
                            right.as_ref()
                        {
                            return self
                                .columns
                                .get(c.name.as_str())
                                .and_then(|m| m.get(v.as_str()));
                        }
                    }
                }
            }
        }

        None
    }

    #[allow(dead_code)]
    fn size(&self) -> usize {
        let mut size = size_of::<Self>();
        for (k, v) in &self.columns {
            size += k.len() + size_of::<String>() + size_of::<HashMap<String, Vec<usize>>>();
            for (k, v) in v {
                size += k.len() + size_of::<String>() + size_of::<Vec<usize>>();
                size += v.len() * size_of::<usize>();
            }
        }
        size
    }
}

pub enum Builder {
    Bool(BooleanBuilder),
    I64(Int64Builder),
    F64(Float64Builder),
    U64(UInt64Builder),
    String(StringBuilder),
    Tag(StringDictionaryBuilder<Int32Type>),
    // For now we use a string dict to be consistent with tags, but in future
    // keys, like fields may support different data types.
    Key(StringDictionaryBuilder<Int32Type>),
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
            Self::Key(b) => Arc::new(b.finish_cloned()),
            Self::Time(b) => Arc::new(b.finish_cloned()),
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
            Self::Key(mut b) => (InfluxColumnType::Tag, Arc::new(b.finish())),
            Self::Time(mut b) => (InfluxColumnType::Timestamp, Arc::new(b.finish())),
        }
    }

    fn get_rows(&self, rows: &[usize]) -> ArrayRef {
        match self {
            Self::Bool(b) => {
                let b = b.finish_cloned();
                let mut builder = BooleanBuilder::with_capacity(rows.len());
                for row in rows {
                    builder.append_value(b.value(*row));
                }
                Arc::new(builder.finish())
            }
            Self::I64(b) => {
                let b = b.finish_cloned();
                let mut builder = Int64Builder::with_capacity(rows.len());
                for row in rows {
                    builder.append_value(b.value(*row));
                }
                Arc::new(builder.finish())
            }
            Self::F64(b) => {
                let b = b.finish_cloned();
                let mut builder = Float64Builder::with_capacity(rows.len());
                for row in rows {
                    builder.append_value(b.value(*row));
                }
                Arc::new(builder.finish())
            }
            Self::U64(b) => {
                let b = b.finish_cloned();
                let mut builder = UInt64Builder::with_capacity(rows.len());
                for row in rows {
                    builder.append_value(b.value(*row));
                }
                Arc::new(builder.finish())
            }
            Self::String(b) => {
                let b = b.finish_cloned();
                let mut builder = StringBuilder::new();
                for row in rows {
                    builder.append_value(b.value(*row));
                }
                Arc::new(builder.finish())
            }
            Self::Tag(b) | Self::Key(b) => {
                let b = b.finish_cloned();
                let bv = b.values();
                let bva: &StringArray = bv.as_any().downcast_ref::<StringArray>().unwrap();

                let mut builder: GenericByteDictionaryBuilder<Int32Type, GenericStringType<i32>> =
                    StringDictionaryBuilder::new();
                for row in rows {
                    let val = b.key(*row).unwrap();
                    let tag_val = bva.value(val);

                    builder
                        .append(tag_val)
                        .expect("shouldn't be able to overflow 32 bit dictionary");
                }
                Arc::new(builder.finish())
            }
            Self::Time(b) => {
                let b = b.finish_cloned();
                let mut builder = TimestampNanosecondBuilder::with_capacity(rows.len());
                for row in rows {
                    builder.append_value(b.value(*row));
                }
                Arc::new(builder.finish())
            }
        }
    }

    #[allow(dead_code)]
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
            Self::Tag(b) | Self::Key(b) => {
                let b = b.finish_cloned();
                b.keys().len() * size_of::<i32>() + b.values().get_array_memory_size()
            }
            Self::Time(b) => size_of::<i64>() * b.capacity(),
        };
        size_of::<Self>() + data_size
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_util::assert_batches_eq;
    use datafusion::common::Column;
    use influxdb3_wal::Field;
    use schema::{InfluxFieldType, SchemaBuilder};

    #[test]
    fn tag_row_index() {
        let mut table_buffer = TableBuffer::new(&["tag"], SortKey::empty());
        let schema = SchemaBuilder::with_capacity(3)
            .tag("tag")
            .influx_field("value", InfluxFieldType::Integer)
            .timestamp()
            .build()
            .unwrap();

        let rows = vec![
            Row {
                time: 1,
                fields: vec![
                    Field {
                        name: "tag".into(),
                        value: FieldData::Tag("a".to_string()),
                    },
                    Field {
                        name: "value".into(),
                        value: FieldData::Integer(1),
                    },
                    Field {
                        name: "time".into(),
                        value: FieldData::Timestamp(1),
                    },
                ],
            },
            Row {
                time: 2,
                fields: vec![
                    Field {
                        name: "tag".into(),
                        value: FieldData::Tag("b".to_string()),
                    },
                    Field {
                        name: "value".into(),
                        value: FieldData::Integer(2),
                    },
                    Field {
                        name: "time".into(),
                        value: FieldData::Timestamp(2),
                    },
                ],
            },
            Row {
                time: 3,
                fields: vec![
                    Field {
                        name: "tag".into(),
                        value: FieldData::Tag("a".to_string()),
                    },
                    Field {
                        name: "value".into(),
                        value: FieldData::Integer(3),
                    },
                    Field {
                        name: "time".into(),
                        value: FieldData::Timestamp(3),
                    },
                ],
            },
        ];

        table_buffer.buffer_chunk(0, rows);

        let filter = &[Expr::BinaryExpr(BinaryExpr {
            left: Box::new(Expr::Column(Column {
                relation: None,
                name: "tag".to_string(),
            })),
            op: datafusion::logical_expr::Operator::Eq,
            right: Box::new(Expr::Literal(datafusion::scalar::ScalarValue::Utf8(Some(
                "a".to_string(),
            )))),
        })];
        let a_rows = table_buffer
            .chunk_time_to_chunks
            .get(&0)
            .unwrap()
            .index
            .get_rows_from_index_for_filter(filter)
            .unwrap();
        assert_eq!(a_rows, &[0, 2]);

        let a = table_buffer
            .record_batches(schema.as_arrow(), filter)
            .unwrap();
        let expected_a = vec![
            "+-----+-------+--------------------------------+",
            "| tag | value | time                           |",
            "+-----+-------+--------------------------------+",
            "| a   | 1     | 1970-01-01T00:00:00.000000001Z |",
            "| a   | 3     | 1970-01-01T00:00:00.000000003Z |",
            "+-----+-------+--------------------------------+",
        ];
        assert_batches_eq!(&expected_a, &a);

        let filter = &[Expr::BinaryExpr(BinaryExpr {
            left: Box::new(Expr::Column(Column {
                relation: None,
                name: "tag".to_string(),
            })),
            op: datafusion::logical_expr::Operator::Eq,
            right: Box::new(Expr::Literal(datafusion::scalar::ScalarValue::Utf8(Some(
                "b".to_string(),
            )))),
        })];

        let b_rows = table_buffer
            .chunk_time_to_chunks
            .get(&0)
            .unwrap()
            .index
            .get_rows_from_index_for_filter(filter)
            .unwrap();
        assert_eq!(b_rows, &[1]);

        let b = table_buffer
            .record_batches(schema.as_arrow(), filter)
            .unwrap();
        let expected_b = vec![
            "+-----+-------+--------------------------------+",
            "| tag | value | time                           |",
            "+-----+-------+--------------------------------+",
            "| b   | 2     | 1970-01-01T00:00:00.000000002Z |",
            "+-----+-------+--------------------------------+",
        ];
        assert_batches_eq!(&expected_b, &b);
    }

    #[test]
    fn computed_size_of_buffer() {
        let mut table_buffer = TableBuffer::new(&["tag"], SortKey::empty());

        let rows = vec![
            Row {
                time: 1,
                fields: vec![
                    Field {
                        name: "tag".into(),
                        value: FieldData::Tag("a".to_string()),
                    },
                    Field {
                        name: "value".into(),
                        value: FieldData::Integer(1),
                    },
                    Field {
                        name: "time".into(),
                        value: FieldData::Timestamp(1),
                    },
                ],
            },
            Row {
                time: 2,
                fields: vec![
                    Field {
                        name: "tag".into(),
                        value: FieldData::Tag("b".to_string()),
                    },
                    Field {
                        name: "value".into(),
                        value: FieldData::Integer(2),
                    },
                    Field {
                        name: "time".into(),
                        value: FieldData::Timestamp(2),
                    },
                ],
            },
            Row {
                time: 3,
                fields: vec![
                    Field {
                        name: "tag".into(),
                        value: FieldData::Tag("this is a long tag value to store".to_string()),
                    },
                    Field {
                        name: "value".into(),
                        value: FieldData::Integer(3),
                    },
                    Field {
                        name: "time".into(),
                        value: FieldData::Timestamp(3),
                    },
                ],
            },
        ];

        table_buffer.buffer_chunk(0, rows);

        let size = table_buffer.computed_size();
        assert_eq!(size, 18094);
    }
}
