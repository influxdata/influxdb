//! The in memory buffer of a table that can be quickly added to and queried

use crate::write_buffer::{FieldData, Row};
use arrow::array::{
    Array, ArrayBuilder, ArrayRef, BooleanBuilder, Float64Builder, GenericByteDictionaryBuilder,
    Int64Builder, StringArray, StringBuilder, StringDictionaryBuilder, TimestampNanosecondBuilder,
    UInt64Builder,
};
use arrow::datatypes::{GenericStringType, Int32Type, SchemaRef};
use arrow::record_batch::RecordBatch;
use data_types::{PartitionKey, TimestampMinMax};
use datafusion::logical_expr::{BinaryExpr, Expr};
use observability_deps::tracing::debug;
use std::collections::{BTreeMap, HashMap, HashSet};
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
    pub segment_key: PartitionKey,
    timestamp_min: i64,
    timestamp_max: i64,
    pub(crate) data: BTreeMap<String, Builder>,
    row_count: usize,
    index: BufferIndex,
}

impl TableBuffer {
    pub fn new(segment_key: PartitionKey, index_columns: &[String]) -> Self {
        Self {
            segment_key,
            timestamp_min: i64::MAX,
            timestamp_max: i64::MIN,
            data: Default::default(),
            row_count: 0,
            index: BufferIndex::new(index_columns),
        }
    }

    pub fn add_rows(&mut self, rows: Vec<Row>) {
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
                        Builder::Time(b) => b.append_null(),
                    }
                }
            }
        }

        self.row_count += new_row_count;
    }

    pub fn timestamp_min_max(&self) -> TimestampMinMax {
        TimestampMinMax {
            min: self.timestamp_min,
            max: self.timestamp_max,
        }
    }

    pub fn record_batch(&self, schema: SchemaRef, filter: &[Expr]) -> Result<RecordBatch> {
        let row_ids = self.index.get_rows_from_index_for_filter(filter);

        let mut cols = Vec::with_capacity(schema.fields().len());

        for f in schema.fields() {
            match row_ids {
                Some(row_ids) => {
                    let b = self
                        .data
                        .get(f.name())
                        .ok_or_else(|| Error::FieldNotFound(f.name().to_string()))?
                        .get_rows(row_ids);
                    cols.push(b);
                }
                None => {
                    let b = self
                        .data
                        .get(f.name())
                        .ok_or_else(|| Error::FieldNotFound(f.name().to_string()))?
                        .as_arrow();
                    cols.push(b);
                }
            }
        }

        Ok(RecordBatch::try_new(schema, cols)?)
    }

    /// Returns an estimate of the size of this table buffer based on the data and index sizes.
    pub fn _computed_size(&self) -> usize {
        let mut size = size_of::<Self>();
        for (k, v) in &self.data {
            size += k.len() + size_of::<String>() + v._size();
        }
        size += self.index._size();
        size
    }
}

// Debug implementation for TableBuffer
impl std::fmt::Debug for TableBuffer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TableBuffer")
            .field("segment_key", &self.segment_key)
            .field("timestamp_min", &self.timestamp_min)
            .field("timestamp_max", &self.timestamp_max)
            .field("row_count", &self.row_count)
            .finish()
    }
}

#[derive(Debug, Default)]
struct BufferIndex {
    // column name -> string value -> row indexes
    columns: HashMap<String, HashMap<String, Vec<usize>>>,
}

impl BufferIndex {
    fn new(columns: &[String]) -> Self {
        let columns = columns
            .iter()
            .map(|c| (c.clone(), HashMap::new()))
            .collect();
        Self { columns }
    }

    fn add_row_if_indexed_column(&mut self, row_index: usize, column_name: &str, value: &str) {
        if let Some(column) = self.columns.get_mut(column_name) {
            if column.contains_key(value) {
                column.get_mut(value).unwrap().push(row_index);
            } else {
                column.insert(value.to_string(), vec![row_index]);
            }
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
                            return self.columns.get(c.name.as_str()).and_then(|m| m.get(v));
                        }
                    }
                }
            }
        }

        None
    }

    fn _size(&self) -> usize {
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
            Self::Tag(b) => {
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

    fn _size(&self) -> usize {
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
mod tests {
    use super::*;
    use crate::write_buffer::Field;
    use arrow_util::assert_batches_eq;
    use datafusion::common::Column;
    use schema::{InfluxFieldType, SchemaBuilder};

    #[test]
    fn tag_row_index() {
        let mut table_buffer = TableBuffer::new(PartitionKey::from("table"), &["tag".to_string()]);
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
                        name: "tag".to_string(),
                        value: FieldData::Tag("a".to_string()),
                    },
                    Field {
                        name: "value".to_string(),
                        value: FieldData::Integer(1),
                    },
                    Field {
                        name: "time".to_string(),
                        value: FieldData::Timestamp(1),
                    },
                ],
            },
            Row {
                time: 2,
                fields: vec![
                    Field {
                        name: "tag".to_string(),
                        value: FieldData::Tag("b".to_string()),
                    },
                    Field {
                        name: "value".to_string(),
                        value: FieldData::Integer(2),
                    },
                    Field {
                        name: "time".to_string(),
                        value: FieldData::Timestamp(2),
                    },
                ],
            },
            Row {
                time: 3,
                fields: vec![
                    Field {
                        name: "tag".to_string(),
                        value: FieldData::Tag("a".to_string()),
                    },
                    Field {
                        name: "value".to_string(),
                        value: FieldData::Integer(3),
                    },
                    Field {
                        name: "time".to_string(),
                        value: FieldData::Timestamp(3),
                    },
                ],
            },
        ];

        table_buffer.add_rows(rows);

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
            .index
            .get_rows_from_index_for_filter(filter)
            .unwrap();
        assert_eq!(a_rows, &[0, 2]);

        let a = table_buffer
            .record_batch(schema.as_arrow(), filter)
            .unwrap();
        let expected_a = vec![
            "+-----+-------+--------------------------------+",
            "| tag | value | time                           |",
            "+-----+-------+--------------------------------+",
            "| a   | 1     | 1970-01-01T00:00:00.000000001Z |",
            "| a   | 3     | 1970-01-01T00:00:00.000000003Z |",
            "+-----+-------+--------------------------------+",
        ];
        assert_batches_eq!(&expected_a, &[a]);

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
            .index
            .get_rows_from_index_for_filter(filter)
            .unwrap();
        assert_eq!(b_rows, &[1]);

        let b = table_buffer
            .record_batch(schema.as_arrow(), filter)
            .unwrap();
        let expected_b = vec![
            "+-----+-------+--------------------------------+",
            "| tag | value | time                           |",
            "+-----+-------+--------------------------------+",
            "| b   | 2     | 1970-01-01T00:00:00.000000002Z |",
            "+-----+-------+--------------------------------+",
        ];
        assert_batches_eq!(&expected_b, &[b]);
    }

    #[test]
    fn computed_size_of_buffer() {
        let mut table_buffer = TableBuffer::new(PartitionKey::from("table"), &["tag".to_string()]);

        let rows = vec![
            Row {
                time: 1,
                fields: vec![
                    Field {
                        name: "tag".to_string(),
                        value: FieldData::Tag("a".to_string()),
                    },
                    Field {
                        name: "value".to_string(),
                        value: FieldData::Integer(1),
                    },
                    Field {
                        name: "time".to_string(),
                        value: FieldData::Timestamp(1),
                    },
                ],
            },
            Row {
                time: 2,
                fields: vec![
                    Field {
                        name: "tag".to_string(),
                        value: FieldData::Tag("b".to_string()),
                    },
                    Field {
                        name: "value".to_string(),
                        value: FieldData::Integer(2),
                    },
                    Field {
                        name: "time".to_string(),
                        value: FieldData::Timestamp(2),
                    },
                ],
            },
            Row {
                time: 3,
                fields: vec![
                    Field {
                        name: "tag".to_string(),
                        value: FieldData::Tag("this is a long tag value to store".to_string()),
                    },
                    Field {
                        name: "value".to_string(),
                        value: FieldData::Integer(3),
                    },
                    Field {
                        name: "time".to_string(),
                        value: FieldData::Timestamp(3),
                    },
                ],
            },
        ];

        table_buffer.add_rows(rows);

        let size = table_buffer._computed_size();
        assert_eq!(size, 18126);
    }
}
