//! The in memory bufffer of a table that can be quickly added to and queried

use crate::write_buffer::{FieldData, Row};
use arrow::array::{
    ArrayRef, BooleanBuilder, Float64Builder, Int64Builder, StringBuilder, StringDictionaryBuilder,
    TimestampNanosecondBuilder, UInt64Builder,
};
use arrow::datatypes::Int32Type;
use arrow::record_batch::RecordBatch;
use data_types::{PartitionKey, TimestampMinMax};
use schema::Schema;
use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;
use std::vec;

pub struct TableBuffer {
    pub segment_key: PartitionKey,
    timestamp_min: i64,
    timestamp_max: i64,
    data: BTreeMap<String, Builder>,
    row_count: usize,
}

impl TableBuffer {
    pub fn new(segment_key: PartitionKey) -> Self {
        Self {
            segment_key,
            timestamp_min: i64::MAX,
            timestamp_max: i64::MIN,
            data: Default::default(),
            row_count: 0,
        }
    }

    pub fn add_rows(&mut self, rows: Vec<Row>) {
        self.row_count += rows.len();

        for r in rows {
            let mut value_added = HashSet::with_capacity(r.fields.len());

            for f in r.fields {
                value_added.insert(f.name.clone());

                match f.value {
                    FieldData::Timestamp(v) => {
                        self.timestamp_min = self.timestamp_min.min(v);
                        self.timestamp_max = self.timestamp_max.max(v);

                        let b = self
                            .data
                            .entry(f.name)
                            .or_insert_with(|| Builder::Time(TimestampNanosecondBuilder::new()));
                        if let Builder::Time(b) = b {
                            b.append_value(v);
                        } else {
                            panic!("unexpected field type");
                        }
                    }
                    FieldData::Tag(v) => {
                        let b = self
                            .data
                            .entry(f.name)
                            .or_insert_with(|| Builder::Tag(StringDictionaryBuilder::new()));
                        if let Builder::Tag(b) = b {
                            b.append(v).unwrap(); // we won't overflow the 32-bit integer for this dictionary
                        } else {
                            panic!("unexpected field type");
                        }
                    }
                    FieldData::String(v) => {
                        let b = self
                            .data
                            .entry(f.name)
                            .or_insert_with(|| Builder::String(StringBuilder::new()));
                        if let Builder::String(b) = b {
                            b.append_value(v);
                        } else {
                            panic!("unexpected field type");
                        }
                    }
                    FieldData::Integer(v) => {
                        let b = self
                            .data
                            .entry(f.name)
                            .or_insert_with(|| Builder::I64(Int64Builder::new()));
                        if let Builder::I64(b) = b {
                            b.append_value(v);
                        } else {
                            panic!("unexpected field type");
                        }
                    }
                    FieldData::UInteger(v) => {
                        let b = self
                            .data
                            .entry(f.name)
                            .or_insert_with(|| Builder::U64(UInt64Builder::new()));
                        if let Builder::U64(b) = b {
                            b.append_value(v);
                        } else {
                            panic!("unexpected field type");
                        }
                    }
                    FieldData::Float(v) => {
                        let b = self
                            .data
                            .entry(f.name)
                            .or_insert_with(|| Builder::F64(Float64Builder::new()));
                        if let Builder::F64(b) = b {
                            b.append_value(v);
                        } else {
                            panic!("unexpected field type");
                        }
                    }
                    FieldData::Boolean(v) => {
                        let b = self
                            .data
                            .entry(f.name)
                            .or_insert_with(|| Builder::Bool(BooleanBuilder::new()));
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
    }

    pub fn timestamp_min_max(&self) -> TimestampMinMax {
        TimestampMinMax {
            min: self.timestamp_min,
            max: self.timestamp_max,
        }
    }

    pub fn record_batches(&self, schema: &Schema) -> Vec<RecordBatch> {
        // ensure the order of the columns matches their order in the Arrow schema definition
        let mut cols = Vec::with_capacity(self.data.len());
        let schema = schema.as_arrow();
        for f in &schema.fields {
            cols.push(self.data.get(f.name()).unwrap().as_arrow());
        }

        vec![RecordBatch::try_new(schema, cols).unwrap()]
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

enum Builder {
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
}
