//! This module contains code to restore write buffer partitions from the WAL
use delorean_generated_types::wal as wb;
use delorean_line_parser::FieldValue;

use std::collections::BTreeSet;

use crate::column::{ColumnValue, Value};

pub fn type_description(value: wb::ColumnValue) -> &'static str {
    use wb::ColumnValue::*;

    match value {
        NONE => "none",
        TagValue => "tag",
        I64Value => "i64",
        U64Value => "u64",
        F64Value => "f64",
        BoolValue => "bool",
        StringValue => "String",
    }
}

#[derive(Debug)]
pub struct WalEntryBuilder<'a> {
    fbb: flatbuffers::FlatBufferBuilder<'a>,
    entries: Vec<flatbuffers::WIPOffset<wb::WriteBufferEntry<'a>>>,
    row_values: Vec<flatbuffers::WIPOffset<wb::Value<'a>>>,
    partitions: BTreeSet<u32>,
}

impl Default for WalEntryBuilder<'_> {
    fn default() -> Self {
        Self::new()
    }
}

impl WalEntryBuilder<'_> {
    pub fn new() -> Self {
        Self {
            fbb: flatbuffers::FlatBufferBuilder::new_with_capacity(1024),
            entries: vec![],
            row_values: vec![],
            partitions: BTreeSet::new(),
        }
    }

    pub fn ensure_partition_exists(&mut self, generation: u32, key: &str) {
        if !self.partitions.contains(&generation) {
            self.add_partition_open(generation, key);
        }
    }

    pub fn add_partition_open(&mut self, generation: u32, key: &str) {
        self.partitions.insert(generation);
        let partition_key = self.fbb.create_string(&key);

        let partition_open = wb::PartitionOpen::create(
            &mut self.fbb,
            &wb::PartitionOpenArgs {
                generation,
                key: Some(partition_key),
            },
        );

        let entry = wb::WriteBufferEntry::create(
            &mut self.fbb,
            &wb::WriteBufferEntryArgs {
                partition_open: Some(partition_open),
                ..Default::default()
            },
        );

        self.entries.push(entry);
    }

    fn add_tag_value(&mut self, column: &str, value: &str) {
        let value = self.fbb.create_string(&value);
        let tv = wb::TagValue::create(&mut self.fbb, &wb::TagValueArgs { value: Some(value) });

        self.add_value(column, wb::ColumnValue::TagValue, tv.as_union_value());
    }

    fn add_string_value(&mut self, column: &str, value: &str) {
        let value_offset = self.fbb.create_string(value);

        let sv = wb::StringValue::create(
            &mut self.fbb,
            &wb::StringValueArgs {
                value: Some(value_offset),
            },
        );

        self.add_value(column, wb::ColumnValue::StringValue, sv.as_union_value());
    }

    fn add_f64_value(&mut self, column: &str, value: f64) {
        let fv = wb::F64Value::create(&mut self.fbb, &wb::F64ValueArgs { value });

        self.add_value(column, wb::ColumnValue::F64Value, fv.as_union_value());
    }

    fn add_i64_value(&mut self, column: &str, value: i64) {
        let iv = wb::I64Value::create(&mut self.fbb, &wb::I64ValueArgs { value });

        self.add_value(column, wb::ColumnValue::I64Value, iv.as_union_value());
    }

    fn add_bool_value(&mut self, column: &str, value: bool) {
        let bv = wb::BoolValue::create(&mut self.fbb, &wb::BoolValueArgs { value });

        self.add_value(column, wb::ColumnValue::BoolValue, bv.as_union_value());
    }

    pub fn add_value(
        &mut self,
        column: &str,
        value_type: wb::ColumnValue,
        value: flatbuffers::WIPOffset<flatbuffers::UnionWIPOffset>,
    ) {
        let column = self.fbb.create_string(column);

        let row_value = wb::Value::create(
            &mut self.fbb,
            &wb::ValueArgs {
                column: Some(column),
                value_type,
                value: Some(value),
            },
        );

        self.row_values.push(row_value);
    }

    pub fn add_row(&mut self, table_name: &str, values: &[ColumnValue<'_>]) {
        let table = self.fbb.create_string(table_name);

        self.row_values = Vec::with_capacity(values.len());

        for col_value in values {
            match col_value.value {
                Value::TagValue(_, v) => {
                    self.add_tag_value(col_value.column, v);
                }
                Value::FieldValue(FieldValue::I64(v)) => {
                    self.add_i64_value(col_value.column, *v);
                }
                Value::FieldValue(FieldValue::F64(v)) => {
                    self.add_f64_value(col_value.column, *v);
                }
                Value::FieldValue(FieldValue::Boolean(v)) => {
                    self.add_bool_value(col_value.column, *v);
                }
                Value::FieldValue(FieldValue::String(v)) => {
                    self.add_string_value(col_value.column, v);
                }
            }
        }

        let values_vec = self.fbb.create_vector(&self.row_values);

        let row = wb::Row::create(
            &mut self.fbb,
            &wb::RowArgs {
                table: Some(table),
                values: Some(values_vec),
            },
        );

        let entry = wb::WriteBufferEntry::create(
            &mut self.fbb,
            &wb::WriteBufferEntryArgs {
                write: Some(row),
                ..Default::default()
            },
        );

        self.entries.push(entry);
        self.row_values = vec![];
    }

    fn create_batch(&mut self) {
        let entry_vec = self.fbb.create_vector(&self.entries);

        let batch = wb::WriteBufferBatch::create(
            &mut self.fbb,
            &wb::WriteBufferBatchArgs {
                entries: Some(entry_vec),
            },
        );

        self.fbb.finish(batch, None);
    }

    pub fn data(mut self) -> Vec<u8> {
        self.create_batch();

        let (mut data, idx) = self.fbb.collapse();
        data.split_off(idx)
    }
}
