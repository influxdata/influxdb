//! This module contains code to restore write buffer partitions from the WAL
use crate::partition::TIME_COLUMN_NAME;
use delorean_generated_types::wal as wb;
use delorean_line_parser::{FieldValue, ParsedLine};

use std::collections::BTreeMap;

use chrono::Utc;
use flatbuffers::FlatBufferBuilder;

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

pub fn split_lines_into_write_entry_partitions(
    partition_key: impl Fn(&ParsedLine<'_>) -> String,
    lines: &[ParsedLine<'_>],
) -> Vec<u8> {
    let mut fbb = flatbuffers::FlatBufferBuilder::new_with_capacity(1024);

    // split the lines into collections that go into partitions
    let mut partition_writes = BTreeMap::new();

    for line in lines {
        let key = partition_key(line);

        partition_writes
            .entry(key)
            .or_insert_with(Vec::new)
            .push(line);
    }

    // create a WALEntry for each batch of lines going to a partition (one WALEntry per partition)
    let entries = partition_writes
        .into_iter()
        .map(|(key, lines)| add_write_entry(&mut fbb, Some(&key), &lines))
        .collect::<Vec<_>>();

    let entries_vec = fbb.create_vector(&entries);

    let batch = wb::WriteBufferBatch::create(
        &mut fbb,
        &wb::WriteBufferBatchArgs {
            entries: Some(entries_vec),
        },
    );

    fbb.finish(batch, None);

    let (mut data, idx) = fbb.collapse();
    data.split_off(idx)
}

fn add_write_entry<'a>(
    fbb: &mut FlatBufferBuilder<'a>,
    partition_key: Option<&str>,
    lines: &[&ParsedLine<'_>],
) -> flatbuffers::WIPOffset<wb::WriteBufferEntry<'a>> {
    // split into tables
    let mut table_batches = BTreeMap::new();
    for line in lines {
        let measurement = line.series.measurement.as_str();
        table_batches
            .entry(measurement)
            .or_insert_with(Vec::new)
            .push(*line);
    }

    // create TableWriteBatch for each table
    let table_batches = table_batches
        .into_iter()
        .map(|(name, lines)| add_table_batch(fbb, name, &lines))
        .collect::<Vec<_>>();

    // create write entry
    let batches_vec = fbb.create_vector(&table_batches);

    let args = match partition_key {
        Some(key) => {
            let key = fbb.create_string(key);
            wb::WriteBufferEntryArgs {
                partition_key: Some(key),
                table_batches: Some(batches_vec),
                ..Default::default()
            }
        }
        None => wb::WriteBufferEntryArgs {
            table_batches: Some(batches_vec),
            ..Default::default()
        },
    };

    wb::WriteBufferEntry::create(fbb, &args)
}

fn add_table_batch<'a>(
    fbb: &mut FlatBufferBuilder<'a>,
    name: &str,
    lines: &[&ParsedLine<'_>],
) -> flatbuffers::WIPOffset<wb::TableWriteBatch<'a>> {
    // create Row
    let rows = lines
        .iter()
        .map(|line| add_line(fbb, line))
        .collect::<Vec<_>>();

    let table_name = fbb.create_string(name);
    let rows = fbb.create_vector(&rows);

    wb::TableWriteBatch::create(
        fbb,
        &wb::TableWriteBatchArgs {
            name: Some(table_name),
            rows: Some(rows),
        },
    )
}

fn add_line<'a>(
    fbb: &mut FlatBufferBuilder<'a>,
    line: &ParsedLine<'_>,
) -> flatbuffers::WIPOffset<wb::Row<'a>> {
    let mut row_values = Vec::new();

    if let Some(tags) = &line.series.tag_set {
        for (column, value) in tags {
            row_values.push(add_tag_value(fbb, column.as_str(), value.as_str()));
        }
    }

    for (column, value) in &line.field_set {
        let val = match value {
            FieldValue::I64(v) => add_i64_value(fbb, column.as_str(), *v),
            FieldValue::F64(v) => add_f64_value(fbb, column.as_str(), *v),
            FieldValue::Boolean(v) => add_bool_value(fbb, column.as_str(), *v),
            FieldValue::String(v) => add_string_value(fbb, column.as_str(), v.as_str()),
        };

        row_values.push(val);
    }

    let time = line
        .timestamp
        .unwrap_or_else(|| Utc::now().timestamp_nanos());
    row_values.push(add_i64_value(fbb, TIME_COLUMN_NAME, time));

    let row_values = fbb.create_vector(&row_values);

    wb::Row::create(
        fbb,
        &wb::RowArgs {
            values: Some(row_values),
        },
    )
}

fn add_tag_value<'a>(
    fbb: &mut FlatBufferBuilder<'a>,
    column: &str,
    value: &str,
) -> flatbuffers::WIPOffset<wb::Value<'a>> {
    let value = fbb.create_string(&value);
    let tv = wb::TagValue::create(fbb, &wb::TagValueArgs { value: Some(value) });

    add_value(fbb, column, wb::ColumnValue::TagValue, tv.as_union_value())
}

fn add_string_value<'a>(
    fbb: &mut FlatBufferBuilder<'a>,
    column: &str,
    value: &str,
) -> flatbuffers::WIPOffset<wb::Value<'a>> {
    let value_offset = fbb.create_string(value);

    let sv = wb::StringValue::create(
        fbb,
        &wb::StringValueArgs {
            value: Some(value_offset),
        },
    );

    add_value(
        fbb,
        column,
        wb::ColumnValue::StringValue,
        sv.as_union_value(),
    )
}

fn add_f64_value<'a>(
    fbb: &mut FlatBufferBuilder<'a>,
    column: &str,
    value: f64,
) -> flatbuffers::WIPOffset<wb::Value<'a>> {
    let fv = wb::F64Value::create(fbb, &wb::F64ValueArgs { value });

    add_value(fbb, column, wb::ColumnValue::F64Value, fv.as_union_value())
}

fn add_i64_value<'a>(
    fbb: &mut FlatBufferBuilder<'a>,
    column: &str,
    value: i64,
) -> flatbuffers::WIPOffset<wb::Value<'a>> {
    let iv = wb::I64Value::create(fbb, &wb::I64ValueArgs { value });

    add_value(fbb, column, wb::ColumnValue::I64Value, iv.as_union_value())
}

fn add_bool_value<'a>(
    fbb: &mut FlatBufferBuilder<'a>,
    column: &str,
    value: bool,
) -> flatbuffers::WIPOffset<wb::Value<'a>> {
    let bv = wb::BoolValue::create(fbb, &wb::BoolValueArgs { value });

    add_value(fbb, column, wb::ColumnValue::BoolValue, bv.as_union_value())
}

fn add_value<'a>(
    fbb: &mut FlatBufferBuilder<'a>,
    column: &str,
    value_type: wb::ColumnValue,
    value: flatbuffers::WIPOffset<flatbuffers::UnionWIPOffset>,
) -> flatbuffers::WIPOffset<wb::Value<'a>> {
    let column = fbb.create_string(column);

    wb::Value::create(
        fbb,
        &wb::ValueArgs {
            column: Some(column),
            value_type,
            value: Some(value),
        },
    )
}
