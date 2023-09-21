//! Code to encode [`MutableBatch`] as pbdata protobuf

use arrow_util::bitset::{iter_set_positions, BitSet};
use dml::DmlWrite;
use generated_types::influxdata::pbdata::v1::column::SemanticType;
use generated_types::influxdata::pbdata::v1::{
    column::Values as PbValues, Column as PbColumn, DatabaseBatch, InternedStrings, PackedStrings,
    TableBatch,
};
use mutable_batch::column::{Column, ColumnData};
use mutable_batch::MutableBatch;
use schema::InfluxColumnType;

/// Convert a [`DmlWrite`] to a [`DatabaseBatch`]
pub fn encode_write(database_id: i64, write: &DmlWrite) -> DatabaseBatch {
    DatabaseBatch {
        table_batches: write
            .tables()
            .map(|(table_id, batch)| encode_batch(table_id.get(), batch))
            .collect(),
        partition_key: write.partition_key().to_string(),
        database_id,
    }
}

/// Convert a [`MutableBatch`] to [`TableBatch`]
pub fn encode_batch(table_id: i64, batch: &MutableBatch) -> TableBatch {
    TableBatch {
        columns: batch
            .columns()
            .filter_map(|(column_name, column)| {
                // Skip encoding any entirely NULL columns.
                //
                // This prevents a type-inference error during deserialisation
                // of the proto wire message.
                //
                //  https://github.com/influxdata/influxdb_iox/issues/4272
                //
                if column.valid_mask().is_all_unset() {
                    return None;
                }

                Some(encode_column(column_name, column))
            })
            .collect(),
        row_count: batch.rows() as u32,
        table_id,
    }
}

fn encode_column(column_name: &str, column: &Column) -> PbColumn {
    let valid_mask = column.valid_mask().bytes();

    let mut values = PbValues {
        i64_values: vec![],
        f64_values: vec![],
        u64_values: vec![],
        string_values: vec![],
        bool_values: vec![],
        bytes_values: vec![],
        packed_string_values: None,
        interned_string_values: None,
    };

    let semantic_type = match column.influx_type() {
        InfluxColumnType::Tag => SemanticType::Tag,
        InfluxColumnType::Field(_) => SemanticType::Field,
        InfluxColumnType::Timestamp => SemanticType::Time,
    };

    match column.data() {
        ColumnData::F64(col_data, _) => {
            values.f64_values = iter_set_positions(valid_mask)
                .map(|idx| col_data[idx])
                .collect();
        }
        ColumnData::I64(col_data, _) => {
            values.i64_values = iter_set_positions(valid_mask)
                .map(|idx| col_data[idx])
                .collect();
        }
        ColumnData::U64(col_data, _) => {
            values.u64_values = iter_set_positions(valid_mask)
                .map(|idx| col_data[idx])
                .collect();
        }
        ColumnData::String(col_data, _) => {
            let (col_offsets, col_storage) = col_data.inner();

            // Nulls are stored as empty strings which take up no space
            let mut offsets = vec![0; 1];
            for idx in iter_set_positions(valid_mask) {
                offsets.push(col_offsets[idx + 1] as u32)
            }

            values.packed_string_values = Some(PackedStrings {
                values: col_storage.to_string(),
                offsets,
            });
        }
        ColumnData::Bool(col_data, _) => {
            values.bool_values = iter_set_positions(valid_mask)
                .map(|idx| col_data.get(idx))
                .collect();
        }
        ColumnData::Tag(col_data, dict, _) => {
            let (val_offsets, val_storage) = dict.values().inner();

            values.interned_string_values = Some(InternedStrings {
                dictionary: Some(PackedStrings {
                    values: val_storage.to_string(),
                    offsets: val_offsets.iter().map(|idx| *idx as u32).collect(),
                }),
                values: iter_set_positions(valid_mask)
                    .map(|idx| col_data[idx] as u32)
                    .collect(),
            });
        }
    };

    PbColumn {
        column_name: column_name.to_string(),
        semantic_type: semantic_type as _,
        values: Some(values),
        null_mask: compute_null_mask(column.valid_mask()),
    }
}

fn compute_null_mask(valid_mask: &BitSet) -> Vec<u8> {
    let mut buffer: Vec<_> = valid_mask.bytes().iter().map(|x| !*x).collect();
    let overrun = valid_mask.len() & 7;
    if overrun > 0 {
        *buffer.last_mut().unwrap() &= (1 << overrun) - 1;
    }
    buffer
}
