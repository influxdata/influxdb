//! Code to encode [`MutableBatch`] as pbdata protobuf

use arrow_util::bitset::{BitSet, iter_set_positions};
use data_types::TableBatchWithoutId;
use dml::DmlWrite;
use generated_types::influxdata::pbdata::v1::column::SemanticType;
use generated_types::influxdata::pbdata::v1::{
    Column as PbColumn, DatabaseBatch, InternedStrings, PackedStrings, TableBatch,
    column::Values as PbValues,
};
use mutable_batch::MutableBatch;
use mutable_batch::column::{Column, ColumnData};
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

/// Encode the given mutable batch to a [`TableBatchWithoutId`]. Basically same as [`encode_batch`],
/// but when you don't have an id.
pub fn encode_batch_without_id(batch: &MutableBatch) -> TableBatchWithoutId {
    let batch_cols = batch.columns();
    let mut columns = vec![None; batch_cols.len()];

    for (column_idx, column_name, column) in batch_cols {
        // Skip encoding any entirely NULL columns.
        //
        // This prevents a type-inference error during deserialisation
        // of the proto wire message.
        //
        //  https://github.com/influxdata/influxdb_iox/issues/4272
        //
        let valid_mask = column.valid_mask();
        if !valid_mask.is_empty() && valid_mask.is_all_unset() {
            continue;
        }

        columns[column_idx] = Some(encode_column(column_name, column));
    }

    TableBatchWithoutId {
        columns: columns.into_iter().flatten().collect(),
        row_count: batch.rows() as u32,
    }
}

/// Convert a [`MutableBatch`] to [`TableBatch`]
pub fn encode_batch(table_id: i64, batch: &MutableBatch) -> TableBatch {
    let TableBatchWithoutId { columns, row_count } = encode_batch_without_id(batch);
    TableBatch {
        columns,
        row_count,
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
        ColumnData::F64(col_data) => {
            values.f64_values = iter_set_positions(valid_mask)
                .map(|idx| col_data[idx])
                .collect();
        }
        ColumnData::I64(col_data) => {
            values.i64_values = iter_set_positions(valid_mask)
                .map(|idx| col_data[idx])
                .collect();
        }
        ColumnData::U64(col_data) => {
            values.u64_values = iter_set_positions(valid_mask)
                .map(|idx| col_data[idx])
                .collect();
        }
        ColumnData::String(col_data) => {
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
        ColumnData::Bool(col_data) => {
            values.bool_values = iter_set_positions(valid_mask)
                .map(|idx| col_data.get(idx))
                .collect();
        }
        ColumnData::Tag(col_data, dict) => {
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
    if valid_mask.is_all_set() {
        return vec![];
    }

    let mut buffer: Vec<_> = valid_mask.bytes().iter().map(|x| !*x).collect();
    let overrun = valid_mask.len() & 7;
    if overrun > 0 {
        *buffer.last_mut().unwrap() &= (1 << overrun) - 1;
    }

    // Clear trailing 0s so we don't transmit them when we don't need to
    while buffer.last().is_some_and(|b| *b == 0) {
        buffer.pop();
    }

    buffer
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn null_masks_from_valid_masks() {
        fn inner(bits_len: usize, initial_bits: &[u8], expected: &[u8]) {
            let mut valid_mask = BitSet::with_capacity(bits_len);
            valid_mask.append_bits(bits_len, initial_bits);
            let null_mask = compute_null_mask(&valid_mask);
            assert_eq!(expected, &*null_mask);
        }

        inner(9, &[0b10100111, 1], &[0b01011000]);

        inner(2, &[0b00], &[0b11]);
        inner(16, &[0b11111111, 0b11111111], &[]);
        inner(16, &[0b00000000, 0b00000000], &[0b11111111, 0b11111111]);
        inner(10, &[0b01010101, 0b10], &[0b10101010, 0b1]);
    }
}
