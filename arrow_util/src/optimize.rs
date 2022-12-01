use std::collections::BTreeSet;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, DictionaryArray, StringArray};
use arrow::datatypes::{DataType, Field, Int32Type, Schema, SchemaRef};
use arrow::error::{ArrowError, Result};
use arrow::record_batch::RecordBatch;
use hashbrown::HashMap;

use crate::dictionary::StringDictionary;

/// Takes a record batch and returns a new record batch with dictionaries
/// optimized to contain no duplicate or unreferenced values
///
/// Where the input dictionaries are sorted, the output dictionaries
/// will also be
pub fn optimize_dictionaries(batch: &RecordBatch) -> Result<RecordBatch> {
    let schema = batch.schema();
    let new_columns = batch
        .columns()
        .iter()
        .zip(schema.fields())
        .map(|(col, field)| match field.data_type() {
            DataType::Dictionary(key, value) => optimize_dict_col(col, key, value),
            _ => Ok(Arc::clone(col)),
        })
        .collect::<Result<Vec<_>>>()?;

    RecordBatch::try_new(schema, new_columns)
}

/// Optimizes the dictionaries for a column
fn optimize_dict_col(
    col: &ArrayRef,
    key_type: &DataType,
    value_type: &DataType,
) -> Result<ArrayRef> {
    if key_type != &DataType::Int32 {
        return Err(ArrowError::NotYetImplemented(format!(
            "truncating non-Int32 dictionaries not supported: {}",
            key_type
        )));
    }

    if value_type != &DataType::Utf8 {
        return Err(ArrowError::NotYetImplemented(format!(
            "truncating non-string dictionaries not supported: {}",
            value_type
        )));
    }

    let col = col
        .as_any()
        .downcast_ref::<DictionaryArray<Int32Type>>()
        .expect("unexpected datatype");

    let keys = col.keys();
    let values = col.values();
    let values = values
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("unexpected datatype");

    // The total length of the resulting values array
    let mut values_len = 0_usize;

    // Keys that appear in the values array
    // Use a BTreeSet to preserve the order of the dictionary
    let mut used_keys = BTreeSet::new();
    for key in keys.iter().flatten() {
        if used_keys.insert(key) {
            values_len += values.value_length(key as usize) as usize;
        }
    }

    // Then perform deduplication
    let mut new_dictionary = StringDictionary::with_capacity(used_keys.len(), values_len);
    let mut old_to_new_idx: HashMap<i32, i32> = HashMap::with_capacity(used_keys.len());
    for key in used_keys {
        let new_key = new_dictionary.lookup_value_or_insert(values.value(key as usize));
        old_to_new_idx.insert(key, new_key);
    }

    let new_keys = keys.iter().map(|x| match x {
        Some(x) => *old_to_new_idx.get(&x).expect("no mapping found"),
        None => -1,
    });

    let offset = keys.data().offset();
    let nulls = keys
        .data()
        .null_buffer()
        .map(|buffer| buffer.bit_slice(offset, keys.len()));

    Ok(Arc::new(new_dictionary.to_arrow(new_keys, nulls)))
}

/// Hydrates a dictionary to its underlying type
///
/// An IPC response, streaming or otherwise, defines its schema up front
/// which defines the mapping from dictionary IDs. It then sends these
/// dictionaries over the wire.
///
/// This requires identifying the different dictionaries in use, assigning
/// them IDs, and sending new dictionaries, delta or otherwise, when needed
///
/// This is tracked by <https://github.com/influxdata/influxdb_iox/issues/1318>
///
/// See also:
/// * <https://github.com/influxdata/influxdb_iox/issues/4275>
/// * <https://github.com/apache/arrow-rs/issues/1206>
///
/// For now we just hydrate the dictionaries to their underlying type
fn hydrate_dictionary(array: &ArrayRef) -> Result<ArrayRef> {
    match array.data_type() {
        DataType::Dictionary(_, value) => arrow::compute::cast(array, value),
        _ => unreachable!("not a dictionary"),
    }
}

/// Prepares a RecordBatch for transport over the Arrow Flight protocol
///
/// This means:
///
/// 1. Hydrates any dictionaries to its underlying type. See
/// hydrate_dictionary for more information.
///
pub fn prepare_batch_for_flight(batch: &RecordBatch, schema: SchemaRef) -> Result<RecordBatch> {
    let columns: Result<Vec<_>> = batch
        .columns()
        .iter()
        .map(|column| {
            if matches!(column.data_type(), DataType::Dictionary(_, _)) {
                hydrate_dictionary(column)
            } else {
                Ok(Arc::clone(column))
            }
        })
        .collect();

    RecordBatch::try_new(schema, columns?)
}

/// Prepare an arrow Schema for transport over the Arrow Flight protocol
///
/// Convert dictionary types to underlying types
///
/// See hydrate_dictionary for more information
pub fn prepare_schema_for_flight(schema: &Schema) -> Schema {
    let fields = schema
        .fields()
        .iter()
        .map(|field| match field.data_type() {
            DataType::Dictionary(_, value_type) => Field::new(
                field.name(),
                value_type.as_ref().clone(),
                field.is_nullable(),
            )
            .with_metadata(field.metadata().clone()),
            _ => field.clone(),
        })
        .collect();

    Schema::new(fields)
}

/// The size to which we limit our [`RecordBatch`] payloads.
///
/// We will slice up the returned [`RecordBatch]s (preserving order) to only produce objects of approximately
/// this size (there's a bit of additional encoding overhead on top of that, but that should be OK).
///
/// This would normally be 4MB, but the size calculation for the record batches is rather inexact, so we set it to 2MB.
const MAX_GRPC_RESPONSE_BATCH_SIZE: usize = 2097152;

/// Split [`RecordBatch`] so it hopefully fits into a gRPC response.
///
/// Max size is controlled by [`MAX_GRPC_RESPONSE_BATCH_SIZE`].
///
/// Data is zero-copy sliced into batches.
pub fn split_batch_for_grpc_response(batch: RecordBatch) -> Vec<RecordBatch> {
    let size = batch
        .columns()
        .iter()
        .map(|col| col.get_array_memory_size())
        .sum::<usize>();

    let n_batches = (size / MAX_GRPC_RESPONSE_BATCH_SIZE
        + usize::from(size % MAX_GRPC_RESPONSE_BATCH_SIZE != 0))
    .max(1);
    let rows_per_batch = batch.num_rows() / n_batches;
    let mut out = Vec::with_capacity(n_batches + 1);

    let mut offset = 0;
    while offset < batch.num_rows() {
        let length = (offset + rows_per_batch).min(batch.num_rows() - offset);
        out.push(batch.slice(offset, length));

        offset += length;
    }

    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate as arrow_util;
    use crate::assert_batches_eq;
    use arrow::array::{
        ArrayDataBuilder, DictionaryArray, Float64Array, Int32Array, StringArray, UInt32Array,
        UInt8Array,
    };
    use arrow::compute::{concat, concat_batches};
    use arrow_flight::utils::flight_data_to_arrow_batch;
    use std::iter::FromIterator;

    #[test]
    fn test_optimize() {
        let values = StringArray::from(vec![
            "duplicate",
            "duplicate",
            "foo",
            "boo",
            "unused",
            "duplicate",
        ]);
        let keys = Int32Array::from(vec![
            Some(0),
            Some(1),
            None,
            Some(1),
            Some(2),
            Some(5),
            Some(3),
        ]);

        let batch = RecordBatch::try_from_iter(vec![(
            "foo",
            Arc::new(build_dict(keys, values)) as ArrayRef,
        )])
        .unwrap();

        let optimized = optimize_dictionaries(&batch).unwrap();

        let col = optimized
            .column(0)
            .as_any()
            .downcast_ref::<DictionaryArray<Int32Type>>()
            .unwrap();

        let values = col.values();
        let values = values.as_any().downcast_ref::<StringArray>().unwrap();
        let values = values.iter().flatten().collect::<Vec<_>>();
        assert_eq!(values, vec!["duplicate", "foo", "boo"]);

        assert_batches_eq!(
            vec![
                "+-----------+",
                "| foo       |",
                "+-----------+",
                "| duplicate |",
                "| duplicate |",
                "|           |",
                "| duplicate |",
                "| foo       |",
                "| duplicate |",
                "| boo       |",
                "+-----------+",
            ],
            &[optimized]
        );
    }

    #[test]
    fn test_concat() {
        let f1_1 = Float64Array::from(vec![Some(1.0), Some(2.0), Some(3.0), Some(4.0)]);
        let t2_1 = DictionaryArray::<Int32Type>::from_iter(vec![
            Some("a"),
            Some("g"),
            Some("a"),
            Some("b"),
        ]);
        let t1_1 = DictionaryArray::<Int32Type>::from_iter(vec![
            Some("a"),
            Some("a"),
            Some("b"),
            Some("b"),
        ]);

        let f1_2 = Float64Array::from(vec![Some(1.0), Some(5.0), Some(2.0), Some(46.0)]);
        let t2_2 = DictionaryArray::<Int32Type>::from_iter(vec![
            Some("a"),
            Some("b"),
            Some("a"),
            Some("a"),
        ]);
        let t1_2 = DictionaryArray::<Int32Type>::from_iter(vec![
            Some("a"),
            Some("d"),
            Some("a"),
            Some("b"),
        ]);

        let concat = RecordBatch::try_from_iter(vec![
            ("f1", concat(&[&f1_1, &f1_2]).unwrap()),
            ("t2", concat(&[&t2_1, &t2_2]).unwrap()),
            ("t1", concat(&[&t1_1, &t1_2]).unwrap()),
        ])
        .unwrap();

        let optimized = optimize_dictionaries(&concat).unwrap();

        let col = optimized
            .column(optimized.schema().column_with_name("t2").unwrap().0)
            .as_any()
            .downcast_ref::<DictionaryArray<Int32Type>>()
            .unwrap();

        let values = col.values();
        let values = values.as_any().downcast_ref::<StringArray>().unwrap();
        let values = values.iter().flatten().collect::<Vec<_>>();
        assert_eq!(values, vec!["a", "g", "b"]);

        let col = optimized
            .column(optimized.schema().column_with_name("t1").unwrap().0)
            .as_any()
            .downcast_ref::<DictionaryArray<Int32Type>>()
            .unwrap();

        let values = col.values();
        let values = values.as_any().downcast_ref::<StringArray>().unwrap();
        let values = values.iter().flatten().collect::<Vec<_>>();
        assert_eq!(values, vec!["a", "b", "d"]);

        assert_batches_eq!(
            vec![
                "+----+----+----+",
                "| f1 | t2 | t1 |",
                "+----+----+----+",
                "| 1  | a  | a  |",
                "| 2  | g  | a  |",
                "| 3  | a  | b  |",
                "| 4  | b  | b  |",
                "| 1  | a  | a  |",
                "| 5  | b  | d  |",
                "| 2  | a  | a  |",
                "| 46 | a  | b  |",
                "+----+----+----+",
            ],
            &[optimized]
        );
    }

    #[test]
    fn test_null() {
        let values = StringArray::from(vec!["bananas"]);
        let keys = Int32Array::from(vec![None, None, Some(0)]);
        let col = Arc::new(build_dict(keys, values)) as ArrayRef;

        let col = optimize_dict_col(&col, &DataType::Int32, &DataType::Utf8).unwrap();

        let batch = RecordBatch::try_from_iter(vec![("t", col)]).unwrap();

        assert_batches_eq!(
            vec![
                "+---------+",
                "| t       |",
                "+---------+",
                "|         |",
                "|         |",
                "| bananas |",
                "+---------+",
            ],
            &[batch]
        );
    }

    #[test]
    fn test_slice() {
        let values = StringArray::from(vec!["bananas"]);
        let keys = Int32Array::from(vec![None, Some(0), None]);
        let col = Arc::new(build_dict(keys, values)) as ArrayRef;
        let col = col.slice(1, 2);

        let col = optimize_dict_col(&col, &DataType::Int32, &DataType::Utf8).unwrap();

        let batch = RecordBatch::try_from_iter(vec![("t", col)]).unwrap();

        assert_batches_eq!(
            vec![
                "+---------+",
                "| t       |",
                "+---------+",
                "| bananas |",
                "|         |",
                "+---------+",
            ],
            &[batch]
        );
    }

    fn build_dict(keys: Int32Array, values: StringArray) -> DictionaryArray<Int32Type> {
        let data = ArrayDataBuilder::new(DataType::Dictionary(
            Box::new(DataType::Int32),
            Box::new(DataType::Utf8),
        ))
        .len(keys.len())
        .add_buffer(keys.data().buffers()[0].clone())
        .null_bit_buffer(keys.data().null_buffer().cloned())
        .add_child_data(values.data().clone())
        .build()
        .unwrap();

        DictionaryArray::from(data)
    }

    #[test]
    fn test_encode_flight_data() {
        let options = arrow::ipc::writer::IpcWriteOptions::default();
        let c1 = UInt32Array::from(vec![1, 2, 3, 4, 5, 6]);

        let batch = RecordBatch::try_from_iter(vec![("a", Arc::new(c1) as ArrayRef)])
            .expect("cannot create record batch");
        let schema = batch.schema();

        let (_, baseline_flight_batch) =
            arrow_flight::utils::flight_data_from_arrow_batch(&batch, &options);

        let big_batch = batch.slice(0, batch.num_rows() - 1);
        let optimized_big_batch =
            prepare_batch_for_flight(&big_batch, Arc::clone(&schema)).expect("failed to optimize");
        let (_, optimized_big_flight_batch) =
            arrow_flight::utils::flight_data_from_arrow_batch(&optimized_big_batch, &options);

        assert_eq!(
            baseline_flight_batch.data_body.len(),
            optimized_big_flight_batch.data_body.len()
        );

        let small_batch = batch.slice(0, 1);
        let optimized_small_batch = prepare_batch_for_flight(&small_batch, Arc::clone(&schema))
            .expect("failed to optimize");
        let (_, optimized_small_flight_batch) =
            arrow_flight::utils::flight_data_from_arrow_batch(&optimized_small_batch, &options);

        assert!(
            baseline_flight_batch.data_body.len() > optimized_small_flight_batch.data_body.len()
        );
    }

    #[test]
    fn test_encode_flight_data_dictionary() {
        let options = arrow::ipc::writer::IpcWriteOptions::default();

        let c1 = UInt32Array::from(vec![1, 2, 3, 4, 5, 6]);
        let c2: DictionaryArray<Int32Type> = vec![
            Some("foo"),
            Some("bar"),
            None,
            Some("fiz"),
            None,
            Some("foo"),
        ]
        .into_iter()
        .collect();

        let batch =
            RecordBatch::try_from_iter(vec![("a", Arc::new(c1) as ArrayRef), ("b", Arc::new(c2))])
                .expect("cannot create record batch");

        let original_schema = batch.schema();
        let optimized_schema = Arc::new(prepare_schema_for_flight(&original_schema));

        let optimized_batch =
            prepare_batch_for_flight(&batch, Arc::clone(&optimized_schema)).unwrap();

        let (_, flight_data) =
            arrow_flight::utils::flight_data_from_arrow_batch(&optimized_batch, &options);

        let dictionary_by_id = std::collections::HashMap::new();
        let batch = flight_data_to_arrow_batch(
            &flight_data,
            Arc::clone(&optimized_schema),
            &dictionary_by_id,
        )
        .unwrap();

        // Should hydrate string dictionary for transport
        assert_eq!(optimized_schema.field(1).data_type(), &DataType::Utf8);
        let array = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        let expected = StringArray::from(vec![
            Some("foo"),
            Some("bar"),
            None,
            Some("fiz"),
            None,
            Some("foo"),
        ]);
        assert_eq!(array, &expected)
    }

    #[test]
    fn test_split_batch_for_grpc_response() {
        // no split
        let c = UInt32Array::from(vec![1, 2, 3, 4, 5, 6]);
        let batch = RecordBatch::try_from_iter(vec![("a", Arc::new(c) as ArrayRef)])
            .expect("cannot create record batch");
        let split = split_batch_for_grpc_response(batch.clone());
        assert_eq!(split.len(), 1);
        assert_eq!(batch, split[0]);

        // split once
        let n_rows = MAX_GRPC_RESPONSE_BATCH_SIZE + 1;
        assert!(n_rows % 2 == 1, "should be an odd number");
        let c = UInt8Array::from((0..n_rows).map(|i| (i % 256) as u8).collect::<Vec<_>>());
        let batch = RecordBatch::try_from_iter(vec![("a", Arc::new(c) as ArrayRef)])
            .expect("cannot create record batch");
        let split = split_batch_for_grpc_response(batch.clone());
        assert_eq!(split.len(), 2);
        assert_eq!(
            split.iter().map(|batch| batch.num_rows()).sum::<usize>(),
            n_rows
        );
        assert_eq!(concat_batches(&batch.schema(), &split).unwrap(), batch);
    }
}
