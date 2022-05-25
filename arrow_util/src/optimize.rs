use std::collections::BTreeSet;
use std::sync::Arc;

use arrow::array::{make_array, Array, ArrayRef, DictionaryArray, MutableArrayData, StringArray};
use arrow::datatypes::{DataType, Field, Int32Type, Schema, SchemaRef};
use arrow::error::{ArrowError, Result};
use arrow::record_batch::RecordBatch;
use hashbrown::HashMap;

use crate::dictionary::StringDictionary;

/// Takes a record batch and returns a new record batch with dictionaries
/// optimized to contain no duplicate or unreferenced values
///
/// Where the input dictionaries are sorted, the output dictionaries will also be
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

/// Some batches are small slices of the underlying arrays.
/// At this stage we only know the number of rows in the record batch
/// and the sizes in bytes of the backing buffers of the column arrays.
/// There is no straight-forward relationship between these two quantities,
/// since some columns can host variable length data such as strings.
///
/// However we can apply a quick&dirty heuristic:
/// if the backing buffer is two orders of magnitudes bigger
/// than the number of rows in the result set, we assume
/// that deep-copying the record batch is cheaper than the and transfer costs.
///
/// Possible improvements: take the type of the columns into consideration
/// and perhaps sample a few element sizes (taking care of not doing more work
/// than to always copying the results in the first place).
///
/// Or we just fix this upstream in
/// arrow_flight::utils::flight_data_from_arrow_batch and re-encode the array
/// into a smaller buffer while we have to copy stuff around anyway.
///
/// See rationale and discussions about future improvements on
/// <https://github.com/influxdata/influxdb_iox/issues/1133>
pub fn optimize_record_batch(batch: &RecordBatch, schema: SchemaRef) -> Result<RecordBatch> {
    let max_buf_len = batch
        .columns()
        .iter()
        .map(|a| a.get_array_memory_size())
        .max()
        .unwrap_or_default();

    let columns: Result<Vec<_>> = batch
        .columns()
        .iter()
        .map(|column| {
            if matches!(column.data_type(), DataType::Dictionary(_, _)) {
                hydrate_dictionary(column)
            } else if max_buf_len > batch.num_rows() * 100 {
                Ok(deep_clone_array(column))
            } else {
                Ok(Arc::clone(column))
            }
        })
        .collect();

    RecordBatch::try_new(schema, columns?)
}

fn deep_clone_array(array: &ArrayRef) -> ArrayRef {
    let mut mutable = MutableArrayData::new(vec![array.data()], false, 0);
    mutable.extend(0, 0, array.len());

    make_array(mutable.freeze())
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
/// This is tracked by #1318
///
/// For now we just hydrate the dictionaries to their underlying type
fn hydrate_dictionary(array: &ArrayRef) -> Result<ArrayRef> {
    match array.data_type() {
        DataType::Dictionary(_, value) => arrow::compute::cast(array, value),
        _ => unreachable!("not a dictionary"),
    }
}

/// Convert dictionary types to underlying types
/// See hydrate_dictionary for more information
pub fn optimize_schema(schema: &Schema) -> Schema {
    let fields = schema
        .fields()
        .iter()
        .map(|field| match field.data_type() {
            DataType::Dictionary(_, value_type) => Field::new(
                field.name(),
                value_type.as_ref().clone(),
                field.is_nullable(),
            ),
            _ => field.clone(),
        })
        .collect();

    Schema::new(fields)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate as arrow_util;
    use crate::assert_batches_eq;
    use arrow::array::{
        ArrayDataBuilder, DictionaryArray, Float64Array, Int32Array, StringArray, UInt32Array,
    };
    use arrow::compute::concat;
    use arrow_flight::utils::flight_data_to_arrow_batch;
    use datafusion::physical_plan::limit::truncate_batch;
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
        .null_bit_buffer(keys.data().null_buffer().unwrap().clone())
        .add_child_data(values.data().clone())
        .build()
        .unwrap();

        DictionaryArray::from(data)
    }

    #[test]
    fn test_deep_clone_array() {
        let mut builder = UInt32Array::builder(1000);
        builder.append_slice(&[1, 2, 3, 4, 5, 6]).unwrap();
        let array: ArrayRef = Arc::new(builder.finish());
        assert_eq!(array.len(), 6);

        let sliced = array.slice(0, 2);
        assert_eq!(sliced.len(), 2);

        let deep_cloned = deep_clone_array(&sliced);
        assert!(sliced.data().get_array_memory_size() > deep_cloned.data().get_array_memory_size());
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

        let big_batch = truncate_batch(&batch, batch.num_rows() - 1);
        let optimized_big_batch =
            optimize_record_batch(&big_batch, Arc::clone(&schema)).expect("failed to optimize");
        let (_, optimized_big_flight_batch) =
            arrow_flight::utils::flight_data_from_arrow_batch(&optimized_big_batch, &options);

        assert_eq!(
            baseline_flight_batch.data_body.len(),
            optimized_big_flight_batch.data_body.len()
        );

        let small_batch = truncate_batch(&batch, 1);
        let optimized_small_batch =
            optimize_record_batch(&small_batch, Arc::clone(&schema)).expect("failed to optimize");
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
        let optimized_schema = Arc::new(optimize_schema(&original_schema));

        let optimized_batch = optimize_record_batch(&batch, Arc::clone(&optimized_schema)).unwrap();

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
}
