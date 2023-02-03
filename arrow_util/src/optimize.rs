use std::collections::BTreeSet;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, DictionaryArray, StringArray};
use arrow::datatypes::{DataType, Int32Type};
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
            "truncating non-Int32 dictionaries not supported: {key_type}"
        )));
    }

    if value_type != &DataType::Utf8 {
        return Err(ArrowError::NotYetImplemented(format!(
            "truncating non-string dictionaries not supported: {value_type}"
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate as arrow_util;
    use crate::assert_batches_eq;
    use arrow::array::{ArrayDataBuilder, DictionaryArray, Float64Array, Int32Array, StringArray};
    use arrow::compute::concat;
    use std::iter::FromIterator;

    #[test]
    fn test_optimize_dictionaries() {
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
    fn test_optimize_dictionaries_concat() {
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
    fn test_optimize_dictionaries_null() {
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
    fn test_optimize_dictionaries_slice() {
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
}
