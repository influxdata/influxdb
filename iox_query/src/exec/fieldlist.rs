//! This module contains the definition of a "FieldList" a set of
//! records of (field_name, field_type, last_timestamp) and code to
//! pull them from RecordBatches
use std::{collections::BTreeMap, sync::Arc};

use arrow::{
    self,
    array::TimestampNanosecondArray,
    datatypes::{DataType, SchemaRef},
    record_batch::RecordBatch,
};
use schema::TIME_COLUMN_NAME;

use snafu::{ensure, ResultExt, Snafu};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "Internal error converting to FieldList. No time column in schema: {:?}. {}",
        schema,
        source
    ))]
    InternalNoTimeColumn {
        schema: SchemaRef,
        source: arrow::error::ArrowError,
    },

    #[snafu(display(
        "Inconsistent data type for field '{}': found both '{:?}' and '{:?}'",
        field_name,
        data_type1,
        data_type2
    ))]
    InconsistentFieldType {
        field_name: String,
        data_type1: DataType,
        data_type2: DataType,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Represents a single Field (column)'s metadata: Name, data_type,
/// and most recent (last) timestamp.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Field {
    pub name: String,
    pub data_type: DataType,
    pub last_timestamp: i64,
}

/// A list of `Fields`
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct FieldList {
    pub fields: Vec<Field>,
}

/// Trait to convert RecordBatch'y things into `FieldLists`. Assumes
/// that the input RecordBatch es can each have a single string
/// column.
pub trait IntoFieldList {
    /// Convert this thing into a fieldlist
    fn into_fieldlist(self) -> Result<FieldList>;
}

/// Converts record batches into FieldLists
impl IntoFieldList for Vec<RecordBatch> {
    fn into_fieldlist(self) -> Result<FieldList> {
        if self.is_empty() {
            return Ok(FieldList::default());
        }

        // For each field in the schema (except time) for all rows
        // that are non-null, update the current most-recent timestamp
        // seen
        let arrow_schema = self[0].schema();

        let time_column_index = arrow_schema.index_of(TIME_COLUMN_NAME).with_context(|_| {
            InternalNoTimeColumnSnafu {
                schema: Arc::clone(&arrow_schema),
            }
        })?;

        // key: fieldname, value: highest value of time column we have seen
        let mut field_times = BTreeMap::new();

        for batch in self {
            let time_column = batch
                .column(time_column_index)
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .expect("Downcasting time to TimestampNanosecondArray");

            for (column_index, arrow_field) in arrow_schema.fields().iter().enumerate() {
                if column_index == time_column_index {
                    continue;
                }
                let array = batch.column(column_index);

                // walk each value in array, looking for non-null values
                let mut max_ts: Option<i64> = None;
                for i in 0..batch.num_rows() {
                    if !array.is_null(i) {
                        let cur_ts = time_column.value(i);
                        max_ts = max_ts.map(|ts| std::cmp::max(ts, cur_ts)).or(Some(cur_ts));
                    }
                }

                if let Some(max_ts) = max_ts {
                    if let Some(ts) = field_times.get_mut(arrow_field.name()) {
                        *ts = std::cmp::max(max_ts, *ts);
                    } else {
                        field_times.insert(arrow_field.name().to_string(), max_ts);
                    }
                }
            }
        }

        let fields = arrow_schema
            .fields()
            .iter()
            .filter_map(|arrow_field| {
                let field_name = arrow_field.name();
                if field_name == TIME_COLUMN_NAME {
                    None
                } else {
                    field_times.get(field_name).map(|ts| Field {
                        name: field_name.to_string(),
                        data_type: arrow_field.data_type().clone(),
                        last_timestamp: *ts,
                    })
                }
            })
            .collect();

        Ok(FieldList { fields })
    }
}

/// Merge several FieldLists into a single field list, merging the
/// entries appropriately
// Clippy gets confused and tells me that I should be using Self
// instead of Vec even though the type of Vec being created is different
#[allow(clippy::use_self)]
impl IntoFieldList for Vec<FieldList> {
    fn into_fieldlist(self) -> Result<FieldList> {
        if self.is_empty() {
            return Ok(FieldList::default());
        }

        // otherwise merge the fields together
        let mut field_map = BTreeMap::<String, Field>::new();

        // iterate over all fields
        let field_iter = self.into_iter().flat_map(|f| f.fields.into_iter());

        for new_field in field_iter {
            if let Some(existing_field) = field_map.get_mut(&new_field.name) {
                ensure!(
                    existing_field.data_type == new_field.data_type,
                    InconsistentFieldTypeSnafu {
                        field_name: new_field.name,
                        data_type1: existing_field.data_type.clone(),
                        data_type2: new_field.data_type,
                    }
                );
                existing_field.last_timestamp =
                    std::cmp::max(existing_field.last_timestamp, new_field.last_timestamp);
            }
            // no entry for field yet
            else {
                field_map.insert(new_field.name.clone(), new_field);
            }
        }

        let mut fields = field_map.into_values().collect::<Vec<_>>();
        fields.sort_by(|a, b| a.name.cmp(&b.name));

        Ok(FieldList { fields })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use arrow::array::ArrayRef;
    use arrow::{
        array::{Int64Array, StringArray},
        datatypes::{DataType as ArrowDataType, Field as ArrowField, Schema},
    };
    use schema::TIME_DATA_TYPE;

    #[test]
    fn test_convert_single_batch() {
        let schema = Arc::new(Schema::new(vec![
            ArrowField::new("string_field", ArrowDataType::Utf8, true),
            ArrowField::new("time", TIME_DATA_TYPE(), true),
        ]));

        let string_array: ArrayRef = Arc::new(StringArray::from(vec!["foo", "bar", "baz", "foo"]));
        let timestamp_array: ArrayRef = Arc::new(TimestampNanosecondArray::from_iter_values(vec![
            1000, 2000, 3000, 4000,
        ]));

        let actual = do_conversion(
            Arc::clone(&schema),
            vec![vec![string_array, timestamp_array]],
        )
        .expect("convert correctly");

        let expected = FieldList {
            fields: vec![Field {
                name: "string_field".into(),
                data_type: ArrowDataType::Utf8,
                last_timestamp: 4000,
            }],
        };

        assert_eq!(
            expected, actual,
            "Expected:\n{:#?}\nActual:\n{:#?}",
            expected, actual
        );

        // expect same even if the timestamp order is different

        let string_array: ArrayRef = Arc::new(StringArray::from(vec!["foo", "bar", "baz", "foo"]));
        let timestamp_array: ArrayRef = Arc::new(TimestampNanosecondArray::from_iter_values(vec![
            1000, 4000, 2000, 3000,
        ]));

        let actual = do_conversion(schema, vec![vec![string_array, timestamp_array]])
            .expect("convert correctly");

        assert_eq!(
            expected, actual,
            "Expected:\n{:#?}\nActual:\n{:#?}",
            expected, actual
        );
    }

    #[test]
    fn test_convert_two_batches() {
        let schema = Arc::new(Schema::new(vec![
            ArrowField::new("string_field", ArrowDataType::Utf8, true),
            ArrowField::new("time", TIME_DATA_TYPE(), true),
        ]));

        let string_array1: ArrayRef = Arc::new(StringArray::from(vec!["foo", "bar"]));
        let timestamp_array1: ArrayRef =
            Arc::new(TimestampNanosecondArray::from_iter_values(vec![1000, 3000]));

        let string_array2: ArrayRef = Arc::new(StringArray::from(vec!["foo", "foo"]));
        let timestamp_array2: ArrayRef =
            Arc::new(TimestampNanosecondArray::from_iter_values(vec![1000, 4000]));

        let actual = do_conversion(
            schema,
            vec![
                vec![string_array1, timestamp_array1],
                vec![string_array2, timestamp_array2],
            ],
        )
        .expect("convert correctly");

        let expected = FieldList {
            fields: vec![Field {
                name: "string_field".into(),
                data_type: ArrowDataType::Utf8,
                last_timestamp: 4000,
            }],
        };

        assert_eq!(
            expected, actual,
            "Expected:\n{:#?}\nActual:\n{:#?}",
            expected, actual
        );
    }

    #[test]
    fn test_convert_all_nulls() {
        let schema = Arc::new(Schema::new(vec![
            ArrowField::new("string_field", ArrowDataType::Utf8, true),
            ArrowField::new("time", TIME_DATA_TYPE(), true),
        ]));

        // string array has no actual values, so should not be returned as a field
        let string_array: ArrayRef = Arc::new(StringArray::from(vec![None, None, None, None]));
        let timestamp_array: ArrayRef = Arc::new(TimestampNanosecondArray::from_iter_values(vec![
            1000, 2000, 3000, 4000,
        ]));

        let actual = do_conversion(schema, vec![vec![string_array, timestamp_array]])
            .expect("convert correctly");

        let expected = FieldList { fields: vec![] };

        assert_eq!(
            expected, actual,
            "Expected:\n{:#?}\nActual:\n{:#?}",
            expected, actual
        );
    }

    // test three columns, with different data types and null
    #[test]
    fn test_multi_column_multi_datatype() {
        let schema = Arc::new(Schema::new(vec![
            ArrowField::new("string_field", ArrowDataType::Utf8, true),
            ArrowField::new("int_field", ArrowDataType::Int64, true),
            ArrowField::new("time", TIME_DATA_TYPE(), true),
        ]));

        let string_array: ArrayRef = Arc::new(StringArray::from(vec!["foo", "bar", "baz", "foo"]));
        let int_array: ArrayRef =
            Arc::new(Int64Array::from(vec![Some(10), Some(20), Some(30), None]));
        let timestamp_array: ArrayRef = Arc::new(TimestampNanosecondArray::from_iter_values(vec![
            1000, 2000, 3000, 4000,
        ]));

        let expected = FieldList {
            fields: vec![
                Field {
                    name: "string_field".into(),
                    data_type: ArrowDataType::Utf8,
                    last_timestamp: 4000,
                },
                Field {
                    name: "int_field".into(),
                    data_type: ArrowDataType::Int64,
                    last_timestamp: 3000,
                },
            ],
        };

        let actual = do_conversion(schema, vec![vec![string_array, int_array, timestamp_array]])
            .expect("conversion successful");

        assert_eq!(
            expected, actual,
            "Expected:\n{:#?}\nActual:\n{:#?}",
            expected, actual
        );
    }

    fn do_conversion(schema: SchemaRef, value_arrays: Vec<Vec<ArrayRef>>) -> Result<FieldList> {
        let batches = value_arrays
            .into_iter()
            .map(|arrays| {
                RecordBatch::try_new(Arc::clone(&schema), arrays).expect("created new record batch")
            })
            .collect::<Vec<_>>();

        batches.into_fieldlist()
    }

    #[test]
    fn test_merge_field_list() {
        let field1 = Field {
            name: "one".into(),
            data_type: ArrowDataType::Utf8,
            last_timestamp: 4000,
        };
        let field2 = Field {
            name: "two".into(),
            data_type: ArrowDataType::Int64,
            last_timestamp: 3000,
        };

        let l1 = FieldList {
            fields: vec![field1, field2.clone()],
        };
        let actual = vec![l1.clone()].into_fieldlist().unwrap();
        let expected = l1.clone();

        assert_eq!(
            expected, actual,
            "Expected:\n{:#?}\nActual:\n{:#?}",
            expected, actual
        );

        let field1_later = Field {
            name: "one".into(),
            data_type: ArrowDataType::Utf8,
            last_timestamp: 5000,
        };

        // use something that has a later timestamp and expect the later one takes
        // precedence
        let l2 = FieldList {
            fields: vec![field1_later.clone()],
        };
        let actual = vec![l1.clone(), l2.clone()].into_fieldlist().unwrap();
        let expected = FieldList {
            fields: vec![field1_later, field2],
        };

        assert_eq!(
            expected, actual,
            "Expected:\n{:#?}\nActual:\n{:#?}",
            expected, actual
        );

        // Now, try to add a field that has a different type

        let field1_new_type = Field {
            name: "one".into(),
            data_type: ArrowDataType::Int64,
            last_timestamp: 5000,
        };

        // use something that has a later timestamp and expect the later one takes
        // precedence
        let l3 = FieldList {
            fields: vec![field1_new_type],
        };
        let actual = vec![l1, l2, l3].into_fieldlist();
        let actual_error = actual.expect_err("should be an error").to_string();

        let expected_error =
            "Inconsistent data type for field 'one': found both 'Utf8' and 'Int64'";

        assert!(
            actual_error.contains(expected_error),
            "Can not find expected '{}' in actual '{}'",
            expected_error,
            actual_error
        );
    }
}
