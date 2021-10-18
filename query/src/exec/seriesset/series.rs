//! This module contains the native Rust version of the Data frames
//! that are sent back in the storage gRPC format.

use std::{convert::TryFrom, fmt, sync::Arc};

use arrow::{
    array::{
        ArrayRef, BooleanArray, Float64Array, Int64Array, StringArray, TimestampNanosecondArray,
        UInt64Array,
    },
    bitmap::Bitmap,
    datatypes::DataType as ArrowDataType,
};

use crate::exec::{field::FieldIndex, seriesset::SeriesSet};
use snafu::Snafu;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unsupported data type while translating to Frames: {}", data_type))]
    UnsupportedDataType { data_type: ArrowDataType },

    #[snafu(display("Unsupported field data while translating to Frames: {}", data_type))]
    UnsupportedFieldType { data_type: ArrowDataType },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// A name=value pair used to represent a series's tag
#[derive(Debug)]
pub struct Tag {
    pub key: Arc<str>,
    pub value: Arc<str>,
}

impl fmt::Display for Tag {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}={}", self.key, self.value)
    }
}

/// Represents a single logical TimeSeries
#[derive(Debug)]
pub struct Series {
    /// key = value pairs that define this series
    /// (including the _measurement and _field that correspond to table name and column name)
    pub tags: Vec<Tag>,

    /// The raw data for this series
    pub data: Data,
}

impl fmt::Display for Series {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Series tags={{")?;
        let mut first = true;
        self.tags.iter().try_for_each(|tag| {
            if !first {
                write!(f, ", ")?;
            } else {
                first = false;
            }
            write!(f, "{}", tag)
        })?;
        writeln!(f, "}}")?;
        write!(f, "  {}", self.data)
    }
}

/// Typed data for a particular timeseries
#[derive(Debug)]
pub enum Data {
    FloatPoints {
        timestamps: Vec<i64>,
        values: Vec<f64>,
    },

    IntegerPoints {
        timestamps: Vec<i64>,
        values: Vec<i64>,
    },

    UnsignedPoints {
        timestamps: Vec<i64>,
        values: Vec<u64>,
    },

    BooleanPoints {
        timestamps: Vec<i64>,
        values: Vec<bool>,
    },

    StringPoints {
        timestamps: Vec<i64>,
        values: Vec<String>,
    },
}

impl fmt::Display for Data {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::FloatPoints { timestamps, values } => write!(
                f,
                "FloatPoints timestamps: {:?}, values: {:?}",
                timestamps, values
            ),
            Self::IntegerPoints { timestamps, values } => write!(
                f,
                "IntegerPoints timestamps: {:?}, values: {:?}",
                timestamps, values
            ),
            Self::UnsignedPoints { timestamps, values } => write!(
                f,
                "UnsignedPoints timestamps: {:?}, values: {:?}",
                timestamps, values
            ),
            Self::BooleanPoints { timestamps, values } => write!(
                f,
                "BooleanPoints timestamps: {:?}, values: {:?}",
                timestamps, values
            ),
            Self::StringPoints { timestamps, values } => write!(
                f,
                "StringPoints timestamps: {:?}, values: {:?}",
                timestamps, values
            ),
        }
    }
}

impl TryFrom<SeriesSet> for Vec<Series> {
    type Error = Error;

    /// Converts a particular SeriesSet into a Vec of Series. Note the
    /// order is important
    fn try_from(value: SeriesSet) -> Result<Self, Self::Error> {
        value
            .field_indexes
            .iter()
            .filter_map(|index| value.field_to_series(index).transpose())
            .collect()
    }
}

impl SeriesSet {
    /// Returns true if the array is entirely null between start_row and
    /// start_row+num_rows
    fn is_all_null(arr: &ArrayRef, start_row: usize, num_rows: usize) -> bool {
        let end_row = start_row + num_rows;
        (start_row..end_row).all(|i| arr.is_null(i))
    }

    // Convert and append the values from a single field to a Series
    // appended to `frames`
    fn field_to_series(&self, index: &FieldIndex) -> Result<Option<Series>> {
        let batch = &self.batch;
        let schema = batch.schema();

        let field = schema.field(index.value_index);
        let array = batch.column(index.value_index);

        let start_row = self.start_row;
        let num_rows = self.num_rows;

        // No values for this field are in the array so it does not
        // contribute to a series.
        if field.is_nullable() && Self::is_all_null(array, start_row, num_rows) {
            return Ok(None);
        }

        let tags = self.create_frame_tags(schema.field(index.value_index).name());

        // Only take timestamps (and values) from the rows that have non
        // null values for this field
        let valid = array.data().null_bitmap().as_ref();

        let timestamps = batch
            .column(index.timestamp_index)
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .unwrap()
            .extract_values(start_row, num_rows, valid);

        let data = match array.data_type() {
            ArrowDataType::Utf8 => {
                let values = array
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .extract_values(start_row, num_rows, valid);
                Data::StringPoints { timestamps, values }
            }
            ArrowDataType::Float64 => {
                let values = array
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap()
                    .extract_values(start_row, num_rows, valid);

                Data::FloatPoints { timestamps, values }
            }
            ArrowDataType::Int64 => {
                let values = array
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .extract_values(start_row, num_rows, valid);
                Data::IntegerPoints { timestamps, values }
            }
            ArrowDataType::UInt64 => {
                let values = array
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .unwrap()
                    .extract_values(start_row, num_rows, valid);
                Data::UnsignedPoints { timestamps, values }
            }
            ArrowDataType::Boolean => {
                let values = array
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .unwrap()
                    .extract_values(start_row, num_rows, valid);
                Data::BooleanPoints { timestamps, values }
            }
            _ => {
                return UnsupportedDataType {
                    data_type: array.data_type().clone(),
                }
                .fail();
            }
        };

        Ok(Some(Series { tags, data }))
    }

    /// Create the tag=value pairs for this series set, adding
    /// adding the _f and _m tags for the field name and measurement
    fn create_frame_tags(&self, field_name: &str) -> Vec<Tag> {
        // Special case "measurement" name which is modeled as a tag of
        // "_measurement" and "field" which is modeled as a tag of "_field"
        //
        // Note by placing these tags at the front of the keys, it
        // means the output will be sorted first by _field and then
        // _measurement even when there are no groups requested
        let mut converted_tags = vec![
            Tag {
                key: "_field".into(),
                value: field_name.into(),
            },
            Tag {
                key: "_measurement".into(),
                value: Arc::clone(&self.table_name),
            },
        ];

        // convert the rest of the tags
        converted_tags.extend(self.tags.iter().map(|(k, v)| Tag {
            key: Arc::clone(k),
            value: Arc::clone(v),
        }));

        converted_tags
    }
}

/// Represents a group of `Series`
#[derive(Debug, Default)]
pub struct Group {
    /// Contains *ALL* tag keys (not just those used for grouping)
    pub tag_keys: Vec<Arc<str>>,

    /// Contains the values that define the group (may be values from
    /// fields other than tags).
    ///
    /// the values of the group tags that defined the group.
    /// For example,
    ///
    /// If there were tags `t0`, `t1`, and `t2`, and the query had
    /// group_keys of `[t1, t2]` then this list would have the values
    /// of the t1 and t2 columns
    pub partition_key_vals: Vec<Arc<str>>,
}

impl fmt::Display for Group {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Group tag_keys: ")?;
        fmt_strings(f, &self.tag_keys)?;
        write!(f, " partition_key_vals: ")?;
        fmt_strings(f, &self.partition_key_vals)?;
        Ok(())
    }
}

#[derive(Debug)]
pub enum Either {
    Series(Series),
    Group(Group),
}

impl From<Series> for Either {
    fn from(value: Series) -> Self {
        Self::Series(value)
    }
}

impl From<Group> for Either {
    fn from(value: Group) -> Self {
        Self::Group(value)
    }
}

impl fmt::Display for Either {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Series(series) => series.fmt(f),
            Self::Group(group) => group.fmt(f),
        }
    }
}

fn fmt_strings(f: &mut fmt::Formatter<'_>, strings: &[Arc<str>]) -> fmt::Result {
    let mut first = true;
    strings.iter().try_for_each(|item| {
        if !first {
            write!(f, ", ")?;
        } else {
            first = false;
        }
        write!(f, "{}", item)
    })
}

trait ExtractValues<T> {
    /// Extracts num_rows of data starting from start_row as a vector,
    /// for all rows `i` where `valid[i]` is set
    fn extract_values(&self, start_row: usize, num_rows: usize, valid: Option<&Bitmap>) -> Vec<T>;
}

/// Implements extract_values for a particular type of array that
macro_rules! extract_values_impl {
    ($DATA_TYPE:ty) => {
        fn extract_values(
            &self,
            start_row: usize,
            num_rows: usize,
            valid: Option<&Bitmap>,
        ) -> Vec<$DATA_TYPE> {
            let end_row = start_row + num_rows;
            match valid {
                Some(valid) => (start_row..end_row)
                    .filter_map(|row| valid.is_set(row).then(|| self.value(row)))
                    .collect(),
                None => (start_row..end_row).map(|row| self.value(row)).collect(),
            }
        }
    };
}

impl ExtractValues<String> for StringArray {
    fn extract_values(
        &self,
        start_row: usize,
        num_rows: usize,
        valid: Option<&Bitmap>,
    ) -> Vec<String> {
        let end_row = start_row + num_rows;
        match valid {
            Some(valid) => (start_row..end_row)
                .filter_map(|row| valid.is_set(row).then(|| self.value(row).to_string()))
                .collect(),
            None => (start_row..end_row)
                .map(|row| self.value(row).to_string())
                .collect(),
        }
    }
}

impl ExtractValues<i64> for Int64Array {
    extract_values_impl! {i64}
}

impl ExtractValues<u64> for UInt64Array {
    extract_values_impl! {u64}
}

impl ExtractValues<f64> for Float64Array {
    extract_values_impl! {f64}
}

impl ExtractValues<bool> for BooleanArray {
    extract_values_impl! {bool}
}

impl ExtractValues<i64> for TimestampNanosecondArray {
    extract_values_impl! {i64}
}

#[cfg(test)]
mod tests {
    use std::convert::TryInto;

    use crate::exec::field::FieldIndexes;
    use arrow::record_batch::RecordBatch;

    use super::*;

    fn series_set_to_series_strings(series_set: SeriesSet) -> Vec<String> {
        let series: Vec<Series> = series_set.try_into().unwrap();

        let series: Vec<String> = series.into_iter().map(|s| s.to_string()).collect();

        series
            .iter()
            .map(|s| s.split('\n'))
            .flatten()
            .map(|s| s.to_string())
            .collect()
    }

    #[test]
    fn test_series_set_conversion() {
        let series_set = SeriesSet {
            table_name: Arc::from("the_table"),
            tags: vec![(Arc::from("tag1"), Arc::from("val1"))],
            field_indexes: FieldIndexes::from_timestamp_and_value_indexes(5, &[0, 1, 2, 3, 4]),
            start_row: 1,
            num_rows: 2,
            batch: make_record_batch(),
        };

        let series_strings = series_set_to_series_strings(series_set);

        let expected = vec![
            "Series tags={_field=string_field, _measurement=the_table, tag1=val1}",
            "  StringPoints timestamps: [2000, 3000], values: [\"bar\", \"baz\"]",
            "Series tags={_field=int_field, _measurement=the_table, tag1=val1}",
            "  IntegerPoints timestamps: [2000, 3000], values: [2, 3]",
            "Series tags={_field=uint_field, _measurement=the_table, tag1=val1}",
            "  UnsignedPoints timestamps: [2000, 3000], values: [22, 33]",
            "Series tags={_field=float_field, _measurement=the_table, tag1=val1}",
            "  FloatPoints timestamps: [2000, 3000], values: [20.1, 30.1]",
            "Series tags={_field=boolean_field, _measurement=the_table, tag1=val1}",
            "  BooleanPoints timestamps: [2000, 3000], values: [false, true]",
        ];

        assert_eq!(
            series_strings, expected,
            "Expected:\n{:#?}\nActual:\n{:#?}",
            expected, series_strings
        );
    }

    #[test]
    fn test_series_set_conversion_different_time_columns() {
        let time1_array: ArrayRef =
            Arc::new(TimestampNanosecondArray::from_vec(vec![1, 2, 3], None));
        let string1_array: ArrayRef = Arc::new(StringArray::from(vec!["foo", "bar", "baz"]));
        let time2_array: ArrayRef =
            Arc::new(TimestampNanosecondArray::from_vec(vec![3, 4, 5], None));
        let string2_array: ArrayRef = Arc::new(StringArray::from(vec!["boo", "far", "faz"]));

        let batch = RecordBatch::try_from_iter(vec![
            ("time1", time1_array as ArrayRef),
            ("string_field1", string1_array),
            ("time2", time2_array),
            ("string_field2", string2_array),
        ])
        .expect("created new record batch");

        let series_set = SeriesSet {
            table_name: Arc::from("the_table"),
            tags: vec![(Arc::from("tag1"), Arc::from("val1"))],
            // field indexes are (value, time)
            field_indexes: FieldIndexes::from_slice(&[(3, 2), (1, 0)]),
            start_row: 1,
            num_rows: 2,
            batch,
        };

        let series_strings = series_set_to_series_strings(series_set);

        let expected = vec![
            "Series tags={_field=string_field2, _measurement=the_table, tag1=val1}",
            "  StringPoints timestamps: [4, 5], values: [\"far\", \"faz\"]",
            "Series tags={_field=string_field1, _measurement=the_table, tag1=val1}",
            "  StringPoints timestamps: [2, 3], values: [\"bar\", \"baz\"]",
        ];

        assert_eq!(
            series_strings, expected,
            "Expected:\n{:#?}\nActual:\n{:#?}",
            expected, series_strings
        );
    }

    #[test]
    fn test_series_set_conversion_with_entirely_null_field() {
        // single series
        let tag_array: ArrayRef = Arc::new(StringArray::from(vec!["MA", "MA", "MA", "MA"]));
        let int_array: ArrayRef = Arc::new(Int64Array::from(vec![None, None, None, None]));
        let float_array: ArrayRef = Arc::new(Float64Array::from(vec![
            Some(10.1),
            Some(20.1),
            None,
            Some(40.1),
        ]));

        let timestamp_array: ArrayRef = Arc::new(TimestampNanosecondArray::from_vec(
            vec![1000, 2000, 3000, 4000],
            None,
        ));

        let batch = RecordBatch::try_from_iter_with_nullable(vec![
            ("state", tag_array, true),
            ("int_field", int_array, true),
            ("float_field", float_array, true),
            ("time", timestamp_array, false),
        ])
        .expect("created new record batch");

        let series_set = SeriesSet {
            table_name: Arc::from("the_table"),
            tags: vec![(Arc::from("state"), Arc::from("MA"))],
            field_indexes: FieldIndexes::from_timestamp_and_value_indexes(3, &[1, 2]),
            start_row: 0,
            num_rows: batch.num_rows(),
            batch,
        };

        // Expect only a single series (for the data in float_field, int_field is all
        // nulls)
        let series_strings = series_set_to_series_strings(series_set);

        let expected = vec![
            "Series tags={_field=float_field, _measurement=the_table, state=MA}",
            "  FloatPoints timestamps: [1000, 2000, 4000], values: [10.1, 20.1, 40.1]",
        ];

        assert_eq!(
            series_strings, expected,
            "Expected:\n{:#?}\nActual:\n{:#?}",
            expected, series_strings
        );
    }

    #[test]
    fn test_series_set_conversion_with_some_null_fields() {
        // single series
        let tag_array = StringArray::from(vec!["MA", "MA"]);
        let string_array = StringArray::from(vec![None, Some("foo")]);
        let float_array = Float64Array::from(vec![None, Some(1.0)]);
        let int_array = Int64Array::from(vec![None, Some(-10)]);
        let uint_array = UInt64Array::from(vec![None, Some(100)]);
        let bool_array = BooleanArray::from(vec![None, Some(true)]);

        let timestamp_array = TimestampNanosecondArray::from_vec(vec![1000, 2000], None);

        let batch = RecordBatch::try_from_iter_with_nullable(vec![
            ("state", Arc::new(tag_array) as ArrayRef, true),
            ("srting_field", Arc::new(string_array), true),
            ("float_field", Arc::new(float_array), true),
            ("int_field", Arc::new(int_array), true),
            ("uint_field", Arc::new(uint_array), true),
            ("bool_field", Arc::new(bool_array), true),
            ("time", Arc::new(timestamp_array), false),
        ])
        .expect("created new record batch");

        let series_set = SeriesSet {
            table_name: Arc::from("the_table"),
            tags: vec![(Arc::from("state"), Arc::from("MA"))],
            field_indexes: FieldIndexes::from_timestamp_and_value_indexes(6, &[1, 2, 3, 4, 5]),
            start_row: 0,
            num_rows: batch.num_rows(),
            batch,
        };

        // Expect only a single series (for the data in float_field, int_field is all
        // nulls)
        let series_strings = series_set_to_series_strings(series_set);

        let expected = vec![
            "Series tags={_field=srting_field, _measurement=the_table, state=MA}",
            "  StringPoints timestamps: [2000], values: [\"foo\"]",
            "Series tags={_field=float_field, _measurement=the_table, state=MA}",
            "  FloatPoints timestamps: [2000], values: [1.0]",
            "Series tags={_field=int_field, _measurement=the_table, state=MA}",
            "  IntegerPoints timestamps: [2000], values: [-10]",
            "Series tags={_field=uint_field, _measurement=the_table, state=MA}",
            "  UnsignedPoints timestamps: [2000], values: [100]",
            "Series tags={_field=bool_field, _measurement=the_table, state=MA}",
            "  BooleanPoints timestamps: [2000], values: [true]",
        ];

        assert_eq!(
            series_strings, expected,
            "Expected:\n{:#?}\nActual:\n{:#?}",
            expected, series_strings
        );
    }

    fn make_record_batch() -> RecordBatch {
        let string_array: ArrayRef = Arc::new(StringArray::from(vec!["foo", "bar", "baz", "foo"]));
        let int_array: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 3, 4]));
        let uint_array: ArrayRef = Arc::new(UInt64Array::from(vec![11, 22, 33, 44]));
        let float_array: ArrayRef = Arc::new(Float64Array::from(vec![10.1, 20.1, 30.1, 40.1]));
        let bool_array: ArrayRef = Arc::new(BooleanArray::from(vec![true, false, true, false]));

        let timestamp_array: ArrayRef = Arc::new(TimestampNanosecondArray::from_vec(
            vec![1000, 2000, 3000, 4000],
            None,
        ));

        RecordBatch::try_from_iter_with_nullable(vec![
            ("string_field", string_array, true),
            ("int_field", int_array, true),
            ("uint_field", uint_array, true),
            ("float_field", float_array, true),
            ("boolean_field", bool_array, true),
            ("time", timestamp_array, true),
        ])
        .expect("created new record batch")
    }
}
