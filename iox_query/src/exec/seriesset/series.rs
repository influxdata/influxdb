//! This module contains the native Rust version of the Data frames
//! that are sent back in the storage gRPC format.

use std::{fmt, sync::Arc};

use arrow::{
    array::{
        Array, ArrayRef, BooleanArray, Float64Array, Int64Array, StringArray,
        TimestampNanosecondArray, UInt64Array,
    },
    compute,
    datatypes::DataType as ArrowDataType,
};
use predicate::rpc_predicate::{FIELD_COLUMN_NAME, MEASUREMENT_COLUMN_NAME};

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
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Tag {
    pub key: Arc<str>,
    pub value: Arc<str>,
}

impl Tag {
    /// Memory usage in bytes, including `self`.
    pub fn size(&self) -> usize {
        std::mem::size_of_val(self) + self.key.len() + self.value.len()
    }
}

impl fmt::Display for Tag {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}={}", self.key, self.value)
    }
}

/// Represents a single logical TimeSeries
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Series {
    /// key = value pairs that define this series
    /// (including the _measurement and _field that correspond to table name and column name)
    pub tags: Vec<Tag>,

    /// The raw data for this series
    pub data: Data,
}

impl Series {
    pub fn num_batches(&self) -> usize {
        match &self.data {
            Data::FloatPoints(batches) => batches.len(),
            Data::IntegerPoints(batches) => batches.len(),
            Data::UnsignedPoints(batches) => batches.len(),
            Data::BooleanPoints(batches) => batches.len(),
            Data::StringPoints(batches) => batches.len(),
        }
    }

    /// Memory usage in bytes, including `self`.
    pub fn size(&self) -> usize {
        std::mem::size_of_val(self)
            + (std::mem::size_of::<Tag>() * self.tags.capacity())
            + self
                .tags
                .iter()
                .map(|tag| tag.size() - std::mem::size_of_val(tag))
                .sum::<usize>()
            + self.data.size()
            - std::mem::size_of_val(&self.data)
    }
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
            write!(f, "{tag}")
        })?;
        writeln!(f, "}}")?;
        write!(f, "  {}", self.data)
    }
}

/// Typed data for a particular timeseries
#[derive(Clone, Debug)]
pub enum Data {
    FloatPoints(Vec<Batch<f64>>),
    IntegerPoints(Vec<Batch<i64>>),
    UnsignedPoints(Vec<Batch<u64>>),
    BooleanPoints(Vec<Batch<bool>>),
    StringPoints(Vec<Batch<String>>),
}

impl Data {
    /// Memory usage in bytes, including `self`.
    pub fn size(&self) -> usize {
        let data_sz: usize = match self {
            Self::FloatPoints(points_vec) => points_vec.iter().map(|ps| ps.size()).sum(),
            Self::IntegerPoints(points_vec) => points_vec.iter().map(|ps| ps.size()).sum(),
            Self::UnsignedPoints(points_vec) => points_vec.iter().map(|ps| ps.size()).sum(),
            Self::BooleanPoints(points_vec) => points_vec.iter().map(|ps| ps.size()).sum(),
            Self::StringPoints(points_vec) => points_vec.iter().map(|ps| ps.size()).sum(),
        };
        std::mem::size_of_val(self) + data_sz
    }
}

impl PartialEq for Data {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::FloatPoints(l_batches), Self::FloatPoints(r_batches)) => l_batches == r_batches,
            (Self::IntegerPoints(l_batches), Self::IntegerPoints(r_batches)) => {
                l_batches == r_batches
            }
            (Self::UnsignedPoints(l_batches), Self::UnsignedPoints(r_batches)) => {
                l_batches == r_batches
            }
            (Self::BooleanPoints(l_batches), Self::BooleanPoints(r_batches)) => {
                l_batches == r_batches
            }
            (Self::StringPoints(l_batches), Self::StringPoints(r_batches)) => {
                l_batches == r_batches
            }
            _ => false,
        }
    }
}

impl Eq for Data {}

/// Returns size of given vector of primitive types in bytes, EXCLUDING `vec` itself.
fn primitive_vec_size<T>(vec: &Vec<T>) -> usize {
    std::mem::size_of::<T>() * vec.capacity()
}

impl fmt::Display for Data {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::FloatPoints(batches) => write!(f, "FloatPoints batches: {batches:?}"),
            Self::IntegerPoints(batches) => write!(f, "IntegerPoints batches: {batches:?}"),
            Self::UnsignedPoints(batches) => write!(f, "UnsignedPoints batches: {batches:?}"),
            Self::BooleanPoints(batches) => write!(f, "BooleanPoints batches: {batches:?}"),
            Self::StringPoints(batches) => write!(f, "StringPoints batches: {batches:?}"),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct Batch<T> {
    pub timestamps: Vec<i64>,
    pub values: Vec<T>,
}

impl<T> Batch<T> {
    fn size(&self) -> usize {
        std::mem::size_of_val(self)
            + primitive_vec_size(&self.timestamps)
            + primitive_vec_size(&self.values)
    }
}

impl SeriesSet {
    /// Returns true if the array is entirely null between start_row and
    /// start_row+num_rows
    fn is_all_null(arr: &ArrayRef) -> bool {
        arr.null_count() == arr.len()
    }

    pub fn is_timestamp_all_null(&self) -> bool {
        self.field_indexes.iter().all(|field_index| {
            let array = self.batch.column(field_index.timestamp_index);
            Self::is_all_null(array)
        })
    }

    pub fn try_into_series(self, batch_size: usize) -> Result<Vec<Series>> {
        self.field_indexes
            .iter()
            .filter_map(|index| self.field_to_series(index, batch_size).transpose())
            .collect()
    }

    // Convert and append the values from a single field to a Series
    // appended to `frames`
    fn field_to_series(&self, index: &FieldIndex, batch_size: usize) -> Result<Option<Series>> {
        let batch = self.batch.slice(self.start_row, self.num_rows);
        let schema = batch.schema();

        let field = schema.field(index.value_index);
        let array = batch.column(index.value_index);

        // No values for this field are in the array so it does not
        // contribute to a series.
        if field.is_nullable() && Self::is_all_null(array) {
            return Ok(None);
        }

        let tags = self.create_frame_tags(schema.field(index.value_index).name());

        let mut timestamps = compute::kernels::nullif::nullif(
            batch.column(index.timestamp_index),
            &compute::is_null(array).expect("is_null"),
        )
        .expect("null handling")
        .as_any()
        .downcast_ref::<TimestampNanosecondArray>()
        .unwrap()
        .extract_batched_values(batch_size);
        timestamps.shrink_to_fit();

        let data = match array.data_type() {
            ArrowDataType::Utf8 => {
                let values = array
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .extract_batched_values(batch_size);
                Data::StringPoints(build_batches(timestamps, values))
            }
            ArrowDataType::Float64 => {
                let values = array
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap()
                    .extract_batched_values(batch_size);
                Data::FloatPoints(build_batches(timestamps, values))
            }
            ArrowDataType::Int64 => {
                let values = array
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .extract_batched_values(batch_size);
                Data::IntegerPoints(build_batches(timestamps, values))
            }
            ArrowDataType::UInt64 => {
                let values = array
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .unwrap()
                    .extract_batched_values(batch_size);
                Data::UnsignedPoints(build_batches(timestamps, values))
            }
            ArrowDataType::Boolean => {
                let values = array
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .unwrap()
                    .extract_batched_values(batch_size);
                Data::BooleanPoints(build_batches(timestamps, values))
            }
            _ => {
                return UnsupportedDataTypeSnafu {
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
        // Add special _field and _measurement tags and return them in
        // lexicographical (sorted) order

        let mut all_tags = self
            .tags
            .iter()
            .cloned()
            .chain([
                (Arc::from(FIELD_COLUMN_NAME), Arc::from(field_name)),
                (
                    Arc::from(MEASUREMENT_COLUMN_NAME),
                    Arc::clone(&self.table_name),
                ),
            ])
            .collect::<Vec<_>>();

        // sort by name
        all_tags.sort_by(|(key1, _value), (key2, _value2)| key1.cmp(key2));

        all_tags
            .into_iter()
            .map(|(key, value)| Tag { key, value })
            .collect()
    }
}

/// Zip together nested vectors of timestamps and values to create batches of points
fn build_batches<T>(timestamps: Vec<Vec<i64>>, values: Vec<Vec<T>>) -> Vec<Batch<T>> {
    timestamps
        .into_iter()
        .zip(values)
        .map(|(timestamps, values)| Batch { timestamps, values })
        .collect()
}

/// Represents a group of `Series`
#[derive(Clone, Debug, Default, PartialEq, Eq)]
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

#[derive(Clone, Debug, PartialEq, Eq)]
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
        write!(f, "{item}")
    })
}

trait ExtractBatchedValues<T> {
    /// Extracts rows as a vector,
    /// for all rows `i` where `valid[i]` is set
    fn extract_batched_values(&self, batch_size: usize) -> Vec<Vec<T>>;
}

/// Implements extract_batched_values for Arrow arrays.
macro_rules! extract_batched_values_impl {
    ($DATA_TYPE:ty) => {
        extract_batched_values_impl! { $DATA_TYPE, identity }
    };
    ($DATA_TYPE:ty, $ITER_ADAPTER:expr) => {
        fn extract_batched_values(&self, batch_size: usize) -> Vec<Vec<$DATA_TYPE>> {
            let num_batches = 1 + self.len() / batch_size;
            let mut batches = Vec::with_capacity(num_batches);

            let mut v = Vec::with_capacity(batch_size);
            for e in $ITER_ADAPTER(self.iter().flatten()) {
                if v.len() >= batch_size {
                    batches.push(v);
                    v = Vec::with_capacity(batch_size);
                }
                v.push(e);
            }
            if !v.is_empty() {
                v.shrink_to_fit();
                batches.push(v);
            }
            batches.shrink_to_fit();
            batches
        }
    };
}

fn identity<T>(t: T) -> T {
    t
}

fn to_owned_string<'a, I>(i: I) -> impl Iterator<Item = String>
where
    I: Iterator<Item = &'a str>,
{
    i.map(str::to_string)
}

impl ExtractBatchedValues<String> for StringArray {
    extract_batched_values_impl! { String,  to_owned_string }
}

impl ExtractBatchedValues<i64> for Int64Array {
    extract_batched_values_impl! {i64}
}

impl ExtractBatchedValues<u64> for UInt64Array {
    extract_batched_values_impl! {u64}
}

impl ExtractBatchedValues<f64> for Float64Array {
    extract_batched_values_impl! {f64}
}

impl ExtractBatchedValues<bool> for BooleanArray {
    extract_batched_values_impl! {bool}
}

impl ExtractBatchedValues<i64> for TimestampNanosecondArray {
    extract_batched_values_impl! {i64}
}

#[cfg(test)]
mod tests {
    use crate::exec::field::FieldIndexes;
    use arrow::{compute::concat_batches, record_batch::RecordBatch};

    use super::*;

    fn series_set_to_series_strings(series_set: SeriesSet, batch_size: usize) -> Vec<String> {
        let series: Vec<Series> = series_set.try_into_series(batch_size).unwrap();

        let series: Vec<String> = series.into_iter().map(|s| s.to_string()).collect();

        series
            .iter()
            .flat_map(|s| s.split('\n'))
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
            num_rows: 4,
            batch: make_record_batch(),
        };

        let series_strings = series_set_to_series_strings(series_set, 3);

        let expected = vec![
            "Series tags={_field=string_field, _measurement=the_table, tag1=val1}",
            "  StringPoints batches: [Batch { timestamps: [2000, 3000, 4000], values: [\"bar\", \"baz\", \"bar\"] }, Batch { timestamps: [5000], values: [\"baz\"] }]",
            "Series tags={_field=int_field, _measurement=the_table, tag1=val1}",
            "  IntegerPoints batches: [Batch { timestamps: [2000, 3000, 4000], values: [2, 3, 4] }, Batch { timestamps: [5000], values: [5] }]",
            "Series tags={_field=uint_field, _measurement=the_table, tag1=val1}",
            "  UnsignedPoints batches: [Batch { timestamps: [2000, 3000, 4000], values: [22, 33, 44] }, Batch { timestamps: [5000], values: [55] }]",
            "Series tags={_field=float_field, _measurement=the_table, tag1=val1}",
            "  FloatPoints batches: [Batch { timestamps: [2000, 3000, 4000], values: [20.1, 30.1, 40.1] }, Batch { timestamps: [5000], values: [50.1] }]",
            "Series tags={_field=boolean_field, _measurement=the_table, tag1=val1}",
            "  BooleanPoints batches: [Batch { timestamps: [2000, 3000, 4000], values: [false, true, false] }, Batch { timestamps: [5000], values: [true] }]",
        ];

        assert_eq!(
            series_strings, expected,
            "Expected:\n{expected:#?}\nActual:\n{series_strings:#?}"
        );
    }

    #[test]
    fn test_series_set_conversion_mixed_case_tags() {
        let time1_array: ArrayRef = Arc::new(TimestampNanosecondArray::from(vec![1, 2, 3]));
        let string1_array: ArrayRef = Arc::new(StringArray::from(vec!["foo", "bar", "baz"]));

        let batch = RecordBatch::try_from_iter(vec![
            ("time1", time1_array as ArrayRef),
            ("string_field1", string1_array),
        ])
        .expect("created new record batch");

        let series_set = SeriesSet {
            table_name: Arc::from("the_table"),
            tags: vec![
                (Arc::from("CAPITAL_TAG"), Arc::from("the_value")),
                (Arc::from("tag1"), Arc::from("val1")),
            ],
            // field indexes are (value, time)
            field_indexes: FieldIndexes::from_slice(&[(1, 0)]),
            start_row: 1,
            num_rows: 2,
            batch,
        };

        let series_strings = series_set_to_series_strings(series_set, 100);

        // expect  CAPITAL_TAG is before `_field` and `_measurement` tags
        // (as that is the correct lexicographical ordering)
        let expected = vec![
            "Series tags={CAPITAL_TAG=the_value, _field=string_field1, _measurement=the_table, tag1=val1}",
            "  StringPoints batches: [Batch { timestamps: [2, 3], values: [\"bar\", \"baz\"] }]",
        ];

        assert_eq!(
            series_strings, expected,
            "Expected:\n{expected:#?}\nActual:\n{series_strings:#?}"
        );
    }

    #[test]
    fn test_series_set_conversion_different_time_columns() {
        let time1_array: ArrayRef = Arc::new(TimestampNanosecondArray::from(vec![1, 2, 3]));
        let string1_array: ArrayRef = Arc::new(StringArray::from(vec!["foo", "bar", "baz"]));
        let time2_array: ArrayRef = Arc::new(TimestampNanosecondArray::from(vec![3, 4, 5]));
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

        let series_strings = series_set_to_series_strings(series_set, 100);

        let expected = vec![
            "Series tags={_field=string_field2, _measurement=the_table, tag1=val1}",
            "  StringPoints batches: [Batch { timestamps: [4, 5], values: [\"far\", \"faz\"] }]",
            "Series tags={_field=string_field1, _measurement=the_table, tag1=val1}",
            "  StringPoints batches: [Batch { timestamps: [2, 3], values: [\"bar\", \"baz\"] }]",
        ];

        assert_eq!(
            series_strings, expected,
            "Expected:\n{expected:#?}\nActual:\n{series_strings:#?}"
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

        let timestamp_array: ArrayRef =
            Arc::new(TimestampNanosecondArray::from(vec![1000, 2000, 3000, 4000]));

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
            batch: batch.clone(),
        };

        // Expect only a single series (for the data in float_field, int_field is all
        // nulls)
        let series_strings = series_set_to_series_strings(series_set, 100);

        let expected = vec![
            "Series tags={_field=float_field, _measurement=the_table, state=MA}",
            "  FloatPoints batches: [Batch { timestamps: [1000, 2000, 4000], values: [10.1, 20.1, 40.1] }]",
        ];

        assert_eq!(
            series_strings, expected,
            "Expected:\n{expected:#?}\nActual:\n{series_strings:#?}"
        );

        // Multi-batch case
        // We can just append record batches here because the tag field does not change
        let batch = repeat_batch(3, &batch);
        let series_set = SeriesSet {
            table_name: Arc::from("the_table"),
            tags: vec![(Arc::from("state"), Arc::from("MA"))],
            field_indexes: FieldIndexes::from_timestamp_and_value_indexes(3, &[1, 2]),
            start_row: 0,
            num_rows: batch.num_rows(),
            batch,
        };

        let series_strings = series_set_to_series_strings(series_set, 4);
        let expected = vec![
            "Series tags={_field=float_field, _measurement=the_table, state=MA}",
            "  FloatPoints batches: [Batch { timestamps: [1000, 2000, 4000, 1000], values: [10.1, 20.1, 40.1, 10.1] }, Batch { timestamps: [2000, 4000, 1000, 2000], values: [20.1, 40.1, 10.1, 20.1] }, Batch { timestamps: [4000], values: [40.1] }]",
        ];

        assert_eq!(
            series_strings, expected,
            "Expected:\n{expected:#?}\nActual:\n{series_strings:#?}"
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

        let timestamp_array = TimestampNanosecondArray::from(vec![1000, 2000]);

        let batch = RecordBatch::try_from_iter_with_nullable(vec![
            ("state", Arc::new(tag_array) as ArrayRef, true),
            ("string_field", Arc::new(string_array), true),
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
            batch: batch.clone(),
        };

        // Expect only a single series (for the data in float_field, int_field is all
        // nulls)
        let series_strings = series_set_to_series_strings(series_set, 100);

        let expected = vec![
            "Series tags={_field=string_field, _measurement=the_table, state=MA}",
            "  StringPoints batches: [Batch { timestamps: [2000], values: [\"foo\"] }]",
            "Series tags={_field=float_field, _measurement=the_table, state=MA}",
            "  FloatPoints batches: [Batch { timestamps: [2000], values: [1.0] }]",
            "Series tags={_field=int_field, _measurement=the_table, state=MA}",
            "  IntegerPoints batches: [Batch { timestamps: [2000], values: [-10] }]",
            "Series tags={_field=uint_field, _measurement=the_table, state=MA}",
            "  UnsignedPoints batches: [Batch { timestamps: [2000], values: [100] }]",
            "Series tags={_field=bool_field, _measurement=the_table, state=MA}",
            "  BooleanPoints batches: [Batch { timestamps: [2000], values: [true] }]",
        ];

        assert_eq!(
            series_strings, expected,
            "Expected:\n{expected:#?}\nActual:\n{series_strings:#?}"
        );

        // multi-batch case

        // the tag columns have just a single value so we can just repeat the original batch to
        // generate more rows
        let batch = repeat_batch(4, &batch);
        let series_set = SeriesSet {
            table_name: Arc::from("the_table"),
            tags: vec![(Arc::from("state"), Arc::from("MA"))],
            field_indexes: FieldIndexes::from_timestamp_and_value_indexes(6, &[1, 2, 3, 4, 5]),
            start_row: 0,
            num_rows: batch.num_rows(),
            batch,
        };

        let series_strings = series_set_to_series_strings(series_set, 3);

        let expected = vec![
            "Series tags={_field=string_field, _measurement=the_table, state=MA}",
            "  StringPoints batches: [Batch { timestamps: [2000, 2000, 2000], values: [\"foo\", \"foo\", \"foo\"] }, Batch { timestamps: [2000], values: [\"foo\"] }]",
            "Series tags={_field=float_field, _measurement=the_table, state=MA}",
            "  FloatPoints batches: [Batch { timestamps: [2000, 2000, 2000], values: [1.0, 1.0, 1.0] }, Batch { timestamps: [2000], values: [1.0] }]",
            "Series tags={_field=int_field, _measurement=the_table, state=MA}",
            "  IntegerPoints batches: [Batch { timestamps: [2000, 2000, 2000], values: [-10, -10, -10] }, Batch { timestamps: [2000], values: [-10] }]",
            "Series tags={_field=uint_field, _measurement=the_table, state=MA}",
            "  UnsignedPoints batches: [Batch { timestamps: [2000, 2000, 2000], values: [100, 100, 100] }, Batch { timestamps: [2000], values: [100] }]",
            "Series tags={_field=bool_field, _measurement=the_table, state=MA}",
            "  BooleanPoints batches: [Batch { timestamps: [2000, 2000, 2000], values: [true, true, true] }, Batch { timestamps: [2000], values: [true] }]",
        ];

        assert_eq!(
            series_strings, expected,
            "Expected:\n{expected:#?}\nActual:\n{series_strings:#?}"
        );
    }

    fn make_record_batch() -> RecordBatch {
        let string_array: ArrayRef = Arc::new(StringArray::from(vec![
            "foo", "bar", "baz", "bar", "baz", "foo",
        ]));
        let int_array: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5, 6]));
        let uint_array: ArrayRef = Arc::new(UInt64Array::from(vec![11, 22, 33, 44, 55, 66]));
        let float_array: ArrayRef =
            Arc::new(Float64Array::from(vec![10.1, 20.1, 30.1, 40.1, 50.1, 60.1]));
        let bool_array: ArrayRef = Arc::new(BooleanArray::from(vec![
            true, false, true, false, true, false,
        ]));

        let timestamp_array: ArrayRef = Arc::new(TimestampNanosecondArray::from(vec![
            1000, 2000, 3000, 4000, 5000, 6000,
        ]));

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

    fn repeat_batch(count: usize, rb: &RecordBatch) -> RecordBatch {
        concat_batches(&rb.schema(), std::iter::repeat(rb).take(count)).unwrap()
    }
}
