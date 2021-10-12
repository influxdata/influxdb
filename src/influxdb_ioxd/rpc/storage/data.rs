//! This module contains code to translate from InfluxDB IOx data
//! formats into the formats needed by gRPC

use std::{collections::BTreeSet, fmt, sync::Arc};

use arrow::{
    array::{
        ArrayRef, BooleanArray, Float64Array, Int64Array, StringArray, TimestampNanosecondArray,
        UInt64Array,
    },
    bitmap::Bitmap,
    datatypes::DataType as ArrowDataType,
};

use observability_deps::tracing::trace;
use query::exec::{
    field::FieldIndex,
    fieldlist::FieldList,
    seriesset::{GroupDescription, SeriesSet, SeriesSetItem},
};

use generated_types::{
    measurement_fields_response::{FieldType, MessageField},
    read_response::{
        frame::Data, BooleanPointsFrame, DataType, FloatPointsFrame, Frame, GroupFrame,
        IntegerPointsFrame, SeriesFrame, StringPointsFrame, UnsignedPointsFrame,
    },
    MeasurementFieldsResponse, ReadResponse, Tag,
};

use super::{TAG_KEY_FIELD, TAG_KEY_MEASUREMENT};
use snafu::Snafu;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unsupported data type in gRPC data translation: {}", data_type))]
    UnsupportedDataType { data_type: ArrowDataType },

    #[snafu(display("Unsupported field data type in gRPC data translation: {}", data_type))]
    UnsupportedFieldType { data_type: ArrowDataType },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Convert a set of tag_keys into a form suitable for gRPC transport,
/// adding the special 0x00 (_m) and 0xff (_f) tag keys
///
/// Namely, a Vec<Vec<u8>>, including the measurement and field names
pub fn tag_keys_to_byte_vecs(tag_keys: Arc<BTreeSet<String>>) -> Vec<Vec<u8>> {
    // special case measurement (0x00) and field (0xff)
    // ensuring they are in the correct sort order (first and last, respectively)
    let mut byte_vecs = Vec::with_capacity(2 + tag_keys.len());
    byte_vecs.push(TAG_KEY_MEASUREMENT.to_vec()); // Shown as _m == _measurement
    tag_keys.iter().for_each(|name| {
        byte_vecs.push(name.bytes().collect());
    });
    byte_vecs.push(TAG_KEY_FIELD.to_vec()); // Shown as _f == _field
    byte_vecs
}

fn series_set_to_frames(series_set: SeriesSet) -> Result<Vec<Frame>> {
    let mut data_records = Vec::new();
    for field_index in series_set.field_indexes.as_slice().iter() {
        field_to_data(&mut data_records, &series_set, field_index)?
    }

    let frames = data_records
        .into_iter()
        .map(|data| {
            let data = Some(data);

            Frame { data }
        })
        .collect();

    Ok(frames)
}

/// Convert `SeriesSetItem` into a form suitable for gRPC transport
///
/// Each `SeriesSetItem` gets converted into this pattern:
///
/// ```
/// (GroupFrame)
///
/// (SeriesFrame for field1)
/// (*Points for field1)
/// (SeriesFrame for field12)
/// (*Points for field1)
/// (....)
/// (SeriesFrame for field1)
/// (*Points for field1)
/// (SeriesFrame for field12)
/// (*Points for field1)
/// (....)
/// ```
///
/// The specific type of (*Points) depends on the type of field column.
pub fn series_set_item_to_read_response(series_set_item: SeriesSetItem) -> Result<ReadResponse> {
    let frames = match series_set_item {
        SeriesSetItem::GroupStart(group_description) => {
            group_description_to_frames(group_description)
        }
        SeriesSetItem::Data(series_set) => series_set_to_frames(series_set)?,
    };
    trace!(frames=%DisplayableFrames::new(&frames), "Response gRPC frames");
    Ok(ReadResponse { frames })
}

/// Converts a [`GroupDescription`] into a storage gRPC `GroupFrame`
/// format that can be returned to the client.
fn group_description_to_frames(group_description: GroupDescription) -> Vec<Frame> {
    // split key=value pairs into two separate vectors
    let GroupDescription { all_tags, gby_vals } = group_description;

    let all_tags = all_tags.into_iter().map(|t| t.bytes().collect());

    // Flux expects there to be `_field` and `_measurement` as the
    // first two "tags". Note this means the lengths of tag_keys and
    // partition_key_values is different.
    //
    // See https://github.com/influxdata/influxdb_iox/issues/2690 for gory details
    let tag_keys = vec![b"_field".to_vec(), b"_measurement".to_vec()]
        .into_iter()
        .chain(all_tags)
        .collect::<Vec<_>>();

    let partition_key_vals = gby_vals
        .into_iter()
        .map(|v| v.bytes().collect())
        .collect::<Vec<_>>();

    let group_frame = GroupFrame {
        tag_keys,
        partition_key_vals,
    };

    let data = Some(Data::Group(group_frame));

    vec![Frame { data }]
}

fn data_type(array: &ArrayRef) -> Result<DataType> {
    match array.data_type() {
        ArrowDataType::Utf8 => Ok(DataType::String),
        ArrowDataType::Float64 => Ok(DataType::Float),
        ArrowDataType::Int64 => Ok(DataType::Integer),
        ArrowDataType::UInt64 => Ok(DataType::Unsigned),
        ArrowDataType::Boolean => Ok(DataType::Boolean),
        _ => UnsupportedDataType {
            data_type: array.data_type().clone(),
        }
        .fail(),
    }
}

/// Returns true if the array is entirely null between start_row and
/// start_row+num_rows
fn is_all_null(arr: &ArrayRef, start_row: usize, num_rows: usize) -> bool {
    let end_row = start_row + num_rows;
    (start_row..end_row).all(|i| arr.is_null(i))
}

// Convert and append a single field to a sequence of frames
fn field_to_data(
    frames: &mut Vec<Data>,
    series_set: &SeriesSet,
    indexes: &FieldIndex,
) -> Result<()> {
    let batch = &series_set.batch;
    let schema = batch.schema();

    let field = schema.field(indexes.value_index);
    let array = batch.column(indexes.value_index);

    let start_row = series_set.start_row;
    let num_rows = series_set.num_rows;

    // No values for this field are in the array so it does not
    // contribute to a series.
    if field.is_nullable() && is_all_null(array, start_row, num_rows) {
        return Ok(());
    }

    let series_frame = SeriesFrame {
        tags: convert_tags(
            series_set.table_name.as_ref(),
            schema.field(indexes.value_index).name(),
            &series_set.tags,
        ),
        data_type: data_type(array)? as i32,
    };
    frames.push(Data::Series(series_frame));

    // Only take timestamps (and values) from the rows that have non
    // null values for this field
    let valid = array.data().null_bitmap().as_ref();

    let timestamps = batch
        .column(indexes.timestamp_index)
        .as_any()
        .downcast_ref::<TimestampNanosecondArray>()
        .unwrap()
        .extract_values(start_row, num_rows, valid);

    frames.push(match array.data_type() {
        ArrowDataType::Utf8 => {
            let values = array
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .extract_values(start_row, num_rows, valid);
            Data::StringPoints(StringPointsFrame { timestamps, values })
        }
        ArrowDataType::Float64 => {
            let values = array
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .extract_values(start_row, num_rows, valid);

            Data::FloatPoints(FloatPointsFrame { timestamps, values })
        }
        ArrowDataType::Int64 => {
            let values = array
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .extract_values(start_row, num_rows, valid);
            Data::IntegerPoints(IntegerPointsFrame { timestamps, values })
        }
        ArrowDataType::UInt64 => {
            let values = array
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap()
                .extract_values(start_row, num_rows, valid);
            Data::UnsignedPoints(UnsignedPointsFrame { timestamps, values })
        }
        ArrowDataType::Boolean => {
            let values = array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap()
                .extract_values(start_row, num_rows, valid);
            Data::BooleanPoints(BooleanPointsFrame { timestamps, values })
        }
        _ => {
            return UnsupportedDataType {
                data_type: array.data_type().clone(),
            }
            .fail();
        }
    });
    Ok(())
}

/// Convert the tag=value pairs from the series set to the correct gRPC
/// format, and add the _f and _m tags for the field name and measurement
fn convert_tags(table_name: &str, field_name: &str, tags: &[(Arc<str>, Arc<str>)]) -> Vec<Tag> {
    // Special case "measurement" name which is modeled as a tag of
    // "_measurement" and "field" which is modeled as a tag of "_field"
    let mut converted_tags = vec![
        Tag {
            key: b"_field".to_vec(),
            value: field_name.bytes().collect(),
        },
        Tag {
            key: b"_measurement".to_vec(),
            value: table_name.bytes().collect(),
        },
    ];

    // convert the rest of the tags
    converted_tags.extend(tags.iter().map(|(k, v)| {
        let key = k.bytes().collect();
        let value = v.bytes().collect();

        Tag { key, value }
    }));

    converted_tags
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

/// Translates FieldList into the gRPC format
pub fn fieldlist_to_measurement_fields_response(
    fieldlist: FieldList,
) -> Result<MeasurementFieldsResponse> {
    let fields = fieldlist
        .fields
        .into_iter()
        .map(|f| {
            Ok(MessageField {
                key: f.name,
                r#type: datatype_to_measurement_field_enum(&f.data_type)? as i32,
                timestamp: f.last_timestamp,
            })
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(MeasurementFieldsResponse { fields })
}

fn datatype_to_measurement_field_enum(data_type: &ArrowDataType) -> Result<FieldType> {
    match data_type {
        ArrowDataType::Float64 => Ok(FieldType::Float),
        ArrowDataType::Int64 => Ok(FieldType::Integer),
        ArrowDataType::UInt64 => Ok(FieldType::Unsigned),
        ArrowDataType::Utf8 => Ok(FieldType::String),
        ArrowDataType::Boolean => Ok(FieldType::Boolean),
        _ => UnsupportedFieldType {
            data_type: data_type.clone(),
        }
        .fail(),
    }
}

/// Wrapper struture that implements [`std::fmt::Display`] for a slice
/// of `Frame`s
struct DisplayableFrames<'a> {
    frames: &'a [Frame],
}

impl<'a> DisplayableFrames<'a> {
    fn new(frames: &'a [Frame]) -> Self {
        Self { frames }
    }
}

impl<'a> fmt::Display for DisplayableFrames<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.frames.iter().try_for_each(|frame| {
            format_frame(frame, f)?;
            writeln!(f)
        })
    }
}

fn format_frame(frame: &Frame, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    let data = &frame.data;
    match data {
        Some(Data::Series(SeriesFrame { tags, data_type })) => write!(
            f,
            "SeriesFrame, tags: {}, type: {:?}",
            dump_tags(tags),
            data_type
        ),
        Some(Data::FloatPoints(FloatPointsFrame { timestamps, values })) => write!(
            f,
            "FloatPointsFrame, timestamps: {:?}, values: {:?}",
            timestamps,
            dump_values(values)
        ),
        Some(Data::IntegerPoints(IntegerPointsFrame { timestamps, values })) => write!(
            f,
            "IntegerPointsFrame, timestamps: {:?}, values: {:?}",
            timestamps,
            dump_values(values)
        ),
        Some(Data::UnsignedPoints(UnsignedPointsFrame { timestamps, values })) => write!(
            f,
            "UnsignedPointsFrame, timestamps: {:?}, values: {:?}",
            timestamps,
            dump_values(values)
        ),
        Some(Data::BooleanPoints(BooleanPointsFrame { timestamps, values })) => write!(
            f,
            "BooleanPointsFrame, timestamps: {:?}, values: {}",
            timestamps,
            dump_values(values)
        ),
        Some(Data::StringPoints(StringPointsFrame { timestamps, values })) => write!(
            f,
            "StringPointsFrame, timestamps: {:?}, values: {}",
            timestamps,
            dump_values(values)
        ),
        Some(Data::Group(GroupFrame {
            tag_keys,
            partition_key_vals,
        })) => write!(
            f,
            "GroupFrame, tag_keys: {}, partition_key_vals: {}",
            dump_u8_vec(tag_keys),
            dump_u8_vec(partition_key_vals)
        ),
        None => write!(f, "<NO data field>"),
    }
}

fn dump_values<T>(v: &[T]) -> String
where
    T: std::fmt::Display,
{
    v.iter()
        .map(|item| format!("{}", item))
        .collect::<Vec<_>>()
        .join(",")
}

fn dump_u8_vec(encoded_strings: &[Vec<u8>]) -> String {
    encoded_strings
        .iter()
        .map(|b| String::from_utf8_lossy(b))
        .collect::<Vec<_>>()
        .join(",")
}

fn dump_tags(tags: &[Tag]) -> String {
    tags.iter()
        .map(|tag| {
            format!(
                "{}={}",
                String::from_utf8_lossy(&tag.key),
                String::from_utf8_lossy(&tag.value),
            )
        })
        .collect::<Vec<_>>()
        .join(",")
}

#[cfg(test)]
mod tests {
    use arrow::{datatypes::DataType as ArrowDataType, record_batch::RecordBatch};
    use query::exec::{field::FieldIndexes, fieldlist::Field};

    use super::*;

    #[test]
    fn test_tag_keys_to_byte_vecs() {
        fn convert_keys(tag_keys: &[&str]) -> Vec<Vec<u8>> {
            let tag_keys = tag_keys
                .iter()
                .map(|s| s.to_string())
                .collect::<BTreeSet<_>>();

            tag_keys_to_byte_vecs(Arc::new(tag_keys))
        }

        assert_eq!(convert_keys(&[]), vec![[0].to_vec(), [255].to_vec()]);
        assert_eq!(
            convert_keys(&["key_a"]),
            vec![[0].to_vec(), b"key_a".to_vec(), [255].to_vec()]
        );
        assert_eq!(
            convert_keys(&["key_a", "key_b"]),
            vec![
                [0].to_vec(),
                b"key_a".to_vec(),
                b"key_b".to_vec(),
                [255].to_vec()
            ]
        );
    }

    fn series_set_to_read_response(series_set: SeriesSet) -> Result<ReadResponse> {
        let frames = series_set_to_frames(series_set)?;
        Ok(ReadResponse { frames })
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

        let response =
            series_set_to_read_response(series_set).expect("Correctly converted series set");

        let dumped_frames = dump_frames(&response.frames);

        let expected_frames = vec![
            "SeriesFrame, tags: _field=string_field,_measurement=the_table,tag1=val1, type: 4",
            "StringPointsFrame, timestamps: [2000, 3000], values: bar,baz",
            "SeriesFrame, tags: _field=int_field,_measurement=the_table,tag1=val1, type: 1",
            "IntegerPointsFrame, timestamps: [2000, 3000], values: \"2,3\"",
            "SeriesFrame, tags: _field=uint_field,_measurement=the_table,tag1=val1, type: 2",
            "UnsignedPointsFrame, timestamps: [2000, 3000], values: \"22,33\"",
            "SeriesFrame, tags: _field=float_field,_measurement=the_table,tag1=val1, type: 0",
            "FloatPointsFrame, timestamps: [2000, 3000], values: \"20.1,30.1\"",
            "SeriesFrame, tags: _field=boolean_field,_measurement=the_table,tag1=val1, type: 3",
            "BooleanPointsFrame, timestamps: [2000, 3000], values: false,true",
        ];

        assert_eq!(
            dumped_frames, expected_frames,
            "Expected:\n{:#?}\nActual:\n{:#?}",
            expected_frames, dumped_frames
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

        let response =
            series_set_to_read_response(series_set).expect("Correctly converted series set");

        let dumped_frames = dump_frames(&response.frames);

        let expected_frames = vec![
            "SeriesFrame, tags: _field=string_field2,_measurement=the_table,tag1=val1, type: 4",
            "StringPointsFrame, timestamps: [4, 5], values: far,faz",
            "SeriesFrame, tags: _field=string_field1,_measurement=the_table,tag1=val1, type: 4",
            "StringPointsFrame, timestamps: [2, 3], values: bar,baz",
        ];

        assert_eq!(
            dumped_frames, expected_frames,
            "Expected:\n{:#?}\nActual:\n{:#?}",
            expected_frames, dumped_frames
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

        let response =
            series_set_to_read_response(series_set).expect("Correctly converted series set");

        let dumped_frames = dump_frames(&response.frames);

        let expected_frames = vec![
            "SeriesFrame, tags: _field=float_field,_measurement=the_table,state=MA, type: 0",
            "FloatPointsFrame, timestamps: [1000, 2000, 4000], values: \"10.1,20.1,40.1\"",
        ];

        assert_eq!(
            dumped_frames, expected_frames,
            "Expected:\n{:#?}\nActual:\n{:#?}",
            expected_frames, dumped_frames
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

        let response =
            series_set_to_read_response(series_set).expect("Correctly converted series set");

        let dumped_frames = dump_frames(&response.frames);

        let expected_frames = vec![
            "SeriesFrame, tags: _field=srting_field,_measurement=the_table,state=MA, type: 4",
            "StringPointsFrame, timestamps: [2000], values: foo",
            "SeriesFrame, tags: _field=float_field,_measurement=the_table,state=MA, type: 0",
            "FloatPointsFrame, timestamps: [2000], values: \"1\"",
            "SeriesFrame, tags: _field=int_field,_measurement=the_table,state=MA, type: 1",
            "IntegerPointsFrame, timestamps: [2000], values: \"-10\"",
            "SeriesFrame, tags: _field=uint_field,_measurement=the_table,state=MA, type: 2",
            "UnsignedPointsFrame, timestamps: [2000], values: \"100\"",
            "SeriesFrame, tags: _field=bool_field,_measurement=the_table,state=MA, type: 3",
            "BooleanPointsFrame, timestamps: [2000], values: true",
        ];

        assert_eq!(
            dumped_frames, expected_frames,
            "Expected:\n{:#?}\nActual:\n{:#?}",
            expected_frames, dumped_frames
        );
    }

    #[test]
    fn test_group_group_conversion() {
        let group_description = GroupDescription {
            all_tags: vec![Arc::from("tag1"), Arc::from("tag2")],
            gby_vals: vec![Arc::from("val1"), Arc::from("val2")],
        };

        let grouped_series_set_item = SeriesSetItem::GroupStart(group_description);

        let response = series_set_item_to_read_response(grouped_series_set_item)
            .expect("Correctly converted grouped_series_set_item");

        let dumped_frames = dump_frames(&response.frames);

        let expected_frames = vec![
            "GroupFrame, tag_keys: _field,_measurement,tag1,tag2, partition_key_vals: val1,val2",
        ];

        assert_eq!(
            dumped_frames, expected_frames,
            "Expected:\n{:#?}\nActual:\n{:#?}",
            expected_frames, dumped_frames
        );
    }

    #[test]
    fn test_group_series_conversion() {
        let float_array: ArrayRef = Arc::new(Float64Array::from(vec![10.1, 20.1, 30.1, 40.1]));
        let timestamp_array: ArrayRef = Arc::new(TimestampNanosecondArray::from_vec(
            vec![1000, 2000, 3000, 4000],
            None,
        ));

        let batch = RecordBatch::try_from_iter(vec![
            ("float_field", float_array),
            ("time", timestamp_array),
        ])
        .expect("created new record batch");

        let series_set = SeriesSet {
            table_name: Arc::from("the_table"),
            tags: vec![(Arc::from("tag1"), Arc::from("val1"))],
            field_indexes: FieldIndexes::from_timestamp_and_value_indexes(1, &[0]),
            start_row: 1,
            num_rows: 2,
            batch,
        };

        let series_set_item = SeriesSetItem::Data(series_set);

        let response = series_set_item_to_read_response(series_set_item)
            .expect("Correctly converted series_set_item");

        let dumped_frames = dump_frames(&response.frames);

        let expected_frames = vec![
            "SeriesFrame, tags: _field=float_field,_measurement=the_table,tag1=val1, type: 0",
            "FloatPointsFrame, timestamps: [2000, 3000], values: \"20.1,30.1\"",
        ];

        assert_eq!(
            dumped_frames, expected_frames,
            "Expected:\n{:#?}\nActual:\n{:#?}",
            expected_frames, dumped_frames
        );
    }

    #[test]
    fn test_field_list_conversion() {
        let input = FieldList {
            fields: vec![
                Field {
                    name: "float".into(),
                    data_type: ArrowDataType::Float64,
                    last_timestamp: 1000,
                },
                Field {
                    name: "int".into(),
                    data_type: ArrowDataType::Int64,
                    last_timestamp: 2000,
                },
                Field {
                    name: "uint".into(),
                    data_type: ArrowDataType::UInt64,
                    last_timestamp: 3000,
                },
                Field {
                    name: "string".into(),
                    data_type: ArrowDataType::Utf8,
                    last_timestamp: 4000,
                },
                Field {
                    name: "bool".into(),
                    data_type: ArrowDataType::Boolean,
                    last_timestamp: 5000,
                },
            ],
        };

        let expected = MeasurementFieldsResponse {
            fields: vec![
                MessageField {
                    key: "float".into(),
                    r#type: FieldType::Float as i32,
                    timestamp: 1000,
                },
                MessageField {
                    key: "int".into(),
                    r#type: FieldType::Integer as i32,
                    timestamp: 2000,
                },
                MessageField {
                    key: "uint".into(),
                    r#type: FieldType::Unsigned as i32,
                    timestamp: 3000,
                },
                MessageField {
                    key: "string".into(),
                    r#type: FieldType::String as i32,
                    timestamp: 4000,
                },
                MessageField {
                    key: "bool".into(),
                    r#type: FieldType::Boolean as i32,
                    timestamp: 5000,
                },
            ],
        };

        let actual = fieldlist_to_measurement_fields_response(input).unwrap();
        assert_eq!(
            actual, expected,
            "Expected:\n{:#?}\nActual:\n{:#?}",
            expected, actual
        );
    }

    #[test]
    fn test_field_list_conversion_error() {
        let input = FieldList {
            fields: vec![Field {
                name: "unsupported".into(),
                data_type: ArrowDataType::Int8,
                last_timestamp: 1000,
            }],
        };
        let result = fieldlist_to_measurement_fields_response(input);
        match result {
            Ok(r) => panic!("Unexpected success: {:?}", r),
            Err(e) => {
                let expected = "Unsupported field data type in gRPC data translation: Int8";
                let actual = format!("{}", e);
                assert!(
                    actual.contains(expected),
                    "Could not find expected '{}' in actual '{}'",
                    expected,
                    actual
                );
            }
        }
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

    fn dump_frames(frames: &[Frame]) -> Vec<String> {
        DisplayableFrames::new(frames)
            .to_string()
            .trim()
            .split('\n')
            .map(|s| s.to_string())
            .collect()
    }
}
