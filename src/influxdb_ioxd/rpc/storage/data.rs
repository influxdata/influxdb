//! This module contains code to translate from InfluxDB IOx data
//! formats into the formats needed by gRPC

use std::{collections::BTreeSet, convert::TryInto, fmt, sync::Arc};

use arrow::datatypes::DataType as ArrowDataType;

use observability_deps::tracing::trace;
use query::exec::{
    fieldlist::FieldList,
    seriesset::{series, GroupDescription, SeriesSet, SeriesSetItem},
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
use snafu::{ResultExt, Snafu};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Error converting series set to gRPC: {}", source))]
    ConvertingSeries {
        source: query::exec::seriesset::series::Error,
    },

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
    let series: Vec<series::Series> = series_set.try_into().context(ConvertingSeries)?;

    let frames = series
        .into_iter()
        .map(|series| {
            let series::Series { tags, data } = series;

            let (data_type, data_frame) = match data {
                series::Data::FloatPoints { timestamps, values } => (
                    DataType::Float,
                    Data::FloatPoints(FloatPointsFrame { timestamps, values }),
                ),
                series::Data::IntegerPoints { timestamps, values } => (
                    DataType::Integer,
                    Data::IntegerPoints(IntegerPointsFrame { timestamps, values }),
                ),
                series::Data::UnsignedPoints { timestamps, values } => (
                    DataType::Unsigned,
                    Data::UnsignedPoints(UnsignedPointsFrame { timestamps, values }),
                ),
                series::Data::BooleanPoints { timestamps, values } => (
                    DataType::Boolean,
                    Data::BooleanPoints(BooleanPointsFrame { timestamps, values }),
                ),
                series::Data::StringPoints { timestamps, values } => (
                    DataType::String,
                    Data::StringPoints(StringPointsFrame { timestamps, values }),
                ),
            };

            let series_frame = Data::Series(SeriesFrame {
                tags: convert_tags(tags),
                data_type: data_type.into(),
            });

            vec![series_frame, data_frame]
        })
        .flatten()
        .map(|data| Frame { data: Some(data) })
        .collect();

    Ok(frames)
}

/// Convert a `SeriesSetItem` into a form suitable for gRPC transport
///
/// Each `SeriesSetItem` gets converted into this pattern:
///
/// ```
/// (GroupFrame) potentially
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
            vec![group_description_to_frame(group_description)]
        }
        SeriesSetItem::Data(series_set) => series_set_to_frames(series_set)?,
    };
    trace!(frames=%DisplayableFrames::new(&frames), "Response gRPC frames");
    Ok(ReadResponse { frames })
}

/// Converts a [`GroupDescription`] into a storage gRPC `GroupFrame`
/// format that can be returned to the client.
fn group_description_to_frame(group_description: GroupDescription) -> Frame {
    let series::Group {
        tag_keys,
        partition_key_vals,
    } = group_description.into();

    let group_frame = GroupFrame {
        tag_keys: arcs_to_bytes(tag_keys),
        partition_key_vals: arcs_to_bytes(partition_key_vals),
    };

    let data = Data::Group(group_frame);

    Frame { data: Some(data) }
}

/// Convert the tag=value pairs from Arc<str> to Vec<u8> for gRPC transport
fn convert_tags(tags: Vec<series::Tag>) -> Vec<Tag> {
    tags.into_iter()
        .map(|series::Tag { key, value }| Tag {
            key: key.bytes().collect(),
            value: value.bytes().collect(),
        })
        .collect()
}

fn arcs_to_bytes(s: Vec<Arc<str>>) -> Vec<Vec<u8>> {
    s.into_iter().map(|s| s.bytes().collect()).collect()
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
    use arrow::{
        array::{
            ArrayRef, BooleanArray, Float64Array, Int64Array, StringArray,
            TimestampNanosecondArray, UInt64Array,
        },
        datatypes::DataType as ArrowDataType,
        record_batch::RecordBatch,
    };
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
