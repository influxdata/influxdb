//! Routines for dumping read_* responses
use generated_types::{
    read_response::{frame::Data, *},
    Tag,
};

/// Convert a slice of frames to a format suitable for
/// comparing in tests.
pub(crate) fn dump_data_frames(frames: &[Data]) -> Vec<String> {
    frames.iter().map(dump_data).collect()
}

fn dump_data(data: &Data) -> String {
    match Some(data) {
        Some(Data::Series(SeriesFrame { tags, data_type })) => format!(
            "SeriesFrame, tags: {}, type: {:?}",
            dump_tags(tags),
            data_type
        ),
        Some(Data::FloatPoints(FloatPointsFrame { timestamps, values })) => format!(
            "FloatPointsFrame, timestamps: {:?}, values: {:?}",
            timestamps,
            dump_values(values)
        ),
        Some(Data::IntegerPoints(IntegerPointsFrame { timestamps, values })) => format!(
            "IntegerPointsFrame, timestamps: {:?}, values: {:?}",
            timestamps,
            dump_values(values)
        ),
        Some(Data::BooleanPoints(BooleanPointsFrame { timestamps, values })) => format!(
            "BooleanPointsFrame, timestamps: {:?}, values: {}",
            timestamps,
            dump_values(values)
        ),
        Some(Data::StringPoints(StringPointsFrame { timestamps, values })) => format!(
            "StringPointsFrame, timestamps: {:?}, values: {}",
            timestamps,
            dump_values(values)
        ),
        Some(Data::UnsignedPoints(UnsignedPointsFrame { timestamps, values })) => format!(
            "UnsignedPointsFrame, timestamps: {:?}, values: {}",
            timestamps,
            dump_values(values)
        ),
        Some(Data::Group(GroupFrame {
            tag_keys,
            partition_key_vals,
        })) => format!(
            "GroupFrame, tag_keys: {}, partition_key_vals: {}",
            dump_u8_vec(tag_keys),
            dump_u8_vec(partition_key_vals),
        ),
        None => "<NO data field>".into(),
    }
}

fn dump_values<T>(v: &[T]) -> String
where
    T: std::fmt::Display,
{
    v.iter()
        .map(|item| format!("{item}"))
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
