//! Data point building and writing

use snafu::{ensure, Snafu};
use std::{collections::BTreeMap, io};

/// Errors that occur while building `DataPoint`s
#[derive(Debug, Snafu)]
pub enum DataPointError {
    /// Returned when calling `build` on a `DataPointBuilder` that has no
    /// fields.
    #[snafu(display(
        "All `DataPoints` must have at least one field. Builder contains: {:?}",
        data_point_builder
    ))]
    AtLeastOneFieldRequired {
        /// The current state of the `DataPointBuilder`
        data_point_builder: DataPointBuilder,
    },
}

/// Incrementally constructs a `DataPoint`.
///
/// Create this via `DataPoint::builder`.
#[derive(Debug)]
pub struct DataPointBuilder {
    measurement: String,
    // Keeping the tags sorted improves performance on the server side
    tags: BTreeMap<String, String>,
    fields: BTreeMap<String, FieldValue>,
    timestamp: Option<i64>,
}

impl DataPointBuilder {
    fn new(measurement: impl Into<String>) -> Self {
        Self {
            measurement: measurement.into(),
            tags: Default::default(),
            fields: Default::default(),
            timestamp: Default::default(),
        }
    }

    /// Sets a tag, replacing any existing tag of the same name.
    pub fn tag(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.tags.insert(name.into(), value.into());
        self
    }

    /// Sets a field, replacing any existing field of the same name.
    pub fn field(mut self, name: impl Into<String>, value: impl Into<FieldValue>) -> Self {
        self.fields.insert(name.into(), value.into());
        self
    }

    /// Sets the timestamp, replacing any existing timestamp.
    ///
    /// The value is treated as the number of nanoseconds since the
    /// UNIX epoch.
    pub fn timestamp(mut self, value: i64) -> Self {
        self.timestamp = Some(value);
        self
    }

    /// Constructs the data point
    pub fn build(self) -> Result<DataPoint, DataPointError> {
        ensure!(
            !self.fields.is_empty(),
            AtLeastOneFieldRequiredSnafu {
                data_point_builder: self
            }
        );

        let Self {
            measurement,
            tags,
            fields,
            timestamp,
        } = self;

        Ok(DataPoint {
            measurement,
            tags,
            fields,
            timestamp,
        })
    }
}

/// A single point of information to send to InfluxDB.
// TODO: If we want to support non-UTF-8 data, all `String`s stored in `DataPoint` would need
// to be `Vec<u8>` instead, the API for creating a `DataPoint` would need some more consideration,
// and there would need to be more `Write*` trait implementations. Because the `Write*` traits work
// on a writer of bytes, that part of the design supports non-UTF-8 data now.
#[derive(Debug)]
pub struct DataPoint {
    measurement: String,
    tags: BTreeMap<String, String>,
    fields: BTreeMap<String, FieldValue>,
    timestamp: Option<i64>,
}

impl DataPoint {
    /// Create a builder to incrementally construct a `DataPoint`.
    pub fn builder(measurement: impl Into<String>) -> DataPointBuilder {
        DataPointBuilder::new(measurement)
    }
}

impl WriteDataPoint for DataPoint {
    fn write_data_point_to<W>(&self, mut w: W) -> io::Result<()>
    where
        W: io::Write,
    {
        self.measurement.write_measurement_to(&mut w)?;

        for (k, v) in &self.tags {
            w.write_all(b",")?;
            k.write_tag_key_to(&mut w)?;
            w.write_all(b"=")?;
            v.write_tag_value_to(&mut w)?;
        }

        for (i, (k, v)) in self.fields.iter().enumerate() {
            let d = if i == 0 { b" " } else { b"," };

            w.write_all(d)?;
            k.write_field_key_to(&mut w)?;
            w.write_all(b"=")?;
            v.write_field_value_to(&mut w)?;
        }

        if let Some(ts) = self.timestamp {
            w.write_all(b" ")?;
            ts.write_timestamp_to(&mut w)?;
        }

        w.write_all(b"\n")?;

        Ok(())
    }
}

/// Possible value types
#[derive(Debug, Clone, PartialEq)]
pub enum FieldValue {
    /// A true or false value
    Bool(bool),
    /// A 64-bit floating point number
    F64(f64),
    /// A 64-bit signed integer number
    I64(i64),
    /// A 64-bit unsigned integer number
    U64(u64),
    /// A string value
    String(String),
}

impl From<bool> for FieldValue {
    fn from(other: bool) -> Self {
        Self::Bool(other)
    }
}

impl From<f64> for FieldValue {
    fn from(other: f64) -> Self {
        Self::F64(other)
    }
}

impl From<i64> for FieldValue {
    fn from(other: i64) -> Self {
        Self::I64(other)
    }
}

impl From<u64> for FieldValue {
    fn from(other: u64) -> Self {
        Self::U64(other)
    }
}

impl From<&str> for FieldValue {
    fn from(other: &str) -> Self {
        Self::String(other.into())
    }
}

impl From<String> for FieldValue {
    fn from(other: String) -> Self {
        Self::String(other)
    }
}

/// Transform a type into valid line protocol lines
///
/// This trait is to enable the conversion of `DataPoint`s to line protocol; it
/// is unlikely that you would need to implement this trait. In the future, a
/// `derive` crate may exist that would facilitate the generation of
/// implementations of this trait on custom types to help uphold the
/// responsibilities for escaping and producing complete lines.
pub trait WriteDataPoint {
    /// Write this data point as line protocol. The implementor is responsible
    /// for properly escaping the data and ensuring that complete lines
    /// are generated.
    fn write_data_point_to<W>(&self, w: W) -> io::Result<()>
    where
        W: io::Write;
}

// The following are traits rather than free functions so that we can limit
// their implementations to only the data types supported for each of
// measurement, tag key, tag value, field key, field value, and timestamp. They
// are a private implementation detail and any custom implementations
// of these traits would be generated by a future derive trait.
trait WriteMeasurement {
    fn write_measurement_to<W>(&self, w: W) -> io::Result<()>
    where
        W: io::Write;
}

impl WriteMeasurement for str {
    fn write_measurement_to<W>(&self, w: W) -> io::Result<()>
    where
        W: io::Write,
    {
        escape_and_write_value(self, MEASUREMENT_DELIMITERS, w)
    }
}

trait WriteTagKey {
    fn write_tag_key_to<W>(&self, w: W) -> io::Result<()>
    where
        W: io::Write;
}

impl WriteTagKey for str {
    fn write_tag_key_to<W>(&self, w: W) -> io::Result<()>
    where
        W: io::Write,
    {
        escape_and_write_value(self, TAG_KEY_DELIMITERS, w)
    }
}

trait WriteTagValue {
    fn write_tag_value_to<W>(&self, w: W) -> io::Result<()>
    where
        W: io::Write;
}

impl WriteTagValue for str {
    fn write_tag_value_to<W>(&self, w: W) -> io::Result<()>
    where
        W: io::Write,
    {
        escape_and_write_value(self, TAG_VALUE_DELIMITERS, w)
    }
}

trait WriteFieldKey {
    fn write_field_key_to<W>(&self, w: W) -> io::Result<()>
    where
        W: io::Write;
}

impl WriteFieldKey for str {
    fn write_field_key_to<W>(&self, w: W) -> io::Result<()>
    where
        W: io::Write,
    {
        escape_and_write_value(self, FIELD_KEY_DELIMITERS, w)
    }
}

trait WriteFieldValue {
    fn write_field_value_to<W>(&self, w: W) -> io::Result<()>
    where
        W: io::Write;
}

impl WriteFieldValue for FieldValue {
    fn write_field_value_to<W>(&self, mut w: W) -> io::Result<()>
    where
        W: io::Write,
    {
        use FieldValue::*;

        match self {
            Bool(v) => write!(w, "{}", if *v { "t" } else { "f" }),
            F64(v) => write!(w, "{v}"),
            I64(v) => write!(w, "{v}i"),
            U64(v) => write!(w, "{v}u"),
            String(v) => {
                w.write_all(br#"""#)?;
                escape_and_write_value(v, FIELD_VALUE_STRING_DELIMITERS, &mut w)?;
                w.write_all(br#"""#)
            }
        }
    }
}

trait WriteTimestamp {
    fn write_timestamp_to<W>(&self, w: W) -> io::Result<()>
    where
        W: io::Write;
}

impl WriteTimestamp for i64 {
    fn write_timestamp_to<W>(&self, mut w: W) -> io::Result<()>
    where
        W: io::Write,
    {
        write!(w, "{self}")
    }
}

const MEASUREMENT_DELIMITERS: &[char] = &[',', ' '];
const TAG_KEY_DELIMITERS: &[char] = &[',', '=', ' '];
const TAG_VALUE_DELIMITERS: &[char] = TAG_KEY_DELIMITERS;
const FIELD_KEY_DELIMITERS: &[char] = TAG_KEY_DELIMITERS;
const FIELD_VALUE_STRING_DELIMITERS: &[char] = &['"'];

fn escape_and_write_value<W>(
    value: &str,
    escaping_specification: &[char],
    mut w: W,
) -> io::Result<()>
where
    W: io::Write,
{
    let mut last = 0;

    for (idx, delim) in value.match_indices(escaping_specification) {
        let s = &value[last..idx];
        write!(w, r#"{s}\{delim}"#)?;
        last = idx + delim.len();
    }

    w.write_all(value[last..].as_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str;

    fn assert_utf8_strings_eq(left: &[u8], right: &[u8]) {
        assert_eq!(
            left,
            right,
            "\n\nleft string value:  `{}`,\nright string value: `{}`",
            str::from_utf8(left).unwrap(),
            str::from_utf8(right).unwrap(),
        );
    }

    #[test]
    fn point_builder_allows_setting_tags_and_fields() {
        let point = DataPoint::builder("swap")
            .tag("host", "server01")
            .tag("name", "disk0")
            .field("in", 3_i64)
            .field("out", 4_i64)
            .timestamp(1)
            .build()
            .unwrap();

        assert_utf8_strings_eq(
            &point.data_point_to_vec().unwrap(),
            b"swap,host=server01,name=disk0 in=3i,out=4i 1\n".as_ref(),
        );
    }

    #[test]
    fn no_tags_or_timestamp() {
        let point = DataPoint::builder("m0")
            .field("f0", 1.0)
            .field("f1", 2_i64)
            .build()
            .unwrap();

        assert_utf8_strings_eq(
            &point.data_point_to_vec().unwrap(),
            b"m0 f0=1,f1=2i\n".as_ref(),
        );
    }

    #[test]
    fn no_timestamp() {
        let point = DataPoint::builder("m0")
            .tag("t0", "v0")
            .tag("t1", "v1")
            .field("f1", 2_i64)
            .build()
            .unwrap();

        assert_utf8_strings_eq(
            &point.data_point_to_vec().unwrap(),
            b"m0,t0=v0,t1=v1 f1=2i\n".as_ref(),
        );
    }

    #[test]
    fn no_field() {
        let point_result = DataPoint::builder("m0").build();

        assert!(point_result.is_err());
    }

    const ALL_THE_DELIMITERS: &str = r#"alpha,beta=delta gamma"epsilon"#;

    #[test]
    fn special_characters_are_escaped_in_measurements() {
        assert_utf8_strings_eq(
            &ALL_THE_DELIMITERS.measurement_to_vec().unwrap(),
            br#"alpha\,beta=delta\ gamma"epsilon"#.as_ref(),
        );
    }

    #[test]
    fn special_characters_are_escaped_in_tag_keys() {
        assert_utf8_strings_eq(
            &ALL_THE_DELIMITERS.tag_key_to_vec().unwrap(),
            br#"alpha\,beta\=delta\ gamma"epsilon"#.as_ref(),
        );
    }

    #[test]
    fn special_characters_are_escaped_in_tag_values() {
        assert_utf8_strings_eq(
            &ALL_THE_DELIMITERS.tag_value_to_vec().unwrap(),
            br#"alpha\,beta\=delta\ gamma"epsilon"#.as_ref(),
        );
    }

    #[test]
    fn special_characters_are_escaped_in_field_keys() {
        assert_utf8_strings_eq(
            &ALL_THE_DELIMITERS.field_key_to_vec().unwrap(),
            br#"alpha\,beta\=delta\ gamma"epsilon"#.as_ref(),
        );
    }

    #[test]
    fn special_characters_are_escaped_in_field_values_of_strings() {
        assert_utf8_strings_eq(
            &FieldValue::from(ALL_THE_DELIMITERS)
                .field_value_to_vec()
                .unwrap(),
            br#""alpha,beta=delta gamma\"epsilon""#.as_ref(),
        );
    }

    #[test]
    fn field_value_of_bool() {
        let e = FieldValue::from(true);
        assert_utf8_strings_eq(&e.field_value_to_vec().unwrap(), b"t");

        let e = FieldValue::from(false);
        assert_utf8_strings_eq(&e.field_value_to_vec().unwrap(), b"f");
    }

    #[test]
    fn field_value_of_float() {
        let e = FieldValue::from(42_f64);
        assert_utf8_strings_eq(&e.field_value_to_vec().unwrap(), b"42");
    }

    #[test]
    fn field_value_of_signed_integer() {
        let e = FieldValue::from(42_i64);
        assert_utf8_strings_eq(&e.field_value_to_vec().unwrap(), b"42i");
    }

    #[test]
    fn field_value_of_unsigned_integer() {
        let e = FieldValue::from(42_u64);
        assert_utf8_strings_eq(&e.field_value_to_vec().unwrap(), b"42u");
    }

    #[test]
    fn field_value_of_string() {
        let e = FieldValue::from("hello");
        assert_utf8_strings_eq(&e.field_value_to_vec().unwrap(), br#""hello""#);
    }

    // Clears up the boilerplate of writing to a vector from the tests
    macro_rules! test_extension_traits {
        ($($ext_name:ident :: $ext_fn_name:ident -> $base_name:ident :: $base_fn_name:ident,)*) => {
            $(
                trait $ext_name: $base_name {
                    fn $ext_fn_name(&self) -> io::Result<Vec<u8>> {
                        let mut v = Vec::new();
                        self.$base_fn_name(&mut v)?;
                        Ok(v)
                    }
                }
                impl<T: $base_name + ?Sized> $ext_name for T {}
            )*
        }
    }

    test_extension_traits! {
        WriteDataPointExt::data_point_to_vec -> WriteDataPoint::write_data_point_to,
        WriteMeasurementExt::measurement_to_vec -> WriteMeasurement::write_measurement_to,
        WriteTagKeyExt::tag_key_to_vec -> WriteTagKey::write_tag_key_to,
        WriteTagValueExt::tag_value_to_vec -> WriteTagValue::write_tag_value_to,
        WriteFieldKeyExt::field_key_to_vec -> WriteFieldKey::write_field_key_to,
        WriteFieldValueExt::field_value_to_vec -> WriteFieldValue::write_field_value_to,
    }
}
