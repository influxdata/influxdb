//! Typestate [line protocol] builder.
//!
//! [line protocol]: https://docs.influxdata.com/influxdb/cloud/reference/syntax/line-protocol
//! [special characters]: https://docs.influxdata.com/influxdb/cloud/reference/syntax/line-protocol/#special-characters
use bytes::BufMut;
use std::{
    fmt::{self},
    marker::PhantomData,
};

// https://docs.influxdata.com/influxdb/cloud/reference/syntax/line-protocol/#special-characters
const COMMA_EQ_SPACE: [char; 3] = [',', '=', ' '];
const COMMA_SPACE: [char; 2] = [',', ' '];
const DOUBLE_QUOTE: [char; 1] = ['"'];

#[doc(hidden)]
#[derive(Clone, Copy, Debug, Default)]
pub struct BeforeMeasurement;
#[doc(hidden)]
#[derive(Clone, Copy, Debug)]
pub struct AfterMeasurement;
#[doc(hidden)]
#[derive(Clone, Copy, Debug)]
pub struct AfterTag;
#[doc(hidden)]
#[derive(Clone, Copy, Debug)]
pub struct AfterField;
#[doc(hidden)]
#[derive(Clone, Copy, Debug)]
pub struct AfterTimestamp;

/// Implements a [line protocol] builder.
///
/// A [`LineProtocolBuilder`] is a statically-typed InfluxDB [line protocol] builder.
/// It writes one or more lines of [line protocol] to a [`bytes::BufMut`].
///
/// ```
/// use influxdb_line_protocol::LineProtocolBuilder;
/// let lp = LineProtocolBuilder::new()
///     .measurement("foo")
///     .tag("bar", "baz")
///     .field("qux", 42.0)
///     .close_line();
///
/// assert_eq!(lp.build(), b"foo,bar=baz qux=42\n");
/// ```
///
/// [`LineProtocolBuilder`] never returns runtime errors. Instead, it employs a type-level state machine
/// to guarantee that users can't build a syntactically-malformed line protocol batch.
///
/// This builder does not check for semantic errors. In particular, it does not check for duplicate tag and field
/// names, nor it does enforce [naming restrictions] on keys.
///
/// Attempts to consume the line protocol before closing a line yield
/// compile-time errors:
///
/// ```compile_fail
/// # use influxdb_line_protocol::LineProtocolBuilder;
/// let lp = LineProtocolBuilder::new()
///     .measurement("foo")
///     .tag("bar", "baz")
///
/// assert_eq!(lp.build(), b"foo,bar=baz qux=42\n");
/// ```
///
/// and attempts to `close_line` the line without at least one field also yield
/// compile-time errors:
///
/// ```compile_fail
/// # use influxdb_line_protocol::LineProtocolBuilder;
/// let lp = LineProtocolBuilder::new()
///     .measurement("foo")
///     .tag("bar", "baz")
///     .close_line();
/// ```
///
/// Tags, if any, must be emitted before fields. This will fail to compile:
///
/// ```compile_fail
/// # use influxdb_line_protocol::LineProtocolBuilder;
/// let lp = LineProtocolBuilder::new()
///     .measurement("foo")
///     .field("qux", 42.0);
///     .tag("bar", "baz")
///     .close_line();
/// ```
///
/// and timestamps, if any, must be specified last before closing the line:
///
/// ```compile_fail
/// # use influxdb_line_protocol::LineProtocolBuilder;
/// let lp = LineProtocolBuilder::new()
///     .measurement("foo")
///     .timestamp(1234)
///     .field("qux", 42.0);
///     .close_line();
/// ```
///
/// (the negative examples part of the documentation is so verbose because it's the only way to test compilation failures)
///
/// [line protocol]: https://docs.influxdata.com/influxdb/cloud/reference/syntax/line-protocol
/// [special characters]: https://docs.influxdata.com/influxdb/cloud/reference/syntax/line-protocol/#special-characters
/// [naming restrictions]: https://docs.influxdata.com/influxdb/cloud/reference/syntax/line-protocol/#naming-restrictions
#[derive(Debug, Default)]
pub struct LineProtocolBuilder<B, S = BeforeMeasurement>
where
    B: BufMut,
{
    buf: B,
    _marker: PhantomData<S>,
}

impl LineProtocolBuilder<Vec<u8>, BeforeMeasurement> {
    /// Creates a new [`LineProtocolBuilder`] with an empty buffer.
    pub fn new() -> Self {
        Self::new_with(vec![])
    }
}

impl<B> LineProtocolBuilder<B, BeforeMeasurement>
where
    B: BufMut,
{
    /// Like `new` but appending to an existing `BufMut`.
    pub fn new_with(buf: B) -> Self {
        Self {
            buf,
            _marker: PhantomData,
        }
    }

    /// Provide the measurement name.
    ///
    /// It returns a new builder whose type allows only setting tags and fields.
    pub fn measurement(self, measurement: &str) -> LineProtocolBuilder<B, AfterMeasurement> {
        let measurement = escape(measurement, COMMA_SPACE);
        self.write(format_args!("{measurement}"))
    }

    /// Finish building the line protocol and return the inner buffer.
    pub fn build(self) -> B {
        self.buf
    }
}

impl<B> LineProtocolBuilder<B, AfterMeasurement>
where
    B: BufMut,
{
    /// Add a tag (key + value).
    ///
    /// Tag keys and tag values will be escaped according to the rules defined in [the special characters documentation].
    ///
    /// [special characters]: https://docs.influxdata.com/influxdb/cloud/reference/syntax/line-protocol/#special-characters
    pub fn tag(self, tag_key: &str, tag_value: &str) -> Self {
        let tag_key = escape(tag_key, COMMA_EQ_SPACE);
        let tag_value = escape(tag_value, COMMA_EQ_SPACE);
        self.write(format_args!(",{tag_key}={tag_value}"))
    }

    /// Add a field (key + value).
    ///
    /// Field keys will be escaped according to the rules defined in [the special characters documentation].
    ///
    /// Field values will encoded according to the rules defined in [the data types and formats documentation].
    ///
    /// This function is called for the first field only. It returns a new builder whose type no longer allows adding tags.
    ///
    /// [special characters]: https://docs.influxdata.com/influxdb/cloud/reference/syntax/line-protocol/#special-characters
    /// [data types and formats]: https://docs.influxdata.com/influxdb/cloud/reference/syntax/line-protocol/#data-types-and-format
    pub fn field<F>(self, field_key: &str, field_value: F) -> LineProtocolBuilder<B, AfterField>
    where
        F: FieldValue,
    {
        self.write(format_args!(" {}", format_field(field_key, &field_value)))
    }
}

impl<B> LineProtocolBuilder<B, AfterField>
where
    B: BufMut,
{
    /// Add a field (key + value).
    ///
    /// This function is called for the second and subsequent fields.
    pub fn field<F: FieldValue>(self, field_key: &str, field_value: F) -> Self {
        self.write(format_args!(",{}", format_field(field_key, &field_value)))
    }

    /// Provide a timestamp.
    ///
    /// It returns a builder whose type allows only closing the line.
    ///
    /// The precision of the timestamp is by default nanoseconds (ns) but the unit
    /// can be changed when performing the request that carries the line protocol body.
    /// Setting the unit is outside of the scope of a line protocol builder.
    pub fn timestamp(self, ts: i64) -> LineProtocolBuilder<B, AfterTimestamp> {
        self.write(format_args!(" {ts}"))
    }

    /// Closing a line is required before starting a new one or finishing building the batch.
    pub fn close_line(self) -> LineProtocolBuilder<B, BeforeMeasurement> {
        self.close()
    }
}

impl<B> LineProtocolBuilder<B, AfterTimestamp>
where
    B: BufMut,
{
    /// Closing a line is required before starting a new one or finishing building the batch.
    pub fn close_line(self) -> LineProtocolBuilder<B, BeforeMeasurement> {
        self.close()
    }
}

impl<B, S> LineProtocolBuilder<B, S>
where
    B: BufMut,
{
    fn close(self) -> LineProtocolBuilder<B, BeforeMeasurement> {
        self.write(format_args!("\n"))
    }

    fn write<S2>(self, args: fmt::Arguments<'_>) -> LineProtocolBuilder<B, S2> {
        use std::io::Write;
        // MutBuf's Write adapter is infallible.
        let mut writer = self.buf.writer();
        write!(&mut writer, "{args}").unwrap();
        LineProtocolBuilder {
            buf: writer.into_inner(),
            _marker: PhantomData,
        }
    }
}

// Return a [`fmt::Display`] that renders string while escaping any characters in the `special_characters` array
// with a `\`
fn escape<const N: usize>(src: &str, special_characters: [char; N]) -> Escaped<'_, N> {
    Escaped {
        src,
        special_characters,
    }
}

struct Escaped<'a, const N: usize> {
    src: &'a str,
    special_characters: [char; N],
}

impl<'a, const N: usize> fmt::Display for Escaped<'a, N> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for ch in self.src.chars() {
            if self.special_characters.contains(&ch) || ch == '\\' {
                write!(f, "\\")?;
            }
            write!(f, "{ch}")?;
        }
        Ok(())
    }
}

// This method is used by the two [`LineProtocolBuilder::field`] variants in order to render the
// `key=value` encoding of a field.
fn format_field<'a, F>(field_key: &'a str, field_value: &'a F) -> impl fmt::Display + 'a
where
    F: FieldValue,
{
    FormattedField {
        field_key,
        field_value,
    }
}

struct FormattedField<'a, F>
where
    F: FieldValue,
{
    field_key: &'a str,
    field_value: &'a F,
}

impl<'a, F> fmt::Display for FormattedField<'a, F>
where
    F: FieldValue,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}=", escape(self.field_key, COMMA_EQ_SPACE))?;
        self.field_value.fmt(f)
    }
}

/// The [`FieldValue`] trait is implemented by the legal [line protocol types].
///
/// [line protocol types]: https://docs.influxdata.com/influxdb/cloud/reference/syntax/line-protocol/#data-types-and-format
pub trait FieldValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result;
}

impl FieldValue for &str {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "\"{}\"", escape(self, DOUBLE_QUOTE))
    }
}

impl FieldValue for f64 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self}")
    }
}

impl FieldValue for bool {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self}")
    }
}

impl FieldValue for i64 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self}i")
    }
}

impl FieldValue for u64 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self}u")
    }
}

#[cfg(test)]
mod tests {
    use crate::{parse_lines, FieldSet, ParsedLine};

    use super::*;

    #[test]
    fn test_string_escape() {
        assert_eq!(
            format!("\"{}\"", escape(r#"foo"#, DOUBLE_QUOTE)),
            r#""foo""#
        );
        assert_eq!(
            format!("\"{}\"", escape(r#"foo \ bar"#, DOUBLE_QUOTE)),
            r#""foo \\ bar""#
        );
        assert_eq!(
            format!("\"{}\"", escape(r#"foo " bar"#, DOUBLE_QUOTE)),
            r#""foo \" bar""#
        );
        assert_eq!(
            format!("\"{}\"", escape(r#"foo \" bar"#, DOUBLE_QUOTE)),
            r#""foo \\\" bar""#
        );
    }

    #[test]
    fn test_lp_builder() {
        const PLAIN: &str = "plain";
        const WITH_SPACE: &str = "with space";
        const WITH_COMMA: &str = "with,comma";
        const WITH_EQ: &str = "with=eq";
        const WITH_DOUBLE_QUOTE: &str = r#"with"doublequote"#;
        const WITH_SINGLE_QUOTE: &str = "with'singlequote";
        const WITH_BACKSLASH: &str = r#"with\ backslash"#;

        let builder = LineProtocolBuilder::new()
            // line 0
            .measurement("tag_keys")
            .tag(PLAIN, "dummy")
            .tag(WITH_SPACE, "dummy")
            .tag(WITH_COMMA, "dummy")
            .tag(WITH_EQ, "dummy")
            .tag(WITH_DOUBLE_QUOTE, "dummy")
            .tag(WITH_SINGLE_QUOTE, "dummy")
            .tag(WITH_BACKSLASH, "dummy")
            .field("dummy", true)
            .close_line()
            // line 1
            .measurement("tag_values")
            .tag("plain", PLAIN)
            .tag("withspace", WITH_SPACE)
            .tag("withcomma", WITH_COMMA)
            .tag("witheq", WITH_EQ)
            .tag("withdoublequote", WITH_DOUBLE_QUOTE)
            .tag("withsinglaquote", WITH_SINGLE_QUOTE)
            .tag("withbackslash", WITH_BACKSLASH)
            .field("dummy", true)
            .close_line()
            // line 2
            .measurement("field keys")
            .field(PLAIN, true)
            .field(WITH_SPACE, true)
            .field(WITH_COMMA, true)
            .field(WITH_EQ, true)
            .field(WITH_DOUBLE_QUOTE, true)
            .field(WITH_SINGLE_QUOTE, true)
            .field(WITH_BACKSLASH, true)
            .close_line()
            // line3
            .measurement("field values")
            .field("mybool", false)
            .field("mysigned", 51_i64)
            .field("myunsigned", 51_u64)
            .field("myfloat", 51.0)
            .field("mystring", "some value")
            .field("mystringwithquotes", "some \" value")
            .close_line()
            // line 4
            .measurement(PLAIN)
            .field("dummy", true)
            .close_line()
            // line 5
            .measurement(WITH_SPACE)
            .field("dummy", true)
            .close_line()
            // line 6
            .measurement(WITH_COMMA)
            .field("dummy", true)
            .close_line()
            // line 7
            .measurement(WITH_EQ)
            .field("dummy", true)
            .close_line()
            // line 8
            .measurement(WITH_DOUBLE_QUOTE)
            .field("dummy", true)
            .close_line()
            // line 9
            .measurement(WITH_SINGLE_QUOTE)
            .field("dummy", true)
            .close_line()
            // line 10
            .measurement(WITH_BACKSLASH)
            .field("dummy", true)
            .close_line()
            // line 11
            .measurement("without timestamp")
            .field("dummy", true)
            .close_line()
            // line 12
            .measurement("with timestamp")
            .field("dummy", true)
            .timestamp(1234)
            .close_line();

        let lp = String::from_utf8(builder.build()).unwrap();
        println!("-----\n{lp}-----");

        let parsed_lines = parse_lines(&lp)
            .collect::<Result<Vec<ParsedLine<'_>>, _>>()
            .unwrap();

        let get_tag_key = |n: usize, f: usize| {
            format!("{}", parsed_lines[n].series.tag_set.as_ref().unwrap()[f].0)
        };
        let row = 0;
        assert_eq!(get_tag_key(row, 0), PLAIN);
        assert_eq!(get_tag_key(row, 1), WITH_SPACE);
        assert_eq!(get_tag_key(row, 2), WITH_COMMA);
        assert_eq!(get_tag_key(row, 3), WITH_EQ);
        assert_eq!(get_tag_key(row, 4), WITH_DOUBLE_QUOTE);
        assert_eq!(get_tag_key(row, 5), WITH_SINGLE_QUOTE);
        assert_eq!(get_tag_key(row, 6), WITH_BACKSLASH);

        let get_tag_value = |n: usize, f: usize| {
            format!("{}", parsed_lines[n].series.tag_set.as_ref().unwrap()[f].1)
        };
        let row = 1;
        assert_eq!(get_tag_value(row, 0), PLAIN);
        assert_eq!(get_tag_value(row, 1), WITH_SPACE);
        assert_eq!(get_tag_value(row, 2), WITH_COMMA);
        assert_eq!(get_tag_value(row, 3), WITH_EQ);
        assert_eq!(get_tag_value(row, 4), WITH_DOUBLE_QUOTE);
        assert_eq!(get_tag_value(row, 5), WITH_SINGLE_QUOTE);
        assert_eq!(get_tag_value(row, 6), WITH_BACKSLASH);

        let get_field_key = |n: usize, f: usize| format!("{}", parsed_lines[n].field_set[f].0);
        let row = 2;
        assert_eq!(get_field_key(row, 0), PLAIN);
        assert_eq!(get_field_key(row, 1), WITH_SPACE);
        assert_eq!(get_field_key(row, 2), WITH_COMMA);
        assert_eq!(get_field_key(row, 3), WITH_EQ);
        assert_eq!(get_field_key(row, 4), WITH_DOUBLE_QUOTE);
        assert_eq!(get_field_key(row, 5), WITH_SINGLE_QUOTE);
        assert_eq!(get_field_key(row, 6), WITH_BACKSLASH);

        let get_field_value = |n: usize, f: usize| format!("{}", parsed_lines[n].field_set[f].1);
        let row = 3;
        assert_eq!(get_field_value(row, 0), "false");
        assert_eq!(get_field_value(row, 1), "51i");
        assert_eq!(get_field_value(row, 2), "51u");
        assert_eq!(get_field_value(row, 3), "51");
        assert_eq!(get_field_value(row, 4), "some value");
        // TODO(mkm): file an issue for the parser since it incorrectly decodes an escaped double quote (possibly also the Go version).
        // assert_eq!(get_field_value(row, 5), "some \" value");

        let get_measurement = |n: usize| format!("{}", parsed_lines[n].series.measurement);
        assert_eq!(get_measurement(4), PLAIN);
        assert_eq!(get_measurement(5), WITH_SPACE);
        assert_eq!(get_measurement(6), WITH_COMMA);
        assert_eq!(get_measurement(7), WITH_EQ);
        assert_eq!(get_measurement(8), WITH_DOUBLE_QUOTE);
        assert_eq!(get_measurement(9), WITH_SINGLE_QUOTE);
        assert_eq!(get_measurement(10), WITH_BACKSLASH);

        let get_timestamp = |n: usize| parsed_lines[n].timestamp;
        assert_eq!(get_timestamp(11), None);
        assert_eq!(get_timestamp(12), Some(1234));
    }

    #[test]
    fn test_float_formatting() {
        // ensure that my_float is printed in a way that it is parsed
        // as a float (not an int)
        let builder = LineProtocolBuilder::new()
            .measurement("tag_keys")
            .tag("foo", "bar")
            .field("my_float", 3.0)
            .close_line();

        let lp = String::from_utf8(builder.build()).unwrap();
        println!("-----\n{lp}-----");

        let parsed_lines = parse_lines(&lp)
            .collect::<Result<Vec<ParsedLine<'_>>, _>>()
            .unwrap();

        assert_eq!(parsed_lines.len(), 1);
        let parsed_line = &parsed_lines[0];

        let expected_fields = vec![("my_float".into(), crate::FieldValue::F64(3.0))]
            .into_iter()
            .collect::<FieldSet<'_>>();

        assert_eq!(parsed_line.field_set, expected_fields)
    }
}
