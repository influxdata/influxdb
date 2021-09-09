//! This module contains a pure rust implementation of a parser for InfluxDB
//! Line Protocol <https://v2.docs.influxdata.com/v2.0/reference/syntax/line-protocol>
//!
//! This implementation is intended to be compatible with the Go implementation,
//! <https://github.com/influxdata/influxdb/blob/217eddc87e14a79b01d0c22994fc139f530094a2/models/points_parser.go>
//!
//! However, this implementation uses a nom combinator based parser
//! rather than attempting to port the imperative Go logic.

#![deny(rustdoc::broken_intra_doc_links, rustdoc::bare_urls, rust_2018_idioms)]
#![warn(
    missing_copy_implementations,
    missing_debug_implementations,
    clippy::explicit_iter_loop,
    clippy::use_self,
    clippy::clone_on_ref_ptr
)]

use fmt::Display;
use nom::{
    branch::alt,
    bytes::complete::{tag, take_while1},
    character::complete::digit1,
    combinator::{map, opt, recognize},
    multi::many0,
    sequence::{preceded, separated_pair, terminated, tuple},
};
use observability_deps::tracing::debug;
use smallvec::SmallVec;
use snafu::{ResultExt, Snafu};
use std::cmp::Ordering;
use std::{
    borrow::Cow,
    char,
    collections::{btree_map::Entry, BTreeMap},
    fmt,
    ops::Deref,
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(r#"Must not contain duplicate tags, but "{}" was repeated"#, tag_key))]
    DuplicateTag { tag_key: String },

    #[snafu(display(r#"No fields were provided"#))]
    FieldSetMissing,

    #[snafu(display(r#"Unable to parse integer value '{}'"#, value))]
    IntegerValueInvalid {
        source: std::num::ParseIntError,
        value: String,
    },

    #[snafu(display(r#"Unable to parse unsigned integer value '{}'"#, value))]
    UIntegerValueInvalid {
        source: std::num::ParseIntError,
        value: String,
    },

    #[snafu(display(r#"Unable to parse floating-point value '{}'"#, value))]
    FloatValueInvalid {
        source: std::num::ParseFloatError,
        value: String,
    },

    #[snafu(display(r#"Unable to parse timestamp value '{}'"#, value))]
    TimestampValueInvalid {
        source: std::num::ParseIntError,
        value: String,
    },

    // This error is for compatibility with the Go parser
    #[snafu(display(
        r#"Measurements, tag keys and values, and field keys may not end with a backslash"#
    ))]
    EndsWithBackslash,

    #[snafu(display(
        "Could not parse entire line. Found trailing content: '{}'",
        trailing_content
    ))]
    CannotParseEntireLine { trailing_content: String },

    // TODO: Replace this with specific failures.
    #[snafu(display(r#"A generic parsing error occurred: {:?}"#, kind))]
    GenericParsingError {
        kind: nom::error::ErrorKind,
        trace: Vec<Error>,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;
type IResult<I, T, E = Error> = nom::IResult<I, T, E>;

impl nom::error::ParseError<&str> for Error {
    fn from_error_kind(_input: &str, kind: nom::error::ErrorKind) -> Self {
        GenericParsingError {
            kind,
            trace: vec![],
        }
        .build()
    }

    fn append(_input: &str, kind: nom::error::ErrorKind, other: Self) -> Self {
        GenericParsingError {
            kind,
            trace: vec![other],
        }
        .build()
    }
}

/// Represents a single parsed line of line protocol data
///
/// Here is an example of how to parse the line protocol data
/// `cpu,host=A,region=west usage_system=64.2 1590488773254420000`
/// into a `ParsedLine`:
///
/// ```
/// use influxdb_line_protocol::{ParsedLine, FieldValue};
///
/// let mut parsed_lines =
///     influxdb_line_protocol::parse_lines(
///         "cpu,host=A,region=west usage_system=64i 1590488773254420000"
///     );
/// let parsed_line = parsed_lines
///     .next()
///     .expect("Should have at least one line")
///     .expect("Should parse successfully");
///
/// let ParsedLine {
///     series,
///     field_set,
///     timestamp,
/// } = parsed_line;
///
/// assert_eq!(series.measurement, "cpu");
///
/// let tags = series.tag_set.unwrap();
/// assert_eq!(tags[0].0, "host");
/// assert_eq!(tags[0].1, "A");
/// assert_eq!(tags[1].0, "region");
/// assert_eq!(tags[1].1, "west");
///
/// let field = &field_set[0];
/// assert_eq!(field.0, "usage_system");
/// assert_eq!(field.1, FieldValue::I64(64));
///
/// assert_eq!(timestamp, Some(1590488773254420000));
/// ```
#[derive(Debug)]
pub struct ParsedLine<'a> {
    pub series: Series<'a>,
    pub field_set: FieldSet<'a>,
    pub timestamp: Option<i64>,
}

impl<'a> ParsedLine<'a> {
    /// Total number of columns on this line, including fields, tags, and
    /// timestamp (which is always present).
    ///
    /// ```
    /// use influxdb_line_protocol::{ParsedLine, FieldValue};
    ///
    /// let mut parsed_lines =
    ///     influxdb_line_protocol::parse_lines(
    ///         "cpu,host=A,region=west usage_system=64i 1590488773254420000"
    ///     );
    /// let parsed_line = parsed_lines
    ///     .next()
    ///     .expect("Should have at least one line")
    ///     .expect("Should parse successfully");
    ///
    /// assert_eq!(parsed_line.column_count(), 4);
    /// ```
    pub fn column_count(&self) -> usize {
        1 + self.field_set.len() + self.series.tag_set.as_ref().map_or(0, |t| t.len())
    }

    /// Returns the value of the passed in tag, if present.
    pub fn tag_value(&self, tag_key: &str) -> Option<&EscapedStr<'a>> {
        match &self.series.tag_set {
            Some(t) => {
                let t = t.iter().find(|(k, _)| *k == tag_key);
                t.map(|(_, val)| val)
            }
            None => None,
        }
    }

    /// Returns the value of the passed in field, if present.
    pub fn field_value(&self, field_key: &str) -> Option<&FieldValue<'a>> {
        let f = self.field_set.iter().find(|(f, _)| *f == field_key);
        f.map(|(_, val)| val)
    }
}

/// Converts from a ParsedLine back to (canonical) LineProtocol
///
/// A note on validity: This code does not errors or panics if the
/// `ParsedLine` represents invalid LineProtocol (for example, if it
/// has 0 fields).
///
/// Thus, if the ParsedLine represents invalid LineProtocol, then
/// the result of `Display` / `to_string()` will also be invalid.
impl<'a> Display for ParsedLine<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.series)?;

        if !self.field_set.is_empty() {
            write!(f, " ")?;

            let mut first = true;
            for (field_name, field_value) in &self.field_set {
                if !first {
                    write!(f, ",")?;
                }
                first = false;
                escape_and_write_value(f, field_name.as_str(), FIELD_KEY_DELIMITERS)?;
                write!(f, "={}", field_value)?;
            }
        }

        if let Some(timestamp) = self.timestamp {
            write!(f, " {}", timestamp)?
        }
        Ok(())
    }
}

/// Represents the identifier of a series (measurement, tagset) for
/// line protocol data
#[derive(Debug)]
pub struct Series<'a> {
    raw_input: &'a str,
    pub measurement: EscapedStr<'a>,
    pub tag_set: Option<TagSet<'a>>,
}

/// Converts Series back to LineProtocol
impl<'a> Display for Series<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        escape_and_write_value(f, self.measurement.as_str(), MEASUREMENT_DELIMITERS)?;
        if let Some(tag_set) = &self.tag_set {
            write!(f, ",")?;
            let mut first = true;
            for (tag_name, tag_value) in tag_set {
                if !first {
                    write!(f, ",")?;
                }
                first = false;
                escape_and_write_value(f, tag_name.as_str(), TAG_KEY_DELIMITERS)?;
                write!(f, "=")?;
                escape_and_write_value(f, tag_value.as_str(), TAG_VALUE_DELIMITERS)?;
            }
        }
        Ok(())
    }
}

impl<'a> Series<'a> {
    pub fn generate_base(self) -> Result<Cow<'a, str>> {
        match (!self.is_escaped(), self.is_sorted_and_unique()) {
            (true, true) => Ok(self.raw_input.into()),
            (_, true) => Ok(self.generate_base_with_escaping().into()),
            (_, _) => self
                .generate_base_with_escaping_sorting_deduplicating()
                .map(Into::into),
        }
    }

    fn generate_base_with_escaping(self) -> String {
        let mut series_base = self.measurement.to_string();
        for (tag_key, tag_value) in self.tag_set.unwrap_or_default() {
            use std::fmt::Write;
            write!(&mut series_base, ",{}={}", tag_key, tag_value)
                .expect("Could not append string");
        }
        series_base
    }

    fn generate_base_with_escaping_sorting_deduplicating(self) -> Result<String> {
        let mut unique_sorted_tag_set = BTreeMap::new();
        for (tag_key, tag_value) in self.tag_set.unwrap_or_default() {
            match unique_sorted_tag_set.entry(tag_key) {
                Entry::Vacant(e) => {
                    e.insert(tag_value);
                }
                Entry::Occupied(e) => {
                    let (tag_key, _) = e.remove_entry();
                    return DuplicateTag {
                        tag_key: tag_key.to_string(),
                    }
                    .fail();
                }
            }
        }

        let mut series_base = self.measurement.to_string();
        for (tag_key, tag_value) in unique_sorted_tag_set {
            use std::fmt::Write;
            write!(&mut series_base, ",{}={}", tag_key, tag_value)
                .expect("Could not append string");
        }

        Ok(series_base)
    }

    fn is_escaped(&self) -> bool {
        self.measurement.is_escaped() || {
            match &self.tag_set {
                None => false,
                Some(tag_set) => tag_set
                    .iter()
                    .any(|(tag_key, tag_value)| tag_key.is_escaped() || tag_value.is_escaped()),
            }
        }
    }

    fn is_sorted_and_unique(&self) -> bool {
        match &self.tag_set {
            None => true,
            Some(tag_set) => {
                let mut i = tag_set.iter().zip(tag_set.iter().skip(1));
                i.all(|((last_tag_key, _), (this_tag_key, _))| last_tag_key < this_tag_key)
            }
        }
    }
}

pub type FieldSet<'a> = SmallVec<[(EscapedStr<'a>, FieldValue<'a>); 4]>;
pub type TagSet<'a> = SmallVec<[(EscapedStr<'a>, EscapedStr<'a>); 8]>;

/// Allowed types of Fields in a `ParsedLine`. One of the types described in
/// <https://docs.influxdata.com/influxdb/v2.0/reference/syntax/line-protocol/#data-types-and-format>
#[derive(Debug, Clone, PartialEq)]
pub enum FieldValue<'a> {
    I64(i64),
    U64(u64),
    F64(f64),
    String(EscapedStr<'a>),
    Boolean(bool),
}

/// Converts FieldValue back to LineProtocol
/// See <https://docs.influxdata.com/influxdb/v2.0/reference/syntax/line-protocol/>
/// for more detail.
impl<'a> Display for FieldValue<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::I64(v) => write!(f, "{}i", v),
            Self::U64(v) => write!(f, "{}u", v),
            Self::F64(v) => write!(f, "{}", v),
            Self::String(v) => escape_and_write_value(f, v, FIELD_VALUE_STRING_DELIMITERS),
            Self::Boolean(v) => write!(f, "{}", v),
        }
    }
}

/// Represents single logical string in the input.
///
/// We do not use `&str` directly here because the actual input may be
/// escaped, in which case the data in the input buffer is not
/// contiguous. This enum provides an interface to access all such
/// strings as contiguous string slices for compatibility with other
/// code, and is optimized for the common case where the
/// input was all in a contiguous string slice.
///
/// For example the 8 character string `Foo\\Bar` (note the double
/// `\\`) is parsed into the logical 7 character string `Foo\Bar`
/// (note the single `\`)
#[derive(Debug, Clone, Eq)]
pub enum EscapedStr<'a> {
    SingleSlice(&'a str),
    CopiedValue(String),
}

impl fmt::Display for EscapedStr<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            EscapedStr::SingleSlice(s) => s.fmt(f)?,
            EscapedStr::CopiedValue(s) => s.fmt(f)?,
        }
        Ok(())
    }
}

impl<'a> EscapedStr<'a> {
    fn from_slices(v: &[&'a str]) -> EscapedStr<'a> {
        match v.len() {
            0 => EscapedStr::SingleSlice(""),
            1 => EscapedStr::SingleSlice(v[0]),
            _ => EscapedStr::CopiedValue(v.join("")),
        }
    }

    fn is_escaped(&self) -> bool {
        match self {
            EscapedStr::SingleSlice(_) => false,
            EscapedStr::CopiedValue(_) => true,
        }
    }

    /// Return the logical representation for the EscapedStr as a
    /// single slice. The slice may not point into the original
    /// buffer.
    pub fn as_str(&self) -> &str {
        &*self
    }
}

impl<'a> Deref for EscapedStr<'a> {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        match &self {
            EscapedStr::SingleSlice(s) => s,
            EscapedStr::CopiedValue(s) => s,
        }
    }
}

impl<'a> PartialEq for EscapedStr<'a> {
    fn eq(&self, other: &Self) -> bool {
        self.as_str() == other.as_str()
    }
}

impl<'a> PartialOrd for EscapedStr<'a> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<'a> Ord for EscapedStr<'a> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.as_str().cmp(other.as_str())
    }
}

impl<'a> From<&'a str> for EscapedStr<'a> {
    fn from(other: &'a str) -> Self {
        EscapedStr::SingleSlice(other)
    }
}

impl From<EscapedStr<'_>> for String {
    fn from(other: EscapedStr<'_>) -> Self {
        match other {
            EscapedStr::SingleSlice(s) => s.into(),
            EscapedStr::CopiedValue(s) => s,
        }
    }
}

impl From<&EscapedStr<'_>> for String {
    fn from(other: &EscapedStr<'_>) -> Self {
        other.to_string()
    }
}

impl PartialEq<&str> for EscapedStr<'_> {
    fn eq(&self, other: &&str) -> bool {
        self.as_str() == *other
    }
}

impl PartialEq<String> for EscapedStr<'_> {
    fn eq(&self, other: &String) -> bool {
        self.as_str() == other
    }
}

pub fn parse_lines(input: &str) -> impl Iterator<Item = Result<ParsedLine<'_>>> {
    split_lines(input).filter_map(|line| {
        let i = trim_leading(line);

        if i.is_empty() {
            return None;
        }

        let res = match parse_line(i) {
            Ok((remaining, line)) => {
                // should have parsed the whole input line, if any
                // data remains it is a parse error for this line
                // corresponding Go logic:
                // https://github.com/influxdata/influxdb/blob/217eddc87e14a79b01d0c22994fc139f530094a2/models/points_parser.go#L259-L266
                if !remaining.is_empty() {
                    Some(Err(Error::CannotParseEntireLine {
                        trailing_content: String::from(remaining),
                    }))
                } else {
                    Some(Ok(line))
                }
            }
            Err(nom::Err::Error(e)) | Err(nom::Err::Failure(e)) => Some(Err(e)),
            Err(nom::Err::Incomplete(_)) => unreachable!("Cannot have incomplete data"), // Only streaming parsers have this
        };

        if let Some(Err(r)) = &res {
            debug!("Error parsing line: '{}'. Error was {:?}", line, r);
        }
        res
    })
}

/// Split `input` into individual lines to be parsed, based on the
/// rules of the Line Protocol format.
///
/// This code is more or less a direct port of the [Go implementation of
/// `scanLine`](https://github.com/influxdata/influxdb/blob/217eddc87e14a79b01d0c22994fc139f530094a2/models/points.go#L1078)
///
/// While this choice of implementation definitely means there is
/// logic duplication for scanning fields, duplicating it also means
/// we can be more sure of the compatibility of the rust parser and
/// the canonical Go parser.
fn split_lines(input: &str) -> impl Iterator<Item = &str> {
    // NB: This is ported as closely as possibly from the original Go code:
    let mut quoted = false;
    let mut fields = false;

    // tracks how many '=' and commas we've seen
    // this duplicates some of the functionality in scanFields
    let mut equals = 0;
    let mut commas = 0;

    let mut in_escape = false;
    input.split(move |c| {
        // skip past escaped characters
        if in_escape {
            in_escape = false;
            return false;
        }

        if c == '\\' {
            in_escape = true;
            return false;
        }

        if c == ' ' {
            fields = true;
            return false;
        }

        // If we see a double quote, makes sure it is not escaped
        if fields {
            if !quoted && c == '=' {
                equals += 1;
                return false;
            } else if !quoted && c == ',' {
                commas += 1;
                return false;
            } else if c == '"' && equals > commas {
                quoted = !quoted;
                return false;
            }
        }

        if c == '\n' && !quoted {
            // reset all the state -- we found a line
            quoted = false;
            fields = false;
            equals = 0;
            commas = 0;
            assert!(!in_escape);
            in_escape = false;
            return true;
        }

        false
    })
}

fn parse_line(i: &str) -> IResult<&str, ParsedLine<'_>> {
    let field_set = preceded(whitespace, field_set);
    let timestamp = preceded(whitespace, terminated(timestamp, opt(whitespace)));

    let line = tuple((series, field_set, opt(timestamp)));

    map(line, |(series, field_set, timestamp)| ParsedLine {
        series,
        field_set,
        timestamp,
    })(i)
}

fn series(i: &str) -> IResult<&str, Series<'_>> {
    let tag_set = preceded(tag(","), tag_set);
    let series = tuple((measurement, opt(tag_set)));

    let series_and_raw_input = parse_and_recognize(series);

    map(
        series_and_raw_input,
        |(raw_input, (measurement, tag_set))| Series {
            raw_input,
            measurement,
            tag_set,
        },
    )(i)
}

fn measurement(i: &str) -> IResult<&str, EscapedStr<'_>> {
    let normal_char = take_while1(|c| !is_whitespace_boundary_char(c) && c != ',' && c != '\\');

    let space = map(tag(" "), |_| " ");
    let comma = map(tag(","), |_| ",");
    let backslash = map(tag("\\"), |_| "\\");

    let escaped = alt((space, comma, backslash));

    escape_or_fallback(normal_char, "\\", escaped)(i)
}

fn tag_set(i: &str) -> IResult<&str, TagSet<'_>> {
    let one_tag = separated_pair(tag_key, tag("="), tag_value);
    parameterized_separated_list(tag(","), one_tag, SmallVec::new, |v, i| v.push(i))(i)
}

fn tag_key(i: &str) -> IResult<&str, EscapedStr<'_>> {
    let normal_char = take_while1(|c| !is_whitespace_boundary_char(c) && c != '=' && c != '\\');

    escaped_value(normal_char)(i)
}

fn tag_value(i: &str) -> IResult<&str, EscapedStr<'_>> {
    let normal_char = take_while1(|c| !is_whitespace_boundary_char(c) && c != ',' && c != '\\');
    escaped_value(normal_char)(i)
}

fn field_set(i: &str) -> IResult<&str, FieldSet<'_>> {
    let one_field = separated_pair(field_key, tag("="), field_value);
    let sep = tag(",");

    match parameterized_separated_list1(sep, one_field, SmallVec::new, |v, i| v.push(i))(i) {
        Err(nom::Err::Error(_)) => FieldSetMissing.fail().map_err(nom::Err::Error),
        other => other,
    }
}

fn field_key(i: &str) -> IResult<&str, EscapedStr<'_>> {
    let normal_char = take_while1(|c| !is_whitespace_boundary_char(c) && c != '=' && c != '\\');
    escaped_value(normal_char)(i)
}

fn field_value(i: &str) -> IResult<&str, FieldValue<'_>> {
    let int = map(field_integer_value, FieldValue::I64);
    let uint = map(field_uinteger_value, FieldValue::U64);
    let float = map(field_float_value, FieldValue::F64);
    let string = map(field_string_value, FieldValue::String);
    let boolv = map(field_bool_value, FieldValue::Boolean);

    alt((int, uint, float, string, boolv))(i)
}

fn field_integer_value(i: &str) -> IResult<&str, i64> {
    let tagged_value = terminated(integral_value_signed, tag("i"));
    map_fail(tagged_value, |value| {
        value.parse().context(IntegerValueInvalid { value })
    })(i)
}

fn field_uinteger_value(i: &str) -> IResult<&str, u64> {
    let tagged_value = terminated(digit1, tag("u"));
    map_fail(tagged_value, |value| {
        value.parse().context(UIntegerValueInvalid { value })
    })(i)
}

fn field_float_value(i: &str) -> IResult<&str, f64> {
    let value = alt((
        field_float_value_with_exponential_and_decimal,
        field_float_value_with_exponential_no_decimal,
        field_float_value_with_decimal,
        field_float_value_no_decimal,
    ));
    map_fail(value, |value| {
        value.parse().context(FloatValueInvalid { value })
    })(i)
}

fn field_float_value_with_decimal(i: &str) -> IResult<&str, &str> {
    recognize(separated_pair(integral_value_signed, tag("."), digit1))(i)
}

fn field_float_value_with_exponential_and_decimal(i: &str) -> IResult<&str, &str> {
    recognize(separated_pair(
        integral_value_signed,
        tag("."),
        exponential_value,
    ))(i)
}

fn field_float_value_with_exponential_no_decimal(i: &str) -> IResult<&str, &str> {
    exponential_value(i)
}

fn exponential_value(i: &str) -> IResult<&str, &str> {
    recognize(separated_pair(
        digit1,
        tuple((alt((tag("e"), tag("E"))), alt((tag("-"), tag("+"))))),
        digit1,
    ))(i)
}

fn field_float_value_no_decimal(i: &str) -> IResult<&str, &str> {
    integral_value_signed(i)
}

fn integral_value_signed(i: &str) -> IResult<&str, &str> {
    recognize(preceded(opt(tag("-")), digit1))(i)
}

pub fn timestamp(i: &str) -> IResult<&str, i64> {
    map_fail(integral_value_signed, |value| {
        value.parse().context(TimestampValueInvalid { value })
    })(i)
}

fn field_string_value(i: &str) -> IResult<&str, EscapedStr<'_>> {
    // https://docs.influxdata.com/influxdb/v2.0/reference/syntax/line-protocol/#data-types-and-format
    // For string field values, backslash is only used to escape itself(\) or double
    // quotes.
    let string_data = alt((
        map(tag(r#"\""#), |_| r#"""#), // escaped double quote -> double quote
        map(tag(r#"\\"#), |_| r#"\"#), // escaped backslash --> single backslash
        tag(r#"\"#),                   // unescaped single backslash
        take_while1(|c| c != '\\' && c != '"'), // anything else w/ no special handling
    ));

    // NB: many0 doesn't allow combinators that match the empty string so
    // we need to special case a pair of double quotes.
    let empty_str = map(tag(r#""""#), |_| Vec::new());

    let quoted_str = alt((
        preceded(tag("\""), terminated(many0(string_data), tag("\""))),
        empty_str,
    ));

    map(quoted_str, |vec| EscapedStr::from_slices(&vec))(i)
}

fn field_bool_value(i: &str) -> IResult<&str, bool> {
    // https://docs.influxdata.com/influxdb/v2.0/reference/syntax/line-protocol/#data-types-and-format
    // "specify TRUE with t, T, true, True, or TRUE. Specify FALSE with f, F, false,
    // False, or FALSE
    alt((
        map(tag("true"), |_| true),
        map(tag("True"), |_| true),
        map(tag("TRUE"), |_| true),
        map(tag("t"), |_| true),
        map(tag("T"), |_| true),
        map(tag("false"), |_| false),
        map(tag("False"), |_| false),
        map(tag("FALSE"), |_| false),
        map(tag("f"), |_| false),
        map(tag("F"), |_| false),
    ))(i)
}

/// Truncates the input slice to remove all whitespace from the
/// beginning (left), including completely commented-out lines
fn trim_leading(mut i: &str) -> &str {
    loop {
        let offset = i
            .find(|c| !is_whitespace_boundary_char(c))
            .unwrap_or_else(|| i.len());
        i = &i[offset..];

        if i.starts_with('#') {
            let offset = i.find('\n').unwrap_or_else(|| i.len());
            i = &i[offset..];
        } else {
            break i;
        }
    }
}

fn whitespace(i: &str) -> IResult<&str, &str> {
    take_while1(|c| c == ' ')(i)
}

fn is_whitespace_boundary_char(c: char) -> bool {
    c == ' ' || c == '\t' || c == '\n'
}

/// While not all of these escape characters are required to be
/// escaped, we support the client escaping them proactively to
/// provide a common experience.
fn escaped_value<'a>(
    normal: impl Fn(&'a str) -> IResult<&'a str, &'a str>,
) -> impl FnOnce(&'a str) -> IResult<&'a str, EscapedStr<'a>> {
    move |i| {
        let backslash = map(tag("\\"), |_| "\\");
        let comma = map(tag(","), |_| ",");
        let equal = map(tag("="), |_| "=");
        let space = map(tag(" "), |_| " ");

        let escaped = alt((backslash, comma, equal, space));

        escape_or_fallback(normal, "\\", escaped)(i)
    }
}

/// Parse an unescaped piece of text, interspersed with
/// potentially-escaped characters. If the character *isn't* escaped,
/// treat it as a literal character.
fn escape_or_fallback<'a>(
    normal: impl FnMut(&'a str) -> IResult<&'a str, &'a str>,
    escape_char: &'static str,
    escaped: impl FnMut(&'a str) -> IResult<&'a str, &'a str>,
) -> impl FnOnce(&'a str) -> IResult<&'a str, EscapedStr<'a>> {
    move |i| {
        let (remaining, s) = escape_or_fallback_inner(normal, escape_char, escaped)(i)?;

        if s.ends_with('\\') {
            EndsWithBackslash.fail().map_err(nom::Err::Failure)
        } else {
            Ok((remaining, s))
        }
    }
}

fn escape_or_fallback_inner<'a, Error>(
    mut normal: impl FnMut(&'a str) -> IResult<&'a str, &'a str, Error>,
    escape_char: &'static str,
    mut escaped: impl FnMut(&'a str) -> IResult<&'a str, &'a str, Error>,
) -> impl FnMut(&'a str) -> IResult<&'a str, EscapedStr<'a>, Error>
where
    Error: nom::error::ParseError<&'a str>,
{
    move |i| {
        let mut result = SmallVec::<[&str; 4]>::new();
        let mut head = i;

        loop {
            match normal(head) {
                Ok((remaining, parsed)) => {
                    result.push(parsed);
                    head = remaining;
                }
                Err(nom::Err::Error(_)) => {
                    // FUTURE: https://doc.rust-lang.org/std/primitive.str.html#method.strip_prefix
                    if head.starts_with(escape_char) {
                        let after = &head[escape_char.len()..];

                        match escaped(after) {
                            Ok((remaining, parsed)) => {
                                result.push(parsed);
                                head = remaining;
                            }
                            Err(nom::Err::Error(_)) => {
                                result.push(escape_char);
                                head = after;

                                // The Go parser assumes that *any* unknown escaped character is
                                // valid.
                                match head.chars().next() {
                                    Some(c) => {
                                        let (escaped, remaining) = head.split_at(c.len_utf8());
                                        result.push(escaped);
                                        head = remaining;
                                    }
                                    None => return Ok((head, EscapedStr::from_slices(&result))),
                                }
                            }
                            Err(e) => return Err(e),
                        }
                    } else {
                        // have we parsed *anything*?
                        if head == i {
                            return Err(nom::Err::Error(Error::from_error_kind(
                                head,
                                nom::error::ErrorKind::EscapedTransform,
                            )));
                        } else {
                            return Ok((head, EscapedStr::from_slices(&result)));
                        }
                    }
                }
                Err(e) => return Err(e),
            }
        }
    }
}

/// This is a copied version of nom's `separated_list` that allows
/// parameterizing the created collection via closures.
pub fn parameterized_separated_list<I, O, O2, E, F, G, Ret>(
    mut sep: G,
    mut f: F,
    cre: impl FnOnce() -> Ret,
    mut add: impl FnMut(&mut Ret, O),
) -> impl FnOnce(I) -> IResult<I, Ret, E>
where
    I: Clone + PartialEq,
    F: FnMut(I) -> IResult<I, O, E>,
    G: FnMut(I) -> IResult<I, O2, E>,
    E: nom::error::ParseError<I>,
{
    move |mut i: I| {
        let mut res = cre();

        match f(i.clone()) {
            Err(nom::Err::Error(_)) => return Ok((i, res)),
            Err(e) => return Err(e),
            Ok((i1, o)) => {
                if i1 == i {
                    return Err(nom::Err::Error(E::from_error_kind(
                        i1,
                        nom::error::ErrorKind::SeparatedList,
                    )));
                }

                add(&mut res, o);
                i = i1;
            }
        }

        loop {
            match sep(i.clone()) {
                Err(nom::Err::Error(_)) => return Ok((i, res)),
                Err(e) => return Err(e),
                Ok((i1, _)) => {
                    if i1 == i {
                        return Err(nom::Err::Error(E::from_error_kind(
                            i1,
                            nom::error::ErrorKind::SeparatedList,
                        )));
                    }

                    match f(i1.clone()) {
                        Err(nom::Err::Error(_)) => return Ok((i, res)),
                        Err(e) => return Err(e),
                        Ok((i2, o)) => {
                            if i2 == i {
                                return Err(nom::Err::Error(E::from_error_kind(
                                    i2,
                                    nom::error::ErrorKind::SeparatedList,
                                )));
                            }

                            add(&mut res, o);
                            i = i2;
                        }
                    }
                }
            }
        }
    }
}

pub fn parameterized_separated_list1<I, O, O2, E, F, G, Ret>(
    mut sep: G,
    mut f: F,
    cre: impl FnOnce() -> Ret,
    mut add: impl FnMut(&mut Ret, O),
) -> impl FnOnce(I) -> IResult<I, Ret, E>
where
    I: Clone + PartialEq,
    F: FnMut(I) -> IResult<I, O, E>,
    G: FnMut(I) -> IResult<I, O2, E>,
    E: nom::error::ParseError<I>,
{
    move |i| {
        let (rem, first) = f(i)?;

        let mut res = cre();
        add(&mut res, first);

        match sep(rem.clone()) {
            Ok((rem, _)) => parameterized_separated_list(sep, f, move || res, add)(rem),
            Err(nom::Err::Error(_)) => Ok((rem, res)),
            Err(e) => Err(e),
        }
    }
}

/// This is a copied version of nom's `recognize` that runs the parser
/// **and** returns the entire matched input.
pub fn parse_and_recognize<
    I: Clone + nom::Offset + nom::Slice<std::ops::RangeTo<usize>>,
    O,
    E: nom::error::ParseError<I>,
    F,
>(
    mut parser: F,
) -> impl FnMut(I) -> IResult<I, (I, O), E>
where
    F: FnMut(I) -> IResult<I, O, E>,
{
    move |input: I| {
        let i = input.clone();
        match parser(i) {
            Ok((i, o)) => {
                let index = input.offset(&i);
                Ok((i, (input.slice(..index), o)))
            }
            Err(e) => Err(e),
        }
    }
}

/// This is very similar to nom's `map_res`, but creates a
/// `nom::Err::Failure` instead.
fn map_fail<'a, R1, R2>(
    mut first: impl FnMut(&'a str) -> IResult<&'a str, R1>,
    second: impl FnOnce(R1) -> Result<R2, Error>,
) -> impl FnOnce(&'a str) -> IResult<&'a str, R2> {
    move |i| {
        let (remaining, value) = first(i)?;

        match second(value) {
            Ok(v) => Ok((remaining, v)),
            Err(e) => Err(nom::Err::Failure(e)),
        }
    }
}

// copy / pasted from influxdb2_client to avoid a dependency on that crate

/// Characters to escape when writing measurement names
const MEASUREMENT_DELIMITERS: &[char] = &[',', ' '];

/// Characters to escape when writing tag keys
const TAG_KEY_DELIMITERS: &[char] = &[',', '=', ' '];

/// Characters to escape when writing tag values
const TAG_VALUE_DELIMITERS: &[char] = TAG_KEY_DELIMITERS;

/// Characters to escape when writing field keys
const FIELD_KEY_DELIMITERS: &[char] = TAG_KEY_DELIMITERS;

/// Characters to escape when writing string values in fields
const FIELD_VALUE_STRING_DELIMITERS: &[char] = &['"'];

/// Writes a str value to f, escaping all caracters in
/// escaping_escaping specificiation.
///
/// Use the constants defined in this module
fn escape_and_write_value(
    f: &mut fmt::Formatter<'_>,
    value: &str,
    escaping_specification: &[char],
) -> fmt::Result {
    let mut last = 0;

    for (idx, delim) in value.match_indices(escaping_specification) {
        let s = &value[last..idx];
        write!(f, r#"{}\{}"#, s, delim)?;
        last = idx + delim.len();
    }

    f.write_str(&value[last..])
}

#[cfg(test)]
mod test {
    use super::*;
    use smallvec::smallvec;
    use test_helpers::approximately_equal;

    impl FieldValue<'_> {
        fn unwrap_i64(&self) -> i64 {
            match self {
                Self::I64(v) => *v,
                _ => panic!("field was not an i64"),
            }
        }

        fn unwrap_u64(&self) -> u64 {
            match self {
                Self::U64(v) => *v,
                _ => panic!("field was not an u64"),
            }
        }

        fn unwrap_f64(&self) -> f64 {
            match self {
                Self::F64(v) => *v,
                _ => panic!("field was not an f64"),
            }
        }

        fn unwrap_string(&self) -> String {
            match self {
                Self::String(v) => v.to_string(),
                _ => panic!("field was not a String"),
            }
        }

        fn unwrap_bool(&self) -> bool {
            match self {
                Self::Boolean(v) => *v,
                _ => panic!("field was not a Bool"),
            }
        }
    }

    #[test]
    fn escaped_str_basic() {
        // Demonstrate how strings without any escapes are handled.
        let es = EscapedStr::from("Foo");
        assert_eq!(es, "Foo");
        assert!(!es.is_escaped(), "There are no escaped values");
        assert!(!es.ends_with('F'));
        assert!(!es.ends_with('z'));
        assert!(!es.ends_with("zz"));
        assert!(es.ends_with('o'));
        assert!(es.ends_with("oo"));
        assert!(es.ends_with("Foo"));
    }

    #[test]
    fn escaped_str_multi() {
        // Get an EscapedStr that has multiple parts by parsing a
        // measurement name with a non-whitespace escape character
        let (remaining, es) = measurement("Foo\\aBar").unwrap();
        assert!(remaining.is_empty());
        assert_eq!(es, EscapedStr::from_slices(&["Foo", "\\", "a", "Bar"]));
        assert!(es.is_escaped());

        // Test ends with across boundaries
        assert!(es.ends_with("Bar"));

        // Test PartialEq implementation for escaped str
        assert!(es == "Foo\\aBar");
        assert!(es != "Foo\\aBa");
        assert!(es != "Foo\\aBaz");
        assert!(es != "Foo\\a");
        assert!(es != "Foo\\");
        assert!(es != "Foo");
        assert!(es != "Fo");
        assert!(es != "F");
        assert!(es != "");
    }

    #[test]
    fn test_trim_leading() {
        assert_eq!(trim_leading(&String::from("")), "");
        assert_eq!(trim_leading(&String::from("  a b c ")), "a b c ");
        assert_eq!(trim_leading(&String::from("  a ")), "a ");
        assert_eq!(trim_leading(&String::from("\n  a ")), "a ");
        assert_eq!(trim_leading(&String::from("\t  a ")), "a ");

        // comments
        assert_eq!(trim_leading(&String::from("  #comment\n a ")), "a ");
        assert_eq!(trim_leading(&String::from("#comment\tcomment")), "");
        assert_eq!(
            trim_leading(&String::from("#comment\n #comment2\n#comment\na")),
            "a"
        );
    }

    #[test]
    fn test_split_lines() {
        assert_eq!(split_lines("").collect::<Vec<_>>(), vec![""]);
        assert_eq!(split_lines("foo").collect::<Vec<_>>(), vec!["foo"]);
        assert_eq!(
            split_lines("foo\nbar").collect::<Vec<_>>(),
            vec!["foo", "bar"]
        );
        assert_eq!(
            split_lines("foo\nbar\nbaz").collect::<Vec<_>>(),
            vec!["foo", "bar", "baz"]
        );

        assert_eq!(
            split_lines("foo\\nbar\nbaz").collect::<Vec<_>>(),
            vec!["foo\\nbar", "baz"]
        );
        assert_eq!(
            split_lines("meas tag=val field=1\nnext\n").collect::<Vec<_>>(),
            vec!["meas tag=val field=1", "next", ""]
        );
        assert_eq!(
            split_lines("meas tag=val field=\"\nval\"\nnext").collect::<Vec<_>>(),
            vec!["meas tag=val field=\"\nval\"", "next"]
        );
        assert_eq!(
            split_lines("meas tag=val field=\\\"\nval\"\nnext").collect::<Vec<_>>(),
            vec!["meas tag=val field=\\\"", "val\"", "next"]
        );
        assert_eq!(
            split_lines("meas tag=val field=1,field=\"\nval\"\nnext").collect::<Vec<_>>(),
            vec!["meas tag=val field=1,field=\"\nval\"", "next"]
        );
        assert_eq!(
            split_lines("meas tag=val field=1,field=\\\"\nval\"\nnext").collect::<Vec<_>>(),
            vec!["meas tag=val field=1,field=\\\"", "val\"", "next"]
        );
    }

    #[test]
    fn escaped_str_multi_to_string() {
        let (_, es) = measurement("Foo\\aBar").unwrap();
        // test the From<> implementation
        assert_eq!(es, "Foo\\aBar");
    }

    fn parse(s: &str) -> Result<Vec<ParsedLine<'_>>, super::Error> {
        super::parse_lines(s).collect()
    }

    #[test]
    fn parse_empty() {
        let input = "";
        let vals = parse(input);
        assert_eq!(vals.unwrap().len(), 0);
    }

    #[test]
    fn parse_no_fields() {
        let input = "foo 1234";
        let vals = parse(input);

        assert!(matches!(vals, Err(super::Error::FieldSetMissing)));
    }

    #[test]
    fn parse_single_field_integer() {
        let input = "foo asdf=23i 1234";
        let vals = parse(input).unwrap();

        assert_eq!(vals[0].series.measurement, "foo");
        assert_eq!(vals[0].timestamp, Some(1234));
        assert_eq!(vals[0].field_set[0].0, "asdf");
        assert_eq!(vals[0].field_set[0].1.unwrap_i64(), 23);
    }

    #[test]
    fn parse_single_field_unteger() {
        let input = "foo asdf=23u 1234";
        let vals = parse(input).unwrap();

        assert_eq!(vals[0].series.measurement, "foo");
        assert_eq!(vals[0].timestamp, Some(1234));
        assert_eq!(vals[0].field_set[0].0, "asdf");
        assert_eq!(vals[0].field_set[0].1.unwrap_u64(), 23);
    }

    #[test]
    fn parse_single_field_float_no_decimal() {
        let input = "foo asdf=44 546";
        let vals = parse(input).unwrap();

        assert_eq!(vals[0].series.measurement, "foo");
        assert_eq!(vals[0].timestamp, Some(546));
        assert_eq!(vals[0].field_set[0].0, "asdf");
        assert!(approximately_equal(
            vals[0].field_set[0].1.unwrap_f64(),
            44.0
        ));
    }

    #[test]
    fn parse_single_field_float_with_decimal() {
        let input = "foo asdf=3.74 123";
        let vals = parse(input).unwrap();

        assert_eq!(vals[0].series.measurement, "foo");
        assert_eq!(vals[0].timestamp, Some(123));
        assert_eq!(vals[0].field_set[0].0, "asdf");
        assert!(approximately_equal(
            vals[0].field_set[0].1.unwrap_f64(),
            3.74
        ));
    }

    #[test]
    fn parse_single_field_string() {
        let input = r#"foo asdf="the string value" 1234"#;
        let vals = parse(input).unwrap();

        assert_eq!(vals[0].series.measurement, "foo");
        assert_eq!(vals[0].timestamp, Some(1234));
        assert_eq!(vals[0].field_set[0].0, "asdf");
        assert_eq!(&vals[0].field_set[0].1.unwrap_string(), "the string value");
    }

    #[test]
    fn parse_single_field_bool() {
        let input = r#"foo asdf=true 1234"#;
        let vals = parse(input).unwrap();

        assert_eq!(vals[0].series.measurement, "foo");
        assert_eq!(vals[0].timestamp, Some(1234));
        assert_eq!(vals[0].field_set[0].0, "asdf");
        assert!(vals[0].field_set[0].1.unwrap_bool());
    }

    #[test]
    fn parse_string_values() {
        let test_data = vec![
            (r#"foo asdf="""#, ""),
            (r#"foo asdf="str val""#, "str val"),
            (r#"foo asdf="The \"string\" val""#, r#"The "string" val"#),
            (
                r#"foo asdf="The \"string w/ single double quote""#,
                r#"The "string w/ single double quote"#,
            ),
            // Examples from
            // https://docs.influxdata.com/influxdb/v1.8/write_protocols/line_protocol_tutorial/#special-characters
            (r#"foo asdf="too hot/cold""#, r#"too hot/cold"#),
            (r#"foo asdf="too hot\cold""#, r#"too hot\cold"#),
            (r#"foo asdf="too hot\\cold""#, r#"too hot\cold"#),
            (r#"foo asdf="too hot\\\cold""#, r#"too hot\\cold"#),
            (r#"foo asdf="too hot\\\\cold""#, r#"too hot\\cold"#),
            (r#"foo asdf="too hot\\\\\cold""#, r#"too hot\\\cold"#),
        ];

        for (input, expected_parsed_string_value) in test_data {
            let vals = parse(input).unwrap();
            assert_eq!(vals[0].series.tag_set, None);
            assert_eq!(vals[0].field_set.len(), 1);
            assert_eq!(vals[0].field_set[0].0, "asdf");
            assert_eq!(
                &vals[0].field_set[0].1.unwrap_string(),
                expected_parsed_string_value
            );
        }
    }

    #[test]
    fn parse_bool_values() {
        let test_data = vec![
            (r#"foo asdf=t"#, true),
            (r#"foo asdf=T"#, true),
            (r#"foo asdf=true"#, true),
            (r#"foo asdf=True"#, true),
            (r#"foo asdf=TRUE"#, true),
            (r#"foo asdf=f"#, false),
            (r#"foo asdf=F"#, false),
            (r#"foo asdf=false"#, false),
            (r#"foo asdf=False"#, false),
            (r#"foo asdf=FALSE"#, false),
        ];

        for (input, expected_parsed_bool_value) in test_data {
            let vals = parse(input).unwrap();
            assert_eq!(vals[0].series.tag_set, None);
            assert_eq!(vals[0].field_set.len(), 1);
            assert_eq!(vals[0].field_set[0].0, "asdf");
            assert_eq!(
                vals[0].field_set[0].1.unwrap_bool(),
                expected_parsed_bool_value
            );
        }
    }

    #[test]
    fn parse_two_fields_integer() {
        let input = "foo asdf=23i,bar=5i 1234";
        let vals = parse(input).unwrap();

        assert_eq!(vals[0].series.measurement, "foo");
        assert_eq!(vals[0].timestamp, Some(1234));

        assert_eq!(vals[0].field_set[0].0, "asdf");
        assert_eq!(vals[0].field_set[0].1.unwrap_i64(), 23);

        assert_eq!(vals[0].field_set[1].0, "bar");
        assert_eq!(vals[0].field_set[1].1.unwrap_i64(), 5);
    }

    #[test]
    fn parse_two_fields_unteger() {
        let input = "foo asdf=23u,bar=5u 1234";
        let vals = parse(input).unwrap();

        assert_eq!(vals[0].series.measurement, "foo");
        assert_eq!(vals[0].timestamp, Some(1234));

        assert_eq!(vals[0].field_set[0].0, "asdf");
        assert_eq!(vals[0].field_set[0].1.unwrap_u64(), 23);

        assert_eq!(vals[0].field_set[1].0, "bar");
        assert_eq!(vals[0].field_set[1].1.unwrap_u64(), 5);
    }

    #[test]
    fn parse_two_fields_float() {
        let input = "foo asdf=23.1,bar=5 1234";
        let vals = parse(input).unwrap();

        assert_eq!(vals[0].series.measurement, "foo");
        assert_eq!(vals[0].timestamp, Some(1234));

        assert_eq!(vals[0].field_set[0].0, "asdf");
        assert!(approximately_equal(
            vals[0].field_set[0].1.unwrap_f64(),
            23.1
        ));

        assert_eq!(vals[0].field_set[1].0, "bar");
        assert!(approximately_equal(
            vals[0].field_set[1].1.unwrap_f64(),
            5.0
        ));
    }

    #[test]
    fn parse_mixed_field_types() {
        let input = r#"foo asdf=23.1,bar=-5i,qux=9u,baz="the string",frab=false 1234"#;
        let vals = parse(input).unwrap();

        assert_eq!(vals[0].series.measurement, "foo");
        assert_eq!(vals[0].timestamp, Some(1234));

        assert_eq!(vals[0].field_set[0].0, "asdf");
        assert!(approximately_equal(
            vals[0].field_set[0].1.unwrap_f64(),
            23.1
        ));

        assert_eq!(vals[0].field_set[1].0, "bar");
        assert_eq!(vals[0].field_set[1].1.unwrap_i64(), -5);

        assert_eq!(vals[0].field_set[2].0, "qux");
        assert_eq!(vals[0].field_set[2].1.unwrap_u64(), 9);

        assert_eq!(vals[0].field_set[3].0, "baz");
        assert_eq!(vals[0].field_set[3].1.unwrap_string(), "the string");

        assert_eq!(vals[0].field_set[4].0, "frab");
        assert!(!vals[0].field_set[4].1.unwrap_bool());
    }

    #[test]
    fn parse_negative_integer() {
        let input = "m0 field=-1i 99";
        let vals = parse(input).unwrap();

        assert_eq!(vals.len(), 1);
        assert_eq!(vals[0].field_set[0].1.unwrap_i64(), -1);
    }

    #[test]
    fn parse_negative_uinteger() {
        let input = "m0 field=-1u 99";
        let parsed = parse(input);

        assert!(
            matches!(parsed, Err(super::Error::CannotParseEntireLine { .. })),
            "Wrong error: {:?}",
            parsed,
        );
    }

    #[test]
    fn parse_scientific_float() {
        // Positive tests
        let input = "m0 field=-1.234456e+06 1615869152385000000";
        let vals = parse(input).unwrap();
        assert_eq!(vals.len(), 1);

        let input = "m0 field=-1.234456E+3 1615869152385000000";
        let vals = parse(input).unwrap();
        assert_eq!(vals.len(), 1);

        let input = "m0 field=1.234456e+02 1615869152385000000";
        let vals = parse(input).unwrap();
        assert_eq!(vals.len(), 1);

        let input = "m0 field=1.234456E+16 1615869152385000000";
        let vals = parse(input).unwrap();
        assert_eq!(vals.len(), 1);

        let input = "m0 field=1.234456E-16";
        let vals = parse(input).unwrap();
        assert_eq!(vals.len(), 1);

        let input = "m0 field=1.234456e-03";
        let vals = parse(input).unwrap();
        assert_eq!(vals.len(), 1);

        let input = "m0 field=1.234456e-0";
        let vals = parse(input).unwrap();
        assert_eq!(vals.len(), 1);

        /////////////////////
        // Negative tests

        // NO "+" sign
        let input = "m0 field=-1.234456e06 1615869152385000000";
        let parsed = parse(input);
        assert!(
            matches!(parsed, Err(super::Error::CannotParseEntireLine { .. })),
            "Wrong error: {:?}",
            parsed,
        );

        let input = "m0 field=1.234456e06 1615869152385000000";
        let parsed = parse(input);
        assert!(
            matches!(parsed, Err(super::Error::CannotParseEntireLine { .. })),
            "Wrong error: {:?}",
            parsed,
        );

        let input = "m0 field=-1.234456E06 1615869152385000000";
        let parsed = parse(input);
        assert!(
            matches!(parsed, Err(super::Error::CannotParseEntireLine { .. })),
            "Wrong error: {:?}",
            parsed,
        );

        let input = "m0 field=1.234456E06 1615869152385000000";
        let parsed = parse(input);
        assert!(
            matches!(parsed, Err(super::Error::CannotParseEntireLine { .. })),
            "Wrong error: {:?}",
            parsed,
        );

        // No digits after e
        let input = "m0 field=-1.234456e 1615869152385000000";
        let parsed = parse(input);
        assert!(
            matches!(parsed, Err(super::Error::CannotParseEntireLine { .. })),
            "Wrong error: {:?}",
            parsed,
        );

        let input = "m0 field=-1.234456e+ 1615869152385000000";
        let parsed = parse(input);
        assert!(
            matches!(parsed, Err(super::Error::CannotParseEntireLine { .. })),
            "Wrong error: {:?}",
            parsed,
        );

        let input = "m0 field=-1.234456E 1615869152385000000";
        let parsed = parse(input);
        assert!(
            matches!(parsed, Err(super::Error::CannotParseEntireLine { .. })),
            "Wrong error: {:?}",
            parsed,
        );

        let input = "m0 field=-1.234456E+ 1615869152385000000";
        let parsed = parse(input);
        assert!(
            matches!(parsed, Err(super::Error::CannotParseEntireLine { .. })),
            "Wrong error: {:?}",
            parsed,
        );

        let input = "m0 field=-1.234456E-";
        let parsed = parse(input);
        assert!(
            matches!(parsed, Err(super::Error::CannotParseEntireLine { .. })),
            "Wrong error: {:?}",
            parsed,
        );
    }

    #[test]
    fn parse_negative_float() {
        let input = "m0 field2=-1 99";
        let vals = parse(input).unwrap();

        assert_eq!(vals.len(), 1);
        assert!(approximately_equal(
            vals[0].field_set[0].1.unwrap_f64(),
            -1.0
        ));
    }

    #[test]
    fn parse_out_of_range_integer() {
        let input = "m0 field=99999999999999999999999999999999i 99";
        let parsed = parse(input);

        assert!(
            matches!(parsed, Err(super::Error::IntegerValueInvalid { .. })),
            "Wrong error: {:?}",
            parsed,
        );
    }

    #[test]
    fn parse_out_of_range_uinteger() {
        let input = "m0 field=99999999999999999999999999999999u 99";
        let parsed = parse(input);

        assert!(
            matches!(parsed, Err(super::Error::UIntegerValueInvalid { .. })),
            "Wrong error: {:?}",
            parsed,
        );
    }

    #[test]
    fn parse_out_of_range_float() {
        let input = format!("m0 field={val}.{val} 99", val = "9".repeat(200));
        let parsed = parse(&input);

        assert!(
            matches!(parsed, Err(super::Error::FloatValueInvalid { .. })),
            "Wrong error: {:?}",
            parsed,
        );
    }

    #[test]
    fn parse_tag_set_included_in_series() {
        let input = "foo,tag1=1,tag2=2 value=1 123";
        let vals = parse(input).unwrap();

        assert_eq!(vals[0].series.measurement, "foo");

        assert_eq!(vals[0].series.tag_set.as_ref().unwrap()[0].0, "tag1");
        assert_eq!(vals[0].series.tag_set.as_ref().unwrap()[0].1, "1");

        assert_eq!(vals[0].series.tag_set.as_ref().unwrap()[1].0, "tag2");
        assert_eq!(vals[0].series.tag_set.as_ref().unwrap()[1].1, "2");

        assert_eq!(vals[0].field_set[0].0, "value");
    }

    #[test]
    fn parse_tag_set_unsorted() {
        let input = "foo,tag2=2,tag1=1";
        let (remaining, series) = series(input).unwrap();

        assert!(remaining.is_empty());
        assert_eq!(series.generate_base().unwrap(), "foo,tag1=1,tag2=2");
    }

    #[test]
    fn parse_tag_set_duplicate_tags() {
        let input = "foo,tag=1,tag=2";
        let (remaining, series) = series(input).unwrap();

        assert!(remaining.is_empty());
        let err = series
            .generate_base()
            .expect_err("Parsing duplicate tags should fail");

        assert_eq!(
            err.to_string(),
            r#"Must not contain duplicate tags, but "tag" was repeated"#
        );
    }

    #[test]
    fn parse_multiple_lines_become_multiple_points() {
        let input = r#"foo value1=1i 123
foo value2=2i 123"#;
        let vals = parse(input).unwrap();

        assert_eq!(vals[0].series.measurement, "foo");
        assert_eq!(vals[0].timestamp, Some(123));
        assert_eq!(vals[0].field_set[0].0, "value1");
        assert_eq!(vals[0].field_set[0].1.unwrap_i64(), 1);

        assert_eq!(vals[1].series.measurement, "foo");
        assert_eq!(vals[1].timestamp, Some(123));
        assert_eq!(vals[1].field_set[0].0, "value2");
        assert_eq!(vals[1].field_set[0].1.unwrap_i64(), 2);
    }

    #[test]
    fn parse_multiple_measurements_become_multiple_points() {
        let input = r#"foo value1=1i 123
bar value2=2i 123"#;
        let vals = parse(input).unwrap();

        assert_eq!(vals[0].series.measurement, "foo");
        assert_eq!(vals[0].timestamp, Some(123));
        assert_eq!(vals[0].field_set[0].0, "value1");
        assert_eq!(vals[0].field_set[0].1.unwrap_i64(), 1);

        assert_eq!(vals[1].series.measurement, "bar");
        assert_eq!(vals[1].timestamp, Some(123));
        assert_eq!(vals[1].field_set[0].0, "value2");
        assert_eq!(vals[1].field_set[0].1.unwrap_i64(), 2);
    }

    #[test]
    fn parse_trailing_whitespace_is_fine() {
        let input = r#"foo,tag=val value1=1i 123

"#;
        let vals = parse(input).unwrap();
        assert_eq!(vals.len(), 1);

        assert_eq!(vals[0].series.measurement, "foo");
        assert_eq!(vals[0].timestamp, Some(123));
        assert_eq!(vals[0].field_set[0].0, "value1");
        assert_eq!(vals[0].field_set[0].1.unwrap_i64(), 1);
    }

    #[test]
    fn parse_negative_timestamp() {
        let input = r#"foo value1=1i -123"#;
        let vals = parse(input).unwrap();

        assert_eq!(vals[0].series.measurement, "foo");
        assert_eq!(vals[0].timestamp, Some(-123));
        assert_eq!(vals[0].field_set[0].0, "value1");
        assert_eq!(vals[0].field_set[0].1.unwrap_i64(), 1);
    }

    #[test]
    fn parse_out_of_range_timestamp() {
        let input = "m0 field=1i 99999999999999999999999999999999";
        let parsed = parse(input);

        assert!(
            matches!(parsed, Err(super::Error::TimestampValueInvalid { .. })),
            "Wrong error: {:?}",
            parsed,
        );
    }

    #[test]
    fn parse_blank_lines_are_ignored() {
        let input = "\n\n\n";
        let vals = parse(input).unwrap();

        assert!(vals.is_empty());
    }

    #[test]
    fn parse_commented_lines_are_ignored() {
        let input = "# comment";
        let vals = parse(input).unwrap();

        assert!(vals.is_empty());
    }

    #[test]
    fn parse_multiple_whitespace_between_elements_is_allowed() {
        let input = "  measurement  a=1i  123  ";
        let vals = parse(input).unwrap();

        assert_eq!(vals[0].series.measurement, "measurement");
        assert_eq!(vals[0].timestamp, Some(123));
        assert_eq!(vals[0].field_set[0].0, "a");
        assert_eq!(vals[0].field_set[0].1.unwrap_i64(), 1);
    }

    macro_rules! assert_fully_parsed {
        ($parse_result:expr, $output:expr $(,)?) => {{
            let (remaining, parsed) = $parse_result.unwrap();

            assert!(
                remaining.is_empty(),
                "Some input remained to be parsed: {:?}",
                remaining,
            );
            assert_eq!(parsed, $output, "Did not parse the expected output");
        }};
    }

    #[test]
    fn measurement_allows_escaping_comma() {
        assert_fully_parsed!(measurement(r#"wea\,ther"#), r#"wea,ther"#);
    }

    #[test]
    fn measurement_allows_escaping_space() {
        assert_fully_parsed!(measurement(r#"wea\ ther"#), r#"wea ther"#);
    }

    #[test]
    fn measurement_allows_escaping_backslash() {
        assert_fully_parsed!(measurement(r#"\\wea\\ther"#), r#"\wea\ther"#);
    }

    #[test]
    fn measurement_allows_backslash_with_unknown_escape() {
        assert_fully_parsed!(measurement(r#"\wea\ther"#), r#"\wea\ther"#);
    }

    #[test]
    fn measurement_allows_literal_newline_as_unknown_escape() {
        assert_fully_parsed!(
            measurement(
                r#"weat\
her"#
            ),
            "weat\\\nher",
        );
    }

    #[test]
    fn measurement_disallows_literal_newline() {
        let (remaining, parsed) = measurement(
            r#"weat
her"#,
        )
        .unwrap();
        assert_eq!(parsed, "weat");
        assert_eq!(remaining, "\nher");
    }

    #[test]
    fn measurement_disallows_ending_in_backslash() {
        let parsed = measurement(r#"weather\"#);
        assert!(matches!(
            parsed,
            Err(nom::Err::Failure(super::Error::EndsWithBackslash))
        ));
    }

    #[test]
    fn tag_key_allows_escaping_comma() {
        assert_fully_parsed!(tag_key(r#"wea\,ther"#), r#"wea,ther"#);
    }

    #[test]
    fn tag_key_allows_escaping_equal() {
        assert_fully_parsed!(tag_key(r#"wea\=ther"#), r#"wea=ther"#);
    }

    #[test]
    fn tag_key_allows_escaping_space() {
        assert_fully_parsed!(tag_key(r#"wea\ ther"#), r#"wea ther"#);
    }

    #[test]
    fn tag_key_allows_escaping_backslash() {
        assert_fully_parsed!(tag_key(r#"\\wea\\ther"#), r#"\wea\ther"#);
    }

    #[test]
    fn tag_key_allows_backslash_with_unknown_escape() {
        assert_fully_parsed!(tag_key(r#"\wea\ther"#), r#"\wea\ther"#);
    }

    #[test]
    fn tag_key_allows_literal_newline_as_unknown_escape() {
        assert_fully_parsed!(
            tag_key(
                r#"weat\
her"#
            ),
            "weat\\\nher",
        );
    }

    #[test]
    fn tag_key_disallows_literal_newline() {
        let (remaining, parsed) = tag_key(
            r#"weat
her"#,
        )
        .unwrap();
        assert_eq!(parsed, "weat");
        assert_eq!(remaining, "\nher");
    }

    #[test]
    fn tag_key_disallows_ending_in_backslash() {
        let parsed = tag_key(r#"weather\"#);
        assert!(matches!(
            parsed,
            Err(nom::Err::Failure(super::Error::EndsWithBackslash))
        ));
    }

    #[test]
    fn tag_value_allows_escaping_comma() {
        assert_fully_parsed!(tag_value(r#"wea\,ther"#), r#"wea,ther"#);
    }

    #[test]
    fn tag_value_allows_escaping_equal() {
        assert_fully_parsed!(tag_value(r#"wea\=ther"#), r#"wea=ther"#);
    }

    #[test]
    fn tag_value_allows_escaping_space() {
        assert_fully_parsed!(tag_value(r#"wea\ ther"#), r#"wea ther"#);
    }

    #[test]
    fn tag_value_allows_escaping_backslash() {
        assert_fully_parsed!(tag_value(r#"\\wea\\ther"#), r#"\wea\ther"#);
    }

    #[test]
    fn tag_value_allows_backslash_with_unknown_escape() {
        assert_fully_parsed!(tag_value(r#"\wea\ther"#), r#"\wea\ther"#);
    }

    #[test]
    fn tag_value_allows_literal_newline_as_unknown_escape() {
        assert_fully_parsed!(
            tag_value(
                r#"weat\
her"#
            ),
            "weat\\\nher",
        );
    }

    #[test]
    fn tag_value_disallows_literal_newline() {
        let (remaining, parsed) = tag_value(
            r#"weat
her"#,
        )
        .unwrap();
        assert_eq!(parsed, "weat");
        assert_eq!(remaining, "\nher");
    }

    #[test]
    fn tag_value_disallows_ending_in_backslash() {
        let parsed = tag_value(r#"weather\"#);
        assert!(matches!(
            parsed,
            Err(nom::Err::Failure(super::Error::EndsWithBackslash))
        ));
    }

    #[test]
    fn field_key_allows_escaping_comma() {
        assert_fully_parsed!(field_key(r#"wea\,ther"#), r#"wea,ther"#);
    }

    #[test]
    fn field_key_allows_escaping_equal() {
        assert_fully_parsed!(field_key(r#"wea\=ther"#), r#"wea=ther"#);
    }

    #[test]
    fn field_key_allows_escaping_space() {
        assert_fully_parsed!(field_key(r#"wea\ ther"#), r#"wea ther"#);
    }

    #[test]
    fn field_key_allows_escaping_backslash() {
        assert_fully_parsed!(field_key(r#"\\wea\\ther"#), r#"\wea\ther"#);
    }

    #[test]
    fn field_key_allows_backslash_with_unknown_escape() {
        assert_fully_parsed!(field_key(r#"\wea\ther"#), r#"\wea\ther"#);
    }

    #[test]
    fn field_key_allows_literal_newline_as_unknown_escape() {
        assert_fully_parsed!(
            field_key(
                r#"weat\
her"#
            ),
            "weat\\\nher",
        );
    }

    #[test]
    fn field_key_disallows_literal_newline() {
        let (remaining, parsed) = field_key(
            r#"weat
her"#,
        )
        .unwrap();
        assert_eq!(parsed, "weat");
        assert_eq!(remaining, "\nher");
    }

    #[test]
    fn field_key_disallows_ending_in_backslash() {
        let parsed = field_key(r#"weather\"#);
        assert!(matches!(
            parsed,
            Err(nom::Err::Failure(super::Error::EndsWithBackslash))
        ));
    }

    #[test]
    fn parse_no_time() {
        let input = "foo,tag0=value1 asdf=23.1,bar=5i";
        let vals = parse(input).unwrap();

        assert_eq!(vals[0].series.measurement, "foo");
        assert_eq!(vals[0].series.tag_set.as_ref().unwrap()[0].0, "tag0");
        assert_eq!(vals[0].series.tag_set.as_ref().unwrap()[0].1, "value1");

        assert_eq!(vals[0].timestamp, None);

        assert_eq!(vals[0].field_set[0].0, "asdf");
        assert!(approximately_equal(
            vals[0].field_set[0].1.unwrap_f64(),
            23.1
        ));

        assert_eq!(vals[0].field_set[1].0, "bar");
        assert_eq!(vals[0].field_set[1].1.unwrap_i64(), 5);
    }

    #[test]
    fn parse_advance_after_error() {
        // Note that the first line has an error (23.1.22 is not a number)
        // but there is valid data afterwrds,
        let input = "foo,tag0=value1 asdf=23.1.22,jkl=4\n\
                     foo,tag0=value2 asdf=22.1,jkl=5";

        let vals: Vec<_> = super::parse_lines(input).collect();

        assert_eq!(vals.len(), 2);
        assert!(vals[0].is_err());
        assert_eq!(
            format!("{:?}", &vals[0]),
            "Err(CannotParseEntireLine { trailing_content: \".22,jkl=4\" })"
        );

        assert!(vals[1].is_ok());
        let parsed_line = vals[1].as_ref().expect("second line succeeded");
        assert_eq!(parsed_line.series.measurement, "foo");
        assert_eq!(parsed_line.series.tag_set.as_ref().unwrap()[0].0, "tag0");
        assert_eq!(parsed_line.series.tag_set.as_ref().unwrap()[0].1, "value2");

        assert_eq!(parsed_line.timestamp, None);

        assert_eq!(parsed_line.field_set[0].0, "asdf");
        assert!(approximately_equal(
            parsed_line.field_set[0].1.unwrap_f64(),
            22.1
        ));

        assert_eq!(parsed_line.field_set[1].0, "jkl");
        assert!(approximately_equal(
            parsed_line.field_set[1].1.unwrap_f64(),
            5.
        ));
    }

    #[test]
    fn field_value_display() {
        assert_eq!(FieldValue::I64(-42).to_string(), "-42i");
        assert_eq!(FieldValue::U64(42).to_string(), "42u");
        assert_eq!(FieldValue::F64(42.11).to_string(), "42.11");
        assert_eq!(
            FieldValue::String(EscapedStr::from("foo")).to_string(),
            "foo"
        );
        assert_eq!(FieldValue::Boolean(true).to_string(), "true");
        assert_eq!(FieldValue::Boolean(false).to_string(), "false");
    }

    #[test]
    fn series_display_no_tags() {
        let series = Series {
            raw_input: "foo",
            measurement: EscapedStr::from("m"),
            tag_set: None,
        };
        assert_eq!(series.to_string(), "m");
    }

    #[test]
    fn series_display_one_tag() {
        let series = Series {
            raw_input: "foo",
            measurement: EscapedStr::from("m"),
            tag_set: Some(smallvec![(
                EscapedStr::from("tag1"),
                EscapedStr::from("val1")
            )]),
        };
        assert_eq!(series.to_string(), "m,tag1=val1");
    }

    #[test]
    fn series_display_two_tags() {
        let series = Series {
            raw_input: "foo",
            measurement: EscapedStr::from("m"),
            tag_set: Some(smallvec![
                (EscapedStr::from("tag1"), EscapedStr::from("val1")),
                (EscapedStr::from("tag2"), EscapedStr::from("val2")),
            ]),
        };
        assert_eq!(series.to_string(), "m,tag1=val1,tag2=val2");
    }

    #[test]
    fn parsed_line_display_one_field_no_timestamp() {
        let series = Series {
            raw_input: "foo",
            measurement: EscapedStr::from("m"),
            tag_set: Some(smallvec![(
                EscapedStr::from("tag1"),
                EscapedStr::from("val1")
            ),]),
        };
        let field_set = smallvec![(EscapedStr::from("field1"), FieldValue::F64(42.1))];

        let parsed_line = ParsedLine {
            series,
            field_set,
            timestamp: None,
        };

        assert_eq!(parsed_line.to_string(), "m,tag1=val1 field1=42.1");
    }

    #[test]
    fn parsed_line_display_one_field_timestamp() {
        let series = Series {
            raw_input: "foo",
            measurement: EscapedStr::from("m"),
            tag_set: Some(smallvec![(
                EscapedStr::from("tag1"),
                EscapedStr::from("val1")
            ),]),
        };
        let field_set = smallvec![(EscapedStr::from("field1"), FieldValue::F64(42.1))];

        let parsed_line = ParsedLine {
            series,
            field_set,
            timestamp: Some(33),
        };

        assert_eq!(parsed_line.to_string(), "m,tag1=val1 field1=42.1 33");
    }

    #[test]
    fn parsed_line_display_two_fields_timestamp() {
        let series = Series {
            raw_input: "foo",
            measurement: EscapedStr::from("m"),
            tag_set: Some(smallvec![(
                EscapedStr::from("tag1"),
                EscapedStr::from("val1")
            ),]),
        };
        let field_set = smallvec![
            (EscapedStr::from("field1"), FieldValue::F64(42.1)),
            (EscapedStr::from("field2"), FieldValue::Boolean(false)),
        ];

        let parsed_line = ParsedLine {
            series,
            field_set,
            timestamp: Some(33),
        };

        assert_eq!(
            parsed_line.to_string(),
            "m,tag1=val1 field1=42.1,field2=false 33"
        );
    }

    #[test]
    fn parsed_line_display_escaped() {
        let series = Series {
            raw_input: "foo",
            measurement: EscapedStr::from("m,and m"),
            tag_set: Some(smallvec![(
                EscapedStr::from("tag ,1"),
                EscapedStr::from("val ,1")
            ),]),
        };
        let field_set = smallvec![(
            EscapedStr::from("field ,1"),
            FieldValue::String(EscapedStr::from("Foo\"Bar"))
        ),];

        let parsed_line = ParsedLine {
            series,
            field_set,
            timestamp: Some(33),
        };

        assert_eq!(
            parsed_line.to_string(),
            r#"m\,and\ m,tag\ \,1=val\ \,1 field\ \,1=Foo\"Bar 33"#
        );
    }

    #[test]
    fn field_value_returned() {
        let input = r#"foo asdf=true 1234"#;
        let vals = parse(input).unwrap();

        assert!(vals[0].field_value("asdf").unwrap().unwrap_bool());
    }

    #[test]
    fn field_value_missing() {
        let input = r#"foo asdf=true 1234"#;
        let vals = parse(input).unwrap();

        assert_eq!(vals[0].field_value("jkl"), None);
    }

    #[test]
    fn tag_value_returned() {
        let input = r#"foo,test=stuff asdf=true 1234"#;
        let vals = parse(input).unwrap();

        assert_eq!(*vals[0].tag_value("test").unwrap(), "stuff");
    }

    #[test]
    fn tag_value_missing() {
        let input = r#"foo,test=stuff asdf=true 1234"#;
        let vals = parse(input).unwrap();

        assert_eq!(vals[0].tag_value("asdf"), None);
    }
}
