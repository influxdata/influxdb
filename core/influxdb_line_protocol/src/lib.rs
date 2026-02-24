//! This crate contains pure Rust implementations of
//!
//! 1. A [parser](crate::parse_lines) for [InfluxDB Line Protocol] developed as part of the
//!    [InfluxDB IOx] project. This implementation is intended to be compatible with the [Go
//!    implementation], however, this implementation uses a [nom] combinator-based parser rather
//!    than attempting to port the imperative Go logic so there are likely some small differences.
//!
//! 2. A [builder](crate::builder::LineProtocolBuilder) to construct valid [InfluxDB Line Protocol]
//!
//! # Example
//!
//! Here is an example of how to parse the following line
//! protocol data into a `ParsedLine`:
//!
//! ```text
//!cpu,host=A,region=west usage_system=64.2 1590488773254420000
//!```
//!
//! ```
//! use influxdb_line_protocol::{ParsedLine, FieldValue};
//!
//! let mut parsed_lines =
//!     influxdb_line_protocol::parse_lines(
//!         "cpu,host=A,region=west usage_system=64i 1590488773254420000"
//!     );
//! let parsed_line = parsed_lines
//!     .next()
//!     .expect("Should have at least one line")
//!     .expect("Should parse successfully");
//!
//! let ParsedLine {
//!     series,
//!     field_set,
//!     timestamp,
//! } = parsed_line;
//!
//! assert_eq!(series.measurement, "cpu");
//!
//! let tags = series.tag_set.unwrap();
//! assert_eq!(tags[0].0, "host");
//! assert_eq!(tags[0].1, "A");
//! assert_eq!(tags[1].0, "region");
//! assert_eq!(tags[1].1, "west");
//!
//! let field = &field_set[0];
//! assert_eq!(field.0, "usage_system");
//! assert_eq!(field.1, FieldValue::I64(64));
//!
//! assert_eq!(timestamp, Some(1590488773254420000));
//! ```
//!
//! [InfluxDB Line Protocol]: https://v2.docs.influxdata.com/v2.0/reference/syntax/line-protocol
//! [Go implementation]: https://github.com/influxdata/influxdb/blob/217eddc87e14a79b01d0c22994fc139f530094a2/models/points_parser.go
//! [InfluxDB IOx]: https://github.com/influxdata/influxdb_iox
//! [nom]: https://crates.io/crates/nom

// Note this crate is published as its own crate on crates.io but kept in this repository for
// maintenance convenience.
//
// Thus this crate can't use workspace lints, so these lint configurations must be here.
#![deny(rustdoc::broken_intra_doc_links, rustdoc::bare_urls, rust_2018_idioms)]
#![warn(
    missing_copy_implementations,
    missing_debug_implementations,
    clippy::explicit_iter_loop,
    clippy::use_self,
    clippy::clone_on_ref_ptr,
    clippy::todo,
    clippy::dbg_macro,
    unused_crate_dependencies
)]
// DO NOT REMOVE these lint configurations; see note above!

pub mod builder;
pub use builder::LineProtocolBuilder;
pub mod v3;

#[cfg(feature = "test_helpers")]
pub mod test_helpers;

use fmt::Display;
use log::debug;
use nom::{
    Parser,
    branch::alt,
    bytes::complete::{tag, take_while1},
    character::complete::digit1,
    combinator::{cut, map, opt, recognize},
    error::context,
    multi::many0,
    sequence::{preceded, separated_pair, terminated},
};
use smallvec::SmallVec;
use snafu::{ResultExt, Snafu};
use std::{
    char,
    cmp::Ordering,
    fmt,
    hash::{Hash, Hasher},
    ops::Deref,
};

/// Maximum byte length for string components in line protocol
///
/// Default in IOx is 64 KiB per the [v2 spec]. The `large-strings` feature raises
/// this to 1 MiB for v1 migration compatibility.
#[cfg(not(feature = "large-strings"))]
const STRING_LENGTH_LIMIT_IN_BYTES: usize = 65_536;

#[cfg(feature = "large-strings")]
const STRING_LENGTH_LIMIT_IN_BYTES: usize = 1_048_576;

/// Maximum number of chars of context shown in error messages
const MAX_ERROR_CONTEXT_CHARS: usize = 10;

fn maybe_truncate_context(message: &str) -> String {
    if message.len() > MAX_ERROR_CONTEXT_CHARS {
        format!(
            "{}...",
            message
                .chars()
                .take(MAX_ERROR_CONTEXT_CHARS)
                .collect::<String>()
        )
    } else {
        message.to_string()
    }
}

/// Parsing errors that describe how a particular line is invalid line protocol.
#[derive(Debug, Snafu)]
#[non_exhaustive]
pub enum Error {
    #[snafu(display(r#"Must not contain duplicate tags, but `{}` was repeated"#, tag_key))]
    DuplicateTag { tag_key: String },

    #[snafu(display(r#"Invalid measurement was provided"#))]
    MeasurementValueInvalid,

    #[snafu(display(r#"No fields were provided"#))]
    FieldSetMissing,

    #[snafu(display(r#"Unable to parse integer value `{}`"#, value))]
    IntegerValueInvalid {
        source: std::num::ParseIntError,
        value: String,
    },

    #[snafu(display(r#"Unable to parse unsigned integer value `{}`"#, value))]
    UIntegerValueInvalid {
        source: std::num::ParseIntError,
        value: String,
    },

    #[snafu(display(r#"Unable to parse floating-point value `{}`"#, value))]
    FloatValueInvalid {
        source: std::num::ParseFloatError,
        value: String,
    },

    #[snafu(display(r#"Unable to parse timestamp value `{}`"#, value))]
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
        "Could not parse entire line. Found trailing content: `{}`",
        maybe_truncate_context(trailing_content)
    ))]
    CannotParseEntireLine { trailing_content: String },

    #[snafu(display(r#"Tag set malformed"#))]
    TagSetMalformed,

    #[snafu(display(
        r#"Tag set malformed: could not find {context} in `{}`"#,
        maybe_truncate_context(input)
    ))]
    TagSetMalformedWithContext {
        context: &'static str,
        input: String,
    },

    // TODO: Replace this with specific failures.
    #[snafu(display(r#"A generic parsing error occurred: {:?}"#, kind))]
    GenericParsingError {
        kind: nom::error::ErrorKind,
        #[expect(clippy::use_self, reason = "SNAFU derive generates intermediate types")]
        trace: Vec<Error>,
    },

    #[snafu(display("Expected {context}: {inner}"))]
    Context {
        context: &'static str,
        #[expect(clippy::use_self, reason = "SNAFU derive generates intermediate types")]
        inner: Box<Error>,
    },

    #[snafu(display("Expected at least one space character, got {}", if input.is_empty() {
            "end of input".into()
        } else {
            format!("`{input}`")
        }
    ))]
    ExpectedSpace { input: String },

    #[snafu(display("Expected tag key, got {}", if input.is_empty() {
            "end of input".into()
        } else {
            format!("`{}`", maybe_truncate_context(input))
        }
    ))]
    ExpectedTagKey { input: String },

    #[snafu(display("Expected tag value, got {}", if input.is_empty() {
            "end of input".into()
        } else {
            format!("`{}`", maybe_truncate_context(input))
        }
    ))]
    ExpectedTagValue { input: String },

    #[cfg_attr(
        not(feature = "large-strings"),
        snafu(display("String is greater than 64KB"))
    )]
    #[cfg_attr(
        feature = "large-strings",
        snafu(display("String is greater than 1MB"))
    )]
    FieldStringValueTooLarge,
}

/// A specialized [`Result`] type with a default error type of [`Error`].
///
/// [`Result`]: std::result::Result
pub type Result<T, E = Error> = std::result::Result<T, E>;
type IResult<I, T, E = Error> = nom::IResult<I, T, E>;

impl nom::error::ParseError<&str> for Error {
    fn from_error_kind(_input: &str, kind: nom::error::ErrorKind) -> Self {
        GenericParsingSnafu {
            kind,
            trace: vec![],
        }
        .build()
    }

    fn append(_input: &str, kind: nom::error::ErrorKind, other: Self) -> Self {
        GenericParsingSnafu {
            kind,
            trace: vec![other],
        }
        .build()
    }
}

impl nom::error::ContextError<&str> for Error {
    fn add_context(_input: &str, context: &'static str, inner: Self) -> Self {
        ContextSnafu {
            context,
            inner: Box::new(inner),
        }
        .build()
    }
}

/// Represents a single parsed line of line protocol data. See the [crate-level documentation](self)
/// for more information and examples.
#[derive(Debug)]
pub struct ParsedLine<'a> {
    pub series: Series<'a>,
    pub field_set: FieldSet<'a>,
    pub timestamp: Option<i64>,
}

impl Clone for ParsedLine<'static> {
    fn clone(&self) -> Self {
        Self {
            series: self.series.clone(),
            field_set: self.field_set.clone(),
            timestamp: self.timestamp,
        }
    }
}

impl<'a> ParsedLine<'a> {
    /// Total number of columns in this line, including fields, tags, and
    /// timestamp (timestamp is always present).
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

    /// Returns the value of the passed-in tag, if present.
    pub fn tag_value(&self, tag_key: &str) -> Option<&EscapedStr<'a>> {
        match &self.series.tag_set {
            Some(t) => {
                let t = t.iter().find(|(k, _)| *k == tag_key);
                t.map(|(_, val)| val)
            }
            None => None,
        }
    }

    /// Returns the value of the passed-in field, if present.
    pub fn field_value(&self, field_key: &str) -> Option<&FieldValue<'a>> {
        let f = self.field_set.iter().find(|(f, _)| *f == field_key);
        f.map(|(_, val)| val)
    }
}

/// Converts from a `ParsedLine` back to (canonical) line protocol
///
/// A note on validity: This code does not error or panic if the
/// `ParsedLine` represents invalid line protocol (for example, if it
/// has 0 fields).
///
/// Thus, if the `ParsedLine` represents invalid line protocol, then
/// the result of `Display` / `to_string()` will also be invalid.
impl Display for ParsedLine<'_> {
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
                write!(f, "={field_value}")?;
            }
        }

        if let Some(timestamp) = self.timestamp {
            write!(f, " {timestamp}")?
        }
        Ok(())
    }
}

/// Represents the identifier of a series (measurement, tagset) for
/// line protocol data
#[derive(Debug)]
pub struct Series<'a> {
    pub raw_input: &'a str,
    pub measurement: Measurement<'a>,
    pub tag_set: Option<TagSet<'a>>,
}

impl Clone for Series<'static> {
    fn clone(&self) -> Self {
        Self {
            raw_input: self.raw_input,
            measurement: self.measurement.clone(),
            tag_set: self.tag_set.clone(),
        }
    }
}

/// Converts `Series` back to line protocol
impl Display for Series<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        escape_and_write_value(f, self.measurement.as_str(), MEASUREMENT_DELIMITERS)?;
        if let Some(tag_set) = &self.tag_set {
            for (tag_name, tag_value) in tag_set {
                write!(f, ",")?;
                escape_and_write_value(f, tag_name.as_str(), TAG_KEY_DELIMITERS)?;
                write!(f, "=")?;
                escape_and_write_value(f, tag_value.as_str(), TAG_VALUE_DELIMITERS)?;
            }
        }
        Ok(())
    }
}

pub type Measurement<'a> = EscapedStr<'a>;

pub const OPTIMISTIC_MAX_NUM_FIELDS: usize = 4;

/// The [field] keys and values that appear in the line of line protocol.
///
/// [field]: https://docs.influxdata.com/influxdb/v2.0/reference/syntax/line-protocol/#field-set
pub type FieldSet<'a> = SmallVec<[(EscapedStr<'a>, FieldValue<'a>); OPTIMISTIC_MAX_NUM_FIELDS]>;

pub const OPTIMISTIC_MAX_NUM_TAGS: usize = 8;

/// The [tag] keys and values that appear in the line of line protocol.
///
/// [tag]: https://docs.influxdata.com/influxdb/v2.0/reference/syntax/line-protocol/#tag-set
pub type TagSet<'a> = SmallVec<[(EscapedStr<'a>, EscapedStr<'a>); OPTIMISTIC_MAX_NUM_TAGS]>;

/// Allowed types of fields in a `ParsedLine`. One of the types described in [the line protocol
/// reference].
///
/// [the line protocol reference]: https://docs.influxdata.com/influxdb/v2.0/reference/syntax/line-protocol/#data-types-and-format
#[derive(Debug, Clone, PartialEq)]
pub enum FieldValue<'a> {
    I64(i64),
    U64(u64),
    F64(f64),
    String(EscapedStr<'a>),
    Boolean(bool),
}

impl FieldValue<'_> {
    /// Returns true if `self` and `other` are of the same data type.
    pub fn is_same_type(&self, other: &Self) -> bool {
        std::mem::discriminant(self) == std::mem::discriminant(other)
    }
}

/// Converts `FieldValue` back to line protocol.
/// See [the line protocol reference] for more detail.
///
/// [the line protocol reference]: https://docs.influxdata.com/influxdb/v2.0/reference/syntax/line-protocol/#data-types-and-format
impl Display for FieldValue<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::I64(v) => write!(f, "{v}i"),
            Self::U64(v) => write!(f, "{v}u"),
            Self::F64(v) => write!(f, "{v}"),
            Self::String(v) => escape_and_write_value(f, v, FIELD_VALUE_STRING_DELIMITERS),
            Self::Boolean(v) => write!(f, "{v}"),
        }
    }
}

/// Represents a single logical string in the input.
///
/// We do not use `&str` directly here because the actual input may be
/// escaped, in which case the data in the input buffer is not
/// contiguous. This enum provides an interface to access all such
/// strings as contiguous string slices for compatibility with other
/// code, and is optimized for the common case where the
/// input is all in a contiguous string slice.
///
/// For example, the 8-character string `Foo\\Bar` (note the double
/// `\\`) is parsed into the logical 7-character string `Foo\Bar`
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
    fn from_slices(v: &[&'a str]) -> Self {
        match v.len() {
            0 => EscapedStr::SingleSlice(""),
            1 => EscapedStr::SingleSlice(v[0]),
            _ => EscapedStr::CopiedValue(v.join("")),
        }
    }

    /// Return the logical representation for the `EscapedStr` as a
    /// single slice. The slice might not point into the original
    /// buffer.
    pub fn as_str(&self) -> &str {
        self
    }
}

impl Deref for EscapedStr<'_> {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        match &self {
            EscapedStr::SingleSlice(s) => s,
            EscapedStr::CopiedValue(s) => s,
        }
    }
}

impl Hash for EscapedStr<'_> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.as_str().hash(state);
    }
}

impl PartialEq for EscapedStr<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.as_str() == other.as_str()
    }
}

impl PartialOrd for EscapedStr<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for EscapedStr<'_> {
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

/// Parses a new line-delimited string into an iterator of
/// [`ParsedLine`]. See the [crate-level documentation](self) for more
/// information and examples.
pub fn parse_lines(input: &str) -> impl Iterator<Item = Result<ParsedLine<'_>>> {
    split_lines(input).filter_map(|line| {
        let i = trim_leading(line);

        if i.is_empty() {
            return None;
        }

        let res = match parse_line(i) {
            Ok((remaining, line)) => {
                // should have parsed the whole input line; if any
                // data remains it is a parse error for this line.
                // Corresponding Go logic:
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
            debug!("Error parsing line: '{line}'. Error was {r:?}");
        }
        res
    })
}

/// Split `input` into individual lines to be parsed, based on the
/// rules of the line protocol format.
///
/// This code is more or less a direct port of the [Go implementation of
/// `scanLine`](https://github.com/influxdata/influxdb/blob/217eddc87e14a79b01d0c22994fc139f530094a2/models/points.go#L1078)
///
/// While this choice of implementation definitely means there is
/// logic duplication for scanning fields, duplicating it also means
/// we can be more sure of the compatibility of the Rust parser and
/// the canonical Go parser.
pub fn split_lines(input: &str) -> impl Iterator<Item = &str> {
    // NB: This is ported as closely as possible from the original Go code:
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
                // " this closing quote fixes editor highlighting that's confused by the prev line
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

    let line = (series, field_set, opt(timestamp));

    map(line, |(series, field_set, timestamp)| ParsedLine {
        series,
        field_set,
        timestamp,
    })
    .parse(i)
}

fn series(i: &str) -> IResult<&str, Series<'_>> {
    let series = (measurement, maybe_tagset);
    let series_and_raw_input = parse_and_recognize(series);

    map(
        series_and_raw_input,
        |(raw_input, (measurement, tag_set))| Series {
            raw_input,
            measurement,
            tag_set,
        },
    )
    .parse(i)
}

/// Tagsets are optional, but if a comma follows the measurement, then we must have at least one tag=value pair.
/// anything else is an error
fn maybe_tagset(i: &str) -> IResult<&str, Option<TagSet<'_>>, Error> {
    match tag::<&str, &str, Error>(",")(i) {
        Err(nom::Err::Error(_)) => Ok((i, None)),
        Ok((remainder, _)) => {
            match tag_set(remainder) {
                Ok((i, ts)) => {
                    // reaching here, we must find a tagset, which is at least one tag=value pair.
                    if ts.is_empty() {
                        return Err(nom::Err::Error(Error::TagSetMalformed));
                    }
                    Ok((i, Some(ts)))
                }
                Err(nom::Err::Error(_)) => TagSetMalformedSnafu.fail().map_err(nom::Err::Failure),
                Err(nom::Err::Failure(Error::Context { context, .. })) => {
                    TagSetMalformedWithContextSnafu {
                        input: remainder,
                        context,
                    }
                    .fail()
                    .map_err(nom::Err::Failure)
                }
                Err(e) => Err(e),
            }
        }
        Err(e) => Err(e),
    }
}

fn measurement(i: &str) -> IResult<&str, Measurement<'_>, Error> {
    let normal_char = take_while1(|c| {
        !is_whitespace_boundary_char(c) && !is_null_char(c) && c != ',' && c != '\\'
    });

    let space = map(tag::<_, _, Error>(" "), |_| " ");
    let comma = map(tag(","), |_| ",");
    let backslash = map(tag("\\"), |_| "\\");

    let escaped = alt((comma, space, backslash));

    match escape_or_fallback(normal_char, "\\", escaped)(i) {
        Err(nom::Err::Error(_)) => MeasurementValueInvalidSnafu.fail().map_err(nom::Err::Error),
        other => other,
    }
}

fn tag_set(i: &str) -> IResult<&str, TagSet<'_>> {
    let one_tag = separated_pair(
        cut(tag_key),
        context("equals sign", cut(tag("="))),
        cut(tag_value),
    );
    parameterized_separated_list(tag(","), one_tag, SmallVec::new, |v, i| v.push(i))(i)
}

fn tag_key(input: &str) -> IResult<&str, EscapedStr<'_>> {
    let normal_char =
        take_while1::<_, _, Error>(|c| !is_whitespace_boundary_char(c) && c != '=' && c != '\\');

    escaped_value(normal_char)(input).map_err(|e: nom::Err<Error>| match e {
        nom::Err::Error(Error::GenericParsingError {
            kind: nom::error::ErrorKind::TakeWhile1,
            ..
        }) => nom::Err::Error(ExpectedTagKeySnafu { input }.build()),
        other => other,
    })
}

fn tag_value(input: &str) -> IResult<&str, EscapedStr<'_>> {
    let normal_char =
        take_while1::<_, _, Error>(|c| !is_whitespace_boundary_char(c) && c != ',' && c != '\\');

    escaped_value(normal_char)(input).map_err(|e: nom::Err<Error>| match e {
        nom::Err::Error(Error::GenericParsingError {
            kind: nom::error::ErrorKind::TakeWhile1,
            ..
        }) => nom::Err::Error(ExpectedTagValueSnafu { input }.build()),
        other => other,
    })
}

fn field_set(i: &str) -> IResult<&str, FieldSet<'_>> {
    let one_field = separated_pair(field_key, tag("="), field_value);
    let sep = tag(",");

    match parameterized_separated_list1(sep, one_field, SmallVec::new, |v, i| v.push(i))(i) {
        Err(nom::Err::Error(_)) => FieldSetMissingSnafu.fail().map_err(nom::Err::Error),
        other => other,
    }
}

fn field_key(i: &str) -> IResult<&str, EscapedStr<'_>> {
    let normal_char =
        take_while1::<_, _, Error>(|c| !is_whitespace_boundary_char(c) && c != '=' && c != '\\');
    escaped_value(normal_char)(i)
}

fn field_value(i: &str) -> IResult<&str, FieldValue<'_>> {
    let int = map(field_integer_value, FieldValue::I64);
    let uint = map(field_uinteger_value, FieldValue::U64);
    let float = map(field_float_value, FieldValue::F64);
    let string = map(field_string_value, FieldValue::String);
    let boolv = map(field_bool_value, FieldValue::Boolean);

    alt((int, uint, float, string, boolv)).parse(i)
}

fn field_integer_value(i: &str) -> IResult<&str, i64> {
    let tagged_value = terminated(integral_value_signed, tag("i"));
    map_fail(tagged_value, |value| {
        value.parse().context(IntegerValueInvalidSnafu { value })
    })(i)
}

fn field_uinteger_value(i: &str) -> IResult<&str, u64> {
    let tagged_value = terminated(digit1, tag("u"));
    map_fail(tagged_value, |value| {
        value.parse().context(UIntegerValueInvalidSnafu { value })
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
        value.parse().context(FloatValueInvalidSnafu { value })
    })(i)
}

fn field_float_value_with_decimal(i: &str) -> IResult<&str, &str> {
    recognize(separated_pair(integral_value_signed, tag("."), digit1)).parse(i)
}

fn field_float_value_with_exponential_and_decimal(i: &str) -> IResult<&str, &str> {
    recognize(separated_pair(
        integral_value_signed,
        tag("."),
        exponential_value,
    ))
    .parse(i)
}

fn field_float_value_with_exponential_no_decimal(i: &str) -> IResult<&str, &str> {
    recognize(preceded(opt(tag("-")), exponential_value)).parse(i)
}

fn exponential_value(i: &str) -> IResult<&str, &str> {
    recognize(separated_pair(
        digit1,
        (alt((tag("e"), tag("E"))), opt(alt((tag("-"), tag("+"))))),
        digit1,
    ))
    .parse(i)
}

fn field_float_value_no_decimal(i: &str) -> IResult<&str, &str> {
    integral_value_signed(i)
}

fn integral_value_signed(i: &str) -> IResult<&str, &str> {
    recognize(preceded(opt(tag("-")), digit1)).parse(i)
}

fn timestamp(i: &str) -> IResult<&str, i64> {
    map_fail(integral_value_signed, |value| {
        value.parse().context(TimestampValueInvalidSnafu { value })
    })(i)
}

fn field_string_value(i: &str) -> IResult<&str, EscapedStr<'_>> {
    // https://docs.influxdata.com/influxdb/v2.0/reference/syntax/line-protocol/#data-types-and-format
    // For string field values, backslash is only used to escape itself (`\`) or double
    // quotes.
    let string_data = alt((
        map(tag(r#"\""#), |_| r#"""#), // escaped double quote -> double quote
        map(tag(r"\\"), |_| r"\"),     // escaped backslash --> single backslash
        tag(r"\"),                     // unescaped single backslash
        take_while1(|c| c != '\\' && c != '"'), // anything else w/ no special handling
    ));

    // NB: `many0` doesn't allow combinators that match the empty string so
    // we need to special case a pair of double quotes.
    let empty_str = map(tag(r#""""#), |_| Vec::new());

    let quoted_str = alt((
        preceded(tag("\""), terminated(many0(string_data), tag("\""))),
        empty_str,
    ));

    map_fail(quoted_str, |value| {
        let size = value.iter().map(|s| s.len()).sum::<usize>();
        if STRING_LENGTH_LIMIT_IN_BYTES >= size {
            Ok(EscapedStr::from_slices(&value))
        } else {
            Err(Error::FieldStringValueTooLarge)
        }
    })(i)
}

fn field_bool_value(i: &str) -> IResult<&str, bool> {
    // https://docs.influxdata.com/influxdb/v2.0/reference/syntax/line-protocol/#data-types-and-format
    // "specify TRUE with t, T, true, True, or TRUE. Specify FALSE with f, F, false,
    // False, or FALSE"
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
    ))
    .parse(i)
}

/// Truncates the input slice to remove all whitespace from the
/// beginning (left), including completely commented-out lines
fn trim_leading(mut i: &str) -> &str {
    loop {
        let offset = i
            .find(|c| !is_whitespace_boundary_char(c))
            .unwrap_or(i.len());
        i = &i[offset..];

        if i.starts_with('#') {
            let offset = i.find('\n').unwrap_or(i.len());
            i = &i[offset..];
        } else {
            break i;
        }
    }
}

fn whitespace(input: &str) -> IResult<&str, &str> {
    take_while1(|c| c == ' ')(input)
        .map_err(|_e: nom::Err<Error>| nom::Err::Error(ExpectedSpaceSnafu { input }.build()))
}

fn is_whitespace_boundary_char(c: char) -> bool {
    c == ' ' || c == '\t' || c == '\n'
}

fn is_null_char(c: char) -> bool {
    c == '\0'
}

/// While not all of these escape characters are required to be
/// escaped, we support the client escaping them proactively to
/// provide a common experience.
fn escaped_value<'a>(
    normal: impl Parser<&'a str, Output = &'a str, Error = Error>,
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
    normal: impl Parser<&'a str, Output = &'a str, Error = Error>,
    escape_char: &'static str,
    escaped: impl Parser<&'a str, Output = &'a str, Error = Error>,
) -> impl FnOnce(&'a str) -> IResult<&'a str, EscapedStr<'a>, Error> {
    move |i| {
        let (remaining, s) = escape_or_fallback_inner(normal, escape_char, escaped)(i)?;

        if s.ends_with('\\') {
            EndsWithBackslashSnafu.fail().map_err(nom::Err::Failure)
        } else if s.len() > STRING_LENGTH_LIMIT_IN_BYTES {
            FieldStringValueTooLargeSnafu
                .fail()
                .map_err(nom::Err::Failure)
        } else {
            Ok((remaining, s))
        }
    }
}

fn escape_or_fallback_inner<'a, Error>(
    mut normal: impl Parser<&'a str, Output = &'a str, Error = Error>,
    escape_char: &'static str,
    mut escaped: impl Parser<&'a str, Output = &'a str, Error = Error>,
) -> impl FnMut(&'a str) -> IResult<&'a str, EscapedStr<'a>, Error>
where
    Error: nom::error::ParseError<&'a str>,
{
    move |i| {
        let mut result = SmallVec::<[&str; 4]>::new();
        let mut head = i;

        loop {
            match normal.parse(head) {
                Ok((remaining, parsed)) => {
                    result.push(parsed);
                    head = remaining;
                }
                Err(nom::Err::Error(e)) => {
                    // FUTURE: https://doc.rust-lang.org/std/primitive.str.html#method.strip_prefix
                    if head.starts_with(escape_char) {
                        let after = &head[escape_char.len()..];

                        match escaped.parse(after) {
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
                            return Err(nom::Err::Error(e));
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
fn parameterized_separated_list<I, O, O2, E, F, G, Ret>(
    mut sep: G,
    mut f: F,
    cre: impl FnOnce() -> Ret,
    mut add: impl FnMut(&mut Ret, O),
) -> impl FnOnce(I) -> IResult<I, Ret, E>
where
    I: Clone + PartialEq,
    F: Parser<I, Output = O, Error = E>,
    G: Parser<I, Output = O2, Error = E>,
    E: nom::error::ParseError<I>,
{
    move |mut i: I| {
        let mut res = cre();

        match f.parse(i.clone()) {
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
            match sep.parse(i.clone()) {
                Err(nom::Err::Error(_)) => return Ok((i, res)),
                Err(e) => return Err(e),
                Ok((i1, _)) => {
                    if i1 == i {
                        return Err(nom::Err::Error(E::from_error_kind(
                            i1,
                            nom::error::ErrorKind::SeparatedList,
                        )));
                    }

                    match f.parse(i1.clone()) {
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

fn parameterized_separated_list1<I, O, O2, E, F, G, Ret>(
    mut sep: G,
    mut f: F,
    cre: impl FnOnce() -> Ret,
    mut add: impl FnMut(&mut Ret, O),
) -> impl FnOnce(I) -> IResult<I, Ret, E>
where
    I: Clone + PartialEq,
    F: Parser<I, Output = O, Error = E>,
    G: Parser<I, Output = O2, Error = E>,
    E: nom::error::ParseError<I> + std::fmt::Debug,
{
    move |i| {
        let (rem, first) = f.parse(i)?;

        let mut res = cre();
        add(&mut res, first);

        match sep.parse(rem.clone()) {
            Ok((rem, _)) => parameterized_separated_list(sep, f, move || res, add)(rem),
            Err(nom::Err::Error(_)) => Ok((rem, res)),
            Err(e) => Err(e),
        }
    }
}

/// This is a copied version of nom's `recognize` that runs the parser
/// **and** returns the entire matched input.
fn parse_and_recognize<I: Clone + nom::Offset + nom::Input, O, E: nom::error::ParseError<I>, F>(
    mut parser: F,
) -> impl FnMut(I) -> IResult<I, (I, O), E>
where
    F: Parser<I, Output = O, Error = E>,
{
    move |input: I| {
        let i = input.clone();
        match parser.parse(i) {
            Ok((i, o)) => {
                let index = input.offset(&i);
                Ok((i, (input.take(index), o)))
            }
            Err(e) => Err(e),
        }
    }
}

/// This is very similar to nom's `map_res`, but creates a
/// `nom::Err::Failure` instead.
fn map_fail<'a, R1, R2>(
    mut first: impl Parser<&'a str, Output = R1, Error = Error>,
    second: impl FnOnce(R1) -> Result<R2, Error>,
) -> impl FnOnce(&'a str) -> IResult<&'a str, R2> {
    move |i| {
        let (remaining, value) = first.parse(i)?;

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
const FIELD_VALUE_STRING_DELIMITERS: &[char] = &['"']; // " Close quotes for buggy editor

/// Writes a `&str` value to `f`, escaping all characters in
/// `escaping_specificiation`.
///
/// Use the constants defined in this module.
fn escape_and_write_value(
    f: &mut fmt::Formatter<'_>,
    value: &str,
    escaping_specification: &[char],
) -> fmt::Result {
    let mut last = 0;

    for (idx, delim) in value.match_indices(escaping_specification) {
        let s = &value[last..idx];
        write!(f, r#"{s}\{delim}"#)?;
        last = idx + delim.len();
    }

    f.write_str(&value[last..])
}

#[cfg(test)]
mod test {
    use super::*;
    use ::test_helpers::{approximately_equal, assert_error};
    use smallvec::smallvec;

    #[test]
    fn better_error_messages() {
        let lp = "measurement";
        let result = parse_lines(lp).next().unwrap().unwrap_err();
        assert_eq!(
            "Expected at least one space character, got end of input",
            result.to_string()
        );

        let lp = ",";
        let result = parse_lines(lp).next().unwrap().unwrap_err();
        assert_eq!("Invalid measurement was provided", result.to_string());

        let lp = "J,#";
        let result = parse_lines(lp).next().unwrap().unwrap_err();
        assert_eq!(
            "Tag set malformed: could not find equals sign in `#`",
            result.to_string()
        );

        let lp = format!("J,{}", "a".repeat(200));
        let result = parse_lines(&lp).next().unwrap().unwrap_err();
        assert_eq!(
            "Tag set malformed: could not find equals sign in `aaaaaaaaaa...`",
            result.to_string()
        );

        let lp = "J,";
        let result = parse_lines(lp).next().unwrap().unwrap_err();
        assert_eq!("Expected tag key, got end of input", result.to_string());

        let lp = "testmeasure, bar=1i";
        let result = parse_lines(lp).next().unwrap().unwrap_err();
        assert_eq!("Expected tag key, got ` bar=1i`", result.to_string());

        let lp = format!("testmeasure, {}=1i", "b".repeat(200));
        let result = parse_lines(&lp).next().unwrap().unwrap_err();
        assert_eq!("Expected tag key, got ` bbbbbbbbb...`", result.to_string());

        let lp = "0,,=";
        let result = parse_lines(lp).next().unwrap().unwrap_err();
        assert_eq!("Expected tag value, got end of input", result.to_string());

        let lp = "testmeasure,foo\\=\\,baz= bar=1i";
        let result = parse_lines(lp).next().unwrap().unwrap_err();
        assert_eq!("Expected tag value, got ` bar=1i`", result.to_string());

        let lp = format!("testmeasure,foo\\=\\,baz= bar={}i", "1".repeat(100));
        let result = parse_lines(&lp).next().unwrap().unwrap_err();
        assert_eq!(
            "Expected tag value, got ` bar=11111...`",
            result.to_string()
        );

        let lp = "testmeasure,foo=,baz= bar=1i";
        let result = parse_lines(lp).next().unwrap().unwrap_err();
        assert_eq!(
            "Expected tag value, got `,baz= bar=...`",
            result.to_string()
        );

        let lp = "metrics,bananas=,platanos=great value=1.000000 1725629157696678000";
        let result = parse_lines(lp).next().unwrap().unwrap_err();
        assert_eq!(
            "Expected tag value, got `,platanos=...`",
            result.to_string()
        );

        let lp = "metrics,\
            metrics_name=kube_node_spec_taint,node=10.0.213.170,itsEmpty=,effect=NoSchedule \
            value=1.000000 \
            1725628131752524000";
        let result = parse_lines(lp).next().unwrap().unwrap_err();
        assert_eq!(
            "Expected tag value, got `,effect=No...`",
            result.to_string()
        );

        let lp = "testmeasure,foo= sed=1i";
        let result = parse_lines(lp).next().unwrap().unwrap_err();
        assert_eq!("Expected tag value, got ` sed=1i`", result.to_string());

        let lp = "testmeasure,foo=f bar=1i,baz=";
        let result = parse_lines(lp).next().unwrap().unwrap_err();
        assert_eq!(
            "Could not parse entire line. Found trailing content: `baz=`",
            result.to_string()
        );

        let lp = format!("testmeasure,foo=f bar=1i,ba{}=", "z".repeat(100));
        let result = parse_lines(&lp).next().unwrap().unwrap_err();
        assert_eq!(
            "Could not parse entire line. Found trailing content: `bazzzzzzzz...`",
            result.to_string()
        );

        let lp = "testmeasure,foo\\=\\,baz= bar=1i";
        let result = parse_lines(lp).next().unwrap().unwrap_err();
        assert_eq!("Expected tag value, got ` bar=1i`", result.to_string());

        let lp = "\u{1b}# /";
        let result = parse_lines(lp).next().unwrap().unwrap_err();
        assert_eq!("No fields were provided", result.to_string());

        let lp = ":\u{14}\\";
        let result = parse_lines(lp).next().unwrap().unwrap_err();
        assert_eq!(
            "Measurements, tag keys and values, and field keys may not end with a backslash",
            result.to_string()
        );

        let lp = "testmeasure field=3.0 \
        188888888888888888888888888888888888888888888888888888888888888888888888";
        let result = parse_lines(lp).next().unwrap().unwrap_err();
        assert_eq!(
            "Unable to parse timestamp value \
            `188888888888888888888888888888888888888888888888888888888888888888888888`",
            result.to_string()
        );

        let lp = "testmeasure field=66666666666666666666666666666666666666666666668i";
        let result = parse_lines(lp).next().unwrap().unwrap_err();
        assert_eq!(
            "Unable to parse integer value \
            `66666666666666666666666666666666666666666666668`",
            result.to_string()
        );

        let lp = "goodline,tag1=one,tag2=2 value=1 123
        badline,
        anothergoodline,tag3=3,tag4=4 value=67 1234";
        let result: Vec<_> = parse_lines(lp)
            .filter_map(|r| r.err().map(|e| e.to_string()))
            .collect();
        assert_eq!(
            &[String::from("Expected tag key, got end of input")],
            &result.as_slice(),
        );
    }

    impl FieldValue<'_> {
        pub(crate) fn unwrap_i64(&self) -> i64 {
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
    fn parse_lines_returns_all_lines_even_when_a_line_errors() {
        let input = ",tag1=1,tag2=2 value=1 123\nm,tag1=one,tag2=2 value=1 123";
        let vals = super::parse_lines(input).collect::<Vec<Result<_>>>();
        assert!(matches!(
            &vals[..],
            &[Err(Error::MeasurementValueInvalid), Ok(_)]
        ));
    }

    #[test]
    fn escaped_str_basic() {
        // Demonstrate how strings without any escapes are handled.
        let es = EscapedStr::from("Foo");
        assert_eq!(es, "Foo");
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

        // Test `ends_with` across boundaries
        assert!(es.ends_with("Bar"));

        // Test PartialEq implementation for escaped str
        assert_eq!(es, "Foo\\aBar");
        assert_ne!(es, "Foo\\aBa");
        assert_ne!(es, "Foo\\aBaz");
        assert_ne!(es, "Foo\\a");
        assert_ne!(es, "Foo\\");
        assert_ne!(es, "Foo");
        assert_ne!(es, "Fo");
        assert_ne!(es, "F");
        assert_ne!(es, "");
    }

    #[test]
    fn optionally_escaped_strs_are_equal_and_hash_the_same() {
        let (_remaining, field_name_without_escaping) = field_key("foo,bar").unwrap();
        assert!(field_name_without_escaping == "foo,bar");

        let (_remaining, field_name_with_escaping) = field_key("foo\\,bar").unwrap();
        assert!(field_name_with_escaping == "foo,bar");

        assert_eq!(field_name_without_escaping, field_name_with_escaping);
        assert_eq!(
            calculate_hash(&field_name_without_escaping),
            calculate_hash(&field_name_with_escaping)
        );
    }

    fn calculate_hash<T: std::hash::Hash>(t: &T) -> u64 {
        let mut s = std::collections::hash_map::DefaultHasher::new();
        t.hash(&mut s);
        s.finish()
    }

    #[test]
    fn test_trim_leading() {
        assert_eq!(trim_leading(""), "");
        assert_eq!(trim_leading("  a b c "), "a b c ");
        assert_eq!(trim_leading("  a "), "a ");
        assert_eq!(trim_leading("\n  a "), "a ");
        assert_eq!(trim_leading("\t  a "), "a ");

        // comments
        assert_eq!(trim_leading("  #comment\n a "), "a ");
        assert_eq!(trim_leading("#comment\tcomment"), "");
        assert_eq!(trim_leading("#comment\n #comment2\n#comment\na"), "a");
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

    // tests that an incomplete tag=value pair returns an error about a malformed tagset
    #[test]
    fn parse_tag_no_value() {
        let input = "testmeasure,foo= bar=1i";
        assert_error!(parse(input), Error::ExpectedTagValue { ref input } if input == " bar=1i");
    }

    // tests that just a comma after the measurement is an error
    #[test]
    fn parse_no_tagset() {
        let input = "testmeasure, bar=1i";
        assert_error!(parse(input), Error::ExpectedTagKey { ref input } if input == " bar=1i");
    }

    #[test]
    fn parse_no_measurement() {
        let input = ",tag1=1,tag2=2 value=1 123";
        assert_error!(parse(input), Error::MeasurementValueInvalid);

        // accepts `field=1` as measurement, and errors on missing field
        let input = "field=1 1234";
        assert_error!(parse(input), Error::FieldSetMissing);
    }

    // matches behavior in influxdb golang parser
    #[test]
    fn parse_measurement_with_eq() {
        let input = "tag1=1 field=1 1234";
        let vals = parse(input);
        assert!(vals.is_ok());

        let input = "tag1=1,tag2=2 value=1 123";
        let vals = parse(input);
        assert!(vals.is_ok());
    }

    #[test]
    fn parse_null_measurement() {
        let input = "\0 field=1 1234";
        assert_error!(parse(input), Error::MeasurementValueInvalid);

        let input = "\0,tag1=1,tag2=2 value=1 123";
        assert_error!(parse(input), Error::MeasurementValueInvalid);
    }

    #[test]
    fn parse_where_nulls_accepted() {
        let input = "m,tag\x001=one,tag2=2 value=1 123
            m,tag1=o\0ne,tag2=2 value=1 123
            m,tag1=one,tag2=\0 value=1 123
            m,tag1=one,tag2=2 val\0ue=1 123
            m,tag1=one,tag2=2 value=\"v\0\" 123";
        let vals = parse(input);
        assert!(vals.is_ok());
        assert_eq!(vals.unwrap().len(), 5);
    }

    #[test]
    fn parse_no_fields() {
        let input = "foo 1234";

        assert_error!(parse(input), Error::FieldSetMissing);
    }

    #[test]
    fn parse_null_in_field_value() {
        let input = "m,tag1=one,tag2=2 value=\0 123";
        assert_error!(parse(input), Error::FieldSetMissing);
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
            (r#"foo asdf="too hot\cold""#, r"too hot\cold"),
            (r#"foo asdf="too hot\\cold""#, r"too hot\cold"),
            (r#"foo asdf="too hot\\\cold""#, r"too hot\\cold"),
            (r#"foo asdf="too hot\\\\cold""#, r"too hot\\cold"),
            (r#"foo asdf="too hot\\\\\cold""#, r"too hot\\\cold"),
            (r#"foo asdf="too hot\\\\\\cold""#, r"too hot\\\cold"),
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

        assert_error!(parse(input), Error::CannotParseEntireLine { .. });
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

        let input = "m0 field=1e-0";
        let vals = parse(input).unwrap();
        assert_eq!(vals.len(), 1);
        assert_eq!(vals[0].field_value("field"), Some(&FieldValue::F64(1.0)));

        let input = "m0 field=-1e-0";
        let vals = parse(input).unwrap();
        assert_eq!(vals.len(), 1);
        assert_eq!(vals[0].field_value("field"), Some(&FieldValue::F64(-1.0)));

        // NO "+" sign is accepted by IDPE
        let input = "m0 field=-1.234456e06 1615869152385000000";
        let vals = parse(input).unwrap();
        assert_eq!(vals.len(), 1);
        assert_float_field(&vals[0], "field", -1.234456e06);

        let input = "m0 field=1.234456e06 1615869152385000000";
        let vals = parse(input).unwrap();
        assert_eq!(vals.len(), 1);
        assert_float_field(&vals[0], "field", 1.234456e06);

        let input = "m0 field=-1.234456E06 1615869152385000000";
        let vals = parse(input).unwrap();
        assert_eq!(vals.len(), 1);
        assert_float_field(&vals[0], "field", -1.234456e06);

        let input = "m0 field=1.234456E06 1615869152385000000";
        let vals = parse(input).unwrap();
        assert_eq!(vals.len(), 1);
        assert_float_field(&vals[0], "field", 1.234456e06);

        /////////////////////
        // Negative tests

        // No digits after e
        let input = "m0 field=-1.234456e 1615869152385000000";
        assert_error!(parse(input), Error::CannotParseEntireLine { .. });

        let input = "m0 field=-1.234456e+ 1615869152385000000";
        assert_error!(parse(input), Error::CannotParseEntireLine { .. });

        let input = "m0 field=-1.234456E 1615869152385000000";
        assert_error!(parse(input), Error::CannotParseEntireLine { .. });

        let input = "m0 field=-1.234456E+ 1615869152385000000";
        assert_error!(parse(input), Error::CannotParseEntireLine { .. });

        let input = "m0 field=-1.234456E-";
        assert_error!(parse(input), Error::CannotParseEntireLine { .. });
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

        assert_error!(parse(input), Error::IntegerValueInvalid { .. });
    }

    #[test]
    fn parse_out_of_range_uinteger() {
        let input = "m0 field=99999999999999999999999999999999u 99";

        assert_error!(parse(input), Error::UIntegerValueInvalid { .. });
    }

    #[test]
    fn parse_out_of_range_float() {
        // this works since rust 1.55
        let input = format!("m0 field={val}.{val} 99", val = "9".repeat(200));
        let vals = parse(&input).unwrap();

        assert_eq!(vals.len(), 1);
        assert!(approximately_equal(
            vals[0].field_set[0].1.unwrap_f64(),
            1e200f64
        ));

        // even very long inputs now work
        let input = format!("m0 field={val}.{val} 99", val = "9".repeat(1_000));
        let vals = parse(&input).unwrap();

        assert_eq!(vals.len(), 1);
        assert!(vals[0].field_set[0].1.unwrap_f64().is_infinite());
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

        assert_error!(parse(input), Error::TimestampValueInvalid { .. });
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
        assert_fully_parsed!(measurement(r"wea\,ther"), r#"wea,ther"#);
    }

    #[test]
    fn measurement_allows_escaping_space() {
        assert_fully_parsed!(measurement(r"wea\ ther"), r#"wea ther"#);
    }

    #[test]
    fn measurement_allows_escaping_backslash() {
        assert_fully_parsed!(measurement(r"\\wea\\ther"), r"\wea\ther");
    }

    #[test]
    fn measurement_allows_backslash_with_unknown_escape() {
        assert_fully_parsed!(measurement(r"\wea\ther"), r"\wea\ther");
    }

    #[test]
    fn measurement_allows_literal_newline_as_unknown_escape() {
        assert_fully_parsed!(
            measurement(
                r"weat\
her"
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
        assert_error!(
            measurement(r"weather\"),
            nom::Err::Failure(Error::EndsWithBackslash)
        );
    }

    #[test]
    fn tag_key_allows_escaping_comma() {
        assert_fully_parsed!(tag_key(r"wea\,ther"), r#"wea,ther"#);
    }

    #[test]
    fn tag_key_allows_escaping_equal() {
        assert_fully_parsed!(tag_key(r"wea\=ther"), r#"wea=ther"#);
    }

    #[test]
    fn tag_key_allows_escaping_space() {
        assert_fully_parsed!(tag_key(r"wea\ ther"), r#"wea ther"#);
    }

    #[test]
    fn tag_key_allows_escaping_backslash() {
        assert_fully_parsed!(tag_key(r"\\wea\\ther"), r"\wea\ther");
    }

    #[test]
    fn tag_key_allows_backslash_with_unknown_escape() {
        assert_fully_parsed!(tag_key(r"\wea\ther"), r"\wea\ther");
    }

    #[test]
    fn tag_key_allows_literal_newline_as_unknown_escape() {
        assert_fully_parsed!(
            tag_key(
                r"weat\
her"
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
        assert_error!(
            tag_key(r"weather\"),
            nom::Err::Failure(Error::EndsWithBackslash)
        );
    }

    #[test]
    fn tag_value_allows_escaping_comma() {
        assert_fully_parsed!(tag_value(r"wea\,ther"), r#"wea,ther"#);
    }

    #[test]
    fn tag_value_allows_escaping_equal() {
        assert_fully_parsed!(tag_value(r"wea\=ther"), r#"wea=ther"#);
    }

    #[test]
    fn tag_value_allows_escaping_space() {
        assert_fully_parsed!(tag_value(r"wea\ ther"), r#"wea ther"#);
    }

    #[test]
    fn tag_value_allows_escaping_backslash() {
        assert_fully_parsed!(tag_value(r"\\wea\\ther"), r"\wea\ther");
    }

    #[test]
    fn tag_value_allows_backslash_with_unknown_escape() {
        assert_fully_parsed!(tag_value(r"\wea\ther"), r"\wea\ther");
    }

    #[test]
    fn tag_value_allows_literal_newline_as_unknown_escape() {
        assert_fully_parsed!(
            tag_value(
                r"weat\
her"
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
        assert_error!(
            tag_value(r"weather\"),
            nom::Err::Failure(Error::EndsWithBackslash)
        );
    }

    #[test]
    fn field_key_allows_escaping_comma() {
        assert_fully_parsed!(field_key(r"wea\,ther"), r#"wea,ther"#);
    }

    #[test]
    fn field_key_allows_escaping_equal() {
        assert_fully_parsed!(field_key(r"wea\=ther"), r#"wea=ther"#);
    }

    #[test]
    fn field_key_allows_escaping_space() {
        assert_fully_parsed!(field_key(r"wea\ ther"), r#"wea ther"#);
    }

    #[test]
    fn field_key_allows_escaping_backslash() {
        assert_fully_parsed!(field_key(r"\\wea\\ther"), r"\wea\ther");
    }

    #[test]
    fn field_key_allows_backslash_with_unknown_escape() {
        assert_fully_parsed!(field_key(r"\wea\ther"), r"\wea\ther");
    }

    #[test]
    fn field_key_allows_literal_newline_as_unknown_escape() {
        assert_fully_parsed!(
            field_key(
                r"weat\
her"
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
        assert_error!(
            field_key(r"weather\"),
            nom::Err::Failure(Error::EndsWithBackslash)
        );
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
        // Note that the first line has an error (23.1.22 is not a number),
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

    #[test]
    fn test_field_value_same_type() {
        // True cases
        assert!(FieldValue::I64(0).is_same_type(&FieldValue::I64(42)));
        assert!(FieldValue::U64(0).is_same_type(&FieldValue::U64(42)));
        assert!(FieldValue::F64(0.0).is_same_type(&FieldValue::F64(4.2)));
        // String & String
        assert!(
            FieldValue::String(EscapedStr::CopiedValue("bananas".to_string())).is_same_type(
                &FieldValue::String(EscapedStr::CopiedValue("platanos".to_string()))
            )
        );
        // str & str
        assert!(
            FieldValue::String(EscapedStr::SingleSlice("bananas"))
                .is_same_type(&FieldValue::String(EscapedStr::SingleSlice("platanos")))
        );
        // str & String
        assert!(
            FieldValue::String(EscapedStr::SingleSlice("bananas")).is_same_type(
                &FieldValue::String(EscapedStr::CopiedValue("platanos".to_string()))
            )
        );

        assert!(FieldValue::Boolean(true).is_same_type(&FieldValue::Boolean(false)));

        // Some false cases
        assert!(!FieldValue::I64(0).is_same_type(&FieldValue::U64(42)));
        assert!(!FieldValue::U64(0).is_same_type(&FieldValue::I64(42)));
        assert!(!FieldValue::F64(0.0).is_same_type(&FieldValue::U64(42)));
        assert!(
            !FieldValue::String(EscapedStr::CopiedValue("bananas".to_string()))
                .is_same_type(&FieldValue::U64(42))
        );
        assert!(!FieldValue::Boolean(true).is_same_type(&FieldValue::U64(42)));
    }

    #[test]
    fn test_large_tag_value() {
        let input = format!(
            r#"foo,tag1=normal,tag={} value=1i 123"#,
            "a".repeat(STRING_LENGTH_LIMIT_IN_BYTES + 1)
        );
        assert_error!(parse(&input), Error::FieldStringValueTooLarge);
    }

    #[test]
    fn test_large_tag_value_with_exact_maximum() {
        let tag_value = "a".repeat(STRING_LENGTH_LIMIT_IN_BYTES);
        let input = format!(r#"foo,tag1=normal,tag={tag_value} value=1i 123"#);
        let parsed = parse(&input);
        assert!(parsed.is_ok());
        let vals = parsed.unwrap();

        assert_eq!(vals[0].series.measurement, "foo");
        assert_eq!(vals[0].timestamp, Some(123));
        assert_eq!(*vals[0].tag_value("tag1").unwrap(), "normal");
        assert_eq!(*vals[0].tag_value("tag").unwrap(), tag_value);
        assert_eq!(vals[0].field_set[0].1.unwrap_i64(), 1);
    }

    #[test]
    fn test_large_tag_value_in_one_line_of_multiple_line_protocol() {
        let input = format!(
            "foo,tag1=very_long_value_is_okay,tag2=bar value=1i 123\n\
                     foo,tag1=very_long_value_is_okay,tag2=bar value=2i 123\n\
                     foo,tag1=very_long_value_is_okay,tag2=bar value=3i 123\n\
                     foo,tag1=very_long_value_is_okay,tag2=bar value=4i 123\n\
                     foo,tag1=very_long_value_is_okay,tag2=bar value=5i 123\n\
                     foo,tag1=very_long_value_is_okay,tag2=bar value=6i 123\n\
                     foo,tag1={}, tag2=bar value=7i 123\n\
                     foo,tag1=very_long_value_is_okay,tag2=bar value=8i 123\n\
                     foo,tag1=very_long_value_is_okay,tag2=bar value=9i 123\n\
                     foo,tag1=very_long_value_is_okay,tag2=bar value=10i 123\n\
                     foo,tag1=very_long_value_is_okay,tag2=bar value=11i 123\n\
                     foo,tag1=very_long_value_is_okay,tag2=bar value=12i 123\n\
                     foo,tag1=very_long_value_is_okay,tag2=bar value=13i 123\n\
                     foo,tag1=very_long_value_is_okay,tag2=bar value=14i 123\n\
                     foo,tag1=very_long_value_is_okay,tag2=bar value=15i 123\n\
                     foo,tag1=very_long_value_is_okay,tag2=bar value=16i 123\n\
                     foo,tag1=very_long_value_is_okay,tag2=bar value=17i 123\n\
                     foo,tag1=very_long_value_is_okay,tag2=bar value=18i 123\n\
                     foo,tag1=very_long_value_is_okay,tag2=bar value=19i 123\n\
                     foo,tag1=very_long_value_is_okay,tag2=bar value=20i",
            "a".repeat(STRING_LENGTH_LIMIT_IN_BYTES + 2)
        );

        assert_error!(parse(&input), Error::FieldStringValueTooLarge);
    }

    #[test]
    fn test_large_field_value() {
        let input = format!(
            "foo,tag1=bar value=\"{}\" 123",
            "a".repeat(STRING_LENGTH_LIMIT_IN_BYTES + 1)
        );

        assert_error!(parse(&input), Error::FieldStringValueTooLarge);
    }

    #[test]
    fn test_large_field_value_with_exact_maximum() {
        let value = "a".repeat(STRING_LENGTH_LIMIT_IN_BYTES);
        let input = format!("foo,tag1=bar value=\"{value}\" 123");

        let parsed = parse(&input);

        assert!(parsed.is_ok());
        let vals = parsed.unwrap();

        assert_eq!(vals[0].series.measurement, "foo");
        assert_eq!(vals[0].timestamp, Some(123));
        assert_eq!(*vals[0].tag_value("tag1").unwrap(), "bar");
        assert_eq!(vals[0].field_set[0].1.unwrap_string(), value);
    }

    #[test]
    fn test_large_field_value_in_one_line_of_multiple_line_protocol() {
        let input = format!(
            "foo,tag1=very_long_value_is_okay,tag2=bar value=2i 123\n\
        foo,tag1=very_long_value_is_okay,tag2=bar value=2i 123\n\
        foo,tag1=bar value=\"{}\" 123",
            "a".repeat(STRING_LENGTH_LIMIT_IN_BYTES + 1)
        );

        assert_error!(parse(&input), Error::FieldStringValueTooLarge);
    }

    #[test]
    fn test_large_measurement_name() {
        let measurement = "a".repeat(STRING_LENGTH_LIMIT_IN_BYTES + 1);
        let input = format!("{measurement},tag1=bar value=1i 123");

        assert_error!(parse(&input), Error::FieldStringValueTooLarge);
    }

    #[test]
    fn test_large_measurement_name_exact_maximum_length() {
        let measurement = "a".repeat(STRING_LENGTH_LIMIT_IN_BYTES);
        let input = format!("{measurement},tag1=bar value=1i 123");

        let parsed = parse(&input);
        assert!(parsed.is_ok());
        let vals = parsed.unwrap();

        assert_eq!(vals[0].series.measurement, measurement);
    }

    #[test]
    fn test_large_tag_name() {
        let tag_name = "a".repeat(STRING_LENGTH_LIMIT_IN_BYTES + 1);
        let input = format!("foo,{tag_name}=bar value=1i 123");

        assert_error!(parse(&input), Error::FieldStringValueTooLarge);
    }

    #[test]
    fn test_large_tag_name_exact_maximum_length() {
        let tag_name = "a".repeat(STRING_LENGTH_LIMIT_IN_BYTES);
        let input = format!("foo,{tag_name}=bar value=1i 123");

        let parsed = parse(&input);
        assert!(parsed.is_ok());
        let vals = parsed.unwrap();

        assert_eq!(vals[0].series.measurement, "foo");
        assert_eq!(*vals[0].tag_value(&tag_name).unwrap(), "bar");
    }

    #[test]
    fn test_large_value_name() {
        let value_name = "a".repeat(STRING_LENGTH_LIMIT_IN_BYTES + 1);
        let input = format!("foo,tag1=bar {value_name}=1i 123");

        assert_error!(parse(&input), Error::FieldStringValueTooLarge);
    }

    #[test]
    fn test_large_value_name_exact_maximum_length() {
        let value_name = "a".repeat(STRING_LENGTH_LIMIT_IN_BYTES);
        let input = format!("foo,tag1=bar {value_name}=1i 123");

        let parsed = parse(&input);
        assert!(parsed.is_ok());
        let vals = parsed.unwrap();

        assert_eq!(vals[0].field_value(&value_name).unwrap().unwrap_i64(), 1);
    }

    #[test]
    #[ignore = "https://github.com/influxdata/influxdb_iox/issues/13372"]
    fn test_many_canonical_escape() {
        let input = r#"bananas,tag1=\\\\\\0 test=42i"#;
        let want_tag_value = r#"\\\0"#;

        // Parse the input and assert the tag value is correctly escaped.
        let parsed = parse(input).unwrap();
        assert_eq!(
            parsed[0].tag_value("tag1").unwrap().as_str(),
            want_tag_value
        );

        // Convert the parsed form into the canonical representation.
        let input2 = parsed[0].to_string();

        // Which should match the first parsed output.
        let parsed = parse(&input2).unwrap();
        assert_eq!(
            parsed[0].tag_value("tag1").unwrap().as_str(),
            want_tag_value
        );
    }

    /// Assert that the field named `field_name` has a float value
    /// within 0.0001% of `expected_value`, panic'ing if not
    fn assert_float_field(parsed_line: &ParsedLine<'_>, field_name: &str, expected_value: f64) {
        let field_value = parsed_line
            .field_value(field_name)
            .expect("did not contain field name");

        let actual_value = if let FieldValue::F64(v) = field_value {
            *v
        } else {
            panic!("field {field_name} had value {field_value:?}, expected F64");
        };

        assert!(approximately_equal(expected_value, actual_value));
    }
}
