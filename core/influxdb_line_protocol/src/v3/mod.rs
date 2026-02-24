use crate::{
    Error, EscapedStr, FIELD_KEY_DELIMITERS, FieldSetMissingSnafu, FieldValue, IResult, Series,
    escape_and_write_value, escaped_value, field_value, is_whitespace_boundary_char,
    parameterized_separated_list1, series, split_lines, timestamp, trim_leading, whitespace,
};
use crate::{Result, field_key};
use log::debug;
use nom::Parser;
use nom::bytes::complete::tag;
use nom::combinator::{map, opt};
use nom::error::{ErrorKind, ParseError};
use nom::sequence::{pair, preceded, separated_pair, terminated};
use smallvec::SmallVec;
use snafu::whatever;
use std::fmt;
use std::fmt::{Display, Write};

const FIELD_FAMILY_DELIMITER: &str = "::";

#[derive(Debug, PartialOrd, PartialEq)]
pub enum FieldName<'a> {
    /// An unqualified field only contains a field name.
    Unqualified(EscapedStr<'a>),
    /// A qualified field includes the field family and field name.
    Qualified(EscapedStr<'a>, EscapedStr<'a>),
}

impl FieldName<'_> {
    pub fn field_key(&self) -> &str {
        match self {
            FieldName::Unqualified(k) | FieldName::Qualified(_, k) => k.as_ref(),
        }
    }
}

impl Display for FieldName<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FieldName::Unqualified(name) => {
                escape_and_write_value(f, name.as_str(), FIELD_KEY_DELIMITERS)
            }
            FieldName::Qualified(qualifier, name) => {
                escape_and_write_value(f, qualifier.as_str(), FIELD_KEY_DELIMITERS)?;
                f.write_str(FIELD_FAMILY_DELIMITER)?;
                escape_and_write_value(f, name.as_str(), FIELD_KEY_DELIMITERS)
            }
        }
    }
}

pub type FieldSet<'a> = SmallVec<[(FieldName<'a>, FieldValue<'a>); 4]>;

/// Represents a single parsed line of line protocol data. See the [crate-level documentation](self)
/// for more information and examples.
#[derive(Debug)]
pub struct ParsedLine<'a> {
    pub series: Series<'a>,
    pub field_set: FieldSet<'a>,
    pub timestamp: Option<i64>,
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
        let f = self
            .field_set
            .iter()
            .find(|(f, _)| f.field_key() == field_key);
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
        self.series.fmt(f)?;

        if !self.field_set.is_empty() {
            f.write_char(' ')?;

            let mut first = true;
            for (field_name, field_value) in &self.field_set {
                if !first {
                    f.write_char(',')?;
                }
                first = false;
                field_name.fmt(f)?;
                f.write_char('=')?;
                field_value.fmt(f)?;
            }
        }

        if let Some(timestamp) = self.timestamp {
            f.write_char(' ')?;
            timestamp.fmt(f)?
        }
        Ok(())
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

fn field_set(i: &str) -> IResult<&str, FieldSet<'_>> {
    let sep = tag(",");

    match parameterized_separated_list1(sep, field_and_value, SmallVec::new, |v, i| v.push(i))(i) {
        Err(nom::Err::Error(_)) => FieldSetMissingSnafu.fail().map_err(nom::Err::Error),
        other => other,
    }
}

/// Parser for a single field and value
///
/// ```text
/// field_and_value := qualified_field "=" value
///
/// qualified_field := field_family? identifier
///
/// field_family := identifier "::"
/// ```
///
/// # NOTE
///
/// When parsing a `field_family`, the `identifier` scanning is not greedy,
/// and finds the _first_ [FIELD_FAMILY_DELIMITER].
fn field_and_value(i: &str) -> IResult<&str, (FieldName<'_>, FieldValue<'_>)> {
    separated_pair(field_name, tag("="), field_value).parse(i)
}

/// Parses a field name from a string.
///
/// ```text
/// qualified_field := field_family? identifier
///
/// field_family := identifier "::"
/// ```
pub fn parse_field_name(i: &str) -> Result<FieldName<'_>, snafu::Whatever> {
    match field_name(i) {
        Ok(("", val)) => Ok(val),
        Ok((remaining, _)) => {
            whatever!("Unexpected remaining input: '{remaining}'")
        }
        Err(nom::Err::Error(_) | nom::Err::Failure(_)) => whatever!("Unable to parse field name"),
        Err(nom::Err::Incomplete(_)) => whatever!("Not enough data"),
    }
}

fn field_name(i: &str) -> IResult<&str, FieldName<'_>> {
    let (i, (ff, field_key)) = pair(
        opt(terminated(field_family, tag(FIELD_FAMILY_DELIMITER))),
        field_key,
    )
    .parse(i)?;

    Ok((
        i,
        match (ff, field_key) {
            (Some(ff), field_key) => FieldName::Qualified(ff, field_key),
            (None, field_key) => FieldName::Unqualified(field_key),
        },
    ))
}

fn field_family(i: &str) -> IResult<&str, EscapedStr<'_>> {
    escaped_value(field_family_normal_char)(i)
}

// Helper function for field_family that parses until ::, =, whitespace, or backslash
fn field_family_normal_char(i: &str) -> IResult<&str, &str> {
    let mut iter = i.char_indices().peekable();
    while let Some((pos, c)) = iter.next() {
        if !is_whitespace_boundary_char(c) && c != '=' && c != '\\' && c != ':' {
            continue;
        }

        if c == ':' {
            // Peek to see if the next char is also a colon, and if not,
            // keep consuming.
            if let Some((_, next_ch)) = iter.peek()
                && *next_ch != ':'
            {
                continue;
            }
        }

        // If we don't make progress, exit the loop.
        if pos == 0 {
            break;
        }

        let (a, b) = i.split_at(pos);
        return Ok((b, a));
    }
    Err(nom::Err::Error(Error::from_error_kind(
        i,
        ErrorKind::TakeWhile1,
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    use smallvec::smallvec;

    fn parse(s: &str) -> Result<Vec<ParsedLine<'_>>, super::Error> {
        parse_lines(s).collect()
    }

    #[test]
    fn parse_line_field_family() {
        let input = r#"foo field_fam::value1=1i 123"#;
        let vals = parse(input).unwrap();

        assert_eq!(vals[0].series.measurement, "foo");
        assert_eq!(vals[0].timestamp, Some(123));
        assert_eq!(
            vals[0].field_set[0].0,
            FieldName::Qualified("field_fam".into(), "value1".into())
        );
        assert_eq!(vals[0].field_set[0].1.unwrap_i64(), 1);
    }

    #[test]
    fn parse_field_and_value() {
        let (i, (key, val)) = field_and_value("foo=5").unwrap();
        assert!(i.is_empty());
        assert_eq!(key, FieldName::Unqualified("foo".into()));
        assert_eq!(val, FieldValue::F64(5.0));

        let (i, (key, val)) = field_and_value(r#"foo\ bar=5"#).unwrap();
        assert!(i.is_empty());
        assert_eq!(key, FieldName::Unqualified("foo bar".into()));
        assert_eq!(val, FieldValue::F64(5.0));

        let (i, (key, val)) = field_and_value("field_fam::foo=5i").unwrap();
        assert!(i.is_empty());
        assert_eq!(key, FieldName::Qualified("field_fam".into(), "foo".into()));
        assert_eq!(val, FieldValue::I64(5));

        let (i, (key, val)) = field_and_value(r#"field:fam::foo="str""#).unwrap();
        assert!(i.is_empty());
        assert_eq!(key, FieldName::Qualified("field:fam".into(), "foo".into()));
        assert_eq!(val, FieldValue::String("str".into()));

        let (i, (key, val)) = field_and_value(r#"field:fam:foo="str""#).unwrap();
        assert!(i.is_empty());
        assert_eq!(key, FieldName::Unqualified("field:fam:foo".into()));
        assert_eq!(val, FieldValue::String("str".into()));

        let (i, (key, val)) = field_and_value(r#"field:fam::foo::bar="str""#).unwrap();
        assert!(i.is_empty());
        assert_eq!(
            key,
            FieldName::Qualified("field:fam".into(), "foo::bar".into())
        );
        assert_eq!(val, FieldValue::String("str".into()));

        // This test verifies the first FIELD_FAMILY_DELIMITER determines the field family.
        let (i, (key, val)) = field_and_value(r#"field:fam::foo::bar::boo="str""#).unwrap();
        assert!(i.is_empty());
        assert_eq!(
            key,
            FieldName::Qualified("field:fam".into(), "foo::bar::boo".into())
        );
        assert_eq!(val, FieldValue::String("str".into()));

        let (i, (key, val)) = field_and_value(r#"col\=fam::foo\ bar="str""#).unwrap();
        assert!(i.is_empty());
        assert_eq!(
            key,
            FieldName::Qualified("col=fam".into(), "foo bar".into())
        );
        assert_eq!(val, FieldValue::String("str".into()));
    }

    #[test]
    fn field_name_display() {
        // Test unqualified field name
        let unqualified = FieldName::Unqualified(EscapedStr::SingleSlice("temperature"));
        assert_eq!(unqualified.to_string(), "temperature");

        // Test unqualified field name with special characters that need escaping
        let unqualified_escaped = FieldName::Unqualified(EscapedStr::SingleSlice("temp erature"));
        assert_eq!(unqualified_escaped.to_string(), "temp\\ erature");

        // Test qualified field name
        let qualified = FieldName::Qualified(
            EscapedStr::SingleSlice("sensor"),
            EscapedStr::SingleSlice("temperature"),
        );
        assert_eq!(qualified.to_string(), "sensor::temperature");

        // Test qualified field name with special characters
        let qualified_escaped = FieldName::Qualified(
            EscapedStr::SingleSlice("my sensor"),
            EscapedStr::SingleSlice("temp,erature"),
        );
        assert_eq!(qualified_escaped.to_string(), "my\\ sensor::temp\\,erature");

        // Test field_key() method
        assert_eq!(unqualified.field_key(), "temperature");
        assert_eq!(qualified.field_key(), "temperature");
    }

    #[test]
    fn parsed_line_display_two_fields_timestamp() {
        let series = Series {
            raw_input: "foo",
            measurement: "m".into(),
            tag_set: Some(smallvec![
                ("tag1".into(), "val1".into()),
                ("tag2".into(), "val2".into())
            ]),
        };
        let field_set = smallvec![
            (
                FieldName::Unqualified("field1".into()),
                FieldValue::F64(42.1)
            ),
            (
                FieldName::Qualified("field_fam".into(), "field2".into()),
                FieldValue::Boolean(false)
            ),
        ];

        let parsed_line = ParsedLine {
            series,
            field_set,
            timestamp: Some(33),
        };

        assert_eq!(
            parsed_line.to_string(),
            "m,tag1=val1,tag2=val2 field1=42.1,field_fam::field2=false 33"
        );
    }

    #[test]
    fn test_parse_field_name() {
        let res = parse_field_name("foo");
        assert!(matches!(res, Ok(FieldName::Unqualified(name)) if name == "foo"));

        let res = parse_field_name("field_fam::field_name");
        assert!(
            matches!(res, Ok(FieldName::Qualified(ff, name)) if ff == "field_fam" && name == "field_name")
        );

        let res = parse_field_name("col\\ name");
        assert!(matches!(res, Ok(FieldName::Unqualified(name)) if name == "col name"));

        let res = parse_field_name("col\\ fam::col\\ name");
        assert!(
            matches!(res, Ok(FieldName::Qualified(ff, name)) if ff == "col fam" && name == "col name")
        );

        let res = parse_field_name("col:name");
        assert!(matches!(res, Ok(FieldName::Unqualified(name)) if name == "col:name"));

        let res = parse_field_name("col name").unwrap_err();
        assert_eq!(res.to_string(), "Unexpected remaining input: ' name'");

        let res = parse_field_name(" col name").unwrap_err();
        assert_eq!(res.to_string(), "Unable to parse field name");
    }
}
