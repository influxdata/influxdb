//! # Parse an InfluxQL [identifier]
//!
//! Identifiers are parsed using the following rules:
//!
//! * double quoted identifiers can contain any unicode character other than a new line
//! * double quoted identifiers can contain escaped characters, namely `\"`, `\n`, `\t`, `\\` and `\'`
//! * double quoted identifiers can contain [InfluxQL keywords][keywords]
//! * unquoted identifiers must start with an upper or lowercase ASCII character or `_`
//! * unquoted identifiers may contain only ASCII letters, decimal digits, and `_`
//!
//! [identifier]: https://docs.influxdata.com/influxdb/v1.8/query_language/spec/#identifiers
//! [keywords]: https://docs.influxdata.com/influxdb/v1.8/query_language/spec/#keywords

#![allow(dead_code)]

use crate::keywords::sql_keyword;
use nom::branch::alt;
use nom::bytes::complete::tag;
use nom::bytes::streaming::is_not;
use nom::character::complete::{alpha1, alphanumeric1};
use nom::character::streaming::char;
use nom::combinator::{map, not, recognize, value, verify};
use nom::multi::{fold_many0, many0_count};
use nom::sequence::{delimited, pair, preceded};
use nom::IResult;
use std::fmt;
use std::fmt::{Display, Formatter, Write};

// Taken liberally from https://github.com/Geal/nom/blob/main/examples/string.rs and
// amended for InfluxQL.

/// Parse an escaped character: `\n`, `\t`, `\"`, `\\` and `\'`.
fn escaped_char(input: &str) -> IResult<&str, char> {
    preceded(
        char('\\'),
        alt((
            value('\n', char('n')),
            value('\t', char('t')),
            value('\\', char('\\')),
            value('"', char('"')),
            value('\'', char('\'')),
        )),
    )(input)
}

/// Parse a non-empty block of text that doesn't include `\` or `"`.
fn literal(input: &str) -> IResult<&str, &str> {
    // Skip newlines, " and \.
    let not_quote_slash_newline = is_not("\"\\\n");

    verify(not_quote_slash_newline, |s: &str| !s.is_empty())(input)
}

/// A string fragment contains a fragment of a string being parsed: either
/// a non-empty Literal (a series of non-escaped characters) or a single.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StringFragment<'a> {
    Literal(&'a str),
    EscapedChar(char),
}

/// Combine [`literal`], and [`escaped_char`] into a [`StringFragment`].
fn fragment(i: &str) -> IResult<&str, StringFragment<'_>> {
    alt((
        map(literal, StringFragment::Literal),
        map(escaped_char, StringFragment::EscapedChar),
    ))(i)
}

/// Parse an identifier string.
pub fn string(i: &str) -> IResult<&str, String> {
    // fold_many0 is the equivalent of iterator::fold. It runs a parser in a loop,
    // and for each output value, calls a folding function on each output value.
    let build_string = fold_many0(fragment, String::new, |mut string, fragment| {
        match fragment {
            StringFragment::Literal(s) => string.push_str(s),
            StringFragment::EscapedChar(c) => string.push(c),
        }
        string
    });

    delimited(char('"'), build_string, char('"'))(i)
}

/// Parse an unquoted InfluxQL identifier.
fn unquoted_identifier(i: &str) -> IResult<&str, &str> {
    preceded(
        not(sql_keyword),
        recognize(pair(
            alt((alpha1, tag("_"))),
            many0_count(alt((alphanumeric1, tag("_")))),
        )),
    )(i)
}

/// `Identifier` is a type that represents either a quoted ([`Identifier::Quoted`]) or unquoted ([`Identifier::Unquoted`])
/// InfluxQL identifier.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum Identifier<'a> {
    /// Contains an unquoted identifier
    Unquoted(&'a str),

    /// Contains an unescaped quoted identifier
    Quoted(String),
}

impl Display for Identifier<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unquoted(s) => write!(f, "{}", s)?,
            Self::Quoted(s) => {
                f.write_char('"')?;
                for c in s.chars() {
                    // escape characters per https://github.com/influxdata/influxql/blob/df51a45762be9c1b578f01718fa92d286a843fe9/scanner.go#L576-L583
                    match c {
                        '\n' => f.write_str(r#"\n"#)?,
                        '\\' => f.write_str(r#"\\"#)?,
                        '\'' => f.write_str(r#"\'"#)?,
                        '"' => f.write_str(r#"\""#)?,
                        _ => f.write_char(c)?,
                    }
                }
                f.write_char('"')?;
            }
        };

        Ok(())
    }
}

/// Parses an InfluxQL [Identifier].
pub fn identifier(i: &str) -> IResult<&str, Identifier<'_>> {
    // See: https://github.com/influxdata/influxql/blob/df51a45762be9c1b578f01718fa92d286a843fe9/scanner.go#L358-L362
    alt((
        map(unquoted_identifier, Identifier::Unquoted),
        map(string, Identifier::Quoted),
    ))(i)
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_string() {
        // ascii
        let (_, got) = string(r#""quick draw""#).unwrap();
        assert_eq!(got, "quick draw");

        // unicode
        let (_, got) = string("\"quick draw\u{1f47d}\"").unwrap();
        assert_eq!(
            got,
            "quick draw\u{1f47d}" // 👽
        );

        // escaped characters
        let (_, got) = string(r#""\n\t\'\"""#).unwrap();
        assert_eq!(got, "\n\t'\"");

        // literal tab
        let (_, got) = string("\"quick\tdraw\"").unwrap();
        assert_eq!(got, "quick\tdraw");

        // literal carriage return
        let (_, got) = string("\"quick\rdraw\"").unwrap();
        assert_eq!(got, "quick\rdraw");

        // ┌─────────────────────────────┐
        // │       Fallible tests        │
        // └─────────────────────────────┘

        // Not terminated
        let res = string(r#""quick draw"#);
        assert!(res.is_err());

        // Literal newline
        let res = string("\"quick\ndraw\"");
        assert!(res.is_err());

        // Invalid escape
        let res = string(r#""quick\idraw""#);
        assert!(res.is_err());
    }

    #[test]
    fn test_unquoted_identifier() {
        // all ascii
        let (_, got) = unquoted_identifier("cpu").unwrap();
        assert_eq!(got, "cpu");

        // all valid chars
        let (_, got) = unquoted_identifier("cpu_0").unwrap();
        assert_eq!(got, "cpu_0");

        // begin with underscore
        let (_, got) = unquoted_identifier("_cpu_0").unwrap();
        assert_eq!(got, "_cpu_0");

        // ┌─────────────────────────────┐
        // │       Fallible tests        │
        // └─────────────────────────────┘

        // start with number
        let res = unquoted_identifier("0cpu");
        assert!(res.is_err());

        // is a keyword
        let res = unquoted_identifier("as");
        assert!(res.is_err());
    }

    #[test]
    fn test_identifier() {
        // quoted
        let (_, got) = identifier("\"quick draw\"").unwrap();
        assert!(matches!(got, Identifier::Quoted(s) if s == "quick draw"));

        // unquoted
        let (_, got) = identifier("quick_draw").unwrap();
        assert!(matches!(got, Identifier::Unquoted(s) if s == "quick_draw"));
    }

    #[test]
    fn test_identifier_display() {
        // test quoted identifier properly escapes specific characters
        let got = format!(
            "{}",
            Identifier::Quoted("quick\n\t\\\"'draw \u{1f47d}".to_string())
        );
        assert_eq!(got, r#""quick\n	\\\"\'draw 👽""#);

        // test unquoted identifier
        let got = format!("{}", Identifier::Unquoted("quick_draw"));
        assert_eq!(got, "quick_draw");
    }
}
