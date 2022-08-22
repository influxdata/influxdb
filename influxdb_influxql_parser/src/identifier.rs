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

use nom::branch::alt;
use nom::bytes::streaming::is_not;
use nom::character::streaming::char;
use nom::combinator::{map, value, verify};
use nom::multi::fold_many0;
use nom::sequence::{delimited, preceded};
use nom::IResult;

// Taken liberally from https://github.com/Geal/nom/blob/main/examples/string.rs and
// adjusted for InfluxQL

/// Parse an escaped character: `\n`, `\t`, `\"`, `\\` and `\'`.
fn parse_escaped_char(input: &str) -> IResult<&str, char> {
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

/// Parse a non-empty block of text that doesn't include \ or "
fn parse_literal(input: &str) -> IResult<&str, &str> {
    // Skip newlines, " and \.
    let not_quote_slash_newline = is_not("\"\\\n");

    verify(not_quote_slash_newline, |s: &str| !s.is_empty())(input)
}

/// A string fragment contains a fragment of a string being parsed: either
/// a non-empty Literal (a series of non-escaped characters), a single
/// parsed escaped character, or a block of escaped whitespace.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StringFragment<'a> {
    Literal(&'a str),
    EscapedChar(char),
}

/// Combine parse_literal, parse_escaped_whitespace, and parse_escaped_char
/// into a StringFragment.
fn parse_fragment(input: &str) -> IResult<&str, StringFragment<'_>> {
    alt((
        map(parse_literal, StringFragment::Literal),
        map(parse_escaped_char, StringFragment::EscapedChar),
    ))(input)
}

/// Parse a string. Use a loop of parse_fragment and push all of the fragments
/// into an output string.
pub fn parse_string(input: &str) -> IResult<&str, String> {
    // fold_many0 is the equivalent of iterator::fold. It runs a parser in a loop,
    // and for each output value, calls a folding function on each output value.
    let build_string = fold_many0(parse_fragment, String::new, |mut string, fragment| {
        match fragment {
            StringFragment::Literal(s) => string.push_str(s),
            StringFragment::EscapedChar(c) => string.push(c),
        }
        string
    });

    delimited(char('"'), build_string, char('"'))(input)
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_parse_string() {
        // ascii
        let (_, got) = parse_string(r#""quick draw""#).unwrap();
        assert_eq!(got, "quick draw");

        // unicode
        let (_, got) = parse_string("\"quick draw\u{1f47d}\"").unwrap();
        assert_eq!(
            got,
            "quick draw\u{1f47d}" // 👽
        );

        // escaped characters
        let (_, got) = parse_string(r#""\n\t\'\"""#).unwrap();
        assert_eq!(got, "\n\t'\"");

        // literal tab
        let (_, got) = parse_string("\"quick\tdraw\"").unwrap();
        assert_eq!(got, "quick\tdraw");

        // literal carriage return
        let (_, got) = parse_string("\"quick\rdraw\"").unwrap();
        assert_eq!(got, "quick\rdraw");

        // ┌─────────────────────────────┐
        // │       Fallible tests        │
        // └─────────────────────────────┘

        // Not terminated
        let res = parse_string(r#""quick draw"#);
        assert!(res.is_err());

        // Literal newline
        let res = parse_string("\"quick\ndraw\"");
        assert!(res.is_err());

        // Invalid escape
        let res = parse_string(r#""quick\idraw""#);
        assert!(res.is_err());
    }
}
