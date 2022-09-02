#![allow(dead_code)]

//! Parse delimited string inputs.
//!

// Taken liberally from https://github.com/Geal/nom/blob/main/examples/string.rs and
// amended for InfluxQL.

use nom::branch::alt;
use nom::bytes::complete::{is_not, tag};
use nom::character::complete::char;
use nom::combinator::{map, value, verify};
use nom::error::Error;
use nom::multi::fold_many0;
use nom::sequence::{delimited, preceded};
use nom::{IResult, Parser};
use std::fmt::{Display, Formatter, Write};

/// Writes `s` to `f`, mapping any characters from => to their escaped equivalents.
#[macro_export]
macro_rules! write_escaped {
    ($f: expr, $s: expr $(, $from:expr => $to:expr)+) => {
        for c in $s.chars() {
            match c {
                $(
                $from => $f.write_str($to)?,
                )+
                _ => $f.write_char(c)?,
            }
        }
    };
}

/// A string fragment contains a fragment of a string being parsed: either
/// a non-empty Literal (a series of non-escaped characters) or a single.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StringFragment<'a> {
    Literal(&'a str),
    EscapedChar(char),
}

/// Parse a single-quoted literal string.
pub fn single_quoted_string(i: &str) -> IResult<&str, String> {
    let escaped = preceded(
        char('\\'),
        alt((char('\\'), char('\''), value('\n', char('n')))),
    );

    string(
        '\'',
        verify(is_not("'\\\n"), |s: &str| !s.is_empty()),
        escaped,
    )(i)
}

/// Parse a double-quoted identifier string.
pub fn double_quoted_string(i: &str) -> IResult<&str, String> {
    let escaped = preceded(
        char('\\'),
        alt((char('\\'), char('"'), value('\n', char('n')))),
    );

    string(
        '"',
        verify(is_not("\"\\\n"), |s: &str| !s.is_empty()),
        escaped,
    )(i)
}

fn string<'a, T, U>(
    delimiter: char,
    literal: T,
    escaped: U,
) -> impl FnMut(&'a str) -> IResult<&'a str, String>
where
    T: Parser<&'a str, &'a str, Error<&'a str>>,
    U: Parser<&'a str, char, Error<&'a str>>,
{
    let fragment = alt((
        map(literal, StringFragment::Literal),
        map(escaped, StringFragment::EscapedChar),
    ));

    let build_string = fold_many0(fragment, String::new, |mut string, fragment| {
        match fragment {
            StringFragment::Literal(s) => string.push_str(s),
            StringFragment::EscapedChar(ch) => string.push(ch),
        }
        string
    });

    delimited(char(delimiter), build_string, char(delimiter))
}

/// Parse regular expression literal characters.
///
/// Consumes i until reaching and escaped delimiter ("\/"), newline or eof.
fn regex_literal(i: &str) -> IResult<&str, &str> {
    let mut remaining = &i[..i.len()];
    let mut consumed = &i[..0];

    loop {
        // match everything except `\`, `/` or `\n`
        let (_, match_i) = is_not("\\/\n")(remaining)?;
        consumed = &i[..(consumed.len() + match_i.len())];
        remaining = &i[consumed.len()..];

        // Try and consume '\' followed by a '/'
        if let Ok((remaining_i, _)) = char::<_, Error<&str>>('\\')(remaining) {
            if char::<_, Error<&str>>('/')(remaining_i).is_ok() {
                // We're escaping a '/' (a regex delimiter), so finish and let
                // the outer parser match and unescape
                return Ok((remaining, consumed));
            }
            // Skip the '/' and continue consuming
            consumed = &i[..consumed.len() + 1];
            remaining = &i[consumed.len()..];
        } else {
            return Ok((remaining, consumed));
        }
    }
}

/// An unescaped regular expression.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Regex(String);

impl Display for Regex {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_char('/')?;
        write_escaped!(f, self.0, '/' => "\\/");
        f.write_char('/')?;
        Ok(())
    }
}

impl From<String> for Regex {
    fn from(v: String) -> Self {
        Self(v)
    }
}

impl From<&str> for Regex {
    fn from(v: &str) -> Self {
        Self(v.into())
    }
}

/// Parse a regular expression, delimited by `/`.
pub fn regex(i: &str) -> IResult<&str, Regex> {
    map(string('/', regex_literal, map(tag("\\/"), |_| '/')), Regex)(i)
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_double_quoted_string() {
        // ascii
        let (_, got) = double_quoted_string(r#""quick draw""#).unwrap();
        assert_eq!(got, "quick draw");

        // unicode
        let (_, got) = double_quoted_string("\"quick draw\u{1f47d}\"").unwrap();
        assert_eq!(
            got,
            "quick draw\u{1f47d}" // 👽
        );

        // escaped characters
        let (_, got) = double_quoted_string(r#""\n\\\"""#).unwrap();
        assert_eq!(got, "\n\\\"");

        // literal tab
        let (_, got) = double_quoted_string("\"quick\tdraw\"").unwrap();
        assert_eq!(got, "quick\tdraw");

        // literal carriage return
        let (_, got) = double_quoted_string("\"quick\rdraw\"").unwrap();
        assert_eq!(got, "quick\rdraw");

        // ┌─────────────────────────────┐
        // │       Fallible tests        │
        // └─────────────────────────────┘

        // Not terminated
        double_quoted_string(r#""quick draw"#).unwrap_err();

        // Literal newline
        double_quoted_string("\"quick\ndraw\"").unwrap_err();

        // Invalid escape
        double_quoted_string(r#""quick\idraw""#).unwrap_err();
    }

    #[test]
    fn test_single_quoted_string() {
        // ascii
        let (_, got) = single_quoted_string(r#"'quick draw'"#).unwrap();
        assert_eq!(got, "quick draw");

        // unicode
        let (_, got) = single_quoted_string("'quick draw\u{1f47d}'").unwrap();
        assert_eq!(
            got,
            "quick draw\u{1f47d}" // 👽
        );

        // escaped characters
        let (_, got) = single_quoted_string(r#"'\n\''"#).unwrap();
        assert_eq!(got, "\n'");

        // literal tab
        let (_, got) = single_quoted_string("'quick\tdraw'").unwrap();
        assert_eq!(got, "quick\tdraw");

        // literal carriage return
        let (_, got) = single_quoted_string("'quick\rdraw'").unwrap();
        assert_eq!(got, "quick\rdraw");

        // ┌─────────────────────────────┐
        // │       Fallible tests        │
        // └─────────────────────────────┘

        // Not terminated
        single_quoted_string(r#"'quick draw"#).unwrap_err();

        // Invalid escape
        single_quoted_string(r#"'quick\idraw'"#).unwrap_err();
    }

    #[test]
    fn test_regex() {
        let (_, got) = regex("/hello/").unwrap();
        assert_eq!(got, "hello".into());

        // handle escaped delimiters "\/"
        let (_, got) = regex(r#"/this\/is\/a\/path/"#).unwrap();
        assert_eq!(got, "this/is/a/path".into());

        // ignores any other possible escape sequence
        let (_, got) = regex(r#"/hello\n/"#).unwrap();
        assert_eq!(got, "hello\\n".into());

        // Empty regex
        let (_, got) = regex("//").unwrap();
        assert_eq!(got, "".into());

        // Fallible cases

        // Missing trailing delimiter
        regex(r#"/hello"#).unwrap_err();

        // Embedded newline
        regex("/hello\nworld").unwrap_err();

        // Single backslash fails, which matches Go implementation
        // See: https://go.dev/play/p/_8J1v5-382G
        regex(r#"/\/"#).unwrap_err();
    }
}
