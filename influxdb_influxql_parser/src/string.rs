//! Parse delimited string inputs.
//!

// Taken liberally from https://github.com/Geal/nom/blob/main/examples/string.rs and
// amended for InfluxQL.

use crate::impl_tuple_clause;
use crate::internal::{expect, ParseError, ParseResult};
use nom::branch::alt;
use nom::bytes::complete::{is_not, tag, take_till};
use nom::character::complete::{anychar, char};
use nom::combinator::{map, value, verify};
use nom::error::Error;
use nom::multi::fold_many0;
use nom::sequence::{delimited, preceded};
use nom::Parser;
use std::fmt::{Display, Formatter, Write};

/// Writes `S` to `F`, mapping any characters `FROM` => `TO` their escaped equivalents.
#[macro_export]
macro_rules! write_escaped {
    ($F: expr, $STRING: expr $(, $FROM:expr => $TO:expr)+) => {
        for c in $STRING.chars() {
            match c {
                $(
                $FROM => $F.write_str($TO)?,
                )+
                _ => $F.write_char(c)?,
            }
        }
    };
}
/// Writes `S` to `F`, optionally surrounding in `QUOTE`s, if FN(S) fails,
/// and mapping any characters `FROM` => `TO` their escaped equivalents.
#[macro_export]
macro_rules! write_quoted_string {
    ($F: expr, $QUOTE: literal, $STRING: expr, $FN: expr $(, $FROM:expr => $TO:expr)+) => {
        if nom::sequence::terminated($FN, nom::combinator::eof)($STRING).is_ok() {
            $F.write_str($STRING)?;
        } else {
            // must be escaped
            $F.write_char($QUOTE)?;
            for c in $STRING.chars() {
                match c {
                    $(
                    $FROM => $F.write_str($TO)?,
                    )+
                    _ => $F.write_char(c)?,
                }
            }
            $F.write_char($QUOTE)?;
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
pub(crate) fn single_quoted_string(i: &str) -> ParseResult<&str, String> {
    let escaped = preceded(
        char('\\'),
        expect(
            r"invalid escape sequence, expected \\, \' or \n",
            alt((char('\\'), char('\''), value('\n', char('n')))),
        ),
    );

    string(
        '\'',
        "unterminated string literal",
        verify(is_not("'\\\n"), |s: &str| !s.is_empty()),
        escaped,
    )(i)
}

/// Parse a double-quoted identifier string.
pub(crate) fn double_quoted_string(i: &str) -> ParseResult<&str, String> {
    let escaped = preceded(
        char('\\'),
        expect(
            r#"invalid escape sequence, expected \\, \" or \n"#,
            alt((char('\\'), char('"'), value('\n', char('n')))),
        ),
    );

    string(
        '"',
        "unterminated string literal",
        verify(is_not("\"\\\n"), |s: &str| !s.is_empty()),
        escaped,
    )(i)
}

fn string<'a, T, U, E>(
    delimiter: char,
    unterminated_message: &'static str,
    literal: T,
    escaped: U,
) -> impl FnMut(&'a str) -> ParseResult<&'a str, String, E>
where
    T: Parser<&'a str, &'a str, E>,
    U: Parser<&'a str, char, E>,
    E: ParseError<'a>,
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

    delimited(
        char(delimiter),
        build_string,
        expect(unterminated_message, char(delimiter)),
    )
}

/// Parse regular expression literal characters.
///
/// Consumes i until reaching and escaped delimiter ("\/"), newline or eof.
fn regex_literal(i: &str) -> ParseResult<&str, &str> {
    let mut remaining = &i[..i.len()];
    let mut consumed = &i[..0];

    loop {
        // match everything except `\`, `/` or `\n`
        let (_, match_i) = take_till(|c| c == '\\' || c == '/' || c == '\n')(remaining)?;
        consumed = &i[..(consumed.len() + match_i.len())];
        remaining = &i[consumed.len()..];

        // If we didn't consume anything, check whether it is a newline or regex delimiter,
        // which signals we should leave this parser for outer processing.
        if consumed.is_empty() {
            is_not("/\n")(remaining)?;
        }

        // Try and consume '\' followed by a '/'
        if let Ok((remaining_i, _)) = char::<_, Error<&str>>('\\')(remaining) {
            if char::<_, Error<&str>>('/')(remaining_i).is_ok() {
                // If we didn't consume anything, but we found "\/" sequence,
                // we need to return an error so the outer fold_many0 parser does not trigger
                // an infinite recursion error.
                anychar(consumed)?;

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
pub struct Regex(pub(crate) String);

impl_tuple_clause!(Regex, String);

impl Display for Regex {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_char('/')?;
        write_escaped!(f, self.0, '/' => "\\/");
        f.write_char('/')
    }
}

impl From<&str> for Regex {
    fn from(v: &str) -> Self {
        Self(v.into())
    }
}

/// Parse a regular expression, delimited by `/`.
pub(crate) fn regex(i: &str) -> ParseResult<&str, Regex> {
    map(
        string(
            '/',
            "unterminated regex literal",
            regex_literal,
            map(tag("\\/"), |_| '/'),
        ),
        Regex,
    )(i)
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::assert_expect_error;

    #[test]
    fn test_double_quoted_string() {
        // ascii
        let (_, got) = double_quoted_string(r#""quick draw""#).unwrap();
        assert_eq!(got, "quick draw");

        // ascii
        let (_, got) = double_quoted_string(r#""n.asks""#).unwrap();
        assert_eq!(got, "n.asks");

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

        // Empty string
        let (i, got) = double_quoted_string("\"\"").unwrap();
        assert_eq!(i, "");
        assert_eq!(got, "");

        // ┌─────────────────────────────┐
        // │       Fallible tests        │
        // └─────────────────────────────┘

        // Not terminated
        assert_expect_error!(
            double_quoted_string(r#""quick draw"#),
            "unterminated string literal"
        );

        // Literal newline
        assert_expect_error!(
            double_quoted_string("\"quick\ndraw\""),
            "unterminated string literal"
        );

        // Invalid escape
        assert_expect_error!(
            double_quoted_string(r#""quick\idraw""#),
            r#"invalid escape sequence, expected \\, \" or \n"#
        );
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
        let (_, got) = single_quoted_string(r"'\n\''").unwrap();
        assert_eq!(got, "\n'");

        let (_, got) = single_quoted_string(r"'\'hello\''").unwrap();
        assert_eq!(got, "'hello'");

        // literal tab
        let (_, got) = single_quoted_string("'quick\tdraw'").unwrap();
        assert_eq!(got, "quick\tdraw");

        // literal carriage return
        let (_, got) = single_quoted_string("'quick\rdraw'").unwrap();
        assert_eq!(got, "quick\rdraw");

        // Empty string
        let (i, got) = single_quoted_string("''").unwrap();
        assert_eq!(i, "");
        assert_eq!(got, "");

        // ┌─────────────────────────────┐
        // │       Fallible tests        │
        // └─────────────────────────────┘
        // Not terminated
        assert_expect_error!(
            single_quoted_string(r#"'quick draw"#),
            "unterminated string literal"
        );

        // Invalid escape
        assert_expect_error!(
            single_quoted_string(r"'quick\idraw'"),
            r"invalid escape sequence, expected \\, \' or \n"
        );
    }

    #[test]
    fn test_regex() {
        let (_, got) = regex("/hello/").unwrap();
        assert_eq!(got, "hello".into());

        // handle escaped delimiters "\/"
        let (_, got) = regex(r"/\/this\/is\/a\/path/").unwrap();
        assert_eq!(got, "/this/is/a/path".into());

        // ignores any other possible escape sequence
        let (_, got) = regex(r"/hello\n/").unwrap();
        assert_eq!(got, "hello\\n".into());

        // can parse possible escape sequence at beginning of regex
        let (_, got) = regex(r"/\w.*/").unwrap();
        assert_eq!(got, "\\w.*".into());

        // Empty regex
        let (i, got) = regex("//").unwrap();
        assert_eq!(i, "");
        assert_eq!(got, "".into());

        // Fallible cases

        // Missing trailing delimiter
        assert_expect_error!(regex(r#"/hello"#), "unterminated regex literal");

        // Embedded newline
        assert_expect_error!(regex("/hello\nworld/"), "unterminated regex literal");

        // Single backslash fails, which matches Go implementation
        // See: https://go.dev/play/p/_8J1v5-382G
        assert_expect_error!(regex(r"/\/"), "unterminated regex literal");
    }
}
