//! # Parse an InfluxQL [identifier]
//!
//! Identifiers are parsed using the following rules:
//!
//! * double quoted identifiers can contain any unicode character other than a new line
//! * double quoted identifiers can contain escaped characters, namely `\"`, `\n`, `\t`, `\\` and `\'`
//! * double quoted identifiers can contain [InfluxQL keywords][keywords]
//! * unquoted identifiers must start with an upper or lowercase ASCII character or `_`
//! * unquoted identifiers may contain only ASCII letters, decimal digits, and `_`
//! * identifiers may be preceded by whitespace
//!
//! [identifier]: https://docs.influxdata.com/influxdb/v1.8/query_language/spec/#identifiers
//! [keywords]: https://docs.influxdata.com/influxdb/v1.8/query_language/spec/#keywords

use crate::common::ws0;
use crate::internal::ParseResult;
use crate::keywords::sql_keyword;
use crate::string::double_quoted_string;
use crate::{impl_tuple_clause, write_quoted_string};
use nom::branch::alt;
use nom::bytes::complete::tag;
use nom::character::complete::{alpha1, alphanumeric1};
use nom::combinator::{map, not, recognize};
use nom::multi::many0_count;
use nom::sequence::{pair, preceded};
use std::fmt::{Display, Formatter, Write};
use std::{fmt, mem};

/// Parse an unquoted InfluxQL identifier.
pub(crate) fn unquoted_identifier(i: &str) -> ParseResult<&str, &str> {
    preceded(
        not(sql_keyword),
        recognize(pair(
            alt((alpha1, tag("_"))),
            many0_count(alt((alphanumeric1, tag("_")))),
        )),
    )(i)
}

/// A type that represents an InfluxQL identifier.
#[derive(Clone, Debug, Eq, Hash, PartialEq, Ord, PartialOrd)]
pub struct Identifier(pub(crate) String);

impl_tuple_clause!(Identifier, String);

impl From<&str> for Identifier {
    fn from(s: &str) -> Self {
        Self(s.to_string())
    }
}

impl Identifier {
    /// Returns true if the identifier requires quotes.
    pub fn requires_quotes(&self) -> bool {
        nom::sequence::terminated(unquoted_identifier, nom::combinator::eof)(&self.0).is_err()
    }

    /// Takes the string value out of the identifier, leaving a default string value in its place.
    pub fn take(&mut self) -> String {
        mem::take(&mut self.0)
    }
}

impl Display for Identifier {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write_quoted_string!(f, '"', self.0.as_str(), unquoted_identifier, '\n' => "\\n", '\\' => "\\\\", '"' => "\\\"");
        Ok(())
    }
}

/// Parses an InfluxQL [Identifier].
///
/// EBNF for an identifier is approximately:
///
/// ```text
/// identifier          ::= whitespace? ( quoted_identifier | unquoted_identifier )
/// unquoted_identifier ::= [_a..zA..Z] [_a..zA..Z0..9]*
/// quoted_identifier   ::= '"' [^"\n] '"'
/// ```
pub(crate) fn identifier(i: &str) -> ParseResult<&str, Identifier> {
    // See: https://github.com/influxdata/influxql/blob/7e7d61973256ffeef4b99edd0a89f18a9e52fa2d/parser.go#L432-L438
    preceded(
        ws0,
        alt((
            map(unquoted_identifier, Into::into),
            map(double_quoted_string, Into::into),
        )),
    )(i)
}

#[cfg(test)]
mod test {
    use super::*;

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
        unquoted_identifier("0cpu").unwrap_err();

        // is a keyword
        unquoted_identifier("as").unwrap_err();
    }

    #[test]
    fn test_identifier() {
        // quoted
        let (_, got) = identifier("\"quick draw\"").unwrap();
        assert_eq!(got, "quick draw".into());
        // validate that `as_str` returns the unquoted string
        assert_eq!(got.as_str(), "quick draw");

        // unquoted
        let (_, got) = identifier("quick_draw").unwrap();
        assert_eq!(got, "quick_draw".into());

        // leading whitespace
        let (_, got) = identifier("  quick_draw").unwrap();
        assert_eq!(got, "quick_draw".into());
    }

    #[test]
    fn test_identifier_display() {
        // Identifier properly escapes specific characters and quotes output
        let got = Identifier("quick\n\t\\\"'draw \u{1f47d}".into()).to_string();
        assert_eq!(got, r#""quick\n	\\\"'draw 👽""#);

        // Identifier displays unquoted output
        let got = Identifier("quick_draw".into()).to_string();
        assert_eq!(got, "quick_draw");
    }

    #[test]
    fn test_identifier_requires_quotes() {
        // Following examples require quotes

        // Quotes, spaces, non-ASCII
        assert!(Identifier("quick\n\t\\\"'draw \u{1f47d}".into()).requires_quotes());
        // non-ASCII
        assert!(Identifier("quick_\u{1f47d}".into()).requires_quotes());
        // starts with number
        assert!(Identifier("0quick".into()).requires_quotes());

        // Following examples do not require quotes

        // starts with underscore
        assert!(!Identifier("_quick".into()).requires_quotes());

        // Only ASCII, non-space
        assert!(!Identifier("quick_90".into()).requires_quotes());
    }
}
