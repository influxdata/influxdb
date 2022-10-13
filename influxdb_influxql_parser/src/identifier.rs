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
use std::fmt;
use std::fmt::{Display, Formatter, Write};

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
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct Identifier(pub(crate) String);

impl_tuple_clause!(Identifier, String);

impl From<&str> for Identifier {
    fn from(s: &str) -> Self {
        Self(s.to_string())
    }
}

impl Display for Identifier {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write_quoted_string!(f, '"', self.0.as_str(), unquoted_identifier, '\n' => "\\n", '\\' => "\\\\", '"' => "\\\"");
        Ok(())
    }
}

/// Parses an InfluxQL [Identifier].
pub(crate) fn identifier(i: &str) -> ParseResult<&str, Identifier> {
    // See: https://github.com/influxdata/influxql/blob/df51a45762be9c1b578f01718fa92d286a843fe9/scanner.go#L358-L362
    alt((
        map(unquoted_identifier, Into::into),
        map(double_quoted_string, Into::into),
    ))(i)
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

        // unquoted
        let (_, got) = identifier("quick_draw").unwrap();
        assert_eq!(got, "quick_draw".into());
    }

    #[test]
    fn test_identifier_display() {
        // Identifier properly escapes specific characters and quotes output
        let got = format!("{}", Identifier("quick\n\t\\\"'draw \u{1f47d}".into()));
        assert_eq!(got, r#""quick\n	\\\"'draw 👽""#);

        // Identifier displays unquoted output
        let got = format!("{}", Identifier("quick_draw".into()));
        assert_eq!(got, "quick_draw");
    }
}
