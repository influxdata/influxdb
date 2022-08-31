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
use crate::string::double_quoted_string;
use crate::write_escaped;
use nom::branch::alt;
use nom::bytes::complete::tag;
use nom::character::complete::{alpha1, alphanumeric1};
use nom::combinator::{map, not, recognize};
use nom::multi::many0_count;
use nom::sequence::{pair, preceded};
use nom::IResult;
use std::fmt;
use std::fmt::{Display, Formatter, Write};

/// Parse an unquoted InfluxQL identifier.
fn unquoted_identifier(i: &str) -> IResult<&str, String> {
    map(
        preceded(
            not(sql_keyword),
            recognize(pair(
                alt((alpha1, tag("_"))),
                many0_count(alt((alphanumeric1, tag("_")))),
            )),
        ),
        str::to_string,
    )(i)
}

/// `Identifier` is a type that represents either a quoted ([`Identifier::Quoted`]) or unquoted ([`Identifier::Unquoted`])
/// InfluxQL identifier.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum Identifier {
    /// Contains an unquoted identifier
    Unquoted(String),

    /// Contains an unescaped quoted identifier
    Quoted(String),
}

impl Display for Identifier {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unquoted(s) => write!(f, "{}", s)?,
            Self::Quoted(s) => {
                f.write_char('"')?;
                // escape characters per https://github.com/influxdata/influxql/blob/df51a45762be9c1b578f01718fa92d286a843fe9/scanner.go#L576-L583
                write_escaped!(f, s, '\n' => "\\n", '\\' => "\\\\", '"' => "\\\"");
                f.write_char('"')?;
            }
        };

        Ok(())
    }
}

/// Parses an InfluxQL [Identifier].
pub fn identifier(i: &str) -> IResult<&str, Identifier> {
    // See: https://github.com/influxdata/influxql/blob/df51a45762be9c1b578f01718fa92d286a843fe9/scanner.go#L358-L362
    alt((
        map(unquoted_identifier, Identifier::Unquoted),
        map(double_quoted_string, Identifier::Quoted),
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
        assert_eq!(got, r#""quick\n	\\\"'draw 👽""#);

        // test unquoted identifier
        let got = format!("{}", Identifier::Unquoted("quick_draw".to_string()));
        assert_eq!(got, "quick_draw");
    }
}
