//! # Parse an InfluxQL [bind parameter]
//!
//! Bind parameters are parsed where a literal value may appear and are prefixed
//! by a `$`. Per the original Go [implementation], the token following the `$` is
//! parsed as an identifier, and therefore may appear in double quotes.
//!
//! [bind parameter]: https://docs.influxdata.com/influxdb/v1.8/tools/api/#bind-parameters
//! [implementation]: https://github.com/influxdata/influxql/blob/df51a45762be9c1b578f01718fa92d286a843fe9/scanner.go#L57-L62

#![allow(dead_code)]

use crate::string::double_quoted_string;
use crate::write_escaped;
use nom::branch::alt;
use nom::bytes::complete::tag;
use nom::character::complete::{alphanumeric1, char};
use nom::combinator::{map, recognize};
use nom::multi::many1_count;
use nom::sequence::preceded;
use nom::IResult;
use std::fmt;
use std::fmt::{Display, Formatter, Write};

/// Parse an unquoted InfluxQL bind parameter.
fn unquoted_parameter(i: &str) -> IResult<&str, String> {
    map(
        recognize(many1_count(alt((alphanumeric1, tag("_"))))),
        str::to_string,
    )(i)
}

/// `BindParameter` is a type that represents either a quoted ([`BindParameter::Quoted`]) or unquoted ([`BindParameter::Unquoted`])
/// InfluxQL bind parameter.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum BindParameter {
    /// Contains an unquoted bind parameter
    Unquoted(String),

    /// Contains an unescaped quoted identifier
    Quoted(String),
}

impl Display for BindParameter {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unquoted(s) => write!(f, "${}", s)?,
            Self::Quoted(s) => {
                f.write_str("$\"")?;
                // escape characters per https://github.com/influxdata/influxql/blob/df51a45762be9c1b578f01718fa92d286a843fe9/scanner.go#L576-L583
                write_escaped!(f, s, '\n' => "\\n", '\\' => "\\\\", '"' => "\\\"");
                f.write_char('"')?;
            }
        };

        Ok(())
    }
}

/// Parses an InfluxQL [BindParameter].
pub fn parameter(i: &str) -> IResult<&str, BindParameter> {
    // See: https://github.com/influxdata/influxql/blob/df51a45762be9c1b578f01718fa92d286a843fe9/scanner.go#L358-L362
    preceded(
        char('$'),
        alt((
            map(unquoted_parameter, BindParameter::Unquoted),
            map(double_quoted_string, BindParameter::Quoted),
        )),
    )(i)
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_parameter() {
        // all ascii
        let (_, got) = parameter("$cpu").unwrap();
        assert_eq!(got, BindParameter::Unquoted("cpu".into()));

        // digits
        let (_, got) = parameter("$01").unwrap();
        assert_eq!(got, BindParameter::Unquoted("01".into()));

        // all valid chars
        let (_, got) = parameter("$cpu_0").unwrap();
        assert_eq!(got, BindParameter::Unquoted("cpu_0".into()));

        // keyword
        let (_, got) = parameter("$from").unwrap();
        assert_eq!(got, BindParameter::Unquoted("from".into()));

        // quoted
        let (_, got) = parameter("$\"quick draw\"").unwrap();
        assert!(matches!(got, BindParameter::Quoted(s) if s == "quick draw"));

        // ┌─────────────────────────────┐
        // │       Fallible tests        │
        // └─────────────────────────────┘

        // missing `$` prefix
        let res = parameter("cpu");
        assert!(res.is_err());
    }

    #[test]
    fn test_bind_parameter_display() {
        // test quoted identifier properly escapes specific characters
        let got = format!("{}", BindParameter::Quoted("from".to_string()));
        assert_eq!(got, r#"$"from""#);

        // test unquoted identifier
        let got = format!("{}", BindParameter::Unquoted("quick_draw".to_string()));
        assert_eq!(got, "$quick_draw");
    }
}
