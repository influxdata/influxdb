//! # Parse an InfluxQL [bind parameter]
//!
//! Bind parameters are parsed where a literal value may appear and are prefixed
//! by a `$`. Per the original Go [implementation], the token following the `$` is
//! parsed as an identifier, and therefore may appear in double quotes.
//!
//! [bind parameter]: https://docs.influxdata.com/influxdb/v1.8/tools/api/#bind-parameters
//! [implementation]: https://github.com/influxdata/influxql/blob/df51a45762be9c1b578f01718fa92d286a843fe9/scanner.go#L57-L62

use crate::internal::ParseResult;
use crate::string::double_quoted_string;
use crate::{impl_tuple_clause, write_quoted_string};
use nom::branch::alt;
use nom::bytes::complete::tag;
use nom::character::complete::{alphanumeric1, char};
use nom::combinator::{map, recognize};
use nom::multi::many1_count;
use nom::sequence::preceded;
use std::fmt;
use std::fmt::{Display, Formatter, Write};

/// Parse an unquoted InfluxQL bind parameter.
fn unquoted_parameter(i: &str) -> ParseResult<&str, &str> {
    recognize(many1_count(alt((alphanumeric1, tag("_")))))(i)
}

/// A type that represents an InfluxQL bind parameter.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct BindParameter(pub(crate) String);

impl_tuple_clause!(BindParameter, String);

impl From<&str> for BindParameter {
    fn from(s: &str) -> Self {
        Self(s.to_string())
    }
}

impl Display for BindParameter {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_char('$')?;
        write_quoted_string!(f, '"', self.0.as_str(), unquoted_parameter, '\n' => "\\n", '\\' => "\\\\", '"' => "\\\"");
        Ok(())
    }
}

/// Parses an InfluxQL [BindParameter].
pub(crate) fn parameter(i: &str) -> ParseResult<&str, BindParameter> {
    // See: https://github.com/influxdata/influxql/blob/df51a45762be9c1b578f01718fa92d286a843fe9/scanner.go#L358-L362
    preceded(
        char('$'),
        alt((
            map(unquoted_parameter, Into::into),
            map(double_quoted_string, Into::into),
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
        assert_eq!(got, "cpu".into());

        // digits
        let (_, got) = parameter("$01").unwrap();
        assert_eq!(got, "01".into());

        // all valid chars
        let (_, got) = parameter("$cpu_0").unwrap();
        assert_eq!(got, "cpu_0".into());

        // keyword
        let (_, got) = parameter("$from").unwrap();
        assert_eq!(got, "from".into());

        // quoted
        let (_, got) = parameter("$\"quick draw\"").unwrap();
        assert_eq!(got, "quick draw".into());

        // ┌─────────────────────────────┐
        // │       Fallible tests        │
        // └─────────────────────────────┘

        // missing `$` prefix
        parameter("cpu").unwrap_err();
    }

    #[test]
    fn test_bind_parameter_display() {
        // BindParameter displays quoted output
        let got = BindParameter("from foo".into()).to_string();
        assert_eq!(got, r#"$"from foo""#);

        // BindParameter displays quoted and escaped output
        let got = BindParameter("from\nfoo".into()).to_string();
        assert_eq!(got, r#"$"from\nfoo""#);

        // BindParameter displays unquoted output
        let got = BindParameter("quick_draw".into()).to_string();
        assert_eq!(got, "$quick_draw");
    }
}
