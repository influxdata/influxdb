//! # Parse an InfluxQL [bind parameter]
//!
//! Bind parameters are parsed where a literal value may appear and are prefixed
//! by a `$`. Per the original Go [implementation], the token following the `$` is
//! parsed as an identifier, and therefore may appear in double quotes.
//!
//! [bind parameter]: https://docs.influxdata.com/influxdb/v1.8/tools/api/#bind-parameters
//! [implementation]: https://github.com/influxdata/influxql/blob/df51a45762be9c1b578f01718fa92d286a843fe9/scanner.go#L57-L62

use std::collections::HashSet;

use iox_query_params::{StatementParam, StatementParams};
use thiserror::Error;

use crate::expression::Expr;
use crate::internal::ParseResult;
use crate::literal::Literal;
use crate::statement::Statement;
use crate::string::double_quoted_string;
use crate::visit_mut::{Recursion, VisitableMut, VisitorMut};
use crate::{impl_tuple_clause, write_quoted_string};
use nom::Parser;
use nom::branch::alt;
use nom::bytes::complete::tag;
use nom::character::complete::{alphanumeric1, char};
use nom::combinator::{map, recognize};
use nom::multi::many1_count;
use nom::sequence::preceded;
use std::fmt;
use std::fmt::{Display, Formatter, Write};

#[derive(Debug, Clone, Error)]
/// Errors that occur during bind parameter replacement
pub enum BindParameterError {
    /// Error that occurs when the user provides a parameter value
    /// whose type is not supported by InfluxQL.
    #[error("Parameter type '{0}' is not supported by InfluxQL")]
    TypeNotSupported(String),
    /// Error that occurs when the user provides a parameter value that
    /// was not found within the query/statement.
    #[error("Bind parameter '{0}' was provided but not found in the InfluxQL statement")]
    NotFound(String),
    /// Error that occurs when a query contains a bind parameter placeholder
    /// but the corresponding parameter value was not provided by the user.
    #[error(
        "Bind parameter '{0}' was referenced in the InfluxQL statement but its value is undefined."
    )]
    NotDefined(String),
}

/// Parse an unquoted InfluxQL bind parameter.
fn unquoted_parameter(i: &str) -> ParseResult<&str, &str> {
    recognize(many1_count(alt((alphanumeric1, tag("_"))))).parse(i)
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
    )
    .parse(i)
}

/// Convert a [StatementParam] value to an InfluxQL [Literal]
///
/// Will return an error on NULL
fn param_value_to_literal(value: StatementParam) -> Result<Literal, BindParameterError> {
    match value {
        StatementParam::Null => Err(BindParameterError::TypeNotSupported("NULL".to_string())),
        StatementParam::Int64(i) => Ok(Literal::Integer(i)),
        StatementParam::UInt64(u) => Ok(Literal::Unsigned(u)),
        StatementParam::Boolean(b) => Ok(Literal::Boolean(b)),
        StatementParam::Float64(f) => Ok(Literal::Float(f)),
        StatementParam::String(s) => Ok(Literal::String(s)),
    }
}

struct ReplaceBindParamsWithValuesVisitor {
    params: StatementParams,
    found: HashSet<String>,
}

impl ReplaceBindParamsWithValuesVisitor {
    fn new(params: StatementParams) -> Self {
        let len = params.len();
        Self {
            params,
            found: HashSet::with_capacity(len),
        }
    }
}

impl VisitorMut for ReplaceBindParamsWithValuesVisitor {
    type Error = BindParameterError;
    fn pre_visit_expr(&mut self, expr: &mut Expr) -> Result<Recursion, Self::Error> {
        match expr {
            Expr::BindParameter(BindParameter(id)) => {
                if let Some(value) = self.params.get(id) {
                    self.found.insert(id.clone());
                    *expr = Expr::Literal(param_value_to_literal(value.clone())?);
                    Ok(Recursion::Continue)
                } else {
                    Err(BindParameterError::NotDefined(format!("${id}")))
                }
            }
            _ => Ok(Recursion::Continue),
        }
    }
    fn post_visit_statement(&mut self, _n: &mut Statement) -> Result<(), Self::Error> {
        for name in self.params.names() {
            if !self.found.contains(name) {
                return Err(BindParameterError::NotFound(format!("${name}")));
            }
        }
        Ok(())
    }
}

/// Transforms an InfluxQL [Statement] by replacing [Expr::BindParameter]s
/// that match the given [StatementParam]s
///
/// Will error if a parameter is not found in the statement.
pub fn replace_bind_params_with_values(
    mut stmt: Statement,
    params: StatementParams,
) -> Result<Statement, BindParameterError> {
    stmt.accept(&mut ReplaceBindParamsWithValuesVisitor::new(params))?;
    Ok(stmt)
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
