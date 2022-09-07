#![allow(dead_code)]

use crate::expression::{conditional_expression, Expr};
use crate::identifier::{identifier, Identifier};
use core::fmt;
use nom::branch::alt;
use nom::bytes::complete::{tag, tag_no_case};
use nom::character::complete::{digit1, line_ending, multispace0, multispace1};
use nom::combinator::{cut, eof, map, map_res, opt, value};
use nom::sequence::{delimited, pair, preceded, terminated};
use nom::IResult;
use std::fmt::Formatter;

/// Represents a fully-qualified measurement name.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct MeasurementNameExpression {
    pub database: Option<Identifier>,
    pub retention_policy: Option<Identifier>,
    pub name: Identifier,
}

impl fmt::Display for MeasurementNameExpression {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self {
                database: None,
                retention_policy: None,
                name,
            } => write!(f, "{}", name)?,
            Self {
                database: Some(db),
                retention_policy: None,
                name,
            } => write!(f, "{}..{}", db, name)?,
            Self {
                database: None,
                retention_policy: Some(rp),
                name,
            } => write!(f, "{}.{}", rp, name)?,
            Self {
                database: Some(db),
                retention_policy: Some(rp),
                name,
            } => write!(f, "{}.{}.{}", db, rp, name)?,
        };
        Ok(())
    }
}

/// Match a 3-part measurement name expression.
pub fn measurement_name_expression(i: &str) -> IResult<&str, MeasurementNameExpression> {
    let (remaining_input, (opt_db_rp, name)) = pair(
        opt(alt((
            // database "." retention_policy "."
            map(
                pair(
                    terminated(identifier, tag(".")),
                    terminated(identifier, tag(".")),
                ),
                |(db, rp)| (Some(db), Some(rp)),
            ),
            // database ".."
            map(terminated(identifier, tag("..")), |db| (Some(db), None)),
            // retention_policy "."
            map(terminated(identifier, tag(".")), |rp| (None, Some(rp))),
        ))),
        identifier,
    )(i)?;

    // Extract possible `database` and / or `retention_policy`
    let (database, retention_policy) = match opt_db_rp {
        Some(db_rp) => db_rp,
        _ => (None, None),
    };

    Ok((
        remaining_input,
        MeasurementNameExpression {
            database,
            retention_policy,
            name,
        },
    ))
}

/// Parse an unsigned integer.
fn unsigned_number(i: &str) -> IResult<&str, u64> {
    map_res(digit1, |s: &str| s.parse())(i)
}

/// Parse a LIMIT <n> clause.
pub fn limit_clause(i: &str) -> IResult<&str, u64> {
    preceded(pair(tag_no_case("LIMIT"), multispace1), unsigned_number)(i)
}

/// Parse an OFFSET <n> clause.
pub fn offset_clause(i: &str) -> IResult<&str, u64> {
    preceded(pair(tag_no_case("OFFSET"), multispace1), unsigned_number)(i)
}

/// Parse a terminator that ends a SQL statement.
pub fn statement_terminator(i: &str) -> IResult<&str, ()> {
    let (remaining_input, _) =
        delimited(multispace0, alt((tag(";"), line_ending, eof)), multispace0)(i)?;

    Ok((remaining_input, ()))
}

/// Parse a `WHERE` clause.
pub(crate) fn where_clause(i: &str) -> IResult<&str, Expr> {
    preceded(
        pair(tag_no_case("WHERE"), multispace1),
        conditional_expression,
    )(i)
}

/// Represents an InfluxQL `ORDER BY` clause.
#[derive(Default, Debug, Clone, Eq, PartialEq)]
pub enum OrderByClause {
    #[default]
    Ascending,
    Descending,
}

/// Parse an InfluxQL `ORDER BY` clause.
///
/// An `ORDER BY` in InfluxQL is limited when compared to the equivalent
/// SQL definition. It is defined by the following [EBNF] notation:
///
/// ```text
/// order_by   ::= "ORDER" "BY" (time_order | order)
/// order      ::= "ASC | "DESC
/// time_order ::= "TIME" order?
/// ```
///
/// Resulting in the following valid strings:
///
/// ```text
/// ORDER BY ASC
/// ORDER BY DESC
/// ORDER BY time
/// ORDER BY time ASC
/// ORDER BY time DESC
/// ```
///
/// [EBNF]: https://www.w3.org/TR/2010/REC-xquery-20101214/#EBNFNotation
pub(crate) fn order_by_clause(i: &str) -> IResult<&str, OrderByClause> {
    let order = || {
        preceded(
            multispace1,
            alt((
                value(OrderByClause::Ascending, tag_no_case("ASC")),
                value(OrderByClause::Descending, tag_no_case("DESC")),
            )),
        )
    };

    preceded(
        // "ORDER" "BY"
        pair(
            tag_no_case("ORDER"),
            preceded(multispace1, tag_no_case("BY")),
        ),
        // cut to force failure, as `ORDER BY` must be followed by one of the following
        cut(alt((
            // "ASC" | "DESC"
            order(),
            // "TIME" ( "ASC" | "DESC" )?
            map(
                preceded(preceded(multispace1, tag_no_case("TIME")), opt(order())),
                Option::<_>::unwrap_or_default,
            ),
        ))),
    )(i)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::assert_failure;

    #[test]
    fn test_measurement_name_expression() {
        let (_, got) = measurement_name_expression("diskio").unwrap();
        assert_eq!(
            got,
            MeasurementNameExpression {
                database: None,
                retention_policy: None,
                name: Identifier::Unquoted("diskio".into()),
            }
        );

        let (_, got) = measurement_name_expression("telegraf.autogen.diskio").unwrap();
        assert_eq!(
            got,
            MeasurementNameExpression {
                database: Some(Identifier::Unquoted("telegraf".into())),
                retention_policy: Some(Identifier::Unquoted("autogen".into())),
                name: Identifier::Unquoted("diskio".into()),
            }
        );

        let (_, got) = measurement_name_expression("telegraf..diskio").unwrap();
        assert_eq!(
            got,
            MeasurementNameExpression {
                database: Some(Identifier::Unquoted("telegraf".into())),
                retention_policy: None,
                name: Identifier::Unquoted("diskio".into()),
            }
        );
    }

    #[test]
    fn test_limit_clause() {
        let (_, got) = limit_clause("LIMIT 587").unwrap();
        assert_eq!(got, 587);

        // case insensitive
        let (_, got) = limit_clause("limit 587").unwrap();
        assert_eq!(got, 587);

        // extra spaces between tokens
        let (_, got) = limit_clause("LIMIT     123").unwrap();
        assert_eq!(got, 123);

        // not digits
        limit_clause("LIMIT sdf").unwrap_err();

        // overflow
        limit_clause("LIMIT 34593745733489743985734857394").unwrap_err();
    }

    #[test]
    fn test_offset_clause() {
        let (_, got) = offset_clause("OFFSET 587").unwrap();
        assert_eq!(got, 587);

        // case insensitive
        let (_, got) = offset_clause("offset 587").unwrap();
        assert_eq!(got, 587);

        // extra spaces between tokens
        let (_, got) = offset_clause("OFFSET     123").unwrap();
        assert_eq!(got, 123);

        // not digits
        offset_clause("OFFSET sdf").unwrap_err();

        // overflow
        offset_clause("OFFSET 34593745733489743985734857394").unwrap_err();
    }

    #[test]
    fn test_order_by() {
        use OrderByClause::*;

        let (_, got) = order_by_clause("ORDER by asc").unwrap();
        assert_eq!(got, Ascending);

        let (_, got) = order_by_clause("ORDER by desc").unwrap();
        assert_eq!(got, Descending);

        let (_, got) = order_by_clause("ORDER by time asc").unwrap();
        assert_eq!(got, Ascending);

        let (_, got) = order_by_clause("ORDER by time desc").unwrap();
        assert_eq!(got, Descending);

        // default case is ascending
        let (_, got) = order_by_clause("ORDER by time").unwrap();
        assert_eq!(got, Ascending);

        // does not consume remaining input
        let (i, got) = order_by_clause("ORDER by time LIMIT 10").unwrap();
        assert_eq!(got, Ascending);
        assert_eq!(i, " LIMIT 10");

        // Fallible cases

        // Must be "time" identifier
        assert_failure!(order_by_clause("ORDER by foo"));
    }

    #[test]
    fn test_where_clause() {
        // Can parse a WHERE clause
        where_clause("WHERE foo = 'bar'").unwrap();

        // Remaining input is not consumed
        let (i, _) = where_clause("WHERE foo = 'bar' LIMIT 10").unwrap();
        assert_eq!(i, " LIMIT 10");

        // Fallible cases
        where_clause("WHERE foo = LIMIT 10").unwrap_err();
        where_clause("WHERE").unwrap_err();
    }
}
