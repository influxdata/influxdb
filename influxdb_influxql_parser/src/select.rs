//! Types and parsers for the [`SELECT`][sql] statement.
//!
//! [sql]: https://docs.influxdata.com/influxdb/v1.8/query_language/explore-data/#the-basic-select-statement

use crate::common::{
    limit_clause, offset_clause, order_by_clause, qualified_measurement_name, where_clause, ws0,
    ws1, LimitClause, OffsetClause, OrderByClause, Parser, QualifiedMeasurementName, WhereClause,
    ZeroOrMore,
};
use crate::expression::arithmetic::Expr::Wildcard;
use crate::expression::arithmetic::{
    arithmetic, call_expression, var_ref, ArithmeticParsers, Expr, WildcardType,
};
use crate::expression::conditional::is_valid_now_call;
use crate::identifier::{identifier, Identifier};
use crate::internal::{expect, verify, ParseResult};
use crate::keywords::keyword;
use crate::literal::{duration, literal, number, unsigned_integer, Literal, Number};
use crate::parameter::parameter;
use crate::select::MeasurementSelection::Subquery;
use crate::string::{regex, single_quoted_string, Regex};
use crate::{impl_tuple_clause, write_escaped};
use nom::branch::alt;
use nom::bytes::complete::tag;
use nom::character::complete::char;
use nom::combinator::{map, opt, value};
use nom::sequence::{delimited, pair, preceded, tuple};
use std::fmt;
use std::fmt::{Display, Formatter, Write};

/// Represents a `SELECT` statement.
#[derive(Clone, Debug, PartialEq)]
pub struct SelectStatement {
    /// Expressions returned by the selection.
    pub fields: FieldList,

    /// A list of measurements or subqueries used as the source data for the selection.
    pub from: FromMeasurementClause,

    /// A conditional expression to filter the selection.
    pub condition: Option<WhereClause>,

    /// Expressions used for grouping the selection.
    pub group_by: Option<GroupByClause>,

    /// The [fill clause] specifies the fill behaviour for the selection. If the value is [`None`],
    /// it is the same behavior as `fill(none)`.
    ///
    /// [fill]: https://docs.influxdata.com/influxdb/v1.8/query_language/explore-data/#group-by-time-intervals-and-fill
    pub fill: Option<FillClause>,

    /// Configures the ordering of the selection by time.
    pub order_by: Option<OrderByClause>,

    /// A value to restrict the number of rows returned.
    pub limit: Option<LimitClause>,

    /// A value to specify an offset to start retrieving rows.
    pub offset: Option<OffsetClause>,

    /// A value to restrict the number of series returned.
    pub series_limit: Option<SLimitClause>,

    /// A value to specify an offset to start retrieving series.
    pub series_offset: Option<SOffsetClause>,

    /// The timezone for the query, specified as [`tz('<time zone>')`][time_zone_clause].
    ///
    /// [time_zone_clause]: https://docs.influxdata.com/influxdb/v1.8/query_language/explore-data/#the-time-zone-clause
    pub timezone: Option<TimeZoneClause>,
}

impl Display for SelectStatement {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "SELECT {} {}", self.fields, self.from)?;

        if let Some(where_clause) = &self.condition {
            write!(f, " {}", where_clause)?;
        }

        if let Some(group_by) = &self.group_by {
            write!(f, " {}", group_by)?;
        }

        if let Some(fill_clause) = &self.fill {
            write!(f, " {}", fill_clause)?;
        }

        if let Some(order_by) = &self.order_by {
            write!(f, " {}", order_by)?;
        }

        if let Some(limit) = &self.limit {
            write!(f, " {}", limit)?;
        }

        if let Some(offset) = &self.offset {
            write!(f, " {}", offset)?;
        }

        if let Some(slimit) = &self.series_limit {
            write!(f, " {}", slimit)?;
        }

        if let Some(soffset) = &self.series_offset {
            write!(f, " {}", soffset)?;
        }

        if let Some(tz_clause) = &self.timezone {
            write!(f, " {}", tz_clause)?;
        }

        Ok(())
    }
}

pub(crate) fn select_statement(i: &str) -> ParseResult<&str, SelectStatement> {
    let (
        remaining,
        (
            _, // SELECT
            _, // whitespace
            fields,
            from,
            condition,
            group_by,
            fill,
            order_by,
            limit,
            offset,
            series_limit,
            series_offset,
            timezone,
        ),
    ) = tuple((
        keyword("SELECT"),
        ws0,
        field_list,
        preceded(ws0, from_clause),
        opt(preceded(ws0, where_clause)),
        opt(preceded(ws0, group_by_clause)),
        opt(preceded(ws0, fill_clause)),
        opt(preceded(ws0, order_by_clause)),
        opt(preceded(ws0, limit_clause)),
        opt(preceded(ws0, offset_clause)),
        opt(preceded(ws0, slimit_clause)),
        opt(preceded(ws0, soffset_clause)),
        opt(preceded(ws0, timezone_clause)),
    ))(i)?;

    Ok((
        remaining,
        SelectStatement {
            fields,
            from,
            condition,
            group_by,
            fill,
            order_by,
            limit,
            offset,
            series_limit,
            series_offset,
            timezone,
        },
    ))
}

/// Represents a single measurement selection for a `FROM` clause.
#[derive(Clone, Debug, PartialEq)]
pub enum MeasurementSelection {
    /// The measurement selection is measurement name or regular expression.
    Name(QualifiedMeasurementName),

    /// The measurement selection is a subquery.
    Subquery(Box<SelectStatement>),
}

impl Display for MeasurementSelection {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Name(ref name) => fmt::Display::fmt(name, f),
            Self::Subquery(ref subquery) => write!(f, "({})", subquery),
        }
    }
}

impl Parser for MeasurementSelection {
    fn parse(i: &str) -> ParseResult<&str, Self> {
        alt((
            map(qualified_measurement_name, MeasurementSelection::Name),
            map(
                delimited(
                    preceded(ws0, char('(')),
                    preceded(ws0, select_statement),
                    preceded(ws0, char(')')),
                ),
                |s| Subquery(Box::new(s)),
            ),
        ))(i)
    }
}

/// Represents a `FROM` clause for a `SELECT` statement.
pub type FromMeasurementClause = ZeroOrMore<MeasurementSelection>;

impl Display for FromMeasurementClause {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        if let Some(first) = self.head() {
            write!(f, "FROM {}", first)?;
            for arg in self.tail() {
                write!(f, ", {}", arg)?;
            }
        }

        Ok(())
    }
}

fn from_clause(i: &str) -> ParseResult<&str, FromMeasurementClause> {
    preceded(
        pair(keyword("FROM"), ws0),
        FromMeasurementClause::separated_list1(
            "invalid FROM clause, expected identifier, regular expression or subquery",
        ),
    )(i)
}

/// Represents the collection of dimensions for a `GROUP BY` clause.
pub type GroupByClause = ZeroOrMore<Dimension>;

impl Display for GroupByClause {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        if let Some(first) = self.head() {
            write!(f, "GROUP BY {}", first)?;
            for arg in self.tail() {
                write!(f, ", {}", arg)?;
            }
        }

        Ok(())
    }
}

/// Used to parse the interval argument of the TIME function
struct TimeCallIntervalArgument;

impl ArithmeticParsers for TimeCallIntervalArgument {
    fn operand(i: &str) -> ParseResult<&str, Expr> {
        // Any literal
        preceded(
            ws0,
            map(
                alt((
                    map(duration, Literal::Duration),
                    map(unsigned_integer, Literal::Unsigned),
                )),
                Expr::Literal,
            ),
        )(i)
    }
}

/// Used to parse the offset argument of the TIME function
///
/// The offset argument accepts either a duration, datetime-like string or `now`.
struct TimeCallOffsetArgument;

impl TimeCallOffsetArgument {
    /// Parse the `now()` function call
    fn now_call(i: &str) -> ParseResult<&str, Expr> {
        verify(
            "invalid expression, the only valid function call is 'now' with no arguments",
            call_expression::<Self>,
            is_valid_now_call,
        )(i)
    }
}

impl ArithmeticParsers for TimeCallOffsetArgument {
    fn operand(i: &str) -> ParseResult<&str, Expr> {
        preceded(
            ws0,
            alt((
                Self::now_call,
                map(duration, |v| Expr::Literal(Literal::Duration(v))),
                map(single_quoted_string, |v| Expr::Literal(Literal::String(v))),
            )),
        )(i)
    }
}

/// Represents a dimension of a `GROUP BY` clause.
#[derive(Clone, Debug, PartialEq)]
pub enum Dimension {
    /// Represents a `TIME` call in a `GROUP BY` clause.
    Time {
        /// The first argument of the `TIME` call.
        interval: Expr,
        /// An optional second argument to specify the offset applied to the `TIME` call.
        offset: Option<Expr>,
    },

    /// Represents a literal tag reference in a `GROUP BY` clause.
    Tag(Identifier),

    /// Represents a regular expression in a `GROUP BY` clause.
    Regex(Regex),

    /// Represents a wildcard in a `GROUP BY` clause.
    Wildcard,
}

impl Display for Dimension {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Time {
                interval,
                offset: Some(offset),
            } => write!(f, "TIME({}, {})", interval, offset),
            Self::Time { interval, .. } => write!(f, "TIME({})", interval),
            Self::Tag(v) => Display::fmt(v, f),
            Self::Regex(v) => Display::fmt(v, f),
            Self::Wildcard => f.write_char('*'),
        }
    }
}

impl Parser for Dimension {
    fn parse(i: &str) -> ParseResult<&str, Self> {
        alt((
            // Explicitly ignore the `WildCardType`, is InfluxQL always assumes `*::tag`
            map(wildcard, |_| Self::Wildcard),
            time_call_expression,
            map(regex, Self::Regex),
            map(var_ref, |v| {
                Self::Tag(match v {
                    Expr::VarRef { name, .. } => name,
                    // var_ref only returns Expr::VarRef
                    _ => unreachable!(),
                })
            }),
        ))(i)
    }
}

fn time_call_expression(i: &str) -> ParseResult<&str, Dimension> {
    map(
        preceded(
            keyword("TIME"),
            delimited(
                expect(
                    "invalid TIME call, expected 1 or 2 arguments",
                    preceded(ws0, char('(')),
                ),
                pair(
                    expect(
                        "invalid TIME call, expected a duration for the interval",
                        arithmetic::<TimeCallIntervalArgument>,
                    ),
                    opt(preceded(
                        preceded(ws0, char(',')),
                        preceded(ws0, arithmetic::<TimeCallOffsetArgument>),
                    )),
                ),
                expect("invalid TIME call, expected ')'", preceded(ws0, char(')'))),
            ),
        ),
        |(interval, offset)| Dimension::Time { interval, offset },
    )(i)
}

/// Parse a `GROUP BY` clause.
///
/// ```text
/// group_by_clause ::= dimension ( "," dimension )*
/// ```
fn group_by_clause(i: &str) -> ParseResult<&str, GroupByClause> {
    preceded(
        tuple((
            keyword("GROUP"),
            ws1,
            expect("invalid GROUP BY clause, expected BY", keyword("BY")),
            ws1,
        )),
        GroupByClause::separated_list1(
            "invalid GROUP BY clause, expected wildcard, TIME, identifier or regular expression",
        ),
    )(i)
}

/// Represents a `FILL` clause, and specifies all possible cases of the argument to the `FILL` clause.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum FillClause {
    /// Empty aggregate windows will contain null values and is specified as `fill(null)`
    Null,

    /// Empty aggregate windows will be discarded and is specified as `fill(none)`.
    None,

    /// Empty aggregate windows will be filled with the specified numerical value and is specified as
    /// `fill(<value>)`
    Value(Number),

    /// Empty aggregate windows will be filled with the value from the previous aggregate window
    /// and is specified as `fill(previous)`
    Previous,

    /// Empty aggregate windows will be filled with a value that is the linear interpolation of
    /// the prior two non-null window values and is specified as `fill(linear)`
    Linear,
}

impl Display for FillClause {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str("FILL(")?;
        match self {
            Self::Null => f.write_str("NULL")?,
            Self::None => f.write_str("NONE")?,
            Self::Value(v) => fmt::Display::fmt(v, f)?,
            Self::Previous => f.write_str("PREVIOUS")?,
            Self::Linear => f.write_str("LINEAR")?,
        }
        f.write_str(")")
    }
}

/// Represents an expression specified in the projection list of a `SELECT` statement.
#[derive(Debug, Clone, PartialEq)]
pub struct Field {
    /// The expression which represents the field projection.
    pub expr: Expr,

    /// An optional alias for the field projection.
    pub alias: Option<Identifier>,
}

impl Display for Field {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        Display::fmt(&self.expr, f)?;
        if let Some(alias) = &self.alias {
            write!(f, " AS {}", alias)?;
        }
        Ok(())
    }
}

impl Parser for Field {
    /// Parse a field expression that appears in the projection list of a `SELECT` clause.
    ///
    /// ```text
    /// field ::= field_expression ( "AS" identifier )?
    /// ```
    fn parse(i: &str) -> ParseResult<&str, Self> {
        map(
            pair(
                arithmetic::<FieldExpression>,
                opt(preceded(
                    preceded(ws0, keyword("AS")),
                    expect("invalid field alias, expected identifier", identifier),
                )),
            ),
            |(expr, alias)| Self { expr, alias },
        )(i)
    }
}

/// Parse a wildcard expression.
///
/// wildcard ::= "*" ( "::" ("field" | "tag")?
fn wildcard(i: &str) -> ParseResult<&str, Option<WildcardType>> {
    preceded(
        char('*'),
        opt(preceded(
            tag("::"),
            expect(
                "invalid wildcard type specifier, expected TAG or FIELD",
                alt((
                    value(WildcardType::Tag, keyword("TAG")),
                    value(WildcardType::Field, keyword("FIELD")),
                )),
            ),
        )),
    )(i)
}

/// Represents the field projection list of a `SELECT` statement.
pub type FieldList = ZeroOrMore<Field>;

impl Display for FieldList {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        if let Some(first) = self.head() {
            Display::fmt(first, f)?;
            for arg in self.tail() {
                write!(f, ", {}", arg)?;
            }
        }

        Ok(())
    }
}

/// Parse a field expression.
///
/// A field expression is an arithmetic expression accepting
/// a specific set of operands.
struct FieldExpression;

impl ArithmeticParsers for FieldExpression {
    fn operand(i: &str) -> ParseResult<&str, Expr> {
        preceded(
            ws0,
            alt((
                // distinct_expression ::= "DISTINCT" ws+ identifier
                map(
                    preceded(
                        pair(keyword("DISTINCT"), ws1),
                        expect(
                            "invalid DISTINCT expression, expected identifier",
                            identifier,
                        ),
                    ),
                    Expr::Distinct,
                ),
                // *
                map(wildcard, Wildcard),
                // Any literal
                map(literal, Expr::Literal),
                // A call expression
                call_expression::<Self>,
                // A tag or field reference
                var_ref,
                // A bind parameter
                map(parameter, Expr::BindParameter),
            )),
        )(i)
    }
}

/// Parse the projection list of a `SELECT` statement.
///
/// ```text
/// field_list ::= field ( "," field )*
/// ```
fn field_list(i: &str) -> ParseResult<&str, FieldList> {
    FieldList::separated_list1("invalid SELECT statement, expected field")(i)
}

/// Parse a `FILL(option)` clause.
///
/// ```text
/// fill_clause ::= "FILL" "(" fill_option ")"
/// fill_option ::= "NULL" | "NONE" | "PREVIOUS" | "LINEAR" | number
/// number      ::= signed_integer | signed_float
/// ```
fn fill_clause(i: &str) -> ParseResult<&str, FillClause> {
    preceded(
        keyword("FILL"),
        delimited(
            preceded(ws0, char('(')),
            expect(
                "invalid FILL option, expected NULL, NONE, PREVIOUS, LINEAR, or a number",
                preceded(
                    ws0,
                    alt((
                        value(FillClause::Null, keyword("NULL")),
                        value(FillClause::None, keyword("NONE")),
                        map(number, FillClause::Value),
                        value(FillClause::Previous, keyword("PREVIOUS")),
                        value(FillClause::Linear, keyword("LINEAR")),
                    )),
                ),
            ),
            preceded(ws0, char(')')),
        ),
    )(i)
}

/// Represents the value for a `SLIMIT` clause.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SLimitClause(pub(crate) u64);

impl_tuple_clause!(SLimitClause, u64);

impl Display for SLimitClause {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "SLIMIT {}", self.0)
    }
}

/// Parse a series limit (`SLIMIT <n>`) clause.
///
/// ```text
/// slimit_clause ::= "SLIMIT" unsigned_integer
/// ```
fn slimit_clause(i: &str) -> ParseResult<&str, SLimitClause> {
    preceded(
        pair(keyword("SLIMIT"), ws1),
        expect(
            "invalid SLIMIT clause, expected unsigned integer",
            map(unsigned_integer, SLimitClause),
        ),
    )(i)
}

/// Represents the value for a `SOFFSET` clause.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SOffsetClause(pub(crate) u64);

impl_tuple_clause!(SOffsetClause, u64);

impl Display for SOffsetClause {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "SOFFSET {}", self.0)
    }
}

/// Parse a series offset (`SOFFSET <n>`) clause.
///
/// ```text
/// soffset_clause ::= "SOFFSET" unsigned_integer
/// ```
fn soffset_clause(i: &str) -> ParseResult<&str, SOffsetClause> {
    preceded(
        pair(keyword("SOFFSET"), ws1),
        expect(
            "invalid SLIMIT clause, expected unsigned integer",
            map(unsigned_integer, SOffsetClause),
        ),
    )(i)
}

/// Represents the value of the time zone string of a `TZ` clause.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TimeZoneClause(pub(crate) String);

impl_tuple_clause!(TimeZoneClause, String);

impl Display for TimeZoneClause {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str("TZ('")?;
        write_escaped!(f, self.0, '\n' => "\\n", '\\' => "\\\\", '\'' => "\\'", '"' => "\\\"");
        f.write_str("')")
    }
}

/// Parse a timezone clause.
///
/// ```text
/// timezone_clause ::= "TZ" "(" single_quoted_string ")"
/// ```
fn timezone_clause(i: &str) -> ParseResult<&str, TimeZoneClause> {
    preceded(
        keyword("TZ"),
        delimited(
            preceded(ws0, char('(')),
            expect(
                "invalid TZ clause, expected string",
                preceded(ws0, map(single_quoted_string, TimeZoneClause)),
            ),
            preceded(ws0, char(')')),
        ),
    )(i)
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{assert_expect_error, binary_op, call, distinct, regex, var_ref, wildcard};
    use assert_matches::assert_matches;

    #[test]
    fn test_select_statement() {
        let (_, got) = select_statement("SELECT value FROM foo").unwrap();
        assert_eq!(format!("{}", got), "SELECT value FROM foo");

        let (_, got) =
            select_statement(r#"SELECT f1,/f2/, f3 AS "a field" FROM foo WHERE host =~ /c1/"#)
                .unwrap();
        assert_eq!(
            format!("{}", got),
            r#"SELECT f1, /f2/, f3 AS "a field" FROM foo WHERE host =~ /c1/"#
        );

        let (_, got) =
            select_statement("SELECT sum(value) FROM foo GROUP BY time(5m), host").unwrap();
        assert_eq!(
            format!("{}", got),
            r#"SELECT sum(value) FROM foo GROUP BY TIME(5m), host"#
        );

        // Parses TIME() with expressions
        let (_, got) =
            select_statement("SELECT sum(value) FROM foo GROUP BY time(5m * 10), host").unwrap();
        assert_eq!(
            format!("{}", got),
            r#"SELECT sum(value) FROM foo GROUP BY TIME(5m * 10), host"#
        );

        // TIME only supports integer and duration literals
        select_statement("SELECT sum(value) FROM foo GROUP BY time(5m + 'foo'), host").unwrap_err();
        select_statement("SELECT sum(value) FROM foo GROUP BY time(5m + 5.4), host").unwrap_err();
        select_statement("SELECT sum(value) FROM foo GROUP BY time(5m + true), host").unwrap_err();

        let (_, got) =
            select_statement("SELECT sum(value) FROM foo GROUP BY time(5m), host FILL(previous)")
                .unwrap();
        assert_eq!(
            format!("{}", got),
            r#"SELECT sum(value) FROM foo GROUP BY TIME(5m), host FILL(PREVIOUS)"#
        );

        let (_, got) = select_statement("SELECT value FROM foo ORDER BY DESC").unwrap();
        assert_eq!(
            format!("{}", got),
            r#"SELECT value FROM foo ORDER BY TIME DESC"#
        );

        let (_, got) = select_statement("SELECT value FROM foo ORDER BY TIME ASC").unwrap();
        assert_eq!(
            format!("{}", got),
            r#"SELECT value FROM foo ORDER BY TIME ASC"#
        );

        let (_, got) = select_statement("SELECT value FROM foo LIMIT 5").unwrap();
        assert_eq!(format!("{}", got), r#"SELECT value FROM foo LIMIT 5"#);

        let (_, got) = select_statement("SELECT value FROM foo OFFSET 20").unwrap();
        assert_eq!(format!("{}", got), r#"SELECT value FROM foo OFFSET 20"#);

        let (_, got) = select_statement("SELECT value FROM foo SLIMIT 25").unwrap();
        assert_eq!(format!("{}", got), r#"SELECT value FROM foo SLIMIT 25"#);

        let (_, got) = select_statement("SELECT value FROM foo SOFFSET 220").unwrap();
        assert_eq!(format!("{}", got), r#"SELECT value FROM foo SOFFSET 220"#);

        let (_, got) = select_statement("SELECT value FROM foo tz('Australia/Hobart')").unwrap();
        assert_eq!(
            format!("{}", got),
            r#"SELECT value FROM foo TZ('Australia/Hobart')"#
        );

        // validate spacing between keywords

        let (rem, _) = select_statement("SELECT value FROM(SELECT val FROM cpu)").unwrap();
        assert_eq!(rem, "");

        let (rem, _) = select_statement("SELECT (value)FROM cpu").unwrap();
        assert_eq!(rem, "");

        let (rem, _) =
            select_statement("SELECT value FROM (SELECT val FROM cpu)WHERE 1=1").unwrap();
        assert_eq!(rem, "");

        let (rem, _) =
            select_statement("SELECT value FROM cpu WHERE time <= now()FILL(previous)").unwrap();
        assert_eq!(rem, "");

        let (rem, _) =
            select_statement("SELECT value FROM cpu WHERE time <= now()ORDER BY time").unwrap();
        assert_eq!(rem, "");

        let (rem, _) =
            select_statement("SELECT value FROM cpu WHERE time <= now()LIMIT 10").unwrap();
        assert_eq!(rem, "");

        let (rem, _) =
            select_statement("SELECT value FROM cpu WHERE time <= now()OFFSET 10").unwrap();
        assert_eq!(rem, "");

        let (rem, _) =
            select_statement("SELECT value FROM cpu WHERE time <= now()SLIMIT 10").unwrap();
        assert_eq!(rem, "");

        let (rem, _) =
            select_statement("SELECT value FROM cpu WHERE time <= now()SOFFSET 10").unwrap();
        assert_eq!(rem, "");

        let (rem, _) =
            select_statement("SELECT value FROM cpu WHERE time <= now()TZ('Australia/Hobart')")
                .unwrap();
        assert_eq!(rem, "");

        // segmented var ref identifiers
        let (rem, _) =
            select_statement(r#"SELECT LAST("n.usage_user") FROM cpu WHERE n.usage_user > 0"#)
                .unwrap();
        assert_eq!(rem, "");
    }

    #[test]
    fn test_field() {
        // Parse a VarRef
        let (_, got) = Field::parse("foo").unwrap();
        assert_eq!(
            got,
            Field {
                expr: var_ref!("foo"),
                alias: None
            }
        );

        // Parse expression
        let (_, got) = Field::parse("foo + 1").unwrap();
        assert_eq!(
            got,
            Field {
                expr: binary_op!(var_ref!("foo"), Add, 1),
                alias: None
            }
        );

        // Parse a DISTINCT unary operator
        let (_, got) = Field::parse("distinct foo").unwrap();
        assert_eq!(
            got,
            Field {
                expr: distinct!("foo"),
                alias: None
            }
        );

        // Parse a VarRef with an alias
        let (_, got) = Field::parse("foo AS bar").unwrap();
        assert_eq!(
            got,
            Field {
                expr: var_ref!("foo"),
                alias: Some("bar".into())
            }
        );

        // Parse expression with an alias using lowercase AS token
        let (_, got) = Field::parse("foo + 1 as bar").unwrap();
        assert_eq!(
            got,
            Field {
                expr: binary_op!(var_ref!("foo"), Add, 1),
                alias: Some("bar".into())
            }
        );

        // Parse a distinct VarRef with an alias
        let (_, got) = Field::parse("DISTINCT foo AS bar").unwrap();
        assert_eq!(
            got,
            Field {
                expr: distinct!("foo"),
                alias: Some("bar".into())
            }
        );

        // Parse expression with an alias and no unnecessary whitespace
        let (_, got) = Field::parse("COUNT(foo)AS bar").unwrap();
        assert_eq!(
            got,
            Field {
                expr: call!("COUNT", var_ref!("foo")),
                alias: Some("bar".into())
            }
        );

        // Parse expression with an alias and no unnecessary whitespace
        let (_, got) = Field::parse("LAST(\"n.asks\")").unwrap();
        assert_eq!(
            got,
            Field {
                expr: call!("LAST", var_ref!("n.asks")),
                alias: None
            }
        );

        // Parse a call with a VarRef
        let (_, got) = Field::parse("DISTINCT foo AS bar").unwrap();
        assert_eq!(
            got,
            Field {
                expr: distinct!("foo"),
                alias: Some("bar".into())
            }
        );

        // Parse a call with a VarRef
        let (_, got) = Field::parse("COUNT(DISTINCT foo) AS bar").unwrap();
        assert_eq!(
            got,
            Field {
                expr: call!("COUNT", distinct!("foo")),
                alias: Some("bar".into())
            }
        );

        // Parse a call with a nested distinct call
        let (_, got) = Field::parse("COUNT(DISTINCT(foo))").unwrap();
        assert_eq!(
            got,
            Field {
                expr: call!("COUNT", call!("DISTINCT", var_ref!("foo"))),
                alias: None
            }
        );

        // Parse a wildcard
        let (_, got) = Field::parse("*").unwrap();
        assert_eq!(
            got,
            Field {
                expr: wildcard!(),
                alias: None,
            }
        );

        // Parse a wildcard with a data type
        let (_, got) = Field::parse("*::tag").unwrap();
        assert_eq!(
            got,
            Field {
                expr: wildcard!(tag),
                alias: None,
            }
        );

        // Parse a wildcard with a data type and an alias
        let (_, got) = Field::parse("*::field as foo").unwrap();
        assert_eq!(
            got,
            Field {
                expr: wildcard!(field),
                alias: Some("foo".into()),
            }
        );

        // Parse a call with a wildcard
        let (_, got) = Field::parse("COUNT(*)").unwrap();
        assert_eq!(
            got,
            Field {
                expr: call!("COUNT", wildcard!()),
                alias: None,
            }
        );

        // Regex
        let (_, got) = Field::parse("/foo/").unwrap();
        assert_eq!(
            got,
            Field {
                expr: regex!("foo"),
                alias: None,
            }
        );

        // Fallible cases
        assert_expect_error!(
            Field::parse("distinct *"),
            "invalid DISTINCT expression, expected identifier"
        );
        assert_expect_error!(
            Field::parse("foo as 1"),
            "invalid field alias, expected identifier"
        );
    }

    impl Field {
        fn new(expr: Expr) -> Self {
            Self { expr, alias: None }
        }

        fn new_alias(expr: Expr, alias: Identifier) -> Self {
            Self {
                expr,
                alias: Some(alias),
            }
        }
    }

    #[test]
    fn test_field_list() {
        // Single field
        let (_, got) = field_list("foo").unwrap();
        assert_eq!(got, FieldList::new(vec![Field::new(var_ref!("foo"))]));

        // Many fields
        let (_, got) = field_list("foo, bar AS foobar").unwrap();
        assert_eq!(
            got,
            FieldList::new(vec![
                Field::new(var_ref!("foo")),
                Field::new_alias(var_ref!("bar"), "foobar".into())
            ])
        );

        // Fallible cases

        // Unable to parse any valid fields
        assert_expect_error!(field_list("."), "invalid SELECT statement, expected field");
    }

    #[test]
    fn test_measurement_selection() {
        // measurement name expression
        let (_, got) = MeasurementSelection::parse("diskio").unwrap();
        assert_matches!(got, MeasurementSelection::Name(_));

        let (_, got) = MeasurementSelection::parse("/regex/").unwrap();
        assert_matches!(got, MeasurementSelection::Name(_));

        let (_, got) = MeasurementSelection::parse("(SELECT foo FROM bar)").unwrap();
        assert_matches!(got, MeasurementSelection::Subquery(_));
    }

    #[test]
    fn test_from_clause() {
        // Single, exact-match measurement source
        let (got, _) = from_clause("FROM diskio").unwrap();
        // Validate we consumed all input, which is a successful result
        assert_eq!(got, "");

        // Single, regex measurement source
        let (got, _) = from_clause("FROM /^c/").unwrap();
        // Validate we consumed all input
        assert_eq!(got, "");

        // Single, subquery measurement source
        let (got, _) = from_clause("FROM (SELECT value FROM cpu)").unwrap();
        // Validate we consumed all input
        assert_eq!(got, "");

        // Multiple measurement sources with lots of unnecessary whitespace
        let (got, _) = from_clause("FROM  ( select *  from  cpu    ),\n/cpu/,diskio").unwrap();
        assert_eq!(got, "");

        // Can use keyword in quotes
        let (got, _) = from_clause("FROM \"where\"").unwrap();
        assert_eq!(got, "");

        // Fallible cases

        assert_expect_error!(
            from_clause("FROM"),
            "invalid FROM clause, expected identifier, regular expression or subquery"
        );
        assert_expect_error!(
            from_clause("FROM 1"),
            "invalid FROM clause, expected identifier, regular expression or subquery"
        );
        assert_expect_error!(
            from_clause("FROM (foo)"),
            "invalid FROM clause, expected identifier, regular expression or subquery"
        );
        assert_expect_error!(
            from_clause("FROM WHERE"),
            "invalid FROM clause, expected identifier, regular expression or subquery"
        );
    }

    #[test]
    fn test_dimension() {
        // Test the valid dimension expressions for a GROUP BY clause

        let (_, got) = Dimension::parse("*").unwrap();
        assert_matches!(got, Dimension::Wildcard);

        let (_, got) = Dimension::parse("TIME(5m)").unwrap();
        // TIME parsing is validated with test_time_call_expression, so we just
        // validate that we matched a Time case.
        assert_matches!(got, Dimension::Time { .. });

        let (_, got) = Dimension::parse("foo").unwrap();
        assert_matches!(got, Dimension::Tag(t) if t == "foo".into());

        let (_, got) = Dimension::parse("/bar/").unwrap();
        assert_matches!(got, Dimension::Regex(_));
    }

    #[test]
    fn test_group_by_clause() {
        let (got, _) = group_by_clause("GROUP BY time(1m)").unwrap();
        // Validate we consumed all input, which is a successful result
        assert_eq!(got, "");

        let (got, _) = group_by_clause("GROUP BY foo").unwrap();
        assert_eq!(got, "");

        let (got, _) = group_by_clause("GROUP BY *").unwrap();
        assert_eq!(got, "");

        let (got, _) = group_by_clause("GROUP BY *::tag").unwrap();
        assert_eq!(got, "");

        let (got, _) = group_by_clause("GROUP BY /foo/").unwrap();
        assert_eq!(got, "");

        let (got, _) = group_by_clause("GROUP BY time(5m), foo").unwrap();
        assert_eq!(got, "");

        let (got, _) = group_by_clause("GROUP BY time(5m), /foo/, *").unwrap();
        assert_eq!(got, "");

        // Fallible cases

        assert_expect_error!(
            group_by_clause("GROUP time(5m)"),
            "invalid GROUP BY clause, expected BY"
        );

        assert_expect_error!(
            group_by_clause("GROUP BY 1"),
            "invalid GROUP BY clause, expected wildcard, TIME, identifier or regular expression"
        );
    }

    #[test]
    fn test_time_call_expression() {
        let (got, _) = time_call_expression("TIME(5m)").unwrap();
        assert_eq!(got, "");

        let (got, _) = time_call_expression("TIME(5m , 1m)").unwrap();
        assert_eq!(got, "");

        let (got, _) = time_call_expression("TIME(5m3s)").unwrap();
        assert_eq!(got, "");

        let (got, _) = time_call_expression("TIME(5m + 3s)").unwrap();
        assert_eq!(got, "");

        let (got, _) = time_call_expression("TIME(5m, now())").unwrap();
        assert_eq!(got, "");

        // Strings are later evaluated to be datetime-like:
        // https://github.com/influxdata/influxql/blob/1ba470371ec093d57a726b143fe6ccbacf1b452b/ast.go#L3660-L3676
        let (got, _) = time_call_expression("TIME(5m, 'some string')").unwrap();
        assert_eq!(got, "");

        // Limited expressions are supported
        let (got, _) = time_call_expression("TIME(5m * 10)").unwrap();
        assert_eq!(got, "");

        // Fallible cases
        assert_expect_error!(
            time_call_expression("TIME"),
            "invalid TIME call, expected 1 or 2 arguments"
        );
        assert_expect_error!(
            time_call_expression("TIME(5m"),
            "invalid TIME call, expected ')'"
        );

        // The offset argument parser does not recognise the 3, therefore it results
        // in attempting to parse a `)`, and fails.
        assert_expect_error!(
            time_call_expression("TIME(5m, 3)"),
            "invalid TIME call, expected ')'"
        );
    }

    #[test]
    fn test_fill_clause() {
        let (_, got) = fill_clause("FILL(null)").unwrap();
        assert_matches!(got, FillClause::Null);

        let (_, got) = fill_clause("FILL(NONE)").unwrap();
        assert_matches!(got, FillClause::None);

        let (_, got) = fill_clause("FILL(53)").unwrap();
        assert_matches!(got, FillClause::Value(v) if v == 53.into());

        let (_, got) = fill_clause("FILL(-18.9)").unwrap();
        assert_matches!(got, FillClause::Value(v) if v == (-18.9).into());

        let (_, got) = fill_clause("FILL(previous)").unwrap();
        assert_matches!(got, FillClause::Previous);

        let (_, got) = fill_clause("FILL(linear)").unwrap();
        assert_matches!(got, FillClause::Linear);

        // unnecessary whitespace
        let (_, got) = fill_clause("FILL ( null )").unwrap();
        assert_matches!(got, FillClause::Null);

        // Fallible cases

        assert_expect_error!(
            fill_clause("FILL(foo)"),
            "invalid FILL option, expected NULL, NONE, PREVIOUS, LINEAR, or a number"
        );
    }

    #[test]
    fn test_timezone_clause() {
        let (_, got) = timezone_clause("TZ('Australia/Hobart')").unwrap();
        assert_eq!(*got, "Australia/Hobart");

        // Fallible cases
        assert_expect_error!(
            timezone_clause("TZ(foo)"),
            "invalid TZ clause, expected string"
        );
    }

    #[test]
    fn test_wildcard() {
        let (_, got) = wildcard("*").unwrap();
        assert_matches!(got, None);
        let (_, got) = wildcard("*::tag").unwrap();
        assert_matches!(got, Some(v) if v == WildcardType::Tag);
        let (_, got) = wildcard("*::field").unwrap();
        assert_matches!(got, Some(v) if v == WildcardType::Field);

        // Fallible cases

        assert_expect_error!(
            wildcard("*::foo"),
            "invalid wildcard type specifier, expected TAG or FIELD"
        );
    }
}
