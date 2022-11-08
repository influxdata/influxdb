use crate::common::ws0;
use crate::expression::arithmetic::{
    arithmetic, call_expression, var_ref, ArithmeticParsers, Expr,
};
use crate::internal::{expect, verify, ParseResult};
use crate::keywords::keyword;
use crate::literal::{literal_no_regex, literal_regex, Literal};
use crate::parameter::parameter;
use nom::branch::alt;
use nom::bytes::complete::tag;
use nom::character::complete::char;
use nom::combinator::{map, value};
use nom::multi::many0;
use nom::sequence::{delimited, preceded, tuple};
use std::fmt;
use std::fmt::{Display, Formatter, Write};

/// Represents on of the conditional operators supported by [`ConditionalExpression::Binary`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConditionalOperator {
    /// Represents the `=` operator.
    Eq,
    /// Represents the `!=` or `<>` operator.
    NotEq,
    /// Represents the `=~` (regular expression equals) operator.
    EqRegex,
    /// Represents the `!~` (regular expression not equals) operator.
    NotEqRegex,
    /// Represents the `<` operator.
    Lt,
    /// Represents the `<=` operator.
    LtEq,
    /// Represents the `>` operator.
    Gt,
    /// Represents the `>=` operator.
    GtEq,
    /// Represents the `IN` operator.
    In,
    /// Represents the `AND` operator.
    And,
    /// Represents the `OR` operator.
    Or,
}

impl Display for ConditionalOperator {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Eq => f.write_char('='),
            Self::NotEq => f.write_str("!="),
            Self::EqRegex => f.write_str("=~"),
            Self::NotEqRegex => f.write_str("!~"),
            Self::Lt => f.write_char('<'),
            Self::LtEq => f.write_str("<="),
            Self::Gt => f.write_char('>'),
            Self::GtEq => f.write_str(">="),
            Self::In => f.write_str("IN"),
            Self::And => f.write_str("AND"),
            Self::Or => f.write_str("OR"),
        }
    }
}

/// Represents a conditional expression.
#[derive(Debug, Clone, PartialEq)]
pub enum ConditionalExpression {
    /// Represents an arithmetic expression.
    Expr(Box<Expr>),

    /// Binary operations, such as `foo = 'bar'` or `true AND false`.
    Binary {
        /// Represents the left-hand side of the conditional binary expression.
        lhs: Box<ConditionalExpression>,
        /// Represents the operator to apply to the conditional binary expression.
        op: ConditionalOperator,
        /// Represents the right-hand side of the conditional binary expression.
        rhs: Box<ConditionalExpression>,
    },

    /// Represents a conditional expression enclosed in parenthesis.
    Grouped(Box<ConditionalExpression>),
}

impl Display for ConditionalExpression {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Expr(v) => fmt::Display::fmt(v, f),
            Self::Binary { lhs, op, rhs } => write!(f, "{} {} {}", lhs, op, rhs),
            Self::Grouped(v) => write!(f, "({})", v),
        }
    }
}

impl From<Literal> for ConditionalExpression {
    fn from(v: Literal) -> Self {
        Self::Expr(Box::new(Expr::Literal(v)))
    }
}

/// Parse a parenthesis expression.
fn parens(i: &str) -> ParseResult<&str, ConditionalExpression> {
    delimited(
        preceded(ws0, char('(')),
        map(conditional_expression, |e| {
            ConditionalExpression::Grouped(e.into())
        }),
        preceded(ws0, char(')')),
    )(i)
}

fn expr_or_group(i: &str) -> ParseResult<&str, ConditionalExpression> {
    alt((
        map(arithmetic_expression, |v| {
            ConditionalExpression::Expr(Box::new(v))
        }),
        parens,
    ))(i)
}

/// Parse the conditional regular expression operators `=~` and `!~`.
fn conditional_regex(i: &str) -> ParseResult<&str, ConditionalExpression> {
    let (input, f1) = expr_or_group(i)?;
    let (input, exprs) = many0(tuple((
        preceded(
            ws0,
            alt((
                value(ConditionalOperator::EqRegex, tag("=~")),
                value(ConditionalOperator::NotEqRegex, tag("!~")),
            )),
        ),
        map(
            expect(
                "invalid conditional, expected regular expression",
                preceded(ws0, literal_regex),
            ),
            From::from,
        ),
    )))(input)?;
    Ok((input, reduce_expr(f1, exprs)))
}

/// Parse conditional operators.
fn conditional(i: &str) -> ParseResult<&str, ConditionalExpression> {
    let (input, f1) = conditional_regex(i)?;
    let (input, exprs) = many0(tuple((
        preceded(
            ws0,
            alt((
                // try longest matches first
                value(ConditionalOperator::LtEq, tag("<=")),
                value(ConditionalOperator::GtEq, tag(">=")),
                value(ConditionalOperator::NotEq, tag("!=")),
                value(ConditionalOperator::NotEq, tag("<>")),
                value(ConditionalOperator::Lt, char('<')),
                value(ConditionalOperator::Gt, char('>')),
                value(ConditionalOperator::Eq, char('=')),
            )),
        ),
        expect("invalid conditional expression", conditional_regex),
    )))(input)?;
    Ok((input, reduce_expr(f1, exprs)))
}

/// Parse conjunction operators, such as `AND`.
fn conjunction(i: &str) -> ParseResult<&str, ConditionalExpression> {
    let (input, f1) = conditional(i)?;
    let (input, exprs) = many0(tuple((
        value(ConditionalOperator::And, preceded(ws0, keyword("AND"))),
        expect("invalid conditional expression", conditional),
    )))(input)?;
    Ok((input, reduce_expr(f1, exprs)))
}

/// Parse disjunction operator, such as `OR`.
fn disjunction(i: &str) -> ParseResult<&str, ConditionalExpression> {
    let (input, f1) = conjunction(i)?;
    let (input, exprs) = many0(tuple((
        value(ConditionalOperator::Or, preceded(ws0, keyword("OR"))),
        expect("invalid conditional expression", conjunction),
    )))(input)?;
    Ok((input, reduce_expr(f1, exprs)))
}

/// Parse an InfluxQL conditional expression.
pub(crate) fn conditional_expression(i: &str) -> ParseResult<&str, ConditionalExpression> {
    disjunction(i)
}

/// Folds `expr` and `remainder` into a [ConditionalExpression::Binary] tree.
fn reduce_expr(
    expr: ConditionalExpression,
    remainder: Vec<(ConditionalOperator, ConditionalExpression)>,
) -> ConditionalExpression {
    remainder
        .into_iter()
        .fold(expr, |lhs, val| ConditionalExpression::Binary {
            lhs: lhs.into(),
            op: val.0,
            rhs: val.1.into(),
        })
}

/// Returns true if `expr` is a valid [`Expr::Call`] expression for the `now` function.
pub(crate) fn is_valid_now_call(expr: &Expr) -> bool {
    match expr {
        Expr::Call { name, args } => name.to_lowercase() == "now" && args.is_empty(),
        _ => false,
    }
}

impl ConditionalExpression {
    /// Parse the `now()` function call
    fn call(i: &str) -> ParseResult<&str, Expr> {
        verify(
            "invalid expression, the only valid function call is 'now' with no arguments",
            call_expression::<Self>,
            is_valid_now_call,
        )(i)
    }
}

impl ArithmeticParsers for ConditionalExpression {
    fn operand(i: &str) -> ParseResult<&str, Expr> {
        preceded(
            ws0,
            alt((
                map(literal_no_regex, Expr::Literal),
                Self::call,
                var_ref,
                map(parameter, Expr::BindParameter),
            )),
        )(i)
    }
}

/// Parse an arithmetic expression used by conditional expressions.
fn arithmetic_expression(i: &str) -> ParseResult<&str, Expr> {
    arithmetic::<ConditionalExpression>(i)
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::expression::arithmetic::Expr;
    use crate::{
        assert_expect_error, assert_failure, binary_op, call, cond_op, grouped, regex, unary,
        var_ref,
    };

    impl From<Expr> for ConditionalExpression {
        fn from(v: Expr) -> Self {
            Self::Expr(Box::new(v))
        }
    }

    impl From<u64> for Box<ConditionalExpression> {
        fn from(v: u64) -> Self {
            Self::new(ConditionalExpression::Expr(Box::new(Expr::Literal(
                v.into(),
            ))))
        }
    }

    impl From<Expr> for Box<ConditionalExpression> {
        fn from(v: Expr) -> Self {
            Self::new(ConditionalExpression::Expr(v.into()))
        }
    }

    impl From<Box<Expr>> for Box<ConditionalExpression> {
        fn from(v: Box<Expr>) -> Self {
            Self::new(ConditionalExpression::Expr(v))
        }
    }

    #[test]
    fn test_arithmetic_expression() {
        // now() function call is permitted
        let (_, got) = arithmetic_expression("now() + 3").unwrap();
        assert_eq!(got, binary_op!(call!("now"), Add, 3));

        // Fallible cases

        assert_expect_error!(
            arithmetic_expression("sum(foo)"),
            "invalid expression, the only valid function call is 'now' with no arguments"
        );

        assert_expect_error!(
            arithmetic_expression("now(1)"),
            "invalid expression, the only valid function call is 'now' with no arguments"
        );
    }

    #[test]
    fn test_conditional_expression() {
        let (_, got) = conditional_expression("foo = 5").unwrap();
        assert_eq!(got, *cond_op!(var_ref!("foo"), Eq, 5));

        let (_, got) = conditional_expression("foo != 5").unwrap();
        assert_eq!(got, *cond_op!(var_ref!("foo"), NotEq, 5));

        let (_, got) = conditional_expression("foo > 5").unwrap();
        assert_eq!(got, *cond_op!(var_ref!("foo"), Gt, 5));

        let (_, got) = conditional_expression("foo >= 5").unwrap();
        assert_eq!(got, *cond_op!(var_ref!("foo"), GtEq, 5));

        let (_, got) = conditional_expression("foo < 5").unwrap();
        assert_eq!(got, *cond_op!(var_ref!("foo"), Lt, 5));

        let (_, got) = conditional_expression("foo <= 5").unwrap();
        assert_eq!(got, *cond_op!(var_ref!("foo"), LtEq, 5));

        let (_, got) = conditional_expression("foo > 5 + 6 ").unwrap();
        assert_eq!(got, *cond_op!(var_ref!("foo"), Gt, binary_op!(5, Add, 6)));

        let (_, got) = conditional_expression("5 <= -6").unwrap();
        assert_eq!(got, *cond_op!(5, LtEq, unary!(-6)));

        // simple expressions
        let (_, got) = conditional_expression("true").unwrap();
        assert_eq!(
            got,
            ConditionalExpression::Expr(Box::new(Expr::Literal(true.into())))
        );

        // Expressions are still valid when whitespace is omitted

        let (_, got) = conditional_expression("foo>5+6 ").unwrap();
        assert_eq!(got, *cond_op!(var_ref!("foo"), Gt, binary_op!(5, Add, 6)));

        let (_, got) = conditional_expression("5<=-6").unwrap();
        assert_eq!(got, *cond_op!(5, LtEq, unary!(-6)));

        // var refs with cast operator
        let (_, got) = conditional_expression("foo::integer = 5").unwrap();
        assert_eq!(got, *cond_op!(var_ref!("foo", Integer), Eq, 5));

        // Fallible cases

        // conditional expression must be complete
        assert_failure!(conditional_expression("5 <="));

        // should not accept a regex literal
        assert_failure!(conditional_expression("5 = /regex/"));
    }

    #[test]
    fn test_logical_expression() {
        let (_, got) = conditional_expression("5 AND 6").unwrap();
        assert_eq!(got, *cond_op!(5, And, 6));

        let (_, got) = conditional_expression("5 AND 6 OR 7").unwrap();
        assert_eq!(got, *cond_op!(cond_op!(5, And, 6), Or, 7));

        let (_, got) = conditional_expression("5 > 3 OR 6 = 7 AND 7 != 1").unwrap();
        assert_eq!(
            got,
            *cond_op!(
                cond_op!(5, Gt, 3),
                Or,
                cond_op!(cond_op!(6, Eq, 7), And, cond_op!(7, NotEq, 1))
            )
        );

        let (_, got) = conditional_expression("5 AND (6 OR 7)").unwrap();
        assert_eq!(got, *cond_op!(5, And, grouped!(cond_op!(6, Or, 7))));

        // <> is recognised as !=
        let (_, got) = conditional_expression("5 <> 6").unwrap();
        assert_eq!(got, *cond_op!(5, NotEq, 6));

        // In the following cases, we validate that the `OR` keyword is not eagerly
        // parsed from substrings
        let (got, _) = conditional_expression("foo = bar ORDER BY time ASC").unwrap();
        assert_eq!(got, " ORDER BY time ASC");

        let (got, _) = conditional_expression("foo = bar OR1").unwrap();
        assert_eq!(got, " OR1");

        // Whitespace is optional for certain characters
        let (got, _) = conditional_expression("foo = bar OR(foo > bar) ORDER BY time ASC").unwrap();
        assert_eq!(got, " ORDER BY time ASC");

        // Fallible cases

        // Expects Expr after operator
        assert_failure!(conditional_expression("5 OR -"));
        assert_failure!(conditional_expression("5 OR"));
        assert_failure!(conditional_expression("5 AND"));

        // Can't use "and" as identifier
        assert_failure!(conditional_expression("5 AND and OR 5"));
    }

    #[test]
    fn test_regex() {
        let (_, got) = conditional_expression("foo =~ /(a > b)/").unwrap();
        assert_eq!(got, *cond_op!(var_ref!("foo"), EqRegex, regex!("(a > b)")));

        let (_, got) = conditional_expression("foo !~ /bar/").unwrap();
        assert_eq!(got, *cond_op!(var_ref!("foo"), NotEqRegex, regex!("bar")));

        // Expressions are still valid when whitespace is omitted

        let (_, got) = conditional_expression("foo=~/(a > b)/").unwrap();
        assert_eq!(got, *cond_op!(var_ref!("foo"), EqRegex, regex!("(a > b)")));

        // Fallible cases

        // Expects a regex literal after regex conditional operators
        assert_expect_error!(
            conditional_expression("foo =~ 5"),
            "invalid conditional, expected regular expression"
        );
        assert_expect_error!(
            conditional_expression("foo !~ 5"),
            "invalid conditional, expected regular expression"
        );
    }

    #[test]
    fn test_display_expr() {
        let (_, e) = conditional_expression("foo = 'test'").unwrap();
        let got = format!("{}", e);
        assert_eq!(got, "foo = 'test'");
    }
}
