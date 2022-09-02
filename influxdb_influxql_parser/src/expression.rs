#![allow(dead_code)]

use crate::literal::literal_regex;
use crate::{
    identifier::{identifier, Identifier},
    literal::{literal, Literal},
    parameter::{parameter, BindParameter},
};
use nom::branch::alt;
use nom::bytes::complete::{tag, tag_no_case};
use nom::character::complete::{char, multispace0};
use nom::combinator::{cut, map, value};
use nom::multi::many0;
use nom::sequence::{delimited, preceded, tuple};
use nom::IResult;
use std::fmt::{Display, Formatter, Write};

/// An InfluxQL expression of any type.
#[derive(Clone, Debug, PartialEq)]
pub enum Expr {
    /// Identifier name, such as a tag or field key
    Identifier(Identifier),

    /// BindParameter identifier
    BindParameter(BindParameter),

    /// Literal value such as 'foo', 5 or /^(a|b)$/
    Literal(Literal),

    /// Unary operation such as + 5 or - 1h3m
    UnaryOp(UnaryOperator, Box<Expr>),

    /// Binary operations, such as the
    /// conditional foo = 'bar' or the arithmetic 1 + 2 expressions.
    BinaryOp {
        lhs: Box<Expr>,
        op: BinaryOperator,
        rhs: Box<Expr>,
    },

    /// Nested expression, such as (foo = 'bar') or (1)
    Nested(Box<Expr>),
}

impl From<Literal> for Expr {
    fn from(v: Literal) -> Self {
        Self::Literal(v)
    }
}

impl From<u64> for Expr {
    fn from(v: u64) -> Self {
        Self::Literal(v.into())
    }
}

impl From<f64> for Expr {
    fn from(v: f64) -> Self {
        Self::Literal(v.into())
    }
}

impl From<u64> for Box<Expr> {
    fn from(v: u64) -> Self {
        Self::new(v.into())
    }
}

impl Display for Expr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Identifier(v) => write!(f, "{}", v)?,
            Self::BindParameter(v) => write!(f, "{}", v)?,
            Self::Literal(v) => write!(f, "{}", v)?,
            Self::UnaryOp(op, e) => write!(f, "{}{}", op, e)?,
            Self::BinaryOp { lhs, op, rhs } => write!(f, "{} {} {}", lhs, op, rhs)?,
            Self::Nested(e) => write!(f, "({})", e)?,
        }

        Ok(())
    }
}

/// An InfluxQL unary operator.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnaryOperator {
    Plus,
    Minus,
}

impl Display for UnaryOperator {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Plus => f.write_char('+')?,
            Self::Minus => f.write_char('-')?,
        }

        Ok(())
    }
}

/// An InfluxQL binary operators.
#[derive(Clone, Debug, Copy, PartialEq, Eq)]
pub enum BinaryOperator {
    Add,        // +
    Sub,        // -
    Mul,        // *
    Div,        // /
    Mod,        // %
    BitwiseAnd, // &
    BitwiseOr,  // |
    BitwiseXor, // ^
    Eq,         // =
    NotEq,      // !=
    EqRegex,    // =~
    NotEqRegex, // !~
    Lt,         // <
    LtEq,       // <=
    Gt,         // >
    GtEq,       // >=
    In,         // IN
    And,        // AND
    Or,         // OR
}

impl Display for BinaryOperator {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Add => f.write_char('+')?,
            Self::Sub => f.write_char('-')?,
            Self::Mul => f.write_char('*')?,
            Self::Div => f.write_char('/')?,
            Self::Mod => f.write_char('%')?,
            Self::BitwiseAnd => f.write_char('&')?,
            Self::BitwiseOr => f.write_char('|')?,
            Self::BitwiseXor => f.write_char('^')?,
            Self::Eq => f.write_char('=')?,
            Self::NotEq => f.write_str("!=")?,
            Self::EqRegex => f.write_str("=~")?,
            Self::NotEqRegex => f.write_str("!~")?,
            Self::Lt => f.write_char('<')?,
            Self::LtEq => f.write_str("<=")?,
            Self::Gt => f.write_char('>')?,
            Self::GtEq => f.write_str(">=")?,
            Self::In => f.write_str("IN")?,
            Self::And => f.write_str("AND")?,
            Self::Or => f.write_str("OR")?,
        }

        Ok(())
    }
}

/// Parse a unary expression.
fn unary(i: &str) -> IResult<&str, Expr> {
    let (i, op) = preceded(
        multispace0,
        alt((
            value(UnaryOperator::Plus, char('+')),
            value(UnaryOperator::Minus, char('-')),
        )),
    )(i)?;

    let (i, e) = factor(i)?;

    Ok((i, Expr::UnaryOp(op, e.into())))
}

/// Parse a parenthesis expression.
fn parens(i: &str) -> IResult<&str, Expr> {
    delimited(
        preceded(multispace0, char('(')),
        map(conditional_expression, |e| Expr::Nested(e.into())),
        preceded(multispace0, char(')')),
    )(i)
}

/// Parse an operand expression, such as a literal, identifier or bind parameter.
fn operand(i: &str) -> IResult<&str, Expr> {
    preceded(
        multispace0,
        alt((
            map(literal, Expr::Literal),
            map(identifier, Expr::Identifier),
            map(parameter, Expr::BindParameter),
        )),
    )(i)
}

/// Parse precedence priority 1 operators.
///
/// These are the highest precedence operators, and include parenthesis and the unary operators.
fn factor(i: &str) -> IResult<&str, Expr> {
    alt((unary, parens, operand))(i)
}

/// Parse arithmetic, precedence priority 2 operators.
///
/// This includes the multiplication, division, bitwise and, and modulus operators.
fn term(i: &str) -> IResult<&str, Expr> {
    let (input, left) = factor(i)?;
    let (input, remaining) = many0(tuple((
        preceded(
            multispace0,
            alt((
                value(BinaryOperator::Mul, char('*')),
                value(BinaryOperator::Div, char('/')),
                value(BinaryOperator::BitwiseAnd, char('&')),
                value(BinaryOperator::Mod, char('%')),
            )),
        ),
        factor,
    )))(input)?;
    Ok((input, reduce_expr(left, remaining)))
}

/// Parse arithmetic, precedence priority 3 operators.
///
/// This includes the addition, subtraction, bitwise or, and bitwise xor operators.
fn arithmetic(i: &str) -> IResult<&str, Expr> {
    let (input, left) = term(i)?;
    let (input, remaining) = many0(tuple((
        preceded(
            multispace0,
            alt((
                value(BinaryOperator::Add, char('+')),
                value(BinaryOperator::Sub, char('-')),
                value(BinaryOperator::BitwiseOr, char('|')),
                value(BinaryOperator::BitwiseXor, char('^')),
            )),
        ),
        cut(term),
    )))(input)?;
    Ok((input, reduce_expr(left, remaining)))
}

/// Parse the conditional regular expression operators `=~` and `!~`.
fn conditional_regex(i: &str) -> IResult<&str, Expr> {
    let (input, f1) = arithmetic(i)?;
    let (input, exprs) = many0(tuple((
        preceded(
            multispace0,
            alt((
                value(BinaryOperator::EqRegex, tag("=~")),
                value(BinaryOperator::NotEqRegex, tag("!~")),
            )),
        ),
        map(cut(preceded(multispace0, literal_regex)), From::from),
    )))(input)?;
    Ok((input, reduce_expr(f1, exprs)))
}

/// Parse conditional operators.
fn conditional(i: &str) -> IResult<&str, Expr> {
    let (input, f1) = conditional_regex(i)?;
    let (input, exprs) = many0(tuple((
        preceded(
            multispace0,
            alt((
                // try longest matches first
                value(BinaryOperator::LtEq, tag("<=")),
                value(BinaryOperator::GtEq, tag(">=")),
                value(BinaryOperator::NotEq, tag("!=")),
                value(BinaryOperator::Lt, char('<')),
                value(BinaryOperator::Gt, char('>')),
                value(BinaryOperator::Eq, char('=')),
            )),
        ),
        cut(conditional_regex),
    )))(input)?;
    Ok((input, reduce_expr(f1, exprs)))
}

/// Parse conjunction operators, such as `AND`.
fn conjunction(i: &str) -> IResult<&str, Expr> {
    let (input, f1) = conditional(i)?;
    let (input, exprs) = many0(tuple((
        value(
            BinaryOperator::And,
            preceded(multispace0, tag_no_case("and")),
        ),
        cut(conditional),
    )))(input)?;
    Ok((input, reduce_expr(f1, exprs)))
}

/// Parse disjunction operator, such as `OR`.
fn disjunction(i: &str) -> IResult<&str, Expr> {
    let (input, f1) = conjunction(i)?;
    let (input, exprs) = many0(tuple((
        value(BinaryOperator::Or, preceded(multispace0, tag_no_case("or"))),
        cut(conjunction),
    )))(input)?;
    Ok((input, reduce_expr(f1, exprs)))
}

/// Parse an InfluxQL conditional expression.
pub fn conditional_expression(i: &str) -> IResult<&str, Expr> {
    disjunction(i)
}

/// Folds `expr` and `remainder` into a [Expr::BinaryOp] tree.
fn reduce_expr(expr: Expr, remainder: Vec<(BinaryOperator, Expr)>) -> Expr {
    remainder.into_iter().fold(expr, |lhs, val| Expr::BinaryOp {
        lhs: lhs.into(),
        op: val.0,
        rhs: val.1.into(),
    })
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::assert_failure;

    /// Constructs an [Expr::Identifier] expression.
    macro_rules! ident {
        ($EXPR: expr) => {
            Expr::Identifier(crate::identifier::Identifier::Unquoted($EXPR.into()))
        };
    }

    /// Constructs a regular expression [Expr::Literal].
    macro_rules! regex {
        ($EXPR: expr) => {
            Expr::Literal(crate::literal::Literal::Regex($EXPR.into()).into())
        };
    }

    /// Constructs a [Expr::BindParameter] expression.
    macro_rules! param {
        ($EXPR: expr) => {
            Expr::BindParameter(crate::parameter::BindParameter::Unquoted($EXPR.into()).into())
        };
    }

    /// Constructs a [Expr::Nested] expression.
    macro_rules! nested {
        ($EXPR: expr) => {
            <Expr as std::convert::Into<Box<Expr>>>::into(Expr::Nested($EXPR.into()))
        };
    }

    /// Constructs a [Expr::UnaryOp] expression.
    macro_rules! unary {
        (- $EXPR:expr) => {
            <Expr as std::convert::Into<Box<Expr>>>::into(Expr::UnaryOp(
                UnaryOperator::Minus,
                $EXPR.into(),
            ))
        };
        (+ $EXPR:expr) => {
            <Expr as std::convert::Into<Box<Expr>>>::into(Expr::UnaryOp(
                UnaryOperator::Plus,
                $EXPR.into(),
            ))
        };
    }

    /// Constructs a [Expr::BinaryOp] expression.
    macro_rules! binary_op {
        ($LHS: expr, $OP: ident, $RHS: expr) => {
            <Expr as std::convert::Into<Box<Expr>>>::into(Expr::BinaryOp {
                lhs: $LHS.into(),
                op: BinaryOperator::$OP,
                rhs: $RHS.into(),
            })
        };
    }

    #[test]
    fn test_arithmetic_expression() {
        let (_, got) = conditional_expression("5 + 51").unwrap();
        assert_eq!(got, *binary_op!(5, Add, 51));

        let (_, got) = conditional_expression("5 + $foo").unwrap();
        assert_eq!(got, *binary_op!(5, Add, param!("foo")));

        // Following two tests validate that operators of higher precedence
        // are nested deeper in the AST.

        let (_, got) = conditional_expression("5 % -3 | 2").unwrap();
        assert_eq!(
            got,
            *binary_op!(binary_op!(5, Mod, unary!(-3)), BitwiseOr, 2)
        );

        let (_, got) = conditional_expression("-3 | 2 % 5").unwrap();
        assert_eq!(
            got,
            *binary_op!(unary!(-3), BitwiseOr, binary_op!(2, Mod, 5))
        );

        let (_, got) = conditional_expression("5 % 2 | -3").unwrap();
        assert_eq!(
            got,
            *binary_op!(binary_op!(5, Mod, 2), BitwiseOr, unary!(-3))
        );

        let (_, got) = conditional_expression("2 | -3 % 5").unwrap();
        assert_eq!(
            got,
            *binary_op!(2, BitwiseOr, binary_op!(unary!(-3), Mod, 5))
        );

        let (_, got) = conditional_expression("5 - -(3 | 2)").unwrap();
        assert_eq!(
            got,
            *binary_op!(5, Sub, unary!(-nested!(binary_op!(3, BitwiseOr, 2))))
        );

        let (_, got) = conditional_expression("2 | 5 % 3").unwrap();
        assert_eq!(got, *binary_op!(2, BitwiseOr, binary_op!(5, Mod, 3)));

        // Fallible cases

        // invalid operator / incomplete expression
        assert_failure!(conditional_expression("5 || 3"));
    }

    #[test]
    fn test_conditional_expression() {
        let (_, got) = conditional_expression("foo = 5").unwrap();
        assert_eq!(got, *binary_op!(ident!("foo"), Eq, 5));

        let (_, got) = conditional_expression("foo != 5").unwrap();
        assert_eq!(got, *binary_op!(ident!("foo"), NotEq, 5));

        let (_, got) = conditional_expression("foo > 5").unwrap();
        assert_eq!(got, *binary_op!(ident!("foo"), Gt, 5));

        let (_, got) = conditional_expression("foo >= 5").unwrap();
        assert_eq!(got, *binary_op!(ident!("foo"), GtEq, 5));

        let (_, got) = conditional_expression("foo < 5").unwrap();
        assert_eq!(got, *binary_op!(ident!("foo"), Lt, 5));

        let (_, got) = conditional_expression("foo <= 5").unwrap();
        assert_eq!(got, *binary_op!(ident!("foo"), LtEq, 5));

        let (_, got) = conditional_expression("foo > 5 + 6 ").unwrap();
        assert_eq!(got, *binary_op!(ident!("foo"), Gt, binary_op!(5, Add, 6)));

        let (_, got) = conditional_expression("5 <= -6").unwrap();
        assert_eq!(got, *binary_op!(5, LtEq, unary!(-6)));

        // simple expressions
        let (_, got) = conditional_expression("true").unwrap();
        assert_eq!(got, Expr::Literal(true.into()));

        // Fallible cases

        // conditional expression must be complete
        assert_failure!(conditional_expression("5 <="));

        // should not accept a regex literal
        assert_failure!(conditional_expression("5 = /regex/"));
    }

    #[test]
    fn test_logical_expression() {
        let (_, got) = conditional_expression("5 AND 6").unwrap();
        assert_eq!(got, *binary_op!(5, And, 6));

        let (_, got) = conditional_expression("5 AND 6 OR 7").unwrap();
        assert_eq!(got, *binary_op!(binary_op!(5, And, 6), Or, 7));

        let (_, got) = conditional_expression("5 > 3 OR 6 = 7 AND 7 != 1").unwrap();
        assert_eq!(
            got,
            *binary_op!(
                binary_op!(5, Gt, 3),
                Or,
                binary_op!(binary_op!(6, Eq, 7), And, binary_op!(7, NotEq, 1))
            )
        );

        let (_, got) = conditional_expression("5 AND (6 OR 7)").unwrap();
        assert_eq!(got, *binary_op!(5, And, nested!(binary_op!(6, Or, 7))));

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
        assert_eq!(got, *binary_op!(ident!("foo"), EqRegex, regex!("(a > b)")));

        let (_, got) = conditional_expression("foo !~ /bar/").unwrap();
        assert_eq!(got, *binary_op!(ident!("foo"), NotEqRegex, regex!("bar")));

        // Fallible cases

        // Expects a regex literal after regex conditional operators
        assert_failure!(conditional_expression("foo =~ 5"));
        assert_failure!(conditional_expression("foo !~ 5"));
    }

    #[test]
    fn test_spacing_and_remaining_input() {
        // Validate that the remaining input is returned
        let (got, _) = conditional_expression("foo < 1 + 2 LIMIT 10").unwrap();
        assert_eq!(got, " LIMIT 10");

        // Any whitespace preceding the expression is consumed
        conditional_expression("  foo < 1 + 2").unwrap();

        // Various whitespace separators are supported between tokens
        let (got, _) = conditional_expression("foo\n > 1 \t + \n \t3").unwrap();
        assert!(got.is_empty())
    }

    #[test]
    fn test_display_expr() {
        let (_, e) = conditional_expression("5 + 51").unwrap();
        let got = format!("{}", e);
        assert_eq!(got, "5 + 51");

        let (_, e) = conditional_expression("5 + -10").unwrap();
        let got = format!("{}", e);
        assert_eq!(got, "5 + -10");

        let (_, e) = conditional_expression("-(5 % 6)").unwrap();
        let got = format!("{}", e);
        assert_eq!(got, "-(5 % 6)");

        // vary spacing
        let (_, e) = conditional_expression("( 5 + 6 ) * -( 7+ 8)").unwrap();
        let got = format!("{}", e);
        assert_eq!(got, "(5 + 6) * -(7 + 8)");

        // multiple unary and parenthesis
        let (_, e) = conditional_expression("(-(5 + 6) & -+( 7 + 8 ))").unwrap();
        let got = format!("{}", e);
        assert_eq!(got, "(-(5 + 6) & -+(7 + 8))");

        // unquoted identifier
        let (_, e) = conditional_expression("foo + 5").unwrap();
        let got = format!("{}", e);
        assert_eq!(got, "foo + 5");

        // bind parameter identifier
        let (_, e) = conditional_expression("foo + $0").unwrap();
        let got = format!("{}", e);
        assert_eq!(got, "foo + $0");

        // quoted identifier
        let (_, e) = conditional_expression(r#""foo" + 'bar'"#).unwrap();
        let got = format!("{}", e);
        assert_eq!(got, r#""foo" + 'bar'"#);

        // Duration
        let (_, e) = conditional_expression("- 6h30m").unwrap();
        let got = format!("{}", e);
        assert_eq!(got, "-6h30m");

        // can't parse literal regular expressions as part of an arithmetic expression
        assert_failure!(conditional_expression(r#""foo" + /^(no|match)$/"#));
    }
}
