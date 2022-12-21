use crate::common::ws0;
use crate::identifier::unquoted_identifier;
use crate::internal::{expect, ParseResult};
use crate::keywords::keyword;
use crate::literal::literal_regex;
use crate::{
    identifier::{identifier, Identifier},
    literal::Literal,
    parameter::BindParameter,
};
use nom::branch::alt;
use nom::bytes::complete::tag;
use nom::character::complete::char;
use nom::combinator::{cut, map, opt, value};
use nom::multi::{many0, separated_list0};
use nom::sequence::{delimited, pair, preceded, separated_pair, terminated, tuple};
use std::fmt::{Display, Formatter, Write};

/// An InfluxQL arithmetic expression.
#[derive(Clone, Debug, PartialEq)]
pub enum Expr {
    /// Reference to a tag or field key.
    VarRef {
        /// The name of the tag or field.
        name: Identifier,

        /// An optional data type selection specified using the `::` operator.
        ///
        /// When the `::` operator follows an identifier, it instructs InfluxQL to fetch
        /// only data of the matching data type.
        ///
        /// The `::` operator appears after an [`Identifier`] and may be described using
        /// the following EBNF:
        ///
        /// ```text
        /// variable_ref ::= identifier ( "::" data_type )?
        /// data_type    ::= "float" | "integer" | "boolean" | "string" | "tag" | "field"
        /// ```
        ///
        /// For example:
        ///
        /// ```text
        /// SELECT foo::field, host::tag, usage_idle::integer, idle::boolean FROM cpu
        /// ```
        ///
        /// Specifies the following:
        ///
        /// * `foo::field` will return a field of any data type named `foo`
        /// * `host::tag` will return a tag named `host`
        /// * `usage_idle::integer` will return either a float or integer field named `usage_idle`,
        ///   and casting it to an `integer`
        /// * `idle::boolean` will return a field named `idle` that has a matching data type of
        ///    `boolean`
        data_type: Option<VarRefDataType>,
    },

    /// BindParameter identifier
    BindParameter(BindParameter),

    /// Literal value such as 'foo', 5 or /^(a|b)$/
    Literal(Literal),

    /// A literal wildcard (`*`) with an optional data type selection.
    Wildcard(Option<WildcardType>),

    /// A DISTINCT `<identifier>` expression.
    Distinct(Identifier),

    /// Unary operation such as + 5 or - 1h3m
    UnaryOp(UnaryOperator, Box<Expr>),

    /// Function call
    Call {
        /// Represents the name of the function call.
        name: String,

        /// Represents the list of arguments to the function call.
        args: Vec<Expr>,
    },

    /// Binary operations, such as `1 + 2`.
    Binary {
        /// Represents the left-hand side of the binary expression.
        lhs: Box<Expr>,
        /// Represents the operator to apply to the binary expression.
        op: BinaryOperator,
        /// Represents the right-hand side of the binary expression.
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
            Self::VarRef { name, data_type } => {
                write!(f, "{}", name)?;
                if let Some(d) = data_type {
                    write!(f, "::{}", d)?;
                }
            }
            Self::BindParameter(v) => write!(f, "{}", v)?,
            Self::Literal(v) => write!(f, "{}", v)?,
            Self::UnaryOp(op, e) => write!(f, "{}{}", op, e)?,
            Self::Binary { lhs, op, rhs } => write!(f, "{} {} {}", lhs, op, rhs)?,
            Self::Nested(e) => write!(f, "({})", e)?,
            Self::Call { name, args } => {
                write!(f, "{}(", name)?;
                if !args.is_empty() {
                    let args = args.as_slice();
                    write!(f, "{}", args[0])?;
                    for arg in &args[1..] {
                        write!(f, ", {}", arg)?;
                    }
                }
                write!(f, ")")?;
            }
            Self::Wildcard(Some(dt)) => write!(f, "*::{}", dt)?,
            Self::Wildcard(None) => f.write_char('*')?,
            Self::Distinct(ident) => write!(f, "DISTINCT {}", ident)?,
        }

        Ok(())
    }
}

/// Specifies the data type of a wildcard (`*`) when using the `::` operator.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WildcardType {
    /// Indicates the wildcard refers to tags only.
    Tag,

    /// Indicates the wildcard refers to fields only.
    Field,
}

impl Display for WildcardType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Tag => f.write_str("tag"),
            Self::Field => f.write_str("field"),
        }
    }
}

/// Represents the primitive data types of a [`Expr::VarRef`] when specified
/// using a [cast operation][cast].
///
/// InfluxQL only supports casting between [`Self::Float`] and [`Self::Integer`] types.
///
/// [cast]: https://docs.influxdata.com/influxdb/v1.8/query_language/explore-data/#cast-operations
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum VarRefDataType {
    /// Represents a 64-bit float.
    Float,
    /// Represents a 64-bit integer.
    Integer,
    /// Represents a UTF-8 string.
    String,
    /// Represents a boolean.
    Boolean,
    /// Represents a tag.
    Tag,
    /// Represents a field.
    Field,
}

impl Display for VarRefDataType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Float => f.write_str("float"),
            Self::Integer => f.write_str("integer"),
            Self::String => f.write_str("string"),
            Self::Boolean => f.write_str("boolean"),
            Self::Tag => f.write_str("tag"),
            Self::Field => f.write_str("field"),
        }
    }
}

/// An InfluxQL unary operator.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnaryOperator {
    /// Represents the unary `+` operator.
    Plus,
    /// Represents the unary `-` operator.
    Minus,
}

impl Display for UnaryOperator {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Plus => f.write_char('+'),
            Self::Minus => f.write_char('-'),
        }
    }
}

/// An InfluxQL binary operators.
#[derive(Clone, Debug, Copy, PartialEq, Eq)]
pub enum BinaryOperator {
    /// Represents the `+` operator.
    Add,
    /// Represents the `-` operator.
    Sub,
    /// Represents the `*` operator.
    Mul,
    /// Represents the `/` operator.
    Div,
    /// Represents the `%` or modulus operator.
    Mod,
    /// Represents the `&` or bitwise-and operator.
    BitwiseAnd,
    /// Represents the `|` or bitwise-or operator.
    BitwiseOr,
    /// Represents the `^` or bitwise-xor operator.
    BitwiseXor,
}

impl Display for BinaryOperator {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Add => f.write_char('+'),
            Self::Sub => f.write_char('-'),
            Self::Mul => f.write_char('*'),
            Self::Div => f.write_char('/'),
            Self::Mod => f.write_char('%'),
            Self::BitwiseAnd => f.write_char('&'),
            Self::BitwiseOr => f.write_char('|'),
            Self::BitwiseXor => f.write_char('^'),
        }
    }
}

/// Parse a unary expression.
fn unary<T>(i: &str) -> ParseResult<&str, Expr>
where
    T: ArithmeticParsers,
{
    let (i, op) = preceded(
        ws0,
        alt((
            value(UnaryOperator::Plus, char('+')),
            value(UnaryOperator::Minus, char('-')),
        )),
    )(i)?;

    let (i, e) = factor::<T>(i)?;

    Ok((i, Expr::UnaryOp(op, e.into())))
}

/// Parse a parenthesis expression.
fn parens<T>(i: &str) -> ParseResult<&str, Expr>
where
    T: ArithmeticParsers,
{
    delimited(
        preceded(ws0, char('(')),
        map(arithmetic::<T>, |e| Expr::Nested(e.into())),
        preceded(ws0, char(')')),
    )(i)
}

/// Parse a function call expression
pub(crate) fn call_expression<T>(i: &str) -> ParseResult<&str, Expr>
where
    T: ArithmeticParsers,
{
    map(
        separated_pair(
            // special case to handle `DISTINCT`, which is allowed as an identifier
            // in a call expression
            map(
                alt((unquoted_identifier, keyword("DISTINCT"))),
                &str::to_string,
            ),
            ws0,
            delimited(
                char('('),
                alt((
                    // A single regular expression to match 0 or more field keys
                    map(preceded(ws0, literal_regex), |re| vec![re.into()]),
                    // A list of Expr, separated by commas
                    separated_list0(preceded(ws0, char(',')), arithmetic::<T>),
                )),
                cut(preceded(ws0, char(')'))),
            ),
        ),
        |(name, args)| Expr::Call { name, args },
    )(i)
}

/// Parse a segmented identifier
///
/// ```text
/// segmented_identifier ::= identifier |
///                          ( identifier "." identifier ) |
///                          ( identifier "." identifier? "." identifier )
/// ```
fn segmented_identifier(i: &str) -> ParseResult<&str, Identifier> {
    let (remaining, (opt_prefix, name)) = pair(
        opt(alt((
            // ident2 "." ident1 "."
            map(
                pair(
                    terminated(identifier, tag(".")),
                    terminated(identifier, tag(".")),
                ),
                |(ident2, ident1)| (Some(ident2), Some(ident1)),
            ),
            // identifier ".."
            map(terminated(identifier, tag("..")), |ident2| {
                (Some(ident2), None)
            }),
            // identifier "."
            map(terminated(identifier, tag(".")), |ident1| {
                (None, Some(ident1))
            }),
        ))),
        identifier,
    )(i)?;

    Ok((
        remaining,
        match opt_prefix {
            Some((None, Some(ident1))) => format!("{}.{}", ident1.0, name.0).into(),
            Some((Some(ident2), None)) => format!("{}..{}", ident2.0, name.0).into(),
            Some((Some(ident2), Some(ident1))) => {
                format!("{}.{}.{}", ident2.0, ident1.0, name.0).into()
            }
            _ => name,
        },
    ))
}

/// Parse a variable reference, which is a segmented identifier followed by an optional cast expression.
pub(crate) fn var_ref(i: &str) -> ParseResult<&str, Expr> {
    map(
        pair(
            segmented_identifier,
            opt(preceded(
                tag("::"),
                expect(
                    "invalid data type for tag or field reference, expected float, integer, string, boolean, tag or field",
                    alt((
                        value(VarRefDataType::Float, keyword("FLOAT")),
                        value(VarRefDataType::Integer, keyword("INTEGER")),
                        value(VarRefDataType::String, keyword("STRING")),
                        value(VarRefDataType::Boolean, keyword("BOOLEAN")),
                        value(VarRefDataType::Tag, keyword("TAG")),
                        value(VarRefDataType::Field, keyword("FIELD"))
                    ))
                )
            )),
        ),
        |(name, data_type)| Expr::VarRef { name, data_type },
    )(i)
}

/// Parse precedence priority 1 operators.
///
/// These are the highest precedence operators, and include parenthesis and the unary operators.
fn factor<T>(i: &str) -> ParseResult<&str, Expr>
where
    T: ArithmeticParsers,
{
    alt((unary::<T>, parens::<T>, T::operand))(i)
}

/// Parse arithmetic, precedence priority 2 operators.
///
/// This includes the multiplication, division, bitwise and, and modulus operators.
fn term<T>(i: &str) -> ParseResult<&str, Expr>
where
    T: ArithmeticParsers,
{
    let (input, left) = factor::<T>(i)?;
    let (input, remaining) = many0(tuple((
        preceded(
            ws0,
            alt((
                value(BinaryOperator::Mul, char('*')),
                value(BinaryOperator::Div, char('/')),
                value(BinaryOperator::BitwiseAnd, char('&')),
                value(BinaryOperator::Mod, char('%')),
            )),
        ),
        factor::<T>,
    )))(input)?;
    Ok((input, reduce_expr(left, remaining)))
}

/// Parse an arithmetic expression.
///
/// This includes the addition, subtraction, bitwise or, and bitwise xor operators.
pub(crate) fn arithmetic<T>(i: &str) -> ParseResult<&str, Expr>
where
    T: ArithmeticParsers,
{
    let (input, left) = term::<T>(i)?;
    let (input, remaining) = many0(tuple((
        preceded(
            ws0,
            alt((
                value(BinaryOperator::Add, char('+')),
                value(BinaryOperator::Sub, char('-')),
                value(BinaryOperator::BitwiseOr, char('|')),
                value(BinaryOperator::BitwiseXor, char('^')),
            )),
        ),
        cut(term::<T>),
    )))(input)?;
    Ok((input, reduce_expr(left, remaining)))
}

/// A trait for customizing arithmetic parsers.
pub(crate) trait ArithmeticParsers {
    /// Parse an operand of an arithmetic expression.
    fn operand(i: &str) -> ParseResult<&str, Expr>;
}

/// Folds `expr` and `remainder` into a [Expr::Binary] tree.
fn reduce_expr(expr: Expr, remainder: Vec<(BinaryOperator, Expr)>) -> Expr {
    remainder.into_iter().fold(expr, |lhs, val| Expr::Binary {
        lhs: lhs.into(),
        op: val.0,
        rhs: val.1.into(),
    })
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::literal::literal_no_regex;
    use crate::parameter::parameter;
    use crate::{assert_expect_error, assert_failure, binary_op, nested, param, unary, var_ref};

    struct TestParsers;

    impl ArithmeticParsers for TestParsers {
        fn operand(i: &str) -> ParseResult<&str, Expr> {
            preceded(
                ws0,
                alt((
                    map(literal_no_regex, Expr::Literal),
                    var_ref,
                    map(parameter, Expr::BindParameter),
                )),
            )(i)
        }
    }

    fn arithmetic_expression(i: &str) -> ParseResult<&str, Expr> {
        arithmetic::<TestParsers>(i)
    }

    #[test]
    fn test_arithmetic() {
        let (_, got) = arithmetic_expression("5 + 51").unwrap();
        assert_eq!(got, binary_op!(5, Add, 51));

        let (_, got) = arithmetic_expression("5 + $foo").unwrap();
        assert_eq!(got, binary_op!(5, Add, param!("foo")));

        // Following two tests validate that operators of higher precedence
        // are nested deeper in the AST.

        let (_, got) = arithmetic_expression("5 % -3 | 2").unwrap();
        assert_eq!(
            got,
            binary_op!(binary_op!(5, Mod, unary!(-3)), BitwiseOr, 2)
        );

        let (_, got) = arithmetic_expression("-3 | 2 % 5").unwrap();
        assert_eq!(
            got,
            binary_op!(unary!(-3), BitwiseOr, binary_op!(2, Mod, 5))
        );

        let (_, got) = arithmetic_expression("5 % 2 | -3").unwrap();
        assert_eq!(
            got,
            binary_op!(binary_op!(5, Mod, 2), BitwiseOr, unary!(-3))
        );

        let (_, got) = arithmetic_expression("2 | -3 % 5").unwrap();
        assert_eq!(
            got,
            binary_op!(2, BitwiseOr, binary_op!(unary!(-3), Mod, 5))
        );

        let (_, got) = arithmetic_expression("5 - -(3 | 2)").unwrap();
        assert_eq!(
            got,
            binary_op!(5, Sub, unary!(-nested!(binary_op!(3, BitwiseOr, 2))))
        );

        let (_, got) = arithmetic_expression("2 | 5 % 3").unwrap();
        assert_eq!(got, binary_op!(2, BitwiseOr, binary_op!(5, Mod, 3)));

        // Expressions are still valid when unnecessary whitespace is omitted

        let (_, got) = arithmetic_expression("5+51").unwrap();
        assert_eq!(got, binary_op!(5, Add, 51));

        let (_, got) = arithmetic_expression("5+$foo").unwrap();
        assert_eq!(got, binary_op!(5, Add, param!("foo")));

        let (_, got) = arithmetic_expression("5- -(3|2)").unwrap();
        assert_eq!(
            got,
            binary_op!(5, Sub, unary!(-nested!(binary_op!(3, BitwiseOr, 2))))
        );

        // whitespace is not significant between unary operators
        let (_, got) = arithmetic_expression("5+-(3|2)").unwrap();
        assert_eq!(
            got,
            binary_op!(5, Add, unary!(-nested!(binary_op!(3, BitwiseOr, 2))))
        );

        // Fallible cases

        // invalid operator / incomplete expression
        assert_failure!(arithmetic_expression("5 || 3"));
        // TODO: skip until https://github.com/influxdata/influxdb_iox/issues/5663 is implemented
        // assert_failure!(arithmetic("5+--(3|2)"));
    }

    #[test]
    fn test_var_ref() {
        let (_, got) = var_ref("foo").unwrap();
        assert_eq!(got, var_ref!("foo"));

        // Whilst this is parsed as a 3-part name, it is treated as a quoted string 🙄
        // VarRefs are parsed as segmented identifiers
        //
        //   * https://github.com/influxdata/influxql/blob/7e7d61973256ffeef4b99edd0a89f18a9e52fa2d/parser.go#L2515-L2516
        //
        // and then the segments are joined as a single string
        //
        //   * https://github.com/influxdata/influxql/blob/7e7d61973256ffeef4b99edd0a89f18a9e52fa2d/parser.go#L2551
        let (rem, got) = var_ref("db.rp.foo").unwrap();
        assert_eq!(got, var_ref!("db.rp.foo"));
        assert_eq!(format!("{}", got), r#""db.rp.foo""#);
        assert_eq!(rem, "");

        // with cast operator
        let (_, got) = var_ref("foo::tag").unwrap();
        assert_eq!(got, var_ref!("foo", Tag));

        // Fallible cases

        assert_expect_error!(var_ref("foo::invalid"), "invalid data type for tag or field reference, expected float, integer, string, boolean, tag or field");
    }

    #[test]
    fn test_spacing_and_remaining_input() {
        // Validate that the remaining input is returned
        let (got, _) = arithmetic_expression("foo - 1 + 2 LIMIT 10").unwrap();
        assert_eq!(got, " LIMIT 10");

        // Any whitespace preceding the expression is consumed
        let (got, _) = arithmetic_expression("  foo - 1 + 2").unwrap();
        assert_eq!(got, "");

        // Various whitespace separators are supported between tokens
        let (got, _) = arithmetic_expression("foo\n | 1 \t + \n \t3").unwrap();
        assert!(got.is_empty())
    }

    #[test]
    fn test_segmented_identifier() {
        // Unquoted
        let (rem, id) = segmented_identifier("part0").unwrap();
        assert_eq!(rem, "");
        assert_eq!(format!("{}", id), "part0");

        // id.id
        let (rem, id) = segmented_identifier("part1.part0").unwrap();
        assert_eq!(rem, "");
        assert_eq!(format!("{}", id), "\"part1.part0\"");

        // id..id
        let (rem, id) = segmented_identifier("part2..part0").unwrap();
        assert_eq!(rem, "");
        assert_eq!(format!("{}", id), "\"part2..part0\"");

        // id.id.id
        let (rem, id) = segmented_identifier("part2.part1.part0").unwrap();
        assert_eq!(rem, "");
        assert_eq!(format!("{}", id), "\"part2.part1.part0\"");

        // "id"."id".id
        let (rem, id) = segmented_identifier(r#""part 2"."part 1".part0"#).unwrap();
        assert_eq!(rem, "");
        assert_eq!(format!("{}", id), "\"part 2.part 1.part0\"");

        // Only parses 3 segments
        let (rem, id) = segmented_identifier("part2.part1.part0.foo").unwrap();
        assert_eq!(rem, ".foo");
        assert_eq!(format!("{}", id), "\"part2.part1.part0\"");

        // Quoted
        let (rem, id) = segmented_identifier("\"part0\"").unwrap();
        assert_eq!(rem, "");
        assert_eq!(format!("{}", id), "part0");

        // Additional test cases, with compatibility proven via https://go.dev/play/p/k2150CJocVl

        let (rem, id) = segmented_identifier(r#""part" 2"."part 1".part0"#).unwrap();
        assert_eq!(rem, r#" 2"."part 1".part0"#);
        assert_eq!(format!("{}", id), "part");

        let (rem, id) = segmented_identifier(r#""part" 2."part 1".part0"#).unwrap();
        assert_eq!(rem, r#" 2."part 1".part0"#);
        assert_eq!(format!("{}", id), "part");

        let (rem, id) = segmented_identifier(r#""part "2"."part 1".part0"#).unwrap();
        assert_eq!(rem, r#"2"."part 1".part0"#);
        assert_eq!(format!("{}", id), r#""part ""#);

        let (rem, id) = segmented_identifier(r#""part ""2"."part 1".part0"#).unwrap();
        assert_eq!(rem, r#""2"."part 1".part0"#);
        assert_eq!(format!("{}", id), r#""part ""#);
    }

    #[test]
    fn test_display_expr() {
        let (_, e) = arithmetic_expression("5 + 51").unwrap();
        let got = format!("{}", e);
        assert_eq!(got, "5 + 51");

        let (_, e) = arithmetic_expression("5 + -10").unwrap();
        let got = format!("{}", e);
        assert_eq!(got, "5 + -10");

        let (_, e) = arithmetic_expression("-(5 % 6)").unwrap();
        let got = format!("{}", e);
        assert_eq!(got, "-(5 % 6)");

        // vary spacing
        let (_, e) = arithmetic_expression("( 5 + 6 ) * -( 7+ 8)").unwrap();
        let got = format!("{}", e);
        assert_eq!(got, "(5 + 6) * -(7 + 8)");

        // multiple unary and parenthesis
        let (_, e) = arithmetic_expression("(-(5 + 6) & -+( 7 + 8 ))").unwrap();
        let got = format!("{}", e);
        assert_eq!(got, "(-(5 + 6) & -+(7 + 8))");

        // unquoted identifier
        let (_, e) = arithmetic_expression("foo + 5").unwrap();
        let got = format!("{}", e);
        assert_eq!(got, "foo + 5");

        // bind parameter identifier
        let (_, e) = arithmetic_expression("foo + $0").unwrap();
        let got = format!("{}", e);
        assert_eq!(got, "foo + $0");

        // quoted identifier
        let (_, e) = arithmetic_expression(r#""foo" + 'bar'"#).unwrap();
        let got = format!("{}", e);
        assert_eq!(got, r#"foo + 'bar'"#);

        // Duration
        let (_, e) = arithmetic_expression("- 6h30m").unwrap();
        let got = format!("{}", e);
        assert_eq!(got, "-6h30m");

        // Validate other expression types

        assert_eq!(format!("{}", Expr::Wildcard(None)), "*");
        assert_eq!(
            format!("{}", Expr::Wildcard(Some(WildcardType::Field))),
            "*::field"
        );
        assert_eq!(format!("{}", Expr::Distinct("foo".into())), "DISTINCT foo");

        // can't parse literal regular expressions as part of an arithmetic expression
        assert_failure!(arithmetic_expression(r#""foo" + /^(no|match)$/"#));
    }

    /// Test call expressions using `ConditionalExpression`
    fn call(i: &str) -> ParseResult<&str, Expr> {
        call_expression::<TestParsers>(i)
    }

    #[test]
    fn test_call() {
        // These tests validate a `Call` expression and also it's Display implementation.
        // We don't need to validate Expr trees, as we do that in the conditional and arithmetic
        // tests.

        // No arguments
        let (_, ex) = call("FN()").unwrap();
        let got = format!("{}", ex);
        assert_eq!(got, "FN()");

        // Single argument with surrounding whitespace
        let (_, ex) = call("FN ( 1 )").unwrap();
        let got = format!("{}", ex);
        assert_eq!(got, "FN(1)");

        // Multiple arguments with varying whitespace
        let (_, ex) = call("FN ( 1,2\n,3,\t4 )").unwrap();
        let got = format!("{}", ex);
        assert_eq!(got, "FN(1, 2, 3, 4)");

        // Arguments as expressions
        let (_, ex) = call("FN ( 1 + 2, foo, 'bar' )").unwrap();
        let got = format!("{}", ex);
        assert_eq!(got, "FN(1 + 2, foo, 'bar')");

        // A single regular expression argument
        let (_, ex) = call("FN ( /foo/ )").unwrap();
        let got = format!("{}", ex);
        assert_eq!(got, "FN(/foo/)");

        // Fallible cases

        call("FN ( 1").unwrap_err();
        call("FN ( 1, )").unwrap_err();
        call("FN ( 1,, 2 )").unwrap_err();

        // Conditionals not supported
        call("FN ( 1 = 2 )").unwrap_err();

        // Multiple regular expressions not supported
        call("FN ( /foo/, /bar/ )").unwrap_err();
    }

    #[test]
    fn test_var_ref_display() {
        assert_eq!(
            format!(
                "{}",
                Expr::VarRef {
                    name: "foo".into(),
                    data_type: None
                }
            ),
            "foo"
        );
        assert_eq!(
            format!(
                "{}",
                Expr::VarRef {
                    name: "foo".into(),
                    data_type: Some(VarRefDataType::Field)
                }
            ),
            "foo::field"
        );
    }
}
