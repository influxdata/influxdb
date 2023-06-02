use crate::common::ws0;
use crate::identifier::unquoted_identifier;
use crate::internal::{expect, Error, ParseError, ParseResult};
use crate::keywords::keyword;
use crate::literal::{literal_regex, Duration};
use crate::timestamp::Timestamp;
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
use num_traits::cast;
use std::fmt::{Display, Formatter, Write};
use std::ops::Neg;

/// Reference to a tag or field key.
#[derive(Clone, Debug, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub struct VarRef {
    /// The name of the tag or field.
    pub name: Identifier,

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
    pub data_type: Option<VarRefDataType>,
}

impl Display for VarRef {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let Self { name, data_type } = self;
        write!(f, "{name}")?;
        if let Some(d) = data_type {
            write!(f, "::{d}")?;
        }
        Ok(())
    }
}

/// Function call
#[derive(Clone, Debug, PartialEq)]
pub struct Call {
    /// Represents the name of the function call.
    pub name: String,

    /// Represents the list of arguments to the function call.
    pub args: Vec<Expr>,
}

impl Display for Call {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let Self { name, args } = self;
        write!(f, "{name}(")?;
        if !args.is_empty() {
            let args = args.as_slice();
            write!(f, "{}", args[0])?;
            for arg in &args[1..] {
                write!(f, ", {arg}")?;
            }
        }
        write!(f, ")")
    }
}

/// Binary operations, such as `1 + 2`.
#[derive(Clone, Debug, PartialEq)]
pub struct Binary {
    /// Represents the left-hand side of the binary expression.
    pub lhs: Box<Expr>,
    /// Represents the operator to apply to the binary expression.
    pub op: BinaryOperator,
    /// Represents the right-hand side of the binary expression.
    pub rhs: Box<Expr>,
}

impl Display for Binary {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let Self { lhs, op, rhs } = self;
        write!(f, "{lhs} {op} {rhs}")
    }
}

/// An InfluxQL arithmetic expression.
#[derive(Clone, Debug, PartialEq)]
pub enum Expr {
    /// Reference to a tag or field key.
    VarRef(VarRef),

    /// BindParameter identifier
    BindParameter(BindParameter),

    /// Literal value such as 'foo', 5 or /^(a|b)$/
    Literal(Literal),

    /// A literal wildcard (`*`) with an optional data type selection.
    Wildcard(Option<WildcardType>),

    /// A DISTINCT `<identifier>` expression.
    Distinct(Identifier),

    /// Function call
    Call(Call),

    /// Binary operations, such as `1 + 2`.
    Binary(Binary),

    /// Nested expression, such as (foo = 'bar') or (1)
    Nested(Box<Expr>),
}

impl From<Literal> for Expr {
    fn from(v: Literal) -> Self {
        Self::Literal(v)
    }
}

impl From<i64> for Expr {
    fn from(v: i64) -> Self {
        Self::Literal(v.into())
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

impl From<i64> for Box<Expr> {
    fn from(v: i64) -> Self {
        Self::new(v.into())
    }
}

impl From<i32> for Box<Expr> {
    fn from(v: i32) -> Self {
        Self::new((v as i64).into())
    }
}

impl Display for Expr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::VarRef(v) => write!(f, "{v}"),
            Self::BindParameter(v) => write!(f, "{v}"),
            Self::Literal(v) => write!(f, "{v}"),
            Self::Binary(v) => write!(f, "{v}"),
            Self::Nested(v) => write!(f, "({v})"),
            Self::Call(v) => write!(f, "{v}"),
            Self::Wildcard(Some(v)) => write!(f, "*::{v}"),
            Self::Wildcard(None) => f.write_char('*'),
            Self::Distinct(v) => write!(f, "DISTINCT {v}"),
        }
    }
}

/// Traits to help creating InfluxQL [`Expr`]s containing
/// a [`VarRef`].
pub trait AsVarRefExpr {
    /// Creates an InfluxQL [`VarRef`] expression.
    fn to_var_ref_expr(&self) -> Expr;
}

impl AsVarRefExpr for str {
    fn to_var_ref_expr(&self) -> Expr {
        Expr::VarRef(VarRef {
            name: self.into(),
            data_type: None,
        })
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
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum VarRefDataType {
    /// Represents a 64-bit float.
    Float,
    /// Represents a 64-bit integer.
    Integer,
    /// Represents a 64-bit unsigned integer.
    Unsigned,
    /// Represents a UTF-8 string.
    String,
    /// Represents a boolean.
    Boolean,
    /// Represents a field.
    Field,
    /// Represents a tag.
    Tag,
    /// Represents a timestamp.
    Timestamp,
}

impl VarRefDataType {
    /// Returns true if the receiver is a data type that identifies as a field type.
    pub fn is_field_type(&self) -> bool {
        *self < Self::Tag
    }

    /// Returns true if the receiver is a data type that identifies as a tag type.
    pub fn is_tag_type(&self) -> bool {
        *self == Self::Tag
    }

    /// Returns true if the receiver is a numeric type.
    pub fn is_numeric_type(&self) -> bool {
        *self <= Self::Unsigned
    }
}

impl Display for VarRefDataType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Float => f.write_str("float"),
            Self::Integer => f.write_str("integer"),
            Self::Unsigned => f.write_str("unsigned"),
            Self::String => f.write_str("string"),
            Self::Boolean => f.write_str("boolean"),
            Self::Tag => f.write_str("tag"),
            Self::Field => f.write_str("field"),
            Self::Timestamp => f.write_str("timestamp"),
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

impl BinaryOperator {
    fn reduce_number<T>(&self, lhs: T, rhs: T) -> T
    where
        T: num_traits::NumOps,
        T: num_traits::identities::Zero,
    {
        match self {
            Self::Add => lhs + rhs,
            Self::Sub => lhs - rhs,
            Self::Mul => lhs * rhs,
            // Divide by zero yields zero per
            // https://github.com/influxdata/influxql/blob/1ba470371ec093d57a726b143fe6ccbacf1b452b/ast.go#L5216-L5218
            Self::Div if rhs.is_zero() => T::zero(),
            Self::Div => lhs / rhs,
            Self::Mod => lhs % rhs,
            _ => unreachable!(),
        }
    }

    /// Return a value by applying the operation defined by the receiver.
    pub fn reduce<T: num_traits::int::PrimInt>(&self, lhs: T, rhs: T) -> T {
        match self {
            Self::Add | Self::Sub | Self::Mul | Self::Div | Self::Mod => {
                self.reduce_number(lhs, rhs)
            }
            Self::BitwiseAnd => lhs & rhs,
            Self::BitwiseOr => lhs | rhs,
            Self::BitwiseXor => lhs ^ rhs,
        }
    }

    /// Return a value by applying the operation defined by the receiver or [`None`]
    /// if the operation is not supported.
    pub fn try_reduce<T, U>(&self, lhs: T, rhs: U) -> Option<T>
    where
        T: num_traits::Float,
        U: num_traits::NumOps,
        U: num_traits::NumCast,
    {
        match self {
            Self::Add | Self::Sub | Self::Mul | Self::Div | Self::Mod => Some(self.reduce_number(
                lhs,
                match cast(rhs) {
                    Some(v) => v,
                    None => return None,
                },
            )),
            _ => None,
        }
    }
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

    // Unary minus is expressed by negating existing literals,
    // or producing a binary arithmetic expression that multiplies
    // Expr `e` by -1
    let e = if op == UnaryOperator::Minus {
        match e {
            Expr::Literal(Literal::Float(v)) => Expr::Literal(Literal::Float(v.neg())),
            Expr::Literal(Literal::Integer(v)) => Expr::Literal(Literal::Integer(v.neg())),
            Expr::Literal(Literal::Duration(v)) => Expr::Literal(Literal::Duration((v.0.neg()).into())),
            Expr::Literal(Literal::Unsigned(v)) => {
                if v == (i64::MAX as u64) + 1 {
                    // The minimum i64 is parsed as a Literal::Unsigned, as it exceeds
                    // int64::MAX, so we explicitly handle that case per
                    // https://github.com/influxdata/influxql/blob/7e7d61973256ffeef4b99edd0a89f18a9e52fa2d/parser.go#L2750-L2755
                    Expr::Literal(Literal::Integer(i64::MIN))
                } else {
                    return Err(nom::Err::Failure(Error::from_message(
                        i,
                        "constant overflows signed integer",
                    )));
                }
            },
            v @ Expr::VarRef { .. } | v @ Expr::Call { .. } | v @ Expr::Nested(..) | v @ Expr::BindParameter(..) => {
                Expr::Binary(Binary {
                    lhs: Box::new(Expr::Literal(Literal::Integer(-1))),
                    op: BinaryOperator::Mul,
                    rhs: Box::new(v),
                })
            }
            _ => {
                return Err(nom::Err::Failure(Error::from_message(
                    i,
                    "unexpected unary expression: expected literal integer, float, duration, field, function or parenthesis",
                )))
            }
        }
    } else {
        e
    };

    Ok((i, e))
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

/// Parse a function call expression.
///
/// The `name` field of the [`Expr::Call`] variant is guaranteed to be in lowercase.
pub(crate) fn call_expression<T>(i: &str) -> ParseResult<&str, Expr>
where
    T: ArithmeticParsers,
{
    map(
        separated_pair(
            // special case to handle `DISTINCT`, which is allowed as an identifier
            // in a call expression
            map(alt((unquoted_identifier, keyword("DISTINCT"))), |n| {
                n.to_ascii_lowercase()
            }),
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
        |(name, args)| Expr::Call(Call { name, args }),
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
                    "invalid data type for tag or field reference, expected float, integer, unsigned, string, boolean, field, tag",
                    alt((
                        value(VarRefDataType::Float, keyword("FLOAT")),
                        value(VarRefDataType::Integer, keyword("INTEGER")),
                        value(VarRefDataType::Unsigned, keyword("UNSIGNED")),
                        value(VarRefDataType::String, keyword("STRING")),
                        value(VarRefDataType::Boolean, keyword("BOOLEAN")),
                        value(VarRefDataType::Tag, keyword("TAG")),
                        value(VarRefDataType::Field, keyword("FIELD"))
                    ))
                )
            )),
        ),
        |(name, data_type)| Expr::VarRef(VarRef { name, data_type }),
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
    remainder.into_iter().fold(expr, |lhs, val| {
        Expr::Binary(Binary {
            lhs: lhs.into(),
            op: val.0,
            rhs: val.1.into(),
        })
    })
}

/// Trait for converting a type to a [`Expr::Literal`] expression.
pub trait LiteralExpr {
    /// Convert the receiver to a literal expression.
    fn lit(self) -> Expr;
}

/// Convert `v` to a literal expression.
pub fn lit<T: LiteralExpr>(v: T) -> Expr {
    v.lit()
}

impl LiteralExpr for Literal {
    fn lit(self) -> Expr {
        Expr::Literal(self)
    }
}

impl LiteralExpr for Duration {
    fn lit(self) -> Expr {
        Expr::Literal(Literal::Duration(self))
    }
}

impl LiteralExpr for bool {
    fn lit(self) -> Expr {
        Expr::Literal(Literal::Boolean(self))
    }
}

impl LiteralExpr for i64 {
    fn lit(self) -> Expr {
        Expr::Literal(Literal::Integer(self))
    }
}

impl LiteralExpr for f64 {
    fn lit(self) -> Expr {
        Expr::Literal(Literal::Float(self))
    }
}

impl LiteralExpr for String {
    fn lit(self) -> Expr {
        Expr::Literal(Literal::String(self))
    }
}

impl LiteralExpr for Timestamp {
    fn lit(self) -> Expr {
        Expr::Literal(Literal::Timestamp(self))
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::literal::literal_no_regex;
    use crate::parameter::parameter;
    use crate::{assert_expect_error, assert_failure, binary_op, nested, param, var_ref};

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
        assert_eq!(got, binary_op!(binary_op!(5, Mod, -3), BitwiseOr, 2));

        let (_, got) = arithmetic_expression("-3 | 2 % 5").unwrap();
        assert_eq!(got, binary_op!(-3, BitwiseOr, binary_op!(2, Mod, 5)));

        let (_, got) = arithmetic_expression("5 % 2 | -3").unwrap();
        assert_eq!(got, binary_op!(binary_op!(5, Mod, 2), BitwiseOr, -3));

        let (_, got) = arithmetic_expression("2 | -3 % 5").unwrap();
        assert_eq!(got, binary_op!(2, BitwiseOr, binary_op!(-3, Mod, 5)));

        let (_, got) = arithmetic_expression("5 - -(3 | 2)").unwrap();
        assert_eq!(
            got,
            binary_op!(
                5,
                Sub,
                binary_op!(-1, Mul, nested!(binary_op!(3, BitwiseOr, 2)))
            )
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
            binary_op!(
                5,
                Sub,
                binary_op!(-1, Mul, nested!(binary_op!(3, BitwiseOr, 2)))
            )
        );

        // whitespace is not significant between unary operators
        let (_, got) = arithmetic_expression("5+-(3|2)").unwrap();
        assert_eq!(
            got,
            binary_op!(
                5,
                Add,
                binary_op!(-1, Mul, nested!(binary_op!(3, BitwiseOr, 2)))
            )
        );

        // Test unary max signed
        let (_, got) = arithmetic_expression("-9223372036854775808").unwrap();
        assert_eq!(got, Expr::Literal(Literal::Integer(-9223372036854775808)));

        // Fallible cases

        // invalid operator / incomplete expression
        assert_failure!(arithmetic_expression("5 || 3"));
        assert_failure!(arithmetic_expression("5+--(3|2)"));
        // exceeds i64::MIN
        assert_failure!(arithmetic_expression("-9223372036854775809"));
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
        assert_eq!(got.to_string(), r#""db.rp.foo""#);
        assert_eq!(rem, "");

        // with cast operators

        let (_, got) = var_ref("foo::float").unwrap();
        assert_eq!(got, var_ref!("foo", Float));
        let (_, got) = var_ref("foo::integer").unwrap();
        assert_eq!(got, var_ref!("foo", Integer));
        let (_, got) = var_ref("foo::unsigned").unwrap();
        assert_eq!(got, var_ref!("foo", Unsigned));
        let (_, got) = var_ref("foo::string").unwrap();
        assert_eq!(got, var_ref!("foo", String));
        let (_, got) = var_ref("foo::boolean").unwrap();
        assert_eq!(got, var_ref!("foo", Boolean));
        let (_, got) = var_ref("foo::field").unwrap();
        assert_eq!(got, var_ref!("foo", Field));
        let (_, got) = var_ref("foo::tag").unwrap();
        assert_eq!(got, var_ref!("foo", Tag));

        // Fallible cases

        assert_expect_error!(var_ref("foo::invalid"), "invalid data type for tag or field reference, expected float, integer, unsigned, string, boolean, field, tag");
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
        assert_eq!(id.to_string(), "part0");

        // id.id
        let (rem, id) = segmented_identifier("part1.part0").unwrap();
        assert_eq!(rem, "");
        assert_eq!(id.to_string(), "\"part1.part0\"");

        // id..id
        let (rem, id) = segmented_identifier("part2..part0").unwrap();
        assert_eq!(rem, "");
        assert_eq!(id.to_string(), "\"part2..part0\"");

        // id.id.id
        let (rem, id) = segmented_identifier("part2.part1.part0").unwrap();
        assert_eq!(rem, "");
        assert_eq!(id.to_string(), "\"part2.part1.part0\"");

        // "id"."id".id
        let (rem, id) = segmented_identifier(r#""part 2"."part 1".part0"#).unwrap();
        assert_eq!(rem, "");
        assert_eq!(id.to_string(), "\"part 2.part 1.part0\"");

        // Only parses 3 segments
        let (rem, id) = segmented_identifier("part2.part1.part0.foo").unwrap();
        assert_eq!(rem, ".foo");
        assert_eq!(id.to_string(), "\"part2.part1.part0\"");

        // Quoted
        let (rem, id) = segmented_identifier("\"part0\"").unwrap();
        assert_eq!(rem, "");
        assert_eq!(id.to_string(), "part0");

        // Additional test cases, with compatibility proven via https://go.dev/play/p/k2150CJocVl

        let (rem, id) = segmented_identifier(r#""part" 2"."part 1".part0"#).unwrap();
        assert_eq!(rem, r#" 2"."part 1".part0"#);
        assert_eq!(id.to_string(), "part");

        let (rem, id) = segmented_identifier(r#""part" 2."part 1".part0"#).unwrap();
        assert_eq!(rem, r#" 2."part 1".part0"#);
        assert_eq!(id.to_string(), "part");

        let (rem, id) = segmented_identifier(r#""part "2"."part 1".part0"#).unwrap();
        assert_eq!(rem, r#"2"."part 1".part0"#);
        assert_eq!(id.to_string(), r#""part ""#);

        let (rem, id) = segmented_identifier(r#""part ""2"."part 1".part0"#).unwrap();
        assert_eq!(rem, r#""2"."part 1".part0"#);
        assert_eq!(id.to_string(), r#""part ""#);
    }

    #[test]
    fn test_display_expr() {
        #[track_caller]
        fn assert_display_expr(input: &str, expected: &str) {
            let (_, e) = arithmetic_expression(input).unwrap();
            assert_eq!(e.to_string(), expected);
        }

        assert_display_expr("5 + 51", "5 + 51");
        assert_display_expr("5 + -10", "5 + -10");
        assert_display_expr("-(5 % 6)", "-1 * (5 % 6)");

        // vary spacing
        assert_display_expr("( 5 + 6 ) * -( 7+ 8)", "(5 + 6) * -1 * (7 + 8)");

        // multiple unary and parenthesis
        assert_display_expr("(-(5 + 6) & -+( 7 + 8 ))", "(-1 * (5 + 6) & -1 * (7 + 8))");

        // unquoted identifier
        assert_display_expr("foo + 5", "foo + 5");

        // identifier, negated
        assert_display_expr("-foo + 5", "-1 * foo + 5");

        // bind parameter identifier
        assert_display_expr("foo + $0", "foo + $0");

        // quoted identifier
        assert_display_expr(r#""foo" + 'bar'"#, r#"foo + 'bar'"#);

        // quoted identifier, negated
        assert_display_expr(r#"-"foo" + 'bar'"#, r#"-1 * foo + 'bar'"#);

        // quoted identifier with spaces, negated
        assert_display_expr(r#"-"foo bar" + 'bar'"#, r#"-1 * "foo bar" + 'bar'"#);

        // Duration
        assert_display_expr("6h30m", "6h30m");

        // Negated
        assert_display_expr("- 6h30m", "-6h30m");

        // Validate other expression types

        assert_eq!(Expr::Wildcard(None).to_string(), "*");
        assert_eq!(
            Expr::Wildcard(Some(WildcardType::Field)).to_string(),
            "*::field"
        );
        assert_eq!(Expr::Distinct("foo".into()).to_string(), "DISTINCT foo");

        // can't parse literal regular expressions as part of an arithmetic expression
        assert_failure!(arithmetic_expression(r#""foo" + /^(no|match)$/"#));
    }

    /// Test call expressions using `ConditionalExpression`
    fn call(i: &str) -> ParseResult<&str, Expr> {
        call_expression::<TestParsers>(i)
    }

    #[test]
    fn test_call() {
        #[track_caller]
        fn assert_call(input: &str, expected: &str) {
            let (_, ex) = call(input).unwrap();
            assert_eq!(ex.to_string(), expected);
        }

        // These tests validate a `Call` expression and also it's Display implementation.
        // We don't need to validate Expr trees, as we do that in the conditional and arithmetic
        // tests.

        // No arguments
        assert_call("FN()", "fn()");

        // Single argument with surrounding whitespace
        assert_call("FN ( 1 )", "fn(1)");

        // Multiple arguments with varying whitespace
        assert_call("FN ( 1,2\n,3,\t4 )", "fn(1, 2, 3, 4)");

        // Arguments as expressions
        assert_call("FN ( 1 + 2, foo, 'bar' )", "fn(1 + 2, foo, 'bar')");

        // A single regular expression argument
        assert_call("FN ( /foo/ )", "fn(/foo/)");

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
            Expr::VarRef(VarRef {
                name: "foo".into(),
                data_type: None
            })
            .to_string(),
            "foo"
        );
        assert_eq!(
            Expr::VarRef(VarRef {
                name: "foo".into(),
                data_type: Some(VarRefDataType::Field)
            })
            .to_string(),
            "foo::field"
        );
    }

    #[test]
    fn test_var_ref_data_type() {
        use VarRefDataType::*;

        // Ensure ordering of data types relative to one another.

        assert!(Float < Integer);
        assert!(Integer < Unsigned);
        assert!(Unsigned < String);
        assert!(String < Boolean);
        assert!(Boolean < Field);
        assert!(Field < Tag);

        assert!(Float.is_field_type());
        assert!(Integer.is_field_type());
        assert!(Unsigned.is_field_type());
        assert!(String.is_field_type());
        assert!(Boolean.is_field_type());
        assert!(Field.is_field_type());
        assert!(Tag.is_tag_type());

        assert!(!Float.is_tag_type());
        assert!(!Integer.is_tag_type());
        assert!(!Unsigned.is_tag_type());
        assert!(!String.is_tag_type());
        assert!(!Boolean.is_tag_type());
        assert!(!Field.is_tag_type());
        assert!(!Tag.is_field_type());

        assert!(Float.is_numeric_type());
        assert!(Integer.is_numeric_type());
        assert!(Unsigned.is_numeric_type());
        assert!(!String.is_numeric_type());
        assert!(!Boolean.is_numeric_type());
        assert!(!Field.is_numeric_type());
        assert!(!Tag.is_numeric_type());
    }

    #[test]
    fn test_binary_operator_reduce() {
        use BinaryOperator::*;

        //
        // Integer, Integer
        //

        // Numeric operations
        assert_eq!(Add.reduce(10, 2), 12);
        assert_eq!(Sub.reduce(10, 2), 8);
        assert_eq!(Mul.reduce(10, 2), 20);
        assert_eq!(Div.reduce(10, 2), 5);
        // Divide by zero yields zero
        assert_eq!(Div.reduce(10, 0), 0);
        assert_eq!(Mod.reduce(10, 2), 0);
        // Bitwise operations
        assert_eq!(BitwiseAnd.reduce(0b1111, 0b1010), 0b1010);
        assert_eq!(BitwiseOr.reduce(0b0101, 0b1010), 0b1111);
        assert_eq!(BitwiseXor.reduce(0b1101, 0b1010), 0b0111);

        //
        // Float, Float
        //

        assert_eq!(Add.try_reduce(10.0, 2.0).unwrap(), 12.0);
        assert_eq!(Sub.try_reduce(10.0, 2.0).unwrap(), 8.0);
        assert_eq!(Mul.try_reduce(10.0, 2.0).unwrap(), 20.0);
        assert_eq!(Div.try_reduce(10.0, 2.0).unwrap(), 5.0);
        // Divide by zero yields zero
        assert_eq!(Div.try_reduce(10.0, 0.0).unwrap(), 0.0);
        assert_eq!(Mod.try_reduce(10.0, 2.0).unwrap(), 0.0);

        // Bitwise operations
        assert!(BitwiseAnd.try_reduce(1.0, 1.0).is_none());
        assert!(BitwiseOr.try_reduce(1.0, 1.0).is_none());
        assert!(BitwiseXor.try_reduce(1.0, 1.0).is_none());

        //
        // Float, Integer
        //

        assert_eq!(Add.try_reduce(10.0, 2).unwrap(), 12.0);
        assert_eq!(Sub.try_reduce(10.0, 2).unwrap(), 8.0);
        assert_eq!(Mul.try_reduce(10.0, 2).unwrap(), 20.0);
        assert_eq!(Div.try_reduce(10.0, 2).unwrap(), 5.0);
        // Divide by zero yields zero
        assert_eq!(Div.try_reduce(10.0, 0).unwrap(), 0.0);
        assert_eq!(Mod.try_reduce(10.0, 2).unwrap(), 0.0);
    }
}
