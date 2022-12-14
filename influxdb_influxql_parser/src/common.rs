//! Type and parsers common to many statements.

use crate::expression::conditional::{conditional_expression, ConditionalExpression};
use crate::identifier::{identifier, Identifier};
use crate::internal::{expect, verify, ParseResult};
use crate::keywords::{keyword, Token};
use crate::literal::unsigned_integer;
use crate::string::{regex, Regex};
use core::fmt;
use nom::branch::alt;
use nom::bytes::complete::{is_not, tag, take_until};
use nom::character::complete::{char, multispace1};
use nom::combinator::{map, opt, recognize, value};
use nom::multi::{fold_many0, fold_many1, separated_list1};
use nom::sequence::{delimited, pair, preceded, terminated};
use std::fmt::{Display, Formatter};
use std::ops::{Deref, DerefMut};

/// Represents a measurement name as either an identifier or a regular expression.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MeasurementName {
    /// A measurement name expressed as an [`Identifier`].
    Name(Identifier),

    /// A measurement name expressed as a [`Regex`].
    Regex(Regex),
}

impl Parser for MeasurementName {
    /// Parse a measurement name, which may be an identifier or a regular expression.
    fn parse(i: &str) -> ParseResult<&str, Self> {
        alt((
            map(identifier, MeasurementName::Name),
            map(regex, MeasurementName::Regex),
        ))(i)
    }
}

impl Display for MeasurementName {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Name(ident) => fmt::Display::fmt(ident, f),
            Self::Regex(regex) => fmt::Display::fmt(regex, f),
        }
    }
}

/// Represents a fully-qualified, 3-part measurement name.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QualifiedMeasurementName {
    /// An optional database name.
    pub database: Option<Identifier>,

    /// An optional retention policy.
    pub retention_policy: Option<Identifier>,

    /// The measurement name.
    pub name: MeasurementName,
}

impl Display for QualifiedMeasurementName {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self {
                database: None,
                retention_policy: None,
                name,
            } => write!(f, "{}", name),
            Self {
                database: Some(db),
                retention_policy: None,
                name,
            } => write!(f, "{}..{}", db, name),
            Self {
                database: None,
                retention_policy: Some(rp),
                name,
            } => write!(f, "{}.{}", rp, name),
            Self {
                database: Some(db),
                retention_policy: Some(rp),
                name,
            } => write!(f, "{}.{}.{}", db, rp, name),
        }
    }
}

/// Match a fully-qualified, 3-part measurement name.
///
/// ```text
/// qualified_measurement_name ::= measurement_name |
///                              ( policy_name "." measurement_name ) |
///                              ( db_name "." policy_name? "." measurement_name )
///
/// db_name          ::= identifier
/// policy_name      ::= identifier
/// measurement_name ::= identifier | regex_lit
/// ```
pub(crate) fn qualified_measurement_name(i: &str) -> ParseResult<&str, QualifiedMeasurementName> {
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
        MeasurementName::parse,
    )(i)?;

    // Extract possible `database` and / or `retention_policy`
    let (database, retention_policy) = match opt_db_rp {
        Some(db_rp) => db_rp,
        _ => (None, None),
    };

    Ok((
        remaining_input,
        QualifiedMeasurementName {
            database,
            retention_policy,
            name,
        },
    ))
}

/// Parse a SQL-style single-line comment
fn comment_single_line(i: &str) -> ParseResult<&str, &str> {
    recognize(pair(tag("--"), is_not("\n\r")))(i)
}

/// Parse a SQL-style inline comment, which can span multiple lines
fn comment_inline(i: &str) -> ParseResult<&str, &str> {
    recognize(delimited(
        tag("/*"),
        expect(
            "invalid inline comment, missing closing */",
            take_until("*/"),
        ),
        tag("*/"),
    ))(i)
}

/// Repeats the embedded parser until it fails, discarding the results.
///
/// This parser is used as a non-allocating version of [`nom::multi::many0`].
fn many0_<'a, A, F>(mut f: F) -> impl FnMut(&'a str) -> ParseResult<&'a str, ()>
where
    F: FnMut(&'a str) -> ParseResult<&'a str, A>,
{
    move |i| fold_many0(&mut f, || (), |_, _| ())(i)
}

/// Optionally consume all whitespace, single-line or inline comments
pub(crate) fn ws0(i: &str) -> ParseResult<&str, ()> {
    many0_(alt((multispace1, comment_single_line, comment_inline)))(i)
}

/// Runs the embedded parser until it fails, discarding the results.
/// Fails if the embedded parser does not produce at least one result.
///
/// This parser is used as a non-allocating version of [`nom::multi::many1`].
fn many1_<'a, A, F>(mut f: F) -> impl FnMut(&'a str) -> ParseResult<&'a str, ()>
where
    F: FnMut(&'a str) -> ParseResult<&'a str, A>,
{
    move |i| fold_many1(&mut f, || (), |_, _| ())(i)
}

/// Must consume either whitespace, single-line or inline comments
pub(crate) fn ws1(i: &str) -> ParseResult<&str, ()> {
    many1_(alt((multispace1, comment_single_line, comment_inline)))(i)
}

/// Implements common behaviour for u64 tuple-struct types
#[macro_export]
macro_rules! impl_tuple_clause {
    ($NAME:ident, $FOR:ty) => {
        impl $NAME {
            /// Create a new instance with the specified value.
            pub fn new(value: $FOR) -> Self {
                Self(value)
            }
        }

        impl std::ops::DerefMut for $NAME {
            fn deref_mut(&mut self) -> &mut Self::Target {
                &mut self.0
            }
        }

        impl std::ops::Deref for $NAME {
            type Target = $FOR;

            fn deref(&self) -> &Self::Target {
                &self.0
            }
        }

        impl From<$FOR> for $NAME {
            fn from(value: $FOR) -> Self {
                Self(value)
            }
        }
    };
}

/// Represents the value for a `LIMIT` clause.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LimitClause(pub(crate) u64);

impl_tuple_clause!(LimitClause, u64);

impl Display for LimitClause {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "LIMIT {}", self.0)
    }
}

/// Parse a `LIMIT <n>` clause.
pub(crate) fn limit_clause(i: &str) -> ParseResult<&str, LimitClause> {
    preceded(
        pair(keyword("LIMIT"), ws1),
        expect(
            "invalid LIMIT clause, expected unsigned integer",
            map(unsigned_integer, LimitClause),
        ),
    )(i)
}

/// Represents the value for a `OFFSET` clause.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OffsetClause(pub(crate) u64);

impl_tuple_clause!(OffsetClause, u64);

impl Display for OffsetClause {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "OFFSET {}", self.0)
    }
}

/// Parse an `OFFSET <n>` clause.
pub(crate) fn offset_clause(i: &str) -> ParseResult<&str, OffsetClause> {
    preceded(
        pair(keyword("OFFSET"), ws1),
        expect(
            "invalid OFFSET clause, expected unsigned integer",
            map(unsigned_integer, OffsetClause),
        ),
    )(i)
}

/// Parse a terminator that ends a SQL statement.
pub(crate) fn statement_terminator(i: &str) -> ParseResult<&str, ()> {
    value((), char(';'))(i)
}

/// Represents the `WHERE` clause of a statement.
#[derive(Debug, Clone, PartialEq)]
pub struct WhereClause(pub(crate) ConditionalExpression);

impl WhereClause {
    /// Create an instance of a `WhereClause` using `expr`
    pub fn new(expr: ConditionalExpression) -> Self {
        Self(expr)
    }
}

impl DerefMut for WhereClause {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl Deref for WhereClause {
    type Target = ConditionalExpression;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Display for WhereClause {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "WHERE {}", self.0)
    }
}

/// Parse a `WHERE` clause.
pub(crate) fn where_clause(i: &str) -> ParseResult<&str, WhereClause> {
    preceded(
        pair(keyword("WHERE"), ws0),
        map(conditional_expression, WhereClause),
    )(i)
}

/// Represents an InfluxQL `ORDER BY` clause.
#[derive(Default, Debug, Clone, Copy, Eq, PartialEq)]
pub enum OrderByClause {
    /// Signals the `ORDER BY` is in ascending order.
    #[default]
    Ascending,

    /// Signals the `ORDER BY` is in descending order.
    Descending,
}

impl Display for OrderByClause {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ORDER BY TIME {}",
            match self {
                Self::Ascending => "ASC",
                Self::Descending => "DESC",
            }
        )
    }
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
pub(crate) fn order_by_clause(i: &str) -> ParseResult<&str, OrderByClause> {
    let order = || {
        preceded(
            ws1,
            alt((
                value(OrderByClause::Ascending, keyword("ASC")),
                value(OrderByClause::Descending, keyword("DESC")),
            )),
        )
    };

    preceded(
        // "ORDER" "BY"
        pair(keyword("ORDER"), preceded(ws1, keyword("BY"))),
        expect(
            "invalid ORDER BY, expected ASC, DESC or TIME",
            alt((
                // "ASC" | "DESC"
                order(),
                // "TIME" ( "ASC" | "DESC" )?
                map(
                    preceded(
                        preceded(
                            ws1,
                            verify("invalid ORDER BY, expected TIME column", identifier, |v| {
                                Token(&v.0) == Token("time")
                            }),
                        ),
                        opt(order()),
                    ),
                    Option::<_>::unwrap_or_default,
                ),
            )),
        ),
    )(i)
}

/// Parser is a trait that allows a type to parse itself.
pub trait Parser: Sized {
    /// Parse this type from the string `i`.
    fn parse(i: &str) -> ParseResult<&str, Self>;
}

/// `OneOrMore` is a container for representing a minimum of one `T`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OneOrMore<T> {
    pub(crate) contents: Vec<T>,
}

#[allow(clippy::len_without_is_empty)]
impl<T> OneOrMore<T> {
    /// Construct a new `OneOrMore<T>` with `contents`.
    ///
    /// **NOTE:** that `new` panics if contents is empty.
    pub fn new(contents: Vec<T>) -> Self {
        if contents.is_empty() {
            panic!("OneOrMore requires elements");
        }

        Self { contents }
    }

    /// Returns the first element.
    pub fn head(&self) -> &T {
        self.contents.first().unwrap()
    }

    /// Returns the remaining elements after [Self::head].
    pub fn tail(&self) -> &[T] {
        &self.contents[1..]
    }

    /// Returns the total number of elements.
    /// Note that `len` ≥ 1.
    pub fn len(&self) -> usize {
        self.contents.len()
    }
}

impl<T> Deref for OneOrMore<T> {
    type Target = [T];

    fn deref(&self) -> &Self::Target {
        &self.contents
    }
}

impl<T: Parser> OneOrMore<T> {
    /// Parse a list of one or more `T`, separated by commas.
    ///
    /// Returns an error using `msg` if `separated_list1` fails to parse any elements.
    pub(crate) fn separated_list1<'a>(
        msg: &'static str,
    ) -> impl FnMut(&'a str) -> ParseResult<&'a str, Self> {
        move |i: &str| {
            map(
                expect(
                    msg,
                    separated_list1(preceded(ws0, char(',')), preceded(ws0, T::parse)),
                ),
                Self::new,
            )(i)
        }
    }
}

/// `ZeroOrMore` is a container for representing zero or more elements of type `T`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ZeroOrMore<T> {
    pub(crate) contents: Vec<T>,
}

impl<T> ZeroOrMore<T> {
    /// Construct a new `ZeroOrMore<T>` with `contents`.
    pub fn new(contents: Vec<T>) -> Self {
        Self { contents }
    }

    /// Returns the first element or `None` if the container is empty.
    pub fn head(&self) -> Option<&T> {
        self.contents.first()
    }

    /// Returns the remaining elements after [Self::head].
    pub fn tail(&self) -> &[T] {
        if self.contents.len() < 2 {
            &[]
        } else {
            &self.contents[1..]
        }
    }

    /// Returns the total number of elements in the container.
    pub fn len(&self) -> usize {
        self.contents.len()
    }

    /// Returns true if the container has no elements.
    pub fn is_empty(&self) -> bool {
        self.contents.is_empty()
    }
}

impl<T> Deref for ZeroOrMore<T> {
    type Target = [T];

    fn deref(&self) -> &Self::Target {
        &self.contents
    }
}

impl<T: Parser> ZeroOrMore<T> {
    /// Parse a list of one or more `T`, separated by commas.
    ///
    /// Returns an error using `msg` if `separated_list1` fails to parse any elements.
    pub(crate) fn separated_list1<'a>(
        msg: &'static str,
    ) -> impl FnMut(&'a str) -> ParseResult<&'a str, Self> {
        move |i: &str| {
            map(
                expect(
                    msg,
                    separated_list1(preceded(ws0, char(',')), preceded(ws0, T::parse)),
                ),
                Self::new,
            )(i)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{assert_error, assert_expect_error};
    use assert_matches::assert_matches;
    use nom::character::complete::alphanumeric1;

    impl From<&str> for MeasurementName {
        /// Convert a `str` to [`MeasurementName::Name`].
        fn from(s: &str) -> Self {
            Self::Name(Identifier::new(s.into()))
        }
    }

    impl QualifiedMeasurementName {
        /// Constructs a new `MeasurementNameExpression` with the specified `name`.
        pub fn new(name: MeasurementName) -> Self {
            Self {
                database: None,
                retention_policy: None,
                name,
            }
        }

        /// Constructs a new `MeasurementNameExpression` with the specified `name` and `database`.
        pub fn new_db(name: MeasurementName, database: Identifier) -> Self {
            Self {
                database: Some(database),
                retention_policy: None,
                name,
            }
        }

        /// Constructs a new `MeasurementNameExpression` with the specified `name`, `database` and `retention_policy`.
        pub fn new_db_rp(
            name: MeasurementName,
            database: Identifier,
            retention_policy: Identifier,
        ) -> Self {
            Self {
                database: Some(database),
                retention_policy: Some(retention_policy),
                name,
            }
        }
    }

    #[test]
    fn test_qualified_measurement_name() {
        use MeasurementName::*;

        let (_, got) = qualified_measurement_name("diskio").unwrap();
        assert_eq!(
            got,
            QualifiedMeasurementName {
                database: None,
                retention_policy: None,
                name: Name("diskio".into()),
            }
        );

        let (_, got) = qualified_measurement_name("/diskio/").unwrap();
        assert_eq!(
            got,
            QualifiedMeasurementName {
                database: None,
                retention_policy: None,
                name: Regex("diskio".into()),
            }
        );

        let (_, got) = qualified_measurement_name("telegraf.autogen.diskio").unwrap();
        assert_eq!(
            got,
            QualifiedMeasurementName {
                database: Some("telegraf".into()),
                retention_policy: Some("autogen".into()),
                name: Name("diskio".into()),
            }
        );

        let (_, got) = qualified_measurement_name("telegraf.autogen./diskio/").unwrap();
        assert_eq!(
            got,
            QualifiedMeasurementName {
                database: Some("telegraf".into()),
                retention_policy: Some("autogen".into()),
                name: Regex("diskio".into()),
            }
        );

        let (_, got) = qualified_measurement_name("telegraf..diskio").unwrap();
        assert_eq!(
            got,
            QualifiedMeasurementName {
                database: Some("telegraf".into()),
                retention_policy: None,
                name: Name("diskio".into()),
            }
        );

        let (_, got) = qualified_measurement_name("telegraf../diskio/").unwrap();
        assert_eq!(
            got,
            QualifiedMeasurementName {
                database: Some("telegraf".into()),
                retention_policy: None,
                name: Regex("diskio".into()),
            }
        );

        // With whitespace
        let (_, got) = qualified_measurement_name("\"telegraf\".. \"diskio\"").unwrap();
        assert_eq!(
            got,
            QualifiedMeasurementName {
                database: Some("telegraf".into()),
                retention_policy: None,
                name: Name("diskio".into()),
            }
        );

        let (_, got) =
            qualified_measurement_name("telegraf. /* a comment */  autogen. diskio").unwrap();
        assert_eq!(
            got,
            QualifiedMeasurementName {
                database: Some("telegraf".into()),
                retention_policy: Some("autogen".into()),
                name: Name("diskio".into()),
            }
        );

        // Whitespace following identifier is not supported
        let (rem, got) = qualified_measurement_name("telegraf . autogen. diskio").unwrap();
        assert_eq!(rem, " . autogen. diskio");
        assert_eq!(
            got,
            QualifiedMeasurementName {
                database: None,
                retention_policy: None,
                name: Name("telegraf".into()),
            }
        );

        // Fallible

        // Whitespace preceding regex is not supported
        qualified_measurement_name("telegraf.autogen. /diskio/").unwrap_err();
    }

    #[test]
    fn test_limit_clause() {
        let (_, got) = limit_clause("LIMIT 587").unwrap();
        assert_eq!(*got, 587);

        // case insensitive
        let (_, got) = limit_clause("limit 587").unwrap();
        assert_eq!(*got, 587);

        // extra spaces between tokens
        let (_, got) = limit_clause("LIMIT     123").unwrap();
        assert_eq!(*got, 123);

        // not digits
        assert_expect_error!(
            limit_clause("LIMIT from"),
            "invalid LIMIT clause, expected unsigned integer"
        );

        // incomplete input
        assert_expect_error!(
            limit_clause("LIMIT "),
            "invalid LIMIT clause, expected unsigned integer"
        );

        // overflow
        assert_expect_error!(
            limit_clause("LIMIT 34593745733489743985734857394"),
            "unable to parse unsigned integer"
        );
    }

    #[test]
    fn test_offset_clause() {
        let (_, got) = offset_clause("OFFSET 587").unwrap();
        assert_eq!(*got, 587);

        // case insensitive
        let (_, got) = offset_clause("offset 587").unwrap();
        assert_eq!(*got, 587);

        // extra spaces between tokens
        let (_, got) = offset_clause("OFFSET     123").unwrap();
        assert_eq!(*got, 123);

        // not digits
        assert_expect_error!(
            offset_clause("OFFSET from"),
            "invalid OFFSET clause, expected unsigned integer"
        );

        // incomplete input
        assert_expect_error!(
            offset_clause("OFFSET "),
            "invalid OFFSET clause, expected unsigned integer"
        );

        // overflow
        assert_expect_error!(
            offset_clause("OFFSET 34593745733489743985734857394"),
            "unable to parse unsigned integer"
        );
    }

    #[test]
    fn test_order_by() {
        use OrderByClause::*;

        let (_, got) = order_by_clause("ORDER by asc").unwrap();
        assert_eq!(got, Ascending);

        let (_, got) = order_by_clause("ORDER by desc").unwrap();
        assert_eq!(got, Descending);

        // "time" as a quoted identifier
        let (_, got) = order_by_clause("ORDER by \"time\" asc").unwrap();
        assert_eq!(got, Ascending);

        let (_, got) = order_by_clause("ORDER by time asc").unwrap();
        assert_eq!(got, Ascending);

        let (_, got) = order_by_clause("ORDER by time desc").unwrap();
        assert_eq!(got, Descending);

        // default case is ascending
        let (_, got) = order_by_clause("ORDER by time").unwrap();
        assert_eq!(got, Ascending);

        // case insensitive
        let (_, got) = order_by_clause("ORDER by \"TIME\"").unwrap();
        assert_eq!(got, Ascending);

        let (_, got) = order_by_clause("ORDER by Time").unwrap();
        assert_eq!(got, Ascending);

        // does not consume remaining input
        let (i, got) = order_by_clause("ORDER by time LIMIT 10").unwrap();
        assert_eq!(got, Ascending);
        assert_eq!(i, " LIMIT 10");

        // Fallible cases

        // Must be "time" identifier
        assert_expect_error!(
            order_by_clause("ORDER by foo"),
            "invalid ORDER BY, expected TIME column"
        );
    }

    #[test]
    fn test_where_clause() {
        // Can parse a WHERE clause
        where_clause("WHERE foo = 'bar'").unwrap();

        // Remaining input is not consumed
        let (i, _) = where_clause("WHERE foo = 'bar' LIMIT 10").unwrap();
        assert_eq!(i, " LIMIT 10");

        // Without unnecessary whitespace
        where_clause("WHERE(foo = 'bar')").unwrap();

        let (rem, _) = where_clause("WHERE/* a comment*/foo = 'bar'").unwrap();
        assert_eq!(rem, "");

        // Fallible cases
        where_clause("WHERE foo = LIMIT 10").unwrap_err();
        where_clause("WHERE").unwrap_err();
    }

    #[test]
    fn test_statement_terminator() {
        let (i, _) = statement_terminator(";foo").unwrap();
        assert_eq!(i, "foo");

        let (i, _) = statement_terminator("; foo").unwrap();
        assert_eq!(i, " foo");

        // Fallible cases
        statement_terminator("foo").unwrap_err();
    }

    impl Parser for String {
        fn parse(i: &str) -> ParseResult<&str, Self> {
            map(alphanumeric1, &str::to_string)(i)
        }
    }

    type OneOrMoreString = OneOrMore<String>;

    impl Display for OneOrMoreString {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            Display::fmt(self.head(), f)?;
            for arg in self.tail() {
                write!(f, ", {}", arg)?;
            }
            Ok(())
        }
    }

    #[test]
    #[should_panic(expected = "OneOrMore requires elements")]
    fn test_one_or_more() {
        let (_, got) = OneOrMoreString::separated_list1("Expects one or more")("foo").unwrap();
        assert_eq!(got.len(), 1);
        assert_eq!(got.head(), "foo");
        assert_eq!(*got, vec!["foo"]); // deref
        assert_eq!(format!("{}", got), "foo");

        let (_, got) =
            OneOrMoreString::separated_list1("Expects one or more")("foo ,  bar,foobar").unwrap();
        assert_eq!(got.len(), 3);
        assert_eq!(got.head(), "foo");
        assert_eq!(got.tail(), vec!["bar", "foobar"]);
        assert_eq!(*got, vec!["foo", "bar", "foobar"]); // deref
        assert_eq!(format!("{}", got), "foo, bar, foobar");

        // Fallible cases

        assert_expect_error!(
            OneOrMoreString::separated_list1("Expects one or more")("+"),
            "Expects one or more"
        );

        // should panic
        OneOrMoreString::new(vec![]);
    }

    type ZeroOrMoreString = ZeroOrMore<String>;

    impl Display for ZeroOrMoreString {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            if let Some(first) = self.head() {
                Display::fmt(first, f)?;
                for arg in self.tail() {
                    write!(f, ", {}", arg)?;
                }
            }

            Ok(())
        }
    }

    #[test]
    fn test_zero_or_more() {
        let (_, got) = ZeroOrMoreString::separated_list1("Expects one or more")("foo").unwrap();
        assert_eq!(got.len(), 1);
        assert_eq!(got.head().unwrap(), "foo");
        assert_eq!(*got, vec!["foo"]); // deref
        assert_eq!(format!("{}", got), "foo");

        let (_, got) =
            ZeroOrMoreString::separated_list1("Expects one or more")("foo ,  bar,foobar").unwrap();
        assert_eq!(got.len(), 3);
        assert_eq!(got.head().unwrap(), "foo");
        assert_eq!(got.tail(), vec!["bar", "foobar"]);
        assert_eq!(*got, vec!["foo", "bar", "foobar"]); // deref
        assert_eq!(format!("{}", got), "foo, bar, foobar");

        // should not panic
        let got = ZeroOrMoreString::new(vec![]);
        assert!(got.is_empty());
        assert_matches!(got.head(), None);
        assert_eq!(got.tail().len(), 0);

        // Fallible cases

        assert_expect_error!(
            OneOrMoreString::separated_list1("Expects one or more")("+"),
            "Expects one or more"
        );
    }

    #[test]
    fn test_comment_single_line() {
        // Comment to EOF
        let (rem, _) = comment_single_line("-- this is a test").unwrap();
        assert_eq!(rem, "");

        // Comment to EOL
        let (rem, _) = comment_single_line("-- this is a test\nmore text").unwrap();
        assert_eq!(rem, "\nmore text");
    }

    #[test]
    fn test_comment_inline() {
        let (rem, _) = comment_inline("/* this is a test */").unwrap();
        assert_eq!(rem, "");

        let (rem, _) = comment_inline("/* this is a test*/more text").unwrap();
        assert_eq!(rem, "more text");

        let (rem, _) = comment_inline("/* this\nis a test*/more text").unwrap();
        assert_eq!(rem, "more text");

        // Ignores embedded /*
        let (rem, _) = comment_inline("/* this /* is a test*/more text").unwrap();
        assert_eq!(rem, "more text");

        // Fallible cases

        assert_expect_error!(
            comment_inline("/* this is a test"),
            "invalid inline comment, missing closing */"
        );
    }

    #[test]
    fn test_ws0() {
        let (rem, _) = ws0("  -- this is a comment\n/* and some more*/  \t").unwrap();
        assert_eq!(rem, "");

        let (rem, _) = ws0("  -- this is a comment\n/* and some more*/  \tSELECT").unwrap();
        assert_eq!(rem, "SELECT");

        // no whitespace
        let (rem, _) = ws0("SELECT").unwrap();
        assert_eq!(rem, "SELECT");
    }

    #[test]
    fn test_ws1() {
        let (rem, _) = ws1("  -- this is a comment\n/* and some more*/  \t").unwrap();
        assert_eq!(rem, "");

        let (rem, _) = ws1("  -- this is a comment\n/* and some more*/  \tSELECT").unwrap();
        assert_eq!(rem, "SELECT");

        // Fallible cases

        // Missing whitespace
        assert_error!(ws1("SELECT"), Many1);
    }
}
