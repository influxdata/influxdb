use crate::expression::conditional::{conditional_expression, ConditionalExpression};
use crate::identifier::{identifier, Identifier};
use crate::internal::{expect, ParseResult};
use crate::literal::unsigned_integer;
use crate::string::{regex, Regex};
use core::fmt;
use nom::branch::alt;
use nom::bytes::complete::{tag, tag_no_case};
use nom::character::complete::{char, multispace0, multispace1};
use nom::combinator::{map, opt, value};
use nom::multi::separated_list1;
use nom::sequence::{pair, preceded, terminated};
use std::fmt::{Display, Formatter};

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
    pub database: Option<Identifier>,
    pub retention_policy: Option<Identifier>,
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
pub fn qualified_measurement_name(i: &str) -> ParseResult<&str, QualifiedMeasurementName> {
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

/// Parse a `LIMIT <n>` clause.
pub fn limit_clause(i: &str) -> ParseResult<&str, u64> {
    preceded(
        pair(tag_no_case("LIMIT"), multispace1),
        expect(
            "invalid LIMIT clause, expected unsigned integer",
            unsigned_integer,
        ),
    )(i)
}

/// Parse an `OFFSET <n>` clause.
pub fn offset_clause(i: &str) -> ParseResult<&str, u64> {
    preceded(
        pair(tag_no_case("OFFSET"), multispace1),
        expect(
            "invalid OFFSET clause, expected unsigned integer",
            unsigned_integer,
        ),
    )(i)
}

/// Parse a terminator that ends a SQL statement.
pub fn statement_terminator(i: &str) -> ParseResult<&str, ()> {
    value((), char(';'))(i)
}

/// Parse a `WHERE` clause.
pub fn where_clause(i: &str) -> ParseResult<&str, ConditionalExpression> {
    preceded(
        pair(tag_no_case("WHERE"), multispace1),
        conditional_expression,
    )(i)
}

/// Represents an InfluxQL `ORDER BY` clause.
#[derive(Default, Debug, Clone, Copy, Eq, PartialEq)]
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
pub fn order_by_clause(i: &str) -> ParseResult<&str, OrderByClause> {
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
        expect(
            "invalid ORDER BY, expected ASC, DESC or TIME",
            alt((
                // "ASC" | "DESC"
                order(),
                // "TIME" ( "ASC" | "DESC" )?
                map(
                    preceded(preceded(multispace1, tag_no_case("TIME")), opt(order())),
                    Option::<_>::unwrap_or_default,
                ),
            )),
        ),
    )(i)
}

/// Parser is a trait that allows a type to parse itself.
pub trait Parser: Sized {
    fn parse(i: &str) -> ParseResult<&str, Self>;
}

/// `OneOrMore` is a container for representing a minimum of one `T`.
///
/// `OneOrMore` provides a default implementation of [`fmt::Display`],
/// which displays the contents separated by commas.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OneOrMore<T: Display + Parser> {
    contents: Vec<T>,
}

impl<T: Display + Parser> Display for OneOrMore<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(self.first(), f)?;
        for arg in self.rest() {
            write!(f, ", {}", arg)?;
        }
        Ok(())
    }
}

impl<T: Display + Parser> OneOrMore<T> {
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
    pub fn first(&self) -> &T {
        self.contents.first().unwrap()
    }

    /// Returns any remaining elements.
    pub fn rest(&self) -> &[T] {
        &self.contents[1..]
    }

    /// Returns the total number of elements.
    /// Note that `len` ≥ 1.
    pub fn len(&self) -> usize {
        self.contents.len()
    }
}

impl<T: Display + Parser> OneOrMore<T> {
    /// Parse a list of one or more `T`, separated by commas.
    ///
    /// Returns an error using `msg` if `separated_list1` fails to parse any elements.
    pub fn separated_list1<'a>(
        msg: &'static str,
    ) -> impl FnMut(&'a str) -> ParseResult<&'a str, Self> {
        move |i: &str| {
            map(
                expect(
                    msg,
                    separated_list1(
                        preceded(multispace0, char(',')),
                        preceded(multispace0, T::parse),
                    ),
                ),
                Self::new,
            )(i)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::assert_expect_error;
    use nom::character::complete::alphanumeric1;

    impl From<&str> for MeasurementName {
        /// Convert a `str` to [`MeasurementName::Name`].
        fn from(s: &str) -> Self {
            Self::Name(Identifier(s.into()))
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
        assert_eq!(got, 587);

        // case insensitive
        let (_, got) = offset_clause("offset 587").unwrap();
        assert_eq!(got, 587);

        // extra spaces between tokens
        let (_, got) = offset_clause("OFFSET     123").unwrap();
        assert_eq!(got, 123);

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
        assert_expect_error!(
            order_by_clause("ORDER by foo"),
            "invalid ORDER BY, expected ASC, DESC or TIME"
        );
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

    #[test]
    #[should_panic(expected = "OneOrMore requires elements")]
    fn test_one_or_more() {
        let (_, got) = OneOrMoreString::separated_list1("Expects one or more")("foo").unwrap();
        assert_eq!(got.len(), 1);
        assert_eq!(got.first(), "foo");
        assert_eq!(format!("{}", got), "foo");

        let (_, got) =
            OneOrMoreString::separated_list1("Expects one or more")("foo ,  bar,foobar").unwrap();
        assert_eq!(got.len(), 3);
        assert_eq!(got.first(), "foo");
        assert_eq!(got.rest(), vec!["bar", "foobar"]);
        assert_eq!(format!("{}", got), "foo, bar, foobar");

        // Fallible cases

        assert_expect_error!(
            OneOrMoreString::separated_list1("Expects one or more")("+"),
            "Expects one or more"
        );

        // should panic
        OneOrMoreString::new(vec![]);
    }
}
