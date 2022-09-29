use crate::common::{measurement_name_expression, MeasurementNameExpression, OneOrMore, Parser};
use crate::identifier::{identifier, Identifier};
use crate::internal::ParseResult;
use crate::string::{regex, Regex};
use nom::branch::alt;
use nom::bytes::complete::tag_no_case;
use nom::character::complete::multispace1;
use nom::combinator::map;
use nom::sequence::{pair, preceded};
use std::fmt;
use std::fmt::Formatter;

/// Represents a single measurement selection found in a `FROM` measurement clause.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MeasurementSelection<T: Parser> {
    Name(T),
    Regex(Regex),
}

impl<T: Parser> Parser for MeasurementSelection<T> {
    fn parse(i: &str) -> ParseResult<&str, Self> {
        alt((
            map(T::parse, MeasurementSelection::Name),
            map(regex, MeasurementSelection::Regex),
        ))(i)
    }
}

impl<T: fmt::Display + Parser> fmt::Display for MeasurementSelection<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Name(ref name) => fmt::Display::fmt(name, f)?,
            Self::Regex(ref re) => fmt::Display::fmt(re, f)?,
        };

        Ok(())
    }
}

/// Represents a `FROM` clause of a `DELETE` or `SHOW` statement.
///
/// A `FROM` clause for a `DELETE` can only accept [`Identifier`] or regular expressions
/// for measurements names.
///
/// A `FROM` clause for a number of `SHOW` statements can accept a 3-part measurement name or
pub type FromMeasurementClause<U> = OneOrMore<MeasurementSelection<U>>;

fn from_clause<T: Parser + fmt::Display>(i: &str) -> ParseResult<&str, FromMeasurementClause<T>> {
    preceded(
        pair(tag_no_case("FROM"), multispace1),
        FromMeasurementClause::<T>::separated_list1(
            "invalid FROM clause, expected identifier or regular expression",
        ),
    )(i)
}

impl Parser for MeasurementNameExpression {
    fn parse(i: &str) -> ParseResult<&str, Self> {
        measurement_name_expression(i)
    }
}

/// Represents a `FROM` clause used by various `SHOW` statements.
///
/// A `FROM` clause for a `SHOW` statements differs from a `FROM` in a
/// `SELECT`, as it can only contain measurement name or regular expressions.
///
/// It is defined by the following EBNF notation:
///
/// ```text
/// from_clause ::= "FROM" measurement_selection ("," measurement_selection)*
/// measurement_selection ::= measurement
///
/// measurement      ::= measurement_name |
///                      ( policy_name "." measurement_name ) |
///                      ( db_name "." policy_name? "." measurement_name )
///
/// db_name          ::= identifier
/// measurement_name ::= identifier | regex_lit
/// policy_name      ::= identifier
/// ```
///
/// A minimal `FROM` clause would be a single identifier
///
/// ```text
/// FROM foo
/// ```
///
/// A more complicated example may include a variety of fully-qualified
/// identifiers and regular expressions
///
/// ```text
/// FROM foo, /bar/, some_database..foo, some_retention_policy.foobar
/// ```
pub type ShowFromClause = FromMeasurementClause<MeasurementNameExpression>;

/// Parse a `FROM` clause for various `SHOW` statements.
pub fn show_from_clause(i: &str) -> ParseResult<&str, ShowFromClause> {
    from_clause(i)
}

impl Parser for Identifier {
    fn parse(i: &str) -> ParseResult<&str, Self> {
        identifier(i)
    }
}

/// Represents a `FROM` clause for a `DELETE` statement.
pub type DeleteFromClause = FromMeasurementClause<Identifier>;

/// Parse a `FROM` clause for a `DELETE` statement.
pub fn delete_from_clause(i: &str) -> ParseResult<&str, DeleteFromClause> {
    from_clause(i)
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_show_from_clause() {
        use crate::simple_from_clause::MeasurementSelection::*;

        let (_, from) = show_from_clause("FROM c").unwrap();
        assert_eq!(
            from,
            ShowFromClause::new(vec![Name(MeasurementNameExpression::new("c".into()))])
        );

        let (_, from) = show_from_clause("FROM a..c").unwrap();
        assert_eq!(
            from,
            ShowFromClause::new(vec![Name(MeasurementNameExpression::new_db(
                "c".into(),
                "a".into()
            ))])
        );

        let (_, from) = show_from_clause("FROM a.b.c").unwrap();
        assert_eq!(
            from,
            ShowFromClause::new(vec![Name(MeasurementNameExpression::new_db_rp(
                "c".into(),
                "a".into(),
                "b".into()
            ))])
        );

        let (_, from) = show_from_clause("FROM /reg/").unwrap();
        assert_eq!(from, ShowFromClause::new(vec![Regex("reg".into())]));

        let (_, from) = show_from_clause("FROM c, /reg/").unwrap();
        assert_eq!(
            from,
            ShowFromClause::new(vec![
                Name(MeasurementNameExpression::new("c".into())),
                Regex("reg".into())
            ])
        );
    }

    #[test]
    fn test_delete_from_clause() {
        use crate::simple_from_clause::MeasurementSelection::*;

        let (_, from) = delete_from_clause("FROM c").unwrap();
        assert_eq!(from, DeleteFromClause::new(vec![Name("c".into())]));

        let (_, from) = delete_from_clause("FROM /reg/").unwrap();
        assert_eq!(from, DeleteFromClause::new(vec![Regex("reg".into())]));

        let (_, from) = delete_from_clause("FROM c, /reg/").unwrap();
        assert_eq!(
            from,
            DeleteFromClause::new(vec![Name("c".into()), Regex("reg".into())])
        );

        // Demonstrate that the 3-part name is not parsed
        let (i, from) = delete_from_clause("FROM a.b.c").unwrap();
        assert_eq!(from, DeleteFromClause::new(vec![Name("a".into())]));
        // The remaining input will fail in a later parser
        assert_eq!(i, ".b.c");
    }
}
