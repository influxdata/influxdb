//! Types and parsers for the `FROM` clause common to `DELETE` or `SHOW` schema statements.

use crate::common::{
    qualified_measurement_name, ws1, MeasurementName, OneOrMore, Parser, QualifiedMeasurementName,
};
use crate::identifier::{identifier, Identifier};
use crate::internal::ParseResult;
use crate::keywords::keyword;
use nom::sequence::{pair, preceded};
use std::fmt;
use std::fmt::{Display, Formatter};

/// Represents a `FROM` clause of a `DELETE` or `SHOW` statement.
///
/// A `FROM` clause for a `DELETE` can only accept [`Identifier`] or regular expressions
/// for measurements names.
///
/// A `FROM` clause for a number of `SHOW` statements can accept a [`QualifiedMeasurementName`].
pub type FromMeasurementClause<U> = OneOrMore<U>;

fn from_clause<T: Parser + fmt::Display>(i: &str) -> ParseResult<&str, FromMeasurementClause<T>> {
    preceded(
        pair(keyword("FROM"), ws1),
        FromMeasurementClause::<T>::separated_list1(
            "invalid FROM clause, expected identifier or regular expression",
        ),
    )(i)
}

impl Parser for QualifiedMeasurementName {
    fn parse(i: &str) -> ParseResult<&str, Self> {
        qualified_measurement_name(i)
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
/// from_clause ::= "FROM" qualified_measurement_name ("," qualified_measurement_name)*
///
/// qualified_measurement_name ::= measurement_name |
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
pub type ShowFromClause = FromMeasurementClause<QualifiedMeasurementName>;

impl Display for ShowFromClause {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "FROM {}", self.head())?;
        for arg in self.tail() {
            write!(f, ", {arg}")?;
        }
        Ok(())
    }
}

/// Parse a `FROM` clause for various `SHOW` statements.
pub(crate) fn show_from_clause(i: &str) -> ParseResult<&str, ShowFromClause> {
    from_clause(i)
}

impl Parser for Identifier {
    fn parse(i: &str) -> ParseResult<&str, Self> {
        identifier(i)
    }
}

/// Represents a `FROM` clause for a `DELETE` statement.
pub type DeleteFromClause = FromMeasurementClause<MeasurementName>;

impl Display for DeleteFromClause {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "FROM {}", self.head())?;
        for arg in self.tail() {
            write!(f, ", {arg}")?;
        }
        Ok(())
    }
}

/// Parse a `FROM` clause for a `DELETE` statement.
pub(crate) fn delete_from_clause(i: &str) -> ParseResult<&str, DeleteFromClause> {
    from_clause(i)
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_show_from_clause() {
        use crate::common::MeasurementName::*;

        let (_, from) = show_from_clause("FROM c").unwrap();
        assert_eq!(
            from,
            ShowFromClause::new(vec![QualifiedMeasurementName::new(Name("c".into()))])
        );

        let (_, from) = show_from_clause("FROM a..c").unwrap();
        assert_eq!(
            from,
            ShowFromClause::new(vec![QualifiedMeasurementName::new_db(
                Name("c".into()),
                "a".into()
            )])
        );

        let (_, from) = show_from_clause("FROM a.b.c").unwrap();
        assert_eq!(
            from,
            ShowFromClause::new(vec![QualifiedMeasurementName::new_db_rp(
                Name("c".into()),
                "a".into(),
                "b".into()
            )])
        );

        let (_, from) = show_from_clause("FROM /reg/").unwrap();
        assert_eq!(
            from,
            ShowFromClause::new(vec![QualifiedMeasurementName::new(Regex("reg".into()))])
        );

        let (_, from) = show_from_clause("FROM c, /reg/").unwrap();
        assert_eq!(
            from,
            ShowFromClause::new(vec![
                QualifiedMeasurementName::new(Name("c".into())),
                QualifiedMeasurementName::new(Regex("reg".into()))
            ])
        );
    }

    #[test]
    fn test_delete_from_clause() {
        use crate::common::MeasurementName::*;

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
