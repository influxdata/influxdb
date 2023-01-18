//! Types and parsers for the [`DELETE`][sql] statement.
//!
//! [sql]: https://docs.influxdata.com/influxdb/v1.8/query_language/manage-database/#delete-series-with-delete

use crate::common::{where_clause, ws0, ws1, WhereClause};
use crate::internal::{expect, ParseResult};
use crate::keywords::keyword;
use crate::simple_from_clause::{delete_from_clause, DeleteFromClause};
use nom::branch::alt;
use nom::combinator::{map, opt};
use nom::sequence::{pair, preceded};
use std::fmt::{Display, Formatter};

/// Represents a `DELETE` statement.
#[derive(Clone, Debug, PartialEq)]
pub enum DeleteStatement {
    /// A DELETE with a `FROM` clause specifying one or more measurements
    /// and an optional `WHERE` clause to restrict which series are deleted.
    FromWhere {
        /// Represents the `FROM` clause.
        from: DeleteFromClause,

        /// Represents the optional `WHERE` clause.
        condition: Option<WhereClause>,
    },

    /// A `DELETE` with a `WHERE` clause to restrict which series are deleted.
    Where(WhereClause),
}

impl Display for DeleteStatement {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "DELETE")?;

        match self {
            Self::FromWhere { from, condition } => {
                write!(f, " {}", from)?;
                if let Some(where_clause) = condition {
                    write!(f, " {}", where_clause)?;
                }
            }
            Self::Where(where_clause) => write!(f, " {}", where_clause)?,
        };

        Ok(())
    }
}

/// Parse a `DELETE` statement.
pub(crate) fn delete_statement(i: &str) -> ParseResult<&str, DeleteStatement> {
    // delete ::=  "DELETE" ( from_clause where_clause? | where_clause )
    preceded(
        keyword("DELETE"),
        expect(
            "invalid DELETE statement, expected FROM or WHERE",
            preceded(
                ws1,
                alt((
                    // delete ::= from_clause where_clause?
                    map(
                        pair(delete_from_clause, opt(preceded(ws0, where_clause))),
                        |(from, condition)| DeleteStatement::FromWhere { from, condition },
                    ),
                    // delete ::= where_clause
                    map(where_clause, DeleteStatement::Where),
                )),
            ),
        ),
    )(i)
}

#[cfg(test)]
mod test {
    use crate::assert_expect_error;
    use crate::delete::delete_statement;

    #[test]
    fn test_delete() {
        // Validate via the Display trait, as we don't need to validate the contents of the
        // FROM and / or WHERE clauses, given they are tested in their on modules.

        // Measurement name expressed as an identifier
        let (_, got) = delete_statement("DELETE FROM foo").unwrap();
        assert_eq!(got.to_string(), "DELETE FROM foo");

        // Measurement name expressed as a regular expression
        let (_, got) = delete_statement("DELETE FROM /foo/").unwrap();
        assert_eq!(got.to_string(), "DELETE FROM /foo/");

        let (_, got) = delete_statement("DELETE FROM foo WHERE time > 10").unwrap();
        assert_eq!(got.to_string(), "DELETE FROM foo WHERE time > 10");

        let (_, got) = delete_statement("DELETE WHERE time > 10").unwrap();
        assert_eq!(got.to_string(), "DELETE WHERE time > 10");

        // Fallible cases
        assert_expect_error!(
            delete_statement("DELETE"),
            "invalid DELETE statement, expected FROM or WHERE"
        );

        assert_expect_error!(
            delete_statement("DELETE FOO"),
            "invalid DELETE statement, expected FROM or WHERE"
        );
    }
}
