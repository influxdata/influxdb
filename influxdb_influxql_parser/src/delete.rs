use crate::common::where_clause;
use crate::expression::conditional::ConditionalExpression;
use crate::internal::{expect, ParseResult};
use crate::simple_from_clause::{delete_from_clause, DeleteFromClause};
use nom::branch::alt;
use nom::bytes::complete::tag_no_case;
use nom::character::complete::{multispace0, multispace1};
use nom::combinator::{map, opt};
use nom::sequence::{pair, preceded};
use std::fmt::{Display, Formatter};

#[derive(Clone, Debug, PartialEq)]
pub enum DeleteStatement {
    /// A DELETE with a measurement or measurements and an optional conditional expression
    /// to restrict which series are deleted.
    FromWhere {
        from: DeleteFromClause,
        condition: Option<ConditionalExpression>,
    },

    /// A `DELETE` with a conditional expression to restrict which series are deleted.
    Where(ConditionalExpression),
}

impl Display for DeleteStatement {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "DELETE")?;

        match self {
            Self::FromWhere { from, condition } => {
                write!(f, " FROM {}", from)?;
                if let Some(condition) = condition {
                    write!(f, " WHERE {}", condition)?;
                }
            }
            Self::Where(condition) => write!(f, " WHERE {}", condition)?,
        };

        Ok(())
    }
}

/// Parse a `DELETE` statement.
pub fn delete_statement(i: &str) -> ParseResult<&str, DeleteStatement> {
    // delete ::=  "DELETE" ( from_clause where_clause? | where_clause )
    preceded(
        tag_no_case("DELETE"),
        expect(
            "invalid DELETE statement, expected FROM or WHERE",
            preceded(
                multispace1,
                alt((
                    // delete ::= from_clause where_clause?
                    map(
                        pair(delete_from_clause, opt(preceded(multispace0, where_clause))),
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

        let (_, got) = delete_statement("DELETE FROM foo").unwrap();
        assert_eq!(format!("{}", got), "DELETE FROM foo");

        let (_, got) = delete_statement("DELETE FROM foo WHERE time > 10").unwrap();
        assert_eq!(format!("{}", got), "DELETE FROM foo WHERE time > 10");

        let (_, got) = delete_statement("DELETE WHERE time > 10").unwrap();
        assert_eq!(format!("{}", got), "DELETE WHERE time > 10");

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
