use crate::common::{limit_clause, offset_clause, where_clause};
use crate::expression::conditional::ConditionalExpression;
use crate::identifier::Identifier;
use crate::internal::ParseResult;
use crate::show::on_clause;
use crate::simple_from_clause::{show_from_clause, ShowFromClause};
use nom::bytes::complete::tag_no_case;
use nom::character::complete::multispace1;
use nom::combinator::opt;
use nom::sequence::{preceded, tuple};
use std::fmt;
use std::fmt::Formatter;

/// Represents a `SHOW TAG KEYS` InfluxQL statement.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ShowTagKeysStatement {
    /// The name of the database to query. If `None`, a default
    /// database will be used.
    pub database: Option<Identifier>,

    /// The measurement or measurements to restrict which tag keys
    /// are retrieved.
    pub from: Option<ShowFromClause>,

    /// A conditional expression to filter the tag keys.
    pub condition: Option<ConditionalExpression>,

    /// A value to restrict the number of tag keys returned.
    pub limit: Option<u64>,

    /// A value to specify an offset to start retrieving tag keys.
    pub offset: Option<u64>,
}

impl fmt::Display for ShowTagKeysStatement {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "SHOW TAG KEYS")?;

        if let Some(ref expr) = self.database {
            write!(f, " ON {}", expr)?;
        }

        if let Some(ref expr) = self.from {
            write!(f, " FROM {}", expr)?;
        }

        if let Some(ref cond) = self.condition {
            write!(f, " WHERE {}", cond)?;
        }

        if let Some(limit) = self.limit {
            write!(f, " LIMIT {}", limit)?;
        }

        if let Some(offset) = self.offset {
            write!(f, " OFFSET {}", offset)?;
        }

        Ok(())
    }
}

/// Parse a `SHOW TAG KEYS` statement, starting from the `KEYS` token.
pub fn show_tag_keys(i: &str) -> ParseResult<&str, ShowTagKeysStatement> {
    let (
        remaining_input,
        (
            _, // "KEYS"
            database,
            from,
            condition,
            limit,
            offset,
        ),
    ) = tuple((
        tag_no_case("KEYS"),
        opt(preceded(multispace1, on_clause)),
        opt(preceded(multispace1, show_from_clause)),
        opt(preceded(multispace1, where_clause)),
        opt(preceded(multispace1, limit_clause)),
        opt(preceded(multispace1, offset_clause)),
    ))(i)?;

    Ok((
        remaining_input,
        ShowTagKeysStatement {
            database,
            from,
            condition,
            limit,
            offset,
        },
    ))
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_show_tag_keys() {
        // No optional clauses
        let (_, got) = show_tag_keys("KEYS").unwrap();
        assert_eq!(format!("{}", got), "SHOW TAG KEYS");

        let (_, got) = show_tag_keys("KEYS ON db").unwrap();
        assert_eq!(format!("{}", got), "SHOW TAG KEYS ON db");

        // measurement selection using name
        let (_, got) = show_tag_keys("KEYS FROM db..foo").unwrap();
        assert_eq!(format!("{}", got), "SHOW TAG KEYS FROM db..foo");

        // measurement selection using regex
        let (_, got) = show_tag_keys("KEYS FROM /foo/").unwrap();
        assert_eq!(format!("{}", got), "SHOW TAG KEYS FROM /foo/");

        // measurement selection using list
        let (_, got) = show_tag_keys("KEYS FROM /foo/ , bar, \"foo bar\"").unwrap();
        assert_eq!(
            format!("{}", got),
            "SHOW TAG KEYS FROM /foo/, bar, \"foo bar\""
        );

        let (_, got) = show_tag_keys("KEYS WHERE foo = 'bar'").unwrap();
        assert_eq!(format!("{}", got), "SHOW TAG KEYS WHERE foo = 'bar'");

        let (_, got) = show_tag_keys("KEYS LIMIT 1").unwrap();
        assert_eq!(format!("{}", got), "SHOW TAG KEYS LIMIT 1");

        let (_, got) = show_tag_keys("KEYS OFFSET 2").unwrap();
        assert_eq!(format!("{}", got), "SHOW TAG KEYS OFFSET 2");

        // all optional clauses
        let (_, got) =
            show_tag_keys("KEYS ON db FROM /foo/ WHERE foo = 'bar' LIMIT 1 OFFSET 2").unwrap();
        assert_eq!(
            format!("{}", got),
            "SHOW TAG KEYS ON db FROM /foo/ WHERE foo = 'bar' LIMIT 1 OFFSET 2"
        );

        // Fallible cases are tested by the various combinator functions
    }
}
