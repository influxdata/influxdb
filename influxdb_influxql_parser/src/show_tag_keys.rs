//! Types and parsers for the [`SHOW TAG KEYS`][sql] statement.
//!
//! [sql]: https://docs.influxdata.com/influxdb/v1.8/query_language/explore-schema/#show-tag-keys

use crate::common::{
    limit_clause, offset_clause, where_clause, ws1, LimitClause, OffsetClause, WhereClause,
};
use crate::internal::ParseResult;
use crate::keywords::keyword;
use crate::show::{on_clause, OnClause};
use crate::simple_from_clause::{show_from_clause, ShowFromClause};
use nom::combinator::opt;
use nom::sequence::{preceded, tuple};
use std::fmt;
use std::fmt::Formatter;

/// Represents a `SHOW TAG KEYS` InfluxQL statement.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ShowTagKeysStatement {
    /// The name of the database to query. If `None`, a default
    /// database will be used.
    pub database: Option<OnClause>,

    /// The measurement or measurements to restrict which tag keys
    /// are retrieved.
    pub from: Option<ShowFromClause>,

    /// A conditional expression to filter the tag keys.
    pub condition: Option<WhereClause>,

    /// A value to restrict the number of tag keys returned.
    pub limit: Option<LimitClause>,

    /// A value to specify an offset to start retrieving tag keys.
    pub offset: Option<OffsetClause>,
}

impl fmt::Display for ShowTagKeysStatement {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "SHOW TAG KEYS")?;

        if let Some(ref on_clause) = self.database {
            write!(f, " {on_clause}")?;
        }

        if let Some(ref expr) = self.from {
            write!(f, " {expr}")?;
        }

        if let Some(ref cond) = self.condition {
            write!(f, " {cond}")?;
        }

        if let Some(ref limit) = self.limit {
            write!(f, " {limit}")?;
        }

        if let Some(ref offset) = self.offset {
            write!(f, " {offset}")?;
        }

        Ok(())
    }
}

/// Parse a `SHOW TAG KEYS` statement, starting from the `KEYS` token.
pub(crate) fn show_tag_keys(i: &str) -> ParseResult<&str, ShowTagKeysStatement> {
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
        keyword("KEYS"),
        opt(preceded(ws1, on_clause)),
        opt(preceded(ws1, show_from_clause)),
        opt(preceded(ws1, where_clause)),
        opt(preceded(ws1, limit_clause)),
        opt(preceded(ws1, offset_clause)),
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
        assert_eq!(got.to_string(), "SHOW TAG KEYS");

        let (_, got) = show_tag_keys("KEYS ON db").unwrap();
        assert_eq!(got.to_string(), "SHOW TAG KEYS ON db");

        // measurement selection using name
        let (_, got) = show_tag_keys("KEYS FROM db..foo").unwrap();
        assert_eq!(got.to_string(), "SHOW TAG KEYS FROM db..foo");

        // measurement selection using regex
        let (_, got) = show_tag_keys("KEYS FROM /foo/").unwrap();
        assert_eq!(got.to_string(), "SHOW TAG KEYS FROM /foo/");

        // measurement selection using list
        let (_, got) = show_tag_keys("KEYS FROM /foo/ , bar, \"foo bar\"").unwrap();
        assert_eq!(
            got.to_string(),
            "SHOW TAG KEYS FROM /foo/, bar, \"foo bar\""
        );

        let (_, got) = show_tag_keys("KEYS WHERE foo = 'bar'").unwrap();
        assert_eq!(got.to_string(), "SHOW TAG KEYS WHERE foo = 'bar'");

        let (_, got) = show_tag_keys("KEYS LIMIT 1").unwrap();
        assert_eq!(got.to_string(), "SHOW TAG KEYS LIMIT 1");

        let (_, got) = show_tag_keys("KEYS OFFSET 2").unwrap();
        assert_eq!(got.to_string(), "SHOW TAG KEYS OFFSET 2");

        // all optional clauses
        let (_, got) =
            show_tag_keys("KEYS ON db FROM /foo/ WHERE foo = 'bar' LIMIT 1 OFFSET 2").unwrap();
        assert_eq!(
            got.to_string(),
            "SHOW TAG KEYS ON db FROM /foo/ WHERE foo = 'bar' LIMIT 1 OFFSET 2"
        );

        // Fallible cases are tested by the various combinator functions
    }
}
