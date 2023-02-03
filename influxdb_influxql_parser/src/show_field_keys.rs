//! Types and parsers for the [`SHOW FIELD KEYS`][sql] statement.
//!
//! [sql]: https://docs.influxdata.com/influxdb/v1.8/query_language/explore-schema/#show-field-keys

use crate::common::{limit_clause, offset_clause, ws1, LimitClause, OffsetClause};
use crate::internal::{expect, ParseResult};
use crate::keywords::keyword;
use crate::show::{on_clause, OnClause};
use crate::simple_from_clause::{show_from_clause, ShowFromClause};
use nom::combinator::opt;
use nom::sequence::{preceded, tuple};
use std::fmt;
use std::fmt::Formatter;

/// Represents a `SHOW FIELD KEYS` InfluxQL statement.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ShowFieldKeysStatement {
    /// The name of the database to query. If `None`, a default
    /// database will be used.
    pub database: Option<OnClause>,

    /// The measurement or measurements to restrict which field keys
    /// are retrieved.
    pub from: Option<ShowFromClause>,

    /// A value to restrict the number of field keys returned.
    pub limit: Option<LimitClause>,

    /// A value to specify an offset to start retrieving field keys.
    pub offset: Option<OffsetClause>,
}

impl fmt::Display for ShowFieldKeysStatement {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str("SHOW FIELD KEYS")?;

        if let Some(ref on_clause) = self.database {
            write!(f, " {on_clause}")?;
        }

        if let Some(ref expr) = self.from {
            write!(f, " {expr}")?;
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

/// Parse a `SHOW FIELD KEYS` statement, starting from the `FIELD` token.
pub(crate) fn show_field_keys(i: &str) -> ParseResult<&str, ShowFieldKeysStatement> {
    let (
        remaining_input,
        (
            _, // FIELD
            _, // whitespace
            _, // "KEYS"
            database,
            from,
            limit,
            offset,
        ),
    ) = tuple((
        keyword("FIELD"),
        ws1,
        expect(
            "invalid SHOW FIELD KEYS statement, expected KEYS",
            keyword("KEYS"),
        ),
        opt(preceded(ws1, on_clause)),
        opt(preceded(ws1, show_from_clause)),
        opt(preceded(ws1, limit_clause)),
        opt(preceded(ws1, offset_clause)),
    ))(i)?;

    Ok((
        remaining_input,
        ShowFieldKeysStatement {
            database,
            from,
            limit,
            offset,
        },
    ))
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::assert_expect_error;

    #[test]
    fn test_show_field_keys() {
        // No optional clauses
        let (_, got) = show_field_keys("FIELD KEYS").unwrap();
        assert_eq!(got.to_string(), "SHOW FIELD KEYS");

        let (_, got) = show_field_keys("FIELD KEYS ON db").unwrap();
        assert_eq!(got.to_string(), "SHOW FIELD KEYS ON db");

        // measurement selection using name
        let (_, got) = show_field_keys("FIELD KEYS FROM db..foo").unwrap();
        assert_eq!(got.to_string(), "SHOW FIELD KEYS FROM db..foo");

        // measurement selection using regex
        let (_, got) = show_field_keys("FIELD KEYS FROM /foo/").unwrap();
        assert_eq!(got.to_string(), "SHOW FIELD KEYS FROM /foo/");

        // measurement selection using list
        let (_, got) = show_field_keys("FIELD KEYS FROM /foo/ , bar, \"foo bar\"").unwrap();
        assert_eq!(
            got.to_string(),
            "SHOW FIELD KEYS FROM /foo/, bar, \"foo bar\""
        );

        let (_, got) = show_field_keys("FIELD KEYS LIMIT 1").unwrap();
        assert_eq!(got.to_string(), "SHOW FIELD KEYS LIMIT 1");

        let (_, got) = show_field_keys("FIELD KEYS OFFSET 2").unwrap();
        assert_eq!(got.to_string(), "SHOW FIELD KEYS OFFSET 2");

        // all optional clauses
        let (_, got) = show_field_keys("FIELD KEYS ON db FROM /foo/ LIMIT 1 OFFSET 2").unwrap();
        assert_eq!(
            got.to_string(),
            "SHOW FIELD KEYS ON db FROM /foo/ LIMIT 1 OFFSET 2"
        );

        // Fallible cases
        assert_expect_error!(
            show_field_keys("FIELD ON db"),
            "invalid SHOW FIELD KEYS statement, expected KEYS"
        );
    }
}
