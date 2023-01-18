//! Types and parsers for the [`SHOW TAG VALUES`][sql] statement.
//!
//! [sql]: https://docs.influxdata.com/influxdb/v1.8/query_language/explore-schema/#show-tag-values

use crate::common::{
    limit_clause, offset_clause, where_clause, ws0, ws1, LimitClause, OffsetClause, OneOrMore,
    WhereClause,
};
use crate::identifier::{identifier, Identifier};
use crate::internal::{expect, ParseResult};
use crate::keywords::keyword;
use crate::show::{on_clause, OnClause};
use crate::simple_from_clause::{show_from_clause, ShowFromClause};
use crate::string::{regex, Regex};
use nom::branch::alt;
use nom::bytes::complete::tag;
use nom::character::complete::char;
use nom::combinator::{map, opt};
use nom::sequence::{delimited, preceded, tuple};
use std::fmt;
use std::fmt::{Display, Formatter};

/// Represents a `SHOW TAG VALUES` InfluxQL statement.
#[derive(Clone, Debug, PartialEq)]
pub struct ShowTagValuesStatement {
    /// The name of the database to query. If `None`, a default
    /// database will be used.
    pub database: Option<OnClause>,

    /// The measurement or measurements to restrict which tag keys
    /// are retrieved.
    pub from: Option<ShowFromClause>,

    /// Represents the `WITH KEY` clause, to restrict the tag values to
    /// the matching tag keys.
    pub with_key: WithKeyClause,

    /// A conditional expression to filter the tag keys.
    pub condition: Option<WhereClause>,

    /// A value to restrict the number of tag keys returned.
    pub limit: Option<LimitClause>,

    /// A value to specify an offset to start retrieving tag keys.
    pub offset: Option<OffsetClause>,
}

impl Display for ShowTagValuesStatement {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "SHOW TAG VALUES")?;

        if let Some(ref on_clause) = self.database {
            write!(f, " {}", on_clause)?;
        }

        if let Some(ref from_clause) = self.from {
            write!(f, " {}", from_clause)?;
        }

        write!(f, " {}", self.with_key)?;

        if let Some(ref where_clause) = self.condition {
            write!(f, " {}", where_clause)?;
        }

        if let Some(ref limit) = self.limit {
            write!(f, " {}", limit)?;
        }

        if let Some(ref offset) = self.offset {
            write!(f, " {}", offset)?;
        }

        Ok(())
    }
}

/// Parse a `SHOW TAG VALUES` statement, starting from the `VALUES` token.
pub(crate) fn show_tag_values(i: &str) -> ParseResult<&str, ShowTagValuesStatement> {
    let (
        remaining_input,
        (
            _, // "VALUES"
            database,
            from,
            with_key,
            condition,
            limit,
            offset,
        ),
    ) = tuple((
        keyword("VALUES"),
        opt(preceded(ws1, on_clause)),
        opt(preceded(ws1, show_from_clause)),
        expect(
            "invalid SHOW TAG VALUES statement, expected WITH KEY clause",
            preceded(ws1, with_key_clause),
        ),
        opt(preceded(ws1, where_clause)),
        opt(preceded(ws1, limit_clause)),
        opt(preceded(ws1, offset_clause)),
    ))(i)?;

    Ok((
        remaining_input,
        ShowTagValuesStatement {
            database,
            from,
            with_key,
            condition,
            limit,
            offset,
        },
    ))
}

/// Represents a list of identifiers when the `WITH KEY` clause
/// specifies the `IN` operator.
pub type InList = OneOrMore<Identifier>;

impl Display for InList {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        Display::fmt(self.head(), f)?;
        for arg in self.tail() {
            write!(f, ", {}", arg)?;
        }
        Ok(())
    }
}

/// Represents a `WITH KEY` clause.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WithKeyClause {
    /// Select a single tag key that equals the identifier.
    Eq(Identifier),
    /// Select all tag keys that do not equal the identifier.
    NotEq(Identifier),
    /// Select any tag keys that pass the regular expression.
    EqRegex(Regex),
    /// Select the tag keys that do not pass the regular expression.
    NotEqRegex(Regex),
    /// Select the tag keys matching each of the identifiers in the list.
    In(InList),
}

impl Display for WithKeyClause {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str("WITH KEY ")?;

        match self {
            Self::Eq(v) => write!(f, "= {}", v),
            Self::NotEq(v) => write!(f, "!= {}", v),
            Self::EqRegex(v) => write!(f, "=~ {}", v),
            Self::NotEqRegex(v) => write!(f, "=! {}", v),
            Self::In(list) => write!(f, "IN ({})", list),
        }
    }
}

/// Parse an identifier list, as expected by the `WITH KEY IN` clause.
fn identifier_list(i: &str) -> ParseResult<&str, InList> {
    delimited(
        preceded(ws0, char('(')),
        InList::separated_list1("invalid IN clause, expected identifier"),
        expect(
            "invalid identifier list, expected ')'",
            preceded(ws0, char(')')),
        ),
    )(i)
}

fn with_key_clause(i: &str) -> ParseResult<&str, WithKeyClause> {
    preceded(
        tuple((
            keyword("WITH"),
            ws1,
            expect("invalid WITH KEY clause, expected KEY", keyword("KEY")),
        )),
        expect(
            "invalid WITH KEY clause, expected condition",
            alt((
                map(
                    preceded(
                        delimited(ws0, tag("=~"), ws0),
                        expect(
                            "invalid WITH KEY clause, expected regular expression following =~",
                            regex,
                        ),
                    ),
                    WithKeyClause::EqRegex,
                ),
                map(
                    preceded(
                        delimited(ws0, tag("!~"), ws0),
                        expect(
                            "invalid WITH KEY clause, expected regular expression following =!",
                            regex,
                        ),
                    ),
                    WithKeyClause::NotEqRegex,
                ),
                map(
                    preceded(
                        preceded(ws0, char('=')),
                        expect(
                            "invalid WITH KEY clause, expected identifier following =",
                            identifier,
                        ),
                    ),
                    WithKeyClause::Eq,
                ),
                map(
                    preceded(
                        preceded(ws0, tag("!=")),
                        expect(
                            "invalid WITH KEY clause, expected identifier following !=",
                            identifier,
                        ),
                    ),
                    WithKeyClause::NotEq,
                ),
                map(
                    preceded(
                        preceded(ws1, tag("IN")),
                        expect(
                            "invalid WITH KEY clause, expected identifier list following IN",
                            identifier_list,
                        ),
                    ),
                    WithKeyClause::In,
                ),
            )),
        ),
    )(i)
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::assert_expect_error;

    #[test]
    fn test_show_tag_values() {
        // No optional clauses
        let (_, got) = show_tag_values("VALUES WITH KEY = some_key").unwrap();
        assert_eq!(got.to_string(), "SHOW TAG VALUES WITH KEY = some_key");

        let (_, got) = show_tag_values("VALUES ON db WITH KEY = some_key").unwrap();
        assert_eq!(got.to_string(), "SHOW TAG VALUES ON db WITH KEY = some_key");

        // measurement selection using name
        let (_, got) = show_tag_values("VALUES FROM db..foo WITH KEY = some_key").unwrap();
        assert_eq!(
            got.to_string(),
            "SHOW TAG VALUES FROM db..foo WITH KEY = some_key"
        );

        // measurement selection using regex
        let (_, got) = show_tag_values("VALUES FROM /foo/ WITH KEY = some_key").unwrap();
        assert_eq!(
            got.to_string(),
            "SHOW TAG VALUES FROM /foo/ WITH KEY = some_key"
        );

        // measurement selection using list
        let (_, got) =
            show_tag_values("VALUES FROM /foo/ , bar, \"foo bar\" WITH KEY = some_key").unwrap();
        assert_eq!(
            got.to_string(),
            "SHOW TAG VALUES FROM /foo/, bar, \"foo bar\" WITH KEY = some_key"
        );

        let (_, got) = show_tag_values("VALUES WITH KEY = some_key WHERE foo = 'bar'").unwrap();
        assert_eq!(
            got.to_string(),
            "SHOW TAG VALUES WITH KEY = some_key WHERE foo = 'bar'"
        );

        let (_, got) = show_tag_values("VALUES WITH KEY = some_key LIMIT 1").unwrap();
        assert_eq!(
            got.to_string(),
            "SHOW TAG VALUES WITH KEY = some_key LIMIT 1"
        );

        let (_, got) = show_tag_values("VALUES WITH KEY = some_key OFFSET 2").unwrap();
        assert_eq!(
            got.to_string(),
            "SHOW TAG VALUES WITH KEY = some_key OFFSET 2"
        );

        // all optional clauses
        let (_, got) = show_tag_values(
            "VALUES ON db FROM /foo/ WITH KEY = some_key WHERE foo = 'bar' LIMIT 1 OFFSET 2",
        )
        .unwrap();
        assert_eq!(
            got.to_string(),
            "SHOW TAG VALUES ON db FROM /foo/ WITH KEY = some_key WHERE foo = 'bar' LIMIT 1 OFFSET 2"
        );

        let (_, got) = show_tag_values("VALUES WITH KEY IN( foo )").unwrap();
        assert_eq!(got.to_string(), "SHOW TAG VALUES WITH KEY IN (foo)");

        // Fallible cases are tested by the various combinator functions
    }

    #[test]
    fn test_with_key_clause() {
        let (_, got) = with_key_clause("WITH KEY = foo").unwrap();
        assert_eq!(got, WithKeyClause::Eq("foo".into()));

        let (_, got) = with_key_clause("WITH KEY != foo").unwrap();
        assert_eq!(got, WithKeyClause::NotEq("foo".into()));

        let (_, got) = with_key_clause("WITH KEY =~ /foo/").unwrap();
        assert_eq!(got, WithKeyClause::EqRegex("foo".into()));

        let (_, got) = with_key_clause("WITH KEY !~ /foo/").unwrap();
        assert_eq!(got, WithKeyClause::NotEqRegex("foo".into()));

        let (_, got) = with_key_clause("WITH KEY IN (foo)").unwrap();
        assert_eq!(got, WithKeyClause::In(InList::new(vec!["foo".into()])));

        let (_, got) = with_key_clause("WITH KEY IN (foo, bar, \"foo bar\")").unwrap();
        assert_eq!(
            got,
            WithKeyClause::In(InList::new(vec![
                "foo".into(),
                "bar".into(),
                "foo bar".into()
            ]))
        );

        // Expressions are still valid when whitespace is omitted
        let (_, got) = with_key_clause("WITH KEY=foo").unwrap();
        assert_eq!(got, WithKeyClause::Eq("foo".into()));

        // Fallible cases

        assert_expect_error!(
            with_key_clause("WITH = foo"),
            "invalid WITH KEY clause, expected KEY"
        );

        assert_expect_error!(
            with_key_clause("WITH KEY"),
            "invalid WITH KEY clause, expected condition"
        );

        assert_expect_error!(
            with_key_clause("WITH KEY foo"),
            "invalid WITH KEY clause, expected condition"
        );

        assert_expect_error!(
            with_key_clause("WITH KEY = /foo/"),
            "invalid WITH KEY clause, expected identifier following ="
        );

        assert_expect_error!(
            with_key_clause("WITH KEY IN = foo"),
            "invalid WITH KEY clause, expected identifier list following IN"
        );
    }

    #[test]
    fn test_identifier_list() {
        let (_, got) = identifier_list("(foo)").unwrap();
        assert_eq!(got, InList::new(vec!["foo".into()]));

        // Test first and rest as well as removing unnecessary whitespace
        let (_, got) = identifier_list("( foo, bar,\"foo bar\" )").unwrap();
        assert_eq!(
            got,
            InList::new(vec!["foo".into(), "bar".into(), "foo bar".into()])
        );

        // Fallible cases

        assert_expect_error!(
            identifier_list("(foo"),
            "invalid identifier list, expected ')'"
        );

        assert_expect_error!(
            identifier_list("(foo bar)"),
            "invalid identifier list, expected ')'"
        );
    }
}
