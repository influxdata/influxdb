use crate::common::{limit_clause, offset_clause, where_clause, OneOrMore};
use crate::expression::conditional::ConditionalExpression;
use crate::identifier::{identifier, Identifier};
use crate::internal::{expect, ParseResult};
use crate::show::on_clause;
use crate::simple_from_clause::{show_from_clause, ShowFromClause};
use crate::string::{regex, Regex};
use nom::branch::alt;
use nom::bytes::complete::{tag, tag_no_case};
use nom::character::complete::{char, multispace0, multispace1};
use nom::combinator::{map, opt};
use nom::sequence::{delimited, preceded, tuple};
use std::fmt;
use std::fmt::{Display, Formatter};

/// Represents a `SHOW TAG VALUES` InfluxQL statement.
#[derive(Clone, Debug, PartialEq)]
pub struct ShowTagValuesStatement {
    /// The name of the database to query. If `None`, a default
    /// database will be used.
    pub database: Option<Identifier>,

    /// The measurement or measurements to restrict which tag keys
    /// are retrieved.
    pub from: Option<ShowFromClause>,

    /// `WITH KEY` expression, to limit the values retrieved to
    /// the matching tag keys.
    pub with_key: WithKeyExpression,

    /// A conditional expression to filter the tag keys.
    pub condition: Option<ConditionalExpression>,

    /// A value to restrict the number of tag keys returned.
    pub limit: Option<u64>,

    /// A value to specify an offset to start retrieving tag keys.
    pub offset: Option<u64>,
}

impl Display for ShowTagValuesStatement {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "SHOW TAG VALUES")?;

        if let Some(ref expr) = self.database {
            write!(f, " ON {}", expr)?;
        }

        if let Some(ref expr) = self.from {
            write!(f, " FROM {}", expr)?;
        }

        write!(f, " {}", self.with_key)?;

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

/// Parse a `SHOW TAG VALUES` statement, starting from the `VALUES` token.
pub fn show_tag_values(i: &str) -> ParseResult<&str, ShowTagValuesStatement> {
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
        tag_no_case("VALUES"),
        opt(preceded(multispace1, on_clause)),
        opt(preceded(multispace1, show_from_clause)),
        expect(
            "invalid SHOW TAG VALUES statement, expected WITH KEY clause",
            preceded(multispace1, with_key_clause),
        ),
        opt(preceded(multispace1, where_clause)),
        opt(preceded(multispace1, limit_clause)),
        opt(preceded(multispace1, offset_clause)),
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

pub type InList = OneOrMore<Identifier>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WithKeyExpression {
    Eq(Identifier),
    NotEq(Identifier),
    EqRegex(Regex),
    NotEqRegex(Regex),
    /// IN expression
    In(InList),
}

impl Display for WithKeyExpression {
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
        preceded(multispace0, char('(')),
        InList::separated_list1("invalid IN clause, expected identifier"),
        expect(
            "invalid identifier list, expected ')'",
            preceded(multispace0, char(')')),
        ),
    )(i)
}

fn with_key_clause(i: &str) -> ParseResult<&str, WithKeyExpression> {
    preceded(
        tuple((
            tag_no_case("WITH"),
            multispace1,
            expect("invalid WITH KEY clause, expected KEY", tag_no_case("KEY")),
        )),
        expect(
            "invalid WITH KEY clause, expected condition",
            alt((
                map(
                    preceded(
                        delimited(multispace0, tag("=~"), multispace0),
                        expect(
                            "invalid WITH KEY clause, expected regular expression following =~",
                            regex,
                        ),
                    ),
                    WithKeyExpression::EqRegex,
                ),
                map(
                    preceded(
                        delimited(multispace0, tag("!~"), multispace0),
                        expect(
                            "invalid WITH KEY clause, expected regular expression following =!",
                            regex,
                        ),
                    ),
                    WithKeyExpression::NotEqRegex,
                ),
                map(
                    preceded(
                        delimited(multispace0, char('='), multispace0),
                        expect(
                            "invalid WITH KEY clause, expected identifier following =",
                            identifier,
                        ),
                    ),
                    WithKeyExpression::Eq,
                ),
                map(
                    preceded(
                        delimited(multispace0, tag("!="), multispace0),
                        expect(
                            "invalid WITH KEY clause, expected identifier following !=",
                            identifier,
                        ),
                    ),
                    WithKeyExpression::NotEq,
                ),
                map(
                    preceded(
                        preceded(multispace1, tag("IN")),
                        expect(
                            "invalid WITH KEY clause, expected identifier list following IN",
                            identifier_list,
                        ),
                    ),
                    WithKeyExpression::In,
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
        assert_eq!(format!("{}", got), "SHOW TAG VALUES WITH KEY = some_key");

        let (_, got) = show_tag_values("VALUES ON db WITH KEY = some_key").unwrap();
        assert_eq!(
            format!("{}", got),
            "SHOW TAG VALUES ON db WITH KEY = some_key"
        );

        // measurement selection using name
        let (_, got) = show_tag_values("VALUES FROM db..foo WITH KEY = some_key").unwrap();
        assert_eq!(
            format!("{}", got),
            "SHOW TAG VALUES FROM db..foo WITH KEY = some_key"
        );

        // measurement selection using regex
        let (_, got) = show_tag_values("VALUES FROM /foo/ WITH KEY = some_key").unwrap();
        assert_eq!(
            format!("{}", got),
            "SHOW TAG VALUES FROM /foo/ WITH KEY = some_key"
        );

        // measurement selection using list
        let (_, got) =
            show_tag_values("VALUES FROM /foo/ , bar, \"foo bar\" WITH KEY = some_key").unwrap();
        assert_eq!(
            format!("{}", got),
            "SHOW TAG VALUES FROM /foo/, bar, \"foo bar\" WITH KEY = some_key"
        );

        let (_, got) = show_tag_values("VALUES WITH KEY = some_key WHERE foo = 'bar'").unwrap();
        assert_eq!(
            format!("{}", got),
            "SHOW TAG VALUES WITH KEY = some_key WHERE foo = 'bar'"
        );

        let (_, got) = show_tag_values("VALUES WITH KEY = some_key LIMIT 1").unwrap();
        assert_eq!(
            format!("{}", got),
            "SHOW TAG VALUES WITH KEY = some_key LIMIT 1"
        );

        let (_, got) = show_tag_values("VALUES WITH KEY = some_key OFFSET 2").unwrap();
        assert_eq!(
            format!("{}", got),
            "SHOW TAG VALUES WITH KEY = some_key OFFSET 2"
        );

        // all optional clauses
        let (_, got) = show_tag_values(
            "VALUES ON db FROM /foo/ WITH KEY = some_key WHERE foo = 'bar' LIMIT 1 OFFSET 2",
        )
        .unwrap();
        assert_eq!(
            format!("{}", got),
            "SHOW TAG VALUES ON db FROM /foo/ WITH KEY = some_key WHERE foo = 'bar' LIMIT 1 OFFSET 2"
        );

        let (_, got) = show_tag_values("VALUES WITH KEY IN( foo )").unwrap();
        assert_eq!(format!("{}", got), "SHOW TAG VALUES WITH KEY IN (foo)");

        // Fallible cases are tested by the various combinator functions
    }

    #[test]
    fn test_with_key_clause() {
        let (_, got) = with_key_clause("WITH KEY = foo").unwrap();
        assert_eq!(got, WithKeyExpression::Eq("foo".into()));

        let (_, got) = with_key_clause("WITH KEY != foo").unwrap();
        assert_eq!(got, WithKeyExpression::NotEq("foo".into()));

        let (_, got) = with_key_clause("WITH KEY =~ /foo/").unwrap();
        assert_eq!(got, WithKeyExpression::EqRegex("foo".into()));

        let (_, got) = with_key_clause("WITH KEY !~ /foo/").unwrap();
        assert_eq!(got, WithKeyExpression::NotEqRegex("foo".into()));

        let (_, got) = with_key_clause("WITH KEY IN (foo)").unwrap();
        assert_eq!(got, WithKeyExpression::In(InList::new(vec!["foo".into()])));

        let (_, got) = with_key_clause("WITH KEY IN (foo, bar, \"foo bar\")").unwrap();
        assert_eq!(
            got,
            WithKeyExpression::In(InList::new(vec![
                "foo".into(),
                "bar".into(),
                "foo bar".into()
            ]))
        );

        // Expressions are still valid when whitespace is omitted
        let (_, got) = with_key_clause("WITH KEY=foo").unwrap();
        assert_eq!(got, WithKeyExpression::Eq("foo".into()));

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
