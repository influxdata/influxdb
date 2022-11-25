//! # Parse InfluxQL [keywords]
//!
//! [keywords]: https://docs.influxdata.com/influxdb/v1.8/query_language/spec/#keywords

use crate::internal::ParseResult;
use nom::branch::alt;
use nom::bytes::complete::{tag, tag_no_case};
use nom::character::complete::alpha1;
use nom::combinator::{eof, fail, peek, verify};
use nom::sequence::terminated;
use once_cell::sync::Lazy;
use std::collections::HashSet;
use std::hash::{Hash, Hasher};

/// Peeks at the input for acceptable characters separating a keyword.
///
/// Will return a failure if one of the expected characters is not found.
fn keyword_follow_char(i: &str) -> ParseResult<&str, &str> {
    peek(alt((
        tag(" "),
        tag("\n"),
        tag(";"),
        tag("("),
        tag(")"),
        tag("\t"),
        tag(","),
        tag("="),
        tag("/"), // possible comment
        tag("-"), // possible comment
        eof,
        fail, // Return a failure if we reach the end of this alternation
    )))(i)
}

/// Token represents a string with case-insensitive ordering and equality.
#[derive(Debug, Clone)]
pub(crate) struct Token<'a>(pub(crate) &'a str);

impl PartialEq<Self> for Token<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.0.len() == other.0.len()
            && self
                .0
                .chars()
                .zip(other.0.chars())
                .all(|(l, r)| l.to_ascii_uppercase() == r.to_ascii_uppercase())
    }
}

impl<'a> Eq for Token<'a> {}

/// The Hash implementation for Token ensures
/// that two tokens, regardless of case, hash to the same
/// value.
impl<'a> Hash for Token<'a> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0
            .as_bytes()
            .iter()
            .map(u8::to_ascii_uppercase)
            .for_each(|v| state.write_u8(v));
    }
}

static KEYWORDS: Lazy<HashSet<Token<'static>>> = Lazy::new(|| {
    HashSet::from([
        Token("ALL"),
        Token("ALTER"),
        Token("ANALYZE"),
        Token("AND"),
        Token("ANY"),
        Token("AS"),
        Token("ASC"),
        Token("BEGIN"),
        Token("BY"),
        Token("CARDINALITY"),
        Token("CREATE"),
        Token("CONTINUOUS"),
        Token("DATABASE"),
        Token("DATABASES"),
        Token("DEFAULT"),
        Token("DELETE"),
        Token("DESC"),
        Token("DESTINATIONS"),
        Token("DIAGNOSTICS"),
        Token("DISTINCT"),
        Token("DROP"),
        Token("DURATION"),
        Token("END"),
        Token("EVERY"),
        Token("EXACT"),
        Token("EXPLAIN"),
        Token("FIELD"),
        Token("FOR"),
        Token("FROM"),
        Token("GRANT"),
        Token("GRANTS"),
        Token("GROUP"),
        Token("GROUPS"),
        Token("IN"),
        Token("INF"),
        Token("INSERT"),
        Token("INTO"),
        Token("KEY"),
        Token("KEYS"),
        Token("KILL"),
        Token("LIMIT"),
        Token("MEASUREMENT"),
        Token("MEASUREMENTS"),
        Token("NAME"),
        Token("OFFSET"),
        Token("OR"),
        Token("ON"),
        Token("ORDER"),
        Token("PASSWORD"),
        Token("POLICY"),
        Token("POLICIES"),
        Token("PRIVILEGES"),
        Token("QUERIES"),
        Token("QUERY"),
        Token("READ"),
        Token("REPLICATION"),
        Token("RESAMPLE"),
        Token("RETENTION"),
        Token("REVOKE"),
        Token("SELECT"),
        Token("SERIES"),
        Token("SET"),
        Token("SHOW"),
        Token("SHARD"),
        Token("SHARDS"),
        Token("SLIMIT"),
        Token("SOFFSET"),
        Token("STATS"),
        Token("SUBSCRIPTION"),
        Token("SUBSCRIPTIONS"),
        Token("TAG"),
        Token("TO"),
        Token("USER"),
        Token("USERS"),
        Token("VALUES"),
        Token("WHERE"),
        Token("WITH"),
        Token("WRITE"),
    ])
});

/// Matches any InfluxQL reserved keyword.
pub(crate) fn sql_keyword(i: &str) -> ParseResult<&str, &str> {
    verify(terminated(alpha1, keyword_follow_char), |tok: &str| {
        KEYWORDS.contains(&Token(tok))
    })(i)
}

/// Recognizes a case-insensitive `keyword`, ensuring it is followed by
/// a valid separator.
pub fn keyword<'a>(keyword: &'static str) -> impl FnMut(&'a str) -> ParseResult<&str, &str> {
    terminated(tag_no_case(keyword), keyword_follow_char)
}

#[cfg(test)]
mod test {
    use super::*;
    use assert_matches::assert_matches;

    #[test]
    fn test_keywords() {
        // all keywords

        sql_keyword("ALL").unwrap();
        sql_keyword("ALTER").unwrap();
        sql_keyword("ANALYZE").unwrap();
        sql_keyword("ANY").unwrap();
        sql_keyword("AS").unwrap();
        sql_keyword("ASC").unwrap();
        sql_keyword("BEGIN").unwrap();
        sql_keyword("BY").unwrap();
        sql_keyword("CARDINALITY").unwrap();
        sql_keyword("CREATE").unwrap();
        sql_keyword("CONTINUOUS").unwrap();
        sql_keyword("DATABASE").unwrap();
        sql_keyword("DATABASES").unwrap();
        sql_keyword("DEFAULT").unwrap();
        sql_keyword("DELETE").unwrap();
        sql_keyword("DESC").unwrap();
        sql_keyword("DESTINATIONS").unwrap();
        sql_keyword("DIAGNOSTICS").unwrap();
        sql_keyword("DISTINCT").unwrap();
        sql_keyword("DROP").unwrap();
        sql_keyword("DURATION").unwrap();
        sql_keyword("END").unwrap();
        sql_keyword("EVERY").unwrap();
        sql_keyword("EXACT").unwrap();
        sql_keyword("EXPLAIN").unwrap();
        sql_keyword("FIELD").unwrap();
        sql_keyword("FOR").unwrap();
        sql_keyword("FROM").unwrap();
        sql_keyword("GRANT").unwrap();
        sql_keyword("GRANTS").unwrap();
        sql_keyword("GROUP").unwrap();
        sql_keyword("GROUPS").unwrap();
        sql_keyword("IN").unwrap();
        sql_keyword("INF").unwrap();
        sql_keyword("INSERT").unwrap();
        sql_keyword("INTO").unwrap();
        sql_keyword("KEY").unwrap();
        sql_keyword("KEYS").unwrap();
        sql_keyword("KILL").unwrap();
        sql_keyword("LIMIT").unwrap();
        sql_keyword("MEASUREMENT").unwrap();
        sql_keyword("MEASUREMENTS").unwrap();
        sql_keyword("NAME").unwrap();
        sql_keyword("OFFSET").unwrap();
        sql_keyword("ON").unwrap();
        sql_keyword("ORDER").unwrap();
        sql_keyword("PASSWORD").unwrap();
        sql_keyword("POLICY").unwrap();
        sql_keyword("POLICIES").unwrap();
        sql_keyword("PRIVILEGES").unwrap();
        sql_keyword("QUERIES").unwrap();
        sql_keyword("QUERY").unwrap();
        sql_keyword("READ").unwrap();
        sql_keyword("REPLICATION").unwrap();
        sql_keyword("RESAMPLE").unwrap();
        sql_keyword("RETENTION").unwrap();
        sql_keyword("REVOKE").unwrap();
        sql_keyword("SELECT").unwrap();
        sql_keyword("SERIES").unwrap();
        sql_keyword("SET").unwrap();
        sql_keyword("SHOW").unwrap();
        sql_keyword("SHARD").unwrap();
        sql_keyword("SHARDS").unwrap();
        sql_keyword("SLIMIT").unwrap();
        sql_keyword("SOFFSET").unwrap();
        sql_keyword("STATS").unwrap();
        sql_keyword("SUBSCRIPTION").unwrap();
        sql_keyword("SUBSCRIPTIONS").unwrap();
        sql_keyword("TAG").unwrap();
        sql_keyword("TO").unwrap();
        sql_keyword("USER").unwrap();
        sql_keyword("USERS").unwrap();
        sql_keyword("VALUES").unwrap();
        sql_keyword("WHERE").unwrap();
        sql_keyword("WITH").unwrap();
        sql_keyword("WRITE").unwrap();

        // case insensitivity
        sql_keyword("all").unwrap();

        // ┌─────────────────────────────┐
        // │       Fallible tests        │
        // └─────────────────────────────┘

        sql_keyword("NOT_A_KEYWORD").unwrap_err();
    }

    #[test]
    fn test_keyword() {
        // Create a parser for the OR keyword
        let mut or_keyword = keyword("OR");

        // Can parse with matching case
        let (rem, got) = or_keyword("OR").unwrap();
        assert_eq!(rem, "");
        assert_eq!(got, "OR");

        // Not case sensitive
        let (rem, got) = or_keyword("or").unwrap();
        assert_eq!(rem, "");
        assert_eq!(got, "or");

        // Does not consume input that follows a keyword
        let (rem, got) = or_keyword("or(a AND b)").unwrap();
        assert_eq!(rem, "(a AND b)");
        assert_eq!(got, "or");

        // Will fail because keyword `OR` in `ORDER` is not recognized, as is not terminated by a valid character
        let err = or_keyword("ORDER").unwrap_err();
        assert_matches!(err, nom::Err::Error(crate::internal::Error::Nom(_, kind)) if kind == nom::error::ErrorKind::Fail);
    }

    #[test]
    fn test_token() {
        // Are equal with differing case
        let (a, b) = (Token("and"), Token("AND"));
        assert_eq!(a, b);

        // Are equal with same case
        let (a, b) = (Token("and"), Token("and"));
        assert_eq!(a, b);

        // a < b
        let (a, b) = (Token("and"), Token("apple"));
        assert_ne!(a, b);

        // a < b
        let (a, b) = (Token("and"), Token("APPLE"));
        assert_ne!(a, b);

        // a < b
        let (a, b) = (Token("AND"), Token("apple"));
        assert_ne!(a, b);

        // a > b
        let (a, b) = (Token("and"), Token("aardvark"));
        assert_ne!(a, b);

        // a > b
        let (a, b) = (Token("and"), Token("AARDVARK"));
        assert_ne!(a, b);

        // a > b
        let (a, b) = (Token("AND"), Token("aardvark"));
        assert_ne!(a, b);

        // Validate prefixes don't match and are correct ordering

        let (a, b) = (Token("aaa"), Token("aaabbb"));
        assert_ne!(a, b);

        let (a, b) = (Token("aaabbb"), Token("aaa"));
        assert_ne!(a, b);

        let (a, b) = (Token("aaa"), Token("AAABBB"));
        assert_ne!(a, b);

        let (a, b) = (Token("AAABBB"), Token("aaa"));
        assert_ne!(a, b);
    }
}
