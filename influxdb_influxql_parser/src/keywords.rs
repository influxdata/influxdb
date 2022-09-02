//! # Parse InfluxQL [keywords]
//!
//! [keywords]: https://docs.influxdata.com/influxdb/v1.8/query_language/spec/#keywords

#![allow(dead_code)]

use nom::branch::alt;
use nom::bytes::complete::{tag, tag_no_case};
use nom::combinator::{eof, peek};
use nom::sequence::terminated;
use nom::IResult;

/// Peeks at the input for acceptable characters following a keyword.
fn keyword_follow_char(i: &str) -> IResult<&str, &str> {
    peek(alt((
        tag(" "),
        tag("\n"),
        tag(";"),
        tag("("),
        tag(")"),
        tag("\t"),
        tag(","),
        tag("="),
        eof,
    )))(i)
}

/// Parses the input for matching InfluxQL keywords from ALL to DROP.
fn keyword_all_to_drop(i: &str) -> IResult<&str, &str> {
    alt((
        terminated(tag_no_case("ALL"), keyword_follow_char),
        terminated(tag_no_case("ALTER"), keyword_follow_char),
        terminated(tag_no_case("ANALYZE"), keyword_follow_char),
        terminated(tag_no_case("AND"), keyword_follow_char),
        terminated(tag_no_case("ANY"), keyword_follow_char),
        terminated(tag_no_case("AS"), keyword_follow_char),
        terminated(tag_no_case("ASC"), keyword_follow_char),
        terminated(tag_no_case("BEGIN"), keyword_follow_char),
        terminated(tag_no_case("BY"), keyword_follow_char),
        terminated(tag_no_case("CARDINALITY"), keyword_follow_char),
        terminated(tag_no_case("CREATE"), keyword_follow_char),
        terminated(tag_no_case("CONTINUOUS"), keyword_follow_char),
        terminated(tag_no_case("DATABASE"), keyword_follow_char),
        terminated(tag_no_case("DATABASES"), keyword_follow_char),
        terminated(tag_no_case("DEFAULT"), keyword_follow_char),
        terminated(tag_no_case("DELETE"), keyword_follow_char),
        terminated(tag_no_case("DESC"), keyword_follow_char),
        terminated(tag_no_case("DESTINATIONS"), keyword_follow_char),
        terminated(tag_no_case("DIAGNOSTICS"), keyword_follow_char),
        terminated(tag_no_case("DISTINCT"), keyword_follow_char),
        terminated(tag_no_case("DROP"), keyword_follow_char),
    ))(i)
}

/// Parses the input for matching InfluxQL keywords from DURATION to LIMIT.
fn keyword_duration_to_limit(i: &str) -> IResult<&str, &str> {
    alt((
        terminated(tag_no_case("DURATION"), keyword_follow_char),
        terminated(tag_no_case("END"), keyword_follow_char),
        terminated(tag_no_case("EVERY"), keyword_follow_char),
        terminated(tag_no_case("EXACT"), keyword_follow_char),
        terminated(tag_no_case("EXPLAIN"), keyword_follow_char),
        terminated(tag_no_case("FIELD"), keyword_follow_char),
        terminated(tag_no_case("FOR"), keyword_follow_char),
        terminated(tag_no_case("FROM"), keyword_follow_char),
        terminated(tag_no_case("GRANT"), keyword_follow_char),
        terminated(tag_no_case("GRANTS"), keyword_follow_char),
        terminated(tag_no_case("GROUP"), keyword_follow_char),
        terminated(tag_no_case("GROUPS"), keyword_follow_char),
        terminated(tag_no_case("IN"), keyword_follow_char),
        terminated(tag_no_case("INF"), keyword_follow_char),
        terminated(tag_no_case("INSERT"), keyword_follow_char),
        terminated(tag_no_case("INTO"), keyword_follow_char),
        terminated(tag_no_case("KEY"), keyword_follow_char),
        terminated(tag_no_case("KEYS"), keyword_follow_char),
        terminated(tag_no_case("KILL"), keyword_follow_char),
        terminated(tag_no_case("LIMIT"), keyword_follow_char),
    ))(i)
}

/// Parses the input for matching InfluxQL keywords from MEASUREMENT to SET.
fn keyword_measurement_to_set(i: &str) -> IResult<&str, &str> {
    alt((
        terminated(tag_no_case("MEASUREMENT"), keyword_follow_char),
        terminated(tag_no_case("MEASUREMENTS"), keyword_follow_char),
        terminated(tag_no_case("NAME"), keyword_follow_char),
        terminated(tag_no_case("OFFSET"), keyword_follow_char),
        terminated(tag_no_case("OR"), keyword_follow_char),
        terminated(tag_no_case("ON"), keyword_follow_char),
        terminated(tag_no_case("ORDER"), keyword_follow_char),
        terminated(tag_no_case("PASSWORD"), keyword_follow_char),
        terminated(tag_no_case("POLICY"), keyword_follow_char),
        terminated(tag_no_case("POLICIES"), keyword_follow_char),
        terminated(tag_no_case("PRIVILEGES"), keyword_follow_char),
        terminated(tag_no_case("QUERIES"), keyword_follow_char),
        terminated(tag_no_case("QUERY"), keyword_follow_char),
        terminated(tag_no_case("READ"), keyword_follow_char),
        terminated(tag_no_case("REPLICATION"), keyword_follow_char),
        terminated(tag_no_case("RESAMPLE"), keyword_follow_char),
        terminated(tag_no_case("RETENTION"), keyword_follow_char),
        terminated(tag_no_case("REVOKE"), keyword_follow_char),
        terminated(tag_no_case("SELECT"), keyword_follow_char),
        terminated(tag_no_case("SERIES"), keyword_follow_char),
        terminated(tag_no_case("SET"), keyword_follow_char),
    ))(i)
}

/// Parses the input for matching InfluxQL keywords from SHOW to WRITE.
fn keyword_show_to_write(i: &str) -> IResult<&str, &str> {
    alt((
        terminated(tag_no_case("SHOW"), keyword_follow_char),
        terminated(tag_no_case("SHARD"), keyword_follow_char),
        terminated(tag_no_case("SHARDS"), keyword_follow_char),
        terminated(tag_no_case("SLIMIT"), keyword_follow_char),
        terminated(tag_no_case("SOFFSET"), keyword_follow_char),
        terminated(tag_no_case("STATS"), keyword_follow_char),
        terminated(tag_no_case("SUBSCRIPTION"), keyword_follow_char),
        terminated(tag_no_case("SUBSCRIPTIONS"), keyword_follow_char),
        terminated(tag_no_case("TAG"), keyword_follow_char),
        terminated(tag_no_case("TO"), keyword_follow_char),
        terminated(tag_no_case("USER"), keyword_follow_char),
        terminated(tag_no_case("USERS"), keyword_follow_char),
        terminated(tag_no_case("VALUES"), keyword_follow_char),
        terminated(tag_no_case("WHERE"), keyword_follow_char),
        terminated(tag_no_case("WITH"), keyword_follow_char),
        terminated(tag_no_case("WRITE"), keyword_follow_char),
    ))(i)
}

// Matches any InfluxQL reserved keyword.
pub fn sql_keyword(i: &str) -> IResult<&str, &str> {
    // NOTE that the alt function takes a tuple with a maximum arity of 21, hence
    // the reason these are broken into groups
    alt((
        keyword_all_to_drop,
        keyword_duration_to_limit,
        keyword_measurement_to_set,
        keyword_show_to_write,
    ))(i)
}

#[cfg(test)]
mod test {
    use super::*;

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
}
