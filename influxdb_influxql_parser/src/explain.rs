//! Types and parsers for the [`EXPLAIN`][sql] statement.
//!
//! [sql]: https://docs.influxdata.com/influxdb/v1.8/query_language/spec/#explain

#![allow(dead_code)] // Temporary

use crate::common::ws1;
use crate::internal::{expect, ParseResult};
use crate::keywords::keyword;
use crate::statement::{statement, Statement};
use nom::branch::alt;
use nom::combinator::{map, opt, value};
use nom::sequence::{preceded, tuple};
use std::fmt::{Display, Formatter};

/// Represents various options for an `EXPLAIN` statement.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExplainOption {
    /// `EXPLAIN VERBOSE statement`
    Verbose,
    /// `EXPLAIN ANALYZE statement`
    Analyze,
    /// `EXPLAIN ANALYZE VERBOSE statement`
    AnalyzeVerbose,
}

impl Display for ExplainOption {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Verbose => f.write_str("VERBOSE"),
            Self::Analyze => f.write_str("ANALYZE"),
            Self::AnalyzeVerbose => f.write_str("ANALYZE VERBOSE"),
        }
    }
}

/// Represents an `EXPLAIN` statement.
///
/// ```text
/// explain         ::= "EXPLAIN" explain_options? select_statement
/// explain_options ::= "VERBOSE" | ( "ANALYZE" "VERBOSE"? )
/// ```
#[derive(Debug, Clone, PartialEq)]
pub struct ExplainStatement {
    /// Represents any options specified for the `EXPLAIN` statement.
    pub options: Option<ExplainOption>,

    /// Represents the `SELECT` statement to be explained and / or analyzed.
    pub statement: Box<Statement>,
}

impl Display for ExplainStatement {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str("EXPLAIN ")?;
        if let Some(options) = &self.options {
            write!(f, "{options} ")?;
        }
        Display::fmt(&self.statement, f)
    }
}

/// Parse an `EXPLAIN` statement.
pub(crate) fn explain_statement(i: &str) -> ParseResult<&str, ExplainStatement> {
    map(
        tuple((
            keyword("EXPLAIN"),
            opt(preceded(
                ws1,
                alt((
                    map(
                        preceded(keyword("ANALYZE"), opt(preceded(ws1, keyword("VERBOSE")))),
                        |v| match v {
                            // If the optional combinator is Some, then it matched VERBOSE
                            Some(_) => ExplainOption::AnalyzeVerbose,
                            _ => ExplainOption::Analyze,
                        },
                    ),
                    value(ExplainOption::Verbose, keyword("VERBOSE")),
                )),
            )),
            ws1,
            expect(
                "invalid EXPLAIN statement, expected InfluxQL statement",
                statement,
            ),
        )),
        |(_, options, _, statement)| ExplainStatement {
            options,
            statement: Box::new(statement),
        },
    )(i)
}

#[cfg(test)]
mod test {
    use crate::assert_expect_error;
    use crate::explain::{explain_statement, ExplainOption};
    use assert_matches::assert_matches;

    #[test]
    fn test_explain_statement() {
        // EXPLAIN SELECT cases

        let (remain, got) = explain_statement("EXPLAIN SELECT val from temp").unwrap();
        assert_eq!(remain, ""); // assert that all input was consumed
        assert_matches!(got.options, None);
        assert_eq!(got.to_string(), "EXPLAIN SELECT val FROM temp");

        let (remain, got) = explain_statement("EXPLAIN VERBOSE SELECT val from temp").unwrap();
        assert_eq!(remain, "");
        assert_matches!(&got.options, Some(o) if *o == ExplainOption::Verbose);
        assert_eq!(got.to_string(), "EXPLAIN VERBOSE SELECT val FROM temp");

        let (remain, got) = explain_statement("EXPLAIN ANALYZE SELECT val from temp").unwrap();
        assert_eq!(remain, "");
        assert_matches!(&got.options, Some(o) if *o == ExplainOption::Analyze);
        assert_eq!(got.to_string(), "EXPLAIN ANALYZE SELECT val FROM temp");

        let (remain, got) =
            explain_statement("EXPLAIN ANALYZE VERBOSE SELECT val from temp").unwrap();
        assert_eq!(remain, "");
        assert_matches!(&got.options, Some(o) if *o == ExplainOption::AnalyzeVerbose);
        assert_eq!(
            got.to_string(),
            "EXPLAIN ANALYZE VERBOSE SELECT val FROM temp"
        );

        // EXPLAIN SHOW MEASUREMENTS cases
        let (remain, got) = explain_statement("EXPLAIN SHOW MEASUREMENTS").unwrap();
        assert_eq!(remain, ""); // assert that all input was consumed
        assert_matches!(got.options, None);
        assert_eq!(got.to_string(), "EXPLAIN SHOW MEASUREMENTS");

        let (remain, got) = explain_statement("EXPLAIN VERBOSE SHOW MEASUREMENTS").unwrap();
        assert_eq!(remain, ""); // assert that all input was consumed
        assert_matches!(&got.options, Some(o) if *o == ExplainOption::Verbose);
        assert_eq!(got.to_string(), "EXPLAIN VERBOSE SHOW MEASUREMENTS");

        let (remain, got) = explain_statement("EXPLAIN ANALYZE SHOW MEASUREMENTS").unwrap();
        assert_eq!(remain, ""); // assert that all input was consumed
        assert_matches!(&got.options, Some(o) if *o == ExplainOption::Analyze);
        assert_eq!(got.to_string(), "EXPLAIN ANALYZE SHOW MEASUREMENTS");

        let (remain, got) = explain_statement("EXPLAIN ANALYZE VERBOSE SHOW MEASUREMENTS").unwrap();
        assert_eq!(remain, ""); // assert that all input was consumed
        assert_matches!(&got.options, Some(o) if *o == ExplainOption::AnalyzeVerbose);
        assert_eq!(got.to_string(), "EXPLAIN ANALYZE VERBOSE SHOW MEASUREMENTS");

        // EXPLAIN SHOW TAG KEYS cases
        let (remain, got) = explain_statement("EXPLAIN SHOW TAG KEYS").unwrap();
        assert_eq!(remain, ""); // assert that all input was consumed
        assert_matches!(got.options, None);
        assert_eq!(got.to_string(), "EXPLAIN SHOW TAG KEYS");

        let (remain, got) = explain_statement("EXPLAIN VERBOSE SHOW TAG KEYS").unwrap();
        assert_eq!(remain, ""); // assert that all input was consumed
        assert_matches!(&got.options, Some(o) if *o == ExplainOption::Verbose);
        assert_eq!(got.to_string(), "EXPLAIN VERBOSE SHOW TAG KEYS");

        let (remain, got) = explain_statement("EXPLAIN ANALYZE SHOW TAG KEYS").unwrap();
        assert_eq!(remain, ""); // assert that all input was consumed
        assert_matches!(&got.options, Some(o) if *o == ExplainOption::Analyze);
        assert_eq!(got.to_string(), "EXPLAIN ANALYZE SHOW TAG KEYS");

        let (remain, got) = explain_statement("EXPLAIN ANALYZE VERBOSE SHOW TAG KEYS").unwrap();
        assert_eq!(remain, ""); // assert that all input was consumed
        assert_matches!(&got.options, Some(o) if *o == ExplainOption::AnalyzeVerbose);
        assert_eq!(got.to_string(), "EXPLAIN ANALYZE VERBOSE SHOW TAG KEYS");

        // EXPLAIN SHOW TAG VALUES cases
        let (remain, got) =
            explain_statement("EXPLAIN SHOW TAG VALUES WITH KEY = \"Key\"").unwrap();
        assert_eq!(remain, ""); // assert that all input was consumed
        assert_matches!(got.options, None);
        assert_eq!(
            got.to_string(),
            "EXPLAIN SHOW TAG VALUES WITH KEY = \"Key\""
        );

        let (remain, got) =
            explain_statement("EXPLAIN VERBOSE SHOW TAG VALUES WITH KEY = \"Key\"").unwrap();
        assert_eq!(remain, ""); // assert that all input was consumed
        assert_matches!(&got.options, Some(o) if *o == ExplainOption::Verbose);
        assert_eq!(
            got.to_string(),
            "EXPLAIN VERBOSE SHOW TAG VALUES WITH KEY = \"Key\""
        );

        let (remain, got) =
            explain_statement("EXPLAIN ANALYZE SHOW TAG VALUES WITH KEY = \"Key\"").unwrap();
        assert_eq!(remain, ""); // assert that all input was consumed
        assert_matches!(&got.options, Some(o) if *o == ExplainOption::Analyze);
        assert_eq!(
            got.to_string(),
            "EXPLAIN ANALYZE SHOW TAG VALUES WITH KEY = \"Key\""
        );

        let (remain, got) =
            explain_statement("EXPLAIN ANALYZE VERBOSE SHOW TAG VALUES WITH KEY = \"Key\"")
                .unwrap();
        assert_eq!(remain, ""); // assert that all input was consumed
        assert_matches!(&got.options, Some(o) if *o == ExplainOption::AnalyzeVerbose);
        assert_eq!(
            got.to_string(),
            "EXPLAIN ANALYZE VERBOSE SHOW TAG VALUES WITH KEY = \"Key\""
        );

        // EXPLAIN SHOW FIELD KEYS cases
        let (remain, got) = explain_statement("EXPLAIN SHOW FIELD KEYS").unwrap();
        assert_eq!(remain, ""); // assert that all input was consumed
        assert_matches!(got.options, None);
        assert_eq!(got.to_string(), "EXPLAIN SHOW FIELD KEYS");

        let (remain, got) = explain_statement("EXPLAIN VERBOSE SHOW FIELD KEYS").unwrap();
        assert_eq!(remain, ""); // assert that all input was consumed
        assert_matches!(&got.options, Some(o) if *o == ExplainOption::Verbose);
        assert_eq!(got.to_string(), "EXPLAIN VERBOSE SHOW FIELD KEYS");

        let (remain, got) = explain_statement("EXPLAIN ANALYZE SHOW FIELD KEYS").unwrap();
        assert_eq!(remain, ""); // assert that all input was consumed
        assert_matches!(&got.options, Some(o) if *o == ExplainOption::Analyze);
        assert_eq!(got.to_string(), "EXPLAIN ANALYZE SHOW FIELD KEYS");

        let (remain, got) = explain_statement("EXPLAIN ANALYZE VERBOSE SHOW FIELD KEYS").unwrap();
        assert_eq!(remain, ""); // assert that all input was consumed
        assert_matches!(&got.options, Some(o) if *o == ExplainOption::AnalyzeVerbose);
        assert_eq!(got.to_string(), "EXPLAIN ANALYZE VERBOSE SHOW FIELD KEYS");

        // EXPLAIN SHOW RETENTION POLICIES cases
        let (remain, got) = explain_statement("EXPLAIN SHOW RETENTION POLICIES").unwrap();
        assert_eq!(remain, ""); // assert that all input was consumed
        assert_matches!(got.options, None);
        assert_eq!(got.to_string(), "EXPLAIN SHOW RETENTION POLICIES");

        let (remain, got) = explain_statement("EXPLAIN VERBOSE SHOW RETENTION POLICIES").unwrap();
        assert_eq!(remain, ""); // assert that all input was consumed
        assert_matches!(&got.options, Some(o) if *o == ExplainOption::Verbose);
        assert_eq!(got.to_string(), "EXPLAIN VERBOSE SHOW RETENTION POLICIES");

        let (remain, got) = explain_statement("EXPLAIN ANALYZE SHOW RETENTION POLICIES").unwrap();
        assert_eq!(remain, ""); // assert that all input was consumed
        assert_matches!(&got.options, Some(o) if *o == ExplainOption::Analyze);
        assert_eq!(got.to_string(), "EXPLAIN ANALYZE SHOW RETENTION POLICIES");

        let (remain, got) =
            explain_statement("EXPLAIN ANALYZE VERBOSE SHOW RETENTION POLICIES").unwrap();
        assert_eq!(remain, ""); // assert that all input was consumed
        assert_matches!(&got.options, Some(o) if *o == ExplainOption::AnalyzeVerbose);
        assert_eq!(
            got.to_string(),
            "EXPLAIN ANALYZE VERBOSE SHOW RETENTION POLICIES"
        );

        // EXPLAIN SHOW DATABASES cases
        let (remain, got) = explain_statement("EXPLAIN SHOW DATABASES").unwrap();
        assert_eq!(remain, ""); // assert that all input was consumed
        assert_matches!(got.options, None);
        assert_eq!(got.to_string(), "EXPLAIN SHOW DATABASES");

        let (remain, got) = explain_statement("EXPLAIN VERBOSE SHOW DATABASES").unwrap();
        assert_eq!(remain, ""); // assert that all input was consumed
        assert_matches!(&got.options, Some(o) if *o == ExplainOption::Verbose);
        assert_eq!(got.to_string(), "EXPLAIN VERBOSE SHOW DATABASES");

        let (remain, got) = explain_statement("EXPLAIN ANALYZE SHOW DATABASES").unwrap();
        assert_eq!(remain, ""); // assert that all input was consumed
        assert_matches!(&got.options, Some(o) if *o == ExplainOption::Analyze);
        assert_eq!(got.to_string(), "EXPLAIN ANALYZE SHOW DATABASES");

        let (remain, got) = explain_statement("EXPLAIN ANALYZE VERBOSE SHOW DATABASES").unwrap();
        assert_eq!(remain, ""); // assert that all input was consumed
        assert_matches!(&got.options, Some(o) if *o == ExplainOption::AnalyzeVerbose);
        assert_eq!(got.to_string(), "EXPLAIN ANALYZE VERBOSE SHOW DATABASES");

        // NOTE: Nested EXPLAIN is valid; DataFusion will throw a "No Nested EXPLAIN" error later
        let (remain, got) =
            explain_statement("EXPLAIN ANALYZE EXPLAIN SELECT val from temp").unwrap();
        assert_eq!(remain, ""); // assert that all input was consumed
        assert_matches!(&got.options, Some(o) if *o == ExplainOption::Analyze);
        assert_eq!(
            got.to_string(),
            "EXPLAIN ANALYZE EXPLAIN SELECT val FROM temp"
        );

        // surfaces statement-specific errors
        assert_expect_error!(
            explain_statement("EXPLAIN ANALYZE SELECT cpu FROM 'foo'"),
            "invalid FROM clause, expected identifier, regular expression or subquery"
        );
    }
}
