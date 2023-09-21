//! Types and parsers for the [`EXPLAIN`][sql] statement.
//!
//! [sql]: https://docs.influxdata.com/influxdb/v1.8/query_language/spec/#explain

#![allow(dead_code)] // Temporary

use crate::common::ws1;
use crate::internal::{expect, ParseResult};
use crate::keywords::keyword;
use crate::select::{select_statement, SelectStatement};
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
    pub select: Box<SelectStatement>,
}

impl Display for ExplainStatement {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str("EXPLAIN ")?;
        if let Some(options) = &self.options {
            write!(f, "{options} ")?;
        }
        Display::fmt(&self.select, f)
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
                "invalid EXPLAIN statement, expected SELECT statement",
                select_statement,
            ),
        )),
        |(_, options, _, select)| ExplainStatement {
            options,
            select: Box::new(select),
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

        // Fallible cases

        assert_expect_error!(
            explain_statement("EXPLAIN ANALYZE SHOW DATABASES"),
            "invalid EXPLAIN statement, expected SELECT statement"
        );

        assert_expect_error!(
            explain_statement("EXPLAIN ANALYZE EXPLAIN SELECT val from temp"),
            "invalid EXPLAIN statement, expected SELECT statement"
        );

        // surfaces statement-specific errors
        assert_expect_error!(
            explain_statement("EXPLAIN ANALYZE SELECT cpu FROM 'foo'"),
            "invalid FROM clause, expected identifier, regular expression or subquery"
        );
    }
}
