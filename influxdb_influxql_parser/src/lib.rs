//! # Parse a subset of [InfluxQL]
//!
//! [InfluxQL]: https://docs.influxdata.com/influxdb/v1.8/query_language

#![deny(rustdoc::broken_intra_doc_links, rust_2018_idioms)]
#![warn(
    missing_copy_implementations,
    missing_docs,
    clippy::explicit_iter_loop,
    clippy::future_not_send,
    clippy::use_self,
    clippy::clone_on_ref_ptr
)]

use crate::common::statement_terminator;
use crate::internal::Error as InternalError;
use crate::statement::statement;
pub use crate::statement::Statement;
use nom::character::complete::multispace0;
use nom::combinator::eof;
use nom::Offset;
use std::fmt::{Debug, Display, Formatter};

mod common;
mod expression;
mod identifier;
mod internal;
mod keywords;
mod literal;
mod parameter;
mod show;
mod show_measurements;
mod statement;
mod string;

#[cfg(test)]
mod test_util;

/// A error returned when parsing an InfluxQL query using
/// [`parse_statements`] fails.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParseError {
    message: String,
    pos: usize,
}

impl Display for ParseError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} at pos {}", self.message, self.pos)?;
        Ok(())
    }
}

/// ParseResult is type that represents the success or failure of parsing
/// a given input into a set of InfluxQL statements.
///
/// Errors are human-readable messages indicating the cause of the parse failure.
pub type ParseResult = Result<Vec<Statement>, ParseError>;

/// Parse the input into a set of InfluxQL statements.
pub fn parse_statements(input: &str) -> ParseResult {
    let mut res = Vec::new();
    let mut i: &str = input;

    loop {
        // Consume whitespace from the input
        i = match multispace0::<_, nom::error::Error<_>>(i) {
            Ok((i1, _)) => i1,
            _ => unreachable!("multispace0 is infallible"),
        };

        if eof::<_, nom::error::Error<_>>(i).is_ok() {
            return Ok(res);
        }

        if let Ok((i1, _)) = statement_terminator(i) {
            i = i1;
            continue;
        }

        match statement(i) {
            Ok((i1, o)) => {
                res.push(o);
                i = i1;
            }
            Err(nom::Err::Failure(InternalError::Syntax {
                input: pos,
                message,
            })) => {
                return Err(ParseError {
                    message: message.into(),
                    pos: input.offset(pos),
                })
            }
            // any other error indicates an invalid statement
            Err(_) => {
                return Err(ParseError {
                    message: "invalid SQL statement".into(),
                    pos: input.offset(i),
                })
            }
        }
    }
}

#[cfg(test)]
mod test {
    use crate::parse_statements;

    /// Validates that the [`parse_statements`] function
    /// handles statement terminators and errors.
    #[test]
    fn test_parse_statements() {
        // Parse a single statement, without a terminator
        let got = parse_statements("SHOW MEASUREMENTS").unwrap();
        assert_eq!(format!("{}", got.first().unwrap()), "SHOW MEASUREMENTS");

        // Parse a single statement, with a terminator
        let got = parse_statements("SHOW MEASUREMENTS;").unwrap();
        assert_eq!(format!("{}", got[0]), "SHOW MEASUREMENTS");

        // Parse multiple statements with whitespace
        let got = parse_statements("SHOW MEASUREMENTS;\nSHOW MEASUREMENTS LIMIT 1").unwrap();
        assert_eq!(format!("{}", got[0]), "SHOW MEASUREMENTS");
        assert_eq!(format!("{}", got[1]), "SHOW MEASUREMENTS LIMIT 1");

        // Parse multiple statements with a terminator in quotes, ensuring it is not interpreted as
        // a terminator
        let got = parse_statements(
            "SHOW MEASUREMENTS WITH MEASUREMENT = \";\";SHOW MEASUREMENTS LIMIT 1",
        )
        .unwrap();
        assert_eq!(
            format!("{}", got[0]),
            "SHOW MEASUREMENTS WITH MEASUREMENT = \";\""
        );
        assert_eq!(format!("{}", got[1]), "SHOW MEASUREMENTS LIMIT 1");

        // Returns error for invalid statement
        let got = parse_statements("BAD SQL").unwrap_err();
        assert_eq!(format!("{}", got), "invalid SQL statement at pos 0");

        // Returns error for invalid statement after first
        let got = parse_statements("SHOW MEASUREMENTS;BAD SQL").unwrap_err();
        assert_eq!(format!("{}", got), "invalid SQL statement at pos 18");
    }
}
