//! Types and parsers for the [`DROP MEASUREMENT`][sql] statement.
//!
//! [sql]: https://docs.influxdata.com/influxdb/v1.8/query_language/manage-database/#delete-measurements-with-drop-measurement

use crate::common::ws1;
use crate::identifier::{identifier, Identifier};
use crate::internal::{expect, ParseResult};
use crate::keywords::keyword;
use nom::combinator::map;
use nom::sequence::{pair, preceded};
use std::fmt::{Display, Formatter};

/// Represents a `DROP MEASUREMENT` statement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DropMeasurementStatement {
    /// The name of the measurement to delete.
    name: Identifier,
}

impl Display for DropMeasurementStatement {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "DROP MEASUREMENT {}", self.name)
    }
}

pub(crate) fn drop_statement(i: &str) -> ParseResult<&str, DropMeasurementStatement> {
    preceded(
        pair(keyword("DROP"), ws1),
        expect(
            "invalid DROP statement, expected MEASUREMENT",
            drop_measurement,
        ),
    )(i)
}

fn drop_measurement(i: &str) -> ParseResult<&str, DropMeasurementStatement> {
    preceded(
        keyword("MEASUREMENT"),
        map(
            expect(
                "invalid DROP MEASUREMENT statement, expected identifier",
                identifier,
            ),
            |name| DropMeasurementStatement { name },
        ),
    )(i)
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::assert_expect_error;

    #[test]
    fn test_drop_statement() {
        drop_statement("DROP MEASUREMENT foo").unwrap();

        // Fallible cases
        assert_expect_error!(
            drop_statement("DROP foo"),
            "invalid DROP statement, expected MEASUREMENT"
        );
    }

    #[test]
    fn test_drop_measurement() {
        let (_, got) = drop_measurement("MEASUREMENT \"foo\"").unwrap();
        assert_eq!(got, DropMeasurementStatement { name: "foo".into() });
        // validate Display
        assert_eq!(format!("{}", got), "DROP MEASUREMENT foo");

        // Fallible cases
        assert_expect_error!(
            drop_measurement("MEASUREMENT 'foo'"),
            "invalid DROP MEASUREMENT statement, expected identifier"
        );
    }
}
