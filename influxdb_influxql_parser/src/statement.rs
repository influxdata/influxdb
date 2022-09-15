use crate::internal::ParseResult;
use crate::show::show_statement;
use crate::show_measurements::ShowMeasurementsStatement;
use std::fmt::{Display, Formatter};

/// An InfluxQL statement.
#[derive(Debug, Clone, PartialEq)]
pub enum Statement {
    /// Represents a `SHOW MEASUREMENTS` statement.
    ShowMeasurements(Box<ShowMeasurementsStatement>),
}

impl Display for Statement {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ShowMeasurements(s) => write!(f, "{}", s)?,
        };

        Ok(())
    }
}

/// Parse a single InfluxQL statement.
pub fn statement(i: &str) -> ParseResult<&str, Statement> {
    // NOTE: This will become an alt(()) once more statements are added
    show_statement(i)
}
