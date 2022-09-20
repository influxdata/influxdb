use crate::delete::{delete_statement, DeleteStatement};
use crate::drop::{drop_statement, DropMeasurementStatement};
use crate::internal::ParseResult;
use crate::show::{show_statement, ShowDatabasesStatement};
use crate::show_field_keys::ShowFieldKeysStatement;
use crate::show_measurements::ShowMeasurementsStatement;
use crate::show_retention_policies::ShowRetentionPoliciesStatement;
use crate::show_tag_keys::ShowTagKeysStatement;
use crate::show_tag_values::ShowTagValuesStatement;
use nom::branch::alt;
use nom::combinator::map;
use std::fmt::{Display, Formatter};

/// An InfluxQL statement.
#[derive(Debug, Clone, PartialEq)]
pub enum Statement {
    /// Represents a `DELETE` statement.
    Delete(Box<DeleteStatement>),
    /// Represents a `DROP MEASUREMENT` statement.
    DropMeasurement(Box<DropMeasurementStatement>),
    /// Represents a `SHOW DATABASES` statement.
    ShowDatabases(Box<ShowDatabasesStatement>),
    /// Represents a `SHOW MEASUREMENTS` statement.
    ShowMeasurements(Box<ShowMeasurementsStatement>),
    /// Represents a `SHOW RETENTION POLICIES` statement.
    ShowRetentionPolicies(Box<ShowRetentionPoliciesStatement>),
    /// Represents a `SHOW TAG KEYS` statement.
    ShowTagKeys(Box<ShowTagKeysStatement>),
    /// Represents a `SHOW TAG VALUES` statement.
    ShowTagValues(Box<ShowTagValuesStatement>),
    /// Represents a `SHOW FIELD KEYS` statement.
    ShowFieldKeys(Box<ShowFieldKeysStatement>),
}

impl Display for Statement {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Delete(s) => Display::fmt(s, f)?,
            Self::DropMeasurement(s) => Display::fmt(s, f)?,
            Self::ShowDatabases(s) => Display::fmt(s, f)?,
            Self::ShowMeasurements(s) => Display::fmt(s, f)?,
            Self::ShowRetentionPolicies(s) => Display::fmt(s, f)?,
            Self::ShowTagKeys(s) => Display::fmt(s, f)?,
            Self::ShowTagValues(s) => Display::fmt(s, f)?,
            Self::ShowFieldKeys(s) => Display::fmt(s, f)?,
        };

        Ok(())
    }
}

/// Parse a single InfluxQL statement.
pub fn statement(i: &str) -> ParseResult<&str, Statement> {
    alt((
        map(delete_statement, |s| Statement::Delete(Box::new(s))),
        map(drop_statement, |s| Statement::DropMeasurement(Box::new(s))),
        show_statement,
    ))(i)
}

#[cfg(test)]
mod test {
    use crate::statement;

    #[test]
    fn test_statement() {
        // validate one of each statement parser is accepted

        // delete_statement combinator
        statement("DELETE FROM foo").unwrap();

        // drop_statement combinator
        statement("DROP MEASUREMENT foo").unwrap();

        // show_statement combinator
        statement("SHOW TAG KEYS").unwrap();
    }
}
