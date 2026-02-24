//! InfluxDB IOx implementation of FlightSQL

// Workaround for "unused crate" lint false positives.
use workspace_hack as _;

mod cmd;
mod error;
mod planner;
mod sql_info;
mod xdbc_type_info;

pub use cmd::{FlightSQLCommand, PreparedStatementHandle};
pub use error::{Error, Result};
pub use planner::FlightSQLPlanner;

/// Enum representing the base table type for FlightSQL requests.
///
/// `BaseTable` is the default
#[derive(Default, Debug, Clone, Copy, PartialEq, Eq)]
pub enum BaseTableType {
    /// Represents "BASE TABLE"
    /// JDBC compatible
    #[default]
    BaseTable,
    /// Represents "TABLE"
    /// ODBC compatible
    Table,
}

impl BaseTableType {
    /// Convert the enum to its string representation
    pub fn as_str(self) -> &'static str {
        match self {
            Self::BaseTable => "BASE TABLE",
            Self::Table => "TABLE",
        }
    }
}

impl std::str::FromStr for BaseTableType {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_uppercase().as_str() {
            "BASE TABLE" => Ok(Self::BaseTable),
            "TABLE" => Ok(Self::Table),
            _ => Err(Error::InvalidArgument {
                description: format!(
                    "Invalid base table type: '{s}'. Valid values are 'BASE TABLE' or 'TABLE'"
                ),
            }),
        }
    }
}
