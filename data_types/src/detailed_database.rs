use crate::DatabaseName;
use chrono::{DateTime, Utc};

/// Detailed metadata about a database.
#[derive(Debug, Clone, PartialEq)]
pub struct DetailedDatabase {
    /// The name of the database
    pub name: DatabaseName<'static>,
    /// The UTC datetime at which this database was deleted, if applicable
    pub deleted_at: Option<DateTime<Utc>>,
}
