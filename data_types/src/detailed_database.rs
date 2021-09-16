use crate::DatabaseName;
use chrono::{DateTime, Utc};
use std::{fmt, str::FromStr};

/// Detailed metadata about a database.
#[derive(Debug, Clone, PartialEq)]
pub struct DetailedDatabase {
    /// The name of the database
    pub name: DatabaseName<'static>,
    /// The generation ID of the database in object storage
    pub generation_id: GenerationId,
    /// The UTC datetime at which this database was deleted, if applicable
    pub deleted_at: Option<DateTime<Utc>>,
}

/// Identifier for a generation of a particular database
#[derive(Debug, Copy, Clone, PartialEq, Eq, Ord, PartialOrd)]
pub struct GenerationId {
    pub inner: usize,
}

impl FromStr for GenerationId {
    type Err = std::num::ParseIntError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self { inner: s.parse()? })
    }
}

impl fmt::Display for GenerationId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.inner)
    }
}
