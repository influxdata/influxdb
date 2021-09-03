use crate::DatabaseName;
use chrono::{DateTime, Utc};
use std::{fmt, str::FromStr};

/// Metadata about a deleted database that could be restored or permanently deleted.
#[derive(Debug, Clone, PartialEq)]
pub struct DeletedDatabase {
    pub name: DatabaseName<'static>,
    pub generation_id: GenerationId,
    pub deleted_at: DateTime<Utc>,
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
