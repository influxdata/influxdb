use chrono::{DateTime, Utc};

/// A description of a set of writes
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct WriteSummary {
    /// The wall clock timestamp of the last write in this summary
    pub time_of_first_write: DateTime<Utc>,

    /// The wall clock timestamp of the last write in this summary
    pub time_of_last_write: DateTime<Utc>,

    /// The minimum row timestamp for data in this summary
    pub min_timestamp: DateTime<Utc>,

    /// The maximum row timestamp value for data in this summary
    pub max_timestamp: DateTime<Utc>,

    /// The number of rows in this summary
    pub row_count: usize,
}
