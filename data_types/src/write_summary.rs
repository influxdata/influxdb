use crate::partition_metadata::StatValues;
use chrono::{DateTime, Timelike, Utc};

/// A description of a set of writes
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct WriteSummary {
    /// The wall clock timestamp of the first write in this summary
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

/// A description of the distribution of timestamps in a
/// set of writes, bucketed based on minute within the hour
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct TimestampSummary {
    /// Stores the count of how many rows in the set of writes have a timestamp
    /// with a minute matching a given index
    ///
    /// E.g. a row with timestamp 12:31:12 would store a count at index 31
    pub counts: [u32; 60],

    /// Standard timestamp statistics
    pub stats: StatValues<i64>,
}

impl Default for TimestampSummary {
    fn default() -> Self {
        Self {
            counts: [0; 60],
            stats: Default::default(),
        }
    }
}

impl TimestampSummary {
    /// Returns an iterator returning cumulative counts suitable for exposing
    /// as a cumulative histogram
    pub fn cumulative_counts(&self) -> impl Iterator<Item = (usize, u64)> + '_ {
        let mut acc = 0_u64;
        self.counts.iter().enumerate().map(move |(idx, count)| {
            acc += *count as u64;
            (idx, acc)
        })
    }

    /// Merges the counts from the provided summary into this
    pub fn merge(&mut self, other: &Self) {
        for (a, b) in self.counts.iter_mut().zip(&other.counts) {
            *a += *b
        }
    }

    /// Records a timestamp value
    pub fn record(&mut self, timestamp: DateTime<Utc>) {
        self.counts[timestamp.minute() as usize] += 1;
        self.stats.update(&timestamp.timestamp_nanos())
    }
}
