//! This module contains structs that describe the metadata for a partition
//! including schema, summary statistics, and file locations in storage.

use std::fmt::{Debug, Display};

use serde::{Deserialize, Serialize};

/// Describes the schema, summary statistics for each column in each table and
/// the location of the partition in storage.
#[derive(Debug, Deserialize, Serialize, PartialEq)]
pub struct Partition {
    /// The identifier for the partition, the partition key computed from
    /// PartitionRules
    pub key: String,
    /// The tables in this partition
    pub tables: Vec<Table>,
}

/// Metadata and statistics information for a table.
#[derive(Debug, Deserialize, Serialize, PartialEq)]
pub struct Table {
    pub name: String,
    pub columns: Vec<Column>,
}

/// Column name, statistics which encode type information
#[derive(Debug, Deserialize, Serialize, PartialEq)]
pub struct Column {
    pub name: String,
    pub stats: ColumnStats,
}

impl Column {
    /// Returns the total number of rows in this column
    pub fn count(&self) -> u32 {
        self.stats.count()
    }
}

/// Statistics and type information for a column.
#[derive(Debug, Deserialize, Serialize, PartialEq)]
pub enum ColumnStats {
    I64(Statistics<i64>),
    U64(Statistics<u64>),
    F64(Statistics<f64>),
    Bool(Statistics<bool>),
    String(Statistics<String>),
}

impl ColumnStats {
    /// Returns the total number of rows in this column
    pub fn count(&self) -> u32 {
        match self {
            Self::I64(s) => s.count,
            Self::U64(s) => s.count,
            Self::F64(s) => s.count,
            Self::Bool(s) => s.count,
            Self::String(s) => s.count,
        }
    }
}

/// Summary statistics for a column.
#[derive(Debug, Deserialize, Serialize, Clone, PartialEq)]
pub struct Statistics<T: PartialEq + PartialOrd + Debug + Display + Clone> {
    pub min: T,
    pub max: T,
    /// number of non-nil values in this column
    pub count: u32,
}

impl<T> Statistics<T>
where
    T: PartialEq + PartialOrd + Debug + Display + Clone,
{
    pub fn new(starting_value: T) -> Self {
        Self {
            min: starting_value.clone(),
            max: starting_value,
            count: 1,
        }
    }

    /// updates the statistics keeping the min, max and incrementing count.
    pub fn update(&mut self, other: T) {
        self.count += 1;

        let set_min = self.min > other;
        let set_max = self.max < other;

        match (set_min, set_max) {
            (true, true) => {
                self.min = other.clone();
                self.max = other;
            }
            (true, false) => {
                self.min = other;
            }
            (false, true) => {
                self.max = other;
            }
            (false, false) => (),
        }
    }
}

impl Statistics<String> {
    /// Function for string stats to avoid allocating if we're not updating min
    /// or max
    pub fn update_string(stats: &mut Self, other: &str) {
        stats.count += 1;

        if stats.min.as_str() > other {
            stats.min = other.to_string();
        }

        if stats.max.as_str() < other {
            stats.max = other.to_string();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn statistics_update() {
        let mut stat = Statistics::new(23);
        assert_eq!(stat.min, 23);
        assert_eq!(stat.max, 23);
        assert_eq!(stat.count, 1);

        stat.update(55);
        assert_eq!(stat.min, 23);
        assert_eq!(stat.max, 55);
        assert_eq!(stat.count, 2);

        stat.update(6);
        assert_eq!(stat.min, 6);
        assert_eq!(stat.max, 55);
        assert_eq!(stat.count, 3);

        stat.update(30);
        assert_eq!(stat.min, 6);
        assert_eq!(stat.max, 55);
        assert_eq!(stat.count, 4);
    }

    #[test]
    fn update_string() {
        let mut stat = Statistics::new("bbb".to_string());
        assert_eq!(stat.min, "bbb".to_string());
        assert_eq!(stat.max, "bbb".to_string());
        assert_eq!(stat.count, 1);

        Statistics::update_string(&mut stat, "aaa");
        assert_eq!(stat.min, "aaa".to_string());
        assert_eq!(stat.max, "bbb".to_string());
        assert_eq!(stat.count, 2);

        Statistics::update_string(&mut stat, "z");
        assert_eq!(stat.min, "aaa".to_string());
        assert_eq!(stat.max, "z".to_string());
        assert_eq!(stat.count, 3);

        Statistics::update_string(&mut stat, "p");
        assert_eq!(stat.min, "aaa".to_string());
        assert_eq!(stat.max, "z".to_string());
        assert_eq!(stat.count, 4);
    }
}
