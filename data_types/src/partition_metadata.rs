//! This module contains structs that describe the metadata for a partition
//! including schema, summary statistics, and file locations in storage.

use std::fmt::{Debug, Display};

use serde::{Deserialize, Serialize};

/// Describes the schema, summary statistics for each column in each table and
/// the location of the partition in storage.
#[derive(Debug, Deserialize, Serialize, PartialEq)]
pub struct PartitionSummary {
    /// The identifier for the partition, the partition key computed from
    /// PartitionRules
    pub key: String,
    /// The tables in this partition
    pub tables: Vec<TableSummary>,
}

impl PartitionSummary {
    /// Create a partition summary from a collection of table summaries. These
    /// summaries can come from many chunks so a table can appear multiple
    /// times in the collection. They will be combined together for a single
    /// summary. Field type conflicts will be ignored.
    pub fn from_table_summaries(key: impl Into<String>, mut summaries: Vec<TableSummary>) -> Self {
        summaries.sort_by(|a, b| a.name.cmp(&b.name));

        let mut tables = Vec::with_capacity(summaries.len());

        let mut summaries = summaries.into_iter();

        if let Some(mut table) = summaries.next() {
            for t in summaries {
                if table.name != t.name {
                    tables.push(table);
                    table = t;
                } else {
                    table.update_from(&t);
                }
            }

            tables.push(table);
        }

        Self {
            key: key.into(),
            tables,
        }
    }

    /// Returns the table summary for the table name
    pub fn table(&self, name: &str) -> Option<&TableSummary> {
        self.tables.iter().find(|t| t.name == name)
    }
}

/// Metadata and statistics information for a table.
#[derive(Debug, Deserialize, Serialize, PartialEq, Clone)]
pub struct TableSummary {
    pub name: String,
    pub columns: Vec<ColumnSummary>,
}

impl TableSummary {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            columns: vec![],
        }
    }

    /// Updates the table summary with combined stats from the other. Counts are
    /// treated as non-overlapping so they're just added together. If the
    /// type of a column differs between the two tables, no update is done
    /// on that column. Columns that only exist in the other are cloned into
    /// this table summary.
    pub fn update_from(&mut self, other: &Self) {
        for col in &mut self.columns {
            if let Some(other_col) = other.column(&col.name) {
                col.update_from(other_col);
            }
        }

        for col in &other.columns {
            if self.column(&col.name).is_none() {
                self.columns.push(col.clone());
            }
        }
    }

    /// Get the column summary by name.
    pub fn column(&self, name: &str) -> Option<&ColumnSummary> {
        self.columns.iter().find(|c| c.name == name)
    }
}

/// Column name, statistics which encode type information
#[derive(Debug, Deserialize, Serialize, PartialEq, Clone)]
pub struct ColumnSummary {
    pub name: String,
    pub stats: Statistics,
}

impl ColumnSummary {
    /// Returns the total number of rows in this column
    pub fn count(&self) -> u64 {
        self.stats.count()
    }

    // Updates statistics from other if the same type, otherwise a noop
    pub fn update_from(&mut self, other: &Self) {
        match (&mut self.stats, &other.stats) {
            (Statistics::F64(s), Statistics::F64(o)) => {
                s.count += o.count;
                if o.min < s.min {
                    s.min = o.min;
                }
                if o.max > s.max {
                    s.max = o.max;
                }
            }
            (Statistics::I64(s), Statistics::I64(o)) => {
                s.count += o.count;
                if o.min < s.min {
                    s.min = o.min;
                }
                if o.max > s.max {
                    s.max = o.max;
                }
            }
            (Statistics::Bool(s), Statistics::Bool(o)) => {
                s.count += o.count;
                if s.min {
                    s.min = o.min
                }
                if !s.max {
                    s.max = o.max
                }
            }
            (Statistics::String(s), Statistics::String(o)) => {
                s.count += o.count;
                if o.min < s.min {
                    s.min = o.min.clone();
                }
                if o.max > s.max {
                    s.max = o.max.clone();
                }
            }
            (Statistics::U64(s), Statistics::U64(o)) => {
                s.count += o.count;
                if o.min < s.min {
                    s.min = o.min;
                }
                if o.max > s.max {
                    s.max = o.max;
                }
            }
            // do catch alls for the specific types, that way if a new type gets added, the compiler
            // will complain.
            (Statistics::F64(_), _) => (),
            (Statistics::I64(_), _) => (),
            (Statistics::U64(_), _) => (),
            (Statistics::Bool(_), _) => (),
            (Statistics::String(_), _) => (),
        }
    }
}

/// Column name, statistics which encode type information
#[derive(Debug, Deserialize, Serialize, PartialEq, Clone)]
pub struct Column {
    pub name: String,
    pub stats: Statistics,
}

impl Column {
    /// Returns the total number of rows in this column
    pub fn count(&self) -> u64 {
        self.stats.count()
    }
}

/// Statistics and type information for a column.
#[derive(Debug, Deserialize, Serialize, PartialEq, Clone)]
pub enum Statistics {
    I64(StatValues<i64>),
    U64(StatValues<u64>),
    F64(StatValues<f64>),
    Bool(StatValues<bool>),
    String(StatValues<String>),
}

impl Statistics {
    /// Returns the total number of rows in this column
    pub fn count(&self) -> u64 {
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
pub struct StatValues<T: PartialEq + PartialOrd + Debug + Display + Clone> {
    pub min: T,
    pub max: T,
    /// number of non-nil values in this column
    pub count: u64,
}

impl<T> StatValues<T>
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

impl StatValues<String> {
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
        let mut stat = StatValues::new(23);
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
        let mut stat = StatValues::new("bbb".to_string());
        assert_eq!(stat.min, "bbb".to_string());
        assert_eq!(stat.max, "bbb".to_string());
        assert_eq!(stat.count, 1);

        StatValues::update_string(&mut stat, "aaa");
        assert_eq!(stat.min, "aaa".to_string());
        assert_eq!(stat.max, "bbb".to_string());
        assert_eq!(stat.count, 2);

        StatValues::update_string(&mut stat, "z");
        assert_eq!(stat.min, "aaa".to_string());
        assert_eq!(stat.max, "z".to_string());
        assert_eq!(stat.count, 3);

        StatValues::update_string(&mut stat, "p");
        assert_eq!(stat.min, "aaa".to_string());
        assert_eq!(stat.max, "z".to_string());
        assert_eq!(stat.count, 4);
    }

    #[test]
    fn table_update_from() {
        let mut string_stats = StatValues::new("foo".to_string());
        string_stats.update("bar".to_string());
        let string_col = ColumnSummary {
            name: "string".to_string(),
            stats: Statistics::String(string_stats),
        };

        let mut int_stats = StatValues::new(1);
        int_stats.update(5);
        let int_col = ColumnSummary {
            name: "int".to_string(),
            stats: Statistics::I64(int_stats),
        };

        let mut float_stats = StatValues::new(9.1);
        float_stats.update(1.3);
        let float_col = ColumnSummary {
            name: "float".to_string(),
            stats: Statistics::F64(float_stats),
        };

        let mut table_a = TableSummary {
            name: "a".to_string(),
            columns: vec![string_col, int_col, float_col],
        };

        let mut string_stats = StatValues::new("aaa".to_string());
        string_stats.update("zzz".to_string());
        let string_col = ColumnSummary {
            name: "string".to_string(),
            stats: Statistics::String(string_stats),
        };

        let mut int_stats = StatValues::new(3);
        int_stats.update(9);
        let int_col = ColumnSummary {
            name: "int".to_string(),
            stats: Statistics::I64(int_stats),
        };

        let mut table_b = TableSummary {
            name: "a".to_string(),
            columns: vec![int_col, string_col],
        };

        // keep this to test joining the other way
        let table_c = table_a.clone();

        table_a.update_from(&table_b);
        let col = table_a.column("string").unwrap();
        assert_eq!(
            col.stats,
            Statistics::String(StatValues {
                min: "aaa".to_string(),
                max: "zzz".to_string(),
                count: 4
            })
        );

        let col = table_a.column("int").unwrap();
        assert_eq!(
            col.stats,
            Statistics::I64(StatValues {
                min: 1,
                max: 9,
                count: 4
            })
        );

        let col = table_a.column("float").unwrap();
        assert_eq!(
            col.stats,
            Statistics::F64(StatValues {
                min: 1.3,
                max: 9.1,
                count: 2
            })
        );

        table_b.update_from(&table_c);
        let col = table_b.column("string").unwrap();
        assert_eq!(
            col.stats,
            Statistics::String(StatValues {
                min: "aaa".to_string(),
                max: "zzz".to_string(),
                count: 4
            })
        );

        let col = table_b.column("int").unwrap();
        assert_eq!(
            col.stats,
            Statistics::I64(StatValues {
                min: 1,
                max: 9,
                count: 4
            })
        );

        let col = table_b.column("float").unwrap();
        assert_eq!(
            col.stats,
            Statistics::F64(StatValues {
                min: 1.3,
                max: 9.1,
                count: 2
            })
        );
    }

    #[test]
    fn from_table_summaries() {
        let mut string_stats = StatValues::new("foo".to_string());
        string_stats.update("bar".to_string());
        let string_col = ColumnSummary {
            name: "string".to_string(),
            stats: Statistics::String(string_stats),
        };

        let mut int_stats = StatValues::new(1);
        int_stats.update(5);
        let int_col = ColumnSummary {
            name: "int".to_string(),
            stats: Statistics::I64(int_stats),
        };

        let table_a = TableSummary {
            name: "a".to_string(),
            columns: vec![string_col, int_col],
        };

        let int_col = ColumnSummary {
            name: "int".to_string(),
            stats: Statistics::I64(StatValues::new(10)),
        };
        let table_b = TableSummary {
            name: "b".to_string(),
            columns: vec![int_col.clone()],
        };

        let table_a_2 = TableSummary {
            name: "a".to_string(),
            columns: vec![int_col],
        };

        let int_col = ColumnSummary {
            name: "int".to_string(),
            stats: Statistics::I64(StatValues::new(203)),
        };
        let table_b_2 = TableSummary {
            name: "b".to_string(),
            columns: vec![int_col],
        };

        let partition = PartitionSummary::from_table_summaries(
            "key",
            vec![table_b_2, table_a, table_b, table_a_2],
        );
        let t = partition.table("a").unwrap();
        let col = t.column("string").unwrap();
        assert_eq!(
            col.stats,
            Statistics::String(StatValues {
                min: "bar".to_string(),
                max: "foo".to_string(),
                count: 2
            })
        );
        let col = t.column("int").unwrap();
        assert_eq!(
            col.stats,
            Statistics::I64(StatValues {
                min: 1,
                max: 10,
                count: 3
            })
        );
        let t = partition.table("b").unwrap();
        let col = t.column("int").unwrap();
        assert_eq!(
            col.stats,
            Statistics::I64(StatValues {
                min: 10,
                max: 203,
                count: 2
            })
        );
    }

    #[test]
    fn column_update_from_boolean() {
        let bool_false = ColumnSummary {
            name: "b".to_string(),
            stats: Statistics::Bool(StatValues {
                min: false,
                max: false,
                count: 1,
            }),
        };
        let bool_true = ColumnSummary {
            name: "b".to_string(),
            stats: Statistics::Bool(StatValues {
                min: true,
                max: true,
                count: 1,
            }),
        };

        let expected_stats = Statistics::Bool(StatValues {
            min: false,
            max: true,
            count: 2,
        });

        let mut b = bool_false.clone();
        b.update_from(&bool_true);
        assert_eq!(b.stats, expected_stats);

        let mut b = bool_true;
        b.update_from(&bool_false);
        assert_eq!(b.stats, expected_stats);
    }

    #[test]
    fn column_update_from_u64() {
        let mut min = ColumnSummary {
            name: "foo".to_string(),
            stats: Statistics::U64(StatValues {
                min: 5,
                max: 23,
                count: 1,
            }),
        };

        let max = ColumnSummary {
            name: "foo".to_string(),
            stats: Statistics::U64(StatValues {
                min: 6,
                max: 506,
                count: 43,
            }),
        };

        min.update_from(&max);

        let expected = Statistics::U64(StatValues {
            min: 5,
            max: 506,
            count: 44,
        });
        assert_eq!(min.stats, expected);
    }
}
