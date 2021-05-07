//! This module contains structs that describe the metadata for a partition
//! including schema, summary statistics, and file locations in storage.

use std::{borrow::Cow, mem};

use serde::{Deserialize, Serialize};
use std::borrow::Borrow;

/// Describes the aggregated (across all chunks) summary
/// statistics for each column in each table in a partition
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
    pub fn from_table_summaries(
        key: impl Into<String>,
        summaries: impl IntoIterator<Item = TableSummary>,
    ) -> Self {
        let mut summaries: Vec<_> = summaries.into_iter().collect();
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

/// Describes the (unaggregated) summary statistics for
/// each column in each table in each chunk of a Partition.
#[derive(Debug, Deserialize, Serialize, PartialEq)]
pub struct UnaggregatedPartitionSummary {
    /// The identifier for the partition, the partition key computed from
    /// PartitionRules
    pub key: String,

    /// The chunks of the tables in this partition
    pub tables: Vec<UnaggregatedTableSummary>,
}

/// Metadata and statistics for a Chunk *within* a partition
#[derive(Debug, Deserialize, Serialize, PartialEq, Clone)]
pub struct UnaggregatedTableSummary {
    pub chunk_id: u32,
    pub table: TableSummary,
}

/// Metadata and statistics information for a table, aggregated across
/// chunks.
#[derive(Debug, Deserialize, Serialize, PartialEq, Clone)]
pub struct TableSummary {
    /// Table name
    pub name: String,

    /// Per column statistics
    pub columns: Vec<ColumnSummary>,
}

impl TableSummary {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            columns: vec![],
        }
    }

    pub fn size(&self) -> usize {
        // Total size of all ColumnSummaries that belong to this table which include
        // column names and their stats
        let size: usize = self.columns.iter().map(|c| c.size()).sum();
        size
            + self.name.len() // Add size of the table name
            + mem::size_of::<Self>() // Add size of this struct that points to
                                     // table and ColumnSummary
    }
    pub fn has_table(&self, table_name: &str) -> bool {
        self.name.eq(table_name)
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

// Replicate this enum here as it can't be derived from the existing statistics
#[derive(Debug, Deserialize, Serialize, PartialEq, Clone)]
pub enum InfluxDbType {
    Tag,
    Field,
    Timestamp,
}

impl InfluxDbType {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Tag => "Tag",
            Self::Field => "Field",
            Self::Timestamp => "Timestamp",
        }
    }
}

/// Column name, statistics which encode type information
#[derive(Debug, Deserialize, Serialize, PartialEq, Clone)]
pub struct ColumnSummary {
    /// Column name
    pub name: String,

    /// Column's Influx data model type (if any)
    pub influxdb_type: Option<InfluxDbType>,

    /// Per column statistics
    pub stats: Statistics,
}

impl ColumnSummary {
    /// Returns the total number of rows in this column
    pub fn count(&self) -> u64 {
        self.stats.count()
    }

    /// Return a human interprertable string for this column's IOx
    /// data type
    pub fn type_name(&self) -> &'static str {
        self.stats.type_name()
    }

    /// Return size in bytes of this Column metadata (not the underlying column)
    pub fn size(&self) -> usize {
        mem::size_of::<Self>() + self.name.len() + mem::size_of_val(&self.stats)
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

    /// Return a human interpretable description of this type
    pub fn type_name(&self) -> &'static str {
        match self {
            Self::I64(_) => "I64",
            Self::U64(_) => "U64",
            Self::F64(_) => "F64",
            Self::Bool(_) => "Bool",
            Self::String(_) => "String",
        }
    }

    /// Return the minimum value, if any, formatted as a string
    pub fn min_as_str(&self) -> Option<Cow<'_, str>> {
        match self {
            Self::I64(v) => Some(Cow::Owned(v.min.to_string())),
            Self::U64(v) => Some(Cow::Owned(v.min.to_string())),
            Self::F64(v) => Some(Cow::Owned(v.min.to_string())),
            Self::Bool(v) => Some(Cow::Owned(v.min.to_string())),
            Self::String(v) => Some(Cow::Borrowed(&v.min)),
        }
    }

    /// Return the maximum value, if any, formatted as a string
    pub fn max_as_str(&self) -> Option<Cow<'_, str>> {
        match self {
            Self::I64(v) => Some(Cow::Owned(v.max.to_string())),
            Self::U64(v) => Some(Cow::Owned(v.max.to_string())),
            Self::F64(v) => Some(Cow::Owned(v.max.to_string())),
            Self::Bool(v) => Some(Cow::Owned(v.max.to_string())),
            Self::String(v) => Some(Cow::Borrowed(&v.max)),
        }
    }
}

/// Summary statistics for a column.
#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, Default)]
pub struct StatValues<T> {
    pub min: T,
    pub max: T,
    /// number of non-nil values in this column
    pub count: u64,
}

impl<T> StatValues<T>
where
    T: Default + Clone,
{
    pub fn new_with_value(starting_value: T) -> Self {
        Self {
            min: starting_value.clone(),
            max: starting_value,
            count: 1,
        }
    }

    pub fn new(min: T, max: T, count: u64) -> Self {
        Self { min, max, count }
    }
}

impl<T> StatValues<T> {
    /// updates the statistics keeping the min, max and incrementing count.
    ///
    /// The type plumbing exists to allow calling with &str on a StatValues<String>
    pub fn update<U: ?Sized>(&mut self, other: &U)
    where
        T: Borrow<U>,
        U: ToOwned<Owned = T> + PartialOrd,
    {
        self.count += 1;

        if self.count == 1 || self.min.borrow() > other {
            self.min = other.to_owned();
        }

        if self.count == 1 || self.max.borrow() < other {
            self.max = other.to_owned();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn statistics_update() {
        let mut stat = StatValues::new_with_value(23);
        assert_eq!(stat.min, 23);
        assert_eq!(stat.max, 23);
        assert_eq!(stat.count, 1);

        stat.update(&55);
        assert_eq!(stat.min, 23);
        assert_eq!(stat.max, 55);
        assert_eq!(stat.count, 2);

        stat.update(&6);
        assert_eq!(stat.min, 6);
        assert_eq!(stat.max, 55);
        assert_eq!(stat.count, 3);

        stat.update(&30);
        assert_eq!(stat.min, 6);
        assert_eq!(stat.max, 55);
        assert_eq!(stat.count, 4);
    }

    #[test]
    fn statistics_default() {
        let mut stat = StatValues::default();
        assert_eq!(stat.min, 0);
        assert_eq!(stat.max, 0);
        assert_eq!(stat.count, 0);

        stat.update(&55);
        assert_eq!(stat.min, 55);
        assert_eq!(stat.max, 55);
        assert_eq!(stat.count, 1);

        let mut stat = StatValues::default();
        assert_eq!(&stat.min, "");
        assert_eq!(&stat.max, "");
        assert_eq!(stat.count, 0);

        stat.update("cupcakes");
        assert_eq!(&stat.min, "cupcakes");
        assert_eq!(&stat.max, "cupcakes");
        assert_eq!(stat.count, 1);

        stat.update("woo");
        assert_eq!(&stat.min, "cupcakes");
        assert_eq!(&stat.max, "woo");
        assert_eq!(stat.count, 2);
    }

    #[test]
    fn update_string() {
        let mut stat = StatValues::new_with_value("bbb".to_string());
        assert_eq!(stat.min, "bbb".to_string());
        assert_eq!(stat.max, "bbb".to_string());
        assert_eq!(stat.count, 1);

        stat.update("aaa");
        assert_eq!(stat.min, "aaa".to_string());
        assert_eq!(stat.max, "bbb".to_string());
        assert_eq!(stat.count, 2);

        stat.update("z");
        assert_eq!(stat.min, "aaa".to_string());
        assert_eq!(stat.max, "z".to_string());
        assert_eq!(stat.count, 3);

        stat.update("p");
        assert_eq!(stat.min, "aaa".to_string());
        assert_eq!(stat.max, "z".to_string());
        assert_eq!(stat.count, 4);
    }

    #[test]
    fn stats_as_str_i64() {
        let stat = Statistics::I64(StatValues::new(-1, 100, 1));
        assert_eq!(stat.min_as_str(), Some("-1".into()));
        assert_eq!(stat.max_as_str(), Some("100".into()));
    }

    #[test]
    fn stats_as_str_u64() {
        let stat = Statistics::U64(StatValues::new(1, 100, 1));
        assert_eq!(stat.min_as_str(), Some("1".into()));
        assert_eq!(stat.max_as_str(), Some("100".into()));
    }

    #[test]
    fn stats_as_str_f64() {
        let stat = Statistics::F64(StatValues::new(99.0, 101.0, 1));
        assert_eq!(stat.min_as_str(), Some("99".into()));
        assert_eq!(stat.max_as_str(), Some("101".into()));
    }

    #[test]
    fn stats_as_str_bool() {
        let stat = Statistics::Bool(StatValues::new(false, true, 1));
        assert_eq!(stat.min_as_str(), Some("false".into()));
        assert_eq!(stat.max_as_str(), Some("true".into()));
    }

    #[test]
    fn stats_as_str_str() {
        let stat = Statistics::String(StatValues::new("a".to_string(), "zz".to_string(), 1));
        assert_eq!(stat.min_as_str(), Some("a".into()));
        assert_eq!(stat.max_as_str(), Some("zz".into()));
    }

    #[test]
    fn table_update_from() {
        let mut string_stats = StatValues::new_with_value("foo".to_string());
        string_stats.update("bar");
        let string_col = ColumnSummary {
            name: "string".to_string(),
            influxdb_type: None,
            stats: Statistics::String(string_stats),
        };

        let mut int_stats = StatValues::new_with_value(1);
        int_stats.update(&5);
        let int_col = ColumnSummary {
            name: "int".to_string(),
            influxdb_type: None,
            stats: Statistics::I64(int_stats),
        };

        let mut float_stats = StatValues::new_with_value(9.1);
        float_stats.update(&1.3);
        let float_col = ColumnSummary {
            name: "float".to_string(),
            influxdb_type: None,
            stats: Statistics::F64(float_stats),
        };

        let mut table_a = TableSummary {
            name: "a".to_string(),
            columns: vec![string_col, int_col, float_col],
        };

        let mut string_stats = StatValues::new_with_value("aaa".to_string());
        string_stats.update("zzz");
        let string_col = ColumnSummary {
            name: "string".to_string(),
            influxdb_type: None,
            stats: Statistics::String(string_stats),
        };

        let mut int_stats = StatValues::new_with_value(3);
        int_stats.update(&9);
        let int_col = ColumnSummary {
            name: "int".to_string(),
            influxdb_type: None,
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
        let mut string_stats = StatValues::new_with_value("foo".to_string());
        string_stats.update("bar");
        let string_col = ColumnSummary {
            name: "string".to_string(),
            influxdb_type: None,
            stats: Statistics::String(string_stats),
        };

        let mut int_stats = StatValues::new_with_value(1);
        int_stats.update(&5);
        let int_col = ColumnSummary {
            name: "int".to_string(),
            influxdb_type: None,
            stats: Statistics::I64(int_stats),
        };

        let table_a = TableSummary {
            name: "a".to_string(),
            columns: vec![string_col, int_col],
        };

        let int_col = ColumnSummary {
            name: "int".to_string(),
            influxdb_type: None,
            stats: Statistics::I64(StatValues::new_with_value(10)),
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
            influxdb_type: None,
            stats: Statistics::I64(StatValues::new_with_value(203)),
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
            influxdb_type: None,
            stats: Statistics::Bool(StatValues {
                min: false,
                max: false,
                count: 1,
            }),
        };
        let bool_true = ColumnSummary {
            name: "b".to_string(),
            influxdb_type: None,
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
            influxdb_type: None,
            stats: Statistics::U64(StatValues {
                min: 5,
                max: 23,
                count: 1,
            }),
        };

        let max = ColumnSummary {
            name: "foo".to_string(),
            influxdb_type: None,
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
