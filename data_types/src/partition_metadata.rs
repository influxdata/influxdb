//! This module contains structs that describe the metadata for a partition
//! including schema, summary statistics, and file locations in storage.

use std::{borrow::Cow, cmp::Ordering, mem};

use serde::{Deserialize, Serialize};
use std::borrow::Borrow;
use std::num::NonZeroU64;

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

/// Metadata and statistics information for a table. This can be
/// either for the portion of a Table stored within a single chunk or
/// aggregated across chunks.
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

    /// Returns the total number of rows in the columns of this summary
    pub fn count(&self) -> u64 {
        // Assumes that all tables have the same number of rows, so
        // pick the first one
        self.columns.get(0).map(|c| c.count()).unwrap_or(0)
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

    /// Return the columns used for the "primary key" in this table.
    ///
    /// Currently this relies on the InfluxDB data model annotations
    /// for what columns to include in the key columns
    pub fn primary_key_columns(&self) -> Vec<&ColumnSummary> {
        use InfluxDbType::*;
        let mut key_summaries: Vec<&ColumnSummary> = self
            .columns
            .iter()
            .filter(|s| match s.influxdb_type {
                Some(Tag) => true,
                Some(Field) => false,
                Some(Timestamp) => true,
                None => false,
            })
            .collect();

        // Now, sort lexographically (but put timestamp last)
        key_summaries.sort_by(
            |a, b| match (a.influxdb_type.as_ref(), b.influxdb_type.as_ref()) {
                (Some(Tag), Some(Tag)) => a.name.cmp(&b.name),
                (Some(Timestamp), Some(Tag)) => Ordering::Greater,
                (Some(Tag), Some(Timestamp)) => Ordering::Less,
                (Some(Timestamp), Some(Timestamp)) => panic!("multiple timestamps in summary"),
                _ => panic!("Unexpected types in key summary"),
            },
        );

        key_summaries
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
        mem::size_of::<Self>() + self.name.len() + self.stats.size()
    }

    // Updates statistics from other if the same type, otherwise a noop
    pub fn update_from(&mut self, other: &Self) {
        match (&mut self.stats, &other.stats) {
            (Statistics::F64(s), Statistics::F64(o)) => {
                s.update_from(o);
            }
            (Statistics::I64(s), Statistics::I64(o)) => {
                s.update_from(o);
            }
            (Statistics::Bool(s), Statistics::Bool(o)) => {
                s.update_from(o);
            }
            (Statistics::String(s), Statistics::String(o)) => {
                s.update_from(o);
            }
            (Statistics::U64(s), Statistics::U64(o)) => {
                s.update_from(o);
            }
            // do catch alls for the specific types, that way if a new type gets added, the compiler
            // will complain.
            (Statistics::F64(_), _) => unreachable!(),
            (Statistics::I64(_), _) => unreachable!(),
            (Statistics::U64(_), _) => unreachable!(),
            (Statistics::Bool(_), _) => unreachable!(),
            (Statistics::String(_), _) => unreachable!(),
        }
    }

    /// Returns true if this column is a part of the primary key which 
    /// is either Tag or Timestamp
    pub fn is_key_part(&self) -> bool {
        matches!(self.influxdb_type, Some(InfluxDbType::Tag) | Some(InfluxDbType::Timestamp))
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

    /// Returns true if both the min and max values are None (aka not known)
    pub fn is_none(&self) -> bool {
        match self {
            Self::I64(v) => v.is_none(),
            Self::U64(v) => v.is_none(),
            Self::F64(v) => v.is_none(),
            Self::Bool(v) => v.is_none(),
            Self::String(v) => v.is_none(),
        }
    }

    /// Return the minimum value, if any, formatted as a string
    pub fn min_as_str(&self) -> Option<Cow<'_, str>> {
        match self {
            Self::I64(v) => v.min.map(|x| Cow::Owned(x.to_string())),
            Self::U64(v) => v.min.map(|x| Cow::Owned(x.to_string())),
            Self::F64(v) => v.min.map(|x| Cow::Owned(x.to_string())),
            Self::Bool(v) => v.min.map(|x| Cow::Owned(x.to_string())),
            Self::String(v) => v.min.as_deref().map(|x| Cow::Borrowed(x)),
        }
    }

    /// Return the maximum value, if any, formatted as a string
    pub fn max_as_str(&self) -> Option<Cow<'_, str>> {
        match self {
            Self::I64(v) => v.max.map(|x| Cow::Owned(x.to_string())),
            Self::U64(v) => v.max.map(|x| Cow::Owned(x.to_string())),
            Self::F64(v) => v.max.map(|x| Cow::Owned(x.to_string())),
            Self::Bool(v) => v.max.map(|x| Cow::Owned(x.to_string())),
            Self::String(v) => v.max.as_deref().map(|x| Cow::Borrowed(x)),
        }
    }

    /// Return the size in bytes of this stats instance
    pub fn size(&self) -> usize {
        match self {
            Self::String(v) => std::mem::size_of::<Self>() + v.string_size(),
            _ => std::mem::size_of::<Self>(),
        }
    }
}

/// Summary statistics for a column.
#[derive(Debug, Deserialize, Serialize, Clone, PartialEq)]
pub struct StatValues<T> {
    /// minimum (non-NaN, non-NULL) value, if any
    pub min: Option<T>,

    /// maximum (non-NaN, non-NULL) value, if any
    pub max: Option<T>,

    /// number of non-nil values in this column
    pub count: u64,

    /// number of distinct values in this column if known
    ///
    /// This includes NULLs and NANs
    pub distinct_count: Option<NonZeroU64>,
}

impl<T> Default for StatValues<T> {
    fn default() -> Self {
        Self {
            min: None,
            max: None,
            count: 0,
            distinct_count: None,
        }
    }
}

impl<T> StatValues<T>
where
    T: Clone + PartialEq + PartialOrd + IsNan,
{
    pub fn new_with_value(starting_value: T) -> Self {
        let starting_value = if starting_value.is_nan() {
            None
        } else {
            Some(starting_value)
        };

        Self {
            min: starting_value.clone(),
            max: starting_value,
            count: 1,
            distinct_count: None,
        }
    }

    pub fn new(min: Option<T>, max: Option<T>, count: u64) -> Self {
        if let Some(min) = &min {
            assert!(!min.is_nan());
        }
        if let Some(max) = &max {
            assert!(!max.is_nan());
        }
        if let (Some(min), Some(max)) = (&min, &max) {
            assert!(min <= max);
        }

        Self {
            min,
            max,
            count,
            distinct_count: None,
        }
    }

    pub fn update_from(&mut self, other: &Self) {
        self.count += other.count;

        // No way to accurately aggregate counts
        self.distinct_count = None;

        match (&self.min, &other.min) {
            (None, None) | (Some(_), None) => {}
            (None, Some(o)) => self.min = Some(o.clone()),
            (Some(s), Some(o)) => {
                if s > o {
                    self.min = Some(o.clone());
                }
            }
        }

        match (&self.max, &other.max) {
            (None, None) | (Some(_), None) => {}
            (None, Some(o)) => self.max = Some(o.clone()),
            (Some(s), Some(o)) => {
                if o > s {
                    self.max = Some(o.clone());
                }
            }
        };
    }

    /// Returns true if both the min and max values are None (aka not known)
    pub fn is_none(&self) -> bool {
        self.min.is_none() && self.max.is_none()
    }
}

impl<T> StatValues<T> {
    /// updates the statistics keeping the min, max and incrementing count.
    ///
    /// The type plumbing exists to allow calling with &str on a StatValues<String>
    pub fn update<U: ?Sized>(&mut self, other: &U)
    where
        T: Borrow<U>,
        U: ToOwned<Owned = T> + PartialOrd + IsNan,
    {
        self.count += 1;

        if !other.is_nan() {
            match &self.min {
                None => self.min = Some(other.to_owned()),
                Some(s) => {
                    if s.borrow() > other {
                        self.min = Some(other.to_owned());
                    }
                }
            }

            match &self.max {
                None => {
                    self.max = Some(other.to_owned());
                }
                Some(s) => {
                    if other > s.borrow() {
                        self.max = Some(other.to_owned());
                    }
                }
            }
        }
    }
}

impl StatValues<String> {
    /// Returns the bytes associated by storing min/max string values
    pub fn string_size(&self) -> usize {
        self.min.as_ref().map(|x| x.len()).unwrap_or(0)
            + self.max.as_ref().map(|x| x.len()).unwrap_or(0)
    }
}

/// Represents the result of comparing the min/max ranges of two [`StatValues`]
#[derive(Debug, PartialEq)]
pub enum StatOverlap {
    /// There is at least one value that exists in both ranges
    NonZero,

    /// There are zero values that exists in both ranges
    Zero,

    /// It is not known if there are any intersections (e.g. because
    /// one of the bounds is not Known / is None)
    Unknown,
}

impl<T> StatValues<T>
where
    T: PartialOrd,
{
    /// returns information about the overlap between two `StatValues`
    pub fn overlaps(&self, other: &Self) -> StatOverlap {
        match (&self.min, &self.max, &other.min, &other.max) {
            (Some(self_min), Some(self_max), Some(other_min), Some(other_max)) => {
                if self_min <= other_max && self_max >= other_min {
                    StatOverlap::NonZero
                } else {
                    StatOverlap::Zero
                }
            }
            // At least one of the values was None
            _ => StatOverlap::Unknown,
        }
    }
}

pub trait IsNan {
    fn is_nan(&self) -> bool;
}

impl<T: IsNan> IsNan for &T {
    fn is_nan(&self) -> bool {
        (*self).is_nan()
    }
}

macro_rules! impl_is_nan_false {
    ($t:ty) => {
        impl IsNan for $t {
            fn is_nan(&self) -> bool {
                false
            }
        }
    };
}

impl_is_nan_false!(bool);
impl_is_nan_false!(str);
impl_is_nan_false!(String);
impl_is_nan_false!(i8);
impl_is_nan_false!(i16);
impl_is_nan_false!(i32);
impl_is_nan_false!(i64);
impl_is_nan_false!(u8);
impl_is_nan_false!(u16);
impl_is_nan_false!(u32);
impl_is_nan_false!(u64);

impl IsNan for f64 {
    fn is_nan(&self) -> bool {
        Self::is_nan(*self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn statistics_update() {
        let mut stat = StatValues::new_with_value(23);
        assert_eq!(stat.min, Some(23));
        assert_eq!(stat.max, Some(23));
        assert_eq!(stat.count, 1);

        stat.update(&55);
        assert_eq!(stat.min, Some(23));
        assert_eq!(stat.max, Some(55));
        assert_eq!(stat.count, 2);

        stat.update(&6);
        assert_eq!(stat.min, Some(6));
        assert_eq!(stat.max, Some(55));
        assert_eq!(stat.count, 3);

        stat.update(&30);
        assert_eq!(stat.min, Some(6));
        assert_eq!(stat.max, Some(55));
        assert_eq!(stat.count, 4);
    }

    #[test]
    fn statistics_default() {
        let mut stat = StatValues::default();
        assert_eq!(stat.min, None);
        assert_eq!(stat.max, None);
        assert_eq!(stat.count, 0);

        stat.update(&55);
        assert_eq!(stat.min, Some(55));
        assert_eq!(stat.max, Some(55));
        assert_eq!(stat.count, 1);

        let mut stat = StatValues::<String>::default();
        assert_eq!(stat.min, None);
        assert_eq!(stat.max, None);
        assert_eq!(stat.count, 0);

        stat.update("cupcakes");
        assert_eq!(stat.min, Some("cupcakes".to_string()));
        assert_eq!(stat.max, Some("cupcakes".to_string()));
        assert_eq!(stat.count, 1);

        stat.update("woo");
        assert_eq!(stat.min, Some("cupcakes".to_string()));
        assert_eq!(stat.max, Some("woo".to_string()));
        assert_eq!(stat.count, 2);
    }

    #[test]
    fn statistics_is_none() {
        let mut stat = StatValues::default();
        assert!(stat.is_none());
        stat.min = Some(0);
        assert!(!stat.is_none());
        stat.max = Some(1);
        assert!(!stat.is_none());
    }

    #[test]
    fn statistics_overlaps() {
        let stat1 = StatValues {
            min: Some(10),
            max: Some(20),
            ..Default::default()
        };
        assert_eq!(stat1.overlaps(&stat1), StatOverlap::NonZero);

        //    [--stat1--]
        // [--stat2--]
        let stat2 = StatValues {
            min: Some(5),
            max: Some(15),
            ..Default::default()
        };
        assert_eq!(stat1.overlaps(&stat2), StatOverlap::NonZero);
        assert_eq!(stat2.overlaps(&stat1), StatOverlap::NonZero);

        //    [--stat1--]
        //        [--stat3--]
        let stat3 = StatValues {
            min: Some(15),
            max: Some(25),
            ..Default::default()
        };
        assert_eq!(stat1.overlaps(&stat3), StatOverlap::NonZero);
        assert_eq!(stat3.overlaps(&stat1), StatOverlap::NonZero);

        //    [--stat1--]
        //                [--stat4--]
        let stat4 = StatValues {
            min: Some(25),
            max: Some(35),
            ..Default::default()
        };
        assert_eq!(stat1.overlaps(&stat4), StatOverlap::Zero);
        assert_eq!(stat4.overlaps(&stat1), StatOverlap::Zero);

        //              [--stat1--]
        // [--stat5--]
        let stat5 = StatValues {
            min: Some(0),
            max: Some(5),
            ..Default::default()
        };
        assert_eq!(stat1.overlaps(&stat5), StatOverlap::Zero);
        assert_eq!(stat5.overlaps(&stat1), StatOverlap::Zero);
    }

    #[test]
    fn statistics_overlaps_none() {
        let stat1 = StatValues {
            min: Some(10),
            max: Some(20),
            ..Default::default()
        };

        let stat2 = StatValues {
            min: None,
            max: Some(20),
            ..Default::default()
        };
        assert_eq!(stat1.overlaps(&stat2), StatOverlap::Unknown);
        assert_eq!(stat2.overlaps(&stat1), StatOverlap::Unknown);

        let stat3 = StatValues {
            min: Some(10),
            max: None,
            ..Default::default()
        };
        assert_eq!(stat1.overlaps(&stat3), StatOverlap::Unknown);
        assert_eq!(stat3.overlaps(&stat1), StatOverlap::Unknown);

        let stat4 = StatValues {
            min: None,
            max: None,
            ..Default::default()
        };
        assert_eq!(stat1.overlaps(&stat4), StatOverlap::Unknown);
        assert_eq!(stat4.overlaps(&stat1), StatOverlap::Unknown);
    }

    #[test]
    fn statistics_overlaps_mixed_none() {
        let stat1 = StatValues {
            min: Some(10),
            max: None,
            ..Default::default()
        };

        let stat2 = StatValues {
            min: None,
            max: Some(5),
            ..Default::default()
        };
        assert_eq!(stat1.overlaps(&stat2), StatOverlap::Unknown);
        assert_eq!(stat2.overlaps(&stat1), StatOverlap::Unknown);
    }

    #[test]
    fn update_string() {
        let mut stat = StatValues::new_with_value("bbb".to_string());
        assert_eq!(stat.min, Some("bbb".to_string()));
        assert_eq!(stat.max, Some("bbb".to_string()));
        assert_eq!(stat.count, 1);

        stat.update("aaa");
        assert_eq!(stat.min, Some("aaa".to_string()));
        assert_eq!(stat.max, Some("bbb".to_string()));
        assert_eq!(stat.count, 2);

        stat.update("z");
        assert_eq!(stat.min, Some("aaa".to_string()));
        assert_eq!(stat.max, Some("z".to_string()));
        assert_eq!(stat.count, 3);

        stat.update("p");
        assert_eq!(stat.min, Some("aaa".to_string()));
        assert_eq!(stat.max, Some("z".to_string()));
        assert_eq!(stat.count, 4);
    }

    #[test]
    fn stats_is_none() {
        let stat = Statistics::I64(StatValues::new(Some(-1), Some(100), 1));
        assert!(!stat.is_none());

        let stat = Statistics::I64(StatValues::new(None, Some(100), 1));
        assert!(!stat.is_none());

        let stat = Statistics::I64(StatValues::new(None, None, 0));
        assert!(stat.is_none());
    }

    #[test]
    fn stats_as_str_i64() {
        let stat = Statistics::I64(StatValues::new(Some(-1), Some(100), 1));
        assert_eq!(stat.min_as_str(), Some("-1".into()));
        assert_eq!(stat.max_as_str(), Some("100".into()));

        let stat = Statistics::I64(StatValues::new(None, None, 1));
        assert_eq!(stat.min_as_str(), None);
        assert_eq!(stat.max_as_str(), None);
    }

    #[test]
    fn stats_as_str_u64() {
        let stat = Statistics::U64(StatValues::new(Some(1), Some(100), 1));
        assert_eq!(stat.min_as_str(), Some("1".into()));
        assert_eq!(stat.max_as_str(), Some("100".into()));

        let stat = Statistics::U64(StatValues::new(None, None, 1));
        assert_eq!(stat.min_as_str(), None);
        assert_eq!(stat.max_as_str(), None);
    }

    #[test]
    fn stats_as_str_f64() {
        let stat = Statistics::F64(StatValues::new(Some(99.0), Some(101.0), 1));
        assert_eq!(stat.min_as_str(), Some("99".into()));
        assert_eq!(stat.max_as_str(), Some("101".into()));

        let stat = Statistics::F64(StatValues::new(None, None, 1));
        assert_eq!(stat.min_as_str(), None);
        assert_eq!(stat.max_as_str(), None);
    }

    #[test]
    fn stats_as_str_bool() {
        let stat = Statistics::Bool(StatValues::new(Some(false), Some(true), 1));
        assert_eq!(stat.min_as_str(), Some("false".into()));
        assert_eq!(stat.max_as_str(), Some("true".into()));

        let stat = Statistics::Bool(StatValues::new(None, None, 1));
        assert_eq!(stat.min_as_str(), None);
        assert_eq!(stat.max_as_str(), None);
    }

    #[test]
    fn stats_as_str_str() {
        let stat = Statistics::String(StatValues::new(
            Some("a".to_string()),
            Some("zz".to_string()),
            1,
        ));
        assert_eq!(stat.min_as_str(), Some("a".into()));
        assert_eq!(stat.max_as_str(), Some("zz".into()));

        let stat = Statistics::String(StatValues::new(None, None, 1));
        assert_eq!(stat.min_as_str(), None);
        assert_eq!(stat.max_as_str(), None);
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
            Statistics::String(StatValues::new(
                Some("aaa".to_string()),
                Some("zzz".to_string()),
                4
            ))
        );

        let col = table_a.column("int").unwrap();
        assert_eq!(
            col.stats,
            Statistics::I64(StatValues::new(Some(1), Some(9), 4))
        );

        let col = table_a.column("float").unwrap();
        assert_eq!(
            col.stats,
            Statistics::F64(StatValues::new(Some(1.3), Some(9.1), 2))
        );

        table_b.update_from(&table_c);
        let col = table_b.column("string").unwrap();
        assert_eq!(
            col.stats,
            Statistics::String(StatValues::new(
                Some("aaa".to_string()),
                Some("zzz".to_string()),
                4
            ))
        );

        let col = table_b.column("int").unwrap();
        assert_eq!(
            col.stats,
            Statistics::I64(StatValues::new(Some(1), Some(9), 4))
        );

        let col = table_b.column("float").unwrap();
        assert_eq!(
            col.stats,
            Statistics::F64(StatValues::new(Some(1.3), Some(9.1), 2))
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
            Statistics::String(StatValues::new(
                Some("bar".to_string()),
                Some("foo".to_string()),
                2
            ))
        );
        let col = t.column("int").unwrap();
        assert_eq!(
            col.stats,
            Statistics::I64(StatValues::new(Some(1), Some(10), 3))
        );
        let t = partition.table("b").unwrap();
        let col = t.column("int").unwrap();
        assert_eq!(
            col.stats,
            Statistics::I64(StatValues::new(Some(10), Some(203), 2))
        );
    }

    #[test]
    fn column_update_from_boolean() {
        let bool_false = ColumnSummary {
            name: "b".to_string(),
            influxdb_type: None,
            stats: Statistics::Bool(StatValues::new(Some(false), Some(false), 1)),
        };
        let bool_true = ColumnSummary {
            name: "b".to_string(),
            influxdb_type: None,
            stats: Statistics::Bool(StatValues::new(Some(true), Some(true), 1)),
        };

        let expected_stats = Statistics::Bool(StatValues::new(Some(false), Some(true), 2));

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
            stats: Statistics::U64(StatValues::new(Some(5), Some(23), 1)),
        };

        let max = ColumnSummary {
            name: "foo".to_string(),
            influxdb_type: None,
            stats: Statistics::U64(StatValues::new(Some(6), Some(506), 43)),
        };

        min.update_from(&max);

        let expected = Statistics::U64(StatValues::new(Some(5), Some(506), 44));
        assert_eq!(min.stats, expected);
    }

    #[test]
    fn nans() {
        let mut stat = StatValues::default();
        assert_eq!(stat.min, None);
        assert_eq!(stat.max, None);
        assert_eq!(stat.count, 0);

        stat.update(&f64::NAN);
        assert_eq!(stat.min, None);
        assert_eq!(stat.max, None);
        assert_eq!(stat.count, 1);

        stat.update(&1.0);
        assert_eq!(stat.min, Some(1.0));
        assert_eq!(stat.max, Some(1.0));
        assert_eq!(stat.count, 2);

        stat.update(&2.0);
        assert_eq!(stat.min, Some(1.0));
        assert_eq!(stat.max, Some(2.0));
        assert_eq!(stat.count, 3);

        stat.update(&f64::INFINITY);
        assert_eq!(stat.min, Some(1.0));
        assert_eq!(stat.max, Some(f64::INFINITY));
        assert_eq!(stat.count, 4);

        stat.update(&-1.0);
        assert_eq!(stat.min, Some(-1.0));
        assert_eq!(stat.max, Some(f64::INFINITY));
        assert_eq!(stat.count, 5);

        // ===========

        let mut stat = StatValues::new_with_value(2.0);
        stat.update(&f64::INFINITY);
        assert_eq!(stat.min, Some(2.0));
        assert_eq!(stat.max, Some(f64::INFINITY));
        assert_eq!(stat.count, 2);

        stat.update(&f64::NAN);
        assert_eq!(stat.min, Some(2.0));
        assert_eq!(stat.max, Some(f64::INFINITY));
        assert_eq!(stat.count, 3);

        // ===========

        let mut stat2 = StatValues::new_with_value(1.0);
        stat2.update_from(&stat);
        assert_eq!(stat2.min, Some(1.0));
        assert_eq!(stat.max, Some(f64::INFINITY));
        assert_eq!(stat2.count, 4);

        // ===========

        let stat2 = StatValues::new_with_value(1.0);
        stat.update_from(&stat2);
        assert_eq!(stat.min, Some(1.0));
        assert_eq!(stat.max, Some(f64::INFINITY));
        assert_eq!(stat.count, 4);

        // ===========

        let stat = StatValues::new_with_value(f64::NAN);
        assert_eq!(stat.min, None);
        assert_eq!(stat.max, None);
        assert_eq!(stat.count, 1);
    }
}
