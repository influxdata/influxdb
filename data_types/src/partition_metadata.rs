//! This module contains structs that describe the metadata for a partition
//! including schema, summary statistics, and file locations in storage.

use observability_deps::tracing::warn;
use serde::{Deserialize, Serialize};
use std::{
    borrow::{Borrow, Cow},
    iter::FromIterator,
    mem,
    num::NonZeroU64,
    sync::Arc,
};

/// Address of the chunk within the catalog
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct PartitionAddr {
    /// Database name
    pub db_name: Arc<str>,

    /// What table does the chunk belong to?
    pub table_name: Arc<str>,

    /// What partition does the chunk belong to?
    pub partition_key: Arc<str>,
}

impl std::fmt::Display for PartitionAddr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Partition('{}':'{}':'{}')",
            self.db_name, self.table_name, self.partition_key
        )
    }
}

/// Describes the aggregated (across all chunks) summary
/// statistics for each column in a partition
#[derive(Debug, Deserialize, Serialize, PartialEq)]
pub struct PartitionSummary {
    /// The identifier for the partition, the partition key computed from
    /// PartitionRules
    pub key: String,
    pub table: TableSummary,
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
        Self {
            key: key.into(),
            table: TableSummary::from_iter(summaries),
        }
    }
}

/// Metadata and statistics for a Chunk *within* a partition
#[derive(Debug, Deserialize, Serialize, PartialEq, Clone)]
pub struct PartitionChunkSummary {
    pub chunk_id: u32,
    pub table: TableSummary,
}

impl FromIterator<Self> for TableSummary {
    fn from_iter<T: IntoIterator<Item = Self>>(iter: T) -> Self {
        let mut iter = iter.into_iter();
        let mut s = iter.next().expect("must contain at least one element");

        for other in iter {
            s.update_from(&other)
        }
        s
    }
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
    pub fn total_count(&self) -> u64 {
        // Assumes that all tables have the same number of rows, so
        // pick the first one
        let count = self.columns.get(0).map(|c| c.total_count()).unwrap_or(0);

        // Validate that the counts are consistent across columns
        for c in &self.columns {
            // Restore to assert when https://github.com/influxdata/influxdb_iox/issues/2124 is fixed
            if c.total_count() != count {
                warn!(table_name=%self.name, column_name=%c.name,
                      column_count=c.total_count(), previous_count=count,
                      "Mismatch in statistics count, see #2124");
            }
        }
        count
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

    /// Updates the table summary with combined stats from the other. Counts are
    /// treated as non-overlapping so they're just added together. If the
    /// type of a column differs between the two tables, no update is done
    /// on that column. Columns that only exist in the other are cloned into
    /// this table summary.
    pub fn update_from(&mut self, other: &Self) {
        assert_eq!(self.name, other.name);

        let new_total_count = self.total_count() + other.total_count();

        // update all existing columns
        for col in &mut self.columns {
            if let Some(other_col) = other.column(&col.name) {
                col.update_from(other_col);
            } else {
                col.update_to_total_count(new_total_count);
            }
        }

        // Add any columns that were new
        for col in &other.columns {
            if self.column(&col.name).is_none() {
                let mut new_col = col.clone();
                // ensure the count is consistent
                new_col.update_to_total_count(new_total_count);
                self.columns.push(new_col);
            }
        }
    }

    /// Get the column summary by name.
    pub fn column(&self, name: &str) -> Option<&ColumnSummary> {
        self.columns.iter().find(|c| c.name == name)
    }
}

/// Delete information
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct DeleteInfo {
    /// Database name
    pub db_name: Arc<str>,

    /// Table with data to delete
    pub table_name: Arc<str>,

    /// Delete Predicate
    // Ideally, this can be any complicated expressions that DataFusion supports
    // but in our first version, we only support what our read buffer does which is
    // conjunctive expressions with columns being compared to literals using = or != operators.
    pub delete_predicate: Arc<str>,

    /// Start time range of deleting data
    pub start_time: Arc<str>,

    /// Stop time range of deleting data
    pub stop_time: Arc<str>,
}

impl std::fmt::Display for DeleteInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Delete('{}':'{}':'{}')",
            self.db_name, self.table_name, self.delete_predicate
        )
    }
}

// Replicate this enum here as it can't be derived from the existing statistics
#[derive(Debug, Deserialize, Serialize, PartialEq, Clone)]
pub enum InfluxDbType {
    IOx,
    Tag,
    Field,
    Timestamp,
}

impl InfluxDbType {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::IOx => "IOx",
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

    /// Per column
    pub stats: Statistics,
}

impl ColumnSummary {
    /// Returns the total number of rows (including nulls) in this column
    pub fn total_count(&self) -> u64 {
        self.stats.total_count()
    }

    /// Returns the number of null values in this column
    pub fn null_count(&self) -> u64 {
        self.stats.null_count()
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

    /// Updates these statistics so that that the total length of this
    /// column is `len` rows, padding it with trailing NULLs if
    /// necessary
    pub fn update_to_total_count(&mut self, len: u64) {
        let total_count = self.total_count();
        assert!(
            total_count <= len,
            "trying to shrink column stats from {} to {}",
            total_count,
            len
        );
        let delta = len - total_count;
        self.stats.update_for_nulls(delta);
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
    pub fn total_count(&self) -> u64 {
        self.stats.total_count()
    }

    /// Returns the number of nulls in this column
    pub fn null_count(&self) -> u64 {
        self.stats.null_count()
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
    pub fn total_count(&self) -> u64 {
        match self {
            Self::I64(s) => s.total_count,
            Self::U64(s) => s.total_count,
            Self::F64(s) => s.total_count,
            Self::Bool(s) => s.total_count,
            Self::String(s) => s.total_count,
        }
    }

    /// Returns the number of null rows in this column
    pub fn null_count(&self) -> u64 {
        match self {
            Self::I64(s) => s.null_count,
            Self::U64(s) => s.null_count,
            Self::F64(s) => s.null_count,
            Self::Bool(s) => s.null_count,
            Self::String(s) => s.null_count,
        }
    }

    /// Returns the distinct count if known
    pub fn distinct_count(&self) -> Option<NonZeroU64> {
        match self {
            Self::I64(s) => s.distinct_count,
            Self::U64(s) => s.distinct_count,
            Self::F64(s) => s.distinct_count,
            Self::Bool(s) => s.distinct_count,
            Self::String(s) => s.distinct_count,
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

    /// Update the statistics values to account for `num_nulls` additional null values
    pub fn update_for_nulls(&mut self, num_nulls: u64) {
        match self {
            Self::I64(v) => v.update_for_nulls(num_nulls),
            Self::U64(v) => v.update_for_nulls(num_nulls),
            Self::F64(v) => v.update_for_nulls(num_nulls),
            Self::Bool(v) => v.update_for_nulls(num_nulls),
            Self::String(v) => v.update_for_nulls(num_nulls),
        }
    }
}

/// Summary statistics for a column.
#[derive(Debug, Deserialize, Serialize, Clone, Eq, PartialEq)]
pub struct StatValues<T> {
    /// minimum (non-NaN, non-NULL) value, if any
    pub min: Option<T>,

    /// maximum (non-NaN, non-NULL) value, if any
    pub max: Option<T>,

    /// total number of values in this column, including null values
    pub total_count: u64,

    /// number of null values in this column
    pub null_count: u64,

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
            total_count: 0,
            null_count: 0,
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

        let min = starting_value.clone();
        let max = starting_value;
        let total_count = 1;
        let null_count = 0;
        let distinct_count = None;
        Self::new_with_distinct(min, max, total_count, null_count, distinct_count)
    }

    /// Create new statitics with the specified count and null count
    pub fn new(min: Option<T>, max: Option<T>, total_count: u64, null_count: u64) -> Self {
        let distinct_count = None;
        Self::new_with_distinct(min, max, total_count, null_count, distinct_count)
    }

    /// Create new statitics with the specified count and null count and distinct values
    fn new_with_distinct(
        min: Option<T>,
        max: Option<T>,
        total_count: u64,
        null_count: u64,
        distinct_count: Option<NonZeroU64>,
    ) -> Self {
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
            total_count,
            null_count,
            distinct_count,
        }
    }

    /// Create statistics for a column with zero nulls and unknown distinct count
    pub fn new_non_null(min: Option<T>, max: Option<T>, total_count: u64) -> Self {
        let null_count = 0;
        let distinct_count = None;
        Self::new_with_distinct(min, max, total_count, null_count, distinct_count)
    }

    /// Create statistics for a column that only has nulls up to now
    pub fn new_all_null(total_count: u64) -> Self {
        let min = None;
        let max = None;
        let null_count = total_count;
        let distinct_count = NonZeroU64::new(1);
        Self::new_with_distinct(min, max, total_count, null_count, distinct_count)
    }

    pub fn update_from(&mut self, other: &Self) {
        self.total_count += other.total_count;
        self.null_count += other.null_count;

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
        self.total_count += 1;
        self.distinct_count = None;

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

    /// Update the statistics values to account for `num_nulls` additional null values
    pub fn update_for_nulls(&mut self, num_nulls: u64) {
        self.total_count += num_nulls;
        self.null_count += num_nulls;
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
    fn statistics_new_non_null() {
        let actual = StatValues::new_non_null(Some(-1i64), Some(1i64), 3);
        let expected = StatValues {
            min: Some(-1i64),
            max: Some(1i64),
            total_count: 3,
            null_count: 0,
            distinct_count: None,
        };
        assert_eq!(actual, expected);
    }

    #[test]
    fn statistics_new_all_null() {
        let actual = StatValues::<i64>::new_all_null(3);
        let expected = StatValues {
            min: None,
            max: None,
            total_count: 3,
            null_count: 3,
            distinct_count: NonZeroU64::new(1),
        };
        assert_eq!(actual, expected);
    }

    #[test]
    fn statistics_update() {
        let mut stat = StatValues::new_with_value(23);
        assert_eq!(stat.min, Some(23));
        assert_eq!(stat.max, Some(23));
        assert_eq!(stat.total_count, 1);

        stat.update(&55);
        assert_eq!(stat.min, Some(23));
        assert_eq!(stat.max, Some(55));
        assert_eq!(stat.total_count, 2);

        stat.update(&6);
        assert_eq!(stat.min, Some(6));
        assert_eq!(stat.max, Some(55));
        assert_eq!(stat.total_count, 3);

        stat.update(&30);
        assert_eq!(stat.min, Some(6));
        assert_eq!(stat.max, Some(55));
        assert_eq!(stat.total_count, 4);
    }

    #[test]
    fn statistics_default() {
        let mut stat = StatValues::default();
        assert_eq!(stat.min, None);
        assert_eq!(stat.max, None);
        assert_eq!(stat.total_count, 0);

        stat.update(&55);
        assert_eq!(stat.min, Some(55));
        assert_eq!(stat.max, Some(55));
        assert_eq!(stat.total_count, 1);

        let mut stat = StatValues::<String>::default();
        assert_eq!(stat.min, None);
        assert_eq!(stat.max, None);
        assert_eq!(stat.total_count, 0);

        stat.update("cupcakes");
        assert_eq!(stat.min, Some("cupcakes".to_string()));
        assert_eq!(stat.max, Some("cupcakes".to_string()));
        assert_eq!(stat.total_count, 1);

        stat.update("woo");
        assert_eq!(stat.min, Some("cupcakes".to_string()));
        assert_eq!(stat.max, Some("woo".to_string()));
        assert_eq!(stat.total_count, 2);
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
        assert_eq!(stat.total_count, 1);

        stat.update("aaa");
        assert_eq!(stat.min, Some("aaa".to_string()));
        assert_eq!(stat.max, Some("bbb".to_string()));
        assert_eq!(stat.total_count, 2);

        stat.update("z");
        assert_eq!(stat.min, Some("aaa".to_string()));
        assert_eq!(stat.max, Some("z".to_string()));
        assert_eq!(stat.total_count, 3);

        stat.update("p");
        assert_eq!(stat.min, Some("aaa".to_string()));
        assert_eq!(stat.max, Some("z".to_string()));
        assert_eq!(stat.total_count, 4);
    }

    #[test]
    fn stats_is_none() {
        let stat = Statistics::I64(StatValues::new_non_null(Some(-1), Some(100), 1));
        assert!(!stat.is_none());

        let stat = Statistics::I64(StatValues::new_non_null(None, Some(100), 1));
        assert!(!stat.is_none());

        let stat = Statistics::I64(StatValues::new_non_null(None, None, 0));
        assert!(stat.is_none());
    }

    #[test]
    fn stats_as_str_i64() {
        let stat = Statistics::I64(StatValues::new_non_null(Some(-1), Some(100), 1));
        assert_eq!(stat.min_as_str(), Some("-1".into()));
        assert_eq!(stat.max_as_str(), Some("100".into()));

        let stat = Statistics::I64(StatValues::new_non_null(None, None, 1));
        assert_eq!(stat.min_as_str(), None);
        assert_eq!(stat.max_as_str(), None);
    }

    #[test]
    fn stats_as_str_u64() {
        let stat = Statistics::U64(StatValues::new_non_null(Some(1), Some(100), 1));
        assert_eq!(stat.min_as_str(), Some("1".into()));
        assert_eq!(stat.max_as_str(), Some("100".into()));

        let stat = Statistics::U64(StatValues::new_non_null(None, None, 1));
        assert_eq!(stat.min_as_str(), None);
        assert_eq!(stat.max_as_str(), None);
    }

    #[test]
    fn stats_as_str_f64() {
        let stat = Statistics::F64(StatValues::new_non_null(Some(99.0), Some(101.0), 1));
        assert_eq!(stat.min_as_str(), Some("99".into()));
        assert_eq!(stat.max_as_str(), Some("101".into()));

        let stat = Statistics::F64(StatValues::new_non_null(None, None, 1));
        assert_eq!(stat.min_as_str(), None);
        assert_eq!(stat.max_as_str(), None);
    }

    #[test]
    fn stats_as_str_bool() {
        let stat = Statistics::Bool(StatValues::new_non_null(Some(false), Some(true), 1));
        assert_eq!(stat.min_as_str(), Some("false".into()));
        assert_eq!(stat.max_as_str(), Some("true".into()));

        let stat = Statistics::Bool(StatValues::new_non_null(None, None, 1));
        assert_eq!(stat.min_as_str(), None);
        assert_eq!(stat.max_as_str(), None);
    }

    #[test]
    fn stats_as_str_str() {
        let stat = Statistics::String(StatValues::new_non_null(
            Some("a".to_string()),
            Some("zz".to_string()),
            1,
        ));
        assert_eq!(stat.min_as_str(), Some("a".into()));
        assert_eq!(stat.max_as_str(), Some("zz".into()));

        let stat = Statistics::String(StatValues::new_non_null(None, None, 1));
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
            Statistics::String(StatValues::new_non_null(
                Some("aaa".to_string()),
                Some("zzz".to_string()),
                4,
            ))
        );

        let col = table_a.column("int").unwrap();
        assert_eq!(
            col.stats,
            Statistics::I64(StatValues::new_non_null(Some(1), Some(9), 4))
        );

        let col = table_a.column("float").unwrap();
        assert_eq!(
            col.stats,
            Statistics::F64(StatValues::new(Some(1.3), Some(9.1), 4, 2))
        );

        table_b.update_from(&table_c);
        let col = table_b.column("string").unwrap();
        assert_eq!(
            col.stats,
            Statistics::String(StatValues::new_non_null(
                Some("aaa".to_string()),
                Some("zzz".to_string()),
                4,
            ))
        );

        let col = table_b.column("int").unwrap();
        assert_eq!(
            col.stats,
            Statistics::I64(StatValues::new_non_null(Some(1), Some(9), 4))
        );

        let col = table_b.column("float").unwrap();
        assert_eq!(
            col.stats,
            Statistics::F64(StatValues::new(Some(1.3), Some(9.1), 4, 2))
        );
    }

    #[test]
    fn table_update_from_new_column() {
        let string_stats = StatValues::new_with_value("bar".to_string());
        let string_col = ColumnSummary {
            name: "string".to_string(),
            influxdb_type: None,
            stats: Statistics::String(string_stats),
        };

        let int_stats = StatValues::new_with_value(5);
        let int_col = ColumnSummary {
            name: "int".to_string(),
            influxdb_type: None,
            stats: Statistics::I64(int_stats),
        };

        // table summary that does not have the "string" col
        let table1 = TableSummary {
            name: "the_table".to_string(),
            columns: vec![int_col.clone()],
        };

        // table summary that has both columns
        let table2 = TableSummary {
            name: "the_table".to_string(),
            columns: vec![int_col, string_col],
        };

        // Statistics should be the same regardless of the order we update the stats

        let expected_string_stats = Statistics::String(StatValues::new(
            Some("bar".to_string()),
            Some("bar".to_string()),
            2, // total count is 2 even though did not appear in the update
            1, // 1 null
        ));

        let expected_int_stats = Statistics::I64(StatValues::new(
            Some(5),
            Some(5),
            2,
            0, // no nulls
        ));

        // update table 1 with table 2
        let mut table = table1.clone();
        table.update_from(&table2);

        assert_eq!(
            &table.column("string").unwrap().stats,
            &expected_string_stats
        );

        assert_eq!(&table.column("int").unwrap().stats, &expected_int_stats);

        // update table 2 with table 1
        let mut table = table2;
        table.update_from(&table1);

        assert_eq!(
            &table.column("string").unwrap().stats,
            &expected_string_stats
        );

        assert_eq!(&table.column("int").unwrap().stats, &expected_int_stats);
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
        let table_a_2 = TableSummary {
            name: "a".to_string(),
            columns: vec![int_col],
        };
        let partition = PartitionSummary::from_table_summaries("key", vec![table_a, table_a_2]);
        let t = partition.table;
        let col = t.column("string").unwrap();
        assert_eq!(
            col.stats,
            Statistics::String(StatValues::new(
                Some("bar".to_string()),
                Some("foo".to_string()),
                3, // no entry for "string" column in table_a_2
                1,
            ))
        );
        let col = t.column("int").unwrap();
        assert_eq!(
            col.stats,
            Statistics::I64(StatValues::new_non_null(Some(1), Some(10), 3))
        );
    }

    #[test]
    fn column_update_from_boolean() {
        let bool_false = ColumnSummary {
            name: "b".to_string(),
            influxdb_type: None,
            stats: Statistics::Bool(StatValues::new(Some(false), Some(false), 1, 1)),
        };
        let bool_true = ColumnSummary {
            name: "b".to_string(),
            influxdb_type: None,
            stats: Statistics::Bool(StatValues::new(Some(true), Some(true), 1, 2)),
        };

        let expected_stats = Statistics::Bool(StatValues::new(Some(false), Some(true), 2, 3));

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
            stats: Statistics::U64(StatValues::new(Some(5), Some(23), 1, 1)),
        };

        let max = ColumnSummary {
            name: "foo".to_string(),
            influxdb_type: None,
            stats: Statistics::U64(StatValues::new(Some(6), Some(506), 43, 2)),
        };

        min.update_from(&max);

        let expected = Statistics::U64(StatValues::new(Some(5), Some(506), 44, 3));
        assert_eq!(min.stats, expected);
    }

    #[test]
    fn nans() {
        let mut stat = StatValues::default();
        assert_eq!(stat.min, None);
        assert_eq!(stat.max, None);
        assert_eq!(stat.total_count, 0);

        stat.update(&f64::NAN);
        assert_eq!(stat.min, None);
        assert_eq!(stat.max, None);
        assert_eq!(stat.total_count, 1);

        stat.update(&1.0);
        assert_eq!(stat.min, Some(1.0));
        assert_eq!(stat.max, Some(1.0));
        assert_eq!(stat.total_count, 2);

        stat.update(&2.0);
        assert_eq!(stat.min, Some(1.0));
        assert_eq!(stat.max, Some(2.0));
        assert_eq!(stat.total_count, 3);

        stat.update(&f64::INFINITY);
        assert_eq!(stat.min, Some(1.0));
        assert_eq!(stat.max, Some(f64::INFINITY));
        assert_eq!(stat.total_count, 4);

        stat.update(&-1.0);
        assert_eq!(stat.min, Some(-1.0));
        assert_eq!(stat.max, Some(f64::INFINITY));
        assert_eq!(stat.total_count, 5);

        // ===========

        let mut stat = StatValues::new_with_value(2.0);
        stat.update(&f64::INFINITY);
        assert_eq!(stat.min, Some(2.0));
        assert_eq!(stat.max, Some(f64::INFINITY));
        assert_eq!(stat.total_count, 2);

        stat.update(&f64::NAN);
        assert_eq!(stat.min, Some(2.0));
        assert_eq!(stat.max, Some(f64::INFINITY));
        assert_eq!(stat.total_count, 3);

        // ===========

        let mut stat2 = StatValues::new_with_value(1.0);
        stat2.update_from(&stat);
        assert_eq!(stat2.min, Some(1.0));
        assert_eq!(stat.max, Some(f64::INFINITY));
        assert_eq!(stat2.total_count, 4);

        // ===========

        let stat2 = StatValues::new_with_value(1.0);
        stat.update_from(&stat2);
        assert_eq!(stat.min, Some(1.0));
        assert_eq!(stat.max, Some(f64::INFINITY));
        assert_eq!(stat.total_count, 4);

        // ===========

        let stat = StatValues::new_with_value(f64::NAN);
        assert_eq!(stat.min, None);
        assert_eq!(stat.max, None);
        assert_eq!(stat.total_count, 1);
    }
}
