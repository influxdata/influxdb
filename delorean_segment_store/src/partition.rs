use std::collections::{BTreeMap, BTreeSet};

use crate::column::{AggregateResult, AggregateType, Values};
use crate::segment::{ColumnName, GroupKey};
use crate::table::Table;

// The name of a measurement, i.e., a table name.
type MeasurementName = String;

/// A Partition comprises a collection of tables that have been organised
/// according to a partition rule, e.g., based on time or arrival, some column
/// value etc.
///
/// A partition's key uniquely identifies a property that all tables within the
/// the partition share.
///
/// Each table within a partition can be identified by its measurement name.
pub struct Partition<'a> {
    // The partition key uniquely identifies this partition.
    key: String,

    // Metadata about this partition.
    meta: MetaData,

    // The set of tables within this partition. Each table is identified by
    // a measurement name.
    tables: BTreeMap<MeasurementName, Table<'a>>,
}

impl<'a> Partition<'a> {
    pub fn new(key: String, table: Table<'a>) -> Self {
        let mut p = Self {
            key,
            meta: MetaData::new(&table),
            tables: BTreeMap::new(),
        };
        p.tables.insert(table.name().to_owned(), table);
        p
    }

    /// Returns vectors of columnar data for the specified column
    /// selections on the specified table name (measurement).
    ///
    /// Results may be filtered by (currently only) equality predicates, but can
    /// be ranged by time, which should be represented as nanoseconds since the
    /// epoch. Results are included if they satisfy the predicate and fall
    /// with the [min, max) time range domain.
    pub fn select(
        &self,
        table_name: &str,
        time_range: (i64, i64),
        predicates: &[(&str, &str)],
        select_columns: Vec<ColumnName>,
    ) -> BTreeMap<ColumnName, Values> {
        // Find the measurement name on the partition and dispatch query to the
        // table for that measurement if the partition's time range overlaps the
        // requested time range.
        todo!();
    }

    /// Returns aggregates segmented by grouping keys for the specified
    /// measurement.
    ///
    /// The set of data to be aggregated may be filtered by (currently only)
    /// equality predicates, but can be ranged by time, which should be
    /// represented as nanoseconds since the epoch. Results are included if they
    /// satisfy the predicate and fall with the [min, max) time range domain.
    ///
    /// Group keys are determined according to the provided group column names.
    /// Currently only grouping by string (tag key) columns is supported.
    ///
    /// Required aggregates are specified via a tuple comprising a column name
    /// and the type of aggregation required. Multiple aggregations can be
    /// applied to the same column.
    pub fn aggregate(
        &self,
        table_name: &str,
        time_range: (i64, i64),
        predicates: &[(&str, &str)],
        group_columns: Vec<ColumnName>,
        aggregates: Vec<(ColumnName, AggregateType)>,
    ) -> BTreeMap<GroupKey, Vec<(ColumnName, AggregateResult<'_>)>> {
        // Find the measurement name on the partition and dispatch query to the
        // table for that measurement if the partition's time range overlaps the
        // requested time range.
        todo!()
    }

    //
    // ---- Schema API queries
    //

    /// Returns the distinct set of table names that contain data that satisfies
    /// the time range and predicates.
    pub fn table_names(
        &self,
        time_range: (i64, i64),
        predicates: &[(&str, &str)],
    ) -> BTreeSet<String> {
        //
        // TODO(edd): do we want to add the ability to apply a predicate to the
        // table names? For example, a regex where you only want table names
        // beginning with /cpu.+/ or something?
        todo!()
    }

    /// Returns the distinct set of tag keys (column names) matching the provided
    /// optional predicates and time range.
    pub fn tag_keys(
        &self,
        table_name: String,
        time_range: (i64, i64),
        predicates: &[(&str, &str)],
        found_keys: &BTreeSet<ColumnName>,
    ) -> BTreeSet<ColumnName> {
        // Dispatch query to the table for the provided measurement if the
        // partition's time range overlaps the requested time range *and* there
        // exists columns in the table's schema that are *not* already found.
        todo!();
    }

    /// Returns the distinct set of tag values (column values) for each provided
    /// tag key, where each returned value lives in a row matching the provided
    /// optional predicates and time range.
    ///
    /// As a special case, if `tag_keys` is empty then all distinct values for
    /// all columns (tag keys) are returned for the partition.
    pub fn tag_values(
        &self,
        table_name: String,
        time_range: (i64, i64),
        predicates: &[(&str, &str)],
        tag_keys: &[ColumnName],
        found_tag_values: &BTreeMap<ColumnName, BTreeSet<&String>>,
    ) -> BTreeMap<ColumnName, BTreeSet<&String>> {
        // Find the measurement name on the partition and dispatch query to the
        // table for that measurement if the partition's time range overlaps the
        // requested time range.
        //
        // This method also has the ability to short-circuit execution against
        // columns if those columns only contain values that have already been
        // found.
        todo!();
    }
}

// Partition metadata that is used to track statistics about the partition and
// whether it may contains data for a query.
struct MetaData {
    size: u64, // size in bytes of the partition
    rows: u64, // Total number of rows across all tables

    // The total time range of *all* data (across all tables) within this
    // partition.
    //
    // This would only be None if the partition contained only tables that had
    // no time-stamp column or the values were all NULL.
    time_range: Option<(i64, i64)>,
}

impl MetaData {
    pub fn new(table: &Table<'_>) -> Self {
        Self {
            size: table.size(),
            rows: table.rows(),
            time_range: table.time_range(),
        }
    }

    pub fn add_table(&mut self, table: &Table<'_>) {
        // Update size, rows, time_range
        todo!()
    }

    // invalidate should be called when a table is removed that impacts the
    // meta data.
    pub fn invalidate(&mut self) {
        // Update size, rows, time_range by linearly scanning all tables.
        todo!()
    }
}
