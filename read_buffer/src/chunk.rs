use std::collections::{BTreeMap, BTreeSet};

use crate::column::AggregateType;
use crate::row_group::{ColumnName, Predicate};
use crate::table::{ReadFilterResults, ReadGroupResults, Table};

type TableName = String;

/// A `Chunk` comprises a collection of `Tables` where every table must have a
/// unique identifier (name).
pub struct Chunk {
    // The unique identifier for this chunk.
    id: u32,

    // Metadata about the tables within this chunk.
    meta: MetaData,

    // The set of tables within this chunk. Each table is identified by a
    // measurement name.
    tables: BTreeMap<TableName, Table>,
}

impl Chunk {
    pub fn new(id: u32, table: Table) -> Self {
        let mut p = Self {
            id,
            meta: MetaData::new(&table),
            tables: BTreeMap::new(),
        };
        p.tables.insert(table.name().to_owned(), table);
        p
    }

    /// Returns data for the specified column selections on the specified table
    /// name.
    ///
    /// Results may be filtered by conjunctive predicates. Time predicates
    /// should use as nanoseconds since the epoch.
    pub fn select(
        &self,
        table_name: &str,
        predicates: &[Predicate<'_>],
        select_columns: &[ColumnName<'_>],
    ) -> ReadFilterResults<'_, '_> {
        // Lookup table by name and dispatch execution.
        todo!();
    }

    /// Returns aggregates segmented by grouping keys for the specified
    /// table name.
    ///
    /// The set of data to be aggregated may be filtered by optional conjunctive
    /// predicates.
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
        predicates: &[Predicate<'_>],
        group_columns: Vec<ColumnName<'_>>,
        aggregates: Vec<(ColumnName<'_>, AggregateType)>,
    ) -> ReadGroupResults<'_, '_> {
        // Lookup table by name and dispatch execution.
        todo!()
    }

    //
    // ---- Schema API queries
    //

    /// Returns the distinct set of table names that contain data that satisfies
    /// the time range and predicates.
    pub fn table_names(&self, predicates: &[Predicate<'_>]) -> BTreeSet<String> {
        //
        // TODO(edd): do we want to add the ability to apply a predicate to the
        // table names? For example, a regex where you only want table names
        // beginning with /cpu.+/ or something?
        todo!()
    }

    /// Returns the distinct set of tag keys (column names) matching the
    /// provided optional predicates and time range.
    pub fn tag_keys(
        &self,
        table_name: String,
        predicates: &[Predicate<'_>],
        found_keys: &BTreeSet<ColumnName<'_>>,
    ) -> BTreeSet<ColumnName<'_>> {
        // Lookup table by name and dispatch execution if the table's time range
        // overlaps the requested time range *and* there exists columns in the
        // table's schema that are *not* already found.
        todo!();
    }

    /// Returns the distinct set of tag values (column values) for each provided
    /// tag key, where each returned value lives in a row matching the provided
    /// optional predicates and time range.
    ///
    /// As a special case, if `tag_keys` is empty then all distinct values for
    /// all columns (tag keys) are returned for the chunk.
    pub fn tag_values(
        &self,
        table_name: String,
        predicates: &[Predicate<'_>],
        tag_keys: &[ColumnName<'_>],
        found_tag_values: &BTreeMap<ColumnName<'_>, BTreeSet<&String>>,
    ) -> BTreeMap<ColumnName<'_>, BTreeSet<&String>> {
        // Lookup table by name and dispatch execution to the table for that
        // if the chunk's time range overlaps the requested time range.
        //
        // This method also has the ability to short-circuit execution against
        // columns if those columns only contain values that have already been
        // found.
        todo!();
    }
}

// `Chunk` metadata that is used to track statistics about the chunk and
// whether it could contain data necessary to execute a query.
struct MetaData {
    size: u64, // size in bytes of the chunk
    rows: u64, // Total number of rows across all tables

    // The total time range of *all* data (across all tables) within this
    // chunk.
    //
    // This would only be None if the chunk contained only tables that had
    // no time-stamp column or the values were all NULL.
    time_range: Option<(i64, i64)>,
}

impl MetaData {
    pub fn new(table: &Table) -> Self {
        Self {
            size: table.size(),
            rows: table.rows(),
            time_range: table.time_range(),
        }
    }

    pub fn add_table(&mut self, table: &Table) {
        // Update size, rows, time_range
        todo!()
    }

    // invalidate should be called when a table is removed. All meta data must
    // be determined by asking each table in the chunk for its meta data.
    pub fn invalidate(&mut self) {
        // Update size, rows, time_range by linearly scanning all tables.
        todo!()
    }
}
