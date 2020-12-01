#![deny(rust_2018_idioms)]
#![allow(dead_code)]
#![allow(clippy::too_many_arguments)]
#![allow(unused_variables)]
pub mod column;
pub(crate) mod partition;
pub mod segment;
pub(crate) mod table;

use std::collections::BTreeMap;

use arrow_deps::arrow::record_batch::RecordBatch;

use column::AggregateType;
use partition::Partition;
use segment::ColumnName;

/// The Segment Store is responsible for providing read access to partition data.
///
///
#[derive(Default)]
pub struct Store {
    // A mapping from database name (tenant id, bucket id etc) to a database.
    databases: BTreeMap<String, Database>,

    // The current total size of the store, in bytes
    size: u64,
}

impl Store {
    // TODO(edd): accept a configuration of some sort.
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a new database to the store
    pub fn add_database(&mut self, id: String, database: Database) {
        self.size += database.size();
        self.databases.insert(id, database);
    }

    /// Remove an entire database from the store.
    pub fn remove_database(&mut self, id: String) {
        todo!()
    }

    /// This method adds a partition to the segment store. It is probably what
    /// the `WriteBuffer` will call.
    ///
    /// The partition should comprise a single table (record batch) for each
    /// measurement name in the partition.
    pub fn add_partition(&mut self, database_id: String, partition: BTreeMap<String, RecordBatch>) {
        todo!()
    }

    /// Executes selections against matching partitions, returning a single
    /// record batch with all partition results appended.
    ///
    /// Results may be filtered by (currently only) equality predicates, but can
    /// be ranged by time, which should be represented as nanoseconds since the
    /// epoch. Results are included if they satisfy the predicate and fall
    /// with the [min, max) time range domain.
    pub fn select(
        &self,
        database_name: &str,
        table_name: &str,
        time_range: (i64, i64),
        predicates: &[(&str, &str)],
        select_columns: Vec<String>,
    ) -> Option<RecordBatch> {
        // Execute against matching database.
        //
        // TODO(edd): error handling on everything...................
        //
        if let Some(db) = self.databases.get(database_name) {
            return db.select(table_name, time_range, predicates, select_columns);
        }
        None
    }

    /// Returns aggregates segmented by grouping keys for the specified
    /// measurement as record batches, with one record batch per matching
    /// partition.
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
        database_name: &str,
        table_name: &str,
        time_range: (i64, i64),
        predicates: &[(&str, &str)],
        group_columns: Vec<String>,
        aggregates: Vec<(ColumnName<'_>, AggregateType)>,
    ) -> Option<RecordBatch> {
        if let Some(db) = self.databases.get(database_name) {
            return db.aggregate(
                table_name,
                time_range,
                predicates,
                group_columns,
                aggregates,
            );
        }
        None
    }

    /// Returns aggregates segmented by grouping keys and windowed by time.
    ///
    /// The set of data to be aggregated may be filtered by (currently only)
    /// equality predicates, but can be ranged by time, which should be
    /// represented as nanoseconds since the epoch. Results are included if they
    /// satisfy the predicate and fall with the [min, max) time range domain.
    ///
    /// Group keys are determined according to the provided group column names
    /// (`group_columns`). Currently only grouping by string (tag key) columns
    /// is supported.
    ///
    /// Required aggregates are specified via a tuple comprising a column name
    /// and the type of aggregation required. Multiple aggregations can be
    /// applied to the same column.
    ///
    /// Results are grouped and windowed according to the `window` parameter,
    /// which represents an interval in nanoseconds. For example, to window
    /// results by one minute, window should be set to 600_000_000_000.
    pub fn aggregate_window(
        &self,
        database_name: &str,
        table_name: &str,
        time_range: (i64, i64),
        predicates: &[(&str, &str)],
        group_columns: Vec<String>,
        aggregates: Vec<(ColumnName<'_>, AggregateType)>,
        window: i64,
    ) -> Option<RecordBatch> {
        if let Some(db) = self.databases.get(database_name) {
            return db.aggregate_window(
                table_name,
                time_range,
                predicates,
                group_columns,
                aggregates,
                window,
            );
        }
        None
    }

    //
    // ---- Schema API queries
    //

    /// Returns the distinct set of table names that contain data that satisfies
    /// the time range and predicates.
    pub fn table_names(
        &self,
        database_name: &str,
        time_range: (i64, i64),
        predicates: &[(&str, &str)],
    ) -> Option<RecordBatch> {
        if let Some(db) = self.databases.get(database_name) {
            return db.table_names(database_name, time_range, predicates);
        }
        None
    }

    /// Returns the distinct set of tag keys (column names) matching the provided
    /// optional predicates and time range.
    pub fn tag_keys(
        &self,
        database_name: &str,
        table_name: &str,
        time_range: (i64, i64),
        predicates: &[(&str, &str)],
    ) -> Option<RecordBatch> {
        if let Some(db) = self.databases.get(database_name) {
            return db.tag_keys(table_name, time_range, predicates);
        }
        None
    }

    /// Returns the distinct set of tag values (column values) for each provided
    /// tag key, where each returned value lives in a row matching the provided
    /// optional predicates and time range.
    ///
    /// As a special case, if `tag_keys` is empty then all distinct values for
    /// all columns (tag keys) are returned for the partition.
    pub fn tag_values(
        &self,
        database_name: &str,
        table_name: &str,
        time_range: (i64, i64),
        predicates: &[(&str, &str)],
        tag_keys: &[String],
    ) -> Option<RecordBatch> {
        if let Some(db) = self.databases.get(database_name) {
            return db.tag_values(table_name, time_range, predicates, tag_keys);
        }
        None
    }
}

/// Generate a predicate for the time range [from, to).
pub fn time_range_predicate<'a>(from: i64, to: i64) -> Vec<segment::Predicate<'a>> {
    vec![
        (
            segment::TIME_COLUMN_NAME,
            (
                column::cmp::Operator::GTE,
                column::Value::Scalar(column::Scalar::I64(from)),
            ),
        ),
        (
            segment::TIME_COLUMN_NAME,
            (
                column::cmp::Operator::LT,
                column::Value::Scalar(column::Scalar::I64(to)),
            ),
        ),
    ]
}

// A database is scoped to a single tenant. Within a database there exists
// tables for measurements. There is a 1:1 mapping between a table and a
// measurement name.
#[derive(Default)]
pub struct Database {
    // The collection of partitions in the database. Each partition is uniquely
    // identified by a partition key.
    partitions: BTreeMap<String, Partition>,

    // The current total size of the database.
    size: u64,
}

impl Database {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_partition(&mut self, partition: Partition) {
        todo!()
    }

    pub fn remove_partition(&mut self, partition: Partition) {
        todo!()
    }

    pub fn size(&self) -> u64 {
        self.size
    }

    /// Executes selections against matching partitions, returning a single
    /// record batch with all partition results appended.
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
        select_columns: Vec<String>,
    ) -> Option<RecordBatch> {
        // Find all matching partitions using:
        //   - time range
        //   - measurement name.
        //
        // Execute against each partition and append each result set into a
        // single record batch.
        todo!();
    }

    /// Returns aggregates segmented by grouping keys for the specified
    /// measurement as record batches, with one record batch per matching
    /// partition.
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
        group_columns: Vec<String>,
        aggregates: Vec<(ColumnName<'_>, AggregateType)>,
    ) -> Option<RecordBatch> {
        // Find all matching partitions using:
        //   - time range
        //   - measurement name.
        //
        // Execute query against each matching partition and get result set.
        // For each result set it may be possible for there to be duplicate
        // group keys, e.g., due to back-filling. So partition results may need
        // to be merged together with the aggregates from identical group keys
        // being resolved.
        //
        // Finally a record batch is returned.
        todo!()
    }

    /// Returns aggregates segmented by grouping keys and windowed by time.
    ///
    /// The set of data to be aggregated may be filtered by (currently only)
    /// equality predicates, but can be ranged by time, which should be
    /// represented as nanoseconds since the epoch. Results are included if they
    /// satisfy the predicate and fall with the [min, max) time range domain.
    ///
    /// Group keys are determined according to the provided group column names
    /// (`group_columns`). Currently only grouping by string (tag key) columns
    /// is supported.
    ///
    /// Required aggregates are specified via a tuple comprising a column name
    /// and the type of aggregation required. Multiple aggregations can be
    /// applied to the same column.
    ///
    /// Results are grouped and windowed according to the `window` parameter,
    /// which represents an interval in nanoseconds. For example, to window
    /// results by one minute, window should be set to 600_000_000_000.
    pub fn aggregate_window(
        &self,
        table_name: &str,
        time_range: (i64, i64),
        predicates: &[(&str, &str)],
        group_columns: Vec<String>,
        aggregates: Vec<(ColumnName<'_>, AggregateType)>,
        window: i64,
    ) -> Option<RecordBatch> {
        // Find all matching partitions using:
        //   - time range
        //   - measurement name.
        //
        // Execute query against each matching partition and get result set.
        // For each result set it may be possible for there to be duplicate
        // group keys, e.g., due to back-filling. So partition results may need
        // to be merged together with the aggregates from identical group keys
        // being resolved.
        //
        // Finally a record batch is returned.
        todo!()
    }

    //
    // ---- Schema API queries
    //

    /// Returns the distinct set of table names that contain data that satisfies
    /// the time range and predicates.
    pub fn table_names(
        &self,
        database_name: &str,
        time_range: (i64, i64),
        predicates: &[(&str, &str)],
    ) -> Option<RecordBatch> {
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
        table_name: &str,
        time_range: (i64, i64),
        predicates: &[(&str, &str)],
    ) -> Option<RecordBatch> {
        // Find all matching partitions using:
        //   - time range
        //   - measurement name.
        //
        // Execute query against matching partitions. The `tag_keys` method for
        // a partition allows the caller to provide already found tag keys
        // (column names). This allows the execution to skip entire partitions,
        // tables or segments if there are no new columns to be found there...
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
        table_name: &str,
        time_range: (i64, i64),
        predicates: &[(&str, &str)],
        tag_keys: &[String],
    ) -> Option<RecordBatch> {
        // Find the measurement name on the partition and dispatch query to the
        // table for that measurement if the partition's time range overlaps the
        // requested time range.
        todo!();
    }
}
