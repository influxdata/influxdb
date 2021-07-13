use crate::{
    column,
    row_group::{self, ColumnName, Predicate, RowGroup},
    schema::{AggregateType, ColumnType, LogicalDataType, ResultSchema},
    value::{OwnedValue, Scalar, Value},
};
use arrow::record_batch::RecordBatch;
use data_types::{chunk_metadata::ChunkColumnSummary, partition_metadata::TableSummary};
use internal_types::selection::Selection;
use parking_lot::RwLock;
use snafu::{ensure, Snafu};
use std::{
    collections::{BTreeMap, BTreeSet},
    convert::TryInto,
    fmt::Display,
    sync::Arc,
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("cannot drop last row group in table; drop table"))]
    EmptyTableError {},

    #[snafu(display("table does not have InfluxDB timestamp column"))]
    NoTimestampColumnError {},

    #[snafu(display("unsupported column operation on {}: {}", column_name, msg))]
    UnsupportedColumnOperation { msg: String, column_name: String },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// A Table represents data for a single measurement.
///
/// Tables contain potentially many collections of rows in the form of row
/// groups. These row groups can be thought of as horizontally sliced sections
/// of the entire table, where each row within any row group is unique and not
/// found on any other row groups for the table.
///
/// Rows within a table's row groups can be sorted arbitrarily, therefore it is
/// possible that time-ranges (for example) can overlap across row groups.
///
/// The current write path ensures that a single row group emitted for a
/// table within any chunk will have the same schema, therefore this
/// table's schema applies to all of the row groups held within it.
///
/// The total size of a table is tracked and can be increased or reduced by
/// adding or removing row groups for that table.
pub struct Table {
    name: String,

    // A table's data is held in a collection of immutable row groups and
    // mutable meta data (`RowGroupData`).
    //
    // Concurrent access to the `RowGroupData` is managed via an `RwLock`, which is
    // taken in the following circumstances:
    //
    //    * A lock is needed when adding a new row group. It is held as long as it takes to push
    //      the new row group on a `Vec` and update the table meta-data. This is not long.
    //
    //    * A lock is needed when removing row groups. It is held as long as it takes to remove
    //      something from a `Vec`, and re-construct new meta-data. This is not long.
    //
    //    * A read lock is needed for all read operations over table data (row groups). However,
    //      the read lock is only held for as long as it takes to shallow-clone the table data (via
    //      Arcs) that are required for the read. The expensive process of performing the read
    //      operation is done in a lock-free manner.
    table_data: RwLock<RowGroupData>,
}

// Tie data and meta-data together so that they can be wrapped in RWLock.
#[derive(Default)]
struct RowGroupData {
    meta: Arc<MetaData>,
    data: Vec<Arc<RowGroup>>,
}

impl Table {
    /// Create a new empty table.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            table_data: Default::default(),
        }
    }

    /// Create a new table with the provided row_group.
    pub fn with_row_group(name: impl Into<String>, rg: RowGroup) -> Self {
        Self {
            name: name.into(),
            table_data: RwLock::new(RowGroupData {
                meta: Arc::new(MetaData::new(&rg)),
                data: vec![Arc::new(rg)],
            }),
        }
    }

    /// Add a new row group to this table.
    pub fn add_row_group(&mut self, rg: RowGroup) {
        let mut row_groups = self.table_data.write();

        // `meta` can't be modified whilst protected by an Arc so create a new one.
        row_groups.meta = Arc::new(MetaData::update_with(
            MetaData::clone(&row_groups.meta), // clone meta-data not Arc
            &rg,
        ));

        // Add the new row group data to the table.
        row_groups.data.push(Arc::new(rg));
    }

    /// TODO(edd): wire up
    ///
    /// Remove the row group at `position` from table, returning an error if the
    /// caller has attempted to drop the last row group.
    ///
    /// To drop the last row group from the table, the caller should instead
    /// drop the table.
    pub fn drop_row_group(&mut self, position: usize) -> Result<()> {
        let mut row_groups = self.table_data.write();

        // Tables must always have at least one row group.
        ensure!(row_groups.data.len() > 1, EmptyTableError);

        row_groups.data.remove(position); // removes row group data
        row_groups.meta = Arc::new(MetaData::from(&row_groups.data)); // rebuild meta

        Ok(())
    }

    /// The name of the table (equivalent to measurement or table name).
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Determines if this table contains no row groups.
    pub fn is_empty(&self) -> bool {
        self.table_data.read().data.is_empty()
    }

    /// The total number of row groups within this table.
    pub fn len(&self) -> usize {
        self.table_data.read().data.len()
    }

    /// An estimation of the total size of the table in bytes in memory
    pub fn size(&self) -> usize {
        let base_size = std::mem::size_of::<Self>() + self.name.len();
        // meta.size accounts for all the row group data.
        base_size + self.table_data.read().meta.size()
    }

    /// The estimated size for each column in this table.
    pub fn column_sizes(&self) -> Vec<ChunkColumnSummary> {
        self.table_data
            .read()
            .data
            .iter()
            .flat_map(|rg| rg.column_sizes())
            // combine statistics for columns across row groups
            .fold(BTreeMap::new(), |mut map, (name, memory_bytes)| {
                let entry = map.entry(name).or_insert(0);
                *entry += memory_bytes;
                map
            })
            // Now turn into Vec<ChunkColumnSummary>
            .into_iter()
            .map(|(name, memory_bytes)| ChunkColumnSummary {
                name: name.into(),
                memory_bytes,
            })
            .collect()
    }

    /// An estimation of the total size of the table in bytes if all values were
    /// stored contiguously and uncompressed. This size is useful to determine
    /// a rough compression that the table is under.
    pub fn size_raw(&self, include_nulls: bool) -> usize {
        self.table_data
            .read()
            .data
            .iter()
            .map(|rg| rg.size_raw(include_nulls))
            .sum::<usize>()
    }

    // Returns the total number of row groups in this table.
    pub fn row_groups(&self) -> usize {
        self.table_data.read().data.len()
    }

    /// The number of rows in this table.
    pub fn rows(&self) -> u64 {
        self.table_data.read().meta.rows
    }

    /// Return a summary of all columns in this table
    pub fn table_summary(&self) -> TableSummary {
        self.table_data.read().meta.to_summary(&self.name)
    }

    /// Returns the column range associated with an InfluxDB Timestamp column
    /// or None if the table's schema does not have such a column.
    pub fn time_range(&self) -> Option<(i64, i64)> {
        let table_data = self.table_data.read();

        let time_column = table_data
            .meta
            .columns
            .values()
            .filter(|cm| matches!(cm.typ, crate::schema::ColumnType::Timestamp(_)))
            .collect::<Vec<_>>();

        if time_column.is_empty() {
            return None;
        }

        assert_eq!(time_column.len(), 1); // can only be one timestamp column.
        let range = &time_column[0].range;

        let (min, max) = match (&range.0, &range.1) {
            (OwnedValue::Scalar(Scalar::I64(min)), OwnedValue::Scalar(Scalar::I64(max))) => {
                (min, max)
            }
            (min, max) => {
                panic!(
                    "invalid range type for timestamp column: ({:?}, {:?})",
                    min, max
                );
            }
        };

        Some((*min, *max))
    }

    // Helper function used in tests.
    // Returns an immutable reference to the table's current meta data.
    pub fn meta(&self) -> Arc<MetaData> {
        Arc::clone(&self.table_data.read().meta)
    }

    /// Determines if one of more row groups in the `Table` could possibly
    /// contain one or more rows that satisfy the provided predicate.
    pub fn could_pass_predicate(&self, predicate: &Predicate) -> bool {
        let table_data = self.table_data.read();

        table_data.data.iter().any(|row_group| {
            row_group.could_satisfy_conjunctive_binary_expressions(predicate.iter())
        })
    }

    // Identify set of row groups that might satisfy the predicate.
    //
    // Produce a set of these row groups along with a snapshot of the table meta
    // data associated with them.
    //
    // N.B the table read lock is only held as long as it takes to determine
    // with meta data whether each row group may satisfy the predicate.
    fn filter_row_groups(&self, predicate: &Predicate) -> (Arc<MetaData>, Vec<Arc<RowGroup>>) {
        let table_data = self.table_data.read();
        let mut row_groups = Vec::with_capacity(table_data.data.len());

        'rowgroup: for rg in table_data.data.iter() {
            // check all expressions in predicate
            if !rg.could_satisfy_conjunctive_binary_expressions(predicate.iter()) {
                continue 'rowgroup;
            }

            // row group could potentially satisfy predicate
            row_groups.push(Arc::clone(&rg));
        }

        (Arc::clone(&table_data.meta), row_groups)
    }

    /// Select data for the specified column selections with the provided
    /// predicates applied.
    ///
    /// All selection columns **must** exist within the schema.
    ///
    /// Results may be filtered by (currently only) conjunctive (AND)
    /// predicates, but can be ranged by time, which should be represented
    /// as nanoseconds since the epoch. Results are included if they satisfy
    /// the predicate and fall with the [min, max) time range domain.
    pub fn read_filter<'a>(
        &'a self,
        columns: &Selection<'_>,
        predicate: &Predicate,
    ) -> ReadFilterResults {
        // identify row groups where time range and predicates match could match
        // the predicate. Get a snapshot of those and the meta-data.
        let (meta, row_groups) = self.filter_row_groups(predicate);

        let schema = ResultSchema {
            select_columns: match columns {
                Selection::All => meta.schema_for_all_columns(),
                Selection::Some(column_names) => meta.schema_for_column_names(column_names),
            },
            ..ResultSchema::default()
        };

        // TODO(edd): I think I can remove `predicates` from the results
        ReadFilterResults {
            predicate: predicate.clone(),
            schema,
            row_groups,
        }
    }

    /// Returns an iterable collection of data in group columns and aggregate
    /// columns, optionally filtered by the provided predicate. Results are
    /// merged across all row groups within the table.
    ///
    /// Collectively, row-wise values in the group columns comprise a "group
    /// key", and each value in the same row for the aggregate columns contains
    /// aggregate values for those group keys.
    ///
    /// Note: `read_aggregate` currently only supports "tag" columns.
    pub fn read_aggregate<'input>(
        &self,
        predicate: Predicate,
        group_columns: &'input Selection<'_>,
        aggregates: &'input [(ColumnName<'input>, AggregateType)],
    ) -> Result<ReadAggregateResults> {
        let (meta, row_groups) = self.filter_row_groups(&predicate);

        // Filter out any column names that we do not have data for.
        let schema = ResultSchema {
            group_columns: match group_columns {
                Selection::All => meta.schema_for_all_columns(),
                Selection::Some(column_names) => meta.schema_for_column_names(column_names),
            },
            aggregate_columns: meta.schema_for_aggregate_column_names(aggregates),
            ..ResultSchema::default()
        };

        // Check all grouping columns are valid for grouping operation.
        for (ct, _) in &schema.group_columns {
            ensure!(
                matches!(ct, ColumnType::Tag(_)),
                UnsupportedColumnOperation {
                    msg: format!("column type must be ColumnType::Tag, got {:?}", ct),
                    column_name: ct.as_str().to_string(),
                },
            )
        }

        // return the iterator to build the results.
        Ok(ReadAggregateResults {
            schema,
            predicate,
            row_groups,
            ..Default::default()
        })
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
    pub fn aggregate_window<'a>(
        &self,
        _time_range: (i64, i64),
        _predicates: &[(&str, &str)],
        _group_columns: Vec<ColumnName<'a>>,
        _aggregates: Vec<(ColumnName<'a>, AggregateType)>,
        _window: i64,
    ) -> BTreeMap<Vec<String>, Vec<(ColumnName<'a>, ReadAggregateResults)>> {
        // identify segments where time range and predicates match could match
        // using segment meta data, and then execute against those segments and
        // merge results.
        todo!()
    }

    //
    // ---- Fast-path first/last selectors.
    //

    // Returns the first value for the specified column across the table
    // where the corresponding value in the time column is >= `time_lower_bound`.
    //
    // The first value is based on the values in time column that best satisfy
    // the provided time lower bound. The first value returned may be NULL. If
    // the time column has multiple values that are all the minimum, then the
    // value returned from this method will be stable but from one of the
    // corresponding minimum-timestamp rows.
    //
    // Note: this returns an option at the moment because there is an assumption
    // that timestamps could be NULL. I think we could add a constraint to make
    // timestamps non-null.
    fn first(&self, _column_name: &str, _time_lower_bound: i64) -> Option<(i64, Value<'_>)> {
        // Find the segment(s) that best satisfy the lower time bound. These will
        // be the segments (or more likely, segment) that has the lowest min
        // time-range.
        //
        // The segment(s) will provide the timestamp value and row_id from its
        // zone map. This row_id can then be used to efficiently lookup the
        // first value for the specified column_name.
        //
        // Tied values (multiple equivalent min timestamps) results in an
        // arbitrary value from the result set being returned.
        todo!();
    }

    /// The inverse of `first`. Of note here is that the returned value must
    /// have a
    fn last(&self, _column_name: &str, _time_upper_bound: i64) -> Option<(i64, Value<'_>)> {
        // Find the segment(s) that best satisfy the upper time bound. These will
        // be the segments (or more likely, segment) that has the highest max
        // time-range.
        //
        // The segment(s) will provide the timestamp value and row_id from its
        // zone map. This row_id can then be used to efficiently lookup the last
        // value for the specified column_name.
        //
        // Tied values (multiple equivalent min timestamps) results in an
        // arbitrary value from the result set being returned.
        todo!();
    }

    //
    // ---- Schema API queries
    //

    /// Returns a distinct set of column names in the table.
    ///
    /// Optionally a predicate may be provided. In such a case only column names
    /// will be returned belonging to columns whom have at least one non-null
    /// value for any row satisfying the predicate. Finally, the caller can
    /// specify a set of column names to limit execution to only those.
    pub fn column_names(
        &self,
        predicate: &Predicate,
        columns: Selection<'_>,
        mut dst: BTreeSet<String>,
    ) -> BTreeSet<String> {
        let table_data = self.table_data.read();

        // Short circuit execution if we have already got all of this table's
        // columns in the results.
        if table_data
            .meta
            .columns
            .keys()
            .all(|name| dst.contains(name))
        {
            return dst;
        }

        // Identify row groups where time range and predicates match could match
        // the predicate. Get a snapshot of those, and the table meta-data.
        //
        // NOTE(edd): this takes another read lock on `self`. I think this is
        // ok, but if it turns out it's not then we can move the
        // `filter_row_groups` logic into here and not take the second read
        // lock.
        let (_, row_groups) = self.filter_row_groups(predicate);
        for row_group in row_groups {
            row_group.column_names(predicate, columns, &mut dst);
        }

        dst
    }

    /// Returns the distinct set of column values for each provided column,
    /// where each returned value sits in a row matching the provided
    /// predicate. All values are deduplicated across row groups in the table.
    ///
    /// If the predicate is empty then all distinct values are returned.
    pub fn column_values<'a>(
        &'a self,
        predicate: &Predicate,
        columns: &[ColumnName<'_>],
        mut dst: BTreeMap<String, BTreeSet<String>>,
    ) -> Result<BTreeMap<String, BTreeSet<String>>> {
        let (meta, row_groups) = self.filter_row_groups(predicate);

        // Validate that only supported columns present in `columns`.
        for (name, (ct, _)) in columns.iter().zip(meta.schema_for_column_names(columns)) {
            ensure!(
                matches!(ct, ColumnType::Tag(_)),
                UnsupportedColumnOperation {
                    msg: format!("column type must be ColumnType::Tag, got {:?}", ct),
                    column_name: name.to_string(),
                },
            )
        }

        for row_group in row_groups {
            dst = row_group.column_values(predicate, columns, dst)
        }

        Ok(dst)
    }

    /// Determines if this table contains one or more rows that satisfy the
    /// predicate.
    pub fn satisfies_predicate(&self, predicate: &Predicate) -> bool {
        // Get a snapshot of the table data under a read lock.
        let (meta, row_groups) = {
            let table_data = self.table_data.read();
            (Arc::clone(&table_data.meta), table_data.data.to_vec())
        };

        // if the table doesn't have a column for one of the predicate's
        // expressions then the table cannot satisfy the predicate.
        if !predicate
            .iter()
            .all(|expr| meta.columns.contains_key(expr.column()))
        {
            return false;
        }

        // Apply the predicate to all row groups. Each row group will do its own
        // column pruning based on its column ranges.

        // The following could be expensive if row group data needs to be
        // processed but this operation is now lock-free.
        row_groups
            .iter()
            .any(|row_group| row_group.satisfies_predicate(predicate))
    }

    pub(crate) fn column_storage_statistics(&self) -> Vec<column::Statistics> {
        let table_data = self.table_data.read();
        table_data
            .data
            .iter()
            .map(|rg| rg.column_storage_statistics())
            .flatten()
            .collect()
    }
}

// TODO(edd): reduce owned strings here by, e.g., using references as keys.
#[derive(Clone, Default)]
pub struct MetaData {
    // The total size of all row-group data associated with the table in bytes.
    rgs_size: usize,

    // The total number of rows in the table.
    rows: u64,

    // The distinct set of columns for this table (all of these columns will
    // appear in all of the table's row groups) and meta data about those
    // columns including their schema and range.
    columns: BTreeMap<String, row_group::ColumnMeta>,

    // The names of the columns for this table in the order they appear.
    column_names: Vec<String>,
}

impl MetaData {
    pub fn new(rg: &row_group::RowGroup) -> Self {
        Self {
            rgs_size: rg.size(),
            rows: rg.rows() as u64,
            columns: rg.metadata().columns.clone(),
            column_names: rg.metadata().column_names.clone(),
        }
    }

    /// Returns the estimated size in bytes of the `MetaData` struct and all of
    /// the row group data associated with a `Table`.
    fn size(&self) -> usize {
        let base_size = std::mem::size_of::<Self>();
        let columns_meta_size = self
            .columns
            .iter()
            .map(|(k, v)| k.len() + v.size())
            .sum::<usize>();

        let column_names_size = self.column_names.iter().map(|c| c.len()).sum::<usize>();
        (base_size + columns_meta_size + column_names_size) + self.rgs_size
    }

    /// Create a new `MetaData` by consuming `this` and incorporating `other`.
    pub fn update_with(mut this: Self, rg: &row_group::RowGroup) -> Self {
        let other_meta = rg.metadata();

        // first row group added to the table.
        if this.columns.is_empty() {
            this.rgs_size = rg.size();
            this.rows = rg.rows() as u64;
            this.columns = other_meta.columns.clone();
            this.column_names = other_meta.column_names.clone();

            return this;
        }

        // Subsequent row groups...

        // The incoming row group must have exactly the same schema as any
        // existing row groups in the table.
        assert_eq!(&this.columns, &other_meta.columns);

        // update size, rows, column ranges, time range
        this.rgs_size += rg.size();
        this.rows += rg.rows() as u64;

        // Update the table schema using the incoming row group schema
        for (column_name, column_meta) in &other_meta.columns {
            let (column_range_min, column_range_max) = &column_meta.range;
            let mut curr_meta = this.columns.get_mut(&column_name.to_string()).unwrap();

            // No way to accurately aggregate counts across RowGroups
            curr_meta.distinct_count = None;

            if column_range_min < &curr_meta.range.0 {
                curr_meta.range.0 = column_range_min.clone();
            }

            if column_range_max > &curr_meta.range.1 {
                curr_meta.range.1 = column_range_max.clone();
            }
        }

        this
    }

    /// Extract schema information for a set of columns. If a column name does
    /// not exist within the `Table` schema it is ignored and not present within
    /// the resulting schema information.
    pub fn schema_for_column_names(
        &self,
        names: &[ColumnName<'_>],
    ) -> Vec<(ColumnType, LogicalDataType)> {
        names
            .iter()
            .filter_map(|&name| {
                self.columns
                    .get(name)
                    .map(|schema| (schema.typ.clone(), schema.logical_data_type))
            })
            .collect::<Vec<_>>()
    }

    /// As `schema_for_column_names` but for all columns in the table. Schema
    /// information is returned in the same order as columns in the table.
    pub fn schema_for_all_columns(&self) -> Vec<(ColumnType, LogicalDataType)> {
        let mut column_schema = vec![];
        for column_name in &self.column_names {
            let schema = self.columns.get(column_name).unwrap();
            column_schema.push((schema.typ.clone(), schema.logical_data_type));
        }
        column_schema
    }

    // As `schema_for_column_names` but also embeds the provided aggregate type.
    fn schema_for_aggregate_column_names(
        &self,
        names: &[(ColumnName<'_>, AggregateType)],
    ) -> Vec<(ColumnType, AggregateType, LogicalDataType)> {
        names
            .iter()
            .filter_map(|(name, agg_type)| {
                self.columns.get(*name).map(|schema| {
                    // TODO(edd): this check happens because an aggregate does
                    // not have to have the same physical type as the logical
                    // type of the column it is aggregating on. An example of
                    // this is Count. I'm going to fix this by associated data
                    // types with the aggregate itself.
                    let physical_data_type = if let AggregateType::Count = agg_type {
                        LogicalDataType::Unsigned
                    } else {
                        schema.logical_data_type
                    };

                    (schema.typ.clone(), *agg_type, physical_data_type)
                })
            })
            .collect::<Vec<_>>()
    }

    pub fn all_column_names(&self) -> Vec<&str> {
        self.column_names.iter().map(|name| name.as_str()).collect()
    }

    pub fn to_summary(&self, table_name: impl Into<String>) -> TableSummary {
        use data_types::partition_metadata::{ColumnSummary, StatValues, Statistics};
        let columns = self
            .columns
            .iter()
            .map(|(name, column_meta)| {
                let count = self.rows;
                let distinct_count = column_meta.distinct_count;

                let stats = match &column_meta.range {
                    (OwnedValue::String(min), OwnedValue::String(max)) => {
                        Statistics::String(StatValues {
                            min: Some(min.to_string()),
                            max: Some(max.to_string()),
                            distinct_count,
                            count,
                        })
                    }
                    (OwnedValue::Boolean(min), OwnedValue::Boolean(max)) => {
                        Statistics::Bool(StatValues {
                            min: Some(*min),
                            max: Some(*max),
                            distinct_count,
                            count,
                        })
                    }
                    (OwnedValue::Scalar(min), OwnedValue::Scalar(max)) => match (min, max) {
                        (Scalar::I64(min), Scalar::I64(max)) => Statistics::I64(StatValues {
                            min: Some(*min),
                            max: Some(*max),
                            distinct_count,
                            count,
                        }),
                        (Scalar::U64(min), Scalar::U64(max)) => Statistics::U64(StatValues {
                            min: Some(*min),
                            max: Some(*max),
                            distinct_count,
                            count,
                        }),
                        (Scalar::F64(min), Scalar::F64(max)) => Statistics::F64(StatValues {
                            min: Some(*min),
                            max: Some(*max),
                            distinct_count,
                            count,
                        }),
                        _ => panic!(
                            "unsupported type scalar stats in read buffer: {:?}, {:?}",
                            min, max
                        ),
                    },
                    _ => panic!(
                        "unsupported type of stats in read buffer: {:?}",
                        column_meta.range
                    ),
                };

                ColumnSummary {
                    name: name.to_string(),
                    stats,
                    influxdb_type: column_meta.typ.as_influxdb_type(),
                }
            })
            .collect();

        TableSummary {
            name: table_name.into(),
            columns,
        }
    }

    pub fn has_column(&self, name: &str) -> bool {
        self.columns.contains_key(name)
    }
}

// Builds new table meta-data from a collection of row groups. Useful for
// rebuilding state when a row group has been removed from the table.
impl From<&Vec<Arc<RowGroup>>> for MetaData {
    fn from(row_groups: &Vec<Arc<RowGroup>>) -> Self {
        if row_groups.is_empty() {
            panic!("row groups required for meta data construction");
        }

        let mut meta = Self::new(&row_groups[0]);
        for row_group in row_groups.iter().skip(1) {
            meta = Self::update_with(meta, &row_group);
        }

        meta
    }
}

/// Results of a `read_filter` execution on the table. Execution is lazy -
/// row groups are only queried when `ReadFilterResults` is iterated.
pub struct ReadFilterResults {
    // schema of all columns in the query results
    schema: ResultSchema,

    // These row groups passed the predicates and need to be queried.
    row_groups: Vec<Arc<RowGroup>>,

    // TODO(edd): encapsulate this into a single executor function that just
    // executes on the next row group.
    predicate: Predicate,
}

impl ReadFilterResults {
    pub fn is_empty(&self) -> bool {
        self.row_groups.is_empty()
    }

    pub fn len(&self) -> usize {
        self.row_groups.len()
    }

    /// Returns the schema associated with table result and therefore all of the
    /// results for all of row groups in the table results.
    pub fn schema(&self) -> &ResultSchema {
        &self.schema
    }

    // useful for testing - materialise all results but don't convert them to
    // record batches. Skips any row groups that don't have any results
    fn row_group_results(&self) -> Vec<row_group::ReadFilterResult<'_>> {
        let select_columns = &self
            .schema()
            .select_column_names_iter()
            .map(|name| name.as_str())
            .collect::<Vec<_>>();

        self.row_groups
            .iter()
            .map(|row_group| row_group.read_filter(select_columns, &self.predicate))
            .filter(|result| !result.is_empty())
            .collect()
    }
}

impl Iterator for ReadFilterResults {
    type Item = RecordBatch;

    fn next(&mut self) -> Option<Self::Item> {
        if self.is_empty() {
            return None;
        }

        let row_group = self.row_groups.remove(0);
        let result = row_group.read_filter(
            &self
                .schema()
                .select_column_names_iter()
                .map(|name| name.as_str())
                .collect::<Vec<_>>(),
            &self.predicate,
        );

        if result.is_empty() {
            return self.next(); // try next row group
        }

        assert_eq!(result.schema(), self.schema()); // validate schema
        Some(result.try_into().unwrap())
    }
}

// Helper type that can pretty print a set of results for `read_filter`.
struct DisplayReadFilterResults<'a>(Vec<row_group::ReadFilterResult<'a>>);

impl<'a> Display for DisplayReadFilterResults<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.0.is_empty() {
            return Ok(());
        }

        // write out the schema of the first result as the table header
        std::fmt::Display::fmt(&self.0[0].schema(), f)?;
        writeln!(f)?;

        // write out each row group result
        for row_group in self.0.iter() {
            std::fmt::Display::fmt(&row_group, f)?;
        }

        Ok(())
    }
}

#[derive(Default)]
pub struct ReadAggregateResults {
    // schema information for the results
    schema: ResultSchema,

    // the predicate to apply to each row group.
    predicate: Predicate,

    // row groups that will be executed against. The columns to group on and the
    // aggregates to produce are determined by the `schema`.
    row_groups: Vec<Arc<RowGroup>>,

    drained: bool, // currently this iterator only yields once.
}

impl ReadAggregateResults {
    /// Returns the schema associated with table result and therefore all of
    /// results from row groups.
    pub fn schema(&self) -> &ResultSchema {
        &self.schema
    }

    // Logic to get next result merged across all row groups for the table is
    // pulled out so we can decouple this from materialising record batches,
    // which means we're not forced to use record batches in tests.
    fn next_merged_result(&mut self) -> Option<row_group::ReadAggregateResult<'_>> {
        if self.row_groups.is_empty() || self.drained {
            return None;
        }

        let mut merged_results = self.row_groups.get(0).unwrap().read_aggregate(
            &self.predicate,
            &self
                .schema
                .group_column_names_iter()
                .map(|s| s.as_str())
                .collect::<Vec<_>>(),
            &self
                .schema
                .aggregate_columns
                .iter()
                .map(|(name, agg_type, _)| (name.as_str(), *agg_type))
                .collect::<Vec<_>>(),
        );
        assert_eq!(merged_results.schema(), self.schema()); // validate schema

        // Execute against remaining row groups, merging each into the merged
        // set.
        for row_group in self.row_groups.iter().skip(1) {
            let result = row_group.read_aggregate(
                &self.predicate,
                &self
                    .schema
                    .group_column_names_iter()
                    .map(|s| s.as_str())
                    .collect::<Vec<_>>(),
                &self
                    .schema
                    .aggregate_columns
                    .iter()
                    .map(|(name, agg_type, _)| (name.as_str(), *agg_type))
                    .collect::<Vec<_>>(),
            );

            if result.is_empty() {
                continue;
            }
            assert_eq!(result.schema(), self.schema()); // validate schema

            // merge result into on-going results.
            merged_results = merged_results.merge(result);
        }

        self.drained = true;
        Some(merged_results)
    }
}

/// Implements an iterator on the Table's results for `read_aggregate`. This
/// iterator will execute against one or more row groups, merging each row group
/// result into the last before returning a final set of results.
///
/// Merging in this context means unioning all group keys in multiple sets of
/// results, and aggregating together aggregates for duplicate group keys.
///
/// Given that, it's expected that this iterator will only iterate once, but
/// perhaps in the future we will break the work up and send intermediate
/// results back.
impl Iterator for ReadAggregateResults {
    type Item = RecordBatch;

    fn next(&mut self) -> Option<Self::Item> {
        self.next_merged_result()
            .map(|merged_result| merged_result.try_into().unwrap())
    }
}

// Helper type that can pretty print a set of results for `read_aggregate`.
struct DisplayReadAggregateResults<'a>(Vec<row_group::ReadAggregateResult<'a>>);

impl std::fmt::Display for DisplayReadAggregateResults<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.0.is_empty() {
            return Ok(());
        }

        // write out the schema of the first result as the table header
        std::fmt::Display::fmt(&self.0[0].schema(), f)?;

        // write out each row group result
        for row_group in self.0.iter() {
            std::fmt::Display::fmt(&row_group, f)?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use crate::column::Column;
    use crate::row_group::{BinaryExpr, ColumnType, ReadAggregateResult};
    use crate::schema;
    use crate::schema::LogicalDataType;
    use crate::value::{AggregateVec, OwnedValue, Scalar};

    #[test]
    fn meta_data_update_with() {
        let columns = vec![
            (
                "time".to_string(),
                ColumnType::create_time(&[100, 200, 300]),
            ),
            (
                "region".to_string(),
                ColumnType::create_tag(&["west", "west", "north"]),
            ),
        ];
        let rg = RowGroup::new(3, columns);

        let mut meta = MetaData::new(&rg);
        assert_eq!(meta.rows, 3);
        let meta_size = meta.rgs_size;
        assert!(meta_size > 0);
        assert_eq!(
            meta.columns.get("region").unwrap().range,
            (
                OwnedValue::String("north".to_owned()),
                OwnedValue::String("west".to_owned())
            )
        );

        let columns = vec![
            ("time".to_string(), ColumnType::create_time(&[10, 400])),
            (
                "region".to_string(),
                ColumnType::create_tag(&["east", "south"]),
            ),
        ];
        let rg = RowGroup::new(2, columns);

        meta = MetaData::update_with(meta, &rg);
        assert_eq!(meta.rows, 5);
        assert!(meta.rgs_size > meta_size);
        assert_eq!(
            meta.columns.get("region").unwrap().range,
            (
                OwnedValue::String("east".to_owned()),
                OwnedValue::String("west".to_owned())
            )
        );
    }

    #[test]
    fn meta_data_new_or_update_with_same_size() {
        let columns = vec![
            (
                "time".to_string(),
                ColumnType::create_time(&[100, 200, 300]),
            ),
            (
                "region".to_string(),
                ColumnType::create_tag(&["west", "west", "north"]),
            ),
        ];
        let rg = RowGroup::new(3, columns);

        let meta_new = MetaData::new(&rg);

        let meta_default = MetaData::default();
        let meta_default = MetaData::update_with(meta_default, &rg);

        // The rgs_size field is the part that was failing
        assert_eq!(meta_new.rgs_size, meta_default.rgs_size);
        // The value from the size method should match too
        assert_eq!(meta_new.size(), meta_default.size());
    }

    #[test]
    fn add_remove_row_groups() {
        let tc = ColumnType::Time(Column::from(&[0_i64, 2, 3][..]));
        let columns = vec![("time".to_string(), tc)];

        let rg = RowGroup::new(3, columns);
        let mut table = Table::new("cpu".to_owned());
        table.add_row_group(rg);

        assert_eq!(table.rows(), 3);

        // add another row group
        let tc = ColumnType::Time(Column::from(&[1_i64, 2, 3, 4, 5][..]));
        let columns = vec![("time".to_string(), tc)];
        let rg = RowGroup::new(5, columns);
        table.add_row_group(rg);

        assert_eq!(table.rows(), 8);
        assert_eq!(
            table.meta().columns.get("time").unwrap().range,
            (
                OwnedValue::Scalar(Scalar::I64(0)),
                OwnedValue::Scalar(Scalar::I64(5))
            )
        );

        // remove the first row group
        table.drop_row_group(0).unwrap();
        assert_eq!(table.rows(), 5);
        assert_eq!(
            table.meta().columns.get("time").unwrap().range,
            (
                OwnedValue::Scalar(Scalar::I64(1)),
                OwnedValue::Scalar(Scalar::I64(5))
            )
        );

        // attempt to remove the last row group.
        table
            .drop_row_group(0)
            .expect_err("drop_row_group should have returned an error");
    }

    #[test]
    fn column_sizes() {
        let tc = ColumnType::Time(Column::from(&[10_i64, 20, 30][..]));
        let fc = ColumnType::Field(Column::from(&[1000_u64, 1002, 1200][..]));
        let columns = vec![("time".to_string(), tc), ("count".to_string(), fc)];
        let row_group = RowGroup::new(3, columns);
        let mut table = Table::new("cpu");
        table.add_row_group(row_group);

        // add another row group
        let tc = ColumnType::Time(Column::from(&[1_i64, 2, 3, 4, 5, 6][..]));
        let fc = ColumnType::Field(Column::from(&[100_u64, 101, 200, 203, 203, 10][..]));
        let columns = vec![("time".to_string(), tc), ("count".to_string(), fc)];
        let rg = RowGroup::new(6, columns);
        table.add_row_group(rg);

        // expect only a single entry for each column, in name order
        let expected = vec![
            ChunkColumnSummary {
                name: "count".into(),
                memory_bytes: 110,
            },
            ChunkColumnSummary {
                name: "time".into(),
                memory_bytes: 107,
            },
        ];
        assert_eq!(table.column_sizes(), expected);
    }

    #[test]
    fn could_pass_predicate() {
        let tc = ColumnType::Time(Column::from(&[10_i64, 20, 30][..]));
        let rc = ColumnType::Tag(Column::from(&["south", "north", "east"][..]));
        let fc = ColumnType::Field(Column::from(&[1000_u64, 1002, 1200][..]));
        let columns = vec![
            ("time".to_string(), tc),
            ("region".to_string(), rc),
            ("count".to_string(), fc),
        ];
        let row_group = RowGroup::new(3, columns);

        let mut table = Table::new("cpu");
        table.add_row_group(row_group);

        // add another row group
        let tc = ColumnType::Time(Column::from(&[1_i64, 2, 3, 4, 5, 6][..]));
        let rc = ColumnType::Tag(Column::from(
            &["west", "west", "east", "west", "south", "north"][..],
        ));
        let fc = ColumnType::Field(Column::from(&[100_u64, 101, 200, 203, 203, 10][..]));
        let columns = vec![
            ("time".to_string(), tc),
            ("region".to_string(), rc),
            ("count".to_string(), fc),
        ];
        let rg = RowGroup::new(6, columns);
        table.add_row_group(rg);

        // everything could match empty predicate
        let predicate = Predicate::default();
        assert!(table.could_pass_predicate(&predicate));

        // matches first row group
        let predicate = Predicate::new(vec![BinaryExpr::from(("time", ">=", 7_i64))]);
        assert!(table.could_pass_predicate(&predicate));

        // matches first row group different column
        let predicate = Predicate::new(vec![BinaryExpr::from(("region", "=", "east"))]);
        assert!(table.could_pass_predicate(&predicate));

        // matches multiple columns
        let predicate = Predicate::new(vec![
            BinaryExpr::from(("region", "=", "east")),
            BinaryExpr::from(("count", "=", 1200_u64)),
        ]);
        assert!(table.could_pass_predicate(&predicate));

        // Columns matches predicate but on different rows (although no row
        // exists that satisfies the predicate).
        let predicate = Predicate::new(vec![
            BinaryExpr::from(("region", "=", "east")),
            BinaryExpr::from(("count", "=", 1002_u64)),
        ]);
        assert!(table.could_pass_predicate(&predicate));

        // matches second row group
        let predicate = Predicate::new(vec![BinaryExpr::from(("region", ">=", "west"))]);
        assert!(table.could_pass_predicate(&predicate));

        // doesn't match either row group no column
        let predicate = Predicate::new(vec![BinaryExpr::from(("temp", ">=", 0_u64))]);
        assert!(!table.could_pass_predicate(&predicate));

        // doesn't match either row group column exists but no matching value
        let predicate = Predicate::new(vec![BinaryExpr::from(("time", ">=", 10192929_i64))]);
        assert!(!table.could_pass_predicate(&predicate));

        // doesn't match either row group; one column could satisfy predicate but
        // other can't.
        let predicate = Predicate::new(vec![
            BinaryExpr::from(("region", "=", "east")),
            BinaryExpr::from(("count", "<=", 0_u64)),
        ]);
        assert!(!table.could_pass_predicate(&predicate));
    }

    #[test]
    fn select() {
        // Build first row group.
        let tc = ColumnType::Time(Column::from(&[1_i64, 2, 3, 4, 5, 6][..]));
        let rc = ColumnType::Tag(Column::from(
            &["west", "west", "east", "west", "south", "north"][..],
        ));
        let fc = ColumnType::Field(Column::from(&[100_u64, 101, 200, 203, 203, 10][..]));
        let columns = vec![
            ("time".to_string(), tc),
            ("region".to_string(), rc),
            ("count".to_string(), fc),
        ];

        let rg = RowGroup::new(6, columns);

        let mut table = Table::new("cpu");
        table.add_row_group(rg);

        let exp_col_types = vec![
            ("region", LogicalDataType::String),
            ("count", LogicalDataType::Unsigned),
            ("time", LogicalDataType::Integer),
        ]
        .into_iter()
        .collect::<BTreeMap<_, _>>();
        assert_eq!(
            table
                .meta()
                .columns
                .iter()
                .map(|(k, v)| (k.as_str(), v.logical_data_type))
                .collect::<BTreeMap<_, _>>(),
            exp_col_types
        );

        // Build another row group.
        let tc = ColumnType::Time(Column::from(&[10_i64, 20, 30][..]));
        let rc = ColumnType::Tag(Column::from(&["south", "north", "east"][..]));
        let fc = ColumnType::Field(Column::from(&[1000_u64, 1002, 1200][..]));
        let columns = vec![
            ("time".to_string(), tc),
            ("region".to_string(), rc),
            ("count".to_string(), fc),
        ];
        let row_group = RowGroup::new(3, columns);
        table.add_row_group(row_group);

        // Get all the results
        let predicate = Predicate::with_time_range(&[], 1, 31);
        let results = table.read_filter(&Selection::Some(&["time", "count", "region"]), &predicate);

        // check the column types
        let exp_schema = ResultSchema {
            select_columns: vec![
                (
                    schema::ColumnType::Timestamp("time".to_owned()),
                    LogicalDataType::Integer,
                ),
                (
                    schema::ColumnType::Field("count".to_owned()),
                    LogicalDataType::Unsigned,
                ),
                (
                    schema::ColumnType::Tag("region".to_owned()),
                    LogicalDataType::String,
                ),
            ],
            ..ResultSchema::default()
        };
        assert_eq!(results.schema(), &exp_schema);

        let results = results.row_group_results();
        for result in &results {
            assert_eq!(result.schema(), &exp_schema);
        }

        assert_eq!(
            format!("{}", DisplayReadFilterResults(results)),
            "time,count,region
1,100,west
2,101,west
3,200,east
4,203,west
5,203,south
6,10,north
10,1000,south
20,1002,north
30,1200,east
",
        );

        let predicate =
            Predicate::with_time_range(&[BinaryExpr::from(("region", "!=", "south"))], 1, 25);

        // Apply a predicate `WHERE "region" != "south"`
        let results = table.read_filter(&Selection::Some(&["time", "region"]), &predicate);

        let exp_schema = ResultSchema {
            select_columns: vec![
                (
                    schema::ColumnType::Timestamp("time".to_owned()),
                    LogicalDataType::Integer,
                ),
                (
                    schema::ColumnType::Tag("region".to_owned()),
                    LogicalDataType::String,
                ),
            ],
            ..ResultSchema::default()
        };

        let results = results.row_group_results();
        for result in &results {
            assert_eq!(result.schema(), &exp_schema);
        }

        assert_eq!(
            format!("{}", DisplayReadFilterResults(results)),
            "time,region
1,west
2,west
3,east
4,west
6,north
20,north
",
        );
    }

    #[test]
    fn read_aggregate_no_groups() {
        // Build first row group.
        let columns = vec![
            (
                "time".to_string(),
                ColumnType::create_time(&[100, 200, 300]),
            ),
            (
                "region".to_string(),
                ColumnType::create_tag(&["west", "west", "east"]),
            ),
        ];
        let rg = RowGroup::new(3, columns);
        let mut table = Table::new("cpu");
        table.add_row_group(rg);

        // Build another row group.
        let columns = vec![
            ("time".to_string(), ColumnType::create_time(&[2, 3])),
            (
                "region".to_string(),
                ColumnType::create_tag(&["north", "north"]),
            ),
        ];
        let rg = RowGroup::new(2, columns);
        table.add_row_group(rg);

        // no predicate aggregate
        let mut results = table
            .read_aggregate(
                Predicate::default(),
                &Selection::Some(&[]),
                &[("time", AggregateType::Count), ("time", AggregateType::Sum)],
            )
            .unwrap();

        // check the column result schema
        let exp_schema = ResultSchema {
            aggregate_columns: vec![
                (
                    schema::ColumnType::Timestamp("time".to_owned()),
                    AggregateType::Count,
                    LogicalDataType::Unsigned,
                ),
                (
                    schema::ColumnType::Timestamp("time".to_owned()),
                    AggregateType::Sum,
                    LogicalDataType::Integer,
                ),
            ],
            ..ResultSchema::default()
        };
        assert_eq!(results.schema(), &exp_schema);

        assert_eq!(
            DisplayReadAggregateResults(vec![results.next_merged_result().unwrap()]).to_string(),
            "time_count,time_sum\n5,605\n",
        );
        assert!(matches!(results.next_merged_result(), None));

        // apply a predicate
        let mut results = table
            .read_aggregate(
                Predicate::new(vec![BinaryExpr::from(("region", "=", "west"))]),
                &Selection::Some(&[]),
                &[("time", AggregateType::Count), ("time", AggregateType::Sum)],
            )
            .unwrap();

        assert_eq!(
            DisplayReadAggregateResults(vec![results.next_merged_result().unwrap()]).to_string(),
            "time_count,time_sum\n2,300\n",
        );
        assert!(matches!(results.next_merged_result(), None));

        // group on wrong columns.
        let results = table.read_aggregate(
            Predicate::new(vec![BinaryExpr::from(("region", "=", "west"))]),
            &Selection::Some(&["time"]),
            &[("min", AggregateType::Min)],
        );

        assert!(matches!(
            &results,
            Err(Error::UnsupportedColumnOperation { .. })
        ),);
    }

    #[test]
    fn read_aggregate_result_display() {
        let result_a = ReadAggregateResult {
            schema: ResultSchema {
                select_columns: vec![],
                group_columns: vec![
                    (
                        schema::ColumnType::Tag("region".to_owned()),
                        LogicalDataType::String,
                    ),
                    (
                        schema::ColumnType::Tag("host".to_owned()),
                        LogicalDataType::String,
                    ),
                ],
                aggregate_columns: vec![(
                    schema::ColumnType::Tag("temp".to_owned()),
                    AggregateType::Sum,
                    LogicalDataType::Integer,
                )],
            },
            group_key_cols: vec![vec![Some("east")], vec![Some("host-a")]],
            aggregate_cols: vec![AggregateVec::SumI64(vec![Some(10)])],
            ..ReadAggregateResult::default()
        };

        let result_b = ReadAggregateResult {
            schema: ResultSchema {
                select_columns: vec![],
                group_columns: vec![
                    (
                        schema::ColumnType::Tag("region".to_owned()),
                        LogicalDataType::String,
                    ),
                    (
                        schema::ColumnType::Tag("host".to_owned()),
                        LogicalDataType::String,
                    ),
                ],
                aggregate_columns: vec![(
                    schema::ColumnType::Tag("temp".to_owned()),
                    AggregateType::Sum,
                    LogicalDataType::Integer,
                )],
            },
            group_key_cols: vec![vec![Some("west")], vec![Some("host-b")]],
            aggregate_cols: vec![AggregateVec::SumI64(vec![Some(100)])],
            ..Default::default()
        };

        let results = DisplayReadAggregateResults(vec![result_a, result_b]); //Display implementation
        assert_eq!(
            format!("{}", &results),
            "region,host,temp_sum
east,host-a,10
west,host-b,100
"
        );
    }

    #[test]
    fn column_names() {
        // Build a row group.
        let tc = ColumnType::Time(Column::from(&[1_i64, 2, 3][..]));
        let rc = ColumnType::Tag(Column::from(&["west", "south", "north"][..]));
        let columns = vec![("time".to_string(), tc), ("region".to_string(), rc)];

        let rg = RowGroup::new(3, columns);
        let mut table = Table::new("cpu".to_owned());
        table.add_row_group(rg);

        // add another row group
        let tc = ColumnType::Time(Column::from(&[200_i64, 300, 400][..]));
        let rc = ColumnType::Tag(Column::from(vec![Some("north"), None, None].as_slice()));
        let columns = vec![("time".to_string(), tc), ("region".to_string(), rc)];

        let rg = RowGroup::new(3, columns);
        table.add_row_group(rg);

        // Table looks like:
        //
        // region, time
        // ------------
        // west,     1
        // south,    2
        // north,    3
        // <- next row group ->
        // north,  200
        // NULL,   300
        // NULL,   400

        let mut dst: BTreeSet<String> = BTreeSet::new();
        dst = table.column_names(&Predicate::default(), Selection::All, dst);

        assert_eq!(
            dst.iter().cloned().collect::<Vec<_>>(),
            vec!["region".to_owned(), "time".to_owned()],
        );

        // re-run and get the same answer
        dst = table.column_names(&Predicate::default(), Selection::All, dst);
        assert_eq!(
            dst.iter().cloned().collect::<Vec<_>>(),
            vec!["region".to_owned(), "time".to_owned()],
        );

        // include a predicate that doesn't match any region rows and still get
        // region from previous results.
        dst = table.column_names(
            &Predicate::new(vec![BinaryExpr::from(("time", ">=", 300_i64))]),
            Selection::All,
            dst,
        );
        assert_eq!(
            dst.iter().cloned().collect::<Vec<_>>(),
            vec!["region".to_owned(), "time".to_owned()],
        );

        // wipe the destination buffer and region won't show up
        dst = table.column_names(
            &Predicate::new(vec![BinaryExpr::from(("time", ">=", 300_i64))]),
            Selection::All,
            BTreeSet::new(),
        );
        assert_eq!(
            dst.iter().cloned().collect::<Vec<_>>(),
            vec!["time".to_owned()],
        );
    }

    #[test]
    fn time_range() {
        // Build a row group.
        let tc = ColumnType::Time(Column::from(&[-29_i64, -100, 3, 2][..]));
        let rc = ColumnType::Tag(Column::from(&["west", "south", "north", "west"][..]));
        let columns = vec![
            (row_group::TIME_COLUMN_NAME.to_string(), tc),
            ("region".to_string(), rc),
        ];

        let rg = RowGroup::new(4, columns);
        let mut table = Table::new("cpu".to_owned());
        table.add_row_group(rg);

        assert_eq!(table.time_range().unwrap(), (-100, 3));
    }
}
