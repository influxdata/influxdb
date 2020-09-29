use std::collections::{BTreeMap, BTreeSet};
use std::slice::Iter;

use crate::column::{AggregateResult, AggregateType, Scalar, Value, Values};
use crate::segment::Segment;

/// A Table represents data for a single measurement.
///
/// Tables contain potentially many collections of rows in the form of segments.
/// These segments can be thought of as horizontally sliced segments of the
/// entire table, where each row within any segment is unique and not found on
/// any other segments for the table.
///
/// Rows within a table's segments can be sorted arbitrarily, therefore it is
/// possible that time-ranges (for example) can overlap across segments.
///
/// The current write path ensures that a single table emitted for a
/// measurement within any partition will have the same schema, therefore this
/// table's schema applies to all of the segments held within it.
///
/// The total size of a table is tracked and can be increased or reduced by
/// adding or removing segments.
pub struct Table {
    name: String,
    // schema // TODO(edd): schema type
    segments: Vec<Segment>,
}

impl Table {
    /// Create a new table.
    ///
    /// TODO(edd): it doesn't make sense to have a table without a segment.
    pub fn new(name: String) -> Self {
        Self {
            name,
            segments: vec![],
        }
    }

    /// Add a new segment to this table.
    pub fn add_segment(&mut self, segment: Segment) {
        todo!();
    }

    /// Remove the segment at `position` from table.
    pub fn drop_segment(&mut self, position: usize) {
        todo!();
    }

    /// Iterate over all segments for the table.
    pub fn iter(&mut self) -> Iter<'_, Segment> {
        self.segments.iter()
    }

    /// The name of the table (equivalent to measurement or table name).
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Determines if this table contains no segments.
    pub fn is_empty(&self) -> bool {
        self.segments.is_empty()
    }

    /// The total number of segments within this table.
    pub fn len(&self) -> usize {
        self.segments.len()
    }

    //
    // TODO:(edd) we need to figure out what the predicate object looks like
    // and then this API can be changed to support arbitrary predicates rather
    // the the equality predicates it currently supports.
    //

    /// Returns vectors of columnar data for the specified column
    /// selections.
    ///
    /// Results may be filtered by (currently only) equality predicates, but can
    /// be ranged by time, which should be represented as nanoseconds since the
    /// epoch. Results are included if they satisfy the predicate and fall
    /// with the [min, max) time range domain.
    pub fn select(
        &self,
        time_range: (i64, i64),
        predicates: &[(&str, &str)],
        select_columns: Vec<String>,
    ) -> BTreeMap<String, Values<'_>> {
        todo!();
    }

    /// Returns aggregates segmented by grouping keys.
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
        time_range: (i64, i64),
        predicates: &[(&str, &str)],
        group_columns: Vec<String>,
        aggregates: Vec<(String, AggregateType)>,
    ) -> BTreeMap<Vec<String>, Vec<(String, AggregateResult<'_>)>> {
        self.aggregate_window(time_range, predicates, group_columns, aggregates, 0)
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
        time_range: (i64, i64),
        predicates: &[(&str, &str)],
        group_columns: Vec<String>,
        aggregates: Vec<(String, AggregateType)>,
        window: i64,
    ) -> BTreeMap<Vec<String>, Vec<(String, AggregateResult<'_>)>> {
        todo!()
    }

    // Perform aggregates without any grouping. Filtering on optional predicates
    // and time range is still supported.
    fn read_aggregate_no_group(
        &self,
        time_range: (i64, i64),
        predicates: &[(&str, &str)],
        aggregates: Vec<(String, AggregateType)>,
    ) -> Vec<(String, AggregateResult<'_>)> {
        // The fast path where there are no predicates or a time range to apply.
        // We just want the equivalent of column statistics.
        if predicates.is_empty() {
            let mut results = Vec::with_capacity(aggregates.len());
            for (col_name, agg_type) in &aggregates {
                match agg_type {
                    AggregateType::Count => {
                        results.push((
                            col_name,
                            AggregateResult::Count(self.count(col_name, time_range)),
                        ));
                    }
                    AggregateType::First => {
                        results.push((
                            col_name,
                            AggregateResult::First(self.first(col_name, time_range.0)),
                        ));
                    }
                    AggregateType::Last => {
                        results.push((
                            col_name,
                            AggregateResult::Last(self.last(col_name, time_range.1)),
                        ));
                    }
                    AggregateType::Min => {
                        results.push((
                            col_name,
                            AggregateResult::Min(self.min(col_name, time_range)),
                        ));
                    }
                    AggregateType::Max => {
                        results.push((
                            col_name,
                            AggregateResult::Max(self.max(col_name, time_range)),
                        ));
                    }
                    AggregateType::Sum => {
                        results.push((
                            col_name,
                            AggregateResult::Sum(self.sum(col_name, time_range)),
                        ));
                    }
                }
            }
        }

        // Otherwise we have predicates so for each segment we will execute a
        // generalised aggregation method and build up the result set.
        todo!();
    }

    //
    // ---- Fast-path aggregations on single columns.
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
    fn first(&self, column_name: &str, time_lower_bound: i64) -> Option<(i64, Value<'_>)> {
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
    fn last(&self, column_name: &str, time_upper_bound: i64) -> Option<(i64, Value<'_>)> {
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

    /// The minimum non-null value in the column for the table.
    fn min(&self, column_name: &str, time_range: (i64, i64)) -> Value<'_> {
        // Loop over segments, skipping any that don't satisfy the time range.
        // Any segments completely overlapped can have a candidate min taken
        // directly from their zone map. Partially overlapped segments will be
        // read using the appropriate execution API.
        //
        // Return the min of minimums.
        todo!();
    }

    /// The maximum non-null value in the column for the table.
    fn max(&self, column_name: &str, time_range: (i64, i64)) -> Value<'_> {
        // Loop over segments, skipping any that don't satisfy the time range.
        // Any segments completely overlapped can have a candidate max taken
        // directly from their zone map. Partially overlapped segments will be
        // read using the appropriate execution API.
        //
        // Return the max of maximums.
        todo!();
    }

    /// The number of non-null values in the column for the table.
    fn count(&self, column_name: &str, time_range: (i64, i64)) -> u64 {
        // Loop over segments, skipping any that don't satisfy the time range.
        // Execute appropriate aggregation call on each segment and aggregate
        // the results.
        todo!();
    }

    /// The total sum of non-null values in the column for the table.
    fn sum(&self, column_name: &str, time_range: (i64, i64)) -> Option<Scalar> {
        // Loop over segments, skipping any that don't satisfy the time range.
        // Execute appropriate aggregation call on each segment and aggregate
        // the results.
        todo!();
    }

    //
    // ---- Schema API queries
    //

    /// Returns the distinct set of tag keys (column names) matching the provided
    /// optional predicates and time range.
    pub fn tag_keys(
        &self,
        time_range: (i64, i64),
        predicates: &[(&str, &str)],
    ) -> BTreeSet<String> {
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
        time_range: (i64, i64),
        predicates: &[(&str, &str)],
        tag_keys: &[String],
    ) -> BTreeMap<String, BTreeSet<&String>> {
        todo!();
    }
}

#[derive(Debug)]
struct PartitionMetaData {
    // The current size of the partition in bytes.
    size: usize,

    // The total number of rows in the partition.
    rows: usize,

    //
    column_names: Vec<String>,
    time_range: (i64, i64),
}
