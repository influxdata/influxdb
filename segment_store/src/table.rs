use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Display;
use std::slice::Iter;

use arrow_deps::arrow::record_batch::RecordBatch;

use crate::segment::{ColumnName, GroupKey, Predicate, Segment};
use crate::{
    column::{AggregateResult, AggregateType, OwnedValue, Scalar, Value},
    segment::{ReadFilterResult, ReadGroupResult},
};

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

    // Metadata about the table's segments
    meta: MetaData,

    // schema // TODO(edd): schema type
    segments: Vec<Segment>,
}

impl Table {
    /// Create a new table with the provided segment.
    pub fn new(name: String, segment: Segment) -> Self {
        Self {
            name,
            meta: MetaData::new(&segment),
            segments: vec![segment],
        }
    }

    /// Add a new segment to this table.
    pub fn add_segment(&mut self, segment: Segment) {
        self.segments.push(segment);
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

    /// The total size of the table in bytes.
    pub fn size(&self) -> u64 {
        todo!()
    }

    /// The number of rows in this table.
    pub fn rows(&self) -> u64 {
        todo!()
    }

    /// The time range of all segments within this table.
    pub fn time_range(&self) -> Option<(i64, i64)> {
        todo!()
    }

    /// The ranges on each column in the table (across all segments).
    pub fn column_ranges(&self) -> BTreeMap<String, (OwnedValue, OwnedValue)> {
        todo!()
    }

    // Determines if schema contains all the provided column names.
    fn has_all_columns(&self, names: &[ColumnName<'_>]) -> bool {
        for &name in names {
            if !self.meta.column_ranges.contains_key(name) {
                return false;
            }
        }
        true
    }

    // Identify set of segments that may satisfy the predicates.
    fn filter_segments(&self, predicates: &[Predicate<'_>]) -> Vec<&Segment> {
        let mut segments = Vec::with_capacity(self.segments.len());

        'seg: for segment in &self.segments {
            // check all provided predicates
            for (col_name, pred) in predicates {
                if !segment.column_could_satisfy_predicate(col_name, pred) {
                    continue 'seg;
                }
            }

            // segment could potentially satisfy all predicates
            segments.push(segment);
        }

        segments
    }

    /// Returns vectors of columnar data for the specified column
    /// selections.
    ///
    /// Results may be filtered by (currently only) conjunctive (AND) predicates,
    /// but can be ranged by time, which should be represented as nanoseconds
    /// since the epoch. Results are included if they satisfy the predicate and
    /// fall with the [min, max) time range domain.
    pub fn select<'a>(
        &'a self,
        columns: &[ColumnName<'a>],
        predicates: &[Predicate<'_>],
    ) -> ReadFilterResults<'a> {
        // identify segments where time range and predicates match could match
        // using segment meta data, and then execute against those segments and
        // merge results.
        let segments = self.filter_segments(predicates);

        let mut results = ReadFilterResults {
            names: columns.to_vec(),
            values: vec![],
        };

        if segments.is_empty() {
            return results;
        }

        for segment in segments {
            results
                .values
                .push(segment.read_filter(columns, predicates));
        }

        results
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
    pub fn aggregate<'a>(
        &'a self,
        predicates: &[Predicate<'a>],
        group_columns: &'a [ColumnName<'a>],
        aggregates: &'a [(ColumnName<'a>, AggregateType)],
    ) -> ReadGroupResults<'a> {
        if !self.has_all_columns(&group_columns) {
            return ReadGroupResults::default(); //TODO(edd): return an error here "group key column x not found"
        }

        if !self.has_all_columns(&aggregates.iter().map(|(name, _)| *name).collect::<Vec<_>>()) {
            return ReadGroupResults::default(); //TODO(edd): return an error here "aggregate column x not found"
        }

        if !self.has_all_columns(&predicates.iter().map(|(name, _)| *name).collect::<Vec<_>>()) {
            return ReadGroupResults::default(); //TODO(edd): return an error here "predicate column x not found"
        }

        // identify segments where time range and predicates match could match
        // using segment meta data, and then execute against those segments and
        // merge results.
        let segments = self.filter_segments(predicates);
        if segments.is_empty() {
            return ReadGroupResults::default();
        }

        let mut results = ReadGroupResults::default();
        results.values.reserve(segments.len());
        for segment in segments {
            let segment_result = segment.read_group(predicates, &group_columns, &aggregates);
            results.values.push(segment_result);
        }

        results.groupby_columns = group_columns;
        results.aggregate_columns = aggregates;
        results
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
        time_range: (i64, i64),
        predicates: &[(&str, &str)],
        group_columns: Vec<ColumnName<'a>>,
        aggregates: Vec<(ColumnName<'a>, AggregateType)>,
        window: i64,
    ) -> BTreeMap<GroupKey<'_>, Vec<(ColumnName<'a>, AggregateResult<'_>)>> {
        // identify segments where time range and predicates match could match
        // using segment meta data, and then execute against those segments and
        // merge results.
        todo!()
    }

    // Perform aggregates without any grouping. Filtering on optional predicates
    // and time range is still supported.
    fn read_aggregate_no_group<'a>(
        &self,
        time_range: (i64, i64),
        predicates: &[(&str, &str)],
        aggregates: Vec<(ColumnName<'a>, AggregateType)>,
    ) -> Vec<(ColumnName<'a>, AggregateResult<'_>)> {
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
                        let res = match self.sum(col_name, time_range) {
                            Some(x) => x,
                            None => Scalar::Null,
                        };

                        results.push((col_name, AggregateResult::Sum(res)));
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
    pub fn tag_keys<'a>(
        &self,
        time_range: (i64, i64),
        predicates: &[(&str, &str)],
        found_columns: &BTreeSet<String>,
    ) -> BTreeSet<ColumnName<'a>> {
        // Firstly, this should short-circuit early if all of the table's columns
        // are present in `found_columns`.
        //
        // Otherwise, identify segments where time range and predicates match could match
        // using segment meta data and then execute against those segments and
        // merge results.
        todo!();
    }

    /// Returns the distinct set of tag values (column values) for each provided
    /// tag key, where each returned value lives in a row matching the provided
    /// optional predicates and time range.
    ///
    /// As a special case, if `tag_keys` is empty then all distinct values for
    /// all columns (tag keys) are returned for the partition.
    pub fn tag_values<'a>(
        &self,
        time_range: (i64, i64),
        predicates: &[(&str, &str)],
        tag_keys: &[String],
        found_tag_values: &BTreeMap<String, BTreeSet<&String>>,
    ) -> BTreeMap<ColumnName<'a>, BTreeSet<&String>> {
        // identify segments where time range, predicates and tag keys match
        // could match using segment meta data, and then execute against those
        // segments and merge results.
        //
        // For each segment push the tag values that have already been found for
        // the tag key down in an attempt to reduce execution against columns
        // that only have values that have already been found.
        todo!();
    }
}

/// Convert a record batch into a table.
impl From<RecordBatch> for Table {
    fn from(rb: RecordBatch) -> Self {
        todo!()
    }
}

struct MetaData {
    // The total size of the table in bytes.
    size: u64,

    // The total number of rows in the table.
    rows: u64,

    // The distinct set of columns for this table (all of these columns will
    // appear in all of the table's segments) and the range of values for
    // each of those columns.
    //
    // This can be used to skip the table entirely if a logical predicate can't
    // possibly match based on the range of values a column has.
    column_ranges: BTreeMap<String, (OwnedValue, OwnedValue)>,

    // The total time range of this table spanning all of the segments within
    // the table.
    //
    // This can be used to skip the table entirely if the time range for a query
    // falls outside of this range.
    time_range: Option<(i64, i64)>,
}

impl MetaData {
    pub fn new(segment: &Segment) -> Self {
        Self {
            size: segment.size(),
            rows: u64::from(segment.rows()),
            column_ranges: segment
                .column_ranges()
                .iter()
                .map(|(k, v)| (k.to_string(), (v.0.clone(), v.1.clone())))
                .collect(),
            time_range: Some(segment.time_range()),
        }
    }

    pub fn add_segment(&mut self, segment: &Segment) {
        // update size, rows, column ranges, time range
        self.size += segment.size();
        self.rows += u64::from(segment.rows());

        assert_eq!(self.column_ranges.len(), segment.column_ranges().len());
        for (segment_column_name, (segment_column_range_min, segment_column_range_max)) in
            segment.column_ranges()
        {
            let mut curr_range = self
                .column_ranges
                .get_mut(&segment_column_name.to_string())
                .unwrap();
            if segment_column_range_min < &curr_range.0 {
                curr_range.0 = segment_column_range_min.clone();
            }

            if segment_column_range_max > &curr_range.1 {
                curr_range.1 = segment_column_range_max.clone();
            }
        }
    }

    // invalidate should be called when a segment is removed that impacts the
    // meta data.
    pub fn invalidate(&mut self) {
        // Update size, rows, time_range by inspecting each segment's metadata
        todo!()
    }
}

/// Encapsulates results from tables with a structure that makes them easier
/// to work with and display.
pub struct ReadFilterResults<'a> {
    pub names: Vec<ColumnName<'a>>,
    pub values: Vec<ReadFilterResult<'a>>,
}

impl<'a> ReadFilterResults<'a> {
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }
}

impl<'a> Display for ReadFilterResults<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // header line.
        for (i, k) in self.names.iter().enumerate() {
            write!(f, "{}", k)?;

            if i < self.names.len() - 1 {
                write!(f, ",")?;
            }
        }
        writeln!(f)?;

        if self.is_empty() {
            return Ok(());
        }

        // Display all the results of each segment
        for segment_values in &self.values {
            segment_values.fmt(f)?;
        }
        Ok(())
    }
}

#[derive(Default)]
pub struct ReadGroupResults<'a> {
    // column-wise collection of columns being grouped by
    groupby_columns: &'a [ColumnName<'a>],

    // column-wise collection of columns being aggregated on
    aggregate_columns: &'a [(ColumnName<'a>, AggregateType)],

    // segment-wise result sets containing grouped values and aggregates
    values: Vec<ReadGroupResult<'a>>,
}

impl<'a> std::fmt::Display for ReadGroupResults<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // header line - display group columns first
        for (i, name) in self.groupby_columns.iter().enumerate() {
            write!(f, "{},", name)?;
        }

        // then display aggregate columns
        for (i, (col_name, col_agg)) in self.aggregate_columns.iter().enumerate() {
            write!(f, "{}_{}", col_name, col_agg)?;

            if i < self.aggregate_columns.len() - 1 {
                write!(f, ",")?;
            }
        }
        writeln!(f)?;

        // Display all the results of each segment
        for segment_values in &self.values {
            segment_values.fmt(f)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::column::{cmp::Operator, Column};
    use crate::segment::{ColumnType, TIME_COLUMN_NAME};

    fn build_predicates(
        from: i64,
        to: i64,
        column_predicates: Vec<Predicate<'_>>,
    ) -> Vec<Predicate<'_>> {
        let mut arr = vec![
            (
                TIME_COLUMN_NAME,
                (Operator::GTE, Value::Scalar(Scalar::I64(from))),
            ),
            (
                TIME_COLUMN_NAME,
                (Operator::LT, Value::Scalar(Scalar::I64(to))),
            ),
        ];

        arr.extend(column_predicates);
        arr
    }

    #[test]
    fn select() {
        // Build first segment.
        let mut columns = BTreeMap::new();
        let tc = ColumnType::Time(Column::from(&[1_i64, 2, 3, 4, 5, 6][..]));
        columns.insert("time".to_string(), tc);

        let rc = ColumnType::Tag(Column::from(
            &["west", "west", "east", "west", "south", "north"][..],
        ));
        columns.insert("region".to_string(), rc);

        let fc = ColumnType::Field(Column::from(&[100_u64, 101, 200, 203, 203, 10][..]));
        columns.insert("count".to_string(), fc);

        let segment = Segment::new(6, columns);

        let mut table = Table::new("cpu".to_owned(), segment);

        // Build another segment.
        let mut columns = BTreeMap::new();
        let tc = ColumnType::Time(Column::from(&[10_i64, 20, 30][..]));
        columns.insert("time".to_string(), tc);
        let rc = ColumnType::Tag(Column::from(&["south", "north", "east"][..]));
        columns.insert("region".to_string(), rc);
        let fc = ColumnType::Field(Column::from(&[1000_u64, 1002, 1200][..]));
        columns.insert("count".to_string(), fc);
        let segment = Segment::new(3, columns);
        table.add_segment(segment);

        // Get all the results
        let results = table.select(
            &["time", "count", "region"],
            &build_predicates(1, 31, vec![]),
        );
        assert_eq!(
            format!("{}", &results),
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

        // Apply a predicate `WHERE "region" != "south"`
        let results = table.select(
            &["time", "region"],
            &build_predicates(
                1,
                25,
                vec![("region", (Operator::NotEqual, Value::String("south")))],
            ),
        );

        assert_eq!(
            format!("{}", &results),
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
}
