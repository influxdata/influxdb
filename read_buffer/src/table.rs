use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Display;
use std::slice::Iter;

use crate::column::{AggregateResult, AggregateType, OwnedValue, Scalar, Value};
use crate::schema::ResultSchema;
use crate::{
    column,
    column::LogicalDataType,
    row_group::{self, ColumnName, GroupKey, Predicate, RowGroup},
};

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

    // Metadata about the table's segments
    meta: MetaData,

    row_groups: Vec<RowGroup>,
}

impl Table {
    /// Create a new table with the provided row_group.
    pub fn new(name: String, rg: RowGroup) -> Self {
        Self {
            name,
            meta: MetaData::new(&rg),
            row_groups: vec![rg],
        }
    }

    /// Add a new row group to this table.
    pub fn add_row_group(&mut self, rg: RowGroup) {
        self.meta.update(&rg);
        self.row_groups.push(rg);
    }

    /// Remove the row group at `position` from table.
    pub fn drop_segment(&mut self, position: usize) {
        todo!();
    }

    /// Iterate over all row groups for the table.
    pub fn iter(&mut self) -> Iter<'_, RowGroup> {
        self.row_groups.iter()
    }

    /// The name of the table (equivalent to measurement or table name).
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Determines if this table contains no row groups.
    pub fn is_empty(&self) -> bool {
        self.row_groups.is_empty()
    }

    /// The total number of row groups within this table.
    pub fn len(&self) -> usize {
        self.row_groups.len()
    }

    /// The total size of the table in bytes.
    pub fn size(&self) -> u64 {
        self.meta.size
    }

    /// The number of rows in this table.
    pub fn rows(&self) -> u64 {
        self.meta.rows
    }

    /// The time range of all row groups within this table.
    pub fn time_range(&self) -> Option<(i64, i64)> {
        self.meta.time_range
    }

    /// The ranges on each column in the table (across all row groups).
    pub fn column_ranges(&self) -> BTreeMap<String, (OwnedValue, OwnedValue)> {
        todo!()
    }

    /// The logical data-type of each column in the `Table`'s schema.
    pub fn column_logical_types(&self) -> &BTreeMap<String, column::LogicalDataType> {
        &self.meta.column_types
    }

    // Identify set of row groups that might satisfy the predicate.
    fn filter_row_groups(&self, predicate: &Predicate) -> Vec<&RowGroup> {
        let mut rgs = Vec::with_capacity(self.row_groups.len());

        'rowgroup: for rg in &self.row_groups {
            // check all expressions in predicate
            if !rg.could_satisfy_conjunctive_binary_expressions(predicate.iter()) {
                continue 'rowgroup;
            }

            // row group could potentially satisfy predicate
            rgs.push(rg);
        }

        rgs
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
        columns: &ColumnSelection<'_>,
        predicate: &Predicate,
    ) -> ReadFilterResults<'a> {
        // identify row groups where time range and predicates match could match
        // using row group meta data, and then execute against those row groups
        // and merge results.
        let rgs = self.filter_row_groups(predicate);

        let schema = match columns {
            ColumnSelection::All => self.meta.schema(),
            ColumnSelection::Some(column_names) => self.meta.schema_for_column_names(column_names),
        };

        // temp I think I can remove `columns` and `predicates` from this..
        let columns = schema
            .iter()
            .map(|(name, _)| name.to_string())
            .collect::<Vec<_>>();
        ReadFilterResults {
            columns,
            predicate: predicate.clone(),
            schema,
            row_groups: rgs,
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
        group_columns: &'input [ColumnName<'input>],
        aggregates: &'input [(ColumnName<'input>, AggregateType)],
    ) -> ReadAggregateResults<'_> {
        // Filter out any column names that we do not have data for.
        let schema = ResultSchema {
            group_columns: self.meta.schema_for_column_names(group_columns),
            aggregate_columns: self.meta.schema_for_aggregate_column_names(aggregates),
            ..ResultSchema::default()
        };

        // TODO(edd): for now punt on predicate pruning but need to figure it out.
        let row_groups = self.filter_row_groups(&predicate);

        // return the iterator to build the results.
        ReadAggregateResults {
            schema,
            predicate,
            row_groups,
        }
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

    /// Returns the distinct set of tag keys (column names) matching the
    /// provided optional predicates and time range.
    pub fn tag_keys<'a>(
        &self,
        time_range: (i64, i64),
        predicates: &[(&str, &str)],
        found_columns: &BTreeSet<String>,
    ) -> BTreeSet<ColumnName<'a>> {
        // Firstly, this should short-circuit early if all of the table's columns
        // are present in `found_columns`.
        //
        // Otherwise, identify segments where time range and predicates match could
        // match using segment meta data and then execute against those segments
        // and merge results.
        todo!();
    }

    /// Returns the distinct set of tag values (column values) for each provided
    /// tag key, where each returned value lives in a row matching the provided
    /// optional predicates and time range.
    ///
    /// As a special case, if `tag_keys` is empty then all distinct values for
    /// all columns (tag keys) are returned for the chunk.
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

// TODO(edd): reduce owned strings here by, e.g., using references as keys.
struct MetaData {
    // The total size of the table in bytes.
    size: u64,

    // The total number of rows in the table.
    rows: u64,

    // The distinct set of columns for this table (all of these columns will
    // appear in all of the table's row groups) and the range of values for
    // each of those columns.
    //
    // This can be used to skip the table entirely if a logical predicate can't
    // possibly match based on the range of values a column has.
    column_ranges: BTreeMap<String, (OwnedValue, OwnedValue)>,

    // The `ReadBuffer` logical types associated with the columns in the table
    column_types: BTreeMap<String, column::LogicalDataType>,

    column_names: Vec<String>,

    // The total time range of this table spanning all of the row groups within
    // the table.
    //
    // This can be used to skip the table entirely if the time range for a query
    // falls outside of this range.
    time_range: Option<(i64, i64)>,
}

impl MetaData {
    pub fn new(rg: &RowGroup) -> Self {
        Self {
            size: rg.size(),
            rows: u64::from(rg.rows()),
            column_ranges: rg
                .column_ranges()
                .iter()
                .map(|(k, v)| (k.to_string(), (v.0.clone(), v.1.clone())))
                .collect(),
            column_types: rg
                .column_logical_types()
                .iter()
                .map(|(k, v)| (k.clone(), *v))
                .collect(),
            column_names: rg.column_ranges().keys().cloned().collect(),
            time_range: Some(rg.time_range()),
        }
    }

    // Extract schema information for a set of columns. If a column name does
    // not exist within the `Table` schema it is ignored and not present within
    // the resulting schema information.
    fn schema_for_column_names(&self, names: &[ColumnName<'_>]) -> Vec<(String, LogicalDataType)> {
        names
            .iter()
            .filter_map(|&name| match self.column_types.get_key_value(name) {
                Some((name, data_type)) => Some((name.clone(), *data_type)),
                None => None,
            })
            .collect::<Vec<_>>()
    }

    // As `schema_for_column_names` but also embeds the provided aggregate type.
    fn schema_for_aggregate_column_names(
        &self,
        names: &[(ColumnName<'_>, AggregateType)],
    ) -> Vec<(String, AggregateType, LogicalDataType)> {
        names
            .iter()
            .filter_map(
                |(name, agg_type)| match self.column_types.get_key_value(*name) {
                    Some((name, data_type)) => Some((name.clone(), *agg_type, *data_type)),
                    None => None,
                },
            )
            .collect::<Vec<_>>()
    }

    // Extract all schema information for the `Table`.
    fn schema(&self) -> Vec<(String, LogicalDataType)> {
        self.column_types
            .iter()
            .map(|(k, v)| (k.clone(), *v))
            .collect::<Vec<_>>()
    }

    pub fn all_column_names(&self) -> Vec<&str> {
        self.column_names.iter().map(|name| name.as_str()).collect()
    }

    pub fn update(&mut self, rg: &RowGroup) {
        // update size, rows, column ranges, time range
        self.size += rg.size();
        self.rows += u64::from(rg.rows());

        // The incoming row group must have the same schema as the existing row
        // groups in the table.
        assert_eq!(&self.column_types, rg.column_logical_types());

        assert_eq!(self.column_ranges.len(), rg.column_ranges().len());
        for (column_name, (column_range_min, column_range_max)) in rg.column_ranges() {
            let mut curr_range = self
                .column_ranges
                .get_mut(&column_name.to_string())
                .unwrap();
            if column_range_min < &curr_range.0 {
                curr_range.0 = column_range_min.clone();
            }

            if column_range_max > &curr_range.1 {
                curr_range.1 = column_range_max.clone();
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

/// A collection of columns to include in query results.
///
/// The `All` variant denotes that the caller wishes to include all table
/// columns in the results.
pub enum ColumnSelection<'a> {
    All,
    Some(&'a [&'a str]),
}

/// Results of a `read_filter` execution on the table. Execution is lazy -
/// row groups are only queried when `ReadFilterResults` is iterated.
pub struct ReadFilterResults<'table> {
    // schema of all columns in the query results
    schema: Vec<(String, LogicalDataType)>,

    // These row groups passed the predicates and need to be queried.
    row_groups: Vec<&'table RowGroup>,

    // TODO(edd): encapsulate these into a single executor function that just
    // executes on the next row group.
    columns: Vec<String>,
    predicate: Predicate,
}

impl<'table> ReadFilterResults<'table> {
    pub fn is_empty(&self) -> bool {
        self.row_groups.is_empty()
    }

    /// Returns the schema associated with table result and therefore all of the
    /// results for all of row groups in the table results.
    pub fn schema(&self) -> &Vec<(String, LogicalDataType)> {
        &self.schema
    }
}

impl<'a> Iterator for ReadFilterResults<'a> {
    type Item = row_group::ReadFilterResult<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.is_empty() {
            return None;
        }

        let row_group = self.row_groups.remove(0);
        let result = row_group.read_filter(
            &self
                .columns
                .iter()
                .map(|name| name.as_str())
                .collect::<Vec<_>>(),
            &self.predicate,
        );
        if result.is_empty() {
            return self.next(); // try next row group
        }

        assert_eq!(result.schema(), self.schema()); // validate schema
        Some(result)
    }
}

// Helper type that can pretty print a set of results for `read_filter`.
struct DisplayReadFilterResults<'a>(Vec<row_group::ReadFilterResult<'a>>);

impl<'a> Display for DisplayReadFilterResults<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.0.is_empty() {
            return Ok(());
        }

        let schema = self.0[0].schema();
        // header line.
        for (i, (k, _)) in schema.iter().enumerate() {
            write!(f, "{}", k)?;

            if i < schema.len() - 1 {
                write!(f, ",")?;
            }
        }
        writeln!(f)?;

        // Display all the results of each row group
        for row_group in &self.0 {
            row_group.fmt(f)?;
        }
        Ok(())
    }
}

#[derive(Default)]
pub struct ReadAggregateResults<'table> {
    // schema information for the results
    schema: ResultSchema,

    // the predicate to apply to each row group.
    predicate: Predicate,

    // row groups that will be executed against
    row_groups: Vec<&'table RowGroup>,
}

impl<'a> ReadAggregateResults<'a> {
    /// Returns the schema associated with table result and therefore all of
    /// results from row groups.
    pub fn schema(&self) -> &ResultSchema {
        &self.schema
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
impl<'a> Iterator for ReadAggregateResults<'a> {
    type Item = row_group::ReadAggregateResult<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.row_groups.is_empty() {
            return None;
        }

        let merged_results = self.row_groups.remove(0).read_aggregate(
            &self.predicate,
            &self.schema.group_column_names_iter().collect::<Vec<_>>(),
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
        for row_group in &self.row_groups {
            let result = row_group.read_aggregate(
                &self.predicate,
                &self.schema.group_column_names_iter().collect::<Vec<_>>(),
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
            todo!();
        }

        Some(merged_results)
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
    use crate::column::{Column, LogicalDataType};
    use crate::row_group::{BinaryExpr, ColumnType, ReadAggregateResult};

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

        let rg = RowGroup::new(6, columns);

        let mut table = Table::new("cpu".to_owned(), rg);
        let exp_col_types = vec![
            ("region".to_owned(), LogicalDataType::String),
            ("count".to_owned(), LogicalDataType::Unsigned),
            ("time".to_owned(), LogicalDataType::Integer),
        ]
        .into_iter()
        .collect::<BTreeMap<_, _>>();
        assert_eq!(table.column_logical_types(), &exp_col_types);

        // Build another segment.
        let mut columns = BTreeMap::new();
        let tc = ColumnType::Time(Column::from(&[10_i64, 20, 30][..]));
        columns.insert("time".to_string(), tc);
        let rc = ColumnType::Tag(Column::from(&["south", "north", "east"][..]));
        columns.insert("region".to_string(), rc);
        let fc = ColumnType::Field(Column::from(&[1000_u64, 1002, 1200][..]));
        columns.insert("count".to_string(), fc);
        let segment = RowGroup::new(3, columns);
        table.add_row_group(segment);

        // Get all the results
        let predicate = Predicate::with_time_range(&[], 1, 31);
        let results = table.read_filter(
            &ColumnSelection::Some(&["time", "count", "region"]),
            &predicate,
        );

        // check the column types
        let exp_schema = vec![
            ("time".to_owned(), LogicalDataType::Integer),
            ("count".to_owned(), LogicalDataType::Unsigned),
            ("region".to_owned(), LogicalDataType::String),
        ];
        assert_eq!(results.schema(), &exp_schema);

        let mut all = vec![];
        for result in results {
            assert_eq!(result.schema(), &exp_schema);
            all.push(result);
        }

        assert_eq!(
            format!("{}", DisplayReadFilterResults(all)),
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
        let results = table.read_filter(&ColumnSelection::Some(&["time", "region"]), &predicate);

        let mut all = vec![];
        for result in results {
            all.push(result);
        }

        assert_eq!(
            format!("{}", DisplayReadFilterResults(all)),
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
    fn read_group_result() {
        let results = DisplayReadAggregateResults(vec![
            ReadAggregateResult {
                schema: ResultSchema {
                    select_columns: vec![],
                    group_columns: vec![
                        ("region".to_owned(), LogicalDataType::String),
                        ("host".to_owned(), LogicalDataType::String),
                    ],
                    aggregate_columns: vec![(
                        "temp".to_owned(),
                        AggregateType::Sum,
                        LogicalDataType::Integer,
                    )],
                },
                group_keys: vec![vec![Value::String("east"), Value::String("host-a")].into()],
                aggregates: vec![vec![AggregateResult::Sum(Scalar::I64(10))]],
            },
            ReadAggregateResult {
                schema: ResultSchema {
                    select_columns: vec![],
                    group_columns: vec![
                        ("region".to_owned(), LogicalDataType::String),
                        ("host".to_owned(), LogicalDataType::String),
                    ],
                    aggregate_columns: vec![(
                        "temp".to_owned(),
                        AggregateType::Sum,
                        LogicalDataType::Integer,
                    )],
                },
                group_keys: vec![vec![Value::String("west"), Value::String("host-b")].into()],
                aggregates: vec![vec![AggregateResult::Sum(Scalar::I64(100))]],
            },
        ]);

        //Display implementation
        assert_eq!(
            format!("{}", &results),
            "region,host,temp_sum
east,host-a,10
west,host-b,100
"
        );
    }
}
