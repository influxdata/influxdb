use std::collections::BTreeMap;

use arrow_deps::arrow::datatypes::SchemaRef;

use crate::column::{cmp::Operator, Column, RowIDs, RowIDsOption, Scalar, Value, Values};

/// The name used for a timestamp column.
pub const TIME_COLUMN_NAME: &str = data_types::TIME_COLUMN_NAME;

#[derive(Debug)]
pub struct Schema {
    schema_ref: SchemaRef,
    // TODO(edd): column sort order??
}

impl Schema {
    pub fn new(schema: SchemaRef) -> Self {
        Self { schema_ref: schema }
    }

    pub fn schema_ref(&self) -> SchemaRef {
        self.schema_ref.clone()
    }

    pub fn cols(&self) -> usize {
        self.schema_ref.fields().len()
    }
}

/// A Segment is an immutable horizontal section (segment) of a table. By
/// definition it has the same schema as all the other segments in the table.
/// Further, all the columns within the segment have the same number of logical
/// rows.
///
/// This implementation will pull over the bulk of the prototype segment store
/// crate.
pub struct Segment<'a> {
    meta: MetaData<'a>,

    all_columns: BTreeMap<ColumnName<'a>, &'a Column>,

    tag_columns: Vec<&'a Column>,
    field_columns: Vec<&'a Column>,
    time_column: &'a Column,
}

impl<'a> Segment<'a> {
    pub fn new(rows: u32, columns: BTreeMap<ColumnName<'a>, &'a ColumnType>) -> Self {
        let mut meta = MetaData {
            rows,
            ..MetaData::default()
        };

        let mut tag_columns: Vec<&'a Column> = vec![];
        let mut field_columns: Vec<&'a Column> = vec![];
        let mut time_column: Option<&'a Column> = None;
        let mut all_columns = BTreeMap::new();

        for (name, ct) in columns {
            meta.size += ct.size();

            match ct {
                ColumnType::Tag(c) => {
                    assert_eq!(c.num_rows(), rows);

                    tag_columns.push(&c);

                    if let Some(range) = tag_columns.last().unwrap().column_range() {
                        meta.column_ranges.insert(name, range);
                    }
                    all_columns.insert(name, c);
                }
                ColumnType::Field(c) => {
                    assert_eq!(c.num_rows(), rows);

                    field_columns.push(&c);

                    if let Some(range) = c.column_range() {
                        meta.column_ranges.insert(name, range);
                    }
                    all_columns.insert(name, c);
                }
                ColumnType::Time(c) => {
                    assert_eq!(c.num_rows(), rows);

                    let range = c.column_range();
                    meta.time_range = match range {
                        None => panic!("time column must have non-null value"),
                        Some((
                            Value::Scalar(Scalar::I64(min)),
                            Value::Scalar(Scalar::I64(max)),
                        )) => (min, max),
                        Some((_, _)) => unreachable!("unexpected types for time range"),
                    };

                    meta.column_ranges.insert(name, range.unwrap());
                    match time_column {
                        Some(_) => panic!("multiple time columns unsupported"),
                        None => {
                            // probably not a firm requirement....
                            assert_eq!(name, TIME_COLUMN_NAME);
                            time_column = Some(&c);
                        }
                    }
                    all_columns.insert(name, c);
                }
            }
        }

        Self {
            meta,
            all_columns,
            tag_columns,
            field_columns,
            time_column: time_column.unwrap(),
        }
    }

    /// The total size in bytes of the segment
    pub fn size(&self) -> u64 {
        self.meta.size
    }

    /// The number of rows in the segment (all columns have the same number of
    /// rows).
    pub fn rows(&self) -> u32 {
        self.meta.rows
    }

    /// The ranges on each column in the segment
    pub fn column_ranges(&self) -> &BTreeMap<ColumnName<'a>, (Value<'a>, Value<'a>)> {
        &self.meta.column_ranges
    }

    /// The time range of the segment (of the time column).
    pub fn time_range(&self) -> (i64, i64) {
        self.meta.time_range
    }

    /// Efficiently determine if the provided predicate might be satisfied by
    /// the provided column.
    pub fn column_could_satisfy_predicate(
        &self,
        column_name: ColumnName<'_>,
        predicate: &(Operator, Value<'_>),
    ) -> bool {
        self.meta
            .segment_could_satisfy_predicate(column_name, predicate)
    }

    ///
    /// Methods for reading the segment.
    ///

    /// Returns a set of materialised column values that satisfy a set of
    /// predicates.
    ///
    /// Right now, predicates are conjunctive (AND).
    pub fn read_filter(
        &self,
        columns: &[ColumnName<'a>],
        predicates: &[Predicate<'_>],
    ) -> Vec<(ColumnName<'a>, Values)> {
        let row_ids = self.row_ids_from_predicates(predicates);
        self.materialise_rows(columns, row_ids)
    }

    fn materialise_rows(
        &self,
        columns: &[ColumnName<'a>],
        row_ids: RowIDsOption,
    ) -> Vec<(ColumnName<'a>, Values)> {
        let mut results = vec![];
        match row_ids {
            RowIDsOption::None(_) => results, // nothing to materialise
            RowIDsOption::Some(row_ids) => {
                // TODO(edd): causes an allocation. Implement a way to pass a pooled
                // buffer to the croaring Bitmap API.
                let row_ids = row_ids.to_vec();

                for col_name in columns {
                    let col = self.all_columns.get(*col_name).unwrap();
                    results.push((*col_name, col.values(row_ids.as_slice())));
                }
                results
            }

            RowIDsOption::All(_) => {
                // TODO(edd): Perf - add specialised method to get all
                // materialised values from a column without having to
                // materialise a vector of row ids.......
                let row_ids = (0..self.rows()).collect::<Vec<_>>();

                for col_name in columns {
                    let col = self.all_columns.get(*col_name).unwrap();
                    results.push((*col_name, col.values(row_ids.as_slice())));
                }
                results
            }
        }
    }

    // Determines the set of row ids that satisfy the time range and all of the
    // optional predicates.
    //
    // TODO(edd): right now `time_range` is special cased so we can use the
    // optimised execution path in the column to filter on a range. However,
    // eventually we should be able to express the time range as just another
    // one or two predicates.
    fn row_ids_from_predicates(&self, predicates: &[Predicate<'_>]) -> RowIDsOption {
        // TODO(edd): perf - pool the dst buffer so we can re-use it across
        // subsequent calls to `row_ids_from_predicates`.
        // Right now this buffer will be re-used across all columns in the
        // segment with predicates.
        let mut dst = RowIDs::new_bitmap();

        // find the time range predicates and execute a specialised range based
        // row id lookup.
        let time_predicates = predicates
            .iter()
            .filter(|(col_name, _)| col_name == &TIME_COLUMN_NAME)
            .collect::<Vec<_>>();
        assert!(time_predicates.len() == 2);

        let time_row_ids = self.time_column.row_ids_filter_range(
            &time_predicates[0].1, // min time
            &time_predicates[1].1, // max time
            dst,
        );

        // TODO(edd): potentially pass this in so we can re-use it once we
        // have materialised any results.
        let mut result_row_ids = RowIDs::new_bitmap();

        match time_row_ids {
            // No matching rows based on time range - return buffer
            RowIDsOption::None(_) => return time_row_ids,

            // all rows match - continue to apply predicates
            RowIDsOption::All(_dst) => {
                dst = _dst; // hand buffer back
            }

            // some rows match - continue to apply predicates
            RowIDsOption::Some(row_ids) => {
                // union empty result set with matching timestamp rows
                result_row_ids.union(&row_ids);
                dst = row_ids // hand buffer back
            }
        }

        for (col_name, (op, value)) in predicates {
            if col_name == &TIME_COLUMN_NAME {
                continue; // we already processed the time column as a special case.
            }
            // N.B column should always exist because validation of
            // predicates should happen at the `Table` level.
            let col = self.all_columns.get(*col_name).unwrap();

            // Explanation of how this buffer pattern works here. The idea is
            // that the buffer should be returned to the caller so it can be
            // re-used on other columns. To do that we need to hand the buffer
            // back even if we haven't populated it with any results.
            match col.row_ids_filter(op, value, dst) {
                // No rows will be returned for the segment because this column
                // doe not match any rows.
                RowIDsOption::None(_dst) => return RowIDsOption::None(_dst),

                // Intersect the row ids found at this column with all those
                // found on other column predicates.
                RowIDsOption::Some(row_ids) => {
                    if result_row_ids.is_empty() {
                        result_row_ids.union(&row_ids)
                    }
                    result_row_ids.intersect(&row_ids);
                    dst = row_ids; // hand buffer back
                }

                // This is basically a no-op because all rows match the
                // predicate on this column.
                RowIDsOption::All(_dst) => {
                    dst = _dst; // hand buffer back
                }
            }
        }

        if result_row_ids.is_empty() {
            // All rows matched all predicates - return the empty buffer.
            return RowIDsOption::All(result_row_ids);
        }
        RowIDsOption::Some(result_row_ids)
    }
}

pub type Predicate<'a> = (ColumnName<'a>, (Operator, Value<'a>));

// A GroupKey is an ordered collection of row values. The order determines which
// columns the values originated from.
pub type GroupKey = Vec<String>;

// A representation of a column name.
pub type ColumnName<'a> = &'a str;

/// The logical type that a column could have.
pub enum ColumnType {
    Tag(Column),
    Field(Column),
    Time(Column),
}

impl ColumnType {
    // The total size in bytes of the column
    pub fn size(&self) -> u64 {
        match &self {
            ColumnType::Tag(c) => c.size(),
            ColumnType::Field(c) => c.size(),
            ColumnType::Time(c) => c.size(),
        }
    }
}

// The GroupingStrategy determines which algorithm is used for calculating
// groups.
// enum GroupingStrategy {
//     // AutoGroup lets the executor determine the most appropriate grouping
//     // strategy using heuristics.
//     AutoGroup,

//     // HashGroup specifies that groupings should be done using a hashmap.
//     HashGroup,

//     // SortGroup specifies that groupings should be determined by first sorting
//     // the data to be grouped by the group-key.
//     SortGroup,
// }

#[derive(Default, Debug)]
struct MetaData<'a> {
    // The total size of the table in bytes.
    size: u64,

    // The total number of rows in the table.
    rows: u32,

    // The distinct set of columns for this table (all of these columns will
    // appear in all of the table's segments) and the range of values for
    // each of those columns.
    //
    // This can be used to skip the table entirely if a logical predicate can't
    // possibly match based on the range of values a column has.
    column_ranges: BTreeMap<ColumnName<'a>, (Value<'a>, Value<'a>)>,

    // The total time range of this table spanning all of the segments within
    // the table.
    //
    // This can be used to skip the table entirely if the time range for a query
    // falls outside of this range.
    time_range: (i64, i64),
}

impl MetaData<'_> {
    // helper function to determine if the provided predicate could be satisfied by
    // the segment. If this function returns `false` then there is no point
    // attempting to read data from the segment.
    //
    pub fn segment_could_satisfy_predicate(
        &self,
        column_name: ColumnName<'_>,
        predicate: &(Operator, Value<'_>),
    ) -> bool {
        let (column_min, column_max) = match self.column_ranges.get(column_name) {
            Some(range) => range,
            None => return false, // column doesn't exist.
        };

        let (op, value) = predicate;
        match op {
            // If the column range covers the value then it could contain that value.
            Operator::Equal => column_min <= value && value <= column_max,

            // If every value in the column is equal to "value" then this will
            // be false, otherwise it must be satisfied
            Operator::NotEqual => (column_min != column_max) || column_max != value,

            // if the column max is larger than value then the column could
            // contain the value.
            Operator::GT => column_max > value,

            // if the column max is at least as large as `value` then the column
            // could contain the value.
            Operator::GTE => column_max >= value,

            // if the column min is smaller than value then the column could
            // contain the value.
            Operator::LT => column_min < value,

            // if the column min is at least as small as value then the column
            // could contain the value.
            Operator::LTE => column_min <= value,
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::column::ValuesIterator;

    fn stringify_read_filter_results(results: Vec<(ColumnName<'_>, Values)>) -> String {
        let mut out = String::new();
        // header line.
        for (i, (k, _)) in results.iter().enumerate() {
            out.push_str(k);
            if i < results.len() - 1 {
                out.push(',');
            }
        }
        out.push('\n');

        // TODO: handle empty results?
        let expected_rows = results[0].1.len();
        let mut rows = 0;

        let mut iter_map = results
            .iter()
            .map(|(k, v)| (*k, ValuesIterator::new(v)))
            .collect::<BTreeMap<&str, ValuesIterator<'_>>>();

        while rows < expected_rows {
            if rows > 0 {
                out.push('\n');
            }

            for (i, (k, _)) in results.iter().enumerate() {
                if let Some(itr) = iter_map.get_mut(k) {
                    out.push_str(&format!("{}", itr.next().unwrap()));
                    if i < results.len() - 1 {
                        out.push(',');
                    }
                }
            }

            rows += 1;
        }

        out
    }

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
    fn read_filter() {
        let mut columns = BTreeMap::new();
        let tc = ColumnType::Time(Column::from(&[1_i64, 2, 3, 4, 5, 6][..]));
        columns.insert("time", &tc);

        let rc = ColumnType::Tag(Column::from(
            &["west", "west", "east", "west", "south", "north"][..],
        ));
        columns.insert("region", &rc);

        let mc = ColumnType::Tag(Column::from(
            &["GET", "POST", "POST", "POST", "PUT", "GET"][..],
        ));
        columns.insert("method", &mc);

        let fc = ColumnType::Field(Column::from(&[100_u64, 101, 200, 203, 203, 10][..]));
        columns.insert("count", &fc);

        let segment = Segment::new(6, columns);

        let results = segment.read_filter(
            &["count", "region", "time"],
            &build_predicates(1, 6, vec![]),
        );
        let expected = "count,region,time
100,west,1
101,west,2
200,east,3
203,west,4
203,south,5";
        assert_eq!(stringify_read_filter_results(results), expected);

        let results = segment.read_filter(
            &["time", "region", "method"],
            &build_predicates(-19, 2, vec![]),
        );
        let expected = "time,region,method
1,west,GET";
        assert_eq!(stringify_read_filter_results(results), expected);

        let results = segment.read_filter(
            &["method", "region", "time"],
            &build_predicates(-19, 1, vec![]),
        );
        let expected = "";
        assert!(results.is_empty());

        let results = segment.read_filter(&["time"], &build_predicates(0, 3, vec![]));
        let expected = "time
1
2";
        assert_eq!(stringify_read_filter_results(results), expected);

        let results = segment.read_filter(&["method"], &build_predicates(0, 3, vec![]));
        let expected = "method
GET
POST";
        assert_eq!(stringify_read_filter_results(results), expected);

        let results = segment.read_filter(
            &["count", "method", "time"],
            &build_predicates(
                0,
                6,
                vec![("method", (Operator::Equal, Value::String("POST")))],
            ),
        );
        let expected = "count,method,time
101,POST,2
200,POST,3
203,POST,4";
        assert_eq!(stringify_read_filter_results(results), expected);

        let results = segment.read_filter(
            &["region", "time"],
            &build_predicates(
                0,
                6,
                vec![("method", (Operator::Equal, Value::String("POST")))],
            ),
        );
        let expected = "region,time
west,2
east,3
west,4";
        assert_eq!(stringify_read_filter_results(results), expected);
    }

    #[test]
    fn segment_could_satisfy_predicate() {
        let mut columns = BTreeMap::new();
        let tc = ColumnType::Time(Column::from(&[1_i64, 2, 3, 4, 5, 6][..]));
        columns.insert("time", &tc);

        let rc = ColumnType::Tag(Column::from(
            &["west", "west", "east", "west", "south", "north"][..],
        ));
        columns.insert("region", &rc);

        let mc = ColumnType::Tag(Column::from(
            &["GET", "GET", "GET", "GET", "GET", "GET"][..],
        ));
        columns.insert("method", &mc);

        let segment = Segment::new(6, columns);

        let cases = vec![
            ("az", &(Operator::Equal, Value::String("west")), false), // no az column
            ("region", &(Operator::Equal, Value::String("west")), true), // region column does contain "west"
            ("region", &(Operator::Equal, Value::String("over")), true), // region column might contain "over"
            ("region", &(Operator::Equal, Value::String("abc")), false), // region column can't contain "abc"
            ("region", &(Operator::Equal, Value::String("zoo")), false), // region column can't contain "zoo"
            (
                "region",
                &(Operator::NotEqual, Value::String("hello")),
                true,
            ), // region column might not contain "hello"
            ("method", &(Operator::NotEqual, Value::String("GET")), false), // method must only contain "GET"
            ("region", &(Operator::GT, Value::String("abc")), true), // region column might contain something > "abc"
            ("region", &(Operator::GT, Value::String("north")), true), // region column might contain something > "north"
            ("region", &(Operator::GT, Value::String("west")), false), // region column can't contain something > "west"
            ("region", &(Operator::GTE, Value::String("abc")), true), // region column might contain something ≥ "abc"
            ("region", &(Operator::GTE, Value::String("east")), true), // region column might contain something ≥ "east"
            ("region", &(Operator::GTE, Value::String("west")), true), // region column might contain something ≥ "west"
            ("region", &(Operator::GTE, Value::String("zoo")), false), // region column can't contain something ≥ "zoo"
            ("region", &(Operator::LT, Value::String("foo")), true), // region column might contain something < "foo"
            ("region", &(Operator::LT, Value::String("north")), true), // region column might contain something < "north"
            ("region", &(Operator::LT, Value::String("south")), true), // region column might contain something < "south"
            ("region", &(Operator::LT, Value::String("east")), false), // region column can't contain something < "east"
            ("region", &(Operator::LT, Value::String("abc")), false), // region column can't contain something < "abc"
            ("region", &(Operator::LTE, Value::String("east")), true), // region column might contain something ≤ "east"
            ("region", &(Operator::LTE, Value::String("north")), true), // region column might contain something ≤ "north"
            ("region", &(Operator::LTE, Value::String("south")), true), // region column might contain something ≤ "south"
            ("region", &(Operator::LTE, Value::String("abc")), false), // region column can't contain something ≤ "abc"
        ];

        for (column_name, predicate, exp) in cases {
            assert_eq!(
                segment.column_could_satisfy_predicate(column_name, predicate),
                exp,
                "({:?}, {:?}) failed",
                column_name,
                predicate
            );
        }
    }
}
