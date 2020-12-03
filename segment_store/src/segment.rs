use std::{borrow::Cow, collections::BTreeMap};

use arrow_deps::arrow::datatypes::SchemaRef;

use crate::column::{
    cmp::Operator, Column, OwnedValue, RowIDs, RowIDsOption, Scalar, Value, Values, ValuesIterator,
};

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
pub struct Segment {
    meta: MetaData,

    columns: Vec<Column>,
    all_columns_by_name: BTreeMap<String, usize>,
    tag_columns_by_name: BTreeMap<String, usize>,
    field_columns_by_name: BTreeMap<String, usize>,
    time_column: usize,
}

impl Segment {
    pub fn new(rows: u32, columns: BTreeMap<String, ColumnType>) -> Self {
        let mut meta = MetaData {
            rows,
            ..MetaData::default()
        };

        let mut segment_columns = vec![];
        let mut all_columns_by_name = BTreeMap::new();
        let mut tag_columns_by_name = BTreeMap::new();
        let mut field_columns_by_name = BTreeMap::new();
        let mut time_column = None;

        for (name, ct) in columns {
            meta.size += ct.size();
            match ct {
                ColumnType::Tag(c) => {
                    assert_eq!(c.num_rows(), rows);

                    meta.column_ranges
                        .insert(name.clone(), c.column_range().unwrap());
                    all_columns_by_name.insert(name.clone(), segment_columns.len());
                    tag_columns_by_name.insert(name, segment_columns.len());
                    segment_columns.push(c);
                }
                ColumnType::Field(c) => {
                    assert_eq!(c.num_rows(), rows);

                    meta.column_ranges
                        .insert(name.clone(), c.column_range().unwrap());
                    all_columns_by_name.insert(name.clone(), segment_columns.len());
                    field_columns_by_name.insert(name, segment_columns.len());
                    segment_columns.push(c);
                }
                ColumnType::Time(c) => {
                    assert_eq!(c.num_rows(), rows);

                    meta.time_range = match c.column_range() {
                        None => panic!("time column must have non-null value"),
                        Some((
                            OwnedValue::Scalar(Scalar::I64(min)),
                            OwnedValue::Scalar(Scalar::I64(max)),
                        )) => (min, max),
                        Some((_, _)) => unreachable!("unexpected types for time range"),
                    };

                    meta.column_ranges
                        .insert(name.clone(), c.column_range().unwrap());

                    all_columns_by_name.insert(name.clone(), segment_columns.len());
                    time_column = Some(segment_columns.len());
                    segment_columns.push(c);
                }
            }
        }

        Self {
            meta,
            columns: segment_columns,
            all_columns_by_name,
            tag_columns_by_name,
            field_columns_by_name,
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
    pub fn column_ranges(&self) -> &BTreeMap<String, (OwnedValue, OwnedValue)> {
        &self.meta.column_ranges
    }

    // Returns a reference to a column from the column name.
    fn column_by_name(&self, name: ColumnName<'_>) -> Option<&Column> {
        match self.all_columns_by_name.get(name) {
            Some(&idx) => Some(&self.columns[idx]),
            None => None,
        }
    }

    // Returns a reference to the timestamp column.
    fn time_column(&self) -> &Column {
        &self.columns[self.time_column]
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
    pub fn read_filter<'a>(
        &self,
        columns: &[ColumnName<'a>],
        predicates: &[Predicate<'_>],
    ) -> ReadFilterResult<'a> {
        let row_ids = self.row_ids_from_predicates(predicates);
        ReadFilterResult(self.materialise_rows(columns, row_ids))
    }

    fn materialise_rows<'a>(
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
                for &col_name in columns {
                    let col = self.column_by_name(col_name).unwrap();
                    results.push((col_name, col.values(row_ids.as_slice())));
                }
                results
            }

            RowIDsOption::All(_) => {
                // TODO(edd): Perf - add specialised method to get all
                // materialised values from a column without having to
                // materialise a vector of row ids.......
                let row_ids = (0..self.rows()).collect::<Vec<_>>();

                for &col_name in columns {
                    let col = self.column_by_name(col_name).unwrap();
                    results.push((col_name, col.values(row_ids.as_slice())));
                }
                results
            }
        }
    }

    // Determines the set of row ids that satisfy the provided predicates.
    // If `predicates` contains two predicates on the time column they are
    // special-cased.
    fn row_ids_from_predicates(&self, predicates: &[Predicate<'_>]) -> RowIDsOption {
        // TODO(edd): perf - potentially pool this so we can re-use it once
        // rows have been materialised and it's no longer needed.
        // Initialise a bitmap RowIDs because it's like that set operations will
        // be necessary.
        let mut result_row_ids = RowIDs::new_bitmap();

        // TODO(edd): perf - pool the dst buffer so we can re-use it across
        // subsequent calls to `row_ids_from_predicates`.
        // Right now this buffer will be re-used across all columns in the
        // segment but not re-used for subsequent calls _to_ the segment.
        let mut dst = RowIDs::new_bitmap();

        let mut predicates = Cow::Borrowed(predicates);
        // If there is a time-range in the predicates (two time predicates), then
        // execute an optimised version that will use a range based predicate
        // on the time column.
        if predicates
            .iter()
            .filter(|(col, _)| *col == TIME_COLUMN_NAME)
            .count()
            // Check if we have two predicates on the time column, i.e., a time range.
            == 2
        {
            // Apply optimised filtering to time column
            let time_pred_row_ids =
                self.row_ids_from_predicates_with_time_range(predicates.as_ref(), dst);
            match time_pred_row_ids {
                // No matching rows based on time range
                RowIDsOption::None(_) => return time_pred_row_ids,

                // all rows match - continue to apply other predicates
                RowIDsOption::All(_dst) => {
                    dst = _dst; // hand buffer back
                }

                // some rows match - continue to apply predicates
                RowIDsOption::Some(row_ids) => {
                    // fill the result row id set with the matching rows from the
                    // time column.
                    result_row_ids.union(&row_ids);
                    dst = row_ids // hand buffer back
                }
            }

            // remove time predicates so they're not processed again
            let mut filtered_predicates = predicates.to_vec();
            filtered_predicates.retain(|(col, _)| *col != TIME_COLUMN_NAME);
            predicates = Cow::Owned(filtered_predicates);
        }

        for (col_name, (op, value)) in predicates.iter() {
            // N.B column should always exist because validation of
            // predicates is not the responsibility of the `Segment`.
            let col = self.column_by_name(col_name).unwrap();

            // Explanation of how this buffer pattern works. The idea is
            // that the buffer should be returned to the caller so it can be
            // re-used on other columns. Each call to `row_ids_filter` returns
            // the buffer back enabling it to be re-used.
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
            // All rows matched all predicates because any predictates not matching
            // any rows would have resulted in an early return.
            return RowIDsOption::All(result_row_ids);
        }
        RowIDsOption::Some(result_row_ids)
    }

    // An optimised function for applying two comparison predicates to a time
    // column at once.
    fn row_ids_from_predicates_with_time_range(
        &self,
        predicates: &[Predicate<'_>],
        dst: RowIDs,
    ) -> RowIDsOption {
        // find the time range predicates and execute a specialised range based
        // row id lookup.
        let time_predicates = predicates
            .iter()
            .filter(|(col_name, _)| col_name == &TIME_COLUMN_NAME)
            .collect::<Vec<_>>();
        assert!(time_predicates.len() == 2);

        self.time_column().row_ids_filter_range(
            &time_predicates[0].1, // min time
            &time_predicates[1].1, // max time
            dst,
        )
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
struct MetaData {
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
    column_ranges: BTreeMap<String, (OwnedValue, OwnedValue)>,

    // The total time range of this table spanning all of the segments within
    // the table.
    //
    // This can be used to skip the table entirely if the time range for a query
    // falls outside of this range.
    time_range: (i64, i64),
}

impl MetaData {
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
            Operator::Equal => column_min <= value && column_max >= value,

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

/// Encapsulates results from segments with a structure that makes them easier
/// to work with and display.
pub struct ReadFilterResult<'a>(pub Vec<(ColumnName<'a>, Values)>);

impl<'a> ReadFilterResult<'a> {
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl<'a> std::fmt::Debug for &ReadFilterResult<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // header line.
        for (i, (k, _)) in self.0.iter().enumerate() {
            write!(f, "{}", k)?;

            if i < self.0.len() - 1 {
                write!(f, ",")?;
            }
        }
        writeln!(f)?;

        // Display the rest of the values.
        std::fmt::Display::fmt(&self, f)
    }
}

impl<'a> std::fmt::Display for &ReadFilterResult<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.is_empty() {
            return Ok(());
        }

        let expected_rows = self.0[0].1.len();
        let mut rows = 0;

        let mut iter_map = self
            .0
            .iter()
            .map(|(k, v)| (*k, ValuesIterator::new(v)))
            .collect::<BTreeMap<&str, ValuesIterator<'_>>>();

        while rows < expected_rows {
            if rows > 0 {
                writeln!(f)?;
            }

            for (i, (k, _)) in self.0.iter().enumerate() {
                if let Some(itr) = iter_map.get_mut(k) {
                    write!(f, "{}", itr.next().unwrap())?;
                    if i < self.0.len() - 1 {
                        write!(f, ",")?;
                    }
                }
            }

            rows += 1;
        }
        writeln!(f)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    fn build_predicates_with_time(
        from: i64,
        to: i64,
        others: Vec<Predicate<'_>>,
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

        arr.extend(others);
        arr
    }

    #[test]
    fn row_ids_from_predicates() {
        let mut columns = BTreeMap::new();
        let tc = ColumnType::Time(Column::from(&[100_i64, 200, 500, 600, 300, 300][..]));
        columns.insert("time".to_string(), tc);
        let rc = ColumnType::Tag(Column::from(
            &["west", "west", "east", "west", "south", "north"][..],
        ));
        columns.insert("region".to_string(), rc);
        let segment = Segment::new(6, columns);

        // Closed partially covering "time range" predicate
        let row_ids =
            segment.row_ids_from_predicates(&build_predicates_with_time(200, 600, vec![]));
        assert_eq!(row_ids.unwrap().to_vec(), vec![1, 2, 4, 5]);

        // Fully covering "time range" predicate
        let row_ids = segment.row_ids_from_predicates(&build_predicates_with_time(10, 601, vec![]));
        assert!(matches!(row_ids, RowIDsOption::All(_)));

        // Open ended "time range" predicate
        let row_ids = segment.row_ids_from_predicates(&[(
            TIME_COLUMN_NAME,
            (Operator::GTE, Value::Scalar(Scalar::I64(300))),
        )]);
        assert_eq!(row_ids.unwrap().to_vec(), vec![2, 3, 4, 5]);

        // Closed partially covering "time range" predicate and other column predicate
        let row_ids = segment.row_ids_from_predicates(&build_predicates_with_time(
            200,
            600,
            vec![("region", (Operator::Equal, Value::String("south")))],
        ));
        assert_eq!(row_ids.unwrap().to_vec(), vec![4]);

        // Fully covering "time range" predicate and other column predicate
        let row_ids = segment.row_ids_from_predicates(&build_predicates_with_time(
            10,
            601,
            vec![("region", (Operator::Equal, Value::String("west")))],
        ));
        assert_eq!(row_ids.unwrap().to_vec(), vec![0, 1, 3]);

        // "time range" predicate and other column predicate that doesn't match
        let row_ids = segment.row_ids_from_predicates(&build_predicates_with_time(
            200,
            600,
            vec![("region", (Operator::Equal, Value::String("nope")))],
        ));
        assert!(matches!(row_ids, RowIDsOption::None(_)));

        // Just a column predicate
        let row_ids = segment
            .row_ids_from_predicates(&[("region", (Operator::Equal, Value::String("east")))]);
        assert_eq!(row_ids.unwrap().to_vec(), vec![2]);

        // Predicate can matches all the rows
        let row_ids = segment
            .row_ids_from_predicates(&[("region", (Operator::NotEqual, Value::String("abba")))]);
        assert!(matches!(row_ids, RowIDsOption::All(_)));

        // No predicates
        let row_ids = segment.row_ids_from_predicates(&[]);
        assert!(matches!(row_ids, RowIDsOption::All(_)));
    }

    #[test]
    fn read_filter() {
        let mut columns = BTreeMap::new();
        let tc = ColumnType::Time(Column::from(&[1_i64, 2, 3, 4, 5, 6][..]));
        columns.insert("time".to_string(), tc);

        let rc = ColumnType::Tag(Column::from(
            &["west", "west", "east", "west", "south", "north"][..],
        ));
        columns.insert("region".to_string(), rc);

        let mc = ColumnType::Tag(Column::from(
            &["GET", "POST", "POST", "POST", "PUT", "GET"][..],
        ));
        columns.insert("method".to_string(), mc);

        let fc = ColumnType::Field(Column::from(&[100_u64, 101, 200, 203, 203, 10][..]));
        columns.insert("count".to_string(), fc);

        let segment = Segment::new(6, columns);

        let cases = vec![
            (
                vec!["count", "region", "time"],
                build_predicates_with_time(1, 6, vec![]),
                "count,region,time
100,west,1
101,west,2
200,east,3
203,west,4
203,south,5
",
            ),
            (
                vec!["time", "region", "method"],
                build_predicates_with_time(-19, 2, vec![]),
                "time,region,method
1,west,GET
",
            ),
            (
                vec!["time"],
                build_predicates_with_time(0, 3, vec![]),
                "time
1
2
",
            ),
            (
                vec!["method"],
                build_predicates_with_time(0, 3, vec![]),
                "method
GET
POST
",
            ),
            (
                vec!["count", "method", "time"],
                build_predicates_with_time(
                    0,
                    6,
                    vec![("method", (Operator::Equal, Value::String("POST")))],
                ),
                "count,method,time
101,POST,2
200,POST,3
203,POST,4
",
            ),
            (
                vec!["region", "time"],
                build_predicates_with_time(
                    0,
                    6,
                    vec![("method", (Operator::Equal, Value::String("POST")))],
                ),
                "region,time
west,2
east,3
west,4
",
            ),
        ];

        for (cols, predicates, expected) in cases {
            let results = segment.read_filter(&cols, &predicates);
            assert_eq!(format!("{:?}", &results), expected);
        }

        // test no matching rows
        let results = segment.read_filter(
            &["method", "region", "time"],
            &build_predicates_with_time(-19, 1, vec![]),
        );
        let expected = "";
        assert!(results.is_empty());
    }

    #[test]
    fn segment_could_satisfy_predicate() {
        let mut columns = BTreeMap::new();
        let tc = ColumnType::Time(Column::from(&[1_i64, 2, 3, 4, 5, 6][..]));
        columns.insert("time".to_string(), tc);

        let rc = ColumnType::Tag(Column::from(
            &["west", "west", "east", "west", "south", "north"][..],
        ));
        columns.insert("region".to_string(), rc);

        let mc = ColumnType::Tag(Column::from(
            &["GET", "GET", "GET", "GET", "GET", "GET"][..],
        ));
        columns.insert("method".to_string(), mc);

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
