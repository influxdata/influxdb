use std::collections::BTreeMap;

use crate::column::{Column, Value};

/// A Segment is an immutable horizontal section (segment) of a table. By
/// definition it has the same schema as all the other segments in the table.
/// Further, all the columns within the segment have the same number of logical
/// rows.
///
/// This implementation will pull over the bulk of the prototype segment store
/// crate.
pub struct Segment<'a> {
    meta: MetaData<'a>,

    columns: Vec<Column>,
}

impl<'a> Segment<'a> {
    pub fn new(columns: &[Column]) -> Self {
        // columns must contain a single Column::Time column.
        todo!()
    }

    /// The total size in bytes of the segment
    pub fn size(&self) -> u64 {
        todo!()
    }

    /// The number of rows in the segment (all columns have the same number of
    /// rows).
    pub fn rows(&self) -> u64 {
        todo!()
    }

    /// The ranges on each column in the segment
    pub fn column_ranges(&self) -> BTreeMap<String, (Value<'a>, Value<'a>)> {
        todo!()
    }

    /// The time range of the segment (of the time column).
    ///
    /// It could be None if the segment only contains NULL values in the time
    /// column.
    pub fn time_range(&self) -> Option<(i64, i64)> {
        todo!()
    }
}

// A GroupKey is an ordered collection of row values. The order determines which
// columns the values originated from.
pub type GroupKey = Vec<String>;

// A representation of a column name.
pub type ColumnName = String;

/// The logical type that a column could have.
pub enum ColumnType {
    Tag(Column),
    Field(Column),
    Time(Column),
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

struct MetaData<'a> {
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
    column_ranges: BTreeMap<String, (Value<'a>, Value<'a>)>,

    // The total time range of this table spanning all of the segments within
    // the table.
    //
    // This can be used to skip the table entirely if the time range for a query
    // falls outside of this range.
    time_range: Option<(i64, i64)>,
}
