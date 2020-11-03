use std::collections::BTreeMap;

use delorean_arrow::arrow::datatypes::SchemaRef;

use crate::column::{Column, Scalar, Value};

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

    column_names: Vec<String>,

    tag_columns: Vec<&'a Column>,
    field_columns: Vec<&'a Column>,
    time_column: &'a Column,
}

impl<'a> Segment<'a> {
    pub fn new(schema: Schema, rows: u32, columns: BTreeMap<String, &'a ColumnType>) -> Self {
        assert_eq!(schema.cols(), columns.len());

        let mut meta = MetaData::default();
        meta.rows = rows;

        let mut tag_columns: Vec<&'a Column> = vec![];
        let mut field_columns: Vec<&'a Column> = vec![];
        let mut time_column: Option<&'a Column> = None;
        let mut column_names = Vec::with_capacity(columns.len());

        for (name, ct) in columns {
            match ct {
                ColumnType::Tag(c) => {
                    assert_eq!(c.num_rows(), rows);

                    tag_columns.push(&c);

                    if let Some(range) = tag_columns.last().unwrap().column_range() {
                        meta.column_ranges.insert(name.clone(), range);
                    }
                }
                ColumnType::Field(c) => {
                    assert_eq!(c.num_rows(), rows);

                    field_columns.push(&c);

                    if let Some(range) = c.column_range() {
                        meta.column_ranges.insert(name.clone(), range);
                    }
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

                    meta.column_ranges.insert(name.clone(), range.unwrap());
                    match time_column {
                        Some(_) => panic!("multiple time columns unsupported"),
                        None => {
                            // probably not a firm requirement....
                            assert_eq!(name.clone(), "time".to_string());
                            time_column = Some(&c);
                        }
                    }
                }
            }
            column_names.push(name);
        }

        Self {
            meta,
            column_names,
            tag_columns,
            field_columns,
            time_column: time_column.unwrap(),
        }
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

#[derive(Default)]
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
    column_ranges: BTreeMap<String, (Value<'a>, Value<'a>)>,

    // The total time range of this table spanning all of the segments within
    // the table.
    //
    // This can be used to skip the table entirely if the time range for a query
    // falls outside of this range.
    time_range: (i64, i64),
}
