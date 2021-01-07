#![deny(rust_2018_idioms)]
#![allow(dead_code)]
#![allow(clippy::too_many_arguments)]
#![allow(unused_variables)]
pub(crate) mod chunk;
pub mod column;
pub mod row_group;
pub(crate) mod table;

use std::{
    collections::{btree_map::Entry, BTreeMap, BTreeSet},
    fmt,
    sync::Arc,
};

use arrow_deps::arrow::{
    array::{ArrayRef, StringArray},
    datatypes::{DataType::Utf8, Field, Schema},
    record_batch::RecordBatch,
};
use snafu::{ResultExt, Snafu};

use chunk::Chunk;
use column::AggregateType;
pub use column::{FIELD_COLUMN_TYPE, TAG_COLUMN_TYPE, TIME_COLUMN_TYPE};
use row_group::{ColumnName, Predicate, RowGroup};
use table::Table;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("arrow conversion error: {}", source))]
    ArrowError {
        source: arrow_deps::arrow::error::ArrowError,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

// A database is scoped to a single tenant. Within a database there exists
// tables for measurements. There is a 1:1 mapping between a table and a
// measurement name.
#[derive(Default)]
pub struct Database {
    // The collection of chunks in the database. Each chunk is uniquely
    // identified by a chunk id.
    chunks: BTreeMap<u32, Chunk>,

    // The current total size of the database.
    size: u64,

    // Total number of rows in the database.
    rows: u64,
}

impl Database {
    pub fn new() -> Self {
        Self::default()
    }

    /// Add new table data for a chunk. If the `Chunk` does not exist it will be
    /// created.
    pub fn update_chunk(&mut self, chunk_id: u32, table_name: String, table_data: RecordBatch) {
        // validate table data contains appropriate meta data.
        let schema = table_data.schema();
        if schema.fields().len() != schema.metadata().len() {
            todo!("return error with missing column types for fields")
        }

        // ensure that a valid column type is specified for each column in
        // the record batch.
        for (col_name, col_type) in schema.metadata() {
            match col_type.as_str() {
                TAG_COLUMN_TYPE | FIELD_COLUMN_TYPE | TIME_COLUMN_TYPE => continue,
                _ => todo!("return error with incorrect column type specified"),
            }
        }

        let row_group = RowGroup::from(table_data);
        self.size += row_group.size();
        self.rows += row_group.rows() as u64;

        // create a new chunk if one doesn't exist, or add the table data to
        // the existing chunk.
        match self.chunks.entry(chunk_id) {
            Entry::Occupied(mut e) => {
                let chunk = e.get_mut();
                chunk.update_table(table_name, row_group);
            }
            Entry::Vacant(e) => {
                e.insert(Chunk::new(chunk_id, Table::new(table_name, row_group)));
            }
        };
    }

    pub fn remove_chunk(&mut self, chunk_id: u32) {
        todo!()
    }

    pub fn size(&self) -> u64 {
        self.size
    }

    pub fn rows(&self) -> u64 {
        self.rows
    }

    /// Determines the total number of tables under all chunks within the
    /// database.
    pub fn tables(&self) -> usize {
        self.chunks.values().map(|chunk| chunk.tables()).sum()
    }

    /// Determines the total number of row groups under all tables under all
    /// chunks, within the database.
    pub fn row_groups(&self) -> usize {
        self.chunks.values().map(|chunk| chunk.row_groups()).sum()
    }

    /// Executes selections against matching chunks, returning a single
    /// record batch with all chunk results appended.
    ///
    /// Results may be filtered by (currently only) equality predicates, but can
    /// be ranged by time, which should be represented as nanoseconds since the
    /// epoch. Results are included if they satisfy the predicate and fall
    /// with the [min, max) time range domain.
    pub fn select(
        &self,
        table_name: &str,
        time_range: (i64, i64),
        predicates: &[Predicate<'_>],
        select_columns: Vec<String>,
    ) -> Option<RecordBatch> {
        // Find all matching chunks using:
        //   - time range
        //   - measurement name.
        //
        // Execute against each chunk and append each result set into a
        // single record batch.
        todo!();
    }

    /// Returns aggregates segmented by grouping keys for the specified
    /// measurement as record batches, with one record batch per matching
    /// chunk.
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
        predicates: &[Predicate<'_>],
        group_columns: Vec<String>,
        aggregates: Vec<(ColumnName<'_>, AggregateType)>,
    ) -> Option<RecordBatch> {
        // Find all matching chunks using:
        //   - time range
        //   - measurement name.
        //
        // Execute query against each matching chunk and get result set.
        // For each result set it may be possible for there to be duplicate
        // group keys, e.g., due to back-filling. So chunk results may need
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
        predicates: &[Predicate<'_>],
        group_columns: Vec<String>,
        aggregates: Vec<(ColumnName<'_>, AggregateType)>,
        window: i64,
    ) -> Option<RecordBatch> {
        // Find all matching chunks using:
        //   - time range
        //   - measurement name.
        //
        // Execute query against each matching chunk and get result set.
        // For each result set it may be possible for there to be duplicate
        // group keys, e.g., due to back-filling. So chunk results may need
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
    ///
    /// TODO(edd): Implement predicate support.
    pub fn table_names(&self, predicates: &[Predicate<'_>]) -> Result<Option<RecordBatch>> {
        let mut intersection = BTreeSet::new();
        let chunk_table_names = self
            .chunks
            .values()
            .map(|chunk| chunk.table_names(predicates))
            .for_each(|mut names| intersection.append(&mut names));

        if intersection.is_empty() {
            return Ok(None);
        }

        let schema = Schema::new(vec![Field::new("table", Utf8, false)]);
        let columns: Vec<ArrayRef> = vec![Arc::new(StringArray::from(
            intersection
                .iter()
                .map(|s| s.as_str())
                .collect::<Vec<&str>>(),
        ))];

        match RecordBatch::try_new(Arc::new(schema), columns).context(ArrowError {}) {
            Ok(rb) => Ok(Some(rb)),
            Err(e) => Err(e),
        }
    }

    /// Returns the distinct set of tag keys (column names) matching the
    /// provided optional predicates and time range.
    pub fn tag_keys(
        &self,
        table_name: &str,
        time_range: (i64, i64),
        predicates: &[Predicate<'_>],
    ) -> Option<RecordBatch> {
        // Find all matching chunks using:
        //   - time range
        //   - measurement name.
        //
        // Execute query against matching chunks. The `tag_keys` method for
        // a chunk allows the caller to provide already found tag keys
        // (column names). This allows the execution to skip entire chunks,
        // tables or segments if there are no new columns to be found there...
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
        table_name: &str,
        time_range: (i64, i64),
        predicates: &[Predicate<'_>],
        tag_keys: &[String],
    ) -> Option<RecordBatch> {
        // Find the measurement name on the chunk and dispatch query to the
        // table for that measurement if the chunk's time range overlaps the
        // requested time range.
        todo!();
    }
}

impl fmt::Debug for Database {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Database")
            .field("chunks", &self.chunks.keys())
            .field("size", &self.size)
            .finish()
    }
}

/// Generate a predicate for the time range [from, to).
pub fn time_range_predicate<'a>(from: i64, to: i64) -> Vec<row_group::Predicate<'a>> {
    vec![
        (
            row_group::TIME_COLUMN_NAME,
            (
                column::cmp::Operator::GTE,
                column::Value::Scalar(column::Scalar::I64(from)),
            ),
        ),
        (
            row_group::TIME_COLUMN_NAME,
            (
                column::cmp::Operator::LT,
                column::Value::Scalar(column::Scalar::I64(to)),
            ),
        ),
    ]
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;
    use std::sync::Arc;

    use arrow_deps::arrow::{
        array::{ArrayRef, Float64Array, Int64Array, StringArray},
        datatypes::{
            DataType::{Float64, Int64, Utf8},
            Field, Schema,
        },
    };

    use super::*;

    // helper to make the `database_update_chunk` test simpler to read.
    fn gen_recordbatch() -> RecordBatch {
        let metadata = vec![
            ("region".to_owned(), TAG_COLUMN_TYPE.to_owned()),
            ("counter".to_owned(), FIELD_COLUMN_TYPE.to_owned()),
            (
                row_group::TIME_COLUMN_NAME.to_owned(),
                TIME_COLUMN_TYPE.to_owned(),
            ),
        ]
        .into_iter()
        .collect::<HashMap<String, String>>();

        let schema = Schema::new_with_metadata(
            vec![
                ("region", Utf8),
                ("counter", Float64),
                (row_group::TIME_COLUMN_NAME, Int64),
            ]
            .into_iter()
            .map(|(name, typ)| Field::new(name, typ, false))
            .collect(),
            metadata,
        );

        let data: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(vec!["west", "west", "east"])),
            Arc::new(Float64Array::from(vec![1.2, 3.3, 45.3])),
            Arc::new(Int64Array::from(vec![11111111, 222222, 3333])),
        ];

        RecordBatch::try_new(Arc::new(schema), data).unwrap()
    }

    #[test]
    fn database_update_chunk() {
        let mut db = Database::new();
        db.update_chunk(22, "a_table".to_owned(), gen_recordbatch());

        assert_eq!(db.rows(), 3);
        assert_eq!(db.tables(), 1);
        assert_eq!(db.row_groups(), 1);

        let chunk = db.chunks.values().next().unwrap();
        assert_eq!(chunk.tables(), 1);
        assert_eq!(chunk.rows(), 3);
        assert_eq!(chunk.row_groups(), 1);

        // Updating the chunk with another row group for the table just adds
        // that row group to the existing table.
        db.update_chunk(22, "a_table".to_owned(), gen_recordbatch());
        assert_eq!(db.rows(), 6);
        assert_eq!(db.tables(), 1); // still one table
        assert_eq!(db.row_groups(), 2);

        let chunk = db.chunks.values().next().unwrap();
        assert_eq!(chunk.tables(), 1); // it's the same table.
        assert_eq!(chunk.rows(), 6);
        assert_eq!(chunk.row_groups(), 2);

        // Adding the same data under another table would increase the table
        // count.
        db.update_chunk(22, "b_table".to_owned(), gen_recordbatch());
        assert_eq!(db.rows(), 9);
        assert_eq!(db.tables(), 2);
        assert_eq!(db.row_groups(), 3);

        let chunk = db.chunks.values().next().unwrap();
        assert_eq!(chunk.tables(), 2);
        assert_eq!(chunk.rows(), 9);
        assert_eq!(chunk.row_groups(), 3);

        // Adding the data under another chunk adds a new chunk.
        db.update_chunk(29, "a_table".to_owned(), gen_recordbatch());
        assert_eq!(db.rows(), 12);
        assert_eq!(db.tables(), 3); // two distinct tables but across two chunks.
        assert_eq!(db.row_groups(), 4);

        let chunk = db.chunks.values().next().unwrap();
        assert_eq!(chunk.tables(), 2);
        assert_eq!(chunk.rows(), 9);
        assert_eq!(chunk.row_groups(), 3);

        let chunk = db.chunks.values().nth(1).unwrap();
        assert_eq!(chunk.tables(), 1);
        assert_eq!(chunk.rows(), 3);
        assert_eq!(chunk.row_groups(), 1);
    }

    // Helper function to assert the contents of a column on a record batch.
    fn assert_rb_column_equals(rb: &RecordBatch, col_name: &str, exp: &column::Values<'_>) {
        let got_column = rb.column(rb.schema().index_of(col_name).unwrap());

        match exp {
            column::Values::String(exp_data) => {
                let arr: &StringArray = got_column.as_any().downcast_ref::<StringArray>().unwrap();
                assert_eq!(&arr.iter().collect::<Vec<_>>(), exp_data);
            }
            column::Values::I64(exp_data) => {
                let arr: &Int64Array = got_column.as_any().downcast_ref::<Int64Array>().unwrap();
                assert_eq!(arr.values(), exp_data);
            }
            column::Values::U64(_) => {}
            column::Values::F64(_) => {}
            column::Values::I64N(_) => {}
            column::Values::U64N(_) => {}
            column::Values::F64N(_) => {}
            column::Values::Bool(_) => {}
            column::Values::ByteArray(_) => {}
        }
    }

    #[test]
    fn table_names() {
        let mut db = Database::new();
        assert!(matches!(db.table_names(&[]), Ok(None)));

        db.update_chunk(22, "Coolverine".to_owned(), gen_recordbatch());
        let data = db.table_names(&[]).unwrap().unwrap();
        assert_rb_column_equals(
            &data,
            "table",
            &column::Values::String(vec![Some("Coolverine")]),
        );

        db.update_chunk(22, "Coolverine".to_owned(), gen_recordbatch());
        let data = db.table_names(&[]).unwrap().unwrap();
        assert_rb_column_equals(
            &data,
            "table",
            &column::Values::String(vec![Some("Coolverine")]),
        );

        db.update_chunk(2, "Coolverine".to_owned(), gen_recordbatch());
        let data = db.table_names(&[]).unwrap().unwrap();
        assert_rb_column_equals(
            &data,
            "table",
            &column::Values::String(vec![Some("Coolverine")]),
        );

        db.update_chunk(2, "20 Size".to_owned(), gen_recordbatch());
        let data = db.table_names(&[]).unwrap().unwrap();
        assert_rb_column_equals(
            &data,
            "table",
            &column::Values::String(vec![Some("20 Size"), Some("Coolverine")]),
        );
    }
}
