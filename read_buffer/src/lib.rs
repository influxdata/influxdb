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

    #[snafu(display("partition key does not exist: {}", key))]
    PartitionNotFound { key: String },

    #[snafu(display("chunk id does not exist: {}", id))]
    ChunkNotFound { id: u32 },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

// A database is scoped to a single tenant. Within a database there exists
// partitions, chunks, tables and row groups.
#[derive(Default)]
pub struct Database {
    // The collection of partitions for the database. Each partition is uniquely
    // identified by a partition key
    partitions: BTreeMap<String, Partition>,

    // The current total size of the database.
    size: u64,

    // Total number of rows in the database.
    rows: u64,
}

impl Database {
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds new data for a chunk.
    ///
    /// Data should be provided as a single row group for a table within the
    /// chunk. If the `Table` or `Chunk` does not exist they will be created,
    /// otherwise relevant structures will be updated.
    pub fn upsert_partition(
        &mut self,
        partition_key: &str,
        chunk_id: u32,
        table_name: &str,
        table_data: RecordBatch,
    ) {
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
        match self.partitions.entry(partition_key.to_owned()) {
            Entry::Occupied(mut e) => {
                let partition = e.get_mut();
                partition.upsert_chunk(chunk_id, table_name.to_owned(), row_group);
            }
            Entry::Vacant(e) => {
                e.insert(Partition::new(
                    partition_key,
                    Chunk::new(chunk_id, Table::new(table_name.to_owned(), row_group)),
                ));
            }
        };
    }

    /// Remove all row groups, tables and chunks within the specified partition
    /// key.
    pub fn drop_partition(&mut self, partition_key: &str) -> Result<()> {
        if self.partitions.remove(partition_key).is_some() {
            return Ok(());
        }

        Err(Error::PartitionNotFound {
            key: partition_key.to_owned(),
        })
    }

    /// Remove all row groups and tables for the specified chunks and partition.
    pub fn drop_chunk(&mut self, partition_key: &str, chunk_id: u32) -> Result<()> {
        let partition = self
            .partitions
            .get_mut(partition_key)
            .ok_or(Error::PartitionNotFound {
                key: partition_key.to_owned(),
            })?;

        if partition.chunks.remove(&chunk_id).is_some() {
            return Ok(());
        }

        Err(Error::ChunkNotFound { id: chunk_id })
    }

    // Lists all partition keys with data for this database.
    pub fn partition_keys(&mut self) -> Vec<&String> {
        self.partitions.keys().collect::<Vec<_>>()
    }

    pub fn size(&self) -> u64 {
        self.size
    }

    pub fn rows(&self) -> u64 {
        self.rows
    }

    /// Determines the total number of tables under all partitions within the
    /// database.
    pub fn tables(&self) -> usize {
        self.partitions
            .values()
            .map(|partition| partition.tables())
            .sum()
    }

    /// Determines the total number of row groups under all tables under all
    /// chunks, within the database.
    pub fn row_groups(&self) -> usize {
        self.partitions
            .values()
            .map(|chunk| chunk.row_groups())
            .sum()
    }

    /// Returns rows for the specified columns in the provided table, for the
    /// specified partition key and chunks within that partition.
    ///
    /// Results may be filtered by conjunctive predicates.
    /// Whilst the `ReadBuffer` will carry out the most optimal execution
    /// possible by pruning columns, row groups and tables, it is assumed
    /// that the caller has already provided an appropriately pruned
    /// collection of chunks.
    ///
    /// `read_filter` return an iterator that will emit record batches for all
    /// row groups help under the provided chunks.
    ///
    /// `read_filter` is lazy - it does not execute against the next chunk until
    /// the results for the previous one have been emitted.
    pub fn read_filter<'a>(
        &self,
        partition_key: &str,
        table_name: &'a str,
        chunk_ids: &[u32],
        predicates: &'a [Predicate<'a>],
        select_columns: table::ColumnSelection<'a>,
    ) -> Result<ReadFilterResults<'a, '_>> {
        match self.partitions.get(partition_key) {
            Some(partition) => {
                let mut chunks = vec![];
                for chunk_id in chunk_ids {
                    chunks.push(
                        partition
                            .chunks
                            .get(chunk_id)
                            .ok_or_else(|| Error::ChunkNotFound { id: *chunk_id })?,
                    )
                }

                // TODO(edd): encapsulate execution of `read_filter` on each chunk
                // into an anonymous function, rather than having to store all
                // the input context arguments in the iterator state.
                Ok(ReadFilterResults::new(
                    chunks,
                    table_name,
                    predicates,
                    select_columns,
                ))
            }
            None => Err(Error::PartitionNotFound {
                key: partition_key.to_owned(),
            }),
        }
    }

    /// Returns aggregates for each group specified by the values of the
    /// grouping keys, limited to the specified partition key table name and
    /// chunk ids.
    ///
    /// Results may be filtered by conjunctive predicates.
    /// Whilst the `ReadBuffer` will carry out the most optimal execution
    /// possible by pruning columns, row groups and tables, it is assumed
    /// that the caller has already provided an appropriately pruned
    /// collection of chunks.
    ///
    /// Currently, only grouping by string (tag key) columns is supported.
    /// Required aggregates are specified via a tuple comprising a column name
    /// and the type of aggregation required. Multiple aggregations can be
    /// applied to the same column.
    ///
    /// Because identical groups may exist across chunks `read_aggregate` will
    /// first merge across the provided chunks before returning an iterator of
    /// record batches, which will be lazily constructed.
    pub fn read_aggregate(
        &self,
        partition_key: &str,
        table_name: &str,
        chunk_ids: &[u32],
        predicates: &[Predicate<'_>],
        group_columns: Vec<String>,
        aggregates: Vec<(ColumnName<'_>, AggregateType)>,
    ) -> Result<()> {
        // TODO - return type for iterator

        match self.partitions.get(partition_key) {
            Some(partition) => {
                let mut chunks = vec![];
                for chunk_id in chunk_ids {
                    chunks.push(
                        partition
                            .chunks
                            .get(chunk_id)
                            .ok_or_else(|| Error::ChunkNotFound { id: *chunk_id })?,
                    );
                }

                // Execute query against each chunk and get results.
                // For each result set it may be possible for there to be
                // duplicate group keys, e.g., due to
                // back-filling. So chunk results may need to be
                // merged together with the aggregates from identical group keys
                // being resolved.

                // Merge these results and then return an iterator that lets the
                // caller stream through record batches. The number of record
                // batches is an implementation detail of the `ReadBuffer`.
                Ok(())
            }
            None => Err(Error::PartitionNotFound {
                key: partition_key.to_owned(),
            }),
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
    pub fn table_names(
        &self,
        partition_key: &str,
        chunk_ids: &[u32],
        predicates: &[Predicate<'_>],
    ) -> Result<Option<RecordBatch>> {
        let partition = self
            .partitions
            .get(partition_key)
            .ok_or(Error::PartitionNotFound {
                key: partition_key.to_owned(),
            })?;

        let mut intersection = BTreeSet::new();
        let chunk_table_names = partition
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
            .field("partitions", &self.partitions.keys())
            .field("size", &self.size)
            .finish()
    }
}

// A partition is a collection of `Chunks`.
#[derive(Default)]
pub struct Partition {
    // The partition's key
    key: String,

    // The collection of chunks in the partition. Each chunk is uniquely
    // identified by a chunk id.
    chunks: BTreeMap<u32, Chunk>,

    // The current total size of the partition.
    size: u64,

    // Total number of rows in the partition.
    rows: u64,
}

impl Partition {
    pub fn new(partition_key: &str, chunk: Chunk) -> Self {
        let mut p = Self {
            key: partition_key.to_owned(),
            size: chunk.size(),
            rows: chunk.rows(),
            chunks: BTreeMap::new(),
        };
        p.chunks.insert(chunk.id(), chunk);
        p
    }

    /// Adds new data for a chunk.
    ///
    /// Data should be provided as a single row group for a table within the
    /// chunk. If the `Table` or `Chunk` does not exist they will be created,
    /// otherwise relevant structures will be updated.
    fn upsert_chunk(&mut self, chunk_id: u32, table_name: String, row_group: RowGroup) {
        self.size += row_group.size();
        self.rows += row_group.rows() as u64;

        // create a new chunk if one doesn't exist, or add the table data to
        // the existing chunk.
        match self.chunks.entry(chunk_id) {
            Entry::Occupied(mut e) => {
                let chunk = e.get_mut();
                chunk.upsert_table(table_name, row_group);
            }
            Entry::Vacant(e) => {
                e.insert(Chunk::new(chunk_id, Table::new(table_name, row_group)));
            }
        };
    }

    /// Determines the total number of tables under all chunks within the
    /// partition.
    pub fn tables(&self) -> usize {
        self.chunks.values().map(|chunk| chunk.tables()).sum()
    }

    /// Determines the total number of row groups under all tables under all
    /// chunks, within the partition.
    pub fn row_groups(&self) -> usize {
        self.chunks.values().map(|chunk| chunk.row_groups()).sum()
    }

    pub fn rows(&self) -> u64 {
        self.rows
    }
}

/// ReadFilterResults implements ...
pub struct ReadFilterResults<'input, 'chunk> {
    chunks: Vec<&'chunk Chunk>,
    next_i: usize,
    curr_table_results: Option<table::ReadFilterResults<'input, 'chunk>>,

    table_name: &'input str,
    predicates: &'input [Predicate<'input>],
    select_columns: table::ColumnSelection<'input>,
}

impl<'input, 'chunk> ReadFilterResults<'input, 'chunk> {
    fn new(
        chunks: Vec<&'chunk Chunk>,
        table_name: &'input str,
        predicates: &'input [Predicate<'input>],
        select_columns: table::ColumnSelection<'input>,
    ) -> Self {
        Self {
            chunks,
            next_i: 0,
            curr_table_results: None,
            table_name,
            predicates,
            select_columns,
        }
    }
}

impl<'input, 'chunk> Iterator for ReadFilterResults<'input, 'chunk> {
    type Item = RecordBatch;

    fn next(&mut self) -> Option<Self::Item> {
        if self.next_i == self.chunks.len() {
            return None;
        }

        // Try next chunk's table.
        if self.curr_table_results.is_none() {
            self.curr_table_results = self.chunks[self.next_i].read_filter(
                self.table_name,
                self.predicates,
                &self.select_columns,
            );
        }

        match &mut self.curr_table_results {
            // Table potentially has some results.
            Some(table_results) => {
                // Table has found results in a row group.
                if let Some(row_group_result) = table_results.next() {
                    return Some(row_group_result.record_batch());
                }

                // no more results for row groups in the table. Try next chunk.
                self.next_i += 1;
                self.curr_table_results = None;
                self.next()
            }
            // Table does not exist.
            None => {
                // Since chunk pruning is the caller's responsibility, I don't
                // think the caller should request chunk data that does not have
                // the right tables.
                todo!(
                    "What to do about this? Seems like this is an error on the part of the caller"
                );
            }
        }
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
        array::{
            ArrayRef, BinaryArray, BooleanArray, Float64Array, Int64Array, StringArray, UInt64Array,
        },
        datatypes::{
            DataType::{Float64, Int64, Utf8},
            Field, Schema,
        },
    };

    use column::Values;

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
    fn database_update_partition() {
        let mut db = Database::new();
        db.upsert_partition("hour_1", 22, "a_table", gen_recordbatch());

        assert_eq!(db.rows(), 3);
        assert_eq!(db.tables(), 1);
        assert_eq!(db.row_groups(), 1);

        let partition = db.partitions.values().next().unwrap();
        assert_eq!(partition.tables(), 1);
        assert_eq!(partition.rows(), 3);
        assert_eq!(partition.row_groups(), 1);

        // Updating the chunk with another row group for the table just adds
        // that row group to the existing table.
        db.upsert_partition("hour_1", 22, "a_table", gen_recordbatch());
        assert_eq!(db.rows(), 6);
        assert_eq!(db.tables(), 1); // still one table
        assert_eq!(db.row_groups(), 2);

        let partition = db.partitions.values().next().unwrap();
        assert_eq!(partition.tables(), 1); // it's the same table.
        assert_eq!(partition.rows(), 6);
        assert_eq!(partition.row_groups(), 2);

        // Adding the same data under another table would increase the table
        // count.
        db.upsert_partition("hour_1", 22, "b_table", gen_recordbatch());
        assert_eq!(db.rows(), 9);
        assert_eq!(db.tables(), 2);
        assert_eq!(db.row_groups(), 3);

        let partition = db.partitions.values().next().unwrap();
        assert_eq!(partition.tables(), 2);
        assert_eq!(partition.rows(), 9);
        assert_eq!(partition.row_groups(), 3);

        // Adding the data under another chunk adds a new chunk.
        db.upsert_partition("hour_1", 29, "a_table", gen_recordbatch());
        assert_eq!(db.rows(), 12);
        assert_eq!(db.tables(), 3); // two distinct tables but across two chunks.
        assert_eq!(db.row_groups(), 4);

        let partition = db.partitions.values().next().unwrap();
        assert_eq!(partition.tables(), 3);
        assert_eq!(partition.rows(), 12);
        assert_eq!(partition.row_groups(), 4);

        let chunk_22 = db
            .partitions
            .get("hour_1")
            .unwrap()
            .chunks
            .values()
            .next()
            .unwrap();
        assert_eq!(chunk_22.tables(), 2);
        assert_eq!(chunk_22.rows(), 9);
        assert_eq!(chunk_22.row_groups(), 3);

        let chunk_29 = db
            .partitions
            .get("hour_1")
            .unwrap()
            .chunks
            .values()
            .nth(1)
            .unwrap();
        assert_eq!(chunk_29.tables(), 1);
        assert_eq!(chunk_29.rows(), 3);
        assert_eq!(chunk_29.row_groups(), 1);
    }

    // Helper function to assert the contents of a column on a record batch.
    fn assert_rb_column_equals(rb: &RecordBatch, col_name: &str, exp: &Values<'_>) {
        let got_column = rb.column(rb.schema().index_of(col_name).unwrap());

        match exp {
            Values::String(exp_data) => {
                let arr: &StringArray = got_column.as_any().downcast_ref::<StringArray>().unwrap();
                assert_eq!(&arr.iter().collect::<Vec<_>>(), exp_data);
            }
            Values::I64(exp_data) => {
                let arr: &Int64Array = got_column.as_any().downcast_ref::<Int64Array>().unwrap();
                assert_eq!(arr.values(), exp_data);
            }
            Values::U64(exp_data) => {
                let arr: &UInt64Array = got_column.as_any().downcast_ref::<UInt64Array>().unwrap();
                assert_eq!(arr.values(), exp_data);
            }
            Values::F64(exp_data) => {
                let arr: &Float64Array =
                    got_column.as_any().downcast_ref::<Float64Array>().unwrap();
                assert_eq!(arr.values(), exp_data);
            }
            Values::I64N(exp_data) => {
                let arr: &Int64Array = got_column.as_any().downcast_ref::<Int64Array>().unwrap();
                let got_data = (0..got_column.len())
                    .map(|i| {
                        if got_column.is_null(i) {
                            None
                        } else {
                            Some(arr.value(i))
                        }
                    })
                    .collect::<Vec<_>>();
                assert_eq!(&got_data, exp_data);
            }
            Values::U64N(exp_data) => {
                let arr: &UInt64Array = got_column.as_any().downcast_ref::<UInt64Array>().unwrap();
                let got_data = (0..got_column.len())
                    .map(|i| {
                        if got_column.is_null(i) {
                            None
                        } else {
                            Some(arr.value(i))
                        }
                    })
                    .collect::<Vec<_>>();
                assert_eq!(&got_data, exp_data);
            }
            Values::F64N(exp_data) => {
                let arr: &Float64Array =
                    got_column.as_any().downcast_ref::<Float64Array>().unwrap();
                let got_data = (0..got_column.len())
                    .map(|i| {
                        if got_column.is_null(i) {
                            None
                        } else {
                            Some(arr.value(i))
                        }
                    })
                    .collect::<Vec<_>>();
                assert_eq!(&got_data, exp_data);
            }
            Values::Bool(exp_data) => {
                let arr: &BooleanArray =
                    got_column.as_any().downcast_ref::<BooleanArray>().unwrap();
                let got_data = (0..got_column.len())
                    .map(|i| {
                        if got_column.is_null(i) {
                            None
                        } else {
                            Some(arr.value(i))
                        }
                    })
                    .collect::<Vec<_>>();
                assert_eq!(&got_data, exp_data);
            }
            Values::ByteArray(exp_data) => {
                let arr: &BinaryArray = got_column.as_any().downcast_ref::<BinaryArray>().unwrap();
                let got_data = (0..got_column.len())
                    .map(|i| {
                        if got_column.is_null(i) {
                            None
                        } else {
                            Some(arr.value(i))
                        }
                    })
                    .collect::<Vec<_>>();
                assert_eq!(&got_data, exp_data);
            }
        }
    }

    #[test]
    fn table_names() {
        let mut db = Database::new();

        db.upsert_partition("hour_1", 22, "Coolverine", gen_recordbatch());
        let data = db.table_names("hour_1", &[22], &[]).unwrap().unwrap();
        assert_rb_column_equals(&data, "table", &Values::String(vec![Some("Coolverine")]));

        db.upsert_partition("hour_1", 22, "Coolverine", gen_recordbatch());
        let data = db.table_names("hour_1", &[22], &[]).unwrap().unwrap();
        assert_rb_column_equals(&data, "table", &Values::String(vec![Some("Coolverine")]));

        db.upsert_partition("hour_1", 2, "Coolverine", gen_recordbatch());
        let data = db.table_names("hour_1", &[22], &[]).unwrap().unwrap();
        assert_rb_column_equals(&data, "table", &Values::String(vec![Some("Coolverine")]));

        db.upsert_partition("hour_1", 2, "20 Size", gen_recordbatch());
        let data = db.table_names("hour_1", &[22], &[]).unwrap().unwrap();
        assert_rb_column_equals(
            &data,
            "table",
            &Values::String(vec![Some("20 Size"), Some("Coolverine")]),
        );
    }

    #[test]
    fn read_filter_single_chunk() {
        let mut db = Database::new();

        // Add a bunch of row groups to a single table in a single chunk
        for &i in &[100, 200, 300] {
            let metadata = vec![
                ("env".to_owned(), TAG_COLUMN_TYPE.to_owned()),
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
                    ("env", Utf8),
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
                Arc::new(StringArray::from(vec!["us-west", "us-east", "us-west"])),
                Arc::new(StringArray::from(vec!["west", "west", "east"])),
                Arc::new(Float64Array::from(vec![1.2, 300.3, 4500.3])),
                Arc::new(Int64Array::from(vec![i, 2 * i, 3 * i])),
            ];

            // Add a record batch to a single partition
            let rb = RecordBatch::try_new(Arc::new(schema), data).unwrap();
            db.upsert_partition("hour_1", 22, "Coolverine", rb);
        }

        // Build the following query:
        //
        //   SELECT * FROM "table_1"
        //   WHERE "env" = 'us-west' AND
        //   "time" >= 0 AND  "time" < 17
        //
        let mut predicates = time_range_predicate(100, 205); // filter on time
        predicates.push((
            "env",
            (
                column::cmp::Operator::Equal,
                column::Value::String("us-west"),
            ),
        ));

        let mut itr = db
            .read_filter(
                "hour_1",
                "Coolverine",
                &[22],
                &predicates,
                table::ColumnSelection::All,
            )
            .unwrap();

        let exp_env_values = Values::String(vec![Some("us-west")]);
        let exp_region_values = Values::String(vec![Some("west")]);
        let exp_counter_values = Values::F64(vec![1.2]);

        let first_row_group = itr.next().unwrap();
        println!("{:?}", first_row_group);
        assert_rb_column_equals(&first_row_group, "env", &exp_env_values);
        assert_rb_column_equals(&first_row_group, "region", &exp_region_values);
        assert_rb_column_equals(&first_row_group, "counter", &exp_counter_values);
        assert_rb_column_equals(&first_row_group, "time", &Values::I64(vec![100])); // first row from first record batch

        let second_row_group = itr.next().unwrap();
        println!("{:?}", second_row_group);
        assert_rb_column_equals(&second_row_group, "env", &exp_env_values);
        assert_rb_column_equals(&second_row_group, "region", &exp_region_values);
        assert_rb_column_equals(&second_row_group, "counter", &exp_counter_values);
        assert_rb_column_equals(&second_row_group, "time", &Values::I64(vec![200])); // first row from second record batch

        // No more data
        assert!(itr.next().is_none());
    }

    #[test]
    fn read_filter_multiple_chunks() {
        let mut db = Database::new();

        // Add a bunch of row groups to a single table across multiple chunks
        for &i in &[100, 200, 300] {
            let metadata = vec![
                ("env".to_owned(), TAG_COLUMN_TYPE.to_owned()),
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
                    ("env", Utf8),
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
                Arc::new(StringArray::from(vec!["us-west", "us-east", "us-west"])),
                Arc::new(StringArray::from(vec!["west", "west", "east"])),
                Arc::new(Float64Array::from(vec![1.2, 300.3, 4500.3])),
                Arc::new(Int64Array::from(vec![i, 2 * i, 3 * i])),
            ];

            // Add a record batch to a single partition
            let rb = RecordBatch::try_new(Arc::new(schema), data).unwrap();

            // The row group gets added to a different chunk each time.
            db.upsert_partition("hour_1", i as u32, "Coolverine", rb);
        }

        // Build the following query:
        //
        //   SELECT * FROM "table_1"
        //   WHERE "env" = 'us-west' AND
        //   "time" >= 0 AND  "time" < 17
        //
        let mut predicates = time_range_predicate(100, 205); // filter on time
        predicates.push((
            "env",
            (
                column::cmp::Operator::Equal,
                column::Value::String("us-west"),
            ),
        ));

        let mut itr = db
            .read_filter(
                "hour_1",
                "Coolverine",
                &[100, 200, 300],
                &predicates,
                table::ColumnSelection::Some(&["env", "region", "counter", "time"]),
            )
            .unwrap();

        let exp_env_values = Values::String(vec![Some("us-west")]);
        let exp_region_values = Values::String(vec![Some("west")]);
        let exp_counter_values = Values::F64(vec![1.2]);

        let first_row_group = itr.next().unwrap();
        assert_rb_column_equals(&first_row_group, "env", &exp_env_values);
        assert_rb_column_equals(&first_row_group, "region", &exp_region_values);
        assert_rb_column_equals(&first_row_group, "counter", &exp_counter_values);
        assert_rb_column_equals(&first_row_group, "time", &Values::I64(vec![100])); // first row from first record batch

        let second_row_group = itr.next().unwrap();
        assert_rb_column_equals(&second_row_group, "env", &exp_env_values);
        assert_rb_column_equals(&second_row_group, "region", &exp_region_values);
        assert_rb_column_equals(&second_row_group, "counter", &exp_counter_values);
        assert_rb_column_equals(&second_row_group, "time", &Values::I64(vec![200])); // first row from second record batch

        // No matching data for chunk 3, so iteration ends.
        assert!(itr.next().is_none());
    }
}
