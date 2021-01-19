#![deny(rust_2018_idioms)]
#![allow(dead_code)]
#![allow(clippy::too_many_arguments)]
#![allow(unused_variables)]
pub(crate) mod chunk;
pub(crate) mod column;
pub(crate) mod row_group;
mod schema;
pub(crate) mod table;

use std::{
    collections::{btree_map::Entry, BTreeMap, BTreeSet},
    convert::TryInto,
    fmt,
    sync::Arc,
};

use arrow_deps::arrow::{
    array::{ArrayRef, StringArray},
    datatypes::{DataType::Utf8, Field, Schema},
    record_batch::RecordBatch,
};
use snafu::{ensure, OptionExt, ResultExt, Snafu};

// Identifiers that are exported as part of the public API.
pub use row_group::{BinaryExpr, Predicate};
pub use schema::*;
pub use table::ColumnSelection;

use chunk::Chunk;
use row_group::{ColumnName, RowGroup};
use table::Table;

/// The name of the column containing table names returned by a call to
/// `table_names`.
pub const TABLE_NAMES_COLUMN_NAME: &str = "table";

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

    #[snafu(display("table does not exist: {}", table_name))]
    TableNotFound { table_name: String },

    #[snafu(display("unsupported operation: {}", msg))]
    UnsupportedOperation { msg: String },
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
    pub fn partition_keys(&self) -> Vec<&String> {
        self.partitions.keys().collect()
    }

    /// Lists all chunk ids in the given partition key. Returns empty
    /// `Vec` if no partition with the given key exists
    pub fn chunk_ids(&self, partition_key: &str) -> Vec<u32> {
        self.partitions
            .get(partition_key)
            .map(|partition| partition.chunk_ids())
            .unwrap_or_default()
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
        predicate: Predicate,
        select_columns: ColumnSelection<'a>,
    ) -> Result<ReadFilterResults<'a, '_>> {
        match self.partitions.get(partition_key) {
            Some(partition) => {
                let mut chunks = vec![];
                for chunk_id in chunk_ids {
                    let chunk = partition
                        .chunks
                        .get(chunk_id)
                        .context(ChunkNotFound { id: *chunk_id })?;

                    ensure!(chunk.has_table(table_name), TableNotFound { table_name });

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
                    predicate,
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
    /// This method might be deprecated in the future, replaced by a call to
    /// `read_aggregate_window` with a `window` of `0`.
    pub fn read_aggregate(
        &self,
        partition_key: &str,
        table_name: &str,
        chunk_ids: &[u32],
        predicate: Predicate,
        group_columns: ColumnSelection<'_>,
        aggregates: Vec<(ColumnName<'_>, AggregateType)>,
    ) -> Result<ReadAggregateResults> {
        Err(Error::UnsupportedOperation {
            msg: "`read_aggregate` not yet implemented".to_owned(),
        })
    }

    /// Returns windowed aggregates for each group specified by the values of
    /// the grouping keys and window, limited to the specified partition key
    /// table name and chunk ids.
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
    /// `window` should be a positive value indicating a duration in
    /// nanoseconds.
    pub fn read_window_aggregate(
        &self,
        partition_key: &str,
        table_name: &str,
        chunk_ids: &[u32],
        predicate: Predicate,
        group_columns: ColumnSelection<'_>,
        aggregates: Vec<(ColumnName<'_>, AggregateType)>,
        window: u64,
    ) -> Result<ReadWindowAggregateResults> {
        Err(Error::UnsupportedOperation {
            msg: "`read_aggregate_window` not yet implemented".to_owned(),
        })
    }

    ///
    /// NOTE: this is going to contain a specialised execution path for
    /// essentially doing:
    ///
    /// SELECT DISTINCT(column_name) WHERE XYZ
    ///
    /// In the future the `ReadBuffer` should just probably just add this
    /// special execution to read_filter queries with `DISTINCT` expressions
    /// on the selector columns.
    ///
    /// Returns the distinct set of tag values (column values) for each provided
    /// column, which *must* be considered a tag key.
    ///
    /// As a special case, if `tag_keys` is empty then all distinct values for
    /// all columns (tag keys) are returned for the provided chunks.
    pub fn tag_values(
        &self,
        partition_key: &str,
        table_name: &str,
        chunk_ids: &[u32],
        predicate: Predicate,
        select_columns: ColumnSelection<'_>,
    ) -> Result<TagValuesResults> {
        Err(Error::UnsupportedOperation {
            msg: "`tag_values` call not yet hooked up".to_owned(),
        })
    }

    //
    // ---- Schema API queries
    //

    /// Returns the distinct set of table names that contain data that satisfies
    /// the provided predicate.
    ///
    /// TODO(edd): Implement predicate support.
    pub fn table_names(
        &self,
        partition_key: &str,
        chunk_ids: &[u32],
        predicate: Predicate,
    ) -> Result<RecordBatch> {
        if !predicate.is_empty() {
            return Err(Error::UnsupportedOperation {
                msg: "predicate support on `table_names` not implemented".to_owned(),
            });
        }

        let partition = self
            .partitions
            .get(partition_key)
            .ok_or(Error::PartitionNotFound {
                key: partition_key.to_owned(),
            })?;

        let chunks = partition.chunks_by_ids(chunk_ids)?;
        let mut intersection = BTreeSet::new();
        let chunk_table_names = chunks
            .iter()
            .map(|chunk| chunk.table_names(&predicate))
            .for_each(|mut names| intersection.append(&mut names));

        let schema = Schema::new(vec![Field::new(TABLE_NAMES_COLUMN_NAME, Utf8, false)]);
        let columns: Vec<ArrayRef> = vec![Arc::new(StringArray::from(
            intersection
                .iter()
                .map(|s| s.as_str())
                .collect::<Vec<&str>>(),
        ))];

        match RecordBatch::try_new(Arc::new(schema), columns).context(ArrowError {}) {
            Ok(rb) => Ok(rb),
            Err(e) => Err(e),
        }
    }

    /// Returns the distinct set of column names (tag keys) that satisfy the
    /// provided predicate.
    pub fn column_names(
        &self,
        partition_key: &str,
        chunk_ids: &[u32],
        predicate: Predicate,
    ) -> Result<RecordBatch> {
        // Find all matching chunks using:
        //   - time range
        //   - measurement name.
        //
        // Execute query against matching chunks. The `tag_keys` method for
        // a chunk allows the caller to provide already found tag keys
        // (column names). This allows the execution to skip entire chunks,
        // tables or segments if there are no new columns to be found there...
        Err(Error::UnsupportedOperation {
            msg: "`column_names` call not yet hooked up".to_owned(),
        })
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

    /// Return the chunk ids stored in this partition, in order of id
    fn chunk_ids(&self) -> Vec<u32> {
        self.chunks.keys().cloned().collect()
    }

    fn chunks_by_ids(&self, ids: &[u32]) -> Result<Vec<&Chunk>> {
        let mut chunks = vec![];
        for chunk_id in ids {
            chunks.push(
                self.chunks
                    .get(chunk_id)
                    .ok_or_else(|| Error::ChunkNotFound { id: *chunk_id })?,
            );
        }
        Ok(chunks)
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
    curr_table_results: Option<table::ReadFilterResults<'chunk>>,

    table_name: &'input str,
    predicate: Predicate,
    select_columns: table::ColumnSelection<'input>,
}

impl<'input, 'chunk> fmt::Debug for ReadFilterResults<'input, 'chunk> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReadFilterResults")
            .field("chunks.len", &self.chunks.len())
            .field("next_i", &self.next_i)
            .field("curr_table_results", &"<OPAQUE>")
            .field("table_name", &self.table_name)
            .field("predicate", &self.predicate)
            .field("select_columns", &self.select_columns)
            .finish()
    }
}

impl<'input, 'chunk> ReadFilterResults<'input, 'chunk> {
    fn new(
        chunks: Vec<&'chunk Chunk>,
        table_name: &'input str,
        predicate: Predicate,
        select_columns: table::ColumnSelection<'input>,
    ) -> Self {
        Self {
            chunks,
            next_i: 0,
            curr_table_results: None,
            table_name,
            predicate,
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
            self.curr_table_results = Some(
                self.chunks[self.next_i]
                    .read_filter(self.table_name, &self.predicate, &self.select_columns)
                    .unwrap(),
            );
        }

        match &mut self.curr_table_results {
            // Table potentially has some results.
            Some(table_results) => {
                // Table has found results in a row group.
                if let Some(row_group_result) = table_results.next() {
                    // it should not be possible for the conversion to record
                    // batch to fail here
                    let rb = row_group_result.try_into();
                    return Some(rb.unwrap());
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

/// An iterable set of results for calls to `read_aggregate`.
///
/// There may be some internal buffering and merging of results before a record
/// batch can be emitted from the iterator.
pub struct ReadAggregateResults {}

impl Iterator for ReadAggregateResults {
    type Item = RecordBatch;

    fn next(&mut self) -> Option<Self::Item> {
        None
    }
}

/// An iterable set of results for calls to `read_window_aggregate`.
///
/// There may be some internal buffering and merging of results before a record
/// batch is emitted from the iterator.
pub struct ReadWindowAggregateResults {}

impl Iterator for ReadWindowAggregateResults {
    type Item = RecordBatch;

    fn next(&mut self) -> Option<Self::Item> {
        None
    }
}

/// An iterable set of results for calls to `tag_values`.
///
/// There may be some internal buffering and merging of results before a record
/// batch is emitted from the iterator.
pub struct TagValuesResults {}

impl Iterator for TagValuesResults {
    type Item = RecordBatch;

    fn next(&mut self) -> Option<Self::Item> {
        None
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::sync::Arc;

    use arrow_deps::arrow::{
        array::{
            ArrayRef, BinaryArray, BooleanArray, Float64Array, Int64Array, StringArray, UInt64Array,
        },
        datatypes::DataType::Float64,
    };

    use column::Values;
    use data_types::schema::builder::SchemaBuilder;

    // helper to make the `database_update_chunk` test simpler to read.
    fn gen_recordbatch() -> RecordBatch {
        let schema = SchemaBuilder::new()
            .non_null_tag("region")
            .non_null_field("counter", Float64)
            .timestamp()
            .build()
            .unwrap()
            .into();

        let data: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(vec!["west", "west", "east"])),
            Arc::new(Float64Array::from(vec![1.2, 3.3, 45.3])),
            Arc::new(Int64Array::from(vec![11111111, 222222, 3333])),
        ];

        RecordBatch::try_new(schema, data).unwrap()
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
        let data = db
            .table_names("hour_1", &[22], Predicate::default())
            .unwrap();
        assert_rb_column_equals(&data, "table", &Values::String(vec![Some("Coolverine")]));

        db.upsert_partition("hour_1", 22, "Coolverine", gen_recordbatch());
        let data = db
            .table_names("hour_1", &[22], Predicate::default())
            .unwrap();
        assert_rb_column_equals(&data, "table", &Values::String(vec![Some("Coolverine")]));

        db.upsert_partition("hour_1", 2, "Coolverine", gen_recordbatch());
        let data = db
            .table_names("hour_1", &[22], Predicate::default())
            .unwrap();
        assert_rb_column_equals(&data, "table", &Values::String(vec![Some("Coolverine")]));

        db.upsert_partition("hour_1", 2, "20 Size", gen_recordbatch());
        let data = db
            .table_names("hour_1", &[2, 22], Predicate::default())
            .unwrap();
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
            let schema = SchemaBuilder::new()
                .non_null_tag("env")
                .non_null_tag("region")
                .non_null_field("counter", Float64)
                .timestamp()
                .build()
                .unwrap();

            let data: Vec<ArrayRef> = vec![
                Arc::new(StringArray::from(vec!["us-west", "us-east", "us-west"])),
                Arc::new(StringArray::from(vec!["west", "west", "east"])),
                Arc::new(Float64Array::from(vec![1.2, 300.3, 4500.3])),
                Arc::new(Int64Array::from(vec![i, 2 * i, 3 * i])),
            ];

            // Add a record batch to a single partition
            let rb = RecordBatch::try_new(schema.into(), data).unwrap();
            db.upsert_partition("hour_1", 22, "Coolverine", rb);
        }

        // Build the following query:
        //
        //   SELECT * FROM "table_1"
        //   WHERE "env" = 'us-west' AND
        //   "time" >= 100 AND  "time" < 205
        //
        let predicate = Predicate::with_time_range(
            &[row_group::BinaryExpr::from(("env", "=", "us-west"))],
            100,
            205,
        ); // filter on time

        let mut itr = db
            .read_filter(
                "hour_1",
                "Coolverine",
                &[22],
                predicate,
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
            let schema = SchemaBuilder::new()
                .non_null_tag("env")
                .non_null_tag("region")
                .non_null_field("counter", Float64)
                .timestamp()
                .build()
                .unwrap();

            let data: Vec<ArrayRef> = vec![
                Arc::new(StringArray::from(vec!["us-west", "us-east", "us-west"])),
                Arc::new(StringArray::from(vec!["west", "west", "east"])),
                Arc::new(Float64Array::from(vec![1.2, 300.3, 4500.3])),
                Arc::new(Int64Array::from(vec![i, 2 * i, 3 * i])),
            ];

            // Add a record batch to a single partition
            let rb = RecordBatch::try_new(schema.into(), data).unwrap();

            // The row group gets added to a different chunk each time.
            db.upsert_partition("hour_1", i as u32, "Coolverine", rb);
        }

        // Build the following query:
        //
        //   SELECT * FROM "table_1"
        //   WHERE "env" = 'us-west' AND
        //   "time" >= 100 AND  "time" < 205
        //
        let predicate = Predicate::with_time_range(
            &[row_group::BinaryExpr::from(("env", "=", "us-west"))],
            100,
            205,
        ); // filter on time

        let mut itr = db
            .read_filter(
                "hour_1",
                "Coolverine",
                &[100, 200, 300],
                predicate,
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

/// THIS MODULE SHOULD ONLY BE IMPORTED FOR BENCHMARKS.
///
/// This module lets us expose internal parts of the crate so that we can use
/// libraries like criterion for benchmarking.
///
/// It should not be imported into any non-testing or benchmarking crates.
pub mod benchmarks {
    pub use crate::column::{
        cmp::Operator, dictionary, fixed::Fixed, fixed_null::FixedNull, Column, RowIDs,
    };

    pub use crate::row_group::{ColumnType, RowGroup};
}
