#![deny(rust_2018_idioms)]
#![allow(dead_code)]
#![allow(clippy::too_many_arguments)]
#![allow(unused_variables)]
#![warn(clippy::clone_on_ref_ptr)]
pub(crate) mod chunk;
pub(crate) mod column;
pub(crate) mod row_group;
mod schema;
pub(crate) mod table;
pub(crate) mod value;

use std::{
    collections::{btree_map::Entry, BTreeMap, BTreeSet},
    convert::TryInto,
    fmt,
    sync::RwLock,
};

use arrow_deps::arrow::record_batch::RecordBatch;
use data_types::{
    schema::{builder::SchemaMerger, Schema},
    selection::Selection,
};
use snafu::{ensure, OptionExt, ResultExt, Snafu};

// Identifiers that are exported as part of the public API.
pub use row_group::{BinaryExpr, Predicate};
pub use schema::*;

use chunk::Chunk;
use row_group::{ColumnName, RowGroup};
use table::Table;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("arrow conversion error: {}", source))]
    ArrowError {
        source: arrow_deps::arrow::error::ArrowError,
    },

    // TODO add more context / helpful error here
    #[snafu(display("Error building unioned read buffer schema for chunks: {}", source))]
    BuildingSchema {
        source: data_types::schema::builder::Error,
    },

    #[snafu(display("partition key does not exist: {}", key))]
    PartitionNotFound { key: String },

    #[snafu(display("chunk id does not exist: {}", id))]
    ChunkNotFound { id: u32 },

    #[snafu(display("table does not exist: {}", table_name))]
    TableNotFound { table_name: String },

    #[snafu(display("unsupported operation: {}", msg))]
    UnsupportedOperation { msg: String },

    #[snafu(display("unsupported aggregate: {}", agg))]
    UnsupportedAggregate { agg: AggregateType },

    #[snafu(display("error processing chunk: {}", source))]
    ChunkError { source: chunk::Error },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

// A database is scoped to a single tenant. Within a database there exists
// partitions, chunks, tables and row groups.
#[derive(Default)]
pub struct Database {
    data: RwLock<PartitionData>,
}

#[derive(Default)]
struct PartitionData {
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
        &self,
        partition_key: &str,
        chunk_id: u32,
        table_name: &str,
        table_data: RecordBatch,
    ) {
        // This call is expensive. Complete it before locking.
        let row_group = RowGroup::from(table_data);

        // Take lock on partitions and update.
        let mut partition_data = self.data.write().unwrap();
        partition_data.size += row_group.size();
        partition_data.rows += row_group.rows() as u64;

        // create a new chunk if one doesn't exist, or add the table data to
        // the existing chunk.
        match partition_data.partitions.entry(partition_key.to_owned()) {
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

    /// Remove all row groups, tables and chunks associated with the specified
    /// partition key.
    ///
    /// This operation requires a write lock on the database, but the duration
    /// of time the lock is held is limited only to the time needed to update
    /// containers and drop memory.
    pub fn drop_partition(&mut self, partition_key: &str) -> Result<()> {
        let mut partition_data = self.data.write().unwrap();

        let partition = partition_data
            .partitions
            .remove(partition_key)
            .context(PartitionNotFound { key: partition_key })?;

        partition_data.size -= partition.size();
        partition_data.rows -= partition.rows();
        Ok(())
    }

    /// Remove all row groups and tables for the specified chunks and partition.
    pub fn drop_chunk(&self, partition_key: &str, chunk_id: u32) -> Result<()> {
        let mut partition_data = self.data.write().unwrap();

        let partition = partition_data
            .partitions
            .get_mut(partition_key)
            .context(PartitionNotFound { key: partition_key })?;

        partition.drop_chunk(chunk_id).map(|chunk| {
            partition_data.size -= chunk.size();
            partition_data.rows -= chunk.rows();
            // don't return chunk from `drop_chunk`
        })
    }

    /// Clones and returns all partition keys with data for this database.
    pub fn partition_keys(&self) -> Vec<String> {
        self.data
            .read()
            .unwrap()
            .partitions
            .keys()
            .cloned()
            .collect()
    }

    /// Lists all chunk ids in the given partition key. Returns empty
    /// `Vec` if no partition with the given key exists
    pub fn chunk_ids(&self, partition_key: &str) -> Vec<u32> {
        self.data
            .read()
            .unwrap()
            .partitions
            .get(partition_key)
            .map(|partition| partition.chunk_ids())
            .unwrap_or_default()
    }

    pub fn size(&self) -> u64 {
        self.data.read().unwrap().size
    }

    pub fn rows(&self) -> u64 {
        self.data.read().unwrap().rows
    }

    /// Determines the total number of tables under all partitions within the
    /// database.
    pub fn tables(&self) -> usize {
        self.data
            .read()
            .unwrap()
            .partitions
            .values()
            .map(|partition| partition.tables())
            .sum()
    }

    /// Determines the total number of row groups under all tables under all
    /// chunks, within the database.
    pub fn row_groups(&self) -> usize {
        self.data
            .read()
            .unwrap()
            .partitions
            .values()
            .map(|chunk| chunk.row_groups())
            .sum()
    }

    /// returns true if the table exists in at least one of the specified chunks
    pub fn has_table(&self, partition_key: &str, table_name: &str, chunk_ids: &[u32]) -> bool {
        let partition_data = self.data.read().unwrap();

        if let Some(partition) = partition_data.partitions.get(partition_key) {
            partition.has_table(table_name, chunk_ids)
        } else {
            false
        }
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
        select_columns: Selection<'a>,
    ) -> Result<ReadFilterResults> {
        // Get read lock on database's partitions.
        let partition_data = self.data.read().unwrap();
        let mut chunk_table_results = vec![];

        match partition_data.partitions.get(partition_key) {
            Some(partition) => {
                for chunk_id in chunk_ids {
                    // Get read lock on partition's chunks.
                    let chunk_data = partition.data.read().unwrap();

                    let chunk = chunk_data
                        .chunks
                        .get(chunk_id)
                        .context(ChunkNotFound { id: *chunk_id })?;

                    ensure!(chunk.has_table(table_name), TableNotFound { table_name });

                    // Get all relevant row groups for this chunk's table. This
                    // is cheap because it doesn't execute the read operation,
                    // but just gets pointers to the necessary data for
                    // execution.
                    let chunk_result = chunk
                        .read_filter(table_name, &predicate, &select_columns)
                        .context(ChunkError)?;
                    chunk_table_results.push(chunk_result);
                }

                Ok(ReadFilterResults::new(chunk_table_results))
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
    pub fn read_aggregate<'input>(
        &self,
        partition_key: &str,
        table_name: &'input str,
        chunk_ids: &[u32],
        predicate: Predicate,
        group_columns: Selection<'input>,
        aggregates: Vec<(ColumnName<'input>, AggregateType)>,
    ) -> Result<ReadAggregateResults> {
        for (_, agg) in &aggregates {
            match agg {
                AggregateType::First | AggregateType::Last => {
                    return Err(Error::UnsupportedAggregate { agg: *agg });
                }
                _ => {}
            }
        }

        // get read lock on database
        let partition_data = self.data.read().unwrap();
        let mut chunk_table_results = vec![];

        let partition = partition_data
            .partitions
            .get(partition_key)
            .context(PartitionNotFound { key: partition_key })?;

        for chunk_id in chunk_ids {
            // Get read lock on partition's chunks.
            let chunk_data = partition.data.read().unwrap();

            let chunk = chunk_data
                .chunks
                .get(chunk_id)
                .context(ChunkNotFound { id: *chunk_id })?;

            ensure!(chunk.has_table(table_name), TableNotFound { table_name });

            // Get all relevant row groups for this chunk's table. This
            // is cheap because it doesn't execute the read operation,
            // but just gets references to the needed to data to do so.
            if let Some(table_results) =
                chunk.read_aggregate(table_name, predicate.clone(), &group_columns, &aggregates)
            {
                chunk_table_results.push(table_results);
            }
        }

        Ok(ReadAggregateResults::new(chunk_table_results))
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
        group_columns: Selection<'_>,
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
        select_columns: Selection<'_>,
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
    pub fn table_names(
        &self,
        partition_key: &str,
        chunk_ids: &[u32],
        predicate: Predicate,
    ) -> Result<BTreeSet<String>> {
        let partition_data = self.data.read().unwrap();

        let partition = partition_data
            .partitions
            .get(partition_key)
            .ok_or_else(|| Error::PartitionNotFound {
                key: partition_key.to_owned(),
            })?;

        let chunk_data = partition.data.read().unwrap();
        let mut filtered_chunks = vec![];
        for id in chunk_ids {
            filtered_chunks.push(
                chunk_data
                    .chunks
                    .get(id)
                    .ok_or_else(|| Error::ChunkNotFound { id: *id })?,
            );
        }

        let names = filtered_chunks
            .iter()
            .fold(BTreeSet::new(), |mut names, chunk| {
                // notice that `names` is pushed into the chunk `table_name`
                // implementation. This ensure we don't process more tables than
                // we need to.
                names.append(&mut chunk.table_names(&predicate, &names));
                names
            });
        Ok(names)
    }

    /// Returns the distinct set of column names (tag keys) that satisfy the
    /// provided predicate.
    pub fn column_names(
        &self,
        partition_key: &str,
        table_name: &str,
        chunk_ids: &[u32],
        predicate: Predicate,
    ) -> Result<Option<BTreeSet<String>>> {
        let partition_data = self.data.read().unwrap();

        let partition = partition_data
            .partitions
            .get(partition_key)
            .ok_or_else(|| Error::PartitionNotFound {
                key: partition_key.to_owned(),
            })?;

        let chunk_data = partition.data.read().unwrap();
        let mut filtered_chunks = Vec::with_capacity(chunk_ids.len());
        for id in chunk_ids {
            filtered_chunks.push(
                chunk_data
                    .chunks
                    .get(id)
                    .ok_or_else(|| Error::ChunkNotFound { id: *id })?,
            );
        }

        let names = filtered_chunks.iter().fold(BTreeSet::new(), |dst, chunk| {
            // the dst buffer is pushed into each chunk's `column_names`
            // implementation ensuring that we short-circuit any tables where
            // we have already determined column names.
            chunk.column_names(table_name, &predicate, dst)
        });

        Ok(Some(names))
    }
}

impl fmt::Debug for Database {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let partition_data = self.data.read().unwrap();
        f.debug_struct("Database")
            .field("partitions", &partition_data.partitions.keys())
            .field("size", &partition_data.size)
            .finish()
    }
}

#[derive(Default)]
struct ChunkData {
    // The collection of chunks in the partition. Each chunk is uniquely
    // identified by a chunk id.
    chunks: BTreeMap<u32, Chunk>,

    // The current total size of the partition.
    size: u64,

    // The current number of row groups in this partition.
    row_groups: usize,

    // Total number of rows in the partition.
    rows: u64,
}

// A partition is a collection of `Chunks`.
#[derive(Default)]
struct Partition {
    // The partition's key
    key: String,

    data: RwLock<ChunkData>,
}

impl Partition {
    pub fn new(partition_key: &str, chunk: Chunk) -> Self {
        Self {
            key: partition_key.to_owned(),
            data: RwLock::new(ChunkData {
                size: chunk.size(),
                row_groups: chunk.row_groups(),
                rows: chunk.rows(),
                chunks: vec![(chunk.id(), chunk)].into_iter().collect(),
            }),
        }
    }

    /// Adds new data for a chunk.
    ///
    /// Data should be provided as a single row group for a table within the
    /// chunk. If the `Table` or `Chunk` does not exist they will be created,
    /// otherwise relevant structures will be updated.
    ///
    /// This operation locks the partition for the duration of the call.
    fn upsert_chunk(&mut self, chunk_id: u32, table_name: String, row_group: RowGroup) {
        let mut chunk_data = self.data.write().unwrap();

        chunk_data.size += row_group.size();
        chunk_data.row_groups += 1;
        chunk_data.rows += row_group.rows() as u64;

        // create a new chunk if one doesn't exist, or add the table data to
        // the existing chunk.
        match chunk_data.chunks.entry(chunk_id) {
            Entry::Occupied(mut chunk_entry) => {
                let chunk = chunk_entry.get_mut();
                chunk.upsert_table(table_name, row_group);
            }
            Entry::Vacant(chunk_entry) => {
                chunk_entry.insert(Chunk::new(chunk_id, Table::new(table_name, row_group)));
            }
        };
    }

    // Drops the chunk and all associated data.
    fn drop_chunk(&mut self, chunk_id: u32) -> Result<Chunk> {
        let mut chunk_data = self.data.write().unwrap();

        let chunk = chunk_data
            .chunks
            .remove(&chunk_id)
            .context(ChunkNotFound { id: chunk_id })?;

        chunk_data.size -= chunk.size();
        chunk_data.rows -= chunk.rows();
        chunk_data.row_groups -= chunk.row_groups();
        Ok(chunk)
    }

    /// Return the chunk ids stored in this partition, in order of id
    fn chunk_ids(&self) -> Vec<u32> {
        self.data.read().unwrap().chunks.keys().cloned().collect()
    }

    /// Determines the total number of tables under all chunks within the
    /// partition. Useful for tests but not something that is highly performant.
    fn tables(&self) -> usize {
        self.data
            .read()
            .unwrap()
            .chunks
            .values()
            .map(|chunk| chunk.tables())
            .sum()
    }

    /// returns true if the table exists in this chunk
    pub fn has_table(&self, table_name: &str, chunk_ids: &[u32]) -> bool {
        let chunk_data = self.data.read().unwrap();

        chunk_ids
            .iter()
            .filter_map(|chunk_id| chunk_data.chunks.get(chunk_id))
            .any(|chunk| chunk.has_table(table_name))
    }

    /// Determines the total number of row groups under all tables under all
    /// chunks, within the partition.
    pub fn row_groups(&self) -> usize {
        self.data.read().unwrap().row_groups
    }

    pub fn rows(&self) -> u64 {
        self.data.read().unwrap().rows
    }

    pub fn size(&self) -> u64 {
        self.data.read().unwrap().size
    }
}

/// ReadFilterResults implements ...
pub struct ReadFilterResults {
    // The table results for all chunks being executed against
    all_chunks_table_results: Vec<table::ReadFilterResults>,
    next_chunk: usize,
}

impl fmt::Debug for ReadFilterResults {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReadFilterResults")
            .field(
                "all_chunks_table_results.len",
                &self.all_chunks_table_results.len(),
            )
            .field("next_chunk", &self.next_chunk)
            .finish()
    }
}

impl ReadFilterResults {
    fn new(results: Vec<table::ReadFilterResults>) -> Self {
        Self {
            all_chunks_table_results: results,
            next_chunk: 0,
        }
    }

    /// Return the union of the schemas that this result will produce,
    /// or an Error if they are not compatible
    pub fn schema(&self) -> Result<Schema> {
        let builder = self.all_chunks_table_results.iter().try_fold(
            SchemaMerger::new(),
            |builder, table_result| {
                let table_schema = table_result.schema();

                let schema: Schema = table_schema.try_into().context(BuildingSchema)?;

                let builder = builder.merge(schema).context(BuildingSchema)?;

                Ok(builder)
            },
        )?;

        builder.build().context(BuildingSchema)
    }
}

impl Iterator for ReadFilterResults {
    type Item = RecordBatch;

    fn next(&mut self) -> Option<Self::Item> {
        if self.next_chunk == self.all_chunks_table_results.len() {
            return None;
        }

        let table_result_itr = &mut self.all_chunks_table_results[self.next_chunk];
        match table_result_itr.next() {
            Some(rb) => Some(rb),
            None => {
                // no more row group results for the table in the chunk. Try
                // next chunk.
                self.next_chunk += 1;
                self.next()
            }
        }
    }
}

/// An iterable set of results for calls to `read_aggregate`.
///
/// The iterator lazily executes against each chunk on a call to `next`.
/// Currently all row group results inside the chunk's table are merged before
/// this iterator returns a record batch. Therefore the caller can expect at
/// most one record batch to be yielded for each chunk.
pub struct ReadAggregateResults {
    // The table results for all chunks being executed against
    all_chunks_table_results: Vec<table::ReadAggregateResults>,
    next_chunk: usize,
}

impl ReadAggregateResults {
    fn new(results: Vec<table::ReadAggregateResults>) -> Self {
        Self {
            all_chunks_table_results: results,
            next_chunk: 0,
        }
    }
}

impl Iterator for ReadAggregateResults {
    type Item = RecordBatch;

    fn next(&mut self) -> Option<Self::Item> {
        if self.next_chunk == self.all_chunks_table_results.len() {
            return None;
        }

        let table_result_itr = &mut self.all_chunks_table_results[self.next_chunk];
        match table_result_itr.next() {
            Some(rb) => Some(rb),
            None => {
                // no more row group results for the table in the chunk. Try
                // next chunk.
                self.next_chunk += 1;
                self.next()
            }
        }
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
        datatypes::DataType::{Boolean, Float64, Int64, UInt64},
    };
    use data_types::schema::builder::SchemaBuilder;

    use crate::value::Values;

    // helper to make the `database_update_chunk` test simpler to read.
    fn gen_recordbatch() -> RecordBatch {
        let schema = SchemaBuilder::new()
            .non_null_tag("region")
            .non_null_field("counter", Float64)
            .non_null_field("active", Boolean)
            .timestamp()
            .field("sketchy_sensor", Float64)
            .build()
            .unwrap()
            .into();

        let data: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(vec!["west", "west", "east"])),
            Arc::new(Float64Array::from(vec![1.2, 3.3, 45.3])),
            Arc::new(BooleanArray::from(vec![true, false, true])),
            Arc::new(Int64Array::from(vec![11111111, 222222, 3333])),
            Arc::new(Float64Array::from(vec![Some(11.0), None, Some(12.0)])),
        ];

        RecordBatch::try_new(schema, data).unwrap()
    }

    #[test]
    fn database_add_drop_row_groups() {
        let mut db = Database::new();
        db.upsert_partition("hour_1", 22, "a_table", gen_recordbatch());

        assert_eq!(db.rows(), 3);
        assert_eq!(db.tables(), 1);
        assert_eq!(db.row_groups(), 1);

        {
            let partition_data = db.data.read().unwrap();
            let partition = partition_data.partitions.values().next().unwrap();
            assert_eq!(partition.tables(), 1);
            assert_eq!(partition.rows(), 3);
            assert_eq!(partition.row_groups(), 1);
        }

        // Updating the chunk with another row group for the table just adds
        // that row group to the existing table.
        db.upsert_partition("hour_1", 22, "a_table", gen_recordbatch());
        assert_eq!(db.rows(), 6);
        assert_eq!(db.tables(), 1); // still one table
        assert_eq!(db.row_groups(), 2);

        {
            let partition_data = db.data.read().unwrap();
            let partition = partition_data.partitions.values().next().unwrap();
            assert_eq!(partition.tables(), 1); // it's the same table.
            assert_eq!(partition.rows(), 6);
            assert_eq!(partition.row_groups(), 2);
        }

        // Adding the same data under another table would increase the table
        // count.
        db.upsert_partition("hour_1", 22, "b_table", gen_recordbatch());
        assert_eq!(db.rows(), 9);
        assert_eq!(db.tables(), 2);
        assert_eq!(db.row_groups(), 3);

        {
            let partition_data = db.data.read().unwrap();
            let partition = partition_data.partitions.values().next().unwrap();
            assert_eq!(partition.tables(), 2);
            assert_eq!(partition.rows(), 9);
            assert_eq!(partition.row_groups(), 3);
        }

        // Adding the data under another chunk adds a new chunk.
        db.upsert_partition("hour_1", 29, "a_table", gen_recordbatch());
        assert_eq!(db.rows(), 12);
        assert_eq!(db.tables(), 3); // two distinct tables but across two chunks.
        assert_eq!(db.row_groups(), 4);

        {
            let partition_data = db.data.read().unwrap();
            let partition = partition_data.partitions.values().next().unwrap();
            assert_eq!(partition.tables(), 3);
            assert_eq!(partition.rows(), 12);
            assert_eq!(partition.row_groups(), 4);
        }

        {
            let partition_data = db.data.read().unwrap();
            let chunk_data = partition_data
                .partitions
                .get("hour_1")
                .unwrap()
                .data
                .read()
                .unwrap();
            let chunk_22 = chunk_data.chunks.get(&22).unwrap();
            assert_eq!(chunk_22.tables(), 2);
            assert_eq!(chunk_22.rows(), 9);
            assert_eq!(chunk_22.row_groups(), 3);
        }

        {
            let partition_data = db.data.read().unwrap();
            let chunk_data = partition_data
                .partitions
                .get("hour_1")
                .unwrap()
                .data
                .read()
                .unwrap();
            let chunk_29 = chunk_data.chunks.get(&29).unwrap();
            assert_eq!(chunk_29.tables(), 1);
            assert_eq!(chunk_29.rows(), 3);
            assert_eq!(chunk_29.row_groups(), 1);
        }

        // drop a chunk.
        db.drop_chunk("hour_1", 22).unwrap();
        assert_eq!(db.rows(), 3);
        assert_eq!(db.tables(), 1);
        assert_eq!(db.row_groups(), 1);

        // drop a partition.
        db.drop_partition("hour_1").unwrap();
        assert_eq!(db.rows(), 0);
        assert_eq!(db.tables(), 0);
        assert_eq!(db.row_groups(), 0);

        // dropping an unknown partition returns an error.
        db.drop_partition("hour_1")
            .expect_err("expected partition not found error");
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
        let db = Database::new();

        db.upsert_partition("hour_1", 22, "Coolverine", gen_recordbatch());
        let data = db
            .table_names("hour_1", &[22], Predicate::default())
            .unwrap();
        assert_eq!(data, to_set(&["Coolverine"]));

        db.upsert_partition("hour_1", 22, "Coolverine", gen_recordbatch());
        let data = db
            .table_names("hour_1", &[22], Predicate::default())
            .unwrap();
        assert_eq!(data, to_set(&["Coolverine"]));

        db.upsert_partition("hour_1", 2, "Coolverine", gen_recordbatch());
        let data = db
            .table_names("hour_1", &[22], Predicate::default())
            .unwrap();
        assert_eq!(data, to_set(&["Coolverine"]));

        db.upsert_partition("hour_1", 2, "20 Size", gen_recordbatch());
        let data = db
            .table_names("hour_1", &[2, 22], Predicate::default())
            .unwrap();
        assert_eq!(data, to_set(&["20 Size", "Coolverine"]));

        let data = db
            .table_names(
                "hour_1",
                &[2, 22],
                Predicate::new(vec![BinaryExpr::from(("region", ">", "north"))]),
            )
            .unwrap();
        assert_eq!(data, to_set(&["20 Size", "Coolverine"]));
    }

    #[test]
    fn column_names() {
        let db = Database::new();

        let schema = SchemaBuilder::new()
            .non_null_tag("region")
            .non_null_field("counter", Float64)
            .timestamp()
            .field("sketchy_sensor", Float64)
            .build()
            .unwrap()
            .into();

        let data: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(vec!["west", "west", "east"])),
            Arc::new(Float64Array::from(vec![1.2, 3.3, 45.3])),
            Arc::new(Int64Array::from(vec![11111111, 222222, 3333])),
            Arc::new(Float64Array::from(vec![Some(11.0), None, Some(12.0)])),
        ];

        // Add the above table to a chunk and partition
        let rb = RecordBatch::try_new(schema, data).unwrap();
        db.upsert_partition("hour_1", 22, "Utopia", rb);

        // Add a different but compatible table to a different chunk in the same
        // partition.
        let schema = SchemaBuilder::new()
            .field("active", Boolean)
            .timestamp()
            .build()
            .unwrap()
            .into();

        let data: Vec<ArrayRef> = vec![
            Arc::new(BooleanArray::from(vec![Some(true), None, None])),
            Arc::new(Int64Array::from(vec![10, 20, 30])),
        ];
        let rb = RecordBatch::try_new(schema, data).unwrap();
        db.upsert_partition("hour_1", 40, "Utopia", rb);

        // Just query against the first chunk.
        let result = db
            .column_names("hour_1", "Utopia", &[22], Predicate::default())
            .unwrap();

        assert_eq!(
            result,
            Some(to_set(&["counter", "region", "sketchy_sensor", "time"]))
        );
        let result = db
            .column_names("hour_1", "Utopia", &[40], Predicate::default())
            .unwrap();

        assert_eq!(result, Some(to_set(&["active", "time"])));

        // And now the union across all chunks.
        let result = db
            .column_names("hour_1", "Utopia", &[22, 40], Predicate::default())
            .unwrap();

        assert_eq!(
            result,
            Some(to_set(&[
                "active",
                "counter",
                "region",
                "sketchy_sensor",
                "time"
            ]))
        );

        // Testing predicates
        let result = db
            .column_names(
                "hour_1",
                "Utopia",
                &[22, 40],
                Predicate::new(vec![BinaryExpr::from(("time", "=", 30_i64))]),
            )
            .unwrap();

        // only time will be returned - "active" in the second chunk is NULL for
        // matching rows
        assert_eq!(result, Some(to_set(&["time"])));

        let result = db
            .column_names(
                "hour_1",
                "Utopia",
                &[22, 40],
                Predicate::new(vec![BinaryExpr::from(("active", "=", true))]),
            )
            .unwrap();

        // there exists at least one row in the second chunk with a matching
        // non-null value across the active and time columns.
        assert_eq!(result, Some(to_set(&["active", "time"])));
    }

    #[test]
    fn read_filter_single_chunk() {
        let db = Database::new();

        // Add a bunch of row groups to a single table in a single chunk
        for &i in &[100, 200, 300] {
            let schema = SchemaBuilder::new()
                .non_null_tag("env")
                .non_null_tag("region")
                .non_null_field("counter", Float64)
                .field("sketchy_sensor", Int64)
                .non_null_field("active", Boolean)
                .timestamp()
                .build()
                .unwrap();

            let data: Vec<ArrayRef> = vec![
                Arc::new(StringArray::from(vec!["us-west", "us-east", "us-west"])),
                Arc::new(StringArray::from(vec!["west", "west", "east"])),
                Arc::new(Float64Array::from(vec![1.2, 300.3, 4500.3])),
                Arc::new(Int64Array::from(vec![None, Some(33), Some(44)])),
                Arc::new(BooleanArray::from(vec![true, false, false])),
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
            .read_filter("hour_1", "Coolverine", &[22], predicate, Selection::All)
            .unwrap();

        let exp_env_values = Values::String(vec![Some("us-west")]);
        let exp_region_values = Values::String(vec![Some("west")]);
        let exp_counter_values = Values::F64(vec![1.2]);
        let exp_sketchy_sensor_values = Values::I64N(vec![None]);
        let exp_active_values = Values::Bool(vec![Some(true)]);

        let first_row_group = itr.next().unwrap();
        println!("{:?}", first_row_group);
        assert_rb_column_equals(&first_row_group, "env", &exp_env_values);
        assert_rb_column_equals(&first_row_group, "region", &exp_region_values);
        assert_rb_column_equals(&first_row_group, "counter", &exp_counter_values);
        assert_rb_column_equals(
            &first_row_group,
            "sketchy_sensor",
            &exp_sketchy_sensor_values,
        );
        assert_rb_column_equals(&first_row_group, "active", &exp_active_values);
        assert_rb_column_equals(&first_row_group, "time", &Values::I64(vec![100])); // first row from first record batch

        let second_row_group = itr.next().unwrap();
        println!("{:?}", second_row_group);
        assert_rb_column_equals(&second_row_group, "env", &exp_env_values);
        assert_rb_column_equals(&second_row_group, "region", &exp_region_values);
        assert_rb_column_equals(&second_row_group, "counter", &exp_counter_values);
        assert_rb_column_equals(
            &first_row_group,
            "sketchy_sensor",
            &exp_sketchy_sensor_values,
        );
        assert_rb_column_equals(&first_row_group, "active", &exp_active_values);
        assert_rb_column_equals(&second_row_group, "time", &Values::I64(vec![200])); // first row from second record batch

        // No more data
        assert!(itr.next().is_none());
    }

    #[test]
    fn read_filter_multiple_chunks() {
        let db = Database::new();

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
                Selection::Some(&["env", "region", "counter", "time"]),
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

    #[test]
    fn read_aggregate() {
        let db = Database::new();

        // Add a bunch of row groups to a single table in a single chunks
        for &i in &[100, 200, 300] {
            let schema = SchemaBuilder::new()
                .non_null_tag("env")
                .non_null_tag("region")
                .non_null_field("temp", Float64)
                .non_null_field("counter", UInt64)
                .field("sketchy_sensor", UInt64)
                .non_null_field("active", Boolean)
                .timestamp()
                .build()
                .unwrap();

            let data: Vec<ArrayRef> = vec![
                Arc::new(StringArray::from(vec!["prod", "dev", "prod"])),
                Arc::new(StringArray::from(vec!["west", "west", "east"])),
                Arc::new(Float64Array::from(vec![10.0, 30000.0, 4500.0])),
                Arc::new(UInt64Array::from(vec![1000, 3000, 5000])),
                Arc::new(UInt64Array::from(vec![Some(44), None, Some(55)])),
                Arc::new(BooleanArray::from(vec![true, true, false])),
                Arc::new(Int64Array::from(vec![i, 20 + i, 30 + i])),
            ];

            // Add a record batch to a single partition
            let rb = RecordBatch::try_new(schema.into(), data).unwrap();
            // The row group gets added to the same chunk each time.
            db.upsert_partition("hour_1", 1, "table1", rb);
        }

        //
        // Simple Aggregates - no group keys.
        //
        //   QUERY:
        //
        //   SELECT SUM("counter"), COUNT("counter"), MIN("counter"), MAX("counter")
        //   FROM "table_1"
        //   WHERE "time" <= 130
        //

        let itr = db
            .read_aggregate(
                "hour_1",
                "table1",
                &[1],
                Predicate::new(vec![BinaryExpr::from(("time", "<=", 130_i64))]),
                Selection::Some(&[]),
                vec![
                    ("counter", AggregateType::Count),
                    ("counter", AggregateType::Sum),
                    ("counter", AggregateType::Min),
                    ("counter", AggregateType::Max),
                    ("sketchy_sensor", AggregateType::Count),
                    ("sketchy_sensor", AggregateType::Sum),
                    ("sketchy_sensor", AggregateType::Min),
                    ("sketchy_sensor", AggregateType::Max),
                    ("active", AggregateType::Count),
                    ("active", AggregateType::Min),
                    ("active", AggregateType::Max),
                ],
            )
            .unwrap();
        let result = itr.collect::<Vec<RecordBatch>>();
        assert_eq!(result.len(), 1);
        let result = &result[0];

        assert_rb_column_equals(&result, "counter_count", &Values::U64(vec![3]));
        assert_rb_column_equals(&result, "counter_sum", &Values::U64(vec![9000]));
        assert_rb_column_equals(&result, "counter_min", &Values::U64(vec![1000]));
        assert_rb_column_equals(&result, "counter_max", &Values::U64(vec![5000]));
        assert_rb_column_equals(&result, "sketchy_sensor_count", &Values::U64(vec![2])); // count of non-null values
        assert_rb_column_equals(&result, "sketchy_sensor_sum", &Values::U64(vec![99])); // sum of non-null values
        assert_rb_column_equals(&result, "sketchy_sensor_min", &Values::U64(vec![44])); // min of non-null values
        assert_rb_column_equals(&result, "sketchy_sensor_max", &Values::U64(vec![55])); // max of non-null values
        assert_rb_column_equals(&result, "active_count", &Values::U64(vec![3]));
        assert_rb_column_equals(&result, "active_min", &Values::Bool(vec![Some(false)]));
        assert_rb_column_equals(&result, "active_max", &Values::Bool(vec![Some(true)]));

        //
        // With group keys
        //
        //   QUERY:
        //
        //   SELECT SUM("temp"), MIN("temp"), SUM("counter"), COUNT("counter")
        //   FROM "table_1"
        //   GROUP BY "region"
        //

        let itr = db
            .read_aggregate(
                "hour_1",
                "table1",
                &[1],
                Predicate::default(),
                Selection::Some(&["region"]),
                vec![
                    ("temp", AggregateType::Sum),
                    ("temp", AggregateType::Min),
                    ("temp", AggregateType::Max),
                    ("counter", AggregateType::Sum),
                    ("counter", AggregateType::Count),
                ],
            )
            .unwrap();
        let result = itr.collect::<Vec<RecordBatch>>();
        assert_eq!(result.len(), 1);
        let result = &result[0];

        assert_rb_column_equals(
            &result,
            "region",
            &Values::String(vec![Some("east"), Some("west")]),
        );

        assert_rb_column_equals(&result, "temp_sum", &Values::F64(vec![13500.0, 90030.0]));
        assert_rb_column_equals(&result, "temp_min", &Values::F64(vec![4500.0, 10.0]));
        assert_rb_column_equals(&result, "temp_max", &Values::F64(vec![4500.0, 30000.0]));
        assert_rb_column_equals(&result, "counter_sum", &Values::U64(vec![15000, 12000]));
        assert_rb_column_equals(&result, "counter_count", &Values::U64(vec![3, 6]));
    }

    fn to_set(v: &[&str]) -> BTreeSet<String> {
        v.iter().map(|s| s.to_string()).collect()
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
        cmp::Operator, encoding::dictionary, encoding::fixed::Fixed,
        encoding::fixed_null::FixedNull, Column, RowIDs,
    };

    pub use crate::row_group::{ColumnType, RowGroup};
}
