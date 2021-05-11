use std::{
    collections::{btree_map::Entry, BTreeMap, BTreeSet},
    convert::TryFrom,
};

use parking_lot::RwLock;
use snafu::{OptionExt, ResultExt, Snafu};

use arrow::record_batch::RecordBatch;
use data_types::{chunk::ChunkColumnSummary, partition_metadata::TableSummary};
use internal_types::{schema::builder::Error as SchemaError, schema::Schema, selection::Selection};
use observability_deps::tracing::info;
use tracker::{MemRegistry, MemTracker};

use crate::row_group::RowGroup;
use crate::row_group::{ColumnName, Predicate};
use crate::schema::{AggregateType, ResultSchema};
use crate::table;
use crate::table::Table;

type TableName = String;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("unsupported operation: {}", msg))]
    UnsupportedOperation { msg: String },

    #[snafu(display("error processing table: {}", source))]
    TableError { source: table::Error },

    #[snafu(display("error generating schema for table: {}", source))]
    TableSchemaError { source: SchemaError },

    #[snafu(display("table '{}' does not exist", table_name))]
    TableNotFound { table_name: String },

    #[snafu(display("column '{}' does not exist in table '{}'", column_name, table_name))]
    ColumnDoesNotExist {
        column_name: String,
        table_name: String,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// A `Chunk` comprises a collection of `Tables` where every table must have a
/// unique identifier (name).
pub struct Chunk {
    // The unique identifier for this chunk.
    id: u32,

    // A chunk's data is held in a collection of mutable tables and
    // mutable meta data (`TableData`).
    //
    // Concurrent access to the `TableData` is managed via an `RwLock`, which is
    // taken in the following circumstances:
    //
    //    * A lock is needed when updating a table with a new row group. It is held as long as it
    //      takes to update the table and update the chunk's meta-data. This is not long.
    //
    //    * A lock is needed when removing an entire table. It is held as long as it takes to
    //      remove the table from the `TableData`'s map, and re-construct new meta-data. This is
    //      not long.
    //
    //    * A read lock is needed for all read operations over chunk data (tables). However, the
    //      read lock is only taken for as long as it takes to determine which table data is needed
    //      to perform the read, shallow-clone that data (via Arcs), and construct an iterator for
    //      executing that operation. Once the iterator is returned to the caller, the lock is
    //      freed. Therefore, read execution against the chunk is mostly lock-free.
    //
    //    TODO(edd): `table_names` is currently one exception to execution that is mostly
    //               lock-free. At the moment the read-lock is held for the duration of the
    //               call. Whilst this execution will probably be in the order of micro-seconds
    //               I plan to improve this situation in due course.
    pub(crate) chunk_data: RwLock<TableData>,
}

/// Tie data and meta-data together so that they can be wrapped in RWLock.
pub(crate) struct TableData {
    /// Total number of rows across all tables
    rows: u64,

    /// Total number of row groups across all tables in the chunk.
    row_groups: usize,

    /// The set of tables within this chunk. Each table is identified by a
    /// measurement name.
    data: BTreeMap<TableName, Table>,

    /// keep track of memory used by table data in chunk
    tracker: MemTracker,
}

impl Default for TableData {
    fn default() -> Self {
        Self {
            rows: 0,
            row_groups: 0,
            data: BTreeMap::new(),
            tracker: MemRegistry::new().register(),
        }
    }
}

impl TableData {
    // Returns the total size of the contents of the tables stored under
    // `TableData`.
    fn size(&self) -> usize {
        self.data
            .iter()
            .map(|(k, table)| k.len() + table.size() as usize)
            .sum::<usize>()
    }
}

impl Chunk {
    /// Initialises a new `Chunk` with the associated chunk ID.
    pub fn new(id: u32) -> Self {
        Self {
            id,
            chunk_data: RwLock::new(TableData::default()),
        }
    }

    /// Initialises a new `Chunk` with the associated chunk ID. The returned
    /// `Chunk` will be tracked according to the provided memory tracker
    /// registry.
    pub fn new_with_memory_tracker(id: u32, registry: &MemRegistry) -> Self {
        let chunk = Self::new(id);

        {
            let mut chunk_data = chunk.chunk_data.write();
            chunk_data.tracker = registry.register();
            let size = Self::base_size() + chunk_data.size();
            chunk_data.tracker.set_bytes(size);
        }

        chunk
    }

    /// Initialises a new `Chunk` seeded with the provided `Table`.
    ///
    /// TODO(edd): potentially deprecate.
    pub(crate) fn new_with_table(id: u32, table: Table) -> Self {
        Self {
            id,
            chunk_data: RwLock::new(TableData {
                rows: table.rows(),
                row_groups: table.row_groups(),
                data: vec![(table.name().to_owned(), table)].into_iter().collect(),
                tracker: MemRegistry::new().register(),
            }),
        }
    }

    /// The chunk's ID.
    pub fn id(&self) -> u32 {
        self.id
    }

    // The total size taken up by an empty instance of `Chunk`.
    fn base_size() -> usize {
        std::mem::size_of::<Self>()
    }

    /// The total estimated size in bytes of this `Chunk` and all contained
    /// data.
    pub fn size(&self) -> usize {
        let table_data = self.chunk_data.read();
        Self::base_size() + table_data.size()
    }

    /// Return the estimated size for each column in the specific table.
    /// Note there may be multiple entries for each column.
    ///
    /// If no such table exists in this chunk, an empty Vec is returned.
    pub fn column_sizes(&self, table_name: &str) -> Vec<ChunkColumnSummary> {
        let chunk_data = self.chunk_data.read();
        chunk_data
            .data
            .get(table_name)
            .map(|table| table.column_sizes())
            .unwrap_or_default()
    }

    /// The total number of rows in all row groups in all tables in this chunk.
    pub fn rows(&self) -> u64 {
        self.chunk_data.read().rows
    }

    /// The total number of row groups in all tables in this chunk.
    pub(crate) fn row_groups(&self) -> usize {
        self.chunk_data.read().row_groups
    }

    /// The total number of tables in this chunk.
    pub(crate) fn tables(&self) -> usize {
        self.chunk_data.read().data.len()
    }

    /// Returns true if the chunk contains data for this table.
    pub fn has_table(&self, table_name: &str) -> bool {
        self.chunk_data.read().data.contains_key(table_name)
    }

    /// Returns true if there are no tables under this chunk.
    pub(crate) fn is_empty(&self) -> bool {
        self.chunk_data.read().data.len() == 0
    }

    /// Add a row_group to a table in the chunk, updating all Chunk meta data.
    ///
    /// This operation locks the chunk for the duration of the call.
    ///
    /// TODO(edd): to be deprecated.
    pub(crate) fn upsert_table_with_row_group(
        &self,
        table_name: impl Into<String>,
        row_group: RowGroup,
    ) {
        let table_name = table_name.into();

        // Take write lock to modify chunk.
        let mut chunk_data = self.chunk_data.write();

        // update the meta-data for this chunk with contents of row group.
        chunk_data.rows += row_group.rows() as u64;
        chunk_data.row_groups += 1;

        match chunk_data.data.entry(table_name.clone()) {
            Entry::Occupied(mut table_entry) => {
                let table = table_entry.get_mut();
                table.add_row_group(row_group);
            }
            Entry::Vacant(table_entry) => {
                // add a new table to this chunk.
                table_entry.insert(Table::new(table_name, row_group));
            }
        };

        // Get and set new size of chunk on memory tracker
        let size = Self::base_size() + chunk_data.size();
        chunk_data.tracker.set_bytes(size);
    }

    /// Add a record batch of data to to a `Table` in the chunk.
    ///
    /// The data is converted to a `RowGroup` outside of any locking on the
    /// `Chunk` so the caller does not need to be concerned about the size of
    /// the update. If the `Table` already exists then a new `RowGroup` will be
    /// added to the `Table`. Otherwise a new `Table` with a single `RowGroup`
    /// will be created.
    pub fn upsert_table(&self, table_name: impl Into<String>, table_data: RecordBatch) {
        // Approximate heap size of record batch.
        let rb_size = table_data
            .columns()
            .iter()
            .map(|c| c.get_buffer_memory_size())
            .sum::<usize>();
        let columns = table_data.num_columns();

        // This call is expensive. Complete it before locking.
        let now = std::time::Instant::now();
        let row_group = RowGroup::from(table_data);
        let compressing_took = now.elapsed();
        let table_name = table_name.into();

        let rows = row_group.rows();
        let rg_size = row_group.size();
        let compression = format!("{:.2}%", (1.0 - (rg_size as f64 / rb_size as f64)) * 100.0);
        let chunk_id = self.id();
        info!(%rows, %columns, rb_size, rg_size, %compression, ?table_name, %chunk_id, ?compressing_took, "row group added");

        let mut chunk_data = self.chunk_data.write();

        // update the meta-data for this chunk with contents of row group.
        chunk_data.rows += row_group.rows() as u64;
        chunk_data.row_groups += 1;

        // create a new table if one doesn't exist, or add the table data to
        // the existing table.
        match chunk_data.data.entry(table_name.clone()) {
            Entry::Occupied(mut table_entry) => {
                let table = table_entry.get_mut();
                table.add_row_group(row_group);
            }
            Entry::Vacant(table_entry) => {
                // add a new table to this chunk.
                table_entry.insert(Table::new(table_name, row_group));
            }
        };

        // Get and set new size of chunk on memory tracker
        let size = Self::base_size() + chunk_data.size();
        chunk_data.tracker.set_bytes(size);
    }

    /// Removes the table specified by `name` along with all of its contained
    /// data. Data may not be freed until all concurrent read operations against
    /// the specified table have finished.
    ///
    /// Dropping a table that does not exist is effectively an no-op.
    pub(crate) fn drop_table(&mut self, name: &str) {
        let mut chunk_data = self.chunk_data.write();

        // Remove table and update chunk meta-data if table exists.
        if let Some(table) = chunk_data.data.remove(name) {
            chunk_data.rows -= table.rows();
            chunk_data.row_groups -= table.row_groups();
        }
    }

    //
    // Methods for executing queries.
    //

    /// Returns selected data for the specified columns in the provided table.
    ///
    /// Results may be filtered by conjunctive predicates.
    /// The `ReadBuffer` will optimally prune columns and tables to improve
    /// execution where possible.
    ///
    /// `read_filter` return an iterator that will emit record batches for all
    /// row groups help under the provided chunks.
    ///
    /// `read_filter` is lazy - it does not execute against the next chunk until
    /// the results for the previous one have been emitted.
    ///
    /// Returns an error if `table_name` does not exist.
    pub fn read_filter(
        &self,
        table_name: &str,
        predicate: Predicate,
        select_columns: Selection<'_>,
    ) -> Result<table::ReadFilterResults, Error> {
        // read lock on chunk.
        let chunk_data = self.chunk_data.read();

        let table = chunk_data
            .data
            .get(table_name)
            .context(TableNotFound { table_name })?;

        Ok(table.read_filter(&select_columns, &predicate))
    }

    /// Returns an iterable collection of data in group columns and aggregate
    /// columns, optionally filtered by the provided predicate. Results are
    /// merged across all row groups within the returned table.
    ///
    /// Returns an error if the specified table does not exist.
    ///
    /// Note: `read_aggregate` currently only supports grouping on "tag"
    /// columns.
    pub(crate) fn read_aggregate(
        &self,
        table_name: &str,
        predicate: Predicate,
        group_columns: &Selection<'_>,
        aggregates: &[(ColumnName<'_>, AggregateType)],
    ) -> Result<table::ReadAggregateResults> {
        // read lock on chunk.
        let chunk_data = self.chunk_data.read();

        let table = chunk_data
            .data
            .get(table_name)
            .context(TableNotFound { table_name })?;

        table
            .read_aggregate(predicate, group_columns, aggregates)
            .context(TableError)
    }

    //
    // ---- Schema queries
    //

    /// Determines if one of more rows in the provided table could possibly
    /// match the provided predicate.
    ///
    /// If the provided table does not exist then `could_pass_predicate` returns
    /// `false`.
    pub fn could_pass_predicate(&self, table_name: &str, predicate: Predicate) -> bool {
        // read lock on chunk.
        let chunk_data = self.chunk_data.read();

        match chunk_data.data.get(table_name) {
            Some(table) => table.could_pass_predicate(&predicate),
            None => false,
        }
    }

    /// Return table summaries or all tables in this chunk.
    /// Each table will be represented exactly once.
    pub fn table_summaries(&self) -> Vec<TableSummary> {
        // read lock on chunk.
        let chunk_data = self.chunk_data.read();

        chunk_data
            .data
            .values()
            .map(|table| table.table_summary())
            .collect()
    }

    /// A helper method for determining the time-range associated with the
    /// specified table.
    ///
    /// A table's schema need not contain a column representing the time,
    /// however any table that represents data using the InfluxDB model does
    /// contain a column that represents the timestamp associated with each
    /// row.
    ///
    /// `table_time_range` will return the min and max values for that column
    /// if the table is using the InfluxDB data-model, otherwise it will return
    /// `None`. An error will be returned if the table does not exist.
    pub fn table_time_range(&self, table_name: &str) -> Result<Option<(i64, i64)>> {
        // read lock on chunk.
        let chunk_data = self.chunk_data.read();

        let table = chunk_data
            .data
            .get(table_name)
            .context(TableNotFound { table_name })?;

        Ok(table.time_range())
    }

    /// Returns a schema object for a `read_filter` operation using the provided
    /// column selection. An error is returned if the specified columns do not
    /// exist.
    pub fn read_filter_table_schema(
        &self,
        table_name: &str,
        columns: Selection<'_>,
    ) -> Result<Schema> {
        // read lock on chunk.
        let chunk_data = self.chunk_data.read();

        let table = chunk_data
            .data
            .get(table_name)
            .context(TableNotFound { table_name })?;

        // Validate columns exist in table.
        let table_meta = table.meta();
        if let Selection::Some(cols) = columns {
            for column_name in cols {
                if !table_meta.has_column(column_name) {
                    return ColumnDoesNotExist {
                        column_name: column_name.to_string(),
                        table_name: table_name.to_string(),
                    }
                    .fail();
                }
            }
        }

        // Build a table schema
        Schema::try_from(&ResultSchema {
            select_columns: match columns {
                Selection::All => table_meta.schema_for_all_columns(),
                Selection::Some(column_names) => table_meta.schema_for_column_names(column_names),
            },
            ..ResultSchema::default()
        })
        .context(TableSchemaError)
    }

    /// A helper method for retrieving all table names for the `Chunk`.
    pub fn all_table_names(&self, skip_table_names: &BTreeSet<String>) -> BTreeSet<String> {
        self.table_names(&Predicate::default(), skip_table_names)
    }

    /// Returns the distinct set of table names that contain data satisfying the
    /// provided predicate.
    ///
    /// `skip_table_names` can be used to provide a set of table names to
    /// skip, typically because they're already included in results from other
    /// chunks.
    pub fn table_names(
        &self,
        predicate: &Predicate,
        skip_table_names: &BTreeSet<String>,
    ) -> BTreeSet<String> {
        // read lock on chunk.
        let chunk_data = self.chunk_data.read();

        if predicate.is_empty() {
            return chunk_data
                .data
                .keys()
                .filter(|&name| !skip_table_names.contains(name))
                .cloned()
                .collect::<BTreeSet<_>>();
        }

        // TODO(edd): potential contention. The read lock is held on the chunk
        // for the duration of determining if its table satisfies the predicate.
        // This may be expensive in pathological cases. This can be improved
        // by releasing the lock before doing the execution.
        chunk_data
            .data
            .iter()
            .filter_map(|(name, table)| {
                if skip_table_names.contains(name) {
                    return None;
                }

                table
                    .satisfies_predicate(predicate)
                    .then(|| name.to_owned())
            })
            .collect::<BTreeSet<_>>()
    }

    /// Returns the distinct set of column names that contain data matching the
    /// provided predicate, which may be empty.
    ///
    /// Results can be further limited to a specific selection of columns.
    ///
    /// `dst` is a buffer that will be populated with results. `column_names` is
    /// smart enough to short-circuit processing on row groups when it
    /// determines that all the columns in the row group are already contained
    /// in the results buffer. Callers can skip this behaviour by passing in
    /// an empty `BTreeSet`.
    pub fn column_names(
        &self,
        table_name: &str,
        predicate: Predicate,
        only_columns: Selection<'_>,
        dst: BTreeSet<String>,
    ) -> Result<BTreeSet<String>> {
        let chunk_data = self.chunk_data.read();

        // TODO(edd): same potential contention as `table_names` but I'm ok
        // with this for now.
        match chunk_data.data.get(table_name) {
            Some(table) => Ok(table.column_names(&predicate, only_columns, dst)),
            None => TableNotFound {
                table_name: table_name.to_owned(),
            }
            .fail(),
        }
    }

    /// Returns the distinct set of column values for each provided column,
    /// where each returned value lives in a row matching the provided
    /// predicate. All values are deduplicated across row groups in the table.
    ///
    /// If the predicate is empty then all distinct values are returned for the
    /// table.
    ///
    /// Returns an error if the provided table does not exist.
    ///
    /// `dst` is intended to allow for some more sophisticated execution,
    /// wherein execution can be short-circuited for distinct values that have
    /// already been found. Callers can simply provide an empty `BTreeMap` to
    /// skip this behaviour.
    pub fn column_values(
        &self,
        table_name: &str,
        predicate: Predicate,
        columns: Selection<'_>,
        dst: BTreeMap<String, BTreeSet<String>>,
    ) -> Result<BTreeMap<String, BTreeSet<String>>> {
        let columns = match columns {
            Selection::All => {
                return UnsupportedOperation {
                    msg: "column_values does not support All columns".to_owned(),
                }
                .fail();
            }
            Selection::Some(columns) => columns,
        };

        let chunk_data = self.chunk_data.read();

        // TODO(edd): same potential contention as `table_names` but I'm ok
        // with this for now.
        match chunk_data.data.get(table_name) {
            Some(table) => table
                .column_values(&predicate, columns, dst)
                .context(TableError),
            None => TableNotFound {
                table_name: table_name.to_owned(),
            }
            .fail(),
        }
    }
}

impl std::fmt::Debug for Chunk {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "Chunk: id: {:?}, rows: {:?}", self.id(), self.rows())
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use arrow::{
        array::{
            ArrayRef, BinaryArray, BooleanArray, Float64Array, Int64Array, StringArray,
            TimestampNanosecondArray, UInt64Array,
        },
        datatypes::DataType::{Boolean, Float64, Int64, UInt64, Utf8},
    };
    use data_types::partition_metadata::{ColumnSummary, InfluxDbType, StatValues, Statistics};
    use internal_types::schema::builder::SchemaBuilder;

    use super::*;
    use crate::{column::Column, BinaryExpr};
    use crate::{
        row_group::{ColumnType, RowGroup},
        value::Values,
    };
    use arrow::array::DictionaryArray;
    use arrow::datatypes::Int32Type;

    // helper to make the `add_remove_tables` test simpler to read.
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
            Arc::new(
                vec!["west", "west", "east"]
                    .into_iter()
                    .collect::<DictionaryArray<Int32Type>>(),
            ),
            Arc::new(Float64Array::from(vec![1.2, 3.3, 45.3])),
            Arc::new(BooleanArray::from(vec![true, false, true])),
            Arc::new(TimestampNanosecondArray::from_vec(
                vec![11111111, 222222, 3333],
                None,
            )),
            Arc::new(Float64Array::from(vec![Some(11.0), None, Some(12.0)])),
        ];

        RecordBatch::try_new(schema, data).unwrap()
    }

    // Helper function to assert the contents of a column on a record batch.
    fn assert_rb_column_equals(rb: &RecordBatch, col_name: &str, exp: &Values<'_>) {
        use arrow::datatypes::DataType;

        let got_column = rb.column(rb.schema().index_of(col_name).unwrap());

        match exp {
            Values::String(exp_data) => match got_column.data_type() {
                DataType::Utf8 => {
                    let arr = got_column.as_any().downcast_ref::<StringArray>().unwrap();
                    assert_eq!(&arr.iter().collect::<Vec<_>>(), exp_data);
                }
                DataType::Dictionary(key, value)
                    if key.as_ref() == &DataType::Int32 && value.as_ref() == &DataType::Utf8 =>
                {
                    let dictionary = got_column
                        .as_any()
                        .downcast_ref::<DictionaryArray<Int32Type>>()
                        .unwrap();
                    let values = dictionary.values();
                    let values = values.as_any().downcast_ref::<StringArray>().unwrap();

                    let hydrated: Vec<_> = dictionary
                        .keys()
                        .iter()
                        .map(|key| key.map(|key| values.value(key as _)))
                        .collect();

                    assert_eq!(&hydrated, exp_data)
                }
                d => panic!("Unexpected type {:?}", d),
            },
            Values::I64(exp_data) => {
                if let Some(arr) = got_column.as_any().downcast_ref::<Int64Array>() {
                    assert_eq!(arr.values(), exp_data);
                } else if let Some(arr) = got_column
                    .as_any()
                    .downcast_ref::<TimestampNanosecondArray>()
                {
                    assert_eq!(arr.values(), exp_data);
                } else {
                    panic!("Unexpected type");
                }
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
    fn add_remove_tables() {
        let chunk = Chunk::new(22);

        // Add a new table to the chunk.
        chunk.upsert_table("a_table", gen_recordbatch());

        assert_eq!(chunk.id(), 22);
        assert_eq!(chunk.rows(), 3);
        assert_eq!(chunk.tables(), 1);
        assert_eq!(chunk.row_groups(), 1);
        assert!(chunk.size() > 0);

        {
            let chunk_data = chunk.chunk_data.read();
            let table = chunk_data.data.get("a_table").unwrap();
            assert_eq!(table.rows(), 3);
            assert_eq!(table.row_groups(), 1);
        }

        // Add a row group to the same table in the Chunk.
        let last_chunk_size = chunk.size();
        chunk.upsert_table("a_table", gen_recordbatch());

        assert_eq!(chunk.rows(), 6);
        assert_eq!(chunk.tables(), 1);
        assert_eq!(chunk.row_groups(), 2);
        assert!(chunk.size() > last_chunk_size);

        {
            let chunk_data = chunk.chunk_data.read();
            let table = chunk_data.data.get("a_table").unwrap();
            assert_eq!(table.rows(), 6);
            assert_eq!(table.row_groups(), 2);
        }

        // Add a row group to a new table in the Chunk.
        let last_chunk_size = chunk.size();
        chunk.upsert_table("b_table", gen_recordbatch());

        assert_eq!(chunk.rows(), 9);
        assert_eq!(chunk.tables(), 2);
        assert_eq!(chunk.row_groups(), 3);
        assert!(chunk.size() > last_chunk_size);

        {
            let chunk_data = chunk.chunk_data.read();
            let table = chunk_data.data.get("b_table").unwrap();
            assert_eq!(table.rows(), 3);
            assert_eq!(table.row_groups(), 1);
        }

        {
            let chunk_data = chunk.chunk_data.read();
            let table = chunk_data.data.get("a_table").unwrap();
            assert_eq!(table.rows(), 6);
            assert_eq!(table.row_groups(), 2);
        }
    }

    #[test]
    fn read_filter_table_schema() {
        let chunk = Chunk::new(22);

        // Add a new table to the chunk.
        chunk.upsert_table("a_table", gen_recordbatch());
        let schema = chunk
            .read_filter_table_schema("a_table", Selection::All)
            .unwrap();

        let exp_schema: Arc<Schema> = SchemaBuilder::new()
            .tag("region")
            .field("counter", Float64)
            .field("active", Boolean)
            .timestamp()
            .field("sketchy_sensor", Float64)
            .build()
            .unwrap()
            .into();
        assert_eq!(Arc::new(schema), exp_schema);

        let schema = chunk
            .read_filter_table_schema(
                "a_table",
                Selection::Some(&["sketchy_sensor", "counter", "region"]),
            )
            .unwrap();

        let exp_schema: Arc<Schema> = SchemaBuilder::new()
            .field("sketchy_sensor", Float64)
            .field("counter", Float64)
            .tag("region")
            .build()
            .unwrap()
            .into();
        assert_eq!(Arc::new(schema), exp_schema);

        // Verify error handling
        assert!(matches!(
            chunk.read_filter_table_schema("a_table", Selection::Some(&["random column name"])),
            Err(Error::ColumnDoesNotExist { .. })
        ));
    }

    #[test]
    fn has_table() {
        let chunk = Chunk::new(22);

        // Add a new table to the chunk.
        chunk.upsert_table("a_table", gen_recordbatch());
        assert!(chunk.has_table("a_table"));
        assert!(!chunk.has_table("b_table"));
    }

    #[test]
    fn table_summaries() {
        let chunk = Chunk::new(22);

        let schema = SchemaBuilder::new()
            .non_null_tag("env")
            .non_null_field("temp", Float64)
            .non_null_field("counter", UInt64)
            .non_null_field("icounter", Int64)
            .non_null_field("active", Boolean)
            .non_null_field("msg", Utf8)
            .timestamp()
            .build()
            .unwrap();

        let data: Vec<ArrayRef> = vec![
            Arc::new(
                vec!["prod", "dev", "prod"]
                    .into_iter()
                    .collect::<DictionaryArray<Int32Type>>(),
            ),
            Arc::new(Float64Array::from(vec![10.0, 30000.0, 4500.0])),
            Arc::new(UInt64Array::from(vec![1000, 3000, 5000])),
            Arc::new(Int64Array::from(vec![1000, -1000, 4000])),
            Arc::new(BooleanArray::from(vec![true, true, false])),
            Arc::new(StringArray::from(vec![Some("msg a"), Some("msg b"), None])),
            Arc::new(TimestampNanosecondArray::from_vec(
                vec![11111111, 222222, 3333],
                None,
            )),
        ];

        // Add a record batch to a single partition
        let rb = RecordBatch::try_new(schema.into(), data).unwrap();
        // The row group gets added to the same chunk each time.
        chunk.upsert_table("table1", rb);

        let summaries = chunk.table_summaries();
        let expected = vec![TableSummary {
            name: "table1".into(),
            columns: vec![
                ColumnSummary {
                    name: "active".into(),
                    influxdb_type: Some(InfluxDbType::Field),
                    stats: Statistics::Bool(StatValues {
                        min: Some(false),
                        max: Some(true),
                        count: 3,
                    }),
                },
                ColumnSummary {
                    name: "counter".into(),
                    influxdb_type: Some(InfluxDbType::Field),
                    stats: Statistics::U64(StatValues {
                        min: Some(1000),
                        max: Some(5000),
                        count: 3,
                    }),
                },
                ColumnSummary {
                    name: "env".into(),
                    influxdb_type: Some(InfluxDbType::Tag),
                    stats: Statistics::String(StatValues {
                        min: Some("dev".into()),
                        max: Some("prod".into()),
                        count: 3,
                    }),
                },
                ColumnSummary {
                    name: "icounter".into(),
                    influxdb_type: Some(InfluxDbType::Field),
                    stats: Statistics::I64(StatValues {
                        min: Some(-1000),
                        max: Some(4000),
                        count: 3,
                    }),
                },
                ColumnSummary {
                    name: "msg".into(),
                    influxdb_type: Some(InfluxDbType::Field),
                    stats: Statistics::String(StatValues {
                        min: Some("msg a".into()),
                        max: Some("msg b".into()),
                        count: 3,
                    }),
                },
                ColumnSummary {
                    name: "temp".into(),
                    influxdb_type: Some(InfluxDbType::Field),
                    stats: Statistics::F64(StatValues {
                        min: Some(10.0),
                        max: Some(30000.0),
                        count: 3,
                    }),
                },
                ColumnSummary {
                    name: "time".into(),
                    influxdb_type: Some(InfluxDbType::Timestamp),
                    stats: Statistics::I64(StatValues {
                        min: Some(3333),
                        max: Some(11111111),
                        count: 3,
                    }),
                },
            ],
        }];

        assert_eq!(
            expected, summaries,
            "expected:\n{:#?}\n\nactual:{:#?}\n\n",
            expected, summaries
        );
    }

    #[test]
    fn read_filter() {
        let chunk = Chunk::new(22);

        // Add a bunch of row groups to a single table in a single chunk
        for &i in &[100, 200, 300] {
            let schema = SchemaBuilder::new()
                .non_null_tag("env")
                .non_null_tag("region")
                .non_null_field("counter", Float64)
                .field("sketchy_sensor", Int64)
                .non_null_field("active", Boolean)
                .field("msg", Utf8)
                .timestamp()
                .build()
                .unwrap();

            let data: Vec<ArrayRef> = vec![
                Arc::new(
                    vec!["us-west", "us-east", "us-west"]
                        .into_iter()
                        .collect::<DictionaryArray<Int32Type>>(),
                ),
                Arc::new(
                    vec!["west", "west", "east"]
                        .into_iter()
                        .collect::<DictionaryArray<Int32Type>>(),
                ),
                Arc::new(Float64Array::from(vec![1.2, 300.3, 4500.3])),
                Arc::new(Int64Array::from(vec![None, Some(33), Some(44)])),
                Arc::new(BooleanArray::from(vec![true, false, false])),
                Arc::new(StringArray::from(vec![
                    Some("message a"),
                    Some("message b"),
                    None,
                ])),
                Arc::new(TimestampNanosecondArray::from_vec(
                    vec![i, 2 * i, 3 * i],
                    None,
                )),
            ];

            // Add a record batch to a single partition
            let rb = RecordBatch::try_new(schema.into(), data).unwrap();
            chunk.upsert_table("Coolverine", rb);
        }

        // Build the operation equivalent to the following query:
        //
        //   SELECT * FROM "table_1"
        //   WHERE "env" = 'us-west' AND
        //   "time" >= 100 AND  "time" < 205
        //
        let predicate =
            Predicate::with_time_range(&[BinaryExpr::from(("env", "=", "us-west"))], 100, 205); // filter on time

        let mut itr = chunk
            .read_filter("Coolverine", predicate, Selection::All)
            .unwrap();

        let exp_env_values = Values::String(vec![Some("us-west")]);
        let exp_region_values = Values::String(vec![Some("west")]);
        let exp_counter_values = Values::F64(vec![1.2]);
        let exp_sketchy_sensor_values = Values::I64N(vec![None]);
        let exp_active_values = Values::Bool(vec![Some(true)]);

        let first_row_group = itr.next().unwrap();
        assert_rb_column_equals(&first_row_group, "env", &exp_env_values);
        assert_rb_column_equals(&first_row_group, "region", &exp_region_values);
        assert_rb_column_equals(&first_row_group, "counter", &exp_counter_values);
        assert_rb_column_equals(
            &first_row_group,
            "sketchy_sensor",
            &exp_sketchy_sensor_values,
        );
        assert_rb_column_equals(&first_row_group, "active", &exp_active_values);
        assert_rb_column_equals(
            &first_row_group,
            "msg",
            &Values::String(vec![Some("message a")]),
        );
        assert_rb_column_equals(&first_row_group, "time", &Values::I64(vec![100])); // first row from first record batch

        let second_row_group = itr.next().unwrap();
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
    fn could_pass_predicate() {
        let chunk = Chunk::new(22);

        // Add a new table to the chunk.
        chunk.upsert_table("a_table", gen_recordbatch());

        assert!(!chunk.could_pass_predicate("not my table", Predicate::default()));
        assert!(chunk.could_pass_predicate(
            "a_table",
            Predicate::new(vec![BinaryExpr::from(("region", "=", "east"))])
        ));
    }

    #[test]
    fn table_names() {
        let columns = vec![
            (
                "time".to_owned(),
                ColumnType::create_time(&[1_i64, 2, 3, 4, 5, 6]),
            ),
            (
                "region".to_owned(),
                ColumnType::create_tag(&["west", "west", "east", "west", "south", "north"]),
            ),
        ];
        let rg = RowGroup::new(6, columns);
        let table = Table::new("table_1", rg);

        let chunk = Chunk::new_with_table(22, table);

        // All table names returned when no predicate.
        let table_names = chunk.table_names(&Predicate::default(), &BTreeSet::new());
        assert_eq!(
            table_names
                .iter()
                .map(|v| v.as_str())
                .collect::<Vec<&str>>(),
            vec!["table_1"]
        );

        // All table names returned if no predicate and not in skip list
        let table_names = chunk.table_names(
            &Predicate::default(),
            &["table_2".to_owned()].iter().cloned().collect(),
        );
        assert_eq!(
            table_names
                .iter()
                .map(|v| v.as_str())
                .collect::<Vec<&str>>(),
            vec!["table_1"]
        );

        // Table name not returned if it is in skip list
        let table_names = chunk.table_names(
            &Predicate::default(),
            &["table_1".to_owned()].iter().cloned().collect(),
        );
        assert!(table_names.is_empty());

        // table returned when predicate matches
        let table_names = chunk.table_names(
            &Predicate::new(vec![BinaryExpr::from(("region", ">=", "west"))]),
            &BTreeSet::new(),
        );
        assert_eq!(
            table_names
                .iter()
                .map(|v| v.as_str())
                .collect::<Vec<&str>>(),
            vec!["table_1"]
        );

        // table not returned when predicate doesn't match
        let table_names = chunk.table_names(
            &Predicate::new(vec![BinaryExpr::from(("region", ">", "west"))]),
            &BTreeSet::new(),
        );
        assert!(table_names.is_empty());

        // create another table with different timestamps.
        let columns = vec![
            (
                "time".to_owned(),
                ColumnType::Time(Column::from(&[100_i64, 200, 300, 400, 500, 600][..])),
            ),
            (
                "region".to_owned(),
                ColumnType::create_tag(&["west", "west", "east", "west", "south", "north"][..]),
            ),
        ];
        let rg = RowGroup::new(6, columns);
        chunk.upsert_table_with_row_group("table_2", rg);

        // all tables returned when predicate matches both
        let table_names = chunk.table_names(
            &Predicate::new(vec![BinaryExpr::from(("region", "!=", "north-north-east"))]),
            &BTreeSet::new(),
        );
        assert_eq!(
            table_names
                .iter()
                .map(|v| v.as_str())
                .collect::<Vec<&str>>(),
            vec!["table_1", "table_2"]
        );

        // only one table returned when one table matches predicate
        let table_names = chunk.table_names(
            &Predicate::new(vec![BinaryExpr::from(("time", ">", 300_i64))]),
            &BTreeSet::new(),
        );
        assert_eq!(
            table_names
                .iter()
                .map(|v| v.as_str())
                .collect::<Vec<&str>>(),
            vec!["table_2"]
        );
    }

    fn to_set(v: &[&str]) -> BTreeSet<String> {
        v.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn column_names() {
        let chunk = Chunk::new(22);

        let schema = SchemaBuilder::new()
            .non_null_tag("region")
            .non_null_field("counter", Float64)
            .timestamp()
            .field("sketchy_sensor", Float64)
            .build()
            .unwrap()
            .into();

        let data: Vec<ArrayRef> = vec![
            Arc::new(
                vec!["west", "west", "east"]
                    .into_iter()
                    .collect::<DictionaryArray<Int32Type>>(),
            ),
            Arc::new(Float64Array::from(vec![1.2, 3.3, 45.3])),
            Arc::new(TimestampNanosecondArray::from_vec(
                vec![11111111, 222222, 3333],
                None,
            )),
            Arc::new(Float64Array::from(vec![Some(11.0), None, Some(12.0)])),
        ];

        // Add the above table to the chunk
        let rb = RecordBatch::try_new(schema, data).unwrap();
        chunk.upsert_table("Utopia", rb);

        let result = chunk
            .column_names(
                "Utopia",
                Predicate::default(),
                Selection::All,
                BTreeSet::new(),
            )
            .unwrap();

        assert_eq!(
            result,
            to_set(&["counter", "region", "sketchy_sensor", "time"])
        );

        // Testing predicates
        let result = chunk
            .column_names(
                "Utopia",
                Predicate::new(vec![BinaryExpr::from(("time", "=", 222222_i64))]),
                Selection::All,
                BTreeSet::new(),
            )
            .unwrap();

        // sketchy_sensor won't be returned because it has a NULL value for the
        // only matching row.
        assert_eq!(result, to_set(&["counter", "region", "time"]));
    }

    fn to_map(arr: Vec<(&str, &[&str])>) -> BTreeMap<String, BTreeSet<String>> {
        arr.iter()
            .map(|(k, values)| {
                (
                    k.to_string(),
                    values
                        .iter()
                        .map(|s| s.to_string())
                        .collect::<BTreeSet<_>>(),
                )
            })
            .collect::<BTreeMap<_, _>>()
    }

    #[test]
    fn column_values() {
        let chunk = Chunk::new(22);

        let schema = SchemaBuilder::new()
            .non_null_tag("region")
            .non_null_tag("env")
            .timestamp()
            .build()
            .unwrap()
            .into();

        let data: Vec<ArrayRef> = vec![
            Arc::new(
                vec!["north", "south", "east"]
                    .into_iter()
                    .collect::<DictionaryArray<Int32Type>>(),
            ),
            Arc::new(
                vec![Some("prod"), None, Some("stag")]
                    .into_iter()
                    .collect::<DictionaryArray<Int32Type>>(),
            ),
            Arc::new(TimestampNanosecondArray::from_vec(
                vec![11111111, 222222, 3333],
                None,
            )),
        ];

        // Add the above table to a chunk and partition
        let rb = RecordBatch::try_new(schema, data).unwrap();
        chunk.upsert_table("my_table", rb);

        let result = chunk
            .column_values(
                "my_table",
                Predicate::default(),
                Selection::Some(&["region", "env"]),
                BTreeMap::new(),
            )
            .unwrap();

        assert_eq!(
            result,
            to_map(vec![
                ("region", &["north", "south", "east"]),
                ("env", &["prod", "stag"])
            ])
        );

        // With a predicate
        let result = chunk
            .column_values(
                "my_table",
                Predicate::new(vec![
                    BinaryExpr::from(("time", ">=", 20_i64)),
                    BinaryExpr::from(("time", "<=", 3333_i64)),
                ]),
                Selection::Some(&["region", "env"]),
                BTreeMap::new(),
            )
            .unwrap();

        assert_eq!(
            result,
            to_map(vec![
                ("region", &["east"]),
                ("env", &["stag"]) // column_values returns non-null values.
            ])
        );

        // Error when All column selection provided.
        assert!(matches!(
            chunk.column_values("x", Predicate::default(), Selection::All, BTreeMap::new()),
            Err(Error::UnsupportedOperation { .. })
        ));
    }
}
