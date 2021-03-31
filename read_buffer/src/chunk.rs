use std::{
    collections::{btree_map::Entry, BTreeMap, BTreeSet},
    convert::TryFrom,
    sync::RwLock,
};

use arrow_deps::arrow::record_batch::RecordBatch;
use data_types::partition_metadata::TableSummary;
use internal_types::{schema::builder::Error as SchemaError, schema::Schema, selection::Selection};
use snafu::{OptionExt, ResultExt, Snafu};

use crate::row_group::RowGroup;
use crate::row_group::{ColumnName, Predicate};
use crate::schema::{AggregateType, ResultSchema};
use crate::table;
use crate::table::Table;

type TableName = String;

#[derive(Debug, Snafu)]
pub enum Error {
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

// Tie data and meta-data together so that they can be wrapped in RWLock.
#[derive(Default)]
pub(crate) struct TableData {
    rows: u64, // Total number of rows across all tables

    // Total number of row groups across all tables in the chunk.
    row_groups: usize,

    // The set of tables within this chunk. Each table is identified by a
    // measurement name.
    data: BTreeMap<TableName, Table>,
}

impl Chunk {
    /// Initialises a new `Chunk`.
    pub fn new(id: u32) -> Self {
        Self {
            id,
            chunk_data: RwLock::new(TableData::default()),
        }
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
            }),
        }
    }

    /// The chunk's ID.
    pub fn id(&self) -> u32 {
        self.id
    }

    /// The total estimated size in bytes of this `Chunk` and all contained
    /// data.
    pub fn size(&self) -> u64 {
        let base_size = std::mem::size_of::<Self>();

        let table_data = self.chunk_data.read().unwrap();
        base_size as u64
            + table_data
                .data
                .iter()
                .map(|(k, table)| k.len() as u64 + table.size())
                .sum::<u64>()
    }

    /// The total number of rows in all row groups in all tables in this chunk.
    pub(crate) fn rows(&self) -> u64 {
        self.chunk_data.read().unwrap().rows
    }

    /// The total number of row groups in all tables in this chunk.
    pub(crate) fn row_groups(&self) -> usize {
        self.chunk_data.read().unwrap().row_groups
    }

    /// The total number of tables in this chunk.
    pub(crate) fn tables(&self) -> usize {
        self.chunk_data.read().unwrap().data.len()
    }

    /// Returns true if the chunk contains data for this table.
    pub fn has_table(&self, table_name: &str) -> bool {
        self.chunk_data
            .read()
            .unwrap()
            .data
            .contains_key(table_name)
    }

    /// Returns true if there are no tables under this chunk.
    pub(crate) fn is_empty(&self) -> bool {
        self.chunk_data.read().unwrap().data.len() == 0
    }

    /// Add a row_group to a table in the chunk, updating all Chunk meta data.
    ///
    /// This operation locks the chunk for the duration of the call.
    ///
    /// TODO(edd): to be deprecated.
    pub(crate) fn upsert_table_with_row_group(
        &mut self,
        table_name: impl Into<String>,
        row_group: RowGroup,
    ) {
        let table_name = table_name.into();
        let mut chunk_data = self.chunk_data.write().unwrap();

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
    }

    /// Add a record batch of data to to a `Table` in the chunk.
    ///
    /// The data is converted to a `RowGroup` outside of any locking on the
    /// `Chunk` so the caller does not need to be concerned about the size of
    /// the update. If the `Table` already exists then a new `RowGroup` will be
    /// added to the `Table`. Otherwise a new `Table` with a single `RowGroup`
    /// will be created.
    pub fn upsert_table(&mut self, table_name: impl Into<String>, table_data: RecordBatch) {
        // This call is expensive. Complete it before locking.
        let row_group = RowGroup::from(table_data);
        let table_name = table_name.into();

        let mut chunk_data = self.chunk_data.write().unwrap();

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
    }

    /// Removes the table specified by `name` along with all of its contained
    /// data. Data may not be freed until all concurrent read operations against
    /// the specified table have finished.
    ///
    /// Dropping a table that does not exist is effectively an no-op.
    pub(crate) fn drop_table(&mut self, name: &str) {
        let mut chunk_data = self.chunk_data.write().unwrap();

        // Remove table and update chunk meta-data if table exists.
        if let Some(table) = chunk_data.data.remove(name) {
            chunk_data.rows -= table.rows();
            chunk_data.row_groups -= table.row_groups();
        }
    }

    /// Return table summaries or all tables in this chunk. Note that
    /// there can be more than one TableSummary for each table.
    pub fn table_summaries(&self) -> Vec<TableSummary> {
        // read lock on chunk.
        let chunk_data = self.chunk_data.read().unwrap();

        chunk_data
            .data
            .values()
            .map(|table| table.table_summary())
            .collect()
    }

    /// Returns an iterator of lazily executed `read_filter` operations on the
    /// provided table for the specified column selections.
    ///
    /// Results may be filtered by conjunctive predicates. Returns an error if
    /// the specified table does not exist.
    pub fn read_filter(
        &self,
        table_name: &str,
        predicate: &Predicate,
        select_columns: &Selection<'_>,
    ) -> Result<table::ReadFilterResults, Error> {
        // read lock on chunk.
        let chunk_data = self.chunk_data.read().unwrap();

        let table = chunk_data
            .data
            .get(table_name)
            .context(TableNotFound { table_name })?;

        Ok(table.read_filter(select_columns, predicate))
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
        let chunk_data = self.chunk_data.read().unwrap();

        let table = chunk_data
            .data
            .get(table_name)
            .context(TableNotFound { table_name })?;

        table
            .read_aggregate(predicate, group_columns, aggregates)
            .context(TableError)
    }

    //
    // ---- Schema API queries
    //

    /// Returns a schema object for a `read_filter` operation using the provided
    /// column selection. An error is returned if the specified columns do not
    /// exist.
    pub fn read_filter_table_schema(
        &self,
        table_name: &str,
        columns: Selection<'_>,
    ) -> Result<Schema> {
        // read lock on chunk.
        let chunk_data = self.chunk_data.read().unwrap();

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
        let chunk_data = self.chunk_data.read().unwrap();

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

                match table.satisfies_predicate(predicate) {
                    true => Some(name.to_owned()),
                    false => None,
                }
            })
            .collect::<BTreeSet<_>>()
    }

    /// Returns the distinct set of column names that contain data matching the
    /// provided predicate, which may be empty.
    ///
    /// `dst` is a buffer that will be populated with results. `column_names` is
    /// smart enough to short-circuit processing on row groups when it
    /// determines that all the columns in the row group are already contained
    /// in the results buffer.
    pub fn column_names(
        &self,
        table_name: &str,
        predicate: &Predicate,
        columns: Selection<'_>,
        dst: BTreeSet<String>,
    ) -> BTreeSet<String> {
        let chunk_data = self.chunk_data.read().unwrap();

        // TODO(edd): same potential contention as `table_names` but I'm ok
        // with this for now.
        match chunk_data.data.get(table_name) {
            Some(table) => table.column_names(predicate, columns, dst),
            None => dst,
        }
    }

    /// Returns the distinct set of column values for each provided column,
    /// where each returned value sits in a row matching the provided
    /// predicate. All values are deduplicated across row groups in the table.
    ///
    /// All specified columns must be of `String` type
    /// If the predicate is empty then all distinct values are returned for the
    /// table.
    pub fn column_values<'a>(
        &'a self,
        table_name: &str,
        predicate: &Predicate,
        columns: &[ColumnName<'_>],
        dst: BTreeMap<String, BTreeSet<String>>,
    ) -> Result<BTreeMap<String, BTreeSet<String>>> {
        let chunk_data = self.chunk_data.read().unwrap();

        // TODO(edd): same potential contention as `table_names` but I'm ok
        // with this for now.
        match chunk_data.data.get(table_name) {
            Some(table) => table
                .column_values(predicate, columns, dst)
                .context(TableError),
            None => Ok(dst),
        }
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use arrow_deps::arrow::{
        array::{
            ArrayRef, BinaryArray, BooleanArray, Float64Array, Int64Array, StringArray, UInt64Array,
        },
        datatypes::DataType::{Boolean, Float64},
    };
    use internal_types::schema::builder::SchemaBuilder;

    use super::*;
    use crate::{column::Column, BinaryExpr};
    use crate::{
        row_group::{ColumnType, RowGroup},
        value::Values,
    };

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
            Arc::new(StringArray::from(vec!["west", "west", "east"])),
            Arc::new(Float64Array::from(vec![1.2, 3.3, 45.3])),
            Arc::new(BooleanArray::from(vec![true, false, true])),
            Arc::new(Int64Array::from(vec![11111111, 222222, 3333])),
            Arc::new(Float64Array::from(vec![Some(11.0), None, Some(12.0)])),
        ];

        RecordBatch::try_new(schema, data).unwrap()
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
    fn add_remove_tables() {
        let mut chunk = Chunk::new(22);

        // Add a new table to the chunk.
        chunk.upsert_table("a_table", gen_recordbatch());

        assert_eq!(chunk.id(), 22);
        assert_eq!(chunk.rows(), 3);
        assert_eq!(chunk.tables(), 1);
        assert_eq!(chunk.row_groups(), 1);
        assert!(chunk.size() > 0);

        {
            let chunk_data = chunk.chunk_data.read().unwrap();
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
            let chunk_data = chunk.chunk_data.read().unwrap();
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
            let chunk_data = chunk.chunk_data.read().unwrap();
            let table = chunk_data.data.get("b_table").unwrap();
            assert_eq!(table.rows(), 3);
            assert_eq!(table.row_groups(), 1);
        }

        {
            let chunk_data = chunk.chunk_data.read().unwrap();
            let table = chunk_data.data.get("a_table").unwrap();
            assert_eq!(table.rows(), 6);
            assert_eq!(table.row_groups(), 2);
        }
    }

    #[test]
    fn read_filter_table_schema() {
        let mut chunk = Chunk::new(22);

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
        let mut chunk = Chunk::new_with_table(22, table);

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
}
