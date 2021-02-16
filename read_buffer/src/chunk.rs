use std::{
    collections::{btree_map::Entry, BTreeMap, BTreeSet},
    sync::RwLock,
};

use data_types::selection::Selection;
use snafu::Snafu;

use crate::row_group::RowGroup;
use crate::row_group::{ColumnName, Predicate};
use crate::schema::AggregateType;
use crate::table;
use crate::table::Table;

type TableName = String;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("table '{}' does not exist", table_name))]
    TableNotFound { table_name: String },
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
    chunk_data: RwLock<TableData>,
}

// Tie data and meta-data together so that they can be wrapped in RWLock.
struct TableData {
    rows: u64, // Total number of rows across all tables

    // Total number of row groups across all tables in the chunk.
    row_groups: usize,

    // The set of tables within this chunk. Each table is identified by a
    // measurement name.
    data: BTreeMap<TableName, Table>,
}

impl Chunk {
    pub fn new(id: u32, table: Table) -> Self {
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
    pub fn rows(&self) -> u64 {
        self.chunk_data.read().unwrap().rows
    }

    /// The total number of row groups in all tables in this chunk.
    pub fn row_groups(&self) -> usize {
        self.chunk_data.read().unwrap().row_groups
    }

    /// The total number of tables in this chunk.
    pub fn tables(&self) -> usize {
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
    pub fn is_empty(&self) -> bool {
        self.chunk_data.read().unwrap().data.len() == 0
    }

    /// Add a row_group to a table in the chunk, updating all Chunk meta data.
    ///
    /// This operation locks the chunk for the duration of the call.
    pub fn upsert_table(&mut self, table_name: impl Into<String>, row_group: RowGroup) {
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

    /// Removes the table specified by `name` along with all of its contained
    /// data. Data may not be freed until all concurrent read operations against
    /// the specified table have finished.
    ///
    /// Dropping a table that does not exist is effectively an no-op.
    pub fn drop_table(&mut self, name: &str) {
        let mut chunk_data = self.chunk_data.write().unwrap();

        // Remove table and update chunk meta-data if table exists.
        if let Some(table) = chunk_data.data.remove(name) {
            chunk_data.rows -= table.rows();
            chunk_data.row_groups -= table.row_groups();
        }
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
            .ok_or(Error::TableNotFound {
                table_name: table_name.to_owned(),
            })?;

        Ok(table.read_filter(select_columns, predicate))
    }

    /// Returns an iterable collection of data in group columns and aggregate
    /// columns, optionally filtered by the provided predicate. Results are
    /// merged across all row groups within the returned table.
    ///
    /// Returns `None` if the table no longer exists within the chunk.
    ///
    /// Note: `read_aggregate` currently only supports grouping on "tag"
    /// columns.
    pub fn read_aggregate(
        &self,
        table_name: &str,
        predicate: Predicate,
        group_columns: &Selection<'_>,
        aggregates: &[(ColumnName<'_>, AggregateType)],
    ) -> Option<table::ReadAggregateResults> {
        // read lock on chunk.
        let chunk_data = self.chunk_data.read().unwrap();

        // Lookup table by name and dispatch execution.
        //
        // TODO(edd): this should return an error
        chunk_data
            .data
            .get(table_name)
            .map(|table| table.read_aggregate(predicate, group_columns, aggregates))
    }

    //
    // ---- Schema API queries
    //

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
        dst: BTreeSet<String>,
    ) -> BTreeSet<String> {
        let chunk_data = self.chunk_data.read().unwrap();

        // TODO(edd): same potential contention as `table_names` but I'm ok
        // with this for now.
        match chunk_data.data.get(table_name) {
            Some(table) => table.column_names(predicate, dst),
            None => dst,
        }
    }

    /// Returns the distinct set of tag values (column values) for each provided
    /// tag key, where each returned value lives in a row matching the provided
    /// optional predicates and time range.
    ///
    /// As a special case, if `tag_keys` is empty then all distinct values for
    /// all columns (tag keys) are returned for the chunk.
    pub fn tag_values(
        &self,
        table_name: String,
        predicate: Predicate,
        tag_keys: &[ColumnName<'_>],
        found_tag_values: &BTreeMap<ColumnName<'_>, BTreeSet<&String>>,
    ) -> BTreeMap<ColumnName<'_>, BTreeSet<&String>> {
        // Lookup table by name and dispatch execution to the table for that
        // if the chunk's time range overlaps the requested time range.
        //
        // This method also has the ability to short-circuit execution against
        // columns if those columns only contain values that have already been
        // found.
        todo!();
    }
}

#[cfg(test)]
mod test {
    use std::collections::BTreeMap;

    use super::*;
    use crate::row_group::{ColumnType, RowGroup};
    use crate::{column::Column, BinaryExpr};

    #[test]
    fn add_remove_tables() {
        // Create a Chunk from a Table.
        let columns = vec![(
            "time".to_owned(),
            ColumnType::create_time(&[1_i64, 2, 3, 4, 5, 6]),
        )]
        .into_iter()
        .collect();
        let rg = RowGroup::new(6, columns);
        let table = Table::new("table_1", rg);
        let mut chunk = Chunk::new(22, table);

        assert_eq!(chunk.rows(), 6);
        assert_eq!(chunk.row_groups(), 1);
        assert_eq!(chunk.tables(), 1);
        let chunk_size = chunk.size();
        assert!(chunk_size > 0);

        // Add a row group to the same table in the Chunk.
        let columns = vec![("time", ColumnType::create_time(&[-2_i64, 2, 8]))]
            .into_iter()
            .map(|(k, v)| (k.to_owned(), v))
            .collect();
        let rg = RowGroup::new(3, columns);
        let rg_size = rg.size();
        chunk.upsert_table("table_1", rg);
        assert_eq!(chunk.size(), chunk_size + rg_size);

        assert_eq!(chunk.rows(), 9);
        assert_eq!(chunk.row_groups(), 2);
        assert_eq!(chunk.tables(), 1);

        // Add a row group to another table in the Chunk.
        let columns = vec![("time", ColumnType::create_time(&[-3_i64, 2]))]
            .into_iter()
            .map(|(k, v)| (k.to_owned(), v))
            .collect();
        let rg = RowGroup::new(2, columns);
        chunk.upsert_table("table_2", rg);

        assert_eq!(chunk.rows(), 11);
        assert_eq!(chunk.row_groups(), 3);
        assert_eq!(chunk.tables(), 2);

        // Drop table_1
        let chunk_size = chunk.size();
        chunk.drop_table("table_1");
        assert_eq!(chunk.rows(), 2);
        assert_eq!(chunk.row_groups(), 1);
        assert_eq!(chunk.tables(), 1);
        assert!(chunk.size() < chunk_size);

        // Drop table_2 - empty table
        chunk.drop_table("table_2");
        assert_eq!(chunk.rows(), 0);
        assert_eq!(chunk.row_groups(), 0);
        assert_eq!(chunk.tables(), 0);
        assert_eq!(chunk.size(), 64); // base size of `Chunk`

        // Drop table_2 - no-op
        chunk.drop_table("table_2");
        assert_eq!(chunk.rows(), 0);
        assert_eq!(chunk.row_groups(), 0);
        assert_eq!(chunk.tables(), 0);
    }

    #[test]
    fn table_names() {
        let columns = vec![
            ("time", ColumnType::create_time(&[1_i64, 2, 3, 4, 5, 6])),
            (
                "region",
                ColumnType::create_tag(&["west", "west", "east", "west", "south", "north"]),
            ),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_owned(), v))
        .collect::<BTreeMap<_, _>>();
        let rg = RowGroup::new(6, columns);
        let table = Table::new("table_1", rg);
        let mut chunk = Chunk::new(22, table);

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
                "time",
                ColumnType::Time(Column::from(&[100_i64, 200, 300, 400, 500, 600][..])),
            ),
            (
                "region",
                ColumnType::create_tag(&["west", "west", "east", "west", "south", "north"][..]),
            ),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_owned(), v))
        .collect::<BTreeMap<_, _>>();
        let rg = RowGroup::new(6, columns);
        chunk.upsert_table("table_2", rg);

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
