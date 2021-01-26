use std::collections::{btree_map::Entry, BTreeMap, BTreeSet};

use crate::row_group::RowGroup;
use crate::row_group::{ColumnName, Predicate};
use crate::schema::AggregateType;
use crate::table;
use crate::table::{ColumnSelection, Table};
use crate::Error;

type TableName = String;

/// A `Chunk` comprises a collection of `Tables` where every table must have a
/// unique identifier (name).
pub struct Chunk {
    // The unique identifier for this chunk.
    id: u32,

    // Metadata about the tables within this chunk.
    meta: MetaData,

    // The set of tables within this chunk. Each table is identified by a
    // measurement name.
    tables: BTreeMap<TableName, Table>,
}

impl Chunk {
    pub fn new(id: u32, table: Table) -> Self {
        let mut p = Self {
            id,
            meta: MetaData::new(&table),
            tables: BTreeMap::new(),
        };
        p.tables.insert(table.name().to_owned(), table);
        p
    }

    /// The chunk's ID.
    pub fn id(&self) -> u32 {
        self.id
    }

    /// The total size in bytes of all row groups in all tables in this chunk.
    pub fn size(&self) -> u64 {
        self.meta.size
    }

    /// The total number of rows in all row groups in all tables in this chunk.
    pub fn rows(&self) -> u64 {
        self.meta.rows
    }

    /// The total number of row groups in all tables in this chunk.
    pub fn row_groups(&self) -> usize {
        self.meta.row_groups
    }

    /// The total number of tables in this chunk.
    pub fn tables(&self) -> usize {
        self.tables.len()
    }

    /// Returns true if the chunk contains data for this table.
    pub fn has_table(&self, table_name: &str) -> bool {
        self.tables.contains_key(table_name)
    }

    /// Returns true if there are no tables under this chunk.
    pub fn is_empty(&self) -> bool {
        self.tables() == 0
    }

    /// Add a row_group to a table in the chunk, updating all Chunk meta data.
    pub fn upsert_table(&mut self, table_name: impl Into<String>, row_group: RowGroup) {
        // update meta data
        self.meta.update(&row_group);
        let table_name = table_name.into();

        match self.tables.entry(table_name.clone()) {
            Entry::Occupied(mut e) => {
                let table = e.get_mut();
                table.add_row_group(row_group);
            }
            Entry::Vacant(e) => {
                e.insert(Table::new(table_name, row_group));
            }
        };
    }

    /// Returns an iterator of lazily executed `read_filter` operations on the
    /// provided table for the specified column selections.
    ///
    /// Results may be filtered by conjunctive predicates.
    ///
    /// `None` indicates that the table was not found on the chunk, whilst a
    /// `ReadFilterResults` value that immediately yields `None` indicates that
    /// there were no matching results.
    ///
    /// TODO(edd): Alternatively we could assert the caller must have done
    /// appropriate pruning and that the table should always exist, meaning we
    /// can blow up here and not need to return an option.
    pub fn read_filter(
        &self,
        table_name: &str,
        predicate: &Predicate,
        select_columns: &ColumnSelection<'_>,
    ) -> Result<table::ReadFilterResults<'_>, Error> {
        // Lookup table by name and dispatch execution.
        match self.tables.get(table_name) {
            Some(table) => Ok(table.read_filter(select_columns, predicate)),
            None => crate::TableNotFound {
                table_name: table_name.to_owned(),
            }
            .fail(),
        }
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
        group_columns: &ColumnSelection<'_>,
        aggregates: &[(ColumnName<'_>, AggregateType)],
    ) -> Option<table::ReadAggregateResults<'_>> {
        // Lookup table by name and dispatch execution.
        self.tables
            .get(table_name)
            .map(|table| table.read_aggregate(predicate, group_columns, aggregates))
    }

    //
    // ---- Schema API queries
    //

    /// Returns the distinct set of table names that contain data satisfying the
    /// provided predicate.
    ///
    /// `exclude_table_names` can be used to provide a set of table names to
    /// skip, typically because they're already included in results from other
    /// chunks.
    pub fn table_names(
        &self,
        predicate: &Predicate,
        skip_table_names: &BTreeSet<&String>,
    ) -> BTreeSet<&String> {
        if predicate.is_empty() {
            return self
                .tables
                .keys()
                .filter(|&name| !skip_table_names.contains(name))
                .collect::<BTreeSet<_>>();
        }

        self.tables
            .iter()
            .filter_map(|(name, table)| {
                if skip_table_names.contains(name) {
                    return None;
                }

                match table.satisfies_predicate(predicate) {
                    true => Some(name),
                    false => None,
                }
            })
            .collect::<BTreeSet<_>>()
    }

    /// Returns the distinct set of tag keys (column names) matching the
    /// provided optional predicates and time range.
    pub fn tag_keys(
        &self,
        table_name: String,
        predicate: Predicate,
        found_keys: &BTreeSet<ColumnName<'_>>,
    ) -> BTreeSet<ColumnName<'_>> {
        // Lookup table by name and dispatch execution if the table's time range
        // overlaps the requested time range *and* there exists columns in the
        // table's schema that are *not* already found.
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

// `Chunk` metadata that is used to track statistics about the chunk and
// whether it could contain data necessary to execute a query.
struct MetaData {
    size: u64, // size in bytes of the chunk
    rows: u64, // Total number of rows across all tables

    row_groups: usize, // Total number of row groups across all tables in the chunk.

    // The total time range of *all* data (across all tables) within this
    // chunk.
    //
    // This would only be None if the chunk contained only tables that had
    // no time-stamp column or the values were all NULL.
    time_range: Option<(i64, i64)>,
}

impl MetaData {
    pub fn new(table: &Table) -> Self {
        Self {
            size: table.size(),
            rows: table.rows(),
            time_range: table.time_range(),
            row_groups: 1,
        }
    }

    /// Updates the meta data associated with the `Chunk` based on the provided
    /// row_group
    pub fn update(&mut self, table_data: &RowGroup) {
        self.size += table_data.size();
        self.rows += table_data.rows() as u64;
        self.row_groups += 1;

        let (them_min, them_max) = table_data.time_range();
        self.time_range = Some(match self.time_range {
            Some((this_min, this_max)) => (them_min.min(this_min), them_max.max(this_max)),
            None => (them_min, them_max),
        })
    }

    // invalidate should be called when a table is removed. All meta data must
    // be determined by asking each table in the chunk for its meta data.
    pub fn invalidate(&mut self) {
        // Update size, rows, time_range by linearly scanning all tables.
        todo!()
    }
}

#[cfg(test)]
mod test {
    use std::collections::BTreeMap;

    use super::*;
    use crate::row_group::{ColumnType, RowGroup};
    use crate::{column::Column, BinaryExpr};

    #[test]
    fn table_names() {
        let columns = vec![
            (
                "time",
                ColumnType::Time(Column::from(&[1_i64, 2, 3, 4, 5, 6][..])),
            ),
            (
                "region",
                ColumnType::Tag(Column::from(
                    &["west", "west", "east", "west", "south", "north"][..],
                )),
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
            &["table_2".to_owned()].iter().collect::<BTreeSet<&String>>(),
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
            &["table_1".to_owned()].iter().collect::<BTreeSet<&String>>(),
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
                ColumnType::Tag(Column::from(
                    &["west", "west", "east", "west", "south", "north"][..],
                )),
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
