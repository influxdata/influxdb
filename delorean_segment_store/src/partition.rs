use std::collections::BTreeMap;

use crate::table::Table;

// The name of a measurement, i.e., a table name.
type MeasurementName = String;

pub struct Partition<'a> {
    // The partition key uniquely identifies this partition.
    key: String,

    // Metadata about this partition.
    meta: MetaData,

    // The set of tables within this partition. Each table is identified by
    // a measurement name.
    tables: BTreeMap<MeasurementName, Table<'a>>,
}

impl<'a> Partition<'a> {
    pub fn new(key: String, table: Table<'a>) -> Self {
        let mut p = Self {
            key,
            meta: MetaData::new(&table),
            tables: BTreeMap::new(),
        };
        p.tables.insert(table.name().to_owned(), table);
        p
    }
}

// Partition metadata that is used to track statistics about the partition and
// whether it may contains data for a query.
struct MetaData {
    size: u64, // size in bytes of the partition
    rows: u64, // Total number of rows across all tables

    // The total time range of *all* data (across all tables) within this
    // partition.
    //
    // This would only be None if the partition contained only tables that had
    // no time-stamp column or the values were all NULL.
    time_range: Option<(i64, i64)>,
}

impl MetaData {
    pub fn new(table: &Table<'_>) -> Self {
        Self {
            size: table.size(),
            rows: table.rows(),
            time_range: table.time_range(),
        }
    }

    pub fn add_table(&mut self, table: &Table<'_>) {
        // Update size, rows, time_range
        todo!()
    }

    // invalidate should be called when a table is removed that impacts the
    // meta data.
    pub fn invalidate(&mut self) {
        // Update size, rows, time_range by linearly scanning all tables.
        todo!()
    }
}
