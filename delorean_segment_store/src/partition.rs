use std::collections::BTreeMap;

use crate::table::Table;

// The name of a measurement, i.e., a table name.
type MeasurementName = String;

pub struct Partition {
    // The partition key uniquely identifies this partition.
    key: String,

    // The set of tables within this partition. Each table is identified by
    // a measurement name.
    tables: BTreeMap<MeasurementName, Table>,
}
