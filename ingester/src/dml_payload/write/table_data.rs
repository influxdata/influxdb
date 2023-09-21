use data_types::TableId;

use super::partitioned_data::PartitionedData;

/// A container for all data for an individual table as part of a write
/// operation
#[derive(Debug, Clone)]
pub struct TableData {
    table: TableId,
    // The partitioned data for `table` in the write. Currently data is
    // partitioned in a way that each table has a single partition of
    // data associated with it per write
    partitioned_data: PartitionedData,
}

impl TableData {
    /// Constructs a new set of table associated data
    pub fn new(table: TableId, partitioned_data: PartitionedData) -> Self {
        Self {
            table,
            partitioned_data,
        }
    }

    /// Returns the [`TableId`] which the data is for
    pub fn table(&self) -> TableId {
        self.table
    }

    /// Returns a reference to the [`PartitionedData`] for the table
    pub fn partitioned_data(&self) -> &PartitionedData {
        &self.partitioned_data
    }

    /// Consumes `self`, returning the [`PartitionedData`] for the table
    pub fn into_partitioned_data(self) -> PartitionedData {
        self.partitioned_data
    }
}
