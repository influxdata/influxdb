use data_types::partition_metadata::TableSummary;
use object_store::path::Path;

use std::mem;

/// Table that belongs to a chunk persisted in a parquet file in object store
#[derive(Debug, Clone)]
pub struct Table {
    /// Meta data of the table
    pub table_summary: TableSummary,

    /// Path in the object store. Format:
    ///  <writer id>/<database>/data/<partition key>/<chunk
    /// id>/<tablename>.parquet
    pub object_store_path: Path,
}

impl Table {
    pub fn new(meta: TableSummary, path: Path) -> Self {
        Self {
            table_summary: meta,
            object_store_path: path,
        }
    }

    pub fn has_table(&self, table_name: &str) -> bool {
        self.table_summary.has_table(table_name)
    }

    /// Return the approximate memory size of the table
    pub fn size(&self) -> usize {
        mem::size_of::<Self>()
            + self.table_summary.size()
            + mem::size_of_val(&self.object_store_path)
    }

    /// Return name of this table
    pub fn name(&self) -> String {
        self.table_summary.name.clone()
    }
}
