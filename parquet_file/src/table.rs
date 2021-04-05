use data_types::partition_metadata::TableSummary;
use object_store::path::Path;

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
}
