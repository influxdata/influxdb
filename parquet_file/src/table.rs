use object_store::path::Path;
use read_buffer::table::MetaData;

type TableName = String;

/// Table that belongs to a chunk persisted in a parquet file in object store
#[derive(Debug, Clone)]
pub struct Table {
    /// Name of the table
    name: TableName,

    /// Path in the object store. Format:
    ///  <writer id>/<database>/data/<partition key>/<chunk
    /// id>/<tablename>.parquet
    pub object_store_path: Path,

    /// Meta data of the table
    /// Reuse the one defined for read buffer
    pub meta_data: Vec<MetaData>,
}

impl Table {
    pub fn new(table_name: &str, path: Path, meta: Vec<MetaData>) -> Self {
        Self {
            name: table_name.to_string(),
            object_store_path: path,
            meta_data: meta,
        }
    }
}
