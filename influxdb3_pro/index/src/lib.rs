pub mod memory;

use hashbrown::HashMap;
use influxdb3_write::ParquetFileId;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
/// The `FileIndex` is an index of Column/Values to an `object_store::Path`. It's expected that
/// other code will handle what database this index is mapped too. Consumers of this API can insert
/// a column and a value and a path and retrieve a list of ids with a lookup of the column and
/// value.
/// The idea is that if a query like 'select * from foo where host="us-east-1"' comes in, instead of
/// querying every single parquet file and the buffer, we instead will look in the index and only
/// query the parquet files where 'host="us-east-1"'. This can lead to speed ups so that Datafusion
/// doesn't have to scan and read parquet files only to toss them out of what's queryable.
pub struct FileIndex {
    /// A map of column name to column value to parquet file ids that contain that column value
    pub index: HashMap<String, HashMap<String, Vec<ParquetFileId>>>,
}

impl FileIndex {
    /// Create a new empty `FileIndex`
    pub fn new() -> Self {
        Self {
            index: HashMap::new(),
        }
    }

    /// Inserts an id for a given value and column or creates the entry if it does not yet exist
    pub fn insert(&mut self, column: &str, value: &str, file_id: &ParquetFileId) {
        let ids = self
            .index
            .entry_ref(column)
            .or_default()
            .entry_ref(value)
            .or_insert_with(Vec::new);
        if !ids.contains(file_id) {
            ids.push(*file_id);
        }
    }

    /// Get all `object_store::Path`s for a given column and value if they exist
    pub fn lookup(&self, column: &str, value: &str) -> &[ParquetFileId] {
        self.index
            .get(column)
            .and_then(|m| m.get(value))
            .map(|v| v.as_slice())
            .unwrap_or_default()
    }
}

impl Default for FileIndex {
    fn default() -> Self {
        Self::new()
    }
}

#[test]
/// Test that `new`, `remove`, `insert`, and `lookup` work as expected
fn file_index() {
    let mut index = FileIndex::new();
    let id = "id";
    let value = "1";

    // Test that insertion works as expected
    index.insert(id, value, &ParquetFileId::from(9000));
    index.insert(id, value, &ParquetFileId::from(9001));
    // We want to make sure extra insertions of the same one aren't included
    index.insert(id, value, &ParquetFileId::from(9000));

    // Test that lookup works as expected
    assert_eq!(index.lookup(id, value), &[9000.into(), 9001.into()]);
}
