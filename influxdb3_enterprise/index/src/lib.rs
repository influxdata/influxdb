pub mod memory;

use influxdb3_id::{ParquetFileId, SerdeVecMap};
use serde::{Deserialize, Serialize};
use twox_hash::XxHash64;

const INDEX_HASH_SEED: u64 = 0;

pub(crate) fn hash_for_index(data: &[u8]) -> u64 {
    XxHash64::oneshot(INDEX_HASH_SEED, data)
}

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
    pub index: SerdeVecMap<u64, SerdeVecMap<u64, Vec<ParquetFileId>>>,
}

impl FileIndex {
    /// Create a new empty `FileIndex`
    pub fn new() -> Self {
        Self {
            index: SerdeVecMap::new(),
        }
    }

    /// Inserts an id for a given value and column or creates the entry if it does not yet exist
    pub fn insert(&mut self, column: &str, value: &str, file_id: &ParquetFileId) {
        let column = hash_for_index(column.as_bytes());
        let value = hash_for_index(value.as_bytes());
        let ids = self
            .index
            .entry(column)
            .or_default()
            .entry(value)
            .or_default();
        if !ids.contains(file_id) {
            ids.push(*file_id);
        }
    }

    /// Get all `object_store::Path`s for a given column and value if they exist
    pub fn lookup(&self, column: &str, value: &str) -> &[ParquetFileId] {
        let column = hash_for_index(column.as_bytes());
        let value = hash_for_index(value.as_bytes());
        self.index
            .get(&column)
            .and_then(|m| m.get(&value))
            .map(|v| v.as_slice())
            .unwrap_or_default()
    }
}

impl Default for FileIndex {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use pretty_assertions::assert_eq;

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

    #[test]
    /// Test that the output looks like we'd expect and we get back the same
    /// data
    fn file_index_json() {
        let mut index = FileIndex::new();
        let id = "id";
        let value = "1";

        // Test that insertion works as expected
        index.insert(id, value, &ParquetFileId::from(9000));
        index.insert(id, value, &ParquetFileId::from(9001));
        // We want to make sure extra insertions of the same one aren't included
        index.insert(id, value, &ParquetFileId::from(9000));

        let string = serde_json::to_string(&index).unwrap();
        assert_eq!(
            "{\"index\":[[6524628971699625766,[[13237225503670494420,[9000,9001]]]]]}",
            string
        );
        let index: FileIndex = serde_json::from_str(&string).unwrap();
        assert_eq!(index.lookup(id, value), &[9000.into(), 9001.into()]);
    }
}
