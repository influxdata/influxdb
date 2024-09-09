use influxdb3_write::ParquetFileId;
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Debug)]
/// The `FileIndex` is an index of Column/Values to an `object_store::Path`. It's expected that
/// other code will handle what database this index is mapped too. Consumers of this API can insert
/// a column and a value and a path and retrieve a list of ids with a lookup of the column and
/// value.
/// The idea is that if a query like 'select * from foo where host="us-east-1"' comes in, instead of
/// querying every single parquet file and the buffer, we instead will look in the index and only
/// query the parquet files where 'host="us-east-1"'. This can lead to speed ups so that Datafusion
/// doesn't have to scan and read parquet files only to toss them out of what's queryable.
pub struct FileIndex {
    /// This is `HashMap<(Column Name, Value), Vec<ParquetFileId>>`
    index: HashMap<(Arc<str>, Arc<str>), Vec<ParquetFileId>>,
}

impl FileIndex {
    /// Create a new empty `FileIndex`
    pub fn new() -> Self {
        Self {
            index: HashMap::new(),
        }
    }

    /// Removes the file ids for a given column if they exist
    pub fn remove(&mut self, column: Arc<str>, value: Arc<str>) {
        self.index.remove(&(column, value));
    }

    /// Inserts an id for a given value and column or creates the entry if it does not yet exist
    pub fn insert(&mut self, column: Arc<str>, value: Arc<str>, file_id: &ParquetFileId) {
        self.index
            .entry((column, value))
            .and_modify(|ids| {
                if !ids.contains(file_id) {
                    ids.push(*file_id)
                }
            })
            .or_insert(vec![*file_id]);
    }

    /// Get all `object_store::Path`s for a given column and value if they exist
    pub fn lookup(&self, column: Arc<str>, value: Arc<str>) -> &[ParquetFileId] {
        self.index
            .get(&(column, value))
            .map(|vec| vec.as_slice())
            .unwrap_or(&[])
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
    let id: Arc<str> = Arc::from("id");
    let value: Arc<str> = Arc::from("1");

    // Test that insertion works as expected
    index.insert(
        Arc::clone(&id),
        Arc::clone(&value),
        &ParquetFileId::from(9000),
    );
    index.insert(
        Arc::clone(&id),
        Arc::clone(&value),
        &ParquetFileId::from(9001),
    );
    // We want to make sure extra insertions of the same one aren't included
    index.insert(
        Arc::clone(&id),
        Arc::clone(&value),
        &ParquetFileId::from(9000),
    );

    // Test that lookup works as expected
    assert_eq!(
        index.lookup(Arc::clone(&id), Arc::clone(&value)),
        &[9000.into(), 9001.into()]
    );

    // Test that removal works as expected
    index.remove(Arc::clone(&id), Arc::clone(&value));
    assert_eq!(index.lookup(Arc::clone(&id), Arc::clone(&value)), &[]);
}
