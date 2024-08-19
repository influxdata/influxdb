use std::collections::HashMap;
use std::sync::Arc;

pub type FileId = u64;

#[derive(Debug)]
pub struct FileIndex {
    index: HashMap<Arc<str>, HashMap<Arc<str>, Vec<FileId>>>,
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
        self.index
            .get_mut(&column)
            .and_then(|map| map.remove(&value));
    }

    /// Inserts an id for a given value and column or creates the entry if it does not yet exist
    pub fn insert(&mut self, column: Arc<str>, value: Arc<str>, id: FileId) {
        self.index
            .entry(column)
            .and_modify(|map| {
                map.entry(Arc::clone(&value))
                    .and_modify(|ids| {
                        if !ids.contains(&id) {
                            ids.push(id)
                        }
                    })
                    .or_insert(vec![id]);
            })
            .or_insert([(value, vec![id])].into());
    }

    /// Get all `FileId`s for a given column and value if they exist
    pub fn lookup(&self, column: Arc<str>, value: Arc<str>) -> &[FileId] {
        self.index
            .get(&column)
            .and_then(|map| map.get(&value).map(|vec| vec.as_slice()))
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
    index.insert(Arc::clone(&id), Arc::clone(&value), 9000);
    index.insert(Arc::clone(&id), Arc::clone(&value), 9001);
    // We want to make sure extra insertions of the same one aren't included
    index.insert(Arc::clone(&id), Arc::clone(&value), 9000);

    // Test that lookup works as expected
    assert_eq!(
        index.lookup(Arc::clone(&id), Arc::clone(&value)),
        &[9000, 9001]
    );

    // Test that removal works as expected
    index.remove(Arc::clone(&id), Arc::clone(&value));
    assert_eq!(index.lookup(Arc::clone(&id), Arc::clone(&value)), &[]);
}
