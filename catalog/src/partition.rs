//! The catalog representation of a Partition

#[derive(Debug, Default)]
/// IOx Catalog Partition
///
/// A partition contains multiple Chunks.
pub struct Partition {
    /// The partition key
    key: String,
}

impl Partition {
    /// Create a new partition catalog object.
    pub(crate) fn new(key: impl Into<String>) -> Self {
        let key = key.into();

        Self { key }
    }

    pub fn key(&self) -> &str {
        &self.key
    }
}
