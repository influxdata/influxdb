//! This module contains the implementation of the InfluxDB IOx Metadata catalog
#![deny(rust_2018_idioms)]
#![warn(
    missing_debug_implementations,
    clippy::explicit_iter_loop,
    clippy::use_self,
    clippy::clone_on_ref_ptr
)]
use std::collections::{btree_map::Entry, BTreeMap};

use snafu::{OptionExt, Snafu};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("unknown partition: {}", partition_key))]
    UnknownPartition { partition_key: String },

    #[snafu(display("unknown chunk: {}:{}", partition_key, chunk_id))]
    UnknownChunk {
        partition_key: String,
        chunk_id: u32,
    },

    #[snafu(display("partition already exists: {}", partition_key))]
    PartitionAlreadyExists { partition_key: String },

    #[snafu(display("chunk already exists: {}:{}", partition_key, chunk_id))]
    ChunkAlreadyExists {
        partition_key: String,
        chunk_id: u32,
    },
}
pub type Result<T, E = Error> = std::result::Result<T, E>;

pub mod chunk;
pub mod partition;

use chunk::Chunk;
use partition::Partition;

/// InfluxDB IOx Metadata Catalog
///
/// The Catalog stores information such as which chunks exist, what
/// state they are in, and what objects on object store are used, etc.
///
/// The catalog is also responsible for (eventually) persisting this
/// information as well as ensuring that references between different
/// objects remain valid (e.g. that the `partition_key` field of all
/// Chunk's refer to valid partitions).
///
///
/// Note that the Partition does not "own" the Chunks (in the sense
/// there is not a list of chunks on the Partition object).  This is
/// so that the Catalog carefully controls when objects are created /
/// removed from the catalog. Since the catalog can passing out
/// references to `Partition`, we don't want callers to be able to add
/// new chunks to partitions in any way other than calling
/// `Catalog::create_chunk`
#[derive(Debug, Default)]
pub struct Catalog {
    /// The set of chunks in this database. The key is the partition
    /// key, the values are the chunks for that partition, in some
    /// arbitrary order
    chunks: BTreeMap<String, Vec<Chunk>>,

    /// key is partition_key, value is Partition
    partitions: BTreeMap<String, Partition>,
}

impl Catalog {
    pub fn new() -> Self {
        Self {
            ..Default::default()
        }
    }

    /// Return an immutable chunk reference given the specified partition and
    /// chunk id
    pub fn chunk(&self, partition_key: impl AsRef<str>, chunk_id: u32) -> Result<&Chunk> {
        let partition_key = partition_key.as_ref();

        self.partition_chunks(partition_key)?
            .find(|c| c.id() == chunk_id)
            .context(UnknownChunk {
                partition_key,
                chunk_id,
            })
    }

    /// Return an mutable chunk reference given the specified partition and
    /// chunk id
    pub fn chunk_mut(
        &mut self,
        partition_key: impl AsRef<str>,
        chunk_id: u32,
    ) -> Result<&mut Chunk> {
        let partition_key = partition_key.as_ref();
        let chunks = self
            .chunks
            .get_mut(partition_key)
            .context(UnknownPartition { partition_key })?;

        chunks
            .iter_mut()
            .find(|c| c.id() == chunk_id)
            .context(UnknownChunk {
                partition_key,
                chunk_id,
            })
    }

    /// Creates a new `Chunk` with id `id` within a specified Partition.
    ///
    /// This function also validates 'referential integrity' - aka
    /// that the partition referred to by this chunk exists in the
    /// catalog.
    pub fn create_chunk(&mut self, partition_key: impl AsRef<str>, chunk_id: u32) -> Result<()> {
        let partition_key = partition_key.as_ref();
        let chunks = self
            .chunks
            .get_mut(partition_key)
            .context(UnknownPartition { partition_key })?;

        // Ensure this chunk doesn't already exist
        if chunks.iter().any(|c| c.id() == chunk_id) {
            return ChunkAlreadyExists {
                partition_key,
                chunk_id,
            }
            .fail();
        }

        chunks.push(Chunk::new(partition_key, chunk_id));
        Ok(())
    }

    /// Removes the specified chunk from the catalog
    pub fn drop_chunk(&mut self, partition_key: impl AsRef<str>, chunk_id: u32) -> Result<()> {
        let partition_key = partition_key.as_ref();
        let chunks = self
            .chunks
            .get_mut(partition_key)
            .context(UnknownPartition { partition_key })?;

        let idx = chunks
            .iter()
            .enumerate()
            .filter(|(_, c)| c.id() == chunk_id)
            .map(|(i, _)| i)
            .next()
            .context(UnknownChunk {
                partition_key,
                chunk_id,
            })?;

        chunks.remove(idx);
        Ok(())
    }

    /// List all chunks in this database
    pub fn chunks(&self) -> impl Iterator<Item = &Chunk> {
        self.chunks.values().map(|chunks| chunks.iter()).flatten()
    }

    /// List all chunks in a particular partition
    pub fn partition_chunks(
        &self,
        partition_key: impl AsRef<str>,
    ) -> Result<impl Iterator<Item = &Chunk>> {
        let partition_key = partition_key.as_ref();
        let chunks = self
            .chunks
            .get(partition_key)
            .context(UnknownPartition { partition_key })?;

        Ok(chunks.iter())
    }

    // List all partitions in this dataase
    pub fn partitions(&self) -> impl Iterator<Item = &Partition> {
        self.partitions.values()
    }

    // Get a specific partition by name
    pub fn partition(&self, partition_key: impl AsRef<str>) -> Option<&Partition> {
        let partition_key = partition_key.as_ref();
        self.partitions.get(partition_key)
    }

    // Create a new partition in the catalog, returning an error if it already
    // exists
    pub fn create_partition(&mut self, partition_key: impl Into<String>) -> Result<()> {
        let partition_key = partition_key.into();

        let entry = self.partitions.entry(partition_key);
        match entry {
            Entry::Vacant(entry) => {
                let chunks = self.chunks.insert(entry.key().to_string(), Vec::new());
                assert!(chunks.is_none()); // otherwise the structures are out of sync

                let partition = Partition::new(entry.key());
                entry.insert(partition);
                Ok(())
            }
            Entry::Occupied(entry) => PartitionAlreadyExists {
                partition_key: entry.key(),
            }
            .fail(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn partition_create() {
        let mut catalog = Catalog::new();
        catalog.create_partition("p1").unwrap();

        let err = catalog.create_partition("p1").unwrap_err();
        assert_eq!(err.to_string(), "partition already exists: p1");
    }

    #[test]
    fn partition_get() {
        let mut catalog = Catalog::new();
        catalog.create_partition("p1").unwrap();
        catalog.create_partition("p2").unwrap();

        let p1 = catalog.partition("p1").unwrap();
        assert_eq!(p1.key(), "p1");

        let p2 = catalog.partition("p2").unwrap();
        assert_eq!(p2.key(), "p2");

        let p3 = catalog.partition("p3");
        assert!(p3.is_none());
    }

    #[test]
    fn partition_list() {
        let mut catalog = Catalog::new();

        assert_eq!(catalog.partitions().count(), 0);

        catalog.create_partition("p1").unwrap();
        catalog.create_partition("p2").unwrap();
        catalog.create_partition("p3").unwrap();

        let mut partition_keys: Vec<String> =
            catalog.partitions().map(|p| p.key().into()).collect();
        partition_keys.sort_unstable();

        assert_eq!(partition_keys, vec!["p1", "p2", "p3"]);
    }

    #[test]
    fn chunk_create_no_partition() {
        let mut catalog = Catalog::new();
        let err = catalog
            .create_chunk("non existent partition", 0)
            .unwrap_err();
        assert_eq!(err.to_string(), "unknown partition: non existent partition");
    }

    #[test]
    fn chunk_create() {
        let mut catalog = Catalog::new();
        catalog.create_partition("p1").unwrap();
        catalog.create_chunk("p1", 0).unwrap();
        catalog.create_chunk("p1", 1).unwrap();

        let c1_0 = catalog.chunk("p1", 0).unwrap();
        assert_eq!(c1_0.key(), "p1");
        assert_eq!(c1_0.id(), 0);

        let c1_0 = catalog.chunk_mut("p1", 0).unwrap();
        assert_eq!(c1_0.key(), "p1");
        assert_eq!(c1_0.id(), 0);

        let c1_1 = catalog.chunk("p1", 1).unwrap();
        assert_eq!(c1_1.key(), "p1");
        assert_eq!(c1_1.id(), 1);

        let err = catalog.chunk("p3", 0).unwrap_err();
        assert_eq!(err.to_string(), "unknown partition: p3");

        let err = catalog.chunk("p1", 100).unwrap_err();
        assert_eq!(err.to_string(), "unknown chunk: p1:100");
    }

    #[test]
    fn chunk_create_dupe() {
        let mut catalog = Catalog::new();
        catalog.create_partition("p1").unwrap();
        catalog.create_chunk("p1", 0).unwrap();

        let res = catalog.create_chunk("p1", 0).unwrap_err();
        assert_eq!(res.to_string(), "chunk already exists: p1:0");
    }

    #[test]
    fn chunk_list() {
        let mut catalog = Catalog::new();
        assert_eq!(catalog.chunks().count(), 0);

        catalog.create_partition("p1").unwrap();
        catalog.create_chunk("p1", 0).unwrap();
        catalog.create_chunk("p1", 1).unwrap();

        catalog.create_partition("p2").unwrap();
        catalog.create_chunk("p2", 100).unwrap();

        assert_eq!(
            chunk_strings(&catalog),
            vec!["Chunk p1:0", "Chunk p1:1", "Chunk p2:100"]
        );

        assert_eq!(
            partition_chunk_strings(&catalog, "p1"),
            vec!["Chunk p1:0", "Chunk p1:1"]
        );
        assert_eq!(
            partition_chunk_strings(&catalog, "p2"),
            vec!["Chunk p2:100"]
        );
    }

    #[test]
    fn chunk_list_err() {
        let catalog = Catalog::new();

        match catalog.partition_chunks("p3") {
            Err(err) => assert_eq!(err.to_string(), "unknown partition: p3"),
            Ok(_) => panic!("unexpected success"),
        };
    }

    fn chunk_strings(catalog: &Catalog) -> Vec<String> {
        let mut chunks: Vec<String> = catalog
            .chunks()
            .map(|c| format!("Chunk {}:{}", c.key(), c.id()))
            .collect();
        chunks.sort_unstable();

        chunks
    }

    fn partition_chunk_strings(catalog: &Catalog, partition_key: &str) -> Vec<String> {
        let mut chunks: Vec<String> = catalog
            .partition_chunks(partition_key)
            .unwrap()
            .map(|c| format!("Chunk {}:{}", c.key(), c.id()))
            .collect();
        chunks.sort_unstable();

        chunks
    }

    #[test]
    fn chunk_drop() {
        let mut catalog = Catalog::new();

        catalog.create_partition("p1").unwrap();
        catalog.create_chunk("p1", 0).unwrap();
        catalog.create_chunk("p1", 1).unwrap();

        catalog.create_partition("p2").unwrap();
        catalog.create_chunk("p2", 0).unwrap();

        assert_eq!(catalog.chunks().count(), 3);

        catalog.drop_chunk("p1", 1).unwrap();
        catalog.chunk("p1", 1).unwrap_err(); // chunk is gone
        assert_eq!(catalog.chunks().count(), 2);

        catalog.drop_chunk("p2", 0).unwrap();
        catalog.chunk("p2", 0).unwrap_err(); // chunk is gone
        assert_eq!(catalog.chunks().count(), 1);
    }

    #[test]
    fn chunk_drop_non_existent_partition() {
        let mut catalog = Catalog::new();
        let err = catalog.drop_chunk("p3", 0).unwrap_err();
        assert_eq!(err.to_string(), "unknown partition: p3");
    }

    #[test]
    fn chunk_drop_non_existent_chunk() {
        let mut catalog = Catalog::new();
        catalog.create_partition("p3").unwrap();

        let err = catalog.drop_chunk("p3", 0).unwrap_err();
        assert_eq!(err.to_string(), "unknown chunk: p3:0");
    }

    #[test]
    fn chunk_recreate_dropped() {
        let mut catalog = Catalog::new();

        catalog.create_partition("p1").unwrap();
        catalog.create_chunk("p1", 0).unwrap();
        catalog.create_chunk("p1", 1).unwrap();
        assert_eq!(catalog.chunks().count(), 2);

        catalog.drop_chunk("p1", 0).unwrap();
        assert_eq!(catalog.chunks().count(), 1);

        // should be ok to recreate
        catalog.create_chunk("p1", 0).unwrap();
        assert_eq!(catalog.chunks().count(), 2);
    }
}
