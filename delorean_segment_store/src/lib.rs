#![deny(rust_2018_idioms)]
#![allow(dead_code)]
pub(crate) mod column;
pub(crate) mod partition;
pub(crate) mod segment;
pub(crate) mod table;

use std::collections::BTreeMap;

use partition::Partition;

/// The Segment Store is responsible for providing read access to partition data.
///
///
#[derive(Default)]
pub struct Store<'a> {
    // A mapping from database name (tenant id, bucket id etc) to a database.
    databases: BTreeMap<String, Database<'a>>,

    // The current total size of the store
    size: u64,
}

impl<'a> Store<'a> {
    // TODO(edd): accept a configuration of some sort.
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a new database to the store
    pub fn add_database(&mut self, id: String, database: Database<'a>) {
        self.size += database.size();
        self.databases.insert(id, database);
    }
}

// A database is scoped to a single tenant. Within a database there exists
// tables for measurements. There is a 1:1 mapping between a table and a
// measurement name.
#[derive(Default)]
pub struct Database<'a> {
    // The collection of partitions in the database.
    //
    // TODO(edd): need to implement efficient ways of skipping partitions.
    partitions: Vec<Partition<'a>>,

    // The current total size of the database.
    size: u64,
}

impl<'a> Database<'a> {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_partition(&mut self, partition: Partition<'_>) {
        todo!()
    }

    pub fn remove_partition(&mut self, partition: Partition<'_>) {
        todo!()
    }

    pub fn size(&self) -> u64 {
        self.size
    }
}
