//! The catalog representation of a Partition

use std::{collections::BTreeMap, sync::Arc};

use super::{
    chunk::{Chunk, ChunkState},
    Result, UnknownChunk, UnknownTable,
};
use chrono::{DateTime, Utc};
use data_types::partition_metadata::PartitionSummary;
use data_types::{chunk::ChunkSummary, server_id::ServerId};
use entry::{ClockValue, TableBatch};
use query::predicate::Predicate;
use snafu::OptionExt;
use tracker::{LockTracker, MemRegistry, RwLock};

/// IOx Catalog Partition
///
/// A partition contains multiple Chunks.
#[derive(Debug)]
pub struct Partition {
    /// The partition key
    key: String,

    /// Tables within this partition
    tables: BTreeMap<String, PartitionTable>,

    /// When this partition was created
    created_at: DateTime<Utc>,

    /// the last time at which write was made to this
    /// partition. Partition::new initializes this to now.
    last_write_at: DateTime<Utc>,

    /// Lock Tracker for chunk-level locks
    lock_tracker: LockTracker,
}

impl Partition {
    /// Return the partition_key of this Partition
    pub fn key(&self) -> &str {
        &self.key
    }
}

impl Partition {
    /// Create a new partition catalog object.
    ///
    /// This function is not pub because `Partition`s should be
    /// created using the interfaces on [`Catalog`](crate::db::catalog::Catalog) and not
    /// instantiated directly.
    pub(crate) fn new(key: impl Into<String>) -> Self {
        let key = key.into();

        let now = Utc::now();
        Self {
            key,
            tables: BTreeMap::new(),
            created_at: now,
            last_write_at: now,
            lock_tracker: Default::default(),
        }
    }

    /// Update the last write time to now
    pub fn update_last_write_at(&mut self) {
        self.last_write_at = Utc::now();
    }

    /// Return the time at which this partition was created
    pub fn created_at(&self) -> DateTime<Utc> {
        self.created_at
    }

    /// Return the time at which the last write was written to this partititon
    pub fn last_write_at(&self) -> DateTime<Utc> {
        self.last_write_at
    }

    /// Create a new Chunk in the open state.
    ///
    /// This will draw a new chunk ID, call [`Chunk::new_open`](Chunk::new_open) using the provided parameters, and will
    /// insert the chunk into this partition.
    pub fn create_open_chunk(
        &mut self,
        batch: TableBatch<'_>,
        clock_value: ClockValue,
        server_id: ServerId,
        memory_registry: &MemRegistry,
    ) -> Result<Arc<RwLock<Chunk>>> {
        let table_name: String = batch.name().into();

        let table = self
            .tables
            .entry(table_name)
            .or_insert_with(PartitionTable::new);
        let chunk_id = table.next_chunk_id;
        table.next_chunk_id += 1;

        let chunk = Arc::new(self.lock_tracker.new_lock(Chunk::new_open(
            batch,
            &self.key,
            chunk_id,
            clock_value,
            server_id,
            memory_registry,
        )?));

        if table.chunks.insert(chunk_id, Arc::clone(&chunk)).is_some() {
            // A fundamental invariant has been violated - abort
            panic!("chunk already existed with id {}", chunk_id)
        }

        Ok(chunk)
    }

    /// Drop the specified chunk
    pub fn drop_chunk(&mut self, table_name: impl Into<String>, chunk_id: u32) -> Result<()> {
        let table_name = table_name.into();

        match self.tables.get_mut(&table_name) {
            Some(table) => match table.chunks.remove(&chunk_id) {
                Some(_) => Ok(()),
                None => UnknownChunk {
                    partition_key: self.key(),
                    table_name,
                    chunk_id,
                }
                .fail(),
            },
            None => UnknownTable {
                partition_key: self.key(),
                table_name,
            }
            .fail(),
        }
    }

    /// return the first currently open chunk, if any
    pub fn open_chunk(&self, table_name: impl Into<String>) -> Result<Option<Arc<RwLock<Chunk>>>> {
        let table_name = table_name.into();

        match self.tables.get(&table_name) {
            Some(table) => Ok(table
                .chunks
                .values()
                .find(|chunk| {
                    let chunk = chunk.read();
                    matches!(chunk.state(), ChunkState::Open(_))
                })
                .cloned()),
            None => UnknownTable {
                partition_key: self.key(),
                table_name,
            }
            .fail(),
        }
    }

    /// Return an immutable chunk reference by chunk id
    pub fn chunk(
        &self,
        table_name: impl Into<String>,
        chunk_id: u32,
    ) -> Result<Arc<RwLock<Chunk>>> {
        let table_name = table_name.into();

        match self.tables.get(&table_name) {
            Some(table) => table.chunks.get(&chunk_id).cloned().context(UnknownChunk {
                partition_key: self.key(),
                table_name,
                chunk_id,
            }),
            None => UnknownTable {
                partition_key: self.key(),
                table_name,
            }
            .fail(),
        }
    }

    /// Return a iterator over chunks in this partition
    pub fn chunks(&self) -> impl Iterator<Item = &Arc<RwLock<Chunk>>> {
        self.tables.values().flat_map(|table| table.chunks.values())
    }

    /// Return an iterator over chunks in this partition that
    /// may pass the provided predicate
    pub fn filtered_chunks<'a>(
        &'a self,
        predicate: &'a Predicate,
    ) -> impl Iterator<Item = &Arc<RwLock<Chunk>>> + 'a {
        self.tables
            .iter()
            .filter(move |(table_name, _)| predicate.should_include_table(table_name))
            .flat_map(|(_, table)| table.chunks.values())
    }

    /// Return a PartitionSummary for this partition
    pub fn summary(&self) -> PartitionSummary {
        let table_summaries = self
            .chunks()
            .map(|chunk| {
                let chunk = chunk.read();
                chunk.table_summary()
            })
            .collect();

        PartitionSummary::from_table_summaries(&self.key, table_summaries)
    }

    /// Return chunk summaries for all chunks in this partition
    pub fn chunk_summaries(&self) -> impl Iterator<Item = ChunkSummary> + '_ {
        self.chunks().map(|x| x.read().summary())
    }
}

#[derive(Debug)]
struct PartitionTable {
    /// What the next chunk id is
    next_chunk_id: u32,

    /// The chunks that make up this partition, indexed by id
    chunks: BTreeMap<u32, Arc<RwLock<Chunk>>>,
}

impl PartitionTable {
    fn new() -> Self {
        Self {
            next_chunk_id: 0,
            chunks: BTreeMap::new(),
        }
    }
}
