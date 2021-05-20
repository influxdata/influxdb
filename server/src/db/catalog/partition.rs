//! The catalog representation of a Partition

use std::{collections::BTreeMap, sync::Arc};

use chrono::{DateTime, Utc};
use snafu::OptionExt;

use data_types::chunk_metadata::ChunkSummary;
use data_types::partition_metadata::{
    PartitionSummary, UnaggregatedPartitionSummary, UnaggregatedTableSummary,
};
use query::predicate::Predicate;
use tracker::RwLock;

use crate::db::catalog::metrics::PartitionMetrics;

use super::{
    chunk::{Chunk, ChunkState},
    Error, Result, UnknownChunk, UnknownTable,
};

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

    /// Partition metrics
    metrics: PartitionMetrics,
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
    pub(crate) fn new(key: impl Into<String>, metrics: PartitionMetrics) -> Self {
        let key = key.into();

        let now = Utc::now();
        Self {
            key,
            tables: BTreeMap::new(),
            created_at: now,
            last_write_at: now,
            metrics,
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
    /// This will add a new chunk to the catalog and increases the chunk ID counter for that table-partition
    /// combination.
    ///
    /// Returns an error if the chunk is empty.
    pub fn create_open_chunk(
        &mut self,
        chunk: mutable_buffer::chunk::Chunk,
    ) -> Result<Arc<RwLock<Chunk>>> {
        let table_name: String = chunk.table_name().to_string();

        let table = self
            .tables
            .entry(table_name)
            .or_insert_with(PartitionTable::new);

        let chunk_id = table.next_chunk_id;

        // Technically this only causes an issue on the next upsert but
        // the MUB treats u32::MAX as a sentinel value
        assert_ne!(table.next_chunk_id, u32::MAX, "Chunk ID Overflow");

        table.next_chunk_id += 1;

        let chunk = Arc::new(self.metrics.new_lock(Chunk::new_open(
            chunk_id,
            &self.key,
            chunk,
            self.metrics.new_chunk_metrics(),
        )?));

        if table.chunks.insert(chunk_id, Arc::clone(&chunk)).is_some() {
            // A fundamental invariant has been violated - abort
            panic!("chunk already existed with id {}", chunk_id)
        }

        Ok(chunk)
    }

    /// Create new chunk that is only in object store (= parquet file).
    ///
    /// The table-specific chunk ID counter will be set to
    /// `max(current, chunk_id + 1)`.
    ///
    /// Fails if the chunk already exists.
    pub fn create_object_store_only_chunk(
        &mut self,
        chunk_id: u32,
        chunk: Arc<parquet_file::chunk::Chunk>,
    ) -> Result<Arc<RwLock<Chunk>>> {
        // workaround until https://github.com/influxdata/influxdb_iox/issues/1295 is fixed
        let table_name = chunk
            .table_names(None)
            .next()
            .expect("chunk must have exactly 1 table");

        let chunk = Arc::new(self.metrics.new_lock(Chunk::new_object_store_only(
            chunk_id,
            &self.key,
            chunk,
            self.metrics.new_chunk_metrics(),
        )));

        let table = self
            .tables
            .entry(table_name.clone())
            .or_insert_with(PartitionTable::new);
        match table.chunks.insert(chunk_id, Arc::clone(&chunk)) {
            Some(_) => Err(Error::ChunkAlreadyExists {
                partition_key: self.key.clone(),
                table_name,
                chunk_id,
            }),
            None => {
                // bump chunk ID counter
                table.next_chunk_id = table.next_chunk_id.max(chunk_id + 1);

                Ok(chunk)
            }
        }
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

    /// Return the unaggregated chunk summary information for tables
    /// in this partition
    pub fn unaggregated_summary(&self) -> UnaggregatedPartitionSummary {
        let tables = self
            .chunks()
            .map(|chunk| {
                let chunk = chunk.read();
                UnaggregatedTableSummary {
                    chunk_id: chunk.id(),
                    table: chunk.table_summary(),
                }
            })
            .collect();

        UnaggregatedPartitionSummary {
            key: self.key.to_string(),
            tables,
        }
    }

    /// Return a PartitionSummary for this partition
    pub fn summary(&self) -> PartitionSummary {
        let UnaggregatedPartitionSummary { key, tables } = self.unaggregated_summary();

        let table_summaries = tables.into_iter().map(|t| t.table);

        PartitionSummary::from_table_summaries(key, table_summaries)
    }

    /// Return chunk summaries for all chunks in this partition
    pub fn chunk_summaries(&self) -> impl Iterator<Item = ChunkSummary> + '_ {
        self.chunks().map(|x| x.read().summary())
    }

    pub fn metrics(&self) -> &PartitionMetrics {
        &self.metrics
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
