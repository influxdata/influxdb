use std::sync::Arc;

use chrono::{DateTime, Utc};
use data_types::{
    chunk::{ChunkStorage, ChunkSummary},
    partition_metadata::TableSummary,
    server_id::ServerId,
};
use internal_types::entry::{ClockValue, TableBatch};
use mutable_buffer::chunk::Chunk as MBChunk;
use parquet_file::chunk::Chunk as ParquetChunk;
use read_buffer::Chunk as ReadBufferChunk;
use snafu::ResultExt;

use super::{InternalChunkState, OpenChunk, Result};
use tracker::MemRegistry;

/// The state a Chunk is in and what its underlying backing storage is
#[derive(Debug)]
pub enum ChunkState {
    /// An invalid chunk state that should not be externally observed
    ///
    /// Used internally to allow moving data between enum variants
    Invalid,

    /// Chunk can accept new writes
    Open(MBChunk),

    /// Chunk can still accept new writes, but will likely be closed soon
    Closing(MBChunk),

    /// Chunk is closed for new writes, and is actively moving to the read
    /// buffer
    Moving(Arc<MBChunk>),

    /// Chunk has been completely loaded in the read buffer
    Moved(Arc<ReadBufferChunk>),

    // Chunk is actively writing to object store
    WritingToObjectStore(Arc<ReadBufferChunk>),

    // Chunk has been completely written into object store
    WrittenToObjectStore(Arc<ReadBufferChunk>, Arc<ParquetChunk>),
    // Todo : There must be another state for chunk that only in object store
    // ticket #1314
}

impl ChunkState {
    pub fn name(&self) -> &'static str {
        match self {
            Self::Invalid => "Invalid",
            Self::Open(_) => "Open",
            Self::Closing(_) => "Closing",
            Self::Moving(_) => "Moving",
            Self::Moved(_) => "Moved",
            Self::WritingToObjectStore(_) => "Writing to Object Store",
            Self::WrittenToObjectStore(_, _) => "Written to Object Store",
        }
    }

    /// A more concise version of `name`, more suited to metric labels.
    pub fn metric_label(&self) -> &'static str {
        match self {
            Self::Invalid => "invalid",
            Self::Open(_) => "open",
            Self::Closing(_) => "closing",
            Self::Moving(_) => "moving",
            Self::Moved(_) => "moved",
            Self::WritingToObjectStore(_) => "writing_os",
            Self::WrittenToObjectStore(_, _) => "os",
        }
    }
}

/// The catalog representation of a Chunk in IOx. Note that a chunk
/// may exist in several physical locations at any given time (e.g. in
/// mutable buffer and in read buffer)
#[derive(Debug)]
pub struct Chunk {
    /// What partition does the chunk belong to?
    partition_key: Arc<String>,

    /// What table does the chunk belong to?
    table_name: Arc<String>,

    /// The ID of the chunk
    id: u32,

    /// The state of this chunk
    state: ChunkState,

    /// Time at which the first data was written into this chunk. Note
    /// this is not the same as the timestamps on the data itself
    time_of_first_write: Option<DateTime<Utc>>,

    /// Most recent time at which data write was initiated into this
    /// chunk. Note this is not the same as the timestamps on the data
    /// itself
    time_of_last_write: Option<DateTime<Utc>>,

    /// Time at which this chunk was maked as closing. Note this is
    /// not the same as the timestamps on the data itself
    time_closing: Option<DateTime<Utc>>,
}

macro_rules! unexpected_state {
    ($SELF: expr, $OP: expr, $EXPECTED: expr, $STATE: expr) => {
        InternalChunkState {
            partition_key: $SELF.partition_key.as_str(),
            table_name: $SELF.table_name.as_str(),
            chunk_id: $SELF.id,
            operation: $OP,
            expected: $EXPECTED,
            actual: $STATE.name(),
        }
        .fail()
    };
}

impl Chunk {
    /// Creates a new open chunk from given table batch.
    ///
    /// This consists of the following steps:
    ///
    /// 1. [`MutableBuffer`](MBChunk) is created using this ID and the passed memory registry
    /// 2. the passed table batch is written into the buffer using the provided clock value
    /// 3. a new open chunk is created using the buffer
    /// 4. a write is recorded (see [`record_write`](Self::record_write))
    pub(crate) fn new_open(
        batch: TableBatch<'_>,
        partition_key: impl Into<String>,
        id: u32,
        clock_value: ClockValue,
        server_id: ServerId,
        memory_registry: &MemRegistry,
    ) -> Result<Self> {
        let table_name = batch.name().to_string();
        let partition_key: String = partition_key.into();

        let mut mb = mutable_buffer::chunk::Chunk::new(id, memory_registry);
        mb.write_table_batches(clock_value, server_id, &[batch])
            .context(OpenChunk {
                partition_key: partition_key.clone(),
                chunk_id: id,
            })?;

        let state = ChunkState::Open(mb);
        let mut chunk = Self {
            partition_key: Arc::new(partition_key),
            table_name: Arc::new(table_name),
            id,
            state,
            time_of_first_write: None,
            time_of_last_write: None,
            time_closing: None,
        };
        chunk.record_write();
        Ok(chunk)
    }

    /// Used for testing
    #[cfg(test)]
    pub(crate) fn set_timestamps(
        &mut self,
        time_of_first_write: Option<DateTime<Utc>>,
        time_of_last_write: Option<DateTime<Utc>>,
    ) {
        self.time_of_first_write = time_of_first_write;
        self.time_of_last_write = time_of_last_write;
    }

    pub fn id(&self) -> u32 {
        self.id
    }

    pub fn key(&self) -> &str {
        self.partition_key.as_ref()
    }

    pub fn table_name(&self) -> &str {
        self.table_name.as_ref()
    }

    pub fn state(&self) -> &ChunkState {
        &self.state
    }

    pub fn time_of_first_write(&self) -> Option<DateTime<Utc>> {
        self.time_of_first_write
    }

    pub fn time_of_last_write(&self) -> Option<DateTime<Utc>> {
        self.time_of_last_write
    }

    pub fn time_closing(&self) -> Option<DateTime<Utc>> {
        self.time_closing
    }

    /// Update the write timestamps for this chunk
    pub fn record_write(&mut self) {
        let now = Utc::now();
        if self.time_of_first_write.is_none() {
            self.time_of_first_write = Some(now);
        }
        self.time_of_last_write = Some(now);
    }

    /// Return ChunkSummary metadata for this chunk
    pub fn summary(&self) -> ChunkSummary {
        let (estimated_bytes, row_count, storage) = match &self.state {
            ChunkState::Invalid => panic!("invalid chunk state"),
            ChunkState::Open(chunk) => {
                (chunk.size(), chunk.rows(), ChunkStorage::OpenMutableBuffer)
            }
            ChunkState::Closing(chunk) => (
                chunk.size(),
                chunk.rows(),
                ChunkStorage::ClosedMutableBuffer,
            ),
            ChunkState::Moving(chunk) => (
                chunk.size(),
                chunk.rows(),
                ChunkStorage::ClosedMutableBuffer,
            ),
            ChunkState::Moved(chunk) => (
                chunk.size(),
                chunk.rows() as usize,
                ChunkStorage::ReadBuffer,
            ),
            ChunkState::WritingToObjectStore(chunk) => (
                chunk.size(),
                chunk.rows() as usize,
                ChunkStorage::ReadBufferAndObjectStore,
            ),
            ChunkState::WrittenToObjectStore(chunk, parquet_chunk) => (
                chunk.size() + parquet_chunk.size(),
                chunk.rows() as usize,
                ChunkStorage::ReadBufferAndObjectStore,
            ),
        };

        ChunkSummary {
            partition_key: Arc::clone(&self.partition_key),
            table_name: Arc::clone(&self.table_name),
            id: self.id,
            storage,
            estimated_bytes,
            row_count,
            time_of_first_write: self.time_of_first_write,
            time_of_last_write: self.time_of_last_write,
            time_closing: self.time_closing,
        }
    }

    /// Return TableSummary metadata.
    pub fn table_summary(&self) -> TableSummary {
        match &self.state {
            ChunkState::Invalid => panic!("invalid chunk state"),
            ChunkState::Open(chunk) | ChunkState::Closing(chunk) => {
                let mut summaries = chunk.table_summaries();
                assert_eq!(summaries.len(), 1);
                summaries.remove(0)
            }
            ChunkState::Moving(chunk) => {
                let mut summaries = chunk.table_summaries();
                assert_eq!(summaries.len(), 1);
                summaries.remove(0)
            }
            ChunkState::Moved(chunk) => {
                let mut summaries = chunk.table_summaries();
                assert_eq!(summaries.len(), 1);
                summaries.remove(0)
            }
            ChunkState::WritingToObjectStore(chunk) => {
                let mut summaries = chunk.table_summaries();
                assert_eq!(summaries.len(), 1);
                summaries.remove(0)
            }
            ChunkState::WrittenToObjectStore(chunk, _) => {
                let mut summaries = chunk.table_summaries();
                assert_eq!(summaries.len(), 1);
                summaries.remove(0)
            }
        }
    }

    /// Returns an approximation of the amount of process memory consumed by the
    /// chunk
    pub fn size(&self) -> usize {
        match &self.state {
            ChunkState::Invalid => 0,
            ChunkState::Open(chunk) | ChunkState::Closing(chunk) => chunk.size(),
            ChunkState::Moving(chunk) => chunk.size(),
            ChunkState::Moved(chunk) => chunk.size() as usize,
            ChunkState::WritingToObjectStore(chunk) => chunk.size() as usize,
            ChunkState::WrittenToObjectStore(chunk, parquet_chunk) => {
                parquet_chunk.size() + chunk.size() as usize
            }
        }
    }

    /// Returns a mutable reference to the mutable buffer storage for
    /// chunks in the Open or Closing state
    ///
    /// Must be in open or closing state
    pub fn mutable_buffer(&mut self) -> Result<&mut MBChunk> {
        match &mut self.state {
            ChunkState::Open(chunk) => Ok(chunk),
            ChunkState::Closing(chunk) => Ok(chunk),
            state => unexpected_state!(self, "mutable buffer reference", "Open or Closing", state),
        }
    }

    /// Set the chunk to the Closing state
    pub fn set_closing(&mut self) -> Result<()> {
        let mut s = ChunkState::Invalid;
        std::mem::swap(&mut s, &mut self.state);

        match s {
            ChunkState::Open(s) | ChunkState::Closing(s) => {
                assert!(self.time_closing.is_none());
                self.time_closing = Some(Utc::now());
                self.state = ChunkState::Closing(s);
                Ok(())
            }
            state => {
                self.state = state;
                unexpected_state!(self, "setting closing", "Open or Closing", &self.state)
            }
        }
    }

    /// Set the chunk to the Moving state, returning a handle to the underlying
    /// storage
    pub fn set_moving(&mut self) -> Result<Arc<MBChunk>> {
        let mut s = ChunkState::Invalid;
        std::mem::swap(&mut s, &mut self.state);

        match s {
            ChunkState::Open(chunk) | ChunkState::Closing(chunk) => {
                let chunk = Arc::new(chunk);
                self.state = ChunkState::Moving(Arc::clone(&chunk));
                Ok(chunk)
            }
            state => {
                self.state = state;
                unexpected_state!(self, "setting moving", "Open or Closing", &self.state)
            }
        }
    }

    /// Set the chunk in the Moved state, setting the underlying
    /// storage handle to db, and discarding the underlying mutable buffer
    /// storage.
    pub fn set_moved(&mut self, chunk: Arc<ReadBufferChunk>) -> Result<()> {
        let mut s = ChunkState::Invalid;
        std::mem::swap(&mut s, &mut self.state);

        match s {
            ChunkState::Moving(_) => {
                self.state = ChunkState::Moved(chunk);
                Ok(())
            }
            state => {
                self.state = state;
                unexpected_state!(self, "setting moved", "Moving", &self.state)
            }
        }
    }

    /// Set the chunk to the MovingToObjectStore state
    pub fn set_writing_to_object_store(&mut self) -> Result<Arc<ReadBufferChunk>> {
        let mut s = ChunkState::Invalid;
        std::mem::swap(&mut s, &mut self.state);

        match s {
            ChunkState::Moved(db) => {
                self.state = ChunkState::WritingToObjectStore(Arc::clone(&db));
                Ok(db)
            }
            state => {
                self.state = state;
                unexpected_state!(self, "setting object store", "Moved", &self.state)
            }
        }
    }

    /// Set the chunk to the MovedToObjectStore state, returning a handle to the
    /// underlying storage
    pub fn set_written_to_object_store(&mut self, chunk: Arc<ParquetChunk>) -> Result<()> {
        let mut s = ChunkState::Invalid;
        std::mem::swap(&mut s, &mut self.state);

        match s {
            ChunkState::WritingToObjectStore(db) => {
                self.state = ChunkState::WrittenToObjectStore(db, chunk);
                Ok(())
            }
            state => {
                self.state = state;
                unexpected_state!(
                    self,
                    "setting object store",
                    "MovingToObjectStore",
                    &self.state
                )
            }
        }
    }
}
